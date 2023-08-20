/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include "fmt/core.h"

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // s.,can后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

//    //liamY 加入了处理聚合函数的变量 col_as_name_ 记录聚合函数的as的名字 op_记录操作
//    std::string col_as_name_;
//    AggregateOp op_;

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

    std::unique_ptr<RmRecord> rec_; // 存储下一个要返回的rec_

    bool is_end_{false}; // 指示是否完成了扫描
    bool dml_mode_;
   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context, bool dml_mode) {
        dml_mode_ = dml_mode;
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len; // 输出字段长度
        context_ = context;
        fed_conds_ = conds_;
    }
//    //liamY 重载了构造函数，使得对聚合函数有新的信息col_as_name_ 和 op_
//    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context,std::string col_as_name,AggregateOp op) {
//        sm_manager_ = sm_manager;
//        tab_name_ = std::move(tab_name);
//        conds_ = std::move(conds);
//        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
//        fh_ = sm_manager_->fhs_.at(tab_name_).get();
//        cols_ = tab.cols;
//        len_ = cols_.back().offset + cols_.back().len; // 输出字段长度
//        context_ = context;
//        fed_conds_ = conds_;
//        col_as_name_ = col_as_name;
//        op_ = op;
//    }

    void beginTuple() override {
        // 首先初始化。
        is_end_ = false;
        scan_ = std::make_unique<RmScan>(fh_); // 首先通过RmScan 获取对表的扫描
        while(!scan_->is_end()) {
            rid_ = scan_->rid();

            // LOG_DEBUG("%s", fmt::format("rid page_no {} slot_no{}",rid_.page_no,rid_.slot_no).c_str());
            if(CheckConditionByRid(rid_)) {
                // 找到了满足条件的
                // 找到了满足条件的
                if(context_->txn_->get_isolation_level()==IsolationLevel::REPEATABLE_READ) {
                    if(dml_mode_) {
                        context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid_, fh_->GetFd());
                    } else {
                        if(!context_->txn_->IsRowSharedLocked(fh_->GetFd(),rid_)&&!context_->txn_->IsRowExclusiveLocked(fh_->GetFd(),rid_)) {
                            context_->lock_mgr_->lock_shared_on_record(context_->txn_, rid_, fh_->GetFd());
                        }
                    }
                }
                if(fh_->is_mark_delete(rid_,context_)) {
                    scan_->next();
                    continue;
                }
                is_end_ = false;
                return;
            }
            scan_->next();
        }
        is_end_ = true;
    }

    void nextTuple() override {
        scan_->next();
        while(!scan_->is_end()) {
            rid_ = scan_->rid();

            // LOG_DEBUG("%s", fmt::format("rid page_no {} slot_no{}",rid_.page_no,rid_.slot_no).c_str());
            if(CheckConditionByRid(rid_)) {
                // 找到了满足条件的
                if(context_->txn_->get_isolation_level()==IsolationLevel::REPEATABLE_READ) {
                    if(dml_mode_) {
                        context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid_, fh_->GetFd());
                    } else {
                        if(!context_->txn_->IsRowSharedLocked(fh_->GetFd(),rid_)&&!context_->txn_->IsRowExclusiveLocked(fh_->GetFd(),rid_)) {
                            context_->lock_mgr_->lock_shared_on_record(context_->txn_, rid_, fh_->GetFd());
                        }
                    }
                }
                if(fh_->is_mark_delete(rid_,context_)) {
                    scan_->next();
                    continue;
                }
                return;
            }
            scan_->next();
        }
        is_end_ = true;
    }

    std::unique_ptr<RmRecord> Next() override {
        if(is_end()) {
            // check(AntiO2) 这里是否需要抛出异常
            return nullptr;
        }
        return std::move(rec_);
    }

    Rid &rid() override { return rid_; }

    [[nodiscard]] bool is_end() const override{
      return is_end_;
    }

    bool CheckConditionByRid(const Rid& rid) {
        rec_ = fh_->get_record(rid,context_);
        auto rec = rec_.get();
        return CheckConditions(rec_.get(),conds_);
    }
    /**
     * TODO(AntiO2) 这里可以考虑创建 Filter Executor， 从而在Seq Scan中不进行逻辑判断
     * 判断是否符合条件
     * @param rec
     * @param conds
     * @return
     */
    bool CheckConditions(RmRecord* rec, const std::vector<Condition>& conditions) {
        /**
         * 检查所有条件
         */
        return std::all_of(conditions.begin(),conditions.end(),[rec, this](const Condition& condition){
            return CheckCondition(rec,condition);
        });
    }
    /**
     * @description 检查单个断言是否成立
     * @param rec
     * @param conditions
     * @return
     */
    bool CheckCondition(RmRecord* rec, const Condition& condition) {
            if(condition.is_always_false_) {
                return false;
            }
            auto left_col = get_col(cols_,condition.lhs_col); // 首先根据condition中，左侧列的名字，来获取该列的数据
            char* l_value = rec->data+left_col->offset; // 获得左值。CHECK(AntiO2) 这里左值一定是常量吗？有没有可能两边都是常数。
            char* r_value;
            ColType r_type{};
            if(condition.is_rhs_val) {
                // 如果右值是一个常数
                r_value = condition.rhs_val.raw->data;
                r_type = condition.rhs_val.type;
            } else {
                // check(AntiO2) 这里只有同一张表上两个列比较的情况吗？
               auto r_col =  get_col(cols_,condition.rhs_col);
               r_value = rec->data + r_col->offset;
               r_type = r_col->type;
            }
             //  assert(left_col->type==r_type); // 保证两个值类型一样。
            return evaluate_compare(l_value, r_value, r_type, left_col->len, condition.op); // 判断该condition是否成立（断言为真）
    }

    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    [[nodiscard]] size_t tupleLen() const override {
        return len_;
    }

    std::string getType() override {
        return AbstractExecutor::getType();
    }

    ColMeta get_col_offset(const TabCol &target) override {
        return AbstractExecutor::get_col_offset(target);
    }
};