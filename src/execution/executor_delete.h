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
#include <utility>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

    std::vector<IxIndexHandle*> index_handlers;
   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;

        for(auto &index:tab_.indexes) {
            auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_,index.cols);
            auto iter = sm_manager_->ihs_.find(index_name);
            if(iter==sm_manager_->ihs_.end()) {
                auto index_handler = sm_manager_->get_ix_manager()->open_index(index_name);
                iter = sm_manager_->ihs_.emplace(index_name,std::move(index_handler)).first;
            }
            index_handlers.emplace_back(iter->second.get());
        }
    }

    std::unique_ptr<RmRecord> Next() override {
      if(context_->txn_->get_isolation_level()==IsolationLevel::SERIALIZABLE) {
        context_->lock_mgr_->lock_exclusive_on_table(context_->txn_,fh_->GetFd());
      }
        std::for_each(rids_.begin(),rids_.end(),[this](const Rid&rid){
            auto tuple = fh_->get_record(rid,context_);
            auto tuple_ptr = tuple.get();
            if(CheckConditions(tuple_ptr,conds_)) {
                // 如果满足条件

                auto index_size = index_handlers.size();
                for(size_t i = 0; i < index_size;i++) {
                    index_handlers.at(i)->delete_entry(tuple->key_from_rec(tab_.indexes.at(i).cols)->data, context_->txn_);
                }
                RmRecord delete_record(*tuple);
                auto* writeRecord = new WriteRecord(WType::DELETE_TUPLE,tab_name_,rid,delete_record, context_->txn_->get_prev_lsn()); // 注意先获得了undo_next,再进行删除操作
                fh_->delete_record(rid,context_,&tab_name_);
                context_->txn_->append_write_record(writeRecord);
            }
      });
        LOG_DEBUG("Delete Complete");
        return nullptr;
    }
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
        auto left_col = get_col(tab_.cols,condition.lhs_col); // 首先根据condition中，左侧列的名字，来获取该列的数据
        char* l_value = rec->data+left_col->offset; // 获得左值。CHECK(AntiO2) 这里左值一定是常量吗？有没有可能两边都是常数。
        char* r_value;
        ColType r_type{};
        if(condition.is_rhs_val) {
            // 如果右值是一个常数
            r_value = condition.rhs_val.raw->data;
            r_type = condition.rhs_val.type;
        } else {
            // check(AntiO2) 这里只有同一张表上两个列比较的情况吗？
            auto r_col =  get_col(tab_.cols,condition.rhs_col);
            r_value = rec->data + r_col->offset;
            r_type = r_col->type;
        }
        // assert(left_col->type==r_type); // 保证两个值类型一样。
        return evaluate_compare(l_value, r_value, r_type, left_col->len, condition.op); // 判断该condition是否成立（断言为真）
    }
    Rid &rid() override { return _abstract_rid; }
};