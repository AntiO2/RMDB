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

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同
    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    size_t index_match_length_;
    IxIndexHandle* ix_handler_{nullptr};
    Rid rid_;
    SmManager *sm_manager_;
    std::unique_ptr<IxScan> ix_scan_;
    std::unique_ptr<RmRecord> rm_; //下一个Next返回的record

    bool is_end_{false};
   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    IndexMeta indexMeta, size_t index_match_length, Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = std::move(index_col_names);
        //index_meta_ = *(tab_.get_index_meta(index_col_names_));
        index_meta_ = std::move(indexMeta);
        index_match_length_ = index_match_length;
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                // assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
      if(context_->txn_->get_isolation_level()==IsolationLevel::SERIALIZABLE) {
        context_->lock_mgr_->lock_shared_on_table(context_->txn_,fh_->GetFd());
      }
        auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_,index_meta_.cols);
        auto iter = sm_manager_->ihs_.find(index_name);
        if(iter==sm_manager_->ihs_.end()) {
            auto index_handler = sm_manager_->get_ix_manager()->open_index(index_name);
            iter = sm_manager_->ihs_.emplace(index_name,std::move(index_handler)).first;
        }
        ix_handler_=iter->second.get(); // 获取索引

        char upper_key[index_meta_.col_tot_len]; // 上限
        char lower_key[index_meta_.col_tot_len]; //下限

        bool upper = false;
        bool lower = false;
        long equal = -1;
        int offset = 0;

        long prev_pos = -1;
        auto prev_len = 0;
        std::vector<ColType> index_col_types;
        std::vector<int> index_col_lens;
        for(auto& index_col:index_meta_.cols) {
            index_col_types.emplace_back(index_col.type);
            index_col_lens.emplace_back(index_col.len);
        }

        for(size_t i = 0; i < index_match_length_; i++) {
            const auto&cond = conds_.at(i);
            auto col = get_col(cols_,cond.lhs_col); // 找到被比较的列
            auto pos = std::distance(index_meta_.cols.cbegin() ,get_col(index_meta_.cols,cond.lhs_col)); // 找到当前的条件左侧列在联合索引中是第几个
            if(pos!=prev_pos) {
                //  如果移到了下一列
                offset+=prev_len;
            }
            switch (conds_.at(i).op) {
                case OP_EQ:
                    if(equal==pos) {
                        // 如果在该列上有两个等于，直接跳过
                        break;
                    }
                    memcpy(upper_key+offset,cond.rhs_val.raw->data,col->len);
                    memcpy(lower_key+offset,cond.rhs_val.raw->data,col->len);
                    equal = pos;
                    break;
                case OP_LT:
                case OP_LE:
                    if(equal==pos) {
                        // 如果在该列上已经有了等于条件
                        break;
                    }
                    if(upper) {
                        // 如果已经有了上限，需要看哪个更小
                        char new_upper[index_meta_.col_tot_len];
                        memcpy(new_upper,upper_key,offset); // 将先前的key 前面的部分复制进去
                        memcpy(new_upper+offset,cond.rhs_val.raw->data,col->len);
                        if(ix_compare(new_upper,upper_key,index_col_types,index_col_lens, pos) < 0) {
                            memcpy(upper_key+offset,cond.rhs_val.raw->data,col->len);
                        }
                    } else {
                        memcpy(upper_key+offset,cond.rhs_val.raw->data,col->len);
                        upper = true;
                    }
                    break;
                case OP_GT:
                case OP_GE:
                    if(equal==pos) {
                        // 如果在该列上已经有了等于条件
                        break;
                    }
                    if(lower) {
                        // 如果已经有了上限，需要看哪个更小
                        char new_lower[index_meta_.col_tot_len];
                        memcpy(new_lower,lower_key,offset); // 将先前的key 前面的部分复制进去
                        memcpy(new_lower+offset,cond.rhs_val.raw->data,col->len);
                        if(ix_compare(new_lower,upper_key,index_col_types,index_col_lens, pos) > 0) { // 比如where a>1 and a>3 出现了更大的条件，更新lower key
                            memcpy(upper_key+offset,cond.rhs_val.raw->data,col->len);
                        }
                    } else {
                        memcpy(lower_key+offset,cond.rhs_val.raw->data,col->len);
                        lower = true;
                    }
                    break;
                default:
                    break;
            }
            prev_pos = pos;
            prev_len = col->len;
        }
        Iid lower_iid{},upper_iid{};
        if(lower) {
            // 最后的判断条件有大于/大于等于
            lower_iid = ix_handler_->lower_bound_cnt(lower_key,prev_pos + 1);
        } else {
            // 没有下限
            if(equal >=0) {
                // 前面有等于号，从等于号开始找
                lower_iid = ix_handler_->lower_bound_cnt(lower_key,equal+1);
            } else {
                // 相当于没有下限了
                lower_iid = ix_handler_->leaf_begin();
            }
        }
        if(upper) {
            // 最后的判断条件有小于/小于等于
            upper_iid = ix_handler_->upper_bound_cnt(upper_key,prev_pos + 1);
        } else {
            // 没有上限
            if(equal >=0) {
                // 前面有等于号，从等于号开始找
                upper_iid = ix_handler_->upper_bound_cnt(upper_key,equal+1);
            } else {
                // 相当于没有下限了
                upper_iid = ix_handler_->leaf_end();
            }
        }
        ix_scan_ = std::make_unique<IxScan>(ix_handler_,lower_iid,upper_iid,sm_manager_->get_bpm());

        nextTuple();
    }

    void nextTuple() override {
        if(is_end_) {
            return;
        }
        while(!ix_scan_->is_end()) {
            rid_ = ix_scan_->rid(); // 获取下一个rid
            rm_ = fh_->get_record(rid_,context_);
            if(CheckConditions()) {
                // 如果条件为真（所有where 通过）
                ix_scan_->next();
                return;
            }
            ix_scan_->next();
        }
        is_end_=true;
    }

    std::unique_ptr<RmRecord> Next() override {
        return std::move(rm_);
    }

    Rid &rid() override { return rid_; }

    [[nodiscard]] size_t tupleLen() const override {
        return len_;
    }

    [[nodiscard]] const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    std::string getType() override {
        return "Index Scan";
    }

    [[nodiscard]] bool is_end() const override {
        return is_end_;
    }
    bool CheckConditions() {
        /**
         * 检查所有条件
         */
        return std::all_of(conds_.begin(),conds_.end(),[ this](const Condition& condition){
            return CheckCondition(condition);
        });
    }
    bool CheckCondition(const Condition& condition) {
        if(condition.is_always_false_) {
            return false;
        }
        auto left_col = get_col(cols_,condition.lhs_col); // 首先根据condition中，左侧列的名字，来获取该列的数据
        char* l_value = rm_->data+left_col->offset; // 获得左值。CHECK(AntiO2) 这里左值一定是常量吗？有没有可能两边都是常数。
        char* r_value;
        ColType r_type{};
        if(condition.is_rhs_val) {
            // 如果右值是一个常数
            r_value = condition.rhs_val.raw->data;
            r_type = condition.rhs_val.type;
        } else {
            // check(AntiO2) 这里只有同一张表上两个列比较的情况吗？
            auto r_col =  get_col(cols_,condition.rhs_col);
            r_value = rm_->data + r_col->offset;
            r_type = r_col->type;
        }
     //    assert(left_col->type==r_type); // 保证两个值类型一样。
        return evaluate_compare(l_value, r_value, r_type, left_col->len, condition.op); // 判断该condition是否成立（断言为真）
    }
};