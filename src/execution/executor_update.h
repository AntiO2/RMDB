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

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

    std::vector<IxIndexHandle*> index_handlers;
   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = std::move(set_clauses); // 设置哪一列为哪一个右值
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids); // rids_
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
        std::vector<int> set_cols; // 需要被设置的cols offset
        std::vector<int> set_lens;
        auto set_size = set_clauses_.size();
        std::for_each(set_clauses_.begin(), set_clauses_.end(),[this,&set_cols, &set_lens](SetClause& set_clause) { //  引用捕获
            auto set_col = tab_.get_col(set_clause.lhs.col_name); // 找到在原表中更新的列
            if(set_col->type!=set_clause.rhs.type) {
                // 需要目标列和右值匹配
                throw IncompatibleTypeError(coltype2str(set_col->type), coltype2str(set_clause.rhs.type));
            }
            set_lens.emplace_back(set_col->len);
            set_clause.rhs.init_raw(set_col->len); // 使得原始的二进制数据能够读取
            set_cols.emplace_back(set_col->offset);

        });
        // assert(set_cols.size()==set_size);
        std::for_each(rids_.begin(),rids_.end(),[this, set_size,&set_cols, &set_lens](const Rid&rid){
            auto tuple = fh_->get_record(rid,context_);
            auto tuple_ptr = tuple.get();
            if(CheckConditions(tuple_ptr,conds_)) {
                // 如果满足条件
                RmRecord new_tuple(tuple->size,tuple->data);
                auto index_size = index_handlers.size();
                for(decltype(set_size) i = 0; i < set_size; i++) {
                    memcpy(new_tuple.data+set_cols[i], set_clauses_[i].rhs.raw->data,set_lens[i]); // 修改所有列
                }
                for(size_t i = 0; i < index_size;i++) {
                    index_handlers.at(i)->delete_entry(tuple->key_from_rec(tab_.indexes.at(i).cols)->data, context_->txn_);
                    try {
                        index_handlers.at(i)->insert_entry(new_tuple.key_from_rec(tab_.indexes.at(i).cols)->data, rid, context_->txn_);
                    } catch(IndexEntryDuplicateError &e) {
                        for(size_t j = 0; j <= i; j++) {
                            index_handlers.at(i)->insert_entry(tuple->key_from_rec(tab_.indexes.at(i).cols)->data, rid, context_->txn_);
                        }
                        throw std::move(e);
                    }
                }
                fh_->update_record(rid,new_tuple.data,context_,&tab_name_);
                RmRecord update_record(*tuple);
                auto* writeRecord = new WriteRecord(WType::UPDATE_TUPLE,tab_name_,rid,update_record);
                context_->txn_->append_write_record(writeRecord);
            }});
        // LOG_DEBUG("Update Complete");
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