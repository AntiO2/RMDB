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
/**
 *
 * @tip 这里我默认了右侧的所有元组可以全部放入内存
 * @tip 实现的Join类型： INNER_JOIN 未实现 LEFT_JOIN, RIGHT_JOIN, FULL_JOIN
 * @description 流程描述
 * @description for(left_tuples)
 * @description     for(right_tuple)
 * @description         if(条件满足)
 * @description             emit tuple
 * @check 是否需要块嵌套连接
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段
    // JoinType join_type_{JoinType::INNER_JOIN}; check(AntiO2) 是否需要实现不同类型的JoinType
    std::vector<Condition> fed_conds_;          // join条件

    std::vector<std::pair<ColMeta, ColMeta> > join_cols_;

    bool is_end_;

    std::unique_ptr<RmRecord> left_tuple_;
    std::vector<std::unique_ptr<RmRecord>> right_tuples_;
    std::vector<std::unique_ptr<RmRecord>>::const_iterator  right_tuples_iter_;
    std::unique_ptr<RmRecord> rmRecord;
    size_t left_len_;
    size_t right_len_;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        left_len_ = left_->tupleLen();
        right_len_ = right_->tupleLen();
        len_ = left_len_ + right_len_;
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen(); // Check(AntiO2) offset应该为size_t,而不是int
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        is_end_ = false;
        fed_conds_ = std::move(conds);
        for(auto const&cond:fed_conds_) {

            assert(!cond.is_rhs_val); // 需要右值不是常数
            auto left_join_col = *get_col(cols_,cond.lhs_col);
            auto right_join_col = *get_col(cols_,cond.rhs_col);
            if(left_join_col.type!=right_join_col.type) {
                throw IncompatibleTypeError(coltype2str(left_join_col.type),
                                            coltype2str(right_join_col.type));
            }
            if(left_join_col.offset>=left_len_) {
                left_join_col.offset = left_join_col.offset- left_len_;
            }
            if(right_join_col.offset>=left_len_) {
                right_join_col.offset = right_join_col.offset- left_len_;
            }
            join_cols_.emplace_back(left_join_col, right_join_col);
        }
    }

    void beginTuple() override {
        for(right_->beginTuple();!right_->is_end();right_->nextTuple()) {
            right_tuples_.emplace_back(right_->Next()); // 取出所有右执行器的tuple.
        }
        right_tuples_iter_ = right_tuples_.begin();
        left_->beginTuple();
        left_tuple_ = left_->Next();
        left_->nextTuple();
        nextTuple();
    }

    void nextTuple() override {
        while(!is_end_) {
            if(right_tuples_iter_==right_tuples_.end()) {
                if(left_->is_end()) {
                    // 两层都已经循环完了
                    is_end_ = true;
                    return;
                }
                //
                left_tuple_ = left_->Next(); // 获取外层循环的tuple;
                left_->nextTuple();
                right_tuples_iter_ = right_tuples_.begin();
            }
            // 比较当前是否满足join条件
            if(CheckConditions()) {
                RmRecord rmRecord1(len_);
                memcpy(rmRecord1.data,left_tuple_->data,left_len_);
                memcpy(rmRecord1.data+left_len_,(*right_tuples_iter_)->data,right_len_);

                rmRecord = std::make_unique<RmRecord>(rmRecord1);

                right_tuples_iter_++;
                return;
            }
            right_tuples_iter_++; // 比较下一个tuple
        }
    }
    std::unique_ptr<RmRecord> Next() override {
        if(is_end_) {
            return nullptr;
        }
        return std::move(rmRecord);
    }

    Rid &rid() override { return _abstract_rid; }

    size_t tupleLen() const override {
        return len_;
    }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    std::string getType() override {
        return "NestedLoop Join Executor";
    }

    bool is_end() const override {
        return is_end_;
    }

    ColMeta get_col_offset(const TabCol &target) override {
        return AbstractExecutor::get_col_offset(target);
    }
    bool CheckConditions() {
        /**
         * 检查所有条件
         */
        bool result = true;
        auto join_size = fed_conds_.size();
        for(decltype(join_size) i = 0; i < join_size; i++) {
            result&= Check_ith_Condition(i); // Check(AntiO2) 此处bool运算是否正确
        }
        return result;

    }
    /**
     * @description 检查第i个条件是否成立
     * @param i
     * @return
     */
    bool Check_ith_Condition(const size_t i) {
        const auto &left_col = join_cols_.at(i).first;
        const auto &right_col = join_cols_.at(i).second;
        char* l_value = left_tuple_->data+left_col.offset;
        char* r_value = (*right_tuples_iter_)->data+right_col.offset;
        return evaluate_compare(l_value, r_value, left_col.type, left_col.len, fed_conds_.at(i).op); // 判断该condition是否成立（断言为真）
    }
};