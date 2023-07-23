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

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    //ColMeta cols_;                              // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    size_t tuple_num;                             //limit有限制时进行计数
    //bool is_desc_;
    std::vector<OrderCol> order_cols;
    int limit;
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;
    //收集一下找到的全部record, 方便排序
    std::vector<std::unique_ptr<RmRecord>> tuples;

    bool is_end_{false}; // 指示是否完成

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<OrderCol> order_cols_, int limit_) {
        prev_ = std::move(prev);
//        cols_ = prev_->get_col_offset(sel_cols);
//        is_desc_ = is_desc;
        limit = limit_;
        order_cols = std::move(order_cols_);
        tuple_num = 0;
        used_tuple.clear();
    }

    bool compare(const std::unique_ptr<RmRecord> &lhs, const std::unique_ptr<RmRecord> &rhs)
    {
        for( auto &order_col : order_cols){
            auto colMeta = *get_col(prev_->cols(), order_col.tab_col);

            int res = value_compare(lhs->data + colMeta.offset, rhs->data + colMeta.offset, colMeta.type, colMeta.len);
            //ia < ib，则返回 -1; ia > ib，则返回 1
            //ia == ib，则返回 0
            if((res == -1 && !order_col.is_desc_) || (res == 1 && order_col.is_desc_))
                return true;
            else if(res == 0)
                continue;
            else
                return false;
        }
    }

    void beginTuple() override {
        used_tuple.clear();
        prev_->beginTuple();
        if(prev_->is_end()) {
            is_end_ = true;
        }
        //先遍历一遍，获取到所有record, 再进行排序
        for( ; !prev_->is_end() ; prev_->nextTuple()){
            tuples.push_back(prev_->Next());
        }

        std::sort(tuples.begin(), tuples.end(), [this](const std::unique_ptr<RmRecord> &lhs, const std::unique_ptr<RmRecord> &rhs) {
            return compare(lhs, rhs);
        });
        tuple_num = 0;
    }

    void nextTuple() override {
        prev_->nextTuple();
        if(prev_->is_end()) {
            is_end_ = true;
        }
        tuple_num++;
    }

    std::unique_ptr<RmRecord> Next() override {
        if(is_end()) {
            return nullptr;
        }
        return std::move(tuples[tuple_num]);
    }

    [[nodiscard]] bool is_end() const override {
        if((limit > 0 && tuple_num == limit))
            return true;
        return is_end_;
    }

    Rid &rid() override { return _abstract_rid; }
};