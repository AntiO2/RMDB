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

    void beginTuple() override { 
        for( prev_->beginTuple(); !prev_->is_end() ; prev_->nextTuple()){

        }
    }

    void nextTuple() override {

    }

    std::unique_ptr<RmRecord> Next() override {
        if(is_end()) {
            return nullptr;
        }
    }

    bool is_end() const override {
        if((limit > 0 && tuple_num == limit))
            return true;
        return is_end_;
    }

    Rid &rid() override { return _abstract_rid; }
};