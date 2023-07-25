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

class SortExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> prev_;
    //std::vector<ColMeta> cols_;                              // 这是要查询的cols吧
    size_t tuple_num;                             //limit有限制时进行计数
    //bool is_desc_;
    std::vector<OrderCol> order_cols;
    int limit;
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;
    //收集一下找到的全部record, 方便排序
    std::vector<std::unique_ptr<RmRecord>> tuples;

    bool is_end_{false}; // 指示是否完成
    //   SmManager *sm_manager_;

public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<OrderCol> order_cols_, int limit_)
    {
        prev_ = std::move(prev);
//        is_desc_ = is_desc;
        limit = limit_;
        order_cols = std::move(order_cols_);
        tuple_num = 0;

//        sm_manager_ = smManager;

//        std::vector<ColMeta> cols;
//        cols.reserve(order_cols.size());
//        for(auto &order :order_cols){
//            cols.push_back(prev_->get_col_meta(sm_manager_,order.tab_col));
//        }
//        cols_ = cols;

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
        return false;
    }

    void beginTuple() override
    {
        used_tuple.clear();
        prev_->beginTuple();
        if(prev_->is_end()) {
            is_end_ = true;
        } else
            is_end_ = false;

        //一开始先遍历一遍，获取到所有record, 再进行排序
        for( ; !prev_->is_end(); prev_->nextTuple()){
            tuples.push_back(prev_->Next());
        }

        //进行排序，使用lambda表达式
        std::sort(tuples.begin(), tuples.end(), [this](const std::unique_ptr<RmRecord> &lhs, const std::unique_ptr<RmRecord> &rhs) {
            return compare(lhs, rhs);
        });

        //重置，还需要第二次遍历, 这个对join不行，通过tuple_num来判断结束
//        prev_->beginTuple();
//        if(prev_->is_end()) {
//            is_end_ = true;
//        } else
//            is_end_ = false;
         tuple_num = 0;
    }

    void nextTuple() override
    {
        prev_->nextTuple();
        if(prev_->is_end()) {
            is_end_ = true;
        }
        tuple_num++;          // +1使得与limit进行比对,并支持next返回
    }

    std::unique_ptr<RmRecord> Next() override
    {
        return std::move(tuples[tuple_num]);
    }

    Rid &rid() override { return _abstract_rid; }

    //limit 的is_end判断不能用原来的
    [[nodiscard]] bool is_end() const override {
        if(limit > 0 && tuple_num == limit) {
            return true;
        }
        return tuple_num == tuples.size();
    };

    [[nodiscard]] const std::vector<ColMeta> &cols() const override
    {
        return prev_->cols();
    };
    size_t tupleLen() const override {
      return prev_->tupleLen();
    }
};
