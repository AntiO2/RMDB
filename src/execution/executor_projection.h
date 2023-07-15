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

class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
    std::vector<ColMeta> cols_;                     // 需要投影的字段
    size_t len_;                                    // 字段总长度
    std::vector<size_t> sel_idxs_;                  
    bool is_end_{false};
   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols(); // 从被投影的列中找到 需要选出来的列
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin()); // 指示投影的列 在第几个
            auto col = *pos;
            col.offset = curr_offset; // 这里的offset是指相对于 被投影后的tuple的
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;
    }

    void beginTuple() override {
        prev_->beginTuple();
        if(prev_->is_end()) {
            is_end_ = true;
        }
    }

    void nextTuple() override {
        prev_->nextTuple();
        if(prev_->is_end()) {
            is_end_ = true;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if(is_end()) {
            return nullptr;
        }
        auto projection_record = prev_->Next();
        return GetRecordFromKeys(projection_record.get());;
    }

    Rid &rid() override { return _abstract_rid; }

    [[nodiscard]] bool is_end() const override {
        return is_end_;
    }
    std::unique_ptr<RmRecord> GetRecordFromKeys(RmRecord* rm) {
        auto col_num = cols_.size();
        auto rec = RmRecord(len_); // 通过rec大小，创建空的rmrecord;
        for(decltype(col_num) i = 0; i < col_num; i++) {

            auto &col = cols_[i]; // 投影后的列
            auto &prev_col = prev_->cols().at( sel_idxs_[i]); // check(AntiO2) 这里可以优化。 如果子算子不需要cols，可以直接move过来。
            if(col.type!=prev_col.type) {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(prev_col.type));
            }
            memcpy(rec.data+col.offset, rm->data+prev_col.offset, col.len);
//            if (col.type != val.type) {
//                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
//            }
            //  val.init_raw(col.len);
            // 将Value数据存入rec中。
        }
        return std::make_unique<RmRecord>(rec);
    }

    size_t tupleLen() const override {
        return len_;
    }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    std::string getType() override {
        return "Projection Executor";
    }

    ColMeta get_col_offset(const TabCol &target) override {
        // TODO(AntiO2) 完善这个
        return AbstractExecutor::get_col_offset(target);
    }
};