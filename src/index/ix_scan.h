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

#include "ix_defs.h"
#include "ix_index_handle.h"

// class IxIndexHandle;

// 用于遍历叶子结点
// 用于直接遍历叶子结点，而不用findleafpage来得到叶子结点
// TODO：对page遍历时，要加上读锁
class IxScan : public RecScan {
    const IxIndexHandle *ih_;
    Iid iid_;  // 初始为lower（用于遍历的指针）
    // Iid end_;  // 初始为upper
    char* end_key_;
    size_t end_key_size_;
    size_t col_cmp_num_; // 多少个col_cmp_num用于最终比较
    BufferPoolManager *bpm_;
    bool is_end_{false};
    IxNodeHandle* leaf_node_; // 当前的leaf_page
    GapLockPointType right_point_type; // 右key的约束类型，是 NE:<, 还是N: <, 还是INF(无右端点)
   public:
    IxScan(const IxIndexHandle *ih, const Iid &iid, char *endKey, size_t endKeySize, size_t colCmpNum,
           BufferPoolManager *bpm, IxNodeHandle* leaf_node, GapLockPointType rightPointType);
    void next() override;

    bool is_end() const override { return is_end_; }
    void flush_is_end();
    Rid rid() const override;

    const Iid &iid() const { return iid_; }

    ~IxScan();
};