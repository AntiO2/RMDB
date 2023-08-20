/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_scan.h"

/**
 * @brief 
 * @todo 加上读锁（需要使用缓冲池得到page）
 */
void IxScan::next() {
    assert(!is_end());
    // node->page->RLock();
    assert(leaf_node_->is_leaf_page());
    assert(iid_.slot_no < leaf_node_->get_size());
    // increment slot no
    iid_.slot_no++;
    if (iid_.page_no != ih_->file_hdr_->last_leaf_ && iid_.slot_no == leaf_node_->get_size()) {
        // go to next leaf
        auto next_leaf_node_ = ih_->fetch_node(leaf_node_->get_next_leaf());
        iid_.slot_no = 0;
        next_leaf_node_->page->RLock();
        leaf_node_->page->RUnlock();
        bpm_->unpin_page(leaf_node_->get_page_id(), false);
        leaf_node_ = next_leaf_node_;
        iid_.page_no = leaf_node_->get_page_no();
        flush_is_end();
        return;
    }
    flush_is_end();
}

Rid IxScan::rid() const {
    return ih_->get_rid(iid_);
}

IxScan::IxScan(const IxIndexHandle *ih, const Iid &iid, char *endKey, size_t endKeySize, size_t colCmpNum,
               BufferPoolManager *bpm, IxNodeHandle* leaf_node_, GapLockPointType rightPointType) : ih_(ih), iid_(iid),
                                                                                          end_key_size_(endKeySize),
                                                                                          col_cmp_num_(colCmpNum),
                                                                                          bpm_(bpm),
                                                                                          leaf_node_(leaf_node_),
                                                                                          right_point_type(
                                                                                                  rightPointType) {
    if(rightPointType!=GapLockPointType::INF) {
        end_key_ = new char[end_key_size_];
        memcpy(end_key_, endKey, end_key_size_);
    }
    flush_is_end();
}

void IxScan::flush_is_end() {
    if(leaf_node_->is_root_page()&&iid_.slot_no==leaf_node_->get_size()) {
        is_end_ = true;
        return;
    }
    if(leaf_node_->get_page_no()==ih_->file_hdr_->last_leaf_&&iid_.slot_no==leaf_node_->get_size()) {
        is_end_ = true;
        return;
    }
    if (right_point_type==GapLockPointType::INF) {
        // check(AntiO2) 可以忽略这种情况
        return;
    }
    auto cmp = ix_compare(leaf_node_->get_key(iid_.slot_no),end_key_, ih_->file_hdr_->col_types_, ih_->file_hdr_->col_lens_, col_cmp_num_);
    if( (right_point_type==GapLockPointType::E&&cmp>0) ||
            (right_point_type==GapLockPointType::NE&&cmp>=0)) {
        is_end_ = true;
    }
}

IxScan::~IxScan() {
    if(leaf_node_!= nullptr&&leaf_node_->page!= nullptr) {
        leaf_node_->page->RUnlock();
        bpm_->unpin_page(leaf_node_->page->get_page_id(), false);
    }
}
