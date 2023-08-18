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
#include "transaction/transaction.h"
#include "common/common.h"
#include "common/rwlatch.h"
enum class Operation { FIND = 0, INSERT, DELETE };  // 三种操作：查找、插入、删除
enum class FIND_TYPE {LOWER,UPPER,COMMON};
static const bool binary_search = false;

inline int ix_compare(const char* a, const char* b, const std::vector<ColType>& col_types, const std::vector<int>& col_lens, size_t col_num = 0) {
    if(col_num== 0) {
        col_num=col_types.size();
    }
    int offset = 0;
    for(size_t i = 0; i < col_num; ++i) {
        int res = value_compare(a + offset, b + offset, col_types[i], col_lens[i]);
        if(res != 0) return res;
        offset += col_lens[i];
    }
    return 0;
}

/* 管理B+树中的每个节点 */
class IxNodeHandle {
    friend class IxIndexHandle;
    friend class IxScan;

   private:
    const IxFileHdr *file_hdr;      // 节点所在文件的头部信息
    Page *page;                     // 存储节点的页面
    IxPageHdr *page_hdr;            // page->data的第一部分，指针指向首地址，长度为sizeof(IxPageHdr)
    char *keys;                     // page->data的第二部分，指针指向首地址，长度为file_hdr->keys_size，每个key的长度为file_hdr->col_len
    Rid *rids;                      // page->data的第三部分，指针指向首地址

   public:
    IxNodeHandle() = default;
    // 存储结构： page_hdr| keys| rids
    IxNodeHandle(const IxFileHdr *file_hdr_, Page *page_) : file_hdr(file_hdr_), page(page_) {
        page_hdr = reinterpret_cast<IxPageHdr *>(page->get_data());
        keys = page->get_data() + sizeof(IxPageHdr);
        rids = reinterpret_cast<Rid *>(keys + file_hdr->keys_size_);
    }

    [[nodiscard]] int get_size() const { return page_hdr->num_key; }

    void set_size(int size) const { page_hdr->num_key = size; }

    int get_max_size() const { return file_hdr->btree_order_ + 1; }

    int get_min_size() { return get_max_size() / 2; }

    int key_at(int i) { return *(int *)get_key(i); }
    int key_2nd(int i) {return *(int *)(get_key(i)+4);}
    /* 得到第i个孩子结点的page_no */
    page_id_t value_at(int i) { return get_rid(i)->page_no; }

    page_id_t get_page_no() { return page->get_page_id().page_no; }

    PageId get_page_id() { return page->get_page_id(); }

    page_id_t get_next_leaf() { return page_hdr->next_leaf; }

    page_id_t get_prev_leaf() { return page_hdr->prev_leaf; }

    page_id_t get_parent_page_no() { return page_hdr->parent; }

    bool is_leaf_page() { return page_hdr->is_leaf; }

    bool is_root_page() { return get_parent_page_no() == INVALID_PAGE_ID; }

    void set_next_leaf(page_id_t page_no) { page_hdr->next_leaf = page_no; }

    void set_prev_leaf(page_id_t page_no) { page_hdr->prev_leaf = page_no; }

    void set_parent_page_no(page_id_t parent) { page_hdr->parent = parent; }

    char *get_key(int key_idx) const {
        return keys + key_idx * file_hdr->col_tot_len_;
    }

    Rid *get_rid(int rid_idx) const {
        return &rids[rid_idx];
    }

    void set_key(int key_idx, const char *key) { memcpy(keys + key_idx * file_hdr->col_tot_len_, key, file_hdr->col_tot_len_); }

    void set_rid(int rid_idx, const Rid &rid) { rids[rid_idx] = rid; }

    int lower_bound(const char *target, size_t col_num) const;
    int upper_bound(const char *target,  size_t col_num) const;
    void insert_pairs(int pos, const char *key, const Rid *rid, int n);

    page_id_t internal_lookup(const char *key, size_t col_num,FIND_TYPE findType=FIND_TYPE::COMMON);

    bool leaf_lookup(const char *key, Rid **value,  size_t col_num,FIND_TYPE findType=FIND_TYPE::COMMON);

    int insert(const char *key, const Rid &value);

    // 用于在结点中的指定位置插入单个键值对
    void insert_pair(int pos, const char *key, const Rid &rid) { insert_pairs(pos, key, &rid, 1); }

    void erase_pair(int pos);

    int remove(const char *key);

    void init(page_id_t parent_page_id = INVALID_PAGE_ID, page_id_t next_free_page_no = IX_NO_PAGE, bool is_leaf = false ) {
        page_hdr->num_key = 0;
        page_hdr->parent = parent_page_id;
        page_hdr->next_free_page_no = next_free_page_no;
        page_hdr->is_leaf = is_leaf;
    }
    /**
     * @brief used in internal node to remove the last key in root node, and return the last child
     *
     * @return the last child
     */
    page_id_t remove_and_return_only_child() {
        assert(get_size() == 1);
        page_id_t child_page_no = value_at(0);
        erase_pair(0);
        assert(get_size() == 0);
        return child_page_no;
    }

    /**
     * @brief 由parent调用，寻找child，返回child在parent中的rid_idx∈[0,page_hdr->num_key)
     * @param child
     * @return int
     */
    int find_child(IxNodeHandle *child) {
        int rid_idx;
        for (rid_idx = 0; rid_idx < page_hdr->num_key; rid_idx++) {
            if (get_rid(rid_idx)->page_no == child->get_page_no()) {
                break;
            }
        }
        assert(rid_idx < page_hdr->num_key);
        return rid_idx;
    }

private:

};

/* B+树 */
class IxIndexHandle {
    friend class IxScan;
    friend class IxManager;

   private:
    DiskManager *disk_manager_;
    BufferPoolManager *buffer_pool_manager_;
    int fd_;                                    // 存储B+树的文件
    IxFileHdr* file_hdr_;                       // 存了root_page，但其初始化为2（第0页存FILE_HDR_PAGE，第1页存LEAF_HEADER_PAGE）
    // std::mutex root_latch_;
    RWLatch root_latch_;
   public:
    IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd);
    inline int get_num_per_page() {
        return file_hdr_.
    }
    DiskManager *getDiskManager() const;

    BufferPoolManager *getBufferPoolManager() const;

    int getFd() const;

    IxFileHdr *getFileHdr() const;

    // for search
    bool get_value(const char *key, std::vector<Rid> *result, Transaction *transaction);

    std::pair<IxNodeHandle *, bool> find_leaf_page(const char *key, Operation operation, Transaction *transaction,
                                                   size_t col_cnt,FIND_TYPE find_type,bool left_most, bool right_most);
    // for insert
    page_id_t insert_entry(const char *key, const Rid &value, Transaction *transaction);

    IxNodeHandle *split(IxNodeHandle *node);

    void insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node, Transaction *transaction);

    // for delete
    bool delete_entry(const char *key, Transaction *transaction);

    bool coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction = nullptr,
                                bool *root_is_latched = nullptr);
    bool adjust_root(IxNodeHandle *old_root_node);

    void redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index);

    bool coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                  Transaction *transaction, bool *root_is_latched);

    Iid lower_bound(const char *key);
    Iid lower_bound_cnt(const char *key, size_t cnt);
    Iid upper_bound(const char *key);
    Iid upper_bound_cnt(const char *key, size_t cnt);
    Iid leaf_end();
    Iid leaf_begin();

    // for index test
    Rid get_rid(const Iid &iid) const;

private:
    // 辅助函数
    void update_root_page_no(page_id_t root) { file_hdr_->root_page_ = root; }

    bool is_empty() const { return file_hdr_->root_page_ == IX_NO_PAGE; }

    // for get/create node
    IxNodeHandle *fetch_node(int page_no) const;

    IxNodeHandle *create_node();

    // for maintain data structure
    void maintain_parent(IxNodeHandle *node);

    void erase_leaf(IxNodeHandle *leaf);

    void release_node_handle(IxNodeHandle &node);

    void maintain_child(IxNodeHandle *node, int child_idx);
    void release_ancestors(Transaction*transaction);
};