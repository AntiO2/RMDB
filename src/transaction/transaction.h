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

#include <atomic>
#include <deque>
#include <string>
#include <thread>
#include <memory>
#include <unordered_set>
#include "storage/page.h"
#include "txn_defs.h"

class Transaction {
   public:
    explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::SERIALIZABLE)
        : state_(TransactionState::DEFAULT), isolation_level_(isolation_level), txn_id_(txn_id),
           s_table_lock_set_{new std::unordered_set<int>},
           x_table_lock_set_{new std::unordered_set<int>},
           is_table_lock_set_{new std::unordered_set<int>},
           ix_table_lock_set_{new std::unordered_set<int>},
           six_table_lock_set_{new std::unordered_set<int>},
           s_row_lock_set_{new std::unordered_map<int, std::unordered_set<Rid,RidHash>>},
           x_row_lock_set_{new std::unordered_map<int, std::unordered_set<Rid,RidHash>>} {
        write_set_ = std::make_shared<std::deque<WriteRecord *>>();
        lock_set_ = std::make_shared<std::unordered_set<LockDataId>>();
        index_latch_page_set_ = std::make_shared<std::deque<Page *>>();
        index_deleted_page_set_ = std::make_shared<std::deque<Page*>>();
        prev_lsn_ = INVALID_LSN;
        thread_id_ = std::this_thread::get_id();
    }

    ~Transaction() = default;

    inline txn_id_t get_transaction_id() { return txn_id_; }

    inline std::thread::id get_thread_id() { return thread_id_; }

    inline void set_txn_mode(bool txn_mode) { txn_mode_ = txn_mode; }
    inline bool get_txn_mode() { return txn_mode_; }

    inline void set_start_ts(timestamp_t start_ts) { start_ts_ = start_ts; }
    inline timestamp_t get_start_ts() { return start_ts_; }

    inline IsolationLevel get_isolation_level() { return isolation_level_; }
    inline void set_isolation_level(IsolationLevel level) {isolation_level_ = level;};
    inline TransactionState get_state() { return state_; }
    inline void set_state(TransactionState state) { state_ = state; }

    inline lsn_t get_prev_lsn() { return prev_lsn_; }
    inline void set_prev_lsn(lsn_t prev_lsn) { prev_lsn_ = prev_lsn; }

    inline std::shared_ptr<std::deque<WriteRecord *>> get_write_set() { return write_set_; }  
    inline void append_write_record(WriteRecord* write_record) { write_set_->push_back(write_record); }

    inline std::shared_ptr<std::deque<Page*>> get_index_deleted_page_set() { return index_deleted_page_set_; }
    inline void append_index_deleted_page(Page* page) { index_deleted_page_set_->push_back(page); }

    inline std::shared_ptr<std::deque<Page*>> get_index_latch_page_set() { return index_latch_page_set_; }
    inline void append_index_latch_page_set(Page* page) { index_latch_page_set_->push_back(page); }

    inline std::shared_ptr<std::unordered_set<LockDataId>> get_lock_set() { return lock_set_; }

    [[nodiscard]] bool isTxnMode() const { return txn_mode_; }
    [[nodiscard]] TransactionState getState() const { return state_; }
    [[nodiscard]] IsolationLevel getIsolationLevel() const { return isolation_level_; }
    [[nodiscard]] const std::thread::id &getThreadId() const { return thread_id_; }
    [[nodiscard]] lsn_t getPrevLsn() const { return prev_lsn_; }
    [[nodiscard]] txn_id_t getTxnId() const { return txn_id_; }
    [[nodiscard]] timestamp_t getStartTs() const { return start_ts_; }
    [[nodiscard]] const std::shared_ptr<std::deque<WriteRecord *>> &getWriteSet() const {
      return write_set_;
    }
    [[nodiscard]] const std::shared_ptr<std::unordered_set<LockDataId>> &getLockSet() const {
      return lock_set_;
    }
    [[nodiscard]] const std::shared_ptr<std::deque<Page *>> &getIndexLatchPageSet() const {
      return index_latch_page_set_;
    }
    [[nodiscard]] const std::shared_ptr<std::deque<Page *>> &getIndexDeletedPageSet() const {
      return index_deleted_page_set_;
    }
    [[nodiscard]] const std::shared_ptr<std::unordered_set<int>> &getSTableLockSet() const {
      return s_table_lock_set_;
    }
    [[nodiscard]] const std::shared_ptr<std::unordered_set<int>> &getXTableLockSet() const {
      return x_table_lock_set_;
    }
    [[nodiscard]] const std::shared_ptr<std::unordered_set<int>> &getIsTableLockSet() const {
      return is_table_lock_set_;
    }
    [[nodiscard]] const std::shared_ptr<std::unordered_set<int>> &getIxTableLockSet() const {
      return ix_table_lock_set_;
    }
    [[nodiscard]] const std::shared_ptr<std::unordered_set<int>> &getSixTableLockSet() const {
      return six_table_lock_set_;
    }
    [[nodiscard]] const std::shared_ptr<std::unordered_map<int, std::unordered_set<Rid,RidHash>>> &
    getSRowLockSet() const {
      return s_row_lock_set_;
    }
    [[nodiscard]] const std::shared_ptr<std::unordered_map<int, std::unordered_set<Rid,RidHash>>> &
    getXRowLockSet() const {
      return x_row_lock_set_;
    }
    auto IsRowSharedLocked(const int &oid, const Rid&rid) -> bool {
      auto row_lock_set = s_row_lock_set_->find(oid);
      if (row_lock_set == s_row_lock_set_->end()) {
        return false;
      }
      return row_lock_set->second.find(rid) != row_lock_set->second.end();
    }

    /** @return true if rid (belong to table oid) is exclusive locked by this transaction */
    auto IsRowExclusiveLocked(const int &oid, const Rid&rid) -> bool {
      auto row_lock_set = x_row_lock_set_->find(oid);
      if (row_lock_set == x_row_lock_set_->end()) {
        return false;
      }
      return row_lock_set->second.find(rid) != row_lock_set->second.end();
    }

    auto IsTableIntentionSharedLocked(const int &oid) -> bool {
      return is_table_lock_set_->find(oid) != is_table_lock_set_->end();
    }

    auto IsTableSharedLocked(const int &oid) -> bool {
      return s_table_lock_set_->find(oid) != s_table_lock_set_->end();
    }

    auto IsTableIntentionExclusiveLocked(const int &oid) -> bool {
      return ix_table_lock_set_->find(oid) != ix_table_lock_set_->end();
    }

    auto IsTableExclusiveLocked(const int &oid) -> bool {
      return x_table_lock_set_->find(oid) != x_table_lock_set_->end();
    }

    auto IsTableSharedIntentionExclusiveLocked(const int &oid) -> bool {
      return six_table_lock_set_->find(oid) != six_table_lock_set_->end();
    }



  private:
    bool txn_mode_;                   // 用于标识当前事务为显式事务还是单条SQL语句的隐式事务 [如果是true,表示显示事务]
    TransactionState state_;          // 事务状态
    IsolationLevel isolation_level_;  // 事务的隔离级别，默认隔离级别为可串行化
    std::thread::id thread_id_;       // 当前事务对应的线程id
    lsn_t prev_lsn_;                  // 当前事务执行的最后一条操作对应的lsn，用于系统故障恢复
    txn_id_t txn_id_;                 // 事务的ID，唯一标识符
    timestamp_t start_ts_;            // 事务的开始时间戳

    std::shared_ptr<std::deque<WriteRecord *>> write_set_;  // 事务包含的所有写操作
    std::shared_ptr<std::unordered_set<LockDataId>> lock_set_;  // 事务申请的所有锁

    std::shared_ptr<std::deque<Page*>> index_latch_page_set_;          // 维护事务执行过程中加锁的索引页面
    std::shared_ptr<std::deque<Page*>> index_deleted_page_set_;    // 维护事务执行过程中删除的索引页面

    // 事务拥有的表锁
    std::shared_ptr<std::unordered_set<int>> s_table_lock_set_;
    std::shared_ptr<std::unordered_set<int>> x_table_lock_set_;
    std::shared_ptr<std::unordered_set<int>> is_table_lock_set_;
    std::shared_ptr<std::unordered_set<int>> ix_table_lock_set_;
    std::shared_ptr<std::unordered_set<int>> six_table_lock_set_;

    std::shared_ptr<std::unordered_map<int, std::unordered_set<Rid,RidHash>>> s_row_lock_set_;
    std::shared_ptr<std::unordered_map<int, std::unordered_set<Rid,RidHash>>> x_row_lock_set_;
public:
    // 事务的间隙锁
    std::shared_ptr<std::unordered_set<int>> gap_lock_set_;
};
