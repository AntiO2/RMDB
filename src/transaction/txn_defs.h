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
#include <cassert>

#include "common/config.h"
#include "defs.h"
#include "record/rm_defs.h"

/* 标识事务状态 */
enum class TransactionState { DEFAULT, GROWING, SHRINKING, COMMITTED, ABORTED };

/* 系统的隔离级别，当前赛题中为可串行化隔离级别 */
enum class IsolationLevel { READ_UNCOMMITTED, REPEATABLE_READ, READ_COMMITTED, SERIALIZABLE };

/* 事务写操作类型，包括插入、删除、更新三种操作 */
enum class WType { INSERT_TUPLE = 0, DELETE_TUPLE, UPDATE_TUPLE};

/**
 * @brief 事务的写操作记录，用于事务的回滚
 * INSERT
 * --------------------------------
 * | wtype | tab_name | tuple_rid |
 * --------------------------------
 * DELETE / UPDATE
 * ----------------------------------------------
 * | wtype | tab_name | tuple_rid | tuple_value |
 * ----------------------------------------------
 */
class WriteRecord {
   public:
    WriteRecord() = default;

    // constructor for insert operation
    WriteRecord(WType wtype, const std::string &tab_name, const Rid &rid, lsn_t undo_next)
        : wtype_(wtype), tab_name_(tab_name), rid_(rid), undo_next_(undo_next) {}

    // constructor for delete & update operation
    WriteRecord(WType wtype, const std::string &tab_name, const Rid &rid, const RmRecord &record, lsn_t undo_next)
        : wtype_(wtype), tab_name_(tab_name), rid_(rid), record_(record), undo_next_(undo_next) {}

    ~WriteRecord() = default;

    inline RmRecord &GetRecord() { return record_; }

    inline Rid &GetRid() { return rid_; }

    inline WType &GetWriteType() { return wtype_; }

    inline std::string &GetTableName() { return tab_name_; }

    lsn_t getUndoNext() const {
        return undo_next_;
    }

    void setRid(const Rid &rid) {
        rid_ = rid;
    }

private:
    WType wtype_;
    std::string tab_name_;
    Rid rid_;
    RmRecord record_;
    lsn_t undo_next_;
};
/*间隙锁的端点*/
enum GapLockPointType{INF,E,NE}; // 无端点，相等，空心端点
class GapLockPoint {
public:
    GapLockPoint() {}

    GapLockPoint(char *key, GapLockPointType type, size_t tot_len,size_t col_len): col_len_{col_len}{
        key = new char[tot_len];
        type_ = type;
        if(key!= nullptr) {
            memcpy(key_,key,tot_len);
        }
    }
    char* key_{};
    size_t col_len_;
    GapLockPointType type_;
};
class GapLockRequest {
public:
    GapLockRequest(const GapLockPoint &leftLock, const GapLockPoint &rightLock, txn_id_t txnId) : right_lock(rightLock), left_lock(
            leftLock), txn_id(txnId) {}

    GapLockPoint right_lock;
    GapLockPoint left_lock;
    txn_id_t txn_id;
    bool granted_{false};
};
/* 多粒度锁，加锁对象的类型，包括记录和表 */
enum class LockDataType { TABLE = 0, RECORD = 1 };

/**
 * @description: 加锁对象的唯一标识
 */
class LockDataId {
   public:
    /* 表级锁 */
    LockDataId(int fd, LockDataType type) {
        assert(type == LockDataType::TABLE);
        fd_ = fd;
        type_ = type;
        rid_.page_no = -1;
        rid_.slot_no = -1;
    }

    /* 行级锁 */
    LockDataId(int fd, const Rid &rid, LockDataType type) {
        assert(type == LockDataType::RECORD);
        fd_ = fd;
        rid_ = rid;
        type_ = type;
    }

    inline int64_t Get() const {
        if (type_ == LockDataType::TABLE) {
            // fd_
            return static_cast<int64_t>(fd_);
        } else {
            // fd_, rid_.page_no, rid.slot_no
            return ((static_cast<int64_t>(type_)) << 63) | ((static_cast<int64_t>(fd_)) << 31) |
                   ((static_cast<int64_t>(rid_.page_no)) << 16) | rid_.slot_no;
        }
    }

    bool operator==(const LockDataId &other) const {
        if (type_ != other.type_) return false;
        if (fd_ != other.fd_) return false;
        return rid_ == other.rid_;
    }
    int fd_;
    Rid rid_;
    LockDataType type_;
};

template <>
struct std::hash<LockDataId> {
    size_t operator()(const LockDataId &obj) const { return std::hash<int64_t>()(obj.Get()); }
};

/* 事务回滚原因 */
enum class AbortReason { LOCK_ON_SHRINKING = 0, UPGRADE_CONFLICT, DEADLOCK_PREVENTION,
                         ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD,
                         TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS,TABLE_LOCK_NOT_PRESENT,
                         ATTEMPTED_INTENTION_LOCK_ON_ROW,LOCK_SHARED_ON_READ_UNCOMMITTED};

/* 事务回滚异常，在rmdb.cpp中进行处理 */
class TransactionAbortException : public std::exception {
    txn_id_t txn_id_;
    AbortReason abort_reason_;

   public:
    explicit TransactionAbortException(txn_id_t txn_id, AbortReason abort_reason)
        : txn_id_(txn_id), abort_reason_(abort_reason) {}

    txn_id_t get_transaction_id() { return txn_id_; }
    AbortReason GetAbortReason() { return abort_reason_; }
    std::string GetInfo() {
        switch (abort_reason_) {
            case AbortReason::LOCK_ON_SHRINKING: {
                return "Transaction " + std::to_string(txn_id_) +
                       " aborted because it cannot request locks on SHRINKING phase\n";
            } break;

            case AbortReason::UPGRADE_CONFLICT: {
                return "Transaction " + std::to_string(txn_id_) +
                       " aborted because another transaction is waiting for upgrading\n";
            } break;

            case AbortReason::DEADLOCK_PREVENTION: {
                return "Transaction " + std::to_string(txn_id_) + " aborted for deadlock prevention\n";
            } break;

            case AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD:
              return "Transaction " + std::to_string(txn_id_) + " aborted for attempt to unlock but no lock held\n";
              break;
            case AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS:
              return "Transaction " + std::to_string(txn_id_) + " aborted for unlock table before unlocking rows\n";
              break;
            case AbortReason::TABLE_LOCK_NOT_PRESENT:
              return "Transaction " + std::to_string(txn_id_) + " aborted for table lock not present\n";
              break;

              //            default: {
              //                return "Transaction aborted\n";
              //            } break;
            }
    }
};