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

#include <mutex>
#include <condition_variable>
#include <list>
#include "transaction/transaction.h"
#include "common/rwlatch.h"
#include "logger.h"
static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};

class LockManager {
    /* 加锁类型，包括共享锁、排他锁、意向共享锁、意向排他锁、SIX（意向排他锁+共享锁） */
    enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, S_IX };

    /* 用于标识加锁队列中排他性最强的锁类型，例如加锁队列中有SHARED和EXLUSIVE两个加锁操作，则该队列的锁模式为X */
    // comment(AntiO2) 感觉这个没有什么用。排它性感觉并没有先序关系。比如IX锁和S锁谁的排它性更强？
    enum class GroupLockMode { NON_LOCK, IS, IX, S, X, SIX};

    enum class LockObject { TABLE, ROW };
    enum class ModifyMode { ADD, REMOVE };
    /* 事务的加锁申请 */
    class LockRequest {
    public:
        LockRequest(txn_id_t txn_id, LockMode lock_mode)
            : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

        txn_id_t txn_id_;   // 申请加锁的事务ID
        LockMode lock_mode_;    // 事务申请加锁的类型
        bool granted_;          // 该事务是否已经被赋予锁

    };

    /* 数据项上的加锁队列 */
    class LockRequestQueue {
    public:
        std::list<std::shared_ptr<LockRequest>> request_queue_;  // 加锁队列
        std::condition_variable cv_;            // 条件变量，用于唤醒正在等待加锁的申请，在no-wait策略下无需使用
        GroupLockMode group_lock_mode_ = GroupLockMode::NON_LOCK;   // 加锁队列的锁模式
        txn_id_t upgrading_{INVALID_TXN_ID}; // 当前正在等待升级的事务编号
        std::mutex latch_;
    };

public:
    LockManager() {}

    ~LockManager() {}

    bool lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    bool lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    bool lock_shared_on_table(Transaction* txn, int tab_fd);

    bool lock_exclusive_on_table(Transaction* txn, int tab_fd);

    bool lock_IS_on_table(Transaction* txn, int tab_fd);

    bool lock_IX_on_table(Transaction* txn, int tab_fd);

    bool unlock(Transaction* txn, LockDataId lock_data_id);


private:
    std::mutex latch_;      // 用于锁表的并发
    std::unordered_map<LockDataId, std::shared_ptr<LockRequestQueue>> lock_table_;   // 全局锁表

    auto HandleLockRequest(Transaction *txn, int tab_fd, const std::shared_ptr<LockRequestQueue> &lock_request_queue,
                           LockMode lock_mode, LockObject lock_object, const Rid *rid = nullptr) -> bool;
    auto HandleUnlockRequest(Transaction *txn, int tab_fd,
                             const std::shared_ptr<LockRequestQueue> &lock_request_queue, LockObject lock_object,
                             const Rid *rid = nullptr) -> bool;

    auto CheckLock(Transaction *txn, LockMode lock_mode, LockObject lock_object) -> void;

    auto CheckTableIntentionLock(Transaction *txn, const LockMode &lockMode, int tab_fd) -> void;

    auto CheckUpgrade(LockMode old_lock, LockMode new_lock) -> bool;

    auto CheckGrant(const std::shared_ptr<LockRequest> &checked_request,
                    const std::shared_ptr<LockRequestQueue> &request_queue) -> bool;

    auto ModifyLockSet(Transaction *txn, int tab_fd, LockMode lock_mode, LockObject lock_object,
                       ModifyMode modify_mode, const Rid *rid = nullptr) -> void;
    auto ModifyRowLockSet(Transaction *txn,
                          const std::shared_ptr<std::unordered_map<int, std::unordered_set<Rid,RidHash>>> &row_lock_set,
                          const int tab_fd, const Rid *rid, ModifyMode modifyMode) -> void;

    /**
     * 检查兼容性矩阵
     * @param old_lock
     * @param new_lock
     * @return
     */
    auto CheckCompatible(LockMode old_lock, LockMode new_lock) -> bool;
};
