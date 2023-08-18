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
#include <utility>
#include "transaction/transaction.h"
#include "common/rwlatch.h"
#include "common/common.h"
#include "logger.h"
static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};
inline int gap_point_compare(const char* a, const char* b, const std::vector<ColType>& col_types, const std::vector<int>& col_lens, size_t col_num = 0) {
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

class LockManager {
public:
    /* 加锁类型，包括共享锁、排他锁、意向共享锁、意向排他锁、SIX（意向排他锁+共享锁） */
    enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, S_IX };
private:
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

    class GapLockRequestQueue {
    public:
        explicit GapLockRequestQueue(std::vector<ColMeta>& colMeta);

        GapLockRequestQueue(const std::vector<ColMeta> vector1);

        std::condition_variable cv_;
        std::mutex latch_;
        std::list<std::shared_ptr<GapLockRequest>> s_request_queue_; // 申请S的范围锁
        std::list<std::shared_ptr<GapLockRequest>> x_request_queue_; // 申请X的范围锁
        std::vector<ColMeta> col_meta_;
        std::vector<ColType> col_type_;
        std::vector<int> col_len_;
        /**
         * 检查两个request是否有重叠
         * @param a_request
         * @param b_request
         * @return
         */
        bool CheckGapLockCompat(GapLockRequest &a_request, GapLockRequest &b_request) {
            // (1,1) (1,2) (1,3) (2,1) (2,2) (2,3)
            // 假设有left > 1 , right< 1,1是否构成相交？ 否
            // 假设有left>=1 right<1,2是否构成相交？ 是
            // 假设有left>=1 right<1,0是否构成相交？ 是（假设插入（1，-1），在left~right之间）
            // left > 1,2 right <= 1
            // 这里采用逆向判断，也即a.right < b.left
            // a.left > b.right 这种情况是不相交的。

                // 开始比较第一个gap lock的右端点是否在第二个的gap lock的左端点左边

                switch (a_request.right_lock.type_) {

                    case INF:
                        break;
                    case E: {
                        auto cmp_len = std::min(a_request.right_lock.col_len_,b_request.left_lock.col_len_);
                        auto res = ix_compare(a_request.right_lock.key_,b_request.left_lock.key_,col_type_,col_len_,cmp_len);
                        switch (b_request.left_lock.type_) {
                            case INF:
                                break;
                            case E:
                                // 需要b.left>a.right
                            {
                                if(res < 0 ) {
                                    // a <= a.right < b.left<=b
                                    return true;
                                }
                                if(res > 0) {
                                    // a.right > b.left
                                    break;
                                }
                                // a <=1，+inf b >= 1,2 相交
                                // a <= 1,3 b >=1,-inf 相交
                                break;
                            }
                                break;
                            case NE:
                                // 需要b.left>=a.right
                            {
                                if(res < 0 ) {
                                    // a <= a.right < b.left<=b
                                    return true;
                                }
                                if(res > 0) {
                                    // a.right > b.left
                                    break;
                                }
                                // a <=1，+inf b > 1,2 相交
                                if(b_request.left_lock.col_len_ > cmp_len) {
                                    return true;
                                }
                                // a<=1,2 b>1 不相交
                            }
                                break;
                        }

                        break;
                    }

                    case NE: {
                        auto cmp_len = std::min(a_request.right_lock.col_len_,b_request.left_lock.col_len_);
                        auto res = ix_compare(a_request.right_lock.key_,b_request.left_lock.key_,col_type_,col_len_,cmp_len);
                        switch (b_request.left_lock.type_) {
                            case INF:
                                // a < 1 b>-inf 相交
                                break;
                            case E: {
                                // 需要b.left>=a.right
                                if(res < 0 ) {
                                    // a <= a.right < b.left<=b
                                    return true;
                                }
                                if(res > 0) {
                                    // a.right > b.left
                                    break;
                                }
                                // b >=1 a <1
                                // b >=1,2 ,a <1
                                if(a_request.right_lock.col_len_<=b_request.left_lock.col_len_) {
                                    return true;
                                }
                                // b>=1 a <1,2相交，可能存在1，1
                                break;
                             }
                            case NE: {
                                if(res < 0 ) {
                                    // a <= a.right < b.left<=b
                                    return true;
                                }
                                if(res > 0) {
                                    // a.right > b.left
                                    break;
                                }
                                // 需要b.left>=a.right
                                // b > 1, a <1
                                // b> 1,2 , a <1
                                // b>1,a <1,2
                                // 都不相交
                                return true;
                                break;
                            }

                        }
                        break;
                    }

                }
            // a.left > b.right的情况
            switch (b_request.right_lock.type_) {
                case INF:
                    break;
                case E: {
                    auto cmp_len = std::min(a_request.left_lock.col_len_,b_request.right_lock.col_len_);
                    auto res = ix_compare(a_request.left_lock.key_,b_request.right_lock.key_,col_type_,col_len_,cmp_len);
                    switch (a_request.left_lock.type_) {
                        case INF:
                            break;
                        case E: {
                            // a.left > b.right

                            if(res > 0) {
                                // a > a.left > b.right > b
                                return true;
                            }
                            if(res < 0) {
                                // a>=1,2 b <=2
                                break;
                            }
                            // a >=1,2 b<=1 存在1，3相交
                            // a >=1 b<=1 相交
                            // a >=1 b<=1,2 相交
                            break;
                        }

                        case NE: {
                            if(res > 0) {
                                // a > a.left > b.right > b
                                return true;
                            }
                            if(res < 0) {
                                // a>=1,2 b <=2
                                break;
                            }
                            // a > 1 b<=1 不相交
                            // a > 1,2 b <=1 存在1，3
                            // a > 1, b <=1,2, 不相交
                            if(a_request.left_lock.col_len_ <= b_request.right_lock.col_len_) {
                                return true;
                            }
                            break;
                        }
                    }
                    break;
                }

                case NE: {
                    // b.right < 2
                    auto cmp_len = std::min(a_request.left_lock.col_len_,b_request.right_lock.col_len_);
                    auto res = ix_compare(a_request.left_lock.key_,b_request.right_lock.key_,col_type_,col_len_,cmp_len);
                    switch (a_request.left_lock.type_) {
                        case INF:
                            break;
                        case E: {
                            // a.left >= b.right
                            if(res > 0) {
                                // a > a.left > b.right > b
                                return true;
                            }
                            if(res < 0) {
                                // a>=1,2 b <2
                                break;
                            }
                            // b<2 a>=2 no
                            // b<2 a>=2,1 不相交
                            // b<2,1 a>=2 相交
                            if(a_request.left_lock.col_len_ >= b_request.right_lock.col_len_) {
                                return true;
                            }
                            break;
                        }
                        case NE:
                            // a.left >= b.right
                            if(res > 0) {
                                // a > a.left > b.right > b
                                return true;
                            }
                            if(res < 0) {
                                // a>1,2 b <2
                                break;
                            }
                            // b <2 a >2 不相交
                            // b <2,1 a >2 不相交
                            // b <2 a > 2,1 不相交
                            return true;
                            break;
                    }
                    break;
                }
            }
            //不兼容
            return false;
        }
        bool CheckSLock(GapLockRequest &request) {
            // 检查是否和读锁冲突
            for(auto &s_request:s_request_queue_) {
                if(s_request->granted_) {
                    if(s_request->txn_id!=request.txn_id&&!CheckGapLockCompat(*(s_request), request)) {
                        // 如果有区间重复
                        return false;
                    }
                }
            }
            return true;
        }
        bool CheckXLock(GapLockRequest &request) {
            // 检查是否和读锁冲突
            auto res = true;
            for(auto &x_request:x_request_queue_) {
                if(x_request->granted_) {
                    if(x_request->txn_id!=request.txn_id&&!CheckGapLockCompat(*(x_request), request)) {
                        // 如果有区间重复
                        return false;
                    }
                }
            }
            return true;
        }
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

    bool lock_gap_on_index(Transaction *txn,GapLockRequest request, int iid, const std::vector<ColMeta> &col_meta,LockMode lock_mode);
    bool unlock_gap_on_index(Transaction *txn, int iid);
private:
    std::mutex latch_;      // 用于锁表的并发
    std::unordered_map<LockDataId, std::shared_ptr<LockRequestQueue>> lock_table_;   // 全局锁表
    std::unordered_map<oid_t , std::shared_ptr<GapLockRequestQueue>> gap_lock_table_; // 用于给间隙上锁
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
