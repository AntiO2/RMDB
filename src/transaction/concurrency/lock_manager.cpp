/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 *
 * @return {bool} 加锁是否成功
 *
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {

  std::unique_lock<std::mutex> lock(latch_);
  if(txn->get_state()==TransactionState::SHRINKING) {
    txn->set_state(TransactionState::ABORTED);
    // 如果在收缩阶段加锁，抛出异常
    throw TransactionAbortException(txn->get_transaction_id(),AbortReason::LOCK_ON_SHRINKING);
  }
  LockDataId lock_data = LockDataId(tab_fd, rid, LockDataType::RECORD); // 获取要加锁的item
  auto lock_request_queue_iter = lock_table_.find(lock_data); // 检查在该lock item上是否已经有队列
  if(lock_request_queue_iter==lock_table_.end()) {
    lock_request_queue_iter = lock_table_.emplace(lock_data,std::make_shared<LockRequestQueue>()).first;
  }
  auto lock_request_queue = lock_request_queue_iter->second;

  return HandleLockRequest(txn,tab_fd,lock_request_queue,LockMode::SHARED, LockObject::ROW,&rid);

}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
  // assert(false);
  std::unique_lock<std::mutex> lock(latch_);
  if(txn->get_state()==TransactionState::SHRINKING) {
    txn->set_state(TransactionState::ABORTED);
    // 如果在收缩阶段加锁，抛出异常
    throw TransactionAbortException(txn->get_transaction_id(),AbortReason::LOCK_ON_SHRINKING);
  }
  LockDataId lock_data = LockDataId(tab_fd, rid, LockDataType::RECORD); // 获取要加锁的item
  auto lock_request_queue_iter = lock_table_.find(lock_data); // 检查在该lock item上是否已经有队列
  if(lock_request_queue_iter==lock_table_.end()) {
    lock_request_queue_iter = lock_table_.emplace(lock_data,std::make_shared<LockRequestQueue>()).first;
  }
  auto lock_request_queue = lock_request_queue_iter->second;

  return HandleLockRequest(txn,tab_fd,lock_request_queue,LockMode::EXCLUSIVE,LockObject::ROW,&rid);
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {

  std::unique_lock<std::mutex> lock(latch_);
  if(txn->get_state()==TransactionState::SHRINKING) {
    txn->set_state(TransactionState::ABORTED);
    // 如果在收缩阶段加锁，抛出异常
    throw TransactionAbortException(txn->get_transaction_id(),AbortReason::LOCK_ON_SHRINKING);
  }
  LockDataId lock_data = LockDataId(tab_fd,  LockDataType::TABLE); // 获取要加锁的item
  auto lock_request_queue_iter = lock_table_.find(lock_data); // 检查在该lock item上是否已经有队列
  if(lock_request_queue_iter==lock_table_.end()) {
    lock_request_queue_iter = lock_table_.emplace(lock_data,std::make_shared<LockRequestQueue>()).first;
  }
  auto lock_request_queue = lock_request_queue_iter->second;
  // check(AntiO2)
  return HandleLockRequest(txn, tab_fd, lock_request_queue, LockMode::SHARED,LockObject::TABLE);
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
  std::unique_lock<std::mutex> lock(latch_);
  if(txn->get_state()==TransactionState::SHRINKING) {
    txn->set_state(TransactionState::ABORTED);
    // 如果在收缩阶段加锁，抛出异常
    throw TransactionAbortException(txn->get_transaction_id(),AbortReason::LOCK_ON_SHRINKING);
  }
  LockDataId lock_data = LockDataId(tab_fd,  LockDataType::TABLE); // 获取要加锁的item
  auto lock_request_queue_iter = lock_table_.find(lock_data); // 检查在该lock item上是否已经有队列
  if(lock_request_queue_iter==lock_table_.end()) {
    lock_request_queue_iter = lock_table_.emplace(lock_data,std::make_shared<LockRequestQueue>()).first;
  }
  auto lock_request_queue = lock_request_queue_iter->second;
  // check(AntiO2)
  return HandleLockRequest(txn, tab_fd, lock_request_queue, LockMode::EXCLUSIVE,LockObject::TABLE);
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
  std::unique_lock<std::mutex> lock(latch_);
  if(txn->get_state()==TransactionState::SHRINKING) {
    txn->set_state(TransactionState::ABORTED);
    // 如果在收缩阶段加锁，抛出异常
    throw TransactionAbortException(txn->get_transaction_id(),AbortReason::LOCK_ON_SHRINKING);
  }
  LockDataId lock_data = LockDataId(tab_fd,  LockDataType::TABLE); // 获取要加锁的item
  auto lock_request_queue_iter = lock_table_.find(lock_data); // 检查在该lock item上是否已经有队列
  if(lock_request_queue_iter==lock_table_.end()) {
    lock_request_queue_iter = lock_table_.emplace(lock_data,std::make_shared<LockRequestQueue>()).first;
  }
  auto lock_request_queue = lock_request_queue_iter->second;
  // check(AntiO2)
  return HandleLockRequest(txn, tab_fd, lock_request_queue, LockMode::INTENTION_SHARED,LockObject::TABLE);
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
  std::unique_lock<std::mutex> lock(latch_);
  if(txn->get_state()==TransactionState::SHRINKING) {
    txn->set_state(TransactionState::ABORTED);
    // 如果在收缩阶段加锁，抛出异常
    throw TransactionAbortException(txn->get_transaction_id(),AbortReason::LOCK_ON_SHRINKING);
  }
  LockDataId lock_data = LockDataId(tab_fd,  LockDataType::TABLE); // 获取要加锁的item
  auto lock_request_queue_iter = lock_table_.find(lock_data); // 检查在该lock item上是否已经有队列
  if(lock_request_queue_iter==lock_table_.end()) {
    lock_request_queue_iter = lock_table_.emplace(lock_data,std::make_shared<LockRequestQueue>()).first;
  }
  auto lock_request_queue = lock_request_queue_iter->second;
  // check(AntiO2)
  return HandleLockRequest(txn, tab_fd, lock_request_queue, LockMode::INTENTION_EXCLUSIVE,LockObject::TABLE);
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock(latch_);
    auto lock_request_queue_it = lock_table_.find(lock_data_id);
    if (lock_request_queue_it == lock_table_.end()) {
      txn->set_state(TransactionState::ABORTED);
      throw TransactionAbortException(txn->get_transaction_id(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }

    return HandleUnlockRequest(txn, lock_data_id.fd_, lock_request_queue_it->second, lock_data_id.type_==LockDataType::TABLE?LockObject::TABLE:LockObject::ROW,
                               &lock_data_id.rid_);
}
auto LockManager::HandleLockRequest(
    Transaction *txn, int tab_fd,
    const std::shared_ptr<LockRequestQueue> &lock_request_queue,
    LockManager::LockMode lock_mode, LockManager::LockObject lock_object,
    const Rid *rid) -> bool {
  auto txn_id = txn->get_transaction_id();
  lock_request_queue->latch_.lock();
  txn->set_state(TransactionState::GROWING);
  for (auto &request : lock_request_queue->request_queue_) {
    if(request->granted_) {
//      if(request->txn_id_ < txn_id &&!CheckCompatible(request->lock_mode_,lock_mode)) {
//        // 采用wait-die策略
//        txn->set_state(TransactionState::ABORTED);
//        lock_request_queue->latch_.unlock();
//        throw TransactionAbortException(txn->get_transaction_id(),AbortReason::DEADLOCK_PREVENTION);
//      }
        if(request->txn_id_ != txn_id &&!CheckCompatible(request->lock_mode_,lock_mode)) {
          // 采用no-wait策略
          txn->set_state(TransactionState::ABORTED);
          lock_request_queue->latch_.unlock();
          throw TransactionAbortException(txn->get_transaction_id(),AbortReason::DEADLOCK_PREVENTION);
        }
    }
    if (request->txn_id_ == txn_id) {
      // 找到了之前该事务的请求 已加锁或是升级
      if (request->lock_mode_ == lock_mode) {
        // 说明之前已经有了该锁
        lock_request_queue->latch_.unlock();
        return true;
      }
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(),
                                        AbortReason::UPGRADE_CONFLICT);
      }
      if (!CheckUpgrade(request->lock_mode_, lock_mode)) { // 检查是否可以升级
        lock_request_queue->latch_.unlock();
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(),
                                        AbortReason::UPGRADE_CONFLICT);
      }

      //      LOG_DEBUG("Txn: %d Want To Upgrade %s On %d", txn->GetTransactionId(),
      //                LockString(request->lock_mode_, lock_object).c_str(), oid);
      // 如果可以升级
      ModifyLockSet(txn, tab_fd, request->lock_mode_, lock_object,
                    ModifyMode::REMOVE, rid); // 删除事务中之前的锁。

      lock_request_queue->request_queue_.remove(request); // 移除被升级的请求
      auto upgrade_request = std::make_shared<LockRequest>(txn_id,lock_mode);

      // 找到第一个还在等待的请求，将升级请求插到它前面去
      auto waiting_iter =
          std::find_if(lock_request_queue->request_queue_.begin(),
                       lock_request_queue->request_queue_.end(),
                       [](auto lockRequest) { return !lockRequest->granted_; });
      // LOG_DEBUG("Waiting iter find: %d distance: %ld",
      // !waiting_iter==lock_request_queue->request_queue_.end(),std::distance(lock_request_queue->request_queue_.begin(),waiting_iter));
      auto upgrade_request_iter =
          lock_request_queue->request_queue_.insert(waiting_iter,
                                                    upgrade_request); //

      lock_request_queue->upgrading_ = txn_id;
      // 等待唤醒
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_,
                                        std::adopt_lock);
      while (!CheckGrant(upgrade_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->get_state() == TransactionState::ABORTED) {
          lock_request_queue->request_queue_.erase(upgrade_request_iter);
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      // 获得锁
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgrade_request->granted_ = true;
      // 为事务添加锁
      ModifyLockSet(txn, tab_fd, lock_mode, lock_object, ModifyMode::ADD, rid);
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_
            .notify_all(); // 这里的if是个小优化，因为对于X锁，通知了其他线程也没有用，它们是无法满足得到锁的条件的。
      }
      return true;
    }
  }
  // 循环完队列， 未找到相同事务id
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  auto new_request = std::make_shared<LockRequest>(txn_id, lock_mode);  // 创建新

  lock_request_queue->request_queue_.push_back(new_request);
  while (!CheckGrant(new_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->get_state() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(new_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  // 获取锁
  new_request->granted_ = true;
  ModifyLockSet(txn, tab_fd, lock_mode, lock_object, ModifyMode::ADD, rid);
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_
        .notify_all();  // 这里的if是个小优化，因为对于X锁，通知了其他线程也没有用，它们是无法满足得到锁的条件的。
  }
  return true;
}
auto LockManager::HandleUnlockRequest(
        Transaction * txn, int tab_fd,
        const std::shared_ptr<LockRequestQueue> &lock_request_queue,
        LockManager::LockObject lock_object, const Rid *rid)
        ->bool {
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  auto txn_id = txn->get_transaction_id();
  for (auto it = lock_request_queue->request_queue_.begin();
       it != lock_request_queue->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn_id && (*it)->granted_) {
      // 解决引起收缩的情况
      if (txn->get_state() == TransactionState::GROWING) {
        switch (txn->get_isolation_level()) {
        case IsolationLevel::SERIALIZABLE:
          if ((*it)->lock_mode_ == LockMode::EXCLUSIVE ||
              (*it)->lock_mode_ == LockMode::SHARED) {
            txn->set_state(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::REPEATABLE_READ:
          if ((*it)->lock_mode_ == LockMode::EXCLUSIVE ||
              (*it)->lock_mode_ == LockMode::SHARED) {
            txn->set_state(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::READ_COMMITTED:
          if ((*it)->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->set_state(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::READ_UNCOMMITTED:
          if ((*it)->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->set_state(TransactionState::SHRINKING);
          }
          if ((*it)->lock_mode_ == LockMode::SHARED) {
            txn->set_state(TransactionState::ABORTED);
            // assert(false); // uncommitted 不会加s锁
          }
          break;
        }
      }
      ModifyLockSet(txn, tab_fd, (*it)->lock_mode_, lock_object,
                    ModifyMode::REMOVE, rid);
      lock_request_queue->request_queue_.erase(it);
      lock_request_queue->cv_.notify_all();
      return true;
    }
    // 未找到对应的锁
    txn->set_state(TransactionState::ABORTED);
    throw TransactionAbortException(
        txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
}
auto LockManager::CheckLock(Transaction *txn, LockManager::LockMode lock_mode,
                            LockManager::LockObject lock_object) -> void {
  auto txn_id = txn->get_transaction_id();
  // 检查是否在行上加锁
  if (lock_object == LockObject::ROW) {
    if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
        lock_mode == LockMode::S_IX) {
      txn->set_state(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    }
  }
  if (txn->get_isolation_level() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::S_IX) {
      txn->set_state(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->get_state() == TransactionState::SHRINKING) {
      txn->set_state(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    }
    return;
  }
  if (txn->get_isolation_level() == IsolationLevel::REPEATABLE_READ) {
    if (txn->get_state() == TransactionState::SHRINKING) {
      txn->set_state(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    }
    return;
  }
  if (txn->get_isolation_level() == IsolationLevel::READ_COMMITTED) {
    if (txn->get_state() == TransactionState::SHRINKING) {
      if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      }
    }
    return;
  }
  if(txn->get_isolation_level()==IsolationLevel::SERIALIZABLE) {
    if (txn->get_state() == TransactionState::SHRINKING) {
      // 强2PL 不允许在收缩阶段加锁
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    }
  }
}
auto LockManager::CheckTableIntentionLock(Transaction *txn,
                                          const LockManager::LockMode &lockMode,
                                          int tab_fd) -> void {
  auto txn_id = txn->get_transaction_id();
  switch (lockMode) {
  case LockMode::SHARED:
    if (!txn->IsTableIntentionSharedLocked(tab_fd) && !txn->IsTableSharedLocked(tab_fd) &&
        !txn->IsTableExclusiveLocked(tab_fd) && !txn->IsTableIntentionExclusiveLocked(tab_fd) &&
        !txn->IsTableSharedIntentionExclusiveLocked(tab_fd)) {
      txn->set_state(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
    break;
  case LockMode::EXCLUSIVE:
    if (!txn->IsTableExclusiveLocked(tab_fd) && !txn->IsTableIntentionExclusiveLocked(tab_fd) &&
        !txn->IsTableSharedIntentionExclusiveLocked(tab_fd)) {
      txn->set_state(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
    break;
  default:
    break;  // 其他三种情况之前已经抛出。
  }
}
auto LockManager::CheckUpgrade(LockManager::LockMode old_lock,
                               LockManager::LockMode new_lock) -> bool {
  switch (old_lock) {
  case LockMode::SHARED:
    if (new_lock == LockMode::EXCLUSIVE || new_lock == LockMode::S_IX) {
      return true;
    }
    break;
  case LockMode::EXCLUSIVE:
    break;
  case LockMode::INTENTION_SHARED:
    if (new_lock == LockMode::SHARED || new_lock == LockMode::EXCLUSIVE ||
        new_lock == LockMode::INTENTION_EXCLUSIVE || new_lock == LockMode::S_IX) {
      return true;
    }
    break;
  case LockMode::INTENTION_EXCLUSIVE:
    if (new_lock == LockMode::EXCLUSIVE || new_lock == LockMode::S_IX) {
      return true;
    }
    break;
  case LockMode::S_IX:
    if (new_lock == LockMode::EXCLUSIVE) {
      return true;
    }
    break;
  }
  return false;
}
auto LockManager::CheckGrant(
    // TODO no-wait检测
    const std::shared_ptr<LockRequest> &checked_request,
    const std::shared_ptr<LockRequestQueue> &request_queue) -> bool {
  const auto &lock_mode = checked_request->lock_mode_;
  for (const auto &request : request_queue->request_queue_) {
    if (request->granted_) {
      switch (request->lock_mode_) {
      case LockMode::SHARED:
        if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED) {
          break;
        } else {
          return false;
        }
      case LockMode::EXCLUSIVE:
        return false;
      case LockMode::INTENTION_SHARED:
        if (lock_mode == LockMode::EXCLUSIVE) {
          return false;
        } else {
          break;
        }
      case LockMode::INTENTION_EXCLUSIVE:
        if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
          break;
        } else {
          return false;
        }
      case LockMode::S_IX:
        if (lock_mode == LockMode::INTENTION_SHARED) {
          break;
        } else {
          return false;
        }
      }
    } else {
      // 遇到了未授权的请求， 按照先进先出的原则，如果不是当前请求，返回false，否则返回true
      return request.get() == checked_request.get();
      // 如果就是当前请求，可以授权
    }
  }
}
auto LockManager::ModifyLockSet(Transaction *txn, int tab_fd,
                                LockManager::LockMode lock_mode,
                                LockManager::LockObject lock_object,
                                LockManager::ModifyMode modify_mode,
                                const Rid *rid) -> void {
  if (lock_object == LockObject::TABLE) {

    LockDataId lockDataId(tab_fd,LockDataType::TABLE);
    if(modify_mode==ModifyMode::ADD) {
      txn->getLockSet()->emplace(lockDataId);
    } else  {
      txn->getLockSet()->erase(lockDataId);
    }
    switch (lock_mode) {
    case LockMode::SHARED:
      if (modify_mode == ModifyMode::ADD) {
        txn->getSTableLockSet()->emplace(tab_fd);
      }
      if (modify_mode == ModifyMode::REMOVE) {
        txn->getSTableLockSet()->erase(tab_fd);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (modify_mode == ModifyMode::ADD) {
        txn->getXTableLockSet()->emplace(tab_fd);
      }
      if (modify_mode == ModifyMode::REMOVE) {
        txn->getXTableLockSet()->erase(tab_fd);
      }
      break;
    case LockMode::INTENTION_SHARED:
      if (modify_mode == ModifyMode::ADD) {
        txn->getIsTableLockSet()->emplace(tab_fd);
      }
      if (modify_mode == ModifyMode::REMOVE) {
        txn->getIsTableLockSet()->erase(tab_fd);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (modify_mode == ModifyMode::ADD) {
        txn->getIxTableLockSet()->emplace(tab_fd);
      }
      if (modify_mode == ModifyMode::REMOVE) {
        txn->getIxTableLockSet()->erase(tab_fd);
      }
      break;
    case LockMode::S_IX:
      if (modify_mode == ModifyMode::ADD) {
        txn->getSixTableLockSet()->emplace(tab_fd);
      }
      if (modify_mode == ModifyMode::REMOVE) {
        txn->getSixTableLockSet()->erase(tab_fd);
      }
      break;
    }
  }
  if (lock_object == LockObject::ROW) {
    LockDataId lockDataId(tab_fd,*rid,LockDataType::RECORD);
    if(modify_mode==ModifyMode::ADD) {
      txn->getLockSet()->emplace(lockDataId);
    } else  {
      txn->getLockSet()->erase(lockDataId);
    }
    if (lock_mode == LockMode::SHARED) {
      ModifyRowLockSet(txn, txn->getSRowLockSet(), tab_fd, rid, modify_mode);
    }
    if (lock_mode == LockMode::EXCLUSIVE) {
      ModifyRowLockSet(txn, txn->getXRowLockSet(), tab_fd, rid, modify_mode);
    }
  }
}
auto LockManager::ModifyRowLockSet(
    Transaction *txn,
    const std::shared_ptr<
        std::unordered_map<int, std::unordered_set<Rid, RidHash>>>
        &row_lock_set,
    const int tab_fd, const Rid *rid, LockManager::ModifyMode modifyMode)
    -> void {

  auto iter = row_lock_set->find(tab_fd);
  switch (modifyMode) {
  case ModifyMode::ADD:
    if (iter == row_lock_set->end()) {
      // 说明之前没有该队列
      iter = row_lock_set->emplace(tab_fd, std::unordered_set<Rid,RidHash>{}).first;
    }
    iter->second.emplace(*rid);
    break;
  case ModifyMode::REMOVE:
    if (iter == row_lock_set->end()) {
      // 说明之前没有该队列
      // 应该这里有异常
      // LOG_ERROR("尝试删除不正确 行锁");
      txn->set_state(TransactionState::ABORTED);
      throw TransactionAbortException(txn->get_transaction_id(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
    iter->second.erase(*rid);
    break;
  }

}
auto LockManager::CheckCompatible(LockManager::LockMode old_lock,
                                  LockManager::LockMode new_lock) -> bool {
  switch (old_lock) {
  case LockMode::SHARED:
    if (new_lock == LockMode::INTENTION_SHARED ||
        new_lock == LockMode::SHARED) {
      return true;
    } else {
      return false;
    }
  case LockMode::EXCLUSIVE:
    return false;
  case LockMode::INTENTION_SHARED:
    if (new_lock == LockMode::EXCLUSIVE) {
      return false;
    } else {
      return true;
    }
  case LockMode::INTENTION_EXCLUSIVE:
    if (new_lock == LockMode::INTENTION_SHARED ||
        new_lock == LockMode::INTENTION_EXCLUSIVE) {
      return true;
    } else {
      return false;
    }
  case LockMode::S_IX:
    if (new_lock == LockMode::INTENTION_SHARED) {
      return true;
    } else {
      return false;
    }
  }
}