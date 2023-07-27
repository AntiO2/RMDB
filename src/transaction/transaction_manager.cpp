/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    if(txn== nullptr) {
      txn = new Transaction(next_txn_id_++);
    }
    // 3. 把开始事务加入到全局事务表中
    txn_map[txn->get_transaction_id()] = txn;
    BeginLogRecord record(txn->get_transaction_id());
    log_manager->add_log_to_buffer(&record);
    txn->set_prev_lsn(record.prev_lsn_);
    // 4. 返回当前事务指针
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    if(txn== nullptr) {
      return;
    }
    // 1. 如果存在未提交的写操作，提交所有的写操作
    auto context = new Context(lock_manager_, log_manager, txn);
    auto write_set = txn->get_write_set();
    for(auto &write:*write_set) {
        switch (write->GetWriteType()) {
            case WType::INSERT_TUPLE: {
                InsertLogRecord record(txn->get_transaction_id(),write->GetRecord(),write->GetRid(),write->GetTableName(),txn->getPrevLsn());
                log_manager->add_log_to_buffer(&record);
                txn->set_prev_lsn(record.lsn_);
                break;
            }
            case WType::DELETE_TUPLE: {
                DeleteLogRecord record(txn->get_transaction_id(),write->GetRecord(),write->GetRid(),write->GetTableName(),txn->getPrevLsn());
                log_manager->add_log_to_buffer(&record);
                txn->set_prev_lsn(record.lsn_);
                break;
            }

            case WType::UPDATE_TUPLE: {
                auto tab_name = write->GetTableName();
                auto &table =  sm_manager_->fhs_.at(tab_name);
                auto new_rec = table->get_record(write->GetRid(),context);
                UpdateLogRecord record(txn->get_transaction_id(),write->GetRecord(),*new_rec,write->GetRid(),write->GetTableName(),txn->getPrevLsn());
                log_manager->add_log_to_buffer(&record);
                txn->set_prev_lsn(record.lsn_);
                break;
            }
        }
    }
    // 2. 释放所有锁
    ReleaseLocks(txn);
    // 3. 释放事务相关资源，eg.锁集
//    for(auto& write:*txn->get_write_set()) {
//      delete write;
//    }
    txn->get_write_set()->clear();
    txn->get_lock_set()->clear();
    // 4. 把事务日志刷入磁盘中
    auto record = CommitLogRecord(txn->get_transaction_id());
    record.prev_lsn_ = txn->getPrevLsn();
    log_manager->add_log_to_buffer(&record);
    log_manager->flush_log_to_disk();
    txn->set_prev_lsn(record.lsn_);
    // 5. 更新事务状态
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    if (txn == nullptr) {
      return;
    }
    // 1. 回滚所有写操作

    auto write_set = txn->get_write_set();
    auto context = new Context(lock_manager_, log_manager, txn);

    for(auto &write:*write_set) {
        switch (write->GetWriteType()) {
            case WType::INSERT_TUPLE: {
                InsertLogRecord record(txn->get_transaction_id(),write->GetRecord(),write->GetRid(),write->GetTableName(),txn->getPrevLsn());
                log_manager->add_log_to_buffer(&record);
                txn->set_prev_lsn(record.lsn_);
                break;
            }
            case WType::DELETE_TUPLE: {
                DeleteLogRecord record(txn->get_transaction_id(),write->GetRecord(),write->GetRid(),write->GetTableName(),txn->getPrevLsn());
                log_manager->add_log_to_buffer(&record);
                txn->set_prev_lsn(record.lsn_);
                break;
            }

            case WType::UPDATE_TUPLE: {
                auto tab_name = write->GetTableName();
                auto &table =  sm_manager_->fhs_.at(tab_name);
                auto new_rec = table->get_record(write->GetRid(),context);
                UpdateLogRecord record(txn->get_transaction_id(),write->GetRecord(),*new_rec,write->GetRid(),write->GetTableName(),txn->getPrevLsn());
                log_manager->add_log_to_buffer(&record);
                txn->set_prev_lsn(record.lsn_);
                break;
            }
        }
    }

    for(auto r_write_iter = write_set->rbegin();r_write_iter!=write_set->rend();++r_write_iter){//改成倒着读取record内容
        auto write = *r_write_iter;
      auto context = new Context(lock_manager_, log_manager, txn);
      auto tab_name = write->GetTableName();
      auto &table =  sm_manager_->fhs_.at(tab_name);
      switch (write->GetWriteType()) {
      case WType::INSERT_TUPLE: {
        auto rec = table->get_record(write->GetRid(),context); // 获取到插入的记录
        for(const auto& index:sm_manager_->db_.get_table(tab_name).indexes) {
          auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name,index.cols);
          auto &index_handler = sm_manager_->ihs_.at(index_name);
          index_handler->delete_entry(rec->key_from_rec(index.cols)->data,txn); // check 这里是否能将之前的txn传入
        }
        table->delete_record(write->GetRid(),context);
        break;
      }
      case WType::DELETE_TUPLE: {
        auto old_rec = write->GetRecord();
        auto rid = table->insert_record(old_rec.data,context);
        for(const auto& index:sm_manager_->db_.get_table(tab_name).indexes) {
          auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name,index.cols);
          auto &index_handler = sm_manager_->ihs_.at(index_name);
          index_handler->insert_entry(old_rec.key_from_rec(index.cols)->data,rid,txn); // check 这里是否能将之前的txn传入
        }
        break;
      }
      case WType::UPDATE_TUPLE:
        auto old_rec = write->GetRecord();
        auto new_rec = table->get_record(write->GetRid(),context);
        for(const auto& index:sm_manager_->db_.get_table(tab_name).indexes) {
          auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name,index.cols);
          auto &index_handler = sm_manager_->ihs_.at(index_name);
          index_handler->delete_entry(new_rec->key_from_rec(index.cols)->data,txn);
        }
        auto rid = write->GetRid();
        table->update_record(rid, old_rec.data, context);
        for(const auto& index:sm_manager_->db_.get_table(tab_name).indexes) {
          auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name,index.cols);
          auto &index_handler = sm_manager_->ihs_.at(index_name);
          index_handler->insert_entry(old_rec.key_from_rec(index.cols)->data,rid,txn);
        }
        break;
      }
      delete write;
    }
    AbortLogRecord record(txn->get_transaction_id(),txn->get_transaction_id());
    log_manager->add_log_to_buffer(&record);
    txn->set_prev_lsn(record.lsn_);
    write_set->clear();
    // 2. 释放所有锁
    ReleaseLocks(txn);
    // 3. 清空事务相关资源，eg.锁集
    txn->get_index_latch_page_set()->clear();
    txn->get_index_deleted_page_set()->clear();
    // 4. 把事务日志刷入磁盘中
    log_manager->flush_log_to_disk()
    // 5. 更新事务状态
    txn->set_state(TransactionState::ABORTED);
}
auto TransactionManager::ReleaseLocks(Transaction *txn) -> void {
  auto lock_set = txn->getLockSet();
//  for(auto lock:*lock_set) {
//    lock_manager_->unlock(txn,lock); // 这种写法是错的，因为unlock操作会改变lock_set
//  }
  std::vector<LockDataId> lock_list;
    for(auto lock:*lock_set) {
      lock_list.emplace_back(lock);
    }
    for(auto lock:lock_list) {
      lock_manager_->unlock(txn,lock);
    }
    lock_set->clear();
}
