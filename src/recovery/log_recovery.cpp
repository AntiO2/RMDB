/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <queue>
#include "log_recovery.h"
#include "logger.h"
/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze() {
    LOG_DEBUG("ARIES: analyze");
    auto records = log_manager_->get_records();

    auto log_num = records.size();
    if(!log_num) {
        LOG_DEBUG("Log File is Empty");
        return;
    }
    logs_.reserve(log_num);
    while(!records.empty()) {
        logs_.emplace_back(std::move(records.front()));
        records.pop_front();
    }
    assert(logs_.size()==log_num);
    log_offset_ = -logs_.at(0)->lsn_;
    if(sm_manager_->master_record_end_!=INVALID_LSN) {
        auto ckpt_end = dynamic_cast<CkptEndLogRecord *>(get_log_by_lsn(sm_manager_->master_record_end_));
        log_manager_->active_txn_table_ = std::move(ckpt_end->att_); // 初始化att
        log_manager_->dirty_page_table_ = std::move(ckpt_end->dpt_); // 初始化dpt
    }
    auto idx = sm_manager_->master_record_begin_!=INVALID_LSN?sm_manager_->master_record_begin_+log_offset_:0; // 从最近的ckpt开始寻找,或者从第一条记录开始
    auto length = logs_.size();
    for(;idx<length;idx++) {
        // 读取磁盘上的日志文件，并将其存储到缓存中
        auto log_record = &logs_[idx];
        auto txn_id = log_record->get()->log_tid_;
        auto lsn = log_record->get()->lsn_;
        switch (log_record->get()->log_type_) {
            case UPDATE: {
                auto update_record = dynamic_cast<UpdateLogRecord *>(log_record->get());
                std::string table_name(update_record->table_name_, update_record->table_name_size_);
                auto table = sm_manager_->fhs_[table_name].get();
                auto page_id = PageId{.fd = table->GetFd(), .page_no = update_record->rid_.page_no};
                if ( log_manager_->dirty_page_table_.find(page_id) ==  log_manager_->dirty_page_table_.end()) {
                     log_manager_->dirty_page_table_[page_id] = lsn; // 记录第一个使该页面变脏的log (reclsn)
                }
                log_manager_->active_txn_table_[txn_id] = lsn;  // 更新该事务的last lsn
                break;
            }

            case INSERT: {
                auto insert_record = dynamic_cast<InsertLogRecord *>(log_record->get());
                std::string table_name(insert_record->table_name_, insert_record->table_name_size_);
                auto table = sm_manager_->fhs_[table_name].get();
                auto page_id = PageId{.fd=table->GetFd(), .page_no=insert_record->rid_.page_no};
                if ( log_manager_->dirty_page_table_.find(page_id) ==  log_manager_->dirty_page_table_.end()) {
                     log_manager_->dirty_page_table_[page_id] = lsn; // 记录第一个使该页面变脏的log (reclsn)
                }
                log_manager_->active_txn_table_[txn_id] = lsn;  // 更新该事务的last lsn
                break;
            }
            case DELETE: {
                auto delete_record = dynamic_cast<DeleteLogRecord *>(log_record->get());
                std::string table_name(delete_record->table_name_, delete_record->table_name_size_);
                auto table = sm_manager_->fhs_[table_name].get();
                auto page_id = PageId{.fd=table->GetFd(), .page_no=delete_record->rid_.page_no};
                if ( log_manager_->dirty_page_table_.find(page_id) ==  log_manager_->dirty_page_table_.end()) {
                     log_manager_->dirty_page_table_[page_id] = lsn; // 记录第一个使该页面变脏的log (reclsn)
                }
                log_manager_->active_txn_table_[txn_id] = lsn;  // 更新该事务的last lsn
                break;
            }
            case BEGIN: {
                log_manager_->active_txn_table_[txn_id] = lsn;  // 更新该事务的last lsn
                break;
            }
            case COMMIT: {
                log_manager_->active_txn_table_.erase(txn_id);
                break;
            }
            case ABORT: {
                log_manager_->active_txn_table_.erase(txn_id);
                break;
            }
            case CLR_INSERT: {
                auto insert_record = dynamic_cast<CLR_Insert_Record *>(log_record->get());
                std::string table_name(insert_record->table_name_, insert_record->table_name_size_);
                auto table = sm_manager_->fhs_[table_name].get();
                auto page_id = PageId{.fd=table->GetFd(), .page_no=insert_record->rid_.page_no};
                if ( log_manager_->dirty_page_table_.find(page_id) ==  log_manager_->dirty_page_table_.end()) {
                     log_manager_->dirty_page_table_[page_id] = lsn; // 记录第一个使该页面变脏的log (reclsn)
                }
                log_manager_->active_txn_table_[txn_id] = lsn;  // 更新该事务的last lsn
                break;
            }
            case CLR_DELETE: {
                auto delete_record = dynamic_cast<CLR_Delete_Record *>(log_record->get());
                std::string table_name(delete_record->table_name_, delete_record->table_name_size_);
                auto table = sm_manager_->fhs_[table_name].get();
                auto page_id = PageId{.fd=table->GetFd(), .page_no=delete_record->rid_.page_no};
                if ( log_manager_->dirty_page_table_.find(page_id) ==  log_manager_->dirty_page_table_.end()) {
                     log_manager_->dirty_page_table_[page_id] = lsn; // 记录第一个使该页面变脏的log (reclsn)
                }
                log_manager_->active_txn_table_[txn_id] = lsn;  // 更新该事务的last lsn
                break;
            }
            case CLR_UPDATE: {
                auto update_record = dynamic_cast<CLR_UPDATE_RECORD *>(log_record->get());
                std::string table_name(update_record->table_name_, update_record->table_name_size_);
                auto table = sm_manager_->fhs_[table_name].get();
                auto page_id = PageId{.fd = table->GetFd(), .page_no = update_record->rid_.page_no};
                if ( log_manager_->dirty_page_table_.find(page_id) ==  log_manager_->dirty_page_table_.end()) {
                     log_manager_->dirty_page_table_[page_id] = lsn; // 记录第一个使该页面变脏的log (reclsn)
                }
                log_manager_->active_txn_table_[txn_id] = lsn;  // 更新该事务的last lsn
                break;
            }
            case CKPT_BEGIN: {
                // 无事发生
                break;
            }
            case CKPT_END: {
                break;
            }
        }
    }
    log_manager_->set_global_lsn(idx-log_offset_); // 设置global lsn
}

/**

@description: 重做所有未落盘的操作
*/
void RecoveryManager::redo() {
    LOG_DEBUG("ARIES: redo");
    if(log_manager_->dirty_page_table_.empty()) {
        return;
    }
    auto dpt_iter =  log_manager_->dirty_page_table_.begin();
    lsn_t redo_lsn = dpt_iter->second;
    dpt_iter++;
    while (dpt_iter!= log_manager_->dirty_page_table_.end()) {
        // 找到redo的起点(最小的rec_lsn)
        redo_lsn = std::min(redo_lsn,dpt_iter->second);
        dpt_iter++;
    }
    LogRecord* log;
    while((log = get_log_by_lsn(redo_lsn))!= nullptr) {
        auto lsn = log->lsn_;
        switch (log->log_type_) {
            case UPDATE: {
                auto update_log = dynamic_cast<UpdateLogRecord*>(log);
                std::string table_name(update_log->table_name_, update_log->table_name_size_);
                auto table = sm_manager_->fhs_[table_name].get();
                auto page_id = PageId{.fd = table->GetFd(), .page_no = update_log->rid_.page_no};
                if ( log_manager_->dirty_page_table_.find(page_id) ==  log_manager_->dirty_page_table_.end()) {
                    // 如果不在脏页表中，不需要重做
                    break;
                }
                auto page = buffer_pool_manager_->fetch_page(page_id);
                if(page->get_page_lsn() >= lsn) {
                    // 如果已经被持久化，不需要更新
                    buffer_pool_manager_->unpin_page(page_id, false);
                    break;
                }
                auto fh = sm_manager_->fhs_.at(table_name).get();
                fh->update_record_recover(update_log->rid_,update_log->after_update_value_.data,lsn,update_log->first_free_page_no_, update_log->num_pages_);
                buffer_pool_manager_->unpin_page(page_id, true);
                break;
            }
            case INSERT: {
                    auto insert_log = dynamic_cast<InsertLogRecord*>(log);
                    std::string table_name(insert_log->table_name_, insert_log->table_name_size_);
                    auto table = sm_manager_->fhs_[table_name].get();
                    auto page_id = PageId{.fd = table->GetFd(), .page_no = insert_log->rid_.page_no};
                    if ( log_manager_->dirty_page_table_.find(page_id) ==  log_manager_->dirty_page_table_.end()) {
                        // 如果不在脏页表中，不需要重做
                        break;
                    }
                    auto page = buffer_pool_manager_->fetch_page(page_id);
                    if(page->get_page_lsn() >= lsn) {
                        // 如果已经被持久化，不需要更新
                        buffer_pool_manager_->unpin_page(page_id, false);
                        break;
                    }
                    auto fh = sm_manager_->fhs_.at(table_name).get();
                    fh->insert_record_recover(insert_log->rid_, insert_log->insert_value_.data, lsn, insert_log->first_free_page_no_, insert_log->num_pages_);
                    buffer_pool_manager_->unpin_page(page_id, true);
                    break;
            }
            case DELETE: {
                auto delete_log = dynamic_cast<DeleteLogRecord*>(log);
                std::string table_name(delete_log->table_name_, delete_log->table_name_size_);
                auto table = sm_manager_->fhs_[table_name].get();
                auto page_id = PageId{.fd = table->GetFd(), .page_no = delete_log->rid_.page_no};
                if ( log_manager_->dirty_page_table_.find(page_id) ==  log_manager_->dirty_page_table_.end()) {
                    // 如果不在脏页表中，不需要重做
                    break;
                }
                auto page = buffer_pool_manager_->fetch_page(page_id);
                if(page->get_page_lsn() >= lsn) {
                    // 如果已经被持久化，不需要更新
                    buffer_pool_manager_->unpin_page(page_id, false);
                    break;
                }
                auto fh = sm_manager_->fhs_.at(table_name).get();
                fh->delete_record_recover(delete_log->rid_,lsn,delete_log->first_free_page_no_,delete_log->num_pages_);
                buffer_pool_manager_->unpin_page(page_id, true);
                break;
            }
            case CLR_INSERT:{
                auto insert_log = dynamic_cast<CLR_Insert_Record*>(log);
                std::string table_name(insert_log->table_name_, insert_log->table_name_size_);
                auto table = sm_manager_->fhs_[table_name].get();
                auto page_id = PageId{.fd = table->GetFd(), .page_no = insert_log->rid_.page_no};
                if ( log_manager_->dirty_page_table_.find(page_id) ==  log_manager_->dirty_page_table_.end()) {
                    // 如果不在脏页表中，不需要重做
                    break;
                }
                auto page = buffer_pool_manager_->fetch_page(page_id);
                if(page->get_page_lsn() >= lsn) {
                    // 如果已经被持久化，不需要更新
                    buffer_pool_manager_->unpin_page(page_id, false);
                    break;
                }
                auto fh = sm_manager_->fhs_.at(table_name).get();
                fh->insert_record_recover(insert_log->rid_, insert_log->insert_value_.data,lsn,insert_log->first_free_page_no_, insert_log->num_pages_);
                buffer_pool_manager_->unpin_page(page_id, true);
                break;
            }
            case CLR_DELETE:  {
                auto delete_log = dynamic_cast<CLR_Delete_Record*>(log);
                std::string table_name(delete_log->table_name_, delete_log->table_name_size_);
                auto table = sm_manager_->fhs_[table_name].get();
                auto page_id = PageId{.fd = table->GetFd(), .page_no = delete_log->rid_.page_no};
                if ( log_manager_->dirty_page_table_.find(page_id) ==  log_manager_->dirty_page_table_.end()) {
                    // 如果不在脏页表中，不需要重做
                    break;
                }
                auto page = buffer_pool_manager_->fetch_page(page_id);
                if(page->get_page_lsn() >= lsn) {
                    // 如果已经被持久化，不需要更新
                    buffer_pool_manager_->unpin_page(page_id, false);
                    break;
                }
                auto fh = sm_manager_->fhs_.at(table_name).get();
                fh->delete_record_recover(delete_log->rid_,lsn,delete_log->first_free_page_no_,delete_log->num_pages_);
                buffer_pool_manager_->unpin_page(page_id, true);
                break;
            }

            case CLR_UPDATE: {
                auto update_log = dynamic_cast<CLR_UPDATE_RECORD*>(log);
                std::string table_name(update_log->table_name_, update_log->table_name_size_);
                auto table = sm_manager_->fhs_[table_name].get();
                auto page_id = PageId{.fd = table->GetFd(), .page_no = update_log->rid_.page_no};
                if ( log_manager_->dirty_page_table_.find(page_id) ==  log_manager_->dirty_page_table_.end()) {
                    // 如果不在脏页表中，不需要重做
                    break;
                }
                auto page = buffer_pool_manager_->fetch_page(page_id);
                if(page->get_page_lsn() >= lsn) {
                    // 如果已经被持久化，不需要更新
                    buffer_pool_manager_->unpin_page(page_id, false);
                    break;
                }
                auto fh = sm_manager_->fhs_.at(table_name).get();
                fh->update_record_recover(update_log->rid_,update_log->after_update_value_.data,lsn,update_log->first_free_page_no_, update_log->num_pages_);
                buffer_pool_manager_->unpin_page(page_id, true);
                break;
            }
//            case CKPT_BEGIN:
//                break;
//            case CKPT_END:
//                break;
//            case BEGIN:
//                break;
//            case COMMIT:
//                break;
//            case ABORT:
//                break;
            default:
                break;
        }
        redo_lsn++;
    }
    log_manager_->set_global_lsn(redo_lsn);
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    LOG_DEBUG("ARIES: UNDO");
    auto undo_cmp=[](const std::pair<lsn_t,Context*>&at1, const std::pair<lsn_t,Context*>&at2) {
        return at1.first < at2.first;
    };
    std::priority_queue< std::pair<lsn_t,Context*> ,
            std::vector<std::pair<lsn_t,Context*>> ,
    decltype(undo_cmp)> undo_list(undo_cmp);
    auto lock_mgr = std::make_unique<LockManager>();
    for(auto undo_txn:log_manager_->active_txn_table_) {
        auto txn = new Transaction(undo_txn.first);
        txn->set_prev_lsn(undo_txn.second);
        auto context = new Context(lock_mgr.get(), log_manager_,txn);
        undo_list.emplace(undo_txn.second, context);
    }
    while(!undo_list.empty()) {
        auto undo_iter = undo_list.top();
        auto undo_lsn = undo_iter.first;
        auto context = undo_iter.second;
        auto log = get_log_by_lsn(undo_lsn);
        if(log== nullptr) {
            LOG_ERROR("undo lsn is invalid");
            undo_list.pop();
            continue;
        }
        switch (log->log_type_) {
                case UPDATE: {
                    auto update_log = dynamic_cast<UpdateLogRecord *>(log);
                    std::string table_name(update_log->table_name_, update_log->table_name_size_);
                    auto table = sm_manager_->fhs_[table_name].get();
                    auto page_id = PageId{.fd = table->GetFd(), .page_no = update_log->rid_.page_no};
                    auto fh = sm_manager_->fhs_.at(table_name).get();
                    fh->update_record(update_log->rid_,update_log->before_update_value_.data, context,&table_name, LogOperation::UNDO, update_log->prev_lsn_);
                    undo_list.pop();
                    undo_list.emplace(log->prev_lsn_,context);
                    break;
                }
                case INSERT: {
                    auto insert_log = dynamic_cast<InsertLogRecord *>(log);
                    std::string table_name(insert_log->table_name_, insert_log->table_name_size_);
                    auto table = sm_manager_->fhs_[table_name].get();
                    auto fh = sm_manager_->fhs_.at(table_name).get();
                    fh->delete_record(insert_log->rid_,context,&table_name, LogOperation::UNDO,insert_log->prev_lsn_);
                    undo_list.pop();
                    undo_list.emplace(log->prev_lsn_,context);
                    break;
                }
                case DELETE: {
                    auto delete_log = dynamic_cast<DeleteLogRecord*>(log);
                    std::string table_name(delete_log->table_name_, delete_log->table_name_size_);
                    auto table = sm_manager_->fhs_[table_name].get();
                    auto fh = sm_manager_->fhs_.at(table_name).get();
                    fh->insert_record(delete_log->delete_value_.data, context, &table_name, LogOperation::UNDO, delete_log->prev_lsn_);
                    undo_list.pop();
                    undo_list.emplace(log->prev_lsn_,context);
                    break;
                }

                case CLR_INSERT: {
                    auto clr_log = dynamic_cast<CLR_Insert_Record*>(log);
                    undo_list.pop();
                    undo_list.emplace( clr_log->undo_next_, context);
                    break;
                }

                case CLR_DELETE: {
                    auto clr_log = dynamic_cast<CLR_Delete_Record*>(log);
                    undo_list.pop();
                    undo_list.emplace( clr_log->undo_next_, context);
                    break;
                }
                case CLR_UPDATE: {
                    auto clr_log = dynamic_cast<CLR_Insert_Record*>(log);
                    undo_list.pop();
                    undo_list.emplace( clr_log->undo_next_, context);
                    break;
                }
                case BEGIN:{
                    delete context;
                    undo_list.pop();
                    break;
                }
                case COMMIT: {
                    LOG_ERROR("Rollback Committed Txn");
                    undo_iter.first = log->prev_lsn_;
                    break;

                }
                case ABORT: {
                    LOG_ERROR("Rollback Aborted Txn");
                    undo_iter.first = log->prev_lsn_;
                    break;
                }
                default: {
                    LOG_ERROR("Error log type while undoing");
                }
            }
    }
    assert(undo_list.empty());
}

LogRecord *RecoveryManager::get_log_by_lsn(lsn_t lsn) {
    auto idx = lsn + log_offset_;
    if(idx >= logs_.size()||idx < 0) {
        return nullptr;
    }
    return logs_.at(idx).get();
}

void RecoveryManager::rebuild() {
    if(!INDEX_REBUILD_MODE) {
        return;
    }
    auto txn = std::make_unique<Transaction>(INVALID_TXN_ID);
    auto lock_mgr = std::make_unique<LockManager>();
    auto context = new Context(lock_mgr.get(),log_manager_, txn.get());
    for(auto &table_iter:sm_manager_->fhs_) {
        auto tab = sm_manager_->db_.get_table(table_iter.first);
        for(auto &index:tab.indexes) {
            sm_manager_->rebuild_index(table_iter.first, index,context);
        }
    }
    delete context;
}
