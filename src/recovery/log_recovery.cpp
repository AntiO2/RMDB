/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_recovery.h"

/**

@description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
*/
void RecoveryManager::analyze() {
// 读取磁盘上的日志文件，并将其存储到缓存中
    auto buffer = log_manager_.get_log_buffer();

// 构建脏页表
    std::unordered_map<PageId , RedoLogsInPage> dirty_page_table;
    auto records = buffer->GetRecords();
    for (auto& log_record : records) {
        if (log_record->log_type_ == LogType::UPDATE) {
            auto update_record = dynamic_cast<UpdateLogRecord*>(log_record.get());
            auto lsn = log_record->lsn_;
            std::string table_name(update_record->table_name_,update_record->table_name_size_);
            auto table = sm_manager_->fhs_[table_name].get();
            auto page_id = PageId{.fd = table->GetFd(),.page_no = update_record->rid_.page_no};
            if (dirty_page_table.find(page_id) == dirty_page_table.end()) {
                dirty_page_table[page_id] = RedoLogsInPage();

                dirty_page_table[page_id].table_file_ = ;
            }
            dirty_page_table[page_id].redo_logs_.push_back(lsn);
        }
        if (log_record->log_type_ == LogType::INSERT) {
            auto insert_record = dynamic_cast<InsertLogRecord*>(log_record.get());
            auto lsn = log_record->lsn_;
            std::string table_name(insert_record->table_name_,insert_record->table_name_size_);
            auto table = sm_manager_->fhs_[table_name].get();
            auto page_id = PageId{.fd=table->GetFd(),.page_no=insert_record->rid_.page_no};
            if (dirty_page_table.find(page_id) == dirty_page_table.end()) {
                dirty_page_table[page_id] = RedoLogsInPage();
            }
            dirty_page_table[page_id].redo_logs_.push_back(lsn);
        }
        if (log_record->log_type_ == LogType::DELETE) {
            auto delete_record = dynamic_cast<DeleteLogRecord*>(log_record.get());
            auto lsn = log_record->lsn_;
            std::string table_name(delete_record->table_name_, delete_record->table_name_size_);
            auto table = sm_manager_->fhs_[table_name].get();
            auto page_id = PageId{.fd=table->GetFd(),.page_no=delete_record->rid_.page_no};
            if (dirty_page_table.find(page_id) == dirty_page_table.end()) {
                dirty_page_table[page_id] = RedoLogsInPage();
                dirty_page_table[page_id].table_file_ = sm_manager_->fhs_[table_name].get();
            }
            dirty_page_table[page_id].redo_logs_.push_back(lsn);
        }
    }

// 将脏页表加入缓存中

// 构建未完成的事务列表
// 将未完成的事务列表加入缓存中

}

/**

@description: 重做所有未落盘的操作
*/
void RecoveryManager::redo() {

}
/**

@description: 回滚未完成的事务
*/
void RecoveryManager::undo() {

}