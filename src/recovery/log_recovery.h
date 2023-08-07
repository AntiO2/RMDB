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

#include <map>
#include <unordered_map>
#include "log_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"

class RedoLogsInPage {
public:
    RedoLogsInPage() { table_file_ = nullptr; }
    RmFileHandle* table_file_;
    std::vector<lsn_t> redo_logs_;   // 在该page上需要redo的操作的lsn
};

class RecoveryManager {
public:
    RecoveryManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, SmManager* sm_manager):
     disk_manager_(disk_manager), log_manager_(disk_manager) {
        buffer_pool_manager_ = buffer_pool_manager;
        sm_manager_ = sm_manager;
        auto context = new Context(lock_manager_, log_manager_, txn);
    }

    void analyze();
    void redo();
    void undo();
    /**
     * 通过lsn获得log （加上偏移量）
     * @param lsn
     * @return
     */
    LogRecord* get_log_by_lsn(lsn_t lsn);
private:
    LogBuffer buffer_;                                              // 读入日志
    DiskManager* disk_manager_;                                     // 用来读写文件
    BufferPoolManager* buffer_pool_manager_;                        // 对页面进行读写
    SmManager* sm_manager_;                                         // 访问数据库元数据
    LogManager log_manager_;
    LockManager lock_manager_;
    // 构建脏页表
    dirty_page_table_t dirty_page_table_;
    active_txn_table_t active_txn_table_;
    std::vector<std::unique_ptr<LogRecord>> logs_; // 在系统重启时的log
    lsn_t log_offset_{0}; // log的偏移量 log_offset = (log idx in logs_) - lsn。也就是说通过lsn+log_offset可以获取在logs_数组中的下标
};