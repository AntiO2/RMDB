/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <cstring>
#include "log_manager.h"

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
lsn_t LogManager::add_log_to_buffer(LogRecord* log_record) {
    latch_.lock();
    // 如果日志缓冲区已满，则将其刷新到磁盘中
    //    if(ARIES_DEBUG_MODE) {
    //        LOG_DEBUG("%s", fmt::format("\nLSN {}\nbuffer size {}\n log_len{}\nappend len {}\n",
    //                              global_lsn_+1,log_buffer_.offset_,log_record->log_tot_len_,log_buffer_.offset_+log_record->log_tot_len_).c_str());
    //    }
    if (log_buffer_.is_full(log_record->log_tot_len_)) {
        LOG_DEBUG("Log Buffer is full, begin flussh");
        latch_.unlock();
        flush_log_to_disk();
        latch_.lock();
    }
    // 获取全局 LSN
    lsn_t lsn = ++global_lsn_;
    // 设置 LogRecord 的 LSN
    log_record->lsn_ = lsn;
    // 序列化 LogRecord 并添加到日志缓冲区
    log_record->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
    log_buffer_.offset_ += log_record->log_tot_len_;
    latch_.unlock();
    return lsn;
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk() {
    std::unique_lock<std::mutex> latch(latch_);
    // 将日志缓冲区中的内容写入磁盘中
    disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);
    // 更新 flushed_lsn_
    flushed_lsn_ = global_lsn_;
    // 重置日志缓冲区
    log_buffer_.offset_ = 0;
    memset(log_buffer_.buffer_, 0, sizeof(log_buffer_.buffer_));
}

bool LogManager::flush_log_buffer() {
    prev_offset_ = current_offset_;
    auto read_size =  disk_manager_->read_log(log_buffer_.buffer_,LOG_BUFFER_SIZE, current_offset_);
    if(read_size <= 0 ) {
        return false;
    }
    log_buffer_.offset_ = read_size;
    current_offset_+=read_size;
    return true;
}

std::list<std::unique_ptr<LogRecord>> LogManager::get_records() {
    current_offset_ = 0;
    std::list<std::unique_ptr<LogRecord>> records;

    while(flush_log_buffer()) {
        int current_offset = 0; // current_offset和log_manager.current_offset_不同，指的是在当前log_buffer中的偏移量
        while (current_offset + OFFSET_LOG_TID < log_buffer_.offset_) { // 需要保证可以获取到长度
            auto len = *(uint32_t*)(log_buffer_.buffer_+current_offset+OFFSET_LOG_TOT_LEN);
            if(current_offset + len > log_buffer_.offset_) {
                // 这条log跨了两页, 要读取该log，需要从current_offset开始，重新读一页
                if(ARIES_DEBUG_MODE) {
                    LOG_DEBUG("Across log appears");
                }
                break;
            }
            auto type = *(LogType*)(log_buffer_.buffer_+ current_offset+ OFFSET_LOG_TYPE);
            switch (type) {
                case UPDATE:
                {
                    UpdateLogRecord record(log_buffer_.buffer_+current_offset);
                    record.format_print();
                    records.emplace_back(std::make_unique<UpdateLogRecord>(record));
                    current_offset += record.log_tot_len_;
                    break;
                }
                case INSERT:
                {
                    InsertLogRecord record(log_buffer_.buffer_+current_offset);
                    records.emplace_back(std::make_unique<InsertLogRecord>(record));
                    record.format_print();
                    current_offset += record.log_tot_len_;
                    break;
                }
                case DELETE:
                {
                    DeleteLogRecord record(log_buffer_.buffer_+current_offset);
                    records.emplace_back(std::make_unique< DeleteLogRecord>(record));
                    record.format_print();
                    current_offset += record.log_tot_len_;
                    break;
                }
                case BEGIN:
                {
                    BeginLogRecord record(log_buffer_.buffer_+current_offset);
                    records.emplace_back(std::make_unique<BeginLogRecord>(record));
                    record.format_print();
                    current_offset += record.log_tot_len_;
                    break;
                }
                case COMMIT:
                {
                    CommitLogRecord record(log_buffer_.buffer_+current_offset);
                    records.emplace_back(std::make_unique<CommitLogRecord>(record));
                    record.format_print();
                    current_offset += record.log_tot_len_;
                    break;
                }
                case ABORT:
                {
                    AbortLogRecord record(log_buffer_.buffer_+current_offset);
                    records.emplace_back(std::make_unique<AbortLogRecord>(record));
                    record.format_print();
                    current_offset += record.log_tot_len_;
                    break;
                }
                case CLR_INSERT: {
                    CLR_Insert_Record record(log_buffer_.buffer_+current_offset);
                    record.format_print();
                    current_offset += record.log_tot_len_;
                    records.emplace_back(std::make_unique<CLR_Insert_Record>(record));
                    break;
                }

                case CLR_DELETE:{
                    CLR_Delete_Record record(log_buffer_.buffer_+current_offset);
                    record.format_print();
                    current_offset += record.log_tot_len_;
                    records.emplace_back(std::make_unique<CLR_Delete_Record>(record));
                    break;
                }
                case CLR_UPDATE:{
                    CLR_UPDATE_RECORD record(log_buffer_.buffer_+current_offset);
                    record.format_print();
                    current_offset += record.log_tot_len_;
                    records.emplace_back(std::make_unique<CLR_UPDATE_RECORD>(record));
                    break;
                }
                case CKPT_BEGIN: {
                    CkptBeginLogRecord record(log_buffer_.buffer_+current_offset);
                    record.format_print();
                    current_offset += record.log_tot_len_;
                    records.emplace_back(std::make_unique<CkptBeginLogRecord>(record));
                    break;
                }
                case CKPT_END:{
                    CkptEndLogRecord record(log_buffer_.buffer_+current_offset);
                    record.format_print();
                    current_offset += record.log_tot_len_;
                    records.emplace_back(std::make_unique<CkptEndLogRecord>(record));
                    break;
                }
            }
        }
        current_offset_ = prev_offset_ + current_offset;
    }

    return records;
}

