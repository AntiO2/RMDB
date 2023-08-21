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
#include <vector>
#include <iostream>
#include <list>
#include "log_defs.h"
#include "common/config.h"
#include "logger.h"
#include "record/rm_defs.h"
#include "storage/page.h"
#include "fmt/format.h"
/* 日志记录对应操作的类型 */
enum LogType: int {
    UPDATE = 0,
    INSERT,
    DELETE,
    BEGIN,
    COMMIT,
    ABORT,
    CLR_INSERT, // insert 补偿记录
    CLR_DELETE,
    CLR_UPDATE,
    CKPT_BEGIN,
    CKPT_END, // fuzzy checkpoint end
    Mark_Delete,
    CLR_MARK_DELETE
};
static std::string LogTypeStr[] = {
    "UPDATE",
    "INSERT",
    "DELETE",
    "BEGIN",
    "COMMIT",
    "ABORT",
    "CLR_INSERT",
    "CLR_DELETE",
    "CLR_UPDATE",
    "CKPT_BEGIN",
    "CKPT_END",
    "Mark Delete",
    "CLR Mark Delete"
};
enum LogOperation {
    REDO,
    UNDO,
};
class LogRecord {
public:
    LogType log_type_;         /* 日志对应操作的类型 */
    lsn_t lsn_;                /* 当前日志的lsn */
    uint32_t log_tot_len_;     /* 整个日志记录的长度 */
    txn_id_t log_tid_;         /* 创建当前日志的事务ID */
    lsn_t prev_lsn_;           /* 事务创建的前一条日志记录的lsn，用于undo */

    // 把日志记录序列化到dest中
    virtual void serialize (char* dest) const {
        memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));
        memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_t));
        memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(uint32_t));
        memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(txn_id_t));
        memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(lsn_t));
    }
    // 从src中反序列化出一条日志记录
    virtual void deserialize(const char* src) {
        log_type_ = *reinterpret_cast<const LogType*>(src);
        lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_LSN);
        log_tot_len_ = *reinterpret_cast<const uint32_t*>(src + OFFSET_LOG_TOT_LEN);
        log_tid_ = *reinterpret_cast<const txn_id_t*>(src + OFFSET_LOG_TID);
        prev_lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_PREV_LSN);
    }
    // used for debug
    virtual void format_print() {
        if(ARIES_DEBUG_MODE) {
            LOG_DEBUG("%s",
                      fmt::format(
                                  "log_type_: {}\n"
                                  "lsn: {}\n"
                                  "log_tid: {}\n"
                                  "prev_lsn: {}\n"
                                  "log_tot_len: {}",LogTypeStr[log_type_],  lsn_,log_tid_,prev_lsn_, log_tot_len_).c_str());
        }

    }
};

class BeginLogRecord: public LogRecord {
public:
    BeginLogRecord() {
        log_type_ = LogType::BEGIN;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    BeginLogRecord(txn_id_t txn_id) : BeginLogRecord() {
        log_tid_ = txn_id;
    }
    BeginLogRecord(char *src) {
        deserialize(src);
    }
    // 序列化Begin日志记录到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    // 从src中反序列化出一条Begin日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);   
    }
    virtual void format_print() override {
        LogRecord::format_print();
    }
};
class CkptBeginLogRecord: public LogRecord {
public:
    CkptBeginLogRecord() {
        log_type_=LogType::CKPT_BEGIN;
        lsn_=INVALID_LSN;
        log_tot_len_=LOG_HEADER_SIZE;
        log_tid_=INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    CkptBeginLogRecord(char *src) {
        deserialize(src);
    }
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
    }
    virtual void format_print() override {
        std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
        LogRecord::format_print();
    }
};

class CkptEndLogRecord: public LogRecord {
public:
    CkptEndLogRecord() {
        log_type_=LogType::CKPT_END;
        lsn_=INVALID_LSN;
        log_tot_len_=LOG_HEADER_SIZE;
        log_tid_=INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    CkptEndLogRecord(char *src) {
        deserialize(src);
    }
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        // Serialize dirty page table
        int offset = LOG_HEADER_SIZE;
        int dpt_size = static_cast<int>(dpt_.size());
        memcpy(dest + offset, &dpt_size, sizeof(int));
        offset += sizeof(int);
        for (const auto& entry : dpt_) {
            memcpy(dest + offset, &(entry.first), sizeof(PageId));
            offset += sizeof(PageId);
            memcpy(dest + offset, &(entry.second), sizeof(lsn_t));
            offset += sizeof(lsn_t);
        }
        // Serialize active transaction table
        int att_size = static_cast<int>(att_.size());
        memcpy(dest + offset, &att_size, sizeof(int));
        offset += sizeof(int);
        for (const auto& entry : att_) {
            memcpy(dest + offset, &(entry.first), sizeof(txn_id_t));
            offset += sizeof(txn_id_t);
            memcpy(dest + offset, &(entry.second), sizeof(lsn_t));
            offset += sizeof(lsn_t);
        }
    }

    void deserialize(const char* src) override {
        LogRecord::deserialize(src);

        // Deserialize dirty page table
        int offset = LOG_HEADER_SIZE;
        int dpt_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        for (int i = 0; i < dpt_size; ++i) {
            PageId page_id = *reinterpret_cast<const PageId*>(src + offset);
            offset += sizeof(PageId);
            lsn_t reclsn = *reinterpret_cast<const lsn_t*>(src + offset);
            offset += sizeof(lsn_t);
            dpt_[page_id] = reclsn;
        }

        // Deserialize active transaction table
        int att_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        for (int i = 0; i < att_size; ++i) {
            txn_id_t txn_id = *reinterpret_cast<const txn_id_t*>(src + offset);
            offset += sizeof(txn_id_t);
            lsn_t lsn = *reinterpret_cast<const lsn_t*>(src + offset);
            offset += sizeof(lsn_t);
            att_[txn_id] = lsn;
        }
    }
    virtual void format_print() override {
        LogRecord::format_print();
    }
    dirty_page_table_t dpt_;
    active_txn_table_t att_;
};
class CommitLogRecord: public LogRecord {
public:
    CommitLogRecord(txn_id_t txn_id, lsn_t prev_lsn) {
        log_type_ = LogType::COMMIT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = txn_id;
        prev_lsn_ = prev_lsn;
    }
    CommitLogRecord(char *src) {
        deserialize(src);
    }
    // 序列化commit日志记录到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    // 从src中反序列化出一条commit日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
    }

    void format_print() override {
        LogRecord::format_print();
    }
};
class AbortLogRecord: public LogRecord {
public:
    AbortLogRecord(txn_id_t txn_id, lsn_t prev_lsn) {
        log_type_ = LogType::ABORT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = txn_id;
        prev_lsn_ = prev_lsn;
    }
    AbortLogRecord(char *src) {
        deserialize(src);
    }
    // 序列化abort日志记录到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    // 从src中反序列化出一条abort日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
    }
    void format_print() override {
        LogRecord::format_print();
    }
};

class InsertLogRecord: public LogRecord {
public:
    InsertLogRecord() {
        log_type_ = LogType::INSERT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
    }
    InsertLogRecord(char *src) {
        deserialize(src);
    }
    InsertLogRecord(txn_id_t txn_id, RmRecord& insert_value, Rid& rid, const std::string& table_name,lsn_t prev_lsn, int  first_free_page_no, int num_page)
        : InsertLogRecord() {

        log_tid_ = txn_id;

        // insert value
        insert_value_ = insert_value;
        log_tot_len_ += insert_value_.size;
        log_tot_len_ +=sizeof(insert_value.size);

        // rid
        rid_ = rid;
        log_tot_len_ += sizeof(Rid);

        // prev_lsn
        prev_lsn_ = prev_lsn;

        // table name
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_;

        first_free_page_no_ = first_free_page_no;
        log_tot_len_+= sizeof(first_free_page_no);
        num_pages_ = num_page;
        log_tot_len_ += sizeof(num_page);
    }
    // 把insert日志记录序列化到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &insert_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, insert_value_.data, insert_value_.size);
        offset += insert_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);

        offset+=table_name_size_;
        memcpy(dest + offset, &first_free_page_no_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &num_pages_, sizeof(int));
        offset += sizeof(int);
    }
    // 从src中反序列化出一条Insert日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);  
        insert_value_.Deserialize(src + OFFSET_LOG_DATA);
        int offset = OFFSET_LOG_DATA + insert_value_.size + sizeof(int);
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);

        offset+=table_name_size_;
        first_free_page_no_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        num_pages_ =  *reinterpret_cast<const int*>(src + offset);
    }
    void format_print() override {
        if(ARIES_DEBUG_MODE) {
            LogRecord::format_print();
            LOG_DEBUG("%s", fmt::format("table name: {}\n"
                                        "insert rid: {} {}\n"
                                        "first_free_page: {}\n"
                                        "num_pages:{}", table_name_,rid_.page_no, rid_.slot_no,first_free_page_no_,
                                        num_pages_).c_str());
        }
    }

    RmRecord insert_value_;     // 插入的记录
    Rid rid_;                   // 记录插入的位置
    char* table_name_;          // 插入记录的表名称
    size_t table_name_size_;    // 表名称的大小

    /**
     * file_hdr的内容
     */
    int first_free_page_no_;
    int num_pages_;
    /**
     * page_hdr的内容
     */
     // int next_page
};


class DeleteLogRecord: public LogRecord {
public:
    DeleteLogRecord() {
        log_type_ = LogType::DELETE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
    }
    DeleteLogRecord(txn_id_t txn_id, RmRecord& delete_value,const Rid& rid, const std::string& table_name, lsn_t prev_lsn, int  first_free_page_no, int num_page)
            : DeleteLogRecord() {
        prev_lsn_ = prev_lsn;
        log_tid_ = txn_id;
        delete_value_ = delete_value;
        rid_ = rid;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += delete_value_.size;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_;

        first_free_page_no_ = first_free_page_no;
        log_tot_len_+= sizeof(first_free_page_no);
        num_pages_ = num_page;
        log_tot_len_ += sizeof(num_page);
    }

    // 把delete日志记录序列化到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &delete_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, delete_value_.data, delete_value_.size);
        offset += delete_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);

        offset+=table_name_size_;
        memcpy(dest + offset, &first_free_page_no_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &num_pages_, sizeof(int));
        offset += sizeof(int);
    }
    // 从src中反序列化出一条Delete日志记录
    DeleteLogRecord(char *src) {
        deserialize(src);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        delete_value_.Deserialize(src + OFFSET_LOG_DATA);
        int offset = OFFSET_LOG_DATA + delete_value_.size + sizeof(int);
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);

        offset+=table_name_size_;
        first_free_page_no_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        num_pages_ =  *reinterpret_cast<const int*>(src + offset);
    }
    void format_print() override {
        if(ARIES_DEBUG_MODE) {
            LogRecord::format_print();
            LOG_DEBUG("%s", fmt::format("table name: {}\n"
                                        "delete rid: {} {}\n"
                                        "first_free_page: {}\n"
                                        "num_pages:{}", table_name_,rid_.page_no, rid_.slot_no,first_free_page_no_,
                                        num_pages_).c_str());
        }
    }

    RmRecord delete_value_;     // 被删除的记录
    Rid rid_;                   // 记录被删除的位置
    char* table_name_;          // 删除记录的表名称
    size_t table_name_size_;    // 表名称的大小

    /**
     * file_hdr的内容
     */
    int first_free_page_no_;
    int num_pages_;
};

class UpdateLogRecord: public LogRecord {
public:
    UpdateLogRecord() {
        log_type_ = LogType::UPDATE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
    }
    UpdateLogRecord(txn_id_t txn_id, RmRecord& before_update_value, RmRecord& after_update_value,const Rid& rid, const std::string& table_name, lsn_t prev_lsn, int  first_free_page_no, int num_page)
            : UpdateLogRecord() {
        log_tid_ = txn_id;
        before_update_value_ = before_update_value;
        after_update_value_ = after_update_value;
        rid_ = rid;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += before_update_value_.size;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += after_update_value_.size;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_;
        prev_lsn_ = prev_lsn;

        first_free_page_no_ = first_free_page_no;
        log_tot_len_+= sizeof(first_free_page_no);
        num_pages_ = num_page;
        log_tot_len_ += sizeof(num_page);
    }
    UpdateLogRecord(char *src) {
        deserialize(src);
    }
    // 序列化update日志记录到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &before_update_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, before_update_value_.data, before_update_value_.size);
        offset += before_update_value_.size;
        memcpy(dest + offset, &after_update_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, after_update_value_.data, after_update_value_.size);
        offset += after_update_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);

        offset+=table_name_size_;
        memcpy(dest + offset, &first_free_page_no_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &num_pages_, sizeof(int));
        offset += sizeof(int);
    }
    // 从src中反序列化出一条update日志记录
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        before_update_value_.Deserialize(src + OFFSET_LOG_DATA);
        int offset = OFFSET_LOG_DATA + before_update_value_.size + sizeof(int);
        after_update_value_.Deserialize(src + offset);
        offset += after_update_value_.size + sizeof(int);
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);

        offset+=table_name_size_;
        first_free_page_no_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        num_pages_ =  *reinterpret_cast<const int*>(src + offset);
    }
    void format_print() override {
        if(ARIES_DEBUG_MODE) {
            LogRecord::format_print();
            LOG_DEBUG("%s", fmt::format("table name: {}\n"
                                        "update rid: {} {}\n"
                                        "first_free_page: {}\n"
                                        "num_pages:{}", table_name_,rid_.page_no, rid_.slot_no,first_free_page_no_,
                                        num_pages_).c_str());
        }
    }

    RmRecord before_update_value_;  // 更新前的记录
    RmRecord after_update_value_;   // 更新后的记录
    Rid rid_;                       // 记录被更新的位置
    char* table_name_;              // 更新记录的表名称
    size_t table_name_size_;        // 表名称的大小

    /**
     * file_hdr的内容
     */
    int first_free_page_no_;
    int num_pages_;
};
class CLR_UPDATE_RECORD : public UpdateLogRecord {
public:
    lsn_t undo_next_;  // LSN of the next undo record

    CLR_UPDATE_RECORD() : UpdateLogRecord() {
        log_type_ = CLR_UPDATE;
        undo_next_ = INVALID_LSN;
    }

    CLR_UPDATE_RECORD(txn_id_t txn_id, RmRecord& before_update_value, RmRecord& after_update_value,const Rid& rid, const std::string& table_name, lsn_t prev_lsn, lsn_t undo_next,  int  first_free_page_no, int num_page)
            : UpdateLogRecord(txn_id, before_update_value, after_update_value, rid, table_name, prev_lsn, first_free_page_no, num_page) {
        log_type_ = CLR_UPDATE;
        undo_next_ = undo_next;
        log_tot_len_+= sizeof(undo_next_);
    }

    CLR_UPDATE_RECORD(char* src) : UpdateLogRecord(src) {
        deserialize(src);
    }

    void serialize(char* dest) const override {
        UpdateLogRecord::serialize(dest);
        int offset = log_tot_len_-sizeof(lsn_t);
        memcpy(dest + offset, &undo_next_, sizeof(lsn_t));
    }

    void deserialize(const char* src) override {
        UpdateLogRecord::deserialize(src);
        int offset = log_tot_len_-sizeof(lsn_t);
        undo_next_ = *reinterpret_cast<const lsn_t*>(src + offset);
    }

    void format_print() override {
        if(ARIES_DEBUG_MODE) {
            UpdateLogRecord::format_print();
            LOG_DEBUG("%s", fmt::format("Undo next{}",undo_next_).c_str());
        }
    }
};
class CLR_Delete_Record : public LogRecord {
public:
    Rid rid_;                   // 记录被删除的位置
    char* table_name_;          // 删除记录的表名称
    size_t table_name_size_;    // 表名称的大小
    lsn_t undo_next_;  // LSN of the next undo record

    /**
     * file_hdr的内容
     */
    int first_free_page_no_;
    int num_pages_;
public:
    CLR_Delete_Record() {
        log_type_ = LogType::CLR_DELETE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
    }
    CLR_Delete_Record(txn_id_t txn_id,const Rid& rid, const std::string& table_name, lsn_t prev_lsn, lsn_t undo_next, int  first_free_page_no, int num_page) {
        log_type_ = LogType::CLR_DELETE;
        lsn_=INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = txn_id;
        prev_lsn_ = prev_lsn;
        // LOG HEAD 部分结束
        rid_ = rid;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_; // table name长度和本体




        first_free_page_no_ = first_free_page_no;
        log_tot_len_+= sizeof(first_free_page_no);
        num_pages_ = num_page;
        log_tot_len_ += sizeof(num_page);

        undo_next_ = undo_next;
        log_tot_len_ += sizeof(lsn_t);
    }

    // 把delete日志记录序列化到dest中
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
        offset+=table_name_size_;
        memcpy(dest + offset, &first_free_page_no_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &num_pages_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest+offset, &undo_next_, sizeof(lsn_t));

    }
    // 从src中反序列化出一条Delete日志记录
    CLR_Delete_Record(char *src) {
        deserialize(src);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
        offset+=table_name_size_;
        first_free_page_no_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        num_pages_ =  *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        undo_next_ = *reinterpret_cast<const lsn_t*>(src+offset);
    }
    void format_print() override {
        if(ARIES_DEBUG_MODE) {
            LogRecord::format_print();
            LOG_DEBUG("%s", fmt::format("table name: {}\n"
                                        "delete rid: {} {}\n"
                                        "first_free_page: {}\n"
                                        "num_pages: {}\n"
                                        "undo_next: {}", table_name_,rid_.page_no, rid_.slot_no,first_free_page_no_,
                                        num_pages_,undo_next_).c_str());
        }
    }

};

class Mark_Delete_Record: public CLR_Delete_Record {
public:
    Mark_Delete_Record(txn_id_t txn_id,const Rid& rid, const std::string& table_name, lsn_t prev_lsn, int  first_free_page_no, int num_page) {
        log_type_ = LogType::Mark_Delete;
        lsn_=INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = txn_id;
        prev_lsn_ = prev_lsn;
        // LOG HEAD 部分结束
        rid_ = rid;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_; // table name长度和本体
        first_free_page_no_ = first_free_page_no;
        log_tot_len_+= sizeof(first_free_page_no);
        num_pages_ = num_page;
        log_tot_len_ += sizeof(num_page);
        log_tot_len_ += sizeof(lsn_t);
    }

    void serialize(char *dest) const override {
        CLR_Delete_Record::serialize(dest);
    }

    void deserialize(const char *src) override {
        CLR_Delete_Record::deserialize(src);
    }

    void format_print() override {
        CLR_Delete_Record::format_print();
    }
};
class CLR_Mark_Delete_Record: public CLR_Delete_Record {
public:
    CLR_Mark_Delete_Record(txn_id_t txn_id,const Rid& rid, const std::string& table_name, lsn_t prev_lsn, lsn_t undo_next, int  first_free_page_no, int num_page)
    : CLR_Delete_Record(txn_id, rid, table_name, prev_lsn, undo_next, first_free_page_no, num_page){
        log_type_ = LogType::CLR_MARK_DELETE;
    }

    void serialize(char *dest) const override {
        CLR_Delete_Record::serialize(dest);
    }

    void deserialize(const char *src) override {
        CLR_Delete_Record::deserialize(src);
    }

    void format_print() override {
        CLR_Delete_Record::format_print();
    }
};
class CLR_Insert_Record : public InsertLogRecord {
public:
    lsn_t undo_next_;  // LSN of the next undo record

    CLR_Insert_Record() : InsertLogRecord() {
        log_type_ = CLR_INSERT;
        undo_next_ = INVALID_LSN;
    }

    CLR_Insert_Record(txn_id_t txn_id, RmRecord& before_update_value, Rid& rid, const std::string& table_name, lsn_t prev_lsn, lsn_t undo_next,  int  first_free_page_no, int num_page)
            : InsertLogRecord(txn_id, before_update_value, rid, table_name, prev_lsn, first_free_page_no, num_page) {
        log_type_ = CLR_INSERT;
        undo_next_ = undo_next;
        log_tot_len_+= sizeof(undo_next_);
    }

    CLR_Insert_Record(char* src) : InsertLogRecord(src) {
        deserialize(src);
    }

    void serialize(char* dest) const override {
        InsertLogRecord::serialize(dest);
        int offset = log_tot_len_-sizeof(lsn_t);
        memcpy(dest + offset, &undo_next_, sizeof(lsn_t));
    }

    void deserialize(const char* src) override {
        InsertLogRecord::deserialize(src);
        int offset = log_tot_len_-sizeof(lsn_t);
        undo_next_ = *reinterpret_cast<const lsn_t*>(src + offset);
    }

    void format_print() override {
        if(ARIES_DEBUG_MODE) {
            InsertLogRecord::format_print();
            LOG_DEBUG("%s", fmt::format("Undo next{}",undo_next_).c_str());
        }
    }
};
/* 日志缓冲区，只有一个buffer，因此需要阻塞地去把日志写入缓冲区中 */

class LogBuffer {
public:
    LogBuffer() { 
        offset_ = 0; 
        memset(buffer_, 0, sizeof(buffer_));
    }

    bool is_full(int append_size) {
        if(offset_ + append_size > LOG_BUFFER_SIZE) {
            return true;
        }
        return false;
    }

    char buffer_[LOG_BUFFER_SIZE+1];
    int offset_;    // 写入log的offset
    // 获取日志缓冲区中的所有日志记录
};

/* 日志管理器，负责把日志写入日志缓冲区，以及把日志缓冲区中的内容写入磁盘中 */
class LogManager {
public:
    LogManager(DiskManager* disk_manager) { disk_manager_ = disk_manager; }
    
    lsn_t add_log_to_buffer(LogRecord* log_record);
    void flush_log_to_disk();
    bool flush_log_buffer();
    LogBuffer* get_log_buffer() {
        return &log_buffer_;
    }
    void set_global_lsn(lsn_t lsn) {
        global_lsn_ = lsn;
    }
    std::list<std::unique_ptr<LogRecord>> get_records();
private:    
    std::atomic<lsn_t> global_lsn_{0};  // 全局lsn，递增，用于为每条记录分发lsn
    std::mutex latch_;                  // 用于对log_buffer_的互斥访问
    LogBuffer log_buffer_;              // 日志缓冲区
    DiskManager* disk_manager_;
    int current_offset_; // 当前在log文件中的偏移量
    int prev_offset_; // 在flush之前的偏移量
public:
    dirty_page_table_t dirty_page_table_;
    active_txn_table_t active_txn_table_;
    lsn_t flushed_lsn_;                 // 记录已经持久化到磁盘中的最后一条日志的日志号
};
