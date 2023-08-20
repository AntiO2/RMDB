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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class InsertExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Value> values_;     // 需要插入的数据
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    Rid rid_;                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;
    std::vector<IxIndexHandle*> index_handlers;
    int len_;
    std::vector<ColMeta> cols_;
   public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
        cols_ = tab_.cols;
        for(auto &index:tab_.indexes) {
            auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_,index.cols);
            auto iter = sm_manager_->ihs_.find(index_name);
            if(iter==sm_manager_->ihs_.end()) {
                auto index_handler = sm_manager_->get_ix_manager()->open_index(index_name);
                iter = sm_manager_->ihs_.emplace(index_name,std::move(index_handler)).first;
            }
            index_handlers.emplace_back(iter->second.get());
        }
        for(auto & col:cols_) {
          len_+=col.len;
        }
    };

    std::unique_ptr<RmRecord> Next() override {
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            if (col.type != val.type) {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }
            val.init_raw(col.len);
            // 将Value数据存入rec中。
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }

        auto undo_next = context_->txn_->get_prev_lsn();
        // Insert into record file
        rid_ = fh_->insert_record(rec.data, context_,&tab_name_);
        // Insert into index
        if(context_->txn_->get_isolation_level()==IsolationLevel::REPEATABLE_READ) {
            context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid_, fh_->GetFd());
        }
        context_->txn_->append_write_record(std::make_unique<WriteRecord>(WType::INSERT_TUPLE,tab_name_,rid_, undo_next));
        for(size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto& index = tab_.indexes[i];
            char* key = new char[index.col_tot_len];
            int offset = 0;
            for(size_t j = 0; j < index.col_num; ++j) {
                memcpy(key + offset, rec.data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }
            try {
                GapLockPoint left_point(key,GapLockPointType::E, offset, index.col_num);

                // lock_gap_on_index(Transaction *txn,GapLockRequest request, int iid, const std::vector<ColMeta> &col_meta,LockMode lock_mode);
                index_handlers.at(i)->insert_entry(key, rid_, context_->txn_);
                if(context_->txn_->get_isolation_level()==IsolationLevel::REPEATABLE_READ) {
                    context_->lock_mgr_->lock_gap_on_index(context_->txn_, GapLockRequest(left_point,context_->txn_->getTxnId()),
                                                           index_handlers.at(i)->getFd(),  index.cols, LockManager::LockMode::EXCLUSIVE);
                }

            } catch(IndexEntryDuplicateError &e) {
                // 第i个索引发生重复key
                // 需要将前i - 1个index回滚
                for(int j = 0;j < i;j++) {
                    index = tab_.indexes[j];
                    key = new char[index.col_tot_len];
                    offset = 0;
                    for(size_t k = 0; k < index.col_num; ++k) {
                        memcpy(key + offset, rec.data + index.cols[k].offset, index.cols[k].len);
                        offset += index.cols[k].len;
                    }
                    index_handlers.at(j)->delete_entry(key,context_->txn_);
                }
                undo_next = context_->txn_->get_prev_lsn();
                fh_->mark_delete_record(rid_,context_, &tab_name_);
                context_->txn_->append_write_record(std::make_unique<WriteRecord>(WType::DELETE_TUPLE,tab_name_,rid_, rec, undo_next));
                throw std::move(e);
            } catch (TransactionAbortException &e) {
                for(int j = 0;j < i;j++) {
                    index = tab_.indexes[j];
                    key = new char[index.col_tot_len];
                    offset = 0;
                    for(size_t k = 0; k < index.col_num; ++k) {
                        memcpy(key + offset, rec.data + index.cols[k].offset, index.cols[k].len);
                        offset += index.cols[k].len;
                    }
                    index_handlers.at(j)->delete_entry(key,context_->txn_);
                }
                undo_next = context_->txn_->get_prev_lsn();
                fh_->mark_delete_record(rid_,context_, &tab_name_);
                context_->txn_->append_write_record(std::make_unique<WriteRecord>(WType::DELETE_TUPLE,tab_name_,rid_, rec, undo_next));
                throw std::move(e);
            }
        }
        return nullptr;
    }
    size_t tupleLen() const override {
      return len_;
    }
    const std::vector<ColMeta> &cols() const override {
      return cols_;
    }
    bool is_end() const override {
      true;
    }
    Rid &rid() override { return rid_; }
};