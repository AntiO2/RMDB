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
      if(context_->txn_->get_isolation_level()==IsolationLevel::SERIALIZABLE) {
        context_->lock_mgr_->lock_exclusive_on_table(context_->txn_,fh_->GetFd());
      }
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
        // Insert into index
        for(size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto& index = tab_.indexes[i];
            char* key = new char[index.col_tot_len];
            int offset = 0;
            for(size_t j = 0; j < index.col_num; ++j) {
                memcpy(key + offset, rec.data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }
            try {
                index_handlers.at(i)->insert_entry(key, rid_, context_->txn_);
            } catch(IndexEntryDuplicateError &e) {
                // 需要将前i - 1个index回滚
                for(int j = 0;j < i;j++) {
                    index_handlers.at(i)->delete_entry(key,context_->txn_);
                }
                throw std::move(e);
            }
        }
        // Insert into record file
        rid_ = fh_->insert_record(rec.data, context_,&tab_name_);
        auto* writeRecord = new WriteRecord(WType::INSERT_TUPLE,tab_name_,rid_);
        context_->txn_->append_write_record(writeRecord);
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