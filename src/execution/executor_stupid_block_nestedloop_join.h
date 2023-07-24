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
/**
 * 使用单个left buffer连接，测试正确性
 *
 */
class StupidBlockNestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段
    // JoinType join_type_{JoinType::INNER_JOIN}; check(AntiO2) 是否需要实现不同类型的JoinType
    std::vector<Condition> fed_conds_;          // join条件
    std::vector<std::pair<ColMeta, ColMeta> > join_cols_;
    bool is_end_;


    size_t left_len_;
    size_t right_len_;
    BufferPoolManager* bpm_;

    Page* right_buffer_page_; // 当前的innerpage tuple缓存的页。
    PageId right_page_id_;
    std::vector<Page*> left_buffer_pages_; // outer_page的页。
    int left_buffer_page_cnt_{0}; // left_page中有多少页有效？

    int left_buffer_page_iter_; // 指示当前在查找哪个 缓冲池中的left page
    int left_buffer_page_inner_iter_; // 指示当前在查找 缓冲池中的left page中的slot编号
    int right_buffer_page_iter_; // 当前在查找right_page中的哪个位置。

    int left_num_per_page_; // // 每页最多可以存放多少个左侧记录。
    int right_num_per_page_; // // 每页最多可以存放多少个右侧记录。


    std::unordered_map<PageId, int> left_num_now_; // buffer中，page含有的left_num数量
    int left_num_now_inner_; // 当前查找的buffer page中，含有的tuple数量
    int right_num_now_; // buffer中有多少个右侧记录

    bool left_over{false}; // 左侧记录是否已经全部进入过buffer_pool_size
    bool right_over{false}; // 右侧记录是否已经遍历完？ 注意，right_over不一定等于right_.is_end_!

    RmRecord left_record_; // 暂存
    RmRecord right_record_;
    std::unique_ptr<RmRecord> emit_record_;

   public:
    StupidBlockNestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                            std::vector<Condition> conds, BufferPoolManager* bpm) {
        bpm_ = bpm;
        left_ = std::move(left);
        right_ = std::move(right);

        left_len_ = left_->tupleLen();
        left_num_per_page_ = PAGE_SIZE/left_len_;
        left_record_=RmRecord(left_len_);

        right_len_ = right_->tupleLen();
        right_num_per_page_ = PAGE_SIZE/right_len_;
        right_record_=RmRecord(right_len_);

        len_ = left_len_ + right_len_;

        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        is_end_ = false;
        fed_conds_ = std::move(conds);
        for(auto const&cond:fed_conds_) {
            assert(!cond.is_rhs_val); // 需要右值不是常数
            auto left_join_col = *get_col(cols_,cond.lhs_col);
            auto right_join_col = *get_col(cols_,cond.rhs_col);
            if(left_join_col.type!=right_join_col.type) {
                throw IncompatibleTypeError(coltype2str(left_join_col.type),
                                            coltype2str(right_join_col.type));
            }
            if(left_join_col.offset>=left_len_) {
                left_join_col.offset = left_join_col.offset- left_len_;
            }
            if(right_join_col.offset>=left_len_) {
                right_join_col.offset = right_join_col.offset- left_len_;
            }
            join_cols_.emplace_back(left_join_col, right_join_col);
        }


    }

    void beginTuple() override {
        init_right_page();
        fill_right_page();
        init_left_page();
        left_buffer_page_iter_ = 0;
        left_buffer_page_inner_iter_ = 0;
        right_buffer_page_iter_ = 0;
        nextTuple();
    }

    void nextTuple() override {
        // for(left buffer)
        //      for(right buffer in whole right)
        //          for(page in left buffer)
        //              for(tuple in left buffer)
        //                  for(tuple in right buffer)
        while(!left_over) {
            while(!right_over) {
                while(left_buffer_page_iter_ < left_buffer_page_cnt_) {
                    auto left_page = left_buffer_pages_[left_buffer_page_iter_];
                    left_num_now_inner_ = left_num_now_.find(left_page->get_page_id())->second;
                    while(left_buffer_page_inner_iter_ < left_num_now_inner_) {
                        memcpy(left_record_.data, left_page->get_data() + left_buffer_page_inner_iter_*left_len_,
                               left_len_);

                        while(right_buffer_page_iter_ < right_num_now_) {
                            memcpy(right_record_.data, right_buffer_page_->get_data() + right_buffer_page_iter_*right_len_,
                                   right_len_);
                            right_buffer_page_iter_++;
                            if(CheckConditions()) {
                                RmRecord rm(len_);
                                memcpy(rm.data, left_record_.data,left_len_);
                                memcpy(rm.data+left_len_, right_record_.data,right_len_);
                                emit_record_ = std::make_unique<RmRecord>(rm);
                                return;
                            }
                        }
                        right_buffer_page_iter_ = 0;
                        left_buffer_page_inner_iter_++;
                    }
                    left_buffer_page_iter_++;
                    left_buffer_page_inner_iter_ = 0;
                }
                // 此时对于已在缓存中的right_tuple,已经比较完了。
                if(!right_->is_end()) {
                    fill_right_page();
                } else {
                    right_over = true;
                }
                left_buffer_page_iter_ = 0;
            }
            if(left_->is_end()) {
                // left已经刷完了.
                left_over = true;
                break;
            }
            // flush left
            int left_buffer_new_page_cnt_ = 0;
            while(!left_->is_end()) {
                if(left_buffer_new_page_cnt_>=left_buffer_page_cnt_) {
                    // 说明join buffer又填充满了
                    break;
                }
                // 创建新的一页
                auto left_buffer_page = left_buffer_pages_.at(left_buffer_new_page_cnt_);
                fill_left_page(left_buffer_page);
                left_buffer_new_page_cnt_++;
            }
            fill_right_page();
            left_buffer_page_cnt_ = left_buffer_new_page_cnt_;
            right_over = false;
        }

        is_end_ = true;

        for(auto left_page:left_buffer_pages_) {
            bpm_->unpin_tmp_page(left_page->get_page_id());
        }
        bpm_->unpin_tmp_page(right_page_id_);
        // 释放资源
    }
    std::unique_ptr<RmRecord> Next() override {
        if(is_end_) {
            return nullptr;
        }
        return std::move(emit_record_);
    }

    Rid &rid() override { return _abstract_rid; }
    void init_right_page() {
        right_page_id_.fd=TMP_FD;
        right_buffer_page_ = bpm_->new_tmp_page(&right_page_id_); // 为右侧 缓冲池。
        if(right_buffer_page_== nullptr) {
            throw RunOutMemError();
        }
        right_->beginTuple();
    }

    /**
     * 尝试填充inner page
     * @return 指示右侧表是否已经读完
     */
    bool fill_right_page() {
        if(right_over) {
            // 之前已经走完过了一次right_,重新开始
            right_->beginTuple();
        }
        memset(right_buffer_page_->get_data(),0,PAGE_SIZE);
        right_num_now_ = 0;
        while(!right_->is_end()&&right_num_now_<right_num_per_page_) {
            memcpy(right_buffer_page_->get_data()+right_num_now_*right_len_,right_->Next()->data,right_len_);
            right_num_now_++;
            right_->nextTuple();
        }
        return right_->is_end();
    }
     /**
      * 初始化左侧表
      * @return 指示左侧表是否已经读完
      */
    void init_left_page() {
        left_->beginTuple();
        while(!left_->is_end()) {
          // if(bpm_->get_free_size() <= 35) {
             if(left_buffer_pages_.size()>=100) { // 在测试时，可以只用两个buffer page
                // 已经缓存了足够数量的左侧tuple
                // 这个35是我随便写的数字，最后给bpm 留个几页防止出什么问题。
                // 比如 如果不小心调用到了index scan，给b+树的页留个几页。
                // 具体怎么解决还留待查资料
                break;
            }

            // 创建新的一页
            PageId left_page_id{.fd=TMP_FD,.page_no=INVALID_PAGE_ID};
            auto left_buffer_page = bpm_->new_tmp_page(&left_page_id);
            if(left_buffer_page== nullptr) {
                throw RunOutMemError();
            }
            left_buffer_pages_.emplace_back(left_buffer_page);

            fill_left_page(left_buffer_page);
        }

        left_buffer_page_cnt_ = left_buffer_pages_.size();
    }
    bool fill_left_page(Page* page) {
        int record_cnt = 0;// 当前页存放的page数
        while(!left_->is_end()&&record_cnt < left_num_per_page_) {
            memcpy(page->get_data()+record_cnt*left_len_,left_->Next()->data,left_len_);
            record_cnt++;
            left_->nextTuple();
        }
        left_num_now_[page->get_page_id()] = record_cnt;
        return left_->is_end();
    }

    size_t tupleLen() const override {
        return len_;
    }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    std::string getType() override {
        return "Block NestedLoop Join Executor";
    }

    bool is_end() const override {
        return is_end_;
    }

    ColMeta get_col_offset(const TabCol &target) override {
        return AbstractExecutor::get_col_offset(target);
    }
    bool CheckConditions() {
        /**
         * 检查所有条件
         */
        bool result = true;
        auto join_size = fed_conds_.size();
        for(decltype(join_size) i = 0; i < join_size; i++) {
            result&= Check_ith_Condition(i); // Check(AntiO2) 此处bool运算是否正确
        }
        return result;

    }
    /**
     * @description 检查第i个条件是否成立
     * @param i
     * @return
     */
    bool Check_ith_Condition(const size_t i) {
        const auto &left_col = join_cols_.at(i).first;
        const auto &right_col = join_cols_.at(i).second;
        char* l_value = left_record_.data+left_col.offset;
        char* r_value = right_record_.data+right_col.offset;
        return evaluate_compare(l_value, r_value, left_col.type, left_col.len, fed_conds_.at(i).op); // 判断该condition是否成立（断言为真）
    }
};