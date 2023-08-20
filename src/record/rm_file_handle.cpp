/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // Todo: v
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）

    // 1. 获取指定记录所在的page handle
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    pageHandle.page->RLock();
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
    int size = pageHandle.file_hdr->record_size;
    std::unique_ptr<RmRecord> record = std::make_unique<RmRecord>(size);
    record->size = size;
    memcpy(record->data, pageHandle.get_slot(rid.slot_no),size);
    pageHandle.page->RUnlock();
    buffer_pool_manager_->unpin_page(PageId{fd_,rid.page_no}, false); // check(AntiO2) 这里是否需要unpin

    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context, std::string* table_name,  LogOperation log_op, lsn_t undo_next) {
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no

    // 1. 获取当前未满的page handle
    RmPageHandle pageHandle = create_page_handle();
    pageHandle.page->WLock();
    // 2. 在page handle中找到空闲slot位置,从位图找
    int slot_no = Bitmap::first_bit(false, pageHandle.bitmap, file_hdr_.num_records_per_page);
    if(slot_no >= file_hdr_.num_records_per_page) {
        LOG_ERROR("Try insert into invalid slot");
        assert(false);
    }
    Bitmap::set(pageHandle.bitmap,slot_no);
    char* addr_slot = pageHandle.get_slot(slot_no);

    RmRecord insert_value(file_hdr_.record_size, buf);

    auto rid =  Rid{pageHandle.page->get_page_id().page_no, slot_no};
    auto log_mgr = context->log_mgr_;
    LogRecord* log_record= nullptr;

    if(log_op==LogOperation::REDO) {
        log_record = new InsertLogRecord(context->txn_->getTxnId(),insert_value,rid,*table_name,context->txn_->get_prev_lsn(), file_hdr_.first_free_page_no, file_hdr_.num_pages);
    } else {
        log_record = new CLR_Insert_Record(context->txn_->getTxnId(),insert_value,rid,*table_name,context->txn_->get_prev_lsn(), undo_next, file_hdr_.first_free_page_no, file_hdr_.num_pages);
    }
    log_mgr->add_log_to_buffer(log_record);
    context->txn_->set_prev_lsn(log_record->lsn_);
    log_mgr->active_txn_table_[context->txn_->getTxnId()] = log_record->lsn_;
    auto page_id = PageId{fd_,rid.page_no};

    // 3. 将buf复制到空闲slot位置
    memcpy(addr_slot,buf,pageHandle.file_hdr->record_size);

    // 4. 更新page_handle.page_hdr中的数据结构
    pageHandle.page_hdr->num_records++;
    //考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no

    if(pageHandle.page_hdr->num_records >= pageHandle.file_hdr->num_records_per_page)
    {
        //next_free_page_no怎么更新? v不更新了，等create_page_handle()自己调
        file_hdr_.first_free_page_no = pageHandle.page_hdr->next_free_page_no;
        pageHandle.page_hdr->next_free_page_no=RM_NO_PAGE;
        disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    }


    if(log_mgr->dirty_page_table_.find(PageId{fd_,rid.page_no})==log_mgr->dirty_page_table_.end()) {
        log_mgr->dirty_page_table_.emplace(page_id, log_record->lsn_);
    }

    pageHandle.page->set_page_lsn(log_record->lsn_);
    //该页面结束使用，取消对该页面的固定,并标记为dirty
    pageHandle.page->WUnlock();
    buffer_pool_manager_->unpin_page(pageHandle.page->get_page_id(),true);
    return rid;
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record_recover(const Rid &rid, char *buf, lsn_t lsn, int first_free_page, int num_pages) {

    file_hdr_.first_free_page_no = first_free_page;
    file_hdr_.num_pages = num_pages;
    //1. 拿到pageHandle
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    pageHandle.page->WLock();
    //2. 判断并更新位图
    assert(!Bitmap::is_set(pageHandle.bitmap,rid.slot_no));
    Bitmap::set(pageHandle.bitmap,rid.slot_no);
    pageHandle.page_hdr->num_records++;
    //3. 复制数据
    char* addr_slot = pageHandle.get_slot(rid.slot_no);
    memcpy(addr_slot,buf,pageHandle.file_hdr->record_size);
    if(pageHandle.page_hdr->num_records >= pageHandle.file_hdr->num_records_per_page)
    {
        //next_free_page_no怎么更新? v不更新了，等create_page_handle()自己调
        file_hdr_.first_free_page_no = pageHandle.page_hdr->next_free_page_no;
        pageHandle.page_hdr->next_free_page_no=RM_NO_PAGE;
        disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    }
    pageHandle.page->set_page_lsn(lsn);
    pageHandle.page->WUnlock();
    //该页面结束使用，取消对该页面的固定,并标记为dirty
    buffer_pool_manager_->unpin_page(pageHandle.page->get_page_id(),true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context, std::string* table_name,  LogOperation log_op, lsn_t undo_next) {
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()

    // 1. 获取指定记录所在的page handle
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    pageHandle.page->WLock();
    //位图判断及更新
    if(!Bitmap::is_set(pageHandle.bitmap,rid.slot_no))
        throw RecordNotFoundError(rid.page_no,rid.slot_no);
    char* addr_slot = pageHandle.get_slot(rid.slot_no);
    RmRecord delete_value(file_hdr_.record_size, addr_slot);

    auto txn = context->txn_;
    auto log_mgr = context->log_mgr_;
    auto page_id = PageId{fd_,rid.page_no};
    LogRecord* log_record= nullptr;
    if(log_op==LogOperation::REDO) {
        log_record = new DeleteLogRecord(txn->getTxnId(), delete_value, rid, *table_name, txn->getPrevLsn(), file_hdr_.first_free_page_no, file_hdr_.num_pages);
    } else {
        log_record = new CLR_Delete_Record(context->txn_->getTxnId(), rid, *table_name,context->txn_->get_prev_lsn(), undo_next, file_hdr_.first_free_page_no, file_hdr_.num_pages);
    }

    log_mgr->add_log_to_buffer(log_record);
    txn->set_prev_lsn(log_record->lsn_);
    log_mgr->active_txn_table_[txn->getTxnId()] = log_record->lsn_;
    if(log_mgr->dirty_page_table_.find(PageId{fd_,rid.page_no})==log_mgr->dirty_page_table_.end()) {
        log_mgr->dirty_page_table_.emplace(page_id,log_record->lsn_);
    }
    Bitmap::reset(pageHandle.mark_delete,rid.slot_no);
    Bitmap::reset(pageHandle.bitmap,rid.slot_no);
    // 2. 更新page_handle.page_hdr中的数据结构
    pageHandle.page_hdr->num_records--;

    if(pageHandle.page_hdr->num_records == file_hdr_.num_records_per_page - 1) {
        // 当从无空位转化成有空位，进行release操作
        release_page_handle(pageHandle);
    }
    pageHandle.page->set_page_lsn(log_record->lsn_);
    pageHandle.page->WUnlock();
    buffer_pool_manager_->unpin_page(PageId{fd_, rid.page_no}, true);
}

void RmFileHandle::delete_record_recover(const Rid& rid,lsn_t lsn, int first_free_page, int num_pages) {
    // 重做delete操作(redo)
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()

    // 1. 获取指定记录所在的page handle
    file_hdr_.first_free_page_no = first_free_page;
    file_hdr_.num_pages = num_pages;
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    pageHandle.page->WLock();
    //位图判断及更新
    if(!Bitmap::is_set(pageHandle.bitmap,rid.slot_no))
        throw RecordNotFoundError(rid.page_no,rid.slot_no);

    Bitmap::reset(pageHandle.mark_delete,rid.slot_no);
    Bitmap::reset(pageHandle.bitmap,rid.slot_no);

    // 2. 更新page_handle.page_hdr中的数据结构
    pageHandle.page_hdr->num_records--;
    pageHandle.page->set_page_lsn(lsn);
    pageHandle.page->WUnlock();
    if(pageHandle.page_hdr->num_records == file_hdr_.num_records_per_page - 1) {
        // 当从无空位转化成有空位，进行release操作
        release_page_handle(pageHandle);
    }
    buffer_pool_manager_->unpin_page(PageId{fd_, rid.page_no}, true);
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context, std::string* table_name,  LogOperation log_op, lsn_t undo_next) {

    // 1. 获取指定记录所在的page handle
    // 2. 更新记录

    // 1. 获取指定记录所在的page handle
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    pageHandle.page->WLock();
    //判断位图
    if (!Bitmap::is_set(pageHandle.bitmap, rid.slot_no)) {
        assert(false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    // 2. 更新记录

    char* addr_slot = pageHandle.get_slot(rid.slot_no);
    auto size = pageHandle.file_hdr->record_size;
    RmRecord before_value(size, addr_slot);
    RmRecord after_value(size, buf);

    auto tid = context->txn_->getTxnId();
    LogRecord* log_record= nullptr;
    if(log_op==LogOperation::REDO) {
        log_record=new UpdateLogRecord(tid,before_value,after_value,rid, *table_name,context->txn_->getPrevLsn(),file_hdr_.first_free_page_no,file_hdr_.num_pages);
    } else {
        log_record=new CLR_UPDATE_RECORD(tid,before_value,after_value,rid, *table_name,context->txn_->getPrevLsn(),undo_next,file_hdr_.first_free_page_no,file_hdr_.num_pages);
    }
    // context->log_mgr_->active_txn_table_[context->txn_->getTxnId()];
    auto log_mgr = context->log_mgr_;
    log_mgr->add_log_to_buffer(log_record);
    context->txn_->set_prev_lsn(log_record->lsn_);
    log_mgr->active_txn_table_[tid] = log_record->lsn_; // 维护att中的last lsn
    auto page_id = PageId{fd_,rid.page_no};
    auto table_iter = log_mgr->dirty_page_table_.find(page_id);
    if(table_iter==log_mgr->dirty_page_table_.end()) {
        // 维护rec lsn
        log_mgr->dirty_page_table_.emplace(page_id, log_record->lsn_);
    }

    memcpy(addr_slot,buf,size);
    pageHandle.page->set_page_lsn(log_record->lsn_);
    buffer_pool_manager_->unpin_page(PageId{fd_,rid.page_no}, true);
}
/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record_recover(const Rid& rid, char* buf,lsn_t lsn, int first_free_page, int num_pages) {


    file_hdr_.first_free_page_no = first_free_page;
    file_hdr_.num_pages = num_pages;
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录

    // 1. 获取指定记录所在的page handle
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    //判断位图
    if (!Bitmap::is_set(pageHandle.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    // 2. 更新记录
    char* addr_slot = pageHandle.get_slot(rid.slot_no);
    memcpy(addr_slot,buf,pageHandle.file_hdr->record_size);
    pageHandle.page->set_page_lsn(lsn);
    pageHandle.page->WUnlock();

    buffer_pool_manager_->unpin_page(PageId{fd_,rid.page_no}, true);
}
/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // Todo: v
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception

    //1.if page_no is invalid, throw PageNotExistError exception
    if(page_no >= file_hdr_.num_pages)
        throw PageNotExistError("",page_no);

    //使用缓冲池获取指定页面
    Page* page = buffer_pool_manager_->fetch_page(PageId{fd_,page_no});
    //如果page是null
    if(page == nullptr)
        throw PageNotExistError("",page_no);

    //生成page_handle返回给上层
    return RmPageHandle{&file_hdr_,page};
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // Todo: v
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_

    // 1.使用缓冲池来创建一个新page
    auto *pageId = new PageId;
    pageId->fd = fd_;
    Page* page = buffer_pool_manager_->new_page(pageId);
    if(page == nullptr)
        return {&file_hdr_, nullptr};

    // 2.更新page handle中的相关信息
    RmPageHandle pageHandle = RmPageHandle(&file_hdr_,page);
    pageHandle.page_hdr->num_records = 0;
    pageHandle.page_hdr->next_free_page_no = RM_NO_PAGE;
    Bitmap::init(pageHandle.bitmap,pageHandle.file_hdr->bitmap_size);

    // 3.更新file_hdr_
    file_hdr_.num_pages++;
    file_hdr_.first_free_page_no = pageHandle.page->get_page_id().page_no;
    disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_)); // 更新之后，需要立即写回磁盘
    return pageHandle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo: v
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层

    if(file_hdr_.first_free_page_no == RM_NO_PAGE)
        return create_new_page_handle();
    else
    {
        int page_no = file_hdr_.first_free_page_no;
        return fetch_page_handle(page_no);
    }
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle&page_handle) {
    // Todo: v
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no

    //链表
    if(page_handle.page->get_page_id().page_no == page_handle.file_hdr->first_free_page_no) {
        // 如何判断已经在链表中的情况？
        return;
    }
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
    disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_)); // 更新之后，需要立即写回磁盘
}

void RmFileHandle::mark_delete_record(const Rid &rid, Context *context, std::string *table_name, LogOperation log_op,
                                      lsn_t undo_next) {
    // 1. 获取指定记录所在的page handle
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    pageHandle.page->WLock();
    auto txn = context->txn_;
    auto log_mgr = context->log_mgr_;
    auto page_id = PageId{fd_,rid.page_no};
    LogRecord* log_record= nullptr;
    if(log_op==LogOperation::REDO) {
        log_record = new Mark_Delete_Record(txn->getTxnId(), rid, *table_name, txn->getPrevLsn(), file_hdr_.first_free_page_no, file_hdr_.num_pages);
    } else {
        log_record = new CLR_Delete_Record(context->txn_->getTxnId(), rid, *table_name,context->txn_->get_prev_lsn(), undo_next, file_hdr_.first_free_page_no, file_hdr_.num_pages);
    }
    log_mgr->add_log_to_buffer(log_record);
    txn->set_prev_lsn(log_record->lsn_);
    log_mgr->active_txn_table_[txn->getTxnId()] = log_record->lsn_;
    if(log_mgr->dirty_page_table_.find(PageId{fd_,rid.page_no})==log_mgr->dirty_page_table_.end()) {
        log_mgr->dirty_page_table_.emplace(page_id,log_record->lsn_);
    }

    if(log_op==LogOperation::REDO) {
        Bitmap::set(pageHandle.mark_delete,rid.slot_no);
    } else {
        Bitmap::reset(pageHandle.mark_delete,rid.slot_no);
    }
    buffer_pool_manager_->unpin_page(PageId{fd_, rid.page_no}, true);

    pageHandle.page->set_page_lsn(log_record->lsn_);
    pageHandle.page->WUnlock();
    buffer_pool_manager_->unpin_page(PageId{fd_, rid.page_no}, true);
}

bool RmFileHandle::is_mark_delete(const Rid &rid, Context *context) const {
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    pageHandle.page->RLock();
    auto res = Bitmap::is_set(pageHandle.mark_delete, rid.slot_no);
    pageHandle.page->RUnlock();
    return res;

}

void
RmFileHandle::mark_delete_record_recover(const Rid &rid, lsn_t lsn, int first_free_page, int num_pages, bool mark) {
    file_hdr_.first_free_page_no = first_free_page;
    file_hdr_.num_pages = num_pages;
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录

    // 1. 获取指定记录所在的page handle
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    pageHandle.page->WLock();
    // 2. 更新记
    if(mark) {
        Bitmap::set(pageHandle.mark_delete, rid.slot_no);
    } else {
        Bitmap::reset(pageHandle.mark_delete, rid.slot_no);
    }
    pageHandle.page->set_page_lsn(lsn);
    pageHandle.page->WUnlock();
    buffer_pool_manager_->unpin_page(PageId{fd_,rid.page_no}, true);
}




