/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // 初始化file_handle和rid（指向第一个存放了记录的位置）

    //初始化
    file_handle_ = file_handle;
    rid_ = Rid{RM_FIRST_RECORD_PAGE,-1};

    //使rid指向第一个存放了记录的位置
    next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置

    //遍历页
    for(int page_no = rid_.page_no; page_no < file_handle_->file_hdr_.num_pages;page_no++)
    {
        RmPageHandle pageHandle = file_handle_->fetch_page_handle(page_no);
        int ret = Bitmap::next_bit(true, pageHandle.bitmap, file_handle_->file_hdr_.num_records_per_page,rid_.slot_no);
        if(ret != file_handle_->file_hdr_.num_records_per_page)
        {
            rid_.page_no = page_no;
            rid_.slot_no = ret;
            file_handle_->buffer_pool_manager_->unpin_page(PageId{file_handle_->fd_,page_no}, false);
            return;
        }
        file_handle_->buffer_pool_manager_->unpin_page(PageId{file_handle_->fd_,page_no}, false);
        //下一轮又是从-1开始
        rid_.slot_no = -1;
    }
    //未找到存放记录的非空闲位置
    rid_.page_no = RM_NO_PAGE;
    rid_.slot_no = -1;
}

/**
 * @brief  判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    return rid_.page_no == RM_NO_PAGE;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}