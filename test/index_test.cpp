//
// Created by Anti on 2023/7/17.
//


#include <algorithm>
#include <cstdio>
#include "fmt/core.h"
#include "logger.h"
#include "gtest/gtest.h"
#define private public
#include "system/sm.h"
#include "index/ix.h"
#undef private
const std::string TEST_DB_NAME = "test_db";
const std::string TEST_FILE_NAME = "table1";
const int POOL_SIZE = 1000;
class IndexTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<IxIndexHandle> ih_;
    std::unique_ptr<Transaction> txn_;
    std::vector<ColDef> col_defs;
    std::vector<ColMeta> cols; //用于创建索引的列
public:

    void SetUp() override {
        LOG_INFO("索引初始化开始");
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(POOL_SIZE, disk_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        txn_ = std::make_unique<Transaction>(0);
        int curr_offset = 0;

        ColDef colA{.name="A",.type=ColType::TYPE_INT,.len=col2len(ColType::TYPE_INT)};
        col_defs.emplace_back(colA);

        for (auto &col_def : col_defs) {
            ColMeta col = {.tab_name = TEST_FILE_NAME,
                    .name = col_def.name,
                    .type = col_def.type,
                    .len = col_def.len,
                    .offset = curr_offset,
                    .index = false};
            curr_offset += col_def.len;
            cols.emplace_back(col);
        }
        if(disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->destroy_dir(TEST_DB_NAME); // 先删除之前的数据
        }
        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
        if (ix_manager_->exists(TEST_FILE_NAME, cols)) {
            ix_manager_->destroy_index(TEST_FILE_NAME, cols);
        }

        ix_manager_->create_index(TEST_FILE_NAME, cols);

        assert(ix_manager_->exists(TEST_FILE_NAME, cols));
        ih_ = ix_manager_->open_index(TEST_FILE_NAME, cols);
        assert(ih_ != nullptr);

        LOG_INFO("索引初始化完成");
    }

    void TearDown() override {
        ix_manager_->close_index(ih_.get());
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
        // disk_manager_->destroy_dir(TEST_DB_NAME); // 删除测试文件
        LOG_INFO("索引已删除");
    };
};
TEST(UPPERBOUND,DISABLED_SIMPLE_TEST) {
    // 测试二分查找正确性。
    int num = 10;
    std::vector<int> a ;
    for(int i = 1; i <= num;i++) {
        a.emplace_back(i);
    }
    ASSERT_EQ(a.size(),num);
    int l = 0, r = num, mid, flag;
    int target = 4;
    while(l < r){ //use binary search
        mid = (l+r)/2;
        flag = a[mid] < target?(-1):(a[mid]>target?1:0);
        if(flag <= 0)
            l = mid + 1;
        else
            r = mid;
    }
    EXPECT_EQ(a[l], 5);

    target = num;
    l = 0;
    r = num;
    while(l < r){ //use binary search
        mid = (l+r)/2;
        flag = a[mid] < target?(-1):(a[mid]>target?1:0);
        LOG_DEBUG("%s", fmt::format("l {} r {} mid {} mid_value {}",l,r,mid,a[mid]).c_str());
        if(flag <= 0)
            l = mid + 1;
        else
            r = mid;
    }
    LOG_DEBUG("%d",l);
    EXPECT_EQ(l,num);
}

/**
 * 初始化索引测试
 *
 */
TEST_F(IndexTest, DISABLED_InitTest) {
    LOG_INFO("Init test");
}

