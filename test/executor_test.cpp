//
// Created by Anti on 2023/7/15.
//
#include <random>
#include <thread>
#include "gtest/gtest.h"
#include "system/sm.h"
#include "record/rm.h"
#include "logger.h"
constexpr int MAX_FILES = 32;
constexpr int MAX_PAGES = 128;
constexpr size_t POOL_SIZE  = 128;
const std::string TEST_DB_NAME = "ExecutorTest_db";  // 以TEST_DB_NAME作为存放测试文件的根目录名

class ExeTest : public ::testing::Test {
public:
    std::unique_ptr<SmManager> sm_manager_;
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<RmManager> rm_manager_;// RmManager
    std::unique_ptr<IxManager> ixm_;
public:
    // This function is called before every test.
    void SetUp() override {
        ::testing::Test::SetUp();
        disk_manager_ = std::make_unique<DiskManager>();
        bpm_ = std::make_unique<BufferPoolManager>(POOL_SIZE,disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), bpm_.get());
        ixm_ = std::make_unique<IxManager>(disk_manager_.get(), bpm_.get());

        sm_manager_ = std::make_unique<SmManager>(disk_manager_.get(), bpm_.get(), rm_manager_.get(), ixm_.get()); // 创建sm_manager

        // 如果测试数据库存在首先删除
        if (sm_manager_->is_dir(TEST_DB_NAME)) {
            sm_manager_->drop_db(TEST_DB_NAME);
        }

        sm_manager_->create_db(TEST_DB_NAME);
        LOG_DEBUG("CREATE DB");
        assert(sm_manager_->is_dir(TEST_DB_NAME));
        sm_manager_->open_db(TEST_DB_NAME);
        LOG_DEBUG("OPEN DB");
    }

    // This function is called after every test.
    void TearDown() override {
        sm_manager_->close_db();
        LOG_DEBUG("CLOSE DB");
        sm_manager_->drop_db(TEST_DB_NAME);
        LOG_DEBUG("DROP DB");
    };

    /**
     * @brief 将buf填充size个字节的随机数据
     */
    void rand_buf(char *buf, int size) {
        srand((unsigned) time(nullptr));
        for (int i = 0; i < size; i++) {
            int rand_ch = rand() & 0xff;
            buf[i] = rand_ch;
        }
    }
};

TEST_F(ExeTest,SAMPLE_TEST) {
    LOG_DEBUG("Testing");
}