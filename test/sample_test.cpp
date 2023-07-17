//
// Created by Anti on 2023/7/9.
//
#include <string>
#include <gtest/gtest.h>
#include "logger.h"
#include "system/sm.h"
#include "common/rwlatch.h"
#include "fmt/core.h"
TEST(LOGGER_TEST, INFO_TEST) {
    std::string msg = "Test Info Logger";
    int code = 1;
    size_t dota = 825;
    LOG_INFO("code: %d", code);
    LOG_INFO("this is test info %zu", dota);
    LOG_INFO("msg: %s", msg.c_str());
}
TEST(LOGGER_TEST, DEBUG_TEST) {
    std::string msg = "Test Debug Logger";
    int code = 1;
    size_t dota = 825;
    LOG_DEBUG("code: %d", code);
    LOG_DEBUG("this is debug info %zu", dota);
    LOG_DEBUG("msg: %s", msg.c_str());
}
TEST(LOGGER_TEST, ERROR_TEST) {
    std::string msg = "Test Error Logger";
    int code = 1;
    size_t dota = 825;
    LOG_ERROR("code: %d", code);
    LOG_ERROR("this is error info %zu", dota);
    LOG_ERROR("msg: %s", msg.c_str());
}
TEST(SAMPLE_TEST,CHECKPOINT_1) {
    EXPECT_EQ(true, true);
    EXPECT_NE(true,false);
}

TEST(SAMPLE_TEST, DISABLED_SET_TEST) {
    std::unordered_map<int,bool> map;
    map.emplace(1,true);
    EXPECT_EQ(map.size(), 1);
    map.emplace(1,true);
    EXPECT_EQ(map.size(), 1);
}

TEST(SAMPLE_TEST, STRINGCMP_TEST) {
    char str1[15];
    char str2[15];

    int ret;

    memcpy(str1, "Data Structure",15);
    memcpy(str2, "A",15);

    LOG_DEBUG("Result: %d",memcmp(str1,str2,6));
}

class rwlatch_test : public ::testing::Test {
public:
    RWLatch latch_;
    std::vector<std::thread> readers{};
    std::vector<std::thread> writers{};
protected:
    void SetUp() override {
        for(int i = 1; i <=3; i++) {
            readers.emplace_back([&](){
                rwlatch_test::Reader(i);
            });
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        for(int i = 1; i <=3; i++) {
            writers.emplace_back([&](){
                rwlatch_test::Writer(i);
            });
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        for(auto &reader:readers) {
            reader.join();
        }
        for(auto &writer:writers) {
            writer.join();
        }
    }

    void TearDown() override {
        Test::TearDown();
    }
public:
    void Reader(int num) {
        LOG_DEBUG("%s", fmt::format("Reader{} begin",num).c_str());

        int cnt = 0;
        while(cnt < 10) {
            latch_.read_lock();
            LOG_INFO("%s", fmt::format("Reader {} get lock",num).c_str());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            LOG_INFO("%s", fmt::format("{} Readers get read lock",latch_.getReaderCnt()).c_str());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            latch_.read_unlock();
            LOG_INFO("%s", fmt::format("Reader {} unlock",num).c_str());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            LOG_INFO("%s", fmt::format("{} Readers get read lock",latch_.getReaderCnt()).c_str());
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            cnt++;
        }
    }
    void Writer(int num) {
        LOG_DEBUG("%s", fmt::format("Writer{} begin",num).c_str());
        int cnt = 0;
        while(cnt < 10) {
            latch_.write_lock();
            LOG_INFO("%s", fmt::format("Writer {} get lock",num).c_str());
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            EXPECT_EQ(latch_.getReaderCnt(),0);
            latch_.write_unlock();

            LOG_INFO("%s", fmt::format("Writer {} unlock",num).c_str());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            cnt++;
        }
    }

};
TEST_F(rwlatch_test, RWLATCH) {
    // TODO (AntiO2) 强化这里的Rwlatch测试
}