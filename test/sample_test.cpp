//
// Created by Anti on 2023/7/9.
//
#include <string>
#include <gtest/gtest.h>
#include "logger.h"
#include "system/sm.h"

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

TEST(SAMPLE_TEST, VECTOR_RM) {
    using namespace std;
    vector<int> count = {0,1,2,2,2,2,2,3,4};
    for(auto iter=count.begin();iter!=count.end();)
    {
            iter = count.erase(iter);
    }

    struct test_rm {
        vector<int> count_ = {0,1,2,2,2,2,2,3,4};
        bool test_rm_count() {
            count_.erase(count_.begin());
        }
    };
}

// std::vector<IndexMeta> indexes;
TEST(SAMPLE_TEST, INDEX_RM) {
    using namespace std;
    std::vector<IndexMeta> indexes;
    for(auto iter=indexes.begin();iter!=indexes.end();)
    {
       indexes.erase(iter);
    }
    cout<<endl;
}