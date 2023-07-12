//
// Created by Anti on 2023/7/9.
//
#include <gtest/gtest.h>

TEST(SAMPLE_TEST,CHECKPOINT_1) {
    EXPECT_EQ(true, true);
    EXPECT_NE(true,false);
}

TEST(SAMPLE_TEST, SET_TEST) {
    std::unordered_map<int,bool> map;
    map.emplace(1,true);
    EXPECT_EQ(map.size(), 1);
    map.emplace(1,true);
    EXPECT_EQ(map.size(), 1);
}