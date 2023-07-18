//
// Created by Anti on 2023/7/17.
//
#include "gtest/gtest.h"
#include "fmt/core.h"
#include "logger.h"
TEST(UPPERBOUND,SIMPLE_TEST) {
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