//
// Created by Anti on 2023/7/23.
// 用于生成join所需要的sql语句
//
#include "fmt/core.h"
#include "gtest/gtest.h"

TEST(BLOCK_JOIN,GEN_SQL) {
  fmt::print("create table t ( id int , t_name char (500));\ncreate table d (id int,d_name char(500));\n"); // 这里char填多一点，方便测试溢出的情况
  for(int i = 1; i <= 50; i++) {
    fmt::print("insert into t values({},'');\n",i);
  }
  for(int i = 1; i <= 50; i++) {
    fmt::print("insert into d values({},'');\n",i);
  }
  fmt::print("select * from t, d;");
}