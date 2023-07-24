//
// Created by Anti on 2023/7/23.
// 用于生成join所需要的sql语句
//
#include "fmt/core.h"
#include "gtest/gtest.h"

TEST(BLOCK_JOIN,GEN_SQL) {
  int scale = 12345;
  fmt::print("create table t1 ( id int , t_name char (500));\ncreate table t2 (id int,d_name char(500));\n"); // 这里char填多一点，方便测试溢出的情况
  for(int i = scale; i > 0; i--) {
    fmt::print("insert into t1 values({},'');\n",i);
  }
  for(int i = scale; i > 0; i--) {
    fmt::print("insert into t2 values({},'');\n",i);
  }

  fmt::print("select * from t1,d1 where t1.id  = d1.id order by t1.id;\n");

  // fmt::print("drop table t1;\ndrop table d1;\n");
}