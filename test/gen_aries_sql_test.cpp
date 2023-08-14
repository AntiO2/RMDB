//
// Created by Anti on 2023/8/14.
//

#include "gtest/gtest.h"
#include "fmt/core.h"
TEST(Aries,GEN_SQL) {
    int scale = 12345;
    fmt::print("create table t1 ( id int , t_name char (500));\ncreate table t2 (id int,d_name char(500));\n"); // 这里char填多一点，方便测试溢出的情况
    for(int i = scale; i > 0; i--) {
        fmt::print("insert into t1 values({},'');\n",i);
    }
    for(int i = scale; i > 0; i--) {
        fmt::print("insert into t2 values({},'');\n",i);
    }

    fmt::print("select * from t1,t2 where t1.id  = t2.id order by t1.id;\n");

    // fmt::print("drop table t1;\ndrop table d1;\n");
}