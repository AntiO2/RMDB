//
// Created by Anti on 2023/8/17.
// 用于生成index所需要的sql语句
//
#include "fmt/core.h"
#include "gtest/gtest.h"

TEST(BLOCK_JOIN,DISABLED_GEN_SQL) {
    int scale = 10000;
    fmt::print("create table t1 ( id int , t_name char (500));\n"); // 这里char填多一点，方便测试溢出的情况
    fmt::print("create index t1 (id);\n\n");
    for(int i = scale; i > 0; i--) {
        fmt::print("insert into t1 values({},'');\n",i);
        if(i%2) {
            fmt::print("delete from t1 where id={};\n",i);
        }
        fmt::print("select * from t1 where id={};\n",i);
    }
    fmt::print("select * from t1;\n");
    // fmt::print("drop table t1;\n");
}
TEST(BLOCK_JOIN,DISABLED_GEN_SQL2) {
    int scale = 1000;
    fmt::print("create table t1 ( id int , t_name char (500));\ncreate table t2 (d_name char(500),id2 int);\n"); // 这里char填多一点，方便测试溢出的情况
    for(int i = 2; i <= scale; i+=2) {
        fmt::print("insert into t1 values({},'');\n",i);
    }
    for(int i = 3; i <= scale; i+=3) {
        fmt::print("insert into t2 values('',{});\n",i);
    }

    fmt::print("select * from t1,t2 where t1.id  = t2.id2 order by t1.id;\n");

    // fmt::print("drop table t1;\ndrop table d1;\n");
}
TEST(INDEX, TWO_COL) {
    int scale = 3000;
    fmt::print("create table t1 (w_id int,name char(8),flo float);\n"
               "create index t1(w_id,flo);\n");
    for(int i = 1; i <= scale; i++) {
        fmt::print("insert into t1 values({},'{}{}',{});\n",i,i+1,i+2,i*i);
    }
    for(int i = 1; i <= scale; i++) {
        fmt::print("select w_id from t1 where w_id = {};\n",i);
    }
}