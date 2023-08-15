/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <regex>
#include "defs.h"
#include "record/rm_defs.h"
#include "logger.h"

struct TabCol {
    std::string tab_name;
    std::string col_name;

    friend bool operator<(const TabCol &x, const TabCol &y) {
        return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
    }
};

//将每个排序键与顺序绑定起来
struct OrderCol {
    TabCol tab_col;
    bool is_desc_;
};

struct Value {
    ColType type;  // type of value
    union {
        int int_val;      // int value
        float float_val;  // float value
    };
    //CHECK(liamY) 这里后面改进可以做成union
    std::string str_val;  // string value
    int64_t bigint_val;
    int64_t datetime_val;//date value 8bytes

    std::shared_ptr<RmRecord> raw;  // raw record buffer

    void set_int(int int_val_) {
        type = TYPE_INT;
        int_val = int_val_;
    }

    void set_float(float float_val_) {
        type = TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_) {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }

    void set_bigint(int64_t bigint_val_){
        type = TYPE_BIGINT;
        bigint_val = bigint_val_;
    }

    //也支持int值填入bigint
    void set_bigint(int int_val_){
        type = TYPE_BIGINT;
        bigint_val = int_val_;
    }

    //注意超出bigint范围的要抛出异常
    void set_bigint(const std::string &bigint_val_){
        type = TYPE_BIGINT;
        try {
            bigint_val = std::stoll(bigint_val_);
        }catch(std::out_of_range const& ex) {
            throw BigintOutOfRangeError("", bigint_val_);
        }
    }

    //liamY检测字符串是否符合日期格式
inline bool is_valid_date(const std::string& date_str) {
        static const std::regex pattern(R"(^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})$)");
        std::smatch match;
        if (!std::regex_match(date_str, match, pattern)) {
            return false; // 格式不匹配
        }
        if (match.size() != 7) {
            return false; // 捕获组数量错误
        }
        int year = std::stoi(match.str(1));
        int month = std::stoi(match.str(2));
        int day = std::stoi(match.str(3));
        int hour = std::stoi(match.str(4));
        int minute = std::stoi(match.str(5));
        int second = std::stoi(match.str(6));
        if (year < 1000 || year > 9999 || month < 1 || month > 12 || day < 1 || day > 31 ||
            hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
            return false; // 字段值非法
        }
        if (month == 2 && day > 29) {
            return false; // 2月的天数不合法
        }
        //如果还需要更准确的话
//        if ((month == 4 || month == 6 || month == 9 || month == 11) && day > 30) {
//            return false; // 4、6、9、11月的天数不合法
//        }
        return true;
    }
    //@description: 将字符串型日期转为8bytes的整数储存，表示方式为只留下数字，由年为最高位一直到秒。需要前面已经校验过准确性了
    //@auth: liamY
    inline int64_t datetime2datenum(const std::string &datetime_val_){
        static const std::regex pattern(R"(^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})$)");
        std::smatch match;
        std::regex_match(datetime_val_, match, pattern);
        std::string resStr;
        for(int i=1;i<=6;i++){
            resStr += match.str(i);
        }
        return std::stoll(resStr);
    }

    //liamY
    void set_datetime(const std::string &datetime_val_){
        //如果日期无效，报错
        if(!is_valid_date(datetime_val_)){throw DateTimeAbsurdError("",datetime_val_);}
        type = TYPE_DATETIME;
        datetime_val = datetime2datenum(datetime_val_);//存为64位整数数据
    }

    void init_raw(int len) {
        assert(raw == nullptr);
        raw = std::make_shared<RmRecord>(len);
        if (type == TYPE_INT) {
            assert(len == sizeof(int));
            *(int *)(raw->data) = int_val;
        } else if (type == TYPE_FLOAT) {
            assert(len == sizeof(float));
            *(float *)(raw->data) = float_val;
        } else if (type == TYPE_STRING) {
            if (len < (int)str_val.size()) {
                throw StringOverflowError();
            }
            memset(raw->data, 0, len);
            memcpy(raw->data, str_val.c_str(), str_val.size());
        } else if(type == TYPE_BIGINT){
            *(int64_t *)(raw->data) = bigint_val;
        } else if(type == TYPE_DATETIME){//liamY
            *(int64_t *)(raw->data) =   datetime_val;
        }
    }
};

//@description: 将64位整数对应的字符串的日期转为标准日期格式的字符串形式进行输出
inline  std::string datenum2datetime(const std::string &str){
    std::string datetime_str;
    datetime_str.reserve(16);  // 预分配内存
    // 拼接年
    datetime_str += str.substr(0, 4);
    datetime_str += "-";
    // 拼接月
    datetime_str += str.substr(4, 2);
    datetime_str += "-";
    // 拼接日
    datetime_str += str.substr(6, 2);
    datetime_str += " ";
    // 拼接时
    datetime_str += str.substr(8, 2);
    datetime_str += ":";
    // 拼接分
    datetime_str += str.substr(10, 2);
    datetime_str += ":";
    // 拼接秒
    datetime_str += str.substr(12, 2);
    return datetime_str;
}

/**
 * @description 这个是在ix_index_handle::ix_compare() 改过来的，用于在值之间进行判断大小。之后有新类型需要在这里增加
 *
 * @param a
 * @param b
 * @param type
 * @param col_len
 * @return a > b 返回1
 * @return a <&lt; b 返回-1
 * @return a = b 返回0
 * @modify_by antio2
 */
inline int value_compare(const char *a, const char *b, ColType type, int col_len) {
    switch (type) {
        case TYPE_INT: {
            int ia = *(int *)a;
            int ib = *(int *)b;
            return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
        }
        case TYPE_FLOAT: {
            float fa = *(float *)a;
            float fb = *(float *)b;
            return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
        }
        case TYPE_STRING:
        {
            //            if(strlen(a)> strlen(b))
            //                return 1;
            //            if(strlen(a)< strlen(b))
            //                return -1;

            auto res = memcmp(a, b, col_len);
            // LOG_DEBUG("String compare %d",res);
            return res > 0? 1: (res<0 ? -1: 0);
            break;
        }
        case TYPE_BIGINT:{
            int64_t ia = *(int64_t *)a;
            int64_t ib = *(int64_t *)b;
            return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
            break;
        }
        case TYPE_DATETIME:{
            //liamY
            int64_t ia = *(int64_t *)a;
            int64_t ib = *(int64_t *)b;
            return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
            break;
        }
        default:
            throw InternalError("Unexpected data type");
    }
}
inline int value_compare(const char *a, const char *b, ColType type) {
    return value_compare(a, b, type, col2len(type));
}
enum CompOp { OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE };
enum AggregateOp { AG_OP_NONE , AG_OP_COUNT, AG_OP_MAX, AG_OP_MIN, AG_OP_SUM};//liamY 不要从0开始,要有一个none

struct AggreInfo {
    AggregateOp op_;
    TabCol select_col_;
};

/**
 * TODO(AntiO2) 创建从type 到col_len的映射，从而减少参数传递
 * @param a
 * @param b
 * @param type
 * @param col_len
 * @param op
 * @return
 */
inline bool evaluate_compare(const char*a, const char* b, ColType type, int col_len,  CompOp op) {
    switch (op) {
        case OP_EQ:
            return value_compare(a, b, type, col_len) == 0;
        case OP_NE:
            return value_compare(a, b, type, col_len) != 0;
        case OP_LT:
            return value_compare(a, b, type, col_len) == -1;
        case OP_GT:
            return value_compare(a, b, type, col_len) == 1;
        case OP_LE:
            return value_compare(a, b, type, col_len) != 1;
        case OP_GE:
            return value_compare(a, b, type, col_len) != -1;
    }
}

struct Condition {
    TabCol lhs_col;   // left-hand side column
    CompOp op;        // comparison operator
    bool is_rhs_val{};  // true if right-hand side is a value (not a column)
    TabCol rhs_col;   // right-hand side column
    Value rhs_val;    // right-hand side value
    bool is_always_false_{false}; // 如果condition永远为false，该值为true。比如Where 1 = 2, 此时is_always_false_为true。
public:

//    /**
//     *
//     * @param lhs_col
//     * @return
//     */
//    inline bool evaluate_condition(ColMeta lhs_col_, char* ) const {
//        assert(lhs_col_.type==rhs_val.type);
//    }
};

struct SetClause {
    TabCol lhs;
    Value rhs;
};
/**
 * 通过Value和ColMetas构造RMrecord
 * @param values
 * @param cols
 * @param rec_size
 * @return
 */
//static std::unique_ptr<RmRecord> GetRecord(std::vector<Value>& values, std::vector<ColMeta> &cols, int rec_size) {
//    assert(values.size()==cols.size());
//    auto col_num = cols.size();
//    auto rec = RmRecord(rec_size); // 通过rec大小，创建空的rmrecord;
//    for(decltype(col_num) i = 0; i < col_num; i++) {
//        auto &val = values[i];
//        auto &col = cols[i];
//        if (col.type != val.type) {
//            throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
//        }
//        val.init_raw(col.len);
//        // 将Value数据存入rec中。
//        memcpy(rec.data + col.offset, val.raw->data, col.len);
//    }
//    return std::make_unique<RmRecord>(rec);
//}
