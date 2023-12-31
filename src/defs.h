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

#include <iostream>
#include <map>
#include "errors.h"
// 此处重载了<<操作符，在ColMeta中进行了调用
template<typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::ostream &operator<<(std::ostream &os, const T &enum_val) {
    os << static_cast<int>(enum_val);
    return os;
}

template<typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::istream &operator>>(std::istream &is, T &enum_val) {
    int int_val;
    is >> int_val;
    enum_val = static_cast<T>(int_val);
    return is;
}

struct Rid {
    int page_no;
    int slot_no;

//    Rid(int pageNo, int slotNo) : page_no(pageNo), slot_no(slotNo) {}
//    Rid() = default;
    friend bool operator==(const Rid &x, const Rid &y) {
        return x.page_no == y.page_no && x.slot_no == y.slot_no;
    }

    friend bool operator!=(const Rid &x, const Rid &y) { return !(x == y); }

//    auto operator==(const Rid&other) const ->bool {
//      return page_no==other.page_no&&slot_no==other.slot_no;
//    }
};

struct RidHash {
  std::size_t operator()(const Rid& rid) const {
    std::size_t h1 = std::hash<int>{}(rid.page_no);
    std::size_t h2 = std::hash<int>{}(rid.slot_no);
    return h1 ^ (h2 << 1); // 组合哈希值
  }
};
/**
 *
 * TODO 添加新的类信息。
 */
enum ColType {
    TYPE_INT, TYPE_FLOAT, TYPE_STRING, TYPE_BIGINT,TYPE_DATETIME,
};

inline int col2len(ColType type) {
    std::map<ColType, int> l = {
            {TYPE_INT,    sizeof(int)},
            {TYPE_FLOAT,  sizeof(float)},
            // 未知大小 {TYPE_STRING, "STRING"}
            {TYPE_BIGINT, sizeof (int64_t)},
            {TYPE_DATETIME,sizeof(int64_t)},
    };
    auto iter_=l.find(type);
    if(iter_==l.end()) {
        throw InternalError("Type len unknown");
    }
    return iter_->second;
}

/**
 * @TODO 添加新的类之后，在这里添加字符信息
 * @param type
 * @return
 */
inline std::string coltype2str(ColType type) {
    std::map<ColType, std::string> m = {
            {TYPE_INT,    "INT"},
            {TYPE_FLOAT,  "FLOAT"},
            {TYPE_STRING, "STRING"},
            {TYPE_BIGINT,"BIGINT"},//liamY
            {TYPE_DATETIME,"DATETIME"}
    };
    return m.at(type);
}

class RecScan {
public:
    virtual ~RecScan() = default;

    virtual void next() = 0;

    virtual bool is_end() const = 0;

    virtual Rid rid() const = 0;
};
