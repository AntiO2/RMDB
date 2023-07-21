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

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "errors.h"
#include "sm_defs.h"

/* 字段元数据 */
struct ColMeta {
    std::string tab_name;   // 字段所属表名称
    std::string name;       // 字段名称
    ColType type;           // 字段类型
    int len;                // 字段长度
    int offset;             // 字段位于记录中的偏移量
    bool index;             /** unused */

    friend std::ostream &operator<<(std::ostream &os, const ColMeta &col) {
        // ColMeta中有各个基本类型的变量，然后调用重载的这些变量的操作符<<（具体实现逻辑在defs.h）
        return os << col.tab_name << ' ' << col.name << ' ' << col.type << ' ' << col.len << ' ' << col.offset << ' '
                  << col.index;
    }

    friend std::istream &operator>>(std::istream &is, ColMeta &col) {
        return is >> col.tab_name >> col.name >> col.type >> col.len >> col.offset >> col.index;
    }
};

/* 索引元数据 */
struct IndexMeta {
    std::string tab_name;           // 索引所属表名称
    int col_tot_len;                // 索引字段长度总和（sum(cols.len)）
    int col_num;                    // 索引字段数量
    std::vector<ColMeta> cols;      // 索引包含的字段

    friend std::ostream &operator<<(std::ostream &os, const IndexMeta &index) {
        os << index.tab_name << " " << index.col_tot_len << " " << index.col_num;
        for(auto& col: index.cols) {
            os << "\n" << col;
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, IndexMeta &index) {
        is >> index.tab_name >> index.col_tot_len >> index.col_num;
        for(int i = 0; i < index.col_num; ++i) {
            ColMeta col;
            is >> col;
            index.cols.push_back(col);
        }
        return is;
    }
};

/* 表元数据 */
struct TabMeta {
    std::string name;                   // 表名称
    std::vector<ColMeta> cols;          // 表包含的字段
    std::vector<IndexMeta> indexes;     // 表上建立的索引


    TabMeta(){}

    TabMeta(const TabMeta &other) {
        name = other.name;
        for(auto col : other.cols) cols.push_back(col);
    }

    /* 判断当前表中是否存在名为col_name的字段 */
    bool is_col(const std::string &col_name) const {
        auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) { return col.name == col_name; });
        return pos != cols.end();
    }
    void remove_index(const std::vector<std::string>& col_names) {
        for(auto index = indexes.begin(); index!=indexes.end();index++) {
            if(index->col_num == col_names.size()) {
                size_t i = 0;
                for(; i < index->col_num; ++i) {
                    if(index->cols[i].name != col_names[i])
                        break;
                }
                if(i == index->col_num) {
                    indexes.erase(index);
                    return;
                }
            }
        }
        throw IndexNotFoundError(name,col_names);
    }
    /* 判断当前表上是否建有指定索引，索引包含的字段为col_names */
    bool is_index(const std::vector<std::string>& col_names) const {
        for(auto& index: indexes) {
            if(index.col_num == col_names.size()) {
                size_t i = 0;
                for(; i < index.col_num; ++i) {
                    if(index.cols[i].name.compare(col_names[i]) != 0)
                        break;
                }
                if(i == index.col_num) return true;
            }
        }
        return false;
    }

    template<typename T>
    std::vector<typename T::iterator> findAllElements(const T& container, const typename T::value_type& value) {
        std::vector<typename T::iterator> foundElements;
        typename T::iterator iter = std::find(container.begin(), container.end(), value);

        while (iter != container.end()) {
            foundElements.push_back(iter);
            iter = std::find_if(std::next(iter), container.end(), [&value](const typename T::value_type& element) {
                return element == value;
            });
        }

        return foundElements;
    }

    struct FoundItem {
        std::string value;
        size_t index;
    };

    std::vector<FoundItem> find_all(const std::vector<std::string>& container, const std::string& value) const {
        std::vector<FoundItem> result;
        size_t index = 0;
        for (auto iter = container.begin(); iter != container.end(); ++iter, ++index) {
            if (*iter == value) {
                result.push_back({*iter, index});
            }
        }
        return result;
    }

    /* 代替原来is_index返回bool的使用
     * 如果有index返回IndexMeta*/
    //TODO 参数传CompOp的话好像有嵌套调用的错误,先用int草率处理一下
    //0:NOT_EQ, 1:EQ, 2:其他
    std::pair<IndexMeta,size_t> get_index(const std::vector<std::string>& col_names, const std::vector<int>& ops) const {
        //最左匹配, 并支持列的顺序交换

        size_t max_match_cols = 0;
        size_t min_mismatch_cols =INT32_MAX;
        size_t match_cols = 0;
        size_t mismatch_cols = 0;
        IndexMeta const* best_choice = nullptr;

        for(auto& index: indexes) {
            size_t i = 0;
            bool flag_break = false; //标记还能不能走下一列
            bool flag_exit;

            for(; i < index.col_num; ++i) {
                //原版不支持顺序调换,且要求列和索引每一项完全一致的匹配
//                    if(index.cols[i].name.compare(col_names[i]) != 0)
//                        break;

                //1. 假如索引有a,b,c,d，先检测是否有a; 并且可以检测到最多的连续的,比如col_names有a,b,d,这里能找到a,b. i挪到c的位置
                // 查找字符串在第一个向量中的位置

                std::vector<FoundItem> found_items = find_all(col_names,index.cols[i].name);

                if(found_items.empty())
                    break;
                else {
                    for (const auto& item : found_items) {
                        size_t op_index = item.index;
                        int op = ops[op_index];
                        if(!op){
                            flag_exit = true;
                            break;      //不等于的话直接break
                        }
                        else {
                            match_cols++;
                            if(op!=1)
                                flag_break = true;
                        }
                    }
                    if(flag_break && !flag_exit){
                        i++;
                        break;
                    }
                    if(flag_exit)
                        break;
                }

            }

            //i=0的话显然就是a都没有,可以检测下一个Index了
            if(i == 0) continue;
            if(index.col_num < match_cols)
                mismatch_cols = 0;
            else
                mismatch_cols = index.col_num - match_cols;
            if(match_cols > max_match_cols || (match_cols == max_match_cols && mismatch_cols < min_mismatch_cols)) {
                best_choice = &index;
                min_mismatch_cols = mismatch_cols;
                max_match_cols = match_cols;
            }
            match_cols = 0;
        }

        if(best_choice)
            return {*best_choice, max_match_cols};
        return {IndexMeta(),0};
    }

    /* 根据字段名称集合获取索引元数据 */
    std::vector<IndexMeta>::iterator get_index_meta(const std::vector<std::string>& col_names) {
        for(auto index = indexes.begin(); index != indexes.end(); ++index) {
            if((*index).col_num != col_names.size()) continue;
            auto& index_cols = (*index).cols;
            size_t i = 0;
            for(; i < col_names.size(); ++i) {
                if(index_cols[i].name.compare(col_names[i]) != 0) 
                    break;
            }
            if(i == col_names.size()) return index;
        }
        throw IndexNotFoundError(name, col_names);
    }

    /* 根据字段名称获取字段元数据 */
    std::vector<ColMeta>::iterator get_col(const std::string &col_name) {
        auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) { return col.name == col_name; });
        if (pos == cols.end()) {
            throw ColumnNotFoundError(col_name);
        }
        return pos;
    }
    ColMeta get_col_meta(const std::string &col_name) {
        for(auto &col:cols) {
            if(col.name==col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(col_name);
    }
    friend std::ostream &operator<<(std::ostream &os, const TabMeta &tab) {
        os << tab.name << '\n' << tab.cols.size() << '\n';
        for (auto &col : tab.cols) {
            os << col << '\n';  // col是ColMeta类型，然后调用重载的ColMeta的操作符<<
        }
        os << tab.indexes.size() << "\n";
        for (auto &index : tab.indexes) {
            os << index << "\n";
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, TabMeta &tab) {
        size_t n;
        is >> tab.name >> n;
        for (size_t i = 0; i < n; i++) {
            ColMeta col;
            is >> col;
            tab.cols.push_back(col);
        }
        is >> n;
        for(size_t i = 0; i < n; ++i) {
            IndexMeta index;
            is >> index;
            tab.indexes.push_back(index);
        }
        return is;
    }
};

// 注意重载了操作符 << 和 >>，这需要更底层同样重载TabMeta、ColMeta的操作符 << 和 >>
/* 数据库元数据 */
class DbMeta {
    friend class SmManager;

   private:
    std::string name_;                      // 数据库名称
    std::map<std::string, TabMeta> tabs_;   // 数据库中包含的表

   public:
    // DbMeta(std::string name) : name_(name) {}

    /* 判断数据库中是否存在指定名称的表 */
    bool is_table(const std::string &tab_name) const { return tabs_.find(tab_name) != tabs_.end(); }

    void SetTabMeta(const std::string &tab_name, const TabMeta &meta) {
        tabs_[tab_name] = meta;
    }

    /* 获取指定名称表的元数据 */
    TabMeta &get_table(const std::string &tab_name) {
        auto pos = tabs_.find(tab_name);
        if (pos == tabs_.end()) {
            throw TableNotFoundError(tab_name);
        }

        return pos->second;
    }

    // 重载操作符 <<
    friend std::ostream &operator<<(std::ostream &os, const DbMeta &db_meta) {
        os << db_meta.name_ << '\n' << db_meta.tabs_.size() << '\n';
        for (auto &entry : db_meta.tabs_) {
            os << entry.second << '\n';
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, DbMeta &db_meta) {
        size_t n;
        is >> db_meta.name_ >> n;
        for (size_t i = 0; i < n; i++) {
            TabMeta tab;
            is >> tab;
            db_meta.tabs_[tab.name] = tab;
        }
        return is;
    }
};
