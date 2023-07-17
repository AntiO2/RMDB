/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze.h"

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query 
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse)
{
    std::shared_ptr<Query> query = std::make_shared<Query>();
    if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse))
    {
        // 处理表名
        query->tables = std::move(x->tabs);
        /** TODO: 检查表是否存在 */
        //lsy
        for (auto &sv_sel_tab : query->tables){
            if(!sm_manager_->db_.is_table(sv_sel_tab))
                throw TableNotFoundError(sv_sel_tab);
        }

        // 处理target list，再target list中添加上表名，例如 a.id
        for (auto &sv_sel_col : x->cols) {
            TabCol sel_col = {.tab_name = sv_sel_col->tab_name, .col_name = sv_sel_col->col_name};
            query->cols.push_back(sel_col);
        }
        
        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);
        if (query->cols.empty()) {
            // select all columns
            for (auto &col : all_cols) {
                TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};
                query->cols.push_back(sel_col);
            }
        } else {
            // infer table name from column name
            for (auto &sel_col : query->cols) {
                sel_col = check_column(all_cols, sel_col);  // 列元数据校验
                std::cout << sel_col.tab_name << std::endl;
            }
        }
        //处理where条件
        get_clause(x->conds, query->conds,query->tables);
        check_clause(query->tables, query->conds);
    }
    else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {
        /** TODO: */
        //lsy
        //处理set_clause的list
        for(auto &set_clause : x->set_clauses)
        {
            SetClause setClause;
            setClause.lhs = {.tab_name = x->tab_name, .col_name = set_clause->col_name};
            setClause.rhs = convert_sv_value(set_clause->val);
            //支持一下类型转换
            fix_setClause(setClause,{x->tab_name});
            query->set_clauses.push_back(setClause);
        }

        //处理where条件
        get_clause(x->conds, query->conds,{x->tab_name});
        check_clause({x->tab_name}, query->conds);        //check:直接用的下面自带的deleteStmt的处理where条件，这里就不语义检查tab_name了吗?

    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
        //处理where条件
        get_clause(x->conds, query->conds,{x->tab_name});
        check_clause({x->tab_name}, query->conds);        
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
        //拿到所有col的信息
        std::vector<ColMeta> all_cols;
        get_all_cols({x->tab_name}, all_cols);

        // 处理insert 的values值
        for(int i = 0;i < x->vals.size();i++){
            Value value = convert_sv_value(x->vals[i]);
            //如果是bigint类型插入int,报错
            //CHECK(liamY)bigint应该只能插入bigint的话，可以改为assert(value.type != TYPE_BIGINT || all_cols[i].type == TYPE_BIGINT);
            if(value.type == TYPE_BIGINT && all_cols[i].type != TYPE_BIGINT) {
                throw BigintOutOfRangeError("",std::to_string(value.bigint_val));
            }
            //如果是int类型插入bigint,转换成bigint
            if(value.type == TYPE_INT && all_cols[i].type == TYPE_BIGINT){
                Value num;
                num.set_bigint(value.int_val);
                query->values.push_back(num);
            }
            else
                query->values.push_back(value);
        }

//        for (auto &sv_val : x->vals) {
//            query->values.push_back(convert_sv_value(sv_val));
//        }
    } else {
        // do nothing
    }
    query->parse = std::move(parse);
    return query;
}


TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
    if (target.tab_name.empty()) {
        // Table name not specified, infer table name from column name
        std::string tab_name;
        for (auto &col : all_cols) {
            if (col.name == target.col_name) {
                if (!tab_name.empty()) {
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
            }
        }
        if (tab_name.empty()) {
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;
    } else {
        /** TODO: Make sure target column exists */
        //lsy
        bool flag = false;
        for (auto &col : all_cols) {
            if (col.name == target.col_name && col.tab_name == target.tab_name) {
                flag = true;
                break;
            }
        }
        if(!flag)
            throw ColumnNotFoundError(target.col_name);
    }
    return target;
}

void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols) {
    for (auto &sel_tab_name : tab_names) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
}

void Analyze::fix_setClause(SetClause &setClause, const std::vector<std::string> &tab_names)
{
    //1.要先填充上列的tab_name
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);
    setClause.lhs = check_column(all_cols, setClause.lhs);
    TabMeta &lhs_tab = sm_manager_->db_.get_table(setClause.lhs.tab_name);
    auto lhs_col = lhs_tab.get_col(setClause.lhs.col_name);
    ColType lhs_type = lhs_col->type;

    //比照右值与左值列的类型,不相等则类型转换
    if(setClause.rhs.type != lhs_type)
    {
        if(lhs_type == TYPE_FLOAT)
        {
            //如果要更新的值是bigint,直接抛出异常
            if(setClause.rhs.type == TYPE_BIGINT)
                throw BigintOutOfRangeError("",std::to_string(setClause.rhs.bigint_val));

            //int转float
            if(setClause.rhs.type == TYPE_INT) {
                auto num = static_cast<float>(setClause.rhs.int_val);
                setClause.rhs.int_val = 0;
                setClause.rhs.set_float(num);
            }
            else if(setClause.rhs.type == TYPE_STRING){
                float num = std::stof(setClause.rhs.str_val);
                setClause.rhs.set_float(num);
            }
        }
        else if(lhs_type == TYPE_INT)
        {
            //如果要更新的值是bigint,直接抛出异常
            if(setClause.rhs.type == TYPE_BIGINT)
                throw BigintOutOfRangeError("",std::to_string(setClause.rhs.bigint_val));

            //float转int
            if(setClause.rhs.type == TYPE_FLOAT){
                int num = static_cast<int>(setClause.rhs.float_val);
                setClause.rhs.float_val = 0;
                setClause.rhs.set_int(num);
            }
            else if(setClause.rhs.type == TYPE_STRING){
                int num = std::stoi(setClause.rhs.str_val);
                setClause.rhs.set_int(num);
            }
        }
        else if(lhs_type == TYPE_BIGINT){
            //int转bigint
            if(setClause.rhs.type == TYPE_INT) {
                Value value;
                value.set_bigint(setClause.rhs.int_val);
                setClause.rhs = value;
            }
            //float转bigint
            if(setClause.rhs.type == TYPE_FLOAT){
                Value value;
                value.set_bigint(static_cast<int>(setClause.rhs.float_val));
                setClause.rhs = value;
            }
        }
    }
}

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds,const std::vector<std::string> &tab_names) {
    conds.clear();
    for (auto &expr : sv_conds) {
        Condition cond;

        //左边是value，与右边进行一个交换
        if (auto lhs_val = std::dynamic_pointer_cast<ast::Value>(expr->lhs)) {
            cond.is_rhs_val = true;        //交换后右边一定是value
            cond.rhs_val = convert_sv_value(lhs_val);
            //将op取反
            cond.op = find_convert_comp_op(expr->op);

            //获取变换后的lhs,check:右边应该只能是列了吧
            if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
                cond.lhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
            }
        }
        else
        {
            cond.lhs_col = {.tab_name = expr->lhs->tab_name, .col_name = expr->lhs->col_name};
            cond.op = convert_sv_comp_op(expr->op);

            if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
                cond.is_rhs_val = true;
                cond.rhs_val = convert_sv_value(rhs_val);
            } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
                cond.is_rhs_val = false;
                cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
            }
        }

        //如果cond的右值是常数，尝试转化成和左值一样的类型
        if(cond.is_rhs_val)
        {
            //1.要先填充上列的tab_name
            std::vector<ColMeta> all_cols;
            get_all_cols(tab_names, all_cols);
            cond.lhs_col = check_column(all_cols, cond.lhs_col);
            //std::cout << cond.lhs_col.tab_name << std::endl;
            TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
            auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
            ColType lhs_type = lhs_col->type;

            //比照右值与左值列的类型,不相等则类型转换
            //TODO 比如左边是int,右边是bigint的话，无法转换，但可以比较 ? 看看怎么处理好
            if(cond.rhs_val.type != lhs_type)
            {
                if(lhs_type == TYPE_FLOAT)
                {
                    //int转float
                    if(cond.rhs_val.type == TYPE_INT) {
                        auto num = static_cast<float>(cond.rhs_val.int_val);
                        cond.rhs_val.int_val = 0;
                        cond.rhs_val.set_float(num);
                    }
                    else if(cond.rhs_val.type == TYPE_STRING){
                        float num = std::stof(cond.rhs_val.str_val);
                        cond.rhs_val.set_float(num);
                    }
                }
                else if(lhs_type == TYPE_INT)
                {
                    //float转int
                    if(cond.rhs_val.type == TYPE_FLOAT){
                        int num = static_cast<int>(cond.rhs_val.float_val);
                        cond.rhs_val.float_val = 0;
                        cond.rhs_val.set_int(num);
                    }
                    else if(cond.rhs_val.type == TYPE_STRING){
                        int num = std::stoi(cond.rhs_val.str_val);
                        cond.rhs_val.set_int(num);
                    }
                }
                else if(lhs_type == TYPE_BIGINT){
                    //int转bigint
                    if(cond.rhs_val.type == TYPE_INT) {
                        Value value;
                        value.set_bigint(cond.rhs_val.int_val);
                        cond.rhs_val = value;
                    }
                    //float转bigint
                    if(cond.rhs_val.type == TYPE_FLOAT){
                        Value value;
                        value.set_bigint(static_cast<int>(cond.rhs_val.float_val));
                        cond.rhs_val = value;
                    }
                }
            }
        }

        conds.push_back(cond);
    }
}

void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds) {
    // auto all_cols = get_all_cols(tab_names);
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);
    // Get raw values in where clause
    for (auto &cond : conds) {
        // Infer table name from column name
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        if (!cond.is_rhs_val) {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            //试试直接failure
            if(lhs_col->type == TYPE_INT && cond.rhs_val.type == TYPE_BIGINT)
                throw BigintOutOfRangeError("","");

            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }
        if (lhs_type != rhs_type) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
    }
}


Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
    Value val;
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
        val.set_int(int_lit->val);
    } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
        val.set_float(float_lit->val);
    } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
        val.set_str(str_lit->val);
    } else if (auto bigint_lit = std::dynamic_pointer_cast<ast::BigintLit>(sv_val)) {
        val.set_bigint(bigint_lit->val);
    } else if (auto dateTime_lit = std::dynamic_pointer_cast<ast::DateTimeLit>(sv_val)) {
        //TODO datetime
        val.set_str(dateTime_lit->val);
    } else {
        throw InternalError("Unexpected sv value type");
    }
    return val;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
    std::map<ast::SvCompOp, CompOp> m = {
        {ast::SV_OP_EQ, OP_EQ}, {ast::SV_OP_NE, OP_NE}, {ast::SV_OP_LT, OP_LT},
        {ast::SV_OP_GT, OP_GT}, {ast::SV_OP_LE, OP_LE}, {ast::SV_OP_GE, OP_GE},
    };
    return m.at(op);
}

//获取相反的op，用于左右值列交换时变换符号
CompOp Analyze::find_convert_comp_op(ast::SvCompOp op) {
    std::map<ast::SvCompOp, CompOp> m = {
            {ast::SV_OP_EQ, OP_NE}, {ast::SV_OP_NE, OP_EQ}, {ast::SV_OP_LT, OP_GT},
            {ast::SV_OP_GT, OP_LT}, {ast::SV_OP_LE, OP_GE}, {ast::SV_OP_GE, OP_LE},
    };
    return m.at(op);
}
