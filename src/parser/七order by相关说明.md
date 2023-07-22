# order by相关说明

解析完成后的ast类型影响： 对SelectStmt进行了补充修改（主要是加了limit并且将原来支持的一个orderby改为了vector）：

```cpp
struct SelectStmt : public TreeNode {
    std::vector<std::shared_ptr<Col>> cols;
    std::vector<std::string> tabs;
    std::vector<std::shared_ptr<BinaryExpr>> conds;
    std::vector<std::shared_ptr<JoinExpr>> jointree;

    
    bool has_sort;
    //std::shared_ptr<Opt_Orders> opt_orders;
    std::vector<std::shared_ptr<OrderBy>> orders;
    int limit;                      //负数表示是没有limit, 如果非负，则就是需要的limit值

    SelectStmt(std::vector<std::shared_ptr<Col>> cols_,
               std::vector<std::string> tabs_,
               std::vector<std::shared_ptr<BinaryExpr>> conds_,
               std::pair<std::vector<std::shared_ptr<OrderBy>>, int> orders_) :
            cols(std::move(cols_)), tabs(std::move(tabs_)), conds(std::move(conds_)),
            orders(std::move(orders_.first)), limit(orders_.second) {
        if(orders.empty())
            has_sort = false;
        else
            has_sort = true;
    }
```

目前还没有改后面的，这可能会对后面有一点影响，需要修改，比如planner.cpp的TODO



OrderBy没有进行修改：

```cpp
struct OrderBy : public TreeNode
{
    std::shared_ptr<Col> cols;               //排序用的列
    OrderByDir orderby_dir;                  //desc,asc,default
    OrderBy( std::shared_ptr<Col> cols_, OrderByDir orderby_dir_) :
       cols(std::move(cols_)), orderby_dir(orderby_dir_) {}
};
```



可通过test_parser.cpp看语法树打印后的结果