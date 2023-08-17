/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);
    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    //lsy
    //1.判断是否是目录并进入
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }

    //2.加载数据库元数据和相关文件
    //lsy:模仿了create_db的代码
    // 打开一个名为DB_META_NAME的文件
    std::ifstream ofs(DB_META_NAME);

    // 读入ofs打开的DB_META_NAME文件，写入到db_
    ofs >> db_;

    //将数据库中包含的表 导入到当前fhs
    for (const auto &table: db_.tabs_) {

        auto file = rm_manager_->open_file(table.first);
        disk_manager_->set_fd2pageno(file->GetFd(), file->getFileHdr().num_pages);
        fhs_.emplace(table.first, std::move(file));
        const auto &indices = table.second.indexes;
        if (!INDEX_REBUILD_MODE) {
            for (const auto &index: table.second.indexes) {
                auto index_name = ix_manager_->get_index_name(table.first, index.cols);
                ihs_.emplace(index_name,
                             ix_manager_->open_index(index_name));
            }
        }
    }
}
/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    //lsy
    //1.将数据落盘
    flush_meta();

    //2.关闭当前fhs_的文件及退出目录
    for(auto &pair : fhs_)
        rm_manager_->close_file(pair.second.get());

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/** TODO
 * @description: 性能题目增加,停止向output.txt中写入输出结果，在未接收到该命令时，默认需要开启向output.txt中写入结果的功能
 * @param {Context*} context
 */
void SmManager::setOff(Context* context){

}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

void SmManager::show_index(const std::string& tab_name,Context* context) {
    auto index_metas = db_.get_table(tab_name).indexes;
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    RecordPrinter printer(3);
    for(auto &index:index_metas){
        outfile << "| " << tab_name << " | unique | " << "(" ;
        std::stringstream ss;
        ss << "(";
        auto it = index.cols.begin();
        outfile << (*it).name;
        ss << (*it).name;
        it++;
        for( ;it != index.cols.end();++it){
            outfile << ","<< (*it).name;
            ss << "," << (*it).name;
        }

        outfile <<") |\n";
        ss << ")";
        printer.print_record({tab_name, "unique", ss.str()}, context);
    }
}


/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    // lsy
    // 如果不存在table才需要报错
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    auto& indexes = db_.tabs_[tab_name].indexes;
    for(auto &index:indexes) {
        drop_index(tab_name, index.cols,context);
    }
    auto tab = fhs_.find(tab_name)->second.get();
    //关闭这个table文件
    rm_manager_->close_file(tab);
    buffer_pool_manager_->delete_all_pages(tab->GetFd());
    //删除db中对这个表的记录
    //tab与fhs
    fhs_.erase(tab_name);
    db_.tabs_.erase(tab_name);
    flush_meta();
    //删除表文件
    disk_manager_->destroy_file(tab_name);
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    if(!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    auto &table = db_.get_table(tab_name); // 获取table
    if(table.is_index(col_names)) {
        // 如果已经存在index，需要抛出异常
        throw IndexExistsError(tab_name, col_names);
    }
    IndexMeta indexMeta{.tab_name=tab_name};
    auto col_num = static_cast<int>(col_names.size());
    int tot_len = 0;
    std::vector<ColMeta> index_cols;
    for(const auto&col_name:col_names) {
        // 从col中找到对应名字的列
        ColMeta  col = table.get_col_meta(col_name);
        index_cols.emplace_back(col);
        tot_len+=col.len;
    }
    ix_manager_->create_index(tab_name,index_cols);
    auto index_name = ix_manager_->get_index_name(tab_name,index_cols);
    assert(ihs_.count(index_name)==0); // 确保之前没有创建过该index
    ihs_.emplace(index_name,ix_manager_->open_index(tab_name, index_cols));
    auto index_handler = ihs_.find(index_name)->second.get();
    indexMeta.col_num = col_num;
    indexMeta.col_tot_len = tot_len;
    indexMeta.cols=index_cols;
    table.indexes.emplace_back(indexMeta);

    auto table_file_handle = fhs_.find(tab_name)->second.get();
    RmScan rm_scan(table_file_handle);
    Transaction transaction(INVALID_TXN_ID); // TODO (AntiO2) 事务
    while (!rm_scan.is_end()) {
        auto rid = rm_scan.rid();
        auto origin_key = table_file_handle->get_record(rid,context);
        auto key = origin_key->key_from_rec(index_cols);
        index_handler->insert_entry(key->data,rid, &transaction);
        rm_scan.next();
    }
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    auto ix_name = ix_manager_->get_index_name(tab_name, col_names);
    if(!ix_manager_->exists(ix_name)) {
        throw IndexNotFoundError(tab_name, col_names);
    }
    auto ihs_iter  = ihs_.find(ix_name);
    if(ihs_iter==ihs_.end()) {
        throw IndexNotFoundError(tab_name, col_names);
    }

    buffer_pool_manager_->delete_all_pages(ihs_iter->second->getFd());
    ix_manager_->close_index(ihs_iter->second.get());
    ix_manager_->destroy_index(ix_name); // 删除索引文件
    ihs_.erase(ihs_iter); // check(AntiO2) 此处删除迭代器是否有错

    db_.tabs_[tab_name].remove_index(col_names);

    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    std::vector<std::string> col_name;
    for(const auto& col :cols) {
        col_name.emplace_back(col.name);
    }
    drop_index(tab_name,col_name,context);
}

void SmManager::rebuild_index(const std::string &tab_name, const IndexMeta&index_meta, Context *context) {
    const auto&index_cols = index_meta.cols;
    std::vector<std::string> col_names;
    for(auto&col:index_meta.cols) {
        col_names.emplace_back(col.name);
    }
    auto ix_name = IxManager::get_index_name(tab_name, col_names);
    disk_manager_->reset_file(ix_name);
    int fd = disk_manager_->open_file(ix_name);
    int col_tot_len = 0;
    int col_num = index_cols.size();
    for(auto& col: index_cols) {
        col_tot_len += col.len;
    }
    if (col_tot_len > IX_MAX_COL_LEN) {
        throw InvalidColLengthError(col_tot_len);
    }
    // 根据 |page_hdr| + (|attr| + |rid|) * (n + 1) <= PAGE_SIZE 求得n的最大值btree_order
    // 即 n <= btree_order，那么btree_order就是每个结点最多可插入的键值对数量（实际还多留了一个空位，但其不可插入）
    // Key: index cols
    // Value: RID
    int btree_order = static_cast<int>((PAGE_SIZE - sizeof(IxPageHdr)) / (col_tot_len + sizeof(Rid)) - 1);
    assert(btree_order > 2);

    // Create file header and write to file
    IxFileHdr* fhdr = new IxFileHdr(IX_NO_PAGE, IX_INIT_NUM_PAGES, IX_INIT_ROOT_PAGE,
                                    col_num, col_tot_len, btree_order, (btree_order + 1) * col_tot_len, // 在这里初始化最大值
                                    IX_INIT_ROOT_PAGE, IX_INIT_ROOT_PAGE);
    for(int i = 0; i < col_num; ++i) {
        fhdr->col_types_.push_back(index_cols[i].type);
        fhdr->col_lens_.push_back(index_cols[i].len);
    }
    fhdr->update_tot_len();

    char* data = new char[fhdr->tot_len_];
    fhdr->serialize(data); // 将fhdr的数据结构化，存储到data中

    disk_manager_->write_page(fd, IX_FILE_HDR_PAGE, data, fhdr->tot_len_); // 将索引数据写到索引文件的第0页中（header page）

    char page_buf[PAGE_SIZE];  // 在内存中初始化page_buf中的内容，然后将其写入磁盘
    memset(page_buf, 0, PAGE_SIZE);
    // 注意leaf header页号为1，也标记为叶子结点，其前一个/后一个叶子均指向root node
    // Create leaf list header page and write to file
    {
        memset(page_buf, 0, PAGE_SIZE);
        auto phdr = reinterpret_cast<IxPageHdr *>(page_buf);
        *phdr = {
                .next_free_page_no = IX_NO_PAGE,
                .parent = IX_NO_PAGE,
                .num_key = 0,
                .is_leaf = true,
                .prev_leaf = IX_INIT_ROOT_PAGE,
                .next_leaf = IX_INIT_ROOT_PAGE,
        };
        disk_manager_->write_page(fd, IX_LEAF_HEADER_PAGE, page_buf, PAGE_SIZE);
    }
    // 注意root node页号为2，也标记为叶子结点，其前一个/后一个叶子均指向leaf header
    // Create root node and write to file
    {
        memset(page_buf, 0, PAGE_SIZE);
        auto phdr = reinterpret_cast<IxPageHdr *>(page_buf);
        *phdr = {
                .next_free_page_no = IX_NO_PAGE,
                .parent = IX_NO_PAGE,
                .num_key = 0,
                .is_leaf = true,
                .prev_leaf = IX_LEAF_HEADER_PAGE,
                .next_leaf = IX_LEAF_HEADER_PAGE,
        };
        // Must write PAGE_SIZE here in case of future fetch_node()
        disk_manager_->write_page(fd, IX_INIT_ROOT_PAGE, page_buf, PAGE_SIZE);
    }

    disk_manager_->set_fd2pageno(fd, IX_INIT_NUM_PAGES - 1);  // DEBUG

//    // Close index file
//    disk_manager_->close_file(fd);
    const auto&index_name = ix_name;
    assert(ihs_.count(index_name)==0); // 确保之前没有创建过该index
    ihs_.emplace(index_name, std::make_unique<IxIndexHandle>(disk_manager_, buffer_pool_manager_, fd));
    auto index_handler = ihs_.find(index_name)->second.get();
    auto table_file_handle = fhs_.find(tab_name)->second.get();
    RmScan rm_scan(table_file_handle);
    Transaction transaction(INVALID_TXN_ID);
    while (!rm_scan.is_end()) {
        auto rid = rm_scan.rid();
        auto origin_key = table_file_handle->get_record(rid,context);
        auto key = origin_key->key_from_rec(index_cols);
        index_handler->insert_entry(key->data,rid, &transaction);
        rm_scan.next();
    }
}

//load lsy
/**  TODO
 * @description: load data
 * @param {string} 要读取的文件名
 * @param {string} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::load_csv(std::string file_name,std::string tab_name,Context* context){
    //思路是借鉴insert的方式，对csv里面的每一行进行insert
    //1.打开文件
    std::ifstream infile;
    infile.open(file_name);
    if(!infile.is_open()){
        throw UnixError();
    }
    //2.读取文件 第一行 是每列的列名，舍弃
    std::string line;
    std::vector<std::string> values;
    std::getline(infile,line);
    //3.对每一行进行insert
    //去拿sm_manager
    extern std::unique_ptr<SmManager>  sm_manager;
    auto sm_manager_ = sm_manager.get();
    while(std::getline(infile,line)){
        //将values清空
        values.clear();
        std::stringstream ss(line);
        std::string value;
        while(std::getline(ss,value,',')){
            values.push_back(value);
        }

        //这个时候values里面就是装有每一行的所有数据了，按照从左到右的顺序，先模仿portal的操作，对insert进行初始化
//        if(context->txn_->get_isolation_level()==IsolationLevel::SERIALIZABLE) {
//            auto fd = sm_manager->fhs_.at(tab_name).get()->GetFd();
//            context->lock_mgr_->lock_exclusive_on_table(context->txn_,fd);
//        }不需要加锁
        auto fh_ = sm_manager_->fhs_.at(tab_name).get();
        auto tab_ = sm_manager_->db_.get_table(tab_name);
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        //根据table里面的数据得到各个列元素的类型，从而把values数据转化为std::vector<Value> values_
        std::vector<Value> values_;
        for (size_t i = 0; i < values.size(); i++) {//遍历一行的数据
            auto &col = tab_.cols[i];
            auto &val = values[i];
            Value value_;//根据col来初始化value
            if(col.type==ColType::TYPE_INT) {
                value_.set_int(std::stoi(val));//将string转化为int
            }else if(col.type==ColType::TYPE_FLOAT) {
                value_.set_float(std::stof(val));//将string转化为float
            }else if(col.type==ColType::TYPE_STRING) {
                value_.set_str(val);//将string转化为string
            }else if(col.type==ColType::TYPE_BIGINT) {
                value_.set_bigint(val);//将string转化为bigint
            }else if(col.type==TYPE_DATETIME){
                value_.set_datetime(val);
            }
            values_.push_back(value_);//将value_放入values_中
        }
        //将values_中的数据存入rec中
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            val.init_raw(col.len);
            // 将Value数据存入rec中。
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }
        auto undo_next = context->txn_->get_transaction_id();
        // Insert into record file
        auto rid_ = fh_->insert_record(rec.data, context,&tab_name);
        auto* writeRecord = new WriteRecord(WType::INSERT_TUPLE,tab_name,rid_, undo_next);
        context->txn_->append_write_record(writeRecord);
    }
}
