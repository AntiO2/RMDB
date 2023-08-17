//
// Created by Anti on 2023/7/17.
//


#include <algorithm>
#include <cstdio>
#include <random>
#include "fmt/core.h"
#include "logger.h"
#include "gtest/gtest.h"
#define private public
#include "system/sm.h"
#include "index/ix.h"
#undef private
const std::string TEST_DB_NAME = "test_db";
const std::string TEST_FILE_NAME = "table1";
const int POOL_SIZE = 1000;
class IndexTest_2COL : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<IxIndexHandle> ih_;
    std::unique_ptr<Transaction> txn_;
    std::vector<ColDef> col_defs;
    std::vector<ColMeta> cols; //用于创建索引的列
public:

    void SetUp() override {
        LOG_INFO("索引初始化开始");
        disk_manager_ = std::make_unique<DiskManager>();
        log_manager_ = std::make_unique<LogManager>(disk_manager_.get());
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(POOL_SIZE, disk_manager_.get(), log_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        txn_ = std::make_unique<Transaction>(0);
        int curr_offset = 0;

        // 创建两列索引
        ColDef colA{.name="A",.type=ColType::TYPE_INT,.len=col2len(ColType::TYPE_INT)};
        ColDef colB{.name="B",.type=ColType::TYPE_STRING,.len=8}; // 第二列为字符串

        col_defs.emplace_back(colA);
        col_defs.emplace_back(colB);

        for (auto &col_def : col_defs) {
            ColMeta col = {.tab_name = TEST_FILE_NAME,
                    .name = col_def.name,
                    .type = col_def.type,
                    .len = col_def.len,
                    .offset = curr_offset,
                    .index = false};
            curr_offset += col_def.len;
            cols.emplace_back(col);
        }
        if(disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->destroy_dir(TEST_DB_NAME); // 先删除之前的数据
        }
        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
        if (ix_manager_->exists(TEST_FILE_NAME, cols)) {
            ix_manager_->destroy_index(TEST_FILE_NAME, cols);
        }

        ix_manager_->create_index(TEST_FILE_NAME, cols); // 创建索引

        assert(ix_manager_->exists(TEST_FILE_NAME, cols));
        ih_ = ix_manager_->open_index(TEST_FILE_NAME, cols);
        assert(ih_ != nullptr);

        LOG_INFO("索引初始化完成");
    }

    void TearDown() override {
        ix_manager_->close_index(ih_.get());
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
        // disk_manager_->destroy_dir(TEST_DB_NAME); // 删除测试文件
        LOG_INFO("索引已删除");
    };
    void to_graph(const IxIndexHandle *ih, IxNodeHandle *node, BufferPoolManager *bpm, std::ofstream &out) const {
        std::string leaf_prefix("LEAF_");
        std::string internal_prefix("INT_");
        if (node->is_leaf_page()) {
            IxNodeHandle *leaf = node;
            out << leaf_prefix << leaf->get_page_no();
            out << "[shape=plain color=red ";
            out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
            out << "<TR><TD COLSPAN=\"" << leaf->get_size() << "\">page_no=" << leaf->get_page_no() << "</TD></TR>\n";
            out << "<TR><TD COLSPAN=\"" << leaf->get_size() << "\">"
                << "max_size=" << leaf->get_max_size() << ",min_size=" << leaf->get_min_size() << "</TD></TR>\n";
            out << "<TR>";
            for (int i = 0; i < leaf->get_size(); i++) {
                out << "<TD> (" << leaf->key_at(i) << ") </TD>\n";
            }
            out << "</TR>";
            // Print table end
            out << "</TABLE>>];\n";
            // Print Leaf node link if there is a next page
            if (leaf->get_next_leaf() != INVALID_PAGE_ID && leaf->get_next_leaf() > 1) {
                out << leaf_prefix << leaf->get_page_no() << " -> " << leaf_prefix << leaf->get_next_leaf() << ";\n";
                out << "{rank=same " << leaf_prefix << leaf->get_page_no() << " " << leaf_prefix << leaf->get_next_leaf()
                    << "};\n";
            }

            // Print parent links if there is a parent
            if (leaf->get_parent_page_no() != INVALID_PAGE_ID) {
                out << internal_prefix << leaf->get_parent_page_no() << ":p" << leaf->get_page_no() << " -> " << leaf_prefix
                    << leaf->get_page_no() << ";\n";
            }
        } else {
            IxNodeHandle *inner = node;
            // Print node name
            out << internal_prefix << inner->get_page_no();
            // Print node properties
            out << "[shape=plain color=blue ";
            // Print data of the node
            out << "label=<<TABLE BORDER=\"1\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
            // Print data
            out << "<TR><TD COLSPAN=\"" << inner->get_size() << "\">page_no=" << inner->get_page_no()<< "</TD></TR>\n";
            out << "<TR><TD COLSPAN=\"" << inner->get_size() << "\">"
                << "max_size=" << inner->get_max_size() << ",min_size=" << inner->get_min_size() << "</TD></TR>\n";
            out << "<TR>";
            for (int i = 0; i < inner->get_size(); i++) {
                out << "<TD PORT=\"p" << inner->value_at(i) << "\">";
                out << "("<<inner->key_at(i)<<")";
                out << "</TD>\n";
            }
            out << "</TR>";
            out << "</TABLE>>];\n";
            if (inner->get_parent_page_no() != INVALID_PAGE_ID) {
                out << internal_prefix << inner->get_parent_page_no() << ":p" << inner->get_page_no() << " -> "
                    << internal_prefix << inner->get_page_no() << ";\n";
            }
            for (int i = 0; i < inner->get_size(); i++) {
                IxNodeHandle *child_node = ih->fetch_node(inner->value_at(i));
                to_graph(ih, child_node, bpm, out);
                if (i > 0) {
                    IxNodeHandle *sibling_node = ih->fetch_node(inner->value_at(i - 1));
                    if (!sibling_node->is_leaf_page() && !child_node->is_leaf_page()) {
                        out << "{rank=same " << internal_prefix << sibling_node->get_page_no() << " " << internal_prefix
                            << child_node->get_page_no() << "};\n";
                    }
                    bpm->unpin_page(sibling_node->get_page_id(), false);
                }
            }
        }
        bpm->unpin_page(node->get_page_id(), false);
    }
    void draw(BufferPoolManager *bpm, const std::string &outf) const {
        std::ofstream out(outf);
        out << "digraph G {" << std::endl;
        IxNodeHandle *node = ih_->fetch_node(ih_->file_hdr_->root_page_);
        to_graph(ih_.get(), node, bpm, out);
        out << "}" << std::endl;
        out.close();
        std::string prefix = outf;
        prefix.replace(outf.rfind(".dot"), 4, "");
        std::string png_name = prefix + ".png";
        std::string cmd = "dot -Tpng " + outf + " -o " + png_name;
        system(cmd.c_str());
    }
};

/**
 * 联合索引测试
 *
 */
TEST_F(IndexTest_2COL, InitTest) {
    LOG_INFO("Init test");
    const int64_t scale = 10;
    const int order = 6;

    assert(order > 2 && order <= ih_->file_hdr_->btree_order_);
    ih_->file_hdr_->btree_order_ = order;

    std::vector<int64_t> keys_a;
    for (int64_t key = 1; key <= scale; key++) {
        keys_a.push_back(key);
    }
    std::vector<int64_t> keys_b;
    for (int64_t key = 1; key <= scale; key++) {
        keys_b.push_back(key);
    }
    for (auto key_a : keys_a) {
        for(auto key_b : keys_b) {
            int value_a = key_a & 0xFFFFFFFF;
            int value_b = key_b & 0xFFFFFFFF;
            Rid rid = {.page_no = static_cast<int32_t>(key_a >> 32),
                    .slot_no = static_cast<int>(value_a*scale+value_b)};
            auto index_key = new char[8];
            memcpy(index_key,(const char*)&value_a,4);
            memcpy(index_key + 4,(const char*)&value_b,4);
            bool insert_ret = ih_->insert_entry(index_key, rid, txn_.get());  // 调用Insert
            ASSERT_EQ(insert_ret, true);
            // LOG_DEBUG("%s", fmt::format("Insert {} {}",key_a, key_b).c_str());
        }
    }

    draw(buffer_pool_manager_.get(),"a.dot");
    for (auto key_a : keys_a) {
        // 查找第一列
        LOG_DEBUG("%ld",key_a);
        int value_a = key_a & 0xFFFFFFFF;
        auto iid_begin = ih_->lower_bound_cnt((const char*)&value_a,1);
        auto iid_end = ih_->upper_bound_cnt((const char*)&value_a,1);
        IxScan scan_(ih_.get(),iid_begin,iid_end,buffer_pool_manager_.get());
        while(!scan_.is_end()) {
            auto rid = scan_.rid();
            LOG_DEBUG("%s", fmt::format("page {} slot{}",rid.page_no,rid.slot_no).c_str());
            scan_.next();
        }

    }
}
TEST_F(IndexTest_2COL, INTANDSTRING) {
    LOG_INFO("Init test");
    const int64_t scale = 3000;
    const int order = 10;

    assert(order > 2 && order <= ih_->file_hdr_->btree_order_);
    // ih_->file_hdr_->btree_order_ = order;

    std::vector<int64_t> keys_a;
//    for (int64_t key = 1; key <= scale; key++) {
//        keys_a.push_back(key);
//    }
    keys_a={10,534,500};
    std::vector<const char *> keys_b;
    for (int64_t key = 1; key <= scale; key++) {
        keys_b.push_back(std::to_string(key*10000+key).c_str());
    }
    for (int i = 0; i< keys_a.size();i++) {
        auto key_a = keys_a[i];
            auto key_b = keys_b[i];
            int value_a = key_a & 0xFFFFFFFF;
            Rid rid = {.page_no = static_cast<int32_t>(0),
                    .slot_no = static_cast<int>(value_a)};
            auto index_key = new char[8];
            memcpy(index_key,(const char*)&value_a,4);
            memcpy(index_key + 4, key_b, 8);
            bool insert_ret = ih_->insert_entry(index_key, rid, txn_.get());  // 调用Insert
            ASSERT_EQ(insert_ret, true);
            LOG_DEBUG("%s", fmt::format("Insert {}",key_a).c_str());
            // draw(buffer_pool_manager_.get(),fmt::format("Insert{}.dot",key_a).c_str());
    }

    draw(buffer_pool_manager_.get(),"a.dot");
    for (auto key_a : keys_a) {
        // 查找第一列
        LOG_DEBUG("%ld",key_a);
        int value_a = key_a & 0xFFFFFFFF;
        auto iid_begin = ih_->lower_bound_cnt((const char*)&value_a,1);
        auto iid_end = ih_->upper_bound_cnt((const char*)&value_a,1);
        IxScan scan_(ih_.get(),iid_begin,iid_end,buffer_pool_manager_.get());
        while(!scan_.is_end()) {
            auto rid = scan_.rid();
            LOG_DEBUG("%s", fmt::format("page {} slot{}",rid.page_no,rid.slot_no).c_str());
            scan_.next();
        }
    }
}
/**
 * 测试点2
 *
 */
TEST_F(IndexTest_2COL, UPDATE) {
    LOG_INFO("Init test");
    const int64_t scale = 100;
    const int order = 10;

    assert(order > 2 && order <= ih_->file_hdr_->btree_order_);
    ih_->file_hdr_->btree_order_ = order;

    std::vector<int64_t> keys_a;
    for (int64_t key = 1; key <= scale; key++) {
        keys_a.push_back(key);
    }
    // keys_a={10,534,500};
    std::vector<char *> keys_b;
    for (int64_t key = 0; key < scale; key++) {
        keys_b.emplace_back(new char[4]);
        memcpy(keys_b.at(key),std::to_string(key*1000+key).c_str(), 8 );
    }
    for (int i = 0; i < 100;i++) {
        auto key_a = keys_a[i];
        auto key_b = keys_b[i];
        int value_a = key_a & 0xFFFFFFFF;
        Rid rid = {.page_no = static_cast<int32_t>(0),
                .slot_no = static_cast<int>(value_a)};
        auto index_key = new char[8];
        memcpy(index_key,(const char*)&value_a,4);
        memcpy(index_key + 4, key_b, 8);
        bool insert_ret = ih_->insert_entry(index_key, rid, txn_.get());  // 调用Insert
        ASSERT_EQ(insert_ret, true);
        LOG_DEBUG("%s", fmt::format("Insert {}",key_a).c_str());
        // draw(buffer_pool_manager_.get(),fmt::format("Insert{}.dot",key_a).c_str());
    }
    draw(buffer_pool_manager_.get(),"insert.dot");
    for (auto key_a : keys_a) {
        // 查找第一列
        LOG_DEBUG("%ld",key_a);
        int value_a = key_a & 0xFFFFFFFF;
        auto iid_begin = ih_->lower_bound_cnt((const char*)&value_a,1);
        auto iid_end = ih_->upper_bound_cnt((const char*)&value_a,1);
        IxScan scan_(ih_.get(),iid_begin,iid_end,buffer_pool_manager_.get());
        while(!scan_.is_end()) {
            auto rid = scan_.rid();
            LOG_DEBUG("%s", fmt::format("page {} slot{}",rid.page_no,rid.slot_no).c_str());
            scan_.next();
        }
    }
    /**
     * 删除所有奇数
     *
     */
    for (int i = 0; i < 100;i +=2 ) {
        auto key_a = keys_a[i];
        auto key_b = keys_b[i];
        int value_a = key_a & 0xFFFFFFFF;
        Rid rid = {.page_no = static_cast<int32_t>(0),
                .slot_no = static_cast<int>(value_a)};
        auto index_key = new char[8];
        memcpy(index_key,(const char*)&value_a,4);
        memcpy(index_key + 4, key_b, 8);
        bool delete_ret = ih_->delete_entry(index_key, txn_.get());  // 调用Insert
        ASSERT_EQ(delete_ret, true);
        LOG_DEBUG("%s", fmt::format("delete {}",key_a).c_str());
        draw(buffer_pool_manager_.get(),fmt::format("delete{}.dot",key_a).c_str());
    }
    draw(buffer_pool_manager_.get(),"delete.dot");
    for (int i = 1; i < 100;i +=2 ) {
        auto key_a = keys_a.at(i);
        int value_a = key_a & 0xFFFFFFFF;
        auto iid_begin = ih_->lower_bound_cnt((const char*)&value_a,1);
        auto iid_end = ih_->upper_bound_cnt((const char*)&value_a,1);
        IxScan scan_(ih_.get(),iid_begin,iid_end,buffer_pool_manager_.get());
        while(!scan_.is_end()) {
            auto rid = scan_.rid();
            LOG_DEBUG("%s", fmt::format("page {} slot{}",rid.page_no,rid.slot_no).c_str());
            scan_.next();
        }
    }
    // 再把偶数插入回来
    for (int i = 0; i < 100;i+=2) {
        auto key_a = keys_a[i];
        auto key_b = keys_b[i];
        int value_a = key_a & 0xFFFFFFFF;
        Rid rid = {.page_no = static_cast<int32_t>(0),
                .slot_no = static_cast<int>(value_a)};
        auto index_key = new char[8];
        memcpy(index_key,(const char*)&value_a,4);
        memcpy(index_key + 4, key_b, 8);
        bool insert_ret = ih_->insert_entry(index_key, rid, txn_.get());  // 调用Insert
        ASSERT_EQ(insert_ret, true);
        LOG_DEBUG("%s", fmt::format("Insert {}",key_a).c_str());
        // draw(buffer_pool_manager_.get(),fmt::format("Insert{}.dot",key_a).c_str());
    }
    draw(buffer_pool_manager_.get(),"insert2.dot");
    for (auto key_a : keys_a) {
        // 查找第一列
        LOG_DEBUG("%ld",key_a);
        int value_a = key_a & 0xFFFFFFFF;
        auto iid_begin = ih_->lower_bound_cnt((const char*)&value_a,1);
        auto iid_end = ih_->upper_bound_cnt((const char*)&value_a,1);
        IxScan scan_(ih_.get(),iid_begin,iid_end,buffer_pool_manager_.get());
        while(!scan_.is_end()) {
            auto rid = scan_.rid();
            LOG_DEBUG("%s", fmt::format("page {} slot{}",rid.page_no,rid.slot_no).c_str());
            scan_.next();
        }
    }
}

class IndexTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<IxIndexHandle> ih_;
    std::unique_ptr<Transaction> txn_;
    std::vector<ColDef> col_defs;
    std::unique_ptr<LogManager> log_manager_;
    std::vector<ColMeta> cols; //用于创建索引的列
public:

    void SetUp() override {
        LOG_INFO("索引初始化开始");
        disk_manager_ = std::make_unique<DiskManager>();
        log_manager_ = std::make_unique<LogManager>(disk_manager_.get());
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(POOL_SIZE, disk_manager_.get(), log_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        txn_ = std::make_unique<Transaction>(0);
        int curr_offset = 0;

        ColDef colA{.name="A",.type=ColType::TYPE_INT,.len=col2len(ColType::TYPE_INT)};
        col_defs.emplace_back(colA);

        for (auto &col_def : col_defs) {
            ColMeta col = {.tab_name = TEST_FILE_NAME,
                    .name = col_def.name,
                    .type = col_def.type,
                    .len = col_def.len,
                    .offset = curr_offset,
                    .index = false};
            curr_offset += col_def.len;
            cols.emplace_back(col);
        }
        if(disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->destroy_dir(TEST_DB_NAME); // 先删除之前的数据
        }
        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
        if (ix_manager_->exists(TEST_FILE_NAME, cols)) {
            ix_manager_->destroy_index(TEST_FILE_NAME, cols);
        }

        ix_manager_->create_index(TEST_FILE_NAME, cols);

        assert(ix_manager_->exists(TEST_FILE_NAME, cols));
        ih_ = ix_manager_->open_index(TEST_FILE_NAME, cols);
        assert(ih_ != nullptr);

        LOG_INFO("索引初始化完成");
    }

    void TearDown() override {
        ix_manager_->close_index(ih_.get());
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
        // disk_manager_->destroy_dir(TEST_DB_NAME); // 删除测试文件
        LOG_INFO("索引已删除");
    };
    /**
     * @reference bustub
     *
     */
    void to_graph(const IxIndexHandle *ih, IxNodeHandle *node, BufferPoolManager *bpm, std::ofstream &out) const {
        std::string leaf_prefix("LEAF_");
        std::string internal_prefix("INT_");
        if (node->is_leaf_page()) {
            IxNodeHandle *leaf = node;
            out << leaf_prefix << leaf->get_page_no();
            out << "[shape=plain color=green ";
            out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
            out << "<TR><TD COLSPAN=\"" << leaf->get_size() << "\">page_no=" << leaf->get_page_no() << "</TD></TR>\n";
            out << "<TR><TD COLSPAN=\"" << leaf->get_size() << "\">"
                << "max_size=" << leaf->get_max_size() << ",min_size=" << leaf->get_min_size() <<" num="<<leaf->get_size()<< "</TD></TR>\n";
            out << "<TR>";
            for (int i = 0; i < leaf->get_size(); i++) {
                out << "<TD>" << leaf->key_at(i) << "</TD>\n";
            }
            out << "</TR>";
            // Print table end
            out << "</TABLE>>];\n";
            // Print Leaf node link if there is a next page
            if (leaf->get_next_leaf() != INVALID_PAGE_ID && leaf->get_next_leaf() > 1) {
                // 注意加上一个大于1的判断条件，否则若GetNextPageNo()是1，会把1那个结点也画出来
                out << leaf_prefix << leaf->get_page_no() << " -> " << leaf_prefix << leaf->get_next_leaf() << ";\n";
                out << "{rank=same " << leaf_prefix << leaf->get_page_no() << " " << leaf_prefix << leaf->get_next_leaf()
                    << "};\n";
            }

            // Print parent links if there is a parent
            if (leaf->get_parent_page_no() != INVALID_PAGE_ID) {
                out << internal_prefix << leaf->get_parent_page_no() << ":p" << leaf->get_page_no() << " -> " << leaf_prefix
                    << leaf->get_page_no() << ";\n";
            }
        } else {
            IxNodeHandle *inner = node;
            // Print node name
            out << internal_prefix << inner->get_page_no();
            // Print node properties
            out << "[shape=plain color=pink ";  // why not?
            // Print data of the node
            out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
            // Print data
            out << "<TR><TD COLSPAN=\"" << inner->get_size() << "\">page_no=" << inner->get_page_no()<< "</TD></TR>\n";
            out << "<TR><TD COLSPAN=\"" << inner->get_size() << "\">"
                << "max_size=" << inner->get_max_size() << ",min_size=" << inner->get_min_size() <<" num="<<inner->get_size()<<"</TD></TR>\n";
            out << "<TR>";
            for (int i = 0; i < inner->get_size(); i++) {
                out << "<TD PORT=\"p" << inner->value_at(i) << "\">";
                out << inner->key_at(i);
                out << "</TD>\n";
            }
            out << "</TR>";
            out << "</TABLE>>];\n";
            if (inner->get_parent_page_no() != INVALID_PAGE_ID) {
                out << internal_prefix << inner->get_parent_page_no() << ":p" << inner->get_page_no() << " -> "
                    << internal_prefix << inner->get_page_no() << ";\n";
            }
            for (int i = 0; i < inner->get_size(); i++) {
                IxNodeHandle *child_node = ih->fetch_node(inner->value_at(i));
                to_graph(ih, child_node, bpm, out);
                if (i > 0) {
                    IxNodeHandle *sibling_node = ih->fetch_node(inner->value_at(i - 1));
                    if (!sibling_node->is_leaf_page() && !child_node->is_leaf_page()) {
                        out << "{rank=same " << internal_prefix << sibling_node->get_page_no() << " " << internal_prefix
                            << child_node->get_page_no() << "};\n";
                    }
                    bpm->unpin_page(sibling_node->get_page_id(), false);
                }
            }
        }
        bpm->unpin_page(node->get_page_id(), false);
    }
    void draw(BufferPoolManager *bpm, const std::string &outf) const {
        std::ofstream out(outf);
        out << "digraph G {" << std::endl;
        IxNodeHandle *node = ih_->fetch_node(ih_->file_hdr_->root_page_);
        to_graph(ih_.get(), node, bpm, out);
        out << "}" << std::endl;
        out.close();

        // 由dot文件生成png文件
        std::string prefix = outf;
        prefix.replace(outf.rfind(".dot"), 4, "");
        std::string png_name = prefix + ".png";
        std::string cmd = "dot -Tpng " + outf + " -o " + png_name;
        system(cmd.c_str());
    }
    void check_leaf(const IxIndexHandle *ih) {
        // check leaf list
        page_id_t leaf_no = ih->file_hdr_->first_leaf_;
        while (leaf_no != IX_LEAF_HEADER_PAGE) {
            IxNodeHandle *curr = ih->fetch_node(leaf_no);
            IxNodeHandle *prev = ih->fetch_node(curr->get_prev_leaf());
            IxNodeHandle *next = ih->fetch_node(curr->get_next_leaf());
            // Ensure prev->next == curr && next->prev == curr
            ASSERT_EQ(prev->get_next_leaf(), leaf_no);
            ASSERT_EQ(next->get_prev_leaf(), leaf_no);
            leaf_no = curr->get_next_leaf();
            buffer_pool_manager_->unpin_page(curr->get_page_id(), false);
            buffer_pool_manager_->unpin_page(prev->get_page_id(), false);
            buffer_pool_manager_->unpin_page(next->get_page_id(), false);
        }
    }

    void check_tree(const IxIndexHandle *ih, int now_page_no) {
        IxNodeHandle *node = ih->fetch_node(now_page_no);
        if (node->is_leaf_page()) {
            buffer_pool_manager_->unpin_page(node->get_page_id(), false);
            return;
        }
        for (int i = 0; i < node->get_size(); i++) {                 // 遍历node的所有孩子
            IxNodeHandle *child = ih->fetch_node(node->value_at(i));  // 第i个孩子
            // check parent
            assert(child->get_parent_page_no() == now_page_no);
            // check first key
            int node_key = node->key_at(i);  // node的第i个key
            int child_first_key = child->key_at(0);
            int child_last_key = child->key_at(child->get_size() - 1);
            if (i != 0) {
                // 除了第0个key之外，node的第i个key与其第i个孩子的第0个key的值相同
                ASSERT_EQ(node_key, child_first_key);
            }
            if (i + 1 < node->get_size()) {
                // 满足制约大小关系
                ASSERT_LT(child_last_key, node->key_at(i + 1));  // child_last_key < node->key_at(i + 1)
            }

            buffer_pool_manager_->unpin_page(child->get_page_id(), false);

            check_tree(ih, node->value_at(i));  // 递归子树
        }
        buffer_pool_manager_->unpin_page(node->get_page_id(), false);
    }
    void check_all(IxIndexHandle *ih, const std::multimap<int, Rid> &mock) {
        check_tree(ih, ih->file_hdr_->root_page_);
        if (!ih->is_empty()) {
            check_leaf(ih);
        }

        for (auto &entry : mock) {
            int mock_key = entry.first;
            LOG_DEBUG("Mock Key %d",mock_key);
            // test lower bound
            {
                auto mock_lower = mock.lower_bound(mock_key);        // multimap的lower_bound方法
                Iid iid = ih->lower_bound((const char *)&mock_key);  // IxIndexHandle的lower_bound方法

                // iid = ih->upper_bound((const char *)&mock_key);
                Rid rid = ih->get_rid(iid);
                EXPECT_EQ(rid, mock_lower->second);
            }
            // test upper bound
            {
                auto mock_upper = mock.upper_bound(mock_key);
                auto iid = ih->upper_bound((const char *)&mock_key);
                if (iid != ih->leaf_end()) {
                    Rid rid = ih->get_rid(iid);
                    ASSERT_EQ(rid, mock_upper->second);
                }
            }
        }

        // test scan
        IxScan scan(ih, ih->leaf_begin(), ih->leaf_end(), buffer_pool_manager_.get());
        auto it = mock.begin();
        int leaf_no = ih->file_hdr_->first_leaf_;
        assert(leaf_no == scan.iid().page_no);
        // 注意在scan里面是iid的slot_no进行自增
        while (!scan.is_end() && it != mock.end()) {
            Rid mock_rid = it->second;
            Rid rid = scan.rid();
            ASSERT_EQ(rid, mock_rid);
            // go to next slot_no
            it++;
            scan.next();
        }
        ASSERT_EQ(scan.is_end(), true);
        ASSERT_EQ(it, mock.end());
    }
};
TEST(UPPERBOUND,SIMPLE_TEST) {
    // 测试二分查找正确性。
    int num = 10;
    std::vector<int> a ;
    for(int i = 1; i <= num;i++) {
        a.emplace_back(i);
    }
    ASSERT_EQ(a.size(),num);
    int l = 0, r = num, mid, flag;
    int target = 4;
    while(l < r){ //use binary search
        mid = (l+r)/2;
        flag = a[mid] < target?(-1):(a[mid]>target?1:0);
        if(flag <= 0)
            l = mid + 1;
        else
            r = mid;
    }
    EXPECT_EQ(a[l], 5);

    target = num;
    l = 0;
    r = num;
    while(l < r){ //use binary search
        mid = (l+r)/2;
        flag = a[mid] < target?(-1):(a[mid]>target?1:0);
        LOG_DEBUG("%s", fmt::format("l {} r {} mid {} mid_value {}",l,r,mid,a[mid]).c_str());
        if(flag <= 0)
            l = mid + 1;
        else
            r = mid;
    }
    LOG_DEBUG("%d",l);
    EXPECT_EQ(l,num);
}

/**
 * 初始化索引测试
 *
 */
TEST_F(IndexTest, InitTest) {
    LOG_INFO("Init test");
}


TEST_F(IndexTest, InsertTest) {
    const int64_t scale = 10;
    const int order = 3;

    assert(order > 2 && order <= ih_->file_hdr_->btree_order_);
    ih_->file_hdr_->btree_order_ = order;

    std::vector<int64_t> keys;
    for (int64_t key = 1; key <= scale; key++) {
        keys.push_back(key);
    }

    const char *index_key;
    for (auto key : keys) {
        int32_t value = key & 0xFFFFFFFF;
        Rid rid = {.page_no = static_cast<int32_t>(key >> 32),
                .slot_no = value};
        index_key = (const char *)&key;
        bool insert_ret = ih_->insert_entry(index_key, rid, txn_.get());  // 调用Insert
        ASSERT_EQ(insert_ret, true);
        LOG_DEBUG("%s", fmt::format("Insert {}",key).c_str());
        draw(buffer_pool_manager_.get(), "insert" + std::to_string(key) + ".dot");
    }

    std::vector<Rid> rids;
    for (auto key : keys) {
        rids.clear();
        index_key = (const char *)&key;
        ih_->get_value(index_key, &rids, txn_.get());
        LOG_DEBUG("%s", fmt::format("Search {}",key).c_str());
        EXPECT_EQ(rids.size(), 1);

        int32_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].slot_no, value);
    }

    for (int key = scale + 1; key <= scale + 100; key++) {
        rids.clear();
        index_key = (const char *)&key;
        ih_->get_value(index_key, &rids, txn_.get());
        EXPECT_EQ(rids.size(), 0);
    }
}
TEST_F(IndexTest, LargeScaleTest) {
    // 测试大量数据插入
    const int64_t scale = 10000;
    const int order = 256;

    assert(order > 2 && order <= ih_->file_hdr_->btree_order_);
    ih_->file_hdr_->btree_order_ = order;

    std::vector<int64_t> keys;
    for (int64_t key = 1; key <= scale; key++) {
        keys.push_back(key);
    }

    auto rng = std::default_random_engine{};
    std::shuffle(keys.begin(), keys.end(), rng);

    const char *index_key;
    for (auto key : keys) {
        int32_t value = key & 0xFFFFFFFF;
        Rid rid = {.page_no = static_cast<int32_t>(key >> 32),
                .slot_no = value};  // page_id = (key>>32), slot_num = (key & 0xFFFFFFFF)
        index_key = (const char *)&key;

        bool insert_ret = ih_->insert_entry(index_key, rid, txn_.get());  // 调用Insert
        // draw(buffer_pool_manager_.get(), "insert" + std::to_string(key) + ".dot");
        if(key==9241) {
            draw(buffer_pool_manager_.get(), "large_scale_test.dot");
        }
        ASSERT_EQ(insert_ret, true);
        std::vector<Rid> rids;

        LOG_DEBUG("%s", fmt::format("Success Check Insert {}",key).c_str());
    }
    LOG_DEBUG("Insert Complete");
    // draw(buffer_pool_manager_.get(), "large_scale_test.dot");

    std::vector<Rid> rids;
    for (auto key:keys) {
        // LOG_DEBUG("%s", fmt::format("Check {}",key).c_str());
        rids.clear();
        index_key = (const char *)&key;
        ih_->get_value(index_key, &rids, txn_.get());  // 调用get_value
        ASSERT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].slot_no, value);
    }
    // test Ixscan
    int64_t start_key = 1;
    int64_t current_key = start_key;
    IxScan scan(ih_.get(), ih_->leaf_begin(), ih_->leaf_end(), buffer_pool_manager_.get());
    while (!scan.is_end()) {
        int32_t insert_page_no = static_cast<int32_t>(current_key >> 32);
        int32_t insert_slot_no = current_key & 0xFFFFFFFF;
        Rid rid = scan.rid();
        EXPECT_EQ(rid.page_no, insert_page_no);
        EXPECT_EQ(rid.slot_no, insert_slot_no);
        current_key++;
        scan.next();
    }
    EXPECT_EQ(current_key, keys.size() + 1);
}

TEST_F(IndexTest, DeleteTest) {
    const int64_t scale = 10;
    const int order = 3;

    assert(order > 2 && order <= ih_->file_hdr_->btree_order_);
    ih_->file_hdr_->btree_order_ = order;

    std::vector<int64_t> keys;
    for (int64_t key = 1; key <= scale; key++) {
        keys.push_back(key);
    }

    const char *index_key;
    for (auto key : keys) {
        int32_t value = key & 0xFFFFFFFF;
        Rid rid = {.page_no = static_cast<int32_t>(key >> 32),
                .slot_no = value};
        index_key = (const char *)&key;
        bool insert_ret = ih_->insert_entry(index_key, rid, txn_.get());  // 调用Insert
        ASSERT_EQ(insert_ret, true);
        LOG_DEBUG("%s", fmt::format("Insert {}",key).c_str());
        draw(buffer_pool_manager_.get(), "insert" + std::to_string(key) + ".dot");
    }

    std::vector<Rid> rids;
    for (auto key : keys) {
        rids.clear();
        index_key = (const char *)&key;
        ih_->get_value(index_key, &rids, txn_.get());
        LOG_DEBUG("%s", fmt::format("Search {}",key).c_str());
        EXPECT_EQ(rids.size(), 1);

        int32_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].slot_no, value);
    }

    for (int key = scale + 1; key <= scale + 100; key++) {
        rids.clear();
        index_key = (const char *)&key;
        ih_->get_value(index_key, &rids, txn_.get());
        EXPECT_EQ(rids.size(), 0);
    }

    // delete keys
    std::vector<int64_t> delete_keys;

    const int64_t delete_scale = 9;
    for (int64_t key = 1; key <= delete_scale; key++) {  // 1~9
        delete_keys.push_back(key);
    }
    draw(buffer_pool_manager_.get(),"before_delete.dot");
    for (auto key : delete_keys) {
        index_key = (const char *)&key;
        bool delete_ret = ih_->delete_entry(index_key, txn_.get());  // 调用Delete
        draw(buffer_pool_manager_.get(),"after_delete"+std::to_string(key)+".dot");
        ASSERT_EQ(delete_ret, true);
    }

    int64_t start_key = *delete_keys.rbegin() + 1;
    int64_t current_key = start_key;
    int64_t size = 0;

    IxScan scan(ih_.get(), ih_->leaf_begin(), ih_->leaf_end(), buffer_pool_manager_.get());
    while (!scan.is_end()) {
        auto rid = scan.rid();
        EXPECT_EQ(rid.page_no, 0);
        EXPECT_EQ(rid.slot_no, current_key);
        current_key++;
        size++;
        scan.next();
    }
    EXPECT_EQ(size, keys.size() - delete_keys.size());
}


TEST_F(IndexTest, DeleteTest2) {
    const int64_t scale = 1000;
    const int order = 10;

    assert(order > 2 && order <= ih_->file_hdr_->btree_order_);
    ih_->file_hdr_->btree_order_ = order;

    std::vector<int64_t> keys;
    for (int64_t key = 1; key <= scale; key++) {
        keys.push_back(key);
    }

    // insert keys
    const char *index_key;
    for (auto key : keys) {
        int32_t value = key & 0xFFFFFFFF;  // key的低32位
        Rid rid = {.page_no = static_cast<int32_t>(key >> 32),
                .slot_no = value};  // page_id = (key>>32), slot_num = (key & 0xFFFFFFFF)
        index_key = (const char *)&key;
        bool insert_ret = ih_->insert_entry(index_key, rid, txn_.get());  // 调用Insert
        ASSERT_EQ(insert_ret, true);
        LOG_DEBUG(fmt::format("Insert {}",key).c_str());
    }
    draw(buffer_pool_manager_.get(), "insert.dot");

    // scan keys by get_value()
    std::vector<Rid> rids;
    for (auto key : keys) {
        rids.clear();
        index_key = (const char *)&key;
        ih_->get_value(index_key, &rids, txn_.get());  // 调用get_value
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].slot_no, value);
    }

    for (auto key : keys) {
        index_key = (const char *)&key;
        if(key==24) continue;
        bool delete_ret = ih_->delete_entry(index_key, txn_.get());  // 调用Delete
        ASSERT_EQ(delete_ret, true);

    }
    draw(buffer_pool_manager_.get(), fmt::format("Delete.dot"));
}

TEST_F(IndexTest, LargeDeleteTest) {
    const int order = 255;
    const int scale = 200;

    if (order <= ih_->file_hdr_->btree_order_) {
        ih_->file_hdr_->btree_order_ = order;
    }
    int add_cnt = 0;
    int del_cnt = 0;


    std::multimap<int, Rid> mock;
    mock.clear();
    while (add_cnt + del_cnt < scale) {
        LOG_DEBUG("");
        double dice = rand() * 1. / RAND_MAX;
        double insert_prob = 1. - mock.size() / (0.5 * scale);
        if (mock.empty() || dice < insert_prob) {
            // Insert
            int rand_key = rand() % scale;
            if (mock.find(rand_key) != mock.end()) {  // 防止插入重复的key
                // printf("重复key=%d!\n", rand_key);
                continue;
            }
            Rid rand_val = {.page_no = rand(), .slot_no = rand()};
            LOG_DEBUG(fmt::format("Insert {}", rand_key).c_str());
            bool insert_ret = ih_->insert_entry((const char *)&rand_key, rand_val, txn_.get());  // 调用Insert
            ASSERT_EQ(insert_ret, true);
            mock.insert(std::make_pair(rand_key, rand_val));
            add_cnt++;
            draw(buffer_pool_manager_.get(),fmt::format("Insert{}.dot", rand_key).c_str());
        } else {
            // Delete
            if (mock.size() == 1) {  // 只剩最后一个结点时不删除，以防变成空树
                continue;
            }
            int rand_idx = rand() % mock.size();
            auto it = mock.begin();
            for (int k = 0; k < rand_idx; k++) {
                it++;
            }
            int key = it->first;
            // printf("delete rand key=%d\n", key);
            LOG_DEBUG(fmt::format("DELETE {}", key).c_str());
            bool delete_ret = ih_->delete_entry((const char *)&key, txn_.get());

            draw(buffer_pool_manager_.get(),fmt::format("Delete{}.dot", key).c_str());
            ASSERT_EQ(delete_ret, true);
            mock.erase(it);
            del_cnt++;
            // Draw(buffer_pool_manager_.get(),
            //      "MixTest2_" + std::to_string(num) + "_delete" + std::to_string(key) + ".dot");
        }
        // check_all(ih_.get(), mock);
    }
    std::cout << "Insert keys count: " << add_cnt << '\n' << "Delete keys count: " << del_cnt << '\n';
    check_all(ih_.get(), mock);
}

template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
    std::vector<std::thread> thread_group;

    // Launch a group of threads
    for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
        thread_group.push_back(std::thread(args..., thread_itr));
    }

    // Join the threads with the main thread
    for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
        thread_group[thread_itr].join();
    }
}

// only for DEBUG
int getThreadId() {
    // std::scoped_lock latch{latch_};
    std::stringstream ss;
    ss << std::this_thread::get_id();
    // ss << transaction->GetThreadId();
    uint64_t thread_id = std::stoull(ss.str());
    return static_cast<int>(thread_id % 13);
}

// helper function to insert
void InsertHelper(IxIndexHandle *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
    // create transaction
    Transaction *transaction = new Transaction(0);  // 注意，每个线程都有一个事务；不能从上层传入一个共用的事务

    const char *index_key;
    for (auto key : keys) {
        int32_t value = key & 0xFFFFFFFF;
        Rid rid = {.page_no = static_cast<int32_t>(key >> 32), .slot_no = value};
        index_key = (const char *)&key;
        // LOG_DEBUG("Insert %d",key);
        try {
            tree->insert_entry(index_key, rid, transaction);
        } catch (IndexEntryDuplicateError e) {

        }

    }

    std::vector<Rid> rids;
    for (auto key : keys) {
        rids.clear();
        index_key = (const char *)&key;
        tree->get_value(index_key, &rids, transaction);  // 调用get_value
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].slot_no, value);
    }

    delete transaction;
}

// helper function to delete
void DeleteHelper(IxIndexHandle *tree, const std::vector<int64_t> &keys, size_t * cnt,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
    // create transaction
    auto *transaction = new Transaction(0);
    const char *index_key;
    for (auto key : keys) {
        index_key = (const char *)&key;
       if(tree->delete_entry(index_key, transaction)) {
           LOG_DEBUG("[%ld] Delete %ld", thread_itr,key );
           (*cnt)++;
       }
    }

    delete transaction;
}


TEST_F(IndexTest, InsertScaleTest) {
    const int64_t scale = 1000;
    const int thread_num = 2;
    const int order = 100;

    assert(order > 2 && order <= ih_->file_hdr_->btree_order_);
    ih_->file_hdr_->btree_order_ = order;

    // keys to Insert
    std::vector<int64_t> keys;
    for (int64_t key = 1; key <= scale; key++) {
        keys.push_back(key);
    }

    // randomized the insertion order
    auto rng = std::default_random_engine{};
//    std::shuffle(keys.begin(), keys.end(), rng);


    LaunchParallelTest(thread_num, InsertHelper, ih_.get(), keys);
    LOG_DEBUG("Insert key 1~%ld finished\n", scale);

    draw(buffer_pool_manager_.get(),"insert.dot");
    int64_t start_key = 1;
    int64_t current_key = start_key;

    IxScan scan(ih_.get(), ih_->leaf_begin(), ih_->leaf_end(), buffer_pool_manager_.get());
    while (!scan.is_end()) {
        auto rid = scan.rid();
        EXPECT_EQ(rid.page_no, 0);
        EXPECT_EQ(rid.slot_no, current_key);
        current_key = current_key + 1;
        scan.next();
    }


    EXPECT_EQ(current_key, keys.size() + 1);
}
TEST_F(IndexTest, MixScaleTest) {
    const int64_t scale = 1000;
    const int64_t delete_scale = 990;
    const int thread_num = 50;
    const int order = 10;

    assert(order > 2 && order <= ih_->file_hdr_->btree_order_);
    ih_->file_hdr_->btree_order_ = order;

    // keys to Insert
    std::vector<int64_t> keys;
    for (int64_t key = 1; key <= scale; key++) {
        keys.push_back(key);
    }
    // 这里调用了insert_entry，并且用thread_num个进程并发插入（包括并发查找）
    LaunchParallelTest(thread_num, InsertHelper, ih_.get(), keys);
    LOG_DEBUG("Insert key 1~%ld finished\n", scale);

    draw(buffer_pool_manager_.get(),"ii.dot");
    // keys to Delete
    std::vector<int64_t> delete_keys;
    for (int64_t key = 1; key <= delete_scale; key++) {
        delete_keys.push_back(key);
    }
    size_t cnt = 0;
    LaunchParallelTest(thread_num, DeleteHelper, ih_.get(), delete_keys,  &cnt);
    LOG_DEBUG("Delete key 1~%ld finished\n", delete_scale);
    LOG_DEBUG("Delete tot %ld",cnt);
    draw(buffer_pool_manager_.get(),"dd.dot");
    int64_t start_key = *delete_keys.rbegin() + 1;
    int64_t current_key = start_key;
    int64_t size = 0;

    IxScan scan(ih_.get(), ih_->leaf_begin(), ih_->leaf_end(), buffer_pool_manager_.get());
    while (!scan.is_end()) {
        auto rid = scan.rid();
        EXPECT_EQ(rid.page_no, 0);
        EXPECT_EQ(rid.slot_no, current_key);
        current_key++;
        size++;
        scan.next();
    }
    // EXPECT_EQ(size, keys.size() - delete_keys.size());
}
