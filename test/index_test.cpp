//
// Created by Anti on 2023/7/17.
//


#include <algorithm>
#include <cstdio>
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
class IndexTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager_;
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
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(POOL_SIZE, disk_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        txn_ = std::make_unique<Transaction>(0);
        int curr_offset = 0;

        // 创建两列索引
        ColDef colA{.name="A",.type=ColType::TYPE_INT,.len=col2len(ColType::TYPE_INT)};
        ColDef colB{.name="B",.type=ColType::TYPE_INT,.len=col2len(ColType::TYPE_INT)};

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
                out << "<TD> (" << leaf->key_at(i) <<","<<leaf->key_2nd(i)<< ") </TD>\n";
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
                out << "("<<inner->key_at(i)<< ","<<inner->key_2nd(i)<<")";
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
TEST(UPPERBOUND,DISABLED_SIMPLE_TEST) {
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

