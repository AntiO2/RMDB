//
// Created by Anti on 2023/7/9.
//

#include <random>
#include <thread>
#include "gtest/gtest.h"
#include "replacer/lru_replacer.h"
#include "storage/disk_manager.h"

constexpr int MAX_FILES = 32;
constexpr int MAX_PAGES = 128;
const std::string TEST_DB_NAME = "DiskManagerTest_db";  // 以TEST_DB_NAME作为存放测试文件的根目录名

class DiskManagerTest : public ::testing::Test {
public:
    std::unique_ptr<DiskManager> disk_manager_;

public:
    // This function is called before every test.
    void SetUp() override {
        ::testing::Test::SetUp();
        // 对于每个测试点，创建一个disk manager
        disk_manager_ = std::make_unique<DiskManager>();
        // 如果测试目录不存在，则先创建测试目录
        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));  // 检查是否创建目录成功
        // 进入测试目录
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
    }

    // This function is called after every test.
    void TearDown() override {
        // 返回上一层目录
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
    };

    /**
     * @brief 将buf填充size个字节的随机数据
     */
    void rand_buf(char *buf, int size) {
        srand((unsigned) time(nullptr));
        for (int i = 0; i < size; i++) {
            int rand_ch = rand() & 0xff;
            buf[i] = rand_ch;
        }
    }
};


TEST_F(DiskManagerTest, FileOperation) {
    std::vector<std::string> filenames(MAX_FILES);   // MAX_FILES=32
    std::unordered_map<int, std::string> fd2name;    // fd -> filename
    for (size_t i = 0; i < filenames.size(); i++) {  // 创建MAX_FILES个文件
        auto &filename = filenames[i];
        filename = "FileOperationTestFile" + std::to_string(i);
// 清理残留文件
        if (disk_manager_->is_file(filename)) {
            disk_manager_->destroy_file(filename);
        }
// 测试异常：如果没有创建文件就打开文件
        try {
            disk_manager_->open_file(filename);
            assert(false);
        } catch (const FileNotFoundError &e) {
        }
// 创建文件
        disk_manager_->create_file(filename);
        EXPECT_EQ(disk_manager_->is_file(filename), true);  // 检查是否创建文件成功
        try {
            disk_manager_->create_file(filename);
            assert(false);
        } catch (const FileExistsError &e) {
        }
// 打开文件
        int fd = disk_manager_->open_file(filename);
        fd2name[fd] = filename;

// 关闭后重新打开
        if (rand() % 5 == 0) {
            disk_manager_->close_file(fd);
            int new_fd = disk_manager_->open_file(filename);
            fd2name[new_fd] = filename;
        }
    }

// 关闭&删除文件
    for (auto &entry: fd2name) {
        int fd = entry.first;
        auto &filename = entry.second;
        disk_manager_->close_file(fd);
        disk_manager_->destroy_file(filename);
        EXPECT_EQ(disk_manager_->is_file(filename), false);  // 检查是否删除文件成功
        try {
            disk_manager_->destroy_file(filename);
            assert(false);
        } catch (const FileNotFoundError &e) {
        }
    }
}

/**
 * @brief 测试读写页面，分配页面编号 read/write page, allocate page_no
 * @note lab1 计分：5 points
 */
TEST_F(DiskManagerTest, PageOperation) {
    const std::string filename = "PageOperationTestFile";
// 清理残留文件
    if (disk_manager_->is_file(filename)) {
        disk_manager_->destroy_file(filename);
    }
// 创建文件
    disk_manager_->create_file(filename);
    EXPECT_EQ(disk_manager_->is_file(filename), true);
// 打开文件
    int fd = disk_manager_->open_file(filename);
// 初始页面编号为0
    disk_manager_->set_fd2pageno(fd, 0);

// 读写页面&分配页面编号（在单个文件上测试）
    char buf[PAGE_SIZE] = {0};
    char data[PAGE_SIZE] = {0};
    for (int page_no = 0; page_no < MAX_PAGES; page_no++) {
// 分配页面编号
        int ret_page_no = disk_manager_->allocate_page(fd);  // 注意此处返回值是分配编号之前的值
        EXPECT_EQ(ret_page_no, page_no);
// 读写页面
        rand_buf(data, PAGE_SIZE);                                // generate data
        disk_manager_->write_page(fd, page_no, data, PAGE_SIZE);  // write data to disk (data -> disk page)
        std::memset(buf, 0, sizeof(buf));                         // clear buf
        disk_manager_->read_page(fd, page_no, buf, PAGE_SIZE);    // read buf from disk (disk page -> buf)
        EXPECT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);        //
    }

// 关闭&删除文件
    disk_manager_->close_file(fd);
    disk_manager_->destroy_file(filename);
    EXPECT_EQ(disk_manager_->is_file(filename), false);
}

TEST(LRU_TEST, SAMPLE_TEST) {
    LRUReplacer lru_replacer(7);

    // Scenario: unpin six elements, i.e. add them to the replacer.
    lru_replacer.unpin(1);
    lru_replacer.unpin(2);
    lru_replacer.unpin(3);
    lru_replacer.unpin(4);
    lru_replacer.unpin(5);
    lru_replacer.unpin(6);
    // lru_replacer.unpin(1);
    EXPECT_EQ(6, lru_replacer.Size());

    // Scenario: get three victims from the lru.
    int value;
    lru_replacer.victim(&value);
    EXPECT_EQ(1, value);
    lru_replacer.victim(&value);
    EXPECT_EQ(2, value);
    lru_replacer.victim(&value);
    EXPECT_EQ(3, value);

    // Scenario: pin elements in the replacer.
    // Note that 3 has already been victimized, so pinning 3 should have no effect.
    lru_replacer.pin(3);
    lru_replacer.pin(4);
    EXPECT_EQ(2, lru_replacer.Size());

    // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
    lru_replacer.unpin(4);

    // Scenario: continue looking for victims. We expect these victims.
    lru_replacer.victim(&value);
    EXPECT_EQ(5, value);
    lru_replacer.victim(&value);
    EXPECT_EQ(6, value);
    lru_replacer.victim(&value);
    EXPECT_EQ(4, value);
}

TEST(LRU_TEST, HARD_TEST) {
    int result;
    int value_size = 10000;
    auto lru_replacer = new LRUReplacer(value_size);
    std::vector<int> value(value_size);
    for (int i = 0; i < value_size; i++) {
        value[i] = i;
    }
    auto rng = std::default_random_engine();
    std::shuffle(value.begin(), value.end(), rng);
    for (int i = 0; i < value_size; i++) {
        lru_replacer->unpin(value[i]);
    }
    EXPECT_EQ(value_size, lru_replacer->Size());
    lru_replacer->pin(777);
    lru_replacer->unpin(777);
    EXPECT_EQ(1, lru_replacer->victim(&result));
    EXPECT_EQ(value[0], result);
    lru_replacer->unpin(value[0]);
    for (int i = 0; i < value_size / 2; i++) {
        if (value[i] != value[0] && value[i] != 777) {
            lru_replacer->pin(value[i]);
            lru_replacer->unpin(value[i]);
        }
    }
    std::vector<int> lru_array;
    for (int i = value_size / 2; i < value_size; ++i) {
        if (value[i] != value[0] && value[i] != 777) {
            lru_array.push_back(value[i]);
        }
    }
    lru_array.push_back(777);
    lru_array.push_back(value[0]);
    for (int i = 0; i < value_size / 2; ++i) {
        if (value[i] != value[0] && value[i] != 777) {
            lru_array.push_back(value[i]);
        }
    }
    EXPECT_EQ(value_size, lru_replacer->Size());
    for (int e: lru_array) {
        EXPECT_EQ(true, lru_replacer->victim(&result));
        EXPECT_EQ(e, result);
    }
    EXPECT_EQ(value_size - lru_array.size(), lru_replacer->Size());
    delete lru_replacer;
}

TEST(LRU_TEST, Conc_Test) {
    const int num_threads = 5;
    const int num_runs = 50;
    for (int run = 0; run < num_runs; run++) {
        int value_size = 1000;
        std::shared_ptr<LRUReplacer> lru_replacer{new LRUReplacer(value_size)};
        std::vector<std::thread> threads;
        int result;
        std::vector<int> value(value_size);
        for (int i = 0; i < value_size; i++) {
            value[i] = i;
        }
        auto rng = std::default_random_engine{};
        std::shuffle(value.begin(), value.end(), rng);

        for (int tid = 0; tid < num_threads; tid++) {
            threads.emplace_back([tid, &lru_replacer, &value]() {
                int share = 1000 / 5;
                for (int i = 0; i < share; i++) {
                    lru_replacer->unpin(value[tid * share + i]);
                }
            });
        }
        for (int i = 0; i < num_threads; i++) {
            threads[i].join();
        }
        std::vector<int> out_values;
        for (int i = 0; i < value_size; i++) {
            EXPECT_EQ(1, lru_replacer->victim(&result));
            out_values.push_back(result);
        }
        std::sort(value.begin(), value.end());
        std::sort(out_values.begin(), out_values.end());
        EXPECT_EQ(value, out_values);
        EXPECT_EQ(0, lru_replacer->victim(&result));
    }
}
