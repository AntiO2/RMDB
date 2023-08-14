#include <netdb.h>
#include <netinet/in.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <termios.h>
#include <unistd.h>

#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <fstream>
#include <sstream>
#include <mutex>

#include "clock.h"
#include "config.h"
#include "table.h"

#define MAX_MEM_BUFFER_SIZE 8192
#define PORT_DEFAULT 8765
#define MAX_CLIENT_NUM 4    // 同时连接服务端的客户端数量
#define MAX_TXN_PER_CLIENT 1000 // 每个客户端执行的事务数量

#define load_data(table_name) table_name* table_name##_data = new table_name(); \
                              table_name##_data->generate_data_csv(file_name); \

int main(int argc, char *argv[]) {
    SystemClock* clock = new SystemClock();
    RandomGenerator::init();

    // load data
    std::string file_name = "../src/test/performance_test/table_data/warehouse.csv";
    load_data(Warehouse);
    file_name = "../src/test/performance_test/table_data/district.csv";
    load_data(District);
    file_name = "../src/test/performance_test/table_data/customer.csv";
    load_data(Customer);
    file_name = "../src/test/performance_test/table_data/history.csv";
    load_data(History);
    file_name = "../src/test/performance_test/table_data/new_orders.csv";
    load_data(NewOrders);
    file_name = "../src/test/performance_test/table_data/orders.csv";
    load_data(Orders);
    file_name = "../src/test/performance_test/table_data/order_line.csv";
    load_data(OrderLine);
    file_name = "../src/test/performance_test/table_data/item.csv";
    load_data(Item);
    file_name = "../src/test/performance_test/table_data/stock.csv";
    load_data(Stock);

    return 0;
}
