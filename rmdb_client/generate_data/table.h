#ifndef TABLE_H
#define TABLE_H

#include "clock.h"
#include "config.h"
#include "random.h"
#include <cstring>
#include <iostream>
#include <cstdio>
#include <vector>
#include <fstream>

class Warehouse {
public:
    int w_id;
    char w_name[11];    // varchar
    char w_street_1[21];    // varchar
    char w_street_2[21];    // varchar
    char w_city[21];        // varchar
    char w_state[3];
    char w_zip[10];
    float w_tax;
    float w_ytd;
    std::vector<std::string> sqls;

    Warehouse() {
        sqls.clear();
    }

    void print_record() {
        printf("(w_id: %d, w_name: %s, w_street_1: %s, w_city: %s, w_state: %s, w_state: %s, w_zip: %s, w_tax: %lf, w_ytd: %lf)\n", 
        w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd);
    }

    void generate_insert_sql() {
        generate_table_data();
    }

    void generate_table_data() {
        w_ytd = 3000.5;
        std::string sql;
        for(w_id = 1; w_id <= NUM_WARE; ++w_id) {
            sql = "insert into warehouse values ";
            // RandomGenerator::generate_random_varchar(w_name, 6, 10);
            RandomGenerator::generate_random_str(w_name, 10);
            RandomGenerator::generate_randome_address(w_street_1, w_street_2, w_city, w_state, w_zip);
            w_tax = (float)RandomGenerator::generate_random_int(10, 20) / 100.0;    // 0.1-0.2

            // print_record();
            sql += "(";
            sql += std::to_string(w_id) + ", '";
            sql.append(w_name);
            sql += "', '";
            sql.append(w_street_1);
            sql += "', '";
            sql.append(w_street_2);
            sql += "', '";
            sql.append(w_city);
            sql += "', '";
            sql.append(w_state);
            sql += "', '";
            sql.append(w_zip);
            sql += "', " + std::to_string(w_tax) + ", " + std::to_string(w_ytd);
            sql += ");";
            sqls.push_back(sql);
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "w_id,w_name,w_street_1,w_street_2,w_city,w_state,w_zip,w_tax,w_ytd" << std::endl;
        outfile.close();
        write_data_into_file(file_name);
    }

    void write_data_into_file(std::string file_name) {
        w_ytd = 3000.5;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(w_id = 1; w_id <= NUM_WARE; ++w_id) {
            RandomGenerator::generate_random_str(w_name, 10);
            RandomGenerator::generate_randome_address(w_street_1, w_street_2, w_city, w_state, w_zip);
            int tmp = RandomGenerator::generate_random_int(1, 2);
            if(tmp == 1)
                w_tax = 0.125;
            else
                w_tax = 0.3125;

            outfile << w_id << "," << w_name << "," << w_street_1 << "," << w_street_2 << "," << w_city << "," << w_state << "," << w_zip << "," << w_tax << "," << w_ytd << std::endl;
        }
        outfile.close();
    }
};

class District {
public:
    int d_id;
    int d_w_id;
    char d_name[11];    // varchar
    char d_street_1[21];    // varchar
    char d_street_2[21];    // varchar
    char d_city[21];    // varchar
    char d_state[3];
    char d_zip[10];
    float d_tax;
    float d_ytd;
    int d_next_o_id;
    std::vector<std::string> sqls;

    District() {
        sqls.clear();
    }

    void generate_insert_sql() {
        for(int i = 1; i <= NUM_WARE; ++i) {
            generate_table_data(i);
        }
    }

    void generate_table_data(int w_id) {
        d_w_id = w_id;
        d_ytd = 3000.5;
        // d_next_o_id = 3001;
        d_next_o_id = ORDER_PER_DISTRICT + 1;
        std::string sql;
        for(d_id = 1; d_id <= DISTRICT_PER_WARE; ++d_id) {
            // RandomGenerator::generate_random_varchar(d_name, 6, 10);
            RandomGenerator::generate_random_str(d_name, 10);
            RandomGenerator::generate_randome_address(d_street_1, d_street_2, d_city, d_state, d_zip);
            // d_tax = (float)RandomGenerator::generate_random_int(10, 20) / 100.0;
            d_tax = RandomGenerator::generate_random_float(10, 20);

            sql = "insert into district values ";
            sql += "(";
            sql += std::to_string(d_id) + ", " + std::to_string(d_w_id) + ", '";
            sql.append(d_name);
            sql += "', '";
            sql.append(d_street_1);
            sql += "', '";
            sql.append(d_street_2);
            sql += "', '";
            sql.append(d_city);
            sql += "', '";
            sql.append(d_state);
            sql += "', '";
            sql.append(d_zip);
            sql += "', " + std::to_string(d_tax) + ", " + std::to_string(d_ytd) + ", " + std::to_string(d_next_o_id);
            sql += ");";
            sqls.push_back(sql);
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "d_id,d_w_id,d_name,d_street_1,d_street_2,d_city,d_state,d_zip,d_tax,d_ytd,d_next_o_id" << std::endl;
        outfile.close();
        for(int i = 1; i <= NUM_WARE; ++i) {
            write_data_into_file(file_name, i);
        }
    }

    void write_data_into_file(std::string file_name, int w_id) {
        d_w_id = w_id;
        d_ytd = 3000.5;
        // d_next_o_id = 3001;
        d_next_o_id = ORDER_PER_DISTRICT + 1;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(d_id = 1; d_id <= DISTRICT_PER_WARE; ++d_id) {
            RandomGenerator::generate_random_str(d_name, 10);
            RandomGenerator::generate_randome_address(d_street_1, d_street_2, d_city, d_state, d_zip);
            int tmp = RandomGenerator::generate_random_int(1, 2);
            if(tmp == 1)
                d_tax = 0.125;
            else
                d_tax = 0.3125;

            outfile << d_id << "," << d_w_id << "," << d_name << "," << d_street_1 << "," << d_street_2 << ",";
            outfile << d_city << "," << d_state << "," << d_zip << "," << d_tax << "," << d_ytd << "," << d_next_o_id << std::endl;
        }
        outfile.close();
    }
};

class Customer {
public:
    int c_id;
    int c_d_id;
    int c_w_id;
    char c_first[17];   // varchar
    char c_middle[3]; 
    char c_last[17];    // varchar
    char c_street_1[21];    // varchar
    char c_street_2[21];    // varchar
    char c_city[21];    // varchar
    char c_state[3];
    char c_zip[10];
    char c_phone[17];
    char c_since[Clock::DATETIME_SIZE + 1];
    char c_credit[3];
    int c_credit_lim;
    float c_discount;
    float c_balance;
    float c_ytd_payment;
    int c_paymeny_cnt;
    int c_delivery_cnt;
    // char c_data[501];
    char c_data[51];
    SystemClock* clock;
    std::vector<std::string> sqls;

    Customer() {
        clock = new SystemClock();
        sqls.clear();
    }

    void generate_insert_sql() {
        for(int i = 1; i <= NUM_WARE; ++i) {
            generate_table_data(i);
        }
    }

    void generate_table_data(int w_id) {
        c_w_id = w_id;
        c_credit_lim = 50000;
        c_balance = 10.5;
        c_ytd_payment = 10.5;
        c_paymeny_cnt = 1;
        c_delivery_cnt = 0;
        std::string sql;
        for(c_d_id = 1; c_d_id <= DISTRICT_PER_WARE; ++c_d_id) {
            for(c_id = 1; c_id <= CUSTOMER_PER_DISTRICT; ++c_id) {
                // if(c_id % 10 == 1) sql = "insert into customer values ";
                // else sql += ", ";
                sql = "insert into customer values ";
                // RandomGenerator::generate_random_varchar(c_first, 8, 16);
                RandomGenerator::generate_random_str(c_first, 16);
                c_middle[0] = 'O';
                c_middle[1] = 'E';
                c_middle[2] = 0;
                if(c_id <= 1000) {
                    RandomGenerator::generate_random_lastname(c_id - 1, c_last);
                } 
                else {
                    RandomGenerator::generate_random_lastname(RandomGenerator::NURand(255, 0, 999), c_last);
                }

                RandomGenerator::generate_randome_address(c_street_1, c_street_2, c_city, c_state, c_zip);
                RandomGenerator::generate_random_numer_str(c_phone, 16);
                clock->getDateTimestamp(c_since);
                if (RandomGenerator::generate_random_int(0, 1))
                    c_credit[0] = 'G';
                else
                    c_credit[0] = 'B';
                c_credit[1] = 'C';
                c_credit[2] = 0;
                // c_discount = (float)RandomGenerator::generate_random_int(0, 50) / 100.0;
                c_discount = RandomGenerator::generate_random_float(0, 50);
                // RandomGenerator::generate_random_varchar(c_data, 300, 500);
                // RandomGenerator::generate_random_varchar(c_data, 30, 50);
                RandomGenerator::generate_random_str(c_data, 50);

                sql += "(";
                sql += std::to_string(c_id) + ", " + std::to_string(c_d_id) + ", " + std::to_string(c_w_id) + ", '";
                sql.append(c_first);
                sql += "', '";
                sql.append(c_middle);
                sql += "', '";
                sql.append(c_last);
                sql += "', '";
                sql.append(c_street_1);
                sql += "', '";
                sql.append(c_street_2);
                sql += "', '";
                sql.append(c_city);
                sql += "', '";
                sql.append(c_state);
                sql += "', '";
                sql.append(c_zip);
                sql += "', '";
                sql.append(c_phone);
                sql += "', '";
                sql.append(c_since);
                sql += "', '";
                sql.append(c_credit);
                sql += "', " + std::to_string(c_credit_lim) + ", " + std::to_string(c_discount) + ", " + std::to_string(c_balance) + ", ";
                sql += std::to_string(c_ytd_payment) + ", " + std::to_string(c_paymeny_cnt) + ", " + std::to_string(c_delivery_cnt) + ", '";
                sql.append(c_data);
                sql += "')";

                // if(c_id % 10 == 0) sql += ";", sqls.push_back(sql);
                sql += ";", sqls.push_back(sql);
            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "c_id,c_d_id,c_w_id,c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,";
        outfile << "c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data" << std::endl;
        outfile.close();
        for(int i = 1; i <= NUM_WARE; ++i) {
            write_data_into_file(file_name, i);
        }
    }

    void write_data_into_file(std::string file_name, int w_id) {
        c_w_id = w_id;
        c_credit_lim = 50000;
        c_balance = 10.5;
        c_ytd_payment = 10.5;
        c_paymeny_cnt = 1;
        c_delivery_cnt = 0;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(c_d_id = 1; c_d_id <= DISTRICT_PER_WARE; ++c_d_id) {
            for(c_id = 1; c_id <= CUSTOMER_PER_DISTRICT; ++c_id) {
                RandomGenerator::generate_random_str(c_first, 16);
                c_middle[0] = 'O';
                c_middle[1] = 'E';
                c_middle[2] = 0;
                if(c_id <= 1000) {
                    RandomGenerator::generate_random_lastname(c_id - 1, c_last);
                } 
                else {
                    RandomGenerator::generate_random_lastname(RandomGenerator::NURand(255, 0, 999), c_last);
                }

                RandomGenerator::generate_randome_address(c_street_1, c_street_2, c_city, c_state, c_zip);
                RandomGenerator::generate_random_numer_str(c_phone, 16);
                clock->getDateTimestamp(c_since);
                if (RandomGenerator::generate_random_int(0, 1))
                    c_credit[0] = 'G';
                else
                    c_credit[0] = 'B';
                c_credit[1] = 'C';
                c_credit[2] = 0;
                // c_discount = (float)RandomGenerator::generate_random_int(0, 50) / 100.0;
                c_discount = RandomGenerator::generate_random_float(0, 1);
                // RandomGenerator::generate_random_varchar(c_data, 300, 500);
                // RandomGenerator::generate_random_varchar(c_data, 30, 50);
                RandomGenerator::generate_random_str(c_data, 50);

                outfile << c_id << "," << c_d_id << "," << c_w_id << "," << c_first << "," << c_middle << "," << c_last << ",";
                outfile << c_street_1 << "," << c_street_2 << "," << c_city << "," << c_state << "," << c_zip << "," << c_phone << ",";
                outfile << c_since << "," << c_credit << "," << c_credit_lim << "," << c_discount << "," << c_balance << ",";
                outfile << c_ytd_payment << "," << c_paymeny_cnt << "," << c_delivery_cnt << "," << c_data << std::endl;
            }
        }
        outfile.close();
    }
};

class History {
public:
    int h_c_id;
    int h_c_d_id;
    int h_c_w_id;
    int h_d_id;
    int h_w_id;
    char h_date[Clock::DATETIME_SIZE + 1];
    float h_amount;
    char h_data[25];    // varchar
    SystemClock* clock;
    std::vector<std::string> sqls;

    History() {
        clock = new SystemClock();
        sqls.clear();
    }

    void generate_insert_sql() {
        for(int i = 1; i <= NUM_WARE; ++i) {
            generate_table_data(i);
        }
    }

    void generate_table_data(int w_id) {
        h_w_id = w_id;
        h_c_w_id = w_id;
        h_amount = 10.5;
        std::string sql;
        for(h_d_id = 1; h_d_id <= DISTRICT_PER_WARE; ++h_d_id) {
            for(h_c_id = 1; h_c_id <= CUSTOMER_PER_DISTRICT; ++h_c_id) {
                // if(h_c_id % 10 == 1) sql = "insert into history values ";
                // else sql += ", ";
                sql = "insert into history values ";
                h_c_d_id = h_d_id;
                clock->getDateTimestamp(h_date);
                // RandomGenerator::generate_random_varchar(h_data, 10, 24);
                RandomGenerator::generate_random_str(h_data, 24);

                sql += "(";
                sql += std::to_string(h_c_id) + ", " + std::to_string(h_c_d_id) + ", " + std::to_string(h_c_w_id) + ", ";
                sql += std::to_string(h_d_id) + ", " + std::to_string(h_w_id) + ", '";
                sql.append(h_date);
                sql += "', " + std::to_string(h_amount) + ", '";
                sql.append(h_data);
                sql += "')";

                // if(h_c_id % 10 == 0) sql += ";", sqls.push_back(sql);
                sql += ";", sqls.push_back(sql);
            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "h_c_id,h_c_d_id,h_c_w_id,h_d_id,h_w_id,h_date,h_amount,h_data" << std::endl;
        outfile.close();
        for(int i = 1; i <= NUM_WARE; ++i) {
            write_data_into_file(file_name, i);
        }
    }

    void write_data_into_file(std::string file_name, int w_id) {
        h_w_id = w_id;
        h_c_w_id = w_id;
        h_amount = 10.5;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(h_d_id = 1; h_d_id <= DISTRICT_PER_WARE; ++h_d_id) {
            for(h_c_id = 1; h_c_id <= CUSTOMER_PER_DISTRICT; ++h_c_id) {
                h_c_d_id = h_d_id;
                clock->getDateTimestamp(h_date);
                RandomGenerator::generate_random_str(h_data, 24);

                outfile << h_c_id << "," << h_c_d_id << "," << h_c_w_id << "," << h_d_id << "," << h_w_id << "," << h_date << ",";
                outfile << h_amount << "," << h_data << std::endl;
            }
        }
        outfile.close();
    }
};

class NewOrders {
public:
    int no_o_id;
    int no_d_id;
    int no_w_id;
    std::vector<std::string> sqls;

    NewOrders() {
        sqls.clear();
    }

    void generate_insert_sql() {
        for(int i = 1; i <= NUM_WARE; ++i) {
            generate_table_data(i);
        }
    }

    void generate_table_data(int w_id) {
        no_w_id = w_id;
        std::string sql;
        for(no_d_id = 1; no_d_id <= DISTRICT_PER_WARE; ++no_d_id) {
            for(no_o_id = FIRST_UNPROCESSED_O_ID; no_o_id <= ORDER_PER_DISTRICT; ++ no_o_id) {
                // if(no_o_id % 10 == 1) sql = "insert into new_orders values ";
                sql = "insert into new_orders values ";
                // else sql += ", ";
                sql += "(";
                sql += std::to_string(no_o_id) + ", " + std::to_string(no_d_id) + ", " + std::to_string(no_w_id);
                sql += ")";
                // if(no_o_id % 10 == 0) sql += ";", sqls.push_back(sql);
                sql += ";", sqls.push_back(sql);
            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "no_o_id,no_d_id,no_w_id" << std::endl;
        outfile.close();
        for(int i = 1; i <= NUM_WARE; ++i) {
            write_data_into_file(file_name, i);
        }
    }

    void write_data_into_file(std::string file_name, int w_id) {
        no_w_id = w_id;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(no_d_id = 1; no_d_id <= DISTRICT_PER_WARE; ++no_d_id) {
            for(no_o_id = FIRST_UNPROCESSED_O_ID; no_o_id <= ORDER_PER_DISTRICT; ++ no_o_id) {
                outfile << no_o_id << "," << no_d_id << "," << no_w_id << std::endl;
            }
        }
        outfile.close();
    }
};

class Orders {
public:
    int o_id;
    int o_d_id;
    int o_w_id;
    int o_c_id;
    char o_entry_d[Clock::DATETIME_SIZE + 1];
    int o_carrier_id;
    int o_ol_cnt;
    int o_all_local;
    SystemClock* clock;
    std::vector<std::string> sqls;

    Orders() {
        clock = new SystemClock();
        sqls.clear();
    }

    void generate_insert_sql() {
        for(int i = 1; i <= NUM_WARE; ++i) {
            generate_table_data(i);
        }
    }

    void generate_table_data(int w_id) {
        o_w_id = w_id;
        std::string sql;
        for(o_d_id = 1; o_d_id <= DISTRICT_PER_WARE; ++o_d_id) {
            // o_c_id must be a permutation of [1, 3000]
            int c_ids[CUSTOMER_PER_DISTRICT + 1];
            for(int i = 1; i <= CUSTOMER_PER_DISTRICT; ++i)
                c_ids[i - 1] = i;
            for(int i = 1; i <= CUSTOMER_PER_DISTRICT; ++i) {
                int index = RandomGenerator::generate_random_int(0, CUSTOMER_PER_DISTRICT - 1);
                std::swap(c_ids[i - 1], c_ids[index]);
            }
            for(int i = 1; i <= CUSTOMER_PER_DISTRICT; ++i) {
                // if(i % 10 == 1) sql = "insert into orders values ";
                // else sql += ", ";
                sql = "insert into orders values ";
                o_c_id = c_ids[i -1];
                o_id = i;
                clock->getDateTimestamp(o_entry_d);
                if(o_id < FIRST_UNPROCESSED_O_ID) {
                    o_carrier_id = RandomGenerator::generate_random_int(1, 10);
                }
                else {
                    o_carrier_id = 0;
                }
                // o_ol_cnt = RandomGenerator::generate_random_int(5, 15);
                o_ol_cnt = 10;
                o_all_local = 1;

                sql += "(";
                sql += std::to_string(o_id) + ", " + std::to_string(o_d_id) + ", " + std::to_string(o_w_id) + ", " + std::to_string(o_c_id) + ", '";
                sql.append(o_entry_d);
                sql += "', " + std::to_string(o_carrier_id) + ", " + std::to_string(o_ol_cnt) + ", " + std::to_string(o_all_local);
                sql += ")";

                // if(i % 10 == 0) sql += ";", sqls.push_back(sql);
                sql += ";", sqls.push_back(sql);
            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "o_id,o_d_id,o_w_id,o_c_id,o_entry_d,o_carrier_id,o_ol_cnt,o_all_local" << std::endl;
        outfile.close();
        for(int i = 1; i <= NUM_WARE; ++i) {
            write_data_into_file(file_name, i);
        }
    }

    void write_data_into_file(std::string file_name, int w_id) {
        o_w_id = w_id;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(o_d_id = 1; o_d_id <= DISTRICT_PER_WARE; ++o_d_id) {
            // o_c_id must be a permutation of [1, 3000]
            int c_ids[CUSTOMER_PER_DISTRICT + 1];
            for(int i = 1; i <= CUSTOMER_PER_DISTRICT; ++i)
                c_ids[i - 1] = i;
            for(int i = 1; i <= CUSTOMER_PER_DISTRICT; ++i) {
                int index = RandomGenerator::generate_random_int(0, CUSTOMER_PER_DISTRICT - 1);
                std::swap(c_ids[i - 1], c_ids[index]);
            }
            for(int i = 1; i <= CUSTOMER_PER_DISTRICT; ++i) {
                o_c_id = c_ids[i -1];
                o_id = i;
                clock->getDateTimestamp(o_entry_d);
                if(o_id < FIRST_UNPROCESSED_O_ID) {
                    o_carrier_id = RandomGenerator::generate_random_int(1, 10);
                }
                else {
                    o_carrier_id = 0;
                }
                // o_ol_cnt = RandomGenerator::generate_random_int(5, 15);
                o_ol_cnt = 10;
                o_all_local = 1;

                outfile << o_id << "," << o_d_id << "," << o_w_id << "," << o_c_id << "," << o_entry_d << "," << o_carrier_id << ",";
                outfile << o_ol_cnt << "," << o_all_local << std::endl;
            }
        }
        outfile.close();
    }
};

class OrderLine{
public:
    int ol_o_id;
    int ol_d_id;
    int ol_w_id;
    int ol_number;
    int ol_i_id;
    int ol_supply_w_id;
    char ol_delivery_d[Clock::DATETIME_SIZE + 1];
    int ol_quantity;
    float ol_amount;
    char ol_dist_info[25];
    SystemClock* clock;
    std::vector<std::string> sqls;

    OrderLine() {
        clock = new SystemClock();
        sqls.clear();
    }

    void generate_insert_sql() {
        for(int i = 1; i <= NUM_WARE; ++i) {
            generate_table_data(i);
        }
    }

    void generate_table_data(int w_id) {
        ol_w_id = w_id;
        std::string sql;
        for(ol_d_id = 1; ol_d_id <= DISTRICT_PER_WARE; ++ol_d_id) {
            for(ol_o_id = 1; ol_o_id <= ORDER_PER_DISTRICT; ++ol_o_id) {
                // sql = "insert into order_line values ";
                // int ol_cnt = RandomGenerator::generate_random_int(5, 15);
                int ol_cnt = 10;
                for(ol_number = 1; ol_number <= ol_cnt; ++ol_number) {
                    // if(ol_number > 1) sql += ", ";
                    sql = "insert into order_line values ";

                    ol_i_id = RandomGenerator::generate_random_int(1, MAXITEMS);
                    ol_supply_w_id = w_id;
                    clock->getDateTimestamp(ol_delivery_d);
                    ol_quantity = 5;
                    ol_amount = 0.5;
                    RandomGenerator::generate_random_str(ol_dist_info, 24);

                    sql += "(";
                    sql += std::to_string(ol_o_id) + ", " + std::to_string(ol_d_id) + ", " + std::to_string(ol_w_id) + ", ";
                    sql += std::to_string(ol_number) + ", " + std::to_string(ol_i_id) + ", " + std::to_string(ol_supply_w_id) + ", '";
                    sql.append(ol_delivery_d);
                    sql += "', " + std::to_string(ol_quantity) + ", " + std::to_string(ol_amount) + ", '";
                    sql.append(ol_dist_info);
                    sql += "')";
                    sql += ";";
                    sqls.push_back(sql);
                }
                // sql += ";";
                // sqls.push_back(sql);
            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "ol_o_id,ol_d_id,ol_w_id,ol_number,ol_i_id,ol_supply_w_id,ol_delivery_d,ol_quantity,ol_amount,ol_dist_info" << std::endl;
        outfile.close();
        for(int i = 1; i <= NUM_WARE; ++i) {
            write_data_into_file(file_name, i);
        }
    }

    void write_data_into_file(std::string file_name, int w_id) {
        ol_w_id = w_id;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(ol_d_id = 1; ol_d_id <= DISTRICT_PER_WARE; ++ol_d_id) {
            for(ol_o_id = 1; ol_o_id <= ORDER_PER_DISTRICT; ++ol_o_id) {
                // sql = "insert into order_line values ";
                // int ol_cnt = RandomGenerator::generate_random_int(5, 15);
                int ol_cnt = 10;
                for(ol_number = 1; ol_number <= ol_cnt; ++ol_number) {
                    ol_i_id = RandomGenerator::generate_random_int(1, MAXITEMS);
                    ol_supply_w_id = w_id;
                    clock->getDateTimestamp(ol_delivery_d);
                    ol_quantity = 5;
                    ol_amount = 0.5;
                    RandomGenerator::generate_random_str(ol_dist_info, 24);

                    outfile << ol_o_id << "," << ol_d_id << "," << ol_w_id << "," << ol_number << "," << ol_i_id << "," << ol_supply_w_id << ",";
                    outfile << ol_delivery_d << "," << ol_quantity << "," << ol_amount << "," << ol_dist_info << std::endl;
                }
            }
        }
        outfile.close();
    }
};

class Item {
public:
    int i_id;
    int i_im_id;
    char i_name[25];    // varchar
    float i_price;
    char i_data[51];    // varchar
    std::vector<std::string> sqls;

    Item() {
        sqls.clear();
    }

    void generate_insert_sql() {
        generate_table_data();
    }

    void generate_table_data() {
        std::string sql;
        for(i_id = 1; i_id <= MAXITEMS; ++i_id) {
            // if(i_id % 40 == 1) sql = "insert into item values ";
            // else sql += ", ";
            sql = "insert into item values ";
            
            i_im_id = RandomGenerator::generate_random_int(1, 10000);
            // RandomGenerator::generate_random_varchar(i_name, 14, 24);
            RandomGenerator::generate_random_str(i_name, 24);
            // i_price = (float)RandomGenerator::generate_random_int(100, 10000) / 100.0;
            i_price = RandomGenerator::generate_random_float(100, 1000);
            // RandomGenerator::generate_random_varchar(i_data, 26, 50);
            RandomGenerator::generate_random_str(i_data, 50);

            sql += "(";
            sql += std::to_string(i_id) + ", " + std::to_string(i_im_id) + ", '";
            sql.append(i_name);
            sql += "', " + std::to_string(i_price) + ", '";
            sql.append(i_data);
            sql += "')";

            // if(i_id % 40 == 0) sql += ";", sqls.push_back(sql);
            sql += ";", sqls.push_back(sql);
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "i_id,i_im_id,i_name,i_price,i_data" << std::endl;
        outfile.close();
        write_data_into_file(file_name);
    }

    void write_data_into_file(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(i_id = 1; i_id <= MAXITEMS; ++i_id) {
            i_im_id = RandomGenerator::generate_random_int(1, 10000);
            // RandomGenerator::generate_random_varchar(i_name, 14, 24);
            RandomGenerator::generate_random_str(i_name, 24);
            // i_price = (float)RandomGenerator::generate_random_int(100, 10000) / 100.0;
            i_price = RandomGenerator::generate_random_float(100, 1000);
            // RandomGenerator::generate_random_varchar(i_data, 26, 50);
            RandomGenerator::generate_random_str(i_data, 50);

            outfile << i_id << "," << i_im_id << "," << i_name << "," << i_price << "," << i_data << std::endl;
        }
        outfile.close();
    }
};

class Stock {
public:
    int s_i_id;
    int s_w_id;
    int s_quantity;
    char s_dist_01[25];
    char s_dist_02[25];
    char s_dist_03[25];
    char s_dist_04[25];
    char s_dist_05[25];
    char s_dist_06[25];
    char s_dist_07[25];
    char s_dist_08[25];
    char s_dist_09[25];
    char s_dist_10[25];
    float s_ytd;
    int s_order_cnt;
    int s_remote_cnt;
    char s_data[51];    // varchar
    std::vector<std::string> sqls;

    Stock() {
        sqls.clear();
    }

    void generate_insert_sql() {
        for(int i = 1; i <= NUM_WARE; ++i) {
            generate_table_data(i);
        }
    }

    void generate_table_data(int w_id) {
        s_w_id = w_id;
        std::string sql;
        for(s_i_id = 1; s_i_id <= MAXITEMS; ++s_i_id) {
            // if(s_i_id % 20 == 1) sql = "insert into stock values ";
            // else sql += ", ";
            sql = "insert into stock values ";

            s_quantity = RandomGenerator::generate_random_int(10, 100);
            RandomGenerator::generate_random_str(s_dist_01, 24);
            RandomGenerator::generate_random_str(s_dist_02, 24);
            RandomGenerator::generate_random_str(s_dist_03, 24);
            RandomGenerator::generate_random_str(s_dist_04, 24);
            RandomGenerator::generate_random_str(s_dist_05, 24);
            RandomGenerator::generate_random_str(s_dist_06, 24);
            RandomGenerator::generate_random_str(s_dist_07, 24);
            RandomGenerator::generate_random_str(s_dist_08, 24);
            RandomGenerator::generate_random_str(s_dist_09, 24);
            RandomGenerator::generate_random_str(s_dist_10, 24);
            s_ytd = 0.5;
            s_order_cnt = 0;
            s_remote_cnt = 0;
            // RandomGenerator::generate_random_varchar(s_data, 26, 50);
            RandomGenerator::generate_random_str(s_data, 50);

            sql += "(";
            sql += std::to_string(s_i_id) + ", " + std::to_string(s_w_id) + ", " + std::to_string(s_quantity) + ", '";
            sql.append(s_dist_01);
            sql += "', '";
            sql.append(s_dist_02);
            sql += "', '";
            sql.append(s_dist_03);
            sql += "', '";
            sql.append(s_dist_04);
            sql += "', '";
            sql.append(s_dist_05);
            sql += "', '";
            sql.append(s_dist_06);
            sql += "', '";
            sql.append(s_dist_07);
            sql += "', '";
            sql.append(s_dist_08);
            sql += "', '";
            sql.append(s_dist_09);
            sql += "', '";
            sql.append(s_dist_10);
            sql += "', " + std::to_string(s_ytd) + ", " + std::to_string(s_order_cnt) + ", " + std::to_string(s_remote_cnt) + ", '";
            sql.append(s_data);
            sql += "')";

            // if(s_i_id % 20 == 0) sql += ";", sqls.push_back(sql);
            sql += ";", sqls.push_back(sql);
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "s_i_id,s_w_id,s_quantity,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,";
        outfile << "s_dist_10,s_ytd,s_order_cnt,s_remote_cnt,s_data" << std::endl;
        outfile.close();
        for(int i = 1; i <= NUM_WARE; ++i) {
            write_data_into_file(file_name, i);
        }
    }

    void write_data_into_file(std::string file_name, int w_id) {
        s_w_id = w_id;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(s_i_id = 1; s_i_id <= MAXITEMS; ++s_i_id) {
            s_quantity = RandomGenerator::generate_random_int(10, 100);
            RandomGenerator::generate_random_str(s_dist_01, 24);
            RandomGenerator::generate_random_str(s_dist_02, 24);
            RandomGenerator::generate_random_str(s_dist_03, 24);
            RandomGenerator::generate_random_str(s_dist_04, 24);
            RandomGenerator::generate_random_str(s_dist_05, 24);
            RandomGenerator::generate_random_str(s_dist_06, 24);
            RandomGenerator::generate_random_str(s_dist_07, 24);
            RandomGenerator::generate_random_str(s_dist_08, 24);
            RandomGenerator::generate_random_str(s_dist_09, 24);
            RandomGenerator::generate_random_str(s_dist_10, 24);
            s_ytd = 0.5;
            s_order_cnt = 0;
            s_remote_cnt = 0;
            // RandomGenerator::generate_random_varchar(s_data, 26, 50);
            RandomGenerator::generate_random_str(s_data, 50);

            outfile << s_i_id << "," << s_w_id << "," << s_quantity << "," << s_dist_01 << "," << s_dist_02 << "," << s_dist_03 << ",";
            outfile << s_dist_04 << "," << s_dist_05 << "," << s_dist_06 << "," << s_dist_07 << "," << s_dist_08 << "," << s_dist_09 << ",";
            outfile << s_dist_10 << "," << s_ytd << "," << s_order_cnt << "," << s_remote_cnt << "," << s_data << std::endl;
        }
        outfile.close();
    }
};

#endif