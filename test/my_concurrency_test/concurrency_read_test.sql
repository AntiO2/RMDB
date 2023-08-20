preload 6
create table test_table (id int, name char(10), age int);
create table test_table2 (id int, name char(10), age int);
insert into test_table values (1, 'Alice', 25);
insert into test_table values (2, 'Bob', 30);
insert into test_table2 values (3, 'Chalice', 25);
insert into test_table2 values (4, 'David', 30);
create index test_table(id);
create index test_table2(id);
txn1 4
t1a begin;
t1b select * from test_table where id = 1;
t1c select * from test_table2 where id = 3;
t1d commit;

txn2 4
t2a begin;
t2b select * from test_table where id = 2;
t2c select * from test_table2 where id = 4;
t2d commit;

txn3 4
t3a begin;
t3b select * from test_table where id = 1;
t3c select * from test_table2 where id = 3;
t3d commit;

txn4 4
t4a begin;
t4b select * from test_table where id = 2;
t4c select * from test_table2 where id = 4;
t4d commit;

permutation 16
t1a
t2a
t3a
t4a
t1b
t2b
t3c
t4c
t3b
t4b
t1c
t2c
t3d
t2d
t4d
t1d