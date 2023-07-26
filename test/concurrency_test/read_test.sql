preload 4
create table concurrency_test (id int, name char(8), score float);
insert into concurrency_test values (1, 'xiaohong', 90);
insert into concurrency_test values (2, 'xiaoming', 95);
insert into concurrency_test values (3, 'zhanghua', 88);

txn1 5
t1a begin;
t1b select * from concurrency_test;
t1c select * from concurrency_test where id = 2;
t1e select * from concurrency_test where score = 88;
t1d commit;

txn2 4
t2a begin;
t2b select * from concurrency_test;
t2c select * from concurrency_test where id = 3;
t2d commit;

txn3 4
t3a begin;
t3b select * from concurrency_test;
t3c select * from concurrency_test where id = 1;
t3d commit;

txn4 4
t4a begin;
t4b select * from concurrency_test;
t4c select * from concurrency_test where id = 4;
t4d commit;

txn5 1
t5a insert into concurrency_test values (4, 'xiaoguai', 100);

txn6 3
t6z begin;
t6a select * from concurrency_test;
t6b commit;

txn7 3
t7a begin;
t7b select * from concurrency_test;
t7c commit;

permutation 24
t1a
t2a
t3a
t1b
t1e
t2b
t3b
t2c
t3c
t1c
t3d
t1d
t2d
t5a
t4a
t4b
t4c
t4d
t6z
t7a
t6a
t7b
t6b
t7c