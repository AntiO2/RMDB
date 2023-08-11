create table t1 (id int, num int);
begin;
insert into t1 values(1, 1);
commit;
begin;
insert into t1 values(2, 2);
abort;
begin;
commit;
begin;
insert into t1 values(3,3);
crash