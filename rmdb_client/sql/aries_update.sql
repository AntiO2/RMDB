create table t1 (id int, num int);
insert into t1 values(1, 1);
begin;
insert into t1 values(2, 2);
abort;
begin;
insert into t1 values(3, 3);
commit;
begin;
update t1 set num = 4 where id > 0;
abort ;
select * from t1;
crash