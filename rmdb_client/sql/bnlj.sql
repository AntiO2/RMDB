create table t ( id int , t_name char (3));
create table d (d_name char(5),id int);
insert into t values (1,'aaa');
insert into t values (2,'baa');
insert into t values (3,'bba');
insert into d values ('12345',1);
insert into d values ('23456',2);
select * from t, d;
select t.id,t_name,d_name from t,d where t.id = d.id;
select t.id,t_name,d_name from t join d where t.id = d.id;

drop table t;
drop table d;