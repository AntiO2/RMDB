## TODO

[x] 327 插入问题

有考虑 `where a=3 and a>1 and a<3`的情况吗

### 问题

同时删除和插入时死锁排查，删除加锁好像有问题

## 最左匹配

```
for indexmeta: tabmeta.indexmetas {
	
}
```

## Test

### drop table

```sql
create table t(a int);
create index t(a);
drop table t;
```

### create index

```
create table warehouse (w_id int, name char(8));
insert into warehouse values (10 , 'qweruiop');
insert into warehouse values (534, 'asdfhjkl');
insert into warehouse values (100,'qwerghjk');
insert into warehouse values (500,'bgtyhnmj');
create index warehouse(w_id);
select * from warehouse where w_id = 10;
select * from warehouse where w_id < 534 and w_id > 100;
drop index warehouse(w_id);
create index warehouse(name);
```

or try

```
create table warehouse (w_id int, name char(8));
insert into warehouse values (10 , 'qweruiop');
insert into warehouse values (534, 'asdfhjkl');
insert into warehouse values (100,'qwerghjk');
insert into warehouse values (500,'bgtyhnmj');
create index warehouse(name);
```

下面这个没问题，先删除索引再创建会死锁
