## TODO

[x] 327 插入问题

有考虑 `where a=3 and a>1 and a<3`的情况吗

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

