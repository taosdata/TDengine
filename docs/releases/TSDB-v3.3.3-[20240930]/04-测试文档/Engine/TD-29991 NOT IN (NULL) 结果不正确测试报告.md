# TD-29991 NOT IN (NULL) 结果不正确测试报告

## 1. 测试目标

select 1 not in (2, NULL)结果应该是 false，目前会crash
不仅是常量，列也需要考虑。

## 2. 变更历史

| Date | Version | Owner | Momo |
| --- | --- | --- | --- |
| 2024-08-01 | 1.0 | @黄帅 |  |

## 3. 测试结论

在fix/TD-29991/inNull_main分支上
- select 1 not in (2, NULL)结果是 false，且未出现crash现象
- 在where子句中使用not in 筛选列中常量时，其结果符合下面的预期：
1. in (2, null) : in 语句后的集合中，有null 结果和没有null一致；
2. not in (2, null)  语句中集合中如果有 null ，返回空结果，符合结果为false的定义； 

## 4. 已知问题和限制

无

## 5. 测试环境

- branch: fix/TD-29991/inNull_main
- client info: 3.3.3.0.alpha
- server info: ver:3.3.3.0.alpha
- build:Linux-x64 2024-07-31 11:01:12 +0800
- gitinfo:9f0a2ac3ba0f1fa46a130f10e65a16f3547f1fd8

## 6. 测试范围和方法

### 6.1 测试范围

1. 在命令行使用 `in (2, null)` 和 `not in (2, null)` 语句。
2. 在对表的列查询时where子句包含 `in (2, null)` 和 `not in (2, null)` 语句。

### 6.2 测试方法

1. 直接在taos shell里测试。
2. 建立数据表，其中部分数据含null，执行select语句，其中包含 `in (2, null)` 和 `not in (2, null)` 语句的where子句。

## 7. 测试数据

含null数据的td_29991.test表

| location | id | ts | result |
| --- | --- | --- | --- |
| beijing | 1 | now() | 10 |
| beijing | 1 | now() | null |
| beijing | 1 | now() | 9 |
| beijing | 1 | now() | 8 |
| beijing | 1 | now() | 7 |
| beijing | 1 | now() | 6 |
| beijing | 1 | now() | 5 |
| beijing | 1 | now() | 4 |
| beijing | 1 | now() | 3 |
| beijing | 1 | now() | 2 |
| beijing | 1 | now() | 1 |

不含null数据的td_29991.ttt表

| location | id | ts | result |
| --- | --- | --- | --- |
| nanjing | 2 | now() | 1 |
| nanjing | 2 | now() | 2 |
| nanjing | 2 | now() | 3 |
| nanjing | 2 | now() | 4 |
| nanjing | 2 | now() | 5 |

## 8. 测试用例

### 8.1 创建测试环境

```sql
create database td_29991;
create stable td_29991.meters(ts timestamp, result int) tags(location varchar(20), id int);
create table td_29991.test using meters tags ('beijing', 1);
insert into td_29991.test values(now(), 1);
insert into td_29991.test values(now(), 2);
insert into td_29991.test values(now(), 3);
insert into td_29991.test values(now(), 4);
insert into td_29991.test values(now(), 5);
insert into td_29991.test values(now(), 6);
insert into td_29991.test values(now(), 7);
insert into td_29991.test values(now(), 8);
insert into td_29991.test values(now(), 9);
insert into td_29991.test values(now(), 10);
insert into td_29991.test values(now(), NULL);

create table td_29991.ttt using meters tags ('nanjing', 2);
insert into td_29991.ttt values(now(), 1);
insert into td_29991.ttt values(now(), 2);
insert into td_29991.ttt values(now(), 3);
insert into td_29991.ttt values(now(), 4);
insert into td_29991.ttt values(now(), 5);
```

### 8.2 taos shell `NOT IN (NULL)` 测试

```sql
select 1 in (2, null);
```

```sql
select 1 in (2);
```

```sql
select 2 in (2, null);
```

```sql
select 1 not in (2, null);
```

```sql
select 2 not in (2, null);
```


### 8.3 select语句中含where子句的 `NOT IN (NULL)` 测试

```sql
select * from test where result in (1, 2, 3, null);
```

```sql
select * from test where result in (1, 2, 3);
```

```sql
select * from test where result not in (1, 2, 3);
```

```sql
select * from test where result not (1, 2, 3, null);
```

```sql
select * from ttt where result in (1, 2, 3, null);
```

```sql
select * from ttt where result in (1, 2, 3);
```

```sql
select * from ttt where result not in (1, 2, 3);
```

```sql
select * from ttt where result not (1, 2, 3, null);
```


## 9. 测试计划

20240801

##
