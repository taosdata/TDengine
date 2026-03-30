# Increase the maximum row length to 64K

## 1. Backgrounds

- Since TDengine version 2.6.0.32, the maximum row length has increased from 48K to approximately 64K(65531), and the maximum data column length of varchar/nchar has increased from 16K to approximately 64K(65517). 
- Tag column length and tag row len have no changes.
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
From a technical point of view, it is possible to support the maximum row length to more than 64K, because we use int32_t to store the row length from 3.0.
</callout>

- The maximum row/column length of 3.0 should be consistent with 2.6.
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
This is only applicable to data columns, and the maximum length of tag columns for 2.6/3.0 is still 16K.
</callout>

- Below list the `maximum length` with unit bytes(N.B. The column length listed below does not include 2 bytes VarDataHead):

  | version | max col num(exclude tag) | row max len | column max len | max tag num | tag row max len | tag column max len |
| --- | --- | --- | --- | --- | --- | --- |
| 2.6 | 4096 - 128 | 65531 | 65517 | 128 | 16384 | 16382 |
| 3.0 | 4096 - 128 | 65531 | 65517 | 128 | 16384 | 16382 |

## 2. Reference

- [TD-21161](https://jira.taosdata.com:18080/browse/TD-21161)  Increase the maximum row length to 64K
- [TS-2081](https://jira.taosdata.com:18080/browse/TS-2081) 【五凌电力】行长度64k限制版本

## 3. Grammar

- SQL/Stmt/Schemaless should all support up to 64K.
- Below describes the basic examples in SQL:

### 3.1 create stable/ntable

```sql
create table if not exists stb (ts timestamp, c1 int, c2 binary(65517)) tags (t1 int);
create table if not exists ntb (ts timestamp, c2 binary(65517));
```

### 3.2 insert data

```sql
insert into ntb values(now,'.... 65517 characters at max ...');
```

### 3.3 query

```sql
taos> select * from ntb;
           ts            |               c2               |
===========================================================
 2023-04-20 19:44:07.474 | ...65517 characters at max ... | 
```

## 4. Taos tools

- taosBenchmark and connectors in various languages should support the maximum length metioned above.

## 5. APIs/Stream

- The APIs and stream should support the maximum length mentioned above.
