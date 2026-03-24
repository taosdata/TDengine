# 表过期TTL增强

TD-23739

## 1. 背景介绍

### 1.1 原有行为

摘自：https://jira.taosdata.com:18090/display/DEV/TTL+and+KEEP+of+Tables
TTL (Time to Live)是用户用来指定表的生命周期的参数。当表指定该参数后，超过 到期时间，则 TDengine 会自动删除该表。
1. 只有创建子表和普通表的语法可以设置 TTL 参数，超级表可设置，但不会生效。
2. 用户指定子表的 TTL 为1天，如下：
```sql
CREATE TABLE t using st tags("ggg") TTL 1;
```

1. 用户指定普通表的 TTL，如下：
```sql
CREATE TABLE t (ts TIMESTAMP, a INT) TTL 10;
```

1. 允许修改 TTL，改动SQL如下：
```sql
ALTER TABLE t TTL 10;
```

1. 通过 insert into 自动创建的子表采用默认的 TTL 参数0（不删除），可以修改。
2. 没有设置 TTL 参数的采用默认 TTL 参数0 (不删除)。
3. TTL 参数的单位是 天。 **到期时间为 表创建时间 加上 TTL 时间**。

### 1.2 需求描述

参照 [TTL 行为讨论会](https://taosdata.feishu.cn/wiki/wikcnWvg628axtaogJnUac3FCRP) ，到期时间 可被设置为动态参数，自动随 **变化时间**** **改变。

## 2. 目标行为

当一个表指定了 TTL 参数后，超过 到期时间，则 TDengine 会自动删除该表。
1. **到期时间(delete time) 为 变化时间(change time) 加上 TTL 时间**。变化时间 默认值等于** **表创建时间(birth time)**。**
2. 只有创建子表和普通表的语法可以设置 TTL 参数，超级表不可设置。
3. TTL 参数的单位是 天。TTL 可修改，没有设置 TTL 参数的采用默认 TTL 参数0 (不删除)。
4. btime 与 ctime 为服务器本地时间，与业务内容无关。
5. 全局参数 **ttlChangeOnWrite**** **决定 **到期时间 **是否伴随表的 [**修改操作**](https://taosdata.feishu.cn/wiki/wikcnuCilouVKIDYaEoFq0JT6Me#part-Pb6Qdm3KmobCnKxJYTFclRLYnlh) 改变。
6. 用户指定子表的 TTL 为1天，如下：
```sql
CREATE TABLE t using st tags("ggg") TTL 1;
```

1. 用户指定普通表的 TTL，如下：
```sql
CREATE TABLE t (ts TIMESTAMP, a INT) TTL 10;
```

1. 允许修改 TTL，改动SQL如下：
```sql
ALTER TABLE t TTL 10;
```

### 2.1 **修改操作**

若 **ttlChangeOnWrite **为 true，所有 INSERT/DELETE/ALTER 操作将同步修改 到期时间。
1. 用户向表插入数据等 INSERT 操作，如下：
```sql
INSERT INTO t VALUES (NOW, 1);
```

1. 用户从表删除数据等 DELETE 操作，如下：
```sql
DELETE FROM t WHERE ts = '2023-04-21 08:11:11.121';
```

1. 用户新增表列等 ALTER 操作，如下：
```sql
ALTER TABLE t ADD COLUMN b INT;
```

## 3. 备注

1. 该功能修改了tsdb的文件结构。兼容老客户端，但不能同时打开 **ttlChangeOnWrite**** = true。**否则的话，需要同时升级客户端。
2. 测试人员可通过控制 taos.cfg 中以下 internal 参数，调整 ttl 生效周期：

| 名称 | 说明 | 默认值 |
| --- | --- | --- |
| ttlUnit | ttl 设置值的单位 | 86400，单位秒，当前代表一天 |
| ttlPushInterval | ttl 过期检查触发间隔 | 3600，单位秒，当前代表一小时 |

```cpp

## 4. cat taos.cfg

ttlUnit         1
ttlPushInterval 60

taos -s 'CREATE TABLE t (ts TIMESTAMP, a INT) TTL 10;'
```

Mnode 每 60s 向 vnode 发起 删除过期表请求。
表 t 的到期时间为 now + 10s。
