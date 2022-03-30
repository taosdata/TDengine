# 系统连接、任务查询管理

系统管理员可以从 CLI 查询系统的连接、正在进行的查询、流式计算，并且可以关闭连接、停止正在进行的查询和流式计算。CLI 里 SQL 语法如下：

```sql
SHOW CONNECTIONS;
```

显示数据库的连接，其中一列显示 ip:port, 为连接的 IP 地址和端口号。

```sql
KILL CONNECTION <connection-id>;
```

强制关闭数据库连接，其中的 connection-id 是 SHOW CONNECTIONS 中显示的第一列的数字。

```sql
SHOW QUERIES;
```

显示数据查询，其中第一列显示的以冒号隔开的两个数字为 query-id，为发起该 query 应用连接的 connection-id 和查询次数。

```sql
KILL QUERY <query-id>;
```

强制关闭数据查询，其中 query-id 是 SHOW QUERIES 中显示的 connection-id:query-no 字串，如“105:2”，拷贝粘贴即可。

```sql
SHOW STREAMS;
```

显示流式计算，其中第一列显示的以冒号隔开的两个数字为 stream-id, 为启动该 stream 应用连接的 connection-id 和发起 stream 的次数。

```sql
KILL STREAM <stream-id>;
```

强制关闭流式计算，其中的中 stream-id 是 SHOW STREAMS 中显示的 connection-id:stream-no 字串，如 103:2，拷贝粘贴即可。
