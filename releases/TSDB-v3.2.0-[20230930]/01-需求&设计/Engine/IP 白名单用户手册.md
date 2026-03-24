# IP 白名单用户手册

### 1 权限

只有 root 用户能够修改和查询白名单。非root 用户只能查询白名单。

### 2 语法

#####   2.1 创建白名单

```sql {wrap}
CREATE USER user_name PASS password [SYSINFO value] [HOST host_name1[,host_name2]]     
```

    字段说明
       user_name: 新用户名，如果存在，则报错
       host_nameX：实际 IP 或者IP范围，用子网掩码的方式 
    举例 
       CREATE USER test PASS 'a' HOST "127.0.0.0/24"，"192.168.0.23"

#####    2.2 修改白名单

**    **` ALTER USER user_name ADD HOST host_name1`
    字段说明
       user_name: 是实际的存在user名，如果不存在，则报错
       host_name1: 实际 IP 或者IP范围，用子网掩码的方式 
    举例 
       ATLER USER root ADD HOST "127.0.0.0/24"

#####  2.3 删除白名单

     `ALTER USER user_name DROP HOST ``host_name1`
   字段说明 
      user_name: 是实际的存在user名，如果不存在，则报错.  
      host_name1: 实际 IP 或者IP范围，用子网掩码的方式表表示
   举例 
       alter user root drop host "127.0.0.5"

##### 2.4 删除用户

`   ``DROP USER`
如果一个用户被删除，则系统中与该用户有关的白名单信息都会被删除

#####  2.5 查询当前白名单

   `select user_name, allowed_host from ins_users;`

#### 3 错误码 

    TSDB_CODE_MND_USER_HOST_EXIST "Host already exist in ip white list" ， 对一个user, 添加重复IP, 如果添加了一次，第二次会报错. 
   TSDB_CODE_MND_USER_HOST_NOT_EXIST,      "Host not exist in ip white list， 对一个user, 删除一个不在IP  white list 的IP
    TSDB_CODE_MND_TOO_MANY_USER_HOST,       "Too many host in ip white list"， 对一个user, 添加的IP数目到了上限，上限是2048
    TSDB_CODE_MND_USER_LOCAL_HOST_NOT_DROP,  "Host can not be dropped",  尝试删除ip white list 中的127.0.0.1 
    TSDB_CODE_IP_NOT_IN_WHITE_LIST， "Not allowed to connect"， 不在白名单列表中用户尝试访问

#### 4. 一些边界和备注

1. 开源版和企业版本都能添加成功，且可以查询到，但是开源版本不会对IP做任何限制。
2. create user u_write pass 'taosdata1' host 'iprange1','iprange2', 可以一次添加多个iprange,  服务端会做去重，去重的逻辑是需要iprange 完全一样
3. 默认会把127.0.0.1 添加到白名单列表，且在白名单列表可以查询
4. 集群的节点IP集合会自动添加到白名单列表，但是查询不到。 
5. taosadaper 和 taosd 不在一个机器的时候，需要把taosadaper IP手动添加到taosd 白名单列表中
6. 集群情况下，各个节点 enableWhiteList 成一样，或者全为false,或者全为true, 要不然集群无法启动
7. 白名单变更生效时间1s，不超过2s, 每次变更对收发性能有些微影响（多一次判断，可以忽略），变更完之后、影响忽略不计, 变更过程中对集群没有影响，对正在访问客户端也没有影响（假设这些客户端的IP包含在white list内）
8. 如果添加两个ip range,  192.168.1.1/16(假设为A), 192.168.1.1/24(假设为B), 严格来说，A包含了B，但是考虑情况太复杂，并不会对A和B做合并
9. 要删除的时候，必须严格匹配。 也就是如果添加的是192.168.1.1/24, 要删除也是192.168.1.1/24 
10. 只有root 才有权限对其他用户增删ip white list
11. 兼容之前的版本，但是不支持从当前版本回退到之前版本
12. x.x.x.x/32 和x.x.x.x 属于同一个iprange, 显示为x.x.x.x
13. 如果客户端拿到的 0.0.0.0/0, 说明没有开启白名单。
14. 如果白名单发生了改变， 客户端会在heartbeat里检测到。 
15. 针对一个user, 添加的IP个数上限是2048

### 5 配置

1. taosd 的配置：taos.cfg 中添加 enableWhiteList 的全局配置，该配置项在所有 dnode 上必须强一致，如果不一致则 dnode 会启动失败并报错。如果该选项被配置，则企业版只有在白名单内的 (user, IP) 组合能够访问数据库，社区版无影响（所有IP都可访问）。
2. 如果 enableWhiteList = true，则 (root, dnode1) 到 (root,dnodeN)会被自动加入白名单 ，即集群中所有 dnode 的 IP 会被自动加入 root 的白名单， 如果是非集群列表中IP接入进来，即使是root用户，也需要 `ALTER USER root Add HOST` 显式的添加到白名单中，否则直接报错 `whitelist prohibits current IP acces` 。 
3. 如果一个用户（假定 userA) 可能从 IP1 通过 taosAdapter （假定所在 IP 为 IP2）访问数据库，则首先 （userA, IP2） 要加入白名单，否则 taosAdapter 无法建立与 taosd 的连接。同时 (userA，IP1）也要加入白名单，否则 userA 从 IP1 上发出的请求会被拒绝。
4. 正确的配置组合：taosd 的所有节点都开启或者关闭enableWhitelist

### 6 Use Cases

##### 6.1 客户端直接连接 taosd（enableWhitelist只是服务端的配置）

1. 如果 enableWhiteList = false，行为与现在相同，只要用户名密码有效即可建立连接
2. 如果 enableWhiteList = true，客户端所在 IP 不在该用户的 白名单内，则连接会被拒绝
3. 如果 enableWhiteList = true，客户端所在 IP 在该用户的白名单内，则能够建立连接进行访问；
4. 如果 enableWhiteList = true，但 root 修改了白名单将已经建立连接的某 IP 从白名单中移除，则已经建立的连接会被服务端主动断开。

##### 6.2 白名单被修改

前面几个小节中已经描述了不同场景下白名单被修改后的行为。总地来说，白名单被修改后，mnode 会通知所有 dnode 新的白名单，并根据新的白名单来检查已经建立的连接，如果有不在最新白名单中的 (user, IP) 所建立的连接则服务端会主动断开这些连接（此行为对 taosd 和 taosAdapter/websocket 均成立）。taosAdapter 会根据新的白名单来决定是否处理收到的 REST 请求，是否断开已经存在的某些连接，是否接受新的 websocket 连接请求。

### 7 兼容性

 本次修改在`ins_users`表中添加了一个字段，能够支持在之前版本上升级，但不支持回退

### 8 taos.h api 修改

当whitelist 版本变化后, 客户端可以得到通知
```cpp {wrap}
typedef enum {
  TAOS_NOTIFY_PASSVER = 0,
  TAOS_NOTIFY_WHITELIST_VER = 1
} TAOS_NOTIFY_TYPE;
DLL_EXPORT int taos_set_notify_cb(TAOS *taos, __taos_notify_fn_t fp, void *param, int type);
```

通过以下api获得更新后的白名单
```cpp {wrap}
typedef void (*__taos_async_whitelist_fn_t)(void *param, int code, TAOS *taos, int numOfWhiteLists, uint64_t* pWhiteLists);
DLL_EXPORT void taos_fetch_whitelist_a(TAOS *taos, __taos_async_whitelist_fn_t fp, void *param);
```
