# Grant 优化

## JIRA:

- [TD-21649](https://jira.taosdata.com:18080/browse/TD-21649) [3.0][grant] 企业版授权支持"过期时间/测点数"等改大改小.
- [TD-21650](https://jira.taosdata.com:18080/browse/TD-21650) [3.0][grant] 企业版授权增加支持 cluster id

## [TD-21649](https://jira.taosdata.com:18080/browse/TD-21649) 企业版授权支持"过期时间/测点数"等改大改小

### 2.1 基本规则

```sql {wrap}
1) 企业版授权支持"过期时间/测点数" 等改大改小;
   - 单节点/集群 vnode 1或3 副本/集群 mnode 1或3 副本: 支持改大改小;
2) 改大立即 (1分钟内) 生效, 改小(或无)延迟 1 小时 生效.
3) 如果集群中包含多个企业授权码, 则 "过期时间/测点数"等 指标取并集, 即所有授权码中相关指标的最大值.
```

<callout emoji="white_medium_small_square" background-color="light-orange" border-color="light-orange">
指标改小时, 延迟生效的原因: 考虑到有授权码的服务器异常, 或者要更新操作码, 或者升级服务器, 或者服务器搬迁等场景, 要保留必要的提醒或者操作时间)
</callout>

### 2.2 详细描述

```sql {wrap}
1) 生效规则为从小到大立即生效, 从大到小(或者无)则缓生效.
1.1) 如果集群处在未授权状态, 在配置授权码后, 则授权状态立即生效.
1.2) 如果集群处在授权状态, 但是新增加的授权码期限更长, 则期限更长的授权码立即生效.
1.3) 如果集群处在授权状态, 清除所有的授权码, 则需要 3600 秒 才会进入 trial 状态.
1.4) 如果集群有多个授权码, 清除期限更长的授权码, 则需要 3600 秒 才会启用期限短的授权码.
```

### 2.3 测试说明

- 测试效率说明
```sql {wrap}
1) 因为授权降级的时间为 3600 秒, 会导致测试周期较长, 可以通过修改代码中的 GRANT_CHECK_INTERVAL/GRANT_HEART_BEAT_MSG 加快测试流程.
     #define GRANT_CHECK_INTERVAL 3600 //3600seconds 
     #define GRANT_HEART_BEAT_MSG 60 //300seconds
```

- 异常条件测试示例
```sql {wrap}
1) mnode 3 副本, node 1 为 leader, 包含授权码 2023-01-31, node 2/3 follower, 不包含授权码或者授权时间较短(e.g. 2023-01-20). 
1.1) 关闭 node 1, 查询集群授权状态, 应该在 1 个小时之内, 延用 node 1 的授权码(2023-01-31), 1 个小时之后, 集群 grants 变为 trial 状态或者 2023-01-20. 
1.2) 启动 node 1, 集群在 1 分钟内, grants 恢复为 2023-01-31.
```

## [TD-21650](https://jira.taosdata.com:18080/browse/TD-21650) 企业版授权增加支持 cluster id

-  N.B. 以下输出均来自自测结果, 因此, 也作为测试报告使用.
- 除了支持根据 "机器码" 进行授权之外, 增加支持 cluster id 授权.  使用说明如下:

### 3.1 通过 taos 客户端输入 show cluster 获取 cluster id:

- 下述示例中, clusterId 为 5681662865067127406
```sql
taos> show cluster\G;
*************************** 1.row ***************************
         id: 5681662865067127406
       name: daec0ea1-5e9b-48b7-bdd6-26a8a45de3be
     uptime: 0
create_time: 2023-02-03 06:05:33.228
    version: trial
expire_time: 2023-04-05 06:05:33.000
Query OK, 1 row(s) in set (0.001252s)
```

### 3.2 通过 taosGrant -u ${clusterId} 生成授权码

- 下述示例中, 授权码为  z9sdqG8w67fqBXlHnxWAQezQc/mabvN9N2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdmAt3P46xKs4Q=
```sql {wrap}
root@u3-31 $ ./taosGrant_linux64 -u 5491178878311373698 -expire 2023-06-30 -timeseries 1000
--------------------------------------------------------------------------------
#ServerCode 5491178878311373698
activeCode 
z9sdqG8w67fqBXlHnxWAQezQc/mabvN9N2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdmAt3P46xKs4Q=
```

### 3.3 通过 taosGrant -u ${clusterId} -a {activeCode} 校验授权码

- 授权码校验成功示例
```sql {wrap}
root@u3-31 $ ./taosGrant_linux64 -u 5491178878311373698 -a z9sdqG8w67fqBXlHnxWAQezQc/mabvN9N2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdmAt3P46xKs4Q=
TDengine: official version
expire: 2023-06-30 1688054400
storage: unlimited
speed: unlimited
timeseries: 1000
querytime: unlimited
database: unlimited
user: unlimited
connection: unlimited
stream: unlimited
account: unlimited
dnode: unlimited
cpu: unlimited
```

- 授权码校验失败示例
```sql {wrap}
root@u3-31 $ ./taosGrant_linux64 -u 5491178878311373698 -a z9sdqG8w67dJWl7OxrPZ2OzQc/mabvN9N2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdmlNkl5iStffQ+
--------------------------------------------------------------------------------
failed to parse active code
```

### 3.4 在 taos.cfg 中配置授权码

- 在 taos.cfg 中按行添加, 格式为: activeCode ${activeCode}
```sql {wrap}

## 3.0 cluster 2023-06-30 1000

activeCode z9sdqG8w67fqBXlHnxWAQezQc/mabvN9N2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdmAt3P46xKs4Q=
```

- activeCode 动态生效, 不需要重启 taosd.
- 如果配置了多个 activeCode, 只有第一个会生效. 例如, 第一个为无效授权码, 即使后边存在有效的授权码, 整个集群仍然为无效状态.

### 3.5 查看 taosd 授权是否生效

- 授权码生效示例
```sql
taos> show grants\G;
*************************** 1.row ***************************
    version: official
expire_time: 2023-06-30 00:00:00
    expired: false
    storage: unlimited
 timeseries: 0/1000
  databases: unlimited
      users: unlimited
   accounts: unlimited
     dnodes: unlimited
connections: unlimited
    streams: unlimited
  cpu_cores: unlimited
      speed: unlimited
  querytime: unlimited
Query OK, 1 row(s) in set (0.001483s)

taos> show cluster\G;
*************************** 1.row ***************************
         id: 5681662865067127406
       name: daec0ea1-5e9b-48b7-bdd6-26a8a45de3be
     uptime: 300
create_time: 2023-02-03 06:05:33.228
    version: official
expire_time: 2023-06-30 00:00:00.000
Query OK, 1 row(s) in set (0.001284s)
```

- 授权码未生效示例
```sql
taos> show grants\G;
*************************** 1.row ***************************
    version: trial
expire_time: 2023-04-05 06:05:33.000
    expired: false
    storage: unlimited
 timeseries: unlimited
  databases: unlimited
      users: unlimited
   accounts: unlimited
     dnodes: unlimited
connections: unlimited
    streams: unlimited
  cpu_cores: unlimited
      speed: unlimited
  querytime: unlimited
Query OK, 1 row(s) in set (0.002376s)

taos> show cluster\G;
*************************** 1.row ***************************
         id: 5681662865067127406
       name: daec0ea1-5e9b-48b7-bdd6-26a8a45de3be
     uptime: 600
create_time: 2023-02-03 06:05:33.228
    version: trial
expire_time: 2023-04-05 06:05:33.000
Query OK, 1 row(s) in set (0.001302s)
```
