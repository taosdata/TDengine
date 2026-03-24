# IP 白名单功能

### 1. 原始需求

 来源： 长庆油田
需求列表：
   - 增加IP白名单的功能。 即使有账号/密码，但不在白名单的IP，不可以访问集群，在白名单中用户才可以访问集群。 
   - 支持IP段的控制，比如在一个范围中的IP。  
   - 可以动态增删更改白名单 （root 用户）
   - 可以查看的IP白名白名单（root 用户有权限查看）
**当前为了应对他们那边即将到来的安全审计，说服他们暂且使用防火墙的方式从nginx 入口处做限制，还没有实施， **
另外，据说这个安全审计比较严格，由公安部的人来长庆做漏洞检测，如果不能通过，可能导致项目被暂停。 

### 2. 方案

 涉及mnode 、RPC及其taosadapter。 mnode 负责增删查IPWhiteList，RPC负责利用IPWhiteListCache校验Req 的IP（主要是校验通过native 接口过来的请求）， taosadapter 利用本地的IPWhiteListCache 负责校验前端Req的IP 
 
#### 2.1 大致流程如下

  dnode 启动之后，mnode 向所有的dnode 去推送IPWhiteList, dnode 收到这个IPWhiteList 之后，更新本地RPC 的IPWhiteListCache， 如果mnode 上的IPWhiteList 发生了变更，则mnode 向所有的dnode推送这个IPWhileList 到各个dnode，各个dnode 更新本地RPC 的IPWhiteListCache（IP 白名单变更是很低频的操作，发生变更的时候，mnode 主动推送可能更好）
    通过taosadapter 来的请求，当前的IP 并不是用户的IP，而是taosadapter所在机器的IP。  因此，taosadapter 可以周期性从taosd 拉取IPWhiteList,  并通过这个IPWhiteList 去校验用户的请求（IP白名单变更是很低频的操作， taoadapter 可以通过taos-c-driver 和mnode 之间的heartbeat 信息,  来获取当前mnode 的IPWhiteList 是不是发生了变更，当检测实际发生了变更，才去主动拉取， 而不是taosadapter 周期性去拉取mnode 拉取）
~~ 1.   Dnode 启动之后，dnode mnode 调用接口去更新~~~~RPC~~~~模块的WhiteListCache, 之后RPC利用这个WhiteListCache 验证WhiteListCache 合法行。 ~~
~~ 2.  用户动态动态更改了mnode 上的IP白名单之后，mnode 调用接口去更新~~~~RPC~~~~ 模块的WhileListCache。 ~~
~~ 3.  如果当前dnode上没有mnode，则需要在该dnode 启动之后，去mnode 拉一次IP白名单，并更新调用接口去更新~~~~RPC~~~~ 模块的WhileListCache,  ~~
~~  4.  如果当前dnode 上没有mnode， 并且如果mnode 上的IP白名单发生了变更的时候，dnode 需要得知这个信息，并更新本地PRC 的WhileListCache ~~
   
#### 2.2 可能的问题

    业务 --> nginx --> taosadapter --> taosd ，taosadapter 要想拿到客户端的原始 IP ，需要在 nginx 中配置参数 proxy_set_header X-Real-IP $remote_addr;

#### 2.3 主要可能涉及的工作

   - 新增一套权限管理的接口（是不是要和当前的权限管理的内容统一？）及其新增一套增删改查相关的SQL语法
   - mnode 增加 WhiteList 的存储、更改和查询
   - RPC 内部更新WhileListCache 机制 , 对来的请求进行校验. 
   - taosadapter 利用拉取到的WhiteList 对来的请求进行校验。
