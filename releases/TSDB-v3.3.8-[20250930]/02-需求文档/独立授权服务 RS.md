# 独立授权服务 RS

## 1. 引言

### 1.1 术语与缩写名词

1. AuthTD：提供授权服务 TDengine 集群 
2. AuthServer：AuthTD 提供的授权服务
3. ClientTD：被授权服务授权的 TDengine 集群
4. AuthClient：从 ClientTD 向 AuthTD 发送授权请求的客户端

### 1.2 相关文档资料

JIRA [TS-6666](https://jira.taosdata.com:18080/browse/TS-6666)

### 1.3 优先级要求

高

### 1.4 版本要求

仅企业版支持

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/07/18 | 1.0 | 关胜亮 | 新建 |

## 3. 需求目标

1. AuthTD 为 ClientTD 提供授权服务
2. ClientTD 的授权总量不超过 AuthTD 的授权总量
3. AuthTD 支持高可用
4. AuthTD 是一个特殊的 TDengine 版本，不提供常规读写服务

## 4. 功能需求

### 4.1 AuthTD 工作流程

AuthTD 和普通集群一样进行授权，但授权信息为各个 ClientTD 共享。AuthTD 中的 AuthServer 服务和 Arbitrator 服务相似，由 AuthTD 的 Mnode Leader 启动。ClientTD 和 AuthTD 进行通信，通信的主要内容采用文本，以便于扩展
1. 请求:
   - ClusterID
   - 集群的基本信息（机器码、FQDN、FirstEP 等）
   - 集群的状态信息（创建时间、启动时间、当前用量）
   - 用于校验的信息
2. 回复
   - 授权信息
   - 用于校验的信息
AuthTD 接收到 ClientTD 请求后，将收到的信息写到子表。子表已经被提前创建，归属 Auth 数据库的 Grants 超级表，子表名称基于 ClusterID。结构示例
1. 数据列
   - 本次请求时间
   - 当前授权时间
   - 当前授权状态
   - 当前授权用量
   - 本次请求是否更新了授权信息
   - 集群的基本信息（机器码、FQDN、FirstEP 等）
   - 集群的状态信息（创建时间、启动时间、当前用量）
2. 标签列
   - ClusterID
   - 是否生效
   - 授权用量
AuthTD 对于请求的处理逻辑
1. 如果子表不存在：返回错误
2. AuthServer 发现 ClientTD 无授权或者授权接近过期时，发放新的授权
   - 授权时间：30 天
   - 发放时机：距离过期时间 15 天，可配置但最大为 30 天
   - 授权用量：读取  Auth.Grants 下子表的 “授权用量”
3. AuthServer 在每次收到请求时写入时序数据
管理授权
1. 新增授权，Root 用户依据 ClusterID 创建子表，写入“授权用量”、"是否生效"字段
2. 如想禁用某个 Cluster 的授权，Root 用户将子表中的 “是否授权” 标签设置为 false
3. 如想修改某个 Cluster 的用量，Root 用户将修改子表中的“授权用量”标签值
4. 修改标签、创建子表时，要进行总量校验（由于 AuthTD 是一个特殊的 TD 版本，可使用宏） 
5. 以上若干步骤，通常在交付部门的部署脚本在第一次部署集群时设置，后续修改同样建议通过部署脚本进行

### 4.2 ClientTD 流程

ClientTD 启动 AuthClient，定期向 AuthServer 发送授权请求。
1. AuthClient 服务和 Arbitrator 服务相似，由 ClientTD 的 Mnode Leader 调度
2. AuthClient 服务读取 taos.cfg 文件中的如下配置项
   - 是否发送授权请求
   - 发送授权请求的间隔
   - 发送授权请求的目标地址
3. AuthClient 按照配置信息向 AuthServer 发送授权请求，接收到授权回复后，更新集群状态
4. AuthTD 可随时通过 Show Grants 等系列命令查看当前授权状态，并需要显示当前授权时间和下次续期时间

## 5. 性能需求

无

## 6. 安全需求

ClientTD 和 AuthTD 之间的通信内容需加密，加密方法不放在社区版中。通信中需要增加时间戳等信息，避免消息重放造成的干扰。

## 7. 其他需求

为了避免 AuthTD 被滥用，考虑在授权码上加一个新授权项，判断一个 TD 集群是否能成为 AuthTD。
