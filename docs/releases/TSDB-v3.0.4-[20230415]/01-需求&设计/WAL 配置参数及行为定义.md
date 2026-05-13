# WAL 配置参数及行为定义

## 1. WAL 相关参数及含义

**WAL_RETENTION_PRERIOD**： WAL 最大数据保留时长，单位为秒。默认值为 0 表示及时删除，即 VNODE 落盘持久化后就删除。
**WAL_RETENTION_SIZE：**WAL 保留消息数据量的最大上限，单位为 KB。默认值 0 表示无上限限制。
***WAL_SEGMENT_SIZE**** ：**WAL** 单个文件大小，单位为 KB。**（****注：保留，供内部测试调优使用，对用户隐藏，从对外文档中删除****）*

## 2. WAL 行为 

WAL_RETENTION_PERIOD 和 WAL_RETENTION_SIZE 在都指定的情况下，任何一个条件触发都会导致 WAL 数据的删除。在 WAL_RETENTION_SIZE 的范围内，若有消息超过了 WAL_RETENTION_PERIOD 设置的保留时长，则超过部分消息数据会被删除。在 WAL_RETENTION_PERIOD 范围内，若 WAL 消息文件总大小超过WAL_RETENTION_SIZE 指定的大小，则超过部分会被删除。
**注意**：
- 对于订阅功能而言，WAL清理行为，仅受WAL_RETENTION_PERIOD和WAL_RETENTION_SIZE两个参数控制；不再受订阅注册和消费行为或状态的影响。换句话说，即使全部订阅已经消费，WAL日志也不会被提前清理；即使订阅未消费，当两个参数条件满足一个时，WAL日志也不会被继续保留。
- 如果存在topic，那么alter database WAL_RETENTION_PERIOD 不允许设置为0. 同时， 如果WAL_RETENTION_PERIOD 0, 那么不允许创建topic。
- 在版本升级场景下，如果topic已经存在，且WAL_RETENTION_PERIOD 为0， 那么保留之前的行为方式，不变（i.e. WAL在消费后才会删除）。
- 如果topic存在且WAL_RETENTION_PERIOD非0， 那么按照新的逻辑清理WAL，清理行为不再依赖于订阅消费行为。 

## 3. 配置选项对消息订阅的影响

1. 使用消息队列订阅功能需要用户在创建 DB 时根据业务需求指定 WAL_RETENTION_PRERIOD 或在创建 DB 后通过 alter database 命令修改该配置
2. 待实现：WAL 数据保留不受订阅客户端的影响，在保证内部系统正常运行的前提下，当用户指定的参数生效时，WAL 数据会被删除。若消息队列消费数据太慢，导致数据被删除，则默认消费现存最新的消息队列数据。
3. 订阅需提供一个选项，在订阅数据时，由于保留策略删除数据，导致订阅数据有缺失时情况下报错。
