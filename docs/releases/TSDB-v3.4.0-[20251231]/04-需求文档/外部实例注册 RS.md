# 外部实例注册 RS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-15 | - | 0.1 | 关胜亮 | 新建 |
| 2025-10-15 | 2025-10-15 | 1.0 | 关胜亮 | 按评审记录修改 |

## 2. 引言

### 2.1 术语与缩写名词

1. taosAdapter: TDengine 的配套适配器组件，提供 RESTful、WebSocket 接口及多种数据采集代理软件的兼容支持。
2. taosd: TDengine 的核心守护进程，负责数据存储和查询处理。
3. mnode: TDengine 集群中的管理节点，负责元数据管理。

### 2.2 相关文档资料

JIRA: [TS-7431](https://jira.taosdata.com:18080/browse/TS-7431)
原始需求：[客户端版本兼容性解决方案](https://taosdata.feishu.cn/wiki/VTEuwbf6DiDIHCkAsxRcH0t7nUg) 第 4.3 节

### 2.3 优先级要求

高，期望在 2025-10-31 前完成。

### 2.4 版本要求

社区版和企业版都支持

## 3. 需求目标

设计并实现一套完整的外部实例注册与过期机制，具体目标如下：
1. 建立 taosAdapter 实例向 taosd 的主动服务注册机制
2. 实现 taosd 对已注册 taosAdapter 实例的心跳检测与健康状态监控
3. 提供实例列表查询接口，便于客户端实现负载均衡
4. 设计自动过期机制，及时清理不可用实例的注册信息
5. 确保机制的高性能，不对系统正常读写操作造成显著影响

## 4. 功能需求

### 4.1 外部实例信息表

1. 系统表名称：外部实例信息表 performance_schema.ins_instances
2. 系统表描述：在 mnode 内部创建内存表，用于存储外部实例的注册信息
3. 系统表字段：
   - `id`: 实例唯一标识（字符串，最大长度255）
   - `type`: 实例类型（如 taosAdapter、其他扩展组件）
   - `desc`: 实例描述信息
   - `first_reg_time`: 首次注册时间（timestamp 类型）
   - `last_reg_time`: 最后一次注册/心跳时间（timestamp 类型）
   - `expire`: 过期时间间隔（整数，单位秒）
4. 支持按照 instance_id 筛选
```sql {wrap}
select * from performance_schema.ins_instances where instance_id = '192.168.120.31:6030'
```

1. 支持 show 命令
```sql {wrap}
SHOW INSTANCES [LIKE 'pattern']

-- 只显示 instance_id 字段
```

1. 关键逻辑
   - taosAdapter 启动后注册到 taosd
   - taosc 随后定期通过已有心跳接口，向 taosd 发送注册消息
   - taosd 中的 mnode 将注册信息记录到 `ins_instances` 表
   - taosd 中的 mnode 对于超过过期时间间隔，且未再次更新跳跳的实例，自动从系统表中清除,
   - mnode 的 Leader 切换后，该列表自动清空，更待新的注册

### 4.2 实例管理 API

#### 4.2.1 实例注册

```sql {wrap}
int32_t taos_register_instance(
    const char* id, 
    const char* type,  
    const char* desc, 
    int32_t expire
);
```

#### 4.2.2 实例查看

```sql {wrap}
char** taos_list_instances(const char* filter_type); -- filter_type 可以为空
void   taos_free_instances(char** list);
```

## 5. 性能需求

### 5.1 系统表操作

1. 实例注册操作的平均响应时间应小于 20ms
2. 实例删除操作的平均响应时间应小于 20ms
3. 系统表的查询（存量 10 条记录）的平均响应时间小于 50ms

### 5.2 读写性能

1. 注册操作（存量 10 条记录）不应影响正常数据写入性能，吞吐量下降控制在 5% 以内
2. 注册操作（存量 10 条记录）不应影响正常数据查询性能，吞吐量下降控制在 5% 以内

## 6. 安全需求

1. 参照 [访问控制](https://taosdata.feishu.cn/wiki/Y12Ywd797ieHBBkVZsqcpsRgnAg) 需求，将 ins_instances 表的查询权限控制纳入到该任务的权限控制范围中

## 7. 其他需求

1. 兼容性需求：不涉及
2. 接口需求：已经在第 4.2 节中描述
3. 运维需求：无
4. 易用性需求：无
5. 测试需求：无特殊要求，常规测试
