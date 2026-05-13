# 客户端注册实例 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-11-12 | - | 0.1 | 彭荣坤 | 新建 |
| 2025-11-21 | 2025-11-21 | 1.0 | 彭荣坤 | 发布 |

## 2. 背景

-  [TS-7431](https://jira.taosdata.com:18080/browse/TS-7431)
- [taosadapter高可用](https://taosdata.feishu.cn/wiki/VTEuwbf6DiDIHCkAsxRcH0t7nUg#share-KvQRdBhjWoKcyNxil6ucXrPTn2g)

## 3. 定义

1. instance：客户端可以向taosd主动注册的实例，信息均为用户指定。主要是给taosadapter使用，确保连接有可维护的状态

## 4. 行为说明

### 4.1 API说明

#### 4.1.1 注册函数

```c {wrap}
int32_t taos_register_instance(const char *id, const char *type,
                                const char *desc,int32_t expire);
```

- 功能：向配置的firstEp 所属集群的管理节点（mnode）注册一个运行中的外部组件/实例；若同名实例已存在，则刷新其 last_reg_time、type/desc、expire。
- 参数：
id：实例的唯一标识（最长 255 字节），不能为空或重复。
type：实例的类型标记（最长64字节，如 taosAdapter 等，可选；若传空保留原值）。
desc：实例的说明文字（最长512字节，可选；若传空保留原值）。
expire：实例过期时间（秒）。>0 表示超时自动清理；0 表示无过期；-1 用于注销（等价于 UNREGISTER）。
- 返回值：错误码
- 限制 / 注意事项：
该调用仅维持内存态信息，不写磁盘；需周期性调用以维持心跳。
expire 设置负值（除 -1 外）将导致参数错误。
需要保证调用时 mnode 可达。

#### 4.1.2 查询函数

```c {wrap}
int32_t taos_list_instances(const char *filter_type, char ***pList,                                               int32_t *pCount)
```

- 功能：查询当前集群mnode上所有未过期的实例 ID；可按实例类型过滤。
- 参数：
  filter_type：类型过滤条件（可为 NULL 或空字符串表示不过滤；大小写不敏感）。
  pList：返回实例id字符串数组的指针，不为NULL
  pCount：返回实例id字符串数组长度的指针，不为NULL
- 返回值：错误码
- 限制 / 注意事项：
仅返回 expire 未超时的实例；已过期信息不会显示。
若无匹配项，返回的数组长度为 0。
调用者需要调用释放函数释放list内存（失败时返回 NULL，无需释放）。

#### 4.1.3 释放函数

```c
void taos_free_instances(char ***pList, int count);
```

- 功能：释放查询返回的id字符串数组
- 参数
pList：实例id字符串数组指针，每个id必须以`\0`结尾。
count：实例id字符串数组的长度。
- 行为：遍历列表，针对每个 ID 调用注销 RPC；成功或失败都会继续释放本地内存。 - 所有字符串使用 taosMemoryFree 释放，最后释放列表本身。 - 返回值：无（void）。若注销失败，对应错误会写入 terrno 并在日志中打印警告。 - 限制 / 注意事项 - list 必须非 NULL 且来自 taos_list_instances；否则行为未定义。 - 即便某些注销 RPC 失败，函数仍会释放所有内存；调用者可通过 terrno 判断是否完全成功。 - 若 taos 断开，会立即释放列表，同时 terrno 置为 TSDB_CODE_TSC_DISCONNECTED。这些 API 设计为配套使用：taos_register_instance 定期上报/刷新心跳，taos_list_instances 查阅当前实例，最终用 taos_free_instances 释放并注销。

### 4.2 外部实例信息表

1. 系统表名称：外部实例信息表 performance_schema.perf_instances
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
   - taosd 中的 mnode 对于超过过期时间间隔，且未再次更新跳跳的实例，自动从系统表中清除，检查的心跳间隔为`5s`
   - mnode 的 Leader 切换后，该列表自动清空，更待新的注册

## 5. 性能

### 5.1 系统表操作

1. 实例注册操作的平均响应时间应小于 20ms（不包含网络延时）
2. 实例删除操作的平均响应时间应小于 20ms（不包含网络延时）
3. 系统表的查询（存量 10 条记录）的平均响应时间小于 50ms（不包含网络延时）

### 5.2 读写性能

1. 注册操作（存量 10 条记录）不应影响正常数据写入性能，吞吐量下降控制在 5% 以内
2. 注册操作（存量 10 条记录）不应影响正常数据查询性能，吞吐量下降控制在 5% 以内

## 6. 安全

参照 [访问控制](https://taosdata.feishu.cn/wiki/Y12Ywd797ieHBBkVZsqcpsRgnAg) 需求，将 ins_instances 表的查询权限控制纳入到该任务的权限控制范围中

| 操作 | 超级用户 | 普通用户 sysinfo=1 | 普通用户 sysinfo=0 |
| --- | --- | --- | --- |
| show instances | ✔️ | ✔️ |  |
| query perf_instances | ✔️ | ✔️ | ✔️ （和其他表行为相同） |

## 7. 兼容性

不涉及

## 8. 安装和卸载

无特殊要求

## 9. 文档

taosadapter内部使用，无需修改文档。

## 10. 参考文档

- [外部实例注册 RS](https://taosdata.feishu.cn/wiki/CGzFw7t8EiiMC0knynkcmyFOnkd)

## 11. 附录
