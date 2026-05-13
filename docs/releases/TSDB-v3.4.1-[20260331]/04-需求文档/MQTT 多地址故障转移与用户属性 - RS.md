# MQTT 多地址故障转移与用户属性 - RS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-18 | 2026-03-18 | 1.0 | @闫宇星 | 初始版本 |

## 2. 引言

### 2.1 术语与缩写名词

| 术语 | 定义 |
| --- | --- |
| MQTT | Message Queuing Telemetry Transport，轻量级消息传输协议 |
| MQTT v5 | MQTT 5.0 版本，支持用户属性（User Properties）等高级特性 |
| DSN | Data Source Name，数据源连接字符串 |
| Failover | 故障转移，主连接失败时自动切换到备用连接 |
| User Properties | MQTT v5 支持的自定义键值对属性 |
| ConnectProperties | MQTT v5 CONNECT 报文中的属性 |
| SubscribeProperties | MQTT v5 SUBSCRIBE 报文中的属性 |
| Sub-offset | 订阅初始位置，控制从 earliest 或 latest 开始消费 |
| TLS | Transport Layer Security，传输层安全协议 |

### 2.2 相关文档资料

- MQTT 5.0 协议规范 (OASIS Standard)
- taosx MQTT 数据源现有实现文档

### 2.3 优先级要求

- 重要程度：高
- 期望交付时间：2026-03-18（当前迭代）
- 本功能为客户需求驱动，需要在当前版本中完成

### 2.4 版本要求

- 企业版支持
- 开源版同步支持
- 随 taosx 最新版本发布

## 3. 需求目标

MQTT 数据源当前仅支持单一 Broker 地址连接，缺乏高可用能力；同时 MQTT v5 的 User Properties 特性未被充分利用。本需求旨在：
1. **多地址故障转移**：支持配置多个 MQTT Broker 地址，系统按顺序尝试连接，实现高可用。
2. **连接用户属性**：支持在 MQTT v5 CONNECT 报文中发送自定义键值对属性，用于客户端元数据传递、认证令牌、路由提示等。
3. **订阅用户属性**：支持在 MQTT v5 SUBSCRIBE 报文中发送自定义键值对属性，用于订阅级别的参数控制（如 sub-offset）。
4. **UI 重组**：将 MQTT 配置界面重新组织为连接配置、认证配置两大独立区块，提升易用性。

## 4. 功能需求

| 序号 | 功能类别 | 功能名称 | 功能描述 |
| --- | --- | --- | --- |
| 1 | 连接管理 | 多地址配置 | 前端提供动态 Host+Port 列表，支持添加/删除多个 Broker 地址，最少保留 1 个地址，顺序决定故障转移优先级 |
| 2 | 连接管理 | 故障转移 | 后端将多地址 DSN 拆分为多个单地址任务对，按顺序尝试连接，第一个成功的地址被使用 |
| 3 | MQTT v5 | 连接用户属性 | 支持配置 MQTT v5 CONNECT 报文用户属性（key=value 格式），仅当协议版本为 5.0 时可见 |
| 4 | MQTT v5 | 订阅用户属性 | 支持配置 MQTT v5 SUBSCRIBE 报文用户属性（key=value 格式），仅当协议版本为 5.0 时可见 |
| 5 | MQTT v5 | 订阅初始位置 | 提供 earliest/latest 下拉选择，控制 v5 订阅起始偏移，提交时合并到 subscribe_user_properties |
| 6 | UI 重组 | 连接配置区块 | 将 version、client_id、keep_alive、clean_session、connect_user_properties 归入连接配置 |
| 7 | UI 重组 | 认证配置区块 | 新建 auth_options 区块，包含 username、password、TLS 校验及证书相关字段 |
| 8 | DSN 序列化 | sub-offset 合并 | 提交时将 sub-offset 合并到 subscribe_user_properties；编辑时反向提取 |
| 9 | 通用工具 | parse_kv_pairs | 后端提供通用的 DSN 键值对解析工具函数，支持跨 crate 复用 |
| 10 | 兼容性 | MQTT v3 兼容 | MQTT v3 任务不受影响，用户属性参数在 v3 下被忽略 |

## 5. 性能需求

- 多地址故障转移不应引入额外延迟：系统按顺序尝试地址，每个地址的连接超时由 MQTT 客户端配置决定。
- User Properties 解析为 O(n) 复杂度（n 为键值对数量），对整体性能无显著影响。
- 前端 UI 渲染增加的表单字段不应导致明显的界面卡顿。

## 6. 安全需求

- **凭据保护**：用户名和密码字段通过 password 类型输入框保护，不以明文展示。
- **TLS 支持**：保留单向/双向 TLS 校验能力，证书上传通过 file 类型字段处理。
- **输入校验**：User Properties 的 key 和 value 不允许为空，后端 parse_kv_pairs() 强制校验。
- **DSN 参数安全**：不对用户属性值进行任何特殊解释，避免注入风险。

## 7. 其他需求

### 1. 兼容性需求

- 旧版 DSN 中的 user_properties 参数不再被解析（字段名已变更）。如需向后兼容，可添加回退逻辑。
- MQTT v3 任务完全不受影响。
- 已有的不带用户属性的 v5 任务不受影响。

### 2. 接口需求

- DSN 格式扩展：支持 connect_user_properties 和 subscribe_user_properties 两个新参数。
- 多地址通过 endpoint 参数传递，以逗号分隔。

### 3. 运维需求

- 故障转移日志：记录每个地址的连接尝试、成功/失败状态。
- 无需额外部署步骤，功能随 taosx 版本升级自动启用。

### 4. 易用性需求

- UI 配置分组清晰：连接配置、认证配置分离，减少用户混淆。
- v5 特有字段（用户属性、sub-offset）仅在选择 v5 版本时显示。

### 5. 测试需求

- 单元测试覆盖：parse_kv_pairs、get_datasource_failover_config、build_subscribe_properties 等核心函数。
- 集成测试覆盖：故障转移场景、v5 用户属性传递、v3 兼容性。
- UI 测试覆盖：字段可见性联动、DSN 序列化/反序列化往返验证。
