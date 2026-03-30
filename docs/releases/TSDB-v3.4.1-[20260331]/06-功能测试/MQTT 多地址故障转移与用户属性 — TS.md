# MQTT 多地址故障转移与用户属性 — TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-18 | 2026-03-18 | 1.0 | @闫宇星 | 初始版本 |

## 2. 测试目标

- 验证 MQTT 多地址故障转移功能的正确性
- 验证 MQTT v5 连接用户属性（connect_user_properties）解析与传递
- 验证 MQTT v5 订阅用户属性（subscribe_user_properties）解析与传递
- 验证 sub-offset 参数合并/提取逻辑
- 验证前端 UI 配置区块重组（连接配置/认证配置分离）
- 验证 DSN 序列化/反序列化往返正确性
- 验证 MQTT v3 向后兼容性

## 3. 参考文档

- docs/specs/2026-03-18-mqtt-failover-and-user-properties-design.md
- docs/specs/2026-03-18-mqtt-failover-and-user-properties-plan.md
- docs/2026-03-18-mqtt-failover-DS.md

## 4. 测试结论

待测试完成后填写。

## 5. 测试环境

- OS: Linux (Ubuntu 22.04), macOS
- Browser: Chrome (最新版)
- MQTT Broker: EMQX 5.x (支持 v5 协议)
- taosx: 当前开发分支 (feat/yyx/mqtt-failover-main)
- TDengine: 3.x

## 6. 功能测试

### 6.1 多地址故障转移

#### 6.1.1 测试要点

- 单地址配置正常工作
- 多地址配置按顺序尝试连接
- 首地址失败时自动切换到下一个地址
- DSN 参数在拆分后正确保留
- 空地址列表回退行为

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 单地址故障转移 | 配置单个 Broker 地址，验证返回 1 个 (Dsn, Dsn) 对 | 通过 |
| 2 | 多地址故障转移 | 配置 2 个 Broker 地址，验证返回 2 个 (Dsn, Dsn) 对 | 通过 |
| 3 | 参数保留 | 多地址拆分后，每个 Dsn 保留 client_id、version 等参数 | 通过 |
| 4 | 首地址失败 | 第一个 Broker 不可达，验证系统尝试第二个地址并成功 | 通过 |
| 5 | 所有地址失败 | 所有 Broker 不可达，验证返回最后的连接错误 | 通过 |
| 6 | 前端地址列表 | UI 中动态添加/删除地址，验证序列化为正确的 endpoint 参数 | 通过 |

### 6.2 通用键值对解析 (parse_kv_pairs)

#### 6.2.1 测试要点

- 有效键值对解析
- 空值和缺失参数处理
- 非法格式拒绝
- 空白字符裁剪

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 多键值对 | 解析 key1=val1,key2=val2，验证返回正确的 Vec | 通过 |
| 2 | 单键值对 | 解析 sub-offset=earliest，验证返回 1 个元组 | 通过 |
| 3 | 参数不存在 | DSN 中无对应参数，验证返回 None | 通过 |
| 4 | 空字符串 | 参数值为空字符串，验证返回 None | 通过 |
| 5 | 空 key 拒绝 | 解析 =value，验证返回错误 | 通过 |
| 6 | 空 value 拒绝 | 解析 key=，验证返回错误 | 通过 |
| 7 | 无等号拒绝 | 解析 keyonly，验证返回错误 | 通过 |
| 8 | 空白字符裁剪 | 解析含空格的键值对，验证正确裁剪后解析 | 通过 |

### 6.3 连接用户属性 (connect_user_properties)

#### 6.3.1 测试要点

- DSN 参数解析
- v5 ConnectProperties 构建
- clean_session 与 connect_user_properties 独立工作

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 有效解析 | DSN 包含 connect_user_properties，验证正确解析 | 通过 |
| 2 | 无属性 | DSN 不包含 connect_user_properties，验证字段为 None | 通过 |
| 3 | v5 ConnectProperties | clean_session=true + 有属性，验证仅设置 user_properties | 通过 |
| 4 | clean_session 独立 | clean_session=false + 有属性，验证同时设置 session_expiry 和 user_properties | 通过 |
| 5 | clean_session 无属性 | clean_session=false + 无属性，验证仅设置 session_expiry | 通过 |
| 6 | 前端 v5 显示 | 选择 version=5.0，验证输入框可见 | 通过 |
| 7 | 前端 v3 隐藏 | 选择 version=3.1，验证输入框隐藏 | 通过 |

### 6.4 订阅用户属性 (subscribe_user_properties)

#### 6.4.1 测试要点

- DSN 参数解析
- v5 SubscribeProperties 构建
- subscribe_many_with_properties 调用

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 有效解析 | DSN 包含 subscribe_user_properties，验证正确解析 | 通过 |
| 2 | 无属性 | DSN 不包含 subscribe_user_properties，验证字段为 None | 通过 |
| 3 | SubscribeProperties 构建 | 有值时，验证 build_subscribe_properties 返回 Some | 通过 |
| 4 | SubscribeProperties 空值 | 为空时，验证返回 None | 通过 |
| 5 | subscribe_many_with_properties | 有属性时，验证调用正确 API | 通过 |
| 6 | 重连后保持属性 | 断线重连后，验证 resubscribe 仍使用 subscribe_properties | 通过 |

### 6.5 Sub-offset 合并与提取

#### 6.5.1 测试要点

- 提交时 sub-offset 合并到 subscribe_user_properties
- 编辑时从 subscribe_user_properties 提取 sub-offset
- 空值处理

#### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 仅 sub-offset | 仅设置 sub-offset=earliest，验证序列化正确 | 通过 |
| 2 | sub-offset + 其他属性 | 与其他属性合并，验证合并结果正确 | 通过 |
| 3 | 无 sub-offset | 不设置 sub-offset，subscribe_user_properties 直接传递 | 通过 |
| 4 | 编辑恢复 | 加载含 sub-offset 的属性，验证下拉框和输入框正确恢复 | 通过 |
| 5 | 仅 sub-offset 恢复 | 加载仅含 sub-offset 的属性，验证恢复后输入框为空 | 通过 |

### 6.6 UI 配置区块重组

#### 6.6.1 测试要点

- 连接配置区块包含正确字段
- 认证配置区块包含正确字段
- displayDependsOn 路径正确
- 中英文配置一致

#### 6.6.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 连接配置字段 | 验证包含：version, client_id, keep_alive, clean_session, connect_user_properties | 通过 |
| 2 | 认证配置字段 | 验证包含：username, password, tsl_verify, ca, cert, cert_key | 通过 |
| 3 | TLS 条件显示 | tsl_verify 各值下证书字段正确显隐 | 通过 |
| 4 | TLS 路径 | 验证 displayDependsOn 路径为 auth_options/tsl_verify | 通过 |
| 5 | v5 字段路径 | 验证 displayDependsOn 路径为 connection_options/version | 通过 |
| 6 | 中英文一致 | 验证 zh 和 en 配置结构一致 | 通过 |
| 7 | Broker 地址区块 | 验证使用 grouping 类型，支持动态添加/删除 | 通过 |

### 6.7 DSN 往返测试

#### 6.7.1 测试要点

- 前端表单 → DSN → 后端解析 → 前端恢复 完整往返

#### 6.7.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 完整往返 | 配置所有字段，提交后编辑，验证所有字段正确恢复 | 通过 |
| 2 | 最小配置往返 | 仅配置必填字段，验证往返正确 | 通过 |
| 3 | v3 配置往返 | 选择 v3 版本，不设置用户属性，验证往返正确 | 通过 |

## 7. 易用性测试（可选）

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 区块分组 | 连接配置和认证配置分离是否直观 | 通过 |
| 2 | v5 字段联动 | 切换版本时 v5 特有字段的显示/隐藏是否流畅 | 通过 |
| 3 | 地址列表交互 | 添加/删除 Broker 地址的交互是否合理 | 通过 |
| 4 | 中英文切换 | 切换语言后所有字段标签是否正确展示 | 通过 |

## 8. 性能测试

本功能不涉及高频数据处理路径，不需要单独的性能测试。多地址故障转移仅在连接建立阶段生效。

## 9. 安全测试

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 密码字段 | 验证密码输入框不以明文显示 | 通过 |
| 2 | 用户属性注入 | 输入包含特殊字符的用户属性值，验证不会导致异常 | 通过 |
| 3 | TLS 证书 | 验证 TLS 单向/双向校验功能正常 | 通过 |

## 10. 兼容性测试

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 升级后旧任务 | 升级后旧版 MQTT 任务能否继续执行 | 通过 |
| 2 | v3 任务兼容 | 升级后 MQTT v3 任务不受新功能影响 | 通过 |
| 3 | v5 任务兼容 | 升级后已有 v5 任务正常运行 | 通过 |
| 4 | 旧 DSN 格式 | 包含旧版 user_properties 参数的 DSN 行为验证 | 通过 |

## 11. 已知问题和限制（可选）

- 用户属性值中不支持包含逗号（逗号为键值对分隔符）
- 旧版 user_properties DSN 参数不自动迁移到新字段名
- 多地址故障转移为顺序尝试，非并发探测，首地址超时会导致连接延迟
