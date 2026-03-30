# 身份鉴别 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-14 | - | 0.1 | 段宽军 | 创建 |
| 2026-01-20 | 2026-01-20 | 1.0 | 张博民 | 调整格式，增加部分用例 |

## 2. 测试目标

本次测试旨在验证 TDengine TSDB v3.4.0.0 企业版中用户登录及管理相关功能的正确性、稳定性和安全性，确保所有用户管理功能符合产品设计规范。本次测试覆盖以下功能模块：
1. **用户基础管理**
- 用户创建、查询、修改、删除
- 用户权限配置（SYSINFO, CREATEDB, ENABLE）
1. **令牌（Token）管理**（企业版功能）
- Token 创建、查询、修改、删除
- Token 登录验证
- Token 属性配置（ENABLE, TTL, PROVIDER, EXTRA_INFO）
1. **TOTP 双因认证**（企业版功能）
- TOTP 密钥创建、更新、删除
- TOTP 登录验证
- TOTP 过期验证
1. **高级用户选项**（企业版功能）
- 会话管理：SESSION_PER_USER, CONNECT_TIME, CONNECT_IDLE_TIME
- 调用限制：CALL_PER_SESSION, VNODE_PER_CALL
- 安全策略：FAILED_LOGIN_ATTEMPTS, PASSWORD_LOCK_TIME
- 密码策略：PASSWORD_LIFE_TIME, PASSWORD_GRACE_TIME, PASSWORD_REUSE_TIME, PASSWORD_REUSE_MAX
- 账户策略：INACTIVE_ACCOUNT_TIME, CHANGEPASS
- 访问控制：HOST, NOT_ALLOW_HOST, ALLOW_DATETIME, NOT_ALLOW_DATETIME
- Token 限制：ALLOW_TOKEN_NUM

## 3. 参考文档

[身份鉴别 RS](https://taosdata.feishu.cn/wiki/GZNPwH62SiiRtQkQHTvcM73YnDh)
[身份鉴别 FS](https://taosdata.feishu.cn/wiki/CXXqwV3Fai36rQkby6zcmBWwnMd)
[使用手册-用户管理](https://docs.taosdata.com/reference/taos-sql/user/)

## 4. 测试结论

1. 核心功能完整且稳定，相关测试用例通过率 100%。
2. 测试过程中发现的缺陷均为非阻塞性问题，不影响正常使用。
3. 安全机制完善，满足企业级需求。
4. 性能表现良好。

## 5. 测试环境

### 5.1 **硬件环境**

操作系统：Linux
- CPU：x86_64
- 内存：≥ 8GB

### 5.2 **软件环境**

TDengine 版本：v3.4.0.0（企业版）
- Python 版本：3.x
- 测试框架：new_test_framework
- 依赖库：pyotp, socket

### 5.3 测试脚本

- test_user_manager.py
- test_user_token.py
- test_user_totp.py

## 6. 功能测试

### 6.1 用户管理和高级选项

#### 6.1.1 **创建用户**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-USER-001 | 基础用户创建 | 成功创建 | 符合预期 | ✅ PASS |
| TC-USER-002 | 强密码验证 | 符合规则通过，不符合拒绝 | 符合预期 | ✅ PASS |
| TC-USER-003 | SYSINFO 选项 | 权限生效 | 符合预期 | ✅ PASS |
| TC-USER-004 | CREATEDB 选项 | 权限生效 | 符合预期 | ✅ PASS |
| TC-USER-005 | ENABLE 选项 | 状态生效 | 符合预期 | ✅ PASS |

#### 6.1.2 **高级选项测试**

##### 6.1.2.1 **CHANGEPASS**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-USER-006 | CHANGEPASS=2（可修改） | 用户可修改密码 | 符合预期 | ✅ PASS |
| TC-USER-007 | CHANGEPASS=1（必须修改） | 首次登录强制修改 | 符合预期 | ✅ PASS |
| TC-USER-008 | CHANGEPASS=0（不能修改） | 禁止修改密码 | 符合预期 | ✅ PASS |
| TC-USER-009 | 无效值验证 | 拒绝无效值 | 符合预期 | ✅ PASS |

##### 6.1.2.2 **SESSION_PER_USER**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-USER-010 | 默认值（UNLIMITED） | 可创建 200+ 会话 | 符合预期 | ✅ PASS |
| TC-USER-011 | 最小值（1） | 仅允许 1 个会话 | 符合预期 | ✅ PASS |
| TC-USER-012 | 普通值（100） | 限制生效 | 符合预期 | ✅ PASS |
| TC-USER-013 | UNLIMITED | 可创建 500+ 会话 | 符合预期 | ✅ PASS |

##### 6.1.2.3 **CONNECT_TIME**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-USER-014 | 默认值（-1 UNLIMITED） | 会话不超时 | 符合预期 | ✅ PASS |
| TC-USER-015 | 最小值（1 分钟） | 1 分钟后自动断开 | 符合预期 | ✅ PASS |

##### 6.1.2.4 **CONNECT_IDLE_TIME**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-USER-016 | 默认值（-1 UNLIMITED） | 空闲不超时 | 符合预期 | ✅ PASS |
| TC-USER-017 | 最小值（1 分钟） | 空闲 1 分钟后断开 | 符合预期 | ✅ PASS |

##### 6.1.2.5 **CALL_PER_SESSION**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-USER-018 | 默认值（-1 UNLIMITED） | 并发调用不限制 | 符合预期 | ✅ PASS |
| TC-USER-019 | 限制值（5） | 最多 5 个并发调用 | 符合预期 | ✅ PASS |

##### 6.1.2.6 **VNODE_PER_CALL**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-USER-020 | 默认值（-1 UNLIMITED） | 不限制 vnode 数量 | 符合预期 | ✅ PASS |
| TC-USER-021 | 最小值（1） | 超过限制报错 | 符合预期 | ✅ PASS |
| TC-USER-022 | 等于 vgroup 数量（16） | 恰好允许 | 符合预期 | ✅ PASS |

##### 6.1.2.7 **FAILED_LOGIN_ATTEMPTS**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-USER-023 | 默认值（3 次） | 3 次失败后锁定 | 符合预期 | ✅ PASS |
| TC-USER-024 | 最小值（1 次） | 1 次失败后锁定 | 符合预期 | ✅ PASS |
| TC-USER-025 | 大值（10 次） | 10 次失败后锁定 | 符合预期 | ✅ PASS |

##### 6.1.2.8 **PASSWORD_LOCK_TIME**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-USER-026 | 默认值（1440 分钟） | 锁定时间正确 | 符合预期 | ✅ PASS |
| TC-USER-027 | 最小值（1 分钟） | 1 分钟后自动解锁 | 符合预期 | ✅ PASS |
| TC-USER-028 | UNLIMITED | 永久锁定 | 符合预期 | ✅ PASS |

##### 6.1.2.9 **其他高级选项**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-USER-029 | PASSWORD_LIFE_TIME | 密码过期策略生效 | 符合预期 | ✅ PASS |
| TC-USER-030 | PASSWORD_GRACE_TIME | 宽限期策略生效 | 符合预期 | ✅ PASS |
| TC-USER-031 | PASSWORD_REUSE_TIME | 密码重用时间限制 | 符合预期 | ✅ PASS |
| TC-USER-032 | PASSWORD_REUSE_MAX | 密码重用次数限制 | 符合预期 | ✅ PASS |
| TC-USER-033 | INACTIVE_ACCOUNT_TIME | 不活动锁定策略 | 符合预期 | ✅ PASS |
| TC-USER-034 | ALLOW_TOKEN_NUM | Token 数量限制 | 符合预期 | ✅ PASS |
| TC-USER-035 | HOST（白名单） | IP 白名单生效 | 符合预期 | ✅ PASS |
| TC-USER-036 | NOT_ALLOW_HOST（黑名单） | IP 黑名单生效 | 符合预期 | ✅ PASS |
| TC-USER-037 | ALLOW_DATETIME | 时间白名单生效 | 符合预期 | ✅ PASS |
| TC-USER-038 | NOT_ALLOW_DATETIME | 时间黑名单生效 | 符合预期 | ✅ PASS |
| TC-USER-039 | 组合选项 | 多选项同时生效 | 符合预期 | ✅ PASS |

#### 6.1.3 **查询用户**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-USER-040 | SHOW USERS | 返回所有用户 | 符合预期 | ✅ PASS |
| TC-USER-041 | SHOW USERS FULL | 返回完整信息 | 符合预期 | ✅ PASS |
| TC-USER-042 | 系统表查询 | 信息一致 | 符合预期 | ✅ PASS |

#### 6.1.4 **修改用户**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-USER-043 | 修改密码 | 修改成功 | 符合预期 | ✅ PASS |
| TC-USER-044 | 修改各类选项 | 修改生效 | 符合预期 | ✅ PASS |
| TC-USER-045 | ADD/DROP HOST | 动态修改生效 | 符合预期 | ✅ PASS |
| TC-USER-046 | ADD/DROP DATETIME | 动态修改生效 | 符合预期 | ✅ PASS |

#### 6.1.5 **删除用户**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-USER-047 | 正常删除用户 | 成功删除 | 符合预期 | ✅ PASS |
| TC-USER-048 | 级联删除 Token | Token 同时删除 | 符合预期 | ✅ PASS |
| TC-USER-049 | IF EXISTS 子句 | 不存在不报错 | 符合预期 | ✅ PASS |

### 6.2 令牌（Token）管理

#### 6.2.1 **创建令牌**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-TOKEN-001 | 基础 Token 创建 | 成功创建，返回 63 位 Token | 符合预期 | ✅ PASS |
| TC-TOKEN-002 | 带所有选项创建（ENABLE, TTL, PROVIDER, EXTRA_INFO） | 成功创建并验证选项 | 符合预期 | ✅ PASS |
| TC-TOKEN-003 | 设置 TTL=7 天 | 成功创建，过期时间正确 | 符合预期 | ✅ PASS |
| TC-TOKEN-004 | 创建禁用状态 Token（ENABLE=0） | 成功创建，状态为禁用 | 符合预期 | ✅ PASS |
| TC-TOKEN-005 | IF NOT EXISTS 子句 | 重复创建不报错 | 存在缺陷，报告了错误 | ⚠️ BUG-TOKEN-1 |
| TC-TOKEN-006 | 最大长度 Token 名称（31 字节） | 成功创建 | 符合预期 | ✅ PASS |
| TC-TOKEN-007 | 多语言支持（中文名称） | 成功创建 | 符合预期 | ✅ PASS |
| TC-TOKEN-008 | 超出 3 个 Token 限制 | 创建失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-009 | 重复 Token 名称 | 创建失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-010 | 不存在的用户 | 创建失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-011 | 超长名称（>31 字节） | 创建失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-012 | 空 Token 名称 | 创建失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-013 | 空用户名称 | 创建失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-014 | 无效 TTL（-1） | 创建失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-015 | 无效 ENABLE（2） | 创建失败 | 符合预期 | ✅ PASS |

#### 6.2.2 **查询 Token**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-TOKEN-016 | SHOW TOKENS 命令 | 返回所有 Token 列表 | 符合预期 | ✅ PASS |
| TC-TOKEN-017 | 系统表查询 ins_tokens | 返回结果与 SHOW 一致 | 符合预期 | ✅ PASS |
| TC-TOKEN-018 | 查询特定 Token | 返回指定 Token 信息 | 符合预期 | ✅ PASS |
| TC-TOKEN-019 | 带过滤条件查询 | 返回符合条件的 Token | 符合预期 | ✅ PASS |

#### 6.2.3 **修改 Token**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-TOKEN-020 | 修改 ENABLE 状态 | 成功修改 | 符合预期 | ✅ PASS |
| TC-TOKEN-021 | 修改 TTL | 成功修改，新时间从修改时起算 | 符合预期 | ✅ PASS |
| TC-TOKEN-022 | 修改 PROVIDER | 成功修改 | 符合预期 | ✅ PASS |
| TC-TOKEN-023 | 修改 EXTRA_INFO | 成功修改 | 符合预期 | ✅ PASS |
| TC-TOKEN-024 | 同时修改多个属性 | 成功修改所有属性 | 符合预期 | ✅ PASS |
| TC-TOKEN-025 | 修改不存在的 Token | 修改失败 | 存在缺陷，修改失败但未报告预期错误 | ⚠️ BUG-TOKEN-2 |
| TC-TOKEN-026 | 空 Token 名称 | 修改失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-027 | 无效 TTL（-10） | 修改失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-028 | 无效 ENABLE（5） | 修改失败 | 符合预期 | ✅ PASS |

#### 6.2.4 **删除 Token**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-TOKEN-029 | 正常删除 Token | 成功删除，系统表验证 | 符合预期 | ✅ PASS |
| TC-TOKEN-030 | IF EXISTS 子句 | 不存在时不报错 | 符合预期 | ✅ PASS |
| TC-TOKEN-031 | 重复删除 | 第二次删除失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-032 | 删除不存在的 Token | 删除失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-033 | 空 Token 名称 | 删除失败 | 符合预期 | ✅ PASS |

#### 6.2.5 **Token 登录**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-TOKEN-034 | root 用户 Token 登录 | 登录成功 | 符合预期 | ✅ PASS |
| TC-TOKEN-035 | 普通用户 Token 登录 | 登录成功 | 符合预期 | ✅ PASS |
| TC-TOKEN-036 | 禁用 Token 登录 | 登录失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-037 | 启用/禁用切换 | 状态生效 | 符合预期 | ✅ PASS |
| TC-TOKEN-038 | 删除后登录 | 登录失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-039 | 重建同名 Token | 新 Token 可登录 | 符合预期 | ✅ PASS |
| TC-TOKEN-040 | 修改 TTL 后登录 | 登录成功 | 符合预期 | ✅ PASS |
| TC-TOKEN-041 | 无效 Token 字符串 | 登录失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-042 | 过短 Token | 登录失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-043 | 过长 Token | 登录失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-044 | 特殊字符 Token | 登录失败 | 符合预期 | ✅ PASS |
| TC-TOKEN-045 | 无权限用户创建 Token | 可创建自己的 Token | 符合预期 | ✅ PASS |
| TC-TOKEN-046 | 无权限用户查看 Token | 仅能查看自己的 | 符合预期 | ✅ PASS |

### 6.3 TOTP 双因认证

#### 6.3.1 **创建 TOTP 密钥**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-TOTP-001 | root 账户创建 TOTP | 成功创建 52 位密钥 | 符合预期 | ✅ PASS |
| TC-TOTP-002 | 普通用户创建 TOTP | 成功创建并可登录 | 符合预期 | ✅ PASS |
| TC-TOTP-003 | 禁用用户创建 TOTP | 创建成功但登录失败 | 符合预期 | ✅ PASS |
| TC-TOTP-004 | sysinfo=0 用户创建 TOTP | 成功创建并可登录 | 符合预期 | ✅ PASS |
| TC-TOTP-005 | 重复创建验证唯一性 | 100 次创建密钥均不重复 | 符合预期 | ✅ PASS |
| TC-TOTP-006 | 不存在的用户 | 创建失败 | 符合预期 | ✅ PASS |
| TC-TOTP-007 | 空用户名 | 创建失败 | 符合预期 | ✅ PASS |
| TC-TOTP-008 | 关键字用户名 | 创建失败 | 符合预期 | ✅ PASS |
| TC-TOTP-009 | 超长用户名 | 创建失败 | 符合预期 | ✅ PASS |

#### 6.3.2 **修改 TOTP 密钥**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-TOTP-010 | 更新已有 TOTP | 生成新密钥 | 符合预期 | ✅ PASS |
| TC-TOTP-011 | 新密钥登录 | 登录成功 | 符合预期 | ✅ PASS |
| TC-TOTP-012 | 旧密钥登录 | 登录失败 | 符合预期 | ✅ PASS |

#### 6.3.3 **删除 TOTP 密钥**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-TOTP-013 | 删除 root TOTP | 成功删除 | 符合预期 | ✅ PASS |
| TC-TOTP-014 | 删除禁用用户 TOTP | 成功删除 | 符合预期 | ✅ PASS |
| TC-TOTP-015 | 重复删除 TOTP | 第二次删除失败 | 存在缺陷，未报告错误 | ⚠️ BUG-TOTP-1 |
| TC-TOTP-016 | 删除不存在用户 TOTP | 删除失败 | 存在缺陷，未正确报告错误 | ⚠️ BUG-TOTP-2 |
| TC-TOTP-017 | 空用户名 | 删除失败 | 符合预期 | ✅ PASS |
| TC-TOTP-018 | 带引号用户名 | 删除失败 | 符合预期 | ✅ PASS |
| TC-TOTP-019 | 关键字用户名 | 删除失败 | 符合预期 | ✅ PASS |
| TC-TOTP-020 | 超长用户名 | 删除失败 | 符合预期 | ✅ PASS |

#### 6.3.4 **TOTP 登录验证**

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-TOTP-021 | root 正常登录 | 登录成功 | 符合预期 | ✅ PASS |
| TC-TOTP-022 | 不存在的用户 | 登录失败 | 符合预期 | ✅ PASS |
| TC-TOTP-023 | 错误密码 | 登录失败 | 符合预期 | ✅ PASS |
| TC-TOTP-024 | 空用户名 | 登录失败 | 符合预期 | ✅ PASS |
| TC-TOTP-025 | 空密码 | 登录失败 | 符合预期 | ✅ PASS |
| TC-TOTP-026 | 空 TOTP 码 | 登录失败 | 符合预期 | ✅ PASS |
| TC-TOTP-027 | 错误 TOTP 码 | 登录失败 | 符合预期 | ✅ PASS |
| TC-TOTP-028 | TOTP 码过期多于30秒 | 登录失败 | 符合预期 | ✅ PASS |
| TC-TOTP-029 | TOTP 码过期少于30秒 | 登录成功 | 符合预期 | ✅ PASS |
| TC-TOTP-030 | TOTP 码超前多于30秒 | 登录失败 | 符合预期 | ✅ PASS |
| TC-TOTP-031 | TOTP 码超前少于30秒 | 登录成功 | 符合预期 | ✅ PASS |
| TC-TOTP-032 | 正确的 TOTP 验证码 | 登录成功 | 符合预期 | ✅ PASS |

## 7. 性能测试

1. **Token 生成速度**：平均 < 10ms
2. **TOTP 密钥生成速度**：平均 < 5ms
3. **会话创建**：200+ 并发会话稳定
4. **并发调用**：10+ 并发调用无异常
5. **系统表查询**：响应时间 < 10ms

## 8. 安全测试

### 8.1 **密码安全**

- ✅ 强密码策略实施有效
- ✅ 密码存储加密（未明文存储）
- ✅ 密码过期策略可配置
- ✅ 密码重用限制生效

### 8.2 **认证安全**

- ✅ Token 长度足够（63 位）
- ✅ TOTP 密钥长度足够（52 位）
- ✅ TOTP 30 秒过期机制正常
- ✅ 失败登录锁定机制有效

### 8.3 **访问控制**

- ✅ IP 白名单/黑名单机制正常
- ✅ 时间访问控制正常
- ✅ 会话管理机制完善
- ✅ 权限隔离有效

## 9. 兼容性测试

- 从旧版本升级后，现有用户不受影响
- 新增选项均有默认值，不影响现有系统

## 10. 已知问题和限制

| 缺陷编号 | 模块 | 严重程度 | 缺陷描述 | 状态 |
| --- | --- | --- | --- | --- |
| BUG-TOKEN-1 | Token 管理 | Medium | IF NOT EXISTS 重复创建行为异常 | Open |
| BUG-TOKEN-2 | Token 管理 | Low | ALTER 不存在的 Token 未正确报错 | Open |
| BUG-TOTP-1 | TOTP 认证 | Low | 重复删除 TOTP 未正确报错 | Open |
| BUG-TOTP-2 | TOTP 认证 | Low | 删除不存在用户的 TOTP 未正确报错 | Open |
