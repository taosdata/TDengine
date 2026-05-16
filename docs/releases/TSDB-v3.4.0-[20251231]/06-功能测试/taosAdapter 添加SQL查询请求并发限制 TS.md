# taosAdapter 添加SQL查询请求并发限制 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-20 | 2025-10-20 | 1.0 | 谭雪峰 | 编写文档 |

## 2. 测试目标

<quote-container>
- 限制查询 SQL 并发请求功能正常
- 非限制 SQL 正常请求不限制
</quote-container>

## 3. 参考文档

<quote-container>
https://jira.taosdata.com:18080/browse/TS-6856
https://jira.taosdata.com:18080/browse/TD-36925
[taosAdapter 添加SQL查询请求并发限制 FS](https://taosdata.feishu.cn/wiki/EgdPwgxfUiMMJVk8GRLc838Qnce)
</quote-container>

## 4. 测试结论

<quote-container>
限制查询 SQL 并发请求功能正常，非限制 SQL 正常请求不限制
</quote-container>

## 5. 测试环境

- OS: Linux AMD64 & ARM64
- CI：GitHub Actions

## 6. 功能测试

### 6.1 限流器功能测试

#### 6.1.1 测试要点

测试要点包括验证限流器的基本功能（创建、获取、释放）和边界条件（超时、最大等待数），以及检查SQL过滤规则（类型判断、排除列表匹配）和指标收集机制的正确性。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | TestNewLimiter/limit_0 | 测试创建limit为0的限流器 | 通过 |
| 2 | TestNewLimiter/limit_5 | 测试创建limit为5的限流器 | 通过 |
| 3 | TestNewLimiter/limit_-1 | 测试创建limit为-1的限流器 | 通过 |
| 4 | TestLimiter_Acquire/limit_0 | 测试无限制情况下可正常获取100次 | 通过 |
| 5 | TestLimiter_Acquire/limit_2,_acquire_2 | 测试limit为2时正常获取2次 | 通过 |
| 6 | TestLimiter_Acquire/limit_2,_maxWait_1,_acquire_4,_should_fail | 测试超出最大等待数时返回队列已满错误 | 通过 |
| 7 | TestLimiter_Acquire/limit_2,_maxWait_2,_acquire_4,_should_fail | 测试等待超时情况下返回超时错误 | 通过 |
| 8 | TestGetLimiter/user_exists | 测试获取已存在用户的限流器配置 | 通过 |
| 9 | TestGetLimiter/user_not_exists | 测试获取不存在用户时返回默认限流器配置 | 通过 |
| 10 | TestCheckShouldLimit/not_select_type | 测试非SELECT类型SQL不限流 | 通过 |
| 11 | TestCheckShouldLimit/exclude_by_exact_match | 测试精确匹配排除的SQL不限流 | 通过 |
| 12 | TestCheckShouldLimit/exclude_by_exact_match_with_different_case | 测试大小写不敏感的精确匹配排除 | 通过 |
| 13 | TestCheckShouldLimit/exclude_by_length_too_short | 测试SQL长度过短时排除限流 | 通过 |
| 14 | TestCheckShouldLimit/exclude_by_regex_match | 测试正则匹配排除的SQL不限流 | 通过 |
| 15 | TestCheckShouldLimit/not_exclude,_should_limit | 测试未排除的SQL应该限流 | 通过 |
| 16 | TestCheckShouldLimit/not_exclude,_long_sql,_should_limit | 测试长SQL且未排除时应限流 | 通过 |
| 17 | TestCheckShouldLimit/disable_limit | 测试禁用限流功能时所有SQL不限流 | 通过 |
| 18 | TestGetLimiterMetrics | 测试获取限流器指标数据功能 | 通过 |

### 6.2 配置模块测试

#### 6.2.1 测试要点

验证配置解析的正确性（正常配置、边界条件、错误处理）和用户配置获取逻辑（存在用户、不存在用户、无用户配置情况）

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | TestRequest_setValue/normal_case | 测试正常配置解析，包括排除SQL列表、正则表达式、用户配置 | 通过 |
| 2 | TestRequest_setValue/error_regex | 测试错误的正则表达式配置应该返回错误 | 通过 |
| 3 | TestRequest_setValue/empty_exclude_sql | 测试空排除SQL配置应该返回错误 | 通过 |
| 4 | TestRequest_setValue/non-select_exclude_sql | 测试非SELECT类型排除SQL配置应该返回错误 | 通过 |
| 5 | TestRequest_setValue/only_select_exclude_sql | 测试仅包含"select"的排除SQL配置应该返回错误 | 通过 |
| 6 | TestRequest_setValue/default | 测试默认配置解析 | 通过 |
| 7 | TestRequest_GetUserLimitConfig/user_exists | 测试获取已存在用户的限流配置 | 通过 |
| 8 | TestRequest_GetUserLimitConfig/user_not_exists | 测试获取不存在用户时返回默认配置 | 通过 |
| 9 | TestRequest_GetUserLimitConfig/no_users_configured | 测试无用户配置时返回默认配置 | 通过 |

### 6.3 Rest 查询限流集成测试

#### 6.3.1 测试要点

验证限流器在HTTP接口层面的集成功能，包括正常限流、排除列表生效和正则排除规则工作

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 无限制并发查询测试 | 10个并发查询，QueryMaxWait=0，验证所有请求成功(200) | 通过 |
| 2 | 有限制并发查询测试 | 10个并发查询，QueryMaxWait=1，验证部分请求被限流(503) | 通过 |
| 3 | 排除SQL限流测试 | 10个并发"select 1"查询，验证排除列表SQL不限流 | 通过 |
| 4 | 正则排除限流测试 | 10个并发information_schema查询，验证正则排除SQL不限流 | 通过 |

### 6.4 WS 查询限流集成测试

#### 6.4.1 测试要点

验证WebSocket协议下不同查询格式（JSON和二进制）与不同结果处理方式（释放结果、获取所有结果、二进制获取所有结果）组合时的完整限流流程，涵盖查询执行、结果获取、资源释放和限流恢复的全生命周期

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | JSON查询与释放结果方式限流测试 | 测试JSON查询格式下使用释放结果方式的完整限流流程，包括首次查询成功、二次查询限流、排除SQL查询、正则排除查询和资源释放后恢复 | 通过 |
| 2 | JSON查询与获取所有结果方式限流测试 | 测试JSON查询格式下使用获取所有结果方式的完整限流流程，验证查询执行、结果获取和限流恢复的全过程 | 通过 |
| 3 | JSON查询与二进制获取所有结果方式限流测试 | 测试JSON查询格式下使用二进制获取所有结果方式的完整限流流程，验证二进制结果获取和限流机制 | 通过 |
| 4 | 二进制查询与释放结果方式限流测试 | 测试二进制查询格式下使用释放结果方式的完整限流流程，验证二进制查询的限流行为和资源释放 | 通过 |
| 5 | 二进制查询与获取所有结果方式限流测试 | 测试二进制查询格式下使用获取所有结果方式的完整限流流程，验证二进制查询的结果获取和限流恢复 | 通过 |
| 6 | 二进制查询与二进制获取所有结果方式限流测试 | 测试二进制查询格式下使用二进制获取所有结果方式的完整限流流程，验证端到端的二进制通信限流机制 | 通过 |

## 7. 易用性测试（可选）

无

## 8. 长期稳定性测试（可选）

无

## 9. 性能测试

无

## 10. 安全测试

无

## 11. 兼容性测试

无

## 12. 已知问题和限制（可选）

只限制 select 开头的查询语句
