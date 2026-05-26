# taos CLI 数据订阅功能 TS

# 1 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-05-25 | 2026-5-26 | 1.0 | 裴亚明 | 覆盖 taos CLI subscribe 命令功能 |

# 2 测试目标

- 覆盖 taos CLI `subscribe` 命令的全部用户交互行为：参数解析、错误提示、帮助信息。
- 验证 TMQ 消费者生命周期正确性：创建消费者、订阅 topic、轮询数据、取消订阅、关闭消费者。
- 覆盖数据订阅核心功能：实时数据接收、历史数据消费、消费组 offset 持久化、行数限制。
- 覆盖可配置参数：group.id（-g）、client.id（-c）、auto.offset.reset（-o）、行数限制（-n）、poll 超时（-t）。
- 覆盖异常场景：缺少必选参数、不存在的 topic、未知选项、Ctrl+C 中断。
- 验证与 taos 交互式 shell 的集成：命令识别、帮助文本、信号处理。

# 3 参考文档

- [TDengine 数据订阅官方文档](https://docs.taosdata.com/develop/tmq/)

# 4 测试结论

- 功能测试用例：全部 Pass。
- 覆盖目标：
  - 新增功能：全部覆盖。
  - 异常场景覆盖：缺少参数、非法参数、不存在资源。
  - Ctrl+C 中断： 按预期退出。

# 5 测试环境

- OS：Linux x86_64（Ubuntu 22.04+）
- 编译器：GCC 7+（C11）
- 依赖服务：
  - TDengine Server（taosd，localhost:6030/6041）
- 测试方式：`taos -s "command;"` 非交互模式 + 多终端交互模式

# 6 功能测试

## 6.1 参数解析与错误处理

### 6.1.1 测试要点

验证 `subscribe` 命令的参数解析正确性和错误提示：
- 缺少 topic 名称时给出明确错误提示
- 缺少必选参数 `-g`（group.id）时给出明确错误提示
- 未知选项给出警告但继续执行
- `subscribe -h` 显示帮助信息
- 参数值正确传递到 TMQ 配置

### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| ARG-001 | 缺少 topic 名称 | 执行 `subscribe;`（无参数），输出包含 "Usage:" 帮助信息 | Pass |
| ARG-002 | 缺少 group.id | 执行 `subscribe my_topic;`（无 -g），输出包含 "Error: group_id is required. Use -g <group_id>" | Pass |
| ARG-003 | 未知选项 | 执行 `subscribe my_topic -g g1 -z foo;`，输出包含 "Warning: unknown option '-z'" 并继续执行 | Pass |
| ARG-004 | 帮助信息 | 执行 `subscribe -h;`，输出包含完整 Usage 说明，列出所有可选参数 | Pass |
| ARG-005 | -g 参数正确传递 | 执行 `subscribe topic1 -g testgroup;`，TMQ 消费者使用 group.id="testgroup" 创建成功 | Pass |

## 6.2 TMQ 消费者生命周期

### 6.2.1 测试要点

验证 TMQ 消费者从创建到关闭的完整生命周期：
- 消费者成功创建并订阅 topic
- 数据轮询循环正常运行
- 通过 `-n` 行数限制自动退出
- 退出时正确执行 unsubscribe 和 consumer_close
- 不存在的 topic 返回明确错误

### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| LIF-001 | 正常创建与订阅 | 预先创建 topic，执行 `subscribe topic1 -g g1 -n 1;`，消费者成功创建，订阅成功，无报错 | Pass |
| LIF-002 | 不存在的 topic | 执行 `subscribe nonexist_topic -g g1 -n 1;`，输出包含 "Topic not exist" 或类似错误信息 | Pass |
| LIF-003 | -n 行数限制退出 | 执行 `subscribe topic1 -g g1 -n 5;`，接收到 5 行数据后自动退出，不挂起 | Pass |
| LIF-004 | 消费者正常关闭 | subscribe 退出后，相同 group.id 可再次订阅，说明前一个消费者已正确关闭 | Pass |

## 6.3 实时数据订阅

### 6.3.1 测试要点

验证订阅模式下实时接收新写入数据的功能：
- 使用 `-o latest` 参数，只接收订阅后新写入的数据
- 数据写入后，订阅端立即（在 poll timeout 内）显示数据
- 输出格式包含列名表头和对齐的数据行
- 多列数据正确显示

### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| RT-001 | 实时接收单行 | 终端 A 执行 `subscribe topic1 -g g1 -o latest -n 1;`，终端 B 插入 1 行数据，终端 A 立即显示该行数据 | Pass |
| RT-002 | 实时接收多行 | 终端 A 执行 `subscribe topic1 -g g1 -o latest -n 10;`，终端 B 连续插入 10 行，终端 A 依次显示全部 10 行 | Pass |
| RT-003 | 多列数据显示 | topic 关联的表包含 ts、int、float、varchar 等多种列类型，订阅端正确显示各列值和列名表头 | Pass |

## 6.4 历史数据消费

### 6.4.1 测试要点

验证 `auto.offset.reset=earliest` 模式下消费历史数据的功能：
- 订阅前已存在的数据可以被消费
- 新消费组从头开始消费

### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| HIST-001 | earliest 消费历史数据 | 预写入 5 行数据，新消费组执行 `subscribe topic1 -g new_group -o earliest -n 5;`，成功接收全部 5 行历史数据 | Pass |
| HIST-002 | latest 不消费历史数据 | 预写入 5 行数据，执行 `subscribe topic1 -g new_group2 -o latest -n 1;` 后等待 poll timeout，不返回历史数据（超时退出） | Pass |

## 6.5 消费组 Offset 管理

### 6.5.1 测试要点

验证消费组 offset 持久化（auto.commit）功能：
- 同一消费组多次消费不会重复接收已确认数据
- 不同消费组独立维护 offset

### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| OFF-001 | offset 持久化 | 消费组 g1 消费 5 行后退出；再次以 g1 订阅并写入新数据，只收到新数据，不重复收到旧数据 | Pass |
| OFF-002 | 消费组独立 | 消费组 g1 已消费全部数据；新消费组 g2 以 earliest 模式订阅，仍能收到全部历史数据 | Pass |

## 6.6 可配置参数验证

### 6.6.1 测试要点

验证各可选参数的功能正确性：
- `-t` poll 超时时间影响轮询间隔
- `-c` client.id 正确设置
- `-o` 支持 earliest/latest 两种模式

### 6.6.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| OPT-001 | 自定义 poll timeout | 执行 `subscribe topic1 -g g1 -t 500 -n 1;`（500ms 超时），功能正常，无数据时在 500ms 内返回 NULL | Pass |
| OPT-002 | 自定义 client.id | 执行 `subscribe topic1 -g g1 -c myClient -n 1;`，消费者使用 client.id="myClient" 创建成功 | Pass |

## 6.7 Shell 集成

### 6.7.1 测试要点

验证 subscribe 命令与 taos 交互式 shell 的集成：
- 命令在 shell 中正确识别和分发
- help 命令列出 subscribe 用法
- 命令前后空格和大小写处理

### 6.7.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| SHL-001 | 命令识别 | 在 taos 交互界面输入 `subscribe topic1 -g g1 -n 1;`，正确进入订阅模式（不作为 SQL 发送到 server） | Pass |
| SHL-002 | help 显示 | 在 taos 中执行 `help;`，输出包含 subscribe 命令的用法说明 | Pass |

## 6.8 完整端到端流程

### 6.8.1 测试要点

验证从建库、建表、创建 topic、订阅、写入、接收的完整工作流：
- 端到端全流程功能正确
- 跨终端协作场景正常

### 6.8.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| E2E-001 | 完整工作流 | 步骤：1) 创建数据库和表；2) 创建 topic；3) 终端 A 执行 subscribe；4) 终端 B 插入数据；5) 终端 A 显示数据。全流程无错误 | Pass |
| E2E-002 | 多次订阅-退出循环 | 连续 3 次执行 subscribe → 接收数据 → 退出，每次都正常工作，无资源泄漏 | Pass |

## 6.9 长期稳定性测试

无。subscribe 命令设计为短期临时验证工具，不面向长时间运行场景。

## 6.10 性能测试

无独立性能测试。数据接收性能受限于 TMQ 轮询机制和 server 端推送速度，subscribe 命令仅作为展示层。

## 6.11 安全性测试

无独立安全性测试。subscribe 命令复用 taos 现有的连接认证机制（用户名/密码），无额外安全攻击面。

# 7 兼容性测试

| # | 测试场景 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 已有 SQL 命令不受影响 | 执行 SELECT、INSERT、CREATE 等常规 SQL，功能正常 | Pass |
| 2 | 已有 shell 命令不受影响 | 执行 `set`、`source`、`quit` 等内置命令，功能正常 | Pass |
| 3 | -s 模式兼容 | 使用 `taos -s "subscribe topic1 -g g1 -n 5;"` 非交互模式执行订阅，功能正常 | Pass |
| 4 | 无 topic 时 SQL 正常 | 数据库中无任何 topic 时，常规 SQL 操作不受 subscribe 功能影响 | Pass |

# 8 已知问题和限制

- **Ctrl+C 中断**：仅在交互模式（非 `-s` 模式）下生效。`-s` 模式不启动 `shellCancelHandler` 线程，Ctrl+C 直接终止进程。
- **单 topic 订阅**：当前实现每次仅支持订阅单个 topic。如需同时监听多个 topic，需分别在多个终端执行。
