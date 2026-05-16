# Go 连接器 WebSocket 统一连接管理重构 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-23 | - | 1.0 | 谭雪峰 | 编写文档 |

## 2. 测试目标

本报告记录对 WebSocket Unified 重构的测试设计与结果，目标如下：

- 验证 `ws/unified` 对 Query / Stmt / Stmt2 / Schemaless / TMQ 的功能覆盖是否完整。
- 验证多节点 failover、断连重连、并发场景下的行为正确性与稳定性。
- 验证断连重连时优先尝试当前活跃节点，避免网络闪断造成无必要切换。
- 验证兼容入口（`taosWS`、`ws/stmt`、`ws/schemaless`、`ws/tmq`）在 WebSocket 路径上的兼容行为。
- 性能测试对比不下降。

## 3. 参考文档

- [Go 连接器 WebSocket 统一连接管理重构 FS.md](../05-设计文档/Go%20连接器%20WebSocket%20统一连接管理重构%20FS.md)

## 4. 测试结论

- 功能覆盖结论：`ws/unified` 当前共有 64 个测试文件（根目录 58 + `tests/` 5 + `proto/` 1），核心链路和高风险异常路径均有测试覆盖。
- 重连策略结论：failover 候选顺序已调整为“先试当前活跃节点，再按最少连接数尝试其余节点”，可降低网络闪断导致的误切换。
- 门禁结论：脚本门禁（`quick/loop/full/full-integration/cross-*`）与 CI 定时门禁（`cross-full-loop`）已形成体系化回归路径。
- 性能结论：
  - 3.8.0 WebSocket 新增 `stmt2_insert` 后，QPS 区间达到 `609,659 ~ 6,900,309`。
  - 在 3.8.0 WebSocket 同场景下，`stmt2_insert` 相对 `sql_insert` 全部领先，倍率区间 `1.04x ~ 22.11x`，中位数 `1.81x`。
  - Query 在 3.8.0 相对 3.7.1 平均提升 `+23.54%`，Subscription 与 SQL Insert 整体为小幅波动。

## 5. 测试环境

- Branch: `feat/xftan/ws-stmt2`
- Compare: `main`
- Driver Version: `3.8.0` / `3.7.1`
- Protocol: `WebSocket`
- 性能测试执行日期: `2026-03-20`
- 典型并发: `1 / 4 / 16 / 120`
- 典型参数: `auto_create_sub_table=true/false`、`interlace_rows=0/1`、`tables/records` 组合
- OS: Linux

## 6. 功能测试

### 6.1 `ws/unified` 覆盖范围

#### 6.1.1 测试要点

- 覆盖配置、连接、DSN、消息路由、重连、协议编解码、边界错误、集成路径。
- 覆盖 Query / Stmt / Stmt2 / Schemaless / TMQ 全链路。
- 覆盖 failover、并发、IPv6、回归场景。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | Config/Builder | `config_test.go`、`config_builder_test.go` 验证默认值、边界值、参数拼装 | 已覆盖 |
| 2 | DSN/Driver | `dsn_test.go`、`dsn_boundary_test.go`、`dsn_feature_test.go`、`dsn_driver_test.go` 验证解析与边界 | 已覆盖 |
| 3 | Connector/Open/Bootstrap | `connector_test.go`、`open_integration_test.go`、`conn_bootstrap_test.go`、`conn_feature_test.go` | 已覆盖 |
| 4 | Client 生命周期 | `client_test.go`、`client_generation_test.go`、`client_swapruntime_order_test.go`、`client_lockorder_test.go` | 已覆盖 |
| 5 | Pending/Request/Response | `client_pending_notify_test.go`、`request_send_paths_test.go`、`request_snapshot_paths_test.go`、`request_swap_test.go`、`response_test.go` | 已覆盖 |
| 6 | Reconnect/Failover 回归 | `client_reconnect_test.go`、`failover_test.go`、`reconnect_lifecycle_regression_test.go`、`reconnect_failover_regression_test.go`；新增当前节点优先测试（`TestClientReconnectTriesActiveEndpointFirstThenFallsBack`、`TestFailoverStateReconnectCandidatesActiveFirstRegardlessOfConnectionCount`） | 已覆盖 |
| 7 | Query/ResultSet | `query_test.go`、`query_lifecycle_regression_test.go`、`rows_methods_test.go`、`rows_prefetch_paths_test.go` | 已覆盖 |
| 8 | Query 多节点集成 | `query_failover_multiadapter_integration_test.go` | 已覆盖 |
| 9 | Stmt/Stmt2 | `stmt_feature_test.go`、`stmt_close_test.go`、`stmt_replay_regression_test.go`、`stmt2_*_test.go` | 已覆盖 |
| 10 | Schemaless/TMQ | `schemaless_*_test.go`、`tmq_*_test.go`（含负向、边界、集成） | 已覆盖 |
| 11 | 跨节点与 IPv6 | `reconnect_failover_multiadapter_integration_test.go`、`ipv6_end_to_end_integration_test.go` | 已覆盖 |
| 12 | 协议与安全边界 | `action_encode_test.go`、`proto/proto_test.go`、`internal_boundaries_test.go`、`request_log_context_test.go` | 已覆盖 |

### 6.2 WebSocket stmt2 新增能力验证

#### 6.2.1 测试要点

- 验证 3.8.0 WebSocket 新增 `stmt2_insert` 的可用性与吞吐区间。
- 验证同版本同场景下 `stmt2_insert` 对 `sql_insert` 的性能优势。

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | stmt2 新增能力可用性 | 3.8.0 在 8 组典型场景执行 `stmt2_insert` | 全部成功，QPS `609,659 ~ 6,900,309` |
| 2 | stmt2 vs sql 同场景对比 | 3.8.0 同配置对比 `stmt2_insert` 与 `sql_insert` | 全部领先，`1.04x ~ 22.11x` |
| 3 | 全类型随机写入 | `tests/stmt2_all_types_random_test.go` 验证类型覆盖与稳定性 | 已覆盖 |
| 4 | 错误类型路径 | `tests/stmt_wrong_type_test.go` 验证异常输入与错误语义 | 已覆盖 |

### 6.3 Cross-Failover 功能验证

#### 6.3.1 测试要点

- 验证 Query / Stmt / Schemaless / TMQ 在多节点故障下的连续性。
- 验证并发切换、链式故障、抖动循环、IPv6 场景。

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 即时重连 | 断连后快速切换并恢复请求 | 已覆盖（cross failover 系列） |
| 2 | 并发回切 | 并发请求下 failover 与 switch-back | 已覆盖 |
| 3 | 多节点链式故障 | 候选节点连续故障场景 | 已覆盖 |
| 4 | 双节点抖动循环 | 多轮循环下时序与稳定性 | 已覆盖 |
| 5 | IPv6 cross failover | IPv6 连接场景故障切换 | 已覆盖 |
| 6 | 闪断优先回连当前节点 | 先尝试当前活跃节点，失败后再回退到其他候选节点 | 已覆盖（`TestClientReconnectTriesActiveEndpointFirstThenFallsBack`） |

## 7. 易用性测试（可选）

该重构为 driver 后端能力重构，不涉及 UI 界面，易用性测试不适用。

## 8. 长期稳定性测试（可选）

门禁已纳入正文回归流程，主入口为 `ws/reliability_gate.sh`。

| # | 门禁模式 | 命令 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 快速回归 | `./ws/reliability_gate.sh quick` | 已纳入流程 |
| 2 | 循环回归 | `./ws/reliability_gate.sh loop` | 已纳入流程 |
| 3 | 全量基线 | `./ws/reliability_gate.sh full` | 已纳入流程 |
| 4 | 全量集成 | `./ws/reliability_gate.sh full-integration` | 已纳入流程 |
| 5 | Cross 烟测 | `./ws/reliability_gate.sh cross-smoke` | 已纳入流程 |
| 6 | Cross 全量 | `./ws/reliability_gate.sh cross-full` | 已纳入流程 |
| 7 | Cross 循环 | `LOOP_COUNT=20 ./ws/reliability_gate.sh cross-loop` | 已纳入流程 |
| 8 | Cross 全量循环 | `LOOP_COUNT=20 ./ws/reliability_gate.sh cross-full-loop` | 已纳入流程 |

CI 对应门禁：`.github/workflows/ws-unified-cross-failover.yml` 每日调度 `cross-full-loop`。

## 9. 性能测试

```plaintext
           ts            |   connector_version    |               mode               |     protocol      |            qps            | concurrency | auto_create_sub_table | interlace_rows |   tables    |   records   |     subscribe_mode     |             topic              |
===================================================================================================================================================================================================================================================================================
 2026-03-20 17:06:20.304 | 3.8.0                  | stmt2_insert                     | WebSocket         |          3609643.84294804 |          16 | false                 |              0 |     1000000 |         100 |                        |                                |
 2026-03-20 17:05:13.346 | 3.8.0                  | sql_insert                       | WebSocket         |          2297151.69642533 |          16 | false                 |              0 |     1000000 |         100 |                        |                                |
 2026-03-20 17:03:32.810 | 3.8.0                  | stmt2_insert                     | WebSocket         |          609658.799120691 |          16 | false                 |              1 |     1000000 |         100 |                        |                                |
 2026-03-20 16:57:28.647 | 3.8.0                  | sql_insert                       | WebSocket         |          297774.346679657 |          16 | false                 |              1 |     1000000 |         100 |                        |                                |
 2026-03-20 16:44:38.435 | 3.8.0                  | stmt2_insert                     | WebSocket         |          6232009.38778028 |          16 | false                 |              0 |       10000 |       10000 |                        |                                |
 2026-03-20 16:43:59.503 | 3.8.0                  | sql_insert                       | WebSocket         |          5485470.04766179 |          16 | false                 |              0 |       10000 |       10000 |                        |                                |
 2026-03-20 16:43:13.601 | 3.8.0                  | stmt2_insert                     | WebSocket         |          6291178.64044998 |          16 | false                 |              1 |       10000 |       10000 |                        |                                |
 2026-03-20 16:42:34.908 | 3.8.0                  | sql_insert                       | WebSocket         |          284479.161841563 |          16 | false                 |              1 |       10000 |       10000 |                        |                                |
 2026-03-20 16:30:08.807 | 3.8.0                  | stmt2_insert                     | WebSocket         |          5803846.98203793 |          16 | true                  |              0 |     1000000 |         100 |                        |                                |
 2026-03-20 16:29:19.380 | 3.8.0                  | sql_insert                       | WebSocket         |          5603832.90196224 |          16 | true                  |              0 |     1000000 |         100 |                        |                                |
 2026-03-20 16:28:24.247 | 3.8.0                  | stmt2_insert                     | WebSocket         |          2169107.95339672 |          16 | true                  |              1 |     1000000 |         100 |                        |                                |
 2026-03-20 16:26:35.415 | 3.8.0                  | sql_insert                       | WebSocket         |          501948.824067059 |          16 | true                  |              1 |     1000000 |         100 |                        |                                |
 2026-03-20 16:19:05.202 | 3.8.0                  | stmt2_insert                     | WebSocket         |          6900309.37136159 |          16 | true                  |              0 |       10000 |       10000 |                        |                                |
 2026-03-20 16:18:28.562 | 3.8.0                  | sql_insert                       | WebSocket         |          5927019.02839599 |          16 | true                  |              0 |       10000 |       10000 |                        |                                |
 2026-03-20 16:17:46.291 | 3.8.0                  | stmt2_insert                     | WebSocket         |          6062688.11373474 |          16 | true                  |              1 |       10000 |       10000 |                        |                                |
 2026-03-20 16:17:06.688 | 3.8.0                  | sql_insert                       | WebSocket         |          471249.972548976 |          16 | true                  |              1 |       10000 |       10000 |                        |                                |
 2026-03-20 16:09:34.619 | 3.7.1                  | stmt2_insert                     | WebSocket         |                         0 |           0 | false                 |              0 |     1000000 |         100 |                        |                                |
 2026-03-20 16:08:58.371 | 3.7.1                  | sql_insert                       | WebSocket         |           2387244.2374459 |          16 | false                 |              0 |     1000000 |         100 |                        |                                |
 2026-03-20 16:07:18.639 | 3.7.1                  | stmt2_insert                     | WebSocket         |                         0 |           0 | false                 |              1 |     1000000 |         100 |                        |                                |
 2026-03-20 16:03:22.195 | 3.7.1                  | sql_insert                       | WebSocket         |          296609.264626145 |          16 | false                 |              1 |     1000000 |         100 |                        |                                |
 2026-03-20 15:50:22.821 | 3.7.1                  | stmt2_insert                     | WebSocket         |                         0 |           0 | false                 |              0 |       10000 |       10000 |                        |                                |
 2026-03-20 15:50:03.320 | 3.7.1                  | sql_insert                       | WebSocket         |          5873477.63691492 |          16 | false                 |              0 |       10000 |       10000 |                        |                                |
 2026-03-20 15:49:20.403 | 3.7.1                  | stmt2_insert                     | WebSocket         |                         0 |           0 | false                 |              1 |       10000 |       10000 |                        |                                |
 2026-03-20 15:48:51.304 | 3.7.1                  | sql_insert                       | WebSocket         |          281942.763080899 |          16 | false                 |              1 |       10000 |       10000 |                        |                                |
 2026-03-20 15:36:11.885 | 3.7.1                  | stmt2_insert                     | WebSocket         |                         0 |           0 | true                  |              0 |     1000000 |         100 |                        |                                |
 2026-03-20 15:35:46.552 | 3.7.1                  | sql_insert                       | WebSocket         |          5673076.91505279 |          16 | true                  |              0 |     1000000 |         100 |                        |                                |
 2026-03-20 15:34:55.072 | 3.7.1                  | stmt2_insert                     | WebSocket         |                         0 |           0 | true                  |              1 |     1000000 |         100 |                        |                                |
 2026-03-20 15:32:33.715 | 3.7.1                  | sql_insert                       | WebSocket         |          458116.187022112 |          16 | true                  |              1 |     1000000 |         100 |                        |                                |
 2026-03-20 15:24:39.460 | 3.7.1                  | stmt2_insert                     | WebSocket         |                         0 |           0 | true                  |              0 |       10000 |       10000 |                        |                                |
 2026-03-20 15:24:18.034 | 3.7.1                  | sql_insert                       | WebSocket         |          5892079.44833239 |          16 | true                  |              0 |       10000 |       10000 |                        |                                |
 2026-03-20 15:23:35.143 | 3.7.1                  | stmt2_insert                     | WebSocket         |                         0 |           0 | true                  |              1 |       10000 |       10000 |                        |                                |
 2026-03-20 15:23:13.152 | 3.7.1                  | sql_insert                       | WebSocket         |          471955.840235257 |          16 | true                  |              1 |       10000 |       10000 |                        |                                |
 2026-03-20 15:15:27.318 | 3.8.0                  | subscription                     | WebSocket         |          1585450.12649196 |          16 | true                  |              0 |           0 |           0 | load-balanced          | topic_tmq_b_db_interlace       |
 2026-03-20 15:08:03.032 | 3.8.0                  | subscription                     | WebSocket         |          8361573.30101603 |          16 | true                  |              0 |           0 |           0 | load-balanced          | topic_tmq_b_db_progressive     |
 2026-03-20 15:00:34.788 | 3.8.0                  | subscription                     | WebSocket         |          570849.612375468 |           4 | true                  |              0 |           0 |           0 | load-balanced          | topic_tmq_b_db_interlace       |
 2026-03-20 14:53:38.267 | 3.8.0                  | subscription                     | WebSocket         |          2408894.54900029 |           4 | true                  |              0 |           0 |           0 | load-balanced          | topic_tmq_b_db_progressive     |
 2026-03-20 14:46:40.016 | 3.8.0                  | subscription                     | WebSocket         |          160238.934330762 |           1 | true                  |              0 |           0 |           0 | load-balanced          | topic_tmq_b_db_interlace       |
 2026-03-20 14:39:51.717 | 3.8.0                  | subscription                     | WebSocket         |          645376.546296648 |           1 | true                  |              0 |           0 |           0 | load-balanced          | topic_tmq_b_db_progressive     |
 2026-03-20 14:33:01.549 | 3.8.0                  | subscription                     | WebSocket         |          2160407.15662733 |          16 | true                  |              0 |           0 |           0 | broadcast              | topic_tmq_b_db_interlace       |
 2026-03-20 14:25:35.053 | 3.8.0                  | subscription                     | WebSocket         |          8441474.34821988 |          16 | true                  |              0 |           0 |           0 | broadcast              | topic_tmq_b_db_progressive     |
 2026-03-20 14:18:08.457 | 3.8.0                  | subscription                     | WebSocket         |          618859.440251152 |           4 | true                  |              0 |           0 |           0 | broadcast              | topic_tmq_b_db_interlace       |
 2026-03-20 14:11:09.958 | 3.8.0                  | subscription                     | WebSocket         |          2619738.25923374 |           4 | true                  |              0 |           0 |           0 | broadcast              | topic_tmq_b_db_progressive     |
 2026-03-20 14:04:09.736 | 3.8.0                  | subscription                     | WebSocket         |          158466.117886476 |           1 | true                  |              0 |           0 |           0 | broadcast              | topic_tmq_b_db_interlace       |
 2026-03-20 13:57:19.231 | 3.8.0                  | subscription                     | WebSocket         |           649804.78880927 |           1 | true                  |              0 |           0 |           0 | broadcast              | topic_tmq_b_db_progressive     |
 2026-03-20 13:50:28.919 | 3.7.1                  | subscription                     | WebSocket         |           1579549.7861754 |          16 | true                  |              0 |           0 |           0 | load-balanced          | topic_tmq_b_db_interlace       |
 2026-03-20 13:43:04.570 | 3.7.1                  | subscription                     | WebSocket         |          8463451.04570199 |          16 | true                  |              0 |           0 |           0 | load-balanced          | topic_tmq_b_db_progressive     |
 2026-03-20 13:35:38.464 | 3.7.1                  | subscription                     | WebSocket         |          576333.783767055 |           4 | true                  |              0 |           0 |           0 | load-balanced          | topic_tmq_b_db_interlace       |
 2026-03-20 13:28:41.892 | 3.7.1                  | subscription                     | WebSocket         |          2424213.40903183 |           4 | true                  |              0 |           0 |           0 | load-balanced          | topic_tmq_b_db_progressive     |
 2026-03-20 13:21:41.528 | 3.7.1                  | subscription                     | WebSocket         |          160448.549163623 |           1 | true                  |              0 |           0 |           0 | load-balanced          | topic_tmq_b_db_interlace       |
 2026-03-20 13:14:51.112 | 3.7.1                  | subscription                     | WebSocket         |           671708.12005354 |           1 | true                  |              0 |           0 |           0 | load-balanced          | topic_tmq_b_db_progressive     |
 2026-03-20 13:08:01.061 | 3.7.1                  | subscription                     | WebSocket         |          2164725.99530336 |          16 | true                  |              0 |           0 |           0 | broadcast              | topic_tmq_b_db_interlace       |
 2026-03-20 13:00:34.753 | 3.7.1                  | subscription                     | WebSocket         |          8472668.57447828 |          16 | true                  |              0 |           0 |           0 | broadcast              | topic_tmq_b_db_progressive     |
 2026-03-20 12:53:04.032 | 3.7.1                  | subscription                     | WebSocket         |          624774.953349692 |           4 | true                  |              0 |           0 |           0 | broadcast              | topic_tmq_b_db_interlace       |
 2026-03-20 12:46:07.563 | 3.7.1                  | subscription                     | WebSocket         |          2525394.50514025 |           4 | true                  |              0 |           0 |           0 | broadcast              | topic_tmq_b_db_progressive     |
 2026-03-20 12:39:09.257 | 3.7.1                  | subscription                     | WebSocket         |          160029.580824628 |           1 | true                  |              0 |           0 |           0 | broadcast              | topic_tmq_b_db_interlace       |
 2026-03-20 12:32:21.149 | 3.7.1                  | subscription                     | WebSocket         |          669446.008547918 |           1 | true                  |              0 |           0 |           0 | broadcast              | topic_tmq_b_db_progressive     |
 2026-03-20 12:07:55.715 | 3.8.0                  | query                            | WebSocket         |          19344.2744859442 |         120 | true                  |              0 |           0 |           0 |                        |                                |
 2026-03-20 12:04:13.131 | 3.8.0                  | query                            | WebSocket         |          1500702.88946855 |           4 | true                  |              0 |           0 |           0 |                        |                                |
 2026-03-20 11:59:46.121 | 3.8.0                  | query                            | WebSocket         |          614688.793956704 |           1 | true                  |              0 |           0 |           0 |                        |                                |
 2026-03-20 11:57:09.071 | 3.7.1                  | query                            | WebSocket         |          15583.0392561867 |         120 | true                  |              0 |           0 |           0 |                        |                                |
 2026-03-20 11:52:56.977 | 3.7.1                  | query                            | WebSocket         |            1231446.144461 |           4 | true                  |              0 |           0 |           0 |                        |                                |
 2026-03-20 11:48:01.419 | 3.7.1                  | query                            | WebSocket         |          493255.485127631 |           1 | true                  |              0 |           0 |           0 |                        |                                |
```

### 9.1 关键结果

- `stmt2_insert`（3.8.0 新增 WebSocket 能力）QPS 区间：`609,659 ~ 6,900,309`。
- 3.8.0 同场景下 `stmt2_insert` 对 `sql_insert`：倍率区间 `1.04x ~ 22.11x`，中位数 `1.81x`。
- Query（WebSocket）3.8.0 对比 3.7.1：平均 `+23.54%`。

### 9.2 Query 对比明细（WebSocket）

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 并发 120 | 3.7.1: `15583.04`，3.8.0: `19344.27` | `+24.14%` |
| 2 | 并发 4 | 3.7.1: `1231446.14`，3.8.0: `1500702.89` | `+21.87%` |
| 3 | 并发 1 | 3.7.1: `493255.49`，3.8.0: `614688.79` | `+24.62%` |

### 9.3 stmt2 与 sql_insert 同场景对比（3.8.0 WebSocket）

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | false + 1,000,000/100 + interlace=0 | stmt2: `3609643.84`，sql: `2297151.70` | `1.57x` |
| 2 | false + 1,000,000/100 + interlace=1 | stmt2: `609658.80`，sql: `297774.35` | `2.05x` |
| 3 | false + 10,000/10,000 + interlace=0 | stmt2: `6232009.39`，sql: `5485470.05` | `1.14x` |
| 4 | false + 10,000/10,000 + interlace=1 | stmt2: `6291178.64`，sql: `284479.16` | `22.11x` |
| 5 | true + 1,000,000/100 + interlace=0 | stmt2: `5803846.98`，sql: `5603832.90` | `1.04x` |
| 6 | true + 1,000,000/100 + interlace=1 | stmt2: `2169107.95`，sql: `501948.82` | `4.32x` |
| 7 | true + 10,000/10,000 + interlace=0 | stmt2: `6900309.37`，sql: `5927019.03` | `1.16x` |
| 8 | true + 10,000/10,000 + interlace=1 | stmt2: `6062688.11`，sql: `471249.97` | `12.87x` |

## 10. 安全测试

权限、敏感信息等方面，重点覆盖以下内容：

- 请求日志上下文敏感字段脱敏（OTP 等）。
- 错误路径凭据保护（password/token）。
- 重连与错误包装路径的安全边界。
对应测试：`request_log_context_test.go`、`taosWS/connection_security_test.go`。

## 11. 兼容性测试

WebSocket 兼容性验证重点如下：

- 兼容入口保留：`taosWS`、`ws/stmt`、`ws/schemaless`、`ws/tmq`。
- DSN 兼容路径覆盖：`dsn_boundary_test`、`dsn_driver_test`、`dsn_feature_test`。
- 发布前回归建议：`go test -race ./ws/... -count=1` + `LOOP_COUNT=20 ./ws/reliability_gate.sh cross-full-loop`。

## 12. 已知问题和限制（可选）

- Query ResultSet 仍为连接绑定语义，连接断开后需重新发起查询。
