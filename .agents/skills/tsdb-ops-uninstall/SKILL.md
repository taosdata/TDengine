---
name: tsdb-ops-uninstall
description: "通过 SSH 在指定服务器上卸载 TDengine TSDB 企业版。适用场景：卸载 TSDB、清理 TDengine 实例、重置 TSDB 环境。触发关键词：tsdb-uninstall, uninstall TSDB, remove TDengine, 卸载 TDengine, 卸载 TSDB, 删除 TSDB"
metadata:
  author: yangzy
  version: 1.0.0
  owner_team: engine
---

# skill-tsdb-uninstall

## When to Use

- 需要在一台指定服务器上卸载 TDengine TSDB 企业版
- 需要清理现有 TSDB 实例，为重新安装做准备
- 用户触发命令示例：`/tsdb-uninstall 192.168.2.139`

## Input

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | — | 目标服务器 IP 地址 |

若用户未提供 `host`，向用户询问目标服务器 IP 后继续。

## Procedure

> **卸载脚本**：`scripts/uninstall.sh`（位于本 Skill 目录下）。
> 该脚本封装了 SSH 连接、安装检测、卸载执行的全部逻辑。
> AI 仅负责：解析用户意图、获取缺失参数、展示摘要确认、失败时分析原因。

### Step 0: 运行 Telemetry（MUST，最先执行）

见文末 Telemetry 段落，**在执行任何其他操作前**先运行 telemetry 命令。

### Step 1: 解析用户输入

从用户消息中提取 `HOST`（目标服务器 IP，必需，若缺失则用 `ask_user` 询问）。

### Step 2: 测试 SSH 并获取密码（如需要）

```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no root@<HOST> 'echo ok' 2>&1
```

- 若输出 `ok`：SSH 免密可用，无需额外参数。
- 若失败：使用 `ask_user` 询问 SSH 密码，后续通过 `-p` 参数传递给脚本。

### Step 3: Dry-run — 获取卸载计划

```bash
bash <SKILL_DIR>/scripts/uninstall.sh --dry-run <HOST> [-p PASSWORD]
```

| 退出码 | 含义 | 处理 |
| --- | --- | --- |
| `0` | SSH 正常，TSDB 已安装 | 向用户展示脚本输出的卸载摘要 |
| `1` | SSH 连接失败 | 若未提供密码，询问用户；若已提供，提示连接错误 |
| `2` | TSDB 未安装 | 告知用户目标服务器未安装 TSDB，无需卸载 |

### Step 4: 用户确认

使用 `ask_user` 向用户展示 Step 3 输出的卸载摘要（含不可恢复警告），提供 `[确认卸载, 取消]` 选项。
用户取消则终止，不执行任何远端操作。

### Step 5: 执行卸载

```bash
bash <SKILL_DIR>/scripts/uninstall.sh --yes <HOST> [-p PASSWORD]
```

### Step 6: 结果处理

- **退出码 0**：卸载成功，脚本已输出 ✅ 确认信息。
- **退出码 3**：卸载失败。AI 分析脚本错误输出，结合"常见错误"表给出排查建议。

## Output

- 卸载成功确认信息，包含目标服务器地址
- 失败时：错误信息 + 排查建议

## Common Errors

| 场景 | 排查建议 |
| --- | --- |
| SSH 连接失败（`Connection refused` / `Permission denied`） | 确认目标 IP 正确、SSH 服务运行、root 密钥已配置 |
| `rmtaos: command not found` | TSDB 未安装或 PATH 不包含安装目录，可视为已卸载，正常结束 |
| `rmtaos` 报错退出 | 查看服务器上的卸载日志，可尝试手动停止 TSDB 服务后重试 |

## Safety

- **禁止**在未获得用户明确确认前执行卸载脚本（`--yes` 模式）。
- **禁止**索要、存储或传递 SSH 私钥或任何永久凭据；SSH 密码仅在当前会话中通过 `-p` 参数传递，不写入任何文件。
- `rmtaos -e yes` 会**删除所有数据目录、日志目录和配置文件**，操作不可恢复，必须在用户明确确认后才能执行。

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-ops-uninstall version=1.0.0 author=yangzy`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
