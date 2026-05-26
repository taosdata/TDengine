---
name: tsdb-ops-install
description: "通过 SSH 在指定服务器上安装 TDengine TSDB 企业版并启动服务。适用场景：安装 TSDB、部署 TDengine、更换 TSDB 版本。触发关键词：tsdb-install, install TSDB, deploy TDengine, 安装 TDengine, 安装 TSDB, 部署 TSDB"
metadata:
  author: yangzy
  version: 1.0.0
  owner_team: engine
---

# skill-tsdb-install

## When to Use

- 需要在一台指定服务器上安装 TDengine TSDB 企业版
- 需要替换已有 TSDB 为指定版本
- 用户触发命令示例：
  - `/tsdb-ops-install 192.168.2.139` — 自动发现 NAS 最新版本
  - `/tsdb-ops-install 192.168.2.139 3.4.0.14` — 安装指定完整版本
  - `/tsdb-ops-install 192.168.2.139 3.3.8` — 安装 `3.3.8.x` 系列最新版本
  - `/tsdb-ops-install 192.168.2.139 3.3` — 安装 `3.3` 系列最新版本

## Input

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | — | 目标服务器 IP 地址 |
| `version` | 否 | 自动发现 NAS 最新版本 | 版本号，支持三种格式：四位完整版本（`3.4.0.14`）、三位前缀（`3.3.8` → 最新 `3.3.8.x`）、两位前缀（`3.3` → 最新 `3.3.x.x`） |
| `install_dir` | 否 | `~/skill_tsdb_install` | 目标服务器上的安装工作目录 |

若用户未提供 `host`，向用户询问目标服务器 IP 后继续。

## Procedure

> **安装脚本**：`scripts/install.sh`（位于本 Skill 目录下）。
> 该脚本封装了版本发现、NAS 校验、下载、安装、启动的全部逻辑。
> AI 仅负责：解析用户意图、获取缺失参数、展示摘要确认、失败时分析日志。

### Step 0: 运行 Telemetry（MUST，最先执行）

见文末 Telemetry 段落，**在执行任何其他操作前**先运行 telemetry 命令。

### Step 1: 解析用户输入

从用户消息中提取以下参数：

- `HOST`：目标服务器 IP（必需，若缺失则用 `ask_user` 询问）
- `VERSION`：版本号（可选，支持 2/3/4 段格式）
- `INSTALL_DIR`：安装目录（可选，默认 `~/skill_tsdb_install`）

### Step 2: 测试 SSH 并获取密码（如需要）

```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no root@<HOST> 'echo ok' 2>&1
```

- 若输出 `ok`：SSH 免密可用，无需额外参数。
- 若失败：使用 `ask_user` 询问 SSH 密码，后续通过 `-p` 参数传递给脚本。

### Step 3: Dry-run — 获取安装计划

调用脚本的 `--dry-run` 模式：

```bash
bash <SKILL_DIR>/scripts/install.sh --dry-run <HOST> [VERSION] [-p PASSWORD] [-d INSTALL_DIR]
```

根据退出码处理：

| 退出码 | 含义 | 处理 |
| --- | --- | --- |
| `0` | 版本解析和 NAS 校验成功 | 向用户展示脚本输出的安装摘要 |
| `1` | SSH 连接失败 | 若未提供密码，询问用户；若已提供，提示连接错误 |
| `2` | 版本不存在或 NAS 不可达 | 将错误信息展示给用户 |

### Step 4: 用户确认

使用 `ask_user` 向用户展示 Step 3 输出的安装摘要，提供 `[确认安装, 取消]` 选项。
用户取消则终止，不执行任何远端操作。

### Step 5: 执行安装

```bash
bash <SKILL_DIR>/scripts/install.sh --yes <HOST> [VERSION] [-p PASSWORD] [-d INSTALL_DIR]
```

将安装过程的输出实时展示给用户。

### Step 6: 结果处理

- **退出码 0**：安装成功，脚本已输出 ✅ 确认信息。
- **退出码 3**：安装失败。**此时是 AI 发挥价值的关键步骤** — 分析脚本输出中的错误信息，结合下方"常见错误"表给出针对性的排查建议。

## Output

- 安装成功确认信息（脚本输出，包含版本号和目标服务器地址）
- 失败时：AI 基于脚本错误输出给出排查建议

## Common Errors

| 场景 | 排查建议 |
| --- | --- |
| SSH 连接失败（`Connection refused` / `Permission denied`） | 确认目标 IP 正确、SSH 服务运行、root 密钥已配置 |
| NAS 版本不存在（退出码 2） | 检查版本号，浏览 `http://192.168.1.131/data/nas/TDengine/` 确认可用版本 |
| wget 超时 | 确认目标服务器能访问内网 NAS（`192.168.1.131`），检查防火墙 |
| `install.sh` 报错 | 查看服务器安装日志，确认磁盘空间充足、系统依赖满足 |
| `start-all.sh` 报错 | 查看 `/var/log/taos/`，确认端口未被占用 |

## Safety

- **禁止**在未获得用户明确确认前执行安装脚本（`--yes` 模式）。
- **禁止**索要、存储或传递 SSH 私钥或任何永久凭据；SSH 密码仅在当前会话中通过 `-p` 参数传递，不写入任何文件。
- 若用户取消确认，输出提示后立即终止，不做任何操作。

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-ops-install version=0.2.0 author=yangzy`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

