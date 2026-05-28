# CI 失败分析技能使用说明

## 简介

`SKILL.md` 定义了一个 Claude Code 技能，用于自动分析 TDengine Jenkins CI 失败用例的根本原因。
给定一个失败用例的 URL，技能会自动 SSH 登录测试机器，下载并解压日志，逐层分析用例执行详情、
客户端日志和服务端日志，最终输出结构化根因分析报告。

---

## 前置条件

| 条件 | 说明 |
|------|------|
| `sshpass` | 本地需安装 sshpass，用于无交互式 SSH 连接 |
| CI 机器访问权限 | 需要能访问 192.168.1.49 等 CI 机器的 root 账户 |
| 密码存储 | 首次使用时，技能会引导将密码安全存储到 `~/.tdengine-ci/credentials`（600 权限），不明文出现在脚本中 |

---

## 使用方法

在 Claude Code 中直接粘贴失败用例 URL，技能会自动触发，例如：

```
帮我分析这个 CI 失败：http://192.168.1.49:8081/PR-34969_8441_6_20260331-095848/cases/02-Databases/08-Keep/test_mlevel_except.py.0.34.1.txt
```

```
这个用例为什么失败了？http://192.168.1.49:8081/PR-xxxxx/cases/...
```

---

## 技能执行流程

```
用户提供 CI 失败 URL
        │
        ▼
① 解析 URL → 提取 PR号、Build号、日志路径
        │
        ▼
② 检查/初始化 SSH 凭据（~/.tdengine-ci/credentials）
        │
        ▼
③ SSH 到远端机器，检查并解压日志包 (.sim.tar.gz)
        │
        ▼
④ 读取 psim.info（用例执行详情）
        │
        ▼
⑤ 读取客户端日志（taoslog*）
        │
        ▼
⑥ 读取服务端日志（dnode*/taosdlog*）
        │
        ▼
⑦ 综合分析，输出结构化根因分析报告
        │
        ▼（可选，需用户要求）
⑧ 恢复失败当时的 Docker 环境
```

---

## URL 解析规则

| 组件 | URL 示例中的值 | 提取方式 |
|------|----------------|---------|
| SSH 主机 | `192.168.1.49` | URL host 部分（不含端口） |
| Run ID | `PR-34969_8441_6_20260331-095848` | URL path 第一段 |
| PR 号 | `34969` | Run ID 中 `PR-` 后的第一个数字段 |
| Build 号 | `8441` | Run ID 中第二个数字段 |
| 用例基名 | `test_mlevel_except.py.0.34.1` | 最后文件名去掉 `.txt` |

日志路径推导：
```
LOG_DIR  = /var/lib/jenkins/workspace/log/<RUN_ID>/cases/<CASE_DIR>
TAR_FILE = <LOG_DIR>/<CASE_BASE>.sim.tar.gz
SIM_DIR  = <LOG_DIR>/sim
```

---

## 日志文件说明

| 文件 | 说明 |
|------|------|
| `sim/asan/psim.info` | 用例运行的详细执行信息，包含每个 SQL/命令的执行结果 |
| `sim/psim/log/taoslog*` | 客户端日志（TAOS 客户端连接、查询等） |
| `sim/dnode*/log/taosdlog*` | 服务端日志（各 dnode 节点的运行日志） |

---

## 恢复失败环境

如需在 Docker 容器中复现失败时的环境，告知 Claude："帮我恢复这个失败用例的环境"。

详细步骤参见 [`references/restore-env-guide.md`](references/restore-env-guide.md)。

恢复脚本位于 [`scripts/restore-docker.sh`](scripts/restore-docker.sh)，
使用方式：

```bash
# 在目标 CI 机器的 /var/lib/jenkins/workspace/ 目录下执行
./restore-docker.sh -p 34969 -n 8441 -c pr-34969-8441
```

---

## 密码安全说明

- 密码**不会**出现在任何脚本、命令行参数或日志中
- 首次使用时，技能通过交互式方式（`AskUserQuestion`）收集密码，写入 `~/.tdengine-ci/credentials`
- 该文件权限为 `600`（仅 owner 可读写）
- 后续操作通过 `sshpass -f ~/.tdengine-ci/credentials` 使用，密码不暴露在进程参数中

---

## 注意事项

- 分析阶段仅执行只读操作，不修改远端任何文件
- 解压日志时会跳过已存在的 `sim/` 目录，不重复解压
- 如日志文件不存在，技能会给出明确提示而非静默失败
