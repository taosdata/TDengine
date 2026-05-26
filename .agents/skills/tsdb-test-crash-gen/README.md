# Crash Gen Test

在目标服务器上一键安装 TDengine TSDB 企业版并运行 crash_gen 混沌测试。

## 概述

crash_gen 是 TDengine 的混沌测试工具，通过多线程并发随机生成建库、建表、写入、查询、删除等操作，验证 TDengine 在各种异常操作组合下不崩溃、不死锁。本技能面向需要快速验证 TDengine 版本健壮性的测试工程师，输入目标服务器 IP 和可选版本号，自动完成以下全流程：

1. 调用 `tsdb-ops-install` Skill 安装并启动 TDengine
2. 从 GitHub 下载 [crash_gen](https://github.com/taosdata/TDengine/tree/main/tests/pytest/crash_gen) 测试工具
3. 安装 Python 运行环境及依赖
4. 执行 crash_gen 混沌测试并输出结论

## 触发场景

- 需要在一台指定服务器上安装 TDengine 并运行 crash_gen 混沌测试
- 需要对 TDengine 新版本做快速回归验证 / 版本验收（验证不崩溃、不死锁）
- 触发关键词：`crash-gen`、`crash_gen`、`混沌测试`、`chaos test`、`run crash gen`、`运行 crash_gen`

## 使用方式

在 Agent 对话中直接描述目标即可触发，示例：

```
在 192.168.3.115 上运行 crash_gen 测试
```

```
在 192.168.3.115 上安装 TDengine 3.4.0.14 并运行 crash_gen
```

```
在 192.168.3.115 上跑一下 crash_gen 测试，版本 3.4.0.14，步数调到 100
```

也可以使用 Slash Command 风格：

```
/tsdb-test-crash-gen 192.168.3.115
/tsdb-test-crash-gen 192.168.3.115 3.4.0.14
```

## 参数说明

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | ✅ | — | 目标服务器 IP 地址（密钥或密码登录均可） |
| `version` | 可选 | 自动发现 NAS 最新版本 | 完整四位版本号，如 `3.4.0.14` |
| `branch` | 可选 | `main` | 从 GitHub 拉取 crash_gen 的分支名 |
| `max_steps` | 可选 | `50` | crash_gen 最大执行步数 |
| `max_dbs` | 可选 | `2` | crash_gen 最大数据库数量 |
| `num_threads` | 可选 | `2` | crash_gen 并发线程数 |
| `ignore_errors` | 可选 | 见下方 | 忽略的错误码列表（`-g` 参数） |
| `work_dir` | 可选 | `~/crash_gen_test` | 目标服务器上的工作目录 |

不指定 `version` 时，Skill 会自动从内网 NAS（`192.168.1.131`）查找最新可用版本。

`ignore_errors` 默认值：`0x32c,0x32d,0x3d3,0x18,0x2501,0x369,0x388,0x061a,0x2550,0x0203,0x4012`。

### 调参建议

- **快速冒烟**：使用默认参数（`--max-steps=50 --max-dbs=2`），通常几分钟内完成
- **延长测试**：增大 `max_steps`（如 `200`）和 `max_dbs`（如 `4`）以提高覆盖率
- **加压测试**：增大 `num_threads`（如 `8`），注意目标服务器 CPU/内存资源

## 执行流程

1. **安装 TDengine** — 调用 `tsdb-ops-install` Skill 完成安装并启动 taosd 服务
2. **SSH 连接检测** — 自动测试免密登录；若未配置，提示用户输入 SSH 密码
3. **下载 crash_gen** — 从 GitHub 获取 `crash_gen_bootstrap.py` 及 `crash_gen/` 目录
4. **安装 Python 依赖** — 安装 taospy、psutil、requests、fabric2、tzlocal、distro、pandas、toml
5. **用户确认** — 展示操作摘要，等待用户输入 `y` 后再执行
6. **运行测试** — 执行 crash_gen 混沌测试
7. **输出结果** — 返回码为 0 表示通过，非 0 给出排查建议

## crash_gen 命令参考

Skill 默认执行的完整命令如下：

```bash
python3 crash_gen_bootstrap.py \
    --max-dbs=2 \
    --connector-type=native \
    --larger-data \
    --dynamic-db-table-names \
    --per-thread-db-connection \
    --max-steps=50 \
    --num-threads=2 \
    --continue-on-exception \
    --run-with-pkg \
    -g 0x32c,0x32d,0x3d3,0x18,0x2501,0x369,0x388,0x061a,0x2550,0x0203,0x4012
```

返回码为 `0` 表示执行成功。

## 依赖

- 本 Skill 依赖 `tsdb-ops-install` Skill（位于 `skills/tsdb-ops-install/`），用于安装并启动 TDengine
- crash_gen 源码来自 [taosdata/TDengine](https://github.com/taosdata/TDengine/tree/main/tests/pytest/crash_gen) 仓库

## 前提条件

- Agent 所在机器能通过 SSH 连接目标服务器（密钥或密码均可）；未配置免密登录时，Skill 会在执行过程中提示输入密码
- 目标服务器能访问内网 NAS（`192.168.1.131`），用于下载 TDengine 安装包
- 目标服务器能访问 GitHub（`github.com` / `raw.githubusercontent.com`），用于下载 crash_gen
- 目标服务器为 Linux x64 系统

## 常见错误

| 场景 | 排查建议 |
| --- | --- |
| SSH 连接失败（`Connection refused` / `Permission denied`） | 确认目标 IP 正确、SSH 服务运行、root 密钥已配置或密码正确 |
| TDengine 安装失败 | 参考 `tsdb-ops-install` Skill 的常见错误 |
| GitHub 下载失败（`curl: (7) Failed to connect`） | 检查目标服务器网络，确认能访问 GitHub；或指定正确的 `branch` |
| `python3` 不存在 | 确认系统包管理器（apt/yum/dnf）可用，手动安装 python3 |
| `pip install` 失败 | 检查 pip 源是否可达，可改用国内镜像 `-i https://pypi.tuna.tsinghua.edu.cn/simple` |
| crash_gen 导入报错（`ModuleNotFoundError: taos`） | 确认 taospy 已正确安装，运行 `python3 -c "import taos"` 验证 |
| crash_gen 返回非零 | 查看输出中的异常堆栈信息，检查 `/var/log/taos/taosdlog*` 日志 |

## 作者

- Author: JaydenJia
- Team: Platform
- Version: 0.1.0
