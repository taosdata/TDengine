# TDengine Install

通过 SSH 在指定服务器上一键安装 TDengine TSDB 企业版并启动全部服务。

## 概述

本技能面向需要快速在测试/生产服务器上部署 TDengine TSDB 企业版的工程师。输入目标服务器 IP 和可选版本号，自动从内网 NAS 下载安装包、执行静默安装并启动所有组件（taosd、taosadapter、taosx、taos-explorer、taoskeeper）。

## 触发场景

- 需要在一台指定服务器上安装 TDengine TSDB 企业版
- 需要将现有 TSDB 实例升级或替换为指定版本
- 触发关键词：`tsdb-install`、`install TSDB`、`deploy TDengine`、`安装 TDengine`、`安装 TSDB`、`部署 TSDB`

## 使用方式

在 Agent 对话中直接描述目标即可触发，示例：

```
安装 TSDB 到 192.168.2.139
```

```
在 192.168.2.139 上安装 TDengine 3.3.8.22
```

也可以使用 Slash Command 风格：

```
/tsdb-ops-install 192.168.2.139
/tsdb-ops-install 192.168.2.139 3.3.8.22
/tsdb-ops-install 192.168.2.139 3.3.8
/tsdb-ops-install 192.168.2.139 3.3
```

## 参数说明

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | ✅ | — | 目标服务器 IP 地址（密钥或密码登录均可） |
| `version` | 可选 | 自动发现 NAS 最新版本 | 版本号，支持三种格式：四位完整版本（`3.4.0.14`）、三位前缀（`3.3.8` → 最新 `3.3.8.x`）、两位前缀（`3.3` → 最新 `3.3.x.x`） |
| `install_dir` | 可选 | `~/skill_tsdb_install` | 目标服务器上的安装工作目录 |

不指定 `version` 时，Skill 会自动从内网 NAS（`192.168.1.131`）查找最新可用版本。指定部分版本号时（如 `3.3` 或 `3.3.8`），会自动匹配该前缀下的最新版本。

## 执行流程

1. **SSH 连接检测** — 自动测试免密登录；若未配置，提示用户输入 SSH 密码
2. **NAS 校验** — 验证目标版本的安装包是否存在于 NAS
3. **用户确认** — 展示操作摘要，等待用户输入 `y` 后再执行
4. **卸载旧版** — 执行 `rmtaos -e yes` 清理已有实例及其数据目录、日志、配置文件（未安装时忽略）
5. **下载安装** — 从内网 NAS 下载安装包，解压后执行 `./install.sh -e no` 静默安装
6. **启动服务** — 执行 `./start-all.sh` 启动所有 TSDB 组件

## 前提条件

- Agent 所在机器能通过 SSH 连接目标服务器（密钥或密码均可）；未配置免密登录时，Skill 会在执行过程中提示输入密码
- 目标服务器能访问内网 NAS（`192.168.1.131`）
- 目标服务器满足 TDengine 系统依赖（Linux x64、Java 17+）

## 常见错误

| 场景 | 排查建议 |
| --- | --- |
| SSH 连接失败（`Connection refused` / `Permission denied`） | 确认目标 IP 正确、SSH 服务运行、root 密钥已配置 |
| NAS 版本不存在（HTTP 404） | 检查版本号，浏览 `http://192.168.1.131/data/nas/TDengine/` 确认可用版本 |
| `wget` 超时 | 确认目标服务器能访问内网 NAS，检查防火墙规则 |
| `install.sh` 报错 | 查看服务器安装日志，确认磁盘空间充足、系统依赖满足 |
| `start-all.sh` 报错 | 查看 `/var/log/taos/`，确认相关端口未被占用 |

## 作者

- Author: yangzy
- Team: taosd
- Version: 0.2.0
