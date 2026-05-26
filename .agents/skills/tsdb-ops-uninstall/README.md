# TDengine Uninstall

通过 SSH 在指定服务器上卸载 TDengine TSDB 企业版。

## 概述

本技能面向需要清理服务器上 TDengine TSDB 实例的工程师，常用于重新安装前的环境重置。输入目标服务器 IP，执行 `rmtaos -e yes` 卸载所有 TSDB 组件，**同时删除数据目录、日志目录及配置文件，操作不可恢复，请谨慎使用**。

## 触发场景

- 需要卸载指定服务器上的 TDengine TSDB 企业版
- 需要清理现有 TSDB 实例，为重新安装做准备
- 触发关键词：`tsdb-uninstall`、`uninstall TSDB`、`remove TDengine`、`卸载 TDengine`、`卸载 TSDB`、`删除 TSDB`

## 使用方式

在 Agent 对话中直接描述目标即可触发，示例：

```
卸载 192.168.2.139 上的 TSDB
```

```
清理 192.168.2.139 上的 TDengine 实例
```

也可以使用 Slash Command 风格：

```
/tsdb-ops-uninstall 192.168.2.139
```

## 参数说明

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | ✅ | — | 目标服务器 IP 地址（密钥或密码登录均可） |

## 执行流程

1. **SSH 连接检测** — 自动测试免密登录；若未配置，提示用户输入 SSH 密码
2. **用户确认** — 展示操作摘要，等待用户输入 `y` 后再执行
3. **执行卸载** — SSH 连接目标服务器，执行 `rmtaos -e yes`
4. **输出结果** — 确认卸载成功，或输出失败信息与排查建议

> ⚠️ **注意**：`rmtaos -e yes` 会删除所有数据目录（`/var/lib/taos/`）、日志目录（`/var/log/taos/`）及配置文件（`/etc/taos/`），**操作不可恢复**，执行前请确认数据已备份。

## 前提条件

- Agent 所在机器能通过 SSH 连接目标服务器（密钥或密码均可）；未配置免密登录时，Skill 会在执行过程中提示输入密码

## 常见错误

| 场景 | 排查建议 |
| --- | --- |
| SSH 连接失败（`Connection refused` / `Permission denied`） | 确认目标 IP 正确、SSH 服务运行、root 密钥已配置 |
| `rmtaos: command not found` | TSDB 未安装或 PATH 不包含安装目录，可视为已卸载，正常结束 |
| `rmtaos` 报错退出 | 查看服务器卸载日志，可尝试手动停止 TSDB 服务后重试 |

## 作者

- Author: yangzy
- Team: taosd
- Version: 0.1.0
