# 目录

1. [介绍](#1-介绍)
1. [前置条件](#2-前置条件)
1. [运行](#3-运行)

# 1. 介绍

本手册旨在为开发人员提供全面的指导，以收集过去7天的崩溃信息并将其报告到飞书通知群。

> [!NOTE]
> - 下面的命令和脚本已在 Linux（CentOS 7.9.2009）上验证.

# 2. 前置条件

- 安装 Python3

```bash
yum install python3
yum install python3-pip
```

- 安装 Python 依赖

```bash
pip3 install requests python-dotenv
```

- 调整 .env 文件

```bash
cd $DIR/telemetry/crash-report
cp .env.example .env
vim .env
...
```

- .env 样例
```bash
# 统计详细 crash 信息的版本
VERSION="3.3.2.*"
# 统计不同版本 crash 数量
VERSION_LIST=3.3.5.*,3.3.6.*
# 过滤器排除 IP（公司网络出口 IP）
EXCLUDE_IP="192.168.1.10"
# 英文官网服务器 IP
SERVER_IP="192.168.1.11"
# 内网提供 HTTP 服务的 IP 及端口，用于提供 HTML 报告浏览
HTTP_SERV_IP="192.168.1.12"
HTTP_SERV_PORT=8080
# 飞书群机器人 webhook 地址
FEISHU_MSG_URL="https://open.feishu.cn/open-apis/bot/v2/hook/*******"
# 负责人
OWNER="Jayden Jia"
```

# 3. 运行

在 $DIR/telemetry/crash-report 目录中，有类似文件名为 202501**.txt 的一些文件。Python 脚本会将从这些文本文件中收集崩溃信息，并将报告发送到您的飞书机器人群组中。

```bash
cd $DIR/telemetry/crash-report
python3 CrashCounter.py
```
