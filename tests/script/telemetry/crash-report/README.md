# Table of Contents

1. [Introduction](#1-introduction)
1. [Prerequisites](#2-prerequisites)
1. [Running](#3-running)

# 1. Introduction

This manual is intended to give developers comprehensive guidance to collect crash information from the past 7 days and report it to the FeiShu notification group.

> [!NOTE]
> - The commands and scripts below are verified on Linux (CentOs 7.9.2009).

# 2. Prerequisites

- Install Python3

```bash
yum install python3
yum install python3-pip
```

- Install Python dependencies

```bash
pip3 install requests python-dotenv
```

- Adjust .env file

```bash
cd $DIR/telemetry/crash-report
cp .env.example .env
vim .env
...
```

- Example for .env

```bash
# Statistics of detailed crash information for the version
VERSION="3.3.2.*"
# Statistics of crash counts for different versions
VERSION_LIST=3.3.5.*,3.3.6.*
# Filter to exclude IP (Company network export IP)
EXCLUDE_IP="192.168.1.10"
# Official website server IP
SERVER_IP="192.168.1.11"
# Internal network providing HTTP service IP and port, used for HTML report browsing
HTTP_SERV_IP="192.168.1.12"
HTTP_SERV_PORT=8080
# Webhook address for feiShu group bot
FEISHU_MSG_URL="https://open.feishu.cn/open-apis/bot/v2/hook/*******"
# Owner
OWNER="Jayden Jia"
```

# 3. Running

In `$DIR/telemetry/crash-report` directory, there are several files with names like 202501**.txt. The python script will collect crash information from these text files and send report to your Feishu bot group.

```bash
cd $DIR/telemetry/crash-report
python3 CrashCounter.py
```
