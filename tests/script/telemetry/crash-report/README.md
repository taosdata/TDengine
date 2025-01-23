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
```

# 3. Running

In `$DIR/telemetry/crash-report` directory, there are several files with names like 202501**.txt. The python script will collect crash information from these text files and send report to your Feishu bot group.

```bash
cd $DIR/telemetry/crash-report
python3 CrashCounter.py
```
