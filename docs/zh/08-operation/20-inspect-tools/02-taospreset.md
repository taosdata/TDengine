---
sidebar_label: 安装前预配置工具
title: 安装前预配置工具
toc_max_heading_level: 4
---

## 背景

TDengine TSDB 的安装部署对环境系统有一定的依赖和要求，安装部署前需要进行环境预配置操作，本文档旨在说明安装前预配置工具在安装 TDengine TSDB 前对环境的预配置内容和工具的使用方法。

## 预配置工具使用方法

工具支持通过 help 参数查看支持的语法

```help
usage: taospreset [-h] [--model {local,ssh}] [--config CONFIG] [--backend] [--disable-kysec] [--result RESULT] [--version] [--log-level {debug,info}]

Pre-set for Database installation

optional arguments:
  -h, --help            show this help message and exit
  --model {local,ssh}, -m {local,ssh}
                        connection model, default: local
  --config CONFIG, -f CONFIG
                        Full path of test config file
  --backend, -b         Run process in backend. default: False
  --disable-kysec, -d   Disable kysec, default: False
  --result RESULT, -r RESULT
                        Result directory. default: ./
  --version, -v         Show version
  --log-level {debug,info}, -l {debug,info}
                        Set log level, default: info (options: debug, info)
```

### 参数详细说明

- `model`：预配置工具运行模式，分为 local 和 ssh。安装环境的多节点间支持 SSH 通信，可选择 ssh 模式，在任意节点上运行预配置工具，会依次对所有节点环境完成预配置操作。反之，节点间不支持 SSH 通信时，可选择 local 模式，仅对工具运行所在机器完成预配置操作，默认为 local 模式。
- `config`：预配置工具加载的配置文件，其具体配置方式详见 **配置文件使用说明** 章节。不配置 config 参数时配置文件默认路径为工具运行当前目录。
- `backend`：后台运行预配置工具，默认为前台运行。
- `disable-kysec`：是否关闭 Kylin Security 服务，KySec 是麒麟系统的安全模块框架，类似于 SELinux、AppArmor、Trusted Computing 的集合体，主要用于增强系统安全性。默认为 False。
- `result`: 安装前配置结果文档的输出路径。不配置 result 参数时默认路径为工具运行当前目录。
- `log-level`: 输出日志级别，目前支持 debug 和 info，模式为 info。
- `version`：打印预配置工具版本信息。

### 配置文件使用说明

```config
# 安装部署 TDengine TSDB 的环境信息，支持免密登录和 SSH 登录两种方式，当环境配置了免密登录后可不用配置 password 信息
[test_env]
# 节点间通过 SSH 协议访问
firstep=192.168.0.1||fqdn=tdengine1||username=root||password=123456||port=22
secondep=192.168.0.2||fqdn=tdengine2||username=root||password=123456||port=22
dnode3=192.168.0.3||fqdn=tdengine3||username=root||username=123456||port=22

# 节点间配置免密登录
# firstep=192.168.0.1||fqdn=tdengine1||username=root||port=22
# secondep=192.168.0.2||fqdn=tdengine2||username=root||port=22
# dnode3=192.168.0.3||fqdn=tdengine3||username=root||port=22

# 系统时区，工具会按照下面配置修改系统时区
[timezone]
tz=Asia/Shanghai

# 系统服务状态，工具会按照下面配置关闭对应的系统服务
[services]
firewall=inactive
selinux=inactive

# coredump 配置，工具会按照下面配置 coredump 的生成路径
[coredump]
kernel.core_pattern=/data/taos/core/core-%%e-%%p

# /etc/sysctl.conf 中系统参数，工具会按照下面配置修改系统参数值
[sys_vars:/etc/sysctl.conf]
fs.nr_open=2147483584
fs.file-max=2147483584
net.ipv4.ip_local_port_range=10000 65534

# /etc/security/limits.conf 中系统参数，工具会按照下面配置修改系统参数值
[sys_vars:/etc/security/limits.conf]
* soft nproc=65536
* soft nofile=2147483584
* soft stack=65536
* hard nproc=65536
* hard nofile=2147483584
* hard stack=65536
root soft nproc=65536
root soft nofile=2147483584
root soft stack=65536
root hard nproc=65536
root hard nofile=2147483584
root hard stack=65536
```

## 环境预配置范围

| **预配置项目** | **详细说明** |
|:--|:----------|
| **配置系统时区**   | 配置系统时区为用户预设定时区 |
| **关闭防火墙** | 关闭系统的防火墙服务 |
| **关闭 SElinux 服务**   | 关闭系统 SElinux 服务 |
| **配置系统参数**   | 配置用户预设定的系统参数 |
| **配置 coredump**   | 配置 coredump 生成目录并开启服务 |
| **修改机器 Hostname**   | 当机器 Hostname 为默认的 localhost 时更新为配置文件中预设定的 FQDN |
| **配置域名解析**   | 将配置文件中安装节点的 FQDN 和 IP 配置到 /etd/hosts 文件 |

## 结果文件

安装前预配置工具运行后会在工具运行当前目录下生成 preset_report.md 文件，其中包含了预配置工具修改的具体项目。

## 应用示例

在工具所在节点以 local 模式执行安装前预配置

```shell
./taospreset 
```

以 SSH 模式在所有节点执行安装前预配置

```shell
./taospreset -m ssh
```

指定配置文件并以 SSH 模式在所有节点执行安装前预配置

```shell
./taospreset -m ssh -f /path_to_file/preset.cfg
```

以 SSH 模式在所有节点执行安装前预配置并关闭 Kylin Security 服务

```shell
./taospreset -m ssh -d
```

以 SSH 模式在所有节点执行安装前预配置，开启日志 debug 级别

```shell
./taospreset -m ssh -l debug
```
