---
sidebar_label: 安装前检查工具
title: 安装前检查工具
toc_max_heading_level: 4
---

## 背景

TDengine TSDB 的安装部署对环境系统有一定的依赖和要求，安装部署前需要对环境进行安装前检查，提前发现环境问题，本文档旨在说明安装前预配置工具在安装 TDengine TSDB 前对环境的预配置内容和工具的使用方法。

## 安装件检查工具使用方法

工具支持通过 help 参数查看支持的语法

```help
usage: taosprecheck [-h] [--model {local,ssh}] [--config CONFIG] [--backend] [--result RESULT] [--version] [--log-level {debug,info}]

Pre-check for Database installation

optional arguments:
  -h, --help            show this help message and exit
  --model {local,ssh}, -m {local,ssh}
                        connection model, default: local
  --config CONFIG, -f CONFIG
                        Full path of test config file
  --backend, -b         Run process in backend. default: False
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
- `result`: 安装前检查结果文档的输出路径。不配置 result 参数时默认路径为工具运行当前目录。
- `log-level`: 输出日志级别，目前支持 debug 和 info，模式为 info
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

# 预安装软件列表
[app_list]
screen
tmux
gdb
fio
iperf
iperf3
sysstat
net-tools
jansson
snappy
ntp
chrony
tree
wget
```

## 安装前检查范围

| **检查项目** | **详细说明** |
|:--|:----------|
| **CPU 配置** | CPU 型号、核数 |
| **内存配置** | 物理内存和虚拟内存大小 |
| **磁盘配置** | 磁盘空间、磁盘类型、磁盘挂载信息、fsblk 信息和当前磁盘使用情况 |
| **网络配置** | SSH 服务状态、22 端口是否可用和网络贷款 |
| **系统配置** | 系统名称、当前时区配置、防火墙和 SElinux 服务状态 |
| **coredump 配置** | coredump 路径是否配置 |
| **域名解析配置** | /etd/hosts 文件是否包含安装 TDengine TSDB 集群所有节点的域名解析信息 |
| **预安装软件** | 指定的原装软件是否已安装，若安装记录其版本 |
| **SWAP 配置** | SWAP 状态和 SWAP 的当前配置 |
| **KYSEC 配置** | KYSEC 服务是否关闭，该项检查仅针对麒麟系统 |
| **系统参数配置** | 检查系统参数值是否与配置文件中指定系统参数的配置一致 |
| **时间同步配置** | 时间同步工具是否安装并计算各节点间的时间偏差，精确到秒 |

## 结果文件

安装前检查工具运行后会在工具运行当前目录下生成 precheck_report.md 和 precheck_advice.md 两个文件，其中 precheck_report.md 包含了检查结果，precheck_advice.md 包含了基于检查结果的一些环境配置建议。

## 应用示例

在工具所在节点以 local 模式执行安装前检查

```shell
./taosprecheck
```

以 SSH 模式在所有节点执行安装前检查

```shell
./taosprecheck -m ssh
```

指定配置文件并以 SSH 模式在所有节点执行安装前检查

```shell
./taosprecheck -m ssh -f /path_to_file/precheck.cfg
```

以 SSH 模式在所有节点执行安装前检查，开启日志 debug 级别

```shell
./taosprecheck -m ssh -l debug
```
