---
sidebar_label: 巡检工具
title: 巡检工具
toc_max_heading_level: 4
---

## 背景

TDengine 在运行一段时间后需要针对运行环境和 TDengine 本身的运行状态进行定期巡检，本文档旨在说明如何使用巡检工具对 TDengine 的运行环境进行自动化检查。

## 巡检工具使用方法

工具支持通过 help 参数查看支持的语法

```help
usage: taosinspect [-h] [--model {local,ssh}] [--config CONFIG] [--result RESULT] [--backend] [--check-nginx] [--log-level {debug,info}] [--version]

Check Database deployment environment

optional arguments:
  -h, --help            show this help message and exit
  --model {local,ssh}, -m {local,ssh}
                        connection model, default: local
  --config CONFIG, -f CONFIG
                        Full path of test config file
  --result RESULT, -r RESULT
                        Result directory. default: None
  --backend, -b         Run process in backend. default: False
  --check-nginx, -cn    Whether check nginx's config, default: False
  --log-level {debug,info}, -l {debug,info}
                        Set log level, default: info (options: debug, info)
  --version, -v         Show version
```

### 参数详细说明

- `model`：安装工具运行模式，分为 local 和 ssh。安装环境的多节点间支持 SSH 通信，可选择 ssh 模式，在任意节点上运行安装工具，会依次对所有节点环境完成安装操作。反之，节点间不支持 SSH 通信时，可选择 local 模式，仅对工具运行所在机器完成安装操作，默认为 local 模式。
- `config`：安装工具加载的配置文件，其具体配置方式详见 **配置文件使用说明** 章节。不配置 config 参数时配置文件默认值为/etc/taos/inspect.cfg。
- `result`：巡检运行结束后结果文件和相关日志文件的存储目录，默认是用户在 taos.cfg 中配置的 logDir 对应目录。
- `backend`：后台运行安装工具，默认前台运行。
- `check-nginx`：是否检测负载均衡 nginx 的配置文件，默认值为不检查。
- `log-level`: 输出日志级别，目前支持 debug 和 info，模式为 info。
- `version`：打印安装工具版本信息。

### 配置文件使用说明

```config
########################################################
#                                                      #
#                  Configuration                       #
#                                                      #
########################################################

# 安装部署TDengine的环境信息，支持免密登录和SSH登录两种方式，当环境配置了免密登录后不用配置password信息。
# 除此外还支持从TDengine自动获取集群信息，该模式下不需配置集群节点的ip和FQDN，仅需要配置连接各节点的用户信息（免密时不用配置password信息）
# 配置方式1、2和3不可同时配置
[test_env]
# 配置方式1: 通过TDengine获取集群信息
username=root
password=123456
port=22

# 配置方式2: 节点间通过SSH协议访问
# firstep=192.168.0.1||fqdn=tdengine1||username=root||password=123456||port=22
# secondep=192.168.0.2||fqdn=tdengine2||username=root||password=123456||port=22
# dnode3=192.168.0.3||fqdn=tdengine3||username=root||username=123456||port=22

# 配置方式3: 节点间配置免密登录
# firstep=192.168.0.1||fqdn=tdengine1||username=root||port=22
# secondep=192.168.0.2||fqdn=tdengine2||username=root||port=22
# dnode3=192.168.0.3||fqdn=tdengine3||username=root||port=22

# TDegine的Restful连接信息
[database]
username=root
password=taosdata
port=6030
rest_port=6041

# Nginx服务所在服务器的连接信息
[nginx]
ip=192.168.0.100
username=root
password=123456
port=22

# oem版本的版本名称，默认不使用
# [oem]
# version=prodb

# /etc/sysctl.conf中系统参数，工具会按照下面配置修改系统参数值
[sys_vars:/etc/sysctl.conf]
fs.nr_open=2147483584
fs.file-max=2147483584
net.ipv4.ip_local_port_range=10000 65534

# /etc/security/limits.conf中系统参数，工具会按照下面配置修改系统参数值
[sys_vars:/etc/security/limits.conf]
* soft nproc=65536
* soft nofile=1048576
* soft stack=65536
* hard nproc=65536
* hard nofile=1048576
* hard stack=65536
root soft nproc=65536
root soft nofile=1048576
root soft stack=65536
root hard nproc=65536
root hard nofile=1048576
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

# 巡检覆盖的TDengine服务范围
[td_services]
taosd
taos
taosadapter
taoskeeper
taosx
taos-explorer

# 可忽略的TDengine错误日志
[skip_error_strs]
failed to get monitor info
Table does not exist
failed to send
Fail to get table info
```

## 巡检范围

### 磁盘巡检范围
| **No** | **巡检项目** | **详细说明** | **告警规则** |
|:-------|:------------|:-----------|:-----------|
| 1 | **磁盘基本信息**   | 磁盘类型和磁盘空间 | 磁盘已用空间低于 15% |
| 2 | **磁盘挂载信息** | 通过 lsblk 查询的磁盘挂载信息 | 无 |
| 3 | **数据库数据目录使用情况**   | 数据目录的挂载路径，文件系统，存储类型，已用空间，可用空间和空间使用率 | 磁盘已用空间低于 15% |
| 4 | **数据库数据目录 Inode 情况**   | 数据目录对应的 idnode 已用空间，可用空间和空间使用率 | 无 |

### 系统巡检范围
| **No** | **巡检项目** | **详细说明** | **告警规则** |
|:-------|:------------|:-----------|:-----------|
| 1 | **系统基本信息**   | 系统名称、系统启动时间、防火墙和 SELinux 服务状态 | 防火墙或 SElinux 服务未关闭 |
| 2 | **域名解析配置** | FQDN 和 IP 信息是否配置到/etc/hosts 文件 | 缺少任一 FQDN 的域名解析 |
| 3 | **预安装软件** | 指定的原装软件是否已安装，若安装记录其版本 | 无 |
| 4 | **系统参数配置** | 检查系统参数值是否与配置文件中指定系统参数的配置一致 | 无 |
| 5 | **系统内存错误** | 收集系统内核错误日志信息 | 存在内核错误日志 |
| 6 | **SWAPNESS 配置** | SWAPNESS 配置状态及其配置值大小 | SWAPNESS 配置值大于 10 |
| 7 | **Coredump 配置** | coredump 路径是否配置 | 1. coredump 未配置；2. coredump 挂载目录为系统根目录；3. coredump 文件个数大于 0 |

### 数据库巡检范围
| **No** | **巡检项目** | **详细说明** | **告警规则** |
|:-------|:------------|:-----------|:-----------|
| 1 | **数据库版本**   | taosd、taos、taosKeeper、taosAdapter、taosX 和 taos-explorer 的版本信息 | 服务端和客户端的版本不一致 |
| 2 | **taosd 进行打开文件数**   | taosd 进行打开文件数大小 | 文件数量跟预设值不一致 |
| 3 | **数据库服务状态**   | 服务当前运行状态 | 任一服务运行状态异常 |
| 4 | **数据库服务详情**   | 服务自启动配置、启动时间、持续运行时间、内存占用空间和 CPU 使用率 | CPU 使用率超过 80% 持续时间超过 30 分钟 |
| 5 | **数据库参数配置**   | 数据库所有参数信息 | 无 |
| 6 | **数据库错误日志**   | 统计 taosd、taos 和 taosAdapter 的错误日志数量 | 有任意错误日志 |
| 7 | **数据库 dnode 信息**   | 每个 Dnode 分配的 vnodes 数量，dnode 状态，dnode 启动时间和最近一次重启时间 | dnode 存活数量小于集群 dnode 的总和 |
| 8 | **数据库 mnode 信息**   | 每个 mnode 的角色，mnode 状态，mnode 启动时间和最近一次重启时间 | mnode 存活数量小于集群 dnode 的总和 |
| 9 | **数据库 vnode 信息**   | 每个 vnode 所在 dnodeId、vgroupId、db 名称、当前状态、启动时间和 restored 状态 | 任一 vnode 的 restored 状态部位 True |
| 10 | **数据库用户信息**   | 数据库用户的相关配置和权限 | 1. Root 用户的默认密码未修改；2. 未配置监控专用的数据库用户；3. 普通数据库用户未定义 |
| 11 | **数据库权限信息**   | 数据库 Instance 的权限信息 | 1. 测点使用数超过授权数的 80%；2. 数据库授权到期时间距现在少于 90 天 |
| 12 | **数据库慢查询**   | 最近 30 天慢查询数量 | 最近 30 天有慢查询记录 |
| 13 | **taosx 数据目录**   | taosx 数据目录 | taosX 数据目录是默认系统根目录 |

### 库表巡检范围
| **No** | **巡检项目** | **详细说明** | **告警规则** |
|:-------|:------------|:-----------|:-----------|
| 1 | **库表占用空间**  | 数据库本地占用磁盘空间 | 无 |
| 2 | **库表概要统计**  | 数据库数量、超级表数量、子表数量、普通表数量、流数量、topic 数量和订阅数量。数据库本地占用磁盘空间 | 无 |
| 3 | **测点统计**  | 每个数据库已用测点数 | 测点使用数超过授权数的 80% |
| 4 | **vgroup 分布信息**   | 每个数据库的 vgroup 数量，每个 dnode 的 vgroup 数量 | 无 |
| 5 | **vgroup 详细信息**   | 每个数据库对应 vgroup 的 Leader 和 Follower 分布情况以及 vgroups 详情 | 无 |
| 6 | **vnode 详细信息**   | 每个数据库对应 vnode 的角色、FQDN、数据目录、占用磁盘空间、role_time、start_time 和 restored 状态 | 1. 目录下 SMA 或 WAL 文件占用磁盘空间超过 DATA 文件大小；2. vnode 数量大于 CPU 核数 * 2 |
| 7 | **数据库副本数**   | 每个数据库的副本数量 | 集群副本数小于 3 |
| 8 | **数据库 Schema 定义**   | 每个数据库的 Schema 定义 | 无 |
| 9 | **超级表 Schema 定义**   | 每个超级表的 Schema 定义 | 无 |
| 10 | **超级表详细信息**   | 每个超级表以及对应子表数量 | 数据库中没有任何超级表 |
| 11 | **流计算信息**   | 流 Schema 定义、流计算详情和任务详情 | 无 |
| 12 | **订阅主题信息**   | 主题 schema 定义、主题详情 | 无 |
| 13 | **订阅消费者信息**   | 消费者详情 | 无 |
| 14 | **订阅信息**   | 订阅详情 | 无 |

### Nginx 配置巡检（可选）
| **No** | **巡检项目** | **详细说明** | **告警规则** |
|:-------|:------------|:-----------|:-----------|
| 1 | **Nginx 配置**   | 各节点的 hostname 和 ip 是否正确配置到 Nginx 配置文件 | 配置文件中 FQDN 配置信息缺失或错误 | 

## 结果文件
巡检工具运行后会在工具运行用户在 taos.cfg 中配置的 logDir 目录下生成三类文件，包含了巡检报告 inspect_report.md，巡检结构化数据 inspect.json，数据库和超级表初始化文件 stable_schemas.md、各节点 taos、taosd 和 taosKeeper 对应的错误日志文件和各服务对应的配置文件。最后会将出错误日志文件以外的其他所有文件压缩为 results.zip

## 应用示例

在工具所在节点执行巡检任务
```
./taosinspect -m local
```
在集群所有节点执行巡检任务
```
./taosinspect -m ssh
```
指定配置文件并在集群所有节点执行巡检任务
```
./taosinspect -m ssh -f /path_to_file/inspect.cfg
```
在集群所有节点执行巡检任务，包括检查 nginx 服务配置文件
```
./taosinspect -m ssh -f /path_to_file/inspect.cfg -cn true
```
