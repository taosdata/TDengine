---
sidebar_label: 巡检工具
title: 巡检工具
toc_max_heading_level: 4
---

## 背景

TDengine TSDB 在运行一段时间后需要针对运行环境和 TDengine TSDB 本身的运行状态进行定期巡检，本文档旨在说明如何使用巡检工具对 TDengine TSDB 的运行环境进行自动化检查。

## 巡检工具使用方法

工具支持通过 help 参数查看支持的语法

```help
usage: taosinspect [-h] [--model {local,ssh}] [--basic] [--config CONFIG] [--result RESULT] [--backend] [--check-nginx] [--log-level {debug,info}] [--skip {all,dd,stc,none}] [--version]

Check Database deployment environment

optional arguments:
  -h, --help            show this help message and exit
  --model {local,ssh}, -m {local,ssh}
                        Connection model (options: local, ssh), default: local
  --basic, -b           Run basic inspection
  --config CONFIG, -f CONFIG
                        Full path of test config file
  --result RESULT, -r RESULT
                        Result directory. default: None
  --lookback LOOKBACK, -l LOOKBACK
                        Lookback time for load check (in days). default: 30
  --backend, -be        Run process in backend
  --check-nginx, -cn    Whether check nginx's config
  --log-level {debug,info}, -L {debug,info}
                        Set log level (options: debug, info). default: info
  --skip {all,dd,stc,none}, -s {all,dd,stc,none}
                        Skip some checks (options: all, dd, stc). default: none. [dd: data distribution, stc: subtable_count]
  --version, -v         Show version
```

### 参数详细说明

- `model`：巡检工具运行模式，分为 local 和 ssh。集群环境的多节点间支持 SSH 通信，应选择 ssh 模式，在任意节点上运行巡检工具，会依次对所有节点环境完成巡检操作。反之，节点间不支持 SSH 通信时，应选择 local 模式，仅对工具运行所在机器完成巡检操作，默认为 local 模式。
- `basic`：基础巡检模式。
- `config`：巡检工具加载的配置文件，其具体配置方式详见 **配置文件使用说明** 章节。不配置 config 参数时配置文件默认值为/etc/taos/inspect.cfg。
- `result`：巡检运行结束后结果文件和相关日志文件的存储目录，默认是用户在 taos.cfg 中配置的 logDir 对应目录。
- `lookback`：指定系统负载数据统计时间段，默认为过去 30 天
- `backend`：后台运行巡检工具，默认前台运行。
- `check-nginx`：是否检测负载均衡 nginx 的配置文件，默认不检查。
- `log-level`: 输出日志级别，目前支持 debug 和 info，默认模式为 info。
- `skip`：跳过指定的巡检步骤，避免在运行巡检工具过程中对数据库"较重"的操作对现有业务造成影响。当前支持跳过数据分布数据收集和超级表子表数据收集两项。默认不跳过任何巡检项目。
- `version`：打印工具版本信息。

### 配置文件使用说明

```config
########################################################
#                                                      #
#                  Configuration                       #
#                                                      #
########################################################

# 巡检环境 TDengine TSDB 的环境信息，支持免密登录和SSH登录两种方式，当环境配置了免密登录后不用配置 password 信息。
# 除此外还支持从 TDengine TSDB 自动获取集群信息，该模式下不需配置集群节点的 ip 和 FQDN，仅需要配置连接各节点的用户信息（免密时不用配置 password 信息）
# 配置方式1、2和3不可同时配置
[test_env]
# 配置方式1: 通过 TDengine TSDB 获取集群信息
username=root
password=123456
port=22

# 配置方式2: 节点间通过 SSH 协议访问
# firstep=192.168.0.1||fqdn=tdengine1||username=root||password=123456||port=22
# secondep=192.168.0.2||fqdn=tdengine2||username=root||password=123456||port=22
# dnode3=192.168.0.3||fqdn=tdengine3||username=root||username=123456||port=22

# 配置方式3: 节点间配置免密登录
# firstep=192.168.0.1||fqdn=tdengine1||username=root||port=22
# secondep=192.168.0.2||fqdn=tdengine2||username=root||port=22
# dnode3=192.168.0.3||fqdn=tdengine3||username=root||port=22

# TDegine 的 Restful 连接信息
[database]
username=root
password=taosdata
port=6030
rest_port=6041

# Nginx 服务所在服务器的连接信息
[nginx]
ip=192.168.0.100
username=root
password=123456
port=22

# oem 版本的版本名称，默认不使用
# [oem]
# version=prodb

# /etc/sysctl.con f中系统参数，工具会按照下面配置修改系统参数值
[sys_vars:/etc/sysctl.conf]
fs.nr_open=2147483584
fs.file-max=2147483584
net.ipv4.ip_local_port_range=10000 65534

# /etc/security/limits.conf 中系统参数，工具会按照下面配置修改系统参数值
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

# 巡检覆盖的 TDengine TSDB 服务范围
[td_services]
taosd
taos
taosadapter
taoskeeper
taosx
taos-explorer

# 可忽略的 TDengine TSDB 错误日志
[skip_error_strs]
failed to get monitor info
Table does not exist
failed to send
Fail to get table info
```

## 巡检范围

### 基础巡检

| **No** | **巡检项目** | **详细说明** |
|:-------|:------------|:-----------|
| 1 | **操作系统信息**   | 系统名称、系统版本号、内核版本、系统启动时间 |
| 2 | **CPU 信息** | CPU 型号、CPU 架构、CPU 核数 |
| 3 | **网卡信息** | FQDN 对应网卡名称和带宽数据 |
| 4 | **数据库数据目录使用情况**  | 数据目录的挂载路径，文件系统，存储类型，已用空间，可用空间和空间使用率 |
| 5 | **/etc/hosts 配置**   | hosts 中缺少集群节点 FQDN 对应的 ip-address 信息 |
| 6 | **数据库 dnode 信息**   | 每个 Dnode 分配的 vnodes 数量，dnode 状态，dnode 启动时间和最近一次重启时间 |
| 7 | **数据库 mnode 信息**   | 每个 mnode 的角色，mnode 状态，mnode 启动时间和最近一次重启时间 |
| 8 | **数据库列表**   | 列出所有已定义数据库名称，包含 log 和 audit 库 |
| 9 | **数据库建库语句**   | 数据库建库语句 |
| 10 | **超级表建表语句**   | 超级表建表语句 |
| 11 | **超级表概要统计**  | 普通列数、标签列数、列宽度、nchar 列宽度、varchar 列宽度、非字符列宽度、子表数量 |
| 12 | **超级表磁盘分布情况**   | 超级表磁盘分布情况 |

### 全量巡检

#### 告警巡查点

| **No** | **巡检项目** | **告警规则** |
|:-------|:------------|:-----------|
| 1 | **磁盘使用情况**   | 磁盘已用空间低于 15% |
| 2 | **CPU 使用情况** | CPU 使用率超过 80% 持续时间超过 30 分钟 |
| 3 | **防火墙状态**   | 防火墙服务未关闭 |
| 4 | **SELinux 服务状态**   | SElinux 服务未关闭 |
| 5 | **/etc/hosts 配置** | hosts 中缺少集群节点 FQDN 对应的 ip-address 信息 |
| 6 | **系统内核错误**   | 存在包含关键字 memory、error 或 killed 的内核错误日志 |
| 7 | **SWAPNESS 配置**   | SWAPNESS 服务未关闭或配置值大于 10 |
| 8 | **Coredump 配置** | 1. coredump 未配置；2. coredump 挂载目录为系统根目录；3. coredump 文件个数大于 0 |
| 9 | **数据库服务版本**   | taos 和 taosd 的版本不一致 |
| 10 | **数据库服务状态**   | taosd、taosAdapter、taosKeeper、taosX 或 taos-explorer 服务未启动 |
| 11 | **vnode 文件大小** | 目录下 SMA 或 WAL 文件占用磁盘空间超过 DATA 文件大小 |
| 12 | **数据库错误日志**   | taos、taosd 和 taosAdapter 的 log 中包含 ERROR 日志 |
| 13 | **vnode 数量**   | 集群节点的 vnode 数量大于 CPU 核数 * 2 |
| 14 | **数据库用户** | 1. Root 用户的默认密码未修改；2. 未配置监控专用的数据库用户；3. 普通数据库用户未定义 |
| 15 | **数据库权限**   | 1. 测点使用数超过授权数的 80%；2. 数据库授权到期时间距现在少于 90 天 |
| 16 | **慢查询**   | 最近 30 天有慢查询记录 |
| 17 | **数据库副本数**   | 集群副本数小于 3 |
| 18 | **Nginx 配置文件**   | 配置文件中 FQDN 配置信息缺失或错误 |

#### 集群资源负载数据统计

> **💡 Note**  
>
> 1. 该巡检功能支持版本：TDengine TSDB v3.3.6.25+ 或 v3.3.7.8+
> 2. 数据统计时间段按天为单位，可通过参数 --lookback 配置，默认为过去 30 天
>

| **No** | **巡检项目** | **详细说明** |
|:-------|:------------|:-----------|
| 1 | **CPU 使用率** | 指定时间段的 CPU 使用率的最大、最小、平均值，包括 taosd、taosAdapter、taos-explorer 和 system |
| 2 | **CPU 每日使用率** | 指定时间段的 每日 CPU 使用率的最大、最小、平均值，包括 taosd、taosAdapter、taos-explorer 和 system |
| 3 | **CPU 使用率分布情况** | 指定时间段的 CPU 使用率按照百分比区间的分布统计，包括 taosd、taosAdapter、taos-explorer 和 system |
| 4 | **内存使用率** | 指定时间段的内存使用率的最大、最小、平均值，包括 taosd、taosAdapter、taos-explorer 和 system |
| 5 | **内存每日使用率** | 指定时间段的每日内存使用率的最大、最小、平均值，包括 taosd、taosAdapter、taos-explorer 和 system |
| 6 | **内存使用率分布情况** | 指定时间段的内存使用率按照百分比区间的分布统计，包括 taosd、taosAdapter、taos-explorer 和 system |
| 7 | **异常状态统计** | 指定时间段的服务异常时间统计，包括 taosd、taosAdapter、taos-explorer |

#### 磁盘巡检范围

| **No** | **巡检项目** | **详细说明** |
|:-------|:------------|:-----------|
| 1 | **磁盘基本信息** | 磁盘类型和磁盘空间 |
| 2 | **磁盘挂载信息** | 通过 lsblk 查询的磁盘挂载信息 |
| 3 | **数据库数据目录使用情况** | 数据目录的挂载路径，文件系统，存储类型，已用空间，可用空间和空间使用率 |
| 4 | **数据库数据目录 Inode 情况** | 数据目录对应的 idnode 已用空间，可用空间和空间使用率 |

#### 系统巡检范围

| **No** | **巡检项目** | **详细说明** |
|:-------|:------------|:-----------|
| 1 | **操作系统信息**   | 系统名称、系统版本号、内核版本、系统启动时间 |
| 2 | **CPU 信息** | CPU 型号、CPU 架构、CPU 核数 |
| 3 | **网卡信息** | FQDN 对应网卡名称和带宽数据 |
| 4 | **系统资源统计** | 内存总空间、内存已用空间、内存使用率、SWAP 总空间、SWAP 已用空间、SWAP 使用率、硬盘总空间、硬盘已用空间、硬盘使用率 |
| 5 | **域名解析配置** | FQDN 和 IP 信息是否配置到/etc/hosts 文件 |
| 6 | **预安装软件** | 指定的原装软件是否已安装，若安装记录其版本 |
| 7 | **系统参数配置** | 检查系统参数值是否与配置文件中指定系统参数的配置一致 |
| 8 | **系统内核错误** | 收集系统内核错误日志信息 |
| 9 | **SWAPNESS 配置** | SWAPNESS 配置状态及其配置值大小 |
| 10 | **防火墙状态** | 防火墙是否关闭 |
| 11 | **SELinux 服务状态** | SELinux 服务是否关闭 |
| 12 | **Coredump 配置** | coredump 路径是否配置 |
| 13 | **Coredump 文件** | coredump 文件名称、创建时间、文件大小 |

#### 数据库巡检范围（全局）

| **No** | **巡检项目** | **详细说明** |
|:-------|:------------|:-----------|
| 1 | **数据库版本**   | taosd、taos、taosKeeper、taosAdapter、taosX 和 taos-explorer 的版本信息 |
| 2 | **taosd 进行打开文件数**   | taosd 进行打开文件数大小 |
| 3 | **数据库服务状态**   | 服务当前运行状态 |
| 4 | **数据库服务详情**   | 服务自启动配置、启动时间、持续运行时间、内存占用空间和 CPU 使用率 |
| 5 | **数据库参数配置**   | 数据库所有参数信息 |
| 6 | **数据库错误日志**   | 统计 taosd、taos 和 taosAdapter 的错误日志数量 |
| 7 | **数据库 dnode 信息**   | 每个 Dnode 分配的 vnodes 数量，dnode 状态，dnode 启动时间和最近一次重启时间 |
| 8 | **数据库 mnode 信息**   | 每个 mnode 的角色，mnode 状态，mnode 启动时间和最近一次重启时间 |
| 9 | **数据库 vnode 信息**   | 每个 vnode 所在 dnodeId、vgroupId、db 名称、当前状态、启动时间和 restored 状态 |
| 10 | **数据库用户信息**   | 数据库用户的相关配置和权限 |
| 11 | **数据库权限信息**   | 数据库 Instance 的权限信息 |
| 12 | **数据库慢查询**   | 最近 30 天慢查询数量 |
| 13 | **taosx 数据目录**   | taosx 数据目录 |

#### 数据库巡检范围（每个数据库）

| **No** | **巡检项目** | **详细说明** |
|:-------|:------------|:-----------|
| 1 | **数据库版本**  | 数据库各服务的版本信息 |
| 2 | **数据库逻辑单元信息**  | dnode、mnode 和 vnode 相关数据 |
| 3 | **数据库列表信息**  | 列出所有已定义数据库名称，包含 log 和 audit 库 |
| 4 | **服务端打开文件数配置**  | Soft Limit 和 Hard Limit 的当前配置 |
| 5 | **数据库服务详细信息**  | 数据库各服务的详细信息，包括自启动配置、运行状态、启动时间、运行时间、内存占用空间和 CPU 使用率 |
| 6 | **数据库参数信息**  | 各节点对应的数据库参数信息 |
| 7 | **数据库错误日志信息**  | 统计 taos、taosd 和 taosAdapter 的错误日志数量 |
| 8 | **数据库用户信息**  | 数据库用户及其权限配置信息 |
| 9 | **数据库授权信息**  | 数据库授权信息 |
| 10 | **测点数统计**  | 基于数据库的测点使用量 |
| 11 | **vgroup 按库分布情况**  | 基于数据库的 vgroup 分布情况 |
| 12 | **vgroup leader 按节点分布情况**  | 基于 dnode 节点的 vgroup leader 分布情况 |
| 13 | **vnodes 按节点分布情况**  | 基于 dnode 节点的 vnode 分布情况 |
| 14 | **各库在各节点上的分布情况**  | 每个数据库基于 dnode 节点的 vnode 分布情况 |
| 15 | **基于单数据库的分布情况**  | 基于单数据库的 vnode 和 vgroup 详情 |
| 16 | **数据库副本数统计信息**  | 数据库副本数统计信息 |
| 17 | **慢查询信息**  | 过去 30 天的慢查询记录 |
| 18 | **库表占用空间**  | 数据库本地占用磁盘空间 |

#### 数据表巡检范围

| **No** | **巡检项目** | **详细说明** |
|:-------|:------------|:-----------|
| 1 | **概要统计**  | dataDir 占用总空间、数据库数量、超级表数量、子表数量、普通表数量、流数量、Topic 数量、订阅数量 |
| 2 | **超级表所在数据库定义**  | 超级表所在数据库的建库语句 |
| 3 | **超级表概要统计**  | 普通列数、标签列数、列宽度、nchar 列宽度、varchar 列宽度、非字符列宽度、子表数量 |
| 4 | **超级表详细信息**  | 超级表建表语句、超级表磁盘分布情况 |
| 5 | **普通表概要统计**  | 普通列数、列宽度、nchar 列宽度、varchar 列宽度、非字符列宽度 |
| 6 | **普通表详细信息**  | 普通表建表语句、普通表磁盘分布情况 |

#### 流计算巡检范围

| **No** | **巡检项目** | **详细说明** |
|:-------|:------------|:-----------|
| 1 | **流定义**  | 建流语句 |
| 2 | **流详细信息**  | 流的详细信息，包括 create_time、stream_id、status 等信息|
| 3 | **流任务信息**  | 运行中的流详细信息 |

#### 订阅巡检范围

| **No** | **巡检项目** | **详细说明** |
|:-------|:------------|:-----------|
| 1 | **Topic 定义**  | Topic 的创建语句 |
| 2 | **Topic 明细**  | 包括 topic_name、db_name、create_time 等信息 |
| 3 | **Consumers 信息**  | 包括 consumer_id、consumer_group、client_id 等信息 |
| 3 | **subscriptions 信息**  | 包括 topic_name、consumer_group、vgroup_id 等信息|

#### Nginx 配置巡检（可选）

| **No** | **巡检项目** | **详细说明** |
|:-------|:------------|:-----------|
| 1 | **Nginx 配置**   | 各节点的 hostname 和 ip 是否正确配置到 Nginx 配置文件 |

## 结果文件

巡检工具运行后会在 taos.cfg 中配置的 logDir 目录或指定目录下生成如下文件

| **No** | **文件名称** | **文件功能详细说明** |
|:-------|:------------|:-----------|
| 1 | **inspect_report.md**  | 巡检报告文件 |
| 2 | **inspect.json**  | 巡检结果结构化数据 |
| 3 | **table_schemas.md**  | 存储超级表 schema 文件，仅当超级表 schema 信息长度查过 2000 字符时存在此文件 |
| 4 | **xxxx_error.log**  | 各节点 taos、taosd、taosAdapter 和 taosKeeper 对应的错误日志文件 |
| 5 | **xxxx.cfg(or toml)**  | 各节点 taos、taosd、taosAdapter 和 taosKeeper 对应的配置文件 |
| 6 | **results.zip**  | 最后会将出去错误日志文件以外的其它所有文件压缩为 results.zip |

## 应用示例

针对工具所在节点执行巡检任务

```config
./taosinspect -m local 或 ./taosinspect
```

针对集群所有节点执行巡检任务

```config
./taosinspect -m ssh
```

指定配置文件并在集群所有节点执行巡检任务

```config
./taosinspect -m ssh -f /path_to_file/inspect.cfg
```

针对工具所在节点执行基础巡检任务

```config
./taosinspect -m local -b 或 ./taosinspect -b
```

针对集群所有节点执行基础巡检任务

```config
./taosinspect -m ssh -b
```

跳过数据收集巡检项目

```config
./taosinspect -m ssh -s dd
```

跳过超级表子表数量统计巡检项目

```config
./taosinspect -m ssh -s stc
```

跳过数据收集和超级表子表数量统计巡检项目

```config
./taosinspect -m ssh -s all
```

在集群所有节点执行巡检任务，包括检查 nginx 服务配置文件

```config
./taosinspect -m ssh -f /path_to_file/inspect.cfg -cn true
```
