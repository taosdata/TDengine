---
sidebar_label: 巡检工具
title: 巡检工具
toc_max_heading_level: 4
---

## 背景

TDengine在运行一段时间后需要针对运行环境和TDengine本身的运行状态进行定期巡检，本文档旨在说明如何使用巡检工具对TDengine的运行环境进行自动化检查。 

## 安装工具使用方法

工具支持通过help参数查看支持的语法

```help
Usage: taosinspect [OPTIONS]

  Check Database deployment environment

Options:
  -m, --model [local|ssh]     connection model, default: local
  -f, --config TEXT           Full path of test config file  [required]
  -r, --result TEXT           Full path of result directory  [required]
  -b, --backend BOOLEAN       Run process in backend. default: False
  -cn, --check-nginx BOOLEAN  Whether check nginx's config, default: False
  -v, --version               Show version
  --help                      Show this message and exit.
```

### 参数详细说明

- `model`：安装工具运行模式，分为local和ssh。安装环境的多节点间支持SSH通信，可选择ssh模式，在任意节点上运行安装工具，会依次对所有节点环境完成安装操作。反之，节点间不支持SSH通信时，可选择local模式，仅对工具运行所在机器完成安装操作，默认为local模式。
- `config`：安装工具加载的配置文件，其具体配置方式详见 **配置文件使用说明** 章节。不配置config参数时配置文件默认值为/etc/taos/inspect.cfg。
- `result`：巡检运行结束后结果文件和相关日志文件的存储目录，默认是用户在taos.cfg中配置的logDir对应目录。
- `backend`：后台运行安装工具，选择True后安装工具在自动在后台运行，默认为False。
- `check-nginx`：是否检测负载均衡nginx的配置文件，默认值为False。
- `version`：打印安装工具版本信息。

### 配置文件使用说明

```config
########################################################
#                                                      #
#                  Configuration                       #
#                                                      #
########################################################

# 安装部署TDengine的环境信息，支持免密登录和SSH登录两种方式，当环境配置了免密登录后不用配置password信息。除此外还支持从TDengine自动获取集群信息，该模式下不需配置集群几点的ip和FQDN，仅需要配置连接各节点的用户信息（免密时不用配置password信息）
[test_env]
# 通过TDengine获取集群信息
username=root
password=123456
port=22

# 节点间通过SSH协议访问
# firstep=192.168.0.1||fqdn=tdengine1||username=root||password=123456||port=22
# secondep=192.168.0.2||fqdn=tdengine2||username=root||password=123456||port=22
# dnode3=192.168.0.3||fqdn=tdengine3||username=root||username=123456||port=22

# 节点间配置免密登录
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
app1=screen
app2=tmux
app3=gdb
app4=fio
app5=iperf,iperf3
app6=sysstat
app7=net-tools 
app8=jansson
app9=snappy
app10=ntp,chrony
app11=tree
app12=wget

# 巡检覆盖的TDengine服务范围
[td_services]
ts1=taosd
ts2=taosadapter
ts3=taoskeeper
ts4=taosx
ts5=taos-explorer

# 可忽略的TDengine错误日志
[skip_error_strs]
str1=failed to get monitor info
str2=Table does not exist
str3=failed to send
str4=Fail to get table info
```
## 巡检范围
### 磁盘巡检范围
| **No** | **巡检项目** | **详细说明** | **告警规则** |
|:-------|:------------|:-----------|:-----------|
| 1 | **磁盘基本信息**   | 磁盘类型和磁盘空间 | 无 | 磁盘已用空间低于15% |
| 2 | **磁盘挂载信息** | 通过lsblk查询的磁盘挂载信息 | 无 |
| 3 | **数据库数据目录使用情况**   | 数据目录的挂载路径，文件系统，存储类型，已用空间，可用空间和空间使用率 | 磁盘已用空间低于15% |
| 4 | **数据库数据目录Inode情况**   | 数据目录对应的idnode已用空间，可用空间和空间使用率 | 无 |

### 系统巡检范围
| **No** | **巡检项目** | **详细说明** | **告警规则** |
|:-------|:------------|:-----------|:-----------|
| 1 | **系统基本信息**   | 系统名称、系统启动时间、防火墙和SELinux服务状态 | 防火墙或SElinux服务未关闭 |
| 2 | **域名解析配置** | FQDN和IP信息是否配置到/etc/hosts文件 | 缺少任一FQDN的域名解析 |
| 3 | **预安装软件** | 指定的原装软件是否已安装，若安装记录其版本 | 无 |
| 4 | **系统参数配置** | 检查系统参数值是否与配置文件中指定系统参数的配置一致 | 无 |
| 5 | **系统内存错误** | 收集系统内核错误日志信息 | 存在内核错误日志 |
| 6 | **SWAPNESS配置** | SWAPNESS配置状态及其配置值大小 | SWAPNESS配置值大于10 |
| 7 | **Coredump配置** | coredump路径是否配置 | 1. coredump未配置；2. coredump挂载目录为系统根目录； 3. coredump文件个数大于0 |

### 数据库巡检范围
| **No** | **巡检项目** | **详细说明** | **告警规则** |
|:-------|:------------|:-----------|:-----------|
| 1 | **数据库版本**   | taosd、taos、taosKeeper、taosAdapter、taosX和taos-explorer的版本信息 | 服务端和客户端的版本不一致 |
| 2 | **taosd进行打开文件数**   | taosd进行打开文件数大小 | 文件数量跟预设值不一致 |
| 3 | **数据库服务状态**   | 服务当前运行状态 | 任一服务运行状态异常 |
| 4 | **数据库服务详情**   | 服务自启动配置、启动时间、持续运行时间、内存占用空间和CPU使用率 | CPU使用率超过80%持续时间超过30分钟 |
| 5 | **数据库参数配置**   | 数据库所有参数信息 | 无 |
| 6 | **数据库错误日志**   | 统计taosd、taos和taosAdapter的错误日志数量 | 有任意错误日志 |
| 7 | **数据库dnode信息**   | 每个Dnode分配的vnodes数量，dnode状态，dnode启动时间和最近一次重启时间 | dnode存活数量小于集群dnode的总和 |
| 8 | **数据库mnode信息**   | 每个mnode的角色，mnode状态，mnode启动时间和最近一次重启时间 | mnode存活数量小于集群dnode的总和 |
| 9 | **数据库vnode信息**   | 每个vnode所在dnodeId、vgroupId、db名称、当前状态、启动时间和restored状态 | 任一vnode的restored状态部位True |
| 10 | **数据库用户信息**   | 数据库用户的相关配置和权限 | 1. Root用户的默认密码未修改； 2. 未配置监控专用的数据库用户； 3. 普通数据库用户未定义 |
| 11 | **数据库权限信息**   | 数据库Instance的权限信息 | 1. 测点使用数超过授权数的80%； 2. 数据库授权到期时间距现在少于90天 |
| 12 | **数据库慢查询**   | 最近30天慢查询数量 | 最近30天有慢查询记录 |
| 13 | **taosx数据目录**   | taosx数据目录 | taosX数据目录是默认系统根目录 |

### 库表巡检范围
| **No** | **巡检项目** | **详细说明** | **告警规则** |
|:-------|:------------|:-----------|:-----------|
| 1 | **库表占用空间**  | 数据库本地占用磁盘空间 | 无 |
| 2 | **库表概要统计**  | 数据库数量、超级表数量、子表数量、普通表数量、流数量、topic数量和订阅数量。数据库本地占用磁盘空间 | 无 |
| 3 | **测点统计**  | 每个数据库已用测点数 | 测点使用数超过授权数的80% |
| 4 | **vgroup分布信息**   | 每个数据库的vgroup数量，每个dnode的vgroup数量 | 无 |
| 5 | **vgroup详细信息**   | 每个数据库对应vgroup的Leader和Follower分布情况以及vgroups详情 | 无 |
| 6 | **vnode详细信息**   | 每个数据库对应vnode的角色、FQDN、数据目录、占用磁盘空间、role_time、start_time和restored状态 | 1. 目录下SMA或WAL文件占用磁盘空间超过DATA文件大小；2. vnode数量大于CPU核数 * 2 |
| 7 | **数据库副本数**   | 每个数据库的副本数量 | 集群副本数小于3 |
| 8 | **数据库Schema定义**   | 每个数据库的Schema定义 | 无 |
| 9 | **超级表Schema定义**   | 每个超级表的Schema定义 | 无 |
| 10 | **超级表详细信息**   | 每个超级表以及对应子表数量 | 数据库中没有任何超级表 |
| 11 | **流计算信息**   | 流Schema定义、流计算详情和任务详情 | 无 |
| 12 | **订阅主题信息**   | 主题schema定义、主题详情 | 无 |
| 13 | **订阅消费者信息**   | 消费者详情 | 无 |
| 14 | **订阅信息**   | 订阅详情 | 无 |


### Nginx配置巡检（可选）
| **No** | **巡检项目** | **详细说明** | **告警规则** |
|:-------|:------------|:-----------|:-----------|
| 1 | **Nginx配置**   | 各节点的hostanme和ip是否正确配置到Nginx配置文件 | 配置文件中FQDN配置信息缺失或错误 | 


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
./taosinspect -m ssh -f /path_to_file/install.cfg
```
在集群所有节点执行巡检任务，包括检查nginx服务配置文件
```
./taosinspect -m ssh -f /path_to_file/install.cfg -cn true
```