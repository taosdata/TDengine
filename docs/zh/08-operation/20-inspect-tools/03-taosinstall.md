---
sidebar_label: 安装工具
title: 安装工具
toc_max_heading_level: 4
---

## 背景

TDengine 的安装包自带安装脚本，但无法基于集群进行自动化安装部署，本文档旨在说明如何使用安装工具进行 TDengine 的集群式安装部署。 

## 安装工具支持功能
| **安装方式** | **详细说明** |
|:--|:----------|
| **单节点安装部署**   | 单节点环境安装部署 TDengine |
| **集群安装部署** | 集群环境安装部署 TDengine |
| **指定机器安装部署**   | 指定集群中特定节点安装部署 TDengine |
| **普通升级**   | 单节点或集群关闭服务后升级 TDengine，**仅推荐测试换使用** |   
| **滚动升级**   | 单节点或集群不停服务升级 TDengine，**仅推荐测试换使用** | 

## 安装工具使用方法

工具支持通过 help 参数查看支持的语法

```help
Usage: taosinstall [OPTIONS]

  Install Database

Options:
  -m, --model [local|ssh]  connection model, default: local
  -f, --config TEXT        Full path of test config file  [required]
  -b, --backend BOOLEAN    Run process in backend. default: False
  -w, --workers INTEGER    concurrency, default is 50
  -l, --list TEXT          list of test hostnames, Separate with commas. None
                           by default
  -u, --upgrade            Upgrade Database
  -ru, --rolling-upgrade   Rolling upgrade Database
  -v, --version            Show version
  --help                   Show this message and exit.
```

### 参数详细说明

- `model`：安装工具运行模式，分为 local 和 ssh。安装环境的多节点间支持 SSH 通信，可选择 ssh 模式，在任意节点上运行安装工具，会依次对所有节点环境完成安装操作。反之，节点间不支持 SSH 通信时，可选择 local 模式，仅对工具运行所在机器完成安装操作，默认为 local 模式。
- `config`：安装工具加载的配置文件，其具体配置方式详见 **配置文件使用说明** 章节。不配置 config 参数时配置文件默认路径为工具运行当前目录。
- `backend`：后台运行安装工具，选择 True 后安装工具在自动在后台运行，默认为 False。
- `workers`：集群安装部署时的并发数量，会影响同时向多节点服务文件的并发数，需根据机器资源情况调整，默认是 50。
- `list`：指定部署 TDengine 的机器，前提是配置文件中指定的 firstep 安装完成并服务运行部正常，该参数是预留给安装中断后继续安装剩余节点的场景使用，默认值为 None。
- `upgrade`：普通升级，目前仅推荐测试环境使用。
- `rolling-upgrade`：滚动升级，目前仅推荐测试环境使用。
- `version`：打印安装工具版本信息。

### 配置文件使用说明

```config
########################################################
#                                                      #
#                  Configuration                       #
#                                                      #
########################################################

# 安装部署 TDengine 的环境信息，支持免密登录和 SSH 登录两种方式，当环境配置了免密登录后可不用配置 password 信息
[test_env]
# 节点间通过 SSH 协议访问
firstep=192.168.0.1||fqdn=tdengine1||username=root||password=123456||port=22
secondep=192.168.0.2||fqdn=tdengine2||username=root||password=123456||port=22
dnode3=192.168.0.3||fqdn=tdengine3||username=root||username=123456||port=22

# 节点间配置免密登录
# firstep=192.168.0.1||fqdn=tdengine1||username=root||port=22
# secondep=192.168.0.2||fqdn=tdengine2||username=root||port=22
# dnode3=192.168.0.3||fqdn=tdengine3||username=root||port=22

# TDengine 安装包在本地所在全路径
[local_pack]
3.3.4.10=/path_to_file/TDengine-enterprise-3.3.4.10-Linux-x64.tar.gz

# 复制 TDengine 安装包到远程机器的目录
[remote_pack]
dir=/tmp

# oem 版本的版本名称，默认不使用
# [oem]
# version=prodb

# TDegine 的 Restful 连接信息
[database]
username=root
password=taosdata
port=6030
rest_port=6041

# taosd 预配置文件, 该文件中配置会覆盖到所有 dnode 上对应配置文件
[taos_cfg]
cfg_file=taos.cfg

# taoskeeper的预配置文件, 该文件中配置会覆盖到所有 dnode 上对应配置文件
[taoskeeper_cfg]
cfg_file=taoskeeper.toml

# taosadapter 的预配置文件, 该文件中配置会覆盖到所有 dnode 上对应配置文件
[taosadapter_cfg]
cfg_file=taosadapter.toml

# taosx 的预配置文件, 该文件中配置会覆盖到所有 dnode 上对应配置文件
[taosx_cfg]
cfg_file=taosx.toml

# explorer 的预配置文件, 该文件中配置会覆盖到所有 dnode 上对应配置文件
[taosexplorer_cfg]
cfg_file=explorer.toml

# 监控用户 monitor 的配置信息
[monitor_user]
username=monitor
password=Taosmonitor_125#
```
## 安装流程

| **No** | **安装步骤** | **详细说明** |
|:-------|:------------|:-----------|
| 1 | **复制安装包**   | 复制安装包到集群个节点（local 安装模式跳过该步骤） |
| 2 | **安装 TDengine** | 安装 TDengine |
| 3 | **更新 taos 配置**   | 基于预配置的 taosd 参数更新 taos.cfg，除了预配置的静态参数，还动态更新 firstEp、secondEp、fqdn、minReservedMemorySize |
| 4 | **启动 taosd 服务**   | 通过 sytstemctl 启动 taosd 服务 |   
| 5 | **更新 taosadapter 配置**   | 基于预配置的 taosadapter 参数更新 taosadapter.toml | 
| 6 | **启动 taosadapter 服务**   | 通过 sytstemctl 启动 taosadapter 服务 | 
| 7 | **创建集群所有 dnode**   | 数据库初始化 dnode | 
| 8 | **创建 mnode**   | 在 firstEp、secondEp 和 node3 上创建 monde（local 安装模式跳过该步骤） | 
| 9 | **更新 taosadapter 的 instanceId**   | 更新 taosadapter 的 instanceId 并重启 taosadapter 服务 | 
| 10| **更新 taoskeeper 配置**   | 基于预配置的 taoskeeper 参数更新 taoskeeper.toml 并更新 instanceId | 
| 11| **启动 taoskeeper 服务**   | 通过 sytstemctl 启动 taoskeeper 服务 |
| 12| **更新 taosx 配置**   | 基于预配置的 taosx 参数更新 taosx.toml 并更新 instanceId | 
| 13| **启动 taosx 服务**   | 通过 sytstemctl 启动 taosx 服务 |
| 14| **更新 taos-explorer 配置**   | 基于预配置的 taos-explorer 参数更新 explorer.toml 并更新 instanceId | 
| 15| **启动 taos-explorer 服务**   | 通过 sytstemctl 启动 taos-explorer 服务 |
| 16| **创建监控用户**   | 数据库创建 monitor 用户 |
| 17| **更新 taoskeeper 配置**   | 更新 taoskeeper 配置文件中连接数据库的用户为 monitor |
| 18| **启动 taoskeeper 服务**   | 通过 sytstemctl 启动 taoskeeper 服务 |

## 升级流程
### 停服升级
停服升级会先停止所有节点的所有数据库服务，然后按照 firstEp、secondEp、dnode3...的顺序依次进行升级和重启服务操作
| **No** | **安装步骤** | **详细说明** |
|:-------|:------------|:-----------|
| 1 | **复制安装包**   | 复制安装包到集群个节点（local 安装模式跳过该步骤） |
| 2 | **停止服务** | 停止 taosd、taosadapter、taoskeeper、taosx 和 taos-explorer 服务 |
| 3 | **更新版本**   | 更新 TDengine 到指定版本 |
| 4 | **启动 taosd 服务**   | 通过 sytstemctl 启动 taosd 服务 |   
| 5 | **启动 taosadapter 服务**   | 通过 sytstemctl 启动 taosadapter 服务 | 
| 6 | **启动 taoskeeper 服务**   | 通过 sytstemctl 启动 taoskeeper 服务 |
| 7 | **启动 taosx 服务**   | 通过 sytstemctl 启动 taosx 服务 |
| 8 | **启动 taos-explorer 服务**   | 通过 sytstemctl 启动 taos-explorer 服务 |

### 滚动升级
按照非 monde 所在节点、mnode 为 follower 节点和 monde 为 leader 节点的顺序依次进行升级和重启服务操作
| **No** | **安装步骤** | **详细说明** |
|:-------|:------------|:-----------|
| 1 | **复制安装包**   | 复制安装包到集群个节点（local 安装模式跳过该步骤） |
| 2 | **停止服务** | 停止 taosd、taosadapter、taoskeeper、taosx 和 taos-explorer 服务 |
| 3 | **更新版本**   | 更新 TDengine 到指定版本 |
| 4 | **启动 taosd 服务**   | 通过 sytstemctl 启动 taosd 服务 |   
| 5 | **启动 taosadapter 服务**   | 通过 sytstemctl 启动 taosadapter 服务 | 
| 6 | **启动 taoskeeper 服务**   | 通过 sytstemctl 启动 taoskeeper 服务 |
| 7 | **启动 taosx 服务**   | 通过 sytstemctl 启动 taosx 服务 |
| 8 | **启动 taos-explorer 服务**   | 通过 sytstemctl 启动 taos-explorer 服务 |

## 应用示例

在工具所在节点安装数据库
```
./taosinstall -m local
```
在集群所有节点安装数据库
```
./taosinstall -m ssh
```
指定配置文件并在集群所有节点安装数据库
```
./taosinstall -m ssh -f /path_to_file/install.cfg
```
在集群指定节点安装数据库
```
./taosinstall -m ssh -l server1,server2...
```
停服升级数据库
```
./taosinstall -m ssh -u
```
滚动升级数据库
```
./taosinstall -m ssh -ru
```