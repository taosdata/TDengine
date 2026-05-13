# The dockerization for the TaosX Test

## 1. Objectives

- 主要测试内容：
  - 生成 docker 镜像，验证在全新的环境中按照 spec 中说明的步骤准备环境和生成镜像
  - 使用 docker 镜像, 模拟搭建单机环境、集群环境，并在单机和集群环境中分别验证基本的数据库功能以及数据接入功能。

## 2. Revision History

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.01.03 | 0.1 | @贾晨阳 |  |
| 2024.01.08 | 0.2 | @贾晨阳 |  |

## 3. Scope

- 将生成镜像（192.168.2.14）和运行容器（192.168.2.10）的环境分离
- 验证docker build、docker run、以及 data-in 功能的正确性
- 分别在 Ubuntu 和 CentOS 下验证 docker 容器运行的正确性
- Data in 功能验证的数据源环境参考 [Data Source Env Setup (cn)](https://taosdata.feishu.cn/wiki/I5hHw8KcpiGib1kszEAcTUiVnWd) 
- 涉及到 TDengine 和 taosX 本身的 sanity 功能验证，均通过 explorer 的 UI 操作进行
- 通过安装包生成镜像时，采用企业版3.2.2.0版本安装包

## 4. Limitations and Known Issues

windows 上 docker 的验证因为环境配置问题本次测试中暂未进行。

## 5. 测试结论

本次测试中，分别在Ubuntu、centOS下验证taosx server docker、taosx-agent docker、taosx-intergrate docker三种镜像的安装、部署、基本功能正确性。其中：
1. 对于 taosx-server docker，验证了安装部署后可正常连接外部 TDengine 和 taosx-agent 并正确执行数据接入任务功能
2. 对于 taosx-agent docker，验证了安装部署后可正常连接外部 taosx 并正确执行数据接入任务功能
3. 对于 taosx-intergrate docker，验证了单机一键部署三节点集群功能，验证了集群可正常运行、正常执行写入、查询、建流、建数据订阅的功能

## 6. Environment

- 容器运行环境：Linux（Ubuntu 20.04 <192.168.2.10>、centOS 7 <192.168.2.15>、windows 10 <192.168.1.66>）
- 生成镜像环境：Ubuntu 20.04 <192.168.2.14>
- Browser: Chrome
- 目前已经在公司内部通过 harbor 搭建了一个docker仓库（访问地址：http://192.168.1.40:5000/harbor/projects），用于存储每次发版时生成的三种镜像，可供测试和快速验证使用；镜像生成及推送由jenkins job完成，jenkins job 的编写和调试工作会在本次测试完成后进行。

## 7. Test Data

N/A

## 8. Test Cases

### 8.1 Functional

| Type | Description | Is sanity case？ | Environment | Expected Results | Result | Memo |
| --- | --- | --- | --- | --- | --- | --- |
| 创建镜像 | build taosx serve 镜像（源码） | Y | —— |  | Pass | [https://jira.taosdata.com:18080/browse/TD-28192](https://jira.taosdata.com:18080/browse/TD-28192) |
|  | build taosx agent 镜像（源码） | Y |  |  | Pass |  |
|  | build taosx serve 镜像（安装包） |  |  |  | Pass | 拉取安装包的路径错误 |
|  | build taosx agent 镜像（安装包） |  |  |  | Pass |  |
|  | build taosx intergrate 镜像（安装包） | Y |  |  | Pass |  |
| taosX server docker | 1. 加载docker镜像
1. 创建容器，通过宿主机explorer访问docker中的taosx |  | Ubuntu 20.04 | 成功加载镜像 | Pass |  |
|  |  |  |  | 成功启动运行容器，访问正常 | Pass |  |
|  |  |  | CentOS 7 | 成功加载镜像 | Pass |  |
|  |  |  |  | 成功启动运行容器，访问正常 | Pass |  |
|  |  |  | Windows 10 | 成功加载镜像 |  |  |
|  |  |  |  | 成功启动运行容器，访问正常 |  |  |
| data-in for taosX server docker | TMQ |  | —— | 任务可正常执行 | Pass |  |
|  | Legacy |  |  | 任务可正常执行 | Pass |  |
|  | OPCUA |  |  | 任务可正常执行 | Pass |  |
|  | PI，连接外部windows下agent |  |  | 任务可正常执行 | Pass |  |
| taosX-agent docker | 1. 加载docker镜像
1. 创建容器 |  | Ubuntu 20.04 | 成功加载镜像 | Pass |  |
|  |  |  |  | 成功启动运行容器 | Pass |  |
|  |  |  | CentOS 7 | 成功加载镜像 | Pass |  |
|  |  |  |  | 成功启动运行容器 | Pass |  |
|  |  |  | Windows 10 | 成功加载镜像 |  |  |
|  |  |  |  | 成功启动运行容器 |  |  |
| data-in for taosX-agent docker | OPCUA |  |  | 任务可正常执行 | Pass |  |
| taosX-intergrate docker | 1. 加载docker镜像
1. 创建容器，创建单节点TDengine环境 |  | Ubuntu 20.04 | 成功加载镜像 | Pass |  |
|  |  |  |  | 成功启动容器，可正常访问explorer、taosx、taosd | Pass |  |
|  |  |  | CentOS 7 | 成功加载镜像 | Pass |  |
|  |  |  |  | 成功启动容器，可正常访问explorer、taosx、taosd | Pass |  |
|  |  |  | Windows 10 | 成功加载镜像 |  |  |
|  |  |  |  | 成功启动容器，可正常访问explorer、taosx、taosd |  |  |
| 集群搭建及sanity功能 | 在同一个宿主机配置并启动多个容器，运行后搭建多节点集群 | Y | Ubuntu 20.04 | 集群可正常运行 | Pass |  |
|  |  |  | CentOS 7 | 集群可正常运行 | Pass |  |
|  |  |  | Windows 10 | 集群可正常运行 | Pass |  |
|  | 通过多机运行容器搭建多节点集群 |  | —— | 集群可正常运行 |  |  |
|  | 在集群中进行基本的数据写入（3副本） |  | Ubuntu 20.04 | 在explorer可正常操作并响应 | Pass |  |
|  |  |  | CentOS 7 | 在explorer可正常操作并响应 |  |  |
|  |  |  | Windows 10 | 在explorer可正常操作并响应 |  |  |
|  | 在集群中对写入的数据进行查询 |  | Ubuntu 20.04 | 在explorer可正常操作并响应 | Pass |  |
|  |  |  | CentOS 7 | 在explorer可正常操作并响应 | Pass |  |
|  |  |  | Windows 10 | 在explorer可正常操作并响应 | Pass |  |
|  | 在集群中创建stream |  | Ubuntu 20.04 | 在explorer可正常操作并响应 | Pass |  |
|  |  |  | CentOS 7 | 在explorer可正常操作并响应 | Pass |  |
|  |  |  | Windows 10 | 在explorer可正常操作并响应 |  |  |
|  | 在集群中创建订阅并进行消费 |  | Ubuntu 20.04 | 在explorer可正常操作并响应 | Pass |  |
|  |  |  | CentOS 7 | 在explorer可正常操作并响应 | Pass |  |
|  |  |  | Windows 10 | 在explorer可正常操作并响应 |  |  |

### 8.2 Usability

暂无。

### 8.3 Reliability

暂无。

### 8.4 Performance

暂无

### 8.5 Security

暂无。

### 8.6 Compatibility

在不同的linux发行版（Ubuntu、centOS）和 windows 下安装运行镜像，应当都能够正常运行。

### 8.7 Localization

暂无

## 9. Questions

这里用于记录在Review Metting上需要讨论的问题：
- 

## 10. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: docker

TD-28192


TD-28246


## 11. Schedule

这里用于计划此feature测试的开始和结束时间。

## 12. Notes

- Taosx-integrated 镜像使用方式：
```shell {wrap}
docker run  -d -p 16050:6050 -p 16055:6055 -p 16060:6060 
-v /data/cyjia/taos:/etc/taos 
-v /data/cyjia/taos/var/lib:/var/lib 
-v /data/cyjia/taos/var/log:/var/log 
-v /data/cyjia/taos/var/corefile:/corefile
-v /data/cyjia/taos/taosx:/data/taosx 
image.cloud.taosdata.com/taosx/integrated:3.2.2.0

## 13. 容器内部 6050 映射到外部 16050 端口

## 14. 容器内部 6055 映射到外部 16055 端口

## 15. 容器内部 6060 映射到外部 16060 端口

## 16. 容器内部 TDengine 配置文件路径 /etc/taos 映射到外部 /data/cyjia/taos

## 17. 容器内部 TDengine 数据文件路径 /var/lib 映射到外部 /data/cyjia/taos/var/lib

## 18. 容器内部 TDengine 日志文件路径 /var/log 映射到外部 /data/cyjia/taos/var/log

## 19. 容器内部 TDengine core文件路径 /corefile 映射到外部 /data/cyjia/taos/var/corefile

## 20. 容器内部 taosx 数据、任务、日志、配置文件目录 /data/taosx 映射到 /data/cyjia/taos/taosx

```

- Taosx-agent 镜像使用方式：
```shell {wrap}
docker run  -d
-v /data/cyjia/taos/taosx:/data/taosx 
image.cloud.taosdata.com/taosx/agent:3.2.2.0

## 21. 容器内部 agent 数据、任务、日志、配置文件目录 /data/taosx 映射到 /data/cyjia/taos/taosx

```

- Taosx-serve 镜像使用方式：
```shell {wrap}
docker run  -d -p 16050:6050 -p 16055:6055
-v /data/cyjia/taos/taosx:/data/taosx
```

## 22. Summary

## 23. Reference

[The dockerization for taosX](https://taosdata.feishu.cn/wiki/MgeBwCX4mivAOMkdKL1cUpMen7g)
