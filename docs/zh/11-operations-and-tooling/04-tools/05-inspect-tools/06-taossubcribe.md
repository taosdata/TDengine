---
sidebar_label: 订阅服务功能测试工具
title: 订阅服务功能测试工具
toc_max_heading_level: 4
---

## 背景

TDengine TSDB 的订阅服务在使用过程中经常遇到消费者读取订阅消息报错的情况，该工具可协助验证已创建的订阅服务是否工作正常。

## 工具使用方法

### TSDB 本地部署模式

工具支持通过 help 参数查看支持的语法

```help
usage: taossubscribe local [-h] [--config CONFIG] [--backend] --ip IP [--show-data] [--log-level {debug,info}] [--port PORT]

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG, -f CONFIG
                        Path to config file
  --backend, -b         Run process in backend
  --ip IP, -i IP        Database IP address
  --show-data, -s       Show data in console and save to log
  --log-level {debug,info}, -l {debug,info}
                        Set log level, default: info (options: debug, info)
  --port PORT, -p PORT  Database port
```

#### 参数详细说明

- `config`：工具加载的配置文件，其具体配置方式详见 **配置文件使用说明** 章节。参数时配置文件默认路径为工具运行当前目录。
- `backend`：后台运行安装工具，默认前台运行。
- `ip`：TDengine TSDB 所在机器的对应 IP 地址。
- `show-data`：是否在 console 中打印订阅消息内容，默认打印。
- `log-level`：输出日志级别，目前支持 debug 和 info，模式为 info。
- `port`：TDengine TSDB 的 taosAdapter 服务对外开放端口，默认是 6041。

### TSDB 云部署模式

工具支持通过 help 参数查看支持的语法

```help
usage: taossubscribe cloud [-h] [--config CONFIG] [--backend] --ip IP [--show-data] [--log-level {debug,info}] --token TOKEN

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG, -f CONFIG
                        Path to config file
  --backend, -b         Run process in backend
  --ip IP, -i IP        Database IP address
  --show-data, -s       Show data in console and save to log
  --log-level {debug,info}, -l {debug,info}
                        Set log level, default: info (options: debug, info)
  --token TOKEN, -t TOKEN
                        Cloud access token
```

#### 参数详细说明

- `config`：工具加载的配置文件，其具体配置方式详见 **配置文件使用说明** 章节。参数时配置文件默认路径为工具运行当前目录。
- `backend`：后台运行安装工具，默认前台运行。
- `ip`：TDengine TSDB 云服务的对应 IP 地址。
- `show-data`：是否在 console 中打印订阅消息内容，默认打印。
- `log-level`：输出日志级别，目前支持 debug 和 info，模式为 info。
- `token`：连接 TSDB 云服务的 token 认证信息。

### 配置文件使用说明

```config
########################################################
#                                                      #
#                  Configuration                       #
#                                                      #
########################################################

[parameters]
td.connect.websocket.scheme = ws
group.id = test_group_01
client.id = test_consumer_01
enable.auto.commit = true
auto.commit.interval.ms = 1000
auto.offset.reset = earliest
msg.with.table.name = true
td.connect.user = root
td.connect.pass = taosdata1

# 已定义的Topic名称
[topics]
t1 = test_topic1
```

## 结果文件

工具运行后会生成结果文件 delivery.log，其内容包含消费订阅服务返回的所有信息

## 应用示例

消费部署在 192.168.0.1 服务器上的订阅服务

```shell
./taossubscribe local -i 192.168.0.1 -p 6041 -s
```

消费部署在 192.168.0.1 云服务上的订阅服务

```shell
./taossubscribe local -i 192.168.0.1 -t [token_character] -s
```
