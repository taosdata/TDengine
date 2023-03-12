---
sidebar_label: Telegraf
title: Telegraf 写入
description: 使用 Telegraf 向 TDengine 写入数据
---

Telegraf 是一款十分流行的指标采集开源软件。在数据采集和平台监控系统中，Telegraf 可以采集多种组件的运行信息，而不需要自己手写脚本定时采集，降低数据获取的难度。

只需要将 Telegraf 的输出配置增加指向 taosAdapter 对应的 url 并修改若干配置项即可将 Telegraf 的数据写入到 TDengine 中。将 Telegraf 的数据存在到 TDengine 中可以充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。

## 前置条件

要将 Telegraf 数据写入 TDengine Cloud ，需要首先手动创建一个数据库。登录到 TDengine Cloud ，在左边的菜单点击”数据浏览器“，然后再点击”数据库“标签旁边的”+“按钮添加一个名称是”telegraf“使用默认参数的数据库。

## 安装 Telegraf

假设您使用的是 Ubuntu 操作系统：

```bash
{{#include docs/examples/thirdparty/install-telegraf.sh:null:nrc}}
```

安装结束以后，Telegraf 服务应该已经启动。请先停止它：

```bash
sudo systemctl stop telegraf
```

## 配置环境变量

在您的终端命令行里面执行下面的命令来保存 TDengine Cloud 的令牌和URL为环境变量：

```bash
export TDENGINE_CLOUD_URL="<url>"
export TDENGINE_CLOUD_TOKEN="<token>"
```

<!-- exclude -->
您可以使用真实的 TDengine Cloud 的URL和令牌来替换上面的`<url>`和`<token>`。可以通过访问[TDengine Cloud](https://cloud.taosdata.com)来获取真实的值。
<!-- exclude-end -->

然后运行下面的命令来生成 telegraf.conf 文件。

```bash
{{#include docs/examples/thirdparty/gen-telegraf-conf.sh:null:nrc}}
```

编辑”outputs.http“部分。

```toml
{{#include docs/examples/thirdparty/telegraf-conf.toml:null:nrc}}
```

配置完成后 Telegraf 会开始收集CPU和内容的数据并发送到 TDengine 的数据库”telegraf“。”telegraf“数据库必须先通过 TDengine Cloud 创建。

## 启动 Telegraf

使用新生的 telegraf.conf 文件启动 Telegraf。

```bash
telegraf --config telegraf.conf
```

## 验证

- 通过下面命令检查 `weather` 数据库 `telegraf` 被创建出来：

```sql
show databases;
```
![TDengine show telegraf databases](./telegraf-show-databases.webp)

检查 `weather` 超级表 cpu 和 mem 被创建出来：

```sql
show telegraf.stables;
```

![TDengine Cloud show telegraf stables](./telegraf-show-stables.webp)

:::note

- TDengine 接收 influxdb 格式数据默认生成的子表名是根据规则生成的唯一 ID 值。
用户如需指定生成的表名，可以通过在 taos.cfg 里配置 smlChildTableName 参数来指定。如果通过控制输入数据格式，即可利用 TDengine 这个功能指定生成的表名。
举例如下：配置 smlChildTableName=tname 插入数据为 st,tname=cpu1,t1=4 c1=3 1626006833639000000 则创建的表名为 cpu1。如果多行数据 tname 相同，但是后面的 tag_set 不同，则使用第一行自动建表时指定的 tag_set，其他的行会忽略）。[TDengine 无模式写入参考指南](/reference/schemaless/#无模式写入行协议)
:::


