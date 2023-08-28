---
sidebar_label: InfluxDB 行协议
title: Schemaless - InfluxDB 行协议
description: 通过 Schemaless 行协议写入数据
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->

这个部分我们会介绍如何通过无服务 InfluxDB 行协议的 REST 接口往 TDengine Cloud 写入数据。

## 配置

在您的终端命令行运行下面的命令来设置 TDengine Cloud 的令牌和URL为环境变量：

<Tabs defaultValue="bash">
<TabItem value="bash" label="Bash">

```bash
export TDENGINE_CLOUD_TOKEN="<token>"
export TDENGINE_CLOUD_URL="<url>"
```

</TabItem>
<TabItem value="cmd" label="CMD">

```bash
set TDENGINE_CLOUD_TOKEN="<token>"
set TDENGINE_CLOUD_URL="<url>"
```

</TabItem>
<TabItem value="powershell" label="Powershell">

```powershell
$env:TDENGINE_CLOUD_TOKEN="<token>"
$env:TDENGINE_CLOUD_URL="<url>"
```

</TabItem>
</Tabs>

## 插入

您可以使用任何支持 HTTP 协议的客户端通过访问 RESTful 的接口地址 `<cloud_url>/influxdb/v1/write` 往 TDengine 里面写入兼容 InfluxDB 的数据。访问地址如下：

```text
/influxdb/v1/write?db=<db_name>&token=<cloud_token>
```

支持 InfluxDB 查询参数如下：

- `db` 指定 TDengine 使用的数据库名
- `precision` TDengine 使用的时间精度
    - ns - 纳秒
    - u - 微妙
    - ms - 毫秒
    - s - 秒
    - m - 分
    - h - 小时

## 写入样例
```bash
curl --request POST "$TDENGINE_CLOUD_URL/influxdb/v1/write?db=<db_name>&token=$TDENGINE_CLOUD_TOKEN&precision=ns" --data-binary "measurement,host=host1 field1=2i,field2=2.0 1577846800001000001"
```
  
## 使用 SQL 查询样例
- `measurement` 是超级表名。
- 您可以像这样通过标签过滤数据：`where host="host1"`。
```bash
curl -L -d "select * from <db_name>.measurement where host=\"host1\"" $TDENGINE_CLOUD_URL/rest/sql/test?token=$TDENGINE_CLOUD_TOKEN
```
