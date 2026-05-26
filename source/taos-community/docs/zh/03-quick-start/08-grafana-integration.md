---
sidebar_label: '任务：Grafana 集成'
title: Grafana 集成
toc_max_heading_level: 4
---

[Grafana](https://grafana.com/grafana/) 是一个流行的开源数据可视化和监控平台，TDengine 能够与 Grafana 快速集成，搭建数据可视化和监控告警系统，整个过程无需任何代码开发。下面将以使用 `taosBenchmark` 生成的智能电表数据为例，介绍如何使用 Grafana 创建一个展示电流 (current) 波动情况的面板。

## 前置准备

1. 请先安装并启动 Grafana, 目前 TDengine 支持 Grafana 8.0 及以上的版本
2. 使用以下命令写入测试数据，这个命令将在名为 test 的数据库下，创建超级表 meters, 这个超级表包含 100 个子表，每个子表 1000 条记录，记录的开始时间为 1 小时前：

 ```bash
 taosBenchmark --start-timestamp=$(date --date="1 hours ago" +%s%3N) \
  --time-step=1000 --records=1000 \
  --tables=100 --answer-yes
 ```

## 安装 Grafana 插件

Grafana 与 TDengine 之间的交互，需要通过 [TDengine Datasource](https://github.com/taosdata/grafanaplugin) 插件来完成。在 Linux 平台，该插件可以通过以下命令一键安装。

```bash
bash -c "$(curl -fsSL https://raw.githubusercontent.com/taosdata/grafanaplugin/master/install.sh)"
```

其它平台的安装可参考插件 Github 仓库中的[安装指南](https://github.com/taosdata/grafanaplugin/blob/master/INSTALLATION.md)。

在安装完成后，请重启 Grafana 服务。

```bash
sudo systemctl restart grafana-server.service
```

## 创建连接

安装插件后，请进入 Connections - Add new connection 页面，搜索 "TDengine", 即可查询到 TDengine Datasource 插件。点击 Add new data source 按钮，进入数据源配置页面，并完成以下配置：

- TDengine Host: 填写 taosAdapter 的地址和端口号，如果是在本地运行，可输入 [http://localhost:6041](http://localhost:6041)
- TDengine Authentication: 配置 TDengine 数据库的认证方式，默认使用用户名、密码的认证方式（默认的用户名、密码为：`root/taosdata`）

填写以上信息后，请点击 Save & test 按钮，如果看到以下消息：TDengine Data source is working, 即表示 TDengine 与 Grafana 的连接已创建成功。点击提示消息下方的 building a dashboard 链接，就可以创建 Dashboard 了。

## 创建 Dashboard

具体步骤如下所示：

1. 点击 building a dashboard -> Add visualization, 并选择刚刚添加的 data source
2. 在 Input SQL 文本框，输入以下 SQL 语句后，点击 Apply 按钮，即可查看到平均电流变化情况的曲线图。

```sql
SELECT _wstart AS ts, avg(voltage) AS voltage, avg(phase) AS phase FROM test.meters
WHERE groupid =1 and ts > $from AND ts < $to interval($interval) fill(null)
```

![Grafana Dashboard](./01-download-and-install/assets/grafana.png)

更多细节，请参考：[与 Grafana 集成](../12-ecosystem-integrations/02-visual/01-grafana.mdx)。
