# 使用 TDengine + Telegraf + Grafana 快速搭建 IT 运维展示系统

## 背景介绍
TDengine是涛思数据专为物联网、车联网、工业互联网、IT运维等设计和优化的大数据平台。自从 2019年 7 月开源以来，凭借创新的数据建模设计、快捷的安装方式、易用的编程接口和强大的数据写入查询性能博得了大量时序数据开发者的青睐。

IT 运维监测数据通常都是对时间特性比较敏感的数据，例如：
- 系统资源指标：CPU、内存、IO、带宽等。
- 软件系统指标：存活状态、连接数目、请求数目、超时数目、错误数目、响应时间、服务类型及其他与业务有关的指标。

当前主流的 IT 运维系统通常包含一个数据采集模块，一个数据存储模块，和一个可视化显示模块。Telegraf 和 Grafana 分别是当前最流行的数据采集模块和可视化显示模块之一。而数据存储模块可供选择的软件比较多，其中 OpenTSDB 或 InfluxDB 比较流行。而 TDengine 作为新兴的时序大数据平台，具备极强的高性能、高可靠、易管理、易维护的优势。

本文介绍不需要写一行代码，通过简单修改几行配置文件，就可以快速搭建一个基于 TDengine + Telegraf + Grafana 的 IT 运维系统。架构如下图：

![IT-DevOps-Solutions-Telegraf.png](../../images/IT-DevOps-Solutions-Telegraf.png)


## 安装步骤

### 安装 Telegraf，Grafana 和 TDengine
安装 Telegraf 和 Grafana 请参考相关官方文档，这里仅假设使用 Ubuntu 20.04 LTS 为操作系统为例。

### Telegraf
```
1. wget -qO- https://repos.influxdata.com/influxdb.key | sudo apt-key add -
2. source /etc/lsb-release
3. echo "deb https://repos.influxdata.com/${DISTRIB_ID,,} ${DISTRIB_CODENAME} stable" | sudo tee /etc/apt/sources.list.d/influxdb.list
4. sudo apt-get update
5. sudo apt-get install telegraf
6. sudo systemctl start telegraf
```

### Grafana
```
1. wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add -
2. echo "deb https://packages.grafana.com/enterprise/deb stable main" | sudo tee -a /etc/apt/sources.list.d/grafana.list
3. sudo apt-get update
4. sudo apt-get install grafana
5. sudo systemctl start grafana-server.service
```

### 安装 TDengine 
从涛思数据官网下载页面 （http://taosdata.com/cn/all-downloads/）最新 TDengine-server 2.3.0.0 版本，以 deb 安装包为例。
```
1. sudo dpkg -i TDengine-server-2.3.0.0-Linux-x64.deb
2. sudo systemctl start taosd
```


## 数据链路设置
### 复制 TDengine 插件到 grafana 插件目录
```
1. sudo cp -r /usr/local/taos/connector/grafanaplugin /var/lib/grafana/plugins/tdengine
2. sudo chown grafana:grafana -R /var/lib/grafana/plugins/tdengine
3. echo -e "[plugins]\nallow_loading_unsigned_plugins = taosdata-tdengine-datasource\n" | sudo tee -a /etc/grafana/grafana.ini
4. sudo systemctl restart grafana-server.service
```

### 修改 /etc/telegraf/telegraf.conf 
假设 TDengine 和 Telegraf 在同一台机器上部署，且假设 TDengine 使用默认用户名 root 和密码 taosdata。增加如下文字：
```
[[outputs.http]]
  url = "http://127.0.0.1:6041/influxdb/v1/write?db=metrics"
  method = "POST"
  timeout = "5s"
  username = "root"
  password = "taosdata"
  data_format = "influx"
  influx_max_line_bytes = 250
```
然后重启 telegraf：
```
sudo systemctl start telegraf
```


### 导入 Dashboard

使用 Web 浏览器访问 IP:3000 登录 Grafana 界面，系统初始用户名密码为 admin/admin。
点击左侧齿轮图标并选择 Plugins，应该可以找到 TDengine data source 插件图标。
点击左侧加号图标并选择 Import，按照界面提示选择 /usr/local/taos/connector/grafanaplugin/examples/telegraf/grafana/dashboards/telegraf-dashboard-v0.1.0.json 文件，然后应该可以看到如下界面的仪表板界面：

![IT-DevOps-Solutions-telegraf-dashboard.png](../../images/IT-DevOps-Solutions-telegraf-dashboard.png)


## 总结

以上演示如何快速搭建一个完整的 IT 运维展示系统。得力于 TDengine 2.3.0.0 版本中新增的 schemaless 协议解析功能，以及强大的生态软件适配能力，用户可以短短数分钟就可以搭建一个高效易用的 IT 运维系统。TDengine 强大的数据写入查询性能和其他丰富功能请参考官方文档和产品落地案例。
