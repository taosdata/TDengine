---
title: 系统监控
---

TDengine 启动后，会自动创建一个监测数据库 log，并自动将服务器的 CPU、内存、硬盘空间、带宽、请求数、磁盘读写速度、慢查询等信息定时写入该数据库。TDengine 还将重要的系统操作（比如登录、创建、删除数据库等）日志以及各种错误报警信息记录下来存放在 log 库里。系统管理员可以从 CLI 直接查看这个数据库，也可以在 WEB 通过图形化界面查看这些监测信息。

这些监测信息的采集缺省是打开的，但可以修改配置文件里的选项 monitor 将其关闭或打开。

## TDinsight - 使用监控数据库 + Grafana 对 TDengine 进行监控的解决方案

从 2.3.3.0 开始，监控数据库将提供更多的监控项，您可以从 [TDinsight Grafana Dashboard](https://grafana.com/grafana/dashboards/15167) 了解如何使用 TDinsight 方案对 TDengine 进行监控。

我们提供了一个自动化脚本 `TDinsight.sh` 对 TDinsight 进行部署。

下载 `TDinsight.sh`：

```bash
wget https://github.com/taosdata/grafanaplugin/raw/master/dashboards/TDinsight.sh
chmod +x TDinsight.sh
```

准备：

1. TDengine Server 信息：

   - TDengine RESTful 服务：对本地而言，可以是 `http://localhost:6041`，使用参数 `-a`。
   - TDengine 用户名和密码，使用 `-u` `-p` 参数设置。

2. Grafana 告警通知

   - 使用已经存在的 Grafana Notification Channel `uid`，参数 `-E`。该参数可以使用 `curl -u admin:admin localhost:3000/api/alert-notifications |jq` 来获取。

     ```bash
     sudo ./TDinsight.sh -a http://localhost:6041 -u root -p taosdata -E <notifier uid>
     ```

   - 使用 TDengine 数据源插件内置的阿里云短信告警通知，使用 `-s` 启用之，并设置如下参数：

     1. 阿里云短信服务 Key ID，参数 `-I`
     2. 阿里云短信服务 Key Secret，参数 `K`
     3. 阿里云短信服务签名，参数 `-S`
     4. 短信通知模板号，参数 `-C`
     5. 短信通知模板输入参数，JSON 格式，参数 `-T`，如 `{"alarm_level":"%s","time":"%s","name":"%s","content":"%s"}`
     6. 逗号分隔的通知手机列表，参数 `-B`

     ```bash
     sudo ./TDinsight.sh -a http://localhost:6041 -u root -p taosdata -s \
       -I XXXXXXX -K XXXXXXXX -S taosdata -C SMS_1111111 -B 18900000000 \
       -T '{"alarm_level":"%s","time":"%s","name":"%s","content":"%s"}'
     ```

运行程序并重启 Grafana 服务，打开面板：`http://localhost:3000/d/tdinsight`。

更多使用场景和限制请参考[TDinsight](/reference/tdinsight/) 文档。
