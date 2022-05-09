---
sidebar_label: TDengine + Telegraf + Grafana
title: Quickly Build IT Operations Presentation System with TDengine + Telegraf + Grafana
---

## Background

TDengine is a big data platform designed and optimized for IoT, Telematics, Industrial Internet, IT operation and maintenance by TAOS Data. Since its open source in July 2019, it has won the favor of a large number of time-series data developers with its innovative data modeling design, fast installation, easy-to-use programming interface and powerful data writing and query performance.

IT operations and maintenance monitoring data are usually data that are sensitive to temporal characteristics, such as

- System resource metrics: CPU, memory, IO, bandwidth, etc.
- Software system metrics: survival status, number of connections, number of requests, number of timeouts, number of errors, response time, service type and other business-related metrics.

Current mainstream IT operations systems usually include a data collection module, a data storage module, and a visualization module; Telegraf and Grafana are one of the most popular data collection modules and visualization modules, respectively. The data storage module is available in a wide range of software options, with OpenTSDB or InfluxDB being the most popular. TDengine, as an emerging time-series big data platform, has the advantages of high performance, high reliability, easy management and easy maintenance.

This article introduces how to quickly build a TDengine + Telegraf + Grafana based IT operation and maintenance system without writing a single line of code and by simply modifying a few lines of configuration files. The architecture is as follows.

![IT-DevOps-Solutions-Telegraf.png](/img/IT-DevOps-Solutions-Telegraf.png)

## Installation steps

### Installing Telegraf, Grafana and TDengine

To install Telegraf, Grafana and TDengine, please refer to the relevant official documentation.

### Telegraf

Please refer to the [official documentation](https://portal.influxdata.com/downloads/).

### Grafana

Please refer to the [official documentation](https://grafana.com/grafana/download).

### TDengine

Download the latest TDengine-server 2.3.0.0 or above from the [Downloads](http://taosdata.com/cn/all-downloads/) page on the Taos Data website and install it.

## Data Link Setup

### Download TDengine plug-in to grafana plug-in directory

```bash
1. wget -c https://github.com/taosdata/grafanaplugin/releases/download/v3.1.3/tdengine-datasource-3.1.3.zip
2. sudo unzip tdengine-datasource-3.1.3.zip -d /var/lib/grafana/plugins/
3. sudo chown grafana:grafana -R /var/lib/grafana/plugins/tdengine
4. echo -e "[plugins]\nallow_loading_unsigned_plugins = tdengine-datasource\n" | sudo tee -a /etc/grafana/grafana.ini
5. sudo systemctl restart grafana-server.service
```

### Modify /etc/telegraf/telegraf.conf

For the configuration method, add the following text to /etc/telegraf/telegraf.conf, where database name should be the name of the database where you want to store Telegraf data in TDengine, TDengine server/cluster host, username and password Fill in the actual TDengine values.

```
[[outputs.http]]
  url = "http://<TDengine server/cluster host>:6041/influxdb/v1/write?db=<database name>"
  method = "POST"
  timeout = "5s"
  username = "<TDengine's username>"
  password = "<TDengine's password>"
  data_format = "influx"
  influx_max_line_bytes = 250
```

Then restart telegraf: ``bash

```bash
sudo systemctl start telegraf
```bash

### Importing the Dashboard

Log in to the Grafana interface using a web browser at IP:3000, with the system's initial username and password being admin/admin.
Click on the gear icon on the left and select Plugins, you should find the TDengine data source plugin icon.
Click on the plus icon on the left and select Import to get the data from `https://github.com/taosdata/grafanaplugin/blob/master/examples/telegraf/grafana/dashboards/telegraf-dashboard- v0.1.0.json`, download the dashboard JSON file and import it. You will then see the dashboard in the following screen.

![IT-DevOps-Solutions-telegraf-dashboard.png](/img/IT-DevOps-Solutions-telegraf-dashboard.png)

## Wrap-up

The above demonstrates how to quickly build a complete IT operations display system. Thanks to the new schemaless protocol parsing feature in TDengine version 2.3.0.0 and the powerful ecological software adaptation capability, users can build an efficient and easy-to-use IT operations system in just a few minutes.
Please refer to the official documentation and product implementation cases for other features.

