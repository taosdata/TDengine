# 一分钟快速搭建一个DevOps监控系统
为了让更多的Devops领域的开发者快速体验TDengine的优秀特性，本文介绍了一种快速搭建Devops领域性能监控的demo，方便大家更方便的了解TDengine，并基于此文拓展Devops领域的应用。
为了快速上手，本文用到的软件全部采用Docker容器方式部署，大家只需要安装Docker软件，就可以直接通过脚本运行所有软件，无需安装。这个Demo用到了以下Docker容器，都可以从Dockerhub上拉取相关镜像
- tdengine/tdengine:1.6.4.5                          TDengine开源版1.6.4.5.的镜像
- tdengine/blm_telegraf:latest                     用于telegraf写入TDengine的API，可以schemaless的将telegraf的数据写入TDengine
- tdengine/blm_prometheus:latest             用于Prometheus写入TDengine的API，可以schemaless的将Prometheus的数据写入TDengine
- grafana/grafana                                         Grafana的镜像，一个广泛应用的开源可视化监控软件
- telegraf:latest                                            一个广泛应用的开源数据采集程序
- prom/prometheus:latest                           一个广泛应用的k8s领域的开源数据采集程序

## 说明
本文中的图片链接在Github上显示不出来，建议将MD文件下载后用vscode或其他md文件浏览工具进行查看

## 前提条件
1. 一台linux服务器或运行linux操作系统的虚拟机或者运行MacOS的计算机
2. 安装了Docker软件。Docker软件的安装方法请参考linux下安装Docker
3. sudo权限
4. 下载本文用到的配置文件和脚本压缩包:[下载地址](http://www.taosdata.com/download/minidevops.tar.gz)

压缩包下载下来后解压生成一个minidevops的文件夹，其结构如下
```sh
minidevops$ tree
.
├── demodashboard.json
├── grafana
│   └── tdengine
│       ├── README.md
│       ├── css
│       │   └── query-editor.css
│       ├── datasource.js
│       ├── img
│       │   └── taosdata_logo.png
│       ├── module.js
│       ├── partials
│       │   ├── config.html
│       │   └── query.editor.html
│       ├── plugin.json
│       └── query_ctrl.js
├── prometheus
│   └── prometheus.yml
├── run.sh
└── telegraf
    └── telegraf.conf
```
`grafana`子文件夹里是TDengine的插件，用于在grafana中导入TDengine的数据源。
`prometheus`子文件夹里是prometheus需要的配置文件。
`run.sh`是运行脚本。
`telegraf`子文件夹里是telegraf的配置文件。
## 启动Docker镜像
启动前，请确保系统里没有运行TDengine和Grafana，以及Telegraf和Prometheus，因为这些程序会占用docker所需的端口，造成脚本运行失败，建议先关闭这些程序。
然后，只用在minidevops路径下执行
```sh
sudo run.sh
```
我们来看看`run.sh`里干了些什么：
```sh
#!/bin/bash
 
LP=`pwd`
 
#为了让脚本能够顺利执行，避免重复执行时出现错误， 首先将系统里所有docker容器停止了。请注意，如果该linux上已经运行了其他docker容器，也会被停止掉。
docker rm -f `docker ps -a -q`
 
#专门创建一个叫minidevops的虚拟网络，并指定了172.15.1.1~255这个地址段。
docker network create --ip-range 172.15.1.255/24 --subnet 172.15.1.1/16 minidevops
 
#启动grafana程序，并将tdengine插件文件所在路径绑定到容器中
docker run -d --net minidevops --ip 172.15.1.11 -v $LP/grafana:/var/lib/grafana/plugins -p 3000:3000 grafana/grafana
 
#启动tdengine的docker容器，并指定IP地址为172.15.1.6，绑定需要的端口
docker run -d --net minidevops --ip 172.15.1.6 -p 6030:6030 -p 6020:6020 -p 6031:6031 -p 6032:6032 -p 6033:6033 -p 6034:6034 -p 6035:6035 -p 6036:6036 -p 6037:6037 -p 6038:6038 -p 6039:6039 tdengine/tdengine:1.6.4.5
 
#启动prometheus的写入代理程序，这个程序可以将prometheus发来的数据直接写入TDengine中，无需提前建立相关超级表和表，实现schemaless写入功能
docker run -d --net minidevops --ip 172.15.1.7 -p 10203:10203 tdengine/blm_prometheus 172.15.1.6
 
#启动telegraf的写入代理程序，这个程序可以将telegraf发来的数据直接写入TDengine中，无需提前建立相关超级表和表，实现schemaless写入功能
docker run -d --net minidevops --ip 172.15.1.8 -p 10202:10202 tdengine/blm_telegraf 172.15.1.6
 
#启动prometheus程序，并将配置文件所在路径绑定到容器中
docker run -d  --net minidevops --ip 172.15.1.9 -v $LP/prometheus:/etc/prometheus -p 9090:9090 prom/prometheus
 
#启动telegraf程序，并将配置文件所在路径绑定到容器中
docker run -d --net minidevops --ip 172.15.1.10 -v $LP/telegraf:/etc/telegraf -p 8092:8092 -p 8094:8094 -p 8125:8125 telegraf
 
#通过Grafana的API，将TDengine配置成Grafana的datasources
curl -X POST http://localhost:3000/api/datasources --header "Content-Type:application/json" -u admin:admin -d '{"Name": "TDengine","Type": "tdengine","TypeLogoUrl": "public/plugins/tdengine/img/taosdata_logo.png","Access": "proxy","Url": "http://172.15.1.6:6020","BasicAuth": false,"isDefault": true,"jsonData": {},"readOnly": false}'
 
#通过Grafana的API，配置一个示范的监控面板
curl -X POST http://localhost:3000/api/dashboards/db --header "Content-Type:application/json" -u admin:admin -d '{"dashboard":{"annotations":{"list":[{"builtIn":1,"datasource":"-- Grafana --","enable":true,"hide":true,"iconColor":"rgba(0, 211, 255, 1)","name":"Annotations & Alerts","type":"dashboard"}]},"editable":true,"gnetId":null,"graphTooltip":0,"id":1,"links":[],"panels":[{"datasource":null,"gridPos":{"h":8,"w":6,"x":0,"y":0},"id":6,"options":{"fieldOptions":{"calcs":["mean"],"defaults":{"color":{"mode":"thresholds"},"links":[{"title":"","url":""}],"mappings":[],"max":100,"min":0,"thresholds":{"mode":"absolute","steps":[{"color":"green","value":null},{"color":"red","value":80}]},"unit":"percent"},"overrides":[],"values":false},"orientation":"auto","showThresholdLabels":false,"showThresholdMarkers":true},"pluginVersion":"6.6.0","targets":[{"refId":"A","sql":"select last_row(value) from telegraf.mem where field=\"used_percent\""}],"timeFrom":null,"timeShift":null,"title":"Memory used percent","type":"gauge"},{"aliasColors":{},"bars":false,"dashLength":10,"dashes":false,"datasource":null,"fill":1,"fillGradient":0,"gridPos":{"h":8,"w":12,"x":6,"y":0},"hiddenSeries":false,"id":8,"legend":{"avg":false,"current":false,"max":false,"min":false,"show":true,"total":false,"values":false},"lines":true,"linewidth":1,"nullPointMode":"null","options":{"dataLinks":[]},"percentage":false,"pointradius":2,"points":false,"renderer":"flot","seriesOverrides":[],"spaceLength":10,"stack":false,"steppedLine":false,"targets":[{"alias":"MEMUSED-PERCENT","refId":"A","sql":"select avg(value) from telegraf.mem where field=\"used_percent\" interval(1m)"}],"thresholds":[],"timeFrom":null,"timeRegions":[],"timeShift":null,"title":"Panel Title","tooltip":{"shared":true,"sort":0,"value_type":"individual"},"type":"graph","xaxis":{"buckets":null,"mode":"time","name":null,"show":true,"values":[]},"yaxes":[{"format":"short","label":null,"logBase":1,"max":null,"min":null,"show":true},{"format":"short","label":null,"logBase":1,"max":null,"min":null,"show":true}],"yaxis":{"align":false,"alignLevel":null}},{"datasource":null,"gridPos":{"h":9,"w":6,"x":0,"y":8},"id":10,"options":{"fieldOptions":{"calcs":["mean"],"defaults":{"mappings":[],"thresholds":{"mode":"absolute","steps":[{"color":"green","value":null}]},"unit":"percent"},"overrides":[],"values":false},"orientation":"auto","showThresholdLabels":false,"showThresholdMarkers":true},"pluginVersion":"6.6.0","targets":[{"alias":"CPU-SYS","refId":"A","sql":"select last_row(value) from telegraf.cpu where field=\"usage_system\""},{"alias":"CPU-IDLE","refId":"B","sql":"select last_row(value) from telegraf.cpu where field=\"usage_idle\""},{"alias":"CPU-USER","refId":"C","sql":"select last_row(value) from telegraf.cpu where field=\"usage_user\""}],"timeFrom":null,"timeShift":null,"title":"Panel Title","type":"gauge"},{"aliasColors":{},"bars":false,"dashLength":10,"dashes":false,"datasource":"TDengine","description":"General CPU monitor","fill":1,"fillGradient":0,"gridPos":{"h":9,"w":12,"x":6,"y":8},"hiddenSeries":false,"id":2,"legend":{"avg":false,"current":false,"max":false,"min":false,"show":true,"total":false,"values":false},"lines":true,"linewidth":1,"nullPointMode":"null","options":{"dataLinks":[]},"percentage":false,"pointradius":2,"points":false,"renderer":"flot","seriesOverrides":[],"spaceLength":10,"stack":false,"steppedLine":false,"targets":[{"alias":"CPU-USER","refId":"A","sql":"select avg(value) from telegraf.cpu where field=\"usage_user\" and cpu=\"cpu-total\" interval(1m)"},{"alias":"CPU-SYS","refId":"B","sql":"select avg(value) from telegraf.cpu where field=\"usage_system\" and cpu=\"cpu-total\" interval(1m)"},{"alias":"CPU-IDLE","refId":"C","sql":"select avg(value) from telegraf.cpu where field=\"usage_idle\" and cpu=\"cpu-total\" interval(1m)"}],"thresholds":[],"timeFrom":null,"timeRegions":[],"timeShift":null,"title":"CPU","tooltip":{"shared":true,"sort":0,"value_type":"individual"},"type":"graph","xaxis":{"buckets":null,"mode":"time","name":null,"show":true,"values":[]},"yaxes":[{"format":"short","label":null,"logBase":1,"max":null,"min":null,"show":true},{"format":"short","label":null,"logBase":1,"max":null,"min":null,"show":true}],"yaxis":{"align":false,"alignLevel":null}}],"refresh":"10s","schemaVersion":22,"style":"dark","tags":["demo"],"templating":{"list":[]},"time":{"from":"now-3h","to":"now"},"timepicker":{"refresh_intervals":["5s","10s","30s","1m","5m","15m","30m","1h","2h","1d"]},"timezone":"","title":"TDengineDashboardDemo","id":null,"uid":null,"version":0}}'
```
执行以上脚本后，可以通过docker container ls命令来确认容器运行的状态：
```sh
$docker container ls
CONTAINER ID        IMAGE                       COMMAND                  CREATED             STATUS              PORTS                                                                                        NAMES
f875bd7d90d1        telegraf                    "/entrypoint.sh tele…"   6 hours ago         Up 6 hours          0.0.0.0:8092->8092/tcp, 8092/udp, 0.0.0.0:8094->8094/tcp, 8125/udp, 0.0.0.0:8125->8125/tcp   wonderful_antonelli
38ee2d5c3cb3        prom/prometheus             "/bin/prometheus --c…"   6 hours ago         Up 6 hours          0.0.0.0:9090->9090/tcp                                                                       infallible_mestorf
1a1939386c07        tdengine/blm_telegraf       "/root/blm_telegraf …"   6 hours ago         Up 6 hours          0.0.0.0:10202->10202/tcp                                                                     stupefied_hypatia
7063eb05caa4        tdengine/blm_prometheus     "/root/blm_prometheu…"   6 hours ago         Up 6 hours          0.0.0.0:10203->10203/tcp                                                                     jovial_feynman
4a7b27931d21        tdengine/tdengine:1.6.4.5   "taosd"                  6 hours ago         Up 6 hours          0.0.0.0:6020->6020/tcp, 0.0.0.0:6030-6039->6030-6039/tcp, 6040-6050/tcp                      eager_kowalevski
ad2895760bc0        grafana/grafana             "/run.sh"                6 hours ago         Up 6 hours          0.0.0.0:3000->3000/tcp                                                                       romantic_mccarthy
```
当以上几个容器都已正常运行后，则我们的demo小系统已经开始工作了。
## Grafana中进行配置
打开浏览器，在地址栏输入服务器所在的IP地址
`http://localhost:3000`
就可以访问到grafana的页面，如果不在本机打开浏览器，则将localhost改成server的ip地址即可。
进入登录页面，用户名和密码都是缺省的admin，输入后，即可进入grafana的控制台输入用户名/密码后，会进入修改密码页面，选择skip，跳过这一步。进入Grafana后，可以在页面的左下角看到TDengineDashboardDemo已经创建好了，![](https://www.taosdata.com/blog/wp-content/uploads/2020/02/image2020-2-1_22-50-58-1024x465.png)对于有些浏览器打开时，可能会在home页面中没有TDengineDashboardDemo的选项，可以通过在Dashboard->Manage中选择![](https://www.taosdata.com/blog/wp-content/uploads/2020/02/2-1024x553.png)TDengineDashboardDemo。点击TDengineDashboardDemo进入示例监控面板。刚点进去页面时，监控曲线是空白的，因为监控数据还不够多，需要等待一段时间，让数据采集程序采集更多的数据。![](https://www.taosdata.com/blog/wp-content/uploads/2020/02/image-5-1024x853.png)

如上两个监控面板分别监控了CPU和内存占用率。点击面板上的标题可以选择Edit进入编辑界面，新增监控数据。关于Grafana的监控面板设置，可以详细参考Grafana官网文档[Getting Started](https://grafana.com/docs/grafana/latest/guides/getting_started/)。

## 原理介绍
按上面的操作，我们已经将监控系统搭建起来了，目前可以监控系统的CPU占有率了。下面介绍下这个Demo系统的工作原理。
如下图所示，这个系统由数据采集功能（prometheus，telegraf），时序数据库功能（TDengine和适配程序），可视化功能(Grafana)组成。下面虚线框里的TDengine，blm_prometheus, blm_telegraf三个容器组成了一个schemaless写入的时序数据库，对于采用telegraf和prometheus作为采集程序的监控对象，可以直接将数据写入TDengine，并通过grafana进行可视化呈现。
![architecture](https://www.taosdata.com/blog/wp-content/uploads/2020/02/image2020-1-29_21-22-6.png)
### 数据采集
数据采集由Telegraf和Prometheus完成。Telegraf根据配置，从操作系统层面采集系统的相关统计值，并按配置上报给指定的URL，上报的数据json格式为
```json
{
    "fields":{
    "usage_guest":0,
    "usage_guest_nice":0,
    "usage_idle":87.73726273726274,
    "usage_iowait":0,
    "usage_irq":0,
    "usage_nice":0,
    "usage_softirq":0,
    "usage_steal":0,
    "usage_system":2.6973026973026974,
    "usage_user":9.565434565434565
    },
    "name":"cpu",
    "tags":{
        "cpu":"cpu-total",
        "host":"liutaodeMacBook-Pro.local"
        },
    "timestamp":1571665100
}
```
其中name将被作为超级表的表名，tags作为普通表的tags，fields的名称也会作为一个tag用来描述普通表的标签。举个例子，一个普通表的结构如下，这是一个存储usage_softirq数据的普通表。
![表结构](https://www.taosdata.com/blog/wp-content/uploads/2020/02/image2020-1-29_21-38-24.png)

### Telegraf的配置
对于使用telegraf作为数据采集程序的监控对象，可以在telegraf的配置文件telegraf.conf中将outputs.http部分的配置按以下配置修改，就可以直接将数据写入TDengine中了
```toml
[[outputs.http]]
#   ## URL is the address to send metrics to
url = "http://172.15.1.8:10202/telegraf"
#
#   ## HTTP Basic Auth credentials
#   # username = "username"
#   # password = "pa$$word"
#
 
data_format = "json"
json_timestamp_units = "1ms"
```
可以打开HTTP basic Auth验证机制，本Demo为了简化没有打开验证功能。
对于多个被监控对象，只需要在telegraf.conf文件中都写上以上的配置内容，就可以将数据写入TDengine中了。

### Telegraf数据在TDengine中的存储结构
Telegraf的数据在TDengine中的存储，是以数据name为超级表名，以tags值加上监控对象的ip地址，以及field的属性名作为tag值，存入TDengine中的。
以name为cpu的数据为例，telegraf产生的数据为:
```json
{
    "fields":{
    "usage_guest":0,
    "usage_guest_nice":0,
    "usage_idle":87.73726273726274,
    "usage_iowait":0,
    "usage_irq":0,
    "usage_nice":0,
    "usage_softirq":0,
    "usage_steal":0,
    "usage_system":2.6973026973026974,
    "usage_user":9.565434565434565
    },
    "name":"cpu",
    "tags":{
        "cpu":"cpu-total",
        "host":"liutaodeMacBook-Pro.local"
        },
    "timestamp":1571665100
}
```
则写入TDengine时会自动存入一个名为cpu的超级表中，这个表的结构如下
![telegraf表结构](https://www.taosdata.com/blog/wp-content/uploads/2020/02/image2020-2-2_0-37-49.png)
这个超级表的tag字段有cpu,host,srcip,field；其中cpu，host是原始数据携带的tag，而srcip是监控对象的IP地址，field是监控对象cpu类型数据中的fields属性，取值空间为[usage_guest,usage_guest_nice,usage_idle,usage_iowait,usage_irq,usage_nice,usage_softirq,usage_steal,usage_system,usage_user]，每个field值对应着一个具体含义的数据。

因此，在查询的时候，可以用这些tag来过滤数据，也可以用超级表来聚合数据。
### Prometheus的配置
对于使用Prometheus作为数据采集程序的监控对象，可以在Prometheus的配置文件prometheus.yaml文件中，将remote write部分的配置按以下配置修改，就可以直接将数据写入TDengine中了。
```yaml
remote_write:
  - url: "http://172.15.1.7:10203/receive"
```
对于多个被监控对象，只需要在每个被监控对象的prometheus配置中增加以上配置内容，就可以将数据写入TDengine中了。
### Prometheus数据在TDengine中的存储结构
Prometheus的数据在TDengine中的存储，与telegraf类似，也是以数据的name字段为超级表名，以数据的label作为tag值，存入TDengine中
以prometheus_engine_queries这个数据为例[prom表结构](https://www.taosdata.com/blog/wp-content/uploads/2020/02/image2020-2-2_0-51-4.png)
在TDengine中会自动创建一个prometheus_engine_queries的超级表，tag字段为t_instance，t_job，t_monitor。
查询时，可以用这些tag来过滤数据，也可以用超级表来聚合数据。

## 数据查询
我们可以登陆到TDengine的客户端命令，通过命令行看看TDengine里面都存储了些什么数据，顺便也能体验一下TDengine的高性能查询。如何才能登陆到TDengine的客户端，我们可以通过以下几步来完成。
首先通过下面的命令查询一下tdengine的Docker ID
```sh
docker container ls
```
然后再执行
```sh
docker exec -it tdengine的containerID bash
```
就可以进入TDengine容器的命令行，执行taos，就进入以下界面![](https://www.taosdata.com/blog/wp-content/uploads/2020/02/image2020-1-29_21-55-53.png)
Telegraf的数据写入时，自动创建了一个名为telegraf的database，可以通过
```
use telegraf；
```
使用telegraf这个数据库。然后执行show tables，describe table等命令详细查询下telegraf这个库里保存了些什么数据。
具体TDengine的查询语句可以参考[TDengine官方文档](https://www.taosdata.com/cn/documentation/taos-sql/)
## 接入多个监控对象
<<<<<<< HEAD
就像前面原理介绍的，这个miniDevops的小系统，已经提供了一个时序数据库和可视化系统，对于多台机器的监控，只需要将每台机器的telegraf或prometheus配置按上面所述修改，就可以完成监控数据采集和可视化呈现了。
=======
就像前面原理介绍的，这个miniDevops的小系统，已经提供了一个时序数据库和可视化系统，对于多台机器的监控，只需要将每台机器的telegraf或prometheus配置按上面所述修改，就可以完成监控数据采集和可视化呈现了。
>>>>>>> 740f82af58c4ecc2deecfa36fb1de4ef5ee55efc
