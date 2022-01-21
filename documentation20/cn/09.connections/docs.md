# 与其他工具的连接


## <a class="anchor" id="grafana"></a>Grafana

TDengine 能够与开源数据可视化系统 [Grafana](https://www.grafana.com/)快速集成搭建数据监测报警系统，整个过程无需任何代码开发，TDengine 中数据表中内容可以在仪表盘(DashBoard)上进行可视化展现。关于TDengine插件的使用您可以在[GitHub](https://github.com/taosdata/grafanaplugin/blob/master/README.md)中了解更多。

### 安装Grafana

目前 TDengine 支持 Grafana 7.0 以上的版本。用户可以根据当前的操作系统，到 Grafana 官网下载安装包，并执行安装。下载地址如下：<https://grafana.com/grafana/download>。

### 配置Grafana

TDengine 的 Grafana 插件托管在GitHub，可从 <https://github.com/taosdata/grafanaplugin/releases/latest> 下载，当前最新版本为 3.1.3。

推荐使用 [`grafana-cli` 命令行工具](https://grafana.com/docs/grafana/latest/administration/cli/) 进行插件安装。

```bash
sudo -u grafana grafana-cli \
  --pluginUrl https://github.com/taosdata/grafanaplugin/releases/download/v3.1.3/tdengine-datasource-3.1.3.zip \
  plugins install tdengine-datasource
```

或者下载到本地并解压到 Grafana 插件目录。

```bash
GF_VERSION=3.1.3
wget https://github.com/taosdata/grafanaplugin/releases/download/v$GF_VERSION/tdengine-datasource-$GF_VERSION.zip
```

以 CentOS 7.2 操作系统为例，将插件包解压到 /var/lib/grafana/plugins 目录下，重新启动 grafana 即可。

```bash
sudo unzip tdengine-datasource-$GF_VERSION.zip -d /var/lib/grafana/plugins/
```

Grafana 7.3+ / 8.x 版本会对插件进行签名检查，因此还需要在 grafana.ini 文件中增加如下行，才能正确使用插件：

```ini
[plugins]
allow_loading_unsigned_plugins = tdengine-datasource
```

在Docker环境下，可以使用如下的环境变量设置自动安装并设置 TDengine 插件：

```bash
GF_INSTALL_PLUGINS=https://github.com/taosdata/grafanaplugin/releases/download/v3.1.3/tdengine-datasource-3.1.3.zip;tdengine-datasource
GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=tdengine-datasource
```

### 使用 Grafana

#### 配置数据源

用户可以直接通过 <http://localhost:3000> 的网址，登录 Grafana 服务器（用户名/密码：admin/admin），通过左侧 `Configuration -> Data Sources` 可以添加数据源，如下图所示：

![img](../images/connections/add_datasource1.jpg)

点击 `Add data source` 可进入新增数据源页面，在查询框中输入 TDengine 可选择添加，如下图所示：

![img](../images/connections/add_datasource2.jpg)

进入数据源配置页面，按照默认提示修改相应配置即可：

![img](../images/connections/add_datasource3.jpg)

* Host： TDengine 集群的中任意一台服务器的 IP 地址与 TDengine RESTful 接口的端口号(6041)，默认 http://localhost:6041。注意：从 2.4 版本开始 RESTful 服务默认使用独立组件 taosAdapter 提供，请参考相关文档配置部署。
* User：TDengine 用户名。
* Password：TDengine 用户密码。

点击 `Save & Test` 进行测试，成功会有如下提示：

![img](../images/connections/add_datasource4.jpg)

#### 创建 Dashboard

回到主界面创建 Dashboard，点击 Add Query 进入面板查询页面：

![img](../images/connections/create_dashboard1.jpg)

如上图所示，在 Query 中选中 `TDengine` 数据源，在下方查询框可输入相应 sql 进行查询，具体说明如下：

* INPUT SQL：输入要查询的语句（该 SQL 语句的结果集应为两列多行），例如：`select avg(mem_system) from log.dn where  ts >= $from and ts < $to interval($interval)` ，其中，from、to 和 interval 为 TDengine插件的内置变量，表示从Grafana插件面板获取的查询范围和时间间隔。除了内置变量外，`也支持可以使用自定义模板变量`。
* ALIAS BY：可设置当前查询别名。 
* GENERATE SQL： 点击该按钮会自动替换相应变量，并生成最终执行的语句。
  
按照默认提示查询当前 TDengine 部署所在服务器指定间隔系统内存平均使用量如下：

![img](../images/connections/create_dashboard2.jpg)

> 关于如何使用Grafana创建相应的监测界面以及更多有关使用Grafana的信息，请参考Grafana官方的[文档](https://grafana.com/docs/)。

#### 导入 Dashboard

在 2.3.3.0 及以上版本，您可以导入 TDinsight Dashboard (Grafana Dashboard ID： [15167](https://grafana.com/grafana/dashboards/15167)) 作为 TDengine 集群的监控可视化工具。安装和使用说明请见 [TDinsight 用户手册](https://www.taosdata.com/cn/documentation/tools/insight)。

