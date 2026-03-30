# TDengine 行业版（第三次讨论）

## 1. 需求目标

为了覆盖更多客户群，以较低价格、大量出货、快速占据市场，公司决定裁剪出若干行业版本。TDengine 行业版面向的是有共性需求的、可承受不同价格敏感度的市场，例如电厂、风厂的集控软件，储能站的 EMS 软件等。
1. 生成若干行业版，行业内还可进一步细分，例如水电版、火电版、光伏版，命名由各销售事业部决定，价格、销售范围由中国业务部决定
2. 行业版功能特性是企业版的子集，各销售事业部从功能列表中选择，由研发部生成安装包，行业版的版本号与对应企业版的版本号相同，发版周期也与企业版相同
3. 用户可以通过 taosExplorer、Taos Shell 直观感受到行业版与企业版的不同

## 2. 可裁剪功能列表

### 2.1 社区版功能

1. 无模式写入
2. 时序数据删除
3. 数据订阅
4. 流计算
5. 自定义函数
6. Restful / Websocket 连接
7. 集群功能（三副本）
8. 运行监控
9. 标签索引
10. 复合主键
11. TSMA

### 2.2 企业版功能

1. 权限管理
2. 数据审计
3. 视图功能
4. 多级存储
5. S3 存储
6. 数据库加密
7. 多表低频支持
8. 数据碎片整理
9. IP 白名单
10. 国产操作系统
11. 双副本
12. 主备
13. 数据同步复制
14. 数据备份与恢复
15. 数据源接入
   - PI
   - OPC UA
   - OPC DA
   - Wonderware
   - MQTT
   - InfluxDb
   - OpenTSDB
   - Kafka
   - CSV
   - Oracle
   - MySQL
   - PostgreSQL

## 3. 行业版与企业版的外观差别

### 3.1 安装包

#### 3.1.1 安装包名称

以 3.2.3.0 为例，企业版安装包名称为`TDengine-enterprise-3.2.3.0-Linux-arm64.tar.gz`，行业版的名称是 `TDengine-``Power``-3.2.3.0-Linux-arm64.tar.gz`

#### 3.1.2 可执行文件

1. 如果“运行监控”功能未被勾选，“taoskeeper 和 tdengine-datasource.zip”不应被打包
2. 如果“自定义函数”功能未被勾选，“udfd”不应被打包
3. 如果“数据同步复制、数据备份恢复、双副本、数据源接入”都未被勾选，“taosx、taosx-plugins” 不应被打包
4. 如果“restful、websocket”功能未被勾选，“libtaosws.so” 不应被打包

### 3.2 taosExplorer

#### 3.2.1 网站标题

![](./images/img_QaQfbbWnTodnRpxU3mfcEh2pnCb.png)

#### 3.2.2 登录页面

![](./images/img_GzXjbTQHeod2M0xxnX5cI5pnnvf.png)

#### 3.2.3 标题栏

![](./images/img_IlPTbJtDyoP93XxgljPcp13DnMg.png)

#### 3.2.4 面板

如果“运行监控”功能被裁剪，此页面应被隐藏

#### 3.2.5 数据写入

如果未勾选任何“数据接入连接器”，此页面不再显示
“Add New Data Source”页面只显示被勾选过的数据接入连接器

#### 3.2.6 流计算

如果“流计算”功能被裁剪，此页面应被隐藏

#### 3.2.7 数据订阅

如果“数据订阅”功能被裁剪，此页面应被隐藏

#### 3.2.8 系统管理

![](./images/img_KgfJbtNnpouCULxHCXCcTUh1ndf.png)

1. 如果“数据备份恢复”功能未被勾选，“备份”页面应被隐藏
2. 如果“数据同步复制”功能未被勾选，“同步”页面应被隐藏
3. 如果“集群”功能未被勾选，“集群”页面应被隐藏
4. “许可证” -> “数据库基本功能”
   - “版本3.2.3.0 Trial” 应该做相应修改
   - “数据库可选功能” 不再显示
   - “数据接入任务” 不再显示
  总之，仅仅控制基本功能，“数据库可选功能” “数据接入任务”固定住，行业版不再让用户可配置
1. 如果“数据审计”功能未被勾选，“数据审计”页面应被隐藏

### 3.3 Taos Shell

#### 3.3.1 登录信息

修改
```bash {wrap}
Server is Enterprise trial Edition, ver:3.2.3.0 and will expire at 2024-04-13 09:49:04.
```

为
```bash {wrap}
Server is Power Edition, ver:3.2.3.0 and will expire at 2024-04-13 09:49:04.
```

#### 3.3.2 版本信息

修改 show grants 的版本信息中的 `version` 字段显示内容
```bash {wrap}
taos> show grants \G;
*************************** 1.row ***************************
     version: trial
 expire_time: 2024-04-13 09:49:08
service_time: 2024-04-03 09:49:04
     expired: false
       state: ungranted
  timeseries: unlimited
      dnodes: unlimited
   cpu_cores: unlimited
Query OK, 1 row(s) in set (0.004996s)
```

修改 taosd -v 输出的版本信息
```bash {wrap}
taosd -V
power version: 3.2.3.0 compatible_version: 3.0.0.0
gitinfo: e27fdcff254b7bd0e0ad2f825e0414da4c0f37dc
gitinfoOfInternal: 1fa6f400b011e78e72a75bbc903ca8c34212e2d5
buildInfo: Built Linux-x64 at 2024-02-29 22:16:43 +0800
```
