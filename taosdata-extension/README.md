# 概述

## 功能

本项目支持HiveMQ将指定主题的消息写入到涛思(taos)数据库。

## 架构

作为HiveMQ的一个扩展，结构比较简单，主要类：
- TaosMain HiveMQ入口类，扩展的启停、初始化
- TaosInterceptor 消息拦截器，对接收到的消息进行处理
- TaosDao 涛思数据库类，将消息进行转换，写入数据库
- configuration目录 配置相关类



# 安装

## 编译
### 要求
- Maven 3.x
- JDK 11
### 编译步骤
在项目目录运行 mvn clean package

## 安装
### 要求
部署HiveMQ、Taos请参照官方文档
### 安装步骤
- 将上述编译生成`target目录下taosdata-extension-0.1.0-distribution.zip` 拷贝到 HiveMQ的 extensions目录

- 解压，生成目录如 taosdata-extension

- 将配置文件 taos.properties、taos.yml 拷贝到上述解压的扩展所在目录 taosdata-extension

### 配置
按照实际环境编辑配置文件
- 涛思数据库及druid连接池配置 taos.properties
```
driverClassName=com.taosdata.jdbc.TSDBDriver
# test为库名
url=jdbc:TAOS://taos:6030/test
# username=root
# password=taosdata

initialSize=3
maxActive=10
maxWait=10000
minIdle=3
timeBetweenEvictionRunsMillis=3000
minEvictableIdleTimeMillis=60000
maxEvictableIdleTimeMillis=90000
validationQuery=select server_status();
testWhileIdle=true
testOnBorrow=false
testOnReturn=false
```

- 消息主题映射配置 taos.yml
```
"topics":
  -
    # 消息路由及名称
    "id": t1
    # 可选，写到TAOS库中的表名
    "table": t1
    # 要写库的字段列表
    "fields":
      - t
      - id
      - v1
      - v2
      - v3
```



### 启动HiveMQ
查看日志，有无异常

### 测试
- 创建数据库和表
```
CREATE DATABASE test;
USE test;
CREATE TABLE t1 (t timestamp, id nchar(32), v1 int, v2 float, v3 nchar(16));
```

- 通过MQTT客户端发送消息
```
{
  "t": 1601805363333,
  "id": "diablo",
  "v1": 888,
  "v2": 1.1,
  "v3": "test"
}
```

- 验证

  ```
  # 在 Taos CLI运行
  SELECT * FROM t1;
  ```

  