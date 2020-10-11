典型应用场景

   某省气象预警系统，雨量站小时雨量数据通过MQTT协议同步到数据中心，通过扩展HiveMQ接收数据，通过TDengine rest接口、jdbc接口存储数据。
   （系统运行环境 Docker,开发环境 window 10 ，一下环境特别说明。 ）
一 、基础环境软件

1、HiveMQ  4.4.1 

2、TDengine Server Linux  2.0.4.0 、TDengine client Linux 2.0.4.0 

3、Docker Engine v19.03.12

4、MQTTX 1.3.1 调试工具、 MQTTLoader 压力测试工具

5、IDEA 2019.2 开发工具，JAVA JDK 11

二、源代码简要介绍

java.com

    common 配置文件解析
    
    HiveMQ 扩展服务
    
    TDengine 数据库接口  
    
             .hivemq
             
             .tdengine
             
                      .jdbc
                      
                      .rest
    特别说明 
    1、runtime.properties 
       STYPE=  db，直接JDBC写库,STYPE=rest，调用 rest 接口写库
	   
    2、HiveMQ 通过用户密码安全认证  test_name_1，password_1
三、系统部署

1、TDengine容器部署

1.1 下载镜像，最新版本 2.0.4.0

docker pull tdengine/tdengine

1.2 启动容器 

将配置文件，日志，数据文件， 扩展映射到宿主主机目录。

docker run -d --name tdengine2.0.4 -v E:\taos\etc:/etc/taos -v E:\taos\data:/var/lib/taos -v E:\taos\logs:/var/log/taos --hostname 144td  -p 6030:6030 -p 6035:6035 -p 6041:6041 -p 6030-6040:6030-6040/udp tdengine/tdengine

1.3  创建数据，超级表

建库
create database if not exists rainstation KEEP 7665 DAYS 120 BLOCKS 30;

建超级表
create table if not exists rainstation.monitoring(ts timestamp,  rainfall float) TAGS ( station_code BINARY(10), station_name NCHAR(20), station_type BINARY(2));

2、HiveMQ 容器部署

2.1 下载镜像

docker pull hivmemq/hivmemq

2.2 启动容器

HiveMQ 容器部署，将数据文件，日志，扩展映射到宿主主机目录。

docker run  --name hivemq4  -v E:\hivemq\log:/opt/hivemq/log -v E:\hivemq\data:/opt/hivemq/data -v E:\hivemq\extensions:/opt/hivemq/extensions -p 8080:8080 -p 1883:1883 -p 8083:8083 hivemq/hivemq4
-v E:\hivemq\log:/opt/hivemq/log -v E:\hivemq\data:/opt/hivemq/data -v E:\hivemq\extensions:/opt/hivemq/extensions

2.3、docker中安装 TDengine client 2.0.4.0 客户端

四、扩展程序部署

4.1 HiveMQ扩展程序，支撑rest 接口， jdbc 接口入库。 MAVEN 打包zip，

4.2 解压压缩包放人映射到宿主主机的extensions扩展目录


五、启动容器
    直接在 Docker 客户端中管理容器。
