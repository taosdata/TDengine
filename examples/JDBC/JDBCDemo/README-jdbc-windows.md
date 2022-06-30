# 如何在 windows环境下使用jdbc进行TDengine应用开发

本文以windows环境为例，介绍java如何进行TDengine开发应用

## 环境准备

（1）安装jdk

官网下载jdk-1.8，下载页面：https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html

安装，配置环境变量，把jdk加入到环境变量里。

命令行内查看java的版本。

```shell
>java -version
java version "1.8.0_131"
Java(TM) SE Runtime Environment (build 1.8.0_131-b11)
Java HotSpot(TM) 64-Bit Server VM (build 25.131-b11, mixed mode)
```


（2）安装配置maven

官网下载maven，下载地址：http://maven.apache.org/download.cgi

配置环境变量MAVEN_HOME，将MAVEN_HOME/bin添加到PATH

命令行里查看maven的版本

```shell
>mvn --version
Apache Maven 3.5.0 (ff8f5e7444045639af65f6095c62210b5713f426; 2017-04-04T03:39:06+08:00)
Maven home: D:\apache-maven-3.5.0\bin\..
Java version: 1.8.0_131, vendor: Oracle Corporation
Java home: C:\Program Files\Java\jdk1.8.0_131\jre
Default locale: zh_CN, platform encoding: GBK
OS name: "windows 10", version: "10.0", arch: "amd64", family: "windows"
```

为了加快maven下载依赖的速度，可以为maven配置mirror，修改MAVEN_HOME\config\settings.xml文件

```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
	<!-- 配置本地maven仓库的路径 -->
    <localRepository>D:\apache-maven-localRepository</localRepository>
    
    <mirrors>
        <!-- 配置阿里云Maven镜像仓库 -->
		<mirror>
			<id>alimaven</id>
			<name>aliyun maven</name>
			<url>http://maven.aliyun.com/nexus/content/groups/public/</url>
			<mirrorOf>central</mirrorOf>
		</mirror>
    </mirrors>
    
    <profiles>
        <!-- 配置jdk，maven会默认使用java1.8 -->
        <profile>
            <id>jdk-1.8</id>
            <activation>
                <activeByDefault>true</activeByDefault>
                <jdk>1.8</jdk>
            </activation>
            <properties>
                <maven.compiler.source>1.8</maven.compiler.source>
                <maven.compiler.target>1.8</maven.compiler.target>
                <maven.compiler.compilerVersion>1.8</maven.compiler.compilerVersion>
            </properties>
        </profile>
    </profiles>
</settings>
```



（3）在linux服务器上安装TDengine-server

在taosdata官网下载TDengine-server，下载地址：https://www.taosdata.com/cn/all-downloads/

在linux服务器上安装TDengine-server

```shell
# tar -zxvf package/TDengine-server-2.0.1.1-Linux-x64.tar.gz 
# cd TDengine-server/
# ./install.sh 
```

启动taosd

```shell
# systemctl start taosd
```

在server上用taos连接taosd

```shell
# taos
taos> show dnodes;
   id   |           end_point            | vnodes | cores  |     status     |   role   |       create_time       |
==================================================================================================================
      1 | td01:6030                      |      2 |      4 | ready          | any      | 2020-08-19 18:40:25.045 |
Query OK, 1 row(s) in set (0.005765s)
```

如果可以正确连接到taosd实例，并打印出databases的信息，说明TDengine的server已经正确启动。这里查看server的hostname

```shell
# hostname -f
td01
```

注意，如果安装TDengine后，使用默认的taos.cfg配置文件，taosd会使用当前server的hostname创建dnode实例。之后，在client也需要使用这个hostname来连接taosd。



（4）在windows上安装TDengine-client

在taosdata官网下载taos客户端，下载地址：
https://www.taosdata.com/cn/all-downloads/
下载后，双击exe安装。

修改client的hosts文件（C:\Windows\System32\drivers\etc\hosts），将server的hostname和ip配置到client的hosts文件中

```
192.168.236.136	td01
```

配置完成后，在命令行内使用taos shell连接server端

```shell
C:\TDengine>taos -h td01
Welcome to the TDengine shell from Linux, Client Version:2.0.1.1
Copyright (c) 2017 by TAOS Data, Inc. All rights reserved.

taos> show databases;
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |    keep0,keep1,keep(D)     |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | precision |    status    |
===================================================================================================================================================================================================================================================================
 test                           | 2020-08-19 18:43:50.731 |           1 |           1 |       1 |      1 |      2 | 3650,3650,3650             |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 | ms        | ready        |
 log                            | 2020-08-19 18:40:28.064 |           4 |           1 |       1 |      1 |     10 | 30,30,30                   |           1 |           3 |         100 |        4096 |        1 |        3000 |    2 | us        | ready        |
Query OK, 2 row(s) in set (0.068000s)
```

如果windows上的client能够正常连接，并打印database信息，说明client可以正常连接server了。



## 应用开发

（1）新建maven工程，在pom.xml中引入taos-jdbcdriver依赖。

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.taosdata.demo</groupId>
    <artifactId>JdbcDemo</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <dependency>
            <groupId>com.taosdata.jdbc</groupId>
            <artifactId>taos-jdbcdriver</artifactId>
            <version>2.0.8</version>
        </dependency>
    </dependencies>
</project>
```

（2）使用jdbc查询TDengine数据库

下面是示例代码：

```java
public class JdbcDemo {

    public static void main(String[] args) throws Exception {
        Connection conn = getConn();
        Statement stmt = conn.createStatement();
        // create database
        stmt.executeUpdate("create database if not exists db");
        // use database
        stmt.executeUpdate("use db");
        // create table
        stmt.executeUpdate("create table if not exists tb (ts timestamp, temperature int, humidity float)");
        // insert data
        int affectedRows = stmt.executeUpdate("insert into tb values(now, 23, 10.3) (now + 1s, 20, 9.3)");
        System.out.println("insert " + affectedRows + " rows.");
        // query data
        ResultSet resultSet = stmt.executeQuery("select * from tb");
        Timestamp ts = null;
        int temperature = 0;
        float humidity = 0;
        while(resultSet.next()){
            ts = resultSet.getTimestamp(1);
            temperature = resultSet.getInt(2);
            humidity = resultSet.getFloat("humidity");
            System.out.printf("%s, %d, %s\n", ts, temperature, humidity);
        }
    }

    public static Connection getConn() throws Exception{
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        String jdbcUrl = "jdbc:TAOS://td01:0/log?user=root&password=taosdata";
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
        return conn;
    }

}
```

（3）测试jdbc访问tdengine的sever实例

console输出：

```
insert 2 rows.
2020-08-26 00:06:34.575, 23, 10.3
2020-08-26 00:06:35.575, 20, 9.3
```



## 指南

（1）如何设置主机名和hosts

在server上查看hostname和fqdn
```shell
查看hostname
# hostname
taos-server

查看fqdn
# hostname -f
taos-server
```

windows下hosts文件位于：
C:\\Windows\System32\drivers\etc\hosts
修改hosts文件，添加server的ip和hostname

```s
192.168.56.101  node5
```

（2）什么是fqdn？


> 什么是FQDN？
>
> FQDN（Full qualified domain name）全限定域名，fqdn由2部分组成：hostname+domainname。
>
> 例如，一个邮件服务器的fqdn可能是：mymail.somecollege.edu，其中mymail是hostname（主机名），somcollege.edu是domainname（域名）。本例中，.edu是顶级域名，.somecollege是二级域名。
>
> 当连接服务器时，必须指定fqdn，然后，dns服务器通过查看dns表，将hostname解析为相应的ip地址。如果只指定hostname（不指定domainname），应用程序可能服务解析主机名。因为如果你试图访问不在本地的远程服务器时，本地的dns服务器和可能没有远程服务器的hostname列表。
>
> 参考：https://kb.iu.edu/d/aiuv
