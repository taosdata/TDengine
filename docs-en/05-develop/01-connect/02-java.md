---
sidebar_label: Java
title: Connect with Java Connector
---

## Add Dependency

Build with Maven

```xml
<dependency>
  <groupId>com.taosdata.jdbc</groupId>
  <artifactId>taos-jdbcdriver</artifactId>
  <version>2.0.39</version>
</dependency>
```

Build with Gradle

```groovy
dependencies {
  implementation 'com.taosdata.jdbc:taos-jdbcdriver:2.0.39'
}
```

## Config

Run this command in your terminal to save TDengine cloud token as variables:


```bash
export TDENGINE_CLOUD_TOKEN=<token>
```

## Connect


```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class ConnectCloudExample {
    public static void main(String[] args) throws SQLException {
        String token = System.getenv("TDENGINE_CLOUD_TOKEN");
        String jdbcUrl = "jdbc:TAOS-RS://cloud.taosdata.com:8085?usessl=true&token=" + token;
        Connection conn = DriverManager.getConnection(jdbcUrl);
        System.out.println("Connected");
        conn.close();
    }
}
```

The client connection is then established. For how to write data and query data using the connection, please refer to [usage-examples](https://docs.tdengine.com/reference/connector/java#usage-examples).