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
  <version>2.0.38</version>
</dependency>
```

Build with Gradle

```
dependencies {
  implementation 'com.taosdata.jdbc:taos-jdbcdriver:2.0.39'
}
```

## Config

Run this command in your terminal to save connect parameters as environment variables:

```bash
export TDENGINE_CLOUD_HOST=<host>
export TDENGINE_CLOUD_PORT=<port>
export TDENGINE_CLOUD_TOKEN=<token>
export TDENGINE_USER_NAME=<username>
export TDENGINE_PASSWORD=<password>
```

<!-- exclude -->
:::note
You should replace above placeholders as real values. To get these values, please log in TDengine Cloud and switch to "Connector" section.

:::
<!-- exclude-end -->

## Connect

```java
import com.taosdata.jdbc.TSDBDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectCloudExample {
    public static void main(String[] args) throws SQLException {
        String host = System.getenv("TDENGINE_CLOUD_HOST");
        String port = System.getenv("TDENGINE_CLOUD_PORT");
        String token = System.getenv("TDENGINE_CLOUD_TOKEN");
        String user = System.getenv("TDENGINE_USER_NAME");
        String password = System.getenv("TDENGINE_PASSWORD");
        String jdbcUrl = String.format("jdbc:TAOS-RS://%s:%s?user=%s&password=%s", host, port, user, password);
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TOKEN, token);
        Connection conn = DriverManager.getConnection(jdbcUrl);
        System.out.println("Connected");
        conn.close();
    }
}
```