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

Run this command in your terminal to save your url and token as variables:


```bash
export TDENGINE_CLOUD_URL=<url>
export TDENGINE_CLOUD_TOKEN=<token>
```

<!-- exclude -->
:::note
You should replace above placeholders as real values. To obtain these values, please log in TDengine Cloud and switch to "Connector" section.

:::
<!-- exclude-end -->

## Connect

Code bellow get URL and token from environment variables first and then create a `RestfulConnection` object, witch is a standard JDBC Connection object.

```java
import com.taosdata.jdbc.rs.RestfulConnection;
import java.sql.Connection;


public class ConnectCloudExample {
    public static void main(String[] args) throws Exception {
        String url = System.getenv("TDENGINE_CLOUD_URL");
        String token = System.getenv("TDENGINE_CLOUD_TOKEN");
        Connection conn = new RestfulConnection(url, token);
    }
}
```

The client connection is then established. For how to write data and query data using the connection, please refer to [usage-examples](https://docs.tdengine.com/reference/connector/java#usage-examples).