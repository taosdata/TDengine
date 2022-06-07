---
sidebar_label: Java
title: Connect with Java Connector
---

## Add Dependency {#install}

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

## Config {#config}

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

## Connect {#connect}

```java
import com.taosdata.jdbc.rs.RestfulConnection;
import java.sql.Connection;


public class ConnectCloudExample {
    public static void main(String[] args) throws Exception {
        String url = System.getenv("TDENGINE_CLOUD_URL");
        String token = System.getenv("TDENGINE_CLOUD_TOKEN");
        // create a standard JDBC connection.
        Connection conn = new RestfulConnection(url, token);
    }
}
```

The client connection is then established.