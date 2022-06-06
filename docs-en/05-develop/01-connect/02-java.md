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
You should replace above placeholders as real values. To get these values, please log in TDengine Cloud and switch to "Connector" section.

:::
<!-- exclude-end -->

## Connect

```java
import com.taosdata.jdbc.TSDBDriver;

import java.sql.Connection;
import java.sql.SQLException;


public class ConnectCloudExample {
    public static void main(String[] args) throws SQLException {
        String url = System.getenv("TDENGINE_CLOUD_URL");
        String token = System.getenv("TDENGINE_CLOUD_TOKEN");
        Connection conn = TSDBDriver.connect(url, token); // demo code for discussion.
    }
}
```

The client connection is then established. 