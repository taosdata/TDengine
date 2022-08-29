---
sidebar_label: Java
title: Connect with Java Connector
description: Connect to TDengine cloud service using Java connector
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Add Dependency


<Tabs defaultValue="maven">
<TabItem value="maven" label="Maven">

```xml title="pom.xml"
<dependency>
  <groupId>com.taosdata.jdbc</groupId>
  <artifactId>taos-jdbcdriver</artifactId>
  <version>2.0.39</version>
</dependency>
```

</TabItem>
<TabItem value="gradel" label="Gradle">

```groovy title="build.gradle"
dependencies {
  implementation 'com.taosdata.jdbc:taos-jdbcdriver:2.0.39'
}
```

</TabItem>
</Tabs>

## Config

Run this command in your terminal to save the JDBC URL as variable:


<Tabs defaultValue="bash">
<TabItem value="bash" label="Bash">

```bash
export TDENGINE_JDBC_URL="<jdbcURL>"
```
</TabItem>
<TabItem value="cmd" label="CMD">

```bash
set TDENGINE_JDBC_URL="<jdbcURL>"
```
</TabItem>
<TabItem value="powershell" label="Powershell">

```powershell
$env:TDENGINE_JDBC_URL="<jdbcURL>"
```

</TabItem>
</Tabs>


Alternatively, you can set environment variable in your IDE's run configurations.


<!-- exclude -->
:::note
Replace  <jdbcURL\> with real JDBC URL, it will seems like: `jdbc:TAOS-RS://example.com?usessl=true&token=xxxx`.

To obtain the value of JDBC URL, please log in [TDengine Cloud](https://cloud.tdengine.com) and click "Connector" and then select "Java".
:::
<!-- exclude-end -->
## Connect

Code bellow get JDBC URL from environment variables first and then create a `Connection` object, witch is a standard JDBC Connection object.

```java
{{#include docs/examples/java/src/main/java/com/taos/example/ConnectCloudExample.java:connect}}
```

