---
sidebar_label: Java
title: Connect with Java
description: This document describes how to connect to TDengine Cloud using the Java client library.
---

<!-- exclude -->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->

## Add Dependency

<Tabs defaultValue="maven">
<TabItem value="maven" label="Maven">

```xml title="pom.xml"
{{#include docs/examples/java/pom.xml:dep}}
```

</TabItem>
<TabItem value="spring" label="Spring">

In the "pom.xml" file, please add the Spring Boot and TDengine Java connector dependencies:

```xml
{{#include docs/examples/java/spring/pom.xml:spring}}
```

</TabItem>
</Tabs>

## Config

Run this command in your terminal to save the JDBC URL as variable, or if it is a Spring application, you can use the Spring configuration:

<Tabs defaultValue="bash">
<TabItem value="bash" label="Bash">

```bash
export TDENGINE_JDBC_URL="<jdbcURL>"
```

</TabItem>
<TabItem value="cmd" label="CMD">

```shell
set TDENGINE_JDBC_URL=<jdbcURL>
```

</TabItem>
<TabItem value="powershell" label="Powershell">

```powershell
$env:TDENGINE_JDBC_URL='<jdbcURL>'
```

</TabItem>
<TabItem value="spring" label="Spring">

```yml
{{#include docs/examples/java/spring/src/main/resources/application.yml}}
```

</TabItem>
</Tabs>

<!-- exclude -->

:::important

Replace `<jdbcURL>` with real JDBC URL, it will seems like: `jdbc:TAOS-RS://example.com?useSSL=true&token=xxxx`.

To obtain the value of JDBC URL, please log in [TDengine Cloud](https://cloud.tdengine.com) and click "Programming" on the left menu, then select "Java".

:::

<!-- exclude-end -->

## Connect

<Tabs defaultValue="java">
<TabItem value="java" label="Java">
Code bellow get JDBC URL from environment variables first and then create a `Connection` object, which is a standard JDBC Connection object.

```java
{{#include docs/examples/java/src/main/java/com/taos/example/ConnectCloudExample.java:connect}}
```

</TabItem>
<TabItem value="spring" label="Spring">

1. Define an interface called "meterMapper", which uses the MyBatis framework to map from TDengine database super table to Java object:

```java
{{#include docs/examples/java/spring/src/main/java/com/taos/example/dao/MeterMapper.java:mybatis}}
```

1. Create a `meterMapper.xml` file under `src/main/resources/mapper`, and add the following SQL mapping:

```xml
{{#include docs/examples/java/spring/src/main/resources/mapper/MeterMapper.xml}}
```

1. For more details about how to write or query data from TDngine Cloud instance through Spring, please refer to [Spring Example](https://github.com/taosdata/TDengine/tree/docs-cloud/docs/examples/java/spring/)

</TabItem>
</Tabs>

The client connection is then established. For how to write data and query data, please refer to [Insert](https://docs.tdengine.com/cloud/programming/insert/) and [Query](https://docs.tdengine.com/cloud/programming/query/).

For more details about how to write or query data via REST API, please check [REST API](https://docs.tdengine.com/cloud/programming/connect/rest-api/).
