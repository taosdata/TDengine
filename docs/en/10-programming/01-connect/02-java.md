---
sidebar_label: Java
title: Connect with Java Connector
description: Connect to TDengine cloud service using Java connector
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
<TabItem value="gradel" label="Gradle">

```groovy title="build.gradle"
dependencies {
  implementation 'com.taosdata.jdbc:taos-jdbcdriver:3.0.0.0'
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

```shell
set TDENGINE_JDBC_URL=<jdbcURL>
```
</TabItem>
<TabItem value="powershell" label="Powershell">

```powershell
$env:TDENGINE_JDBC_URL='<jdbcURL>'
```

</TabItem>
</Tabs>


Alternatively, you can set environment variable in your IDE's run configurations.


<!-- exclude -->
:::note
Replace  <jdbcURL\> with real JDBC URL, it will seems like: `jdbc:TAOS-RS://example.com?usessl=true&token=xxxx`.

To obtain the value of JDBC URL, please log in [TDengine Cloud](https://cloud.tdengine.com) and click "Data Insert" on the left menu.
:::
<!-- exclude-end -->
## Connect

Code bellow get JDBC URL from environment variables first and then create a `Connection` object, witch is a standard JDBC Connection object.

```java
{{#include docs/examples/java/src/main/java/com/taos/example/ConnectCloudExample.java:connect}}
```

The client connection is then established. For how to write data and query data, please refer to <https://docs.tdengine.com/cloud/data-in/insert-data/> and <https://docs.tdengine.com/cloud/data-out/query-data/>.

For more details about how to write or query data via REST API, please check [REST API](https://docs.tdengine.com/cloud/programming/connector/rest-api/).
