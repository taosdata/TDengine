---
sidebar_label: Java
title: 使用 Java 连接器建立连接
description: 使用 Java 连接器建立和 TDengine Cloud 的连接
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->
## 增加依赖包


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

## 配置

在您的终端里面执行下面的命令设置 JDBC URL 为环境变量：

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


另外，您也可以在您的 IDE 的运行配置里设置环境变量

<!-- exclude -->
:::note
替换 <jdbcURL\> 为 真实的 JDBC URL，比如 `jdbc:TAOS-RS://example.com?usessl=true&token=xxxx`。

获取真实的 JDBC URL 的值，请登录[TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”编程“菜单，然后选择”Java“。
:::
<!-- exclude-end -->
## 建立连接

下面的代码是先从环境变量里面创建 JDBC URL ，然后创建 `Connection` 这个 JDBC 连接标准对象。

```java
{{#include docs/examples/java/src/main/java/com/taos/example/ConnectCloudExample.java:connect}}
```

客户端连接建立连接以后，想了解更多写入数据和查询数据的内容，请参考 <https://docs.taosdata.com/cloud/data-in/insert-data/> and <https://docs.taosdata.com/cloud/data-out/query-data/>.

想知道更多通过 REST 接口写入数据的详情，请参考[REST 接口](https://docs.taosdata.com/cloud/programming/connector/rest-api/).
