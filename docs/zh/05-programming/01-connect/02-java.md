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
<TabItem value="spring" label="Spring">

在“pom.xml”文件中添加 Spring Boot 和 TDengine Java connector 的依赖：

```xml
{{#include docs/examples/java/spring/pom.xml:spring}}
```

</TabItem>
</Tabs>

## 配置

在您的终端里面执行下面的命令设置 JDBC URL 为环境变量。如果是 Spring 应用，您可以使用如下 Spring 的配置：

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

另外，您也可以在您的 IDE 的运行配置里设置环境变量

<!-- exclude -->

:::note IMPORTANT
替换 \<jdbcURL> 为 真实的 JDBC URL，比如 `jdbc:TAOS-RS://example.com?useSSL=true&token=xxxx`。

获取真实的 JDBC URL 的值，请登录[TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”编程“菜单，然后选择”Java“。
:::

<!-- exclude-end -->

## 建立连接

<Tabs defaultValue="java">
<TabItem value="java" label="Java">

下面的代码是先从环境变量里面创建 JDBC URL，然后创建 `Connection` 这个 JDBC 连接标准对象。

```java
{{#include docs/examples/java/src/main/java/com/taos/example/ConnectCloudExample.java:connect}}
```

</TabItem>
<TabItem value="spring" label="Spring">

1. 定义一个名为 MeterMapper 的接口，它使用 MyBatis 框架在 TDengine 数据库的超级表和 Java 对象之间进行映射。

  ```java
  {{#include docs/examples/java/spring/src/main/java/com/taos/example/dao/MeterMapper.java:mybatis}}
  ```

2. 在“src/main/resources/mapper”中创建“MeterMapper.xml”，文件中添加以下 SQL 映射

```xml
{{#include docs/examples/java/spring/src/main/resources/mapper/MeterMapper.xml}}
```

3. 使用 Spring 进行更多查询和插入 TDengine Cloud 实例的示例代码，请参考 [Spring Example](https://github.com/taosdata/TDengine/tree/docs-cloud/docs/examples/java/spring/)

</TabItem>
</Tabs>

客户端连接建立连接以后，想了解更多写入数据和查询数据的内容，请参考 [写入](https://docs.taosdata.com/cloud/programming/insert/) 和 [查询](https://docs.taosdata.com/cloud/programming/query/)。

想知道更多通过 REST 接口写入数据的详情，请参考[REST 接口](https://docs.taosdata.com/cloud/programming/client-libraries/rest-api/)。
