---
title: 参数绑定写入
sidebar_label: 参数绑定
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

通过参数绑定方式写入数据时，能避免SQL语法解析的资源消耗，从而显著提升写入性能。示例代码如下。

## Websocket 
<Tabs defaultValue="java" groupId="websocket">
<TabItem value="java" label="Java">

```java
public class WSParameterBindingBasicDemo {

    // modify host to your own
    private static final String host = "127.0.0.1";
    private static final Random random = new Random(System.currentTimeMillis());
    private static final int numOfSubTable = 10, numOfRow = 10;

    public static void main(String[] args) throws SQLException {

        String jdbcUrl = "jdbc:TAOS-RS://" + host + ":6041/?batchfetch=true";
        Connection conn = DriverManager.getConnection(jdbcUrl, "root", "taosdata");

        init(conn);

        String sql = "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)";

        try (TSWSPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                // set table name
                pstmt.setTableName("d_bind_" + i);

                // set tags
                pstmt.setTagInt(0, i);
                pstmt.setTagString(1, "location_" + i);

                // set columns
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++) {
                    pstmt.setTimestamp(1, new Timestamp(current + j));
                    pstmt.setFloat(2, random.nextFloat() * 30);
                    pstmt.setInt(3, random.nextInt(300));
                    pstmt.setFloat(4, random.nextFloat());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
        }

        conn.close();
    }

    private static void init(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS power");
            stmt.execute("USE power");
            stmt.execute("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        }
    }
}
```
</TabItem>
<TabItem label="Python" value="python">

    ```python
    {{#include docs/examples/python/connect_websocket_examples.py:connect}}
    ```
</TabItem>
<TabItem label="Go" value="go">

</TabItem>
<TabItem label="Rust" value="rust">

</TabItem>
<TabItem label="Node.js" value="node">

    ```js
        {{#include docs/examples/node/websocketexample/sql_example.js:createConnect}}
    ```
</TabItem>
<TabItem label="C#" value="csharp">

</TabItem>
<TabItem label="R" value="r">

</TabItem>
<TabItem label="C" value="c">

</TabItem>
<TabItem label="PHP" value="php">

</TabItem>
</Tabs>

## 原生
<Tabs  defaultValue="java"  groupId="native">
<TabItem label="Java" value="java">

</TabItem>
<TabItem label="Python" value="python">

</TabItem>
<TabItem label="Go" value="go">

</TabItem>
<TabItem label="Rust" value="rust">

</TabItem>
<TabItem label="C#" value="csharp">

</TabItem>
<TabItem label="R" value="r">

</TabItem>
<TabItem label="C" value="c">

</TabItem>
<TabItem label="PHP" value="php">

</TabItem>

</Tabs>