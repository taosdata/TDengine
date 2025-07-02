---
sidebar_label: Ontop
title: 与 Ontop 集成
toc_max_heading_level: 5
---

[Ontop](https://ontop-vkg.org/) 是由意大利博尔扎诺自由大学的 KRDB 研究小组开发的开源虚拟知识图谱系统，它将任意关系数据库内容转化为知识图谱，这些图谱是虚拟的，数据仍然保留在数据源中。Ontop 通过 SPARQL（SPARQL Protocol and RDF Query Language）语言（W3C 制定）查询数据源中数据，并将结果转换为 RDF 格式。同时支持多种数据库，包括 MySQL、PostgreSQL、Oracle、SQL Server、SQLite 和 TDengine。

Ontop 通过 [TDengine Java connector](../../../reference/connector/java/) 连接 TDengine 数据源。

## 前置条件 

准备以下环境：

- TDengine 3.3.6.0 及以上版本集群已部署并正常运行（企业及社区版均可）。
- taosAdapter 能够正常运行，详细参考 [taosAdapter 参考手册](../../../reference/components/taosadapter)。
- Ontop 5.4.0 及以上版本（ [Spark 下载](https://spark.apache.org/downloads.html)）。
- JDBC 驱动 3.6.4 及以上版本。可从 [maven.org](https://central.sonatype.com/artifact/com.taosdata.jdbc/taos-jdbcdriver) 下载。

## 配置数据源
在 Ontop 中配置 TDengine 数据源需要以下步骤：
1. **安装 JDBC 驱动**：将下载的 JDBC 驱动包放入 Ontop 主程序所在的 `jdbc\` 目录下。
2. **配置 JDBC 驱动**：在 Ontop 的 .properties 文件中配置 JDBC WebSocket 方式连接信息，示例如下：
   ``` sql
    jdbc.url=jdbc:TAOS-WS://[host_name]:[port]/[database_name]
    jdbc.user=user
    jdbc.password=password
    jdbc.driver=com.taosdata.jdbc.ws.WebSocketDriver   
   ```  
   详细参数见：[URL 参数介绍](../../../reference/connector/java/#url-规范)。
3. **配置表映射**：在 .obda 文件中完成 TDengine 与 OnTop 表名与列名映射关系，以下是以智能电表为例，把超级表 meters 映射为 Ontop 对象 ns:Meters，各列同名映射：
   ``` properties
    [PrefixDeclaration]
    :   http://example.org/tde
    ns:  http://example.org/ns#


    [MappingDeclaration] @collection [[
    mappingId	meters-mapping
    target	ns:{ts} a ns:Meters ; ns:ts {ts} ; ns:voltage {voltage} ; ns:phase {phase} ; ns:groupid {groupid} ; ns:location {location}^^xsd:string .
    source	SELECT ts, voltage, phase, groupid, location  from test.meters
    ]]
   ```
   核心是 `source` 字段，指定了 TDengine 的查询语句，可以为任意 SQL 语句，支持复杂查询。
   `targe` 字段为各列映射为 Ontop 的名称对应，可以指定映射数据类型，如果不指定，按以下规则映射：

    | TDengine JDBC 数据类型 | Ontop 数据类型  |
    |:-------------------- |:----------------|
    | java.sql.Timestamp   | xsd:dateTime    |  
    | java.lang.Boolean    | xsd:boolean     |
    | java.lang.Byte       | xsd:byte        |
    | java.lang.Short      | xsd:short       |
    | java.lang.Integer    | xsd:int         |
    | java.lang.Long       | xsd:long        |
    | java.math.BigInteger | xsd:nonNegativeInteger |
    | java.lang.Float      | xsd:float       |
    | java.lang.Double     | xsd:double      |
    | byte[]               | xsd:base64Binary|
    | java.lang.String     | xsd:string      |
    | java.math.BigDecimal | xsd:decimal     |

   更多 .obda 文件格式细节请参考 [Ontop OBDA](https://ontop-vkg.org/guide/advanced/mapping-language.html)。

4. **测试连接**：完成以上两项配置工作后，即可启动 ontop endpoint 测试连接正确性。
   假设:
   - .properties 文件命名为 db.properties
   - .obda       文件命名为 db.obda
   - 在本机完成测试
   以上两文件所在目录启动 ontop endpoint，命令如下：
   ``` bash
   ontop endpoint -p db.properties -m db.obda
   ```
   启动成功后，访问 http://localhost:8080/ontop/endpoint/ 即可看到 Ontop SPARQL 查询界面后，表明连接及映射配置都正确。


## 数据分析

### 场景介绍
我们以智能电表为例，通过 Ontop 把智能电表数据转化为虚拟知识图谱数据（RDF），通过标准的虚拟知识图谱查询语言 SPARQL，查询电压超过 240V 的高负载电表设备。

### 数据准备
使用 taosBenchmark 工具直接生成 100 台智能电表，每台电表存放有 1000 条采集数据的模拟数据集。
``` bash
taosBenchmark -t 100 -n 1000 -y
```

### 数据源
创建数据源配置属性文件 db.properties，假设数据源在本机，使用默认账号登录，数据库名 test，配置内容如下：
``` sql
jdbc.url=jdbc:TAOS-WS://localhost:6041/test
jdbc.user=root
jdbc.password=taosdata
jdbc.driver=com.taosdata.jdbc.ws.WebSocketDriver   
```  

### 表映射
创建表映射表配置文件 db.obda，内容参照“配置数据源”->”配置表映射”中示例填写。

### 启动 Ontop
参照“配置数据源”->”测试连接”中方法启动 Ontop, 进入 SPARQL 查询界面。

### 查询分析
1. 制作 SPARQL 查询语句，查询电压超过 240V 的智能电表设备，并显示 2 条最高电压数据：
   ``` sparql
    PREFIX ns: <http://example.org/ns#>

    SELECT ?ts ?voltage ?phase ?groupid ?location
    WHERE {
        ?m a ns:Meters ;
                ns:ts ?ts;
                ns:voltage ?voltage;
                ns:phase ?phase;         
                ns:groupid ?groupid;
                ns:location ?location.
        FILTER(?voltage > 240)
    }
    ORDER BY DESC(?voltage)
    LIMIT 2
   ```
2. 在 SPARQL 查询界面输入以上语句，点击“Run”按钮， 查询结果如下：
   ![ontop-query](img/ontop-query.webp)

3. 查询结果以 SPARQL JSON 格式返回，包含了电表采集时间戳、采集电压、相位、分组 ID 及设备位置等信息。
   ``` json
    {
    "head" : {
        "vars" : [
        "ts",
        "voltage",
        "phase",
        "groupid",
        "location"
        ]
    },
    "results" : {
        "bindings" : [
        {
            "ts" : {
            "datatype" : "http://www.w3.org/2001/XMLSchema#dateTime",
            "type" : "literal",
            "value" : "2017-07-14T10:40:00.040"
            },
            "voltage" : {
            "datatype" : "http://www.w3.org/2001/XMLSchema#integer",
            "type" : "literal",
            "value" : "258"
            },
            "phase" : {
            "datatype" : "http://www.w3.org/2001/XMLSchema#double",
            "type" : "literal",
            "value" : "147.5"
            },
            "groupid" : {
            "datatype" : "http://www.w3.org/2001/XMLSchema#integer",
            "type" : "literal",
            "value" : "2"
            },
            "location" : {
            "type" : "literal",
            "value" : "California.PaloAlto"
            }
        },
        {
            "ts" : {
            "datatype" : "http://www.w3.org/2001/XMLSchema#dateTime",
            "type" : "literal",
            "value" : "2017-07-14T10:40:00.044"
            },
            "voltage" : {
            "datatype" : "http://www.w3.org/2001/XMLSchema#integer",
            "type" : "literal",
            "value" : "258"
            },
            "phase" : {
            "datatype" : "http://www.w3.org/2001/XMLSchema#double",
            "type" : "literal",
            "value" : "147.0"
            },
            "groupid" : {
            "datatype" : "http://www.w3.org/2001/XMLSchema#integer",
            "type" : "literal",
            "value" : "2"
            },
            "location" : {
            "type" : "literal",
            "value" : "California.PaloAlto"
            }
        }
        ]
    }
    }   
   ```