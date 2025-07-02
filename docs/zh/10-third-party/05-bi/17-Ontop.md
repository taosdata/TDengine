---
sidebar_label: Ontop
title: 与 Ontop 集成
toc_max_heading_level: 5
---

[Ontop](https://ontop-vkg.org/) 是由意大利博尔扎诺自由大学 KRDB 研究小组开发的开源虚拟知识图谱系统。它能够将关系数据库内容动态转化为知识图谱，数据无需迁移仍保留在原始数据源中。Ontop 通过 SPARQL（W3C 制定的 RDF 查询语言）查询数据库，并将结果转换为 RDF 格式。支持包括 MySQL、PostgreSQL、Oracle、SQL Server、SQLite 和 **TDengine** 在内的多种数据库。

:::tip 核心价值
通过 Ontop + TDengine 的组合，您可以直接使用标准 SPARQL 查询时序数据库中的物联网数据，无需额外数据迁移。
:::

## 前置条件 

Ontop 通过 [TDengine Java connector](../../../reference/connector/java/) 连接 TDengine 数据源，需准备以下环境：

- TDengine 3.3.6.0 及以上版本集群已部署并正常运行（企业及社区版均可）。
- taosAdapter 能够正常运行，详细参考 [taosAdapter 参考手册](../../../reference/components/taosadapter)。
- Ontop 5.4.0 及以上版本（ [Spark 下载](https://spark.apache.org/downloads.html)）。
- JDBC 驱动 3.6.4 及以上版本。可从 [maven.org](https://central.sonatype.com/artifact/com.taosdata.jdbc/taos-jdbcdriver) 下载。

## 配置数据源
在 Ontop 中配置 TDengine 数据源需要以下步骤：
1. **安装 JDBC 驱动**：将下载的 JDBC 驱动包（`.jar`文件）置于 Ontop 主程序的 `jdbc/` 目录下。
2. **配置 JDBC 驱动**：在 Ontop 的 `.properties` 文件中配置 JDBC 连接信息：
   ``` sql
    jdbc.url = jdbc:TAOS-WS://[host]:[port]/[database]
    jdbc.user = [用户名]
    jdbc.password = [密码]
    jdbc.driver = com.taosdata.jdbc.ws.WebSocketDriver  
   ```  
   URL 参数详情参阅：[TDengine URL 规范](../../../reference/connector/java/#url-规范)。
3. **配置表映射**：在 .obda 文件中定义 TDengine 与 Ontop 的映射关系（以智能电表场景为例）：
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
   示例把超级表 meters 映射为 Ontop 对象 ns:Meters，各列同名映射。
   
    | 关键字段  | 说明  |
    |:-------  |:----------------------------------- |
    | source   | TDengine SQL 查询语句（支持复杂查询）   |  
    | target   | 字段映射关系（未指定类型时按默认规则转换） |
   

   **数据类型默认映射规则**

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

   完整 .obda 文件格式介绍请参考 [Ontop OBDA 文档](https://ontop-vkg.org/guide/advanced/mapping-language.html)。

4. **测试连接**
   启动 Ontop 端点服务验证配置：
   ``` bash
   ontop endpoint -p db.properties -m db.obda
   ```
   访问 http://localhost:8080/ontop/endpoint/，若显示 SPARQL 查询界面，则表示配置成功。 


## 数据分析

### 场景介绍
使用 Ontop 将 TDengine 中的智能电表数据转化为虚拟知识图谱，通过 SPARQL 查询电压超过 240V 的高负载设备。

### 数据准备
通过 taosBenchmark 生成模拟数据：
``` bash
# 生成 100 台设备，每台 1000 条记录
taosBenchmark -t 100 -n 1000 -y
```

### 配置文件
**db.properties**​（连接配置）：
``` sql
jdbc.url=jdbc:TAOS-WS://localhost:6041/test
jdbc.user=root
jdbc.password=taosdata
jdbc.driver=com.taosdata.jdbc.ws.WebSocketDriver   
```  
**db.obda** （映射配置）：
复用 2.3 节示例内容。


### 执行查询
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
2. 在 SPARQL 查询界面输入上述语句，点击“运行”按钮，查询结果如下：
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

## 总结
本文介绍了如何将 TDengine 与 Ontop 集成，实现了：
- 时序数据语义化查询：将 TDengine 中的时序数据转换为 RDF 格式，支持 SPARQL 语义查询。
- 标准化数据访问：通过 W3C 标准 SPARQL 语言，提供统一的数据访问接口。
- 知识图谱应用：为工业 IoT 场景提供了强大的语义分析和知识推理能力。
Ontop 与 TDengine 集成为企业构建智能化时序数据分析平台提供了新的技术路径，大模型语言泛化能力结合知识图谱逻辑推理能力，工业物联网复杂智能化查询应用场景提供一片新天地。