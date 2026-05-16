#  虚拟表 TS

## 1. 测试目标

JIRA: [TS-4897](https://jira.taosdata.com:18080/browse/TS-4897)
1. 测试虚拟表的创建删除等功能是否可以正常使用。
2. 测试虚拟表的权限控制是否符合预期。
3. 测试查询虚拟表的结果是否符合预期。
4. 测试修改虚拟表的数据源、列信息后是否可以正常使用。
5. 测试虚拟表的性能。

## 2. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2024/12/3 | 0.1 | @司马靖 | 初稿 |
|  |  |  |  |

## 3. 测试范围

### 3.1 功能测试

1. 测试虚拟表的创建删除。
2. 测试虚拟表的权限控制。
3. 测试虚拟表的查询。
4. 测试虚拟表的修改（修改表 schema/修改数据源）。
5. 测试虚拟表的错误场景。
6. 测试虚拟表元数据查询

### 3.2 性能测试

1. 测试虚拟表创建的性能。
2. 测试虚拟表查询的性能。

## 4. 测试结论

TBD

## 5. 已知问题和限制

1. 本版不包括填充函数
2. 本版不支持将表达式下推到每个原始表的查询，而是将原始表中的数据汇总后再进行过滤和计算，性能较差。
3. 本版不支持查询虚拟超级表，只支持查询虚拟子表和虚拟普通表。

## 6. 测试环境

- OS: Windows, Linux, macOS
- 版本：企业版

## 7. 测试数据

### 7.1 功能测试数据

#### 7.1.1 创建原始表（子表） json

```json
{
  "filetype": "insert",
  "cfgdir": "/etc/taos",
  "host": "127.0.0.1",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "connection_pool_size": 8,
  "thread_count": 4,
  "create_table_thread_count": 4,
  "result_file": "./insert_res.txt",
  "confirm_parameter_prompt": "no",
  "num_of_records_per_req": 10000,
  "prepared_rand": 10000,
  "chinese": "no",
  "escape_character": "yes",
  "continue_if_fail": "no",
  "databases": [
    {
      "dbinfo": {
        "name": "testvtable",
        "drop": "yes",
        "vgroups": 4,
        "precision": "ms"
      },
      "super_tables": [
        {
          "name": "stbstbstb",
          "child_table_exists": "no",
          "childtable_count": 1,
          "childtable_prefix": "dbool",
          "auto_create_table": "no",
          "batch_create_tbl_num": 5,
          "data_source": "rand",
          "insert_mode": "taosc",
          "non_stop_mode": "no",
          "line_protocol": "line",
          "insert_rows": 10000,
          "childtable_limit": 0,
          "childtable_offset": 0,
          "interlace_rows": 0,
          "insert_interval": 0,
          "partial_col_num": 0,
          "timestamp_step": 5,
          "start_timestamp": "2020-10-01 00:00:00.000",
          "sample_format": "csv",
          "sample_file": "./sample.csv",
          "use_sample_ts": "no",
          "tags_file": "",
          "columns": [
            {"type": "utinyint", "name": "u_tinyint_col", "count": 1, "max": 255, "min": 0 },
            {"type": "usmallint", "name": "u_smallint_col", "count": 1, "max": 65535, "min": 0 },
            {"type": "uint", "name": "u_int_col", "count": 1, "max": 4294967295, "min": 0 },
            {"type": "ubigint", "name": "u_bigint_col", "count": 1, "max": 18446744073709551615, "min": 0 },
            {"type": "tinyint", "name": "tinyint_col", "count": 1, "max": 127, "min": -128 },
            {"type": "smallint", "name": "smallint_col", "count": 1, "max": 32767, "min": -32768 },
            {"type": "int", "name": "int_col", "count": 1, "max": 2147483647, "min": -2147483648 },
            {"type": "bigint", "name": "bigint_col", "count": 1, "max": 9223372036854775807, "min": -9223372036854775808 },
            {"type": "float", "name": "float_col", "count": 1, "max": 100000, "min": -100000 },
            {"type": "double", "name": "double_col", "count": 1, "max": 100000000, "min": -100000000 },
            {"type": "bool", "name": "bool_col", "count": 1, "max": 1, "min": 0 },
            {"type": "binary", "name": "binary_16_col", "len": 16,
              "values": ["San Francisco", "Los Angles", "San Diego",
              "San Jose", "Palo Alto", "Campbell", "Mountain View",
              "Sunnyvale", "Santa Clara", "Cupertino"] },
            {"type": "binary", "name": "binary_32_col", "len": 32,
              "values": ["Beijing - San Francisco", "Shanghai - Los Angles", "Hangzhou - San Diego",
                "Chengdu - San Jose", "Hong Kong - Palo Alto", "Harbin - Campbell", "Tianjin - Mountain View",
                "Xian - Sunnyvale", "Taiyuan - Santa Clara", "Shijiazhuang - Cupertino"] },
            {"type": "nchar", "name": "nchar_16_col", "len": 16,
              "values": ["一。San Francisco", "二。Los Angles", "三。San Diego",
                "四。San Jose", "五。Palo Alto", "六。Campbell", "七。Mountain View",
                "八。Sunnyvale", "九。Santa Clara", "十。Cupertino"] },
            {"type": "nchar", "name": "nchar_32_col", "len": 32,
              "values": ["旧金山 - San Francisco", "洛杉矶 - Los Angles", "圣地亚哥 - San Diego",
                "圣何塞 - San Jose", "帕洛阿托 - Palo Alto", "坎贝尔 - Campbell", "山景城 - Mountain View",
                "森尼韦尔 - Sunnyvale", "圣克拉拉 - Santa Clara", "库比蒂诺 - Cupertino"] }
          ],
          "tags": [
            {"type": "TINYINT", "name": "groupid", "max": 10, "min": 1},
            {"type": "BINARY",  "name": "location", "len": 16,
              "values": ["San Francisco", "Los Angles", "San Diego",
                "San Jose", "Palo Alto", "Campbell", "Mountain View",
                "Sunnyvale", "Santa Clara", "Cupertino"]
            }
          ]
        }
      ]
    }
  ]
}
```

#### 7.1.2 创建原始表（普通表）

和 7.1 中的原始子表结构相同，数据相同，唯一不同只有表类型不同。

### 7.2 性能测试数据

#### 7.2.1 创建原始表（子表） json

```json
{
  "filetype": "insert",
  "cfgdir": "/etc/taos",
  "host": "127.0.0.1",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "connection_pool_size": 8,
  "thread_count": 4,
  "create_table_thread_count": 4,
  "result_file": "./insert_res.txt",
  "confirm_parameter_prompt": "no",
  "num_of_records_per_req": 10000,
  "prepared_rand": 10000,
  "chinese": "no",
  "escape_character": "yes",
  "continue_if_fail": "no",
  "databases": [
    {
      "dbinfo": {
        "name": "testvtable",
        "drop": "yes",
        "vgroups": 4,
        "precision": "ms"
      },
      "super_tables": [
        {
          "name": "stbstbstb",
          "child_table_exists": "no",
          "childtable_count": 1,
          "childtable_prefix": "dbool",
          "auto_create_table": "no",
          "batch_create_tbl_num": 5,
          "data_source": "rand",
          "insert_mode": "taosc",
          "non_stop_mode": "no",
          "line_protocol": "line",
          "insert_rows": 10000,
          "childtable_limit": 0,
          "childtable_offset": 0,
          "interlace_rows": 0,
          "insert_interval": 0,
          "partial_col_num": 0,
          "timestamp_step": 5,
          "start_timestamp": "2020-10-01 00:00:00.000",
          "sample_format": "csv",
          "sample_file": "./sample.csv",
          "use_sample_ts": "no",
          "tags_file": "",
          "columns": [
            {"type": "utinyint", "name": "u_tinyint_col", "count": 1, "max": 255, "min": 0 },
            {"type": "usmallint", "name": "u_smallint_col", "count": 1, "max": 65535, "min": 0 },
            {"type": "uint", "name": "u_int_col", "count": 1, "max": 4294967295, "min": 0 },
            {"type": "ubigint", "name": "u_bigint_col", "count": 1, "max": 18446744073709551615, "min": 0 },
            {"type": "tinyint", "name": "tinyint_col", "count": 1, "max": 127, "min": -128 },
            {"type": "smallint", "name": "smallint_col", "count": 1, "max": 32767, "min": -32768 },
            {"type": "int", "name": "int_col", "count": 1, "max": 2147483647, "min": -2147483648 },
            {"type": "bigint", "name": "bigint_col", "count": 1, "max": 9223372036854775807, "min": -9223372036854775808 },
            {"type": "float", "name": "float_col", "count": 1, "max": 100000, "min": -100000 },
            {"type": "double", "name": "double_col", "count": 1, "max": 100000000, "min": -100000000 },
            {"type": "bool", "name": "bool_col", "count": 1, "max": 1, "min": 0 },
            {"type": "binary", "name": "binary_16_col", "len": 16,
              "values": ["San Francisco", "Los Angles", "San Diego",
              "San Jose", "Palo Alto", "Campbell", "Mountain View",
              "Sunnyvale", "Santa Clara", "Cupertino"] },
            {"type": "binary", "name": "binary_32_col", "len": 32,
              "values": ["Beijing - San Francisco", "Shanghai - Los Angles", "Hangzhou - San Diego",
                "Chengdu - San Jose", "Hong Kong - Palo Alto", "Harbin - Campbell", "Tianjin - Mountain View",
                "Xian - Sunnyvale", "Taiyuan - Santa Clara", "Shijiazhuang - Cupertino"] },
            {"type": "nchar", "name": "nchar_16_col", "len": 16,
              "values": ["一。San Francisco", "二。Los Angles", "三。San Diego",
                "四。San Jose", "五。Palo Alto", "六。Campbell", "七。Mountain View",
                "八。Sunnyvale", "九。Santa Clara", "十。Cupertino"] },
            {"type": "nchar", "name": "nchar_32_col", "len": 32,
              "values": ["旧金山 - San Francisco", "洛杉矶 - Los Angles", "圣地亚哥 - San Diego",
                "圣何塞 - San Jose", "帕洛阿托 - Palo Alto", "坎贝尔 - Campbell", "山景城 - Mountain View",
                "森尼韦尔 - Sunnyvale", "圣克拉拉 - Santa Clara", "库比蒂诺 - Cupertino"] }
          ],
          "tags": [
            {"type": "TINYINT", "name": "groupid", "max": 10, "min": 1},
            {"type": "BINARY",  "name": "location", "len": 16,
              "values": ["San Francisco", "Los Angles", "San Diego",
                "San Jose", "Palo Alto", "Campbell", "Mountain View",
                "Sunnyvale", "Santa Clara", "Cupertino"]
            }
          ]
        }
      ]
    }
  ]
}
```

## 8. 测试用例

### 8.1 功能

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 测试虚拟表的创建（正确情况） | 创建数据库以及原始表 | test_vtable_create
use test_vtable_create;

tdSql.execute(f"CREATE STABLE `vtb_org_stb` ("
              "ts timestamp, "
              "u_tinyint_col tinyint unsigned, "
              "u_smallint_col smallint unsigned, "
              "u_int_col int unsigned, "
              "u_bigint_col bigint unsigned, "
              "tinyint_col tinyint, "
              "smallint_col smallint, "
              "int_col int, "
              "bigint_col bigint, "
              "float_col float, "
              "double_col double, "
              "bool_col bool, "
              "binary_16_col binary(16),"
              "binary_32_col binary(32),"
              "nchar_16_col nchar(16),"
              "nchar_32_col nchar(32),"
              "varbinary_16_col varbinary(16),"
              "varbinary_32_col varbinary(32),"
              "geo_16_col geometry(16),"
              "geo_32_col geometry(32)"
              ") TAGS ("
              "int_tag int,"
              "bool_tag bool,"
              "float_tag float,"
              "double_tag double,"
              "nchar_32_tag nchar(32),"
              "binary_32_tag binary(32))")

for i in range(30):
    tdSql.execute(f"CREATE TABLE `vtb_org_child_{i}` USING `vtb_org_stb` TAGS ({i}, false, {i}, {i}, 'child{i}', 'child{i}');")

for i in range(30):
    tdSql.execute(f"CREATE TABLE `vtb_org_normal_{i}` (ts timestamp, u_tinyint_col tinyint unsigned, u_smallint_col smallint unsigned, u_int_col int unsigned, u_bigint_col bigint unsigned, tinyint_col tinyint, smallint_col smallint, int_col int, bigint_col bigint, float_col float, double_col double, bool_col bool, binary_16_col binary(16), binary_32_col binary(32), nchar_16_col nchar(16), nchar_32_col nchar(32), varbinary_16_col varbinary(16), varbinary_32_col varbinary(32), geo_16_col geometry(16), geo_32_col geometry(32))") | 成功 | Y | Pass |  |
|  | 创建虚拟子表对应的超级表 | "CREATE STABLE `vtb_virtual_stb` ("
              "ts timestamp, "
              "u_tinyint_col tinyint unsigned, "
              "u_smallint_col smallint unsigned, "
              "u_int_col int unsigned, "
              "u_bigint_col bigint unsigned, "
              "tinyint_col tinyint, "
              "smallint_col smallint, "
              "int_col int, "
              "bigint_col bigint, "
              "float_col float, "
              "double_col double, "
              "bool_col bool, "
              "binary_16_col binary(16),"
              "binary_32_col binary(32),"
              "nchar_16_col nchar(16),"
              "nchar_32_col nchar(32),"
              "varbinary_16_col varbinary(16),"
              "varbinary_32_col varbinary(32),"
              "geo_16_col geometry(16),"
              "geo_32_col geometry(32)"
              ") TAGS ("
              "int_tag int,"
              "bool_tag bool,"
              "float_tag float,"
              "double_tag double,"
              "nchar_32_tag nchar(32),"
              "binary_32_tag binary(32))" 
              "VIRTUAL 1" | 成功 | Y | Pass |  |
|  | 创建虚拟子表（不用 from 指定/指定全部列） | "CREATE VTABLE `vtb_virtual_ctb3`("
              "vtb_org_child_0.u_tinyint_col, "
              "vtb_org_child_1.u_smallint_col, "
              "vtb_org_child_2.u_int_col, "
              "vtb_org_child_3.u_bigint_col,"
              "vtb_org_child_4.tinyint_col, "
              "vtb_org_child_5.smallint_col, "
              "vtb_org_child_6.int_col, "
              "vtb_org_child_7.bigint_col,"
              "vtb_org_child_8.float_col, "
              "vtb_org_child_9.double_col, "
              "vtb_org_child_10.bool_col, "
              "vtb_org_child_11.binary_16_col,"
              "vtb_org_child_12.binary_32_col, "
              "vtb_org_child_13.nchar_16_col, "
              "vtb_org_child_14.nchar_32_col,"
              "vtb_org_child_15.varbinary_16_col, "
              "vtb_org_child_16.varbinary_32_col, "
              "vtb_org_child_17.geo_16_col, "
              "vtb_org_child_18.geo_32_col) USING vtb_virtual_stb TAGS (3, false, 3, 3, 'vchild3', 'vchild3')" | 成功 | Y | Pass |  |
|  | 创建虚拟子表（使用 from 指定/指定全部列） | "CREATE VTABLE `vtb_virtual_ctb9`("
              "u_tinyint_col FROM vtb_org_child_0.u_tinyint_col, "
              "u_smallint_col FROM vtb_org_child_1.u_smallint_col, "
              "u_int_col FROM vtb_org_child_2.u_int_col, "
              "u_bigint_col FROM vtb_org_child_3.u_bigint_col,"
              "tinyint_col FROM vtb_org_child_4.tinyint_col, "
              "smallint_col FROM vtb_org_child_5.smallint_col, "
              "int_col FROM vtb_org_child_6.int_col, "
              "bigint_col FROM vtb_org_child_7.bigint_col,"
              "float_col FROM vtb_org_child_8.float_col, "
              "double_col FROM vtb_org_child_9.double_col, "
              "bool_col FROM vtb_org_child_10.bool_col, "
              "binary_16_col FROM vtb_org_child_11.binary_16_col,"
              "binary_32_col FROM vtb_org_child_12.binary_32_col, "
              "nchar_16_col FROM vtb_org_child_13.nchar_16_col, "
              "nchar_32_col FROM vtb_org_child_14.nchar_32_col,"
              "varbinary_16_col FROM vtb_org_child_15.varbinary_16_col, "
              "varbinary_32_col FROM vtb_org_child_16.varbinary_32_col, "
              "geo_16_col FROM vtb_org_child_17.geo_16_col, "
              "geo_32_col FROM vtb_org_child_18.geo_32_col) USING vtb_virtual_stb TAGS (9, false, 9, 9, 'vchild9', 'vchild9')" | 成功 | Y | Pass |  |
|  | 创建虚拟子表（使用 from 指定/指定部分列） | "CREATE VTABLE `vtb_virtual_ctb6`("
              "u_tinyint_col FROM vtb_org_child_0.u_tinyint_col, "
              "u_bigint_col FROM vtb_org_child_3.u_bigint_col,"
              "int_col FROM vtb_org_child_6.int_col,"
              "float_col FROM vtb_org_child_8.float_col,"
              "bool_col FROM vtb_org_child_10.bool_col,"
              "binary_32_col FROM vtb_org_child_12.binary_32_col) USING vtb_virtual_stb  TAGS (6, false, 6, 6, 'vchild6', 'vchild6')" | 成功 | Y | Pass |  |
|  | 创建虚拟子表（不用 from 指定/指定部分列） | "CREATE VTABLE `vtb_virtual_ctb0`("
              "vtb_org_child_0.u_tinyint_col, "
              "vtb_org_child_1.u_smallint_col, "
              "vtb_org_child_2.u_int_col, "
              "vtb_org_child_3.u_bigint_col,"
              "vtb_org_child_4.tinyint_col) USING vtb_virtual_stb TAGS (0, false, 0, 0, 'vchild0', 'vchild0')" | 成功 | Y | Pass |  |
|  | 创建虚拟子表（不指定数据来源） | CREATE VTABLE `vtb_virtual_ctb15` USING vtb_virtual_stb TAGS (15, false, 15, 15, 'vchild15', 'vchild15'); | 成功 | Y | Pass |  |
|  | 创建虚拟子表（从数据源指定 tag） | create vtable test_child_vtb5 USING vtb_stb_virtual tags(dbool5.device); | 成功 | Y |  | 暂不支持 |
|  | 创建虚拟普通表（指定全部列） | "CREATE VTABLE `vtb_virtual_ntb3` ("
              "ts timestamp, "
              "u_tinyint_col tinyint unsigned from vtb_org_child_0.u_tinyint_col, "
              "u_smallint_col smallint unsigned from vtb_org_child_1.u_smallint_col, "
              "u_int_col int unsigned from vtb_org_child_2.u_int_col, "
              "u_bigint_col bigint unsigned from vtb_org_child_3.u_bigint_col, "
              "tinyint_col tinyint from vtb_org_child_4.tinyint_col, "
              "smallint_col smallint from vtb_org_child_5.smallint_col, "
              "int_col int from vtb_org_child_6.int_col, "
              "bigint_col bigint from vtb_org_child_7.bigint_col, "
              "float_col float from vtb_org_child_8.float_col, "
              "double_col double from vtb_org_child_9.double_col, "
              "bool_col bool from vtb_org_child_10.bool_col, "
              "binary_16_col binary(16) from vtb_org_child_11.binary_16_col,"
              "binary_32_col binary(32) from vtb_org_child_12.binary_32_col,"
              "nchar_16_col nchar(16) from vtb_org_child_13.nchar_16_col,"
              "nchar_32_col nchar(32) from vtb_org_child_14.nchar_32_col,"
              "varbinary_16_col varbinary(16) from vtb_org_child_15.varbinary_16_col,"
              "varbinary_32_col varbinary(32) from vtb_org_child_16.varbinary_32_col,"
              "geo_16_col geometry(16) from vtb_org_child_17.geo_16_col,"
              "geo_32_col geometry(32) from vtb_org_child_18.geo_32_col)" | 成功 | Y | Pass |  |
|  | 创建虚拟普通表（指定部分列） | "CREATE VTABLE `vtb_virtual_ntb0` ("
              "ts timestamp, "
              "u_tinyint_col tinyint unsigned from vtb_org_child_0.u_tinyint_col, "
              "u_smallint_col smallint unsigned from vtb_org_child_1.u_smallint_col, "
              "u_int_col int unsigned, "
              "u_bigint_col bigint unsigned from vtb_org_child_3.u_bigint_col, "
              "tinyint_col tinyint from vtb_org_child_4.tinyint_col, "
              "smallint_col smallint, "
              "int_col int, "
              "bigint_col bigint, "
              "float_col float from vtb_org_child_8.float_col, "
              "double_col double from vtb_org_child_9.double_col, "
              "bool_col bool from vtb_org_child_10.bool_col, "
              "binary_16_col binary(16),"
              "binary_32_col binary(32),"
              "nchar_16_col nchar(16),"
              "nchar_32_col nchar(32) from vtb_org_child_14.nchar_32_col,"
              "varbinary_16_col varbinary(16),"
              "varbinary_32_col varbinary(32),"
              "geo_16_col geometry(16) from vtb_org_child_17.geo_16_col,"
              "geo_32_col geometry(32) from vtb_org_child_18.geo_32_col)" | 成功 | Y | Pass |  |
|  | 创建虚拟普通表（不指定数据源） | "CREATE VTABLE `vtb_virtual_ntb6` ("
              "ts timestamp, "
              "u_tinyint_col tinyint unsigned, "
              "u_smallint_col smallint unsigned, "
              "u_int_col int unsigned, "
              "u_bigint_col bigint unsigned, "
              "tinyint_col tinyint, "
              "smallint_col smallint, "
              "int_col int, "
              "bigint_col bigint, "
              "float_col float, "
              "double_col double, "
              "bool_col bool, "
              "binary_16_col binary(16),"
              "binary_32_col binary(32),"
              "nchar_16_col nchar(16),"
              "nchar_32_col nchar(32),"
              "varbinary_16_col varbinary(16),"
              "varbinary_32_col varbinary(32),"
              "geo_16_col geometry(16),"
              "geo_32_col geometry(32))" | 成功 | Y | Pass |  |
| 测试虚拟表的创建（错误情况） | 在非虚拟超级表下创建虚拟子表 | "CREATE VTABLE `error_vtb_virtual_ctb0`("
            "vtb_org_child_0.u_tinyint_col, "
            "vtb_org_child_1.u_smallint_col, "
            "vtb_org_child_2.u_int_col, "
            "vtb_org_child_3.u_bigint_col,"
            "vtb_org_child_4.tinyint_col) USING vtb_org_stb TAGS (0, false, 0, 0, 'vchild0', 'vchild0')" | 失败 | Y | Pass |  |
|  | 在虚拟超级表下创建非虚拟子表 | CREATE TABLE `error_vtb_virtual_ctb1` USING vtb_virtual_stb TAGS (1, false, 1, 1, 'vchild1', 'vchild1') | 失败 | Y | Pass |  |
|  | 虚拟表定义与数据源数据类型不同 | create vtable test_error_data_type_child_vtb0(
        dint0.int_col) 
        USING vtb_stb_virtual tags('d0');

create vtable test_error_data_type_child_vtb1(
        doucol from dint1.int_col) 
        USING vtb_stb_virtual tags('d1');

create vtable test_error_data_type_normal_vtb0(
        ts timestamp,
        boolcol bool from ddouble6.double_col,
        intcol int,
        doucol double from ddouble6.double_col,
        bincol varchar(16),
        flocol float from dchar6.float_col
); | 失败 | Y | Pass |  |
|  | 给主键时间戳列设置数据源 | "CREATE VTABLE `error_vtb_virtual_ctb5`("
            "ts timestamp FROM vtb_org_child_0.ts, "
            "u_tinyint_col FROM vtb_org_child_0.u_tinyint_col, "
            "u_bigint_col FROM vtb_org_child_3.u_bigint_col,"
            "int_col FROM vtb_org_child_6.int_col,"
            "float_col FROM vtb_org_child_8.float_col,"
            "bool_col FROM vtb_org_child_10.bool_col,"
            "binary_32_col FROM vtb_org_child_12.binary_32_col) USING vtb_virtual_stb TAGS (5, false, 5, 5, 'vchild5', 'vchild5')"

"CREATE VTABLE `error_vtb_virtual_ntb1` ("
            "ts timestamp FROM vtb_org_normal_0.ts, "
            "u_tinyint_col tinyint unsigned from vtb_org_normal_0.u_tinyint_col, "
            "u_smallint_col smallint unsigned from vtb_org_normal_1.u_smallint_col, "
            "u_int_col int unsigned)" | 失败 | Y | Pass |  |
|  | 设置的数据源不存在 | "CREATE VTABLE `error_vtb_virtual_ctb6`("
            "u_tinyint_col FROM vtb_org_child_0.not_exists_col"
            ") USING vtb_virtual_stb TAGS (6, false, 6, 6, 'vchild6', 'vchild6')"

"CREATE VTABLE `error_vtb_virtual_ntb2` ("
            "ts timestamp, "
            "u_tinyint_col tinyint unsigned from vtb_org_child_0.not_exists_col)" | 失败 | Y | Pass |  |
| 测试虚拟表的查询 | 准备数据以及表 | create database test_vtable_select;
"CREATE STABLE `vtb_org_stb` ("
              "ts timestamp, "
              "u_tinyint_col tinyint unsigned, "
              "u_smallint_col smallint unsigned, "
              "u_int_col int unsigned, "
              "u_bigint_col bigint unsigned, "
              "tinyint_col tinyint, "
              "smallint_col smallint, "
              "int_col int, "
              "bigint_col bigint, "
              "float_col float, "
              "double_col double, "
              "bool_col bool, "
              "binary_16_col binary(16),"
              "binary_32_col binary(32),"
              "nchar_16_col nchar(16),"
              "nchar_32_col nchar(32)"
              ") TAGS ("
              "int_tag int,"
              "bool_tag bool,"
              "float_tag float,"
              "double_tag double,"
              "nchar_32_tag nchar(32),"
              "binary_32_tag binary(32))"

for i in range(15):
    tdSql.execute(f"CREATE TABLE `vtb_org_child_{i}` USING `vtb_org_stb` TAGS ({i}, false, {i}, {i}, 'child{i}', 'child{i}');")

for i in range(15):
    tdSql.execute(f"CREATE TABLE `vtb_org_normal_{i}` (ts timestamp, u_tinyint_col tinyint unsigned, u_smallint_col smallint unsigned, u_int_col int unsigned, u_bigint_col bigint unsigned, tinyint_col tinyint, smallint_col smallint, int_col int, bigint_col bigint, float_col float, double_col double, bool_col bool, binary_16_col binary(16), binary_32_col binary(32), nchar_16_col nchar(16), nchar_32_col nchar(32)) SMA(u_tinyint_col)")

CREATE VTABLE `vtb_virtual_ntb_full` ("
              "ts timestamp, "
              "u_tinyint_col tinyint unsigned from vtb_org_normal_0.u_tinyint_col, "
              "u_smallint_col smallint unsigned from vtb_org_normal_1.u_smallint_col, "
              "u_int_col int unsigned from vtb_org_normal_2.u_int_col, "
              "u_bigint_col bigint unsigned from vtb_org_normal_0.u_bigint_col, "
              "tinyint_col tinyint from vtb_org_normal_1.tinyint_col, "
              "smallint_col smallint from vtb_org_normal_2.smallint_col, "
              "int_col int from vtb_org_normal_0.int_col, "
              "bigint_col bigint from vtb_org_normal_1.bigint_col, "
              "float_col float from vtb_org_normal_2.float_col, "
              "double_col double from vtb_org_normal_0.double_col, "
              "bool_col bool from vtb_org_normal_1.bool_col, "
              "binary_16_col binary(16) from vtb_org_normal_2.binary_16_col,"
              "binary_32_col binary(32) from vtb_org_normal_0.binary_32_col,"
              "nchar_16_col nchar(16) from vtb_org_normal_1.nchar_16_col,"
              "nchar_32_col nchar(32) from vtb_org_normal_2.nchar_32_col)

"CREATE VTABLE `vtb_virtual_ntb_half_full` ("
              "ts timestamp, "
              "u_tinyint_col tinyint unsigned from vtb_org_normal_0.u_tinyint_col, "
              "u_smallint_col smallint unsigned from vtb_org_normal_1.u_smallint_col, "
              "u_int_col int unsigned from vtb_org_normal_2.u_int_col, "
              "u_bigint_col bigint unsigned, "
              "tinyint_col tinyint, "
              "smallint_col smallint, "
              "int_col int from vtb_org_normal_0.int_col, "
              "bigint_col bigint from vtb_org_normal_1.bigint_col, "
              "float_col float from vtb_org_normal_2.float_col, "
              "double_col double, "
              "bool_col bool, "
              "binary_16_col binary(16),"
              "binary_32_col binary(32) from vtb_org_normal_0.binary_32_col,"
              "nchar_16_col nchar(16) from vtb_org_normal_1.nchar_16_col,"
              "nchar_32_col nchar(32) from vtb_org_normal_2.nchar_32_col)"

"CREATE VTABLE `vtb_virtual_ntb_empty` ("
              "ts timestamp, "
              "u_tinyint_col tinyint unsigned, "
              "u_smallint_col smallint unsigned, "
              "u_int_col int unsigned, "
              "u_bigint_col bigint unsigned, "
              "tinyint_col tinyint, "
              "smallint_col smallint, "
              "int_col int, "
              "bigint_col bigint, "
              "float_col float, "
              "double_col double, "
              "bool_col bool, "
              "binary_16_col binary(16),"
              "binary_32_col binary(32),"
              "nchar_16_col nchar(16),"
              "nchar_32_col nchar(32))"

"CREATE STABLE `vtb_virtual_stb` ("
              "ts timestamp, "
              "u_tinyint_col tinyint unsigned, "
              "u_smallint_col smallint unsigned, "
              "u_int_col int unsigned, "
              "u_bigint_col bigint unsigned, "
              "tinyint_col tinyint, "
              "smallint_col smallint, "
              "int_col int, "
              "bigint_col bigint, "
              "float_col float, "
              "double_col double, "
              "bool_col bool, "
              "binary_16_col binary(16),"
              "binary_32_col binary(32),"
              "nchar_16_col nchar(16),"
              "nchar_32_col nchar(32)"
              ") TAGS ("
              "int_tag int,"
              "bool_tag bool,"
              "float_tag float,"
              "double_tag double,"
              "nchar_32_tag nchar(32),"
              "binary_32_tag binary(32))"
              "VIRTUAL 1"

"CREATE VTABLE `vtb_virtual_ctb_full` ("
              "u_tinyint_col from vtb_org_normal_0.u_tinyint_col, "
              "u_smallint_col from vtb_org_normal_1.u_smallint_col, "
              "u_int_col from vtb_org_normal_2.u_int_col, "
              "u_bigint_col from vtb_org_normal_0.u_bigint_col, "
              "tinyint_col from vtb_org_normal_1.tinyint_col, "
              "smallint_col from vtb_org_normal_2.smallint_col, "
              "int_col from vtb_org_normal_0.int_col, "
              "bigint_col from vtb_org_normal_1.bigint_col, "
              "float_col from vtb_org_normal_2.float_col, "
              "double_col from vtb_org_normal_0.double_col, "
              "bool_col from vtb_org_normal_1.bool_col, "
              "binary_16_col from vtb_org_normal_2.binary_16_col,"
              "binary_32_col from vtb_org_normal_0.binary_32_col,"
              "nchar_16_col from vtb_org_normal_1.nchar_16_col,"
              "nchar_32_col from vtb_org_normal_2.nchar_32_col)"
              "USING `vtb_virtual_stb` TAGS (0, false, 0, 0, 'child0', 'child0')"

"CREATE VTABLE `vtb_virtual_ctb_half_full` ("
              "u_tinyint_col from vtb_org_normal_0.u_tinyint_col, "
              "u_smallint_col from vtb_org_normal_1.u_smallint_col, "
              "u_int_col from vtb_org_normal_2.u_int_col, "
              "int_col from vtb_org_normal_0.int_col, "
              "bigint_col from vtb_org_normal_1.bigint_col, "
              "float_col from vtb_org_normal_2.float_col, "
              "binary_32_col from vtb_org_normal_0.binary_32_col,"
              "nchar_16_col from vtb_org_normal_1.nchar_16_col,"
              "nchar_32_col from vtb_org_normal_2.nchar_32_col)"
              "USING `vtb_virtual_stb` TAGS (1, false, 1, 1, 'child1', 'child1')"

"CREATE VTABLE `vtb_virtual_ctb_empty` "
              "USING `vtb_virtual_stb` TAGS (2, false, 2, 2, 'child2', 'child2')"

"CREATE VTABLE `vtb_virtual_ctb_mix` ("
              f"u_tinyint_col from vtb_org_child_0.u_tinyint_col, "
              f"u_smallint_col from vtb_org_child_1.u_smallint_col, "
              f"u_int_col from vtb_org_child_2.u_int_col, "
              f"u_bigint_col from vtb_org_child_0.u_bigint_col, "
              f"tinyint_col from vtb_org_child_1.tinyint_col, "
              f"smallint_col from vtb_org_child_2.smallint_col, "
              f"int_col from vtb_org_child_0.int_col, "
              f"bigint_col from vtb_org_child_1.bigint_col, "
              f"float_col from vtb_org_child_2.float_col, "
              f"double_col from vtb_org_child_0.double_col, "
              f"bool_col from vtb_org_child_1.bool_col, "
              f"binary_16_col from vtb_org_child_2.binary_16_col,"
              f"binary_32_col from vtb_org_child_0.binary_32_col,"
              f"nchar_16_col from vtb_org_child_1.nchar_16_col,"
              f"nchar_32_col from vtb_org_child_2.nchar_32_col)"
              f"USING `vtb_virtual_stb` TAGS (3, false, 3, 3, 'child3', 'child3')" |  |  |  |  |
|  | 普通投影查询（所有列均有数据源） | 执行 [test_vtable_select_test_projection.in](http://test_vtable_select_test_projection.in)
执行 [test_vstable_select_test_projection.in](http://test_vstable_select_test_projection.in)
执行 [test_vctable_select_test_projection.in](http://test_vctable_select_test_projection.in) | 成功 | Y | Pass |  |
|  | 普通投影查询（部分列没有数据源） | 执行 [test_vtable_select_test_projection.in](http://test_vtable_select_test_projection.in)
执行 [test_vstable_select_test_projection.in](http://test_vstable_select_test_projection.in)
执行 [test_vctable_select_test_projection.in](http://test_vctable_select_test_projection.in) | 成功 | Y | Pass |  |
|  | 普通投影查询，带where条件（所有列均有数据源） | 执行 [test_vtable_select_test_projection_filter.in](http://test_vtable_select_test_projection_filter.in)
执行 [test_vstable_select_test_projection_filter.in](http://test_vstable_select_test_projection_filter.in)
执行 [test_vctable_select_test_projection_filter.in](http://test_vctable_select_test_projection_filter.in) | 成功 | Y | Pass |  |
|  | 普通投影查询，带where条件（部分列没有数据源） | 执行 [test_vtable_select_test_projection_filter.in](http://test_vtable_select_test_projection_filter.in)
执行 [test_vstable_select_test_projection_filter.in](http://test_vstable_select_test_projection_filter.in)
执行 [test_vctable_select_test_projection_filter.in](http://test_vctable_select_test_projection_filter.in) | 成功 | Y | Pass |  |
|  | scalar function（所有列均有数据源） | 执行 [test_vtable_select_test_fuction.in](http://test_vtable_select_test_fuction.in)
执行 [test_vstable_select_test_fuction.in](http://test_vstable_select_test_projection_filter.in)
执行 [test_vctable_select_test_](http://test_vctable_select_test_projection_filter.in)[fuction.in](http://fuction.in) | 成功 | Y | Pass |  |
|  | scalar function（部分列没有数据源） | 执行 [test_vtable_select_test_fuction.in](http://test_vtable_select_test_fuction.in)
执行 [test_vstable_select_test_fuction.in](http://test_vstable_select_test_projection_filter.in)
执行 [test_vctable_select_test_](http://test_vctable_select_test_projection_filter.in)[fuction.in](http://fuction.in) | 成功 | Y | Pass |  |
|  | agg function（所有列均有数据源） | 执行 [test_vtable_select_test_fuction.in](http://test_vtable_select_test_fuction.in)
执行 [test_vstable_select_test_fuction.in](http://test_vstable_select_test_projection_filter.in)
执行 [test_vctable_select_test_](http://test_vctable_select_test_projection_filter.in)[fuction.in](http://fuction.in) | 成功 | Y | Pass |  |
|  | agg function（部分列没有数据源） | 执行 [test_vtable_select_test_fuction.in](http://test_vtable_select_test_fuction.in)
执行 [test_vstable_select_test_fuction.in](http://test_vstable_select_test_projection_filter.in)
执行 [test_vctable_select_test_](http://test_vctable_select_test_projection_filter.in)[fuction.in](http://fuction.in) | 成功 | Y | Pass |  |
|  | selection function（所有列均有数据源） | 执行 [test_vtable_select_test_fuction.in](http://test_vtable_select_test_fuction.in)
执行 [test_vstable_select_test_fuction.in](http://test_vstable_select_test_projection_filter.in)
执行 [test_vctable_select_test_](http://test_vctable_select_test_projection_filter.in)[fuction.in](http://fuction.in) | 成功 | Y | Pass |  |
|  | selection function（部分列没有数据源） | 执行 [test_vtable_select_test_fuction.in](http://test_vtable_select_test_fuction.in)
执行 [test_vstable_select_test_fuction.in](http://test_vstable_select_test_projection_filter.in)
执行 [test_vctable_select_test_](http://test_vctable_select_test_projection_filter.in)[fuction.in](http://fuction.in) | 成功 | Y | Pass |  |
|  | partition/group by | 执行 [test_vtable_select_test_partition.in](http://test_vtable_select_test_partition.in)
执行 [test_vctable_select_test_partition.in](http://test_vctable_select_test_partition.in)
执行 [test_vstable_select_test_partition.in](http://test_vstable_select_test_partition.in)
执行 [test_vtable_select_test_g](http://test_vtable_select_test_partition.in)[roup.in](http://roup.in)
执行 [test_vctable_select_test_g](http://test_vctable_select_test_partition.in)[roup.in](http://roup.in)
执行 [test_vstable_select_test_g](http://test_vstable_select_test_partition.in)[roup.in](http://roup.in) | 成功 | Y | Pass |  |
|  | interp测试 | 执行 [test_vtable_select_test_interp.in](http://test_vtable_select_test_interp.in)
执行 [test_vctable_select_test_interp.in](http://test_vctable_select_test_interp.in)
执行 [test_vstable_select_test_interp.in](http://test_vstable_select_test_interp.in) | 成功 | Y | Pass |  |
|  | 窗口测试 | 执行 [test_vtable_select_test_interval.in](http://test_vtable_select_test_interval.in)
执行 [test_vctable_select_test_interval.in](http://test_vctable_select_test_interval.in)
执行 [test_vstable_select_test_interval.in](http://test_vstable_select_test_interval.in)
将 interval 替换为 state / session / event / count 可测试其他窗口 | 成功 | Y | Pass |  |
|  | order by | 执行 [test_vtable_select_test_orderby.in](http://test_vtable_select_test_orderby.in)
执行 [test_vctable_select_test_orderby.in](http://test_vctable_select_test_orderby.in)
执行 [test_vstable_select_test_orderby.in](http://test_vstable_select_test_orderby.in) | 成功 | Y | Pass |  |
|  | limit/slimit | 前文的测试中都有 limit 语句限制 | 成功 | Y | Pass |  |
| 测试修改虚拟普通表（正确场景） | 准备原始表 | "CREATE STABLE `vtb_org_stb` ("
              "ts timestamp, "
              "u_tinyint_col tinyint unsigned, "
              "u_smallint_col smallint unsigned, "
              "u_int_col int unsigned, "
              "u_bigint_col bigint unsigned, "
              "tinyint_col tinyint, "
              "smallint_col smallint, "
              "int_col int, "
              "bigint_col bigint, "
              "float_col float, "
              "double_col double, "
              "bool_col bool, "
              "binary_16_col binary(16),"
              "binary_32_col binary(32),"
              "nchar_16_col nchar(16),"
              "nchar_32_col nchar(32),"
              "varbinary_16_col varbinary(16),"
              "varbinary_32_col varbinary(32),"
              "geo_16_col geometry(16),"
              "geo_32_col geometry(32)"
              ") TAGS ("
              "int_tag int,"
              "bool_tag bool,"
              "float_tag float,"
              "double_tag double,"
              "nchar_32_tag nchar(32),"
              "binary_32_tag binary(32))"

for i in range(30):
    tdSql.execute(f"CREATE TABLE `vtb_org_child_{i}` USING `vtb_org_stb` TAGS ({i}, false, {i}, {i}, 'child{i}', 'child{i}');")

for i in range(30):
    tdSql.execute(f"CREATE TABLE `vtb_org_normal_{i}` (ts timestamp, u_tinyint_col tinyint unsigned, u_smallint_col smallint unsigned, u_int_col int unsigned, u_bigint_col bigint unsigned, tinyint_col tinyint, smallint_col smallint, int_col int, bigint_col bigint, float_col float, double_col double, bool_col bool, binary_16_col binary(16), binary_32_col binary(32), nchar_16_col nchar(16), nchar_32_col nchar(32), varbinary_16_col varbinary(16), varbinary_32_col varbinary(32), geo_16_col geometry(16), geo_32_col geometry(32))") |  |  |  |  |
|  | 创建需要测试的虚拟普通表 | "CREATE VTABLE `vtb_virtual_ntb0` ("
              "ts timestamp, "
              "u_tinyint_col tinyint unsigned from vtb_org_child_0.u_tinyint_col, "
              "u_smallint_col smallint unsigned from vtb_org_normal_1.u_smallint_col, "
              "u_int_col int unsigned from vtb_org_child_2.u_int_col, "
              "u_bigint_col bigint unsigned from vtb_org_normal_3.u_bigint_col, "
              "tinyint_col tinyint from vtb_org_child_4.tinyint_col, "
              "smallint_col smallint from vtb_org_normal_5.smallint_col, "
              "int_col int from vtb_org_child_6.int_col, "
              "bigint_col bigint from vtb_org_normal_7.bigint_col, "
              "float_col float from vtb_org_child_8.float_col, "
              "double_col double from vtb_org_normal_9.double_col, "
              "bool_col bool from vtb_org_child_10.bool_col, "
              "binary_16_col binary(16) from vtb_org_normal_11.binary_16_col,"
              "binary_32_col binary(32) from vtb_org_child_12.binary_32_col,"
              "nchar_16_col nchar(16) from vtb_org_normal_13.nchar_16_col,"
              "nchar_32_col nchar(32) from vtb_org_child_14.nchar_32_col,"
              "varbinary_16_col varbinary(16) from vtb_org_normal_15.varbinary_16_col,"
              "varbinary_32_col varbinary(32) from vtb_org_child_16.varbinary_32_col,"
              "geo_16_col geometry(16) from vtb_org_normal_17.geo_16_col,"
              "geo_32_col geometry(32) from vtb_org_child_18.geo_32_col)" | 成功 | Y | Pass |  |
|  | 虚拟普通表增加列 | alter vtable vtb_virtual_ntb0 add column extra_boolcol bool
alter vtable vtb_virtual_ntb0 add column extra_intcol int from vtb_org_child_19.int_col | 成功 | Y | Pass |  |
|  | 虚拟普通表删除列 | alter vtable vtb_virtual_ntb0 drop column extra_intcol | 成功 | Y | Pass |  |
|  | 虚拟普通表修改数据源 | alter vtable vtb_virtual_ntb0 alter column extra_boolcol set vtb_org_child_19.bool_col;
alter vtable vtb_virtual_ntb0 alter column extra_boolcol set NULL; | 成功 | Y | Pass |  |
|  | 虚拟普通表修改列长度 | alter vtable vtb_virtual_ntb0 alter column nchar_16_col set NULL;
alter vtable vtb_virtual_ntb0 modify column nchar_16_col nchar(32);
alter vtable vtb_virtual_ntb0 alter column nchar_16_col set vtb_org_child_19.nchar_32_col; | 成功 | Y | Pass |  |
|  | 虚拟普通表修改列名 | alter vtable vtb_virtual_ntb0 rename column u_smallint_col u_smallint_col_rename; | 成功 | Y | Pass |  |
| 测试修改虚拟普通表（错误场景） | 虚拟普通表增加列，数据源和定义不符合 | alter vtable vtb_virtual_ntb0 add column extra_intcol int from vtb_org_child_19.tinyint_col | 失败 | Y | Pass |  |
|  | 虚拟普通表修改数据源，数据源和定义不符合 | alter vtable vtb_virtual_ntb0 alter column int_col set vtb_org_child_19.tinyint_col | 失败 | Y | Pass |  |
|  | 虚拟普通表修改列长度，但是没有清空数据源。 | alter vtable vtb_virtual_ntb0 modify column nchar_16_col nchar(32); | 失败 | Y | Pass |  |
| 测试修改虚拟子表（正确场景） | 创建需要测试的虚拟子表 | "CREATE VTABLE `vtb_virtual_ctb0`("
              "u_tinyint_col FROM vtb_org_child_0.u_tinyint_col, "
              "u_smallint_col FROM vtb_org_child_1.u_smallint_col, "
              "u_int_col FROM vtb_org_child_2.u_int_col, "
              "u_bigint_col FROM vtb_org_child_3.u_bigint_col,"
              "tinyint_col FROM vtb_org_child_4.tinyint_col, "
              "smallint_col FROM vtb_org_child_5.smallint_col, "
              "int_col FROM vtb_org_child_6.int_col, "
              "bigint_col FROM vtb_org_child_7.bigint_col,"
              "float_col FROM vtb_org_child_8.float_col, "
              "double_col FROM vtb_org_child_9.double_col, "
              "bool_col FROM vtb_org_child_10.bool_col, "
              "binary_16_col FROM vtb_org_child_11.binary_16_col,"
              "binary_32_col FROM vtb_org_child_12.binary_32_col, "
              "nchar_16_col FROM vtb_org_child_13.nchar_16_col, "
              "nchar_32_col FROM vtb_org_child_14.nchar_32_col,"
              "varbinary_16_col FROM vtb_org_child_15.varbinary_16_col, "
              "varbinary_32_col FROM vtb_org_child_16.varbinary_32_col, "
              "geo_16_col FROM vtb_org_child_17.geo_16_col, "
              "geo_32_col FROM vtb_org_child_18.geo_32_col) USING vtb_virtual_stb TAGS (0, false, 0, 0, 'vchild0', 'vchild0')" | 成功 | Y | Pass |  |
|  | 虚拟子表修改数据源 | alter vtable vtb_virtual_ctb0 alter column bool_col set vtb_org_child_19.bool_col;
alter vtable vtb_virtual_ctb0 alter column bool_col set NULL; | 成功 | Y | Pass |  |
|  | 虚拟子表修改 tag 值 | alter vtable vtb_virtual_ctb0 set tag int_tag = 10; | 成功 | Y | Pass |  |
|  | 虚拟子表对应超级表增加列 | alter stable vtb_virtual_stb add column extra_boolcol bool | 成功 | Y | Pass |  |
|  | 虚拟子表对应超级表删除列 | alter stable vtb_virtual_stb drop column extra_boolcol; | 成功 | Y | Pass |  |
|  | 虚拟子表对应超级表修改列长度 | alter vtable vtb_virtual_ctb0 alter column nchar_16_col set NULL;
alter stable vtb_virtual_stb modify column nchar_16_col nchar(32); | 成功 | Y | Pass |  |
|  | 虚拟子表对应超级表增加 tag | alter stable vtb_virtual_stb add tag extra_int_tag int; | 成功 | Y | Pass |  |
|  | 虚拟子表对应超级表删除 tag | alter stable vtb_virtual_stb drop tag extra_int_tag; | 成功 | Y | Pass |  |
|  | 虚拟子表对应超级表修改 tag 名 | alter stable vtb_virtual_stb rename tag int_tag int_tag_rename; | 成功 | Y | Pass |  |
|  | 虚拟子表对应超级表修改 tag 长度 | alter stable vtb_virtual_stb modify tag nchar_32_tag nchar(64); | 成功 | Y | Pass |  |
| 测试修改虚拟子表（错误场景） | 虚拟子表修改数据源，数据源和定义不符合 | alter vtable vtb_virtual_ctb0 alter column int_col set vtb_org_child_19.tinyint_col | 失败 | Y | Pass |  |
|  | 虚拟子表对应超级表修改列长度，但是没有清空所有子表的数据源。 | alter stable vtb_virtual_stb modify column nchar_16_col nchar(32); | 失败 | Y | Pass |  |
| 测试删除虚拟表 | 创建需要测试的虚拟子表 | CREATE STABLE `vtb_virtual_stb` ("
          "ts timestamp, "
          "u_tinyint_col tinyint unsigned, "
          "u_smallint_col smallint unsigned, "
          "u_int_col int unsigned, "
          "u_bigint_col bigint unsigned, "
          "tinyint_col tinyint, "
          "smallint_col smallint, "
          "int_col int, "
          "bigint_col bigint, "
          "float_col float, "
          "double_col double, "
          "bool_col bool, "
          "binary_16_col binary(16),"
          "binary_32_col binary(32),"
          "nchar_16_col nchar(16),"
          "nchar_32_col nchar(32),"
          "varbinary_16_col varbinary(16),"
          "varbinary_32_col varbinary(32),"
          "geo_16_col geometry(16),"
          "geo_32_col geometry(32)"
          ") TAGS ("
          "int_tag int,"
          "bool_tag bool,"
          "float_tag float,"
          "double_tag double,"
          "nchar_32_tag nchar(32),"
          "binary_32_tag binary(32))"
          "VIRTUAL 1"


for i in range(30):
    tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb{i}` "
                  f"(vtb_org_child_0.u_tinyint_col, "
                  f"vtb_org_child_1.u_smallint_col, "
                  f"vtb_org_child_2.u_int_col, "
                  f"vtb_org_child_3.u_bigint_col,"
                  f"vtb_org_child_4.tinyint_col) "
                  f"USING vtb_virtual_stb TAGS ({i}, false, {i}, {i}, 'vchild{i}', 'vchild{i}')") | 成功 | Y | Pass |  |
|  | 删除虚拟子表 | drop vtable vtb_virtual_ctb0 | 成功 | Y | Pass |  |
|  | 创建需要测试的虚拟普通表 | for i in range(30):
    tdSql.execute(f"CREATE TABLE `vtb_org_normal_{i}` (ts timestamp, u_tinyint_col tinyint unsigned, u_smallint_col smallint unsigned, u_int_col int unsigned, u_bigint_col bigint unsigned, tinyint_col tinyint, smallint_col smallint, int_col int, bigint_col bigint, float_col float, double_col double, bool_col bool, binary_16_col binary(16), binary_32_col binary(32), nchar_16_col nchar(16), nchar_32_col nchar(32), varbinary_16_col varbinary(16), varbinary_32_col varbinary(32), geo_16_col geometry(16), geo_32_col geometry(32))") | 成功 | Y | Pass |  |
|  | 删除虚拟普通表 | drop vtable vtb_virtual_ntb0 | 成功 | Y | Pass |  |
|  | 删除虚拟子表对应的超级表 | drop stable vtb_virtual_stb | 成功 | Y | Pass |  |
| 测试虚拟表元数据相关 | 查看数据库下所有虚拟表 | 执行 [test_vtable_meta.in](http://test_vtable_meta.in) | 成功 | Y | Pass |  |
|  | 查看数据库下所有虚拟超级表 | 执行 [test_vtable_meta.in](http://test_vtable_meta.in) | 成功 | Y | Pass |  |
|  | 查看虚拟表创建语句 | 执行 [test_vtable_meta.in](http://test_vtable_meta.in) | 成功 | Y | Pass |  |
|  | 查看虚拟表列信息 | 执行 [test_vtable_meta.in](http://test_vtable_meta.in) | 成功 | Y | Pass |  |
|  | 查询元数据表 ins_stables 中的虚拟超级表 | 执行 [test_vtable_meta.in](http://test_vtable_meta.in) | 成功 | Y | Pass |  |
|  | 查询元数据表 ins_tables 中的虚拟表 | 执行 [test_vtable_meta.in](http://test_vtable_meta.in) | 成功 | Y | Pass |  |
|  | 查询元数据表 ins_columns 中的虚拟表列信息。 | 执行 [test_vtable_meta.in](http://test_vtable_meta.in) | 成功 | Y | Pass |  |
| 虚拟表权限测试 | 创建虚拟普通表
db 权限：{READ \| WRITE \| ALL \| NONE}
原始表权限：{all READ \| part READ \| WRITE \| ALL \| NONE} | 1. 使用 root 用户创建并使用库 test_vtb_priv. 
        create database test_vtb_priv;
        use test_vtb_priv;
1. 使用 root 用户创建两张普通表。
        create table orgtb1(ts timestamp, int_col int);
        create table orgtb2(ts timestamp, bool_col bool);
1. 使用 root 用户创建新用户 vtb_test
        create user vtb_test PASS 'pswd';
1. 给 vtb_test 用户授权 test_vtb_priv 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| WRITE \| ALL \| NONE} on test_vtb_priv to vtb_test;
1. 给 vtb_test 用户授权 orgtb1 和 orgtb2 的 {all READ \| part READ \| ALL \| NONE \| WRITE} 权限。
        grant {READ \| ALL \| NONE \| WRITE} on test_vtb_priv.orgtb1 to vtb_test;
        grant {READ \| ALL \| NONE \| WRITE}  on test_vtb_priv.orgtb2 to vtb_test;
1. 用 vtb_test 用户登陆，并使用 test_vtb_priv 库。
        use test_vtb_priv;
1. 创建虚拟表 vtb0。
        create vtable test_vtb0(
                ts timestamp,
                boolcol bool from orgtb2.bool_col,
                intcol int from orgtb1.int_col
        );
1. 撤销用户 vtb_test 的权限。
        revoke all on test_vtb_priv from vtb_test;
        revoke all on test_vtb_priv.orgtb1 from vtb_test;
        revoke all on test_vtb_priv.orgtb2 from vtb_test;
1. 使用 root 用户删除库 test_vtb_priv。
         drop database test_vtb_priv;
1. 使用 root 用户删除 vtb_test 用户。
        drop user vtb_test; | 只有
1. 对 db 有写入权限
2. 对全部原始表有读取权限
同时满足 1 2 才会创建成功，其余均失败 | Y | Pass |  |
|  | 创建虚拟子表
db 权限：{READ \| WRITE \| ALL \| NONE}
原始表权限：{all READ \| part READ \| WRITE \| ALL \| NONE}
虚拟超级表权限：{ READ \| WRITE \| ALL \| NONE} | 1. 使用 root 用户创建并使用库 test_vtb_priv. 
        create database test_vtb_priv;
        use test_vtb_priv;
1. 使用 root 用户创建两张普通表。
        create table orgtb1(ts timestamp, int_col int);
        create table orgtb2(ts timestamp, bool_col bool);
1. 使用 root 用户创建虚拟超级表。
        create stable test_vstb0( 
                ts timestamp,
                boolcol bool,
                intcol int) 
               TAGS (device varchar(16));
1. 使用 root 用户创建新用户 vtb_test
        create user vtb_test PASS 'pswd';
1. 给 vtb_test 用户授权 test_vtb_priv 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| WRITE \| ALL \| NONE} on test_vtb_priv to vtb_test;
1. 给 vtb_test 用户授权 orgtb1 和 orgtb2 的 {all READ \| part READ \| ALL \| NONE \| WRITE} 权限。
        grant {READ \| ALL \| NONE \| WRITE} on test_vtb_priv.orgtb1 to vtb_test;
        grant {READ \| ALL \| NONE \| WRITE}  on test_vtb_priv.orgtb2 to vtb_test;
1. 给 vtb_test 用户授权虚拟超级表 test_vstb0 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| ALL \| NONE \| WRITE} on test_vtb_priv.test_vstb0 to vtb_test;
1. 用 vtb_test 用户登陆，并使用 test_vtb_priv 库。
        use test_vtb_priv;
1. 创建虚拟子表 test_vctb0。
        create vtable test_vctb0(
                boolcol from orgtb2.bool_col,
                intcol from orgtb1.int_col
        ) TAGS ('d0');
1. 撤销用户 vtb_test 的权限。
        revoke all on test_vtb_priv from vtb_test;
        revoke all on test_vtb_priv.orgtb1 from vtb_test;
        revoke all on test_vtb_priv.orgtb2 from vtb_test;
        revoke all on test_vtb_priv.test_vstb0 from vtb_test;
1. 使用 root 用户删除库 test_vtb_priv。
         drop database test_vtb_priv;
1. 使用 root 用户删除 vtb_test 用户。
        drop user vtb_test; | 只有
1. 对 db 有写入权限
2. 对全部原始表有读取权限
3. 对虚拟超级表有写入权限
同时满足1 2 或 2 3才会创建成功，其余均失败 | Y | Pass |  |
|  | 查询虚拟普通表
db 权限：{READ \| WRITE \| NONE \| ALL}
对原始表权限：{ part READ \| all READ \| WRITE \| NONE \| ALL}
对虚拟表权限：{READ \| WRITE \| NONE \| ALL} | 1. 使用 root 用户创建并使用库 test_vtb_priv. 
        create database test_vtb_priv;
        use test_vtb_priv;
1. 使用 root 用户创建两张普通表。
        create table orgtb1(ts timestamp, int_col int);
        create table orgtb2(ts timestamp, bool_col bool);
1. 使用 root 用户创建新用户 vtb_test
        create user vtb_test PASS 'pswd';
1. 使用 root 用户创建虚拟表 vtb0。
        create vtable test_vtb0(
                ts timestamp,
                boolcol bool from orgtb2.bool_col,
                intcol int from orgtb1.int_col
        );
1. 给 vtb_test 用户授权 test_vtb_priv 的 {READ \| WRITE \| NONE \| ALL} 权限。
        grant {READ \| WRITE \| ALL } on test_vtb_priv to vtb_test;
1. 给 vtb_test 用户授权虚拟表 test_vtb0 的 { READ \| WRITE \| NONE \| ALL } 权限。
        grant {READ \| WRITE \| ALL} on test_vtb0 to vtb_test;
1. 给 vtb_test 用户授权 orgtb1 和 orgtb2 的{ READ \| WRITE \| NONE \| ALL \| 只有 orgtb1 READ}权限。
        grant {READ \| WRITE \| ALL } on orgtb1 to vtb_test;
        grant {READ \| WRITE \| ALL } on orgtb2 to vtb_test;
1. 用 vtb_test 用户登陆，并查询虚拟表 test_vtb0;
        select * from test_vtb_priv.test_vtb0;
1. 使用 root 用户撤销用户 vtb_test 的权限。
        revoke all on test_vtb_priv from vtb_test;
1. 使用 root 用户删除库 test_vtb_priv。
         drop database test_vtb_priv;
1. 使用 root 用户删除 vtb_test 用户。
        drop user vtb_test; | 1. 对 db 有 READ 权限
1. 对全部原始表有 READ 权限
2. 对虚拟表有 READ 权限

需要同时满足 1 2 或 2 3 才会成功，其余均失败。 | Y | Pass |  |
|  | 查询虚拟子表
db 权限：{READ \| WRITE \| NONE \| ALL}
对原始表权限：{part READ \| all READ \| WRITE \| NONE \| ALL}
对虚拟超级表权限：{ READ \| WRITE \| NONE \| ALL} | 1. 使用 root 用户创建并使用库 test_vtb_priv. 
        create database test_vtb_priv;
        use test_vtb_priv;
1. 使用 root 用户创建两张普通表。
        create table orgtb1(ts timestamp, int_col int);
        create table orgtb2(ts timestamp, bool_col bool);
1. 使用 root 用户创建虚拟超级表。
        create stable test_vstb0( 
                ts timestamp,
                boolcol bool,
                intcol int) 
               TAGS (device varchar(16));
1. 使用 root 用户创建虚拟子表 test_vctb0。
        create vtable test_vctb0(
                boolcol from orgtb2.bool_col,
                intcol from orgtb1.int_col
        ) TAGS ('d0');
1. 使用 root 用户创建新用户 vtb_test
        create user vtb_test PASS 'pswd';
1. 给 vtb_test 用户授权 test_vtb_priv 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| WRITE \| ALL \| NONE} on test_vtb_priv to vtb_test;
1. 给 vtb_test 用户授权 orgtb1 和 orgtb2 的 {all READ \| part READ \| ALL \| NONE \| WRITE} 权限。
        grant {READ \| ALL \| NONE \| WRITE} on test_vtb_priv.orgtb1 to vtb_test;
        grant {READ \| ALL \| NONE \| WRITE}  on test_vtb_priv.orgtb2 to vtb_test;
1. 给 vtb_test 用户授权虚拟超级表 test_vstb0 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| ALL \| NONE \| WRITE} on test_vtb_priv.test_vstb0 to vtb_test;
1. 用 vtb_test 用户登陆，查询 test_vtb_priv.test_vctb0。
        select * from test_vtb_priv.test_vctb0;
1. 撤销用户 vtb_test 的权限。
        revoke all on test_vtb_priv from vtb_test;
        revoke all on test_vtb_priv.orgtb1 from vtb_test;
        revoke all on test_vtb_priv.orgtb2 from vtb_test;
        revoke all on test_vtb_priv.test_vstb0 from vtb_test;
1. 使用 root 用户删除库 test_vtb_priv。
         drop database test_vtb_priv;
1. 使用 root 用户删除 vtb_test 用户。
        drop user vtb_test; | 1. 对 DB 有 READ 权限
1. 对全部原始表有 READ 权限
2. 对虚拟超级表有 READ 权限

同时满足 1 2 或 2 3 才成功，其余均失败。 | Y | Pass |  |
|  | 修改虚拟普通表
db 权限：{READ \| WRITE \| NONE \| ALL}
对原始表权限：{part READ \| all READ \| WRITE \| NONE \| ALL}
对虚拟表权限：{ READ \| ALTER \| NONE \| ALL} | 1. 使用 root 用户创建并使用库 test_vtb_priv. 
        create database test_vtb_priv;
        use test_vtb_priv;
1. 使用 root 用户创建三张普通表。
        create table orgtb1(ts timestamp, int_col int);
        create table orgtb2(ts timestamp, bool_col bool);
        create table orgtb3(ts timestamp, bool_col bool, var_col varchar(16));
1. 使用 root 用户创建虚拟表 test_vtb0。
        create vtable test_vtb0(
                ts timestamp,
                boolcol bool from orgtb2.bool_col,
                intcol int from orgtb1.int_col
        );
1. 使用 root 用户创建新用户 vtb_test
        create user vtb_test PASS 'pswd';
1. 给 vtb_test 用户授权 test_vtb_priv 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| WRITE \| ALL \| NONE} on test_vtb_priv to vtb_test;
1. 给 vtb_test 用户授权 orgtb1 和 orgtb2, orgtb3 的 {all READ \| part READ \| ALL \| NONE \| WRITE} 权限。
        grant {READ \| ALL \| NONE \| WRITE} on test_vtb_priv.orgtb1 to vtb_test;
        grant {READ \| ALL \| NONE \| WRITE}  on test_vtb_priv.orgtb2 to vtb_test;
        grant {READ \| ALL \| NONE \| WRITE}  on test_vtb_priv.orgtb3 to vtb_test;
1. 给 vtb_test 用户授权虚拟超级表 test_vtb0 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| ALL \| NONE \| WRITE} on test_vtb_priv.test_vtb0 to vtb_test;
1. 用 vtb_test 用户登陆，给 test_vtb_priv.test_vtb0 增加一列（不指定数据源）。
        alter vtable test_vtb_priv.test_vtb0 ADD COLUMN extra_bool bool;
1. 用 vtb_test 用户登陆，给 test_vtb_priv.test_vtb0 增加一列（指定数据源）。
        alter vtable test_vtb_priv.test_vtb0 ADD COLUMN extra_var varchar(16) SET orgtb3.var_col;
1. describe test_vtb_priv.test_vtb0;
2. 使用 vtb_test 用户登陆，删除列 boolcol。
        alter vtable test_vtb_priv.test_vtb0 DROP COLUMN boolcol;
1. describe test_vtb_priv.test_vtb0;
2. 使用 vtb_test 用户登录，指定 extra_bool 列的数据源。
        alter vtable test_vtb_priv.test_vtb0 ALTER COLUMN extra_bool SET orgtb3.bool_col;
1. describe test_vtb_priv.test_vtb0;
2. 使用 vtb_test 用户修改列长度。首先把列数据源置空。
        alter vtable test_vtb_priv.test_vtb0 ALTER COLUMN extra_var SET NULL;
        alter vtable test_vtb_priv.test_vtb0 MODIFY COLUMN extra_var varchar(32);
1. describe test_vtb_priv.test_vtb0;
2. 使用 vtb_test 修改虚拟普通表列名。
       alter vtable test_vtb_priv.test_vtb0 RENAME COLUMN extra_bool boolcol;
1. describe test_vtb_priv.test_vtb0;
2. 撤销用户 vtb_test 的权限。
        revoke all on test_vtb_priv from vtb_test;
        revoke all on test_vtb_priv.orgtb1 from vtb_test;
        revoke all on test_vtb_priv.orgtb2 from vtb_test;
        revoke all on test_vtb_priv.orgtb3 from vtb_test;
        revoke all on test_vtb_priv.test_vstb0 from vtb_test;
1. 使用 root 用户删除库 test_vtb_priv。
         drop database test_vtb_priv;
1. 使用 root 用户删除 vtb_test 用户。
        drop user vtb_test; | 1. 对 DB 有 WRITE 权限
1. 对虚拟表有 WRITE 权限。

如果不涉及到新增数据源，则需要满足 1 或 2 才可以成功，其余均失败。

如果需要新增数据源，需要在满足 1 或 2 的基础上对数据源所在的原始表有 READ 权限才会成功。 | Y | Pass |  |
|  | 修改虚拟子表
db 权限：{READ \| WRITE \| NONE \| ALL}
对原始表权限：{part READ \| all READ \| WRITE \| NONE \| ALL}
对虚拟超级表权限：{ READ \| WRITE \| NONE \| ALL} | 1. 使用 root 用户创建并使用库 test_vtb_priv. 
        create database test_vtb_priv;
        use test_vtb_priv;
1. 使用 root 用户创建三张普通表。
        create table orgtb1(ts timestamp, int_col int);
        create table orgtb2(ts timestamp, bool_col bool);
        create table orgtb3(ts timestamp, bool_col bool, var_col varchar(16));
1. 使用 root 用户创建虚拟超级表。
        create stable test_vstb0( 
                ts timestamp,
                boolcol bool,
                intcol int) 
               TAGS (device varchar(16));
1. 使用 root 用户创建虚拟子表 test_vctb0。
        create vtable test_vctb0(
                boolcol from orgtb2.bool_col,
                intcol from orgtb1.int_col
        ) TAGS ('d0');
1. 使用 root 用户创建新用户 vtb_test
        create user vtb_test PASS 'pswd';
1. 给 vtb_test 用户授权 test_vtb_priv 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| WRITE \| ALL \| NONE} on test_vtb_priv to vtb_test;
1. 给 vtb_test 用户授权 orgtb1 和 orgtb2、orgtb3 的 {all READ \| part READ \| ALL \| NONE \| WRITE} 权限。
        grant {READ \| ALL \| NONE \| WRITE} on test_vtb_priv.orgtb1 to vtb_test;
        grant {READ \| ALL \| NONE \| WRITE}  on test_vtb_priv.orgtb2 to vtb_test;
        grant {READ \| ALL \| NONE \| WRITE}  on test_vtb_priv.orgtb3 to vtb_test;
1. 给 vtb_test 用户授权虚拟超级表 test_vstb0 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| ALL \| NONE \| WRITE} on test_vtb_priv.test_vstb0 to vtb_test;
1. 用 vtb_test 用户登陆，修改数据源。
        alter vtable test_vtb_priv.test_vctb0 ALTER COLUMN boolcol SET test_vtb_priv.orgtb3.bool_col;
1. describe test_vtb_priv.test_vctb0;
2. 用 vtb_test 用户修改 TAG;
        alter vtable test_vtb_priv set tag device = 'd1';
1. describe test_vtb_priv.test_vctb0;
2. 撤销用户 vtb_test 的权限。
        revoke all on test_vtb_priv from vtb_test;
        revoke all on test_vtb_priv.orgtb1 from vtb_test;
        revoke all on test_vtb_priv.orgtb2 from vtb_test;
        revoke all on test_vtb_priv.orgtb3 from vtb_test;
        revoke all on test_vtb_priv.test_vctb0 from vtb_test;
1. 使用 root 用户删除库 test_vtb_priv。
         drop database test_vtb_priv;
1. 使用 root 用户删除 vtb_test 用户。
        drop user vtb_test; | 1. 对 DB 有 WRITE 权限
1. 对虚拟超级表有 WRITE 权限。

如果不涉及到新增数据源，则需要满足 1 或 2 才可以成功，其余均失败。

如果需要新增数据源，需要在满足 1 或 2 的基础上对数据源所在的原始表有 READ 权限才会成功。 | Y | Pass |  |
|  | 修改虚拟超级表
db 权限：{READ \| WRITE \| NONE \| ALL}
对原始表权限：{ part READ \| all READ \| WRITE \| NONE \| ALL}
对虚拟超级表权限：{ READ \| WRITE \| NONE \| ALL} | 1. 使用 root 用户创建并使用库 test_vtb_priv. 
        create database test_vtb_priv;
        use test_vtb_priv;
1. 使用 root 用户创建三张普通表。
        create table orgtb1(ts timestamp, int_col int);
        create table orgtb2(ts timestamp, bool_col bool);
        create table orgtb3(ts timestamp, bool_col bool, var_col varchar(16));
1. 使用 root 用户创建虚拟超级表。
        create stable test_vstb0( 
                ts timestamp,
                boolcol bool,
                intcol int) 
               TAGS (device varchar(16));
1. 使用 root 用户创建虚拟子表 test_vctb0。
        create vtable test_vctb0(
                boolcol from orgtb2.bool_col,
                intcol from orgtb1.int_col
        ) TAGS ('d0');
1. 使用 root 用户创建新用户 vtb_test
        create user vtb_test PASS 'pswd';
1. 给 vtb_test 用户授权 test_vtb_priv 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| WRITE \| ALL \| NONE} on test_vtb_priv to vtb_test;
1. 给 vtb_test 用户授权 orgtb1 和 orgtb2、orgtb3 的 {all READ \| part READ \| ALL \| NONE \| WRITE} 权限。
        grant {READ \| ALL \| NONE \| WRITE} on test_vtb_priv.orgtb1 to vtb_test;
        grant {READ \| ALL \| NONE \| WRITE}  on test_vtb_priv.orgtb2 to vtb_test;
        grant {READ \| ALL \| NONE \| WRITE}  on test_vtb_priv.orgtb3 to vtb_test;
1. 给 vtb_test 用户授权虚拟超级表 test_vstb0 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| ALL \| NONE \| WRITE} on test_vtb_priv.test_vstb0 to vtb_test;
1. 用 vtb_test 用户登陆，给超级表增加列。
        alter vtable test_vtb_priv.test_vstb0 ADD COLUMN extra_boolcol bool;
1. describe test_vtb_priv.test_vstb0;
2. 用 vtb_test 用户给超级表删除列。
        alter vtable test_vtb_priv.test_vstb0 DROP COLUMN extra_boolcol;
1. describe test_vtb_priv.test_vstb0;
2. 用 vtb_test 用户给超级表增加 tag。
        alter vtable test_vtb_priv.test_vstb0 ADD TAG groupid int;
1. describe test_vtb_priv.test_vstb0;
2. 用 vtb_test 用户给超级表删除 tag。
        alter vtable test_vtb_priv.test_vstb0 DROP TAG groupid int;
1. describe test_vtb_priv.test_vstb0;
2. 撤销用户 vtb_test 的权限。
        revoke all on test_vtb_priv from vtb_test;
        revoke all on test_vtb_priv.orgtb1 from vtb_test;
        revoke all on test_vtb_priv.orgtb2 from vtb_test;
        revoke all on test_vtb_priv.orgtb3 from vtb_test;
        revoke all on test_vtb_priv.test_vctb0 from vtb_test;
1. 使用 root 用户删除库 test_vtb_priv。
         drop database test_vtb_priv;
1. 使用 root 用户删除 vtb_test 用户。
        drop user vtb_test; | 1. 对 DB 有 WRITE 权限
1. 对虚拟超级表有 WRITE 权限。

满足 1 或 2 才可以成功，其余均失败。 | Y | Pass |  |
|  | 删除虚拟普通表
db 权限：{READ \| WRITE \| NONE \| ALL}
对虚拟表权限：{ READ \| WRITE \| NONE \| ALL} | 1. 使用 root 用户创建并使用库 test_vtb_priv. 
        create database test_vtb_priv;
        use test_vtb_priv;
1. 使用 root 用户创建三张普通表。
        create table orgtb1(ts timestamp, int_col int);
        create table orgtb2(ts timestamp, bool_col bool);
        create table orgtb3(ts timestamp, bool_col bool, var_col varchar(16));
1. 使用 root 用户创建虚拟表 test_vtb0。
        create vtable test_vtb0(
                ts timestamp,
                boolcol bool from orgtb2.bool_col,
                intcol int from orgtb1.int_col
        );
1. 使用 root 用户创建新用户 vtb_test
        create user vtb_test PASS 'pswd';
1. 给 vtb_test 用户授权 test_vtb_priv 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| WRITE \| ALL \| NONE} on test_vtb_priv to vtb_test;
1. 给 vtb_test 用户授权虚拟表 test_vtb0 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| ALL \| NONE \| WRITE} on test_vtb_priv.test_vtb0 to vtb_test;
1. 用 vtb_test 用户登陆，删除虚拟表 test_vtb_priv.test_vtb0。
        drop vtable test_vtb_priv.test_vtb0;
1. describe test_vtb_priv.test_vtb0;
2. 撤销用户 vtb_test 的权限。
        revoke all on test_vtb_priv from vtb_test;
        revoke all on test_vtb_priv.orgtb1 from vtb_test;
        revoke all on test_vtb_priv.orgtb2 from vtb_test;
        revoke all on test_vtb_priv.orgtb3 from vtb_test;
        revoke all on test_vtb_priv.test_vtb0 from vtb_test;
1. 使用 root 用户删除库 test_vtb_priv。
         drop database test_vtb_priv;
1. 使用 root 用户删除 vtb_test 用户。
        drop user vtb_test; | 1. 对 DB 有 WRITE 权限
1. 对虚拟表有 WRITE 权限。

满足 1 或 2 才可以成功，其余均失败。 | Y | Pass |  |
|  | 删除虚拟子表
db 权限：{READ \| WRITE \| NONE \| ALL}
对虚拟超级表权限：{ READ \| WRITE \| NONE \| ALL} | 1. 使用 root 用户创建并使用库 test_vtb_priv. 
        create database test_vtb_priv;
        use test_vtb_priv;
1. 使用 root 用户创建三张普通表。
        create table orgtb1(ts timestamp, int_col int);
        create table orgtb2(ts timestamp, bool_col bool);
        create table orgtb3(ts timestamp, bool_col bool, var_col varchar(16));
1. 使用 root 用户创建虚拟超级表。
        create stable test_vstb0( 
                ts timestamp,
                boolcol bool,
                intcol int) 
               TAGS (device varchar(16));
1. 使用 root 用户创建虚拟子表 test_vctb0。
        create vtable test_vctb0(
                boolcol from orgtb2.bool_col,
                intcol from orgtb1.int_col
        ) TAGS ('d0');
1. 使用 root 用户创建新用户 vtb_test
        create user vtb_test PASS 'pswd';
1. 给 vtb_test 用户授权 test_vtb_priv 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| WRITE \| ALL \| NONE} on test_vtb_priv to vtb_test;
7.. 给 vtb_test 用户授权虚拟超级表 test_vstb0 的 {READ \| WRITE \| ALL \| NONE} 权限。
        grant {READ \| ALL \| NONE \| WRITE} on test_vtb_priv.test_vstb0 to vtb_test;
1. 用 vtb_test 用户登陆，删除虚拟子表。
        drop vtable test_vtb_priv.test_vctb0;
1. describe test_vtb_priv.test_vstb0;
        alter vtable test_vtb_priv.test_vstb0 DROP COLUMN extra_boolcol;
1. 撤销用户 vtb_test 的权限。
        revoke all on test_vtb_priv from vtb_test;
        revoke all on test_vtb_priv.orgtb1 from vtb_test;
        revoke all on test_vtb_priv.orgtb2 from vtb_test;
        revoke all on test_vtb_priv.orgtb3 from vtb_test;
        revoke all on test_vtb_priv.test_vctb0 from vtb_test;
1. 使用 root 用户删除库 test_vtb_priv。
         drop database test_vtb_priv;
1. 使用 root 用户删除 vtb_test 用户。
        drop user vtb_test; | 1. 对 DB 有 WRITE 权限
1. 对虚拟超级表有 WRITE 权限。

满足 1 或 2 才可以成功，其余均失败。 | Y | Pass |  |

### 8.2 可用性

无

### 8.3 可靠性

无

### 8.4 性能

#### 8.4.1 创建性能

```json
create vtable test_perf_vtb0(
    ts timestamp, 
    col1 int from dorg0.intcol,
    col2 int from dorg1.intcol,
    ...);
```


| 10 原始表 | 20 原始表 | 50 原始表 | 100 原始表 | 200 原始表 | 500 原始表 |
| --- | --- | --- | --- | --- | --- |
| 暂未测试 | 暂未测试 | 暂未测试 | 暂未测试 | 暂未测试 | 暂未测试 |

#### 8.4.2 查询性能

虚拟表查询性能可见 [虚拟表查询性能验证](https://taosdata.feishu.cn/wiki/J75YwU6XbinjJMku0XUcgxKSnch)

### 8.5 安全性

无

### 8.6 兼容性

无

### 8.7 本地化

无

## 9. 待讨论（可选）

无

## 10. Jira（可选）

无

## 11. 测试计划（可选）

TBD

## 12. 风险评估

风险较高

## 13. 测试备忘（可选）

无

## 14. 参考文档

这里用于添加对该需求测试有帮助的文档链接：
- Func Spec: [虚拟表](https://taosdata.feishu.cn/wiki/Dq4FwEEynirUxkkxL1JcsE3WnJd)
- Design Spec: [虚拟表 Design spec](https://taosdata.feishu.cn/wiki/Dp4cwbT4nitQPDkzUrzcWxgInEb)
