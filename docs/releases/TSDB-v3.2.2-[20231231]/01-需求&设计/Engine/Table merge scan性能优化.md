# Table merge scan性能优化

#### 1. 测试场景

1supertable, 5000 subtables, 36000 records each table, each table has 10 durations.
```json
    "databases": [
        {
            "dbinfo": {
                "name": "duration_db",
                "drop": "yes",
                "vgroups": 1,
                "replica": 1,
                "precision": "ms",
                "duration":"1h"
            },
            "super_tables": [
                {
                    "name": "meters",
                    "child_table_exists": "no",
                    "childtable_count": 5000,
                    "insert_rows": 36000,
                    "childtable_prefix": "d",
                    "insert_mode": "taosc",
                    "timestamp_step": 1000,
                    "start_timestamp":"2021-04-19 00:00:00.000",
                    "columns": [
                        { "type": "bool",        "name": "bc"},
                        { "type": "float",       "name": "fc",  "max": 1, "min": 0 },
                        { "type": "double",      "name": "dc",  "max": 1, "min": 0 },
                        { "type": "tinyint",     "name": "ti",  "max": 100, "min": 0 },
                        { "type": "smallint",    "name": "si",  "max": 100, "min": 0 },
                        { "type": "int",         "name": "ic",  "max": 100, "min": 0 },
                        { "type": "bigint",      "name": "bi",  "max": 100, "min": 0 },
                        { "type": "utinyint",    "name": "uti", "max": 100, "min": 0 },
                        { "type": "usmallint",   "name": "usi", "max": 100, "min": 0 },
                        { "type": "uint",        "name": "ui",  "max": 100, "min": 0 },
                        { "type": "ubigint",     "name": "ubi", "max": 100, "min": 0 },
                        { "type": "binary",      "name": "bin", "len": 32},
                        { "type": "nchar",       "name": "nch", "len": 64}
                    ],
                    "tags": [
                        {
                            "type": "int",
                            "name": "groupid",
                            "max": 10000,
                            "min": 1
                        },
                        {
                            "name": "location",
                            "type": "binary",
                            "len": 16,
                            "values": ["San Francisco", "Los Angles", "San Diego",
                                "San Jose", "Palo Alto", "Campbell", "Mountain View",
                                "Sunnyvale", "Santa Clara", "Cupertino"]
                        }
                    ]
                }
            ]
        }
    ]
```

#### 2. 性能数据

以下为执行耗时，单位为秒

##### 2.1 select * from duration_db.meters order by ts  [asc|desc] limit [100|1000|1000]


|  | Limit 100 | Limit 1000 | Limit 10000 |
| --- | --- | --- | --- |
| asc | 10.3 | 10.3 | 58 |
| desc | 8.0 | 9.8 | 61.5 |
| Before optimization, asc | 138 |  |  |

##### 2.2 select cols from duration_db.meters order by ts [asc|desc]

|  |  | Output by duration | Normal output |
| --- | --- | --- | --- |
| ts,ti | asc | 69 | 76 |
|  | desc | 62 | 70 |
| ts | asc | 55 | 64 |
|  | desc | 51 | 56 |
| ts,bi | asc | 69 | 78 |
|  | desc | 65 | 72 |
| ts,nch | asc | 437 | 689 |
|  | desc | 418 | 644 |
| * | asc | 651 | 1111 |
|  | desc | 653 | 1090 |
