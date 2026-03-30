# [Test Report] - TD-25972 Table merge scan性能优化

## 1. 测试场景

1supertable, 5000 subtables, 36000 records each table, each table has 10 durations.
```json
{
    "filetype": "insert",
    "cfgdir": "/etc/taos",
    "host": "u1-44",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "connection_pool_size": 8,
    "thread_count": 10,
    "create_table_thread_count": 7,
    "result_file": "./insert_res.txt",
    "confirm_parameter_prompt": "no",
    "insert_interval": 0,
    "interlace_rows": 0,
    "num_of_records_per_req": 1000,
    "prepared_rand": 1000,
    "chinese": "no",
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
}
```

## 2. 测试结果

main分支未优化，3.0分支已优化，经过对比都有不同程度的优化提升，从1.5倍到35倍不等。

| 测试sql | main分支 | 3.0分支 | 备注 |
| --- | --- | --- | --- |
| select count(*) from duration_db.meters; | 0.197658s | 0.184934s |  |
| select * from duration_db.meters order by ts ; | 1473.134865s | 938.457938s |  |
| select * from duration_db.meters order by ts desc ; | 1498.916719s | 935.225232s |  |
| select * from duration_db.meters order by ts limit 100; | 207.603394s | 19.249942s |  |
| select * from duration_db.meters order by ts desc limit 100; | 249.711827s | 19.380677s |  |
| select * from duration_db.meters order by ts limit 1000; | 209.646330s | 20.045735s |  |
| select * from duration_db.meters order by ts desc limit 1000; | 215.971953s | 20.660773s |  |
| select * from duration_db.meters order by ts limit 10000; | 212.550631s | 99.077901s |  |
| select * from duration_db.meters order by ts desc limit 10000; | 213.152815s | 101.507740s |  |
| select ts from duration_db.meters order by ts limit 1000; | 16.873433s | 0.668101s |  |
| select ts from duration_db.meters order by ts desc limit 1000; | 17.159128s | 0.500326s |  |
| select ts,nch from duration_db.meters order by ts limit 1000; | 172.273868s | 15.858801s |  |
| select ts,nch from duration_db.meters order by ts desc limit 1000; | 175.367394s | 17.473644s |  |
| select elapsed(ts) from duration_db.meters ; | 325.056933s | 102.020744s | 以前3.0版本中出现过卡死，修复后正常了 |
| select interp(ic,0) from duration_db.meters range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(next); | 1537.842667s | 123.479261s | 同上 |
| show table distributed duration_db.meters; | 0.103372s | 0.102647s | 同上 |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |
