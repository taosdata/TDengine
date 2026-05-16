# Tag查询语法及性能优化

#### 1. Syntax

```json
SELECT [TAGS] [DISTINCT] select_list from tables ...
```

If TAGS is present and only tag columns are referenced, tags of all specified child tables are returned. If one normal column is referenced,  the SQL query is executed to fetch rows of each child table, filling the tag column if required. 

#### 2. Performance

##### 2.1 Prepare Test Data

The tag scan is applied on one super table with 400k tables and 3k bytes row and 1 vgroup. The pages and pagesize parameters of database are set so that meta caches the ctb.idx pages.
```json
{
    "filetype": "insert",
    "cfgdir": "/etc/taos",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "connection_pool_size": 8,
    "thread_count": 8,
    "create_table_thread_count": 7,
    "result_file": "./insert_res.txt",
    "confirm_parameter_prompt": "no",
    "insert_interval": 0,
    "interlace_rows": 100,
    "num_of_records_per_req": 100,
    "prepared_rand": 10000,
    "chinese": "no",
    "databases": [
        {
            "dbinfo": {
                "name": "test1",
                "drop": "yes",
                "replica": 1,
                "precision": "ms",
                "keep": 3650,
                "minRows": 100,
                "maxRows": 200,
                "comp": 2,
                "vgroups": 1,
                "pages":1024,
                "pagesize":2048
            },
            "super_tables": [
                {
                    "name": "meters",
                    "child_table_exists": "no",
                    "childtable_count": 400000,
                    "childtable_prefix": "d",
                    "escape_character": "yes",
                    "auto_create_table": "no",
                    "batch_create_tbl_num": 500,
                    "data_source": "rand",
                    "insert_mode": "taosc",
                    "non_stop_mode": "no",
                    "line_protocol": "line",
                    "insert_rows": 5,
                    "interlace_rows": 0,
                    "insert_interval": 0,
                    "partial_col_num": 0,
                    "disorder_ratio": 0,
                    "disorder_range": 1000,
                    "timestamp_step": 10,
                    "start_timestamp": "2020-10-01 00:00:00.000",
                    "sample_format": "csv",
                    "sample_file": "./sample.csv",
                    "use_sample_ts": "no",
                    "tags_file": "",
                    "columns": [
                        {
                            "type": "FLOAT",
                            "name": "current",
                            "count": 1,
                            "max": 12,
                            "min": 8
                        },
                        { "type": "INT", "name": "voltage", "max": 225, "min": 215 },
                        { "type": "FLOAT", "name": "phase", "max": 1, "min": 0 }
                    ],
                    "tags": [
                        {
                            "type": "TINYINT",
                            "name": "t0",
                            "max": 10,
                            "min": 1
                        },
                        {
                            "name": "t1",
                            "type": "BINARY",
                            "len": 16,
                            "values": ["San Francisco", "Los Angles", "San Diego",
                                "San Jose", "Palo Alto", "Campbell", "Mountain View",
                                "Sunnyvale", "Santa Clara", "Cupertino"]
                        },
                        {"name": "t2", "type": "BINARY", "len": 100},
                        {"name": "t3", "type": "BINARY", "len": 200},
                        {"name": "t4", "type": "BINARY", "len": 400},
                        {"name": "t5", "type": "BINARY", "len": 800},
                        {"name": "t6", "type": "BINARY", "len": 1600}
                    ]
                }
            ]
        }
    ]
}
```

#### 3. Test results

| cols | Time | version |
| --- | --- | --- |
| tbname, t0, t1, t2, t3, t4, t5, t6 | 3.9s | Original |
| t0, t1, t2, t3, t4, t5, t6 | 2.2s | optimized |
| tbname,t0 | 1.7s | Original |
| t0 | 1.7s | original |
| t0 | 0.4s | optimized |
| t2 | 1.7s | original |
| t2 | 0.5s | optimized |
