from typing import Dict, Any, Union
from ..errors import ParamTypeError

import os
import json

DEFAULT_INSERT_CONFIG = {
    "filetype": "insert",
    "cfgdir": "/etc/taos",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "thread_count": 4,
    "thread_count_create_tbl": 4,
    "result_file": "./insert_res.txt",
    "confirm_parameter_prompt": "no",
    "insert_interval": 0,
    "num_of_records_per_req": 100,
    "max_sql_len": 1024000,
    "databases": [{
        "dbinfo": {
            "name": "db",
            "drop": "yes",
            "replica": 1,
            "days": 10,
            "cache": 16,
            "blocks": 8,
            "precision": "ms",
            "keep": 36500,
            "minRows": 100,
            "maxRows": 4096,
            "comp": 2,
            "walLevel": 1,
            "cachelast": 0,
            "quorum": 1,
            "fsync": 3000,
            "update": 0
        },
        "super_tables": [{
            "name": "stb",
            "child_table_exists": "no",
            "childtable_count": 10000,
            "childtable_prefix": "stb_",
            "auto_create_table": "no",
            "data_source": "rand",
            "insert_mode": "taosc",
            "insert_rows": 100000,
            "interlace_rows": 0,
            "max_sql_len": 1024000,
            "disorder_ratio": 0,
            "disorder_range": 1000,
            "timestamp_step": 1,
            "start_timestamp": "2020-10-01 00:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "tags_file": "",
            "columns": [{"type": "INT"}, {"type": "DOUBLE", "count": 10}, {"type": "BINARY", "len": 16, "count": 3}, {"type": "BINARY", "len": 32, "count": 6}],
            "tags": [{"type": "TINYINT", "count": 2}, {"type": "BINARY", "len": 16, "count": 5}]
        }]
    }]
}

DEFAULT_QUERY_CONFIG = {
    "filetype": "query",
    "cfgdir": "/etc/taos",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "confirm_parameter_prompt": "yes",
    "databases": "dbx",
    "query_times": 1,
    "specified_table_query": {
        "query_interval": 1,
        "concurrent": 4,
        "sqls": [
            {
                "sql": "select last_row(*) from stb where color='red'",
                "result": "./query_res0.txt"
            },
            {
                "sql": "select count(*) from stb_01",
                "result": "./query_res1.txt"
            }
        ]
    },
    "super_table_query": {
        "stblname": "stb",
        "query_interval": 1,
        "threads": 4,
        "sqls": [
            {
                "sql": "select last_row(*) from xxxx",
                "result": "./query_res2.txt"
            }
        ]
    }
}


def gen_benchmark_json(out_dir: str, out_file_name: str,
                       base_config: Union[str, Dict[str, Any]],
                       updates: Dict[str, Any] = None, **kwargs) -> None:
    """
    This is an utility method for generating taosBenchmark's configuration file.
    You can create a brand new json file or update existing one.
    You can specify configuration keys by dict or by keyword arguments.
    For code samples please visit https://github.com/taosdata/taos-test-framework/wiki/genBenchmarkJson
    @param out_dir: out put directory
    @param out_file_name: out put file name
    @param base_config: An existing configuration file path or a dict that new configuration file will base on.
    @param updates:Keys will be updated or added to base_config
    @param kwargs: Keys will be updated or added to base_config in key word argument form
    @return: None
    """
    if isinstance(base_config, dict):
        result = base_config
    elif isinstance(base_config, str):
        with open(base_config) as f:
            result = json.load(f)
    else:
        raise ParamTypeError("only dict and str are allowed for base_config")

    if updates:
        for k, v in updates.items():
            _update_dict_by_dot_path(result, k, v)
    for k, v in kwargs.items():
        result[k] = v
    out_file = os.path.join(out_dir, out_file_name)
    with open(out_file, mode="w", encoding="utf8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


def _update_dict_by_dot_path(d: dict, dot_key: str, v: Any):
    ks = dot_key.split(".")
    cur = d
    while len(ks) > 1:
        k = ks.pop(0)
        if k.isdigit():
            cur = cur[int(k)]
        else:
            cur = cur[k]
    cur[ks[0]] = v
