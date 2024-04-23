import datetime
import json

class InsertJson:
    def __init__(self, tables = 10000, records_per_table = 10000, interlace_rows = 0, stt_trigger = 1) -> None:
        self.tables = tables
        self.records_per_table = records_per_table
        self.interlace_rows = interlace_rows
        self.stt_trigger = stt_trigger

    def get_db_cfg(self) -> dict:
        return {
            "name": "test",
            "drop": "true",
            "replica": 1,        
            "precision": "ms",
            "cachemodel": "'both'",
            "keep": 3650,
            "minRows": 100,
            "maxRows": 4096,
            "comp": 2,
            "vgroups": 10,
            "stt_trigger": self.stt_trigger     
        }
    
    def get_stb_cfg(self) -> list:        
        return [
            {
                "name": "meters",
                "child_table_exists": "no",
                "childtable_count": self.tables,
                "childtable_prefix": "d",
                "escape_character": "yes",
                "auto_create_table": "no",
                "batch_create_tbl_num": 5,
                "data_source": "rand",
                "insert_mode": "taosc",
                "non_stop_mode": "no",
                "line_protocol": "line",
                "insert_rows": self.records_per_table,
                "childtable_limit": 10000,
                "childtable_offset": 100,
                "interlace_rows": self.interlace_rows,
                "insert_interval": 0,
                "partial_col_num": 0,
                "disorder_ratio": 0,
                "disorder_range": 1000,
                "timestamp_step": 10,
                "start_timestamp": "2022-10-01 00:00:00.000",
                "sample_format": "csv",
                "sample_file": "./sample.csv",
                "use_sample_ts": "no",
                "tags_file": "",        
                "columns": self.get_column_list(),
                "tags": self.get_tag_list()
            }
        ]
        
    def get_column_list(self) -> list:
        return [
            {"type": "FLOAT", "name": "current", "count": 1, "max": 12, "min": 8},
            {"type": "INT", "name": "voltage", "max": 225, "min": 215},
            {"type": "FLOAT", "name": "phase", "max": 1, "min": 0},
        ]
        
    def get_tag_list(self) -> list:
        return [
            { "type": "TINYINT", "name": "groupid", "max": 10, "min": 1 },
            { "name": "location", "type": "BINARY", "len": 16, "values": ["San Francisco", "Los Angles", "San Diego", "San Jose", "Palo Alto", "Campbell", "Mountain View", "Sunnyvale", "Santa Clara", "Cupertino"]}
        ]

    def get_insert_cfg(self) -> dict:
        return {
            "filetype": "insert",
            "cfgdir": "/etc/taos",
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "thread_count": 10,
            "create_table_thread_count": 7,
            "result_file": "/tmp/insert_res.txt",
            "confirm_parameter_prompt": "no",
            "insert_interval": 0,
            "num_of_records_per_req": 1000,
            "max_sql_len": 1024000,
            "databases": [{
                "dbinfo": self.get_db_cfg(),
                "super_tables": self.get_stb_cfg()
            }]
        }
    
    def create_insert_file(self) -> str:
        date = datetime.datetime.now()
        file_create_table = f"/tmp/insert_{date:%F-%H%M}.json"

        with open(file_create_table, 'w') as f:
            json.dump(self.get_insert_cfg(), f)

        return file_create_table