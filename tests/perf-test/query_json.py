import datetime
import json

class QueryJson:
    def __init__(self, sql, query_times = 1) -> None:
        self.sql = sql
        self.query_times = query_times

    def gen_query_json(self) -> dict:
        return {
            "filetype": "query",
            "cfgdir": "/etc/taos",
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "confirm_parameter_prompt": "no",
            "databases": "test",
            "query_times": self.query_times,
            "query_mode": "taosc",
            "specified_table_query": {
                "query_interval": 1,
                "concurrent": 1,
                "sqls": [
                    {
                        "sql": "%s" % self.sql,
                        "result": "./query_res.txt"
                    }
                ]
            }
            
        }
    
    def create_query_file(self) -> str:
        date = datetime.datetime.now()
        file_create_table = f"/tmp/query_{date:%F-%H%M}.json"

        with open(file_create_table, 'w') as f:
            json.dump(self.gen_query_json(), f)

        return file_create_table