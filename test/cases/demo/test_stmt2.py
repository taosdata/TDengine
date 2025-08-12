# -*- coding: utf-8 -*-

"""
Example showing how to use the stmt2 API
"""

from new_test_framework.utils import tdLog
from new_test_framework.utils.stmt2 import tdStmt2
import time

class TestStmt2:
    """Test case demonstrating stmt2 API usage"""
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls._conn = cls.conn

    def run_basic_stmt2(self, conn):
        """Test basic stmt2 operations """
        
        dbname = "stmt2_test"
        conn.execute(f"DROP DATABASE IF EXISTS {dbname}")
        conn.execute(f"CREATE DATABASE {dbname}")
        conn.select_db(dbname)
        conn.execute("CREATE TABLE sensor_data (ts TIMESTAMP, temperature FLOAT, humidity INT, location BINARY(50))")
        
        # Single record insert using stmt2
        sql = "INSERT INTO sensor_data VALUES (?, ?, ?, ?)"
        current_ts = int(time.time() * 1000) 
        params = [current_ts, 25.5, 60, "Beijing"]
        
        affected_rows = tdStmt2.execute_single(sql, params, check_affected=True, expected_rows=1)
        tdLog.debug(f"Single insert: {affected_rows} rows affected")
    
    def run_super_table(self, conn):
        """Test super table operations with stmt2 API"""
        
        dbname = "stmt2_stable_test"
        conn.execute(f"DROP DATABASE IF EXISTS {dbname}")
        conn.execute(f"CREATE DATABASE {dbname}")
        conn.select_db(dbname)
        conn.execute("CREATE TABLE device_metrics (ts TIMESTAMP,val DOUBLE,status INT) TAGS (device_id BINARY(50),type INT,location BINARY(100))")
        
        sql = "INSERT INTO ? USING device_metrics TAGS(?, ?, ?) VALUES (?, ?, ?)"
        current_ts = int(time.time() * 1000)
        tbnames = ["device_001_data", "device_002_data", "device_003_data"]
    
        tags = [
            ["device_001", 1, "Building_A"],
            ["device_002", 2, "Building_B"], 
            ["device_003", 1, "Building_C"]
        ]
        
        datas = [
            # device_001_data
            [
                [current_ts,      100.1, 1],
                [current_ts+1000, 100.2, 1],
                [current_ts+2000, 100.3, 1]
            ],
            # device_002_data
            [
                [current_ts+3000, 200.1, 2],
                [current_ts+4000, 200.2, 2]
            ],
            # device_003_data
            [
                [current_ts+5000, 300.1, 1],
                [current_ts+6000, 300.2, 1],
                [current_ts+7000, 300.3, 1],
                [current_ts+8000, 300.4, 1]
            ]
        ]
        
        affected_rows = tdStmt2.execute_super_table(sql, tbnames, tags, datas, check_affected=True, expected_rows=9)
        tdLog.debug(f"Super table insert: {affected_rows} rows affected")


    def run_batch_insert(self, conn):
        """Test stmt2 API"""
        
        dbname = "stmt2_batch_test"
        conn.execute(f"DROP DATABASE IF EXISTS {dbname}")
        conn.execute(f"CREATE DATABASE {dbname}")
        conn.select_db(dbname)
        conn.execute("CREATE TABLE batch_test (ts TIMESTAMP, val1 INT, val2 DOUBLE, val3 BINARY(50))")
        
        batch_size = 1000
        base_ts = int(time.time() * 1000)  
        sql = "INSERT INTO batch_test VALUES (?, ?, ?, ?)"
        batch_params = []
        for i in range(batch_size):
            params = [base_ts + i * 1000, i, float(i * 0.1), f"data_{i}"]
            batch_params.append(params)
        
        affected_rows = tdStmt2.execute_batch(sql, batch_params, check_affected=True, expected_rows=batch_size)
        tdLog.debug(f"Batch insert: {affected_rows} rows affected")
                    
        # Verify the data if needed
        result = conn.query("SELECT COUNT(*) FROM batch_test")
        count = result.fetch_all()[0][0]
        assert count == batch_size, f"Should insert {batch_size} records, got {count}"
        

    def test_stmt2(self):
        """summary: example of using stmt2 API

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
        - xxx:xxx

        History:
        - xxx
        - xxx

        """   
        self.run_basic_stmt2(self._conn)
        self.run_super_table(self._conn)
        self.run_batch_insert(self._conn)
        
        tdLog.success(f"{__file__} successfully executed")
