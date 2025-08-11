# -*- coding: utf-8 -*-

"""
Example showing how to use the stmt2 API
"""

from new_test_framework.utils import tdLog, tdCom
from new_test_framework.utils.stmt2 import TDStmt2
import os
import taos
from taos import *
import time

class TestStmt2:
    """Test case demonstrating stmt2 API usage"""
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def newcon(self, host, cfg):
        """Create new connection"""
        user = "root"
        password = "taosdata"
        port = 6030
        con = taos.connect(host=host, user=user, password=password, config=cfg, port=port)
        tdLog.debug(f"Connected to TDengine: {con}")
        return con

    def run_basic_stmt2(self, conn):
        """Test basic stmt2 operations """
        
        dbname = "stmt2_test"
        conn.execute(f"DROP DATABASE IF EXISTS {dbname}")
        conn.execute(f"CREATE DATABASE {dbname}")
        conn.select_db(dbname)
        
        conn.execute("CREATE TABLE sensor_data (ts TIMESTAMP, temperature FLOAT, humidity INT, location BINARY(50))")
        
        # Test 1: Single record insert using stmt2
        stmt = TDStmt2(conn)
        try:
            sql = "INSERT INTO sensor_data VALUES (?, ?, ?, ?)"
            stmt.prepare(sql)
            
            import time
            current_ts = int(time.time() * 1000) 
            params = [current_ts, 25.5, 60, "Beijing"]
            stmt.bind_params(params)
            
            affected_rows = stmt.execute()
            assert affected_rows == 1, f"Should insert 1 record, got {affected_rows}"
            tdLog.debug(f"Single insert: {affected_rows} rows affected")
        finally:
            stmt.close()
        
        # Test 2: Batch insert using stmt2
        stmt = TDStmt2(conn)
        try:
            sql = "INSERT INTO sensor_data VALUES (?, ?, ?, ?)"
            stmt.prepare(sql)
            
            base_ts = int(time.time() * 1000)
            batch_params = [
                [base_ts + 1000, 26.1, 65, "Shanghai"],
                [base_ts + 2000, 24.8, 58, "Guangzhou"],
                [base_ts + 3000, 27.2, 70, "Shenzhen"]
            ]
            stmt.bind_batch_params(batch_params)
            
            affected_rows = stmt.execute()
            assert affected_rows == 3, f"Should insert 3 records, got {affected_rows}"
            tdLog.debug(f"Batch insert: {affected_rows} rows affected")
        finally:
            stmt.close()
        
        tdLog.debug("All basic stmt2 operations completed successfully")
    
    def run_super_table(self, conn):
        """Test super table operations with stmt2 API"""
        
        dbname = "stmt2_stable_test"
        conn.execute(f"DROP DATABASE IF EXISTS {dbname}")
        conn.execute(f"CREATE DATABASE {dbname}")
        conn.select_db(dbname)
        
        conn.execute("CREATE TABLE device_metrics (ts TIMESTAMP,val DOUBLE,status INT) TAGS (device_id BINARY(50),type INT,location BINARY(100))")
        
        stmt = TDStmt2()
        stmt.connect(host="localhost",port=6030,username="root",passwd="taosdata", database=dbname)
        try:
            sql = "INSERT INTO ? USING device_metrics TAGS(?, ?, ?) VALUES (?, ?, ?)"
            stmt.prepare(sql)
            
            # Prepare data for stmt2 super table insert
            import time
            current_ts = int(time.time() * 1000)
            
            tbnames = ["device_001_data"]  
            tags = [["device_001", 1, "Building_A"]]  
            datas = [[[current_ts], [100.5], [1]]] 
            
            stmt.bind_super_table_data(tbnames, tags, datas)
            
            affected_rows = stmt.execute()
            assert affected_rows == 1, f"Should insert 1 record, got {affected_rows}"
            tdLog.debug(f"Super table insert: {affected_rows} rows affected")
        finally:
            stmt.close()

    def run_batch_insert(self, conn):
        """Test stmt2 API"""
        
        dbname = "stmt2_batch_test"
        conn.execute(f"DROP DATABASE IF EXISTS {dbname}")
        conn.execute(f"CREATE DATABASE {dbname}")
        conn.select_db(dbname)
        
        conn.execute("CREATE TABLE batch_test (ts TIMESTAMP, val1 INT, val2 DOUBLE, val3 BINARY(50))")
        
        batch_size = 1000
        base_ts = int(time.time() * 1000)  
        
        start_time = time.time()
        
        stmt = TDStmt2(conn)
        try:
            sql = "INSERT INTO batch_test VALUES (?, ?, ?, ?)"
            stmt.prepare(sql)
            
            batch_params = []
            for i in range(batch_size):
                params = [base_ts + i * 1000, i, float(i * 0.1), f"data_{i}"]
                batch_params.append(params)
            
            stmt.bind_batch_params(batch_params)
            affected_rows = stmt.execute()
            
            assert affected_rows == batch_size, f"Should insert {batch_size} records, got {affected_rows}"
            
        finally:
            stmt.close()
        
        end_time = time.time()
        
        # Verify the data
        result = conn.query("SELECT COUNT(*) FROM batch_test")
        count = result.fetch_all()[0][0]
        assert count == batch_size, f"Should insert {batch_size} records, got {count}"
        
        tdLog.info(f"Insert of {batch_size} records took: {end_time - start_time:.4f} seconds")

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
        config = tdCom.getClientCfgPath()
        host = "localhost"
        conn = self.newcon(host, config)
        
        self.run_basic_stmt2(conn)
        self.run_super_table(conn)
        self.run_batch_insert(conn)
        
        tdLog.success(f"{__file__} successfully executed")
        
        conn.close()
