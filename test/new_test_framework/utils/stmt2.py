# -*- coding: utf-8 -*-

import taos
from taos import *
from ctypes import *
from .log import tdLog

class TDStmt2:
    """
    TDengine Stmt2 utility class 
    """
    
    def __init__(self, conn=None):
        """
        Initialize stmt2 with connection
        
        Args:
            conn: TDengine connection object
        """
        self.conn = conn
        self.stmt = None
        self._is_prepared = False
    
    def connect(self, host="localhost", port=6030, username="root", passwd="taosdata", database=None, **kwargs):
        """
        Connect to TDengine server
        
        Args:
            host (str): TDengine server host
            port (int): TDengine server port  
            username (str): Username
            passwd (str): Password
            database (str): Default database
            **kwargs: Additional connection parameters
        """
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
        
        if self.stmt:
            try:
                self.stmt.close()
            except:
                pass
            self.stmt = None
            self._is_prepared = False
            
        conn_params = {
            'host': host,
            'port': port,
            'user': username,
            'password': passwd,
        }
        
        if database:
            conn_params['database'] = database
            
        conn_params.update(kwargs)
        
        try:
            self.conn = taos.connect(**conn_params)
            tdLog.info(f"Connected to TDengine at {host}:{port}")
            return self
        except Exception as e:
            tdLog.error(f"Failed to connect: {e}")
            raise
          
    def prepare(self, sql):
        """
        Prepare a statement using stmt2 API
        
        Args:
            sql: SQL statement with ? placeholders
            
        Returns:
            self for method chaining
        """
        try:
            # Check if connection exists
            if self.conn is None:
                raise ValueError("Connection is not established. Please call connect() first.")
            
            if self.stmt:
                try:
                    self.stmt.close()
                except:
                    pass

            self.stmt = self.conn.statement2(sql)
            self._is_prepared = True
            tdLog.debug(f"Prepared statement: {sql}")
            return self
        except Exception as e:
            tdLog.error(f"Failed to prepare statement: {e}")
            raise

    def bind_params(self, params):
        """
        Bind parameters to statement for simple INSERT
        
        Args:
            params: List of parameter values for a single row
            
        Returns:
            self for method chaining
        """
        try:
            if self.conn is None:
                raise ValueError("Connection is not established. Please call connect() first.")
                
            if not self._is_prepared:
                raise ValueError("Statement must be prepared before binding parameters")
            
            if not isinstance(params, (list, tuple)):
                raise ValueError("Parameters must be a list or tuple")
            
            # For stmt2 single row insert, we need to convert row data to column data
            column_data = [[param] for param in params]
        
            self.stmt.bind_param(None, None, [column_data])
            
            tdLog.debug("Parameters bound successfully")
            return self
        except Exception as e:
            tdLog.error(f"Failed to bind parameters: {e}")
            raise
        
    def bind_batch_params(self, params):
        """
        Bind batch parameters to statement
        
        Args:
            params: List of parameter batches (list of lists)
                    Example: [[ts1, temp1, hum1, loc1], [ts2, temp2, hum2, loc2], ...]
            
        Returns:
            self for method chaining
        """
        try:
            if self.conn is None:
                raise ValueError("Connection is not established. Please call connect() first.")
                
            if not self._is_prepared:
                raise ValueError("Statement must be prepared before binding parameters")
            
            if not isinstance(params, list) or len(params) == 0:
                raise ValueError("Batch parameters must be a non-empty list")
            
            # Convert row-oriented batch data to column-oriented data
            # Input: [[ts1, temp1, hum1, loc1], [ts2, temp2, hum2, loc2], [ts3, temp3, hum3, loc3]]
            # Output: [[ts1, ts2, ts3], [temp1, temp2, temp3], [hum1, hum2, hum3], [loc1, loc2, loc3]]
            
            num_cols = len(params[0])
            column_data = []
            
            for col_idx in range(num_cols):
                column_values = [row[col_idx] for row in params]
                column_data.append(column_values)
            
            self.stmt.bind_param(None, None, [column_data])
            
            tdLog.debug("Batch parameters bound successfully")
            return self
        except Exception as e:
            tdLog.error(f"Failed to bind batch parameters: {e}")
            raise
        
    def bind_super_table_data(self, tbnames, tags, datas):
        """
        Bind data for super table operations 
        
        Args:
            tbnames: List of sub-table names
            tags: List of tag values for each table 
            datas: List of column-oriented data for each table
            
        Returns:
            self for method chaining
            
        Example:
            # example data structure
            tbnames = ["d_bind_0", "d_bind_1"]
            tags = [[0, "location_0"], [1, "location_1"]]  
            datas = [
                [[ts1, ts2], [curr1, curr2], [volt1, volt2], [phase1, phase2]],  # table 0 data
                [[ts3, ts4], [curr3, curr4], [volt3, volt4], [phase3, phase4]]   # table 1 data
            ]
        """
        try:
            if self.conn is None:
                raise ValueError("Connection is not established. Please call connect() first.")
                
            if not self._is_prepared:
                raise ValueError("Statement must be prepared before binding parameters")
            
            self.stmt.bind_param(tbnames, tags, datas)
            
            tdLog.debug("Super table data bound successfully")
            return self
        except Exception as e:
            tdLog.error(f"Failed to bind super table data: {e}")
            raise
    
    def execute(self):
        """
        Execute the prepared statement
        
        Returns:
            Number of affected rows
        """
        try:
            if self.conn is None:
                raise ValueError("Connection is not established. Please call connect() first.")
                
            if not self._is_prepared:
                raise ValueError("Statement must be prepared before execution")
                
            affected_rows = self.stmt.execute()
            tdLog.debug(f"Statement executed, affected rows: {affected_rows}")
            return affected_rows
        except Exception as e:
            tdLog.error(f"Failed to execute statement: {e}")
            raise

    def affect_rows(self):
        """
        Get affected rows count
        
        Returns:
            Number of affected rows
        """
        try:
            if self.conn is None:
                tdLog.warning("Connection is not established")
                return 0
                
            if self.stmt:
                return self.stmt.affect_rows()
            return 0
        except Exception as e:
            tdLog.warning(f"Failed to get affected rows: {e}")
            return 0
            
    def close(self):
        """
        Close the statement
        """
        if self.stmt:
            try:
                self.stmt.close()
                tdLog.debug("Statement closed")
            except Exception as e:
                tdLog.warning(f"Error closing statement: {e}")
            finally:
                self.stmt = None
                self._is_prepared = False
        
    def __exit__(self):
        self.close()