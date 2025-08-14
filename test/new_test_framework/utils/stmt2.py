# -*- coding: utf-8 -*-

import taos
from .log import tdLog

class TDStmt2:
    """
    TDengine Stmt2 utility class 
    """
    
    def __init__(self):
        self.conn = None
        self.stmt = None
        self._is_prepared = False
    
    def init(self, conn):
        """
        Initialize stmt2 with connection
        """
        self.conn = conn
        if self.stmt:
            self.stmt.close()
        self.stmt = None
        self._is_prepared = False
        tdLog.debug("TDStmt2 initialized with connection")
        return self
    
    def reconnect(self, **kwargs):
        """
        Reconnect to the database with new parameters
        
        Args:
            **kwargs: Connection parameters like host, user, password, etc.
            
        Returns:
            self for method chaining
        """
        if self.conn:
            self.conn.close()
        
        self.conn = taos.connect(**kwargs)
        if self.stmt:
            self.stmt.close()
        self.stmt = None
        self._is_prepared = False
        return self
          
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
            datas: List of row-oriented data for each table
            
        Returns:
            self for method chaining
            
        Example:
            # Row-oriented data format (easier to use)
            tbnames = ["d_bind_0", "d_bind_1"]
            tags = [[0, "location_0"], [1, "location_1"]]  
            datas = [
                # table 0 data: 2 rows, 4 columns each
                [
                    [ts1, curr1, volt1, phase1],  # row 1
                    [ts2, curr2, volt2, phase2]   # row 2
                ],
                # table 1 data: 2 rows, 4 columns each  
                [
                    [ts3, curr3, volt3, phase3],  # row 1
                    [ts4, curr4, volt4, phase4]   # row 2
                ]
            ]
        """
        try:
            if self.conn is None:
                raise ValueError("Connection is not established. Please call connect() first.")
                
            if not self._is_prepared:
                raise ValueError("Statement must be prepared before binding parameters")
            
            # Convert row-oriented data to column-oriented data for each table
            converted_datas = []
            
            for table_idx, table_data in enumerate(datas):
                if not isinstance(table_data, list) or len(table_data) == 0:
                    raise ValueError(f"Table {table_idx} data must be a non-empty list")
                
                num_cols = len(table_data[0])
                for row_idx, row in enumerate(table_data):
                    if len(row) != num_cols:
                        raise ValueError(f"Table {table_idx}, row {row_idx}: inconsistent column count")
                
                # Convert from row-oriented to column-oriented
                # Input:  [[ts1, curr1, volt1], [ts2, curr2, volt2]]
                # Output: [[ts1, ts2], [curr1, curr2], [volt1, volt2]]
                column_data = []
                for col_idx in range(num_cols):
                    column_values = [row[col_idx] for row in table_data]
                    column_data.append(column_values)
                
                converted_datas.append(column_data)
            
            self.stmt.bind_param(tbnames, tags, converted_datas)
            
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
                tdLog.error("Connection is not established")
                return 0
                
            if self.stmt:
                return self.stmt.affect_rows()
            return 0
        except Exception as e:
            tdLog.error(f"Failed to get affected rows: {e}")
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
                tdLog.error(f"Error closing statement: {e}")
            finally:
                self.stmt = None
                self._is_prepared = False
    
    def execute_stmt(self, sql, params=None, batch_params=None, tbnames=None, tags=None, datas=None, check_affected=True, expected_rows=None):
        """
        Execute a complete statement with prepare, bind, execute and affect_rows check
        
        Args:
            sql (str): SQL statement with ? placeholders
            params (list, optional): Parameters for single row insert
            batch_params (list, optional): Parameters for batch insert
            tbnames (list, optional): Table names for super table operations
            tags (list, optional): Tag values for super table operations  
            datas (list, optional): Column data for super table operations
            check_affected (bool): Whether to check and return affected rows
            expected_rows (int, optional): Expected number of affected rows for validation
            
        Returns:
            Number of affected rows if check_affected=True, otherwise None
            
        Raises:
            AssertionError: When expected_rows is provided and doesn't match actual affected rows
            
        Examples:
            # Single row insert with expected rows check
            affected = stmt.execute_stmt(
                "INSERT INTO sensor_data VALUES (?, ?, ?, ?)",
                params=[timestamp, temp, humidity, location],
                expected_rows=1
            )
            
            # Batch insert with expected rows check
            affected = stmt.execute_stmt(
                "INSERT INTO sensor_data VALUES (?, ?, ?, ?)",
                batch_params=[[ts1, temp1, hum1, loc1], [ts2, temp2, hum2, loc2]],
                expected_rows=2
            )
            
            # Super table insert with expected rows check
            affected = stmt.execute_stmt(
                "INSERT INTO ? USING device_metrics TAGS(?, ?, ?) VALUES (?, ?, ?)",
                tbnames=["device_001"], 
                tags=[["device_001", 1, "Building_A"],["device_002", 2, "Building_B"]], 
                datas=datas = [[[ts1,100.1, 1]],[[ts2, 200.1, 2],[ts3, 200.2, 2]]],
                expected_rows=3
            )
        """
        try:
            self.prepare(sql)
            
            param_count = sum([
                1 if params is not None else 0,
                1 if batch_params is not None else 0,
                1 if all(x is not None for x in [tbnames, tags, datas]) else 0
            ])
            
            if param_count == 0:
                raise ValueError("At least one parameter type must be provided: params, batch_params, or super table data")
            elif param_count > 1:
                raise ValueError("Only one parameter type should be provided at a time")
            
            if params is not None:
                # Single row insert
                self.bind_params(params)
            elif batch_params is not None:
                # Batch insert
                self.bind_batch_params(batch_params)
            elif all(x is not None for x in [tbnames, tags, datas]):
                # Super table insert
                self.bind_super_table_data(tbnames, tags, datas)
            
            affected_rows = self.execute()
            
            if expected_rows is not None:
                if affected_rows != expected_rows:
                    error_msg = f"Expected {expected_rows} affected rows, but got {affected_rows}"
                    tdLog.error(error_msg)
                    raise AssertionError(error_msg)
                else:
                    tdLog.info(f"Expected rows validation passed: {affected_rows} rows affected")
                
            if check_affected:
                tdLog.debug(f"Statement executed successfully, affected rows: {affected_rows}")
                return affected_rows
            else:
                tdLog.debug("Statement executed successfully")
                return None
                
        except Exception as e:
            tdLog.error(f"Failed to execute complete statement: {e}")
            raise
        finally:
            self.close()

    def execute_single(self, sql, params, check_affected=True, expected_rows=None):
        """
        Convenience method for single row insert
        
        Args:
            sql (str): SQL statement 
            params (list): Single row parameters
            check_affected (bool): Whether to check affected rows
            expected_rows (int, optional): Expected number of affected rows
            
        Returns:
            Number of affected rows if check_affected=True
            
        Examples:
            # Insert with expected rows validation
            affected = stmt.execute_single(
                "INSERT INTO sensor_data VALUES (?, ?, ?, ?)",
                [timestamp, temp, humidity, location],
                expected_rows=1
            )
        """
        return self.execute_stmt(sql, params=params, check_affected=check_affected, expected_rows=expected_rows)

    def execute_batch(self, sql, batch_params, check_affected=True, expected_rows=None):
        """
        Convenience method for batch insert
        
        Args:
            sql (str): SQL statement
            batch_params (list): Batch parameters (list of lists)
            check_affected (bool): Whether to check affected rows
            expected_rows (int, optional): Expected number of affected rows
            
        Returns:
            Number of affected rows if check_affected=True
            
        Examples:
            # Batch insert with expected rows validation
            batch_data = [[ts1, v1], [ts2, v2], [ts3, v3]]
            affected = stmt.execute_batch(
                "INSERT INTO table VALUES (?, ?)",
                batch_data,
                expected_rows=3
            )
        """
        return self.execute_stmt(sql, batch_params=batch_params, check_affected=check_affected, expected_rows=expected_rows)

    def execute_super_table(self, sql, tbnames, tags, datas, check_affected=True, expected_rows=None):
        """
        Convenience method for super table operations
        
        Args:
            sql (str): SQL statement
            tbnames (list): Table names
            tags (list): Tag values
            datas (list): Column data
            check_affected (bool): Whether to check affected rows
            expected_rows (int, optional): Expected number of affected rows
            
        Returns:
            Number of affected rows if check_affected=True
            
        Examples:
            # Super table insert with expected rows check
            affected = stmt.execute_stmt(
                "INSERT INTO ? USING device_metrics TAGS(?, ?, ?) VALUES (?, ?, ?)",
                tbnames=["device_001"], 
                tags=[["device_001", 1, "Building_A"],["device_002", 2, "Building_B"]], 
                datas=datas = [[[ts1,100.1, 1]],[[ts2, 200.1, 2],[ts3, 200.2, 2]]],
                expected_rows=3
            )
        """
        return self.execute_stmt(sql, tbnames=tbnames, tags=tags, datas=datas, check_affected=check_affected, expected_rows=expected_rows)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

tdStmt2 = TDStmt2()