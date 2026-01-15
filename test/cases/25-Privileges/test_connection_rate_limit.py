# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, TDSql
import taos
import time
import signal

class TimeoutError(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutError("Test timeout exceeded 5 minutes")

class TestConnectionRateLimit:
    """Test connection rate limiting basic functionality
    
    This test validates basic rate limiting functionality:
    1. Connection rate limiting
    2. Query rate limiting
    3. Rate limit recovery
    
    Since: v3.4.0.0
    Labels: common,ci,rate-limit
    History:
        - 2025-01-12 Basic functionality tests
    """
    
    test_connections = []
    test_db = "test_rate_limit"
    
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.prepare_minimal_data()
        
    def prepare_minimal_data(self):
        """Prepare minimal test data (10 records)"""
        try:
            tdSql.execute(f"drop database if exists {self.test_db}")
        except:
            pass
            
        tdSql.execute(f"create database {self.test_db}")
        tdSql.execute(f"use {self.test_db}")
        tdSql.execute("create table simple_test (ts timestamp, value int, user_id int)")
        
        # Insert 10 records for testing
        for i in range(10):
            tdSql.execute(f"insert into simple_test values (now+{i}s, {i}, {i%3})")
        
        tdLog.info("Prepared minimal test data with 10 records")
    
    def is_rate_limit_error(self, error_msg):
        """Check if error message indicates rate limiting"""
        rate_indicators = ['rate', 'limit', 'many', 'exceed', 'frequent', 'throttle']
        error_msg_lower = error_msg.lower()
        return any(ind in error_msg_lower for ind in rate_indicators)
    
    def cleanup_test_data(self):
        """Clean up test resources"""
        try:
            tdSql.execute(f"drop database if exists {self.test_db}")
        except:
            pass
        
        for conn in self.test_connections:
            try:
                conn.close()
            except:
                pass
        self.test_connections = []
    
    def test_connection_rate_limit(self):
        """Test basic connection rate limiting
        
        1. Attempt rapid connections to trigger rate limit
        2. Verify rate limit error messages
        3. Test normal connection behavior
        """
        tdLog.info("========== step1: test connection rate limiting ==========")
        
        # Test normal connection first
        tdLog.info("Testing normal connection...")
        try:
            conn1 = taos.connect()
            conn1.close()
            tdLog.info("Normal connection successful")
        except Exception as e:
            tdLog.exit(f"Normal connection failed: {e}")
        
        # Test rapid connections with smaller limit expectation
        tdLog.info("Testing 15 rapid connections to trigger rate limit...")
        rapid_connections = []
        rate_limit_triggered = False
        rate_limit_error = None
        
        for i in range(15):
            try:
                start_time = time.time()
                conn = taos.connect()
                conn.close()
                end_time = time.time()
                rapid_connections.append({
                    'success': True,
                    'duration': end_time - start_time,
                    'index': i
                })
            except Exception as e:
                end_time = time.time()
                rapid_connections.append({
                    'success': False,
                    'error': str(e),
                    'duration': end_time - start_time,
                    'index': i
                })
                
                if self.is_rate_limit_error(str(e)):
                    rate_limit_triggered = True
                    rate_limit_error = str(e)
                    tdLog.info(f"Rate limit triggered at connection {i}: {e}")
                    break
        
        # Analyze results
        successful_connections = sum(1 for c in rapid_connections if c['success'])
        failed_connections = len(rapid_connections) - successful_connections
        
        tdLog.info(f"Successful connections: {successful_connections}")
        tdLog.info(f"Failed connections: {failed_connections}")
        tdLog.info(f"Rate limit triggered: {rate_limit_triggered}")
        
        # Verify rate limit was triggered
        if not rate_limit_triggered:
            tdLog.exit(f"FAILED: Rate limit was not triggered by 15 rapid connections. Test FAILED.")
        
        tdLog.info("Connection rate limiting test PASSED")
    
    def test_query_rate_limit(self):
        """Test query rate limiting per connection
        
        1. Execute rapid queries on single connection
        2. Verify query rate limit activation
        3. Test different query types
        """
        tdLog.info("========== step2: test query rate limiting ==========")
        
        conn, testSql = self.create_test_connection()
        self.test_connections.append(conn)
        
        # Test rapid queries to trigger rate limit
        tdLog.info("Testing 25 rapid queries to trigger rate limit...")
        query_results = []
        rate_limit_triggered = False
        rate_limit_error = None
        
        for i in range(25):
            try:
                start_time = time.time()
                testSql.query(f"select * from simple_test where user_id = {i % 3}")
                end_time = time.time()
                query_results.append({
                    'success': True,
                    'duration': end_time - start_time,
                    'query': i
                })
            except Exception as e:
                end_time = time.time()
                query_results.append({
                    'success': False,
                    'error': str(e),
                    'duration': end_time - start_time,
                    'query': i
                })
                
                if self.is_rate_limit_error(str(e)):
                    rate_limit_triggered = True
                    rate_limit_error = str(e)
                    tdLog.info(f"Query rate limit triggered at query {i}: {e}")
                    break
        
        # Analyze query results
        successful_queries = sum(1 for q in query_results if q['success'])
        failed_queries = len(query_results) - successful_queries
        
        tdLog.info(f"Successful queries: {successful_queries}")
        tdLog.info(f"Failed queries: {failed_queries}")
        tdLog.info(f"Query rate limit triggered: {rate_limit_triggered}")
        
        # Verify rate limit was triggered
        if not rate_limit_triggered:
            tdLog.exit(f"FAILED: Query rate limit was not triggered by 25 rapid queries. Test FAILED.")
        
        tdLog.info("Query rate limiting test PASSED")
    
    def test_rate_limit_recovery(self):
        """Test rate limit recovery after timeout
        
        1. Trigger rate limit
        2. Wait for recovery period
        3. Verify normal operation resumes
        """
        tdLog.info("========== step3: test rate limit recovery ==========")
        
        # First, trigger rate limit with rapid connections
        tdLog.info("Triggering rate limit with rapid connections...")
        rate_limit_triggered = False
        rate_limit_time = None
        
        for i in range(20):
            try:
                conn = taos.connect()
                conn.close()
            except Exception as e:
                if self.is_rate_limit_error(str(e)):
                    rate_limit_triggered = True
                    rate_limit_time = time.time()
                    tdLog.info(f"Rate limit triggered at {rate_limit_time}: {e}")
                    break
        
        if not rate_limit_triggered:
            tdLog.exit(f"FAILED: Could not trigger rate limit for recovery test. Test FAILED.")
        
        # Wait for recovery (3 seconds)
        tdLog.info("Waiting 3 seconds for rate limit recovery...")
        time.sleep(3)
        
        # Test recovery - try 5 connections
        tdLog.info("Testing rate limit recovery with 5 connection attempts...")
        recovery_success = 0
        recovery_failure = 0
        
        for i in range(5):
            try:
                start_time = time.time()
                conn = taos.connect()
                conn.close()
                end_time = time.time()
                recovery_success += 1
                tdLog.info(f"Recovery connection {i+1} successful (duration: {end_time-start_time:.3f}s)")
            except Exception as e:
                recovery_failure += 1
                tdLog.info(f"Recovery connection {i+1} failed: {e}")
        
        tdLog.info(f"Recovery test results: {recovery_success} successful, {recovery_failure} failed")
        
        # Verify recovery (at least 3 out of 5 should succeed)
        if recovery_success < 3:
            tdLog.exit(f"FAILED: Rate limit recovery test failed. Only {recovery_success}/5 connections successful. Test FAILED.")
        
        tdLog.info("Rate limit recovery test PASSED")
    
    def create_test_connection(self):
        """Create and initialize test connection"""
        try:
            conn = taos.connect()
            testSql = TDSql()
            testSql.init(conn.cursor())
            return conn, testSql
        except Exception as e:
            tdLog.exit(f"Failed to create test connection: {e}")
    
    def run_all_tests(self):
        """Execute all rate limiting tests with timeout"""
        # Set timeout handler for 5 minutes
        try:
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(300)  # 5 minutes timeout
            
            start_time = time.time()
            
            self.test_connection_rate_limit()
            time.sleep(1)
            
            self.test_query_rate_limit()
            time.sleep(1)
            
            self.test_rate_limit_recovery()
            
            # Cleanup
            self.cleanup_test_data()
            
            end_time = time.time()
            total_duration = end_time - start_time
            
            tdLog.info(f"===========================================")
            tdLog.info(f"All rate limiting tests PASSED")
            tdLog.info(f"Total test duration: {total_duration:.1f} seconds")
            tdLog.success(f"{__file__} successfully executed")
            
            signal.alarm(0)  # Cancel timeout
            
        except TimeoutError:
            tdLog.exit(f"FAILED: Test timeout exceeded 5 minutes. Test FAILED.")
        except Exception as e:
            tdLog.exit(f"FAILED: Test suite failed with error: {e}")
        finally:
            self.cleanup_test_data()
            signal.alarm(0)  # Ensure timeout is cancelled
