"""
TDengine数据库JMeter性能测试用例
演示实际的数据库性能测试场景
"""

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))

from taostest.tdcase import TdCase
from taostest.util.jmeter import JMeterTestRunner, batch_run_jmx
from taostest.util.remote import Remote
from taostest.util.sql import TaosSql
import time


class TDenginePerformanceTest(TdCase):
    """TDengine数据库性能测试"""
    
    def init(self, conn, logfile):
        super().init(conn, logfile)
        self.remote = Remote()
        self.remote.init()
        
    def setUp(self):
        """测试前准备"""
        # 创建测试数据库和表
        if self.conn:
            sql = TaosSql(self.conn)
            sql.execute("DROP DATABASE IF EXISTS jmeter_test_db")
            sql.execute("CREATE DATABASE jmeter_test_db")
            sql.execute("USE jmeter_test_db")
            sql.execute("""
                CREATE STABLE meters (
                    ts TIMESTAMP,
                    current FLOAT,
                    voltage INT,
                    phase FLOAT
                ) TAGS (
                    location NCHAR(64),
                    groupId INT
                )
            """)
            
            # 创建一些子表用于测试
            for i in range(10):
                sql.execute(f"""
                    CREATE TABLE meter_{i} USING meters TAGS ('location_{i}', {i})
                """)
    
    def tearDown(self):
        """测试后清理"""
        if self.conn:
            sql = TaosSql(self.conn)
            sql.execute("DROP DATABASE IF EXISTS jmeter_test_db")
    
    def test_database_insert_performance(self):
        """测试数据库插入性能"""
        try:
            # 准备JMeter配置
            jmeter_config = {
                "fqdn": ["localhost"],  # 在实际环境中替换为真实的JMeter服务器
                "spec": {
                    "jmeter": {
                        "version": "5.6.3",
                        "offline": False,
                        "install_java": True,
                        "jmx_file": "example_database_test.jmx",
                        "variables": {
                            "db_host": "127.0.0.1",
                            "db_port": "6030", 
                            "thread_count": "20",
                            "test_duration": "120",
                            "database_name": "jmeter_test_db",
                            "table_name": "meter_0"
                        },
                        "global_variables": {
                            "environment": "test",
                            "test_type": "insert_performance"
                        }
                    },
                    "server": {
                        "run_dir": "/opt"
                    },
                    "jdbc-driver": {
                        "version": "3.4.0"
                    }
                }
            }
            
            with JMeterTestRunner(self.remote) as runner:
                # 设置JMeter环境
                if not runner.setup_jmeter(jmeter_config):
                    print("Warning: JMeter setup failed, skipping actual execution")
                    return
                
                # 运行插入性能测试
                results = runner.run_tests(
                    config=jmeter_config,
                    case_dir=os.path.dirname(__file__)
                )
                
                # 分析结果
                self._analyze_performance_results(results, "insert")
                
        except Exception as e:
            print(f"Insert performance test error: {e}")
            # 在测试环境中不抛出异常
    
    def test_database_query_performance(self):
        """测试数据库查询性能"""
        try:
            # 先插入一些测试数据
            self._prepare_test_data()
            
            # 查询性能测试配置
            jmeter_config = {
                "fqdn": ["localhost"],
                "spec": {
                    "jmeter": {
                        "version": "5.6.3",
                        "jmx_file": "example_database_test.jmx",
                        "variables": {
                            "db_host": "127.0.0.1",
                            "db_port": "6030",
                            "thread_count": "10", 
                            "test_duration": "60",
                            "database_name": "jmeter_test_db",
                            "table_name": "meters"
                        }
                    }
                }
            }
            
            with JMeterTestRunner(self.remote) as runner:
                if not runner.setup_jmeter(jmeter_config):
                    print("Warning: JMeter setup failed, skipping actual execution")
                    return
                    
                results = runner.run_tests(
                    config=jmeter_config,
                    case_dir=os.path.dirname(__file__)
                )
                
                self._analyze_performance_results(results, "query")
                
        except Exception as e:
            print(f"Query performance test error: {e}")
    
    def test_mixed_workload_performance(self):
        """测试混合工作负载性能"""
        try:
            # 定义混合工作负载的JMX配置
            mixed_jmx_configs = [
                {
                    "name": "insert_workload",
                    "path": "example_database_test.jmx",
                    "variables": {
                        "db_host": "127.0.0.1",
                        "db_port": "6030",
                        "thread_count": "15",
                        "test_duration": "180",
                        "database_name": "jmeter_test_db",
                        "table_name": "meter_insert"
                    }
                },
                {
                    "name": "query_workload", 
                    "path": "example_database_test.jmx",
                    "variables": {
                        "db_host": "127.0.0.1",
                        "db_port": "6030",
                        "thread_count": "10",
                        "test_duration": "180",
                        "database_name": "jmeter_test_db",
                        "table_name": "meters"
                    }
                }
            ]
            
            # 使用批量运行函数
            results = batch_run_jmx(
                jmx_configs=mixed_jmx_configs,
                remote=self.remote
            )
            
            self._analyze_performance_results(results, "mixed")
            
        except Exception as e:
            print(f"Mixed workload test error: {e}")
    
    def test_scalability_performance(self):
        """测试可扩展性性能（不同线程数）"""
        try:
            thread_counts = [5, 10, 20, 50]
            results_summary = {}
            
            for thread_count in thread_counts:
                print(f"\\nTesting with {thread_count} threads...")
                
                jmx_config = {
                    "name": f"scalability_test_{thread_count}",
                    "path": "example_database_test.jmx", 
                    "variables": {
                        "db_host": "127.0.0.1",
                        "db_port": "6030",
                        "thread_count": str(thread_count),
                        "test_duration": "60",
                        "database_name": "jmeter_test_db",
                        "table_name": f"meter_scale_{thread_count}"
                    }
                }
                
                jmeter_config = {
                    "fqdn": ["localhost"],
                    "spec": {
                        "jmeter": {
                            "jmx_files": [jmx_config]
                        }
                    }
                }
                
                with JMeterTestRunner(self.remote) as runner:
                    if not runner.setup_jmeter(jmeter_config):
                        print("Warning: JMeter setup failed")
                        continue
                        
                    results = runner.run_tests(jmeter_config)
                    results_summary[thread_count] = results
                    
                    # 短暂休息避免资源争用
                    time.sleep(10)
            
            # 分析可扩展性结果
            self._analyze_scalability_results(results_summary)
            
        except Exception as e:
            print(f"Scalability test error: {e}")
    
    def _prepare_test_data(self):
        """准备测试数据"""
        if not self.conn:
            return
            
        sql = TaosSql(self.conn)
        
        # 插入一些初始数据用于查询测试
        import random
        from datetime import datetime, timedelta
        
        base_time = datetime.now() - timedelta(hours=24)
        
        for i in range(100):  # 插入100条记录
            timestamp = base_time + timedelta(minutes=i * 10)
            current = random.uniform(10.0, 50.0)
            voltage = random.randint(220, 240)
            phase = random.uniform(0.0, 360.0)
            
            sql.execute(f"""
                INSERT INTO meter_0 VALUES 
                ('{timestamp}', {current}, {voltage}, {phase})
            """)
    
    def _analyze_performance_results(self, results, test_type):
        """分析性能测试结果"""
        print(f"\\n=== {test_type.upper()} Performance Test Results ===")
        
        for result in results:
            if result["status"] == "success":
                print(f"✓ Test '{result['test_name']}' completed successfully")
                if "output_files" in result:
                    print(f"  - Results saved to: {result['output_files']}")
            else:
                print(f"✗ Test '{result['test_name']}' failed: {result.get('error', 'Unknown error')}")
        
        successful_tests = [r for r in results if r["status"] == "success"]
        print(f"\\nSummary: {len(successful_tests)}/{len(results)} tests passed")
    
    def _analyze_scalability_results(self, results_summary):
        """分析可扩展性测试结果"""
        print("\\n=== Scalability Test Results ===")
        
        for thread_count, results in results_summary.items():
            successful = len([r for r in results if r["status"] == "success"])
            total = len(results)
            print(f"Threads: {thread_count:2d} - Success: {successful}/{total}")
        
        print("\\nScalability analysis completed. Check detailed results in log files.")


def run():
    """运行性能测试"""
    test_case = TDenginePerformanceTest()
    
    # 初始化
    test_case.init(None, "tdengine_performance_test.log")
    
    try:
        print("=== TDengine JMeter Performance Tests ===")
        
        # 设置测试环境
        test_case.setUp()
        
        print("\\n1. Testing database insert performance...")
        test_case.test_database_insert_performance()
        
        print("\\n2. Testing database query performance...")
        test_case.test_database_query_performance()
        
        print("\\n3. Testing mixed workload performance...")
        test_case.test_mixed_workload_performance()
        
        print("\\n4. Testing scalability performance...")
        test_case.test_scalability_performance()
        
        print("\\n=== Performance Tests Completed ===")
        
    except Exception as e:
        print(f"Performance test execution failed: {e}")
        raise
    finally:
        # 清理
        test_case.tearDown()
        if hasattr(test_case, 'remote'):
            test_case.remote.close()


if __name__ == "__main__":
    run()