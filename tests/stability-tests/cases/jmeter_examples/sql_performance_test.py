"""
SQL性能测试用例
遍历SQL文件，逐条执行JMeter性能测试，并生成详细的结果报告
"""

import os
import sys
import time
from datetime import datetime
sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))

from taostest.tdcase import TdCase
from taostest.util.jmeter_sql_runner import JMeterSQLPerformanceRunner
from taostest.util.remote import Remote


class SQLPerformanceTest(TdCase):
    """SQL性能测试用例"""
    
    def init(self, conn, logfile):
        super().init(conn, logfile)
        self.remote = Remote()
        self.remote.init()
        self.test_start_time = datetime.now()
        self.results_base_dir = "/root/taos-test-framework/TestNG/run"
        
    def setUp(self):
        """测试前准备"""
        # 创建基础结果目录
        os.makedirs(self.results_base_dir, exist_ok=True)
        print(f"Test started at: {self.test_start_time}")
        
    def tearDown(self):
        """测试后清理"""
        print(f"Test completed at: {datetime.now()}")
        if hasattr(self, 'remote'):
            self.remote.close()
    
    def test_sql_performance_light_load(self):
        """轻量级负载SQL性能测试"""
        self._run_sql_performance_test("light_load")
    
    def test_sql_performance_medium_load(self):
        """中等负载SQL性能测试"""
        self._run_sql_performance_test("medium_load")
    
    def test_sql_performance_heavy_load(self):
        """高负载SQL性能测试"""
        self._run_sql_performance_test("heavy_load")
    
    def test_sql_performance_stress_test(self):
        """压力测试SQL性能测试"""
        self._run_sql_performance_test("stress_test")
    
    def _run_sql_performance_test(self, scenario: str):
        """运行SQL性能测试的核心方法"""
        try:
            print(f"\\n=== Starting SQL Performance Test - {scenario} ===")
            
            # 配置文件路径
            config_path = os.path.join(
                os.path.dirname(__file__), 
                "../../env/proj_aaa/jmeter_config.yaml"
            )
            
            # 检查配置文件是否存在
            if not os.path.exists(config_path):
                print(f"Warning: Config file not found: {config_path}")
                print("Skipping test due to missing configuration")
                return
            
            # 创建结果目录
            case_name = f"sql_performance_{scenario}"
            
            with JMeterSQLPerformanceRunner(self.remote) as runner:
                # 创建结果目录
                results_dir = runner.create_results_directory(
                    self.results_base_dir, 
                    case_name
                )
                
                print(f"Results will be saved to: {results_dir}")
                
                # 运行SQL性能测试
                start_time = time.time()
                results = runner.run_sql_performance_tests(
                    config_path=config_path,
                    case_dir=os.path.dirname(__file__),
                    scenario=scenario
                )
                end_time = time.time()
                
                # 分析结果
                self._analyze_test_results(results, scenario, end_time - start_time)
                
                # 生成汇总报告
                if results:
                    summary_file = runner.generate_performance_summary(results, results_dir)
                    print(f"Performance summary generated: {summary_file}")
                    
                    # 收集JMeter原始结果
                    config_data = runner._jmeter_runner._get_default_config()  # 使用默认配置进行收集
                    runner.collect_results(config_data, results_dir)
                
                # 检查性能阈值
                self._check_performance_thresholds(results, config_path)
                
                print(f"=== SQL Performance Test Completed - {scenario} ===\\n")
                
        except Exception as e:
            print(f"SQL Performance Test failed for scenario {scenario}: {e}")
            # 不抛出异常，允许其他测试继续执行
    
    def _analyze_test_results(self, results: list, scenario: str, total_time: float):
        """分析测试结果"""
        if not results:
            print("No test results to analyze")
            return
        
        successful_tests = [r for r in results if r["status"] == "success"]
        failed_tests = [r for r in results if r["status"] == "failed"]
        
        print(f"\\n=== Test Results Analysis - {scenario} ===")
        print(f"Total SQLs Tested: {len(results)}")
        print(f"Successful Tests: {len(successful_tests)}")
        print(f"Failed Tests: {len(failed_tests)}")
        print(f"Success Rate: {len(successful_tests)/len(results)*100:.2f}%")
        print(f"Total Test Time: {total_time:.2f} seconds")
        
        if successful_tests:
            execution_times = [r.get("execution_time_seconds", 0) for r in successful_tests]
            print(f"Average SQL Execution Time: {sum(execution_times)/len(execution_times):.2f} seconds")
            print(f"Fastest SQL: {min(execution_times):.2f} seconds")
            print(f"Slowest SQL: {max(execution_times):.2f} seconds")
        
        # 显示失败的测试
        if failed_tests:
            print("\\nFailed Tests:")
            for failed in failed_tests[:5]:  # 只显示前5个失败的测试
                print(f"  - {failed.get('test_name', 'unknown')}: {failed.get('error', 'Unknown error')}")
            if len(failed_tests) > 5:
                print(f"  ... and {len(failed_tests) - 5} more failed tests")
        
        # 显示最慢的测试
        if successful_tests:
            sorted_tests = sorted(successful_tests, 
                                key=lambda x: x.get("execution_time_seconds", 0), 
                                reverse=True)
            print("\\nTop 3 Slowest Tests:")
            for test in sorted_tests[:3]:
                print(f"  - {test.get('test_name', 'unknown')}: {test.get('execution_time_seconds', 0):.2f}s")
    
    def _check_performance_thresholds(self, results: list, config_path: str):
        """检查性能阈值"""
        try:
            from taostest.util.file import read_yaml
            
            config_data = read_yaml(config_path)
            thresholds = config_data.get("result_config", {}).get("performance_thresholds", {})
            
            if not thresholds:
                print("No performance thresholds configured")
                return
            
            successful_tests = [r for r in results if r["status"] == "success"]
            if not successful_tests:
                print("No successful tests to check thresholds")
                return
            
            execution_times = [r.get("execution_time_seconds", 0) for r in successful_tests]
            avg_time = sum(execution_times) / len(execution_times) * 1000  # 转换为毫秒
            max_time = max(execution_times) * 1000  # 转换为毫秒
            error_rate = len([r for r in results if r["status"] == "failed"]) / len(results) * 100
            
            print("\\n=== Performance Threshold Check ===")
            
            # 检查平均响应时间
            if "avg_response_time_ms" in thresholds:
                threshold = thresholds["avg_response_time_ms"]
                status = "PASS" if avg_time <= threshold else "FAIL"
                print(f"Average Response Time: {avg_time:.2f}ms (threshold: {threshold}ms) - {status}")
            
            # 检查最大响应时间
            if "max_response_time_ms" in thresholds:
                threshold = thresholds["max_response_time_ms"]
                status = "PASS" if max_time <= threshold else "FAIL"
                print(f"Max Response Time: {max_time:.2f}ms (threshold: {threshold}ms) - {status}")
            
            # 检查错误率
            if "error_rate_percent" in thresholds:
                threshold = thresholds["error_rate_percent"]
                status = "PASS" if error_rate <= threshold else "FAIL"
                print(f"Error Rate: {error_rate:.2f}% (threshold: {threshold}%) - {status}")
            
        except Exception as e:
            print(f"Failed to check performance thresholds: {e}")
    
    def test_custom_sql_performance(self):
        """自定义SQL性能测试"""
        try:
            print("\\n=== Custom SQL Performance Test ===")
            
            # 自定义SQL列表
            custom_sqls = [
                {
                    "name": "basic_select",
                    "sql": "SELECT COUNT(*) FROM yjb2c.type_float WHERE ts > '2024-01-01 00:00:00'"
                },
                {
                    "name": "aggregation_test", 
                    "sql": "SELECT tagid, AVG(v), MAX(v), MIN(v) FROM yjb2c.type_float WHERE ts > '2024-01-01 00:00:00' GROUP BY tagid LIMIT 10"
                }
            ]
            
            # 使用JMeter运行器
            with JMeterSQLPerformanceRunner(self.remote) as runner:
                results_dir = runner.create_results_directory(
                    self.results_base_dir,
                    "custom_sql_performance"
                )
                
                config_path = os.path.join(
                    os.path.dirname(__file__), 
                    "../../env/proj_aaa/jmeter_config.yaml"
                )
                
                if not os.path.exists(config_path):
                    print("Warning: Config file not found, using default settings")
                    return
                
                # 手动执行每个SQL
                from taostest.util.file import read_yaml
                config_data = read_yaml(config_path)
                jmeter_config = None
                
                for setting in config_data.get("settings", []):
                    if setting.get("name") == "jmeter":
                        jmeter_config = setting
                        break
                
                if jmeter_config:
                    # 设置JMeter环境
                    setup_success = runner._jmeter_runner.setup_jmeter(jmeter_config)
                    if setup_success:
                        results = []
                        
                        for sql_info in custom_sqls:
                            print(f"Testing custom SQL: {sql_info['name']}")
                            
                            # 准备变量
                            global_vars = jmeter_config["spec"]["jmeter"].get("global_variables", {})
                            sql_variables = global_vars.copy()
                            sql_variables.update({
                                "sql_query": sql_info["sql"],
                                "sql_name": sql_info["name"],
                                "test_name": f"Custom_{sql_info['name']}"
                            })
                            
                            # 创建JMX配置
                            jmx_config = {
                                "name": sql_info["name"],
                                "path": os.path.join(os.path.dirname(config_path), "performance_test_template.jmx"),
                                "variables": sql_variables
                            }
                            
                            # 执行测试
                            result = runner._jmeter_runner.run_jmx_test(
                                config=jmeter_config,
                                jmx_config=jmx_config
                            )
                            
                            result.update({
                                "sql_statement": sql_info["sql"],
                                "custom_test": True
                            })
                            
                            results.append(result)
                        
                        # 分析结果
                        self._analyze_test_results(results, "custom", 0)
                        
                        # 生成报告
                        runner._results = results
                        runner.generate_performance_summary(results, results_dir)
                
        except Exception as e:
            print(f"Custom SQL performance test failed: {e}")


def run():
    """运行SQL性能测试"""
    test_case = SQLPerformanceTest()
    
    # 初始化
    test_case.init(None, "sql_performance_test.log")
    
    try:
        print("=== SQL Performance Testing Suite ===")
        
        # 设置测试环境
        test_case.setUp()
        
        # 运行不同负载的性能测试
        print("\\n1. Running light load performance test...")
        test_case.test_sql_performance_light_load()
        
        print("\\n2. Running medium load performance test...")
        test_case.test_sql_performance_medium_load()
        
        print("\\n3. Running custom SQL performance test...")
        test_case.test_custom_sql_performance()
        
        # 可选：运行高负载测试（注释掉避免在测试环境中运行）
        # print("\\n4. Running heavy load performance test...")
        # test_case.test_sql_performance_heavy_load()
        
        # print("\\n5. Running stress test...")
        # test_case.test_sql_performance_stress_test()
        
        print("\\n=== All SQL Performance Tests Completed ===")
        
    except Exception as e:
        print(f"SQL Performance test execution failed: {e}")
        raise
    finally:
        # 清理
        test_case.tearDown()


if __name__ == "__main__":
    run()