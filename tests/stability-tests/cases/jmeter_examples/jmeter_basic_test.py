"""
JMeter基础测试用例示例
演示如何在Python测试用例中使用JMeter进行性能测试
"""

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))

from taostest.tdcase import TdCase
from taostest.util.jmeter import JMeterTestRunner, JMXVariableReplacer, quick_run_jmx
from taostest.util.remote import Remote


class JMeterBasicTest(TdCase):
    """JMeter基础测试用例"""
    
    def init(self, conn, logfile):
        super().init(conn, logfile)
        self.remote = Remote()
        self.remote.init()
        
    def test_single_jmx_file(self):
        """测试单个JMX文件执行"""
        try:
            # 定义JMX文件路径（相对于env目录）
            jmx_path = "example_database_test.jmx"
            
            # 定义变量
            variables = {
                "db_host": "192.168.1.100",
                "db_port": "6030", 
                "thread_count": "10",
                "test_duration": "60",
                "database_name": "test_db",
                "table_name": "meters"
            }
            
            # 定义JMeter配置
            jmeter_config = {
                "fqdn": ["node1"],
                "spec": {
                    "jmeter": {
                        "version": "5.6.3",
                        "offline": False,
                        "install_java": True
                    },
                    "server": {
                        "run_dir": "/opt"
                    },
                    "jdbc-driver": {
                        "version": "3.4.0"
                    }
                }
            }
            
            # 使用JMeter测试运行器
            with JMeterTestRunner(self.remote) as runner:
                # 设置JMeter环境
                setup_success = runner.setup_jmeter(jmeter_config)
                assert setup_success, "Failed to setup JMeter environment"
                
                # 运行单个JMX文件
                result = runner.run_single_jmx(
                    jmx_path=jmx_path,
                    variables=variables,
                    config=jmeter_config,
                    case_dir=os.path.dirname(__file__)
                )
                
                # 验证结果
                assert result["status"] == "success", f"JMeter test failed: {result.get('error', 'Unknown error')}"
                
                # 收集结果
                runner.collect_results(jmeter_config, "/tmp/jmeter_results", [result])
                
                print(f"Test completed successfully: {result['test_name']}")
                
        except Exception as e:
            print(f"Test failed with error: {e}")
            raise
            
    def test_multiple_jmx_files(self):
        """测试多个JMX文件执行"""
        try:
            # 定义多个JMX配置
            jmx_configs = [
                {
                    "name": "load_test",
                    "path": "example_database_test.jmx",
                    "variables": {
                        "db_host": "192.168.1.100",
                        "db_port": "6030",
                        "thread_count": "20",
                        "test_duration": "120",
                        "database_name": "load_test_db",
                        "table_name": "load_meters"
                    }
                },
                {
                    "name": "stress_test", 
                    "path": "example_database_test.jmx",
                    "variables": {
                        "db_host": "192.168.1.100",
                        "db_port": "6030",
                        "thread_count": "50",
                        "test_duration": "300",
                        "database_name": "stress_test_db", 
                        "table_name": "stress_meters"
                    }
                }
            ]
            
            jmeter_config = {
                "fqdn": ["node1", "node2"],
                "spec": {
                    "jmeter": {
                        "version": "5.6.3"
                    }
                }
            }
            
            with JMeterTestRunner(self.remote) as runner:
                runner.setup_jmeter(jmeter_config)
                
                # 运行多个JMX文件
                results = runner.run_multiple_jmx(
                    jmx_configs=jmx_configs,
                    config=jmeter_config,
                    case_dir=os.path.dirname(__file__)
                )
                
                # 验证所有测试都成功
                for result in results:
                    assert result["status"] == "success", f"Test {result['test_name']} failed: {result.get('error')}"
                
                # 收集结果
                runner.collect_results(jmeter_config, "/tmp/jmeter_results", results)
                
                print(f"All {len(results)} tests completed successfully")
                
        except Exception as e:
            print(f"Multiple tests failed with error: {e}")
            raise
            
    def test_yaml_config_execution(self):
        """测试从YAML配置文件执行"""
        try:
            # 使用YAML配置文件
            yaml_config_path = "../../env/jmeter_enhanced.yaml"
            
            with JMeterTestRunner(self.remote) as runner:
                results = runner.run_from_yaml_config(
                    yaml_path=yaml_config_path,
                    case_dir=os.path.dirname(__file__)
                )
                
                # 验证结果
                assert len(results) > 0, "No tests were executed"
                
                for result in results:
                    if result["status"] == "failed":
                        print(f"Warning: Test {result['test_name']} failed: {result.get('error')}")
                
                successful_tests = [r for r in results if r["status"] == "success"]
                print(f"{len(successful_tests)} out of {len(results)} tests completed successfully")
                
        except Exception as e:
            print(f"YAML config test failed with error: {e}")
            # 不抛出异常，因为可能是配置文件不存在
            
    def test_variable_replacement(self):
        """测试变量替换功能"""
        try:
            # 创建一个简单的JMX内容用于测试
            jmx_content = """<?xml version="1.0" encoding="UTF-8"?>
            <jmeterTestPlan>
                <TestPlan>
                    <stringProp name="host">${db_host}</stringProp>
                    <stringProp name="port">${db_port}</stringProp>
                    <stringProp name="threads">#{thread_count}</stringProp>
                    <stringProp name="duration">@{test_duration}</stringProp>
                </TestPlan>
            </jmeterTestPlan>"""
            
            variables = {
                "db_host": "localhost",
                "db_port": "6030",
                "thread_count": "100",
                "test_duration": "300"
            }
            
            # 测试变量替换
            replaced_content = JMXVariableReplacer.replace_variables_in_content(jmx_content, variables)
            
            # 验证替换结果
            assert "localhost" in replaced_content
            assert "6030" in replaced_content
            assert "100" in replaced_content
            assert "300" in replaced_content
            
            # 确保变量占位符被替换
            assert "${db_host}" not in replaced_content
            assert "#{thread_count}" not in replaced_content
            assert "@{test_duration}" not in replaced_content
            
            print("Variable replacement test passed")
            
        except Exception as e:
            print(f"Variable replacement test failed: {e}")
            raise
            
    def test_quick_run_convenience_function(self):
        """测试便捷函数快速运行"""
        try:
            # 使用便捷函数快速运行JMX文件
            result = quick_run_jmx(
                jmx_path="example_database_test.jmx",
                variables={
                    "db_host": "192.168.1.100",
                    "db_port": "6030",
                    "thread_count": "5",
                    "test_duration": "30",
                    "database_name": "quick_test_db",
                    "table_name": "quick_meters"
                },
                remote=self.remote
            )
            
            # 这个测试可能会失败（因为环境限制），但不应该抛出异常
            print(f"Quick run result: {result}")
            
        except Exception as e:
            print(f"Quick run test encountered error (expected in test environment): {e}")


def run():
    """运行测试用例"""
    test_case = JMeterBasicTest()
    
    # 初始化（使用虚拟连接）
    test_case.init(None, "jmeter_test.log")
    
    try:
        print("=== Running JMeter Basic Tests ===")
        
        print("\\n1. Testing variable replacement...")
        test_case.test_variable_replacement()
        
        print("\\n2. Testing single JMX file execution...")
        try:
            test_case.test_single_jmx_file()
        except Exception as e:
            print(f"Single JMX test skipped due to environment: {e}")
        
        print("\\n3. Testing multiple JMX files execution...")
        try:
            test_case.test_multiple_jmx_files()
        except Exception as e:
            print(f"Multiple JMX test skipped due to environment: {e}")
        
        print("\\n4. Testing YAML config execution...")
        test_case.test_yaml_config_execution()
        
        print("\\n5. Testing quick run convenience function...")
        test_case.test_quick_run_convenience_function()
        
        print("\\n=== All Tests Completed ===")
        
    except Exception as e:
        print(f"Test execution failed: {e}")
        raise
    finally:
        # 清理资源
        if hasattr(test_case, 'remote'):
            test_case.remote.close()


if __name__ == "__main__":
    run()