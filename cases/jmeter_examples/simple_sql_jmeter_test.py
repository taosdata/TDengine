"""
Simple SQL JMeter Test
Iterate through SQL list in YAML config, replace JMX parameters and run JMeter tests
"""

import os
import sys
import re
import tempfile
import time
from datetime import datetime
from taostest import TDCase, T

from taostest.util.jmeter import JMeterTestRunner, JMXVariableReplacer
from taostest.util.remote import Remote
from taostest.util.file import read_yaml
from taostest.util.common import TDCom


class SimpleSqlJmeterTest(TDCase):
    """Simple SQL JMeter test case"""

    def init(self):
        """Initialize test environment"""
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.jmeter_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "jmeter")
        self.jmeter_fqdn = self.jmeter_setting["fqdn"][0]
        self.results_dir = os.path.join(self.run_log_dir, "jmeter_results")
        os.makedirs(self.results_dir, exist_ok=True)
        self.test_root = os.environ['TEST_ROOT']
        self.sql_file_path = ""
        # Record start time
        self.start_time = datetime.now()
        self._remote._logger.info(f"Test started at: {self.start_time}")

    def read_sql_file(self, sql_file_path):
        """Read SQL file and parse SQL statements"""
        if not os.path.exists(sql_file_path):
            self._remote._logger.info(f"SQL file does not exist: {sql_file_path}")
            return []

        with open(sql_file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Use regular expression to match SQL statements, ignoring comments
        sql_pattern = r'--.*?$|/\*[\s\S]*?\*/|(;|\n)'
        sqls = []
        current_sql = ""

        # Read and process line by line
        for line in content.splitlines():
            # Skip empty lines and comment lines
            if not line.strip() or line.strip().startswith('--'):
                continue

            # Accumulate SQL statements
            current_sql += line + " "

            # If line ends with a semicolon, it's a complete SQL
            if line.strip().endswith(';'):
                if current_sql.strip():
                    sqls.append(current_sql.strip())
                current_sql = ""

        # Process the last SQL statement that might not end with a semicolon
        if current_sql.strip():
            sqls.append(current_sql.strip())

        return sqls

    def extract_sql_name(self, sql):
        """Extract a short name from SQL"""
        # Remove comments
        sql = re.sub(r'--.*?$|/\*[\s\S]*?\*/', '', sql, flags=re.MULTILINE)

        # Get a short description of the SQL
        sql_trim = sql.strip()
        if len(sql_trim) > 60:
            sql_trim = sql_trim[:57] + "..."

        return f"SQL - {sql_trim}"

    def run_test(self, scenario=None):
        """Run SQL JMeter test"""
        try:

            # Get scenario configuration
            scenario_config = None
            server_config = self.jmeter_setting.get("spec", {}).get("server", {})
            if scenario:
                for sc in server_config.get("test_scenarios", []):
                    if sc.get("name") == scenario:
                        scenario_config = sc
                        break

            # Get SQL file path
            sql_file = server_config.get("sql_file")
            if not sql_file:
                self._remote._logger.info("SQL file not specified")
                return
            self.sql_file_path = os.path.join(self.test_root, f"env/jmeter/{sql_file}")

            # Read SQL list
            sql_list = self.read_sql_file(self.sql_file_path)
            self._remote._logger.info("Read SQL file:", sql_list)
            if not sql_list:
                self._remote._logger.info("SQL list is empty")
                return

            self._remote._logger.info(f"Found {len(sql_list)} SQL statements")

            # Get JMX template path
            jmx_template = server_config.get("jmx_template")
            self._remote._logger.info(f"JMX template: {jmx_template}")
            if not jmx_template:
                self._remote._logger.info("JMX template not specified")
                return

            jmx_template_path =  os.path.join(self.test_root, f"env/jmeter/{jmx_template}")
            self._remote._logger.info(f"JMX template path: {jmx_template_path}")
            if not os.path.exists(jmx_template_path):
                self._remote._logger.info(f"JMX template file does not exist: {jmx_template_path}")
                return

            # Prepare JMeter runner
            with JMeterTestRunner(self._remote) as runner:
                # Set JMeter environment
                setup_success = runner.setup_jmeter(self.jmeter_setting)
                if not setup_success:
                    self._remote._logger.info("JMeter environment setup failed")
                    return

                # Create result directory
                test_result_dir = os.path.join(
                    self.results_dir,
                    f"sql_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                )
                os.makedirs(test_result_dir, exist_ok=True)

                # Get global variables
                global_variables = server_config.get("global_variables", {})

                # If there's scenario configuration, override global variables
                if scenario_config:
                    for key, value in scenario_config.items():
                        if key != "name":
                            global_variables[key] = value

                # Execute each SQL
                results = []
                for i, sql in enumerate(sql_list):
                    sql_name = self.extract_sql_name(sql)
                    self._remote._logger.info(f"\n[{i+1}/{len(sql_list)}] Testing SQL: {sql_name}")

                    # Prepare variables
                    variables = global_variables.copy()
                    variables["sql_query"] = sql
                    variables["sql_name"] = sql_name

                    # Create temporary JMX file
                    with tempfile.NamedTemporaryFile(suffix='.jmx', delete=False) as tmp_jmx:
                        tmp_jmx_path = tmp_jmx.name

                    # Replace variables and save to temporary file
                    JMXVariableReplacer.replace_variables_in_file(
                        jmx_template_path,
                        variables,
                        tmp_jmx_path
                    )

                    # Run JMeter test
                    jmx_config = {
                        "name": f"sql_test_{i+1}",
                        "path": tmp_jmx_path
                    }

                    start_time = time.time()
                    result = runner.run_single_jmx(
                        jmx_path=tmp_jmx_path,
                        config=self.jmeter_setting
                    )
                    end_time = time.time()

                    # Add execution time
                    result["execution_time_seconds"] = end_time - start_time
                    result["sql"] = sql
                    result["sql_name"] = sql_name

                    # Save execution parameters for use in result analysis
                    result["execution_params"] = {
                        "thread_count": variables.get("thread_count", "N/A"),
                        "loop_count": variables.get("loop_count", "N/A"),
                        "ramp_time": variables.get("ramp_time", "N/A"),
                        "query_timeout": variables.get("query_timeout", "N/A"),
                        "pool_max": variables.get("pool_max", "N/A"),
                        "connection_timeout": variables.get("connection_timeout", "N/A")
                    }

                    results.append(result)

                    # Delete temporary file
                    try:
                        os.unlink(tmp_jmx_path)
                    except:
                        pass

                    # Display result
                    status = "Success" if result["status"] == "success" else "Failed"
                    self._remote._logger.info(f"   Result: {status}, Execution Time: {result['execution_time_seconds']:.2f} seconds")

                # Collect results
                runner.collect_results(self.jmeter_setting, self.results_dir, results)

                # Analyze results
                self._analyze_results(results, self.results_dir, server_config.get("result_config", {}))

        except Exception as e:
            self._remote._logger.info(f"Test execution failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Clean up resources
            if hasattr(self, 'remote'):
                self.remote.close()

    def _analyze_results(self, results, result_dir, result_config=None):
        """Analyze test results"""
        if not results:
            self._remote._logger.info("No test results to analyze")
            return

        import json
        import glob
        import os

        # Calculate statistics
        successful_tests = [r for r in results if r["status"] == "success"]
        failed_tests = [r for r in results if r["status"] == "failed"]

        self._remote._logger.info("\n=== Test Result Analysis ===")
        self._remote._logger.info(f"Total SQL count: {len(results)}")
        self._remote._logger.info(f"Successful tests: {len(successful_tests)}")
        self._remote._logger.info(f"Failed tests: {len(failed_tests)}")
        self._remote._logger.info(f"Success rate: {len(successful_tests)/len(results)*100:.2f}%")

        # Display failed tests
        if failed_tests:
            self._remote._logger.info("\nFailed tests:")
            for failed in failed_tests:
                self._remote._logger.info(f"  - {failed.get('sql_name', 'unknown')}: {failed.get('error', 'Unknown error')}")

        # Collect JMeter statistics
        jmeter_stats = {}
        for i, result in enumerate(results):
            if result["status"] == "success":
                # Find corresponding statistics.json file
                sql_name = result.get("sql_name", "unknown")
                test_name = f"sql_test_{i+1}"  # Match with name in jmeter configuration

                # Get execution parameters
                execution_params = result.get("execution_params", {})

                # Find statistics.json file in test result directory
                statistics_paths = []
                for root, dirs, files in os.walk(result_dir):
                    for file in files:
                        if file == "statistics.json":
                            statistics_paths.append(os.path.join(root, file))

                for stats_path in statistics_paths:
                    try:
                        with open(stats_path, 'r', encoding='utf-8') as f:
                            stats_data = json.load(f)

                            # Check if statistics data contains SQL name
                            for label, data in stats_data.items():
                                if sql_name.lower() in label.lower():
                                    throughput = data.get("throughput", 0)
                                    avg_response_time = data.get("meanResTime", 0)
                                    error_rate = data.get("errorPct", 0)

                                    # Extend statistics data with execution parameters
                                    jmeter_stats[sql_name] = {
                                        "throughput": throughput,
                                        "avg_response_time": avg_response_time,
                                        "error_rate": error_rate,
                                        "thread_count": execution_params.get("thread_count", "N/A"),
                                        "loop_count": execution_params.get("loop_count", "N/A"),
                                        "query_timeout": execution_params.get("query_timeout", "N/A"),
                                        "pool_max": execution_params.get("pool_max", "N/A"),
                                        "connection_timeout": execution_params.get("connection_timeout", "N/A"),
                                        # Add more JMeter metrics
                                        "min_response_time": data.get("minResTime", 0),
                                        "max_response_time": data.get("maxResTime", 0),
                                        "median_response_time": data.get("medianResTime", 0),
                                        "90th_percentile": data.get("pct1ResTime", 0),
                                        "95th_percentile": data.get("pct2ResTime", 0),
                                        "99th_percentile": data.get("pct3ResTime", 0)
                                    }

                                    self._remote._logger.info(f"Found JMeter statistics data: {sql_name}")
                                    self._remote._logger.info(f"  - Throughput: {throughput:.2f}/sec")
                                    self._remote._logger.info(f"  - Average response time: {avg_response_time:.2f}ms")
                                    self._remote._logger.info(f"  - Error rate: {error_rate:.2f}%")
                                    self._remote._logger.info(f"  - Thread count: {execution_params.get('thread_count', 'N/A')}")
                                    self._remote._logger.info(f"  - Loop count: {execution_params.get('loop_count', 'N/A')}")
                                    break
                    except Exception as e:
                        self._remote._logger.info(f"Failed to read JMeter statistics file {stats_path}: {e}")

        # Check performance thresholds (if configured)
        if result_config and "performance_thresholds" in result_config:
            self._check_performance_thresholds(results, result_config)

        # Save test summary
        summary_file = os.path.join(result_dir, "test_summary.txt")
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("=== SQL JMeter Test Summary ===\n")
            f.write(f"Test time: {datetime.now()}\n")
            f.write(f"Total SQL count: {len(results)}\n")
            f.write(f"Successful tests: {len(successful_tests)}\n")
            f.write(f"Failed tests: {len(failed_tests)}\n")
            f.write(f"Success rate: {len(successful_tests)/len(results)*100:.2f}%\n\n")

            f.write("=== Detailed Test Results ===\n")
            for i, result in enumerate(results):
                status = "Success" if result["status"] == "success" else "Failed"
                sql_name = result.get("sql_name", "unknown")

                f.write(f"[{i+1}] {sql_name} - {status}\n")
                f.write(f"     Execution time: {result.get('execution_time_seconds', 0):.2f} seconds\n")

                # Add JMeter statistics data
                if sql_name in jmeter_stats:
                    stats = jmeter_stats[sql_name]
                    f.write(f"     Execution parameters:\n")
                    f.write(f"       Thread count: {stats['thread_count']}\n")
                    f.write(f"       Loop count: {stats['loop_count']}\n")
                    f.write(f"       Query timeout: {stats['query_timeout']}ms\n")
                    f.write(f"       Pool max: {stats['pool_max']}\n")
                    f.write(f"       Connection timeout: {stats['connection_timeout']}ms\n")

                    f.write(f"     Performance statistics:\n")
                    f.write(f"       Throughput: {stats['throughput']:.2f}/sec\n")
                    f.write(f"       Average response time: {stats['avg_response_time']:.2f}ms\n")
                    f.write(f"       Minimum response time: {stats['min_response_time']:.2f}ms\n")
                    f.write(f"       Maximum response time: {stats['max_response_time']:.2f}ms\n")
                    f.write(f"       Median response time: {stats['median_response_time']:.2f}ms\n")
                    f.write(f"       90% response time: {stats['90th_percentile']:.2f}ms\n")
                    f.write(f"       95% response time: {stats['95th_percentile']:.2f}ms\n")
                    f.write(f"       99% response time: {stats['99th_percentile']:.2f}ms\n")
                    f.write(f"       Error rate: {stats['error_rate']:.2f}%\n")

                if result["status"] == "failed":
                    f.write(f"     Error: {result.get('error', 'Unknown error')}\n")

        self._remote._logger.info(f"\nTest summary saved to: {summary_file}")

        # Save CSV format summary
        csv_summary_file = os.path.join(result_dir, "test_summary.csv")
        with open(csv_summary_file, 'w', encoding='utf-8') as f:
            # Write CSV header
            f.write("SQL name,Status,Execution time (seconds),Thread count,Loop count,Throughput (transactions/sec),Average response time (ms),Maximum response time (ms),Error rate (%),90% response time (ms)\n")

            # Write data row for each test
            for i, result in enumerate(results):
                status = "Success" if result["status"] == "success" else "Failed"
                sql_name = result.get("sql_name", "unknown")
                execution_time = result.get("execution_time_seconds", 0)

                # Default values
                thread_count = "N/A"
                loop_count = "N/A"
                throughput = 0
                avg_response_time = 0
                max_response_time = 0
                error_rate = 0
                percentile_90 = 0

                # If there's JMeter statistics data, use it
                if sql_name in jmeter_stats:
                    stats = jmeter_stats[sql_name]
                    thread_count = stats.get("thread_count", "N/A")
                    loop_count = stats.get("loop_count", "N/A")
                    throughput = stats.get("throughput", 0)
                    avg_response_time = stats.get("avg_response_time", 0)
                    max_response_time = stats.get("max_response_time", 0)
                    error_rate = stats.get("error_rate", 0)
                    percentile_90 = stats.get("90th_percentile", 0)

                # Write one CSV data row
                f.write(f"{sql_name},{status},{execution_time:.2f},{thread_count},{loop_count},{throughput:.2f},{avg_response_time:.2f},{max_response_time:.2f},{error_rate:.2f},{percentile_90:.2f}\n")

        self._remote._logger.info(f"CSV format summary saved to: {csv_summary_file}")

    def _check_performance_thresholds(self, results, result_config):
        """Check performance thresholds"""
        thresholds = result_config.get("performance_thresholds", {})
        if not thresholds:
            return

        successful_tests = [r for r in results if r["status"] == "success"]
        if not successful_tests:
            return

        execution_times = [r.get("execution_time_seconds", 0) for r in successful_tests]
        avg_time = sum(execution_times) / len(execution_times) * 1000  # Convert to milliseconds
        max_time = max(execution_times) * 1000  # Convert to milliseconds
        error_rate = len([r for r in results if r["status"] == "failed"]) / len(results) * 100

        self._remote._logger.info("\n=== Performance Threshold Check ===")

        # Check average response time
        if "avg_response_time_ms" in thresholds:
            threshold = thresholds["avg_response_time_ms"]
            status = "Passed" if avg_time <= threshold else "Failed"
            self._remote._logger.info(f"Average response time: {avg_time:.2f}ms (Threshold: {threshold}ms) - {status}")

        # Check maximum response time
        if "max_response_time_ms" in thresholds:
            threshold = thresholds["max_response_time_ms"]
            status = "Passed" if max_time <= threshold else "Failed"
            self._remote._logger.info(f"Maximum response time: {max_time:.2f}ms (Threshold: {threshold}ms) - {status}")

        # Check error rate
        if "error_rate_percent" in thresholds:
            threshold = thresholds["error_rate_percent"]
            status = "Passed" if error_rate <= threshold else "Failed"
            self._remote._logger.info(f"Error rate: {error_rate:.2f}% (Threshold: {threshold}%) - {status}")


    def run(self):
        """Run test case"""

        # Run default scenario
        self.run_test()

        # Can also specify scenario
        # test_case.run_test("light_load")
        # test_case.run_test("medium_load")
        # test_case.run_test("heavy_load")
        # test_case.run_test("stress_test")

    def cleanup(self):
        pass

    def desc(self) :
        case_description = """
            alter_stable check <jiacy>:  [TD-15384] : alter stable check;
            """
        return case_description

    def author(self) :
        return "Jiacy"

    def tags(self):
        return T.Write.TaoscSql.Stable.Alter

