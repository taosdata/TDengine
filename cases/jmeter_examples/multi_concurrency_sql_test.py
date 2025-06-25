"""
Multi-concurrency SQL JMeter Test
Run tests for each SQL at different concurrency levels, collect and compare performance data
"""

import os
import sys
import re
import tempfile
import time
import traceback
from datetime import datetime
import json
from taostest import TDCase, T

from taostest.util.jmeter import JMeterTestRunner, JMXVariableReplacer
from taostest.util.remote import Remote
from taostest.util.file import read_yaml
from taostest.util.common import TDCom


class MultiConcurrencySqlTest(TDCase):
    """Multi-concurrency SQL JMeter test case"""

    def init(self):
        """Initialize test environment"""
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.jmeter_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "jmeter")
        self._remote._logger.info(f"JMeter Settings: {self.jmeter_setting}")
        self.jmeter_fqdn = self.jmeter_setting["fqdn"][0]

        # Create results directory
        self.results_dir = os.path.join(self.run_log_dir, "jmeter_results")
        os.makedirs(self.results_dir, exist_ok=True)

        # Initialize JMeter results directory
        self.jmeter_results_dir = ""

        # Initialize other paths
        self.test_root = os.environ['TEST_ROOT']
        self.sql_file_path = ""

        # Create JMeter runner
        self.jmeter_runner = JMeterTestRunner(self._remote)

        # Initialize results list
        self.results = []

        # Record start time
        self.start_time = datetime.now()
        self._remote._logger.info(f"Test started at: {self.start_time}")

    def read_sql_file(self, sql_file_path):
        """Read SQL file and parse SQL statements"""
        if not os.path.exists(sql_file_path):
            self._remote._logger.error(f"SQL file does not exist: {sql_file_path}")
            return []

        with open(sql_file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Use regex to match SQL statements, ignore comments
        sqls = []
        current_sql = ""

        # Read and process line by line
        for line in content.splitlines():
            # Skip empty lines and comment lines
            if not line.strip() or line.strip().startswith('--'):
                continue

            # Accumulate SQL statement
            current_sql += line + " "

            # If line ends with semicolon, it indicates a complete SQL
            if line.strip().endswith(';'):
                if current_sql.strip():
                    sqls.append(current_sql.strip())
                current_sql = ""

        # Process the last SQL that might not have a semicolon
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

    def run_sql_with_concurrency(self, sql, concurrency_config):
        """Run a single SQL test at the specified concurrency level

        Args:
            sql: SQL statement
            concurrency_config: Concurrency configuration dictionary

        Returns:
            Test result dictionary
        """
        try:
            start_time = time.time()

            # Extract name from SQL
            sql_name = self.extract_sql_name(sql)
            concurrency_name = concurrency_config.get('name', 'unknown')

            # Combine test name
            test_name = f"{sql_name} [{concurrency_name}]"
            self._remote._logger.info(f"\nExecuting test: {test_name}")

            # Get global variables
            server_config = self.jmeter_setting.get("spec", {}).get("server", {})
            global_variables = server_config.get("global_variables", {}).copy()

            # Merge concurrency level parameters
            for key, value in concurrency_config.items():
                if key not in ['name', 'description']:
                    global_variables[key] = value

            # Add SQL parameters
            global_variables["sql_query"] = sql
            global_variables["sql_name"] = sql_name

            # Add concurrency level information to variables
            global_variables["concurrency_level"] = concurrency_name

            # Get JMX template path
            jmx_template = server_config.get("jmx_template")
            jmx_template_path = os.path.join(self.test_root, f"env/jmeter/{jmx_template}")

            if not os.path.exists(jmx_template_path):
                raise FileNotFoundError(f"JMX template file does not exist: {jmx_template_path}")

            # Create temporary JMX file
            with tempfile.NamedTemporaryFile(suffix='.jmx', delete=False) as tmp_jmx:
                tmp_jmx_path = tmp_jmx.name

            # Replace variables and save to temporary file
            JMXVariableReplacer.replace_variables_in_file(
                jmx_template_path,
                global_variables,
                tmp_jmx_path
            )

            # Run JMeter test
            result = self.jmeter_runner.run_single_jmx(
                jmx_path=tmp_jmx_path,
                config=self.jmeter_setting
            )

            # Record execution time
            execution_time = time.time() - start_time

            # Add detailed information to the result
            result["execution_time_seconds"] = execution_time
            result["sql"] = sql
            result["sql_name"] = sql_name
            result["concurrency_level"] = concurrency_name
            result["concurrency_description"] = concurrency_config.get('description', '')

            # Save execution parameters
            result["execution_params"] = {
                "thread_count": global_variables.get("thread_count", "N/A"),
                "loop_count": global_variables.get("loop_count", "N/A"),
                "ramp_time": global_variables.get("ramp_time", "N/A"),
                "query_timeout": global_variables.get("query_timeout", "N/A"),
                "pool_max": global_variables.get("pool_max", "N/A"),
                "connection_timeout": global_variables.get("connection_timeout", "N/A")
            }

            # Delete temporary file
            try:
                os.unlink(tmp_jmx_path)
            except:
                pass

            # Display results
            status = "success" if result.get("status") == "success" else "failed"
            self._remote._logger.info(f"  Result: {status}, Execution time: {execution_time:.2f} seconds")

            return result

        except Exception as e:
            self._remote._logger.error(f"Test execution failed: {e}")
            traceback.print_exc()

            # Return failure result
            return {
                "status": "failed",
                "sql": sql,
                "sql_name": sql_name if 'sql_name' in locals() else "unknown",
                "concurrency_level": concurrency_config.get('name', 'unknown'),
                "error": str(e),
                "execution_time_seconds": time.time() - start_time if 'start_time' in locals() else 0
            }

    def run_test(self):
        """Run multi-concurrency SQL tests"""
        try:
            # Setup JMeter environment
            self.jmeter_runner.setup_jmeter(self.jmeter_setting)

            # Get SQL file path
            server_config = self.jmeter_setting.get("spec", {}).get("server", {})
            sql_file = server_config.get("sql_file")
            if not sql_file:
                self._remote._logger.error("SQL file not specified")
                return

            self.sql_file_path = os.path.join(self.test_root, f"env/jmeter/{sql_file}")

            # Read SQL list
            sql_list = self.read_sql_file(self.sql_file_path)
            if not sql_list:
                self._remote._logger.error("SQL list is empty")
                return

            self._remote._logger.info(f"Found {len(sql_list)} SQL statements")

            # Get concurrency level configuration
            concurrency_levels = server_config.get("concurrency_levels", [])
            if not concurrency_levels:
                self._remote._logger.error("No concurrency levels configured")
                return

            self._remote._logger.info(f"Found {len(concurrency_levels)} concurrency level configurations")

            # Create test result directory
            test_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            test_result_dir = os.path.join(self.results_dir, f"multi_concurrency_test_{test_timestamp}")
            os.makedirs(test_result_dir, exist_ok=True)

            # Create result subdirectories for each SQL
            sql_results = {}

            # Run tests for each SQL at all concurrency levels
            for i, sql in enumerate(sql_list):
                sql_name = self.extract_sql_name(sql)
                self._remote._logger.info(f"\n[{i+1}/{len(sql_list)}] Testing SQL: {sql_name}")

                # Create SQL result directory
                sql_result_dir = os.path.join(test_result_dir, f"sql_{i+1}")
                os.makedirs(sql_result_dir, exist_ok=True)

                # Initialize SQL results list
                sql_results[sql_name] = []

                # Run tests for each concurrency level
                for j, concurrency in enumerate(concurrency_levels):
                    concurrency_name = concurrency.get('name', f'level_{j+1}')

                    self._remote._logger.info(f"  [{j+1}/{len(concurrency_levels)}] Concurrency level: {concurrency_name} - {concurrency.get('description', '')}")

                    # Create concurrency level result directory
                    concurrency_result_dir = os.path.join(sql_result_dir, concurrency_name)
                    os.makedirs(concurrency_result_dir, exist_ok=True)

                    # Set JMeter results directory
                    self.jmeter_results_dir = concurrency_result_dir

                    # Run test
                    result = self.run_sql_with_concurrency(sql, concurrency)

                    # Add result to results lists
                    self.results.append(result)
                    sql_results[sql_name].append(result)

                    # Immediately collect results for this test
                    self.jmeter_runner.collect_results(
                        self.jmeter_setting,
                        concurrency_result_dir,
                        [result]
                    )

            # Analyze all results
            self._analyze_results(sql_results, test_result_dir, server_config.get("result_config", {}))

            # Copy all results to main result directory
            self._remote._logger.info("Consolidating all test results...")
            self._consolidate_results(test_result_dir)

        except Exception as e:
            self._remote._logger.error(f"Test execution failed: {e}")
            traceback.print_exc()
        finally:
            # Close JMeter runner
            self.jmeter_runner.__exit__(None, None, None)

    def _consolidate_results(self, result_dir):
        """Consolidate all test results to main directory"""
        try:
            import shutil
            import glob

            # Find HTML reports in all subdirectories
            html_reports = []
            for root, dirs, files in os.walk(result_dir):
                if "index.html" in files and "statistics.json" in files:
                    html_reports.append(root)

            # Create consolidated directory
            consolidated_dir = os.path.join(result_dir, "consolidated_results")
            os.makedirs(consolidated_dir, exist_ok=True)

            # Copy all HTML reports
            for i, report_dir in enumerate(html_reports):
                # Get relative path part to build target directory name
                rel_path = os.path.relpath(report_dir, result_dir)
                target_dir = os.path.join(consolidated_dir, rel_path)
                os.makedirs(os.path.dirname(target_dir), exist_ok=True)

                # Copy key files
                for file in ["index.html", "statistics.json", "content/js/dashboard.js"]:
                    src_file = os.path.join(report_dir, file)
                    if os.path.exists(src_file):
                        # Ensure target directory exists
                        os.makedirs(os.path.dirname(os.path.join(target_dir, file)), exist_ok=True)
                        shutil.copy2(src_file, os.path.join(target_dir, file))

            self._remote._logger.info(f"Consolidated {len(html_reports)} HTML reports to {consolidated_dir}")

        except Exception as e:
            self._remote._logger.error(f"Failed to consolidate results: {e}")
            traceback.print_exc()

    def _analyze_results(self, sql_results, result_dir, result_config=None):
        """Analyze test results"""
        if not sql_results:
            self._remote._logger.error("No test results to analyze")
            return

        import json
        import glob
        import os
        from collections import defaultdict

        # Collect all test results
        all_results = []
        for sql_name, results in sql_results.items():
            all_results.extend(results)

        # Calculate statistics
        successful_tests = [r for r in all_results if r.get("status") == "success"]
        failed_tests = [r for r in all_results if r.get("status") == "failed"]

        self._remote._logger.info("\n=== Test Result Analysis ===")
        self._remote._logger.info(f"Total SQL count: {len(sql_results)}")
        self._remote._logger.info(f"Total test count: {len(all_results)}")
        self._remote._logger.info(f"Successful tests: {len(successful_tests)}")
        self._remote._logger.info(f"Failed tests: {len(failed_tests)}")
        self._remote._logger.info(f"Success rate: {len(successful_tests)/len(all_results)*100:.2f}%")

        # Display failed tests
        if failed_tests:
            self._remote._logger.info("\nFailed tests:")
            for failed in failed_tests:
                self._remote._logger.info(f"  - {failed.get('sql_name', 'unknown')} [{failed.get('concurrency_level', 'unknown')}]: {failed.get('error', 'Unknown error')}")

        # Collect JMeter statistics
        jmeter_stats = {}

        # Find statistics.json files in the result directory
        statistics_paths = []
        for root, dirs, files in os.walk(result_dir):
            for file in files:
                if file == "statistics.json":
                    statistics_paths.append(os.path.join(root, file))

        self._remote._logger.info(f"Found {len(statistics_paths)} statistics files")

        # Process each statistics.json file
        for stats_path in statistics_paths:
            try:
                with open(stats_path, 'r', encoding='utf-8') as f:
                    stats_data = json.load(f)

                    # Print JSON content for debugging
                    self._remote._logger.info(f"Parsing statistics file: {stats_path}")
                    self._remote._logger.info(f"JSON keys: {list(stats_data.keys())}")

                    # Extract information from the directory path
                    relative_path = os.path.relpath(stats_path, result_dir)
                    path_parts = relative_path.split(os.sep)

                    # Find the location of the statistics file
                    self._remote._logger.info(f"Statistics file path: {relative_path}")
                    self._remote._logger.info(f"Path parts: {path_parts}")

                    # Parse SQL and concurrency level information from the path
                    if len(path_parts) >= 3:
                        sql_dir = path_parts[0]  # e.g., sql_1
                        concurrency_level = path_parts[1]  # e.g., low

                        key = f"{sql_dir}_{concurrency_level}"
                        self._remote._logger.info(f"Extracted key: {key}")

                        # Check statistics data, support multiple label formats
                        found_stats = False

                        # 1. First try to find non-"Total" labels
                        for label, data in stats_data.items():
                            if label != "Total":
                                self._remote._logger.info(f"Found data label: {label}")
                                # Collect performance data
                                throughput = data.get("throughput", 0)
                                avg_response_time = data.get("meanResTime", 0)
                                error_rate = data.get("errorPct", 0)

                                jmeter_stats[key] = {
                                    "throughput": throughput,
                                    "avg_response_time": avg_response_time,
                                    "error_rate": error_rate,
                                    "min_response_time": data.get("minResTime", 0),
                                    "max_response_time": data.get("maxResTime", 0),
                                    "median_response_time": data.get("medianResTime", 0),
                                    "90th_percentile": data.get("pct1ResTime", 0),
                                    "95th_percentile": data.get("pct2ResTime", 0),
                                    "99th_percentile": data.get("pct3ResTime", 0),
                                    "received_kb": data.get("receivedKBytesPerSec", 0),
                                    "sent_kb": data.get("sentKBytesPerSec", 0)
                                }

                                self._remote._logger.info(f"Found JMeter statistics data: {key}")
                                self._remote._logger.info(f"  - Throughput: {throughput:.2f}/sec")
                                self._remote._logger.info(f"  - Average response time: {avg_response_time:.2f}ms")
                                self._remote._logger.info(f"  - Error rate: {error_rate:.2f}%")
                                found_stats = True
                                break

                        # 2. If no non-"Total" label found, use "Total"
                        if not found_stats and "Total" in stats_data:
                            data = stats_data["Total"]
                            throughput = data.get("throughput", 0)
                            avg_response_time = data.get("meanResTime", 0)
                            error_rate = data.get("errorPct", 0)

                            jmeter_stats[key] = {
                                "throughput": throughput,
                                "avg_response_time": avg_response_time,
                                "error_rate": error_rate,
                                "min_response_time": data.get("minResTime", 0),
                                "max_response_time": data.get("maxResTime", 0),
                                "median_response_time": data.get("medianResTime", 0),
                                "90th_percentile": data.get("pct1ResTime", 0),
                                "95th_percentile": data.get("pct2ResTime", 0),
                                "99th_percentile": data.get("pct3ResTime", 0),
                                "received_kb": data.get("receivedKBytesPerSec", 0),
                                "sent_kb": data.get("sentKBytesPerSec", 0)
                            }

                            self._remote._logger.info(f"Using Total label found JMeter statistics data: {key}")
                            self._remote._logger.info(f"  - Throughput: {throughput:.2f}/sec")
                            self._remote._logger.info(f"  - Average response time: {avg_response_time:.2f}ms")
                            self._remote._logger.info(f"  - Error rate: {error_rate:.2f}%")
                            found_stats = True
            except Exception as e:
                self._remote._logger.error(f"Failed to read JMeter statistics file {stats_path}: {e}")
                import traceback
                traceback.print_exc()

        self._remote._logger.info(f"Collected statistics data: {list(jmeter_stats.keys())}")

        # Save results summary
        summary_file = os.path.join(result_dir, "test_summary.txt")
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("=== Multi-concurrency SQL JMeter Test Summary ===\n")
            f.write(f"Test time: {datetime.now()}\n")
            f.write(f"Total SQL count: {len(sql_results)}\n")
            f.write(f"Total test count: {len(all_results)}\n")
            f.write(f"Successful tests: {len(successful_tests)}\n")
            f.write(f"Failed tests: {len(failed_tests)}\n")
            f.write(f"Success rate: {len(successful_tests)/len(all_results)*100:.2f}%\n\n")

            f.write("=== Detailed Test Results ===\n")

            # Group results by SQL
            for i, (sql_name, results) in enumerate(sql_results.items()):
                sql_short = sql_name
                if len(sql_short) > 70:
                    sql_short = sql_short[:67] + "..."

                f.write(f"[{i+1}] {sql_short}\n")

                # Display results by concurrency level
                for j, result in enumerate(results):
                    status = "success" if result.get("status") == "success" else "failed"
                    concurrency = result.get("concurrency_level", "unknown")
                    description = result.get("concurrency_description", "")

                    f.write(f"  [{j+1}] Concurrency level: {concurrency} - {description} - {status}\n")
                    f.write(f"      Execution time: {result.get('execution_time_seconds', 0):.2f} seconds\n")

                    # Add execution parameters
                    execution_params = result.get("execution_params", {})
                    if execution_params:
                        f.write(f"      Execution parameters:\n")
                        f.write(f"        Thread count: {execution_params.get('thread_count', 'N/A')}\n")
                        f.write(f"        Loop count: {execution_params.get('loop_count', 'N/A')}\n")
                        f.write(f"        Query timeout: {execution_params.get('query_timeout', 'N/A')}ms\n")
                        f.write(f"        Connection pool size: {execution_params.get('pool_max', 'N/A')}\n")
                        f.write(f"        Connection timeout: {execution_params.get('connection_timeout', 'N/A')}ms\n")

                    # Add performance statistics
                    # Construct lookup key in the format "sql_INDEX_CONCURRENCY"
                    stats_key = f"sql_{i+1}_{concurrency}"
                    if stats_key in jmeter_stats:
                        stats = jmeter_stats[stats_key]
                        f.write(f"      Performance statistics:\n")
                        f.write(f"        Throughput: {stats['throughput']:.2f}/sec\n")
                        f.write(f"        Average response time: {stats['avg_response_time']:.2f}ms\n")
                        f.write(f"        Minimum response time: {stats['min_response_time']:.2f}ms\n")
                        f.write(f"        Maximum response time: {stats['max_response_time']:.2f}ms\n")
                        f.write(f"        Median response time: {stats['median_response_time']:.2f}ms\n")
                        f.write(f"        90% response time: {stats['90th_percentile']:.2f}ms\n")
                        f.write(f"        95% response time: {stats['95th_percentile']:.2f}ms\n")
                        f.write(f"        99% response time: {stats['99th_percentile']:.2f}ms\n")
                        f.write(f"        Error rate: {stats['error_rate']:.2f}%\n")
                    else:
                        # Try using other possible key formats
                        found = False
                        for key in jmeter_stats.keys():
                            if concurrency in key:
                                stats = jmeter_stats[key]
                                f.write(f"      Performance statistics (using key {key}):\n")
                                f.write(f"        Throughput: {stats['throughput']:.2f}/sec\n")
                                f.write(f"        Average response time: {stats['avg_response_time']:.2f}ms\n")
                                f.write(f"        Minimum response time: {stats['min_response_time']:.2f}ms\n")
                                f.write(f"        Maximum response time: {stats['max_response_time']:.2f}ms\n")
                                f.write(f"        Median response time: {stats['median_response_time']:.2f}ms\n")
                                f.write(f"        90% response time: {stats['90th_percentile']:.2f}ms\n")
                                f.write(f"        95% response time: {stats['95th_percentile']:.2f}ms\n")
                                f.write(f"        99% response time: {stats['99th_percentile']:.2f}ms\n")
                                f.write(f"        Error rate: {stats['error_rate']:.2f}%\n")
                                found = True
                                break
                        if not found:
                            f.write(f"      Performance statistics: Not found (lookup key: {stats_key})\n")
                            f.write(f"      Available keys: {list(jmeter_stats.keys())}\n")

                    if result.get("status") == "failed":
                        f.write(f"      Error: {result.get('error', 'Unknown error')}\n")

                f.write("\n")

        self._remote._logger.info(f"\nTest summary saved to: {summary_file}")

        # Save CSV format summary
        csv_summary_file = os.path.join(result_dir, "test_summary.csv")
        with open(csv_summary_file, 'w', encoding='utf-8') as f:
            # Write CSV header
            f.write("SQL Name,Concurrency Level,Thread Count,Loop Count,Status,Execution Time(sec),Throughput(tps),Avg Response Time(ms),Max Response Time(ms),Error Rate(%),90% Response Time(ms)\n")

            # Write a row for each test
            for sql_name, results in sql_results.items():
                for result in results:
                    status = "success" if result.get("status") == "success" else "failed"
                    concurrency = result.get("concurrency_level", "unknown")
                    execution_time = result.get("execution_time_seconds", 0)

                    # Get execution parameters
                    execution_params = result.get("execution_params", {})
                    thread_count = execution_params.get("thread_count", "N/A")
                    loop_count = execution_params.get("loop_count", "N/A")

                    # Get performance statistics
                    i = next((i for i, (name, _) in enumerate(sql_results.items()) if name == sql_name), 0)
                    key = f"sql_{i+1}_{concurrency}"

                    throughput = 0
                    avg_response_time = 0
                    max_response_time = 0
                    error_rate = 0
                    percentile_90 = 0
                    received_kb = 0

                    if key in jmeter_stats:
                        stats = jmeter_stats[key]
                        throughput = stats.get("throughput", 0)
                        avg_response_time = stats.get("avg_response_time", 0)
                        max_response_time = stats.get("max_response_time", 0)
                        error_rate = stats.get("error_rate", 0)
                        percentile_90 = stats.get("90th_percentile", 0)
                        received_kb = stats.get("received_kb", 0)
                    else:
                        # Try using other possible key formats
                        for key in jmeter_stats.keys():
                            if concurrency in key:
                                stats = jmeter_stats[key]
                                throughput = stats.get("throughput", 0)
                                avg_response_time = stats.get("avg_response_time", 0)
                                max_response_time = stats.get("max_response_time", 0)
                                error_rate = stats.get("error_rate", 0)
                                percentile_90 = stats.get("90th_percentile", 0)
                                received_kb = stats.get("received_kb", 0)
                                break

                    # Write a CSV data row
                    f.write(f"{sql_name},{concurrency},{thread_count},{loop_count},{status},{execution_time:.2f},{throughput:.2f},{avg_response_time:.2f},{max_response_time:.2f},{error_rate:.2f},{percentile_90:.2f}\n")

        self._remote._logger.info(f"CSV format summary saved to: {csv_summary_file}")

    def run(self):
        """Run test case"""
        self.run_test()

    def cleanup(self):
        """Clean up resources"""
        pass

    def desc(self):
        """Test case description"""
        return """
            Multi-concurrency SQL JMeter Test:
            Run tests for each SQL at different concurrency levels, collect and compare performance data
            """

    def author(self):
        """Author"""
        return "TDengine Test Team"

    def tags(self):
        """Tags"""
        return T