"""
Multi-concurrency SQL JMeter Test
Run tests for each SQL at different concurrency levels, collect and compare performance data
"""

import os
from taostest import TDCase, T
from taostest.util.jmeter_util import JMeterUtil
from taostest.util.common import TDCom


class MultiConcurrencySqlTest(TDCase):
    """Multi-concurrency SQL JMeter test case"""

    def init(self):
        """Initialize test environment"""
        self.tdCom = TDCom(self.tdSql)
        self.jmeter_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "jmeter")
        self.logger.info(f"JMeter Settings: {self.jmeter_setting}")

        # Create results directory
        self.results_dir = os.path.join(self.run_log_dir, "jmeter_results")
        os.makedirs(self.results_dir, exist_ok=True)

        # Initialize JMeter utility
        self.jmeter_util = JMeterUtil(self.envMgr)
        
        # Setup JMeter environment
        if not self.jmeter_util.setup_jmeter(self.jmeter_setting):
            raise RuntimeError("Failed to setup JMeter environment")

        # Initialize other paths
        self.test_root = os.environ['TEST_ROOT']


    def run_test(self):
        """Run multi-concurrency SQL tests"""
        try:
            # Get SQL file path
            server_config = self.jmeter_setting.get("spec", {}).get("server", {})
            sql_file = server_config.get("sql_file")
            if not sql_file:
                self.logger.error("SQL file not specified")
                return

            sql_file_path = os.path.join(self.test_root, f"env/jmeter/{sql_file}")

            # Run multi-concurrency test using utility
            results = self.jmeter_util.run_multi_concurrency_test(
                sql_file_path=sql_file_path,
                results_dir=self.results_dir,
                test_root=self.test_root
            )

            if results:
                self.logger.info(f"Multi-concurrency test completed with {len(results)} total test runs")
            else:
                self.logger.error("Multi-concurrency test failed to produce results")

        except Exception as e:
            self.logger.error(f"Test execution failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Clean up JMeter runner resources
            self.jmeter_util.cleanup()


    def run(self):
        """Run test case"""
        self.run_test()

    def cleanup(self):
        """Clean up resources"""
        if hasattr(self, 'jmeter_util'):
            self.jmeter_util.cleanup()

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