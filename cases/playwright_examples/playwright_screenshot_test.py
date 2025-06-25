#!/usr/bin/env python3
"""
Playwright screenshot functionality test case example
Demonstrates how to use Playwright component for web page, file and text screenshots
"""

import os
import time
from taostest.tdcase import TDCase
from taostest.util.playwright_util import PlaywrightUtil


class PlaywrightScreenshotTest(TDCase):
    def author(self):
        return "taostest"

    def desc(self):
        return "Playwright screenshot functionality test"

    def tags(self):
        return ["playwright", "screenshot", "demo"]

    def init(self):
        """Initialize test environment"""
        self.logger.info("Initializing Playwright screenshot test")
        # Similar to TDCom initialization pattern
        self.playwright = PlaywrightUtil(self.envMgr)

    def run(self):
        """Run test"""

        # 1. Test URL screenshot (if network is available)
        self.logger.info("Testing URL screenshot...")
        url_success = self.playwright.take_screenshot(
            target="https://www.baidu.com",
            output_file="baidu_homepage.png"
        )
        if not url_success:
            self.logger.warning("URL screenshot failed, but continuing with other tests")

        # 2. Create sample text file and take screenshot
        self.logger.info("Testing text file screenshot...")
        sample_text_content = f"""=== Playwright Text Screenshot Demo ===
Generated at: {time.strftime("%Y-%m-%d %H:%M:%S")}

=== Test Summary ===
Total tests: 5
Successful tests: 4
Failed tests: 1
Success rate: 80.00%

=== Detailed Results ===
[1] URL Screenshot Test - Success
    Execution time: 2.35 seconds
    Performance metrics:
      Response time: 1250ms
      File size: 245KB

[2] Text File Screenshot Test - Success
    Execution time: 0.45 seconds
    Performance metrics:
      Processing time: 125ms
      Output size: 89KB

[3] Network Connection Test - Failed
    Error: Connection timeout
    Execution time: 30.00 seconds (timeout)

[4] File Format Test - Success
    Execution time: 0.12 seconds
    Supported formats: PNG, JPG, HTML

[5] Batch Processing Test - Success
    Files processed: 15
    Execution time: 3.67 seconds
    Average time per file: 244ms
"""

        # Create sample text file
        sample_text_file = f"/tmp/playwright_test_results_{time.strftime('%Y%m%d_%H%M%S')}.txt"
        with open(sample_text_file, 'w', encoding='utf-8') as f:
            f.write(sample_text_content)

        # Take screenshot of text file
        text_success = self.playwright.take_screenshot_text(
            text_file=sample_text_file,
            output_file="test_results_screenshot.png",
            title="Playwright Demo Test Results"
        )

        if text_success:
            self.logger.info("Text file screenshot successful")
        else:
            self.logger.warning("Text file screenshot failed")

        # 3. Test batch screenshot functionality (screenshot existing test results)
        self.logger.info("Testing batch screenshot of existing test results...")

        # Find existing test result directories
        run_dir = os.path.dirname(self.run_log_dir)  # TestNG/run
        if os.path.exists(run_dir):
            # Screenshot all test_summary.txt files
            batch_results = self.playwright.take_screenshots_from_directory(
                directory=run_dir,
                file_patterns=['test_summary.txt', '*.result', '*.log'],
                recursive=True
            )

            successful_screenshots = sum(1 for success in batch_results.values() if success)
            total_files = len(batch_results)

            if total_files > 0:
                self.logger.info(f"Batch screenshot completed: {successful_screenshots}/{total_files} files processed")
            else:
                self.logger.info("No existing test result files found for batch screenshot")

        # 4. Clean up sample files
        try:
            if os.path.exists(sample_text_file):
                os.remove(sample_text_file)
        except:
            pass

        # 5. Collect all screenshot files to run log directory
        self.logger.info("Collecting screenshots...")
        self.playwright.collect_screenshots(self.run_log_dir)
        self.playwright.reset()
        self.logger.info("Playwright screenshot test completed successfully!")

        return True

    def cleanup(self):
        """Clean up test environment"""
        self.logger.info("Cleaning up Playwright screenshot test")
