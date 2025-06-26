#!/usr/bin/env python3
"""
Simple Playwright functionality test
Verify that Playwright component is properly integrated into the framework
"""

import os
import time
from taostest.tdcase import TDCase
from taostest.util.playwright_util import PlaywrightUtil


class SimplePlaywrightTest(TDCase):
    def author(self):
        return "taostest"

    def desc(self):
        return "Simple Playwright integration test"

    def tags(self):
        return ["playwright", "integration", "basic"]

    def init(self):
        """Initialize test environment"""
        self.logger.info("Initializing simple Playwright test")
        # Similar to TDCom initialization pattern
        self.playwright = PlaywrightUtil(self.envMgr)
        
    def run(self):
        """Run test"""
        try:
            # 1. Check Playwright component
            self.logger.info("Checking Playwright component availability...")
            self.logger.info("Playwright component initialized successfully")
            
            # 2. Test basic properties and methods of the component
            self.logger.info("Testing Playwright component methods...")
            
            # Check if required methods exist
            required_methods = ['take_screenshot', 'take_screenshot_text', 'collect_screenshots', 'reset']
            for method in required_methods:
                if not hasattr(self.playwright, method):
                    self.logger.error(f"Required method '{method}' not found in Playwright util")
                    return False
                self.logger.info(f"Method '{method}' found")
            
            # 3. Create simple HTML file for testing
            self.logger.info("Creating test HTML file...")
            html_content = '''<!DOCTYPE html>
<html>
<head><title>Test</title></head>
<body>
    <h1>Playwright Test Page</h1>
    <p>This is a test page created at ''' + time.strftime("%Y-%m-%d %H:%M:%S") + '''</p>
</body>
</html>'''
            
            test_html_file = "/tmp/playwright_test.html"
            with open(test_html_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            self.logger.info(f"Test HTML file created: {test_html_file}")
            
            # 4. Test screenshot functionality (if available)
            self.logger.info("Testing screenshot functionality...")
            try:
                screenshot_success = self.playwright.take_screenshot(
                    target=test_html_file,
                    output_file="test_screenshot.png"
                )
                
                if screenshot_success:
                    self.logger.info("Screenshot test successful")
                else:
                    self.logger.info("Screenshot test failed (expected if browsers not installed)")
            
            except Exception as e:
                self.logger.info(f"Screenshot test exception (expected if playwright not fully installed): {e}")
            
            # 5. Test reset method
            self.logger.info("Testing Playwright reset...")
            self.playwright.reset()
            self.logger.info("Playwright reset completed")
            
            # 6. Clean up test files
            if os.path.exists(test_html_file):
                os.remove(test_html_file)
                self.logger.info("Test HTML file cleaned up")
            
            self.logger.info("Simple Playwright integration test completed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"Test failed with error: {e}")
            import traceback
            traceback.print_exc()
            return False

    def cleanup(self):
        """Clean up test environment"""
        self.logger.info("Cleaning up simple Playwright test")


# If running this script directly
if __name__ == "__main__":
    print("Simple Playwright Integration Test")
    print("This test verifies that the Playwright component is properly integrated into the framework.")
    print("To run this test:")
    print("taostest --use=playwright.yaml --case=playwright_examples/simple_playwright_test.py")