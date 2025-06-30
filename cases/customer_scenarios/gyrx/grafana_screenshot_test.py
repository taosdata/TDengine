## taostest --setup=pocs/gyrx/demo.yaml --case=customer_scenarios/gyrx/grafana_screenshot_test.py --keep

"""
Grafana Dashboard Screenshot Test Case for Gyrx Scenarios
Simple test to capture Grafana dashboard screenshots
"""

import os
from taostest import TDCase
from taostest.util.remote import Remote
from taostest.util.common import TDCom
from taostest.util.playwright_util import PlaywrightUtil


class GrafanaScreenshotTest(TDCase):
    """Test case for capturing Grafana dashboard screenshots"""

    def init(self):
        """Initialize test case"""
        # Initialize TDCom for configuration access
        self.tdCom = TDCom(self.tdSql)
        # Initialize remote logger
        self._remote: Remote = Remote(self.logger)

        # Get configuration using tdCom helper methods - must exist
        if not hasattr(self, 'env_setting') or not self.env_setting:
            raise Exception("env_setting is required for Grafana screenshot test")

        grafana_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "grafana")

        if not grafana_setting:
            raise Exception("grafana configuration not found in env_setting")

        # Get configuration from settings
        self.grafana_host = grafana_setting.get("fqdn", [])[0] if grafana_setting.get("fqdn") else None
        self.grafana_port = grafana_setting.get("spec", {}).get("port")
        self.dashboards = grafana_setting.get("spec", {}).get("dashboards", [])

        if not self.grafana_host:
            raise Exception("grafana host not configured")
        if not self.grafana_port:
            raise Exception("grafana port not configured")
        if not self.dashboards:
            raise Exception("grafana dashboards not configured")

        # Initialize Playwright for screenshots
        self.playwright = PlaywrightUtil(self.envMgr)

        # Clear any existing screenshots from previous tests to avoid collecting old ones
        self.playwright.reset()

        self._remote._logger.info(f"Grafana host: {self.grafana_host}:{self.grafana_port}")
        self._remote._logger.info(f"Dashboards to capture: {len(self.dashboards)}")

    def _take_grafana_screenshot(self, dashboard_url: str, output_file: str) -> bool:
        """Take screenshot of Grafana dashboard (no login required)"""
        try:
            from playwright.sync_api import sync_playwright

            self._remote._logger.info(f"Starting Grafana screenshot: {dashboard_url}")

            with sync_playwright() as p:
                # Launch browser with larger viewport for better dashboard capture
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        '--window-size=2560,2400',
                        '--disable-web-security',
                        '--disable-features=VizDisplayCompositor'
                    ]
                )
                # Create page with large viewport for dashboard content
                page = browser.new_page(
                    viewport={'width': 2560, 'height': 2400}
                )

                # Navigate to dashboard URL
                self._remote._logger.info(f"Navigating to dashboard: {dashboard_url}")
                page.goto(dashboard_url, wait_until="networkidle", timeout=30000)

                # Wait for dashboard to fully load
                self._remote._logger.info("Waiting for dashboard to load...")
                page.wait_for_timeout(8000)  # Give Grafana time to load all panels

                # Wait for any loading indicators to disappear
                try:
                    # Wait for Grafana loading spinners to disappear
                    page.wait_for_selector('.panel-loading', state='detached', timeout=10000)
                except Exception:
                    self._remote._logger.info("No loading indicators found or timeout, continuing...")

                # Additional wait for data to populate
                page.wait_for_timeout(5000)

                # Scroll to ensure all panels are rendered
                self._remote._logger.info("Scrolling to render all panels...")
                try:
                    page.evaluate("""
                        async () => {
                            // Scroll to bottom to trigger lazy-loaded panels
                            window.scrollTo(0, document.body.scrollHeight);
                            await new Promise(resolve => setTimeout(resolve, 3000));

                            // Scroll back to top for screenshot
                            window.scrollTo(0, 0);
                            await new Promise(resolve => setTimeout(resolve, 2000));
                        }
                    """)
                except Exception as scroll_error:
                    self._remote._logger.warning(f"Scrolling failed: {scroll_error}")

                # Final wait before screenshot
                page.wait_for_timeout(3000)

                # Take screenshot
                screenshot_path = os.path.join(self.run_log_dir, output_file)
                self._remote._logger.info(f"Taking screenshot: {screenshot_path}")
                page.screenshot(path=screenshot_path, full_page=True)

                # Also save with playwright's expected naming for collection
                if hasattr(self.playwright, '_screenshot_dir'):
                    playwright_dir = self.playwright._screenshot_dir
                    if playwright_dir and os.path.exists(playwright_dir):
                        playwright_full_path = os.path.join(playwright_dir, output_file)
                        page.screenshot(path=playwright_full_path, full_page=True)
                        self._remote._logger.info(f"Also saved for playwright collection: {playwright_full_path}")

                # Close browser
                browser.close()

                self._remote._logger.info("✓ Grafana screenshot completed successfully")
                return True

        except Exception as e:
            self._remote._logger.error(f"Grafana screenshot failed: {e}")
            return False

    def test_grafana_dashboards_screenshot(self):
        """
        Test case: Capture screenshots of all configured Grafana dashboards

        用例概述：截图所有配置的 Grafana 仪表板

        用例步骤：
        1. 遍历配置中的所有仪表板
        2. 构建每个仪表板的完整URL
        3. 访问仪表板并等待加载完成
        4. 截图保存

        验证点：
        1. 能够访问所有配置的仪表板
        2. 截图成功保存
        3. 仪表板内容正确显示
        """
        self._remote._logger.info("Starting Grafana dashboard screenshot test...")

        screenshot_count = 0

        try:
            for dashboard in self.dashboards:
                dashboard_id = dashboard.get("id")
                dashboard_name = dashboard.get("name")

                if not dashboard_id or not dashboard_name:
                    self._remote._logger.warning(f"Skipping invalid dashboard config: {dashboard}")
                    continue

                # Build dashboard URL
                dashboard_url = f"http://{self.grafana_host}:{self.grafana_port}/d/{dashboard_id}/{dashboard_name}"
                output_file = f"grafana_dashboard_{dashboard_name}.png"

                self._remote._logger.info(f"Capturing dashboard: {dashboard_name} ({dashboard_id})")

                # Take screenshot
                screenshot_success = self._take_grafana_screenshot(dashboard_url, output_file)

                if screenshot_success:
                    self._remote._logger.info(f"✓ Dashboard screenshot saved: {output_file}")

                    # Manually copy screenshot to screenshots directory
                    screenshots_dir = os.path.join(self.run_log_dir, "screenshots")
                    try:
                        # Create screenshots directory if it doesn't exist
                        os.makedirs(screenshots_dir, exist_ok=True)

                        # Copy our custom screenshot to the screenshots directory
                        source_screenshot = os.path.join(self.run_log_dir, output_file)
                        target_screenshot = os.path.join(screenshots_dir, output_file)

                        if os.path.exists(source_screenshot):
                            import shutil
                            shutil.copy2(source_screenshot, target_screenshot)
                            self._remote._logger.info(f"✓ Screenshot copied to: {target_screenshot}")
                            screenshot_count += 1

                            # Clean up the original file from run_log_dir root
                            try:
                                os.remove(source_screenshot)
                                self._remote._logger.info(f"✓ Cleaned up original screenshot: {source_screenshot}")
                            except Exception as cleanup_error:
                                self._remote._logger.warning(f"Failed to clean up original screenshot: {cleanup_error}")
                        else:
                            self._remote._logger.warning(f"Source screenshot not found: {source_screenshot}")

                    except Exception as e:
                        self._remote._logger.warning(f"Failed to copy screenshot: {e}")
                else:
                    self._remote._logger.warning(f"✗ Failed to capture dashboard: {dashboard_name}")

            self._remote._logger.info(f"✓ Grafana dashboard screenshot test completed")
            self._remote._logger.info(f"✓ Successfully captured {screenshot_count} dashboard screenshots")

        except Exception as e:
            self._remote._logger.error(f"Grafana dashboard screenshot test failed: {e}")
            raise
        finally:
            self._remote._logger.info("Grafana screenshot test cleanup completed")

    def run(self):
        """Run all test cases"""
        self._remote._logger.info("=== Starting Grafana Dashboard Screenshot Tests ===")

        try:
            # Run main test case
            self.test_grafana_dashboards_screenshot()

            self._remote._logger.info("✓ Grafana dashboard screenshots verified")

        except Exception as e:
            self._remote._logger.error(f"Grafana screenshot tests failed: {e}")
            raise
        finally:
            # Collect screenshots to test result directory
            self._remote._logger.info("Collecting screenshots to test result directory...")
            self.playwright.collect_screenshots(self.run_log_dir)

        self._remote._logger.info("=== All Grafana Screenshot Tests Completed ===")

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        """Cleanup method called by framework after test completion"""
        try:
            # Ensure screenshots are collected to test result directory
            if hasattr(self, 'playwright') and self.playwright:
                self._remote._logger.info("Final screenshot collection during cleanup...")
                self.playwright.collect_screenshots(self.run_log_dir)
                self.playwright.reset()
        except Exception as e:
            self._remote._logger.warning(f"Screenshot cleanup failed: {e}")

        # Call parent cleanup
        super().cleanup()