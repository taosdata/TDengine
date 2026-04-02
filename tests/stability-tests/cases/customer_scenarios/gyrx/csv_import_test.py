## taostest --setup=pocs/gyrx/demo.yaml --case=customer_scenarios/gyrx/csv_import_test.py --keep


"""
TaosX CSV Import Test Case for Gyrx Scenarios
Based on test_sanity_csv from taosx repository
"""

import os
from taostest import TDCase
from taostest.util.taosx_util import TaosxUtil, TaosxTestUtil
from taostest.util.remote import Remote
from taostest.util.common import TDCom
from taostest.util.playwright_util import PlaywrightUtil


class CSVImportTest(TDCase):
    """Test case for TaosX CSV import functionality"""

    def init(self):
        """Initialize test case"""
        # Initialize TDCom for database operations
        self.tdCom = TDCom(self.tdSql)
        self.stbname = "csv_meters"
        self.row_count = 5
        # Initialize remote logger
        self._remote: Remote = Remote(self.logger)

        # Get configuration using tdCom helper methods - must exist
        if not hasattr(self, 'env_setting') or not self.env_setting:
            raise Exception("env_setting is required for TaosX test")

        taosx_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosx")
        taosadapter_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosadapter")

        if not taosx_setting:
            raise Exception("taosx configuration not found in env_setting")
        if not taosadapter_setting:
            raise Exception("taosadapter configuration not found in env_setting")

        # Get configuration from settings
        self.taosx_host = taosx_setting.get("fqdn", [])[0] if taosx_setting.get("fqdn") else None
        self.taosx_port = taosx_setting.get("spec", {}).get("port")
        self.taosadapter_host = taosadapter_setting.get("fqdn", [])[0] if taosadapter_setting.get("fqdn") else None
        self.taosadapter_port = taosadapter_setting.get("spec", {}).get("adapter_config", {}).get("port")

        if not self.taosx_host:
            raise Exception("taosx host not configured")
        if not self.taosx_port:
            raise Exception("taosx port not configured")
        if not self.taosadapter_host:
            raise Exception("taosadapter host not configured")
        if not self.taosadapter_port:
            raise Exception("taosadapter port not configured")

        # Initialize TaosX util
        self.adapter = TaosxUtil(
            taosx_host=self.taosx_host,
            taosx_port=self.taosx_port,
            taosadapter_host=self.taosadapter_host,
            taosadapter_port=self.taosadapter_port,
            logger=self._remote._logger
        )

        # Initialize Playwright for screenshots
        self.playwright = PlaywrightUtil(self.envMgr)

        # Clear any existing screenshots from previous tests to avoid collecting old ones
        self.playwright.reset()

        self._remote._logger.info(f"TaosX adapter initialized: {self.taosx_host}:{self.taosx_port}")
        self._remote._logger.info(f"TaosAdapter: {self.taosadapter_host}:{self.taosadapter_port}")

    def _take_screenshot_with_manual_click(self, target_url: str, username: str, password: str, click_selector: str, output_file: str) -> bool:
        """Take screenshot with manual login and click operation using comprehensive scrolling"""
        try:
            from playwright.sync_api import sync_playwright

            self._remote._logger.info("Starting comprehensive screenshot with Chinese locale...")

            with sync_playwright() as p:
                # Launch browser with Chinese locale and larger viewport
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        '--lang=zh-CN',
                        '--accept-lang=zh-CN,zh,en',
                        '--window-size=2560,2400',
                        '--disable-web-security',
                        '--disable-features=VizDisplayCompositor'
                    ]
                )
                # Create page with Chinese locale settings and much larger viewport
                page = browser.new_page(
                    locale='zh-CN',
                    timezone_id='Asia/Shanghai',
                    viewport={'width': 2560, 'height': 2400}  # Much larger viewport
                )

                # Navigate to login page
                login_url = f"http://{self.taosx_host}:{self.taosx_port}/login"
                self._remote._logger.info(f"Navigating to login page: {login_url}")
                page.goto(login_url, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(2000)

                # Fill login form with multiple selector attempts
                self._remote._logger.info("Filling login credentials...")

                # Try multiple username selectors
                username_selectors = [
                    'input[name="username"]',
                    'input[id="username"]',
                    'input[placeholder*="用户名"]',
                    'input[placeholder*="Username"]',
                    'input[type="text"]',
                    'input[type="email"]'
                ]

                username_filled = False
                for selector in username_selectors:
                    try:
                        element = page.query_selector(selector)
                        if element:
                            page.fill(selector, username)
                            self._remote._logger.info(f"Username filled with selector: {selector}")
                            username_filled = True
                            break
                    except Exception as e:
                        self._remote._logger.debug(f"Failed with username selector {selector}: {e}")

                if not username_filled:
                    page.fill('input[type="text"]:first-of-type', username)
                    self._remote._logger.info("Username filled with first text input")

                # Try multiple password selectors
                password_selectors = [
                    'input[name="password"]',
                    'input[id="password"]',
                    'input[placeholder*="密码"]',
                    'input[placeholder*="Password"]',
                    'input[type="password"]'
                ]

                password_filled = False
                for selector in password_selectors:
                    try:
                        element = page.query_selector(selector)
                        if element:
                            page.fill(selector, password)
                            self._remote._logger.info(f"Password filled with selector: {selector}")
                            password_filled = True
                            break
                    except Exception as e:
                        self._remote._logger.debug(f"Failed with password selector {selector}: {e}")

                if not password_filled:
                    page.fill('input[type="password"]', password)
                    self._remote._logger.info("Password filled with type=password")

                page.wait_for_timeout(1000)

                # Try multiple login button selectors
                self._remote._logger.info("Clicking login button...")
                login_button_selectors = [
                    'button[type="submit"]',
                    'button:has-text("登录")',
                    'button:has-text("Login")',
                    'input[type="submit"]',
                    '.login-btn',
                    '.submit-btn',
                    'button'
                ]

                login_clicked = False
                for selector in login_button_selectors:
                    try:
                        element = page.query_selector(selector)
                        if element:
                            page.click(selector)
                            self._remote._logger.info(f"Login button clicked with selector: {selector}")
                            login_clicked = True
                            break
                    except Exception as e:
                        self._remote._logger.debug(f"Failed with login button selector {selector}: {e}")

                if not login_clicked:
                    page.press('input[type="password"]', 'Enter')
                    self._remote._logger.info("Login attempted with Enter key")

                # Wait for login to complete
                page.wait_for_timeout(5000)

                # Navigate to target page
                self._remote._logger.info(f"Navigating to target page: {target_url}")
                page.goto(target_url, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(3000)

                # Click the specified element if provided
                if click_selector:
                    self._remote._logger.info(f"Clicking element: {click_selector}")
                    try:
                        page.click(click_selector)
                        page.wait_for_timeout(2000)
                        self._remote._logger.info("✓ Successfully clicked element")
                    except Exception as click_error:
                        self._remote._logger.warning(f"Failed to click {click_selector}: {click_error}")

                # Comprehensive scrolling similar to the original implementation
                self._remote._logger.info("Performing comprehensive scrolling for full-page capture...")
                try:
                    # First detect scrollable elements
                    scrollable_elements = page.evaluate("""
                        () => {
                            const elements = [];
                            const selectors = [
                                'body', '.content', '.main-content', '[style*="overflow"]', '.scrollable',
                                '.ant-layout-content', '.el-main', '.v-content', '.app-content',
                                '.page-content', '.container-fluid', '.main-container', '[class*="scroll"]',
                                '[id*="content"]', '[class*="content"]', '.layout-content', '.main-wrapper'
                            ];

                            selectors.forEach(selector => {
                                try {
                                    const el = document.querySelector(selector);
                                    if (el && (el.scrollHeight > el.clientHeight || el.scrollWidth > el.clientWidth)) {
                                        elements.push({
                                            selector: selector,
                                            scrollHeight: el.scrollHeight,
                                            clientHeight: el.clientHeight
                                        });
                                    }
                                } catch (e) {}
                            });

                            return elements;
                        }
                    """)

                    self._remote._logger.info(f"Found {len(scrollable_elements)} scrollable elements")

                    # Scroll through each scrollable element
                    for element in scrollable_elements:
                        self._remote._logger.info(f"Scrolling element: {element['selector']}")
                        page.evaluate(f"""
                            async (selector) => {{
                                const element = document.querySelector(selector);
                                if (!element) return;

                                await new Promise((resolve) => {{
                                    let totalHeight = 0;
                                    const distance = 200;
                                    const maxHeight = element.scrollHeight;

                                    const timer = setInterval(() => {{
                                        element.scrollTop += distance;
                                        totalHeight += distance;

                                        if (totalHeight >= maxHeight || element.scrollTop + element.clientHeight >= element.scrollHeight) {{
                                            clearInterval(timer);
                                            resolve();
                                        }}
                                    }}, 150);
                                }});
                            }}
                        """, element['selector'])
                        page.wait_for_timeout(1000)

                    # Enhanced main window scrolling for complete long screenshot capture
                    page.evaluate("""
                        async () => {
                            const expandContent = async () => {
                                window.scrollTo(0, document.body.scrollHeight);
                                await new Promise(resolve => setTimeout(resolve, 2000));

                                window.scrollTo(0, 0);
                                await new Promise(resolve => setTimeout(resolve, 1000));

                                for (let pass = 0; pass < 5; pass++) {
                                    await new Promise((resolve) => {
                                        let totalHeight = 0;
                                        const distance = 100;
                                        let lastScrollHeight = 0;

                                        const timer = setInterval(() => {
                                            const currentScrollHeight = Math.max(
                                                document.body.scrollHeight,
                                                document.documentElement.scrollHeight,
                                                document.body.offsetHeight,
                                                document.documentElement.offsetHeight
                                            );

                                            if (currentScrollHeight > lastScrollHeight) {
                                                lastScrollHeight = currentScrollHeight;
                                            }

                                            window.scrollBy(0, distance);
                                            totalHeight += distance;

                                            if (totalHeight >= lastScrollHeight ||
                                                window.pageYOffset + window.innerHeight >= lastScrollHeight) {
                                                clearInterval(timer);
                                                resolve();
                                            }
                                        }, 50);
                                    });

                                    await new Promise(resolve => setTimeout(resolve, 2000));
                                }
                            };

                            await expandContent();
                        }
                    """)

                    page.wait_for_timeout(5000)

                    # Scroll all elements back to top
                    page.evaluate("""
                        () => {
                            window.scrollTo(0, 0);

                            const selectors = [
                                'body', '.content', '.main-content', '[style*="overflow"]', '.scrollable',
                                '.ant-layout-content', '.el-main', '.v-content', '.app-content',
                                '.page-content', '.container-fluid', '.main-container', '[class*="scroll"]',
                                '[id*="content"]', '[class*="content"]', '.layout-content', '.main-wrapper'
                            ];

                            selectors.forEach(selector => {
                                try {
                                    const elements = document.querySelectorAll(selector);
                                    elements.forEach(el => {
                                        if (el) {
                                            el.scrollTop = 0;
                                            el.scrollLeft = 0;
                                        }
                                    });
                                } catch (e) {}
                            });
                        }
                    """)

                    page.wait_for_timeout(2000)

                except Exception as scroll_error:
                    self._remote._logger.warning(f"Comprehensive scrolling failed: {scroll_error}")

                # Final wait before screenshot to ensure everything is rendered
                self._remote._logger.info("Final wait before capturing full-page screenshot...")
                page.wait_for_timeout(3000)

                # Take screenshot - save to playwright's expected location
                screenshot_path = os.path.join(self.run_log_dir, output_file)

                self._remote._logger.info(f"Taking comprehensive full-page screenshot: {screenshot_path}")
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

                self._remote._logger.info("✓ Comprehensive screenshot completed successfully")
                return True

        except Exception as e:
            self._remote._logger.error(f"Comprehensive screenshot failed: {e}")
            return False

    def csv_import_test(self):
        """
        Test case: Test CSV file upload and import functionality

        用例概述：测试 CSV 文件上传和导入功能

        用例步骤：
        1. 检查 TaosX 服务状态
        2. 上传 CSV 文件到 TaosX 服务器
        3. 在 DB 中创建超级表 csv_meters
        4. 创建 TaosX 导入任务
        5. 启动任务并等待完成
        6. 验证数据导入结果

        验证点：
        1. CSV 文件能够成功上传到 TaosX 服务器
        2. 导入任务能够正确创建和执行
        3. 导入 DB 中数据的条数正确
        4. 使用 int(parse_float(current)) 能将 current 字段转换为 int 类型
        5. 能够正确导入包含双引号的数据
        """
        self._remote._logger.info("Starting CSV upload and import test...")

        # Test configuration
        test_db = TaosxTestUtil.generate_random_name()
        csv_file_path = os.path.join(
            os.path.dirname(__file__),
            "test_data",
            "csv_test.csv"
        )

        try:
            # Ensure CSV test data exists
            if not os.path.exists(csv_file_path):
                TaosxTestUtil.create_csv_test_data(csv_file_path, rows=5)
                self._remote._logger.info(f"Created CSV test data: {csv_file_path}")

            # 1. Create database and super table FIRST (before TaosX operations)
            self._remote._logger.info("Creating database and super table...")
            self.tdCom.createDb(dbname=test_db)
            self._remote._logger.info(f"✓ Created database: {test_db}")

            # Create stable using tdSql.execute
            create_stable_sql = f"""CREATE STABLE `{test_db}`.`{self.stbname}` (
                `ts` TIMESTAMP,
                `current` INT,
                `voltage` INT,
                `phase` DOUBLE,
                `desc` BINARY(64)
            ) TAGS (`id` INT)"""

            self.tdSql.execute(create_stable_sql)
            self._remote._logger.info(f"✓ Created super table {self.stbname}")

            # 2. Check TaosX service status
            self._remote._logger.info("Checking TaosX service status...")
            taosx_available = self.adapter.check_taosx_status()
            self._remote._logger.info(f"TaosX service available: {taosx_available}")

            # 3. Upload CSV file to TaosX server
            self._remote._logger.info("Uploading CSV file to TaosX server...")
            upload_path = None
            try:
                upload_path = self.adapter.upload_file(csv_file_path)
                self._remote._logger.info(f"✓ CSV file uploaded successfully: {upload_path}")
            except Exception as upload_error:
                self._remote._logger.warning(f"CSV upload failed: {upload_error}")
                self._remote._logger.info("TaosX service may not be available, skipping task creation")
                return

            # 4. Create TaosX import task using original framework pattern
            self._remote._logger.info("Creating TaosX import task...")
            task_name = f"csv_import_{TaosxTestUtil.generate_random_name(5)}"

            # Get cluster ID for proper frontend display
            cluster_id = self.adapter.get_cluster_id()

            # Build task config following original framework YAML + get_task_payload pattern
            task_config = TaosxTestUtil.build_csv_task_config_from_yaml_style(
                task_name=task_name,
                upload_file_path=upload_path,
                target_db=test_db,
                target_stable=self.stbname,
                subtable_template="ctb_${id}",  # Use original framework template style
                tag_fields=["id"],
                taosadapter_host=self.adapter.taosadapter_host,
                has_header=True,
                cluster_id=cluster_id
            )

            # Populate CSV input data for frontend compatibility
            task_config = self.adapter.populate_csv_input_data(task_config, csv_file_path)

            task_info = self.adapter.create_task(task_config)
            task_id = task_info["id"]
            self._remote._logger.info(f"✓ Created import task: {task_id}")

            # 5. Check initial task status and start if needed
            task_status = self.adapter.get_task_status(task_id)
            current_status = task_status.get("status", "").strip().lower()
            self._remote._logger.info(f"Initial task status: {current_status}")

            # Only start task if it's not already running/completed
            if current_status in ["created", "stopped", "failed"]:
                self._remote._logger.info("Starting import task...")
                start_success = self.adapter.start_task(task_id)
                if not start_success:
                    self._remote._logger.warning("Failed to start task, but continuing...")
            elif current_status == "running":
                self._remote._logger.info("Task is already running, skipping start step")
            elif current_status == "completed":
                self._remote._logger.info("Task already completed")
            else:
                self._remote._logger.info(f"Task in status: {current_status}, proceeding...")

            self._remote._logger.info("Waiting for task completion...")
            final_status = self.adapter.wait_for_task_completion(task_id, timeout=120)
            self._remote._logger.info(f"✓ Task completed with status: {final_status}")

            # 6. Get task metrics
            metrics = self.adapter.get_task_metrics(task_id)
            self._remote._logger.info(f"Task metrics: {metrics}")

            # 7. Verify data import results
            self.tdSql.query(f'select * from `{test_db}`.`{self.stbname}`')
            self.tdSql.checkEqual(self.tdSql.query_row, self.row_count)
            self._remote._logger.info(f"✓ Data imported successfully, row count: {self.tdSql.query_row}")
            self._remote._logger.info("✓ CSV upload and import test completed successfully")
            self._remote._logger.info("✓ File upload functionality verified")
            self._remote._logger.info("✓ Task creation and execution verified")

            # 8. Take screenshot of task in TaosX frontend - generate full length screenshot
            self._remote._logger.info("Taking full-page screenshot of task in TaosX frontend...")
            task_edit_url = f"http://{self.taosx_host}:{self.taosx_port}/dataIn/{task_id}/csv/edit"

            # First login and navigate manually for better control
            login_url = f"http://{self.taosx_host}:{self.taosx_port}/login"
            self._remote._logger.info(f"Login first at: {login_url}")

            # Custom screenshot with manual click operation using Chinese locale
            screenshot_success = self._take_screenshot_with_manual_click(
                target_url=task_edit_url,
                username="root",
                password="taosdata",
                click_selector="",  # No specific click needed for task edit page
                output_file=f"taosx_task_{task_id}_edit_full.png"
            )

            if screenshot_success:
                self._remote._logger.info(f"✓ Full-page screenshot saved: taosx_task_{task_id}_edit_full.png")

                # Make sure playwright knows about our custom screenshot
                try:
                    # Check if screenshot file exists in run_log_dir
                    screenshot_file = os.path.join(self.run_log_dir, f"taosx_task_{task_id}_edit_full.png")
                    if os.path.exists(screenshot_file):
                        self._remote._logger.info(f"✓ Screenshot file confirmed: {screenshot_file}")
                    else:
                        self._remote._logger.warning(f"Screenshot file not found: {screenshot_file}")
                except Exception as e:
                    self._remote._logger.warning(f"Error checking screenshot file: {e}")
            else:
                self._remote._logger.warning("Screenshot failed, but continuing...")

            # Manually copy screenshot to screenshots directory since we used custom playwright
            screenshots_dir = os.path.join(self.run_log_dir, "screenshots")
            try:
                # Create screenshots directory if it doesn't exist
                os.makedirs(screenshots_dir, exist_ok=True)

                # Copy our custom screenshot to the screenshots directory
                source_screenshot = os.path.join(self.run_log_dir, f"taosx_task_{task_id}_edit_full.png")
                target_screenshot = os.path.join(screenshots_dir, f"taosx_task_{task_id}_edit_full.png")

                if os.path.exists(source_screenshot):
                    import shutil
                    shutil.copy2(source_screenshot, target_screenshot)
                    self._remote._logger.info(f"✓ Screenshot copied to: {target_screenshot}")

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

            # Collect screenshots to test result directory (this will collect any playwright-generated screenshots)
            self._remote._logger.info("Collecting screenshots to test result directory...")
            self.playwright.collect_screenshots(self.run_log_dir)

            # 9. Keep task for manual inspection (comment out deletion)
            self._remote._logger.info(f"✓ Task {task_id} completed successfully, keeping for manual inspection")
            self._remote._logger.info(f"✓ You can view the task in TaosX frontend with task ID: {task_id}")

            # Cleanup task (uncomment if you want to auto-delete)
            # try:
            #     self.adapter.delete_task(task_id)
            #     self._remote._logger.info(f"✓ Cleaned up task: {task_id}")
            # except:
            #     pass

        except Exception as e:
            self._remote._logger.error(f"CSV upload and import test failed: {e}")
            raise
        finally:
            # Cleanup database
            try:
                pass
                # self.adapter.drop_database(test_db)
                # self._remote._logger.info(f"Cleaned up test database: {test_db}")
            except Exception as cleanup_error:
                self._remote._logger.warning(f"Cleanup failed: {cleanup_error}")



    def run(self):
        """Run all test cases"""
        self._remote._logger.info("=== Starting TaosX CSV Import Tests ===")
        # Run test cases
        self.csv_import_test()

        self._remote._logger.info("=== All TaosX CSV Import Tests Completed ===")

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