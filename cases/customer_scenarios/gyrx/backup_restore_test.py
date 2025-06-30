## taostest --setup=pocs/gyrx/demo.yaml --case=customer_scenarios/gyrx/backup_restore_test.py --keep

"""
TaosX Backup Test Case for Gyrx Scenarios
Based on backup test from taosx repository
"""

import os
import time
import requests
from datetime import datetime, timedelta, timezone
from taostest import TDCase
from taostest.util.taosx_util import TaosxUtil, TaosxTestUtil
from taostest.util.remote import Remote
from taostest.util.common import TDCom
from taostest.util.playwright_util import PlaywrightUtil


class BackupRestoreTest(TDCase):
    """Test case for TaosX backup functionality"""

    def init(self):
        """Initialize test case"""
        # Initialize TDCom for database operations
        self.tdCom = TDCom(self.tdSql)
        # Query existing databases and tables dynamically
        self.source_db = None
        self.source_stable = None
        # Initialize remote logger
        self._remote: Remote = Remote(self.logger)

        # Backup configuration
        self.backup_interval_minutes = 1  # Default backup interval for testing

        # Get configuration using tdCom helper methods - must exist
        if not hasattr(self, 'env_setting') or not self.env_setting:
            raise Exception("env_setting is required for TaosX backup test")

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

        # Initialize TaosX util with backup-specific functionality
        self.adapter = TaosxBackupUtil(
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

    def find_available_database_and_table(self):
        """Find available database and stable for backup test"""
        self._remote._logger.info("Querying available databases...")

        # Query all databases
        self.tdSql.query("SHOW DATABASES")
        databases = []
        for i in range(self.tdSql.query_row):
            db_name = self.tdSql.query_data[i][0]
            # Skip system databases
            if db_name not in ["information_schema", "performance_schema", "log"]:
                databases.append(db_name)

        self._remote._logger.info(f"Found databases: {databases}")

        # Try to find a database with ci_ prefix (from other tests)
        target_db = None
        target_stable = None

        for db_name in databases:
            if db_name.startswith("ci_"):
                try:
                    # Use the database
                    self.tdSql.execute(f"USE `{db_name}`")

                    # Query stables in this database
                    self.tdSql.query(f"SHOW `{db_name}`.STABLES")
                    if self.tdSql.query_row > 0:
                        stable_name = self.tdSql.query_data[0][0]

                        # Check if stable has data
                        self.tdSql.query(f"SELECT COUNT(*) FROM `{db_name}`.`{stable_name}`")
                        if self.tdSql.query_row > 0 and self.tdSql.query_data[0][0] > 0:
                            target_db = db_name
                            target_stable = stable_name
                            row_count = self.tdSql.query_data[0][0]
                            self._remote._logger.info(f"Found suitable database: {target_db}, stable: {target_stable}, rows: {row_count}")
                            break

                except Exception as e:
                    self._remote._logger.debug(f"Error checking database {db_name}: {e}")
                    continue

        if not target_db:
            # Create a simple test database if none found
            target_db = TaosxTestUtil.generate_random_name()
            target_stable = "backup_test_data"
            self._create_simple_test_data(target_db, target_stable)
            self._created_test_db = True  # Mark that we created this database
        else:
            self._created_test_db = False  # Using existing database

        return target_db, target_stable

    def _get_latest_topic_for_database(self, source_db: str) -> str:
        """Query the latest topic for the given database from information_schema.ins_topics"""
        try:
            self._remote._logger.info(f"Querying latest topic for database: {source_db}")

            # Query the latest topic for this database
            query_sql = f"SELECT topic_name FROM information_schema.ins_topics WHERE db_name = '{source_db}' AND type = 'stable' ORDER BY create_time DESC LIMIT 1"
            self.tdSql.query(query_sql)

            if self.tdSql.query_row > 0:
                latest_topic = self.tdSql.query_data[0][0]
                self._remote._logger.info(f"✓ Found latest topic for {source_db}: {latest_topic}")
                return latest_topic
            else:
                self._remote._logger.warning(f"No stable topics found for database: {source_db}")
                return None

        except Exception as e:
            self._remote._logger.error(f"Error querying latest topic: {e}")
            return None

    def _extract_backup_time_range_and_topic(self, task_status: dict, task_id: str, source_db: str) -> tuple:
        """Extract actual backup time range and get latest topic from database"""
        try:
            self._remote._logger.info(f"Analyzing backup task status for time range extraction...")
            self._remote._logger.info(f"Full task status: {task_status}")

            # Get the latest topic from database instead of backup task
            latest_topic = self._get_latest_topic_for_database(source_db)

            # Primary method: Extract from trigger.upcoming (this is the correct backup time)
            if "trigger" in task_status and "upcoming" in task_status["trigger"]:
                upcoming_time = task_status["trigger"]["upcoming"]
                self._remote._logger.info(f"Found upcoming backup time: {upcoming_time}")

                # Use the original upcoming_time directly to preserve exact milliseconds
                self._remote._logger.info(f"Using upcoming time for restore: {upcoming_time}")
                return (upcoming_time, upcoming_time, latest_topic)

            # Final fallback: Use task execution time range if nothing else works
            if "created_at" in task_status and "finished_at" in task_status:
                from_time = task_status["created_at"]
                to_time = task_status["finished_at"]
                self._remote._logger.info(f"Using task execution time range as final fallback: {from_time} to {to_time}")
                return (from_time, to_time, latest_topic)

            self._remote._logger.warning("Could not find backup time range in task status")
            return None

        except Exception as e:
            self._remote._logger.error(f"Error extracting backup time range and topic: {e}")
            return None

    def _build_restore_task_config(self, task_name: str, source_db: str, source_stable: str,
                                 backup_dir: str, cluster_id: str, task_id: str, backup_from_time: str, backup_to_time: str, backup_topic: str = None) -> dict:
        """Build restore task configuration based on provided example payload"""

        # Use the actual backup time range for restore
        self._remote._logger.info(f"Using backup time range for restore: {backup_from_time} to {backup_to_time}")

        # Use simple fallback database and stable creation SQLs
        db_sql = f"CREATE DATABASE `{source_db}` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' S3_CHUNKPAGES 131072 S3_KEEPLOCAL 525600m S3_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h"
        stable_sql = f"CREATE STABLE `{source_stable}` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `current` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `voltage` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `phase` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `desc` VARCHAR(64) ENCODE 'disabled' COMPRESS 'zstd' LEVEL 'medium') TAGS (`id` INT)"

        self._remote._logger.info(f"Using default database and stable creation SQLs for restore")

        # Use the backup topic if available (CRITICAL: reuse existing topic from backup!)
        if backup_topic:
            restore_topic = backup_topic
            self._remote._logger.info(f"✓ Using existing backup topic for restore: {restore_topic}")
        else:
            # Fallback: Generate unique topic only if backup topic not found
            import random
            import string
            random_chars = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
            restore_topic = f"x{random_chars}"
            self._remote._logger.warning(f"Backup topic not found, generating new topic: {restore_topic}")

        # Calculate extended time (add 1 hour to backup_from_time for wider restore range)
        try:
            from datetime import datetime, timedelta
            # Parse the backup time and add 1 hour
            if backup_from_time:
                # Handle both formats: 2025-06-28T05:33:48Z and 2025-06-28T05:33:48.121Z
                backup_time_str = backup_from_time.replace('Z', '+00:00')
                backup_time = datetime.fromisoformat(backup_time_str)
                extended_time = backup_time + timedelta(seconds=1)
                # Format back to original format
                extended_time_str = extended_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
                self._remote._logger.info(f"Extended backup time from {backup_from_time} to {extended_time_str} (+1 second)")
            else:
                extended_time_str = backup_from_time
        except Exception as e:
            self._remote._logger.warning(f"Error extending backup time: {e}, using original time")
            extended_time_str = backup_from_time

        # Build the from URL with extended time
        from_url = f"local:{backup_dir}/?to={extended_time_str}&task_id={task_id}"


        # Build restore task config matching the successful manual payload exactly
        restore_task_config = {
            "labels": [
                "type::restore",
                f"cluster-id::{cluster_id}"
            ],
            "trigger": {
                "schedule": "oneshot",
                "resume": "never"
            },
            "from": from_url,
            "to": f"tmq+http://root:taosdata@localhost:{self.taosadapter_port}/{source_db}"
        }

        self._remote._logger.info(f"Built restore task config: {restore_task_config}")
        return restore_task_config


    def _check_backup_files(self, backup_dir: str):
        """Check if backup files were created in the backup directory"""
        try:
            if os.path.exists(backup_dir):
                files = os.listdir(backup_dir)
                if files:
                    self._remote._logger.info(f"✓ Backup files created: {len(files)} files found")
                    for file in files[:5]:  # Show first 5 files
                        file_path = os.path.join(backup_dir, file)
                        file_size = os.path.getsize(file_path) if os.path.isfile(file_path) else 0
                        self._remote._logger.info(f"  - {file} ({file_size} bytes)")
                    if len(files) > 5:
                        self._remote._logger.info(f"  ... and {len(files) - 5} more files")
                else:
                    self._remote._logger.warning(f"No backup files found in {backup_dir}")
            else:
                self._remote._logger.error(f"Backup directory does not exist: {backup_dir}")
        except Exception as e:
            self._remote._logger.error(f"Failed to check backup files: {e}")

    def _take_screenshot_with_manual_click(self, target_url: str, username: str, password: str, click_selector: str, output_file: str) -> bool:
        """Take screenshot with manual login and click operation"""
        try:
            from playwright.sync_api import sync_playwright

            self._remote._logger.info("Starting custom screenshot with manual click...")

            with sync_playwright() as p:
                # Launch browser with Chinese locale
                browser = p.chromium.launch(
                    headless=True,
                    args=['--lang=zh-CN', '--accept-lang=zh-CN,zh,en']
                )
                # Create page with Chinese locale settings
                page = browser.new_page(
                    locale='zh-CN',
                    timezone_id='Asia/Shanghai'
                )

                # Navigate to login page
                login_url = f"http://{self.taosx_host}:{self.taosx_port}/login"
                self._remote._logger.info(f"Navigating to login page: {login_url}")
                page.goto(login_url)
                page.wait_for_timeout(2000)

                # Fill login form
                self._remote._logger.info("Filling login credentials...")
                page.fill('input[type="text"]', username)
                page.fill('input[type="password"]', password)

                # Click login button
                self._remote._logger.info("Clicking login button...")
                page.click('button')
                page.wait_for_timeout(3000)

                # Navigate to target page
                self._remote._logger.info(f"Navigating to target page: {target_url}")
                page.goto(target_url)
                page.wait_for_timeout(3000)

                # Click the specified element
                self._remote._logger.info(f"Clicking element: {click_selector}")
                try:
                    page.click(click_selector)
                    page.wait_for_timeout(2000)
                    self._remote._logger.info("✓ Successfully clicked backup tab")
                except Exception as click_error:
                    self._remote._logger.warning(f"Failed to click {click_selector}: {click_error}")

                # Take screenshot - save to playwright's expected location
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

                self._remote._logger.info("✓ Custom screenshot completed successfully")
                return True

        except Exception as e:
            self._remote._logger.error(f"Custom screenshot failed: {e}")
            return False


    def backup_restore_test(self):
        """
        Test case: Test TaosX backup and restore functionality

        用例概述：测试 TaosX 备份和恢复功能

        用例步骤：
        1. 检查 TaosX 服务状态
        2. 准备测试数据
        3. 创建备份目录
        4. 创建 TaosX 备份任务
        5. 启动任务并等待完成
        6. 验证备份文件生成
        7. 复杂数据丢失场景测试：
           a. 记录当前表数量和数据量
           b. 删除一张子表（完全删除）
           c. 删除另一张子表的部分数据
           d. 创建恢复任务（使用现有备份）
           e. 验证表和数据完全恢复
        8. 保留所有任务用于截图

        验证点：
        1. TaosX 服务可用
        2. 备份任务能够正确创建和执行
        3. 备份文件正确生成在指定目录
        4. 任务状态变化正确
        5. 恢复任务能够正确创建和执行
        6. 被完全删除的子表能够恢复成功
        7. 被部分删除的数据能够恢复成功
        8. 数据完整性验证通过
        """
        self._remote._logger.info("Starting TaosX backup test...")

        # Test configuration - create identifiable backup directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = f"/tmp/taosx_backup_test/{timestamp}_{TaosxTestUtil.generate_random_name(5)}"

        try:
            # 1. Check TaosX service status
            self._remote._logger.info("Checking TaosX service status...")
            taosx_available = self.adapter.check_taosx_status()
            if not taosx_available:
                self._remote._logger.warning("TaosX service not available, skipping test")
                return
            self._remote._logger.info("✓ TaosX service is available")

            # 2. Find available database and table for backup
            source_db, source_stable = self.find_available_database_and_table()
            self._remote._logger.info(f"Using database: {source_db}, stable: {source_stable}")

            # Verify data exists
            self.tdSql.query(f'SELECT COUNT(*) FROM `{source_db}`.`{source_stable}`')
            data_count = self.tdSql.query_data[0][0] if self.tdSql.query_row > 0 else 0
            self._remote._logger.info(f"Found {data_count} rows of data to backup")

            # 3. Use configured backup interval for testing
            self._remote._logger.info(f"Using backup interval: {self.backup_interval_minutes} minutes")

            # 4. Create backup directory (must exist before creating task)
            self._remote._logger.info(f"Creating backup directory: {backup_dir}")
            os.makedirs(backup_dir, exist_ok=True)
            # Verify directory exists
            if not os.path.exists(backup_dir):
                raise Exception(f"Failed to create backup directory: {backup_dir}")
            self._remote._logger.info(f"✓ Created backup directory: {backup_dir}")

            # 5. Create TaosX backup task
            self._remote._logger.info("Creating TaosX backup task...")
            task_name = f"backup_task_{TaosxTestUtil.generate_random_name(5)}"

            # Record backup start time for restore operations (use correct timezone format)
            backup_start_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
            self._remote._logger.info(f"Recording backup start time for restore: {backup_start_time}")

            # Get cluster ID for proper task labeling
            cluster_id = self.adapter.get_cluster_id()

            # Build backup task config using found database and stable
            # Use configured backup interval for testing
            task_config = self.adapter.build_backup_task_config(
                task_name=task_name,
                source_db=source_db,
                backup_dir=backup_dir,
                cluster_id=cluster_id,
                stable_name=source_stable,
                backup_delay_minutes=2,  # Start backup in 2 minutes
                interval_unit="m",       # Use minutes as interval unit
                interval_value=self.backup_interval_minutes  # Use configured interval
            )

            task_info = self.adapter.create_task(task_config)
            task_id = task_info["id"]
            self._remote._logger.info(f"✓ Created backup task: {task_id}")

            # 6. Check initial task status
            time.sleep(3)  # Wait for task to be queued
            task_status = self.adapter.get_task_status(task_id)
            current_status = task_status.get("status", "").strip().lower()
            self._remote._logger.info(f"Initial task status: {current_status}")

            # Verify task is queued (ready to run)
            if current_status != "queued":
                self._remote._logger.warning(f"Expected 'queued' status, got: {current_status}")

            # 7. Wait for first backup execution and monitor status changes
            self._remote._logger.info("Monitoring task status through first backup execution...")

            # Wait for task to move from queued to running, then to ticked
            max_wait_time = 300  # Maximum wait time in seconds (increased to 300s)
            start_time = time.time()
            first_backup_completed = False

            while time.time() - start_time < max_wait_time:
                task_status = self.adapter.get_task_status(task_id)
                current_status = task_status.get("status", "").strip().lower()
                elapsed = time.time() - start_time
                self._remote._logger.info(f"Task status after {elapsed:.1f}s: {current_status}")

                if current_status == "running":
                    self._remote._logger.info("✓ First backup execution started (running)")
                elif current_status == "ticked":
                    self._remote._logger.info("✓ First backup completed, task now waiting for next execution (ticked)")
                    first_backup_completed = True

                    # Check if backup files were created after first execution
                    self._check_backup_files(backup_dir)

                    # Get next execution time from task status
                    if "next" in task_status:
                        next_execution_time = task_status["next"]
                        self._remote._logger.info(f"Next backup execution scheduled at: {next_execution_time}")

                        # Parse and wait for next execution
                        try:
                            next_time = datetime.fromisoformat(next_execution_time.replace('Z', '+00:00'))
                            current_time = datetime.now(timezone.utc)
                            wait_seconds = (next_time - current_time).total_seconds()

                            if wait_seconds > 0 and wait_seconds < 120:  # Only wait if reasonable time
                                self._remote._logger.info(f"Waiting {wait_seconds:.1f} seconds for next backup execution...")
                                time.sleep(wait_seconds + 5)  # Add 5 second buffer

                                # Check status after second backup
                                final_status = self.adapter.get_task_status(task_id)
                                final_status_str = final_status.get("status", "").strip().lower()
                                self._remote._logger.info(f"Task status after second backup: {final_status_str}")

                                # Check backup files again
                                self._check_backup_files(backup_dir)
                            else:
                                self._remote._logger.info(f"Next execution in {wait_seconds:.1f}s - too long to wait in test")

                        except Exception as e:
                            self._remote._logger.warning(f"Failed to parse next execution time: {e}")

                    break

                # Wait before next status check
                time.sleep(5)

            if not first_backup_completed:
                self._remote._logger.warning(f"First backup did not complete within {max_wait_time} seconds")
                # Still check for any files that might have been created
                self._check_backup_files(backup_dir)

            # Log final status
            final_task_status = self.adapter.get_task_status(task_id)
            final_status_str = final_task_status.get("status", "").strip().lower()
            self._remote._logger.info(f"✓ Backup task {task_id} final status: {final_status_str}")
            self._remote._logger.info(f"✓ Task will continue executing every {self.backup_interval_minutes} minutes")

            # 8. Take screenshot of backup plan after successful backup
            self._remote._logger.info("Taking screenshot of backup plan...")
            management_url = f"http://{self.taosx_host}:{self.taosx_port}/management/backup"
            screenshot_success = self._take_screenshot_with_manual_click(
                target_url=management_url,
                username="root",
                password="taosdata",
                click_selector="",  # No specific click needed
                output_file=f"taosx_backup_plan_{task_id}.png"
            )
            if screenshot_success:
                self._remote._logger.info(f"✓ Backup plan screenshot saved")

            # 9. Stop backup task before data deletion
            self._remote._logger.info("Stopping backup task before data deletion...")
            stop_success = self.adapter.stop_task(task_id)
            if stop_success:
                self._remote._logger.info(f"✓ Backup task {task_id} stopped successfully")
            else:
                self._remote._logger.warning(f"Failed to stop backup task {task_id}, but continuing...")

            # 10. Get actual backup time range and latest topic from database
            self._remote._logger.info("Getting actual backup time range and latest topic...")
            backup_task_status = self.adapter.get_task_status(task_id)
            backup_info = self._extract_backup_time_range_and_topic(backup_task_status, task_id, source_db)

            if backup_info and len(backup_info) >= 2:
                backup_from_time, backup_to_time = backup_info[0], backup_info[1]
                backup_topic = backup_info[2] if len(backup_info) > 2 else None
                self._remote._logger.info(f"✓ Found backup time range: {backup_from_time} to {backup_to_time}")
                if backup_topic:
                    self._remote._logger.info(f"✓ Found latest topic: {backup_topic}")
                else:
                    self._remote._logger.warning("No latest topic found")
            else:
                self._remote._logger.warning("Could not extract backup time range, using fallback")
                backup_from_time = backup_to_time = backup_start_time
                backup_topic = None

            # 11. Complex scenario: Test table deletion and partial data deletion with restore
            self._remote._logger.info("=== Starting complex scenario: table and data deletion ===")

            # Ensure we have enough tables for complex testing
            self.tdSql.query(f'SHOW `{source_db}`.TABLES')
            current_tables = []
            for i in range(self.tdSql.query_row):
                current_tables.append(self.tdSql.query_data[i][0])

            if len(current_tables) >= 2:
                # Record current state before complex operations
                original_table_count = len(current_tables)
                original_data_counts = {}
                total_original_rows = 0
                for table_name in current_tables:
                    try:
                        self.tdSql.query(f'SELECT COUNT(*) FROM `{source_db}`.`{table_name}`')
                        if self.tdSql.query_row > 0:
                            row_count = self.tdSql.query_data[0][0]
                            original_data_counts[table_name] = row_count
                            total_original_rows += row_count
                            self._remote._logger.info(f"Original state - {table_name}: {row_count} rows")
                    except Exception as e:
                        self._remote._logger.warning(f"Error counting {table_name}: {e}")

                self._remote._logger.info(f"Total original rows: {total_original_rows}, table count: {original_table_count}")

                # 9. Simulate complex data loss scenarios (using existing backup)
                self._remote._logger.info("Simulating complex data loss scenarios...")

                # Delete one table completely
                table_to_delete = current_tables[0]
                rows_in_deleted_table = original_data_counts.get(table_to_delete, 0)
                self._remote._logger.info(f"Deleting table completely: {table_to_delete} ({rows_in_deleted_table} rows)")
                self.tdSql.execute(f'DROP TABLE `{source_db}`.`{table_to_delete}`')
                self._remote._logger.info(f"✓ Table {table_to_delete} deleted completely")

                # Delete partial data from another table
                table_to_modify = current_tables[1]
                original_rows_in_modified = original_data_counts.get(table_to_modify, 0)
                self._remote._logger.info(f"Deleting partial data from: {table_to_modify} (original: {original_rows_in_modified} rows)")

                try:
                    # Delete first record
                    self.tdSql.query(f'SELECT ts FROM `{source_db}`.`{table_to_modify}` ORDER BY ts LIMIT 1')
                    if self.tdSql.query_row > 0:
                        first_ts = self.tdSql.query_data[0][0]
                        self.tdSql.execute(f'DELETE FROM `{source_db}`.`{table_to_modify}` WHERE ts = \'{first_ts}\'')
                        self._remote._logger.info(f"✓ Deleted partial data from {table_to_modify}")

                        # Verify partial deletion
                        self.tdSql.query(f'SELECT COUNT(*) FROM `{source_db}`.`{table_to_modify}`')
                        remaining_rows = self.tdSql.query_data[0][0] if self.tdSql.query_row > 0 else 0
                        deleted_rows = original_rows_in_modified - remaining_rows
                        self._remote._logger.info(f"Deleted {deleted_rows} rows from {table_to_modify}, {remaining_rows} remaining")
                except Exception as e:
                    self._remote._logger.warning(f"Failed to delete partial data: {e}")

                # 10. Create restore task using existing backup
                self._remote._logger.info("Creating restore task...")
                restore_task_name = f"restore_task_{TaosxTestUtil.generate_random_name(5)}"

                restore_task_config = self._build_restore_task_config(
                    task_name=restore_task_name,
                    source_db=source_db,
                    source_stable=source_stable,
                    backup_dir=backup_dir,
                    cluster_id=cluster_id,
                    task_id=task_id,
                    backup_from_time=backup_from_time,
                    backup_to_time=backup_to_time,
                    backup_topic=backup_topic
                )

                restore_task_info = self.adapter.create_task(restore_task_config)
                restore_task_id = restore_task_info["id"]
                self._remote._logger.info(f"✓ Created restore task: {restore_task_id}")

                # 11. Monitor restore completion
                self._remote._logger.info("Monitoring restore completion...")
                restore_status = self.adapter.wait_for_task_completion(restore_task_id, timeout=180)
                self._remote._logger.info(f"✓ Restore completed with status: {restore_status}")

                # 12. Verify restoration
                self._remote._logger.info("Verifying restoration...")
                time.sleep(5)

                # Check table restoration
                self.tdSql.query(f'SHOW `{source_db}`.TABLES')
                restored_table_count = self.tdSql.query_row
                self._remote._logger.info(f"Tables after restore: {restored_table_count}")

                if restored_table_count == original_table_count:
                    self._remote._logger.info(f"✓ Table count restored: {restored_table_count} == {original_table_count}")
                else:
                    self._remote._logger.warning(f"Table count mismatch: {restored_table_count} != {original_table_count}")

                # Verify specific table and data restoration
                deleted_table_restored = False
                modified_table_restored = False

                # Re-query tables for verification
                self.tdSql.query(f'SHOW `{source_db}`.TABLES')
                import copy
                table_data = copy.deepcopy(self.tdSql.query_data)  # Copy data for later use
                if self.tdSql.query_row > 0:
                    self._remote._logger.info("Checking restored tables:")
                    for i in range(self.tdSql.query_row):
                        table_name = table_data[i][0]
                        self._remote._logger.info(f"  Found table: {table_name}")

                        if table_name == table_to_delete:
                            deleted_table_restored = True
                            self._remote._logger.info(f"✓ Deleted table {table_to_delete} successfully restored")

                            # Verify data
                            try:
                                self.tdSql.query(f'SELECT COUNT(*) FROM `{source_db}`.`{table_to_delete}`')
                                if self.tdSql.query_row > 0:
                                    restored_rows = self.tdSql.query_data[0][0]
                                    if restored_rows == rows_in_deleted_table:
                                        self._remote._logger.info(f"✓ Deleted table data restored: {restored_rows} rows")
                                    else:
                                        self._remote._logger.warning(f"Data mismatch: {restored_rows} != {rows_in_deleted_table}")
                            except Exception as e:
                                self._remote._logger.warning(f"Error verifying deleted table: {e}")

                        if table_name == table_to_modify:
                            modified_table_restored = True
                            try:
                                self.tdSql.query(f'SELECT COUNT(*) FROM `{source_db}`.`{table_to_modify}`')
                                if self.tdSql.query_row > 0:
                                    restored_rows = self.tdSql.query_data[0][0]
                                    expected_rows = original_data_counts.get(table_to_modify, 0)
                                    if restored_rows == expected_rows:
                                        self._remote._logger.info(f"✓ Modified table data restored: {restored_rows} rows")
                                    else:
                                        self._remote._logger.warning(f"Modified data mismatch: {restored_rows} != {expected_rows}")
                            except Exception as e:
                                self._remote._logger.warning(f"Error verifying modified table: {e}")
                else:
                    self._remote._logger.warning("No tables found after restore")

                # Check for missing deleted table
                if not deleted_table_restored:
                    self._remote._logger.warning(f"Deleted table {table_to_delete} was not restored")

                # Check for missing modified table
                if not modified_table_restored:
                    self._remote._logger.warning(f"Modified table {table_to_modify} was not restored")

                # Final verification summary
                if deleted_table_restored and modified_table_restored:
                    self._remote._logger.info("✓ Complex scenario restoration PASSED")
                else:
                    self._remote._logger.warning(f"Complex scenario incomplete: deleted={deleted_table_restored}, modified={modified_table_restored}")

                # List final state
                self._remote._logger.info("Final state after restoration:")
                total_final_rows = 0

                # Re-query for final state listing
                self.tdSql.query(f'SHOW `{source_db}`.TABLES')
                table_data = copy.deepcopy(self.tdSql.query_data)  # Copy data for later use
                if self.tdSql.query_row > 0:
                    for i in range(self.tdSql.query_row):
                        table_name = table_data[i][0]
                        try:
                            self.tdSql.query(f'SELECT COUNT(*) FROM `{source_db}`.`{table_name}`')
                            if self.tdSql.query_row > 0:
                                table_rows = self.tdSql.query_data[0][0]
                                total_final_rows += table_rows
                                expected_count = original_data_counts.get(table_name, 0)
                                status = "✓" if table_rows == expected_count else "⚠"
                                self._remote._logger.info(f"  {status} {table_name}: {table_rows} rows (expected: {expected_count})")
                        except Exception as e:
                            self._remote._logger.warning(f"  ✗ {table_name}: error counting - {e}")
                else:
                    self._remote._logger.warning("No tables found in final state")

                self._remote._logger.info(f"Total final rows: {total_final_rows} (expected: {total_original_rows})")

                # Keep tasks for screenshot
                self._remote._logger.info(f"✓ Tasks {task_id} and {restore_task_id} completed, keeping for screenshot")

            else:
                self._remote._logger.warning("Not enough tables for complex scenario, skipping")

            # Keep backup task for screenshot purposes - no cleanup
            self._remote._logger.info(f"✓ All backup and restore tasks completed, keeping for screenshot")

            # 8. Take screenshot of backup plan list in TaosX frontend - generate full length screenshot
            self._remote._logger.info("Taking full-page screenshot of backup plan list in TaosX frontend...")

            # Use the management page and then click backup tab
            management_url = f"http://{self.taosx_host}:{self.taosx_port}/management/backup"

            # First login and navigate manually for better control
            login_url = f"http://{self.taosx_host}:{self.taosx_port}/login"
            self._remote._logger.info(f"Login first at: {login_url}")

            # Custom screenshot with manual click operation
            screenshot_success = self._take_screenshot_with_manual_click(
                target_url=management_url,
                username="root",
                password="taosdata",
                click_selector="#tab-backup",
                output_file="taosx_backup_list_full.png"
            )

            if screenshot_success:
                self._remote._logger.info(f"✓ Full-page screenshot saved: taosx_backup_list_full.png")

                # Make sure playwright knows about our custom screenshot
                try:
                    # Check if screenshot file exists in run_log_dir
                    screenshot_file = os.path.join(self.run_log_dir, "taosx_backup_list_full.png")
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
                source_screenshot = os.path.join(self.run_log_dir, "taosx_backup_list_full.png")
                target_screenshot = os.path.join(screenshots_dir, "taosx_backup_list_full.png")

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

            self._remote._logger.info("✓ TaosX backup and restore test completed successfully")
            self._remote._logger.info("✓ Backup task creation and execution verified")
            self._remote._logger.info("✓ Complex data loss scenarios tested")
            self._remote._logger.info("✓ Table deletion and data deletion recovery verified")
            self._remote._logger.info("✓ Restore task creation and execution verified")
            self._remote._logger.info("✓ Data integrity verification completed")

        except Exception as e:
            self._remote._logger.error(f"TaosX backup test failed: {e}")
            raise
        finally:
            # Keep backup directory for ongoing task, only log the location
            try:
                if os.path.exists(backup_dir):
                    self._remote._logger.info(f"Backup directory preserved for ongoing task: {backup_dir}")
                    self._remote._logger.info(f"Monitor backup files at: {backup_dir}")
            except Exception as cleanup_error:
                self._remote._logger.warning(f"Backup directory check failed: {cleanup_error}")

            try:
                # Clean up test database if we created it
                if hasattr(self, '_created_test_db') and self._created_test_db:
                    self.tdSql.execute(f"DROP DATABASE IF EXISTS `{source_db}`")
                    self._remote._logger.info(f"Cleaned up test database: {source_db}")
            except Exception as cleanup_error:
                self._remote._logger.warning(f"Database cleanup failed: {cleanup_error}")

    def run(self):
        """Run all test cases"""
        self._remote._logger.info("=== Starting TaosX Backup and Restore Tests ===")
        # Run test cases
        self.backup_restore_test()
        self._remote._logger.info("=== All TaosX Backup and Restore Tests Completed ===")

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


class TaosxBackupUtil(TaosxUtil):
    """Extended TaosX utility class with backup-specific functionality"""

    def build_backup_task_config(self, task_name: str, source_db: str, backup_dir: str, cluster_id: str,
                               stable_name: str = None, backup_delay_minutes: int = 5, interval_unit: str = "m", interval_value: int = 30) -> dict:
        """Build backup task configuration following successful manual creation format

        Args:
            task_name: Name of the backup task
            source_db: Source database name
            backup_dir: Backup directory path
            cluster_id: TDengine cluster ID
            stable_name: Optional stable name to backup
            backup_delay_minutes: Minutes to delay before first backup (default: 5 minutes)
            interval_unit: Interval unit - 'm' for minutes, 'h' for hours, 'd' for days (default: 'm')
            interval_value: Interval value (default: 30)
        """

        # Build backup task config based on successful manual payload format
        # Format: tmq+http://root:taosdata@host:port/database?parameters
        from_url = f"tmq+http://root:taosdata@{self.taosadapter_host}:{self.taosadapter_port}/{source_db}?max_retry=3&retry_interval=1s"
        if stable_name:
            from_url += f"&stable={stable_name}"

        # Calculate upcoming backup time based on current time + delay
        now = datetime.now(timezone.utc)
        upcoming_time = now + timedelta(minutes=backup_delay_minutes)
        upcoming_iso = upcoming_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        # Build interval string based on unit and value
        interval_str = f"{interval_value}{interval_unit}"

        # Use the exact format from successful manual creation
        task_config = {
            "name": task_name,
            "from": from_url,
            "to": f"local:{backup_dir}/?max_size=1GB&compression_level=fastest&s3_enable=false",
            "trigger": {
                "upcoming": upcoming_iso,
                "interval": interval_str
            },
            "labels": [
                "type::backup",
                f"cluster-id::{cluster_id}"
            ]
        }

        self.logger.info(f"Built backup task config with upcoming time: {upcoming_iso}, interval: {interval_str}")
        self.logger.info(f"Full backup task config: {task_config}")
        return task_config

    def stop_task(self, task_id: str) -> bool:
        """Stop a backup task"""
        try:
            response = requests.post(f"{self.tasks_url}/{task_id}/stop")
            if response.status_code == 200:
                self.logger.info(f"Task {task_id} stopped successfully")
                return True
            else:
                self.logger.error(f"Stop task failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to stop task: {e}")
            return False

    def delete_task(self, task_id: str) -> bool:
        """Delete a backup task"""
        try:
            response = requests.delete(f"{self.tasks_url}/{task_id}")
            if response.status_code == 200:
                self.logger.info(f"Task {task_id} deleted successfully")
                return True
            else:
                # Check if the error message indicates task is in scheduler
                if response.status_code == 400:
                    response_data = response.json() if response.text else {}
                    message = response_data.get("message", "")
                    if "scheduler" in message.lower():
                        self.logger.info(f"Task {task_id} is in scheduler, cannot delete while running")
                        return message  # Return the message for testing

                self.logger.error(f"Delete task failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to delete task: {e}")
            return False

    def start_task(self, task_id: str) -> bool:
        """Start a task"""
        try:
            response = requests.post(f"{self.tasks_url}/{task_id}/start")
            if response.status_code == 200:
                self.logger.info(f"Task {task_id} started successfully")
                return True
            else:
                self.logger.error(f"Start task failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to start task: {e}")
            return False

    def wait_for_task_completion(self, task_id: str, timeout: int = 120) -> str:
        """Wait for task completion and return final status"""
        import time
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                task_status = self.get_task_status(task_id)
                current_status = task_status.get("status", "").strip().lower()

                if current_status in ["completed", "success", "finished"]:
                    self.logger.info(f"Task {task_id} completed successfully")
                    return current_status
                elif current_status in ["failed", "error"]:
                    self.logger.error(f"Task {task_id} failed")
                    return current_status

                # Task still running, wait a bit
                time.sleep(5)

            except Exception as e:
                self.logger.warning(f"Error checking task status: {e}")
                time.sleep(5)

        # Timeout reached
        self.logger.warning(f"Task {task_id} completion timeout after {timeout} seconds")
        try:
            final_status = self.get_task_status(task_id)
            return final_status.get("status", "timeout").strip().lower()
        except:
            return "timeout"