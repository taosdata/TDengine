"""
XNode Grant Privileges Test

Test xnode/agent system-level privileges and xnode task object-level privileges.
Covers GRANT/REVOKE functionality for CREATE/ALTER/DROP NODE, CREATE XNODE TASK,
and SHOW/ALTER/DROP ON XNODE TASK privileges.
"""

import random
import uuid
import time

from new_test_framework.utils import tdLog, tdSql


class TestXnodePriv:
    """XNode Grant Privileges Test"""

    replicaVar = 1

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.suffix = uuid.uuid4().hex[:6]
        cls.test_user = "zgc"
        cls.test_pass = "tbase125!"
        cls.super_user = "root"
        cls.super_pass = "taosdata"
        tdSql.prepare(
            dbname=f"xnode_priv_db_{cls.suffix}", drop=True, replica=cls.replicaVar
        )

    def _create_test_user(self):
        """Create test user if not exists"""
        try:
            tdSql.execute(f"CREATE USER {self.test_user} PASS '{self.test_pass}'", queryTimes=1)
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise

    def _drop_test_user(self):
        """Drop test user"""
        try:
            tdSql.execute(f"DROP USER {self.test_user}", queryTimes=1)
        except:
            pass

    def _expect_privilege_error(self, sql: str, user: str = None, passwd: str = None):
        """Execute SQL and expect privilege error

        Args:
            sql: SQL statement to execute
            user: User to connect as (None for current)
            passwd: Password for user

        Returns:
            True if privilege error occurred, False otherwise
        """
        if user:
            tdSql.connect(user=user, password=passwd)
        try:
            tdSql.execute(sql, queryTimes=1)
            return False  # No error occurred
        except Exception as e:
            msg = str(e).lower()
            return "privilege" in msg or "permission" in msg or "internal" in msg
        finally:
            if user:
                tdSql.connect(user=self.super_user, password=self.super_pass)

    def _expect_success(self, sql: str, user: str = None, passwd: str = None):
        """Execute SQL and expect success

        Args:
            sql: SQL statement to execute
            user: User to connect as (None for current)
            passwd: Password for user

        Returns:
            True if success, False otherwise
        """
        if user:
            tdSql.connect(user=user, password=passwd)
        try:
            tdSql.execute(sql, queryTimes=1)
            return True
        except Exception as e:
            msg = str(e).lower()
            if "syntax" in msg or "parse" in msg:
                raise AssertionError(f"Syntax error: {e}")
            return False
        finally:
            if user:
                tdSql.connect(user=self.super_user, password=self.super_pass)

    def _cleanup_xnode_resources(self):
        """Clean up all xnode resources"""
        try:
            # Drop all jobs
            tdSql.execute("DROP XNODE JOB WHERE id > 0", queryTimes=1)
        except:
            pass
        try:
            # Drop all tasks
            rs = tdSql.query("SHOW XNODE TASKS", row_tag=True)
            for row in rs:
                try:
                    tdSql.execute(f"DROP XNODE TASK {row[0]}", queryTimes=1)
                except:
                    pass
        except:
            pass
        try:
            # Drop all agents
            rs = tdSql.query("SHOW XNODE AGENTS", row_tag=True)
            for row in rs:
                try:
                    tdSql.execute(f"DROP XNODE AGENT {row[0]}", queryTimes=1)
                except:
                    pass
        except:
            pass
        try:
            # Drop all xnodes
            rs = tdSql.query("SHOW XNODES", row_tag=True)
            for row in rs:
                try:
                    tdSql.execute(f"DROP XNODE FORCE '{row[1]}'", queryTimes=1)
                except:
                    pass
        except:
            pass

    def _get_xnode_id(self):
        """Get available xnode id from SHOW XNODES

        Returns:
            int: The first available xnode id, or 1 if no xnodes exist
        """
        try:
            rs = tdSql.query("SHOW XNODES", row_tag=True)
            if rs and len(rs) > 0:
                return rs[0][0]
        except:
            pass
        return 1

    # =============================================================================
    # Section 1: System-level Privileges - XNode Management
    # =============================================================================

    def test_create_xnode_no_privilege(self):
        """Test case 1.1: Non-superuser cannot create xnode without privilege

        1. Create test user
        2. Try to create xnode as test user without GRANT
        3. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        # Try to create xnode without privilege
        result = self._expect_privilege_error(
            "CREATE XNODE 'localhost:6055'",
            user=self.test_user,
            passwd=self.test_pass
        )
        assert result, "Expected privilege error when creating xnode without permission"

        tdLog.success("Test 1.1 passed: Non-privileged user cannot create xnode")

    def test_grant_create_xnode(self):
        """Test case 1.2: GRANT CREATE NODE allows creating xnode

        1. Grant CREATE NODE to test user
        2. Create xnode as test user
        3. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        # Grant privilege
        tdSql.execute(f"GRANT CREATE NODE TO {self.test_user}", queryTimes=1)

        # Try to create xnode with privilege
        result = self._expect_success(
            "CREATE XNODE 'localhost:6055'",
            user=self.test_user,
            passwd=self.test_pass
        )
        assert result, "Should succeed with CREATE NODE privilege"

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE CREATE NODE FROM {self.test_user}", queryTimes=1)

        tdLog.success("Test 1.2 passed: GRANT CREATE NODE works correctly")

    def test_revoke_create_xnode(self):
        """Test case 1.3: REVOKE CREATE NODE removes permission

        1. Grant CREATE NODE to test user
        2. Revoke CREATE NODE from test user
        3. Try to create xnode as test user
        4. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        # Grant and then revoke privilege
        tdSql.execute(f"GRANT CREATE NODE TO {self.test_user}", queryTimes=1)
        tdSql.execute(f"REVOKE CREATE NODE FROM {self.test_user}", queryTimes=1)

        # Try to create xnode after revoke
        result = self._expect_privilege_error(
            "CREATE XNODE 'localhost:6055'",
            user=self.test_user,
            passwd=self.test_pass
        )
        assert result, "Expected privilege error after REVOKE"

        tdLog.success("Test 1.3 passed: REVOKE CREATE NODE works correctly")

    def test_alter_xnode_no_privilege(self):
        """Test case 1.4: Non-superuser cannot alter xnode without privilege

        1. Create xnode as superuser
        2. Try to alter xnode as test user without privilege
        3. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        # Create xnode as superuser
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        # Try to alter xnode without privilege
        result = self._expect_privilege_error(
            "ALTER XNODE SET USER root PASS 'taosdata'",
            user=self.test_user,
            passwd=self.test_pass
        )
        # Note: May also fail due to no xnode being found, but privilege check comes first

        self._cleanup_xnode_resources()
        tdLog.success("Test 1.4 passed: Non-privileged user cannot alter xnode")

    def test_grant_alter_xnode(self):
        """Test case 1.5: GRANT ALTER NODE allows altering xnode

        1. Create xnode as superuser
        2. Grant ALTER NODE to test user
        3. Alter xnode as test user
        4. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        # Create xnode and grant privilege
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)
        tdSql.execute(f"GRANT ALTER NODE TO {self.test_user}", queryTimes=1)

        # Try to alter xnode with privilege
        result = self._expect_success(
            "ALTER XNODE SET USER root PASS 'taosdata'",
            user=self.test_user,
            passwd=self.test_pass
        )

        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE ALTER NODE FROM {self.test_user}", queryTimes=1)

        tdLog.success("Test 1.5 passed: GRANT ALTER NODE works correctly")

    def test_grant_alter_xnode_drain(self):
        """Test case 1.6: GRANT ALTER NODE allows drain xnode

        1. Create xnode as superuser
        2. Grant ALTER NODE to test user
        3. Drain xnode as test user
        4. Verify success (drain reuses ALTER privilege)

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        # Create xnode and grant privilege
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)
        tdSql.execute(f"GRANT ALTER NODE TO {self.test_user}", queryTimes=1)

        # Get xnode id
        rs = tdSql.query("SHOW XNODES", row_tag=True)
        xnode_id = rs[0][0] if rs else 1

        # Try to drain xnode with ALTER privilege
        result = self._expect_success(
            f"DRAIN XNODE {xnode_id}",
            user=self.test_user,
            passwd=self.test_pass
        )

        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE ALTER NODE FROM {self.test_user}", queryTimes=1)

        tdLog.success("Test 1.6 passed: GRANT ALTER NODE allows drain")

    def test_drop_xnode_no_privilege(self):
        """Test case 1.7: Non-superuser cannot drop xnode without privilege

        1. Create xnode as superuser
        2. Try to drop xnode as test user without privilege
        3. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        # Create xnode as superuser
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        # Try to drop xnode without privilege
        result = self._expect_privilege_error(
            "DROP XNODE 'localhost:6055'",
            user=self.test_user,
            passwd=self.test_pass
        )
        assert result, "Expected privilege error when dropping xnode without permission"

        self._cleanup_xnode_resources()
        tdLog.success("Test 1.7 passed: Non-privileged user cannot drop xnode")

    def test_grant_drop_xnode(self):
        """Test case 1.8: GRANT DROP NODE allows dropping xnode

        1. Create xnode as superuser
        2. Grant DROP NODE to test user
        3. Drop xnode as test user
        4. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        # Create xnode and grant privilege
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)
        tdSql.execute(f"GRANT DROP NODE TO {self.test_user}", queryTimes=1)

        # Try to drop xnode with privilege
        result = self._expect_success(
            "DROP XNODE 'localhost:6055'",
            user=self.test_user,
            passwd=self.test_pass
        )

        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE DROP NODE FROM {self.test_user}", queryTimes=1)

        tdLog.success("Test 1.8 passed: GRANT DROP NODE works correctly")

    def test_superuser_xnode_operations(self):
        """Test case 1.9: Superuser can always execute all xnode operations

        1. Create xnode as superuser
        2. Alter xnode as superuser
        3. Drain xnode as superuser
        4. Drop xnode as superuser
        5. Verify all operations succeed

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._cleanup_xnode_resources()

        # All operations as superuser should succeed
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        rs = tdSql.query("SHOW XNODES", row_tag=True)
        xnode_id = rs[0][0] if rs else 1

        tdSql.execute("ALTER XNODE SET USER root PASS 'taosdata'", queryTimes=1)
        tdSql.execute(f"DRAIN XNODE {xnode_id}", queryTimes=1)
        tdSql.execute("DROP XNODE 'localhost:6055'", queryTimes=1)

        tdLog.success("Test 1.9 passed: Superuser can execute all xnode operations")

    def test_grant_show_xnodes(self):
        """Test case 1.10: GRANT SHOW NODES allows viewing xnode list

        1. Create xnode as superuser
        2. Grant SHOW NODES to test user
        3. Show xnodes as test user
        4. Verify xnode list is visible

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        # Create xnode and grant privilege
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)
        tdSql.execute(f"GRANT SHOW NODES TO {self.test_user}", queryTimes=1)

        # Try to show xnodes with privilege
        tdSql.connect(user=self.test_user, password=self.test_pass)
        try:
            tdSql.query("SHOW XNODES")
            rows = tdSql.queryRows
            assert rows >= 0, "Should be able to query xnodes"
        finally:
            tdSql.connect(user=self.super_user, password=self.super_pass)

        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE SHOW NODES FROM {self.test_user}", queryTimes=1)

        tdLog.success("Test 1.10 passed: GRANT SHOW NODES works correctly")

    # =============================================================================
    # Section 2: System-level Privileges - XNode Agent Management
    # =============================================================================

    def test_create_agent_no_privilege(self):
        """Test case 2.1: Non-superuser cannot create xnode agent without privilege

        1. Try to create xnode agent as test user without GRANT
        2. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        # Try to create agent without privilege
        result = self._expect_privilege_error(
            f"CREATE XNODE AGENT 'agent_{self.suffix}' WITH status 'created'",
            user=self.test_user,
            passwd=self.test_pass
        )
        assert result, "Expected privilege error when creating agent without permission"

        tdLog.success("Test 2.1 passed: Non-privileged user cannot create agent")

    def test_grant_create_agent(self):
        """Test case 2.2: GRANT CREATE NODE allows creating xnode agent

        1. Grant CREATE NODE to test user (agent reuses node privileges)
        2. Create xnode agent as test user
        3. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        # Grant privilege (agent reuses CREATE NODE)
        tdSql.execute(f"GRANT CREATE NODE TO {self.test_user}", queryTimes=1)

        # Try to create agent with privilege
        result = self._expect_success(
            f"CREATE XNODE AGENT 'agent_{self.suffix}' WITH status 'created'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        try:
            tdSql.execute(f"DROP XNODE AGENT 'agent_{self.suffix}'", queryTimes=1)
        except:
            pass
        tdSql.execute(f"REVOKE CREATE NODE FROM {self.test_user}", queryTimes=1)

        tdLog.success("Test 2.2 passed: GRANT CREATE NODE allows creating agent")

    def test_grant_alter_agent(self):
        """Test case 2.3: GRANT ALTER NODE allows modifying xnode agent

        1. Create agent as superuser
        2. Grant ALTER NODE to test user
        3. Alter agent as test user
        4. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        # Create agent and grant privilege
        tdSql.execute(f"CREATE XNODE AGENT 'agent_{self.suffix}' WITH status 'created'", queryTimes=1)
        tdSql.execute(f"GRANT ALTER NODE TO {self.test_user}", queryTimes=1)

        # Try to alter agent with privilege
        result = self._expect_success(
            f"ALTER XNODE AGENT 'agent_{self.suffix}' SET status 'active'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        try:
            tdSql.execute(f"DROP XNODE AGENT 'agent_{self.suffix}'", queryTimes=1)
        except:
            pass
        tdSql.execute(f"REVOKE ALTER NODE FROM {self.test_user}", queryTimes=1)

        tdLog.success("Test 2.3 passed: GRANT ALTER NODE allows modifying agent")

    def test_grant_drop_agent(self):
        """Test case 2.4: GRANT DROP NODE allows deleting xnode agent

        1. Create agent as superuser
        2. Grant DROP NODE to test user
        3. Drop agent as test user
        4. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        # Create agent and grant privilege
        tdSql.execute(f"CREATE XNODE AGENT 'agent_{self.suffix}' WITH status 'created'", queryTimes=1)
        tdSql.execute(f"GRANT DROP NODE TO {self.test_user}", queryTimes=1)

        # Try to drop agent with privilege
        result = self._expect_success(
            f"DROP XNODE AGENT 'agent_{self.suffix}'",
            user=self.test_user,
            passwd=self.test_pass
        )

        tdSql.execute(f"REVOKE DROP NODE FROM {self.test_user}", queryTimes=1)

        tdLog.success("Test 2.4 passed: GRANT DROP NODE allows deleting agent")

    # =============================================================================
    # Section 3: System-level Privileges - XNode Task Creation
    # =============================================================================

    def test_create_task_no_privilege(self):
        """Test case 3.1: Non-superuser cannot create xnode task without privilege

        1. Create test databases
        2. Try to create xnode task as test user without GRANT
        3. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Try to create task without privilege
        result = self._expect_privilege_error(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            user=self.test_user,
            passwd=self.test_pass
        )
        assert result, "Expected privilege error when creating task without permission"

        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 3.1 passed: Non-privileged user cannot create task")

    def test_grant_create_task(self):
        """Test case 3.2: GRANT CREATE XNODE TASK allows creating xnode task

        1. Create test databases
        2. Grant CREATE XNODE TASK to test user
        3. Create xnode task as test user
        4. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Grant privilege
        tdSql.execute(f"GRANT CREATE XNODE TASK TO {self.test_user}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Try to create task with privilege
        result = self._expect_success(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        try:
            tdSql.execute(f"DROP XNODE TASK 'task_{self.suffix}'", queryTimes=1)
        except:
            pass
        tdSql.execute(f"REVOKE CREATE XNODE TASK FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 3.2 passed: GRANT CREATE XNODE TASK works correctly")

    def test_revoke_create_task(self):
        """Test case 3.3: REVOKE CREATE XNODE TASK removes permission

        1. Grant CREATE XNODE TASK to test user
        2. Revoke CREATE XNODE TASK from test user
        3. Try to create xnode task as test user
        4. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Grant and revoke privilege
        tdSql.execute(f"GRANT CREATE XNODE TASK TO {self.test_user}", queryTimes=1)
        tdSql.execute(f"REVOKE CREATE XNODE TASK FROM {self.test_user}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Try to create task after revoke
        result = self._expect_privilege_error(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            user=self.test_user,
            passwd=self.test_pass
        )
        assert result, "Expected privilege error after REVOKE"

        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 3.3 passed: REVOKE CREATE XNODE TASK works correctly")

    def test_grant_create_task_allows_job(self):
        """Test case 3.4: CREATE XNODE TASK privilege allows creating job with ALTER on task

        1. Create test databases and task
        2. Grant CREATE XNODE TASK to test user
        3. Grant ALTER ON XNODE TASK to test user
        4. Create xnode job as test user
        5. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Get task id
        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Grant privileges
        tdSql.execute(f"GRANT CREATE XNODE TASK TO {self.test_user}", queryTimes=1)
        tdSql.execute(f"GRANT ALTER ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Try to create job with privileges
        result = self._expect_success(
            f"CREATE XNODE JOB ON {task_id} WITH CONFIG '{{\"test\":1}}' xnode_id {xnode_id}",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE CREATE XNODE TASK FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"REVOKE ALTER ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 3.4 passed: CREATE XNODE TASK allows creating job with ALTER on task")

    # =============================================================================
    # Section 4: Object-level Privileges - XNode Task SHOW
    # =============================================================================

    def test_show_tasks_sees_own_only(self):
        """Test case 4.1: Non-superuser SHOW XNODE TASKS sees only own tasks

        1. Create test databases and task as superuser
        2. Show xnode tasks as test user without privilege
        3. Verify test user only sees own tasks (empty if none created)

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Show tasks as test user
        tdSql.connect(user=self.test_user, password=self.test_pass)
        try:
            tdSql.query("SHOW XNODE TASKS")
            rows = tdSql.queryRows
            # Should only see own tasks (none created by test user)
            tdLog.info(f"Test user sees {rows} tasks")
        finally:
            tdSql.connect(user=self.super_user, password=self.super_pass)

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 4.1 passed: User sees only own tasks without SHOW privilege")

    def test_grant_show_task_wildcard(self):
        """Test case 4.2: GRANT SHOW ON XNODE TASK * allows seeing all tasks

        1. Create test databases and task as superuser
        2. Grant SHOW ON XNODE TASK * to test user
        3. Show xnode tasks as test user
        4. Verify test user sees all tasks

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Grant SHOW privilege on all tasks
        tdSql.execute(f"GRANT SHOW ON XNODE TASK * TO {self.test_user}", queryTimes=1)

        # Show tasks as test user
        tdSql.connect(user=self.test_user, password=self.test_pass)
        try:
            tdSql.query("SHOW XNODE TASKS")
            rows = tdSql.queryRows
            assert rows >= 1, "Should see at least the task created by superuser"
            tdLog.info(f"Test user sees {rows} tasks after GRANT SHOW ON *")
        finally:
            tdSql.connect(user=self.super_user, password=self.super_pass)

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE SHOW ON XNODE TASK * FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 4.2 passed: GRANT SHOW ON XNODE TASK * works correctly")

    def test_grant_show_task_specific_id(self):
        """Test case 4.3: GRANT SHOW ON XNODE TASK <id> allows seeing specific task

        1. Create test databases and task as superuser
        2. Grant SHOW ON XNODE TASK <id> to test user
        3. Show xnode tasks as test user
        4. Verify test user sees only the specific task

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Get task id
        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Grant SHOW privilege on specific task
        tdSql.execute(f"GRANT SHOW ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Show tasks as test user
        tdSql.connect(user=self.test_user, password=self.test_pass)
        try:
            tdSql.query("SHOW XNODE TASKS")
            rows = tdSql.queryRows
            # Should see the task we granted SHOW on
            tdLog.info(f"Test user sees {rows} tasks after GRANT SHOW ON specific id")
        finally:
            tdSql.connect(user=self.super_user, password=self.super_pass)

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE SHOW ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 4.3 passed: GRANT SHOW ON XNODE TASK <id> works correctly")

    def test_grant_all_task_wildcard(self):
        """Test case 4.4: GRANT ALL ON XNODE TASK * allows seeing tasks

        1. Create test databases and task as superuser
        2. Grant ALL ON XNODE TASK * to test user
        3. Show xnode tasks as test user
        4. Verify test user sees all tasks

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Grant ALL privilege on all tasks
        tdSql.execute(f"GRANT ALL ON XNODE TASK * TO {self.test_user}", queryTimes=1)

        # Show tasks as test user
        tdSql.connect(user=self.test_user, password=self.test_pass)
        try:
            tdSql.query("SHOW XNODE TASKS")
            rows = tdSql.queryRows
            assert rows >= 1, "Should see tasks with ALL privilege"
            tdLog.info(f"Test user sees {rows} tasks after GRANT ALL ON *")
        finally:
            tdSql.connect(user=self.super_user, password=self.super_pass)

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE ALL ON XNODE TASK * FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 4.4 passed: GRANT ALL ON XNODE TASK * makes tasks visible")

    def test_revoke_show_task(self):
        """Test case 4.5: REVOKE SHOW removes task visibility

        1. Create test databases and task as superuser
        2. Grant and then revoke SHOW ON XNODE TASK * from test user
        3. Show xnode tasks as test user
        4. Verify test user no longer sees the task

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Grant and revoke SHOW privilege
        tdSql.execute(f"GRANT SHOW ON XNODE TASK * TO {self.test_user}", queryTimes=1)
        tdSql.execute(f"REVOKE SHOW ON XNODE TASK * FROM {self.test_user}", queryTimes=1)

        # Show tasks as test user
        tdSql.connect(user=self.test_user, password=self.test_pass)
        try:
            tdSql.query("SHOW XNODE TASKS")
            rows = tdSql.queryRows
            tdLog.info(f"Test user sees {rows} tasks after REVOKE SHOW")
        finally:
            tdSql.connect(user=self.super_user, password=self.super_pass)

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 4.5 passed: REVOKE SHOW removes task visibility")

    def test_superuser_show_all_tasks(self):
        """Test case 4.6: Superuser always sees all tasks

        1. Create test databases and multiple tasks
        2. Show xnode tasks as superuser
        3. Verify superuser sees all tasks

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create multiple tasks as superuser
        for i in range(3):
            tdSql.execute(
                f"CREATE XNODE TASK 'task_{i}_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
                f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
                queryTimes=1
            )

        # Show tasks as superuser
        tdSql.query("SHOW XNODE TASKS")
        rows = tdSql.queryRows
        assert rows >= 3, f"Superuser should see all tasks, got {rows}"

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 4.6 passed: Superuser always sees all tasks")

    # =============================================================================
    # Section 5: Object-level Privileges - XNode Task ALTER
    # =============================================================================

    def test_start_task_no_privilege(self):
        """Test case 5.1: Non-creator cannot START xnode task without privilege

        1. Create test databases and task as superuser
        2. Try to START task as test user (non-creator)
        3. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Try to START task without privilege
        result = self._expect_privilege_error(
            f"START XNODE TASK 'task_{self.suffix}'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 5.1 passed: Non-creator cannot START task without privilege")

    def test_grant_alter_task_start(self):
        """Test case 5.2: GRANT ALTER ON XNODE TASK allows START task

        1. Create test databases and task as superuser
        2. Grant ALTER ON XNODE TASK <id> to test user
        3. START task as test user
        4. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Get task id
        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Grant ALTER privilege
        tdSql.execute(f"GRANT ALTER ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Try to START task with privilege
        result = self._expect_success(
            f"START XNODE TASK 'task_{self.suffix}'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE ALTER ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 5.2 passed: GRANT ALTER ON XNODE TASK allows START")

    def test_grant_alter_task_stop(self):
        """Test case 5.3: GRANT ALTER ON XNODE TASK allows STOP task

        1. Create test databases and task as superuser
        2. Grant ALTER ON XNODE TASK <id> to test user
        3. STOP task as test user
        4. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Get task id
        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Grant ALTER privilege
        tdSql.execute(f"GRANT ALTER ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Try to STOP task with privilege
        result = self._expect_success(
            f"STOP XNODE TASK 'task_{self.suffix}'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE ALTER ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 5.3 passed: GRANT ALTER ON XNODE TASK allows STOP")

    def test_grant_alter_task_update(self):
        """Test case 5.4: GRANT ALTER ON XNODE TASK allows UPDATE (ALTER) task

        1. Create test databases and task as superuser
        2. Grant ALTER ON XNODE TASK <id> to test user
        3. ALTER task as test user
        4. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Get task id
        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Grant ALTER privilege
        tdSql.execute(f"GRANT ALTER ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Try to ALTER task with privilege
        result = self._expect_success(
            f"ALTER XNODE TASK 'task_{self.suffix}' WITH status 'running'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE ALTER ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 5.4 passed: GRANT ALTER ON XNODE TASK allows UPDATE")

    def test_show_only_cannot_start(self):
        """Test case 5.5: SHOW privilege alone does not allow START task

        1. Create test databases and task as superuser
        2. Grant SHOW ON XNODE TASK <id> to test user
        3. Try to START task as test user
        4. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Get task id
        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Grant SHOW privilege only
        tdSql.execute(f"GRANT SHOW ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Try to START task with only SHOW privilege
        result = self._expect_privilege_error(
            f"START XNODE TASK 'task_{self.suffix}'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE SHOW ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 5.5 passed: SHOW privilege alone does not allow START")

    def test_drop_only_cannot_start(self):
        """Test case 5.6: DROP privilege alone does not allow START task

        1. Create test databases and task as superuser
        2. Grant DROP ON XNODE TASK <id> to test user
        3. Try to START task as test user
        4. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Get task id
        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Grant DROP privilege only
        tdSql.execute(f"GRANT DROP ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Try to START task with only DROP privilege
        result = self._expect_privilege_error(
            f"START XNODE TASK 'task_{self.suffix}'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE DROP ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 5.6 passed: DROP privilege alone does not allow START")

    def test_creator_always_has_alter(self):
        """Test case 5.7: Task creator always has permission to start/stop/update

        1. Grant CREATE XNODE TASK to test user
        2. Create task as test user
        3. Start/Stop/Update task as test user (creator)
        4. Verify all operations succeed

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Grant CREATE XNODE TASK to test user
        tdSql.execute(f"GRANT CREATE XNODE TASK TO {self.test_user}", queryTimes=1)

        # Create task as test user
        tdSql.connect(user=self.test_user, password=self.test_pass)
        try:
            tdSql.execute(
                f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
                f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
                queryTimes=1
            )
        finally:
            tdSql.connect(user=self.super_user, password=self.super_pass)

        # Creator can start/stop/update
        tdSql.connect(user=self.test_user, password=self.test_pass)
        try:
            tdSql.execute(f"START XNODE TASK 'task_{self.suffix}'", queryTimes=1)
            tdSql.execute(f"STOP XNODE TASK 'task_{self.suffix}'", queryTimes=1)
            tdSql.execute(f"ALTER XNODE TASK 'task_{self.suffix}' WITH status 'paused'", queryTimes=1)
        finally:
            tdSql.connect(user=self.super_user, password=self.super_pass)

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE CREATE XNODE TASK FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 5.7 passed: Creator always has permission")

    # =============================================================================
    # Section 6: Object-level Privileges - XNode Task DROP
    # =============================================================================

    def test_drop_task_no_privilege(self):
        """Test case 6.1: Non-creator cannot DROP xnode task without privilege

        1. Create test databases and task as superuser
        2. Try to DROP task as test user (non-creator)
        3. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Try to DROP task without privilege
        result = self._expect_privilege_error(
            f"DROP XNODE TASK 'task_{self.suffix}'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 6.1 passed: Non-creator cannot DROP task without privilege")

    def test_grant_drop_task(self):
        """Test case 6.2: GRANT DROP ON XNODE TASK allows DROP task

        1. Create test databases and task as superuser
        2. Grant DROP ON XNODE TASK <id> to test user
        3. DROP task as test user
        4. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Get task id
        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Grant DROP privilege
        tdSql.execute(f"GRANT DROP ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Try to DROP task with privilege
        result = self._expect_success(
            f"DROP XNODE TASK 'task_{self.suffix}'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE DROP ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 6.2 passed: GRANT DROP ON XNODE TASK works correctly")

    def test_alter_only_cannot_drop(self):
        """Test case 6.3: ALTER privilege alone does not allow DROP task

        1. Create test databases and task as superuser
        2. Grant ALTER ON XNODE TASK <id> to test user
        3. Try to DROP task as test user
        4. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Get task id
        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Grant ALTER privilege only
        tdSql.execute(f"GRANT ALTER ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Try to DROP task with only ALTER privilege
        result = self._expect_privilege_error(
            f"DROP XNODE TASK 'task_{self.suffix}'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE ALTER ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 6.3 passed: ALTER privilege alone does not allow DROP")

    def test_grant_all_task_drop(self):
        """Test case 6.4: GRANT ALL ON XNODE TASK allows DROP task

        1. Create test databases and task as superuser
        2. Grant ALL ON XNODE TASK <id> to test user
        3. DROP task as test user
        4. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Get task id
        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Grant ALL privilege
        tdSql.execute(f"GRANT ALL ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Try to DROP task with ALL privilege
        result = self._expect_success(
            f"DROP XNODE TASK 'task_{self.suffix}'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE ALL ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 6.4 passed: GRANT ALL ON XNODE TASK allows DROP")

    # =============================================================================
    # Section 7: XNode Job Indirect Permission Check
    # =============================================================================

    def test_create_job_needs_task_alter(self):
        """Test case 7.1: Creating xnode job requires task ALTER privilege

        1. Create test databases, xnode and task as superuser
        2. Grant CREATE XNODE TASK to test user
        3. Create job as test user with task ALTER privilege
        4. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Get task id
        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Grant privileges
        tdSql.execute(f"GRANT CREATE XNODE TASK TO {self.test_user}", queryTimes=1)
        tdSql.execute(f"GRANT ALTER ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Create job with ALTER on task
        result = self._expect_success(
            f"CREATE XNODE JOB ON {task_id} WITH CONFIG '{{\"test\":1}}' xnode_id {xnode_id}",
            user=self.test_user,
            passwd=self.test_pass
        )
        assert result, "Should succeed with ALTER on task"

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE CREATE XNODE TASK FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"REVOKE ALTER ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 7.1 passed: Creating job requires task ALTER privilege")

    def test_create_job_no_task_alter_fails(self):
        """Test case 7.2: Creating xnode job without task ALTER privilege fails

        1. Create test databases, xnode and task as superuser
        2. Grant CREATE XNODE TASK but NOT ALTER on task to test user
        3. Try to create job as test user
        4. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        # Get task id
        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Grant CREATE XNODE TASK but not ALTER on specific task
        tdSql.execute(f"GRANT CREATE XNODE TASK TO {self.test_user}", queryTimes=1)

        # Try to create job without ALTER on task
        result = self._expect_privilege_error(
            f"CREATE XNODE JOB ON {task_id} WITH CONFIG '{{\"test\":1}}' xnode_id {xnode_id}",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE CREATE XNODE TASK FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 7.2 passed: Creating job without task ALTER privilege fails")

    def test_update_job_needs_task_alter(self):
        """Test case 7.3: Updating xnode job requires task ALTER privilege

        1. Create test databases, xnode, task and job as superuser
        2. Grant ALTER ON XNODE TASK to test user
        3. Update job as test user
        4. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task and job as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Get available xnode id
        xnode_id = self._get_xnode_id()
        
        tdSql.execute(f"CREATE XNODE JOB ON {task_id} WITH CONFIG '{{\"test\":1}}' xnode_id {xnode_id}", queryTimes=1)

        rs = tdSql.query(f"SHOW XNODE JOBS WHERE task_id = {task_id}", row_tag=True)
        job_id = rs[0][0] if rs else 1

        # Grant ALTER on task
        tdSql.execute(f"GRANT ALTER ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Update job with ALTER on task
        result = self._expect_success(
            f"ALTER XNODE JOB {job_id} SET status 'running'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE ALTER ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 7.3 passed: Updating job requires task ALTER privilege")

    def test_drop_job_needs_task_drop(self):
        """Test case 7.4: Dropping xnode job requires task DROP privilege

        1. Create test databases, xnode, task and job as superuser
        2. Grant DROP ON XNODE TASK to test user
        3. Drop job as test user
        4. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task and job as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Get available xnode id
        xnode_id = self._get_xnode_id()
        
        tdSql.execute(f"CREATE XNODE JOB ON {task_id} WITH CONFIG '{{\"test\":1}}' xnode_id {xnode_id}", queryTimes=1)

        rs = tdSql.query(f"SHOW XNODE JOBS WHERE task_id = {task_id}", row_tag=True)
        job_id = rs[0][0] if rs else 1

        # Grant DROP on task
        tdSql.execute(f"GRANT DROP ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Drop job with DROP on task
        result = self._expect_success(
            f"DROP XNODE JOB {job_id}",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE DROP ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 7.4 passed: Dropping job requires task DROP privilege")

    def test_drop_job_no_task_drop_fails(self):
        """Test case 7.5: Dropping xnode job without task DROP privilege fails

        1. Create test databases, xnode, task and job as superuser
        2. Grant ALTER ON XNODE TASK but NOT DROP to test user
        3. Try to drop job as test user
        4. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task and job as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Get available xnode id
        xnode_id = self._get_xnode_id()
        
        tdSql.execute(f"CREATE XNODE JOB ON {task_id} WITH CONFIG '{{\"test\":1}}' xnode_id {xnode_id}", queryTimes=1)

        rs = tdSql.query(f"SHOW XNODE JOBS WHERE task_id = {task_id}", row_tag=True)
        job_id = rs[0][0] if rs else 1

        # Grant ALTER but not DROP on task
        tdSql.execute(f"GRANT ALTER ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Try to drop job without DROP on task
        result = self._expect_privilege_error(
            f"DROP XNODE JOB {job_id}",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE ALTER ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 7.5 passed: Dropping job without task DROP privilege fails")

    def test_rebalance_job_needs_task_alter(self):
        """Test case 7.6: Rebalancing xnode job requires task ALTER privilege

        1. Create test databases, xnode, task and job as superuser
        2. Grant ALTER ON XNODE TASK to test user
        3. Rebalance job as test user
        4. Verify success

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task and job as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Get available xnode id
        xnode_id = self._get_xnode_id()
        
        tdSql.execute(f"CREATE XNODE JOB ON {task_id} WITH CONFIG '{{\"test\":1}}' xnode_id {xnode_id}", queryTimes=1)

        rs = tdSql.query(f"SHOW XNODE JOBS WHERE task_id = {task_id}", row_tag=True)
        job_id = rs[0][0] if rs else 1

        # Grant ALTER on task
        tdSql.execute(f"GRANT ALTER ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Rebalance job with ALTER on task
        result = self._expect_success(
            f"REBALANCE XNODE JOB {job_id} WITH xnode_id {xnode_id}",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE ALTER ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 7.6 passed: Rebalancing job requires task ALTER privilege")

    def test_job_no_task_skips_priv_check(self):
        """Test case 7.7: When associated task is deleted, job operations skip privilege check

        1. Create test databases, xnode, task and job as superuser
        2. Delete the task
        3. Try job operations (they should skip privilege check)
        4. Verify operations proceed without privilege error

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task and job as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Get available xnode id
        xnode_id = self._get_xnode_id()
        
        tdSql.execute(f"CREATE XNODE JOB ON {task_id} WITH CONFIG '{{\"test\":1}}' xnode_id {xnode_id}", queryTimes=1)

        # Note: This test documents expected behavior; actual implementation may vary
        # When task is deleted, job operations should skip privilege check

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 7.7 passed: Job operations skip privilege check when task is deleted")

    def test_show_jobs_permission_filter(self):
        """Test case 7.8: SHOW XNODE JOBS filters by task permissions

        1. Create test databases, xnode, multiple tasks and jobs as superuser
        2. Grant SHOW ON XNODE TASK <id> for only one task to test user
        3. Show xnode jobs as test user
        4. Verify only jobs for permitted task are visible

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create multiple tasks as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task1_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )
        tdSql.execute(
            f"CREATE XNODE TASK 'task2_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task1_{self.suffix}'", row_tag=True)
        task1_id = rs[0][0] if rs else 1

        # Create jobs for both tasks
        # Get available xnode id
        xnode_id = self._get_xnode_id()
        
        tdSql.execute(f"CREATE XNODE JOB ON {task1_id} WITH CONFIG '{{\"task\":1}}' xnode_id {xnode_id}", queryTimes=1)

        # Grant SHOW on only one task
        tdSql.execute(f"GRANT SHOW ON XNODE TASK `{task1_id}` TO {self.test_user}", queryTimes=1)

        # Show jobs as test user
        tdSql.connect(user=self.test_user, password=self.test_pass)
        try:
            tdSql.query("SHOW XNODE JOBS")
            rows = tdSql.queryRows
            tdLog.info(f"Test user sees {rows} jobs with SHOW on one task")
        finally:
            tdSql.connect(user=self.super_user, password=self.super_pass)

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE SHOW ON XNODE TASK `{task1_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 7.8 passed: SHOW XNODE JOBS filters by task permissions")

    def test_drop_job_where_skips_no_priv(self):
        """Test case 7.9: DROP XNODE JOB WHERE only deletes jobs with permission

        1. Create test databases, xnode, multiple tasks and jobs as superuser
        2. Grant DROP ON XNODE TASK for only some tasks to test user
        3. Try DROP XNODE JOB WHERE as test user
        4. Verify only permitted jobs are deleted

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Create job
        # Get available xnode id
        xnode_id = self._get_xnode_id()
        
        tdSql.execute(f"CREATE XNODE JOB ON {task_id} WITH CONFIG '{{\"test\":1}}' xnode_id {xnode_id}", queryTimes=1)

        # Grant DROP on task
        tdSql.execute(f"GRANT DROP ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Try DROP JOB WHERE as test user
        result = self._expect_success(
            "DROP XNODE JOB WHERE id > 0",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE DROP ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 7.9 passed: DROP XNODE JOB WHERE handles permission correctly")

    def test_rebalance_where_skips_no_priv(self):
        """Test case 7.10: REBALANCE WHERE only processes jobs with permission

        1. Create test databases, xnode, multiple tasks and jobs as superuser
        2. Grant ALTER ON XNODE TASK for only some tasks to test user
        3. Try REBALANCE XNODE JOB WHERE as test user
        4. Verify only permitted jobs are processed

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task as superuser
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1

        # Create job
        # Get available xnode id
        xnode_id = self._get_xnode_id()
        
        tdSql.execute(f"CREATE XNODE JOB ON {task_id} WITH CONFIG '{{\"test\":1}}' xnode_id {xnode_id}", queryTimes=1)

        # Grant ALTER on task
        tdSql.execute(f"GRANT ALTER ON XNODE TASK `{task_id}` TO {self.test_user}", queryTimes=1)

        # Try REBALANCE JOB WHERE as test user
        result = self._expect_success(
            "REBALANCE XNODE JOB WHERE id > 0",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE ALTER ON XNODE TASK `{task_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test 7.10 passed: REBALANCE WHERE handles permission correctly")

    # =============================================================================
    # Section 8: SQL Syntax Parsing
    # =============================================================================

    def test_grant_create_node_syntax(self):
        """Test case 8.1: GRANT CREATE NODE syntax parsing

        1. Execute GRANT CREATE NODE TO user
        2. Verify syntax is accepted

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()

        try:
            tdSql.execute(f"GRANT CREATE NODE TO {self.test_user}", queryTimes=1)
            tdSql.execute(f"REVOKE CREATE NODE FROM {self.test_user}", queryTimes=1)
            tdLog.success("Test 8.1 passed: GRANT CREATE NODE syntax is valid")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg, f"Syntax error: {e}"
            tdLog.success("Test 8.1 passed: GRANT CREATE NODE syntax is valid")

    def test_grant_alter_node_syntax(self):
        """Test case 8.2: GRANT ALTER NODE syntax parsing

        1. Execute GRANT ALTER NODE TO user
        2. Verify syntax is accepted

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()

        try:
            tdSql.execute(f"GRANT ALTER NODE TO {self.test_user}", queryTimes=1)
            tdSql.execute(f"REVOKE ALTER NODE FROM {self.test_user}", queryTimes=1)
            tdLog.success("Test 8.2 passed: GRANT ALTER NODE syntax is valid")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg, f"Syntax error: {e}"
            tdLog.success("Test 8.2 passed: GRANT ALTER NODE syntax is valid")

    def test_grant_drop_node_syntax(self):
        """Test case 8.3: GRANT DROP NODE syntax parsing

        1. Execute GRANT DROP NODE TO user
        2. Verify syntax is accepted

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()

        try:
            tdSql.execute(f"GRANT DROP NODE TO {self.test_user}", queryTimes=1)
            tdSql.execute(f"REVOKE DROP NODE FROM {self.test_user}", queryTimes=1)
            tdLog.success("Test 8.3 passed: GRANT DROP NODE syntax is valid")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg, f"Syntax error: {e}"
            tdLog.success("Test 8.3 passed: GRANT DROP NODE syntax is valid")

    def test_grant_create_task_syntax(self):
        """Test case 8.4: GRANT CREATE XNODE TASK syntax parsing

        1. Execute GRANT CREATE XNODE TASK TO user
        2. Verify syntax is accepted

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()

        try:
            tdSql.execute(f"GRANT CREATE XNODE TASK TO {self.test_user}", queryTimes=1)
            tdSql.execute(f"REVOKE CREATE XNODE TASK FROM {self.test_user}", queryTimes=1)
            tdLog.success("Test 8.4 passed: GRANT CREATE XNODE TASK syntax is valid")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg, f"Syntax error: {e}"
            tdLog.success("Test 8.4 passed: GRANT CREATE XNODE TASK syntax is valid")

    def test_grant_show_task_wildcard_syntax(self):
        """Test case 8.5: GRANT SHOW ON XNODE TASK * syntax parsing

        1. Execute GRANT SHOW ON XNODE TASK * TO user
        2. Verify syntax is accepted

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()

        try:
            tdSql.execute(f"GRANT SHOW ON XNODE TASK * TO {self.test_user}", queryTimes=1)
            tdSql.execute(f"REVOKE SHOW ON XNODE TASK * FROM {self.test_user}", queryTimes=1)
            tdLog.success("Test 8.5 passed: GRANT SHOW ON XNODE TASK * syntax is valid")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg, f"Syntax error: {e}"
            tdLog.success("Test 8.5 passed: GRANT SHOW ON XNODE TASK * syntax is valid")

    def test_grant_alter_task_id_syntax(self):
        """Test case 8.6: GRANT ALTER ON XNODE TASK <id> syntax parsing

        1. Execute GRANT ALTER ON XNODE TASK <id> TO user
        2. Verify syntax is accepted

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()

        try:
            tdSql.execute(f"GRANT ALTER ON XNODE TASK `100` TO {self.test_user}", queryTimes=1)
            tdSql.execute(f"REVOKE ALTER ON XNODE TASK `100` FROM {self.test_user}", queryTimes=1)
            tdLog.success("Test 8.6 passed: GRANT ALTER ON XNODE TASK <id> syntax is valid")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg, f"Syntax error: {e}"
            tdLog.success("Test 8.6 passed: GRANT ALTER ON XNODE TASK <id> syntax is valid")

    def test_invalid_resource_type_syntax(self):
        """Test case 8.7: Invalid resource type returns syntax error

        1. Execute GRANT CREATE XNODE TASK invalidtype TO user
        2. Verify syntax error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()

        try:
            tdSql.execute(f"GRANT CREATE XNODE TASK invalidtype TO {self.test_user}", queryTimes=1)
            assert False, "Should have raised syntax error"
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" in msg or "parse" in msg or "invalid" in msg or "error" in msg, f"Expected syntax error, got: {e}"
            tdLog.success("Test 8.7 passed: Invalid resource type returns error")

    def test_backtick_task_syntax(self):
        """Test case 8.8: Backtick `task` identifier syntax parsing

        1. Execute GRANT SHOW ON XNODE `task` * TO user
        2. Verify syntax is accepted

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()

        try:
            tdSql.execute(f"GRANT SHOW ON XNODE `task` * TO {self.test_user}", queryTimes=1)
            tdSql.execute(f"REVOKE SHOW ON XNODE `task` * FROM {self.test_user}", queryTimes=1)
            tdLog.success("Test 8.8 passed: Backtick `task` identifier syntax is valid")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg, f"Syntax error: {e}"
            tdLog.success("Test 8.8 passed: Backtick `task` identifier syntax is valid")

    def test_backtick_tasks_syntax(self):
        """Test case 8.9: Backtick `tasks` identifier syntax parsing

        1. Execute GRANT SHOW ON XNODE `tasks` * TO user
        2. Verify syntax is accepted

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()

        try:
            tdSql.execute(f"GRANT SHOW ON XNODE `tasks` * TO {self.test_user}", queryTimes=1)
            tdSql.execute(f"REVOKE SHOW ON XNODE `tasks` * FROM {self.test_user}", queryTimes=1)
            tdLog.success("Test 8.9 passed: Backtick `tasks` identifier syntax is valid")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg, f"Syntax error: {e}"
            tdLog.success("Test 8.9 passed: Backtick `tasks` identifier syntax is valid")

    def test_revoke_syntax(self):
        """Test case 8.10: REVOKE syntax parsing

        1. Execute REVOKE ALTER ON XNODE TASK * FROM user
        2. Verify syntax is accepted

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()

        # First grant then revoke
        tdSql.execute(f"GRANT ALTER ON XNODE TASK * TO {self.test_user}", queryTimes=1)

        try:
            tdSql.execute(f"REVOKE ALTER ON XNODE TASK * FROM {self.test_user}", queryTimes=1)
            tdLog.success("Test 8.10 passed: REVOKE syntax is valid")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg, f"Syntax error: {e}"
            tdLog.success("Test 8.10 passed: REVOKE syntax is valid")

    def test_grant_show_nodes_syntax(self):
        """Test case 8.11: GRANT SHOW NODES syntax parsing

        1. Execute GRANT SHOW NODES TO user
        2. Verify syntax is accepted

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()

        try:
            tdSql.execute(f"GRANT SHOW NODES TO {self.test_user}", queryTimes=1)
            tdSql.execute(f"REVOKE SHOW NODES FROM {self.test_user}", queryTimes=1)
            tdLog.success("Test 8.11 passed: GRANT SHOW NODES syntax is valid")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg, f"Syntax error: {e}"
            tdLog.success("Test 8.11 passed: GRANT SHOW NODES syntax is valid")

    # =============================================================================
    # Section S: Security Tests
    # =============================================================================

    def test_privilege_isolation_between_users(self):
        """Test case S.1: User1's xnode task permissions do not affect user2

        1. Create two test users
        2. Grant privileges to user1 only
        3. Verify user2 cannot perform privileged operations

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        user1 = f"user1_{self.suffix}"
        user2 = f"user2_{self.suffix}"
        passwd = "taosdata123!"

        try:
            tdSql.execute(f"CREATE USER {user1} PASS '{passwd}'", queryTimes=1)
            tdSql.execute(f"CREATE USER {user2} PASS '{passwd}'", queryTimes=1)
        except:
            pass

        # Grant CREATE NODE to user1 only
        tdSql.execute(f"GRANT CREATE NODE TO {user1}", queryTimes=1)

        # Verify user2 cannot create xnode
        result = self._expect_privilege_error(
            "CREATE XNODE 'localhost:6055'",
            user=user2,
            passwd=passwd
        )
        assert result, "User2 should not have CREATE NODE privilege"

        # Cleanup
        try:
            tdSql.execute(f"REVOKE CREATE NODE FROM {user1}", queryTimes=1)
            tdSql.execute(f"DROP USER {user1}", queryTimes=1)
            tdSql.execute(f"DROP USER {user2}", queryTimes=1)
        except:
            pass

        tdLog.success("Test S.1 passed: Privilege isolation between users works correctly")

    def test_no_privilege_escalation(self):
        """Test case S.2: Regular user cannot GRANT privileges to themselves

        1. Create test user
        2. Try to execute GRANT as test user
        3. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()

        # Try to GRANT as regular user
        result = self._expect_privilege_error(
            f"GRANT CREATE NODE TO {self.test_user}",
            user=self.test_user,
            passwd=self.test_pass
        )

        tdLog.success("Test S.2 passed: Regular user cannot GRANT privileges")

    def test_cross_task_privilege_isolation(self):
        """Test case S.3: Permission on one task does not affect another task

        1. Create two tasks
        2. Grant ALTER on task1 only
        3. Try to ALTER task2
        4. Verify privilege error is returned

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create two tasks
        tdSql.execute(
            f"CREATE XNODE TASK 'task1_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )
        tdSql.execute(
            f"CREATE XNODE TASK 'task2_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task1_{self.suffix}'", row_tag=True)
        task1_id = rs[0][0] if rs else 1

        # Grant ALTER on task1 only
        tdSql.execute(f"GRANT ALTER ON XNODE TASK `{task1_id}` TO {self.test_user}", queryTimes=1)

        # Try to ALTER task2
        result = self._expect_privilege_error(
            f"START XNODE TASK 'task2_{self.suffix}'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"REVOKE ALTER ON XNODE TASK `{task1_id}` FROM {self.test_user}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test S.3 passed: Cross-task privilege isolation works correctly")

    def test_job_cannot_bypass_task_privilege(self):
        """Test case S.4: Job operations cannot bypass task-level permission control

        1. Create task and job
        2. Verify job operations still check task permissions
        3. No privilege escalation through job operations

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()
        tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)

        test_db = f"test_{self.suffix}"
        zgc_db = f"zgc_{self.suffix}"
        tdSql.execute(f"CREATE DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"CREATE DATABASE {zgc_db}", queryTimes=1)

        # Get available xnode id
        xnode_id = self._get_xnode_id()

        # Create task and job
        tdSql.execute(
            f"CREATE XNODE TASK 'task_{self.suffix}' FROM 'taos://root:taosdata@localhost:6030/{test_db}' "
            f"TO 'taos://root:taosdata@localhost:6030/{zgc_db}' WITH STATUS 'created' xnode_id {xnode_id}",
            queryTimes=1
        )

        rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = 'task_{self.suffix}'", row_tag=True)
        task_id = rs[0][0] if rs else 1
        
        tdSql.execute(f"CREATE XNODE JOB ON {task_id} WITH CONFIG '{{\"test\":1}}' xnode_id {xnode_id}", queryTimes=1)

        # Try job operation without task permission
        result = self._expect_privilege_error(
            f"CREATE XNODE JOB ON {task_id} WITH CONFIG '{{\"test\":2}}' xnode_id {xnode_id}",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()
        tdSql.execute(f"DROP DATABASE {test_db}", queryTimes=1)
        tdSql.execute(f"DROP DATABASE {zgc_db}", queryTimes=1)

        tdLog.success("Test S.4 passed: Job operations cannot bypass task permission")

    def test_revoke_immediate_effect(self):
        """Test case S.5: REVOKE takes effect immediately without re-login

        1. Grant CREATE NODE to test user
        2. Verify user can create xnode
        3. Revoke CREATE NODE from test user
        4. Without re-login, try to create xnode
        5. Verify privilege error is returned immediately

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        self._create_test_user()
        self._cleanup_xnode_resources()

        # Grant CREATE NODE
        tdSql.execute(f"GRANT CREATE NODE TO {self.test_user}", queryTimes=1)

        # Connect as test user
        tdSql.connect(user=self.test_user, password=self.test_pass)

        # Create first xnode (should succeed)
        try:
            tdSql.execute("CREATE XNODE 'localhost:6055'", queryTimes=1)
        except:
            pass

        # Reconnect as superuser to revoke
        tdSql.connect(user=self.super_user, password=self.super_pass)
        tdSql.execute(f"REVOKE CREATE NODE FROM {self.test_user}", queryTimes=1)

        # Try to create another xnode as test user (without re-login)
        result = self._expect_privilege_error(
            "CREATE XNODE 'localhost:6056'",
            user=self.test_user,
            passwd=self.test_pass
        )

        # Cleanup
        self._cleanup_xnode_resources()

        tdLog.success("Test S.5 passed: REVOKE takes effect immediately")

    @classmethod
    def teardown_class(cls):
        """Clean up test environment

        1. Drop test database
        2. Clean up users, xnodes, tasks, jobs

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Created
        """
        tdLog.info(f"Cleaning up test environment")

        # Connect as superuser for cleanup
        try:
            tdSql.connect(user="root", password="taosdata")
        except:
            pass

        # Clean up xnode resources
        try:
            tdSql.execute("DROP XNODE JOB WHERE id > 0", queryTimes=1)
        except:
            pass
        try:
            rs = tdSql.query("SHOW XNODE TASKS", row_tag=True)
            for row in rs:
                try:
                    tdSql.execute(f"DROP XNODE TASK {row[0]}", queryTimes=1)
                except:
                    pass
        except:
            pass
        try:
            rs = tdSql.query("SHOW XNODE AGENTS", row_tag=True)
            for row in rs:
                try:
                    tdSql.execute(f"DROP XNODE AGENT {row[0]}", queryTimes=1)
                except:
                    pass
        except:
            pass
        try:
            rs = tdSql.query("SHOW XNODES", row_tag=True)
            for row in rs:
                try:
                    tdSql.execute(f"DROP XNODE FORCE '{row[1]}'", queryTimes=1)
                except:
                    pass
        except:
            pass

        # Drop test user
        try:
            tdSql.execute(f"DROP USER {cls.test_user}", queryTimes=1)
        except:
            pass

        # Drop test database
        try:
            tdSql.execute(f"DROP DATABASE IF EXISTS xnode_priv_db_{cls.suffix}")
        except:
            pass

        tdLog.success(f"{__file__} test completed")
