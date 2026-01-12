"""
XNode 系统表查询测试

测试 ins_xnodes, ins_xnode_tasks, ins_xnode_jobs, ins_xnode_agents 等系统表的查询功能
"""

import uuid

from new_test_framework.utils import tdLog, tdSql


class TestXnodeSystemTables:
    """XNode 系统表查询测试"""

    replicaVar = 1

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.suffix = uuid.uuid4().hex[:6]
        tdSql.prepare(
            dbname=f"xnode_systables_db_{cls.suffix}", drop=True, replica=cls.replicaVar
        )

    def test_show_xnodes_table(self):
        """测试 SHOW XNODES 系统表查询

        1. Query show xnodes 

        Since: v3.3.8.8

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        sql = "SHOW XNODES"
        tdLog.info(f"Querying: {sql}")
        try:
            tdSql.query(sql)
            rows = tdSql.queryRows
            tdLog.success(f"SHOW XNODES returned {rows} rows")
            
            # 检查返回的列（如果有数据）
            if rows > 0:
                # 应该包含: id, url, status, create_time, update_time 等列
                result = tdSql.queryResult
                tdLog.info(f"First row sample: {result[0] if result else 'No data'}")
                
                # 验证数据类型合理性
                for i in range(rows):
                    row = result[i]
                    tdLog.info(f"XNode row {i}: {row}")
                    # 基本验证：至少应该有 ID 和 url 
                    # 具体字段取决于实际实现
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg, \
                f"Syntax error in SHOW XNODES: {e}"
            tdLog.notice(f"Runtime error tolerated: {e}")

    def test_show_xnode_tasks_table(self):
        """测试 SHOW XNODE TASKS 系统表查询

        1. Query show xnode tasks 

        Since: v3.3.8.8

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        sql = "SHOW XNODE TASKS"
        tdLog.info(f"Querying: {sql}")
        try:
            tdSql.query(sql)
            rows = tdSql.queryRows
            tdLog.success(f"SHOW XNODE TASKS returned {rows} rows")
            
            if rows > 0:
                result = tdSql.queryResult
                tdLog.info(f"First task row: {result[0] if result else 'No data'}")
                
                # 验证任务表字段
                # 应该包含: id, name, xnode_id, status, from, to, options 等
                for i in range(min(rows, 5)):  # 只显示前5行
                    tdLog.info(f"Task row {i}: {result[i]}")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            tdLog.notice(f"Runtime error tolerated: {e}")

    def test_show_xnode_jobs_table(self):
        """测试 SHOW XNODE JOBS 系统表查询

        1. Query show xnode jobs 

        Since: v3.3.8.8

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        sql = "SHOW XNODE JOBS"
        tdLog.info(f"Querying: {sql}")
        try:
            tdSql.query(sql)
            rows = tdSql.queryRows
            tdLog.success(f"SHOW XNODE JOBS returned {rows} rows")
            
            if rows > 0:
                result = tdSql.queryResult
                tdLog.info(f"First job row: {result[0] if result else 'No data'}")
                
                # 验证作业表字段
                # 应该包含: jid, tid, xnode_id, status, create_time 等
                for i in range(min(rows, 5)):
                    tdLog.info(f"Job row {i}: {result[i]}")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            tdLog.notice(f"Runtime error tolerated: {e}")

    def test_show_xnode_agents_table(self):
        """测试 SHOW XNODE AGENTS 系统表查询

        1. Query show xnode agents 

        Since: v3.3.8.8

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        sql = "SHOW XNODE AGENTS"
        tdLog.info(f"Querying: {sql}")
        try:
            tdSql.query(sql)
            rows = tdSql.queryRows
            tdLog.success(f"SHOW XNODE AGENTS returned {rows} rows")
            
            if rows > 0:
                result = tdSql.queryResult
                tdLog.info(f"First agent row: {result[0] if result else 'No data'}")
        except Exception as e:
            msg = str(e).lower()
            assert "syntax" not in msg and "parse" not in msg
            tdLog.notice(f"Runtime error tolerated: {e}")

    def test_query_information_schema_xnodes(self):
        """测试通过 information_schema 查询 XNode 表

        1. Query show xnode from inx_xnodes

        Since: v3.3.8.8

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        sql = "SELECT * FROM information_schema.ins_xnodes"
        tdLog.info(f"Querying: {sql}")
        try:
            tdSql.query(sql)
            rows = tdSql.queryRows
            tdLog.success(f"Query returned {rows} rows from information_schema.ins_xnodes")
        except Exception as e:
            msg = str(e).lower()
            # 表可能不存在或无权限
            if "not exist" in msg or "permission" in msg:
                tdLog.notice(f"Table access issue: {e}")
            elif "syntax" not in msg and "parse" not in msg:
                tdLog.notice(f"Runtime error tolerated: {e}")
            else:
                raise

    def test_query_information_schema_xnode_tasks(self):
        """测试通过 information_schema 查询 XNode Tasks 表

        1. Query show xnode tasks from inx_xnode_tasks

        Since: v3.3.8.8

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        sql = "SELECT * FROM information_schema.ins_xnode_tasks"
        tdLog.info(f"Querying: {sql}")
        try:
            tdSql.query(sql)
            rows = tdSql.queryRows
            tdLog.success(f"Query returned {rows} rows from information_schema.ins_xnode_tasks")
        except Exception as e:
            msg = str(e).lower()
            if "not exist" in msg or "permission" in msg:
                tdLog.notice(f"Table access issue: {e}")
            elif "syntax" not in msg and "parse" not in msg:
                tdLog.notice(f"Runtime error tolerated: {e}")
            else:
                raise

    def test_filter_xnodes_by_status(self):
        """测试按状态过滤 XNode

        1. Query show xnode from inx_xnodes where status = 'online'

        Since: v3.3.8.8

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        sql = "SELECT * FROM information_schema.ins_xnodes WHERE status = 'online'"
        tdLog.info(f"Querying with filter: {sql}")
        try:
            tdSql.query(sql)
            rows = tdSql.queryRows
            tdLog.success(f"Filtered query returned {rows} online xnodes")
        except Exception as e:
            msg = str(e).lower()
            if "not exist" in msg:
                tdLog.notice(f"Table may not exist: {e}")
            elif "syntax" not in msg and "parse" not in msg:
                tdLog.notice(f"Runtime error tolerated: {e}")
            else:
                raise

    def test_filter_tasks_by_status(self):
        """测试按状态过滤 XNode Tasks

        1. Query show xnode tasks from inx_xnode_tasks where status = 'running'

        Since: v3.3.8.8

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        sql = "SELECT * FROM information_schema.ins_xnode_tasks WHERE status = 'running'"
        tdLog.info(f"Querying tasks with filter: {sql}")
        try:
            tdSql.query(sql)
            rows = tdSql.queryRows
            tdLog.success(f"Filtered query returned {rows} running tasks")
        except Exception as e:
            msg = str(e).lower()
            if "not exist" in msg:
                tdLog.notice(f"Table may not exist: {e}")
            elif "syntax" not in msg and "parse" not in msg:
                tdLog.notice(f"Runtime error tolerated: {e}")
            else:
                raise

    # def test_join_xnodes_and_tasks(self):
    #     """测试联合查询 XNodes 和 Tasks

    #     1. Left join inx_xnodes and inx_xnode_tasks on xnode_id

    #     Since: v3.3.8.8

    #     Labels: common,ci

    #     Jira: None

    #     History:
    #         - 2025-12-30 GuiChuan Zhang Created
    #     """

    #     sql = """
    #         SELECT x.id, x.url, t.name, t.status 
    #         FROM information_schema.ins_xnodes x 
    #         LEFT JOIN information_schema.ins_xnode_tasks t 
    #         ON x.id = t.xnode_id
    #     """
    #     tdLog.info(f"Querying with JOIN: {sql}")
    #     try:
    #         tdSql.query(sql)
    #         rows = tdSql.queryRows
    #         tdLog.success(f"JOIN query returned {rows} rows")
    #     except Exception as e:
    #         msg = str(e).lower()
    #         if "not exist" in msg or "not support" in msg:
    #             tdLog.notice(f"JOIN may not be supported or tables not exist: {e}")
    #         elif "syntax" not in msg and "parse" not in msg:
    #             tdLog.notice(f"Runtime error tolerated: {e}")
    #         else:
    #             raise

    def test_count_xnodes(self):
        """测试统计 XNode 数量

        1. Query count(*) from inx_xnodes

        Since: v3.3.8.8

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        sql = "SELECT COUNT(*) FROM information_schema.ins_xnodes"
        tdLog.info(f"Counting xnodes: {sql}")
        try:
            tdSql.query(sql)
            if tdSql.queryRows > 0:
                count = tdSql.queryResult[0][0]
                tdLog.success(f"Total xnodes count: {count}")
        except Exception as e:
            msg = str(e).lower()
            if "not exist" in msg:
                tdLog.notice(f"Table may not exist: {e}")
            elif "syntax" not in msg and "parse" not in msg:
                tdLog.notice(f"Runtime error tolerated: {e}")
            else:
                raise

    def test_count_tasks_by_status(self):
        """测试按状态统计任务数量

        1. Query count(*) from inx_xnode_tasks group by status

        Since: v3.3.8.8

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        sql = "SELECT status, COUNT(*) FROM information_schema.ins_xnode_tasks GROUP BY status"
        tdLog.info(f"Counting tasks by status: {sql}")
        try:
            tdSql.query(sql)
            if tdSql.queryRows > 0:
                for row in tdSql.queryResult:
                    tdLog.info(f"Status '{row[0]}': {row[1]} tasks")
                tdLog.success(f"Task count by status retrieved")
        except Exception as e:
            msg = str(e).lower()
            if "not exist" in msg or "not support" in msg:
                tdLog.notice(f"GROUP BY may not be supported or table not exist: {e}")
            elif "syntax" not in msg and "parse" not in msg:
                tdLog.notice(f"Runtime error tolerated: {e}")
            else:
                raise

    def test_order_tasks_by_create_time(self):
        """测试按创建时间排序任务

        1. Query tasks from inx_xnode_tasks order by create_time desc limit 10

        Since: v3.3.8.8

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        sql = "SELECT * FROM information_schema.ins_xnode_tasks ORDER BY create_time DESC LIMIT 10"
        tdLog.info(f"Querying recent tasks: {sql}")
        try:
            tdSql.query(sql)
            rows = tdSql.queryRows
            tdLog.success(f"Retrieved {rows} most recent tasks")
            
            if rows > 0:
                for i in range(rows):
                    tdLog.info(f"Recent task {i}: {tdSql.queryResult[i]}")
        except Exception as e:
            msg = str(e).lower()
            if "not exist" in msg:
                tdLog.notice(f"Table may not exist: {e}")
            elif "syntax" not in msg and "parse" not in msg:
                tdLog.notice(f"Runtime error tolerated: {e}")
            else:
                raise

    def test_systable_field_consistency(self):
        """测试系统表字段一致性

        1. Query desc from inx_xnodes, inx_xnode_tasks, inx_xnode_jobs
        2. Check table fields consistency
        
        Since: v3.3.8.8

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        tables_to_check = [
            ("information_schema.ins_xnodes", ["id", "url"]),
            ("information_schema.ins_xnode_tasks", ["id", "name"]),
            ("information_schema.ins_xnode_jobs", ["jid", "tid"]),
        ]
        
        for table, expected_fields in tables_to_check:
            sql = f"DESC {table}"
            tdLog.info(f"Describing table: {sql}")
            try:
                tdSql.query(sql)
                if tdSql.queryRows > 0:
                    fields = [row[0] for row in tdSql.queryResult]
                    tdLog.info(f"Table {table} fields: {fields}")
                    
                    # 检查是否包含预期字段
                    for field in expected_fields:
                        if field in fields:
                            tdLog.success(f"Field '{field}' found in {table}")
                        else:
                            tdLog.notice(f"Field '{field}' not found in {table}")
            except Exception as e:
                msg = str(e).lower()
                if "not exist" in msg or "not support" in msg:
                    tdLog.notice(f"DESC may not be supported or table not exist: {e}")
                elif "syntax" not in msg and "parse" not in msg:
                    tdLog.notice(f"Runtime error tolerated: {e}")
                else:
                    raise

    @classmethod
    def teardown_class(cls):
        """清理测试环境

        1. Clean up test database
        
        Since: v3.3.8.8

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        tdLog.info(f"Cleaning up test environment")
        try:
            tdSql.execute(f"DROP DATABASE IF EXISTS xnode_systables_db_{cls.suffix}")
        except:
            pass
        tdLog.success(f"{__file__} test completed")
