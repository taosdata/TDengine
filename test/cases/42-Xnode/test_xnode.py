import random
import uuid
import time

from new_test_framework.utils import tdLog, tdSql


class TestXnode:
    """
    Robust xnode syntax coverage with dynamic identifiers.

    Goals:
    - Cover parser rules for CREATE/DROP/DRAIN XNODE
    - Cover TASK/AGENT/JOB create/alter/start/stop/rebalance/drop/show
    - Allow runtime/semantic failures while failing on syntax errors only
    """

    replicaVar = 1

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.suffix = uuid.uuid4().hex[:6]
        cls.rand_ids = [random.randint(1000, 9999) for _ in range(8)]
        tdSql.prepare(
            dbname=f"xnode_db_{cls.suffix}", drop=True, replica=cls.replicaVar
        )

    # Helpers -----------------------------------------------------------------

    def no_syntax_fail_execute(self, sql: str):
        """test no syntax fail

        1. execute sql

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-08 GuiChuan Zhang Created
        """
        try:
            tdSql.execute(sql)
            return True
        except Exception as err:  # tolerate runtime errors, only fail on syntax/parse
            msg = str(err).lower()
            assert (
                "syntax" not in msg and "parse" not in msg
            ), f"Syntax failure for [{sql}]: {err}"
            tdLog.notice(f"runtime/semantic error tolerated for sql: {sql} | {err}")
            return False

    def no_syntax_fail_query(self, sql: str):
        """test no syntax fail query

        1. query sql

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-08 GuiChuan Zhang Created
        """
        try:
            tdSql.query(sql)
            return True
        except Exception as err:
            msg = str(err).lower()
            assert (
                "syntax" not in msg and "parse" not in msg
            ), f"Syntax failure for [{sql}]: {err}"
            tdLog.notice(f"runtime/semantic error tolerated for query: {sql} | {err}")
            return False

    # Tests -------------------------------------------------------------------

    def test_show_primitives(self):
        """测试显示 SHOW STMT 语句

        1. Query show xnodes, xnode tasks, xnode agents, xnode jobs

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        show_sqls = [
            "SHOW XNODES",
            "SHOW XNODE TASKS",
            "SHOW XNODE AGENTS",
            "SHOW XNODE JOBS",
        ]
        for sql in show_sqls:
            tdLog.debug(f"query: {sql}")
            self.no_syntax_fail_query(sql)

    def test_xnode_crud(self):
        """测试 XNode CRUD 操作

        1. Drop xnode force by endpoint and id
        2. Create xnode with endpoint and user/pass
        3. Create xnode with id only
        4. Drop xnode by endpoint and id
        5. Drain xnode by id

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        ep1 = f"xn-{self.suffix}-1:6050"
        ep2 = f"xn-{self.suffix}-2:6051"
        node_id = self.rand_ids[0]

        sqls = [
            f"DROP XNODE FORCE '{ep1}'",
            f"DROP XNODE FORCE {node_id}",
            # f"CREATE XNODE '{ep1}' USER __xnode__ PASS 'Ab{self.suffix}!'",
            f"CREATE XNODE '{ep1}' USER root PASS 'taosdata'",
            f"CREATE XNODE '{ep2}'",
            f"SHOW XNODES where id > 1",
            f"SHOW XNODES where status != 'online'",
            f"SHOW XNODES where status = 'online'",
            f"SHOW XNODES where url = '{ep2}'",
            f"DROP XNODE '{ep2}'",
            f"DROP XNODE {node_id}",
            f"DRAIN XNODE {self.rand_ids[1]}",
        ]
        for sql in sqls:
            tdLog.debug(f"exec: {sql}")
            self.no_syntax_fail_execute(sql)

    def test_task_lifecycle(self):
        """测试 XNode Task 生命周期

        1. Create xnode task with FROM/TO and WITH options (comma separated + trigger)
        2. Create xnode task with topic source and DSN sink using AND
        3. Create xnode task without FROM/TO but with options only
        4. Alter xnode task with new source/sink/options
        5. Alter xnode task with options only
        6. Start/stop xnode task
        7. Rebalance xnode job by id with options
        8. Rebalance xnode job by where clause
        9. Drop xnode task variations

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        dbname = f"xnode_db_{self.suffix}"
        t_ingest = f"t_ingest_{self.suffix}"
        t_export = f"t_export_{self.suffix}"
        t_opts = f"t_opts_{self.suffix}"
        topic = f"tp_{self.suffix}"
        task_id_1 = self.rand_ids[2]
        task_id_2 = self.rand_ids[3]

        task_sqls = [
            # create with FROM/TO and WITH options (comma separated + trigger)
            f"CREATE XNODE TASK '{t_ingest}' FROM 'mqtt://broker:1883' TO DATABASE {dbname} "
             "WITH parser 'parser_json', batch 1024, TRIGGER 'manual'",
            f"CREATE XNODE TASK '{t_ingest}' FROM 'mqtt://broker:1883' TO DATABASE {dbname} "
             "WITH parser 'parser_json', batch 1024, TRIGGER 'manual' labels ''",
            f"CREATE XNODE TASK '{t_ingest}' FROM 'mqtt://broker:1883' TO DATABASE {dbname} "
             "WITH parser 'parser_json', batch 1024, TRIGGER 'manual' labels '{\"key\":\"value\"}'",
            # create with topic source and DSN sink using AND
            f"CREATE XNODE TASK '{t_export}' FROM TOPIC {topic} TO 'kafka://broker:9092' "
             "WITH group_id 'g1' AND client_id 'c1'",
            # create task without FROM/TO but with options only
            f"CREATE XNODE TASK '{t_opts}' WITH retry 5 AND TRIGGER 'cron_5m'",

            # alter with new source/sink/options
            f"ALTER XNODE TASK {task_id_1} FROM DATABASE {dbname} TO 'influxdb://remote' "
             "WITH parser 'parser_line', concurrency 4",
            # alter task with options only
            f"ALTER XNODE TASK {task_id_2} WITH batch 2048 AND timeout 30",
            f'ALTER XNODE TASK {task_id_2} WITH batch 2048 AND timeout 30 labels \'{{"k":"v"}}\'',
            # start/stop task
            f"START XNODE TASK {task_id_1}",
            f"STOP XNODE TASK {task_id_1}",
            # rebalance by id with options
            f"REBALANCE XNODE JOB {self.rand_ids[4]} WITH xnode_id 3",
            # rebalance by where clause
            f"REBALANCE XNODE JOB WHERE id > 0",
            # drop variations
            f"DROP XNODE TASK '{t_ingest}'",
            f"DROP XNODE TASK {task_id_2}",

            #f"DROP XNODE TASK WHERE name = '{t_export}'",
            #f"DROP XNODE TASK ON 1 WHERE id = {task_id_1}",
        ]
        for sql in task_sqls:
            tdLog.debug(f"exec: {sql}")
            self.no_syntax_fail_execute(sql)

    def test_agent_lifecycle(self):
        """test no syntax fail query

        1. agent action: create, drop, alter
        2. execute by no_syntax_fail_execute function

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-08 GuiChuan Zhang Created
        """
        agent1 = f"a1_{self.suffix}"
        agent2 = f"a2_{self.suffix}"

        agent_sqls = [
            f"DROP XNODE AGENT '{agent1}'",
            f"CREATE XNODE AGENT '{agent1}' WITH `ttl` '1y', ipwhitelist '127.0.0.1'",
            f"CREATE XNODE AGENT '{agent2}' WITH `regionA` 'cn-north-1' AND TRIGGER 'heartbeat'",
            f"ALTER XNODE AGENT '{agent2}' WITH status 'running', `regionA` 'cn-north-1' AND TRIGGER 'heartbeat'",
            f"ALTER XNODE AGENT '{agent2}' WITH status 'stop' `regionA` 'cn-north-1' TRIGGER 'heartbeat'",
            f"DROP XNODE AGENT '{agent2}'",
        ]
        for sql in agent_sqls:
            tdLog.debug(f"exec: {sql}")
            self.no_syntax_fail_execute(sql)

    def test_job_lifecycle(self):
        """测试 XNode Job 生命周期

        1. Drop xnode job by id
        2. Create xnode job on xnode with config
        3. Alter xnode job with new trigger/priority
        4. Start/stop xnode job
        5. Rebalance xnode job by id with options
        6. Rebalance xnode job by where clause
        7. Drop xnode job variations

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        job_on = self.rand_ids[5]
        job_id = self.rand_ids[6]

        job_sqls = [
            f"DROP XNODE JOB {job_id}",
            f"CREATE XNODE JOB ON {job_on} WITH config '{{\"json\":true}}'",
            #f"ALTER XNODE JOB {job_id} WITH TRIGGER 'manual', priority 10",
            f"REBALANCE XNODE JOB {job_id} WITH xnode_id {job_on}",
            f"REBALANCE XNODE JOB WHERE jid >= 1",
            #f"DROP XNODE JOB ON {job_on} WHERE jid = {job_id}",
            "DROP XNODE JOB WHERE jid > 1",
            "DROP XNODE JOB WHERE task_id = 2 and status = 'running'",
        ]

        for sql in job_sqls:
            tdLog.debug(f"exec: {sql}")
            self.no_syntax_fail_execute(sql)

    def test_sources_and_sinks_variants(self):
        """测试 XNode 任务源和 sink 变体

        1. Create xnode task with database source and sink
        2. Create xnode task with topic source and database sink
        3. Create xnode task with string source and sink
        4. Alter xnode task with new sink and options
        5. Drop xnode task variations

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        dbname = f"xnode_db_{self.suffix}"
        t_db = f"t_db_{self.suffix}"
        t_topic = f"t_topic_{self.suffix}"
        t_str = f"t_str_{self.suffix}"
        alt_task_id = self.rand_ids[7]

        sqls = [
            # f"CREATE XNODE TASK '{t_db}' FROM DATABASE {dbname} TO DATABASE {dbname} "
            # "WITH mode 'full' AND window 60",
            f"CREATE XNODE TASK '{t_topic}' FROM TOPIC tp2_{self.suffix} TO DATABASE {dbname} WITH batch 500",
            f"CREATE XNODE TASK '{t_str}' FROM 'dsn://source' TO 'dsn://sink' WITH k1 'v1', k2 = 2",
            f"ALTER XNODE TASK {alt_task_id} TO 'dsn://sink2' WITH retry 3",
            f"DROP XNODE TASK '{t_db}'",
            f"DROP XNODE TASK '{t_topic}'",
            f"DROP XNODE TASK '{t_str}'",
        ]
        for sql in sqls:
            tdLog.debug(f"exec: {sql}")
            self.no_syntax_fail_execute(sql)

    def test_show_with_where_conditions(self):
        """测试 SHOW XNODE 语句的 WHERE 条件

        1. Show xnode tasks with various where conditions
        2. Show xnode jobs with various where conditions
        3. Show xnode agents with various where conditions
        4. Complex where conditions with AND/OR operators
        5. Where conditions with different comparison operators

        Since: v3.4.0.1

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-15 GuiChuan Zhang Created
        """

        # 测试 TASK 表的 WHERE 条件
        task_where_sqls = [
            "SHOW XNODE TASKS WHERE id > 1",
            "SHOW XNODE TASKS WHERE id >= 100",
            "SHOW XNODE TASKS WHERE id < 9999",
            "SHOW XNODE TASKS WHERE id <= 5000",
            "SHOW XNODE TASKS WHERE id = 123",
            "SHOW XNODE TASKS WHERE id != 456",
            "SHOW XNODE TASKS WHERE name = 'test_task'",
            "SHOW XNODE TASKS WHERE name != 'old_task'",
            "SHOW XNODE TASKS WHERE name LIKE 'ingest%'",
            "SHOW XNODE TASKS WHERE status = 'running'",
            "SHOW XNODE TASKS WHERE status != 'stopped'",
            "SHOW XNODE TASKS WHERE status IN ('running', 'pending')",
            "SHOW XNODE TASKS WHERE via = 1",
            "SHOW XNODE TASKS WHERE xnode_id = 1001",
            "SHOW XNODE TASKS WHERE created_by = 'admin'",
            "SHOW XNODE TASKS WHERE create_time > '2024-01-01 00:00:00'",
            "SHOW XNODE TASKS WHERE update_time <= NOW()",
            "SHOW XNODE TASKS WHERE id > 10 AND status = 'running'",
            "SHOW XNODE TASKS WHERE name LIKE 'test%' AND via = 1",
            "SHOW XNODE TASKS WHERE status = 'running' OR status = 'pending'",
            "SHOW XNODE TASKS WHERE id BETWEEN 1 AND 100",
            "SHOW XNODE TASKS WHERE create_time > '2024-01-01' AND update_time < NOW()",
        ]

        # 测试 JOB 表的 WHERE 条件
        job_where_sqls = [
            "SHOW XNODE JOBS WHERE id > 1",
            "SHOW XNODE JOBS WHERE id >= 50",
            "SHOW XNODE JOBS WHERE id < 1000",
            "SHOW XNODE JOBS WHERE id <= 500",
            "SHOW XNODE JOBS WHERE id = 789",
            "SHOW XNODE JOBS WHERE id != 999",
            "SHOW XNODE JOBS WHERE task_id = 123",
            "SHOW XNODE JOBS WHERE task_id != 456",
            "SHOW XNODE JOBS WHERE task_id > 10",
            "SHOW XNODE JOBS WHERE status = 'success'",
            "SHOW XNODE JOBS WHERE status != 'failed'",
            "SHOW XNODE JOBS WHERE status IN ('success', 'running')",
            "SHOW XNODE JOBS WHERE via = 2",
            "SHOW XNODE JOBS WHERE xnode_id = 1002",
            "SHOW XNODE JOBS WHERE create_time > '2024-01-01 00:00:00'",
            "SHOW XNODE JOBS WHERE update_time <= NOW()",
            "SHOW XNODE JOBS WHERE task_id = 123 AND status = 'success'",
            "SHOW XNODE JOBS WHERE xnode_id = 1 AND via = 2",
            "SHOW XNODE JOBS WHERE status = 'failed' OR status = 'error'",
            "SHOW XNODE JOBS WHERE id BETWEEN 100 AND 200",
            "SHOW XNODE JOBS WHERE create_time > '2024-01-01' AND update_time < NOW()",
        ]

        # 测试 AGENT 表的 WHERE 条件
        agent_where_sqls = [
            "SHOW XNODE AGENTS WHERE id > 1",
            "SHOW XNODE AGENTS WHERE id >= 10",
            "SHOW XNODE AGENTS WHERE id < 100",
            "SHOW XNODE AGENTS WHERE id <= 50",
            "SHOW XNODE AGENTS WHERE id = 111",
            "SHOW XNODE AGENTS WHERE id != 222",
            "SHOW XNODE AGENTS WHERE name = 'agent_1'",
            "SHOW XNODE AGENTS WHERE name != 'old_agent'",
            "SHOW XNODE AGENTS WHERE name LIKE 'agent%'",
            "SHOW XNODE AGENTS WHERE status = 'active'",
            "SHOW XNODE AGENTS WHERE status != 'inactive'",
            "SHOW XNODE AGENTS WHERE status IN ('active', 'running')",
            "SHOW XNODE AGENTS WHERE create_time > '2024-01-01 00:00:00'",
            "SHOW XNODE AGENTS WHERE update_time <= NOW()",
            "SHOW XNODE AGENTS WHERE name = 'test_agent' AND status = 'active'",
            "SHOW XNODE AGENTS WHERE id > 5 AND status != 'stopped'",
            "SHOW XNODE AGENTS WHERE status = 'active' OR status = 'running'",
            "SHOW XNODE AGENTS WHERE id BETWEEN 1 AND 50",
            "SHOW XNODE AGENTS WHERE create_time > '2024-01-01' AND update_time < NOW()",
        ]

        # 创建测试数据
        test_task1 = f"task_1_{self.suffix}"
        test_task2 = f"task_2_{self.suffix}"
        test_task3 = f"task_ingest_{self.suffix}"
        test_agent1 = f"agent_1_{self.suffix}"
        test_agent2 = f"agent_2_{self.suffix}"
        dbname = f"xnode_db_{self.suffix}"

        # 创建测试任务
        create_sqls = [
            f"CREATE XNODE TASK '{test_task1}' FROM 'mqtt://broker1:1883' TO DATABASE {dbname} "
            "WITH parser 'parser_json', batch 1024, TRIGGER 'manual' "
            "labels '{\"env\":\"test\",\"type\":\"ingest\"}'",

            f"CREATE XNODE TASK '{test_task2}' FROM TOPIC tp1_{self.suffix} TO 'kafka://broker:9092' "
            "WITH group_id 'g1', client_id 'c1', TRIGGER 'auto' "
            "labels '{\"env\":\"prod\"}'",

            f"CREATE XNODE TASK '{test_task3}' FROM 'http://source' TO DATABASE {dbname} "
            "WITH parser 'parser_csv', batch 512, TRIGGER 'cron_5m'",

            # 创建测试 agent
            f"CREATE XNODE AGENT '{test_agent1}' WITH `regionA` 'cn-north-1', `ttl` '1y', ipwhitelist '127.0.0.1'",
            f"CREATE XNODE AGENT '{test_agent2}' WITH `regionA` 'cn-south-1', status 'active'",
        ]

        for sql in create_sqls:
            tdLog.debug(f"create test data: {sql}")
            self.no_syntax_fail_execute(sql)

        # 修改测试用例，使用实际创建的测试数据
        task_where_sqls = [
            f"SHOW XNODE TASKS WHERE name = '{test_task1}'",
            f"SHOW XNODE TASKS WHERE name != '{test_task2}'",
            f"SHOW XNODE TASKS WHERE name LIKE 'task__{self.suffix}'",
            f"SHOW XNODE TASKS WHERE name LIKE 'task_ingest%'",
            "SHOW XNODE TASKS WHERE status = 'running'",
            "SHOW XNODE TASKS WHERE status != 'stopped'",
            "SHOW XNODE TASKS WHERE status IN ('running', 'pending')",
            "SHOW XNODE TASKS WHERE via = 1",
            "SHOW XNODE TASKS WHERE xnode_id = 1001",
            "SHOW XNODE TASKS WHERE created_by = 'admin'",
            "SHOW XNODE TASKS WHERE create_time > '2024-01-01 00:00:00'",
            "SHOW XNODE TASKS WHERE update_time <= NOW()",
            f"SHOW XNODE TASKS WHERE name = '{test_task1}' AND via = 1",
            "SHOW XNODE TASKS WHERE status = 'running' OR status = 'pending'",
            "SHOW XNODE TASKS WHERE id BETWEEN 1 AND 100",
            "SHOW XNODE TASKS WHERE create_time > '2024-01-01' AND update_time < NOW()",
        ]

        agent_where_sqls = [
            f"SHOW XNODE AGENTS WHERE name = '{test_agent1}'",
            f"SHOW XNODE AGENTS WHERE name != '{test_agent2}'",
            f"SHOW XNODE AGENTS WHERE name LIKE 'agent__{self.suffix}'",
            "SHOW XNODE AGENTS WHERE status = 'active'",
            "SHOW XNODE AGENTS WHERE status != 'inactive'",
            "SHOW XNODE AGENTS WHERE status IN ('active', 'running')",
            "SHOW XNODE AGENTS WHERE create_time > '2024-01-01 00:00:00'",
            "SHOW XNODE AGENTS WHERE update_time <= NOW()",
            f"SHOW XNODE AGENTS WHERE name = '{test_agent1}' AND status = 'active'",
            "SHOW XNODE AGENTS WHERE id > 5 AND status != 'stopped'",
            "SHOW XNODE AGENTS WHERE status = 'active' OR status = 'running'",
            "SHOW XNODE AGENTS WHERE id BETWEEN 1 AND 50",
            "SHOW XNODE AGENTS WHERE create_time > '2024-01-01' AND update_time < NOW()",
        ]

        # 执行所有测试用例
        all_sqls = task_where_sqls + job_where_sqls + agent_where_sqls
        for sql in all_sqls:
            tdLog.debug(f"query: {sql}")
            self.no_syntax_fail_query(sql)

        # 清理测试数据
        cleanup_sqls = [
            f"DROP XNODE TASK '{test_task1}'",
            f"DROP XNODE TASK '{test_task2}'",
            f"DROP XNODE TASK '{test_task3}'",
            f"DROP XNODE AGENT '{test_agent1}'",
            f"DROP XNODE AGENT '{test_agent2}'",
        ]

        for sql in cleanup_sqls:
            tdLog.debug(f"cleanup: {sql}")
            self.no_syntax_fail_execute(sql)

    def test_show_where_complex_conditions(self):
        """测试 SHOW XNODE 语句的复杂 WHERE 条件

        1. Create test data with various attributes
        2. 多层嵌套条件 with parentheses
        3. 复杂 AND/OR 组合
        4. 模糊查询 with LIKE patterns
        5. 时间范围查询
        6. ID 范围查询
        7. Clean up test data

        Since: v3.4.0.1

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-15 GuiChuan Zhang Created
        """

        # 创建测试数据
        test_task1 = f"complex_task_1_{self.suffix}"
        test_task2 = f"complex_task_2_{self.suffix}"
        test_task3 = f"ingest_data_{self.suffix}"
        test_agent1 = f"worker_agent_1_{self.suffix}"
        test_agent2 = f"backup_agent_{self.suffix}"
        dbname = f"xnode_db_{self.suffix}"

        # 创建测试任务和代理
        create_sqls = [
            f"CREATE XNODE TASK '{test_task1}' FROM 'mqtt://broker1:1883' TO DATABASE {dbname} "
            "WITH parser 'parser_json', batch 2048, TRIGGER 'manual' "
            "labels '{\"env\":\"prod\",\"type\":\"critical\",\"owner\":\"admin\"}'",

            f"CREATE XNODE TASK '{test_task2}' FROM TOPIC tp2_{self.suffix} TO 'kafka://broker:9092' "
            "WITH group_id 'g2', client_id 'c2', TRIGGER 'auto' "
            "labels '{\"env\":\"test\",\"type\":\"standard\"}'",

            f"CREATE XNODE TASK '{test_task3}' FROM 'http://api.source' TO DATABASE {dbname} "
            "WITH parser 'parser_xml', batch 1024, TRIGGER 'cron_10m' "
            "labels '{\"env\":\"prod\",\"type\":\"ingest\"}'",

            # 创建测试 agent
            f"CREATE XNODE AGENT '{test_agent1}' WITH `regionA` 'cn-east-1', `ttl` '2y', status 'active'",
            f"CREATE XNODE AGENT '{test_agent2}' WITH `regionA` 'cn-west-1', `ttl` '1y', status 'standby'",
        ]

        for sql in create_sqls:
            tdLog.debug(f"create test data: {sql}")
            self.no_syntax_fail_execute(sql)

        complex_where_sqls = [
            # 复杂 TASK 查询 - 使用实际创建的测试数据
            f"SHOW XNODE TASKS WHERE (id > 10 AND status = 'running') OR (id < 5 AND status = 'pending')",
            f"SHOW XNODE TASKS WHERE name LIKE 'complex_task_%' AND (via = 1 OR via = 2)",
            f"SHOW XNODE TASKS WHERE name LIKE 'ingest_data%' AND status = 'running'",
            f"SHOW XNODE TASKS WHERE name IN ('{test_task1}', '{test_task2}') AND create_time > '2024-01-01'",
            f"SHOW XNODE TASKS WHERE name != '' AND created_by = 'admin' AND update_time > create_time",
            f"SHOW XNODE TASKS WHERE (id BETWEEN 1 AND 100) AND name LIKE '%{self.suffix}'",
            f"SHOW XNODE TASKS WHERE labels LIKE '%critical%' AND status = 'running'",

            # 复杂 AGENT 查询 - 使用实际创建的测试数据
            f"SHOW XNODE AGENTS WHERE (name LIKE 'worker_agent%' OR name LIKE 'backup_%') AND status = 'active'",
            f"SHOW XNODE AGENTS WHERE name = '{test_agent1}' AND (create_time > '2024-01-01' OR update_time > '2024-01-01')",
            f"SHOW XNODE AGENTS WHERE status IN ('active', 'running', 'ready') AND update_time > create_time",
            f"SHOW XNODE AGENTS WHERE (id BETWEEN 1 AND 100) AND name != '' AND status != 'stopped'",
            #f"SHOW XNODE AGENTS WHERE `regionA` = 'cn-east-1' AND `ttl` = '2y'",
            f"SHOW XNODE AGENTS WHERE name LIKE '%{self.suffix}' AND status != 'inactive'",
        ]

        for sql in complex_where_sqls:
            tdLog.debug(f"query: {sql}")
            self.no_syntax_fail_query(sql)

        # 清理测试数据
        cleanup_sqls = [
            f"DROP XNODE TASK '{test_task1}'",
            f"DROP XNODE TASK '{test_task2}'",
            f"DROP XNODE TASK '{test_task3}'",
            f"DROP XNODE AGENT '{test_agent1}'",
            f"DROP XNODE AGENT '{test_agent2}'",
        ]

        for sql in cleanup_sqls:
            tdLog.debug(f"cleanup: {sql}")
            self.no_syntax_fail_execute(sql)

    def test_show_where_edge_cases(self):
        """测试 SHOW XNODE 语句的边界条件 WHERE 条件

        1. Create test data with special characteristics
        2. 空值检查
        3. 特殊字符处理
        4. 极限值测试
        5. 格式边界测试
        6. Clean up test data

        Since: v3.4.0.1

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-15 GuiChuan Zhang Created
        """

        # 创建测试数据
        test_task_edge = f"edge_task_{self.suffix}"
        test_task_long = f"very_long_task_name_with_special_chars_{self.suffix}"
        test_agent_edge = f"edge_agent_{self.suffix}"
        test_agent_special = f"special_agent_{self.suffix}"
        dbname = f"xnode_db_{self.suffix}"

        # 创建边界测试数据
        create_sqls = [
            f"CREATE XNODE TASK '{test_task_edge}' FROM 'mqtt://edge.broker:1883' TO DATABASE {dbname} "
            "WITH parser 'parser_edge', batch 1, TRIGGER 'manual' "
            "labels '{\"test\":\"edge_case\",\"empty_reason\":\"\"}'",

            f"CREATE XNODE TASK '{test_task_long}' FROM 'http://very.long.source.url.with.parameters/api/data' "
            "TO 'kafka://very.long.destination.url.with.parameters:9092/topic' "
            "WITH parser 'parser_complex', batch 99999, TRIGGER 'auto' "
            "labels '{\"environment\":\"production\",\"type\":\"data_processing\"}'",

            # 创建边界测试 agent
            f"CREATE XNODE AGENT '{test_agent_edge}' WITH `regionA` 'edge-region', `ttl` '1d', status 'active'",
            f"CREATE XNODE AGENT '{test_agent_special}' WITH `regionA` 'special@region#123', `ttl` '100y', status 'inactive'",
        ]

        for sql in create_sqls:
            tdLog.debug(f"create edge case test data: {sql}")
            self.no_syntax_fail_execute(sql)

        edge_case_sqls = [
            # 空值和特殊值测试 - 使用实际创建的测试数据
            f"SHOW XNODE TASKS WHERE name = '{test_task_edge}' AND name IS NOT NULL",
            #f"SHOW XNODE TASKS WHERE name = '{test_task_long}' AND LENGTH(name) > 50",
            "SHOW XNODE TASKS WHERE reason = '' OR reason IS NULL",
            "SHOW XNODE TASKS WHERE labels != '{}' AND labels IS NOT NULL",
            "SHOW XNODE TASKS WHERE create_time IS NOT NULL AND update_time IS NOT NULL",

            "SHOW XNODE JOBS WHERE config != '{}' AND config IS NOT NULL",
            "SHOW XNODE JOBS WHERE reason IS NOT NULL",
            "SHOW XNODE JOBS WHERE task_id IS NOT NULL AND task_id > 0",
            "SHOW XNODE JOBS WHERE create_time < update_time OR create_time = update_time",

            f"SHOW XNODE AGENTS WHERE name = '{test_agent_edge}' AND name IS NOT NULL",
            f"SHOW XNODE AGENTS WHERE name = '{test_agent_special}' AND `token` != ''",
            "SHOW XNODE AGENTS WHERE status IS NOT NULL AND status IN ('active', 'inactive')",
            "SHOW XNODE AGENTS WHERE create_time <= update_time",

            # 极限值测试
            "SHOW XNODE TASKS WHERE id = 2147483647",  # INT 最大值
            "SHOW XNODE TASKS WHERE id = -2147483648",  # INT 最小值
            #f"SHOW XNODE TASKS WHERE name = '{test_task_long}' AND LENGTH(name) > 10",
            #"SHOW XNODE TASKS WHERE LENGTH(from) > 0 AND LENGTH(to) > 0",
            "SHOW XNODE TASKS WHERE via > 0 AND via < 1000",

            # 格式测试
            "SHOW XNODE TASKS WHERE create_time BETWEEN '2024-01-01' AND '2024-12-31'",
            "SHOW XNODE TASKS WHERE create_time > '2024-01-01 00:00:00' AND create_time < '2025-01-01'",
            "SHOW XNODE JOBS WHERE update_time > '2024-01-01'",
            "SHOW XNODE AGENTS WHERE status REGEXP '^[a-z]+$' OR status = 'active'",
            #"SHOW XNODE AGENTS WHERE `regionA` LIKE '%region%' AND `ttl` > '1d'",
        ]

        for sql in edge_case_sqls:
            tdLog.debug(f"query: {sql}")
            self.no_syntax_fail_query(sql)

        # 清理测试数据
        cleanup_sqls = [
            f"DROP XNODE TASK '{test_task_edge}'",
            f"DROP XNODE TASK '{test_task_long}'",
            f"DROP XNODE AGENT '{test_agent_edge}'",
            f"DROP XNODE AGENT '{test_agent_special}'",
        ]

        for sql in cleanup_sqls:
            tdLog.debug(f"cleanup edge case: {sql}")
            self.no_syntax_fail_execute(sql)

    def test_show_after_operations(self):
        """测试 XNode 操作后的 SHOW 语句

        1. Show xnodes
        2. Show xnode tasks
        3. Show xnode agents
        4. Show xnode jobs

        Since: v3.4.0.1

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-30 GuiChuan Zhang Created
        """

        show_sqls = [
            "SHOW XNODES",
            "SHOW XNODE TASKS",
            "SHOW XNODE AGENTS",
            "SHOW XNODE JOBS",
        ]
        for sql in show_sqls:
            tdLog.debug(f"query: {sql}")
            self.no_syntax_fail_query(sql)
        tdLog.info(f"{__file__} xnode syntax coverage completed")

    def test_drop_xnode_job_where_conditions(self):
        """测试 DROP XNODE JOB WHERE 条件语句

        1. Create test jobs with different attributes
        2. Drop jobs by id condition
        3. Drop jobs by task_id condition
        4. Drop jobs by status condition
        5. Drop jobs by via condition
        6. Drop jobs by xnode_id condition
        7. Drop jobs by time conditions
        8. Drop jobs by config content conditions
        9. Drop jobs by complex AND/OR/NOT conditions
        10. Clean up remaining test data

        Since: v3.4.0.1

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-19 GuiChuan Zhang Created
        """

        dbname = f"xnode_db_{self.suffix}"

        # 创建测试任务
        test_task1 = f"drop_test_task_1_{self.suffix}"
        test_task2 = f"drop_test_task_2_{self.suffix}"
        test_task3 = f"drop_test_task_3_{self.suffix}"

        create_task_sqls = [
            f"CREATE XNODE TASK '{test_task1}' FROM 'mqtt://broker1:1883' TO DATABASE {dbname} "
            "WITH parser 'parser_json', batch 1024, TRIGGER 'manual'",

            f"CREATE XNODE TASK '{test_task2}' FROM TOPIC tp_drop_{self.suffix} TO 'kafka://broker:9092' "
            "WITH group_id 'g_drop', client_id 'c_drop', TRIGGER 'auto'",

            f"CREATE XNODE TASK '{test_task3}' FROM 'http://api.source' TO DATABASE {dbname} "
            "WITH parser 'parser_csv', batch 512, TRIGGER 'cron_5m'",
        ]

        for sql in create_task_sqls:
            tdLog.debug(f"create test task: {sql}")
            self.no_syntax_fail_execute(sql)

        # 先获取任务ID，用于后续创建job
        task_id_1 = 1001  # 假设的任务ID
        task_id_2 = 1002  # 假设的任务ID
        task_id_3 = 1003  # 假设的任务ID

        # 创建测试 jobs - 使用 CREATE XNODE JOB ON task_id WITH config 语法
        create_job_sqls = [
            f"CREATE XNODE JOB ON {task_id_1} WITH config '{{\"timeout\":30,\"retry\":3}}'",
            f"CREATE XNODE JOB ON {task_id_2} WITH config '{{\"timeout\":60,\"retry\":5,\"batch_size\":1000}}'",
            f"CREATE XNODE JOB ON {task_id_3} WITH config '{{\"timeout\":120,\"retry\":1,\"priority\":\"high\"}}'",
            f"CREATE XNODE JOB ON {task_id_1} WITH config '{{\"timeout\":45,\"retry\":2}}'",
            f"CREATE XNODE JOB ON {task_id_2} WITH config '{{\"timeout\":90,\"retry\":3,\"mode\":\"fast\"}}'",
        ]

        for sql in create_job_sqls:
            tdLog.debug(f"create test job: {sql}")
            self.no_syntax_fail_execute(sql)

        # 测试基本的 WHERE 条件 DROP XNODE JOB - 只使用支持的简单比较运算
        basic_where_sqls = [
            # 按 id 条件删除
            "DROP XNODE JOB WHERE id > 100",
            "DROP XNODE JOB WHERE id >= 50",
            "DROP XNODE JOB WHERE id < 1000",
            "DROP XNODE JOB WHERE id <= 500",
            "DROP XNODE JOB WHERE id = 789",
            "DROP XNODE JOB WHERE id != 999",
            # 按 task_id 条件删除
            f"DROP XNODE JOB WHERE task_id = {task_id_1}",
            f"DROP XNODE JOB WHERE task_id != {task_id_2}",
            "DROP XNODE JOB WHERE task_id > 10",
            "DROP XNODE JOB WHERE task_id < 100",
            "DROP XNODE JOB WHERE task_id >= 1",
            "DROP XNODE JOB WHERE task_id <= 999",
            # 按状态条件删除
            "DROP XNODE JOB WHERE status = 'running'",
            "DROP XNODE JOB WHERE status != 'failed'",
            # 按 via 条件删除
            "DROP XNODE JOB WHERE via = 1",
            "DROP XNODE JOB WHERE via != 2",
            "DROP XNODE JOB WHERE via > 0",
            "DROP XNODE JOB WHERE via < 10",
            "DROP XNODE JOB WHERE via >= 1",
            "DROP XNODE JOB WHERE via <= 5",
            # 按 xnode_id 条件删除
            "DROP XNODE JOB WHERE xnode_id = 1001",
            "DROP XNODE JOB WHERE xnode_id != 1002",
            "DROP XNODE JOB WHERE xnode_id > 0",
            "DROP XNODE JOB WHERE xnode_id < 2000",
            "DROP XNODE JOB WHERE xnode_id >= 1",
            "DROP XNODE JOB WHERE xnode_id <= 9999",
            # 按配置内容删除 - 简单字符串比较
            "DROP XNODE JOB WHERE config = '{}'",
            "DROP XNODE JOB WHERE config != '{}'",
            "DROP XNODE JOB WHERE config = ''",
            "DROP XNODE JOB WHERE config != ''",
            # 按原因删除
            "DROP XNODE JOB WHERE reason = ''",
            "DROP XNODE JOB WHERE reason != ''",
        ]

        for sql in basic_where_sqls:
            tdLog.debug(f"drop job with basic where: {sql}")
            self.no_syntax_fail_execute(sql)

        # 测试时间条件 - 简单时间字符串比较
        time_where_sqls = [
            "DROP XNODE JOB WHERE create_time > '2024-01-01 00:00:00'",
            "DROP XNODE JOB WHERE create_time < '2025-01-01 00:00:00'",
            "DROP XNODE JOB WHERE create_time >= '2024-06-01 00:00:00'",
            "DROP XNODE JOB WHERE update_time > '2024-01-01 00:00:00'",
            "DROP XNODE JOB WHERE update_time < '2025-01-01 00:00:00'",
            "DROP XNODE JOB WHERE update_time >= '2024-06-01 00:00:00'",
            "DROP XNODE JOB WHERE create_time != '1970-01-01 00:00:00'",
            "DROP XNODE JOB WHERE update_time != '1970-01-01 00:00:00'",
        ]

        for sql in time_where_sqls:
            tdLog.debug(f"drop job with time where: {sql}")
            self.no_syntax_fail_execute(sql)

        # 测试复杂 AND/OR/NOT 条件 - 只使用支持的逻辑运算
        complex_where_sqls = [
            # 简单 AND 组合
            f"DROP XNODE JOB WHERE task_id = {task_id_1} AND status = 'running'",
            "DROP XNODE JOB WHERE via = 1 AND xnode_id = 1001",
            "DROP XNODE JOB WHERE status = 'success' AND create_time > '2024-01-01'",
            f"DROP XNODE JOB WHERE task_id = {task_id_2} AND via != 0",
            # 简单 OR 组合
            "DROP XNODE JOB WHERE status = 'running' OR status = 'pending'",
            "DROP XNODE JOB WHERE via = 1 OR via = 2",
            f"DROP XNODE JOB WHERE task_id = {task_id_1} OR task_id = {task_id_2}",
            "DROP XNODE JOB WHERE xnode_id = 1001 OR xnode_id = 1002",
            # AND + OR 组合
            "DROP XNODE JOB WHERE (status = 'running' OR status = 'pending') AND via = 1",
            "DROP XNODE JOB WHERE status = 'success' AND (xnode_id = 1001 OR xnode_id = 1002)",
            "DROP XNODE JOB WHERE (task_id > 10 AND task_id < 100) OR status = 'failed'",
            f"DROP XNODE JOB WHERE (task_id = {task_id_1} OR task_id = {task_id_2}) AND status != 'error'",
            # NOT 条件
            "DROP XNODE JOB WHERE NOT status = 'failed'",
            "DROP XNODE JOB WHERE NOT id < 0",
            "DROP XNODE JOB WHERE NOT (status = 'error' AND via = 0)",
            "DROP XNODE JOB WHERE NOT task_id = 999",
            # 多层逻辑组合
            "DROP XNODE JOB WHERE (status = 'running' OR status = 'pending') AND (via = 1 OR via = 2)",
            "DROP XNODE JOB WHERE (id > 100 AND id < 1000) OR (status = 'failed' AND via != 0)",
            "DROP XNODE JOB WHERE NOT (status = 'error' OR status = 'failed')",
            f"DROP XNODE JOB WHERE (task_id = {task_id_1} OR task_id = {task_id_2}) AND NOT status = 'error'",
            # 复杂括号组合
            "DROP XNODE JOB WHERE ((status = 'running' OR status = 'pending') AND via = 1) OR id > 1000",
            "DROP XNODE JOB WHERE (status = 'success' AND create_time > '2024-01-01') OR (status = 'failed' AND update_time < '2025-01-01')",
            "DROP XNODE JOB WHERE (task_id > 0 AND task_id < 2000) AND (status != 'error' OR via >= 1)",
        ]

        for sql in complex_where_sqls:
            tdLog.debug(f"drop job with complex where: {sql}")
            self.no_syntax_fail_execute(sql)

        # 清理测试任务
        cleanup_sqls = [
            f"DROP XNODE TASK '{test_task1}'",
            f"DROP XNODE TASK '{test_task2}'",
            f"DROP XNODE TASK '{test_task3}'",
        ]

        for sql in cleanup_sqls:
            tdLog.debug(f"cleanup test tasks: {sql}")
            self.no_syntax_fail_execute(sql)

    def test_drop_xnode_job_where_edge_cases(self):
        """测试 DROP XNODE JOB WHERE 边界条件和语法变体

        1. Test operator variations
        2. Test parentheses usage
        3. Test spacing and formatting
        4. Test field name case sensitivity
        5. Test value format variations
        6. Test complex nested conditions

        Since: v3.4.0.1

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-19 GuiChuan Zhang Created
        """

        # 测试操作符变体
        operator_sqls = [
            # 标准比较操作符
            "DROP XNODE JOB WHERE id > 1",
            "DROP XNODE JOB WHERE id >= 1",
            "DROP XNODE JOB WHERE id < 9999",
            "DROP XNODE JOB WHERE id <= 9999",
            "DROP XNODE JOB WHERE id = 1000",
            "DROP XNODE JOB WHERE id != 1000",
            "DROP XNODE JOB WHERE id = 0",
            "DROP XNODE JOB WHERE id != 0",
            # 字符串比较
            "DROP XNODE JOB WHERE status = 'running'",
            "DROP XNODE JOB WHERE status != 'failed'",
            "DROP XNODE JOB WHERE config = '{}'",
            "DROP XNODE JOB WHERE config != ''",
            "DROP XNODE JOB WHERE reason = ''",
            "DROP XNODE JOB WHERE reason != 'error'",
            # 时间字符串比较
            "DROP XNODE JOB WHERE create_time = '2024-01-01 00:00:00'",
            "DROP XNODE JOB WHERE create_time != '1970-01-01 00:00:00'",
            "DROP XNODE JOB WHERE update_time = '2024-12-31 23:59:59'",
            "DROP XNODE JOB WHERE update_time != '1970-01-01 00:00:00'",
        ]

        for sql in operator_sqls:
            tdLog.debug(f"drop job with operator variant: {sql}")
            self.no_syntax_fail_execute(sql)

        # 测试括号使用
        parentheses_sqls = [
            # 简单括号
            "DROP XNODE JOB WHERE (id > 1)",
            "DROP XNODE JOB WHERE (status = 'running')",
            "DROP XNODE JOB WHERE (via = 1)",
            # 多层括号
            "DROP XNODE JOB WHERE ((id > 1))",
            "DROP XNODE JOB WHERE (((status = 'running')))",
            # AND/OR 组合括号
            "DROP XNODE JOB WHERE (id > 1 AND status = 'running')",
            "DROP XNODE JOB WHERE (status = 'running' OR status = 'pending')",
            "DROP XNODE JOB WHERE (id > 1 AND (status = 'running'))",
            "DROP XNODE JOB WHERE (id > 1) OR (status = 'running')",
            # 复杂嵌套括号
            "DROP XNODE JOB WHERE ((id > 1 AND status = 'running') OR (id < 0))",
            "DROP XNODE JOB WHERE (id > 1 AND (status = 'running' OR status = 'pending'))",
            "DROP XNODE JOB WHERE ((id > 100 AND id < 1000) AND (status != 'error'))",
        ]

        for sql in parentheses_sqls:
            tdLog.debug(f"drop job with parentheses: {sql}")
            self.no_syntax_fail_execute(sql)

        # 测试空格和格式变体
        formatting_sqls = [
            # 无空格
            "DROP XNODE JOB WHERE id>1",
            "DROP XNODE JOB WHERE status='running'",
            "DROP XNODE JOB WHERE via=1",
            # 部分空格
            "DROP XNODE JOB WHERE id >1",
            "DROP XNODE JOB WHERE id> 1",
            "DROP XNODE JOB WHERE status ='running'",
            "DROP XNODE JOB WHERE status= 'running'",
            # 多余空格
            "DROP XNODE JOB  WHERE  id  >  1",
            "DROP XNODE JOB  WHERE  status  =  'running'",
            "DROP XNODE JOB   WHERE   via   =   1",
            # 混合空格
            "DROP XNODE JOB WHERE ( id > 1 AND status = 'running' )",
            "DROP XNODE JOB WHERE (id > 1) AND (status = 'running')",
        ]

        for sql in formatting_sqls:
            tdLog.debug(f"drop job with formatting variant: {sql}")
            self.no_syntax_fail_execute(sql)

        # 测试字段名大小写（假设不敏感）
        case_sqls = [
            "DROP XNODE JOB WHERE ID > 1",
            "DROP XNODE JOB WHERE Id > 1",
            "DROP XNODE JOB WHERE iD > 1",
            "DROP XNODE JOB WHERE TASK_ID = 1",
            "DROP XNODE JOB WHERE Task_Id = 1",
            "DROP XNODE JOB WHERE STATUS = 'running'",
            "DROP XNODE JOB WHERE Status = 'running'",
            "DROP XNODE JOB WHERE XNODE_ID = 1001",
            "DROP XNODE JOB WHERE Xnode_Id = 1001",
        ]

        for sql in case_sqls:
            tdLog.debug(f"drop job with case variant: {sql}")
            self.no_syntax_fail_execute(sql)

        # 测试 NOT 逻辑运算的各种用法
        not_sqls = [
            # 简单 NOT
            "DROP XNODE JOB WHERE NOT id = 0",
            "DROP XNODE JOB WHERE NOT status = 'failed'",
            "DROP XNODE JOB WHERE NOT via = 0",
            "DROP XNODE JOB WHERE NOT config = ''",
            # NOT 与比较运算
            "DROP XNODE JOB WHERE NOT id < 1",
            "DROP XNODE JOB WHERE NOT id > 10000",
            "DROP XNODE JOB WHERE NOT status != 'running'",
            "DROP XNODE JOB WHERE NOT create_time = '1970-01-01 00:00:00'",
            # NOT 与括号
            "DROP XNODE JOB WHERE NOT (id = 0)",
            "DROP XNODE JOB WHERE NOT (status = 'error' AND via = 0)",
            "DROP XNODE JOB WHERE NOT (id > 0 AND id < 10)",
            "DROP XNODE JOB WHERE NOT (status = 'failed' OR status = 'error')",
            # 多层 NOT
            #"DROP XNODE JOB WHERE NOT NOT id > 0",  # 双重否定
            #"DROP XNODE JOB WHERE NOT NOT status = 'running'",
        ]

        for sql in not_sqls:
            tdLog.debug(f"drop job with NOT condition: {sql}")
            self.no_syntax_fail_execute(sql)

        # 测试复杂嵌套逻辑条件
        complex_logic_sqls = [
            # 多层 AND/OR 组合
            "DROP XNODE JOB WHERE (id > 1 AND id < 1000) AND (status = 'running' OR status = 'pending')",
            "DROP XNODE JOB WHERE (via = 1 OR via = 2) AND (status != 'error' AND status != 'failed')",
            "DROP XNODE JOB WHERE (task_id > 0 AND task_id < 100) AND (xnode_id = 1 OR xnode_id = 2)",
            # NOT 与 AND/OR 组合
            "DROP XNODE JOB WHERE NOT (status = 'error' OR status = 'failed')",
            "DROP XNODE JOB WHERE NOT (id < 1 AND status = 'failed')",
            "DROP XNODE JOB WHERE (NOT status = 'error') AND (NOT status = 'failed')",
            "DROP XNODE JOB WHERE NOT (via = 0 OR via = 999)",
            # 复杂括号优先级
            "DROP XNODE JOB WHERE id > 1 AND (status = 'running' OR (via = 1 AND xnode_id = 1001))",
            "DROP XNODE JOB WHERE (id > 100 OR id < 0) AND (status != 'error' AND (via >= 1))",
            "DROP XNODE JOB WHERE ((id > 1 AND status = 'running') OR (id < 0)) AND via != 0",
            # 混合逻辑运算
            "DROP XNODE JOB WHERE NOT (id = 0) AND (status = 'running' OR status = 'pending')",
            "DROP XNODE JOB WHERE (NOT status = 'failed') AND (id > 0 AND id < 10000)",
            "DROP XNODE JOB WHERE (id > 1 OR id < 0) AND NOT (status = 'error' AND via = 0)",
        ]

        for sql in complex_logic_sqls:
            tdLog.debug(f"drop job with complex logic: {sql}")
            self.no_syntax_fail_execute(sql)

        # 测试边界值和特殊值
        boundary_sqls = [
            # 整数边界值
            "DROP XNODE JOB WHERE id = 0",
            "DROP XNODE JOB WHERE id = -1",
            "DROP XNODE JOB WHERE id = 1",
            "DROP XNODE JOB WHERE id > -1",
            "DROP XNODE JOB WHERE id < 1",
            # 字符串边界值
            "DROP XNODE JOB WHERE status = ''",
            "DROP XNODE JOB WHERE config = ''",
            "DROP XNODE JOB WHERE reason = ''",
            "DROP XNODE JOB WHERE status != ''",
            "DROP XNODE JOB WHERE config != ''",
            # 时间边界值
            "DROP XNODE JOB WHERE create_time = '1970-01-01 00:00:00'",
            "DROP XNODE JOB WHERE create_time > '1970-01-01 00:00:00'",
            "DROP XNODE JOB WHERE update_time < '2025-01-01 00:00:00'",
            "DROP XNODE JOB WHERE create_time != '1970-01-01 00:00:00'",
            # via 和 xnode_id 边界值
            "DROP XNODE JOB WHERE via = 0",
            "DROP XNODE JOB WHERE via = -1",
            "DROP XNODE JOB WHERE xnode_id = 0",
            "DROP XNODE JOB WHERE xnode_id = -1",
        ]

        for sql in boundary_sqls:
            tdLog.debug(f"drop job with boundary value: {sql}")
            self.no_syntax_fail_execute(sql)

    def test_drop_xnode_job_where_simple(self):
        """测试 DROP XNODE JOB WHERE 简单条件

        1. Test create xnode job
        2. Test rebalance xnode job
        3. Test drop xnode job

        Since: v3.4.0.1

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-20 GuiChuan Zhang Created
        """
        for _ in range(20):
            self.no_syntax_fail_execute("CREATE XNODE JOB ON 1 WITH config 'test' status 'run'")

        rs = tdSql.query("show xnode jobs", row_tag=True)
        self.no_syntax_fail_execute(f"REBALANCE XNODE JOB WHERE task_id=1")
        self.no_syntax_fail_execute(f"REBALANCE XNODE JOB WHERE id>=1")
        self.no_syntax_fail_execute(f"REBALANCE XNODE JOB WHERE config='test'")
        self.no_syntax_fail_execute(f"REBALANCE XNODE JOB WHERE status='run'")
        self.no_syntax_fail_execute(f"DROP XNODE JOB {rs[0][0]}")
        self.no_syntax_fail_execute(f"DROP XNODE JOB WHERE task_id=1")
        self.wait_transaction_to_commit()
        rs = tdSql.query(f"show xnode jobs where id={rs[0][0]}", row_tag=True)
        assert len(rs) == 0

        self.no_syntax_fail_execute("CREATE XNODE JOB ON 1 WITH config 'test' status 'run'")
        self.no_syntax_fail_execute("CREATE XNODE JOB ON 1 WITH config 'test' status 'run'")
        self.no_syntax_fail_execute("CREATE XNODE JOB ON 1 WITH config 'test' status 'run'")
        rs = tdSql.query("show xnode jobs", row_tag=True)
        self.no_syntax_fail_execute(f"DROP XNODE JOB {rs[0][0]}")
        self.no_syntax_fail_execute(f"DROP XNODE JOB WHERE id>{rs[0][0]}")
        self.wait_transaction_to_commit()
        rs = tdSql.query(f"show xnode jobs where id={rs[0][0]}", row_tag=True)
        assert len(rs) == 0

        self.no_syntax_fail_execute("CREATE XNODE JOB ON 1 WITH config 'test' status 'run'")
        self.no_syntax_fail_execute("CREATE XNODE JOB ON 1 WITH config 'test' status 'run'")
        self.no_syntax_fail_execute("CREATE XNODE JOB ON 1 WITH config 'test1' status 'run'")
        self.no_syntax_fail_execute("CREATE XNODE JOB ON 1 WITH config 'test1' status 'run'")
        rs = tdSql.query("show xnode jobs", row_tag=True)
        self.no_syntax_fail_execute(f"DROP XNODE JOB {rs[0][0]}")
        self.no_syntax_fail_execute(f"DROP XNODE JOB WHERE config='test' and status='run'")
        self.no_syntax_fail_execute(f"DROP XNODE JOB WHERE config='test1'")
        self.wait_transaction_to_commit()
        rs = tdSql.query(f"show xnode jobs where id={rs[0][0]}", row_tag=True)
        assert len(rs) == 0

    def wait_transaction_to_commit(self):
        """等待 transactions 完成

        1. show transactions
        2. wait 3 seconds

        Since: v3.4.0.1

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-19 GuiChuan Zhang Created
        """
        cnt = 0
        while True and cnt < 3:
            cnt += 1
            rs = tdSql.query("show transactions", row_tag=True)
            if len(rs) <= 0:
                break
            tdLog.info(f"wait {cnt} times {rs} transactions to finish")
            time.sleep(3)
    
    def test_alter_userpass(self):
        """测试 ALTER XNODE SET USER/PASS

        1. Test create xnode 
        2. Test alter xnode set user/pass

        Since: v3.4.0.1

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-27 GuiChuan Zhang Created
        """
        rid = random.randint(1000, 9999)
        self.no_syntax_fail_execute(f"CREATE XNODE 'localhost_{rid}:6055'")
        self.no_syntax_fail_execute("ALTER XNODE SET USER root pass 'taosdata'")
        self.wait_transaction_to_commit()
        rs = tdSql.query(f"show xnodes where url='localhost_{rid}:6055'", row_tag=True)
        assert rs[0][1] == f'localhost_{rid}:6055'
        tdSql.query(f"drop xnode 'localhost_{rid}:6055'")
        self.wait_transaction_to_commit()

        rid = random.randint(1000, 9999)
        self.no_syntax_fail_execute(f"CREATE XNODE 'localhost:6055_{rid}' token 'vcUTCJ6spXeIVPFBvyuHlqgd9XgJHAFVoSqO6HLS4rUDLT2OgQxN96WMWBZpExJ'")
        self.no_syntax_fail_execute("ALTER XNODE SET USER root pass 'taosdata'")
        rs = tdSql.query(f"show xnodes where url='localhost:6055_{rid}'", row_tag=True)
        assert rs[0][1] == f'localhost:6055_{rid}'
        tdSql.query(f"drop xnode 'localhost:6055_{rid}'")
        self.wait_transaction_to_commit()

        rid = random.randint(1000, 9999)
        self.no_syntax_fail_execute(f"CREATE XNODE 'localhost:6055_{rid}' user root pass 'taosdata1'")
        self.no_syntax_fail_execute("ALTER XNODE SET USER root pass 'taosdata'")
        rs = tdSql.query(f"show xnodes where url='localhost:6055_{rid}'", row_tag=True)
        assert rs[0][1] == f'localhost:6055_{rid}'
        tdSql.query(f"drop xnode 'localhost:6055_{rid}'")
        self.wait_transaction_to_commit()

    def test_alter_token(self):
        """测试 ALTER XNODE SET TOKEN

        1. Test create xnode 
        2. Test alter xnode set token

        Since: v3.4.0.1

        Labels: common,ci

        Jira: None

        History:
            - 2026-01-27 GuiChuan Zhang Created
        """
        rid = random.randint(1000, 9999)
        self.no_syntax_fail_execute(f"CREATE XNODE 'localhost_{rid}:6055'")
        self.wait_transaction_to_commit()
        rs = tdSql.query(f"show xnodes where url='localhost_{rid}:6055'", row_tag=True)
        assert rs[0][1] == f'localhost_{rid}:6055'
        rs = tdSql.query("show tokens", row_tag=True)
        assert rs[0][0] == '__xnode__'
        self.no_syntax_fail_execute("ALTER XNODE SET token 'vcUTCJ6spXeIVPFBvyuHlqgd9XgJHAFVoSqO6HLS4rUDLT2OgQxN96WMWBZpExJ'")
        tdSql.query(f"drop xnode 'localhost_{rid}:6055'")
        self.wait_transaction_to_commit()

        rid = random.randint(1000, 9999)
        self.no_syntax_fail_execute(f"CREATE XNODE 'localhost_{rid}:6055' user root pass 'taosdata'")
        self.no_syntax_fail_execute("ALTER XNODE SET USER root pass 'taosdata'")
        self.no_syntax_fail_execute("ALTER XNODE SET token 'vcUTCJ6spXeIVPFBvyuHlqgd9XgJHAFVoSqO6HLS4rUDLT2OgQxN96WMWBZpExJ'")
        rs = tdSql.query(f"show xnodes where url='localhost_{rid}:6055'", row_tag=True)
        assert rs[0][1] == f'localhost_{rid}:6055'
        tdSql.query(f"drop xnode 'localhost_{rid}:6055'")
        self.wait_transaction_to_commit()