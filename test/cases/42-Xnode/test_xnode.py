import random
import uuid

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

    def _no_syntax_fail_execute(self, sql: str):
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

    def _no_syntax_fail_query(self, sql: str):
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
        
        Since: v3.3.8.8

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
            self._no_syntax_fail_query(sql)

    def test_xnode_crud(self):
        """测试 XNode CRUD 操作

        1. Drop xnode force by endpoint and id
        2. Create xnode with endpoint and user/pass
        3. Create xnode with id only
        4. Drop xnode by endpoint and id
        5. Drain xnode by id
        
        Since: v3.3.8.8

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
            f"CREATE XNODE '{ep1}' USER __xnode__ PASS 'Ab{self.suffix}!'",
            f"CREATE XNODE '{ep2}'",
            f"DROP XNODE '{ep2}'",
            f"DROP XNODE {node_id}",
            f"DRAIN XNODE {self.rand_ids[1]}",
        ]
        for sql in sqls:
            tdLog.debug(f"exec: {sql}")
            self._no_syntax_fail_execute(sql)

    def _test_task_lifecycle(self):
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

        Since: v3.3.8.8

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
            #f"CREATE XNODE TASK '{t_ingest}' FROM 'mqtt://broker:1883' TO DATABASE {dbname} "
            #"WITH parser 'parser_json', batch 1024, TRIGGER 'manual'",
            # create with topic source and DSN sink using AND
            # f"CREATE XNODE TASK '{t_export}' FROM TOPIC {topic} TO 'kafka://broker:9092' "
            # "WITH group_id 'g1' AND client_id 'c1'",
            # create task without FROM/TO but with options only
            f"CREATE XNODE TASK '{t_opts}' WITH retry 5 AND TRIGGER 'cron_5m'",
            # alter with new source/sink/options
            f"ALTER XNODE TASK {task_id_1} FROM DATABASE {dbname} TO 'influxdb://remote' "
            "WITH parser 'parser_line', concurrency 4",
            # alter task with options only
            f"ALTER XNODE TASK {task_id_2} WITH batch 2048 AND timeout 30",
            # start/stop task
            f"START XNODE TASK {task_id_1}",
            f"STOP XNODE TASK {task_id_1}",
            # rebalance by id with options
            f"REBALANCE XNODE JOB {self.rand_ids[4]} WITH xnode_id 3",
            # rebalance by where clause
            "REBALANCE XNODE JOB WHERE id > 0",
            # drop variations
            f"DROP XNODE TASK '{t_ingest}'",
            f"DROP XNODE TASK {task_id_2}",
            f"DROP XNODE TASK WHERE name = '{t_export}'",
            f"DROP XNODE TASK ON 1 WHERE id = {task_id_1}",
        ]
        for sql in task_sqls:
            tdLog.debug(f"exec: {sql}")
            self._no_syntax_fail_execute(sql)

    # def test_agent_lifecycle(self):
    #     agent1 = f"a1_{self.suffix}"
    #     agent2 = f"a2_{self.suffix}"

    #     agent_sqls = [
    #         f"DROP XNODE AGENT '{agent1}'",
    #         f"CREATE XNODE AGENT '{agent1}' WITH ttl '1y', ipwhitelist '127.0.0.1'",
    #         f"CREATE XNODE AGENT '{agent2}' WITH region 'cn-north-1' AND TRIGGER 'heartbeat'",
    #         f"DROP XNODE AGENT '{agent2}'",
    #         f"DROP XNODE AGENT WHERE name = '{agent1}'",
    #     ]
    #     for sql in agent_sqls:
    #         tdLog.debug(f"exec: {sql}")
    #         self._no_syntax_fail_execute(sql)

    def _test_job_lifecycle(self):
        """测试 XNode Job 生命周期

        1. Drop xnode job by id
        2. Create xnode job on xnode with config
        3. Alter xnode job with new trigger/priority
        4. Start/stop xnode job
        5. Rebalance xnode job by id with options
        6. Rebalance xnode job by where clause
        7. Drop xnode job variations

        Since: v3.3.8.8

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
            # f"ALTER XNODE JOB {job_id} WITH TRIGGER 'manual', priority 10",
            f"REBALANCE XNODE JOB {job_id} WITH xnode_id {job_on}",
            "REBALANCE XNODE JOB WHERE jid >= 1",
            #f"DROP XNODE JOB ON {job_on} WHERE jid = {job_id}",
            #"DROP XNODE JOB WHERE jid > 1",
        ]
        for sql in job_sqls:
            tdLog.debug(f"exec: {sql}")
            self._no_syntax_fail_execute(sql)

    def _test_sources_and_sinks_variants(self):
        """测试 XNode 任务源和 sink 变体

        1. Create xnode task with database source and sink
        2. Create xnode task with topic source and database sink
        3. Create xnode task with string source and sink
        4. Alter xnode task with new sink and options
        5. Drop xnode task variations

        Since: v3.3.8.8

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
            self._no_syntax_fail_execute(sql)

    def test_show_after_operations(self):
        """测试 XNode 操作后的 SHOW 语句

        1. Show xnodes
        2. Show xnode tasks
        3. Show xnode agents
        4. Show xnode jobs

        Since: v3.3.8.8

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
            self._no_syntax_fail_query(sql)
        tdLog.info(f"{__file__} xnode syntax coverage completed")
