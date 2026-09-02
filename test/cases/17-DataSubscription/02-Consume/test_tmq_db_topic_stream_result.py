import os
import time

from new_test_framework.utils import tdCom, tdLog, tdSql, tdStream


class TestTmqDbTopicStreamResult:
    updatecfgDict = {"debugFlag": 135, "asynclog": 0}
    clientCfgDict = {"debugFlag": 135, "asynclog": 0}
    updatecfgDict["clientCfg"] = clientCfgDict

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def do_replay_stream_result(self):
        try:
            tdSql.execute("create snode on dnode 1")
        except Exception:
            pass

        tdSql.execute('alter all dnodes "debugflag 135"')
        tdSql.execute("drop topic if exists db_5466_topic")
        tdSql.execute("drop database if exists db_5466")
        tdSql.execute("drop database if exists db_taosx")
        tdSql.execute("create database db_5466 vgroups 1")
        tdSql.execute("create database db_taosx vgroups 1")
        tdSql.execute("use db_5466")
        tdSql.execute("create table source_t (ts timestamp, val int)")
        tdSql.execute(
            "create stream stream_t interval(1s) sliding(1s) from source_t into stream_result "
            "as select _twstart wstart, count(*) cnt_v, sum(val) sum_v "
            "from source_t where ts >= _twstart and ts < _twend;"
        )
        tdStream.checkStreamStatus("stream_t")

        tdSql.execute(
            "insert into db_5466.source_t values "
            "('2025-01-01 00:00:00', 1) "
            "('2025-01-01 00:00:01', 3)"
        )

        for _ in range(30):
            tdSql.query("select count(*) from db_5466.stream_result")
            if tdSql.getData(0, 0) == 1:
                break
            time.sleep(0.5)
        else:
            tdLog.exit("stream result table was not generated as expected")

        tdSql.execute("flush database db_5466")
        tdSql.execute("create topic db_5466_topic with meta as database db_5466")

        command = f"{tdCom.getBuildPath()}/build/bin/tmq_ts5466 false"
        tdLog.info(command)
        if os.system(command) != 0:
            tdLog.exit(command)

        for _ in range(30):
            tdSql.query("select count(*) from db_taosx.stream_result")
            if tdSql.getData(0, 0) == 1:
                break
            time.sleep(0.5)
        else:
            tdLog.exit("replicated stream result table was not generated as expected")

        tdSql.checkResultsBySql(
            sql="select * from db_5466.stream_result order by wstart",
            exp_sql="select * from db_taosx.stream_result order by wstart",
            retry=1,
        )
        print("db topic stream result sync ................ [passed]")

    def test_tmq_db_topic_stream_result(self):
        """Replay a stream-generated normal table through a database topic

        1. Create a stream result table from a normal source table
        2. Replay database-topic metadata and data without a snapshot

        Catalog:
            - DataSubscription

        Since: v3.0.0.0

        Labels: common,ci

        Jira: 7056409303

        History:
            - 2026-07-30 mmwang Added regression for normal-table TMQ metadata replay

        """
        self.do_replay_stream_result()
