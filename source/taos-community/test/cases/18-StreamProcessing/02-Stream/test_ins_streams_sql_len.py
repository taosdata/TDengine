import time
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestInsStreamsSqlLen:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_ins_streams_sql_len(self):
        """ins_streams sql length

        1. Create a stream with SQL length > 2048
        2. Verify information_schema.ins_streams.sql is not truncated

        Since: v3.3.8.11

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/6593026343

        History:
            - 2026-03-03 Jinqing Kuang Created

        """

        db_name = "db_ins_streams_sql_len"
        stream_name = "s_sql_len"
        tail_marker = "ins_streams_sql_len_tail_marker_20260303"

        tdStream.createSnode(1)

        sqls = [
            f"drop database if exists {db_name}",
            f"create database {db_name} vgroups 1",
            f"create table {db_name}.meters (ts timestamp, c1 int)",
            f"insert into {db_name}.meters values(now, 1)",
        ]
        tdSql.executes(sqls)

        padding_conditions = " ".join(["and c1 >= 0" for _ in range(350)])
        create_stream_sql = (
            f"create stream {db_name}.{stream_name} interval(10s) sliding(10s) "
            f"from {db_name}.meters into {db_name}.meters_out as "
            f"select _twstart ts, count(*) c1 from {db_name}.meters "
            f"where ts >= _twstart and ts < _twend {padding_conditions} "
            f"and '{tail_marker}'='{tail_marker}';"
        )
        if len(create_stream_sql) <= 2048:
            raise Exception(f"invalid test SQL length: {len(create_stream_sql)}")

        tdSql.execute(create_stream_sql)

        stream_sql = None
        def _check_stream_row():
            nonlocal stream_sql
            # tdSql.checkResultsByFunc will execute the query; here we just
            # verify the result and capture the SQL text when it appears.
            if tdSql.getRows() == 1:
                stream_sql = tdSql.getData(0, 0)
                return True
            return False

        tdSql.checkResultsByFunc(
            f"select sql from information_schema.ins_streams where db_name='{db_name}' and stream_name='{stream_name}'",
            func=_check_stream_row,
        )
    
        if stream_sql is None:
            raise Exception("cannot find stream in information_schema.ins_streams")

        if len(stream_sql) <= 2048:
            raise Exception(
                f"sql is still truncated, expected > 2048 but got {len(stream_sql)}"
            )
        if tail_marker not in stream_sql:
            raise Exception("tail marker is missing, sql was truncated unexpectedly")

        tdLog.info(
            f"ins_streams sql length check passed, create_sql_len={len(create_stream_sql)}, stored_sql_len={len(stream_sql)}"
        )
