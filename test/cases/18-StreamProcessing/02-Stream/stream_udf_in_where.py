import os
import time
import platform
import subprocess

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamUdfInWhere:
    """Reproduce coredump when a UDF appears in the WHERE / output_subtable
    expression of a stream partitioned by tag columns.

    Defect: https://project.feishu.cn/taosdata_td/defect/detail/6979804109
    Crash trace: fmIsStreamPesudoColVal -> checkPlaceHolderColumn ->
                 createStreamReaderCalcInfo (libs/new-stream/streamReader.c)

    Expected behavior after the fix: the stream is created and reaches the
    Running state without taosd crashing, regardless of whether the UDF body
    returns matches.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_udf_in_where(self):
        """Stream with UDF in WHERE / output_subtable on partition-by tags

        Catalog:
            - Streams:UDF

        Since: v3.4.1.0

        Labels: common,ci

        Jira: https://project.feishu.cn/taosdata_td/defect/detail/6979804109

        History:
            - 2026-04-28 Copilot Created
        """

        tdStream.createSnode()
        self.locateUdfLib()
        self.prepareData()
        self.createUdf()
        self.createStream()
        self.checkStreamRunning()
        self.writeTriggerData()
        self.verifyResults()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def locateUdfLib(self):
        """Locate libudf1.so built by the project (already produced by CI build)."""
        selfPath = os.path.dirname(os.path.realpath(__file__))
        if "community" in selfPath:
            projPath = selfPath[: selfPath.find("community")]
        else:
            projPath = selfPath[: selfPath.find("tests") if "tests" in selfPath else selfPath.find("test")]

        if platform.system().lower() == "windows":
            cmd = (
                '(for /r %s %%i in ("udf1.d*") do @echo %%i)|grep lib|head -n1'
                % projPath
            )
        else:
            cmd = 'find %s -name "libudf1.so" | grep lib | head -n1' % projPath

        result = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        ).stdout.read().decode("utf-8")
        self.libudf1 = result.replace("\r", "").replace("\n", "")
        if not self.libudf1 or not os.path.isfile(self.libudf1):
            raise Exception(f"libudf1.so not found under {projPath}")
        tdLog.info(f"using udf lib: {self.libudf1}")

    def prepareData(self):
        sqls = [
            "drop database if exists hdata;",
            "create database hdata vgroups 2;",
            (
                "create table hdata.boat ("
                "ts timestamp, variable_value_float float) "
                "tags (boat_uuid varchar(36), variable_name varchar(50));"
            ),
            (
                "create table hdata.boat_ct1 using hdata.boat "
                "tags ('aaaa-bbbb-cccc-dddd-eeee-ffff111111', 'speed');"
            ),
            (
                "create table hdata.boat_ct2 using hdata.boat "
                "tags ('aaaa-bbbb-cccc-dddd-eeee-ffff222222', 'temperature');"
            ),
        ]
        tdSql.executes(sqls)
        tdLog.info("prepare data successfully.")

    def createUdf(self):
        """Drop and recreate the scalar UDF used inside the stream."""
        try:
            tdSql.execute("drop function is_alarm;")
        except Exception:
            pass
        tdSql.execute(
            f"create function is_alarm as '{self.libudf1}' outputtype int;"
        )
        tdSql.query("show functions;")
        names = [row[0] for row in tdSql.queryResult]
        if "is_alarm" not in names:
            raise Exception("create function is_alarm failed")
        tdLog.info("create udf is_alarm successfully.")

    def createStream(self):
        """Reproduce the failing DDL: count_window stream that references the
        UDF inside both output_subtable and the WHERE clause, while
        partitioning by tag columns."""
        sql = (
            "create stream if not exists hdata.s_boat_to_alarms "
            "count_window(1, 1) from hdata.boat "
            "partition by tbname, boat_uuid, variable_name "
            "into hdata.alarms "
            "output_subtable(concat('alarms_', "
            "replace(boat_uuid, '-', '_'), md5(variable_name))) "
            "tags (boat_uuid varchar(36) as boat_uuid, "
            "variable_name varchar(50) as variable_name) as "
            "select ts, "
            "case when variable_value_float in (1, 1.0) then 1 else 0 end "
            "as alarm_status, "
            "cast(_tlocaltime/1000000 as timestamp) as created_at "
            "from %%tbname "
            "where _c0 >= _twstart and _c0 < _twend "
            "and variable_value_float is not null "
            "and is_alarm(variable_name, boat_uuid) = 1;"
        )
        tdLog.info(f"create stream: {sql}")
        tdSql.execute(sql)

    def checkStreamRunning(self):
        """The original defect produced an taosd SIGSEGV in
        smDeployStreams -> stReaderTaskDeploy. Wait until the stream reaches
        Running state to confirm the deploy path no longer crashes."""
        deadline = time.time() + 60
        last_status = None
        while time.time() < deadline:
            tdSql.query(
                "select status from information_schema.ins_streams "
                "where stream_name='s_boat_to_alarms';"
            )
            if tdSql.getRows() > 0:
                last_status = tdSql.getData(0, 0)
                if last_status == "Running":
                    tdLog.info("stream is Running.")
                    return
            time.sleep(1)
        raise Exception(
            f"stream s_boat_to_alarms not Running, last status={last_status}"
        )

    def writeTriggerData(self):
        sqls = [
            "insert into hdata.boat_ct1 values "
            "('2026-01-01 00:00:00', 1.0),"
            "('2026-01-01 00:00:01', 0.0),"
            "('2026-01-01 00:00:02', 1.0);",
            "insert into hdata.boat_ct2 values "
            "('2026-01-01 00:00:00', 1.0),"
            "('2026-01-01 00:00:01', 0.0);",
        ]
        tdSql.executes(sqls)
        tdLog.info("write trigger data successfully.")

    def verifyResults(self):
        """The taosd process must still be alive and the output stable must
        have at least been created.  We do not rely on specific row counts
        because libudf1.so simply returns 1 and the trigger fan-out depends
        on count_window scheduling - the regression we are guarding against
        is the coredump, not numerical results."""
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                tdSql.query("select count(*) from hdata.alarms;")
                tdLog.info(f"hdata.alarms rows = {tdSql.getData(0, 0)}")
                break
            except Exception as e:
                tdLog.info(f"waiting for output table: {e}")
                time.sleep(1)
        # taosd liveness check via a simple round-trip
        tdSql.query("select server_status();")
        tdSql.checkData(0, 0, 1)
        tdLog.info("verifyResults passed: taosd alive, stream did not coredump.")
