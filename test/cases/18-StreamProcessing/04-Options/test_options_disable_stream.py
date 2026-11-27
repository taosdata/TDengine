import json
import os
import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    tdDnodes,
)


def _patchPersistedDisableStream(index, enabled):
    """Force disableStream into the persisted dnode config.

    TDengine v3.4+ persists CFG_DYN_SERVER_LAZY items (disableStream is one)
    to data/dnode/config/local.json on first startup, and that persisted
    value wins over taos.cfg on every later startup. So appending
    "disableStream 1" to taos.cfg via tdDnodes.cfg() has no effect once the
    dnode has started once; the JSON file itself must be patched instead.
    """
    dataDir = tdDnodes.dnodes[index - 1].dataDir[0]
    cfgPath = os.path.join(dataDir, "dnode", "config", "local.json")
    with open(cfgPath, "r") as f:
        cfg = json.load(f)
    cfg["configs"]["disableStream"] = enabled
    with open(cfgPath, "w") as f:
        json.dump(cfg, f)


class TestStreamOptionsDisableStream:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_options_disable_stream(self):
        """Options: stop/drop stream while disableStream is enabled

        Regression test for the bug where setting the server config
        `disableStream` to true made STOP STREAM / DROP STREAM fail outright,
        because the two message handlers were not registered at all in that
        case. This case verifies:

        1. Streams that already exist are not (re)deployed while disableStream
           is true: no task shows up for them and their status stays
           "Undeployed" after a restart with the config enabled.
        2. STOP STREAM and DROP STREAM still succeed against those streams
           while disableStream is true.
        3. CREATE STREAM is still refused while disableStream is true, i.e.
           the config keeps blocking new deployments as intended.

        Catalog:
            - Streams:Options

        Since: v3.3.7.x

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-7-8 wangmingming Created to cover the disableStream
              stop/drop stream fix

        """

        dbname = "db_disable_stream"
        stream1 = "s_disable_1"
        stream2 = "s_disable_2"

        tdStream.createSnode()

        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1")
        tdSql.execute(f"use {dbname}")
        tdSql.execute("create table t1 (ts timestamp, c1 int)")

        tdSql.execute(
            f"create stream {stream1} interval(5s) sliding(5s) from {dbname}.t1 "
            f"into {dbname}.r1 as select _twstart ts, count(*) cnt from %%trows"
        )
        tdSql.execute(
            f"create stream {stream2} interval(5s) sliding(5s) from {dbname}.t1 "
            f"into {dbname}.r2 as select _twstart ts, count(*) cnt from %%trows"
        )

        # both streams must be actually deployed and running before disabling
        tdStream.checkStreamStatus()

        tdSql.query(
            f"select count(*) from information_schema.ins_stream_tasks where stream_name='{stream1}'"
        )
        deployedTaskNum = tdSql.getData(0, 0)
        assert deployedTaskNum > 0, "stream task should be deployed while stream is enabled"

        try:
            tdLog.info("restart dnode 1 with disableStream enabled")
            tdDnodes.stop(1)
            time.sleep(2)
            tdDnodes.cfg(1, "disableStream", "1")
            _patchPersistedDisableStream(1, True)
            tdDnodes.start(1)
            time.sleep(5)

            # no task should ever be (re)deployed for the pre-existing stream
            # while disableStream is enabled; check a few times to make sure
            # nothing shows up asynchronously either.
            for _ in range(3):
                tdSql.query(
                    f"select count(*) from information_schema.ins_stream_tasks where stream_name='{stream1}'"
                )
                tdSql.checkData(0, 0, 0)
                time.sleep(3)

            tdSql.query(
                f"select status from information_schema.ins_streams where stream_name='{stream1}'"
            )
            tdSql.checkData(0, 0, "Undeployed")

            # create stream must still be refused while disableStream is enabled
            tdSql.error(
                f"create stream {dbname}.s_disable_3 interval(5s) sliding(5s) from {dbname}.t1 "
                f"into {dbname}.r3 as select _twstart ts, count(*) cnt from %%trows"
            )

            # START / RECALC are gated by the same flag: their handlers are not registered under
            # disableStream, so the server answers TSDB_CODE_MSG_NOT_PROCESSED (0x0121). Pin that
            # errno, otherwise unrelated business errors (e.g. START on a never-stopped stream, or
            # RECALC while mnode stream mgmt is not NORMAL) would also satisfy the assertion and a
            # gate regression could slip through.
            tdSql.error(f"start stream {stream2}", expectedErrno=0x80000121)
            tdSql.error(f"recalculate stream {stream2} from 0", expectedErrno=0x80000121)

            # the regression under test: stop/drop must succeed even though
            # the stream was never deployed under disableStream=1.
            # Note: the reported stream status stays "Undeployed" here too,
            # because mstGetStreamStatusStr() returns Undeployed unconditionally
            # while mnode stream mgmt state != NORMAL (which never happens
            # under disableStream=1, since the heartbeat handler that would
            # drive that transition isn't registered); tdSql.execute() below
            # already asserts that STOP/DROP STREAM themselves do not error out.
            tdSql.execute(f"stop stream {stream1}")

            tdSql.execute(f"drop stream {stream1}")
            tdSql.execute(f"drop stream {stream2}")

            tdSql.query(
                f"select count(*) from information_schema.ins_streams "
                f"where stream_name in ('{stream1}','{stream2}')"
            )
            tdSql.checkData(0, 0, 0)
        finally:
            tdLog.info("restore disableStream to 0")
            tdDnodes.stop(1)
            time.sleep(2)
            tdDnodes.cfg(1, "disableStream", "0")
            _patchPersistedDisableStream(1, False)
            tdDnodes.start(1)
            time.sleep(5)
            tdSql.execute(f"drop database if exists {dbname}")
