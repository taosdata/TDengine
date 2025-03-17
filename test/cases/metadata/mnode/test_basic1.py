import pytest
from utils.log import tdLog
from utils.sql import tdSql
from utils.army.frame.clusterCommonCheck import * 

# script,./test.sh -f tsim/db/basic1.sim
# run command: pytest test_basic1.py -N 2


class TestBasic1:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_basic1(self):
        tdLog.info("=============== select * from information_schema.ins_dnodes")
        tdSql.query("select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 2, "leader")

        clusterComCheck.checkDnodes(2)
        tdSql.error("create mnode on dnode 1")
        tdSql.error("drop mnode on dnode 1")

        tdLog.info("=============== create mnode 2")
        tdSql.execute("create mnode on dnode 2")

        tdLog.info("=============== create mnode 2 finished")
        
        clusterComCheck.checkMnodeStatus(2)

        tdLog.info("============ drop mnode 2")
        tdSql.execute("drop mnode on dnode 2")

        tdLog.info("============ drop mnode 2 finished")
        tdSql.query("select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)

        tdSql.error("drop mnode on dnode 2")
        clusterComCheck.checkMnodeStatus(1)

        time.sleep(2)
        tdLog.info("============== create mnodes")
        tdSql.execute("create mnode on dnode 2")

        tdLog.info("============== create mnode 2 finished")
        tdSql.query("select * from information_schema.ins_mnodes")
        tdSql.checkRows(2)

        clusterComCheck.checkMnodeStatus(2)

    def teardown_class(cls):
        tdLog.debug(f"end to excute {__file__}")
