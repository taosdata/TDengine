import taos
import sys
import time
import socket
import os
import threading

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from taos.tmq import *
from taos import *

sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    updatecfgDict = {'debugFlag': 143, 'asynclog': 0}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def consumeTest_TS_4563(self):
        tdSql.execute(f'use db_stmt')

        tdSql.query("select ts,k from st")
        tdSql.checkRows(2)

        tdSql.execute(f'create topic t_unorder_data as select ts,k from st')
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["t_unorder_data"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        cnt = 0
        try:
            while True:
                res = consumer.poll(1)
                print(res)
                if not res:
                    if cnt == 0:
                        tdLog.exit("consume error")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    cnt += len(block.fetchall())

                if cnt != 2:
                    tdLog.exit("consume error")

        finally:
            consumer.close()

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    def newcon(self,host,cfg):
        user = "root"
        password = "taosdata"
        port =6030
        con=taos.connect(host=host, user=user, password=password, config=cfg ,port=port)
        print(con)
        return con

    def test_stmt_insert_multi(self,conn):
        # type: (TaosConnection) -> None

        dbname = "db_stmt"
        try:
            conn.execute("drop database if exists %s" % dbname)
            conn.execute("create database if not exists %s" % dbname)
            conn.select_db(dbname)

            conn.execute(
                "create table st(ts timestamp, i int, j int, k int)",
            )
            # conn.load_table_info("log")
            tdLog.debug("statement start")
            start = datetime.now()
            stmt = conn.statement("insert into st(ts,j) values(?, ?)")

            params = new_multi_binds(2)
            params[0].timestamp((1626861392589, 1626861392590))
            params[1].int([3, None])

            # print(type(stmt))
            tdLog.debug("bind_param_batch start")
            stmt.bind_param_batch(params)
            tdLog.debug("bind_param_batch end")
            stmt.execute()
            tdLog.debug("execute end")
            end = datetime.now()
            print("elapsed time: ", end - start)
            assert stmt.affected_rows == 2
            tdLog.debug("close start")

            stmt.close()

            # conn.execute("drop database if exists %s" % dbname)
            conn.close()

        except Exception as err:
            # conn.execute("drop database if exists %s" % dbname)
            conn.close()
            raise err

    def run(self):
        buildPath = self.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"
        host="localhost"
        connectstmt=self.newcon(host,config)
        self.test_stmt_insert_multi(connectstmt)
        self.consumeTest_TS_4563()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())