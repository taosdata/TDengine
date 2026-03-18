import sys
import time
import os

from new_test_framework.utils import tdLog, tdSql, tdDnodes
import datetime


class TestPrimaryKey:
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    updatecfgDict["clientCfg"] = clientCfgDict

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def prepareData(self):
        tdLog.info(f"prepare data")

        tdSql.execute(f'create snode on dnode 1;')
        tdSql.execute(f'create database if not exists db_pk_query vgroups 1 wal_retention_period 3600;')
        tdSql.execute(f'use db_pk_query;')
        tdSql.execute(f'create table if not exists pk (ts timestamp, c1 varchar(16) primary key, c2 int) tags(t int)')
        tdSql.execute(f'insert into pk1 using pk tags(1) values(1669092069068, "pk1", 1);')
        tdSql.execute(f'insert into pk1 using pk tags(1) values(1669092069068, "pk11", 2);')
        # tdSql.execute(f'flush database db_pk_query')

        tdLog.info(f"create successfully.")
    
    
    def createOneStream(self):
        tdLog.info(f"create stream:")
        sql = (
        f"create stream db_pk_query.stream_pk state_window(c2) from db_pk_query.pk partition by tbname into db_pk_query.pk_result as select * from db_pk_query.pk where ts >= _twstart and ts <= _twend"
        )
        tdLog.info(f"create stream:{sql}")
        try:
            tdSql.execute(sql)
        except Exception as e:
                if "No stream available snode now" not in str(e):
                    raise Exception(f" user cant  create stream no snode ,but create success")
    
    def checkStreamRunning(self):
        tdLog.info(f"check stream running status:")

        timeout = 60
        start_time = time.time()

        while True:
            if time.time() - start_time > timeout:
                tdLog.error("Timeout waiting for stream to be running.")
                raise TimeoutError(
                    f"Stream status did not reach 'Running' within {timeout}s timeout."
                )

            tdSql.query(
                f"select status from information_schema.ins_streams order by stream_name;"
            )
            streamRunning = tdSql.getColData(0)

            if all(status == "Running" for status in streamRunning):
                tdLog.info("stream running!")
                return
            else:
                tdLog.info("Stream not running! Wait stream running ...")
                time.sleep(1)

    def checkResultRows(self):
        tdSql.checkResultsByFunc(
            f"select count(*) from db_pk_query.pk_result;",
            lambda: tdSql.getData(0, 0) != 0,
            delay=0.5,
            retry=60,
        )

    def insertData(self):
        tdSql.execute(f'insert into db_pk_query.pk2 using db_pk_query.pk tags(2) values(1669092069069, "pk2", 11) (1669092069069, "pk22", 12);')
        tdSql.execute(f'insert into db_pk_query.pk3 using db_pk_query.pk tags(3) values(1669092069069, "pk3", 13) (1669092069069, "pk33", 14);')
        tdSql.execute(f'insert into db_pk_query.pk4 using db_pk_query.pk tags(4) values(1669092069068, "pk4", 15) (1669092069068, "pk44", 16);')


    def test_primary_key(self):
        """Basic: primary key test
        
        1. Create tables with primary key
        2. Create stream
        3. Test result
        
        Since: v3.0.0.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/6771200393

        History:
            - 2026-02-04 created by mingming wang

        """
        self.prepareData()
        self.createOneStream()
        self.checkStreamRunning()
        self.insertData()
        self.checkResultRows()




