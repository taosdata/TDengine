import os
import sys
import taos

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *


class TDTestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def _create_stable(self):
        # NOTE: TAGS schema order is:
        #   tab, underlying_date, trade_date, company_code, company_name,
        #   un_name, unit_alias_name, un_id
        # The INSERT below binds tags in a DIFFERENT order:
        #   tab, underlying_date, trade_date, company_code, company_name,
        #   un_id, un_name, unit_alias_name
        # This intentionally exercises the parser path where the in-memory
        # tag-value array (sorted by cid) and the tag-name array (in binding
        # order) would otherwise misalign, producing wrong tagName <-> value
        # pairs in the tmq JSON meta. The C verifier checks the pairing.
        stb_sql = (
            "CREATE STABLE `trade_realtime_log` ("
            "`ts` TIMESTAMP,"
            "`u_date` TIMESTAMP,"
            "`t_date` TIMESTAMP,"
            "`receive_time` TIMESTAMP,"
            "`pull_time` TIMESTAMP,"
            "`time_code` TINYINT,"
            "`buy_price1` DOUBLE,"
            "`sale_price1` DOUBLE,"
            "`tradeseq_id` VARCHAR(50),"
            "`tab_name` VARCHAR(100),"
            "`new_price` DOUBLE"
            ") TAGS ("
            "`tab` VARCHAR(100),"
            "`underlying_date` TIMESTAMP,"
            "`trade_date` TIMESTAMP,"
            "`company_code` VARCHAR(50),"
            "`company_name` VARCHAR(200),"
            "`un_name` VARCHAR(200),"
            "`unit_alias_name` VARCHAR(200),"
            "`un_id` VARCHAR(50))"
        )
        tdSql.execute(stb_sql)

    def _insert_sql(self, ts):
        # Single subtable insert. Tag binding column order differs from
        # the STABLE TAGS schema order (un_id appears before un_name /
        # unit_alias_name in the binding list).
        return (
            "INSERT INTO spot_trading.`realtime_20260524_20260520_19_11` "
            "USING spot_trading.trade_realtime_log "
            "(`tab`, `underlying_date`, `trade_date`, `company_code`, "
            " `company_name`, `un_id`, `un_name`, `unit_alias_name`) "
            "TAGS ('2026spot-trade-20260524', '2026-05-24 00:00:00', "
            "      '2026-05-20 00:00:00', '91320811MADR1RFF2J01', "
            "      'GuangJing-Energy-Company-Long-Name', '19', "
            "      'GuangJing-Energy-UnName', 'GuangJing-Energy-UnitAlias') "
            "(ts, u_date, t_date, receive_time, pull_time, time_code, "
            " buy_price1, sale_price1, tradeseq_id, tab_name, new_price) "
            f"VALUES ('{ts}', '2026-05-24 00:00:00', '2026-05-20 00:00:00', "
            f"        '{ts}', '{ts}', 11, 327.3, 328.0, "
            "         'PHDJS2026052442000811', 'spot-trade-2026-05-24', "
            "         328.0)"
        )

    def run(self):
        db_name = "spot_trading"
        topic_name = "topic_spot_trading"

        tdSql.execute(f"drop topic if exists {topic_name}")
        tdSql.execute(f"drop database if exists {db_name}")
        tdSql.execute(f"create database if not exists {db_name} vgroups 1 wal_retention_period 3600")
        tdSql.execute(f"use {db_name}")

        self._create_stable()

        tdSql.execute(
            f"create topic {topic_name} with meta as database {db_name}"
        )

        tdLog.info("execute first insert (ts=2026-05-20 16:03:46)")
        tdSql.execute(self._insert_sql("2026-05-20 16:03:46"))

        # Re-run insert with a different primary key timestamp so the
        # subtable already exists (no new CREATE TABLE meta) but a new
        # data row is appended.
        tdLog.info("execute second insert (ts=2026-05-20 16:04:46)")
        tdSql.execute(self._insert_sql("2026-05-20 16:04:46"))

        tdSql.query(
            f"select count(*) from {db_name}.trade_realtime_log"
        )
        tdSql.checkData(0, 0, 2)  # 1 subtable x 2 rows

        build_path = tdCom.getBuildPath()
        cmd = f"{build_path}/build/bin/tmq_spot_trading {topic_name}"
        tdLog.info(cmd)
        ret = os.system(cmd)
        if ret != 0:
            tdLog.exit(f"{cmd} failed, ret={ret}")

        tdSql.execute(f"drop topic if exists {topic_name}")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
