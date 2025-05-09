import platform
import os
from new_test_framework.utils import tdLog, tdSql, tdCom
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestImportFile:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_import_file(self):
        """import file

        1.

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/import_file.sim

        """

        tdSql.execute(f"drop database if exists indb")
        tdSql.execute(f"create database if not exists indb")
        tdSql.execute(f"use indb")

        if platform.system().lower() == "windows":
            inFileName = "'C:\\Windows\\Temp\\data.sql'"
        else:
            inFileName = "'/tmp/data.sql'"

        os.system("tools/gendata.sh")

        tdSql.execute(
            f"create table stbx (ts TIMESTAMP, collect_area NCHAR(12), device_id BINARY(16), imsi BINARY(16),  imei BINARY(16),  mdn BINARY(10), net_type BINARY(4), mno NCHAR(4), province NCHAR(10), city NCHAR(16), alarm BINARY(2)) tags(a int, b binary(12))"
        )

        tdSql.execute(
            f"create table tbx (ts TIMESTAMP, collect_area NCHAR(12), device_id BINARY(16), imsi BINARY(16),  imei BINARY(16),  mdn BINARY(10), net_type BINARY(4), mno NCHAR(4), province NCHAR(10), city NCHAR(16), alarm BINARY(2))"
        )
        tdLog.info(f"====== create tables success, starting insert data")

        tdSql.execute(f"insert into tbx file {inFileName}")
        tdSql.execute(f"import into tbx file {inFileName}")

        tdSql.query(f"select count(*) from tbx")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.execute(f"drop table tbx")

        tdSql.execute(f"insert into tbx using stbx tags(1,'abc') file {inFileName}")
        tdSql.execute(f"insert into tbx using stbx tags(1,'abc') file {inFileName}")

        tdSql.query(f"select count(*) from tbx")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.execute(f"drop table tbx")
        tdSql.execute(f"insert into tbx using stbx(b) tags('abcf') file {inFileName}")

        tdSql.query(f"select ts,a,b from tbx")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2020-01-01 01:01:01")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, "abcf")

        os.system(f"rm -f {inFileName}")
