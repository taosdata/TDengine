from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestNormalTableAlter1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_alter_1(self):
        """alter normal table 1

        1. add column
        2. drop column
        3. modify column

        Catalog:
            - Table:NormalTable:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/alter/table.sim

        """

        tdLog.info(f"======== step1")
        tdSql.prepare("d1", drop=True)
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table tb (ts timestamp, a int)")
        tdSql.execute(f"insert into tb values(now-28d, -28)")
        tdSql.execute(f"insert into tb values(now-27d, -27)")
        tdSql.execute(f"insert into tb values(now-26d, -26)")
        tdSql.query(f"select * from tb")
        tdSql.checkRows(3)

        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdLog.info(f"======== step2")
        tdSql.execute(f"alter table tb add column b smallint")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdLog.info(f"======== step3")
        tdSql.execute(f"alter table tb add column c tinyint")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "c")

        tdSql.checkData(3, 1, "TINYINT")

        tdLog.info(f"======== step4")
        tdSql.execute(f"alter table tb add column d int")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "c")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 0, "d")

        tdSql.checkData(4, 1, "INT")

        tdLog.info(f"======== step5")
        tdSql.execute(f"alter table tb add column e bigint")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "c")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 0, "d")

        tdSql.checkData(4, 1, "INT")

        tdSql.checkData(5, 0, "e")

        tdSql.checkData(5, 1, "BIGINT")

        tdLog.info(f"======== step6")
        tdSql.execute(f"alter table tb add column f float")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "c")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 0, "d")

        tdSql.checkData(4, 1, "INT")

        tdSql.checkData(5, 0, "e")

        tdSql.checkData(5, 1, "BIGINT")

        tdSql.checkData(6, 0, "f")

        tdSql.checkData(6, 1, "FLOAT")

        tdLog.info(f"======== step7")
        tdSql.execute(f"alter table tb add column g double")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "c")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 0, "d")

        tdSql.checkData(4, 1, "INT")

        tdSql.checkData(5, 0, "e")

        tdSql.checkData(5, 1, "BIGINT")

        tdSql.checkData(6, 0, "f")

        tdSql.checkData(6, 1, "FLOAT")

        tdSql.checkData(7, 0, "g")

        tdSql.checkData(7, 1, "DOUBLE")

        tdLog.info(f"======== step8")
        tdSql.execute(f"alter table tb add column h binary(10)")
        tdSql.query(f"select * from tb")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "c")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 0, "d")

        tdSql.checkData(4, 1, "INT")

        tdSql.checkData(5, 0, "e")

        tdSql.checkData(5, 1, "BIGINT")

        tdSql.checkData(6, 0, "f")

        tdSql.checkData(6, 1, "FLOAT")

        tdSql.checkData(7, 0, "g")

        tdSql.checkData(7, 1, "DOUBLE")

        tdSql.checkData(8, 0, "h")

        tdSql.checkData(8, 1, "VARCHAR")

        tdSql.checkData(8, 2, 10)

        tdLog.info(f"======== step9")
        tdLog.info(f"======== step10")

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.execute(f"use d1")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "c")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 0, "d")

        tdSql.checkData(4, 1, "INT")

        tdSql.checkData(5, 0, "e")

        tdSql.checkData(5, 1, "BIGINT")

        tdSql.checkData(6, 0, "f")

        tdSql.checkData(6, 1, "FLOAT")

        tdSql.checkData(7, 0, "g")

        tdSql.checkData(7, 1, "DOUBLE")

        tdSql.checkData(8, 0, "h")

        tdSql.checkData(8, 1, "VARCHAR")

        tdSql.checkData(8, 2, 10)

        tdLog.info(f"======== step11")
        tdSql.error(f"alter table drop column a")
        tdSql.error(f"alter table drop column ts")
        tdSql.error(f"alter table drop column cdfg")
        tdSql.error(f"alter table add column a")
        tdSql.error(f"alter table add column b")

        tdLog.info(f"======== step12")
        tdSql.execute(f"alter table tb drop column b")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "c")

        tdSql.checkData(2, 1, "TINYINT")

        tdSql.checkData(3, 0, "d")

        tdSql.checkData(3, 1, "INT")

        tdSql.checkData(4, 0, "e")

        tdSql.checkData(4, 1, "BIGINT")

        tdSql.checkData(5, 0, "f")

        tdSql.checkData(5, 1, "FLOAT")

        tdSql.checkData(6, 0, "g")

        tdSql.checkData(6, 1, "DOUBLE")

        tdSql.checkData(7, 0, "h")

        tdSql.checkData(7, 1, "VARCHAR")

        tdSql.checkData(7, 2, 10)

        tdLog.info(f"======== step13")
        tdSql.execute(f"alter table tb drop column c")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "d")

        tdSql.checkData(2, 1, "INT")

        tdSql.checkData(3, 0, "e")

        tdSql.checkData(3, 1, "BIGINT")

        tdSql.checkData(4, 0, "f")

        tdSql.checkData(4, 1, "FLOAT")

        tdSql.checkData(5, 0, "g")

        tdSql.checkData(5, 1, "DOUBLE")

        tdSql.checkData(6, 0, "h")

        tdSql.checkData(6, 1, "VARCHAR")

        tdSql.checkData(6, 2, 10)

        tdLog.info(f"======== step14")
        tdSql.execute(f"alter table tb drop column d")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "e")

        tdSql.checkData(2, 1, "BIGINT")

        tdSql.checkData(3, 0, "f")

        tdSql.checkData(3, 1, "FLOAT")

        tdSql.checkData(4, 0, "g")

        tdSql.checkData(4, 1, "DOUBLE")

        tdSql.checkData(5, 0, "h")

        tdSql.checkData(5, 1, "VARCHAR")

        tdSql.checkData(5, 2, 10)

        tdLog.info(f"======== step15")
        tdSql.execute(f"alter table tb drop column e")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "f")

        tdSql.checkData(2, 1, "FLOAT")

        tdSql.checkData(3, 0, "g")

        tdSql.checkData(3, 1, "DOUBLE")

        tdSql.checkData(4, 0, "h")

        tdSql.checkData(4, 1, "VARCHAR")

        tdSql.checkData(4, 2, 10)

        tdLog.info(f"======== step16")
        tdSql.execute(f"alter table tb drop column f")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "g")

        tdSql.checkData(2, 1, "DOUBLE")

        tdSql.checkData(3, 0, "h")

        tdSql.checkData(3, 1, "VARCHAR")

        tdSql.checkData(3, 2, 10)

        tdLog.info(f"======== step17")
        tdSql.execute(f"alter table tb drop column g")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "h")

        tdSql.checkData(2, 1, "VARCHAR")

        tdSql.checkData(2, 2, 10)

        tdLog.info(f"============= step18")
        tdSql.execute(f"alter table tb drop column h")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 0, "a")
        tdSql.checkData(1, 1, "INT")

        tdLog.info(f"=============== error for normal table")
        tdSql.execute(f"create table tb2023(ts timestamp, f int);")
        tdSql.error(f"alter table tb2023 add column v varchar(65518);")
        tdSql.error(f"alter table tb2023 add column v varchar(65531);")
        tdSql.error(f"alter table tb2023 add column v varchar(65535);")
        tdSql.execute(f"alter table tb2023 add column v varchar(65517);")
        tdSql.error(f"alter table tb2023 modify column v varchar(65518);")
        tdSql.query(f"desc tb2023")
        tdSql.execute(f"alter table tb2023 drop column v")
        tdSql.error(f"alter table tb2023 add column v nchar(16380);")
        tdSql.execute(f"alter table tb2023 add column v nchar(16379);")
        tdSql.error(f"alter table tb2023 modify column v nchar(16380);")
        tdSql.query(f"desc tb2023")

        tdLog.info(f"=============== modify column for normal table")
        tdSql.execute(f"create table ntb_ts3841(ts timestamp, c0 varchar(64000));")
        tdSql.execute(f"alter table ntb_ts3841 modify column c0 varchar(64001);")
        tdSql.execute(f"create table ntb1_ts3841(ts timestamp, c0 nchar(15000));")
        tdSql.execute(f"alter table ntb1_ts3841 modify column c0 nchar(15001);")

        tdLog.info(f"=============== error for super table")
        tdSql.execute(f"create table stb2023(ts timestamp, f int) tags(t1 int);")
        tdSql.error(f"alter table stb2023 add column v varchar(65518);")
        tdSql.error(f"alter table stb2023 add column v varchar(65531);")
        tdSql.error(f"alter table stb2023 add column v varchar(65535);")
        tdSql.execute(f"alter table stb2023 add column v varchar(65517);")
        tdSql.error(f"alter table stb2023 modify column v varchar(65518);")
        tdSql.query(f"desc stb2023")
        tdSql.execute(f"alter table stb2023 drop column v")
        tdSql.error(f"alter table stb2023 add column v nchar(16380);")
        tdSql.execute(f"alter table stb2023 add column v nchar(16379);")
        tdSql.error(f"alter table stb2023 modify column v nchar(16380);")
        tdSql.query(f"desc stb2023")

        tdLog.info(f"=============== modify column/tag for super table")
        tdSql.execute(
            f"create table stb_ts3841(ts timestamp, c0 varchar(64000)) tags(t1 binary(16380));"
        )
        tdSql.execute(f"alter table stb_ts3841 modify column c0 varchar(64001);")
        tdSql.execute(f"alter table stb_ts3841 modify tag t1 binary(16381);")
        tdSql.execute(f"alter table stb_ts3841 modify tag t1 binary(16382);")
        tdSql.error(f"alter table stb_ts3841 modify tag t1 binary(16383);")

        tdSql.execute(
            f"create table stb1_ts3841(ts timestamp, c0 nchar(15000)) tags(t1 nchar(4093));"
        )
        tdSql.execute(f"alter table stb1_ts3841 modify column c0 nchar(15001);")
        tdSql.execute(f"alter table stb1_ts3841 modify tag t1 nchar(4094);")
        tdSql.execute(f"alter table stb1_ts3841 modify tag t1 nchar(4095);")
        tdSql.error(f"alter table stb1_ts3841 modify tag t1 nchar(4096);")
        tdSql.error(f"alter table stb1_ts3841 modify tag t1 binary(16382);")

        tdLog.info(f"======= over")
        tdSql.execute(f"drop database d1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
