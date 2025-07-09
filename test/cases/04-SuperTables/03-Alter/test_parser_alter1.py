from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestParserAlter1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_parser_alter_1(self):
        """alter 1

        1.

        Catalog:
            - SuperTable:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/alter1.sim

        """

        dbPrefix = "alt1_db"

        tdLog.info(f"========== alter1.sim")
        db = dbPrefix

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create table stb (ts timestamp, speed double, mileage double) tags(carId int, carModel int)"
        )
        tdSql.execute(f"create table car1 using stb tags (1, 1)")
        tdSql.execute(f"create table car2 using stb tags (2, 1)")
        tdSql.execute(f"create table car3 using stb tags (3, 2)")
        tdSql.execute(f"insert into car1 values (now-1s, 100, 10000)")
        tdSql.execute(f"insert into car2 values (now, 100, 10000)")
        tdSql.execute(f"insert into car3 values (now, 100, 10000)")
        tdSql.execute(f"insert into car1 values (now, 120, 11000)")
        tdLog.info(f"================== add a column")
        tdSql.execute(f"alter table stb add column c1 int")
        tdSql.query(f"describe stb")
        tdSql.checkRows(6)

        tdSql.query(f"select * from stb")
        tdLog.info(f"rows = {tdSql.getRows()})")
        tdSql.checkRows(4)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(1, 3, None)
        tdSql.checkData(2, 3, None)
        tdSql.checkData(3, 3, None)

        tdSql.query(f"select c1 from stb")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select c1+speed from stb")
        tdSql.checkRows(4)

        tdSql.query(f"select c1+speed from car1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)

        tdSql.query(f"select * from car1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(1, 3, None)

        tdLog.info(f"================== insert values into table")
        tdSql.execute(
            f"insert into car1 values (now, 1, 1,1 ) (now +1s, 2,2,2) car2 values (now, 1,3,3)"
        )

        tdSql.query(f"select c1+speed from stb where c1 > 0")
        tdSql.checkRows(3)

        tdLog.info(f"================== add a tag")
        tdSql.execute(f"alter table stb add tag t1 int")
        tdSql.query(f"describe stb")
        tdSql.checkRows(7)
        tdSql.checkData(6, 0, "t1")

        tdLog.info(f"================== change a tag value")
        tdSql.execute(f"alter table car1 set tag carid=10")
        tdSql.query(f"select distinct carId, carmodel from car1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select * from stb where carid = 10")
        tdSql.checkRows(4)

        tdSql.execute(f"alter table car2 set tag carmodel = 2")
        tdSql.query(f"select * from stb where carmodel = 2")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(3)
