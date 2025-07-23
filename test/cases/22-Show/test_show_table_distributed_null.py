from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck

class TestShowTableDistributed:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_show_table_distributed(self):
        """Show Table Distributed Test

        1.Create db
        2.Create supper table and sub table
        4.Insert data into sub table
        5.Run show table distributed command

        Catalog:
            - Show

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TS-6908

        History:
            - 2025-7-23 Ethan liu adds test for show table distributed

        """

        tdLog.info(f"========== start show table distributed test")
        tdSql.execute(f"drop database if exists test_show_table")
        tdSql.execute(f"create database test_show_table")
        tdSql.execute(f"use test_show_table")

        # create super table and sub table
        tdSql.execute(f"create table super_t (ts timestamp, second_key varchar(100) composite key, alarm varchar(50), event varchar(50), dest varchar(50), reason varchar(50), type int, category int, name varchar(100)) tags (id VARCHAR(25), location VARCHAR(100), part_no INT)")
        tdSql.execute(f"create table sub_t0 using super_t tags('t1', 'value1', 1)")
        tdSql.execute(f"create table sub_t1 using super_t tags('t2', 'value2', 2)")
        tdSql.execute(f"create table sub_t2 using super_t tags('t3', 'value3', 3)")

        # insert data into sub table
        tdSql.execute(f"insert into sub_t0 values (now, '01', '00', 'up', '90', null, 2, 2, '')")
        tdSql.execute(f"insert into sub_t1 values (now, '11', '10', 'up', '90', null, 2, 2, '')")
        tdSql.execute(f"insert into sub_t2 values (now, '22', '20', 'up', '90', null, 2, 2, '')")

        # run show table distributed command, it should return internal error
        tdSql.query(f"show table distributed super_t")
        tdSql.checkNotEqual(tdSql.getRows(), 0)
        tdLog.info(f"end show table distributed test successfully")
