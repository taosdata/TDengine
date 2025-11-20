import time
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestStreamSubQueryVtableChange:
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_vtable_change(self):
        """Subquery: virtual table meta change

        test meta change (add/drop/modify) cases to stream for virtual table in subquery

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-4 Jing Sima Created

        """

        tdStream.createSnode()
        tdSql.execute(f"alter all dnodes 'debugflag 135';")
        tdSql.execute(f"alter all dnodes 'stdebugflag 135';")

        streams = []
        streams.append(self.Basic0())  # add col ref from new vg for virtual normal table
        streams.append(self.Basic1())  # add col ref from new vg for virtual child table
        streams.append(self.Basic2())  # add col ref from new vg for virtual super table
        streams.append(self.Basic3())  # add new virtual child table, and ref from new vg
        streams.append(self.Basic4())  # virtual child table ref from different stable

        tdStream.checkAll(streams)

    class Basic0(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb0"
            self.refdb1 = "refdb1"
            self.refdb2 = "refdb2"
            self.refdb3 = "refdb3"
            self.refdb4 = "refdb4"
            self.triggertb = "trigger01"
            self.reftb = "reftb_0"
            self.vtb = "vtb"

        def create(self):
            tdSql.execute(f"alter dnode 1 'debugFlag 135';")
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb1} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb2} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb3} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb4} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table if not exists  {self.db}.{self.triggertb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb1}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb2}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb3}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb4}.{self.reftb} (cts timestamp, cint int)")

            tdSql.execute(f"create vtable if not exists  {self.db}.{self.vtb} (cts timestamp, c1 int from {self.refdb1}.{self.reftb}.cint, c2 int, c3 int)")

            tdSql.execute(
                f"create stream s0 state_window(cint) from {self.triggertb} into res_tb as select _twstart, count(cts), sum(c1), sum(c2), sum(c3) from {self.vtb};"
            )

        def insert1(self):
            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:05', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:10', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:15', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:20', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:25', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:30', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:35', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:40', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:45', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:50', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:55', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:01:00', 3);",

                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:01:00', 13);",

                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:01:00', 13);",

                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:01:00', 13);",

                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:01:00', 13);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_tb%"',
                func=lambda: tdSql.getRows() == 1,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_tb",
                schema=[
                    ["_twstart", "TIMESTAMP", 8, ""],
                    ["count(cts)", "BIGINT", 8, ""],
                    ["sum(c1)", "BIGINT", 8, ""],
                    ["sum(c2)", "BIGINT", 8, ""],
                    ["sum(c3)", "BIGINT", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 2
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None),
            )

        def insert2(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vtb} alter column c2 set {self.refdb2}.{self.reftb}.cint")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:05', 4);",
            ]
            tdSql.executes(sqls)

        def check2(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 3
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None),

            )
        def insert3(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vtb} alter column c3 set {self.refdb3}.{self.reftb}.cint")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:10', 5);",
            ]
            tdSql.executes(sqls)

        def check3(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 4
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91),
            )

        def insert4(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vtb} alter column c2 set null")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:15', 6);",
            ]
            tdSql.executes(sqls)

        def check4(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 5
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91),

            )

        def insert5(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vtb} alter column c3 set null")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:20', 7);",
            ]
            tdSql.executes(sqls)

        def check5(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 6
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91)
                             and tdSql.compareData(5, 0, "2025-01-01 00:01:15")
                             and tdSql.compareData(5, 1, 13)
                             and tdSql.compareData(5, 2, 91)
                             and tdSql.compareData(5, 3, None)
                             and tdSql.compareData(5, 4, None),

            )

        def insert6(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vtb} alter column c3 set {self.refdb3}.{self.reftb}.cint")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:25', 8);",
            ]
            tdSql.executes(sqls)

        def check6(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 7
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91)
                             and tdSql.compareData(5, 0, "2025-01-01 00:01:15")
                             and tdSql.compareData(5, 1, 13)
                             and tdSql.compareData(5, 2, 91)
                             and tdSql.compareData(5, 3, None)
                             and tdSql.compareData(5, 4, None)
                             and tdSql.compareData(6, 0, "2025-01-01 00:01:20")
                             and tdSql.compareData(6, 1, 13)
                             and tdSql.compareData(6, 2, 91)
                             and tdSql.compareData(6, 3, None)
                             and tdSql.compareData(6, 4, 91),
            )

        def insert7(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vtb} alter column c2 set {self.refdb2}.{self.reftb}.cint")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:30', 9);",
            ]
            tdSql.executes(sqls)

        def check7(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 8
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91)
                             and tdSql.compareData(5, 0, "2025-01-01 00:01:15")
                             and tdSql.compareData(5, 1, 13)
                             and tdSql.compareData(5, 2, 91)
                             and tdSql.compareData(5, 3, None)
                             and tdSql.compareData(5, 4, None)
                             and tdSql.compareData(6, 0, "2025-01-01 00:01:20")
                             and tdSql.compareData(6, 1, 13)
                             and tdSql.compareData(6, 2, 91)
                             and tdSql.compareData(6, 3, None)
                             and tdSql.compareData(6, 4, 91)
                             and tdSql.compareData(7, 0, "2025-01-01 00:01:25")
                             and tdSql.compareData(7, 1, 13)
                             and tdSql.compareData(7, 2, 91)
                             and tdSql.compareData(7, 3, 91)
                             and tdSql.compareData(7, 4, 91),
            )

        def insert8(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vtb} alter column c3 set {self.refdb4}.{self.reftb}.cint")
            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:35', 10);",
            ]
            tdSql.executes(sqls)

        def check8(self):
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 9
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91)
                             and tdSql.compareData(5, 0, "2025-01-01 00:01:15")
                             and tdSql.compareData(5, 1, 13)
                             and tdSql.compareData(5, 2, 91)
                             and tdSql.compareData(5, 3, None)
                             and tdSql.compareData(5, 4, None)
                             and tdSql.compareData(6, 0, "2025-01-01 00:01:20")
                             and tdSql.compareData(6, 1, 13)
                             and tdSql.compareData(6, 2, 91)
                             and tdSql.compareData(6, 3, None)
                             and tdSql.compareData(6, 4, 91)
                             and tdSql.compareData(7, 0, "2025-01-01 00:01:25")
                             and tdSql.compareData(7, 1, 13)
                             and tdSql.compareData(7, 2, 91)
                             and tdSql.compareData(7, 3, 91)
                             and tdSql.compareData(7, 4, 91)
                             and tdSql.compareData(8, 0, "2025-01-01 00:01:30")
                             and tdSql.compareData(8, 1, 13)
                             and tdSql.compareData(8, 2, 91)
                             and tdSql.compareData(8, 3, 91)
                             and tdSql.compareData(8, 4, 91),
            )

    class Basic1(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb1"
            self.refdb1 = "refdb1"
            self.refdb2 = "refdb2"
            self.refdb3 = "refdb3"
            self.refdb4 = "refdb4"
            self.triggertb = "trigger11"
            self.reftb = "reftb_1"
            self.vstb = "vstb"
            self.vctb = "vctb"

        def create(self):
            tdSql.execute(f"alter dnode 1 'debugFlag 135';")
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb1} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb2} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb3} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb4} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table if not exists  {self.db}.{self.triggertb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb1}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb2}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb3}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb4}.{self.reftb} (cts timestamp, cint int)")

            tdSql.execute(f"create stable if not exists  {self.db}.{self.vstb} (cts timestamp, c1 int, c2 int, c3 int) tags (t1 int, t2 int) virtual 1")
            tdSql.execute(f"create vtable if not exists  {self.db}.{self.vctb} (c1 from {self.refdb1}.{self.reftb}.cint) using {self.vstb} tags (1,1)")

            tdSql.execute(
                f"create stream s1 state_window(cint) from {self.triggertb} into res_tb as select _twstart, count(cts), sum(c1), sum(c2), sum(c3) from {self.vctb};"
            )

        def insert1(self):
            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:05', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:10', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:15', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:20', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:25', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:30', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:35', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:40', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:45', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:50', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:55', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:01:00', 3);",

                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:01:00', 13);",

                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:01:00', 13);",

                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:01:00', 13);",

                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:01:00', 13);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_tb%"',
                func=lambda: tdSql.getRows() == 1,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_tb",
                schema=[
                    ["_twstart", "TIMESTAMP", 8, ""],
                    ["count(cts)", "BIGINT", 8, ""],
                    ["sum(c1)", "BIGINT", 8, ""],
                    ["sum(c2)", "BIGINT", 8, ""],
                    ["sum(c3)", "BIGINT", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 2
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None),
            )

        def insert2(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vctb} alter column c2 set {self.refdb2}.{self.reftb}.cint")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:05', 4);",
            ]
            tdSql.executes(sqls)

        def check2(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 3
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None),

            )
        def insert3(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vctb} alter column c3 set {self.refdb3}.{self.reftb}.cint")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:10', 5);",
            ]
            tdSql.executes(sqls)

        def check3(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 4
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91),
            )

        def insert4(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vctb} alter column c2 set null")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:15', 6);",
            ]
            tdSql.executes(sqls)

        def check4(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 5
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91),

            )

        def insert5(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vctb} alter column c3 set null")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:20', 7);",
            ]
            tdSql.executes(sqls)

        def check5(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 6
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91)
                             and tdSql.compareData(5, 0, "2025-01-01 00:01:15")
                             and tdSql.compareData(5, 1, 13)
                             and tdSql.compareData(5, 2, 91)
                             and tdSql.compareData(5, 3, None)
                             and tdSql.compareData(5, 4, None),

            )

        def insert6(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vctb} alter column c3 set {self.refdb3}.{self.reftb}.cint")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:25', 8);",
            ]
            tdSql.executes(sqls)

        def check6(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 7
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91)
                             and tdSql.compareData(5, 0, "2025-01-01 00:01:15")
                             and tdSql.compareData(5, 1, 13)
                             and tdSql.compareData(5, 2, 91)
                             and tdSql.compareData(5, 3, None)
                             and tdSql.compareData(5, 4, None)
                             and tdSql.compareData(6, 0, "2025-01-01 00:01:20")
                             and tdSql.compareData(6, 1, 13)
                             and tdSql.compareData(6, 2, 91)
                             and tdSql.compareData(6, 3, None)
                             and tdSql.compareData(6, 4, 91),
            )

        def insert7(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vctb} alter column c2 set {self.refdb2}.{self.reftb}.cint")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:30', 9);",
            ]
            tdSql.executes(sqls)

        def check7(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 8
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91)
                             and tdSql.compareData(5, 0, "2025-01-01 00:01:15")
                             and tdSql.compareData(5, 1, 13)
                             and tdSql.compareData(5, 2, 91)
                             and tdSql.compareData(5, 3, None)
                             and tdSql.compareData(5, 4, None)
                             and tdSql.compareData(6, 0, "2025-01-01 00:01:20")
                             and tdSql.compareData(6, 1, 13)
                             and tdSql.compareData(6, 2, 91)
                             and tdSql.compareData(6, 3, None)
                             and tdSql.compareData(6, 4, 91)
                             and tdSql.compareData(7, 0, "2025-01-01 00:01:25")
                             and tdSql.compareData(7, 1, 13)
                             and tdSql.compareData(7, 2, 91)
                             and tdSql.compareData(7, 3, 91)
                             and tdSql.compareData(7, 4, 91),
            )

        def insert8(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vctb} alter column c3 set {self.refdb4}.{self.reftb}.cint")
            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:35', 10);",
            ]
            tdSql.executes(sqls)

        def check8(self):
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 9
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91)
                             and tdSql.compareData(5, 0, "2025-01-01 00:01:15")
                             and tdSql.compareData(5, 1, 13)
                             and tdSql.compareData(5, 2, 91)
                             and tdSql.compareData(5, 3, None)
                             and tdSql.compareData(5, 4, None)
                             and tdSql.compareData(6, 0, "2025-01-01 00:01:20")
                             and tdSql.compareData(6, 1, 13)
                             and tdSql.compareData(6, 2, 91)
                             and tdSql.compareData(6, 3, None)
                             and tdSql.compareData(6, 4, 91)
                             and tdSql.compareData(7, 0, "2025-01-01 00:01:25")
                             and tdSql.compareData(7, 1, 13)
                             and tdSql.compareData(7, 2, 91)
                             and tdSql.compareData(7, 3, 91)
                             and tdSql.compareData(7, 4, 91)
                             and tdSql.compareData(8, 0, "2025-01-01 00:01:30")
                             and tdSql.compareData(8, 1, 13)
                             and tdSql.compareData(8, 2, 91)
                             and tdSql.compareData(8, 3, 91)
                             and tdSql.compareData(8, 4, 91),
            )

    class Basic2(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb2"
            self.refdb1 = "refdb1"
            self.refdb2 = "refdb2"
            self.refdb3 = "refdb3"
            self.refdb4 = "refdb4"
            self.triggertb = "trigger21"
            self.reftb = "reftb_2"
            self.vstb = "vstb"
            self.vctb = "vctb"

        def create(self):
            tdSql.execute(f"alter dnode 1 'debugFlag 135';")
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb1} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb2} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb3} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb4} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table if not exists  {self.db}.{self.triggertb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb1}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb2}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb3}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb4}.{self.reftb} (cts timestamp, cint int)")

            tdSql.execute(f"create stable if not exists  {self.db}.{self.vstb} (cts timestamp, c1 int, c2 int, c3 int) tags (t1 int, t2 int) virtual 1")
            tdSql.execute(f"create vtable if not exists  {self.db}.{self.vctb} (c1 from {self.refdb1}.{self.reftb}.cint) using {self.vstb} tags (1,1)")

            tdSql.execute(
                f"create stream s1 state_window(cint) from {self.triggertb} into res_tb as select _twstart, count(cts), sum(c1), sum(c2), sum(c3) from {self.vstb};"
            )

        def insert1(self):
            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:05', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:10', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:15', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:20', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:25', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:30', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:35', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:40', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:45', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:50', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:55', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:01:00', 3);",

                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:01:00', 13);",

                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:01:00', 13);",

                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:01:00', 13);",

                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb4}.{self.reftb} values ('2025-01-01 00:01:00', 13);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_tb%"',
                func=lambda: tdSql.getRows() == 1,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_tb",
                schema=[
                    ["_twstart", "TIMESTAMP", 8, ""],
                    ["count(cts)", "BIGINT", 8, ""],
                    ["sum(c1)", "BIGINT", 8, ""],
                    ["sum(c2)", "BIGINT", 8, ""],
                    ["sum(c3)", "BIGINT", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 2
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None),
            )

        def insert2(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vctb} alter column c2 set {self.refdb2}.{self.reftb}.cint")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:05', 4);",
            ]
            tdSql.executes(sqls)

        def check2(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 3
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None),

            )
        def insert3(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vctb} alter column c3 set {self.refdb3}.{self.reftb}.cint")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:10', 5);",
            ]
            tdSql.executes(sqls)

        def check3(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 4
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91),
            )
        def insert4(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vctb} alter column c2 set null")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:15', 6);",
            ]
            tdSql.executes(sqls)

        def check4(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 5
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91),

            )

        def insert5(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vctb} alter column c3 set null")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:20', 7);",
            ]
            tdSql.executes(sqls)

        def check5(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 6
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91)
                             and tdSql.compareData(5, 0, "2025-01-01 00:01:15")
                             and tdSql.compareData(5, 1, 13)
                             and tdSql.compareData(5, 2, 91)
                             and tdSql.compareData(5, 3, None)
                             and tdSql.compareData(5, 4, None),

            )

        def insert6(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vctb} alter column c3 set {self.refdb3}.{self.reftb}.cint")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:25', 8);",
            ]
            tdSql.executes(sqls)

        def check6(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 7
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91)
                             and tdSql.compareData(5, 0, "2025-01-01 00:01:15")
                             and tdSql.compareData(5, 1, 13)
                             and tdSql.compareData(5, 2, 91)
                             and tdSql.compareData(5, 3, None)
                             and tdSql.compareData(5, 4, None)
                             and tdSql.compareData(6, 0, "2025-01-01 00:01:20")
                             and tdSql.compareData(6, 1, 13)
                             and tdSql.compareData(6, 2, 91)
                             and tdSql.compareData(6, 3, None)
                             and tdSql.compareData(6, 4, 91),
            )

        def insert7(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vctb} alter column c2 set {self.refdb2}.{self.reftb}.cint")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:30', 9);",
            ]
            tdSql.executes(sqls)

        def check7(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 8
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91)
                             and tdSql.compareData(5, 0, "2025-01-01 00:01:15")
                             and tdSql.compareData(5, 1, 13)
                             and tdSql.compareData(5, 2, 91)
                             and tdSql.compareData(5, 3, None)
                             and tdSql.compareData(5, 4, None)
                             and tdSql.compareData(6, 0, "2025-01-01 00:01:20")
                             and tdSql.compareData(6, 1, 13)
                             and tdSql.compareData(6, 2, 91)
                             and tdSql.compareData(6, 3, None)
                             and tdSql.compareData(6, 4, 91)
                             and tdSql.compareData(7, 0, "2025-01-01 00:01:25")
                             and tdSql.compareData(7, 1, 13)
                             and tdSql.compareData(7, 2, 91)
                             and tdSql.compareData(7, 3, 91)
                             and tdSql.compareData(7, 4, 91),
            )

        def insert8(self):
            tdSql.execute(f"alter vtable {self.db}.{self.vctb} alter column c3 set {self.refdb4}.{self.reftb}.cint")
            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:35', 10);",
            ]
            tdSql.executes(sqls)

        def check8(self):
            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 9
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 13)
                             and tdSql.compareData(2, 2, 91)
                             and tdSql.compareData(2, 3, 91)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 13)
                             and tdSql.compareData(3, 2, 91)
                             and tdSql.compareData(3, 3, 91)
                             and tdSql.compareData(3, 4, 91)
                             and tdSql.compareData(4, 0, "2025-01-01 00:01:10")
                             and tdSql.compareData(4, 1, 13)
                             and tdSql.compareData(4, 2, 91)
                             and tdSql.compareData(4, 3, None)
                             and tdSql.compareData(4, 4, 91)
                             and tdSql.compareData(5, 0, "2025-01-01 00:01:15")
                             and tdSql.compareData(5, 1, 13)
                             and tdSql.compareData(5, 2, 91)
                             and tdSql.compareData(5, 3, None)
                             and tdSql.compareData(5, 4, None)
                             and tdSql.compareData(6, 0, "2025-01-01 00:01:20")
                             and tdSql.compareData(6, 1, 13)
                             and tdSql.compareData(6, 2, 91)
                             and tdSql.compareData(6, 3, None)
                             and tdSql.compareData(6, 4, 91)
                             and tdSql.compareData(7, 0, "2025-01-01 00:01:25")
                             and tdSql.compareData(7, 1, 13)
                             and tdSql.compareData(7, 2, 91)
                             and tdSql.compareData(7, 3, 91)
                             and tdSql.compareData(7, 4, 91)
                             and tdSql.compareData(8, 0, "2025-01-01 00:01:30")
                             and tdSql.compareData(8, 1, 13)
                             and tdSql.compareData(8, 2, 91)
                             and tdSql.compareData(8, 3, 91)
                             and tdSql.compareData(8, 4, 91),
            )
    class Basic3(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb3"
            self.refdb1 = "refdb1"
            self.refdb2 = "refdb2"
            self.refdb3 = "refdb3"
            self.triggertb = "trigger31"
            self.reftb = "reftb_3"
            self.vstb = "vstb"
            self.vctb1 = "vctb1"
            self.vctb2 = "vctb2"
            self.vctb3 = "vctb3"

        def create(self):
            tdSql.execute(f"alter dnode 1 'debugFlag 135';")
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb1} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb2} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"create database if not exists {self.refdb3} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table if not exists  {self.db}.{self.triggertb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb1}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb2}.{self.reftb} (cts timestamp, cint int)")
            tdSql.execute(f"create table if not exists  {self.refdb3}.{self.reftb} (cts timestamp, cint int)")

            tdSql.execute(f"create stable if not exists  {self.db}.{self.vstb} (cts timestamp, c1 int, c2 int, c3 int) tags (t1 int, t2 int) virtual 1")
            tdSql.execute(f"create vtable if not exists  {self.db}.{self.vctb1} (c1 from {self.refdb1}.{self.reftb}.cint) using {self.vstb} tags (1,1)")

            tdSql.execute(
                f"create stream s1 state_window(cint) from {self.triggertb} into res_tb as select _twstart, count(cts), sum(c1), sum(c2), sum(c3) from {self.vstb};"
            )

        def insert1(self):
            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:05', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:10', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:15', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:20', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:25', 1);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:30', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:35', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:40', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:45', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:50', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:00:55', 2);",
                f"insert into {self.triggertb} values ('2025-01-01 00:01:00', 3);",

                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb1}.{self.reftb} values ('2025-01-01 00:01:00', 13);",

                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb2}.{self.reftb} values ('2025-01-01 00:01:00', 13);",

                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:05', 2);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:10', 3);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:15', 4);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:20', 5);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:25', 6);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:30', 7);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:35', 8);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:40', 9);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:45', 10);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:50', 11);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:00:55', 12);",
                f"insert into {self.refdb3}.{self.reftb} values ('2025-01-01 00:01:00', 13);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_tb%"',
                func=lambda: tdSql.getRows() == 1,
            )

            tdSql.checkTableSchema(
                dbname=self.db,
                tbname="res_tb",
                schema=[
                    ["_twstart", "TIMESTAMP", 8, ""],
                    ["count(cts)", "BIGINT", 8, ""],
                    ["sum(c1)", "BIGINT", 8, ""],
                    ["sum(c2)", "BIGINT", 8, ""],
                    ["sum(c3)", "BIGINT", 8, ""],
                ],
            )

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 2
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None),
            )

        def insert2(self):
            tdSql.execute(f"create vtable if not exists  {self.db}.{self.vctb2} (c1 from {self.refdb2}.{self.reftb}.cint) using {self.vstb} tags (2,2)")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:05', 4);",
            ]
            tdSql.executes(sqls)

        def check2(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 3
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 26)
                             and tdSql.compareData(2, 2, 182)
                             and tdSql.compareData(2, 3, None)
                             and tdSql.compareData(2, 4, None),

            )
        def insert3(self):
            tdSql.execute(f"create vtable if not exists  {self.db}.{self.vctb3} (c1 from {self.refdb3}.{self.reftb}.cint) using {self.vstb} tags (3,3)")

            sqls = [
                f"insert into {self.triggertb} values ('2025-01-01 00:01:10', 5);",
            ]
            tdSql.executes(sqls)

        def check3(self):

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_tb",
                func=lambda: tdSql.getRows() == 4
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, 13)
                             and tdSql.compareData(0, 2, 91)
                             and tdSql.compareData(0, 3, None)
                             and tdSql.compareData(0, 4, None)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:30")
                             and tdSql.compareData(1, 1, 13)
                             and tdSql.compareData(1, 2, 91)
                             and tdSql.compareData(1, 3, None)
                             and tdSql.compareData(1, 4, None)
                             and tdSql.compareData(2, 0, "2025-01-01 00:01:00")
                             and tdSql.compareData(2, 1, 26)
                             and tdSql.compareData(2, 2, 182)
                             and tdSql.compareData(2, 3, None)
                             and tdSql.compareData(2, 4, None)
                             and tdSql.compareData(3, 0, "2025-01-01 00:01:05")
                             and tdSql.compareData(3, 1, 39)
                             and tdSql.compareData(3, 2, 273)
                             and tdSql.compareData(3, 3, None)
                             and tdSql.compareData(3, 4, None),
            )
    
    class Basic4(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb4"
            self.stbName = "stb"
            self.ntbName = 'ntb'
            self.vstbName = "vstb"
            self.vntbName = "vntb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8 precision '{TestStreamSubQueryVtableChange.precision}'")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.db}.{self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table {self.db}.ct1 using {self.db}.{self.stbName} tags(1)")

            tdSql.execute(f"create table if not exists  {self.db}.{self.ntbName} (cts timestamp, cint int)")
            
            tdSql.execute(f"create table if not exists  {self.db}.{self.vstbName} (cts timestamp, cint int) tags (tint int) virtual 1")

            tdSql.execute(f"create vtable vct1 (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2 (cint from {self.db}.{self.ntbName}.cint) using {self.db}.{self.vstbName} tags(2)")
            
            tdSql.execute(
                f"create stream s4 state_window(cint) from {self.vstbName} partition by tbname, tint into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            

        def insert1(self):
            sqls = [
                f"insert into {self.db}.{self.ntbName} values ('2025-01-01 00:00:00', 1);",
                f"insert into {self.db}.{self.ntbName} values ('2025-01-01 00:00:01', 10);",
                f"insert into {self.db}.{self.ntbName} values ('2025-01-01 00:00:02', 13);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:05', 1);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:07', 1);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:09', 1);",
                f"insert into {self.db}.ct1 values ('2025-01-01 00:00:25', 13);",
            ]
            tdSql.executes(sqls)

        def check1(self):
            tdSql.checkResultsByFunc(
                sql=f'select * from information_schema.ins_tables where db_name="{self.db}" and table_name like "res_stb%"',
                func=lambda: tdSql.getRows() == 2,
            )

            tdSql.checkResultsByFunc(
                sql=f"select * from {self.db}.res_stb order by firstts",
                func=lambda: tdSql.getRows() == 3
                             and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 1, "2025-01-01 00:00:00")
                             and tdSql.compareData(0, 2, 1)
                             and tdSql.compareData(0, 3, 1)
                             and tdSql.compareData(0, 4, 1)
                             and tdSql.compareData(0, 5, "vct2")
                             and tdSql.compareData(0, 6, 2)
                             and tdSql.compareData(1, 0, "2025-01-01 00:00:01")
                             and tdSql.compareData(1, 1, "2025-01-01 00:00:01")
                             and tdSql.compareData(1, 2, 1)
                             and tdSql.compareData(1, 3, 10)
                             and tdSql.compareData(1, 4, 10)
                             and tdSql.compareData(1, 5, "vct2")
                             and tdSql.compareData(1, 6, 2)
                             and tdSql.compareData(2, 0, "2025-01-01 00:00:05")
                             and tdSql.compareData(2, 1, "2025-01-01 00:00:09")
                             and tdSql.compareData(2, 2, 3)
                             and tdSql.compareData(2, 3, 3)
                             and tdSql.compareData(2, 4, 1)
                             and tdSql.compareData(2, 5, "vct1")
                             and tdSql.compareData(2, 6, 1)
            )
