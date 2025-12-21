###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import re
from collections import defaultdict
import random
import string
import threading
import requests
import time
import taos

from .log import *
from .sql import *
from .server.dnodes import *
from .common import *
from datetime import datetime
from enum import Enum
from new_test_framework.utils import clusterComCheck


class StreamTableType(Enum):
    TYPE_SUP_TABLE = "SUP_TABLE"
    TYPE_SUB_TABLE = "SUB_TABLE"
    TYPE_NORMAL_TABLE = "NORMAL_TABLE"
    TYPE_VIRTUAL_TABLE = "VIRTUAL_TABLE"
    TYPE_SYSTEM_TABLE = "SYSTEM_TABLE"


class StreamResultCheckMode(Enum):
    CHHECK_DEFAULT = "CHECK_DEFAULT"
    CHECK_ARRAY_BY_SQL = "CHECK_ARRAY_BY_SQL"
    CHECK_ROW_BY_SQL = "CHECK_ROW_BY_SQL"
    CHECK_BY_FILE = "CHECK_BY_FILE"


class QuerySqlCase:
    def __init__(
        self,
        query_sql,
        expected_sql="",
        check_mode=StreamResultCheckMode.CHHECK_DEFAULT,
        generate_file=False,
        exp_sql_param_mapping={},
    ):
        self.query_sql = query_sql
        self.expected_sql = expected_sql
        self.check_mode = check_mode
        self.generate_file = generate_file
        self.exp_sql_param_mapping = exp_sql_param_mapping


class StreamTable:
    def __init__(self, db, tbName, type):
        self.tableType = type
        self.tbName = tbName
        self.db = db

        self.precision = "ms"
        self.start = "2025-01-01 00.00.00"
        self.interval = 30
        self.logOpen = False

        self.default_columns = (
            "cts timestamp,"
            "cint int,"
            "cuint int unsigned,"
            "cbigint bigint,"
            "cubigint bigint unsigned,"
            "cfloat float,"
            "cdouble double,"
            "cvarchar varchar(32),"
            "csmallint smallint,"
            "cusmallint smallint unsigned,"
            "ctinyint tinyint,"
            "cutinyint tinyint unsigned,"
            "cbool bool,"
            "cnchar nchar(32),"
            "cvarbinary varbinary(32),"
            "cdecimal8 decimal(8),"
            "cdecimal16 decimal(16),"
            "cgeometry geometry(32)"
        )
        self.default_tags = (
            "  tts timestamp"
            ", tint int"
            ", tuint int unsigned"
            ", tbigint bigint"
            ", tubigint bigint unsigned"
            ", tfloat float"
            ", tdouble double"
            ", tvarchar varchar(32)"
            ", tsmallint smallint"
            ", tusmallint smallint unsigned"
            ", ttinyint tinyint"
            ", tutinyint tinyint unsigned"
            ", tbool bool"
            ", tnchar nchar(16)"
            ", tvarbinary varbinary(32)"
            ", tgeometry geometry(32)"
        )
        self.columns = self.default_columns  # 当前使用的列定义（可被自定义覆盖）
        self.tags = self.default_tags

        self.custom_generators = {}  # name -> function(row, ts) -> str
        self.created = False

    def setInterval(self, interval):
        """
        设置时间间隔
        :param interval: int, 时间间隔，单位为秒
        """
        self.interval = interval

    def setPrecision(self, precision):
        """
        设置时间精度
        :param precision: str, 时间精度，支持 "ms", "us", "ns"
        """
        if precision not in ["ms", "us", "ns"]:
            raise ValueError(
                "Invalid precision. Supported values are 'ms', 'us', 'ns'."
            )
        self.precision = precision

    def setStart(self, start):
        """
        设置起始时间
        :param start: str, 起始时间，格式为 "YYYY-MM-DD HH.MM.SS"
        """
        try:
            datetime.strptime(start, "%Y-%m-%d %H.%M.%S")
            self.start = start
        except ValueError:
            raise ValueError("Invalid start time format. Use 'YYYY-MM-DD HH.MM.SS'.")

    def setLogOpen(self, logOpen):
        """
        设置日志开关
        :param logOpen: bool, 是否开启日志
        """
        self.logOpen = logOpen

    def createTable(self, subTableCount=200):
        if (
            self.tableType == StreamTableType.TYPE_SUP_TABLE
            or self.tableType == StreamTableType.TYPE_SUB_TABLE
        ):
            tdLog.info(f"create super table {self.db}.{self.tbName}")
            self.__createSupTable()
            self.__createSubTables(0, subTableCount)
        elif self.tableType == StreamTableType.TYPE_NORMAL_TABLE:
            tdLog.info(f"create normal table {self.db}.{self.tbName}")
            self.__createNormalTable()
        self.created = True

    def appendSubTables(self, startTbIndex, endTbIndex):
        """
        向超级表中追加子表
        :param startTbIndex: int, 起始子表索引
        :param endTbIndex: int, 结束子表索引
        """
        if (
            self.tableType == StreamTableType.TYPE_SUP_TABLE
            or self.tableType == StreamTableType.TYPE_SUB_TABLE
        ):
            tdLog.info(
                f"create sub tables to sub tables from {startTbIndex} to {endTbIndex}"
            )
            self.__createSubTables(startTbIndex, endTbIndex)

    def set_columns(self, column_def: str):
        """
        允许用户自定义列定义
        :param column_def: str，例如 "ts timestamp, val int"
        """
        self.columns = column_def

    def reset_columns(self):
        """重置为默认列定义"""
        self.columns = self.default_columns

    def append_subtable_data(self, tbName, start_row, end_row):
        """
        向指定子表追加数据
        :param tbName: str, 子表名称
        :param start_row: int, 起始行索引
        :param end_row: int, 结束行索引
        """
        if self.created != True:
            self.createTable()

        full_table_name = f"{self.db}.{tbName}"

        if (
            self.tableType == StreamTableType.TYPE_SUP_TABLE
            or self.tableType == StreamTableType.TYPE_SUB_TABLE
        ):
            self.__info(
                f"append data to sub table {full_table_name} from {start_row} to {end_row}"
            )
            self.__append_data(full_table_name, start_row, end_row)

    def append_data(self, start_row, end_row):
        """
        向表中追加数据
        :param start_row: int, 起始行索引
        :param end_row: int, 结束行索引
        """

        if self.created != True:
            self.createTable()

        full_table_name = f"{self.db}.{self.tbName}"

        if (
            self.tableType == StreamTableType.TYPE_SUP_TABLE
            or self.tableType == StreamTableType.TYPE_SUB_TABLE
        ):
            tbList = tdSql.query(
                f"select table_name from information_schema.ins_tables where stable_name='{self.tbName}'",
                row_tag=True,
            )
            for r in range(len(tbList)):
                tbName = tbList[r][0]
                fullName = f"{self.db}.{tbName}"
                self.__info(
                    f"append data to sub table {fullName} from {start_row} to {end_row}"
                )
                self.__append_data(fullName, start_row, end_row)
        elif self.tableType == StreamTableType.TYPE_NORMAL_TABLE:
            self.__append_data(full_table_name, start_row, end_row)

    def update_data(self, start_row, end_row):
        """
        更新表中的数据
        :param start_row: int, 起始行索引
        :param end_row: int, 结束行索引
        """
        full_table_name = f"{self.db}.{self.tbName}"

        if (
            self.tableType == StreamTableType.TYPE_SUP_TABLE
            or self.tableType == StreamTableType.TYPE_SUB_TABLE
        ):
            tbList = tdSql.query(
                f"select table_name from information_schema.ins_tables where stable_name='{self.tbName}'",
                row_tag=True,
            )
            for r in range(len(tbList)):
                tbName = tbList[r][0]
                fullName = f"{self.db}.{tbName}"
                self.__info(
                    f"update data in sub table {fullName} from {start_row} to {end_row}"
                )
                self.__append_data(fullName, start_row, end_row, offset=10000)
        elif self.tableType == StreamTableType.TYPE_NORMAL_TABLE:
            self.__append_data(full_table_name, start_row, end_row, offset=10000)

    def update_subtable_data(self, tbName, start_row, end_row):
        """
        更新指定子表中的数据
        :param tbName: str, 子表名称
        :param start_row: int, 起始行索引
        :param end_row: int, 结束行索引
        """
        full_table_name = f"{self.db}.{tbName}"

        if (
            self.tableType == StreamTableType.TYPE_SUP_TABLE
            or self.tableType == StreamTableType.TYPE_SUB_TABLE
        ):
            self.__info(
                f"update data in sub table {full_table_name} from {start_row} to {end_row}"
            )
            self.__append_data(full_table_name, start_row, end_row, offset=10000)

    def delete_data(self, start_row, end_row):
        """
        删除表中的数据
        :param start_row: int, 起始行索引
        :param end_row: int, 结束行索引
        """
        full_table_name = f"{self.db}.{self.tbName}"

        if (
            self.tableType == StreamTableType.TYPE_SUP_TABLE
            or self.tableType == StreamTableType.TYPE_SUB_TABLE
        ):
            tbList = tdSql.query(
                f"select table_name from information_schema.ins_tables where stable_name='{self.tbName}'",
                row_tag=True,
            )
            for r in range(len(tbList)):
                tbName = tbList[r][0]
                fullName = f"{self.db}.{tbName}"
                self.__info(
                    f"delete data in sub table {fullName} from {start_row} to {end_row}"
                )
                self.__delete_data(fullName, start_row, end_row)
        elif self.tableType == StreamTableType.TYPE_NORMAL_TABLE:
            self.__delete_data(full_table_name, start_row, end_row)

    def delete_subtable_data(self, tbName, start_row, end_row):
        """
        删除指定子表中的数据
        :param tbName: str, 子表名称
        :param start_row: int, 起始行索引
        :param end_row: int, 结束行索引
        """
        full_table_name = f"{self.db}.{tbName}"

        if (
            self.tableType == StreamTableType.TYPE_SUP_TABLE
            or self.tableType == StreamTableType.TYPE_SUB_TABLE
        ):
            tdLog.info(
                f"delete data in sub table {full_table_name} from {start_row} to {end_row}"
            )
            self.__delete_data(full_table_name, start_row, end_row)

    def register_column_generator(self, column_name: str, generator_func):
        """
        注册某个列名的自定义数据生成函数
        :param column_name: str, 列名
        :param generator_func: function(row_index: int, timestamp: int) -> str
        """
        self.custom_generators[column_name] = generator_func

    def __append_data(self, full_table_name, start_row, end_row, offset=0):
        # 时间精度
        prec = {
            "us": 1_000_000_000,
            "ns": 1_000_000,
        }.get(self.precision, 1_000)

        # 解析时间
        dt = datetime.strptime(self.start, "%Y-%m-%d %H.%M.%S")
        ts_start = int(dt.timestamp() * prec)
        ts_interval = (int)(self.interval * prec)

        # 解析列名和类型
        columns = self._parse_columns(self.columns)

        start = start_row
        while start < end_row:
            end = min(start + 100, end_row)
            value_list = []

            for row in range(start, end):
                values = []
                for col_name, col_type in columns:
                    values.append(
                        self._generate_value(
                            col_name,
                            col_type,
                            row + offset,
                            ts_start + row * ts_interval,
                        )
                    )
                value_list.append(f"({', '.join(values)})")

            sql = f"INSERT INTO {full_table_name} VALUES " + ", ".join(value_list)
            tdSql.execute(sql)
            start = end

        self.__info(
            f"Appended {end_row - start_row} rows to {full_table_name} from {start_row} to {end_row}"
        )

    def __delete_data(self, full_table_name, start_row, end_row):
        """删除指定范围内的数据"""
        # 时间精度
        prec = {
            "us": 1_000_000_000,
            "ns": 1_000_000,
        }.get(self.precision, 1_000)

        # 解析时间
        dt = datetime.strptime(self.start, "%Y-%m-%d %H.%M.%S")
        ts_start = int(dt.timestamp() * prec)
        ts_interval = self.interval * prec

        ts1 = ts_start + start_row * ts_interval
        ts2 = ts_start + end_row * ts_interval
        sql = f"DELETE FROM {full_table_name} WHERE cts >= {ts1} and cts < {ts2}"
        tdSql.execute(sql)

        self.__info(
            f"Deleted rows from {full_table_name} from {start_row} to {end_row}"
        )

    def _parse_columns(self, column_str):
        """将列字符串解析为 [(name, type), ...]"""
        parts = [p.strip() for p in column_str.strip().split(",")]
        columns = []
        for p in parts:
            match = re.match(r"(\w+)\s+([\w()]+(?:\s+unsigned)?)", p)
            if match:
                col_name, col_type = match.groups()
                columns.append((col_name, col_type.lower()))
        return columns

    def _generate_value(self, name, col_type, row, ts):
        if name in self.custom_generators:
            return self.custom_generators[name](row)
        """根据列类型自动生成测试值"""
        if "timestamp" in col_type:
            return str(ts)
        elif "int" in col_type:
            if "tinyint" in col_type:
                return str(row % 128)
            elif "smallint" in col_type:
                return str(row % 32000)
            elif "bigint" in col_type:
                return str(row)
            elif "unsigned" in col_type:
                return str(abs(row))
            else:
                return str(row % 100)
        elif "float" in col_type:
            return str(float(row))
        elif "double" in col_type:
            return str(float(row) * 1.1)
        elif "bool" in col_type:
            return "NULL" if row % 20 == 1 else str(row % 2)
        elif "varchar" in col_type or "nchar" in col_type:
            return f"'{name}_{row}'"
        elif "varbinary" in col_type:
            return f"'{name}_{row}'"
        elif "decimal" in col_type:
            return "'0'" if row % 3 == 1 else "'8'"
        elif "geometry" in col_type:
            return "'POINT(1.0 1.0)'" if row % 3 == 1 else "'POINT(2.0 2.0)'"
        else:
            return "'UNKNOWN'"

    def __createNormalTable(self):
        self.__info(f"create normal table")
        tdSql.execute(f"create table {self.db}.{self.tbName} ({self.columns})")

    def __createSubTables(self, startTbIndex, endTbIndex):
        dt = datetime.strptime(self.start, "%Y-%m-%d %H.%M.%S")
        if self.precision == "us":
            prec = 1000 * 1000 * 1000
        elif self.precision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsNext = tsStart + 86400 * prec

        self.__info(f"create total {endTbIndex-startTbIndex} child tables")

        start = startTbIndex
        while start < endTbIndex:
            end = min(start + 100, endTbIndex)
            sql = "create table "
            for table in range(start, end):
                tts = tsStart if table % 3 == 1 else tsNext
                tint = table % 3 if table % 20 != 1 else "NULL"
                tuint = table % 4
                tbigint = table % 5
                tubigint = table % 6
                tfloat = table % 7
                tdouble = table % 8
                tvarchar = "SanFrancisco" if table % 3 == 1 else "LosAngeles"
                tsmallint = table % 9
                tusmallint = table % 10
                ttinyint = table % 11
                tutinyint = table % 12
                tbool = table % 2
                tnchar = tvarchar
                tvarbinary = tvarchar
                tgeometry = "POINT(1.0 1.0)" if table % 3 == 1 else "POINT(2.0 2.0)"
                sql += f"{self.db}.{self.tbName}_{table} using {self.db}.{self.tbName} tags({tts}, '{tint}', {tuint}, {tbigint}, {tubigint}, {tfloat}, {tdouble}, '{tvarchar}', {tsmallint}, {tusmallint}, {ttinyint}, {tutinyint}, {tbool}, '{tnchar}', '{tvarbinary}', '{tgeometry}') "
            tdSql.execute(sql)
            start = end

    def __createSupTable(self):
        self.__info(f"create super table")
        tdSql.execute(
            f"create stable {self.db}.{self.tbName} ({self.columns}) tags({self.tags})"
        )

    def __info(self, info, *args, **kwargs):
        if self.logOpen:
            tdLog.info(info, args, kwargs)


class StreamUtil:
    def __init__(self):
        self.vgroups = 1  # 默认分区组数

    def init_database(self, db):
        tdLog.info(f"create databases {db}")
        tdSql.prepare(dbname=db, vgroups=self.vgroups)
        clusterComCheck.checkDbReady(db)

    def clean(self):
        self.dropAllStreamsAndDbs()

    def createSnode(self, index=1):
        sql = f"create snode on dnode {index}"
        tdSql.execute(sql)

        tdSql.query("show snodes")
        tdSql.checkKeyExist(index)

    def dropSnode(self, index=1):
        sql = f"drop snode on dnode {index}"
        tdSql.query(sql)

    def checkStreamStatus(self, stream_name="", timeout=60):
        if stream_name == "":
            tdSql.query(f"select * from information_schema.ins_streams")
        else:
            tdSql.query(
                f"select * from information_schema.ins_streams where stream_name = '{stream_name}'"
            )
        streamNum = tdSql.getRows()
        for loop in range(120):
            if stream_name == "":
                tdSql.query(
                    f"select * from information_schema.ins_stream_tasks where type = 'Trigger' and status = 'Running'"
                )
            else:
                tdSql.query(
                    f"select * from information_schema.ins_stream_tasks where type = 'Trigger' and status = 'Running' and stream_name = '{stream_name}'"
                )
            time.sleep(1)
            if tdSql.getRows() == streamNum:
                return
        info = f"stream task status not ready in {loop} seconds"
        print(info)
        tdLog.exit(info)

    def dropAllStreamsAndDbs(self):
        streamNum = 0
        dbList = tdSql.query("show databases", row_tag=True)
        for r in range(len(dbList)):
            dbname = dbList[r][0]
            if dbname != "information_schema" and dbname != "performance_schema":
                streamList = tdSql.query(f"show {dbname}.streams", row_tag=True)
                for r in range(len(streamList)):
                    streamNum = streamNum + 1
                    streamName = streamList[r][0]
                    tdSql.execute(f"drop stream {dbname}.{streamList[r][0]}")
                tdLog.info(f"drop database {dbname}")
                tdSql.execute(f"drop database {dbname}")

        tdLog.info(f"drop {len(dbList)} databases, {streamNum} streams")

    def prepareChildTables(
        self,
        db="qdb",
        stb="meters",
        precision="ms",
        start="2025-01-01 00.00.00",
        interval=30,
        tbBatch=2,
        tbPerBatch=100,
        rowBatch=2,
        rowsPerBatch=500,
    ):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create stable {db}.{stb} ("
            "  cts timestamp"
            ", cint int"
            ", cuint int unsigned"
            ", cbigint bigint"
            ", cubigint bigint unsigned"
            ", cfloat float"
            ", cdouble double"
            ", cvarchar varchar(32)"
            ", csmallint smallint"
            ", cusmallint smallint unsigned"
            ", ctinyint tinyint"
            ", cutinyint tinyint unsigned"
            ", cbool bool"
            ", cnchar nchar(32)"
            ", cvarbinary varbinary(32)"
            ", cdecimal8 decimal(8)"
            ", cdecimal16 decimal(16)"
            ", cgeometry geometry(32)"
            ") tags("
            "  tts timestamp"
            ", tint int"
            ", tuint int unsigned"
            ", tbigint bigint"
            ", tubigint bigint unsigned"
            ", tfloat float"
            ", tdouble double"
            ", tvarchar varchar(32)"
            ", tsmallint smallint"
            ", tusmallint smallint unsigned"
            ", ttinyint tinyint"
            ", tutinyint tinyint unsigned"
            ", tbool bool"
            ", tnchar nchar(16)"
            ", tvarbinary varbinary(32)"
            ", tgeometry geometry(32)"
            ")"
        )

        dt = datetime.strptime(start, "%Y-%m-%d %H.%M.%S")
        if precision == "us":
            prec = 1000 * 1000 * 1000
        elif precision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsNext = tsStart + 86400 * prec
        tsInterval = interval * prec

        totalTables = tbBatch * tbPerBatch
        tdLog.info(f"create total {totalTables} child tables")
        for batch in range(tbBatch):
            sql = "create table "
            for tb in range(tbPerBatch):
                table = batch * tbPerBatch + tb
                tts = tsStart if table % 3 == 1 else tsNext
                tint = table % 3 if table % 20 != 1 else "NULL"
                tuint = table % 4
                tbigint = table % 5
                tubigint = table % 6
                tfloat = table % 7
                tdouble = table % 8
                tvarchar = "SanFrancisco" if table % 3 == 1 else "LosAngeles"
                tsmallint = table % 9
                tusmallint = table % 10
                ttinyint = table % 11
                tutinyint = table % 12
                tbool = table % 2
                tnchar = tvarchar
                tvarbinary = tvarchar
                tgeometry = "POINT(1.0 1.0)" if table % 3 == 1 else "POINT(2.0 2.0)"
                sql += f"{db}.t{table} using {db}.{stb} tags({tts}, '{tint}', {tuint}, {tbigint}, {tubigint}, {tfloat}, {tdouble}, '{tvarchar}', {tsmallint}, {tusmallint}, {ttinyint}, {tutinyint}, {tbool}, '{tnchar}', '{tvarbinary}', '{tgeometry}') "
            tdSql.execute(sql)

        totalRows = rowsPerBatch * rowBatch
        tdLog.info(f"write total:{totalRows} rows, {rowsPerBatch} rows per table")
        for table in range(totalTables):
            for batch in range(rowBatch):
                sql = f"insert into {db}.t{table} values "
                for row in range(rowsPerBatch):
                    rows = batch * rowsPerBatch + row
                    ts = tsStart + rows * tsInterval
                    cint = rows
                    cuint = rows % 4
                    cbigint = rows % 5
                    cubigint = rows % 6
                    cfloat = rows % 7
                    cdouble = rows % 8
                    cvarchar = "SanFrancisco" if rows % 3 == 1 else "LosAngeles"
                    csmallint = 1 if rows % 4 == 1 else 2
                    cusmallint = rows % 10
                    ctinyint = rows % 11
                    cutinyint = rows % 12
                    cbool = rows % 2 if rows % 20 != 1 else "NULL"
                    cnchar = cvarchar
                    cvarbinary = cvarchar
                    cdecimal8 = "0" if rows % 3 == 1 else "8"
                    cdecimal16 = "4" if rows % 3 == 1 else "16"
                    cgeometry = "POINT(1.0 1.0)" if rows % 3 == 1 else "POINT(2.0 2.0)"
                    sql += f"({ts}, {cint}, {cuint}, {cbigint}, {cubigint}, {cfloat}, {cdouble}, '{cvarchar}', {csmallint}, {cusmallint}, {ctinyint}, {cutinyint}, {cbool}, '{cnchar}', '{cvarbinary}', '{cdecimal8}', '{cdecimal16}', '{cgeometry}') "
                tdSql.execute(sql)

    def prepareNormalTables(
        self,
        db="qdb",
        precision="ms",
        start="2025-01-01 00.00.00",
        interval=30,
        tables=10,
        rowBatch=2,
        rowsPerBatch=500,
    ):
        dt = datetime.strptime(start, "%Y-%m-%d %H.%M.%S")
        if precision == "us":
            prec = 1000 * 1000 * 1000
        elif precision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsInterval = interval * prec

        tdLog.info(f"create total {tables} normal tables")
        for table in range(tables):
            tdSql.execute(
                f"create table {db}.n{table} ("
                "  cts timestamp"
                ", cint int"
                ", cuint int unsigned"
                ", cbigint bigint"
                ", cubigint bigint unsigned"
                ", cfloat float"
                ", cdouble double"
                ", cvarchar varchar(32)"
                ", csmallint smallint"
                ", cusmallint smallint unsigned"
                ", ctinyint tinyint"
                ", cutinyint tinyint unsigned"
                ", cbool bool"
                ", cnchar nchar(32)"
                ", cvarbinary varbinary(32)"
                ", cdecimal8 decimal(8)"
                ", cdecimal16 decimal(16)"
                ", cgeometry geometry(32)"
                ")"
            )

        totalRows = rowsPerBatch * rowBatch
        tdLog.info(f"write total:{totalRows} rows, {rowsPerBatch} rows per table")
        for table in range(tables):
            for batch in range(rowBatch):
                sql = f"insert into {db}.n{table} values "
                for row in range(rowsPerBatch):
                    rows = batch * rowsPerBatch + row
                    ts = tsStart + rows * tsInterval
                    cint = rows
                    cuint = rows % 4
                    cbigint = rows % 5
                    cubigint = rows % 6
                    cfloat = rows % 7
                    cdouble = rows % 8
                    cvarchar = "SanFrancisco" if rows % 3 == 1 else "LosAngeles"
                    csmallint = rows % 9
                    cusmallint = rows % 10
                    ctinyint = rows % 11
                    cutinyint = rows % 12
                    cbool = rows % 2
                    cnchar = cvarchar
                    cvarbinary = cvarchar
                    cdecimal8 = "0" if rows % 3 == 1 else "8"
                    cdecimal16 = "4" if rows % 3 == 1 else "16"
                    cgeometry = "POINT(1.0 1.0)" if rows % 3 == 1 else "POINT(2.0 2.0)"
                    sql += f"({ts}, {cint}, {cuint}, {cbigint}, {cubigint}, {cfloat}, {cdouble}, '{cvarchar}', {csmallint}, {cusmallint}, {ctinyint}, {cutinyint}, {cbool}, '{cnchar}', '{cvarbinary}', '{cdecimal8}', '{cdecimal16}', '{cgeometry}')"
                tdSql.execute(sql)

    def prepareVirtualTables(
        self,
        db="qdb",
        stb="vmeters",
        precision="ms",
        start="2025-01-01 00.00.00",
        tables=10,
    ):
        # each virtual table is sourced from 10 child-tables.
        tdSql.execute(f"use {db}")

        dt = datetime.strptime(start, "%Y-%m-%d %H.%M.%S")
        if precision == "us":
            prec = 1000 * 1000 * 1000
        elif precision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsNext = tsStart + 86400 * prec

        tdLog.info(f"create virtual super table")
        tdSql.execute(
            f"create stable {db}.{stb} ("
            "  cts timestamp"
            ", cint int"
            ", cuint int unsigned"
            ", cbigint bigint"
            ", cubigint bigint unsigned"
            ", cfloat float"
            ", cdouble double"
            ", cvarchar varchar(32)"
            ", csmallint smallint"
            ", cusmallint smallint unsigned"
            ", ctinyint tinyint"
            ", cutinyint tinyint unsigned"
            ", cbool bool"
            ", cnchar nchar(32)"
            ", cvarbinary varbinary(32)"
            ", cgeometry geometry(32)"
            ") tags("
            "  tts timestamp"
            ", tint int"
            ", tuint int unsigned"
            ", tbigint bigint"
            ", tubigint bigint unsigned"
            ", tfloat float"
            ", tdouble double"
            ", tvarchar varchar(32)"
            ", tsmallint smallint"
            ", tusmallint smallint unsigned"
            ", ttinyint tinyint"
            ", tutinyint tinyint unsigned"
            ", tbool bool"
            ", tnchar nchar(16)"
            ", tvarbinary varbinary(32)"
            ", tgeometry geometry(32)"
            ") VIRTUAL 1"
        )

        tdLog.info(f"create total {tables} virtual tables")
        for table in range(tables):
            t0 = table * 10
            t1 = table * 10 + 1
            t2 = table * 10 + 2
            t3 = table * 10 + 3
            t4 = table * 10 + 4
            t5 = table * 10 + 5
            t6 = table * 10 + 6
            t7 = table * 10 + 7
            t8 = table * 10 + 8
            t9 = table * 10 + 9

            tts = tsStart if table % 3 == 1 else tsNext
            tint = table % 3
            tuint = table % 4
            tbigint = table % 5
            tubigint = table % 6
            tfloat = table % 7
            tdouble = table % 8
            tvarchar = "SanFrancisco" if table % 3 == 1 else "LosAngeles"
            tsmallint = table % 9
            tusmallint = table % 10
            ttinyint = table % 11
            tutinyint = table % 12
            tbool = table % 2
            tnchar = tvarchar
            tvarbinary = tvarchar
            tgeometry = "POINT(1.0 1.0)" if table % 3 == 1 else "POINT(2.0 2.0)"

            tdSql.execute(
                f"create vtable v{table}("
                f"  t{t0}.cint"
                f", t{t0}.cuint"
                f", t{t1}.cbigint"
                f", t{t1}.cubigint"
                f", t{t2}.cfloat"
                f", t{t2}.cdouble"
                f", t{t3}.cvarchar"
                f", t{t4}.csmallint"
                f", t{t4}.cusmallint"
                f", t{t5}.ctinyint"
                f", t{t5}.cutinyint"
                f", t{t7}.cbool"
                f", t{t8}.cnchar"
                f", t{t8}.cvarbinary"
                f", t{t9}.cgeometry"
                f") using {db}.{stb} tags("
                f"  {tts}"
                f", {tint}"
                f", {tuint}"
                f", {tbigint}"
                f", {tubigint}"
                f", {tfloat}"
                f", {tdouble}"
                f", '{tvarchar}'"
                f", {tsmallint}"
                f", {tusmallint}"
                f", {ttinyint}"
                f", {tutinyint}"
                f", {tbool}"
                f", '{tnchar}'"
                f", '{tvarbinary}'"
                f", '{tgeometry}') "
            )

    def prepareJsonTables(
        self,
        db="qdb",
        stb="jmeters",
        precision="ms",
        start="2025-01-01 00.00.00",
        interval=30,
        tbBatch=1,
        tbPerBatch=10,
        rowBatch=2,
        rowsPerBatch=500,
    ):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create stable {db}.{stb} ("
            "  cts timestamp"
            ", cint int composite key"
            ", cuint int unsigned"
            ", cbigint bigint"
            ", cubigint bigint unsigned"
            ", cfloat float"
            ", cdouble double"
            ", cvarchar varchar(32)"
            ", csmallint smallint"
            ", cusmallint smallint unsigned"
            ", ctinyint tinyint"
            ", cutinyint tinyint unsigned"
            ", cbool bool"
            ", cnchar nchar(32)"
            ", cvarbinary varbinary(32)"
            ", cdecimal8 decimal(8)"
            ", cdecimal16 decimal(16)"
            ", cgeometry geometry(32)"
            ") tags("
            "  tjson JSON"
            ")"
        )

        dt = datetime.strptime(start, "%Y-%m-%d %H.%M.%S")
        if precision == "us":
            prec = 1000 * 1000 * 1000
        elif precision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsNext = tsStart + 86400 * prec
        tsInterval = interval * prec

        totalTables = tbBatch * tbPerBatch
        tdLog.info(f"create total {totalTables} child tables")
        str1 = '{\\"k1\\":\\"v1\\",\\"k2\\":\\"v1\\"}'
        str2 = '{\\"k1\\":\\"v2\\",\\"k2\\":\\"v2\\"}'
        for batch in range(tbBatch):
            sql = "create table "
            for tb in range(tbPerBatch):
                table = batch * tbPerBatch + tb
                tjson = str1 if table % 3 == 1 else str2
                sql += f'{db}.j{table} using {db}.{stb} tags("{tjson}")'
            tdSql.execute(sql)

        totalRows = rowsPerBatch * rowBatch
        tdLog.info(f"write total:{totalRows} rows, {rowsPerBatch} rows per table")
        for table in range(totalTables):
            for batch in range(rowBatch):
                sql = f"insert into {db}.j{table} values "
                for row in range(rowsPerBatch - 1):
                    rows = batch * rowsPerBatch + row
                    ts = (
                        tsStart + rows * tsInterval
                        if rows % 2 == 0
                        else tsStart + (rows - 1) * tsInterval
                    )
                    cint = rows
                    cuint = rows % 4
                    cbigint = rows % 5
                    cubigint = rows % 6
                    cfloat = rows % 7
                    cdouble = rows % 8
                    cvarchar = "SanFrancisco" if rows % 3 == 1 else "LosAngeles"
                    csmallint = rows % 9
                    cusmallint = rows % 10
                    ctinyint = rows % 11
                    cutinyint = rows % 12
                    cbool = rows % 2
                    cnchar = cvarchar
                    cvarbinary = cvarchar
                    cdecimal8 = "0" if rows % 3 == 1 else "8"
                    cdecimal16 = "4" if rows % 3 == 1 else "16"
                    cgeometry = "POINT(1.0 1.0)" if rows % 3 == 1 else "POINT(2.0 2.0)"
                    sql += f"({ts}, {cint}, {cuint}, {cbigint}, {cubigint}, {cfloat}, {cdouble}, '{cvarchar}', {csmallint}, {cusmallint}, {ctinyint}, {cutinyint}, {cbool}, '{cnchar}', '{cvarbinary}', '{cdecimal8}', '{cdecimal16}', '{cgeometry}') "
                tdSql.execute(sql)

                rows = rows + 1
                ts = tsStart + rows * tsInterval
                cint = rows
                sql = f"insert into {db}.j{table} (cts, cint) values ({ts}, {cint})"
                tdSql.execute(sql)

    def prepareViews(
        self,
        db="qdb",
        views=10,
    ):
        tdSql.execute(f"use {db}")

        tdLog.info(f"create total {views} views")
        for v in range(views):
            sql = f"create view view{v} as select cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cgeometry from qdb.t{v}"
            tdSql.execute(sql)

    # for StreamCheckItem, see cases/18-StreamProcessing/31-OldTsimCases/test_oldcase_twa.py
    def checkAll(self, streams):
        for stream in streams:
            tdLog.info(f"stream:{stream.db} - create database, table, stream", color='blue')
            stream.create()

        tdLog.info(f"total:{len(streams)} cases is running", color='blue')
        tdStream.checkStreamStatus()

        for stream in streams:
            if stream.insert1 != None:
                tdLog.info(f"stream:{stream.db} - insert step 1", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.insert1()

        for stream in streams:
            if stream.check1 != None:
                tdLog.info(f"stream:{stream.db} - check step 1", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.check1()

        for stream in streams:
            if stream.insert2 != None:
                tdLog.info(f"stream:{stream.db} - insert step 2", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.insert2()

        for stream in streams:
            if stream.check2 != None:
                tdLog.info(f"stream:{stream.db} - check step 2", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.check2()

        for stream in streams:
            if stream.insert3 != None:
                tdLog.info(f"stream:{stream.db} - insert step 3", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.insert3()

        for stream in streams:
            if stream.check3 != None:
                tdLog.info(f"stream:{stream.db} - check step 3", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.check3()

        for stream in streams:
            if stream.insert4 != None:
                tdLog.info(f"stream:{stream.db} - insert step 4", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.insert4()

        for stream in streams:
            if stream.check4 != None:
                tdLog.info(f"stream:{stream.db} - check step 4", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.check4()

        for stream in streams:
            if stream.insert5 != None:
                tdLog.info(f"stream:{stream.db} - insert step 5", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.insert5()

        for stream in streams:
            if stream.check5 != None:
                tdLog.info(f"stream:{stream.db} - check step 5", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.check5()

        for stream in streams:
            if stream.insert6 != None:
                tdLog.info(f"stream:{stream.db} - insert step 6", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.insert6()

        for stream in streams:
            if stream.check6 != None:
                tdLog.info(f"stream:{stream.db} - check step 6", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.check6()

        for stream in streams:
            if stream.insert7 != None:
                tdLog.info(f"stream:{stream.db} - insert step 7", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.insert7()

        for stream in streams:
            if stream.check7 != None:
                tdLog.info(f"stream:{stream.db} - check step 7", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.check7()

        for stream in streams:
            if stream.insert8 != None:
                tdLog.info(f"stream:{stream.db} - insert step 8", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.insert8()

        for stream in streams:
            if stream.check8 != None:
                tdLog.info(f"stream:{stream.db} - check step 8", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.check8()

        for stream in streams:
            if stream.insert9 != None:
                tdLog.info(f"stream:{stream.db} - insert step 9", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.insert9()

        for stream in streams:
            if stream.check9 != None:
                tdLog.info(f"stream:{stream.db} - check step 9", color='blue')
                tdSql.execute(f"use {stream.db}")
                stream.check9()

        tdLog.info(f"total:{len(streams)} streams check successfully", color='yellow')

    # get stream status
    def getStreamStatus(self, dbname, stream):
        tdSql.query(f"select status from information_schema.ins_streams where stream_name='{stream}' and db_name='{dbname}' ")
        return tdSql.getData(0, 0)

    #  check stream status
    def waitStreamStatus(self, dbname, stream, status, waitSeconds = 60):
        val = ""
        for i in range(waitSeconds):
            val = self.getStreamStatus(dbname, stream)
            #print(f"i={i} {stream} current status {val} ... ")
            if val == status:
                return
            time.sleep(1)
        tdLog.exit(f"stream:{stream} expect status:{status} actual:{val}.")

    # start stream
    def startStream(self, dbname, stream):
        sql = f"start stream {dbname}.{stream}"
        print(sql)
        tdSql.execute(sql)
        self.waitStreamStatus(dbname, stream, "Running")

    # stop stream
    def stopStream(self, dbname, stream):
        sql = f"stop stream {dbname}.{stream}"
        print(sql)
        tdSql.execute(sql)
        self.waitStreamStatus(dbname, stream, "Stopped")

    # drop stream
    def dropStream(self, dbname, stream):
        # drop
        sql = f"drop stream {dbname}.{stream}"
        print(sql)
        tdSql.execute(sql)
        # check drop ok
        sql = f"select count(*) from information_schema.ins_streams where db_name='{dbname}' and stream_name='{stream}'"
        tdSql.query(sql)
        tdSql.checkData(0, 0, 0)


tdStream = StreamUtil()


class StreamItem:
    def __init__(
        self,
        id,
        stream,
        res_query="",
        exp_query="",
        exp_rows=[],
        check_func=None,
        check_func_recalc=None,
        check_mode=StreamResultCheckMode.CHHECK_DEFAULT,
        exp_query_param_mapping={},
        caseName="",
        result_idx="",
        calc_tbname="",
        out_tb_tags=(),
    ):
        self.id = id
        self.stream = stream
        self.res_query = res_query
        self.exp_query = exp_query
        self.exp_rows = exp_rows
        self.check_func = check_func
        self.check_func_recalc = check_func_recalc
        self.exp_query_param_mapping = exp_query_param_mapping
        self.check_mode = check_mode
        self.caseName = caseName
        self.result_idx = result_idx
        self.result_file = ""
        self.calc_tbname = calc_tbname
        self.generate_file = False
        self.out_tb_tags = out_tb_tags

    def createStream(self):
        tdLog.info(self.stream)
        tdSql.execute(self.stream)

    def checkResults(self):
        if self.check_func != None:
            tdLog.info(f"check stream:s{self.id} func")
            self.check_func()

        if self.exp_query != "":
            tdLog.info(f"check stream:s{self.id} query")
            if self.exp_rows == []:
                exp_result = tdSql.getResult(self.exp_query)
            else:
                exp_result = []
                tmp_result = tdSql.getResult(self.exp_query)
                for r in self.exp_rows:
                    exp_result.append(tmp_result[r])
            self.awaitRowStability(len(exp_result))
            tdSql.checkResultsByArray(self.res_query, exp_result, self.exp_query)

        tdLog.info(f"check stream:s{self.id} result successfully")

    def awaitRowStability(self, stable_rows, waitSeconds=300):
        """
        确保流处理结果的行数与预期的稳定行数一致
        :param stable_rows: int, 预期的稳定行数
        """
        tdLog.info(f"ensure stream:s{self.id} has {stable_rows} stable rows")

        for loop in range(waitSeconds):
            if False == tdSql.query(self.res_query, None, 1, None, False, False):
                tdLog.info(f"{self.res_query} has failed for {loop} times")
                time.sleep(1)
                continue

            actual_rows = tdSql.getRows()

            tdLog.info(
                f"Stream:s{self.id} got {actual_rows} rows, expected:{stable_rows}"
            )

            if stable_rows < 0 or (stable_rows > 0 and actual_rows == stable_rows):
                tdLog.info(f"Stream:s{self.id} has {actual_rows} stable rows")
                return
            time.sleep(1)

        tdLog.exit(
            f"Stream:s{self.id} did not stabilize to {stable_rows} rows in {waitSeconds} seconds"
        )

    def awaitStreamRunning(self, waitSeconds=60):
        tdLog.info(
            f"wait stream:s{self.id} status become running at most {waitSeconds} seconds"
        )

        sql = f"select status from information_schema.ins_streams where stream_name='s{self.id}';"
        for loop in range(waitSeconds):
            if False == tdSql.query(sql, None, 1, None, False, False):
                tdLog.info(f"{sql} has failed for {loop} times")
                time.sleep(1)
                continue

            actual_rows = tdSql.getRows()

            if actual_rows != 1:
                tdLog.info(f"Stream:s{self.id} status not got")
                time.sleep(1)
                continue

            stream_status = tdSql.getData(0, 0)
            if stream_status != "Running":
                tdLog.info(f"Stream:s{self.id} status {stream_status} got")
                time.sleep(1)
                continue
            else:
                tdLog.info(f"Stream:s{self.id} status {stream_status} got")
                return

        tdLog.exit(
            f"Stream:s{self.id} status not become Running in {waitSeconds} seconds"
        )

    def set_exp_query_param_mapping(self, mapping: dict):
        """
        设置参数名与列索引的映射，例如 {"_wstart": 0, "_wend": 1}
        """
        if not isinstance(mapping, dict):
            raise ValueError("参数映射必须是字典类型")
        self.exp_query_param_mapping = mapping
        self.check_mode = StreamResultCheckMode.CHECK_ROW_BY_SQL

    def checkResultsByRow(self):
        if self.check_mode != StreamResultCheckMode.CHECK_ROW_BY_SQL:
            return
        tdSql.query(self.res_query)

        rowNum = tdSql.getRows()
        colNum = tdSql.getCols()
        cols = [tdSql.getColData(i) for i in range(colNum)]

        for i in range(0, rowNum):
            params = {
                param_name: cols[col_index][i]
                for param_name, col_index in self.exp_query_param_mapping.items()
                if col_index < colNum
            }
            sql = self.exp_query.format(**params)
            tdLog.info(f"after fomat, sql: {sql}")

            tdSql.query(sql)
            for colIndex in range(0, colNum):
                print(
                    f"type(elm): {type(cols[colIndex][i])}, type(expect_elm): {type(tdSql.getData(0, colIndex))}"
                )
                tdSql.checkEqual(cols[colIndex][i], tdSql.getData(0, colIndex))

    def checkResultsByMode(self):
        if (
            self.check_mode == StreamResultCheckMode.CHHECK_DEFAULT
            or self.check_mode == StreamResultCheckMode.CHECK_ARRAY_BY_SQL
        ):
            tdLog.info(f"check stream:s{self.result_idx} results by array")
            self.checkResults()
        elif self.check_mode == StreamResultCheckMode.CHECK_ROW_BY_SQL:
            tdLog.info(f"check stream:s{self.result_idx} results by row")
            self.awaitResultHasRows()
            self.checkResultsByRow()
        elif self.check_mode == StreamResultCheckMode.CHECK_BY_FILE:
            tdLog.info(f"check stream:s{self.result_idx} results by file")
            self.checkResultWithResultFile()

    def awaitResultHasRows(self, waitSeconds=60):
        """
        确保流处理已有结果，不确认最终结果行数时使用
        """
        tdLog.info(f"await stream:s{self.id} stable rows stabilize")

        last_rows = 0
        rows = 0

        for loop in range(waitSeconds):
            if False == tdSql.query(self.res_query, None, 1, None, False, False):
                tdLog.info(f"{self.res_query} has failed for {loop} times")
                time.sleep(1)
                continue

            rows = tdSql.getRows()

            if rows > 0 and rows == last_rows:
                tdLog.info(f"Stream:s{self.id} has {rows} stable rows")
                return
            last_rows = rows
            time.sleep(2)

        tdLog.exit(
            f"Stream:s{self.id} did not stabilize rows in {waitSeconds} seconds, rows now: {rows}, last rows: {last_rows}"
        )

    def setResultFile(self, file):
        """
        设置结果文件路径
        """
        self.result_file = file

    def addQuerySqlCase(
        self,
        query_sql_case: QuerySqlCase,
    ):
        """
        添加查询SQL用例
        """
        self.check_mode = query_sql_case.check_mode

        if (
            query_sql_case.check_mode == StreamResultCheckMode.CHECK_ARRAY_BY_SQL
            or query_sql_case.check_mode == StreamResultCheckMode.CHECK_ROW_BY_SQL
            or query_sql_case.check_mode == StreamResultCheckMode.CHHECK_DEFAULT
        ):
            self.exp_query = query_sql_case.expected_sql
            if len(self.out_tb_tags) > 0:
                s = ", " + ", ".join(f"{item}" for item in self.out_tb_tags)
                self.exp_query = self.exp_query.replace("{outTbTags}", s)
            tdLog.info(f"set exp query format reslut: {self.exp_query}")
            self.exp_query = self.exp_query.format_map(
                SafeDict({"calcTbname": self.calc_tbname})
            )
        else:
            pass

        if query_sql_case.check_mode == StreamResultCheckMode.CHECK_ARRAY_BY_SQL:
            self.exp_query = self.exp_query.format_map(
                SafeDict({"calcTbname": self.calc_tbname})
            )
        elif query_sql_case.check_mode == StreamResultCheckMode.CHECK_ROW_BY_SQL:
            self.exp_query = self.exp_query.format_map(
                SafeDict({"calcTbname": self.calc_tbname})
            )
            self.set_exp_query_param_mapping(query_sql_case.exp_sql_param_mapping)
        elif query_sql_case.check_mode == StreamResultCheckMode.CHECK_BY_FILE:
            self.generate_file = query_sql_case.generate_file
        else:
            self.exp_query = self.exp_query.format_map(
                SafeDict({"calcTbname": self.calc_tbname})
            )

    def checkResultWithResultFile(self):
        tdLog.info(f"check result with sql: {self.res_query}")
        if self.generate_file:
            tdLog.info(
                f"generate query result file for stream:s{self.caseName} with index {self.result_idx}"
            )
            tdCom.generate_query_result_file(
                self.caseName, self.result_idx, self.res_query
            )
        else:
            tdCom.compare_query_with_result_file(
                self.result_idx,
                self.res_query,
                self.result_file,
                self.caseName,
            )
            tdLog.info("check result with result file succeed")


class StreamCheckItem:
    def __init__(self, db):
        self.db = db

    def create(self):
        return

    def insert1(self):
        return

    def check1(self):
        return

    def insert2(self):
        return

    def check2(self):
        return

    def insert3(self):
        return

    def check3(self):
        return

    def insert4(self):
        return

    def check4(self):
        return

    def insert5(self):
        return

    def check5(self):
        return

    def insert6(self):
        return

    def check6(self):
        return

    def insert7(self):
        return

    def check7(self):
        return

    def insert8(self):
        return

    def check8(self):
        return
    
    def insert9(self):
        return

    def check9(self):
        return


class SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"
