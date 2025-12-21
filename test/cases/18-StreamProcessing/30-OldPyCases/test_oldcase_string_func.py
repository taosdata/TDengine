from new_test_framework.utils import tdLog, tdSql, tdStream
import pytest
import time
import random
import string
import re


class TestStringFunctionInStream:

    @pytest.mark.parametrize(
        "string_func",
        [
            "char_length(val)",
            "concat(val, 'abc')",
            "concat_ws('-', val, 'abc')",
            "length(val)",
            "lower(val)",
            "ltrim(val)",
            "rtrim(val)",
            "substr(val, 1, 3)",
            "upper(val)",
        ],
    )
    def test_string_function(self, string_func):
        """OldPy: string function

        旧用例 tests/system-test/8-stream/scalar_function.py
        测试在流计算中使用字符串函数
        旧的建流语句：
        create stream XXX trigger at_once ignore expired 0 ignore update 0 fill_history 1 into XXX as select ts, char_length(c3) from scalar_tb
        新的建流语句：
        CREATE STREAM XXX SLIDING(10s) FROM tb INTO XXX AS SELECT ts, char_length(val) as val FROM %%trows;

        Catalog:
            - Streams:OldPyCases
            
        Since: v3.3.7.0

        Labels: common,ci

        Jira: https://jira.taosdata.com:18080/browse/TD-36887

        History:
            - 2025-07-23: Created by zyyang90
        """
        # create snode if not exists
        snodes = tdSql.getResult("SHOW SNODES;")
        if snodes is None or len(snodes) == 0:
            tdStream.createSnode()

        self.db = "test_string_func_in_stream"
        self.stream = "stream_output"

        # create database and table
        tdSql.executes(
            [
                f"DROP DATABASE IF EXISTS `{self.db}`;",
                f"CREATE DATABASE IF NOT EXISTS `{self.db}`;",
                f"USE `{self.db}`;",
                "CREATE TABLE tb(ts TIMESTAMP, val VARCHAR(20));",
            ],
            queryTimes=1,
        )

        # create stream
        tdSql.execute(
            f"CREATE STREAM {self.stream} SLIDING(10s) FROM tb INTO {self.stream} AS SELECT _tprev_ts AS wstart, _tcurrent_ts AS wend, {string_func} AS val FROM %%trows;",
            queryTimes=1,
        )
        tdStream.checkStreamStatus()

        # insert data
        ts = (int(time.time()) // 60) * 60 * 1000  # 当前时间取整分钟的时间戳
        # 生成随机字符串
        val = "".join(random.choices(string.ascii_letters + string.digits, k=10))
        if string_func.startswith("ltrim"):
            val = f"   {val}"  # 添加前后空格以测试 ltrim
        elif string_func.startswith("rtrim"):
            val = f"{val}   "
        tdSql.execute(f"INSERT INTO tb VALUES({ts}, '{val}');", queryTimes=1)

        # 计算 expected value
        if string_func.startswith("char_length") or string_func.startswith("length"):
            expected = len(val)
        elif string_func.startswith("concat_ws"):
            # 解析 concat_ws 的参数并组合 expected 字符串
            match = re.match(r"concat_ws\(([^,]+),\s*([^,]+),\s*([^)]+)\)", string_func)
            if match:
                sep = match.group(1).strip("'\"")
                str1 = match.group(2).strip("'\"")
                str2 = match.group(3).strip("'\"")
                if str1 == "val":
                    str1 = val
                elif str2 == "val":
                    str2 = val
                expected = f"{str1}{sep}{str2}"
            else:
                tdLog.error(f"Invalid concat_ws function: {string_func}")
                expected = None
        elif string_func.startswith("concat"):
            # 解析 concat 的参数并组合 expected 字符串
            match = re.match(r"concat\(([^,]+),\s*([^,]+)\)", string_func)
            if match:
                str1 = match.group(1).strip("'\"")
                str2 = match.group(2).strip("'\"")
                if str1 == "val":
                    str1 = val
                elif str2 == "val":
                    str2 = val
                expected = f"{str1}{str2}"
            else:
                tdLog.error(f"Invalid concat function: {string_func}")
                expected = None
        elif string_func.startswith("lower"):
            expected = val.lower()
        elif string_func.startswith("upper"):
            expected = val.upper()
        elif string_func.startswith("ltrim"):
            expected = val.lstrip()
        elif string_func.startswith("rtrim"):
            expected = val.rstrip()
        elif string_func.startswith("substr"):
            # 解析 substr 的参数并截取字符串
            match = re.match(r"substr\(([^,]+),\s*([^,]+),\s*([^)]+)\)", string_func)
            if match:
                start = int(match.group(2)) - 1  # substr 的起始位置从 1 开始
                length = int(match.group(3))
                if start < 0:
                    start = 0
                if start + length > len(val):
                    length = len(val) - start
                expected = val[start : start + length]
        else:
            expected = None
        tdLog.info(f"string_func: {string_func}, val: {val}, expected: {expected}")

        # check result
        tdSql.checkResultsByFunc(
            sql=f"select * from {self.stream}",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(
                0,
                2,
                expected,
            ),
        )

        tdLog.info("test string function done.")
