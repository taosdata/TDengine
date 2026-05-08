from new_test_framework.utils import tdLog, tdSql, tdStream
import pytest
import time
import random
import math


class TestMathFunctionInStream:

    @pytest.mark.parametrize(
        "math_func",
        [
            "abs(val)",
            "acos(val)",
            "asin(val)",
            "atan(val)",
            "ceil(val)",
            "cos(val)",
            "floor(val)",
            "log(val,2)",
            "pow(val,2)",
            "round(val)",
            "sin(val)",
            "sqrt(val)",
            "tan(val)",
        ],
    )
    def test_math_function(self, math_func):
        """OldPy: math function

        旧用例 tests/system-test/8-stream/scalar_function.py
        测试在流计算中使用数学函数
        旧的建流语句：
        create stream XXX trigger at_once ignore expired 0 ignore update 0 fill_history 1 into XXX as select ts, log(c1, 2), log(c2, 2), c3 from scalar_tb
        新的建流语句：
        CREATE STREAM XXX SLIDING(10s) FROM tb INTO XXX AS SELECT ts, log(val, 2) as val FROM %%trows;

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

        self.db = "test_math_func_in_stream"
        self.stream = "stream_output"

        # create database and table
        tdSql.executes(
            [
                f"DROP DATABASE IF EXISTS `{self.db}`;",
                f"CREATE DATABASE IF NOT EXISTS `{self.db}`;",
                f"USE `{self.db}`;",
                "CREATE TABLE tb(ts TIMESTAMP, val FLOAT);",
                "RESET QUERY CACHE;",
            ],
            queryTimes=1,
        )

        # create stream
        tdSql.execute(
            f"CREATE STREAM {self.stream} SLIDING(10s) FROM tb INTO {self.stream} AS SELECT _tprev_ts AS wstart, _tcurrent_ts AS wend, {math_func} AS val FROM %%trows;"
        )
        tdStream.checkStreamStatus()

        # insert data
        ts = (int(time.time()) // 60) * 60 * 1000  # 当前时间取整分钟的时间戳
        if math_func.startswith("acos") or math_func.startswith("asin"):
            val = round(random.uniform(-1, 1), 2)
        elif math_func.startswith("atan"):
            val = round(random.uniform(-(math.pi / 2), math.pi / 2), 2)
        elif math_func.startswith("tan"):
            val = round(random.uniform(-1.4, 1.4), 2)
        else:
            val = round(random.uniform(1, 100), 2)
        tdSql.execute(f"INSERT INTO tb VALUES({ts}, {val});")

        # 计算 expected value
        try:
            math_env = {
                name: getattr(math, name)
                for name in dir(math)
                if callable(getattr(math, name))
            }
            math_env["abs"] = abs  # 加入内置 abs 函数
            math_env["round"] = round  # 加入内置 round 函数
            expected = eval(
                math_func.replace("val", str(val)), {"__builtins__": None}, math_env
            )
        except Exception as e:
            tdLog.error(f"Error evaluating math function: {e}")
            expected = None
        tdLog.info(f"math_func: {math_func}, val: {val}, expected: {expected}")

        # check result
        tdSql.checkResultsByFunc(
            sql=f"select * from {self.stream}",
            func=lambda: tdSql.getRows() == 1
            and math.isclose(tdSql.getData(0, 2), expected, rel_tol=1e-5, abs_tol=1e-8),
        )

        tdLog.info("test math function done.")
