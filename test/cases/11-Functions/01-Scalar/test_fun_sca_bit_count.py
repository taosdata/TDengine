# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from new_test_framework.utils import tdLog, tdSql, tdStream, tdCom, StreamCheckItem
from taos import new_bind_params

import pytest
import time


def popcount(value):
    return int(value & 0xFFFFFFFFFFFFFFFF).bit_count()


def is_community_edition():
    tdSql.query("select server_version()")
    return "community" in str(tdSql.getData(0, 0)).lower()


class TestFunScalarBitCount:
    """bit_count(expr) coverage."""

    def setup_class(cls):
        cls.replicaVar = 1
        tdLog.debug(f"start to execute {__file__}")

    def _prepare(self, db):
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} keep 36500")
        tdSql.execute(f"use {db}")

    def _check_scalar(self, sql, expected):
        if not sql.lstrip().lower().startswith("select"):
            sql = f"select {sql}"
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, expected)

    def _median_elapsed(self, sql, repeats=5):
        elapsed = []
        for _ in range(repeats):
            start = time.perf_counter()
            tdSql.query(sql)
            elapsed.append(time.perf_counter() - start)
        elapsed.sort()
        return elapsed[len(elapsed) // 2]

    def test_bit_count_constants(self):
        """Scalar: bit_count constants and 64-bit two's complement semantics

        1. Cover zero, single-bit, multi-bit and integer boundary values.
        2. Cover negative values counted in 64-bit two's complement.
        3. Cover unsigned BIGINT values above INT64_MAX.
        4. Verify the result metadata type is BIGINT.

        Catalog:
            - Function:Scalar

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: 7172772636

        History:
            - 2026-11-21 Codex Added BIT_COUNT constant coverage from FS/TS.

        """
        self._prepare("bit_count_const")

        samples = (
            ("bit_count(0)", 0),
            ("bit_count(1)", 1),
            ("bit_count(2)", 1),
            ("bit_count(4)", 1),
            ("bit_count(3)", 2),
            ("bit_count(7)", 3),
            ("bit_count(15)", 4),
            ("bit_count(29)", 4),
            ("bit_count(255)", 8),
            ("bit_count(256)", 1),
            ("bit_count(2147483647)", 31),
            ("bit_count(9223372036854775807)", 63),
            ("bit_count(-1)", 64),
            ("bit_count(-2)", 63),
            ("bit_count(cast(-2147483648 as int))", 33),
            ("bit_count(cast(18446744073709551615 as bigint unsigned))", 64),
            ("bit_count(cast(9223372036854775808 as bigint unsigned))", 1),
        )
        for sql, expected in samples:
            self._check_scalar(sql, expected)

        tdSql.query("select bit_count(29)")
        assert tdSql.checkDataType(0, 0, "BIGINT")

    def test_bit_count_numeric_types(self):
        """Scalar: bit_count integer, bool, float, double and decimal conversions

        1. Cover all signed and unsigned integer types.
        2. Cover BOOL TRUE/FALSE.
        3. Cover FLOAT/DOUBLE rounding half away from zero.
        4. Cover DECIMAL rounding half away from zero and the BIGINT range limit.
        5. Cover NaN, +/-Inf and values outside the BIGINT range returning NULL.

        Catalog:
            - Function:Scalar

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: 7172772636

        History:
            - 2026-11-21 Codex Added BIT_COUNT numeric type coverage from FS/TS.
            - 2026-11-23 Align FLOAT/DOUBLE with MySQL rounding and add DECIMAL.

        """
        self._prepare("bit_count_types")

        tdSql.execute("create table t_signed (ts timestamp, ti tinyint, si smallint, i int, bi bigint)")
        tdSql.execute(
            "insert into t_signed values "
            "(1000,-1,-1,-1,-1)(2000,0,0,0,0)(3000,7,7,7,7)"
        )
        tdSql.query(
            "select bit_count(ti), bit_count(si), bit_count(i), bit_count(bi) "
            "from t_signed order by ts"
        )
        tdSql.checkRows(3)
        for col in range(4):
            tdSql.checkData(0, col, 64)
            tdSql.checkData(1, col, 0)
            tdSql.checkData(2, col, 3)

        tdSql.execute(
            "create table t_unsigned (ts timestamp, uti tinyint unsigned, "
            "usi smallint unsigned, ui int unsigned, ubi bigint unsigned)"
        )
        tdSql.execute(
            "insert into t_unsigned values "
            "(1000,255,65535,4294967295,18446744073709551615)"
        )
        tdSql.query(
            "select bit_count(uti), bit_count(usi), bit_count(ui), bit_count(ubi) "
            "from t_unsigned"
        )
        tdSql.checkRows(1)
        for col, expected in enumerate((8, 16, 32, 64)):
            tdSql.checkData(0, col, expected)

        tdSql.execute("create table t_bool (ts timestamp, b bool)")
        tdSql.execute("insert into t_bool values (1000,true)(2000,false)")
        tdSql.query("select bit_count(b) from t_bool order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 0)
        self._check_scalar("select bit_count(true)", 1)
        self._check_scalar("select bit_count(false)", 0)

        float_samples = (
            ("2.9", 2),
            ("3.9", 1),
            ("0.9", 1),
            ("-0.9", 64),
            ("-2.9", 63),
            ("2.5", 2),
            ("-2.5", 63),
        )
        for value, expected in float_samples:
            self._check_scalar(f"select bit_count(cast({value} as float))", expected)
            self._check_scalar(f"select bit_count(cast({value} as double))", expected)

        tdSql.execute("create table t_float (ts timestamp, f float, d double)")
        rows = "".join(
            f"({1000 + idx},{value},{value})"
            for idx, (value, _) in enumerate(float_samples)
        )
        tdSql.execute("insert into t_float values " + rows)
        tdSql.query("select bit_count(f), bit_count(d) from t_float order by ts")
        tdSql.checkRows(len(float_samples))
        for row, (_, expected) in enumerate(float_samples):
            tdSql.checkData(row, 0, expected)
            tdSql.checkData(row, 1, expected)

        for sql in (
            "select bit_count(cast(1e308 as float))",
            "select bit_count(cast(-1e308 as float))",
            "select bit_count(1e308)",
            "select bit_count(-1e308)",
            "select bit_count(1e19)",
            "select bit_count(-1e19)",
        ):
            self._check_scalar(sql, None)

        self._check_scalar(
            "select bit_count(cast(1e18 as double)) = "
            "bit_count(cast(cast(1e18 as double) as bigint))",
            True,
        )

        tdSql.execute("create table t_special (ts timestamp, f float, d double)")
        statement = self.conn.statement("insert into t_special values (?, ?, ?)")
        try:
            for ts, fv, dv in (
                (1000, float("inf"), float("nan")),
                (2000, float("-inf"), float("inf")),
                (3000, float("nan"), float("-inf")),
                (4000, 1.0, 3.0),
            ):
                params = new_bind_params(3)
                params[0].timestamp(ts)
                params[1].float(fv)
                params[2].double(dv)
                statement.bind_param(params)
                statement.execute()
        finally:
            statement.close()

        tdSql.query("select count(*) from t_special")
        tdSql.checkData(0, 0, 4)

        tdSql.query("select bit_count(f), bit_count(d) from t_special order by ts")
        tdSql.checkRows(4)
        for row in range(3):
            tdSql.checkData(row, 0, None)
            tdSql.checkData(row, 1, None)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(3, 1, 2)

        decimal_samples = (
            ("cast(2.9 as decimal(5,1))", 2),
            ("cast(-2.9 as decimal(5,1))", 63),
            ("cast(0 as decimal(5,1))", 0),
            ("cast(2.5 as decimal(5,1))", 2),
            ("cast(-2.5 as decimal(5,1))", 63),
            ("cast(0.4 as decimal(5,1))", 0),
        )
        for expr, expected in decimal_samples:
            self._check_scalar(f"select bit_count({expr})", expected)

        self._check_scalar("select bit_count(cast('9223372036854775808' as decimal(38,0)))", None)
        self._check_scalar("select bit_count(cast('-9223372036854775809' as decimal(38,0)))", None)
        self._check_scalar("select bit_count(cast('9223372036854775807' as decimal(38,0)))", 63)

        tdSql.execute("create table t_decimal (ts timestamp, dec decimal(10,2))")
        tdSql.execute(
            "insert into t_decimal values "
            "(1000,2.9)(2000,-2.9)(3000,0)(4000,2.5)"
        )
        tdSql.query("select bit_count(dec) from t_decimal order by ts")
        tdSql.checkRows(4)
        for row, expected in enumerate((2, 63, 0, 2)):
            tdSql.checkData(row, 0, expected)

    def test_bit_count_string_and_timestamp(self):
        """Scalar: bit_count VARCHAR, NCHAR and TIMESTAMP conversions

        1. Cover decimal integer prefixes, blanks, trailing text and decimals.
        2. Cover empty and invalid text returning zero without an error.
        3. Cover saturated text values beyond the BIGINT range.
        4. Cover TIMESTAMP as the MySQL +0 YYYYMMDDHHMMSS numeric context in
           millisecond, microsecond and nanosecond databases.

        Catalog:
            - Function:Scalar

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: 7172772636

        History:
            - 2026-11-21 Codex Added BIT_COUNT string/timestamp coverage from FS/TS.
            - 2026-11-23 Switch TIMESTAMP to the MySQL +0 YYYYMMDDHHMMSS context.

        """
        self._prepare("bit_count_str")

        tdSql.execute("create table t_str (ts timestamp, vc varchar(64), nc nchar(64))")
        samples = (
            ("29", 4),
            ("0", 0),
            ("-1", 64),
            (" 12 ", 2),
            ("12abc", 2),
            ("2.9", 1),
            ("abc", 0),
            ("", 0),
            ("   ", 0),
            ("18446744073709551615", 63),
            ("-99999999999999999999", 1),
            ("中文", 0),
        )
        values = []
        for idx, (text, _) in enumerate(samples):
            values.append(f"({1000 + idx},'{text}','{text}')")
        tdSql.execute("insert into t_str values " + "".join(values))

        tdSql.query("select bit_count(vc), bit_count(nc) from t_str order by ts")
        tdSql.checkRows(len(samples))
        for row, (text, expected) in enumerate(samples):
            tdSql.checkData(row, 0, expected)
            tdSql.checkData(row, 1, expected)

        for db, frac in (
            ("bit_count_ts_ms", "001"),
            ("bit_count_ts_us", "000001"),
            ("bit_count_ts_ns", "000000001"),
        ):
            precision = db.rsplit("_", 1)[-1]
            tdSql.execute(f"drop database if exists {db}")
            tdSql.execute(
                f"create database {db} precision '{precision}' keep 36500"
            )
            tdSql.execute(f"use {db}")
            tdSql.execute("set timezone 'UTC'")
            # A constant CAST parses the string literal at millisecond precision,
            # so exercise sub-millisecond ticks through an inserted row instead.
            tdSql.execute("create table t (ts timestamp, v int)")
            tdSql.execute(f"insert into t values ('1970-01-01 00:00:00.{frac}', 1)")
            tdSql.query("select bit_count(ts) from t")
            tdSql.checkRows(1)
            # Fractional seconds below half a second round down to 19700101000000.
            tdSql.checkData(0, 0, 20)
            self._check_scalar(
                "select bit_count(cast('1970-01-01 00:00:00.000' as timestamp))", 20
            )
            self._check_scalar("select bit_count(cast(1 as timestamp))", 20)
            self._check_scalar(
                "select bit_count(cast('2026-11-23 15:45:22.123456789' as timestamp))",
                20,
            )
            self._check_scalar(
                "select bit_count(cast('2026-11-23 15:45:22.9' as timestamp))", 21
            )
            tdSql.execute("set timezone 'Asia/Shanghai'")
            # The same textual value maps to the same local YYYYMMDDHHMMSS.
            self._check_scalar(
                "select bit_count(cast('2026-11-23 15:45:22.123456789' as timestamp))",
                20,
            )
            # The same epoch value maps to a different local YYYYMMDDHHMMSS.
            self._check_scalar("select bit_count(cast(1 as timestamp))", 21)
            tdSql.execute("set timezone 'UTC'")

    def test_bit_count_binary_types(self):
        """Scalar: bit_count VARBINARY, BLOB and MEDIUMBLOB conversions

        1. Cover empty, single byte and multi byte binary values.
        2. Verify the result may exceed 64 for long binary values.
        3. Verify column values match the equivalent constants.

        Catalog:
            - Function:Scalar

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: 7172772636

        History:
            - 2026-11-23 Added binary type coverage from the reviewed FS/TS.

        """
        self._prepare("bit_count_bin")

        self._check_scalar("select bit_count(cast('ABCD' as varbinary))", 9)
        self._check_scalar("select bit_count(cast('' as varbinary))", 0)
        self._check_scalar("select bit_count(cast('1' as varbinary))", 3)

        full_byte_row = "'\\x" + "FF" * 16 + "'"
        tdSql.execute("create table t_vb (ts timestamp, vb varbinary(32))")
        tdSql.execute(
            "insert into t_vb values "
            f"(1000,'ABCD')(2000,'')(3000,{full_byte_row})"
        )
        tdSql.query("select bit_count(vb) from t_vb order by ts")
        tdSql.checkRows(3)
        for row, expected in enumerate((9, 0, 128)):
            tdSql.checkData(row, 0, expected)

        tdSql.execute("create table t_blob (ts timestamp, b blob)")
        tdSql.execute(
            "insert into t_blob values "
            f"(1000,'ABCD')(2000,'')(3000,{full_byte_row})"
        )
        tdSql.query("select bit_count(b) from t_blob order by ts")
        tdSql.checkRows(3)
        for row, expected in enumerate((9, 0, 128)):
            tdSql.checkData(row, 0, expected)

        tdSql.execute("create table t_mb (ts timestamp, mb mediumblob)")
        tdSql.execute(
            "insert into t_mb values "
            f"(1000,'ABCD')(2000,'')(3000,{full_byte_row})"
        )
        tdSql.query("select bit_count(mb) from t_mb order by ts")
        tdSql.checkRows(3)
        for row, expected in enumerate((9, 0, 128)):
            tdSql.checkData(row, 0, expected)

    def test_bit_count_null(self):
        """Scalar: bit_count NULL propagation

        1. Cover NULL literals, CAST(NULL) and expressions producing NULL.
        2. Cover NULL rows in nullable columns and mixed result sets.
        3. Verify COUNT(bit_count(v)) ignores NULL rows.

        Catalog:
            - Function:Scalar

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: 7172772636

        History:
            - 2026-11-21 Codex Added BIT_COUNT NULL coverage from FS/TS.

        """
        self._prepare("bit_count_null")

        self._check_scalar("select bit_count(null)", None)
        self._check_scalar("select bit_count(cast(null as bigint))", None)
        self._check_scalar("select bit_count(null + 1)", None)

        tdSql.execute("create table t_nulls (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_nulls values "
            "(1000,29)(2000,NULL)(3000,0)(4000,NULL)(5000,-1)"
        )
        tdSql.query("select bit_count(v) from t_nulls order by ts")
        tdSql.checkRows(5)
        for row, expected in enumerate((4, None, 0, None, 64)):
            tdSql.checkData(row, 0, expected)

        self._check_scalar("select count(bit_count(v)) from t_nulls", 3)

    def test_bit_count_query_context(self):
        """Scalar: bit_count query context coverage

        1. Cover projection, WHERE, GROUP BY, HAVING and ORDER BY.
        2. Cover subquery, JOIN, UNION ALL, supertable and child table.
        3. Cover constant results and EXPLAIN not exposing the scalar expression.

        Catalog:
            - Function:Scalar

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: 7172772636

        History:
            - 2026-11-21 Codex Added BIT_COUNT query-context coverage from FS/TS.

        """
        self._prepare("bit_count_ctx")

        tdSql.execute(
            "create table meters (ts timestamp, current float, voltage int, phase float) "
            "tags (location varchar(64), groupId int)"
        )
        tdSql.execute(
            "insert into d1001 using meters tags ('Beijing.Chaoyang', 2) values "
            "('2018-10-03 14:38:05.000', 10.3, 219, 0.31) "
            "('2018-10-03 14:38:15.000', 12.6, 220, 0.33)"
        )
        tdSql.execute(
            "insert into d1002 using meters tags ('Beijing.Haidian', 3) values "
            "('2018-10-03 14:38:25.000', 11.1, 225, 0.35)"
        )

        expected = {219: popcount(219), 220: popcount(220), 225: popcount(225)}
        tdSql.query("select voltage, bit_count(voltage) from meters order by ts")
        tdSql.checkRows(3)
        for row in range(3):
            tdSql.checkData(row, 1, expected[tdSql.getData(row, 0)])

        tdSql.query("select voltage from meters where bit_count(voltage) > 5 order by ts")
        filtered = [219, 220, 225]
        filtered = [v for v in filtered if popcount(v) > 5]
        tdSql.checkRows(len(filtered))
        for row, voltage in enumerate(filtered):
            tdSql.checkData(row, 0, voltage)

        total = sum(expected.values())
        tdSql.query("select sum(bit_count(voltage)) from meters")
        tdSql.checkData(0, 0, total)

        tdSql.query("select location, sum(bit_count(voltage)) from meters group by location order by location")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "Beijing.Chaoyang")
        tdSql.checkData(0, 1, expected[219] + expected[220])
        tdSql.checkData(1, 0, "Beijing.Haidian")
        tdSql.checkData(1, 1, expected[225])

        tdSql.query(
            "select location, sum(bit_count(voltage)) as s from meters "
            "group by location having sum(bit_count(voltage)) > 5 order by location"
        )
        assert tdSql.queryRows >= 1

        tdSql.query("select bit_count(voltage) from meters order by bit_count(voltage), ts")
        ordered = sorted(expected.values())
        tdSql.checkRows(3)
        for row in range(3):
            tdSql.checkData(row, 0, ordered[row])

        tdSql.query(
            "select c from (select bit_count(voltage) as c from meters) "
            "where c > 5 order by c"
        )
        tdSql.checkRows(len([v for v in expected.values() if v > 5]))

        tdSql.execute("create table j1 (ts timestamp, v int)")
        tdSql.execute("create table j2 (ts timestamp, v int)")
        tdSql.execute("insert into j1 values (1000, 219)(2000, 220)")
        tdSql.execute("insert into j2 values (1000, 219)(3000, 225)")
        tdSql.query(
            "select count(*) from j1 m1 join j2 m2 "
            "on m1.ts = m2.ts and bit_count(m1.v) = bit_count(m2.v)"
        )
        tdSql.checkData(0, 0, 1)

        tdSql.query(
            "select bit_count(voltage) from meters "
            "union all select bit_count(voltage) from d1001"
        )
        tdSql.checkRows(5)
        union_values = sorted(tdSql.getData(row, 0) for row in range(tdSql.queryRows))
        assert union_values == sorted(
            [expected[219], expected[220], expected[225], expected[219], expected[220]]
        )

        tdSql.query("select bit_count(voltage) from d1001 order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, expected[219])
        tdSql.checkData(1, 0, expected[220])

        self._check_scalar("select bit_count(29)", 4)
        self._check_scalar(
            "select max(bit_count(29)) from meters", 4
        )
        self._check_scalar("select max(29) from meters", 29)

        # Constant folding: a folded constant costs the same as a plain constant.
        # The TS performance run applies the 10% bound; CI only guards against a
        # per-row evaluation, so the tolerance here stays loose against noise.
        folded = self._median_elapsed("select max(bit_count(29)) from meters")
        plain = self._median_elapsed("select max(29) from meters")
        ratio = folded / plain if plain > 0 else 1.0
        tdLog.info(
            f"bit_count constant folding: folded={folded:.6f}s "
            f"plain={plain:.6f}s ratio={ratio:.2f}"
        )
        assert ratio < 2.0, f"constant folding not effective, ratio={ratio:.2f}"

        tdSql.query("explain select bit_count(29) from meters")
        plan = " ".join(str(row[0]) for row in tdSql.queryResult)
        assert "Projection" in plan
        assert "columns=" in plan
        assert "width=" in plan
        assert "bit_count" not in plan.lower()

    def test_bit_count_prepared(self):
        """Scalar: bit_count prepared statements

        1. Bind integer, floating point and string parameters.
        2. Verify STMT2 and legacy statement results match constants.

        Catalog:
            - Function:Scalar

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: 7172772636

        History:
            - 2026-11-21 Codex Added BIT_COUNT prepared-statement coverage from FS/TS.

        """
        self._prepare("bit_count_bind")
        tdSql.execute("create table t (ts timestamp, v int)")
        tdSql.execute("insert into t values (1000, 1)")

        for value, expected in ((29, 4), (2.9, 2), ("29", 4), ("12abc", 2)):
            statement = self.conn.statement2("select bit_count(?) from t")
            try:
                statement.bind_param(None, None, [[value]])
                statement.execute()
                assert statement.result().fetch_all() == [(expected,)]
            finally:
                statement.close()

        for use_stmt2 in (False, True):
            sql = "select bit_count(?) from t"
            statement = self.conn.statement2(sql) if use_stmt2 else self.conn.statement(sql)
            try:
                if use_stmt2:
                    statement.bind_param(None, None, [[-1]])
                    statement.execute()
                    rows = statement.result().fetch_all()
                else:
                    params = new_bind_params(1)
                    params[0].bigint(-1)
                    statement.bind_param(params)
                    statement.execute()
                    rows = statement.use_result().fetch_all()
                assert rows == [(64,)]
            finally:
                statement.close()

    def test_bit_count_errors(self):
        """Scalar: bit_count parameter validation

        1. Reject zero and multiple parameters with 0x80002801.
        2. Reject JSON and GEOMETRY with 0x80002802.
        3. Reject the b'...' bit literal and verify the error messages.

        Catalog:
            - Function:Scalar

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: 7172772636

        History:
            - 2026-11-21 Codex Added BIT_COUNT error coverage from FS/TS.
            - 2026-11-23 DECIMAL and binary types are supported, only JSON/GEOMETRY remain rejected.

        """
        self._prepare("bit_count_err")

        num_message = "Invalid number of parameters : bit_count"
        type_message = "Invalid parameter data type : bit_count"
        tdSql.error(
            "select bit_count()",
            expectedErrno=0x80002801,
            expectErrInfo=num_message,
            fullMatched=False,
        )
        tdSql.error(
            "select bit_count(1, 2)",
            expectedErrno=0x80002801,
            expectErrInfo=num_message,
            fullMatched=False,
        )
        tdSql.error(
            "select bit_count(to_json('{}'))",
            expectedErrno=0x80002802,
            expectErrInfo=type_message,
            fullMatched=False,
        )
        tdSql.error(
            "select bit_count(st_geomfromtext('POINT(1 2)'))",
            expectedErrno=0x80002802,
            expectErrInfo=type_message,
            fullMatched=False,
        )
        tdSql.error("select bit_count(b'1011')")

    def test_bit_count_mysql_samples(self):
        """Scalar: bit_count MySQL 8.0 fixed sample comparison

        1. Compare TDengine results with the MySQL 8.0 outputs of the same SQL.
        2. Record the FS section 7 accepted differences separately.

        Catalog:
            - Function:Scalar

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: 7172772636

        History:
            - 2026-11-21 Codex Added the MySQL 8.0 fixed sample comparison from FS/TS.
            - 2026-11-23 FLOAT/DECIMAL/binary samples are now bit-exact with MySQL.

        """
        self._prepare("bit_count_mysql")

        # Expected column: MySQL 8.0 output of the same expression. Every sample
        # below is bit-exact between MySQL and TDengine.
        samples = (
            ("bit_count(0)", 0),
            ("bit_count(1)", 1),
            ("bit_count(2)", 1),
            ("bit_count(3)", 2),
            ("bit_count(7)", 3),
            ("bit_count(29)", 4),
            ("bit_count(255)", 8),
            ("bit_count(256)", 1),
            ("bit_count(65535)", 16),
            ("bit_count(4294967295)", 32),
            ("bit_count(cast(18446744073709551615 as bigint unsigned))", 64),
            ("bit_count(9223372036854775807)", 63),
            ("bit_count(-1)", 64),
            ("bit_count(-2)", 63),
            ("bit_count(-2147483648)", 33),
            ("bit_count(null)", None),
            ("bit_count('29')", 4),
            ("bit_count('12abc')", 2),
            ("bit_count('abc')", 0),
            ("bit_count('')", 0),
            ("bit_count(2.9)", 2),
            ("bit_count(-2.9)", 63),
            ("bit_count(cast(2.9 as decimal(5,1)))", 2),
            ("bit_count(cast('ABCD' as varbinary))", 9),
        )
        for sql, expected in samples:
            self._check_scalar(sql, expected)

        # FS section 7 accepted differences: NaN/Inf and out-of-range values
        # return NULL, text overflow saturates to the signed 64-bit boundary,
        # and the b'...' bit literal is not supported.
        accepted = (
            ("bit_count(cast(1e308 as float))", None),
            ("bit_count(1e308)", None),
            ("bit_count('18446744073709551615')", 63),
        )
        for sql, expected in accepted:
            self._check_scalar(sql, expected)
        tdSql.error("select bit_count(b'1011')")

    def test_bit_count_privilege(self):
        """Scalar: bit_count privilege requirements

        1. A user with USE and SELECT on the database can run bit_count.
        2. A user without SELECT on another database is rejected.
        3. The function itself requires no additional privilege.

        Catalog:
            - Function:Scalar

        Since: v3.5.0.0

        Labels: common,ci,integration,functional,security

        Jira: 7172772636

        History:
            - 2026-11-21 Codex Added the privilege/security coverage from TS section 10.

        """
        if is_community_edition():
            pytest.skip("GRANT is available on enterprise edition only")

        self._prepare("bit_count_priv")
        tdSql.execute("create table t (ts timestamp, v int)")
        tdSql.execute("insert into t values (1000, 29)")

        tdSql.execute("drop database if exists bit_count_priv_other")
        tdSql.execute("create database bit_count_priv_other keep 36500")
        tdSql.execute("create table bit_count_priv_other.t (ts timestamp, v int)")
        tdSql.execute("insert into bit_count_priv_other.t values (1000, 29)")

        tdSql.execute("drop user if exists bit_count_user")
        tdSql.execute("create user bit_count_user pass 'BitCount@2026'")
        tdSql.execute("grant use on database bit_count_priv to bit_count_user")
        tdSql.execute("grant select on bit_count_priv.* to bit_count_user")
        # USE on the other database is granted on purpose so the rejection below
        # comes from the missing SELECT privilege rather than the database one.
        tdSql.execute("grant use on database bit_count_priv_other to bit_count_user")
        try:
            granted = tdCom.newTdSql(user="bit_count_user", password="BitCount@2026")
            granted.query("select bit_count(v) from bit_count_priv.t")
            granted.checkRows(1)
            granted.checkData(0, 0, 4)

            # The user has USE but no SELECT on the other database, so bit_count
            # must be rejected by the privilege check, not by a function rule.
            info = granted.error(
                "select bit_count(v) from bit_count_priv_other.t",
                expectErrInfo="Permission denied",
                fullMatched=False,
            )
            tdLog.info(f"bit_count without privilege rejected: {info}")
        finally:
            tdSql.execute("drop user if exists bit_count_user")
            tdSql.execute("drop database if exists bit_count_priv_other")

    def test_bit_count_stream(self):
        """Stream: bit_count interval window calculation

        1. Verify an interval stream window computes bit_count over rows.
        2. Compare each window result with the equivalent batch query.

        Catalog:
            - Function:Scalar/Stream

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: 7172772636

        History:
            - 2026-11-21 Codex Added BIT_COUNT stream coverage from FS/TS.

        """
        tdStream.ensureSnode()

        class StreamInterval(StreamCheckItem):
            def __init__(self):
                self.db = "bit_count_stream"

            def create(self):
                tdSql.execute("drop database if exists bit_count_stream")
                tdSql.execute("create database bit_count_stream vgroups 1 buffer 8")
                tdSql.execute("use bit_count_stream")
                tdSql.execute("create table t1(ts timestamp, v int)")
                tdSql.execute(
                    "create stream s_bc interval(2s) sliding(2s) from t1 "
                    "stream_options(max_delay(3s)) into bc_out as "
                    "select _twstart as s, bit_count(max(v)) c "
                    "from t1 where ts >= _twstart and ts < _twend"
                )

            def insert1(self):
                tdSql.execute(
                    "insert into t1 values(1704067200000,3)(1704067201000,7)"
                    "(1704067202000,8)(1704067203000,15)"
                )

            def check1(self):
                tdSql.checkResultsByFunc(
                    "select c from bc_out order by s",
                    lambda: tdSql.getRows() == 2
                    and tdSql.getData(0, 0) == popcount(7)
                    and tdSql.getData(1, 0) == popcount(15),
                )

        tdStream.checkAll([StreamInterval()])
