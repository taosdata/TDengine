# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import time
from decimal import Decimal

from new_test_framework.utils import tdLog, tdSql, etool, tdStream
from taos import new_bind_params


class TestMysqlDatetimeFunctions:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepare(self):
        tdSql.execute("drop database if exists mysql_datetime_functions")
        tdSql.execute("create database mysql_datetime_functions precision 'us'")
        tdSql.execute("use mysql_datetime_functions")
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.execute("create table t (ts timestamp, v int, s varchar(40), mode int)")
        tdSql.execute(
            "insert into t values "
            "('2024-02-28 23:59:59.123456', 1, '2024-02-28', 0) "
            "('2024-02-29 00:00:00.123456', 2, '2024-02-29', 3) "
            "('2024-03-01 01:02:03.654321', 3, '2024-03-01', 7)"
        )
        print("mysql datetime setup .................. [ passed ]")

    def do_current(self):
        # Read the us table: SELECT without FROM uses the default ms precision.
        tdSql.query(
            "select curdate(), current_date(), current_date, "
            "curtime(6), current_time(6), current_time, "
            "cast(current_timestamp(0) as bigint), "
            "cast(current_timestamp(3) as bigint), "
            "cast(current_timestamp(6) as bigint), "
            "cast(current_timestamp as bigint), "
            "unix_timestamp(), unix_timestamp() from t limit 1"
        )
        tdSql.checkRows(1)
        assert tdSql.getData(0, 0) == tdSql.getData(0, 1) == tdSql.getData(0, 2)
        assert tdSql.getData(0, 3) == tdSql.getData(0, 4)
        assert len(tdSql.getData(0, 3).split(".")[-1]) == 6
        timestamp0 = tdSql.getData(0, 6)
        timestamp3 = tdSql.getData(0, 7)
        timestamp6 = tdSql.getData(0, 8)
        assert timestamp0 == timestamp6 - timestamp6 % 1_000_000
        assert timestamp3 == timestamp6 - timestamp6 % 1_000
        assert tdSql.getData(0, 9) == timestamp0
        assert tdSql.getData(0, 10) == tdSql.getData(0, 11)
        tdSql.query("select cast(now() as bigint), cast(current_timestamp as bigint) from t limit 1")
        now_ticks = tdSql.getData(0, 0)
        assert tdSql.getData(0, 1) == now_ticks - now_ticks % 1_000_000
        tdSql.query("select curtime(6), current_time(6), unix_timestamp() from t order by ts")
        tdSql.checkRows(3)
        for row in range(3):
            assert tdSql.getData(row, 0) == tdSql.getData(row, 1)
            assert tdSql.getData(row, 0) == tdSql.getData(0, 0)
            assert tdSql.getData(row, 2) == tdSql.getData(0, 2)
        for fsp in range(7):
            tdSql.query(f"select curtime({fsp}), current_time({fsp})")
            actual = tdSql.getData(0, 0)
            assert actual == tdSql.getData(0, 1)
            assert len(actual) == 8 + (fsp + 1 if fsp else 0)
        print("current time and aliases ............... [ passed ]")

    def do_calendar(self):
        samples = (
            ("datediff('2024-03-01 00:00:00','2024-02-29 23:59:59')", 1),
            ("day('2024-02-29')", 29),
            ("dayofmonth('2024-02-29')", 29),
            ("dayofyear('2024-02-29')", 60),
            ("dayname('2024-02-29')", "Thursday"),
            ("hour('272:59:59')", 272),
            ("hour('-272:59:59')", 272),
            ("microsecond('12:34:56.012345')", 12345),
            ("minute('12:34:56')", 34),
            ("month('2024-02-29')", 2),
            ("monthname('2024-02-29')", "February"),
            ("quarter('2024-02-29')", 1),
            ("second('12:34:56')", 56),
            ("year('0000-01-01')", 0),
            ("year('0000-00-00')", 0),
            ("day('0000-00-00')", 0),
            ("month('0000-00-00')", 0),
            ("to_days('0000-00-00')", None),
            ("to_days('0000-01-01')", 1),
            ("to_days('0000-02-29')", None),
            ("to_days('0000-03-01')", 60),
            ("from_days(719528)", "1970-01-01"),
            ("from_days(0)", "0000-00-00"),
            ("from_days(-1)", "0000-00-00"),
            ("from_days(1)", "0000-00-00"),
            ("from_days(365)", "0000-00-00"),
            ("from_days(366)", "0001-01-01"),
            ("to_seconds('0000-01-01')", 86400),
            ("to_days('9999-12-31')", 3652424),
            ("from_days(3652424)", "9999-12-31"),
            ("from_days(3652425)", None),
            ("hour('838:59:59')", 838),
            ("hour('839:00:00')", 838),
            ("minute('839:00:00')", 59),
            ("second('-839:00:00')", 59),
            ("microsecond('838:59:59.123456')", 0),
            ("extract(HOUR_SECOND from '-839:00:00')", -8385959),
            ("yearweek('2000-01-01')", 199952),
        )
        for sql, expected in samples:
            tdSql.query(f"select {sql}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, expected)
        print("calendar values and boundaries ........ [ passed ]")

    def do_text_values(self):
        try:
            for zone in ("UTC", "+08:00"):
                tdSql.execute(f"SET TIMEZONE '{zone}'")
                before = time.time() - 1
                tdSql.query(
                    "select unix_timestamp(ts), unix_timestamp(current_timestamp), "
                    "d, current_date, tm, current_time, date_format(dt, '%Y-%m-%d') "
                    "from TEXT(ts TIMESTAMP, d VARCHAR(10), tm VARCHAR(8), dt TIMESTAMP) VALUES "
                    "(CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME, CURRENT_DATE) "
                    "(CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME, CURRENT_DATE) v"
                )
                after = time.time() + 1
                tdSql.checkRows(2)
                for row in range(2):
                    assert before <= float(tdSql.getData(row, 0)) <= after
                    assert tdSql.getData(row, 0) == tdSql.getData(row, 1)
                    assert tdSql.getData(row, 2) == tdSql.getData(row, 3) == tdSql.getData(row, 6)
                    assert tdSql.getData(row, 4) == tdSql.getData(row, 5)
        finally:
            tdSql.execute("SET TIMEZONE 'UTC'")

    def do_iso8601_extended_years(self):
        for zone, expected in (
            ("UTC", "4000-01-01T00:00:00.000+0000"),
            ("Asia/Shanghai", "4000-01-01T08:00:00.000+0800"),
        ):
            tdSql.query(f"select to_iso8601(CAST(64060588800000 AS TIMESTAMP), '{zone}')")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, expected)

    def do_text_fraction_rounding(self):
        format_text = "%Y-%m-%d %H:%i:%s.%f"
        dates = (
            ("2024-02-29 12:34:56.1234564999", "2024-02-29 12:34:56.123456"),
            ("2024-02-29 12:34:56.1234565", "2024-02-29 12:34:56.123457"),
            ("2024-02-28 23:59:59.9999995", "2024-02-29 00:00:00.000000"),
            ("2023-02-28 23:59:59.9999995", "2023-03-01 00:00:00.000000"),
            ("2024-12-31 23:59:59.9999995", "2025-01-01 00:00:00.000000"),
            ("2024-12-31T23:59:59.9999995Z", "2025-01-01 00:00:00.000000"),
            ("0000-00-00 00:00:00.0000005", "0000-00-00 00:00:00.000001"),
            ("0000-00-00 00:00:00.9999995", None),
            ("2024-00-01 00:00:00.9999995", None),
            ("9999-12-31 23:59:59.9999995", None),
            ("2060-11-31 23:59:59.9999995", None),
        )
        tdSql.execute("create table fraction_dates (ts timestamp, s varchar(64), n nchar(64))")
        values = " ".join(
            f"({1709164800000000 + row}, '{text}', '{text}')" for row, (text, _) in enumerate(dates)
        )
        tdSql.execute(f"insert into fraction_dates values {values}")
        for text, expected in dates:
            tdSql.query(f"select date_format('{text}', '{format_text}')")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, expected)
        tdSql.query(
            f"select date_format(s, '{format_text}'), date_format(n, '{format_text}') "
            "from fraction_dates order by ts"
        )
        tdSql.checkRows(len(dates))
        for row, (_, expected) in enumerate(dates):
            tdSql.checkData(row, 0, expected)
            tdSql.checkData(row, 1, expected)

        expressions = (
            ("year('2024-12-31 23:59:59.9999995')", 2025),
            ("datediff('2024-12-31 23:59:59.9999995', '2025-01-01')", 0),
            ("unix_timestamp('2024-12-31 23:59:59.9999995')", 1735689600),
            ("hour('2024-12-31 23:59:59.9999995')", 24),
            ("extract(DAY_HOUR from '2024-12-31 23:59:59.9999995')", 3124),
            ("microsecond('12:34:56.1234565')", 123457),
            ("second('12:34:56.9999995')", 57),
            ("minute('12:34:59.9999995')", 35),
            ("hour('-23:59:59.9999995')", 24),
            ("extract(HOUR_SECOND from '-23:59:59.9999995')", -240000),
            ("microsecond('838:59:59.0000005')", 0),
            ("extract(HOUR_SECOND from '838:59:58.9999995')", 8385959),
            (f"str_to_date('2024-12-31 23:59:59.9999995', '{format_text}')", "2024-12-31 23:59:59.999999"),
        )
        for expression, expected in expressions:
            tdSql.query(f"select {expression}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, expected)
        print("text fraction rounding ................. [ passed ]")

    def do_from_days_boundaries(self):
        tdSql.execute(
            "create table from_days_bounds "
            "(ts timestamp, unsigned_day bigint unsigned, decimal_day decimal(20, 0), float_day double)"
        )
        tdSql.execute(
            "insert into from_days_bounds values "
            "('2024-01-01 00:00:00', 3652424, 3652424, 3652424) "
            "('2024-01-01 00:00:01', 9223372036854775808, 18446744073710271144, 1e100)"
        )
        tdSql.query(
            "select from_days(unsigned_day), from_days(decimal_day), from_days(float_day) "
            "from from_days_bounds order by ts"
        )
        tdSql.checkRows(2)
        for column in range(3):
            tdSql.checkData(0, column, "9999-12-31")
            tdSql.checkData(1, column, None)
        tdSql.query("select from_days(719528.9e0), from_days(719528.1e0)")
        tdSql.checkData(0, 0, "1970-01-02")
        tdSql.checkData(0, 1, "1970-01-01")
        print("from_days numeric bounds ............... [ passed ]")

    def do_extract(self):
        sample = "'2019-07-02 01:02:03.123456'"
        expected = {
            "MICROSECOND": 123456,
            "SECOND": 3,
            "MINUTE": 2,
            "HOUR": 1,
            "DAY": 2,
            "MONTH": 7,
            "QUARTER": 3,
            "YEAR": 2019,
            "SECOND_MICROSECOND": 3123456,
            "MINUTE_MICROSECOND": 203123456,
            "MINUTE_SECOND": 203,
            "HOUR_MICROSECOND": 10203123456,
            "HOUR_SECOND": 10203,
            "HOUR_MINUTE": 102,
            "DAY_MICROSECOND": 2010203123456,
            "DAY_SECOND": 2010203,
            "DAY_MINUTE": 20102,
            "DAY_HOUR": 201,
            "YEAR_MONTH": 201907,
        }
        for unit, value in expected.items():
            tdSql.query(f"select extract({unit} from {sample})")
            tdSql.checkData(0, 0, value)
        tdSql.query(f"select extract(WEEK from {sample})")
        tdSql.checkData(0, 0, 26)
        tdSql.query("select extract(HOUR from ts), extract(DAY from s) from t order by ts")
        tdSql.checkRows(3)
        for row, (hour, day) in enumerate(((23, 28), (0, 29), (1, 1))):
            tdSql.checkData(row, 0, hour)
            tdSql.checkData(row, 1, day)
        print("extract units .......................... [ passed ]")

    def do_extract_time_inputs(self):
        date_units = (("YEAR", 2024, 2024), ("YEAR_MONTH", 202402, 202400),
                      ("QUARTER", 1, 0), ("MONTH", 2, 0), ("WEEK", 8, 613566753), ("DAY", 29, 10))
        for value in ("'12:34:56.123456'", "'-25:34:56.123456'", "'838:59:59'",
                      "cast('12:34:56.123456' as nchar(32))"):
            expressions = ", ".join(f"extract({unit} from {value})" for unit, _, _ in date_units)
            tdSql.query(f"select {expressions}")
            for column in range(len(date_units)):
                tdSql.checkData(0, column, None)
        values = ("12:34:56.123456", "-25:34:56.123456", "838:59:59", "2024-02-29",
                  "2024-00-10", "0000-00-00", "2060-11-31")
        tdSql.execute("create table extract_inputs (ts timestamp, s varchar(40), n nchar(40))")
        try:
            rows = " ".join(f"('2024-02-{row + 1:02d}', '{value}', '{value}')"
                            for row, value in enumerate(values))
            tdSql.execute(f"insert into extract_inputs values {rows}")
            for unit, complete, incomplete in date_units:
                tdSql.query(f"select extract({unit} from s), extract({unit} from n) "
                            "from extract_inputs order by ts")
                tdSql.checkRows(len(values))
                zero = None if unit == "WEEK" else 0
                for row, expected in enumerate((None, None, None, complete, incomplete, zero, None)):
                    tdSql.checkData(row, 0, expected)
                    tdSql.checkData(row, 1, expected)
            for unit, expected_values in (
                ("HOUR", (12, -25, 838, 0, 0, 0, None)),
                ("HOUR_MINUTE", (1234, -2534, 83859, 0, 0, 0, None)),
            ):
                tdSql.query(f"select extract({unit} from s), extract({unit} from n) "
                            "from extract_inputs order by ts")
                for row, expected in enumerate(expected_values):
                    tdSql.checkData(row, 0, expected)
                    tdSql.checkData(row, 1, expected)
        finally:
            tdSql.execute("drop table extract_inputs")
        print("extract date/time input distinction ... [ passed ]")

    def do_format_parse(self):
        tdSql.query("select date_format('2024-02-29 23:04:05.012345', '%W %M %d %Y %H:%i:%s.%f')")
        tdSql.checkData(0, 0, "Thursday February 29 2024 23:04:05.012345")
        tdSql.query("select date_format('2024-02-29', '%D %j %% %q')")
        tdSql.checkData(0, 0, "29th 060 % q")
        tdSql.query("select date_format('2000-01-01', '%U %u %V %v %X %x')")
        tdSql.checkData(0, 0, "00 00 52 52 1999 1999")
        tdSql.query("select date_format('0000-01-01', '%U %u %V %v %X %x')")
        tdSql.checkData(0, 0, "01 00 01 52 0000 4294967295")
        tdSql.query(
            "select date_format('2024-02-29 23:04:05.012345', "
            "'%a %b %c %D %e %f %H %h %I %i %j %k %l %M %m %p %r %S %s %T %W %w %Y %y %% %q')"
        )
        tdSql.checkData(
            0, 0,
            "Thu Feb 2 29th 29 012345 23 11 11 04 060 23 11 February 02 "
            "PM 11:04:05 PM 05 05 23:04:05 Thursday 4 2024 24 % q",
        )
        tdSql.query("select str_to_date('May 1, 2013tail', '%M %d, %Y')")
        tdSql.checkData(0, 0, "2013-05-01")
        tdSql.query("select str_to_date('abc', 'abc'), str_to_date('Monday', '%W')")
        tdSql.checkData(0, 0, "0000-00-00")
        tdSql.checkData(0, 1, "0000-00-00")
        tdSql.query("select str_to_date('12:34:56 PM', '%r')")
        tdSql.checkData(0, 0, "12:34:56")
        tdSql.query("select str_to_date('12', '%h'), str_to_date('11', '%I'), "
                    "str_to_date('23', '%l')")
        tdSql.checkData(0, 0, "00:00:00")
        tdSql.checkData(0, 1, "11:00:00")
        tdSql.checkData(0, 2, None)
        tdSql.query("select str_to_date('2024 060', '%Y %j')")
        tdSql.checkData(0, 0, "2024-02-29")
        tdSql.query("select str_to_date('1999 52 6', '%X %V %w')")
        tdSql.checkData(0, 0, "2000-01-01")
        for sql, expected in (
            ("str_to_date('2024-02-29', '%Y-%m-%d %H:%i:%s')", "2024-02-29 00:00:00"),
            ("str_to_date('2024-02-29   ', '%Y-%m-%d %H:%i:%s.%f')", "2024-02-29 00:00:00.000000"),
            ("str_to_date('12', '%H:%i:%s')", "12:00:00"),
            ("str_to_date('12:34', '%T')", "12:34:00"),
            ("str_to_date('12', '%r')", "00:00:00"),
            ("str_to_date('2024-02-29x', '%Y-%m-%d %H:%i:%s')", None),
            ("str_to_date('2024-02-29', '%Y-%m-%d %%H')", "2024-02-29"),
            ("str_to_date('2024-02-29', '%Y-%m-%d %p')", "2024-02-29"),
            ("str_to_date('2024%02%29', '%Y%%%m%%%d')", None),
            ("date_format('2024-02-29', '%Y%%%m%%%d')", "2024%02%29"),
            ("date_format('0000-00-00', '%W')", None),
            ("date_format('0000-00-00', '%a')", None),
            ("date_format('0000-00-00', '%w')", None),
            ("date_format('0000-00-00', '%Y %W')", None),
            ("date_format('0000-00-00', '%Y-%m-%d')", "0000-00-00"),
            ("str_to_date('12:34:56.123456', '%T.%f')", "12:34:56.123456"),
            ("str_to_date(' 2024  \t02  29', '%Y %m %d')", "2024-02-29"),
            ("str_to_date('69-01-01', '%Y-%m-%d')", "2069-01-01"),
            ("str_to_date('70-01-01', '%Y-%m-%d')", "1970-01-01"),
            ("str_to_date('2023 366', '%Y %j')", "2024-01-01"),
            ("str_to_date('2024 00 0', '%Y %U %w')", "2023-12-31"),
            ("str_to_date('2021 53 1', '%x %v %w')", "2022-01-03"),
            ("str_to_date('2024 01 0', '%Y %V %w')", None),
            ("str_to_date('2024 01 0', '%X %v %w')", None),
            ("str_to_date('23 PM', '%H %p')", None),
            ("str_to_date('11 PM', '%h %p')", "23:00:00"),
            ("date_format('2024-02-29', '')", None),
            ("date_format('2024-00-10', '%M')", None),
            ("date_format('2024-00-10', '%b')", None),
            ("from_unixtime(0, '')", None),
        ):
            tdSql.query(f"select {sql}")
            tdSql.checkData(0, 0, expected)
        tdSql.query("select str_to_date(date_format(ts, '%X %V %w'), '%X %V %w') "
                    "from t order by ts")
        for row, expected in enumerate(("2024-02-28", "2024-02-29", "2024-03-01")):
            tdSql.checkData(row, 0, expected)
        tdSql.query("select str_to_date('2nd', '%D'), str_to_date('2xx', '%D')")
        tdSql.checkData(0, 0, "0000-00-02")
        tdSql.checkData(0, 1, "0000-00-02")
        tdSql.query("select dayofyear(cast('2024-02-29' as nchar(20))), "
                    "date_format(cast('2024-02-29' as nchar(20)), "
                    "cast('%Y-%m-%d' as nchar(20)))")
        tdSql.checkData(0, 0, 60)
        tdSql.checkData(0, 1, "2024-02-29")
        tdSql.query("select date_format(ts, case when v=2 then '%Y' "
                    "else '%m' end) from t order by ts")
        for row, expected in enumerate(("02", "2024", "03")):
            tdSql.checkData(row, 0, expected)
        tdSql.query("select to_char(ts, 'yyyy-mm-dd'), "
                    "date_format(ts, '%Y-%m-%d') from t where v = 2")
        tdSql.checkData(0, 0, "2024-02-29")
        tdSql.checkData(0, 1, "2024-02-29")
        tdSql.query("select year(to_timestamp('2024-02-29', 'yyyy-mm-dd'))")
        tdSql.checkData(0, 0, 2024)
        maximum_format = "x" * 4095
        tdSql.query(f"select count(*) from t group by date_format(ts, '{maximum_format}')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)
        expanding_format = "%Y" * 1024
        tdSql.query(
            f"select date_format('2024-01-01', '{expanding_format}'), "
            f"from_unixtime(0, '{expanding_format}')"
        )
        tdSql.checkData(0, 0, "2024" * 1024)
        tdSql.checkData(0, 1, "1970" * 1024)
        shorter_format = "%Y" * 600
        tdSql.query(
            "select date_format(ts, case when v=1 then "
            f"'{shorter_format}' else '{expanding_format}' end) "
            "from t order by ts"
        )
        tdSql.checkData(0, 0, "2024" * 600)
        tdSql.checkData(1, 0, "2024" * 1024)
        tdSql.checkData(2, 0, "2024" * 1024)
        nchar_format = "%Y" * 600
        tdSql.query(
            f"select date_format('2024-01-01', cast('{nchar_format}' as nchar(1200))), "
            f"from_unixtime(0, cast('{nchar_format}' as nchar(1200)))"
        )
        tdSql.checkData(0, 0, "2024" * 600)
        tdSql.checkData(0, 1, "1970" * 600)
        tdSql.query(f"select str_to_date('bad', cast('{nchar_format}' as nchar(1200)))")
        tdSql.checkData(0, 0, None)
        tdSql.query(
            f"select date_format(ts, cast('{nchar_format}' as nchar(1200))) "
            "from t order by ts"
        )
        for row in range(3):
            tdSql.checkData(row, 0, "2024" * 600)
        print("format and parse ....................... [ passed ]")

    def do_unix(self):
        tdSql.query("select from_unixtime(0), from_unixtime(1.123456), "
                    "from_unixtime(1, '%Y-%m-%d %H:%i:%s'), unix_timestamp('1970-01-01 00:00:01')")
        tdSql.checkData(0, 0, "1970-01-01 00:00:00")
        tdSql.checkData(0, 1, "1970-01-01 00:00:01.123456")
        tdSql.checkData(0, 2, "1970-01-01 00:00:01")
        tdSql.checkData(0, 3, 1)
        tdSql.query("select unix_timestamp('1970-01-01 00:00:01.123456')")
        assert str(tdSql.getData(0, 0)) == "1.123456"
        tdSql.query("select from_unixtime(-1), from_unixtime(32536771200)")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        print("unix seconds and microseconds ......... [ passed ]")

    def do_relaxed_dates(self):
        samples = (
            ("2024/02/29", 2024, 60, 1, "2024-02-29 00:00:00"),
            ("24-02-29", 2024, 60, 1, "2024-02-29 00:00:00"),
            ("2024^02@29 11*30*45", 2024, 60, 1, "2024-02-29 11:30:45"),
            ("00/02/29", 2000, 60, 8767, "2000-02-29 00:00:00"),
            ("23/02/29", None, None, None, None),
            ("2060/11/31", None, None, None, None),
            ("24/02/29 24*00*00", None, None, None, None),
        )
        tdSql.execute("create table relaxed_dates (ts timestamp, s varchar(64), n nchar(64))")
        values = " ".join(
            f"({1709164800000000 + row}, '{sample[0]}', '{sample[0]}')"
            for row, sample in enumerate(samples)
        )
        tdSql.execute(f"insert into relaxed_dates values {values}")
        for column in ("s", "n"):
            tdSql.query(
                f"select year({column}), dayofyear({column}), datediff('2024/03/01', {column}), "
                f"date_format({column}, '%Y-%m-%d %H:%i:%s') from relaxed_dates order by ts"
            )
            tdSql.checkRows(len(samples))
            for row, sample in enumerate(samples):
                for col, expected in enumerate(sample[1:]):
                    tdSql.checkData(row, col, expected)
        tdSql.query("select year('69/12/31'), year('70/01/01'), year('00/02/29'), "
                    "year('00/00/00'), hour('12:34:56'), hour('2024/02/29 11*30*45'), "
                    "dayofyear('24-02-29'), datediff('2024/03/01', '2024/02/29')")
        for col, expected in enumerate((2069, 1970, 2000, 0, 12, 11, 60, 1)):
            tdSql.checkData(0, col, expected)
        print("relaxed date text and year mapping .... [ passed ]")

    def do_dst_gaps(self):
        samples = (
            ("America/New_York", "2024-03-10 01:59:59.999999", "1710053999.999999"),
            ("America/New_York", "2024-03-10 02:00:00", "1710054000.000000"),
            ("America/New_York", "2024-03-10 02:30:00.123456", "1710054000.123456"),
            ("America/New_York", "2024-03-10 02:59:59.999999", "1710054000.999999"),
            ("America/New_York", "2024-03-10 03:00:00", "1710054000.000000"),
            ("America/New_York", "2024-03-10T02:30:00-05:00", "1710055800.000000"),
            ("Australia/Lord_Howe", "2024-10-06 02:15:00.123456", "1728142200.123456"),
            ("Pacific/Apia", "2011-12-30 12:00:00.123456", "1325239200.123456"),
        )
        tdSql.execute("create table dst_gaps (ts timestamp, s varchar(64), n nchar(64), id int)")
        values = " ".join(
            f"({1709164800000000 + row}, '{sample[1]}', '{sample[1]}', {row})"
            for row, sample in enumerate(samples)
        )
        tdSql.execute(f"insert into dst_gaps values {values}")
        try:
            for row, (zone, text, expected) in enumerate(samples):
                tdSql.execute(f"SET TIMEZONE '{zone}'")
                tdSql.query(f"select unix_timestamp('{text}')")
                assert str(tdSql.getData(0, 0)) == expected
                tdSql.query(f"select unix_timestamp(s), unix_timestamp(n) from dst_gaps where id = {row}")
                tdSql.checkRows(1)
                for col in range(2):
                    assert str(tdSql.getData(0, col)) == expected
        finally:
            tdSql.execute("SET TIMEZONE 'UTC'")
        print("Unix timestamp across timezone gaps .. [ passed ]")

    def do_week_and_timezone(self):
        expected = (202052, 202053, 202052, 202053, 202053, 202052, 202053, 202052)
        for mode, value in enumerate(expected):
            tdSql.query(f"select yearweek('2021-01-01', {mode})")
            tdSql.checkData(0, 0, value)
        for mode in range(8):
            tdSql.query(f"select yearweek('0000-01-01', {mode})")
            tdSql.checkData(0, 0, 4294967248 if mode % 2 else 1)
        tdSql.query("select date_format(ts, '%Y-%m-%d %H:%i:%s') from t where v = 3")
        tdSql.checkData(0, 0, "2024-03-01 01:02:03")
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        try:
            tdSql.query("select date_format(ts, '%Y-%m-%d %H:%i:%s') from t where v = 3")
            tdSql.checkData(0, 0, "2024-02-29 20:02:03")
            tdSql.query("select datediff(ts, '2024-02-28') from t where v = 3")
            tdSql.checkData(0, 0, 1)
            tdSql.query("select hour('2024-02-29T23:00:00Z'), "
                        "from_unixtime(1710055800), "
                        "unix_timestamp('2024-03-10 03:30:00')")
            tdSql.checkData(0, 0, 18)
            tdSql.checkData(0, 1, "2024-03-10 03:30:00")
            assert str(tdSql.getData(0, 2)) == "1710055800.000000"
            tdSql.query(
                "select unix_timestamp('2024-11-03T01:30:00-04:00'), "
                "unix_timestamp('2024-11-03T01:30:00-05:00')"
            )
            assert str(tdSql.getData(0, 0)) == "1730611800.000000"
            assert str(tdSql.getData(0, 1)) == "1730615400.000000"
        finally:
            tdSql.execute("SET TIMEZONE 'UTC'")
        print("week modes and timezones .............. [ passed ]")

    def do_precision(self):
        for precision, literal, expected in (
            ("ms", "2024-02-29 00:00:00.123456", 123000),
            ("us", "2024-02-29 00:00:00.123456", 123456),
            ("ns", "2024-02-29 00:00:00.123456789", 123456),
        ):
            database = f"mysql_datetime_{precision}"
            tdSql.execute(f"drop database if exists {database}")
            tdSql.execute(f"create database {database} precision '{precision}'")
            tdSql.execute(f"use {database}")
            try:
                tdSql.execute("create table t (ts timestamp, v int, historical timestamp)")
                tdSql.execute(f"insert into t values ('{literal}', 1, -1)")
                tdSql.query(
                    "select microsecond(ts), date_format(ts, '%f'), "
                    "year(historical), month(historical), day(historical), "
                    "hour(historical), minute(historical), second(historical), "
                    "microsecond(historical), "
                    "date_format(historical, '%Y-%m-%d %H:%i:%s.%f') from t"
                )
                tdSql.checkData(0, 0, expected)
                tdSql.checkData(0, 1, f"{expected:06d}")
                for column, part in enumerate((1969, 12, 31, 23, 59, 59), start=2):
                    tdSql.checkData(0, column, part)
                historical_fraction = 999000 if precision == "ms" else 999999
                tdSql.checkData(0, 8, historical_fraction)
                tdSql.checkData(
                    0, 9, f"1969-12-31 23:59:59.{historical_fraction:06d}"
                )
                tdSql.execute("create table unix_boundary (ts timestamp, historical timestamp)")
                ticks = (-1000, -999, -1, 0, 1, 999, 1000)
                rows = " ".join(f"('2024-02-29 00:00:{row:02d}', {value})"
                                for row, value in enumerate(ticks))
                tdSql.execute(f"insert into unix_boundary values {rows}")
                tdSql.query("select unix_timestamp(historical) from unix_boundary order by ts")
                tdSql.checkRows(len(ticks))
                for row, value in enumerate(ticks):
                    microseconds = value * 1000 if precision == "ms" else value if precision == "us" else value // 1000
                    expected_unix = None if value < 0 else Decimal(microseconds).scaleb(-6)
                    tdSql.checkData(row, 0, expected_unix)
            finally:
                tdSql.execute("use mysql_datetime_functions")
                tdSql.execute(f"drop database if exists {database}")
        print("millisecond/microsecond/nanosecond ... [ passed ]")

    def do_rows_and_errors(self):
        tdSql.query("select year(ts), dayofyear(s), yearweek(s, mode) from t order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2024)
        tdSql.checkData(1, 1, 60)
        tdSql.query("select count(*) from t where year(ts) = 2024")
        tdSql.checkData(0, 0, 3)
        tdSql.query("select year(ts), count(*) from t group by year(ts)")
        tdSql.checkData(0, 1, 3)
        tdSql.query("select dayofyear(a.s), month(b.ts) "
                    "from t a join t b on a.ts = b.ts order by a.ts")
        tdSql.checkRows(3)
        tdSql.checkData(1, 0, 60)
        tdSql.checkData(2, 1, 3)
        tdSql.query("select yr from (select year(ts) as yr from t) x order by yr")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2024)
        tdSql.query("select day(null), date_format(null, '%Y'), yearweek(null)")
        for column in range(3):
            tdSql.checkData(0, column, None)
        tdSql.query("select str_to_date('invalid', '%Y-%m-%d')")
        tdSql.checkData(0, 0, None)
        tdSql.query("select str_to_date('2021-11-31', '%Y-%m-%d')")
        tdSql.checkData(0, 0, None)
        print("rows, nulls and errors ............... [ passed ]")

    def do_day_time_format(self):
        cases = (
            ("2nd 12", "%D %H", "60:00:00"),
            ("02 12", "%d %H", "60:00:00"),
            ("2 12", "%e %H", "60:00:00"),
            ("2 12:34:56.123456", "%d %T.%f", "60:34:56.123456"),
            ("2 12:34:56 PM", "%e %r", "60:34:56"),
            ("0 00:00:00", "%d %T", "00:00:00"),
            ("31 23:59:59", "%d %T", "767:59:59"),
            ("32 00:00:00", "%d %T", None),
            ("2 24:00:00", "%d %T", None),
            ("2", "%d %H", "48:00:00"),
            ("2 123456", "%d %f", "48:00:00.123456"),
            ("2nd", "%D", "0000-00-02"),
            ("02", "%d", "0000-00-02"),
            ("2", "%e", "0000-00-02"),
            ("2024-02-02 12", "%Y-%m-%d %H", "2024-02-02 12:00:00"),
            ("2 12", "%d %H %Y", "0000-00-02 12:00:00"),
        )
        for text, fmt, expected in cases:
            tdSql.query(f"select str_to_date('{text}', '{fmt}')")
            tdSql.checkData(0, 0, expected)
        tdSql.execute("create table day_time_format (ts timestamp, s varchar(64), fmt varchar(64))")
        rows = " ".join(
            f"({1709164800000000 + i}, '{text}', '{fmt}')"
            for i, (text, fmt, _) in enumerate(cases)
        )
        tdSql.execute(f"insert into day_time_format values {rows}")
        tdSql.query("select str_to_date(s, fmt), "
                    "str_to_date(cast(s as nchar(64)), cast(fmt as nchar(64))) "
                    "from day_time_format order by ts")
        tdSql.checkRows(len(cases))
        for row, (_, _, expected) in enumerate(cases):
            tdSql.checkData(row, 0, expected)
            tdSql.checkData(row, 1, expected)
        tdSql.query("select hour(str_to_date('2nd 12', '%D %H'))")
        tdSql.checkData(0, 0, 60)
        print("day and time format .................... [ passed ]")

    def do_invalid_dates(self):
        tdSql.execute("create table invalid_dates (ts timestamp, s varchar(40))")
        tdSql.execute(
            "insert into invalid_dates values "
            "('2024-01-01 00:00:01', '2024-02-29') "
            "('2024-01-01 00:00:02', '2060-11-31') "
            "('2024-01-01 00:00:03', '2023-02-29') "
            "('2024-01-01 00:00:04', '2024-13-01') "
            "('2024-01-01 00:00:05', '2024-01-01 24:00:00')"
        )
        tdSql.query(
            "select year(s), day(s), date_format(s, '%Y-%m-%d'), to_days(s) "
            "from invalid_dates order by ts"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 2024)
        tdSql.checkData(0, 1, 29)
        tdSql.checkData(0, 2, "2024-02-29")
        assert tdSql.getData(0, 3) is not None
        for row in range(1, 5):
            for column in range(4):
                tdSql.checkData(row, column, None)

        invalid_results = (
            "datediff('2060-11-31', '2060-11-30')",
            "datediff('2060-11-30', '2060-11-31')",
            "dayname('2060-11-31')",
            "dayofyear('2023-02-29')",
            "extract(YEAR_MONTH from '2060-11-31')",
            "monthname('2024-13-01')",
            "quarter('2024-13-01')",
            "to_seconds('2060-11-31')",
            "unix_timestamp('2060-11-31')",
            "yearweek('2060-11-31')",
            "hour('12:60:00')",
            "minute('12:34:60')",
            "second('839:00:60')",
            "microsecond('not-a-time')",
            "str_to_date('2060-11-31', '%Y-%m-%d')",
            "str_to_date('2024-02-30', '%Y-%m-%d')",
            "str_to_date('2024-01-01 24:00:00', '%Y-%m-%d %H:%i:%s')",
        )
        for expression in invalid_results:
            tdSql.query(f"select {expression}")
            tdSql.checkData(0, 0, None)
        print("invalid dates and time values ......... [ passed ]")

    def do_timestamp_boundaries(self):
        # SELECT without FROM interprets numeric TIMESTAMP casts at ms precision.
        for ticks, expected in (
            (-62167219200001, None),
            (-62167219200000, 0),
            (253402300799999, 9999),
            (253402300800000, None),
        ):
            tdSql.query(f"select year(cast({ticks} as timestamp))")
            tdSql.checkData(0, 0, expected)

        samples = (
            ("2024-02-29T12:34:60Z", None, None),
            ("2024-12-31T23:59:61Z", None, None),
            ("2024-02-29T12:34:60+08:00", None, None),
            ("2024-12-31T23:59:61-05:00", None, None),
            ("2024-02-29T12:34:59Z", 2024, 59),
            ("2024-02-29T12:34:59+08:00", 2024, 59),
            ("24-02-29T12:34:56Z", 2024, 56),
            ("69-12-31T23:00:00+08:00", 2069, 0),
            ("70-01-01T00:00:00+00", 1970, 0),
            ("24/02/29T12^34*56+0800", 2024, 56),
            ("1969-12-31T23:59:59Z", 1969, 59),
            ("1969-12-31T23:59:59.123456Z", 1969, 59),
            ("0000-01-01T00:00:00Z", 0, 0),
            ("0000-02-28T00:00:00Z", 0, 0),
            ("0000-03-01T00:00:00Z", 0, 0),
            ("2024-02-29T12:34:56+15:00", None, None),
        )
        tdSql.execute("create table offset_bounds (ts timestamp, s varchar(40), n nchar(40))")
        values = " ".join(
            f"({1709164800000000 + row}, '{text}', '{text}')"
            for row, (text, _, _) in enumerate(samples)
        )
        tdSql.execute(f"insert into offset_bounds values {values}")
        for text, year, second in samples:
            tdSql.query(f"select year('{text}'), second('{text}')")
            tdSql.checkData(0, 0, year)
            tdSql.checkData(0, 1, second)
        for column in ("s", "n"):
            tdSql.query(f"select year({column}), second({column}) from offset_bounds order by ts")
            tdSql.checkRows(len(samples))
            for row, (_, year, second) in enumerate(samples):
                tdSql.checkData(row, 0, year)
                tdSql.checkData(row, 1, second)
        print("timestamp years and offset validation . [ passed ]")

    def do_invalid_arguments(self):
        for sql in (
            "select curdate(1)",
            "select current_date('2024-01-01')",
            "select curtime('3')",
            "select curtime(1.5)",
            "select curtime(-1)",
            "select current_time(-1)",
            "select current_timestamp(7)",
            "select datediff('2024-01-01')",
            "select datediff('2024-01-01', 123)",
            "select date_format('2024-01-01')",
            "select date_format('2024-01-01', 123)",
            "select day(123)",
            "select extract(UNKNOWN_UNIT from '2024-01-01')",
            "select extract(YEAR from 123)",
            "select from_days('invalid')",
            "select from_unixtime('invalid')",
            "select str_to_date('2024-01-01', 123)",
            "select yearweek('2024-01-01', 8)",
            "select yearweek('2024-01-01', 4294967296)",
            "select yearweek('2024-01-01', cast(4294967296 as bigint))",
        ):
            tdSql.error(sql)
        print("invalid signatures and types .......... [ passed ]")

    def do_stream(self):
        tdSql.query("show snodes")
        created_snode = tdSql.queryRows == 0
        if created_snode:
            tdStream.createSnode()
        try:
            tdSql.execute("create table stream_dates (ts timestamp, s varchar(40))")
            for expression in ("current_timestamp(6)", "curdate()", "unix_timestamp()"):
                tdSql.error(
                    "create stream mysql_datetime_clock_stream count_window(1) "
                    "from stream_dates into mysql_datetime_clock_out as "
                    f"select ts, {expression} as clock_value from %%trows"
                )
            tdSql.execute(
                "create stream mysql_datetime_stream count_window(1) "
                "from stream_dates into mysql_datetime_stream_out as "
                "select ts, year(ts) ts_yr, year(s) text_yr, "
                "date_format(s, '%Y-%m-%d') ds from %%trows"
            )
            tdStream.checkStreamStatus("mysql_datetime_stream")
            tdSql.execute(
                "insert into stream_dates values "
                "('2024-02-29 12:00:00', '2024-02-29') "
                "('2024-02-29 12:00:01', '2060-11-31')"
            )
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                tdSql.query("select ts_yr, text_yr, ds from mysql_datetime_stream_out order by ts")
                if tdSql.queryRows == 2:
                    break
                time.sleep(0.5)
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, 2024)
            tdSql.checkData(0, 1, 2024)
            tdSql.checkData(0, 2, "2024-02-29")
            tdSql.checkData(1, 0, 2024)
            tdSql.checkData(1, 1, None)
            tdSql.checkData(1, 2, None)
        finally:
            tdSql.execute("drop stream if exists mysql_datetime_stream")
            if created_snode:
                tdSql.execute("drop snode on dnode 1")
        print("stream scalar calculation ............. [ passed ]")

    def do_prepared_query(self):
        tdSql.query("select unix_timestamp('2024-02-29 12:34:56.123456') from t order by ts")
        tdSql.checkRows(3)
        for row in range(3):
            assert str(tdSql.getData(row, 0)) == "1709210096.123456"
        for sql, bind_value, expected in (
            ("select year(?)", "2024-02-29", 2024),
            ("select dayofyear(s) from t where v = ?", 2, 60),
        ):
            statement = self.conn.statement(sql)
            try:
                params = new_bind_params(1)
                if isinstance(bind_value, int):
                    params[0].int(bind_value)
                else:
                    params[0].binary(bind_value)
                statement.bind_param(params)
                statement.execute()
                assert statement.use_result().fetch_all() == [(expected,)]
            finally:
                statement.close()
        for use_stmt2 in (False, True):
            sql = "select unix_timestamp(?) from t where v = 1"
            statement = self.conn.statement2(sql) if use_stmt2 else self.conn.statement(sql)
            try:
                text = "2024-02-29 12:34:56.123456"
                if use_stmt2:
                    statement.bind_param(None, None, [[text]])
                else:
                    params = new_bind_params(1)
                    params[0].binary(text)
                    statement.bind_param(params)
                statement.execute()
                result = statement.result() if use_stmt2 else statement.use_result()
                rows = result.fetch_all()
                assert len(rows) == 1 and str(rows[0][0]) == "1709210096.123456"
            finally:
                statement.close()
        statement = self.conn.statement2("select unix_timestamp() from t where v = ?")
        try:
            statement.bind_param(None, None, [[1]])
            tdSql.query("select unix_timestamp()")
            bound_second = tdSql.getData(0, 0)
            time.sleep(1.2)
            statement.execute()
            executed_second = statement.result().fetch_all()[0][0]
            tdSql.query("select unix_timestamp()")
            assert bound_second < executed_second <= tdSql.getData(0, 0)
        finally:
            statement.close()
        statement = self.conn.statement("select unix_timestamp() from t where v = ?")
        try:
            params = new_bind_params(1)
            params[0].int(1)
            statement.bind_param(params)
            tdSql.query("select unix_timestamp()")
            bound_second = tdSql.getData(0, 0)
            time.sleep(1.2)
            statement.execute()
            executed_second = statement.use_result().fetch_all()[0][0]
            tdSql.query("select unix_timestamp()")
            assert bound_second < executed_second <= tdSql.getData(0, 0)
        finally:
            statement.close()
        print("prepared query parameters ............ [ passed ]")

    def do_prepared_timezone(self):
        # Keep the current-time function in the nested query so execute must refresh it.
        sql = (
            "select c, h from (select curtime(6) as c, from_unixtime(0, '%H') as h "
            "from t where v = ?) q"
        )
        for use_stmt2 in (False, True):
            tdSql.execute("SET TIMEZONE 'UTC'")
            statement = self.conn.statement2(sql) if use_stmt2 else self.conn.statement(sql)
            try:
                if use_stmt2:
                    statement.bind_param(None, None, [[1]])
                else:
                    params = new_bind_params(1)
                    params[0].int(1)
                    statement.bind_param(params)
                tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
                tdSql.query("select curtime(6)")
                before = tdSql.getData(0, 0)
                statement.execute()
                result = statement.result() if use_stmt2 else statement.use_result()
                rows = result.fetch_all()
                tdSql.query("select curtime(6)")
                after = tdSql.getData(0, 0)
                assert len(rows) == 1
                assert rows[0][1] == "08"
                actual = rows[0][0]
                # Clock strings wrap at midnight.
                in_window = before <= actual <= after if before <= after else actual >= before or actual <= after
                assert in_window
            finally:
                statement.close()
                tdSql.execute("SET TIMEZONE 'UTC'")
        for sql in (
            "select from_unixtime(0, '%H')",
            "select h from (select from_unixtime(0, '%H') as h from t where v = 1) q",
        ):
            tdSql.execute("SET TIMEZONE 'UTC'")
            statement = self.conn.statement2(sql)
            try:
                tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
                statement.execute()
                assert statement.result().fetch_all() == [("08",)]
            finally:
                statement.close()
                tdSql.execute("SET TIMEZONE 'UTC'")
        print("prepared query timezone refresh ...... [ passed ]")

    def cleanup(self):
        tdSql.execute("drop database if exists mysql_datetime_functions")
        print("mysql datetime cleanup ................ [ passed ]")

    def test_mysql_datetime_functions(self):
        """MySQL-compatible date and time scalar functions

        1. Cover all 27 signatures, aliases, EXTRACT units and date formatting.
        2. Cover year zero, leap days, invalid dates, large TIME values and argument errors.
        3. Cover row evaluation, WHERE and GROUP BY.
        4. Cover JOIN, subquery, delayed prepared SELECT execution and stream calculation.
        5. Reject statement-clock functions in persisted stream calculations.
        6. Cover format strings whose expanded result exceeds 4095 bytes.
        7. Cover pre-1970 fractional timestamps and numeric FROM_DAYS overflow.
        8. Cover partial format input, zero-date weekday formatting and prepared-query timezone changes.
        9. Cover day-plus-time parsing, date-only formats, bounds and VARCHAR/NCHAR column formats.
        10. Reject TIME-only input for EXTRACT date units and negative Unix timestamps before precision conversion.
        11. Cover relaxed date separators, two-digit years and timezone gaps with fractional seconds.
        12. Cover decimal constant projections, direct Unix timestamp bindings and literal STMT2 timezone refresh.
        13. Cover native timestamp year bounds, offset clock validation, two-digit years and pre-epoch dates.
        14. Cover text fraction rounding, date/time carry differences, TIME clipping and unchanged STR_TO_DATE precision.
        15. Cover TEXT current-time literals, statement-clock consistency and timezone-aware date casts.
        16. Cover ISO8601 formatting beyond year 3000 with UTC and named zones.

        Catalog:
            - Function:Scalar

        Since: v3.4.4.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-10-20 Peng Rongkun Add MySQL date-time function regression.
            - 2026-10-20 Codex Add invalid-input and stream scenarios.
            - 2026-11-05 Peng Rongkun Cover review fixes for time precision and boundaries.
            - 2026-11-10 Peng Rongkun Cover partial input and prepared-query timezone refresh.
            - 2026-11-10 Peng Rongkun Cover STR_TO_DATE day-plus-time format classification.
            - 2026-11-10 Peng Rongkun Cover EXTRACT input classification and Unix epoch precision boundaries.
            - 2026-11-10 Peng Rongkun Fix current-clock precision checks and cover relaxed dates and timezone gaps.
            - 2026-11-11 Peng Rongkun Cover decimal projections, literal STMT2 context and TIME/format boundaries.
            - 2026-11-11 Peng Rongkun Cover native timestamp years and explicit-offset invalid clock fields.
            - 2026-11-11 Peng Rongkun Cover text fraction rounding for constants and VARCHAR/NCHAR columns.

        """
        self.prepare()
        try:
            self.do_current()
            self.do_text_values()
            self.do_iso8601_extended_years()
            self.do_calendar()
            self.do_text_fraction_rounding()
            self.do_relaxed_dates()
            self.do_from_days_boundaries()
            self.do_extract()
            self.do_extract_time_inputs()
            self.do_format_parse()
            self.do_day_time_format()
            self.do_unix()
            self.do_week_and_timezone()
            self.do_dst_gaps()
            self.do_precision()
            self.do_rows_and_errors()
            self.do_invalid_dates()
            self.do_timestamp_boundaries()
            self.do_invalid_arguments()
            self.do_prepared_query()
            self.do_prepared_timezone()
            self.do_stream()
        finally:
            self.cleanup()
