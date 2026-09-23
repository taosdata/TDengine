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

"""CI coverage for the C UDF examples that are published in the developer guide.

The two files below are inlined into ``docs/{zh,en}/10-developer-guide/06-udf.md``
via ``{{#include docs/examples/udf/...}}``, i.e. they are part of the user facing
contract of the C UDF framework:

* ``docs/examples/udf/max_vol.c``     -- aggregate UDF returning a string (BINARY)
  result: maximum voltage over several input columns together with the device id
  and the (row, column) position of that maximum.
* ``docs/examples/udf/extract_avg.c`` -- aggregate UDF that splits a comma
  separated string and averages all values of all rows (``interBuf`` carries the
  running sum/count across AGG_PROC calls).

Both examples are built by the CMake targets ``max_vol`` / ``extract_avg``
(``source/libs/function/test/CMakeLists.txt``) and are located in the build tree
at run time.  Keeping them under CI means an engine / UDF framework change that
breaks a documented example fails the pipeline instead of the user.
"""

import random

from new_test_framework.utils import tdLog, tdSql

from udf_utils import find_udf_lib

# error reported by the client when a UDF callback fails (input validation)
UDF_EXEC_FAILURE = 0x8000290A  # TSDB_CODE_UDF_FUNC_EXEC_FAILURE


class TestUdfCExamples:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @staticmethod
    def _unique_db(prefix):
        """A unique database name per run avoids "Database in dropping status"
        when a previous run was killed before the DROP had finished."""
        return "udfdoc_%s_%d" % (prefix, random.randint(10000, 99999))

    @staticmethod
    def _find_example_lib(name):
        lib = find_udf_lib(name)
        if not lib:
            raise RuntimeError(
                "UDF library for the '%s' example not found in the build tree. "
                "Build the CMake target first (cmake --build <build-dir> --target %s)."
                % (name, name)
            )
        tdLog.info("using UDF library: %s" % lib)
        return lib

    # ------------------------------------------------------------------ max_vol
    def test_max_vol(self):
        """max_vol aggregate UDF (docs/examples/udf/max_vol.c)

        1. Locate libmax_vol.so built from the documented example
        2. Register it as aggregate function: binary(64) output, bufsize 10240
        3. Check the documented full-table result (max value + device id + position)
        4. Check the result for a subset of the voltage columns
        5. Check per-window results with interval(2s)
        6. Check the input validation (column count / column types)

        Since: v3.4.2.8

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-21 Alex Duan Created to cover docs/examples/udf/max_vol.c in CI

        """
        lib = self._find_example_lib("max_vol")

        db = self._unique_db("maxvol")
        tdSql.execute("drop function if exists max_vol")
        tdSql.execute("drop database if exists %s" % db)
        tdSql.execute("create database %s vgroups 3" % db)
        tdSql.execute("use %s" % db)

        tdSql.execute(
            "create table battery (ts timestamp, vol1 float, vol2 float, "
            "vol3 float, deviceId varchar(16))"
        )
        tdSql.execute(
            "insert into battery values"
            "('2026-01-01 00:00:00', 5.0, 1.0, 2.0, 'devA')"
            "('2026-01-01 00:00:01', 3.0, 4.0, 9.0, 'devB')"
            "('2026-01-01 00:00:02', 7.0, 0.5, 0.5, 'devC')"
            "('2026-01-01 00:00:03', null, 8.0, null, 'devD')"
            "('2026-01-01 00:00:04', 8.0, 1.0, 1.0, 'devE')"
            "('2026-01-01 00:00:05', 2.0, 2.0, 2.0, 'devF')"
        )

        tdSql.execute(
            "create aggregate function max_vol as '%s' "
            "outputtype binary(64) bufsize 10240 language 'C'" % lib
        )

        # ---- full table: 9.0 is the maximum, it is in vol3 (column 2) of row 1
        tdSql.query("select max_vol(vol1, vol2, vol3, deviceId) from battery")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "devB_(1,2)_9.000000")

        # ---- 8.0 in devE/vol1 wins once vol3 is no longer part of the input;
        #      the 8.0 of devD/vol2 is not reported because the columns are
        #      scanned column by column and only a strictly greater value wins
        tdSql.query("select max_vol(vol1, vol2, deviceId) from battery")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "devE_(4,0)_8.000000")

        tdSql.query("select max_vol(vol1, deviceId) from battery")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "devE_(4,0)_8.000000")

        # ---- per window: the reported (row, column) is relative to the window
        tdSql.query(
            "select max_vol(vol1, vol2, vol3, deviceId) from battery interval(2s)"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "devB_(1,2)_9.000000")
        tdSql.checkData(1, 0, "devD_(1,1)_8.000000")
        tdSql.checkData(2, 0, "devE_(0,0)_8.000000")

        # ---- no row at all: empty result set, no error and no bogus value
        tdSql.query(
            "select max_vol(vol1, vol2, vol3, deviceId) from battery "
            "where ts > '2030-01-01 00:00:00'"
        )
        tdSql.checkRows(0)

        # ---- input validation inside the example (not a parser/planner error)
        tdSql.error("select max_vol(vol1) from battery", UDF_EXEC_FAILURE)
        # last input column must be the varchar device id
        tdSql.error("select max_vol(vol1, vol2) from battery", UDF_EXEC_FAILURE)
        # every column but the last one must be float
        tdSql.error("select max_vol(vol1, ts) from battery", UDF_EXEC_FAILURE)

        tdSql.execute("drop function if exists max_vol")
        tdSql.execute("drop database if exists %s" % db)

    # -------------------------------------------------------------- extract_avg
    def test_extract_avg(self):
        """extract_avg aggregate UDF (docs/examples/udf/extract_avg.c)

        1. Locate libextract_avg.so built from the documented example
        2. Register it as aggregate function: double output, bufsize 16
        3. Check that all values of all rows are counted (comma separated string,
           spaces around the values, empty string, invalid token, NULL row)
        4. Check per-window results, including a window without any value
        5. Check a constant input and the input validation
        6. Check aggregation across the child tables of a super table

        Since: v3.4.2.8

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-21 Alex Duan Created to cover docs/examples/udf/extract_avg.c in CI

        """
        lib = self._find_example_lib("extract_avg")

        db = self._unique_db("extractavg")
        tdSql.execute("drop function if exists extract_avg")
        tdSql.execute("drop database if exists %s" % db)
        tdSql.execute("create database %s vgroups 3" % db)
        tdSql.execute("use %s" % db)

        # NOTE: the guide calls this table "scores"; SCORES is a parser keyword
        # (SHOW SCORES) and "insert into scores" is rejected, so a neutral name is
        # used here - the UDF behaviour tested below does not depend on it.
        tdSql.execute("create table scores_t (ts timestamp, varStr varchar(128))")
        tdSql.execute(
            "insert into scores_t values"
            "('2026-01-01 00:00:00', '1, 2, 3')"
            "('2026-01-01 00:00:01', '4,5')"
            "('2026-01-01 00:00:02', '6')"
            "('2026-01-01 00:00:03', '7.5')"
            "('2026-01-01 00:00:04', '')"
            "('2026-01-01 00:00:05', 'abc')"
            "('2026-01-01 00:00:06', null)"
        )

        tdSql.execute(
            "create aggregate function extract_avg as '%s' "
            "outputtype double bufsize 16 language 'C'" % lib
        )

        # ---- all values of all rows: (1+2+3+4+5+6+7.5) / 7, the empty string,
        #      the invalid token and the NULL row contribute nothing
        tdSql.query("select extract_avg(varStr) from scores_t")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.07142857142857)

        # ---- window [0s, 4s) holds all values, window [4s, 8s) holds none
        tdSql.query("select extract_avg(varStr) from scores_t interval(4s)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4.07142857142857)
        tdSql.checkData(1, 0, None)

        # ---- constant input
        tdSql.query("select extract_avg('10,20') from scores_t limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 15.0)

        # ---- input validation inside the example: the argument must be a string
        tdSql.error("select extract_avg(ts) from scores_t", UDF_EXEC_FAILURE)

        # ---- the intermediate sum/count must survive from one table to the next
        tdSql.execute(
            "create stable scores_stb (ts timestamp, varStr varchar(128)) tags (dev int)"
        )
        tdSql.execute("create table scores_d1 using scores_stb tags (1)")
        tdSql.execute("create table scores_d2 using scores_stb tags (2)")
        tdSql.execute(
            "insert into scores_d1 values('2026-01-01 00:00:00', '1,2')"
        )
        tdSql.execute(
            "insert into scores_d2 values('2026-01-01 00:00:00', '10,20')"
        )

        tdSql.query("select extract_avg(varStr) from scores_stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 8.25)

        # ---- one result per child table (order of the groups is not fixed)
        tdSql.query(
            "select extract_avg(varStr) from scores_stb partition by tbname"
        )
        tdSql.checkRows(2)
        avg1 = tdSql.getData(0, 0)
        avg2 = tdSql.getData(1, 0)
        tdSql.checkEqual(sorted([avg1, avg2]), [1.5, 15.0])

        tdSql.execute("drop function if exists extract_avg")
        tdSql.execute("drop database if exists %s" % db)
