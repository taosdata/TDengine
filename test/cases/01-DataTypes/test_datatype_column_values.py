# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""DataTypes: per-type column and tag value validation.

Twelve column types used to live in one file each. Every file repeated the same
scaffold (one deployment, one ``tdSql.prepare`` and the five helpers
``create_table`` / ``insert_data`` / ``auto_create_table`` / ``alter_tag_value`` /
``illegal_input``) and only the column type changed, so they run as one class here:
one deployment and one prepared database, with the type-specific tests kept apart
and using their own object names (``mt_<type>``, ``st_<type>_N``).

Types:
  bigint     bigint column and tag values, incl. NULL literal handling
  bool       bool column and tag values
  double     double column and tag values
  float      float column and tag values
  int        int column and tag values
  json       json column and tag values
  nchar      nchar column and tag values
  smallint   smallint column and tag values
  timestamp  timestamp column and tag values
  tinyint    tinyint column and tag values
  uint       unsigned int column and tag values
  varbinary  varbinary column and tag values
"""

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDatatypeColumnValues:
    def setup_class(cls):
            tdLog.debug(f"start to execute {__file__}")
            tdSql.prepare(dbname="db", drop=True)

    # ======================================================================
    # bigint: bigint column and tag values, incl. NULL literal handling
    # ======================================================================

    def test_datatype_bigint(self):
            """DataTypes: bigint

            1. Create table
            2. Insert data
            3. Auto-create table
            4. Alter tag value
            5. Handle illegal input

            Since: v3.0.0.0

            Labels: common,ci,integration,functional

            Jira: None

            History:
                - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_bigint.sim

            """
            self.bigint_create_table()
            self.bigint_insert_data()
            self.bigint_auto_create_table()
            self.bigint_alter_tag_value()
            self.bigint_illegal_input()

    def bigint_create_table(self):
            tdLog.info(f"create super table")
            tdSql.execute(
                f"create table mt_bigint (ts timestamp, c bigint) tags(tagname bigint)"
            )

            tdLog.info(f"case 0: static create table for test tag values")

            tdSql.execute(f"create table st_bigint_0 using mt_bigint tags(NULL)")
            tdSql.query(f"show create table st_bigint_0")
            tdSql.query(f"show tags from st_bigint_0")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_bigint_1 using mt_bigint tags(NULL)")
            tdSql.query(f"show tags from st_bigint_1")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_bigint_2 using mt_bigint tags('NULL')")
            tdSql.query(f"show tags from st_bigint_2")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_bigint_3 using mt_bigint tags('NULL')")
            tdSql.query(f"show tags from st_bigint_3")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_bigint_4 using mt_bigint tags("NULL")')
            tdSql.query(f"show tags from st_bigint_4")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_bigint_5 using mt_bigint tags("NULL")')
            tdSql.query(f"show tags from st_bigint_5")
            tdSql.checkData(0, 5, None)

            tdSql.execute(
                f"create table st_bigint_6 using mt_bigint tags(-9223372036854775807)"
            )
            tdSql.query(f"show tags from st_bigint_6")
            tdSql.checkData(0, 5, -9223372036854775807)

            tdSql.execute(
                f"create table st_bigint_7 using mt_bigint tags(9223372036854775807)"
            )
            tdSql.query(f"show tags from st_bigint_7")
            tdSql.checkData(0, 5, 9223372036854775807)

            tdSql.execute(f"create table st_bigint_8 using mt_bigint tags(37)")
            tdSql.query(f"show tags from st_bigint_8")
            tdSql.checkData(0, 5, 37)

            tdSql.execute(f"create table st_bigint_9 using mt_bigint tags(-100)")
            tdSql.query(f"show tags from st_bigint_9")
            tdSql.checkData(0, 5, -100)

            tdSql.execute(f"create table st_bigint_10 using mt_bigint tags(+113)")
            tdSql.query(f"show tags from st_bigint_10")
            tdSql.checkData(0, 5, 113)

            tdSql.execute(f"create table st_bigint_11 using mt_bigint tags('-100')")
            tdSql.query(f"show tags from st_bigint_11")
            tdSql.checkData(0, 5, -100)

            tdSql.execute(f'create table st_bigint_12 using mt_bigint tags("+78")')
            tdSql.query(f"show tags from st_bigint_12")
            tdSql.checkData(0, 5, 78)

            tdSql.execute(f"create table st_bigint_13 using mt_bigint tags(+0078)")
            tdSql.query(f"show tags from st_bigint_13")
            tdSql.checkData(0, 5, 78)

            tdSql.execute(f"create table st_bigint_14 using mt_bigint tags(-00078)")
            tdSql.query(f"show tags from st_bigint_14")
            tdSql.checkData(0, 5, -78)

    def bigint_insert_data(self):
            tdLog.info(f"case 1: insert values for test column values")

            tdSql.execute(f"insert into st_bigint_0 values(now, NULL)")
            tdSql.query(f"select * from st_bigint_0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_bigint_1 values(now, NULL)")
            tdSql.query(f"select * from st_bigint_1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_bigint_2 values(now, NULL)")
            tdSql.query(f"select * from st_bigint_2")
            tdSql.checkRows(1)

            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_bigint_3 values(now, NULL)")
            tdSql.query(f"select * from st_bigint_3")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_bigint_4 values(now, NULL)")
            tdSql.query(f"select * from st_bigint_4")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_bigint_5 values(now, NULL)")
            tdSql.query(f"select * from st_bigint_5")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_bigint_6 values(now, 9223372036854775807)")
            tdSql.query(f"select * from st_bigint_6")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 9223372036854775807)

            tdSql.execute(f"insert into st_bigint_7 values(now, -9223372036854775807)")
            tdSql.query(f"select * from st_bigint_7")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -9223372036854775807)

            tdSql.execute(f"insert into st_bigint_8 values(now, +100)")
            tdSql.query(f"select * from st_bigint_8")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 100)

            tdSql.execute(f'insert into st_bigint_9 values(now, "-098")')
            tdSql.query(f"select * from st_bigint_9")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -98)

            tdSql.execute(f"insert into st_bigint_10 values(now, '0')")
            tdSql.query(f"select * from st_bigint_10")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_bigint_11 values(now, -0)")
            tdSql.query(f"select * from st_bigint_11")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f'insert into st_bigint_12 values(now, "+056")')
            tdSql.query(f"select * from st_bigint_12")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 56)

            tdSql.execute(f"insert into st_bigint_13 values(now, +056)")
            tdSql.query(f"select * from st_bigint_13")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 56)

            tdSql.execute(f"insert into st_bigint_14 values(now, -056)")
            tdSql.query(f"select * from st_bigint_14")
            tdSql.checkRows(1)

            tdSql.checkData(0, 1, -56)

    def bigint_auto_create_table(self):
            tdLog.info(f"case 2: dynamic create table for test tag values")

            tdSql.execute(
                f"insert into st_bigint_16 using mt_bigint tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show create table st_bigint_16")
            tdSql.query(f"show tags from st_bigint_16")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_bigint_16")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_bigint_17 using mt_bigint tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show tags from st_bigint_17")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_bigint_17")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_bigint_18 using mt_bigint tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_bigint_18")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_bigint_18")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_bigint_19 using mt_bigint tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_bigint_19")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_bigint_19")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_bigint_20 using mt_bigint tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_bigint_20")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_bigint_20")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_bigint_21 using mt_bigint tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_bigint_21")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_bigint_21")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_bigint_22 using mt_bigint tags(9223372036854775807) values(now, 9223372036854775807)"
            )
            tdSql.query(f"show tags from st_bigint_22")
            tdSql.checkData(0, 5, 9223372036854775807)

            tdSql.query(f"select * from st_bigint_22")
            tdSql.checkData(0, 1, 9223372036854775807)

            tdSql.execute(
                f"insert into st_bigint_23 using mt_bigint tags(-9223372036854775807) values(now, -9223372036854775807)"
            )
            tdSql.query(f"show tags from st_bigint_23")
            tdSql.checkData(0, 5, -9223372036854775807)

            tdSql.query(f"select * from st_bigint_23")
            tdSql.checkData(0, 1, -9223372036854775807)

            tdSql.execute(
                f"insert into st_bigint_24 using mt_bigint tags(10) values(now, 10)"
            )
            tdSql.query(f"show tags from st_bigint_24")
            tdSql.checkData(0, 5, 10)

            tdSql.query(f"select * from st_bigint_24")
            tdSql.checkData(0, 1, 10)

            tdSql.execute(
                f'insert into st_bigint_25 using mt_bigint tags("-0") values(now, "-0")'
            )
            tdSql.query(f"show tags from st_bigint_25")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_bigint_25")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_bigint_26 using mt_bigint tags('123') values(now, '123')"
            )
            tdSql.query(f"show tags from st_bigint_26")
            tdSql.checkData(0, 5, 123)

            tdSql.query(f"select * from st_bigint_26")
            tdSql.checkData(0, 1, 123)

            tdSql.execute(
                f"insert into st_bigint_27 using mt_bigint tags(+056) values(now, +00056)"
            )
            tdSql.query(f"show tags from st_bigint_27")
            tdSql.checkData(0, 5, 56)

            tdSql.query(f"select * from st_bigint_27")
            tdSql.checkData(0, 1, 56)

            tdSql.execute(
                f"insert into st_bigint_28 using mt_bigint tags(-056) values(now, -0056)"
            )
            tdSql.query(f"show tags from st_bigint_28")
            tdSql.checkData(0, 5, -56)

            tdSql.query(f"select * from st_bigint_28")
            tdSql.checkData(0, 1, -56)

    def bigint_alter_tag_value(self):
            tdLog.info(f"case 3: alter tag value")

            tdSql.execute(f"alter table st_bigint_0 set tag tagname=9223372036854775807")
            tdSql.query(f"show tags from  st_bigint_0")
            tdSql.checkData(0, 5, 9223372036854775807)

            tdSql.execute(f"alter table st_bigint_0 set tag tagname=-9223372036854775807")
            tdSql.query(f"show tags from  st_bigint_0")
            tdSql.checkData(0, 5, -9223372036854775807)

            tdSql.execute(f"alter table st_bigint_0 set tag tagname=+100")
            tdSql.query(f"show tags from  st_bigint_0")
            tdSql.checkData(0, 5, 100)

            tdSql.execute(f"alter table st_bigint_0 set tag tagname=-33")
            tdSql.query(f"show tags from  st_bigint_0")
            tdSql.checkData(0, 5, -33)

            tdSql.execute(f"alter table st_bigint_0 set tag tagname='+98'")
            tdSql.query(f"show tags from  st_bigint_0")
            tdSql.checkData(0, 5, 98)

            tdSql.execute(f"alter table st_bigint_0 set tag tagname='-076'")
            tdSql.query(f"show tags from  st_bigint_0")
            tdSql.checkData(0, 5, -76)

            tdSql.execute(f"alter table st_bigint_0 set tag tagname=+0012")
            tdSql.query(f"show tags from  st_bigint_0")
            tdSql.checkData(0, 5, 12)

            tdSql.execute(f"alter table st_bigint_0 set tag tagname=-00063")
            tdSql.query(f"show tags from  st_bigint_0")
            tdSql.checkData(0, 5, -63)

    def bigint_illegal_input(self):
            tdLog.info(f"case 4: illegal input")

            tdSql.error(
                f"create table st_bigint_e0  using mt_bigint tags(9223372036854775808)"
            )
            tdSql.execute(
                f"create table st_bigint_e0_1 using mt_bigint tags(-9223372036854775808)"
            )
            tdSql.error(
                f"create table st_bigint_e0_2 using mt_bigint tags(92233720368547758080)"
            )
            tdSql.error(
                f"create table st_bigint_e0_3 using mt_bigint tags(-9223372036854775809)"
            )

            # truncate integer part
            tdSql.execute(f"create table st_bigint_e0 using mt_bigint tags(12.80)")
            tdSql.execute(f"drop table st_bigint_e0")
            tdSql.execute(f"create table st_bigint_e0 using mt_bigint tags(-11.80)")
            tdSql.execute(f"drop table st_bigint_e0")
            tdSql.error(f"create table st_bigint_e0 using mt_bigint tags(123abc)")
            tdSql.error(f'create table st_bigint_e0 using mt_bigint tags("123abc")')
            tdSql.error(f"create table st_bigint_e0 using mt_bigint tags(abc)")
            tdSql.error(f'create table st_bigint_e0 using mt_bigint tags("abc")')
            tdSql.error(f'create table st_bigint_e0 using mt_bigint tags(" ")')
            tdSql.error(f"create table st_bigint_e0_error using mt_bigint tags('')")

            tdSql.execute(f"create table st_bigint_e0 using mt_bigint tags(123)")
            tdSql.execute(f"create table st_bigint_e1 using mt_bigint tags(123)")
            tdSql.execute(f"create table st_bigint_e2 using mt_bigint tags(123)")
            tdSql.execute(f"create table st_bigint_e3 using mt_bigint tags(123)")
            tdSql.execute(f"create table st_bigint_e4 using mt_bigint tags(123)")
            tdSql.execute(f"create table st_bigint_e5 using mt_bigint tags(123)")
            tdSql.execute(f"create table st_bigint_e6 using mt_bigint tags(123)")
            tdSql.execute(f"create table st_bigint_e7 using mt_bigint tags(123)")
            tdSql.execute(f"create table st_bigint_e8 using mt_bigint tags(123)")
            tdSql.execute(f"create table st_bigint_e9 using mt_bigint tags(123)")
            tdSql.execute(f"create table st_bigint_e10 using mt_bigint tags(123)")
            tdSql.execute(f"create table st_bigint_e11 using mt_bigint tags(123)")
            tdSql.execute(f"create table st_bigint_e12 using mt_bigint tags(123)")

            tdSql.error(f"insert into st_bigint_e0 values(now, 9223372036854775808)")
            tdSql.execute(f"insert into st_bigint_e1 values(now, -9223372036854775808)")
            tdSql.error(f"insert into st_bigint_e2 values(now, 9223372036854775809)")
            tdSql.execute(f"insert into st_bigint_e3 values(now, -9223372036854775808)")
            tdSql.execute(f"insert into st_bigint_e4 values(now, 922337203.6854775808)")
            tdSql.execute(f"insert into st_bigint_e5 values(now, -922337203685477580.9)")
            tdSql.error(f"insert into st_bigint_e6 values(now, 123abc)")
            tdSql.error(f'insert into st_bigint_e7 values(now, "123abc")')
            tdSql.error(f"insert into st_bigint_e9 values(now, abc)")
            tdSql.error(f'insert into st_bigint_e10 values(now, "abc")')
            tdSql.error(f'insert into st_bigint_e11 values(now, " ")')
            tdSql.error(f"insert into st_bigint_e12 values(now, '')")

            tdSql.error(
                f"insert into st_bigint_e13 using mt_bigint tags(033) values(now, 9223372036854775808)"
            )
            tdSql.execute(
                f"insert into st_bigint_e14 using mt_bigint tags(033) values(now, -9223372036854775808)"
            )
            tdSql.error(
                f"insert into st_bigint_e15 using mt_bigint tags(033) values(now, 9223372036854775818)"
            )
            tdSql.error(
                f"insert into st_bigint_e16 using mt_bigint tags(033) values(now, -9923372036854775808)"
            )
            tdSql.execute(
                f"insert into st_bigint_e17 using mt_bigint tags(033) values(now, 92233720368547758.08)"
            )
            tdSql.execute(
                f"insert into st_bigint_e18 using mt_bigint tags(033) values(now, -92233720368547.75808)"
            )
            tdSql.error(
                f"insert into st_bigint_e19 using mt_bigint tags(033) values(now, 123abc)"
            )
            tdSql.error(
                f'insert into st_bigint_e20 using mt_bigint tags(033) values(now, "123abc")'
            )
            tdSql.error(
                f"insert into st_bigint_e22 using mt_bigint tags(033) values(now, abc)"
            )
            tdSql.error(
                f'insert into st_bigint_e23 using mt_bigint tags(033) values(now, "abc")'
            )
            tdSql.error(
                f'insert into st_bigint_e24 using mt_bigint tags(033) values(now, " ")'
            )
            tdSql.error(
                f"insert into st_bigint_e25 using mt_bigint tags(033) values(now, '')"
            )

            tdSql.error(
                f"insert into st_bigint_e13_0 using mt_bigint tags(9223372036854775808) values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_bigint_e14_0 using mt_bigint tags(-9223372036854775808) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_bigint_e15_0 using mt_bigint tags(9223372036854775809) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_bigint_e16_0 using mt_bigint tags(-9223372036854775898) values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_bigint_e17 using mt_bigint tags(12.80) values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_bigint_e18 using mt_bigint tags(-11.80) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_bigint_e19 using mt_bigint tags(123abc) values(now, -033)"
            )
            tdSql.error(
                f'insert into st_bigint_e20 using mt_bigint tags("123abc") values(now, -033)'
            )
            tdSql.error(
                f"insert into st_bigint_e22 using mt_bigint tags(abc) values(now, -033)"
            )
            tdSql.error(
                f'insert into st_bigint_e23 using mt_bigint tags("abc") values(now, -033)'
            )
            tdSql.error(
                f'insert into st_bigint_e24 using mt_bigint tags(" ") values(now, -033)'
            )
            tdSql.error(
                f"insert into st_bigint_e25 using mt_bigint tags(" ") values(now, -033)"
            )

            tdSql.execute(
                f"insert into st_bigint_e13 using mt_bigint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_bigint_e14 using mt_bigint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_bigint_e15 using mt_bigint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_bigint_e16 using mt_bigint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_bigint_e17 using mt_bigint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_bigint_e18 using mt_bigint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_bigint_e19 using mt_bigint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_bigint_e20 using mt_bigint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_bigint_e21 using mt_bigint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_bigint_e22 using mt_bigint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_bigint_e23 using mt_bigint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_bigint_e24 using mt_bigint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_bigint_e25 using mt_bigint tags(033) values(now, 00062)"
            )

            tdSql.error(f"alter table st_bigint_e13 set tag tagname=9223372036854775808")
            tdSql.execute(f"alter table st_bigint_e13 set tag tagname=9223372036854775807")
            tdSql.error(f"alter table st_bigint_e14 set tag tagname=-9223372036854775809")
            tdSql.execute(f"alter table st_bigint_e14 set tag tagname=-9223372036854775808")
            tdSql.error(f"alter table st_bigint_e15 set tag tagname=92233720368547758080")
            tdSql.error(f"alter table st_bigint_e16 set tag tagname=-92233720368547758080")
            tdSql.error(f"alter table st_bigint_e19 set tag tagname=123abc")
            tdSql.error(f'alter table st_bigint_e20 set tag tagname="123abc"')
            tdSql.error(f"alter table st_bigint_e22 set tag tagname=abc")
            tdSql.error(f'alter table st_bigint_e23 set tag tagname="abc"')
            tdSql.error(f'alter table st_bigint_e24 set tag tagname=" "')
            tdSql.error(f"alter table st_bigint_e25 set tag tagname=''")


    # ======================================================================
    # bool: bool column and tag values
    # ======================================================================

    def test_datatype_bool(self):
            """DataTypes: bool

            1. Create table
            2. Insert data
            3. Auto-create table
            4. Alter tag value
            5. Handle illegal input

            Since: v3.0.0.0

            Labels: common,ci,integration,functional

            Jira: None

            History:
                - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_bool.sim

            """
            self.bool_create_table()
            self.bool_insert_data()
            self.bool_auto_create_table()
            self.bool_alter_tag_value()
            self.bool_illegal_input()

    def bool_create_table(self):
            tdLog.info(f"create super table")
            tdSql.execute(f"create table mt_bool (ts timestamp, c bool) tags(tagname bool)")

            tdLog.info(f"case 0: static create table for test tag values")

            tdSql.execute(f"create table st_bool_0 using mt_bool tags(NULL)")
            tdSql.query(f"show create table st_bool_0")
            tdSql.query(f"show tags from st_bool_0")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_bool_1 using mt_bool tags(NULL)")
            tdSql.query(f"show tags from st_bool_1")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_bool_2 using mt_bool tags('NULL')")
            tdSql.query(f"show tags from st_bool_2")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_bool_3 using mt_bool tags('NULL')")
            tdSql.query(f"show tags from st_bool_3")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_bool_4 using mt_bool tags("NULL")')
            tdSql.query(f"show tags from st_bool_4")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_bool_5 using mt_bool tags("NULL")')
            tdSql.query(f"show tags from st_bool_5")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_bool_6 using mt_bool tags("true")')
            tdSql.query(f"show tags from st_bool_6")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"create table st_bool_7 using mt_bool tags('true')")
            tdSql.query(f"show tags from st_bool_7")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"create table st_bool_8 using mt_bool tags(true)")
            tdSql.query(f"show tags from st_bool_8")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'create table st_bool_9 using mt_bool tags("false")')
            tdSql.query(f"show tags from st_bool_9")
            tdSql.checkData(0, 5, "false")

            tdSql.execute(f"create table st_bool_10 using mt_bool tags('false')")
            tdSql.query(f"show tags from st_bool_10")
            tdSql.checkData(0, 5, "false")

            tdSql.execute(f"create table st_bool_11 using mt_bool tags(false)")
            tdSql.query(f"show tags from st_bool_11")
            tdSql.checkData(0, 5, "false")

            tdSql.execute(f"create table st_bool_12 using mt_bool tags(0)")
            tdSql.query(f"show tags from st_bool_12")
            tdSql.checkData(0, 5, "false")

            tdSql.execute(f"create table st_bool_13 using mt_bool tags(1)")
            tdSql.query(f"show tags from st_bool_13")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"create table st_bool_14 using mt_bool tags(6.9)")
            tdSql.query(f"show tags from st_bool_14")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"create table st_bool_15 using mt_bool tags(-3)")
            tdSql.query(f"show tags from st_bool_15")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"create table st_bool_16 using mt_bool tags(+300)")
            tdSql.query(f"show tags from st_bool_16")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"create table st_bool_17 using mt_bool tags(-8.03)")
            tdSql.query(f"show tags from st_bool_17")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'create table st_bool_18 using mt_bool tags("-8.03")')
            tdSql.query(f"show tags from st_bool_18")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'create table st_bool_19 using mt_bool tags("+300")')
            tdSql.query(f"show tags from st_bool_19")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'create table st_bool_20 using mt_bool tags("-8e+2")')
            tdSql.query(f"show tags from st_bool_20")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'create table st_bool_21 using mt_bool tags("0x01")')
            tdSql.query(f"show tags from st_bool_21")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'create table st_bool_22 using mt_bool tags("0b01")')
            tdSql.query(f"show tags from st_bool_22")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'create table st_bool_23 using mt_bool tags("+0x01")')
            tdSql.query(f"show tags from st_bool_23")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'create table st_bool_24 using mt_bool tags("-0b00")')
            tdSql.query(f"show tags from st_bool_24")
            tdSql.checkData(0, 5, "false")

            tdSql.execute(f'create table st_bool_26 using mt_bool tags("-0.11e-30")')
            tdSql.query(f"show tags from st_bool_26")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'create table st_bool_27 using mt_bool tags("-1.0e-307")')
            tdSql.query(f"show tags from st_bool_27")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"create table st_bool_28 using mt_bool tags( -1e-1 )")
            tdSql.query(f"show tags from st_bool_28")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"create table st_bool_29 using mt_bool tags(-0.11e-30)")
            tdSql.query(f"show tags from st_bool_29")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"create table st_bool_30 using mt_bool tags(-1.1e-307)")
            tdSql.query(f"show tags from st_bool_30")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"create table st_bool_31 using mt_bool tags( 0x01)")
            tdSql.query(f"show tags from st_bool_31")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"create table st_bool_32 using mt_bool tags(0b01 )")
            tdSql.query(f"show tags from st_bool_32")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"create table st_bool_33 using mt_bool tags(+0x01)")
            tdSql.query(f"show tags from st_bool_33")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"create table st_bool_34 using mt_bool tags( -0b00 )")
            tdSql.query(f"show tags from st_bool_34")
            tdSql.checkData(0, 5, "false")

    def bool_insert_data(self):
            tdLog.info(f"case 1: insert values for test column values")

            tdSql.execute(f"insert into st_bool_0 values(now, NULL)")
            tdSql.query(f"select * from st_bool_0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_bool_1 values(now, NULL)")
            tdSql.query(f"select * from st_bool_1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_bool_2 values(now, 'NULL')")
            tdSql.query(f"select * from st_bool_2")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_bool_3 values(now, 'NULL')")
            tdSql.query(f"select * from st_bool_3")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f'insert into st_bool_4 values(now, "NULL")')
            tdSql.query(f"select * from st_bool_4")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f'insert into st_bool_5 values(now, "NULL")')
            tdSql.query(f"select * from st_bool_5")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f'insert into st_bool_6 values(now, "true")')
            tdSql.query(f"select * from st_bool_6")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_bool_7 values(now, 'true')")
            tdSql.query(f"select * from st_bool_7")
            tdSql.checkRows(1)

            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_bool_8 values(now, true)")
            tdSql.query(f"select * from st_bool_8")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_bool_9 values(now, "false")')
            tdSql.query(f"select * from st_bool_9")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_bool_10 values(now, 'false')")
            tdSql.query(f"select * from st_bool_10")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_bool_11 values(now, false)")
            tdSql.query(f"select * from st_bool_11")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_bool_12 values(now, 0)")
            tdSql.query(f"select * from st_bool_12")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_bool_13 values(now, 1)")
            tdSql.query(f"select * from st_bool_13")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_bool_14 values(now, 6.9)")
            tdSql.query(f"select * from st_bool_14")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_bool_15 values(now, -3)")
            tdSql.query(f"select * from st_bool_15")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_bool_16 values(now, +300)")
            tdSql.query(f"select * from st_bool_16")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_bool_17 values(now, -3.15)")
            tdSql.query(f"select * from st_bool_17")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_bool_18 values(now,"-8.03")')
            tdSql.query(f"select * from st_bool_18")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_bool_19 values(now,"+300")')
            tdSql.query(f"select * from st_bool_19")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_bool_20 values(now,"-8e+2")')
            tdSql.query(f"select * from st_bool_20")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_bool_21 values(now,"0x01")')
            tdSql.query(f"select * from st_bool_21")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_bool_22 values(now,"0b01")')
            tdSql.query(f"select * from st_bool_22")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_bool_23 values(now,"+0x01")')
            tdSql.query(f"select * from st_bool_23")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_bool_24 values(now,"-0b00")')
            tdSql.query(f"select * from st_bool_24")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f'insert into st_bool_26 values(now,"-0.11e-30")')
            tdSql.query(f"select * from st_bool_26")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_bool_27 values(now,"-1.0e-307")')
            tdSql.query(f"select * from st_bool_27")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_bool_28 values(now, -1e-1 )")
            tdSql.query(f"select * from st_bool_28")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_bool_29 values(now,-0.11e-30)")
            tdSql.query(f"select * from st_bool_29")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_bool_30 values(now,-1.1e-307)")
            tdSql.query(f"select * from st_bool_30")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_bool_31 values(now, 0x01)")
            tdSql.query(f"select * from st_bool_31")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_bool_32 values(now,0b01 )")
            tdSql.query(f"select * from st_bool_32")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_bool_33 values(now,+0x01)")
            tdSql.query(f"select * from st_bool_33")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_bool_34 values(now, -0b00 )")
            tdSql.query(f"select * from st_bool_34")
            tdSql.checkData(0, 1, 0)

    def bool_auto_create_table(self):
            tdLog.info(f"case 2: dynamic create table for test tag values")

            tdSql.execute(
                f"insert into st_bool_116 using mt_bool tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show create table st_bool_116")
            tdSql.query(f"show tags from st_bool_116")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_bool_116")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_bool_117 using mt_bool tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show tags from st_bool_117")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_bool_117")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_bool_118 using mt_bool tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_bool_118")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_bool_118")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_bool_119 using mt_bool tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_bool_119")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_bool_119")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_bool_120 using mt_bool tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_bool_120")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_bool_120")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_bool_121 using mt_bool tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_bool_121")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_bool_121")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_bool_122 using mt_bool tags("true") values(now, "true")'
            )
            tdSql.query(f"show tags from st_bool_122")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_122")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_bool_123 using mt_bool tags('true') values(now, 'true')"
            )
            tdSql.query(f"show tags from st_bool_123")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_123")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_bool_124 using mt_bool tags(true) values(now, true)"
            )
            tdSql.query(f"show tags from st_bool_124")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_124")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_bool_125 using mt_bool tags("false") values(now, "false")'
            )
            tdSql.query(f"show tags from st_bool_125")
            tdSql.checkData(0, 5, "false")

            tdSql.query(f"select * from st_bool_125")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_bool_126 using mt_bool tags('false') values(now, 'false')"
            )
            tdSql.query(f"show tags from st_bool_126")
            tdSql.checkData(0, 5, "false")

            tdSql.query(f"select * from st_bool_126")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_bool_127 using mt_bool tags(false) values(now, false)"
            )
            tdSql.query(f"show tags from st_bool_127")
            tdSql.checkData(0, 5, "false")

            tdSql.query(f"select * from st_bool_127")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_bool_128 using mt_bool tags(0) values(now, 0)")
            tdSql.query(f"show tags from st_bool_128")
            tdSql.checkData(0, 5, "false")

            tdSql.query(f"select * from st_bool_128")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_bool_129 using mt_bool tags(1) values(now, 1)")
            tdSql.query(f"show tags from st_bool_129")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_129")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_bool_130 using mt_bool tags(6.9) values(now, 6.9)"
            )
            tdSql.query(f"show tags from st_bool_130")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_130")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_bool_131 using mt_bool tags(-3) values(now, -3)")
            tdSql.query(f"show tags from st_bool_131")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_131")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_bool_132 using mt_bool tags(+300) values(now, +300)"
            )
            tdSql.query(f"show tags from st_bool_132")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_132")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_bool_133 using mt_bool tags(+30.890) values(now, +30.890)"
            )
            tdSql.query(f"show tags from st_bool_133")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_133")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_bool_218 using mt_bool tags("-8.03") values(now,"-8.03")'
            )
            tdSql.query(f"show tags from st_bool_218")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_218")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_bool_219 using mt_bool tags("+300") values(now,"+300")'
            )
            tdSql.query(f"show tags from st_bool_219")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_219")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_bool_220 using mt_bool tags("-8e+2") values(now,"-8e+2")'
            )
            tdSql.query(f"show tags from st_bool_220")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_220")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_bool_221 using mt_bool tags("0x01") values(now,"0x01")'
            )
            tdSql.query(f"show tags from st_bool_221")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_221")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_bool_222 using mt_bool tags("0b01") values(now,"0b01")'
            )
            tdSql.query(f"show tags from st_bool_222")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_222")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_bool_223 using mt_bool tags("+0x01") values(now,"+0x01")'
            )
            tdSql.query(f"show tags from st_bool_223")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_223")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_bool_224 using mt_bool tags("-0b00") values(now,"-0b00")'
            )
            tdSql.query(f"show tags from st_bool_224")
            tdSql.checkData(0, 5, "false")

            tdSql.query(f"select * from st_bool_224")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f'insert into st_bool_226 using mt_bool tags("-0.11e-30") values(now,"-0.11e-30")'
            )
            tdSql.query(f"show tags from st_bool_226")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_226")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_bool_227 using mt_bool tags("-1.0e-307") values(now,"-1.0e-307")'
            )
            tdSql.query(f"show tags from st_bool_227")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_227")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_bool_228 using mt_bool tags( -1e-1 ) values(now, -1e-1 )"
            )
            tdSql.query(f"show tags from st_bool_228")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_228")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_bool_229 using mt_bool tags(-0.11e-30) values(now,-0.11e-30)"
            )
            tdSql.query(f"show tags from st_bool_229")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_229")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_bool_230 using mt_bool tags(-1.1e-307) values(now,-1.1e-307)"
            )
            tdSql.query(f"show tags from st_bool_230")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_230")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_bool_231 using mt_bool tags( 0x01) values(now, 0x01)"
            )
            tdSql.query(f"show tags from st_bool_231")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_231")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_bool_232 using mt_bool tags(0b01 ) values(now, 0b01)"
            )
            tdSql.query(f"show tags from st_bool_232")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_bool_232")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_bool_233 using mt_bool tags(+0x01) values(now,+0x01)"
            )
            tdSql.query(f"show tags from st_bool_233")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(
                f"insert into st_bool_234 using mt_bool tags( -0b00 ) values(now, -0b00)"
            )
            tdSql.query(f"show tags from st_bool_234")
            tdSql.checkData(0, 5, "false")

            tdSql.query(f"select * from st_bool_234")
            tdSql.checkData(0, 1, 0)

    def bool_alter_tag_value(self):
            tdLog.info(f"case 3: alter tag value")

            tdSql.execute(f"alter table st_bool_16 set tag tagname=+300")
            tdSql.query(f"show tags from st_bool_16")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"alter table st_bool_17 set tag tagname=-8.03")
            tdSql.query(f"show tags from st_bool_17")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'alter table st_bool_18 set tag tagname="-8.03"')
            tdSql.query(f"show tags from st_bool_18")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'alter table st_bool_19 set tag tagname="+300"')
            tdSql.query(f"show tags from st_bool_19")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'alter table st_bool_20 set tag tagname="-8e+2"')
            tdSql.query(f"show tags from st_bool_20")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'alter table st_bool_21 set tag tagname="0x01"')
            tdSql.query(f"show tags from st_bool_21")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'alter table st_bool_22 set tag tagname="0b01"')
            tdSql.query(f"show tags from st_bool_22")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'alter table st_bool_23 set tag tagname="+0x01"')
            tdSql.query(f"show tags from st_bool_23")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'alter table st_bool_24 set tag tagname="-0b00"')
            tdSql.query(f"show tags from st_bool_24")
            tdSql.checkData(0, 5, "false")

            tdSql.execute(f'alter table st_bool_26 set tag tagname="-0.11e-30"')
            tdSql.query(f"show tags from st_bool_26")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f'alter table st_bool_27 set tag tagname="-1.0e-307"')
            tdSql.query(f"show tags from st_bool_27")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"alter table st_bool_28 set tag tagname= -1e-1 ")
            tdSql.query(f"show tags from st_bool_28")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"alter table st_bool_29 set tag tagname=-0.11e-30")
            tdSql.query(f"show tags from st_bool_29")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"alter table st_bool_30 set tag tagname=-1.1e-307")
            tdSql.query(f"show tags from st_bool_30")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"alter table st_bool_31 set tag tagname= 0x01")
            tdSql.query(f"show tags from st_bool_31")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"alter table st_bool_32 set tag tagname=0b01 ")
            tdSql.query(f"show tags from st_bool_32")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"alter table st_bool_33 set tag tagname=+0x01")
            tdSql.query(f"show tags from st_bool_33")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"alter table st_bool_34 set tag tagname= -0b00 ")
            tdSql.query(f"show tags from st_bool_34")
            tdSql.checkData(0, 5, "false")

    def bool_illegal_input(self):
            """illegal input

            1. 使用 bool 作为超级表的标签列
            2. 使用非法标签值创建子表

            Catalog:
                - DataTypes:Bool

            Since: v3.0.0.0

            Labels: common,ci,integration,functional

            Jira: None

            History:
                - 2025-4-28 Simon Guan Migrated to new test framework

            """

            tdLog.info(f"case 4: illegal input")

            tdSql.error(f"create table st_bool_e0 using mt_bool tags(123abc)")
            tdSql.error(f'create table st_bool_e1 using mt_bool tags("123abc")')
            tdSql.execute(f'create table st_bool_e2 using mt_bool tags("123")')
            tdSql.error(f"create table st_bool_e3 using mt_bool tags(abc)")
            tdSql.error(f'create table st_bool_e4 using mt_bool tags("abc")')
            tdSql.error(f'create table st_bool_e5 using mt_bool tags(" ")')
            tdSql.error(f"create table st_bool_e6 using mt_bool tags('')")

            tdSql.execute(f"create table st_bool_f0 using mt_bool tags(true)")
            tdSql.execute(f"create table st_bool_f1 using mt_bool tags(true)")
            tdSql.execute(f"create table st_bool_f2 using mt_bool tags(true)")
            tdSql.execute(f"create table st_bool_f3 using mt_bool tags(true)")
            tdSql.execute(f"create table st_bool_f4 using mt_bool tags(true)")
            tdSql.execute(f"create table st_bool_f5 using mt_bool tags(true)")
            tdSql.execute(f"create table st_bool_f6 using mt_bool tags(true)")

            tdSql.error(f"insert into st_bool_g0 values(now, 123abc)")
            tdSql.error(f'insert into st_bool_g1 values(now, "123abc")')
            tdSql.execute(f'insert into st_bool_f2 values(now, "123")')
            tdSql.error(f"insert into st_bool_g3 values(now, abc)")
            tdSql.error(f'insert into st_bool_g4 values(now, "abc")')
            tdSql.error(f'insert into st_bool_g5 values(now, " ")')
            tdSql.error(f"insert into st_bool_g6 values(now, '')")

            tdSql.error(f"insert into st_bool_h0 using mt_bool tags(123abc) values(now, 1)")
            tdSql.error(
                f'insert into st_bool_h1 using mt_bool tags("123abc") values(now, 1)'
            )
            tdSql.execute(
                f'insert into st_bool_h2 using mt_bool tags("123") values(now, 1)'
            )
            tdSql.error(f"insert into st_bool_h3 using mt_bool tags(abc) values(now, 1)")
            tdSql.error(f'insert into st_bool_h4 using mt_bool tags("abc") values(now, 1)')
            tdSql.error(f'insert into st_bool_h5 using mt_bool tags(" ") values(now, 1)')
            tdSql.error(f"insert into st_bool_h6 using mt_bool tags(" ") values(now, 1)")

            tdSql.error(f"insert into st_bool_h0 using mt_bool tags(1) values(now, 123abc)")
            tdSql.error(
                f'insert into st_bool_h1 using mt_bool tags(1) values(now, "123abc")'
            )
            tdSql.execute(
                f'insert into st_bool_h2 using mt_bool tags(1) values(now, "123")'
            )
            tdSql.error(f"insert into st_bool_h3 using mt_bool tags(1) values(now, abc)")
            tdSql.error(f'insert into st_bool_h4 using mt_bool tags(1) values(now, "abc")')
            tdSql.error(f'insert into st_bool_h5 using mt_bool tags(1) values(now, " ")')
            tdSql.error(f"insert into st_bool_h6 using mt_bool tags(1) values(now, '')")

            tdSql.execute(f"insert into st_bool_i0 using mt_bool tags(1) values(now, 1)")
            tdSql.execute(f"insert into st_bool_i1 using mt_bool tags(1) values(now, 1)")
            tdSql.execute(f"insert into st_bool_i2 using mt_bool tags(1) values(now, 1)")
            tdSql.execute(f"insert into st_bool_i3 using mt_bool tags(1) values(now, 1)")
            tdSql.execute(f"insert into st_bool_i4 using mt_bool tags(1) values(now, 1)")
            tdSql.execute(f"insert into st_bool_i5 using mt_bool tags(1) values(now, 1)")
            tdSql.execute(f"insert into st_bool_i6 using mt_bool tags(1) values(now, 1)")

            tdSql.error(f"alter table st_bool_i0 set tag tagname=123abc")
            tdSql.error(f'alter table st_bool_i1 set tag tagname="123abc"')
            tdSql.execute(f'alter table st_bool_i2 set tag tagname="123"')
            tdSql.error(f"alter table st_bool_i3 set tag tagname=abc")
            tdSql.error(f'alter table st_bool_i4 set tag tagname="abc"')
            tdSql.error(f"alter table st_bool_i4 set tag tagname=now")
            tdSql.error(f"alter table st_bool_i4 set tag tagname=now()+1d")
            tdSql.error(f"alter table st_bool_i4 set tag tagname=1+1d")
            tdSql.error(f'alter table st_bool_i5 set tag tagname=" "')
            tdSql.error(f"alter table st_bool_i6 set tag tagname=''")


    # ======================================================================
    # double: double column and tag values
    # ======================================================================

    def test_datatype_double(self):
            """DataTypes: double

            1. Create table
            2. Insert data
            3. Auto-create table
            4. Alter tag value
            5. Handle illegal input


            Since: v3.0.0.0

            Labels: common,ci,integration,functional

            Jira: None

            History:
                - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_double.sim

            """
            self.double_create_table()
            self.double_insert_data()
            self.double_auto_create_table()
            self.double_alter_tag_value()
            self.double_illegal_input()

    def double_create_table(self):
            tdLog.info(f"create super table")
            tdSql.execute(
                f"create table mt_double (ts timestamp, c double) tags(tagname double)"
            )

            tdLog.info(f"case 0: static create table for test tag values")

            tdSql.execute(f"create table st_double_0 using mt_double tags(NULL )")
            tdSql.query(f"show tags from st_double_0")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_double_1 using mt_double tags(NULL)")
            tdSql.query(f"show tags from st_double_1")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_double_2 using mt_double tags('NULL')")
            tdSql.query(f"show tags from st_double_2")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_double_3 using mt_double tags('NULL')")
            tdSql.query(f"show tags from st_double_3")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_double_4 using mt_double tags("NULL")')
            tdSql.query(f"show tags from st_double_4")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_double_5 using mt_double tags("NULL")')
            tdSql.query(f"show tags from st_double_5")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_double_6 using mt_double tags(-123.321)")
            tdSql.query(f"show tags from st_double_6")
            tdSql.checkData(0, 5, "-123.321000000")

            tdSql.execute(f"create table st_double_7 using mt_double tags(+1.567)")
            tdSql.query(f"show tags from st_double_7")
            tdSql.checkData(0, 5, "1.567000000")

            tdSql.execute(f"create table st_double_8 using mt_double tags(379.001)")
            tdSql.query(f"show tags from st_double_8")
            tdSql.checkData(0, 5, "379.001000000")

            tdSql.execute(f"create table st_double_9 using mt_double tags(1.5e+3)")
            tdSql.query(f"show tags from st_double_9")
            tdSql.checkData(0, 5, "1500.000000000")

            tdSql.execute(f"create table st_double_10 using mt_double tags(-1.5e-3)")
            tdSql.query(f"show tags from st_double_10")
            tdSql.checkData(0, 5, "-0.001500000")

            tdSql.execute(f"create table st_double_11 using mt_double tags(+1.5e+3)")
            tdSql.query(f"show tags from st_double_11")
            tdSql.checkData(0, 5, "1500.000000000")

            tdSql.execute(f"create table st_double_12 using mt_double tags(-1.5e+3)")
            tdSql.query(f"show tags from st_double_12")
            tdSql.checkData(0, 5, "-1500.000000000")

            tdSql.execute(f"create table st_double_13 using mt_double tags(1.5e-3)")
            tdSql.query(f"show tags from st_double_13")
            tdSql.checkData(0, 5, "0.001500000")

            tdSql.execute(f"create table st_double_14 using mt_double tags(1.5E-3)")
            tdSql.query(f"show tags from st_double_14")
            tdSql.checkData(0, 5, "0.001500000")

            tdSql.execute(f"create table st_double_6_0 using mt_double tags('-123.321')")
            tdSql.query(f"show tags from st_double_6_0")
            tdSql.checkData(0, 5, "-123.321000000")

            tdSql.execute(f"create table st_double_7_0 using mt_double tags('+1.567')")
            tdSql.query(f"show tags from st_double_7_0")
            tdSql.checkData(0, 5, "1.567000000")

            tdSql.execute(f"create table st_double_8_0 using mt_double tags('379.001')")
            tdSql.query(f"show tags from st_double_8_0")
            tdSql.checkData(0, 5, "379.001000000")

            tdSql.execute(f"create table st_double_9_0 using mt_double tags('1.5e+3')")
            tdSql.query(f"show tags from st_double_9_0")
            tdSql.checkData(0, 5, "1500.000000000")

            tdSql.execute(f"create table st_double_10_0 using mt_double tags('-1.5e-3')")
            tdSql.query(f"show tags from st_double_10_0")
            tdSql.checkData(0, 5, "-0.001500000")

            tdSql.execute(f"create table st_double_11_0 using mt_double tags('+1.5e+3')")
            tdSql.query(f"show tags from st_double_11_0")
            tdSql.checkData(0, 5, "1500.000000000")

            tdSql.execute(f"create table st_double_12_0 using mt_double tags('-1.5e+3')")
            tdSql.query(f"show tags from st_double_12_0")
            tdSql.checkData(0, 5, "-1500.000000000")

            tdSql.execute(f"create table st_double_13_0 using mt_double tags('1.5e-3')")
            tdSql.query(f"show tags from st_double_13_0")
            tdSql.checkData(0, 5, "0.001500000")

            tdSql.execute(f"create table st_double_14_0 using mt_double tags('1.5E-3')")
            tdSql.query(f"show tags from st_double_14_0")
            tdSql.checkData(0, 5, "0.001500000")

            tdSql.execute(
                f"create table st_double_15_0 using mt_double tags(1.7976931348623157e+308)"
            )
            tdSql.query(f"show tags from st_double_15_0")
            tdSql.checkData(
                0,
                5,
                "179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.000000000",
            )

            tdSql.execute(
                f"create table st_double_16_0 using mt_double tags(-1.7976931348623157e+308)"
            )
            tdSql.query(f"show tags from st_double_16_0")
            tdSql.checkData(
                0,
                5,
                "-179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.000000000",
            )

            tdSql.execute(f'create table st_double_100 using mt_double tags("0x01")')
            tdSql.query(f"show tags from st_double_100")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.execute(f'create table st_double_101 using mt_double tags("0b01")')
            tdSql.query(f"show tags from st_double_101")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.execute(f'create table st_double_102 using mt_double tags("+0x01")')
            tdSql.query(f"show tags from st_double_102")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.execute(f'create table st_double_103 using mt_double tags("-0b01")')
            tdSql.query(f"show tags from st_double_103")
            tdSql.checkData(0, 5, "-1.000000000")

            tdSql.execute(f"create table st_double_200 using mt_double tags( 0x01)")
            tdSql.query(f"show tags from st_double_200")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.execute(f"create table st_double_201 using mt_double tags(0b01 )")
            tdSql.query(f"show tags from st_double_201")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.execute(f"create table st_double_202 using mt_double tags(+0x01)")
            tdSql.query(f"show tags from st_double_202")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.execute(f"create table st_double_203 using mt_double tags( -0b01 )")
            tdSql.query(f"show tags from st_double_203")
            tdSql.checkData(0, 5, "-1.000000000")

    def double_insert_data(self):
            tdLog.info(f"case 1: insert values for test column values")

            tdSql.execute(f"insert into st_double_0 values(now, NULL )")
            tdSql.query(f"select * from st_double_0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_double_1 values(now, NULL)")
            tdSql.query(f"select * from st_double_1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_double_2 values(now, 'NULL')")
            tdSql.query(f"select * from st_double_2")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_double_3 values(now, 'NULL')")
            tdSql.query(f"select * from st_double_3")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f'insert into st_double_4 values(now, "NULL")')
            tdSql.query(f"select * from st_double_4")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f'insert into st_double_5 values(now, "NULL")')
            tdSql.query(f"select * from st_double_5")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_double_6 values(now, 1.7976931348623157e+308)")
            tdSql.query(f"select * from st_double_6")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1.7976931348623157e+308)

            tdSql.execute(f"insert into st_double_7 values(now, -1.7976931348623157e+308)")
            tdSql.query(f"select * from st_double_7")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -1.7976931348623157e+308)

            tdSql.execute(f"insert into st_double_8 values(now, +100.89)")
            tdSql.query(f"select * from st_double_8")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 100.89000000)

            tdSql.execute(f'insert into st_double_9 values(now, "-0.98")')
            tdSql.query(f"select * from st_double_9")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -0.980000000)

            tdSql.execute(f"insert into st_double_10 values(now, '0')")
            tdSql.query(f"select * from st_double_10")
            tdSql.checkRows(1)

            tdSql.execute(f"insert into st_double_11 values(now, -0)")
            tdSql.query(f"select * from st_double_11")
            tdSql.checkRows(1)

            tdSql.execute(f'insert into st_double_12 values(now, "+056")')
            tdSql.query(f"select * from st_double_12")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 56.000000)

            tdSql.execute(f"insert into st_double_13 values(now, +056)")
            tdSql.query(f"select * from st_double_13")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 56.000000000)

            tdSql.execute(f"insert into st_double_14 values(now, -056)")
            tdSql.query(f"select * from st_double_14")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -56)

            tdSql.execute(f'insert into  st_double_100 values(now, "0x01")')
            tdSql.query(f"select * from st_double_100")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1.000000000)

            tdSql.execute(f'insert into  st_double_101 values(now, "0b01")')
            tdSql.query(f"select * from st_double_101")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1.000000000)

            tdSql.execute(f'insert into  st_double_102 values(now, "+0x01")')
            tdSql.query(f"select * from st_double_102")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1.000000000)

            tdSql.execute(f'insert into  st_double_103 values(now, "-0b01")')
            tdSql.query(f"select * from st_double_103")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -1.000000000)

            tdSql.execute(f"insert into  st_double_200 values(now,  0x01)")
            tdSql.query(f"select * from st_double_200")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1.000000000)

            tdSql.execute(f"insert into  st_double_201 values(now, 0b01 )")
            tdSql.query(f"select * from st_double_201")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1.000000000)

            tdSql.execute(f"insert into  st_double_202 values(now, +0x01)")
            tdSql.query(f"select * from st_double_202")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1.000000000)

            tdSql.execute(f"insert into  st_double_203 values(now,  -0b01 )")
            tdSql.query(f"select * from st_double_203")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -1.000000000)

    def double_auto_create_table(self):
            tdLog.info(f"case 2: dynamic create table for test tag values")

            tdSql.execute(
                f"insert into st_double_16 using mt_double tags(NULL ) values(now, NULL )"
            )
            tdSql.query(f"show tags from st_double_16")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_double_16")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_double_17 using mt_double tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show tags from st_double_17")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_double_17")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_double_18 using mt_double tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_double_18")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_double_18")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_double_19 using mt_double tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_double_19")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_double_19")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_double_20 using mt_double tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_double_20")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_double_20")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_double_21 using mt_double tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_double_21")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_double_21")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_double_22 using mt_double tags(127) values(now, 1.7976931348623157e+308)"
            )
            tdSql.query(f"show tags from st_double_22")
            tdSql.checkData(0, 5, "127.000000000")

            tdSql.query(f"select * from st_double_22")
            tdSql.checkData(0, 1, 1.7976931348623157e+308)

            tdSql.execute(
                f"insert into st_double_23 using mt_double tags(-127) values(now, -1.7976931348623157e+308)"
            )
            tdSql.query(f"show tags from st_double_23")
            tdSql.checkData(0, 5, "-127.000000000")

            tdSql.query(f"select * from st_double_23")
            tdSql.checkData(0, 1, -1.7976931348623157e+308)

            tdSql.execute(
                f"insert into st_double_24 using mt_double tags(10) values(now, 10)"
            )
            tdSql.query(f"show tags from st_double_24")

            tdSql.query(f"select * from st_double_24")
            tdSql.checkData(0, 1, 10)

            tdSql.execute(
                f'insert into st_double_25 using mt_double tags("-0") values(now, "-0")'
            )
            tdSql.query(f"show tags from st_double_25")

            tdSql.query(f"select * from st_double_25")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_double_26 using mt_double tags('123') values(now, '12.3')"
            )
            tdSql.query(f"show tags from st_double_26")
            tdSql.checkData(0, 5, "123.000000000")

            tdSql.query(f"select * from st_double_26")
            tdSql.checkData(0, 1, 12.3 )

            tdSql.execute(
                f"insert into st_double_27 using mt_double tags(+056) values(now, +0005.6)"
            )
            tdSql.query(f"show tags from st_double_27")
            tdSql.checkData(0, 5, "56.000000000")

            tdSql.query(f"select * from st_double_27")
            tdSql.checkData(0, 1, 5.6)

            tdSql.execute(
                f"insert into st_double_28 using mt_double tags(-056) values(now, -005.6)"
            )
            tdSql.query(f"show tags from st_double_28")
            tdSql.checkData(0, 5, "-56.000000000")

            tdSql.query(f"select * from st_double_28")
            tdSql.checkData(0, 1, -5.6)

            tdSql.execute(
                f'insert into st_double_100 using mt_double tags("0x01") values(now, "0x01")'
            )
            tdSql.query(f"show tags from st_double_100")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.query(f"select * from st_double_100")
            tdSql.checkData(0, 1, 1.000000000)

            tdSql.execute(
                f'insert into st_double_101 using mt_double tags("0b01") values(now, "0b01")'
            )
            tdSql.query(f"show tags from st_double_101")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.query(f"select * from st_double_101")
            tdSql.checkData(0, 1, 1.000000000)

            tdSql.execute(
                f'insert into st_double_102 using mt_double tags("+0x01") values(now, "+0x01")'
            )
            tdSql.query(f"show tags from st_double_102")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.query(f"select * from st_double_102")
            tdSql.checkData(0, 1, 1.000000000)

            tdSql.execute(
                f'insert into st_double_103 using mt_double tags("-0b01") values(now, "-0b01")'
            )
            tdSql.query(f"show tags from st_double_103")
            tdSql.checkData(0, 5, "-1.000000000")

            tdSql.query(f"select * from st_double_103")
            tdSql.checkData(0, 1, -1.000000000)

            tdSql.execute(
                f"insert into st_double_200 using mt_double tags( 0x01) values(now, 0x01)"
            )
            tdSql.query(f"show tags from st_double_200")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.query(f"select * from st_double_200")
            tdSql.checkData(0, 1, 1.000000000)

            tdSql.execute(
                f"insert into st_double_201 using mt_double tags(0b01 ) values(now, 0b01)"
            )
            tdSql.query(f"show tags from st_double_201")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.query(f"select * from st_double_201")
            tdSql.checkData(0, 1, 1.000000000)

            tdSql.execute(
                f"insert into st_double_202 using mt_double tags(+0x01) values(now, +0x01)"
            )
            tdSql.query(f"show tags from st_double_202")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.query(f"select * from st_double_202")
            tdSql.checkData(0, 1, 1.000000000)

            tdSql.execute(
                f"insert into st_double_203 using mt_double tags( -0b01 ) values(now, -0b01)"
            )
            tdSql.query(f"show tags from st_double_203")
            tdSql.checkData(0, 5, "-1.000000000")

            tdSql.query(f"select * from st_double_203")
            tdSql.checkData(0, 1, -1.000000000)

    def double_alter_tag_value(self):
            tdLog.info(f"case 3: alter tag value")

            tdSql.execute(f'alter table st_double_100 set tag tagname="0x01"')
            tdSql.query(f"show tags from st_double_100")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.execute(f'alter table st_double_101 set tag tagname="0b01"')
            tdSql.query(f"show tags from st_double_101")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.execute(f'alter table st_double_102 set tag tagname="+0x01"')
            tdSql.query(f"show tags from st_double_102")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.execute(f'alter table st_double_103 set tag tagname="-0b01"')
            tdSql.query(f"show tags from st_double_103")
            tdSql.checkData(0, 5, "-1.000000000")

            tdSql.execute(f"alter table st_double_200 set tag tagname= 0x01")
            tdSql.query(f"show tags from st_double_200")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.execute(f"alter table st_double_201 set tag tagname=0b01 ")
            tdSql.query(f"show tags from st_double_201")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.execute(f"alter table st_double_202 set tag tagname=+0x01")
            tdSql.query(f"show tags from st_double_202")
            tdSql.checkData(0, 5, "1.000000000")

            tdSql.execute(f"alter table st_double_203 set tag tagname= -0b01 ")
            tdSql.query(f"show tags from st_double_203")
            tdSql.checkData(0, 5, "-1.000000000")

    def double_illegal_input(self):
            tdLog.info(f"case 4: illegal input")

            tdSql.error(
                f"create table st_double_e0 using mt_double tags(1.8976931348623157e+308)"
            )
            tdSql.error(
                f"create table st_double_e0 using mt_double tags(-1.8976931348623157e+308)"
            )
            tdSql.error(
                f"create table st_double_e0 using mt_double tags(31.7976931348623157e+308)"
            )
            tdSql.error(
                f"create table st_double_e0 using mt_double tags(-31.7976931348623157e+308)"
            )
            # truncate integer part
            tdSql.execute(f"create table st_double_e0 using mt_double tags(12.80)")
            tdSql.execute(f"drop table st_double_e0")
            tdSql.execute(f"create table st_double_e0 using mt_double tags(-11.80)")
            tdSql.execute(f"drop table st_double_e0")
            tdSql.error(f"create table st_double_e0 using mt_double tags(123abc)")
            tdSql.error(f'create table st_double_e0_1 using mt_double tags("123abc")')
            tdSql.error(f"create table st_double_e0 using mt_double tags(abc)")
            tdSql.error(f'create table st_double_e0_2 using mt_double tags("abc")')
            tdSql.error(f'create table st_double_e0_3 using mt_double tags(" ")')
            tdSql.error(f"create table st_double_e0_4 using mt_double tags('')")

            tdSql.execute(f"create table st_double_e0 using mt_double tags(123)")
            tdSql.execute(f"create table st_double_e1 using mt_double tags(123)")
            tdSql.execute(f"create table st_double_e2 using mt_double tags(123)")
            tdSql.execute(f"create table st_double_e3 using mt_double tags(123)")
            tdSql.execute(f"create table st_double_e4 using mt_double tags(123)")
            tdSql.execute(f"create table st_double_e5 using mt_double tags(123)")
            tdSql.execute(f"create table st_double_e6 using mt_double tags(123)")
            tdSql.execute(f"create table st_double_e7 using mt_double tags(123)")
            tdSql.execute(f"create table st_double_e8 using mt_double tags(123)")
            tdSql.execute(f"create table st_double_e9 using mt_double tags(123)")
            tdSql.execute(f"create table st_double_e10 using mt_double tags(123)")
            tdSql.execute(f"create table st_double_e11 using mt_double tags(123)")
            tdSql.execute(f"create table st_double_e12 using mt_double tags(123)")

            tdSql.error(f"insert into st_double_e0 values(now, 11.7976931348623157e+308)")
            tdSql.error(f"insert into st_double_e1 values(now, -11.7976931348623157e+308)")
            tdSql.error(f"insert into st_double_e2 values(now, 111.7976931348623157e+308)")
            tdSql.error(f"insert into st_double_e3 values(now, -111.7976931348623157e+308)")
            tdSql.execute(f"insert into st_double_e4 values(now, 12.80)")
            tdSql.execute(f"insert into st_double_e5 values(now, -11.80)")
            tdSql.error(f"insert into st_double_e6 values(now, 123abc)")
            tdSql.error(f'insert into st_double_e7 values(now, "123abc")')
            tdSql.error(f"insert into st_double_e9 values(now, abc)")
            tdSql.error(f'insert into st_double_e10 values(now, "abc")')
            tdSql.error(f'insert into st_double_e11 values(now, " ")')
            tdSql.error(f"insert into st_double_e12 values(now, '')")

            tdSql.error(
                f"insert into st_double_e13 using mt_double tags(033) values(now, 11.7976931348623157e+308)"
            )
            tdSql.error(
                f"insert into st_double_e14 using mt_double tags(033) values(now, -11.7976931348623157e+308)"
            )
            tdSql.error(
                f"insert into st_double_e15 using mt_double tags(033) values(now, 131.7976931348623157e+308)"
            )
            tdSql.error(
                f"insert into st_double_e16 using mt_double tags(033) values(now, -131.7976931348623157e+308)"
            )
            tdSql.execute(
                f"insert into st_double_e17 using mt_double tags(033) values(now, 12.80)"
            )
            tdSql.execute(
                f"insert into st_double_e18 using mt_double tags(033) values(now, -11.80)"
            )
            tdSql.error(
                f"insert into st_double_e19 using mt_double tags(033) values(now, 123abc)"
            )
            tdSql.error(
                f'insert into st_double_e20 using mt_double tags(033) values(now, "123abc")'
            )
            tdSql.error(
                f"insert into st_double_e22 using mt_double tags(033) values(now, abc)"
            )
            tdSql.error(
                f'insert into st_double_e23 using mt_double tags(033) values(now, "abc")'
            )
            tdSql.error(
                f'insert into st_double_e24 using mt_double tags(033) values(now, " ")'
            )
            tdSql.error(
                f"insert into st_double_e25_1 using mt_double tags(033) values(now, '')"
            )

            tdSql.error(
                f"insert into st_double_e13 using mt_double tags(31.7976931348623157e+308) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_double_e14 using mt_double tags(-31.7976931348623157e+308) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_double_e15 using mt_double tags(131.7976931348623157e+308) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_double_e16 using mt_double tags(-131.7976931348623157e+308) values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_double_e17 using mt_double tags(12.80) values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_double_e18 using mt_double tags(-11.80) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_double_e19 using mt_double tags(123abc) values(now, -033)"
            )
            tdSql.error(
                f'insert into st_double_e20 using mt_double tags("123abc") values(now, -033)'
            )
            tdSql.error(
                f"insert into st_double_e22 using mt_double tags(abc) values(now, -033)"
            )
            tdSql.error(
                f'insert into st_double_e23 using mt_double tags("abc") values(now, -033)'
            )
            tdSql.error(
                f'insert into st_double_e24 using mt_double tags(" ") values(now, -033)'
            )
            tdSql.error(
                f"insert into st_double_e25 using mt_double tags(" ") values(now, -033)"
            )
            tdSql.execute(
                f'insert into st_double_e20 using mt_double tags("123") values(now, -033)'
            )

            tdSql.execute(
                f"insert into st_double_e13 using mt_double tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_double_e14 using mt_double tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_double_e15 using mt_double tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_double_e16 using mt_double tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_double_e17 using mt_double tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_double_e18 using mt_double tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_double_e19 using mt_double tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_double_e20 using mt_double tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_double_e21 using mt_double tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_double_e22 using mt_double tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_double_e23 using mt_double tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_double_e24 using mt_double tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_double_e25 using mt_double tags(033) values(now, 00062)"
            )

            tdSql.error(
                f"alter table st_double_e13 set tag tagname=1.8976931348623157e+308"
            )
            tdSql.error(
                f"alter table st_double_e14 set tag tagname=-1.8976931348623157e+308"
            )
            tdSql.error(
                f"alter table st_double_e15 set tag tagname=131.7976931348623157e+308"
            )
            tdSql.error(
                f"alter table st_double_e16 set tag tagname=-131.7976931348623157e+308"
            )
            tdSql.error(f"alter table st_double_e19 set tag tagname=123abc")
            tdSql.error(f'alter table st_double_e20 set tag tagname="123abc"')
            tdSql.error(f"alter table st_double_e22 set tag tagname=abc")
            tdSql.error(f'alter table st_double_e23 set tag tagname="abc"')
            tdSql.error(f'alter table st_double_e24 set tag tagname=" "')
            tdSql.error(f"alter table st_double_e25 set tag tagname=''")
            tdSql.execute(f"alter table st_double_e25 set tag tagname='123'")


    # ======================================================================
    # float: float column and tag values
    # ======================================================================

    def test_datatype_float(self):
            """DataTypes: float

            1. Create table
            2. Insert data
            3. Auto-create table
            4. Alter tag value
            5. Handle illegal input


            Since: v3.0.0.0

            Labels: common,ci,integration,functional

            Jira: None

            History:
                - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_float.sim

            """
            self.float_create_table()
            self.float_insert_data()
            self.float_auto_create_table()
            self.float_alter_tag_value()
            self.float_illegal_input()

    def float_create_table(self):
            tdLog.info(f"create super table")
            tdSql.execute(
                f"create table mt_float (ts timestamp, c float) tags(tagname float)"
            )

            tdLog.info(f"case 0: static create table for test tag values")

            tdSql.execute(f"create table st_float_0 using mt_float tags(NULL)")
            tdSql.query(f"show create table st_float_0")
            tdSql.query(f"show tags from st_float_0")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_float_1 using mt_float tags(NULL)")
            tdSql.query(f"show tags from st_float_1")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_float_2 using mt_float tags('NULL')")
            tdSql.query(f"show tags from st_float_2")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_float_3 using mt_float tags('NULL')")
            tdSql.query(f"show tags from st_float_3")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_float_4 using mt_float tags("NULL")')
            tdSql.query(f"show tags from st_float_4")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_float_5 using mt_float tags("NULL")')
            tdSql.query(f"show tags from st_float_5")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_float_6 using mt_float tags(-123.321)")
            tdSql.query(f"show tags from st_float_6")
            tdSql.checkData(0, 5, "-123.32100")

            tdSql.execute(f"create table st_float_7 using mt_float tags(+1.567)")
            tdSql.query(f"show tags from st_float_7")
            tdSql.checkData(0, 5, "1.56700")

            tdSql.execute(f"create table st_float_8 using mt_float tags(379.001)")
            tdSql.query(f"show tags from st_float_8")
            tdSql.checkData(0, 5, "379.00101")

            tdSql.execute(f"create table st_float_9 using mt_float tags(1.5e+3)")
            tdSql.query(f"show tags from st_float_9")
            tdSql.checkData(0, 5, "1500.00000")

            tdSql.execute(f"create table st_float_10 using mt_float tags(-1.5e-3)")
            tdSql.query(f"show tags from st_float_10")
            tdSql.checkData(0, 5, "-0.00150")

            tdSql.execute(f"create table st_float_11 using mt_float tags(+1.5e+3)")
            tdSql.query(f"show tags from st_float_11")
            tdSql.checkData(0, 5, "1500.00000")

            tdSql.execute(f"create table st_float_12 using mt_float tags(-1.5e+3)")
            tdSql.query(f"show tags from st_float_12")
            tdSql.checkData(0, 5, "-1500.00000")

            tdSql.execute(f"create table st_float_13 using mt_float tags(1.5e-3)")
            tdSql.query(f"show tags from st_float_13")
            tdSql.checkData(0, 5, "0.00150")

            tdSql.execute(f"create table st_float_14 using mt_float tags(1.5E-3)")
            tdSql.query(f"show tags from st_float_14")
            tdSql.checkData(0, 5, "0.00150")

            tdSql.execute(f"create table st_float_6_0 using mt_float tags('-123.321')")
            tdSql.query(f"show tags from st_float_6_0")
            tdSql.checkData(0, 5, "-123.32100")

            tdSql.execute(f"create table st_float_7_0 using mt_float tags('+1.567')")
            tdSql.query(f"show tags from st_float_7_0")
            tdSql.checkData(0, 5, "1.56700")

            tdSql.execute(f"create table st_float_8_0 using mt_float tags('379.001')")
            tdSql.query(f"show tags from st_float_8_0")
            tdSql.checkData(0, 5, "379.00101")

            tdSql.execute(f"create table st_float_9_0 using mt_float tags('1.5e+3')")
            tdSql.query(f"show tags from st_float_9_0")
            tdSql.checkData(0, 5, "1500.00000")

            tdSql.execute(f"create table st_float_10_0 using mt_float tags('-1.5e-3')")
            tdSql.query(f"show tags from st_float_10_0")
            tdSql.checkData(0, 5, "-0.00150")

            tdSql.execute(f"create table st_float_11_0 using mt_float tags('+1.5e+3')")
            tdSql.query(f"show tags from st_float_11_0")
            tdSql.checkData(0, 5, "1500.00000")

            tdSql.execute(f"create table st_float_12_0 using mt_float tags('-1.5e+3')")
            tdSql.query(f"show tags from st_float_12_0")
            tdSql.checkData(0, 5, "-1500.00000")

            tdSql.execute(f"create table st_float_13_0 using mt_float tags('1.5e-3')")
            tdSql.query(f"show tags from st_float_13_0")
            tdSql.checkData(0, 5, "0.00150")

            tdSql.execute(f"create table st_float_14_0 using mt_float tags('1.5E-3')")
            tdSql.query(f"show tags from st_float_14_0")
            tdSql.checkData(0, 5, "0.00150")

            tdSql.execute(
                f"create table st_float_15_0 using mt_float tags(3.40282346638528859811704183484516925e+38)"
            )
            tdSql.query(f"show tags from st_float_15_0")
            tdSql.execute(
                f"create table st_float_16_0 using mt_float tags(-3.40282346638528859811704183484516925e+38)"
            )
            tdSql.query(f"show tags from st_float_16_0")

            tdSql.execute(f'create table st_float_100 using mt_float tags("0x01")')
            tdSql.query(f"show tags from st_float_100")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.execute(f'create table st_float_101 using mt_float tags("0b01")')
            tdSql.query(f"show tags from st_float_101")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.execute(f'create table st_float_102 using mt_float tags("+0x01")')
            tdSql.query(f"show tags from st_float_102")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.execute(f'create table st_float_103 using mt_float tags("-0b01")')
            tdSql.query(f"show tags from st_float_103")
            tdSql.checkData(0, 5, "-1.00000")

            tdSql.execute(f"create table st_float_200 using mt_float tags( 0x01)")
            tdSql.query(f"show tags from st_float_200")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.execute(f"create table st_float_201 using mt_float tags(0b01 )")
            tdSql.query(f"show tags from st_float_201")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.execute(f"create table st_float_202 using mt_float tags(+0x01)")
            tdSql.query(f"show tags from st_float_202")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.execute(f"create table st_float_203 using mt_float tags( -0b01 )")
            tdSql.query(f"show tags from st_float_203")
            tdSql.checkData(0, 5, "-1.00000")

    def float_insert_data(self):
            tdLog.info(f"case 1: insert values for test column values")

            tdSql.execute(f"insert into st_float_0 values(now, NULL)")
            tdSql.query(f"select * from st_float_0")
            tdSql.checkRows(1)

            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_float_1 values(now, NULL)")
            tdSql.query(f"select * from st_float_1")
            tdSql.checkRows(1)

            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_float_2 values(now, 'NULL')")
            tdSql.query(f"select * from st_float_2")
            tdSql.checkRows(1)

            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_float_3 values(now, 'NULL')")
            tdSql.query(f"select * from st_float_3")
            tdSql.checkRows(1)

            tdSql.checkData(0, 1, None)

            tdSql.execute(f'insert into st_float_4 values(now, "NULL")')
            tdSql.query(f"select * from st_float_4")
            tdSql.checkRows(1)

            tdSql.checkData(0, 1, None)

            tdSql.execute(f'insert into st_float_5 values(now, "NULL")')
            tdSql.query(f"select * from st_float_5")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f'insert into  st_float_100 values(now, "0x01")')
            tdSql.query(f"select * from st_float_100")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1.00000)

            tdSql.execute(f'insert into  st_float_101 values(now, "0b01")')
            tdSql.query(f"select * from st_float_101")
            tdSql.checkRows(1)

            tdSql.checkData(0, 1, 1.00000)

            tdSql.execute(f'insert into  st_float_102 values(now, "+0x01")')
            tdSql.query(f"select * from st_float_102")
            tdSql.checkRows(1)

            tdSql.checkData(0, 1, 1.00000)

            tdSql.execute(f'insert into  st_float_103 values(now, "-0b01")')
            tdSql.query(f"select * from st_float_103")
            tdSql.checkRows(1)

            tdSql.checkData(0, 1, -1.00000)

            tdSql.execute(f"insert into  st_float_200 values(now,  0x01)")
            tdSql.query(f"select * from st_float_200")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1.00000)

            tdSql.execute(f"insert into  st_float_201 values(now, 0b01 )")
            tdSql.query(f"select * from st_float_201")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1.00000)

            tdSql.execute(f"insert into  st_float_202 values(now, +0x01)")
            tdSql.query(f"select * from st_float_202")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1.00000)

            tdSql.execute(f"insert into  st_float_203 values(now,  -0b01 )")
            tdSql.query(f"select * from st_float_203")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -1.00000)

            tdSql.error(f"insert into st_float_6 values(now, 3.40282347e+38)")
            tdSql.error(f"insert into st_float_6 values(now, -3.40282347e+38)")

            tdSql.execute(
                f"insert into st_float_6 values(now, 340282346638528859811704183484516925440.00000)"
            )
            tdSql.query(f"select * from st_float_6")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 340282346638528859811704183484516925440.00000)

            tdSql.execute(
                f"insert into st_float_7 values(now, -340282346638528859811704183484516925440.00000)"
            )
            tdSql.query(f"select * from st_float_7")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -340282346638528859811704183484516925440.00000)

            tdSql.execute(f"insert into st_float_8 values(now, +100.89)")
            tdSql.query(f"select * from st_float_8")
            tdSql.checkRows(1)

            tdSql.execute(f'insert into st_float_9 values(now, "-0.98")')
            tdSql.query(f"select * from st_float_9")
            tdSql.checkRows(1)

            tdSql.execute(f"insert into st_float_10 values(now, '0')")
            tdSql.query(f"select * from st_float_10")
            tdSql.checkRows(1)

            tdSql.execute(f"insert into st_float_11 values(now, -0)")
            tdSql.query(f"select * from st_float_11")
            tdSql.checkRows(1)

            tdSql.execute(f'insert into st_float_12 values(now, "+056")')
            tdSql.query(f"select * from st_float_12")
            tdSql.checkRows(1)

            tdSql.execute(f"insert into st_float_13 values(now, +056)")
            tdSql.query(f"select * from st_float_13")
            tdSql.checkRows(1)

            tdSql.execute(f"insert into st_float_14 values(now, -056)")
            tdSql.query(f"select * from st_float_14")
            tdSql.checkRows(1)

    def float_auto_create_table(self):
            tdLog.info(f"case 2: dynamic create table for test tag values")

            tdSql.execute(
                f"insert into st_float_16 using mt_float tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show create table st_float_16")
            tdSql.query(f"show tags from st_float_16")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_float_16")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_float_17 using mt_float tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show tags from st_float_17")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_float_17")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_float_18 using mt_float tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_float_18")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_float_18")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_float_19 using mt_float tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_float_19")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_float_19")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_float_20 using mt_float tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_float_20")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_float_20")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_float_21 using mt_float tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_float_21")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_float_21")
            tdSql.checkData(0, 1, None)

            tdSql.error(
                f" insert into st_float_22 using mt_float tags(127) values(now, 3.40282347e+38)"
            )

            tdSql.execute(
                f"insert into st_float_22 using mt_float tags(127) values(now, 340282346638528859811704183484516925440.00000)"
            )
            tdSql.query(f"show tags from st_float_22")
            tdSql.checkData(0, 5, "127.00000")

            tdSql.query(f"select tbname, tagname from st_float_22")
            tdSql.checkData(0, 1, 127.00000)

            tdSql.execute(
                f"insert into st_float_23 using mt_float tags(-127) values(now, -340282346638528859811704183484516925440.00000)"
            )
            tdSql.query(f"show tags from st_float_23")
            tdSql.checkData(0, 5, "-127.00000")

            tdSql.execute(
                f"insert into st_float_24 using mt_float tags(10) values(now, 10)"
            )
            tdSql.query(f"show tags from st_float_24")
            tdSql.checkData(0, 5, "10.00000")

            tdSql.query(f"select * from st_float_24")
            tdSql.checkData(0, 1, 10.00000)

            tdSql.execute(
                f'insert into st_float_25 using mt_float tags("-0") values(now, "-0")'
            )
            tdSql.query(f"show tags from st_float_25")
            tdSql.checkData(0, 5, "-0.00000")

            tdSql.query(f"select * from st_float_25")
            tdSql.checkData(0, 1, -0.00000)

            tdSql.execute(
                f"insert into st_float_26 using mt_float tags('123') values(now, '12.3')"
            )
            tdSql.query(f"show tags from st_float_26")
            tdSql.checkData(0, 5, "123.00000")

            tdSql.query(f"select * from st_float_26")
            tdSql.checkData(0, 1, 12.30000)

            tdSql.execute(
                f"insert into st_float_27 using mt_float tags(+056) values(now, +0005.6)"
            )
            tdSql.query(f"show tags from st_float_27")
            tdSql.checkData(0, 5, "56.00000")

            tdSql.query(f"select * from st_float_27")
            tdSql.checkData(0, 1, 5.60000)

            tdSql.execute(
                f"insert into st_float_28 using mt_float tags(-056) values(now, -005.6)"
            )
            tdSql.query(f"show tags from st_float_28")
            tdSql.checkData(0, 5, "-56.00000")

            tdSql.query(f"select * from st_float_28")
            tdSql.checkData(0, 1, -5.60000)

            tdSql.execute(
                f'insert into st_float_100 using mt_float tags("0x01") values(now, "0x01")'
            )
            tdSql.query(f"show tags from st_float_100")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.query(f"select * from st_float_100")
            tdSql.checkData(0, 1, 1.00000)

            tdSql.execute(
                f'insert into st_float_101 using mt_float tags("0b01") values(now, "0b01")'
            )
            tdSql.query(f"show tags from st_float_101")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.query(f"select * from st_float_101")
            tdSql.checkData(0, 1, 1.00000)

            tdSql.execute(
                f'insert into st_float_102 using mt_float tags("+0x01") values(now, "+0x01")'
            )
            tdSql.query(f"show tags from st_float_102")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.query(f"select * from st_float_102")
            tdSql.checkData(0, 1, 1.00000)

            tdSql.execute(
                f'insert into st_float_103 using mt_float tags("-0b01") values(now, "-0b01")'
            )
            tdSql.query(f"show tags from st_float_103")
            tdSql.checkData(0, 5, "-1.00000")

            tdSql.query(f"select * from st_float_103")
            tdSql.checkData(0, 1, -1.00000)

            tdSql.execute(
                f"insert into st_float_200 using mt_float tags( 0x01) values(now, 0x01)"
            )
            tdSql.query(f"show tags from st_float_200")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.query(f"select * from st_float_200")
            tdSql.checkData(0, 1, 1.00000)

            tdSql.execute(
                f"insert into st_float_201 using mt_float tags(0b01 ) values(now, 0b01)"
            )
            tdSql.query(f"show tags from st_float_201")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.query(f"select * from st_float_201")
            tdSql.checkData(0, 1, 1.00000)

            tdSql.execute(
                f"insert into st_float_202 using mt_float tags(+0x01) values(now, +0x01)"
            )
            tdSql.query(f"show tags from st_float_202")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.query(f"select * from st_float_202")
            tdSql.checkData(0, 1, 1.00000)

            tdSql.execute(
                f"insert into st_float_203 using mt_float tags( -0b01 ) values(now, -0b01)"
            )
            tdSql.query(f"show tags from st_float_203")
            tdSql.checkData(0, 5, "-1.00000")

            tdSql.query(f"select * from st_float_203")
            tdSql.checkData(0, 1, -1.00000)

    def float_alter_tag_value(self):
            tdLog.info(f"case 3: alter tag value")

            tdSql.execute(
                f"alter table st_float_0 set tag tagname=340282346638528859811704183484516925440.00000"
            )
            tdSql.query(f"show tags from st_float_0")
            tdSql.checkData(0, 5, "340282346638528859811704183484516925440.00000")

            tdSql.execute(
                f"alter table st_float_0 set tag tagname=-340282346638528859811704183484516925440.00000"
            )
            tdSql.query(f"show tags from st_float_0")
            tdSql.checkData(0, 5, "-340282346638528859811704183484516925440.00000")

            tdSql.execute(f"alter table st_float_0 set tag tagname=+10.340")
            tdSql.query(f"show tags from st_float_0")
            tdSql.checkData(0, 5, "10.34000")

            tdSql.execute(f"alter table st_float_0 set tag tagname=-33.87")
            tdSql.query(f"show tags from st_float_0")
            tdSql.checkData(0, 5, "-33.87000")

            tdSql.execute(f"alter table st_float_0 set tag tagname='+9.8'")
            tdSql.query(f"show tags from st_float_0")
            tdSql.checkData(0, 5, "9.80000")

            tdSql.execute(f"alter table st_float_0 set tag tagname='-07.6'")
            tdSql.query(f"show tags from st_float_0")
            tdSql.checkData(0, 5, "-7.60000")

            tdSql.execute(f"alter table st_float_0 set tag tagname=+0012.871")
            tdSql.query(f"show tags from st_float_0")
            tdSql.checkData(0, 5, "12.87100")

            tdSql.execute(f"alter table st_float_0 set tag tagname=-00063.582")
            tdSql.query(f"show tags from st_float_0")
            tdSql.checkData(0, 5, "-63.58200")

            tdSql.execute(f'alter table st_float_100 set tag tagname="0x01"')
            tdSql.query(f"show tags from st_float_100")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.execute(f'alter table st_float_101 set tag tagname="0b01"')
            tdSql.query(f"show tags from st_float_101")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.execute(f'alter table st_float_102 set tag tagname="+0x01"')
            tdSql.query(f"show tags from st_float_102")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.execute(f'alter table st_float_103 set tag tagname="-0b01"')
            tdSql.query(f"show tags from st_float_103")
            tdSql.checkData(0, 5, "-1.00000")

            tdSql.execute(f"alter table st_float_200 set tag tagname= 0x01")
            tdSql.query(f"show tags from st_float_200")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.execute(f"alter table st_float_201 set tag tagname=0b01 ")
            tdSql.query(f"show tags from st_float_201")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.execute(f"alter table st_float_202 set tag tagname=+0x01")
            tdSql.query(f"show tags from st_float_202")
            tdSql.checkData(0, 5, "1.00000")

            tdSql.execute(f"alter table st_float_203 set tag tagname= -0b01 ")
            tdSql.query(f"show tags from st_float_203")
            tdSql.checkData(0, 5, "-1.00000")

    def float_illegal_input(self):
            tdLog.info(f"case 4: illegal input")

            tdSql.error(f"create table st_float_e0 using mt_float tags(3.50282347e+38)")
            tdSql.error(f"create table st_float_e0 using mt_float tags(-3.50282347e+38)")
            tdSql.error(f"create table st_float_e0 using mt_float tags(333.40282347e+38)")
            tdSql.error(f"create table st_float_e0 using mt_float tags(-333.40282347e+38)")
            # truncate integer part
            tdSql.execute(f"create table st_float_e0 using mt_float tags(12.80)")
            tdSql.execute(f"drop table st_float_e0")
            tdSql.execute(f"create table st_float_e0 using mt_float tags(-11.80)")
            tdSql.execute(f"drop table st_float_e0")
            tdSql.error(f"create table st_float_e0 using mt_float tags(123abc)")
            tdSql.error(f'create table st_float_e0_1 using mt_float tags("123abc")')
            tdSql.error(f"create table st_float_e0 using mt_float tags(abc)")
            tdSql.error(f'create table st_float_e0_2 using mt_float tags("abc")')
            tdSql.error(f'create table st_float_e0_3 using mt_float tags(" ")')
            tdSql.error(f"create table st_float_e0_4 using mt_float tags('')")

            tdSql.execute(f"create table st_float_e0 using mt_float tags(123)")
            tdSql.execute(f"create table st_float_e1 using mt_float tags(123)")
            tdSql.execute(f"create table st_float_e2 using mt_float tags(123)")
            tdSql.execute(f"create table st_float_e3 using mt_float tags(123)")
            tdSql.execute(f"create table st_float_e4 using mt_float tags(123)")
            tdSql.execute(f"create table st_float_e5 using mt_float tags(123)")
            tdSql.execute(f"create table st_float_e6 using mt_float tags(123)")
            tdSql.execute(f"create table st_float_e7 using mt_float tags(123)")
            tdSql.execute(f"create table st_float_e8 using mt_float tags(123)")
            tdSql.execute(f"create table st_float_e9 using mt_float tags(123)")
            tdSql.execute(f"create table st_float_e10 using mt_float tags(123)")
            tdSql.execute(f"create table st_float_e11 using mt_float tags(123)")
            tdSql.execute(f"create table st_float_e12 using mt_float tags(123)")

            tdSql.error(f"insert into st_float_e0 values(now, 3.50282347e+38)")
            tdSql.error(f"insert into st_float_e1 values(now, -3.50282347e+38)")
            tdSql.error(f"insert into st_float_e2 values(now, 13.40282347e+38)")
            tdSql.error(f"insert into st_float_e3 values(now, -13.40282347e+38)")
            tdSql.execute(f"insert into st_float_e4 values(now, 12.80)")
            tdSql.execute(f"insert into st_float_e5 values(now, -11.80)")
            tdSql.error(f"insert into st_float_e6 values(now, 123abc)")
            tdSql.error(f'insert into st_float_e7 values(now, "123abc")')
            tdSql.error(f"insert into st_float_e9 values(now, abc)")
            tdSql.error(f'insert into st_float_e10 values(now, "abc")')
            tdSql.error(f'insert into st_float_e11 values(now, " ")')
            tdSql.error(f"insert into st_float_e12 values(now, '')")

            tdSql.error(
                f"insert into st_float_e13 using mt_float tags(033) values(now, 3.50282347e+38)"
            )
            tdSql.error(
                f"insert into st_float_e14 using mt_float tags(033) values(now, -3.50282347e+38)"
            )
            tdSql.error(
                f"insert into st_float_e15 using mt_float tags(033) values(now, 13.40282347e+38)"
            )
            tdSql.error(
                f"insert into st_float_e16 using mt_float tags(033) values(now, -13.40282347e+38)"
            )
            tdSql.execute(
                f"insert into st_float_e17 using mt_float tags(033) values(now, 12.80)"
            )
            tdSql.execute(
                f"insert into st_float_e18 using mt_float tags(033) values(now, -11.80)"
            )
            tdSql.error(
                f"insert into st_float_e19 using mt_float tags(033) values(now, 123abc)"
            )
            tdSql.error(
                f'insert into st_float_e20 using mt_float tags(033) values(now, "123abc")'
            )
            tdSql.error(
                f"insert into st_float_e22 using mt_float tags(033) values(now, abc)"
            )
            tdSql.error(
                f'insert into st_float_e23 using mt_float tags(033) values(now, "abc")'
            )
            tdSql.error(
                f'insert into st_float_e24 using mt_float tags(033) values(now, " ")'
            )
            tdSql.error(
                f"insert into st_float_e25_1 using mt_float tags(033) values(now, '')"
            )

            tdSql.error(
                f"insert into st_float_e13 using mt_float tags(3.50282347e+38) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_float_e14 using mt_float tags(-3.50282347e+38) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_float_e15 using mt_float tags(13.40282347e+38) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_float_e16 using mt_float tags(-13.40282347e+38) values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_float_e17 using mt_float tags(12.80) values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_float_e18 using mt_float tags(-11.80) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_float_e19 using mt_float tags(123abc) values(now, -033)"
            )
            tdSql.error(
                f'insert into st_float_e20 using mt_float tags("123abc") values(now, -033)'
            )
            tdSql.error(
                f"insert into st_float_e22 using mt_float tags(abc) values(now, -033)"
            )
            tdSql.error(
                f'insert into st_float_e23 using mt_float tags("abc") values(now, -033)'
            )
            tdSql.error(
                f'insert into st_float_e24 using mt_float tags(" ") values(now, -033)'
            )
            tdSql.error(
                f"insert into st_float_e25_3 using mt_float tags(" ") values(now, -033)"
            )

            tdSql.execute(
                f"insert into st_float_e13 using mt_float tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_float_e14 using mt_float tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_float_e15 using mt_float tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_float_e16 using mt_float tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_float_e17 using mt_float tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_float_e18 using mt_float tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_float_e19 using mt_float tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_float_e20 using mt_float tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_float_e21 using mt_float tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_float_e22 using mt_float tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_float_e23 using mt_float tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_float_e24 using mt_float tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_float_e25 using mt_float tags(033) values(now, 00062)"
            )

            tdSql.error(f"alter table st_float_e13 set tag tagname=3.50282347e+38")
            tdSql.error(f"alter table st_float_e14 set tag tagname=-3.50282347e+38")
            tdSql.error(f"alter table st_float_e15 set tag tagname=13.40282347e+38")
            tdSql.error(f"alter table st_float_e16 set tag tagname=-13.40282347e+38")
            tdSql.error(f"alter table st_float_e19 set tag tagname=123abc")
            tdSql.error(f'alter table st_float_e20 set tag tagname="123abc"')
            tdSql.error(f"alter table st_float_e22 set tag tagname=abc")
            tdSql.error(f'alter table st_float_e23 set tag tagname="abc"')
            tdSql.error(f'alter table st_float_e24 set tag tagname=" "')
            tdSql.error(f"alter table st_float_e25 set tag tagname=''")


    # ======================================================================
    # int: int column and tag values
    # ======================================================================

    def test_datatype_int(self):
            """DataTypes: int

            1. Create table
            2. Insert data
            3. Auto-create table
            4. Alter tag value
            5. Handle illegal input


            Since: v3.0.0.0

            Labels: common,ci,integration,functional

            Jira: None

            History:
                - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_int.sim

            """
            self.int_create_table()
            self.int_insert_data()
            self.int_auto_create_table()
            self.int_alter_tag_value()
            self.int_illegal_input()

    def int_create_table(self):
            tdLog.info(f"create super table")
            tdSql.execute(f"create table mt_int (ts timestamp, c int) tags(tagname int)")

            tdLog.info(f"case 0: static create table for test tag values")

            tdSql.execute(f"create table st_int_0 using mt_int tags(NULL)")
            tdSql.query(f"show create table st_int_0")
            tdSql.query(f"show tags from st_int_0")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_int_1 using mt_int tags(NULL)")
            tdSql.query(f"show tags from st_int_1")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_int_2 using mt_int tags('NULL')")
            tdSql.query(f"show tags from st_int_2")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_int_3 using mt_int tags('NULL')")
            tdSql.query(f"show tags from st_int_3")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_int_4 using mt_int tags("NULL")')
            tdSql.query(f"show tags from st_int_4")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_int_5 using mt_int tags("NULL")')
            tdSql.query(f"show tags from st_int_5")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_int_6 using mt_int tags(-2147483647)")
            tdSql.query(f"show tags from st_int_6")
            tdSql.checkData(0, 5, -2147483647)

            tdSql.execute(f"create table st_int_7 using mt_int tags(2147483647)")
            tdSql.query(f"show tags from st_int_7")
            tdSql.checkData(0, 5, 2147483647)

            tdSql.execute(f"create table st_int_8 using mt_int tags(37)")
            tdSql.query(f"show tags from st_int_8")
            tdSql.checkData(0, 5, 37)

            tdSql.execute(f"create table st_int_9 using mt_int tags(-100)")
            tdSql.query(f"show tags from st_int_9")
            tdSql.checkData(0, 5, -100)

            tdSql.execute(f"create table st_int_10 using mt_int tags(+113)")
            tdSql.query(f"show tags from st_int_10")
            tdSql.checkData(0, 5, 113)

            tdSql.execute(f"create table st_int_11 using mt_int tags('-100')")
            tdSql.query(f"show tags from st_int_11")
            tdSql.checkData(0, 5, -100)

            tdSql.execute(f'create table st_int_12 using mt_int tags("+78")')
            tdSql.query(f"show tags from st_int_12")
            tdSql.checkData(0, 5, 78)

            tdSql.execute(f"create table st_int_13 using mt_int tags(+0078)")
            tdSql.query(f"show tags from st_int_13")
            tdSql.checkData(0, 5, 78)

            tdSql.execute(f"create table st_int_14 using mt_int tags(-00078)")
            tdSql.query(f"show tags from st_int_14")
            tdSql.checkData(0, 5, -78)

            tdSql.execute(f'create table st_int_100 using mt_int tags("0x01")')
            tdSql.query(f"show tags from st_int_100")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'create table st_int_101 using mt_int tags("0b01")')
            tdSql.query(f"show tags from st_int_101")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'create table st_int_102 using mt_int tags("+0x01")')
            tdSql.query(f"show tags from st_int_102")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'create table st_int_103 using mt_int tags("-0b01")')
            tdSql.query(f"show tags from st_int_103")
            tdSql.checkData(0, 5, -1)

            tdSql.execute(f'create table st_int_104 using mt_int tags("-123.1")')
            tdSql.query(f"show tags from st_int_104")
            tdSql.checkData(0, 5, -123)

            tdSql.execute(f'create table st_int_105 using mt_int tags("+123.5")')
            tdSql.query(f"show tags from st_int_105")
            tdSql.checkData(0, 5, 124)

            tdSql.execute(f'create table st_int_106 using mt_int tags("-1e-1")')
            tdSql.query(f"show tags from st_int_106")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f'create table st_int_107 using mt_int tags("+0.1235e3")')
            tdSql.query(f"show tags from st_int_107")
            tdSql.checkData(0, 5, 124)

            tdSql.execute(f'create table st_int_108 using mt_int tags("-0.11e-30")')
            tdSql.query(f"show tags from st_int_108")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f'create table st_int_109 using mt_int tags("-1.1e-307")')
            tdSql.query(f"show tags from st_int_109")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"create table st_int_110 using mt_int tags( -1e-1 )")
            tdSql.query(f"show tags from st_int_110")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"create table st_int_111 using mt_int tags( +0.1235e3 )")
            tdSql.query(f"show tags from st_int_111")
            tdSql.checkData(0, 5, 124)

            tdSql.execute(f"create table st_int_112 using mt_int tags(-0.11e-30)")
            tdSql.query(f"show tags from st_int_112")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"create table st_int_113 using mt_int tags(-1.1e-307)")
            tdSql.query(f"show tags from st_int_113")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"create table st_int_200 using mt_int tags( 0x01)")
            tdSql.query(f"show tags from st_int_200")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"create table st_int_201 using mt_int tags(0b01 )")
            tdSql.query(f"show tags from st_int_201")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"create table st_int_202 using mt_int tags(+0x01)")
            tdSql.query(f"show tags from st_int_202")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"create table st_int_203 using mt_int tags( -0b01 )")
            tdSql.query(f"show tags from st_int_203")
            tdSql.checkData(0, 5, -1)

    def int_insert_data(self):
            tdLog.info(f"case 1: insert values for test column values")

            tdSql.execute(f"insert into st_int_0 values(now, NULL)")
            tdSql.query(f"select * from st_int_0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_int_1 values(now, NULL)")
            tdSql.query(f"select * from st_int_1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_int_6 values(now, 2147483647)")
            tdSql.query(f"select * from st_int_6")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 2147483647)

            tdSql.execute(f"insert into st_int_7 values(now, -2147483647)")
            tdSql.query(f"select * from st_int_7")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -2147483647)

            tdSql.execute(f"insert into st_int_8 values(now, +100)")
            tdSql.query(f"select * from st_int_8")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 100)

            tdSql.execute(f'insert into st_int_9 values(now, "-098")')
            tdSql.query(f"select * from st_int_9")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -98)

            tdSql.execute(f"insert into st_int_10 values(now, '0')")
            tdSql.query(f"select * from st_int_10")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_int_11 values(now, -0)")
            tdSql.query(f"select * from st_int_11")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f'insert into st_int_12 values(now, "+056")')
            tdSql.query(f"select * from st_int_12")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 56)

            tdSql.execute(f"insert into st_int_13 values(now, +056)")
            tdSql.query(f"select * from st_int_13")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 56)

            tdSql.execute(f"insert into st_int_14 values(now, -056)")
            tdSql.query(f"select * from st_int_14")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -56)

            tdSql.execute(f'insert into st_int_100 values(now, "0x01")')
            tdSql.query(f"select * from st_int_100")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_int_101 values(now, "0b01")')
            tdSql.query(f"select * from st_int_101")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_int_102 values(now, "+0x01")')
            tdSql.query(f"select * from st_int_102")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_int_103 values(now, "-0b01")')
            tdSql.query(f"select * from st_int_103")
            tdSql.checkData(0, 1, -1)

            tdSql.execute(f'insert into st_int_104 values(now, "-123.1")')
            tdSql.query(f"select * from st_int_104")
            tdSql.checkData(0, 1, -123)

            tdSql.execute(f'insert into st_int_105 values(now, "+123.5")')
            tdSql.query(f"select * from st_int_105")
            tdSql.checkData(0, 1, 124)

            tdSql.execute(f'insert into st_int_106 values(now, "-1e-1")')
            tdSql.query(f"select * from st_int_106")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f'insert into st_int_107 values(now, "+0.1235e3")')
            tdSql.query(f"select * from st_int_107")
            tdSql.checkData(0, 1, 124)

            tdSql.execute(f'insert into st_int_108 values(now, "-0.11e-30")')
            tdSql.query(f"select * from st_int_108")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f'insert into st_int_109 values(now, "-1.1e-307")')
            tdSql.query(f"select * from st_int_109")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_int_110 values(now,  -1e-1 )")
            tdSql.query(f"select * from st_int_110")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_int_111 values(now,  +0.1235e3 )")
            tdSql.query(f"select * from st_int_111")
            tdSql.checkData(0, 1, 124)

            tdSql.execute(f"insert into st_int_112 values(now, -0.11e-30)")
            tdSql.query(f"select * from st_int_112")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_int_113 values(now, -1.1e-307)")
            tdSql.query(f"select * from st_int_113")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_int_200 values(now,  0x01)")
            tdSql.query(f"select * from st_int_200")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_int_201 values(now, 0b01 )")
            tdSql.query(f"select * from st_int_201")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_int_202 values(now, +0x01)")
            tdSql.query(f"select * from st_int_202")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_int_203 values(now,  -0b01 )")
            tdSql.query(f"select * from st_int_203")
            tdSql.checkData(0, 1, -1)

    def int_auto_create_table(self):
            tdLog.info(f"case 2: dynamic create table for test tag values")

            tdSql.execute(
                f"insert into st_int_16 using mt_int tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show create table st_int_16")
            tdSql.query(f"show tags from st_int_16")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_int_16")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_int_17 using mt_int tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show tags from st_int_17")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_int_17")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_int_18 using mt_int tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_int_18")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_int_18")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_int_19 using mt_int tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_int_19")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_int_19")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_int_20 using mt_int tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_int_20")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_int_20")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_int_21 using mt_int tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_int_21")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_int_21")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_int_22 using mt_int tags(2147483647) values(now, 2147483647)"
            )
            tdSql.query(f"show tags from st_int_22")
            tdSql.checkData(0, 5, 2147483647)

            tdSql.query(f"select * from st_int_22")
            tdSql.checkData(0, 1, 2147483647)

            tdSql.execute(
                f"insert into st_int_23 using mt_int tags(-2147483647) values(now, -2147483647)"
            )
            tdSql.query(f"show tags from st_int_23")
            tdSql.checkData(0, 5, -2147483647)

            tdSql.query(f"select * from st_int_23")
            tdSql.checkData(0, 1, -2147483647)

            tdSql.execute(f"insert into st_int_24 using mt_int tags(10) values(now, 10)")
            tdSql.query(f"show tags from st_int_24")
            tdSql.checkData(0, 5, 10)

            tdSql.query(f"select * from st_int_24")
            tdSql.checkData(0, 1, 10)

            tdSql.execute(
                f'insert into st_int_25 using mt_int tags("-0") values(now, "-0")'
            )
            tdSql.query(f"show tags from st_int_25")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_int_25")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_int_26 using mt_int tags('123') values(now, '123')"
            )
            tdSql.query(f"show tags from st_int_26")
            tdSql.checkData(0, 5, 123)

            tdSql.query(f"select * from st_int_26")
            tdSql.checkData(0, 1, 123)

            tdSql.execute(
                f"insert into st_int_27 using mt_int tags(+056) values(now, +00056)"
            )
            tdSql.query(f"show tags from st_int_27")
            tdSql.checkData(0, 5, 56)

            tdSql.query(f"select * from st_int_27")
            tdSql.checkData(0, 1, 56)

            tdSql.execute(
                f"insert into st_int_28 using mt_int tags(-056) values(now, -0056)"
            )
            tdSql.query(f"show tags from st_int_28")
            tdSql.checkData(0, 5, -56)

            tdSql.query(f"select * from st_int_28")
            tdSql.checkData(0, 1, -56)

            tdSql.execute(
                f'insert into st_int_1100 using mt_int tags("0x01") values(now, "0x01");'
            )
            tdSql.query(f"show tags from st_int_1100")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select * from st_int_1100")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_int_1101 using mt_int tags("0b01") values(now, "0b01")'
            )
            tdSql.query(f"show tags from st_int_1101")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select * from st_int_1101")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_int_1102 using mt_int tags("+0x01") values(now, "+0x01")'
            )
            tdSql.query(f"show tags from st_int_1102")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select * from st_int_1102")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_int_1103 using mt_int tags("-0b01") values(now, "-0b01")'
            )
            tdSql.query(f"show tags from st_int_1103")
            tdSql.checkData(0, 5, -1)

            tdSql.query(f"select * from st_int_1103")
            tdSql.checkData(0, 1, -1)

            tdSql.execute(
                f'insert into st_int_1104 using mt_int tags("-123.1") values(now, "-123.1");'
            )
            tdSql.query(f"show tags from st_int_1104")
            tdSql.checkData(0, 5, -123)

            tdSql.query(f"select * from st_int_1104")
            tdSql.checkData(0, 1, -123)

            tdSql.execute(
                f'insert into st_int_1105 using mt_int tags("+123.5") values(now, "+123.5")'
            )
            tdSql.query(f"show tags from st_int_1105")
            tdSql.checkData(0, 5, 124)

            tdSql.query(f"select * from st_int_1105")
            tdSql.checkData(0, 1, 124)

            tdSql.execute(
                f'insert into st_int_1106 using mt_int tags("-1e-1") values(now, "-1e-1")'
            )
            tdSql.query(f"show tags from st_int_1106")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_int_1106")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f'insert into st_int_1107 using mt_int tags("+0.1235e3") values(now, "+0.1235e3");'
            )
            tdSql.query(f"show tags from st_int_1107")
            tdSql.checkData(0, 5, 124)

            tdSql.execute(
                f'insert into st_int_1108 using mt_int tags("-0.11e-30") values(now, "-0.11e-30")'
            )
            tdSql.query(f"show tags from st_int_1108")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_int_1108")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f'insert into st_int_1109 using mt_int tags("-1.1e-307") values(now, "-1.1e-307")'
            )
            tdSql.query(f"show tags from st_int_1109")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_int_1109")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_int_1110 using mt_int tags( -1e-1 ) values(now, -1e-1);"
            )
            tdSql.query(f"show tags from st_int_1110")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_int_1110")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_int_1111 using mt_int tags( +0.1235e3 ) values(now, +0.1235e3)"
            )
            tdSql.query(f"show tags from st_int_1111")
            tdSql.checkData(0, 5, 124)

            tdSql.query(f"select * from st_int_1111")
            tdSql.checkData(0, 1, 124)

            tdSql.execute(
                f"insert into st_int_1112 using mt_int tags(-0.11e-30) values(now, -0.11e-30)"
            )
            tdSql.query(f"show tags from st_int_1112")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_int_1112")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_int_1113 using mt_int tags(-1.1e-307) values(now, -1.1e-307)"
            )
            tdSql.query(f"show tags from st_int_1113")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_int_1113")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_int_1200 using mt_int tags( 0x01) values(now, 0x01)"
            )
            tdSql.query(f"show tags from st_int_1200")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select * from st_int_1200")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_int_1201 using mt_int tags(0b01 ) values(now, 0b01)"
            )
            tdSql.query(f"show tags from st_int_1201")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select * from st_int_1201")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_int_1202 using mt_int tags(+0x01) values(now, +0x01)"
            )
            tdSql.query(f"show tags from st_int_1202")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select * from st_int_1202")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_int_1203 using mt_int tags( -0b01 ) values(now, -0b01)"
            )
            tdSql.query(f"show tags from st_int_1203")
            tdSql.checkData(0, 5, -1)

            tdSql.query(f"select * from st_int_1203")
            tdSql.checkData(0, 1, -1)

    def int_alter_tag_value(self):
            tdLog.info(f"case 3: alter tag value")

            tdSql.execute(f"alter table st_int_0 set tag tagname=2147483647")
            tdSql.query(f"show tags from st_int_0")
            tdSql.checkData(0, 5, 2147483647)

            tdSql.execute(f"alter table st_int_0 set tag tagname=-2147483647")
            tdSql.query(f"show tags from st_int_0")
            tdSql.checkData(0, 5, -2147483647)

            tdSql.execute(f"alter table st_int_0 set tag tagname=+100")
            tdSql.query(f"show tags from st_int_0")
            tdSql.checkData(0, 5, 100)

            tdSql.execute(f"alter table st_int_0 set tag tagname=-33")
            tdSql.query(f"show tags from st_int_0")
            tdSql.checkData(0, 5, -33)

            tdSql.execute(f"alter table st_int_0 set tag tagname='+98'")
            tdSql.query(f"show tags from st_int_0")
            tdSql.checkData(0, 5, 98)

            tdSql.execute(f"alter table st_int_0 set tag tagname='-076'")
            tdSql.query(f"show tags from st_int_0")
            tdSql.checkData(0, 5, -76)

            tdSql.execute(f"alter table st_int_0 set tag tagname=+0012")
            tdSql.query(f"show tags from st_int_0")
            tdSql.checkData(0, 5, 12)

            tdSql.execute(f"alter table st_int_0 set tag tagname=-00063")
            tdSql.query(f"show tags from st_int_0")
            tdSql.checkData(0, 5, -63)

            tdSql.execute(f'alter table st_int_100 set tag tagname="0x01"')
            tdSql.query(f"show tags from st_int_100")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'alter table st_int_101 set tag tagname="0b01"')
            tdSql.query(f"show tags from st_int_101")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'alter table st_int_102 set tag tagname="+0x01"')
            tdSql.query(f"show tags from st_int_102")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'alter table st_int_103 set tag tagname="-0b01"')
            tdSql.query(f"show tags from st_int_103")
            tdSql.checkData(0, 5, -1)

            tdSql.execute(f'alter table st_int_104 set tag tagname="-123.1"')
            tdSql.query(f"show tags from st_int_104")
            tdSql.checkData(0, 5, -123)

            tdSql.execute(f'alter table st_int_105 set tag tagname="+123.5"')
            tdSql.query(f"show tags from st_int_105")
            tdSql.checkData(0, 5, 124)

            tdSql.execute(f'alter table st_int_106 set tag tagname="-1e-1"')
            tdSql.query(f"show tags from st_int_106")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f'alter table st_int_107 set tag tagname="+0.1235e3"')
            tdSql.query(f"show tags from st_int_107")
            tdSql.checkData(0, 5, 124)

            tdSql.execute(f'alter table st_int_108 set tag tagname="-0.11e-30"')
            tdSql.query(f"show tags from st_int_108")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f'alter table st_int_109 set tag tagname="-1.1e-307"')
            tdSql.query(f"show tags from st_int_109")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"alter table st_int_110 set tag tagname= -1e-1 ")
            tdSql.query(f"show tags from st_int_110")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"alter table st_int_111 set tag tagname= +0.1235e3 ")
            tdSql.query(f"show tags from st_int_111")
            tdSql.checkData(0, 5, 124)

            tdSql.execute(f"alter table st_int_112 set tag tagname=-0.11e-30")
            tdSql.query(f"show tags from st_int_112")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"alter table st_int_113 set tag tagname=-1.1e-307")
            tdSql.query(f"show tags from st_int_113")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"alter table st_int_200 set tag tagname= 0x01")
            tdSql.query(f"show tags from st_int_200")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"alter table st_int_201 set tag tagname=0b01 ")
            tdSql.query(f"show tags from st_int_201")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"alter table st_int_202 set tag tagname=+0x01")
            tdSql.query(f"show tags from st_int_202")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"alter table st_int_203 set tag tagname= -0b01 ")
            tdSql.query(f"show tags from st_int_203")
            tdSql.checkData(0, 5, -1)

    def int_illegal_input(self):
            tdLog.info(f"case 4: illegal input")

            tdSql.error(f"create table st_int_e0 using mt_int tags(2147483648)")
            tdSql.execute(f"create table st_int_e0_err1 using mt_int tags(-2147483648)")
            tdSql.error(f"create table st_int_e0_err2 using mt_int tags(-2147483649)")
            tdSql.error(f"create table st_int_e0 using mt_int tags(214748364800)")
            tdSql.error(f"create table st_int_e0 using mt_int tags(-214748364800)")
            # truncate integer part
            tdSql.execute(f"create table st_int_e0 using mt_int tags(12.80)")
            tdSql.execute(f"drop table st_int_e0")
            tdSql.execute(f"create table st_int_e0 using mt_int tags(-11.80)")
            tdSql.execute(f"drop table st_int_e0")
            tdSql.error(f"create table st_int_e0 using mt_int tags(123abc)")
            tdSql.error(f'create table st_int_e0 using mt_int tags("123abc")')
            tdSql.error(f"create table st_int_e0 using mt_int tags(abc)")
            tdSql.error(f'create table st_int_e0 using mt_int tags("abc")')
            tdSql.error(f'create table st_int_e0 using mt_int tags(" ")')
            tdSql.error(f"create table st_int_e0_err2 using mt_int tags('')")

            tdSql.execute(f"create table st_int_e0 using mt_int tags(123)")
            tdSql.execute(f"create table st_int_e1 using mt_int tags(123)")
            tdSql.execute(f"create table st_int_e2 using mt_int tags(123)")
            tdSql.execute(f"create table st_int_e3 using mt_int tags(123)")
            tdSql.execute(f"create table st_int_e4 using mt_int tags(123)")
            tdSql.execute(f"create table st_int_e5 using mt_int tags(123)")
            tdSql.execute(f"create table st_int_e6 using mt_int tags(123)")
            tdSql.execute(f"create table st_int_e7 using mt_int tags(123)")
            tdSql.execute(f"create table st_int_e8 using mt_int tags(123)")
            tdSql.execute(f"create table st_int_e9 using mt_int tags(123)")
            tdSql.execute(f"create table st_int_e10 using mt_int tags(123)")
            tdSql.execute(f"create table st_int_e11 using mt_int tags(123)")
            tdSql.execute(f"create table st_int_e12 using mt_int tags(123)")

            tdSql.error(f"insert into st_int_e0 values(now, 2147483648)")
            tdSql.execute(f"insert into st_int_e1 values(now, -2147483648)")
            tdSql.error(f"insert into st_int_e2 values(now, 3147483648)")
            tdSql.error(f"insert into st_int_e3 values(now, -21474836481)")
            tdSql.execute(f"insert into st_int_e4 values(now, 12.80)")
            tdSql.execute(f"insert into st_int_e5 values(now, -11.80)")
            tdSql.error(f"insert into st_int_e6 values(now, 123abc)")
            tdSql.error(f'insert into st_int_e7 values(now, "123abc")')
            tdSql.error(f"insert into st_int_e9 values(now, abc)")
            tdSql.error(f'insert into st_int_e10 values(now, "abc")')
            tdSql.error(f'insert into st_int_e11 values(now, " ")')
            tdSql.error(f"insert into st_int_e12 values(now, ''")

            tdSql.error(
                f"insert into st_int_e13 using mt_int tags(033) values(now, 2147483648)"
            )
            tdSql.execute(
                f"insert into st_int_e14 using mt_int tags(033) values(now, -2147483648)"
            )
            tdSql.error(
                f"insert into st_int_e15 using mt_int tags(033) values(now, 5147483648)"
            )
            tdSql.error(
                f"insert into st_int_e16 using mt_int tags(033) values(now, -21474836481)"
            )
            tdSql.execute(
                f"insert into st_int_e17 using mt_int tags(033) values(now, 12.80)"
            )
            tdSql.execute(
                f"insert into st_int_e18 using mt_int tags(033) values(now, -11.80)"
            )
            tdSql.error(
                f"insert into st_int_e19 using mt_int tags(033) values(now, 123abc)"
            )
            tdSql.error(
                f'insert into st_int_e20 using mt_int tags(033) values(now, "123abc")'
            )
            tdSql.error(f"insert into st_int_e22 using mt_int tags(033) values(now, abc)")
            tdSql.error(f'insert into st_int_e23 using mt_int tags(033) values(now, "abc")')
            tdSql.error(f'insert into st_int_e24 using mt_int tags(033) values(now, " ")')
            tdSql.error(f"insert into st_int_e25 using mt_int tags(033) values(now, '')")

            tdSql.error(
                f"insert into st_int_e13 using mt_int tags(2147483648) values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_int_e14_1 using mt_int tags(-2147483648) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_int_e15 using mt_int tags(21474836480) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_int_e16 using mt_int tags(-2147483649) values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_int_e17 using mt_int tags(12.80) values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_int_e18 using mt_int tags(-11.80) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_int_e19 using mt_int tags(123abc) values(now, -033)"
            )
            tdSql.error(
                f'insert into st_int_e20 using mt_int tags("123abc") values(now, -033)'
            )
            tdSql.error(f"insert into st_int_e22 using mt_int tags(abc) values(now, -033)")
            tdSql.error(
                f'insert into st_int_e23 using mt_int tags("abc") values(now, -033)'
            )
            tdSql.error(f'insert into st_int_e24 using mt_int tags(" ") values(now, -033)')
            tdSql.error(
                f"insert into st_int_e25_1 using mt_int tags(" ") values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_int_e26_1 using mt_int tags('123') values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_int_e27_1 using mt_int tags('12.80') values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_int_e28_1 using mt_int tags('-11.80') values(now, -033)"
            )

            tdSql.execute(
                f"insert into st_int_e13 using mt_int tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_int_e14 using mt_int tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_int_e15 using mt_int tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_int_e16 using mt_int tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_int_e17 using mt_int tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_int_e18 using mt_int tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_int_e19 using mt_int tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_int_e20 using mt_int tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_int_e21 using mt_int tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_int_e22 using mt_int tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_int_e23 using mt_int tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_int_e24 using mt_int tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_int_e25 using mt_int tags(033) values(now, 00062)"
            )

            tdSql.execute(f"alter table st_int_e13 set tag tagname=2147483647")
            tdSql.error(f"alter table st_int_e13 set tag tagname=2147483648")
            tdSql.execute(f"alter table st_int_e14 set tag tagname=-2147483648")
            tdSql.error(f"alter table st_int_e14 set tag tagname=2147483649")
            tdSql.error(f"alter table st_int_e15 set tag tagname=12147483648")
            tdSql.error(f"alter table st_int_e16 set tag tagname=-3147483648")
            tdSql.error(f"alter table st_int_e19 set tag tagname=123abc")
            tdSql.error(f'alter table st_int_e20 set tag tagname="123abc"')
            tdSql.error(f"alter table st_int_e22 set tag tagname=abc")
            tdSql.error(f'alter table st_int_e23 set tag tagname="abc"')
            tdSql.error(f"alter table st_int_e25 set tag tagname=1+1d")
            tdSql.error(f'alter table st_int_e25 set tag tagname="1"+1d')
            tdSql.error(f'alter table st_int_e24 set tag tagname=" "')
            tdSql.error(f"alter table st_int_e25 set tag tagname=''")
            tdSql.execute(f"alter table st_int_e26_1 set tag tagname='123'")
            tdSql.execute(f"alter table st_int_e27_1 set tag tagname='12.80'")
            tdSql.execute(f"alter table st_int_e28_1 set tag tagname='-11.80'")


    # ======================================================================
    # json: json column and tag values
    # ======================================================================

    def test_datatype_json(self):
            """DataTypes: json

            1. Create table
            2. Insert data
            3. Alter tag value
            4. Handle illegal input

            Since: v3.0.0.0

            Labels: common,ci,integration,functional

            Jira: None

            History:
                - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_json.sim

            """
            self.json_create_table()
            self.json_insert_data()
            self.json_auto_create_table()
            self.json_alter_tag_value()
            self.json_illegal_input()

    def json_create_table(self):
            tdLog.info(f"create super table")
            tdSql.execute(
                f"create table mt_json (ts timestamp, c varchar(50)) tags(tagname json)"
            )

            tdLog.info(f"case 0: static create table for test tag values")
            tdSql.execute(f"create table st_json_0 using mt_json tags(NULL)")
            tdSql.query(f"show tags from st_json_0")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_json_1 using mt_json tags(NULL)")
            tdSql.query(f"show tags from st_json_1")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_json_2 using mt_json tags('NULL')")
            tdSql.query(f"show tags from st_json_2")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_json_3 using mt_json tags('NULL')")
            tdSql.query(f"show tags from st_json_3")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_json_4 using mt_json tags("NULL")')
            tdSql.query(f"show tags from st_json_4")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_json_5 using mt_json tags("NULL")')
            tdSql.query(f"show tags from st_json_5")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_json_6 using mt_json tags("")')
            tdSql.query(f"show tags from st_json_6")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_json_7 using mt_json tags(" ")')
            tdSql.query(f"show tags from st_json_7")
            tdSql.checkData(0, 5, None)

            str = '{\\"k1\\":\\"v1\\",\\"k2\\":\\"v2\\"}'
            tdLog.info(f'create table st_json_8 using mt_json tags("{str}")')
            tdSql.execute(f'create table st_json_8 using mt_json tags("{str}")')
            tdSql.query(f"show tags from st_json_8")
            tdSql.checkData(0, 5, '{"k1":"v1","k2":"v2"}')

    def json_insert_data(self):
            tdLog.info(f"case 1: insert values for test column values")

    def json_auto_create_table(self):
            tdLog.info(f"case 2: dynamic create table for test tag values")

            tdSql.execute(
                f"insert into st_json_100 using mt_json tags(NULL) values(now,NULL)"
            )
            tdSql.query(f"show tags from st_json_100")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_json_100")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_json_101 using mt_json tags(NULL) values(now,NULL)"
            )
            tdSql.query(f"show tags from st_json_101")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_json_101")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_json_102 using mt_json tags('NULL') values(now,'NULL')"
            )
            tdSql.query(f"show tags from st_json_102")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_json_102")
            tdSql.checkData(0, 1, "NULL")

            tdSql.execute(
                f"insert into st_json_103 using mt_json tags('NULL') values(now,'NULL')"
            )
            tdSql.query(f"show tags from st_json_103")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_json_103")
            tdSql.checkData(0, 1, "NULL")

            tdSql.execute(
                f'insert into st_json_104 using mt_json tags("NULL") values(now,"NULL")'
            )
            tdSql.query(f"show tags from st_json_104")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_json_104")
            tdSql.checkData(0, 1, "NULL")

            tdSql.execute(
                f'insert into st_json_105 using mt_json tags("NULL") values(now,"NULL")'
            )
            tdSql.query(f"show tags from st_json_105")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_json_105")
            tdSql.checkData(0, 1, "NULL")

            tdSql.execute(
                f'insert into st_json_106 using mt_json tags("") values(now,"vc")'
            )
            tdSql.query(f"show tags from st_json_106")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_json_106")
            tdSql.checkData(0, 1, "vc")

            tdSql.execute(
                f'insert into st_json_107 using mt_json tags(" ") values(now,"vc")'
            )
            tdSql.query(f"show tags from st_json_107")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_json_107")
            tdSql.checkData(0, 1, "vc")

            str = '{\\"k1\\":\\"v1\\",\\"k2\\":\\"v2\\"}'
            tdSql.execute(
                f'insert into st_json_108 using mt_json tags("{str}") values(now,"vc")'
            )
            tdSql.query(f"show tags from st_json_108")
            tdSql.checkData(0, 5, '{"k1":"v1","k2":"v2"}')

            tdSql.query(f"select * from st_json_108")
            tdSql.checkData(0, 1, "vc")

    def json_alter_tag_value(self):
            tdLog.info(f"case 3: alter tag value")

            tdSql.execute(f"alter table st_json_100  set tag tagname=NULL")
            tdSql.query(f"show tags from st_json_100")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"alter table st_json_101  set tag tagname=NULL")
            tdSql.query(f"show tags from st_json_101")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"alter table st_json_102  set tag tagname='NULL'")
            tdSql.query(f"show tags from st_json_102")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"alter table st_json_103  set tag tagname='NULL'")
            tdSql.query(f"show tags from st_json_103")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'alter table st_json_104  set tag tagname="NULL"')
            tdSql.query(f"show tags from st_json_104")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'alter table st_json_105  set tag tagname="NULL"')
            tdSql.query(f"show tags from st_json_105")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'alter table st_json_106  set tag tagname=""')
            tdSql.query(f"show tags from st_json_106")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'alter table st_json_107  set tag tagname=" "')
            tdSql.query(f"show tags from st_json_107")
            tdSql.checkData(0, 5, None)

            str = '{\\"k1\\":\\"v1\\",\\"k2\\":\\"v2\\"}'
            tdSql.execute(f'alter table st_json_108  set tag tagname="{str}"')
            tdSql.query(f"show tags from st_json_108")
            tdSql.checkData(0, 5, '{"k1":"v1","k2":"v2"}')

    def json_illegal_input(self):
            tdLog.info(f"case 4: illegal input")

            tdSql.error(f"create table st_json_206 using mt_json tags(+0123)")
            tdSql.error(f"create table st_json_207 using mt_json tags(-01.23)")
            tdSql.error(f"create table st_json_208 using mt_json tags(+0x01)")
            tdSql.error(f"create table st_json_209 using mt_json tags(-0b01)")
            tdSql.error(f"create table st_json_2010 using mt_json tags(-0.1e-10)")
            tdSql.error(f"create table st_json_2011 using mt_json tags(+0.1E+2)")
            tdSql.error(f"create table st_json_2012 using mt_json tags(tRue)")
            tdSql.error(f"create table st_json_2013 using mt_json tags(FalsE)")
            tdSql.error(f"create table st_json_2014 using mt_json tags(noW)")
            tdSql.error(f"create table st_json_2015 using mt_json tags(toDay)")
            tdSql.error(
                f"insert into st_json_206 using mt_json tags(+0123) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_json_207 using mt_json tags(-01.23) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_json_208 using mt_json tags(+0x01) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_json_209 using mt_json tags(-0b01) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_json_2010 using mt_json tags(-0.1e-10) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_json_2011 using mt_json tags(+0.1E+2) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_json_2012 using mt_json tags(tRue) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_json_2013 using mt_json tags(FalsE) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_json_2014 using mt_json tags(noW) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_json_2015 using mt_json tags(toDay) values(now, NULL);"
            )


    # ======================================================================
    # nchar: nchar column and tag values
    # ======================================================================

    def test_datatype_nchar(self):
            """DataTypes: nchar

            1. Create table
            2. Insert data
            3. Alter tag value
            4. Handle illegal input

            Since: v3.0.0.0

            Labels: common,ci,integration,functional

            Jira: None

            History:
                - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_nchar.sim

            """
            self.nchar_create_table()
            self.nchar_insert_data()
            self.nchar_auto_create_table()
            self.nchar_alter_tag_value()
            self.nchar_illegal_input()

    def nchar_create_table(self):
            tdLog.info(f"create super table")
            tdSql.execute(
                f"create table mt_nchar (ts timestamp, c nchar(50)) tags(tagname nchar(50))"
            )

            tdLog.info(f"case 0: static create table for test tag values")

            tdSql.execute(f"create table st_nchar_0 using mt_nchar tags(NULL)")
            tdSql.query(f"show create table st_nchar_0")
            tdSql.query(f"show tags from st_nchar_0")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_nchar_1 using mt_nchar tags(NULL)")
            tdSql.query(f"show tags from st_nchar_1")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_nchar_2 using mt_nchar tags('NULL')")
            tdSql.query(f"show tags from st_nchar_2")
            tdSql.checkData(0, 5, "NULL")

            tdSql.execute(f"create table st_nchar_3 using mt_nchar tags('NULL')")
            tdSql.query(f"show tags from st_nchar_3")
            tdSql.checkData(0, 5, "NULL")

            tdSql.execute(f'create table st_nchar_4 using mt_nchar tags("NULL")')
            tdSql.query(f"show tags from st_nchar_4")
            tdSql.checkData(0, 5, "NULL")

            tdSql.execute(f'create table st_nchar_5 using mt_nchar tags("NULL")')
            tdSql.query(f"show tags from st_nchar_5")
            tdSql.checkData(0, 5, "NULL")

            tdSql.execute(f"create table st_nchar_6 using mt_nchar tags(+0123)")
            tdSql.query(f"show tags from st_nchar_6")
            tdSql.checkData(0, 5, "+0123")

            tdSql.execute(f"create table st_nchar_7 using mt_nchar tags(-01.23)")
            tdSql.query(f"show tags from st_nchar_7")
            tdSql.checkData(0, 5, "-01.23")

            tdSql.execute(f"create table st_nchar_8 using mt_nchar tags(+0x01)")
            tdSql.query(f"show tags from st_nchar_8")
            tdSql.checkData(0, 5, "+0x01")

            tdSql.execute(f"create table st_nchar_9 using mt_nchar tags(-0b01)")
            tdSql.query(f"show tags from st_nchar_9")
            tdSql.checkData(0, 5, "-0b01")

            tdSql.execute(f"create table st_nchar_10 using mt_nchar tags(-0.1e-10)")
            tdSql.query(f"show tags from st_nchar_10")
            tdSql.checkData(0, 5, "-0.1e-10")

            tdSql.execute(f"create table st_nchar_11 using mt_nchar tags(+0.1E+2)")
            tdSql.query(f"show tags from st_nchar_11")
            tdSql.checkData(0, 5, "+0.1e+2")

            tdSql.execute(f"create table st_nchar_12 using mt_nchar tags(tRue)")
            tdSql.query(f"show tags from st_nchar_12")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"create table st_nchar_13 using mt_nchar tags(FalsE)")
            tdSql.query(f"show tags from st_nchar_13")
            tdSql.checkData(0, 5, "false")

            tdSql.execute(f"create table st_nchar_14 using mt_nchar tags(noW)")
            tdSql.query(f"show tags from st_nchar_14")
            tdSql.checkData(0, 5, "now")

            tdSql.execute(f"create table st_nchar_15 using mt_nchar tags(toDay)")
            tdSql.query(f"show tags from st_nchar_15")
            tdSql.checkData(0, 5, "today")

    def nchar_insert_data(self):
            tdLog.info(f"case 1: insert values for test column values")

            tdSql.execute(f"insert into st_nchar_0 values(now, NULL)")
            tdSql.query(f"select * from st_nchar_0")
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_nchar_1 values(now, NULL)")
            tdSql.query(f"select * from st_nchar_1")
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_nchar_2 values(now, 'NULL')")
            tdSql.query(f"select * from st_nchar_2")
            tdSql.checkData(0, 1, "NULL")

            tdSql.execute(f"insert into st_nchar_3 values(now, 'NULL')")
            tdSql.query(f"select * from st_nchar_3")
            tdSql.checkData(0, 1, "NULL")

            tdSql.execute(f'insert into st_nchar_4 values(now, "NULL")')
            tdSql.query(f"select * from st_nchar_4")
            tdSql.checkData(0, 1, "NULL")

            tdSql.execute(f'insert into st_nchar_5 values(now, "NULL")')
            tdSql.query(f"select * from st_nchar_5")
            tdSql.checkData(0, 1, "NULL")

            tdSql.execute(f"insert into st_nchar_6 values(now, +0123)")
            tdSql.query(f"select * from st_nchar_6")
            tdSql.checkData(0, 1, "+0123")

            tdSql.execute(f"insert into st_nchar_7 values(now, -01.23)")
            tdSql.query(f"select * from st_nchar_7")
            tdSql.checkData(0, 1, "-01.23")

            tdSql.execute(f"insert into st_nchar_8 values(now, +0x01)")
            tdSql.query(f"select * from st_nchar_8")
            tdSql.checkData(0, 1, "+0x01")

            tdSql.execute(f"insert into st_nchar_9 values(now, -0b01)")
            tdSql.query(f"select * from st_nchar_9")
            tdSql.checkData(0, 1, "-0b01")

            tdSql.execute(f"insert into st_nchar_10 values(now, -0.1e-10)")
            tdSql.query(f"select * from st_nchar_10")
            tdSql.checkData(0, 1, "-0.1e-10")

            tdSql.execute(f"insert into st_nchar_11 values(now, +0.1E+2)")
            tdSql.query(f"select * from st_nchar_11")
            tdSql.checkData(0, 1, "+0.1e+2")

            tdSql.execute(f"insert into st_nchar_12 values(now, tRue)")
            tdSql.query(f"select * from st_nchar_12")
            tdSql.checkData(0, 1, "true")

            tdSql.execute(f"insert into st_nchar_13 values(now, FalsE)")
            tdSql.query(f"select * from st_nchar_13")
            tdSql.checkData(0, 1, "false")

            tdSql.execute(f"insert into st_nchar_14 values(now, noW)")
            tdSql.query(f"select * from st_nchar_14")
            tdSql.checkData(0, 1, "now")

            tdSql.execute(f"insert into st_nchar_15 values(now, toDay)")
            tdSql.query(f"select * from st_nchar_15")
            tdSql.checkData(0, 1, "today")

    def nchar_auto_create_table(self):
            tdLog.info(f"case 2: dynamic create table for test tag values")

            tdSql.execute(
                f"insert into st_nchar_0 using mt_nchar tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show tags from st_nchar_0")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_nchar_0")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_nchar_1 using mt_nchar tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show tags from st_nchar_1")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_nchar_1")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_nchar_2 using mt_nchar tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_nchar_2")
            tdSql.checkData(0, 5, "NULL")

            tdSql.query(f"select * from st_nchar_2")
            tdSql.checkData(0, 1, "NULL")

            tdSql.execute(
                f"insert into st_nchar_3 using mt_nchar tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_nchar_3")
            tdSql.checkData(0, 5, "NULL")

            tdSql.query(f"select * from st_nchar_3")
            tdSql.checkData(0, 1, "NULL")

            tdSql.execute(
                f'insert into st_nchar_4 using mt_nchar tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_nchar_4")
            tdSql.checkData(0, 5, "NULL")

            tdSql.query(f"select * from st_nchar_4")
            tdSql.checkData(0, 1, "NULL")

            tdSql.execute(
                f'insert into st_nchar_5 using mt_nchar tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_nchar_5")
            tdSql.checkData(0, 5, "NULL")

            tdSql.query(f"select * from st_nchar_5")
            tdSql.checkData(0, 1, "NULL")

            tdSql.execute(
                f"insert into st_nchar_6 using mt_nchar tags(+0123) values(now, +0123)"
            )
            tdSql.query(f"show tags from st_nchar_6")
            tdSql.checkData(0, 5, "+0123")

            tdSql.query(f"select * from st_nchar_6")
            tdSql.checkData(0, 1, "+0123")

            tdSql.execute(
                f"insert into st_nchar_7 using mt_nchar tags(-01.23) values(now, -01.23)"
            )
            tdSql.query(f"show tags from st_nchar_7")
            tdSql.checkData(0, 5, "-01.23")

            tdSql.query(f"select * from st_nchar_7")
            tdSql.checkData(0, 1, "-01.23")

            tdSql.execute(
                f"insert into st_nchar_8 using mt_nchar tags(+0x01) values(now, +0x01)"
            )
            tdSql.query(f"show tags from st_nchar_8")
            tdSql.checkData(0, 5, "+0x01")

            tdSql.query(f"select * from st_nchar_8")
            tdSql.checkData(0, 1, "+0x01")

            tdSql.execute(
                f"insert into st_nchar_9 using mt_nchar tags(-0b01) values(now, -0b01)"
            )
            tdSql.query(f"show tags from st_nchar_9")
            tdSql.checkData(0, 5, "-0b01")

            tdSql.query(f"select * from st_nchar_9")
            tdSql.checkData(0, 1, "-0b01")

            tdSql.execute(
                f"insert into st_nchar_10 using mt_nchar tags(-0.1e-10) values(now, -0.1e-10)"
            )
            tdSql.query(f"show tags from st_nchar_10")
            tdSql.checkData(0, 5, "-0.1e-10")

            tdSql.query(f"select * from st_nchar_10")
            tdSql.checkData(0, 1, "-0.1e-10")

            tdSql.execute(
                f"insert into st_nchar_11 using mt_nchar tags(+0.1E+2) values(now, +0.1E+2)"
            )
            tdSql.query(f"show tags from st_nchar_11")
            tdSql.checkData(0, 5, "+0.1e+2")

            tdSql.query(f"select * from st_nchar_11")
            tdSql.checkData(0, 1, "+0.1e+2")

            tdSql.execute(
                f"insert into st_nchar_12 using mt_nchar tags(tRue) values(now, tRue)"
            )
            tdSql.query(f"show tags from st_nchar_12")
            tdSql.checkData(0, 5, "true")

            tdSql.query(f"select * from st_nchar_12")
            tdSql.checkData(0, 1, "true")

            tdSql.execute(
                f"insert into st_nchar_13 using mt_nchar tags(FalsE) values(now, FalsE)"
            )
            tdSql.query(f"show tags from st_nchar_13")
            tdSql.checkData(0, 5, "false")

            tdSql.query(f"select * from st_nchar_13")
            tdSql.checkData(0, 1, "false")

            tdSql.execute(
                f"insert into st_nchar_14 using mt_nchar tags(noW) values(now, noW)"
            )
            tdSql.query(f"show tags from st_nchar_14")
            tdSql.checkData(0, 5, "now")

            tdSql.query(f"select * from st_nchar_14")
            tdSql.checkData(0, 1, "now")

            tdSql.execute(
                f"insert into st_nchar_15 using mt_nchar tags(toDay) values(now, toDay)"
            )
            tdSql.query(f"show tags from st_nchar_15")
            tdSql.checkData(0, 5, "today")

            tdSql.query(f"select * from st_nchar_15")
            tdSql.checkData(0, 1, "today")

    def nchar_alter_tag_value(self):
            tdLog.info(f"case 3: alter tag value")

            tdSql.execute(f"alter table st_nchar_0  set tag tagname=NULL")
            tdSql.query(f"show tags from st_nchar_0")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"alter table st_nchar_1  set tag tagname=NULL")
            tdSql.query(f"show tags from st_nchar_1")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"alter table st_nchar_2  set tag tagname='NULL'")
            tdSql.query(f"show tags from st_nchar_2")
            tdSql.checkData(0, 5, "NULL")

            tdSql.execute(f"alter table st_nchar_3  set tag tagname='NULL'")
            tdSql.query(f"show tags from st_nchar_3")
            tdSql.checkData(0, 5, "NULL")

            tdSql.execute(f'alter table st_nchar_4  set tag tagname="NULL"')
            tdSql.query(f"show tags from st_nchar_4")
            tdSql.checkData(0, 5, "NULL")

            tdSql.execute(f'alter table st_nchar_5  set tag tagname="NULL"')
            tdSql.query(f"show tags from st_nchar_5")
            tdSql.checkData(0, 5, "NULL")

            tdSql.execute(f"alter table st_nchar_6  set tag tagname=+0123")
            tdSql.query(f"show tags from st_nchar_6")
            tdSql.checkData(0, 5, "+0123")

            tdSql.execute(f"alter table st_nchar_7  set tag tagname=-01.23")
            tdSql.query(f"show tags from st_nchar_7")
            tdSql.checkData(0, 5, "-01.23")

            tdSql.execute(f"alter table st_nchar_8  set tag tagname=+0x01")
            tdSql.query(f"show tags from st_nchar_8")
            tdSql.checkData(0, 5, "+0x01")

            tdSql.execute(f"alter table st_nchar_9  set tag tagname=-0b01")
            tdSql.query(f"show tags from st_nchar_9")
            tdSql.checkData(0, 5, "-0b01")

            tdSql.execute(f"alter table st_nchar_10  set tag tagname=-0.1e-10")
            tdSql.query(f"show tags from st_nchar_10")
            tdSql.checkData(0, 5, "-0.1e-10")

            tdSql.execute(f"alter table st_nchar_11  set tag tagname=+0.1E+2")
            tdSql.query(f"show tags from st_nchar_11")
            tdSql.checkData(0, 5, "+0.1e+2")

            tdSql.execute(f"alter table st_nchar_12  set tag tagname=tRue")
            tdSql.query(f"show tags from st_nchar_12")
            tdSql.checkData(0, 5, "true")

            tdSql.execute(f"alter table st_nchar_13  set tag tagname=FalsE")
            tdSql.query(f"show tags from st_nchar_13")
            tdSql.checkData(0, 5, "false")

            tdSql.execute(f"alter table st_nchar_14  set tag tagname=noW")
            tdSql.query(f"show tags from st_nchar_14")
            tdSql.checkData(0, 5, "now")

            tdSql.execute(f"alter table st_nchar_15  set tag tagname=toDay")
            tdSql.query(f"show tags from st_nchar_15")
            tdSql.checkData(0, 5, "today")

    def nchar_illegal_input(self):
            tdLog.info(f"case 4: illegal input")


    # ======================================================================
    # smallint: smallint column and tag values
    # ======================================================================

    def test_datatype_smallint(self):
            """DataTypes: smallint

            1. Create table
            2. Insert data
            3. Auto-create table
            4. Alter tag value
            5. Handle illegal input

            Since: v3.0.0.0

            Labels: common,ci,integration,functional

            Jira: None

            History:
                - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_smallint.sim

            """
            self.smallint_create_table()
            self.smallint_insert_data()
            self.smallint_auto_create_table()
            self.smallint_alter_tag_value()
            self.smallint_illegal_input()

    def smallint_create_table(self):
            tdLog.info(f"create super table")
            tdSql.execute(
                f"create table mt_smallint (ts timestamp, c smallint) tags(tagname smallint)"
            )

            tdLog.info(f"case 0: static create table for test tag values")
            tdSql.execute(f"create table st_smallint_0 using mt_smallint tags(NULL)")
            tdSql.query(f"show create table st_smallint_0")
            tdSql.query(f"show tags from st_smallint_0")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_smallint_1 using mt_smallint tags(NULL)")
            tdSql.query(f"show tags from st_smallint_1")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_smallint_2 using mt_smallint tags('NULL')")
            tdSql.query(f"show tags from st_smallint_2")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_smallint_3 using mt_smallint tags('NULL')")
            tdSql.query(f"show tags from st_smallint_3")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_smallint_4 using mt_smallint tags("NULL")')
            tdSql.query(f"show tags from st_smallint_4")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_smallint_5 using mt_smallint tags("NULL")')
            tdSql.query(f"show tags from st_smallint_5")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_smallint_6 using mt_smallint tags(-32767)")
            tdSql.query(f"show tags from st_smallint_6")
            tdSql.checkData(0, 5, -32767)

            tdSql.execute(f"create table st_smallint_7 using mt_smallint tags(32767)")
            tdSql.query(f"show tags from st_smallint_7")
            tdSql.checkData(0, 5, 32767)

            tdSql.execute(f"create table st_smallint_8 using mt_smallint tags(37)")
            tdSql.query(f"show tags from st_smallint_8")
            tdSql.checkData(0, 5, 37)

            tdSql.execute(f"create table st_smallint_9 using mt_smallint tags(-100)")
            tdSql.query(f"show tags from st_smallint_9")
            tdSql.checkData(0, 5, -100)

            tdSql.execute(f"create table st_smallint_10 using mt_smallint tags(+113)")
            tdSql.query(f"show tags from st_smallint_10")
            tdSql.checkData(0, 5, 113)

            tdSql.execute(f"create table st_smallint_11 using mt_smallint tags('-100')")
            tdSql.query(f"show tags from st_smallint_11")
            tdSql.checkData(0, 5, -100)

            tdSql.execute(f'create table st_smallint_12 using mt_smallint tags("+78")')
            tdSql.query(f"show tags from st_smallint_12")
            tdSql.checkData(0, 5, 78)

            tdSql.execute(f"create table st_smallint_13 using mt_smallint tags(+0078)")
            tdSql.query(f"show tags from st_smallint_13")
            tdSql.checkData(0, 5, 78)

            tdSql.execute(f"create table st_smallint_14 using mt_smallint tags(-00078)")
            tdSql.query(f"show tags from st_smallint_14")
            tdSql.checkData(0, 5, -78)

    def smallint_insert_data(self):
            tdLog.info(f"case 1: insert values for test column values")

            tdSql.execute(f"insert into st_smallint_0 values(now, NULL)")
            tdSql.query(f"select * from st_smallint_0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_smallint_1 values(now, NULL)")
            tdSql.query(f"select * from st_smallint_1")
            tdSql.checkRows(1)

            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_smallint_6 values(now, 32767)")
            tdSql.query(f"select * from st_smallint_6")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 32767)

            tdSql.execute(f"insert into st_smallint_7 values(now, -32767)")
            tdSql.query(f"select * from st_smallint_7")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -32767)

            tdSql.execute(f"insert into st_smallint_8 values(now, +100)")
            tdSql.query(f"select * from st_smallint_8")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 100)

            tdSql.execute(f'insert into st_smallint_9 values(now, "-098")')
            tdSql.query(f"select * from st_smallint_9")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -98)

            tdSql.execute(f"insert into st_smallint_10 values(now, '0')")
            tdSql.query(f"select * from st_smallint_10")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_smallint_11 values(now, -0)")
            tdSql.query(f"select * from st_smallint_11")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f'insert into st_smallint_12 values(now, "+056")')
            tdSql.query(f"select * from st_smallint_12")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 56)

            tdSql.execute(f"insert into st_smallint_13 values(now, +056)")
            tdSql.query(f"select * from st_smallint_13")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 56)

            tdSql.execute(f"insert into st_smallint_14 values(now, -056)")
            tdSql.query(f"select * from st_smallint_14")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -56)

    def smallint_auto_create_table(self):
            tdLog.info(f"case 2: dynamic create table for test tag values")

            tdSql.execute(
                f"insert into st_smallint_16 using mt_smallint tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show create table st_smallint_16")
            tdSql.query(f"show tags from st_smallint_16")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_smallint_16")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_smallint_17 using mt_smallint tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show tags from st_smallint_17")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_smallint_17")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_smallint_18 using mt_smallint tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_smallint_18")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_smallint_18")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_smallint_19 using mt_smallint tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_smallint_19")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_smallint_19")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_smallint_20 using mt_smallint tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_smallint_20")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_smallint_20")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_smallint_21 using mt_smallint tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_smallint_21")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_smallint_21")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_smallint_22 using mt_smallint tags(32767) values(now, 32767)"
            )
            tdSql.query(f"show tags from st_smallint_22")
            tdSql.checkData(0, 5, 32767)

            tdSql.query(f"select * from st_smallint_22")
            tdSql.checkData(0, 1, 32767)

            tdSql.execute(
                f"insert into st_smallint_23 using mt_smallint tags(-32767) values(now, -32767)"
            )
            tdSql.query(f"show tags from st_smallint_23")
            tdSql.checkData(0, 5, -32767)

            tdSql.query(f"select * from st_smallint_23")
            tdSql.checkData(0, 1, -32767)

            tdSql.execute(
                f"insert into st_smallint_24 using mt_smallint tags(10) values(now, 10)"
            )
            tdSql.query(f"show tags from st_smallint_24")
            tdSql.checkData(0, 5, 10)

            tdSql.query(f"select * from st_smallint_24")
            tdSql.checkData(0, 1, 10)

            tdSql.execute(
                f'insert into st_smallint_25 using mt_smallint tags("-0") values(now, "-0")'
            )
            tdSql.query(f"show tags from st_smallint_25")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_smallint_25")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_smallint_26 using mt_smallint tags('123') values(now, '123')"
            )
            tdSql.query(f"show tags from st_smallint_26")
            tdSql.checkData(0, 5, 123)

            tdSql.query(f"select * from st_smallint_26")
            tdSql.checkData(0, 1, 123)

            tdSql.execute(
                f"insert into st_smallint_27 using mt_smallint tags(+056) values(now, +00056)"
            )
            tdSql.query(f"show tags from st_smallint_27")
            tdSql.checkData(0, 5, 56)

            tdSql.query(f"select * from st_smallint_27")
            tdSql.checkData(0, 1, 56)

            tdSql.execute(
                f"insert into st_smallint_28 using mt_smallint tags(-056) values(now, -0056)"
            )
            tdSql.query(f"show tags from st_smallint_28")
            tdSql.checkData(0, 5, -56)

            tdSql.query(f"select * from st_smallint_28")
            tdSql.checkData(0, 1, -56)

    def smallint_alter_tag_value(self):
            tdLog.info(f"case 3: alter tag value")

            tdSql.execute(f"alter table st_smallint_0 set tag tagname=32767")
            tdSql.query(f"show tags from st_smallint_0")
            tdSql.checkData(0, 5, 32767)

            tdSql.execute(f"alter table st_smallint_0 set tag tagname=-32767")
            tdSql.query(f"show tags from st_smallint_0")
            tdSql.checkData(0, 5, -32767)

            tdSql.execute(f"alter table st_smallint_0 set tag tagname=+100")
            tdSql.query(f"show tags from st_smallint_0")
            tdSql.checkData(0, 5, 100)

            tdSql.execute(f"alter table st_smallint_0 set tag tagname=-33")
            tdSql.query(f"show tags from st_smallint_0")
            tdSql.checkData(0, 5, -33)

            tdSql.execute(f"alter table st_smallint_0 set tag tagname='+98'")
            tdSql.query(f"show tags from st_smallint_0")
            tdSql.checkData(0, 5, 98)

            tdSql.execute(f"alter table st_smallint_0 set tag tagname='-076'")
            tdSql.query(f"show tags from st_smallint_0")
            tdSql.checkData(0, 5, -76)

            tdSql.execute(f"alter table st_smallint_0 set tag tagname=+0012")
            tdSql.query(f"show tags from st_smallint_0")
            tdSql.checkData(0, 5, 12)

            tdSql.execute(f"alter table st_smallint_0 set tag tagname=-00063")
            tdSql.query(f"show tags from st_smallint_0")
            tdSql.checkData(0, 5, -63)

    def smallint_illegal_input(self):
            tdLog.info(f"case 4: illegal input")

            tdSql.error(f"create table st_smallint_e0 using mt_smallint tags(32768)")
            tdSql.execute(f"create table st_smallint_e0_0 using mt_smallint tags(-32768)")
            tdSql.error(f"create table st_smallint_e0 using mt_smallint tags(3276899)")
            tdSql.error(f"create table st_smallint_e0 using mt_smallint tags(-3276833)")
            tdSql.error(f"create table st_smallint_e0 using mt_smallint tags(123abc)")
            tdSql.error(f'create table st_smallint_e0 using mt_smallint tags("123abc")')
            tdSql.error(f"create table st_smallint_e0 using mt_smallint tags(abc)")
            tdSql.error(f'create table st_smallint_e0 using mt_smallint tags("abc")')
            tdSql.error(f'create table st_smallint_e0 using mt_smallint tags(" ")')
            tdSql.error(f"create table st_smallint_e0_1 using mt_smallint tags('')")
            tdSql.execute(f"create table st_smallint_e0_2 using mt_smallint tags('123')")

            tdSql.execute(f"create table st_smallint_e0 using mt_smallint tags(123)")
            tdSql.execute(f"create table st_smallint_e1 using mt_smallint tags(123)")
            tdSql.execute(f"create table st_smallint_e2 using mt_smallint tags(123)")
            tdSql.execute(f"create table st_smallint_e3 using mt_smallint tags(123)")
            tdSql.execute(f"create table st_smallint_e4 using mt_smallint tags(123)")
            tdSql.execute(f"create table st_smallint_e5 using mt_smallint tags(123)")
            tdSql.execute(f"create table st_smallint_e6 using mt_smallint tags(123)")
            tdSql.execute(f"create table st_smallint_e7 using mt_smallint tags(123)")
            tdSql.execute(f"create table st_smallint_e8 using mt_smallint tags(123)")
            tdSql.execute(f"create table st_smallint_e9 using mt_smallint tags(123)")
            tdSql.execute(f"create table st_smallint_e10 using mt_smallint tags(123)")
            tdSql.execute(f"create table st_smallint_e11 using mt_smallint tags(123)")
            tdSql.execute(f"create table st_smallint_e12 using mt_smallint tags(123)")

            tdSql.error(f"insert into st_smallint_e0 values(now, 32768)")
            tdSql.execute(f"insert into st_smallint_e1 values(now, -32768)")
            tdSql.error(f"insert into st_smallint_e2 values(now, 42768)")
            tdSql.error(f"insert into st_smallint_e3 values(now, -32769)")
            tdSql.error(f"insert into st_smallint_e6 values(now, 123abc)")
            tdSql.error(f'insert into st_smallint_e7 values(now, "123abc")')
            tdSql.error(f"insert into st_smallint_e9 values(now, abc)")
            tdSql.error(f'insert into st_smallint_e10 values(now, "abc")')
            tdSql.error(f'insert into st_smallint_e11 values(now, " ")')
            tdSql.error(f"insert into st_smallint_e12 values(now, '')")

            tdSql.error(
                f"insert into st_smallint_e13 using mt_smallint tags(033) values(now, 32768)"
            )
            tdSql.execute(
                f"insert into st_smallint_e14_1 using mt_smallint tags(033) values(now, -32768)"
            )
            tdSql.error(
                f"insert into st_smallint_e15 using mt_smallint tags(033) values(now, 32968)"
            )
            tdSql.error(
                f"insert into st_smallint_e16 using mt_smallint tags(033) values(now, -33768)"
            )
            tdSql.error(
                f"insert into st_smallint_e19 using mt_smallint tags(033) values(now, 123abc)"
            )
            tdSql.error(
                f'insert into st_smallint_e20 using mt_smallint tags(033) values(now, "123abc")'
            )
            tdSql.error(
                f"insert into st_smallint_e22 using mt_smallint tags(033) values(now, abc)"
            )
            tdSql.error(
                f'insert into st_smallint_e23 using mt_smallint tags(033) values(now, "abc")'
            )
            tdSql.error(
                f'insert into st_smallint_e24 using mt_smallint tags(033) values(now, " ")'
            )
            tdSql.error(
                f"insert into st_smallint_e25_1 using mt_smallint tags(033) values(now, '')"
            )

            tdSql.error(
                f"insert into st_smallint_e13 using mt_smallint tags(32768) values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_smallint_e14 using mt_smallint tags(-32768) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_smallint_e15 using mt_smallint tags(72768) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_smallint_e16 using mt_smallint tags(-92768) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_smallint_e19 using mt_smallint tags(123abc) values(now, -033)"
            )
            tdSql.error(
                f'insert into st_smallint_e20 using mt_smallint tags("123abc") values(now, -033)'
            )
            tdSql.error(
                f"insert into st_smallint_e22 using mt_smallint tags(abc) values(now, -033)"
            )
            tdSql.error(
                f'insert into st_smallint_e23 using mt_smallint tags("abc") values(now, -033)'
            )
            tdSql.error(
                f'insert into st_smallint_e24 using mt_smallint tags(" ") values(now, -033)'
            )
            tdSql.error(
                f"insert into st_smallint_e25 using mt_smallint tags('') values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_smallint_e26 using mt_smallint tags('123') values(now, -033)"
            )

            tdSql.execute(
                f"insert into st_smallint_e13 using mt_smallint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_smallint_e14 using mt_smallint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_smallint_e15 using mt_smallint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_smallint_e16 using mt_smallint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_smallint_e17 using mt_smallint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_smallint_e18 using mt_smallint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_smallint_e19 using mt_smallint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_smallint_e20 using mt_smallint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_smallint_e21 using mt_smallint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_smallint_e22 using mt_smallint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_smallint_e23 using mt_smallint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_smallint_e24 using mt_smallint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_smallint_e25 using mt_smallint tags(033) values(now, 00062)"
            )

            tdSql.error(f"alter table st_smallint_e13 set tag tagname=32768")
            tdSql.execute(f"alter table st_smallint_e14 set tag tagname=-32768")
            tdSql.error(f"alter table st_smallint_e15 set tag tagname=52768")
            tdSql.error(f"alter table st_smallint_e16 set tag tagname=-32778")
            tdSql.error(f"alter table st_smallint_e19 set tag tagname=123abc")
            tdSql.error(f'alter table st_smallint_e20 set tag tagname="123abc"')
            tdSql.error(f"alter table st_smallint_e22 set tag tagname=abc")
            tdSql.error(f'alter table st_smallint_e23 set tag tagname="abc"')
            tdSql.error(f'alter table st_smallint_e24 set tag tagname=" "')
            tdSql.error(f"alter table st_smallint_e25 set tag tagname=''")
            tdSql.execute(f"alter table st_smallint_e26 set tag tagname='123'")


    # ======================================================================
    # timestamp: timestamp column and tag values
    # ======================================================================

    def test_datatype_timestamp(self):
            """DataTypes: timestamp

            1. Create table
            2. Insert data
            3. Auto-create table
            4. Alter tag value
            5. Handle illegal input

            Since: v3.0.0.0

            Labels: common,ci,integration,functional

            Jira: None

            History:
                - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_timestamp.sim

            """
            self.timestamp_create_table()
            self.timestamp_insert_data()
            self.timestamp_auto_create_table()
            self.timestamp_alter_tag_value()
            self.timestamp_illegal_input()

    def timestamp_create_table(self):
            tdLog.info(f"create super table")
            tdSql.execute(
                f"create table mt_timestamp (ts timestamp, c timestamp) tags(tagname timestamp)"
            )

            tdLog.info(f"case 0: static create table for test tag values")

            tdSql.execute(f"create table st_timestamp_0 using mt_timestamp tags(NULL)")
            tdSql.execute(f"show create table st_timestamp_0")
            tdSql.query(f"show tags from st_timestamp_0")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_timestamp_1 using mt_timestamp tags(NULL)")
            tdSql.query(f"show tags from st_timestamp_1")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_timestamp_2 using mt_timestamp tags('NULL')")
            tdSql.query(f"show tags from st_timestamp_2")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_timestamp_3 using mt_timestamp tags('NULL')")
            tdSql.query(f"show tags from st_timestamp_3")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_timestamp_4 using mt_timestamp tags("NULL")')
            tdSql.query(f"show tags from st_timestamp_4")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_timestamp_5 using mt_timestamp tags("NULL")')
            tdSql.query(f"show tags from st_timestamp_5")
            tdSql.checkData(0, 5, None)

            tdSql.execute(
                f"create table st_timestamp_6 using mt_timestamp tags(-2147483647)"
            )
            tdSql.query(f"show tags from st_timestamp_6")
            tdSql.checkData(0, 5, -2147483647)

            tdSql.execute(
                f"create table st_timestamp_7 using mt_timestamp tags(2147483647)"
            )
            tdSql.query(f"show tags from st_timestamp_7")
            tdSql.checkData(0, 5, 2147483647)

            tdSql.execute(f"create table st_timestamp_8 using mt_timestamp tags(37)")
            tdSql.query(f"show tags from st_timestamp_8")
            tdSql.checkData(0, 5, 37)

            tdSql.execute(f"create table st_timestamp_9 using mt_timestamp tags(-100)")
            tdSql.query(f"show tags from st_timestamp_9")
            tdSql.checkData(0, 5, -100)

            tdSql.execute(f"create table st_timestamp_10 using mt_timestamp tags(+113)")
            tdSql.query(f"show tags from st_timestamp_10")
            tdSql.checkData(0, 5, 113)

            tdSql.execute(f"create table st_timestamp_11 using mt_timestamp tags('-100')")
            tdSql.query(f"show tags from st_timestamp_11")
            tdSql.checkData(0, 5, -100)

            tdSql.execute(f'create table st_timestamp_12 using mt_timestamp tags("-0")')
            tdSql.query(f"show tags from st_timestamp_12")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"create table st_timestamp_13 using mt_timestamp tags(+0078)")
            tdSql.query(f"show tags from st_timestamp_13")
            tdSql.checkData(0, 5, 78)

            tdSql.execute(f"create table st_timestamp_14 using mt_timestamp tags(-00078)")
            tdSql.query(f"show tags from st_timestamp_14")
            tdSql.checkData(0, 5, -78)

            tdSql.execute(f'create table st_timestamp_15 using mt_timestamp tags("0x01")')
            tdSql.query(f"show tags from st_timestamp_15")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'create table st_timestamp_16 using mt_timestamp tags("0b01")')
            tdSql.query(f"show tags from st_timestamp_16")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'create table st_timestamp_17 using mt_timestamp tags("+0x01")')
            tdSql.query(f"show tags from st_timestamp_17")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'create table st_timestamp_18 using mt_timestamp tags("-0b01")')
            tdSql.query(f"show tags from st_timestamp_18")
            tdSql.checkData(0, 5, -1)

            tdSql.execute(f"create table st_timestamp_19 using mt_timestamp tags( 0x01)")
            tdSql.query(f"show tags from st_timestamp_19")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"create table st_timestamp_20 using mt_timestamp tags(0b01 )")
            tdSql.query(f"show tags from st_timestamp_20")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"create table st_timestamp_21 using mt_timestamp tags(+0x01)")
            tdSql.query(f"show tags from st_timestamp_21")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"create table st_timestamp_22 using mt_timestamp tags( -0b01 )")
            tdSql.query(f"show tags from st_timestamp_22")
            tdSql.checkData(0, 5, -1)

            tdSql.execute(f"create table st_timestamp_23 using mt_timestamp tags(1+ 1d )")
            tdSql.query(f"show tags from st_timestamp_23")
            tdSql.checkData(0, 5, 86400001)

            tdSql.execute(f"create table st_timestamp_24 using mt_timestamp tags(-0 + 1d)")
            tdSql.query(f"show tags from st_timestamp_24")
            tdSql.checkData(0, 5, 86400000)

            tdSql.execute(f'create table st_timestamp_25 using mt_timestamp tags("-0" -1s)')
            tdSql.query(f"show tags from st_timestamp_25")
            tdSql.checkData(0, 5, -1000)

            tdSql.execute(f"create table st_timestamp_26 using mt_timestamp tags(0b01 -1a)")
            tdSql.query(f"show tags from st_timestamp_26")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"create table st_timestamp_27 using mt_timestamp tags(0b01 -1s)")
            tdSql.query(f"show tags from st_timestamp_27")
            tdSql.checkData(0, 5, -999)

            tdSql.execute(
                f'create table st_timestamp_28 using mt_timestamp tags("0x01" +1u)'
            )
            tdSql.query(f"show tags from st_timestamp_28")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"create table st_timestamp_29 using mt_timestamp tags(0x01 +1b)")
            tdSql.query(f"show tags from st_timestamp_29")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(
                f"create table st_timestamp_30 using mt_timestamp tags(-0b00 -0a)"
            )
            tdSql.query(f"show tags from st_timestamp_30")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(
                f'create table st_timestamp_31 using mt_timestamp tags("-0x00" +1u)'
            )
            tdSql.query(f"show tags from st_timestamp_31")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(
                f"create table st_timestamp_32 using mt_timestamp tags(-0x00 +1b)"
            )
            tdSql.query(f"show tags from st_timestamp_32")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"create table st_timestamp_33 using mt_timestamp tags(now +1b)")
            tdSql.query(f"show tags from st_timestamp_33")
            tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

            tdSql.execute(
                f'create table st_timestamp_34 using mt_timestamp tags("now()" +1b)'
            )
            tdSql.query(f"show tags from st_timestamp_34")
            tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

            tdSql.execute(
                f"create table st_timestamp_35 using mt_timestamp tags(today() +1d)"
            )
            tdSql.query(f"show tags from st_timestamp_35")
            tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

            tdSql.execute(
                f'create table st_timestamp_36 using mt_timestamp tags("today()" +1d)'
            )
            tdSql.query(f"show tags from st_timestamp_36")
            tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

    def timestamp_insert_data(self):
            tdLog.info(f"case 1: insert values for test column values")

            tdSql.execute(f"insert into st_timestamp_0 values(now,NULL)")
            tdSql.query(f"select * from st_timestamp_0")
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_timestamp_1 values(now,NULL)")
            tdSql.query(f"select * from st_timestamp_1")
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_timestamp_2 values(now,'NULL')")
            tdSql.query(f"select * from st_timestamp_2")
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_timestamp_3 values(now,'NULL')")
            tdSql.query(f"select * from st_timestamp_3")
            tdSql.checkData(0, 1, None)

            tdSql.execute(f'insert into st_timestamp_4 values(now,"NULL")')
            tdSql.query(f"select * from st_timestamp_4")
            tdSql.checkData(0, 1, None)

            tdSql.execute(f'insert into st_timestamp_5 values(now,"NULL")')
            tdSql.query(f"select * from st_timestamp_5")
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_timestamp_6 values(now,-2147483647)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_6")
            tdSql.checkData(0, 1, -2147483647)

            tdSql.execute(f"insert into st_timestamp_7 values(now,2147483647)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_7")
            tdSql.checkData(0, 1, 2147483647)

            tdSql.execute(f"insert into st_timestamp_8 values(now,37)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_8")
            tdSql.checkData(0, 1, 37)

            tdSql.execute(f"insert into st_timestamp_9 values(now,-100)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_9")
            tdSql.checkData(0, 1, -100)

            tdSql.execute(f"insert into st_timestamp_10 values(now,+113)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_10")
            tdSql.checkData(0, 1, 113)

            tdSql.execute(f"insert into st_timestamp_11 values(now,'-100')")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_11")
            tdSql.checkData(0, 1, -100)

            tdSql.execute(f'insert into st_timestamp_12 values(now,"-0")')
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_12")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_timestamp_13 values(now,+0078)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_13")
            tdSql.checkData(0, 1, 78)

            tdSql.execute(f"insert into st_timestamp_14 values(now,-00078)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_14")
            tdSql.checkData(0, 1, -78)

            tdSql.execute(f'insert into st_timestamp_15 values(now,"0x01")')
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_15")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_timestamp_16 values(now,"0b01")')
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_16")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_timestamp_17 values(now,"+0x01")')
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_17")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_timestamp_18 values(now,"-0b01")')
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_18")
            tdSql.checkData(0, 1, -1)

            tdSql.execute(f"insert into st_timestamp_19 values(now, 0x01)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_19")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_timestamp_20 values(now,0b01 )")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_20")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_timestamp_21 values(now,+0x01)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_21")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_timestamp_22 values(now, -0b01 )")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_22")
            tdSql.checkData(0, 1, -1)

            tdSql.execute(f"insert into st_timestamp_23 values(now,1+ 1d )")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_23")
            tdSql.checkData(0, 1, 86400001)

            tdSql.execute(f"insert into st_timestamp_24 values(now,-0 + 1d)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_24")
            tdSql.checkData(0, 1, 86400000)

            tdSql.execute(f'insert into st_timestamp_25 values(now,"-0" -1s)')
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_25")
            tdSql.checkData(0, 1, -1000)

            tdSql.execute(f"insert into st_timestamp_26 values(now,0b01 -1a)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_26")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_timestamp_27 values(now,+0b01 -1s)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_27")
            tdSql.checkData(0, 1, -999)

            tdSql.execute(f'insert into st_timestamp_28 values(now,"+0x01" +1u)')
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_28")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_timestamp_29 values(now,0x01 +1b)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_29")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into  st_timestamp_30 values(now,-0b00 -0a)")
            tdSql.query(f"show tags from st_timestamp_30")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f'insert into  st_timestamp_31 values(now,"-0x00" +1u)')
            tdSql.query(f"show tags from st_timestamp_31")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"insert into st_timestamp_32 values(now,-0x00 +1b)")
            tdSql.query(f"show tags from st_timestamp_32")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"insert into st_timestamp_33 values(now,now +1b)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_33")
            tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

            tdSql.execute(f'insert into st_timestamp_34 values(now,"now()" +1b)')
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_34")
            tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

            tdSql.execute(f"insert into st_timestamp_35 values(now,today() +1d)")
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_35")
            tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

            tdSql.execute(f'insert into st_timestamp_36 values(now,"today()" +1d)')
            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_36")
            tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

    def timestamp_auto_create_table(self):
            tdLog.info(f"case 2: dynamic create table for test tag values")
            tdSql.execute(
                f"insert into st_timestamp_100 using mt_timestamp tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show tags from st_timestamp_100")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_timestamp_100")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_timestamp_101 using mt_timestamp tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show tags from st_timestamp_101")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_timestamp_101")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_timestamp_102 using mt_timestamp tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_timestamp_102")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_timestamp_102")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_timestamp_103 using mt_timestamp tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_timestamp_103")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_timestamp_103")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_timestamp_104 using mt_timestamp tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_timestamp_104")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_timestamp_104")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_timestamp_105 using mt_timestamp tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_timestamp_105")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_timestamp_105")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_timestamp_106 using mt_timestamp tags(-2147483647) values(now, -2147483647)"
            )
            tdSql.query(f"show tags from st_timestamp_106")
            tdSql.checkData(0, 5, -2147483647)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_106")
            tdSql.checkData(0, 1, -2147483647)

            tdSql.execute(
                f"insert into st_timestamp_107 using mt_timestamp tags(2147483647) values(now, 2147483647)"
            )
            tdSql.query(f"show tags from st_timestamp_107")
            tdSql.checkData(0, 5, 2147483647)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_107")
            tdSql.checkData(0, 1, 2147483647)

            tdSql.execute(
                f"insert into st_timestamp_108 using mt_timestamp tags(37) values(now, 37)"
            )
            tdSql.query(f"show tags from st_timestamp_108")
            tdSql.checkData(0, 5, 37)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_108")
            tdSql.checkData(0, 1, 37)

            tdSql.execute(
                f"insert into st_timestamp_109 using mt_timestamp tags(-100) values(now, -100)"
            )
            tdSql.query(f"show tags from st_timestamp_109")
            tdSql.checkData(0, 5, -100)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_109")
            tdSql.checkData(0, 1, -100)

            tdSql.execute(
                f"insert into st_timestamp_1010 using mt_timestamp tags(+113) values(now, +113)"
            )
            tdSql.query(f"show tags from st_timestamp_1010")
            tdSql.checkData(0, 5, 113)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1010")
            tdSql.checkData(0, 1, 113)

            tdSql.execute(
                f"insert into st_timestamp_1011 using mt_timestamp tags('-100') values(now, '-100')"
            )
            tdSql.query(f"show tags from st_timestamp_1011")
            tdSql.checkData(0, 5, -100)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1011")
            tdSql.checkData(0, 1, -100)

            tdSql.execute(
                f'insert into st_timestamp_1012 using mt_timestamp tags("-0") values(now, "-0")'
            )
            tdSql.query(f"show tags from st_timestamp_1012")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1012")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_timestamp_1013 using mt_timestamp tags(+0078) values(now, +0078)"
            )
            tdSql.query(f"show tags from st_timestamp_1013")
            tdSql.checkData(0, 5, 78)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1013")
            tdSql.checkData(0, 1, 78)

            tdSql.execute(
                f"insert into st_timestamp_1014 using mt_timestamp tags(-00078) values(now, -00078)"
            )
            tdSql.query(f"show tags from st_timestamp_1014")
            tdSql.checkData(0, 5, -78)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1014")
            tdSql.checkData(0, 1, -78)

            tdSql.execute(
                f'insert into st_timestamp_1015 using mt_timestamp tags("0x01") values(now, "0x01")'
            )
            tdSql.query(f"show tags from st_timestamp_1015")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1015")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_timestamp_1016 using mt_timestamp tags("0b01") values(now, "0b01")'
            )
            tdSql.query(f"show tags from st_timestamp_1016")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1016")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_timestamp_1017 using mt_timestamp tags("+0x01") values(now, "+0x01")'
            )
            tdSql.query(f"show tags from st_timestamp_1017")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(
                f'insert into st_timestamp_1018 using mt_timestamp tags("-0b01") values(now, "-0b01")'
            )
            tdSql.query(f"show tags from st_timestamp_1018")
            tdSql.checkData(0, 5, -1)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1018")
            tdSql.checkData(0, 1, -1)

            tdSql.execute(
                f"insert into st_timestamp_1019 using mt_timestamp tags( 0x01) values(now, 0x01)"
            )
            tdSql.query(f"show tags from st_timestamp_1019")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(
                f"insert into st_timestamp_1020 using mt_timestamp tags(0b01 ) values(now, 0b01 )"
            )
            tdSql.query(f"show tags from st_timestamp_1020")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1020")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_timestamp_1021 using mt_timestamp tags(+0x01) values(now, +0x01)"
            )
            tdSql.query(f"show tags from st_timestamp_1021")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1021")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_timestamp_1022 using mt_timestamp tags( -0b01 ) values(now,  -0b01)"
            )
            tdSql.query(f"show tags from st_timestamp_1022")
            tdSql.checkData(0, 5, -1)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1022")
            tdSql.checkData(0, 1, -1)

            tdSql.execute(
                f"insert into st_timestamp_1023 using mt_timestamp tags(+1+1d) values(now,+1+ 1d )"
            )
            tdSql.query(f"show tags from st_timestamp_1023")
            tdSql.checkData(0, 5, 86400001)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1023")
            tdSql.checkData(0, 1, 86400001)

            tdSql.execute(
                f"insert into st_timestamp_1024 using mt_timestamp tags(-0+1d) values(now,-0 + 1d)"
            )
            tdSql.query(f"show tags from st_timestamp_1024")
            tdSql.checkData(0, 5, 86400000)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1024")
            tdSql.checkData(0, 1, 86400000)

            tdSql.execute(
                f'insert into st_timestamp_1025 using mt_timestamp tags("-0" -1s) values(now,"-0" -1s)'
            )
            tdSql.query(f"show tags from st_timestamp_1025")
            tdSql.checkData(0, 5, -1000)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1025")
            tdSql.checkData(0, 1, -1000)

            tdSql.execute(
                f"insert into st_timestamp_1026 using mt_timestamp tags(+0b01-1a) values(now,+0b01 -1a)"
            )
            tdSql.query(f"show tags from st_timestamp_1026")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1026")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_timestamp_1027 using mt_timestamp tags(0b01-1s) values(now,0b01 -1s)"
            )
            tdSql.query(f"show tags from st_timestamp_1027")
            tdSql.checkData(0, 5, -999)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1027")
            tdSql.checkData(0, 1, -999)

            tdSql.execute(
                f'insert into st_timestamp_1028 using mt_timestamp tags("0x01" + 1u) values(now,"0x01" +1u)'
            )
            tdSql.query(f"show tags from st_timestamp_1028")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1028")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_timestamp_1029 using mt_timestamp tags(+0x01 +1b) values(now,+0x01 +1b)"
            )
            tdSql.query(f"show tags from st_timestamp_1029")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1029")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_timestamp_1030 using mt_timestamp tags(-0b00 -0a) values(now,-0b00 -0a)"
            )
            tdSql.query(f"show tags from st_timestamp_1030")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1030")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f'insert into st_timestamp_1031 using mt_timestamp tags("-0x00" +1u) values(now,"-0x00" +1u)'
            )
            tdSql.query(f"show tags from st_timestamp_1031")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1031")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_timestamp_1032 using mt_timestamp tags(-0x00 +1b) values(now,-0x00 +1b)"
            )
            tdSql.query(f"show tags from st_timestamp_1032")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1032")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_timestamp_1033 using mt_timestamp tags(now+1b) values(now,now +1b)"
            )
            tdSql.query(f"show tags from st_timestamp_1033")
            tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1033")
            tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

            tdSql.execute(
                f'insert into st_timestamp_1034 using mt_timestamp tags("now" +1b) values(now,"now()" +1b)'
            )
            tdSql.query(f"show tags from st_timestamp_1034")
            tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1034")
            tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

            tdSql.execute(
                f"insert into st_timestamp_1035 using mt_timestamp tags(today() + 1d) values(now,today() +1d)"
            )
            tdSql.query(f"show tags from st_timestamp_1035")
            tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1035")
            tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

            tdSql.execute(
                f'insert into st_timestamp_1036 using mt_timestamp tags("today" +1d) values(now,"today()" +1d)'
            )
            tdSql.query(f"show tags from st_timestamp_1036")
            tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

            tdSql.query(f"select ts, cast(c as bigint) from st_timestamp_1036")
            tdSql.checkAssert(int(tdSql.getData(0, 1)) >= 1711883186000)

    def timestamp_alter_tag_value(self):
            tdLog.info(f"case 3: alter tag value")
            tdSql.execute(f"alter table st_timestamp_0 set tag tagname=NULL")
            tdSql.query(f"show tags from st_timestamp_0")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"alter table st_timestamp_1 set tag tagname=NULL")
            tdSql.query(f"show tags from st_timestamp_1")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"alter table st_timestamp_2 set tag tagname='NULL'")
            tdSql.query(f"show tags from st_timestamp_2")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"alter table st_timestamp_3 set tag tagname='NULL'")
            tdSql.query(f"show tags from st_timestamp_3")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'alter table st_timestamp_4 set tag tagname="NULL"')
            tdSql.query(f"show tags from st_timestamp_4")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'alter table st_timestamp_5 set tag tagname="NULL"')
            tdSql.query(f"show tags from st_timestamp_5")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"alter table st_timestamp_6 set tag tagname=-2147483647")
            tdSql.query(f"show tags from st_timestamp_6")
            tdSql.checkData(0, 5, -2147483647)

            tdSql.execute(f"alter table st_timestamp_7 set tag tagname=2147483647")
            tdSql.query(f"show tags from st_timestamp_7")
            tdSql.checkData(0, 5, 2147483647)

            tdSql.execute(f"alter table st_timestamp_8 set tag tagname=37")
            tdSql.query(f"show tags from st_timestamp_8")
            tdSql.checkData(0, 5, 37)

            tdSql.execute(f"alter table st_timestamp_9 set tag tagname=-100")
            tdSql.query(f"show tags from st_timestamp_9")
            tdSql.checkData(0, 5, -100)

            tdSql.execute(f"alter table st_timestamp_10 set tag tagname=+113")
            tdSql.query(f"show tags from st_timestamp_10")
            tdSql.checkData(0, 5, 113)

            tdSql.execute(f"alter table st_timestamp_11 set tag tagname='-100'")
            tdSql.query(f"show tags from st_timestamp_11")
            tdSql.checkData(0, 5, -100)

            tdSql.execute(f'alter table st_timestamp_12 set tag tagname="-0"')
            tdSql.query(f"show tags from st_timestamp_12")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"alter table st_timestamp_13 set tag tagname=+0078")
            tdSql.query(f"show tags from st_timestamp_13")
            tdSql.checkData(0, 5, 78)

            tdSql.execute(f"alter table st_timestamp_14 set tag tagname=-00078")
            tdSql.query(f"show tags from st_timestamp_14")
            tdSql.checkData(0, 5, -78)

            tdSql.execute(f'alter table st_timestamp_15 set tag tagname="0x01"')
            tdSql.query(f"show tags from st_timestamp_15")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'alter table st_timestamp_16 set tag tagname="0b01"')
            tdSql.query(f"show tags from st_timestamp_16")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'alter table st_timestamp_17 set tag tagname="+0x01"')
            tdSql.query(f"show tags from st_timestamp_17")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'alter table st_timestamp_18 set tag tagname="-0b01"')
            tdSql.query(f"show tags from st_timestamp_18")
            tdSql.checkData(0, 5, -1)

            tdSql.execute(f"alter table st_timestamp_19 set tag tagname= 0x01")
            tdSql.query(f"show tags from st_timestamp_19")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"alter table st_timestamp_20 set tag tagname=0b01 ")
            tdSql.query(f"show tags from st_timestamp_20")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"alter table st_timestamp_21 set tag tagname=+0x01")
            tdSql.query(f"show tags from st_timestamp_21")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"alter table st_timestamp_22 set tag tagname= -0b01 ")
            tdSql.query(f"show tags from st_timestamp_22")
            tdSql.checkData(0, 5, -1)

            tdSql.execute(f"alter table st_timestamp_23 set tag tagname=1+ 1d ")
            tdSql.query(f"show tags from st_timestamp_23")
            tdSql.checkData(0, 5, 86400001)

            tdSql.execute(f"alter table st_timestamp_24 set tag tagname=-0 + 1d")
            tdSql.query(f"show tags from st_timestamp_24")
            tdSql.checkData(0, 5, 86400000)

            tdSql.execute(f'alter table st_timestamp_25 set tag tagname="-0" -1s')
            tdSql.query(f"show tags from st_timestamp_25")
            tdSql.checkData(0, 5, -1000)

            tdSql.execute(f"alter table st_timestamp_26 set tag tagname=+0b01 -1a")
            tdSql.query(f"show tags from st_timestamp_26")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"alter table st_timestamp_27 set tag tagname=0b01 -1s")
            tdSql.query(f"show tags from st_timestamp_27")
            tdSql.checkData(0, 5, -999)

            tdSql.execute(f'alter table st_timestamp_28 set tag tagname="0x01" +1u')
            tdSql.query(f"show tags from st_timestamp_28")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"alter table st_timestamp_29 set tag tagname=0x01 +1b")
            tdSql.query(f"show tags from st_timestamp_29")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"alter table st_timestamp_30 set tag tagname==-0b00 -0a")
            tdSql.query(f"show tags from st_timestamp_30")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f'alter table st_timestamp_31 set tag tagname="-0x00" +1u')
            tdSql.query(f"show tags from st_timestamp_31")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"alter table st_timestamp_32 set tag tagname=-0x00 +1b")
            tdSql.query(f"show tags from st_timestamp_32")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"alter table st_timestamp_33 set tag tagname=now +1b")
            tdSql.query(f"show tags from st_timestamp_33")
            tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

            tdSql.execute(f'alter table st_timestamp_34 set tag tagname="now()" +1b')
            tdSql.query(f"show tags from st_timestamp_34")
            tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

            tdSql.execute(f"alter table st_timestamp_35 set tag tagname=today( ) +1d")
            tdSql.query(f"show tags from st_timestamp_35")
            tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

            tdSql.execute(f'alter table st_timestamp_36 set tag tagname="today()" +1d')
            tdSql.query(f"show tags from st_timestamp_36")
            tdSql.checkAssert(int(tdSql.getData(0, 5)) >= 1711883186000)

    def timestamp_illegal_input(self):
            tdLog.info(f"case 4: illegal input")

            tdSql.error(f"create table st_timestamp_e0 using mt_timestamp tags(123abc)")
            tdSql.error(f'create table st_timestamp_e0 using mt_timestamp tags("123abc")')
            tdSql.error(f"create table st_timestamp_e0 using mt_timestamp tags(abc)")
            tdSql.error(f'create table st_timestamp_e0 using mt_timestamp tags("abc")')
            tdSql.error(
                f"create table st_timestamp_e0 using mt_timestamp tags(now()+1d+1s)"
            )
            tdSql.error(f"create table st_timestamp_e0 using mt_timestamp tags(1+1y)")
            tdSql.error(f"create table st_timestamp_e0 using mt_timestamp tags(0x01+1b+1a)")
            tdSql.error(f'create table st_timestamp_e0 using mt_timestamp tags(" ")')
            tdSql.error(f"create table st_timestamp_e0 using mt_timestamp tags('')")
            tdSql.error(f'create table st_timestamp_104 using mt_timestamp tags("-123.1")')
            tdSql.error(f'create table st_timestamp_105 using mt_timestamp tags("+123.5")')
            tdSql.error(f'create table st_timestamp_106 using mt_timestamp tags("-1e-1")')
            tdSql.error(
                f'create table st_timestamp_107 using mt_timestamp tags("+0.1235e3")'
            )
            tdSql.error(
                f'create table st_timestamp_108 using mt_timestamp tags("-0.11e-30")'
            )
            tdSql.error(
                f'create table st_timestamp_109 using mt_timestamp tags("-1.1e-307")'
            )
            tdSql.error(f"create table st_timestamp_110 using mt_timestamp tags( -1e-1 )")
            tdSql.error(
                f"create table st_timestamp_111 using mt_timestamp tags( +0.1235e3 )"
            )
            tdSql.error(f"create table st_timestamp_112 using mt_timestamp tags(-0.11e-30)")
            tdSql.error(f"create table st_timestamp_113 using mt_timestamp tags(-1.1e-307)")
            tdSql.execute(
                f"create table st_timestamp_114 using mt_timestamp tags(9223372036854775807)"
            )
            tdSql.error(
                f"create table st_timestamp_115 using mt_timestamp tags(9223372036854775808)"
            )
            tdSql.execute(
                f"create table st_timestamp_116 using mt_timestamp tags(-9223372036854775808)"
            )
            tdSql.error(
                f"create table st_timestamp_117 using mt_timestamp tags(-9223372036854775809)"
            )
            tdSql.error(
                f"insert into st_timestamp_118 using mt_timestamp tags(9223372036854775807) values(9223372036854775807, 9223372036854775807)"
            )
            tdSql.error(
                f"insert into st_timestamp_119 using mt_timestamp tags(1+1s-1s) values(now, now)"
            )
            tdSql.error(
                f"insert into st_timestamp_120 using mt_timestamp tags(1-1s) values(now, now-1s+1d)"
            )


    # ======================================================================
    # tinyint: tinyint column and tag values
    # ======================================================================

    def test_datatype_tinyint(self):
            """DataTypes: tinyint

            1. Create table
            2. Insert data
            3. Auto-create table
            4. Alter tag value
            5. Handle illegal input

            Since: v3.0.0.0

            Labels: common,ci,integration,functional

            Jira: None

            History:
                - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_tinyint.sim

            """
            self.tinyint_create_table()
            self.tinyint_insert_data()
            self.tinyint_auto_create_table()
            self.tinyint_alter_tag_value()
            self.tinyint_illegal_input()

    def tinyint_create_table(self):
            tdLog.info(f"create super table")
            tdSql.execute(
                f"create table mt_tinyint (ts timestamp, c tinyint) tags(tagname tinyint)"
            )

            tdLog.info(f"case 0: static create table for test tag values")
            tdSql.execute(f"create table st_tinyint_0 using mt_tinyint tags(NULL)")
            tdSql.query(f"show create table st_tinyint_0")
            tdSql.query(f"show tags from st_tinyint_0")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_tinyint_1 using mt_tinyint tags(NULL)")
            tdSql.query(f"show tags from st_tinyint_1")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_tinyint_2 using mt_tinyint tags('NULL')")
            tdSql.query(f"show tags from st_tinyint_2")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_tinyint_3 using mt_tinyint tags('NULL')")
            tdSql.query(f"show tags from st_tinyint_3")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_tinyint_4 using mt_tinyint tags("NULL")')
            tdSql.query(f"show tags from st_tinyint_4")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_tinyint_5 using mt_tinyint tags("NULL")')
            tdSql.query(f"show tags from st_tinyint_5")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_tinyint_6 using mt_tinyint tags(-127)")
            tdSql.query(f"show tags from st_tinyint_6")
            tdSql.checkData(0, 5, -127)

            tdSql.execute(f"create table st_tinyint_7 using mt_tinyint tags(127)")
            tdSql.query(f"show tags from st_tinyint_7")
            tdSql.checkData(0, 5, 127)

            tdSql.execute(f"create table st_tinyint_8 using mt_tinyint tags(37)")
            tdSql.query(f"show tags from st_tinyint_8")
            tdSql.checkData(0, 5, 37)

            tdSql.execute(f"create table st_tinyint_9 using mt_tinyint tags(-100)")
            tdSql.query(f"show tags from st_tinyint_9")
            tdSql.checkData(0, 5, -100)

            tdSql.execute(f"create table st_tinyint_10 using mt_tinyint tags(+113)")
            tdSql.query(f"show tags from st_tinyint_10")
            tdSql.checkData(0, 5, 113)

            tdSql.execute(f"create table st_tinyint_11 using mt_tinyint tags('-100')")
            tdSql.query(f"show tags from st_tinyint_11")
            tdSql.checkData(0, 5, -100)

            tdSql.execute(f'create table st_tinyint_12 using mt_tinyint tags("+78")')
            tdSql.query(f"show tags from st_tinyint_12")
            tdSql.checkData(0, 5, 78)

            tdSql.execute(f"create table st_tinyint_13 using mt_tinyint tags(+0078)")
            tdSql.query(f"show tags from st_tinyint_13")
            tdSql.checkData(0, 5, 78)

            tdSql.execute(f"create table st_tinyint_14 using mt_tinyint tags(-00078)")
            tdSql.query(f"show tags from st_tinyint_14")
            tdSql.checkData(0, 5, -78)

    def tinyint_insert_data(self):
            tdLog.info(f"case 1: insert values for test column values")

            tdSql.execute(f"insert into st_tinyint_0 values(now, NULL)")
            tdSql.query(f"select * from st_tinyint_0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_tinyint_1 values(now, NULL)")
            tdSql.query(f"select * from st_tinyint_1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_tinyint_2 values(now, NULL)")
            tdSql.query(f"select * from st_tinyint_2")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_tinyint_3 values(now, NULL)")
            tdSql.query(f"select * from st_tinyint_3")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_tinyint_4 values(now, NULL)")
            tdSql.query(f"select * from st_tinyint_4")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_tinyint_5 values(now, NULL)")
            tdSql.query(f"select * from st_tinyint_5")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_tinyint_6 values(now, 127)")
            tdSql.query(f"select * from st_tinyint_6")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 127)

            tdSql.execute(f"insert into st_tinyint_7 values(now, -127)")
            tdSql.query(f"select * from st_tinyint_7")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -127)

            tdSql.execute(f"insert into st_tinyint_8 values(now, +100)")
            tdSql.query(f"select * from st_tinyint_8")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 100)

            tdSql.execute(f'insert into st_tinyint_9 values(now, "-098")')
            tdSql.query(f"select * from st_tinyint_9")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -98)

            tdSql.execute(f"insert into st_tinyint_10 values(now, '0')")
            tdSql.query(f"select * from st_tinyint_10")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_tinyint_11 values(now, -0)")
            tdSql.query(f"select * from st_tinyint_11")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f'insert into st_tinyint_12 values(now, "+056")')
            tdSql.query(f"select * from st_tinyint_12")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 56)

            tdSql.execute(f"insert into st_tinyint_13 values(now, +056)")
            tdSql.query(f"select * from st_tinyint_13")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 56)

            tdSql.execute(f"insert into st_tinyint_14 values(now, -056)")
            tdSql.query(f"select * from st_tinyint_14")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, -56)

    def tinyint_auto_create_table(self):
            tdLog.info(f"case 2: dynamic create table for test tag values")

            tdSql.execute(
                f"insert into st_tinyint_16 using mt_tinyint tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show create table st_tinyint_16")
            tdSql.query(f"show tags from st_tinyint_16")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_tinyint_16")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_tinyint_17 using mt_tinyint tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show tags from st_tinyint_17")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_tinyint_17")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_tinyint_18 using mt_tinyint tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_tinyint_18")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_tinyint_18")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_tinyint_19 using mt_tinyint tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_tinyint_19")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_tinyint_19")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_tinyint_20 using mt_tinyint tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_tinyint_20")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_tinyint_20")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_tinyint_21 using mt_tinyint tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_tinyint_21")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_tinyint_21")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_tinyint_22 using mt_tinyint tags(127) values(now, 127)"
            )
            tdSql.query(f"show tags from st_tinyint_22")
            tdSql.checkData(0, 5, 127)

            tdSql.query(f"select * from st_tinyint_22")
            tdSql.checkData(0, 1, 127)

            tdSql.execute(
                f"insert into st_tinyint_23 using mt_tinyint tags(-127) values(now, -127)"
            )
            tdSql.query(f"show tags from st_tinyint_23")
            tdSql.checkData(0, 5, -127)

            tdSql.query(f"select * from st_tinyint_23")
            tdSql.checkData(0, 1, -127)

            tdSql.execute(
                f"insert into st_tinyint_24 using mt_tinyint tags(10) values(now, 10)"
            )
            tdSql.query(f"show tags from st_tinyint_24")
            tdSql.checkData(0, 5, 10)

            tdSql.query(f"select * from st_tinyint_24")
            tdSql.checkData(0, 1, 10)

            tdSql.execute(
                f'insert into st_tinyint_25 using mt_tinyint tags("-0") values(now, "-0")'
            )
            tdSql.query(f"show tags from st_tinyint_25")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_tinyint_25")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_tinyint_26 using mt_tinyint tags('123') values(now, '123')"
            )
            tdSql.query(f"show tags from st_tinyint_26")
            tdSql.checkData(0, 5, 123)

            tdSql.query(f"select * from st_tinyint_26")
            tdSql.checkData(0, 1, 123)

            tdSql.execute(
                f"insert into st_tinyint_27 using mt_tinyint tags(+056) values(now, +00056)"
            )
            tdSql.query(f"show tags from st_tinyint_27")
            tdSql.checkData(0, 5, 56)

            tdSql.query(f"select * from st_tinyint_27")
            tdSql.checkData(0, 1, 56)

            tdSql.execute(
                f"insert into st_tinyint_28 using mt_tinyint tags(-056) values(now, -0056)"
            )
            tdSql.query(f"show tags from st_tinyint_28")
            tdSql.checkData(0, 5, -56)

            tdSql.query(f"select * from st_tinyint_28")
            tdSql.checkData(0, 1, -56)

    def tinyint_alter_tag_value(self):
            tdLog.info(f"case 3: alter tag value")

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname=127")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, 127)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname=-127")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, -127)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname=+100")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, 100)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname=-33")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, -33)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname='+98'")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, 98)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname='-076'")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, -76)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname=+0012")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, 12)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname=-00063")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, -63)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname=127")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, 127)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname=-127")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, -127)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname=+100")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, 100)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname=-33")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, -33)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname='+98'")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, 98)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname='-076'")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, -76)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname=+0012")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, 12)

            tdSql.execute(f"alter table st_tinyint_0 set tag tagname=-00063")
            tdSql.query(f"show tags from  st_tinyint_0")
            tdSql.checkData(0, 5, -63)

    def tinyint_illegal_input(self):
            tdLog.info(f"case 4: illegal input")

            tdSql.error(f"create table st_tinyint_e0 using mt_tinyint tags(128)")
            tdSql.execute(f"create table st_tinyint_e0_1 using mt_tinyint tags(-128)")
            tdSql.error(f"create table st_tinyint_e0 using mt_tinyint tags(1280)")
            tdSql.error(f"create table st_tinyint_e0 using mt_tinyint tags(-1280)")
            tdSql.error(f"create table st_tinyint_e0 using mt_tinyint tags(123abc)")
            tdSql.error(f'create table st_tinyint_e0 using mt_tinyint tags("123abc")')
            tdSql.error(f"create table st_tinyint_e0 using mt_tinyint tags(abc)")
            tdSql.error(f'create table st_tinyint_e0 using mt_tinyint tags("abc")')
            tdSql.error(f'create table st_tinyint_e0 using mt_tinyint tags(" ")')
            tdSql.error(f"create table st_tinyint_e0_2 using mt_tinyint tags('')")

            tdSql.execute(f"create table st_tinyint_e0 using mt_tinyint tags(123)")
            tdSql.execute(f"create table st_tinyint_e1 using mt_tinyint tags(123)")
            tdSql.execute(f"create table st_tinyint_e2 using mt_tinyint tags(123)")
            tdSql.execute(f"create table st_tinyint_e3 using mt_tinyint tags(123)")
            tdSql.execute(f"create table st_tinyint_e4 using mt_tinyint tags(123)")
            tdSql.execute(f"create table st_tinyint_e5 using mt_tinyint tags(123)")
            tdSql.execute(f"create table st_tinyint_e6 using mt_tinyint tags(123)")
            tdSql.execute(f"create table st_tinyint_e7 using mt_tinyint tags(123)")
            tdSql.execute(f"create table st_tinyint_e8 using mt_tinyint tags(123)")
            tdSql.execute(f"create table st_tinyint_e9 using mt_tinyint tags(123)")
            tdSql.execute(f"create table st_tinyint_e10 using mt_tinyint tags(123)")
            tdSql.execute(f"create table st_tinyint_e11 using mt_tinyint tags(123)")
            tdSql.execute(f"create table st_tinyint_e12 using mt_tinyint tags(123)")

            tdSql.error(f"insert into st_tinyint_e0 values(now, 128)")
            tdSql.execute(f"insert into st_tinyint_e1 values(now, -128)")
            tdSql.error(f"insert into st_tinyint_e2 values(now, 1280)")
            tdSql.error(f"insert into st_tinyint_e3 values(now, -1280)")
            tdSql.error(f"insert into st_tinyint_e6 values(now, 123abc)")
            tdSql.error(f'insert into st_tinyint_e7 values(now, "123abc")')
            tdSql.error(f"insert into st_tinyint_e9 values(now, abc)")
            tdSql.error(f'insert into st_tinyint_e10 values(now, "abc")')
            tdSql.error(f'insert into st_tinyint_e11 values(now, " ")')
            tdSql.error(f"insert into st_tinyint_e12 values(now, '')")

            tdSql.error(
                f"insert into st_tinyint_e13 using mt_tinyint tags(033) values(now, 128)"
            )
            tdSql.execute(
                f"insert into st_tinyint_e14_1 using mt_tinyint tags(033) values(now, -128)"
            )
            tdSql.error(
                f"insert into st_tinyint_e15 using mt_tinyint tags(033) values(now, 1280)"
            )
            tdSql.error(
                f"insert into st_tinyint_e16 using mt_tinyint tags(033) values(now, -1280)"
            )
            tdSql.error(
                f"insert into st_tinyint_e19 using mt_tinyint tags(033) values(now, 123abc)"
            )
            tdSql.error(
                f'insert into st_tinyint_e20 using mt_tinyint tags(033) values(now, "123abc")'
            )
            tdSql.error(
                f"insert into st_tinyint_e22 using mt_tinyint tags(033) values(now, abc)"
            )
            tdSql.error(
                f'insert into st_tinyint_e23 using mt_tinyint tags(033) values(now, "abc")'
            )
            tdSql.error(
                f'insert into st_tinyint_e24 using mt_tinyint tags(033) values(now, " ")'
            )
            tdSql.error(
                f"insert into st_tinyint_e25_2 using mt_tinyint tags(033) values(now, '')"
            )

            tdSql.error(
                f"insert into st_tinyint_e13 using mt_tinyint tags(128) values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_tinyint_e14 using mt_tinyint tags(-128) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_tinyint_e15 using mt_tinyint tags(1280) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_tinyint_e16 using mt_tinyint tags(-1280) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_tinyint_e19 using mt_tinyint tags(123abc) values(now, -033)"
            )
            tdSql.error(
                f'insert into st_tinyint_e20 using mt_tinyint tags("123abc") values(now, -033)'
            )
            tdSql.error(
                f"insert into st_tinyint_e22 using mt_tinyint tags(abc) values(now, -033)"
            )
            tdSql.error(
                f'insert into st_tinyint_e23 using mt_tinyint tags("abc") values(now, -033)'
            )
            tdSql.error(
                f'insert into st_tinyint_e24 using mt_tinyint tags(" ") values(now, -033)'
            )
            tdSql.error(
                f"insert into st_tinyint_e25 using mt_tinyint tags('') values(now, -033)"
            )

            tdSql.execute(
                f"insert into st_tinyint_e13 using mt_tinyint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_tinyint_e14 using mt_tinyint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_tinyint_e15 using mt_tinyint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_tinyint_e16 using mt_tinyint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_tinyint_e17 using mt_tinyint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_tinyint_e18 using mt_tinyint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_tinyint_e19 using mt_tinyint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_tinyint_e20 using mt_tinyint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_tinyint_e21 using mt_tinyint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_tinyint_e22 using mt_tinyint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_tinyint_e23 using mt_tinyint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_tinyint_e24 using mt_tinyint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_tinyint_e25 using mt_tinyint tags(033) values(now, 00062)"
            )

            tdSql.error(f"alter table st_tinyint_e13 set tag tagname=128")
            tdSql.execute(f"alter table st_tinyint_e14 set tag tagname=-128")
            tdSql.error(f"alter table st_tinyint_e15 set tag tagname=1280")
            tdSql.error(f"alter table st_tinyint_e16 set tag tagname=-1280")
            tdSql.error(f"alter table st_tinyint_e19 set tag tagname=123abc")
            tdSql.error(f'alter table st_tinyint_e20 set tag tagname="123abc"')
            tdSql.error(f"alter table st_tinyint_e22 set tag tagname=abc")
            tdSql.error(f'alter table st_tinyint_e23 set tag tagname="abc"')
            tdSql.error(f'alter table st_tinyint_e24 set tag tagname=" "')
            tdSql.error(f"alter table st_tinyint_e25 set tag tagname=''")


    # ======================================================================
    # uint: unsigned int column and tag values
    # ======================================================================

    def test_datatype_uint(self):
            """DataTypes: unsigned int

            1. Create table
            2. Insert data
            3. Auto-create table
            4. Alter tag value
            5. Handle illegal input

            Since: v3.0.0.0

            Labels: common,ci,integration,functional

            Jira: None

            History:
                - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_uint.sim

            """
            self.uint_create_table()
            self.uint_insert_data()
            self.uint_auto_create_table()
            self.uint_alter_tag_value()
            self.uint_illegal_input()

    def uint_create_table(self):
            tdLog.info(f"create super table")
            tdSql.execute(
                f"create table mt_uint (ts timestamp, c int unsigned) tags(tagname int unsigned)"
            )

            tdLog.info(f"case 0: static create table for test tag values")

            tdSql.execute(f"create table st_uint_0 using mt_uint tags(NULL)")
            tdSql.query(f"show create table st_uint_0")
            tdSql.query(f"show tags from st_uint_0")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_uint_1 using mt_uint tags(NULL)")
            tdSql.query(f"show tags from st_uint_1")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_uint_2 using mt_uint tags('NULL')")
            tdSql.query(f"show tags from st_uint_2")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_uint_3 using mt_uint tags('NULL')")
            tdSql.query(f"show tags from st_uint_3")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_uint_4 using mt_uint tags("NULL")')
            tdSql.query(f"show tags from st_uint_4")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f'create table st_uint_5 using mt_uint tags("-0")')
            tdSql.query(f"show tags from st_uint_5")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"create table st_uint_6 using mt_uint tags(-0 )")
            tdSql.query(f"show tags from st_uint_6")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"create table st_uint_7 using mt_uint tags(2147483647)")
            tdSql.query(f"show tags from st_uint_7")
            tdSql.checkData(0, 5, 2147483647)

            tdSql.execute(f"create table st_uint_8 using mt_uint tags(37)")
            tdSql.query(f"show tags from st_uint_8")
            tdSql.checkData(0, 5, 37)

            tdSql.execute(f"create table st_uint_9 using mt_uint tags(098)")
            tdSql.query(f"show tags from st_uint_9")
            tdSql.checkData(0, 5, 98)

            tdSql.execute(f"create table st_uint_10 using mt_uint tags(+113)")
            tdSql.query(f"show tags from st_uint_10")
            tdSql.checkData(0, 5, 113)

            tdSql.execute(f"create table st_uint_11 using mt_uint tags(+000.000)")
            tdSql.query(f"show tags from st_uint_11")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f'create table st_uint_12 using mt_uint tags("+78")')
            tdSql.query(f"show tags from st_uint_12")
            tdSql.checkData(0, 5, 78)

            tdSql.execute(f"create table st_uint_13 using mt_uint tags(+0078)")
            tdSql.query(f"show tags from st_uint_13")
            tdSql.checkData(0, 5, 78)

            tdSql.execute(f"create table st_uint_14 using mt_uint tags(00078)")
            tdSql.query(f"show tags from st_uint_14")
            tdSql.checkData(0, 5, 78)

            tdSql.execute(f'create table st_uint_100 using mt_uint tags("0x01")')
            tdSql.query(f"show tags from st_uint_100")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'create table st_uint_101 using mt_uint tags("0b01")')
            tdSql.query(f"show tags from st_uint_101")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'create table st_uint_102 using mt_uint tags("+0x01")')
            tdSql.query(f"show tags from st_uint_102")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'create table st_uint_103 using mt_uint tags("-0b00")')
            tdSql.query(f"show tags from st_uint_103")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f'create table st_uint_104 using mt_uint tags("123.1")')
            tdSql.query(f"show tags from st_uint_104")
            tdSql.checkData(0, 5, 123)

            tdSql.execute(f'create table st_uint_105 using mt_uint tags("+123.5")')
            tdSql.query(f"show tags from st_uint_105")
            tdSql.checkData(0, 5, 124)

            tdSql.execute(f'create table st_uint_106 using mt_uint tags("-1e-1")')
            tdSql.query(f"show tags from st_uint_106")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f'create table st_uint_107 using mt_uint tags("+0.1235e3")')
            tdSql.query(f"show tags from st_uint_107")
            tdSql.checkData(0, 5, 124)

            tdSql.execute(f'create table st_uint_108 using mt_uint tags("-0.11e-30")')
            tdSql.query(f"show tags from st_uint_108")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f'create table st_uint_109 using mt_uint tags("-1.1e-307")')
            tdSql.query(f"show tags from st_uint_109")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"create table st_uint_110 using mt_uint tags( -1e-1 )")
            tdSql.query(f"show tags from st_uint_110")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"create table st_uint_111 using mt_uint tags( +0.1235e3 )")
            tdSql.query(f"show tags from st_uint_111")
            tdSql.checkData(0, 5, 124)

            tdSql.execute(f"create table st_uint_112 using mt_uint tags(-0.11e-30)")
            tdSql.query(f"show tags from st_uint_112")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"create table st_uint_113 using mt_uint tags(-1.1e-307)")
            tdSql.query(f"show tags from st_uint_113")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"create table st_uint_200 using mt_uint tags( 0x01)")
            tdSql.query(f"show tags from st_uint_200")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"create table st_uint_201 using mt_uint tags(0b01 )")
            tdSql.query(f"show tags from st_uint_201")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"create table st_uint_202 using mt_uint tags(+0x01)")
            tdSql.query(f"show tags from st_uint_202")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"create table st_uint_203 using mt_uint tags( -0b00 )")
            tdSql.query(f"show tags from st_uint_203")
            tdSql.checkData(0, 5, 0)

    def uint_insert_data(self):
            tdLog.info(f"case 1: insert values for test column values")

            tdSql.execute(f"insert into st_uint_0 values(now, NULL)")
            tdSql.query(f"select * from st_uint_0")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)

            tdSql.execute(f'insert into st_uint_1 values(now, "-0")')
            tdSql.query(f"select * from st_uint_1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_uint_6 values(now, 2147483647)")
            tdSql.query(f"select * from st_uint_6")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 2147483647)

            tdSql.execute(f"insert into st_uint_7 values(now, -0)")
            tdSql.query(f"select * from st_uint_7")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_uint_8 values(now, +100)")
            tdSql.query(f"select * from st_uint_8")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 100)

            tdSql.execute(f'insert into st_uint_9 values(now, "098")')
            tdSql.query(f"select * from st_uint_9")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 98)

            tdSql.execute(f"insert into st_uint_10 values(now, '0')")
            tdSql.query(f"select * from st_uint_10")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_uint_11 values(now, +000.000)")
            tdSql.query(f"select * from st_uint_11")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f'insert into st_uint_12 values(now, "+056")')
            tdSql.query(f"select * from st_uint_12")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 56)

            tdSql.execute(f"insert into st_uint_13 values(now, +056)")
            tdSql.query(f"select * from st_uint_13")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 56)

            tdSql.execute(f"insert into st_uint_14 values(now, 056)")
            tdSql.query(f"select * from st_uint_14")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 56)

            tdSql.execute(f'insert into st_uint_100 values(now, "0x01")')
            tdSql.query(f"select * from st_uint_100")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_uint_101 values(now, "0b01")')
            tdSql.query(f"select * from st_uint_101")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_uint_102 values(now, "+0x01")')
            tdSql.query(f"select * from st_uint_102")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f'insert into st_uint_103 values(now, "-0b00")')
            tdSql.query(f"select * from st_uint_103")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f'insert into st_uint_104 values(now, "123.1")')
            tdSql.query(f"select * from st_uint_104")
            tdSql.checkData(0, 1, 123)

            tdSql.execute(f'insert into st_uint_105 values(now, "+123.5")')
            tdSql.query(f"select * from st_uint_105")
            tdSql.checkData(0, 1, 124)

            tdSql.execute(f'insert into st_uint_106 values(now, "-1e-1")')
            tdSql.query(f"select * from st_uint_106")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f'insert into st_uint_107 values(now, "+0.1235e3")')
            tdSql.query(f"select * from st_uint_107")
            tdSql.checkData(0, 1, 124)

            tdSql.execute(f'insert into st_uint_108 values(now, "-0.11e-30")')
            tdSql.query(f"select * from st_uint_108")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f'insert into st_uint_109 values(now, "-1.1e-307")')
            tdSql.query(f"select * from st_uint_109")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_uint_110 values(now,  -1e-1 )")
            tdSql.query(f"select * from st_uint_110")

            tdSql.execute(f"insert into st_uint_111 values(now,  +0.1235e3 )")
            tdSql.query(f"select * from st_uint_111")
            tdSql.checkData(0, 1, 124)

            tdSql.execute(f"insert into st_uint_112 values(now, -0.11e-30)")
            tdSql.query(f"select * from st_uint_112")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_uint_113 values(now, -1.1e-307)")
            tdSql.query(f"select * from st_uint_113")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_uint_200 values(now,  0x01)")
            tdSql.query(f"select * from st_uint_200")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_uint_201 values(now, 0b01 )")
            tdSql.query(f"select * from st_uint_201")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_uint_202 values(now, +0x01)")
            tdSql.query(f"select * from st_uint_202")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 1)

            tdSql.execute(f"insert into st_uint_203 values(now,  -0b00 )")
            tdSql.query(f"select * from st_uint_203")
            tdSql.checkData(0, 1, 0)

    def uint_auto_create_table(self):
            tdLog.info(f"case 2: dynamic create table for test tag values")

            tdSql.execute(
                f"insert into st_uint_16 using mt_uint tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show create table st_uint_16")
            tdSql.query(f"show tags from st_uint_16")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_uint_16")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_uint_17 using mt_uint tags(NULL) values(now, NULL)"
            )
            tdSql.query(f"show tags from st_uint_17")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_uint_17")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_uint_18 using mt_uint tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_uint_18")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_uint_18")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_uint_19 using mt_uint tags('NULL') values(now, 'NULL')"
            )
            tdSql.query(f"show tags from st_uint_19")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_uint_19")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_uint_20 using mt_uint tags("NULL") values(now, "NULL")'
            )
            tdSql.query(f"show tags from st_uint_20")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_uint_20")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f'insert into st_uint_21 using mt_uint tags("-0") values(now, "-0")'
            )
            tdSql.query(f"show tags from st_uint_21")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_uint_21")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_uint_22 using mt_uint tags(2147483647) values(now, 2147483647)"
            )
            tdSql.query(f"show tags from st_uint_22")
            tdSql.checkData(0, 5, 2147483647)

            tdSql.query(f"select * from st_uint_22")
            tdSql.checkData(0, 1, 2147483647)

            tdSql.execute(f"insert into st_uint_23 using mt_uint tags(-0) values(now, -0)")
            tdSql.query(f"show tags from st_uint_23")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_uint_23")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(f"insert into st_uint_24 using mt_uint tags(10) values(now, 10)")
            tdSql.query(f"show tags from st_uint_24")
            tdSql.checkData(0, 5, 10)

            tdSql.query(f"select * from st_uint_24")
            tdSql.checkData(0, 1, 10)

            tdSql.execute(
                f'insert into st_uint_25 using mt_uint tags("-0") values(now, "-0")'
            )
            tdSql.query(f"show tags from st_uint_25")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_uint_25")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_uint_26 using mt_uint tags('123') values(now, '123')"
            )
            tdSql.query(f"show tags from st_uint_26")
            tdSql.checkData(0, 5, 123)

            tdSql.query(f"select * from st_uint_26")
            tdSql.checkData(0, 1, 123)

            tdSql.execute(
                f"insert into st_uint_27 using mt_uint tags(+056) values(now, +00056)"
            )
            tdSql.query(f"show tags from st_uint_27")
            tdSql.checkData(0, 5, 56)

            tdSql.query(f"select * from st_uint_27")
            tdSql.checkData(0, 1, 56)

            tdSql.execute(
                f"insert into st_uint_28 using mt_uint tags(056) values(now, 0056)"
            )
            tdSql.query(f"show tags from st_uint_28")
            tdSql.checkData(0, 5, 56)

            tdSql.query(f"select * from st_uint_28")
            tdSql.checkData(0, 1, 56)

            tdSql.execute(
                f'insert into st_uint_1100 using mt_uint tags("0x01") values(now, "0x01")'
            )
            tdSql.query(f"show tags from st_uint_1100")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select * from st_uint_1100")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_uint_1101 using mt_uint tags("0b01") values(now, "0b01")'
            )
            tdSql.query(f"show tags from st_uint_1101")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select * from st_uint_1101")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_uint_1102 using mt_uint tags("+0x01") values(now, "+0x01")'
            )
            tdSql.query(f"show tags from st_uint_1102")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select * from st_uint_1102")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f'insert into st_uint_1103 using mt_uint tags("-0b00") values(now, "-0b000")'
            )
            tdSql.query(f"show tags from st_uint_1103")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_uint_1103")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f'insert into st_uint_1104 using mt_uint tags("123.1") values(now, "123.1")'
            )
            tdSql.query(f"show tags from st_uint_1104")
            tdSql.checkData(0, 5, 123)

            tdSql.query(f"select * from st_uint_1104")
            tdSql.checkData(0, 1, 123)

            tdSql.execute(
                f'insert into st_uint_1105 using mt_uint tags("+123.5") values(now, "+123.5")'
            )
            tdSql.query(f"show tags from st_uint_1105")
            tdSql.checkData(0, 5, 124)

            tdSql.query(f"select * from st_uint_1105")
            tdSql.checkData(0, 1, 124)

            tdSql.execute(
                f'insert into st_uint_1106 using mt_uint tags("-1e-1") values(now, "-1e-1")'
            )
            tdSql.query(f"show tags from st_uint_1106")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_uint_1106")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f'insert into st_uint_1107 using mt_uint tags("+0.1235e3") values(now, "+0.1235e3")'
            )
            tdSql.query(f"show tags from st_uint_1107")
            tdSql.checkData(0, 5, 124)

            tdSql.execute(
                f'insert into st_uint_1108 using mt_uint tags("-0.11e-30") values(now, "-0.11e-30")'
            )
            tdSql.query(f"show tags from st_uint_1108")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_uint_1108")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f'insert into st_uint_1109 using mt_uint tags("-1.1e-307") values(now, "-1.1e-307")'
            )
            tdSql.query(f"show tags from st_uint_1109")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_uint_1109")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_uint_1110 using mt_uint tags( -1e-1 ) values(now, -1e-1)"
            )
            tdSql.query(f"show tags from st_uint_1110")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_uint_1110")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_uint_1111 using mt_uint tags( +0.1235e3 ) values(now, +0.1235e3)"
            )
            tdSql.query(f"show tags from st_uint_1111")
            tdSql.checkData(0, 5, 124)

            tdSql.query(f"select * from st_uint_1111")
            tdSql.checkData(0, 1, 124)

            tdSql.execute(
                f"insert into st_uint_1112 using mt_uint tags(-0.11e-30) values(now, -0.11e-30)"
            )
            tdSql.query(f"show tags from st_uint_1112")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_uint_1112")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_uint_1113 using mt_uint tags(-1.1e-307) values(now, -1.1e-307)"
            )
            tdSql.query(f"show tags from st_uint_1113")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_uint_1113")
            tdSql.checkData(0, 1, 0)

            tdSql.execute(
                f"insert into st_uint_1200 using mt_uint tags( 0x01) values(now, 0x01)"
            )
            tdSql.query(f"show tags from st_uint_1200")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select * from st_uint_1200")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_uint_1201 using mt_uint tags(0b01 ) values(now, 0b01)"
            )
            tdSql.query(f"show tags from st_uint_1201")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select * from st_uint_1201")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_uint_1202 using mt_uint tags(+0x01) values(now, +0x01)"
            )
            tdSql.query(f"show tags from st_uint_1202")
            tdSql.checkData(0, 5, 1)

            tdSql.query(f"select * from st_uint_1202")
            tdSql.checkData(0, 1, 1)

            tdSql.execute(
                f"insert into st_uint_1203 using mt_uint tags( 0b000000 ) values(now, -0b0000)"
            )
            tdSql.query(f"show tags from st_uint_1203")
            tdSql.checkData(0, 5, 0)

            tdSql.query(f"select * from st_uint_1203")
            tdSql.checkData(0, 1, 0)

    def uint_alter_tag_value(self):
            tdLog.info(f"case 3: alter tag value")

            tdSql.execute(f"alter table st_uint_0 set tag tagname=2147483647")
            tdSql.query(f"show tags from st_uint_0")
            tdSql.checkData(0, 5, 2147483647)

            tdSql.execute(f"alter table st_uint_0 set tag tagname=-0")
            tdSql.query(f"show tags from st_uint_0")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"alter table st_uint_0 set tag tagname=+100")
            tdSql.query(f"show tags from st_uint_0")
            tdSql.checkData(0, 5, 100)

            tdSql.execute(f"alter table st_uint_0 set tag tagname=+33.333")
            tdSql.query(f"show tags from st_uint_0")
            tdSql.checkData(0, 5, 33)

            tdSql.execute(f"alter table st_uint_0 set tag tagname='+98'")
            tdSql.query(f"show tags from st_uint_0")
            tdSql.checkData(0, 5, 98)

            tdSql.execute(f"alter table st_uint_0 set tag tagname='076'")
            tdSql.query(f"show tags from st_uint_0")
            tdSql.checkData(0, 5, 76)

            tdSql.execute(f"alter table st_uint_0 set tag tagname=+0012")
            tdSql.query(f"show tags from st_uint_0")
            tdSql.checkData(0, 5, 12)

            tdSql.execute(f"alter table st_uint_0 set tag tagname=00063")
            tdSql.query(f"show tags from st_uint_0")
            tdSql.checkData(0, 5, 63)

            tdSql.execute(f'alter table st_uint_100 set tag tagname="0x01"')
            tdSql.query(f"show tags from st_uint_100")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'alter table st_uint_101 set tag tagname="0b01"')
            tdSql.query(f"show tags from st_uint_101")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'alter table st_uint_102 set tag tagname="+0x01"')
            tdSql.query(f"show tags from st_uint_102")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f'alter table st_uint_103 set tag tagname="-0b00"')
            tdSql.query(f"show tags from st_uint_103")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f'alter table st_uint_104 set tag tagname="123.1"')
            tdSql.query(f"show tags from st_uint_104")
            tdSql.checkData(0, 5, 123)

            tdSql.execute(f'alter table st_uint_105 set tag tagname="+123.5"')
            tdSql.query(f"show tags from st_uint_105")
            tdSql.checkData(0, 5, 124)

            tdSql.execute(f'alter table st_uint_106 set tag tagname="-1e-1"')
            tdSql.query(f"show tags from st_uint_106")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f'alter table st_uint_107 set tag tagname="+0.1235e3"')
            tdSql.query(f"show tags from st_uint_107")
            tdSql.checkData(0, 5, 124)

            tdSql.execute(f'alter table st_uint_108 set tag tagname="-0.11e-30"')
            tdSql.query(f"show tags from st_uint_108")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f'alter table st_uint_109 set tag tagname="-1.1e-307"')
            tdSql.query(f"show tags from st_uint_109")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"alter table st_uint_110 set tag tagname= -1e-1 ")
            tdSql.query(f"show tags from st_uint_110")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"alter table st_uint_111 set tag tagname= +0.1235e3 ")
            tdSql.query(f"show tags from st_uint_111")
            tdSql.checkData(0, 5, 124)

            tdSql.execute(f"alter table st_uint_112 set tag tagname=-0.11e-30")
            tdSql.query(f"show tags from st_uint_112")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"alter table st_uint_113 set tag tagname=-1.1e-307")
            tdSql.query(f"show tags from st_uint_113")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"alter table st_uint_200 set tag tagname= 0x01")
            tdSql.query(f"show tags from st_uint_200")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"alter table st_uint_201 set tag tagname=0b00")
            tdSql.query(f"show tags from st_uint_201")
            tdSql.checkData(0, 5, 0)

            tdSql.execute(f"alter table st_uint_202 set tag tagname=+0x01")
            tdSql.query(f"show tags from st_uint_202")
            tdSql.checkData(0, 5, 1)

            tdSql.execute(f"alter table st_uint_203 set tag tagname= -0b0")
            tdSql.query(f"show tags from st_uint_203")
            tdSql.checkData(0, 5, 0)

    def uint_illegal_input(self):
            tdLog.info(f"case 4: illegal input")

            tdSql.error(f"create table st_uint_e0_err0 using mt_uint tags(4294967296)")
            tdSql.error(f"create table st_uint_e0_err1 using mt_uint tags(-4294967297)")
            tdSql.error(f"create table st_uint_e0_err2 using mt_uint tags(-214748364800)")
            tdSql.error(f"create table st_uint_e0_err3 using mt_uint tags(123abc)")
            tdSql.error(f'create table st_uint_e0_err4 using mt_uint tags("123abc")')
            tdSql.error(f"create table st_uint_e0_err5 using mt_uint tags(abc)")
            tdSql.error(f'create table st_uint_e0_err6 using mt_uint tags("abc")')
            tdSql.error(f'create table st_uint_e0_err7 using mt_uint tags(" ")')
            tdSql.error(f"create table st_uint_e0_err8 using mt_uint tags('')")

            tdSql.execute(f"create table st_uint_e0 using mt_uint tags(123)")
            tdSql.execute(f"create table st_uint_e1 using mt_uint tags(123)")
            tdSql.execute(f"create table st_uint_e2 using mt_uint tags(123)")
            tdSql.execute(f"create table st_uint_e3 using mt_uint tags(123)")
            tdSql.execute(f"create table st_uint_e4 using mt_uint tags(123)")
            tdSql.execute(f"create table st_uint_e5 using mt_uint tags(123)")
            tdSql.execute(f"create table st_uint_e6 using mt_uint tags(123)")
            tdSql.execute(f"create table st_uint_e7 using mt_uint tags(123)")
            tdSql.execute(f"create table st_uint_e8 using mt_uint tags(123)")
            tdSql.execute(f"create table st_uint_e9 using mt_uint tags(123)")
            tdSql.execute(f"create table st_uint_e10 using mt_uint tags(123)")
            tdSql.execute(f"create table st_uint_e11 using mt_uint tags(123)")
            tdSql.execute(f"create table st_uint_e12 using mt_uint tags(123)")

            tdSql.error(f"insert into st_uint_e0 values(now, 4294967296)")
            tdSql.error(f"insert into st_uint_e1 values(now, -4294967297)")
            tdSql.error(f"insert into st_uint_e3 values(now, -21474836481)")
            tdSql.error(f"insert into st_uint_e6 values(now, 123abc)")
            tdSql.error(f'insert into st_uint_e7 values(now, "123abc")')
            tdSql.error(f"insert into st_uint_e9 values(now, abc)")
            tdSql.error(f'insert into st_uint_e10 values(now, "abc")')
            tdSql.error(f'insert into st_uint_e11 values(now, " ")')
            tdSql.error(f"insert into st_uint_e12 values(now, '')")

            tdSql.error(
                f"insert into st_uint_e13 using mt_uint tags(033) values(now, 4294967296)"
            )
            tdSql.error(
                f"insert into st_uint_e14 using mt_uint tags(033) values(now, -4294967297)"
            )
            tdSql.error(
                f"insert into st_uint_e16 using mt_uint tags(033) values(now, -21474836481)"
            )
            tdSql.error(
                f"insert into st_uint_e19 using mt_uint tags(033) values(now, 123abc)"
            )
            tdSql.error(
                f'insert into st_uint_e20 using mt_uint tags(033) values(now, "123abc")'
            )
            tdSql.error(f"insert into st_uint_e22 using mt_uint tags(033) values(now, abc)")
            tdSql.error(
                f'insert into st_uint_e23 using mt_uint tags(033) values(now, "abc")'
            )
            tdSql.error(f'insert into st_uint_e24 using mt_uint tags(033) values(now, " ")')
            tdSql.error(f"insert into st_uint_e25 using mt_uint tags(033) values(now, '')")

            tdSql.error(
                f"insert into st_uint_e13 using mt_uint tags(21474294967296483648) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_uint_e14_1 using mt_uint tags(-2147483648) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_uint_e16 using mt_uint tags(-2147483649) values(now, -033)"
            )
            tdSql.error(
                f"insert into st_uint_e19 using mt_uint tags(123abc) values(now, -033)"
            )
            tdSql.error(
                f'insert into st_uint_e20 using mt_uint tags("123abc") values(now, -033)'
            )
            tdSql.error(
                f"insert into st_uint_e22 using mt_uint tags(abc) values(now, -033)"
            )
            tdSql.error(
                f'insert into st_uint_e23 using mt_uint tags("abc") values(now, -033)'
            )
            tdSql.error(
                f'insert into st_uint_e24 using mt_uint tags(" ") values(now, -033)'
            )
            tdSql.error(
                f"insert into st_uint_e25_1 using mt_uint tags('') values(now, -033)"
            )
            tdSql.execute(
                f"insert into st_uint_e26_1 using mt_uint tags('123') values(now, 033)"
            )
            tdSql.execute(
                f"insert into st_uint_e27_1 using mt_uint tags('12.80') values(now, 033)"
            )
            tdSql.error(
                f"insert into st_uint_e28_1 using mt_uint tags('-11.80') values(now, 033)"
            )

            tdSql.execute(
                f"insert into st_uint_e13 using mt_uint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_uint_e14 using mt_uint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_uint_e15 using mt_uint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_uint_e16 using mt_uint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_uint_e17 using mt_uint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_uint_e18 using mt_uint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_uint_e19 using mt_uint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_uint_e20 using mt_uint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_uint_e21 using mt_uint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_uint_e22 using mt_uint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_uint_e23 using mt_uint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_uint_e24 using mt_uint tags(033) values(now, 00062)"
            )
            tdSql.execute(
                f"insert into st_uint_e25 using mt_uint tags(033) values(now, 00062)"
            )

            tdSql.error(f"alter table st_uint_e13 set tag tagname=4294967296")
            tdSql.error(f"alter table st_uint_e14 set tag tagname=-4294967297")
            tdSql.error(f"alter table st_uint_e16 set tag tagname=-3147483648")
            tdSql.error(f"alter table st_uint_e19 set tag tagname=123abc")
            tdSql.error(f'alter table st_uint_e20 set tag tagname="123abc"')
            tdSql.error(f"alter table st_uint_e22 set tag tagname=abc")
            tdSql.error(f'alter table st_uint_e23 set tag tagname="abc"')
            tdSql.error(f'alter table st_uint_e24 set tag tagname=" "')
            tdSql.error(f"alter table st_uint_e25 set tag tagname=''")
            tdSql.execute(f"alter table st_uint_e26_1 set tag tagname='123'")
            tdSql.execute(f"alter table st_uint_e27_1 set tag tagname='12.80'")
            tdSql.error(f"alter table st_uint_e28_1 set tag tagname='-11.80'")


    # ======================================================================
    # varbinary: varbinary column and tag values
    # ======================================================================

    def test_datatype_varbinary(self):
            """DataTypes: varbinary

            1. Create table
            2. Insert data
            3. Auto-create table
            4. Alter tag value
            5. Handle illegal input

            Since: v3.0.0.0

            Labels: common,ci,integration,functional

            Jira: None

            History:
                - 2025-5-12 Simon Guan Migrated from tsim/parser/columnValue_varbinary.sim

            """
            self.varbinary_create_table()
            self.varbinary_insert_data()
            self.varbinary_auto_create_table()
            self.varbinary_alter_tag_value()
            self.varbinary_illegal_input()

    def varbinary_create_table(self):
            tdLog.info(f"create super table")
            tdSql.execute(
                f"create table mt_varbinary (ts timestamp, c varbinary(50)) tags(tagname varbinary(50))"
            )

            tdLog.info(f"case 0: static create table for test tag values")

            tdSql.execute(f"create table st_varbinary_0 using mt_varbinary tags(NULL)")
            tdSql.query(f"show tags from st_varbinary_0")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_varbinary_1 using mt_varbinary tags(NULL)")
            tdSql.query(f"show tags from st_varbinary_1")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"create table st_varbinary_2 using mt_varbinary tags('NULL')")
            tdSql.query(f"show tags from st_varbinary_2")
            tdSql.checkData(0, 5, "\\x4E554C4C")

            tdSql.execute(f"create table st_varbinary_3 using mt_varbinary tags('NULL')")
            tdSql.query(f"show tags from st_varbinary_3")
            tdSql.checkData(0, 5, "\\x4E554C4C")

            tdSql.execute(f'create table st_varbinary_4 using mt_varbinary tags("NULL")')
            tdSql.query(f"show tags from st_varbinary_4")
            tdSql.checkData(0, 5, "\\x4E554C4C")

            tdSql.execute(f'create table st_varbinary_5 using mt_varbinary tags("NULL")')
            tdSql.query(f"show tags from st_varbinary_5")
            tdSql.checkData(0, 5, "\\x4E554C4C")

            tdSql.execute(f'create table st_varbinary_6 using mt_varbinary tags("")')
            tdSql.query(f"show tags from st_varbinary_6")
            tdSql.checkData(0, 5, "\\x")

            tdSql.execute(f'create table st_varbinary_7 using mt_varbinary tags(" ")')
            tdSql.query(f"show tags from st_varbinary_7")
            tdSql.checkData(0, 5, "\\x20")

            str = "\\x"
            tdSql.execute(f'create table st_varbinary_8 using mt_varbinary tags("{str}")')
            tdSql.query(f"show tags from st_varbinary_8")
            tdSql.checkData(0, 5, "\\x")

            tdSql.execute(f'create table st_varbinary_9 using mt_varbinary tags("{str}aB")')
            tdSql.query(f"show tags from st_varbinary_9")
            tdSql.checkData(0, 5, "\\xAB")

            tdSql.execute(f'create table st_varbinary_10 using mt_varbinary tags("aB")')
            tdSql.query(f"show tags from st_varbinary_10")
            tdSql.checkData(0, 5, "\\x6142")

    def varbinary_insert_data(self):
            tdLog.info(f"case 1: insert values for test column values")

            tdSql.execute(f"insert into st_varbinary_0 values(now, NULL)")
            tdSql.query(f"select * from st_varbinary_0")
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_varbinary_1 values(now, NULL)")
            tdSql.query(f"select * from st_varbinary_1")
            tdSql.checkData(0, 1, None)

            tdSql.execute(f"insert into st_varbinary_2 values(now, 'NULL')")
            tdSql.query(f"select * from st_varbinary_2")
            tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

            tdSql.execute(f"insert into st_varbinary_3 values(now, 'NULL')")
            tdSql.query(f"select * from st_varbinary_3")
            tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

            tdSql.execute(f'insert into st_varbinary_4 values(now, "NULL")')
            tdSql.query(f"select * from st_varbinary_4")
            tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

            tdSql.execute(f'insert into st_varbinary_5 values(now, "NULL")')
            tdSql.query(f"select * from st_varbinary_5")
            tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

            tdSql.execute(f'insert into st_varbinary_6 values(now, "")')
            tdSql.query(f"select * from st_varbinary_6")
            tdSql.checkData(0, 1, bytes.fromhex(''))

            tdSql.execute(f'insert into st_varbinary_7 values(now, " ")')
            tdSql.query(f"select * from st_varbinary_7")
            tdSql.checkData(0, 1, bytes.fromhex('20'))

            str = "\\x"
            tdSql.execute(f'insert into st_varbinary_8 values(now, "{str}")')
            tdSql.query(f"select * from st_varbinary_8")
            tdSql.checkData(0, 1, bytes.fromhex(''))

            tdSql.execute(f'insert into st_varbinary_9 values(now, "{str}aB")')
            tdSql.query(f"select * from st_varbinary_9")
            tdSql.checkData(0, 1, bytes.fromhex('AB'))

            tdSql.execute(f'insert into st_varbinary_10 values(now, "aB")')
            tdSql.query(f"select * from st_varbinary_10")
            tdSql.checkData(0, 1, bytes.fromhex('6142'))

    def varbinary_auto_create_table(self):
            tdLog.info(f"case 2: dynamic create table for test tag values")

            tdSql.execute(
                f"insert into st_varbinary_100 using mt_varbinary tags(NULL) values(now,NULL)"
            )
            tdSql.query(f"show tags from st_varbinary_100")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_varbinary_100")
            # tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_varbinary_101 using mt_varbinary tags(NULL) values(now,NULL)"
            )
            tdSql.query(f"show tags from st_varbinary_101")
            tdSql.checkData(0, 5, None)

            tdSql.query(f"select * from st_varbinary_101")
            tdSql.checkData(0, 1, None)

            tdSql.execute(
                f"insert into st_varbinary_102 using mt_varbinary tags('NULL') values(now,'NULL')"
            )
            tdSql.query(f"show tags from st_varbinary_102")
            tdSql.checkData(0, 5, "\\x4E554C4C")

            tdSql.query(f"select * from st_varbinary_102")
            tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

            tdSql.execute(
                f"insert into st_varbinary_103 using mt_varbinary tags('NULL') values(now,'NULL')"
            )
            tdSql.query(f"show tags from st_varbinary_103")
            tdSql.checkData(0, 5, "\\x4E554C4C")

            tdSql.query(f"select * from st_varbinary_103")
            tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

            tdSql.execute(
                f'insert into st_varbinary_104 using mt_varbinary tags("NULL") values(now,"NULL")'
            )
            tdSql.query(f"show tags from st_varbinary_104")
            tdSql.checkData(0, 5, "\\x4E554C4C")

            tdSql.query(f"select * from st_varbinary_104")
            tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

            tdSql.execute(
                f'insert into st_varbinary_105 using mt_varbinary tags("NULL") values(now,"NULL")'
            )
            tdSql.query(f"show tags from st_varbinary_105")
            tdSql.checkData(0, 5, "\\x4E554C4C")

            tdSql.query(f"select * from st_varbinary_105")
            tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

            tdSql.execute(
                f'insert into st_varbinary_106 using mt_varbinary tags("") values(now,"")'
            )
            tdSql.query(f"show tags from st_varbinary_106")
            tdSql.checkData(0, 5, "\\x")

            tdSql.query(f"select * from st_varbinary_106")
            tdSql.checkData(0, 1, bytes.fromhex(''))

            tdSql.execute(
                f'insert into st_varbinary_107 using mt_varbinary tags(" ") values(now," ")'
            )
            tdSql.query(f"show tags from st_varbinary_107")
            tdSql.checkData(0, 5, "\\x20")

            tdSql.query(f"select * from st_varbinary_107")
            tdSql.checkData(0, 1, bytes.fromhex('20'))

            str = "\\x"
            tdSql.execute(
                f'insert into st_varbinary_108 using mt_varbinary tags("{str}") values(now,"{str}")'
            )
            tdSql.query(f"show tags from st_varbinary_108")
            tdSql.checkData(0, 5, "\\x")

            tdSql.query(f"select * from st_varbinary_108")
            tdSql.checkData(0, 1, bytes.fromhex(''))

            tdSql.execute(
                f'insert into st_varbinary_109 using mt_varbinary tags("{str}aB") values(now,"{str}aB")'
            )
            tdSql.query(f"show tags from st_varbinary_109")
            tdSql.checkData(0, 5, "\\xAB")

            tdSql.query(f"select * from st_varbinary_109")
            tdSql.checkData(0, 1, bytes.fromhex('AB'))

            tdSql.execute(
                f'insert into st_varbinary_1010 using mt_varbinary tags("aB") values(now,"aB")'
            )
            tdSql.query(f"show tags from st_varbinary_1010")
            tdSql.checkData(0, 5, "\\x6142")

            tdSql.query(f"select * from st_varbinary_1010")
            tdSql.checkData(0, 1, bytes.fromhex('6142'))

    def varbinary_alter_tag_value(self):
            tdLog.info(f"case 3: alter tag value")

            tdSql.execute(f"alter table st_varbinary_100  set tag tagname=NULL")
            tdSql.query(f"show tags from st_varbinary_100")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"alter table st_varbinary_101  set tag tagname=NULL")
            tdSql.query(f"show tags from st_varbinary_101")
            tdSql.checkData(0, 5, None)

            tdSql.execute(f"alter table st_varbinary_102  set tag tagname='NULL'")
            tdSql.query(f"show tags from st_varbinary_102")
            tdSql.checkData(0, 5, "\\x4E554C4C")

            tdSql.execute(f"alter table st_varbinary_103  set tag tagname='NULL'")
            tdSql.query(f"show tags from st_varbinary_103")
            tdSql.checkData(0, 5, "\\x4E554C4C")

            tdSql.execute(f'alter table st_varbinary_104  set tag tagname="NULL"')
            tdSql.query(f"show tags from st_varbinary_104")
            tdSql.checkData(0, 5, "\\x4E554C4C")

            tdSql.execute(f'alter table st_varbinary_105  set tag tagname="NULL"')
            tdSql.query(f"show tags from st_varbinary_105")
            tdSql.checkData(0, 5, "\\x4E554C4C")

            tdSql.execute(f'alter table st_varbinary_106  set tag tagname=""')
            tdSql.query(f"show tags from st_varbinary_106")
            tdSql.checkData(0, 5, "\\x")

            tdSql.execute(f'alter table st_varbinary_107  set tag tagname=" "')
            tdSql.query(f"show tags from st_varbinary_107")
            tdSql.checkData(0, 5, "\\x20")

            str = "\\x"
            tdSql.execute(f'alter table st_varbinary_108  set tag tagname="{str}"')
            tdSql.query(f"show tags from st_varbinary_108")
            tdSql.checkData(0, 5, "\\x")

            tdSql.execute(f'alter table st_varbinary_109  set tag tagname="{str}aB"')
            tdSql.query(f"show tags from st_varbinary_109")
            tdSql.checkData(0, 5, "\\xAB")

            tdSql.execute(f'alter table st_varbinary_1010  set tag tagname="aB"')
            tdSql.query(f"show tags from st_varbinary_1010")
            tdSql.checkData(0, 5, "\\x6142")

    def varbinary_illegal_input(self):
            tdLog.info(f"case 4: illegal input")

            tdSql.error(f"create table st_varbinary_106 using mt_varbinary tags(+0123)")
            tdSql.error(f"create table st_varbinary_107 using mt_varbinary tags(-01.23)")
            tdSql.error(f"create table st_varbinary_108 using mt_varbinary tags(+0x01)")
            tdSql.error(f"create table st_varbinary_109 using mt_varbinary tags(-0b01)")
            tdSql.error(f"create table st_varbinary_1010 using mt_varbinary tags(-0.1e-10)")
            tdSql.error(f"create table st_varbinary_1011 using mt_varbinary tags(+0.1E+2)")
            tdSql.error(f"create table st_varbinary_1012 using mt_varbinary tags(tRue)")
            tdSql.error(f"create table st_varbinary_1013 using mt_varbinary tags(FalsE)")
            tdSql.error(f"create table st_varbinary_1014 using mt_varbinary tags(noW)")
            tdSql.error(f"create table st_varbinary_1015 using mt_varbinary tags(toDay)")
            tdSql.error(f"create table st_varbinary_1016 using mt_varbinary tags(now()+1s)")
            tdSql.error(f"create table st_varbinary_1017 using mt_varbinary tags(1+1s)")
            tdSql.error(
                f"insert into st_varbinary_106 using mt_varbinary tags(+0123) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_varbinary_107 using mt_varbinary tags(-01.23) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_varbinary_108 using mt_varbinary tags(+0x01) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_varbinary_109 using mt_varbinary tags(-0b01) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_varbinary_1010 using mt_varbinary tags(-0.1e-10) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_varbinary_1011 using mt_varbinary tags(+0.1E+2) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_varbinary_1012 using mt_varbinary tags(tRue) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_varbinary_1013 using mt_varbinary tags(FalsE) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_varbinary_1014 using mt_varbinary tags(noW) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_varbinary_1015 using mt_varbinary tags(toDay) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_varbinary_1016 using mt_varbinary tags(now()+1s) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_varbinary_1017 using mt_varbinary tags(1+1s) values(now, NULL);"
            )
            tdSql.error(
                f"insert into st_varbinary_106 using mt_varbinary tags(NULL) values(now(), +0123)"
            )
            tdSql.error(
                f"insert into st_varbinary_107 using mt_varbinary tags(NULL) values(now(), -01.23)"
            )
            tdSql.error(
                f"insert into st_varbinary_108 using mt_varbinary tags(NULL) values(now(), +0x01)"
            )
            tdSql.error(
                f"insert into st_varbinary_109 using mt_varbinary tags(NULL) values(now(), -0b01)"
            )
            tdSql.error(
                f"insert into st_varbinary_1010 using mt_varbinary tags(NULL) values(now(), -0.1e-10)"
            )
            tdSql.error(
                f"insert into st_varbinary_1011 using mt_varbinary tags(NULL) values(now(), +0.1E+2)"
            )
            tdSql.error(
                f"insert into st_varbinary_1012 using mt_varbinary tags(NULL) values(now(), tRue)"
            )
            tdSql.error(
                f"insert into st_varbinary_1013 using mt_varbinary tags(NULL) values(now(), FalsE)"
            )
            tdSql.error(
                f"insert into st_varbinary_1014 using mt_varbinary tags(NULL) values(now(), noW)"
            )
            tdSql.error(
                f"insert into st_varbinary_1015 using mt_varbinary tags(NULL) values(now(), toDay)"
            )
            tdSql.error(
                f"insert into st_varbinary_1016 using mt_varbinary tags(NULL) values(now(), now()+1s)"
            )
            tdSql.error(
                f"insert into st_varbinary_1017 using mt_varbinary tags(NULL) values(now(), 1+1s)"
            )
