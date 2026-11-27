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

import re
from new_test_framework.utils import tdLog, tdSql
import zlib

from new_test_framework.utils import tdLog, tdSql, tdStmt2


class TestDatatypeBlob:

    # TSDB_MAX_BLOB_LEN (include/util/tdef.h) includes the 4-byte BLOBSTR_HEADER_SIZE, so
    # the largest writable payload is MAX - 4.
    MAX_BLOB_PAYLOAD = (16 << 20) - 4      # 16777212

    # Every cast from BLOB to a numeric or bool type copies the value into one fixed size
    # buffer (TSDB_MAX_FIELD_LEN + 1) before parsing it, so 65519 bytes is the largest
    # value such a cast accepts and 65520 bytes is refused instead of being copied.
    BLOB_CAST_BUFFER = 65520

    # Values whose own size exceeds the storage engine block size, so each one has to
    # become its own block; and enough of them to span more than one query result block
    # (the per-block BLOB budget is 256 MB, i.e. 16 values of 16 MB fill one block).
    BLOB_BIG_ROWS = 18

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="db", drop=True)

    # --- large BLOB helpers ---

    def blob_payload(self, size, seed):
        """Build a payload of exactly `size` bytes: seed marker then a 256-byte ramp.

        Both parts are position dependent, so a truncated, shifted or mixed up value is
        always detected by the length / crc32 / window comparisons below.
        """
        ramp = bytes(range(256))
        body = (ramp * (size // 256 + 1))[:size]
        if size >= 4:
            body = seed.to_bytes(4, "big") + body[4:]
        return body

    def blob_ascii_payload(self, size):
        """Build a payload of exactly `size` printable ASCII bytes.

        A cast to VARCHAR/BINARY/NCHAR is decoded as text, so a ramp payload (which
        contains bytes above 0x7f) cannot be read back on the client side.
        """
        pattern = bytes(range(0x20, 0x7F))
        return (pattern * (size // len(pattern) + 1))[:size]

    def insert_blob_row(self, db, tb, ts, payload):
        tdStmt2.execute_single(
            f"insert into {db}.{tb} values(?, ?)", [ts, payload], expected_rows=1
        )

    def check_blob_value(self, expr, src, payload):
        """Check that the single row selected by `src` yields exactly `payload` for `expr`.

        `expr` is a BLOB expression (a column, an aggregate, a scalar function) and `src`
        is a table name with an optional WHERE clause.

        The value is verified on the server: `length()` for the reported size, `crc32()`
        as a whole-value checksum (the standard CRC-32, comparable to zlib.crc32(), so
        any difference anywhere in the value is caught) and `substr()` windows to pin
        down the content at the start, the middle and the end.  Checking it there also
        proves the value crossed the RPC boundary intact, and keeps the test independent
        of how much of a multi-megabyte value a client chooses to materialize.
        """
        want_len = len(payload)
        tdSql.query(f"select length({expr}) from {src}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, want_len)

        tdSql.query(f"select crc32({expr}) from {src}")
        tdSql.checkData(0, 0, zlib.crc32(payload))

        # start, middle and end of the value; a window at position 1 is always valid and
        # the tail window is clamped for values shorter than the window itself
        width = 16
        for start in sorted({1, max(1, want_len // 2), max(1, want_len - 15)}):
            tdSql.query(f"select substr({expr}, {start}, {width}) from {src}")
            tdSql.checkData(0, 0, payload[start - 1 : start - 1 + width])

    def check_blob_scalar(self, expr, src, expect):
        """Check a single-row scalar expression whose result is not itself a BLOB."""
        tdSql.query(f"select {expr} from {src}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, expect)

    def test_datatype_blob(self):
        """DataTypes: blob

        1. Create table
        2. Insert data
        3. Auto-create table
        4. Alter tag value
        5. Handle illegal input
        6. Check auto create table with blob cols


        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
           - 2025-7-27 yhDeng create 
           - 2025-11-24 Tony Zhang add check auto create table with blob cols

        """
        self.check_limit() 
        self.create_table()
        self.insert_data()
        self.auto_create_table()
        self.alter_tag_value()
        self.illegal_input()
        self.check_auto_create_table()
        self.clearup_data()
        self.checkUnderedData()

    def check_limit(self):  
        tdLog.info(f"create super table")
        tdSql.error(
            f"create table mt_blob_t (ts timestamp, c blob, c2 blob) tags(tagname varbinary(50))"
        )

        tdSql.error(
            f"create table mt_blob_t (ts timestamp, c int) tags(tagname blob)"
        )
        tdSql.execute(
            f"create table mt_blob_t (ts timestamp, c int) tags(t int)"
        )
        tdSql.execute(
            f"alter table mt_blob_t Add column b1 blob"
        )

        tdSql.error(
            f"alter table mt_blob_t Add column b2 blob"
        )

        tdSql.execute(
            f"alter table mt_blob_t drop column b1"
        )

        tdSql.execute(
            f"alter table mt_blob_t add column b1 blob"
        )
       
    def create_table(self):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create table mt_blob (ts timestamp, c blob) tags(tagname varbinary(50))"
        )
        tdSql.error(
            f"ALTER TABLE mt_blob ADD COLUMN c1 BLOB"
        )

        tdLog.info(f"case 0: static create table for test tag values")

        tdSql.execute(f"create table st_blob_0 using mt_blob tags(NULL)")
        tdSql.query(f"show tags from st_blob_0")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_blob_1 using mt_blob tags(NULL)")
        tdSql.query(f"show tags from st_blob_1")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"create table st_blob_2 using mt_blob tags('NULL')")
        tdSql.query(f"show tags from st_blob_2")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f"create table st_blob_3 using mt_blob tags('NULL')")
        tdSql.query(f"show tags from st_blob_3")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'create table st_blob_4 using mt_blob tags("NULL")')
        tdSql.query(f"show tags from st_blob_4")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'create table st_blob_5 using mt_blob tags("NULL")')
        tdSql.query(f"show tags from st_blob_5")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'create table st_blob_6 using mt_blob tags("")')
        tdSql.query(f"show tags from st_blob_6")
        tdSql.checkData(0, 5, "\\x")

        tdSql.execute(f'create table st_blob_7 using mt_blob tags(" ")')
        tdSql.query(f"show tags from st_blob_7")
        tdSql.checkData(0, 5, "\\x20")

        str = "\\x"
        tdSql.execute(f'create table st_blob_8 using mt_blob tags("{str}")')
        tdSql.query(f"show tags from st_blob_8")
        tdSql.checkData(0, 5, "\\x")

        tdSql.execute(f'create table st_blob_9 using mt_blob tags("{str}aB")')
        tdSql.query(f"show tags from st_blob_9")
        tdSql.checkData(0, 5, "\\xAB")

        tdSql.execute(f'create table st_blob_10 using mt_blob tags("aB")')
        tdSql.query(f"show tags from st_blob_10")
        tdSql.checkData(0, 5, "\\x6142")

    def insert_data(self):
        tdLog.info(f"case 1: insert values for test column values")

        tdSql.execute(f"insert into st_blob_0 values(now, NULL)")
        tdSql.query(f"select * from st_blob_0")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_blob_1 values(now, NULL)")
        tdSql.query(f"select * from st_blob_1")
        tdSql.checkData(0, 1, None)

        tdSql.execute(f"insert into st_blob_2 values(now, 'NULL')")
        tdSql.query(f"select * from st_blob_2")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(f"insert into st_blob_3 values(now, 'NULL')")
        tdSql.query(f"select * from st_blob_3")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(f'insert into st_blob_4 values(now, "NULL")')
        tdSql.query(f"select * from st_blob_4")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(f'insert into st_blob_5 values(now, "NULL")')
        tdSql.query(f"select * from st_blob_5")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(f'insert into st_blob_6 values(now, "")')
        tdSql.query(f"select * from st_blob_6")
        tdSql.checkData(0, 1, bytes.fromhex(''))

        tdSql.execute(f'insert into st_blob_7 values(now, " ")')
        tdSql.query(f"select * from st_blob_7")
        tdSql.checkData(0, 1, bytes.fromhex('20'))
        
        tdSql.flushDb("db")
        str = "\\x"
        tdSql.execute(f'insert into st_blob_8 values(now, "{str}")')
        tdSql.query(f"select * from st_blob_8")
        tdSql.checkData(0, 1, bytes.fromhex(''))

        tdSql.execute(f'insert into st_blob_9 values(now, "{str}aB")')
        tdSql.query(f"select * from st_blob_9")
        tdSql.checkData(0, 1, bytes.fromhex('AB'))

        tdSql.execute(f'insert into st_blob_10 values(now, "aB")')
        tdSql.query(f"select * from st_blob_10")
        tdSql.checkData(0, 1, bytes.fromhex('6142'))

    def auto_create_table(self):
        tdLog.info(f"case 2: dynamic create table for test tag values")

        tdSql.execute(
            f"insert into st_blob_100 using mt_blob tags(NULL) values(now,NULL)"
        )
        tdSql.query(f"show tags from st_blob_100")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_blob_100")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_blob_101 using mt_blob tags(NULL) values(now,NULL)"
        )
        tdSql.query(f"show tags from st_blob_101")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"select * from st_blob_101")
        tdSql.checkData(0, 1, None)

        tdSql.execute(
            f"insert into st_blob_102 using mt_blob tags('NULL') values(now,'NULL')"
        )
        tdSql.query(f"show tags from st_blob_102")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.query(f"select * from st_blob_102")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(
            f"insert into st_blob_103 using mt_blob tags('NULL') values(now,'NULL')"
        )
        tdSql.query(f"show tags from st_blob_103")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.query(f"select * from st_blob_103")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(
            f'insert into st_blob_104 using mt_blob tags("NULL") values(now,"NULL")'
        )
        tdSql.query(f"show tags from st_blob_104")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.query(f"select * from st_blob_104")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(
            f'insert into st_blob_105 using mt_blob tags("NULL") values(now,"NULL")'
        )
        tdSql.query(f"show tags from st_blob_105")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.query(f"select * from st_blob_105")
        tdSql.checkData(0, 1, bytes.fromhex('4E554C4C'))

        tdSql.execute(
            f'insert into st_blob_106 using mt_blob tags("") values(now,"")'
        )
        tdSql.query(f"show tags from st_blob_106")
        tdSql.checkData(0, 5, "\\x")

        tdSql.query(f"select * from st_blob_106")
        tdSql.checkData(0, 1, bytes.fromhex(''))

        tdSql.execute(
            f'insert into st_blob_107 using mt_blob tags(" ") values(now," ")'
        )
        tdSql.query(f"show tags from st_blob_107")
        tdSql.checkData(0, 5, "\\x20")

        tdSql.query(f"select * from st_blob_107")
        tdSql.checkData(0, 1, bytes.fromhex('20'))

        str = "\\x"
        tdSql.execute(
            f'insert into st_blob_108 using mt_blob tags("{str}") values(now,"{str}")'
        )
        tdSql.query(f"show tags from st_blob_108")
        tdSql.checkData(0, 5, "\\x")

        tdSql.query(f"select * from st_blob_108")
        tdSql.checkData(0, 1, bytes.fromhex(''))

        tdSql.execute(
            f'insert into st_blob_109 using mt_blob tags("{str}aB") values(now,"{str}aB")'
        )
        tdSql.query(f"show tags from st_blob_109")
        tdSql.checkData(0, 5, "\\xAB")

        tdSql.query(f"select * from st_blob_109")
        tdSql.checkData(0, 1, bytes.fromhex('AB'))

        tdSql.execute(
            f'insert into st_blob_1010 using mt_blob tags("aB") values(now,"aB")'
        )
        tdSql.query(f"show tags from st_blob_1010")
        tdSql.checkData(0, 5, "\\x6142")

        tdSql.query(f"select * from st_blob_1010")
        tdSql.checkData(0, 1, bytes.fromhex('6142'))

    def alter_tag_value(self):
        tdLog.info(f"case 3: alter tag value")

        tdSql.execute(f"alter table st_blob_100  set tag tagname=NULL")
        tdSql.query(f"show tags from st_blob_100")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_blob_101  set tag tagname=NULL")
        tdSql.query(f"show tags from st_blob_101")
        tdSql.checkData(0, 5, None)

        tdSql.execute(f"alter table st_blob_102  set tag tagname='NULL'")
        tdSql.query(f"show tags from st_blob_102")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f"alter table st_blob_103  set tag tagname='NULL'")
        tdSql.query(f"show tags from st_blob_103")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'alter table st_blob_104  set tag tagname="NULL"')
        tdSql.query(f"show tags from st_blob_104")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'alter table st_blob_105  set tag tagname="NULL"')
        tdSql.query(f"show tags from st_blob_105")
        tdSql.checkData(0, 5, "\\x4E554C4C")

        tdSql.execute(f'alter table st_blob_106  set tag tagname=""')
        tdSql.query(f"show tags from st_blob_106")
        tdSql.checkData(0, 5, "\\x")

        tdSql.execute(f'alter table st_blob_107  set tag tagname=" "')
        tdSql.query(f"show tags from st_blob_107")
        tdSql.checkData(0, 5, "\\x20")

        str = "\\x"
        tdSql.execute(f'alter table st_blob_108  set tag tagname="{str}"')
        tdSql.query(f"show tags from st_blob_108")
        tdSql.checkData(0, 5, "\\x")

        tdSql.execute(f'alter table st_blob_109  set tag tagname="{str}aB"')
        tdSql.query(f"show tags from st_blob_109")
        tdSql.checkData(0, 5, "\\xAB")

        tdSql.execute(f'alter table st_blob_1010  set tag tagname="aB"')
        tdSql.query(f"show tags from st_blob_1010")
        tdSql.checkData(0, 5, "\\x6142")

    def illegal_input(self):
        tdLog.info(f"case 4: illegal input")

        tdSql.error(f"create table st_blob_106 using mt_blob tags(+0123)")
        tdSql.error(f"create table st_blob_107 using mt_blob tags(-01.23)")
        tdSql.error(f"create table st_blob_108 using mt_blob tags(+0x01)")
        tdSql.error(f"create table st_blob_109 using mt_blob tags(-0b01)")
        tdSql.error(f"create table st_blob_1010 using mt_blob tags(-0.1e-10)")
        tdSql.error(f"create table st_blob_1011 using mt_blob tags(+0.1E+2)")
        tdSql.error(f"create table st_blob_1012 using mt_blob tags(tRue)")
        tdSql.error(f"create table st_blob_1013 using mt_blob tags(FalsE)")
        tdSql.error(f"create table st_blob_1014 using mt_blob tags(noW)")
        tdSql.error(f"create table st_blob_1015 using mt_blob tags(toDay)")
        tdSql.error(f"create table st_blob_1016 using mt_blob tags(now()+1s)")
        tdSql.error(f"create table st_blob_1017 using mt_blob tags(1+1s)")
        tdSql.error(
            f"insert into st_blob_106 using mt_blob tags(+0123) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_blob_107 using mt_blob tags(-01.23) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_blob_108 using mt_blob tags(+0x01) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_blob_109 using mt_blob tags(-0b01) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_blob_1010 using mt_blob tags(-0.1e-10) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_blob_1011 using mt_blob tags(+0.1E+2) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_blob_1012 using mt_blob tags(tRue) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_blob_1013 using mt_blob tags(FalsE) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_blob_1014 using mt_blob tags(noW) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_blob_1015 using mt_blob tags(toDay) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_blob_1016 using mt_blob tags(now()+1s) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_blob_1017 using mt_blob tags(1+1s) values(now, NULL);"
        )
        tdSql.error(
            f"insert into st_blob_106 using mt_blob tags(NULL) values(now(), +0123)"
        )
        tdSql.error(
            f"insert into st_blob_107 using mt_blob tags(NULL) values(now(), -01.23)"
        )
        tdSql.error(
            f"insert into st_blob_108 using mt_blob tags(NULL) values(now(), +0x01)"
        )
        tdSql.error(
            f"insert into st_blob_109 using mt_blob tags(NULL) values(now(), -0b01)"
        )
        tdSql.error(
            f"insert into st_blob_1010 using mt_blob tags(NULL) values(now(), -0.1e-10)"
        )
        tdSql.error(
            f"insert into st_blob_1011 using mt_blob tags(NULL) values(now(), +0.1E+2)"
        )
        tdSql.error(
            f"insert into st_blob_1012 using mt_blob tags(NULL) values(now(), tRue)"
        )
        tdSql.error(
            f"insert into st_blob_1013 using mt_blob tags(NULL) values(now(), FalsE)"
        )
        tdSql.error(
            f"insert into st_blob_1014 using mt_blob tags(NULL) values(now(), noW)"
        )
        tdSql.error(
            f"insert into st_blob_1015 using mt_blob tags(NULL) values(now(), toDay)"
        )
        tdSql.error(
            f"insert into st_blob_1016 using mt_blob tags(NULL) values(now(), now()+1s)"
        )
        tdSql.error(
            f"insert into st_blob_1017 using mt_blob tags(NULL) values(now(), 1+1s)"
        )
    def clearup_data(self):
        tdSql.execute(f"drop database if exists db ")

    def checkUnderedData(self):
        tdSql.execute(f"create database blob_ordered")
        tdSql.execute(f"use blob_ordered")

        tdSql.execute(f"create table blob_ordered_t (ts timestamp, desc1 blob)")

        tdSql.execute(f"insert into blob_ordered_t values(now, NULL) (now - 1s, NULL) (now - 2s, NULL) (now - 3s, NULL) (now - 4s, NULL) (now - 5s, NULL) (now - 6s, NULL) (now - 7s, NULL) (now - 8s, NULL) (now - 9s, NULL)")       
        tdSql.query(f"select * from blob_ordered_t order by ts")  

        tdSql.checkRows(10)
        for i in range(10):
            tdSql.checkData(i, 1, None)
        tdSql.execute(f"drop database blob_ordered")

    def test_datatype_blob_max_size(self):
        """DataTypes: BLOB payload size boundary after the 4 MB -> 16 MB raise

        1. STMT2 write and read back of payloads around the old 4 MB ceiling and up to the new 16 MB ceiling
        2. Payloads of 64 KB +/- 1, where the length and the CRC stop fitting into 16 bits
        3. One byte above the ceiling is rejected and taosd stays alive
        4. Every accepted payload is byte-exact and its length() is reported correctly

        Catalog:
            - DataTypes

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-20 Bomin Zhang create

        """
        self.do_blob_size_boundary()

    def test_datatype_blob_large_values(self):
        """DataTypes: many maximum size BLOB values in one table

        1. Every value is larger than a storage engine block, so it needs its own block
        2. No row is lost or turned into NULL, every length() is exact
        3. select * spanning several query result blocks returns all rows
        4. first/last/last_row and order by ... desc read back whole values
        5. Columns before and after BLOB stay aligned across byte-limited blocks

        Catalog:
            - DataTypes

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-20 Bomin Zhang create
            - 2026-10-20 Copilot Verify byte-limited blocks and companion columns

        """
        self.do_blob_many_large_values()

    def test_datatype_blob_functions(self):
        """DataTypes: BLOB in aggregate, window and scalar functions

        1. first/last/last_row/mode are byte-exact on multi-megabyte values
        2. first_value/nth_value window functions work on BLOB
        3. length/substr/substring/cast/crc32 return whole values
        4. cast to every numeric, bool, string and timestamp type, in both directions
        5. sample/tail/min/max/greatest/least/unique reject BLOB without killing taosd

        Catalog:
            - DataTypes

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-20 Bomin Zhang create

        """
        self.do_blob_aggregate_functions()
        self.do_blob_window_functions()
        self.do_blob_scalar_functions()
        self.do_blob_cast_functions()
        self.do_blob_unsupported_functions()

    # --- large BLOB cases ---

    def do_blob_size_boundary(self):
        db = "blob_size"
        tb = "t_size"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, v blob)")

        base_ts = 1700000000000
        # The interesting sizes are 65535/65536/65537, where a 16-bit length would stop
        # being able to describe the value, and the ceiling itself.
        sizes = [
            1,
            4096,
            65535,
            65536,
            65537,
            8 << 20,
            self.MAX_BLOB_PAYLOAD,
        ]

        for i, size in enumerate(sizes):
            ts = base_ts + i * 1000
            self.insert_blob_row(db, tb, ts, self.blob_payload(size, 0x10 + i))
            tdLog.info(f"inserted payload of {size} bytes")

        tdSql.query(f"select ts, length(v) from {tb} order by ts")
        tdSql.checkRows(len(sizes))
        for i, size in enumerate(sizes):
            tdSql.checkData(i, 1, size)

        for i, size in enumerate(sizes):
            ts = base_ts + i * 1000
            self.check_blob_value("v", f"{tb} where ts = {ts}", self.blob_payload(size, 0x10 + i))

        # one byte above the ceiling has to be refused.  Whether the failure surfaces as
        # an exception or not, the rejected row must not end up in the table, so the row
        # count is what is checked.
        try:
            self.insert_blob_row(
                db,
                tb,
                base_ts + 100000,
                self.blob_payload(self.MAX_BLOB_PAYLOAD + 1, 0x40),
            )
        except Exception as err:
            tdLog.info(f"payload above the ceiling rejected: {err}")

        tdSql.query(f"select count(*) from {tb}")
        tdSql.checkData(0, 0, len(sizes))

        tdSql.execute(f"drop database if exists {db}")
        tdLog.info("blob payload size boundary ................. [ passed ]")

    def test_blob_count_sma(self):
        """BLOB count uses block statistics without materializing payloads.

        1. Count mixed NULL, empty and nonempty values before and after flush
        2. Verify clean blocks use SMA and filtered queries retain exact counts
        3. Verify columns added after flush count historical rows as NULL

        Catalog:
            - DataTypes

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-20 Copilot Add BLOB count SMA regression

        """
        self.do_blob_count_sma()

    def do_blob_count_sma(self):
        db = "blob_count_sma"
        rows = 8192
        base_ts = 1700000000000
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(
            f"create database {db} vgroups 1 stt_trigger 1 "
            "minrows 100 maxrows 4096 keep 36500"
        )
        tdSql.execute(f"create table {db}.t (ts timestamp, v blob, n int)")
        tdSql.execute(f"create table {db}.added (ts timestamp, n int)")
        tdSql.execute(f"create table {db}.all_null (ts timestamp, v blob)")
        for start in range(0, rows, 512):
            values = []
            for i in range(start, start + 512):
                value = ("NULL", "'\\x'", "'\\x616263'", "'\\x00'")[i % 4]
                values.append(f"({base_ts + i}, {value}, {i})")
            tdSql.execute(f"insert into {db}.t values " + " ".join(values))
            tdSql.execute(
                f"insert into {db}.added values "
                + " ".join(f"({base_ts + i}, {i})" for i in range(start, start + 512))
            )
            tdSql.execute(
                f"insert into {db}.all_null values "
                + " ".join(f"({base_ts + i}, NULL)" for i in range(start, start + 512))
            )

        for flushed in (False, True):
            if flushed:
                tdSql.flushDb(db)
            tdSql.query(f"select count(v), count(n), count(*) from {db}.t")
            for col, expected in enumerate((rows * 3 // 4, rows, rows)):
                tdSql.checkData(0, col, expected)
            tdSql.query(f"select count(v) from {db}.t where n < 1000")
            tdSql.checkData(0, 0, 750)
            tdSql.query(f"select count(v) from {db}.t where ts < {base_ts + 513}")
            tdSql.checkData(0, 0, 384)
            tdSql.query(f"select count(v) from {db}.t where length(v) > 0")
            tdSql.checkData(0, 0, rows // 2)
            tdSql.query(f"select count(v) from {db}.all_null")
            tdSql.checkData(0, 0, 0)
            tdSql.query(f"select count(v) from {db}.t where ts < {base_ts}")
            tdSql.checkData(0, 0, 0)

        tdSql.query(f"explain analyze verbose true select count(v) from {db}.t")
        plan = "\n".join(str(row[0]) for row in tdSql.queryResult)
        sma_blocks = re.search(r"sma_load_blocks=(\d+)", plan)
        assert sma_blocks is not None and int(sma_blocks.group(1)) > 0, plan
        assert re.search(r"check_rows=0\b", plan), plan

        tdSql.execute(f"alter table {db}.added add column v blob")
        tdSql.query(f"select count(v), count(n) from {db}.added")
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, rows)
        tdSql.execute(f"drop database {db}")
        tdLog.info("blob count block statistics ............... [ passed ]")

    def do_blob_many_large_values(self):
        db = "blob_big"
        tb = "t_big"
        rows = self.BLOB_BIG_ROWS
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {tb} (ts timestamp, before_blob int, v blob, "
            "after_blob int, text_col varchar(32))"
        )

        base_ts = 1700000000000
        payloads = []
        for i in range(rows):
            payload = self.blob_payload(self.MAX_BLOB_PAYLOAD, 0x20 + i)
            payloads.append(payload)
            tdStmt2.execute_single(
                f"insert into {db}.{tb} values(?, ?, ?, ?, ?)",
                [base_ts + i * 1000, i, payload, -i, f"row-{i}"],
                expected_rows=1,
            )
        tdSql.flushDb(db)
        tdLog.info(f"inserted {rows} rows at the maximum payload size")

        # A storage block that is built while a value does not fit used to be flushed
        # without it, so exactly one row per block read back as NULL.  count(v) skips
        # NULL, so it is the direct regression check for that.
        tdSql.query(f"select count(*), count(v) from {tb}")
        tdSql.checkData(0, 0, rows)
        tdSql.checkData(0, 1, rows)

        tdSql.query(f"select length(v) from {tb} order by ts")
        tdSql.checkRows(rows)
        for i in range(rows):
            tdSql.checkData(i, 0, self.MAX_BLOB_PAYLOAD)

        for order in ("asc", "desc"):
            tdSql.query(
                f"select before_blob, length(v), crc32(v), after_blob, text_col "
                f"from {tb} order by ts {order}"
            )
            tdSql.checkRows(rows)
            for i in range(rows):
                source = i if order == "asc" else rows - i - 1
                tdSql.checkData(i, 0, source)
                tdSql.checkData(i, 1, self.MAX_BLOB_PAYLOAD)
                tdSql.checkData(i, 2, zlib.crc32(payloads[source]))
                tdSql.checkData(i, 3, -source)
                tdSql.checkData(i, 4, f"row-{source}")

        # more values than fit into a single result block, so the operator has to emit
        # intermediate blocks and keep merging the rest
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(rows)

        self.check_blob_value("first(v)", tb, payloads[0])
        self.check_blob_value("last(v)", tb, payloads[-1])
        self.check_blob_value("last_row(v)", tb, payloads[-1])

        # order by ... desc has to keep the values intact while sorting and limiting
        want = [zlib.crc32(payloads[-1]), zlib.crc32(payloads[-2])]
        tdSql.query(f"select length(v), crc32(v) from {tb} order by ts desc limit 2")
        tdSql.checkRows(2)
        for i in range(2):
            tdSql.checkData(i, 0, self.MAX_BLOB_PAYLOAD)
            tdSql.checkData(i, 1, want[i])

        tdSql.execute(f"drop database if exists {db}")
        tdLog.info("blob many maximum size values .............. [ passed ]")

    def do_blob_aggregate_functions(self):
        db = "blob_func"
        tb = "t_func"
        size = self.MAX_BLOB_PAYLOAD
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, v blob)")

        # 3 rows of A and one each of B/C/D, so mode() has a defined answer and
        # first/last differ from each other.
        payloads = {
            "A": self.blob_payload(size, 0xA1),
            "B": self.blob_payload(size, 0xB2),
            "C": self.blob_payload(size, 0xC3),
            "D": self.blob_payload(size, 0xD4),
        }
        layout = ["A", "A", "A", "B", "C", "D"]
        base_ts = 1700000000000
        for i, key in enumerate(layout):
            self.insert_blob_row(db, tb, base_ts + i * 1000, payloads[key])
        tdSql.flushDb(db)

        # the aggregates keep one whole value per group in their intermediate buffer;
        # that buffer used to be sized from the declared column width (a few bytes for
        # BLOB), so these either returned a prefix or corrupted the heap
        self.check_blob_value("first(v)", tb, payloads["A"])
        self.check_blob_value("last(v)", tb, payloads["D"])
        self.check_blob_value("last_row(v)", tb, payloads["D"])
        self.check_blob_value("mode(v)", tb, payloads["A"])

        # ... and the same values have to come out of the ordered scan, which builds the
        # block through a completely different code path
        tdSql.query(f"select length(v), crc32(v) from {tb} order by ts asc limit 1")
        tdSql.checkData(0, 0, size)
        tdSql.checkData(0, 1, zlib.crc32(payloads["A"]))
        tdSql.query(f"select length(v), crc32(v) from {tb} order by ts desc limit 1")
        tdSql.checkData(0, 0, size)
        tdSql.checkData(0, 1, zlib.crc32(payloads["D"]))

        tdLog.info("blob first/last/last_row/mode ............... [ passed ]")

    def do_blob_window_functions(self):
        db = "blob_win"
        tb = "t_win"
        rows = 4
        # Any value above the 4 KB page the operator used to allocate reproduces the
        # original failure, so a directly comparable size is enough here.
        size = 60000
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, v blob)")

        base_ts = 1700000000000
        payloads = []
        for i in range(rows):
            payload = self.blob_payload(size, 0xE0 + i)
            payloads.append(payload)
            self.insert_blob_row(db, tb, base_ts + i * 1000, payload)
        tdSql.flushDb(db)

        # the window function operator stores complete input rows in a paged buffer whose
        # page size came from the declared row width, which is unusable for BLOB.  A
        # window function cannot be wrapped in a scalar function, so the values are read
        # as they are, and any value above the old 4 KB page already reproduces the
        # original failure.  They are compared byte for byte.
        for expr in ("first_value(v) over (order by ts)", "nth_value(v, 1) over (order by ts)"):
            tdSql.query(f"select {expr} from {tb}")
            tdSql.checkRows(rows)
            for i in range(rows):
                tdSql.checkData(i, 0, payloads[0])

        tdLog.info("blob window functions ...................... [ passed ]")

    def do_blob_scalar_functions(self):
        db = "blob_sca"
        tb = "t_sca"
        size = self.MAX_BLOB_PAYLOAD
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, v blob)")

        payload = self.blob_payload(size, 0x5A)
        self.insert_blob_row(db, tb, 1700000000000, payload)
        tdSql.flushDb(db)

        # length() and crc32() are checked by check_blob_value for every value it sees;
        # they are the two scalar functions that reduce a BLOB to a number
        tdSql.query(f"select length(v) from {tb}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, size)

        # a scalar function that copies the value out must size its buffer from the real
        # BLOB bound, not from the declared column width
        self.check_blob_value("cast(v as blob)", tb, payload)
        self.check_blob_value("v", tb, payload)

        # substr() and its alias substring(), with windows taken from the head, the middle
        # and the tail of the value, i.e. copies that start far into a multi-megabyte buffer
        for func in ("substr", "substring"):
            for start in (1, size // 2, size - 15):
                self.check_blob_value(
                    f"{func}(v, {start}, 16)", tb, payload[start - 1 : start - 1 + 16]
                )

        tdSql.execute(f"drop database if exists {db}")
        tdLog.info("blob scalar functions ...................... [ passed ]")

    def do_blob_cast_functions(self):
        db = "blob_cast"
        tb = "t_cast"
        fits = self.BLOB_CAST_BUFFER - 1
        over = self.BLOB_CAST_BUFFER
        number = b"123"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, v blob)")

        # "123" followed by trailing junk parses as 123, so the converted value is known
        # at the buffer boundary as well and both sides of it can be checked exactly
        rows = [
            ("number", number),
            ("at_limit", number + b"x" * (fits - len(number))),
            ("over_limit", number + b"x" * (over - len(number))),
            ("ascii_big", self.blob_ascii_payload(1 << 20)),
            ("ramp_big", self.blob_payload(self.MAX_BLOB_PAYLOAD, 0x5A)),
        ]
        base_ts = 1700000000000
        ts_of = {}
        for i, (key, payload) in enumerate(rows):
            ts_of[key] = base_ts + i * 1000
            self.insert_blob_row(db, tb, ts_of[key], payload)
        tdSql.flushDb(db)

        # all eleven numeric and bool output types, each with its own conversion buffer
        numeric = [
            ("tinyint", 123),
            ("smallint", 123),
            ("int", 123),
            ("bigint", 123),
            ("tinyint unsigned", 123),
            ("smallint unsigned", 123),
            ("int unsigned", 123),
            ("bigint unsigned", 123),
            ("float", 123.0),
            ("double", 123.0),
            ("bool", True),
        ]

        src_small = f"{tb} where ts = {ts_of['number']}"
        src_at_limit = f"{tb} where ts = {ts_of['at_limit']}"
        src_over = f"{tb} where ts = {ts_of['over_limit']}"
        src_ramp = f"{tb} where ts = {ts_of['ramp_big']}"

        # a value that fits is parsed into every one of the output types.  This is the
        # half that a wrong guard would break: the branch used to compare the length
        # against sizeof(buf) on a char*, i.e. against 8, so anything longer than 8 bytes
        # was refused even though it fits the buffer.
        for type_name, expect in numeric:
            self.check_blob_scalar(f"cast(v as {type_name})", src_small, expect)

        # exactly one byte below the buffer size still fits ...
        tdSql.query(f"select length(v) from {src_at_limit}")
        tdSql.checkData(0, 0, fits)
        for type_name, expect in numeric:
            self.check_blob_scalar(f"cast(v as {type_name})", src_at_limit, expect)

        # ... and one byte more is refused instead of being copied into the buffer
        tdSql.query(f"select length(v) from {src_over}")
        tdSql.checkData(0, 0, over)
        for type_name, _ in numeric:
            tdSql.error(f"select cast(v as {type_name}) from {src_over}")

        # a multi-megabyte value is refused the same way.  Ten of the eleven branches had
        # no bound at all, so this memcpy'd the whole value into the conversion buffer.
        for type_name, _ in numeric:
            tdSql.error(f"select cast(v as {type_name}) from {src_ramp}")

        tdLog.info("blob cast to numeric types ................. [ passed ]")

        # --- BLOB -> string types: truncated to the declared width, content is the prefix ---
        ascii_payload = rows[3][1]
        src_ascii = f"{tb} where ts = {ts_of['ascii_big']}"

        # varbinary keeps the bytes, so the content can be compared exactly
        for width in (1, 64, 1000):
            tdSql.query(f"select cast(v as varbinary({width})) from {src_ascii}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, ascii_payload[:width])

        for type_name in ("binary(64)", "varchar(64)", "nchar(64)"):
            tdSql.query(f"select cast(v as {type_name}) from {src_ascii}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, ascii_payload[:64].decode("ascii"))

        # a value whose leading bytes are not valid UTF-8 cannot become NCHAR, so the
        # conversion is reported as an error instead of producing garbage
        tdSql.error(f"select cast(v as nchar(1000)) from {src_ramp}")

        tdLog.info("blob cast to string types .................. [ passed ]")

        # --- the other direction: a scalar value cast to BLOB ---
        # NUM_TO_STRING has no BOOL case, so BOOL needs its own branch on this side
        for expr, expect in [
            ("cast(cast(1 as int) as blob)", b"1"),
            ("cast(cast(true as bool) as blob)", b"true"),
            ("cast(cast(false as bool) as blob)", b"false"),
            ("cast(cast(1.5 as double) as blob)", b"1.5"),
            ("cast(cast(1.50 as decimal(10,2)) as blob)", b"1.50"),
            ("cast('abc' as blob)", b"abc"),
        ]:
            tdSql.query(f"select {expr}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, expect)

        # a BLOB holding a timestamp has to reach the same value as the plain string
        # route; comparing them inside the query keeps the check independent of the
        # server timezone
        tdSql.query(
            "select cast(cast('2023-11-15 06:13:20' as blob) as timestamp) = "
            "cast('2023-11-15 06:13:20' as timestamp)"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, True)

        tdSql.execute(f"drop database if exists {db}")
        tdLog.info("blob cast from scalar types ................ [ passed ]")

    def do_blob_unsupported_functions(self):
        db = "blob_func"
        tb = "t_func"
        tdSql.execute(f"use {db}")

        # sample/tail store their values inline at a fixed stride taken from the declared
        # width, and min/max are TSMA accelerated without BLOB support, so all of them
        # reject BLOB by design
        for sql in [
            f"select sample(v, 10) from {tb}",
            f"select tail(v, 10) from {tb}",
            f"select min(v) from {tb}",
            f"select max(v) from {tb}",
            f"select greatest(v, cast(v as blob)) from {tb}",
            f"select least(v, cast(v as blob)) from {tb}",
        ]:
            tdSql.error(sql)

        # unique() is planned as a hash group by whose key must fit the declared width,
        # so it fails with an executor error rather than returning wrong data
        tdSql.error(f"select unique(v) from {tb}")

        # none of the rejected statements above may have taken the server down
        tdSql.query(f"select count(v) from {tb}")
        tdSql.checkData(0, 0, 6)

        tdSql.execute(f"drop database if exists {db}")
        tdLog.info("blob unsupported functions ................. [ passed ]")

    def check_auto_create_table(self):
        tdLog.info(f"case 6: check auto create table with blob cols")

        tdSql.execute("""
            create table auto_blob_t (ts timestamp, c1 blob) tags(t1 int)""")
        tdSql.execute("""
            insert into auto_blob_t (tbname, ts, c1, t1) values ('ctb1', now,
            '\x48656c6c6f20576f726c64', 1)""")
        
        tdSql.query(f"select c1 from ctb1")
        tdSql.checkRows(1)
        hex_str: str = tdSql.getData(0, 0).decode()  # 'H656c6c6f20576f726c64'
        assert b'Hello World' == hex_str[0].encode() + bytes.fromhex(hex_str[1:])
