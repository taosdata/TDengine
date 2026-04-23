from new_test_framework.utils import tdLog, tdSql, tdDnodes, etool, TDSetSql
from taos.tmq import Consumer
from itertools import product
import os
import time
import shutil

class TestCase:

    test_pass = "Password_123!"

    @classmethod
    def setup_cls(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.setsql = TDSetSql()

    def do_basic_user_privileges(self):
        """Test basic user privileges(grant/revoke/show user privileges)"""
        tdSql.connect("root", "taosdata")
        # Best-effort cleanup for previous interrupted runs.
        try:
            tdSql.execute("drop topic if exists topic1", queryTimes=1)
        except Exception:
            pass
        for stmt in [
            "drop database if exists d1",
            "drop database if exists d2",
            "drop database if exists d3",
            "drop database if exists d_mask",
            "drop role r1",
            "drop user u1",
            "drop user u2",
            "drop user u_col_2",
            "drop user u_topic",
            "drop user u_consumer",
            "drop user ur1",
            "drop user u3",
            "drop user u_legacy",
            "drop user u_mask",
        ]:
            try:
                tdSql.execute(stmt, queryTimes=1)
            except Exception:
                pass

        tdSql.execute("drop database if exists d0")
        tdSql.execute("create database d0 keep 36500")
        tdSql.execute("use d0")
        tdSql.execute("create table d0.stb0 (ts timestamp, c0 int,c1 int) tags(t1 int)")
        tdSql.execute("create table d0.stb2 (ts timestamp, c0 int,c1 int) tags(t1 int)")
        tdSql.execute("create table ctb0 using stb0 tags(0)")
        tdSql.execute("create table ctb1 using stb0 tags(1)")
        tdSql.execute("insert into ctb0 values(now,0,0)")
        tdSql.execute("insert into ctb0 values(now+1s,10,10)")
        tdSql.execute("insert into ctb1 values(now,1,1)")
        tdSql.execute("insert into ctb1 values(now+1s,11,11)")
        tdSql.execute("create table ctb2 using stb2 tags(0)")
        tdSql.execute("insert into ctb2 values(now,2,2)")
        tdSql.execute("insert into ctb2 values(now+1s,22,22)")
        tdSql.execute("select * from d0.stb0")
        tdSql.execute("flush database d0")

        tdSql.execute(f"create user u1 pass '{self.test_pass}'");
        tdSql.execute(f"create user u2 pass '{self.test_pass}'")
        tdSql.execute("create role r1")
        tdSql.execute("show roles")
        tdSql.execute("show role privileges")

        tdSql.execute("grant role r1 to u1")
        tdSql.execute("revoke role `SYSINFO_1` from u1")
        tdSql.execute("show users")
        tdSql.execute("show user privileges")
        tdSql.execute("grant create database to u1")
        tdSql.execute("grant create table on database d0 to u1")
        tdSql.execute("grant use database on database d0 to u1")
        tdSql.execute("grant use on database d0 to u1")
        tdSql.execute("grant lock role,unlock role,lock user,unlock user to u1")
        tdSql.execute("grant select(c0,c1),insert(ts,c0),delete on table d0.stb0 with t1=0 and ts=0 to u1")

    def do_basic_role_privileges(self):
        """Test basic role privileges(grant/revoke/show role privileges)"""
        
        tdSql.execute("grant select on table d0.stb0 to r1")
        tdSql.execute("grant insert on table d0.stb0 to r1")
        tdSql.execute("show role privileges")
        tdSql.execute("revoke insert on table d0.stb0 from r1")
        tdSql.execute("show role privileges")
        tdSql.execute("show role privileges")
        tdSql.execute("revoke select on table d0.stb0 from r1")
        tdSql.execute("show role privileges")
        tdSql.error("grant insert(c0,c1),delete on table d0.stb0 to r1", expectErrInfo="Lack of primary key column", fullMatched=False)
        tdSql.execute("grant select(c0,c1),insert(ts,c0),delete on table d0.stb0 with t1=0 to r1")
        tdSql.error("grant select(c0,c1),insert(ts,c0),delete on table d0.stb0 with t1=0 and ts=0 to r1", expectErrInfo="Already have this privilege", fullMatched=False)
        tdSql.execute("revoke all on table d0.stb0 from r1")
        tdSql.execute("grant select(c0,c1),insert(ts,c0),delete on table d0.stb0 with t1=0 and ts=0 to r1")

    def do_check_column_privileges(self):
        """Test column privileges"""

        tdSql.execute(f"create user u_col_2 pass '{self.test_pass}'")
        tdSql.execute(f"grant use on database d0 to u_col_2")
        tdSql.execute(f"grant select(c0),insert(ts,c0) on table d0.stb0 with t1=0 to u_col_2")
        tdSql.connect("u_col_2", self.test_pass)
        tdSql.error("select * from d0.stb0 where t1=0", expectErrInfo="Permission denied for column: ts", fullMatched=False)
        tdSql.error("select c0,c1 from d0.stb0", expectErrInfo="Permission denied for column: c1", fullMatched=False)
        tdSql.error("select c0,t1 from d0.stb0", expectErrInfo="Permission denied for column: t1", fullMatched=False)
        tdSql.error("select c0 from d0.stb0 where t1=0 and ts=0", expectErrInfo="Permission denied for column: ts", fullMatched=False)
        tdSql.error("select c0,t1 from d0.ctb0", expectErrInfo="Permission denied for column: t1", fullMatched=False)
        tdSql.query("select c0 from d0.stb0")
        tdSql.checkRows(2)
        tdSql.query("select c0 from d0.ctb0")
        tdSql.checkRows(2)
        tdSql.error("select c1 from d0.stb0 where t1=0", expectErrInfo="Permission denied for column: c1", fullMatched=False)
        for i in range(10):
            tdSql.execute("insert into d0.ctb0 (ts,c0) values(now+%ds,%d)" % (i, i))
            tdSql.error("insert into d0.ctb0 (ts,c1) values(now+%ds,%d)" % (i, i), expectErrInfo="Permission denied for column: c1", fullMatched=False)

    def subscribe_topic(self, user, password, group_id, topic_name):
        attr = {
            'group.id': group_id,
            'td.connect.user': user,
            'td.connect.pass': password,
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(attr)
        consumer.subscribe([topic_name])

    def do_check_topic_privileges(self):
        """Test topic privileges"""
        tdSql.connect("root", "taosdata")
        tdSql.execute(f"create user u_topic pass '{self.test_pass}'")
        tdSql.execute(f"create user u_consumer pass '{self.test_pass}'")
        tdSql.execute(f"grant use on database d0 to u_topic")
        tdSql.execute(f"grant create topic on database d0 to u_topic")
        tdSql.execute(f"grant select on d0.stb0 to u_topic")
        tdSql.connect("u_topic", self.test_pass)
        time.sleep(5)  # wait for privileges to take effect
        tdSql.query("select * from d0.stb0")
        tdSql.execute(f"create topic topic1 as select * from d0.stb0")
        tdSql.error(f"create topic topic2 as select * from d0.stb1", expectErrInfo="Permission denied", fullMatched=False)
        self.subscribe_topic("u_topic", self.test_pass, "g1", "topic1")
        tdSql.execute(f"show consumers")
        tdSql.connect("root", "taosdata")
        tdSql.execute(f"grant use on database d0 to u_consumer")
        tdSql.execute(f"grant subscribe on topic d0.topic1 to u_consumer")
        tdSql.connect("u_consumer", self.test_pass)
        time.sleep(5)  # wait for privileges to take effect
        self.subscribe_topic("u_consumer", self.test_pass, "g1", "topic1")
        # check legacy grammar of topics
        tdSql.connect("root", "taosdata")
        tdSql.query(f"select * from information_schema.ins_user_privileges where priv_scope='TOPIC'")
        tdSql.checkRows(1)
        tdSql.execute("revoke subscribe on d0.topic1 from u_consumer")
        tdSql.query(f"select * from information_schema.ins_user_privileges where priv_scope='TOPIC'")
        tdSql.checkRows(0)
        tdSql.error(f"grant subscribe on topic db_none.topic_none to u_consumer", expectErrInfo="Database not exist", fullMatched=False)
        tdSql.error(f"grant subscribe on db_none.topic_none to u_consumer", expectErrInfo="Database not exist", fullMatched=False)
        tdSql.error(f"grant subscribe on topic d0.topic_none to u_consumer", expectErrInfo="Topic not exist", fullMatched=False)
        tdSql.error(f"grant subscribe on d0.topic_none to u_consumer", expectErrInfo="Topic not exist", fullMatched=False)
        tdSql.error(f"grant all on d0.topic_none to u_consumer", expectErrInfo="Grant object not exist", fullMatched=False)
        tdSql.error(f"grant read on d0.topic_none to u_consumer", expectErrInfo="Grant object not exist", fullMatched=False)
        tdSql.error(f"grant write on d0.topic_none to u_consumer", expectErrInfo="Grant object not exist", fullMatched=False)
        tdSql.error(f"grant read,write,show,show create on d0.topic_none to u_consumer", expectErrInfo="Grant object not exist", fullMatched=False)
        tdSql.error(f"grant all,read,write,show,show create on d0.topic_none to u_consumer", expectErrInfo="Cannot mix ALL PRIVILEGES with other privileges", fullMatched=False)
        tdSql.error(f"grant show on d0.topic_none to u_consumer", expectErrInfo="Grant object not exist", fullMatched=False)
        tdSql.error(f"grant show create on d0.topic_none to u_consumer", expectErrInfo="Grant object not exist", fullMatched=False)
        tdSql.error(f"grant alter on d0.topic_none to u_consumer", expectErrInfo="Conflict between privilege type and target", fullMatched=False)
        tdSql.error(f"grant create user on d0.topic_none to u_consumer", expectErrInfo="System privileges should not have target", fullMatched=False)
        tdSql.error(f"grant create user,select on d0.topic_none to u_consumer", expectErrInfo="System privileges and object privileges cannot be mixed", fullMatched=False)
        tdSql.execute(f"grant subscribe on topic1 to u_consumer")
        tdSql.query(f"select * from information_schema.ins_user_privileges where priv_scope='TOPIC'")
        tdSql.checkRows(1)

    def do_check_role_privileges(self):
        """Test role privileges"""
        tdSql.connect("root", "taosdata")
        tdSql.execute(f"create user ur1 pass '{self.test_pass}'")
        tdSql.execute(f"grant role `SYSDBA` to ur1")
        tdSql.error("grant role `SYSSEC` to ur1", expectErrInfo=f"Conflicts with existing role", fullMatched=False)
        tdSql.error("grant role `SYSAUDIT` to ur1", expectErrInfo=f"Conflicts with existing role", fullMatched=False)
        tdSql.error("grant role `SYSAUDIT_LOG` to ur1", expectErrInfo=f"Conflicts with existing role", fullMatched=False)
        tdSql.execute(f"grant role `SYSINFO_0` to ur1")
        tdSql.execute(f"grant role `SYSINFO_1` to ur1")
        tdSql.execute(f"show users")

    def do_check_6841225129(self):
        """ Test for drop not exist table """

        tdSql.execute("drop database if exists d1")
        tdSql.execute("create database d1 keep 36500")
        tdSql.execute("use d1")
        tdSql.execute(f"create user u3 pass '{self.test_pass}'")
        tdSql.execute("drop table if exists d1.not_exist_table")
        tdSql.error("drop table d1.not_exist_table", expectErrInfo="Table does not exist", fullMatched=False)
        tdSql.connect("u3", self.test_pass)
        tdSql.error("drop table if exists d1.not_exist_table", expectErrInfo="Permission denied to use database", fullMatched=False)
        tdSql.error("drop table d1.not_exist_table", expectErrInfo="Permission denied to use database", fullMatched=False)
        tdSql.connect("root", "taosdata")
        tdSql.execute("grant use on database d1 to u3")
        tdSql.execute("grant drop on table d1.* to u3")
        tdSql.connect("u3", self.test_pass)
        tdSql.execute("drop table if exists d1.not_exist_table")
        tdSql.error("drop table d1.not_exist_table", expectErrInfo="Table does not exist", fullMatched=False)
        tdSql.connect("root", "taosdata")
        tdSql.execute("revoke drop on table d1.* from u3")
        tdSql.connect("u3", self.test_pass)
        time.sleep(5)  # wait for privileges to take effect
        tdSql.error("drop table if exists d1.not_exist_table", expectErrInfo="Permission denied or target object not exist", fullMatched=False)
        tdSql.error("drop table d1.not_exist_table", expectErrInfo="Permission denied or target object not exist", fullMatched=False)
        tdSql.connect("root", "taosdata")
        tdSql.execute("grant create database to u3")
        tdSql.connect("u3", self.test_pass)
        tdSql.execute("create database d2")
        tdSql.execute("drop table if exists d2.not_exist_table")
        tdSql.error("drop table d2.not_exist_table", expectErrInfo="Table does not exist", fullMatched=False)

    def do_check_user_privileges(self, user, expected_privs):
        tdSql.query(f"select * from information_schema.ins_user_privileges where user_name='{user}'")
        tdSql.checkRows(expected_privs)

    def do_check_legacy_grammar(self):
        """ Test for legacy grammar of privileges: 6841578151 """

        dict_array = [
            {"enableGrantLegacySyntax": 1, "readPrivNum": 29, "writePrivNum": 31, "allPrivNum": 59},
            {"enableGrantLegacySyntax": 0, "readPrivNum": 6, "writePrivNum": 10, "allPrivNum": 16},
        ]

        tdSql.connect("root", "taosdata")
        tdSql.execute("drop database if exists d3")
        tdSql.execute("create database d3 keep 36500")
        tdSql.execute("use d3")
        tdSql.execute(f"create user u_legacy pass '{self.test_pass}'")
        for item in dict_array:
            tdSql.execute(f"alter all dnodes 'enableGrantLegacySyntax {item['enableGrantLegacySyntax']}'")
            self.do_check_user_privileges("u_legacy", 0)
            tdSql.execute("grant all on d3.* to u_legacy")
            self.do_check_user_privileges("u_legacy", item["allPrivNum"])
            tdSql.execute("revoke all on d3 from u_legacy")
            self.do_check_user_privileges("u_legacy", 0)
            tdSql.checkRows(0)
            tdSql.execute("grant all on d3 to u_legacy")
            self.do_check_user_privileges("u_legacy", item["allPrivNum"])
            tdSql.execute("revoke all on d3.* from u_legacy")
            self.do_check_user_privileges("u_legacy", 0)
            tdSql.execute("grant read on d3 to u_legacy")
            self.do_check_user_privileges("u_legacy", item["readPrivNum"])
            tdSql.execute("revoke all on d3 from u_legacy")
            self.do_check_user_privileges("u_legacy", 0)
            tdSql.execute("grant read,write on d3.* to u_legacy")
            self.do_check_user_privileges("u_legacy", item["allPrivNum"])
            tdSql.execute("revoke read,write on d3 from u_legacy")
            self.do_check_user_privileges("u_legacy", 0)
            tdSql.execute("grant all on d3 to u_legacy")
            self.do_check_user_privileges("u_legacy", item["allPrivNum"])
            tdSql.execute("revoke read,write on d3.* from u_legacy")
            self.do_check_user_privileges("u_legacy", 0)

    def _check_mask_scalar_funcs(self, table, col="c1"):
        """Verify scalar string functions on a masked column.
        With column-level masking (value replacement at column reference),
        functions naturally operate on '*'."""
        masked_exprs = [
            # basic string transforms
            (f"upper({col})", '*'),
            (f"lower({col})", '*'),
            (f"ltrim({col})", '*'),
            (f"rtrim({col})", '*'),
            (f"trim({col})", '*'),
            (f"substr({col}, 1, 1)", '*'),
            (f"substring({col}, 1, 1)", '*'),
            (f"concat({col}, 'x')", '*x'),
            (f"replace({col}, 'a', 'b')", '*'),
            (f"repeat({col}, 2)", '**'),
            # nested scalar
            (f"lower(upper({col}))", '*'),
            (f"ltrim(rtrim({col}))", '*'),
            (f"upper(concat({col}, 'x'))", '*X'),
        ]
        for expr, expected in masked_exprs:
            tdSql.query(f"select {expr} from {table} limit 1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, expected)

    def _check_mask_substr_funcs(self, table, col="c1"):
        """Verify substring_index and position on masked column."""
        # substring_index('*', '.', 1) -> '*' (no delimiter found)
        tdSql.query(f"select substring_index({col}, '.', 1) from {table} limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*')
        # position('*' in '*') -> 1
        tdSql.query(f"select position('*' in {col}) from {table} limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        # find_in_set('*', '*') -> 1
        tdSql.query(f"select find_in_set({col}, '*,a,b') from {table} limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

    def _check_mask_ascii_char_funcs(self, table, col="c1"):
        """Verify ascii() on masked column returns ascii of '*' = 42."""
        tdSql.query(f"select ascii({col}) from {table} limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 42)

    def _check_mask_hash_funcs(self, table, col="c1"):
        """Verify hash/digest functions on masked column operate on '*'."""
        # md5('*') is deterministic — just verify it returns a non-null string
        for func in ["md5", "sha1", "sha"]:
            tdSql.query(f"select {func}({col}) from {table} limit 1")
            tdSql.checkRows(1)
            val = tdSql.queryResult[0][0]
            if val is None or len(val) == 0:
                raise Exception(f"{func} returned empty on masked col")
        # sha2('*', 256) — needs hash length param
        tdSql.query(f"select sha2({col}, 256) from {table} limit 1")
        tdSql.checkRows(1)
        val = tdSql.queryResult[0][0]
        if val is None or len(val) == 0:
            raise Exception("sha2 returned empty on masked col")

    def _check_mask_encoding_funcs(self, table, col="c1"):
        """Verify base64 encode/decode on masked column."""
        # to_base64('*') -> 'Kg=='
        tdSql.query(f"select to_base64({col}) from {table} limit 1")
        tdSql.checkRows(1)
        val = tdSql.queryResult[0][0]
        if val is None or len(val) == 0:
            raise Exception("to_base64 returned empty on masked col")
        # round-trip: from_base64(to_base64('*')) -> '*'
        tdSql.query(f"select from_base64(to_base64({col})) from {table} limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*')

    def _check_mask_conversion_funcs(self, table, col="c1"):
        """Verify cast and conversion functions on masked column."""
        # cast masked col to nchar
        tdSql.query(f"select cast({col} as nchar(10)) from {table} limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*')
        # cast masked col to varchar
        tdSql.query(f"select cast({col} as varchar(10)) from {table} limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*')

    def _check_mask_comparison_funcs(self, table, col="c1"):
        """Verify greatest/least on masked column."""
        tdSql.query(f"select greatest({col}, 'a') from {table} limit 1")
        tdSql.checkRows(1)
        val = tdSql.queryResult[0][0]
        if val is None:
            raise Exception("greatest returned NULL on masked col")
        tdSql.query(f"select least({col}, 'z') from {table} limit 1")
        tdSql.checkRows(1)
        val = tdSql.queryResult[0][0]
        if val is None:
            raise Exception("least returned NULL on masked col")

    def _check_mask_agg_funcs(self, table, col="c1"):
        """Verify aggregate functions that reveal content return '*'."""
        for func in ["first", "last", "last_row", "mode"]:
            tdSql.query(f"select {func}({col}) from {table}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, '*')
        # min/max on string: operates on masked value '*'
        for func in ["min", "max"]:
            tdSql.query(f"select {func}({col}) from {table}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, '*')

    def _check_mask_agg_numeric_funcs(self, table, col="c1"):
        """Verify aggregate functions returning numeric results on masked column."""
        # count: should return real row count (no content leak)
        tdSql.query(f"select count({col}) from {table}")
        tdSql.checkRows(1)
        if tdSql.queryResult[0][0] < 1:
            raise Exception("count should return real count")
        # hyperloglog: cardinality count on masked col — should be 1 (all '*')
        tdSql.query(f"select hyperloglog({col}) from {table}")
        tdSql.checkRows(1)
        if tdSql.queryResult[0][0] is None:
            raise Exception("hyperloglog returned NULL")

    def _check_mask_multi_row_funcs(self, table, col="c1"):
        """Verify multi-row selection functions on masked column."""
        # sample(col, 1): returns 1 random row — should be masked
        tdSql.query(f"select sample({col}, 1) from {table}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*')
        # tail(col, 1): last 1 row — should be masked
        tdSql.query(f"select tail({col}, 1) from {table}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*')
        # unique(col): distinct values — all masked to '*'
        tdSql.query(f"select unique({col}) from {table}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*')

    def _check_mask_metadata_funcs(self, table, col="c1"):
        """Verify metadata functions: length/char_length return natural
        results for masked columns. length('*') = 1, count unaffected."""
        for func in ["length", "char_length"]:
            tdSql.query(f"select {func}({col}) from {table} limit 1")
            tdSql.checkData(0, 0, 1)
        # count doesn't reveal content — should return real row count
        tdSql.query(f"select count({col}) from {table}")
        tdSql.checkRows(1)
        if tdSql.queryResult[0][0] < 1:
            raise Exception("count should return real row count")
        # crc32 on masked col: deterministic, just check non-null
        tdSql.query(f"select crc32({col}) from {table} limit 1")
        if tdSql.queryResult[0][0] is None:
            raise Exception("crc32 returned NULL on masked col")

    def _mask_setup(self):
        """Create d_mask environment: database, tables, data, user, grants."""
        tdSql.connect("root", "taosdata")
        tdSql.execute("drop database if exists d_mask")
        try:
            tdSql.execute("drop user u_mask", queryTimes=1)
        except Exception:
            pass

        tdSql.execute("create database d_mask keep 36500")
        tdSql.execute("use d_mask")

        # Supertable with VARCHAR/NCHAR maskable columns
        tdSql.execute(
            "create table d_mask.stb_mask "
            "(ts timestamp, c0 int, c1 varchar(20), c2 nchar(20)) "
            "tags(t0 int, t1 varchar(20), t2 nchar(20))"
        )
        tdSql.execute(
            "create table d_mask.ctb_mask using d_mask.stb_mask "
            "tags(0, 'tag0', 'tag0')"
        )
        # Normal table with VARCHAR/NCHAR maskable columns
        tdSql.execute(
            "create table d_mask.ntb_mask "
            "(ts timestamp, c0 int, c1 varchar(20), c2 nchar(20))"
        )
        tdSql.execute(
            "create table d_mask.ntb_mask_unsupported "
            "(ts timestamp, c0 int, c1 varbinary(20), c2 geometry(100))"
        )
        tdSql.execute(
            "create table d_mask.stb_json "
            "(ts timestamp, c0 int, c1 varchar(20)) "
            "tags(jtag json)"
        )

        tdSql.execute("insert into d_mask.ctb_mask values(now, 1, 'hello', 'world')")
        tdSql.execute("insert into d_mask.ctb_mask values(now+1s, 3, 'secret2', 'hidden2')")
        tdSql.execute("insert into d_mask.ntb_mask values(now, 2, 'foo', 'bar')")
        tdSql.execute("insert into d_mask.ntb_mask values(now+1s, 4, 'secret2', 'hidden2')")

        tdSql.execute(f"create user u_mask pass '{self.test_pass}'")
        tdSql.execute("grant use on database d_mask to u_mask")

        # mask(col) must reject unsupported data types
        tdSql.error(
            "grant select(ts, c0, mask(c1)) "
            "on table d_mask.ntb_mask_unsupported to u_mask",
            expectErrInfo="Not support mask for data type",
            fullMatched=False,
        )
        tdSql.error(
            "grant select(ts, c0, mask(c2)) "
            "on table d_mask.ntb_mask_unsupported to u_mask",
            expectErrInfo="Not support mask for data type",
            fullMatched=False,
        )
        tdSql.error(
            "grant select(ts, c0, mask(jtag)) "
            "on table d_mask.stb_json to u_mask",
            expectErrInfo="Not support mask for data type",
            fullMatched=False,
        )

        # Grant select with mask on VARCHAR/NCHAR columns for the supertable
        tdSql.execute(
            "grant select(ts, c0, mask(c1), mask(c2), "
            "t0, mask(t1), mask(t2)) "
            "on table d_mask.stb_mask to u_mask"
        )
        # Grant select with mask on VARCHAR/NCHAR columns for the normal table
        tdSql.execute(
            "grant select(ts, c0, mask(c1), mask(c2)) "
            "on table d_mask.ntb_mask to u_mask"
        )

        # Switch to the restricted user and query
        tdSql.connect("u_mask", self.test_pass)
        # Wait for privileges to take effect
        max_wait_seconds = 5
        poll_interval_seconds = 0.5
        start_time = time.time()
        last_exception = None
        while True:
            try:
                tdSql.query("select ts,c0,c1,c2 from d_mask.ntb_mask limit 1")
                break
            except Exception as e:
                last_exception = e
                if time.time() - start_time >= max_wait_seconds:
                    raise last_exception
                time.sleep(poll_interval_seconds)


    def _mask_teardown(self):
        """Clean up d_mask environment."""
        tdSql.connect("root", "taosdata")
        tdSql.execute("drop database if exists d_mask")
        try:
            tdSql.execute("drop user u_mask", queryTimes=1)
        except Exception:
            pass

    def _check_mask_basic_and_denial(self):
        """Basic masking output for ntb/stb/ctb + column permission denial."""
        # ==== Normal table ====
        tdSql.query("select * from d_mask.ntb_mask")
        tdSql.checkRows(2)
        tdSql.checkData(0, 2, '*')  # c1 masked
        tdSql.checkData(0, 3, '*')  # c2 masked
        tdSql.checkData(1, 2, '*')  # c1 masked row 2
        tdSql.checkData(1, 3, '*')  # c2 masked row 2

        tdSql.query("select c0, c1, c2 from d_mask.ntb_mask")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')

        # Add a real column that is not part of the existing column-level GRANT,
        # then verify the failure is permission-related rather than "unknown column".
        tdSql.connect("root", "taosdata")
        tdSql.execute("alter table d_mask.ntb_mask add column c3 varchar(32)")
        tdSql.connect("u_mask", self.test_pass)
        tdSql.error(
            "select c3 from d_mask.ntb_mask",
            expectErrInfo="Permission denied for column: c3",
            fullMatched=False,
        )

        # Non-granted column used in ORDER BY must also be denied —
        # ORDER BY can leak data distribution via observable sort order.
        tdSql.error(
            "select c0 from d_mask.ntb_mask order by c3",
            expectErrInfo="Permission denied for column: c3",
            fullMatched=False,
        )
        # Non-granted column in ORDER BY expression
        tdSql.error(
            "select c0 from d_mask.ntb_mask order by length(c3)",
            expectErrInfo="Permission denied for column: c3",
            fullMatched=False,
        )
        # Non-granted column in GROUP BY must also be denied
        tdSql.error(
            "select count(*) from d_mask.ntb_mask group by c3",
            expectErrInfo="Permission denied for column: c3",
            fullMatched=False,
        )
        # Non-granted column in HAVING must also be denied
        tdSql.error(
            "select c0, count(*) from d_mask.ntb_mask group by c0 having max(c3) is not null",
            expectErrInfo="Permission denied for column: c3",
            fullMatched=False,
        )
        # Non-granted column in WHERE (should already be denied, sanity check)
        tdSql.error(
            "select c0 from d_mask.ntb_mask where c3 = 'x'",
            expectErrInfo="Permission denied for column: c3",
            fullMatched=False,
        )
        # Granted columns in ORDER BY should still work fine
        tdSql.query("select c0 from d_mask.ntb_mask order by c1")
        tdSql.checkRows(2)
        tdSql.query("select c0 from d_mask.ntb_mask order by c0")
        tdSql.checkRows(2)

        # concat_ws with two masked cols: concat_ws('-', '*', '*') = '*-*'
        tdSql.query("select concat_ws('-', c1, c2) from d_mask.ntb_mask")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*-*')

        # non-masked col alongside masked expression
        tdSql.query("select c0, upper(c1) from d_mask.ntb_mask")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, '*')

        # alias should not bypass mask
        tdSql.query("select c1 as name from d_mask.ntb_mask")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')

        # Inference-prone expressions: case naturally evaluates on '*'
        tdSql.query("select case when c1 = 'foo' then 1 else 0 end from d_mask.ntb_mask")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 0)
        # GROUP BY operates on original values (projection-only masking, like Oracle/OpenGauss)
        # 2 distinct original values ('foo','secret2') → 2 groups, both displayed as '*'
        tdSql.query("select c1 from d_mask.ntb_mask group by c1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(1, 0, '*')


    def _check_mask_function_coverage(self):
        """Comprehensive function coverage: scalar, hash, encoding, agg, etc."""
        # -- Comprehensive function coverage on normal table --
        self._check_mask_scalar_funcs("d_mask.ntb_mask", "c1")
        self._check_mask_scalar_funcs("d_mask.ntb_mask", "c2")
        self._check_mask_substr_funcs("d_mask.ntb_mask", "c1")
        self._check_mask_ascii_char_funcs("d_mask.ntb_mask", "c1")
        self._check_mask_hash_funcs("d_mask.ntb_mask", "c1")
        self._check_mask_encoding_funcs("d_mask.ntb_mask", "c1")
        self._check_mask_conversion_funcs("d_mask.ntb_mask", "c1")
        self._check_mask_comparison_funcs("d_mask.ntb_mask", "c1")
        self._check_mask_agg_funcs("d_mask.ntb_mask", "c1")
        self._check_mask_agg_numeric_funcs("d_mask.ntb_mask", "c1")
        self._check_mask_multi_row_funcs("d_mask.ntb_mask", "c1")
        self._check_mask_metadata_funcs("d_mask.ntb_mask", "c1")

        # ==== Supertable ====
        tdSql.query("select * from d_mask.stb_mask")
        tdSql.checkRows(2)
        tdSql.checkData(0, 2, '*')  # c1 masked
        tdSql.checkData(0, 3, '*')  # c2 masked
        tdSql.checkData(0, 4, 0)    # t0 visible
        tdSql.checkData(0, 5, '*')  # t1 masked
        tdSql.checkData(0, 6, '*')  # t2 masked

        tdSql.query("select c0, c1, c2 from d_mask.stb_mask")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')

        tdSql.query("select t1, t2 from d_mask.stb_mask")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(0, 1, '*')

        # -- Comprehensive function coverage on supertable --
        self._check_mask_scalar_funcs("d_mask.stb_mask", "c1")
        self._check_mask_substr_funcs("d_mask.stb_mask", "c1")
        self._check_mask_ascii_char_funcs("d_mask.stb_mask", "c1")
        self._check_mask_hash_funcs("d_mask.stb_mask", "c1")
        self._check_mask_encoding_funcs("d_mask.stb_mask", "c1")
        self._check_mask_conversion_funcs("d_mask.stb_mask", "c1")
        self._check_mask_comparison_funcs("d_mask.stb_mask", "c1")
        self._check_mask_agg_funcs("d_mask.stb_mask", "c1")
        self._check_mask_agg_numeric_funcs("d_mask.stb_mask", "c1")
        self._check_mask_multi_row_funcs("d_mask.stb_mask", "c1")
        self._check_mask_metadata_funcs("d_mask.stb_mask", "c1")

        # Scalar/metadata functions on masked tags
        self._check_mask_scalar_funcs("d_mask.stb_mask", "t1")
        self._check_mask_hash_funcs("d_mask.stb_mask", "t1")
        self._check_mask_encoding_funcs("d_mask.stb_mask", "t1")
        self._check_mask_metadata_funcs("d_mask.stb_mask", "t1")

        # Mixed column + tag functions
        tdSql.query("select upper(c1), lower(t1) from d_mask.stb_mask")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(0, 1, '*')

        # ==== Child table ====
        tdSql.query("select * from d_mask.ctb_mask")
        tdSql.checkRows(2)
        tdSql.checkData(0, 2, '*')  # c1 masked
        tdSql.checkData(0, 3, '*')  # c2 masked

        tdSql.query("select c0, c1, c2, t1, t2 from d_mask.ctb_mask")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')
        tdSql.checkData(0, 3, '*')
        tdSql.checkData(0, 4, '*')

        # -- Comprehensive function coverage on child table --
        self._check_mask_scalar_funcs("d_mask.ctb_mask", "c1")
        self._check_mask_substr_funcs("d_mask.ctb_mask", "c1")
        self._check_mask_ascii_char_funcs("d_mask.ctb_mask", "c1")
        self._check_mask_hash_funcs("d_mask.ctb_mask", "c1")
        self._check_mask_encoding_funcs("d_mask.ctb_mask", "c1")
        self._check_mask_conversion_funcs("d_mask.ctb_mask", "c1")
        self._check_mask_comparison_funcs("d_mask.ctb_mask", "c1")
        self._check_mask_agg_funcs("d_mask.ctb_mask", "c1")
        self._check_mask_agg_numeric_funcs("d_mask.ctb_mask", "c1")
        self._check_mask_multi_row_funcs("d_mask.ctb_mask", "c1")
        self._check_mask_metadata_funcs("d_mask.ctb_mask", "c1")

        # ==== group_concat on masked column ====
        tdSql.query("select group_concat(c1, ',') from d_mask.ntb_mask")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*,*')

        # ==== mask_full/mask_none on already-masked column ====
        # mask_full requires 2 args; inner c1 is already masked to '*'
        tdSql.query("select mask_full(c1, 'X') from d_mask.ntb_mask")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'X')
        tdSql.query("select mask_none(c1) from d_mask.ntb_mask")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')

        # ==== WHERE clause operates on original values (projection-only masking) ====
        tdSql.query("select c0 from d_mask.ntb_mask where c1 = 'foo'")
        tdSql.checkRows(1)  # WHERE sees original 'foo'
        tdSql.query("select c0 from d_mask.ntb_mask where c1 = '*'")
        tdSql.checkRows(0)  # no original '*' value exists

        # ==== DISTINCT on masked column ====
        # DISTINCT operates on projected (masked) values → all '*' → 1 row
        tdSql.query("select distinct c1 from d_mask.ntb_mask")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*')

        # ==== HAVING on masked column ====
        # GROUP BY on original values → 2 groups; both satisfy HAVING
        tdSql.query("select c1, count(*) from d_mask.ntb_mask group by c1 having count(*) > 0")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(1, 0, '*')

        # ==== UNION on masked column ====
        tdSql.query("select c1 from d_mask.ntb_mask union all select c1 from d_mask.ctb_mask")
        tdSql.checkRows(4)
        for i in range(4):
            tdSql.checkData(i, 0, '*')


    def _check_mask_projidx_and_subqueries(self):
        """ProjIdx preservation, ORDER BY on masked columns, subqueries."""
        # ==== Projection index (projIdx) preservation tests ====
        # When mask rewrites a column to mask_full(), the new SFunctionNode
        # must carry forward projIdx from the original SColumnNode.  The
        # planner uses projIdx to map projection output slots correctly.

        # Multi-column: verify slot ordering c0(clear), c1(masked), c2(masked)
        tdSql.query("select c0, c1, c2 from d_mask.ntb_mask")
        tdSql.checkRows(2)
        tdSql.checkCols(3)
        for i in range(2):
            if tdSql.queryResult[i][0] is None:
                raise Exception("c0 should not be None (not masked)")
            tdSql.checkData(i, 1, '*')                    # c1 masked at slot 1
            tdSql.checkData(i, 2, '*')                    # c2 masked at slot 2

        # Reversed column order: projIdx must not be positional
        tdSql.query("select c2, c1, c0 from d_mask.ntb_mask")
        tdSql.checkRows(2)
        tdSql.checkCols(3)
        for i in range(2):
            tdSql.checkData(i, 0, '*')                    # c2 masked at slot 0
            tdSql.checkData(i, 1, '*')                    # c1 masked at slot 1
            if tdSql.queryResult[i][2] is None:
                raise Exception("c0 clear at slot 2 should not be None")

        # Interleaved masked/non-masked: slots must map correctly
        tdSql.query("select c0, c1, t0, c2 from d_mask.stb_mask")
        tdSql.checkRows(2)
        tdSql.checkCols(4)
        for i in range(2):
            if tdSql.queryResult[i][0] is None:
                raise Exception("c0 clear should not be None")
            tdSql.checkData(i, 1, '*')                    # c1 masked
            if tdSql.queryResult[i][2] is None:
                raise Exception("t0 clear should not be None")
            tdSql.checkData(i, 3, '*')                    # c2 masked

        # ORDER BY with masked columns
        tdSql.query("select c0, c1 from d_mask.ntb_mask order by c0")
        tdSql.checkRows(2)
        tdSql.checkCols(2)
        if tdSql.queryResult[0][0] is None:
            raise Exception("c0 should not be None")
        tdSql.checkData(0, 1, '*')

        # ORDER BY ts with all columns
        tdSql.query("select c0, c1, c2 from d_mask.ntb_mask order by ts")
        tdSql.checkRows(2)
        tdSql.checkCols(3)
        if tdSql.queryResult[0][0] is None:
            raise Exception("c0 should not be None")
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')

        # ORDER BY desc with limit
        tdSql.query("select c0, c1, c2 from d_mask.ntb_mask order by ts desc limit 1")
        tdSql.checkRows(1)
        tdSql.checkCols(3)

        # ==== ORDER BY on masked column directly ====
        # ORDER BY masked col should sort by original values, output masked
        tdSql.query("select c1 from d_mask.ntb_mask order by c1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(1, 0, '*')

        tdSql.query("select c2 from d_mask.ntb_mask order by c2")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(1, 0, '*')

        # ORDER BY masked col with other columns
        tdSql.query("select c0, c1, c2 from d_mask.ntb_mask order by c1")
        tdSql.checkRows(2)
        tdSql.checkCols(3)
        if tdSql.queryResult[0][0] is None:
            raise Exception("c0 should not be None")
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')

        # ORDER BY masked col desc
        tdSql.query("select c0, c1 from d_mask.ntb_mask order by c1 desc")
        tdSql.checkRows(2)
        tdSql.checkCols(2)
        if tdSql.queryResult[0][0] is None:
            raise Exception("c0 should not be None")
        tdSql.checkData(0, 1, '*')

        # ORDER BY masked col on supertable
        tdSql.query("select c1 from d_mask.stb_mask order by c1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(1, 0, '*')

        tdSql.query("select c0, c1, c2 from d_mask.stb_mask order by c2")
        tdSql.checkRows(2)
        tdSql.checkCols(3)
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')

        # ORDER BY masked col on child table
        tdSql.query("select c1 from d_mask.ctb_mask order by c1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')

        # Subquery with ORDER BY on masked column
        tdSql.query("select * from (select c2 from d_mask.ntb_mask order by c2)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(1, 0, '*')

        tdSql.query("select * from (select c1 from d_mask.stb_mask order by c1)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')

        # Nested subquery: outer SELECT * references inner projIdx
        tdSql.query("select * from (select c0, c1, c2 from d_mask.ntb_mask)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, '*')                        # c1 masked
        tdSql.checkData(0, 2, '*')                        # c2 masked

        # Nested subquery: explicit outer columns
        tdSql.query("select c1, c2 from (select ts, c0, c1, c2 from d_mask.ntb_mask)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(0, 1, '*')

        # Nested subquery with alias on masked column
        tdSql.query("select a from (select c1 as a from d_mask.ntb_mask)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')

        # Two-level nesting: mixed masked / non-masked columns
        tdSql.query(
            "select v1, v2 from "
            "(select c0 as v1, c1 as v2 from d_mask.ntb_mask)"
        )
        tdSql.checkRows(2)
        if tdSql.queryResult[0][0] is None:
            raise Exception("v1 = c0, clear should not be None")
        tdSql.checkData(0, 1, '*')                        # v2 = c1, masked

        # Masked column expression inside subquery
        tdSql.query(
            "select expr from "
            "(select concat(c1, c2) as expr from d_mask.ntb_mask)"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '**')                       # concat('*','*')

        # UNION ALL: projIdx mapping across set operations
        tdSql.query(
            "select c0, c1 from d_mask.ntb_mask "
            "union all "
            "select c0, c1 from d_mask.ntb_mask"
        )
        tdSql.checkRows(4)
        for i in range(4):
            if tdSql.queryResult[i][0] is None:
                raise Exception("c0 should not be None")
            tdSql.checkData(i, 1, '*')

        # Aggregates with masked columns
        tdSql.query("select count(*), first(c1), last(c2) from d_mask.ntb_mask")
        tdSql.checkRows(1)
        tdSql.checkCols(3)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')

        # Expression mixed with masked column
        tdSql.query("select c0 + 1 as v, c1 from d_mask.ntb_mask")
        tdSql.checkRows(2)
        tdSql.checkCols(2)
        if tdSql.queryResult[0][0] is None:
            raise Exception("result should not be None")
        tdSql.checkData(0, 1, '*')

        # ================================================================

    def _check_mask_advanced_clauses(self):
        """GROUP BY, HAVING, PARTITION BY, INTERVAL, JOIN, complex combos."""
        # ==== Advanced clause coverage: GROUP BY / HAVING / PARTITION
        # ====   BY / ORDER BY combos / JOIN / window / complex SQL
        # ================================================================
        # Primary goal: verify these queries do NOT produce unexpected
        # errors (like "invalid input") and that masked columns in
        # projection output contain '*'.

        # ---- GROUP BY masked column ----
        # GROUP BY uses original values; projection masked
        tdSql.query("select c1, count(*) from d_mask.ntb_mask group by c1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(1, 0, '*')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)

        # GROUP BY masked column with multiple aggregates
        tdSql.query("select c1, count(*), first(c0) from d_mask.ntb_mask group by c1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(1, 0, '*')

        # GROUP BY masked NCHAR column
        tdSql.query("select c2, count(*) from d_mask.ntb_mask group by c2")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(1, 0, '*')

        # GROUP BY two masked columns
        tdSql.query("select c1, c2, count(*) from d_mask.ntb_mask group by c1, c2")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(0, 1, '*')

        # GROUP BY non-masked column, select masked column with aggregate
        tdSql.query("select c0, first(c1) from d_mask.ntb_mask group by c0")
        tdSql.checkRows(2)
        for i in range(2):
            tdSql.checkData(i, 1, '*')

        # GROUP BY masked column + ORDER BY count
        tdSql.query(
            "select c1, count(*) as cnt from d_mask.ntb_mask "
            "group by c1 order by cnt desc"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(1, 0, '*')

        # GROUP BY masked column + ORDER BY masked column
        tdSql.query(
            "select c1, count(*) from d_mask.ntb_mask "
            "group by c1 order by c1"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(1, 0, '*')

        # ---- HAVING with masked column ----
        # HAVING count filter — both groups have count=1
        tdSql.query(
            "select c1, count(*) from d_mask.ntb_mask "
            "group by c1 having count(*) >= 1"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(1, 0, '*')

        # HAVING filters out all groups
        tdSql.query(
            "select c1, count(*) from d_mask.ntb_mask "
            "group by c1 having count(*) > 100"
        )
        tdSql.checkRows(0)

        # GROUP BY + HAVING + ORDER BY combo on masked column
        tdSql.query(
            "select c1, count(*) as cnt from d_mask.ntb_mask "
            "group by c1 having count(*) >= 1 order by c1"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(1, 0, '*')

        # GROUP BY + HAVING + ORDER BY desc
        tdSql.query(
            "select c2, count(*) as cnt from d_mask.ntb_mask "
            "group by c2 having count(*) >= 1 order by c2 desc"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(1, 0, '*')

        # ---- PARTITION BY masked column (supertable) ----
        # PARTITION BY operates on original values; projection masked
        tdSql.query(
            "select c1, count(*) from d_mask.stb_mask "
            "partition by c1"
        )
        rows = tdSql.queryRows
        if rows < 1:
            raise Exception("partition by masked col should return at least 1 row")
        for i in range(rows):
            tdSql.checkData(i, 0, '*')

        # PARTITION BY masked column with first/last
        tdSql.query(
            "select first(c1), last(c2) from d_mask.stb_mask "
            "partition by c1"
        )
        rows = tdSql.queryRows
        if rows < 1:
            raise Exception("partition by masked col should return at least 1 row")
        for i in range(rows):
            tdSql.checkData(i, 0, '*')
            tdSql.checkData(i, 1, '*')

        # PARTITION BY masked tag
        tdSql.query(
            "select t1, count(*) from d_mask.stb_mask "
            "partition by t1"
        )
        rows = tdSql.queryRows
        if rows < 1:
            raise Exception("partition by masked tag should return at least 1 row")
        tdSql.checkData(0, 0, '*')

        # PARTITION BY non-masked + select masked
        tdSql.query(
            "select t0, first(c1) from d_mask.stb_mask "
            "partition by t0"
        )
        rows = tdSql.queryRows
        if rows < 1:
            raise Exception("partition by non-masked col should return at least 1 row")
        if tdSql.queryResult[0][0] is None:
            raise Exception("t0 not masked should not be None")
        tdSql.checkData(0, 1, '*')

        # PARTITION BY + ORDER BY masked column
        tdSql.query(
            "select c1, count(*) as cnt from d_mask.stb_mask "
            "partition by c1 order by c1"
        )
        rows = tdSql.queryRows
        if rows < 1:
            raise Exception("partition by + order by should return at least 1 row")
        for i in range(rows):
            tdSql.checkData(i, 0, '*')

        # ---- INTERVAL window with masked column ----
        tdSql.query(
            "select _wstart, first(c1), last(c2) from d_mask.ntb_mask "
            "interval(1h)"
        )
        rows = tdSql.queryRows
        if rows < 1:
            raise Exception("interval should return at least 1 row")
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')

        # INTERVAL + GROUP BY on supertable (partition by tbname)
        tdSql.query(
            "select _wstart, first(c1) from d_mask.stb_mask "
            "partition by tbname interval(1h)"
        )
        rows = tdSql.queryRows
        if rows < 1:
            raise Exception("interval + partition by should return at least 1 row")
        tdSql.checkData(0, 1, '*')

        # INTERVAL + masked column in partition by
        tdSql.query(
            "select _wstart, count(*) from d_mask.ntb_mask "
            "partition by c1 interval(1d)"
        )
        rows = tdSql.queryRows
        if rows < 1:
            raise Exception("interval + partition by masked col should return at least 1 row")

        # ---- ORDER BY multiple columns including masked ----
        tdSql.query("select c0, c1, c2 from d_mask.ntb_mask order by c1, c0")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')

        tdSql.query("select c0, c1, c2 from d_mask.ntb_mask order by c0, c1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')

        # ORDER BY masked + non-masked mixed, desc/asc
        tdSql.query("select c0, c1 from d_mask.ntb_mask order by c1 asc, c0 desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, '*')

        # ORDER BY masked column with LIMIT/OFFSET
        tdSql.query("select c0, c1 from d_mask.ntb_mask order by c1 limit 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, '*')

        tdSql.query("select c0, c1 from d_mask.ntb_mask order by c1 limit 1 offset 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, '*')

        # ---- WHERE + ORDER BY + masked column combos ----
        tdSql.query(
            "select c0, c1 from d_mask.ntb_mask "
            "where c1 = 'foo' order by c1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, '*')

        tdSql.query(
            "select c0, c1, c2 from d_mask.ntb_mask "
            "where c0 > 0 order by c1 desc"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')

        # ---- Subquery + ORDER BY masked column ----
        tdSql.query(
            "select * from ("
            "  select c0, c1, c2 from d_mask.ntb_mask order by c1"
            ")"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')

        # Subquery GROUP BY masked column, outer ORDER BY
        tdSql.query(
            "select * from ("
            "  select c1, count(*) as cnt from d_mask.ntb_mask group by c1"
            ") order by cnt"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')

        # Nested subquery: inner ORDER BY masked, outer aggregation
        tdSql.query(
            "select count(*) from ("
            "  select c1 from d_mask.ntb_mask order by c1"
            ")"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        # ---- UNION with ORDER BY masked column ----
        tdSql.query(
            "select c1 from d_mask.ntb_mask "
            "union all "
            "select c1 from d_mask.ctb_mask "
            "order by c1"
        )
        tdSql.checkRows(4)
        for i in range(4):
            tdSql.checkData(i, 0, '*')

        # ---- DISTINCT + ORDER BY masked column ----
        tdSql.query("select distinct c1 from d_mask.ntb_mask order by c1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*')

        # ---- JOIN tests ----
        # Setup: create a second normal table with mask grant for JOIN testing.
        # Use explicit timestamp literals shared by both tables so the equi-join
        # on ts is guaranteed to produce rows regardless of wall-clock timing.
        JOIN_TS1 = "'2021-07-01 00:00:00.000'"
        JOIN_TS2 = "'2021-07-01 00:00:01.000'"
        tdSql.connect("root", "taosdata")
        # Add fixed-ts rows to ntb_mask that ntb_mask2 will join against.
        # Use explicit column list because c3 was added in the denial test above.
        tdSql.execute(f"insert into d_mask.ntb_mask(ts, c0, c1, c2) values({JOIN_TS1}, 10, 'jfoo', 'jbar')")
        tdSql.execute(f"insert into d_mask.ntb_mask(ts, c0, c1, c2) values({JOIN_TS2}, 20, 'jsecret', 'jhidden')")
        tdSql.execute(
            "create table d_mask.ntb_mask2 "
            "(ts timestamp, c0 int, c1 varchar(20), c2 nchar(20))"
        )
        tdSql.execute(f"insert into d_mask.ntb_mask2 values({JOIN_TS1}, 30, 'join1', 'jn1')")
        tdSql.execute(f"insert into d_mask.ntb_mask2 values({JOIN_TS2}, 40, 'join2', 'jn2')")
        tdSql.execute(
            "grant select(ts, c0, mask(c1), mask(c2)) "
            "on table d_mask.ntb_mask2 to u_mask"
        )

        # Create a second child table for supertable queries
        tdSql.execute(
            "create table d_mask.ctb_mask2 using d_mask.stb_mask "
            "tags(1, 'tag1', 'tag1')"
        )
        tdSql.execute("insert into d_mask.ctb_mask2 values(now, 5, 'ct2hello', 'ct2world')")
        tdSql.execute("insert into d_mask.ctb_mask2 values(now+1s, 7, 'ct2sec', 'ct2hid')")

        tdSql.connect("u_mask", self.test_pass)
        time.sleep(5)

        # Basic JOIN: both tables have masked columns — 2 rows guaranteed by fixed timestamps
        tdSql.query(
            "select a.c1, b.c1 from d_mask.ntb_mask a "
            "join d_mask.ntb_mask2 b on a.ts = b.ts"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(0, 1, '*')

        # JOIN with non-masked + masked columns
        tdSql.query(
            "select a.c0, a.c1, b.c0, b.c1 from d_mask.ntb_mask a "
            "join d_mask.ntb_mask2 b on a.ts = b.ts"
        )
        tdSql.checkRows(2)
        if tdSql.queryResult[0][0] is None:
            raise Exception("a.c0 clear should not be None")
        tdSql.checkData(0, 1, '*')                   # a.c1 masked
        if tdSql.queryResult[0][2] is None:
            raise Exception("b.c0 clear should not be None")
        tdSql.checkData(0, 3, '*')                   # b.c1 masked

        # JOIN + ORDER BY masked column
        tdSql.query(
            "select a.c0, a.c1 from d_mask.ntb_mask a "
            "join d_mask.ntb_mask2 b on a.ts = b.ts "
            "order by a.c1"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, '*')

        # JOIN + WHERE on masked column (matches one of the two fixed-ts rows)
        tdSql.query(
            "select a.c0, a.c1 from d_mask.ntb_mask a "
            "join d_mask.ntb_mask2 b on a.ts = b.ts "
            "where a.c1 = 'jfoo'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, '*')

        # Supertable: multiple child tables, query stb
        tdSql.query("select * from d_mask.stb_mask")
        tdSql.checkRows(4)  # 2 from ctb_mask + 2 from ctb_mask2
        for i in range(4):
            tdSql.checkData(i, 2, '*')  # c1 masked
            tdSql.checkData(i, 3, '*')  # c2 masked

        # Supertable ORDER BY masked col
        tdSql.query("select c0, c1 from d_mask.stb_mask order by c1")
        tdSql.checkRows(4)
        for i in range(4):
            tdSql.checkData(i, 1, '*')

        # Supertable GROUP BY masked col
        tdSql.query("select c1, count(*) from d_mask.stb_mask group by c1")
        rows = tdSql.queryRows
        if rows < 1:
            raise Exception("group by masked col should return at least 1 row")
        for i in range(rows):
            tdSql.checkData(i, 0, '*')

        # Supertable PARTITION BY masked tag + ORDER BY
        tdSql.query(
            "select t1, count(*) from d_mask.stb_mask "
            "partition by t1 order by t1"
        )
        rows = tdSql.queryRows
        if rows < 1:
            raise Exception("partition by masked tag should return at least 1 row")
        for i in range(rows):
            tdSql.checkData(i, 0, '*')

        # Supertable GROUP BY masked col + HAVING + ORDER BY
        tdSql.query(
            "select c1, count(*) as cnt from d_mask.stb_mask "
            "group by c1 having count(*) >= 1 order by c1"
        )
        rows = tdSql.queryRows
        if rows < 1:
            raise Exception("group by + having should return at least 1 row")
        for i in range(rows):
            tdSql.checkData(i, 0, '*')

        # ---- Complex combo: GROUP BY + HAVING + ORDER BY + LIMIT ----
        tdSql.query(
            "select c1, count(*) as cnt from d_mask.ntb_mask "
            "group by c1 having count(*) >= 1 "
            "order by c1 limit 1"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*')

        # ---- Subquery + JOIN ----
        tdSql.query(
            "select a.c1 from "
            "(select ts, c0, c1 from d_mask.ntb_mask) a "
            "join "
            "(select ts, c0, c1 from d_mask.ntb_mask2) b "
            "on a.ts = b.ts"
        )
        rows = tdSql.queryRows
        if rows > 0:
            tdSql.checkData(0, 0, '*')

        # ---- CASE WHEN + GROUP BY masked column ----
        # ntb_mask now has 4 rows (original 2 + 2 JOIN inserts above),
        # giving 4 distinct c1 groups: 'foo', 'secret2', 'jfoo', 'jsecret'.
        tdSql.query(
            "select case when c1 = 'foo' then 'match' else 'no' end as flag, "
            "count(*) from d_mask.ntb_mask group by c1"
        )
        tdSql.checkRows(4)
        # CASE evaluates on masked '*' in projection → all 'no'
        for i in range(4):
            tdSql.checkData(i, 0, 'no')

        # ---- Expressions in ORDER BY involving masked column ----
        tdSql.query(
            "select c0, c1 from d_mask.ntb_mask order by length(c1)"
        )
        tdSql.checkRows(4)  # ntb_mask has 4 rows after JOIN inserts
        tdSql.checkData(0, 1, '*')

        tdSql.query(
            "select c0, c1 from d_mask.ntb_mask order by upper(c1)"
        )
        tdSql.checkRows(4)  # ntb_mask has 4 rows after JOIN inserts
        tdSql.checkData(0, 1, '*')

        # ---- SLIMIT/SOFFSET with PARTITION BY masked column ----
        tdSql.query(
            "select c1, count(*) from d_mask.stb_mask "
            "partition by c1 slimit 2"
        )
        rows = tdSql.queryRows
        if rows < 1:
            raise Exception("slimit should return at least 1 row")
        for i in range(rows):
            tdSql.checkData(i, 0, '*')

        # ---- last_row with masked column ----
        tdSql.query("select last_row(c1) from d_mask.ntb_mask")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*')

        tdSql.query("select last_row(c1) from d_mask.stb_mask")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*')

        # ---- SPREAD/ELAPSED with non-masked, result alongside masked ----
        tdSql.query("select spread(c0), first(c1) from d_mask.ntb_mask")
        tdSql.checkRows(1)
        if tdSql.queryResult[0][0] is None:
            raise Exception("spread result should not be None")
        tdSql.checkData(0, 1, '*')

        # ---- Multiple ORDER BY with expressions ----
        tdSql.query(
            "select c0, c1, c2 from d_mask.ntb_mask "
            "order by c1 asc, c2 desc"
        )
        tdSql.checkRows(4)  # ntb_mask has 4 rows after JOIN inserts
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')

        # ---- FILL with masked column in interval (needs time range) ----
        tdSql.query(
            "select _wstart, last(c1) from d_mask.ntb_mask "
            "where ts >= now() - 1d and ts <= now() + 1d "
            "interval(1h) fill(prev)"
        )
        rows = tdSql.queryRows
        if rows < 1:
            raise Exception("fill should return at least 1 row")
        # Check some row has masked c1
        for i in range(rows):
            val = tdSql.queryResult[i][1]
            if val is not None:
                if val != '*':
                    raise Exception(f"fill row {i} c1 should be masked but got {val}")

        # ---- TOP/BOTTOM with non-masked col, select masked ----
        tdSql.query("select top(c0, 1), c1 from d_mask.ntb_mask")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, '*')

        tdSql.query("select bottom(c0, 1), c1 from d_mask.ntb_mask")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, '*')

        # ---- APERCENTILE with non-masked, alongside masked ----
        tdSql.query(
            "select apercentile(c0, 50), first(c1) from d_mask.ntb_mask"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, '*')

        # ---- Three-level nested subquery with masked columns ----
        tdSql.query(
            "select * from ("
            "  select * from ("
            "    select c0, c1, c2 from d_mask.ntb_mask order by c1"
            "  )"
            ")"
        )
        tdSql.checkRows(4)  # ntb_mask has 4 rows after JOIN inserts
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')

        # ---- Subquery with GROUP BY, outer with ORDER BY ----
        tdSql.query(
            "select * from ("
            "  select c1, count(*) as cnt from d_mask.ntb_mask group by c1"
            ") order by cnt desc"
        )
        tdSql.checkRows(4)  # 4 distinct c1 groups after JOIN inserts
        for i in range(4):
            tdSql.checkData(i, 0, '*')

        # ---- Subquery with PARTITION BY masked col ----
        tdSql.query(
            "select * from ("
            "  select c1, count(*) as cnt from d_mask.stb_mask "
            "  partition by c1"
            ")"
        )
        rows = tdSql.queryRows
        if rows < 1:
            raise Exception("partition by subquery should return at least 1 row")
        for i in range(rows):
            tdSql.checkData(i, 0, '*')

        # ---- UNION ALL + ORDER BY ----
        # ntb_mask has 4 rows, ntb_mask2 has 2 rows → 6 total
        tdSql.query(
            "select c0, c1 from d_mask.ntb_mask "
            "union all "
            "select c0, c1 from d_mask.ntb_mask2 "
            "order by c1"
        )
        tdSql.checkRows(6)
        for i in range(6):
            tdSql.checkData(i, 1, '*')

        # ---- Multiple masked cols: GROUP BY one, ORDER BY another ----
        tdSql.query(
            "select c1, first(c2), count(*) from d_mask.ntb_mask "
            "group by c1 order by c1"
        )
        tdSql.checkRows(4)  # 4 distinct c1 groups after JOIN inserts
        tdSql.checkData(0, 0, '*')
        tdSql.checkData(0, 1, '*')

        # ================================================================

    def _check_mask_view_enforcement(self):
        """View privilege and masking enforcement for querying user."""
        # ==== VIEW privilege and masking enforcement ====
        # ================================================================
        # When a user with column-level privileges queries a view, the
        # column restrictions AND masking must still be enforced on the
        # view's output based on the querying user's grants, not the
        # view creator's (root's) privileges.
        #
        # The implementation checks outer-query columns against the
        # querying user's column grants on the underlying physical tables.
        #   - select * from view (includes ungrantable col) → error
        #   - select <ungrantable_col> from view → error
        #   - select <masked_col> from view → shows '*' (masking enforced)
        tdSql.connect("root", "taosdata")

        # ---- View with select * on ntb_mask: includes ungranted c3 ----
        # ntb_mask now has columns ts, c0, c1, c2, c3 (c3 was added by
        # alter-table but NOT granted to u_mask).  A view created by root
        # with "select *" expands to all columns.  When u_mask queries
        # select * from the view, c3 triggers a permission error — just
        # like directly querying a supertable with an ungrantable column.
        tdSql.execute(
            "create view d_mask.v_ntb_star as "
            "select * from d_mask.ntb_mask"
        )
        tdSql.execute(
            "grant select on view d_mask.v_ntb_star to u_mask"
        )
        tdSql.connect("u_mask", self.test_pass)
        time.sleep(5)

        # select * errors because c3 (ungrantable) is in the view's projection
        tdSql.error(
            "select * from d_mask.v_ntb_star",
            expectErrInfo="Permission denied for column",
            fullMatched=False,
        )

        # Explicitly requesting the ungrantable column should also fail
        tdSql.error(
            "select c3 from d_mask.v_ntb_star",
            expectErrInfo="Permission denied for column",
            fullMatched=False,
        )

        # Requesting only granted columns through the star-view works
        tdSql.query("select ts, c0, c1, c2 from d_mask.v_ntb_star")
        tdSql.checkRows(4)
        for i in range(4):
            tdSql.checkData(i, 2, '*')   # c1 masked
            tdSql.checkData(i, 3, '*')   # c2 masked

        # ---- View with explicit granted columns: masking enforced ----
        tdSql.connect("root", "taosdata")
        tdSql.execute(
            "create view d_mask.v_ntb_mask as "
            "select ts, c0, c1, c2 from d_mask.ntb_mask"
        )
        tdSql.execute(
            "grant select on view d_mask.v_ntb_mask to u_mask"
        )
        tdSql.execute(
            "create view d_mask.v_stb_mask as "
            "select ts, c0, c1, c2, t1, t2 from d_mask.stb_mask"
        )
        tdSql.execute(
            "grant select on view d_mask.v_stb_mask to u_mask"
        )

        tdSql.connect("u_mask", self.test_pass)
        time.sleep(5)

        # Query through normal table view — masked columns should show '*'
        tdSql.query("select c0, c1, c2 from d_mask.v_ntb_mask")
        tdSql.checkRows(4)
        for i in range(4):
            if tdSql.queryResult[i][0] is None:
                raise Exception(
                    f"VIEW ntb row {i}: c0 (non-masked) should not be None"
                )
            tdSql.checkData(i, 1, '*')
            tdSql.checkData(i, 2, '*')

        # Query through supertable view — masked columns and tags show '*'
        tdSql.query(
            "select c0, c1, c2, t1, t2 from d_mask.v_stb_mask"
        )
        tdSql.checkRows(4)
        for i in range(4):
            if tdSql.queryResult[i][0] is None:
                raise Exception(
                    f"VIEW stb row {i}: c0 (non-masked) should not be None"
                )
            tdSql.checkData(i, 1, '*')   # c1
            tdSql.checkData(i, 2, '*')   # c2
            tdSql.checkData(i, 3, '*')   # t1
            tdSql.checkData(i, 4, '*')   # t2

        # Root should still see cleartext through the same view
        tdSql.connect("root", "taosdata")
        tdSql.query(
            "select c1, c2 from d_mask.v_ntb_mask order by ts limit 1"
        )
        tdSql.checkRows(1)
        # Root has no mask — should see original values
        if tdSql.queryResult[0][0] == '*':
            raise Exception("Root should see cleartext via view, got '*'")

        # ---- View with select * on stb_mask: all cols granted → masking ----
        # stb_mask has columns ts, c0, c1, c2 and tags t0, t1, t2.  u_mask
        # has grants for ALL of them (mask on c1, c2, t1, t2).  A select *
        # view should succeed but with masking applied.
        tdSql.connect("root", "taosdata")
        tdSql.execute(
            "create view d_mask.v_stb_star as "
            "select * from d_mask.stb_mask"
        )
        tdSql.execute(
            "grant select on view d_mask.v_stb_star to u_mask"
        )
        tdSql.connect("u_mask", self.test_pass)
        time.sleep(5)
        tdSql.query("select * from d_mask.v_stb_star")
        tdSql.checkRows(4)
        # Verify masking: c1 is col index 2, c2 is 3, t1 is 5, t2 is 6
        for i in range(4):
            tdSql.checkData(i, 2, '*')   # c1 masked
            tdSql.checkData(i, 3, '*')   # c2 masked
            tdSql.checkData(i, 5, '*')   # t1 masked
            tdSql.checkData(i, 6, '*')   # t2 masked


    def do_check_column_mask_privileges(self):
        """Test column-level mask privileges for SELECT (data desensitization).

        Runs focused sub-tests for masking behavior:
          - Basic output and column permission denial
          - Comprehensive function coverage
          - ProjIdx preservation and subqueries
          - Advanced SQL clauses (GROUP BY, JOIN, etc.)
          - View privilege enforcement
        """
        self._mask_setup()
        try:
            self._check_mask_basic_and_denial()
            self._check_mask_function_coverage()
            self._check_mask_projidx_and_subqueries()
            self._check_mask_advanced_clauses()
            self._check_mask_view_enforcement()
        finally:
            self._mask_teardown()

    def do_check_reserved_principal_names(self):
        """User/Role names must reject reserved identities, keywords and illegal patterns."""

        tdSql.connect("root", "taosdata")

        invalid_user_names = [
            "`SYSTEM`", "`ROOT`", "`ANONYMOUS`", "`SYS`",
            "`PUBLIC`", "`NONE`", "`NULL`", "`DEFAULT`", "`ALL`", "`ANY`",
            "`INFORMATION_SCHEMA`", "`PERFORMANCE_SCHEMA`", "`INS`",
            "`[u_bad`", "`u bad`"
        ]
        for user_name in invalid_user_names:
            tdSql.error(f"create user {user_name} pass '{self.test_pass}'", expectErrInfo="Invalid user format", fullMatched=False)

        invalid_role_names = [
            "`ROOT`", "`ANONYMOUS`", "`PUBLIC`", "`NONE`", "`NULL`",
            "`DEFAULT`", "`ALL`", "`ANY`", "`INFORMATION_SCHEMA`", "`PERFORMANCE_SCHEMA`", "`INS`",
            "`[r_bad`", "`r bad`"
        ]
        for role_name in invalid_role_names:
            tdSql.error(f"create role {role_name}", expectErrInfo="Invalid role format", fullMatched=False)

    #
    # ------------------- main ----------------
    #
    def test_priv_basic(self):
        """Privileges basic
        
        1. Test basic user privileges(grant/revoke/show user privileges)
        2. Test basic role privileges(grant/revoke/show role privileges)
        3. Test system privileges
        4. Test database privileges
        5. Test table privileges
        6. Test row privileges
        7. Test column privileges
        8. Test grant privileges
        9. Test view privileges
        10. Test audit privileges
        11. Test user privileges
        12. Test role privileges

        13. Test variable privileges
        
        Since: v3.4.0.0

        Labels: basic,ci

        Jira: TS-7232

        History:
            - 2025-12-23 Kaili Xu Initial creation(TS-7232)
        """
        self.do_basic_user_privileges()
        self.do_basic_role_privileges()
        # self.do_check_sys_privileges()
        # self.do_check_db_privileges()
        # self.do_check_table_privileges()
        # self.do_check_row_privileges()
        self.do_check_column_privileges()
        self.do_check_column_mask_privileges()
        # self.do_check_grant_privileges()
        # self.do_check_view_privileges()
        self.do_check_topic_privileges()
        # self.do_check_audit_privileges()
        # self.do_check_user_privileges()
        self.do_check_role_privileges()
        # self.do_check_variable_privileges()
        self.do_check_6841225129()
        self.do_check_legacy_grammar()
        self.do_check_reserved_principal_names()
        
        tdLog.debug("finish executing %s" % __file__)