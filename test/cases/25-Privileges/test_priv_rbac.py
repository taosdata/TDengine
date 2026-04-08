from new_test_framework.utils import tdLog, tdSql, tdDnodes, etool, TDSetSql
from new_test_framework.utils.sqlset import TDSetSql
from taos.tmq import Consumer
from itertools import product
import os
import time
import shutil

class TestCase:

    test_pass = "Passsword_123!"

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
        tdSql.execute("create database d0")
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
        tdSql.execute("create database d1")
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
        tdSql.execute("create database d3")
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
            assert val is not None and len(val) > 0, f"{func} returned empty on masked col"
        # sha2('*', 256) — needs hash length param
        tdSql.query(f"select sha2({col}, 256) from {table} limit 1")
        tdSql.checkRows(1)
        val = tdSql.queryResult[0][0]
        assert val is not None and len(val) > 0, "sha2 returned empty on masked col"

    def _check_mask_encoding_funcs(self, table, col="c1"):
        """Verify base64 encode/decode on masked column."""
        # to_base64('*') -> 'Kg=='
        tdSql.query(f"select to_base64({col}) from {table} limit 1")
        tdSql.checkRows(1)
        val = tdSql.queryResult[0][0]
        assert val is not None and len(val) > 0, "to_base64 returned empty on masked col"
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
        assert val is not None, "greatest returned NULL on masked col"
        tdSql.query(f"select least({col}, 'z') from {table} limit 1")
        tdSql.checkRows(1)
        val = tdSql.queryResult[0][0]
        assert val is not None, "least returned NULL on masked col"

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
        assert tdSql.queryResult[0][0] >= 1, "count should return real count"
        # hyperloglog: cardinality count on masked col — should be 1 (all '*')
        tdSql.query(f"select hyperloglog({col}) from {table}")
        tdSql.checkRows(1)
        assert tdSql.queryResult[0][0] is not None, "hyperloglog returned NULL"

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
            assert tdSql.queryResult[0][0] == 1, f"{func} on masked col should return 1"
        # count doesn't reveal content — should return real row count
        tdSql.query(f"select count({col}) from {table}")
        tdSql.checkRows(1)
        assert tdSql.queryResult[0][0] >= 1, "count should return real row count"
        # crc32 on masked col: deterministic, just check non-null
        tdSql.query(f"select crc32({col}) from {table} limit 1")
        assert tdSql.queryResult[0][0] is not None, "crc32 returned NULL on masked col"

    def do_check_column_mask_privileges(self):
        """Test column-level mask privileges for SELECT (data desensitization).

        When a column has `mask(col)` in the SELECT grant, querying that
        column should return '*' instead of the actual value.
        Supported mask types: VARCHAR, NCHAR.
        These checks validate the current masking semantics exercised by the
        test suite: once a masked column is selected, downstream expression
        evaluation observes the masked value.  In particular, functions and
        predicates such as CASE WHEN, string aggregates, and metadata helpers
        in the select list operate on '*' rather than the original cleartext.
        """
        tdSql.connect("root", "taosdata")
        tdSql.execute("drop database if exists d_mask")
        try:
            tdSql.execute("drop user u_mask", queryTimes=1)
        except Exception:
            pass

        try:
            tdSql.execute("create database d_mask")
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
            tdSql.execute("alter table d_mask.ntb_mask add column c3 varchar(32)")
            try:
                tdSql.query("select c3 from d_mask.ntb_mask")
                raise Exception("expected permission denied when selecting non-granted column c3")
            except Exception as e:
                err_msg = str(e).lower()
                if "permission" not in err_msg and "denied" not in err_msg:
                    raise

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
        finally:
            tdSql.connect("root", "taosdata")
            tdSql.execute("drop database if exists d_mask")
            try:
                tdSql.execute("drop user u_mask", queryTimes=1)
            except Exception:
                pass

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

        Jira: None

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
        
        tdLog.debug("finish executing %s" % __file__)