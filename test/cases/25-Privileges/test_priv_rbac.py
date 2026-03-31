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

    def do_check_column_mask_privileges(self):
        """Test column-level mask privileges for SELECT (data desensitization).

        When a column has `mask(col)` in the SELECT grant, querying that
        column should return '*' instead of the actual value.
        """
        tdSql.connect("root", "taosdata")

        tdSql.execute("drop database if exists d_mask")
        tdSql.execute("create database d_mask")
        tdSql.execute("use d_mask")
        tdSql.execute(
            "create table d_mask.stb_mask "
            "(ts timestamp, c0 int, c1 varchar(20), c2 nchar(20)) "
            "tags(t0 int, t1 varchar(20), t2 nchar(20))"
        )
        tdSql.execute("create table d_mask.ctb_mask using d_mask.stb_mask tags(0,'tag0','tag0')")
        tdSql.execute("create table d_mask.ntb_mask (ts timestamp, c0 int, c1 varchar(20), c2 nchar(20))")

        tdSql.execute("insert into d_mask.ctb_mask values(now, 1, 'hello', 'world')")
        tdSql.execute("insert into d_mask.ntb_mask values(now, 2, 'foo', 'bar')")

        tdSql.execute(f"create user u_mask pass '{self.test_pass}'")
        tdSql.execute("grant use on database d_mask to u_mask")
        # Grant select with mask on varchar/nchar columns for the supertable
        tdSql.execute(
            "grant select(c0, mask(c1), mask(c2), mask(t1), mask(t2)) "
            "on table d_mask.stb_mask to u_mask"
        )
        # Grant select with mask on varchar/nchar columns for the normal table
        tdSql.execute(
            "grant select(c0, mask(c1), mask(c2)) "
            "on table d_mask.ntb_mask to u_mask"
        )

        # Switch to the restricted user and query
        tdSql.connect("u_mask", self.test_pass)
        time.sleep(2)  # wait for privileges to take effect

        # --- Normal table: c1 and c2 should be masked ---
        tdSql.query("select c0, c1, c2 from d_mask.ntb_mask")
        tdSql.checkRows(1)
        # c0 (int) is not masked, should have real value 2
        tdSql.checkData(0, 0, 2)
        # c1 (varchar masked) should be '*'
        tdSql.checkData(0, 1, '*')
        # c2 (nchar masked) should be '*'
        tdSql.checkData(0, 2, '*')

        # --- Supertable: c1 and c2 masked, c0 visible ---
        tdSql.query("select c0, c1, c2 from d_mask.stb_mask")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')

        # --- Permission denied for masked column used without privilege ---
        tdSql.error(
            "select t1 from d_mask.stb_mask",
            expectErrInfo="Permission denied for column",
            fullMatched=False,
        )

        # --- Supertable via child table: tag columns t1 and t2 should be masked ---
        tdSql.query("select c0, c1, c2, t1, t2 from d_mask.ctb_mask")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, '*')
        tdSql.checkData(0, 2, '*')
        tdSql.checkData(0, 3, '*')
        tdSql.checkData(0, 4, '*')

        # Cleanup
        tdSql.connect("root", "taosdata")
        tdSql.execute("drop database if exists d_mask")
        tdSql.execute("drop user u_mask")

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