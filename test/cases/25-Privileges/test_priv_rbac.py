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
        tdSql.connect("root", "taosdata")
        tdSql.execute(f"grant use on database d0 to u_consumer")
        tdSql.execute(f"grant subscribe on topic d0.topic1 to u_consumer")
        tdSql.connect("u_consumer", self.test_pass)
        time.sleep(5)  # wait for privileges to take effect
        self.subscribe_topic("u_consumer", self.test_pass, "g1", "topic1")

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
        # self.do_check_grant_privileges()
        # self.do_check_view_privileges()
        self.do_check_topic_privileges()
        # self.do_check_audit_privileges()
        # self.do_check_user_privileges()
        self.do_check_role_privileges()
        # self.do_check_variable_privileges()
        self.do_check_6841225129()
        
        tdLog.debug("finish executing %s" % __file__)