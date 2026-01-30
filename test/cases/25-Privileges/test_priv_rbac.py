from new_test_framework.utils import tdLog, tdSql, tdDnodes, etool, TDSetSql
from new_test_framework.utils.sqlset import TDSetSql
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

    def do_check_role_privileges(self):
        """Test role privileges"""
        
        tdSql.execute(f"create user ur1 pass '{self.test_pass}'")
        tdSql.execute(f"grant role `SYSDBA` to ur1")
        tdSql.error("grant role `SYSSEC` to ur1", expectErrInfo=f"Conflicts with existing role", fullMatched=False)
        tdSql.error("grant role `SYSAUDIT` to ur1", expectErrInfo=f"Conflicts with existing role", fullMatched=False)
        tdSql.error("grant role `SYSAUDIT_LOG` to ur1", expectErrInfo=f"Conflicts with existing role", fullMatched=False)
        tdSql.execute(f"grant role `SYSINFO_0` to ur1")
        tdSql.execute(f"grant role `SYSINFO_1` to ur1")
        tdSql.execute(f"show users")

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
        # self.do_check_column_privileges()
        # self.do_check_grant_privileges()
        # self.do_check_view_privileges()
        # self.do_check_audit_privileges()
        # self.do_check_user_privileges()
        self.do_check_role_privileges()
        # self.do_check_variable_privileges()
        
        tdLog.debug("finish executing %s" % __file__)