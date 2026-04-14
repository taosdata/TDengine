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

    def do_check_init_env(self):
        """Check initial environment, including users and security policies"""
        # check users and their security levels
        tdSql.query("show users")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "root")
        tdSql.checkData(0, 9, "SYSAUDIT,SYSDBA,SYSSEC")
        tdSql.checkData(0, 10, "[0,4]")        
        tdSql.query("select name,sec_levels from information_schema.ins_users where name='root'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "root")
        tdSql.checkData(0, 1, "[0,4]")
        tdSql.query("select name,sec_levels from information_schema.ins_users_full where name='root'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "root")
        tdSql.checkData(0, 1, "[0,4]")
        # check security policies
        tdSql.query("show security_policies")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "SoD")
        tdSql.checkData(0, 1, "enabled")
        tdSql.checkData(0, 2, "SYSTEM")
        tdSql.checkData(0, 4, "non-mandatory, root not disabled")
        tdSql.checkData(1, 0, "MAC")
        tdSql.checkData(1, 1, "mandatory")
        tdSql.checkData(1, 2, "SYSTEM")
        tdSql.checkData(1, 4, "security levels 0-4, non-configurable")

    def do_check_sod(self):
        """Test basic Separation of Duties (SoD) with Mandatory Access Control (MAC)"""

        tdSql.execute(f"create user u1 pass '{self.test_pass}'");
        tdSql.execute(f"create user u2 pass '{self.test_pass}' security_level 0,3")
        tdSql.execute(f"create user u3 pass '{self.test_pass}' security_level 4,4")
        tdSql.execute("create role r1")
        tdSql.query("show roles")
        tdSql.checkRows(7)
        tdSql.execute("show role privileges")

        tdSql.query("select name,sec_levels from information_schema.ins_users where name='u1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "u1")
        tdSql.checkData(0, 1, "[0,1]")
        tdSql.query("select name,sec_levels from information_schema.ins_users where name='u2'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "u2")
        tdSql.checkData(0, 1, "[0,3]")
        tdSql.query("select name,sec_levels from information_schema.ins_users where name='u3'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "u3")
        tdSql.checkData(0, 1, "[4,4]")

        tdSql.error("alter cluster 'sod' 'enabled'", expectErrInfo="Invalid configuration value", fullMatched=False)
        tdSql.error("alter cluster 'separation_of_duties' 'mandatory'", expectErrInfo="No enabled user with SYSDBA role found to satisfy SoD policy", fullMatched=False)
        tdSql.execute("grant role `SYSDBA` to u1")
        tdSql.error("alter cluster 'sod' 'mandatory'", expectErrInfo="No enabled user with SYSSEC role found to satisfy SoD policy", fullMatched=False)
        tdSql.execute("grant role `SYSSEC` to u2")
        tdSql.error("alter cluster 'sod' 'mandatory'", expectErrInfo="No enabled user with SYSAUDIT role found to satisfy SoD policy", fullMatched=False)
        tdSql.execute("grant role `SYSAUDIT` to u3")
        tdSql.execute("alter cluster 'sod' 'mandatory'")
        time.sleep(5) # wait for hb dispatch and SoD state update
        tdSql.error("show security_policies", expectErrInfo="User is disabled", fullMatched=False)
        tdSql.error("show cluster", expectErrInfo="User is disabled", fullMatched=False)
        tdSql.error("select server_version()", expectErrInfo="User is disabled", fullMatched=False)
        tdSql.error("show grants", expectErrInfo="User is disabled", fullMatched=False)
        tdSql.error("create database d1", expectErrInfo="User is disabled", fullMatched=False)
        tdSql.error("grant create database to u1", expectErrInfo="User is disabled", fullMatched=False)
        tdSql.error("select now()", expectErrInfo="User is disabled", fullMatched=False)


        tdSql.connect(user="u1", password=self.test_pass)
        tdSql.query("show security_policies")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "SoD")
        tdSql.checkData(0, 1, "mandatory")
        tdSql.checkData(0, 2, "root")
        tdSql.checkData(0, 4, "system is operational, root disabled permanently")
        tdSql.checkData(1, 0, "MAC")
        tdSql.checkData(1, 1, "mandatory")
        tdSql.checkData(1, 2, "SYSTEM")
        tdSql.checkData(1, 4, "security levels 0-4, non-configurable")

        # F1-T7: Close SoD after mandatory → rejected (no downgrade)
        tdSql.error("alter cluster 'sod' 'enabled'",
                     expectErrInfo="Invalid configuration value", fullMatched=False)

        # drop user restricted in SoD mandatory mode
        tdSql.error("drop user u1", expectErrInfo="No enabled user with SYSDBA role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("drop user u2", expectErrInfo="No enabled user with SYSSEC role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("drop user u3", expectErrInfo="No enabled user with SYSAUDIT role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("drop user u3", expectErrInfo="No enabled user with SYSAUDIT role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("drop user u2", expectErrInfo="No enabled user with SYSSEC role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("drop user u1", expectErrInfo="No enabled user with SYSDBA role found to satisfy SoD policy", fullMatched=False)
        # disable user retricted in SoD mandatory mode
        tdSql.error("alter user u1 enable 0", expectErrInfo="No enabled user with SYSDBA role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("alter user u2 enable 0", expectErrInfo="No enabled user with SYSSEC role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("alter user u3 enable 0", expectErrInfo="No enabled user with SYSAUDIT role found to satisfy SoD policy", fullMatched=False)
        # enable root is restricted in SoD mandatory mode
        tdSql.error("alter user root enable 1", expectErrInfo="Insufficient privilege for operation", fullMatched=False)
        # revoke role from user restricted in SoD mandatory mode
        tdSql.error("revoke role `SYSDBA` from u1", expectErrInfo="No enabled user with SYSDBA role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("revoke role `SYSSEC` from u2", expectErrInfo="Permission denied or target object not exist", fullMatched=False)
        tdSql.error("revoke role `SYSAUDIT` from u3", expectErrInfo="Permission denied or target object not exist", fullMatched=False)
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.error("revoke role `SYSSEC` from u2", expectErrInfo="No enabled user with SYSSEC role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("revoke role `SYSAUDIT` from u3", expectErrInfo="Permission denied or target object not exist", fullMatched=False)
        tdSql.error("revoke role `SYSDBA` from u1", expectErrInfo="Permission denied or target object not exist", fullMatched=False)
        tdSql.connect(user="u3", password=self.test_pass)
        tdSql.error("revoke role `SYSAUDIT` from u3", expectErrInfo="No enabled user with SYSAUDIT role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("revoke role `SYSSEC` from u2", expectErrInfo="Permission denied or target object not exist", fullMatched=False)
        tdSql.error("revoke role `SYSDBA` from u1", expectErrInfo="Permission denied or target object not exist", fullMatched=False)


        tdSql.connect(user="u1", password=self.test_pass)
        tdSql.execute("drop database if exists d0")
        tdSql.execute("create database d0")
        tdSql.query("select name, sec_level from information_schema.ins_databases where name='d0'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "d0")
        tdSql.checkData(0, 1, 0)
        tdSql.execute("use d0")
        tdSql.execute("create table d0.stb0 (ts timestamp, c0 int,c1 int) tags(t1 int)")
        tdSql.query("select stable_name, sec_level from information_schema.ins_stables where stable_name='stb0'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb0")
        tdSql.checkData(0, 1, 0)
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


        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("grant role r1 to u1")
        tdSql.execute("revoke role `SYSINFO_1` from u1")
        tdSql.execute("show users")
        tdSql.execute("show user privileges")
        tdSql.execute("grant create database to u1")
        tdSql.execute("grant create table on database d0 to u1")
        tdSql.execute("grant use database on database d0 to u1")
        tdSql.execute("grant use on database d0 to u1")
        tdSql.execute("grant select(c0,c1),insert(ts,c0),delete on table d0.stb0 with t1=0 and ts=0 to u1")

        # F1-T9 extended: create a second SYSDBA holder, then drop the original
        tdSql.connect(user="u1", password=self.test_pass)
        tdSql.execute(f"create user u_dba2 pass '{self.test_pass}'")
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("grant role `SYSDBA` to u_dba2")
        time.sleep(2)
        tdSql.connect(user="u_dba2", password=self.test_pass)
        # Now can drop original SYSDBA holder since u_dba2 also has SYSDBA
        tdSql.execute("drop user u1")
        # Verify the new SYSDBA user works
        tdSql.execute("show users")
        tdSql.execute("drop database if exists d0")
        tdSql.execute("create database d0")
        tdSql.execute("drop database d0")

    def do_check_mac(self):
        """Test basic mandatory access control with security levels"""

        # Note: after do_check_sod, u1 is dropped, u_dba2 has SYSDBA, u2 has SYSSEC, u3 has SYSAUDIT
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop database if exists d0")
        tdSql.execute("create database d0")
        tdSql.query("select name, sec_level from information_schema.ins_databases where name='d0'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "d0")
        tdSql.checkData(0, 1, 0)

        # --- Alter user security_level ---
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute(f"create user u_mac1 pass '{self.test_pass}'")
        tdSql.execute("alter user u_mac1 security_level 1,3")
        tdSql.query("select name, sec_levels from information_schema.ins_users where name='u_mac1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "[1,3]")

        # Also check ins_users_full
        tdSql.query("select name, sec_levels from information_schema.ins_users_full where name='u_mac1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "[1,3]")

        # --- Create DB with explicit security_level ---
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop database if exists d_mac1")
        tdSql.execute("create database d_mac1 security_level 3")
        tdSql.query("select name, sec_level from information_schema.ins_databases where name='d_mac1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        # --- Alter DB security_level ---
        tdSql.execute("alter database d0 security_level 2")
        tdSql.query("select name, sec_level from information_schema.ins_databases where name='d0'")
        tdSql.checkData(0, 1, 2)

        # --- Create STB with explicit security_level ---
        tdSql.execute("create table d_mac1.stb_mac0 (ts timestamp, val int) tags(t1 int)")
        tdSql.query("select stable_name, sec_level from information_schema.ins_stables where stable_name='stb_mac0'")
        tdSql.checkRows(1)
        # inherits max(user.min_level, db.security_level)

        tdSql.execute("create table d_mac1.stb_mac1 (ts timestamp, val int) tags(t1 int) security_level 4")
        tdSql.query("select stable_name, sec_level from information_schema.ins_stables where stable_name='stb_mac1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 4)

        # --- Invalid security_level ranges ---
        tdSql.error("create database d_mac_bad security_level 5",
                     expectErrInfo="Invalid option security_level", fullMatched=False)
        tdSql.error("create database d_mac_bad security_level -1",
                     expectErrInfo="Invalid option security_level", fullMatched=False)
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.error(f"create user u_mac_bad pass '{self.test_pass}' security_level 5,5",
                     expectErrInfo="Invalid", fullMatched=False)

        # --- System databases have security_level = 0 ---
        tdSql.query("select name, sec_level from information_schema.ins_databases where name='information_schema'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        tdSql.query("select name, sec_level from information_schema.ins_databases where name='performance_schema'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)

        # --- Cleanup ---
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop database if exists d0")
        tdSql.execute("drop database if exists d_mac1")
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("drop user u_mac1")


    #
    # ------------------- main ----------------
    #
    def test_priv_dac_mac(self):
        """Test basic privileges of Discretionary Access Control and Mandatory Access Control
        
        1. Test mandatory SoD(Separation of Duty).
        2. Test mandatory access control with security levels.
        
        Since: v3.4.1.0

        Labels: basic,ci

        Jira: 6670071929,6671585124

        History:
            - 2026-02-19 Kaili Xu Initial creation(6670071929,6671585124)
        """

        self.do_check_init_env()
        self.do_check_sod()
        self.do_check_mac()
    
        tdLog.debug("finish executing %s" % __file__)