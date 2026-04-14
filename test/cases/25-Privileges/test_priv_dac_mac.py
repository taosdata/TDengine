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
        tdSql.execute(f"create user u2 pass '{self.test_pass}'")
        tdSql.execute("alter user u2 security_level 0,3")
        tdSql.execute(f"create user u3 pass '{self.test_pass}'")
        tdSql.execute("alter user u3 security_level 4,4")
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

        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.error("alter cluster 'sod' 'enabled'",
                     expectErrInfo="Invalid configuration value", fullMatched=False)

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
                     expectErrInfo="Insufficient privilege for operation", fullMatched=False)

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
        tdSql.checkData(0, 1, 1)  # MAC: DB default secLevel = creator.maxSecLevel = 1
        tdSql.execute("use d0")
        tdSql.execute("create table d0.stb0 (ts timestamp, c0 int,c1 int) tags(t1 int)")
        tdSql.query("select stable_name, sec_level from information_schema.ins_stables where stable_name='stb0'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb0")
        tdSql.checkData(0, 1, 1)  # MAC: STB default secLevel = max(creator.max, db.level) = max(1,1) = 1
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
        tdSql.execute("grant role `SYSDBA` to u_dba2")
        tdSql.connect(user="u_dba2", password=self.test_pass)
        # Now can drop original SYSDBA holder since u_dba2 also has SYSDBA
        tdSql.execute("drop user u1")
        # Verify the new SYSDBA user works
        tdSql.execute("show users")
        tdSql.execute("drop database if exists d0")
        tdSql.execute("create database d0")
        tdSql.execute("drop database d0")

    def do_check_mac(self):
        """Test Mandatory Access Control: NRU (No Read Up) and NWD (No Write Down)"""
        # After do_check_sod: u_dba2=SYSDBA, u2=SYSSEC[0,3], u3=SYSAUDIT[4,4], root disabled
        self.do_check_mac_setup()
        self.do_check_mac_user_security_level()
        self.do_check_mac_db_nru()
        self.do_check_mac_select_nru()
        self.do_check_mac_insert_nwd()
        self.do_check_mac_delete_nru()
        self.do_check_mac_ddl()
        self.do_check_mac_cleanup()

    def do_check_mac_setup(self):
        """Setup MAC test environment: users, databases, tables, and grants"""
        # SYSSEC sets u_dba2's security level to [0,2]
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter user u_dba2 security_level 0,2")

        # SYSDBA creates test users
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute(f"create user u_mac_low pass '{self.test_pass}'")    # default [0,1]
        tdSql.execute(f"create user u_mac_mid pass '{self.test_pass}'")
        tdSql.execute(f"create user u_mac_high pass '{self.test_pass}'")

        # SYSSEC sets security levels
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter user u_mac_mid security_level 1,3")
        tdSql.execute("alter user u_mac_high security_level 3,4")

        # Create databases
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop database if exists d_mac0")
        tdSql.execute("drop database if exists d_mac2")
        tdSql.execute("create database d_mac0")    # default level 0
        tdSql.execute("create database d_mac2")    # will be altered to level 2

        # SYSSEC alters DB security levels
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter database d_mac2 security_level 2")

        # Create STBs (u_dba2 max=2, d_mac0 level=0 → default STB level = max(2,0) = 2)
        # Insert data FIRST, then raise security levels
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("create table d_mac0.stb_lvl2 (ts timestamp, val int) tags(t1 int)")
        tdSql.execute("create table d_mac0.stb_lvl3 (ts timestamp, val int) tags(t1 int)")
        tdSql.execute("create table d_mac0.ntb1 (ts timestamp, val int)")
        tdSql.execute("create table d_mac0.ctb_l2 using d_mac0.stb_lvl2 tags(1)")
        tdSql.execute("create table d_mac0.ctb_l3 using d_mac0.stb_lvl3 tags(1)")
        tdSql.execute("insert into d_mac0.ctb_l2 values(now, 100)")
        tdSql.execute("insert into d_mac0.ctb_l3 values(now, 200)")
        tdSql.execute("insert into d_mac0.ntb1 values(now, 300)")

        # Create STB in d_mac2 (u_dba2 max=2, db level=2 → STB level = max(2,2) = 2)
        tdSql.execute("create table d_mac2.stb_d2 (ts timestamp, val int) tags(t1 int)")
        tdSql.execute("create table d_mac2.ctb_d2 using d_mac2.stb_d2 tags(1)")
        tdSql.execute("insert into d_mac2.ctb_d2 values(now, 400)")

        # SYSSEC alters stb_lvl3 from level 2 to level 3
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter table d_mac0.stb_lvl3 security_level 3")

        # Verify STB levels
        tdSql.query("select stable_name, sec_level from information_schema.ins_stables where db_name='d_mac0' and stable_name='stb_lvl2'")
        tdSql.checkData(0, 1, 2)
        tdSql.query("select stable_name, sec_level from information_schema.ins_stables where db_name='d_mac0' and stable_name='stb_lvl3'")
        tdSql.checkData(0, 1, 3)

        # Grant DAC privileges to test users (SYSSEC manages grants)
        for user in ['u_mac_low', 'u_mac_mid', 'u_mac_high']:
            tdSql.execute(f"grant use database on database d_mac0 to {user}")
            tdSql.execute(f"grant use database on database d_mac2 to {user}")
            for tbl in ['stb_lvl2', 'stb_lvl3', 'ntb1']:
                tdSql.execute(f"grant select,insert,delete on table d_mac0.{tbl} to {user}")
            tdSql.execute(f"grant select,insert,delete on table d_mac2.stb_d2 to {user}")
        tdSql.execute("flush database d_mac0")
        tdSql.execute("flush database d_mac2")
        time.sleep(2)

    def do_check_mac_user_security_level(self):
        """Test ALTER USER security_level requires SYSSEC and validates range"""
        # Verify user levels are set correctly
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.query("select name, sec_levels from information_schema.ins_users where name='u_mac_low'")
        tdSql.checkData(0, 1, "[0,1]")
        tdSql.query("select name, sec_levels from information_schema.ins_users where name='u_mac_mid'")
        tdSql.checkData(0, 1, "[1,3]")
        tdSql.query("select name, sec_levels from information_schema.ins_users where name='u_mac_high'")
        tdSql.checkData(0, 1, "[3,4]")

        # Non-SYSSEC user cannot ALTER security_level
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.error("alter user u_mac_low security_level 0,2",
                     expectErrInfo="Insufficient privilege", fullMatched=False)

        # Invalid security_level range
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.error("alter user u_mac_low security_level 5,5",
                     expectErrInfo="Invalid", fullMatched=False)
        tdSql.error("alter user u_mac_low security_level 3,1",
                     expectErrInfo="Invalid", fullMatched=False)

        # System databases always level 0
        tdSql.query("select name, sec_level from information_schema.ins_databases where name='information_schema'")
        tdSql.checkData(0, 1, 0)

    def do_check_mac_db_nru(self):
        """Test NRU enforcement at database level: user.max must >= db.securityLevel"""
        # u_mac_low (max=1) cannot USE d_mac2 (level=2): NRU blocks
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.execute("use d_mac0")  # level 0 → OK
        tdSql.error("use d_mac2",
                     expectErrInfo="Insufficient privilege", fullMatched=False)
        tdSql.error("select * from d_mac2.stb_d2",
                     expectErrInfo="Insufficient privilege", fullMatched=False)

        # u_mac_mid (max=3) can USE d_mac2 (level=2): 3 >= 2
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("use d_mac2")
        tdSql.execute("select * from d_mac2.ctb_d2")

        # u_mac_high (max=4) can USE d_mac2 (level=2): 4 >= 2
        tdSql.connect(user="u_mac_high", password=self.test_pass)
        tdSql.execute("use d_mac2")
        tdSql.execute("select * from d_mac2.ctb_d2")

    def do_check_mac_select_nru(self):
        """Test NRU for SELECT: user.maxSecLevel must be >= table.securityLevel"""
        # u_mac_low (max=1) cannot SELECT from stb_lvl2 (level=2)
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.error("select * from d_mac0.stb_lvl2",
                     expectErrInfo="MAC security level", fullMatched=False)
        # u_mac_low cannot SELECT from CTB of stb_lvl2 (inherits level 2)
        tdSql.error("select * from d_mac0.ctb_l2",
                     expectErrInfo="MAC security level", fullMatched=False)
        # u_mac_low cannot SELECT from stb_lvl3 (level=3)
        tdSql.error("select * from d_mac0.stb_lvl3",
                     expectErrInfo="MAC security level", fullMatched=False)
        # u_mac_low CAN SELECT from ntb1 (NTB inherits DB level 0, no table-level MAC block)
        tdSql.execute("select * from d_mac0.ntb1")

        # u_mac_mid (max=3) can SELECT from stb_lvl2 (level=2): 3 >= 2
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("select * from d_mac0.stb_lvl2")
        tdSql.execute("select * from d_mac0.ctb_l2")
        # u_mac_mid can SELECT from stb_lvl3 (level=3): 3 >= 3
        tdSql.execute("select * from d_mac0.stb_lvl3")
        tdSql.execute("select * from d_mac0.ctb_l3")

        # u_mac_high (max=4) can SELECT from all
        tdSql.connect(user="u_mac_high", password=self.test_pass)
        tdSql.execute("select * from d_mac0.stb_lvl2")
        tdSql.execute("select * from d_mac0.stb_lvl3")
        tdSql.execute("select * from d_mac0.ctb_l2")
        tdSql.execute("select * from d_mac0.ctb_l3")

    def do_check_mac_insert_nwd(self):
        """Test NWD+NRU for INSERT: user.min <= table.secLvl <= user.max"""
        # u_mac_low (min=0, max=1) INSERT ctb_l2 (level=2): NRU blocks (1 < 2)
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.error("insert into d_mac0.ctb_l2 values(now, 10)",
                     expectErrInfo="MAC security level", fullMatched=False)

        # u_mac_mid (min=1, max=3) INSERT ctb_l2 (level=2): allowed (1 <= 2 <= 3)
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("insert into d_mac0.ctb_l2 values(now, 11)")

        # u_mac_high (min=3, max=4) INSERT ctb_l2 (level=2): NWD blocks (3 > 2)
        tdSql.connect(user="u_mac_high", password=self.test_pass)
        tdSql.error("insert into d_mac0.ctb_l2 values(now, 12)",
                     expectErrInfo="MAC security level", fullMatched=False)

        # u_mac_mid (min=1, max=3) INSERT ctb_l3 (level=3): allowed (1 <= 3 <= 3)
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("insert into d_mac0.ctb_l3 values(now, 21)")

        # u_mac_high (min=3, max=4) INSERT ctb_l3 (level=3): allowed (3 <= 3 <= 4)
        tdSql.connect(user="u_mac_high", password=self.test_pass)
        tdSql.execute("insert into d_mac0.ctb_l3 values(now, 22)")

        # u_mac_low (min=0, max=1) INSERT ctb_l3 (level=3): NRU blocks (1 < 3)
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.error("insert into d_mac0.ctb_l3 values(now, 20)",
                     expectErrInfo="MAC security level", fullMatched=False)

    def do_check_mac_delete_nru(self):
        """Test NRU for DELETE: user.maxSecLevel must be >= table.securityLevel"""
        # u_mac_low (max=1) cannot DELETE from stb_lvl2 (level=2)
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.error("delete from d_mac0.stb_lvl2 where ts < now",
                     expectErrInfo="MAC security level", fullMatched=False)

        # u_mac_mid (max=3) can DELETE from stb_lvl2 (level=2): 3 >= 2
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("delete from d_mac0.ctb_l2 where ts < '2000-01-01'")

    def do_check_mac_ddl(self):
        """Test DDL operations: ALTER/DROP STB requires SYSSEC for security_level, NRU for access"""
        # Non-SYSSEC user cannot ALTER STB security_level
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.error("alter table d_mac0.stb_lvl2 security_level 1",
                     expectErrInfo="Insufficient privilege", fullMatched=False)

        # SYSSEC can ALTER STB security_level
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter table d_mac0.stb_lvl2 security_level 2")  # no-op, same level

        # SYSSEC cannot ALTER DB security_level if not SYSSEC
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.error("alter database d_mac0 security_level 1",
                     expectErrInfo="Insufficient privilege", fullMatched=False)

        # SYSSEC can ALTER DB security_level
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter database d_mac0 security_level 0")  # restore to 0

    def do_check_mac_cleanup(self):
        """Clean up MAC test objects"""
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop database if exists d_mac0")
        tdSql.execute("drop database if exists d_mac2")
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("drop user u_mac_low")
        tdSql.execute("drop user u_mac_mid")
        tdSql.execute("drop user u_mac_high")


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