from new_test_framework.utils import tdLog, tdSql, tdDnodes, etool, TDSetSql
from new_test_framework.utils.sqlset import TDSetSql
from itertools import product
import os
import time
import shutil
import taos
from taos import SmlProtocol, SmlPrecision

class TestCase:

    test_pass = "Passsword_123!"

    @classmethod
    def setup_cls(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.setsql = TDSetSql()

    def restart_dnode_and_reconnect(self, user="root", password="taosdata", retry=20):
        tdDnodes.stop(1)
        tdDnodes.startWithoutSleep(1)
        last_err = None
        for _ in range(retry):
            try:
                time.sleep(1)
                tdSql.connect(user=user, password=password)
                return
            except Exception as err:
                last_err = err
        raise last_err

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
        tdSql.checkData(1, 1, "disabled")  # MAC defaults to disabled; must be explicitly activated
        tdSql.checkData(1, 4, "not activated; enable via: ALTER CLUSTER 'MAC' 'mandatory'")

    def do_check_sod(self):
        """Test basic Separation of Duties (SoD) with Mandatory Access Control (MAC)"""

        tdSql.execute(f"create user u1 pass '{self.test_pass}'");
        tdSql.execute(f"create user u2 pass '{self.test_pass}'")
        tdSql.execute("alter user u2 security_level 0,4")
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
        tdSql.checkData(0, 1, "[0,4]")
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
        tdSql.checkData(1, 1, "disabled")
        tdSql.checkData(1, 4, "not activated; enable via: ALTER CLUSTER 'MAC' 'mandatory'")

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
        # After do_check_sod: u_dba2=SYSDBA, u2=SYSSEC[0,4], u3=SYSAUDIT[4,4], root disabled
        self.do_check_mac_activation()
        self.do_check_mac_setup()
        self.do_check_mac_user_security_level()
        self.do_check_mac_db_nru()
        self.do_check_mac_select_nru()
        self.do_check_mac_insert_nwd()
        self.do_check_mac_delete_nru()
        self.do_check_mac_ddl()
        self.do_check_mac_show_and_show_create()
        self.do_check_mac_stmt_stmt2()
        self.do_check_mac_schemaless()
        self.do_check_mac_cleanup()

    def do_check_mac_activation(self):
        """Test F2-T19 to F2-T28: MAC activation and role-floor constraint under MAC"""
        # F2-T19: MAC disabled — no security enforcement before activation
        # After SoD: u2=SYSSEC, root disabled. Connect as u_dba2 (SYSDBA) who can create DBs.
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("create database if not exists d_mac_test")
        tdSql.execute("create stable if not exists d_mac_test.stb0 (ts timestamp, v int) tags (t int)")
        # Verify show security_policies shows MAC as inactive
        tdSql.query("select name, mode from information_schema.ins_security_policies where name='MAC'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "disabled")

        # F2-T25: MAC disabled → GRANT high-level role (SYSDBA, floor=3) to user with maxSecLevel=1
        # No floor check should be enforced when MAC is not active.
        # Only SYSDBA (u_dba2) can grant SYSDBA to another user.
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute(f"create user u_floor_test pass '{self.test_pass}'")
        # u_floor_test sec_level=[0,1] (default); SYSDBA floor=3; MAC not active → grant succeeds
        tdSql.execute("grant role `SYSDBA` to u_floor_test")
        tdSql.connect(user="u2", password=self.test_pass)
        # MAC disabled: ALTER USER security_level also does not enforce role floor
        tdSql.execute("alter user u_floor_test security_level 0,2")
        tdSql.query("select name, sec_levels from information_schema.ins_users where name='u_floor_test'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "[0,2]")  # floor not enforced while MAC is inactive

        # F2-T20: Non-SYSSEC user cannot activate MAC
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.error("alter cluster 'MAC' 'mandatory'", expectErrInfo="Insufficient privilege for operation")
        tdSql.error("alter cluster 'mandatory_access_control' 'mandatory'", expectErrInfo="Insufficient privilege for operation")
        # F2-T20b: Invalid value 'enabled' is rejected
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.error("alter cluster 'MAC' 'enabled'", expectErrInfo="Invalid configuration value")
        tdSql.error("alter cluster 'MAC' 'disabled'", expectErrInfo="Invalid configuration value")

        # F2-T21: SYSSEC activates MAC — succeeds
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter cluster 'MAC' 'mandatory'")
        # Verify show security_policies shows MAC as mandatory
        tdSql.query("select name, mode from information_schema.ins_security_policies where name='MAC'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "mandatory")

        # F2-T28: MAC activation auto-upgrades users whose maxSecLevel < role floor (atomically).
        # u_floor_test: SYSDBA (floor=3), maxSecLevel=1 → should be auto-upgraded to [0,3]
        tdSql.query("select name, sec_levels from information_schema.ins_users where name='u_floor_test'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "[0,3]")
        # u_dba2: SYSDBA (floor=3), maxSecLevel=1 → auto-upgraded to [0,3]
        tdSql.query("select name, sec_levels from information_schema.ins_users where name='u_dba2'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "[0,3]")

        # F2-T26: MAC active → GRANT role with floor > user.maxSecLevel → fail.
        # Use a fresh user (no management role) and try to grant SYSSEC (floor=4) when maxSecLevel=1.
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute(f"create user u_floor_test2 pass '{self.test_pass}'")  # default sec_level=[0,1]
        tdSql.connect(user="u2", password=self.test_pass)
        # SYSSEC floor=4; u_floor_test2 maxSecLevel=1 < 4 → rejected under MAC
        tdSql.error("grant role `SYSSEC` to u_floor_test2",
                    expectErrInfo="Insufficient", fullMatched=False)
        # After raising maxSecLevel to 4, GRANT succeeds.
        tdSql.execute("alter user u_floor_test2 security_level 0,4")
        tdSql.execute("grant role `SYSSEC` to u_floor_test2")

        # F2-T27: MAC active → ALTER USER security_level below current role floor → fail.
        # u_floor_test has SYSDBA (floor=3), maxSecLevel=3; try to lower maxSecLevel to 2 → fail.
        tdSql.error("alter user u_floor_test security_level 0,2",
                    expectErrInfo="Insufficient", fullMatched=False)
        # SYSAUDIT floor=4: existing user u3 (SYSAUDIT) cannot be lowered below maxSecLevel=4
        tdSql.error("alter user u3 security_level 0,3",
                expectErrInfo="Insufficient", fullMatched=False)
        # Set to exactly the floor (=3) → success (already at floor, no-op).
        tdSql.execute("alter user u_floor_test security_level 0,3")

        # Cleanup floor-test users (u_dba2=SYSDBA, u2=SYSSEC, u3=SYSAUDIT all still present)
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop user u_floor_test")
        tdSql.execute("drop user u_floor_test2")
        tdSql.connect(user="u2", password=self.test_pass)

        # F2-T22: Repeat activation is idempotent (no error)
        tdSql.execute("alter cluster 'MAC' 'mandatory'")
        tdSql.query("select name, mode from information_schema.ins_security_policies where name='MAC'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "mandatory")

        # F2-T23: Users with security_level [0,4] hit Layer 1 fast-path after MAC active
        # Use u_dba2 (SYSDBA, owner of d_mac_test) with [0,4] to verify fast-path
        # u2 (SYSSEC[0,4]) sets u_dba2's level before the test
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter user u_dba2 security_level 0,4")
        tdSql.connect(user="u_dba2", password=self.test_pass)
        # SELECT and INSERT should both succeed (NRU and NWD both guaranteed at Layer 1)
        tdSql.execute("insert into d_mac_test.ctb_fp using d_mac_test.stb0 tags(1) values(now(), 1)")
        tdSql.query("select count(*) from d_mac_test.stb0")
        tdSql.checkRows(1)

        # F2-T24: MNode restart persistence — MAC mode remains mandatory after restart.
        self.restart_dnode_and_reconnect(user="u2", password=self.test_pass)
        tdSql.query("show security_policies")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "SoD")
        tdSql.checkData(1, 0, "MAC")
        tdSql.checkData(1, 1, "mandatory")

    def do_check_mac_setup(self):
        """Setup MAC test environment: users, databases, tables, and grants"""
        # NOTE: u_dba2 has SYSDBA role (floor=3), so maxSecLevel cannot be set below 3 while MAC
        # is active. STB security levels are set explicitly by SYSSEC after creation.

        # SYSDBA creates test users
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute(f"create user u_mac_low pass '{self.test_pass}'")    # default [0,1]
        tdSql.execute(f"create user u_mac_mid pass '{self.test_pass}'")
        tdSql.execute(f"create user u_mac_high pass '{self.test_pass}'")

        # SYSSEC sets security levels
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter user u_mac_mid security_level 1,3")
        tdSql.execute("alter user u_mac_high security_level 3,3")

        # Create databases; SYSSEC will set explicit security levels immediately after.
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop database if exists d_mac0")
        tdSql.execute("drop database if exists d_mac2")
        tdSql.execute("create database d_mac0")
        tdSql.execute("create database d_mac2")

        # SYSSEC alters DB security levels explicitly
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter database d_mac0 security_level 0")  # lower to 0 for NRU tests
        tdSql.execute("alter database d_mac2 security_level 2")  # keep at 2

        # Create STBs; inserts happen before SYSSEC sets explicit STB levels.
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("create table d_mac0.stb_lvl2 (ts timestamp, val int) tags(t1 int)")
        tdSql.execute("create table d_mac0.stb_lvl3 (ts timestamp, val int) tags(t1 int)")
        tdSql.execute("create table d_mac0.ntb1 (ts timestamp, val int)")
        tdSql.execute("create table d_mac0.ctb_l2 using d_mac0.stb_lvl2 tags(1)")
        tdSql.execute("create table d_mac0.ctb_l3 using d_mac0.stb_lvl3 tags(1)")
        tdSql.execute("insert into d_mac0.ctb_l2 values(now, 100)")
        tdSql.execute("insert into d_mac0.ctb_l3 values(now, 200)")
        tdSql.execute("insert into d_mac0.ntb1 values(now, 300)")

        # Create STB in d_mac2 (db level=2)
        tdSql.execute("create table d_mac2.stb_d2 (ts timestamp, val int) tags(t1 int)")
        tdSql.execute("create table d_mac2.ctb_d2 using d_mac2.stb_d2 tags(1)")
        tdSql.execute("create table d_mac2.ntb_d2 (ts timestamp, val int)")
        tdSql.execute("insert into d_mac2.ctb_d2 values(now, 400)")
        tdSql.execute("insert into d_mac2.ntb_d2 values(now, 401)")

        # SYSSEC sets explicit STB security levels after creation
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter table d_mac0.stb_lvl2 security_level 2")   # explicit level 2
        tdSql.execute("alter table d_mac0.stb_lvl3 security_level 3")   # explicit level 3
        tdSql.execute("alter table d_mac2.stb_d2 security_level 2")     # explicit level 2

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
                tdSql.execute(f"grant select,insert,delete,alter on table d_mac0.{tbl} to {user}")
            tdSql.execute(f"grant select,insert,delete,alter on table d_mac2.stb_d2 to {user}")
            tdSql.execute(f"grant select,insert,delete,alter on table d_mac2.ntb_d2 to {user}")
        tdSql.connect(user="u_dba2", password=self.test_pass)
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
        tdSql.checkData(0, 1, "[3,3]")

        # Non-SYSSEC user cannot ALTER security_level
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.error("alter user u_mac_low security_level 0,2",
                     expectErrInfo="Insufficient privilege", fullMatched=False)

        # Invalid security_level range
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.error("alter user u_mac_low security_level 5,5",
                     expectErrInfo="out of range", fullMatched=False)
        tdSql.error("alter user u_mac_low security_level 3,1",
                     expectErrInfo="cannot be larger", fullMatched=False)
        # SYSSEC with max=4 CAN set user to [0,4], then restore
        tdSql.execute("alter user u_mac_low security_level 0,4")
        tdSql.execute("alter user u_mac_low security_level 0,1")

        # System databases: sec_level is NULL (system DBs only show minimal config)
        tdSql.query("select name, sec_level from information_schema.ins_databases where name='information_schema'")
        tdSql.checkData(0, 1, None)

    def do_check_mac_db_nru(self):
        """Test NRU enforcement at database level: user.max must >= db.securityLevel"""
        # u_mac_low (max=1) cannot USE d_mac2 (level=2): NRU blocks
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.execute("use d_mac0")  # level 0 → OK
        tdSql.error("use d_mac2",
                     expectErrInfo="security level", fullMatched=False)
        tdSql.error("select * from d_mac2.stb_d2",
                     expectErrInfo="security level", fullMatched=False)

        # u_mac_mid (max=3) can USE d_mac2 (level=2): 3 >= 2
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("use d_mac2")
        tdSql.execute("select * from d_mac2.ctb_d2")

        # u_mac_high (max=3) can USE d_mac2 (level=2): 3 >= 2
        tdSql.connect(user="u_mac_high", password=self.test_pass)
        tdSql.execute("use d_mac2")
        tdSql.execute("select * from d_mac2.ctb_d2")

    def do_check_mac_select_nru(self):
        """Test NRU for SELECT: user.maxSecLevel must be >= table.securityLevel"""
        # u_mac_low (max=1) cannot SELECT from stb_lvl2 (level=2)
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.error("select * from d_mac0.stb_lvl2",
                     expectErrInfo="security level", fullMatched=False)
        # u_mac_low cannot SELECT from CTB of stb_lvl2 (inherits level 2)
        tdSql.error("select * from d_mac0.ctb_l2",
                     expectErrInfo="security level", fullMatched=False)
        # u_mac_low cannot SELECT from stb_lvl3 (level=3)
        tdSql.error("select * from d_mac0.stb_lvl3",
                     expectErrInfo="security level", fullMatched=False)
        # u_mac_low CAN SELECT from ntb1 (NTB inherits DB level 0, no table-level MAC block)
        tdSql.execute("select * from d_mac0.ntb1")

        # u_mac_mid (max=3) can SELECT from stb_lvl2 (level=2): 3 >= 2
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("select * from d_mac0.stb_lvl2")
        tdSql.execute("select * from d_mac0.ctb_l2")
        # u_mac_mid can SELECT from stb_lvl3 (level=3): 3 >= 3
        tdSql.execute("select * from d_mac0.stb_lvl3")
        tdSql.execute("select * from d_mac0.ctb_l3")

        # u_mac_high (max=3) can SELECT from all current test objects
        tdSql.connect(user="u_mac_high", password=self.test_pass)
        tdSql.execute("select * from d_mac0.stb_lvl2")
        tdSql.execute("select * from d_mac0.stb_lvl3")
        tdSql.execute("select * from d_mac0.ctb_l2")
        tdSql.execute("select * from d_mac0.ctb_l3")

        # Cross-DB: u_mac_mid (max=3) can SELECT from d_mac2 tables (db level=2, stb level=2)
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("select * from d_mac2.ctb_d2")
        # u_mac_low (max=1) cannot SELECT from d_mac2 tables (db level=2, NRU blocks at DB level)
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.error("select * from d_mac2.ctb_d2",
                     expectErrInfo="Insufficient", fullMatched=False)

    def do_check_mac_insert_nwd(self):
        """Test NWD+NRU for INSERT: user.min <= table.secLvl <= user.max"""
        # u_mac_low (min=0, max=1) INSERT ctb_l2 (level=2): NRU blocks (1 < 2)
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.error("insert into d_mac0.ctb_l2 values(now, 10)",
                     expectErrInfo="security level", fullMatched=False)

        # u_mac_mid (min=1, max=3) INSERT ctb_l2 (level=2): allowed (1 <= 2 <= 3)
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("insert into d_mac0.ctb_l2 values(now, 11)")

        # u_mac_high (min=3, max=3) INSERT ctb_l2 (level=2): NWD blocks (3 > 2)
        tdSql.connect(user="u_mac_high", password=self.test_pass)
        tdSql.error("insert into d_mac0.ctb_l2 values(now, 12)",
                     expectErrInfo="too high to write", fullMatched=False)

        # u_mac_mid (min=1, max=3) INSERT ctb_l3 (level=3): allowed (1 <= 3 <= 3)
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("insert into d_mac0.ctb_l3 values(now, 21)")

        # u_mac_high (min=3, max=3) INSERT ctb_l3 (level=3): allowed (3 <= 3 <= 3)
        tdSql.connect(user="u_mac_high", password=self.test_pass)
        tdSql.execute("insert into d_mac0.ctb_l3 values(now, 22)")

        # u_mac_low (min=0, max=1) INSERT ctb_l3 (level=3): NRU blocks (1 < 3)
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.error("insert into d_mac0.ctb_l3 values(now, 20)",
                     expectErrInfo="security level", fullMatched=False)

        # INSERT into NTB (normal table inherits DB level=0): u_mac_low (min=0, max=1) allowed
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.execute("insert into d_mac0.ntb1 values(now, 50)")

        # INSERT into d_mac2.ctb_d2 (db level=2, stb level=2):
        # u_mac_low (max=1) blocked at DB level (NRU: 1 < 2)
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.error("insert into d_mac2.ctb_d2 values(now, 60)",
                     expectErrInfo="Insufficient", fullMatched=False)
        # u_mac_mid (min=1, max=3) allowed: 1 <= 2 <= 3
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("insert into d_mac2.ctb_d2 values(now, 61)")
        # u_mac_high (min=3, max=3) NWD blocks: 3 > 2
        tdSql.connect(user="u_mac_high", password=self.test_pass)
        tdSql.error("insert into d_mac2.ctb_d2 values(now, 62)",
                     expectErrInfo="too high to write", fullMatched=False)

    def do_check_mac_delete_nru(self):
        """Test NRU for DELETE: user.maxSecLevel must be >= table.securityLevel"""
        # u_mac_low (max=1) cannot DELETE from stb_lvl2 (level=2)
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.error("delete from d_mac0.stb_lvl2 where ts < now",
                     expectErrInfo="security level", fullMatched=False)
        # u_mac_low cannot DELETE from ctb_l3 (level=3)
        tdSql.error("delete from d_mac0.ctb_l3 where ts < now",
                     expectErrInfo="security level", fullMatched=False)

        # u_mac_mid (max=3) can DELETE from stb_lvl2 (level=2): 3 >= 2
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("delete from d_mac0.ctb_l2 where ts < '2000-01-01'")
        # u_mac_mid can DELETE from stb_lvl3 (level=3): 3 >= 3
        tdSql.execute("delete from d_mac0.ctb_l3 where ts < '2000-01-01'")

        # u_mac_high (max=3) can DELETE from any current test table: 3 >= 3
        tdSql.connect(user="u_mac_high", password=self.test_pass)
        tdSql.execute("delete from d_mac0.ctb_l3 where ts < '2000-01-01'")

        # DELETE does NOT check NWD — u_mac_high (min=3) can DELETE from ctb_l2 (level=2)
        tdSql.execute("delete from d_mac0.ctb_l2 where ts < '2000-01-01'")

        # u_mac_low can DELETE from ntb1 (level=0): 1 >= 0
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.execute("delete from d_mac0.ntb1 where ts < '2000-01-01'")

        # DB-level NRU: u_mac_low cannot DELETE from d_mac2 (db level=2)
        tdSql.error("delete from d_mac2.ctb_d2 where ts < now",
                     expectErrInfo="Insufficient", fullMatched=False)

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

        # STB level below DB level is rejected
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.error("alter table d_mac2.stb_d2 security_level 1",
                     expectErrInfo="Object level below", fullMatched=False)

        # DESCRIBE accessible tables OK, inaccessible tables (DB-level NRU) blocked
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.execute("describe d_mac0.ntb1")
        tdSql.error("describe d_mac0.stb_lvl3",
                 expectErrInfo="security level", fullMatched=False)
        tdSql.error("describe d_mac2.stb_d2",
                     expectErrInfo="Insufficient", fullMatched=False)

        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("describe d_mac0.stb_lvl3")

        # F2-T14: Low-level user with ALTER privilege on high-level object → blocked by MAC NRU
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        # u_mac_low (max=1) has ALTER privilege on stb_lvl2 (level=2), but NRU blocks (1 < 2)
        tdSql.error("alter table d_mac0.stb_lvl2 add column c_new int",
                     expectErrInfo="security level", fullMatched=False)
        # u_mac_mid (max=3) can ALTER stb_lvl2 (level=2): 3 >= 2
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("alter table d_mac0.stb_lvl2 add column c_new int")
        tdSql.execute("alter table d_mac0.stb_lvl2 drop column c_new")

        # Batch SET TAG on child tables: MAC NRU enforced per-clause via macCheckTableAccess
        # u_mac_low (max=1) cannot SET TAG on ctb_l2 (inherits STB level=2) or ctb_l3 (level=3)
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.error("alter table d_mac0.ctb_l2 set tag t1 = 11 d_mac0.ctb_l3 set tag t1 = 22",
                     expectErrInfo="security level", fullMatched=False)
        # Single child table SET TAG also blocked
        tdSql.error("alter table d_mac0.ctb_l2 set tag t1 = 11",
                     expectErrInfo="security level", fullMatched=False)
        # u_mac_mid (max=3) can SET TAG on both (3 >= 2, 3 >= 3)
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("alter table d_mac0.ctb_l2 set tag t1 = 11 d_mac0.ctb_l3 set tag t1 = 22")

    def do_check_mac_show_and_show_create(self):
        """Test MAC on SHOW / SHOW CREATE operations"""
        # SHOW CREATE on high-level object should be blocked for low-level user
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.error("show create table d_mac0.stb_lvl3",
                     expectErrInfo="security level", fullMatched=False)
        tdSql.error("show create table d_mac2.stb_d2",
                     expectErrInfo="security level", fullMatched=False)

        # Mid-level user can SHOW CREATE high-level table in db0 and db2
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("show create table d_mac0.stb_lvl3")
        tdSql.execute("show create table d_mac2.stb_d2")

        # SHOW STABLES / SHOW TABLES should filter rows above the user's max security level.
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.execute("use d_mac0")
        tdSql.query("show stables")
        stable_names = {row[0] for row in tdSql.queryResult}
        assert "stb_lvl2" not in stable_names
        assert "stb_lvl3" not in stable_names

        tdSql.query("show tables")
        table_names = {row[0] for row in tdSql.queryResult}
        assert "ntb1" in table_names
        assert "ctb_l2" not in table_names
        assert "ctb_l3" not in table_names

        tdSql.query("select table_name from information_schema.ins_tables where db_name='d_mac0' order by table_name")
        info_table_names = {row[0] for row in tdSql.queryResult}
        assert "ntb1" in info_table_names
        assert "ctb_l2" not in info_table_names
        assert "ctb_l3" not in info_table_names

        tdSql.query("select table_name from information_schema.ins_tables where db_name='d_mac2' order by table_name")
        info_table_names = {row[0] for row in tdSql.queryResult}
        assert "ntb_d2" not in info_table_names
        assert "ctb_d2" not in info_table_names

        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("use d_mac0")
        tdSql.query("show stables")
        stable_names = {row[0] for row in tdSql.queryResult}
        assert "stb_lvl2" in stable_names
        assert "stb_lvl3" in stable_names

        tdSql.query("show tables")
        table_names = {row[0] for row in tdSql.queryResult}
        assert "ntb1" in table_names
        assert "ctb_l2" in table_names
        assert "ctb_l3" in table_names

        tdSql.query("select table_name from information_schema.ins_tables where db_name='d_mac2' order by table_name")
        info_table_names = {row[0] for row in tdSql.queryResult}
        assert "ntb_d2" in info_table_names
        assert "ctb_d2" in info_table_names

    def do_check_mac_stmt_stmt2(self):
        """Test MAC on STMT / STMT2 paths"""
        ts_now_ms = int(time.time() * 1000)

        # STMT INSERT: low user cannot write high-level table (NRU)
        low_conn = taos.connect(user="u_mac_low", password=self.test_pass)
        try:
            stmt = low_conn.statement("insert into d_mac0.ctb_l3 values(?, ?)")
            params = taos.new_bind_params(2)
            params[0].timestamp(ts_now_ms, taos.PrecisionEnum.Milliseconds)
            params[1].int(9001)
            try:
                stmt.bind_param(params)
                stmt.execute()
                tdLog.exit("MAC STMT failed: low-level user unexpectedly inserted into ctb_l3")
            except Exception as err:
                if "security level" not in str(err).lower() and "insufficient" not in str(err).lower():
                    tdLog.exit(f"Unexpected STMT error for MAC deny: {err}")
            stmt.close()
        finally:
            low_conn.close()

        # STMT INSERT: mid user can write table level 3
        mid_conn = taos.connect(user="u_mac_mid", password=self.test_pass)
        try:
            stmt = mid_conn.statement("insert into d_mac0.ctb_l3 values(?, ?)")
            params = taos.new_bind_params(2)
            params[0].timestamp(ts_now_ms + 1, taos.PrecisionEnum.Milliseconds)
            params[1].int(9002)
            stmt.bind_param(params)
            stmt.execute()
            stmt.close()
        finally:
            mid_conn.close()

        # STMT2 path: low user must not succeed on high-level target.
        # Current stmt2 limitations may return API errors before MAC text surfaces,
        # so we only assert the operation does not succeed.
        low_conn2 = taos.connect(user="u_mac_low", password=self.test_pass)
        try:
            try:
                stmt2 = low_conn2.statement2("insert into d_mac0.ctb_l3 values(?, ?)")
                stmt2.bind_param(None, None, [[[ts_now_ms + 2], [9003]]])
                stmt2.execute()
                tdLog.exit("MAC STMT2 failed: low-level user unexpectedly operated on d_mac0.ctb_l3")
            except Exception as err:
                tdLog.info(f"STMT2 low-level deny path raised: {err}")
            else:
                stmt2.close()
        finally:
            low_conn2.close()

    def do_check_mac_schemaless(self):
        """Test MAC on schemaless insert paths"""
        line_low = ["sml_mac_low,site=s1 v=1i 1744680000000000000"]

        # Low-level user blocked at DB-level NRU in d_mac2 (db sec level=2)
        low_conn = None
        try:
            low_conn = taos.connect(user="u_mac_low", password=self.test_pass, database="d_mac2")
            low_conn.schemaless_insert(line_low, SmlProtocol.LINE_PROTOCOL, SmlPrecision.NANO_SECONDS)
            tdLog.exit("MAC schemaless failed: low-level user unexpectedly inserted into d_mac2")
        except Exception as err:
            if "security level" not in str(err).lower() and "insufficient" not in str(err).lower():
                tdLog.exit(f"Unexpected schemaless error for MAC deny: {err}")
        finally:
            if low_conn is not None:
                low_conn.close()

        # Coverage focus here is MAC denial for low-level user on schemaless path.

    def do_check_mac_cleanup(self):
        """Clean up MAC test objects"""
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop database if exists d_mac0")
        tdSql.execute("drop database if exists d_mac2")
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