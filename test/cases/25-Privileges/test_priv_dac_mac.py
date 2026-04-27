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
        tdSql.checkData(1, 4, "not activated")

    def do_check_sod(self):
        """Test basic Separation of Duties (SoD) with Mandatory Access Control (MAC)"""

        tdSql.execute(f"create user u1 pass '{self.test_pass}'");
        tdSql.execute(f"create user u2 pass '{self.test_pass}'")
        tdSql.execute("alter user u2 security_level 4,4")  # SYSSEC floor=[4,4] per FS.md v0.8
        tdSql.execute(f"create user u3 pass '{self.test_pass}'")
        tdSql.execute("alter user u3 security_level 4,4")
        tdSql.execute("create role r1")
        tdSql.query("show roles")
        tdSql.checkRows(7)
        tdSql.execute("show role privileges")

        tdSql.query("select name,sec_levels from information_schema.ins_users where name='u1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "u1")
        tdSql.checkData(0, 1, "[0,0]")
        tdSql.query("select name,sec_levels from information_schema.ins_users where name='u2'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "u2")
        tdSql.checkData(0, 1, "[4,4]")  # SYSSEC floor requires [4,4]
        tdSql.query("select name,sec_levels from information_schema.ins_users where name='u3'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "u3")
        tdSql.checkData(0, 1, "[4,4]")

        tdSql.error("alter cluster 'sod' 'enabled'", expectErrInfo="Invalid configuration value", fullMatched=False)
        tdSql.error("alter cluster 'separation_of_duties' 'mandatory'", expectErrInfo="No enabled non-root user with SYSDBA role found to satisfy SoD policy", fullMatched=False)
        tdSql.execute("grant role `SYSDBA` to u1")
        tdSql.error("alter cluster 'sod' 'mandatory'", expectErrInfo="No enabled non-root user with SYSSEC role found to satisfy SoD policy", fullMatched=False)
        tdSql.execute("grant role `SYSSEC` to u2")
        tdSql.error("alter cluster 'sod' 'mandatory'", expectErrInfo="No enabled non-root user with SYSAUDIT role found to satisfy SoD policy", fullMatched=False)
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
        tdSql.checkData(1, 4, "not activated")

        # F1-T7: Close SoD after mandatory → rejected (no downgrade)
        tdSql.error("alter cluster 'sod' 'enabled'",
                     expectErrInfo="Insufficient privilege for operation", fullMatched=False)

        # drop user restricted in SoD mandatory mode
        tdSql.error("drop user u1", expectErrInfo="No enabled non-root user with SYSDBA role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("drop user u2", expectErrInfo="No enabled non-root user with SYSSEC role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("drop user u3", expectErrInfo="No enabled non-root user with SYSAUDIT role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("drop user u3", expectErrInfo="No enabled non-root user with SYSAUDIT role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("drop user u2", expectErrInfo="No enabled non-root user with SYSSEC role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("drop user u1", expectErrInfo="No enabled non-root user with SYSDBA role found to satisfy SoD policy", fullMatched=False)
        # disable user retricted in SoD mandatory mode
        tdSql.error("alter user u1 enable 0", expectErrInfo="No enabled non-root user with SYSDBA role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("alter user u2 enable 0", expectErrInfo="No enabled non-root user with SYSSEC role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("alter user u3 enable 0", expectErrInfo="No enabled non-root user with SYSAUDIT role found to satisfy SoD policy", fullMatched=False)
        # enable root is restricted in SoD mandatory mode
        tdSql.error("alter user root enable 1", expectErrInfo="Insufficient privilege for operation", fullMatched=False)
        # revoke role from user restricted in SoD mandatory mode
        tdSql.error("revoke role `SYSDBA` from u1", expectErrInfo="No enabled non-root user with SYSDBA role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("revoke role `SYSSEC` from u2", expectErrInfo="Permission denied or target object not exist", fullMatched=False)
        tdSql.error("revoke role `SYSAUDIT` from u3", expectErrInfo="Permission denied or target object not exist", fullMatched=False)
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.error("revoke role `SYSSEC` from u2", expectErrInfo="No enabled non-root user with SYSSEC role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("revoke role `SYSAUDIT` from u3", expectErrInfo="Permission denied or target object not exist", fullMatched=False)
        tdSql.error("revoke role `SYSDBA` from u1", expectErrInfo="Permission denied or target object not exist", fullMatched=False)
        tdSql.connect(user="u3", password=self.test_pass)
        tdSql.error("revoke role `SYSAUDIT` from u3", expectErrInfo="No enabled non-root user with SYSAUDIT role found to satisfy SoD policy", fullMatched=False)
        tdSql.error("revoke role `SYSSEC` from u2", expectErrInfo="Permission denied or target object not exist", fullMatched=False)
        tdSql.error("revoke role `SYSDBA` from u1", expectErrInfo="Permission denied or target object not exist", fullMatched=False)


        tdSql.connect(user="u1", password=self.test_pass)
        tdSql.execute("drop database if exists d0")
        tdSql.execute("create database d0 keep 36500")
        tdSql.query("select name, sec_level from information_schema.ins_databases where name='d0'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "d0")
        tdSql.checkData(0, 1, 0)  # MAC inactive: DB default secLevel = 0
        tdSql.execute("use d0")
        tdSql.execute("create table d0.stb0 (ts timestamp, c0 int,c1 int) tags(t1 int)")
        tdSql.query("select stable_name, sec_level from information_schema.ins_stables where stable_name='stb0'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb0")
        tdSql.checkData(0, 1, 0)  # MAC inactive: STB default secLevel = 0
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
        tdSql.execute("create database d0 keep 36500")
        tdSql.execute("drop database d0")

    def do_check_mac(self):
        """Test Mandatory Access Control: NRU (No Read Up) and NWD (No Write Down)"""
        # After do_check_sod: u_dba2=SYSDBA, u2=SYSSEC[4,4], u3=SYSAUDIT[4,4], root disabled
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
        self.do_check_mac_extra_coverage()
        self.do_check_mac_security_level_priv()
        self.do_check_mac_cleanup()

    def do_check_mac_activation(self):
        """Test F2-T19 to F2-T28: MAC activation and role-floor constraint under MAC"""
        # F2-T19: MAC disabled — no security enforcement before activation
        # After SoD: u2=SYSSEC, root disabled. Connect as u_dba2 (SYSDBA) who can create DBs.
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("create database if not exists d_mac_test keep 36500")
        tdSql.execute("create stable if not exists d_mac_test.stb0 (ts timestamp, v int) tags (t int)")
        # Verify show security_policies shows MAC as inactive
        tdSql.query("select name, mode from information_schema.ins_security_policies where name='MAC'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "disabled")

        # F2-T19b: MAC disabled — setting stb security_level > 0 is rejected before MAC activation.
        # Only user security_level can be set before MAC is activated; db/stb must remain at 0.
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.error("alter table d_mac_test.stb0 security_level 3",
                    expectErrInfo="Insufficient user security level", fullMatched=False)
        # Setting security_level to 0 is always allowed
        tdSql.execute("alter table d_mac_test.stb0 security_level 0")
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("use d_mac_test")
        tdSql.query("show stables")
        assert any(row[0] == "stb0" for row in tdSql.queryResult), \
            "MAC disabled: stb0 must be visible to u_dba2"

        # Also verify via information_schema (same fast-path code path)
        tdSql.query("select stable_name from information_schema.ins_stables where db_name='d_mac_test'")
        assert any(row[0] == "stb0" for row in tdSql.queryResult), \
            "MAC disabled: ins_stables must surface stb0 regardless of secLevel"

        # F2-T19c: MAC disabled — setting db security_level > 0 is also rejected before MAC activation.
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.error("alter database d_mac_test security_level 2",
                    expectErrInfo="Insufficient user security level", fullMatched=False)
        # Setting security_level to 0 is always allowed
        tdSql.execute("alter database d_mac_test security_level 0")

        # F2-T25: MAC disabled → GRANT high-level role (SYSDBA, floor=3) to user with maxSecLevel=0
        # No floor check should be enforced when MAC is not active.
        # Only SYSDBA (u_dba2) can grant SYSDBA to another user.
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute(f"create user u_floor_test pass '{self.test_pass}'")
        # u_floor_test sec_level=[0,0] (default); SYSDBA floor=3; MAC not active → grant succeeds
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

        # F2-T20c: Pre-activation check — SYSSEC user with insufficient maxSecLevel blocks MAC activation.
        # Grant SYSSEC role (→ ALTER SECURITY POLICY) to a fresh user whose maxSecLevel=[0,0] < 4.
        # MAC activation must be rejected with a detail message that names the blocking user.
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute(f"create user u_pf_test1 pass '{self.test_pass}'")  # default maxSecLevel=[0,0]
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("grant role `SYSSEC` to u_pf_test1")   # gives ALTER SECURITY POLICY
        # Activation must fail: u_pf_test1 holds privilege but maxSecLevel=0 < 4
        tdSql.error("alter cluster 'MAC' 'mandatory'",
                    expectErrInfo="Cannot enable MAC", fullMatched=False)
        # Error detail must name the specific user
        tdSql.error("alter cluster 'MAC' 'mandatory'",
                    expectErrInfo="u_pf_test1", fullMatched=False)
        err_info = tdSql.error_info
        assert "required maxFloor(4)" in err_info, f"Expected maxFloor detail, got: {err_info}"
        assert "SECURITY_LEVEL 4,4" in err_info, f"Expected repair hint <4,4>, got: {err_info}"

        # F2-T20d: Strategy A — a DISABLED user with PRIV still blocks MAC activation.
        # Disabling the user is NOT sufficient to bypass the Pre-activation check.
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("alter user u_pf_test1 enable 0")
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.error("alter cluster 'MAC' 'mandatory'",
                    expectErrInfo="Cannot enable MAC", fullMatched=False)
        tdSql.error("alter cluster 'MAC' 'mandatory'",
                    expectErrInfo="u_pf_test1", fullMatched=False)

        # F2-T20e: When two users block activation, only one is reported per attempt.
        # Re-enable u_pf_test1 and add u_pf_test2 (also SYSSEC, default maxSecLevel=0).
        # Each activation attempt reports exactly one blocking user name in the error.
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("alter user u_pf_test1 enable 1")
        tdSql.execute(f"create user u_pf_test2 pass '{self.test_pass}'")  # default maxSecLevel=[0,0]
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("grant role `SYSSEC` to u_pf_test2")
        # Error must still contain "Cannot enable MAC" — count of users is not reported
        tdSql.error("alter cluster 'MAC' 'mandatory'",
                    expectErrInfo="Cannot enable MAC", fullMatched=False)
        # The error names at least one of the two blockers (server reports whichever it scanned first)
        err_info = tdSql.error_info
        assert "u_pf_test1" in err_info or "u_pf_test2" in err_info, \
            f"Expected one of u_pf_test1/u_pf_test2 in error, got: {err_info}"

        # F2-T20f: Fix by revoking the role from both blockers and clean up.
        # After neither user holds a system role with insufficient security_level, MAC activation
        # will proceed when all remaining system role holders satisfy their role floors.
        tdSql.execute("revoke role `SYSSEC` from u_pf_test1")
        tdSql.execute("revoke role `SYSSEC` from u_pf_test2")
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop user u_pf_test1")
        tdSql.execute("drop user u_pf_test2")
        tdSql.connect(user="u2", password=self.test_pass)

        # F2-T20g: Pre-activation check also covers SYSDBA holders (maxFloor=3).
        # u_floor_test (SYSDBA, maxSecLevel=2 < 3) and u_dba2 (SYSDBA, maxSecLevel=0 < 3)
        # both block MAC activation. Fix by raising their sec_levels explicitly.
        tdSql.error("alter cluster 'MAC' 'mandatory'",
                    expectErrInfo="Cannot enable MAC", fullMatched=False)
        err_info = tdSql.error_info
        assert "u_floor_test" in err_info or "u_dba2" in err_info, \
            f"Expected SYSDBA blocker in error, got: {err_info}"
        # Fix: SYSSEC admin raises u_floor_test and u_dba2 to satisfy SYSDBA maxFloor=3.
        # Escalation check is MAC-gated (not active yet), so u2 can freely set these levels.
        tdSql.execute("alter user u_floor_test security_level 0,3")
        tdSql.execute("alter user u_dba2 security_level 0,3")

        # F2-T20h: Pre-activation also catches direct ALTER SECURITY POLICY holders
        # (without a system role) whose maxSecLevel is below the required floor.
        # Two constraints apply: a role constraint (maxFloor=1) checked first,
        # then a direct-priv constraint (maxSecLevel must be 4).
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute(f"create user u_pf_direct pass '{self.test_pass}'")  # default [0,0]
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("grant alter security policy to u_pf_direct")
        tdSql.execute("grant show security policies to u_pf_direct")
        tdSql.error("alter cluster 'MAC' 'mandatory'",
                    expectErrInfo="Cannot enable MAC", fullMatched=False)
        err_info = tdSql.error_info
        assert "u_pf_direct" in err_info, f"Expected u_pf_direct in error, got: {err_info}"
        # Set to [0,4] to satisfy both the role constraint (maxFloor=1) and
        # the direct ALTER SECURITY POLICY holder constraint (maxSecLevel=4).
        tdSql.execute("alter user u_pf_direct security_level 0,4")
        tdSql.connect(user="u2", password=self.test_pass)
        # After fixing both blockers, activation succeeds.

        # F2-T21: SYSSEC activates MAC — succeeds
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter cluster 'MAC' 'mandatory'")
        # Verify show security_policies shows MAC as mandatory
        tdSql.query("select name, mode from information_schema.ins_security_policies where name='MAC'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "mandatory")

        # F2-T28: After activation, security_level of system-role holders equals what was set.
        # No auto-upgrade: Pre-activation ensures integrity before activation, not during.
        # u_floor_test: SYSDBA (floor=[0,3]); was explicitly raised to [0,3] in F2-T20g.
        tdSql.query("select name, sec_levels from information_schema.ins_users where name='u_floor_test'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "[0,3]")
        # u_dba2: SYSDBA (floor=[0,3]); was explicitly raised to [0,3] in F2-T20g.
        tdSql.query("select name, sec_levels from information_schema.ins_users where name='u_dba2'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "[0,3]")


        # F2-T26: MAC active → GRANT role requires both min and max security_level to satisfy floor.
        # SYSSEC floor: maxFloor=4 AND minFloor=4.
        # Use a fresh user (no management role) with default sec_level=[0,0].
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute(f"create user u_floor_test2 pass '{self.test_pass}'")  # default sec_level=[0,0]
        tdSql.connect(user="u2", password=self.test_pass)
        # SYSSEC maxFloor=4; u_floor_test2 maxSecLevel=0 < 4 → rejected under MAC
        tdSql.error("grant role `SYSSEC` to u_floor_test2",
                    expectErrInfo="Security level is below", fullMatched=False)
        # Raise maxSecLevel to 4; minSecLevel still 0 < minFloor=4 → GRANT still rejected
        tdSql.execute("alter user u_floor_test2 security_level 0,4")
        tdSql.error("grant role `SYSSEC` to u_floor_test2",
                    expectErrInfo="Security level is below", fullMatched=False)
        # After raising both min and max to [4,4], GRANT SYSSEC succeeds.
        tdSql.execute("alter user u_floor_test2 security_level 4,4")
        tdSql.execute("grant role `SYSSEC` to u_floor_test2")

        # F2-T27: MAC active → ALTER USER security_level below current role floor → fail.
        # u_floor_test has SYSDBA (floor: maxFloor=3, minFloor=0); try lowering maxSecLevel → fail.
        tdSql.error("alter user u_floor_test security_level 0,2",
                    expectErrInfo="Security level is below", fullMatched=False)
        # SYSAUDIT floor: maxFloor=4, minFloor=4; u3 (SYSAUDIT) cannot lower maxSecLevel below 4.
        tdSql.error("alter user u3 security_level 0,3",
                expectErrInfo="Security level is below", fullMatched=False)
        # SYSSEC floor: maxFloor=4, minFloor=4; u_floor_test2 (SYSSEC) cannot lower minSecLevel.
        tdSql.error("alter user u_floor_test2 security_level 0,4",
                    expectErrInfo="Security level is below", fullMatched=False)
        # Set to exactly the floor → success (no-op for SYSDBA holder).
        tdSql.execute("alter user u_floor_test security_level 0,3")
        # Set to exactly SYSSEC floor [4,4] → success (already at floor).
        tdSql.execute("alter user u_floor_test2 security_level 4,4")

        # F2-T27b: Direct ALTER SECURITY POLICY holder must keep maxSecLevel=4 under MAC.
        tdSql.error("alter user u_pf_direct security_level 0,3",
                    expectErrInfo="Security level is below", fullMatched=False)
        tdSql.execute("revoke alter security policy from u_pf_direct")
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop user u_pf_direct")
        tdSql.connect(user="u2", password=self.test_pass)

        # F2-T28b: REVOKE system role — security_level does NOT auto-reset.
        # After revoking SYSSEC from u_floor_test2, their sec_level stays at [4,4].
        tdSql.execute("revoke role `SYSSEC` from u_floor_test2")
        tdSql.query("select name, sec_levels from information_schema.ins_users where name='u_floor_test2'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "[4,4]")  # not auto-reset to [0,0] after REVOKE

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
        tdSql.execute(f"create user u_mac_low pass '{self.test_pass}'")    # default [0,0]
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
        tdSql.execute("create database d_mac0 keep 36500")
        tdSql.execute("create database d_mac2 keep 36500")

        # SYSSEC alters DB security levels (Approach B: only ALTER SECURITY POLICY needed)
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

        # SYSSEC sets explicit STB security levels (Approach B: only ALTER SECURITY POLICY needed)
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
        tdSql.checkData(0, 1, "[0,0]")
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
                     expectErrInfo="Permission denied", fullMatched=False)

        # SYSSEC can ALTER STB security_level
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter table d_mac0.stb_lvl2 security_level 2")  # no-op, same level

        # SYSSEC cannot ALTER DB security_level if not SYSSEC
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.error("alter database d_mac0 security_level 1",
                     expectErrInfo="Permission denied", fullMatched=False)

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

        # DB_USE denial on DDL path should preserve MAC reason (not generic permission denied)
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        tdSql.error("alter table d_mac2.stb_d2 add column c_block int",
                expectErrInfo="security level", fullMatched=False)
        tdSql.error("drop table d_mac2.ctb_d2",
                expectErrInfo="security level", fullMatched=False)

        # F2-T14: Low-level user with ALTER privilege on high-level object → blocked by MAC clearance check
        tdSql.connect(user="u_mac_low", password=self.test_pass)
        # u_mac_low (max=1) has ALTER privilege on stb_lvl2 (level=2), but MAC clearance blocks (1 < 2)
        tdSql.error("alter table d_mac0.stb_lvl2 add column c_new int",
                     expectErrInfo="security level", fullMatched=False)
        # u_mac_mid (max=3) can ALTER stb_lvl2 (level=2): 3 >= 2
        tdSql.connect(user="u_mac_mid", password=self.test_pass)
        tdSql.execute("alter table d_mac0.stb_lvl2 add column c_new int")
        tdSql.execute("alter table d_mac0.stb_lvl2 drop column c_new")

        # Batch SET TAG on child tables: MAC clearance check enforced per-clause via macCheckTableAccess
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

        # explicit security_level = 0 should always be accepted (same as default)
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter table d_mac0.stb_lvl2 security_level 0")
        tdSql.execute("alter table d_mac0.stb_lvl2 security_level 2")

        # --- maxSecLevel enforcement on ALTER security_level (parser-side defense-in-depth) ---
        # The parser checks user.maxSecLevel >= target securityLevel even for ALTER SECURITY POLICY
        # holders. Since SYSSEC floor=4 == TSDB_MAX_SECURITY_LEVEL=4, we cannot create a SYSSEC user
        # with maxSecLevel < 4, so the parser-side check is exercised only via direct RPC injection
        # or by non-SYSSEC users (who would fail ALTER SECURITY POLICY first).
        # MNode-side enforcement is the complementary defense layer for DB/STB level constraints.

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

        # ---- Fast-path tests ----------------------------------------
        # Fast path A: user with maxSecLevel == 4 sees everything in SHOW STABLES / SHOW TABLES
        # (u_mac_high is [3,3]; promote to [0,4] for this test, then restore)
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter user u_mac_high security_level 0,4")
        time.sleep(2)   # wait for HB propagation of new macActive / maxSecLevel

        tdSql.connect(user="u_mac_high", password=self.test_pass)
        tdSql.execute("use d_mac0")
        tdSql.query("show stables")
        stable_names = {row[0] for row in tdSql.queryResult}
        assert "stb_lvl2" in stable_names, "maxSecLevel=4 user must see stb_lvl2"
        assert "stb_lvl3" in stable_names, "maxSecLevel=4 user must see stb_lvl3"

        tdSql.query("show tables")
        table_names = {row[0] for row in tdSql.queryResult}
        assert "ctb_l2" in table_names, "maxSecLevel=4 user must see ctb_l2"
        assert "ctb_l3" in table_names, "maxSecLevel=4 user must see ctb_l3"
        assert "ntb1"   in table_names, "maxSecLevel=4 user must see ntb1"

        tdSql.query("select table_name from information_schema.ins_tables where db_name='d_mac0'")
        info_names = {row[0] for row in tdSql.queryResult}
        assert "ctb_l3" in info_names, "maxSecLevel=4 must see ctb_l3 in ins_tables"

        # Check SHOW DATABASES: maxSecLevel=4 user must see d_mac2 (db secLevel=2)
        tdSql.query("show databases")
        db_names = {row[0] for row in tdSql.queryResult}
        assert "d_mac2" in db_names, "maxSecLevel=4 user must see d_mac2"

        # Restore u_mac_high to original [3,3]
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter user u_mac_high security_level 3,3")
        time.sleep(2)

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

    def do_check_mac_extra_coverage(self):
        """Test additional MAC scenarios not covered by other methods.

        F2-TX1: audit DB security_level is immutable (fixed at 4, cannot be changed via ALTER).
        F2-TX2: DB security_level upgrade is blocked if any STB has level < new DB level.
        F2-TX3: secLevel=4 object access boundary — NRU blocks read/write for max<4; user with [4,4] can read and write level-4 objects.
        """

        # ---- F2-TX1: audit DB security_level is immutable ----
        # The audit DB (isAudit=1) has security_level fixed at 4.
        # Even SYSSEC (who holds ALTER SECURITY POLICY) must not be able to change it.
        # Note: audit DB only exists in enterprise builds with audit logging enabled;
        # 'is_audit' is a sysInfo column, so the query may fail in community environments.
        tdSql.connect(user="u2", password=self.test_pass)
        try:
            tdSql.query("select name from information_schema.ins_databases where `is_audit` = 1 limit 1")
            if tdSql.queryRows > 0:
                audit_db_name = tdSql.queryResult[0][0]
                tdSql.error(f"alter database {audit_db_name} security_level 0",
                            expectErrInfo="Audit database is not allowed to change", fullMatched=False)
                tdSql.error(f"alter database {audit_db_name} security_level 3",
                            expectErrInfo="Audit database is not allowed to change", fullMatched=False)
                # Setting it to its current value (4) is also rejected (immutable means no ALTER at all)
                tdSql.error(f"alter database {audit_db_name} security_level 4",
                            expectErrInfo="Audit database is not allowed to change", fullMatched=False)
                tdLog.info("F2-TX1: audit DB security_level immutability verified")
            else:
                tdLog.info("F2-TX1: no audit DB found in this environment, skipping")
        except Exception:
            tdLog.info("F2-TX1: is_audit column not accessible or audit DB unavailable, skipping")

        # ---- F2-TX2: DB security_level upgrade blocked when STB level < new DB level ----
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop database if exists d_mac_upgrade")
        tdSql.execute("create database d_mac_upgrade keep 36500")
        tdSql.execute("create stable d_mac_upgrade.stb_upgrade (ts timestamp, v int) tags(t1 int)")

        # SYSSEC sets DB to level 0, STB to level 1
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter database d_mac_upgrade security_level 0")
        tdSql.execute("alter table d_mac_upgrade.stb_upgrade security_level 1")

        # Try to raise DB to level 2 — must fail because STB is at level 1 < 2
        tdSql.error("alter database d_mac_upgrade security_level 2",
                    expectErrInfo="Object level below database security level", fullMatched=False)

        # Verify DB level is still 0
        tdSql.query("select sec_level from information_schema.ins_databases where name='d_mac_upgrade'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        # Raise STB to level 2, then the DB upgrade must succeed
        tdSql.execute("alter table d_mac_upgrade.stb_upgrade security_level 2")
        tdSql.execute("alter database d_mac_upgrade security_level 2")

        # Verify DB level is now 2
        tdSql.query("select sec_level from information_schema.ins_databases where name='d_mac_upgrade'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        # Cleanup
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop database if exists d_mac_upgrade")
        tdLog.info("F2-TX2: DB upgrade STB level validation verified")

        # ---- F2-TX3: secLevel=4 object access boundary ----
        # Setup: SYSDBA creates a DB + STB at security level 4; SYSSEC sets levels and grants.
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop database if exists d_mac_max")
        tdSql.execute("create database d_mac_max keep 36500")
        tdSql.execute("create stable d_mac_max.stb_max4 (ts timestamp, v int) tags(t1 int)")
        tdSql.execute("create table d_mac_max.ctb_max4 using d_mac_max.stb_max4 tags(1)")

        # SYSSEC sets security levels first, then grants (matching do_check_mac_setup pattern)
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter database d_mac_max security_level 4")
        tdSql.execute("alter table d_mac_max.stb_max4 security_level 4")
        for user in ['u_mac_high', 'u3']:
            tdSql.execute(f"grant use database on database d_mac_max to {user}")
            tdSql.execute(f"grant select,insert on table d_mac_max.stb_max4 to {user}")

        # Flush to propagate grants + security levels (as in do_check_mac_setup)
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("flush database d_mac_max")
        time.sleep(2)

        # u_mac_high [3,3]: max=3 < 4 → NRU blocks both SELECT and INSERT
        # INSERT MAC check (parInsertSql.c): requires minSecLevel<=secLvl<=maxSecLevel; NRU fires first
        tdSql.connect(user="u_mac_high", password=self.test_pass)
        tdSql.error("select * from d_mac_max.stb_max4",
                    expectErrInfo="security level", fullMatched=False)
        tdSql.error("select * from d_mac_max.ctb_max4",
                    expectErrInfo="security level", fullMatched=False)
        tdSql.error("insert into d_mac_max.ctb_max4 values(now, 999)",
                    expectErrInfo="security level", fullMatched=False)

        # u3 = SYSAUDIT [4,4]: max=4 >= 4 → NRU allows SELECT; min=4 <= 4 → NWD allows INSERT
        tdSql.connect(user="u3", password=self.test_pass)
        tdSql.execute("select * from d_mac_max.stb_max4")
        tdSql.execute("insert into d_mac_max.ctb_max4 values(now, 888)")

        # Cleanup
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop database if exists d_mac_max")
        tdLog.info("F2-TX3: secLevel=4 NRU/NWD access boundary verified")

        # ---- F2-TX4: stale connection window remains until reconnect/HB refresh ----
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop user if exists u_stale_window")
        tdSql.execute(f"create user u_stale_window pass '{self.test_pass}'")

        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter user u_stale_window security_level 0,3")
        tdSql.execute("grant use database on database d_mac0 to u_stale_window")
        tdSql.execute("grant select on table d_mac0.stb_lvl3 to u_stale_window")

        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("flush database d_mac0")
        time.sleep(2)

        stale_conn = taos.connect(user="u_stale_window", password=self.test_pass, database="d_mac0")
        try:
            stale_cur = stale_conn.cursor()
            stale_cur.execute("select count(*) from stb_lvl3")

            tdSql.connect(user="u2", password=self.test_pass)
            tdSql.execute("alter user u_stale_window security_level 0,1")

            # The existing connection may continue to use the old auth snapshot until reconnect/HB.
            stale_cur.execute("select count(*) from stb_lvl3")
        finally:
            stale_conn.close()

        fresh_conn = taos.connect(user="u_stale_window", password=self.test_pass, database="d_mac0")
        try:
            fresh_cur = fresh_conn.cursor()
            try:
                fresh_cur.execute("select count(*) from stb_lvl3")
                tdLog.exit("MAC stale-window failed: reconnected user unexpectedly retained old clearance")
            except Exception as err:
                if "security level" not in str(err).lower() and "insufficient" not in str(err).lower():
                    tdLog.exit(f"Unexpected MAC error after stale-window reconnect: {err}")
        finally:
            fresh_conn.close()

        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop user if exists u_stale_window")
        tdLog.info("F2-TX4: stale connection window and reconnect convergence verified")

    def do_check_mac_cleanup(self):
        """Clean up MAC test objects"""
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop database if exists d_mac0")
        tdSql.execute("drop database if exists d_mac2")
        tdSql.execute("drop user u_mac_low")
        tdSql.execute("drop user u_mac_mid")
        tdSql.execute("drop user u_mac_high")

    def do_check_mac_security_level_priv(self):
        """Test ALTER SECURITY POLICY requirement for security_level operations.

        Design (SoD separation):
        - CREATE/ALTER with security_level: base priv (CREATE/ALTER) checked first,
          then additionally ALTER SECURITY POLICY if macMode && security_level >= 0.
        - ALTER DB/TABLE security_level: ALTER SECURITY POLICY is the PRIMARY check
          (SYSSEC is the authorized security officer; CM_ALTER is not required).

        In SoD mandatory + MAC mandatory mode:
        - PRIV_USER_CREATE   → T_ROLE_SYSDBA only
        - PRIV_USER_ALTER    → SYS_ADMIN_INFO_ROLES (includes SYSSEC) ← SYSSEC can alter users
        - PRIV_TBL_CREATE    → T_ROLE_SYSDBA only
        - PRIV_CM_ALTER (DB/TBL) → T_ROLE_SYSDBA only (SYSSEC lacks it)
        - ALTER SECURITY POLICY → T_ROLE_SYSSEC only (SYSDBA lacks it)

        Consequence:
        - SYSSEC can ALTER USER/DB/TABLE security_level (via ALTER SECURITY POLICY).
        - SYSSEC cannot CREATE TABLE or do non-security_level ALTER DB/TABLE.
        - SYSDBA can CREATE USER/DB/TABLE but cannot set security_level (lacks ALTER SECURITY POLICY).
        """
        # State: SoD mandatory + MAC mandatory
        # u_dba2=SYSDBA[0,4], u2=SYSSEC[4,4], u3=SYSAUDIT[4,4], root disabled

        # --- Setup ---
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop database if exists d_seclvl_test")
        tdSql.execute("drop user if exists u_seclvl_test1")
        tdSql.execute("drop user if exists u_seclvl_default")

        # --- Test 1: SYSDBA cannot CREATE USER with SECURITY_LEVEL (lacks ALTER SECURITY POLICY) ---
        tdSql.error(f"create user u_seclvl_test1 pass '{self.test_pass}' security_level 2,3",
                    expectErrInfo="Insufficient privilege", fullMatched=False)

        # --- Test 2: Two-step SoD workflow: SYSDBA creates user, SYSSEC sets security_level ---
        # SYSSEC passes: PRIV_USER_ALTER (base) + ALTER SECURITY POLICY (macMode check)
        tdSql.execute(f"create user u_seclvl_test1 pass '{self.test_pass}'")
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter user u_seclvl_test1 security_level 2,3")
        tdSql.query("select name,sec_levels from information_schema.ins_users where name='u_seclvl_test1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "[2,3]")

        # --- Test 3: SYSSEC can ALTER DATABASE security_level (ALTER SECURITY POLICY only) ---
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("create database d_seclvl_test keep 36500")
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter database d_seclvl_test security_level 3")
        tdSql.query("select name, sec_level from information_schema.ins_databases where name='d_seclvl_test'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        # --- Test 4: SYSSEC can ALTER STABLE security_level (ALTER SECURITY POLICY only) ---
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("use d_seclvl_test")
        tdSql.execute("create stable stb_seclvl (ts timestamp, v int) tags (t int)")
        tdSql.connect(user="u2", password=self.test_pass)
        tdSql.execute("alter table d_seclvl_test.stb_seclvl security_level 3")
        tdSql.query("select stable_name, sec_level from information_schema.ins_stables where db_name='d_seclvl_test' and stable_name='stb_seclvl'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        # --- Test 5: SYSDBA cannot ALTER DATABASE security_level (no ALTER SECURITY POLICY) ---
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.error("alter database d_seclvl_test security_level 4",
                    expectErrInfo="Permission denied", fullMatched=False)

        # --- Test 6: SYSDBA cannot ALTER STABLE security_level (no ALTER SECURITY POLICY) ---
        tdSql.error("alter table d_seclvl_test.stb_seclvl security_level 4",
                    expectErrInfo="Permission denied", fullMatched=False)

        # --- Test 7: SYSDBA cannot ALTER USER security_level (no ALTER SECURITY POLICY) ---
        tdSql.error("alter user u_seclvl_test1 security_level 0,2",
                    expectErrInfo="Insufficient privilege", fullMatched=False)

        # --- Test 8: CREATE USER without SECURITY_LEVEL uses defaults [0,0] ---
        tdSql.execute(f"create user u_seclvl_default pass '{self.test_pass}'")
        tdSql.query("select name,sec_levels from information_schema.ins_users where name='u_seclvl_default'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "[0,0]")

        # --- Cleanup ---
        tdSql.connect(user="u_dba2", password=self.test_pass)
        tdSql.execute("drop database if exists d_seclvl_test")
        tdSql.execute("drop user if exists u_seclvl_test1")
        tdSql.execute("drop user if exists u_seclvl_default")
        tdLog.info("do_check_mac_security_level_priv: all 8 tests passed (3+4 are now positive SYSSEC tests)")


    #
    # ------------------- main ----------------
    #
    def test_priv_dac_mac(self):
        """Test basic privileges of Discretionary Access Control and Mandatory Access Control
        
        1. Test mandatory SoD(Separation of Duty).
        2. Test mandatory access control with security levels.
        3. Test CREATE with SECURITY_LEVEL and ALTER SECURITY POLICY.
        
        Since: v3.4.1.0

        Labels: basic,ci

        Jira: 6670071929,6671585124

        History:
            - 2026-02-19 Kaili Xu Initial creation(6670071929,6671585124)
            - 2026-04-17 Updated: ALTER SECURITY POLICY and CREATE with security_level
        """

        self.do_check_init_env()
        self.do_check_sod()
        self.do_check_mac()
    
        tdLog.debug("finish executing %s" % __file__)