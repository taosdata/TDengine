import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestUserPassword:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_user_password(self):
        """user password

        1. -

        Catalog:
            - User

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/user/password.sim

        """

        tdLog.info(f"============= step1")
        tdSql.execute(f"create user u_read pass 'tbx12F132!'")
        tdSql.execute(f"create user u_write pass 'tbx12145&*'")

        tdSql.execute(f"alter user u_read pass 'taosdata'")
        tdSql.execute(f"alter user u_write pass 'taosdata'")

        tdSql.query(f"show users")
        tdSql.checkRows(3)

        # invalid password format

        tdSql.error(f"create user user_p1 pass 'taosdata1'")
        tdSql.error(f"create user user_p1 pass 'taosdata2'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&3'")
        tdSql.error(f"create user user_p1 pass '1234564'")
        tdSql.error(f"create user user_p1 pass 'taosdataa'")
        tdSql.error(f"create user user_p1 pass 'taosdatab'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&c'")
        tdSql.error(f"create user user_p1 pass '123456d'")
        tdSql.error(f"create user user_p1 pass 'taosdataE'")
        tdSql.error(f"create user user_p1 pass 'taosdataF'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&G'")
        tdSql.error(f"create user user_p1 pass '12333315H'")
        tdSql.error(f"create user user_p1 pass 'aaaaaaaat1'")
        tdSql.error(f"create user user_p1 pass 'TTTTTTTTT2'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&!3'")
        tdSql.error(f"create user user_p1 pass '12345654'")
        tdSql.error(f"create user user_p1 pass 'taosdatata'")
        tdSql.error(f"create user user_p1 pass 'TAOSDATATb'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&!c'")
        tdSql.error(f"create user user_p1 pass '1234565d'")
        tdSql.error(f"create user user_p1 pass 'taosdatatE'")
        tdSql.error(f"create user user_p1 pass 'TAOSDATATF'")
        tdSql.error(f"create user user_p1 pass '!@#$$*!G'")
        tdSql.error(f"create user user_p1 pass '1234565H'")
        tdSql.error(f"create user user_p1 pass 'taosdataaosdata!'")
        tdSql.error(f"create user user_p1 pass 'taosdataaosdata@'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#@'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*##'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#$'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#%'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#^'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#&'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#*'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#('")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#)'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#-'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#_'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#+'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#='")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#['")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#]'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#{{'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#}}'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#:'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#;'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#>'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#<'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#?'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#|'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#~'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#,'")
        tdSql.error(f"create user user_p1 pass '!@#$%^&@*#.'")
        # tdSql.error(f"create user user_p1 pass 'tbd1234TTT'")
        tdSql.error(f"create user user_p1 pass 'tbd1234TTT/'")
        tdSql.error(f"create user user_p1 pass 'tbd1234TTT`'")
        tdSql.error(f"create user user_p1 pass 'taosdatax'")
        tdSql.error(f"create user user_p1 pass 'taosdatay'")

        tdSql.error(f"create user user_p1 pass 'abcd!@1'")
        tdSql.execute(f"create user user_p2 pass 'abcd!@12'")
        tdSql.execute(f"create user user_p3 pass 'abcd!@123'")
        tdSql.execute(f"create user user_p4 pass 'abcd!@1234'")
        tdSql.execute(f"create user user_p5 pass 'abcd!@12345'")
        tdSql.execute(f"create user user_p6 pass 'abcd!@123456'")
        tdSql.execute(f"create user user_p7 pass 'abcd!@1234567'")
        tdSql.execute(f"create user user_p8 pass 'abcd!@123456789'")
        tdSql.execute(f"create user user_p9 pass 'abcd!@1234567890'")
        tdSql.error(
            f"create user user_p10 pass 'abcd!@123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345T'"
        )
        tdSql.execute(f"drop user user_p2")
        tdSql.execute(f"drop user user_p3")
        tdSql.execute(f"drop user user_p4")
        tdSql.execute(f"drop user user_p5")
        tdSql.execute(f"drop user user_p6")
        tdSql.execute(f"drop user user_p7")
        tdSql.execute(f"drop user user_p8")
        tdSql.execute(f"drop user user_p9")

        tdSql.execute(f"create user user_p1 pass 'xt12!@cd'")

        tdSql.error(f"alter user user_p1 pass 'abcd!@1'")
        tdSql.execute(f"alter user user_p1 pass 'abcd!@12'")
        tdSql.execute(f"alter user user_p1 pass 'abcd!@123'")
        tdSql.execute(f"alter user user_p1 pass 'abcd!@1234'")
        tdSql.execute(f"alter user user_p1 pass 'abcd!@12345'")
        tdSql.execute(f"alter user user_p1 pass 'abcd!@123456'")
        tdSql.execute(f"alter user user_p1 pass 'abcd!@1234567'")
        tdSql.execute(f"alter user user_p1 pass 'abcd!@123456789'")
        tdSql.execute(f"alter user user_p1 pass 'abcd!@1234567890'")
        tdSql.error(f"user user_p1 pass 'abcd!@1234567890T'")
        tdSql.error(f"alter user user_p1 pass 'taosdata1'")
        tdSql.error(f"alter user user_p1 pass 'taosdata2'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&3'")
        tdSql.error(f"alter user user_p1 pass '1234564'")
        tdSql.error(f"alter user user_p1 pass 'taosdataa'")
        tdSql.error(f"alter user user_p1 pass 'taosdatab'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&c'")
        tdSql.error(f"alter user user_p1 pass '123456d'")
        tdSql.error(f"alter user user_p1 pass 'taosdataE'")
        tdSql.error(f"alter user user_p1 pass 'taosdataF'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&G'")
        tdSql.error(f"alter user user_p1 pass '12334515H'")
        tdSql.error(f"alter user user_p1 pass 'aasfdsft1'")
        tdSql.error(f"alter user user_p1 pass 'TAOSDATAT2'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&!3'")
        tdSql.error(f"alter user user_p1 pass '12345654'")
        tdSql.error(f"alter user user_p1 pass 'taosdatata'")
        tdSql.error(f"alter user user_p1 pass 'TAOSDATATb'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&!c'")
        tdSql.error(f"alter user user_p1 pass '1234565d'")
        tdSql.error(f"alter user user_p1 pass 'taosdatatE'")
        tdSql.error(f"alter user user_p1 pass 'TAOSDATATF'")
        tdSql.error(f"alter user user_p1 pass '*%^^%###!G'")
        tdSql.error(f"alter user user_p1 pass '1234565H'")
        tdSql.error(f"alter user user_p1 pass 'taosdataaosdata!'")
        tdSql.error(f"alter user user_p1 pass 'taosdataaosdata@'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#@'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*##'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#$'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#%'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#^'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#&'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#*'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#('")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#)'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#-'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#_'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#+'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#='")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#['")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#]'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#{{'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#}}'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#:'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#;'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#>'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#<'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#?'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#|'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#~'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#,'")
        tdSql.error(f"alter user user_p1 pass '!@#$%^&@*#.'")
        # tdSql.error(f"alter user user_p1 pass 'tbd1234TTT'")
        tdSql.error(f"alter user user_p1 pass 'tbd1234TTT/'")
        tdSql.error(f"alter user user_p1 pass 'tbd1234TTT`'")
        tdSql.error(f"alter user user_p1 pass 'taosdatax'")
        tdSql.error(f"alter user user_p1 pass 'taosdatay'")

        tdSql.execute(f"drop user user_p1")

        tdSql.execute(f"create user user_px pass 'taosdata'")
        tdSql.execute(f"drop user user_px")

        tdLog.info(f"============= step2")
        tdLog.info(f"user u_read login")
        tdSql.connect("u_read")

        tdSql.execute(f"alter user u_read pass 'taosdata'")
        tdSql.error(f"alter user u_write pass 'taosdata1'")

        tdSql.error(f"create user read1 pass 'taosdata1'")
        tdSql.error(f"create user write1 pass 'taosdata1'")

        tdSql.query(f"show users")
        tdSql.checkRows(3)

        tdLog.info(f"============= step3")
        tdLog.info(f"user u_write login")
        tdSql.connect("u_write")

        tdSql.error(f"create user read2 pass 'taosdata1'")
        tdSql.error(f"create user write2 pass 'taosdata1'")
        tdSql.execute(f"alter user u_write pass 'taosdata'")
        tdSql.error(f"alter user u_read pass 'taosdata'")

        tdSql.query(f"show users")
        tdSql.checkRows(3)

        tdLog.info(f"============= step4")
        tdLog.info(f"user root login")
        tdSql.connect("root")
        tdSql.execute(f"create user oroot pass 'taosdata'")
        tdSql.error(
            f"create user PASS 'abcd012345678901234567891234567890abcd012345678901234567891234567890abcd012345678901234567891234567890abcd012345678901234567891234567890123'"
        )
        tdSql.error(
            f"create userabcd012345678901234567891234567890abcd01234567890123456789123456789  PASS 'taosdata'"
        )
        tdSql.error(f"create user abcd0123456789012345678901234567890111 PASS '123'")
        tdSql.execute(f"create user abc01234567890123456789 PASS '123xyzYDE'")

        tdSql.query(f"show users")
        tdSql.checkRows(5)

        tdLog.info(f"============= step5")
        tdSql.execute(f"create database db vgroups 1")
        tdSql.error(f"ALTER USER o_root SYSINFO 0")
        tdSql.error(f"ALTER USER o_root SYSINFO 1")
        tdSql.error(f"ALTER USER o_root enable 0")
        tdSql.error(f"ALTER USER o_root enable 1")

        tdSql.error(f"create database db vgroups 1;")
        tdSql.error(f"GRANT read ON db.* to o_root;")
        tdSql.error(f"GRANT read ON *.* to o_root;")
        tdSql.error(f"REVOKE read ON db.* from o_root;")
        tdSql.error(f"REVOKE read ON *.* from o_root;")
        tdSql.error(f"GRANT write ON db.* to o_root;")
        tdSql.error(f"GRANT write ON *.* to o_root;")
        tdSql.error(f"REVOKE write ON db.* from o_root;")
        tdSql.error(f"REVOKE write ON *.* from o_root;")
        tdSql.error(f"REVOKE write ON *.* from o_root;")

        tdSql.error(f"GRANT all ON *.* to o_root;")
        tdSql.error(f"REVOKE all ON *.* from o_root;")
        tdSql.error(f"GRANT read,write ON *.* to o_root;")
        tdSql.error(f"REVOKE read,write ON *.* from o_root;")

        tdSql.execute(f"create user u01 pass 'taosdata1!'")
        tdSql.execute(f"create user u02 pass 'taosdata1@'")
        tdSql.execute(f"create user u03 pass 'taosdata1#'")
        # sql create user u04 pass 'taosdata1$'
        tdSql.execute(f"create user u05 pass 'taosdata1%'")
        tdSql.execute(f"create user u06 pass 'taosdata1^'")
        tdSql.execute(f"create user u07 pass 'taosdata1&'")
        tdSql.execute(f"create user u08 pass 'taosdata1*'")
        tdSql.execute(f"create user u09 pass 'taosdata1('")
        tdSql.execute(f"create user u10 pass 'taosdata1)'")
        tdSql.execute(f"create user u11 pass 'taosdata1-'")
        tdSql.execute(f"create user u12 pass 'taosdata1_'")
        tdSql.execute(f"create user u13 pass 'taosdata1+'")
        tdSql.execute(f"create user u14 pass 'taosdata1='")
        tdSql.execute(f"create user u15 pass 'taosdata1['")
        tdSql.execute(f"create user u16 pass 'taosdata1]'")
        tdSql.execute(f"create user u17 pass 'taosdata1{{'")
        tdSql.execute(f"create user u18 pass 'taosdata1}}'")
        tdSql.execute(f"create user u19 pass 'taosdata1:'")
        tdSql.execute(f"create user u20 pass 'taosdata1;'")
        tdSql.execute(f"create user u21 pass 'taosdata1>'")
        tdSql.execute(f"create user u22 pass 'taosdata1<'")
        tdSql.execute(f"create user u23 pass 'taosdata1?'")
        tdSql.execute(f"create user u24 pass 'taosdata1|'")
        tdSql.execute(f"create user u25 pass 'taosdata1~'")
        tdSql.execute(f"create user u26 pass 'taosdata1,'")
        tdSql.execute(f"create user u27 pass 'taosdata1.'")

        tdSql.execute(
            f"CREATE USER `_xTest1` PASS '2729c41a99b2c5222aa7dd9fc1ce3de7' SYSINFO 1 CREATEDB 0 IS_IMPORT 1 HOST '127.0.0.1';"
        )
        tdSql.error(
            f"CREATE USER `_xTest2` PASS '2729c41a99b2c5222aa7dd9fc1ce3de7' SYSINFO 1 CREATEDB 0 IS_IMPORT 0 HOST '127.0.0.1';"
        )
        tdSql.error(
            f"CREATE USER `_xTest3` PASS '2729c41' SYSINFO 1 CREATEDB 0 IS_IMPORT 1 HOST '127.0.0.1';"
        )
        tdSql.error(
            f"CREATE USER `_xTest4` PASS '2729c417' SYSINFO 1 CREATEDB 0 IS_IMPORT 0 HOST '127.0.0.1';"
        )
        tdSql.error(
            f"CREATE USER `_xTest5` PASS '2xF' SYSINFO 1 CREATEDB 0 IS_IMPORT 1' HOST '127.0.0.1';"
        )
        tdSql.error(
            f"CREATE USER `_xTest6` PASS '2xF' SYSINFO 1 CREATEDB 0 IS_IMPORT 0 HOST '127.0.0.1';"
        )

        tdSql.error(f"alter USER `_xTest1` PASS '2729c41a99b2c5222aa7dd9fc1ce3de7';")
        tdSql.error(f"alter USER `_xTest1` PASS '2729c417';")
        tdSql.error(f"alter USER `_xTest1` PASS '2xF';")
