from new_test_framework.utils import tdLog, tdSql, epath, sc
import time
import os


class TestStrongPassword:
    
    def setup_class(cls):
        cls.checkColName="c1"

    def test_strong_password(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        # strong
        tdSql.error("create user test pass '12345678' sysinfo 0;", expectErrInfo="Invalid password", fullMatched=False)

        tdSql.execute("create user test pass '12345678@Abc' sysinfo 0;")

        tdSql.error("alter user test pass '23456789'", expectErrInfo="Invalid password", fullMatched=False)

        tdSql.execute("alter user test pass '23456789@Abc';")

        # change setting
        tdSql.execute("ALTER ALL DNODES 'enableStrongPassword' '0'")

        time.sleep(3)

        # weak
        tdSql.execute("create user test1 pass '12345678' sysinfo 0;")

        tdSql.execute("alter user test1 pass '12345678';")

        # pass length    
        tdSql.error("alter user test1 pass '1234567';", expectErrInfo="Password too short or empty")
        
        tdSql.error("create user test2 pass '1234567' sysinfo 0;", expectErrInfo="Password too short or empty")

        tdSql.error("create user test2 pass '1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456' sysinfo 0;", expectErrInfo="Name or password too long", fullMatched=False)

        tdSql.execute("create user test2 pass '123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345' sysinfo 0;")

        cmd = "taos -u test2 -p123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345 -s 'show databases;'"
        if os.system(cmd) != 0:
            raise Exception("failed to execute system command. cmd: %s" % cmd)

        tdSql.error("alter user test2 pass '1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456';", expectErrInfo="Name or password too long", fullMatched=False)      
        tdLog.success(f"{__file__} successfully executed")


