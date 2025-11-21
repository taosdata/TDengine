###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql
import os
import time


class TestCreateStbKeep:

    def prepare_database(self):
        tdLog.info(f"prepare database")
        tdSql.execute("DROP DATABASE IF EXISTS test")
        tdSql.execute("CREATE DATABASE IF NOT EXISTS test")
        tdSql.execute("USE test")


    def check_create_stb_with_keep(self):
        tdLog.info(f"check create stb with keep")
        tdSql.execute("USE test")
        tdSql.execute(f"CREATE STABLE IF NOT EXISTS stb_0 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 1d")
        tdSql.execute(f"CREATE STABLE IF NOT EXISTS stb_1 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 1440m")
        tdSql.execute(f"CREATE STABLE IF NOT EXISTS stb_2 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 24h")
        tdSql.execute(f"CREATE STABLE IF NOT EXISTS stb_3 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 7d")
        tdSql.execute(f"CREATE STABLE IF NOT EXISTS stb_4 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 30d")
        tdSql.execute(f"CREATE STABLE IF NOT EXISTS stb_5 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 365")
        tdSql.error(f"CREATE STABLE IF NOT EXISTS stb_6 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 365000d",expectErrInfo="Invalid option keep value", fullMatched=False)

    def check_create_stb_with_err_keep_duration(self):
        tdLog.info(f"check create stb with err keep duration")
        tdSql.execute("USE test")
        tdSql.error(f"CREATE STABLE IF NOT EXISTS stb_7 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 0d",expectErrInfo="Invalid option keep value", fullMatched=False)
        tdSql.error(f"CREATE STABLE IF NOT EXISTS stb_8 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP -1d",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error(f"CREATE STABLE IF NOT EXISTS stb_9 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP -1",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error(f"CREATE STABLE IF NOT EXISTS stb_10 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 1m",expectErrInfo="Invalid option keep value", fullMatched=False)
        tdSql.error(f"CREATE STABLE IF NOT EXISTS stb_11 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 1h",expectErrInfo="Invalid option keep value", fullMatched=False)
        tdSql.error(f"CREATE STABLE IF NOT EXISTS stb_12 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 365001d",expectErrInfo="Invalid option keep value", fullMatched=False)
        tdSql.error(f"CREATE STABLE IF NOT EXISTS stb_13 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 365001",expectErrInfo="Invalid option keep value", fullMatched=False)
        tdSql.error(f"CREATE STABLE IF NOT EXISTS stb_14 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 1f",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error(f"CREATE STABLE IF NOT EXISTS stb_15 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 1d1",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error(f"CREATE STABLE IF NOT EXISTS stb_16 (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 21474836479999",expectErrInfo="Invalid option keep value", fullMatched=False)

    def check_alter_stb_with_keep(self):
        tdLog.info(f"check alter stb with keep")
        tdSql.execute("USE test")
        tdSql.execute(f"ALTER STABLE stb_0 KEEP 1440m")
        tdSql.execute(f"ALTER STABLE stb_0 KEEP 24h")
        tdSql.execute(f"ALTER STABLE stb_0 KEEP 7d")
        tdSql.execute(f"ALTER STABLE stb_0 KEEP 30d")
        tdSql.execute(f"ALTER STABLE stb_0 KEEP 365")
        tdSql.error(f"ALTER STABLE stb_0 KEEP 365000d",expectErrInfo="Invalid option keep value", fullMatched=False)

    def check_alter_stb_with_keep_err(self):
        tdLog.info(f"check alter stb with keep err")
        tdSql.execute("USE test")
        tdSql.error(f"ALTER STABLE stb_0 KEEP 0d",expectErrInfo="Invalid option keep value", fullMatched=False)
        tdSql.error(f"ALTER STABLE stb_0 KEEP -1d",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error(f"ALTER STABLE stb_0 KEEP -1",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error(f"ALTER STABLE stb_0 KEEP 1m",expectErrInfo="Invalid option keep value", fullMatched=False)
        tdSql.error(f"ALTER STABLE stb_0 KEEP 1h",expectErrInfo="Invalid option keep value", fullMatched=False)
        tdSql.error(f"ALTER STABLE stb_0 KEEP 365001d",expectErrInfo="Invalid option keep value", fullMatched=False)
        tdSql.error(f"ALTER STABLE stb_0 KEEP 365001",expectErrInfo="Invalid option keep value", fullMatched=False)
        tdSql.error(f"ALTER STABLE stb_0 KEEP 1f",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error(f"ALTER STABLE stb_0 KEEP 1d1",expectErrInfo="syntax error", fullMatched=False)

    def check_child_table_with_keep(self):
        tdLog.info(f"check child table with keep")
        tdSql.execute("USE test")
        tdSql.execute("CREATE DATABASE db")
        tdSql.execute("USE db")
        tdSql.execute("CREATE STABLE stb (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 1d")
        tdSql.error(f"CREATE TABLE ctb USING stb TAGS (1) KEEP 1d",expectErrInfo="child table cannot set keep duration", fullMatched=False)
        tdSql.execute(f"CREATE TABLE ctb USING stb TAGS (1)")
        tdSql.error(f"ALTER TABLE ctb keep 1d",expectErrInfo="only super table can alter keep duration", fullMatched=False)

    def check_normal_table_with_keep(self):
        tdLog.info(f"check normal table with keep")
        tdSql.execute("USE test")
        tdSql.error("CREATE TABLE ntb (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) KEEP 1d",expectErrInfo="KEEP parameter is not allowed when creating normal table", fullMatched=False)
        tdSql.execute("CREATE TABLE ntb (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10))")
        tdSql.error("ALTER TABLE ntb keep 1d",expectErrInfo="only super table can alter keep duration", fullMatched=False)

    def chceck_stb_keep_show_create(self):
        tdSql.execute('alter local \'showFullCreateTableColumn\' \'1\'')
        tdLog.info(f"check stb keep show create")
        tdSql.execute("USE test")
        tdSql.execute("CREATE STABLE stb (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT) KEEP 10d")
        tdSql.query("SHOW CREATE TABLE stb")
        tdSql.checkData(0, 1, "CREATE STABLE `stb` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `a` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `b` FLOAT ENCODE 'bss' COMPRESS 'lz4' LEVEL 'medium', `c` VARCHAR(10) ENCODE 'disabled' COMPRESS 'zstd' LEVEL 'medium') TAGS (`e_id` INT) KEEP 14400m")
        tdSql.execute("ALTER TABLE stb KEEP 5d")
        tdSql.query("SHOW CREATE TABLE stb")
        tdSql.checkData(0, 1, "CREATE STABLE `stb` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `a` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `b` FLOAT ENCODE 'bss' COMPRESS 'lz4' LEVEL 'medium', `c` VARCHAR(10) ENCODE 'disabled' COMPRESS 'zstd' LEVEL 'medium') TAGS (`e_id` INT) KEEP 7200m")

    def check_stb_keep_ins_table(self):
        tdLog.info(f"check stb keep ins table")
        tdSql.execute("CREATE DATABASE res_test")
        tdSql.execute("USE res_test")
        tdSql.execute("CREATE STABLE stb (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT)")
        tdSql.query("SELECT * FROM information_schema.ins_stables where db_name = 'res_test'")
        tdSql.checkData(0, 12, "-1")
        tdSql.execute("ALTER TABLE stb KEEP 10d")
        tdSql.query("SELECT * FROM information_schema.ins_stables where db_name = 'res_test'")
        tdSql.checkData(0, 12, "14400")
        
        
    # run
    def test_create_stb_keep(self):
        """Stable keep options
        
        1. prepare database
        2. check create stb with keep
        3. check create stb with err keep duration
        4. check alter stb with keep
        5. check alter stb with keep err
        6. check child table with keep
        7. check normal table with keep
        8. check stb keep show create
        9. check stb keep ins_stables
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-23 Alex Duan Migrated from uncatalog/army/create/test_create_stb_keep.py

        """
        tdLog.debug(f"start to excute {__file__}")

        # prepare database
        self.prepare_database()

        # check create stb with keep
        self.check_create_stb_with_keep()

        # check create stb with err keep duration
        self.check_create_stb_with_err_keep_duration()

        # check alter stb with keep
        self.check_alter_stb_with_keep()

        # check alter stb with keep err
        self.check_alter_stb_with_keep_err()

        # check child table with keep
        self.check_child_table_with_keep()

        # check normal table with keep
        self.check_normal_table_with_keep()

        # check stb keep show create
        self.chceck_stb_keep_show_create()

        # check stb keep ins table
        self.check_stb_keep_ins_table()

        tdLog.success(f"{__file__} successfully executed")

