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


import os
import subprocess
import time

from new_test_framework.utils import tdDnodes, tdLog, tdSql


class TestSysinfo:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.dbname = "db"
        cls.delaytime = 3

    def get_database_info(self):
        tdSql.query("select database()")
        tdSql.checkData(0, 0, None)
        tdSql.execute(f"create database if not exists {self.dbname}")
        tdSql.execute(f"use {self.dbname}")
        tdSql.query("select database()")
        tdSql.checkData(0, 0, self.dbname)
        tdSql.execute(f"drop database {self.dbname}")

    def check_version(self):
        taos_list = ["server", "client"]
        for i in taos_list:
            tdSql.query(f"select {i}_version()")
            version_c_file = os.path.join(
                os.path.dirname(os.getcwd()), "cmake", "version.cmake"
            )
            tdLog.info(f"version_c_file: {version_c_file}")
            version_info = str(
                subprocess.run(
                    f'cat {version_c_file} |grep "SET(TD_VER_NUMBER "',
                    shell=True,
                    capture_output=True,
                ).stdout.decode("utf8")
            ).split('"')[1]
            tdSql.checkData(0, 0, version_info)

    def get_server_status(self):
        time.sleep(self.delaytime)
        tdSql.query("select server_status()")
        tdSql.checkData(0, 0, 1)
        #!for bug
        tdDnodes.stoptaosd(1)
        time.sleep(self.delaytime * 5)
        tdSql.error("select server_status()")

    def check_current_user(self):
        tdSql.query("select current_user()")
        user = tdSql.getData(0, 0).split("@")[0]
        if user == "root":
            tdLog.info(f"current_user is {user}")
        else:
            tdLog.exit(f"current_user is {user}, expected root")

    def test_fun_sys_client_version(self):
        """Fun: client_version()

        1. Get td_version value from version.c
        2. Compare with the result of client_version()

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-17 Alex Duan Migrated from uncatalog/system-test/0-other/test_sysinfo.py

        """
        self.check_version()

    def test_fun_sys_server_version(self):
        """Fun: server_version()

        1. Get td_version value from version.c
        2. Compare with the result of server_version()

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-17 Alex Duan Migrated from uncatalog/system-test/0-other/test_sysinfo.py

        """
        self.check_version()

    def test_fun_sys_current_user(self):
        """Fun: current_user()

        1. Query "select current_user()"
        2. Check the result is "root"

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-17 Alex Duan Created

        """
        self.check_current_user()

    def test_fun_sys_database(self):
        """Fun: database()

        1. Create database test
        2. Use database test
        3. Query "select database()"
        4. Check the result is "test"

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-17 Alex Duan Migrated from uncatalog/system-test/0-other/test_sysinfo.py

        """
        self.get_database_info()

    def test_fun_sys_server_status(self):
        """Fun: server_status()

        1. taosd server is running
        2. Query server status expect 1
        3. Stop taosd server
        4. Sleep 15 seconds
        5. Query server status throw error

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-17 Alex Duan Created

        """
        self.get_server_status()
