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

from new_test_framework.utils import tdLog, tdSql, epath, sc, etool
import os
import time





class TestCreateCtbUsingCsvFile:
    updatecfgDict = {
        "maxInsertBatchRows": 10
    }

    def prepare_database(self):
        tdLog.info(f"prepare database")
        tdSql.execute("DROP DATABASE IF EXISTS qxjtest")
        tdSql.execute("create database qxjtest buffer 200 vgroups 20 stt_trigger 1 pagesize 64 maxrows 7501 keep 90d duration 1d;")
        tdSql.execute("USE qxjtest")
        tdSql.execute("create stable qxjtest.gdcdata(ts timestamp, gdvalue float) tags (element varchar(10), height smallint, validtime tinyint, latitude smallint)")


    def create_ctb_using_csv_file(self):
        tdLog.info(f"create ctb using csv file")
        tdSql.execute("USE qxjtest")
        datafile =etool.getFilePath(__file__, "data", "create_ctb_127.csv")
        tdLog.info(f"create table if not exists using qxjtest.gdcdata(tbname,element,height,validtime,latitude) file '{datafile}';")
        tdSql.execute(f"create table if not exists using qxjtest.gdcdata(tbname,element,height,validtime,latitude) file '{datafile}';")

    def check_create_ctb_using_csv_file(self):
        tdLog.info(f"check create ctb using csv file")
        tdSql.execute("USE qxjtest")
        tdSql.query("show tables;")
        tdSql.checkRows(127)
        
    # run
    def test_subtable_create_using_csv(self):
        """Child table create using csv
        
        1. Create database with vgroups 20 stt_trigger 1
        2. Create super table according to csvfile format
        3. Create child table using csv file
        4. Check created child table number

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-23 Alex Duan Migrated from uncatalog/army/create/test_create_ctb_using_csv_file.py

        """
        tdLog.debug(f"start to excute {__file__}")

        # prepare database
        self.prepare_database()

        # create ctb using csv file
        self.create_ctb_using_csv_file()

        # check create ctb using csv file
        self.check_create_ctb_using_csv_file()

        tdLog.success(f"{__file__} successfully executed")

