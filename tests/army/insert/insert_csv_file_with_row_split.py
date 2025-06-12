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

import sys
import time
import random

import taos
import frame
import frame.etool


from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def prepare_database(self):
        tdLog.info(f"prepare database")
        tdSql.execute("DROP DATABASE IF EXISTS `vehicle_prod2`")
        tdSql.execute("CREATE DATABASE `vehicle_prod2` BUFFER 32 CACHESIZE 1 CACHEMODEL 'last_row' COMP 2 DURATION 50d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 1 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' S3_CHUNKPAGES 262144 S3_KEEPLOCAL 5256000m S3_COMPACT 0 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h ")
        tdSql.execute("USE vehicle_prod2")
        tdSql.execute("CREATE STABLE `up_topic` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `msg_id` BIGINT UNSIGNED ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY, `topic_content` VARCHAR(50) ENCODE 'disabled' COMPRESS 'lz4' LEVEL 'medium', `command_code` VARCHAR(3) ENCODE 'disabled' COMPRESS 'lz4' LEVEL 'medium', `msg` VARCHAR(1500) ENCODE 'disabled' COMPRESS 'lz4' LEVEL 'medium', `data_source` TINYINT UNSIGNED ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`device_id` VARCHAR(50))")


    def create_ctb_using_csv_file(self):
        tdLog.info(f"create ctb using csv file")
        tdSql.execute("USE vehicle_prod2")
        datafile = etool.curFile(__file__, "data/test.csv")
        tdLog.info(f"INSERT INTO vehicle_prod2.up_topic (tbname,ts,msg_id,topic_content,command_code,msg,data_source,device_id) FILE '{datafile}';")
        tdSql.execute(f"INSERT INTO vehicle_prod2.up_topic (tbname,ts,msg_id,topic_content,command_code,msg,data_source,device_id) FILE '{datafile}';")

    def check_create_ctb_using_csv_file(self):
        tdLog.info(f"check create ctb using csv file")
        tdSql.execute("USE vehicle_prod2")
        tdSql.query("select * from up_topic;")
        tdSql.checkRows(13)
        
    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # prepare database
        self.prepare_database()

        # create ctb using csv file
        self.create_ctb_using_csv_file()

        # check create ctb using csv file
        self.check_create_ctb_using_csv_file()

        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())