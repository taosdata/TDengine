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
from new_test_framework.utils import tdLog, tdSql, etool

class TestSelectWithJson:
    updatecfgDict = {
        "keepColumnName": "1",
        "ttlChangeOnWrite": "1",
        "querySmaOptimize": "1",
        "slowLogScope": "none",
        "queryBufferSize": 10240
    }

    def test_select_with_json(self):
        """Select: with json params

        test select function with json params

        Since: v3.3.0.0

        Labels: decimal

        History:
            - 2024-12-13 Jing Sima Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        tdLog.info(f"insert data.")
        tdSql.execute("drop database if exists ts_5763;")
        tdSql.execute("create database ts_5763;")
        tdSql.execute("use ts_5763;")
        tdSql.execute("select database();")
        tdSql.execute("CREATE STABLE metrics (ts TIMESTAMP, v DOUBLE) TAGS (labels JSON)")
        tdSql.execute("""CREATE TABLE `metrics_0` USING `metrics` (`labels`) TAGS ('{"ident":"192.168.56.167"}');""")
        tdSql.execute("""CREATE TABLE `metrics_1` USING `metrics` (`labels`) TAGS ('{"ident":"192.168.56.168"}');""")
        tdSql.execute("""CREATE TABLE `metrics_2` USING `metrics` (`labels`) TAGS ('{"ident":"192.168.56.169"}');""")
        tdSql.execute("""CREATE TABLE `metrics_3` USING `metrics` (`labels`) TAGS ('{"ident":"192.168.56.170"}');""")
        tdSql.execute("""CREATE TABLE `metrics_5` USING `metrics` (`labels`) TAGS ('{"asset_name":"中国政务网"}');""")
        tdSql.execute("""CREATE TABLE `metrics_6` USING `metrics` (`labels`) TAGS ('{"asset_name":"地大物博阿拉丁快解放啦上课交电费"}');""")
        tdSql.execute("""CREATE TABLE `metrics_7` USING `metrics` (`labels`) TAGS ('{"asset_name":"no1241-上的六块腹肌阿斯利康的肌肤轮廓设计大方"}');""")
        tdSql.execute("""CREATE TABLE `metrics_8` USING `metrics` (`labels`) TAGS ('{"asset_name":"no1241-上的六块腹肌阿斯利康的肌肤轮廓设计大方","ident":"192.168.0.1"}');""")
        tdSql.execute("""CREATE TABLE `metrics_9` USING `metrics` (`labels`) TAGS ('{"asset_name":"no1241-上的六块腹肌阿斯利康的肌肤轮廓设计大方","ident":"192.168.0.1"}');""")
        tdSql.execute("""CREATE TABLE `metrics_10` USING `metrics` (`labels`) TAGS ('{"asset_name":"上的咖啡机no1241-上的六块腹肌阿斯利康的肌肤轮廓设计大方","ident":"192.168.0.1"}');""")

        tdSql.execute("insert into metrics_0 values ('2024-12-12 16:34:39.326',1)")
        tdSql.execute("insert into metrics_0 values ('2024-12-12 16:34:40.891',2)")
        tdSql.execute("insert into metrics_0 values ('2024-12-12 16:34:41.986',3)")
        tdSql.execute("insert into metrics_0 values ('2024-12-12 16:34:42.992',4)")
        tdSql.execute("insert into metrics_0 values ('2024-12-12 16:34:46.927',5)")
        tdSql.execute("insert into metrics_0 values ('2024-12-12 16:34:48.473',6)")
        tdSql.execute("insert into metrics_1 select * from metrics_0")
        tdSql.execute("insert into metrics_2 select * from metrics_0")
        tdSql.execute("insert into metrics_3 select * from metrics_0")
        tdSql.execute("insert into metrics_5 select * from metrics_0")
        tdSql.execute("insert into metrics_6 select * from metrics_0")
        tdSql.execute("insert into metrics_7 select * from metrics_0")
        tdSql.execute("insert into metrics_8 values ('2024-12-12 19:05:36.459',1)")
        tdSql.execute("insert into metrics_8 values ('2024-12-12 19:05:37.388',1)")
        tdSql.execute("insert into metrics_8 values ('2024-12-12 19:05:37.622',1)")
        tdSql.execute("insert into metrics_8 values ('2024-12-12 19:05:37.852',1)")
        tdSql.execute("insert into metrics_8 values ('2024-12-12 19:05:38.081',1)")
        tdSql.execute("insert into metrics_8 values ('2024-12-12 19:05:38.307',1)")
        tdSql.execute("insert into metrics_8 values ('2024-12-12 19:05:38.535',1)")
        tdSql.execute("insert into metrics_8 values ('2024-12-12 19:05:38.792',1)")
        tdSql.execute("insert into metrics_8 values ('2024-12-12 19:05:39.035',1)")
        tdSql.execute("insert into metrics_8 values ('2024-12-12 19:05:39.240',1)")
        tdSql.execute("insert into metrics_9 values ('2024-12-12 19:05:29.270',1)")
        tdSql.execute("insert into metrics_9 values ('2024-12-12 19:05:30.508',1)")
        tdSql.execute("insert into metrics_9 values ('2024-12-12 19:05:31.035',1)")
        tdSql.execute("insert into metrics_9 values ('2024-12-12 19:05:31.523',1)")
        tdSql.execute("insert into metrics_9 values ('2024-12-12 19:05:31.760',1)")
        tdSql.execute("insert into metrics_9 values ('2024-12-12 19:05:32.001',1)")
        tdSql.execute("insert into metrics_9 values ('2024-12-12 19:05:32.228',1)")
        tdSql.execute("insert into metrics_9 values ('2024-12-12 19:05:32.453',1)")
        tdSql.execute("insert into metrics_9 values ('2024-12-12 19:05:32.690',1)")
        tdSql.execute("insert into metrics_9 values ('2024-12-12 19:05:32.906',1)")
        tdSql.execute("insert into metrics_10 values ('2024-12-12 19:06:14.538',1)")
        tdSql.execute("insert into metrics_10 values ('2024-12-12 19:06:15.114',1)")
        tdSql.execute("insert into metrics_10 values ('2024-12-12 19:06:15.613',1)")
        tdSql.execute("insert into metrics_10 values ('2024-12-12 19:06:15.853',1)")
        tdSql.execute("insert into metrics_10 values ('2024-12-12 19:06:16.054',1)")
        tdSql.execute("insert into metrics_10 values ('2024-12-12 19:06:16.295',1)")
        tdSql.execute("insert into metrics_10 values ('2024-12-12 19:06:16.514',1)")
        tdSql.execute("insert into metrics_10 values ('2024-12-12 19:06:16.731',1)")
        tdSql.execute("insert into metrics_10 values ('2024-12-12 19:06:16.958',1)")
        tdSql.execute("insert into metrics_10 values ('2024-12-12 19:06:17.176',1)")

        for i in range(1, 10):
            tdSql.query("select _wstart,first(v)-last(v), first(labels->'asset_name'),first(labels->'ident'),mode(labels->'asset_name'),mode(labels->'ident'),last(labels->'asset_name'),last(labels->'ident') from ts_5763.metrics interval(1s)")
            tdSql.checkRows(18)



