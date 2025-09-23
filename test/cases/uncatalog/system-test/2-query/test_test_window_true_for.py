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

import time
from new_test_framework.utils import tdLog, tdSql
class TestTestWindowTrueFor:
    # init
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    def create_objects(self):
        tdSql.execute("drop database if exists test", show=True)
        tdSql.execute("create database test keep 36500 precision 'ms'", show=True)
        tdSql.execute("use test", show=True)
        tdSql.execute("create stable st (ts timestamp, c1 int) tags (gid int)", show=True)
        tdSql.execute("create table ct_0 using st(gid) tags (0)")
        tdSql.execute("create table ct_1 using st(gid) tags (1)")

        tdSql.execute(f'''create stream s_event_1 into d_event_1 as
                            select _wstart, _wend, count(*) from ct_0
                            event_window start with c1 > 0 end with c1 < 0 true_for(3s);''', show=True)
        tdSql.execute(f'''create stream s_event_2 ignore update 0 ignore expired 0 into d_event_2 as
                            select _wstart, _wend, count(*) from ct_0
                            event_window start with c1 > 0 end with c1 < 0 true_for(3s);''', show=True)

        tdSql.execute(f'''create stream s_event_3 into d_event_3 as
                            select _wstart, _wend, count(*) from ct_0
                            event_window start with c1 > 0 end with c1 < 0 true_for(2999);''', show=True)
        tdSql.execute(f'''create stream s_event_4 ignore update 0 ignore expired 0 into d_event_4 as
                            select _wstart, _wend, count(*) from ct_0
                            event_window start with c1 > 0 end with c1 < 0 true_for(2999);''', show=True)

        tdSql.execute(f'''create stream s_event_5 into d_event_5 as
                            select _wstart, _wend, count(*) from ct_0
                            event_window start with c1 > 0 end with c1 < 0 true_for('3001a');''', show=True)
        tdSql.execute(f'''create stream s_event_6 ignore update 0 ignore expired 0 into d_event_6 as
                            select _wstart, _wend, count(*) from ct_0
                            event_window start with c1 > 0 end with c1 < 0 true_for('3001a');''', show=True)

        tdSql.execute(f'''create stream s_state_1 into d_state_1 as
                            select _wstart, _wend, count(*) from ct_1
                            state_window(c1) true_for (3s);''', show=True)
        tdSql.execute(f'''create stream s_state_2 ignore update 0 ignore expired 0 into d_state_2 as
                            select _wstart, _wend, count(*) from ct_1
                            state_window(c1) true_for (3s);''', show=True)

        tdSql.execute(f'''create stream s_state_3 into d_state_3 as
                            select _wstart, _wend, count(*) from ct_1
                            state_window(c1) true_for (2999);''', show=True)
        tdSql.execute(f'''create stream s_state_4 ignore update 0 ignore expired 0 into d_state_4 as
                            select _wstart, _wend, count(*) from ct_1
                            state_window(c1) true_for (2999);''', show=True)

        tdSql.execute(f'''create stream s_state_5 into d_state_5 as
                            select _wstart, _wend, count(*) from ct_1
                            state_window(c1) true_for ('3001a');''', show=True)
        tdSql.execute(f'''create stream s_state_6 ignore update 0 ignore expired 0 into d_state_6 as
                            select _wstart, _wend, count(*) from ct_1
                            state_window(c1) true_for ('3001a');''', show=True)
        # Wait for the stream tasks to be ready
        for i in range(50):
            tdLog.info(f"i={i} wait for stream tasks ready ...")
            time.sleep(1)
            rows = tdSql.query("select * from information_schema.ins_stream_tasks where status <> 'ready';")
            if rows == 0:
                break
    
    def insert_data(self):
        tdSql.execute(f'''insert into ct_0 values
                            ('2025-01-01 00:00:00.000', -1),
                            ('2025-01-01 00:00:01.000', 1),
                            ('2025-01-01 00:00:02.000', -1),
                            ('2025-01-01 00:00:03.000', 1),
                            ('2025-01-01 00:00:04.000', 1),
                            ('2025-01-01 00:00:05.000', -1),
                            ('2025-01-01 00:00:06.000', 1),
                            ('2025-01-01 00:00:07.000', 1),
                            ('2025-01-01 00:00:08.000', 1),
                            ('2025-01-01 00:00:08.999', -1),
                            ('2025-01-01 00:00:10.000', 1),
                            ('2025-01-01 00:00:11.000', 1),
                            ('2025-01-01 00:00:12.000', 1),
                            ('2025-01-01 00:00:13.000', -1),
                            ('2025-01-01 00:00:14.000', 1),
                            ('2025-01-01 00:00:15.000', 1),
                            ('2025-01-01 00:00:16.000', 1),
                            ('2025-01-01 00:00:17.001', -1),
                            ('2025-01-01 00:00:18.000', 1),
                            ('2025-01-01 00:00:19.000', 1),
                            ('2025-01-01 00:00:20.000', 1),
                            ('2025-01-01 00:00:21.000', 1),
                            ('2025-01-01 00:00:22.000', -1),
                            ('2025-01-01 00:00:23.000', -1),
                            ('2025-01-01 00:00:24.000', 1),
                            ('2025-01-01 00:00:25.000', 1),
                            ('2025-01-01 00:00:26.000', 1),
                            ('2025-01-01 00:00:27.000', 1),
                            ('2025-01-01 00:00:28.000', 1),
                            ('2025-01-01 00:00:29.000', 1),
                            ('2025-01-01 00:00:30.000', -1),
                            ('2025-01-01 00:00:31.000', 0);''', show=True)
        tdSql.execute(f'''insert into ct_1 values
                            ('2025-01-01 00:00:00.000', 0),
                            ('2025-01-01 00:00:01.000', 1),
                            ('2025-01-01 00:00:02.000', 1),
                            ('2025-01-01 00:00:03.000', 2),
                            ('2025-01-01 00:00:04.000', 2),
                            ('2025-01-01 00:00:05.000', 2),
                            ('2025-01-01 00:00:06.000', 3),
                            ('2025-01-01 00:00:07.000', 3),
                            ('2025-01-01 00:00:08.000', 3),
                            ('2025-01-01 00:00:08.999', 3),
                            ('2025-01-01 00:00:10.000', 4),
                            ('2025-01-01 00:00:11.000', 4),
                            ('2025-01-01 00:00:12.000', 4),
                            ('2025-01-01 00:00:13.000', 4),
                            ('2025-01-01 00:00:14.000', 5),
                            ('2025-01-01 00:00:15.000', 5),
                            ('2025-01-01 00:00:16.000', 5),
                            ('2025-01-01 00:00:17.001', 5),
                            ('2025-01-01 00:00:18.000', 6),
                            ('2025-01-01 00:00:19.000', 6),
                            ('2025-01-01 00:00:20.000', 6),
                            ('2025-01-01 00:00:21.000', 6),
                            ('2025-01-01 00:00:22.000', 6),
                            ('2025-01-01 00:00:23.000', 0),
                            ('2025-01-01 00:00:24.000', 7),
                            ('2025-01-01 00:00:25.000', 7),
                            ('2025-01-01 00:00:26.000', 7),
                            ('2025-01-01 00:00:27.000', 7),
                            ('2025-01-01 00:00:28.000', 7),
                            ('2025-01-01 00:00:29.000', 7),
                            ('2025-01-01 00:00:30.000', 7),
                            ('2025-01-01 00:00:31.000', 0);''', show=True)
        tdLog.info("wait for all stream tasks to be ready ...")
        time.sleep(10)
    
    def update_data(self):
        tdSql.execute(f'''insert into ct_0 values
                            ('2025-01-01 00:00:00.000', 1),
                            ('2025-01-01 00:00:22.000', 1),
                            ('2025-01-01 00:00:28.000', -1);''', show=True)
        tdSql.execute(f'''insert into ct_1 values
                            ('2025-01-01 00:00:00.000', 1),
                            ('2025-01-01 00:00:23.000', 6),
                            ('2025-01-01 00:00:29.000', 8),
                            ('2025-01-01 00:00:30.000', 8);''', show=True)
        tdLog.info("wait for all stream tasks to be ready ...")
        time.sleep(5)

    def check_result(self):
        tdSql.query("select * from d_event_1", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, '2025-01-01 00:00:10.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:13.000')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:22.000')
        tdSql.checkData(2, 2, 5)
        tdSql.checkData(3, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(3, 1, '2025-01-01 00:00:30.000')
        tdSql.checkData(3, 2, 7)

        tdSql.query("select * from d_event_2", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, '2025-01-01 00:00:10.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:13.000')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:23.000')
        tdSql.checkData(2, 2, 6)
        tdSql.checkData(3, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(3, 1, '2025-01-01 00:00:28.000')
        tdSql.checkData(3, 2, 5)

        tdSql.query("select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3s);", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, '2025-01-01 00:00:10.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:13.000')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:23.000')
        tdSql.checkData(2, 2, 6)
        tdSql.checkData(3, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(3, 1, '2025-01-01 00:00:28.000')
        tdSql.checkData(3, 2, 5)

        tdSql.query("select * from d_event_3", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, '2025-01-01 00:00:06.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:08.999')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:10.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:13.000')
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(3, 1, '2025-01-01 00:00:22.000')
        tdSql.checkData(3, 2, 5)
        tdSql.checkData(4, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(4, 1, '2025-01-01 00:00:30.000')
        tdSql.checkData(4, 2, 7)

        tdSql.query("select * from d_event_4", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, '2025-01-01 00:00:06.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:08.999')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:10.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:13.000')
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(3, 1, '2025-01-01 00:00:23.000')
        tdSql.checkData(3, 2, 6)
        tdSql.checkData(4, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(4, 1, '2025-01-01 00:00:28.000')
        tdSql.checkData(4, 2, 5)

        tdSql.query("select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(2999);", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, '2025-01-01 00:00:06.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:08.999')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:10.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:13.000')
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(3, 1, '2025-01-01 00:00:23.000')
        tdSql.checkData(3, 2, 6)
        tdSql.checkData(4, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(4, 1, '2025-01-01 00:00:28.000')
        tdSql.checkData(4, 2, 5)

        tdSql.query("select * from d_event_5", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:22.000')
        tdSql.checkData(1, 2, 5)
        tdSql.checkData(2, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:30.000')
        tdSql.checkData(2, 2, 7)

        tdSql.query("select * from d_event_6", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:23.000')
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(2, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:28.000')
        tdSql.checkData(2, 2, 5)

        tdSql.query("select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for('3001a');", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:23.000')
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(2, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:28.000')
        tdSql.checkData(2, 2, 5)

        tdSql.query("select * from d_state_1", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, '2025-01-01 00:00:10.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:13.000')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:22.000')
        tdSql.checkData(2, 2, 5)
        tdSql.checkData(3, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(3, 1, '2025-01-01 00:00:30.000')
        tdSql.checkData(3, 2, 7)

        tdSql.query("select * from d_state_2", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, '2025-01-01 00:00:10.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:13.000')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:23.000')
        tdSql.checkData(2, 2, 6)
        tdSql.checkData(3, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(3, 1, '2025-01-01 00:00:28.000')
        tdSql.checkData(3, 2, 5)

        tdSql.query("select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for(3s);", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, '2025-01-01 00:00:10.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:13.000')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:23.000')
        tdSql.checkData(2, 2, 6)
        tdSql.checkData(3, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(3, 1, '2025-01-01 00:00:28.000')
        tdSql.checkData(3, 2, 5)

        tdSql.query("select * from d_state_3", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, '2025-01-01 00:00:06.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:08.999')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:10.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:13.000')
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(3, 1, '2025-01-01 00:00:22.000')
        tdSql.checkData(3, 2, 5)
        tdSql.checkData(4, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(4, 1, '2025-01-01 00:00:30.000')
        tdSql.checkData(4, 2, 7)

        tdSql.query("select * from d_state_4", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, '2025-01-01 00:00:06.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:08.999')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:10.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:13.000')
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(3, 1, '2025-01-01 00:00:23.000')
        tdSql.checkData(3, 2, 6)
        tdSql.checkData(4, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(4, 1, '2025-01-01 00:00:28.000')
        tdSql.checkData(4, 2, 5)

        tdSql.query("select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for(2999);", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, '2025-01-01 00:00:06.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:08.999')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:10.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:13.000')
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(3, 1, '2025-01-01 00:00:23.000')
        tdSql.checkData(3, 2, 6)
        tdSql.checkData(4, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(4, 1, '2025-01-01 00:00:28.000')
        tdSql.checkData(4, 2, 5)

        tdSql.query("select * from d_state_5", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:22.000')
        tdSql.checkData(1, 2, 5)
        tdSql.checkData(2, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:30.000')
        tdSql.checkData(2, 2, 7)

        tdSql.query("select * from d_state_6", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:23.000')
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(2, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:28.000')
        tdSql.checkData(2, 2, 5)

        tdSql.query("select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for('3001a');", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2025-01-01 00:00:14.000')
        tdSql.checkData(0, 1, '2025-01-01 00:00:17.001')
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 0, '2025-01-01 00:00:18.000')
        tdSql.checkData(1, 1, '2025-01-01 00:00:23.000')
        tdSql.checkData(1, 2, 6)
        tdSql.checkData(2, 0, '2025-01-01 00:00:24.000')
        tdSql.checkData(2, 1, '2025-01-01 00:00:28.000')
        tdSql.checkData(2, 2, 5)

    def check_abnormal_query(self):
        tdLog.info("test abnormal window true_for limit")
        tdSql.error("select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3n);")
        tdSql.error("select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3y);")
        tdSql.error("select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(-1);")
        tdSql.error("select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(-1a);")
        tdSql.error("select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for('-1a');")
        tdSql.error("create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3n);")
        tdSql.error("create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3y);")
        tdSql.error("create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(-1);")
        tdSql.error("create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(-1a);")
        tdSql.error("create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for('-1a');")

    def check_window_true_for_limit(self):
        """ Test the functionality of the true_for window function.

        This test covers:
        1. Both batch query and stream computing scenarios.
        2. Two types of windows: event_window and state_window.
        3. Parameter types for true_for: numeric and string.
        4. Boundary value tests.
        5. Error case tests.

        Since: v3.3.6.0

        Labels: true_for, state_window, event_window

        Jira: TS-5470

        History:
            - 2025-02-21 Kuang Jinqing Created
        """
        self.create_objects()
        self.insert_data()
        self.update_data()
        self.check_result()
        self.check_abnormal_query()

    # run
    def test_test_window_true_for(self):
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

        self.check_window_true_for_limit()

    # stop
        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
