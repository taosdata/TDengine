###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from taostest import TDCase, T
from taostest.util.common import TDCom
import time

class TestTs2899(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def run(self):
        self.tdSql.execute('drop database if exists ctg_tsdb;')
        self.tdSql.execute('create database ctg_tsdb replica 1 keep 3650 minRows 100 maxRows 4096 duration 1 comp 2 vgroups 1 precision "ms";')
        self.tdSql.execute('use ctg_tsdb;')
        self.tdSql.execute('create table if not exists stb_sxny_cn (dt timestamp, val int) tags(point varchar(10), point_name int, point_path int, index_tag int);')
        self.tdSql.execute('create table ctb1 using stb_sxny_cn tags("1",2,3,4);')
        self.tdSql.execute('create table ctb2 using stb_sxny_cn tags("1",3,3,4);')
        self.tdSql.execute('create table ctb3 using stb_sxny_cn tags("1",3,3,4);')
        self.tdSql.execute('create table ctb4 using stb_sxny_cn tags("1",4,3,4);')
        self.tdSql.execute('create table ctb5 using stb_sxny_cn tags("1",5,3,4);')
        self.tdSql.execute('create stream `stb_sxny_cn_index_prepare_drzfdl5` trigger window_close ignore expired 1 fill_history 1 into `ctg_tsdb`.`stb_sxny_cn_index_prepare5` tags(point varchar(50)) subtable (point) as select _wstart dt, first(val) fir_val,last(val) sec_val from ctg_tsdb.stb_sxny_cn where 1=1 and dt >= "2023-12-01" partition by point,point_name,point_path,index_tag interval(1d);')
        self.tdSql.execute('insert into ctb1 values("2024-01-01 00:00:00", 1);')
        self.tdSql.execute('insert into ctb2 values("2024-01-01 00:00:00", 1);')
        self.tdSql.execute('insert into ctb3 values("2024-01-01 00:00:00", 1);')
        self.tdSql.execute('insert into ctb4 values("2024-01-01 00:00:00", 1);')
        self.tdSql.execute('insert into ctb5 values("2024-01-01 00:00:00", 1);')
        time.sleep(2)
        self.tdSql.query('select _wstart as dt, first(val) fir_val,last(val) sec_val from ctg_tsdb.stb_sxny_cn where 1=1 and dt >= "2023-12-01" partition by point,point_name,point_path,index_tag interval(1d);')
        self.tdSql.query('show tables')
        for query_data in self.tdSql.query_data:
            self.tdSql.query(f'select * from `{query_data[0]}`')
        self.tdSql.execute('insert into ctb1 values("2024-01-02 00:00:00", 1);')
        self.tdSql.execute('insert into ctb2 values("2024-01-02 00:00:00", 1);')
        self.tdSql.execute('insert into ctb3 values("2024-01-02 00:00:00", 1);')
        self.tdSql.execute('insert into ctb4 values("2024-01-02 00:00:00", 1);')
        self.tdSql.execute('insert into ctb5 values("2024-01-02 00:00:00", 1);')
        self.tdSql.query('show tables')
        for query_data in self.tdSql.query_data:
            self.tdSql.query(f'select * from `{query_data[0]}`')
        

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-td28032
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write
