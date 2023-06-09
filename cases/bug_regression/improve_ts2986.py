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
from taostest.util.remote import Remote

class TestTs2986(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.taospy_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taospy")
        self.host = self.taosd_setting["spec"]["dnodes"][0]["endpoint"].split(":")[0]
        self.target_ts = self.tdCom.genTs()[0]
        self.max_query_time_range = int(self.taospy_setting["spec"]["config"]["maxQueryTimeRange"]) * 1000
        self._remote: Remote = Remote(self.logger)
        self.legal_condition_list = list()
        self.illegal_condition_list = list()

        self.dbname = "test"
        self.stbname = "stb"
        self.ctbname = "ctb"

    def prepare_data(self):
        using_ts = self.target_ts
        self.tdCom.createDb(self.dbname)
        self.tdCom.create_stable(dbname=self.dbname, stbname=self.stbname)
        self.tdCom.create_ctable(dbname=self.dbname, stbname=self.stbname, ctbname=self.ctbname)
        for i in range(-int(self.taospy_setting["spec"]["config"]["maxQueryTimeRange"])-2, 0):
            _tmp_ts = using_ts + i*1000
            self.tdCom.insert_rows(dbname=self.dbname, tbname=self.ctbname, ts_value=_tmp_ts)
            _tmp_ts = using_ts - i*1000
            self.tdCom.insert_rows(dbname=self.dbname, tbname=self.ctbname, ts_value=_tmp_ts)
        self.tdCom.insert_rows(dbname=self.dbname, tbname=self.ctbname, ts_value=self.target_ts)


    def gen_legal_condition_list(self):
        using_ts = self.target_ts
        self.legal_condition_list.append(f'ts > {using_ts} and ts < {using_ts + self.max_query_time_range}')
        self.legal_condition_list.append(f'ts >= {using_ts} and ts < {using_ts + self.max_query_time_range}')
        self.legal_condition_list.append(f'ts > {using_ts} and ts <= {using_ts + self.max_query_time_range}')
        self.legal_condition_list.append(f'ts > {using_ts} and ts < {using_ts + self.max_query_time_range +1}')

        self.legal_condition_list.append(f'ts > {using_ts - self.max_query_time_range} and ts < {using_ts}')
        self.legal_condition_list.append(f'ts >= {using_ts - self.max_query_time_range} and ts < {using_ts}')
        self.legal_condition_list.append(f'ts > {using_ts - self.max_query_time_range} and ts <= {using_ts}')
        self.legal_condition_list.append(f'ts > {using_ts - self.max_query_time_range -1} and ts < {using_ts}')

        self.legal_condition_list.append(f'ts > {using_ts - self.max_query_time_range} and ts < {using_ts}')

    def gen_illegal_condition_list(self):
        using_ts = self.target_ts
        self.illegal_condition_list.append(f'ts >= {using_ts} and ts <= {using_ts + self.max_query_time_range}')
        self.illegal_condition_list.append(f'ts >= {using_ts} and ts < {using_ts + self.max_query_time_range+1}')
        self.illegal_condition_list.append(f'ts > {using_ts-1} and ts <= {using_ts + self.max_query_time_range}')
        self.illegal_condition_list.append(f'ts > {using_ts} and ts <= {using_ts + self.max_query_time_range +1}')

        self.illegal_condition_list.append(f'ts >= {using_ts - self.max_query_time_range} and ts <= {using_ts}')
        self.illegal_condition_list.append(f'ts >= {using_ts - self.max_query_time_range} and ts < {using_ts+1}')
        self.illegal_condition_list.append(f'ts > {using_ts - self.max_query_time_range -1} and ts <= {using_ts}')
        self.illegal_condition_list.append(f'ts >= {using_ts - self.max_query_time_range -1} and ts < {using_ts}')

        self.illegal_condition_list.append(f'ts > {using_ts - self.max_query_time_range-1} and ts < {using_ts} or ts = {using_ts + 1}')

    def run(self):
        self.prepare_data()
        self.gen_legal_condition_list()
        for sql in self.legal_condition_list:
            self.tdSql.query(f'select count(*) from {self.stbname} where {sql}')
            self.tdSql.query(f'select count(*) from {self.stbname} where {sql} order by ts desc')
            self.tdSql.query(f'select * from (select * from {self.stbname} where {sql}) where {sql}')

        self.gen_illegal_condition_list()
        for sql in self.illegal_condition_list:
            self.tdSql.error(f'select count(*) from {self.stbname} where {sql}')
            self.tdSql.error(f'select count(*) from {self.stbname} where {sql} order by ts desc')
            self.tdSql.error(f'select * from (select * from {self.stbname} where {sql}) where {sql}')
        self.tdSql.error(f'select * from (select * from {self.stbname} where ts > {self.target_ts} and ts < {self.target_ts + self.max_query_time_range})')
        self.tdSql.error(f'select * from (select * from {self.stbname}) where ts > {self.target_ts} and ts < {self.target_ts + self.max_query_time_range}')

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            test_ts2986
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Query