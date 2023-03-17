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
from taostest.components import TaosD
from taostest.util.remote import Remote
import taos

class TestTs2899(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.vgroups = 10
        self.rows = 10

    def run(self):
        self.tdCom.createDb(dbname="bug2899_test", vgroups=self.vgroups)
        self.tdSql.execute('create stable bug2899_test.t_realtime_value (ts timestamp, parttime bigint, close bigint, security_id int, high int, low int, pre_close bigint, volume double, settle_price double, pre_settle_price double, money double, open double) tags (tt int);')
        self.tdSql.execute('create table bug2899_test.ctb using bug2899_test.t_realtime_value tags(1);')
        self.tdSql.execute('create stream realtime_value_stream into bug2899_test.t_realtime_value_stream subtable(concat("real-", tname)) as select last(parttime) as parttime,last(close) as close, last(security_id) as security_id, max(high) as high, min(low) as low, last(pre_close) as pre_close, last(volume ) as volume, last(settle_price) as settle_price, last(pre_settle_price) as pre_settle_price, last(money) as money , last(open) as open from bug2899_test.t_realtime_value partition by tbname tname state_window(parttime);')
        ts = self.tdCom.genTs()[0]
        for i in range(self.rows):
            self.tdSql.execute(f'insert into bug2899_test.ctb values ({ts}, {i}, {i}, {i}, {i}, {i}, {i}, {i}, {i}, {i}, {i}, {i});')
            ts += 1
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.taosd.update_taosd(self.taosd_setting)
        for dnode in self.taosd_setting["spec"]["dnodes"]:
            self.taosd.restart(dnode)
        for i in range(self.rows, self.rows*2):
            res = self._remote.cmd(self.taosd_setting["fqdn"][0], [f'taos -s "insert into bug2899_test.ctb values ({ts}, {i}, {i}, {i}, {i}, {i}, {i}, {i}, {i}, {i}, {i}, {i})";'])
            ts += 1
        res = self._remote.cmd(self.taosd_setting["fqdn"][0], [f'pidof taosd'])
        self.tdSql.checkEqual(int(res) > 0, True)

        # self.tdSql.close()
        # taosd_conf = self.get_component_by_name("taosd")[0]
        # conn2 = taos.connect(host=taosd_conf["fqdn"][0])
        # conn2.query(f'select count(*) from bug2899_test.t_realtime_value')
        # # conn2 = self.tdSql.get_connection(taosd_conf)
        # try:
        #     for i in range(11, 20):
        #         conn2.execute(f'insert into bug2899_test.ctb values ({ts}, {i}, {i}, {i}, {i}, {i}, {i}, {i}, {i}, {i}, {i}, {i});')
        #         ts += 1
        # finally:
        #     conn2.close()
        # self.tdSql.query(f'select count(*) from bug2899_test.t_realtime_value')
        # expected_row_count = self.tdSql.query_data[0][0]
        # self.tdSql.query(f'select count(*) from bug2899_test.t_realtime_value_stream')
        # self.tdSql.checkEqual(self.tdSql.query_data[0][0], expected_row_count-1)

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts29899
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write