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
import os

class TestTd22981(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.wal_file_type = [".log"]
        self.mnd_wal_file_path = f'{self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"]}/mnode/wal'

    def get_wal_file_list(self):
        mnd_wal_file_file_list = [os.path.join(self.mnd_wal_file_path, wal_path) for wal_path in os.listdir(self.mnd_wal_file_path) if os.path.splitext(wal_path)[1] in self.wal_file_type]
        return sorted(list(map(lambda x:int(x.split("/")[-1].replace(self.wal_file_type[0], "")), mnd_wal_file_file_list)))

    def run(self):
        for i in range(self.taosd_setting["spec"]["dnodes"][0]["config"]["mndSdbWriteDelta"]):
            self.tdSql.execute('create user u1 pass "u1";')
            self.tdSql.execute('drop user u1;')
        mnd_wal_file_file_list = self.get_wal_file_list()
        if mnd_wal_file_file_list[-1] < self.taosd_setting["spec"]["dnodes"][0]["config"]["mndLogRetention"]:
            self.tdSql.checkEqual(0 in mnd_wal_file_file_list, True)
            for i in range(int(self.taosd_setting["spec"]["dnodes"][0]["config"]["mndLogRetention"]/((mnd_wal_file_file_list[-1]-mnd_wal_file_file_list[0])/self.taosd_setting["spec"]["dnodes"][0]["config"]["mndSdbWriteDelta"]))):
                self.tdSql.execute('create user u1 pass "u1";')
                self.tdSql.execute('drop user u1;')
        mnd_wal_file_file_list = self.get_wal_file_list()
        self.tdSql.checkEqual(0 not in mnd_wal_file_file_list, True)
        for i in range(len(mnd_wal_file_file_list)-1):
            self.tdSql.checkEqual(mnd_wal_file_file_list[i+1]-mnd_wal_file_file_list[i], self.taosd_setting["spec"]["dnodes"][0]["config"]["mndSdbWriteDelta"])

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            test_td22981
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write