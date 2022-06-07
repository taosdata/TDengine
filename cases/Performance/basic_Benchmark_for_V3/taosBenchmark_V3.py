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
import os ,sys
class TestBigintBoundary(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def bigint_boundary_check(self):
        """
        max: +- 9223372036854775807
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        
        self.tdSql.execute(f'drop database if exists {dbname}')

    def run(self):
        self.bigint_boundary_check()
        case_path = os.path.realpath(__file__)
        len_case = len(case_path.split("/")[-1])
        case_dir = case_path[:len(case_path)-len_case]
        print(case_dir)
        host = self.get_component_by_name(
            "taosd")[0]['fqdn'][0]
        print(host)
        ret = self.envMgr._remote.cmd2(host ,["hostname","taos -s 'show dnodes;'", f"taosBenchmark -f  {case_dir}pre_datas_insert.json"])

        if ret.failed:
            print("prepare data done ! ")
            sys.exit(ret)

        for _ in range(10):
            # basic aggregate query for 3.0 branch 
            ret = self.envMgr._remote.cmd2(host ,["hostname","taos -s 'show dnodes;'",  f"taosBenchmark -f  {case_dir}basic_agg_query.json"])

            if ret.failed:
                print("basic aggregate done ! ")
                sys.exit(ret)

            # basic long query for 3.0 branch 

            ret = self.envMgr._remote.cmd2(host ,["hostname","taos -s 'show dnodes;'" , f"taosBenchmark -f  {case_dir}basic_long_query.json"])

            if ret.failed:
                print("basic long done ! ")
                sys.exit(ret)

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bigint_boundary_check <jayden>: [TD-12748] : bigint boundary check (max 9223372036854775807);
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Insert.BoundaryTest.Bigint
