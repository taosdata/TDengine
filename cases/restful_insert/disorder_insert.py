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
from taostest.util.rest import TDRest
import random

class TestDisorderInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest(env_setting=self.env_setting)

    def disorder_insert(self):
        """
        disorder_insert
        """
        dbname = self.tdCom.get_long_name(length=5, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname} precision "ms"')
        self.tdRest.request(f'create table {dbname}.stb (ts timestamp, c11 int, c12 float ) TAGS(t11 int, t12 int )')
        self.tdRest.request(f'create table {dbname}.tb using {dbname}.stb TAGS(1, 1)')
        timestamp = self.tdCom.genTs("ms", protype="restful")[0]
        ts_list = list()
        for i in range(1, 101):
            ts = timestamp - 1000 + i
            ts_list.append(ts)
        random.shuffle(ts_list)
        for ts in ts_list:
            sql = f'insert into {dbname}.tb values ({ts}, 1, 1)'
            self.tdRest.request(sql)
        self.tdRest.request(f'select count(*) from {dbname}.tb')
        self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][0]), 100)

    def run(self):
        self.disorder_insert()

    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            disorder_insert <jayden>: [TD-12748] : disorder insert;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.RestfulSql.Insert.Disorder