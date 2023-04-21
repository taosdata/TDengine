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

class TestTs3236(TDCase):
    def init(self):
        pass

    def run(self):
        self.tdSql.execute('create database if not exists perf_test replica 1 vgroups 4 stt_trigger 1;')
        self.tdSql.execute('use perf_test;')
        self.tdSql.execute('CREATE TABLE perf_test.stb0 (ts TIMESTAMP,c0 int,c1 float,c2 float) TAGS (t0 tinyint,t1 binary(16));')
        self.tdSql.execute('alter local "querySmaOptimize" "1";')
        self.tdSql.execute('create sma index if not exists tsma_test on stb0 function(min(c0),max(c1),sum(c2),first(c0),last(c1),avg(c0),count(c1)) interval(10s) max_delay 5s;')
        self.tdSql.execute('show databases')
    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts3236
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write