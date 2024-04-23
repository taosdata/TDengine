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
import subprocess
import random
import math
import numpy as np
import inspect
import re

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *

msec_per_min=60 * 1000
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def csum_query_form(self, col="c1",  alias="", table_expr="db.t1", condition=""):

        '''
        csum function:
        :param col:         string, column name, required parameters;
        :param alias:       string, result column another name，or add other funtion;
        :param table_expr:  string or expression, data source（eg,table/stable name, result set）, required parameters;
        :param condition:   expression；
        :param args:        other funtions,like: ', last(col)',or give result column another name, like 'c2'
        :return:            csum query statement,default: select csum(c1) from t1
        '''

        return f"select csum({col}) {alias} from {table_expr} {condition}"

    def checkcsum(self,col="c1", alias="", table_expr="db.t1", condition="" ):
        line = sys._getframe().f_back.f_lineno
        pre_sql = self.csum_query_form(
            col=col, table_expr=table_expr, condition=condition
        ).replace("csum", "count")
        tdSql.query(pre_sql)

        if tdSql.queryRows == 0:
            tdSql.query(self.csum_query_form(
                col=col, alias=alias, table_expr=table_expr.replace("csum", "ts, csum"), condition=condition
            ))
            print(f"case in {line}: ", end='')
            tdSql.checkRows(0)
            return

        # if "order by tbname" in condition:
        #     tdSql.error(self.csum_query_form(
        #         col=col, alias=alias, table_expr=table_expr, condition=condition
        #     ))
        #     return

        if "group" in condition:

            tb_condition = condition.split("group by")[1].split(" ")[1]
            tdSql.query(f"select distinct {tb_condition} from {table_expr}")
            query_result = tdSql.queryResult
            query_rows = tdSql.queryRows
            clear_condition = re.sub('order by [0-9a-z]*|slimit [0-9]*|soffset [0-9]*', "", condition)

            pre_row = 0
            for i in range(query_rows):
                group_name = query_result[i][0]
                if "where" in clear_condition:
                    pre_condition = re.sub('group by [0-9a-z]*', f"{tb_condition}='{group_name}'", clear_condition)
                else:
                    pre_condition = "where " + re.sub('group by [0-9a-z]*',f"{tb_condition}='{group_name}'", clear_condition)

                tdSql.query(f"select {col} {alias} from {table_expr} {pre_condition}")
                pre_data = np.array(tdSql.queryResult)[np.array(tdSql.queryResult) != None]
                if (platform.system().lower() == 'windows' and pre_data.dtype == 'int32'):
                    pre_data = np.array(pre_data, dtype = 'int64')
                print("data is ", pre_data)
                pre_csum = np.cumsum(pre_data)
                tdSql.query(self.csum_query_form(
                    col=col, alias=alias, table_expr=table_expr, condition=condition
                ))
                for j in range(len(pre_csum)):
                    print(f"case in {line}:", end='')
                    tdSql.checkData(pre_row+j, 1, pre_csum[j])
                pre_row += len(pre_csum)
            return
        elif "union" in condition:
            union_sql_0 = self.csum_query_form(
                col=col, alias=alias, table_expr=table_expr, condition=condition
            ).split("union all")[0]

            union_sql_1 = self.csum_query_form(
                col=col, alias=alias, table_expr=table_expr, condition=condition
            ).split("union all")[1]

            tdSql.query(union_sql_0)
            union_csum_0 = tdSql.queryResult
            row_union_0 = tdSql.queryRows

            tdSql.query(union_sql_1)
            union_csum_1 = tdSql.queryResult

            tdSql.query(self.csum_query_form(
                col=col, alias=alias, table_expr=table_expr, condition=condition
            ))
            for i in range(tdSql.queryRows):
                print(f"case in {line}: ", end='')
                if i < row_union_0:
                    tdSql.checkData(i, 0, union_csum_0[i][0])
                else:
                    tdSql.checkData(i, 0, union_csum_1[i-row_union_0][0])
            return

        else:

            tdSql.query(f"select {col} from {table_expr} {re.sub('limit [0-9]*|offset [0-9]*','',condition)} " )
            offset_val = condition.split("offset")[1].split(" ")[1] if "offset" in condition else 0
            pre_result = np.array(tdSql.queryResult)[np.array(tdSql.queryResult) != None]
            if (platform.system().lower() == 'windows' and pre_result.dtype == 'int32'):
                pre_result = np.array(pre_result, dtype = 'int64')
            pre_csum = np.cumsum(pre_result)[offset_val:]
            tdSql.query(self.csum_query_form(
                col=col, alias=alias, table_expr=table_expr.replace("csum", "ts,csum"), condition=condition
            ))

            for i in range(tdSql.queryRows):
                print(f"case in {line}: ", end='')
                if pre_csum[i] >1.7e+308 or pre_csum[i] < -1.7e+308:
                    continue
                else:
                    tdSql.checkData(i, 0, pre_csum[i])

        pass

    def csum_current_query(self) :

        # table schema :ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool
        #                 c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)

        # case1～6： numeric col:int/bigint/tinyint/smallint/float/double
        self.checkcsum()
        case2 =  {"col": "c2"}
        self.checkcsum(**case2)
        case3 =  {"col": "c5"}
        self.checkcsum(**case3)
        case4 =  {"col": "c7"}
        self.checkcsum(**case4)
        case5 =  {"col": "c8"}
        self.checkcsum(**case5)
        case6 =  {"col": "c9"}
        self.checkcsum(**case6)

        # unsigned check
        case61 =  {"col": "c11"}
        self.checkcsum(**case61)        
        case62 =  {"col": "c12"}
        self.checkcsum(**case62)        
        case63 =  {"col": "c13"}
        self.checkcsum(**case63)        
        case64 =  {"col": "c14"}
        self.checkcsum(**case64)

        # case7~8: nested query
        case7 = {"table_expr": "(select ts,c1 from db.stb1 order by ts, tbname )"}
        self.checkcsum(**case7)
        case8 = {"table_expr": "(select csum(c1) c1 from db.t1)"}
        self.checkcsum(**case8)

        # case9~10: mix with tbname/ts/tag/col not support , must partition by alias  ,such as select tbname ,csum(c1) partition by tbname
        # case9 = {"alias": ", tbname"}
        # self.checkcsum(**case9)
        # case10 = {"alias": ", _c0"}
        # self.checkcsum(**case10)
        # case11 = {"alias": ", st1"}
        # self.checkcsum(**case11)
        # case12 = {"alias": ", c1"}
        # self.checkcsum(**case12)

        # case13~15: with  single condition
        case13 = {"condition": "where c1 <= 10"}
        self.checkcsum(**case13)
        case14 = {"condition": "where c6 in (0, 1)"}
        self.checkcsum(**case14)
        case15 = {"condition": "where c1 between 1 and 10"}
        self.checkcsum(**case15)

        # case16:  with multi-condition
        case16 = {"condition": "where c6=1 or c6 =0"}
        self.checkcsum(**case16)

        # case17: only support normal table join
        case17 = {
            "col": "t1.c1",
            "table_expr": "t1, t2",
            "condition": "where t1.ts=t2.ts"
        }
        self.checkcsum(**case17)
        # case18~19: with group by
        case18 = {
            "table_expr": "db.t1",
            "condition": "where c6 <0 partition by c6 order by c6"
        }
        self.checkcsum(**case18)
        case19 = {
            "table_expr": "db.t1",
            "condition": " "  # partition by tbname
        }
        self.checkcsum(**case19)

        # case20~21: with order by
        case20 = {"condition": "partition by tbname order by tbname  "}
        # self.checkcsum(**case20) # order by without order by tbname ,because check will random failed

        # case22: with union
        case22 = {
            "condition": "union all select csum(c1) from db.t2"
        }
        # self.checkcsum(**case22) union all without check result becasue ,it will random return table_records

        # case23: with limit/slimit
        case23 = {
            "condition": "limit 1"
        }
        self.checkcsum(**case23)
        case24 = {
            "table_expr": "db.t1",
            "condition": "partition by tbname "
        }
        self.checkcsum(**case24)

        pass

    def csum_error_query(self) -> None :
        # unusual test
        #
        # table schema :ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool
        #                 c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)
        #
        # form test
        tdSql.error(self.csum_query_form(col=""))   # no col
        tdSql.error("csum(c1) from stb1")           # no select
        tdSql.error("select csum from t1")          # no csum condition
        tdSql.error("select csum c1 from t1")       # no brackets
        tdSql.error("select csum(c1)   t1")         # no from
        tdSql.error("select csum( c1 )  from ")     # no table_expr
        # tdSql.error(self.csum_query_form(col="st1"))    # tag col
        # tdSql.error(self.csum_query_form(col=1))        # col is a value
        tdSql.error(self.csum_query_form(col="'c1'"))   # col is a string
        tdSql.error(self.csum_query_form(col=None))     # col is NULL 1
        tdSql.error(self.csum_query_form(col="NULL"))   # col is NULL 2
        tdSql.error(self.csum_query_form(col='""'))     # col is ""
        tdSql.error(self.csum_query_form(col='c%'))     # col is spercial char 1
        tdSql.error(self.csum_query_form(col='c_'))     # col is spercial char 2
        tdSql.error(self.csum_query_form(col='c.'))     # col is spercial char 3
        tdSql.error(self.csum_query_form(col='c3'))     # timestamp col
        tdSql.error(self.csum_query_form(col='ts'))     # Primary key
        tdSql.error(self.csum_query_form(col='avg(c1)'))    # expr col
        tdSql.error(self.csum_query_form(col='c6'))     # bool col
        tdSql.error(self.csum_query_form(col='c4'))     # binary col
        tdSql.error(self.csum_query_form(col='c10'))    # nachr col
        tdSql.error(self.csum_query_form(col='c10'))    # not table_expr col
        tdSql.error(self.csum_query_form(col='t1'))     # tbname
        tdSql.error(self.csum_query_form(col='stb1'))   # stbname
        tdSql.error(self.csum_query_form(col='db'))     # datbasename
        tdSql.error(self.csum_query_form(col=True))     # col is BOOL 1
        tdSql.error(self.csum_query_form(col='True'))   # col is BOOL 2
        tdSql.error(self.csum_query_form(col='*'))      # col is all col
        tdSql.error("select csum[c1] from t1")          # sql form error 1
        tdSql.error("select csum{c1} from t1")          # sql form error 2
        tdSql.error(self.csum_query_form(col="[c1]"))   # sql form error 3
        # tdSql.error(self.csum_query_form(col="c1, c2")) # sql form error 3
        # tdSql.error(self.csum_query_form(col="c1, 2"))  # sql form error 3
        tdSql.error(self.csum_query_form(alias=", count(c1)"))  # mix with aggregate function 1
        tdSql.error(self.csum_query_form(alias=", avg(c1)"))    # mix with aggregate function 2
        tdSql.error(self.csum_query_form(alias=", min(c1)"))    # mix with select function 1
        tdSql.error(self.csum_query_form(alias=", top(c1, 5)")) # mix with select function 2
        tdSql.error(self.csum_query_form(alias=", spread(c1)")) # mix with calculation function  1
        tdSql.error(self.csum_query_form(alias=", diff(c1)"))   # mix with calculation function  2
        # tdSql.error(self.csum_query_form(alias=" + 2"))         # mix with arithmetic 1
        tdSql.error(self.csum_query_form(alias=" + avg(c1)"))   # mix with arithmetic 2
        # tdSql.error(self.csum_query_form(alias=", c2"))         # mix with other 1
        # tdSql.error(self.csum_query_form(table_expr="stb1"))    # select stb directly
        #stb_join = {
        #    "col": "stb1.c1",
        #    "table_expr": "stb1, stb2",
        #    "condition": "where stb1.ts=stb2.ts and stb1.st1=stb2.st2 order by stb1.ts"
        #}
        #tdSql.error(self.csum_query_form(**stb_join))           # stb join
        interval_sql = {
            "condition": "where ts>0 and ts < now interval(1h) fill(next)"
        }
        tdSql.error(self.csum_query_form(**interval_sql))       # interval
        group_normal_col = {
            "table_expr": "db.t1",
            "condition": "group by c6"
        }
        tdSql.error(self.csum_query_form(**group_normal_col))       # group by normal col
        slimit_soffset_sql = {
            "table_expr": "db.stb1",
            "condition": "group by tbname slimit 1 soffset 1"
        }
        # tdSql.error(self.csum_query_form(**slimit_soffset_sql))
        order_by_tbname_sql = {
            "table_expr": "db.stb1",
            "condition": "group by tbname order by tbname"
        }
        tdSql.error(self.csum_query_form(**order_by_tbname_sql))

        pass

    def csum_test_data(self, tbnum:int, data_row:int, basetime:int) -> None :
        for i in range(tbnum):
            for j in range(data_row):
                tdSql.execute(
                    f"insert into t{i} values ("
                    f"{basetime + (j+1)*10 + i * msec_per_min}, {random.randint(-200, -1)}, {random.uniform(200, -1)}, {basetime + random.randint(-200, -1)}, "
                    f"'binary_{j}', {random.uniform(-200, -1)}, {random.choice([0,1])}, {random.randint(-200,-1)}, "
                    f"{random.randint(-200, -1)}, {random.randint(-127, -1)}, 'nchar_{j}', {j},{j},{j},{j} )"
                )

                tdSql.execute(
                    f"insert into t{i} values ("
                    f"{basetime - (j+1) * 10 + i * msec_per_min}, {random.randint(1, 200)}, {random.uniform(1, 200)}, {basetime - random.randint(1, 200)}, "
                    f"'binary_{j}_1', {random.uniform(1, 200)}, {random.choice([0, 1])}, {random.randint(1,200)}, "
                    f"{random.randint(1,200)}, {random.randint(1,127)}, 'nchar_{j}_1', {j*2},{j*2},{j*2},{j*2} )"
                )
                tdSql.execute(
                    f"insert into tt{i} values ( {basetime-(j+1) * 10 + i * msec_per_min}, {random.randint(1, 200)} )"
                )

        pass

    def csum_test_table(self,tbnum: int) -> None :
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 3650")
        tdSql.execute("use db")

        tdSql.execute(
            "create stable db.stb1 (\
                ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool, \
                c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16),\
                c11 int unsigned, c12 smallint unsigned, c13 tinyint unsigned, c14 bigint unsigned) \
            tags(st1 int)"
        )
        tdSql.execute(
            "create stable db.stb2 (ts timestamp, c1 int) tags(st2 int)"
        )
        for i in range(tbnum):
            tdSql.execute(f"create table db.t{i} using db.stb1 tags({i})")
            tdSql.execute(f"create table db.tt{i} using db.stb2 tags({i})")

        pass

    def csum_test_run(self) :
        tdLog.printNoPrefix("==========TD-10594==========")
        tbnum = 10
        nowtime = int(round(time.time() * 1000))
        per_table_rows = 2
        self.csum_test_table(tbnum)

        tdLog.printNoPrefix("######## no data test:")
        self.csum_current_query()
        self.csum_error_query()

        tdLog.printNoPrefix("######## insert only NULL test:")
        for i in range(tbnum):
            tdSql.execute(f"insert into db.t{i}(ts) values ({nowtime - 5 + i * msec_per_min})")
            tdSql.execute(f"insert into db.t{i}(ts) values ({nowtime + 5 + i * msec_per_min})")
        self.csum_current_query()
        self.csum_error_query()

        tdLog.printNoPrefix("######## insert data in the range near the max(bigint/double):")
        self.csum_test_table(tbnum)
        tdSql.execute(f"insert into db.t1(ts, c1,c2,c5,c7,c11) values "
                      f"({nowtime - (per_table_rows + 1) * 10 + i * msec_per_min}, {2**31-1}, {3.4*10**38}, {1.7*10**308}, {2**63-1}, 128)")
        tdSql.execute(f"insert into db.t1(ts, c1,c2,c5,c7,c11) values "
                      f"({nowtime - (per_table_rows + 2) * 10 + i * msec_per_min}, {2**31-1}, {3.4*10**38}, {1.7*10**308}, {2**63-1}, 129)")
        self.csum_current_query()
        self.csum_error_query()

        tdLog.printNoPrefix("######## insert data in the range near the min(bigint/double):")
        self.csum_test_table(tbnum)
        tdSql.execute(f"insert into db.t1(ts, c1,c2,c5,c7) values "
                      f"({nowtime - (per_table_rows + 1) * 10 + i * msec_per_min}, {1-2**31}, {-3.4*10**38}, {-1.7*10**308}, {1-2**63})")
        tdSql.execute(f"insert into db.t1(ts, c1,c2,c5,c7) values "
                      f"({nowtime - (per_table_rows + 2) * 10 + i * msec_per_min}, {1-2**31}, {-3.4*10**38}, {-1.7*10**308}, {512-2**63})")
        self.csum_current_query()
        self.csum_error_query()

        tdLog.printNoPrefix("######## insert data without NULL data test:")
        self.csum_test_table(tbnum)
        self.csum_test_data(tbnum, per_table_rows, nowtime)
        self.csum_current_query()
        self.csum_error_query()


        tdLog.printNoPrefix("######## insert data mix with NULL test:")
        for i in range(tbnum):
            tdSql.execute(f"insert into db.t{i}(ts) values ({nowtime + i * msec_per_min})")
            tdSql.execute(f"insert into db.t{i}(ts) values ({nowtime-(per_table_rows+3)*10 + i * msec_per_min})")
            tdSql.execute(f"insert into db.t{i}(ts) values ({nowtime+(per_table_rows+3)*10 + i * msec_per_min})")
        self.csum_current_query()
        self.csum_error_query()



        tdLog.printNoPrefix("######## check after WAL test:")
        tdSql.query("select * from information_schema.ins_dnodes")
        index = tdSql.getData(0, 0)
        tdDnodes.stop(index)
        tdDnodes.start(index)
        self.csum_current_query()
        self.csum_error_query()
        tdSql.query("select csum(1) from db.t1 ")
        tdSql.checkRows(7)
        tdSql.checkData(0,0,1)
        tdSql.checkData(1,0,2)
        tdSql.checkData(2,0,3)
        tdSql.checkData(3,0,4)
        tdSql.query("select csum(abs(c1))+2 from db.t1 ")
        tdSql.checkRows(4)

        # support selectivity
        tdSql.query("select ts, c1, csum(1) from db.t1")
        tdSql.checkRows(7)

        tdSql.query("select csum(1), ts, c1 from db.t1")
        tdSql.checkRows(7)

        tdSql.query("select ts, c1, c2, c3, csum(1), ts, c4, c5, c6 from db.t1")
        tdSql.checkRows(7)

        tdSql.query("select ts, c1, csum(1), c4, c5, csum(1), c6 from db.t1")
        tdSql.checkRows(7)

    def csum_support_stable(self):
        tdSql.query(" select csum(1) from db.stb1 ")
        tdSql.checkRows(70)
        tdSql.query("select csum(c1) from db.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.query("select csum(st1) from db.stb1 partition by tbname")
        tdSql.checkRows(70)
        tdSql.query("select csum(st1+c1) from db.stb1 partition by tbname")
        tdSql.checkRows(40)
        tdSql.query("select csum(st1+c1) from db.stb1 partition by tbname")
        tdSql.checkRows(40)
        tdSql.query("select csum(st1+c1) from db.stb1 partition by tbname")
        tdSql.checkRows(40)

        # # bug need fix
        tdSql.query("select csum(st1+c1) from db.stb1 partition by tbname slimit 1 ")
        tdSql.checkRows(4)
        # tdSql.error("select csum(st1+c1) from db.stb1 partition by tbname limit 1 ")


        # bug need fix
        tdSql.query("select csum(st1+c1) from db.stb1 partition by tbname")
        tdSql.checkRows(40)

        # bug need fix
        tdSql.query("select tbname , csum(c1), csum(c12) from db.stb1 partition by tbname")
        tdSql.checkRows(40)
        tdSql.query("select tbname , csum(st1) from db.stb1 partition by tbname")
        tdSql.checkRows(70)
        tdSql.query("select tbname , csum(st1) from db.stb1 partition by tbname slimit 1")
        tdSql.checkRows(7)

        # partition by tags
        tdSql.query("select st1 , csum(c1), csum(c13) from db.stb1 partition by st1")
        tdSql.checkRows(40)
        tdSql.query("select csum(c1) from db.stb1 partition by st1")
        tdSql.checkRows(40)
        tdSql.query("select st1 , csum(c1) from db.stb1 partition by st1 slimit 1")
        tdSql.checkRows(4)
        tdSql.query("select csum(c1) from db.stb1 partition by st1 slimit 1")
        tdSql.checkRows(4)

        # partition by col
        # tdSql.query("select c1 , csum(c1) from db.stb1 partition by c1")
        # tdSql.checkRows(41)
        # tdSql.query("select csum(c1) from db.stb1 partition by c1")
        # tdSql.checkRows(41)
        # tdSql.query("select c1 , csum(c1) from db.stb1 partition by st1 slimit 1")
        # tdSql.checkRows(4)
        # tdSql.query("select csum(c1) from db.stb1 partition by st1 slimit 1")
        # tdSql.checkRows(4)



    def run(self):
        import traceback
        try:
            # run in  develop branch
            self.csum_test_run()
            self.csum_support_stable()
            pass
        except Exception as e:
            traceback.print_exc()
            raise e



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
