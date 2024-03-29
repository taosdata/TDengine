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

msec_per_min=60*1000
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def diff_query_form(self, col="c1",  alias="", table_expr="db.t1", condition=""):

        '''
        diff function:
        :param col:         string, column name, required parameters;
        :param alias:       string, result column another name，or add other funtion;
        :param table_expr:  string or expression, data source（eg,table/stable name, result set）, required parameters;
        :param condition:   expression；
        :param args:        other funtions,like: ', last(col)',or give result column another name, like 'c2'
        :return:            diff query statement,default: select diff(c1) from t1
        '''

        return f"select diff({col}) {alias} from {table_expr} {condition}"

    def checkdiff(self,col="c1", alias="", table_expr="db.t1", condition="" ):
        line = sys._getframe().f_back.f_lineno
        pre_sql = self.diff_query_form(
            col=col, table_expr=table_expr, condition=condition
        ).replace("diff", "count")
        tdSql.query(pre_sql)

        if tdSql.queryRows == 0:
            tdSql.query(self.diff_query_form(
                col=col, alias=alias, table_expr=table_expr, condition=condition
            ))
            print(f"case in {line}: ", end='')
            tdSql.checkRows(0)
            return

        if "order by tbname" in condition:
            tdSql.query(self.diff_query_form(
                col=col, alias=alias, table_expr=table_expr, condition=condition
            ))
            return

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
                pre_diff = np.diff(pre_data)
                # trans precision for data
                tdSql.query(self.diff_query_form(
                    col=col, alias=alias, table_expr=table_expr, condition=condition
                ))
                for j in range(len(pre_diff)):
                    print(f"case in {line}:", end='')
                    if  isinstance(pre_diff[j] , float) :
                        pass
                    else:
                        tdSql.checkData(pre_row+j, 1, pre_diff[j] )
                pre_row += len(pre_diff)
            return
        elif "union" in condition:
            union_sql_0 = self.diff_query_form(
                col=col, alias=alias, table_expr=table_expr, condition=condition
            ).split("union all")[0]

            union_sql_1 = self.diff_query_form(
                col=col, alias=alias, table_expr=table_expr, condition=condition
            ).split("union all")[1]

            tdSql.query(union_sql_0)
            union_diff_0 = tdSql.queryResult
            row_union_0 = tdSql.queryRows

            tdSql.query(union_sql_1)
            union_diff_1 = tdSql.queryResult

            tdSql.query(self.diff_query_form(
                col=col, alias=alias, table_expr=table_expr, condition=condition
            ))
            for i in range(tdSql.queryRows):
                print(f"case in {line}: ", end='')
                if i < row_union_0:
                    tdSql.checkData(i, 0, union_diff_0[i][0])
                else:
                    tdSql.checkData(i, 0, union_diff_1[i-row_union_0][0])
            return

        else:
            sql = f"select {col} from {table_expr} {re.sub('limit [0-9]*|offset [0-9]*','',condition)}"
            tdSql.query(sql)
            offset_val = condition.split("offset")[1].split(" ")[1] if "offset" in condition else 0
            pre_result = np.array(tdSql.queryResult)[np.array(tdSql.queryResult) != None]
            if (platform.system().lower() == 'windows' and pre_result.dtype == 'int32'):
                pre_result = np.array(pre_result, dtype = 'int64')
            pre_diff = np.diff(pre_result)[offset_val:]
            if len(pre_diff) > 0:
                sql =self.diff_query_form(col=col, alias=alias, table_expr=table_expr, condition=condition)
                tdSql.query(sql)
                j = 0
                diff_cnt = len(pre_diff)
                for i in range(tdSql.queryRows):
                    print(f"case in {line}: i={i} j={j}  pre_diff[j]={pre_diff[j]}  ", end='')
                    if isinstance(pre_diff[j] , float ):
                        if j + 1 < diff_cnt: 
                           j += 1
                        pass
                    else:
                        if tdSql.getData(i,0) != None:
                            tdSql.checkData(i, 0, pre_diff[j])
                            if j + 1 < diff_cnt:
                                j += 1
                        else:
                            print(f"getData i={i} is None j={j} ")
            else:
                print("pre_diff len is zero.")

        pass

    def diff_current_query(self) :

        # table schema :ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool
        #                 c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)

        # case1～6： numeric col:int/bigint/tinyint/smallint/float/double
        self.checkdiff()
        case2 =  {"col": "c2"}
        self.checkdiff(**case2)
        case3 =  {"col": "c5"}
        self.checkdiff(**case3)
        case4 =  {"col": "c7"}
        self.checkdiff(**case4)
        case5 =  {"col": "c8"}
        self.checkdiff(**case5)
        case6 =  {"col": "c9"}
        self.checkdiff(**case6)

        # case7~8: nested query
        # case7 = {"table_expr": "(select c1 from db.stb1)"}
        # self.checkdiff(**case7)
        # case8 = {"table_expr": "(select diff(c1) c1 from db.stb1 group by tbname)"}
        # self.checkdiff(**case8)

        # case9~10: mix with tbname/ts/tag/col
        # case9 = {"alias": ", tbname"}
        # self.checkdiff(**case9)
        # case10 = {"alias": ", _c0"}
        # self.checkdiff(**case10)
        # case11 = {"alias": ", st1"}
        # self.checkdiff(**case11)
        # case12 = {"alias": ", c1"}
        # self.checkdiff(**case12)

        # case13~15: with  single condition
        case13 = {"condition": "where c1 <= 10"}
        self.checkdiff(**case13)
        case14 = {"condition": "where c6 in (0, 1)"}
        self.checkdiff(**case14)
        case15 = {"condition": "where c1 between 1 and 10"}
        self.checkdiff(**case15)

        # case16:  with multi-condition
        case16 = {"condition": "where c6=1 or c6 =0"}
        self.checkdiff(**case16)

        # case17: only support normal table join
        case17 = {
            "col": "table1.c1 ",
            "table_expr": "db.t1 as table1, db.t2 as table2",
            "condition": "where table1.ts=table2.ts"
        }
        self.checkdiff(**case17)
        # case18~19: with group by , function diff not support group by 
    
        case19 = {
            "table_expr": "db.stb1 where tbname =='t0' ",
            "condition": "partition by tbname order by tbname"  # partition by tbname
        }
        self.checkdiff(**case19)

        # case20~21: with order by , Not a single-group group function

        # case22: with union
        # case22 = {
        #     "condition": "union all select diff(c1) from db.t2  "
        # }
        # self.checkdiff(**case22)
        tdSql.query("select count(c1)  from db.t1 union all select count(c1) from db.t2")

        # case23: with limit/slimit
        case23 = {
            "condition": "limit 1"
        }
        self.checkdiff(**case23)
        case24 = {
            "table_expr": "db.stb1",
            "condition": "partition by tbname order by tbname slimit 1 soffset 1"
        }
        self.checkdiff(**case24)

        pass

    def diff_error_query(self) -> None :
        # unusual test
        #
        # table schema :ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool
        #                 c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)
        #
        # form test
        tdSql.error(self.diff_query_form(col=""))   # no col
        tdSql.error("diff(c1) from db.stb1")           # no select
        tdSql.error("select diff from db.t1")          # no diff condition
        tdSql.error("select diff c1 from db.t1")       # no brackets
        tdSql.error("select diff(c1)   db.t1")         # no from
        tdSql.error("select diff( c1 )  from ")     # no table_expr
        # tdSql.error(self.diff_query_form(col="st1"))    # tag col
        tdSql.query("select diff(st1) from db.t1")
        # tdSql.error(self.diff_query_form(col=1))        # col is a value
        tdSql.error(self.diff_query_form(col="'c1'"))   # col is a string
        tdSql.error(self.diff_query_form(col=None))     # col is NULL 1
        tdSql.error(self.diff_query_form(col="NULL"))   # col is NULL 2
        tdSql.error(self.diff_query_form(col='""'))     # col is ""
        tdSql.error(self.diff_query_form(col='c%'))     # col is spercial char 1
        tdSql.error(self.diff_query_form(col='c_'))     # col is spercial char 2
        tdSql.error(self.diff_query_form(col='c.'))     # col is spercial char 3
        tdSql.error(self.diff_query_form(col='avg(c1)'))    # expr col
        # tdSql.error(self.diff_query_form(col='c6'))     # bool col
        tdSql.query("select diff(c6) from db.t1")
        tdSql.error(self.diff_query_form(col='c4'))     # binary col
        tdSql.error(self.diff_query_form(col='c10'))    # nachr col
        tdSql.error(self.diff_query_form(col='c10'))    # not table_expr col
        tdSql.error(self.diff_query_form(col='db.t1'))     # tbname
        tdSql.error(self.diff_query_form(col='db.stb1'))   # stbname
        tdSql.error(self.diff_query_form(col='db'))     # datbasename
        # tdSql.error(self.diff_query_form(col=True))     # col is BOOL 1
        # tdSql.error(self.diff_query_form(col='True'))   # col is BOOL 2
        tdSql.error(self.diff_query_form(col='*'))      # col is all col
        tdSql.error("select diff[c1] from db.t1")          # sql form error 1
        tdSql.error("select diff{c1} from db.t1")          # sql form error 2
        tdSql.error(self.diff_query_form(col="[c1]"))   # sql form error 3
        # tdSql.error(self.diff_query_form(col="c1, c2")) # sql form error 3
        # tdSql.error(self.diff_query_form(col="c1, 2"))  # sql form error 3
        tdSql.error(self.diff_query_form(alias=", count(c1)"))  # mix with aggregate function 1
        tdSql.error(self.diff_query_form(alias=", avg(c1)"))    # mix with aggregate function 2
        tdSql.error(self.diff_query_form(alias=", min(c1)"))    # mix with select function 1
        tdSql.error(self.diff_query_form(alias=", top(c1, 5)")) # mix with select function 2
        tdSql.error(self.diff_query_form(alias=", spread(c1)")) # mix with calculation function  1
        tdSql.query(self.diff_query_form(alias=", diff(c1)"))   # mix with calculation function  2
        # tdSql.error(self.diff_query_form(alias=" + 2"))         # mix with arithmetic 1
        tdSql.error(self.diff_query_form(alias=" + avg(c1)"))   # mix with arithmetic 2
        tdSql.query(self.diff_query_form(alias=", c2"))         # mix with other 1
        # tdSql.error(self.diff_query_form(table_expr="db.stb1"))    # select stb directly
        stb_join = {
            "col": "stable1.c1",
            "table_expr": "db.stb1 as stable1, db.stb2 as stable2",
            "condition": "where stable1.ts=stable2.ts and stable1.st1=stable2.st2 order by stable1.ts"
        }
        tdSql.query(self.diff_query_form(**stb_join))           # stb join
        interval_sql = {
            "condition": "where ts>0 and ts < now interval(1h) fill(next)"
        }
        tdSql.error(self.diff_query_form(**interval_sql))       # interval
        group_normal_col = {
            "table_expr": "db.t1",
            "condition": "group by c6"
        }
        tdSql.error(self.diff_query_form(**group_normal_col))       # group by normal col
        slimit_soffset_sql = {
            "table_expr": "db.stb1",
            "condition": "group by tbname slimit 1 soffset 1"
        }
        # tdSql.error(self.diff_query_form(**slimit_soffset_sql))
        order_by_tbname_sql = {
            "table_expr": "db.stb1",
            "condition": "group by tbname order by tbname"
        }
        tdSql.error(self.diff_query_form(**order_by_tbname_sql))

        pass

    def diff_test_data(self, tbnum:int, data_row:int, basetime:int) -> None :
        for i in range(tbnum):
            for j in range(data_row):
                tdSql.execute(
                    f"insert into db.t{i} values ("
                    f"{basetime + (j+1)*10 + i* msec_per_min}, {random.randint(-200, -1)}, {random.uniform(200, -1)}, {basetime + random.randint(-200, -1)}, "
                    f"'binary_{j}', {random.uniform(-200, -1)}, {random.choice([0,1])}, {random.randint(-200,-1)}, "
                    f"{random.randint(-200, -1)}, {random.randint(-127, -1)}, 'nchar_{j}' )"
                )

                tdSql.execute(
                    f"insert into db.t{i} values ("
                    f"{basetime - (j+1) * 10 + i* msec_per_min}, {random.randint(1, 200)}, {random.uniform(1, 200)}, {basetime - random.randint(1, 200)}, "
                    f"'binary_{j}_1', {random.uniform(1, 200)}, {random.choice([0, 1])}, {random.randint(1,200)}, "
                    f"{random.randint(1,200)}, {random.randint(1,127)}, 'nchar_{j}_1' )"
                )
                tdSql.execute(
                    f"insert into db.tt{i} values ( {basetime-(j+1) * 10 + i* msec_per_min}, {random.randint(1, 200)} )"
                )

        pass

    def diff_test_table(self,tbnum: int) -> None :
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 3650")
        tdSql.execute("use db")

        tdSql.execute(
            "create stable db.stb1 (\
                ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool, \
                c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)\
                ) \
            tags(st1 int)"
        )
        tdSql.execute(
            "create stable db.stb2 (ts timestamp, c1 int) tags(st2 int)"
        )
        for i in range(tbnum):
            tdSql.execute(f"create table db.t{i} using db.stb1 tags({i})")
            tdSql.execute(f"create table db.tt{i} using db.stb2 tags({i})")

        pass
    def diff_support_stable(self):
        tdSql.query(" select diff(1) from db.stb1 ")
        tdSql.checkRows(229)
        tdSql.checkData(0,0,0)
        tdSql.query("select diff(c1) from db.stb1 partition by tbname ")
        tdSql.checkRows(220)
      
        tdSql.query("select diff(st1+c1) from db.stb1 partition by tbname")
        tdSql.checkRows(220)
        tdSql.query("select diff(st1+c1) from db.stb1 partition by tbname")
        tdSql.checkRows(220)
        tdSql.query("select diff(st1+c1) from db.stb1 partition by tbname")
        tdSql.checkRows(220)

        # bug need fix
        tdSql.query("select diff(st1+c1) from db.stb1 partition by tbname")
        tdSql.checkRows(220)

        # bug need fix
        tdSql.query("select tbname , diff(c1) from db.stb1 partition by tbname")
        tdSql.checkRows(220)
        tdSql.query("select tbname , diff(st1) from db.stb1 partition by tbname")
        tdSql.checkRows(220)


        # partition by tags
        tdSql.query("select st1 , diff(c1) from db.stb1 partition by st1")
        tdSql.checkRows(220)
        tdSql.query("select diff(c1) from db.stb1 partition by st1")
        tdSql.checkRows(220)


    def diff_test_run(self) :
        tdLog.printNoPrefix("==========run test case for diff function==========")
        tbnum = 10
        nowtime = int(round(time.time() * 1000))
        per_table_rows = 10
        self.diff_test_table(tbnum)

        tdLog.printNoPrefix("######## no data test:")
        self.diff_current_query()
        self.diff_error_query()

        tdLog.printNoPrefix("######## insert only NULL test:")
        for i in range(tbnum):
            tdSql.execute(f"insert into db.t{i}(ts) values ({nowtime - 5 + i* msec_per_min})")
            tdSql.execute(f"insert into db.t{i}(ts) values ({nowtime + 5 + i* msec_per_min})")
        self.diff_current_query()
        self.diff_error_query()

        tdLog.printNoPrefix("######## insert data in the range near the max(bigint/double):")
        self.diff_test_table(tbnum)
        tdSql.execute(f"insert into db.t1(ts, c1,c2,c5,c7) values "
                      f"({nowtime - (per_table_rows + 1) * 10 + i* msec_per_min}, {2**31-1}, {3.4*10**38}, {1.7*10**308}, {2**63-1})")
        tdSql.execute(f"insert into db.t1(ts, c1,c2,c5,c7) values "
                      f"({nowtime - (per_table_rows + 2) * 10 + i* msec_per_min}, {2**31-1}, {3.4*10**38}, {1.7*10**308}, {2**63-1})")
        self.diff_current_query()
        self.diff_error_query()

        tdLog.printNoPrefix("######## insert data in the range near the min(bigint/double):")
        self.diff_test_table(tbnum)
        tdSql.execute(f"insert into db.t1(ts, c1,c2,c5,c7) values "
                      f"({nowtime - (per_table_rows + 1) * 10 + i* msec_per_min}, {1-2**31}, {-3.4*10**38}, {-1.7*10**308}, {1-2**63})")
        tdSql.execute(f"insert into db.t1(ts, c1,c2,c5,c7) values "
                      f"({nowtime - (per_table_rows + 2) * 10 + i* msec_per_min}, {1-2**31}, {-3.4*10**38}, {-1.7*10**308}, {512-2**63})")
        self.diff_current_query()
        self.diff_error_query()

        tdLog.printNoPrefix("######## insert data without NULL data test:")
        self.diff_test_table(tbnum)
        self.diff_test_data(tbnum, per_table_rows, nowtime)
        self.diff_current_query()
        self.diff_error_query()


        tdLog.printNoPrefix("######## insert data mix with NULL test:")
        for i in range(tbnum):
            tdSql.execute(f"insert into db.t{i}(ts) values ({nowtime + i* msec_per_min})")
            tdSql.execute(f"insert into db.t{i}(ts) values ({nowtime-(per_table_rows+3)*10 + i* msec_per_min})")
            tdSql.execute(f"insert into db.t{i}(ts) values ({nowtime+(per_table_rows+3)*10 + i* msec_per_min})")
        self.diff_current_query()
        self.diff_error_query()



        tdLog.printNoPrefix("######## check after WAL test:")
        tdSql.query("select * from information_schema.ins_dnodes")
        index = tdSql.getData(0, 0)
        tdDnodes.stop(index)
        tdDnodes.start(index)
        self.diff_current_query()
        self.diff_error_query()

    def run(self):
        import traceback
        try:
            # run in  develop branch
            self.diff_test_run()
            self.diff_support_stable()
            pass
        except Exception as e:
            traceback.print_exc()
            raise e


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
