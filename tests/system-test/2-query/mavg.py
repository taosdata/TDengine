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
import taos

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *

dbname = 'db'
msec_per_min = 60 * 1000
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def mavg_query_form(self, sel="select", func="mavg(", col="c1", m_comm =",", k=1,r_comm=")", alias="", fr="from",table_expr=f"{dbname}.t1", condition=""):
        '''
        mavg function:

        :param sel:         string, must be "select", required parameters;
        :param func:        string, in this case must be "mavg(", otherwise return other function, required parameters;
        :param col:         string, column name, required parameters;
        :param m_comm:      string, comma between col and k , required parameters;
        :param k:           int/float，the width of the  sliding window, [1,100], required parameters;
        :param r_comm:      string, must be ")", use with "(" in func, required parameters;
        :param alias:       string, result column another name，or add other funtion;
        :param fr:          string, must be "from", required parameters;
        :param table_expr:  string or expression, data source（eg,table/stable name, result set）, required parameters;
        :param condition:   expression；
        :return:            mavg query statement,default: select mavg(c1, 1) from t1
        '''

        return f"{sel} {func} {col} {m_comm} {k} {r_comm} {alias} {fr} {table_expr} {condition}"

    def checkmavg(self,sel="select", func="mavg(", col="c1", m_comm =",", k=1,r_comm=")", alias="", fr="from",table_expr=f"{dbname}.t1", condition=""):
        # print(self.mavg_query_form(sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
        #                            table_expr=table_expr, condition=condition))
        line = sys._getframe().f_back.f_lineno

        if not all([sel , func , col , m_comm , k , r_comm , fr , table_expr]):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        sql = f"select * from {dbname}.t1"
        collist =  tdSql.getColNameList(sql)

        if not isinstance(col, str):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        if len([x for x in col.split(",") if x.strip()]) != 1:
            print(f"case in {line}: ", end='')
            return tdSql.error(self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        col = col.replace(",", "").replace(" ", "")

        if any([re.compile('^[a-zA-Z]{1}.*$').match(col) is None , not col.replace(".","").isalnum()]):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        # if all(["," in col , len(col.split(",")) != 2]):
        #     print(f"case in {line}: ", end='')
        #     return tdSql.error(self.mavg_query_form(
        #         sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
        #         table_expr=table_expr, condition=condition
        #     ))
        #
        # if ("," in col):
        #     if (not col.split(",")[0].strip()) ^ (not col.split(",")[1].strip()):
        #         col = col.strip().split(",")[0] if not col.split(",")[1].strip() else col.strip().split(",")[1]
        #     else:
        #         print(f"case in {line}: ", end='')
        #         return tdSql.error(self.mavg_query_form(
        #             sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
        #             table_expr=table_expr, condition=condition
        #         ))
        #     pass

        if '.' in col:
            if any([col.split(".")[0] not in table_expr, col.split(".")[1] not in collist]):
                print(f"case in {line}: ", end='')
                return tdSql.error(self.mavg_query_form(
                    sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                    table_expr=table_expr, condition=condition
                ))
            pass

        if "." not in col:
            if col not in  collist:
                print(f"case in {line}: ", end='')
                return tdSql.error(self.mavg_query_form(
                    sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                    table_expr=table_expr, condition=condition
                ))
            pass

        colname = col if "." not in col else col.split(".")[1]
        col_index = collist.index(colname)
        if any([tdSql.cursor.istype(col_index, "TIMESTAMP"), tdSql.cursor.istype(col_index, "BOOL")]):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        if  any([tdSql.cursor.istype(col_index, "BINARY") , tdSql.cursor.istype(col_index,"NCHAR")]):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        if any( [func != "mavg(" , r_comm != ")" , fr != "from",  sel != "select"]):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        if all(["(" not in table_expr, "stb" in table_expr, "group" not in condition.lower()]):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        if "order by tbname" in condition.lower():
            print(f"case in {line}: ", end='')
            return tdSql.error(self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        if all(["group" in condition.lower(), "tbname" not in condition.lower()]):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        alias_list = ["tbname", "_c0", "st", "ts"]
        if all([alias, "," not in alias, not alias.isalnum()]):
            # actually， column alias also support "_"， but in this case，forbidden that。
            print(f"case in {line}: ", end='')
            return tdSql.error(self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        if all([alias, "," in alias]):
            if  all(parm != alias.lower().split(",")[1].strip() for parm in alias_list):
                print(f"case in {line}: ", end='')
                return tdSql.error(self.mavg_query_form(
                    sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                    table_expr=table_expr, condition=condition
                ))
            pass

        condition_exception = [ "~", "^", "insert", "distinct",
                           "count", "avg", "twa", "irate", "sum", "stddev", "leastquares",
                           "min", "max", "first", "last", "top", "bottom", "percentile",
                           "apercentile", "last_row", "interp", "diff", "derivative",
                           "spread", "ceil", "floor", "round", "interval", "fill", "slimit", "soffset"]
        if "union" not in condition.lower():
            if any(parm in condition.lower().strip() for parm in condition_exception):

                print(f"case in {line}: ", end='')
                return tdSql.error(self.mavg_query_form(
                    sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                    table_expr=table_expr, condition=condition
                ))
            pass

        if not any([isinstance(k, int) ,  isinstance(k, float)])  :
            print(f"case in {line}: ", end='')
            return tdSql.error(self.mavg_query_form(
                col=col, k=k, alias=alias, table_expr=table_expr, condition=condition
            ))

        if not(1 <= k < 1001):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.mavg_query_form(
                col=col, k=k, alias=alias, table_expr=table_expr, condition=condition
            ))

        k = int(k // 1)
        pre_sql = re.sub("mavg\([a-z0-9 .,]*\)", f"count({col})", self.mavg_query_form(
             col=col, table_expr=table_expr, condition=condition
        ))
        tdSql.query(pre_sql)

        if tdSql.queryRows == 0:
            tdSql.query(self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))
            print(f"case in {line}: ", end='')
            tdSql.checkRows(0)
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
                pre_mavg = np.convolve(pre_data, np.ones(k), "valid")/k
                tdSql.query(self.mavg_query_form(
                    sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                    table_expr=table_expr, condition=condition
                ))
                for j in range(len(pre_mavg)):
                    print(f"case in {line}:", end='')
                    tdSql.checkData(pre_row+j, 0, pre_mavg[j])
                pre_row += len(pre_mavg)
            return
        elif "union" in condition:
            union_sql_0 = self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ).split("union all")[0]

            union_sql_1 = self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ).split("union all")[1]

            tdSql.query(union_sql_0)
            union_mavg_0 = tdSql.queryResult
            row_union_0 = tdSql.queryRows

            tdSql.query(union_sql_1)
            union_mavg_1 = tdSql.queryResult

            tdSql.query(self.mavg_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))
            for i in range(tdSql.queryRows):
                print(f"case in {line}: ", end='')
                if i < row_union_0:
                    tdSql.checkData(i, 0, union_mavg_0[i][0])
                else:
                    tdSql.checkData(i, 0, union_mavg_1[i-row_union_0][0])
            return

        else:
            tdSql.query(f"select {col} from {table_expr} {re.sub('limit [0-9]*|offset [0-9]*','',condition)}")
            offset_val = condition.split("offset")[1].split(" ")[1] if "offset" in condition else 0
            # print(f"select {col} from {table_expr} {re.sub('limit [0-9]*|offset [0-9]*','',condition)}")
            if not tdSql.queryResult:
                pre_result = np.array(tdSql.queryResult)[np.array(tdSql.queryResult) != None]
                if (platform.system().lower() == 'windows' and pre_result.dtype == 'int32'):
                    pre_result = np.array(pre_result, dtype = 'int64')

                #pre_mavg = pre_mavg = np.convolve(pre_result, np.ones(k), "valid")[offset_val:]/k
                tdSql.query(self.mavg_query_form(
                    sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                    table_expr=table_expr, condition=condition
                ))
                for i in range(tdSql.queryRows):
                    print(f"case in {line}: ", end='')
                    tdSql.checkData(i, 0, pre_mavg[i])

        pass

    def mavg_current_query(self, dbname="db") :

        # table schema :ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool
        #                 c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)

        # case1～6： numeric col:int/bigint/tinyint/smallint/float/double
        self.checkmavg()
        case2 =  {"col": "c2"}
        self.checkmavg(**case2)
        case3 =  {"col": "c5"}
        self.checkmavg(**case3)
        case4 =  {"col": "c7"}
        self.checkmavg(**case4)
        case5 =  {"col": "c8"}
        self.checkmavg(**case5)
        case6 =  {"col": "c9"}
        self.checkmavg(**case6)

        # case7~8: nested query
        case7 = {"table_expr": f"(select ts, c1 from {dbname}.stb1)"}
        self.checkmavg(**case7)
        # case8 = {"table_expr": f"(select _c0, mavg(c1, 1) c1 from {dbname}.stb1 group by tbname)"}
        # self.checkmavg(**case8)

        # case9~10: mix with tbname/ts/tag/col
        case9 = {"alias": ", tbname"}
        self.checkmavg(**case9)
        case10 = {"alias": ", _c0"}
        self.checkmavg(**case10)
        # case11 = {"alias": ", st1"}
        # self.checkmavg(**case11)
        # case12 = {"alias": ", c1"}
        # self.checkmavg(**case12)

        # case13~15: with  single condition
        case13 = {"condition": "where c1 <= 10"}
        self.checkmavg(**case13)
        case14 = {"condition": "where c6 in (0, 1)"}
        self.checkmavg(**case14)
        case15 = {"condition": "where c1 between 1 and 10"}
        self.checkmavg(**case15)

        # case16:  with multi-condition
        case16 = {"condition": "where c6=1 or c6 =0"}
        self.checkmavg(**case16)

        # case17: only support normal table join
        case17 = {
            "col": "t1.c1",
            "table_expr": f"{dbname}.t1 t1, {dbname}.t2 t2",
            "condition": "where t1.ts=t2.ts"
        }
        self.checkmavg(**case17)
        # # case18~19: with group by
        # case19 = {
        #     "table_expr": f"{dbname}.stb1",
        #     "condition": "partition by tbname"
        # }
        # self.checkmavg(**case19)

        # # case20~21: with order by
        # case20 = {"condition": "order by ts"}
        # self.checkmavg(**case20)
        case21 = {
           "table_expr": f"{dbname}.stb1",
           "condition": "group by tbname order by tbname"
        }
        self.checkmavg(**case21)

        # # case22: with union
        # case22 = {
        #     "condition": f"union all select mavg( c1 , 1 ) from {dbname}.t2"
        # }
        # self.checkmavg(**case22)

        # case23: with limit/slimit
        case23 = {
            "condition": "limit 1"
        }
        self.checkmavg(**case23)

        # case24: value k range[1, 100], can be int or float, k = floor(k)
        case24 = {"k": 3}
        self.checkmavg(**case24)
        case25 = {"k": 2.999}
        self.checkmavg(**case25)
        case26 = {"k": 1000}
        self.checkmavg(**case26)

        pass

    def mavg_error_query(self, dbname="db") -> None :
        # unusual test

        # form test
        err1 =  {"col": ""}
        self.checkmavg(**err1)          # no col
        err2 = {"sel": ""}
        self.checkmavg(**err2)          # no select
        err3 = {"func": "mavg", "col": "", "m_comm": "", "k": "", "r_comm": ""}
        self.checkmavg(**err3)          # no mavg condition: select mavg from
        err4 = {"col": "", "m_comm": "", "k": ""}
        self.checkmavg(**err4)          # no mavg condition: select mavg() from
        err5 = {"func": "mavg", "r_comm": ""}
        self.checkmavg(**err5)          # no brackets: select mavg col, k from
        err6 = {"fr": ""}
        self.checkmavg(**err6)          # no from
        err7 = {"k": ""}
        self.checkmavg(**err7)          # no k
        err8 = {"table_expr": ""}
        self.checkmavg(**err8)          # no table_expr

        err9 = {"col": "st1"}
        # self.checkmavg(**err9)          # col: tag
        err10 = {"col": 1}
        # self.checkmavg(**err10)         # col: value
        err11 = {"col": "NULL"}
        self.checkmavg(**err11)         # col: NULL
        err12 = {"col": "%_"}
        self.checkmavg(**err12)         # col: %_
        err13 = {"col": "c3"}
        self.checkmavg(**err13)         # col: timestamp col
        err14 = {"col": "_c0"}
        self.checkmavg(**err14)         # col: Primary key
        err15 = {"col": "avg(c1)"}
        self.checkmavg(**err15)         # expr col
        err16 = {"col": "c4"}
        self.checkmavg(**err16)         # binary col
        err17 = {"col": "c10"}
        self.checkmavg(**err17)         # nchar col
        err18 = {"col": "c6"}
        self.checkmavg(**err18)         # bool col
        err19 = {"col": "'c1'"}
        self.checkmavg(**err19)         # col: string
        err20 = {"col": None}
        self.checkmavg(**err20)         # col: None
        err21 = {"col": "''"}
        self.checkmavg(**err21)         # col: ''
        err22 = {"col": "tt1.c1"}
        self.checkmavg(**err22)         # not table_expr col
        err23 = {"col": "t1"}
        self.checkmavg(**err23)         # tbname
        err24 = {"col": "stb1"}
        self.checkmavg(**err24)         # stbname
        err25 = {"col": "db"}
        self.checkmavg(**err25)         # datbasename
        err26 = {"col": "True"}
        self.checkmavg(**err26)         # col: BOOL 1
        err27 = {"col": True}
        self.checkmavg(**err27)         # col: BOOL 2
        err28 = {"col": "*"}
        self.checkmavg(**err28)         # col: all col
        err29 = {"func": "mavg[", "r_comm": "]"}
        self.checkmavg(**err29)         # form: mavg[col, k]
        err30 = {"func": "mavg{", "r_comm": "}"}
        self.checkmavg(**err30)         # form: mavg{col, k}
        err31 = {"col": "[c1]"}
        self.checkmavg(**err31)         # form: mavg([col], k)
        err32 = {"col": "c1, c2"}
        self.checkmavg(**err32)         # form: mavg(col, col2, k)
        err33 = {"col": "c1, 2"}
        self.checkmavg(**err33)         # form: mavg(col, k1, k2)
        err34 = {"alias": ", count(c1)"}
        self.checkmavg(**err34)         # mix with aggregate function 1
        err35 = {"alias": ", avg(c1)"}
        self.checkmavg(**err35)         # mix with aggregate function 2
        err36 = {"alias": ", min(c1)"}
        self.checkmavg(**err36)         # mix with select function 1
        err37 = {"alias": ", top(c1, 5)"}
        self.checkmavg(**err37)         # mix with select function 2
        err38 = {"alias": ", spread(c1)"}
        self.checkmavg(**err38)         # mix with calculation function  1
        err39 = {"alias": ", diff(c1)"}
        self.checkmavg(**err39)         # mix with calculation function  2
        # err40 = {"alias": "+ 2"}
        # self.checkmavg(**err40)         # mix with arithmetic 1
        #tdSql.query(" select mavg( c1 , 1 ) + 2 from t1 ")
        err41 = {"alias": "+ avg(c1)"}
        self.checkmavg(**err41)         # mix with arithmetic 2
        # err42 = {"alias": ", c1"}
        # self.checkmavg(**err42)         # mix with other col
        # err43 = {"table_expr": f"{dbname}.stb1"}
        # self.checkmavg(**err43)         # select stb directly
        # err44 = {
        #     "col": "stb1.c1",
        #     "table_expr": "stb1, stb2",
        #     "condition": "where stb1.ts=stb2.ts and stb1.st1=stb2.st2 order by stb1.ts"
        # }
        # self.checkmavg(**err44)         # stb join
        tdSql.query(f"select mavg( stb1.c1 , 1 )  from {dbname}.stb1 stb1, {dbname}.stb2 stb2 where stb1.ts=stb2.ts and stb1.st1=stb2.st2 order by stb1.ts;")
        err45 = {
            "condition": "where ts>0 and ts < now interval(1h) fill(next)"
        }
        self.checkmavg(**err45)         # interval
        err46 = {
            "table_expr": f"{dbname}.t1",
            "condition": "group by c6"
        }
        self.checkmavg(**err46)         # group by normal col
        err47 = {
            "table_expr": f"{dbname}.stb1",
            "condition": "group by tbname slimit 1 "
        }
        # self.checkmavg(**err47)         # with slimit
        err48 = {
            "table_expr": f"{dbname}.stb1",
            "condition": "group by tbname slimit 1 soffset 1"
        }
        # self.checkmavg(**err48)         # with soffset
        err49 = {"k": "2021-01-01 00:00:00.000"}
        self.checkmavg(**err49)         # k: timestamp
        err50 = {"k": False}
        self.checkmavg(**err50)         # k: False
        err51 = {"k": "%"}
        self.checkmavg(**err51)         # k: special char
        err52 = {"k": ""}
        self.checkmavg(**err52)         # k: ""
        err53 = {"k": None}
        self.checkmavg(**err53)         # k: None
        err54 = {"k": "NULL"}
        self.checkmavg(**err54)         # k: null
        err55 = {"k": "binary(4)"}
        self.checkmavg(**err55)         # k: string
        err56 = {"k": "c1"}
        self.checkmavg(**err56)         # k: sring，col name
        err57 = {"col": "c1, 1, c2"}
        self.checkmavg(**err57)         # form: mavg(col1, k1, col2, k2)
        err58 = {"col": "c1 cc1"}
        self.checkmavg(**err58)         # form: mavg(col newname, k)
        err59 = {"k": "'1'"}
        # self.checkmavg(**err59)         # formL mavg(colm, "1")
        err60 = {"k": "-1-(-2)"}
        # self.checkmavg(**err60)         # formL mavg(colm, -1-2)
        err61 = {"k": 1001}
        self.checkmavg(**err61)         # k: right out of [1, 1000]
        err62 = {"k": -1}
        self.checkmavg(**err62)         # k: negative number
        err63 = {"k": 0}
        self.checkmavg(**err63)         # k: 0
        err64 = {"k": 2**63-1}
        self.checkmavg(**err64)         # k: max(bigint)
        err65 = {"k": 1-2**63}
        # self.checkmavg(**err65)         # k: min(bigint)
        err66 = {"k": -2**63}
        self.checkmavg(**err66)         # k: NULL
        err67 = {"k": 0.999999}
        self.checkmavg(**err67)         # k: left out of [1, 1000]
        err68 = {
            "table_expr": f"{dbname}.stb1",
            "condition": f"group by tbname order by tbname" # order by tbname not supported
        }
        self.checkmavg(**err68)

        pass

    def mavg_test_data(self, tbnum:int, data_row:int, basetime:int) -> None :
        for i in range(tbnum):
            for j in range(data_row):
                tdSql.execute(
                    f"insert into {dbname}.t{i} values ("
                    f"{basetime + (j+1)*10 + i * msec_per_min}, {random.randint(-200, -1)}, {random.uniform(200, -1)}, {basetime + random.randint(-200, -1)}, "
                    f"'binary_{j}', {random.uniform(-200, -1)}, {random.choice([0,1])}, {random.randint(-200,-1)}, "
                    f"{random.randint(-200, -1)}, {random.randint(-127, -1)}, 'nchar_{j}' )"
                )

                tdSql.execute(
                    f"insert into {dbname}.t{i} values ("
                    f"{basetime - (j+1) * 10 + i * msec_per_min}, {random.randint(1, 200)}, {random.uniform(1, 200)}, {basetime - random.randint(1, 200)}, "
                    f"'binary_{j}_1', {random.uniform(1, 200)}, {random.choice([0, 1])}, {random.randint(1,200)}, "
                    f"{random.randint(1,200)}, {random.randint(1,127)}, 'nchar_{j}_1' )"
                )
                tdSql.execute(
                    f"insert into {dbname}.tt{i} values ( {basetime-(j+1) * 10 + i * msec_per_min}, {random.randint(1, 200)} )"
                )

        pass

    def mavg_test_table(self,tbnum: int) -> None :
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database  if not exists {dbname} keep 3650")
        tdSql.execute(f"use {dbname}")

        tdSql.execute(
            f"create stable {dbname}.stb1 (\
                ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool, \
                c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)\
                ) \
            tags(st1 int)"
        )
        tdSql.execute(
            f"create stable {dbname}.stb2 (ts timestamp, c1 int) tags(st2 int)"
        )
        for i in range(tbnum):
            tdSql.execute(f"create table {dbname}.t{i} using {dbname}.stb1 tags({i})")
            tdSql.execute(f"create table {dbname}.tt{i} using {dbname}.stb2 tags({i})")

        pass

    def mavg_test_run(self) :
        tdLog.printNoPrefix("==========TD-10594==========")
        tbnum = 10
        nowtime = int(round(time.time() * 1000))
        per_table_rows = 2
        self.mavg_test_table(tbnum)

        tdLog.printNoPrefix("######## no data test:")
        self.mavg_current_query()
        self.mavg_error_query()

        tdLog.printNoPrefix("######## insert only NULL test:")
        for i in range(tbnum):
            tdSql.execute(f"insert into {dbname}.t{i}(ts) values ({nowtime - 5 + i * msec_per_min})")
            tdSql.execute(f"insert into {dbname}.t{i}(ts) values ({nowtime + 5 + i * msec_per_min})")
        self.mavg_current_query()
        self.mavg_error_query()

        tdLog.printNoPrefix("######## insert data in the range near the max(bigint/double):")
        # self.mavg_test_table(tbnum)
        # tdSql.execute(f"insert into {dbname}.t1(ts, c1,c2,c5,c7) values "
        #               f"({nowtime - (per_table_rows + 1) * 10}, {2**31-1}, {3.4*10**38}, {1.7*10**308}, {2**63-1})")
        # tdSql.execute(f"insert into {dbname}.t1(ts, c1,c2,c5,c7) values "
        #               f"({nowtime - (per_table_rows + 2) * 10}, {2**31-1}, {3.4*10**38}, {1.7*10**308}, {2**63-1})")
        # self.mavg_current_query()
        # self.mavg_error_query()

        tdLog.printNoPrefix("######## insert data in the range near the min(bigint/double):")
        # self.mavg_test_table(tbnum)
        # tdSql.execute(f"insert into {dbname}.t1(ts, c1,c2,c5,c7) values "
        #               f"({nowtime - (per_table_rows + 1) * 10}, {1-2**31}, {-3.4*10**38}, {-1.7*10**308}, {1-2**63})")
        # tdSql.execute(f"insert into {dbname}.t1(ts, c1,c2,c5,c7) values "
        #               f"({nowtime - (per_table_rows + 2) * 10}, {1-2**31}, {-3.4*10**38}, {-1.7*10**308}, {512-2**63})")
        # self.mavg_current_query()
        # self.mavg_error_query()

        tdLog.printNoPrefix("######## insert data without NULL data test:")
        self.mavg_test_table(tbnum)
        self.mavg_test_data(tbnum, per_table_rows, nowtime)
        self.mavg_current_query()
        self.mavg_error_query()


        tdLog.printNoPrefix("######## insert data mix with NULL test:")
        for i in range(tbnum):
            tdSql.execute(f"insert into {dbname}.t{i}(ts) values ({nowtime + i * msec_per_min})")
            tdSql.execute(f"insert into {dbname}.t{i}(ts) values ({nowtime-(per_table_rows+3)*10 + i * msec_per_min})")
            tdSql.execute(f"insert into {dbname}.t{i}(ts) values ({nowtime+(per_table_rows+3)*10 + i * msec_per_min})")
        self.mavg_current_query()
        self.mavg_error_query()



        tdLog.printNoPrefix("######## check after WAL test:")
        tdSql.query("select * from information_schema.ins_dnodes")
        index = tdSql.getData(0, 0)
        tdDnodes.stop(index)
        tdDnodes.start(index)
        self.mavg_current_query()
        self.mavg_error_query()
        tdSql.query(f"select mavg(1,1) from {dbname}.t1")
        tdSql.checkRows(7)
        tdSql.checkData(0,0,1.000000000)
        tdSql.checkData(1,0,1.000000000)
        tdSql.checkData(5,0,1.000000000)

        tdSql.query(f"select mavg(abs(c1),1) from {dbname}.t1")
        tdSql.checkRows(4)

    def mavg_support_stable(self):
        tdSql.query(f"select mavg(1,3) from {dbname}.stb1 ")
        tdSql.checkRows(68)
        tdSql.checkData(0,0,1.000000000)
        tdSql.query(f"select mavg(c1,3) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(20)
        tdSql.query(f"select mavg(st1,3) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(50)
        tdSql.query(f"select mavg(st1+c1,3) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(20)
        tdSql.query(f"select mavg(st1+c1,3) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(20)
        tdSql.query(f"select mavg(st1+c1,3) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(20)



        # bug need fix
        tdSql.query(f"select mavg(st1+c1,3) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(20)

        # bug need fix
        tdSql.query(f"select tbname , mavg(c1,3) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(20)
        tdSql.query(f"select tbname , mavg(st1,3) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(50)
        tdSql.query(f"select tbname , mavg(st1,3) from {dbname}.stb1 partition by tbname slimit 1")
        tdSql.checkRows(5)

        # partition by tags
        tdSql.query(f"select st1 , mavg(c1,3) from {dbname}.stb1 partition by st1")
        tdSql.checkRows(20)
        tdSql.query(f"select mavg(c1,3) from {dbname}.stb1 partition by st1")
        tdSql.checkRows(20)
        tdSql.query(f"select st1 , mavg(c1,3) from {dbname}.stb1 partition by st1 slimit 1")
        tdSql.checkRows(2)
        tdSql.query(f"select mavg(c1,3) from {dbname}.stb1 partition by st1 slimit 1")
        tdSql.checkRows(2)

        # partition by col
        tdSql.query(f"select c1 , mavg(c1,1) from {dbname}.stb1 partition by c1")
        tdSql.checkRows(40)
        tdSql.query(f"select c1, c2, c3, c4, mavg(c1,3) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(20)
        tdSql.query(f"select c1, c2, c3, c4, mavg(123,3) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(50)


    def run(self):
        import traceback
        try:
            # run in  develop branch
            self.mavg_test_run()
            self.mavg_support_stable()
            pass
        except Exception as e:
            traceback.print_exc()
            raise e


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
