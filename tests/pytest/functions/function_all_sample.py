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


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def sample_query_form(self, sel="select", func="sample(", col="c1", m_comm =",", k=1,r_comm=")", alias="", fr="from",table_expr="t1", condition=""):
        '''
        sample function:

        :param sel:         string, must be "select", required parameters;
        :param func:        string, in this case must be "sample(", otherwise return other function, required parameters;
        :param col:         string, column name, required parameters;
        :param m_comm:      string, comma between col and k , required parameters;
        :param k:           int/float，the width of the  sliding window, [1,100], required parameters;
        :param r_comm:      string, must be ")", use with "(" in func, required parameters;
        :param alias:       string, result column another name，or add other funtion;
        :param fr:          string, must be "from", required parameters;
        :param table_expr:  string or expression, data source（eg,table/stable name, result set）, required parameters;
        :param condition:   expression；
        :return:            sample query statement,default: select sample(c1, 1) from t1
        '''

        return f"{sel} {func} {col} {m_comm} {k} {r_comm} {alias} {fr} {table_expr} {condition}"

    def checksample(self,sel="select", func="sample(", col="c1", m_comm =",", k=1,r_comm=")", alias="", fr="from",table_expr="t1", condition=""):
        # print(self.sample_query_form(sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
        #                            table_expr=table_expr, condition=condition))
        line = sys._getframe().f_back.f_lineno

        if not all([sel , func , col , m_comm , k , r_comm , fr , table_expr]):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.sample_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))


        sql = "select * from t1"
        collist =  tdSql.getColNameList(sql)

        if not isinstance(col, str):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.sample_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        if len([x for x in col.split(",") if x.strip()]) != 1:
            print(f"case in {line}: ", end='')
            return tdSql.error(self.sample_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        col = col.replace(",", "").replace(" ","")

        if any([re.compile('^[a-zA-Z]{1}.*$').match(col) is None , not col.replace(".","").isalnum()]):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.sample_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        if '.' in col:
            if any([col.split(".")[0] not in table_expr, col.split(".")[1] not in collist]):
                print(f"case in {line}: ", end='')
                return tdSql.error(self.sample_query_form(
                    sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                    table_expr=table_expr, condition=condition
                ))
            pass

        if "." not in col:
            if col not in  collist:
                print(f"case in {line}: ", end='')
                return tdSql.error(self.sample_query_form(
                    sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                    table_expr=table_expr, condition=condition
                ))
            pass

        # colname = col if "." not in col else col.split(".")[1]
        # col_index = collist.index(colname)
        # if any([tdSql.cursor.istype(col_index, "TIMESTAMP"), tdSql.cursor.istype(col_index, "BOOL")]):
        #     print(f"case in {line}: ", end='')
        #     return tdSql.error(self.sample_query_form(
        #         sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
        #         table_expr=table_expr, condition=condition
        #     ))
        #
        # if  any([tdSql.cursor.istype(col_index, "BINARY") , tdSql.cursor.istype(col_index,"NCHAR")]):
        #     print(f"case in {line}: ", end='')
        #     return tdSql.error(self.sample_query_form(
        #         sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
        #         table_expr=table_expr, condition=condition
        #     ))

        if any( [func != "sample(" , r_comm != ")" , fr != "from",  sel != "select"]):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.sample_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        if all(["(" not in table_expr, "stb" in table_expr, "group" not in condition.lower()]):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.sample_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        if all(["group" in condition.lower(), "tbname" not in condition.lower()]):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.sample_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))

        alias_list = ["tbname", "_c0", "st", "ts"]
        if all([alias, "," not in alias]):
            if any([ not alias.isalnum(), re.compile('^[a-zA-Z]{1}.*$').match(col) is None ]):
                # actually， column alias also support "_"， but in this case，forbidden that。
                print(f"case in {line}: ", end='')
                return tdSql.error(self.sample_query_form(
                    sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                    table_expr=table_expr, condition=condition
                ))

        if all([alias, "," in alias]):
            if  all(parm != alias.lower().split(",")[1].strip() for parm in alias_list):
                print(f"case in {line}: ", end='')
                return tdSql.error(self.sample_query_form(
                    sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                    table_expr=table_expr, condition=condition
                ))
            pass

        condition_exception = [ "-", "+", "/", "*", "~", "^", "insert", "distinct",
                           "count", "avg", "twa", "irate", "sum", "stddev", "leastquares",
                           "min", "max", "first", "last", "top", "bottom", "percentile",
                           "apercentile", "last_row", "interp", "diff", "derivative",
                           "spread", "ceil", "floor", "round", "interval", "fill", "slimit", "soffset"]
        if "union" not in condition.lower():
            if any(parm in condition.lower().strip() for parm in condition_exception):

                print(f"case in {line}: ", end='')
                return tdSql.error(self.sample_query_form(
                    sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                    table_expr=table_expr, condition=condition
                ))
            pass

        if not any([isinstance(k, int) ,  isinstance(k, float)])  :
            print(f"case in {line}: ", end='')
            return tdSql.error(self.sample_query_form(
                col=col, k=k, alias=alias, table_expr=table_expr, condition=condition
            ))

        if not(1 <= k < 1001):
            print(f"case in {line}: ", end='')
            return tdSql.error(self.sample_query_form(
                col=col, k=k, alias=alias, table_expr=table_expr, condition=condition
            ))

        k = int(k // 1)
        pre_sql = re.sub("sample\([a-z0-9 .,]*\)", f"count({col})", self.sample_query_form(
             col=col, table_expr=table_expr, condition=condition
        ))
        tdSql.query(pre_sql)
        if tdSql.queryRows == 0:
            tdSql.query(self.sample_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))
            print(f"case in {line}: ", end='')
            tdSql.checkRows(0)
            return

        tdSql.query(self.sample_query_form(
            sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
            table_expr=table_expr, condition=condition
        ))

        sample_result = tdSql.queryResult
        sample_len = tdSql.queryRows

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
                    pre_condition = re.sub('group by [0-9a-z]*', f"and {tb_condition}='{group_name}' and {col} is not null", clear_condition)
                else:
                    pre_condition = "where " + re.sub('group by [0-9a-z]*',f"{tb_condition}='{group_name}' and {col} is not null", clear_condition)

                tdSql.query(f"select ts, {col} {alias} from {table_expr} {pre_condition}")
                # pre_data = np.array(tdSql.queryResult)[np.array(tdSql.queryResult) != None]
                # pre_sample = np.convolve(pre_data, np.ones(k), "valid")/k
                pre_sample = tdSql.queryResult
                pre_len = tdSql.queryRows
                step = pre_len   if pre_len < k else k
                # tdSql.query(self.sample_query_form(
                #     sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                #     table_expr=table_expr, condition=condition
                # ))
                for i in range(step):
                    if sample_result[pre_row:pre_row+step][i] not in pre_sample:
                        tdLog.exit(f"case in {line} is failed: sample data is not in {group_name}")
                    else:
                        tdLog.info(f"case in {line} is success: sample data is in {group_name}")

                # for j in range(len(pre_sample)):
                #     print(f"case in {line}:", end='')
                #     tdSql.checkData(pre_row+j, 1, pre_sample[j])
                pre_row += step
            return
        elif "union" in condition:
            union_sql_0 = self.sample_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ).split("union all")[0]

            union_sql_1 = self.sample_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ).split("union all")[1]

            tdSql.query(union_sql_0)
            # union_sample_0 = tdSql.queryResult
            row_union_0 = tdSql.queryRows

            tdSql.query(union_sql_1)
            # union_sample_1 = tdSql.queryResult
            row_union_1 = tdSql.queryRows

            tdSql.query(self.sample_query_form(
                sel=sel, func=func, col=col, m_comm=m_comm, k=k, r_comm=r_comm, alias=alias, fr=fr,
                table_expr=table_expr, condition=condition
            ))
            # for i in range(tdSql.queryRows):
            #     print(f"case in {line}: ", end='')
            #     if i < row_union_0:
            #         tdSql.checkData(i, 1, union_sample_0[i][1])
            #     else:
            #         tdSql.checkData(i, 1, union_sample_1[i-row_union_0][1])
            if row_union_0 + row_union_1 != sample_len:
                tdLog.exit(f"case in {line} is failed: sample data is not in {group_name}")
            else:
                tdLog.info(f"case in {line} is success: sample data is in {group_name}")
            return

        else:
            if "where" in condition:
                condition = re.sub('where',  f"where  {col} is not null and ", condition)
            else:
                condition = f"where {col} is not null" + condition
            tdSql.query(f"select ts, {col}, {alias} from {table_expr} {re.sub('limit [0-9]*|offset [0-9]*','',condition)}")
            # offset_val = condition.split("offset")[1].split(" ")[1] if "offset" in condition else 0
            pre_sample = tdSql.queryResult
            # pre_len = tdSql.queryRows
            for i in range(sample_len):
                if sample_result[pre_row:pre_row + step][i] not in pre_sample:
                    tdLog.exit(f"case in {line} is failed: sample data is not in {group_name}")
                else:
                    tdLog.info(f"case in {line} is success: sample data is in {group_name}")

        pass

    def sample_current_query(self) :

        # table schema :ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool
        #                 c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)

        # case1～6： numeric col:int/bigint/tinyint/smallint/float/double
        self.checksample()
        case2 =  {"col": "c2"}
        self.checksample(**case2)
        case3 =  {"col": "c5"}
        self.checksample(**case3)
        case4 =  {"col": "c7"}
        self.checksample(**case4)
        case5 =  {"col": "c8"}
        self.checksample(**case5)
        case6 =  {"col": "c9"}
        self.checksample(**case6)

        # case7~8: nested query
        case7 = {"table_expr": "(select c1 from stb1)"}
        self.checksample(**case7)
        case8 = {"table_expr": "(select sample(c1, 1) c1 from stb1 group by tbname)"}
        self.checksample(**case8)

        # case9~10: mix with tbname/ts/tag/col
        # case9 = {"alias": ", tbname"}
        # self.checksample(**case9)
        case10 = {"alias": ", _c0"}
        self.checksample(**case10)
        # case11 = {"alias": ", st1"}
        # self.checksample(**case11)
        # case12 = {"alias": ", c1"}
        # self.checksample(**case12)

        # case13~15: with  single condition
        case13 = {"condition": "where c1 <= 10"}
        self.checksample(**case13)
        case14 = {"condition": "where c6 in (0, 1)"}
        self.checksample(**case14)
        case15 = {"condition": "where c1 between 1 and 10"}
        self.checksample(**case15)

        # case16:  with multi-condition
        case16 = {"condition": "where c6=1 or c6 =0"}
        self.checksample(**case16)

        # case17: only support normal table join
        case17 = {
            "col": "t1.c1",
            "table_expr": "t1, t2",
            "condition": "where t1.ts=t2.ts"
        }
        self.checksample(**case17)
        # # case18~19: with group by
        case19 = {
            "table_expr": "stb1",
            "condition": "group by tbname"
        }
        self.checksample(**case19)

        # case20~21: with order by
        case20 = {"condition": "order by ts"}
        self.checksample(**case20)
        case21 = {
            "table_expr": "stb1",
            "condition": "group by tbname order by tbname"
        }
        self.checksample(**case21)

        # case22: with union
        case22 = {
            "condition": "union all select sample( c1 , 1 ) from t2"
        }
        self.checksample(**case22)

        # case23: with limit/slimit
        case23 = {
            "condition": "limit 1"
        }
        self.checksample(**case23)

        # case24: value k range[1, 100], can be int or float, k = floor(k)
        case24 = {"k": 3}
        self.checksample(**case24)
        case25 = {"k": 2.999}
        self.checksample(**case25)
        case26 = {"k": 1000}
        self.checksample(**case26)
        case27 = {
            "table_expr": "stb1",
            "condition": "group by tbname slimit 1 "
        }
        self.checksample(**case27)         # with slimit
        case28 = {
            "table_expr": "stb1",
            "condition": "group by tbname slimit 1 soffset 1"
        }
        self.checksample(**case28)         # with soffset

        pass

    def sample_error_query(self) -> None :
        # unusual test

        # form test
        err1 =  {"col": ""}
        self.checksample(**err1)          # no col
        err2 = {"sel": ""}
        self.checksample(**err2)          # no select
        err3 = {"func": "sample", "col": "", "m_comm": "", "k": "", "r_comm": ""}
        self.checksample(**err3)          # no sample condition: select sample from
        err4 = {"col": "", "m_comm": "", "k": ""}
        self.checksample(**err4)          # no sample condition: select sample() from
        err5 = {"func": "sample", "r_comm": ""}
        self.checksample(**err5)          # no brackets: select sample col, k from
        err6 = {"fr": ""}
        self.checksample(**err6)          # no from
        err7 = {"k": ""}
        self.checksample(**err7)          # no k
        err8 = {"table_expr": ""}
        self.checksample(**err8)          # no table_expr

        err9 = {"col": "st1"}
        self.checksample(**err9)          # col: tag
        err10 = {"col": 1}
        self.checksample(**err10)         # col: value
        err11 = {"col": "NULL"}
        self.checksample(**err11)         # col: NULL
        err12 = {"col": "%_"}
        self.checksample(**err12)         # col: %_
        err13 = {"col": "c3"}
        self.checksample(**err13)         # col: timestamp col
        err14 = {"col": "_c0"}
        # self.checksample(**err14)         # col: Primary key
        err15 = {"col": "avg(c1)"}
        # self.checksample(**err15)         # expr col
        err16 = {"col": "c4"}
        self.checksample(**err16)         # binary col
        err17 = {"col": "c10"}
        self.checksample(**err17)         # nchar col
        err18 = {"col": "c6"}
        self.checksample(**err18)         # bool col
        err19 = {"col": "'c1'"}
        self.checksample(**err19)         # col: string
        err20 = {"col": None}
        self.checksample(**err20)         # col: None
        err21 = {"col": "''"}
        self.checksample(**err21)         # col: ''
        err22 = {"col": "tt1.c1"}
        self.checksample(**err22)         # not table_expr col
        err23 = {"col": "t1"}
        self.checksample(**err23)         # tbname
        err24 = {"col": "stb1"}
        self.checksample(**err24)         # stbname
        err25 = {"col": "db"}
        self.checksample(**err25)         # datbasename
        err26 = {"col": "True"}
        self.checksample(**err26)         # col: BOOL 1
        err27 = {"col": True}
        self.checksample(**err27)         # col: BOOL 2
        err28 = {"col": "*"}
        self.checksample(**err28)         # col: all col
        err29 = {"func": "sample[", "r_comm": "]"}
        self.checksample(**err29)         # form: sample[col, k]
        err30 = {"func": "sample{", "r_comm": "}"}
        self.checksample(**err30)         # form: sample{col, k}
        err31 = {"col": "[c1]"}
        self.checksample(**err31)         # form: sample([col], k)
        err32 = {"col": "c1, c2"}
        self.checksample(**err32)         # form: sample(col, col2, k)
        err33 = {"col": "c1, 2"}
        self.checksample(**err33)         # form: sample(col, k1, k2)
        err34 = {"alias": ", count(c1)"}
        self.checksample(**err34)         # mix with aggregate function 1
        err35 = {"alias": ", avg(c1)"}
        self.checksample(**err35)         # mix with aggregate function 2
        err36 = {"alias": ", min(c1)"}
        self.checksample(**err36)         # mix with select function 1
        err37 = {"alias": ", top(c1, 5)"}
        self.checksample(**err37)         # mix with select function 2
        err38 = {"alias": ", spread(c1)"}
        self.checksample(**err38)         # mix with calculation function  1
        err39 = {"alias": ", diff(c1)"}
        self.checksample(**err39)         # mix with calculation function  2
        err40 = {"alias": "+ 2"}
        self.checksample(**err40)         # mix with arithmetic 1
        err41 = {"alias": "+ avg(c1)"}
        self.checksample(**err41)         # mix with arithmetic 2
        err42 = {"alias": ", c1"}
        self.checksample(**err42)         # mix with other col
        err43 = {"table_expr": "stb1"}
        self.checksample(**err43)         # select stb directly
        err44 = {
            "col": "stb1.c1",
            "table_expr": "stb1, stb2",
            "condition": "where stb1.ts=stb2.ts and stb1.st1=stb2.st2 order by stb1.ts"
        }
        self.checksample(**err44)         # stb join
        err45 = {
            "condition": "where ts>0 and ts < now interval(1h) fill(next)"
        }
        self.checksample(**err45)         # interval
        err46 = {
            "table_expr": "t1",
            "condition": "group by c6"
        }
        # self.checksample(**err46)         # group by normal col

        err49 = {"k": "2021-01-01 00:00:00.000"}
        self.checksample(**err49)         # k: timestamp
        err50 = {"k": False}
        self.checksample(**err50)         # k: False
        err51 = {"k": "%"}
        self.checksample(**err51)         # k: special char
        err52 = {"k": ""}
        self.checksample(**err52)         # k: ""
        err53 = {"k": None}
        self.checksample(**err53)         # k: None
        err54 = {"k": "NULL"}
        self.checksample(**err54)         # k: null
        err55 = {"k": "binary(4)"}
        self.checksample(**err55)         # k: string
        err56 = {"k": "c1"}
        self.checksample(**err56)         # k: sring，col name
        err57 = {"col": "c1, 1, c2"}
        self.checksample(**err57)         # form: sample(col1, k1, col2, k2)
        err58 = {"col": "c1 cc1"}
        self.checksample(**err58)         # form: sample(col newname, k)
        err59 = {"k": "'1'"}
        # self.checksample(**err59)         # formL sample(colm, "1")
        err60 = {"k": "-1-(-2)"}
        # self.checksample(**err60)         # formL sample(colm, -1-2)
        err61 = {"k": 1001}
        self.checksample(**err61)         # k: right out of [1, 1000]
        err62 = {"k": -1}
        self.checksample(**err62)         # k: negative number
        err63 = {"k": 0}
        self.checksample(**err63)         # k: 0
        err64 = {"k": 2**63-1}
        self.checksample(**err64)         # k: max(bigint)
        err65 = {"k": 1-2**63}
        # self.checksample(**err65)         # k: min(bigint)
        err66 = {"k": -2**63}
        self.checksample(**err66)         # k: NULL
        err67 = {"k": 0.999999}
        self.checksample(**err67)         # k: left out of [1, 1000]

        pass

    def sample_test_data(self, tbnum:int, data_row:int, basetime:int) -> None :
        for i in range(tbnum):
            for j in range(data_row):
                tdSql.execute(
                    f"insert into t{i} values ("
                    f"{basetime + (j+1)*10}, {random.randint(-200, -1)}, {random.uniform(200, -1)}, {basetime + random.randint(-200, -1)}, "
                    f"'binary_{j}', {random.uniform(-200, -1)}, {random.choice([0,1])}, {random.randint(-200,-1)}, "
                    f"{random.randint(-200, -1)}, {random.randint(-127, -1)}, 'nchar_{j}' )"
                )

                tdSql.execute(
                    f"insert into t{i} values ("
                    f"{basetime - (j+1) * 10}, {random.randint(1, 200)}, {random.uniform(1, 200)}, {basetime - random.randint(1, 200)}, "
                    f"'binary_{j}_1', {random.uniform(1, 200)}, {random.choice([0, 1])}, {random.randint(1,200)}, "
                    f"{random.randint(1,200)}, {random.randint(1,127)}, 'nchar_{j}_1' )"
                )
                tdSql.execute(
                    f"insert into tt{i} values ( {basetime-(j+1) * 10}, {random.randint(1, 200)} )"
                )

        pass

    def sample_test_table(self,tbnum: int) -> None :
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
            tdSql.execute(f"create table t{i} using stb1 tags({i})")
            tdSql.execute(f"create table tt{i} using stb2 tags({i})")

        pass

    def sample_test_run(self) :
        tdLog.printNoPrefix("==========TD-10594==========")
        tbnum = 10
        nowtime = int(round(time.time() * 1000))
        per_table_rows = 100
        self.sample_test_table(tbnum)

        tdLog.printNoPrefix("######## no data test:")
        self.sample_current_query()
        self.sample_error_query()

        tdLog.printNoPrefix("######## insert only NULL test:")
        for i in range(tbnum):
            tdSql.execute(f"insert into t{i}(ts) values ({nowtime - 5})")
            tdSql.execute(f"insert into t{i}(ts) values ({nowtime + 5})")
        self.sample_current_query()
        self.sample_error_query()

        tdLog.printNoPrefix("######## insert data in the range near the max(bigint/double):")
        # self.sample_test_table(tbnum)
        # tdSql.execute(f"insert into t1(ts, c1,c2,c5,c7) values "
        #               f"({nowtime - (per_table_rows + 1) * 10}, {2**31-1}, {3.4*10**38}, {1.7*10**308}, {2**63-1})")
        # tdSql.execute(f"insert into t1(ts, c1,c2,c5,c7) values "
        #               f"({nowtime - (per_table_rows + 2) * 10}, {2**31-1}, {3.4*10**38}, {1.7*10**308}, {2**63-1})")
        # self.sample_current_query()
        # self.sample_error_query()

        tdLog.printNoPrefix("######## insert data in the range near the min(bigint/double):")
        # self.sample_test_table(tbnum)
        # tdSql.execute(f"insert into t1(ts, c1,c2,c5,c7) values "
        #               f"({nowtime - (per_table_rows + 1) * 10}, {1-2**31}, {-3.4*10**38}, {-1.7*10**308}, {1-2**63})")
        # tdSql.execute(f"insert into t1(ts, c1,c2,c5,c7) values "
        #               f"({nowtime - (per_table_rows + 2) * 10}, {1-2**31}, {-3.4*10**38}, {-1.7*10**308}, {512-2**63})")
        # self.sample_current_query()
        # self.sample_error_query()

        tdLog.printNoPrefix("######## insert data without NULL data test:")
        self.sample_test_table(tbnum)
        self.sample_test_data(tbnum, per_table_rows, nowtime)
        self.sample_current_query()
        self.sample_error_query()


        tdLog.printNoPrefix("######## insert data mix with NULL test:")
        for i in range(tbnum):
            tdSql.execute(f"insert into t{i}(ts) values ({nowtime})")
            tdSql.execute(f"insert into t{i}(ts) values ({nowtime-(per_table_rows+3)*10})")
            tdSql.execute(f"insert into t{i}(ts) values ({nowtime+(per_table_rows+3)*10})")
        self.sample_current_query()
        self.sample_error_query()



        tdLog.printNoPrefix("######## check after WAL test:")
        tdSql.query("show dnodes")
        index = tdSql.getData(0, 0)
        tdDnodes.stop(index)
        tdDnodes.start(index)
        self.sample_current_query()
        self.sample_error_query()

    def run(self):
        import traceback
        try:
            # run in  develop branch
            self.sample_test_run()
            pass
        except Exception as e:
            traceback.print_exc()
            raise e

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
