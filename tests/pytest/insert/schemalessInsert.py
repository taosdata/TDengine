###################################################################
#           Copyright (c) 2021 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import traceback
import random
import string
from taos.error import LinesError
import time
from copy import deepcopy
import numpy as np
from util.log import *
from util.cases import *
from util.sql import *
import threading


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn 

    def createDb(self, name="test", db_update_tag=0):
        if db_update_tag == 0:
            tdSql.execute(f"drop database if exists {name}")
            tdSql.execute(f"create database if not exists {name} precision 'us'")
        else:
            tdSql.execute(f"drop database if exists {name}")
            tdSql.execute(f"create database if not exists {name} precision 'us' update 1")
        tdSql.execute(f'use {name}')

    def getLongName(self, len, mode = "mixed"):
        """
            generate long name
            mode could be numbers/letters/mixed
        """    
        if mode is "numbers": 
            chars = ''.join(random.choice(string.digits) for i in range(len))
        elif mode is "letters": 
            chars = ''.join(random.choice(string.ascii_letters.lower()) for i in range(len))
        else:
            chars = ''.join(random.choice(string.ascii_letters.lower() + string.digits) for i in range(len))
        return chars

    def timeTrans(self, time_value):
        if time_value.endswith("ns"):
            ts = int(''.join(list(filter(str.isdigit, time_value))))/1000000000
        elif time_value.endswith("us") or time_value.isdigit() and int(time_value) != 0:
            ts = int(''.join(list(filter(str.isdigit, time_value))))/1000000
        elif time_value.endswith("ms"):
            ts = int(''.join(list(filter(str.isdigit, time_value))))/1000
        elif time_value.endswith("s") and list(time_value)[-1] not in "num":
            ts = int(''.join(list(filter(str.isdigit, time_value))))/1
        elif int(time_value) == 0:
            ts = time.time()
        else:
            print("input ts maybe not right format")
        ulsec = repr(ts).split('.')[1][:6]
        if len(ulsec) < 6 and int(ulsec) != 0:
            ulsec = int(ulsec) * (10 ** (6 - len(ulsec)))
        elif int(ulsec) == 0:
            ulsec *= 6
            # * follow two rows added for tsCheckCase
            td_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
            return td_ts
        #td_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
        td_ts = time.strftime("%Y-%m-%d %H:%M:%S.{}".format(ulsec), time.localtime(ts))
        return td_ts
        #return repr(datetime.datetime.strptime(td_ts, "%Y-%m-%d %H:%M:%S.%f"))
    
    def dateToTs(self, datetime_input):
        return int(time.mktime(time.strptime(datetime_input, "%Y-%m-%d %H:%M:%S.%f")))

    def getTdTypeValue(self, value):
        if value.endswith("i8"):
            td_type = "TINYINT"
            td_tag_value = ''.join(list(value)[:-2])
        elif value.endswith("i16"):
            td_type = "SMALLINT"
            td_tag_value = ''.join(list(value)[:-3])
        elif value.endswith("i32"):
            td_type = "INT"
            td_tag_value = ''.join(list(value)[:-3])
        elif value.endswith("i64"):
            td_type = "BIGINT"
            td_tag_value = ''.join(list(value)[:-3])
        elif value.endswith("u64"):
            td_type = "BIGINT UNSIGNED"
            td_tag_value = ''.join(list(value)[:-3])
        elif value.endswith("f32"):
            td_type = "FLOAT"
            td_tag_value = ''.join(list(value)[:-3])
            td_tag_value = '{}'.format(np.float32(td_tag_value))
        elif value.endswith("f64"):
            td_type = "DOUBLE"
            td_tag_value = ''.join(list(value)[:-3])
        elif value.startswith('L"'):
            td_type = "NCHAR"
            td_tag_value = ''.join(list(value)[2:-1])
        elif value.startswith('"') and value.endswith('"'):
            td_type = "BINARY"
            td_tag_value = ''.join(list(value)[1:-1])
        elif value.lower() == "t" or value == "true" or value == "True":
            td_type = "BOOL"
            td_tag_value = "True"
        elif value.lower() == "f" or value == "false" or value == "False":
            td_type = "BOOL"
            td_tag_value = "False"
        else:
            td_type = "FLOAT"
            td_tag_value = value
        return td_type, td_tag_value

    def typeTrans(self, type_list):
        type_num_list = []
        for tp in type_list:
            if tp.upper() == "TIMESTAMP":
                type_num_list.append(9)
            elif tp.upper() == "BOOL":
                type_num_list.append(1)
            elif tp.upper() == "TINYINT":
                type_num_list.append(2)
            elif tp.upper() == "SMALLINT":
                type_num_list.append(3)
            elif tp.upper() == "INT":
                type_num_list.append(4)
            elif tp.upper() == "BIGINT":
                type_num_list.append(5)
            elif tp.upper() == "FLOAT":
                type_num_list.append(6)
            elif tp.upper() == "DOUBLE":
                type_num_list.append(7)
            elif tp.upper() == "BINARY":
                type_num_list.append(8)
            elif tp.upper() == "NCHAR":
                type_num_list.append(10)
            elif tp.upper() == "BIGINT UNSIGNED":
                type_num_list.append(14)
        return type_num_list

    def inputHandle(self, input_sql):
        input_sql_split_list = input_sql.split(" ")

        stb_tag_list = input_sql_split_list[0].split(',')
        stb_col_list = input_sql_split_list[1].split(',')
        ts_value = self.timeTrans(input_sql_split_list[2])

        stb_name = stb_tag_list[0]
        stb_tag_list.pop(0)

        tag_name_list = []
        tag_value_list = []
        td_tag_value_list = []
        td_tag_type_list = []

        col_name_list = []
        col_value_list = []
        td_col_value_list = []
        td_col_type_list = []

        for elm in stb_tag_list:
            if "id=" in elm.lower():
                tb_name = elm.split('=')[1]
            else:
                tag_name_list.append(elm.split("=")[0])
                tag_value_list.append(elm.split("=")[1])
                tb_name = ""
                td_tag_value_list.append(self.getTdTypeValue(elm.split("=")[1])[1])
                td_tag_type_list.append(self.getTdTypeValue(elm.split("=")[1])[0])
        
        for elm in stb_col_list:
            col_name_list.append(elm.split("=")[0])
            col_value_list.append(elm.split("=")[1])
            td_col_value_list.append(self.getTdTypeValue(elm.split("=")[1])[1])
            td_col_type_list.append(self.getTdTypeValue(elm.split("=")[1])[0])

        final_field_list = []
        final_field_list.extend(col_name_list)
        final_field_list.extend(tag_name_list)

        final_type_list = []
        final_type_list.append("TIMESTAMP")
        final_type_list.extend(td_col_type_list)
        final_type_list.extend(td_tag_type_list)
        final_type_list = self.typeTrans(final_type_list)

        final_value_list = []
        final_value_list.append(ts_value)
        final_value_list.extend(td_col_value_list)
        final_value_list.extend(td_tag_value_list)
        return final_value_list, final_field_list, final_type_list, stb_name, tb_name

    def genFullTypeSql(self, stb_name="", tb_name="", t0="", t1="127i8", t2="32767i16", t3="2147483647i32",
                        t4="9223372036854775807i64", t5="11.12345f32", t6="22.123456789f64", t7="\"binaryTagValue\"",
                        t8="L\"ncharTagValue\"", c0="", c1="127i8", c2="32767i16", c3="2147483647i32",
                        c4="9223372036854775807i64", c5="11.12345f32", c6="22.123456789f64", c7="\"binaryColValue\"", 
                        c8="L\"ncharColValue\"", c9="7u64", ts="1626006833639000000ns",
                        id_noexist_tag=None, id_change_tag=None, id_upper_tag=None, id_double_tag=None,
                        ct_add_tag=None, ct_am_tag=None, ct_ma_tag=None, ct_min_tag=None):
        if stb_name == "":
            stb_name = self.getLongName(len=6, mode="letters")
        if tb_name == "":
            tb_name = f'{stb_name}_{random.randint(0, 65535)}_{random.randint(0, 65535)}'
        if t0 == "":
            t0 = random.choice(["f", "F", "false", "False", "t", "T", "true", "True"])
        if c0 == "":
            c0 = random.choice(["f", "F", "false", "False", "t", "T", "true", "True"])
        #sql_seq = f'{stb_name},id=\"{tb_name}\",t0={t0},t1=127i8,t2=32767i16,t3=125.22f64,t4=11.321f32,t5=11.12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" c0={bool_value},c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"binaryValue\",c8=L\"ncharValue\" 1626006833639000000ns'
        if id_upper_tag is not None:
            id = "ID"
        else:
            id = "id"
        sql_seq = f'{stb_name},{id}=\"{tb_name}\",t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if id_noexist_tag is not None:
            sql_seq = f'{stb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
            if ct_add_tag is not None:
                sql_seq = f'{stb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8},t9={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if id_change_tag is not None:
            sql_seq = f'{stb_name},t0={t0},t1={t1},{id}=\"{tb_name}\",t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if id_double_tag is not None:
            sql_seq = f'{stb_name},{id}=\"{tb_name}_1\",t0={t0},t1={t1},{id}=\"{tb_name}_2\",t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if ct_add_tag is not None:
            sql_seq = f'{stb_name},{id}=\"{tb_name}\",t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8},t11={t1},t10={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9},c11={c8},c10={t0} {ts}'
        if ct_am_tag is not None:
            sql_seq = f'{stb_name},{id}=\"{tb_name}\",t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9},c11={c8},c10={t0} {ts}'
            if id_noexist_tag is not None:
                    sql_seq = f'{stb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9},c11={c8},c10={t0} {ts}'
        if ct_ma_tag is not None:
            sql_seq = f'{stb_name},{id}=\"{tb_name}\",t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8},t11={t1},t10={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6} {ts}'
            if id_noexist_tag is not None:
                sql_seq = f'{stb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8},t11={t1},t10={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6} {ts}'
        if ct_min_tag is not None:
            sql_seq = f'{stb_name},{id}=\"{tb_name}\",t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6} {ts}'
        return sql_seq, stb_name
    
    def genMulTagColStr(self, genType, count):
        """
            genType must be tag/col
        """
        tag_str = ""
        col_str = ""
        if genType == "tag":
            for i in range(0, count):
                if i < (count-1):
                    tag_str += f't{i}=f,'
                else:
                    tag_str += f't{i}=f '
            return tag_str
        if genType == "col":
            for i in range(0, count):
                if i < (count-1):
                    col_str += f'c{i}=t,'
                else:
                    col_str += f'c{i}=t '
            return col_str

    def genLongSql(self, tag_count, col_count):
        stb_name = self.getLongName(7, mode="letters")
        tb_name = f'{stb_name}_1'
        tag_str = self.genMulTagColStr("tag", tag_count)
        col_str = self.genMulTagColStr("col", col_count)
        ts = "1626006833640000000ns"
        long_sql = stb_name + ',' + f'id=\"{tb_name}\"' + ',' + tag_str + col_str + ts
        return long_sql, stb_name

    def getNoIdTbName(self, stb_name):
        query_sql = f"select tbname from {stb_name}"
        tb_name = self.resHandle(query_sql, True)[0][0]
        return tb_name

    def resHandle(self, query_sql, query_tag):
        tdSql.execute('reset query cache')
        row_info = tdSql.query(query_sql, query_tag)
        col_info = tdSql.getColNameList(query_sql, query_tag)
        res_row_list = []
        sub_list = []
        for row_mem in row_info:
            for i in row_mem:
                sub_list.append(str(i))
            res_row_list.append(sub_list)
        res_field_list_without_ts = col_info[0][1:]
        res_type_list = col_info[1]
        return res_row_list, res_field_list_without_ts, res_type_list

    def resCmp(self, input_sql, stb_name, query_sql="select * from", condition="", ts=None, id=True, none_check_tag=None):
        expect_list = self.inputHandle(input_sql)
        self._conn.insert_lines([input_sql])
        query_sql = f"{query_sql} {stb_name} {condition}"
        res_row_list, res_field_list_without_ts, res_type_list = self.resHandle(query_sql, True)
        if ts == 0:
            res_ts = self.dateToTs(res_row_list[0][0])
            current_time = time.time()
            if current_time - res_ts < 60:
                tdSql.checkEqual(res_row_list[0][1:], expect_list[0][1:])
            else:
                print("timeout")
                tdSql.checkEqual(res_row_list[0], expect_list[0])
        else:
            if none_check_tag is not None:
                none_index_list = [i for i,x in enumerate(res_row_list[0]) if x=="None"]
                none_index_list.reverse()
                for j in none_index_list:
                    res_row_list[0].pop(j)
                    expect_list[0].pop(j)
            tdSql.checkEqual(res_row_list[0], expect_list[0])
        tdSql.checkEqual(res_field_list_without_ts, expect_list[1])
        for i in range(len(res_type_list)):
            tdSql.checkEqual(res_type_list[i], expect_list[2][i])
        # tdSql.checkEqual(res_type_list, expect_list[2])

    def cleanStb(self):
        query_sql = "show stables"
        res_row_list = tdSql.query(query_sql, True)
        stb_list = map(lambda x: x[0], res_row_list)
        for stb in stb_list:
            tdSql.execute(f'drop table if exists {stb}')

    def initCheckCase(self):
        """
            normal tags and cols, one for every elm
        """
        self.cleanStb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)

    def boolTypeCheckCase(self):
        """
            check all normal type
        """
        self.cleanStb()
        full_type_list = ["f", "F", "false", "False", "t", "T", "true", "True"]
        for t_type in full_type_list:
            input_sql, stb_name = self.genFullTypeSql(c0=t_type, t0=t_type)
            self.resCmp(input_sql, stb_name)
        
    def symbolsCheckCase(self):
        """
            check symbols = `~!@#$%^&*()_-+={[}]\|:;'\",<.>/? 
        """
        '''
            please test :
            binary_symbols = '\"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"\'\'"\"'
        '''
        self.cleanStb()
        binary_symbols = '\"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"\"'
        nchar_symbols = f'L{binary_symbols}'
        input_sql, stb_name = self.genFullTypeSql(c7=binary_symbols, c8=nchar_symbols, t7=binary_symbols, t8=nchar_symbols)
        self.resCmp(input_sql, stb_name)

    def tsCheckCase(self):
        """
            test ts list --> ["1626006833639000000ns", "1626006833639019us", "1626006833640ms", "1626006834s", "1626006822639022"]
            # ! us级时间戳都为0时，数据库中查询显示，但python接口拿到的结果不显示 .000000的情况请确认，目前修改时间处理代码可以通过
        """
        self.cleanStb()
        ts_list = ["1626006833639000000ns", "1626006833639019us", "1626006833640ms", "1626006834s", "1626006822639022", 0]
        for ts in ts_list:
            input_sql, stb_name = self.genFullTypeSql(ts=ts)
            self.resCmp(input_sql, stb_name, ts=ts)
    
    def idSeqCheckCase(self):
        """
            check id.index in tags
            eg: t0=**,id=**,t1=**
        """
        self.cleanStb()
        input_sql, stb_name = self.genFullTypeSql(id_change_tag=True)
        self.resCmp(input_sql, stb_name)
    
    def idUpperCheckCase(self):
        """
            check id param
            eg: id and ID
        """
        self.cleanStb()
        input_sql, stb_name = self.genFullTypeSql(id_upper_tag=True)
        self.resCmp(input_sql, stb_name)
        input_sql, stb_name = self.genFullTypeSql(id_change_tag=True, id_upper_tag=True)
        self.resCmp(input_sql, stb_name)

    def noIdCheckCase(self):
        """
            id not exist
        """
        self.cleanStb()
        input_sql, stb_name = self.genFullTypeSql(id_noexist_tag=True)
        self.resCmp(input_sql, stb_name)
        query_sql = f"select tbname from {stb_name}"
        res_row_list = self.resHandle(query_sql, True)[0]
        if len(res_row_list[0][0]) > 0:
            tdSql.checkColNameList(res_row_list, res_row_list)
        else:
            tdSql.checkColNameList(res_row_list, "please check noIdCheckCase")

    def maxColTagCheckCase(self):
        """
            max tag count is 128
            max col count is ??
        """
        for input_sql in [self.genLongSql(128, 1)[0], self.genLongSql(1, 4094)[0]]:
            self.cleanStb()
            self._conn.insert_lines([input_sql])
        for input_sql in [self.genLongSql(129, 1)[0], self.genLongSql(1, 4095)[0]]:
            self.cleanStb()
            try:
                self._conn.insert_lines([input_sql])
            except LinesError:
                pass
            
    def idIllegalNameCheckCase(self):
        """
            test illegal id name
            mix "`~!@#$¥%^&*()-+={}|[]、「」【】\:;《》<>?"
        """
        self.cleanStb()
        rstr = list("`~!@#$¥%^&*()-+={}|[]、「」【】\:;《》<>?")
        for i in rstr:
            input_sql = self.genFullTypeSql(tb_name=f"\"aaa{i}bbb\"")[0]
            try:
                self._conn.insert_lines([input_sql])
            except LinesError:
                pass

    def idStartWithNumCheckCase(self):
        """
            id is start with num
        """
        self.cleanStb()
        input_sql = self.genFullTypeSql(tb_name=f"\"1aaabbb\"")[0]
        try:
            self._conn.insert_lines([input_sql])
        except LinesError:
            pass

    def nowTsCheckCase(self):
        """
            check now unsupported
        """
        self.cleanStb()
        input_sql = self.genFullTypeSql(ts="now")[0]
        try:
            self._conn.insert_lines([input_sql])
        except LinesError:
            pass

    def dateFormatTsCheckCase(self):
        """
            check date format ts unsupported
        """
        self.cleanStb()
        input_sql = self.genFullTypeSql(ts="2021-07-21\ 19:01:46.920")[0]
        try:
            self._conn.insert_lines([input_sql])
        except LinesError:
            pass
    
    def illegalTsCheckCase(self):
        """
            check ts format like 16260068336390us19
        """
        self.cleanStb()
        input_sql = self.genFullTypeSql(ts="16260068336390us19")[0]
        try:
            self._conn.insert_lines([input_sql])
        except LinesError:
            pass

    def tagValueLengthCheckCase(self):
        """
            check full type tag value limit
        """
        self.cleanStb()
        # i8
        for t1 in ["-127i8", "127i8"]:
            input_sql, stb_name = self.genFullTypeSql(t1=t1)
            self.resCmp(input_sql, stb_name)
        for t1 in ["-128i8", "128i8"]:
            input_sql = self.genFullTypeSql(t1=t1)[0]
            try:
                self._conn.insert_lines([input_sql])
            except LinesError:
                pass

        #i16
        for t2 in ["-32767i16", "32767i16"]:
            input_sql, stb_name = self.genFullTypeSql(t2=t2)
            self.resCmp(input_sql, stb_name)
        for t2 in ["-32768i16", "32768i16"]:
            input_sql = self.genFullTypeSql(t2=t2)[0]
            try:
                self._conn.insert_lines([input_sql])
            except LinesError:
                pass

        #i32
        for t3 in ["-2147483647i32", "2147483647i32"]:
            input_sql, stb_name = self.genFullTypeSql(t3=t3)
            self.resCmp(input_sql, stb_name)
        for t3 in ["-2147483648i32", "2147483648i32"]:
            input_sql = self.genFullTypeSql(t3=t3)[0]
            try:
                self._conn.insert_lines([input_sql])
            except LinesError:
                pass

        #i64
        for t4 in ["-9223372036854775807i64", "9223372036854775807i64"]:
            input_sql, stb_name = self.genFullTypeSql(t4=t4)
            self.resCmp(input_sql, stb_name)
        for t4 in ["-9223372036854775808i64", "9223372036854775808i64"]:
            input_sql = self.genFullTypeSql(t4=t4)[0]
            try:
                self._conn.insert_lines([input_sql])
            except LinesError:
                pass

        # f32
        for t5 in [f"{-3.4028234663852885981170418348451692544*(10**38)}f32", f"{3.4028234663852885981170418348451692544*(10**38)}f32"]:
            input_sql, stb_name = self.genFullTypeSql(t5=t5)
            self.resCmp(input_sql, stb_name)
        # * limit set to 4028234664*(10**38)
        for t5 in [f"{-3.4028234664*(10**38)}f32", f"{3.4028234664*(10**38)}f32"]:
            input_sql = self.genFullTypeSql(t5=t5)[0]
            try:
                self._conn.insert_lines([input_sql])
                raise Exception("should not reach here")
            except LinesError as err:
                tdSql.checkNotEqual(err.errno, 0)


        # f64
        for t6 in [f'{-1.79769*(10**308)}f64', f'{-1.79769*(10**308)}f64']:
            input_sql, stb_name = self.genFullTypeSql(t6=t6)
            self.resCmp(input_sql, stb_name)
        # * limit set to 1.797693134862316*(10**308)
        for c6 in [f'{-1.797693134862316*(10**308)}f64', f'{-1.797693134862316*(10**308)}f64']:
            input_sql = self.genFullTypeSql(c6=c6)[0]
            try:
                self._conn.insert_lines([input_sql])
                raise Exception("should not reach here")
            except LinesError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # binary 
        stb_name = self.getLongName(7, "letters")
        input_sql = f'{stb_name},t0=t,t1="{self.getLongName(16374, "letters")}" c0=f 1626006833639000000ns'
        self._conn.insert_lines([input_sql])
        
        input_sql = f'{stb_name},t0=t,t1="{self.getLongName(16375, "letters")}" c0=f 1626006833639000000ns'
        try:
            self._conn.insert_lines([input_sql])
            raise Exception("should not reach here")
        except LinesError as err:
            pass

        # nchar
        # * legal nchar could not be larger than 16374/4
        stb_name = self.getLongName(7, "letters")
        input_sql = f'{stb_name},t0=t,t1=L"{self.getLongName(4093, "letters")}" c0=f 1626006833639000000ns'
        self._conn.insert_lines([input_sql])

        input_sql = f'{stb_name},t0=t,t1=L"{self.getLongName(4094, "letters")}" c0=f 1626006833639000000ns'
        try:
            self._conn.insert_lines([input_sql])
            raise Exception("should not reach here")
        except LinesError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def colValueLengthCheckCase(self):
        """
            check full type col value limit
        """
        self.cleanStb()
        # i8
        for c1 in ["-127i8", "127i8"]:
            input_sql, stb_name = self.genFullTypeSql(c1=c1)
            self.resCmp(input_sql, stb_name)

        for c1 in ["-128i8", "128i8"]:
            input_sql = self.genFullTypeSql(c1=c1)[0]
            try:
                self._conn.insert_lines([input_sql])
                raise Exception("should not reach here")
            except LinesError as err:
                tdSql.checkNotEqual(err.errno, 0)
        # i16
        for c2 in ["-32767i16"]:
            input_sql, stb_name = self.genFullTypeSql(c2=c2)
            self.resCmp(input_sql, stb_name)
        for c2 in ["-32768i16", "32768i16"]:
            input_sql = self.genFullTypeSql(c2=c2)[0]
            try:
                self._conn.insert_lines([input_sql])
                raise Exception("should not reach here")
            except LinesError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # i32
        for c3 in ["-2147483647i32"]:
            input_sql, stb_name = self.genFullTypeSql(c3=c3)
            self.resCmp(input_sql, stb_name)
        for c3 in ["-2147483648i32", "2147483648i32"]:
            input_sql = self.genFullTypeSql(c3=c3)[0]
            try:
                self._conn.insert_lines([input_sql])
                raise Exception("should not reach here")
            except LinesError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # i64
        for c4 in ["-9223372036854775807i64"]:
            input_sql, stb_name = self.genFullTypeSql(c4=c4)
            self.resCmp(input_sql, stb_name)
        for c4 in ["-9223372036854775808i64", "9223372036854775808i64"]:
            input_sql = self.genFullTypeSql(c4=c4)[0]
            try:
                self._conn.insert_lines([input_sql])
                raise Exception("should not reach here")
            except LinesError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # f32       
        for c5 in [f"{-3.4028234663852885981170418348451692544*(10**38)}f32", f"{3.4028234663852885981170418348451692544*(10**38)}f32"]:
            input_sql, stb_name = self.genFullTypeSql(c5=c5)
            self.resCmp(input_sql, stb_name)
        # * limit set to 4028234664*(10**38)
        for c5 in [f"{-3.4028234664*(10**38)}f32", f"{3.4028234664*(10**38)}f32"]:
            input_sql = self.genFullTypeSql(c5=c5)[0]
            try:
                self._conn.insert_lines([input_sql])
                raise Exception("should not reach here")
            except LinesError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # f64
        for c6 in [f'{-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)}f64', f'{-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)}f64']:
            input_sql, stb_name = self.genFullTypeSql(c6=c6)
            self.resCmp(input_sql, stb_name)
        # * limit set to 1.797693134862316*(10**308)
        for c6 in [f'{-1.797693134862316*(10**308)}f64', f'{-1.797693134862316*(10**308)}f64']:
            input_sql = self.genFullTypeSql(c6=c6)[0]
            try:
                self._conn.insert_lines([input_sql])
                raise Exception("should not reach here")
            except LinesError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # # binary 
        stb_name = self.getLongName(7, "letters")
        input_sql = f'{stb_name},t0=t c0=f,c1="{self.getLongName(16374, "letters")}" 1626006833639000000ns'
        self._conn.insert_lines([input_sql])
        
        input_sql = f'{stb_name},t0=t c0=f,c1="{self.getLongName(16375, "letters")}" 1626006833639000000ns'
        try:
            self._conn.insert_lines([input_sql])
            raise Exception("should not reach here")
        except LinesError as err:
            tdSql.checkNotEqual(err.errno, 0)

        # nchar
        # * legal nchar could not be larger than 16374/4
        stb_name = self.getLongName(7, "letters")
        input_sql = f'{stb_name},t0=t c0=f,c1=L"{self.getLongName(4093, "letters")}" 1626006833639000000ns'
        self._conn.insert_lines([input_sql])

        input_sql = f'{stb_name},t0=t c0=f,c1=L"{self.getLongName(4094, "letters")}" 1626006833639000000ns'
        try:
            self._conn.insert_lines([input_sql])
            raise Exception("should not reach here")
        except LinesError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def tagColIllegalValueCheckCase(self):

        """
            test illegal tag col value
        """
        self.cleanStb()
        # bool
        for i in ["TrUe", "tRue", "trUe", "truE", "FalsE", "fAlse", "faLse", "falSe", "falsE"]:
            input_sql1 = self.genFullTypeSql(t0=i)[0]
            try:
                self._conn.insert_lines([input_sql1])
                raise Exception("should not reach here")
            except LinesError as err:
                tdSql.checkNotEqual(err.errno, 0)
            input_sql2 = self.genFullTypeSql(c0=i)[0]
            try:
                self._conn.insert_lines([input_sql2])
                raise Exception("should not reach here")
            except LinesError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # i8 i16 i32 i64 f32 f64
        for input_sql in [
                self.genFullTypeSql(t1="1s2i8")[0], 
                self.genFullTypeSql(t2="1s2i16")[0],
                self.genFullTypeSql(t3="1s2i32")[0],
                self.genFullTypeSql(t4="1s2i64")[0],
                self.genFullTypeSql(t5="11.1s45f32")[0],
                self.genFullTypeSql(t6="11.1s45f64")[0], 
                self.genFullTypeSql(c1="1s2i8")[0], 
                self.genFullTypeSql(c2="1s2i16")[0],
                self.genFullTypeSql(c3="1s2i32")[0],
                self.genFullTypeSql(c4="1s2i64")[0],
                self.genFullTypeSql(c5="11.1s45f32")[0],
                self.genFullTypeSql(c6="11.1s45f64")[0],
                self.genFullTypeSql(c9="1s1u64")[0]
            ]:
            try:
                self._conn.insert_lines([input_sql])
                raise Exception("should not reach here")
            except LinesError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # check binary and nchar blank
        stb_name = self.getLongName(7, "letters")
        input_sql1 = f'{stb_name},t0=t c0=f,c1="abc aaa" 1626006833639000000ns'
        input_sql2 = f'{stb_name},t0=t c0=f,c1=L"abc aaa" 1626006833639000000ns'
        input_sql3 = f'{stb_name},t0=t,t1="abc aaa" c0=f 1626006833639000000ns'
        input_sql4 = f'{stb_name},t0=t,t1=L"abc aaa" c0=f 1626006833639000000ns'
        for input_sql in [input_sql1, input_sql2, input_sql3, input_sql4]:
            try:
                self._conn.insert_lines([input_sql])
                raise Exception("should not reach here")
            except LinesError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # check accepted binary and nchar symbols 
        # # * ~!@#$¥%^&*()-+={}|[]、「」:;
        for symbol in list('~!@#$¥%^&*()-+={}|[]、「」:;'):
            input_sql1 = f'{stb_name},t0=t c0=f,c1="abc{symbol}aaa" 1626006833639000000ns'
            input_sql2 = f'{stb_name},t0=t,t1="abc{symbol}aaa" c0=f 1626006833639000000ns'
            self._conn.insert_lines([input_sql1])
            self._conn.insert_lines([input_sql2])
        

    def duplicateIdTagColInsertCheckCase(self):
        """
            check duplicate Id Tag Col
        """
        self.cleanStb()
        input_sql_id = self.genFullTypeSql(id_double_tag=True)[0]
        try:
            self._conn.insert_lines([input_sql_id])
            raise Exception("should not reach here")
        except LinesError as err:
            tdSql.checkNotEqual(err.errno, 0)

        input_sql = self.genFullTypeSql()[0]
        input_sql_tag = input_sql.replace("t5", "t6")
        try:
            self._conn.insert_lines([input_sql_tag])
            raise Exception("should not reach here")
        except LinesError as err:
            tdSql.checkNotEqual(err.errno, 0)

        input_sql = self.genFullTypeSql()[0]
        input_sql_col = input_sql.replace("c5", "c6")
        try:
            self._conn.insert_lines([input_sql_col])
            raise Exception("should not reach here")
        except LinesError as err:
            tdSql.checkNotEqual(err.errno, 0)

        input_sql = self.genFullTypeSql()[0]
        input_sql_col = input_sql.replace("c5", "C6")
        try:
            self._conn.insert_lines([input_sql_col])
            raise Exception("should not reach here")
        except LinesError as err:
            tdSql.checkNotEqual(err.errno, 0)

    ##### stb exist #####
    def noIdStbExistCheckCase(self):
        """
            case no id when stb exist
        """
        self.cleanStb()
        input_sql, stb_name = self.genFullTypeSql(tb_name="sub_table_0123456", t0="f", c0="f")
        self.resCmp(input_sql, stb_name)
        input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name, id_noexist_tag=True, t0="f", c0="f")
        self.resCmp(input_sql, stb_name, condition='where tbname like "t_%"')
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)
        # TODO cover other case

    def duplicateInsertExistCheckCase(self):
        """
            check duplicate insert when stb exist
        """
        self.cleanStb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        self._conn.insert_lines([input_sql])
        self.resCmp(input_sql, stb_name)

    def tagColBinaryNcharLengthCheckCase(self):
        """
            check length increase
        """
        self.cleanStb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        tb_name = self.getLongName(5, "letters")
        input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name,t7="\"binaryTagValuebinaryTagValue\"", t8="L\"ncharTagValuencharTagValue\"", c7="\"binaryTagValuebinaryTagValue\"", c8="L\"ncharTagValuencharTagValue\"")
        self.resCmp(input_sql, stb_name, condition=f'where tbname like "{tb_name}"')

    def tagColAddDupIDCheckCase(self):
        """
            check column and tag count add, stb and tb duplicate
            * tag: alter table ...
            * col: when update==0 and ts is same, unchange
            * so this case tag&&value will be added, 
            * col is added without value when update==0
            * col is added with value when update==1
        """
        self.cleanStb()
        tb_name = self.getLongName(7, "letters")
        for db_update_tag in [0, 1]:
            if db_update_tag == 1 :
                self.createDb("test_update", db_update_tag=db_update_tag)
            input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name, t0="f", c0="f")
            self.resCmp(input_sql, stb_name)
            self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t0="f", c0="f", ct_add_tag=True)
            if db_update_tag == 1 :
                self.resCmp(input_sql, stb_name, condition=f'where tbname like "{tb_name}"')
            else:
                self.resCmp(input_sql, stb_name, condition=f'where tbname like "{tb_name}"', none_check_tag=True)

    def tagColAddCheckCase(self):
        """
            check column and tag count add
        """
        self.cleanStb()
        tb_name = self.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name, t0="f", c0="f")
        self.resCmp(input_sql, stb_name)
        tb_name_1 = self.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name_1, t0="f", c0="f", ct_add_tag=True)
        self.resCmp(input_sql, stb_name, condition=f'where tbname like "{tb_name_1}"')
        res_row_list = self.resHandle(f"select c10,c11,t10,t11 from {tb_name}", True)[0]
        tdSql.checkEqual(res_row_list[0], ['None', 'None', 'None', 'None'])
        self.resCmp(input_sql, stb_name, condition=f'where tbname like "{tb_name}"', none_check_tag=True)

    def tagMd5Check(self):
        """
            condition: stb not change
            insert two table, keep tag unchange, change col
        """
        self.cleanStb()
        input_sql, stb_name = self.genFullTypeSql(t0="f", c0="f", id_noexist_tag=True)
        self.resCmp(input_sql, stb_name)
        tb_name1 = self.getNoIdTbName(stb_name)
        input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name, t0="f", c0="f", id_noexist_tag=True)
        self.resCmp(input_sql, stb_name)
        tb_name2 = self.getNoIdTbName(stb_name)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(1)
        tdSql.checkEqual(tb_name1, tb_name2)
        input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name, t0="f", c0="f", id_noexist_tag=True, ct_add_tag=True)
        self._conn.insert_lines([input_sql])
        tb_name3 = self.getNoIdTbName(stb_name)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)
        tdSql.checkNotEqual(tb_name1, tb_name3)

    # * tag binary max is 16384, col+ts binary max  49151
    def tagColBinaryMaxLengthCheckCase(self):
        """
            every binary and nchar must be length+2
        """
        self.cleanStb()
        stb_name = self.getLongName(7, "letters")
        tb_name = f'{stb_name}_1'
        input_sql = f'{stb_name},id="{tb_name}",t0=t c0=f 1626006833639000000ns'
        self._conn.insert_lines([input_sql])

        # * every binary and nchar must be length+2, so here is two tag, max length could not larger than 16384-2*2
        input_sql = f'{stb_name},t0=t,t1="{self.getLongName(16374, "letters")}",t2="{self.getLongName(5, "letters")}" c0=f 1626006833639000000ns'
        self._conn.insert_lines([input_sql])
        
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)
        input_sql = f'{stb_name},t0=t,t1="{self.getLongName(16374, "letters")}",t2="{self.getLongName(6, "letters")}" c0=f 1626006833639000000ns'
        try:
            self._conn.insert_lines([input_sql])
            raise Exception("should not reach here")
        except LinesError:
            pass
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)

        # # * check col，col+ts max in describe ---> 16143
        input_sql = f'{stb_name},t0=t c0=f,c1="{self.getLongName(16374, "letters")}",c2="{self.getLongName(16374, "letters")}",c3="{self.getLongName(16374, "letters")}",c4="{self.getLongName(12, "letters")}" 1626006833639000000ns'
        self._conn.insert_lines([input_sql])

        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(3)
        input_sql = f'{stb_name},t0=t c0=f,c1="{self.getLongName(16374, "letters")}",c2="{self.getLongName(16374, "letters")}",c3="{self.getLongName(16374, "letters")}",c4="{self.getLongName(13, "letters")}" 1626006833639000000ns'
        try:
            self._conn.insert_lines([input_sql])
            raise Exception("should not reach here")
        except LinesError as err:
            tdSql.checkNotEqual(err.errno, 0)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(3)
    
    # * tag nchar max is 16374/4, col+ts nchar max  49151
    def tagColNcharMaxLengthCheckCase(self):
        """
            check nchar length limit
        """
        self.cleanStb()
        stb_name = self.getLongName(7, "letters")
        tb_name = f'{stb_name}_1'
        input_sql = f'{stb_name},id="{tb_name}",t0=t c0=f 1626006833639000000ns'
        code = self._conn.insert_lines([input_sql])

        # * legal nchar could not be larger than 16374/4
        input_sql = f'{stb_name},t0=t,t1=L"{self.getLongName(4093, "letters")}",t2=L"{self.getLongName(1, "letters")}" c0=f 1626006833639000000ns'
        self._conn.insert_lines([input_sql])
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)
        input_sql = f'{stb_name},t0=t,t1=L"{self.getLongName(4093, "letters")}",t2=L"{self.getLongName(2, "letters")}" c0=f 1626006833639000000ns'
        try:
            self._conn.insert_lines([input_sql])
            raise Exception("should not reach here")
        except LinesError as err:
            tdSql.checkNotEqual(err.errno, 0)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)

        input_sql = f'{stb_name},t0=t c0=f,c1=L"{self.getLongName(4093, "letters")}",c2=L"{self.getLongName(4093, "letters")}",c3=L"{self.getLongName(4093, "letters")}",c4=L"{self.getLongName(4, "letters")}" 1626006833639000000ns'
        self._conn.insert_lines([input_sql])
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(3)
        input_sql = f'{stb_name},t0=t c0=f,c1=L"{self.getLongName(4093, "letters")}",c2=L"{self.getLongName(4093, "letters")}",c3=L"{self.getLongName(4093, "letters")}",c4=L"{self.getLongName(5, "letters")}" 1626006833639000000ns'
        try:
            self._conn.insert_lines([input_sql])
            raise Exception("should not reach here")
        except LinesError as err:
            tdSql.checkNotEqual(err.errno, 0)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(3)

    def batchInsertCheckCase(self):
        """
            test batch insert
        """
        self.cleanStb()
        stb_name = self.getLongName(8, "letters")
        tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 bigint)')
        lines = ["st123456,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000ns",
                "st123456,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000ns",
                f"{stb_name},t2=5f64,t3=L\"ste\" c1=true,c2=4i64,c3=\"iam\" 1626056811823316532ns",
                "stf567890,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000ns",
                "st123456,t1=4i64,t2=5f64,t3=\"t4\" c1=3i64,c3=L\"passitagain\",c2=true,c4=5f64 1626006833642000000ns",
                f"{stb_name},t2=5f64,t3=L\"ste2\" c3=\"iamszhou\",c4=false 1626056811843316532ns",
                f"{stb_name},t2=5f64,t3=L\"ste2\" c3=\"iamszhou\",c4=false,c5=32i8,c6=64i16,c7=32i32,c8=88.88f32 1626056812843316532ns",
                "st123456,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000ns",
                "st123456,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64 1626006933641000000ns"
                ]
        self._conn.insert_lines(lines)
    
    def multiInsertCheckCase(self, count):
            """
                test multi insert
            """
            self.cleanStb()
            sql_list = []
            stb_name = self.getLongName(8, "letters")
            tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 bigint)')
            for i in range(count):
                input_sql = self.genFullTypeSql(stb_name=stb_name, t7=f'"{self.getLongName(8, "letters")}"', c7=f'"{self.getLongName(8, "letters")}"', id_noexist_tag=True)[0]
                sql_list.append(input_sql)
            self._conn.insert_lines(sql_list)

    def batchErrorInsertCheckCase(self):
        """
            test batch error insert
        """
        self.cleanStb()
        stb_name = self.getLongName(8, "letters")
        lines = ["st123456,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000ns",
                f"{stb_name},t2=5f64,t3=L\"ste\" c1=tRue,c2=4i64,c3=\"iam\" 1626056811823316532ns"]
        try:
            self._conn.insert_lines(lines)
            raise Exception("should not reach here")
        except LinesError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def genSqlList(self, count=5, stb_name="", tb_name=""):
        """
            stb --> supertable
            tb  --> table
            ts  --> timestamp, same default
            col --> column, same default
            tag --> tag, same default
            d   --> different
            s   --> same
            a   --> add
            m   --> minus
        """
        d_stb_d_tb_list = list()
        s_stb_s_tb_list = list()
        s_stb_s_tb_a_col_a_tag_list = list()
        s_stb_s_tb_m_col_m_tag_list = list()
        s_stb_d_tb_list = list()
        s_stb_d_tb_a_col_m_tag_list = list()
        s_stb_d_tb_a_tag_m_col_list = list()
        s_stb_s_tb_d_ts_list = list()
        s_stb_s_tb_d_ts_a_col_m_tag_list = list()
        s_stb_s_tb_d_ts_a_tag_m_col_list = list()
        s_stb_d_tb_d_ts_list = list()
        s_stb_d_tb_d_ts_a_col_m_tag_list = list()
        s_stb_d_tb_d_ts_a_tag_m_col_list = list()
        for i in range(count):
            d_stb_d_tb_list.append(self.genFullTypeSql(t0="f", c0="f"))
            s_stb_s_tb_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{self.getLongName(8, "letters")}"', c7=f'"{self.getLongName(8, "letters")}"'))
            s_stb_s_tb_a_col_a_tag_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{self.getLongName(8, "letters")}"', c7=f'"{self.getLongName(8, "letters")}"', ct_add_tag=True))
            s_stb_s_tb_m_col_m_tag_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{self.getLongName(8, "letters")}"', c7=f'"{self.getLongName(8, "letters")}"', ct_min_tag=True))
            s_stb_d_tb_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{self.getLongName(8, "letters")}"', c7=f'"{self.getLongName(8, "letters")}"', id_noexist_tag=True))
            s_stb_d_tb_a_col_m_tag_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{self.getLongName(8, "letters")}"', c7=f'"{self.getLongName(8, "letters")}"', id_noexist_tag=True, ct_am_tag=True))
            s_stb_d_tb_a_tag_m_col_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{self.getLongName(8, "letters")}"', c7=f'"{self.getLongName(8, "letters")}"', id_noexist_tag=True, ct_ma_tag=True))
            s_stb_s_tb_d_ts_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{self.getLongName(8, "letters")}"', c7=f'"{self.getLongName(8, "letters")}"', ts=0))
            s_stb_s_tb_d_ts_a_col_m_tag_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{self.getLongName(8, "letters")}"', c7=f'"{self.getLongName(8, "letters")}"', ts=0, ct_am_tag=True))
            s_stb_s_tb_d_ts_a_tag_m_col_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{self.getLongName(8, "letters")}"', c7=f'"{self.getLongName(8, "letters")}"', ts=0, ct_ma_tag=True))
            s_stb_d_tb_d_ts_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{self.getLongName(8, "letters")}"', c7=f'"{self.getLongName(8, "letters")}"', id_noexist_tag=True, ts=0))
            s_stb_d_tb_d_ts_a_col_m_tag_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{self.getLongName(8, "letters")}"', c7=f'"{self.getLongName(8, "letters")}"', id_noexist_tag=True, ts=0, ct_am_tag=True))
            s_stb_d_tb_d_ts_a_tag_m_col_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{self.getLongName(8, "letters")}"', c7=f'"{self.getLongName(8, "letters")}"', id_noexist_tag=True, ts=0, ct_ma_tag=True))

        return d_stb_d_tb_list, s_stb_s_tb_list, s_stb_s_tb_a_col_a_tag_list, s_stb_s_tb_m_col_m_tag_list, \
            s_stb_d_tb_list, s_stb_d_tb_a_col_m_tag_list, s_stb_d_tb_a_tag_m_col_list, s_stb_s_tb_d_ts_list, \
            s_stb_s_tb_d_ts_a_col_m_tag_list, s_stb_s_tb_d_ts_a_tag_m_col_list, s_stb_d_tb_d_ts_list, \
            s_stb_d_tb_d_ts_a_col_m_tag_list, s_stb_d_tb_d_ts_a_tag_m_col_list


    def genMultiThreadSeq(self, sql_list):
        tlist = list()
        for insert_sql in sql_list:
            t = threading.Thread(target=self._conn.insert_lines,args=([insert_sql[0]],))
            tlist.append(t)
        return tlist

    def multiThreadRun(self, tlist):
        for t in tlist:
            t.start()
        for t in tlist:
            t.join()

    def stbInsertMultiThreadCheckCase(self):
        """
            thread input different stb
        """
        self.cleanStb()
        input_sql = self.genSqlList()[0]
        self.multiThreadRun(self.genMultiThreadSeq(input_sql))
        tdSql.query(f"show tables;")
        tdSql.checkRows(5)
    
    def sStbStbDdataInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different data, result keep first data
        """
        self.cleanStb()
        tb_name = self.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name)
        self.resCmp(input_sql, stb_name)
        s_stb_s_tb_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name)[1]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1)
        expected_tb_name = self.getNoIdTbName(stb_name)[0]
        tdSql.checkEqual(tb_name, expected_tb_name)
        tdSql.query(f"select * from {stb_name};")
        tdSql.checkRows(1)

    def sStbStbDdataAtcInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different data, add columes and tags,  result keep first data
        """
        self.cleanStb()
        tb_name = self.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name)
        self.resCmp(input_sql, stb_name)
        s_stb_s_tb_a_col_a_tag_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name)[2]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_a_col_a_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1)
        expected_tb_name = self.getNoIdTbName(stb_name)[0]
        tdSql.checkEqual(tb_name, expected_tb_name)
        tdSql.query(f"select * from {stb_name};")
        tdSql.checkRows(1)
    
    def sStbStbDdataMtcInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different data, minus columes and tags,  result keep first data
        """
        self.cleanStb()
        tb_name = self.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name)
        self.resCmp(input_sql, stb_name)
        s_stb_s_tb_m_col_m_tag_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name)[3]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_m_col_m_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1)
        expected_tb_name = self.getNoIdTbName(stb_name)[0]
        tdSql.checkEqual(tb_name, expected_tb_name)
        tdSql.query(f"select * from {stb_name};")
        tdSql.checkRows(1)

    def sStbDtbDdataInsertMultiThreadCheckCase(self):
        """
            thread input same stb, different tb, different data
        """
        self.cleanStb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        s_stb_d_tb_list = self.genSqlList(stb_name=stb_name)[4]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(6)

    def sStbDtbDdataAcMtInsertMultiThreadCheckCase(self):
        """
            #! concurrency conflict
        """
        """
            thread input same stb, different tb, different data, add col, mul tag
        """
        self.cleanStb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        s_stb_d_tb_a_col_m_tag_list = self.genSqlList(stb_name=stb_name)[5]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_a_col_m_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(6)

    def sStbDtbDdataAtMcInsertMultiThreadCheckCase(self):
        """
            #! concurrency conflict
        """
        """
            thread input same stb, different tb, different data, add tag, mul col
        """
        self.cleanStb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        s_stb_d_tb_a_tag_m_col_list = self.genSqlList(stb_name=stb_name)[6]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_a_tag_m_col_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(6)

    def sStbStbDdataDtsInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different ts
        """
        self.cleanStb()
        tb_name = self.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name)
        self.resCmp(input_sql, stb_name)
        s_stb_s_tb_d_ts_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name)[7]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_d_ts_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(6)

    def sStbStbDdataDtsAcMtInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different ts, add col, mul tag
        """
        self.cleanStb()
        tb_name = self.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name)
        self.resCmp(input_sql, stb_name)
        s_stb_s_tb_d_ts_a_col_m_tag_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name)[8]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_d_ts_a_col_m_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(6)
        tdSql.query(f"select * from {stb_name} where t8 is not NULL")
        tdSql.checkRows(6)
        tdSql.query(f"select * from {tb_name} where c11 is not NULL;")
        tdSql.checkRows(5)

    def sStbStbDdataDtsAtMcInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different ts, add tag, mul col
        """
        self.cleanStb()
        tb_name = self.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name)
        self.resCmp(input_sql, stb_name)
        s_stb_s_tb_d_ts_a_tag_m_col_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name)[9]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_d_ts_a_tag_m_col_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(6)
        for c in ["c7", "c8", "c9"]:
            tdSql.query(f"select * from {stb_name} where {c} is NULL")
            tdSql.checkRows(5)        
        for t in ["t10", "t11"]:
            tdSql.query(f"select * from {stb_name} where {t} is not NULL;")
            tdSql.checkRows(6)

    def sStbDtbDdataDtsInsertMultiThreadCheckCase(self):
        """
            thread input same stb, different tb, data, ts
        """
        self.cleanStb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        s_stb_d_tb_d_ts_list = self.genSqlList(stb_name=stb_name)[10]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_d_ts_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(6)

    def sStbDtbDdataDtsAcMtInsertMultiThreadCheckCase(self):
        """
            # ! concurrency conflict
        """
        """
            thread input same stb, different tb, data, ts, add col, mul tag
        """
        self.cleanStb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        s_stb_d_tb_d_ts_a_col_m_tag_list = self.genSqlList(stb_name=stb_name)[11]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_d_ts_a_col_m_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(6)

    def test(self):
        input_sql1 = "rfasta,id=\"rfasta_1\",t0=true,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"ddzhiksj\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"bnhwlgvj\",c8=L\"ncharTagValue\",c9=7u64 1626006933640000000ns"
        input_sql2 = "rfasta,id=\"rfasta_1\",t0=true,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 1626006933640000000ns"
        try:
            self._conn.insert_lines([input_sql1])
            self._conn.insert_lines([input_sql2])
        except LinesError as err:
            print(err.errno)
        # self._conn.insert_lines([input_sql2])
        # input_sql3 = f'abcd,id="cc¥Ec",t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="ndsfdrum",t8=L"ncharTagValue" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="igwoehkm",c8=L"ncharColValue",c9=7u64 0'
        # print(input_sql3)
        # input_sql4 = 'hmemeb,id="kilrcrldgf",t0=F,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="fysodjql",t8=L"ncharTagValue" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="waszbfvc",c8=L"ncharColValue",c9=7u64 0'
        # code = self._conn.insert_lines([input_sql3])
        # print(code)
        # self._conn.insert_lines([input_sql4])

    def runAll(self):
        self.initCheckCase()
        self.boolTypeCheckCase()
        self.symbolsCheckCase()
        self.tsCheckCase()
        self.idSeqCheckCase()
        self.idUpperCheckCase()
        self.noIdCheckCase()
        self.maxColTagCheckCase()
        self.idIllegalNameCheckCase()
        self.idStartWithNumCheckCase()
        self.nowTsCheckCase()
        self.dateFormatTsCheckCase()
        self.illegalTsCheckCase()
        self.tagValueLengthCheckCase()
        self.colValueLengthCheckCase()
        self.tagColIllegalValueCheckCase()
        self.duplicateIdTagColInsertCheckCase()
        self.noIdStbExistCheckCase()
        self.duplicateInsertExistCheckCase()
        self.tagColBinaryNcharLengthCheckCase()
        self.tagColAddDupIDCheckCase()
        self.tagColAddCheckCase()
        self.tagMd5Check()
        self.tagColBinaryMaxLengthCheckCase()
        # self.tagColNcharMaxLengthCheckCase()
        self.batchInsertCheckCase()
        self.multiInsertCheckCase(1000)
        self.batchErrorInsertCheckCase()
        # MultiThreads
        self.stbInsertMultiThreadCheckCase()
        self.sStbStbDdataInsertMultiThreadCheckCase()
        self.sStbStbDdataAtcInsertMultiThreadCheckCase()
        self.sStbStbDdataMtcInsertMultiThreadCheckCase()
        self.sStbDtbDdataInsertMultiThreadCheckCase()

        # # ! concurrency conflict
        # self.sStbDtbDdataAcMtInsertMultiThreadCheckCase()
        # self.sStbDtbDdataAtMcInsertMultiThreadCheckCase()

        self.sStbStbDdataDtsInsertMultiThreadCheckCase()

        # # ! concurrency conflict
        # self.sStbStbDdataDtsAcMtInsertMultiThreadCheckCase()
        # self.sStbStbDdataDtsAtMcInsertMultiThreadCheckCase()

        self.sStbDtbDdataDtsInsertMultiThreadCheckCase()

        # ! concurrency conflict
        # self.sStbDtbDdataDtsAcMtInsertMultiThreadCheckCase()



    def run(self):
        print("running {}".format(__file__))
        self.createDb()
        try:
            self.runAll()
        except Exception as err:
            print(''.join(traceback.format_exception(None, err, err.__traceback__)))
            raise err
        # self.tagColIllegalValueCheckCase()
        # self.test()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
