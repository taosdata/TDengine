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
from taos.error import SchemalessError
import time
import numpy as np
from util.log import *
from util.cases import *
from util.sql import *
from util.common import tdCom
from util.types import TDSmlProtocolType, TDSmlTimestampType
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

    def timeTrans(self, time_value, ts_type):
        # TDSmlTimestampType.HOUR.value, TDSmlTimestampType.MINUTE.value, TDSmlTimestampType.SECOND.value, TDSmlTimestampType.MICRO_SECOND.value, TDSmlTimestampType.NANO_SECOND.value
        if int(time_value) == 0:
            ts = time.time()
        else:
            if ts_type == TDSmlTimestampType.NANO_SECOND.value or ts_type is None:
                ts = int(''.join(list(filter(str.isdigit, time_value))))/1000000000
            elif ts_type == TDSmlTimestampType.MICRO_SECOND.value:
                ts = int(''.join(list(filter(str.isdigit, time_value))))/1000000
            elif ts_type == TDSmlTimestampType.MILLI_SECOND.value:
                ts = int(''.join(list(filter(str.isdigit, time_value))))/1000
            elif ts_type == TDSmlTimestampType.SECOND.value:
                ts = int(''.join(list(filter(str.isdigit, time_value))))/1
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

    def getTdTypeValue(self, value, vtype="col"):
        """
            vtype must be col or tag
        """
        if vtype == "col":
            if value.lower().endswith("i8"):
                td_type = "TINYINT"
                td_tag_value = ''.join(list(value)[:-2])
            elif value.lower().endswith("i16"):
                td_type = "SMALLINT"
                td_tag_value = ''.join(list(value)[:-3])
            elif value.lower().endswith("i32"):
                td_type = "INT"
                td_tag_value = ''.join(list(value)[:-3])
            elif value.lower().endswith("i64"):
                td_type = "BIGINT"
                td_tag_value = ''.join(list(value)[:-3])
            elif value.lower().lower().endswith("u64"):
                td_type = "BIGINT UNSIGNED"
                td_tag_value = ''.join(list(value)[:-3])
            elif value.lower().endswith("f32"):
                td_type = "FLOAT"
                td_tag_value = ''.join(list(value)[:-3])
                td_tag_value = '{}'.format(np.float32(td_tag_value))
            elif value.lower().endswith("f64"):
                td_type = "DOUBLE"
                td_tag_value = ''.join(list(value)[:-3])
                if "e" in value.lower():
                    td_tag_value = str(float(td_tag_value))
            elif value.lower().startswith('l"'):
                td_type = "NCHAR"
                td_tag_value = ''.join(list(value)[2:-1])
            elif value.startswith('"') and value.endswith('"'):
                td_type = "BINARY"
                td_tag_value = ''.join(list(value)[1:-1])
            elif value.lower() == "t" or value.lower() == "true":
                td_type = "BOOL"
                td_tag_value = "True"
            elif value.lower() == "f" or value.lower() == "false":
                td_type = "BOOL"
                td_tag_value = "False"
            elif value.isdigit():
                td_type = "DOUBLE"
                td_tag_value = str(float(value))
            else:
                td_type = "DOUBLE"
                if "e" in value.lower():
                    td_tag_value = str(float(value))
                else:
                    td_tag_value = value
        elif vtype == "tag":
            td_type = "NCHAR"
            td_tag_value = str(value)
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

    def inputHandle(self, input_sql, ts_type):
        input_sql_split_list = input_sql.split(" ")

        stb_tag_list = input_sql_split_list[0].split(',')
        stb_col_list = input_sql_split_list[1].split(',')
        ts_value = self.timeTrans(input_sql_split_list[2], ts_type)

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
                tag_name_list.append(elm.split("=")[0].lower())
                tag_value_list.append(elm.split("=")[1])
                tb_name = ""
                td_tag_value_list.append(self.getTdTypeValue(elm.split("=")[1], "tag")[1])
                td_tag_type_list.append(self.getTdTypeValue(elm.split("=")[1], "tag")[0])
        
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
                        c8="L\"ncharColValue\"", c9="7u64", ts="1626006833639000000", ts_type=None,
                        id_noexist_tag=None, id_change_tag=None, id_upper_tag=None, id_mixul_tag=None, id_double_tag=None,
                        ct_add_tag=None, ct_am_tag=None, ct_ma_tag=None, ct_min_tag=None, c_multi_tag=None, t_multi_tag=None,
                        c_blank_tag=None, t_blank_tag=None, chinese_tag=None):
        if stb_name == "":
            stb_name = tdCom.getLongName(len=6, mode="letters")
        if tb_name == "":
            tb_name = f'{stb_name}_{random.randint(0, 65535)}_{random.randint(0, 65535)}'
        if t0 == "":
            t0 = "t"
        if c0 == "":
            c0 = random.choice(["f", "F", "false", "False", "t", "T", "true", "True"])
        #sql_seq = f'{stb_name},id=\"{tb_name}\",t0={t0},t1=127i8,t2=32767i16,t3=125.22f64,t4=11.321f32,t5=11.12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" c0={bool_value},c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"binaryValue\",c8=L\"ncharValue\" 1626006833639000000'
        if id_upper_tag is not None:
            id = "ID"
        else:
            id = "id"
        if id_mixul_tag is not None:
            id = random.choice(["iD", "Id"])
        else:
            id = "id"
        sql_seq = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if id_noexist_tag is not None:
            sql_seq = f'{stb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
            if ct_add_tag is not None:
                sql_seq = f'{stb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8},t9={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if id_change_tag is not None:
            sql_seq = f'{stb_name},t0={t0},t1={t1},{id}={tb_name},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if id_double_tag is not None:
            sql_seq = f'{stb_name},{id}=\"{tb_name}_1\",t0={t0},t1={t1},{id}=\"{tb_name}_2\",t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if ct_add_tag is not None:
            sql_seq = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8},t11={t1},t10={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9},c11={c8},c10={t0} {ts}'
        if ct_am_tag is not None:
            sql_seq = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9},c11={c8},c10={t0} {ts}'
            if id_noexist_tag is not None:
                    sql_seq = f'{stb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9},c11={c8},c10={t0} {ts}'
        if ct_ma_tag is not None:
            sql_seq = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8},t11={t1},t10={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6} {ts}'
            if id_noexist_tag is not None:
                sql_seq = f'{stb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8},t11={t1},t10={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6} {ts}'
        if ct_min_tag is not None:
            sql_seq = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6} {ts}'
        if c_multi_tag is not None:
            sql_seq = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} c10={c9} {ts}'
        if t_multi_tag is not None:
            sql_seq = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} t9={t8} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if c_blank_tag is not None:
            sql_seq = f'{stb_name},{id}={tb_name},t0={t0},t1={t1},t2={t2},t3={t3},t4={t4},t5={t5},t6={t6},t7={t7},t8={t8} {ts}'
        if t_blank_tag is not None:
            sql_seq = f'{stb_name},{id}={tb_name} c0={c0},c1={c1},c2={c2},c3={c3},c4={c4},c5={c5},c6={c6},c7={c7},c8={c8},c9={c9} {ts}'
        if chinese_tag is not None:
            sql_seq = f'{stb_name},to=L"涛思数据" c0=L"涛思数据" {ts}'
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
        stb_name = tdCom.getLongName(7, mode="letters")
        tb_name = f'{stb_name}_1'
        tag_str = self.genMulTagColStr("tag", tag_count)
        col_str = self.genMulTagColStr("col", col_count)
        ts = "1626006833640000000"
        long_sql = stb_name + ',' + f'id={tb_name}' + ',' + tag_str + col_str + ts
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

    def resCmp(self, input_sql, stb_name, query_sql="select * from", ts_type=None, condition="", ts=None, id=True, none_check_tag=None, precision=None):
        expect_list = self.inputHandle(input_sql, ts_type)
        if precision == None:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, ts_type)
        else:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, precision)
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
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)

    def boolTypeCheckCase(self):
        """
            check all normal type
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
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
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        binary_symbols = '\"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"\"'
        nchar_symbols = f'L{binary_symbols}'
        input_sql, stb_name = self.genFullTypeSql(c7=binary_symbols, c8=nchar_symbols, t7=binary_symbols, t8=nchar_symbols)
        self.resCmp(input_sql, stb_name)

    def tsCheckCase(self):
        """
            test ts list --> ["1626006833639000000", "1626006833639019us", "1626006833640ms", "1626006834s", "1626006822639022"]
            # ! us级时间戳都为0时，数据库中查询显示，但python接口拿到的结果不显示 .000000的情况请确认，目前修改时间处理代码可以通过
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql, stb_name = self.genFullTypeSql(ts=1626006833639000000)
        self.resCmp(input_sql, stb_name, ts_type=TDSmlTimestampType.NANO_SECOND.value)
        input_sql, stb_name = self.genFullTypeSql(ts=1626006833639019)
        self.resCmp(input_sql, stb_name, ts_type=TDSmlTimestampType.MICRO_SECOND.value)
        input_sql, stb_name = self.genFullTypeSql(ts=1626006833640)
        self.resCmp(input_sql, stb_name, ts_type=TDSmlTimestampType.MILLI_SECOND.value)
        input_sql, stb_name = self.genFullTypeSql(ts=1626006834)
        self.resCmp(input_sql, stb_name, ts_type=TDSmlTimestampType.SECOND.value)
        input_sql, stb_name = self.genFullTypeSql(ts=1626006833639000000)
        self.resCmp(input_sql, stb_name, ts_type=None)
        input_sql, stb_name = self.genFullTypeSql(ts=0)
        self.resCmp(input_sql, stb_name, ts=0)

        tdSql.execute(f"drop database if exists test_ts")
        tdSql.execute(f"create database if not exists test_ts precision 'ms'")
        tdSql.execute("use test_ts")
        input_sql = ['test_ms,t0=t c0=t 1626006833640', 'test_ms,t0=t c0=f 1626006833641']
        self._conn.schemaless_insert(input_sql, TDSmlProtocolType.LINE.value, TDSmlTimestampType.MILLI_SECOND.value)
        res = tdSql.query('select * from test_ms', True)
        tdSql.checkEqual(str(res[0][0]), "2021-07-11 20:33:53.640000")
        tdSql.checkEqual(str(res[1][0]), "2021-07-11 20:33:53.641000")

        tdSql.execute(f"drop database if exists test_ts")
        tdSql.execute(f"create database if not exists test_ts precision 'us'")
        tdSql.execute("use test_ts")
        input_sql = ['test_us,t0=t c0=t 1626006833639000', 'test_us,t0=t c0=f 1626006833639001']
        self._conn.schemaless_insert(input_sql, TDSmlProtocolType.LINE.value, TDSmlTimestampType.MICRO_SECOND.value)
        res = tdSql.query('select * from test_us', True)
        tdSql.checkEqual(str(res[0][0]), "2021-07-11 20:33:53.639000")
        tdSql.checkEqual(str(res[1][0]), "2021-07-11 20:33:53.639001")

        tdSql.execute(f"drop database if exists test_ts")
        tdSql.execute(f"create database if not exists test_ts precision 'ns'")
        tdSql.execute("use test_ts")
        input_sql = ['test_ns,t0=t c0=t 1626006833639000000', 'test_ns,t0=t c0=f 1626006833639000001']
        self._conn.schemaless_insert(input_sql, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        res = tdSql.query('select * from test_ns', True)
        tdSql.checkEqual(str(res[0][0]), "1626006833639000000")
        tdSql.checkEqual(str(res[1][0]), "1626006833639000001")

        self.createDb()

    def zeroTsCheckCase(self):
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        for ts_tag in [TDSmlTimestampType.HOUR.value, TDSmlTimestampType.MINUTE.value, TDSmlTimestampType.SECOND.value, TDSmlTimestampType.MICRO_SECOND.value, TDSmlTimestampType.NANO_SECOND.value]:
            input_sql = f'{tdCom.getLongName(len=10, mode="letters")},t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 0'
            stb_name = input_sql.split(",")[0]
            self.resCmp(input_sql, stb_name, ts=0, precision=ts_tag)
    
    def influxTsCheckCase(self):
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql = f'{tdCom.getLongName(len=10, mode="letters")},t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 454093'
        stb_name = input_sql.split(",")[0]
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.HOUR.value)
        res = tdSql.query(f'select * from {stb_name}', True)
        tdSql.checkEqual(str(res[0][0]), "2021-10-20 21:00:00")
        input_sql = f'{tdCom.getLongName(len=10, mode="letters")},t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 454094'
        stb_name = input_sql.split(",")[0]
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.HOUR.value)
        res = tdSql.query(f'select * from {stb_name}', True)
        tdSql.checkEqual(str(res[0][0]), "2021-10-20 22:00:00")

        input_sql = f'{tdCom.getLongName(len=10, mode="letters")},t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 27245538'
        stb_name = input_sql.split(",")[0]
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.MINUTE.value)
        res = tdSql.query(f'select * from {stb_name}', True)
        tdSql.checkEqual(str(res[0][0]), "2021-10-20 20:18:00")
        input_sql = f'{tdCom.getLongName(len=10, mode="letters")},t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 27245539'
        stb_name = input_sql.split(",")[0]
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.MINUTE.value)
        res = tdSql.query(f'select * from {stb_name}', True)
        tdSql.checkEqual(str(res[0][0]), "2021-10-20 20:19:00")

        input_sql = f'{tdCom.getLongName(len=10, mode="letters")},t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 1634731694'
        stb_name = input_sql.split(",")[0]
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.SECOND.value)
        res = tdSql.query(f'select * from {stb_name}', True)
        tdSql.checkEqual(str(res[0][0]), "2021-10-20 20:08:14")
        input_sql = f'{tdCom.getLongName(len=10, mode="letters")},t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 1634731695'
        stb_name = input_sql.split(",")[0]
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.SECOND.value)
        res = tdSql.query(f'select * from {stb_name}', True)
        tdSql.checkEqual(str(res[0][0]), "2021-10-20 20:08:15")

        input_sql = f'{tdCom.getLongName(len=10, mode="letters")},t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 1634731684002'
        stb_name = input_sql.split(",")[0]
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.MILLI_SECOND.value)
        res = tdSql.query(f'select * from {stb_name}', True)
        tdSql.checkEqual(str(res[0][0]), "2021-10-20 20:08:04.002000")
        input_sql = f'{tdCom.getLongName(len=10, mode="letters")},t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 1634731684003'
        stb_name = input_sql.split(",")[0]
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.MILLI_SECOND.value)
        res = tdSql.query(f'select * from {stb_name}', True)
        tdSql.checkEqual(str(res[0][0]), "2021-10-20 20:08:04.003000")

        input_sql = f'{tdCom.getLongName(len=10, mode="letters")},t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 1634731684000001'
        stb_name = input_sql.split(",")[0]
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.MICRO_SECOND.value)
        res = tdSql.query(f'select * from {stb_name}', True)
        tdSql.checkEqual(str(res[0][0]), "2021-10-20 20:08:04.000001")
        input_sql = f'{tdCom.getLongName(len=10, mode="letters")},t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 1634731684000002'
        stb_name = input_sql.split(",")[0]
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.MICRO_SECOND.value)
        res = tdSql.query(f'select * from {stb_name}', True)
        tdSql.checkEqual(str(res[0][0]), "2021-10-20 20:08:04.000002")

        input_sql = f'{tdCom.getLongName(len=10, mode="letters")},t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 1626006833639000000'
        stb_name = input_sql.split(",")[0]
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        res = tdSql.query(f'select * from {stb_name}', True)
        tdSql.checkEqual(str(res[0][0]), "2021-07-11 20:33:53.639000")
        input_sql = f'{tdCom.getLongName(len=10, mode="letters")},t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 1626007833639000000'
        stb_name = input_sql.split(",")[0]
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        res = tdSql.query(f'select * from {stb_name}', True)
        tdSql.checkEqual(str(res[0][0]), "2021-07-11 20:50:33.639000")

    def iuCheckCase(self):
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql = f'{tdCom.getLongName(len=10, mode="letters")},t0=127 c1=9223372036854775807i,c2=1u 0'
        stb_name = input_sql.split(",")[0]
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        tdSql.query(f'describe {stb_name}', True)
        tdSql.checkData(1, 1, "BIGINT")
        tdSql.checkData(2, 1, "BIGINT UNSIGNED")

    def idSeqCheckCase(self):
        """
            check id.index in tags
            eg: t0=**,id=**,t1=**
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql, stb_name = self.genFullTypeSql(id_change_tag=True)
        self.resCmp(input_sql, stb_name)
    
    def idLetterCheckCase(self):
        """
            check id param
            eg: id and ID
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql, stb_name = self.genFullTypeSql(id_upper_tag=True)
        self.resCmp(input_sql, stb_name)
        input_sql, stb_name = self.genFullTypeSql(id_mixul_tag=True)
        self.resCmp(input_sql, stb_name)
        input_sql, stb_name = self.genFullTypeSql(id_change_tag=True, id_upper_tag=True)
        self.resCmp(input_sql, stb_name)

    def noIdCheckCase(self):
        """
            id not exist
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
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
            max col count is 4096
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        for input_sql in [self.genLongSql(128, 1)[0], self.genLongSql(1, 4094)[0]]:
            tdCom.cleanTb()
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        for input_sql in [self.genLongSql(129, 1)[0], self.genLongSql(1, 4095)[0]]:
            tdCom.cleanTb()
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)
            
    def stbTbNameCheckCase(self):
        """
            test illegal id name
            mix "~!@#$¥%^&*()-+={}|[]、「」【】\:;《》<>?"
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        rstr = list("~!@#$¥%^&*()-+=|[]、「」【】\;:《》<>?")
        for i in rstr:
            stb_name=f"aaa{i}bbb"
            input_sql = self.genFullTypeSql(stb_name=stb_name, tb_name=f'{stb_name}_sub')[0]
            self.resCmp(input_sql, f'`{stb_name}`')
            tdSql.execute(f'drop table if exists `{stb_name}`')

    def idStartWithNumCheckCase(self):
        """
            id is start with num
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql, stb_name = self.genFullTypeSql(tb_name="1aaabbb")
        self.resCmp(input_sql, stb_name)

    def nowTsCheckCase(self):
        """
            check now unsupported
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql = self.genFullTypeSql(ts="now")[0]
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def dateFormatTsCheckCase(self):
        """
            check date format ts unsupported
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql = self.genFullTypeSql(ts="2021-07-21\ 19:01:46.920")[0]
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)
    
    def illegalTsCheckCase(self):
        """
            check ts format like 16260068336390us19
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql = self.genFullTypeSql(ts="16260068336390us19")[0]
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def tbnameCheckCase(self):
        """
            check length 192
            check upper tbname
            chech upper tag
            length of stb_name tb_name <= 192
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        stb_name_192 = tdCom.getLongName(len=192, mode="letters")
        tb_name_192 = tdCom.getLongName(len=192, mode="letters")
        input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name_192, tb_name=tb_name_192)
        self.resCmp(input_sql, stb_name)
        tdSql.query(f'select * from {stb_name}')
        tdSql.checkRows(1)
        for input_sql in [self.genFullTypeSql(stb_name=tdCom.getLongName(len=193, mode="letters"), tb_name=tdCom.getLongName(len=5, mode="letters"))[0], self.genFullTypeSql(tb_name=tdCom.getLongName(len=193, mode="letters"))[0]]:
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        input_sql = 'Abcdffgg,id=Abcddd,T1=127i8 c0=False 1626006833639000000'
        stb_name = "Abcdffgg"
        self.resCmp(input_sql, stb_name)

    def tagValueLengthCheckCase(self):
        """
            check full type tag value limit
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        # nchar
        # * legal nchar could not be larger than 16374/4
        stb_name = tdCom.getLongName(7, "letters")
        input_sql = f'{stb_name},t0=t,t1={tdCom.getLongName(4093, "letters")} c0=f 1626006833639000000'
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        input_sql = f'{stb_name},t0=t,t1={tdCom.getLongName(4094, "letters")} c0=f 1626006833639000000'
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def colValueLengthCheckCase(self):
        """
            check full type col value limit
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        # i8
        for c1 in ["-127i8", "127i8"]:
            input_sql, stb_name = self.genFullTypeSql(c1=c1)
            self.resCmp(input_sql, stb_name)

        for c1 in ["-128i8", "128i8"]:
            input_sql = self.genFullTypeSql(c1=c1)[0]
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)
        # i16
        for c2 in ["-32767i16"]:
            input_sql, stb_name = self.genFullTypeSql(c2=c2)
            self.resCmp(input_sql, stb_name)
        for c2 in ["-32768i16", "32768i16"]:
            input_sql = self.genFullTypeSql(c2=c2)[0]
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # i32
        for c3 in ["-2147483647i32"]:
            input_sql, stb_name = self.genFullTypeSql(c3=c3)
            self.resCmp(input_sql, stb_name)
        for c3 in ["-2147483648i32", "2147483648i32"]:
            input_sql = self.genFullTypeSql(c3=c3)[0]
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # i64
        for c4 in ["-9223372036854775807i64"]:
            input_sql, stb_name = self.genFullTypeSql(c4=c4)
            self.resCmp(input_sql, stb_name)
        for c4 in ["-9223372036854775808i64", "9223372036854775808i64"]:
            input_sql = self.genFullTypeSql(c4=c4)[0]
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # f32       
        for c5 in [f"{-3.4028234663852885981170418348451692544*(10**38)}f32", f"{3.4028234663852885981170418348451692544*(10**38)}f32"]:
            input_sql, stb_name = self.genFullTypeSql(c5=c5)
            self.resCmp(input_sql, stb_name)
        # * limit set to 4028234664*(10**38)
        for c5 in [f"{-3.4028234664*(10**38)}f32", f"{3.4028234664*(10**38)}f32"]:
            input_sql = self.genFullTypeSql(c5=c5)[0]
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # f64
        for c6 in [f'{-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)}f64', f'{-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)}f64']:
            input_sql, stb_name = self.genFullTypeSql(c6=c6)
            self.resCmp(input_sql, stb_name)
        # * limit set to 1.797693134862316*(10**308)
        for c6 in [f'{-1.797693134862316*(10**308)}f64', f'{-1.797693134862316*(10**308)}f64']:
            input_sql = self.genFullTypeSql(c6=c6)[0]
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # # binary 
        stb_name = tdCom.getLongName(7, "letters")
        input_sql = f'{stb_name},t0=t c0=f,c1="{tdCom.getLongName(16374, "letters")}" 1626006833639000000'
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        input_sql = f'{stb_name},t0=t c0=f,c1="{tdCom.getLongName(16375, "letters")}" 1626006833639000000'
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

        # nchar
        # * legal nchar could not be larger than 16374/4
        stb_name = tdCom.getLongName(7, "letters")
        input_sql = f'{stb_name},t0=t c0=f,c1=L"{tdCom.getLongName(4093, "letters")}" 1626006833639000000'
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        input_sql = f'{stb_name},t0=t c0=f,c1=L"{tdCom.getLongName(4094, "letters")}" 1626006833639000000'
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def tagColIllegalValueCheckCase(self):

        """
            test illegal tag col value
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        # bool
        for i in ["TrUe", "tRue", "trUe", "truE", "FalsE", "fAlse", "faLse", "falSe", "falsE"]:
            input_sql1, stb_name = self.genFullTypeSql(t0=i)
            self.resCmp(input_sql1, stb_name)
            input_sql2, stb_name = self.genFullTypeSql(c0=i)
            self.resCmp(input_sql2, stb_name)

        # i8 i16 i32 i64 f32 f64
        for input_sql in [
                self.genFullTypeSql(c1="1s2i8")[0], 
                self.genFullTypeSql(c2="1s2i16")[0],
                self.genFullTypeSql(c3="1s2i32")[0],
                self.genFullTypeSql(c4="1s2i64")[0],
                self.genFullTypeSql(c5="11.1s45f32")[0],
                self.genFullTypeSql(c6="11.1s45f64")[0],
                self.genFullTypeSql(c9="1s1u64")[0]
            ]:
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # check binary and nchar blank
        stb_name = tdCom.getLongName(7, "letters")
        input_sql1 = f'{stb_name}_1,t0=t c0=f,c1="abc aaa" 1626006833639000000'
        input_sql2 = f'{stb_name}_2,t0=t c0=f,c1=L"abc aaa" 1626006833639000000'
        input_sql3 = f'{stb_name}_3,t0=t,t1="abc aaa" c0=f 1626006833639000000'
        input_sql4 = f'{stb_name}_4,t0=t,t1=L"abc aaa" c0=f 1626006833639000000'
        for input_sql in [input_sql1, input_sql2, input_sql3, input_sql4]:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        # check accepted binary and nchar symbols 
        # # * ~!@#$¥%^&*()-+={}|[]、「」:;
        for symbol in list('~!@#$¥%^&*()-+={}|[]、「」:;'):
            input_sql1 = f'{stb_name},t0=t c0=f,c1="abc{symbol}aaa" 1626006833639000000'
            input_sql2 = f'{stb_name},t0=t,t1="abc{symbol}aaa" c0=f 1626006833639000000'
            self._conn.schemaless_insert([input_sql1], TDSmlProtocolType.LINE.value, None)
            self._conn.schemaless_insert([input_sql2], TDSmlProtocolType.LINE.value, None)

    def duplicateIdTagColInsertCheckCase(self):
        """
            check duplicate Id Tag Col
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql_id = self.genFullTypeSql(id_double_tag=True)[0]
        try:
            self._conn.schemaless_insert([input_sql_id], TDSmlProtocolType.LINE.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

        input_sql = self.genFullTypeSql()[0]
        input_sql_tag = input_sql.replace("t5", "t6")
        try:
            self._conn.schemaless_insert([input_sql_tag], TDSmlProtocolType.LINE.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

        input_sql = self.genFullTypeSql()[0]
        input_sql_col = input_sql.replace("c5", "c6")
        try:
            self._conn.schemaless_insert([input_sql_col], TDSmlProtocolType.LINE.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

        input_sql = self.genFullTypeSql()[0]
        input_sql_col = input_sql.replace("c5", "C6")
        try:
            self._conn.schemaless_insert([input_sql_col], TDSmlProtocolType.LINE.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    ##### stb exist #####
    def noIdStbExistCheckCase(self):
        """
            case no id when stb exist
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql, stb_name = self.genFullTypeSql(tb_name="sub_table_0123456", t0="f", c0="f")
        self.resCmp(input_sql, stb_name)
        input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name, id_noexist_tag=True, t0="f", c0="f")
        self.resCmp(input_sql, stb_name, condition='where tbname like "t_%"')
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)

    def duplicateInsertExistCheckCase(self):
        """
            check duplicate insert when stb exist
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.resCmp(input_sql, stb_name)

    def tagColBinaryNcharLengthCheckCase(self):
        """
            check length increase
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        tb_name = tdCom.getLongName(5, "letters")
        input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7="\"binaryTagValuebinaryTagValue\"", t8="L\"ncharTagValuencharTagValue\"", c7="\"binaryTagValuebinaryTagValue\"", c8="L\"ncharTagValuencharTagValue\"")
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
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        tb_name = tdCom.getLongName(7, "letters")
        for db_update_tag in [0, 1]:
            if db_update_tag == 1 :
                self.createDb("test_update", db_update_tag=db_update_tag)
            input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name, t0="t", c0="t")
            self.resCmp(input_sql, stb_name)
            input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t0="t", c0="f", ct_add_tag=True)
            if db_update_tag == 1 :
                self.resCmp(input_sql, stb_name, condition=f'where tbname like "{tb_name}"', none_check_tag=True)
                tdSql.query(f'select * from {stb_name} where tbname like "{tb_name}"')
                tdSql.checkData(0, 11, "ncharColValue")  
                tdSql.checkData(0, 12, True)  
                tdSql.checkData(0, 22, None)  
                tdSql.checkData(0, 23, None)  
            else:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                tdSql.query(f'select * from {stb_name} where tbname like "{tb_name}"')
                tdSql.checkData(0, 1, True)  
                tdSql.checkData(0, 11, None)  
                tdSql.checkData(0, 12, None)  
                tdSql.checkData(0, 22, None)  
                tdSql.checkData(0, 23, None)  
            self.createDb()

    def tagColAddCheckCase(self):
        """
            check column and tag count add
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        tb_name = tdCom.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name, t0="f", c0="f")
        self.resCmp(input_sql, stb_name)
        tb_name_1 = tdCom.getLongName(7, "letters")
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
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
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
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        tb_name3 = self.getNoIdTbName(stb_name)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)
        tdSql.checkNotEqual(tb_name1, tb_name3)

    # * tag binary max is 16384, col+ts binary max  49151
    def tagColBinaryMaxLengthCheckCase(self):
        """
            every binary and nchar must be length+2
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        stb_name = tdCom.getLongName(7, "letters")
        tb_name = f'{stb_name}_1'
        input_sql = f'{stb_name},id={tb_name},t0=t c0=f 1626006833639000000'
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        # # * check col，col+ts max in describe ---> 16143
        input_sql = f'{stb_name},t0=t c0=f,c1="{tdCom.getLongName(16374, "letters")}",c2="{tdCom.getLongName(16374, "letters")}",c3="{tdCom.getLongName(16374, "letters")}",c4="{tdCom.getLongName(12, "letters")}" 1626006833639000000'
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)
        input_sql = f'{stb_name},t0=t c0=f,c1="{tdCom.getLongName(16374, "letters")}",c2="{tdCom.getLongName(16374, "letters")}",c3="{tdCom.getLongName(16374, "letters")}",c4="{tdCom.getLongName(13, "letters")}" 1626006833639000000'
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)
    
    # * tag nchar max is 16374/4, col+ts nchar max  49151
    def tagColNcharMaxLengthCheckCase(self):
        """
            check nchar length limit
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        stb_name = tdCom.getLongName(7, "letters")
        tb_name = f'{stb_name}_1'
        input_sql = f'{stb_name},id={tb_name},t2={tdCom.getLongName(1, "letters")} c0=f 1626006833639000000'
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        # * legal nchar could not be larger than 16374/4
        input_sql = f'{stb_name},t1={tdCom.getLongName(4093, "letters")},t2={tdCom.getLongName(1, "letters")} c0=f 1626006833639000000'
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)
        input_sql = f'{stb_name},t1={tdCom.getLongName(4093, "letters")},t2={tdCom.getLongName(2, "letters")} c0=f 1626006833639000000'
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)

        input_sql = f'{stb_name},t2={tdCom.getLongName(1, "letters")} c0=f,c1=L"{tdCom.getLongName(4093, "letters")}",c2=L"{tdCom.getLongName(4093, "letters")}",c3=L"{tdCom.getLongName(4093, "letters")}",c4=L"{tdCom.getLongName(4, "letters")}" 1626006833639000000'
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(3)
        input_sql = f'{stb_name},t2={tdCom.getLongName(1, "letters")} c0=f,c1=L"{tdCom.getLongName(4093, "letters")}",c2=L"{tdCom.getLongName(4093, "letters")}",c3=L"{tdCom.getLongName(4093, "letters")}",c4=L"{tdCom.getLongName(5, "letters")}" 1626006833639000000'
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(3)

    def batchInsertCheckCase(self):
        """
            test batch insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        stb_name = tdCom.getLongName(8, "letters")
        tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 bigint)')
        lines = ["st123456,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "st123456,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000",
                f"{stb_name},t2=5f64,t3=L\"ste\" c1=true,c2=4i64,c3=\"iam\" 1626056811823316532",
                "stf567890,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000",
                "st123456,t1=4i64,t2=5f64,t3=\"t4\" c1=3i64,c3=L\"passitagain\",c2=true,c4=5f64 1626006833642000000",
                f"{stb_name},t2=5f64,t3=L\"ste2\" c3=\"iamszhou\",c4=false 1626056811843316532",
                f"{stb_name},t2=5f64,t3=L\"ste2\" c3=\"iamszhou\",c4=false,c5=32i8,c6=64i16,c7=32i32,c8=88.88f32 1626056812843316532",
                "st123456,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000",
                "st123456,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64   c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64   1626006933641000000"
                ]
        self._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        tdSql.query('show stables')
        tdSql.checkRows(3)
        tdSql.query('show tables')
        tdSql.checkRows(6)
        tdSql.query('select * from st123456')
        tdSql.checkRows(5)
    
    def multiInsertCheckCase(self, count):
        """
            test multi insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        sql_list = []
        stb_name = tdCom.getLongName(8, "letters")
        tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 nchar(10))')
        for i in range(count):
            input_sql = self.genFullTypeSql(stb_name=stb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', c7=f'"{tdCom.getLongName(8, "letters")}"', id_noexist_tag=True)[0]
            sql_list.append(input_sql)
        self._conn.schemaless_insert(sql_list, TDSmlProtocolType.LINE.value, None)
        tdSql.query('show tables')
        tdSql.checkRows(count)

    def batchErrorInsertCheckCase(self):
        """
            test batch error insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        stb_name = tdCom.getLongName(8, "letters")
        lines = ["st123456,t1=3i64,t2=4f64,t3=\"t3\" c1=3i 64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                f"{stb_name},t2=5f64,t3=L\"ste\" c1=tRue,c2=4i64,c3=\"iam\" 1626056811823316532ns"]
        try:
            self._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def multiColsInsertCheckCase(self):
        """
            test multi cols insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql = self.genFullTypeSql(c_multi_tag=True)[0]
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)
    
    def multiTagsInsertCheckCase(self):
        """
            test multi tags insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql = self.genFullTypeSql(t_multi_tag=True)[0]
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)
    
    def blankColInsertCheckCase(self):
        """
            test blank col insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql = self.genFullTypeSql(c_blank_tag=True)[0]
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def blankTagInsertCheckCase(self):
        """
            test blank tag insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql = self.genFullTypeSql(t_blank_tag=True)[0]
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)
    
    def chineseCheckCase(self):
        """
            check nchar ---> chinese
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql, stb_name = self.genFullTypeSql(chinese_tag=True)
        self.resCmp(input_sql, stb_name)

    def spellCheckCase(self):
        stb_name = tdCom.getLongName(8, "letters")
        tdCom.cleanTb()
        input_sql_list = [f'{stb_name}_1,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=22.123456789F64 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=22.123456789F64 1626006833639000000',
                            f'{stb_name}_2,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=22.123456789F64 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=22.123456789F64 1626006833639000000',
                            f'{stb_name}_3,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=22.123456789F64 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=22.123456789F64 1626006833639000000',
                            f'{stb_name}_4,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=22.123456789F64 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=22.123456789F64 1626006833639000000',
                            f'{stb_name}_5,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=22.123456789F64 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=22.123456789F64 1626006833639000000',
                            f'{stb_name}_6,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=22.123456789F64 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=22.123456789F64 1626006833639000000',
                            f'{stb_name}_7,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=22.123456789F64 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=22.123456789F64 1626006833639000000',
                            f'{stb_name}_8,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=22.123456789F64 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=22.123456789F64 1626006833639000000',
                            f'{stb_name}_9,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=22.123456789F64 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=22.123456789F64 1626006833639000000',
                            f'{stb_name}_10,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=22.123456789F64 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=22.123456789F64 1626006833639000000']
        for input_sql in input_sql_list:
            stb_name = input_sql.split(',')[0]
            self.resCmp(input_sql, stb_name)

    def defaultTypeCheckCase(self):
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        stb_name = tdCom.getLongName(8, "letters")
        input_sql_list = [f'{stb_name}_1,t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 1626006833639000000',
                            f'{stb_name}_2,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=22.123456789 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=22.123456789 1626006833639000000',
                            f'{stb_name}_3,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=10e5F32 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=10e5F64 1626006833639000000',
                            f'{stb_name}_4,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=10.0e5f64 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=10.0e5f32 1626006833639000000',
                            f'{stb_name}_5,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=-10.0e5 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=-10.0e5 1626006833639000000']
        for input_sql in input_sql_list:
            stb_name = input_sql.split(",")[0]
            self.resCmp(input_sql, stb_name)

    def tbnameTagsColsNameCheckCase(self):
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql = 'rFa$sta,id=rFas$ta_1,Tt!0=true,tT@1=127i8,t#2=32767i16,\"t$3\"=2147483647i32,t%4=9223372036854775807i64,t^5=11.12345f32,t&6=22.123456789f64,t*7=\"ddzhiksj\",t!@#$%^&*()_+[];:<>?,9=L\"ncharTagValue\" C)0=True,c{1=127i8,c[2=32767i16,c;3=2147483647i32,c:4=9223372036854775807i64,c<5=11.12345f32,c>6=22.123456789f64,c?7=\"bnhwlgvj\",c.8=L\"ncharTagValue\",c!@#$%^&*()_+[];:<>?,=7u64 1626006933640000000'
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        query_sql = 'select * from `rfa$sta`'
        query_res = tdSql.query(query_sql, True)
        tdSql.checkEqual(query_res, [(datetime.datetime(2021, 7, 11, 20, 35, 33, 640000), True, 127, 32767, 2147483647, 9223372036854775807, 11.12345027923584, 22.123456789, 'bnhwlgvj', 'ncharTagValue', 7, 'true', '127i8', '32767i16', '2147483647i32', '9223372036854775807i64', '11.12345f32', '22.123456789f64', '"ddzhiksj"', 'L"ncharTagValue"')])
        col_tag_res = tdSql.getColNameList(query_sql)
        tdSql.checkEqual(col_tag_res, ['_ts', 'c)0', 'c{1', 'c[2', 'c;3', 'c:4', 'c<5', 'c>6', 'c?7', 'c.8', 'c!@#$%^&*()_+[];:<>?,', 'tt!0', 'tt@1', 't#2', '"t$3"', 't%4', 't^5', 't&6', 't*7', 't!@#$%^&*()_+[];:<>?,9'])
        tdSql.execute('drop table `rfa$sta`')

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
            d_stb_d_tb_list.append(self.genFullTypeSql(c0="t"))
            s_stb_s_tb_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', c7=f'"{tdCom.getLongName(8, "letters")}"'))
            s_stb_s_tb_a_col_a_tag_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', c7=f'"{tdCom.getLongName(8, "letters")}"', ct_add_tag=True))
            s_stb_s_tb_m_col_m_tag_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', c7=f'"{tdCom.getLongName(8, "letters")}"', ct_min_tag=True))
            s_stb_d_tb_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', c7=f'"{tdCom.getLongName(8, "letters")}"', id_noexist_tag=True))
            s_stb_d_tb_a_col_m_tag_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', c7=f'"{tdCom.getLongName(8, "letters")}"', id_noexist_tag=True, ct_am_tag=True))
            s_stb_d_tb_a_tag_m_col_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', c7=f'"{tdCom.getLongName(8, "letters")}"', id_noexist_tag=True, ct_ma_tag=True))
            s_stb_s_tb_d_ts_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', c7=f'"{tdCom.getLongName(8, "letters")}"', ts=0))
            s_stb_s_tb_d_ts_a_col_m_tag_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', c7=f'"{tdCom.getLongName(8, "letters")}"', ts=0, ct_am_tag=True))
            s_stb_s_tb_d_ts_a_tag_m_col_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', c7=f'"{tdCom.getLongName(8, "letters")}"', ts=0, ct_ma_tag=True))
            s_stb_d_tb_d_ts_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', c7=f'"{tdCom.getLongName(8, "letters")}"', id_noexist_tag=True, ts=0))
            s_stb_d_tb_d_ts_a_col_m_tag_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', c7=f'"{tdCom.getLongName(8, "letters")}"', id_noexist_tag=True, ts=0, ct_am_tag=True))
            s_stb_d_tb_d_ts_a_tag_m_col_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', c7=f'"{tdCom.getLongName(8, "letters")}"', id_noexist_tag=True, ts=0, ct_ma_tag=True))

        return d_stb_d_tb_list, s_stb_s_tb_list, s_stb_s_tb_a_col_a_tag_list, s_stb_s_tb_m_col_m_tag_list, \
            s_stb_d_tb_list, s_stb_d_tb_a_col_m_tag_list, s_stb_d_tb_a_tag_m_col_list, s_stb_s_tb_d_ts_list, \
            s_stb_s_tb_d_ts_a_col_m_tag_list, s_stb_s_tb_d_ts_a_tag_m_col_list, s_stb_d_tb_d_ts_list, \
            s_stb_d_tb_d_ts_a_col_m_tag_list, s_stb_d_tb_d_ts_a_tag_m_col_list


    def genMultiThreadSeq(self, sql_list):
        tlist = list()
        for insert_sql in sql_list:
            t = threading.Thread(target=self._conn.schemaless_insert,args=([insert_sql[0]], TDSmlProtocolType.LINE.value, None))
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
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql = self.genSqlList()[0]
        self.multiThreadRun(self.genMultiThreadSeq(input_sql))
        tdSql.query(f"show tables;")
        tdSql.checkRows(5)
    
    def sStbStbDdataInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different data, result keep first data
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        tb_name = tdCom.getLongName(7, "letters")
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
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        tb_name = tdCom.getLongName(7, "letters")
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
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        tb_name = tdCom.getLongName(7, "letters")
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
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        s_stb_d_tb_list = self.genSqlList(stb_name=stb_name)[4]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(6)

    def sStbDtbDdataAcMtInsertMultiThreadCheckCase(self):
        """
            thread input same stb, different tb, different data, add col, mul tag
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        # s_stb_d_tb_a_col_m_tag_list = self.genSqlList(stb_name=stb_name)[5]
        s_stb_d_tb_a_col_m_tag_list = [(f'{stb_name},t0=F,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="ngxgzdzs",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=F 1626006833639000000', 'hpxbys'), \
                                        (f'{stb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="vvfrdtty",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=True 1626006833639000000', 'hpxbys'), \
                                        (f'{stb_name},t0=F,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="kzscucnt",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=f 1626006833639000000', 'hpxbys'), \
                                        (f'{stb_name},t0=F,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="asegdbqk",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=false 1626006833639000000', 'hpxbys'), \
                                        (f'{stb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="yvqnhgmn",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=T 1626006833639000000', 'hpxbys')]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_a_col_m_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(3)

    def sStbDtbDdataAtMcInsertMultiThreadCheckCase(self):
        """
            thread input same stb, different tb, different data, add tag, mul col
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
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
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        tb_name = tdCom.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name)
        self.resCmp(input_sql, stb_name)
        # s_stb_s_tb_d_ts_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name)[7]
        s_stb_s_tb_d_ts_list =[(f'{stb_name},id={tb_name},t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="tgqkvsws",t8=L"ncharTagValue" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="htvnnldm",c8=L"ncharColValue",c9=7u64 0', 'sfzqdz'), \
                                (f'{stb_name},id={tb_name},t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="fvrhhqiy",t8=L"ncharTagValue" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="gybqvhos",c8=L"ncharColValue",c9=7u64 0', 'sfzqdz'), \
                                (f'{stb_name},id={tb_name},t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="vifkabhu",t8=L"ncharTagValue" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="zlvxgquy",c8=L"ncharColValue",c9=7u64 0', 'sfzqdz'), \
                                (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="lsyotcrn",t8=L"ncharTagValue" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="oaupfgtz",c8=L"ncharColValue",c9=7u64 0', 'sfzqdz'), \
                                (f'{stb_name},id={tb_name},t0=T,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="jrwamcgy",t8=L"ncharTagValue" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="vgzadjsh",c8=L"ncharColValue",c9=7u64 0', 'sfzqdz')]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_d_ts_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1)
        # ! Small probability bug ---> temporarily delete it
        # tdSql.query(f"select * from {stb_name}")
        # tdSql.checkRows(6)

    def sStbStbDdataDtsAcMtInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different ts, add col, mul tag
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        tb_name = tdCom.getLongName(7, "letters")
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
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        tb_name = tdCom.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name)
        self.resCmp(input_sql, stb_name)
        # s_stb_s_tb_d_ts_a_tag_m_col_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name)[9]
        s_stb_s_tb_d_ts_a_tag_m_col_list = [(f'{stb_name},id={tb_name},t0=T,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="xsajdfjc",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 0', 'rgqcfb'), \
                                            (f'{stb_name},id={tb_name},t0=T,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="qzeyolgt",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 0', 'rgqcfb'), \
                                            (f'{stb_name},id={tb_name},t0=False,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="suxqziwh",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 0', 'rgqcfb'), \
                                            (f'{stb_name},id={tb_name},t0=False,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="vapolpgr",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 0', 'rgqcfb'), \
                                            (f'{stb_name},id={tb_name},t0=False,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="eustwpfl",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 0', 'rgqcfb')]
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
            tdSql.checkRows(0)

    def sStbDtbDdataDtsInsertMultiThreadCheckCase(self):
        """
            thread input same stb, different tb, data, ts
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        s_stb_d_tb_d_ts_list = self.genSqlList(stb_name=stb_name)[10]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_d_ts_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(6)

    def sStbDtbDdataDtsAcMtInsertMultiThreadCheckCase(self):
        """
            thread input same stb, different tb, data, ts, add col, mul tag
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb()
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        # s_stb_d_tb_d_ts_a_col_m_tag_list = self.genSqlList(stb_name=stb_name)[11]
        s_stb_d_tb_d_ts_a_col_m_tag_list = [(f'{stb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="eltflgpz",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=True 0', 'ynnlov'), \
                                            (f'{stb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="ysznggwl",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=t 0', 'ynnlov'), \
                                            (f'{stb_name},t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="nxwjucch",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=f 0', 'ynnlov'), \
                                            (f'{stb_name},t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="fzseicnt",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=F 0', 'ynnlov'), \
                                            (f'{stb_name},t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="zwgurhdp",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=False 0', 'ynnlov')]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_d_ts_a_col_m_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(3)

    def test(self):
        input_sql1 = "rfa$sta,id=rfas$ta_1,T!0=true,t@1=127i8,t#2=32767i16,t$3=2147483647i32,t%4=9223372036854775807i64,t^5=11.12345f32,t&6=22.123456789f64,t*7=\"ddzhiksj\",t(8=L\"ncharTagValue\" C)0=True,c{1=127i8,c[2=32767i16,c;3=2147483647i32,c:4=9223372036854775807i64,c<5=11.12345f32,c>6=22.123456789f64,c?7=\"bnhwlgvj\",c.8=L\"ncharTagValue\",c,9=7u64 1626006933640000000ns"
        try:
            self._conn.schemaless_insert([input_sql1], TDSmlProtocolType.LINE.value, None)
            # self._conn.schemaless_insert([input_sql2])
        except SchemalessError as err:
            print(err.errno)

    def runAll(self):
        self.initCheckCase()
        self.boolTypeCheckCase()
        self.symbolsCheckCase()
        self.tsCheckCase()
        self.zeroTsCheckCase()
        self.iuCheckCase()
        self.idSeqCheckCase()
        self.idLetterCheckCase()
        self.noIdCheckCase()
        self.maxColTagCheckCase()
        self.stbTbNameCheckCase()
        self.idStartWithNumCheckCase()
        self.nowTsCheckCase()
        self.dateFormatTsCheckCase()
        self.illegalTsCheckCase()
        self.tbnameCheckCase()
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
        self.tagColNcharMaxLengthCheckCase()
        self.batchInsertCheckCase()
        self.multiInsertCheckCase(100)
        self.batchErrorInsertCheckCase()
        self.multiColsInsertCheckCase()
        self.multiTagsInsertCheckCase()
        self.blankColInsertCheckCase()
        self.blankTagInsertCheckCase()
        self.chineseCheckCase()
        self.spellCheckCase()
        self.defaultTypeCheckCase()
        self.tbnameTagsColsNameCheckCase()

        # MultiThreads
        self.stbInsertMultiThreadCheckCase()
        self.sStbStbDdataInsertMultiThreadCheckCase()
        self.sStbStbDdataAtcInsertMultiThreadCheckCase()
        self.sStbStbDdataMtcInsertMultiThreadCheckCase()
        self.sStbDtbDdataInsertMultiThreadCheckCase()
        self.sStbDtbDdataAcMtInsertMultiThreadCheckCase()
        self.sStbDtbDdataAtMcInsertMultiThreadCheckCase()
        self.sStbStbDdataDtsInsertMultiThreadCheckCase()
        self.sStbStbDdataDtsAcMtInsertMultiThreadCheckCase()
        self.sStbStbDdataDtsAtMcInsertMultiThreadCheckCase()
        self.sStbDtbDdataDtsInsertMultiThreadCheckCase()
        self.sStbDtbDdataDtsAcMtInsertMultiThreadCheckCase()

    def run(self):
        print("running {}".format(__file__))
        self.createDb()
        try:
            self.runAll()
        except Exception as err:
            print(''.join(traceback.format_exception(None, err, err.__traceback__)))
            raise err

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())