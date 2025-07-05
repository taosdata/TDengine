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
import platform
import io
if platform.system().lower() == 'windows':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

class TDTestCase:
    updatecfgDict = {'clientCfg': {'smlDot2Underline': 0}}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self._conn = conn
        self.smlChildTableName_value = "id"

    def createDb(self, name="test", db_update_tag=0, protocol=None):
        if protocol == "telnet-tcp":
            name = "opentsdb_telnet"

        if db_update_tag == 0:
            tdSql.execute(f"drop database if exists {name}")
            tdSql.execute(f"create database if not exists {name} precision 'us' schemaless 1")
        else:
            tdSql.execute(f"drop database if exists {name}")
            tdSql.execute(f"create database if not exists {name} precision 'ns' update 1 schemaless 1")
        tdSql.execute(f'use {name}')

    def timeTrans(self, time_value, ts_type):
        if int(time_value) == 0:
            ts = time.time()
        else:
            if ts_type == TDSmlTimestampType.MILLI_SECOND.value or ts_type == None:
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
            elif value.lower().endswith("u64"):
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

    def inputHandle(self, input_sql, ts_type, protocol=None):
        input_sql_split_list = input_sql.split(" ")
        if protocol == "telnet-tcp":
            input_sql_split_list.pop(0)
        stb_name = input_sql_split_list[0]
        stb_tag_list = input_sql_split_list[3:]
        stb_tag_list[-1] = stb_tag_list[-1].strip()
        stb_col_value = input_sql_split_list[2]
        ts_value = self.timeTrans(input_sql_split_list[1], ts_type)

        tag_name_list = []
        tag_value_list = []
        td_tag_value_list = []
        td_tag_type_list = []

        col_name_list = []
        col_value_list = []
        td_col_value_list = []
        td_col_type_list = []

        for elm in stb_tag_list:
            if self.smlChildTableName_value == "ID":
                if "id=" in elm.lower():
                    tb_name = elm.split('=')[1]
                else:
                    tag_name_list.append(elm.split("=")[0].lower())
                    tag_value_list.append(elm.split("=")[1])
                    tb_name = ""
                    td_tag_value_list.append(self.getTdTypeValue(elm.split("=")[1], "tag")[1])
                    td_tag_type_list.append(self.getTdTypeValue(elm.split("=")[1], "tag")[0])
            else:
                if "id" == elm.split("=")[0].lower():
                    tag_name_list.insert(0, elm.split("=")[0])
                    tag_value_list.insert(0, elm.split("=")[1])
                    td_tag_value_list.insert(0, self.getTdTypeValue(elm.split("=")[1], "tag")[1])
                    td_tag_type_list.insert(0, self.getTdTypeValue(elm.split("=")[1], "tag")[0])
                else:
                    tag_name_list.append(elm.split("=")[0])
                    tag_value_list.append(elm.split("=")[1])
                    tb_name = ""
                    td_tag_value_list.append(self.getTdTypeValue(elm.split("=")[1], "tag")[1])
                    td_tag_type_list.append(self.getTdTypeValue(elm.split("=")[1], "tag")[0])

        col_name_list.append('_value')
        col_value_list.append(stb_col_value)

        td_col_value_list.append(self.getTdTypeValue(stb_col_value)[1])
        td_col_type_list.append(self.getTdTypeValue(stb_col_value)[0])

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

    def genFullTypeSql(self, stb_name="", tb_name="", value="", t0="", t1="127i8", t2="32767i16", t3="2147483647i32",
                        t4="9223372036854775807i64", t5="11.12345f32", t6="22.123456789f64", t7="\"binaryTagValue\"",
                        t8="L\"ncharTagValue\"", ts="1626006833641",
                        id_noexist_tag=None, id_change_tag=None, id_upper_tag=None, id_mixul_tag=None, id_double_tag=None,
                        t_add_tag=None, t_mul_tag=None, c_multi_tag=None, c_blank_tag=None, t_blank_tag=None,
                        chinese_tag=None, multi_field_tag=None, point_trans_tag=None, protocol=None, tcp_keyword_tag=None):
        if stb_name == "":
            stb_name = tdCom.getLongName(len=6, mode="letters")
        if tb_name == "":
            tb_name = f'{stb_name}_{random.randint(0, 65535)}_{random.randint(0, 65535)}'
        if t0 == "":
            t0 = "t"
        if value == "":
            value = random.choice(["f", "F", "false", "False", "t", "T", "true", "True", "TRUE", "FALSE"])
        if id_upper_tag is not None:
            id = "ID"
        else:
            id = "id"
        if id_mixul_tag is not None:
            id = random.choice(["iD", "Id"])
        else:
            id = "id"
        sql_seq = f'{stb_name} {ts} {value} {id}={tb_name} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6} t7={t7} t8={t8}'
        if id_noexist_tag is not None:
            sql_seq = f'{stb_name} {ts} {value} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6} t7={t7} t8={t8}'
            if t_add_tag is not None:
                sql_seq = f'{stb_name} {ts} {value} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6} t7={t7} t8={t8} t9={t8}'
        if id_change_tag is not None:
            sql_seq = f'{stb_name} {ts} {value} {id}={tb_name} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6} t7={t7} t8={t8}'
        if id_double_tag is not None:
            sql_seq = f'{stb_name} {ts} {value} {id}=\"{tb_name}_1\" t0={t0} t1={t1} {id}=\"{tb_name}_2\" t2={t2} t3={t3} t4={t4} t5={t5} t6={t6} t7={t7} t8={t8}'
        if t_add_tag is not None:
            sql_seq = f'{stb_name} {ts} {value} {id}={tb_name} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6} t7={t7} t8={t8} t11={t1} t10={t8}'
        if t_mul_tag is not None:
            sql_seq = f'{stb_name} {ts} {value} {id}={tb_name} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6}'
            if id_noexist_tag is not None:
                sql_seq = f'{stb_name} {ts} {value} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6}'
        if c_multi_tag is not None:
            sql_seq = f'{stb_name} {ts} {value} {value} {id}={tb_name} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6}'
        if c_blank_tag is not None:
            sql_seq = f'{stb_name} {ts} {id}={tb_name} t0={t0} t1={t1} t2={t2} t3={t3} t4={t4} t5={t5} t6={t6} t7={t7} t8={t8}'
        if t_blank_tag is not None:
            sql_seq = f'{stb_name} {ts} {value}'
        if chinese_tag is not None:
            sql_seq = f'{stb_name} {ts} L"涛思数据" t0={t0} t1=L"涛思数据"'
        if multi_field_tag is not None:
            sql_seq = f'{stb_name} {ts} {value} {id}={tb_name} t0={t0} {value}'
        if point_trans_tag is not None:
            sql_seq = f'.point.trans.test {ts} {value} t0={t0}'
        if tcp_keyword_tag is not None:
            sql_seq = f'put {ts} {value} t0={t0}'
        if protocol == "telnet-tcp":
            sql_seq = 'put ' + sql_seq + '\n'
        return sql_seq, stb_name

    def genMulTagColStr(self, genType, count=1):
        """
            genType must be tag/col
        """
        tag_str = ""
        col_str = ""
        if genType == "tag":
            for i in range(0, count):
                if i < (count-1):
                    tag_str += f't{i}=f '
                else:
                    tag_str += f't{i}=f'
            return tag_str
        if genType == "col":
            col_str = "t"
            return col_str

    def genLongSql(self, tag_count):
        stb_name = tdCom.getLongName(7, mode="letters")
        tag_str = self.genMulTagColStr("tag", tag_count)
        col_str = self.genMulTagColStr("col")
        ts = "1626006833641"
        long_sql = stb_name + ' ' + ts + ' ' + col_str + ' ' + ' ' + tag_str
        return long_sql, stb_name

    def getNoIdTbName(self, stb_name, protocol=None):
        query_sql = f"select tbname from {stb_name}"
        tb_name = self.resHandle(query_sql, True, protocol)[0][0]
        return tb_name

    def resHandle(self, query_sql, query_tag, protocol=None):
        tdSql.execute('reset query cache')
        if protocol == "telnet-tcp":
            time.sleep(0.5)
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

    def resCmp(self, input_sql, stb_name, query_sql="select * from", condition="", ts=None, ts_type=None, id=True, none_check_tag=None, precision=None, protocol=None):
        expect_list = self.inputHandle(input_sql, ts_type, protocol)
        if protocol == "telnet-tcp":
            tdCom.tcpClient(input_sql)
        else:
            if precision == None:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, ts_type)
            else:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, precision)
        query_sql = f"{query_sql} {stb_name} {condition}"
        res_row_list, res_field_list_without_ts, res_type_list = self.resHandle(query_sql, True, protocol)
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

    def initCheckCase(self, protocol=None):
        """
            normal tags and cols, one for every elm
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(protocol=protocol)
        self.resCmp(input_sql, stb_name, protocol=protocol)

    def boolTypeCheckCase(self, protocol=None):
        """
            check all normal type
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        full_type_list = ["f", "F", "false", "False", "t", "T", "true", "True"]
        for t_type in full_type_list:
            input_sql, stb_name = self.genFullTypeSql(t0=t_type, protocol=protocol)
            self.resCmp(input_sql, stb_name, protocol=protocol)

    def symbolsCheckCase(self, protocol=None):
        """
            check symbols = `~!@#$%^&*()_-+={[}]\|:;'\",<.>/?
        """
        '''
            please test :
            binary_symbols = '\"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"\'\'"\"'
        '''
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        binary_symbols = '"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"'
        nchar_symbols = f'L{binary_symbols}'
        input_sql1, stb_name1 = self.genFullTypeSql(value=binary_symbols, t7=binary_symbols, t8=nchar_symbols, protocol=protocol)
        input_sql2, stb_name2 = self.genFullTypeSql(value=nchar_symbols, t7=binary_symbols, t8=nchar_symbols, protocol=protocol)
        self.resCmp(input_sql1, stb_name1, protocol=protocol)
        self.resCmp(input_sql2, stb_name2, protocol=protocol)

    def tsCheckCase(self):
        """
            test ts list --> ["1626006833640ms", "1626006834s", "1626006822639022"]
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(ts=1626006833640)
        self.resCmp(input_sql, stb_name, ts_type=TDSmlTimestampType.MILLI_SECOND.value)
        input_sql, stb_name = self.genFullTypeSql(ts=1626006833640)
        self.resCmp(input_sql, stb_name, ts_type=None)
        input_sql, stb_name = self.genFullTypeSql(ts=1626006834)
        self.resCmp(input_sql, stb_name, ts_type=TDSmlTimestampType.SECOND.value)

        tdSql.execute(f"drop database if exists test_ts")
        tdSql.execute(f"create database if not exists test_ts precision 'ms'  schemaless 1")
        tdSql.execute("use test_ts")
        input_sql = ['test_ms 1626006833640 t t0=t', 'test_ms 1626006833641 f t0=t']
        self._conn.schemaless_insert(input_sql, TDSmlProtocolType.TELNET.value, None)
        res = tdSql.query('select * from test_ms', True)
        tdSql.checkEqual(str(res[0][0]), "2021-07-11 20:33:53.640000")
        tdSql.checkEqual(str(res[1][0]), "2021-07-11 20:33:53.641000")

    def openTstbTelnetTsCheckCase(self):
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql = f'{tdCom.getLongName(len=10, mode="letters")} 0 127 t0=127 t1=32767I16 t2=2147483647I32 t3=9223372036854775807 t4=11.12345027923584F32 t5=22.123456789F64'
        stb_name = input_sql.split(" ")[0]
        self.resCmp(input_sql, stb_name, ts=0)
        input_sql = f'{tdCom.getLongName(len=10, mode="letters")} 1626006833640 127 t0=127 t1=32767I16 t2=2147483647I32 t3=9223372036854775807 t4=11.12345027923584F32 t5=22.123456789F64'
        stb_name = input_sql.split(" ")[0]
        self.resCmp(input_sql, stb_name, ts_type=TDSmlTimestampType.MILLI_SECOND.value)
        input_sql = f'{tdCom.getLongName(len=10, mode="letters")} 1626006834 127 t0=127 t1=32767I16 t2=2147483647I32 t3=9223372036854775807 t4=11.12345027923584F32 t5=22.123456789F64'
        stb_name = input_sql.split(" ")[0]
        self.resCmp(input_sql, stb_name, ts_type=TDSmlTimestampType.SECOND.value)
        for ts in [1, 12, 123, 1234, 12345, 123456, 1234567, 12345678, 162600683, 16260068341, 162600683412, 16260068336401]:
            try:
                input_sql = f'{tdCom.getLongName(len=10, mode="letters")} {ts} 127 t0=127 t1=32767I16 t2=2147483647I32 t3=9223372036854775807 t4=11.12345027923584F32 t5=22.123456789F64'
                self._conn.schemaless_insert(input_sql, TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

    def idSeqCheckCase(self, protocol=None):
        """
            check id.index in tags
            eg: t0=**,id=**,t1=**
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(id_change_tag=True, protocol=protocol)
        self.resCmp(input_sql, stb_name, protocol=protocol)

    def idLetterCheckCase(self, protocol=None):
        """
            check id param
            eg: id and ID
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(id_upper_tag=True, protocol=protocol)
        self.resCmp(input_sql, stb_name, protocol=protocol)
        input_sql, stb_name = self.genFullTypeSql(id_mixul_tag=True, protocol=protocol)
        self.resCmp(input_sql, stb_name, protocol=protocol)
        input_sql, stb_name = self.genFullTypeSql(id_change_tag=True, id_upper_tag=True, protocol=protocol)
        self.resCmp(input_sql, stb_name, protocol=protocol)

    def noIdCheckCase(self, protocol=None):
        """
            id not exist
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(id_noexist_tag=True, protocol=protocol)
        self.resCmp(input_sql, stb_name, protocol=protocol)
        query_sql = f"select tbname from {stb_name}"
        res_row_list = self.resHandle(query_sql, True)[0]
        if len(res_row_list[0][0]) > 0:
            tdSql.checkColNameList(res_row_list, res_row_list)
        else:
            tdSql.checkColNameList(res_row_list, "please check noIdCheckCase")

    def maxColTagCheckCase(self):
        """
            max tag count is 128
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        for input_sql in [self.genLongSql(128)[0]]:
            tdCom.cleanTb(dbname="test")
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        for input_sql in [self.genLongSql(129)[0]]:
            tdCom.cleanTb(dbname="test")
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

    def stbTbNameCheckCase(self, protocol=None):
        """
            test illegal id name
            mix "`~!@#$¥%^&*()-+{}|[]、「」【】:;《》<>?"
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        rstr = list("~!@#$¥%^&*()-+{}|[]、「」【】:;《》<>?")
        for i in rstr:
            input_sql, stb_name = self.genFullTypeSql(tb_name=f"\"aaa{i}bbb\"", protocol=protocol)
            self.resCmp(input_sql, f'`{stb_name}`', protocol=protocol)
            tdSql.execute(f'drop table if exists `{stb_name}`')

    def idStartWithNumCheckCase(self, protocol=None):
        """
            id is start with num
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(tb_name="1aaabbb", protocol=protocol)
        self.resCmp(input_sql, stb_name, protocol=protocol)

    def nowTsCheckCase(self):
        """
            check now unsupported
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql = self.genFullTypeSql(ts="now")[0]
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def dateFormatTsCheckCase(self):
        """
            check date format ts unsupported
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql = self.genFullTypeSql(ts="2021-07-21\ 19:01:46.920")[0]
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def illegalTsCheckCase(self):
        """
            check ts format like 16260068336390us19
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql = self.genFullTypeSql(ts="16260068336390us19")[0]
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
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
        stb_name_192 = tdCom.getLongName(len=192, mode="letters")
        tb_name_192 = tdCom.getLongName(len=192, mode="letters")
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name_192, tb_name=tb_name_192)
        self.resCmp(input_sql, stb_name)
        tdSql.query(f'select * from {stb_name}')
        tdSql.checkRows(1)
        if self.smlChildTableName_value == "ID":
            for input_sql in [self.genFullTypeSql(stb_name=tdCom.getLongName(len=193, mode="letters"), tb_name=tdCom.getLongName(len=5, mode="letters"))[0], self.genFullTypeSql(tb_name=tdCom.getLongName(len=193, mode="letters"))[0]]:
                try:
                    self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                    raise Exception("should not reach here")
                except SchemalessError as err:
                    tdSql.checkNotEqual(err.errno, 0)
            input_sql = 'Abcdffgg 1626006833640 False T1=127i8 id=Abcddd'
        else:
            input_sql = self.genFullTypeSql(stb_name=tdCom.getLongName(len=193, mode="letters"), tb_name=tdCom.getLongName(len=5, mode="letters"))[0]
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)
            input_sql = 'Abcdffgg 1626006833640 False T1=127i8'
        stb_name = f'`{input_sql.split(" ")[0]}`'
        self.resCmp(input_sql, stb_name)
        tdSql.execute('drop table `Abcdffgg`')

    def tagNameLengthCheckCase(self):
        """
            check tag name limit <= 62
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tag_name = tdCom.getLongName(61, "letters")
        tag_name = f'T{tag_name}'
        stb_name = tdCom.getLongName(7, "letters")
        input_sql = f'{stb_name} 1626006833640 L"bcdaaa" {tag_name}=f'
        self.resCmp(input_sql, stb_name)
        input_sql = f'{stb_name} 1626006833640 L"gggcdaaa" {tdCom.getLongName(65, "letters")}=f'
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def tagValueLengthCheckCase(self):
        """
            check full type tag value limit
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        # nchar
        # * legal nchar could not be larger than 16374/4
        stb_name = tdCom.getLongName(7, "letters")
        input_sql = f'{stb_name} 1626006833640 t t0=t t1={tdCom.getLongName(4093, "letters")}'
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)

        input_sql = f'{stb_name} 1626006833640 t t0=t t1={tdCom.getLongName(4094, "letters")}'
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def colValueLengthCheckCase(self):
        """
            check full type col value limit
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        # i8
        for value in ["-128i8", "127i8"]:
            input_sql, stb_name = self.genFullTypeSql(value=value)
            self.resCmp(input_sql, stb_name)
        tdCom.cleanTb(dbname="test")
        for value in ["-129i8", "128i8"]:
            input_sql = self.genFullTypeSql(value=value)[0]
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)
        # i16
        tdCom.cleanTb(dbname="test")
        for value in ["-32768i16"]:
            input_sql, stb_name = self.genFullTypeSql(value=value)
            self.resCmp(input_sql, stb_name)
        tdCom.cleanTb(dbname="test")
        for value in ["-32769i16", "32768i16"]:
            input_sql = self.genFullTypeSql(value=value)[0]
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # i32
        tdCom.cleanTb(dbname="test")
        for value in ["-2147483648i32"]:
            input_sql, stb_name = self.genFullTypeSql(value=value)
            self.resCmp(input_sql, stb_name)
        tdCom.cleanTb(dbname="test")
        for value in ["-2147483649i32", "2147483648i32"]:
            input_sql = self.genFullTypeSql(value=value)[0]
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # i64
        tdCom.cleanTb(dbname="test")
        for value in ["-9223372036854775808i64"]:
            input_sql, stb_name = self.genFullTypeSql(value=value)
            self.resCmp(input_sql, stb_name)
        tdCom.cleanTb(dbname="test")
        for value in ["-9223372036854775809i64", "9223372036854775808i64"]:
            input_sql = self.genFullTypeSql(value=value)[0]
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # f32
        tdCom.cleanTb(dbname="test")
        for value in [f"{-3.4028234663852885981170418348451692544*(10**38)}f32", f"{3.4028234663852885981170418348451692544*(10**38)}f32"]:
            input_sql, stb_name = self.genFullTypeSql(value=value)
            self.resCmp(input_sql, stb_name)
        # * limit set to 4028234664*(10**38)
        tdCom.cleanTb(dbname="test")
        for value in [f"{-3.4028234664*(10**38)}f32", f"{3.4028234664*(10**38)}f32"]:
            input_sql = self.genFullTypeSql(value=value)[0]
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # f64
        tdCom.cleanTb(dbname="test")
        for value in [f'{-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)}f64', f'{-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)}f64']:
            input_sql, stb_name = self.genFullTypeSql(value=value)
            self.resCmp(input_sql, stb_name)
        # # * limit set to 1.797693134862316*(10**308)
        # tdCom.cleanTb(dbname="test")
        # for value in [f'{-1.797693134862316*(10**308)}f64', f'{-1.797693134862316*(10**308)}f64']:
        #     input_sql = self.genFullTypeSql(value=value)[0]
        #     try:
        #         self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        #         raise Exception("should not reach here")
        #     except SchemalessError as err:
        #         tdSql.checkNotEqual(err.errno, 0)

        # # # binary
        # tdCom.cleanTb(dbname="test")
        # stb_name = tdCom.getLongName(7, "letters")
        # input_sql = f'{stb_name} 1626006833640 "{tdCom.getLongName(16374, "letters")}" t0=t'
        # self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)

        # tdCom.cleanTb(dbname="test")
        # input_sql = f'{stb_name} 1626006833640 "{tdCom.getLongName(16375, "letters")}" t0=t'
        # try:
        #     self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        #     raise Exception("should not reach here")
        # except SchemalessError as err:
        #     tdSql.checkNotEqual(err.errno, 0)

        # # nchar
        # # * legal nchar could not be larger than 16374/4
        # tdCom.cleanTb(dbname="test")
        # stb_name = tdCom.getLongName(7, "letters")
        # input_sql = f'{stb_name} 1626006833640 L"{tdCom.getLongName(4093, "letters")}" t0=t'
        # self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)

        # tdCom.cleanTb(dbname="test")
        # input_sql = f'{stb_name} 1626006833640 L"{tdCom.getLongName(4094, "letters")}" t0=t'
        # try:
        #     self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        #     raise Exception("should not reach here")
        # except SchemalessError as err:
        #     tdSql.checkNotEqual(err.errno, 0)

    def tagColIllegalValueCheckCase(self):

        """
            test illegal tag col value
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        # bool
        for i in ["TrUe", "tRue", "trUe", "truE", "FalsE", "fAlse", "faLse", "falSe", "falsE"]:
            input_sql1, stb_name = self.genFullTypeSql(t0=i)
            self.resCmp(input_sql1, stb_name)
            input_sql2, stb_name = self.genFullTypeSql(value=i)
            self.resCmp(input_sql2, stb_name)

        # i8 i16 i32 i64 f32 f64
        for input_sql in [
                self.genFullTypeSql(value="1s2i8")[0],
                self.genFullTypeSql(value="1s2i16")[0],
                self.genFullTypeSql(value="1s2i32")[0],
                self.genFullTypeSql(value="1s2i64")[0],
                self.genFullTypeSql(value="11.1s45f32")[0],
                self.genFullTypeSql(value="11.1s45f64")[0],
            ]:
            try:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # check accepted binary and nchar symbols
        # # * ~!@#$¥%^&*()-+={}|[]、「」:;
        for symbol in list('~!@#$¥%^&*()-+={}|[]、「」:;'):
            input_sql1 = f'{tdCom.getLongName(7, "letters")} 1626006833640 "abc{symbol}aaa" t0=t'
            input_sql2 = f'{tdCom.getLongName(7, "letters")} 1626006833640 t t0=t t1="abc{symbol}aaa"'
            self._conn.schemaless_insert([input_sql1], TDSmlProtocolType.TELNET.value, None)
            # self._conn.schemaless_insert([input_sql2], TDSmlProtocolType.TELNET.value, None)

    def blankCheckCase(self):
        '''
            check blank case
        '''
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        # input_sql_list = [f'{tdCom.getLongName(7, "letters")}   1626006833640 "abc aaa" t0=t',
        #                 f'{tdCom.getLongName(7, "letters")} 1626006833640   t t0="abaaa"',
        #                 f'{tdCom.getLongName(7, "letters")} 1626006833640 t   t0=L"abaaa"',
        #                 f'{tdCom.getLongName(7, "letters")}  1626006833640   L"aba aa"   t0=L"abcaaa3"   ']
        input_sql_list = [f'{tdCom.getLongName(7, "letters")} 1626006833640   t t0="abaaa"',
                        f'{tdCom.getLongName(7, "letters")} 1626006833640 t   t0=L"abaaa"']
        for input_sql in input_sql_list:
            stb_name = input_sql.split(" ")[0]
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            tdSql.query(f'select * from {stb_name}')
            tdSql.checkRows(1)

    def duplicateIdTagColInsertCheckCase(self):
        """
            check duplicate Id Tag Col
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql_id = self.genFullTypeSql(id_double_tag=True)[0]
        try:
            self._conn.schemaless_insert([input_sql_id], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

        input_sql = self.genFullTypeSql()[0]
        input_sql_tag = input_sql.replace("t5", "t6")
        try:
            self._conn.schemaless_insert([input_sql_tag], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    ##### stb exist #####
    @tdCom.smlPass
    def noIdStbExistCheckCase(self):
        """
            case no id when stb exist
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(tb_name="sub_table_0123456", t0="f", value="f")
        self.resCmp(input_sql, stb_name)
        input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name, id_noexist_tag=True, t0="f", value="f")
        self.resCmp(input_sql, stb_name, condition='where tbname like "t_%"')
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)

    def duplicateInsertExistCheckCase(self):
        """
            check duplicate insert when stb exist
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        self.resCmp(input_sql, stb_name)

    @tdCom.smlPass
    def tagColBinaryNcharLengthCheckCase(self):
        """
            check length increase
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql()
        self.resCmp(input_sql, stb_name)
        tb_name = tdCom.getLongName(5, "letters")
        input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name,t7="\"binaryTagValuebinaryTagValue\"", t8="L\"ncharTagValuencharTagValue\"")
        self.resCmp(input_sql, stb_name, condition=f'where tbname like "{tb_name}"')

    @tdCom.smlPass
    def tagColAddDupIDCheckCase(self):
        """
            check tag count add, stb and tb duplicate
            * tag: alter table ...
            * col: when update==0 and ts is same, unchange
            * so this case tag&&value will be added,
            * col is added without value when update==0
            * col is added with value when update==1
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tb_name = tdCom.getLongName(7, "letters")
        for db_update_tag in [0, 1]:
            if db_update_tag == 1 :
                self.createDb("test_update", db_update_tag=db_update_tag)
            input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name, t0="t", value="t")
            self.resCmp(input_sql, stb_name)
            input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t0="t", value="f", t_add_tag=True)
            if db_update_tag == 1 :
                self.resCmp(input_sql, stb_name, condition=f'where tbname like "{tb_name}"', none_check_tag=True)
                tdSql.query(f'select * from {stb_name} where tbname like "{tb_name}"')
                tdSql.checkData(0, 11, None)
                tdSql.checkData(0, 12, None)
            else:
                self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                tdSql.query(f'select * from {stb_name} where tbname like "{tb_name}"')
                tdSql.checkData(0, 1, True)
                tdSql.checkData(0, 11, None)
                tdSql.checkData(0, 12, None)
            self.createDb()

    @tdCom.smlPass
    def tagColAddCheckCase(self):
        """
            check tag count add
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tb_name = tdCom.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name, t0="f", value="f")
        self.resCmp(input_sql, stb_name)
        tb_name_1 = tdCom.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name_1, t0="f", value="f", t_add_tag=True)
        self.resCmp(input_sql, stb_name, condition=f'where tbname like "{tb_name_1}"')
        res_row_list = self.resHandle(f"select t10,t11 from {tb_name}", True)[0]
        tdSql.checkEqual(res_row_list[0], ['None', 'None'])
        self.resCmp(input_sql, stb_name, condition=f'where tbname like "{tb_name}"', none_check_tag=True)

    def tagMd5Check(self):
        """
            condition: stb not change
            insert two table, keep tag unchange, change col
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(t0="f", value="f", id_noexist_tag=True)
        self.resCmp(input_sql, stb_name)
        tb_name1 = self.getNoIdTbName(stb_name)
        input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name, t0="f", value="f", id_noexist_tag=True)
        self.resCmp(input_sql, stb_name)
        tb_name2 = self.getNoIdTbName(stb_name)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(1)
        tdSql.checkEqual(tb_name1, tb_name2)
        input_sql, stb_name = self.genFullTypeSql(stb_name=stb_name, t0="f", value="f", id_noexist_tag=True, t_add_tag=True)
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        tb_name3 = self.getNoIdTbName(stb_name)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)
        tdSql.checkNotEqual(tb_name1, tb_name3)

    # * tag nchar max is 16374/4, col+ts nchar max  49151
    def tagColNcharMaxLengthCheckCase(self):
        """
            check nchar length limit
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        stb_name = tdCom.getLongName(7, "letters")
        input_sql = f'{stb_name} 1626006833640 f t2={tdCom.getLongName(1, "letters")}'
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)

        # * legal nchar could not be larger than 16374/4
        input_sql = f'{stb_name} 1626006833640 f t1={tdCom.getLongName(4093, "letters")} t2={tdCom.getLongName(1, "letters")}'
        self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)
        input_sql = f'{stb_name} 1626006833640 f t1={tdCom.getLongName(4093, "letters")} t2={tdCom.getLongName(2, "letters")}'
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)

    def batchInsertCheckCase(self):
        """
            test batch insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        stb_name = tdCom.getLongName(8, "letters")
        tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 bigint)')

        lines = ["st123456 1626006833640 1i64 t1=3i64 t2=4f64 t3=\"t3\"",
                "st123456 1626006833641 2i64 t1=4i64 t3=\"t4\" t2=5f64 t4=5f64",
                f'{stb_name} 1626006833642 3i64 t2=5f64 t3=L\"ste\"',
                "stf567890 1626006833643 4i64 t1=4i64 t3=\"t4\" t2=5f64 t4=5f64",
                "st123456 1626006833644 5i64 t1=4i64 t2=5f64 t3=\"t4\"",
                f'{stb_name} 1626006833645 6i64 t2=5f64 t3=L\"ste2\"',
                f'{stb_name} 1626006833646 7i64 t2=5f64 t3=L\"ste2\"',
                "st123456 1626006833647 8i64 t1=4i64 t3=\"t4\" t2=5f64 t4=5f64",
                "st123456 1626006833648 9i64 t1=4i64 t3=\"t4\" t2=5f64 t4=5f64"
                ]
        self._conn.schemaless_insert(lines, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.MILLI_SECOND.value)
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
        tdCom.cleanTb(dbname="test")
        sql_list = []
        stb_name = tdCom.getLongName(8, "letters")
        tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 nchar(10))')
        for i in range(count):
            input_sql = self.genFullTypeSql(stb_name=stb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', value=f'"{tdCom.getLongName(8, "letters")}"', id_noexist_tag=True)[0]
            sql_list.append(input_sql)
        self._conn.schemaless_insert(sql_list, TDSmlProtocolType.TELNET.value, TDSmlTimestampType.MILLI_SECOND.value)
        tdSql.query('show tables')
        tdSql.checkRows(count)

    def batchErrorInsertCheckCase(self):
        """
            test batch error insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        stb_name = tdCom.getLongName(8, "letters")
        lines = ["st123456 1626006833640 3i 64 t1=3i64 t2=4f64 t3=\"t3\"",
                f"{stb_name} 1626056811823316532ns tRue t2=5f64 t3=L\"ste\""]
        try:
            self._conn.schemaless_insert(lines, TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def multiColsInsertCheckCase(self):
        """
            test multi cols insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql = self.genFullTypeSql(c_multi_tag=True)[0]
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def blankColInsertCheckCase(self):
        """
            test blank col insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql = self.genFullTypeSql(c_blank_tag=True)[0]
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def blankTagInsertCheckCase(self):
        """
            test blank tag insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql = self.genFullTypeSql(t_blank_tag=True)[0]
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def chineseCheckCase(self):
        """
            check nchar ---> chinese
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(chinese_tag=True)
        self.resCmp(input_sql, stb_name)

    def multiFieldCheckCase(self):
        '''
            multi_field
        '''
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql = self.genFullTypeSql(multi_field_tag=True)[0]
        try:
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def spellCheckCase(self):
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        stb_name = tdCom.getLongName(8, "letters")
        input_sql_list = [f'{stb_name}_1 1626006833640 127I8 t0=127I8 t1=32767I16 t2=2147483647I32 t3=9223372036854775807I64 t4=11.12345027923584F32 t5=22.123456789F64',
                            f'{stb_name}_2 1626006833640 32767I16 t0=127I8 t1=32767I16 t2=2147483647I32 t3=9223372036854775807I64 t4=11.12345027923584F32 t5=22.123456789F64',
                            f'{stb_name}_3 1626006833640 2147483647I32 t0=127I8 t1=32767I16 t2=2147483647I32 t3=9223372036854775807I64 t4=11.12345027923584F32 t5=22.123456789F64',
                            f'{stb_name}_4 1626006833640 9223372036854775807I64 t0=127I8 t1=32767I16 t2=2147483647I32 t3=9223372036854775807I64 t4=11.12345027923584F32 t5=22.123456789F64',
                            f'{stb_name}_5 1626006833640 11.12345027923584F32 t0=127I8 t1=32767I16 t2=2147483647I32 t3=9223372036854775807I64 t4=11.12345027923584F32 t5=22.123456789F64',
                            f'{stb_name}_6 1626006833640 22.123456789F64 t0=127I8 t1=32767I16 t2=2147483647I32 t3=9223372036854775807I64 t4=11.12345027923584F32 t5=22.123456789F64',
                            f'{stb_name}_7 1626006833640 22.123456789F64 t0=127I8 t1=32767I16 t2=2147483647I32 t3=9223372036854775807I64 t4=11.12345027923584F32 t5=22.123456789F64',
                            f'{stb_name}_8 1626006833640 22.123456789F64 t0=127I8 t1=32767I16 t2=2147483647I32 t3=9223372036854775807I64 t4=11.12345027923584F32 t5=22.123456789F64',
                            f'{stb_name}_9 1626006833640 22.123456789F64 t0=127I8 t1=32767I16 t2=2147483647I32 t3=9223372036854775807I64 t4=11.12345027923584F32 t5=22.123456789F64',
                            f'{stb_name}_10 1626006833640 22.123456789F64 t0=127I8 t1=32767I16 t2=2147483647I32 t3=9223372036854775807I64 t4=11.12345027923584F32 t5=22.123456789F64']
        for input_sql in input_sql_list:
            stb_name = input_sql.split(' ')[0]
            self.resCmp(input_sql, stb_name)

    def pointTransCheckCase(self, protocol=None):
        """
            metric value "." trans to "_"
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql = self.genFullTypeSql(point_trans_tag=True, protocol=protocol)[0]
        if protocol == 'telnet-tcp':
            stb_name = f'`{input_sql.split(" ")[1]}`'
        else:
            stb_name = f'`{input_sql.split(" ")[0]}`'
        self.resCmp(input_sql, stb_name, protocol=protocol)
        tdSql.execute("drop table `.point.trans.test`")

    def defaultTypeCheckCase(self):
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        stb_name = tdCom.getLongName(8, "letters")
        input_sql_list = [f'{stb_name}_1 1626006833640 9223372036854775807 t0=f t1=127 t2=32767i16 t3=2147483647i32 t4=9223372036854775807 t5=11.12345f32 t6=22.123456789f64 t7="vozamcts" t8=L"ncharTagValue"', \
                        f'{stb_name}_2 1626006833641 22.123456789 t0=f t1=127i8 t2=32767I16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789 t7="vozamcts" t8=L"ncharTagValue"', \
                        f'{stb_name}_3 1626006833642 10e5F32 t0=f t1=127i8 t2=32767I16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=10e5F64 t7="vozamcts" t8=L"ncharTagValue"', \
                        f'{stb_name}_4 1626006833643 10.0e5F64 t0=f t1=127i8 t2=32767I16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=10.0e5F32 t7="vozamcts" t8=L"ncharTagValue"', \
                        f'{stb_name}_5 1626006833644 -10.0e5 t0=f t1=127i8 t2=32767I16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=-10.0e5 t7="vozamcts" t8=L"ncharTagValue"']
        for input_sql in input_sql_list:
            stb_name = input_sql.split(" ")[0]
            self.resCmp(input_sql, stb_name)

    def tbnameTagsColsNameCheckCase(self):
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        if self.smlChildTableName_value == "ID":
            input_sql = 'rFa$sta 1626006834 9223372036854775807 id=rFas$ta_1 Tt!0=true tT@1=127Ii8 t#2=32767i16 "t$3"=2147483647i32 t%4=9223372036854775807i64 t^5=11.12345f32 t&6=22.123456789f64 t*7=\"ddzhiksj\" t!@#$%^&*()_+[];:<>?,9=L\"ncharTagValue\"'
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            query_sql = 'select * from `rFa$sta`'
            query_res = tdSql.query(query_sql, True)
            tdSql.checkEqual(query_res, [(datetime.datetime(2021, 7, 11, 20, 33, 54), 9.223372036854776e+18, 'true', '127Ii8', '32767i16', '2147483647i32', '9223372036854775807i64', '11.12345f32', '22.123456789f64', '"ddzhiksj"', 'L"ncharTagValue"')])
            col_tag_res = tdSql.getColNameList(query_sql)
            tdSql.checkEqual(col_tag_res, ['ts', '_value', 'tt!0', 'tt@1', 't#2', '"t$3"', 't%4', 't^5', 't&6', 't*7', 't!@#$%^&*()_+[];:<>?,9'])
            tdSql.execute('drop table `rFa$sta`')
        else:
            input_sql = 'rFa$sta 1626006834 9223372036854775807 Tt!0=true tT@1=127Ii8 t#2=32767i16 "t$3"=2147483647i32 t%4=9223372036854775807i64 t^5=11.12345f32 t&6=22.123456789f64 t*7=\"ddzhiksj\" t!@#$%^&*()_+[];:<>?,9=L\"ncharTagValue\"'
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            query_sql = 'select * from `rFa$sta`'
            query_res = tdSql.query(query_sql, True)
            tdSql.checkEqual(query_res, [(datetime.datetime(2021, 7, 11, 20, 33, 54), 9.223372036854776e+18, 'true', '127Ii8', '32767i16', '2147483647i32', '9223372036854775807i64', '11.12345f32', '22.123456789f64', '"ddzhiksj"', 'L"ncharTagValue"')])
            col_tag_res = tdSql.getColNameList(query_sql)
            tdSql.checkEqual(col_tag_res, ['_ts', '_value', 'Tt!0', 'tT@1', 't#2', '"t$3"', 't%4', 't^5', 't&6', 't*7', 't!@#$%^&*()_+[];:<>?,9'])
            tdSql.execute('drop table `rFa$sta`')

    def tcpKeywordsCheckCase(self, protocol="telnet-tcp"):
        """
            stb = "put"
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql = self.genFullTypeSql(tcp_keyword_tag=True, protocol=protocol)[0]
        stb_name = f'`{input_sql.split(" ")[1]}`'
        self.resCmp(input_sql, stb_name, protocol=protocol)

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
        s_stb_s_tb_a_tag_list = list()
        s_stb_s_tb_m_tag_list = list()
        s_stb_d_tb_list = list()
        s_stb_d_tb_m_tag_list = list()
        s_stb_d_tb_a_tag_list = list()
        s_stb_s_tb_d_ts_list = list()
        s_stb_s_tb_d_ts_m_tag_list = list()
        s_stb_s_tb_d_ts_a_tag_list = list()
        s_stb_d_tb_d_ts_list = list()
        s_stb_d_tb_d_ts_m_tag_list = list()
        s_stb_d_tb_d_ts_a_tag_list = list()
        for i in range(count):
            d_stb_d_tb_list.append(self.genFullTypeSql(t0="f", value="f"))
            s_stb_s_tb_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', value=f'"{tdCom.getLongName(8, "letters")}"'))
            s_stb_s_tb_a_tag_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', value=f'"{tdCom.getLongName(8, "letters")}"', t_add_tag=True))
            s_stb_s_tb_m_tag_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', value=f'"{tdCom.getLongName(8, "letters")}"', t_mul_tag=True))
            s_stb_d_tb_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', value=f'"{tdCom.getLongName(8, "letters")}"', id_noexist_tag=True))
            s_stb_d_tb_m_tag_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', value=f'"{tdCom.getLongName(8, "letters")}"', id_noexist_tag=True, t_mul_tag=True))
            s_stb_d_tb_a_tag_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', value=f'"{tdCom.getLongName(8, "letters")}"', id_noexist_tag=True, t_add_tag=True))
            s_stb_s_tb_d_ts_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', value=f'"{tdCom.getLongName(8, "letters")}"', ts=0))
            s_stb_s_tb_d_ts_m_tag_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', value=f'"{tdCom.getLongName(8, "letters")}"', ts=0, t_mul_tag=True))
            s_stb_s_tb_d_ts_a_tag_list.append(self.genFullTypeSql(stb_name=stb_name, tb_name=tb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', value=f'"{tdCom.getLongName(8, "letters")}"', ts=0, t_add_tag=True))
            s_stb_d_tb_d_ts_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', value=f'"{tdCom.getLongName(8, "letters")}"', id_noexist_tag=True, ts=0))
            s_stb_d_tb_d_ts_m_tag_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', value=f'"{tdCom.getLongName(8, "letters")}"', id_noexist_tag=True, ts=0, t_mul_tag=True))
            s_stb_d_tb_d_ts_a_tag_list.append(self.genFullTypeSql(stb_name=stb_name, t7=f'"{tdCom.getLongName(8, "letters")}"', value=f'"{tdCom.getLongName(8, "letters")}"', id_noexist_tag=True, ts=0, t_add_tag=True))

        return d_stb_d_tb_list, s_stb_s_tb_list, s_stb_s_tb_a_tag_list, s_stb_s_tb_m_tag_list, \
            s_stb_d_tb_list, s_stb_d_tb_m_tag_list, s_stb_d_tb_a_tag_list, s_stb_s_tb_d_ts_list, \
            s_stb_s_tb_d_ts_m_tag_list, s_stb_s_tb_d_ts_a_tag_list, s_stb_d_tb_d_ts_list, \
            s_stb_d_tb_d_ts_m_tag_list, s_stb_d_tb_d_ts_a_tag_list


    def genMultiThreadSeq(self, sql_list):
        tlist = list()
        for insert_sql in sql_list:
            t = threading.Thread(target=self._conn.schemaless_insert,args=([insert_sql[0]], TDSmlProtocolType.TELNET.value, None))
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
        tdCom.cleanTb(dbname="test")
        input_sql = self.genSqlList()[0]
        self.multiThreadRun(self.genMultiThreadSeq(input_sql))
        tdSql.query(f"show tables;")
        tdSql.checkRows(5)

    def sStbStbDdataInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different data, result keep first data
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tb_name = tdCom.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name, value="\"binaryTagValue\"")
        self.resCmp(input_sql, stb_name)
        s_stb_s_tb_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name)[1]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1) if self.smlChildTableName_value == "ID" else tdSql.checkRows(6)
        if self.smlChildTableName_value == "ID":
            expected_tb_name = self.getNoIdTbName(stb_name)[0]
            tdSql.checkEqual(tb_name, expected_tb_name)
        tdSql.query(f"select * from {stb_name};")
        tdSql.checkRows(1) if self.smlChildTableName_value == "ID" else tdSql.checkRows(6)

    def sStbStbDdataAtInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different data, add columes and tags,  result keep first data
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tb_name = tdCom.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name, value="\"binaryTagValue\"")
        self.resCmp(input_sql, stb_name)
        s_stb_s_tb_a_tag_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name)[2]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_a_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1) if self.smlChildTableName_value == "ID" else tdSql.checkRows(6)
        if self.smlChildTableName_value == "ID":
            expected_tb_name = self.getNoIdTbName(stb_name)[0]
            tdSql.checkEqual(tb_name, expected_tb_name)
        tdSql.query(f"select * from {stb_name};")
        tdSql.checkRows(1) if self.smlChildTableName_value == "ID" else tdSql.checkRows(6)

    def sStbStbDdataMtInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different data, minus columes and tags,  result keep first data
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tb_name = tdCom.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name, value="\"binaryTagValue\"")
        self.resCmp(input_sql, stb_name)
        s_stb_s_tb_m_tag_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name)[3]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_m_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1) if self.smlChildTableName_value == "ID" else tdSql.checkRows(2)
        if self.smlChildTableName_value == "ID":
            expected_tb_name = self.getNoIdTbName(stb_name)[0]
            tdSql.checkEqual(tb_name, expected_tb_name)
        tdSql.query(f"select * from {stb_name};")
        tdSql.checkRows(1) if self.smlChildTableName_value == "ID" else tdSql.checkRows(2)

    def sStbDtbDdataInsertMultiThreadCheckCase(self):
        """
            thread input same stb, different tb, different data
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(value="\"binaryTagValue\"")
        self.resCmp(input_sql, stb_name)
        s_stb_d_tb_list = self.genSqlList(stb_name=stb_name)[4]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(6)

    def sStbDtbDdataMtInsertMultiThreadCheckCase(self):
        """
            thread input same stb, different tb, different data, add col, mul tag
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(value="\"binaryTagValue\"")
        self.resCmp(input_sql, stb_name)
        s_stb_d_tb_m_tag_list = [(f'{stb_name} 1626006833640 "omfdhyom" t0=F t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'yzwswz'),  \
                                (f'{stb_name} 1626006833640 "vqowydbc" t0=F t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'yzwswz'),  \
                                (f'{stb_name} 1626006833640 "plgkckpv" t0=F t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'yzwswz'),  \
                                (f'{stb_name} 1626006833640 "cujyqvlj" t0=F t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'yzwswz'),  \
                                (f'{stb_name} 1626006833640 "twjxisat" t0=T t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'yzwswz')]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_m_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(3)

    def sStbDtbDdataAtInsertMultiThreadCheckCase(self):
        """
            thread input same stb, different tb, different data, add tag, mul col
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(value="\"binaryTagValue\"")
        self.resCmp(input_sql, stb_name)
        s_stb_d_tb_a_tag_list = self.genSqlList(stb_name=stb_name)[6]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_a_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(6)

    def sStbStbDdataDtsInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different ts
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tb_name = tdCom.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name, value="\"binaryTagValue\"")
        self.resCmp(input_sql, stb_name)
        s_stb_s_tb_d_ts_list = [(f'{stb_name} 0 "hkgjiwdj" id={tb_name} t0=f t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="vozamcts" t8=L"ncharTagValue"', 'dwpthv'), \
                                (f'{stb_name} 0 "rljjrrul" id={tb_name} t0=False t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="bmcanhbs" t8=L"ncharTagValue"', 'dwpthv'), \
                                (f'{stb_name} 0 "basanglx" id={tb_name} t0=False t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="enqkyvmb" t8=L"ncharTagValue"', 'dwpthv'), \
                                (f'{stb_name} 0 "clsajzpp" id={tb_name} t0=F t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="eivaegjk" t8=L"ncharTagValue"', 'dwpthv'), \
                                (f'{stb_name} 0 "jitwseso" id={tb_name} t0=T t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="yhlwkddq" t8=L"ncharTagValue"', 'dwpthv')]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_d_ts_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1) if self.smlChildTableName_value == "ID" else tdSql.checkRows(6)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(6)

    def sStbStbDdataDtsMtInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different ts, add col, mul tag
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tb_name = tdCom.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name, value="\"binaryTagValue\"")
        self.resCmp(input_sql, stb_name)
        s_stb_s_tb_d_ts_m_tag_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name)[8]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_d_ts_m_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1) if self.smlChildTableName_value == "ID" else tdSql.checkRows(2)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(6)
        tdSql.query(f"select * from {stb_name} where t8 is not NULL")
        tdSql.checkRows(6) if self.smlChildTableName_value == "ID" else tdSql.checkRows(1)

    def sStbStbDdataDtsAtInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different ts, add tag, mul col
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tb_name = tdCom.getLongName(7, "letters")
        input_sql, stb_name = self.genFullTypeSql(tb_name=tb_name, value="\"binaryTagValue\"")
        self.resCmp(input_sql, stb_name)
        s_stb_s_tb_d_ts_a_tag_list = [(f'{stb_name} 0 "clummqfy" id={tb_name} t0=False t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="hpxzrdiw" t8=L"ncharTagValue" t11=127i8 t10=L"ncharTagValue"', 'bokaxl'), \
                                    (f'{stb_name} 0 "yqeztggb" id={tb_name} t0=F t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="gdtblmrc" t8=L"ncharTagValue" t11=127i8 t10=L"ncharTagValue"', 'bokaxl'), \
                                    (f'{stb_name} 0 "gbkinqdk" id={tb_name} t0=f t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="iqniuvco" t8=L"ncharTagValue" t11=127i8 t10=L"ncharTagValue"', 'bokaxl'), \
                                    (f'{stb_name} 0 "ldxxejbd" id={tb_name} t0=f t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="vxkipags" t8=L"ncharTagValue" t11=127i8 t10=L"ncharTagValue"', 'bokaxl'), \
                                    (f'{stb_name} 0 "tlvzwjes" id={tb_name} t0=true t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="enwrlrtj" t8=L"ncharTagValue" t11=127i8 t10=L"ncharTagValue"', 'bokaxl')]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_d_ts_a_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1) if self.smlChildTableName_value == "ID" else tdSql.checkRows(6)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(6)
        for t in ["t10", "t11"]:
            tdSql.query(f"select * from {stb_name} where {t} is not NULL;")
            tdSql.checkRows(0) if self.smlChildTableName_value == "ID" else tdSql.checkRows(5)

    def sStbDtbDdataDtsInsertMultiThreadCheckCase(self):
        """
            thread input same stb, different tb, data, ts
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(value="\"binaryTagValue\"")
        self.resCmp(input_sql, stb_name)
        s_stb_d_tb_d_ts_list = self.genSqlList(stb_name=stb_name)[10]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_d_ts_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(6)

    def sStbDtbDdataDtsMtInsertMultiThreadCheckCase(self):
        """
            thread input same stb, different tb, data, ts, add col, mul tag
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_sql, stb_name = self.genFullTypeSql(value="\"binaryTagValue\"")
        self.resCmp(input_sql, stb_name)
        s_stb_d_tb_d_ts_m_tag_list = [(f'{stb_name} 0 "mnpmtzul" t0=False t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'pcppkg'), \
                                    (f'{stb_name} 0 "zbvwckcd" t0=True t1=126i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'pcppkg'), \
                                    (f'{stb_name} 0 "vymcjfwc" t0=False t1=125i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'pcppkg'), \
                                    (f'{stb_name} 0 "laumkwfn" t0=False t1=124i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'pcppkg'), \
                                    (f'{stb_name} 0 "nyultzxr" t0=false t1=123i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'pcppkg')]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_d_ts_m_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(6)

    def test(self):
        try:
            input_sql = f'test_nchar 0 L"涛思数据" t0=f t1=L"涛思数据" t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64'
            self._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        except SchemalessError as err:
            print(err.errno)

    def runAll(self):
        self.initCheckCase()
        self.boolTypeCheckCase()
        self.symbolsCheckCase()
        self.tsCheckCase()
        self.openTstbTelnetTsCheckCase()
        # self.idSeqCheckCase()
        self.idLetterCheckCase()
        self.noIdCheckCase()
        self.maxColTagCheckCase()
        self.stbTbNameCheckCase()
        self.idStartWithNumCheckCase()
        self.nowTsCheckCase()
        self.dateFormatTsCheckCase()
        self.illegalTsCheckCase()
        self.tbnameCheckCase()
        self.tagNameLengthCheckCase()
        # self.tagValueLengthCheckCase()
        self.colValueLengthCheckCase()
        self.tagColIllegalValueCheckCase()
        self.blankCheckCase()
        self.duplicateIdTagColInsertCheckCase()
        self.noIdStbExistCheckCase()
        self.duplicateInsertExistCheckCase()
        self.tagColBinaryNcharLengthCheckCase()
        self.tagColAddDupIDCheckCase()
        self.tagColAddCheckCase()
        self.tagMd5Check()
        # self.tagColNcharMaxLengthCheckCase()
        # self.batchInsertCheckCase()
        # self.multiInsertCheckCase(10)
        self.batchErrorInsertCheckCase()
        self.multiColsInsertCheckCase()
        self.blankColInsertCheckCase()
        self.blankTagInsertCheckCase()
        self.chineseCheckCase()
        self.multiFieldCheckCase()
        self.spellCheckCase()
        self.pointTransCheckCase()
        self.defaultTypeCheckCase()
        self.tbnameTagsColsNameCheckCase()
        # # # MultiThreads
        # self.stbInsertMultiThreadCheckCase()
        # self.sStbStbDdataInsertMultiThreadCheckCase()
        # self.sStbStbDdataAtInsertMultiThreadCheckCase()
        # self.sStbStbDdataMtInsertMultiThreadCheckCase()
        # self.sStbDtbDdataInsertMultiThreadCheckCase()
        # self.sStbDtbDdataMtInsertMultiThreadCheckCase()
        # self.sStbDtbDdataAtInsertMultiThreadCheckCase()
        # self.sStbStbDdataDtsInsertMultiThreadCheckCase()
        # # self.sStbStbDdataDtsMtInsertMultiThreadCheckCase()
        # self.sStbStbDdataDtsAtInsertMultiThreadCheckCase()
        # self.sStbDtbDdataDtsInsertMultiThreadCheckCase()
        # self.sStbDtbDdataDtsMtInsertMultiThreadCheckCase()

    def run(self):
        print("running {}".format(__file__))

        try:
            self.createDb()
            self.runAll()
            # self.createDb(protocol="telnet-tcp")
            # self.initCheckCase('telnet-tcp')
            # self.boolTypeCheckCase('telnet-tcp')
            # self.symbolsCheckCase('telnet-tcp')
            # self.idSeqCheckCase('telnet-tcp')
            # self.idLetterCheckCase('telnet-tcp')
            # self.noIdCheckCase('telnet-tcp')
            # self.stbTbNameCheckCase('telnet-tcp')
            # self.idStartWithNumCheckCase('telnet-tcp')
            # self.pointTransCheckCase('telnet-tcp')
            # self.tcpKeywordsCheckCase()
        except Exception as err:
            print(''.join(traceback.format_exception(None, err, err.__traceback__)))
            raise err

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
