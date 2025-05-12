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
from util.log import *
from util.cases import *
from util.sql import *
from util.common import tdCom
from util.types import TDSmlProtocolType
import threading
import json

class TDTestCase:
    updatecfgDict = {'clientCfg': {'smlDot2Underline': 0}}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn
        self.defaultJSONStrType_value = "BINARY"

    def createDb(self, name="test", db_update_tag=0, protocol=None):
        if protocol == "telnet-tcp":
            name = "opentsdb_telnet"

        if db_update_tag == 0:
            tdSql.execute(f"drop database if exists {name}")
            tdSql.execute(f"create database if not exists {name} precision 'ms' schemaless 1")
        else:
            tdSql.execute(f"drop database if exists {name}")
            tdSql.execute(f"create database if not exists {name} precision 'ms' update 1 schemaless 1")
        tdSql.execute(f'use {name}')

    def timeTrans(self, time_value):
        if type(time_value) is int:
            if time_value != 0:
                if len(str(time_value)) == 13:
                    ts = int(time_value)/1000
                elif len(str(time_value)) == 10:
                    ts = int(time_value)/1
                else:
                    ts = time_value/1000000
            else:
                ts = time.time()
        elif type(time_value) is dict:
            if time_value["type"].lower() == "ns":
                ts = time_value["value"]/1000000000
            elif time_value["type"].lower() == "us":
                ts = time_value["value"]/1000000
            elif time_value["type"].lower() == "ms":
                ts = time_value["value"]/1000
            elif time_value["type"].lower() == "s":
                ts = time_value["value"]/1
            else:
                ts = time_value["value"]/1000000
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

    def dateToTs(self, datetime_input):
        return int(time.mktime(time.strptime(datetime_input, "%Y-%m-%d %H:%M:%S.%f")))

    def typeTrans(self, type_list):
        type_num_list = []
        for tp in type_list:
            if type(tp) is dict:
                tp = tp['type']
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
            elif tp.upper() == "VARCHAR":
                type_num_list.append(8)
            elif tp.upper() == "NCHAR":
                type_num_list.append(10)
            elif tp.upper() == "BIGINT UNSIGNED":
                type_num_list.append(14)
        return type_num_list

    def inputHandle(self, input_json):
        stb_name = input_json["metric"]
        stb_tag_dict = input_json["tags"]
        stb_col_dict = input_json["value"]
        ts_value = self.timeTrans(input_json["timestamp"])
        tag_name_list = []
        tag_value_list = []
        td_tag_value_list = []
        td_tag_type_list = []

        col_name_list = []
        col_value_list = []
        td_col_value_list = []
        td_col_type_list = []

        # handle tag
        for key,value in stb_tag_dict.items():
            if "id" == key.lower():
                tb_name = value
            else:
                if type(value) is dict:
                    tag_value_list.append(str(value["value"]))
                    td_tag_value_list.append(str(value["value"]))
                    tag_name_list.append(key.lower())
                    if value["type"].lower() == "binary":
                        td_tag_type_list.append("VARCHAR")
                    else:
                        td_tag_type_list.append(value["type"].upper())
                    tb_name = ""
                else:
                    tag_value_list.append(str(value))
                    # td_tag_value_list.append(str(value))
                    tag_name_list.append(key.lower())
                    tb_name = ""

                    if type(value) is bool:
                        td_tag_type_list.append("BOOL")
                        td_tag_value_list.append(str(value))
                    elif type(value) is int:
                        td_tag_type_list.append("DOUBLE")
                        td_tag_value_list.append(str(float(value)))
                    elif type(value) is float:
                        td_tag_type_list.append("DOUBLE")
                        td_tag_value_list.append(str(float(value)))
                    elif type(value) is str:
                        if self.defaultJSONStrType_value == "NCHAR":
                            td_tag_type_list.append("NCHAR")
                            td_tag_value_list.append(str(value))
                        else:
                            td_tag_type_list.append("VARCHAR")
                            td_tag_value_list.append(str(value))

        # handle col
        if type(stb_col_dict) is dict:
            if stb_col_dict["type"].lower() == "bool":
                bool_value = f'{stb_col_dict["value"]}'
                col_value_list.append(bool_value)
                td_col_type_list.append(stb_col_dict["type"].upper())
                col_name_list.append("_value")
                td_col_value_list.append(str(stb_col_dict["value"]))
            else:
                col_value_list.append(stb_col_dict["value"])
                if stb_col_dict["type"].lower() == "binary":
                    td_col_type_list.append("VARCHAR")
                else:
                    td_col_type_list.append(stb_col_dict["type"].upper())
                col_name_list.append("_value")
                td_col_value_list.append(str(stb_col_dict["value"]))
        else:
            col_name_list.append("_value")
            col_value_list.append(str(stb_col_dict))
            # td_col_value_list.append(str(stb_col_dict))
            if type(stb_col_dict) is bool:
                td_col_type_list.append("BOOL")
                td_col_value_list.append(str(stb_col_dict))
            elif type(stb_col_dict) is int:
                td_col_type_list.append("DOUBLE")
                td_col_value_list.append(str(float(stb_col_dict)))
            elif type(stb_col_dict) is float:
                td_col_type_list.append("DOUBLE")
                td_col_value_list.append(str(float(stb_col_dict)))
            elif type(stb_col_dict) is str:
                if self.defaultJSONStrType_value == "NCHAR":
                    td_col_type_list.append("NCHAR")
                    td_col_value_list.append(str(stb_col_dict))
                else:
                    td_col_type_list.append("VARCHAR")
                    td_col_value_list.append(str(stb_col_dict))

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

    def genTsColValue(self, value, t_type=None, value_type="obj"):
        if value_type == "obj":
            if t_type == None:
                ts_col_value = value
            else:
                ts_col_value = {"value": value, "type":	t_type}
        elif value_type == "default":
            ts_col_value = value
        return ts_col_value

    def genTagValue(self, t0_type="bool", t0_value="", t1_type="tinyint", t1_value=127, t2_type="smallint", t2_value=32767,
                    t3_type="int", t3_value=2147483647, t4_type="bigint", t4_value=9223372036854775807,
                    t5_type="float", t5_value=11.12345027923584, t6_type="double", t6_value=22.123456789,
                    t7_type="binary", t7_value="binaryTagValue", t8_type="nchar", t8_value="ncharTagValue", value_type="obj"):
        if t0_value == "":
            t0_value = random.choice([True, False])
        if value_type == "obj":
            tag_value = {
                "t0": {"value": t0_value, "type": t0_type},
                "t1": {"value": t1_value, "type": t1_type},
                "t2": {"value": t2_value, "type": t2_type},
                "t3": {"value": t3_value, "type": t3_type},
                "t4": {"value": t4_value, "type": t4_type},
                "t5": {"value": t5_value, "type": t5_type},
                "t6": {"value": t6_value, "type": t6_type},
                "t7": {"value": t7_value, "type": t7_type},
                "t8": {"value": t8_value, "type": t8_type}
            }
        elif value_type == "default":
            # t5_value = t6_value
            tag_value = {
                "t0": t0_value,
                "t1": t1_value,
                "t2": t2_value,
                "t3": t3_value,
                "t4": t4_value,
                "t5": t5_value,
                "t6": t6_value,
                "t7": t7_value,
                "t8": t8_value
            }
        return tag_value

    def genFullTypeJson(self, ts_value="", col_value="", tag_value="", stb_name="", tb_name="",
                        id_noexist_tag=None, id_change_tag=None, id_upper_tag=None, id_mixul_tag=None, id_double_tag=None,
                        t_add_tag=None, t_mul_tag=None, c_multi_tag=None, c_blank_tag=None, t_blank_tag=None,
                        chinese_tag=None, multi_field_tag=None, point_trans_tag=None, value_type="obj"):
        if value_type == "obj":
            if stb_name == "":
                stb_name = tdCom.getLongName(6, "letters")
            if tb_name == "":
                tb_name = f'{stb_name}_{random.randint(0, 65535)}_{random.randint(0, 65535)}'
            if ts_value == "":
                ts_value = self.genTsColValue(1626006833639000000, "ns")
            if col_value == "":
                col_value = self.genTsColValue(random.choice([True, False]), "bool")
            if tag_value == "":
                tag_value = self.genTagValue()
            # if id_upper_tag is not None:
            #     id = "ID"
            # else:
            #     id = "id"
            # if id_mixul_tag is not None:
            #     id = random.choice(["iD", "Id"])
            # else:
            #     id = "id"
            # if id_noexist_tag is None:
            #     tag_value[id] = tb_name
            sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if id_noexist_tag is not None:
                if t_add_tag is not None:
                    tag_value["t9"] = {"value": "ncharTagValue", "type": "nchar"}
                    sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if id_change_tag is not None:
                tag_value.pop('t8')
                tag_value["t8"] = {"value": "ncharTagValue", "type": "nchar"}
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if id_double_tag is not None:
                tag_value["ID"] = f'"{tb_name}_2"'
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if t_add_tag is not None:
                tag_value["t10"] = {"value": "ncharTagValue", "type": "nchar"}
                tag_value["t11"] = {"value": True, "type": "bool"}
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if t_mul_tag is not None:
                tag_value.pop('t8')
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if c_multi_tag is not None:
                col_value = [{"value": True, "type": "bool"}, {"value": False, "type": "bool"}]
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if t_blank_tag is not None:
                tag_value = ""
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if chinese_tag is not None:
                tag_value = {"t0": {"value": "涛思数据", "type": "nchar"}}
                col_value = {"value": "涛思数据", "type": "nchar"}
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if c_blank_tag is not None:
                sql_json.pop("value")
            if multi_field_tag is not None:
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value, "tags2": tag_value}
            if point_trans_tag is not None:
                sql_json = {"metric": ".point.trans.test", "timestamp": ts_value, "value": col_value, "tags": tag_value}

        elif value_type == "default":
            if stb_name == "":
                stb_name = tdCom.getLongName(6, "letters")
            if tb_name == "":
                tb_name = f'{stb_name}_{random.randint(0, 65535)}_{random.randint(0, 65535)}'
            if ts_value == "":
                ts_value = 1626006834
            if col_value == "":
                col_value = random.choice([True, False])
            if tag_value == "":
                tag_value = self.genTagValue(value_type=value_type)
            # if id_upper_tag is not None:
            #     id = "ID"
            # else:
            #     id = "id"
            # if id_mixul_tag is not None:
            #     id = "iD"
            # else:
            #     id = "id"
            # if id_noexist_tag is None:
            #     tag_value[id] = tb_name
            sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if id_noexist_tag is not None:
                if t_add_tag is not None:
                    tag_value["t9"] = {"value": "ncharTagValue", "type": "nchar"}
                    sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if id_change_tag is not None:
                tag_value.pop('t7')
                tag_value["t7"] = {"value": "ncharTagValue", "type": "nchar"}
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if id_double_tag is not None:
                tag_value["ID"] = f'"{tb_name}_2"'
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if t_add_tag is not None:
                tag_value["t10"] = {"value": "ncharTagValue", "type": "nchar"}
                tag_value["t11"] = True
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if t_mul_tag is not None:
                tag_value.pop('t7')
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if c_multi_tag is not None:
                col_value = True,False
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if t_blank_tag is not None:
                tag_value = ""
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value}
            if c_blank_tag is not None:
                sql_json.pop("value")
            if multi_field_tag is not None:
                sql_json = {"metric": stb_name, "timestamp": ts_value, "value": col_value, "tags": tag_value, "tags2": tag_value}
            if point_trans_tag is not None:
                sql_json = {"metric": ".point.trans.test", "timestamp": ts_value, "value": col_value, "tags": tag_value}
        return sql_json, stb_name

    def genMulTagColDict(self, genType, count=1, value_type="obj"):
        """
            genType must be tag/col
        """
        tag_dict = dict()
        col_dict = dict()
        if value_type == "obj":
            if genType == "tag":
                for i in range(0, count):
                    tag_dict[f't{i}'] = {'value': True, 'type': 'bool'}
                return tag_dict
            if genType == "col":
                col_dict = {'value': True, 'type': 'bool'}
                return col_dict
        elif value_type == "default":
            if genType == "tag":
                for i in range(0, count):
                    tag_dict[f't{i}'] = True
                return tag_dict
            if genType == "col":
                col_dict = True
                return col_dict

    def genLongJson(self, tag_count, value_type="obj"):
        stb_name = tdCom.getLongName(7, mode="letters")
        # tb_name = f'{stb_name}_1'
        tag_dict = self.genMulTagColDict("tag", tag_count, value_type)
        col_dict = self.genMulTagColDict("col", 1, value_type)
        # tag_dict["id"] = tb_name
        ts_dict = {'value': 1626006833639000000, 'type': 'ns'}
        long_json = {"metric": stb_name, "timestamp": ts_dict, "value": col_dict, "tags": tag_dict}
        return long_json, stb_name

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
                if "11.1234" in str(i) and str(i) != "11.12345f32" and str(i) != "11.12345027923584F32":
                    sub_list.append("11.12345027923584")
                elif "22.1234" in str(i) and str(i) != "22.123456789f64" and str(i) != "22.123456789F64":
                    sub_list.append("22.123456789")
                else:
                    sub_list.append(str(i))
            res_row_list.append(sub_list)
        res_field_list_without_ts = col_info[0][1:]
        res_type_list = col_info[1]
        return res_row_list, res_field_list_without_ts, res_type_list

    def resCmp(self, input_json, stb_name, query_sql="select * from", condition="", ts=None, id=True, none_check_tag=None, none_type_check=None):
        expect_list = self.inputHandle(input_json)
        print("----", json.dumps(input_json))
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        print("!!!!!----", json.dumps(input_json))
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
            tdSql.checkEqual(sorted(res_row_list[0]), sorted(expect_list[0]))
        tdSql.checkEqual(sorted(res_field_list_without_ts), sorted(expect_list[1]))
        tdSql.checkEqual(res_type_list, expect_list[2])

    def initCheckCase(self, value_type="obj"):
        """
            normal tags and cols, one for every elm
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json, stb_name = self.genFullTypeJson(value_type=value_type)
        self.resCmp(input_json, stb_name)

    def boolTypeCheckCase(self):
        """
            check all normal type
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        full_type_list = ["f", "F", "false", "False", "t", "T", "true", "True"]
        for t_type in full_type_list:
            input_json_list = [self.genFullTypeJson(tag_value=self.genTagValue(t0_value=t_type))[0],
                                self.genFullTypeJson(col_value=self.genTsColValue(value=t_type, t_type="bool"))[0]]
            for input_json in input_json_list:
                try:
                    self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                    raise Exception("should not reach here")
                except SchemalessError as err:
                    tdSql.checkNotEqual(err.errno, 0)

    def symbolsCheckCase(self, value_type="obj"):
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
        nchar_symbols = binary_symbols
        input_sql1, stb_name1 = self.genFullTypeJson(col_value=self.genTsColValue(value=binary_symbols, t_type="binary", value_type=value_type),
                                    tag_value=self.genTagValue(t7_value=binary_symbols, t8_value=nchar_symbols, value_type=value_type))
        input_sql2, stb_name2 = self.genFullTypeJson(col_value=self.genTsColValue(value=nchar_symbols, t_type="nchar", value_type=value_type),
                                    tag_value=self.genTagValue(t7_value=binary_symbols, t8_value=nchar_symbols, value_type=value_type))
        self.resCmp(input_sql1, stb_name1)
        self.resCmp(input_sql2, stb_name2)

    def tsCheckCase(self, value_type="obj"):
        """
            test ts list --> ["1626006833639000000ns", "1626006833639019us", "1626006833640ms", "1626006834s", "1626006822639022"]
            # ! us级时间戳都为0时，数据库中查询显示，但python接口拿到的结果不显示 .000000的情况请确认，目前修改时间处理代码可以通过
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        ts_list = ["1626006833639000000ns", "1626006833639019us", "1626006833640ms", "1626006834s", "1626006834", 0]
        for ts in ts_list:
            if "s" in str(ts):
                input_json, stb_name = self.genFullTypeJson(ts_value=self.genTsColValue(value=int(tdCom.splitNumLetter(ts)[0]), t_type=tdCom.splitNumLetter(ts)[1]))
                self.resCmp(input_json, stb_name, ts=ts)
            else:
                input_json, stb_name = self.genFullTypeJson(ts_value=self.genTsColValue(value=int(ts), t_type="s", value_type=value_type))
                self.resCmp(input_json, stb_name, ts=ts)
                if int(ts) == 0:
                    if value_type == "obj":
                        input_json_list = [self.genFullTypeJson(ts_value=self.genTsColValue(value=int(ts), t_type="")),
                                            self.genFullTypeJson(ts_value=self.genTsColValue(value=int(ts), t_type="ns")),
                                            self.genFullTypeJson(ts_value=self.genTsColValue(value=int(ts), t_type="us")),
                                            self.genFullTypeJson(ts_value=self.genTsColValue(value=int(ts), t_type="ms")),
                                            self.genFullTypeJson(ts_value=self.genTsColValue(value=int(ts), t_type="s"))]
                    elif value_type == "default":
                        input_json_list = [self.genFullTypeJson(ts_value=self.genTsColValue(value=int(ts), value_type=value_type))]
                    for input_json in input_json_list:
                        self.resCmp(input_json[0], input_json[1], ts=ts)
                else:
                    input_json = self.genFullTypeJson(ts_value=self.genTsColValue(value=int(ts), t_type=""))[0]
                    try:
                        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                        raise Exception("should not reach here")
                    except SchemalessError as err:
                        tdSql.checkNotEqual(err.errno, 0)
        # check result
        #! bug
        tdSql.execute(f"drop database if exists test_ts")
        tdSql.execute(f"create database if not exists test_ts precision 'ms' schemaless 1")
        tdSql.execute("use test_ts")
        input_json = [{"metric": "test_ms", "timestamp": {"value": 1626006833640, "type": "ms"}, "value": True, "tags": {"t0": True}},
                    {"metric": "test_ms", "timestamp": {"value": 1626006833641, "type": "ms"}, "value": False, "tags": {"t0": True}}]
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        res = tdSql.query('select * from test_ms', True)
        tdSql.checkEqual(str(res[0][0]), "2021-07-11 20:33:53.640000")
        tdSql.checkEqual(str(res[1][0]), "2021-07-11 20:33:53.641000")

        tdSql.execute(f"drop database if exists test_ts")
        tdSql.execute(f"create database if not exists test_ts precision 'us' schemaless 1")
        tdSql.execute("use test_ts")
        input_json = [{"metric": "test_us", "timestamp": {"value": 1626006833639000, "type": "us"}, "value": True, "tags": {"t0": True}},
                    {"metric": "test_us", "timestamp": {"value": 1626006833639001, "type": "us"}, "value": False, "tags": {"t0": True}}]
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        res = tdSql.query('select * from test_us', True)
        tdSql.checkEqual(str(res[0][0]), "2021-07-11 20:33:53.639000")
        tdSql.checkEqual(str(res[1][0]), "2021-07-11 20:33:53.639001")

        tdSql.execute(f"drop database if exists test_ts")
        tdSql.execute(f"create database if not exists test_ts precision 'ns' schemaless 1")
        tdSql.execute("use test_ts")
        input_json = [{"metric": "test_ns", "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": True, "tags": {"t0": True}},
                    {"metric": "test_ns", "timestamp": {"value": 1626006833639000001, "type": "ns"}, "value": False, "tags": {"t0": True}}]
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        res = tdSql.query('select * from test_ns', True)
        tdSql.checkEqual(str(res[0][0]), "1626006833639000000")
        tdSql.checkEqual(str(res[1][0]), "1626006833639000001")
        self.createDb()

    def idSeqCheckCase(self, value_type="obj"):
        """
            check id.index in tags
            eg: t0=**,id=**,t1=**
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json, stb_name = self.genFullTypeJson(id_change_tag=True, value_type=value_type)
        self.resCmp(input_json, stb_name)

    def idLetterCheckCase(self, value_type="obj"):
        """
            check id param
            eg: id and ID
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json, stb_name = self.genFullTypeJson(id_upper_tag=True, value_type=value_type)
        self.resCmp(input_json, stb_name)
        input_json, stb_name = self.genFullTypeJson(id_mixul_tag=True, value_type=value_type)
        self.resCmp(input_json, stb_name)
        input_json, stb_name = self.genFullTypeJson(id_change_tag=True, id_upper_tag=True, value_type=value_type)
        self.resCmp(input_json, stb_name)

    def noIdCheckCase(self, value_type="obj"):
        """
            id not exist
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json, stb_name = self.genFullTypeJson(id_noexist_tag=True, value_type=value_type)
        self.resCmp(input_json, stb_name)
        query_sql = f"select tbname from {stb_name}"
        res_row_list = self.resHandle(query_sql, True)[0]
        if len(res_row_list[0][0]) > 0:
            tdSql.checkColNameList(res_row_list, res_row_list)
        else:
            tdSql.checkColNameList(res_row_list, "please check noIdCheckCase")

    def maxColTagCheckCase(self, value_type="obj"):
        """
            max tag count is 128
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        for input_json in [self.genLongJson(128, value_type)[0]]:
            tdCom.cleanTb(dbname="test")
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        for input_json in [self.genLongJson(129, value_type)[0]]:
            tdCom.cleanTb(dbname="test")
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

    def idIllegalNameCheckCase(self, value_type="obj"):
        """
            test illegal id name
            mix "`~!@#$¥%^&*()-+={}|[]、「」【】\:;《》<>?"
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        rstr = list("`~!@#$¥%^&*()-+={}|[]、「」【】\:;《》<>?")
        for i in rstr:
            input_json = self.genFullTypeJson(tb_name=f'aa{i}bb', value_type=value_type)[0]
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

    def idStartWithNumCheckCase(self, value_type="obj"):
        """
            id is start with num
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json = self.genFullTypeJson(tb_name="1aaabbb", value_type=value_type)[0]
        try:
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def nowTsCheckCase(self, value_type="obj"):
        """
            check now unsupported
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json = self.genFullTypeJson(ts_value=self.genTsColValue(value="now", t_type="ns", value_type=value_type))[0]
        try:
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def dateFormatTsCheckCase(self, value_type="obj"):
        """
            check date format ts unsupported
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json = self.genFullTypeJson(ts_value=self.genTsColValue(value="2021-07-21\ 19:01:46.920", t_type="ns", value_type=value_type))[0]
        try:
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

    def illegalTsCheckCase(self, value_type="obj"):
        """
            check ts format like 16260068336390us19
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json = self.genFullTypeJson(ts_value=self.genTsColValue(value="16260068336390us19", t_type="us", value_type=value_type))[0]
        try:
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def tbnameCheckCase(self, value_type="obj"):
        """
            check length 192
            check upper tbname
            chech upper tag
            length of stb_name tb_name <= 192
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tdSql.execute('reset query cache')
        stb_name_192 = tdCom.getLongName(len=192, mode="letters")
        tb_name_192 = tdCom.getLongName(len=192, mode="letters")
        input_json, stb_name = self.genFullTypeJson(stb_name=stb_name_192, tb_name=tb_name_192, value_type=value_type)
        self.resCmp(input_json, stb_name)
        tdSql.query(f'select * from {stb_name}')
        tdSql.checkRows(1)
        for input_json in [self.genFullTypeJson(stb_name=tdCom.getLongName(len=193, mode="letters"), tb_name=tdCom.getLongName(len=5, mode="letters"), value_type=value_type)[0]]:
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)
        stbname = tdCom.getLongName(len=10, mode="letters")
        input_json = {'metric': f'A{stbname}', 'timestamp': {'value': 1626006833639000000, 'type': 'ns'}, 'value': {'value': False, 'type': 'bool'}, 'tags': {'t1': {'value': 127, 'type': 'tinyint'}, "t2": 127}}
        stb_name = f'`A{stbname}`'
        self.resCmp(input_json, stb_name)
        tdSql.execute(f"drop table {stb_name}")

    def tagNameLengthCheckCase(self):
        """
            check tag name limit <= 62
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tag_name = tdCom.getLongName(61, "letters")
        tag_name = f't{tag_name}'
        stb_name = tdCom.getLongName(7, "letters")
        input_json = {'metric': stb_name, 'timestamp': {'value': 1626006833639000000, 'type': 'ns'}, 'value': "bcdaaa", 'tags': {tag_name: {'value': False, 'type': 'bool'}}}
        self.resCmp(input_json, stb_name)
        input_json = {'metric': stb_name, 'timestamp': {'value': 1626006833639000001, 'type': 'ns'}, 'value': "bcdaaaa", 'tags': {tdCom.getLongName(65, "letters"): {'value': False, 'type': 'bool'}}}
        try:
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def tagValueLengthCheckCase(self, value_type="obj"):
        """
            check full type tag value limit
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        # i8
        for t1 in [-127, 127]:
            input_json, stb_name = self.genFullTypeJson(tag_value=self.genTagValue(t1_value=t1, value_type=value_type))
            self.resCmp(input_json, stb_name)
        for t1 in [-128, 128]:
            input_json = self.genFullTypeJson(tag_value=self.genTagValue(t1_value=t1))[0]
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        #i16
        for t2 in [-32767, 32767]:
            input_json, stb_name = self.genFullTypeJson(tag_value=self.genTagValue(t2_value=t2, value_type=value_type))
            self.resCmp(input_json, stb_name)
        for t2 in [-32768, 32768]:
            input_json = self.genFullTypeJson(tag_value=self.genTagValue(t2_value=t2))[0]
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        #i32
        for t3 in [-2147483647, 2147483647]:
            input_json, stb_name = self.genFullTypeJson(tag_value=self.genTagValue(t3_value=t3, value_type=value_type))
            self.resCmp(input_json, stb_name)
        for t3 in [-2147483648, 2147483648]:
            input_json = self.genFullTypeJson(tag_value=self.genTagValue(t3_value=t3))[0]
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        #i64
        for t4 in [-9223372036854775807, 9223372036854775807]:
            input_json, stb_name = self.genFullTypeJson(tag_value=self.genTagValue(t4_value=t4, value_type=value_type))
            self.resCmp(input_json, stb_name)

        for t4 in [-9223372036854775808, 9223372036854775808]:
            input_json = self.genFullTypeJson(tag_value=self.genTagValue(t4_value=t4))[0]
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # f32
        for t5 in [-3.4028234663852885981170418348451692544*(10**38), 3.4028234663852885981170418348451692544*(10**38)]:
            input_json, stb_name = self.genFullTypeJson(tag_value=self.genTagValue(t5_value=t5, value_type=value_type))
            self.resCmp(input_json, stb_name)
        # * limit set to 3.4028234664*(10**38)
        for t5 in [-3.4028234664*(10**38), 3.4028234664*(10**38)]:
            input_json = self.genFullTypeJson(tag_value=self.genTagValue(t5_value=t5))[0]
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # f64
        for t6 in [-1.79769*(10**308), -1.79769*(10**308)]:
            input_json, stb_name = self.genFullTypeJson(tag_value=self.genTagValue(t6_value=t6, value_type=value_type))
            self.resCmp(input_json, stb_name)
        for t6 in [float(-1.797693134862316*(10**308)), -1.797693134862316*(10**308)]:
            input_json = self.genFullTypeJson(tag_value=self.genTagValue(t6_value=t6, value_type=value_type))[0]
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        if value_type == "obj":
            # binary
            stb_name = tdCom.getLongName(7, "letters")
            input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': tdCom.getLongName(16374, "letters"), 'type': 'binary'}}}
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': tdCom.getLongName(16375, "letters"), 'type': 'binary'}}}
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

            # # nchar
            # # * legal nchar could not be larger than 16374/4
            stb_name = tdCom.getLongName(7, "letters")
            input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': tdCom.getLongName(4093, "letters"), 'type': 'nchar'}}}
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

            input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': tdCom.getLongName(4094, "letters"), 'type': 'nchar'}}}
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)
        elif value_type == "default":
            stb_name = tdCom.getLongName(7, "letters")
            if tdSql.getVariable("defaultJSONStrType")[0].lower() == "binary":
                input_json = {"metric": stb_name, "timestamp": 1626006834, "value": True, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1": tdCom.getLongName(16374, "letters")}}
            elif tdSql.getVariable("defaultJSONStrType")[0].lower() == "nchar":
                input_json = {"metric": stb_name, "timestamp": 1626006834, "value": True, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1": tdCom.getLongName(4093, "letters")}}
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            if tdSql.getVariable("defaultJSONStrType")[0].lower() == "binary":
                input_json = {"metric": stb_name, "timestamp": 1626006834, "value": True, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1": tdCom.getLongName(16375, "letters")}}
            elif tdSql.getVariable("defaultJSONStrType")[0].lower() == "nchar":
                input_json = {"metric": stb_name, "timestamp": 1626006834, "value": True, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1": tdCom.getLongName(4094, "letters")}}
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

    def colValueLengthCheckCase(self, value_type="obj"):
        """
            check full type col value limit
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        # i8
        for value in [-128, 127]:
            input_json, stb_name = self.genFullTypeJson(col_value=self.genTsColValue(value=value, t_type="tinyint", value_type=value_type))
            self.resCmp(input_json, stb_name)
        tdCom.cleanTb(dbname="test")
        for value in [-129, 128]:
            input_json = self.genFullTypeJson(col_value=self.genTsColValue(value=value, t_type="tinyint"))[0]
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)
        # i16
        tdCom.cleanTb(dbname="test")
        for value in [-32768]:
            input_json, stb_name = self.genFullTypeJson(col_value=self.genTsColValue(value=value, t_type="smallint", value_type=value_type))
            self.resCmp(input_json, stb_name)
        tdCom.cleanTb(dbname="test")
        for value in [-32769, 32768]:
            input_json = self.genFullTypeJson(col_value=self.genTsColValue(value=value, t_type="smallint"))[0]
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # i32
        tdCom.cleanTb(dbname="test")
        for value in [-2147483648]:
            input_json, stb_name = self.genFullTypeJson(col_value=self.genTsColValue(value=value, t_type="int", value_type=value_type))
            self.resCmp(input_json, stb_name)
        tdCom.cleanTb(dbname="test")
        for value in [-2147483649, 2147483648]:
            input_json = self.genFullTypeJson(col_value=self.genTsColValue(value=value, t_type="int"))[0]
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # i64
        tdCom.cleanTb(dbname="test")
        for value in [-9223372036854775808]:
            input_json, stb_name = self.genFullTypeJson(col_value=self.genTsColValue(value=value, t_type="bigint", value_type=value_type))
            self.resCmp(input_json, stb_name)
        # ! bug
        # tdCom.cleanTb(dbname="test")
        # for value in [-9223372036854775809, 9223372036854775808]:
        #     print(value)
        #     input_json = self.genFullTypeJson(col_value=self.genTsColValue(value=value, t_type="bigint"))[0]
        #     print(json.dumps(input_json))
        #     try:
        #         self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        #         raise Exception("should not reach here")
        #     except SchemalessError as err:
        #         tdSql.checkNotEqual(err.errno, 0)

        # f32
        tdCom.cleanTb(dbname="test")
        for value in [-3.4028234663852885981170418348451692544*(10**38), 3.4028234663852885981170418348451692544*(10**38)]:
            input_json, stb_name = self.genFullTypeJson(col_value=self.genTsColValue(value=value, t_type="float", value_type=value_type))
            self.resCmp(input_json, stb_name)
        # * limit set to 4028234664*(10**38)
        tdCom.cleanTb(dbname="test")
        for value in [-3.4028234664*(10**38), 3.4028234664*(10**38)]:
            input_json = self.genFullTypeJson(col_value=self.genTsColValue(value=value, t_type="float"))[0]
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # f64
        tdCom.cleanTb(dbname="test")
        for value in [-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308), -1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)]:
            input_json, stb_name = self.genFullTypeJson(col_value=self.genTsColValue(value=value, t_type="double", value_type=value_type))
            self.resCmp(input_json, stb_name)
        # * limit set to 1.797693134862316*(10**308)
        tdCom.cleanTb(dbname="test")
        for value in [-1.797693134862316*(10**308), -1.797693134862316*(10**308)]:
            input_json = self.genFullTypeJson(col_value=self.genTsColValue(value=value, t_type="double", value_type=value_type))[0]
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                # raise Exception("should not reach here")
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # if value_type == "obj":
        #     # binary
        #     tdCom.cleanTb(dbname="test")
        #     stb_name = tdCom.getLongName(7, "letters")
        #     input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': tdCom.getLongName(16374, "letters"), 'type': 'binary'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

        #     tdCom.cleanTb(dbname="test")
        #     input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': tdCom.getLongName(16375, "letters"), 'type': 'binary'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     try:
        #         self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        #         raise Exception("should not reach here")
        #     except SchemalessError as err:
        #         tdSql.checkNotEqual(err.errno, 0)

        #     # nchar
        #     # * legal nchar could not be larger than 16374/4
        #     tdCom.cleanTb(dbname="test")
        #     stb_name = tdCom.getLongName(7, "letters")
        #     input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': tdCom.getLongName(4093, "letters"), 'type': 'nchar'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

        #     tdCom.cleanTb(dbname="test")
        #     input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': tdCom.getLongName(4094, "letters"), 'type': 'nchar'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     try:
        #         self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        #         raise Exception("should not reach here")
        #     except SchemalessError as err:
        #         tdSql.checkNotEqual(err.errno, 0)
        # elif value_type == "default":
        #     # binary
        #     tdCom.cleanTb(dbname="test")
        #     stb_name = tdCom.getLongName(7, "letters")
        #     if tdSql.getVariable("defaultJSONStrType")[0].lower() == "binary":
        #         input_json = {"metric": stb_name, "timestamp": 1626006834, "value": tdCom.getLongName(16374, "letters"), "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     elif tdSql.getVariable("defaultJSONStrType")[0].lower() == "nchar":
        #         input_json = {"metric": stb_name, "timestamp": 1626006834, "value": tdCom.getLongName(4093, "letters"), "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        #     tdCom.cleanTb(dbname="test")
        #     if tdSql.getVariable("defaultJSONStrType")[0].lower() == "binary":
        #         input_json = {"metric": stb_name, "timestamp": 1626006834, "value": tdCom.getLongName(16375, "letters"), "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     elif tdSql.getVariable("defaultJSONStrType")[0].lower() == "nchar":
        #         input_json = {"metric": stb_name, "timestamp": 1626006834, "value": tdCom.getLongName(4094, "letters"), "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     try:
        #         self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        #         raise Exception("should not reach here")
        #     except SchemalessError as err:
        #         tdSql.checkNotEqual(err.errno, 0)

    def tagColIllegalValueCheckCase(self, value_type="obj"):

        """
            test illegal tag col value
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        # bool
        for i in ["TrUe", "tRue", "trUe", "truE", "FalsE", "fAlse", "faLse", "falSe", "falsE"]:
            try:
                input_json1 = self.genFullTypeJson(tag_value=self.genTagValue(t0_value=i))[0]
                self._conn.schemaless_insert([json.dumps(input_json1)], 2, None)
                input_json2 = self.genFullTypeJson(col_value=self.genTsColValue(value=i, t_type="bool"))[0]
                self._conn.schemaless_insert([json.dumps(input_json2)], 2, None)
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # i8 i16 i32 i64 f32 f64
        for input_json in [
                self.genFullTypeJson(tag_value=self.genTagValue(t1_value="1s2"))[0],
                self.genFullTypeJson(tag_value=self.genTagValue(t2_value="1s2"))[0],
                self.genFullTypeJson(tag_value=self.genTagValue(t3_value="1s2"))[0],
                self.genFullTypeJson(tag_value=self.genTagValue(t4_value="1s2"))[0],
                self.genFullTypeJson(tag_value=self.genTagValue(t5_value="11.1s45"))[0],
                self.genFullTypeJson(tag_value=self.genTagValue(t6_value="11.1s45"))[0],
            ]:
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # check binary and nchar blank
        input_sql1 = self.genFullTypeJson(col_value=self.genTsColValue(value="abc aaa", t_type="binary", value_type=value_type))[0]
        input_sql2 = self.genFullTypeJson(col_value=self.genTsColValue(value="abc aaa", t_type="nchar", value_type=value_type))[0]
        input_sql3 = self.genFullTypeJson(tag_value=self.genTagValue(t7_value="abc aaa", value_type=value_type))[0]
        input_sql4 = self.genFullTypeJson(tag_value=self.genTagValue(t8_value="abc aaa", value_type=value_type))[0]
        for input_json in [input_sql1, input_sql2, input_sql3, input_sql4]:
            try:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            except SchemalessError as err:
                tdSql.checkNotEqual(err.errno, 0)

        # check accepted binary and nchar symbols
        # # * ~!@#$¥%^&*()-+={}|[]、「」:;
        for symbol in list('~!@#$¥%^&*()-+={}|[]、「」:;'):
            input_json1 = self.genFullTypeJson(col_value=self.genTsColValue(value=f"abc{symbol}aaa", t_type="binary", value_type=value_type))[0]
            input_json2 = self.genFullTypeJson(tag_value=self.genTagValue(t8_value=f"abc{symbol}aaa", value_type=value_type))[0]
            self._conn.schemaless_insert([json.dumps(input_json1)], TDSmlProtocolType.JSON.value, None)
            self._conn.schemaless_insert([json.dumps(input_json2)], TDSmlProtocolType.JSON.value, None)

    def duplicateIdTagColInsertCheckCase(self, value_type="obj"):
        """
            check duplicate Id Tag Col
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json = self.genFullTypeJson(id_double_tag=True, value_type=value_type)[0]
        print(input_json)
        try:
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

        input_json = self.genFullTypeJson(tag_value=self.genTagValue(t5_value=11.12345027923584, t6_type="float", t6_value=22.12345027923584, value_type=value_type))[0]
        try:
            self._conn.schemaless_insert([json.dumps(input_json).replace("t6", "t5")], 2, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    ##### stb exist #####
    def noIdStbExistCheckCase(self, value_type="obj"):
        """
            case no id when stb exist
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json, stb_name = self.genFullTypeJson(tb_name="sub_table_0123456", col_value=self.genTsColValue(value=True, t_type="bool", value_type=value_type), tag_value=self.genTagValue(t0_value=True, value_type=value_type))
        self.resCmp(input_json, stb_name)
        input_json, stb_name = self.genFullTypeJson(stb_name=stb_name, id_noexist_tag=True, col_value=self.genTsColValue(value=True, t_type="bool", value_type=value_type), tag_value=self.genTagValue(t0_value=True, value_type=value_type))
        self.resCmp(input_json, stb_name, condition='where tbname like "t_%"')
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(1)

    def duplicateInsertExistCheckCase(self, value_type="obj"):
        """
            check duplicate insert when stb exist
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json, stb_name = self.genFullTypeJson(value_type=value_type)
        self.resCmp(input_json, stb_name)
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.resCmp(input_json, stb_name)

    def tagColBinaryNcharLengthCheckCase(self, value_type="obj"):
        """
            check length increase
        """
        tdCom.cleanTb(dbname="test")
        input_json, stb_name = self.genFullTypeJson(value_type=value_type)
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.resCmp(input_json, stb_name)
        tb_name = tdCom.getLongName(5, "letters")
        input_json, stb_name = self.genFullTypeJson(stb_name=stb_name, tb_name=tb_name, tag_value=self.genTagValue(t7_value="binaryTagValuebinaryTagValue", t8_value="ncharTagValuencharTagValue", value_type=value_type))
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.resCmp(input_json, stb_name, condition=f'where tbname like "{tb_name}"')

    def lengthIcreaseCrashCheckCase(self):
        """
            check length increase
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        stb_name = "test_crash"
        input_json = self.genFullTypeJson(stb_name=stb_name)[0]
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        os.system('python3 query/schemalessQueryCrash.py &')
        time.sleep(2)
        tb_name = tdCom.getLongName(5, "letters")
        input_json, stb_name = self.genFullTypeJson(stb_name=stb_name, tb_name=tb_name, tag_value=self.genTagValue(t7_value="binaryTagValuebinaryTagValue", t8_value="ncharTagValuencharTagValue"))
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        time.sleep(3)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)

    def tagColAddDupIDCheckCase(self, value_type="obj"):
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
            input_json, stb_name = self.genFullTypeJson(tb_name=tb_name, col_value=self.genTsColValue(value=True, t_type="bool", value_type=value_type), tag_value=self.genTagValue(t0_value=True, value_type=value_type))
            self.resCmp(input_json, stb_name)
            input_json, stb_name = self.genFullTypeJson(stb_name=stb_name, tb_name=tb_name, col_value=self.genTsColValue(value=False, t_type="bool", value_type=value_type), tag_value=self.genTagValue(t0_value=True, value_type=value_type), t_add_tag=True)
            if db_update_tag == 1 :
                self.resCmp(input_json, stb_name, condition=f'where tbname like "{tb_name}"', none_check_tag=True)
                tdSql.query(f'select * from {stb_name} where tbname like "{tb_name}"')
                tdSql.checkData(0, 11, None)
                tdSql.checkData(0, 12, None)
            else:
                self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                tdSql.query(f'select * from {stb_name} where tbname like "{tb_name}"')
                tdSql.checkData(0, 1, True)
                tdSql.checkData(0, 11, None)
                tdSql.checkData(0, 12, None)
            self.createDb()

    def tagAddCheckCase(self, value_type="obj"):
        """
            check tag count add
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tb_name = tdCom.getLongName(7, "letters")
        input_json, stb_name = self.genFullTypeJson(tb_name=tb_name, col_value=self.genTsColValue(value=True, t_type="bool", value_type=value_type), tag_value=self.genTagValue(t0_value=True, value_type=value_type))
        self.resCmp(input_json, stb_name)
        tb_name_1 = tdCom.getLongName(7, "letters")
        input_json, stb_name = self.genFullTypeJson(stb_name=stb_name, tb_name=tb_name_1, col_value=self.genTsColValue(value=True, t_type="bool", value_type=value_type), tag_value=self.genTagValue(t0_value=True, value_type=value_type), t_add_tag=True)
        self.resCmp(input_json, stb_name, condition=f'where tbname like "{tb_name_1}"')
        res_row_list = self.resHandle(f"select t10,t11 from {tb_name}", True)[0]
        tdSql.checkEqual(res_row_list[0], ['None', 'None'])
        self.resCmp(input_json, stb_name, condition=f'where tbname like "{tb_name}"', none_check_tag=True)

    def tagMd5Check(self, value_type="obj"):
        """
            condition: stb not change
            insert two table, keep tag unchange, change col
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json, stb_name = self.genFullTypeJson(col_value=self.genTsColValue(value=True, t_type="bool", value_type=value_type), tag_value=self.genTagValue(t0_value=True, value_type=value_type), id_noexist_tag=True)
        self.resCmp(input_json, stb_name)
        tb_name1 = self.getNoIdTbName(stb_name)
        input_json, stb_name = self.genFullTypeJson(stb_name=stb_name, col_value=self.genTsColValue(value=True, t_type="bool", value_type=value_type), tag_value=self.genTagValue(t0_value=True, value_type=value_type), id_noexist_tag=True)
        self.resCmp(input_json, stb_name)
        tb_name2 = self.getNoIdTbName(stb_name)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(1)
        tdSql.checkEqual(tb_name1, tb_name2)
        input_json, stb_name = self.genFullTypeJson(stb_name=stb_name, col_value=self.genTsColValue(value=True, t_type="bool", value_type=value_type), tag_value=self.genTagValue(t0_value=True, value_type=value_type), id_noexist_tag=True, t_add_tag=True)
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        tb_name3 = self.getNoIdTbName(stb_name)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)
        tdSql.checkNotEqual(tb_name1, tb_name3)

    # * tag binary max is 16384, col+ts binary max  49151
    def tagColBinaryMaxLengthCheckCase(self, value_type="obj"):
        """
            every binary and nchar must be length+2
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        stb_name = tdCom.getLongName(7, "letters")
        tb_name = f'{stb_name}_1'
        tag_value = {"t0": {"value": True, "type": "bool"}}
        tag_value["id"] = tb_name
        col_value=self.genTsColValue(value=True, t_type="bool", value_type=value_type)
        input_json = {"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": col_value, "tags": tag_value}
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

        # * every binary and nchar must be length+2, so here is two tag, max length could not larger than 16384-2*2
        if value_type == "obj":
            tag_value["t1"] = {"value": tdCom.getLongName(16374, "letters"), "type": "binary"}
            tag_value["t2"] = {"value": tdCom.getLongName(5, "letters"), "type": "binary"}
        elif value_type == "default":
            if tdSql.getVariable("defaultJSONStrType")[0].lower() == "binary":
                tag_value["t1"] = tdCom.getLongName(16374, "letters")
                tag_value["t2"] = tdCom.getLongName(5, "letters")
            elif tdSql.getVariable("defaultJSONStrType")[0].lower() == "nchar":
                tag_value["t1"] = tdCom.getLongName(4093, "letters")
                tag_value["t2"] = tdCom.getLongName(1, "letters")
        tag_value.pop('id')
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)
        if value_type == "obj":
            tag_value["t2"] = {"value": tdCom.getLongName(6, "letters"), "type": "binary"}
        elif value_type == "default":
            if tdSql.getVariable("defaultJSONStrType")[0].lower() == "binary":
                tag_value["t2"] = tdCom.getLongName(6, "letters")
            elif tdSql.getVariable("defaultJSONStrType")[0].lower() == "nchar":
                tag_value["t2"] = tdCom.getLongName(2, "letters")
        try:
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)

    # * tag nchar max is 16374/4, col+ts nchar max  49151
    def tagColNcharMaxLengthCheckCase(self, value_type="obj"):
        """
            check nchar length limit
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        stb_name = tdCom.getLongName(7, "letters")
        tb_name = f'{stb_name}_1'
        tag_value = {"t0": True}
        tag_value["id"] = tb_name
        col_value= True
        input_json = {"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": col_value, "tags": tag_value}
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

        # * legal nchar could not be larger than 16374/4
        if value_type == "obj":
            tag_value["t1"] = {"value": tdCom.getLongName(4093, "letters"), "type": "nchar"}
            tag_value["t2"] = {"value": tdCom.getLongName(1, "letters"), "type": "nchar"}
        elif value_type == "default":
            if tdSql.getVariable("defaultJSONStrType")[0].lower() == "binary":
                tag_value["t1"] = tdCom.getLongName(16374, "letters")
                tag_value["t2"] = tdCom.getLongName(5, "letters")
            elif tdSql.getVariable("defaultJSONStrType")[0].lower() == "nchar":
                tag_value["t1"] = tdCom.getLongName(4093, "letters")
                tag_value["t2"] = tdCom.getLongName(1, "letters")
        tag_value.pop('id')
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)
        if value_type == "obj":
            tag_value["t2"] = {"value": tdCom.getLongName(2, "letters"), "type": "binary"}
        elif value_type == "default":
            if tdSql.getVariable("defaultJSONStrType")[0].lower() == "binary":
                tag_value["t2"] = tdCom.getLongName(6, "letters")
            elif tdSql.getVariable("defaultJSONStrType")[0].lower() == "nchar":
                tag_value["t2"] = tdCom.getLongName(2, "letters")
        try:
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)

    def batchInsertCheckCase(self, value_type="obj"):
        """
            test batch insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        stb_name = "stb_name"
        tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 bigint)')
        input_json = [{"metric": "st123456", "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": {"value": 1, "type": "bigint"}, "tags": {"t1": {"value": 3, "type": "bigint"}, "t2": {"value": 4, "type": "double"}, "t3": {"value": "t3", "type": "binary"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006833640000000, "type": "ns"}, "value": {"value": 2, "type": "bigint"}, "tags": {"t1": {"value": 4, "type": "bigint"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}},
                    {"metric": "stb_name", "timestamp": {"value": 1626056811823316532, "type": "ns"}, "value": {"value": 3, "type": "bigint"}, "tags": {"t2": {"value": 5, "type": "double"}, "t3": {"value": "ste", "type": "nchar"}}},
                    {"metric": "stf567890", "timestamp": {"value": 1626006933640000000, "type": "ns"}, "value": {"value": 4, "type": "bigint"}, "tags": {"t1": {"value": 4, "type": "bigint"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006833642000000, "type": "ns"}, "value": {"value": 5, "type": "bigint"}, "tags": {"t1": {"value": 4, "type": "bigint"}, "t2": {"value": 5, "type": "double"}, "t3": {"value": "t4", "type": "binary"}}},
                    {"metric": "stb_name", "timestamp": {"value": 1626056811843316532, "type": "ns"}, "value": {"value": 6, "type": "bigint"}, "tags": {"t2": {"value": 5, "type": "double"}, "t3": {"value": "ste2", "type": "nchar"}}},
                    {"metric": "stb_name", "timestamp": {"value": 1626056812843316532, "type": "ns"}, "value": {"value": 7, "type": "bigint"}, "tags": {"t2": {"value": 5, "type": "double"}, "t3": {"value": "ste2", "type": "nchar"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006933640000000, "type": "ns"}, "value": {"value": 8, "type": "bigint"}, "tags": {"t1": {"value": 4, "type": "bigint"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006933641000000, "type": "ns"}, "value": {"value": 9, "type": "bigint"}, "tags": {"t1": {"value": 4, "type": "bigint"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}}]
        if value_type != "obj":
            input_json = [{"metric": "st123456", "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": 1, "tags": {"t1": 3, "t2": {"value": 4, "type": "double"}, "t3": {"value": "t3", "type": "binary"}}},
                        {"metric": "st123456", "timestamp": {"value": 1626006833640000000, "type": "ns"}, "value": 2, "tags": {"t1": {"value": 4, "type": "double"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}},
                        {"metric": "stb_name", "timestamp": {"value": 1626056811823316532, "type": "ns"}, "value": 3, "tags": {"t2": {"value": 5, "type": "double"}, "t3": {"value": "ste", "type": "nchar"}}},
                        {"metric": "stf567890", "timestamp": {"value": 1626006933640000000, "type": "ns"}, "value": 4, "tags": {"t1": {"value": 4, "type": "bigint"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}},
                        {"metric": "st123456", "timestamp": {"value": 1626006833642000000, "type": "ns"}, "value": {"value": 5, "type": "double"}, "tags": {"t1": {"value": 4, "type": "double"}, "t2": 5.0, "t3": {"value": "t4", "type": "binary"}}},
                        {"metric": "stb_name", "timestamp": {"value": 1626056811843316532, "type": "ns"}, "value": {"value": 6, "type": "double"}, "tags": {"t2": 5.0, "t3": {"value": "ste2", "type": "nchar"}}},
                        {"metric": "stb_name", "timestamp": {"value": 1626056812843316532, "type": "ns"}, "value": {"value": 7, "type": "double"}, "tags": {"t2": {"value": 5, "type": "double"}, "t3": {"value": "ste2", "type": "nchar"}}},
                        {"metric": "st123456", "timestamp": {"value": 1626006933640000000, "type": "ns"}, "value": {"value": 8, "type": "double"}, "tags": {"t1": {"value": 4, "type": "double"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}},
                        {"metric": "st123456", "timestamp": {"value": 1626006933641000000, "type": "ns"}, "value": {"value": 9, "type": "double"}, "tags": {"t1": 4, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}}]
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        tdSql.query('show stables')
        tdSql.checkRows(3)
        tdSql.query('show tables')
        tdSql.checkRows(6)
        tdSql.query('select * from st123456')
        tdSql.checkRows(5)

    def multiInsertCheckCase(self, count, value_type="obj"):
        """
            test multi insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        sql_list = list()
        stb_name = tdCom.getLongName(8, "letters")
        tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 bigint)')
        for i in range(count):
            input_json = self.genFullTypeJson(stb_name=stb_name, col_value=self.genTsColValue(value=tdCom.getLongName(8, "letters"), t_type="binary", value_type=value_type), tag_value=self.genTagValue(t7_value=tdCom.getLongName(8, "letters"), value_type=value_type), id_noexist_tag=True)[0]
            sql_list.append(input_json)
        self._conn.schemaless_insert([json.dumps(sql_list)], TDSmlProtocolType.JSON.value, None)
        tdSql.query('show tables')
        tdSql.checkRows(count)

    def batchErrorInsertCheckCase(self):
        """
            test batch error insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json = [{"metric": "st123456", "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": {"value": "tt", "type": "bool"}, "tags": {"t1": {"value": 3, "type": "bigint"}, "t2": {"value": 4, "type": "double"}, "t3": {"value": "t3", "type": "binary"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006933641000000, "type": "ns"}, "value": {"value": 9, "type": "bigint"}, "tags": {"t1": {"value": 4, "type": "bigint"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}}]
        try:
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def multiColsInsertCheckCase(self, value_type="obj"):
        """
            test multi cols insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json = self.genFullTypeJson(c_multi_tag=True, value_type=value_type)[0]
        try:
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def blankColInsertCheckCase(self, value_type="obj"):
        """
            test blank col insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json = self.genFullTypeJson(c_blank_tag=True, value_type=value_type)[0]
        try:
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def blankTagInsertCheckCase(self, value_type="obj"):
        """
            test blank tag insert
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json = self.genFullTypeJson(t_blank_tag=True, value_type=value_type)[0]
        try:
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def chineseCheckCase(self):
        """
            check nchar ---> chinese
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json, stb_name = self.genFullTypeJson(chinese_tag=True)
        self.resCmp(input_json, stb_name)

    def multiFieldCheckCase(self, value_type="obj"):
        '''
            multi_field
        '''
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json = self.genFullTypeJson(multi_field_tag=True, value_type=value_type)[0]
        try:
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            tdSql.checkNotEqual(err.errno, 0)

    def spellCheckCase(self):
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        stb_name = tdCom.getLongName(8, "letters")
        input_json_list = [{"metric": f'{stb_name}_1', "timestamp": {"value": 1626006833639000000, "type": "Ns"}, "value": {"value": 1, "type": "Bigint"}, "tags": {"t1": {"value": 127, "type": "tinYint"}}},
                        {"metric": f'{stb_name}_2', "timestamp": {"value": 1626006833639000001, "type": "nS"}, "value": {"value": 32767, "type": "smallInt"}, "tags": {"t1": {"value": 32767, "type": "smallInt"}}},
                        {"metric": f'{stb_name}_3', "timestamp": {"value": 1626006833639000002, "type": "NS"}, "value": {"value": 2147483647, "type": "iNt"}, "tags": {"t1": {"value": 2147483647, "type": "iNt"}}},
                        {"metric": f'{stb_name}_4', "timestamp": {"value": 1626006833639019, "type": "Us"}, "value": {"value": 9223372036854775807, "type": "bigInt"}, "tags": {"t1": {"value": 9223372036854775807, "type": "bigInt"}}},
                        {"metric": f'{stb_name}_5', "timestamp": {"value": 1626006833639018, "type": "uS"}, "value": {"value": 11.12345027923584, "type": "flOat"}, "tags": {"t1": {"value": 11.12345027923584, "type": "flOat"}}},
                        {"metric": f'{stb_name}_6', "timestamp": {"value": 1626006833639017, "type": "US"}, "value": {"value": 22.123456789, "type": "douBle"}, "tags": {"t1": {"value": 22.123456789, "type": "douBle"}}},
                        {"metric": f'{stb_name}_7', "timestamp": {"value": 1626006833640, "type": "Ms"}, "value": {"value": "vozamcts", "type": "binaRy"}, "tags": {"t1": {"value": "vozamcts", "type": "binaRy"}}},
                        {"metric": f'{stb_name}_8', "timestamp": {"value": 1626006833641, "type": "mS"}, "value": {"value": "vozamcts", "type": "nchAr"}, "tags": {"t1": {"value": "vozamcts", "type": "nchAr"}}},
                        {"metric": f'{stb_name}_9', "timestamp": {"value": 1626006833642, "type": "MS"}, "value": {"value": "vozamcts", "type": "nchAr"}, "tags": {"t1": {"value": "vozamcts", "type": "nchAr"}}},
                        {"metric": f'{stb_name}_10', "timestamp": {"value": 1626006834, "type": "S"}, "value": {"value": "vozamcts", "type": "nchAr"}, "tags": {"t1": {"value": "vozamcts", "type": "nchAr"}}}]

        for input_sql in input_json_list:
            stb_name = input_sql["metric"]
            self.resCmp(input_sql, stb_name)

    def tbnameTagsColsNameCheckCase(self):
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json = {'metric': 'rFa$sta', 'timestamp': {'value': 1626006834, 'type': 's'}, 'value': {'value': True, 'type': 'bool'}, 'tags': {'Tt!0': {'value': False, 'type': 'bool'}, 'tT@1': {'value': 127, 'type': 'tinyint'}, 't@2': {'value': 32767, 'type': 'smallint'}, 't$3': {'value': 2147483647, 'type': 'int'}, 't%4': {'value': 9223372036854775807, 'type': 'bigint'}, 't^5': {'value': 11.12345027923584, 'type': 'float'}, 't&6': {'value': 22.123456789, 'type': 'double'}, 't*7': {'value': 'binaryTagValue', 'type': 'binary'}, 't!@#$%^&*()_+[];:<>?,9': {'value': 'ncharTagValue', 'type': 'nchar'}, 'id': 'rFas$ta_1'}}
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        query_sql = 'select * from `rFa$sta`'
        query_res = tdSql.query(query_sql, True)
        tdSql.checkEqual(query_res, [(datetime.datetime(2021, 7, 11, 20, 33, 54), True, False, 127, 32767, 2147483647, 9223372036854775807, 11.12345027923584, 22.123456789, 'binaryTagValue', 'ncharTagValue', 'rFas$ta_1')])
        col_tag_res = tdSql.getColNameList(query_sql)
        tdSql.checkEqual(col_tag_res, ['_ts', '_value', 'Tt!0', 'tT@1', 't@2', 't$3', 't%4', 't^5', 't&6', 't*7', 't!@#$%^&*()_+[];:<>?,9', 'id'])
        tdSql.execute('drop table `rFa$sta`')

    def pointTransCheckCase(self, value_type="obj"):
        """
            metric value "." trans to "_"
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json = self.genFullTypeJson(point_trans_tag=True, value_type=value_type)[0]
        self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        tdSql.execute("drop table `.point.trans.test`")

    def genSqlList(self, count=5, stb_name="", tb_name="", value_type="obj"):
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
            d_stb_d_tb_list.append(self.genFullTypeJson(col_value=self.genTsColValue(value=True, t_type="bool", value_type=value_type), tag_value=self.genTagValue(t0_value=True, value_type=value_type)))
            s_stb_s_tb_list.append(self.genFullTypeJson(stb_name=stb_name, tb_name=tb_name, col_value=self.genTsColValue(value=tdCom.getLongName(8, "letters"), t_type="binary", value_type=value_type), tag_value=self.genTagValue(t7_value=tdCom.getLongName(8, "letters"), value_type=value_type)))
            s_stb_s_tb_a_tag_list.append(self.genFullTypeJson(stb_name=stb_name, tb_name=tb_name, col_value=self.genTsColValue(value=tdCom.getLongName(8, "letters"), t_type="binary", value_type=value_type), tag_value=self.genTagValue(t7_value=tdCom.getLongName(8, "letters"), value_type=value_type), t_add_tag=True))
            s_stb_s_tb_m_tag_list.append(self.genFullTypeJson(stb_name=stb_name, tb_name=tb_name, col_value=self.genTsColValue(value=tdCom.getLongName(8, "letters"), t_type="binary", value_type=value_type), tag_value=self.genTagValue(t7_value=tdCom.getLongName(8, "letters"), value_type=value_type), t_mul_tag=True))
            s_stb_d_tb_list.append(self.genFullTypeJson(stb_name=stb_name, col_value=self.genTsColValue(value=tdCom.getLongName(8, "letters"), t_type="binary", value_type=value_type), tag_value=self.genTagValue(t7_value=tdCom.getLongName(8, "letters"), value_type=value_type), id_noexist_tag=True))
            s_stb_d_tb_m_tag_list.append(self.genFullTypeJson(stb_name=stb_name, col_value=self.genTsColValue(value=tdCom.getLongName(8, "letters"), t_type="binary", value_type=value_type), tag_value=self.genTagValue(t7_value=tdCom.getLongName(8, "letters"), value_type=value_type), id_noexist_tag=True, t_mul_tag=True))
            s_stb_d_tb_a_tag_list.append(self.genFullTypeJson(stb_name=stb_name, col_value=self.genTsColValue(value=tdCom.getLongName(8, "letters"), t_type="binary", value_type=value_type), tag_value=self.genTagValue(t7_value=tdCom.getLongName(8, "letters"), value_type=value_type), id_noexist_tag=True, t_add_tag=True))
            s_stb_s_tb_d_ts_list.append(self.genFullTypeJson(stb_name=stb_name, tb_name=tb_name, col_value=self.genTsColValue(value=tdCom.getLongName(8, "letters"), t_type="binary", value_type=value_type), tag_value=self.genTagValue(t7_value=tdCom.getLongName(8, "letters"), value_type=value_type), ts_value = self.genTsColValue(1626006833639000000, "ns")))
            s_stb_s_tb_d_ts_m_tag_list.append(self.genFullTypeJson(stb_name=stb_name, tb_name=tb_name, col_value=self.genTsColValue(value=tdCom.getLongName(8, "letters"), t_type="binary", value_type=value_type), tag_value=self.genTagValue(t7_value=tdCom.getLongName(8, "letters"), value_type=value_type), ts_value = self.genTsColValue(1626006833639000000, "ns"), t_mul_tag=True))
            s_stb_s_tb_d_ts_a_tag_list.append(self.genFullTypeJson(stb_name=stb_name, tb_name=tb_name, col_value=self.genTsColValue(value=tdCom.getLongName(8, "letters"), t_type="binary", value_type=value_type), tag_value=self.genTagValue(t7_value=tdCom.getLongName(8, "letters"), value_type=value_type), ts_value = self.genTsColValue(1626006833639000000, "ns"), t_add_tag=True))
            s_stb_d_tb_d_ts_list.append(self.genFullTypeJson(stb_name=stb_name, col_value=self.genTsColValue(value=tdCom.getLongName(8, "letters"), t_type="binary", value_type=value_type), tag_value=self.genTagValue(t7_value=tdCom.getLongName(8, "letters"), value_type=value_type), id_noexist_tag=True, ts_value = self.genTsColValue(1626006833639000000, "ns")))
            s_stb_d_tb_d_ts_m_tag_list.append(self.genFullTypeJson(stb_name=stb_name, col_value=self.genTsColValue(value=tdCom.getLongName(8, "letters"), t_type="binary", value_type=value_type), tag_value=self.genTagValue(t7_value=tdCom.getLongName(8, "letters"), value_type=value_type), id_noexist_tag=True, ts_value = self.genTsColValue(0, "ns"), t_mul_tag=True))
            s_stb_d_tb_d_ts_a_tag_list.append(self.genFullTypeJson(stb_name=stb_name, col_value=self.genTsColValue(value=tdCom.getLongName(8, "letters"), t_type="binary", value_type=value_type), tag_value=self.genTagValue(t7_value=tdCom.getLongName(8, "letters"), value_type=value_type), id_noexist_tag=True, ts_value = self.genTsColValue(0, "ns"), t_add_tag=True))

        return d_stb_d_tb_list, s_stb_s_tb_list, s_stb_s_tb_a_tag_list, s_stb_s_tb_m_tag_list, \
            s_stb_d_tb_list, s_stb_d_tb_m_tag_list, s_stb_d_tb_a_tag_list, s_stb_s_tb_d_ts_list, \
            s_stb_s_tb_d_ts_m_tag_list, s_stb_s_tb_d_ts_a_tag_list, s_stb_d_tb_d_ts_list, \
            s_stb_d_tb_d_ts_m_tag_list, s_stb_d_tb_d_ts_a_tag_list

    def genMultiThreadSeq(self, sql_list):
        tlist = list()
        for insert_sql in sql_list:
            t = threading.Thread(target=self._conn.schemaless_insert,args=([json.dumps(insert_sql[0])], TDSmlProtocolType.JSON.value, None))
            tlist.append(t)
        return tlist

    def multiThreadRun(self, tlist):
        for t in tlist:
            t.start()
        for t in tlist:
            t.join()

    def stbInsertMultiThreadCheckCase(self, value_type="obj"):
        """
            thread input different stb
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json = self.genSqlList(value_type=value_type)[0]
        self.multiThreadRun(self.genMultiThreadSeq(input_json))
        tdSql.query(f"show tables;")
        tdSql.checkRows(5)

    def sStbStbDdataInsertMultiThreadCheckCase(self, value_type="obj"):
        """
            thread input same stb tb, different data, result keep first data
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tb_name = tdCom.getLongName(7, "letters")
        input_json, stb_name = self.genFullTypeJson(tb_name=tb_name, col_value=self.genTsColValue(value="binaryTagValue", t_type="binary", value_type=value_type))
        self.resCmp(input_json, stb_name)
        s_stb_s_tb_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name, value_type=value_type)[1]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1)
        expected_tb_name = self.getNoIdTbName(stb_name)[0]
        tdSql.checkEqual(tb_name, expected_tb_name)
        tdSql.query(f"select * from {stb_name};")
        tdSql.checkRows(1)

    def sStbStbDdataAtInsertMultiThreadCheckCase(self, value_type="obj"):
        """
            thread input same stb tb, different data, add columes and tags,  result keep first data
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tb_name = tdCom.getLongName(7, "letters")
        input_json, stb_name = self.genFullTypeJson(tb_name=tb_name, col_value=self.genTsColValue(value="binaryTagValue", t_type="binary", value_type=value_type))
        self.resCmp(input_json, stb_name)
        s_stb_s_tb_a_tag_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name, value_type=value_type)[2]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_a_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1)
        expected_tb_name = self.getNoIdTbName(stb_name)[0]
        tdSql.checkEqual(tb_name, expected_tb_name)
        tdSql.query(f"select * from {stb_name};")
        tdSql.checkRows(1)

    def sStbStbDdataMtInsertMultiThreadCheckCase(self, value_type="obj"):
        """
            thread input same stb tb, different data, minus columes and tags,  result keep first data
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tb_name = tdCom.getLongName(7, "letters")
        input_json, stb_name = self.genFullTypeJson(tb_name=tb_name, col_value=self.genTsColValue(value="binaryTagValue", t_type="binary", value_type=value_type))
        self.resCmp(input_json, stb_name)
        s_stb_s_tb_m_tag_list = self.genSqlList(stb_name=stb_name, tb_name=tb_name, value_type=value_type)[3]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_m_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1)
        expected_tb_name = self.getNoIdTbName(stb_name)[0]
        tdSql.checkEqual(tb_name, expected_tb_name)
        tdSql.query(f"select * from {stb_name};")
        tdSql.checkRows(1)

    def sStbDtbDdataInsertMultiThreadCheckCase(self, value_type="obj"):
        """
            thread input same stb, different tb, different data
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json, stb_name = self.genFullTypeJson(col_value=self.genTsColValue(value="binaryTagValue", t_type="binary", value_type=value_type))
        self.resCmp(input_json, stb_name)
        s_stb_d_tb_list = self.genSqlList(stb_name=stb_name, value_type=value_type)[4]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(6)

    def sStbDtbDdataMtInsertMultiThreadCheckCase(self):
        """
            thread input same stb, different tb, different data, add col, mul tag
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json, stb_name = self.genFullTypeJson(col_value=self.genTsColValue(value="binaryTagValue", t_type="binary"))
        self.resCmp(input_json, stb_name)
        s_stb_d_tb_m_tag_list = [({"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": "omfdhyom", "tags": {"t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": "vqowydbc", "tags": {"t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": "plgkckpv", "tags": {"t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": "cujyqvlj", "tags": {"t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": "twjxisat", "tags": {"t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}}}, 'yzwswz')]

        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_m_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(2)

    def sStbDtbDdataAtInsertMultiThreadCheckCase(self, value_type="obj"):
        """
            thread input same stb, different tb, different data, add tag, mul col
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json, stb_name = self.genFullTypeJson(col_value=self.genTsColValue(value="binaryTagValue", t_type="binary", value_type=value_type))
        self.resCmp(input_json, stb_name)
        s_stb_d_tb_a_tag_list = self.genSqlList(stb_name=stb_name, value_type=value_type)[6]
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
        input_json, stb_name = self.genFullTypeJson(tb_name=tb_name, col_value=self.genTsColValue(value="binaryTagValue", t_type="binary"))
        self.resCmp(input_json, stb_name)
        s_stb_s_tb_d_ts_list = [({"metric": stb_name, "timestamp": {"value": 0, "type": "ns"}, "value": "hkgjiwdj", "tags": {"id": tb_name, "t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}, "t7": {"value": "vozamcts", "type": "binary"}, "t8": {"value": "ncharTagValue", "type": "nchar"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 0, "type": "ns"}, "value": "rljjrrul", "tags": {"id": tb_name, "t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}, "t7": {"value": "bmcanhbs", "type": "binary"}, "t8": {"value": "ncharTagValue", "type": "nchar"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 0, "type": "ns"}, "value": "basanglx", "tags": {"id": tb_name, "t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}, "t7": {"value": "enqkyvmb", "type": "binary"}, "t8": {"value": "ncharTagValue", "type": "nchar"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 0, "type": "ns"}, "value": "clsajzpp", "tags": {"id": tb_name, "t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}, "t7": {"value": "eivaegjk", "type": "binary"}, "t8": {"value": "ncharTagValue", "type": "nchar"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 0, "type": "ns"}, "value": "jitwseso", "tags": {"id": tb_name, "t0": {"value": True, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}, "t7": {"value": "yhlwkddq", "type": "binary"}, "t8": {"value": "ncharTagValue", "type": "nchar"}}}, 'yzwswz')]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_d_ts_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(6)

    def sStbStbDdataDtsMtInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different ts, add col, mul tag
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tb_name = tdCom.getLongName(7, "letters")
        input_json, stb_name = self.genFullTypeJson(tb_name=tb_name, col_value=self.genTsColValue(value="binaryTagValue", t_type="binary"))
        self.resCmp(input_json, stb_name)
        s_stb_s_tb_d_ts_m_tag_list = [({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': 'pjndapjb', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'tuzsfrom', 'type': 'binary'}, 'id': tb_name}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': 'llqzvgvw', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'nttjdzgi', 'type': 'binary'}, 'id': tb_name}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': 'tclbosqc', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'uatpzgpi', 'type': 'binary'}, 'id': tb_name}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': 'rlpuzodt', 'tags': {'t0': {'value': True, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'cwnpdnng', 'type': 'binary'}, 'id': tb_name}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': 'rhnikvfq', 'tags': {'t0': {'value': True, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'afcibyeb', 'type': 'binary'}, 'id': tb_name}}, 'punftb')]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_d_ts_m_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(6)
        tdSql.query(f"select * from {stb_name} where t8 is not NULL")
        tdSql.checkRows(6)

    def sStbStbDdataDtsAtInsertMultiThreadCheckCase(self):
        """
            thread input same stb tb, different ts, add tag, mul col
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        tb_name = tdCom.getLongName(7, "letters")
        input_json, stb_name = self.genFullTypeJson(tb_name=tb_name, col_value=self.genTsColValue(value="binaryTagValue", t_type="binary"))
        self.resCmp(input_json, stb_name)
        s_stb_s_tb_d_ts_a_tag_list = [({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': 'pjndapjb', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'tuzsfrom', 'type': 'binary'}, 't8': {'value': 'ncharTagValue', 'type': 'nchar'}, 't11': {'value': 127, 'type': 'tinyint'}, 't10': {'value': 'ncharTagValue', 'type': 'nchar'}, 'id': tb_name}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': 'llqzvgvw', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'nttjdzgi', 'type': 'binary'}, 't8': {'value': 'ncharTagValue', 'type': 'nchar'}, 't11': {'value': 127, 'type': 'tinyint'}, 't10': {'value': 'ncharTagValue', 'type': 'nchar'}, 'id': tb_name}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': {'value': 'tclbosqc', 'type': 'binary'}, 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'uatpzgpi', 'type': 'binary'}, 't8': {'value': 'ncharTagValue', 'type': 'nchar'}, 't11': {'value': 127, 'type': 'tinyint'}, 't10': {'value': 'ncharTagValue', 'type': 'nchar'}, 'id': tb_name}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': 'rlpuzodt', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'cwnpdnng', 'type': 'binary'}, 't8': {'value': 'ncharTagValue', 'type': 'nchar'}, 't11': {'value': 127, 'type': 'tinyint'}, 't10': {'value': 'ncharTagValue', 'type': 'nchar'}, 'id': tb_name}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': {'value': 'rhnikvfq', 'type': 'binary'}, 'tags': {'t0': {'value': True, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'afcibyeb', 'type': 'binary'}, 't8': {'value': 'ncharTagValue', 'type': 'nchar'}, 't11': {'value': 127, 'type': 'tinyint'}, 't10': {'value': 'ncharTagValue', 'type': 'nchar'}, 'id': tb_name}}, 'punftb')]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_s_tb_d_ts_a_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(6)
        for t in ["t10", "t11"]:
            tdSql.query(f"select * from {stb_name} where {t} is not NULL;")
            tdSql.checkRows(0)

    def sStbDtbDdataDtsInsertMultiThreadCheckCase(self, value_type="obj"):
        """
            thread input same stb, different tb, data, ts
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json, stb_name = self.genFullTypeJson(col_value=self.genTsColValue(value="binaryTagValue", t_type="binary", value_type=value_type))
        self.resCmp(input_json, stb_name)
        s_stb_d_tb_d_ts_list = self.genSqlList(stb_name=stb_name, value_type=value_type)[10]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_d_ts_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(6)

    def sStbDtbDdataDtsMtInsertMultiThreadCheckCase(self):
        """
            thread input same stb, different tb, data, ts, add col, mul tag
        """
        tdLog.info(f'{sys._getframe().f_code.co_name}() function is running')
        tdCom.cleanTb(dbname="test")
        input_json, stb_name = self.genFullTypeJson(col_value=self.genTsColValue(value="binaryTagValue", t_type="binary"))
        self.resCmp(input_json, stb_name)
        s_stb_d_tb_d_ts_m_tag_list = [({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': 'pjndapjb', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': {'value': 'llqzvgvw', 'type': 'binary'}, 'tags': {'t0': {'value': True, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': 'tclbosqc', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': {'value': 'rlpuzodt', 'type': 'binary'}, 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': {'value': 'rhnikvfq', 'type': 'binary'}, 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}}}, 'punftb')]
        self.multiThreadRun(self.genMultiThreadSeq(s_stb_d_tb_d_ts_m_tag_list))
        tdSql.query(f"show tables;")
        tdSql.checkRows(3)

    def test(self):
        try:
            input_json = f'test_nchar 0 L"涛思数据" t0=f,t1=L"涛思数据",t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64'
            self._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            # input_json, stb_name = self.genFullTypeJson()
            # self.resCmp(input_json, stb_name)
        except SchemalessError as err:
            print(err.errno)

    def runAll(self):
        for value_type in ["obj", "default"]:
            self.initCheckCase(value_type)
            self.symbolsCheckCase(value_type)
            # self.tsCheckCase(value_type)
            self.idSeqCheckCase(value_type)
            self.idLetterCheckCase(value_type)
            self.noIdCheckCase(value_type)
            self.maxColTagCheckCase(value_type)
            self.idIllegalNameCheckCase(value_type)
            self.idStartWithNumCheckCase(value_type)
            self.nowTsCheckCase(value_type)
            self.dateFormatTsCheckCase(value_type)
            self.illegalTsCheckCase(value_type)
            self.tbnameCheckCase(value_type)
            # self.tagValueLengthCheckCase(value_type)
            self.colValueLengthCheckCase(value_type)
            self.tagColIllegalValueCheckCase(value_type)
            # self.duplicateIdTagColInsertCheckCase(value_type)
            self.noIdStbExistCheckCase(value_type)
            self.duplicateInsertExistCheckCase(value_type)
            # self.tagColBinaryNcharLengthCheckCase(value_type)
            # self.tagColAddDupIDCheckCase(value_type)
            # self.tagAddCheckCase(value_type)
            # self.tagMd5Check(value_type)
            # self.tagColBinaryMaxLengthCheckCase(value_type)
            # self.tagColNcharMaxLengthCheckCase(value_type)
            # self.batchInsertCheckCase(value_type)
            # self.multiInsertCheckCase(10, value_type)
            self.multiColsInsertCheckCase(value_type)
            self.blankColInsertCheckCase(value_type)
            self.blankTagInsertCheckCase(value_type)
            self.multiFieldCheckCase(value_type)
            # self.stbInsertMultiThreadCheckCase(value_type)
        self.pointTransCheckCase(value_type)
        self.tagNameLengthCheckCase()
        self.boolTypeCheckCase()
        self.batchErrorInsertCheckCase()
        self.chineseCheckCase()
        # self.spellCheckCase()
        self.tbnameTagsColsNameCheckCase()
        # # MultiThreads
        # self.sStbStbDdataInsertMultiThreadCheckCase()
        # self.sStbStbDdataAtInsertMultiThreadCheckCase()
        # self.sStbStbDdataMtInsertMultiThreadCheckCase()
        # self.sStbDtbDdataInsertMultiThreadCheckCase()
        # self.sStbDtbDdataAtInsertMultiThreadCheckCase()
        # self.sStbDtbDdataDtsInsertMultiThreadCheckCase()
        # self.sStbDtbDdataMtInsertMultiThreadCheckCase()
        # self.sStbStbDdataDtsInsertMultiThreadCheckCase()
        # self.sStbStbDdataDtsMtInsertMultiThreadCheckCase()
        # self.sStbDtbDdataDtsMtInsertMultiThreadCheckCase()
        # self.lengthIcreaseCrashCheckCase()

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
