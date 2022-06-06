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
from taostest.util.sml_types import TDSmlProtocolType, TDSmlTimestampType
from taos.error import SchemalessError
import datetime
import time
import json
import sys
from taostest.util.remote import Remote

class TestSchemalessDB(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdCom.drop_all_db()
        self._remote: Remote = Remote(self.logger)
        self.taosd_settings = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")

    def show_schema_db(self):
        dbname1 = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname=dbname1, precision="us")
        dbname2 = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname=dbname2, precision="us", schemaless=1)
        self.tdSql.query('show databases')
        db_field_kv = self.tdSql.get_db_field_kv(0, dbname1)
        self.tdSql.checkEqual(db_field_kv["schemaless"], False)
        self.tdSql.query('show databases')
        db_field_kv = self.tdSql.get_db_field_kv(0, dbname2)
        self.tdSql.checkEqual(db_field_kv["schemaless"], True)

    def sml_insert_schema_db(self):
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname=dbname, precision="us")
        
        for sml_type in ["influxdb", "opentsdb_telnet", "opentsdb_json"]:
            self.tdCom.sml_type = sml_type
            try:
                if sml_type == "influxdb":
                    input_sql = self.tdCom.gen_full_type_sql()[0]
                    self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.MICRO_SECOND.value)
                elif sml_type == "opentsdb_telnet":
                    input_sql = self.tdCom.gen_full_type_sql()[0]
                    self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                elif sml_type == "opentsdb_json":
                    input_json = self.tdCom.gen_full_type_json(value_type="default")[0]
                    self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

    def sql_insert_schemaless_db_with_python(self):
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname=dbname, precision="us", schemaless=1)
        for sml_type in ["influxdb", "opentsdb_telnet", "opentsdb_json"]:
            self.tdCom.sml_type = sml_type
            if sml_type == "influxdb":
                input_sql, stbname1 = self.tdCom.gen_full_type_sql()
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                self.tdSql.query(f'show {dbname}.tables')
                tb1 = self.tdSql.query_data[0][0]
                self.tdSql.execute(f"create table {dbname}.stb (ts timestamp, c1 int) tags (t1 int)")
                self.tdSql.error(f"create table {dbname}.tb (ts timestamp, c1 int)")
                self.tdSql.error(f'create table {dbname}.tb1 using {dbname}.{stbname1} tags ("ajvkso_11235_24175", true, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue")')
                self.tdSql.error(f'insert into {dbname}.{tb1} values (now, false, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue", 7)')
                self.tdSql.execute(f'drop table {stbname1}')
            elif sml_type == "opentsdb_telnet":
                input_sql, stbname2 = self.tdCom.gen_full_type_sql()
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                self.tdSql.query(f'show {dbname}.tables')
                tb2 = self.tdSql.query_data[0][0]
                self.tdSql.error(f"create table {dbname}.stb (ts timestamp, c1 int) tags (t1 int)")
                self.tdSql.error(f"create table {dbname}.tb (ts timestamp, c1 int)")
                self.tdSql.error(f'create table {dbname}.tb2 using {dbname}.{stbname2} tags ("bbgtol_22702_30690", true, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue")')
                self.tdSql.error(f'insert into {dbname}.{tb2} values (now, false)')
                self.tdSql.execute(f'drop table {stbname2}')
            elif sml_type == "opentsdb_json":
                input_json, stbname3 = self.tdCom.gen_full_type_json(value_type="default")
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                self.tdSql.query(f'show {dbname}.tables')
                tb3 = self.tdSql.query_data[0][0]
                self.tdSql.error(f"create table {dbname}.stb (ts timestamp, c1 int) tags (t1 int)")
                self.tdSql.error(f"create table {dbname}.tb (ts timestamp, c1 int)")
                self.tdSql.error(f'create table {dbname}.tb3 using {dbname}.{stbname3} tags (true, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue")')
                self.tdSql.error(f'insert into {dbname}.{tb3} values (now, false)')
                self.tdSql.execute(f'drop table {stbname3}')
    
    def sql_insert_schemaless_db_with_shell(self):
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname=dbname, precision="us", schemaless=1)
        for sml_type in ["influxdb", "opentsdb_telnet", "opentsdb_json"]:
            self.tdCom.sml_type = sml_type
            if sml_type == "influxdb":
                input_sql, stbname1 = self.tdCom.gen_full_type_sql()
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                self.tdSql.query(f'show {dbname}.tables')
                tb1 = self.tdSql.query_data[0][0]
                for sql in [f"create table {dbname}.stb (ts timestamp, c1 int) tags (t1 int)",
                            f"create table {dbname}.tb (ts timestamp, c1 int)",
                            f'create table {dbname}.tb1 using {dbname}.{stbname1} tags ("ajvkso_11235_24175", true, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue")',
                            f'insert into {dbname}.{tb1} values (now, false, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue", 7)']:
                    self._remote.cmd(self.taosd_settings["fqdn"][0], [f'taos -s "{sql}"'])
                for sql in [f'show {dbname}.stables', f'show {dbname}.tables', f'select * from {dbname}.{tb1}']:
                    self.tdSql.query(sql)
                    self.tdSql.checkEqual(len(self.tdSql.query_data), 1)
                self.tdSql.execute(f'drop table {dbname}.{tb1}')
                self.tdSql.execute(f'drop table {dbname}.{stbname1}')
            elif sml_type == "opentsdb_telnet":
                input_sql, stbname2 = self.tdCom.gen_full_type_sql()
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                self.tdSql.query(f'show {dbname}.tables')
                tb2 = self.tdSql.query_data[0][0]
                for sql in [f"create table {dbname}.stb (ts timestamp, c1 int) tags (t1 int)",
                            f"create table {dbname}.tb (ts timestamp, c1 int)",
                            f'create table {dbname}.tb2 using {dbname}.{stbname2} tags ("bbgtol_22702_30690", true, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue")',
                            f'insert into {dbname}.{tb2} values (now, false)']:
                    self._remote.cmd(self.taosd_settings["fqdn"][0], [f'taos -s "{sql}"'])
                for sql in [f'show {dbname}.stables', f'show {dbname}.tables', f'select * from {dbname}.{tb2}']:
                    self.tdSql.query(sql)
                    self.tdSql.checkEqual(len(self.tdSql.query_data), 1)
                self.tdSql.execute(f'drop table {dbname}.{tb2}')
                self.tdSql.execute(f'drop table {stbname2}')
            elif sml_type == "opentsdb_json":
                input_json, stbname3 = self.tdCom.gen_full_type_json(value_type="default")
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                self.tdSql.query(f'show {dbname}.tables')
                tb3 = self.tdSql.query_data[0][0]
                for sql in [f"create table {dbname}.stb (ts timestamp, c1 int) tags (t1 int)",
                            f"create table {dbname}.tb (ts timestamp, c1 int)",
                            f'create table {dbname}.tb3 using {dbname}.{stbname3} tags (true, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue")',
                            f'insert into {dbname}.{tb3} values (now, false)']:
                    self._remote.cmd(self.taosd_settings["fqdn"][0], [f'taos -s "{sql}"'])
                for sql in [f'show {dbname}.stables', f'show {dbname}.tables', f'select * from {dbname}.{tb3}']:
                    self.tdSql.query(sql)
                    self.tdSql.checkEqual(len(self.tdSql.query_data), 1)
                self.tdSql.execute(f'drop table {dbname}.{tb3}')
                self.tdSql.execute(f'drop table {stbname3}')

    
    def run(self) -> bool:
        self.show_schema_db()
        # self.sml_insert_schema_db()
        # self.sql_insert_schemaless_db_with_python()
        self.sql_insert_schemaless_db_with_shell()
    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
        init_check()
        bool_check()
        symbols_check()
        ts_check()
        id_seq_check()
        id_letter_check()
        no_id_check()
        max_col_tag_check()
        stb_tb_name_check()
        id_start_with_num_check()
        now_check()
        date_format_check()
        illegal_ts_check()
        tbname_check()
        tag_value_length_check()
        col_value_length_check()
        tag_col_illegal_value_check()
        duplicate_id_tag_col_insert_check()
        no_id_stb_exist_check()
        duplicate_insert_exist_check()
        tag_col_binary_nchar_length_check()
        tag_col_add_dup_id_check()
        tag_col_add_check()
        tag_md5_check()
        tag_col_binary_max_length_check()
        batch_insert_check()
        multi_insert_check(10)
        batch_error_insert_check()
        multi_cols_insert_check()
        multi_tags_insert_check()
        blank_col_insert_check()
        blank_tag_insert_check()
        chinese_check()
        spell_check()
        default_type_check()
        tbname_tags_cols_name_check()
        stb_insert_multi_thread_check()
        s_stb_s_tb_d_data_insert_multi_thread_check()
        s_stb_s_tb_d_data_atc_insert_multi_thread_check()
        s_stb_stb_d_data_mtc_insert_multi_thread_check()
        s_stb_d_tb_d_data_insert_multi_thread_check()
        s_stb_d_tb_d_data_ac_mt_insert_multi_thread_check()
        s_stb_d_tb_d_data_at_mc_insert_multi_thread_check()
        s_stb_s_tb_d_data_d_ts_insert_multi_thread_check()
        s_stb_s_tb_d_data_d_ts_ac_mt_insert_multi_thread_check()
        s_stb_s_tb_d_data_d_ts_at_mc_insert_multi_thread_check()
        s_stb_d_tb_d_data_d_ts_insert_multi_thread_check()
        s_stb_d_tb_d_data_d_ts_ac_mt_insert_multi_thread_check()
        '''
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.Schemaless.Taosc.InfluxDB
