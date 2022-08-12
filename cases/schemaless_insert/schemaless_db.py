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
import json
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
        dbname3 = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname=dbname3, precision="us", schemaless=0)
        self.tdSql.query('select * from information_schema.ins_databases')
        db_field_kv = self.tdSql.get_db_field_kv(0, dbname1)
        self.tdSql.checkEqual(db_field_kv["schemaless"], False)
        self.tdSql.query('select * from information_schema.ins_databases')
        db_field_kv = self.tdSql.get_db_field_kv(0, dbname2)
        self.tdSql.checkEqual(db_field_kv["schemaless"], True)
        self.tdSql.query('select * from information_schema.ins_databases')
        db_field_kv = self.tdSql.get_db_field_kv(0, dbname3)
        self.tdSql.checkEqual(db_field_kv["schemaless"], False)

    def alter_schema_db(self):
        dbname1 = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname=dbname1, precision="us")
        dbname2 = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname=dbname2, precision="us", schemaless=1)
        
        self.tdSql.execute(f'alter database {dbname1} schemaless 1')
        self.tdSql.execute(f'alter database {dbname2} schemaless 0')
        self.tdSql.query('select * from information_schema.ins_databases')
        db_field_kv = self.tdSql.get_db_field_kv(0, dbname1)
        self.tdSql.checkEqual(db_field_kv["schemaless"], True)
        self.tdSql.query('select * from information_schema.ins_databases')
        db_field_kv = self.tdSql.get_db_field_kv(0, dbname2)
        self.tdSql.checkEqual(db_field_kv["schemaless"], False)


    def sml_insert_schema_db(self):
        for i in range(2):
            dbname = self.tdCom.get_long_name(length=10, mode="letters")
            self.tdCom.createDb(dbname=dbname, precision="us")
            if i == 1:
                self.tdCom.createDb(dbname=dbname, precision="us", schemaless=0)
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
                self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{dbname}"')
                tb1 = self.tdSql.query_data[0][0]
                self.tdSql.error(f"create table {dbname}.stb (ts timestamp, c1 int) tags (t1 int)")
                self.tdSql.error(f"create table {dbname}.tb (ts timestamp, c1 int)")
                self.tdSql.error(f'create table {dbname}.tb1 using {dbname}.{stbname1} tags ("ajvkso_11235_24175", true, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue")')
                self.tdSql.error(f'insert into {dbname}.{tb1} values (now, false, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue", 7)')
                self.tdSql.error(f'alter table {dbname}.{tb1} set tag t1 = "1";')
                self.tdSql.error(f'alter table {dbname}.{stbname1} drop tag t1;')
                self.tdSql.error(f'alter table {dbname}.{stbname1} add tag t21 tinyint;')
                self.tdSql.error(f'alter table {dbname}.{stbname1} drop column c1;')
                self.tdSql.error(f'alter table {dbname}.{stbname1} add column c21 tinyint;')
                self.tdSql.error(f'alter table {dbname}.{stbname1} rename column c21 c22;')
                self.tdSql.execute(f'drop table {stbname1}')
            elif sml_type == "opentsdb_telnet":
                input_sql, stbname2 = self.tdCom.gen_full_type_sql()
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{dbname}"')
                tb2 = self.tdSql.query_data[0][0]
                self.tdSql.error(f"create table {dbname}.stb (ts timestamp, c1 int) tags (t1 int)")
                self.tdSql.error(f"create table {dbname}.tb (ts timestamp, c1 int)")
                self.tdSql.error(f'create table {dbname}.tb2 using {dbname}.{stbname2} tags ("bbgtol_22702_30690", true, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue")')
                self.tdSql.error(f'insert into {dbname}.{tb2} values (now, false)')
                self.tdSql.error(f'alter table {dbname}.{tb2} set tag t1 = 1;')
                self.tdSql.error(f'alter table {dbname}.{stbname2} drop tag t1;')
                self.tdSql.error(f'alter table {dbname}.{stbname2} add tag t21 tinyint;')
                self.tdSql.error(f'alter table {dbname}.{stbname2} drop column _value;')
                self.tdSql.error(f'alter table {dbname}.{stbname2} add column c21 tinyint;')
                self.tdSql.error(f'alter table {dbname}.{stbname2} rename column c21 c22;')
                self.tdSql.execute(f'drop table {stbname2}')
            elif sml_type == "opentsdb_json":
                input_json, stbname3 = self.tdCom.gen_full_type_json(value_type="default")
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{dbname}"')
                tb3 = self.tdSql.query_data[0][0]
                self.tdSql.error(f"create table {dbname}.stb (ts timestamp, c1 int) tags (t1 int)")
                self.tdSql.error(f"create table {dbname}.tb (ts timestamp, c1 int)")
                self.tdSql.error(f'create table {dbname}.tb3 using {dbname}.{stbname3} tags (true, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue")')
                self.tdSql.error(f'insert into {dbname}.{tb3} values (now, false)')
                self.tdSql.error(f'alter table {dbname}.{tb3} set tag t1 = 1;')
                self.tdSql.error(f'alter table {dbname}.{stbname3} drop tag t1;')
                self.tdSql.error(f'alter table {dbname}.{stbname3} add tag t21 tinyint;')
                self.tdSql.error(f'alter table {dbname}.{stbname3} drop column _value;')
                self.tdSql.error(f'alter table {dbname}.{stbname3} add column c21 tinyint;')
                self.tdSql.error(f'alter table {dbname}.{stbname3} rename column c21 c22;')
                self.tdSql.execute(f'drop table {stbname3}')
    
    def sql_insert_schemaless_db_with_shell(self):
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname=dbname, precision="us", schemaless=1)
        for sml_type in ["influxdb", "opentsdb_telnet", "opentsdb_json"]:
            self.tdCom.sml_type = sml_type
            if sml_type == "influxdb":
                input_sql, stbname1 = self.tdCom.gen_full_type_sql()
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{dbname}"')
                tb1 = self.tdSql.query_data[0][0]
                for sql in [f"create table {dbname}.stb (ts timestamp, c1 int) tags (t1 int)",
                            f"create table {dbname}.tb (ts timestamp, c1 int)",
                            f'create table {dbname}.tb1 using {dbname}.{stbname1} tags ("ajvkso_11235_24175", true, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue")',
                            f'insert into {dbname}.{tb1} values (now, false, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue", 7)',
                            f'alter table {dbname}.{tb1} set tag t1 = 1;',
                            f'alter table {dbname}.{stbname1} drop tag t1;',
                            f'alter table {dbname}.{stbname1} add tag t21 tinyint;',
                            f'alter table {dbname}.{stbname1} drop column c1;',
                            f'alter table {dbname}.{stbname1} add column c21 tinyint;',
                            f'alter table {dbname}.{stbname1} rename column c21 c22;']:
                    self._remote.cmd(self.taosd_settings["fqdn"][0], [f'taos -s "{sql}"'])
                for sql in [f'select * from information_schema.ins_stables where db_name =  "{dbname}"', f'select * from information_schema.ins_tables where db_name =  "{dbname}"', f'select * from {dbname}.{tb1}']:
                    self.tdSql.query(sql)
                    self.tdSql.checkEqual(len(self.tdSql.query_data), 1)
                self.tdSql.execute(f'drop table {dbname}.{tb1}')
                self.tdSql.execute(f'drop table {dbname}.{stbname1}')
            elif sml_type == "opentsdb_telnet":
                input_sql, stbname2 = self.tdCom.gen_full_type_sql()
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{dbname}"')
                tb2 = self.tdSql.query_data[0][0]
                for sql in [f"create table {dbname}.stb (ts timestamp, c1 int) tags (t1 int)",
                            f"create table {dbname}.tb (ts timestamp, c1 int)",
                            f'create table {dbname}.tb2 using {dbname}.{stbname2} tags ("bbgtol_22702_30690", true, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue")',
                            f'insert into {dbname}.{tb2} values (now, false)',
                            f'alter table {dbname}.{tb2} set tag t1 = 1;',
                            f'alter table {dbname}.{stbname2} drop tag t1;',
                            f'alter table {dbname}.{stbname2} add tag t21 tinyint;',
                            f'alter table {dbname}.{stbname2} drop column _value;',
                            f'alter table {dbname}.{stbname2} add column c21 tinyint;',
                            f'alter table {dbname}.{stbname2} rename column c21 c22;']:
                    self._remote.cmd(self.taosd_settings["fqdn"][0], [f'taos -s "{sql}"'])
                for sql in [f'select * from information_schema.ins_stables where db_name =  "{dbname}"', f'select * from information_schema.ins_tables where db_name =  "{dbname}"', f'select * from {dbname}.{tb2}']:
                    self.tdSql.query(sql)
                    self.tdSql.checkEqual(len(self.tdSql.query_data), 1)
                self.tdSql.execute(f'drop table {dbname}.{tb2}')
                self.tdSql.execute(f'drop table {stbname2}')
            elif sml_type == "opentsdb_json":
                input_json, stbname3 = self.tdCom.gen_full_type_json(value_type="default")
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{dbname}"')
                tb3 = self.tdSql.query_data[0][0]
                for sql in [f"create table {dbname}.stb (ts timestamp, c1 int) tags (t1 int)",
                            f"create table {dbname}.tb (ts timestamp, c1 int)",
                            f'create table {dbname}.tb3 using {dbname}.{stbname3} tags (true, 127, 32767, 2147483647, 9223372036854775807, 11.12345, 22.123456789, "binaryTagValue", "ncharTagValue")',
                            f'insert into {dbname}.{tb3} values (now, false)',
                            f'alter table {dbname}.{tb3} set tag t1 = 1;',
                            f'alter table {dbname}.{stbname3} drop tag t1;',
                            f'alter table {dbname}.{stbname3} add tag t21 tinyint;',
                            f'alter table {dbname}.{stbname3} drop column _value;',
                            f'alter table {dbname}.{stbname3} add column c21 tinyint;',
                            f'alter table {dbname}.{stbname3} rename column c21 c22;']:
                    self._remote.cmd(self.taosd_settings["fqdn"][0], [f'taos -s "{sql}"'])
                for sql in [f'select * from information_schema.ins_stables where db_name =  "{dbname}"', f'select * from information_schema.ins_tables where db_name =  "{dbname}"', f'select * from {dbname}.{tb3}']:
                    self.tdSql.query(sql)
                    self.tdSql.checkEqual(len(self.tdSql.query_data), 1)
                self.tdSql.execute(f'drop table {dbname}.{tb3}')
                self.tdSql.execute(f'drop table {stbname3}')

    
    def run(self) -> bool:
        self.show_schema_db()
        # ! unfinished
        # self.alter_schema_db()
        self.sml_insert_schema_db()
        # ! TD-16397
        # self.sql_insert_schemaless_db_with_python()
        self.sql_insert_schemaless_db_with_shell()
    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
            show_schema_db(): show databases;
            alter_schema_db(): alter schemaless param;
            sml_insert_schema_db(): sml could not insert schema_db;
            sql_insert_schemaless_db_with_python(): sql could not insert schemaless_db with python_connector;
            sql_insert_schemaless_db_with_shell(): sql could not insert schemaless_db with taos-shell;
        '''
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.Schemaless.Taosc.DB
