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
class TestInfluxdbLineTaoscInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdCom.sml_type = "influxdb"
        self.tdCom.drop_all_db()
        self.dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname=self.dbname, precision="us")

    def init_check(self):
        """
        normal tags and cols, one for every elm
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)

    def bool_check(self):
        """
        check all normal bool type
        """
        self.tdCom.cleanTb()
        full_type_list = ["f", "F", "false", "False", "t", "T", "true", "True"]
        for t_type in full_type_list:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c0=t_type, t0=t_type)
            self.tdCom.check_res(input_sql, stb_name)

    def symbols_check(self):
        """
        check symbols = \"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"\"
        """
        self.tdCom.cleanTb()
        binary_symbols = '"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"'
        nchar_symbols = f'L{binary_symbols}'
        input_sql, stb_name = self.tdCom.gen_full_type_sql(c7=binary_symbols, c8=nchar_symbols, t7=binary_symbols, t8=nchar_symbols)
        print(input_sql)
        self.tdCom.check_res(input_sql, stb_name)

    def ts_check(self):
        """
        test ts list --> ["1626006833639000000", "1626006833639019us", "1626006833640ms", "1626006834s", "1626006822639022"]
        # ! us级时间戳都为0时，数据库中查询显示，但python接口拿到的结果不显示 .000000的情况请确认，目前修改时间处理代码可以通过
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(ts=1626006833639000000)
        self.tdCom.check_res(input_sql, stb_name, ts_type=TDSmlTimestampType.NANO_SECOND.value)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(ts=1626006833639019)
        self.tdCom.check_res(input_sql, stb_name, ts_type=TDSmlTimestampType.MICRO_SECOND.value)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(ts=1626006833640)
        self.tdCom.check_res(input_sql, stb_name, ts_type=TDSmlTimestampType.MILLI_SECOND.value)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(ts=1626006834)
        self.tdCom.check_res(input_sql, stb_name, ts_type=TDSmlTimestampType.SECOND.value)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(ts=1626006833639000000)
        self.tdCom.check_res(input_sql, stb_name, ts_type=None)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(ts=0)
        self.tdCom.check_res(input_sql, stb_name, ts=0)

        self.tdSql.execute(f"drop database if exists test_ts")
        self.tdSql.execute(f"create database if not exists test_ts precision 'ms'")
        self.tdSql.execute("use test_ts")
        input_sql = ['test_ms,t0=t c0=t 1626006833640', 'test_ms,t0=t c0=f 1626006833641']
        self.tdSql._conn.schemaless_insert(input_sql, TDSmlProtocolType.LINE.value, TDSmlTimestampType.MILLI_SECOND.value)
        self.tdSql.query('select * from test_ms')
        self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), "2021-07-11 20:33:53.640000")
        self.tdSql.checkEqual(str(self.tdSql.query_data[1][0]), "2021-07-11 20:33:53.641000")

        self.tdSql.execute(f"drop database if exists test_ts")
        self.tdSql.execute(f"create database if not exists test_ts precision 'us'")
        self.tdSql.execute("use test_ts")
        input_sql = ['test_us,t0=t c0=t 1626006833639000', 'test_us,t0=t c0=f 1626006833639001']
        self.tdSql._conn.schemaless_insert(input_sql, TDSmlProtocolType.LINE.value, TDSmlTimestampType.MICRO_SECOND.value)
        self.tdSql.query('select * from test_us')
        self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), "2021-07-11 20:33:53.639000")
        self.tdSql.checkEqual(str(self.tdSql.query_data[1][0]), "2021-07-11 20:33:53.639001")

        self.tdSql.execute(f"drop database if exists test_ts")
        self.tdSql.execute(f"create database if not exists test_ts precision 'ns'")
        self.tdSql.execute("use test_ts")
        input_sql = ['test_ns,t0=t c0=t 1626006833639000000', 'test_ns,t0=t c0=f 1626006833639000001']
        self.tdSql._conn.schemaless_insert(input_sql, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.tdSql.query('select * from test_ns')
        self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), "1626006833639000000")
        self.tdSql.checkEqual(str(self.tdSql.query_data[1][0]), "1626006833639000001")

        self.tdCom.createDb()

    def id_seq_check(self):
        """
        check id.index in tags
        eg: t0=**,id=**,t1=**
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(id_change_tag=True)
        self.tdCom.check_res(input_sql, stb_name)

    def id_letter_check(self):
        """
        check id param
        eg: id and ID
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(id_upper_tag=True)
        self.tdCom.check_res(input_sql, stb_name)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(id_mixul_tag=True)
        self.tdCom.check_res(input_sql, stb_name)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(id_change_tag=True, id_upper_tag=True)
        self.tdCom.check_res(input_sql, stb_name)

    def no_id_check(self):
        """
        id not exist
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(id_noexist_tag=True)
        self.tdCom.check_res(input_sql, stb_name)
        query_sql = f"select tbname from {stb_name}"
        res_row_list = self.tdCom.res_handle(query_sql, stb_name)[0]
        if len(res_row_list[0][0]) > 0:
            self.tdSql.checkEqual(res_row_list, res_row_list)
        else:
            self.tdSql.checkEqual(res_row_list, "please check noId_check")

    def max_col_tag_check(self):
        """
        max tag count is 128
        max col count is 4096
        """
        for input_sql in [self.tdCom.gen_long_sql(127, 1)[0], self.tdCom.gen_long_sql(1, 4093)[0]]:
            self.tdCom.cleanTb()
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        for input_sql in [self.tdCom.gen_long_sql(128, 1)[0], self.tdCom.gen_long_sql(1, 4094)[0]]:
            self.tdCom.cleanTb()
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

    def stb_tb_name_check(self):
        """
        test illegal id name
        mix "~!@#$¥%^&*()-+{}|[]、「」【】:;《》<>?"
        """
        self.tdCom.cleanTb()
        rstr = list("~!@#$¥%^&*()-+|[]、「」【】;:《》<>?")
        for i in rstr:
            stb_name=f"aaa{i}bbb"
            input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, tb_name=f'{stb_name}_sub')
            self.tdCom.check_res(input_sql, f'`{stb_name}`')
            self.tdSql.execute(f'drop table if exists `{stb_name}`')

    def id_start_with_num_check(self):
        """
        id is start with num
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name="1aaabbb")
        self.tdCom.check_res(input_sql, stb_name)

    def now_check(self):
        """
        check now unsupported
        """
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(ts="now")[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def date_format_check(self):
        """
        check date format unsupported
        """
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(ts="2021-07-21\ 19:01:46.920")[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def illegal_ts_check(self):
        """
            check ts format like 16260068336390us19
        """
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(ts="16260068336390us19")[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def tbname_check(self):
        """
        check length 192
        check upper tbname
        chech upper tag
        length of stb_name tb_name <= 192
        """
        self.tdCom.cleanTb()
        stb_name_192 = self.tdCom.get_long_name(length=192, mode="letters")
        tb_name_192 = self.tdCom.get_long_name(length=192, mode="letters")
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name_192, tb_name=tb_name_192)
        self.tdCom.check_res(input_sql, stb_name)
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        for i in [self.tdCom.gen_full_type_sql(stb_name=self.tdCom.get_long_name(length=193, mode="letters"), tb_name=self.tdCom.get_long_name(length=5, mode="letters")), self.tdCom.gen_full_type_sql(tb_name=self.tdCom.get_long_name(length=193, mode="letters"))]:
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        input_sql = 'Abcdffgg,id=Abcddd,T1=127i8 c0=False 1626006833639000000'
        stb_name = "Abcdffgg"
        self.tdCom.check_res(input_sql, stb_name)

    def tag_value_length_check(self):
        """
        check full type tag value limit
        """
        self.tdCom.cleanTb()
        # nchar
        # * legal nchar could not be larger than 16374/4
        stb_name = self.tdCom.get_long_name(7, "letters")
        input_sql = f'{stb_name},t0=t,t1={self.tdCom.get_long_name(4093, "letters")} c0=f 1626006833639000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        input_sql = f'{stb_name},t0=t,t1={self.tdCom.get_long_name(4094, "letters")} c0=f 1626006833639000000'
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def col_value_length_check(self):
        """
        check full type col value limit
        """
        self.tdCom.cleanTb()
        # i8
        for c1 in ["-127i8", "127i8"]:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c1=c1)
            self.tdCom.check_res(input_sql, stb_name)

        for c1 in ["-128i8", "128i8"]:
            input_sql = self.tdCom.gen_full_type_sql(c1=c1)[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)
        # i16
        for c2 in ["-32767i16"]:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c2=c2)
            self.tdCom.check_res(input_sql, stb_name)
        for c2 in ["-32768i16", "32768i16"]:
            input_sql = self.tdCom.gen_full_type_sql(c2=c2)[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # i32
        for c3 in ["-2147483647i32"]:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c3=c3)
            self.tdCom.check_res(input_sql, stb_name)
        for c3 in ["-2147483648i32", "2147483648i32"]:
            input_sql = self.tdCom.gen_full_type_sql(c3=c3)[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # i64
        for c4 in ["-9223372036854775807i64"]:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c4=c4)
            self.tdCom.check_res(input_sql, stb_name)
        for c4 in ["-9223372036854775808i64", "9223372036854775808i64"]:
            input_sql = self.tdCom.gen_full_type_sql(c4=c4)[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # f32
        for c5 in [f"{-3.4028234663852885981170418348451692544*(10**38)}f32", f"{3.4028234663852885981170418348451692544*(10**38)}f32"]:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c5=c5)
            self.tdCom.check_res(input_sql, stb_name)
        # * limit set to 4028234664*(10**38)
        for c5 in [f"{-3.4028234664*(10**38)}f32", f"{3.4028234664*(10**38)}f32"]:
            input_sql = self.tdCom.gen_full_type_sql(c5=c5)[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # f64
        for c6 in [f'{-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)}f64', f'{-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)}f64']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c6=c6)
            self.tdCom.check_res(input_sql, stb_name)
        # * limit set to 1.797693134862316*(10**308)
        for c6 in [f'{-1.797693134862316*(10**308)}f64', f'{-1.797693134862316*(10**308)}f64']:
            input_sql = self.tdCom.gen_full_type_sql(c6=c6)[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # # binary
        stb_name = self.tdCom.get_long_name(7, "letters")
        input_sql = f'{stb_name},t0=t c0=f,c1="{self.tdCom.get_long_name(16374, "letters")}" 1626006833639000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        input_sql = f'{stb_name},t0=t c0=f,c1="{self.tdCom.get_long_name(16375, "letters")}" 1626006833639000000'
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

        # nchar
        # * legal nchar could not be larger than 16374/4
        stb_name = self.tdCom.get_long_name(7, "letters")
        input_sql = f'{stb_name},t0=t c0=f,c1=L"{self.tdCom.get_long_name(4093, "letters")}" 1626006833639000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        input_sql = f'{stb_name},t0=t c0=f,c1=L"{self.tdCom.get_long_name(4094, "letters")}" 1626006833639000000'
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def tag_col_illegal_value_check(self):
        """
        test illegal tag col value
        """
        self.tdCom.cleanTb()
        # bool
        for i in ["TrUe", "tRue", "trUe", "truE", "FalsE", "fAlse", "faLse", "falSe", "falsE"]:
            for input_sql, stb_name in [self.tdCom.gen_full_type_sql(t0=i), self.tdCom.gen_full_type_sql(c0=i)]:
                self.tdCom.check_res(input_sql, stb_name)
        # i8 i16 i32 i64 f32 f64
        for input_sql in [
                self.tdCom.gen_full_type_sql(c1="1s2i8")[0],
                self.tdCom.gen_full_type_sql(c2="1s2i16")[0],
                self.tdCom.gen_full_type_sql(c3="1s2i32")[0],
                self.tdCom.gen_full_type_sql(c4="1s2i64")[0],
                self.tdCom.gen_full_type_sql(c5="11.1s45f32")[0],
                self.tdCom.gen_full_type_sql(c6="11.1s45f64")[0],
                self.tdCom.gen_full_type_sql(c9="1s1u64")[0]
            ]:
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # check binary and nchar blank
        stb_name = self.tdCom.get_long_name(7, "letters")
        input_sql1 = f'{stb_name}_1,t0=t c0=f,c1="abc aaa" 1626006833639000000'
        input_sql2 = f'{stb_name}_2,t0=t c0=f,c1=L"abc aaa" 1626006833639000000'
        input_sql3 = f'{stb_name}_3,t0=t,t1="abc\ aaa" c0=f 1626006833639000000'
        input_sql4 = f'{stb_name}_4,t0=t,t1=L"abc\ aaa" c0=f 1626006833639000000'
        for input_sql in [input_sql1, input_sql2, input_sql3, input_sql4]:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        # check accepted binary and nchar symbols
        # # * ~!@#$¥%^&*()-+{}|[]、「」:;
        for symbol in list('~!@#$¥%^&*()-+{}|[]、「」:;'):
            print("!!!---", symbol)
            input_sql1 = f'{stb_name},t0=t c0=f,c1="abc{symbol}aaa" 1626006833639000000'
            input_sql2 = f'{stb_name},t0=t,t1="abc{symbol}aaa" c0=f 1626006833639000000'
            self.tdSql._conn.schemaless_insert([input_sql1], TDSmlProtocolType.LINE.value, None)
            self.tdSql._conn.schemaless_insert([input_sql2], TDSmlProtocolType.LINE.value, None)

    def duplicate_id_tag_col_insert_check(self):
        """
        check duplicate Id Tag Col
        """
        self.tdCom.cleanTb()
        input_sql_id = self.tdCom.gen_full_type_sql(id_double_tag=True)[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql_id], TDSmlProtocolType.LINE.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

        input_sql = self.tdCom.gen_full_type_sql()[0]
        input_sql_tag = input_sql.replace("t5", "t6")
        try:
            self.tdSql._conn.schemaless_insert([input_sql_tag], TDSmlProtocolType.LINE.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

        input_sql = self.tdCom.gen_full_type_sql()[0]
        input_sql_col = input_sql.replace("c5", "c6")
        try:
            self.tdSql._conn.schemaless_insert([input_sql_col], TDSmlProtocolType.LINE.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

        input_sql = self.tdCom.gen_full_type_sql()[0]
        input_sql_col = input_sql.replace("c5", "C6")
        try:
            self.tdSql._conn.schemaless_insert([input_sql_col], TDSmlProtocolType.LINE.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    ##### stb exist #####
    def no_id_stb_exist_check(self):
        """
        case no id when stb exist
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name="sub_table_0123456", t0="f", c0="f")
        self.tdCom.check_res(input_sql, stb_name)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, id_noexist_tag=True, t0="f", c0="f")
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, None)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        self.tdSql.query(f"select * from {stb_name} where id is Null")
        self.tdSql.checkEqual(self.tdSql.query_row, 1)

    def duplicate_insert_exist_check(self):
        """
        check duplicate insert when stb exist
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.tdCom.check_res(input_sql, stb_name)

    def tag_col_binary_nchar_length_check(self):
        """
        check length increase
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        tb_name = self.tdCom.get_long_name(5, "letters")
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name, t7="\"binaryTagValuebinaryTagValue\"", t8="L\"ncharTagValuencharTagValue\"", c7="\"binaryTagValuebinaryTagValue\"", c8="L\"ncharTagValuencharTagValue\"")
        self.tdCom.check_res(input_sql, stb_name, condition=f'where t7 = \'"binaryTagValuebinaryTagValue"\'')

    def tag_col_add_dup_id_check(self):
        """
        check column and tag count add, stb and tb duplicate
        * tag: alter table ...
        * col: when update==0 and ts is same, unchange
        * so this case tag&&value will be added,
        * col is added without value when update==0
        * col is added with value when update==1
        """
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name(7, "letters")
        for db_update_tag in [0, 1]:
            if db_update_tag == 1 :
                self.tdCom.createDb("test_update", db_update_tag=db_update_tag)
            input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name, t0="t", c0="t")
            self.tdCom.check_res(input_sql, stb_name)
            input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name, t0="t", c0="f", ct_add_tag=True)
            if db_update_tag == 1 :
                self.tdCom.check_res(input_sql, stb_name, condition=f'where tbname like "{tb_name}"', none_check_tag=True)
                self.tdSql.query(f'select * from {stb_name} where tbname like "{tb_name}"')
                self.tdSql.checkData(0, 11, "ncharColValue")
                self.tdSql.checkData(0, 12, True)
                self.tdSql.checkData(0, 22, None)
                self.tdSql.checkData(0, 23, None)
            else:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                self.tdSql.query(f'select * from {stb_name} where tbname like "{tb_name}"')
                self.tdSql.checkData(0, 1, True)
                self.tdSql.checkData(0, 11, None)
                self.tdSql.checkData(0, 12, None)
                self.tdSql.checkData(0, 22, None)
                self.tdSql.checkData(0, 23, None)
            self.tdCom.createDb()

    def tag_col_add_check(self):
        """
        check column and tag count add
        """
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name(7, "letters")
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name, t0="f", c0="f")
        self.tdCom.check_res(input_sql, stb_name)
        tb_name_1 = self.tdCom.get_long_name(7, "letters")
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name_1, t0="f", c0="f", ct_add_tag=True)
        self.tdCom.check_res(input_sql, stb_name, condition=f'where id = "{tb_name_1}"')
        res_row_list = self.tdCom.res_handle(f'select c10,c11,t10,t11 from {stb_name} where id = "{tb_name}"', stb_name)[0]
        self.tdSql.checkEqual(res_row_list[0], ['None', 'None', 'None', 'None'])

    def tag_md5_check(self):
        """
        condition: stb not change
        insert two table, keep tag unchange, change col
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(t0="f", c0="f", id_noexist_tag=True)
        self.tdCom.check_res(input_sql, stb_name)
        tb_name1 = self.tdCom.get_no_id_tbname(stb_name)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, t0="f", c0="f", id_noexist_tag=True)
        self.tdCom.check_res(input_sql, stb_name)
        tb_name2 = self.tdCom.get_no_id_tbname(stb_name)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        self.tdSql.checkEqual(tb_name1, tb_name2)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, t0="f", c0="f", id_noexist_tag=True, ct_add_tag=True)
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        tb_name3 = self.tdCom.get_no_id_tbname(stb_name)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        self.tdSql.checkNotEqual(tb_name1, tb_name3)

    # * tag binary max is 16384, col+ts binary max  49151
    def tag_col_binary_max_length_check(self):
        """
        every binary and nchar must be length+2
        """
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name(7, "letters")
        tb_name = f'{stb_name}_1'
        input_sql = f'{stb_name},id={tb_name},t0=t c0=f 1626006833639000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        # # * check col，col+ts max in describe ---> 16143
        input_sql = f'{stb_name},t0=t c0=f,c1="{self.tdCom.get_long_name(16374, "letters")}",c2="{self.tdCom.get_long_name(16374, "letters")}",c3="{self.tdCom.get_long_name(16374, "letters")}",c4="{self.tdCom.get_long_name(12, "letters")}" 1626006833639000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        input_sql = f'{stb_name},t0=t c0=f,c1="{self.tdCom.get_long_name(16374, "letters")}",c2="{self.tdCom.get_long_name(16374, "letters")}",c3="{self.tdCom.get_long_name(16374, "letters")}",c4="{self.tdCom.get_long_name(13, "letters")}" 1626006833639000000'
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    # * tag nchar max is 16374/4, col+ts nchar max  49151
    def tag_col_nchar_max_length_check(self):
        """
        check nchar length limit
        """
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name(7, "letters")
        tb_name = f'{stb_name}_1'
        input_sql = f'{stb_name},id={tb_name},t2={self.tdCom.get_long_name(1, "letters")} c0=f 1626006833639000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        # * legal nchar could not be larger than 16374/4
        input_sql = f'{stb_name},t1={self.tdCom.get_long_name(4093-9, "letters")},t2={self.tdCom.get_long_name(1, "letters")} c0=f 1626006833639000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        input_sql = f'{stb_name},t1={self.tdCom.get_long_name(4092, "letters")},t2={self.tdCom.get_long_name(2, "letters")} c0=f 1626006833639000000'
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

        input_sql = f'{stb_name},t2={self.tdCom.get_long_name(1, "letters")} c0=f,c1=L"{self.tdCom.get_long_name(4093, "letters")}",c2=L"{self.tdCom.get_long_name(4093, "letters")}",c3=L"{self.tdCom.get_long_name(4093, "letters")}",c4=L"{self.tdCom.get_long_name(4, "letters")}" 1626006833639000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 3)
        input_sql = f'{stb_name},t2={self.tdCom.get_long_name(1, "letters")} c0=f,c1=L"{self.tdCom.get_long_name(4093, "letters")}",c2=L"{self.tdCom.get_long_name(4093, "letters")}",c3=L"{self.tdCom.get_long_name(4093, "letters")}",c4=L"{self.tdCom.get_long_name(5, "letters")}" 1626006833639000000'
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 3)

    def batch_insert_check(self):
        """
        test batch insert
        """
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name(8, "letters")
        self.tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 bigint)')
        lines = ["st123456,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "st123456,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000",
                f"{stb_name},t2=5f64,t3=L\"ste\" c1=true,c2=4i64,c3=\"iam\" 1626056811823316532",
                "stf567890,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000",
                "st123456,t1=4i64,t2=5f64,t3=\"t4\" c1=3i64,c3=L\"passitagain\",c2=true,c4=5f64 1626006933640001000",
                f"{stb_name},t2=5f64,t3=L\"ste2\" c3=\"iamszhou\",c4=false 1626056811843316532",
                f"{stb_name},t2=5f64,t3=L\"ste2\" c3=\"iamszhou\",c4=false,c5=32i8,c6=64i16,c7=32i32,c8=88.88f32 1626056812843316532",
                "st123456,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000",
                "st123456,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64 1626006933641000000"
                ]
        self.tdSql._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.tdSql.query('show stables')
        self.tdSql.checkEqual(self.tdSql.query_row, 3)
        self.tdSql.query('show tables')
        self.tdSql.checkEqual(self.tdSql.query_row, 6)
        self.tdSql.query('select * from st123456')
        self.tdSql.checkEqual(self.tdSql.query_row, 5)

    def multi_insert_check(self, count):
        """
        test multi insert
        """
        self.tdCom.cleanTb()
        sql_list = []
        stb_name = self.tdCom.get_long_name(8, "letters")
        self.tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 nchar(10))')
        for i in range(count):
            input_sql = self.tdCom.gen_full_type_sql(stb_name=stb_name, t7=f'"{self.tdCom.get_long_name(8, "letters")}"', c7=f'"{self.tdCom.get_long_name(8, "letters")}"', id_noexist_tag=True)[0]
            sql_list.append(input_sql)
        self.tdSql._conn.schemaless_insert(sql_list, TDSmlProtocolType.LINE.value, None)
        self.tdSql.query('show tables')
        self.tdSql.checkEqual(self.tdSql.query_row, count)

    def batch_error_insert_check(self):
        """
        test batch error insert
        """
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name(8, "letters")
        lines = ["st123456,t1=3i64,t2=4f64,t3=\"t3\" c1=3i 64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                f"{stb_name},t2=5f64,t3=L\"ste\" c1=tRue,c2=4i64,c3=\"iam\" 1626056811823316532ns"]
        try:
            self.tdSql._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def multi_cols_insert_check(self):
        """
        test multi cols insert
        """
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(c_multi_tag=True)[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def multi_tags_insert_check(self):
        """
        test multi tags insert
        """
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(t_multi_tag=True)[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def blank_col_insert_check(self):
        """
        test blank col insert
        """
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(c_blank_tag=True)[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def blank_tag_insert_check(self):
        """
        test blank tag insert
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(t_blank_tag=True)
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.tdSql.query(f'select * from {stb_name}')
        self.tdSql.checkEqual(self.tdSql.query_data[0][-1], None)

    def chinese_check(self):
        """
        check nchar ---> chinese
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(chinese_tag=True)
        self.tdCom.check_res(input_sql, stb_name)

    def spell_check(self):
        stb_name = self.tdCom.get_long_name(8, "letters")
        self.tdCom.cleanTb()
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
            self.tdCom.check_res(input_sql, stb_name)

    def default_type_check(self):
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name(8, "letters")
        input_sql_list = [f'{stb_name}_1,t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 1626006833639000000',
                            f'{stb_name}_2,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=22.123456789 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=22.123456789 1626006833639000000',
                            f'{stb_name}_3,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=10e5F32 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=10e5F64 1626006833639000000',
                            f'{stb_name}_4,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=10.0e5f64 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=10.0e5f32 1626006833639000000',
                            f'{stb_name}_5,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=-10.0e5 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=-10.0e5 1626006833639000000']
        for input_sql in input_sql_list:
            stb_name = input_sql.split(",")[0]
            self.tdCom.check_res(input_sql, stb_name)

    def tbname_tags_cols_name_check(self):
        self.tdCom.cleanTb()
        input_sql = 'rFa$sta,id=rFas$ta_1,Tt!0=true,tT@1=127i8,t#2=32767i16,\"t$3\"=2147483647i32,t%4=9223372036854775807i64,t^5=11.12345f32,t&6=22.123456789f64,t*7=\"ddzhiksj\",t!@#$%^&*()_+[];:<>?,9=L\"ncharTagValue\" C)0=True,c{1=127i8,c[2=32767i16,c;3=2147483647i32,c:4=9223372036854775807i64,c<5=11.12345f32,c>6=22.123456789f64,c?7=\"bnhwlgvj\",c.8=L\"ncharTagValue\",c!@#$%^&*()_+[];:<>?,=7u64 1626006933640000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        query_sql = 'select * from `rFa$sta`'
        self.tdSql.query(query_sql)
        self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2021, 7, 11, 20, 35, 33, 640000), True, 127, 32767, 2147483647, 9223372036854775807, 11.12345027923584, 22.123456789, 'bnhwlgvj', 'ncharTagValue', 7, '2147483647i32', 'rFas$ta_1', 'L"ncharTagValue"', '32767i16', '9223372036854775807i64', '22.123456789f64', '"ddzhiksj"', '11.12345f32', 'true', '127i8')])
        query_sql = 'describe `rFa$sta`'
        self.tdSql.query(query_sql)
        self.tdSql.checkEqual(self.tdSql.getColNameList(), ['ts', 'C)0', 'c{1', 'c[2', 'c;3', 'c:4', 'c<5', 'c>6', 'c?7', 'c.8', 'c!@#$%^&*()_+[];:<>?,', '"t$3"', 'id', 't!@#$%^&*()_+[];:<>?,9', 't#2', 't%4', 't&6', 't*7', 't^5', 'Tt!0', 'tT@1'])
        self.tdSql.execute('drop table `rFa$sta`')


    def stb_insert_multi_thread_check(self):
        """
        thread input different stb
        """
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_sql_list()[0]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(input_sql))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 5)

    def s_stb_s_tb_d_data_insert_multi_thread_check(self):
        """
        thread input same stb tb, different data, result keep first data
        """
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name(7, "letters")
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_list = self.tdCom.gen_sql_list(stb_name=stb_name, tb_name=tb_name)[1]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        self.tdSql.query(f"select * from {stb_name};")
        self.tdSql.checkEqual(self.tdSql.query_row, 1)

    def s_stb_s_tb_d_data_atc_insert_multi_thread_check(self):
        """
        thread input same stb tb, different data, add columes and tags,  result keep first data
        """
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name(7, "letters")
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_a_col_a_tag_list = self.tdCom.gen_sql_list(stb_name=stb_name, tb_name=tb_name)[2]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_a_col_a_tag_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        self.tdSql.query(f"select * from {stb_name};")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def s_stb_stb_d_data_mtc_insert_multi_thread_check(self):
        """
        thread input same stb tb, different data, minus columes and tags,  result keep first data
        """
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name(7, "letters")
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_m_col_m_tag_list = self.tdCom.gen_sql_list(stb_name=stb_name, tb_name=tb_name)[3]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_m_col_m_tag_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        self.tdSql.query(f"select * from {stb_name};")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def s_stb_d_tb_d_data_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, different data
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_d_tb_list = self.tdCom.gen_sql_list(stb_name=stb_name)[4]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_d_tb_d_data_ac_mt_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, different data, add col, mul tag
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        # s_stb_d_tb_a_col_m_tag_list = self.tdCom.gen_sql_list(stb_name=stb_name)[5]
        s_stb_d_tb_a_col_m_tag_list = [(f'{stb_name},t0=F,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="ngxgzdzs",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=F 1626006833639000000', 'hpxbys'), \
                                        (f'{stb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="vvfrdtty",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=True 1626006833639000000', 'hpxbys'), \
                                        (f'{stb_name},t0=F,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="kzscucnt",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=f 1626006833639000000', 'hpxbys'), \
                                        (f'{stb_name},t0=F,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="asegdbqk",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=false 1626006833639000000', 'hpxbys'), \
                                        (f'{stb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="yvqnhgmn",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=T 1626006833639000000', 'hpxbys')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_a_col_m_tag_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 3)

    def s_stb_d_tb_d_data_at_mc_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, different data, add tag, mul col
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_d_tb_a_tag_m_col_list = self.tdCom.gen_sql_list(stb_name=stb_name)[6]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_a_tag_m_col_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_s_tb_d_data_d_ts_insert_multi_thread_check(self):
        """
        thread input same stb tb, different ts
        """
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name(7, "letters")
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_d_ts_list = [(f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="tgqkvsws",t8=L"ncharTagValue" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="htvnnldm",c8=L"ncharColValue",c9=7u64 1626006833639000000', 'sfzqdz'), \
                                (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="tgqkvsws",t8=L"ncharTagValue" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="gybqvhos",c8=L"ncharColValue",c9=7u64 1626006833639001000', 'sfzqdz'), \
                                (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="tgqkvsws",t8=L"ncharTagValue" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="zlvxgquy",c8=L"ncharColValue",c9=7u64 1626006833639002000', 'sfzqdz'), \
                                (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="tgqkvsws",t8=L"ncharTagValue" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="oaupfgtz",c8=L"ncharColValue",c9=7u64 1626006833639003000', 'sfzqdz'), \
                                (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="tgqkvsws",t8=L"ncharTagValue" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="vgzadjsh",c8=L"ncharColValue",c9=7u64 1626006833639004000', 'sfzqdz')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_d_ts_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_s_tb_d_data_d_ts_ac_mt_insert_multi_thread_check(self):
        """
        thread input same stb tb, different ts, add col, mul tag
        """
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name(7, "letters")
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_d_ts_a_col_m_tag_list = [(f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="htvnnldm",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=t 1626006833639000000', 'sfzqdz'), \
                                            (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="gybqvhos",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=t 1626006833639001000', 'sfzqdz'), \
                                            (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="zlvxgquy",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=t 1626006833639002000', 'sfzqdz'), \
                                            (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="oaupfgtz",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=t 1626006833639003000', 'sfzqdz'), \
                                            (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="vgzadjsh",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=t 1626006833639004000', 'sfzqdz')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_d_ts_a_col_m_tag_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 6)
        self.tdSql.query(f"select * from {stb_name} where t8 is not NULL")
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        self.tdSql.query(f"select * from {stb_name} where c11 is not NULL;")
        self.tdSql.checkEqual(self.tdSql.query_row, 5)

    def s_stb_s_tb_d_data_d_ts_at_mc_insert_multi_thread_check(self):
        """
        thread input same stb tb, different ts, add tag, mul col
        """
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name(7, "letters")
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_d_ts_a_tag_m_col_list = [(f'{stb_name},id={tb_name},t0=False,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="xsajdfjc",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 1626006833639000000', 'rgqcfb'), \
                                            (f'{stb_name},id={tb_name},t0=False,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="xsajdfjc",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 1626006833639001000', 'rgqcfb'), \
                                            (f'{stb_name},id={tb_name},t0=False,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="xsajdfjc",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 1626006833639002000', 'rgqcfb'), \
                                            (f'{stb_name},id={tb_name},t0=False,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="xsajdfjc",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 1626006833639003000', 'rgqcfb'), \
                                            (f'{stb_name},id={tb_name},t0=False,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="xsajdfjc",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 1626006833639004000', 'rgqcfb')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_d_ts_a_tag_m_col_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 6)
        for c in ["c7", "c8", "c9"]:
            self.tdSql.query(f"select * from {stb_name} where {c} is NULL")
            self.tdSql.checkEqual(self.tdSql.query_row, 5)
        for t in ["t10", "t11"]:
            self.tdSql.query(f"select * from {stb_name} where {t} is not NULL;")
            self.tdSql.checkEqual(self.tdSql.query_row, 5)

    def s_stb_d_tb_d_data_d_ts_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, data, ts
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_d_tb_d_ts_list = self.tdCom.gen_sql_list(stb_name=stb_name)[10]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_d_ts_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_d_tb_d_data_d_ts_ac_mt_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, data, ts, add col, mul tag
        """
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_d_tb_d_ts_a_col_m_tag_list = [(f'{stb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="eltflgpz",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=True 0', 'ynnlov'), \
                                            (f'{stb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="ysznggwl",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=t 0', 'ynnlov'), \
                                            (f'{stb_name},t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="nxwjucch",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=f 0', 'ynnlov'), \
                                            (f'{stb_name},t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="fzseicnt",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=F 0', 'ynnlov'), \
                                            (f'{stb_name},t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="zwgurhdp",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=False 0', 'ynnlov')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_d_ts_a_col_m_tag_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 3)

    def run(self) -> bool:
        self.init_check()
        self.bool_check()
        self.symbols_check()
        self.ts_check()
        self.id_seq_check()
        self.id_letter_check()
        self.no_id_check()
        self.max_col_tag_check()
        self.stb_tb_name_check()
        self.id_start_with_num_check()
        self.now_check()
        self.date_format_check()
        self.illegal_ts_check()
        # self.tbname_check()
        self.tag_value_length_check()
        self.col_value_length_check()
        self.tag_col_illegal_value_check()
        self.duplicate_id_tag_col_insert_check()
        self.no_id_stb_exist_check()
        self.duplicate_insert_exist_check()
        self.tag_col_binary_nchar_length_check()
        # self.tag_col_add_dup_id_check()
        self.tag_col_add_check()
        self.tag_md5_check()
        self.tag_col_binary_max_length_check()
        self.batch_insert_check()
        self.multi_insert_check(10)
        self.batch_error_insert_check()
        self.multi_cols_insert_check()
        self.multi_tags_insert_check()
        self.blank_col_insert_check()
        self.blank_tag_insert_check()
        self.chinese_check()
        # TODO confirm
        # self.spell_check()
        # self.default_type_check()
        self.tbname_tags_cols_name_check()
        self.stb_insert_multi_thread_check()
        self.s_stb_s_tb_d_data_insert_multi_thread_check()
        self.s_stb_s_tb_d_data_atc_insert_multi_thread_check()
        self.s_stb_stb_d_data_mtc_insert_multi_thread_check()
        self.s_stb_d_tb_d_data_insert_multi_thread_check()
        self.s_stb_d_tb_d_data_ac_mt_insert_multi_thread_check()
        self.s_stb_d_tb_d_data_at_mc_insert_multi_thread_check()
        # self.s_stb_s_tb_d_data_d_ts_insert_multi_thread_check()
        # self.s_stb_s_tb_d_data_d_ts_ac_mt_insert_multi_thread_check()
        # self.s_stb_s_tb_d_data_d_ts_at_mc_insert_multi_thread_check()
        self.s_stb_d_tb_d_data_d_ts_insert_multi_thread_check()
        self.s_stb_d_tb_d_data_d_ts_ac_mt_insert_multi_thread_check()

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
