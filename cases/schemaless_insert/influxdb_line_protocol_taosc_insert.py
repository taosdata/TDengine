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
import sys
from taostest.util.remote import Remote
import threading
import time
class TestInfluxdbLineTaoscInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdCom.env_setting = self.env_setting
        self.tdCom.sml_type = "influxdb"
        self.tdCom.drop_all_db()
        self.dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname=self.dbname, precision="us")
        self._remote: Remote = Remote(self.logger)
        self.taospy_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taospy")
        if "smlChildTableName" in self.taospy_setting["spec"]["config"]:
            if self.taospy_setting["spec"]["config"]["smlChildTableName"].upper() == "ID":
                self.tdCom.set_sml_specified_value()

    def init_check(self):
        """
        normal tags and cols, one for every elm
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)

    def bool_check(self):
        """
        check all normal bool type
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        full_type_list = ["f", "F", "false", "False", "t", "T", "true", "True"]
        for t_type in full_type_list:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c0=t_type, t0=t_type)
            self.tdCom.check_res(input_sql, stb_name)

    def symbols_check(self):
        """
        check symbols = \"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"\"
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        binary_symbols = '"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"'
        nchar_symbols = f'L{binary_symbols}'
        input_sql, stb_name = self.tdCom.gen_full_type_sql(c7=binary_symbols, c8=nchar_symbols, t7=binary_symbols, t8=nchar_symbols)
        self.tdCom.check_res(input_sql, stb_name)

    def ts_check(self):
        """
        test ts list --> ["1626006833639000000", "1626006833639019us", "1626006833640ms", "1626006834s", "1626006822639022"]
        # ! us级时间戳都为0时，数据库中查询显示，但python接口拿到的结果不显示 .000000的情况请确认，目前修改时间处理代码可以通过
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
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
        kv_dict = {"precision": "ms"}
        self.tdCom.createDb("test_ts", **kv_dict)
        input_sql = ['test_ms,t0=t c0=t 1626006833640', 'test_ms,t0=t c0=f 1626006833641']
        self.tdSql._conn.schemaless_insert(input_sql, TDSmlProtocolType.LINE.value, TDSmlTimestampType.MILLI_SECOND.value)
        self.tdSql.query('select * from test_ms')
        self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), "2021-07-11 20:33:53.640000")
        self.tdSql.checkEqual(str(self.tdSql.query_data[1][0]), "2021-07-11 20:33:53.641000")

        self.tdSql.execute(f"drop database if exists test_ts")
        kv_dict = {"precision": "us"}
        self.tdCom.createDb("test_ts", **kv_dict)
        input_sql = ['test_us,t0=t c0=t 1626006833639000', 'test_us,t0=t c0=f 1626006833639001']
        self.tdSql._conn.schemaless_insert(input_sql, TDSmlProtocolType.LINE.value, TDSmlTimestampType.MICRO_SECOND.value)
        self.tdSql.query('select * from test_us')
        self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), "2021-07-11 20:33:53.639000")
        self.tdSql.checkEqual(str(self.tdSql.query_data[1][0]), "2021-07-11 20:33:53.639001")

        self.tdSql.execute(f"drop database if exists test_ts")
        kv_dict = {"precision": "ns"}
        self.tdCom.createDb("test_ts", **kv_dict)
        input_sql = ['test_ns,t0=t c0=t 1626006833639000000', 'test_ns,t0=t c0=f 1626006833639000001']
        self.tdSql._conn.schemaless_insert(input_sql, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.tdSql.query('select * from test_ns')
        self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), "1626006833639000000")
        self.tdSql.checkEqual(str(self.tdSql.query_data[1][0]), "1626006833639000001")

        self.tdCom.createDb(precision="us")

    def id_seq_check(self):
        """
        check id.index in tags
        eg: t0=**,id=**,t1=**
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(id_change_tag=True)
        self.tdCom.check_res(input_sql, stb_name)

    def id_letter_check(self):
        """
        check id param
        eg: id and ID
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        for input_sql in [self.tdCom.gen_long_sql(self.tdCom.boundary_config["MAX_TAG_COUNT"]-1, 1)[0], self.tdCom.gen_long_sql(1, self.tdCom.boundary_config["MAX_TAG_COL_COUNT"]-3)[0]]:
            # ! TD-19457
            self.tdCom.cleanTb(dbname=self.dbname)
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        for input_sql in [self.tdCom.gen_long_sql(self.tdCom.boundary_config["MAX_TAG_COUNT"], 1)[0], self.tdCom.gen_long_sql(1, self.tdCom.boundary_config["MAX_TAG_COL_COUNT"]-2)[0]]:
            self.tdCom.cleanTb(dbname=self.dbname)
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name="1aaabbb")
        self.tdCom.check_res(input_sql, stb_name)

    def now_check(self):
        """
        check now unsupported
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        stb_name_192 = self.tdCom.get_long_name(length=self.tdCom.boundary_config["STBNAME_MAX_LENGTH"])
        tb_name_192 = self.tdCom.get_long_name(length=self.tdCom.boundary_config["TBNAME_MAX_LENGTH"])
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name_192, tb_name=tb_name_192)
        self.tdCom.check_res(input_sql, stb_name)
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        for i in [self.tdCom.gen_full_type_sql(stb_name=self.tdCom.get_long_name(length=self.tdCom.boundary_config["STBNAME_MAX_LENGTH"]+1), tb_name=self.tdCom.get_long_name(length=5))]:
            try:
                self.tdSql._conn.schemaless_insert([i[0]], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        input_sql = 'Abcdffgg,id=Abcddd,T1=127i8 c0=False 1626006833639000000'
        stb_name = "`Abcdffgg`"
        self.tdCom.check_res(input_sql, stb_name)

    def tag_value_length_check(self):
        """
        check full type tag value limit
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        # nchar
        # * legal nchar could not be larger than 16374/4
        stb_name = self.tdCom.get_long_name()
        legal_length = int(len(self.tdCom.get_long_name(self.tdCom.boundary_config["TAG_COLUMN_MAX_LENGTH"]))/4)
        input_sql = f'{stb_name},t1={self.tdCom.get_long_name(legal_length)} c0=f 1626006833639000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        input_sql = f'{stb_name},t1={self.tdCom.get_long_name(legal_length+1)} c0=f 1626006833639000000'
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def col_value_length_check(self):
        """
        check full type col value limit
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        # i8
        for c1 in [f'-{self.tdCom.boundary_config["TINYINT_MAX"]}i8', f'{self.tdCom.boundary_config["TINYINT_MAX"]}i8']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c1=c1)
            self.tdCom.check_res(input_sql, stb_name)

        for c1 in [f'-{self.tdCom.boundary_config["TINYINT_MAX"]+2}i8', f'{self.tdCom.boundary_config["TINYINT_MAX"]+1}i8']:
            input_sql = self.tdCom.gen_full_type_sql(c1=c1)[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)
        # i16
        for c2 in [f'-{self.tdCom.boundary_config["SMALLINT_MAX"]}i16']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c2=c2)
            self.tdCom.check_res(input_sql, stb_name)
        for c2 in [f'-{self.tdCom.boundary_config["SMALLINT_MAX"]+2}i16', f'{self.tdCom.boundary_config["SMALLINT_MAX"]+1}i16']:
            input_sql = self.tdCom.gen_full_type_sql(c2=c2)[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # i32
        for c3 in [f'-{self.tdCom.boundary_config["INT_MAX"]}i32']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c3=c3)
            self.tdCom.check_res(input_sql, stb_name)
        for c3 in [f'-{self.tdCom.boundary_config["INT_MAX"]+2}i32', f'{self.tdCom.boundary_config["INT_MAX"]+1}i32']:
            input_sql = self.tdCom.gen_full_type_sql(c3=c3)[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)
        # i64
        for c4 in [f'-{self.tdCom.boundary_config["BIGINT_MAX"]}i64', '1076048383523889174i64', f'{self.tdCom.boundary_config["BIGINT_MAX"]}i64']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c4=c4)
            self.tdCom.check_res(input_sql, stb_name)
        for c4 in [f'-{self.tdCom.boundary_config["BIGINT_MAX"]+2}i64', f'{self.tdCom.boundary_config["BIGINT_MAX"]+1}i64']:
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
        # ! bug 
        # # * limit set to 1.797693134862316*(10**308)
        # for c6 in [f'{-1.797693134862316*(10**308)}f64', f'{-1.797693134862316*(10**308)}f64']:
        #     input_sql = self.tdCom.gen_full_type_sql(c6=c6)[0]
        #     try:
        #         self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        #         raise Exception("should not reach here")
        #     except SchemalessError as err:
        #         self.tdSql.checkNotEqual(err.errno, 0)

        # # # binary
        # stb_name = self.tdCom.get_long_name()
        # input_sql = f'{stb_name},t0=t c0=f,c1="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}" 1626006833639000000'
        # self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        # input_sql = f'{stb_name},t0=t c0=f,c1="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"]+1)}" 1626006833639000000'
        # try:
        #     self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        #     raise Exception("should not reach here")
        # except SchemalessError as err:
        #     self.tdSql.checkNotEqual(err.errno, 0)

        # # nchar
        # # * legal nchar could not be larger than 16374/4
        # stb_name = self.tdCom.get_long_name()
        # input_sql = f'{stb_name},t0=t c0=f,c1=L"{self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"])}" 1626006833639000000'
        # self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        input_sql = f'{stb_name},t0=t c0=f,c1=L"{self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"]+1)}" 1626006833639000000'
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def tag_col_illegal_value_check(self):
        """
        test illegal tag col value
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
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
        stb_name = self.tdCom.get_long_name()
        input_sql1 = f'{stb_name}_1,t0=t c0=f,c1="abc aaa" 1626006833639000000'
        input_sql2 = f'{stb_name}_2,t0=t c0=f,c1=L"abc aaa" 1626006833639000000'
        input_sql3 = f'{stb_name}_3,t0=t,t1="abc\ aaa" c0=f 1626006833639000000'
        input_sql4 = f'{stb_name}_4,t0=t,t1=L"abc\ aaa" c0=f 1626006833639000000'
        for input_sql in [input_sql1, input_sql2, input_sql3, input_sql4]:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        # check accepted binary and nchar symbols
        # # * ~!@#$¥%^&*()-+{}|[]、「」:;
        stb_name1 = self.tdCom.get_long_name()
        stb_name2 = self.tdCom.get_long_name()
        for symbol in list('、「」~!@#$¥%^&*()-+{}|[]:;'):
            input_sql1 = f'{stb_name1},t0=t c0=f,c1="abc{symbol}aaa" 1626006833639000000'
            input_sql2 = f'{stb_name2},t0=t,t1="abc{symbol}aaa" c0=f 1626006833639000000'
            self.tdSql._conn.schemaless_insert([input_sql1], TDSmlProtocolType.LINE.value, None)
            # ! bug
            # self.tdSql._conn.schemaless_insert([input_sql2], TDSmlProtocolType.LINE.value, None)

    def duplicate_id_tag_col_insert_check(self):
        """
        check duplicate Id Tag Col
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
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
        stb_name = input_sql_col.split(",")[0]
        self.tdCom.check_res(input_sql, stb_name)

    ##### stb exist #####
    def no_id_stb_exist_check(self):
        """
        case no id when stb exist
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name="sub_table_0123456", t0="f", c0="f")
        self.tdCom.check_res(input_sql, stb_name)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, id_noexist_tag=True, t0="f", c0="f")
        self.tdCom.check_res(input_sql, stb_name, condition='where tbname like "t_%"')
        # self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, None)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        # self.tdSql.query(f"select * from {stb_name} where id is Null")
        # self.tdSql.checkEqual(self.tdSql.query_row, 1)

    def duplicate_insert_exist_check(self):
        """
        check duplicate insert when stb exist
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.tdCom.check_res(input_sql, stb_name)

    def tag_col_binary_nchar_length_check(self):
        """
        check length increase
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        tb_name = self.tdCom.get_long_name()
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name, t0="t", c0="t")
        self.tdCom.check_res(input_sql, stb_name)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name, t0="t", c0="f", ct_add_tag=True)
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.tdSql.query(f'select * from {stb_name} where tbname like "{tb_name}"')
        self.tdSql.checkData(0, 1, False)
        self.tdSql.checkData(0, 11, "ncharColValue")
        self.tdSql.checkData(0, 12, True)
        self.tdSql.checkData(0, 22, None)
        self.tdSql.checkData(0, 23, None)

    def tag_col_add_check(self):
        """
        check column and tag count add
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name, t0="f", c0="f")
        self.tdCom.check_res(input_sql, stb_name)
        tb_name_1 = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name_1, t0="f", c0="f", ct_add_tag=True)
        self.tdCom.check_res(input_sql, stb_name, condition=f'where tbname like "{tb_name_1}"')
        res_row_list = self.tdCom.res_handle(f'select c10,c11,t10,t11 from {tb_name}', stb_name)[0]
        self.tdSql.checkEqual(res_row_list[0], ['None', 'None', 'None', 'None'])
        self.tdCom.check_res(input_sql, stb_name, condition=f'where tbname like "{tb_name}"', none_check_tag=True)

    def tag_md5_check(self):
        """
        condition: stb not change
        insert two table, keep tag unchange, change col
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
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

    def tag_col_binary_max_length_check(self):
        """
        every binary and nchar must be length+2
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        stb_name = self.tdCom.get_long_name()
        tb_name = f'{stb_name}_1'
        input_sql = f'{stb_name},id={tb_name},t0=t c0=f 1626006833639000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        input_sql = f'{stb_name},t0=t c1="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}" 1626006833639000000'
        # old logic
        # input_sql = f'{stb_name},t0=t c0=f,c1="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c2="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c3="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c4="{self.tdCom.get_long_name(12)}" 1626006833639000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        input_sql = f'{stb_name},t0=t c1="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c2="{self.tdCom.get_long_name(1)}" 1626006833639000000'
        # old logic
        # input_sql = f'{stb_name},t0=t c0=f,c1="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c2="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c3="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c4="{self.tdCom.get_long_name(13)}" 1626006833639000000'
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def tag_col_nchar_max_length_check(self):
        """
        check nchar length limit
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        stb_name = self.tdCom.get_long_name(7)
        tb_name = f'{stb_name}_1'
        input_sql = f'{stb_name},id={tb_name},t2={self.tdCom.get_long_name(1)} c0=f 1626006833639000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        legal_length = int(len(self.tdCom.get_long_name(self.tdCom.boundary_config["TAG_COLUMN_MAX_LENGTH"]))/4)
        # * when < 1024 nchar_length -> 2^n
        input_sql = f'{stb_name},t1={self.tdCom.get_long_name(legal_length-19)},t2={self.tdCom.get_long_name(1)} c0=f 1626006833639000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        input_sql = f'{stb_name},t1={self.tdCom.get_long_name(legal_length-18)},t2={self.tdCom.get_long_name(1)} c0=f 1626006833639000000'
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

        input_sql = f'{stb_name},t2={self.tdCom.get_long_name(1)} c0=f,c1=L"{self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"])}",c2=t,c3=t,c4=t,c5=t 1626006833639000000'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 3)
        input_sql = f'{stb_name},t2={self.tdCom.get_long_name(1)} c0=f,c1=L"{self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"])}",c2=t,c3=t,c4=t,c5=t,c6=t 1626006833639000000'
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.createDb(dbname=self.dbname, precision="us")
        self.tdCom.cleanTb(dbname=self.dbname)
        stb_name = self.tdCom.get_long_name()
        # self.tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 bigint)')
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
        self.tdSql.query(f'select * from information_schema.ins_stables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 3)
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 6)
        self.tdSql.query('select * from st123456')
        self.tdSql.checkEqual(self.tdSql.query_row, 5)

    def multi_insert_check(self, count):
        """
        test multi insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        sql_list = []
        stb_name = self.tdCom.get_long_name()
        # TODO commit out
        # self.tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 nchar(10))')
        for i in range(count):
            input_sql = self.tdCom.gen_full_type_sql(stb_name=stb_name, t8=f'"{self.tdCom.get_long_name()}"', c8=f'"{self.tdCom.get_long_name()}"', id_noexist_tag=True)[0]
            sql_list.append(input_sql)
        self.tdSql._conn.schemaless_insert(sql_list, TDSmlProtocolType.LINE.value, None)
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, count)

    def batch_error_insert_check(self):
        """
        test batch error insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        stb_name = self.tdCom.get_long_name()
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql = self.tdCom.gen_full_type_sql(c_multi_tag=True)[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)
    
    def same_ts_batch_insert(self):
        """
        test same ts batch insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql = ['ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="binaryTagValue",t8=L"ncharTagValue" c0=false,c1=1i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="xcxvwjvf",c8=L"ncharColValue",c9=7u64 1626006833639000000',
        'ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="binaryTagValue",t8=L"ncharTagValue" c0=T,c1=2i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="fixrzcuq",c8=L"ncharColValue",c9=7u64 1626006833639000000',
        'ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="binaryTagValue",t8=L"ncharTagValue" c0=t,c1=3i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="iupzdqub",c8=L"ncharColValue",c9=7u64 1626006833639000000',
        'ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="binaryTagValue",t8=L"ncharTagValue" c0=t,c1=4i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="yvvtzzof",c8=L"ncharColValue",c9=7u64 1626006833639000000',
        'ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="binaryTagValue",t8=L"ncharTagValue" c0=t,c1=5i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="vbxpilkj",c8=L"ncharColValue",c9=7u64 1626006833639000000']
        self.tdSql._conn.schemaless_insert(input_sql, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.tdSql.query('select * from ubzlsr')
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        self.tdSql.checkEqual(int(self.tdSql.query_data[0][2]), 5)

    def multi_tags_insert_check(self):
        """
        test multi tags insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(t_blank_tag=True)
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.tdSql.query(f'select * from {stb_name}')
        self.tdSql.checkEqual(self.tdSql.query_data[0][-1], None)

    def chinese_check(self):
        """
        check nchar ---> chinese
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(chinese_tag=True)
        self.tdCom.check_res(input_sql, stb_name)

    def spell_check(self):
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        stb_name = self.tdCom.get_long_name()
        self.tdCom.cleanTb(dbname=self.dbname)
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        stb_name = self.tdCom.get_long_name()
        input_sql_list = [f'{stb_name}_1,t0=127,t1=32767I16,t2=2147483647I32,t3=9223372036854775807,t4=11.12345027923584F32,t5=22.123456789F64 c0=127,c1=32767I16,c2=2147483647I32,c3=9223372036854775807,c4=11.12345027923584F32,c5=22.123456789F64 1626006833639000000',
                            f'{stb_name}_2,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=22.123456789 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=22.123456789 1626006833639000000',
                            f'{stb_name}_3,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=10e5F32 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=10e5F64 1626006833639000000',
                            f'{stb_name}_4,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=10.0e5f64 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=10.0e5f32 1626006833639000000',
                            f'{stb_name}_5,t0=127I8,t1=32767I16,t2=2147483647I32,t3=9223372036854775807I64,t4=11.12345027923584F32,t5=-10.0e5 c0=127I8,c1=32767I16,c2=2147483647I32,c3=9223372036854775807I64,c4=11.12345027923584F32,c5=-10.0e5 1626006833639000000']
        for input_sql in input_sql_list:
            stb_name = input_sql.split(",")[0]
            self.tdCom.check_res(input_sql, stb_name)

    def tbname_tags_cols_name_check(self):
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        if "smlChildTableName" in self.taospy_setting["spec"]["config"]:
            if self.tdCom.smlChildTableName_value.upper() == "ID":
                input_sql = 'rFa$sta,id=rFas$ta_1,Tt!0=true,tT@1=127i8,t#2=32767i16,\"t$3\"=2147483647i32,t%4=9223372036854775807i64,t^5=11.12345f32,t&6=22.123456789f64,t*7=\"ddzhiksj\",t!@#$%^&*()_+[];:<>?\,9=L\"ncharTagValue\" C)0=True,c{1=127i8,c[2=32767i16,c;3=2147483647i32,c:4=9223372036854775807i64,c<5=11.12345f32,c>6=22.123456789f64,c?7=\"bnhwlgvj\",c.8=L\"ncharTagValue\",c!@#$%^&*()_+[];:<>?\,=7u64 1626006933640000000'
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
                query_sql = 'select * from `rFa$sta`'
                self.tdSql.query(query_sql)
                self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2021, 7, 11, 20, 35, 33, 640000), True, 127, 32767, 2147483647, 9223372036854775807, 11.12345027923584, 22.123456789, 'bnhwlgvj', 'ncharTagValue', 7, 'true', '127i8', '32767i16', '2147483647i32', '9223372036854775807i64', '11.12345f32', '22.123456789f64', '"ddzhiksj"', 'L"ncharTagValue"')])
                query_sql = 'describe `rFa$sta`'
                self.tdSql.query(query_sql)
                self.tdSql.checkEqual(self.tdSql.getColNameList(), ['_ts', 'C)0', 'c{1', 'c[2', 'c;3', 'c:4', 'c<5', 'c>6', 'c?7', 'c.8', 'c!@#$%^&*()_+[];:<>?,', 'Tt!0', 'tT@1', 't#2', '"t$3"', 't%4', 't^5', 't&6', 't*7', 't!@#$%^&*()_+[];:<>?,9'])
                self.tdSql.execute('drop table `rFa$sta`')
        else:
            input_sql = 'rFa$sta,id=rFas$ta_1,Tt!0=true,tT@1=127i8,t#2=32767i16,\"t$3\"=2147483647i32,t%4=9223372036854775807i64,t^5=11.12345f32,t&6=22.123456789f64,t*7=\"ddzhiksj\",t!@#$%^&*()_+[];:<>?\,9=L\"ncharTagValue\" C)0=True,c{1=127i8,c[2=32767i16,c;3=2147483647i32,c:4=9223372036854775807i64,c<5=11.12345f32,c>6=22.123456789f64,c?7=\"bnhwlgvj\",c.8=L\"ncharTagValue\",c!@#$%^&*()_+[];:<>?\,=7u64 1626006933640000000'
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
            query_sql = 'select * from `rFa$sta`'
            self.tdSql.query(query_sql)
            self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2021, 7, 11, 20, 35, 33, 640000), True, 127, 32767, 2147483647, 9223372036854775807, 11.12345027923584, 22.123456789, 'bnhwlgvj', 'ncharTagValue', 7, 'rFas$ta_1', 'true', '127i8', '32767i16', '2147483647i32', '9223372036854775807i64', '11.12345f32', '22.123456789f64', '"ddzhiksj"', 'L"ncharTagValue"')])
            query_sql = 'describe `rFa$sta`'
            self.tdSql.query(query_sql)
            self.tdSql.checkEqual(self.tdSql.getColNameList(), ['_ts', 'C)0', 'c{1', 'c[2', 'c;3', 'c:4', 'c<5', 'c>6', 'c?7', 'c.8', 'c!@#$%^&*()_+[];:<>?,', 'id', 'Tt!0', 'tT@1', 't#2', '"t$3"', 't%4', 't^5', 't&6', 't*7', 't!@#$%^&*()_+[];:<>?,9'])
            self.tdSql.execute('drop table `rFa$sta`')


    def stb_insert_multi_thread_check(self):
        """
        thread input different stb
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql = self.tdCom.gen_sql_list()[0]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(input_sql))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 5)

    def s_stb_s_tb_d_data_insert_multi_thread_check(self):
        """
        thread input same stb tb, different data, result keep first data
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_list = self.tdCom.gen_sql_list(stb_name=stb_name, tb_name=tb_name)[1]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        self.tdSql.query(f"select * from {stb_name};")
        self.tdSql.checkEqual(self.tdSql.query_row, 1)

    def s_stb_s_tb_d_data_atc_insert_multi_thread_check(self):
        """
        thread input same stb tb, different data, add columes and tags,  result keep first data
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_a_col_a_tag_list = self.tdCom.gen_sql_list(stb_name=stb_name, tb_name=tb_name)[2]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_a_col_a_tag_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        self.tdSql.query(f"select * from {stb_name};")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def s_stb_stb_d_data_mtc_insert_multi_thread_check(self):
        """
        thread input same stb tb, different data, minus columes and tags,  result keep first data
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_m_col_m_tag_list = self.tdCom.gen_sql_list(stb_name=stb_name, tb_name=tb_name)[3]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_m_col_m_tag_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        self.tdSql.query(f"select * from {stb_name};")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def s_stb_d_tb_d_data_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, different data
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_d_tb_list = self.tdCom.gen_sql_list(stb_name=stb_name)[4]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_d_tb_d_data_ac_mt_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, different data, add col, mul tag
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        # s_stb_d_tb_a_col_m_tag_list = self.tdCom.gen_sql_list(stb_name=stb_name)[5]
        s_stb_d_tb_a_col_m_tag_list = [(f'{stb_name},t0=F,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="ngxgzdzs",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=F 1626006833639000000', 'hpxbys'), \
                                        (f'{stb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="vvfrdtty",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=True 1626006833639000000', 'hpxbys'), \
                                        (f'{stb_name},t0=F,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="kzscucnt",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=f 1626006833639000000', 'hpxbys'), \
                                        (f'{stb_name},t0=F,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="asegdbqk",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=false 1626006833639000000', 'hpxbys'), \
                                        (f'{stb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="yvqnhgmn",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=T 1626006833639000000', 'hpxbys')]

        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_a_col_m_tag_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def s_stb_d_tb_d_data_at_mc_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, different data, add tag, mul col
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_d_tb_a_tag_m_col_list = self.tdCom.gen_sql_list(stb_name=stb_name)[6]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_a_tag_m_col_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_s_tb_d_data_d_ts_insert_multi_thread_check(self):
        """
        thread input same stb tb, different ts
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_d_ts_list = [(f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="tgqkvsws",t8=L"ncharTagValue" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="htvnnldm",c8=L"ncharColValue",c9=7u64 1626006833639000000', 'sfzqdz'), \
                                (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="tgqkvsws",t8=L"ncharTagValue" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="gybqvhos",c8=L"ncharColValue",c9=7u64 1626006833639001000', 'sfzqdz'), \
                                (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="tgqkvsws",t8=L"ncharTagValue" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="zlvxgquy",c8=L"ncharColValue",c9=7u64 1626006833639002000', 'sfzqdz'), \
                                (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="tgqkvsws",t8=L"ncharTagValue" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="oaupfgtz",c8=L"ncharColValue",c9=7u64 1626006833639003000', 'sfzqdz'), \
                                (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="tgqkvsws",t8=L"ncharTagValue" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="vgzadjsh",c8=L"ncharColValue",c9=7u64 1626006833639004000', 'sfzqdz')]

        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_d_ts_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_s_tb_d_data_d_ts_ac_mt_insert_multi_thread_check(self):
        """
        thread input same stb tb, different ts, add col, mul tag
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_d_ts_a_col_m_tag_list = [(f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="htvnnldm",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=t 1626006833639000000', 'sfzqdz'), \
                                            (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="gybqvhos",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=t 1626006833639001000', 'sfzqdz'), \
                                            (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="zlvxgquy",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=t 1626006833639002000', 'sfzqdz'), \
                                            (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="oaupfgtz",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=t 1626006833639003000', 'sfzqdz'), \
                                            (f'{stb_name},id={tb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="vgzadjsh",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=t 1626006833639004000', 'sfzqdz')]
        # for input_sql in s_stb_s_tb_d_ts_a_col_m_tag_list:
        #     self.tdSql._conn.schemaless_insert([input_sql[0]], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)

        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_d_ts_a_col_m_tag_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name, t0=False)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_d_ts_a_tag_m_col_list = [(f'{stb_name},id={tb_name},t0=False,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="xsajdfjc",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 1626006833639000000', 'rgqcfb'), \
                                            (f'{stb_name},id={tb_name},t0=False,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="xsajdfjc",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 1626006833639001000', 'rgqcfb'), \
                                            (f'{stb_name},id={tb_name},t0=False,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="xsajdfjc",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 1626006833639002000', 'rgqcfb'), \
                                            (f'{stb_name},id={tb_name},t0=False,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="xsajdfjc",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 1626006833639003000', 'rgqcfb'), \
                                            (f'{stb_name},id={tb_name},t0=False,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="xsajdfjc",t8=L"ncharTagValue",t11=127i8,t10=L"ncharTagValue" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64 1626006833639004000', 'rgqcfb')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_d_ts_a_tag_m_col_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_d_tb_d_ts_list = self.tdCom.gen_sql_list(stb_name=stb_name)[10]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_d_ts_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_d_tb_d_data_d_ts_ac_mt_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, data, ts, add col, mul tag
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_d_tb_d_ts_a_col_m_tag_list = [(f'{stb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="eltflgpz",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=True 0', 'ynnlov'), \
                                            (f'{stb_name},t0=True,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="ysznggwl",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=t 0', 'ynnlov'), \
                                            (f'{stb_name},t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="nxwjucch",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=f 0', 'ynnlov'), \
                                            (f'{stb_name},t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="fzseicnt",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=F 0', 'ynnlov'), \
                                            (f'{stb_name},t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64 c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="zwgurhdp",c8=L"ncharColValue",c9=7u64,c11=L"ncharColValue",c10=False 0', 'ynnlov')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_d_ts_a_col_m_tag_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def ts_2828(self, thread_count, batch_count, loop_times):
        count = 0
        for i in range(loop_times):
            input_sql_list1 = list()
            input_sql_list_tmp = list()
            for j in range(thread_count):
                for i in range(int(batch_count/2)):
                    input_sql_list1.append(f'E50,VIN=LK{j}ADCE12MB210131{i} TalClNum_G1=L"{i}",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"61",VCUVehDrvMod=L"0",BMSCode=L"0",EPSWorkCur=L"0",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",TMOrd_G=L"1",DrSbltAtc=L"0",RrImpctDet=L"0",BatMiniCelVol=L"3334",OBCORDCACIntnlTemp=L"54",TMDeratingSts=L"0",TemperoHeatMeb3=L"205",BattCellVoltage_16=L"3.337",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",TemperoHeatMeb6=L"205",BatMinTemp=L"25",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"205",PDUHVLock=L"1",BattCellVoltage_17=L"3.337",BatFltNumN1_G=L"0",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"1",BatMaNegRlySts=L"1",VecActTalVol=L"93.4",MCUCur_G=L"0",vehType=L"E50",Longitude=L"108.491185",WhlGrndVlctyLftDrvn=L"0",BatPowSts=L"1",DFCDCANMUTE=L"0",TMActWkSts=L"2",BrkProcInPrgrsIO=L"0",CDUState=L"2",TMActSpd=L"0",objId=L"LK6ADCE12MB210131",PTID=L"2",DFCShiftLvrPlbtyErr=L"0",ABSAtv=L"0",OBCInpVolOv=L"0",EngyRevIndOn=L"0",RemainOdo=L"58",BrakPedalPos=L"0",BatMaPosRlySts=L"1",TMTemp_G=L"24",BatCelVolSensFlt=L"0",CDUType=L"9",Lattd_G=L"21.651174",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",DrTorqRqst=L"0",BatIslateSts=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"1",DCDCIntnlFlt=L"0",GearSts_G=L"15",VecChrgStsIndOn=L"1",zdsjxh=L"1514",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",EvpEquPreVal=L"-47.9",InitaContIndOn_EPS=L"1",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"1",DCDCOtpVol=L"14",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"15",InvActTemp=L"26",BMSGenSts1RollCnt=L"1",GPSspeed=L"0",SignalStrengthOne=L"4",StrWhAng=L"-0.9375",SignalStrengthTwo=L"29",RemtCtrParkHeat=L"0",createTime=L"1678171310001",ACRelayCmd=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"9",BatMinTempCode_G=L"2",CCSts=L"2",BatDvcVlt_G1=L"93.3",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",MCUSupCode=L"8330245",dqsjxh=L"569",Latitude=L"21.651174",VecActPow=L"0",BMSHvPowOnRqst=L"2",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",VecSOCLoWrngIndOn=L"0",OBCOtpVlt=L"94.3",BatMaxCelVolPos=L"4",t2RTime=L"2023-03-07 14:41:50",TMActDrvTorLmt=L"0",InitaContIndOn_OBC=L"0",TalCur_G=L"-15.5",BatAvgTemp=L"25",OBCInpCurOv=L"0",BatMaxClVltCode_G=L"4",CollisionSig=L"0",VecStatRdy=L"0",BatMaxCelVol=L"3337",ChrgCtrlSnglFail=L"0",KyPstn=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",MCUActFltNum=L"0",DCDCTempWrng=L"0",BatSubsyst_Vlt_G=L"1",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"0",BMSSts=L"2",DriverLeftWarning=L"0",BatMinTemp_G=L"25",BatRmaChrgTim=L"237",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",ModeCode=L"136",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"-15.5",BMSChrgCurRqst=L"40",Swit2Status=L"1",BatAvgCelVol=L"3336",t1RTime=L"2023-03-07 14:41:49",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",VacPumpTransRate=L"1",DFCGearReqMongErr=L"0",BatHetPrsrvtn=L"0",DFCCASmax=L"0",BatOthFltNum=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",VecFltIndOn=L"0",BatMinClVltCode_G=L"1",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",VecActTalCur=L"-15.4",ISBF=L"0",BatClVlt_G1=L"3.335_3.336_3.336_3.337_3.336_3.336_3.336_3.335_3.335_3.337_3.336_3.336_3.336_3.336_3.336_3.337_3.337_3.337_3.336_3.336_3.336_3.337_3.337_3.337_3.337_3.336_3.336_3.335",BatFltLvl=L"0",MCUInpVlt_G=L"94",TMWkStsRqst=L"0",BatMaxTempPos=L"6",TMActFbTorLmt=L"0",DrvPowLimtIndOn=L"0",OBCOtpVolOv=L"0",BatAvgCelVolSts=L"0",VCUSftwrVsinNum=L"232",OBCOtpCur=L"15.9",BatTalVolSts=L"0",BMSChrgVolRqst=L"103.2",TalVlt_G=L"93.3",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"11",TMTorqRqst=L"0",InvVolSts=L"0",VehOdoV=L"0",ShifGearFailAtHiSpeed=L"0",VecOptMod=L"4",SoftEdt=L"155",TMorMCUOvTempInd=L"0",OptMod_G=L"1",RemtCtrlSpdLimtRqstSt=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"24",VehOdo_G=L"24467",MCUTemp_G=L"26",TMSpdRqst=L"0",LowBatVol=L"14.01",BatTempSts=L"0",VecChrgRqst=L"1",VecTMActSpdV=L"0",VecTMActSpd=L"0",VCUBatThrmlRunwyAlmInd=L"0",MCUVolFlt=L"0",DCDCOtpVolSts=L"0",TMSpd_G=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"27",EvrmetAblPreVal=L"101.4",OBCInpCur=L"8",BatIslatRes=L"10016",BatMiniCelVolPos=L"1",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",OBCInpVolUn=L"0",VecSOC=L"49",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCOtpCur=L"2.7",InitaContIndOn_ABS=L"1",BMSChrgVolCurRqstAnorm=L"0",CollectTime=L"2023-03-07 14:41:49",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"175",BatMinClVlt_G=L"3.334",RatedEgy=L"9.5",WhlGrndVlctyRtNnDrvn=L"0",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"0",NoPulOutChrgPlugWrng=L"0",AntiSlope=L"0",HVRelSta=L"0",VecActPowPer=L"0",BatChrgVolCpl=L"103.2",MaxWrngLvl_G=L"0",InitaContIndOn_IC=L"1",BatheatindON=L"0",DFCUAcePedlPlbtyErr=L"0",AccActPos=L"0",PTCSwitch=L"2",MaxVltBatSubSysNum_G=L"1",ACSwitch=L"0",VCUCtrlRqst1RollCnt=L"6",TMSts_G=L"4",VehSts_G=L"2",VehSpdAvgDrvn=L"0",InitaContIndOn_SDM=L"1",RealSoc=L"50",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"2",t02RDelay=L"1",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"0",Direction=L"0",Lngtd_G=L"108.491185",RemtCtrlPrkHet=L"0",CPSts=L"2",BatThrmlRunwyAlmInd=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:49",DCDCInpCur=L"0.4",BatTalVolSm=L"93.4",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",BatMaxClVlt_G=L"3.337",AcInpFlt=L"0",VBatChrgStsIndOn=L"1",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"9792",BatCurSensFlt=L"0",AirbgIndOn=L"0",BatMinTemVolPakOrd=L"1",TMCtrlMdRqst=L"2",OBCInpMxCur=L"8",DrivStOccSt=L"1",BatSOH=L"97",BatSOC=L"49",BatEnrgAvail=L"3.7",VehOdo=L"24467",t2RIP=L"192.168.10.106",BatTempSensNum_G1=L"6",BatMaxTempCode_G=L"6",TmnlWkupSurc=L"3",OBCHdErr=L"0",TMActPow=L"0",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",BatTempSensFlt=L"0",BatDvcCur_G1=L"-15.5",TerminalNO=L"7681100322375855700M1130373",InvVol=L"94",InvJctTemp=L"26",DFCMongHvNotIntd=L"0",DCDCInpVol=L"94.3",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"11",MCUCurFlt=L"0",BatExtVol=L"93.2",DrSbltAtcV=L"0",BatIntVol=L"93.4",TMNum_G=L"1",OBCOvTemp=L"0",VCURemtCtrlSpdLmtFb=L"0",SupCode=L"1",DFCTioutForVT=L"0",InvrCur=L"0",Bat2sPlsChrgPowAvail=L"11",vehConfig=L"LV1",VecActGearSts=L"3",VehConf=L"3",BatMinTempPos=L"2",CellBattNum_21=L"21",BalanceSts=L"1",WhlGrndVlctyLftNnDrvn=L"0",VecChrgingSts=L"1",Bat10sPlsDischrgPowAvail=L"15.96",WhlGrndVlctyRtDrvn=L"0",SftwrMjrVsin=L"7",OBCInpVlt=L"218",RatedVol=L"90",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",BatMaxTemp=L"27",EvpRlySta=L"0",TMTorq_G=L"0",BatInfLen=L"0",TMActTemp=L"24",PlsAgainToRdy=L"0",BatChrgSts=L"1",HandBrkSts=L"1",InitaContIndOn_UCU=L"1",SOC_G=L"49",IslatRes_G=L"10016",objType=L"E50",BatSubsysTemp_G1=L"26_25_26_25_25_27",BatContiDischrgPowAvail=L"15.96",InitaContIndOn_MCU=L"1",PowBatErrIndOn=L"0",BatSOCSTS=L"0",VecIslateWrngIndOn=L"0",DCDCInternalTemp=L"39",DFCMCUGen1TiOut=L"0",MinTempSubSysNum_G=L"1",Bat2sPlsDischrgPowAvail=L"15.96",TMActTorq=L"0",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:49",VehDrvMod=L"1",MCUCtrlFlt=L"0",BatChrgTims=L"255",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",VacPumpWrngIndOn=L"0",TalNumOfClBatInThisFrm_G1=L"28",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DFCDOPIN56Flt=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",RemtHeatModRqst=L"0",InitaContIndOn_BMS=L"1" {1678171309000+count}')
                input_sql_list_tmp.append(input_sql_list1)
                for i in range(int(batch_count/2)):
                    input_sql_list1.append(f'E50,VIN=LK{j}ADAE14MB308114{i} BMSCode=L"{i}",TMOrd_G=L"1",RRDoorOpenSwAct=L"0",RrImpctDet=L"0",BattCellVoltage_16=L"4.037",BattCellVoltage_17=L"4.038",BatFltNumN1_G=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"104.9",MCUCur_G=L"0",BatPowSts=L"1",TaPnLgtAtv=L"0",TrnsRvsSwSt=L"0",PDAjrSwAtv=L"0",TMActSpd=L"0",DFCShiftLvrPlbtyErr=L"0",OBCInpVolOv=L"0",RemainOdo=L"147",BatMaPosRlySts=L"1",CDUType=L"9",Lattd_G=L"21.867086",DrTorqRqst=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"3",DCDCIntnlFlt=L"0",GearSts_G=L"0",VecChrgStsIndOn=L"0",zdsjxh=L"1830",EvpEquPreVal=L"-55.5",InitaContIndOn_EPS=L"0",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"2",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"11",InvActTemp=L"34",BMSGenSts1RollCnt=L"2",SignalStrengthOne=L"4",StrWhAng=L"0.4375",SignalStrengthTwo=L"24",TDEnblSts=L"0",createTime=L"1678171310017",ACRelayCmd=L"0",BatDvcVlt_G1=L"104.9",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",dqsjxh=L"319",MCUSupCode=L"8330245",HzrdLtIO=L"0",VecActPow=L"-0.1",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",OBCOtpVlt=L"104.5",BatMaxCelVolPos=L"14",t2RTime=L"2023-03-07 14:41:50",InitaContIndOn_OBC=L"0",TalCur_G=L"6",BatAvgTemp=L"23",OBCInpCurOv=L"0",ChargPrtOpInOnPrmt=L"0",ChrgCtrlSnglFail=L"0",KyPstn=L"2",MCUActFltNum=L"0",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"0",BatMinTemp_G=L"23",ModeCode=L"135",BMSChrgCurRqst=L"0",RtTrnLmpAtv=L"0",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",DFCGearReqMongErr=L"0",DFCCASmax=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",BatMinClVltCode_G=L"10",LSDAjrSwAtv=L"0",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",TMWkStsRqst=L"2",OBCOtpVolOv=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"104.9",TMTorqRqst=L"0",ShifGearFailAtHiSpeed=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"25",VehOdo_G=L"15020",BatTempSts=L"0",LowBatVol=L"13.96",VCUBatThrmlRunwyAlmInd=L"0",VecTMActSpd=L"0",DCDCOtpVolSts=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"25",EvrmetAblPreVal=L"101.1",BatIslatRes=L"10001",BatMiniCelVolPos=L"10",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",VecSOC=L"87",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"160",WhlGrndVlctyRtNnDrvn=L"0",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"0",RemtCtrlSpdLmtRqstSt=L"1",VecActPowPer=L"0",BatChrgVolCpl=L"110",InitaContIndOn_IC=L"0",DFCUAcePedlPlbtyErr=L"0",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",VCUCtrlRqst1RollCnt=L"11",ACSwitch=L"1",TMSts_G=L"4",LftTrnLmpAtv=L"0",RealSoc=L"87",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"2.1",Direction=L"0",RemtCtrlPrkHet=L"0",CPSts=L"0",HzrdLgtSwAtv=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",TDAjrSwAtv=L"0",DCDCInpCur=L"2.6",BatTalVolSm=L"104.9",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",VBatChrgStsIndOn=L"1",BatCurSensFlt=L"0",AirbgIndOn=L"0",TMCtrlMdRqst=L"2",OBCInpMxCur=L"0",BatEnrgAvail=L"10.1",BatTempSensNum_G1=L"6",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",IgnKyPstn=L"2",BatDvcCur_G1=L"6",TerminalNO=L"7683202652373453401M6260143",InvVol=L"106",InvJctTemp=L"34",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"6.73",MCUCurFlt=L"0",BatExtVol=L"104.8",DrSbltAtcV=L"0",TMNum_G=L"1",OBCOvTemp=L"0",SupCode=L"1",DFCTioutForVT=L"0",Bat2sPlsChrgPowAvail=L"6.73",vehConfig=L"LV1",VecActGearSts=L"0",VehConf=L"3",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"0",WhlGrndVlctyRtDrvn=L"0",OBCInpVlt=L"0",BatMaxTemp=L"25",EvpRlySta=L"0",IALPwrMdCtrlSt=L"0",TMActTemp=L"31",PlsAgainToRdy=L"0",HandBrkSts=L"1",IslatRes_G=L"10001",KyOpenSwAct=L"0",objType=L"E50",BatContiDischrgPowAvail=L"18",InitaContIndOn_MCU=L"0",VecIslateWrngIndOn=L"0",DFCMCUGen1TiOut=L"0",Bat2sPlsDischrgPowAvail=L"18",MCUCtrlFlt=L"0",BatChrgTims=L"98",VacPumpWrngIndOn=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",InitaContIndOn_BMS=L"0",TalClNum_G1=L"26",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"202",VCUVehDrvMod=L"0",EPSWorkCur=L"0",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"1",ProtocolVersion=L"1_1_1",DrSbltAtc=L"1",BatMiniCelVol=L"4033",TemperoHeatMeb3=L"205",TMDeratingSts=L"0",OBCORDCACIntnlTemp=L"33",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",BatMinTemp=L"23",TemperoHeatMeb6=L"205",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"37",PDUHVLock=L"1",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"0",vehType=L"E50",Longitude=L"111.240798",DFCDCANMUTE=L"0",WhlGrndVlctyLftDrvn=L"0",BrkProcInPrgrsIO=L"0",TMActWkSts=L"3",CDUState=L"5",objId=L"LK6ADAE14MB308114",PTID=L"2",ABSAtv=L"0",EngyRevIndOn=L"0",BrakPedalPos=L"0",TMTemp_G=L"31",BatCelVolSensFlt=L"0",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",BatIslateSts=L"0",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",AnThWaSt=L"1",DCDCOtpVol=L"14.1",RrFgLtIO=L"0",GPSspeed=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"8",BatMinTempCode_G=L"3",CCSts=L"0",IgnKyInstAtv=L"0",Latitude=L"21.867086",BMSHvPowOnRqst=L"2",VecSOCLoWrngIndOn=L"0",TMActDrvTorLmt=L"82.25",BatMaxClVltCode_G=L"14",CollisionSig=L"0",VecStatRdy=L"1",BatMaxCelVol=L"4040",LwBmIO=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",DCDCTempWrng=L"0",BCMRunMod=L"0",BatSubsyst_Vlt_G=L"1",BMSSts=L"3",DriverLeftWarning=L"1",BatRmaChrgTim=L"0",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"6",FtFgLtIO=L"0",BatAvgCelVol=L"4037",Swit2Status=L"0",t1RTime=L"2023-03-07 14:41:48",VacPumpTransRate=L"1",BCMSftwrVsinNum=L"1",BatHetPrsrvtn=L"0",BatOthFltNum=L"0",VecFltIndOn=L"0",VecActTalCur=L"5.9",ISBF=L"0",BatClVlt_G1=L"4.037_4.037_4.036_4.039_4.037_4.039_4.036_4.036_4.036_4.033_4.035_4.035_4.035_4.04_4.039_4.037_4.038_4.039_4.039_4.04_4.039_4.04_4.04_4.04_4.04_4.04",BatFltLvl=L"0",MCUInpVlt_G=L"106",BatMaxTempPos=L"5",TMActFbTorLmt=L"-82.5",DrvPowLimtIndOn=L"0",BatAvgCelVolSts=L"0",OBCOtpCur=L"0",VCUSftwrVsinNum=L"162",DDAjrSwAtv=L"1",TalVlt_G=L"104.9",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"6.73",InvVolSts=L"0",VehOdoV=L"0",VecOptMod=L"3",SoftEdt=L"180",RSDAjrSwAtv=L"0",TMorMCUOvTempInd=L"0",OptMod_G=L"1",MCUTemp_G=L"34",TMSpdRqst=L"0",VecChrgRqst=L"0",VecTMActSpdV=L"0",RLDoorOpenSwAct=L"0",MCUVolFlt=L"0",TMSpd_G=L"0",OBCInpCur=L"0",OBCInpVolUn=L"0",DCDCOtpCur=L"19",BMSChrgVolCurRqstAnorm=L"0",InitaContIndOn_ABS=L"0",CollectTime=L"2023-03-07 14:41:48",BatMinClVlt_G=L"4.033",RatedEgy=L"14",NoPulOutChrgPlugWrng=L"0",PsDoorOpenSwAct=L"0",AntiSlope=L"0",HVRelSta=L"2",MaxWrngLvl_G=L"0",BatheatindON=L"0",AccActPos=L"0",InitaContIndOn_BCM=L"0",TrnSwAct=L"0",VehSts_G=L"1",VehSpdAvgDrvn=L"0",InitaContIndOn_SDM=L"0",HhBmIO=L"0",t02RDelay=L"2",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Lngtd_G=L"111.240798",BatThrmlRunwyAlmInd=L"0",StpLpSt=L"0",PrkBrkSwAtv=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:48",BatMaxClVlt_G=L"4.04",AcInpFlt=L"0",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"6394",BatMinTemVolPakOrd=L"1",BatSOH=L"100",DrivStOccSt=L"0",BatSOC=L"87",VehOdo=L"15020",t2RIP=L"192.168.40.106",BatMaxTempCode_G=L"5",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"-0.1",WindscenWipSt=L"0",LdspkSt=L"0",BatTempSensFlt=L"0",DFCMongHvNotIntd=L"0",DCDCInpVol=L"104.5",WipSwStat=L"1",BatIntVol=L"104.9",VCURemtCtrlSpdLmtFb=L"0",InvrCur=L"0",BatMinTempPos=L"3",VecChrgingSts=L"0",Bat10sPlsDischrgPowAvail=L"18",SftwrMjrVsin=L"8",FtPnLgtAtv=L"0",RatedVol=L"96",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",TMTorq_G=L"-0.3",BatInfLen=L"0",BatChrgSts=L"0",InitaContIndOn_UCU=L"0",BCMRunModV=L"1",SOC_G=L"87",BatSubsysTemp_G1=L"24_24_23_23_25_24",PowBatErrIndOn=L"0",BatSOCSTS=L"0",DCDCInternalTemp=L"37",MaiLgtSw=L"0",MinTempSubSysNum_G=L"1",TMActTorq=L"-0.25",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:48",VehDrvMod=L"1",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",TalNumOfClBatInThisFrm_G1=L"26",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DrDoorOpenSwAct=L"1",DFCDOPIN56Flt=L"0",RemtHeatModRqst=L"0" {1678171308000+count}')
                input_sql_list_tmp.append(input_sql_list1)
            import threading
            tlist = list()
            for input_sql_list in input_sql_list_tmp:
                t = threading.Thread(target=self.tdSql._conn.schemaless_insert,args=(input_sql_list, TDSmlProtocolType.LINE.value, TDSmlTimestampType.MILLI_SECOND.value))
                tlist.append(t)
            for t in tlist:
                t.start()
            for t in tlist:
                t.join()
            count += 1
        self.tdSql.query(f'select count(*) from {self.dbname}.`E50`')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], thread_count*batch_count*loop_times)

    def ts_3053(self):
        lines = ["meters,location=la,groupid=ca current=11.8,voltage=221","meters,location=la,groupid=ca current=11.8,voltage=221,phase=0.27","ts3038,location=l2a,groupid=ca current=L\"11.8\"","ts3038,location=l2a,groupid=ca voltage=L\"221\"","ts3038,location=l2a,groupid=ca phase=L\"221\""]
        self.tdSql._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, None)
        self.tdSql.query('select count(*) from meters;')

    def ts_3146(self):
        lines = ['1000E0DC000124 /NC_LINK_ROOT/MACHINE/CONTROLLER/WARNING="[]",/NC_LINK_ROOT/MACHINE/PART_COUNT=5900000i,/NC_LINK_ROOT/MACHINE/STATUS=1i,/NC_LINK_ROOT/MACHINE/VARIABLE@PROCESS_TIME_RECORD="[]" 1680918783010000000']
        self.tdSql._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, None)
        self.tdSql.query("desc `1000E0DC000124`")
        self.tdSql.checkEqual(self.tdSql.query_data[-1], ('_tag_null', 'NCHAR', 1, 'TAG'))

    def ts_3116(self):
        self.tdSql.execute('drop database if exists iot_dev;')
        self.tdSql.execute('create database if not exists iot_dev precision "ns";')
        self.tdSql.execute('use iot_dev')
        lines = ['meters,location=la,groupid=ca current=11.8,voltage=221']
        self.tdSql._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, None)
        lines = ['meters,location=la,groupid=ca\\=3 current=11.8,voltage=221 1626006833639000000',
                 'meters,location=la,groupid=ca current=11.8,voltage=221,phase=0.27 1626006833639100000']
        self.tdSql._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, None)
        self.tdSql.query("desc `meters`")

    def thread_insert(self, *line_list):
        for line in line_list:
            try:
                self.tdSql._conn.schemaless_insert([line], TDSmlProtocolType.LINE.value, None)
            except SchemalessError:
                pass

    def ts_3264(self):
        """"""
        self.tdSql.execute('drop database if exists iot_dev;')
        self.tdSql.execute('create database if not exists iot_dev precision "ns";')
        self.tdSql.execute('use iot_dev')
        line_list = ['hvlgpibybg,id="hvlgpibybg_33761_28336_1",t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="binaryTagValue",t8=L"ncharTagValue" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="binaryColValue",c8=L"ncharColValue",c9=7u64',
                     'hvlgpibybg,id="hvlgpibybg_33761_28336_1",t0=t,id="hvlgpibybg_33761_28336_2",t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="binaryTagValue",t8=L"ncharTagValue" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="binaryColValue",c8=L"ncharColValue",c9=7u64',
                     'hvlgpibybg,id="hvlgpibybg_33761_28336_1",t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="binaryTagValue",t8=L"ncharTagValue" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="binaryColValue",c8=L"ncharColValue",c9=8u64 1626006833669000000']

        t = threading.Thread(target=self.thread_insert, args=line_list)
        t.start()
        self.tdSql.query("select * from  hvlgpibybg")
        self.tdSql.checkEqual(self.tdSql.query_row, len(line_list)-1)

    def ts_3262(self):
        start_time = time.time()
        self.tdSql.execute('drop database if exists iot_dev;')
        self.tdSql.execute('create database if not exists iot_dev precision "ns";')
        self.tdSql.execute('use iot_dev')
        line_list = ['hvlgpibybg,id="hvlgpibybg_33761_28336_1",t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="binaryTagValue",t8=L"ncharTagValue" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=1i32,c5=11.12345f32,c6=22.123456789f64,c7="binaryColValue",c8=L"ncharColValue",c9=8u64 1626006833669000000',
                     'hvlgpibybg,id="hvlgpibybg_33761_28336_1",t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7="binaryTagValue",t8=L"ncharTagValue" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="binaryColValue",c8=L"ncharColValue",c9=9u64']
        self.tdSql._conn.schemaless_insert([line_list[0]], TDSmlProtocolType.LINE.value, None)
        try:
            self.tdSql._conn.schemaless_insert([line_list[1]], TDSmlProtocolType.LINE.value, None)
        except SchemalessError:
            pass
        self.tdSql.query("select * from  hvlgpibybg")
        self.tdSql.checkEqual(self.tdSql.query_row, len(line_list)-1)
        end_time = time.time()
        self.tdSql.checkEqual(int(end_time-start_time)<2, True)

    def escape_test(self):
        # odd/enen escape for all
        for stbname in ["esca\pe_test", "esca\\pe_test"]:
            lines = f'{stbname},ta\g1="ta\g1_value",ta\\g2="ta\\g2_value" co\l0="co\l0_value",co\\l1="co\\l1_value" 1680918783010000000'
            self.tdSql._conn.schemaless_insert([lines], TDSmlProtocolType.LINE.value, None)
            self.tdSql.query(f'desc `{stbname}`')
            colname_list = self.tdSql.getColNameList()
            self.tdSql.checkEqual(colname_list, ['_ts', 'co\\l0', 'co\\l1', 'ta\\g1', 'ta\\g2'])
            self.tdSql.query(f'select * from `{stbname}`')
            self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2023, 4, 8, 9, 53, 3, 10000), 'co\\l0_value', 'co\\l1_value', '"ta\\g1_value"', '"ta\\g2_value"')])
            self.tdSql.execute(f'drop table `{stbname}`')
        for stbname in ["esca\\\pe_test", "esca\\\\pe_test"]:
            lines = f'{stbname},ta\\\g1="ta\\\g1_value",ta\\\\g2="ta\\\\g2_value" co\\\l0="co\\\l0_value",co\\\\l1="co\\\\l1_value" 1680918783010000000'
            self.tdSql._conn.schemaless_insert([lines], TDSmlProtocolType.LINE.value, None)
            self.tdSql.query(f'desc `{stbname}`')
            colname_list = self.tdSql.getColNameList()
            self.tdSql.checkEqual(colname_list, ['_ts', 'co\\\l0', 'co\\\\l1', 'ta\\\g1', 'ta\\\\g2'])
            self.tdSql.query(f'select * from `{stbname}`')
            self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2023, 4, 8, 9, 53, 3, 10000), 'co\\l0_value', 'co\\l1_value', '"ta\\\\g1_value"', '"ta\\\\g2_value"')])
            self.tdSql.execute(f'drop table `{stbname}`')
        for stbname in ["esca\\\\\pe_test", "esca\\\\\\pe_test"]:
            lines = f'{stbname},ta\\\\\g1="ta\\\\\g1_value",ta\\\\\\g2="ta\\\\\\g2_value" co\\\\\l0="co\\\\\l0_value",co\\\\\\l1="co\\\\\\l1_value" 1680918783010000000'
            self.tdSql._conn.schemaless_insert([lines], TDSmlProtocolType.LINE.value, None)
            self.tdSql.query(f'desc `{stbname}`')
            colname_list = self.tdSql.getColNameList()
            self.tdSql.checkEqual(colname_list, ['_ts', 'co\\\\\l0', 'co\\\\\\l1', 'ta\\\\\g1', 'ta\\\\\\g2'])
            self.tdSql.query(f'select * from `{stbname}`')
            self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2023, 4, 8, 9, 53, 3, 10000), 'co\\\l0_value', 'co\\\l1_value', '"ta\\\\\g1_value"', '"ta\\\\\\g2_value"')])
            self.tdSql.execute(f'drop table `{stbname}`')

        # space for all, but not support field value
        for stbname in ["esca\ pe_test", "esca\\ pe_test"]:
            lines = f'{stbname},ta\ g1="ta\ g1_value",ta\\ g2="ta\\ g2_value" co\ l0="co\ l0_value",co\\ l1="co\\ l1_value" 1680918783010000000'
            self.tdSql._conn.schemaless_insert([lines], TDSmlProtocolType.LINE.value, None)
            self.tdSql.query(f'desc `esca pe_test`')
            colname_list = self.tdSql.getColNameList()
            self.tdSql.checkEqual(colname_list, ['_ts', 'co l0', 'co l1', 'ta g1', 'ta g2'])
            self.tdSql.query(f'select * from `esca pe_test`')
            self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2023, 4, 8, 9, 53, 3, 10000), 'co\\ l0_value', 'co\\ l1_value', '"ta g1_value"', '"ta g2_value"')])
            self.tdSql.execute(f'drop table `esca pe_test`')
        for stbname in ["esca\\\ pe_test", "esca\\\\ pe_test"]:
            lines = f'{stbname},ta\\\ g1="ta\\\ g1_value",ta\\\\ g2="ta\\ g2_value" co\\\ l0="co\\\ l0_value",co\\\\ l1="co\\\\ l1_value" 1680918783010000000'
            self.tdSql._conn.schemaless_insert([lines], TDSmlProtocolType.LINE.value, None)
            self.tdSql.query(f'desc `esca\\ pe_test`')
            colname_list = self.tdSql.getColNameList()
            self.tdSql.checkEqual(colname_list, ['_ts', 'co\\ l0', 'co\\ l1', 'ta\\ g1', 'ta\\ g2'])
            self.tdSql.query(f'select * from `esca\\ pe_test`')
            self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2023, 4, 8, 9, 53, 3, 10000), 'co\\ l0_value', 'co\\ l1_value', '"ta\\ g1_value"', '"ta g2_value"')])
            self.tdSql.execute(f'drop table `esca\\ pe_test`')

        # comma/Equals Sign for tag key/tag value/field key
        for i in [" ", ","]:
            for stbname in ["esca\,pe_test", "esca\\,pe_test"]:
                lines = f'{stbname},ta\{i}g1="ta\{i}g1_value",ta\\{i}g2="ta\\{i}g2_value" co\{i}l0="co\{i}l0_value",co\\{i}l1="co\\{i}l1_value" 1680918783010000000'
                self.tdSql._conn.schemaless_insert([lines], TDSmlProtocolType.LINE.value, None)
                self.tdSql.query(f'desc `esca,pe_test`')
                colname_list = self.tdSql.getColNameList()
                self.tdSql.checkEqual(colname_list, ['_ts', f'co{i}l0', f'co{i}l1', f'ta{i}g1', f'ta{i}g2'])
                self.tdSql.query(f'select * from `esca,pe_test`')
                self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2023, 4, 8, 9, 53, 3, 10000), f'co\\{i}l0_value', f'co\\{i}l1_value', f'"ta{i}g1_value"', f'"ta{i}g2_value"')])
                self.tdSql.execute(f'drop table `esca,pe_test`')
            for stbname in ["esca\\\,pe_test", "esca\\\\,pe_test"]:
                lines = f'{stbname},ta\\\{i}g1="ta\\\{i}g1_value",ta\\\\{i}g2="ta\\{i}g2_value" co\\\{i}l0="co\\\{i}l0_value",co\\\\{i}l1="co\\\\{i}l1_value" 1680918783010000000'
                self.tdSql._conn.schemaless_insert([lines], TDSmlProtocolType.LINE.value, None)
                self.tdSql.query(f'desc `esca\\,pe_test`')
                colname_list = self.tdSql.getColNameList()
                self.tdSql.checkEqual(colname_list, ['_ts', f'co\\{i}l0', f'co\\{i}l1', f'ta\\{i}g1', f'ta\\{i}g2'])
                self.tdSql.query(f'select * from `esca\\,pe_test`')
                self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2023, 4, 8, 9, 53, 3, 10000), f'co\\{i}l0_value', f'co\\{i}l1_value', f'"ta\\{i}g1_value"', f'"ta{i}g2_value"')])
                self.tdSql.execute(f'drop table `esca\\,pe_test`')
        # double quote for all, but only support field value
        lines = 'esca"pe_test,ta"g1="ta"g1_value",ta\"g2="ta\"g2_value" co"l0="co\\"l\\"0_value",co\"l1="col1_value" 1680918783010000000'
        self.tdSql._conn.schemaless_insert([lines], TDSmlProtocolType.LINE.value, None)
        self.tdSql.query(f'desc `esca"pe_test`')
        colname_list = self.tdSql.getColNameList()
        self.tdSql.checkEqual(colname_list, ['_ts', 'co"l0', 'co"l1', 'ta"g1', 'ta"g2'])
        self.tdSql.query(f'select * from `esca"pe_test`')
        self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2023, 4, 8, 9, 53, 3, 10000), 'co"l"0_value', 'col1_value', '"ta"g1_value"', '"ta"g2_value"')])
        self.tdSql.execute(f'drop table `esca"pe_test`')

        # mix
        lines = 'e\,s\ ca"p\\\,e_t\\\ est,t\,\\\,a\=\\\=g\ \\\ "1="t\,a\\\,g\=1\\\=_\ v\\\ "alue",tag2="tag2_value" c\,\\\,o\=\\\=l\ \\\ "0="c\,\\\,o\=\\\=l\ \\\ 0_val\\"u\\"e",co\"l1="col1_value" 1680918783010000000'
        self.tdSql._conn.schemaless_insert([lines], TDSmlProtocolType.LINE.value, None)
        self.tdSql.query(f'desc `e,s ca"p\,e_t\ est`')
        colname_list = self.tdSql.getColNameList()
        self.tdSql.checkEqual(colname_list, ['_ts', 'c,\\,o=\\=l \\ "0', 'co"l1', 't,\\,a=\\=g \\ "1', 'tag2'])
        self.tdSql.query(f'select * from `e,s ca"p\,e_t\ est`')
        self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2023, 4, 8, 9, 53, 3, 10000), 'c\\,\\,o\\=\\=l\\ \\ 0_val"u"e', 'col1_value', '"t,a\\,g=1\\=_ v\\ "alue"', '"tag2_value"')])
        self.tdSql.execute(f'drop table `e,s ca"p\,e_t\ est`')

        # ilegal test
        line_list = [
                     'esca pe_test,tag1="tag1_value",tag2="tag2_value" col0="col0_value",col1="col1_value" 1680918783010000000',
                     'esca,pe_test,tag1="tag1_value",tag2="tag2_value" col0="col0_value",col1="col1_value" 1680918783010000000',
                     'escape_test,ta,g1="tag1_value",tag2="tag2_value" col0="col0_value",col1="col1_value" 1680918783010000000',
                     'escape_test,ta=g1="tag1_value",tag2="tag2_value" col0="col0_value",col1="col1_value" 1680918783010000000',
                     'escape_test,ta g1="tag1_value",tag2="tag2_value" col0="col0_value",col1="col1_value" 1680918783010000000',
                     'escape_test,tag1="ta,g1_value",tag2="tag2_value" col0="col0_value",col1="col1_value" 1680918783010000000',
                     'escape_test,tag1="ta=g1_value",tag2="tag2_value" col0="col0_value",col1="col1_value" 1680918783010000000',
                     'escape_test,tag1="ta g1_value",tag2="tag2_value" col0="col0_value",col1="col1_value" 1680918783010000000',
                     'escape_test,tag1="tag1_value",tag2="tag2_value" co,l0="col0_value",col1="col1_value" 1680918783010000000',
                     'escape_test,tag1="tag1_value",tag2="tag2_value" co=l0="col0_value",col1="col1_value" 1680918783010000000',
                     'escape_test,tag1="tag1_value",tag2="tag2_value" col0="co\"l"0_value",col1="col1_value" 1680918783010000000'
                     'escape_test,tag1="tag1_value",tag2="tag2_value" co l0="col0_value",col1="col1_value" 1680918783010000000',
                     'esca"pe_test,ta"g1="ta"g1_value",ta\"g2="ta\"g2_value" co"l0="co\"l\\"0_value",co\"l1="col1_value" 1680918783010000000',
                     'esca"pe_test,ta"g1="ta"g1_value",ta\"g2="ta\"g2_value" co"l0="co"l\\"0_value",co\"l1="col1_value" 1680918783010000000',
                     'esca"pe_test,ta"g1="ta"g1_value",ta\"g2="ta\"g2_value" co"l0="col\"0_value",co\"l1="col1_value" 1680918783010000000',
                     ]
        for line in line_list:
            try:
                self.tdSql._conn.schemaless_insert([line], TDSmlProtocolType.LINE.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

    def test(self):
        self.escape_test()
        # self.s_stb_d_tb_d_data_d_ts_ac_mt_insert_multi_thread_check()
        return
        self.tdSql.execute('drop database if exists iot_dev;')
        self.tdSql.execute('create database if not exists iot_dev precision "ns";')
        self.tdSql.execute('use iot_dev')
        lines = ['shadow_history_IMaT6DPrF26v8474,realm_device_id=58_bwjewX2Ytmst Q=560,Pa=706,Uca=200,Pc=635,Ic=81,Ia=89,Sa=250,Qa=397,Uab=759,Sc=562,InPw=389,Pw=224.65,Qv=80,Qb=600,InQvr=742,InPwr=224.19,S=931,P=224.98,Fr=249,Uc=171,Pf=386,InQv=173,Pb=128,Sb=595,Ubc=765,Qc=544,Ub=250,Ib=575,Ua=84 1626006833669000000']
        self.tdSql._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, None)
        self.tdSql.execute('drop table `shadow_history_IMaT6DPrF26v8474`')
        lines = ['shadow_history_IMaT6DPrF26v8474,realm_device_id=58_bwjewX2Ytmst Q=143,Ic=912,Ib=69,Pa=749,Ia=189,InQvr=561,InPwr=227.78,Qa=661,P=221.02,Ubc=648,InPw=19,Pb=447,Uca=868,Sc=834,Sb=1008,Pf=213,Pc=195,Uc=557,Pw=220.41,Ub=957,Qb=747,Qv=739,S=419,Sa=635,Fr=926,Uab=635,Ua=477,InQv=814,Qc=483 1626006833669000000']
        self.tdSql._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, None)
        self.tdSql.query('select * from `shadow_history_IMaT6DPrF26v8474`')
        self.tdSql.execute('drop table `shadow_history_IMaT6DPrF26v8474`')
        lines = ['shadow_history_IMaT6DPrF26v8474,realm_device_id=58_bwjewX2Ytmst InPwr=228.65,P=227.79,Ubc=1000,Pc=109,Pw=225.03,Sc=400,Ua=403,Pa=967,Ub=787,Sa=892,Ic=516,Ia=340,InQv=207,Qv=767,Uc=540,Pf=233,Fr=567,Qc=778,Qb=496,Ib=992,Pb=943,Q=172,Qa=55,S=68,InPw=479,Sb=426,Uca=750,InQvr=985,Uab=209 1626006833669000000']
        self.tdSql._conn.schemaless_insert(lines, TDSmlProtocolType.LINE.value, None)
        self.tdSql.query('select * from `shadow_history_IMaT6DPrF26v8474`')
        self.tdSql.execute('drop table `shadow_history_IMaT6DPrF26v8474`')

        # self.tag_col_binary_max_length_check()
        # self.tdSql.query(f'select * from xx')
        # input_sql = 'reported_j1WhBe0W78Edj6hK,realm_device_id=test_device_id_001 Ia=10.01f32,P=1.32012f32,Ib=9.0100001f32,Ia_source_time=1677834213374i64,P_source_time=1677834213374i64,Ib_source_time=1677834213374i64 1677834213374'
        # input_sql = 'device_reported_qe6N8Di0WSgKaW4s,realm_device_id=241_ow0xnTjY7HrN kWDmdMax=0.0f64,Qv=30949.17f64,kWTotDmd=6867.977f64,Pfa=0.849f64,kvarhNet=27642.12f64,Iavg=11.129f64,Pfc=0.856f64,Pfb=0.869f64,InPw=22434.94f64,P=6.862864f64,Q=-3.950617f64,Pa=2.316923f64,Pb=2.36111f64,S=7.918729f64,Pc=2.184831f64,Pf=0.867f64,Ubc=410.911f64,Uunb=0.0f64,InQvr=1653.53f64,DMDmaxTime=7942.826f64,Iunb=0.032f64,kVAh=38571.08f64,IaRtDmd=11.325f64,Pw=22437.54f64,InQv=29295.64f64,Ulnavg=237.224f64,Fr=50.012f64,Ua=237.205f64,Ullavg=410.884f64,Ub=237.258f64,Uc=237.209f64,Sa=2.707169f64,Sb=2.701234f64,IcRtDmd=10.844f64,Qa=-1.399694f64,Sc=2.511948f64,InPwr=2.6f64,kWhNet=22432.33f64,Qb=-1.311908f64,Uca=410.824f64,Qc=-1.239016f64,Uab=410.916f64,Ia=11.413f64,Ib=11.385f64,Ic=10.59f64,IbRtDmd=11.357f64,Inc=3.239f64,kWDmdMax_source_time=1678259700000i64,Qv_source_time=1678259700000i64,kWTotDmd_source_time=1678259700000i64,Pfa_source_time=1678259700000i64,kvarhNet_source_time=1678259700000i64,Iavg_source_time=1678259700000i64,Pfc_source_time=1678259700000i64,Pfb_source_time=1678259700000i64,InPw_source_time=1678259700000i64,P_source_time=1678259700000i64,Q_source_time=1678259700000i64,Pa_source_time=1678259700000i64,Pb_source_time=1678259700000i64,S_source_time=1678259700000i64,Pc_source_time=1678259700000i64,Pf_source_time=1678259700000i64,Ubc_source_time=1678259700000i64,Uunb_source_time=1678259700000i64,InQvr_source_time=1678259700000i64,DMDmaxTime_source_time=1678259700000i64,Iunb_source_time=1678259700000i64,kVAh_source_time=1678259700000i64,IaRtDmd_source_time=1678259700000i64,Pw_source_time=1678259700000i64,InQv_source_time=1678259700000i64,Ulnavg_source_time=1678259700000i64,Fr_source_time=1678259700000i64,Ua_source_time=1678259700000i64,Ullavg_source_time=1678259700000i64,Ub_source_time=1678259700000i64,Uc_source_time=1678259700000i64,Sa_source_time=1678259700000i64,Sb_source_time=1678259700000i64,IcRtDmd_source_time=1678259700000i64,Qa_source_time=1678259700000i64,Sc_source_time=1678259700000i64,InPwr_source_time=1678259700000i64,kWhNet_source_time=1678259700000i64,Qb_source_time=1678259700000i64,Uca_source_time=1678259700000i64,Qc_source_time=1678259700000i64,Uab_source_time=1678259700000i64,Ia_source_time=1678259700000i64,Ib_source_time=1678259700000i64,Ic_source_time=1678259700000i64,IbRtDmd_source_time=1678259700000i64,Inc_source_time=1678259700000i64 1678259705937'
        # input_sql = 'device_reported_qe6N8Di0WSgKaW4s,realm_device_id=241_ow0xnTjY7HrN kWDmdMax=0.0f64,Qv=30949.17f64,kWTotDmd=6867.977f64,Pfa=0.849f64,kvarhNet=27642.12f64,Iavg=11.129f64,Pfc=0.856f64,Pfb=0.869f64,InPw=22434.94f64,P=6.862864f64,Q=-3.950617f64,Pa=2.316923f64,Pb=2.36111f64,S=7.918729f64,Pc=2.184831f64,Pf=0.867f64,Ubc=410.911f64,Uunb=0.0f64,InQvr=1653.53f64,DMDmaxTime=7942.826f64,Iunb=0.032f64,kVAh=38571.08f64,IaRtDmd=11.325f64,Pw=22437.54f64,InQv=29295.64f64,Ulnavg=237.224f64,Fr=50.012f64,Ua=237.205f64,Ullavg=410.884f64,Ub=237.258f64,Uc=237.209f64,Sa=2.707169f64,Sb=2.701234f64,IcRtDmd=10.844f64,Qa=-1.399694f64,Sc=2.511948f64,InPwr=2.6f64,kWhNet=22432.33f64,Qb=-1.311908f64,Uca=410.824f64,Qc=-1.239016f64,Uab=410.916f64,Ia=11.413f64,Ib=11.385f64,Ic=10.59f64,IbRtDmd=11.357f64,Inc=3.239f64,kWDmdMax_source_time=1678259700000i64,Qv_source_time=1678259700000i64,kWTotDmd_source_time=1678259700000i64,Pfa_source_time=1678259700000i64,kvarhNet_source_time=1678259700000i64,Iavg_source_time=1678259700000i64,Pfc_source_time=1678259700000i64,Pfb_source_time=1678259700000i64,InPw_source_time=1678259700000i64,P_source_time=1678259700000i64,Q_source_time=1678259700000i64,Pa_source_time=1678259700000i64,Pb_source_time=1678259700000i64,S_source_time=1678259700000i64,Pc_source_time=1678259700000i64,Pf_source_time=1678259700000i64,Ubc_source_time=1678259700000i64,Uunb_source_time=1678259700000i64,InQvr_source_time=1678259700000i64,DMDmaxTime_source_time=1678259700000i64,Iunb_source_time=1678259700000i64,kVAh_source_time=1678259700000i64,IaRtDmd_source_time=1678259700000i64,Pw_source_time=1678259700000i64,InQv_source_time=1678259700000i64,Ulnavg_source_time=1678259700000i64,Fr_source_time=1678259700000i64,Ua_source_time=1678259700000i64,Ullavg_source_time=1678259700000i64,Ub_source_time=1678259700000i64,Uc_source_time=1678259700000i64,Sa_source_time=1678259700000i64,Sb_source_time=1678259700000i64,IcRtDmd_source_time=1678259700000i64,Qa_source_time=1678259700000i64,Sc_source_time=1678259700000i64,InPwr_source_time=1678259700000i64,kWhNet_source_time=1678259700000i64,Qb_source_time=1678259700000i64,Uca_source_time=1678259700000i64,Qc_source_time=1678259700000i64,Uab_source_time=1678259700000i64,Ia_source_time=1678259700000i64,Ib_source_time=1678259700000i64,Ic_source_time=1678259700000i64,IbRtDmd_source_time=1678259700000i64,Inc_source_time=1678259700000i64 1678259705937'
        # input_sql = ['E50,VIN=LK6ADCE12MB210131 TalClNum_G1=L"28",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"61",VCUVehDrvMod=L"0",BMSCode=L"0",EPSWorkCur=L"0",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",TMOrd_G=L"1",DrSbltAtc=L"0",RrImpctDet=L"0",BatMiniCelVol=L"3334",OBCORDCACIntnlTemp=L"54",TMDeratingSts=L"0",TemperoHeatMeb3=L"205",BattCellVoltage_16=L"3.337",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",TemperoHeatMeb6=L"205",BatMinTemp=L"25",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"205",PDUHVLock=L"1",BattCellVoltage_17=L"3.337",BatFltNumN1_G=L"0",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"1",BatMaNegRlySts=L"1",VecActTalVol=L"93.4",MCUCur_G=L"0",vehType=L"E50",Longitude=L"108.491185",WhlGrndVlctyLftDrvn=L"0",BatPowSts=L"1",DFCDCANMUTE=L"0",TMActWkSts=L"2",BrkProcInPrgrsIO=L"0",CDUState=L"2",TMActSpd=L"0",objId=L"LK6ADCE12MB210131",PTID=L"2",DFCShiftLvrPlbtyErr=L"0",ABSAtv=L"0",OBCInpVolOv=L"0",EngyRevIndOn=L"0",RemainOdo=L"58",BrakPedalPos=L"0",BatMaPosRlySts=L"1",TMTemp_G=L"24",BatCelVolSensFlt=L"0",CDUType=L"9",Lattd_G=L"21.651174",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",DrTorqRqst=L"0",BatIslateSts=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"1",DCDCIntnlFlt=L"0",GearSts_G=L"15",VecChrgStsIndOn=L"1",zdsjxh=L"1514",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",EvpEquPreVal=L"-47.9",InitaContIndOn_EPS=L"1",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"1",DCDCOtpVol=L"14",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"15",InvActTemp=L"26",BMSGenSts1RollCnt=L"1",GPSspeed=L"0",SignalStrengthOne=L"4",StrWhAng=L"-0.9375",SignalStrengthTwo=L"29",RemtCtrParkHeat=L"0",createTime=L"1678171310001",ACRelayCmd=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"9",BatMinTempCode_G=L"2",CCSts=L"2",BatDvcVlt_G1=L"93.3",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",MCUSupCode=L"8330245",dqsjxh=L"569",Latitude=L"21.651174",VecActPow=L"0",BMSHvPowOnRqst=L"2",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",VecSOCLoWrngIndOn=L"0",OBCOtpVlt=L"94.3",BatMaxCelVolPos=L"4",t2RTime=L"2023-03-07 14:41:50",TMActDrvTorLmt=L"0",InitaContIndOn_OBC=L"0",TalCur_G=L"-15.5",BatAvgTemp=L"25",OBCInpCurOv=L"0",BatMaxClVltCode_G=L"4",CollisionSig=L"0",VecStatRdy=L"0",BatMaxCelVol=L"3337",ChrgCtrlSnglFail=L"0",KyPstn=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",MCUActFltNum=L"0",DCDCTempWrng=L"0",BatSubsyst_Vlt_G=L"1",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"0",BMSSts=L"2",DriverLeftWarning=L"0",BatMinTemp_G=L"25",BatRmaChrgTim=L"237",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",ModeCode=L"136",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"-15.5",BMSChrgCurRqst=L"40",Swit2Status=L"1",BatAvgCelVol=L"3336",t1RTime=L"2023-03-07 14:41:49",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",VacPumpTransRate=L"1",DFCGearReqMongErr=L"0",BatHetPrsrvtn=L"0",DFCCASmax=L"0",BatOthFltNum=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",VecFltIndOn=L"0",BatMinClVltCode_G=L"1",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",VecActTalCur=L"-15.4",ISBF=L"0",BatClVlt_G1=L"3.335_3.336_3.336_3.337_3.336_3.336_3.336_3.335_3.335_3.337_3.336_3.336_3.336_3.336_3.336_3.337_3.337_3.337_3.336_3.336_3.336_3.337_3.337_3.337_3.337_3.336_3.336_3.335",BatFltLvl=L"0",MCUInpVlt_G=L"94",TMWkStsRqst=L"0",BatMaxTempPos=L"6",TMActFbTorLmt=L"0",DrvPowLimtIndOn=L"0",OBCOtpVolOv=L"0",BatAvgCelVolSts=L"0",VCUSftwrVsinNum=L"232",OBCOtpCur=L"15.9",BatTalVolSts=L"0",BMSChrgVolRqst=L"103.2",TalVlt_G=L"93.3",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"11",TMTorqRqst=L"0",InvVolSts=L"0",VehOdoV=L"0",ShifGearFailAtHiSpeed=L"0",VecOptMod=L"4",SoftEdt=L"155",TMorMCUOvTempInd=L"0",OptMod_G=L"1",RemtCtrlSpdLimtRqstSt=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"24",VehOdo_G=L"24467",MCUTemp_G=L"26",TMSpdRqst=L"0",LowBatVol=L"14.01",BatTempSts=L"0",VecChrgRqst=L"1",VecTMActSpdV=L"0",VecTMActSpd=L"0",VCUBatThrmlRunwyAlmInd=L"0",MCUVolFlt=L"0",DCDCOtpVolSts=L"0",TMSpd_G=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"27",EvrmetAblPreVal=L"101.4",OBCInpCur=L"8",BatIslatRes=L"10016",BatMiniCelVolPos=L"1",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",OBCInpVolUn=L"0",VecSOC=L"49",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCOtpCur=L"2.7",InitaContIndOn_ABS=L"1",BMSChrgVolCurRqstAnorm=L"0",CollectTime=L"2023-03-07 14:41:49",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"175",BatMinClVlt_G=L"3.334",RatedEgy=L"9.5",WhlGrndVlctyRtNnDrvn=L"0",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"0",NoPulOutChrgPlugWrng=L"0",AntiSlope=L"0",HVRelSta=L"0",VecActPowPer=L"0",BatChrgVolCpl=L"103.2",MaxWrngLvl_G=L"0",InitaContIndOn_IC=L"1",BatheatindON=L"0",DFCUAcePedlPlbtyErr=L"0",AccActPos=L"0",PTCSwitch=L"2",MaxVltBatSubSysNum_G=L"1",ACSwitch=L"0",VCUCtrlRqst1RollCnt=L"6",TMSts_G=L"4",VehSts_G=L"2",VehSpdAvgDrvn=L"0",InitaContIndOn_SDM=L"1",RealSoc=L"50",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"2",t02RDelay=L"1",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"0",Direction=L"0",Lngtd_G=L"108.491185",RemtCtrlPrkHet=L"0",CPSts=L"2",BatThrmlRunwyAlmInd=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:49",DCDCInpCur=L"0.4",BatTalVolSm=L"93.4",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",BatMaxClVlt_G=L"3.337",AcInpFlt=L"0",VBatChrgStsIndOn=L"1",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"9792",BatCurSensFlt=L"0",AirbgIndOn=L"0",BatMinTemVolPakOrd=L"1",TMCtrlMdRqst=L"2",OBCInpMxCur=L"8",DrivStOccSt=L"1",BatSOH=L"97",BatSOC=L"49",BatEnrgAvail=L"3.7",VehOdo=L"24467",t2RIP=L"192.168.10.106",BatTempSensNum_G1=L"6",BatMaxTempCode_G=L"6",TmnlWkupSurc=L"3",OBCHdErr=L"0",TMActPow=L"0",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",BatTempSensFlt=L"0",BatDvcCur_G1=L"-15.5",TerminalNO=L"7681100322375855700M1130373",InvVol=L"94",InvJctTemp=L"26",DFCMongHvNotIntd=L"0",DCDCInpVol=L"94.3",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"11",MCUCurFlt=L"0",BatExtVol=L"93.2",DrSbltAtcV=L"0",BatIntVol=L"93.4",TMNum_G=L"1",OBCOvTemp=L"0",VCURemtCtrlSpdLmtFb=L"0",SupCode=L"1",DFCTioutForVT=L"0",InvrCur=L"0",Bat2sPlsChrgPowAvail=L"11",vehConfig=L"LV1",VecActGearSts=L"3",VehConf=L"3",BatMinTempPos=L"2",CellBattNum_21=L"21",BalanceSts=L"1",WhlGrndVlctyLftNnDrvn=L"0",VecChrgingSts=L"1",Bat10sPlsDischrgPowAvail=L"15.96",WhlGrndVlctyRtDrvn=L"0",SftwrMjrVsin=L"7",OBCInpVlt=L"218",RatedVol=L"90",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",BatMaxTemp=L"27",EvpRlySta=L"0",TMTorq_G=L"0",BatInfLen=L"0",TMActTemp=L"24",PlsAgainToRdy=L"0",BatChrgSts=L"1",HandBrkSts=L"1",InitaContIndOn_UCU=L"1",SOC_G=L"49",IslatRes_G=L"10016",objType=L"E50",BatSubsysTemp_G1=L"26_25_26_25_25_27",BatContiDischrgPowAvail=L"15.96",InitaContIndOn_MCU=L"1",PowBatErrIndOn=L"0",BatSOCSTS=L"0",VecIslateWrngIndOn=L"0",DCDCInternalTemp=L"39",DFCMCUGen1TiOut=L"0",MinTempSubSysNum_G=L"1",Bat2sPlsDischrgPowAvail=L"15.96",TMActTorq=L"0",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:49",VehDrvMod=L"1",MCUCtrlFlt=L"0",BatChrgTims=L"255",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",VacPumpWrngIndOn=L"0",TalNumOfClBatInThisFrm_G1=L"28",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DFCDOPIN56Flt=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",RemtHeatModRqst=L"0",InitaContIndOn_BMS=L"1" 1678171309000',
        #              'E50,VIN=LK6ADCE15LE203512 TalClNum_G1=L"28",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"133",VCUVehDrvMod=L"0",BMSCode=L"0",EPSWorkCur=L"3.25",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",TMOrd_G=L"1",DrSbltAtc=L"1",RrImpctDet=L"0",BatMiniCelVol=L"3297",OBCORDCACIntnlTemp=L"32",TMDeratingSts=L"0",TemperoHeatMeb3=L"205",BattCellVoltage_16=L"3.304",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",TemperoHeatMeb6=L"205",BatMinTemp=L"24",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"205",PDUHVLock=L"1",BattCellVoltage_17=L"3.307",BatFltNumN1_G=L"0",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"92.5",MCUCur_G=L"-10",vehType=L"E50",Longitude=L"113.999754",WhlGrndVlctyLftDrvn=L"19.49",BatPowSts=L"1",DFCDCANMUTE=L"0",TMActWkSts=L"3",BrkProcInPrgrsIO=L"0",CDUState=L"5",TMActSpd=L"1483",objId=L"LK6ADCE15LE203512",PTID=L"2",DFCShiftLvrPlbtyErr=L"0",ABSAtv=L"0",OBCInpVolOv=L"0",EngyRevIndOn=L"1",RemainOdo=L"74",BrakPedalPos=L"0",BatMaPosRlySts=L"1",TMTemp_G=L"31",BatCelVolSensFlt=L"0",CDUType=L"9",Lattd_G=L"32.992669",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",DrTorqRqst=L"-6",BatIslateSts=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"3",DCDCIntnlFlt=L"0",GearSts_G=L"14",VecChrgStsIndOn=L"0",zdsjxh=L"595",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",EvpEquPreVal=L"-67.2",InitaContIndOn_EPS=L"0",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"1",DCDCOtpVol=L"14",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"10",InvActTemp=L"27",BMSGenSts1RollCnt=L"6",GPSspeed=L"0",SignalStrengthOne=L"4",StrWhAng=L"-12.25",SignalStrengthTwo=L"31",RemtCtrParkHeat=L"0",createTime=L"1678171310020",ACRelayCmd=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"1",BatMinTempCode_G=L"1",CCSts=L"0",BatDvcVlt_G1=L"92.5",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",MCUSupCode=L"8330245",dqsjxh=L"69",Latitude=L"32.992669",VecActPow=L"-1",BMSHvPowOnRqst=L"2",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",VecSOCLoWrngIndOn=L"0",OBCOtpVlt=L"92.6",BatMaxCelVolPos=L"10",t2RTime=L"2023-03-07 14:41:50",TMActDrvTorLmt=L"82",InitaContIndOn_OBC=L"1",TalCur_G=L"-8.6",BatAvgTemp=L"24",OBCInpCurOv=L"0",BatMaxClVltCode_G=L"10",CollisionSig=L"0",VecStatRdy=L"1",BatMaxCelVol=L"3311",ChrgCtrlSnglFail=L"0",KyPstn=L"2",BatPrechrgRlySts=L"0",BatFuSts=L"0",MCUActFltNum=L"0",DCDCTempWrng=L"0",BatSubsyst_Vlt_G=L"1",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"19.9",BMSSts=L"3",DriverLeftWarning=L"0",BatMinTemp_G=L"24",BatRmaChrgTim=L"0",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",ModeCode=L"136",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"-8.6",BMSChrgCurRqst=L"0",Swit2Status=L"0",BatAvgCelVol=L"3306",t1RTime=L"2023-03-07 14:41:50",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",VacPumpTransRate=L"1",DFCGearReqMongErr=L"0",BatHetPrsrvtn=L"1",DFCCASmax=L"0",BatOthFltNum=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",VecFltIndOn=L"0",BatMinClVltCode_G=L"21",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",VecActTalCur=L"-8.8",ISBF=L"0",BatClVlt_G1=L"3.302_3.303_3.308_3.303_3.308_3.308_3.306_3.302_3.307_3.311_3.31_3.308_3.307_3.306_3.307_3.304_3.307_3.305_3.31_3.309_3.298_3.308_3.304_3.299_3.305_3.303_3.308_3.308",BatFltLvl=L"0",MCUInpVlt_G=L"93",TMWkStsRqst=L"2",BatMaxTempPos=L"6",TMActFbTorLmt=L"-82.5",DrvPowLimtIndOn=L"0",OBCOtpVolOv=L"0",BatAvgCelVolSts=L"0",VCUSftwrVsinNum=L"228",OBCOtpCur=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"92.5",TalVlt_G=L"92.5",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"9.34",TMTorqRqst=L"-6",InvVolSts=L"0",VehOdoV=L"0",ShifGearFailAtHiSpeed=L"0",VecOptMod=L"3",SoftEdt=L"153",TMorMCUOvTempInd=L"0",OptMod_G=L"1",RemtCtrlSpdLimtRqstSt=L"1",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"22",VehOdo_G=L"38343",MCUTemp_G=L"27",TMSpdRqst=L"0",LowBatVol=L"14.15",BatTempSts=L"0",VecChrgRqst=L"0",VecTMActSpdV=L"0",VecTMActSpd=L"1493",VCUBatThrmlRunwyAlmInd=L"0",MCUVolFlt=L"0",DCDCOtpVolSts=L"0",TMSpd_G=L"1483",BatChrgCurSts=L"0",BatMaxTemp_G=L"25",EvrmetAblPreVal=L"100",OBCInpCur=L"0",BatIslatRes=L"10024",BatMiniCelVolPos=L"21",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",OBCInpVolUn=L"0",VecSOC=L"65",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCOtpCur=L"5.1",InitaContIndOn_ABS=L"0",BMSChrgVolCurRqstAnorm=L"0",CollectTime=L"2023-03-07 14:41:50",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"157",BatMinClVlt_G=L"3.297",RatedEgy=L"9.5",WhlGrndVlctyRtNnDrvn=L"19.71",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"19.625",NoPulOutChrgPlugWrng=L"0",AntiSlope=L"0",HVRelSta=L"2",VecActPowPer=L"-5",BatChrgVolCpl=L"103.2",MaxWrngLvl_G=L"0",InitaContIndOn_IC=L"0",BatheatindON=L"0",DFCUAcePedlPlbtyErr=L"0",AccActPos=L"0",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",ACSwitch=L"0",VCUCtrlRqst1RollCnt=L"12",TMSts_G=L"4",VehSts_G=L"1",VehSpdAvgDrvn=L"19.921875",InitaContIndOn_SDM=L"0",RealSoc=L"68",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"3.4",t02RDelay=L"0",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"0",Direction=L"0",Lngtd_G=L"113.999754",RemtCtrlPrkHet=L"0",CPSts=L"0",BatThrmlRunwyAlmInd=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:50",DCDCInpCur=L"0.8",BatTalVolSm=L"92.5",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",BatMaxClVlt_G=L"3.311",AcInpFlt=L"0",VBatChrgStsIndOn=L"1",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"8887",BatCurSensFlt=L"0",AirbgIndOn=L"0",BatMinTemVolPakOrd=L"1",TMCtrlMdRqst=L"2",OBCInpMxCur=L"12",DrivStOccSt=L"1",BatSOH=L"95",BatSOC=L"65",BatEnrgAvail=L"4.9",VehOdo=L"38343",t2RIP=L"192.168.20.103",BatTempSensNum_G1=L"6",BatMaxTempCode_G=L"6",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"-1",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",BatTempSensFlt=L"0",BatDvcCur_G1=L"-8.6",TerminalNO=L"7681100322366106200L8252662",InvVol=L"93",InvJctTemp=L"28",DFCMongHvNotIntd=L"0",DCDCInpVol=L"92.6",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"9.34",MCUCurFlt=L"0",BatExtVol=L"92.4",DrSbltAtcV=L"0",BatIntVol=L"92.5",TMNum_G=L"1",OBCOvTemp=L"0",VCURemtCtrlSpdLmtFb=L"0",SupCode=L"0",DFCTioutForVT=L"0",InvrCur=L"-10",Bat2sPlsChrgPowAvail=L"9.34",vehConfig=L"LV1",VecActGearSts=L"1",VehConf=L"3",BatMinTempPos=L"1",CellBattNum_21=L"21",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"19.51",VecChrgingSts=L"0",Bat10sPlsDischrgPowAvail=L"16",WhlGrndVlctyRtDrvn=L"19.68",SftwrMjrVsin=L"4",OBCInpVlt=L"0",RatedVol=L"90",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",BatMaxTemp=L"25",EvpRlySta=L"0",TMTorq_G=L"-6.5",BatInfLen=L"0",TMActTemp=L"31",PlsAgainToRdy=L"0",BatChrgSts=L"0",HandBrkSts=L"0",InitaContIndOn_UCU=L"0",SOC_G=L"65",IslatRes_G=L"10024",objType=L"E50",BatSubsysTemp_G1=L"24_24_24_24_24_25",BatContiDischrgPowAvail=L"16",InitaContIndOn_MCU=L"0",PowBatErrIndOn=L"0",BatSOCSTS=L"0",VecIslateWrngIndOn=L"0",DCDCInternalTemp=L"30",DFCMCUGen1TiOut=L"0",MinTempSubSysNum_G=L"1",Bat2sPlsDischrgPowAvail=L"16",TMActTorq=L"-6.5",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:50",VehDrvMod=L"1",MCUCtrlFlt=L"0",BatChrgTims=L"338",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",VacPumpWrngIndOn=L"0",TalNumOfClBatInThisFrm_G1=L"28",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DFCDOPIN56Flt=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",RemtHeatModRqst=L"0",InitaContIndOn_BMS=L"0" 1678171310000',
        #              'E50,VIN=LK6ADAE19LE560695 TalClNum_G1=L"33",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",BMSCode=L"1",EPSWorkCur=L"0",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",TMOrd_G=L"1",DrSbltAtc=L"0",RrImpctDet=L"0",BatMiniCelVol=L"3327",OBCORDCACIntnlTemp=L"43",TMDeratingSts=L"0",TemperoHeatMeb3=L"205",BattCellVoltage_16=L"3.333",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"13",TemperoHeatMeb7=L"205",TemperoHeatMeb6=L"205",BatMinTemp=L"11",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"205",PDUHVLock=L"1",BattCellVoltage_17=L"3.334",BatFltNumN1_G=L"0",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"1",BatMaNegRlySts=L"1",VecActTalVol=L"109.3",MCUCur_G=L"0",vehType=L"E50",Longitude=L"111.110452",WhlGrndVlctyLftDrvn=L"0",BatPowSts=L"1",DFCDCANMUTE=L"0",TMActWkSts=L"2",BrkProcInPrgrsIO=L"0",CDUState=L"2",TMActSpd=L"0",objId=L"LK6ADAE19LE560695",PTID=L"2",DFCShiftLvrPlbtyErr=L"0",ABSAtv=L"0",OBCInpVolOv=L"0",EngyRevIndOn=L"0",RemainOdo=L"71",BrakPedalPos=L"0",BatMaPosRlySts=L"1",TMTemp_G=L"28",BatCelVolSensFlt=L"0",CDUType=L"9",Lattd_G=L"37.509628",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",DrTorqRqst=L"0",BatIslateSts=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"1",DCDCIntnlFlt=L"0",GearSts_G=L"15",VecChrgStsIndOn=L"1",zdsjxh=L"10",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",EvpEquPreVal=L"-51.9",InitaContIndOn_EPS=L"1",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"1",DCDCOtpVol=L"14.1",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"15",InvActTemp=L"26",BMSGenSts1RollCnt=L"13",GPSspeed=L"0",SignalStrengthOne=L"4",StrWhAng=L"-0.9375",SignalStrengthTwo=L"27",RemtCtrParkHeat=L"0",createTime=L"1678171310012",ACRelayCmd=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"9",BatMinTempCode_G=L"1",CCSts=L"2",BatDvcVlt_G1=L"110",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",MCUSupCode=L"8340040",dqsjxh=L"1163",Latitude=L"37.509628",VecActPow=L"0",BMSHvPowOnRqst=L"2",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",VecSOCLoWrngIndOn=L"0",OBCOtpVlt=L"110.6",BatMaxCelVolPos=L"11",t2RTime=L"2023-03-07 14:41:50",TMActDrvTorLmt=L"84.75",InitaContIndOn_OBC=L"0",TalCur_G=L"-13.1",BatAvgTemp=L"12",OBCInpCurOv=L"0",BatMaxClVltCode_G=L"11",CollisionSig=L"0",VecStatRdy=L"0",BatMaxCelVol=L"3336",ChrgCtrlSnglFail=L"0",KyPstn=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",MCUActFltNum=L"0",DCDCTempWrng=L"0",BatSubsyst_Vlt_G=L"1",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"0",BMSSts=L"2",DriverLeftWarning=L"0",BatMinTemp_G=L"11",BatRmaChrgTim=L"302",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",ModeCode=L"95",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"-13.1",BMSChrgCurRqst=L"31.3",Swit2Status=L"1",BatAvgCelVol=L"3332",t1RTime=L"2023-03-07 14:41:50",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",VacPumpTransRate=L"1",DFCGearReqMongErr=L"0",BatHetPrsrvtn=L"0",DFCCASmax=L"0",BatOthFltNum=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",VecFltIndOn=L"0",BatMinClVltCode_G=L"1",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",VecActTalCur=L"-13.1",ISBF=L"0",BatClVlt_G1=L"3.328_3.329_3.334_3.332_3.331_3.331_3.332_3.331_3.334_3.334_3.336_3.335_3.333_3.332_3.332_3.333_3.334_3.332_3.333_3.329_3.333_3.335_3.335_3.334_3.333_3.333_3.33_3.333_3.335_3.334_3.334_3.333_3.33",BatFltLvl=L"0",MCUInpVlt_G=L"109",TMWkStsRqst=L"0",BatMaxTempPos=L"4",TMActFbTorLmt=L"-85",DrvPowLimtIndOn=L"0",OBCOtpVolOv=L"0",BatAvgCelVolSts=L"0",VCUSftwrVsinNum=L"228",OBCOtpCur=L"13.5",BatTalVolSts=L"0",BMSChrgVolRqst=L"121.4",TalVlt_G=L"110",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"8.1",TMTorqRqst=L"0",InvVolSts=L"0",VehOdoV=L"0",ShifGearFailAtHiSpeed=L"0",VecOptMod=L"4",SoftEdt=L"152",TMorMCUOvTempInd=L"0",OptMod_G=L"1",RemtCtrlSpdLimtRqstSt=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"2",VehOdo_G=L"9737",MCUTemp_G=L"26",TMSpdRqst=L"0",LowBatVol=L"14.08",BatTempSts=L"0",VecChrgRqst=L"1",VecTMActSpdV=L"0",VecTMActSpd=L"0",VCUBatThrmlRunwyAlmInd=L"0",MCUVolFlt=L"0",DCDCOtpVolSts=L"0",TMSpd_G=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"13",EvrmetAblPreVal=L"91.2",OBCInpCur=L"7",BatIslatRes=L"20271",BatMiniCelVolPos=L"1",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",OBCInpVolUn=L"0",VecSOC=L"45",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCOtpCur=L"2.8",InitaContIndOn_ABS=L"1",BMSChrgVolCurRqstAnorm=L"0",CollectTime=L"2023-03-07 14:41:50",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"115",BatMinClVlt_G=L"3.327",RatedEgy=L"13.8",WhlGrndVlctyRtNnDrvn=L"0",DCDCStsWrng=L"0",TMActCtrlMdSts=L"0",VehSpdAvgDrvnRd=L"0",NoPulOutChrgPlugWrng=L"0",AntiSlope=L"0",HVRelSta=L"0",VecActPowPer=L"0",BatChrgVolCpl=L"120.4",MaxWrngLvl_G=L"0",InitaContIndOn_IC=L"1",BatheatindON=L"0",DFCUAcePedlPlbtyErr=L"0",AccActPos=L"0",PTCSwitch=L"2",MaxVltBatSubSysNum_G=L"1",ACSwitch=L"0",VCUCtrlRqst1RollCnt=L"2",TMSts_G=L"4",VehSts_G=L"2",VehSpdAvgDrvn=L"0",InitaContIndOn_SDM=L"1",RealSoc=L"47",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"4.3",t02RDelay=L"0",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Direction=L"0",Lngtd_G=L"111.110452",RemtCtrlPrkHet=L"0",CPSts=L"2",BatThrmlRunwyAlmInd=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:50",DCDCInpCur=L"0.3",BatTalVolSm=L"110",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",BatMaxClVlt_G=L"3.336",AcInpFlt=L"0",VBatChrgStsIndOn=L"1",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"5143",BatCurSensFlt=L"0",AirbgIndOn=L"0",BatMinTemVolPakOrd=L"1",TMCtrlMdRqst=L"2",OBCInpMxCur=L"8",DrivStOccSt=L"1",BatSOH=L"92",BatSOC=L"45",BatEnrgAvail=L"5.7",VehOdo=L"9737",t2RIP=L"192.168.20.104",BatTempSensNum_G1=L"10",BatMaxTempCode_G=L"4",TmnlWkupSurc=L"3",OBCHdErr=L"0",TMActPow=L"0",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",BatTempSensFlt=L"0",BatDvcCur_G1=L"-13.1",TerminalNO=L"7681100322366106200L5200146",InvVol=L"109",InvJctTemp=L"26",DFCMongHvNotIntd=L"0",DCDCInpVol=L"110.6",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"8.1",MCUCurFlt=L"0",BatExtVol=L"109.3",DrSbltAtcV=L"0",BatIntVol=L"109.3",TMNum_G=L"1",OBCOvTemp=L"0",VCURemtCtrlSpdLmtFb=L"0",SupCode=L"0",DFCTioutForVT=L"0",InvrCur=L"0",Bat2sPlsChrgPowAvail=L"8.1",vehConfig=L"LV1",VecActGearSts=L"3",VehConf=L"3",BatMinTempPos=L"1",CellBattNum_21=L"21",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"0",VecChrgingSts=L"1",Bat10sPlsDischrgPowAvail=L"13.8",WhlGrndVlctyRtDrvn=L"0",SftwrMjrVsin=L"4",OBCInpVlt=L"212",RatedVol=L"106",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",BatMaxTemp=L"13",EvpRlySta=L"0",TMTorq_G=L"0",BatInfLen=L"0",TMActTemp=L"28",PlsAgainToRdy=L"0",BatChrgSts=L"1",HandBrkSts=L"1",InitaContIndOn_UCU=L"1",SOC_G=L"45",IslatRes_G=L"20271",objType=L"E50",BatSubsysTemp_G1=L"11_12_12_13_12_13_12_12_11_12",BatContiDischrgPowAvail=L"13.8",InitaContIndOn_MCU=L"1",PowBatErrIndOn=L"0",BatSOCSTS=L"0",VecIslateWrngIndOn=L"0",DCDCInternalTemp=L"42",DFCMCUGen1TiOut=L"0",MinTempSubSysNum_G=L"1",Bat2sPlsDischrgPowAvail=L"13.8",TMActTorq=L"0",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:50",VehDrvMod=L"1",MCUCtrlFlt=L"0",BatChrgTims=L"135",BatManuCode=L"0",MESSAGETYPE=L"REALTIME",VacPumpWrngIndOn=L"0",TalNumOfClBatInThisFrm_G1=L"33",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DFCDOPIN56Flt=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",RemtHeatModRqst=L"0",InitaContIndOn_BMS=L"0" 1678171310000',
        #              'E50,VIN=LK6ADAE12LE270203 TalClNum_G1=L"33",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",BMSCode=L"1",EPSWorkCur=L"0.25",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",TMOrd_G=L"1",DrSbltAtc=L"1",RrImpctDet=L"0",BatMiniCelVol=L"3284",OBCORDCACIntnlTemp=L"39",TMDeratingSts=L"0",TemperoHeatMeb3=L"205",BattCellVoltage_16=L"3.286",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"22",TemperoHeatMeb7=L"205",TemperoHeatMeb6=L"205",BatMinTemp=L"21",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"205",PDUHVLock=L"1",BattCellVoltage_17=L"3.287",BatFltNumN1_G=L"0",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"108",MCUCur_G=L"1",vehType=L"E50",Longitude=L"121.236863",WhlGrndVlctyLftDrvn=L"43.81",BatPowSts=L"1",DFCDCANMUTE=L"0",TMActWkSts=L"3",BrkProcInPrgrsIO=L"0",CDUState=L"5",TMActSpd=L"3276",objId=L"LK6ADAE12LE270203",PTID=L"2",DFCShiftLvrPlbtyErr=L"0",ABSAtv=L"0",OBCInpVolOv=L"0",EngyRevIndOn=L"0",RemainOdo=L"121",BrakPedalPos=L"0",BatMaPosRlySts=L"1",TMTemp_G=L"54",BatCelVolSensFlt=L"0",CDUType=L"9",Lattd_G=L"31.055148",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",DrTorqRqst=L"-1",BatIslateSts=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"3",DCDCIntnlFlt=L"0",GearSts_G=L"14",VecChrgStsIndOn=L"0",zdsjxh=L"387",AclrtrPedaStrk_G=L"8",TMOvSpdInd=L"0",EvpEquPreVal=L"-64.8",InitaContIndOn_EPS=L"0",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"1",DCDCOtpVol=L"14.1",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"4",InvActTemp=L"40",BMSGenSts1RollCnt=L"15",GPSspeed=L"0",SignalStrengthOne=L"4",StrWhAng=L"0.3125",SignalStrengthTwo=L"31",RemtCtrParkHeat=L"0",createTime=L"1678171310019",ACRelayCmd=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"0",BatMinTempCode_G=L"1",CCSts=L"0",BatDvcVlt_G1=L"108.5",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",MCUSupCode=L"8340040",dqsjxh=L"401",Latitude=L"31.055148",VecActPow=L"0",BMSHvPowOnRqst=L"2",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",VecSOCLoWrngIndOn=L"0",OBCOtpVlt=L"108.6",BatMaxCelVolPos=L"18",t2RTime=L"2023-03-07 14:41:50",TMActDrvTorLmt=L"58.25",InitaContIndOn_OBC=L"1",TalCur_G=L"2.1",BatAvgTemp=L"22",OBCInpCurOv=L"0",BatMaxClVltCode_G=L"18",CollisionSig=L"0",VecStatRdy=L"1",BatMaxCelVol=L"3294",ChrgCtrlSnglFail=L"0",KyPstn=L"2",BatPrechrgRlySts=L"0",BatFuSts=L"0",MCUActFltNum=L"0",DCDCTempWrng=L"0",BatSubsyst_Vlt_G=L"1",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"43.6",BMSSts=L"3",DriverLeftWarning=L"0",BatMinTemp_G=L"21",BatRmaChrgTim=L"0",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",ModeCode=L"95",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"2.1",BMSChrgCurRqst=L"0",Swit2Status=L"0",BatAvgCelVol=L"3287",t1RTime=L"2023-03-07 14:41:49",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",VacPumpTransRate=L"1",DFCGearReqMongErr=L"0",BatHetPrsrvtn=L"0",DFCCASmax=L"0",BatOthFltNum=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",VecFltIndOn=L"0",BatMinClVltCode_G=L"1",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",VecActTalCur=L"2.2",ISBF=L"0",BatClVlt_G1=L"3.277_3.281_3.278_3.278_3.28_3.278_3.279_3.281_3.279_3.278_3.279_3.279_3.282_3.28_3.287_3.286_3.287_3.287_3.287_3.278_3.278_3.279_3.278_3.278_3.278_3.28_3.279_3.279_3.279_3.279_3.279_3.279_3.281",BatFltLvl=L"0",MCUInpVlt_G=L"108",TMWkStsRqst=L"2",BatMaxTempPos=L"4",TMActFbTorLmt=L"-58.5",DrvPowLimtIndOn=L"0",OBCOtpVolOv=L"0",BatAvgCelVolSts=L"0",VCUSftwrVsinNum=L"228",OBCOtpCur=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"0",TalVlt_G=L"108.5",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"8",TMTorqRqst=L"-1",InvVolSts=L"0",VehOdoV=L"0",ShifGearFailAtHiSpeed=L"0",VecOptMod=L"3",SoftEdt=L"152",TMorMCUOvTempInd=L"0",OptMod_G=L"1",RemtCtrlSpdLimtRqstSt=L"1",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"3",VehOdo_G=L"18289",MCUTemp_G=L"40",TMSpdRqst=L"0",LowBatVol=L"14.03",BatTempSts=L"0",VecChrgRqst=L"0",VecTMActSpdV=L"0",VecTMActSpd=L"3268",VCUBatThrmlRunwyAlmInd=L"0",MCUVolFlt=L"0",DCDCOtpVolSts=L"0",TMSpd_G=L"3276",BatChrgCurSts=L"0",BatMaxTemp_G=L"22",EvrmetAblPreVal=L"101.4",OBCInpCur=L"0",BatIslatRes=L"27063",BatMiniCelVolPos=L"1",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",OBCInpVolUn=L"0",VecSOC=L"74",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCOtpCur=L"4.6",InitaContIndOn_ABS=L"0",BMSChrgVolCurRqstAnorm=L"0",CollectTime=L"2023-03-07 14:41:49",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"155",BatMinClVlt_G=L"3.284",RatedEgy=L"13.8",WhlGrndVlctyRtNnDrvn=L"43.84",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"43.875",NoPulOutChrgPlugWrng=L"0",AntiSlope=L"0",HVRelSta=L"2",VecActPowPer=L"0",BatChrgVolCpl=L"120.4",MaxWrngLvl_G=L"0",InitaContIndOn_IC=L"0",BatheatindON=L"0",DFCUAcePedlPlbtyErr=L"0",AccActPos=L"8",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",ACSwitch=L"0",VCUCtrlRqst1RollCnt=L"3",TMSts_G=L"4",VehSts_G=L"1",VehSpdAvgDrvn=L"43.671875",InitaContIndOn_SDM=L"0",RealSoc=L"75",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"4.3",t02RDelay=L"1",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Direction=L"0",Lngtd_G=L"121.236863",RemtCtrlPrkHet=L"0",CPSts=L"0",BatThrmlRunwyAlmInd=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:49",DCDCInpCur=L"0.6",BatTalVolSm=L"108.5",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",BatMaxClVlt_G=L"3.294",AcInpFlt=L"0",VBatChrgStsIndOn=L"1",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"14369",BatCurSensFlt=L"0",AirbgIndOn=L"0",BatMinTemVolPakOrd=L"1",TMCtrlMdRqst=L"2",OBCInpMxCur=L"7",DrivStOccSt=L"1",BatSOH=L"92",BatSOC=L"74",BatEnrgAvail=L"9.3",VehOdo=L"18289",t2RIP=L"192.168.40.105",BatTempSensNum_G1=L"10",BatMaxTempCode_G=L"4",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"-0.4",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",BatTempSensFlt=L"0",BatDvcCur_G1=L"2.1",TerminalNO=L"7681100322375855700LB155202",InvVol=L"108",InvJctTemp=L"40",DFCMongHvNotIntd=L"0",DCDCInpVol=L"108.6",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"8",MCUCurFlt=L"0",BatExtVol=L"108",DrSbltAtcV=L"0",BatIntVol=L"108",TMNum_G=L"1",OBCOvTemp=L"0",VCURemtCtrlSpdLmtFb=L"0",SupCode=L"0",DFCTioutForVT=L"0",InvrCur=L"1",Bat2sPlsChrgPowAvail=L"8",vehConfig=L"LV1",VecActGearSts=L"1",VehConf=L"3",BatMinTempPos=L"1",CellBattNum_21=L"21",BalanceSts=L"1",WhlGrndVlctyLftNnDrvn=L"43.84",VecChrgingSts=L"0",Bat10sPlsDischrgPowAvail=L"16.5",WhlGrndVlctyRtDrvn=L"43.81",SftwrMjrVsin=L"4",OBCInpVlt=L"0",RatedVol=L"106",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",BatMaxTemp=L"22",EvpRlySta=L"0",TMTorq_G=L"3",BatInfLen=L"0",TMActTemp=L"54",PlsAgainToRdy=L"0",BatChrgSts=L"0",HandBrkSts=L"0",InitaContIndOn_UCU=L"0",SOC_G=L"74",IslatRes_G=L"27063",objType=L"E50",BatSubsysTemp_G1=L"21_21_21_22_22_22_21_22_22_22",BatContiDischrgPowAvail=L"16.5",InitaContIndOn_MCU=L"0",PowBatErrIndOn=L"0",BatSOCSTS=L"0",VecIslateWrngIndOn=L"0",DCDCInternalTemp=L"41",DFCMCUGen1TiOut=L"0",MinTempSubSysNum_G=L"1",Bat2sPlsDischrgPowAvail=L"16.5",TMActTorq=L"-1",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:49",VehDrvMod=L"1",MCUCtrlFlt=L"0",BatChrgTims=L"150",BatManuCode=L"0",MESSAGETYPE=L"REALTIME",VacPumpWrngIndOn=L"0",TalNumOfClBatInThisFrm_G1=L"33",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DFCDOPIN56Flt=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",RemtHeatModRqst=L"0",InitaContIndOn_BMS=L"0" 1678171309000',
        #              'E50,VIN=LK6ADAE14MB308114 BMSCode=L"5",TMOrd_G=L"1",RRDoorOpenSwAct=L"0",RrImpctDet=L"0",BattCellVoltage_16=L"4.037",BattCellVoltage_17=L"4.038",BatFltNumN1_G=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"104.9",MCUCur_G=L"0",BatPowSts=L"1",TaPnLgtAtv=L"0",TrnsRvsSwSt=L"0",PDAjrSwAtv=L"0",TMActSpd=L"0",DFCShiftLvrPlbtyErr=L"0",OBCInpVolOv=L"0",RemainOdo=L"147",BatMaPosRlySts=L"1",CDUType=L"9",Lattd_G=L"21.867086",DrTorqRqst=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"3",DCDCIntnlFlt=L"0",GearSts_G=L"0",VecChrgStsIndOn=L"0",zdsjxh=L"1830",EvpEquPreVal=L"-55.5",InitaContIndOn_EPS=L"0",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"2",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"11",InvActTemp=L"34",BMSGenSts1RollCnt=L"2",SignalStrengthOne=L"4",StrWhAng=L"0.4375",SignalStrengthTwo=L"24",TDEnblSts=L"0",createTime=L"1678171310017",ACRelayCmd=L"0",BatDvcVlt_G1=L"104.9",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",dqsjxh=L"319",MCUSupCode=L"8330245",HzrdLtIO=L"0",VecActPow=L"-0.1",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",OBCOtpVlt=L"104.5",BatMaxCelVolPos=L"14",t2RTime=L"2023-03-07 14:41:50",InitaContIndOn_OBC=L"0",TalCur_G=L"6",BatAvgTemp=L"23",OBCInpCurOv=L"0",ChargPrtOpInOnPrmt=L"0",ChrgCtrlSnglFail=L"0",KyPstn=L"2",MCUActFltNum=L"0",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"0",BatMinTemp_G=L"23",ModeCode=L"135",BMSChrgCurRqst=L"0",RtTrnLmpAtv=L"0",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",DFCGearReqMongErr=L"0",DFCCASmax=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",BatMinClVltCode_G=L"10",LSDAjrSwAtv=L"0",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",TMWkStsRqst=L"2",OBCOtpVolOv=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"104.9",TMTorqRqst=L"0",ShifGearFailAtHiSpeed=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"25",VehOdo_G=L"15020",BatTempSts=L"0",LowBatVol=L"13.96",VCUBatThrmlRunwyAlmInd=L"0",VecTMActSpd=L"0",DCDCOtpVolSts=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"25",EvrmetAblPreVal=L"101.1",BatIslatRes=L"10001",BatMiniCelVolPos=L"10",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",VecSOC=L"87",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"160",WhlGrndVlctyRtNnDrvn=L"0",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"0",RemtCtrlSpdLmtRqstSt=L"1",VecActPowPer=L"0",BatChrgVolCpl=L"110",InitaContIndOn_IC=L"0",DFCUAcePedlPlbtyErr=L"0",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",VCUCtrlRqst1RollCnt=L"11",ACSwitch=L"1",TMSts_G=L"4",LftTrnLmpAtv=L"0",RealSoc=L"87",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"2.1",Direction=L"0",RemtCtrlPrkHet=L"0",CPSts=L"0",HzrdLgtSwAtv=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",TDAjrSwAtv=L"0",DCDCInpCur=L"2.6",BatTalVolSm=L"104.9",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",VBatChrgStsIndOn=L"1",BatCurSensFlt=L"0",AirbgIndOn=L"0",TMCtrlMdRqst=L"2",OBCInpMxCur=L"0",BatEnrgAvail=L"10.1",BatTempSensNum_G1=L"6",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",IgnKyPstn=L"2",BatDvcCur_G1=L"6",TerminalNO=L"7683202652373453401M6260143",InvVol=L"106",InvJctTemp=L"34",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"6.73",MCUCurFlt=L"0",BatExtVol=L"104.8",DrSbltAtcV=L"0",TMNum_G=L"1",OBCOvTemp=L"0",SupCode=L"1",DFCTioutForVT=L"0",Bat2sPlsChrgPowAvail=L"6.73",vehConfig=L"LV1",VecActGearSts=L"0",VehConf=L"3",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"0",WhlGrndVlctyRtDrvn=L"0",OBCInpVlt=L"0",BatMaxTemp=L"25",EvpRlySta=L"0",IALPwrMdCtrlSt=L"0",TMActTemp=L"31",PlsAgainToRdy=L"0",HandBrkSts=L"1",IslatRes_G=L"10001",KyOpenSwAct=L"0",objType=L"E50",BatContiDischrgPowAvail=L"18",InitaContIndOn_MCU=L"0",VecIslateWrngIndOn=L"0",DFCMCUGen1TiOut=L"0",Bat2sPlsDischrgPowAvail=L"18",MCUCtrlFlt=L"0",BatChrgTims=L"98",VacPumpWrngIndOn=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",InitaContIndOn_BMS=L"0",TalClNum_G1=L"26",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"202",VCUVehDrvMod=L"0",EPSWorkCur=L"0",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"1",ProtocolVersion=L"1_1_1",DrSbltAtc=L"1",BatMiniCelVol=L"4033",TemperoHeatMeb3=L"205",TMDeratingSts=L"0",OBCORDCACIntnlTemp=L"33",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",BatMinTemp=L"23",TemperoHeatMeb6=L"205",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"37",PDUHVLock=L"1",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"0",vehType=L"E50",Longitude=L"111.240798",DFCDCANMUTE=L"0",WhlGrndVlctyLftDrvn=L"0",BrkProcInPrgrsIO=L"0",TMActWkSts=L"3",CDUState=L"5",objId=L"LK6ADAE14MB308114",PTID=L"2",ABSAtv=L"0",EngyRevIndOn=L"0",BrakPedalPos=L"0",TMTemp_G=L"31",BatCelVolSensFlt=L"0",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",BatIslateSts=L"0",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",AnThWaSt=L"1",DCDCOtpVol=L"14.1",RrFgLtIO=L"0",GPSspeed=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"8",BatMinTempCode_G=L"3",CCSts=L"0",IgnKyInstAtv=L"0",Latitude=L"21.867086",BMSHvPowOnRqst=L"2",VecSOCLoWrngIndOn=L"0",TMActDrvTorLmt=L"82.25",BatMaxClVltCode_G=L"14",CollisionSig=L"0",VecStatRdy=L"1",BatMaxCelVol=L"4040",LwBmIO=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",DCDCTempWrng=L"0",BCMRunMod=L"0",BatSubsyst_Vlt_G=L"1",BMSSts=L"3",DriverLeftWarning=L"1",BatRmaChrgTim=L"0",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"6",FtFgLtIO=L"0",BatAvgCelVol=L"4037",Swit2Status=L"0",t1RTime=L"2023-03-07 14:41:48",VacPumpTransRate=L"1",BCMSftwrVsinNum=L"1",BatHetPrsrvtn=L"0",BatOthFltNum=L"0",VecFltIndOn=L"0",VecActTalCur=L"5.9",ISBF=L"0",BatClVlt_G1=L"4.037_4.037_4.036_4.039_4.037_4.039_4.036_4.036_4.036_4.033_4.035_4.035_4.035_4.04_4.039_4.037_4.038_4.039_4.039_4.04_4.039_4.04_4.04_4.04_4.04_4.04",BatFltLvl=L"0",MCUInpVlt_G=L"106",BatMaxTempPos=L"5",TMActFbTorLmt=L"-82.5",DrvPowLimtIndOn=L"0",BatAvgCelVolSts=L"0",OBCOtpCur=L"0",VCUSftwrVsinNum=L"162",DDAjrSwAtv=L"1",TalVlt_G=L"104.9",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"6.73",InvVolSts=L"0",VehOdoV=L"0",VecOptMod=L"3",SoftEdt=L"180",RSDAjrSwAtv=L"0",TMorMCUOvTempInd=L"0",OptMod_G=L"1",MCUTemp_G=L"34",TMSpdRqst=L"0",VecChrgRqst=L"0",VecTMActSpdV=L"0",RLDoorOpenSwAct=L"0",MCUVolFlt=L"0",TMSpd_G=L"0",OBCInpCur=L"0",OBCInpVolUn=L"0",DCDCOtpCur=L"19",BMSChrgVolCurRqstAnorm=L"0",InitaContIndOn_ABS=L"0",CollectTime=L"2023-03-07 14:41:48",BatMinClVlt_G=L"4.033",RatedEgy=L"14",NoPulOutChrgPlugWrng=L"0",PsDoorOpenSwAct=L"0",AntiSlope=L"0",HVRelSta=L"2",MaxWrngLvl_G=L"0",BatheatindON=L"0",AccActPos=L"0",InitaContIndOn_BCM=L"0",TrnSwAct=L"0",VehSts_G=L"1",VehSpdAvgDrvn=L"0",InitaContIndOn_SDM=L"0",HhBmIO=L"0",t02RDelay=L"2",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Lngtd_G=L"111.240798",BatThrmlRunwyAlmInd=L"0",StpLpSt=L"0",PrkBrkSwAtv=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:48",BatMaxClVlt_G=L"4.04",AcInpFlt=L"0",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"6394",BatMinTemVolPakOrd=L"1",BatSOH=L"100",DrivStOccSt=L"0",BatSOC=L"87",VehOdo=L"15020",t2RIP=L"192.168.40.106",BatMaxTempCode_G=L"5",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"-0.1",WindscenWipSt=L"0",LdspkSt=L"0",BatTempSensFlt=L"0",DFCMongHvNotIntd=L"0",DCDCInpVol=L"104.5",WipSwStat=L"1",BatIntVol=L"104.9",VCURemtCtrlSpdLmtFb=L"0",InvrCur=L"0",BatMinTempPos=L"3",VecChrgingSts=L"0",Bat10sPlsDischrgPowAvail=L"18",SftwrMjrVsin=L"8",FtPnLgtAtv=L"0",RatedVol=L"96",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",TMTorq_G=L"-0.3",BatInfLen=L"0",BatChrgSts=L"0",InitaContIndOn_UCU=L"0",BCMRunModV=L"1",SOC_G=L"87",BatSubsysTemp_G1=L"24_24_23_23_25_24",PowBatErrIndOn=L"0",BatSOCSTS=L"0",DCDCInternalTemp=L"37",MaiLgtSw=L"0",MinTempSubSysNum_G=L"1",TMActTorq=L"-0.25",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:48",VehDrvMod=L"1",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",TalNumOfClBatInThisFrm_G1=L"26",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DrDoorOpenSwAct=L"1",DFCDOPIN56Flt=L"0",RemtHeatModRqst=L"0" 1678171308000',
        #              'E50,VIN=LK6ADAE16MG520550 BMSCode=L"2",TMOrd_G=L"1",RRDoorOpenSwAct=L"0",RrImpctDet=L"0",BattCellVoltage_16=L"3.379",BattCellVoltage_17=L"3.382",BatFltNumN1_G=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"101.4",MCUCur_G=L"0",BatPowSts=L"1",TaPnLgtAtv=L"0",TrnsRvsSwSt=L"0",PDAjrSwAtv=L"0",TMActSpd=L"0",DFCShiftLvrPlbtyErr=L"0",OBCInpVolOv=L"0",RemainOdo=L"164",BatMaPosRlySts=L"1",CDUType=L"9",Lattd_G=L"31.854105",DrTorqRqst=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"1",DCDCIntnlFlt=L"0",GearSts_G=L"15",VecChrgStsIndOn=L"1",zdsjxh=L"4845",EvpEquPreVal=L"-54.1",InitaContIndOn_EPS=L"1",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"2",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"7",InvActTemp=L"19",BMSGenSts1RollCnt=L"12",SignalStrengthOne=L"4",StrWhAng=L"5.1875",SignalStrengthTwo=L"28",TDEnblSts=L"0",createTime=L"1678171310033",ACRelayCmd=L"0",BatDvcVlt_G1=L"101.4",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",dqsjxh=L"148",MCUSupCode=L"8340040",HzrdLtIO=L"0",VecActPow=L"0",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",OBCOtpVlt=L"102",BatMaxCelVolPos=L"19",t2RTime=L"2023-03-07 14:41:50",InitaContIndOn_OBC=L"0",TalCur_G=L"-14",BatAvgTemp=L"22",OBCInpCurOv=L"0",ChargPrtOpInOnPrmt=L"1",ChrgCtrlSnglFail=L"0",KyPstn=L"0",MCUActFltNum=L"0",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"0",BatMinTemp_G=L"22",ModeCode=L"132",BMSChrgCurRqst=L"40",RtTrnLmpAtv=L"0",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",DFCGearReqMongErr=L"0",DFCCASmax=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",BatMinClVltCode_G=L"20",LSDAjrSwAtv=L"0",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",TMWkStsRqst=L"0",OBCOtpVolOv=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"110.5",TMTorqRqst=L"0",ShifGearFailAtHiSpeed=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"3",VehOdo_G=L"5580",BatTempSts=L"0",LowBatVol=L"14.15",VCUBatThrmlRunwyAlmInd=L"0",VecTMActSpd=L"0",DCDCOtpVolSts=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"24",EvrmetAblPreVal=L"97",BatIslatRes=L"10044",BatMiniCelVolPos=L"20",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",VecSOC=L"97",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"251",WhlGrndVlctyRtNnDrvn=L"0",DCDCStsWrng=L"0",TMActCtrlMdSts=L"0",VehSpdAvgDrvnRd=L"0",RemtCtrlSpdLmtRqstSt=L"0",VecActPowPer=L"0",BatChrgVolCpl=L"110.5",InitaContIndOn_IC=L"1",DFCUAcePedlPlbtyErr=L"0",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",VCUCtrlRqst1RollCnt=L"1",ACSwitch=L"0",TMSts_G=L"4",LftTrnLmpAtv=L"0",RealSoc=L"92",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"2.1",Direction=L"0",RemtCtrlPrkHet=L"0",CPSts=L"2",HzrdLgtSwAtv=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",TDAjrSwAtv=L"0",DCDCInpCur=L"0.2",BatTalVolSm=L"101.4",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",VBatChrgStsIndOn=L"1",BatCurSensFlt=L"0",AirbgIndOn=L"0",TMCtrlMdRqst=L"2",OBCInpMxCur=L"8",BatEnrgAvail=L"10.6",BatTempSensNum_G1=L"6",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",IgnKyPstn=L"0",BatDvcCur_G1=L"-14",TerminalNO=L"7683202652375880403M8304471",InvVol=L"101",InvJctTemp=L"19",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"4.32",MCUCurFlt=L"0",BatExtVol=L"101.3",DrSbltAtcV=L"0",TMNum_G=L"1",OBCOvTemp=L"0",SupCode=L"1",DFCTioutForVT=L"0",Bat2sPlsChrgPowAvail=L"4.32",vehConfig=L"LV1",VecActGearSts=L"3",VehConf=L"3",BalanceSts=L"1",WhlGrndVlctyLftNnDrvn=L"0",WhlGrndVlctyRtDrvn=L"0",OBCInpVlt=L"224",BatMaxTemp=L"24",EvpRlySta=L"0",IALPwrMdCtrlSt=L"0",TMActTemp=L"20",PlsAgainToRdy=L"0",HandBrkSts=L"1",IslatRes_G=L"10044",KyOpenSwAct=L"0",objType=L"E50",BatContiDischrgPowAvail=L"18",InitaContIndOn_MCU=L"1",VecIslateWrngIndOn=L"0",DFCMCUGen1TiOut=L"0",Bat2sPlsDischrgPowAvail=L"18",MCUCtrlFlt=L"0",BatChrgTims=L"56",VacPumpWrngIndOn=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",InitaContIndOn_BMS=L"1",TalClNum_G1=L"30",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",EPSWorkCur=L"2",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",DrSbltAtc=L"1",BatMiniCelVol=L"3370",TemperoHeatMeb3=L"205",TMDeratingSts=L"0",OBCORDCACIntnlTemp=L"44",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",BatMinTemp=L"22",TemperoHeatMeb6=L"205",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"44",PDUHVLock=L"1",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"1",vehType=L"E50",Longitude=L"106.74963",DFCDCANMUTE=L"0",WhlGrndVlctyLftDrvn=L"0",BrkProcInPrgrsIO=L"0",TMActWkSts=L"2",CDUState=L"2",objId=L"LK6ADAE16MG520550",PTID=L"2",ABSAtv=L"0",EngyRevIndOn=L"0",BrakPedalPos=L"0",TMTemp_G=L"20",BatCelVolSensFlt=L"0",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",BatIslateSts=L"0",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",AnThWaSt=L"5",DCDCOtpVol=L"14",RrFgLtIO=L"0",GPSspeed=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"11",BatMinTempCode_G=L"1",CCSts=L"2",IgnKyInstAtv=L"0",Latitude=L"31.854105",BMSHvPowOnRqst=L"2",VecSOCLoWrngIndOn=L"0",TMActDrvTorLmt=L"84.75",BatMaxClVltCode_G=L"19",CollisionSig=L"0",VecStatRdy=L"0",BatMaxCelVol=L"3389",LwBmIO=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",DCDCTempWrng=L"0",BCMRunMod=L"0",BatSubsyst_Vlt_G=L"1",BMSSts=L"2",DriverLeftWarning=L"0",BatRmaChrgTim=L"217",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"-14",FtFgLtIO=L"0",BatAvgCelVol=L"3381",Swit2Status=L"1",t1RTime=L"2023-03-07 14:41:49",VacPumpTransRate=L"1",BCMSftwrVsinNum=L"1",BatHetPrsrvtn=L"0",BatOthFltNum=L"0",VecFltIndOn=L"0",VecActTalCur=L"-13.9",ISBF=L"0",BatClVlt_G1=L"3.382_3.382_3.383_3.381_3.383_3.381_3.382_3.38_3.382_3.382_3.382_3.382_3.381_3.381_3.381_3.379_3.382_3.382_3.389_3.37_3.382_3.383_3.381_3.382_3.381_3.382_3.38_3.382_3.382_3.382",BatFltLvl=L"0",MCUInpVlt_G=L"101",BatMaxTempPos=L"3",TMActFbTorLmt=L"-85",DrvPowLimtIndOn=L"0",BatAvgCelVolSts=L"0",OBCOtpCur=L"14",VCUSftwrVsinNum=L"162",DDAjrSwAtv=L"0",TalVlt_G=L"101.4",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"4.32",InvVolSts=L"0",VehOdoV=L"0",VecOptMod=L"4",SoftEdt=L"150",RSDAjrSwAtv=L"0",TMorMCUOvTempInd=L"0",OptMod_G=L"1",MCUTemp_G=L"19",TMSpdRqst=L"0",VecChrgRqst=L"1",VecTMActSpdV=L"0",RLDoorOpenSwAct=L"0",MCUVolFlt=L"0",TMSpd_G=L"0",OBCInpCur=L"7",OBCInpVolUn=L"0",DCDCOtpCur=L"1.9",BMSChrgVolCurRqstAnorm=L"0",InitaContIndOn_ABS=L"1",CollectTime=L"2023-03-07 14:41:49",BatMinClVlt_G=L"3.37",RatedEgy=L"13.7",NoPulOutChrgPlugWrng=L"0",PsDoorOpenSwAct=L"0",AntiSlope=L"0",HVRelSta=L"0",MaxWrngLvl_G=L"0",BatheatindON=L"0",AccActPos=L"0",InitaContIndOn_BCM=L"1",TrnSwAct=L"0",VehSts_G=L"2",VehSpdAvgDrvn=L"0",InitaContIndOn_SDM=L"1",HhBmIO=L"0",t02RDelay=L"1",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Lngtd_G=L"106.74963",BatThrmlRunwyAlmInd=L"0",StpLpSt=L"0",PrkBrkSwAtv=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:49",BatMaxClVlt_G=L"3.389",AcInpFlt=L"0",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"3715",BatMinTemVolPakOrd=L"1",BatSOH=L"100",DrivStOccSt=L"1",BatSOC=L"97",VehOdo=L"5580",t2RIP=L"192.168.40.101",BatMaxTempCode_G=L"3",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"0",WindscenWipSt=L"0",LdspkSt=L"0",BatTempSensFlt=L"0",DFCMongHvNotIntd=L"0",DCDCInpVol=L"102",WipSwStat=L"1",BatIntVol=L"101.4",VCURemtCtrlSpdLmtFb=L"0",InvrCur=L"0",BatMinTempPos=L"1",VecChrgingSts=L"1",Bat10sPlsDischrgPowAvail=L"18",SftwrMjrVsin=L"5",FtPnLgtAtv=L"0",RatedVol=L"97",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",TMTorq_G=L"0",BatInfLen=L"0",BatChrgSts=L"1",InitaContIndOn_UCU=L"1",BCMRunModV=L"1",SOC_G=L"97",BatSubsysTemp_G1=L"22_23_24_23_23_22",PowBatErrIndOn=L"0",BatSOCSTS=L"0",DCDCInternalTemp=L"30",MaiLgtSw=L"0",MinTempSubSysNum_G=L"1",TMActTorq=L"0",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:49",VehDrvMod=L"1",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",TalNumOfClBatInThisFrm_G1=L"30",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DrDoorOpenSwAct=L"0",DFCDOPIN56Flt=L"0",RemtHeatModRqst=L"0" 1678171309000',
        #              'E50,VIN=LK6ADAE13NG215671 BMSCode=L"0",TMOrd_G=L"1",RRDoorOpenSwAct=L"0",RrImpctDet=L"0",BattCellVoltage_16=L"3.262",BattCellVoltage_17=L"3.261",BatFltNumN1_G=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"104.1",MCUCur_G=L"2",BatPowSts=L"1",TaPnLgtAtv=L"0",TrnsRvsSwSt=L"0",PDAjrSwAtv=L"0",TMActSpd=L"1803",DFCShiftLvrPlbtyErr=L"0",OBCInpVolOv=L"0",RemainOdo=L"112",BatMaPosRlySts=L"1",CDUType=L"9",Lattd_G=L"31.32682",DrTorqRqst=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"3",DCDCIntnlFlt=L"0",GearSts_G=L"14",VecChrgStsIndOn=L"0",zdsjxh=L"1057",EvpEquPreVal=L"-66.5",InitaContIndOn_EPS=L"0",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"2",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"14",InvActTemp=L"37",BMSGenSts1RollCnt=L"1",SignalStrengthOne=L"4",StrWhAng=L"8",SignalStrengthTwo=L"0",TDEnblSts=L"0",createTime=L"1678171310030",ACRelayCmd=L"0",BatDvcVlt_G1=L"104.2",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",dqsjxh=L"74",MCUSupCode=L"8340040",HzrdLtIO=L"0",VecActPow=L"0.9",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",OBCOtpVlt=L"104.4",BatMaxCelVolPos=L"2",t2RTime=L"2023-03-07 14:41:50",InitaContIndOn_OBC=L"0",TalCur_G=L"5.7",BatAvgTemp=L"20",OBCInpCurOv=L"0",ChargPrtOpInOnPrmt=L"0",ChrgCtrlSnglFail=L"0",KyPstn=L"2",MCUActFltNum=L"0",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"23.7",BatMinTemp_G=L"20",ModeCode=L"133",BMSChrgCurRqst=L"0",RtTrnLmpAtv=L"0",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",DFCGearReqMongErr=L"0",DFCCASmax=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",BatMinClVltCode_G=L"7",LSDAjrSwAtv=L"0",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",TMWkStsRqst=L"2",OBCOtpVolOv=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"104.2",TMTorqRqst=L"1",ShifGearFailAtHiSpeed=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"3",VehOdo_G=L"12793",BatTempSts=L"0",LowBatVol=L"14.03",VCUBatThrmlRunwyAlmInd=L"0",VecTMActSpd=L"1775",DCDCOtpVolSts=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"21",EvrmetAblPreVal=L"101.7",BatIslatRes=L"10021",BatMiniCelVolPos=L"7",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",VecSOC=L"66",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"55",WhlGrndVlctyRtNnDrvn=L"24.21",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"24.296875",RemtCtrlSpdLmtRqstSt=L"1",VecActPowPer=L"4",BatChrgVolCpl=L"117.8",InitaContIndOn_IC=L"0",DFCUAcePedlPlbtyErr=L"0",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",VCUCtrlRqst1RollCnt=L"15",ACSwitch=L"0",TMSts_G=L"1",LftTrnLmpAtv=L"0",RealSoc=L"62",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"12.9",Direction=L"0",RemtCtrlPrkHet=L"0",CPSts=L"0",HzrdLgtSwAtv=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",TDAjrSwAtv=L"0",DCDCInpCur=L"0.7",BatTalVolSm=L"104.2",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",VBatChrgStsIndOn=L"1",BatCurSensFlt=L"0",AirbgIndOn=L"0",TMCtrlMdRqst=L"2",OBCInpMxCur=L"7",BatEnrgAvail=L"7.1",BatTempSensNum_G1=L"4",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",IgnKyPstn=L"2",BatDvcCur_G1=L"5.7",TerminalNO=L"7683202652375880403N1070697",InvVol=L"104",InvJctTemp=L"37",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"9.2",MCUCurFlt=L"0",BatExtVol=L"104.2",DrSbltAtcV=L"0",TMNum_G=L"1",OBCOvTemp=L"0",SupCode=L"0",DFCTioutForVT=L"0",Bat2sPlsChrgPowAvail=L"9.2",vehConfig=L"LV1",VecActGearSts=L"1",VehConf=L"3",BalanceSts=L"1",WhlGrndVlctyLftNnDrvn=L"24.3",WhlGrndVlctyRtDrvn=L"23.99",OBCInpVlt=L"0",BatMaxTemp=L"21",EvpRlySta=L"0",IALPwrMdCtrlSt=L"0",TMActTemp=L"43",PlsAgainToRdy=L"0",HandBrkSts=L"0",IslatRes_G=L"10021",KyOpenSwAct=L"0",objType=L"E50",BatContiDischrgPowAvail=L"16",InitaContIndOn_MCU=L"0",VecIslateWrngIndOn=L"0",DFCMCUGen1TiOut=L"0",Bat2sPlsDischrgPowAvail=L"16",MCUCtrlFlt=L"0",BatChrgTims=L"88",VacPumpWrngIndOn=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",InitaContIndOn_BMS=L"0",TalClNum_G1=L"32",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",EPSWorkCur=L"0.75",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",DrSbltAtc=L"1",BatMiniCelVol=L"3256",TemperoHeatMeb3=L"205",TMDeratingSts=L"0",OBCORDCACIntnlTemp=L"41",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",BatMinTemp=L"20",TemperoHeatMeb6=L"205",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"14",PDUHVLock=L"1",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"0",vehType=L"E50",Longitude=L"120.62236",DFCDCANMUTE=L"0",WhlGrndVlctyLftDrvn=L"24.01",BrkProcInPrgrsIO=L"0",TMActWkSts=L"3",CDUState=L"5",objId=L"LK6ADAE13NG215671",PTID=L"2",ABSAtv=L"0",EngyRevIndOn=L"0",BrakPedalPos=L"0",TMTemp_G=L"43",BatCelVolSensFlt=L"0",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",BatIslateSts=L"0",AclrtrPedaStrk_G=L"13",TMOvSpdInd=L"0",AnThWaSt=L"1",DCDCOtpVol=L"14",RrFgLtIO=L"0",GPSspeed=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"3",BatMinTempCode_G=L"2",CCSts=L"0",IgnKyInstAtv=L"0",Latitude=L"31.32682",BMSHvPowOnRqst=L"2",VecSOCLoWrngIndOn=L"0",TMActDrvTorLmt=L"84.75",BatMaxClVltCode_G=L"2",CollisionSig=L"0",VecStatRdy=L"1",BatMaxCelVol=L"3262",LwBmIO=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",DCDCTempWrng=L"0",BCMRunMod=L"0",BatSubsyst_Vlt_G=L"1",BMSSts=L"3",DriverLeftWarning=L"0",BatRmaChrgTim=L"0",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"5.7",FtFgLtIO=L"0",BatAvgCelVol=L"3258",Swit2Status=L"0",t1RTime=L"2023-03-07 14:41:50",VacPumpTransRate=L"1",BCMSftwrVsinNum=L"2",BatHetPrsrvtn=L"0",BatOthFltNum=L"0",VecFltIndOn=L"0",VecActTalCur=L"20.9",ISBF=L"0",BatClVlt_G1=L"3.259_3.262_3.257_3.261_3.257_3.257_3.256_3.259_3.258_3.257_3.256_3.26_3.259_3.26_3.258_3.262_3.261_3.257_3.258_3.257_3.259_3.256_3.257_3.257_3.258_3.258_3.258_3.257_3.258_3.257_3.26_3.258",BatFltLvl=L"0",MCUInpVlt_G=L"104",BatMaxTempPos=L"1",TMActFbTorLmt=L"-85",DrvPowLimtIndOn=L"0",BatAvgCelVolSts=L"0",OBCOtpCur=L"0",VCUSftwrVsinNum=L"162",DDAjrSwAtv=L"0",TalVlt_G=L"104.2",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"9.2",InvVolSts=L"0",VehOdoV=L"0",VecOptMod=L"3",SoftEdt=L"151",RSDAjrSwAtv=L"0",TMorMCUOvTempInd=L"0",OptMod_G=L"1",MCUTemp_G=L"37",TMSpdRqst=L"0",VecChrgRqst=L"0",VecTMActSpdV=L"0",RLDoorOpenSwAct=L"0",MCUVolFlt=L"0",TMSpd_G=L"1803",OBCInpCur=L"0",OBCInpVolUn=L"0",DCDCOtpCur=L"5.2",BMSChrgVolCurRqstAnorm=L"0",InitaContIndOn_ABS=L"0",CollectTime=L"2023-03-07 14:41:49",BatMinClVlt_G=L"3.256",RatedEgy=L"14.1",NoPulOutChrgPlugWrng=L"0",PsDoorOpenSwAct=L"0",AntiSlope=L"0",HVRelSta=L"2",MaxWrngLvl_G=L"0",BatheatindON=L"0",AccActPos=L"13",InitaContIndOn_BCM=L"0",TrnSwAct=L"0",VehSts_G=L"1",VehSpdAvgDrvn=L"23.734375",InitaContIndOn_SDM=L"0",HhBmIO=L"0",t02RDelay=L"1",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Lngtd_G=L"120.62236",BatThrmlRunwyAlmInd=L"0",StpLpSt=L"0",PrkBrkSwAtv=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:49",BatMaxClVlt_G=L"3.262",AcInpFlt=L"0",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"2748",BatMinTemVolPakOrd=L"1",BatSOH=L"100",DrivStOccSt=L"1",BatSOC=L"66",VehOdo=L"12793",t2RIP=L"192.168.20.106",BatMaxTempCode_G=L"1",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"0.1",WindscenWipSt=L"0",LdspkSt=L"0",BatTempSensFlt=L"0",DFCMongHvNotIntd=L"0",DCDCInpVol=L"104.6",WipSwStat=L"1",BatIntVol=L"104.1",VCURemtCtrlSpdLmtFb=L"0",InvrCur=L"2",BatMinTempPos=L"2",VecChrgingSts=L"0",Bat10sPlsDischrgPowAvail=L"16",SftwrMjrVsin=L"6",FtPnLgtAtv=L"0",RatedVol=L"102",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",TMTorq_G=L"1",BatInfLen=L"0",BatChrgSts=L"0",InitaContIndOn_UCU=L"0",BCMRunModV=L"1",SOC_G=L"66",BatSubsysTemp_G1=L"21_20_21_21",PowBatErrIndOn=L"0",BatSOCSTS=L"0",DCDCInternalTemp=L"39",MaiLgtSw=L"0",MinTempSubSysNum_G=L"1",TMActTorq=L"1",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:49",VehDrvMod=L"1",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",TalNumOfClBatInThisFrm_G1=L"32",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DrDoorOpenSwAct=L"0",DFCDOPIN56Flt=L"0",RemtHeatModRqst=L"0" 1678171309000',
        #              'E50,VIN=LK6ADAE10NB216967 BMSCode=L"2",TMOrd_G=L"1",RRDoorOpenSwAct=L"0",RrImpctDet=L"0",BattCellVoltage_16=L"3.254",BattCellVoltage_17=L"3.253",BatFltNumN1_G=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"95.3",MCUCur_G=L"0",BatPowSts=L"1",TaPnLgtAtv=L"0",TrnsRvsSwSt=L"0",PDAjrSwAtv=L"0",TMActSpd=L"2476",DFCShiftLvrPlbtyErr=L"0",OBCInpVolOv=L"0",RemainOdo=L"61",BatMaPosRlySts=L"1",CDUType=L"10",Lattd_G=L"37.04184",DrTorqRqst=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"3",DCDCIntnlFlt=L"0",GearSts_G=L"14",VecChrgStsIndOn=L"0",zdsjxh=L"489",EvpEquPreVal=L"-59.9",InitaContIndOn_EPS=L"0",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"2",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"8",InvActTemp=L"34",BMSGenSts1RollCnt=L"4",SignalStrengthOne=L"4",StrWhAng=L"0.5",SignalStrengthTwo=L"0",TDEnblSts=L"0",createTime=L"1678171310052",ACRelayCmd=L"0",BatDvcVlt_G1=L"94.3",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",dqsjxh=L"58",MCUSupCode=L"8440176",HzrdLtIO=L"0",VecActPow=L"0",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",OBCOtpVlt=L"94.2",BatMaxCelVolPos=L"3",t2RTime=L"2023-03-07 14:41:50",InitaContIndOn_OBC=L"0",TalCur_G=L"1.3",BatAvgTemp=L"23",OBCInpCurOv=L"0",ChargPrtOpInOnPrmt=L"0",ChrgCtrlSnglFail=L"0",KyPstn=L"2",MCUActFltNum=L"0",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"33.1",BatMinTemp_G=L"23",ModeCode=L"85",BMSChrgCurRqst=L"0",RtTrnLmpAtv=L"0",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",DFCGearReqMongErr=L"0",DFCCASmax=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",BatMinClVltCode_G=L"15",LSDAjrSwAtv=L"0",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",TMWkStsRqst=L"2",OBCOtpVolOv=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"0",TMTorqRqst=L"0",ShifGearFailAtHiSpeed=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"9",VehOdo_G=L"11857",BatTempSts=L"0",LowBatVol=L"13.89",VCUBatThrmlRunwyAlmInd=L"0",VecTMActSpd=L"2477",DCDCOtpVolSts=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"25",EvrmetAblPreVal=L"100.9",BatIslatRes=L"40489",BatMiniCelVolPos=L"15",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",VecSOC=L"36",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"177",WhlGrndVlctyRtNnDrvn=L"33.13",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"33.125",RemtCtrlSpdLmtRqstSt=L"1",VecActPowPer=L"0",BatChrgVolCpl=L"106.9",InitaContIndOn_IC=L"0",DFCUAcePedlPlbtyErr=L"0",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",VCUCtrlRqst1RollCnt=L"1",ACSwitch=L"0",TMSts_G=L"4",LftTrnLmpAtv=L"0",RealSoc=L"38",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"8.1",Direction=L"0",RemtCtrlPrkHet=L"0",CPSts=L"0",HzrdLgtSwAtv=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",TDAjrSwAtv=L"0",DCDCInpCur=L"0.9",BatTalVolSm=L"94.3",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",VBatChrgStsIndOn=L"1",BatCurSensFlt=L"0",AirbgIndOn=L"0",TMCtrlMdRqst=L"2",OBCInpMxCur=L"0",BatEnrgAvail=L"4.8",BatTempSensNum_G1=L"6",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",IgnKyPstn=L"2",BatDvcCur_G1=L"1.3",TerminalNO=L"7683202652375880403N4133445",InvVol=L"95",InvJctTemp=L"34",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"11",MCUCurFlt=L"0",BatExtVol=L"95.2",DrSbltAtcV=L"0",TMNum_G=L"1",OBCOvTemp=L"0",SupCode=L"2",DFCTioutForVT=L"0",Bat2sPlsChrgPowAvail=L"11",vehConfig=L"LV1",VecActGearSts=L"1",VehConf=L"3",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"33.07",WhlGrndVlctyRtDrvn=L"33.07",OBCInpVlt=L"0",BatMaxTemp=L"25",EvpRlySta=L"0",IALPwrMdCtrlSt=L"0",TMActTemp=L"44",PlsAgainToRdy=L"0",HandBrkSts=L"0",IslatRes_G=L"40489",KyOpenSwAct=L"0",objType=L"E50",BatContiDischrgPowAvail=L"14.79",InitaContIndOn_MCU=L"0",VecIslateWrngIndOn=L"0",DFCMCUGen1TiOut=L"0",Bat2sPlsDischrgPowAvail=L"14.79",MCUCtrlFlt=L"0",BatChrgTims=L"80",VacPumpWrngIndOn=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",InitaContIndOn_BMS=L"0",TalClNum_G1=L"29",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",EPSWorkCur=L"0",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",DrSbltAtc=L"1",BatMiniCelVol=L"3251",TemperoHeatMeb3=L"205",TMDeratingSts=L"0",OBCORDCACIntnlTemp=L"30",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"23",TemperoHeatMeb7=L"205",BatMinTemp=L"23",TemperoHeatMeb6=L"205",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"205",PDUHVLock=L"1",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"0",vehType=L"E50",Longitude=L"118.554482",DFCDCANMUTE=L"0",WhlGrndVlctyLftDrvn=L"33.1",BrkProcInPrgrsIO=L"0",TMActWkSts=L"3",CDUState=L"5",objId=L"LK6ADAE10NB216967",PTID=L"2",ABSAtv=L"0",EngyRevIndOn=L"0",BrakPedalPos=L"0",TMTemp_G=L"44",BatCelVolSensFlt=L"0",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",BatIslateSts=L"0",AclrtrPedaStrk_G=L"9",TMOvSpdInd=L"0",AnThWaSt=L"1",DCDCOtpVol=L"13.8",RrFgLtIO=L"0",GPSspeed=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"5",BatMinTempCode_G=L"3",CCSts=L"0",IgnKyInstAtv=L"0",Latitude=L"37.04184",BMSHvPowOnRqst=L"2",VecSOCLoWrngIndOn=L"0",TMActDrvTorLmt=L"77",BatMaxClVltCode_G=L"3",CollisionSig=L"0",VecStatRdy=L"1",BatMaxCelVol=L"3258",LwBmIO=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",DCDCTempWrng=L"0",BCMRunMod=L"0",BatSubsyst_Vlt_G=L"1",BMSSts=L"3",DriverLeftWarning=L"0",BatRmaChrgTim=L"2047",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"1.3",FtFgLtIO=L"0",BatAvgCelVol=L"3254",Swit2Status=L"0",t1RTime=L"2023-03-07 14:41:50",VacPumpTransRate=L"1",BCMSftwrVsinNum=L"2",BatHetPrsrvtn=L"0",BatOthFltNum=L"0",VecFltIndOn=L"0",VecActTalCur=L"1.2",ISBF=L"0",BatClVlt_G1=L"3.257_3.255_3.258_3.252_3.256_3.256_3.256_3.256_3.256_3.256_3.256_3.252_3.253_3.253_3.251_3.254_3.253_3.255_3.255_3.254_3.255_3.255_3.254_3.253_3.253_3.255_3.253_3.254_3.256",BatFltLvl=L"0",MCUInpVlt_G=L"95",BatMaxTempPos=L"1",TMActFbTorLmt=L"-77.25",DrvPowLimtIndOn=L"0",BatAvgCelVolSts=L"0",OBCOtpCur=L"0",VCUSftwrVsinNum=L"162",DDAjrSwAtv=L"0",TalVlt_G=L"94.3",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"11",InvVolSts=L"0",VehOdoV=L"0",VecOptMod=L"3",SoftEdt=L"170",RSDAjrSwAtv=L"0",TMorMCUOvTempInd=L"0",OptMod_G=L"1",MCUTemp_G=L"34",TMSpdRqst=L"0",VecChrgRqst=L"0",VecTMActSpdV=L"0",RLDoorOpenSwAct=L"0",MCUVolFlt=L"0",TMSpd_G=L"2476",OBCInpCur=L"0",OBCInpVolUn=L"0",DCDCOtpCur=L"5.9",BMSChrgVolCurRqstAnorm=L"1",InitaContIndOn_ABS=L"0",CollectTime=L"2023-03-07 14:41:49",BatMinClVlt_G=L"3.251",RatedEgy=L"13.9",NoPulOutChrgPlugWrng=L"0",PsDoorOpenSwAct=L"0",AntiSlope=L"0",HVRelSta=L"2",MaxWrngLvl_G=L"0",BatheatindON=L"0",AccActPos=L"9",InitaContIndOn_BCM=L"0",TrnSwAct=L"0",VehSts_G=L"1",VehSpdAvgDrvn=L"33.140625",InitaContIndOn_SDM=L"0",HhBmIO=L"0",t02RDelay=L"1",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Lngtd_G=L"118.554482",BatThrmlRunwyAlmInd=L"0",StpLpSt=L"0",PrkBrkSwAtv=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:49",BatMaxClVlt_G=L"3.258",AcInpFlt=L"0",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"5216",BatMinTemVolPakOrd=L"1",BatSOH=L"100",DrivStOccSt=L"1",BatSOC=L"36",VehOdo=L"11857",t2RIP=L"192.168.30.106",BatMaxTempCode_G=L"1",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"0",WindscenWipSt=L"0",LdspkSt=L"0",BatTempSensFlt=L"0",DFCMongHvNotIntd=L"0",DCDCInpVol=L"94.2",WipSwStat=L"1",BatIntVol=L"95.4",VCURemtCtrlSpdLmtFb=L"0",InvrCur=L"0",BatMinTempPos=L"3",VecChrgingSts=L"0",Bat10sPlsDischrgPowAvail=L"14.79",SftwrMjrVsin=L"9",FtPnLgtAtv=L"0",RatedVol=L"92",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",TMTorq_G=L"0",BatInfLen=L"0",BatChrgSts=L"0",InitaContIndOn_UCU=L"0",BCMRunModV=L"1",SOC_G=L"36",BatSubsysTemp_G1=L"24_25_23_23_23_23",PowBatErrIndOn=L"0",BatSOCSTS=L"0",DCDCInternalTemp=L"33",MaiLgtSw=L"0",MinTempSubSysNum_G=L"1",TMActTorq=L"0",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:49",VehDrvMod=L"1",BatManuCode=L"3158603",MESSAGETYPE=L"REALTIME",TalNumOfClBatInThisFrm_G1=L"29",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DrDoorOpenSwAct=L"0",DFCDOPIN56Flt=L"0",RemtHeatModRqst=L"0" 1678171309000',
        #              'E50,VIN=LK6ADAE12ME547871 TalClNum_G1=L"29",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",BMSCode=L"0",EPSWorkCur=L"1.25",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",TMOrd_G=L"1",DrSbltAtc=L"0",RrImpctDet=L"0",BatMiniCelVol=L"3363",OBCORDCACIntnlTemp=L"38",TMDeratingSts=L"0",TemperoHeatMeb3=L"205",BattCellVoltage_16=L"3.374",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"34",TemperoHeatMeb7=L"205",TemperoHeatMeb6=L"205",BatMinTemp=L"20",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"205",PDUHVLock=L"1",BattCellVoltage_17=L"3.363",BatFltNumN1_G=L"0",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"1",BatMaNegRlySts=L"0",VecActTalVol=L"98.1",MCUCur_G=L"0",vehType=L"E50",Longitude=L"106.093013",WhlGrndVlctyLftDrvn=L"0",BatPowSts=L"1",DFCDCANMUTE=L"0",TMActWkSts=L"2",BrkProcInPrgrsIO=L"0",CDUState=L"2",TMActSpd=L"0",objId=L"LK6ADAE12ME547871",PTID=L"2",DFCShiftLvrPlbtyErr=L"0",ABSAtv=L"0",OBCInpVolOv=L"0",EngyRevIndOn=L"0",RemainOdo=L"170",BrakPedalPos=L"0",BatMaPosRlySts=L"0",TMTemp_G=L"14",BatCelVolSensFlt=L"0",CDUType=L"9",Lattd_G=L"35.269996",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",DrTorqRqst=L"0",BatIslateSts=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"1",DCDCIntnlFlt=L"0",GearSts_G=L"15",VecChrgStsIndOn=L"0",zdsjxh=L"69",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",EvpEquPreVal=L"-37.3",InitaContIndOn_EPS=L"1",BatHeSts=L"1",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"1",DCDCOtpVol=L"14",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"1",InvActTemp=L"18",BMSGenSts1RollCnt=L"5",GPSspeed=L"0",SignalStrengthOne=L"4",StrWhAng=L"4.875",SignalStrengthTwo=L"30",RemtCtrParkHeat=L"0",createTime=L"1678171310049",ACRelayCmd=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"15",BatMinTempCode_G=L"3",CCSts=L"2",BatDvcVlt_G1=L"98.1",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",MCUSupCode=L"8440176",dqsjxh=L"10265",Latitude=L"35.269996",VecActPow=L"0",BMSHvPowOnRqst=L"2",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",VecSOCLoWrngIndOn=L"0",OBCOtpVlt=L"109.8",BatMaxCelVolPos=L"1",t2RTime=L"2023-03-07 14:41:50",TMActDrvTorLmt=L"84.75",InitaContIndOn_OBC=L"0",TalCur_G=L"0",BatAvgTemp=L"20",OBCInpCurOv=L"0",BatMaxClVltCode_G=L"1",CollisionSig=L"0",VecStatRdy=L"0",BatMaxCelVol=L"3410",ChrgCtrlSnglFail=L"0",KyPstn=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",MCUActFltNum=L"0",DCDCTempWrng=L"0",BatSubsyst_Vlt_G=L"1",BatHeRlySts=L"1",VehSpdAvgDrvn_G=L"0",BMSSts=L"2",DriverLeftWarning=L"0",BatMinTemp_G=L"20",BatRmaChrgTim=L"0",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",ModeCode=L"85",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"0",BMSChrgCurRqst=L"15",Swit2Status=L"1",BatAvgCelVol=L"3382",t1RTime=L"2023-03-07 14:41:49",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",VacPumpTransRate=L"1",DFCGearReqMongErr=L"0",BatHetPrsrvtn=L"0",DFCCASmax=L"0",BatOthFltNum=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",VecFltIndOn=L"0",BatMinClVltCode_G=L"17",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",VecActTalCur=L"0",ISBF=L"0",BatClVlt_G1=L"3.41_3.389_3.402_3.4_3.383_3.404_3.396_3.383_3.393_3.371_3.364_3.373_3.369_3.372_3.37_3.374_3.363_3.371_3.376_3.386_3.373_3.392_3.385_3.382_3.401_3.376_3.385_3.389_3.366",BatFltLvl=L"0",MCUInpVlt_G=L"109",TMWkStsRqst=L"0",BatMaxTempPos=L"2",TMActFbTorLmt=L"-85",DrvPowLimtIndOn=L"0",OBCOtpVolOv=L"0",BatAvgCelVolSts=L"0",VCUSftwrVsinNum=L"233",OBCOtpCur=L"4.3",BatTalVolSts=L"0",BMSChrgVolRqst=L"110",TalVlt_G=L"98.1",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"0",TMTorqRqst=L"0",InvVolSts=L"0",VehOdoV=L"0",ShifGearFailAtHiSpeed=L"0",VecOptMod=L"4",SoftEdt=L"151",TMorMCUOvTempInd=L"0",OptMod_G=L"1",RemtCtrlSpdLimtRqstSt=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"4",VehOdo_G=L"7435.9",MCUTemp_G=L"18",TMSpdRqst=L"0",LowBatVol=L"14.15",BatTempSts=L"0",VecChrgRqst=L"1",VecTMActSpdV=L"0",VecTMActSpd=L"0",VCUBatThrmlRunwyAlmInd=L"0",MCUVolFlt=L"0",DCDCOtpVolSts=L"0",TMSpd_G=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"22",EvrmetAblPreVal=L"82.8",OBCInpCur=L"2",BatIslatRes=L"40455",BatMiniCelVolPos=L"17",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",OBCInpVolUn=L"0",VecSOC=L"100",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCOtpCur=L"1.7",InitaContIndOn_ABS=L"1",BMSChrgVolCurRqstAnorm=L"0",CollectTime=L"2023-03-07 14:41:49",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"14",BatMinClVlt_G=L"3.363",RatedEgy=L"13.9",WhlGrndVlctyRtNnDrvn=L"0",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"0",NoPulOutChrgPlugWrng=L"0",AntiSlope=L"0",HVRelSta=L"0",VecActPowPer=L"0",BatChrgVolCpl=L"106.9",MaxWrngLvl_G=L"0",InitaContIndOn_IC=L"1",BatheatindON=L"1",DFCUAcePedlPlbtyErr=L"0",AccActPos=L"0",PTCSwitch=L"2",MaxVltBatSubSysNum_G=L"1",ACSwitch=L"0",VCUCtrlRqst1RollCnt=L"11",TMSts_G=L"4",VehSts_G=L"2",VehSpdAvgDrvn=L"0",InitaContIndOn_SDM=L"1",RealSoc=L"100",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"22.6",t02RDelay=L"1",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Direction=L"0",Lngtd_G=L"106.093013",RemtCtrlPrkHet=L"0",CPSts=L"2",BatThrmlRunwyAlmInd=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:49",DCDCInpCur=L"0.2",BatTalVolSm=L"98.1",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",BatMaxClVlt_G=L"3.41",AcInpFlt=L"0",VBatChrgStsIndOn=L"1",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"2316",BatCurSensFlt=L"0",AirbgIndOn=L"0",BatMinTemVolPakOrd=L"1",TMCtrlMdRqst=L"2",OBCInpMxCur=L"7",DrivStOccSt=L"1",BatSOH=L"100",BatSOC=L"100",BatEnrgAvail=L"13.4",VehOdo=L"7435.90625",t2RIP=L"192.168.20.102",BatTempSensNum_G1=L"6",BatMaxTempCode_G=L"2",TmnlWkupSurc=L"3",OBCHdErr=L"0",TMActPow=L"0",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",BatTempSensFlt=L"0",BatDvcCur_G1=L"0",TerminalNO=L"7683202652375855703M9132997",InvVol=L"109",InvJctTemp=L"18",DFCMongHvNotIntd=L"0",DCDCInpVol=L"109.8",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"0",MCUCurFlt=L"0",BatExtVol=L"109.4",DrSbltAtcV=L"0",BatIntVol=L"98.1",TMNum_G=L"1",OBCOvTemp=L"0",VCURemtCtrlSpdLmtFb=L"0",SupCode=L"0",DFCTioutForVT=L"0",InvrCur=L"0",Bat2sPlsChrgPowAvail=L"0",vehConfig=L"LV1",VecActGearSts=L"3",VehConf=L"3",BatMinTempPos=L"3",CellBattNum_21=L"21",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"0",VecChrgingSts=L"1",Bat10sPlsDischrgPowAvail=L"18",WhlGrndVlctyRtDrvn=L"0",SftwrMjrVsin=L"9",OBCInpVlt=L"236",RatedVol=L"92",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",BatMaxTemp=L"22",EvpRlySta=L"0",TMTorq_G=L"0",BatInfLen=L"0",TMActTemp=L"14",PlsAgainToRdy=L"0",BatChrgSts=L"1",HandBrkSts=L"1",InitaContIndOn_UCU=L"1",SOC_G=L"100",IslatRes_G=L"40455",objType=L"E50",BatSubsysTemp_G1=L"21_22_20_21_20_21",BatContiDischrgPowAvail=L"18",InitaContIndOn_MCU=L"1",PowBatErrIndOn=L"0",BatSOCSTS=L"0",VecIslateWrngIndOn=L"0",DCDCInternalTemp=L"34",DFCMCUGen1TiOut=L"0",MinTempSubSysNum_G=L"1",Bat2sPlsDischrgPowAvail=L"18",TMActTorq=L"0",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:49",VehDrvMod=L"1",MCUCtrlFlt=L"0",BatChrgTims=L"42",BatManuCode=L"3158603",MESSAGETYPE=L"REALTIME",VacPumpWrngIndOn=L"0",TalNumOfClBatInThisFrm_G1=L"29",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DFCDOPIN56Flt=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",RemtHeatModRqst=L"0",InitaContIndOn_BMS=L"1" 1678171309000',
        #              'E50,VIN=LK6ADCE15LE211979 TalClNum_G1=L"26",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",BMSCode=L"0",EPSWorkCur=L"0.75",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",TMOrd_G=L"1",DrSbltAtc=L"0",RrImpctDet=L"0",BatMiniCelVol=L"4143",OBCORDCACIntnlTemp=L"48",TMDeratingSts=L"0",TemperoHeatMeb3=L"205",BattCellVoltage_16=L"4.161",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",TemperoHeatMeb6=L"205",BatMinTemp=L"20",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"205",PDUHVLock=L"1",BattCellVoltage_17=L"4.162",BatFltNumN1_G=L"0",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"1",BatMaNegRlySts=L"1",VecActTalVol=L"107.9",MCUCur_G=L"0",vehType=L"E50",Longitude=L"117.320254",WhlGrndVlctyLftDrvn=L"0",BatPowSts=L"1",DFCDCANMUTE=L"0",TMActWkSts=L"2",BrkProcInPrgrsIO=L"0",CDUState=L"2",TMActSpd=L"0",objId=L"LK6ADCE15LE211979",PTID=L"2",DFCShiftLvrPlbtyErr=L"0",ABSAtv=L"0",OBCInpVolOv=L"0",EngyRevIndOn=L"0",RemainOdo=L"107",BrakPedalPos=L"0",BatMaPosRlySts=L"1",TMTemp_G=L"22",BatCelVolSensFlt=L"0",CDUType=L"9",Lattd_G=L"39.430714",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",DrTorqRqst=L"0",BatIslateSts=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"1",DCDCIntnlFlt=L"0",GearSts_G=L"15",VecChrgStsIndOn=L"1",zdsjxh=L"11",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",EvpEquPreVal=L"-57.4",InitaContIndOn_EPS=L"1",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"1",DCDCOtpVol=L"14.1",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"9",InvActTemp=L"24",BMSGenSts1RollCnt=L"11",GPSspeed=L"0",SignalStrengthOne=L"4",StrWhAng=L"22.25",SignalStrengthTwo=L"31",RemtCtrParkHeat=L"0",createTime=L"1678171310057",ACRelayCmd=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"7",BatMinTempCode_G=L"1",CCSts=L"2",BatDvcVlt_G1=L"107.9",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",MCUSupCode=L"8340040",dqsjxh=L"1889",Latitude=L"39.430714",VecActPow=L"0",BMSHvPowOnRqst=L"2",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",VecSOCLoWrngIndOn=L"0",OBCOtpVlt=L"108.5",BatMaxCelVolPos=L"16",t2RTime=L"2023-03-07 14:41:50",TMActDrvTorLmt=L"84.75",InitaContIndOn_OBC=L"0",TalCur_G=L"-9.5",BatAvgTemp=L"20",OBCInpCurOv=L"0",BatMaxClVltCode_G=L"16",CollisionSig=L"0",VecStatRdy=L"0",BatMaxCelVol=L"4162",ChrgCtrlSnglFail=L"0",KyPstn=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",MCUActFltNum=L"0",DCDCTempWrng=L"0",BatSubsyst_Vlt_G=L"1",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"0",BMSSts=L"2",DriverLeftWarning=L"0",BatMinTemp_G=L"20",BatRmaChrgTim=L"22",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",ModeCode=L"134",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"-9.5",BMSChrgCurRqst=L"10",Swit2Status=L"1",BatAvgCelVol=L"4152",t1RTime=L"2023-03-07 14:41:50",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",VacPumpTransRate=L"1",DFCGearReqMongErr=L"0",BatHetPrsrvtn=L"0",DFCCASmax=L"0",BatOthFltNum=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",VecFltIndOn=L"0",BatMinClVltCode_G=L"13",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",VecActTalCur=L"-9.5",ISBF=L"0",BatClVlt_G1=L"4.146_4.15_4.149_4.151_4.152_4.152_4.146_4.152_4.144_4.146_4.145_4.145_4.144_4.156_4.157_4.161_4.162_4.161_4.158_4.158_4.156_4.16_4.16_4.156_4.153_4.154",BatFltLvl=L"0",MCUInpVlt_G=L"108",TMWkStsRqst=L"0",BatMaxTempPos=L"6",TMActFbTorLmt=L"-85",DrvPowLimtIndOn=L"0",OBCOtpVolOv=L"0",BatAvgCelVolSts=L"0",VCUSftwrVsinNum=L"228",OBCOtpCur=L"9.9",BatTalVolSts=L"0",BMSChrgVolRqst=L"110",TalVlt_G=L"107.9",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"5.88",TMTorqRqst=L"0",InvVolSts=L"0",VehOdoV=L"0",ShifGearFailAtHiSpeed=L"0",VecOptMod=L"4",SoftEdt=L"153",TMorMCUOvTempInd=L"0",OptMod_G=L"1",RemtCtrlSpdLimtRqstSt=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"2",VehOdo_G=L"23738",MCUTemp_G=L"24",TMSpdRqst=L"0",LowBatVol=L"14.15",BatTempSts=L"0",VecChrgRqst=L"1",VecTMActSpdV=L"0",VecTMActSpd=L"0",VCUBatThrmlRunwyAlmInd=L"0",MCUVolFlt=L"0",DCDCOtpVolSts=L"0",TMSpd_G=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"22",EvrmetAblPreVal=L"100.5",OBCInpCur=L"5",BatIslatRes=L"10025",BatMiniCelVolPos=L"13",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",OBCInpVolUn=L"0",VecSOC=L"91",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCOtpCur=L"2.1",InitaContIndOn_ABS=L"1",BMSChrgVolCurRqstAnorm=L"0",CollectTime=L"2023-03-07 14:41:50",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"122",BatMinClVlt_G=L"4.143",RatedEgy=L"9.5",WhlGrndVlctyRtNnDrvn=L"0",DCDCStsWrng=L"0",TMActCtrlMdSts=L"0",VehSpdAvgDrvnRd=L"0",NoPulOutChrgPlugWrng=L"0",AntiSlope=L"0",HVRelSta=L"0",VecActPowPer=L"0",BatChrgVolCpl=L"110",MaxWrngLvl_G=L"0",InitaContIndOn_IC=L"1",BatheatindON=L"0",DFCUAcePedlPlbtyErr=L"0",AccActPos=L"0",PTCSwitch=L"2",MaxVltBatSubSysNum_G=L"1",ACSwitch=L"0",VCUCtrlRqst1RollCnt=L"11",TMSts_G=L"4",VehSts_G=L"2",VehSpdAvgDrvn=L"0",InitaContIndOn_SDM=L"1",RealSoc=L"91",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"3.4",t02RDelay=L"0",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"0",Direction=L"0",Lngtd_G=L"117.320254",RemtCtrlPrkHet=L"0",CPSts=L"2",BatThrmlRunwyAlmInd=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:50",DCDCInpCur=L"0.2",BatTalVolSm=L"107.9",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",BatMaxClVlt_G=L"4.162",AcInpFlt=L"0",VBatChrgStsIndOn=L"1",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"7420",BatCurSensFlt=L"0",AirbgIndOn=L"0",BatMinTemVolPakOrd=L"1",TMCtrlMdRqst=L"2",OBCInpMxCur=L"7",DrivStOccSt=L"1",BatSOH=L"97",BatSOC=L"91",BatEnrgAvail=L"7.1",VehOdo=L"23738",t2RIP=L"192.168.20.105",BatTempSensNum_G1=L"6",BatMaxTempCode_G=L"6",TmnlWkupSurc=L"3",OBCHdErr=L"0",TMActPow=L"0",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",BatTempSensFlt=L"0",BatDvcCur_G1=L"-9.5",TerminalNO=L"7681100322366106200L9091593",InvVol=L"108",InvJctTemp=L"24",DFCMongHvNotIntd=L"0",DCDCInpVol=L"108.5",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"5.88",MCUCurFlt=L"0",BatExtVol=L"107.9",DrSbltAtcV=L"0",BatIntVol=L"107.9",TMNum_G=L"1",OBCOvTemp=L"0",VCURemtCtrlSpdLmtFb=L"0",SupCode=L"0",DFCTioutForVT=L"0",InvrCur=L"0",Bat2sPlsChrgPowAvail=L"5.88",vehConfig=L"LV0",VecActGearSts=L"3",VehConf=L"3",BatMinTempPos=L"1",CellBattNum_21=L"21",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"0",VecChrgingSts=L"1",Bat10sPlsDischrgPowAvail=L"18",WhlGrndVlctyRtDrvn=L"0",SftwrMjrVsin=L"3",OBCInpVlt=L"221",RatedVol=L"96",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",BatMaxTemp=L"22",EvpRlySta=L"0",TMTorq_G=L"0",BatInfLen=L"0",TMActTemp=L"22",PlsAgainToRdy=L"0",BatChrgSts=L"1",HandBrkSts=L"1",InitaContIndOn_UCU=L"1",SOC_G=L"91",IslatRes_G=L"10025",objType=L"E50",BatSubsysTemp_G1=L"20_20_20_20_20_22",BatContiDischrgPowAvail=L"18",InitaContIndOn_MCU=L"1",PowBatErrIndOn=L"0",BatSOCSTS=L"0",VecIslateWrngIndOn=L"0",DCDCInternalTemp=L"49",DFCMCUGen1TiOut=L"0",MinTempSubSysNum_G=L"1",Bat2sPlsDischrgPowAvail=L"18",TMActTorq=L"0",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:50",VehDrvMod=L"1",MCUCtrlFlt=L"0",BatChrgTims=L"207",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",VacPumpWrngIndOn=L"0",TalNumOfClBatInThisFrm_G1=L"26",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DFCDOPIN56Flt=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",RemtHeatModRqst=L"0",InitaContIndOn_BMS=L"1" 1678171310000',
        #              'E50,VIN=LK6ADCE16LE215202 TalClNum_G1=L"28",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"10",VCUVehDrvMod=L"0",BMSCode=L"0",EPSWorkCur=L"0",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",TMOrd_G=L"1",DrSbltAtc=L"1",RrImpctDet=L"0",BatMiniCelVol=L"3346",OBCORDCACIntnlTemp=L"45",TMDeratingSts=L"0",TemperoHeatMeb3=L"205",BattCellVoltage_16=L"3.359",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",TemperoHeatMeb6=L"205",BatMinTemp=L"23",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"205",PDUHVLock=L"1",BattCellVoltage_17=L"3.358",BatFltNumN1_G=L"0",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"1",BatMaNegRlySts=L"1",VecActTalVol=L"93.9",MCUCur_G=L"0",vehType=L"E50",Longitude=L"110.673602",WhlGrndVlctyLftDrvn=L"0",BatPowSts=L"1",DFCDCANMUTE=L"0",TMActWkSts=L"2",BrkProcInPrgrsIO=L"0",CDUState=L"2",TMActSpd=L"0",objId=L"LK6ADCE16LE215202",PTID=L"2",DFCShiftLvrPlbtyErr=L"0",ABSAtv=L"0",OBCInpVolOv=L"0",EngyRevIndOn=L"0",RemainOdo=L"63",BrakPedalPos=L"0",BatMaPosRlySts=L"1",TMTemp_G=L"30",BatCelVolSensFlt=L"0",CDUType=L"9",Lattd_G=L"31.748172",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",DrTorqRqst=L"0",BatIslateSts=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"1",DCDCIntnlFlt=L"0",GearSts_G=L"15",VecChrgStsIndOn=L"1",zdsjxh=L"8",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",EvpEquPreVal=L"-57.8",InitaContIndOn_EPS=L"1",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"1",DCDCOtpVol=L"14.1",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"0",InvActTemp=L"25",BMSGenSts1RollCnt=L"9",GPSspeed=L"0",SignalStrengthOne=L"4",StrWhAng=L"-291.5",SignalStrengthTwo=L"28",RemtCtrParkHeat=L"0",createTime=L"1678171310041",ACRelayCmd=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"14",BatMinTempCode_G=L"1",CCSts=L"2",BatDvcVlt_G1=L"93.8",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",MCUSupCode=L"8310270",dqsjxh=L"2071",Latitude=L"31.748172",VecActPow=L"0",BMSHvPowOnRqst=L"2",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",VecSOCLoWrngIndOn=L"0",OBCOtpVlt=L"94.5",BatMaxCelVolPos=L"16",t2RTime=L"2023-03-07 14:41:50",TMActDrvTorLmt=L"85",InitaContIndOn_OBC=L"0",TalCur_G=L"-15.4",BatAvgTemp=L"23",OBCInpCurOv=L"0",BatMaxClVltCode_G=L"16",CollisionSig=L"0",VecStatRdy=L"0",BatMaxCelVol=L"3359",ChrgCtrlSnglFail=L"0",KyPstn=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",MCUActFltNum=L"0",DCDCTempWrng=L"0",BatSubsyst_Vlt_G=L"1",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"0",BMSSts=L"2",DriverLeftWarning=L"0",BatMinTemp_G=L"23",BatRmaChrgTim=L"220",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",ModeCode=L"136",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"-15.4",BMSChrgCurRqst=L"40",Swit2Status=L"1",BatAvgCelVol=L"3354",t1RTime=L"2023-03-07 14:41:50",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",VacPumpTransRate=L"1",DFCGearReqMongErr=L"0",BatHetPrsrvtn=L"0",DFCCASmax=L"0",BatOthFltNum=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",VecFltIndOn=L"0",BatMinClVltCode_G=L"4",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",VecActTalCur=L"-15.4",ISBF=L"0",BatClVlt_G1=L"3.352_3.354_3.352_3.346_3.356_3.357_3.354_3.351_3.35_3.353_3.354_3.356_3.352_3.353_3.355_3.359_3.358_3.356_3.354_3.355_3.355_3.354_3.357_3.358_3.356_3.352_3.357_3.355",BatFltLvl=L"0",MCUInpVlt_G=L"91",TMWkStsRqst=L"0",BatMaxTempPos=L"6",TMActFbTorLmt=L"-85",DrvPowLimtIndOn=L"0",OBCOtpVolOv=L"0",BatAvgCelVolSts=L"0",VCUSftwrVsinNum=L"228",OBCOtpCur=L"15.8",BatTalVolSts=L"0",BMSChrgVolRqst=L"103.2",TalVlt_G=L"93.8",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"10.6",TMTorqRqst=L"0",InvVolSts=L"0",VehOdoV=L"0",ShifGearFailAtHiSpeed=L"0",VecOptMod=L"4",SoftEdt=L"153",TMorMCUOvTempInd=L"0",OptMod_G=L"1",RemtCtrlSpdLimtRqstSt=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"22",VehOdo_G=L"12125",MCUTemp_G=L"25",TMSpdRqst=L"0",LowBatVol=L"14.1",BatTempSts=L"0",VecChrgRqst=L"1",VecTMActSpdV=L"0",VecTMActSpd=L"0",VCUBatThrmlRunwyAlmInd=L"0",MCUVolFlt=L"0",DCDCOtpVolSts=L"0",TMSpd_G=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"25",EvrmetAblPreVal=L"90.6",OBCInpCur=L"6",BatIslatRes=L"10039",BatMiniCelVolPos=L"4",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",OBCInpVolUn=L"0",VecSOC=L"56",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCOtpCur=L"1.9",InitaContIndOn_ABS=L"1",BMSChrgVolCurRqstAnorm=L"0",CollectTime=L"2023-03-07 14:41:50",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"181",BatMinClVlt_G=L"3.346",RatedEgy=L"9.5",WhlGrndVlctyRtNnDrvn=L"0",DCDCStsWrng=L"0",TMActCtrlMdSts=L"0",VehSpdAvgDrvnRd=L"0",NoPulOutChrgPlugWrng=L"0",AntiSlope=L"0",HVRelSta=L"0",VecActPowPer=L"0",BatChrgVolCpl=L"103.2",MaxWrngLvl_G=L"0",InitaContIndOn_IC=L"1",BatheatindON=L"0",DFCUAcePedlPlbtyErr=L"0",AccActPos=L"0",PTCSwitch=L"2",MaxVltBatSubSysNum_G=L"1",ACSwitch=L"0",VCUCtrlRqst1RollCnt=L"8",TMSts_G=L"4",VehSts_G=L"2",VehSpdAvgDrvn=L"0",InitaContIndOn_SDM=L"1",RealSoc=L"58",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"3.4",t02RDelay=L"0",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"0",Direction=L"0",Lngtd_G=L"110.673602",RemtCtrlPrkHet=L"0",CPSts=L"2",BatThrmlRunwyAlmInd=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:50",DCDCInpCur=L"0.3",BatTalVolSm=L"93.9",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",BatMaxClVlt_G=L"3.359",AcInpFlt=L"0",VBatChrgStsIndOn=L"1",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"9603",BatCurSensFlt=L"0",AirbgIndOn=L"0",BatMinTemVolPakOrd=L"1",TMCtrlMdRqst=L"2",OBCInpMxCur=L"12",DrivStOccSt=L"1",BatSOH=L"99",BatSOC=L"56",BatEnrgAvail=L"4.5",VehOdo=L"12125",t2RIP=L"192.168.20.105",BatTempSensNum_G1=L"6",BatMaxTempCode_G=L"6",TmnlWkupSurc=L"3",OBCHdErr=L"0",TMActPow=L"0",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",BatTempSensFlt=L"0",BatDvcCur_G1=L"-15.4",TerminalNO=L"7681100322366106200L9093355",InvVol=L"91",InvJctTemp=L"25",DFCMongHvNotIntd=L"0",DCDCInpVol=L"94.5",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"10.6",MCUCurFlt=L"0",BatExtVol=L"93.8",DrSbltAtcV=L"0",BatIntVol=L"93.9",TMNum_G=L"1",OBCOvTemp=L"0",VCURemtCtrlSpdLmtFb=L"0",SupCode=L"0",DFCTioutForVT=L"0",InvrCur=L"0",Bat2sPlsChrgPowAvail=L"10.6",vehConfig=L"LV1",VecActGearSts=L"3",VehConf=L"3",BatMinTempPos=L"1",CellBattNum_21=L"21",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"0",VecChrgingSts=L"1",Bat10sPlsDischrgPowAvail=L"16",WhlGrndVlctyRtDrvn=L"0",SftwrMjrVsin=L"1",OBCInpVlt=L"236",RatedVol=L"90",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",BatMaxTemp=L"25",EvpRlySta=L"0",TMTorq_G=L"0",BatInfLen=L"0",TMActTemp=L"30",PlsAgainToRdy=L"0",BatChrgSts=L"1",HandBrkSts=L"1",InitaContIndOn_UCU=L"1",SOC_G=L"56",IslatRes_G=L"10039",objType=L"E50",BatSubsysTemp_G1=L"23_23_24_23_24_25",BatContiDischrgPowAvail=L"16",InitaContIndOn_MCU=L"1",PowBatErrIndOn=L"0",BatSOCSTS=L"0",VecIslateWrngIndOn=L"0",DCDCInternalTemp=L"44",DFCMCUGen1TiOut=L"0",MinTempSubSysNum_G=L"1",Bat2sPlsDischrgPowAvail=L"16",TMActTorq=L"0",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:50",VehDrvMod=L"1",MCUCtrlFlt=L"0",BatChrgTims=L"116",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",VacPumpWrngIndOn=L"0",TalNumOfClBatInThisFrm_G1=L"28",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DFCDOPIN56Flt=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",RemtHeatModRqst=L"0",InitaContIndOn_BMS=L"1" 1678171310000',
        #              'E50,VIN=LK6ADAE14LE176131 TalClNum_G1=L"28",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"10",VCUVehDrvMod=L"0",BMSCode=L"0",EPSWorkCur=L"0.75",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"1",ProtocolVersion=L"1_1_1",TMOrd_G=L"1",DrSbltAtc=L"1",RrImpctDet=L"0",BatMiniCelVol=L"3141",OBCORDCACIntnlTemp=L"34",TMDeratingSts=L"0",TemperoHeatMeb3=L"205",BattCellVoltage_16=L"3.154",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",TemperoHeatMeb6=L"205",BatMinTemp=L"26",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"205",PDUHVLock=L"1",BattCellVoltage_17=L"3.151",BatFltNumN1_G=L"0",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"88.2",MCUCur_G=L"98",vehType=L"E50",Longitude=L"109.750539",WhlGrndVlctyLftDrvn=L"49.66",BatPowSts=L"1",DFCDCANMUTE=L"0",TMActWkSts=L"3",BrkProcInPrgrsIO=L"0",CDUState=L"5",TMActSpd=L"3712",objId=L"LK6ADAE14LE176131",PTID=L"2",DFCShiftLvrPlbtyErr=L"0",ABSAtv=L"0",OBCInpVolOv=L"0",EngyRevIndOn=L"0",RemainOdo=L"50",BrakPedalPos=L"0",BatMaPosRlySts=L"1",TMTemp_G=L"46",BatCelVolSensFlt=L"0",CDUType=L"9",Lattd_G=L"18.642119",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",DrTorqRqst=L"21",BatIslateSts=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"3",DCDCIntnlFlt=L"0",GearSts_G=L"14",VecChrgStsIndOn=L"0",zdsjxh=L"1467",AclrtrPedaStrk_G=L"60",TMOvSpdInd=L"0",EvpEquPreVal=L"-68.8",InitaContIndOn_EPS=L"0",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"1",DCDCOtpVol=L"14",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"7",InvActTemp=L"35",BMSGenSts1RollCnt=L"14",GPSspeed=L"0",SignalStrengthOne=L"4",StrWhAng=L"2.625",SignalStrengthTwo=L"26",RemtCtrParkHeat=L"0",createTime=L"1678171310048",ACRelayCmd=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"5",BatMinTempCode_G=L"3",CCSts=L"0",BatDvcVlt_G1=L"88.2",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",MCUSupCode=L"8310270",dqsjxh=L"122",Latitude=L"18.642119",VecActPow=L"8.1",BMSHvPowOnRqst=L"2",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",VecSOCLoWrngIndOn=L"0",OBCOtpVlt=L"87.7",BatMaxCelVolPos=L"16",t2RTime=L"2023-03-07 14:41:50",TMActDrvTorLmt=L"51.25",InitaContIndOn_OBC=L"1",TalCur_G=L"103.1",BatAvgTemp=L"26",OBCInpCurOv=L"0",BatMaxClVltCode_G=L"16",CollisionSig=L"0",VecStatRdy=L"1",BatMaxCelVol=L"3156",ChrgCtrlSnglFail=L"0",KyPstn=L"2",BatPrechrgRlySts=L"0",BatFuSts=L"0",MCUActFltNum=L"0",DCDCTempWrng=L"0",BatSubsyst_Vlt_G=L"1",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"49.6",BMSSts=L"3",DriverLeftWarning=L"0",BatMinTemp_G=L"26",BatRmaChrgTim=L"0",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",ModeCode=L"137",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"103.1",BMSChrgCurRqst=L"0",Swit2Status=L"0",BatAvgCelVol=L"3151",t1RTime=L"2023-03-07 14:41:50",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",VacPumpTransRate=L"1",DFCGearReqMongErr=L"0",BatHetPrsrvtn=L"0",DFCCASmax=L"0",BatOthFltNum=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",VecFltIndOn=L"0",BatMinClVltCode_G=L"10",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",VecActTalCur=L"102.6",ISBF=L"0",BatClVlt_G1=L"3.151_3.152_3.15_3.148_3.149_3.15_3.149_3.152_3.151_3.141_3.15_3.151_3.145_3.151_3.152_3.154_3.151_3.152_3.153_3.155_3.151_3.153_3.151_3.153_3.151_3.155_3.151_3.155",BatFltLvl=L"0",MCUInpVlt_G=L"86",TMWkStsRqst=L"2",BatMaxTempPos=L"1",TMActFbTorLmt=L"-51.5",DrvPowLimtIndOn=L"0",OBCOtpVolOv=L"0",BatAvgCelVolSts=L"0",VCUSftwrVsinNum=L"228",OBCOtpCur=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"88.2",TalVlt_G=L"88.2",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"11",TMTorqRqst=L"21",InvVolSts=L"0",VehOdoV=L"0",ShifGearFailAtHiSpeed=L"0",VecOptMod=L"3",SoftEdt=L"153",TMorMCUOvTempInd=L"0",OptMod_G=L"1",RemtCtrlSpdLimtRqstSt=L"1",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"22",VehOdo_G=L"40369",MCUTemp_G=L"35",TMSpdRqst=L"0",LowBatVol=L"13.91",BatTempSts=L"0",VecChrgRqst=L"0",VecTMActSpdV=L"0",VecTMActSpd=L"3709",VCUBatThrmlRunwyAlmInd=L"0",MCUVolFlt=L"0",DCDCOtpVolSts=L"0",TMSpd_G=L"3712",BatChrgCurSts=L"0",BatMaxTemp_G=L"27",EvrmetAblPreVal=L"100.6",OBCInpCur=L"0",BatIslatRes=L"10005",BatMiniCelVolPos=L"10",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",OBCInpVolUn=L"0",VecSOC=L"32",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCOtpCur=L"17.6",InitaContIndOn_ABS=L"0",BMSChrgVolCurRqstAnorm=L"0",CollectTime=L"2023-03-07 14:41:52",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"54",BatMinClVlt_G=L"3.141",RatedEgy=L"14",WhlGrndVlctyRtNnDrvn=L"49.69",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"49.78125",NoPulOutChrgPlugWrng=L"0",AntiSlope=L"0",HVRelSta=L"2",VecActPowPer=L"40",BatChrgVolCpl=L"103.2",MaxWrngLvl_G=L"0",InitaContIndOn_IC=L"0",BatheatindON=L"0",DFCUAcePedlPlbtyErr=L"0",AccActPos=L"60",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",ACSwitch=L"1",VCUCtrlRqst1RollCnt=L"6",TMSts_G=L"4",VehSts_G=L"1",VehSpdAvgDrvn=L"49.640625",InitaContIndOn_SDM=L"0",RealSoc=L"38",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"3.4",t02RDelay=L"-2",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Direction=L"0",Lngtd_G=L"109.750539",RemtCtrlPrkHet=L"0",CPSts=L"0",BatThrmlRunwyAlmInd=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:52",DCDCInpCur=L"3",BatTalVolSm=L"88.2",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",BatMaxClVlt_G=L"3.156",AcInpFlt=L"0",VBatChrgStsIndOn=L"1",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"9991",BatCurSensFlt=L"0",AirbgIndOn=L"0",BatMinTemVolPakOrd=L"1",TMCtrlMdRqst=L"2",OBCInpMxCur=L"7",DrivStOccSt=L"1",BatSOH=L"98",BatSOC=L"32",BatEnrgAvail=L"4.3",VehOdo=L"40369",t2RIP=L"192.168.40.103",BatTempSensNum_G1=L"6",BatMaxTempCode_G=L"1",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"8.1",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",BatTempSensFlt=L"0",BatDvcCur_G1=L"103.1",TerminalNO=L"7681100322366106200L7122018",InvVol=L"86",InvJctTemp=L"35",DFCMongHvNotIntd=L"0",DCDCInpVol=L"87.7",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"11",MCUCurFlt=L"0",BatExtVol=L"87.9",DrSbltAtcV=L"0",BatIntVol=L"88.2",TMNum_G=L"1",OBCOvTemp=L"0",VCURemtCtrlSpdLmtFb=L"0",SupCode=L"0",DFCTioutForVT=L"0",InvrCur=L"98",Bat2sPlsChrgPowAvail=L"11",vehConfig=L"LV1",VecActGearSts=L"1",VehConf=L"3",BatMinTempPos=L"3",CellBattNum_21=L"21",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"49.78",VecChrgingSts=L"0",Bat10sPlsDischrgPowAvail=L"14.78",WhlGrndVlctyRtDrvn=L"49.75",SftwrMjrVsin=L"1",OBCInpVlt=L"0",RatedVol=L"90",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",BatMaxTemp=L"27",EvpRlySta=L"0",TMTorq_G=L"20.7",BatInfLen=L"0",TMActTemp=L"46",PlsAgainToRdy=L"0",BatChrgSts=L"0",HandBrkSts=L"0",InitaContIndOn_UCU=L"0",SOC_G=L"32",IslatRes_G=L"10005",objType=L"E50",BatSubsysTemp_G1=L"27_26_26_26_27_27",BatContiDischrgPowAvail=L"14.78",InitaContIndOn_MCU=L"0",PowBatErrIndOn=L"0",BatSOCSTS=L"0",VecIslateWrngIndOn=L"0",DCDCInternalTemp=L"37",DFCMCUGen1TiOut=L"0",MinTempSubSysNum_G=L"1",Bat2sPlsDischrgPowAvail=L"14.78",TMActTorq=L"20.75",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:52",VehDrvMod=L"1",MCUCtrlFlt=L"0",BatChrgTims=L"203",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",VacPumpWrngIndOn=L"0",TalNumOfClBatInThisFrm_G1=L"28",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DFCDOPIN56Flt=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",RemtHeatModRqst=L"0",InitaContIndOn_BMS=L"0" 1678171312000',
        #              'E50,VIN=LK6ADAE18MB311730 BMSCode=L"5",TMOrd_G=L"1",RRDoorOpenSwAct=L"0",RrImpctDet=L"0",BattCellVoltage_16=L"4.091",BattCellVoltage_17=L"4.088",BatFltNumN1_G=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"106.3",MCUCur_G=L"0",BatPowSts=L"1",TaPnLgtAtv=L"0",TrnsRvsSwSt=L"0",PDAjrSwAtv=L"0",TMActSpd=L"0",DFCShiftLvrPlbtyErr=L"0",OBCInpVolOv=L"0",RemainOdo=L"146",BatMaPosRlySts=L"1",CDUType=L"10",Lattd_G=L"22.642094",DrTorqRqst=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"1",DCDCIntnlFlt=L"0",GearSts_G=L"15",VecChrgStsIndOn=L"1",zdsjxh=L"679",EvpEquPreVal=L"-69.7",InitaContIndOn_EPS=L"1",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"2",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"15",InvActTemp=L"26",BMSGenSts1RollCnt=L"8",SignalStrengthOne=L"4",StrWhAng=L"4086.5",SignalStrengthTwo=L"30",TDEnblSts=L"0",createTime=L"1678171310071",ACRelayCmd=L"0",BatDvcVlt_G1=L"106.3",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",dqsjxh=L"7",MCUSupCode=L"8340040",HzrdLtIO=L"0",VecActPow=L"0",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",OBCOtpVlt=L"106.5",BatMaxCelVolPos=L"18",t2RTime=L"2023-03-07 14:41:50",InitaContIndOn_OBC=L"0",TalCur_G=L"-13.5",BatAvgTemp=L"20",OBCInpCurOv=L"0",ChargPrtOpInOnPrmt=L"1",ChrgCtrlSnglFail=L"0",KyPstn=L"0",MCUActFltNum=L"0",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"0",BatMinTemp_G=L"20",ModeCode=L"135",BMSChrgCurRqst=L"40",RtTrnLmpAtv=L"0",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",DFCGearReqMongErr=L"0",DFCCASmax=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",BatMinClVltCode_G=L"6",LSDAjrSwAtv=L"0",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",TMWkStsRqst=L"0",OBCOtpVolOv=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"110",TMTorqRqst=L"0",ShifGearFailAtHiSpeed=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"3",VehOdo_G=L"34386",BatTempSts=L"0",LowBatVol=L"13.91",VCUBatThrmlRunwyAlmInd=L"0",VecTMActSpd=L"0",DCDCOtpVolSts=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"21",EvrmetAblPreVal=L"100.3",BatIslatRes=L"10004",BatMiniCelVolPos=L"6",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",VecSOC=L"86",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"44",WhlGrndVlctyRtNnDrvn=L"0",DCDCStsWrng=L"0",TMActCtrlMdSts=L"0",VehSpdAvgDrvnRd=L"0",RemtCtrlSpdLmtRqstSt=L"0",VecActPowPer=L"0",BatChrgVolCpl=L"110",InitaContIndOn_IC=L"1",DFCUAcePedlPlbtyErr=L"0",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",VCUCtrlRqst1RollCnt=L"1",ACSwitch=L"0",TMSts_G=L"4",LftTrnLmpAtv=L"0",RealSoc=L"82",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"2.6",Direction=L"0",RemtCtrlPrkHet=L"0",CPSts=L"2",HzrdLgtSwAtv=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",TDAjrSwAtv=L"0",DCDCInpCur=L"0.3",BatTalVolSm=L"106.3",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",VBatChrgStsIndOn=L"1",BatCurSensFlt=L"0",AirbgIndOn=L"0",TMCtrlMdRqst=L"2",OBCInpMxCur=L"8",BatEnrgAvail=L"9.6",BatTempSensNum_G1=L"6",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",IgnKyPstn=L"0",BatDvcCur_G1=L"-13.5",TerminalNO=L"7683202652375880401M6215946",InvVol=L"106",InvJctTemp=L"26",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"7.38",MCUCurFlt=L"0",BatExtVol=L"106.3",DrSbltAtcV=L"0",TMNum_G=L"1",OBCOvTemp=L"0",SupCode=L"2",DFCTioutForVT=L"0",Bat2sPlsChrgPowAvail=L"7.38",vehConfig=L"LV1",VecActGearSts=L"3",VehConf=L"3",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"0",WhlGrndVlctyRtDrvn=L"0",OBCInpVlt=L"228",BatMaxTemp=L"21",EvpRlySta=L"0",IALPwrMdCtrlSt=L"0",TMActTemp=L"26",PlsAgainToRdy=L"0",HandBrkSts=L"1",IslatRes_G=L"10004",KyOpenSwAct=L"0",objType=L"E50",BatContiDischrgPowAvail=L"18",InitaContIndOn_MCU=L"1",VecIslateWrngIndOn=L"0",DFCMCUGen1TiOut=L"0",Bat2sPlsDischrgPowAvail=L"18",MCUCtrlFlt=L"0",BatChrgTims=L"200",VacPumpWrngIndOn=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",InitaContIndOn_BMS=L"1",TalClNum_G1=L"26",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",EPSWorkCur=L"0",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",DrSbltAtc=L"0",BatMiniCelVol=L"4087",TemperoHeatMeb3=L"205",TMDeratingSts=L"0",OBCORDCACIntnlTemp=L"61",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",BatMinTemp=L"20",TemperoHeatMeb6=L"205",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"33",PDUHVLock=L"1",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"1",vehType=L"E50",Longitude=L"108.21574",DFCDCANMUTE=L"0",WhlGrndVlctyLftDrvn=L"0",BrkProcInPrgrsIO=L"0",TMActWkSts=L"2",CDUState=L"2",objId=L"LK6ADAE18MB311730",PTID=L"2",ABSAtv=L"0",EngyRevIndOn=L"0",BrakPedalPos=L"0",TMTemp_G=L"26",BatCelVolSensFlt=L"0",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",BatIslateSts=L"0",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",AnThWaSt=L"1",DCDCOtpVol=L"14",RrFgLtIO=L"0",GPSspeed=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"12",BatMinTempCode_G=L"3",CCSts=L"2",IgnKyInstAtv=L"0",Latitude=L"22.642094",BMSHvPowOnRqst=L"2",VecSOCLoWrngIndOn=L"0",TMActDrvTorLmt=L"84.75",BatMaxClVltCode_G=L"18",CollisionSig=L"0",VecStatRdy=L"0",BatMaxCelVol=L"4093",LwBmIO=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",DCDCTempWrng=L"0",BCMRunMod=L"0",BatSubsyst_Vlt_G=L"1",BMSSts=L"2",DriverLeftWarning=L"0",BatRmaChrgTim=L"75",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"-13.5",FtFgLtIO=L"0",BatAvgCelVol=L"4089",Swit2Status=L"1",t1RTime=L"2023-03-07 14:41:49",VacPumpTransRate=L"1",BCMSftwrVsinNum=L"1",BatHetPrsrvtn=L"0",BatOthFltNum=L"0",VecFltIndOn=L"0",VecActTalCur=L"-13.5",ISBF=L"0",BatClVlt_G1=L"4.088_4.09_4.091_4.088_4.088_4.087_4.087_4.089_4.087_4.088_4.09_4.092_4.087_4.09_4.089_4.091_4.088_4.093_4.089_4.09_4.09_4.091_4.09_4.092_4.091_4.092",BatFltLvl=L"0",MCUInpVlt_G=L"106",BatMaxTempPos=L"1",TMActFbTorLmt=L"-85",DrvPowLimtIndOn=L"0",BatAvgCelVolSts=L"0",OBCOtpCur=L"14.1",VCUSftwrVsinNum=L"162",DDAjrSwAtv=L"0",TalVlt_G=L"106.3",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"7.38",InvVolSts=L"0",VehOdoV=L"0",VecOptMod=L"4",SoftEdt=L"180",RSDAjrSwAtv=L"0",TMorMCUOvTempInd=L"0",OptMod_G=L"1",MCUTemp_G=L"26",TMSpdRqst=L"0",VecChrgRqst=L"1",VecTMActSpdV=L"0",RLDoorOpenSwAct=L"0",MCUVolFlt=L"0",TMSpd_G=L"0",OBCInpCur=L"7",OBCInpVolUn=L"0",DCDCOtpCur=L"2.7",BMSChrgVolCurRqstAnorm=L"1",InitaContIndOn_ABS=L"1",CollectTime=L"2023-03-07 14:41:49",BatMinClVlt_G=L"4.087",RatedEgy=L"14",NoPulOutChrgPlugWrng=L"0",PsDoorOpenSwAct=L"0",AntiSlope=L"0",HVRelSta=L"0",MaxWrngLvl_G=L"0",BatheatindON=L"0",AccActPos=L"0",InitaContIndOn_BCM=L"1",TrnSwAct=L"0",VehSts_G=L"2",VehSpdAvgDrvn=L"0",InitaContIndOn_SDM=L"1",HhBmIO=L"0",t02RDelay=L"1",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Lngtd_G=L"108.21574",BatThrmlRunwyAlmInd=L"0",StpLpSt=L"0",PrkBrkSwAtv=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:49",BatMaxClVlt_G=L"4.093",AcInpFlt=L"0",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"9384",BatMinTemVolPakOrd=L"1",BatSOH=L"98",DrivStOccSt=L"0",BatSOC=L"86",VehOdo=L"34386",t2RIP=L"192.168.10.101",BatMaxTempCode_G=L"1",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"0",WindscenWipSt=L"0",LdspkSt=L"0",BatTempSensFlt=L"0",DFCMongHvNotIntd=L"0",DCDCInpVol=L"106.5",WipSwStat=L"1",BatIntVol=L"106.3",VCURemtCtrlSpdLmtFb=L"0",InvrCur=L"0",BatMinTempPos=L"3",VecChrgingSts=L"1",Bat10sPlsDischrgPowAvail=L"18",SftwrMjrVsin=L"6",FtPnLgtAtv=L"0",RatedVol=L"96",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",TMTorq_G=L"0",BatInfLen=L"0",BatChrgSts=L"1",InitaContIndOn_UCU=L"1",BCMRunModV=L"1",SOC_G=L"86",BatSubsysTemp_G1=L"21_21_20_21_21_21",PowBatErrIndOn=L"0",BatSOCSTS=L"0",DCDCInternalTemp=L"41",MaiLgtSw=L"0",MinTempSubSysNum_G=L"1",TMActTorq=L"0",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:49",VehDrvMod=L"1",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",TalNumOfClBatInThisFrm_G1=L"26",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DrDoorOpenSwAct=L"1",DFCDOPIN56Flt=L"0",RemtHeatModRqst=L"0" 1678171309000',
        #              'E50,VIN=LK6ADCE18MB417767 BMSCode=L"0",TMOrd_G=L"1",RRDoorOpenSwAct=L"0",RrImpctDet=L"0",BattCellVoltage_16=L"3.342",BattCellVoltage_17=L"3.342",BatFltNumN1_G=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"106.7",MCUCur_G=L"0",BatPowSts=L"1",TaPnLgtAtv=L"0",TrnsRvsSwSt=L"0",PDAjrSwAtv=L"0",TMActSpd=L"0",DFCShiftLvrPlbtyErr=L"0",OBCInpVolOv=L"0",RemainOdo=L"64",BatMaPosRlySts=L"1",CDUType=L"10",Lattd_G=L"30.671396",DrTorqRqst=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"1",DCDCIntnlFlt=L"0",GearSts_G=L"15",VecChrgStsIndOn=L"1",zdsjxh=L"2941",EvpEquPreVal=L"-59.6",InitaContIndOn_EPS=L"1",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"2",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"0",InvActTemp=L"23",BMSGenSts1RollCnt=L"1",SignalStrengthOne=L"4",StrWhAng=L"5.4375",SignalStrengthTwo=L"31",TDEnblSts=L"0",createTime=L"1678171310045",ACRelayCmd=L"0",BatDvcVlt_G1=L"106.8",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",dqsjxh=L"9",MCUSupCode=L"8340040",HzrdLtIO=L"0",VecActPow=L"0",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",OBCOtpVlt=L"107.3",BatMaxCelVolPos=L"15",t2RTime=L"2023-03-07 14:41:50",InitaContIndOn_OBC=L"0",TalCur_G=L"-13.6",BatAvgTemp=L"25",OBCInpCurOv=L"0",ChargPrtOpInOnPrmt=L"1",ChrgCtrlSnglFail=L"0",KyPstn=L"0",MCUActFltNum=L"0",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"0",BatMinTemp_G=L"24",ModeCode=L"138",BMSChrgCurRqst=L"40",RtTrnLmpAtv=L"0",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",DFCGearReqMongErr=L"0",DFCCASmax=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",BatMinClVltCode_G=L"11",LSDAjrSwAtv=L"0",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",TMWkStsRqst=L"0",OBCOtpVolOv=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"117.8",TMTorqRqst=L"0",ShifGearFailAtHiSpeed=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"3",VehOdo_G=L"2189",BatTempSts=L"0",LowBatVol=L"14.1",VCUBatThrmlRunwyAlmInd=L"0",VecTMActSpd=L"0",DCDCOtpVolSts=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"26",EvrmetAblPreVal=L"95.3",BatIslatRes=L"10047",BatMiniCelVolPos=L"11",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",VecSOC=L"54",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"15",WhlGrndVlctyRtNnDrvn=L"0",DCDCStsWrng=L"0",TMActCtrlMdSts=L"0",VehSpdAvgDrvnRd=L"0",RemtCtrlSpdLmtRqstSt=L"0",VecActPowPer=L"0",BatChrgVolCpl=L"117.8",InitaContIndOn_IC=L"1",DFCUAcePedlPlbtyErr=L"0",PTCSwitch=L"2",MaxVltBatSubSysNum_G=L"1",VCUCtrlRqst1RollCnt=L"11",ACSwitch=L"0",LftTrnLmpAtv=L"0",RealSoc=L"55",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"4",Direction=L"0",RemtCtrlPrkHet=L"0",CPSts=L"2",HzrdLgtSwAtv=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",TDAjrSwAtv=L"0",DCDCInpCur=L"0.3",BatTalVolSm=L"106.8",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",VBatChrgStsIndOn=L"1",BatCurSensFlt=L"0",AirbgIndOn=L"0",TMCtrlMdRqst=L"2",OBCInpMxCur=L"8",BatEnrgAvail=L"4.8",BatTempSensNum_G1=L"4",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",IgnKyPstn=L"0",BatDvcCur_G1=L"-13.6",TerminalNO=L"7683202652375880403MA184263",InvVol=L"106",InvJctTemp=L"23",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"10.8",MCUCurFlt=L"0",BatExtVol=L"107.2",DrSbltAtcV=L"0",TMNum_G=L"1",OBCOvTemp=L"0",SupCode=L"2",DFCTioutForVT=L"0",Bat2sPlsChrgPowAvail=L"10.8",vehConfig=L"LV1",VecActGearSts=L"3",VehConf=L"3",BalanceSts=L"1",WhlGrndVlctyLftNnDrvn=L"0",WhlGrndVlctyRtDrvn=L"0",OBCInpVlt=L"228",BatMaxTemp=L"26",EvpRlySta=L"0",IALPwrMdCtrlSt=L"0",TMActTemp=L"23",PlsAgainToRdy=L"0",HandBrkSts=L"1",IslatRes_G=L"10047",KyOpenSwAct=L"0",objType=L"E50",BatContiDischrgPowAvail=L"16",InitaContIndOn_MCU=L"1",VecIslateWrngIndOn=L"0",DFCMCUGen1TiOut=L"0",Bat2sPlsDischrgPowAvail=L"16",MCUCtrlFlt=L"0",BatChrgTims=L"17",VacPumpWrngIndOn=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",InitaContIndOn_BMS=L"0",TalClNum_G1=L"32",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",EPSWorkCur=L"2.75",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",DrSbltAtc=L"0",BatMiniCelVol=L"3338",TemperoHeatMeb3=L"205",TMDeratingSts=L"0",OBCORDCACIntnlTemp=L"66",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",BatMinTemp=L"24",TemperoHeatMeb6=L"205",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"6",PDUHVLock=L"1",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"1",vehType=L"E50",Longitude=L"103.807134",DFCDCANMUTE=L"0",WhlGrndVlctyLftDrvn=L"0",BrkProcInPrgrsIO=L"0",TMActWkSts=L"2",CDUState=L"2",objId=L"LK6ADCE18MB417767",PTID=L"2",ABSAtv=L"0",EngyRevIndOn=L"0",BrakPedalPos=L"0",TMTemp_G=L"23",BatCelVolSensFlt=L"0",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",BatIslateSts=L"0",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",AnThWaSt=L"5",DCDCOtpVol=L"14",RrFgLtIO=L"0",GPSspeed=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"8",BatMinTempCode_G=L"2",CCSts=L"2",IgnKyInstAtv=L"0",Latitude=L"30.671396",BMSHvPowOnRqst=L"2",VecSOCLoWrngIndOn=L"0",TMActDrvTorLmt=L"84.75",BatMaxClVltCode_G=L"15",CollisionSig=L"0",VecStatRdy=L"0",BatMaxCelVol=L"3342",LwBmIO=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",DCDCTempWrng=L"0",BCMRunMod=L"0",BatSubsyst_Vlt_G=L"1",BMSSts=L"2",DriverLeftWarning=L"0",BatRmaChrgTim=L"128",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"-13.6",FtFgLtIO=L"0",BatAvgCelVol=L"3340",Swit2Status=L"1",t1RTime=L"2023-03-07 14:41:49",VacPumpTransRate=L"1",BCMSftwrVsinNum=L"1",BatHetPrsrvtn=L"0",BatOthFltNum=L"0",VecFltIndOn=L"0",VecActTalCur=L"-13.6",ISBF=L"0",BatClVlt_G1=L"3.341_3.341_3.341_3.339_3.34_3.34_3.34_3.34_3.339_3.34_3.338_3.34_3.341_3.34_3.342_3.342_3.342_3.34_3.34_3.34_3.341_3.339_3.34_3.341_3.341_3.341_3.341_3.34_3.34_3.341_3.342_3.341",BatFltLvl=L"0",MCUInpVlt_G=L"106",BatMaxTempPos=L"1",TMActFbTorLmt=L"-85",DrvPowLimtIndOn=L"0",BatAvgCelVolSts=L"0",OBCOtpCur=L"14",VCUSftwrVsinNum=L"162",DDAjrSwAtv=L"0",TalVlt_G=L"106.8",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"10.8",InvVolSts=L"0",VehOdoV=L"0",VecOptMod=L"4",SoftEdt=L"150",RSDAjrSwAtv=L"0",TMorMCUOvTempInd=L"0",OptMod_G=L"1",MCUTemp_G=L"23",TMSpdRqst=L"0",VecChrgRqst=L"1",VecTMActSpdV=L"0",RLDoorOpenSwAct=L"0",MCUVolFlt=L"0",TMSpd_G=L"0",OBCInpCur=L"7",OBCInpVolUn=L"0",DCDCOtpCur=L"1.8",BMSChrgVolCurRqstAnorm=L"1",InitaContIndOn_ABS=L"1",CollectTime=L"2023-03-07 14:41:49",BatMinClVlt_G=L"3.338",RatedEgy=L"9.4",NoPulOutChrgPlugWrng=L"0",PsDoorOpenSwAct=L"0",AntiSlope=L"0",HVRelSta=L"0",MaxWrngLvl_G=L"0",BatheatindON=L"0",AccActPos=L"0",InitaContIndOn_BCM=L"1",TrnSwAct=L"0",VehSts_G=L"2",VehSpdAvgDrvn=L"0",InitaContIndOn_SDM=L"1",HhBmIO=L"0",t02RDelay=L"1",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"0",Lngtd_G=L"103.807134",BatThrmlRunwyAlmInd=L"0",StpLpSt=L"0",PrkBrkSwAtv=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:49",BatMaxClVlt_G=L"3.342",AcInpFlt=L"0",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"2672",BatMinTemVolPakOrd=L"1",BatSOH=L"100",DrivStOccSt=L"1",BatSOC=L"54",VehOdo=L"2189",t2RIP=L"192.168.40.104",BatMaxTempCode_G=L"1",TmnlWkupSurc=L"3",OBCHdErr=L"0",TMActPow=L"0",WindscenWipSt=L"0",LdspkSt=L"0",BatTempSensFlt=L"0",DFCMongHvNotIntd=L"0",DCDCInpVol=L"107.3",WipSwStat=L"1",BatIntVol=L"106.8",VCURemtCtrlSpdLmtFb=L"0",InvrCur=L"0",BatMinTempPos=L"2",VecChrgingSts=L"1",Bat10sPlsDischrgPowAvail=L"16",SftwrMjrVsin=L"6",FtPnLgtAtv=L"0",RatedVol=L"102",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",TMTorq_G=L"0",BatInfLen=L"0",BatChrgSts=L"1",InitaContIndOn_UCU=L"1",BCMRunModV=L"1",SOC_G=L"54",BatSubsysTemp_G1=L"26_24_26_24",PowBatErrIndOn=L"0",BatSOCSTS=L"0",DCDCInternalTemp=L"45",MaiLgtSw=L"0",MinTempSubSysNum_G=L"1",TMActTorq=L"0",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:49",VehDrvMod=L"1",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",TalNumOfClBatInThisFrm_G1=L"32",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DrDoorOpenSwAct=L"0",DFCDOPIN56Flt=L"0",RemtHeatModRqst=L"0" 1678171309000',
        #              'E50,VIN=LK6ADAE17NE403427 BMSCode=L"0",TMOrd_G=L"1",RRDoorOpenSwAct=L"0",RrImpctDet=L"0",BattCellVoltage_16=L"3.351",BattCellVoltage_17=L"3.339",BatFltNumN1_G=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"107.1",MCUCur_G=L"0",BatPowSts=L"1",TaPnLgtAtv=L"0",TrnsRvsSwSt=L"0",PDAjrSwAtv=L"0",TMActSpd=L"0",DFCShiftLvrPlbtyErr=L"0",OBCInpVolOv=L"0",RemainOdo=L"107",BatMaPosRlySts=L"1",CDUType=L"9",Lattd_G=L"23.641251",DrTorqRqst=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"1",DCDCIntnlFlt=L"0",GearSts_G=L"15",VecChrgStsIndOn=L"1",zdsjxh=L"8665",EvpEquPreVal=L"-54.7",InitaContIndOn_EPS=L"1",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"2",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"6",InvActTemp=L"25",BMSGenSts1RollCnt=L"8",SignalStrengthOne=L"4",StrWhAng=L"4092.125",SignalStrengthTwo=L"28",TDEnblSts=L"0",createTime=L"1678171310072",ACRelayCmd=L"0",BatDvcVlt_G1=L"107",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",dqsjxh=L"10",MCUSupCode=L"8340040",HzrdLtIO=L"0",VecActPow=L"0",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",OBCOtpVlt=L"107.8",BatMaxCelVolPos=L"16",t2RTime=L"2023-03-07 14:41:50",InitaContIndOn_OBC=L"0",TalCur_G=L"-13.4",BatAvgTemp=L"26",OBCInpCurOv=L"0",ChargPrtOpInOnPrmt=L"1",ChrgCtrlSnglFail=L"0",KyPstn=L"0",MCUActFltNum=L"0",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"0",BatMinTemp_G=L"26",ModeCode=L"66",BMSChrgCurRqst=L"60",RtTrnLmpAtv=L"0",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",DFCGearReqMongErr=L"0",DFCCASmax=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",BatMinClVltCode_G=L"17",LSDAjrSwAtv=L"0",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",TMWkStsRqst=L"0",OBCOtpVolOv=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"116.8",TMTorqRqst=L"0",ShifGearFailAtHiSpeed=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"3",VehOdo_G=L"12295",BatTempSts=L"0",LowBatVol=L"14.15",VCUBatThrmlRunwyAlmInd=L"0",VecTMActSpd=L"0",DCDCOtpVolSts=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"28",EvrmetAblPreVal=L"101.7",BatIslatRes=L"5000",BatMiniCelVolPos=L"17",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",VecSOC=L"63",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"122",WhlGrndVlctyRtNnDrvn=L"0",DCDCStsWrng=L"0",TMActCtrlMdSts=L"0",VehSpdAvgDrvnRd=L"0",RemtCtrlSpdLmtRqstSt=L"0",VecActPowPer=L"0",BatChrgVolCpl=L"116.8",InitaContIndOn_IC=L"1",DFCUAcePedlPlbtyErr=L"0",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",VCUCtrlRqst1RollCnt=L"1",ACSwitch=L"0",LftTrnLmpAtv=L"0",RealSoc=L"64",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"12.9",Direction=L"0",RemtCtrlPrkHet=L"0",CPSts=L"2",HzrdLgtSwAtv=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",TDAjrSwAtv=L"0",DCDCInpCur=L"0.2",BatTalVolSm=L"107",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",VBatChrgStsIndOn=L"1",BatCurSensFlt=L"0",AirbgIndOn=L"0",TMCtrlMdRqst=L"2",OBCInpMxCur=L"7",BatEnrgAvail=L"8.7",BatTempSensNum_G1=L"16",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",IgnKyPstn=L"0",BatDvcCur_G1=L"-13.4",TerminalNO=L"7683202652375880403MC185252",InvVol=L"107",InvJctTemp=L"25",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"10.28",MCUCurFlt=L"0",BatExtVol=L"106.7",DrSbltAtcV=L"0",TMNum_G=L"1",OBCOvTemp=L"0",SupCode=L"0",DFCTioutForVT=L"0",Bat2sPlsChrgPowAvail=L"10.28",vehConfig=L"LV1",VecActGearSts=L"3",VehConf=L"3",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"0",WhlGrndVlctyRtDrvn=L"0",OBCInpVlt=L"225",BatMaxTemp=L"28",EvpRlySta=L"0",IALPwrMdCtrlSt=L"0",TMActTemp=L"23",PlsAgainToRdy=L"0",HandBrkSts=L"1",IslatRes_G=L"5000",KyOpenSwAct=L"0",objType=L"E50",BatContiDischrgPowAvail=L"16",InitaContIndOn_MCU=L"1",VecIslateWrngIndOn=L"0",DFCMCUGen1TiOut=L"0",Bat2sPlsDischrgPowAvail=L"16",MCUCtrlFlt=L"0",BatChrgTims=L"86",VacPumpWrngIndOn=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",InitaContIndOn_BMS=L"0",TalClNum_G1=L"32",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",EPSWorkCur=L"1.5",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",DrSbltAtc=L"0",BatMiniCelVol=L"3338",TemperoHeatMeb3=L"26",TMDeratingSts=L"0",OBCORDCACIntnlTemp=L"49",TemperoHeatMeb2=L"26",TemperoHeatMeb1=L"27",TemperoHeatMeb7=L"205",BatMinTemp=L"26",TemperoHeatMeb6=L"205",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"27",PDUHVLock=L"1",TemperoHeatMeb8=L"64",InitaContIndOn_VCU=L"1",vehType=L"E50",Longitude=L"116.616644",DFCDCANMUTE=L"0",WhlGrndVlctyLftDrvn=L"0",BrkProcInPrgrsIO=L"0",TMActWkSts=L"2",CDUState=L"2",objId=L"LK6ADAE17NE403427",PTID=L"2",ABSAtv=L"0",EngyRevIndOn=L"0",BrakPedalPos=L"0",TMTemp_G=L"23",BatCelVolSensFlt=L"0",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",BatIslateSts=L"0",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",AnThWaSt=L"1",DCDCOtpVol=L"14",RrFgLtIO=L"0",GPSspeed=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"15",BatMinTempCode_G=L"4",CCSts=L"2",IgnKyInstAtv=L"0",Latitude=L"23.641251",BMSHvPowOnRqst=L"2",VecSOCLoWrngIndOn=L"0",TMActDrvTorLmt=L"84.75",BatMaxClVltCode_G=L"16",CollisionSig=L"0",VecStatRdy=L"0",BatMaxCelVol=L"3351",LwBmIO=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",DCDCTempWrng=L"0",BCMRunMod=L"0",BatSubsyst_Vlt_G=L"1",BMSSts=L"2",DriverLeftWarning=L"0",BatRmaChrgTim=L"233",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"-13.4",FtFgLtIO=L"0",BatAvgCelVol=L"3344",Swit2Status=L"1",t1RTime=L"2023-03-07 14:41:49",VacPumpTransRate=L"1",BCMSftwrVsinNum=L"1",BatHetPrsrvtn=L"0",BatOthFltNum=L"0",VecFltIndOn=L"0",VecActTalCur=L"-13.4",ISBF=L"0",BatClVlt_G1=L"3.344_3.343_3.345_3.341_3.341_3.344_3.342_3.34_3.343_3.339_3.34_3.344_3.342_3.346_3.342_3.351_3.339_3.342_3.342_3.341_3.344_3.341_3.342_3.343_3.349_3.348_3.342_3.348_3.347_3.344_3.347_3.348",BatFltLvl=L"0",MCUInpVlt_G=L"107",BatMaxTempPos=L"9",TMActFbTorLmt=L"-85",DrvPowLimtIndOn=L"0",BatAvgCelVolSts=L"0",OBCOtpCur=L"13.8",VCUSftwrVsinNum=L"162",DDAjrSwAtv=L"0",TalVlt_G=L"107",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"10.28",InvVolSts=L"0",VehOdoV=L"0",VecOptMod=L"4",SoftEdt=L"190",RSDAjrSwAtv=L"0",TMorMCUOvTempInd=L"0",OptMod_G=L"1",MCUTemp_G=L"25",TMSpdRqst=L"0",VecChrgRqst=L"1",VecTMActSpdV=L"0",RLDoorOpenSwAct=L"0",MCUVolFlt=L"0",TMSpd_G=L"0",OBCInpCur=L"7",OBCInpVolUn=L"0",DCDCOtpCur=L"2.1",BMSChrgVolCurRqstAnorm=L"0",InitaContIndOn_ABS=L"1",CollectTime=L"2023-03-07 14:41:49",BatMinClVlt_G=L"3.338",RatedEgy=L"13.8",NoPulOutChrgPlugWrng=L"0",PsDoorOpenSwAct=L"0",AntiSlope=L"0",HVRelSta=L"0",MaxWrngLvl_G=L"0",BatheatindON=L"0",AccActPos=L"0",InitaContIndOn_BCM=L"1",TrnSwAct=L"0",VehSts_G=L"2",VehSpdAvgDrvn=L"0",InitaContIndOn_SDM=L"1",HhBmIO=L"0",t02RDelay=L"1",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Lngtd_G=L"116.616644",BatThrmlRunwyAlmInd=L"0",StpLpSt=L"0",PrkBrkSwAtv=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:49",BatMaxClVlt_G=L"3.351",AcInpFlt=L"0",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"3979",BatMinTemVolPakOrd=L"1",BatSOH=L"97",DrivStOccSt=L"1",BatSOC=L"63",VehOdo=L"12295",t2RIP=L"192.168.10.102",BatMaxTempCode_G=L"9",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"0",WindscenWipSt=L"0",LdspkSt=L"0",BatTempSensFlt=L"0",DFCMongHvNotIntd=L"0",DCDCInpVol=L"107.8",WipSwStat=L"1",BatIntVol=L"107.1",VCURemtCtrlSpdLmtFb=L"0",InvrCur=L"0",BatMinTempPos=L"4",VecChrgingSts=L"1",Bat10sPlsDischrgPowAvail=L"16",SftwrMjrVsin=L"6",FtPnLgtAtv=L"0",RatedVol=L"103",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",TMTorq_G=L"0",BatInfLen=L"0",BatChrgSts=L"1",InitaContIndOn_UCU=L"1",BCMRunModV=L"1",SOC_G=L"63",BatSubsysTemp_G1=L"27_27_27_26_26_27_27_27_28_27_27_26_27_27_27_28",PowBatErrIndOn=L"0",BatSOCSTS=L"0",DCDCInternalTemp=L"41",MaiLgtSw=L"0",MinTempSubSysNum_G=L"1",TMActTorq=L"0",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:49",VehDrvMod=L"1",BatManuCode=L"3158579",MESSAGETYPE=L"REALTIME",TalNumOfClBatInThisFrm_G1=L"32",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DrDoorOpenSwAct=L"1",DFCDOPIN56Flt=L"0",RemtHeatModRqst=L"0" 1678171309000',
        #              'E50,VIN=LK6ADAE10NB197143 BMSCode=L"0",TMOrd_G=L"1",RRDoorOpenSwAct=L"0",RrImpctDet=L"0",BattCellVoltage_16=L"3.273",BattCellVoltage_17=L"3.272",BatFltNumN1_G=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"103.9",MCUCur_G=L"39",BatPowSts=L"1",TaPnLgtAtv=L"0",TrnsRvsSwSt=L"0",PDAjrSwAtv=L"0",TMActSpd=L"4409",DFCShiftLvrPlbtyErr=L"0",OBCInpVolOv=L"0",RemainOdo=L"119",BatMaPosRlySts=L"1",CDUType=L"10",Lattd_G=L"36.652725",DrTorqRqst=L"8",VehiReadToDriWarning=L"0",ChrgSts_G=L"3",DCDCIntnlFlt=L"0",GearSts_G=L"14",VecChrgStsIndOn=L"0",zdsjxh=L"592",EvpEquPreVal=L"-64.9",InitaContIndOn_EPS=L"0",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"2",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"10",InvActTemp=L"39",BMSGenSts1RollCnt=L"4",SignalStrengthOne=L"4",StrWhAng=L"1.875",SignalStrengthTwo=L"31",TDEnblSts=L"0",createTime=L"1678171310078",ACRelayCmd=L"0",BatDvcVlt_G1=L"104",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",dqsjxh=L"12",MCUSupCode=L"8340040",HzrdLtIO=L"0",VecActPow=L"3.6",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",OBCOtpVlt=L"103.4",BatMaxCelVolPos=L"2",t2RTime=L"2023-03-07 14:41:50",InitaContIndOn_OBC=L"0",TalCur_G=L"42.5",BatAvgTemp=L"27",OBCInpCurOv=L"0",ChargPrtOpInOnPrmt=L"0",ChrgCtrlSnglFail=L"0",KyPstn=L"2",MCUActFltNum=L"0",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"58.6",BatMinTemp_G=L"27",ModeCode=L"133",BMSChrgCurRqst=L"0",RtTrnLmpAtv=L"0",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",DFCGearReqMongErr=L"0",DFCCASmax=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",BatMinClVltCode_G=L"15",LSDAjrSwAtv=L"0",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",TMWkStsRqst=L"2",OBCOtpVolOv=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"104",TMTorqRqst=L"8",ShifGearFailAtHiSpeed=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"3",VehOdo_G=L"13792",BatTempSts=L"0",LowBatVol=L"13.84",VCUBatThrmlRunwyAlmInd=L"0",VecTMActSpd=L"4387",DCDCOtpVolSts=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"27",EvrmetAblPreVal=L"99.6",BatIslatRes=L"10031",BatMiniCelVolPos=L"15",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",VecSOC=L"70",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"211",WhlGrndVlctyRtNnDrvn=L"58.97",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"59",RemtCtrlSpdLmtRqstSt=L"1",VecActPowPer=L"17",BatChrgVolCpl=L"117.8",InitaContIndOn_IC=L"0",DFCUAcePedlPlbtyErr=L"0",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",VCUCtrlRqst1RollCnt=L"9",ACSwitch=L"1",TMSts_G=L"1",LftTrnLmpAtv=L"0",RealSoc=L"70",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"8.1",Direction=L"0",RemtCtrlPrkHet=L"0",CPSts=L"0",HzrdLgtSwAtv=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",TDAjrSwAtv=L"0",DCDCInpCur=L"2.3",BatTalVolSm=L"104",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",VBatChrgStsIndOn=L"1",BatCurSensFlt=L"0",AirbgIndOn=L"0",TMCtrlMdRqst=L"2",OBCInpMxCur=L"0",BatEnrgAvail=L"8",BatTempSensNum_G1=L"4",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",IgnKyPstn=L"2",BatDvcCur_G1=L"42.5",TerminalNO=L"7683202652375880403N2127827",InvVol=L"104",InvJctTemp=L"39",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"9",MCUCurFlt=L"0",BatExtVol=L"104",DrSbltAtcV=L"0",TMNum_G=L"1",OBCOvTemp=L"0",SupCode=L"2",DFCTioutForVT=L"0",Bat2sPlsChrgPowAvail=L"9",vehConfig=L"LV1",VecActGearSts=L"1",VehConf=L"3",BalanceSts=L"1",WhlGrndVlctyLftNnDrvn=L"59.09",WhlGrndVlctyRtDrvn=L"58.97",OBCInpVlt=L"0",BatMaxTemp=L"27",EvpRlySta=L"0",IALPwrMdCtrlSt=L"0",TMActTemp=L"51",PlsAgainToRdy=L"0",HandBrkSts=L"0",IslatRes_G=L"10031",KyOpenSwAct=L"0",objType=L"E50",BatContiDischrgPowAvail=L"16.06",InitaContIndOn_MCU=L"0",VecIslateWrngIndOn=L"0",DFCMCUGen1TiOut=L"0",Bat2sPlsDischrgPowAvail=L"16.06",MCUCtrlFlt=L"0",BatChrgTims=L"100",VacPumpWrngIndOn=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",InitaContIndOn_BMS=L"0",TalClNum_G1=L"32",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",EPSWorkCur=L"0",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"1",ProtocolVersion=L"1_1_1",DrSbltAtc=L"1",BatMiniCelVol=L"3243",TemperoHeatMeb3=L"205",TMDeratingSts=L"0",OBCORDCACIntnlTemp=L"34",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",BatMinTemp=L"27",TemperoHeatMeb6=L"205",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"20",PDUHVLock=L"1",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"0",vehType=L"E50",Longitude=L"117.080205",DFCDCANMUTE=L"0",WhlGrndVlctyLftDrvn=L"59.09",BrkProcInPrgrsIO=L"0",TMActWkSts=L"3",CDUState=L"5",objId=L"LK6ADAE10NB197143",PTID=L"2",ABSAtv=L"0",EngyRevIndOn=L"0",BrakPedalPos=L"0",TMTemp_G=L"51",BatCelVolSensFlt=L"0",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",BatIslateSts=L"0",AclrtrPedaStrk_G=L"34",TMOvSpdInd=L"0",AnThWaSt=L"1",DCDCOtpVol=L"13.8",RrFgLtIO=L"0",GPSspeed=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"13",BatMinTempCode_G=L"1",CCSts=L"0",IgnKyInstAtv=L"0",Latitude=L"36.652725",BMSHvPowOnRqst=L"2",VecSOCLoWrngIndOn=L"0",TMActDrvTorLmt=L"43.25",BatMaxClVltCode_G=L"2",CollisionSig=L"0",VecStatRdy=L"1",BatMaxCelVol=L"3257",LwBmIO=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",DCDCTempWrng=L"0",BCMRunMod=L"0",BatSubsyst_Vlt_G=L"1",BMSSts=L"3",DriverLeftWarning=L"0",BatRmaChrgTim=L"0",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"42.5",FtFgLtIO=L"0",BatAvgCelVol=L"3252",Swit2Status=L"0",t1RTime=L"2023-03-07 14:41:50",VacPumpTransRate=L"1",BCMSftwrVsinNum=L"2",BatHetPrsrvtn=L"0",BatOthFltNum=L"0",VecFltIndOn=L"0",VecActTalCur=L"42.4",ISBF=L"0",BatClVlt_G1=L"3.269_3.273_3.272_3.273_3.271_3.272_3.271_3.272_3.271_3.272_3.27_3.272_3.272_3.272_3.266_3.273_3.272_3.272_3.272_3.272_3.272_3.271_3.271_3.272_3.272_3.271_3.272_3.272_3.273_3.273_3.271_3.271",BatFltLvl=L"0",MCUInpVlt_G=L"104",BatMaxTempPos=L"1",TMActFbTorLmt=L"-43.5",DrvPowLimtIndOn=L"0",BatAvgCelVolSts=L"0",OBCOtpCur=L"0",VCUSftwrVsinNum=L"162",DDAjrSwAtv=L"0",TalVlt_G=L"104",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"9",InvVolSts=L"0",VehOdoV=L"0",VecOptMod=L"3",SoftEdt=L"151",RSDAjrSwAtv=L"0",TMorMCUOvTempInd=L"0",OptMod_G=L"1",MCUTemp_G=L"39",TMSpdRqst=L"0",VecChrgRqst=L"0",VecTMActSpdV=L"0",RLDoorOpenSwAct=L"0",MCUVolFlt=L"0",TMSpd_G=L"4409",OBCInpCur=L"0",OBCInpVolUn=L"0",DCDCOtpCur=L"17.6",BMSChrgVolCurRqstAnorm=L"0",InitaContIndOn_ABS=L"0",CollectTime=L"2023-03-07 14:41:49",BatMinClVlt_G=L"3.243",RatedEgy=L"14.1",NoPulOutChrgPlugWrng=L"0",PsDoorOpenSwAct=L"0",AntiSlope=L"0",HVRelSta=L"2",MaxWrngLvl_G=L"0",BatheatindON=L"0",AccActPos=L"34",InitaContIndOn_BCM=L"0",TrnSwAct=L"0",VehSts_G=L"1",VehSpdAvgDrvn=L"58.671875",InitaContIndOn_SDM=L"0",HhBmIO=L"0",t02RDelay=L"1",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Lngtd_G=L"117.080205",BatThrmlRunwyAlmInd=L"0",StpLpSt=L"0",PrkBrkSwAtv=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:49",BatMaxClVlt_G=L"3.257",AcInpFlt=L"0",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"3189",BatMinTemVolPakOrd=L"1",BatSOH=L"100",DrivStOccSt=L"1",BatSOC=L"70",VehOdo=L"13792",t2RIP=L"192.168.20.103",BatMaxTempCode_G=L"1",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"3.6",WindscenWipSt=L"0",LdspkSt=L"0",BatTempSensFlt=L"0",DFCMongHvNotIntd=L"0",DCDCInpVol=L"103.4",WipSwStat=L"1",BatIntVol=L"103.8",VCURemtCtrlSpdLmtFb=L"0",InvrCur=L"39",BatMinTempPos=L"1",VecChrgingSts=L"0",Bat10sPlsDischrgPowAvail=L"16.06",SftwrMjrVsin=L"6",FtPnLgtAtv=L"0",RatedVol=L"102",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",TMTorq_G=L"8",BatInfLen=L"0",BatChrgSts=L"0",InitaContIndOn_UCU=L"0",BCMRunModV=L"1",SOC_G=L"70",BatSubsysTemp_G1=L"27_27_27_27",PowBatErrIndOn=L"0",BatSOCSTS=L"0",DCDCInternalTemp=L"38",MaiLgtSw=L"0",MinTempSubSysNum_G=L"1",TMActTorq=L"8",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:49",VehDrvMod=L"1",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",TalNumOfClBatInThisFrm_G1=L"32",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DrDoorOpenSwAct=L"0",DFCDOPIN56Flt=L"0",RemtHeatModRqst=L"0" 1678171309000',
        #              'E50,VIN=LK6ADAE12MB220226 TalClNum_G1=L"32",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",BMSCode=L"2",EPSWorkCur=L"0",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",TMOrd_G=L"1",DrSbltAtc=L"1",RrImpctDet=L"0",BatMiniCelVol=L"3257",OBCORDCACIntnlTemp=L"27",TMDeratingSts=L"0",TemperoHeatMeb3=L"25",BattCellVoltage_16=L"3.255",TemperoHeatMeb2=L"25",TemperoHeatMeb1=L"25",TemperoHeatMeb7=L"205",TemperoHeatMeb6=L"205",BatMinTemp=L"25",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"25",PDUHVLock=L"1",BattCellVoltage_17=L"3.248",BatFltNumN1_G=L"0",TemperoHeatMeb8=L"70",InitaContIndOn_VCU=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"104.8",MCUCur_G=L"2",vehType=L"E50",Longitude=L"111.608393",WhlGrndVlctyLftDrvn=L"57.6",BatPowSts=L"1",DFCDCANMUTE=L"0",TMActWkSts=L"3",BrkProcInPrgrsIO=L"0",CDUState=L"5",TMActSpd=L"4310",objId=L"LK6ADAE12MB220226",PTID=L"2",DFCShiftLvrPlbtyErr=L"0",ABSAtv=L"0",OBCInpVolOv=L"0",EngyRevIndOn=L"0",RemainOdo=L"110",BrakPedalPos=L"0",BatMaPosRlySts=L"1",TMTemp_G=L"69",BatCelVolSensFlt=L"0",CDUType=L"9",Lattd_G=L"30.605939",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",DrTorqRqst=L"0",BatIslateSts=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"3",DCDCIntnlFlt=L"0",GearSts_G=L"14",VecChrgStsIndOn=L"0",zdsjxh=L"389",AclrtrPedaStrk_G=L"24",TMOvSpdInd=L"0",EvpEquPreVal=L"-57.6",InitaContIndOn_EPS=L"0",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"1",DCDCOtpVol=L"14",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"14",InvActTemp=L"40",BMSGenSts1RollCnt=L"5",GPSspeed=L"0",SignalStrengthOne=L"4",StrWhAng=L"0",SignalStrengthTwo=L"20",RemtCtrParkHeat=L"0",createTime=L"1678171310051",ACRelayCmd=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"7",BatMinTempCode_G=L"1",CCSts=L"0",BatDvcVlt_G1=L"104.4",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",MCUSupCode=L"8340040",dqsjxh=L"2264",Latitude=L"30.605939",VecActPow=L"0",BMSHvPowOnRqst=L"2",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",VecSOCLoWrngIndOn=L"0",OBCOtpVlt=L"104.3",BatMaxCelVolPos=L"32",t2RTime=L"2023-03-07 14:41:50",TMActDrvTorLmt=L"44",InitaContIndOn_OBC=L"0",TalCur_G=L"2.3",BatAvgTemp=L"25",OBCInpCurOv=L"0",BatMaxClVltCode_G=L"32",CollisionSig=L"0",VecStatRdy=L"1",BatMaxCelVol=L"3268",ChrgCtrlSnglFail=L"0",KyPstn=L"2",BatPrechrgRlySts=L"0",BatFuSts=L"0",MCUActFltNum=L"0",DCDCTempWrng=L"0",BatSubsyst_Vlt_G=L"1",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"58",BMSSts=L"3",DriverLeftWarning=L"0",BatMinTemp_G=L"25",BatRmaChrgTim=L"0",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",ModeCode=L"66",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"2.3",BMSChrgCurRqst=L"0",Swit2Status=L"0",BatAvgCelVol=L"3263",t1RTime=L"2023-03-07 14:41:49",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",VacPumpTransRate=L"1",DFCGearReqMongErr=L"0",BatHetPrsrvtn=L"0",DFCCASmax=L"0",BatOthFltNum=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",VecFltIndOn=L"0",BatMinClVltCode_G=L"4",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",VecActTalCur=L"2.4",ISBF=L"0",BatClVlt_G1=L"3.261_3.259_3.259_3.258_3.263_3.262_3.263_3.263_3.246_3.254_3.247_3.249_3.247_3.252_3.25_3.255_3.248_3.247_3.246_3.247_3.246_3.249_3.245_3.248_3.248_3.248_3.25_3.249_3.246_3.251_3.246_3.256",BatFltLvl=L"0",MCUInpVlt_G=L"104",TMWkStsRqst=L"2",BatMaxTempPos=L"7",TMActFbTorLmt=L"-44.25",DrvPowLimtIndOn=L"0",OBCOtpVolOv=L"0",BatAvgCelVolSts=L"0",VCUSftwrVsinNum=L"228",OBCOtpCur=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"0",TalVlt_G=L"104.4",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"9.04",TMTorqRqst=L"0",InvVolSts=L"0",VehOdoV=L"0",ShifGearFailAtHiSpeed=L"0",VecOptMod=L"3",SoftEdt=L"150",TMorMCUOvTempInd=L"0",OptMod_G=L"1",RemtCtrlSpdLimtRqstSt=L"1",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"3",VehOdo_G=L"18751",MCUTemp_G=L"40",TMSpdRqst=L"0",LowBatVol=L"14.08",BatTempSts=L"0",VecChrgRqst=L"0",VecTMActSpdV=L"0",VecTMActSpd=L"4339",VCUBatThrmlRunwyAlmInd=L"0",MCUVolFlt=L"0",DCDCOtpVolSts=L"0",TMSpd_G=L"4310",BatChrgCurSts=L"0",BatMaxTemp_G=L"26",EvrmetAblPreVal=L"100",OBCInpCur=L"0",BatIslatRes=L"5000",BatMiniCelVolPos=L"4",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",OBCInpVolUn=L"0",VecSOC=L"68",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCOtpCur=L"3.2",InitaContIndOn_ABS=L"0",BMSChrgVolCurRqstAnorm=L"0",CollectTime=L"2023-03-07 14:41:49",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"179",BatMinClVlt_G=L"3.257",RatedEgy=L"13.8",WhlGrndVlctyRtNnDrvn=L"57.88",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"57.703125",NoPulOutChrgPlugWrng=L"0",AntiSlope=L"0",HVRelSta=L"2",VecActPowPer=L"0",BatChrgVolCpl=L"116.8",MaxWrngLvl_G=L"0",InitaContIndOn_IC=L"0",BatheatindON=L"0",DFCUAcePedlPlbtyErr=L"0",AccActPos=L"24",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",ACSwitch=L"0",VCUCtrlRqst1RollCnt=L"3",TMSts_G=L"4",VehSts_G=L"1",VehSpdAvgDrvn=L"58.015625",InitaContIndOn_SDM=L"0",RealSoc=L"69",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"2.1",t02RDelay=L"1",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Direction=L"0",Lngtd_G=L"111.608393",RemtCtrlPrkHet=L"0",CPSts=L"0",BatThrmlRunwyAlmInd=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:49",DCDCInpCur=L"0.4",BatTalVolSm=L"104.4",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",BatMaxClVlt_G=L"3.268",AcInpFlt=L"0",VBatChrgStsIndOn=L"1",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"6418",BatCurSensFlt=L"0",AirbgIndOn=L"0",BatMinTemVolPakOrd=L"1",TMCtrlMdRqst=L"2",OBCInpMxCur=L"0",DrivStOccSt=L"1",BatSOH=L"98",BatSOC=L"68",BatEnrgAvail=L"9.4",VehOdo=L"18751",t2RIP=L"192.168.20.105",BatTempSensNum_G1=L"8",BatMaxTempCode_G=L"7",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"0",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",BatTempSensFlt=L"0",BatDvcCur_G1=L"2.3",TerminalNO=L"7681100322375855702M1236680",InvVol=L"104",InvJctTemp=L"40",DFCMongHvNotIntd=L"0",DCDCInpVol=L"104.3",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"9.04",MCUCurFlt=L"0",BatExtVol=L"104.9",DrSbltAtcV=L"0",BatIntVol=L"104.8",TMNum_G=L"1",OBCOvTemp=L"0",VCURemtCtrlSpdLmtFb=L"0",SupCode=L"1",DFCTioutForVT=L"0",InvrCur=L"2",Bat2sPlsChrgPowAvail=L"9.04",vehConfig=L"LV1",VecActGearSts=L"1",VehConf=L"3",BatMinTempPos=L"1",CellBattNum_21=L"21",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"57.76",VecChrgingSts=L"0",Bat10sPlsDischrgPowAvail=L"16",WhlGrndVlctyRtDrvn=L"57.74",SftwrMjrVsin=L"6",OBCInpVlt=L"0",RatedVol=L"103",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",BatMaxTemp=L"26",EvpRlySta=L"0",TMTorq_G=L"0",BatInfLen=L"0",TMActTemp=L"69",PlsAgainToRdy=L"0",BatChrgSts=L"0",HandBrkSts=L"0",InitaContIndOn_UCU=L"0",SOC_G=L"68",IslatRes_G=L"5000",objType=L"E50",BatSubsysTemp_G1=L"25_25_25_25_25_25_26_25",BatContiDischrgPowAvail=L"16",InitaContIndOn_MCU=L"0",PowBatErrIndOn=L"0",BatSOCSTS=L"0",VecIslateWrngIndOn=L"0",DCDCInternalTemp=L"30",DFCMCUGen1TiOut=L"0",MinTempSubSysNum_G=L"1",Bat2sPlsDischrgPowAvail=L"16",TMActTorq=L"0",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:49",VehDrvMod=L"1",MCUCtrlFlt=L"0",BatChrgTims=L"171",BatManuCode=L"3158579",MESSAGETYPE=L"REALTIME",VacPumpWrngIndOn=L"0",TalNumOfClBatInThisFrm_G1=L"32",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DFCDOPIN56Flt=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",RemtHeatModRqst=L"0",InitaContIndOn_BMS=L"0" 1678171309000',
        #              'E50,VIN=LK6ADAE16ME550370 TalClNum_G1=L"28",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",BMSCode=L"3",EPSWorkCur=L"0",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",TMOrd_G=L"1",DrSbltAtc=L"1",RrImpctDet=L"0",BatMiniCelVol=L"3258",OBCORDCACIntnlTemp=L"40",TMDeratingSts=L"0",TemperoHeatMeb3=L"205",BattCellVoltage_16=L"3.264",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",TemperoHeatMeb6=L"205",BatMinTemp=L"20",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"205",PDUHVLock=L"1",BattCellVoltage_17=L"3.264",BatFltNumN1_G=L"0",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"91.8",MCUCur_G=L"18",vehType=L"E50",Longitude=L"116.081679",WhlGrndVlctyLftDrvn=L"25.81",BatPowSts=L"1",DFCDCANMUTE=L"0",TMActWkSts=L"3",BrkProcInPrgrsIO=L"0",CDUState=L"5",TMActSpd=L"1949",objId=L"LK6ADAE16ME550370",PTID=L"2",DFCShiftLvrPlbtyErr=L"0",ABSAtv=L"0",OBCInpVolOv=L"0",EngyRevIndOn=L"0",RemainOdo=L"100",BrakPedalPos=L"0",BatMaPosRlySts=L"1",TMTemp_G=L"44",BatCelVolSensFlt=L"0",CDUType=L"9",Lattd_G=L"29.754856",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",DrTorqRqst=L"8",BatIslateSts=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"3",DCDCIntnlFlt=L"0",GearSts_G=L"14",VecChrgStsIndOn=L"0",zdsjxh=L"278",AclrtrPedaStrk_G=L"22",TMOvSpdInd=L"0",EvpEquPreVal=L"-63.8",InitaContIndOn_EPS=L"0",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"1",DCDCOtpVol=L"14",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"11",InvActTemp=L"33",BMSGenSts1RollCnt=L"1",GPSspeed=L"0",SignalStrengthOne=L"4",StrWhAng=L"-2",SignalStrengthTwo=L"30",RemtCtrParkHeat=L"0",createTime=L"1678171310064",ACRelayCmd=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"10",BatMinTempCode_G=L"3",CCSts=L"0",BatDvcVlt_G1=L"91.2",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",MCUSupCode=L"8340040",dqsjxh=L"851",Latitude=L"29.754856",VecActPow=L"1.4",BMSHvPowOnRqst=L"2",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",VecSOCLoWrngIndOn=L"0",OBCOtpVlt=L"91.3",BatMaxCelVolPos=L"28",t2RTime=L"2023-03-07 14:41:50",TMActDrvTorLmt=L"84.75",InitaContIndOn_OBC=L"1",TalCur_G=L"18.1",BatAvgTemp=L"21",OBCInpCurOv=L"0",BatMaxClVltCode_G=L"28",CollisionSig=L"0",VecStatRdy=L"1",BatMaxCelVol=L"3264",ChrgCtrlSnglFail=L"0",KyPstn=L"2",BatPrechrgRlySts=L"0",BatFuSts=L"0",MCUActFltNum=L"0",DCDCTempWrng=L"0",BatSubsyst_Vlt_G=L"1",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"25.7",BMSSts=L"3",DriverLeftWarning=L"0",BatMinTemp_G=L"20",BatRmaChrgTim=L"2047",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",ModeCode=L"116",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"18.1",BMSChrgCurRqst=L"0",Swit2Status=L"0",BatAvgCelVol=L"3260",t1RTime=L"2023-03-07 14:41:49",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",VacPumpTransRate=L"1",DFCGearReqMongErr=L"0",BatHetPrsrvtn=L"0",DFCCASmax=L"0",BatOthFltNum=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",VecFltIndOn=L"0",BatMinClVltCode_G=L"6",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",VecActTalCur=L"14.9",ISBF=L"0",BatClVlt_G1=L"3.267_3.264_3.264_3.264_3.264_3.263_3.264_3.264_3.264_3.263_3.263_3.266_3.266_3.264_3.264_3.264_3.264_3.264_3.264_3.267_3.267_3.265_3.264_3.263_3.264_3.265_3.264_3.268",BatFltLvl=L"0",MCUInpVlt_G=L"91",TMWkStsRqst=L"2",BatMaxTempPos=L"5",TMActFbTorLmt=L"-85",DrvPowLimtIndOn=L"0",OBCOtpVolOv=L"0",BatAvgCelVolSts=L"0",VCUSftwrVsinNum=L"233",OBCOtpCur=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"0",TalVlt_G=L"91.2",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"10",TMTorqRqst=L"8",InvVolSts=L"0",VehOdoV=L"0",ShifGearFailAtHiSpeed=L"0",VecOptMod=L"3",SoftEdt=L"151",TMorMCUOvTempInd=L"0",OptMod_G=L"1",RemtCtrlSpdLimtRqstSt=L"1",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"4",VehOdo_G=L"11768.4",MCUTemp_G=L"33",TMSpdRqst=L"0",LowBatVol=L"14.15",BatTempSts=L"0",VecChrgRqst=L"0",VecTMActSpdV=L"0",VecTMActSpd=L"1923",VCUBatThrmlRunwyAlmInd=L"0",MCUVolFlt=L"0",DCDCOtpVolSts=L"0",TMSpd_G=L"1949",BatChrgCurSts=L"0",BatMaxTemp_G=L"22",EvrmetAblPreVal=L"101.3",OBCInpCur=L"0",BatIslatRes=L"40895",BatMiniCelVolPos=L"6",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",OBCInpVolUn=L"0",VecSOC=L"59",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCOtpCur=L"2.3",InitaContIndOn_ABS=L"0",BMSChrgVolCurRqstAnorm=L"0",CollectTime=L"2023-03-07 14:41:49",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"16",BatMinClVlt_G=L"3.258",RatedEgy=L"13.4",WhlGrndVlctyRtNnDrvn=L"25.84",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"25.875",NoPulOutChrgPlugWrng=L"0",AntiSlope=L"0",HVRelSta=L"2",VecActPowPer=L"7",BatChrgVolCpl=L"103.2",MaxWrngLvl_G=L"0",InitaContIndOn_IC=L"0",BatheatindON=L"0",DFCUAcePedlPlbtyErr=L"0",AccActPos=L"22",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",ACSwitch=L"0",VCUCtrlRqst1RollCnt=L"10",TMSts_G=L"4",VehSts_G=L"1",VehSpdAvgDrvn=L"25.703125",InitaContIndOn_SDM=L"0",RealSoc=L"60",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"22.6",t02RDelay=L"1",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Direction=L"0",Lngtd_G=L"116.081679",RemtCtrlPrkHet=L"0",CPSts=L"0",BatThrmlRunwyAlmInd=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:49",DCDCInpCur=L"0.3",BatTalVolSm=L"91.2",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",BatMaxClVlt_G=L"3.264",AcInpFlt=L"0",VBatChrgStsIndOn=L"1",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"3195",BatCurSensFlt=L"0",AirbgIndOn=L"0",BatMinTemVolPakOrd=L"1",TMCtrlMdRqst=L"2",OBCInpMxCur=L"7",DrivStOccSt=L"1",BatSOH=L"100",BatSOC=L"59",BatEnrgAvail=L"7.6",VehOdo=L"11768.4375",t2RIP=L"192.168.40.103",BatTempSensNum_G1=L"7",BatMaxTempCode_G=L"5",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"1.6",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",BatTempSensFlt=L"0",BatDvcCur_G1=L"18.1",TerminalNO=L"7683202652375855703M9136644",InvVol=L"91",InvJctTemp=L"33",DFCMongHvNotIntd=L"0",DCDCInpVol=L"91.3",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"10",MCUCurFlt=L"0",BatExtVol=L"91.3",DrSbltAtcV=L"0",BatIntVol=L"91.6",TMNum_G=L"1",OBCOvTemp=L"0",VCURemtCtrlSpdLmtFb=L"0",SupCode=L"0",DFCTioutForVT=L"0",InvrCur=L"18",Bat2sPlsChrgPowAvail=L"10",vehConfig=L"LV1",VecActGearSts=L"1",VehConf=L"3",BatMinTempPos=L"3",CellBattNum_21=L"21",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"25.79",VecChrgingSts=L"0",Bat10sPlsDischrgPowAvail=L"16",WhlGrndVlctyRtDrvn=L"25.9",SftwrMjrVsin=L"4",OBCInpVlt=L"0",RatedVol=L"89",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",BatMaxTemp=L"22",EvpRlySta=L"0",TMTorq_G=L"8",BatInfLen=L"0",TMActTemp=L"44",PlsAgainToRdy=L"0",BatChrgSts=L"0",HandBrkSts=L"0",InitaContIndOn_UCU=L"0",SOC_G=L"59",IslatRes_G=L"40895",objType=L"E50",BatSubsysTemp_G1=L"21_21_20_21_22_21_22",BatContiDischrgPowAvail=L"16",InitaContIndOn_MCU=L"0",PowBatErrIndOn=L"0",BatSOCSTS=L"0",VecIslateWrngIndOn=L"0",DCDCInternalTemp=L"39",DFCMCUGen1TiOut=L"0",MinTempSubSysNum_G=L"1",Bat2sPlsDischrgPowAvail=L"16",TMActTorq=L"8",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:49",VehDrvMod=L"1",MCUCtrlFlt=L"0",BatChrgTims=L"76",BatManuCode=L"3158856",MESSAGETYPE=L"REALTIME",VacPumpWrngIndOn=L"0",TalNumOfClBatInThisFrm_G1=L"28",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DFCDOPIN56Flt=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",RemtHeatModRqst=L"0",InitaContIndOn_BMS=L"0" 1678171309000',
        #              'E50,VIN=LK6ADAE17NB212172 BMSCode=L"0",TMOrd_G=L"1",RRDoorOpenSwAct=L"0",RrImpctDet=L"0",BattCellVoltage_16=L"3.195",BattCellVoltage_17=L"3.188",BatFltNumN1_G=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"95.9",MCUCur_G=L"63",BatPowSts=L"1",TaPnLgtAtv=L"0",TrnsRvsSwSt=L"0",PDAjrSwAtv=L"0",TMActSpd=L"1521",DFCShiftLvrPlbtyErr=L"0",OBCInpVolOv=L"0",RemainOdo=L"102",BatMaPosRlySts=L"1",CDUType=L"10",Lattd_G=L"35.111653",DrTorqRqst=L"38",VehiReadToDriWarning=L"0",ChrgSts_G=L"3",DCDCIntnlFlt=L"0",GearSts_G=L"14",VecChrgStsIndOn=L"0",zdsjxh=L"457",EvpEquPreVal=L"-61.9",InitaContIndOn_EPS=L"0",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"2",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"2",InvActTemp=L"35",BMSGenSts1RollCnt=L"13",SignalStrengthOne=L"4",StrWhAng=L"6",SignalStrengthTwo=L"0",TDEnblSts=L"0",createTime=L"1678171310096",ACRelayCmd=L"0",BatDvcVlt_G1=L"95.8",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",dqsjxh=L"1115",MCUSupCode=L"8440176",HzrdLtIO=L"0",VecActPow=L"5.9",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",OBCOtpVlt=L"95.5",BatMaxCelVolPos=L"10",t2RTime=L"2023-03-07 14:41:50",InitaContIndOn_OBC=L"0",TalCur_G=L"77.3",BatAvgTemp=L"19",OBCInpCurOv=L"0",ChargPrtOpInOnPrmt=L"0",ChrgCtrlSnglFail=L"0",KyPstn=L"2",MCUActFltNum=L"0",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"19.9",BatMinTemp_G=L"18",ModeCode=L"132",BMSChrgCurRqst=L"0",RtTrnLmpAtv=L"0",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",DFCGearReqMongErr=L"0",DFCCASmax=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",BatMinClVltCode_G=L"9",LSDAjrSwAtv=L"0",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",TMWkStsRqst=L"2",OBCOtpVolOv=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"95.8",TMTorqRqst=L"38",ShifGearFailAtHiSpeed=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"6",VehOdo_G=L"6089",BatTempSts=L"0",LowBatVol=L"13.89",VCUBatThrmlRunwyAlmInd=L"0",VecTMActSpd=L"1493",DCDCOtpVolSts=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"20",EvrmetAblPreVal=L"96.6",BatIslatRes=L"10044",BatMiniCelVolPos=L"9",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",VecSOC=L"60",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"25",WhlGrndVlctyRtNnDrvn=L"20.61",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"20.53125",RemtCtrlSpdLmtRqstSt=L"1",VecActPowPer=L"29",BatChrgVolCpl=L"110.5",InitaContIndOn_IC=L"0",DFCUAcePedlPlbtyErr=L"0",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",VCUCtrlRqst1RollCnt=L"10",ACSwitch=L"0",TMSts_G=L"4",LftTrnLmpAtv=L"0",RealSoc=L"56",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"8.1",Direction=L"0",RemtCtrlPrkHet=L"0",CPSts=L"0",HzrdLgtSwAtv=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",TDAjrSwAtv=L"0",DCDCInpCur=L"1.1",BatTalVolSm=L"95.8",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",VBatChrgStsIndOn=L"1",BatCurSensFlt=L"0",AirbgIndOn=L"0",TMCtrlMdRqst=L"2",OBCInpMxCur=L"0",BatEnrgAvail=L"6.3",BatTempSensNum_G1=L"6",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",IgnKyPstn=L"2",BatDvcCur_G1=L"77.3",TerminalNO=L"7683202652375880403N4131032",InvVol=L"94",InvJctTemp=L"35",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"9.6",MCUCurFlt=L"0",BatExtVol=L"95.8",DrSbltAtcV=L"0",TMNum_G=L"1",OBCOvTemp=L"0",SupCode=L"2",DFCTioutForVT=L"0",Bat2sPlsChrgPowAvail=L"9.6",vehConfig=L"LV1",VecActGearSts=L"1",VehConf=L"3",BalanceSts=L"1",WhlGrndVlctyLftNnDrvn=L"20.64",WhlGrndVlctyRtDrvn=L"20.72",OBCInpVlt=L"0",BatMaxTemp=L"20",EvpRlySta=L"0",IALPwrMdCtrlSt=L"0",TMActTemp=L"46",PlsAgainToRdy=L"0",HandBrkSts=L"0",IslatRes_G=L"10044",KyOpenSwAct=L"0",objType=L"E50",BatContiDischrgPowAvail=L"16",InitaContIndOn_MCU=L"0",VecIslateWrngIndOn=L"0",DFCMCUGen1TiOut=L"0",Bat2sPlsDischrgPowAvail=L"16",MCUCtrlFlt=L"0",BatChrgTims=L"42",VacPumpWrngIndOn=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",InitaContIndOn_BMS=L"0",TalClNum_G1=L"30",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",EPSWorkCur=L"0",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",DrSbltAtc=L"1",BatMiniCelVol=L"3187",TemperoHeatMeb3=L"205",TMDeratingSts=L"0",OBCORDCACIntnlTemp=L"29",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",BatMinTemp=L"18",TemperoHeatMeb6=L"205",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"9",PDUHVLock=L"1",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"0",vehType=L"E50",Longitude=L"110.788184",DFCDCANMUTE=L"0",WhlGrndVlctyLftDrvn=L"20.81",BrkProcInPrgrsIO=L"0",TMActWkSts=L"3",CDUState=L"5",objId=L"LK6ADAE17NB212172",PTID=L"2",ABSAtv=L"0",EngyRevIndOn=L"0",BrakPedalPos=L"0",TMTemp_G=L"46",BatCelVolSensFlt=L"0",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",BatIslateSts=L"0",AclrtrPedaStrk_G=L"37",TMOvSpdInd=L"0",AnThWaSt=L"1",DCDCOtpVol=L"13.9",RrFgLtIO=L"0",GPSspeed=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"1",BatMinTempCode_G=L"3",CCSts=L"0",IgnKyInstAtv=L"0",Latitude=L"35.111653",BMSHvPowOnRqst=L"2",VecSOCLoWrngIndOn=L"0",TMActDrvTorLmt=L"85",BatMaxClVltCode_G=L"10",CollisionSig=L"0",VecStatRdy=L"1",BatMaxCelVol=L"3207",LwBmIO=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",DCDCTempWrng=L"0",BCMRunMod=L"0",BatSubsyst_Vlt_G=L"1",BMSSts=L"3",DriverLeftWarning=L"0",BatRmaChrgTim=L"0",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"77.3",FtFgLtIO=L"0",BatAvgCelVol=L"3195",Swit2Status=L"0",t1RTime=L"2023-03-07 14:41:50",VacPumpTransRate=L"1",BCMSftwrVsinNum=L"2",BatHetPrsrvtn=L"0",BatOthFltNum=L"0",VecFltIndOn=L"0",VecActTalCur=L"76.5",ISBF=L"0",BatClVlt_G1=L"3.195_3.194_3.198_3.202_3.189_3.191_3.196_3.205_3.188_3.208_3.189_3.188_3.189_3.198_3.191_3.195_3.188_3.197_3.197_3.193_3.201_3.201_3.2_3.195_3.2_3.196_3.199_3.199_3.199_3.202",BatFltLvl=L"0",MCUInpVlt_G=L"94",BatMaxTempPos=L"6",TMActFbTorLmt=L"-85.25",DrvPowLimtIndOn=L"0",BatAvgCelVolSts=L"0",OBCOtpCur=L"0",VCUSftwrVsinNum=L"162",DDAjrSwAtv=L"0",TalVlt_G=L"95.8",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"9.6",InvVolSts=L"0",VehOdoV=L"0",VecOptMod=L"3",SoftEdt=L"151",RSDAjrSwAtv=L"0",TMorMCUOvTempInd=L"0",OptMod_G=L"1",MCUTemp_G=L"35",TMSpdRqst=L"0",VecChrgRqst=L"0",VecTMActSpdV=L"0",RLDoorOpenSwAct=L"0",MCUVolFlt=L"0",TMSpd_G=L"1521",OBCInpCur=L"0",OBCInpVolUn=L"0",DCDCOtpCur=L"7.8",BMSChrgVolCurRqstAnorm=L"0",InitaContIndOn_ABS=L"0",CollectTime=L"2023-03-07 14:41:49",BatMinClVlt_G=L"3.187",RatedEgy=L"13.7",NoPulOutChrgPlugWrng=L"0",PsDoorOpenSwAct=L"0",AntiSlope=L"0",HVRelSta=L"2",MaxWrngLvl_G=L"0",BatheatindON=L"0",AccActPos=L"37",InitaContIndOn_BCM=L"0",TrnSwAct=L"0",VehSts_G=L"1",VehSpdAvgDrvn=L"19.921875",InitaContIndOn_SDM=L"0",HhBmIO=L"0",t02RDelay=L"1",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Lngtd_G=L"110.788184",BatThrmlRunwyAlmInd=L"0",StpLpSt=L"0",PrkBrkSwAtv=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:49",BatMaxClVlt_G=L"3.207",AcInpFlt=L"0",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"5038",BatMinTemVolPakOrd=L"1",BatSOH=L"100",DrivStOccSt=L"1",BatSOC=L"60",VehOdo=L"6089",t2RIP=L"192.168.30.104",BatMaxTempCode_G=L"6",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"5.9",WindscenWipSt=L"0",LdspkSt=L"0",BatTempSensFlt=L"0",DFCMongHvNotIntd=L"0",DCDCInpVol=L"94.9",WipSwStat=L"1",BatIntVol=L"95.9",VCURemtCtrlSpdLmtFb=L"0",InvrCur=L"63",BatMinTempPos=L"3",VecChrgingSts=L"0",Bat10sPlsDischrgPowAvail=L"16",SftwrMjrVsin=L"20",FtPnLgtAtv=L"0",RatedVol=L"97",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",TMTorq_G=L"38",BatInfLen=L"0",BatChrgSts=L"0",InitaContIndOn_UCU=L"0",BCMRunModV=L"1",SOC_G=L"60",BatSubsysTemp_G1=L"19_19_18_19_19_20",PowBatErrIndOn=L"0",BatSOCSTS=L"0",DCDCInternalTemp=L"32",MaiLgtSw=L"0",MinTempSubSysNum_G=L"1",TMActTorq=L"38",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:49",VehDrvMod=L"1",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",TalNumOfClBatInThisFrm_G1=L"30",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DrDoorOpenSwAct=L"0",DFCDOPIN56Flt=L"0",RemtHeatModRqst=L"0" 1678171309000',
        #              'E50,VIN=LK6ADAE1XMB274132 BMSCode=L"5",TMOrd_G=L"1",RRDoorOpenSwAct=L"0",RrImpctDet=L"0",BattCellVoltage_16=L"3.875",BattCellVoltage_17=L"3.874",BatFltNumN1_G=L"0",BatMaNegRlySts=L"0",VecActTalVol=L"100.7",MCUCur_G=L"0",BatPowSts=L"0",TaPnLgtAtv=L"0",TrnsRvsSwSt=L"0",PDAjrSwAtv=L"0",TMActSpd=L"0",DFCShiftLvrPlbtyErr=L"0",OBCInpVolOv=L"0",RemainOdo=L"96",BatMaPosRlySts=L"0",CDUType=L"9",Lattd_G=L"38.314143",DrTorqRqst=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"3",DCDCIntnlFlt=L"0",GearSts_G=L"0",VecChrgStsIndOn=L"0",zdsjxh=L"2",EvpEquPreVal=L"-50.4",InitaContIndOn_EPS=L"1",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"2",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"1",InvActTemp=L"23",BMSGenSts1RollCnt=L"0",SignalStrengthOne=L"4",StrWhAng=L"4078.6875",SignalStrengthTwo=L"31",TDEnblSts=L"0",createTime=L"1678171310052",ACRelayCmd=L"0",BatDvcVlt_G1=L"100.7",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",dqsjxh=L"133",MCUSupCode=L"8340040",HzrdLtIO=L"0",VecActPow=L"0",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",OBCOtpVlt=L"0",BatMaxCelVolPos=L"5",t2RTime=L"2023-03-07 14:41:50",InitaContIndOn_OBC=L"1",TalCur_G=L"0",BatAvgTemp=L"17",OBCInpCurOv=L"0",ChargPrtOpInOnPrmt=L"0",ChrgCtrlSnglFail=L"0",KyPstn=L"0",MCUActFltNum=L"0",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"0",BatMinTemp_G=L"17",ModeCode=L"135",BMSChrgCurRqst=L"0",RtTrnLmpAtv=L"0",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"2",DFCGearReqMongErr=L"0",DFCCASmax=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",BatMinClVltCode_G=L"11",LSDAjrSwAtv=L"0",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"0",TMWkStsRqst=L"0",OBCOtpVolOv=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"100.7",TMTorqRqst=L"0",ShifGearFailAtHiSpeed=L"0",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"3",VehOdo_G=L"8991",BatTempSts=L"0",LowBatVol=L"12.69",VCUBatThrmlRunwyAlmInd=L"0",VecTMActSpd=L"0",DCDCOtpVolSts=L"0",BatChrgCurSts=L"0",BatMaxTemp_G=L"18",EvrmetAblPreVal=L"100.9",BatIslatRes=L"10000",BatMiniCelVolPos=L"11",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",VecSOC=L"57",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCEnable=L"0",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"132",WhlGrndVlctyRtNnDrvn=L"0",DCDCStsWrng=L"0",TMActCtrlMdSts=L"0",VehSpdAvgDrvnRd=L"0",RemtCtrlSpdLmtRqstSt=L"0",VecActPowPer=L"0",BatChrgVolCpl=L"110",InitaContIndOn_IC=L"1",DFCUAcePedlPlbtyErr=L"0",PTCSwitch=L"2",MaxVltBatSubSysNum_G=L"1",VCUCtrlRqst1RollCnt=L"1",ACSwitch=L"0",TMSts_G=L"4",LftTrnLmpAtv=L"0",RealSoc=L"57",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"2.1",Direction=L"0",RemtCtrlPrkHet=L"0",CPSts=L"0",HzrdLgtSwAtv=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",TDAjrSwAtv=L"0",DCDCInpCur=L"0",BatTalVolSm=L"100.7",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",VBatChrgStsIndOn=L"1",BatCurSensFlt=L"0",AirbgIndOn=L"1",TMCtrlMdRqst=L"2",OBCInpMxCur=L"0",BatEnrgAvail=L"6.7",BatTempSensNum_G1=L"6",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",IgnKyPstn=L"0",BatDvcCur_G1=L"0",TerminalNO=L"7681100322375880402M4153809",InvVol=L"1",InvJctTemp=L"23",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"9.4",MCUCurFlt=L"0",BatExtVol=L"0",DrSbltAtcV=L"0",TMNum_G=L"1",OBCOvTemp=L"0",SupCode=L"1",DFCTioutForVT=L"0",Bat2sPlsChrgPowAvail=L"9.4",vehConfig=L"LV1",VecActGearSts=L"0",VehConf=L"3",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"0",WhlGrndVlctyRtDrvn=L"0",OBCInpVlt=L"0",BatMaxTemp=L"18",EvpRlySta=L"0",IALPwrMdCtrlSt=L"0",TMActTemp=L"23",PlsAgainToRdy=L"0",HandBrkSts=L"1",IslatRes_G=L"10000",KyOpenSwAct=L"0",objType=L"E50",BatContiDischrgPowAvail=L"16",InitaContIndOn_MCU=L"1",VecIslateWrngIndOn=L"0",DFCMCUGen1TiOut=L"0",Bat2sPlsDischrgPowAvail=L"16",MCUCtrlFlt=L"0",BatChrgTims=L"54",VacPumpWrngIndOn=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",InitaContIndOn_BMS=L"1",TalClNum_G1=L"26",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"0",VCUVehDrvMod=L"0",EPSWorkCur=L"0",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",DrSbltAtc=L"0",BatMiniCelVol=L"3871",TemperoHeatMeb3=L"205",TMDeratingSts=L"0",OBCORDCACIntnlTemp=L"23",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",BatMinTemp=L"17",TemperoHeatMeb6=L"205",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"7",PDUHVLock=L"1",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"1",vehType=L"E50",Longitude=L"116.863946",DFCDCANMUTE=L"0",WhlGrndVlctyLftDrvn=L"0",BrkProcInPrgrsIO=L"0",TMActWkSts=L"1",CDUState=L"5",objId=L"LK6ADAE1XMB274132",PTID=L"2",ABSAtv=L"0",EngyRevIndOn=L"0",BrakPedalPos=L"0",TMTemp_G=L"23",BatCelVolSensFlt=L"0",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",BatIslateSts=L"0",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",AnThWaSt=L"5",DCDCOtpVol=L"12.5",RrFgLtIO=L"0",GPSspeed=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"7",BatMinTempCode_G=L"3",CCSts=L"0",IgnKyInstAtv=L"0",Latitude=L"38.314143",BMSHvPowOnRqst=L"0",VecSOCLoWrngIndOn=L"0",TMActDrvTorLmt=L"84.75",BatMaxClVltCode_G=L"5",CollisionSig=L"0",VecStatRdy=L"0",BatMaxCelVol=L"3876",LwBmIO=L"0",BatPrechrgRlySts=L"0",BatFuSts=L"0",DCDCTempWrng=L"0",BCMRunMod=L"0",BatSubsyst_Vlt_G=L"1",BMSSts=L"1",DriverLeftWarning=L"0",BatRmaChrgTim=L"0",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"0",BatTalCurr=L"0",FtFgLtIO=L"0",BatAvgCelVol=L"3874",Swit2Status=L"0",t1RTime=L"2023-03-07 14:41:50",VacPumpTransRate=L"1",BCMSftwrVsinNum=L"1",BatHetPrsrvtn=L"0",BatOthFltNum=L"0",VecFltIndOn=L"0",VecActTalCur=L"0",ISBF=L"0",BatClVlt_G1=L"3.872_3.875_3.875_3.874_3.876_3.875_3.873_3.874_3.873_3.875_3.871_3.874_3.875_3.875_3.876_3.875_3.874_3.874_3.875_3.876_3.876_3.875_3.876_3.875_3.874_3.873",BatFltLvl=L"0",MCUInpVlt_G=L"1",BatMaxTempPos=L"1",TMActFbTorLmt=L"-85",DrvPowLimtIndOn=L"0",BatAvgCelVolSts=L"0",OBCOtpCur=L"0",VCUSftwrVsinNum=L"161",DDAjrSwAtv=L"0",TalVlt_G=L"100.7",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"9.4",InvVolSts=L"0",VehOdoV=L"0",VecOptMod=L"0",SoftEdt=L"180",RSDAjrSwAtv=L"0",TMorMCUOvTempInd=L"0",OptMod_G=L"1",MCUTemp_G=L"23",TMSpdRqst=L"0",VecChrgRqst=L"0",VecTMActSpdV=L"0",RLDoorOpenSwAct=L"0",MCUVolFlt=L"0",TMSpd_G=L"0",OBCInpCur=L"0",OBCInpVolUn=L"0",DCDCOtpCur=L"0",BMSChrgVolCurRqstAnorm=L"0",InitaContIndOn_ABS=L"1",CollectTime=L"2023-03-07 14:41:50",BatMinClVlt_G=L"3.871",RatedEgy=L"14",NoPulOutChrgPlugWrng=L"0",PsDoorOpenSwAct=L"0",AntiSlope=L"0",HVRelSta=L"0",MaxWrngLvl_G=L"0",BatheatindON=L"0",AccActPos=L"0",InitaContIndOn_BCM=L"1",TrnSwAct=L"0",VehSts_G=L"2",VehSpdAvgDrvn=L"0",InitaContIndOn_SDM=L"1",HhBmIO=L"0",t02RDelay=L"0",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"1",Lngtd_G=L"116.863946",BatThrmlRunwyAlmInd=L"0",StpLpSt=L"0",PrkBrkSwAtv=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:50",BatMaxClVlt_G=L"3.876",AcInpFlt=L"0",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"6743",BatMinTemVolPakOrd=L"1",BatSOH=L"100",DrivStOccSt=L"1",BatSOC=L"57",VehOdo=L"8991",t2RIP=L"192.168.10.103",BatMaxTempCode_G=L"1",TmnlWkupSurc=L"2",OBCHdErr=L"0",TMActPow=L"0",WindscenWipSt=L"0",LdspkSt=L"0",BatTempSensFlt=L"0",DFCMongHvNotIntd=L"0",DCDCInpVol=L"0",WipSwStat=L"1",BatIntVol=L"100.7",VCURemtCtrlSpdLmtFb=L"0",InvrCur=L"0",BatMinTempPos=L"3",VecChrgingSts=L"0",Bat10sPlsDischrgPowAvail=L"16",SftwrMjrVsin=L"6",FtPnLgtAtv=L"0",RatedVol=L"96",PstnSts_G=L"1",PowBatCutOffIndOn=L"0",TMTorq_G=L"0",BatInfLen=L"0",BatChrgSts=L"0",InitaContIndOn_UCU=L"0",BCMRunModV=L"1",SOC_G=L"57",BatSubsysTemp_G1=L"18_18_17_17_18_18",PowBatErrIndOn=L"0",BatSOCSTS=L"0",DCDCInternalTemp=L"22",MaiLgtSw=L"0",MinTempSubSysNum_G=L"1",TMActTorq=L"0",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:50",VehDrvMod=L"1",BatManuCode=L"3158861",MESSAGETYPE=L"REALTIME",TalNumOfClBatInThisFrm_G1=L"26",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DrDoorOpenSwAct=L"0",DFCDOPIN56Flt=L"0",RemtHeatModRqst=L"0" 1678171310000',
        #              'E50,VIN=LK6ADCE11LE277090 TalClNum_G1=L"30",DCDCOtpShtCicut=L"0",SftwrCalVsin=L"5",VCUVehDrvMod=L"0",BMSCode=L"1",EPSWorkCur=L"1.25",UnvslWrngSig_G=L"0",GBFltWrn=L"0",MinVltBatSubSysNum_G=L"1",ACCmd=L"0",ProtocolVersion=L"1_1_1",TMOrd_G=L"1",DrSbltAtc=L"1",RrImpctDet=L"0",BatMiniCelVol=L"3304",OBCORDCACIntnlTemp=L"28",TMDeratingSts=L"0",TemperoHeatMeb3=L"205",BattCellVoltage_16=L"3.305",TemperoHeatMeb2=L"205",TemperoHeatMeb1=L"205",TemperoHeatMeb7=L"205",TemperoHeatMeb6=L"205",BatMinTemp=L"20",TemperoHeatMeb5=L"205",TemperoHeatMeb4=L"205",PDUHVLock=L"1",BattCellVoltage_17=L"3.305",BatFltNumN1_G=L"0",TemperoHeatMeb8=L"205",InitaContIndOn_VCU=L"0",BatMaNegRlySts=L"1",VecActTalVol=L"98.6",MCUCur_G=L"-3",vehType=L"E50",Longitude=L"116.069523",WhlGrndVlctyLftDrvn=L"4.78",BatPowSts=L"1",DFCDCANMUTE=L"0",TMActWkSts=L"3",BrkProcInPrgrsIO=L"0",CDUState=L"5",TMActSpd=L"369",objId=L"LK6ADCE11LE277090",PTID=L"2",DFCShiftLvrPlbtyErr=L"0",ABSAtv=L"0",OBCInpVolOv=L"0",EngyRevIndOn=L"0",RemainOdo=L"89",BrakPedalPos=L"1",BatMaPosRlySts=L"1",TMTemp_G=L"33",BatCelVolSensFlt=L"0",CDUType=L"9",Lattd_G=L"35.389911",VehSpdAvgDrvnV=L"0",BatSubsysNum_Temp_G=L"1",DrTorqRqst=L"0",BatIslateSts=L"0",VehiReadToDriWarning=L"0",ChrgSts_G=L"3",DCDCIntnlFlt=L"0",GearSts_G=L"14",VecChrgStsIndOn=L"0",zdsjxh=L"524",AclrtrPedaStrk_G=L"0",TMOvSpdInd=L"0",EvpEquPreVal=L"-52.7",InitaContIndOn_EPS=L"0",BatHeSts=L"0",MCUSlfChcklFlt=L"0",VCUSftwrConf=L"1",DCDCOtpVol=L"14.1",TMFltIndOn=L"0",MCUGenSts1RollCnt=L"2",InvActTemp=L"30",BMSGenSts1RollCnt=L"11",GPSspeed=L"0",SignalStrengthOne=L"4",StrWhAng=L"-45.4375",SignalStrengthTwo=L"31",RemtCtrParkHeat=L"0",createTime=L"1678171310064",ACRelayCmd=L"0",PwrStrIo=L"0",DCDCGenStsRollCnt=L"0",BatMinTempCode_G=L"1",CCSts=L"0",BatDvcVlt_G1=L"99.1",PTCRelayCmd=L"0",BatHvIntlkSts=L"1",AirConHVLock=L"0",MCUSupCode=L"8310270",dqsjxh=L"268",Latitude=L"35.389911",VecActPow=L"0",BMSHvPowOnRqst=L"2",StCharLoVolBat=L"1",PlePullHandBraWhenChar=L"0",Prechrgsts=L"0",VecSOCLoWrngIndOn=L"0",OBCOtpVlt=L"99.2",BatMaxCelVolPos=L"1",t2RTime=L"2023-03-07 14:41:50",TMActDrvTorLmt=L"85",InitaContIndOn_OBC=L"1",TalCur_G=L"1.1",BatAvgTemp=L"20",OBCInpCurOv=L"0",BatMaxClVltCode_G=L"1",CollisionSig=L"0",VecStatRdy=L"1",BatMaxCelVol=L"3308",ChrgCtrlSnglFail=L"0",KyPstn=L"2",BatPrechrgRlySts=L"0",BatFuSts=L"0",MCUActFltNum=L"0",DCDCTempWrng=L"0",BatSubsyst_Vlt_G=L"1",BatHeRlySts=L"0",VehSpdAvgDrvn_G=L"5.2",BMSSts=L"3",DriverLeftWarning=L"0",BatMinTemp_G=L"20",BatRmaChrgTim=L"2047",BatMaxTemVolPakOrd=L"1",HVDCOtpOpCirct=L"0",ModeCode=L"115",StrBatNumOfThisFrm_G1=L"1",DCDCWkSts=L"1",BatTalCurr=L"1.1",BMSChrgCurRqst=L"0",Swit2Status=L"0",BatAvgCelVol=L"3305",t1RTime=L"2023-03-07 14:41:50",TMHTempWrng=L"0",AirConDefaIndiOn=L"0",DCDCSts_G=L"1",VacPumpTransRate=L"1",DFCGearReqMongErr=L"0",BatHetPrsrvtn=L"0",DFCCASmax=L"0",BatOthFltNum=L"0",PowBatTempAnormIndOn=L"0",TMReslvActSts=L"0",VecFltIndOn=L"0",BatMinClVltCode_G=L"11",DFCDOPIN31Flt=L"0",BrkPedalSts_G=L"101",VecActTalCur=L"1.1",ISBF=L"0",BatClVlt_G1=L"3.308_3.306_3.306_3.305_3.306_3.306_3.306_3.306_3.309_3.307_3.305_3.306_3.306_3.305_3.305_3.305_3.305_3.307_3.307_3.306_3.306_3.305_3.306_3.306_3.306_3.306_3.306_3.305_3.305_3.308",BatFltLvl=L"0",MCUInpVlt_G=L"97",TMWkStsRqst=L"2",BatMaxTempPos=L"1",TMActFbTorLmt=L"-85",DrvPowLimtIndOn=L"0",OBCOtpVolOv=L"0",BatAvgCelVolSts=L"0",VCUSftwrVsinNum=L"228",OBCOtpCur=L"0",BatTalVolSts=L"0",BMSChrgVolRqst=L"0",TalVlt_G=L"99.1",DCDCInpVolSts=L"0",BatContiChrgPowAvail=L"8",TMTorqRqst=L"0",InvVolSts=L"0",VehOdoV=L"0",ShifGearFailAtHiSpeed=L"0",VecOptMod=L"3",SoftEdt=L"151",TMorMCUOvTempInd=L"0",OptMod_G=L"1",RemtCtrlSpdLimtRqstSt=L"1",BatInCANBsErrFlt=L"0",SftwrMinrVsin=L"3",VehOdo_G=L"14862",MCUTemp_G=L"30",TMSpdRqst=L"0",LowBatVol=L"14.08",BatTempSts=L"0",VecChrgRqst=L"0",VecTMActSpdV=L"0",VecTMActSpd=L"397",VCUBatThrmlRunwyAlmInd=L"0",MCUVolFlt=L"0",DCDCOtpVolSts=L"0",TMSpd_G=L"369",BatChrgCurSts=L"0",BatMaxTemp_G=L"20",EvrmetAblPreVal=L"100.5",OBCInpCur=L"0",BatIslatRes=L"40409",BatMiniCelVolPos=L"11",t1STime=L"2023-03-07 14:41:50",DCDCFlt=L"0",OBCInpVolUn=L"0",VecSOC=L"77",DFCUAcerPedlDblFlt=L"0",MaxTempSubSysNum_G=L"1",DCDCOtpCur=L"8.1",InitaContIndOn_ABS=L"0",BMSChrgVolCurRqstAnorm=L"0",CollectTime=L"2023-03-07 14:41:50",DCDCEnable=L"1",OBCOtpVolUn=L"0",DCDCGenStsStaCksm=L"167",BatMinClVlt_G=L"3.304",RatedEgy=L"9.6",WhlGrndVlctyRtNnDrvn=L"4.69",DCDCStsWrng=L"0",TMActCtrlMdSts=L"2",VehSpdAvgDrvnRd=L"4.78125",NoPulOutChrgPlugWrng=L"0",AntiSlope=L"0",HVRelSta=L"2",VecActPowPer=L"0",BatChrgVolCpl=L"110.5",MaxWrngLvl_G=L"0",InitaContIndOn_IC=L"0",BatheatindON=L"0",DFCUAcePedlPlbtyErr=L"0",AccActPos=L"0",PTCSwitch=L"0",MaxVltBatSubSysNum_G=L"1",ACSwitch=L"0",VCUCtrlRqst1RollCnt=L"6",TMSts_G=L"4",VehSts_G=L"1",VehSpdAvgDrvn=L"5.296875",InitaContIndOn_SDM=L"0",RealSoc=L"77",OthFltNumN4_G=L"0",SoftwareMaVsnNum=L"4.3",t02RDelay=L"0",ABSIO=L"0",BatSubsysOrd_Vlt_G1=L"1",SoftwareMatching=L"0",Direction=L"0",Lngtd_G=L"116.069523",RemtCtrlPrkHet=L"0",CPSts=L"0",BatThrmlRunwyAlmInd=L"0",EngFltNumN3_G=L"0",ConcetFailOfChrg=L"0",DFCMCUGen2TiOut=L"0",BatDischrgCurSts=L"0",objCollectTime=L"2023-03-07 14:41:50",DCDCInpCur=L"1.2",BatTalVolSm=L"99.1",MCUHTempWrng=L"0",TMOvSpIndOn=L"0",BatMaxClVlt_G=L"3.308",AcInpFlt=L"0",VBatChrgStsIndOn=L"1",VCURemtCtrlMod=L"0",BatSubsysOrd_Temp_G1=L"1",sdgcxh=L"8698",BatCurSensFlt=L"0",AirbgIndOn=L"0",BatMinTemVolPakOrd=L"1",TMCtrlMdRqst=L"2",OBCInpMxCur=L"8",DrivStOccSt=L"1",BatSOH=L"99",BatSOC=L"77",BatEnrgAvail=L"7",VehOdo=L"14862",t2RIP=L"192.168.20.105",BatTempSensNum_G1=L"5",BatMaxTempCode_G=L"1",TmnlWkupSurc=L"4",OBCHdErr=L"0",TMActPow=L"-0.1",EvpActWkSta=L"0",OBCOtpCurOv=L"0",TMMCUOvheatIndOn=L"0",BatTempSensFlt=L"0",BatDvcCur_G1=L"1.1",TerminalNO=L"7681100322375855700LB155601",InvVol=L"97",InvJctTemp=L"30",DFCMongHvNotIntd=L"0",DCDCInpVol=L"99.2",HVACOtpOpCirct=L"0",VehiStarWarning=L"0",Bat10sPlsChrgPowAvail=L"8",MCUCurFlt=L"0",BatExtVol=L"98.7",DrSbltAtcV=L"0",BatIntVol=L"98.6",TMNum_G=L"1",OBCOvTemp=L"0",VCURemtCtrlSpdLmtFb=L"0",SupCode=L"0",DFCTioutForVT=L"0",InvrCur=L"-1",Bat2sPlsChrgPowAvail=L"8",vehConfig=L"LV0",VecActGearSts=L"1",VehConf=L"3",BatMinTempPos=L"1",CellBattNum_21=L"21",BalanceSts=L"0",WhlGrndVlctyLftNnDrvn=L"4.78",VecChrgingSts=L"0",Bat10sPlsDischrgPowAvail=L"17.42",WhlGrndVlctyRtDrvn=L"4.92",SftwrMjrVsin=L"2",OBCInpVlt=L"0",RatedVol=L"96",PstnSts_G=L"0",PowBatCutOffIndOn=L"0",BatMaxTemp=L"20",EvpRlySta=L"0",TMTorq_G=L"-0.3",BatInfLen=L"0",TMActTemp=L"33",PlsAgainToRdy=L"0",BatChrgSts=L"0",HandBrkSts=L"0",InitaContIndOn_UCU=L"0",SOC_G=L"77",IslatRes_G=L"40409",objType=L"E50",BatSubsysTemp_G1=L"20_20_20_20_20",BatContiDischrgPowAvail=L"17.42",InitaContIndOn_MCU=L"0",PowBatErrIndOn=L"0",BatSOCSTS=L"0",VecIslateWrngIndOn=L"0",DCDCInternalTemp=L"35",DFCMCUGen1TiOut=L"0",MinTempSubSysNum_G=L"1",Bat2sPlsDischrgPowAvail=L"17.42",TMActTorq=L"-0.25",DCACopenreq=L"0",t0STime=L"2023-03-07 14:41:50",VehDrvMod=L"1",MCUCtrlFlt=L"0",BatChrgTims=L"139",BatManuCode=L"3158856",MESSAGETYPE=L"REALTIME",VacPumpWrngIndOn=L"0",TalNumOfClBatInThisFrm_G1=L"30",RqstFrActDischrg=L"0",DFCMongTqExcLimn=L"0",DFCDOPIN56Flt=L"0",BatOthFltList=L"0",TMFltNumN2_G=L"0",OBCLVSts=L"0",RemtHeatModRqst=L"0",InitaContIndOn_BMS=L"0" 1678171310000'
        #             ]

    def run(self):
        # self.test()
        # return
        if "smlChildTableName" in self.taospy_setting["spec"]["config"]:
            if self.taospy_setting["spec"]["config"]["smlChildTableName"].upper() == "ID":
                self.no_id_stb_exist_check()
                self.tag_col_binary_nchar_length_check()
                self.tag_col_add_dup_id_check()
                self.tag_col_add_check()
                self.tag_md5_check()
                self.tbname_tags_cols_name_check()
        else:
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
            self.tbname_check()
            self.tag_value_length_check()
            self.col_value_length_check()
            self.tag_col_illegal_value_check()
            self.duplicate_id_tag_col_insert_check()
            self.duplicate_insert_exist_check()
            self.tag_col_binary_max_length_check()
            self.tag_col_nchar_max_length_check()
            self.batch_insert_check()
            self.multi_insert_check(100)
            self.same_ts_batch_insert()
            self.batch_error_insert_check()
            self.multi_cols_insert_check()
            self.multi_tags_insert_check()
            self.blank_col_insert_check()
            self.blank_tag_insert_check()
            self.chinese_check()
            self.spell_check()
            self.default_type_check()
            self.tbname_tags_cols_name_check()
            self.stb_insert_multi_thread_check()
            self.s_stb_s_tb_d_data_insert_multi_thread_check()
            self.s_stb_s_tb_d_data_atc_insert_multi_thread_check()
            self.s_stb_stb_d_data_mtc_insert_multi_thread_check()
            self.s_stb_d_tb_d_data_insert_multi_thread_check()
            # TODO not stable
            # self.s_stb_d_tb_d_data_ac_mt_insert_multi_thread_check()
            self.s_stb_d_tb_d_data_at_mc_insert_multi_thread_check()
            self.s_stb_s_tb_d_data_d_ts_insert_multi_thread_check()
            self.s_stb_s_tb_d_data_d_ts_ac_mt_insert_multi_thread_check()
            self.s_stb_s_tb_d_data_d_ts_at_mc_insert_multi_thread_check()
            self.s_stb_d_tb_d_data_d_ts_insert_multi_thread_check()
            # TODO not stable
            # self.s_stb_d_tb_d_data_d_ts_ac_mt_insert_multi_thread_check()
            self.escape_test()

            self.ts_2828(10, 10, 3)
            self.ts_3053()
            self.ts_3146()
            self.ts_3116()
            self.ts_3264()
            self.ts_3262()

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
