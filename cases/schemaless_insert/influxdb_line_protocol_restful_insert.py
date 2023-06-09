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
from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.rest import TDRest
from taostest.util.remote import Remote
import sys
from taostest.components import TaosAdapter
class TestInfluxdbLineRestfulInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql, env_setting=self.env_setting)
        self.tdRest = TDRest(env_setting=self.env_setting)
        self.tdRest.drop_all_db()
        self.tdCom.sml_type = "influxdb_restful"
        self.dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname=self.dbname, precision="us")
        self._remote: Remote = Remote(self.logger)
        self.taosadapter = TaosAdapter(self._remote)
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosadapter":
                self.taosadapter_setting = env_setting
                self.endpoint = self.taosadapter_setting["spec"]["taos_config"]["firstEP"]
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting

    def init_check(self):
        """
        normal tags and cols, one for every elm
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)

    def bool_check(self):
        """
        check all normal type
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        full_type_list = ["f", "F", "false", "False", "t", "T", "true", "True"]
        for t_type in full_type_list:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c0=t_type, t0=t_type)
            self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)

    def symbols_check(self):
        """
            check symbols = `~!@#$%^&*()_-+={[}]\|:;'\",<.>/?
        """
        '''
            please test :
            binary_symbols = '\"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"\'\'"\"'
        '''
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        binary_symbols = '"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"'
        nchar_symbols = f'L{binary_symbols}'
        input_sql, stb_name = self.tdCom.gen_full_type_sql(c7=binary_symbols, c8=nchar_symbols, t7=binary_symbols, t8=nchar_symbols)
        self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)

    def iu_check(self):
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql = f'{self.tdCom.get_long_name()},t0=127 c1=9223372036854775807i,c2=1u 0'
        stb_name = input_sql.split(",")[0]
        self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdRest.request(f'select * from {self.dbname}.{stb_name}')
        self.tdRest.request(f'describe {self.dbname}.{stb_name}')
        self.tdSql.checkEqual(self.tdRest.resp["data"][1][1], "BIGINT")
        self.tdSql.checkEqual(self.tdRest.resp["data"][2][1], "BIGINT UNSIGNED")

    def id_seq_check(self):
        """
        check id.index in tags
        eg: t0=**,id=**,t1=**
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(id_change_tag=True)
        self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)

    def id_letter_check(self):
        """
        check id param
        eg: id and ID
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(id_upper_tag=True)
        self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(id_mixul_tag=True)
        self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(id_change_tag=True, id_upper_tag=True)
        self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)

    def no_id_check(self):
        """
        id not exist
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(id_noexist_tag=True)
        self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)
        query_sql = f"select tbname from {self.dbname}.{stb_name}"
        res_row_list = self.tdCom.restful_res_handle(query_sql, stb_name, dbname=self.dbname)[0]
        if len(res_row_list[0][0]) > 0:
            self.tdSql.checkEqual(res_row_list, res_row_list)
        else:
            self.tdSql.checkEqual(res_row_list, "please check no_id_check")

    def max_col_tag_check(self):
        """
        max tag count is 128
        max col count is 4096
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        for input_sql in [self.tdCom.gen_long_sql(self.tdCom.boundary_config["MAX_TAG_COUNT"], 1)[0], self.tdCom.gen_long_sql(1, self.tdCom.boundary_config["MAX_TAG_COL_COUNT"]-2)[0]]:
            self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
            self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        for input_sql in [self.tdCom.gen_long_sql(self.tdCom.boundary_config["MAX_TAG_COUNT"]+1, 1)[0], self.tdCom.gen_long_sql(1, self.tdCom.boundary_config["MAX_TAG_COL_COUNT"]-1)[0]]:
            self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
            res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)
            if "too many tags than 128" in res.text:
                self.tdSql.checkIn("too many tags than 128", res.text)
            else:
                self.tdSql.checkIn("too many columns than 4096", res.text)

    def stb_name_check(self):
        """
        test illegal id name
        mix "~!@#$¥%^&*()-+={}|[]、「」【】:;《》<>?"
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        rstr = list("~!@#$¥%^&*()-+|[]、「」【】;:《》<>?")
        for i in rstr:
            stb_name=f"aaa{i}bbb"
            input_sql = self.tdCom.gen_full_type_sql(stb_name=stb_name, tb_name=f'{stb_name}_sub')[0]
            self.tdCom.check_res(input_sql, f'`{stb_name}`', dbname=self.dbname)
            self.tdRest.restApiPost(f"drop table if exists {self.dbname}.`{stb_name}`")

    def id_start_with_num_check(self):
        """
        id is start with num
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name="1aaabbb")
        self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)

    def now_check(self):
        """
        check now unsupported
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql = self.tdCom.gen_full_type_sql(ts="now")[0]
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("invalid timestamp:now", res.text)

    def date_format_check(self):
        """
        check date format ts unsupported
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql = self.tdCom.gen_full_type_sql(ts="2021-07-21\ 19:01:46.920")[0]
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("invalid timestamp", res.text)

    def illegal_ts_check(self):
        """
        check ts format like 16260068336390us19
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql = self.tdCom.gen_full_type_sql(ts="16260068336390us19")[0]
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("invalid timestamp", res.text)

    def tbname_check(self):
        """
        check length 192
        check upper tbname
        chech upper tag
        length of stb_name tb_name <= 192
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        stb_name_192 = self.tdCom.get_long_name(length=self.tdCom.boundary_config["STBNAME_MAX_LENGTH"])
        tb_name_192 = self.tdCom.get_long_name(length=self.tdCom.boundary_config["TBNAME_MAX_LENGTH"])
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name_192, tb_name=tb_name_192)
        self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)
        self.tdRest.request(f'select * from {self.dbname}.{stb_name}')
        self.tdSql.checkEqual(self.tdRest.resp["rows"], 1)
        if self.tdCom.smlChildTableName_value == "ID":
            for input_sql in [self.tdCom.gen_full_type_sql(stb_name=self.tdCom.get_long_name(length=self.tdCom.boundary_config["STBNAME_MAX_LENGTH"]+1), tb_name=self.tdCom.get_long_name(length=5))[0], self.tdCom.gen_full_type_sql(tb_name=self.tdCom.get_long_name(length=self.tdCom.boundary_config["TBNAME_MAX_LENGTH"]))[0]]:
                res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
                self.tdSql.checkEqual(res.status_code, 500)
                self.tdSql.checkIn("measure is empty or too large than 192", res.text)
            input_sql = 'Abcdffgg,id=Abcddd,T1=127i8 c0=False 1626006833639000000'
        else:
            input_sql = self.tdCom.gen_full_type_sql(stb_name=self.tdCom.get_long_name(length=self.tdCom.boundary_config["STBNAME_MAX_LENGTH"]+1), tb_name=self.tdCom.get_long_name(length=5))[0]
            res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)
            self.tdSql.checkIn("measure is empty or too large than 192", res.text)
            input_sql = 'Abcdffgg,T1=127i8 c0=False 1626006833639000000'
        stb_name = f'`{input_sql.split(",")[0]}`'
        self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)
        self.tdRest.restApiPost(f'drop table {self.dbname}.`Abcdffgg`')

    def tag_value_length_check(self):
        """
        check full type tag value limit
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        # nchar
        # * legal nchar could not be larger than 16374/4
        stb_name = self.tdCom.get_long_name()
        input_sql = f'{stb_name},t0=t,t1={self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"])} c0=f 1626006833639000000'
        self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        input_sql = f'{stb_name},t0=t,t1={self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"]+1)} c0=f 1626006833639000000'
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("Invalid binary/nchar column/tag length", res.text)

    def col_value_length_check(self):
        """
        check full type col value limit
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        # i8
        for c1 in [f'-{self.tdCom.boundary_config["TINYINT_MAX"]}i8', f'{self.tdCom.boundary_config["TINYINT_MAX"]}i8']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c1=c1)
            self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)

        for c1 in [f'-{self.tdCom.boundary_config["TINYINT_MAX"]+2}i8', f'{self.tdCom.boundary_config["TINYINT_MAX"]+1}i8']:
            input_sql = self.tdCom.gen_full_type_sql(c1=c1)[0]
            res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)
            self.tdSql.checkIn("smlParseValue error", res.text)
        # i16
        for c2 in [f'-{self.tdCom.boundary_config["SMALLINT_MAX"]}i16']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c2=c2)
            self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)
        for c2 in [f'-{self.tdCom.boundary_config["SMALLINT_MAX"]+2}i16', f'{self.tdCom.boundary_config["SMALLINT_MAX"]+1}i16']:
            input_sql = self.tdCom.gen_full_type_sql(c2=c2)[0]
            res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)
            self.tdSql.checkIn("smlParseValue error", res.text)

        # i32
        for c3 in [f'-{self.tdCom.boundary_config["INT_MAX"]}i32']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c3=c3)
            self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)
        for c3 in [f'-{self.tdCom.boundary_config["INT_MAX"]+2}i32', f'{self.tdCom.boundary_config["INT_MAX"]+1}i32']:
            input_sql = self.tdCom.gen_full_type_sql(c3=c3)[0]
            res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)
            self.tdSql.checkIn("smlParseValue error", res.text)

        # i64
        for c4 in [f'-{self.tdCom.boundary_config["BIGINT_MAX"]}i64']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c4=c4)
            self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)
        for c4 in [f'-{self.tdCom.boundary_config["BIGINT_MAX"]+2}i64', f'{self.tdCom.boundary_config["BIGINT_MAX"]+1}i64']:
            input_sql = self.tdCom.gen_full_type_sql(c4=c4)[0]
            res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)
            self.tdSql.checkIn("smlParseValue error", res.text)

        # f64
        for c6 in [f'{-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)}f64', f'{-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)}f64']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(c6=c6)
            self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)
        # * limit set to 1.797693134862316*(10**308)
        # for c6 in [f'{-1.797693134862316*(10**308)}f64', f'{-1.797693134862316*(10**308)}f64']:
        #     input_sql = self.tdCom.gen_full_type_sql(c6=c6)[0]
        #     res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        #     self.tdSql.checkEqual(res.status_code, 500)
            # ! TD-17258
            # self.tdSql.checkIn("Invalid value in client", res.text)

        # # binary
        stb_name = self.tdCom.get_long_name()
        input_sql = f'{stb_name},t0=t c0=f,c1="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}" 1626006833639000000'
        self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        input_sql = f'{stb_name},t0=t c0=f,c1="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"]+1)}" 1626006833639000000'
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("smlParseValue error", res.text)

        # nchar
        # * legal nchar could not be larger than 16374/4
        stb_name = self.tdCom.get_long_name()
        input_sql = f'{stb_name},t0=t c0=f,c1=L"{self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"])}" 1626006833639000000'
        self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        input_sql = f'{stb_name},t0=t c0=f,c1=L"{self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"]+1)}" 1626006833639000000'
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("smlParseValue error", res.text)

    def tag_col_illegal_value_check(self):

        """
        test illegal tag col value
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        # bool
        for i in ["TrUe", "tRue", "trUe", "truE", "FalsE", "fAlse", "faLse", "falSe", "falsE"]:
            input_sql1, stb_name = self.tdCom.gen_full_type_sql(t0=i)
            self.tdCom.check_res(input_sql1, stb_name, dbname=self.dbname)
            input_sql2, stb_name = self.tdCom.gen_full_type_sql(c0=i)
            self.tdCom.check_res(input_sql2, stb_name, dbname=self.dbname)

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
            res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)
            self.tdSql.checkIn("smlParseValue error", res.text)

        # check binary and nchar blank
        stb_name = self.tdCom.get_long_name()
        input_sql1 = f'{stb_name}_1,t0=t c0=f,c1="abc aaa" 1626006833639000000'
        input_sql2 = f'{stb_name}_2,t0=t c0=f,c1=L"abc aaa" 1626006833639000000'
        input_sql3 = f'{stb_name}_3,t0=t,t1="abc\ aaa" c0=f 1626006833639000000'
        input_sql4 = f'{stb_name}_4,t0=t,t1=L"abc\ aaa" c0=f 1626006833639000000'
        for input_sql in [input_sql1, input_sql2, input_sql3, input_sql4]:
            res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 204)

        # check accepted binary and nchar symbols
        # # * ~!@#$¥%^&*()-+{}|[]、「」:;
        for symbol in list('~!@#$¥%^&*()-+{}|[]、「」:;'):
            self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
            input_sql1 = f'{stb_name}_5,t0=t c0=f,c1="abc{symbol}aaa" 1626006833639000000'
            input_sql2 = f'{stb_name}_6,t0=t,t1="abc{symbol}aaa" c0=f 1626006833639000000'
            self.tdCom.check_res(input_sql1, f'{stb_name}_5', dbname=self.dbname)
            self.tdCom.check_res(input_sql2, f'{stb_name}_6', dbname=self.dbname)

    def duplicate_id_tag_col_insert_Check(self):
        """
        check duplicate Id Tag Col
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql_id = self.tdCom.gen_full_type_sql(id_double_tag=True)[0]
        res = self.tdRest.schemalessApiPost(sql=input_sql_id, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("Cannot add duplicate keys to hash", res.text)

        input_sql = self.tdCom.gen_full_type_sql()[0]
        input_sql_tag = input_sql.replace("t5", "t6")
        res = self.tdRest.schemalessApiPost(sql=input_sql_tag, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("Cannot add duplicate keys to hash", res.text)

        input_sql = self.tdCom.gen_full_type_sql()[0]
        input_sql_col = input_sql.replace("c5", "c6")
        res = self.tdRest.schemalessApiPost(sql=input_sql_col, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("Cannot add duplicate keys to hash", res.text)

    ##### stb exist #####
    def duplicate_insert_exist_check(self):
        """
        check duplicate insert when stb exist
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name="duplicate")
        self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)
        self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)

    def tag_col_binary_nchar_length_increase_check(self):
        """
        check length increase
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)
        tb_name = self.tdCom.get_long_name(5)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name, t7="\"binaryTagValuebinaryTagValue\"", t8="L\"ncharTagValuencharTagValue\"", c7="\"binaryTagValuebinaryTagValue\"", c8="L\"ncharTagValuencharTagValue\"")
        self.tdCom.check_res(input_sql, stb_name, condition=f'where t7=\'"binaryTagValuebinaryTagValue"\'', dbname=self.dbname)

    # * tag binary max is 16384, col+ts binary max  49151
    def tag_col_binary_max_length_check(self):
        """
            every binary and nchar must be length+2
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        stb_name = self.tdCom.get_long_name()
        tb_name = f'{stb_name}_1'
        input_sql = f'{stb_name},id={tb_name},t0=t c0=f 1626006833639000000'
        self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)

        # # * check col，col+ts max in describe ---> 16143
        input_sql = f'{stb_name},t0=t c0=f,c1="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c2="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c3="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c4="{self.tdCom.get_long_name(12)}" 1626006833639000000'
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        print(res.text)
        self.tdSql.checkEqual(res.status_code, 204)

        self.tdRest.request(f"select * from {self.dbname}.{stb_name}")
        self.tdSql.checkEqual(self.tdRest.resp["rows"], 2)
        input_sql = f'{stb_name},t0=t c0=f,c1="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c2="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c3="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c4="{self.tdCom.get_long_name(13)}" 1626006833639000000'
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("Invalid operation", res.text)

        self.tdRest.request(f"select * from {self.dbname}.{stb_name}")
        self.tdSql.checkEqual(self.tdRest.resp["rows"], 2)

    # * tag nchar max is 16374/4, col+ts nchar max  49151
    def tag_col_nchar_max_length_check(self):
        """
        check nchar length limit
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        stb_name = self.tdCom.get_long_name()
        input_sql = f'{stb_name},t2=t c0=f 1626006833639000000'
        self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)

        # * legal nchar could not be larger than 16374/4
        input_sql = f'{stb_name},t1={self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"])},t2={self.tdCom.get_long_name(1)} c0=f 1626006833639000000'
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 204)
        self.tdRest.request(f"select * from {self.dbname}.{stb_name}")
        self.tdSql.checkEqual(self.tdRest.resp["rows"], 2)

        input_sql = f'{stb_name},t1={self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"])},t2={self.tdCom.get_long_name(2)} c0=f 1626006833639000000'
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("Invalid operation", res.text)

        self.tdRest.request(f"select * from {self.dbname}.{stb_name}")
        self.tdSql.checkEqual(self.tdRest.resp["rows"], 2)

        input_sql = f'{stb_name},t2=f c0=f,c1=L"{self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"])}",c2=L"{self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"])}",c3=L"{self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"])}",c4=L"{self.tdCom.get_long_name(4)}" 1626006833639000000'
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdRest.request(f"select * from {self.dbname}.{stb_name}")
        self.tdSql.checkEqual(self.tdRest.resp["rows"], 3)
        input_sql = f'{stb_name},t2={self.tdCom.get_long_name(1)} c0=f,c1=L"{self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"])}",c2=L"{self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"])}",c3=L"{self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"])}",c4=L"{self.tdCom.get_long_name(5)}" 1626006833639000000'
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("Invalid operation", res.text)

        self.tdRest.request(f"select * from {self.dbname}.{stb_name}")
        self.tdSql.checkEqual(self.tdRest.resp["rows"], 3)

    def batch_insert_check(self):
        """
        test batch insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        stb_name = self.tdCom.get_long_name()
        # self.tdRest.restApiPost(f'create stable {self.dbname}.{stb_name}(ts timestamp, f int) tags(t1 bigint)')
        lines = f'st123456,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000\n\
st123456,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000\n\
{stb_name},t2=5f64,t3=L\"ste\" c1=true,c2=4i64,c3=\"iam\" 1626056811823316532\n\
stf567890,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000\n\
st123456,t1=4i64,t2=5f64,t3=\"t4\" c1=3i64,c3=L\"passitagain\",c2=true,c4=5f64 1626006933640001000\n\
{stb_name},t2=5f64,t3=L\"ste2\" c3=\"iamszhou\",c4=false 1626056811843316532\n\
{stb_name},t2=5f64,t3=L\"ste2\" c3=\"iamszhou\",c4=false,c5=32i8,c6=64i16,c7=32i32,c8=88.88f32 1626056812843316532\n\
st123456,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000\n\
st123456,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64 1626006933641000000'
        res = self.tdRest.schemalessApiPost(sql=lines, precision="us", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 204)
        self.tdRest.request(f'select * from information_schema.ins_stables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdRest.resp["rows"], 3)
        self.tdRest.request(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdRest.resp["rows"], 6)
        self.tdRest.request(f'select * from {self.dbname}.st123456')
        self.tdSql.checkEqual(self.tdRest.resp["rows"], 5)

    def multi_insert_check(self, count):
        """
        test multi insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        long_sql = ''
        stb_name = self.tdCom.get_long_name(8)
        # self.tdRest.restApiPost(f'create stable {self.dbname}.{stb_name}(ts timestamp, f int) tags(t1 nchar(10))')
        for i in range(count):
            input_sql = self.tdCom.gen_full_type_sql(stb_name=stb_name, t7=f'"{self.tdCom.get_long_name(8)}"', c7=f'"{self.tdCom.get_long_name(8)}"', id_noexist_tag=True)[0]
            long_sql += f'{input_sql}\n'
        res = self.tdRest.schemalessApiPost(sql=long_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 204)
        self.tdRest.request(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdRest.resp["rows"], count)

    def batch_error_insert_check(self):
        """
        test batch error insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        stb_name = self.tdCom.get_long_name(8)
        lines = f'st123456,t1=3i64,t2=4f64,t3=\"t3\" c1=3i 64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000"\n\
                {stb_name},t2=5f64,t3=L\"ste\" c1=tRue,c2=4i64,c3=\"iam\" 1626056811823316532ns'
        res = self.tdRest.schemalessApiPost(sql=lines, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("nvalid timestamp", res.text)

    def multi_cols_insert_check(self):
        """
        test multi cols insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql = self.tdCom.gen_full_type_sql(c_multi_tag=True)[0]
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("nvalid timestamp", res.text)

    def multi_tags_insert_check(self):
        """
        test multi tags insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql = self.tdCom.gen_full_type_sql(t_multi_tag=True)[0]
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("nvalid timestamp", res.text)

    def blank_col_insert_check(self):
        """
        test blank col insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql = self.tdCom.gen_full_type_sql(c_blank_tag=True)[0]
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.checkIn("invalid key or key is too long", res.text)

    def blank_tag_insert_check(self):
        """
        test blank tag insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(t_blank_tag=True)
        self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdRest.request(f'select * from {self.dbname}.{stb_name}')
        self.tdSql.checkEqual(self.tdRest.resp["rows"], 1)

    def chinese_check(self):
        """
        check nchar ---> chinese
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(chinese_tag=True)
        self.tdCom.check_res(input_sql, stb_name, dbname=self.dbname)

    def db_auto_create_test(self):
        """
        check smlAutoCreateDB
        """
        self.dbname = "auto_create_default"
        input_sql = self.tdCom.gen_full_type_sql()[0]
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkIn("Database not exist", res.text)

        self.taosadapter_setting["spec"]["adapter_config"]["SMLAutoCreateDB"] = "false"
        self.taosadapter.update_taosa_toml(self.taosadapter_setting, self.endpoint.split(":")[0], True)
        self.dbname = "auto_create_false"
        input_sql = self.tdCom.gen_full_type_sql()[0]
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkIn("Database not exist", res.text)

        self.dbname = "auto_create_true"
        self.taosadapter_setting["spec"]["adapter_config"]["SMLAutoCreateDB"] = "true"
        self.taosadapter.update_taosa_toml(self.taosadapter_setting, self.endpoint.split(":")[0], True)
        input_sql = self.tdCom.gen_full_type_sql()[0]
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 204)
        self.tdRest.request("show databases")
        self.tdSql.checkIn([self.dbname], self.tdRest.resp["data"])

        self.taosadapter_setting["spec"]["adapter_config"]["SMLAutoCreateDB"] = "false"
        self.taosadapter.update_taosa_toml(self.taosadapter_setting, self.endpoint.split(":")[0], True)

    def ts_3349(self):
        """
        add case for TS-3349
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.dbname = "test"
        self.tdCom.createDb(dbname=self.dbname, precision="us")
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        stb_name = self.tdCom.get_long_name()
        tb_name = f'{stb_name}_1'
        input_sql = f'{stb_name},id={tb_name},t0=t c0=f 1626006833639000000'
        self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.execute(f'drop table {self.dbname}.{stb_name}')
        res = self.tdRest.schemalessApiPost(sql=input_sql, precision="ns", dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 204)
        self.tdSql.checkNotIn("Table does not exist", res.text)

    def run(self):
        self.init_check()
        self.bool_check()
        self.symbols_check()
        self.iu_check()
        self.id_seq_check()
        self.id_letter_check()
        self.no_id_check()
        self.max_col_tag_check()
        self.stb_name_check()
        self.id_start_with_num_check()
        self.now_check()
        self.date_format_check()
        self.illegal_ts_check()
        self.tbname_check()
        self.tag_value_length_check()
        self.col_value_length_check()
        self.tag_col_illegal_value_check()
        self.duplicate_id_tag_col_insert_Check()
        self.duplicate_insert_exist_check()
        self.tag_col_binary_nchar_length_increase_check()
        # self.tag_col_binary_max_length_check()
        # self.tag_col_nchar_max_length_check()
        self.batch_insert_check()
        self.multi_insert_check(100)
        self.batch_error_insert_check()
        self.multi_cols_insert_check()
        self.multi_tags_insert_check()
        self.blank_col_insert_check()
        self.blank_tag_insert_check()
        self.chinese_check()
        self.db_auto_create_test()
        self.ts_3349()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
            init_check()
            bool_check()
            symbols_check()
            iu_check()
            id_seq_check()
            id_letter_check()
            no_id_check()
            max_col_tag_check()
            stb_name_check()
            id_start_with_num_check()
            now_check()
            date_format_check()
            illegal_ts_check()
            tbname_check()
            tag_value_length_check()
            col_value_length_check()
            tag_col_illegal_value_check()
            duplicate_id_tag_col_insert_Check()
            duplicate_insert_exist_check()
            tag_col_binary_nchar_length_increase_check()
            tag_col_binary_max_length_check()
            tag_col_nchar_max_length_check()
            batch_insert_check()
            multi_insert_check(100)
            batch_error_insert_check()
            multi_cols_insert_check()
            multi_tags_insert_check()
            blank_col_insert_check()
            blank_tag_insert_check()
        '''
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.Schemaless.Restful.InfluxDB