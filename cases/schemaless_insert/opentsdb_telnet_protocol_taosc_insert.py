from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.sml_types import TDSmlProtocolType, TDSmlTimestampType
from taos.error import SchemalessError
import datetime
import sys
from taostest.util.remote import Remote

class TestOpentsdbTelnetLineTaoscInsert(TDCase):
    def init(self):
        # self.tdCom = TDCom(self.tdSql, env_setting=self.env_setting)
        self._remote: Remote = Remote(self.logger)
        self.tdCom = TDCom(self.tdSql)
        self.tdCom.env_setting = self.env_setting
        self.tdCom.sml_type = "opentsdb_telnet"
        self.tdCom.drop_all_db()
        self.dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname=self.dbname, precision="us")
        self.taospy_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taospy")
        if "smlChildTableName" in self.taospy_setting["spec"]["config"]:
            if self.taospy_setting["spec"]["config"]["smlChildTableName"].upper() == "ID":
                self.tdCom.set_sml_specified_value()

    # def smlPass(func):
    #     def wrapper(self, *args):
    #         if self.tdCom.smlChildTableName_value is not None:
    #             if self.tdCom.smlChildTableName_value.upper() == "ID":
    #                 return func(self, *args)
    #         else:
    #             pass
    #     return wrapper

    def init_check(self):
        """
        normal tags and cols, one for every elm
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)

    def bool_check(self):
        """
        check all normal type
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        full_type_list = ["f", "F", "false", "False", "t", "T", "true", "True"]
        for t_type in full_type_list:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(t0=t_type)
            self.tdCom.check_res(input_sql, stb_name)

    def symbols_check(self):
        """
        check symbols = `~!@#$%^&*()_-+={[}]\|:;'\",<.>/?
        """
        """
        please test :
        binary_symbols = '\"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"\'\'"\"'
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        binary_symbols = '"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"'
        nchar_symbols = f'L{binary_symbols}'
        input_sql1, stb_name1 = self.tdCom.gen_full_type_sql(value=binary_symbols, t7=binary_symbols, t8=nchar_symbols)
        input_sql2, stb_name2 = self.tdCom.gen_full_type_sql(value=nchar_symbols, t7=binary_symbols, t8=nchar_symbols)
        self.tdCom.check_res(input_sql1, stb_name1)
        self.tdCom.check_res(input_sql2, stb_name2)

    def ts_check(self):
        """
        test ts list --> ["1626006833640ms", "1626006834s", "1626006822639022"]
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(ts=1626006833640)
        self.tdCom.check_res(input_sql, stb_name, ts_type=TDSmlTimestampType.MILLI_SECOND.value)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(ts=1626006833640)
        self.tdCom.check_res(input_sql, stb_name, ts_type=None)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(ts=1626006834)
        self.tdCom.check_res(input_sql, stb_name, ts_type=TDSmlTimestampType.SECOND.value)

        self.tdSql.execute(f"drop database if exists test_ts")
        kv_dict = {"precision": "ms"}
        self.tdCom.createDb("test_ts", **kv_dict)
        input_sql = ['test_ms 1626006833640 t t0=t', 'test_ms 1626006833641 f t0=t']
        self.tdSql._conn.schemaless_insert(input_sql, TDSmlProtocolType.TELNET.value, None)
        self.tdSql.query('select * from test_ms')
        self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), "2021-07-11 20:33:53.640000")
        self.tdSql.checkEqual(str(self.tdSql.query_data[1][0]), "2021-07-11 20:33:53.641000")

    def opentstb_telnet_ts_check(self):
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql = f'{self.tdCom.get_long_name()} 0 127 t0=127 t1=32767I16 t2=2147483647I32 t3=9223372036854775807 t4=11.12345027923584F32 t5=22.123456789F64'
        print(input_sql)
        stb_name = input_sql.split(" ")[0]
        self.tdCom.check_res(input_sql, stb_name, ts=0)
        input_sql = f'{self.tdCom.get_long_name()} 1626006833640 127 t0=127 t1=32767I16 t2=2147483647I32 t3=9223372036854775807 t4=11.12345027923584F32 t5=22.123456789F64'
        stb_name = input_sql.split(" ")[0]
        self.tdCom.check_res(input_sql, stb_name, ts_type=TDSmlTimestampType.MILLI_SECOND.value)
        input_sql = f'{self.tdCom.get_long_name()} 1626006834 127 t0=127 t1=32767I16 t2=2147483647I32 t3=9223372036854775807 t4=11.12345027923584F32 t5=22.123456789F64'
        stb_name = input_sql.split(" ")[0]
        self.tdCom.check_res(input_sql, stb_name, ts_type=TDSmlTimestampType.SECOND.value)
        for ts in [1, 12, 123, 1234, 12345, 123456, 1234567, 12345678, 162600683, 16260068341, 162600683412, 16260068336401]:
            try:
                input_sql = f'{self.tdCom.get_long_name()} {ts} 127 t0=127 t1=32767I16 t2=2147483647I32 t3=9223372036854775807 t4=11.12345027923584F32 t5=22.123456789F64'
                self.tdSql._conn.schemaless_insert(input_sql, TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

    def id_seq_check(self):
        """
        check id.index in tags
        eg: t0=**,id=**,t1=**
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(id_change_tag=True)
        self.tdCom.check_res(input_sql, stb_name)

    def id_letter_check(self):
        """
        check id param
        eg: id and ID
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
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
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
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
        """
        for input_sql in [self.tdCom.gen_long_sql(self.tdCom.boundary_config["MAX_TAG_COUNT"], 1)[0]]:
            self.tdCom.cleanTb()
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        for input_sql in [self.tdCom.gen_long_sql(self.tdCom.boundary_config["MAX_TAG_COUNT"]+1, 1)[0]]:
            self.tdCom.cleanTb()
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

    def stb_tb_name_check(self):
        """
        test illegal id name
        mix "`~!@#$¥%^&*()-+{}|[]、「」【】:;《》<>?"
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        rstr = list("~!@#$¥%^&*()-+{}|[]、「」【】:;《》<>?")
        for i in rstr:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=f"\"aaa{i}bbb\"")
            self.tdCom.check_res(input_sql, f'`{stb_name}`')
            self.tdSql.execute(f'drop table if exists `{stb_name}`')

    def id_start_with_num_check(self):
        """
        id is start with num
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name="1aaabbb")
        self.tdCom.check_res(input_sql, stb_name)

    def now_check(self):
        """
        check now unsupported
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(ts="now")[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def date_format_check(self):
        """
        check date format ts unsupported
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(ts="2021-07-21\ 19:01:46.920")[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def illegal_ts_check(self):
        """
        check ts format like 16260068336390us19
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(ts="16260068336390us19")[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
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
        stb_name_192 = self.tdCom.get_long_name(length=self.tdCom.boundary_config["STBNAME_MAX_LENGTH"])
        tb_name_192 = self.tdCom.get_long_name(length=self.tdCom.boundary_config["TBNAME_MAX_LENGTH"])
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name_192, tb_name=tb_name_192)
        self.tdCom.check_res(input_sql, stb_name)
        self.tdSql.query(f'select * from {stb_name}')
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        if self.tdCom.smlChildTableName_value == "ID":
            #for input_sql in [self.tdCom.gen_full_type_sql(stb_name=self.tdCom.get_long_name(length=self.tdCom.boundary_config["STBNAME_MAX_LENGTH"]+1), tb_name=self.tdCom.get_long_name(length=5))[0], self.tdCom.gen_full_type_sql(tb_name=self.tdCom.get_long_name(length=self.tdCom.boundary_config["TBNAME_MAX_LENGTH"]+1))[0]]:
            for input_sql in [self.tdCom.gen_full_type_sql(stb_name=self.tdCom.get_long_name(length=self.tdCom.boundary_config["STBNAME_MAX_LENGTH"]+1))]:
                try:
                    self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                    raise Exception("should not reach here")
                except SchemalessError as err:
                    self.tdSql.checkNotEqual(err.errno, 0)
            input_sql = 'Abcdffgg 1626006833640 False T1=127i8 id=Abcddd'
        else:
            # input_sql = self.tdCom.gen_full_type_sql(stb_name=self.tdCom.get_long_name(length=self.tdCom.boundary_config["STBNAME_MAX_LENGTH"]+1), tb_name=self.tdCom.get_long_name(length=5))[0]
            input_sql = self.tdCom.gen_full_type_sql(stb_name=self.tdCom.get_long_name(length=self.tdCom.boundary_config["STBNAME_MAX_LENGTH"]+1))[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)
            input_sql = 'Abcdffgg 1626006833640 False T1=127i8'
        stb_name = f'`{input_sql.split(" ")[0]}`'
        self.tdCom.check_res(input_sql, stb_name)
        self.tdSql.execute('drop table `Abcdffgg`')

    def tag_value_length_check(self):
        """
        check full type tag value limit
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name()
        legal_length = int(len(self.tdCom.get_long_name(self.tdCom.boundary_config["TAG_COLUMN_MAX_LENGTH"]))/4)
        input_sql = f'{stb_name} 1626006833640 t t1={self.tdCom.get_long_name(legal_length)}'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        input_sql = f'{stb_name} 1626006833640 t t1={self.tdCom.get_long_name(legal_length+1)}'
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def col_value_length_check(self):
        """
        check full type col value limit
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        # i8
        self._remote._logger.info(f' Running i8---- {sys._getframe().f_code.co_name}()')
        for value in [f'-{self.tdCom.boundary_config["TINYINT_MAX"]}i8', f'{self.tdCom.boundary_config["TINYINT_MAX"]}i8']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(value=value)
            self.tdCom.check_res(input_sql, stb_name)
        self.tdCom.cleanTb()
        for value in [f'-{self.tdCom.boundary_config["TINYINT_MAX"]+2}i8', f'{self.tdCom.boundary_config["TINYINT_MAX"]+1}i8']:
            input_sql = self.tdCom.gen_full_type_sql(value=value)[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)
        # i16
        self._remote._logger.info(f' Running i16---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        for value in [f'-{self.tdCom.boundary_config["SMALLINT_MAX"]}i16']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(value=value)
            self.tdCom.check_res(input_sql, stb_name)
        self.tdCom.cleanTb()
        for value in [f'-{self.tdCom.boundary_config["SMALLINT_MAX"]+2}i16', f'{self.tdCom.boundary_config["SMALLINT_MAX"]+1}i16']:
            input_sql = self.tdCom.gen_full_type_sql(value=value)[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # i32
        self._remote._logger.info(f' Running i32---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        for value in [f'-{self.tdCom.boundary_config["INT_MAX"]}i32']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(value=value)
            self.tdCom.check_res(input_sql, stb_name)
        self.tdCom.cleanTb()
        for value in [f'-{self.tdCom.boundary_config["INT_MAX"]+2}i32', f'{self.tdCom.boundary_config["INT_MAX"]+1}i32']:
            input_sql = self.tdCom.gen_full_type_sql(value=value)[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # i64
        self.tdCom.cleanTb()
        self._remote._logger.info(f' Running i64---- {sys._getframe().f_code.co_name}()')
        for value in [f'-{self.tdCom.boundary_config["BIGINT_MAX"]}i64']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(value=value)
            self.tdCom.check_res(input_sql, stb_name)
        self.tdCom.cleanTb()
        for value in [f'-{self.tdCom.boundary_config["BIGINT_MAX"]+2}i64', f'{self.tdCom.boundary_config["BIGINT_MAX"]+1}i64']:
            input_sql = self.tdCom.gen_full_type_sql(value=value)[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # f32
        self.tdCom.cleanTb()
        self._remote._logger.info(f' Running f32---- {sys._getframe().f_code.co_name}()')
        for value in [f"{-3.4028234663852885981170418348451692544*(10**38)}f32", f"{3.4028234663852885981170418348451692544*(10**38)}f32"]:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(value=value)
            self.tdCom.check_res(input_sql, stb_name)
        # * limit set to 4028234664*(10**38)
        self.tdCom.cleanTb()
        for value in [f"{-3.4028234664*(10**38)}f32", f"{3.4028234664*(10**38)}f32"]:
            input_sql = self.tdCom.gen_full_type_sql(value=value)[0]
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # f64

        self._remote._logger.info(f' Running f64---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        for value in [f'{-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)}f64', f'{-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)}f64']:
            input_sql, stb_name = self.tdCom.gen_full_type_sql(value=value)
            self.tdCom.check_res(input_sql, stb_name)
        # * limit set to 1.797693134862316*(10**308)
        # self.tdCom.cleanTb()
        # for value in [f'{-1.797693134862316*(10**308)}f64', f'{-1.797693134862316*(10**308)}f64']:
        #     input_sql = self.tdCom.gen_full_type_sql(value=value)[0]
        #     try:
        #         self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        #         raise Exception("should not reach here")
        #     except SchemalessError as err:
        #         self.tdSql.checkNotEqual(err.errno, 0)

        # binary
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name()
        input_sql = f'{stb_name} 1626006833640 "{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}" t0=t'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)

        self.tdCom.cleanTb()
        input_sql = f'{stb_name} 1626006833640 "{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"]+1)}" t0=t'
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

        # nchar
        # * legal nchar could not be larger than 16374/4
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name()
        input_sql = f'{stb_name} 1626006833640 L"{self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"])}" t0=t'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)

        self.tdCom.cleanTb()
        input_sql = f'{stb_name} 1626006833640 L"{self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"]+1)}" t0=t'
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def tag_col_illegal_value_check(self):
        """
        test illegal tag col value
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        # bool
        for i in ["TrUe", "tRue", "trUe", "truE", "FalsE", "fAlse", "faLse", "falSe", "falsE"]:
            input_sql1, stb_name = self.tdCom.gen_full_type_sql(t0=i)
            self.tdCom.check_res(input_sql1, stb_name)
            input_sql2, stb_name = self.tdCom.gen_full_type_sql(value=i)
            self.tdCom.check_res(input_sql2, stb_name)

        # i8 i16 i32 i64 f32 f64
        for input_sql in [
                self.tdCom.gen_full_type_sql(value="1s2i8")[0],
                self.tdCom.gen_full_type_sql(value="1s2i16")[0],
                self.tdCom.gen_full_type_sql(value="1s2i32")[0],
                self.tdCom.gen_full_type_sql(value="1s2i64")[0],
                self.tdCom.gen_full_type_sql(value="11.1s45f32")[0],
                self.tdCom.gen_full_type_sql(value="11.1s45f64")[0],
            ]:
            try:
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # check accepted binary and nchar symbols
        # # * ~!@#$¥%^&*()-+={}|[]、「」:;
        for symbol in list('、「」~!@#$¥%^&*()-+={}|[]:;'):
            input_sql1 = f'{self.tdCom.get_long_name()} 1626006833640 "abc{symbol}aaa" t0=t'
            input_sql2 = f'{self.tdCom.get_long_name()} 1626006833640 t t0=t t1="abc{symbol}aaa"'
            self.tdSql._conn.schemaless_insert([input_sql1], TDSmlProtocolType.TELNET.value, None)
            # self.tdSql._conn.schemaless_insert([input_sql2], TDSmlProtocolType.TELNET.value, None)

    def blank_check(self):
        """
        check blank case
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        # input_sql_list = [f'{self.tdCom.get_long_name()}   1626006833640 "abc\ aaa" t0=t',
        #                 f'{self.tdCom.get_long_name()} 1626006833640   t t0="abaaa"',
        #                 f'{self.tdCom.get_long_name()} 1626006833640 t   t0=L"abaaa"',
        #                 f'{self.tdCom.get_long_name()}  1626006833640   L"aba\ aa"   t0=L"abcaaa3"   ']
        input_sql_list = [f'{self.tdCom.get_long_name()} 1626006833640   t t0="abaaa"',
                        f'{self.tdCom.get_long_name()} 1626006833640 t   t0=L"abaaa"']
        for input_sql in input_sql_list:
            stb_name = input_sql.split(" ")[0]
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            self.tdSql.query(f'select * from {stb_name}')
            self.tdSql.checkEqual(self.tdSql.query_row, 1)

    def duplicate_id_tag_col_insert_check(self):
        """
        check duplicate Id Tag Col
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql_id = self.tdCom.gen_full_type_sql(id_double_tag=True)[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql_id], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

        input_sql = self.tdCom.gen_full_type_sql()[0]
        input_sql_tag = input_sql.replace("t5", "t6")
        try:
            self.tdSql._conn.schemaless_insert([input_sql_tag], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    ##### stb exist #####
    def no_id_stb_exist_check(self):
        """
        case no id when stb exist
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name="sub_table_0123456", t0="f", value="f")
        self.tdCom.check_res(input_sql, stb_name)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, id_noexist_tag=True, t0="f", value="f")
        self.tdCom.check_res(input_sql, stb_name, condition='where tbname like "t_%"')
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def duplicate_insert_exist_check(self):
        """
        check duplicate insert when stb exist
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        self.tdCom.check_res(input_sql, stb_name)

    def tag_col_binary_nchar_length_check(self):
        """
        check length increase
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name,t7="\"binaryTagValuebinaryTagValue\"", t8="L\"ncharTagValuencharTagValue\"")
        self.tdCom.check_res(input_sql, stb_name, condition=f'where tbname like "{tb_name}"')

    def tag_col_add_dup_id_check(self):
        """
        check tag count add, stb and tb duplicate
        * tag: alter table ...
        * tag&&value will be added,
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name, t0="t", value="t")
        self.tdCom.check_res(input_sql, stb_name)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name, t0="t", value="f", t_add_tag=True)
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        self.tdSql.query(f'select * from {stb_name} where tbname like "{tb_name}"')
        self.tdSql.checkData(0, 1, False)
        self.tdSql.checkData(0, 11, None)
        self.tdSql.checkData(0, 12, None)

    def tag_col_add_check(self):
        """
        check tag count add
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name, t0="f", value="f")
        self.tdCom.check_res(input_sql, stb_name)
        tb_name_1 = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, tb_name=tb_name_1, t0="f", value="f", t_add_tag=True)
        self.tdCom.check_res(input_sql, stb_name, condition=f'where tbname like "{tb_name_1}"')
        res_row_list = self.tdCom.res_handle(f"select t10,t11 from {tb_name}", stb_name)[0]
        self.tdSql.checkEqual(res_row_list[0], ['None', 'None'])
        self.tdCom.check_res(input_sql, stb_name, condition=f'where tbname like "{tb_name}"', none_check_tag=True)

    def tag_md5_check(self):
        """
        condition: stb not change
        insert two table, keep tag unchange, change col
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(t0="f", value="f", id_noexist_tag=True)
        self.tdCom.check_res(input_sql, stb_name)
        tb_name1 = self.tdCom.get_no_id_tbname(stb_name)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, t0="f", value="f", id_noexist_tag=True)
        self.tdCom.check_res(input_sql, stb_name)
        tb_name2 = self.tdCom.get_no_id_tbname(stb_name)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        self.tdSql.checkEqual(tb_name1, tb_name2)
        input_sql, stb_name = self.tdCom.gen_full_type_sql(stb_name=stb_name, t0="f", value="f", id_noexist_tag=True, t_add_tag=True)
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        tb_name3 = self.tdCom.get_no_id_tbname(stb_name)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        self.tdSql.checkNotEqual(tb_name1, tb_name3)

    def tag_max_length_check(self):
        """
        check tag length limit
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name()
        input_sql = f'{stb_name} 1626006833640 f t2={self.tdCom.get_long_name(1)}'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        legal_length = int(len(self.tdCom.get_long_name(self.tdCom.boundary_config["TAG_COLUMN_MAX_LENGTH"]))/4)
        input_sql = f'{stb_name} 1626006833640 f t1={self.tdCom.get_long_name(legal_length-2)} t2={self.tdCom.get_long_name(1)}'
        self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        input_sql = f'{stb_name} 1626006833640 f t1={self.tdCom.get_long_name(legal_length-1)} t2={self.tdCom.get_long_name(1)}'
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def batch_insert_check(self):
        """
        test batch insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.createDb(dbname=self.dbname, precision="us")
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name()
        # ts = self.tdCom.genTs("us")[0]
        # self.tdSql.execute(f'create stable {self.dbname}.{stb_name}(ts timestamp, f int) tags(t1 bigint)')
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
        self.tdSql._conn.schemaless_insert(lines, TDSmlProtocolType.TELNET.value, None)
        self.tdSql.query(f'select * from information_schema.ins_stables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 3)
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 6)
        self.tdSql.query('select * from st123456')
        self.tdSql.checkEqual(self.tdSql.query_row, 5)

    def multiInsert_check(self, count):
        """
        test multi insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.createDb(dbname=self.dbname, precision="us")
        self.tdCom.cleanTb()
        sql_list = []
        stb_name = self.tdCom.get_long_name()
        # self.tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 nchar(10))')
        for i in range(count):
            input_sql = self.tdCom.gen_full_type_sql(stb_name=stb_name, t7=f'"{self.tdCom.get_long_name()}"', value=f'"{self.tdCom.get_long_name()}"', id_noexist_tag=True)[0]
            sql_list.append(input_sql)
        print(sql_list)
        self.tdSql._conn.schemaless_insert(sql_list, TDSmlProtocolType.TELNET.value, None)
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, count)

    def batch_error_insert_check(self):
        """
        test batch error insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name()
        lines = ["st123456 1626006833640 3i 64 t1=3i64 t2=4f64 t3=\"t3\"",
                f"{stb_name} 1626056811823316532ns tRue t2=5f64 t3=L\"ste\""]
        try:
            self.tdSql._conn.schemaless_insert(lines, TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def multi_cols_insert_check(self):
        """
        test multi cols insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(c_multi_tag=True)[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def blank_col_insert_check(self):
        """
        test blank col insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(c_blank_tag=True)[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def blank_tag_insert_check(self):
        """
        test blank tag insert
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(t_blank_tag=True)[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def chinese_check(self):
        """
        check nchar ---> chinese
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(chinese_tag=True)
        self.tdCom.check_res(input_sql, stb_name)

    def multi_field_check(self):
        '''
        multi_field
        '''
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(multi_field_tag=True)[0]
        try:
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def spell_check(self):
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name()
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
            self.tdCom.check_res(input_sql, stb_name)

    def point_trans_check(self):
        """
        metric value "." trans to "_"
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(point_trans_tag=True)[0]
        if self.tdCom.sml_type == 'telnet-tcp':
            stb_name = f'`{input_sql.split(" ")[1]}`'
        else:
            stb_name = f'`{input_sql.split(" ")[0]}`'
        self.tdCom.check_res(input_sql, stb_name)
        self.tdSql.execute("drop table `.point.trans.test`")

    def defaultType_check(self):
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name()
        input_sql_list = [f'{stb_name}_1 1626006833640 9223372036854775807 t0=f t1=127 t2=32767i16 t3=2147483647i32 t4=9223372036854775807 t5=11.12345f32 t6=22.123456789f64 t7="vozamcts" t8=L"ncharTagValue"', \
                        f'{stb_name}_2 1626006833641 22.123456789 t0=f t1=127i8 t2=32767I16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789 t7="vozamcts" t8=L"ncharTagValue"', \
                        f'{stb_name}_3 1626006833642 10e5F32 t0=f t1=127i8 t2=32767I16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=10e5F64 t7="vozamcts" t8=L"ncharTagValue"', \
                        f'{stb_name}_4 1626006833643 10.0e5F64 t0=f t1=127i8 t2=32767I16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=10.0e5F32 t7="vozamcts" t8=L"ncharTagValue"', \
                        f'{stb_name}_5 1626006833644 -10.0e5 t0=f t1=127i8 t2=32767I16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=-10.0e5 t7="vozamcts" t8=L"ncharTagValue"']
        for input_sql in input_sql_list:
            stb_name = input_sql.split(" ")[0]
            self.tdCom.check_res(input_sql, stb_name)

    def tbname_tags_cols_name_check(self):
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        if "smlChildTableName" in self.taospy_setting["spec"]["config"]:
            if self.tdCom.smlChildTableName_value.upper() == "ID":
                input_sql = 'rFa$sta 1626006834 9223372036854775807 id=rFas$ta_1 Tt!0=true tT@1=127Ii8 t#2=32767i16 "t$3"=2147483647i32 t%4=9223372036854775807i64 t^5=11.12345f32 t&6=22.123456789f64 t*7=\"ddzhiksj\" t!@#$%^&*()_+[];:<>?,9=L\"ncharTagValue\"'
                self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
                query_sql = 'select * from `rFa$sta`'
                self.tdSql.query(query_sql)
                self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2021, 7, 11, 20, 33, 54), 9.223372036854776e+18, 'true', '127Ii8', '32767i16', '2147483647i32', '9223372036854775807i64', '11.12345f32', '22.123456789f64', '"ddzhiksj"', 'L"ncharTagValue"')])
                query_sql = 'describe `rFa$sta`'
                self.tdSql.query(query_sql)
                self.tdSql.checkEqual(self.tdSql.getColNameList(), ['_ts', '_value', 'Tt!0', 'tT@1', 't#2', '"t$3"', 't%4', 't^5', 't&6', 't*7', 't!@#$%^&*()_+[];:<>?,9'])
                self.tdSql.execute('drop table `rFa$sta`')
        else:
            input_sql = 'rFa$sta 1626006834 9223372036854775807 Tt!0=true tT@1=127Ii8 t#2=32767i16 "t$3"=2147483647i32 t%4=9223372036854775807i64 t^5=11.12345f32 t&6=22.123456789f64 t*7=\"ddzhiksj\" t!@#$%^&*()_+[];:<>?,9=L\"ncharTagValue\"'
            self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.TELNET.value, None)
            query_sql = 'select * from `rFa$sta`'
            self.tdSql.query(query_sql)
            self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2021, 7, 11, 20, 33, 54), 9.223372036854776e+18, 'true', '127Ii8', '32767i16', '2147483647i32', '9223372036854775807i64', '11.12345f32', '22.123456789f64', '"ddzhiksj"', 'L"ncharTagValue"')])
            query_sql = 'describe `rFa$sta`'
            self.tdSql.query(query_sql)
            self.tdSql.checkEqual(self.tdSql.getColNameList(), ['_ts', '_value', 'Tt!0', 'tT@1', 't#2', '"t$3"', 't%4', 't^5', 't&6', 't*7', 't!@#$%^&*()_+[];:<>?,9'])
            self.tdSql.execute('drop table `rFa$sta`')

    def stb_insert_multi_thread_check(self):
        """
        thread input different stb
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_sql_list()[0]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(input_sql))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 5)

    def tcp_keywords_check(self, protocol="telnet-tcp"):
        """
        stb = "put"
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql = self.tdCom.gen_full_type_sql(tcp_keyword_tag=True)[0]
        stb_name = f'`{input_sql.split(" ")[1]}`'
        self.tdCom.check_res(input_sql, stb_name)

    def s_stb_s_tb_d_data_insert_multi_thread_check(self):
        """
        thread input same stb tb, different data, result keep first data
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_list = self.tdCom.gen_sql_list(stb_name=stb_name, tb_name=tb_name)[1]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        if self.tdCom.smlChildTableName_value == "ID":
            expected_tb_name = self.tdCom.get_no_id_tbname(stb_name)[0]
            self.tdSql.checkEqual(tb_name, expected_tb_name)
        self.tdSql.query(f"select * from {stb_name};")
        self.tdSql.checkEqual(self.tdSql.query_row, 1)

    def s_stb_s_tb_d_data_at_insert_multi_thread_check(self):
        """
        thread input same stb tb, different data, add tags,  result keep first data
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_a_tag_list = self.tdCom.gen_sql_list(stb_name=stb_name, tb_name=tb_name)[2]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_a_tag_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        if self.tdCom.smlChildTableName_value == "ID":
            expected_tb_name = self.tdCom.get_no_id_tbname(stb_name)[0]
            self.tdSql.checkEqual(tb_name, expected_tb_name)
        self.tdSql.query(f"select * from {stb_name};")
        self.tdSql.checkEqual(self.tdSql.query_row, 1)

    def s_stb_stb_d_data_mt_insert_multi_thread_check(self):
        """
        thread input same stb tb, different data, minus columes and tags,  result keep first data
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name)
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_m_tag_list = self.tdCom.gen_sql_list(stb_name=stb_name, tb_name=tb_name)[3]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_m_tag_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        if self.tdCom.smlChildTableName_value == "ID":
            expected_tb_name = self.tdCom.get_no_id_tbname(stb_name)[0]
            self.tdSql.checkEqual(tb_name, expected_tb_name)
        self.tdSql.query(f"select * from {stb_name};")
        self.tdSql.checkEqual(self.tdSql.query_row, 1)

    def s_stb_d_tb_d_data_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, different data
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_d_tb_list = self.tdCom.gen_sql_list(stb_name=stb_name)[4]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_d_tb_d_data_mt_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, different data, mul tag
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(value="\"binaryTagValue\"")
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_d_tb_m_tag_list = [(f'{stb_name} 1626006833640 "omfdhyom" t0=F t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'yzwswz'),  \
                                (f'{stb_name} 1626006833640 "vqowydbc" t0=F t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'yzwswz'),  \
                                (f'{stb_name} 1626006833640 "plgkckpv" t0=F t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'yzwswz'),  \
                                (f'{stb_name} 1626006833640 "cujyqvlj" t0=F t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'yzwswz'),  \
                                (f'{stb_name} 1626006833640 "twjxisat" t0=T t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'yzwswz')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_m_tag_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 3)

    def s_stb_d_tb_d_data_at_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, different data, add tag
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_d_tb_a_tag_list = self.tdCom.gen_sql_list(stb_name=stb_name)[6]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_a_tag_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_s_tb_d_data_d_ts_insert_multi_thread_check(self):
        """
        thread input same stb tb, different ts
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name, value="\"binaryTagValue\"")
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_d_ts_list = [(f'{stb_name} 1626006833645 "hkgjiwdj" id={tb_name} t0=f t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="vozamcts" t8=L"ncharTagValue"', 'dwpthv'), \
                                (f'{stb_name} 1626006833641 "rljjrrul" id={tb_name} t0=False t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="bmcanhbs" t8=L"ncharTagValue"', 'dwpthv'), \
                                (f'{stb_name} 1626006833642 "basajzpp" id={tb_name} t0=F t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="bavaegjk" t8=L"ncharTagValue"', 'dwpthv'), \
                                (f'{stb_name} 1626006833643 "clsajzpp" id={tb_name} t0=F t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="eivaegjk" t8=L"ncharTagValue"', 'dwpthv'), \
                                (f'{stb_name} 1626006833644 "jitwseso" id={tb_name} t0=T t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="yhlwkddq" t8=L"ncharTagValue"', 'dwpthv')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_d_ts_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 6)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_s_tb_d_data_d_ts_mt_insert_multi_thread_check(self):
        """
        thread input same stb tb, different ts,  tag
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name, value="T")
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_d_ts_m_tag_list = [(f'{stb_name} 1626006833642 T t0=t t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="binaryTagValue" t8=L"ncharTagValue"', 'lakfkr'), 
                                    (f'{stb_name} 1626006833643 False t0=t t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="binaryTagValue" t8=L"ncharTagValue"', 'lakfkr'), 
                                    (f'{stb_name} 1626006833644 f t0=t t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="binaryTagValue" t8=L"ncharTagValue"', 'lakfkr'), 
                                    (f'{stb_name} 1626006833645 FALSE t0=t t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="binaryTagValue" t8=L"ncharTagValue"', 'lakfkr'), 
                                    (f'{stb_name} 1626006833646 TRUE t0=t t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="binaryTagValue" t8=L"ncharTagValue"', 'lakfkr')]
        # s_stb_s_tb_d_ts_m_tag_list = self.tdCom.gen_sql_list(stb_name=stb_name, tb_name=tb_name)[8]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_d_ts_m_tag_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 6)
        self.tdSql.query(f"select * from {stb_name} where id is not NULL")
        self.tdSql.checkEqual(self.tdSql.query_row, 1)

    def s_stb_s_tb_d_data_d_ts_at_insert_multi_thread_check(self):
        """
        thread input same stb tb, different ts, add tag
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(tb_name=tb_name, value="\"binaryTagValue\"")
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_s_tb_d_ts_a_tag_list = [(f'{stb_name} 1626006833641 "clummqfy" id={tb_name} t0=False t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="hpxzrdiw" t8=L"ncharTagValue" t11=127i8 t10=L"ncharTagValue"', 'bokaxl'), \
                                    (f'{stb_name} 1626006833642 "yqeztggb" id={tb_name} t0=False t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="hpxzrdiw" t8=L"ncharTagValue" t11=127i8 t10=L"ncharTagValue"', 'bokaxl'), \
                                    (f'{stb_name} 1626006833643 "gbkinqdk" id={tb_name} t0=False t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="hpxzrdiw" t8=L"ncharTagValue" t11=127i8 t10=L"ncharTagValue"', 'bokaxl'), \
                                    (f'{stb_name} 1626006833644 "ldxxejbd" id={tb_name} t0=False t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="hpxzrdiw" t8=L"ncharTagValue" t11=127i8 t10=L"ncharTagValue"', 'bokaxl'), \
                                    (f'{stb_name} 1626006833645 "tlvzwjes" id={tb_name} t0=False t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7="hpxzrdiw" t8=L"ncharTagValue" t11=127i8 t10=L"ncharTagValue"', 'bokaxl')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_d_ts_a_tag_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 6)
        for t in ["t10", "t11"]:
            self.tdSql.query(f"select * from {stb_name} where {t} is not NULL;")
            self.tdSql.checkEqual(self.tdSql.query_row, 5)

    def s_stb_d_tb_d_data_d_ts_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, data, ts
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql()
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_d_tb_d_ts_list = self.tdCom.gen_sql_list(stb_name=stb_name)[10]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_d_ts_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_d_tb_d_data_d_ts_mt_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, data, ts, mul tag
        """
        self._remote._logger.info(f' Running ---- {sys._getframe().f_code.co_name}()')
        self.tdCom.cleanTb()
        input_sql, stb_name = self.tdCom.gen_full_type_sql(value="\"binaryTagValue\"")
        self.tdCom.check_res(input_sql, stb_name)
        s_stb_d_tb_d_ts_m_tag_list = [(f'{stb_name} 1626006833642 "mnpmtzul" t0=False t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'pcppkg'), \
                                    (f'{stb_name} 1626006833643 "zbvwckcd" t0=False t1=126i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'pcppkg'), \
                                    (f'{stb_name} 1626006833644 "vymcjfwc" t0=False t1=125i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'pcppkg'), \
                                    (f'{stb_name} 1626006833645 "laumkwfn" t0=False t1=124i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'pcppkg'), \
                                    (f'{stb_name} 1626006833646 "nyultzxr" t0=false t1=123i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64', 'pcppkg')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_d_ts_m_tag_list))
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def test(self):
        self.tbname_tags_cols_name_check()
        return

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
            self.opentstb_telnet_ts_check()
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
            self.blank_check()
            self.duplicate_id_tag_col_insert_check()
            self.duplicate_insert_exist_check()
            self.tag_max_length_check()
            self.batch_insert_check()
            self.multiInsert_check(100)
            self.batch_error_insert_check()
            self.multi_cols_insert_check()
            self.blank_col_insert_check()
            self.blank_tag_insert_check()
            self.chinese_check()
            self.multi_field_check()
            self.spell_check()
            self.point_trans_check()
            self.defaultType_check()
            self.tbname_tags_cols_name_check()
            self.stb_insert_multi_thread_check()
            self.s_stb_s_tb_d_data_insert_multi_thread_check()
            self.s_stb_s_tb_d_data_at_insert_multi_thread_check()
            self.s_stb_stb_d_data_mt_insert_multi_thread_check()
            self.s_stb_d_tb_d_data_insert_multi_thread_check()
            self.s_stb_d_tb_d_data_mt_insert_multi_thread_check()
            self.s_stb_d_tb_d_data_at_insert_multi_thread_check()
            self.s_stb_s_tb_d_data_d_ts_insert_multi_thread_check()
            self.s_stb_s_tb_d_data_d_ts_mt_insert_multi_thread_check()
            self.s_stb_s_tb_d_data_d_ts_at_insert_multi_thread_check()
            self.s_stb_d_tb_d_data_d_ts_insert_multi_thread_check()
            self.s_stb_d_tb_d_data_d_ts_mt_insert_multi_thread_check()

            self.tdCom = TDCom(self.tdSql, env_setting=self.env_setting)
            self.tdCom.sml_type = "telnet-tcp"
            for env_setting in self.env_setting["settings"]:
                if env_setting["name"] == "taosAdapter":
                    self.dbname = env_setting["spec"]["adapter_config"]["opentsdb_telnet"]["dbs"][0]
            self.tdCom.createDb(dbname=self.dbname, precision="us")
            self.init_check()
            self.bool_check()
            self.symbols_check()
            self.id_seq_check()
            self.id_letter_check()
            self.no_id_check()
            self.stb_tb_name_check()
            self.id_start_with_num_check()
            self.point_trans_check()
            self.tcp_keywords_check()
    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
            init_check()
            bool_check()
            symbols_check()
            ts_check()
            opentstb_telnet_ts_check()
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
            blank_check()
            duplicate_id_tag_col_insert_check()
            no_id_stb_exist_check()
            duplicate_insert_exist_check()
            tag_col_binary_nchar_length_check()
            tag_col_add_dup_id_check()
            tag_col_add_check()
            tag_md5_check()
            tag_col_binary_max_length_check()
            batch_insert_check()
            multiInsert_check(10)
            batch_error_insert_check()
            multi_cols_insert_check()
            blank_col_insert_check()
            blank_tag_insert_check()
            chinese_check()
            multi_field_check()
            spell_check()
            point_trans_check()
            defaultType_check()
            tbname_tags_cols_name_check()
            stb_insert_multi_thread_check()
            s_stb_s_tb_d_data_insert_multi_thread_check()
            s_stb_s_tb_d_data_at_insert_multi_thread_check()
            s_stb_stb_d_data_mt_insert_multi_thread_check()
            s_stb_d_tb_d_data_insert_multi_thread_check()
            s_stb_d_tb_d_data_mt_insert_multi_thread_check()
            s_stb_d_tb_d_data_at_insert_multi_thread_check()
            s_stb_s_tb_d_data_d_ts_insert_multi_thread_check()
            s_stb_s_tb_d_data_d_ts_mt_insert_multi_thread_check()
            s_stb_s_tb_d_data_d_ts_at_insert_multi_thread_check()
            s_stb_d_tb_d_data_d_ts_insert_multi_thread_check()
            s_stb_d_tb_d_data_d_ts_mt_insert_multi_thread_check()
        '''
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.Schemaless.Taosc.OpenTsDBTelnet, T.Write.Schemaless.Restful.OpenTsDBTelnetTCP



    