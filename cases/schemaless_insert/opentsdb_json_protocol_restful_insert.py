from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.sml_types import TDSmlProtocolType, TDSmlTimestampType
from taos.error import SchemalessError
import datetime
import json

class TestOpentsdbJsonRestfulInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdCom.sml_type = "opentsdb_json_restful"
        self.tdCom.drop_all_db()
        self.dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname=self.dbname, precision="us")

    def init_check(self):
        """
        normal tags and cols, one for every elm
        """
        self.tdCom.cleanTb()
        input_json, stb_name = self.tdCom.gen_full_type_json()
        self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)

    def bool_check(self):
        """
        check all normal type
        """
        self.tdCom.cleanTb()
        full_type_list = ["f", "F", "false", "False", "t", "T", "true", "True"]
        for t_type in full_type_list:
            input_json_list = [self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t0_value=t_type))[0],
                                self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=t_type, t_type="bool"))[0]]
            for input_json in input_json_list:
                try:
                    self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                    raise Exception("should not reach here")
                except SchemalessError as err:
                    self.tdSql.checkNotEqual(err.errno, 0)

    def symbols_check(self):
        """
        check symbols = `~!@#$%^&*()_-+={[}]\|:;'\",<.>/?
        """
        """
        please test :
        binary_symbols = '\"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"\'\'"\"'
        """
        self.tdCom.cleanTb()
        binary_symbols = '"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"'
        nchar_symbols = binary_symbols
        input_json1, stb_name1 = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=binary_symbols, t_type="binary"),
                                    tag_value=self.tdCom.gen_tag_value(t7_value=binary_symbols, t8_value=nchar_symbols))
        input_json2, stb_name2 = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=nchar_symbols, t_type="nchar"), 
                                    tag_value=self.tdCom.gen_tag_value(t7_value=binary_symbols, t8_value=nchar_symbols))
        self.tdCom.check_res(input_json1, stb_name1, dbname=self.dbname)
        self.tdCom.check_res(input_json2, stb_name2, dbname=self.dbname)

    def max_col_tag_check(self):
        """
        max tag count is 128
        """
        for input_json in [self.tdCom.gen_long_json(128)[0]]:
            self.tdCom.cleanTb()
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        for input_json in [self.tdCom.gen_long_json(129)[0]]:
            self.tdCom.cleanTb()
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

    def stb_name_check(self):
        """
        test illegal id name
        mix "`~!@#$¥%^&*()-+{}|[]、「」【】:;《》<>?"
        """
        self.tdCom.cleanTb()
        rstr = list("`~!@#$¥%^&*()-+={}|[]、「」【】\:;《》<>?")
        for i in rstr:
            input_json = self.tdCom.gen_full_type_json(stb_name=f'`aa{i}bb`')[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

    def now_check(self):
        """
        check now unsupported
        """
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value="now", t_type="ns"))[0]
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def date_format_check(self):
        """
        check date format ts unsupported
        """
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value="2021-07-21\ 19:01:46.920", t_type="ns"))[0]
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

    def illegal_ts_check(self):
        """
        check ts format like 16260068336390us19
        """
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value="16260068336390us19", t_type="us"))[0]
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def tag_name_length_check(self):
        """
        check tag name limit <= 62
        """
        self.tdCom.cleanTb()
        tag_name = self.tdCom.get_long_name(62, "letters")
        stb_name = self.tdCom.get_long_name(7, "letters")
        input_json = {'metric': stb_name, 'timestamp': {'value': 1626006833639000000, 'type': 'ns'}, 'value': "bcdaaa", 'tags': {tag_name: {'value': False, 'type': 'bool'}}}
        self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        input_json = {'metric': stb_name, 'timestamp': {'value': 1626006833639000001, 'type': 'ns'}, 'value': "bcdaaaa", 'tags': {self.tdCom.get_long_name(65, "letters"): {'value': False, 'type': 'bool'}}}
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def tag_value_length_check(self):
        """
        check full type tag value limit
        """
        self.tdCom.cleanTb()
        # nchar
        # * legal nchar could not be larger than 16374/4
        stb_name = self.tdCom.get_long_name(7, "letters")
        # i8
        for t1 in [-127, 127]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t1_value=t1))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        for t1 in [-128, 128]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t1_value=t1))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        #i16
        for t2 in [-32767, 32767]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t2_value=t2))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        for t2 in [-32768, 32768]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t2_value=t2))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        #i32
        for t3 in [-2147483647, 2147483647]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t3_value=t3))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        for t3 in [-2147483648, 2147483648]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t3_value=t3))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        #i64
        for t4 in [-9223372036854775807, 9223372036854775807]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t4_value=t4))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)

        for t4 in [-9223372036854775808, 9223372036854775808]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t4_value=t4))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # f64
        for t6 in [-1.79769*(10**308), -1.79769*(10**308)]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t6_value=t6))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        for t6 in [float(-1.797693134862316*(10**308)), -1.797693134862316*(10**308)]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t6_value=t6))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # binary
        stb_name = self.tdCom.get_long_name(7, "letters")
        input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': self.tdCom.get_long_name(16374, "letters"), 'type': 'binary'}}}
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': self.tdCom.get_long_name(16375, "letters"), 'type': 'binary'}}}
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

        # # nchar
        # # * legal nchar could not be larger than 16374/4
        stb_name = self.tdCom.get_long_name(7, "letters")
        input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': self.tdCom.get_long_name(4093, "letters"), 'type': 'nchar'}}}
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

        input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': self.tdCom.get_long_name(4094, "letters"), 'type': 'nchar'}}}
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def col_value_length_check(self):
        """
        check full type col value limit
        """
        self.tdCom.cleanTb()
        # i8
        for value in [-127, 127]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="tinyint"))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        self.tdCom.cleanTb()
        for value in [-128, 128]:
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="tinyint"))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)
        # i16
        self.tdCom.cleanTb()
        for value in [-32767]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="smallint"))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        self.tdCom.cleanTb()
        for value in [-32768, 32768]:
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="smallint"))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # i32
        self.tdCom.cleanTb()
        for value in [-2147483647]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="int"))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        self.tdCom.cleanTb()
        for value in [-2147483648, 2147483648]:
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="int"))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # i64
        self.tdCom.cleanTb()
        for value in [-9223372036854775807]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="bigint"))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        self.tdCom.cleanTb()
        for value in [-9223372036854775808, 9223372036854775808]:
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="bigint"))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # f64
        self.tdCom.cleanTb()
        for value in [-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308), -1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="double"))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        # * limit set to 1.797693134862316*(10**308)
        self.tdCom.cleanTb()
        for value in [-1.797693134862316*(10**308), -1.797693134862316*(10**308)]:
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="double"))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # binary
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name(7, "letters")
        input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': self.tdCom.get_long_name(16374, "letters"), 'type': 'binary'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

        self.tdCom.cleanTb()
        input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': self.tdCom.get_long_name(16375, "letters"), 'type': 'binary'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

        # nchar
        # * legal nchar could not be larger than 16374/4
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name(7, "letters")
        input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': self.tdCom.get_long_name(4093, "letters"), 'type': 'nchar'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

        self.tdCom.cleanTb()
        input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': self.tdCom.get_long_name(4094, "letters"), 'type': 'nchar'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
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
            try:
                input_json1 = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t0_value=i))[0]
                self.tdSql._conn.schemaless_insert([json.dumps(input_json1)], 2, None)
                input_json2 = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=i, t_type="bool"))[0]
                self.tdSql._conn.schemaless_insert([json.dumps(input_json2)], 2, None)
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # i8 i16 i32 i64 f32 f64
        for input_json in [
                self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t1_value="1s2"))[0],
                self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t2_value="1s2"))[0],
                self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t3_value="1s2"))[0],
                self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t4_value="1s2"))[0],
                self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t5_value="11.1s45"))[0],
                self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t6_value="11.1s45"))[0],
            ]:
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # check binary and nchar blank
        input_sql1 = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value="abc aaa", t_type="binary"))[0]
        input_sql2 = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value="abc aaa", t_type="nchar"))[0]
        input_sql3 = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t7_value="abc aaa"))[0]
        input_sql4 = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t8_value="abc aaa"))[0]
        for input_json in [input_sql1, input_sql2, input_sql3, input_sql4]:
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # check accepted binary and nchar symbols
        # # * ~!@#$¥%^&*()-+={}|[]、「」:;
        for symbol in list('~!@#$¥%^&*()-+={}|[]、「」:;'):
            input_json1 = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=f"abc{symbol}aaa", t_type="binary"))[0]
            input_json2 = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t8_value=f"abc{symbol}aaa"))[0]
            self.tdSql._conn.schemaless_insert([json.dumps(input_json1)], TDSmlProtocolType.JSON.value, None)
            self.tdSql._conn.schemaless_insert([json.dumps(input_json2)], TDSmlProtocolType.JSON.value, None)

    ##### stb exist #####
    def duplicate_insert_exist_check(self):
        """
        check duplicate insert when stb exist
        """
        self.tdCom.cleanTb()
        input_json, stb_name = self.tdCom.gen_full_type_json()
        self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)

    def tag_col_binary_nchar_length_increase_check(self):
        """
        check length increase
        """
        self.tdCom.cleanTb()
        input_json, stb_name = self.tdCom.gen_full_type_json()
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        tb_name = self.tdCom.get_long_name(5, "letters")
        input_json, stb_name = self.tdCom.gen_full_type_json(stb_name=stb_name, tb_name=tb_name, tag_value=self.tdCom.gen_tag_value(t7_value="binaryTagValuebinaryTagValue", t8_value="ncharTagValuencharTagValue"))
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdCom.check_res(input_json, stb_name, condition=f'where t7="binaryTagValuebinaryTagValue"', dbname=self.dbname)

    # * tag nchar max is 16374/4, col+ts nchar max  49151
    def tag_col_binary_max_length_check(self):
        """
        check nchar length limit
        """
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name(7, "letters")
        tag_value = {"t0": {"value": True, "type": "bool"}}
        col_value=self.tdCom.gen_ts_col_value(value=True, t_type="bool")
        input_json = {"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": col_value, "tags": tag_value}
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

        # * every binary and nchar must be length+2, so here is two tag, max length could not larger than 16384-2*2
        tag_value["t1"] = {"value": self.tdCom.get_long_name(16374, "letters"), "type": "binary"}
        tag_value["t2"] = {"value": self.tdCom.get_long_name(5, "letters"), "type": "binary"}
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        tag_value["t2"] = {"value": self.tdCom.get_long_name(6, "letters"), "type": "binary"}
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)
        self.tdSql.query(f"select * from {self.dbname}.{stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    # * tag nchar max is 16374/4, col+ts nchar max  49151
    def tag_col_nchar_max_length_check(self):
        """
        check nchar length limit
        """
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name(7, "letters")
        tag_value = {"t0": True}
        col_value= True
        input_json = {"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": col_value, "tags": tag_value}
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

        # * legal nchar could not be larger than 16374/4
        tag_value["t1"] = {"value": self.tdCom.get_long_name(4093, "letters"), "type": "nchar"}
        tag_value["t2"] = {"value": self.tdCom.get_long_name(1, "letters"), "type": "nchar"}
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        tag_value["t2"] = {"value": self.tdCom.get_long_name(2, "letters"), "type": "binary"}
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def batch_insert_check(self):
        """
        test batch insert
        """
        self.tdCom.cleanTb()
        stb_name = "stb_name"
        self.tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 bigint)')
        input_json = [{"metric": "st123456", "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": 1, "tags": {"t1": 3, "t2": {"value": 4, "type": "double"}, "t3": {"value": "t3", "type": "binary"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006833640000000, "type": "ns"}, "value": 2, "tags": {"t1": {"value": 4, "type": "double"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}},
                    {"metric": "stb_name", "timestamp": {"value": 1626056811823316532, "type": "ns"}, "value": 3, "tags": {"t2": {"value": 5, "type": "double"}, "t3": {"value": "ste", "type": "nchar"}}},
                    {"metric": "stf567890", "timestamp": {"value": 1626006933640000000, "type": "ns"}, "value": 4, "tags": {"t1": {"value": 4, "type": "bigint"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006833642000000, "type": "ns"}, "value": {"value": 5, "type": "double"}, "tags": {"t1": {"value": 4, "type": "double"}, "t2": 5.0, "t3": {"value": "t4", "type": "binary"}}},
                    {"metric": "stb_name", "timestamp": {"value": 1626056811843316532, "type": "ns"}, "value": {"value": 6, "type": "double"}, "tags": {"t2": 5.0, "t3": {"value": "ste2", "type": "nchar"}}},
                    {"metric": "stb_name", "timestamp": {"value": 1626056812843316532, "type": "ns"}, "value": {"value": 7, "type": "double"}, "tags": {"t2": {"value": 5, "type": "double"}, "t3": {"value": "ste2", "type": "nchar"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006933640000000, "type": "ns"}, "value": {"value": 8, "type": "double"}, "tags": {"t1": {"value": 4, "type": "double"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006933641000000, "type": "ns"}, "value": {"value": 9, "type": "double"}, "tags": {"t1": 4, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}}]
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdSql.query(f'show {self.dbname}.stables')
        self.tdSql.checkEqual(self.tdSql.query_row, 3)
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(self.tdSql.query_row, 6)
        self.tdSql.query(f'select * from {self.dbname}.st123456')
        self.tdSql.checkEqual(self.tdSql.query_row, 5)

    def multi_insert_check(self, count):
        """
        test multi insert
        """
        self.tdCom.cleanTb()
        sql_list = list()
        stb_name = self.tdCom.get_long_name(8, "letters")
        self.tdSql.execute(f'create stable {self.dbname}.{stb_name}(ts timestamp, f int) tags(t1 bigint)')
        for i in range(count):
            input_json = self.tdCom.gen_full_type_json(stb_name=stb_name, col_value=self.tdCom.gen_ts_col_value(value=self.tdCom.get_long_name(8, "letters"), t_type="binary"), tag_value=self.tdCom.gen_tag_value(t7_value=self.tdCom.get_long_name(8, "letters")), id_noexist_tag=True)[0]
            sql_list.append(input_json)
        self.tdSql._conn.schemaless_insert([json.dumps(sql_list)], TDSmlProtocolType.JSON.value, None)
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(self.tdSql.query_row, count)

    def batch_error_insert_check(self):
        """
        test batch error insert
        """
        self.tdCom.cleanTb()
        input_json = [{"metric": "st123456", "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": {"value": "tt", "type": "bool"}, "tags": {"t1": {"value": 3, "type": "bigint"}, "t2": {"value": 4, "type": "double"}, "t3": {"value": "t3", "type": "binary"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006933641000000, "type": "ns"}, "value": {"value": 9, "type": "bigint"}, "tags": {"t1": {"value": 4, "type": "bigint"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}}]
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def multi_cols_insert_check(self):
        """
        test multi cols insert
        """
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(c_multi_tag=True)[0]
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def blank_col_insert_check(self):
        """
        test blank col insert
        """
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(c_blank_tag=True)[0]
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def blank_tag_insert_check(self):
        """
        test blank tag insert
        """
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(t_blank_tag=True)[0]
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def chinese_check(self):
        """
        check nchar ---> chinese
        """
        self.tdCom.cleanTb()
        input_json, stb_name = self.tdCom.gen_full_type_json(chinese_tag=True)
        self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)

    def multi_field_check(self):
        '''
        multi_field
        '''
        self.tdCom.cleanTb()
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(multi_field_tag=True)[0]
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def point_trans_check(self):
        """
        metric value "." trans to "_"
        """
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(point_trans_tag=True)[0]
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdSql.execute(f"drop table {self.dbname}.`.point.trans.test`")

    def tbname_tags_cols_name_check(self):
        self.tdCom.cleanTb()
        input_json = {'metric': 'rFa$sta', 'timestamp': {'value': 1626006834, 'type': 's'}, 'value': {'value': True, 'type': 'bool'}, 'tags': {'Tt!0': {'value': False, 'type': 'bool'}, 'tT@1': {'value': 127, 'type': 'tinyint'}, 't@2': {'value': 32767, 'type': 'smallint'}, 't$3': {'value': 2147483647, 'type': 'int'}, 't%4': {'value': 9223372036854775807, 'type': 'bigint'}, 't^5': {'value': 11.12345027923584, 'type': 'float'}, 't&6': {'value': 22.123456789, 'type': 'double'}, 't*7': {'value': 'binaryTagValue', 'type': 'binary'}, 't!@#$%^&*()_+[];:<>?,9': {'value': 'ncharTagValue', 'type': 'nchar'}}}
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        query_sql = f'select * from {self.dbname}.`rFa$sta`'
        self.tdSql.query(query_sql)
        self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2021, 7, 11, 20, 33, 54), True, 'ncharTagValue', 2147483647, 9223372036854775807, 22.123456789, 'binaryTagValue', 32767, 11.12345027923584, False, 127)])
        self.tdSql.query(f'describe {self.dbname}.`rFa$sta`')
        self.tdSql.checkEqual(self.tdSql.getColNameList(), ['ts', 'value', 't!@#$%^&*()_+[];:<>?,9', 't$3', 't%4', 't&6', 't*7', 't@2', 't^5', 'Tt!0', 'tT@1'])
        self.tdSql.execute(f'drop table {self.dbname}.`rFa$sta`')


    def run(self) -> bool:
        self.init_check()
        self.symbols_check()
        self.max_col_tag_check()
        self.now_check()
        self.date_format_check()
        self.illegal_ts_check()
        self.tag_value_length_check()
        self.col_value_length_check()
        self.tag_col_illegal_value_check()
        self.tag_col_binary_nchar_length_increase_check()
        self.tag_col_binary_max_length_check()
        self.tag_col_nchar_max_length_check()
        self.batch_insert_check()
        self.multi_insert_check(10)
        self.multi_cols_insert_check()
        self.blank_col_insert_check()
        self.blank_tag_insert_check()
        self.multi_field_check()
        self.point_trans_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
            init_check()
            symbols_check()
            max_col_tag_check()
            now_check()
            date_format_check()
            illegal_ts_check()
            tag_value_length_check()
            col_value_length_check()
            tag_col_illegal_value_check()
            tag_col_binary_nchar_length_increase_check()
            tag_col_binary_max_length_check()
            tag_col_nchar_max_length_check()
            batch_insert_check()
            multi_insert_check(10)
            multi_cols_insert_check()
            blank_col_insert_check()
            blank_tag_insert_check()
            multi_field_check()
            point_trans_check()
        '''
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.Schemaless.Restful.OpenTsDBJson
