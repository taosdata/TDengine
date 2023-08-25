from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.rest import TDRest
from taos.error import SchemalessError
import datetime
import json

class TestOpentsdbJsonRestfulInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql, env_setting=self.env_setting)
        self.tdCom.sml_type = "opentsdb_json_restful"
        self.tdRest = TDRest(env_setting=self.env_setting)
        self.tdRest.drop_all_db()
        self.dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname=self.dbname, precision="us")

    def init_check(self):
        """
        normal tags and cols, one for every elm
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_json, stb_name = self.tdCom.gen_full_type_json()
        self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)

    def bool_check(self):
        """
        check all normal type
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        full_type_list = ["f", "F", "false", "False", "t", "T", "true", "True"]
        for t_type in full_type_list:
            input_json_list = [self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t0_value=t_type))[0],
                                self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=t_type, t_type="bool"))[0]]
            for input_json in input_json_list:
                res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
                self.tdSql.checkEqual(res.status_code, 500)

    def symbols_check(self):
        """
        check symbols = `~!@#$%^&*()_-+={[}]\|:;'\",<.>/?
        """
        """
        please test :
        binary_symbols = '\"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal"\'\'"\"'
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
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
        for input_json in [self.tdCom.gen_long_json(self.tdCom.boundary_config["MAX_TAG_COUNT"])[0]]:
            self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
            self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        for input_json in [self.tdCom.gen_long_json(self.tdCom.boundary_config["MAX_TAG_COUNT"]+1)[0]]:
            self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
            res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)

    def stb_name_check(self):
        """
        test illegal id name
        mix "`~!@#$¥%^&*()-+{}|[]、「」【】:;《》<>?"
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        rstr = list("`~!@#$¥%^&*()-+={}|[]、「」【】\:;《》<>?")
        for i in rstr:
            input_json = self.tdCom.gen_full_type_json(stb_name=f'`aa{i}bb`')[0]
            res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)

    def now_check(self):
        """
        check now unsupported
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_json = self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value="now", t_type="ns"))[0]
        res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)

    def date_format_check(self):
        """
        check date format ts unsupported
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_json = self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value="2021-07-21\ 19:01:46.920", t_type="ns"))[0]
        res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)

    def illegal_ts_check(self):
        """
        check ts format like 16260068336390us19
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_json = self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value="16260068336390us19", t_type="us"))[0]
        res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)

    def tag_name_length_check(self):
        """
        check tag name limit <= 64
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        tag_name = self.tdCom.get_long_name(self.tdCom.boundary_config["TAG_KEY_MAX_LENGTH"])
        stb_name = self.tdCom.get_long_name()
        input_json = {'metric': stb_name, 'timestamp': {'value': 1626006833639000000, 'type': 'ns'}, 'value': "bcdaaa", 'tags': {tag_name: {'value': False, 'type': 'bool'}}}
        self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        input_json = {'metric': stb_name, 'timestamp': {'value': 1626006833639000001, 'type': 'ns'}, 'value': "bcdaaaa", 'tags': {self.tdCom.get_long_name(self.tdCom.boundary_config["TAG_KEY_MAX_LENGTH"]+1): {'value': False, 'type': 'bool'}}}
        res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)

    def tag_value_length_check(self):
        """
        check full type tag value limit
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        # nchar
        # * legal nchar could not be larger than 16374/4
        stb_name = self.tdCom.get_long_name()
        # i8
        for t1 in [-self.tdCom.boundary_config["TINYINT_MAX"]-1, self.tdCom.boundary_config["TINYINT_MAX"]]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t1_value=t1))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        for t1 in [-self.tdCom.boundary_config["TINYINT_MAX"]-2, self.tdCom.boundary_config["TINYINT_MAX"]+1]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t1_value=t1))[0]
            res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)

        #i16
        for t2 in [-self.tdCom.boundary_config["SMALLINT_MAX"]-1, self.tdCom.boundary_config["SMALLINT_MAX"]]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t2_value=t2))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        for t2 in [-self.tdCom.boundary_config["SMALLINT_MAX"]-2, self.tdCom.boundary_config["SMALLINT_MAX"]+1]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t2_value=t2))[0]
            res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)

        #i32
        for t3 in [-self.tdCom.boundary_config["INT_MAX"]-1, self.tdCom.boundary_config["INT_MAX"]]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t3_value=t3))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        for t3 in [-self.tdCom.boundary_config["INT_MAX"]-2, self.tdCom.boundary_config["INT_MAX"]+1]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t3_value=t3))[0]
            res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)

        #i64
        for t4 in [-self.tdCom.boundary_config["BIGINT_MAX"]-1, self.tdCom.boundary_config["BIGINT_MAX"]]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t4_value=t4))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        # json truncate
        # for t4 in [-self.tdCom.boundary_config["BIGINT_MAX"]-2, self.tdCom.boundary_config["BIGINT_MAX"]+1]:
        #     res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            # self.tdSql.checkEqual(res.status_code, 500)

        # f64
        for t6 in [-1.79769*(10**308), -1.79769*(10**308)]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t6_value=t6))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        for t6 in [float(-1.797693134862316*(10**308)), -1.797693134862316*(10**308)]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t6_value=t6))[0]
            res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)

        # ! bug
        # # binary
        # stb_name = self.tdCom.get_long_name()
        # input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"]), 'type': 'binary'}}}
        # self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        # input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"]+1), 'type': 'binary'}}}
        # try:
        #     self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        #     raise Exception("should not reach here")
        # except SchemalessError as err:
        #     self.tdSql.checkNotEqual(err.errno, 0)

        # # # nchar
        # # # * legal nchar could not be larger than 16374/4
        # stb_name = self.tdCom.get_long_name()
        # input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"]), 'type': 'nchar'}}}
        # self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)

        # input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"]+1), 'type': 'nchar'}}}
        # try:
        #     self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        #     raise Exception("should not reach here")
        # except SchemalessError as err:
        #     self.tdSql.checkNotEqual(err.errno, 0)

    def col_value_length_check(self):
        """
        check full type col value limit
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        # i8
        for value in [-self.tdCom.boundary_config["TINYINT_MAX"]-1]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="tinyint"))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        for value in [-self.tdCom.boundary_config["TINYINT_MAX"]-2, self.tdCom.boundary_config["TINYINT_MAX"]+1]:
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="tinyint"))[0]
            res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)
        # i16
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        for value in [-self.tdCom.boundary_config["SMALLINT_MAX"]-1]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="smallint"))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        for value in [-self.tdCom.boundary_config["SMALLINT_MAX"]-2, self.tdCom.boundary_config["SMALLINT_MAX"]+1]:
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="smallint"))[0]
            res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)

        # i32
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        for value in [-self.tdCom.boundary_config["INT_MAX"]-1]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="int"))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        for value in [-self.tdCom.boundary_config["INT_MAX"]-2, self.tdCom.boundary_config["INT_MAX"]+1]:
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="int"))[0]
            res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)

        # i64
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        for value in [-self.tdCom.boundary_config["BIGINT_MAX"]-1]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="bigint"))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        # for value in [-self.tdCom.boundary_config["BIGINT_MAX"]-2, self.tdCom.boundary_config["BIGINT_MAX"]+1]:
        #     input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="bigint"))[0]
        #     try:
        #         self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        #         raise Exception("should not reach here")
        #     except SchemalessError as err:
        #         self.tdSql.checkNotEqual(err.errno, 0)

        # f64
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        for value in [-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308), -1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="double"))
            self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        # * limit set to 1.797693134862316*(10**308)
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        for value in [-1.797693134862316*(10**308), -1.797693134862316*(10**308)]:
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="double"))[0]
            res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)

        # # binary
        # self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        # stb_name = self.tdCom.get_long_name()
        # input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"]), 'type': 'binary'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        # self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)

        # self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        # input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"]+1), 'type': 'binary'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        # try:
        #     self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        #     raise Exception("should not reach here")
        # except SchemalessError as err:
        #     self.tdSql.checkNotEqual(err.errno, 0)

        # # nchar
        # # * legal nchar could not be larger than 16374/4
        # self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        # stb_name = self.tdCom.get_long_name()
        # input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"]), 'type': 'nchar'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        # self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)

        # self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        # input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"]+1), 'type': 'nchar'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        # try:
        #     self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        #     raise Exception("should not reach here")
        # except SchemalessError as err:
        #     self.tdSql.checkNotEqual(err.errno, 0)

    def tag_col_illegal_value_check(self):
        """
        test illegal tag col value
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        # bool
        for i in ["TrUe", "tRue", "trUe", "truE", "FalsE", "fAlse", "faLse", "falSe", "falsE"]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t0_value=i))[0]
            res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=i, t_type="bool"))[0]
            res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)

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
                self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # check binary and nchar blank
        input_sql1 = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value="abc aaa", t_type="binary"))[0]
        input_sql2 = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value="abc aaa", t_type="nchar"))[0]
        input_sql3 = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t7_value="abc aaa"))[0]
        input_sql4 = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t8_value="abc aaa"))[0]
        for input_json in [input_sql1, input_sql2, input_sql3, input_sql4]:
            res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)

        # check accepted binary and nchar symbols
        # # * ~!@#$¥%^&*()-+={}|[]、「」:;
        for symbol in list('~!@#$¥%^&*()-+={}|[]、「」:;'):
            input_json1 = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=f"abc{symbol}aaa", t_type="binary"))[0]
            input_json2 = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t8_value=f"abc{symbol}aaa"))[0]
            res = self.tdRest.schemalessApiPost(input_json1, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)
            res = self.tdRest.schemalessApiPost(input_json2, url_type="json", precision=None, dbname=self.dbname)
            self.tdSql.checkEqual(res.status_code, 500)

    ##### stb exist #####
    def duplicate_insert_exist_check(self):
        """
        check duplicate insert when stb exist
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_json, stb_name = self.tdCom.gen_full_type_json()
        self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)

    def tag_col_binary_nchar_length_increase_check(self):
        """
        check length increase
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_json, stb_name = self.tdCom.gen_full_type_json()
        self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)
        tb_name = self.tdCom.get_long_name(5)
        input_json, stb_name = self.tdCom.gen_full_type_json(stb_name=stb_name, tb_name=tb_name, tag_value=self.tdCom.gen_tag_value(t7_value="binaryTagValuebinaryTagValue", t8_value="ncharTagValuencharTagValue"))
        self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdCom.check_res(input_json, stb_name, condition=f'where t7="binaryTagValuebinaryTagValue"', dbname=self.dbname)

    # * tag nchar max is 16374/4, col+ts nchar max  49151
    def tag_col_binary_max_length_check(self):
        """
        check nchar length limit
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        stb_name = self.tdCom.get_long_name()
        tag_value = {"t0": {"value": True, "type": "bool"}}
        col_value=self.tdCom.gen_ts_col_value(value=True, t_type="bool")
        input_json = {"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": col_value, "tags": tag_value}
        self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)

        # * every binary and nchar must be length+2, so here is two tag, max length could not larger than 16384-2*2
        tag_value["t1"] = {"value": self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"]), "type": "binary"}
        tag_value["t2"] = {"value": self.tdCom.get_long_name(5), "type": "binary"}
        self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        tag_value["t2"] = {"value": self.tdCom.get_long_name(6), "type": "binary"}
        res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.query(f"select * from {self.dbname}.{stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    # * tag nchar max is 16374/4, col+ts nchar max  49151
    def tag_col_nchar_max_length_check(self):
        """
        check nchar length limit
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        stb_name = self.tdCom.get_long_name()
        tag_value = {"t0": True}
        col_value= True
        input_json = {"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": col_value, "tags": tag_value}
        self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)

        # * legal nchar could not be larger than 16374/4
        tag_value["t1"] = {"value": self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"]), "type": "nchar"}
        tag_value["t2"] = {"value": self.tdCom.get_long_name(1), "type": "nchar"}
        self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        tag_value["t2"] = {"value": self.tdCom.get_long_name(2), "type": "binary"}
        res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def batch_insert_check(self):
        """
        test batch insert
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        stb_name = "stb_name"
        # self.tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 bigint)')
        input_json = [{"metric": "st123456", "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": 1, "tags": {"t1": 3, "t2": {"value": 4, "type": "double"}, "t3": {"value": "t3", "type": "binary"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006833640000000, "type": "ns"}, "value": 2, "tags": {"t1": {"value": 4, "type": "double"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}},
                    {"metric": "stb_name", "timestamp": {"value": 1626056811823316532, "type": "ns"}, "value": 3, "tags": {"t2": {"value": 5, "type": "double"}, "t3": {"value": "ste", "type": "nchar"}}},
                    {"metric": "stf567890", "timestamp": {"value": 1626006933640000000, "type": "ns"}, "value": 4, "tags": {"t1": {"value": 4, "type": "bigint"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006833642040000, "type": "ns"}, "value": {"value": 5, "type": "double"}, "tags": {"t1": {"value": 4, "type": "double"}, "t2": 5.0, "t3": {"value": "t4", "type": "binary"}}},
                    {"metric": "stb_name", "timestamp": {"value": 1626056811843316532, "type": "ns"}, "value": {"value": 6, "type": "double"}, "tags": {"t2": 5.0, "t3": {"value": "ste2", "type": "nchar"}}},
                    {"metric": "stb_name", "timestamp": {"value": 1626056812843316532, "type": "ns"}, "value": {"value": 7, "type": "double"}, "tags": {"t2": {"value": 5, "type": "double"}, "t3": {"value": "ste2", "type": "nchar"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006933640000000, "type": "ns"}, "value": {"value": 8, "type": "double"}, "tags": {"t1": {"value": 4, "type": "double"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006933641000000, "type": "ns"}, "value": {"value": 9, "type": "double"}, "tags": {"t1": 4, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}}]
        self.tdRest.schemalessApiPost(json.dumps(input_json), url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.query(f'select * from information_schema.ins_stables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 3)
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, 6)
        self.tdSql.query(f'select * from {self.dbname}.st123456')
        self.tdSql.checkEqual(self.tdSql.query_row, 5)

    def multi_insert_check(self, count):
        """
        test multi insert
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        sql_list = list()
        stb_name = self.tdCom.get_long_name()
        # self.tdSql.execute(f'create stable {self.dbname}.{stb_name}(ts timestamp, f int) tags(t1 tinyint)')
        for i in range(count):
            input_json = self.tdCom.gen_full_type_json(stb_name=stb_name, col_value=self.tdCom.gen_ts_col_value(value=self.tdCom.get_long_name(), t_type="binary"), tag_value=self.tdCom.gen_tag_value(t7_value=self.tdCom.get_long_name()), id_noexist_tag=True)[0]
            sql_list.append(input_json)
        self.tdRest.schemalessApiPost(json.dumps(sql_list), url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.query(f'select * from information_schema.ins_tables where db_name =  "{self.dbname}"')
        self.tdSql.checkEqual(self.tdSql.query_row, count)

    def batch_error_insert_check(self):
        """
        test batch error insert
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_json = [{"metric": "st123456", "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": {"value": "tt", "type": "bool"}, "tags": {"t1": {"value": 3, "type": "bigint"}, "t2": {"value": 4, "type": "double"}, "t3": {"value": "t3", "type": "binary"}}},
                    {"metric": "st123456", "timestamp": {"value": 1626006933641000000, "type": "ns"}, "value": {"value": 9, "type": "bigint"}, "tags": {"t1": {"value": 4, "type": "bigint"}, "t3": {"value": "t4", "type": "binary"}, "t2": {"value": 5, "type": "double"}, "t4": {"value": 5, "type": "double"}}}]
        res = self.tdRest.schemalessApiPost(json.dumps(input_json), url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)

    def multi_cols_insert_check(self):
        """
        test multi cols insert
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_json = self.tdCom.gen_full_type_json(c_multi_tag=True)[0]
        res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)

    def blank_col_insert_check(self):
        """
        test blank col insert
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_json = self.tdCom.gen_full_type_json(c_blank_tag=True)[0]
        res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)

    def blank_tag_insert_check(self):
        """
        test blank tag insert
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_json = self.tdCom.gen_full_type_json(t_blank_tag=True)[0]
        res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)

    def chinese_check(self):
        """
        check nchar ---> chinese
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_json, stb_name = self.tdCom.gen_full_type_json(chinese_tag=True)
        self.tdCom.check_res(input_json, stb_name, dbname=self.dbname)

    def multi_field_check(self):
        '''
        multi_field
        '''
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_json = self.tdCom.gen_full_type_json(multi_field_tag=True)[0]
        res = self.tdRest.schemalessApiPost(input_json, url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 500)

    def point_trans_check(self):
        """
        metric value "." trans to "_"
        """
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_json = self.tdCom.gen_full_type_json(point_trans_tag=True)[0]
        stb_name = input_json["metric"]
        stb_name = stb_name.replace(".", "_")
        res = self.tdRest.schemalessApiPost(json.dumps(input_json), url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 204)
        self.tdSql.execute(f"drop table {self.dbname}.`{stb_name}`")

    def tbname_tags_cols_name_check(self):
        self.tdCom.cleanTb(connect_type="restful", dbname=self.dbname)
        input_json = {'metric': 'rFa$sta', 'timestamp': {'value': 1626006834, 'type': 's'}, 'value': {'value': True, 'type': 'bool'}, 'tags': {'Tt!0': {'value': False, 'type': 'bool'}, 'tT@1': {'value': 127, 'type': 'tinyint'}, 't@2': {'value': 32767, 'type': 'smallint'}, 't$3': {'value': 2147483647, 'type': 'int'}, 't%4': {'value': 9223372036854775807, 'type': 'bigint'}, 't^5': {'value': 11.12345027923584, 'type': 'float'}, 't&6': {'value': 22.123456789, 'type': 'double'}, 't*7': {'value': 'binaryTagValue', 'type': 'binary'}, 't!@#$%^&*()_+[];:<>?,9': {'value': 'ncharTagValue', 'type': 'nchar'}}}
        res = self.tdRest.schemalessApiPost(json.dumps(input_json), url_type="json", precision=None, dbname=self.dbname)
        self.tdSql.checkEqual(res.status_code, 204)
        query_sql = f'select * from {self.dbname}.`rFa$sta`'
        self.tdSql.query(query_sql)
        self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2021, 7, 11, 20, 33, 54), True, False, 127, 32767, 2147483647, 9223372036854775807, 11.12345027923584, 22.123456789, 'binaryTagValue', 'ncharTagValue')])
        self.tdSql.query(f'describe {self.dbname}.`rFa$sta`')
        self.tdSql.checkEqual(self.tdSql.getColNameList(), ['_ts', '_value', 'Tt!0', 'tT@1', 't@2', 't$3', 't%4', 't^5', 't&6', 't*7', 't!@#$%^&*()_+[];:<>?,9'])
        self.tdSql.execute(f'drop table {self.dbname}.`rFa$sta`')


    def run(self) -> bool:
        self.init_check()
        self.bool_check()
        self.symbols_check()
        self.max_col_tag_check()
        self.stb_name_check()
        self.now_check()
        self.date_format_check()
        self.illegal_ts_check()
        self.tag_name_length_check()
        self.tag_value_length_check()
        self.col_value_length_check()
        self.tag_col_illegal_value_check()
        self.duplicate_insert_exist_check()
        self.tag_col_binary_nchar_length_increase_check()
        # self.tag_col_binary_max_length_check()
        # self.tag_col_nchar_max_length_check()
        self.batch_insert_check()
        self.multi_insert_check(10)
        self.batch_error_insert_check()
        self.multi_cols_insert_check()
        self.blank_col_insert_check()
        self.blank_tag_insert_check()
        self.chinese_check()
        self.multi_field_check()
        self.point_trans_check()
        self.tbname_tags_cols_name_check()

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
