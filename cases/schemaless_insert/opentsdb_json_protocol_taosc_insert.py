from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.sml_types import TDSmlProtocolType, TDSmlTimestampType
from taos.error import SchemalessError
import datetime
import json

class TestOpentsdbJsonTaoscInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdCom.sml_type = "opentsdb_json"
        self.tdCom.drop_all_db()
        self.dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname=self.dbname, precision="us")
        self.tdCom.env_setting = self.env_setting
        self.tdCom.set_sml_specified_value()

    def init_check(self, value_type="obj"):
        """
        normal tags and cols, one for every elm
        """
        self.tdCom.cleanTb()
        input_json, stb_name = self.tdCom.gen_full_type_json(value_type=value_type)
        self.tdCom.check_res(input_json, stb_name)

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

    def symbols_check(self, value_type="obj"):
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
        input_json1, stb_name1 = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=binary_symbols, t_type="binary", value_type=value_type),
                                    tag_value=self.tdCom.gen_tag_value(t7_value=binary_symbols, t8_value=nchar_symbols, value_type=value_type))
        input_json2, stb_name2 = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=nchar_symbols, t_type="nchar", value_type=value_type), 
                                    tag_value=self.tdCom.gen_tag_value(t7_value=binary_symbols, t8_value=nchar_symbols, value_type=value_type))
        self.tdCom.check_res(input_json1, stb_name1)
        self.tdCom.check_res(input_json2, stb_name2)

    def ts_check(self, value_type="obj"):
        """
        test ts list --> ["1626006833639000000ns", "1626006833639019us", "1626006833640ms", "1626006834s", "1626006822639022"]
        # ! us级时间戳都为0时，数据库中查询显示，但python接口拿到的结果不显示 .000000的情况请确认，目前修改时间处理代码可以通过
        """
        self.tdCom.cleanTb()
        # TODO commit out
        ts_list = ["1626006833639000000ns", "1626006833639019us", "1626006833640ms", "1626006834s", "1626006834"]
        # ts_list = ["1626006833639000000ns", "1626006833639019us", "1626006833640ms", "1626006834s", "1626006834", 0]
        for ts in ts_list:
            if "s" in str(ts):
                input_json, stb_name = self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value=int(self.tdCom.splitNumLetter(ts)[0]), t_type=self.tdCom.splitNumLetter(ts)[1]))
                self.tdCom.check_res(input_json, stb_name, ts=ts)
            else:
                input_json, stb_name = self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value=int(ts), t_type="s", value_type=value_type))
                print(json.dumps(input_json))
                self.tdCom.check_res(input_json, stb_name, ts=ts)
                if int(ts) == 0:
                    if value_type == "obj":
                        input_json_list = [self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value=int(ts), t_type="")),
                                            self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value=int(ts), t_type="ns")),
                                            self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value=int(ts), t_type="us")),
                                            self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value=int(ts), t_type="ms")),
                                            self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value=int(ts), t_type="s"))]
                    elif value_type == "default":
                        input_json_list = [self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value=int(ts), value_type=value_type))]
                    for input_json in input_json_list:
                        self.tdCom.check_res(input_json[0], input_json[1], ts=ts)
                else:
                    input_json = self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value=int(ts), t_type=""))[0]
                    try:
                        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                        raise Exception("should not reach here")
                    except SchemalessError as err:
                        self.tdSql.checkNotEqual(err.errno, 0)
        # check result
        #! bug
        # self.tdSql.execute(f"drop database if exists test_ts")
        # self.tdSql.execute(f"create database if not exists test_ts precision 'ms'")
        # self.tdSql.execute("use test_ts")
        # input_json = [{"metric": "test_ms", "timestamp": {"value": 1626006833640, "type": "ms"}, "value": False, "tags": {"t0": True}},
        #             {"metric": "test_ms", "timestamp": {"value": 1626006833641, "type": "ms"}, "value": True, "tags": {"t0": True}}]
        # self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        # self.tdSql.query('select * from test_ms')
        # print(self.tdSql.query_data)
        # self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), "2021-07-11 20:33:53.640000")
        # self.tdSql.checkEqual(str(self.tdSql.query_data[1][0]), "2021-07-11 20:33:53.641000")

        self.tdSql.execute(f"drop database if exists test_ts")
        self.tdSql.execute(f"create database if not exists test_ts precision 'us'")
        self.tdSql.execute("use test_ts")
        input_json = [{"metric": "test_us", "timestamp": {"value": 1626006833639000, "type": "us"}, "value": True, "tags": {"t0": True}},
                    {"metric": "test_us", "timestamp": {"value": 1626006833639001, "type": "us"}, "value": False, "tags": {"t0": True}}]
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdSql.query('select * from test_us')
        self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), "2021-07-11 20:33:53.639000")
        self.tdSql.checkEqual(str(self.tdSql.query_data[1][0]), "2021-07-11 20:33:53.639001")

        # ! bug
        # self.tdSql.execute(f"drop database if exists test_ts")
        # self.tdSql.execute(f"create database if not exists test_ts precision 'ns'")
        # self.tdSql.execute("use test_ts")
        # input_json = [{"metric": "test_ns", "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": True, "tags": {"t0": True}},
        #             {"metric": "test_ns", "timestamp": {"value": 1626006833639000001, "type": "ns"}, "value": False, "tags": {"t0": True}}]
        # self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        # self.tdSql.query('select * from test_ns')
        # self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]), "1626006833639000000")
        # self.tdSql.checkEqual(str(self.tdSql.query_data[1][0]), "1626006833639000001")
        # self.tdCom.createDb(dbname=self.dbname, precision="us")


    def max_col_tag_check(self, value_type="obj"):
        """
        max tag count is 128
        """
        for input_json in [self.tdCom.gen_long_json(self.tdCom.boundary_config["MAX_TAG_COUNT"], value_type)[0]]:
            self.tdCom.cleanTb()
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        for input_json in [self.tdCom.gen_long_json(self.tdCom.boundary_config["MAX_TAG_COUNT"]+1, value_type)[0]]:
            self.tdCom.cleanTb()
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

    def stb_name_check(self, value_type="obj"):
        """
        test illegal id name
        mix "`~!@#$¥%^&*()-+{}|[]、「」【】:;《》<>?"
        """
        self.tdCom.cleanTb()
        rstr = list("`~!@#$¥%^&*()-+={}|[]、「」【】\:;《》<>?")
        for i in rstr:
            input_json = self.tdCom.gen_full_type_json(stb_name=f'`aa{i}bb`', value_type=value_type)[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

    def now_check(self, value_type="obj"):
        """
        check now unsupported
        """
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value="now", t_type="ns", value_type=value_type))[0]
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def date_format_check(self, value_type="obj"):
        """
        check date format ts unsupported
        """
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value="2021-07-21\ 19:01:46.920", t_type="ns", value_type=value_type))[0]
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

    def illegal_ts_check(self, value_type="obj"):
        """
        check ts format like 16260068336390us19
        """
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(ts_value=self.tdCom.gen_ts_col_value(value="16260068336390us19", t_type="us", value_type=value_type))[0]
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def tag_name_length_check(self):
        """
        check tag name limit <= 62
        """
        self.tdCom.cleanTb()
        tag_name = self.tdCom.get_long_name(self.tdCom.boundary_config["TAG_KEY_MAX_LENGTH"]-2, "letters")
        stb_name = self.tdCom.get_long_name(7, "letters")
        input_json = {'metric': stb_name, 'timestamp': {'value': 1626006833639000000, 'type': 'ns'}, 'value': "bcdaaa", 'tags': {tag_name: {'value': False, 'type': 'bool'}}}
        self.tdCom.check_res(input_json, stb_name)
        input_json = {'metric': stb_name, 'timestamp': {'value': 1626006833639000001, 'type': 'ns'}, 'value': "bcdaaaa", 'tags': {self.tdCom.get_long_name(self.tdCom.boundary_config["TAG_KEY_MAX_LENGTH"]+1, "letters"): {'value': False, 'type': 'bool'}}}
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def tag_value_length_check(self, value_type="obj"):
        """
        check full type tag value limit
        """
        self.tdCom.cleanTb()
        # nchar
        # * legal nchar could not be larger than 16374/4
        stb_name = self.tdCom.get_long_name(7, "letters")
        # i8
        for t1 in [-self.tdCom.boundary_config["TINYINT_MAX"], self.tdCom.boundary_config["TINYINT_MAX"]]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t1_value=t1, value_type=value_type))
            self.tdCom.check_res(input_json, stb_name)
        for t1 in [-self.tdCom.boundary_config["TINYINT_MAX"]-1, self.tdCom.boundary_config["TINYINT_MAX"]+1]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t1_value=t1))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        #i16
        for t2 in [-self.tdCom.boundary_config["SMALLINT_MAX"], self.tdCom.boundary_config["SMALLINT_MAX"]]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t2_value=t2, value_type=value_type))
            self.tdCom.check_res(input_json, stb_name)
        for t2 in [-self.tdCom.boundary_config["SMALLINT_MAX"]-1, self.tdCom.boundary_config["SMALLINT_MAX"]+1]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t2_value=t2))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        #i32
        for t3 in [-self.tdCom.boundary_config["INT_MAX"], self.tdCom.boundary_config["INT_MAX"]]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t3_value=t3, value_type=value_type))
            self.tdCom.check_res(input_json, stb_name)
        for t3 in [-self.tdCom.boundary_config["INT_MAX"]-1, self.tdCom.boundary_config["INT_MAX"]+1]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t3_value=t3))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        #i64
        for t4 in [-self.tdCom.boundary_config["BIGINT_MAX"], self.tdCom.boundary_config["BIGINT_MAX"]]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t4_value=t4, value_type=value_type))
            self.tdCom.check_res(input_json, stb_name)

        for t4 in [-self.tdCom.boundary_config["BIGINT_MAX"]-1, self.tdCom.boundary_config["BIGINT_MAX"]+1]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t4_value=t4))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # f32
        for t5 in [-3.4028234663852885981170418348451692544*(10**38), 3.4028234663852885981170418348451692544*(10**38)]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t5_value=t5, value_type=value_type))
            self.tdCom.check_res(input_json, stb_name)
        # * limit set to 3.4028234664*(10**38)
        for t5 in [-3.4028234664*(10**38), 3.4028234664*(10**38)]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t5_value=t5))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # f64
        for t6 in [-1.79769*(10**308), -1.79769*(10**308)]:
            input_json, stb_name = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t6_value=t6, value_type=value_type))
            self.tdCom.check_res(input_json, stb_name)
        for t6 in [float(-1.797693134862316*(10**308)), -1.797693134862316*(10**308)]:
            input_json = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t6_value=t6, value_type=value_type))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        if value_type == "obj":
            # binary
            stb_name = self.tdCom.get_long_name(7, "letters")
            input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"], "letters"), 'type': 'binary'}}}
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"]+1, "letters"), 'type': 'binary'}}}
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

            # # nchar
            # # * legal nchar could not be larger than 16374/4
            stb_name = self.tdCom.get_long_name(7, "letters")
            input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"], "letters"), 'type': 'nchar'}}}
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

            input_json = {"metric": stb_name, "timestamp": {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': True, 'type': 'bool'}, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1":{'value': self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"]+1, "letters"), 'type': 'nchar'}}}
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)
        elif value_type == "default":
            stb_name = self.tdCom.get_long_name(7, "letters")
            if self.tdCom.defaultJSONStrType_value == "binary":
                input_json = {"metric": stb_name, "timestamp": 1626006834, "value": True, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1": self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"], "letters")}}
            elif self.tdCom.defaultJSONStrType_value == "nchar" or self.tdCom.defaultJSONStrType_value is None:
                input_json = {"metric": stb_name, "timestamp": 1626006834, "value": True, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1": self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"], "letters")}}
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            if self.tdCom.defaultJSONStrType_value == "binary":
                input_json = {"metric": stb_name, "timestamp": 1626006834, "value": True, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1": self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"]+1, "letters")}}
            elif self.tdCom.defaultJSONStrType_value == "nchar" or self.tdCom.defaultJSONStrType_value is None:
                input_json = {"metric": stb_name, "timestamp": 1626006834, "value": True, "tags": {"t0": {'value': True, 'type': 'bool'}, "t1": self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"]+1, "letters")}}
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

    def col_value_length_check(self, value_type="obj"):
        """
        check full type col value limit
        """
        self.tdCom.cleanTb()
        # i8
        for value in [-self.tdCom.boundary_config["TINYINT_MAX"]]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="tinyint", value_type=value_type))
            self.tdCom.check_res(input_json, stb_name)
        self.tdCom.cleanTb()
        for value in [-self.tdCom.boundary_config["TINYINT_MAX"]-2, self.tdCom.boundary_config["TINYINT_MAX"]+1]:
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="tinyint"))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)
        # i16
        self.tdCom.cleanTb()
        for value in [-self.tdCom.boundary_config["SMALLINT_MAX"]]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="smallint", value_type=value_type))
            self.tdCom.check_res(input_json, stb_name)
        self.tdCom.cleanTb()
        for value in [-self.tdCom.boundary_config["SMALLINT_MAX"]-2, self.tdCom.boundary_config["SMALLINT_MAX"]+1]:
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="smallint"))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # i32
        self.tdCom.cleanTb()
        for value in [-self.tdCom.boundary_config["INT_MAX"]]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="int", value_type=value_type))
            self.tdCom.check_res(input_json, stb_name)
        self.tdCom.cleanTb()
        for value in [-self.tdCom.boundary_config["INT_MAX"]-2, self.tdCom.boundary_config["INT_MAX"]+1]:
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="int"))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # ! i64
        # self.tdCom.cleanTb()
        # for value in [-self.tdCom.boundary_config["BIGINT_MAX"]]:
        #     input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="bigint", value_type=value_type))
        #     self.tdCom.check_res(input_json, stb_name)
        # self.tdCom.cleanTb()
        # for value in [-self.tdCom.boundary_config["BIGINT_MAX"]-2, self.tdCom.boundary_config["BIGINT_MAX"]+1]:
        #     input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="bigint"))[0]
        #     try:
        #         self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        #         raise Exception("should not reach here")
        #     except SchemalessError as err:
        #         self.tdSql.checkNotEqual(err.errno, 0)

        # f32
        self.tdCom.cleanTb()
        for value in [-3.4028234663852885981170418348451692544*(10**38), 3.4028234663852885981170418348451692544*(10**38)]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="float", value_type=value_type))
            self.tdCom.check_res(input_json, stb_name)
        # * limit set to 4028234664*(10**38)
        self.tdCom.cleanTb()
        for value in [-3.4028234664*(10**38), 3.4028234664*(10**38)]:
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="float"))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # f64
        self.tdCom.cleanTb()
        for value in [-1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308), -1.79769313486231570814527423731704356798070567525844996598917476803157260780*(10**308)]:
            input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="double", value_type=value_type))
            self.tdCom.check_res(input_json, stb_name)
        # * limit set to 1.797693134862316*(10**308)
        self.tdCom.cleanTb()
        for value in [-1.797693134862316*(10**308), -1.797693134862316*(10**308)]:
            input_json = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=value, t_type="double", value_type=value_type))[0]
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
                raise Exception("should not reach here")
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)
        #! bug
        # if value_type == "obj":
        #     # binary
        #     self.tdCom.cleanTb()
        #     stb_name = self.tdCom.get_long_name(7, "letters")
        #     input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"], "letters"), 'type': 'binary'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

        #     self.tdCom.cleanTb()
        #     input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"]+1, "letters"), 'type': 'binary'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     try:
        #         self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        #         raise Exception("should not reach here")
        #     except SchemalessError as err:
        #         self.tdSql.checkNotEqual(err.errno, 0)

        #     # nchar
        #     # * legal nchar could not be larger than 16374/4
        #     self.tdCom.cleanTb()
        #     stb_name = self.tdCom.get_long_name(7, "letters")
        #     input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"], "letters"), 'type': 'nchar'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

        #     self.tdCom.cleanTb()
        #     input_json = {"metric": stb_name, "timestamp":  {'value': 1626006833639000000, 'type': 'ns'}, "value": {'value': self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"]+1, "letters"), 'type': 'nchar'}, "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     try:
        #         self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        #         raise Exception("should not reach here")
        #     except SchemalessError as err:
        #         self.tdSql.checkNotEqual(err.errno, 0)
        # elif value_type == "default":
        #     # binary
        #     self.tdCom.cleanTb()
        #     stb_name = self.tdCom.get_long_name(7, "letters")
        #     if self.tdCom.defaultJSONStrType_value == "binary":
        #         input_json = {"metric": stb_name, "timestamp": 1626006834, "value": self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"], "letters"), "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     elif self.tdCom.defaultJSONStrType_value == "nchar" or self.tdCom.defaultJSONStrType_value is None:
        #         input_json = {"metric": stb_name, "timestamp": 1626006834, "value": self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"], "letters"), "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        #     self.tdCom.cleanTb()
        #     if self.tdCom.defaultJSONStrType_value == "binary":
        #         input_json = {"metric": stb_name, "timestamp": 1626006834, "value": self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"]+1, "letters"), "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     elif self.tdCom.defaultJSONStrType_value == "nchar" or self.tdCom.defaultJSONStrType_value is None:
        #         input_json = {"metric": stb_name, "timestamp": 1626006834, "value": self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"]+1, "letters"), "tags": {"t0": {'value': True, 'type': 'bool'}}}
        #     try:
        #         self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        #         raise Exception("should not reach here")
        #     except SchemalessError as err:
        #         self.tdSql.checkNotEqual(err.errno, 0)

    def tag_col_illegal_value_check(self, value_type="obj"):
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
        input_sql1 = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value="abc aaa", t_type="binary", value_type=value_type))[0]
        input_sql2 = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value="abc aaa", t_type="nchar", value_type=value_type))[0]
        input_sql3 = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t7_value="abc aaa", value_type=value_type))[0]
        input_sql4 = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t8_value="abc aaa", value_type=value_type))[0]
        for input_json in [input_sql1, input_sql2, input_sql3, input_sql4]:
            try:
                self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            except SchemalessError as err:
                self.tdSql.checkNotEqual(err.errno, 0)

        # check accepted binary and nchar symbols
        # # * ~!@#$¥%^&*()-+={}|[]、「」:;
        for symbol in list('~!@#$¥%^&*()-+={}|[]、「」:;'):
            input_json1 = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value=f"abc{symbol}aaa", t_type="binary", value_type=value_type))[0]
            input_json2 = self.tdCom.gen_full_type_json(tag_value=self.tdCom.gen_tag_value(t8_value=f"abc{symbol}aaa", value_type=value_type))[0]
            self.tdSql._conn.schemaless_insert([json.dumps(input_json1)], TDSmlProtocolType.JSON.value, None)
            self.tdSql._conn.schemaless_insert([json.dumps(input_json2)], TDSmlProtocolType.JSON.value, None)

    ##### stb exist #####
    def duplicate_insert_exist_check(self, value_type="obj"):
        """
        check duplicate insert when stb exist
        """
        self.tdCom.cleanTb()
        input_json, stb_name = self.tdCom.gen_full_type_json(value_type=value_type)
        self.tdCom.check_res(input_json, stb_name)
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdCom.check_res(input_json, stb_name)

    def tag_col_binary_nchar_length_increase_check(self, value_type="obj"):
        """
        check length increase
        """
        self.tdCom.cleanTb()
        input_json, stb_name = self.tdCom.gen_full_type_json(value_type=value_type)
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdCom.check_res(input_json, stb_name)
        tb_name = self.tdCom.get_long_name(5, "letters")
        input_json, stb_name = self.tdCom.gen_full_type_json(stb_name=stb_name, tb_name=tb_name, tag_value=self.tdCom.gen_tag_value(t7_value="binaryTagValuebinaryTagValue", t8_value="ncharTagValuencharTagValue", value_type=value_type))
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdCom.check_res(input_json, stb_name, condition=f'where t7="binaryTagValuebinaryTagValue"')

    def lengthIcreaseCrashCheckCase(self):
        """
        check length increase
        """
        self.tdCom.cleanTb()
        stb_name = "test_crash"
        input_json = self.tdCom.gen_full_type_json(stb_name=stb_name)[0]
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        os.system('python3 query/schemalessQueryCrash.py &')
        time.sleep(2)
        tb_name = self.tdCom.get_long_name(5, "letters")
        input_json, stb_name = self.tdCom.gen_full_type_json(stb_name=stb_name, tb_name=tb_name, tag_value=self.tdCom.gen_tag_value(t7_value="binaryTagValuebinaryTagValue", t8_value="ncharTagValuencharTagValue"))
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        time.sleep(3)
        self.tdSql.query(f"select * from {stb_name}")
        tdSql.checkRows(2)

    # * tag nchar max is 16374/4, col+ts nchar max  49151
    def tag_col_binary_max_length_check(self, value_type="obj"):
        """
        check nchar length limit
        """
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name(7, "letters")
        tag_value = {"t0": {"value": True, "type": "bool"}}
        col_value=self.tdCom.gen_ts_col_value(value=True, t_type="bool", value_type=value_type)
        input_json = {"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": col_value, "tags": tag_value}
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)

        # * every binary and nchar must be length+2, so here is two tag, max length could not larger than 16384-2*2
        if value_type == "obj":
            tag_value["t1"] = {"value": self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"], "letters"), "type": "binary"}
            tag_value["t2"] = {"value": self.tdCom.get_long_name(5, "letters"), "type": "binary"}
        elif value_type == "default":
            if self.tdCom.defaultJSONStrType_value == "binary":
                tag_value["t1"] = self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"], "letters")
                tag_value["t2"] = self.tdCom.get_long_name(5, "letters")
            elif self.tdCom.defaultJSONStrType_value == "nchar" or self.tdCom.defaultJSONStrType_value == None:
                tag_value["t1"] = self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"], "letters")
                tag_value["t2"] = self.tdCom.get_long_name(1, "letters")
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        if value_type == "obj":
            tag_value["t2"] = {"value": self.tdCom.get_long_name(6, "letters"), "type": "binary"}
        elif value_type == "default":
            if self.tdCom.defaultJSONStrType_value == "binary":
                tag_value["t2"] = self.tdCom.get_long_name(6, "letters")
            elif self.tdCom.defaultJSONStrType_value == "nchar" or self.tdCom.defaultJSONStrType_value == None:
                tag_value["t2"] = self.tdCom.get_long_name(2, "letters")
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    # * tag nchar max is 16374/4, col+ts nchar max  49151
    def tag_col_nchar_max_length_check(self, value_type="obj"):
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
        if value_type == "obj":
            tag_value["t1"] = {"value": self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"], "letters"), "type": "nchar"}
            tag_value["t2"] = {"value": self.tdCom.get_long_name(1, "letters"), "type": "nchar"}
        elif value_type == "default":
            if self.tdCom.defaultJSONStrType_value == "binary":
                tag_value["t1"] = self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"], "letters")
                tag_value["t2"] = self.tdCom.get_long_name(5, "letters")
            elif self.tdCom.defaultJSONStrType_value == "nchar" or self.tdCom.defaultJSONStrType_value == None:
                tag_value["t1"] = self.tdCom.get_long_name(self.tdCom.boundary_config["NCHAR_MAX_LENGTH"], "letters")
                tag_value["t2"] = self.tdCom.get_long_name(1, "letters")
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        if value_type == "obj":
            tag_value["t2"] = {"value": self.tdCom.get_long_name(2, "letters"), "type": "binary"}
        elif value_type == "default":
            if self.tdCom.defaultJSONStrType_value == "binary":
                tag_value["t2"] = self.tdCom.get_long_name(6, "letters")
            elif self.tdCom.defaultJSONStrType_value == "nchar" or self.tdCom.defaultJSONStrType_value == None:
                tag_value["t2"] = self.tdCom.get_long_name(2, "letters")
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def batch_insert_check(self, value_type="obj"):
        """
        test batch insert
        """
        self.tdCom.cleanTb()
        stb_name = "stb_name"
        self.tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 bigint)')
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
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdSql.query('show stables')
        self.tdSql.checkEqual(self.tdSql.query_row, 3)
        self.tdSql.query('show tables')
        self.tdSql.checkEqual(self.tdSql.query_row, 6)
        self.tdSql.query('select * from st123456')
        self.tdSql.checkEqual(self.tdSql.query_row, 5)

    def multi_insert_check(self, count, value_type="obj"):
        """
        test multi insert
        """
        self.tdCom.cleanTb()
        sql_list = list()
        stb_name = self.tdCom.get_long_name(8, "letters")
        # self.tdSql.execute(f'create stable {stb_name}(ts timestamp, f int) tags(t1 bigint)')
        for i in range(count):
            input_json = self.tdCom.gen_full_type_json(stb_name=stb_name, col_value=self.tdCom.gen_ts_col_value(value=self.tdCom.get_long_name(8, "letters"), t_type="binary", value_type=value_type), tag_value=self.tdCom.gen_tag_value(t7_value=self.tdCom.get_long_name(8, "letters"), value_type=value_type), id_noexist_tag=True)[0]
            sql_list.append(input_json)
        self.tdSql._conn.schemaless_insert([json.dumps(sql_list)], TDSmlProtocolType.JSON.value, None)
        self.tdSql.query('show tables')
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

    def multi_cols_insert_check(self, value_type="obj"):
        """
        test multi cols insert
        """
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(c_multi_tag=True, value_type=value_type)[0]
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def blank_col_insert_check(self, value_type="obj"):
        """
        test blank col insert
        """
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(c_blank_tag=True, value_type=value_type)[0]
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def blank_tag_insert_check(self, value_type="obj"):
        """
        test blank tag insert
        """
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(t_blank_tag=True, value_type=value_type)[0]
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
        self.tdCom.check_res(input_json, stb_name)

    def multi_field_check(self, value_type="obj"):
        '''
        multi_field
        '''
        self.tdCom.cleanTb()
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_full_type_json(multi_field_tag=True, value_type=value_type)[0]
        try:
            self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
            raise Exception("should not reach here")
        except SchemalessError as err:
            self.tdSql.checkNotEqual(err.errno, 0)

    def spell_check(self):
        self.tdCom.cleanTb()
        stb_name = self.tdCom.get_long_name(8, "letters")
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
            self.tdCom.check_res(input_sql, stb_name)

    def point_trans_check(self, value_type="obj"):
        """
        metric value "." trans to "_"
        """
        self.tdSql.execute(f"create database if not exists test_point_trans precision 'ms'")
        self.tdSql.execute("use test_point_trans")
        input_json = self.tdCom.gen_full_type_json(point_trans_tag=True, value_type=value_type)[0]
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        self.tdSql.execute("drop table `.point.trans.test`")
        self.tdSql.execute(f"drop database test_point_trans")
        self.tdCom.createDb(dbname=self.dbname, precision="us")

    def tbname_tags_cols_name_check(self):
        self.tdCom.cleanTb()
        input_json = {'metric': 'rFa$sta', 'timestamp': {'value': 1626006834, 'type': 's'}, 'value': {'value': True, 'type': 'bool'}, 'tags': {'Tt!0': {'value': False, 'type': 'bool'}, 'tT@1': {'value': 127, 'type': 'tinyint'}, 't@2': {'value': 32767, 'type': 'smallint'}, 't$3': {'value': 2147483647, 'type': 'int'}, 't%4': {'value': 9223372036854775807, 'type': 'bigint'}, 't^5': {'value': 11.12345027923584, 'type': 'float'}, 't&6': {'value': 22.123456789, 'type': 'double'}, 't*7': {'value': 'binaryTagValue', 'type': 'binary'}, 't!@#$%^&*()_+[];:<>?,9': {'value': 'ncharTagValue', 'type': 'nchar'}}}
        self.tdSql._conn.schemaless_insert([json.dumps(input_json)], TDSmlProtocolType.JSON.value, None)
        query_sql = 'select * from `rFa$sta`'
        self.tdSql.query(query_sql)
        self.tdSql.checkEqual(self.tdSql.query_data, [(datetime.datetime(2021, 7, 11, 20, 33, 54), True, 'ncharTagValue', 2147483647, 9223372036854775807, 22.123456789, 'binaryTagValue', 32767, 11.12345027923584, False, 127)])
        self.tdSql.query('describe `rFa$sta`')
        self.tdSql.checkEqual(self.tdSql.getColNameList(), ['ts', 'value', 't!@#$%^&*()_+[];:<>?,9', 't$3', 't%4', 't&6', 't*7', 't@2', 't^5', 'Tt!0', 'tT@1'])
        self.tdSql.execute('drop table `rFa$sta`')

    def stb_insert_multi_thread_check(self, value_type="obj"):
        """
        thread input different stb
        """
        self.tdCom.cleanTb()
        input_json = self.tdCom.gen_json_list(value_type=value_type)[0]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(input_json))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 5)

    def s_stb_s_tb_d_data_insert_multi_thread_check(self, value_type="obj"):
        """
        thread input same stb tb, different data, result keep first data
        """
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name(7, "letters")
        input_json, stb_name = self.tdCom.gen_full_type_json(tb_name=tb_name, col_value=self.tdCom.gen_ts_col_value(value="binaryTagValue", t_type="binary", value_type=value_type))
        self.tdCom.check_res(input_json, stb_name)
        s_stb_s_tb_list = self.tdCom.gen_json_list(stb_name=stb_name, tb_name=tb_name, value_type=value_type)[1]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        if self.tdCom.smlChildTableName_value == "ID":
            expected_tb_name = self.tdCom.get_no_id_tbname(stb_name)[0]
            self.tdSql.checkEqual(tb_name, expected_tb_name)
        self.tdSql.query(f"select * from {stb_name};")
        self.tdSql.checkEqual(self.tdSql.query_row, 1)

    def s_stb_s_tb_d_data_at_insert_multi_thread_check(self, value_type="obj"):
        """
        thread input same stb tb, different data, add tags,  result keep first data
        """
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name(7, "letters")
        input_json, stb_name = self.tdCom.gen_full_type_json(tb_name=tb_name, col_value=self.tdCom.gen_ts_col_value(value="binaryTagValue", t_type="binary", value_type=value_type))
        self.tdCom.check_res(input_json, stb_name)
        s_stb_s_tb_a_tag_list = self.tdCom.gen_json_list(stb_name=stb_name, tb_name=tb_name, value_type=value_type)[2]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_a_tag_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        if self.tdCom.smlChildTableName_value == "ID":
            expected_tb_name = self.tdCom.get_no_id_tbname(stb_name)[0]
            self.tdSql.checkEqual(tb_name, expected_tb_name)
        self.tdSql.query(f"select * from {stb_name};")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def s_stb_stb_d_data_mt_insert_multi_thread_check(self, value_type="obj"):
        """
        thread input same stb tb, different data, minus columes and tags,  result keep first data
        """
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name(7, "letters")
        input_json, stb_name = self.tdCom.gen_full_type_json(tb_name=tb_name, col_value=self.tdCom.gen_ts_col_value(value="binaryTagValue", t_type="binary", value_type=value_type))
        self.tdCom.check_res(input_json, stb_name)
        s_stb_s_tb_m_tag_list = self.tdCom.gen_json_list(stb_name=stb_name, tb_name=tb_name, value_type=value_type)[3]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_m_tag_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        if self.tdCom.smlChildTableName_value == "ID":
            expected_tb_name = self.tdCom.get_no_id_tbname(stb_name)[0]
            self.tdSql.checkEqual(tb_name, expected_tb_name)
        self.tdSql.query(f"select * from {stb_name};")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def s_stb_d_tb_d_data_insert_multi_thread_check(self, value_type="obj"):
        """
        thread input same stb, different tb, different data
        """
        self.tdCom.cleanTb()
        input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value="binaryTagValue", t_type="binary", value_type=value_type))
        self.tdCom.check_res(input_json, stb_name)
        s_stb_d_tb_list = self.tdCom.gen_json_list(stb_name=stb_name, value_type=value_type)[4]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_d_tb_d_data_mt_insert_multi_thread_check(self):
        """
        thread input same stb, different tb, different data, mul tag
        """
        self.tdCom.cleanTb()
        input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value="binaryTagValue", t_type="binary"))
        self.tdCom.check_res(input_json, stb_name)
        s_stb_d_tb_m_tag_list = [({"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": "omfdhyom", "tags": {"t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": "vqowydbc", "tags": {"t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": "plgkckpv", "tags": {"t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": "cujyqvlj", "tags": {"t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 1626006833639000000, "type": "ns"}, "value": "twjxisat", "tags": {"t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}}}, 'yzwswz')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_m_tag_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)

    def s_stb_d_tb_d_data_at_insert_multi_thread_check(self, value_type="obj"):
        """
        thread input same stb, different tb, different data, add tag
        """
        self.tdCom.cleanTb()
        input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value="binaryTagValue", t_type="binary", value_type=value_type))
        self.tdCom.check_res(input_json, stb_name)
        s_stb_d_tb_a_tag_list = self.tdCom.gen_json_list(stb_name=stb_name, value_type=value_type)[6]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_a_tag_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_s_tb_d_data_d_ts_insert_multi_thread_check(self):
        """
        thread input same stb tb, different ts
        """
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name(7, "letters")
        input_json, stb_name = self.tdCom.gen_full_type_json(tb_name=tb_name, col_value=self.tdCom.gen_ts_col_value(value="binaryTagValue", t_type="binary"))
        self.tdCom.check_res(input_json, stb_name)
        s_stb_s_tb_d_ts_list = [({"metric": stb_name, "timestamp": {"value": 1626006833639001000, "type": "ns"}, "value": "hkgjiwdj", "tags": {"id": tb_name, "t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}, "t7": {"value": "vozamcts", "type": "binary"}, "t8": {"value": "ncharTagValue", "type": "nchar"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 1626006833639002000, "type": "ns"}, "value": "rljjrrul", "tags": {"id": tb_name, "t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}, "t7": {"value": "bmcanhbs", "type": "binary"}, "t8": {"value": "ncharTagValue", "type": "nchar"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 1626006833639003000, "type": "ns"}, "value": "basanglx", "tags": {"id": tb_name, "t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}, "t7": {"value": "enqkyvmb", "type": "binary"}, "t8": {"value": "ncharTagValue", "type": "nchar"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 1626006833639004000, "type": "ns"}, "value": "clsajzpp", "tags": {"id": tb_name, "t0": {"value": False, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}, "t7": {"value": "eivaegjk", "type": "binary"}, "t8": {"value": "ncharTagValue", "type": "nchar"}}}, 'yzwswz'),
                                ({"metric": stb_name, "timestamp": {"value": 1626006833639005000, "type": "ns"}, "value": "jitwseso", "tags": {"id": tb_name, "t0": {"value": True, "type": "bool"}, "t1": {"value": 127, "type": "tinyint"}, "t2": {"value": 32767, "type": "smallint"}, "t3": {"value": 2147483647, "type": "int"}, "t4": {"value": 9223372036854775807, "type": "bigint"}, "t5": {"value": 11.12345, "type": "float"}, "t6": {"value": 22.123456789, "type": "double"}, "t7": {"value": "yhlwkddq", "type": "binary"}, "t8": {"value": "ncharTagValue", "type": "nchar"}}}, 'yzwswz')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_d_ts_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 1) if self.tdCom.smlChildTableName_value == "ID" else self.tdSql.checkEqual(self.tdSql.query_row, 6)
        self.tdSql.query(f"select * from {stb_name}")
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_s_tb_d_data_d_ts_mt_insert_multi_thread_check(self):
        """
        thread input same stb tb, different ts, mul tag
        """
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name(7, "letters")
        input_json, stb_name = self.tdCom.gen_full_type_json(tb_name=tb_name, col_value=self.tdCom.gen_ts_col_value(value="binaryTagValue", t_type="binary"))
        self.tdCom.check_res(input_json, stb_name)
        s_stb_s_tb_d_ts_m_tag_list = [({'metric': stb_name, 'timestamp': {'value': 1626006833639001000, 'type': 'ns'}, 'value': 'pjndapjb', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'tuzsfrom', 'type': 'binary'}, 'id': tb_name}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 1626006833639002000, 'type': 'ns'}, 'value': 'llqzvgvw', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'tuzsfrom', 'type': 'binary'}, 'id': tb_name}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 1626006833639003000, 'type': 'ns'}, 'value': 'tclbosqc', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'tuzsfrom', 'type': 'binary'}, 'id': tb_name}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 1626006833639004000, 'type': 'ns'}, 'value': 'rlpuzodt', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'tuzsfrom', 'type': 'binary'}, 'id': tb_name}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 1626006833639005000, 'type': 'ns'}, 'value': 'rhnikvfq', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'tuzsfrom', 'type': 'binary'}, 'id': tb_name}}, 'punftb')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_d_ts_m_tag_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        self.tdSql.query(f"select * from {stb_name}")
        # ! not stable
        self.tdSql.checkEqual(self.tdSql.query_row, 6)
        self.tdSql.query(f"select * from {stb_name} where t8 is not NULL")
        self.tdSql.checkEqual(self.tdSql.query_row, 1)

    def s_stb_s_tb_d_data_d_ts_at_insert_multi_thread_check(self, value_type="obj"):
        """
        thread input same stb tb, different ts, add tag
        """
        self.tdCom.cleanTb()
        tb_name = self.tdCom.get_long_name(7, "letters")
        input_json, stb_name = self.tdCom.gen_full_type_json(tb_name=tb_name, col_value=self.tdCom.gen_ts_col_value(value="binaryTagValue", t_type="binary"))
        self.tdCom.check_res(input_json, stb_name)
        s_stb_s_tb_d_ts_a_tag_list = [({'metric': stb_name, 'timestamp': {'value': 1626006833639001000, 'type': 'ns'}, 'value': 'pjndapjb', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'tuzsfrom', 'type': 'binary'}, 't8': {'value': 'ncharTagValue', 'type': 'nchar'}, 't11': {'value': 127, 'type': 'tinyint'}, 't10': {'value': 'ncharTagValue', 'type': 'nchar'}, 'id': tb_name}}, 'punftb'), 
                                    ({'metric': stb_name, 'timestamp': {'value': 1626006833639002000, 'type': 'ns'}, 'value': 'llqzvgvw', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'tuzsfrom', 'type': 'binary'}, 't8': {'value': 'ncharTagValue', 'type': 'nchar'}, 't11': {'value': 127, 'type': 'tinyint'}, 't10': {'value': 'ncharTagValue', 'type': 'nchar'}, 'id': tb_name}}, 'punftb'), 
                                    ({'metric': stb_name, 'timestamp': {'value': 1626006833639003000, 'type': 'ns'}, 'value': {'value': 'tclbosqc', 'type': 'binary'}, 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'tuzsfrom'}, 't7': {'value': 'uatpzgpi', 'type': 'binary'}, 't8': {'value': 'ncharTagValue', 'type': 'nchar'}, 't11': {'value': 127, 'type': 'tinyint'}, 't10': {'value': 'ncharTagValue', 'type': 'nchar'}, 'id': tb_name}}, 'punftb'), 
                                    ({'metric': stb_name, 'timestamp': {'value': 1626006833639004000, 'type': 'ns'}, 'value': 'rlpuzodt', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'tuzsfrom', 'type': 'binary'}, 't8': {'value': 'ncharTagValue', 'type': 'nchar'}, 't11': {'value': 127, 'type': 'tinyint'}, 't10': {'value': 'ncharTagValue', 'type': 'nchar'}, 'id': tb_name}}, 'punftb'), 
                                    ({'metric': stb_name, 'timestamp': {'value': 1626006833639005000, 'type': 'ns'}, 'value': {'value': 'rhnikvfq', 'type': 'binary'}, 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'tuzsfrom'}, 't7': {'value': 'afcibyeb', 'type': 'binary'}, 't8': {'value': 'ncharTagValue', 'type': 'nchar'}, 't11': {'value': 127, 'type': 'tinyint'}, 't10': {'value': 'ncharTagValue', 'type': 'nchar'}, 'id': tb_name}}, 'punftb')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_s_tb_d_ts_a_tag_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 2)
        # ! not stable
        # self.tdSql.query(f"select * from {stb_name}")
        # self.tdSql.checkEqual(self.tdSql.query_row, 6)
        # for t in ["t10", "t11"]:
        #     self.tdSql.query(f"select * from {stb_name} where {t} is not NULL;")
        #     self.tdSql.checkEqual(self.tdSql.query_row, 5)

    def s_stb_d_tb_d_data_d_ts_insert_multi_thread_check(self, value_type="obj"):
        """
        thread input same stb, different tb, data, ts
        """
        self.tdCom.cleanTb()
        input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value="binaryTagValue", t_type="binary", value_type=value_type))
        self.tdCom.check_res(input_json, stb_name)
        s_stb_d_tb_d_ts_list = self.tdCom.gen_json_list(stb_name=stb_name, value_type=value_type)[10]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_d_ts_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 6)

    def s_stb_d_tb_d_data_d_ts_mt_insert_multi_thread_check(self, value_type="obj"):
        """
        thread input same stb, different tb, data, ts, add col, mul tag
        """
        self.tdCom.cleanTb()
        input_json, stb_name = self.tdCom.gen_full_type_json(col_value=self.tdCom.gen_ts_col_value(value="binaryTagValue", t_type="binary"))
        self.tdCom.check_res(input_json, stb_name)
        s_stb_d_tb_d_ts_m_tag_list = [({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': 'pjndapjb', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': {'value': 'llqzvgvw', 'type': 'binary'}, 'tags': {'t0': {'value': True, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': 'tclbosqc', 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': {'value': 'rlpuzodt', 'type': 'binary'}, 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}}}, 'punftb'),
                                    ({'metric': stb_name, 'timestamp': {'value': 0, 'type': 'ns'}, 'value': {'value': 'rhnikvfq', 'type': 'binary'}, 'tags': {'t0': {'value': False, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {"value": 9223372036854775807, "type": "bigint"}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}}}, 'punftb')]
        self.tdCom.multi_thread_run(self.tdCom.gen_multi_thread_sql(s_stb_d_tb_d_ts_m_tag_list))
        self.tdSql.query(f"show tables;")
        self.tdSql.checkEqual(self.tdSql.query_row, 3)

    def test(self):
        self.ts_check()

    def run(self) -> bool:
        # self.test()
        for value_type in ["obj", "default"]:
            self.init_check(value_type)
            self.symbols_check(value_type)
        # ! TD-15830  TD-15831   self.ts_check()
            # self.max_col_tag_check(value_type)
            self.now_check(value_type)
            self.date_format_check(value_type)
            self.illegal_ts_check(value_type)
            # self.tag_value_length_check(value_type)
            self.col_value_length_check(value_type)
            self.tag_col_illegal_value_check(value_type)
            # self.tag_col_binary_nchar_length_increase_check(value_type)
            # self.tag_col_binary_max_length_check(value_type)
            # self.tag_col_nchar_max_length_check(value_type)
            # self.batch_insert_check(value_type)
            self.multi_insert_check(10, value_type)
            self.multi_cols_insert_check(value_type)
            self.blank_col_insert_check(value_type)
            self.blank_tag_insert_check(value_type)
            self.multi_field_check(value_type)
            # TODO self.spell_check()
            self.point_trans_check(value_type)
            self.stb_insert_multi_thread_check(value_type)
        self.tag_name_length_check()
        self.bool_check()
        self.stb_name_check()
        self.batch_error_insert_check()
        self.chinese_check()
        # self.tbname_tags_cols_name_check()
        self.s_stb_s_tb_d_data_insert_multi_thread_check()
        # self.s_stb_s_tb_d_data_at_insert_multi_thread_check()
        # self.s_stb_stb_d_data_mt_insert_multi_thread_check()
        self.s_stb_d_tb_d_data_insert_multi_thread_check()
        # self.s_stb_d_tb_d_data_mt_insert_multi_thread_check()
        # self.s_stb_d_tb_d_data_at_insert_multi_thread_check()
        # self.s_stb_s_tb_d_data_d_ts_insert_multi_thread_check()
        # self.s_stb_s_tb_d_data_d_ts_mt_insert_multi_thread_check()
        # self.s_stb_s_tb_d_data_d_ts_at_insert_multi_thread_check()
        # self.s_stb_d_tb_d_data_d_ts_insert_multi_thread_check()
        # self.s_stb_d_tb_d_data_d_ts_mt_insert_multi_thread_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
            init_check()
            symbols_check()
            ts_check()
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
            multi_insert_check(10, )
            multi_cols_insert_check()
            blank_col_insert_check()
            blank_tag_insert_check()
            multi_field_check()
            spell_check()
            point_trans_check()
            stb_insert_multi_thread_check()
            tag_name_length_check()
            bool_check()
            stb_name_check()
            batch_error_insert_check()
            chinese_check()
            tbname_tags_cols_name_check()
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
        return T.Write.Schemaless.Taosc.OpenTsDBJson
