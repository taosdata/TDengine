import math
from random import randrange
import random
import time
import threading
import secrets
import numpy
from pandas.compat import is_platform_arm

from stable import query_after_reset
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from decimal import *
from multiprocessing import Value, Lock
from functools import cmp_to_key

class AtomicCounter:
    def __init__(self, initial_value=0):
        self._value = Value('i', initial_value)
        self._lock = Lock()

    def fetch_add(self, delta = 1):
        with self._lock:
            old_value = self._value.value
            self._value.value += delta
            return old_value

getcontext().prec = 40

def get_decimal(val, scale: int):
    if val == 'NULL':
        return None
    getcontext().prec = 100
    try:
        return Decimal(val).quantize(Decimal("1." + "0" * scale), ROUND_HALF_UP)
    except:
        tdLog.exit(f"faield to convert to decimal for v: {val} scale: {scale}")

syntax_error = -2147473920
invalid_column = -2147473918
invalid_compress_level = -2147483084
invalid_encode_param = -2147483087
invalid_operation = -2147483136
scalar_convert_err = -2147470768


decimal_test_query = True
decimal_insert_validator_test = True
operator_test_round = 1
tb_insert_rows = 1000
binary_op_with_const_test = True
binary_op_with_col_test = True
unary_op_test = True
binary_op_in_where_test = True
test_decimal_funcs = False
cast_func_test_round = 10
in_op_test_round = 10

null_ratio = 0.8
test_round = 100

class DecimalTypeGeneratorConfig:
    def __init__(self):
        self.enable_weight_overflow: bool = False
        self.weightOverflowRatio: float = 0.001
        self.enable_scale_overflow: bool = True
        self.scale_overflow_ratio = 0.1
        self.enable_positive_sign = False
        self.with_corner_case = True
        self.corner_case_ratio = null_ratio
        self.positive_ratio = 0.7
        self.prec = 38
        self.scale = 10


class DecimalStringRandomGenerator:
    def __init__(self):
        self.corner_cases = ["NULL", "0"]
        self.ratio_base: int = 1000000

    def possible(self, possibility: float) -> bool:
        return random.randint(0, self.ratio_base) < possibility * self.ratio_base

    def generate_sign(self, positive_ratio: float) -> str:
        if self.possible(positive_ratio):
            return "+"
        return "-"

    def generate_digit(self) -> str:
        return str(random.randint(0, 9))

    def current_should_generate_corner_case(self, corner_case_ratio: float) -> bool:
        return self.possible(corner_case_ratio)

    def generate_corner_case(self, config: DecimalTypeGeneratorConfig) -> str:
        if self.possible(0.8):
            return random.choice(self.corner_cases)
        else:
            res = self.generate_digit() * (config.prec - config.scale)
            if self.possible(0.8):
                res += "."
                if self.possible(0.8):
                    res += self.generate_digit() * config.scale
        return res

    ## 写入大整数的例子, 如10000000000, scale解析时可能为负数
    ## Generate decimal with E/e
    def generate(self, config: DecimalTypeGeneratorConfig) -> str:
        ret: str = ""
        sign = self.generate_sign(config.positive_ratio)
        if config.with_corner_case and self.current_should_generate_corner_case(
            config.corner_case_ratio
        ):
            ret += self.generate_corner_case(config)
        else:
            if config.enable_positive_sign or sign != "+":
                ret += sign
            weight = random.randint(1, config.prec - config.scale)
            scale = random.randint(1, config.scale)
            for i in range(weight):
                ret += self.generate_digit()

            if config.enable_weight_overflow and self.possible(
                config.weightOverflowRatio
            ):
                extra_weight = (
                    config.prec
                    - weight
                    + 1
                    + random.randint(1, self.get_max_prec(config.prec))
                )
                while extra_weight > 0:
                    ret += self.generate_digit()
                    extra_weight -= 1
            ret += "."
            for i in range(scale):
                ret += self.generate_digit()
            if config.enable_scale_overflow and self.possible(
                config.scale_overflow_ratio
            ):
                extra_scale = (
                    config.scale
                    - scale
                    + 1
                    + random.randint(1, self.get_max_prec(config.prec))
                )
                while extra_scale > 0:
                    ret += self.generate_digit()
                    extra_scale -= 1
        return ret

    def get_max_prec(self, prec):
        if prec <= 18:
            return 18
        else:
            return 38


class DecimalColumnAggregator:
    def __init__(self):
        self.max: Decimal = Decimal("0")
        self.min: Decimal = Decimal("0")
        self.count: int = 0
        self.sum: Decimal = Decimal("0")
        self.null_num: int = 0
        self.none_num: int = 0
        self.first = None
        self.last = None

    def add_value(self, value: str, scale: int):
        self.count += 1
        if value == "NULL":
            self.null_num += 1
        elif value == "None":
            self.none_num += 1
        else:
            v: Decimal = get_decimal(value, scale)
            if self.first is None:
                self.first = v
            self.last = v
            self.sum += v
            if v > self.max:
                self.max = v
            if v < self.min:
                self.min = v

atomic_counter = AtomicCounter(0)

class TypeEnum:
    BOOL = 1
    TINYINT = 2
    SMALLINT = 3
    INT = 4
    BIGINT = 5
    FLOAT = 6
    DOUBLE = 7
    VARCHAR = 8
    TIMESTAMP = 9
    NCHAR = 10
    UTINYINT = 11
    USMALLINT = 12
    UINT = 13
    UBIGINT = 14
    JSON = 15
    VARBINARY = 16
    DECIMAL = 17
    BINARY = 8
    GEOMETRY = 20
    DECIMAL64 = 21

    @staticmethod
    def get_type_prec(type: int):
        type_prec = [0, 1, 3, 5, 10, 19, 38, 38, 0, 19, 10, 3, 5, 10, 20, 0, 0, 0, 0, 0, 0, 0]
        return type_prec[type]

    @staticmethod
    def get_type_str(type: int):
        type_str = [
            "",
            "BOOL",
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            "VARCHAR",
            "TIMESTAMP",
            "NCHAR",
            "TINYINT UNSIGNED",
            "SMALLINT UNSIGNED",
            "INT UNSIGNED",
            "BIGINT UNSIGNED",
            "JSON",
            "VARBINARY",
            "DECIMAL",
            "",
            "",
            "GEOMETRY",
            "DECIMAL",
        ]
        return type_str[type]


class DataType:
    def __init__(self, type: int, length: int = 0, type_mod: int = 0):
        self.type: int = type
        self.length = length
        self.type_mod = type_mod

    def __str__(self):
        if self.type_mod != 0:
            return f"{TypeEnum.get_type_str(self.type)}({self.prec()}, {self.scale()})"
        if self.length:
            return f"{TypeEnum.get_type_str(self.type)}({self.length})"
        return TypeEnum.get_type_str(self.type)

    def __eq__(self, other):
        return self.type == other.type and self.length == other.length and self.type_mod == other.type_mod

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.type, self.length))

    def __repr__(self):
        return f"DataType({self.type}, {self.length}, {self.type_mod})"

    def is_decimal_type(self):
        return self.type == TypeEnum.DECIMAL or self.type == TypeEnum.DECIMAL64

    def is_varchar_type(self):
        return self.type == TypeEnum.VARCHAR or self.type == TypeEnum.NCHAR or self.type == TypeEnum.VARBINARY or self.type == TypeEnum.JSON or self.type == TypeEnum.BINARY

    def is_real_type(self):
        return self.type == TypeEnum.FLOAT or self.type == TypeEnum.DOUBLE

    def prec(self):
        return 0

    def scale(self):
        return 0

    ## TODO generate NULL, None
    def generate_value(self) -> str:
        if random.randint(0, 100000) < null_ratio * 100000:
            return 'NULL'
        if self.type == TypeEnum.BOOL:
            return ['true', 'false'][secrets.randbelow(2)]
        if self.type == TypeEnum.TINYINT:
            return str(secrets.randbelow(256) - 128)
        if self.type == TypeEnum.SMALLINT:
            return str(secrets.randbelow(65536) - 32768)
        if self.type == TypeEnum.INT:
            return str(secrets.randbelow(4294967296) - 2147483648)
        if self.type == TypeEnum.BIGINT:
            return str(secrets.randbelow(9223372036854775808) - 4611686018427387904)
        if self.type == TypeEnum.FLOAT or self.type == TypeEnum.DOUBLE:
            return str(random.uniform(-1e10, 1e10))
        if (
            self.type == TypeEnum.VARCHAR
            or self.type == TypeEnum.NCHAR
            or self.type == TypeEnum.VARBINARY
        ):
            return f"'{str(random.uniform(-1e20, 1e20))[0:self.length]}'"
        if self.type == TypeEnum.TIMESTAMP:
            return str(secrets.randbelow(9223372036854775808))
        if self.type == TypeEnum.UTINYINT:
            return str(secrets.randbelow(256))
        if self.type == TypeEnum.USMALLINT:
            return str(secrets.randbelow(65536))
        if self.type == TypeEnum.UINT:
            return str(secrets.randbelow(4294967296))
        if self.type == TypeEnum.UBIGINT:
            return str(secrets.randbelow(9223372036854775808))
        if self.type == TypeEnum.JSON:
            return f'{{"key": "{secrets.token_urlsafe(10)}"}}'
        if self.type == TypeEnum.GEOMETRY:
            return "'POINT(1.0 1.0)'"
        raise Exception(f"unsupport type {self.type}")

    def generate_sized_val(self, prec: int, scale: int) -> str:
        weight = prec - scale
        if self.type == TypeEnum.BOOL:
            return ['true', 'false'][secrets.randbelow(2)]
        if self.type == TypeEnum.TINYINT or self.type == TypeEnum.SMALLINT or self.type == TypeEnum.INT or self.type == TypeEnum.BIGINT or self.type == TypeEnum.TIMESTAMP:
            return str(secrets.randbelow(10 * weight * 2) - 10 * weight)
        if self.type == TypeEnum.FLOAT or self.type == TypeEnum.DOUBLE:
            return str(random.uniform(-10 * weight, 10 * weight))
        if (
            self.type == TypeEnum.VARCHAR
            or self.type == TypeEnum.NCHAR
            or self.type == TypeEnum.VARBINARY
        ):
            return f"'{str(random.uniform(-(10 * weight + 1), 10 * weight - 1))}'"
        if self.type == TypeEnum.UTINYINT or self.type == TypeEnum.USMALLINT or self.type == TypeEnum.UINT or self.type == TypeEnum.UBIGINT:
            return str(secrets.randbelow(10 * weight * 2))
        if self.type == TypeEnum.JSON:
            return f'{{"key": "{secrets.token_urlsafe(10)}"}}'
        if self.type == TypeEnum.GEOMETRY:
            return "'POINT(1.0 1.0)'"
        raise Exception(f"unsupport type {self.type}")


    def check(self, values, offset: int):
        return True

    def get_typed_val_for_execute(self, val, const_col = False):
        if self.type == TypeEnum.DOUBLE:
            return float(val)
        elif self.type == TypeEnum.BOOL:
            if val == "true":
                return 1
            else:
                return 0
        elif self.type == TypeEnum.FLOAT:
            if const_col:
                val = float(str(numpy.float32(val)))
            else:
                val = float(numpy.float32(val))
        elif self.type == TypeEnum.DECIMAL or self.type == TypeEnum.DECIMAL64:
            return get_decimal(val, self.scale())
        elif isinstance(val, str):
            val = val.strip("'")
            if len(val) == 0:
                return 0
        return val

    def get_typed_val(self, val):
        if self.type == TypeEnum.FLOAT:
            return float(str(numpy.float32(val)))
        elif self.type == TypeEnum.DOUBLE:
            return float(val)
        return val

    @staticmethod
    def get_decimal_types() -> list:
        return [TypeEnum.DECIMAL64, TypeEnum.DECIMAL]

    @staticmethod
    def get_decimal_op_types()-> list:
        return [
            TypeEnum.BOOL,
            TypeEnum.TINYINT,
            TypeEnum.SMALLINT,
            TypeEnum.INT,
            TypeEnum.BIGINT,
            TypeEnum.FLOAT,
            TypeEnum.DOUBLE,
            TypeEnum.VARCHAR,
            TypeEnum.NCHAR,
            TypeEnum.UTINYINT,
            TypeEnum.USMALLINT,
            TypeEnum.UINT,
            TypeEnum.UBIGINT,
            TypeEnum.DECIMAL,
            TypeEnum.DECIMAL64,
        ]
    
    @staticmethod
    def generate_random_type_for(dt: int):
        if dt == TypeEnum.DECIMAL:
            prec = random.randint(1, DecimalType.DECIMAL_MAX_PRECISION)
            return DecimalType(dt, prec, random.randint(0, prec))
        elif dt == TypeEnum.DECIMAL64:
            prec = random.randint(1, DecimalType.DECIMAL64_MAX_PRECISION)
            return DecimalType(dt, prec, random.randint(0, prec))
        elif dt == TypeEnum.BINARY or dt == TypeEnum.VARCHAR:
            return DataType(dt, random.randint(16, 255), 0)
        else:
            return DataType(dt, 0, 0)

class DecimalType(DataType):
    DECIMAL_MAX_PRECISION = 38
    DECIMAL64_MAX_PRECISION = 18
    def __init__(self, type, precision: int, scale: int):
        self.precision_ = precision
        self.scale_ = scale
        if type == TypeEnum.DECIMAL64:
            bytes = 8
        else:
            bytes = 16
        super().__init__(type, bytes, self.get_decimal_type_mod())
        self.decimal_generator: DecimalStringRandomGenerator = DecimalStringRandomGenerator()
        self.generator_config: DecimalTypeGeneratorConfig = DecimalTypeGeneratorConfig()
        #self.generator_config.with_corner_case = False
        self.generator_config.prec = precision
        self.generator_config.scale = scale
        self.aggregator: DecimalColumnAggregator = DecimalColumnAggregator()
        self.values: List[str] = []

    def get_decimal_type_mod(self) -> int:
        return self.precision_ * 100 + self.scale()
    
    def set_prec(self, prec: int):
        self.precision_ = prec
        self.type_mod = self.get_decimal_type_mod()
    
    def set_scale(self, scale: int):
        self.scale_ = scale
        self.type_mod = self.get_decimal_type_mod()

    def prec(self):
        return self.precision_

    def scale(self):
        return self.scale_

    def __str__(self):
        return f"DECIMAL({self.precision_}, {self.scale()})"

    def __eq__(self, other: DataType):
        return self.precision_ == other.prec() and self.scale() == other.scale()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.precision_, self.scale()))

    def __repr__(self):
        return f"DecimalType({self.precision_}, {self.scale()})"

    def generate_value(self) -> str:
        val = self.decimal_generator.generate(self.generator_config)
        self.aggregator.add_value(val, self.scale()) ## convert to Decimal first
        # self.values.append(val)  ## save it into files maybe
        return val

    def get_typed_val(self, val):
        if val == "NULL":
            return None
        return get_decimal(val, self.scale())
    
    def get_typed_val_for_execute(self, val, const_col = False):
        return self.get_typed_val(val)

    @staticmethod
    def default_compression() -> str:
        return "zstd"

    @staticmethod
    def default_encode() -> str:
        return "disabled"

    def check(self, values, offset: int):
        val_from_query = values
        val_insert = self.values[offset:]
        for v_from_query, v_from_insert in zip(val_from_query, val_insert):
            if v_from_insert == "NULL":
                if v_from_query.strip() != "NULL":
                    tdLog.debug(
                        f"val_insert: {val_insert} val_from_query: {val_from_query}"
                    )
                    tdLog.exit(f"insert NULL, query not NULL: {v_from_query}")
                else:
                    continue
            try:
                dec_query: Decimal = Decimal(v_from_query)
                dec_insert: Decimal = Decimal(v_from_insert)
                dec_insert = get_decimal(dec_insert, self.scale())
            except Exception as e:
                tdLog.exit(f"failed to convert {v_from_query} or {v_from_insert} to decimal, {e}")
                return False
            if dec_query != dec_insert:
                tdLog.exit(
                    f"check decimal column failed for insert: {v_from_insert}, query: {v_from_query}, expect {dec_insert}, but get {dec_query}"
                )
                return False
            else:
                tdLog.debug(
                    f"check decimal succ, insert:{v_from_insert} query:{v_from_query}, py dec: {dec_insert}"
                )


    @staticmethod
    def decimal_type_from_other_type(other: DataType):
        prec = 0
        return DecimalType(other.type, other.length, other.type_mod)

class Column:
    def __init__(self, type: DataType):
        self.type_: DataType = type
        self.name_: str = ""
        self.saved_vals:dict[str:[]] = {}

    def is_constant_col(self):
        return '' in self.saved_vals.keys()

    def get_typed_val(self, val):
        return self.type_.get_typed_val(val)

    def get_typed_val_for_execute(self, val, const_col = False):
        return self.type_.get_typed_val_for_execute(val, const_col)

    def get_constant_val(self):
        return self.get_typed_val(self.saved_vals[''][0])

    def get_constant_val_for_execute(self):
        return self.get_typed_val_for_execute(self.saved_vals[''][0], const_col=True)

    def __str__(self):
        if self.is_constant_col():
            return str(self.get_constant_val())
        return self.name_

    def get_val_for_execute(self, tbname: str, idx: int):
        if self.is_constant_col():
            return self.get_constant_val_for_execute()
        if len(self.saved_vals) > 1:
            for key in self.saved_vals.keys():
                l = len(self.saved_vals[key])
                if idx < l:
                    return self.get_typed_val_for_execute(self.saved_vals[key][idx])
                else:
                    idx -= l
        return self.get_typed_val_for_execute(self.saved_vals[tbname][idx])

    def get_cardinality(self, tbname):
        if self.is_constant_col():
            return 1
        elif len(self.saved_vals) > 1:
            return len(self.saved_vals['t0'])
        else:
            return len(self.saved_vals[tbname])
    
    def seq_scan_col(self, tbname: str, idx: int):
        if self.is_constant_col():
            return self.get_constant_val_for_execute(), False
        elif len(self.saved_vals) > 1:
            keys = list(self.saved_vals.keys())
            for i, key in enumerate(keys):
                l = len(self.saved_vals[key])
                if idx < l:
                    return self.get_typed_val_for_execute(self.saved_vals[key][idx]), True
                else:
                    idx -= l

            return 1, False
        else:
            if idx > len(self.saved_vals[tbname]) - 1:
                return 1, False
            v = self.get_typed_val_for_execute(self.saved_vals[tbname][idx])
            return v, True

    @staticmethod
    def comp_key(key1, key2):
        if key1 is None:
            return -1
        if key2 is None:
            return 1
        return key1 - key2

    def get_ordered_result(self, tbname: str, asc: bool) -> list:
        if tbname in self.saved_vals:
            return sorted(
                [
                    get_decimal(val, self.type_.scale())
                    for val in self.saved_vals[tbname]
                ],
                reverse=not asc,
                key=cmp_to_key(Column.comp_key)
            )
        else:
            res = []
            for val in self.saved_vals.values():
                res.extend(val)
            return sorted(
                [get_decimal(val, self.type_.scale()) for val in res], reverse=not asc,
                key=cmp_to_key(Column.comp_key)
            )
    
    def get_group_num(self, tbname, ignore_null=False) -> int:
        if tbname in self.saved_vals:
            s = set(get_decimal(val, self.type_.scale()) for val in self.saved_vals[tbname])
            if ignore_null:
                s.remove(None)
            return len(s)
        else:
            res = set()
            for vals in self.saved_vals.values():
                for v in vals:
                    res.add(get_decimal(v, self.type_.scale()))
            if ignore_null:
                res.remove(None)
            return len(res)

    ## tbName: for normal table, pass the tbname, for child table, pass the child table name
    def generate_value(self, tbName: str = '', save: bool = True):
        val = self.type_.generate_value()
        if save:
            if tbName not in self.saved_vals:
                self.saved_vals[tbName] = []
            ## for constant columns, always replace the last val
            if self.is_constant_col():
                self.saved_vals[tbName] = [val]
            else:
                self.saved_vals[tbName].append(val)
        return val

    def get_type_str(self) -> str:
        return str(self.type_)

    def set_name(self, name: str):
        self.name_ = name

    def check(self, values, offset: int):
        return self.type_.check(values, offset)

    def construct_type_value(self, val: str):
        if (
            self.type_.type == TypeEnum.BINARY
            or self.type_.type == TypeEnum.VARCHAR
            or self.type_.type == TypeEnum.NCHAR
            or self.type_.type == TypeEnum.VARBINARY
            or self.type_.type == TypeEnum.JSON
        ):
            return f"'{val}'"
        else:
            return val

    @staticmethod
    def get_decimal_unsupported_types() -> list:
        return [
            TypeEnum.JSON,
            TypeEnum.GEOMETRY,
            TypeEnum.VARBINARY,
        ]

    @staticmethod
    def get_decimal_oper_const_cols() -> list:
        types_unable_to_be_const = [
            TypeEnum.TINYINT,
            TypeEnum.SMALLINT,
            TypeEnum.INT,
            TypeEnum.UINT,
            TypeEnum.USMALLINT,
            TypeEnum.UTINYINT,
            TypeEnum.UBIGINT,
        ]
        return Column.get_all_type_columns(
            Column.get_decimal_unsupported_types()
            + Column.get_decimal_types()
            + types_unable_to_be_const
        )

    @staticmethod
    def get_decimal_types() -> List:
        return [TypeEnum.DECIMAL, TypeEnum.DECIMAL64]

    @staticmethod
    def get_all_type_columns(types_to_exclude: List[TypeEnum] = []) -> List:
        all_types = [
            Column(DataType(TypeEnum.BOOL)),
            Column(DataType(TypeEnum.TINYINT)),
            Column(DataType(TypeEnum.SMALLINT)),
            Column(DataType(TypeEnum.INT)),
            Column(DataType(TypeEnum.BIGINT)),
            Column(DataType(TypeEnum.FLOAT)),
            Column(DataType(TypeEnum.DOUBLE)),
            Column(DataType(TypeEnum.VARCHAR, 255)),
            Column(DataType(TypeEnum.TIMESTAMP)),
            Column(DataType(TypeEnum.NCHAR, 255)),
            Column(DataType(TypeEnum.UTINYINT)),
            Column(DataType(TypeEnum.USMALLINT)),
            Column(DataType(TypeEnum.UINT)),
            Column(DataType(TypeEnum.UBIGINT)),
            Column(DataType(TypeEnum.JSON)),
            Column(DataType(TypeEnum.VARBINARY, 255)),
            Column(DecimalType(TypeEnum.DECIMAL, 38, 10)),
            Column(DataType(TypeEnum.BINARY, 255)),
            Column(DataType(TypeEnum.GEOMETRY, 10240)),
            Column(DecimalType(TypeEnum.DECIMAL64, 18, 4)),
        ]
        ret = []
        for c in all_types:
            found = False
            for type in types_to_exclude:
                if c.type_.type == type:
                    found = True
                    break
            if not found:
                ret.append(c)
        return ret


class DecimalColumnTableCreater:
    def __init__(
        self,
        conn,
        dbName: str,
        tbName: str,
        columns: List[Column],
        tags_cols: List[Column] = [],
        col_prefix: str = "c",
        tag_prefix: str = "t",
    ):
        self.conn = conn
        self.dbName = dbName
        self.tbName = tbName
        self.tags_cols = tags_cols
        self.columns: List[Column] = columns
        self.col_prefix = col_prefix
        self.tag_prefix = tag_prefix

    def create(self):
        if len(self.tags_cols) > 0:
            table = "stable"
        else:
            table = "table"
        sql = f"create {table} {self.dbName}.{self.tbName} (ts timestamp"
        for i, column in enumerate(self.columns):
            tbname = f"{self.col_prefix}{i+1}"
            sql += f", {tbname} {column.get_type_str()}"
            column.set_name(tbname)
        if self.tags_cols:
            sql += ") tags("
            for i, tag in enumerate(self.tags_cols):
                tagname = f"{self.tag_prefix}{i+1}"
                sql += f"{tagname} {tag.get_type_str()}"
                tag.set_name(tagname)
                if i != len(self.tags_cols) - 1:
                    sql += ", "
        sql += ")"
        self.conn.execute(sql, queryTimes=1)

    def create_child_table(
        self,
        ctbPrefix: str,
        ctbNum: int,
        tag_cols: List[Column],
        tag_values: List[str],
    ):
        for i in range(ctbNum):
            tbname = f"{ctbPrefix}{i}"
            sql = f"create table {self.dbName}.{tbname} using {self.dbName}.{self.tbName} tags("
            for j, tag in enumerate(tag_cols):
                sql += f"{tag.construct_type_value(tag_values[j])}"
                if j != len(tag_cols) - 1:
                    sql += ", "
            sql += ")"
            self.conn.execute(sql, queryTimes=1)


class TableInserter:
    def __init__(
        self,
        conn,
        dbName: str,
        tbName: str,
        columns: List[Column],
        tags_cols: List[Column] = [],
    ):
        self.conn: TDSql = conn
        self.dbName = dbName
        self.tbName = tbName
        self.tag_cols = tags_cols
        self.columns = columns

    def insert(self, rows: int, start_ts: int, step: int, flush_database: bool = False):
        step = random.randint(step / 2, step * 1.5)
        pre_insert = f"insert into {self.dbName}.{self.tbName} values"
        sql = pre_insert
        for i in range(rows):
            sql += f"({start_ts + i * step}"
            for column in self.columns:
                sql += f", {column.generate_value(self.tbName)}"
            sql += ")"
            if i != rows - 1:
                sql += ", "
            local_flush_database = i % 5000 == 0
            if len(sql) > 1000:
                # tdLog.debug(f"insert into with sql{sql}")
                if flush_database and local_flush_database:
                    self.conn.execute(f"flush database {self.dbName}", queryTimes=1)
                self.conn.execute(sql, queryTimes=1)
                sql = pre_insert
        if len(sql) > len(pre_insert):
            # tdLog.debug(f"insert into with sql{sql}")
            if flush_database:
                self.conn.execute(f"flush database {self.dbName}", queryTimes=1)
            self.conn.execute(sql, queryTimes=1)

class FillQueryGenerator:
    def __init__(self, dbname: str, tbname:str, columns: list, ts_start, ts_end):
        self.dbname = dbname
        self.tbname = tbname
        self.columns: list[Column] = columns
        self.ts_start: int = ts_start
        self.ts_end: int = ts_end
        self.interval = None
        self.select = None
        self.partition = None
        self.fill = None
        self.where = None
        self.where_start = None
        self.where_end = None
        self.window_count = None
    
    def is_partition(self) -> bool:
        return self.partition != ''
    
    def get_agg_funcs(self) ->list:
        res: list[str] = []
        funcs = ['avg', 'min', 'max', 'sum', 'count']
        decimal_funcs = ['min', 'max', 'count']
        for col in self.columns:
            if not col.type_.is_varchar_type():
                if col.type_.is_decimal_type():
                    #res.append(random.choice(decimal_funcs) + f'({col.name_})')
                    pass
                else:
                    res.append(random.choice(funcs) + f'({col.name_})')
        return res
   
    def generate_select_list(self):
        self.generate_partition_by()
        if self.select is None:
            select = '_wstart, '
            funcs = self.get_agg_funcs()
            if self.is_partition():
                funcs.append('tbname')
            select += ','.join(funcs)
            self.select = select
        return self.select
    
    def generate_partition_by(self):
        if self.partition is None:
            partition_by_tbname = random.randint(0, 10)
            if partition_by_tbname < 5 and self.generate_fill() != 'NULL_F':
                self.partition = 'partition by tbname'
            else:
                self.partition = ''
        return self.partition
    
    def generate_interval(self):
        if self.interval is None:
            self.interval = random.randint(1, 40000)
        return self.interval
    
    def generate_fill(self):
        fill = ['PREV', 'NEXT', 'NULL', 'LINEAR', 'NULL_F']
        if self.fill is None:
            self.fill = random.choice(fill)
        return self.fill

    def fix_where_out_of_interval_range(self):
        return
        if self.where_end < self.ts_start or self.where_start > self.ts_end:
            self.where = None
            self.generate_where()
    
    def generate_where(self):
        if self.where is None:
            where = 'ts '
            if self.tbname == 'meters':
                window_count = random.randint(1, 10000)
            else:
                window_count = random.randint(1, 50000)
            ts_start = self.ts_start - window_count * self.generate_interval() * 1.2
            ts_end = self.ts_end + window_count * self.generate_interval() * 1.2
            ts_start = random.randint(int(ts_start), int(ts_end))
            ts_end = ts_start + window_count * self.generate_interval()
            self.where_start = ts_start
            self.where_end = ts_end
            self.where = f'ts >= {ts_start} and ts < {ts_end}'
            self.window_count = window_count
            self.fix_where_out_of_interval_range()
        return self.where

    def desc(self, sql: str) -> str:
        if self.is_partition():
            return sql + ' desc'
        else:
            return sql + ' order by _wstart desc'
    
    def generate_extra(self):
        if self.is_partition():
            return 'order by tbname, _wstart'
        else:
            return ''
    
    def no_fill_sql(self)-> str:
        return f'select {self.generate_select_list()} from {self.dbname}.{self.tbname} WHERE {self.generate_where()} {self.generate_partition_by()} INTERVAL({self.generate_interval()}a) {self.generate_extra()}'

    def generate_sql(self)-> str:
        return f'select {self.generate_select_list()} FROM {self.dbname}.{self.tbname} WHERE {self.generate_where()} {self.generate_partition_by()} INTERVAL({self.generate_interval()}a) FILL({self.generate_fill()}) {self.generate_extra()}'

class FillResValidator:
    def __init__(self, fill_res, interval_res, desc_res, generator: FillQueryGenerator):
        self.fill_res = fill_res
        self.interval_res = interval_res
        self.desc_res = desc_res
        self.generator = generator
    
    def get_row(self, res, rowIdx):
        if len(res) > rowIdx:
            return res[rowIdx]
        return None
    
    def find_valid_val_for_col(self, res, fromRowIdx, forward: bool, colIdx):
        if forward:
            step = 1
            end = len(res)
        else:
            step = -1
            end = -1
        for i in range(fromRowIdx, end, step):
            if res[i][colIdx] is not None:
                return res[i][colIdx]
        return None
    
    def find_last_valid_rows(self, res, colNum):
        last_row = []
        rowNum = len(res)
        for colIdx in range(0, colNum):
            last_row.append(None)
            for rowIdx in range(rowNum-1, -1, -1):
                if res[rowIdx][colIdx] is not None:
                    last_row[colIdx] = res[rowIdx][colIdx]
                    break
        return last_row
    
    def validate_fill_prev_one_group(self, fill_res_one_group, interval_res_one_group, desc_res_one_group: List):
        if len(interval_res_one_group) == 0:
            if len(fill_res_one_group) != 0:
                tdLog.exit(f"interval got no res, fill should got not res too, but not: {len(fill_res_one_group)}")
            return
        if len(fill_res_one_group) != len(desc_res_one_group):
            tdLog.exit(f"fill res: {len(fill_res_one_group)} desc res: {len(desc_res_one_group)}")
        i = 0
        j = 0
        last_valid_row_val = self.find_last_valid_rows(interval_res_one_group, len(fill_res_one_group[0]))
        desc_res_one_group.reverse()
        while i < len(fill_res_one_group):
            fill_row = fill_res_one_group[i]
            desc_row = desc_res_one_group[i]
            fill_ts = fill_row[0]
            colNum = len(fill_row)
            if self.generator.is_partition():
                colNum -= 1
            interval_row = self.get_row(interval_res_one_group, j)
            if interval_row is None:
                ## from now on, all rows are generated by fill
                for colIdx in range(1, colNum):
                    if fill_row[colIdx] != last_valid_row_val[colIdx]:
                        tdLog.exit(f"1 got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {last_valid_row_val[colIdx]} got: {fill_row}")
                    if desc_row[colIdx] != last_valid_row_val[colIdx]:
                        tdLog.exit(f"2 got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {last_valid_row_val[colIdx]} got: {desc_row}")
                i += 1
            else:
                if fill_ts < interval_row[0]:
                    ## this row is generated by fill
                    for colIdx in range(1, colNum):
                        val = self.find_valid_val_for_col(interval_res_one_group, j-1, False, colIdx)
                        if val != fill_row[colIdx]:
                            tdLog.exit(f"3 got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {fill_row}")
                        if val != desc_row[colIdx]:
                            tdLog.exit(f"4 got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {desc_row}")
                    i += 1
                else:
                    ## this row is copied from interval, but NULL cols are generated by fill
                    for colIdx in range(1, colNum):
                        filled = False
                        if interval_row[colIdx] is None:
                            val = self.find_valid_val_for_col(interval_res_one_group, j, False, colIdx)
                            filled = True
                        else:
                            val = interval_row[colIdx]
                        if val != fill_row[colIdx]:
                            tdLog.exit(f"5 got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {fill_row}")
                        if filled and val != desc_row[colIdx]:
                            tdLog.exit(f"6 got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {desc_row}")
                    i += 1
                    j += 1

    def validate_fill_next_one_group(self, fill_res_one_group, interval_res_one_group, desc_res_one_group: List):
        if len(interval_res_one_group) == 0:
            if len(fill_res_one_group) != 0:
                tdLog.exit(f"interval got no res, fill should got not res too, but not: {len(fill_res_one_group)}")
            return
        if len(fill_res_one_group) != len(desc_res_one_group):
            tdLog.exit(f"fill res: {len(fill_res_one_group)} desc res: {len(desc_res_one_group)}")
        i = 0
        j = 0
        desc_res_one_group.reverse()
        while i < len(fill_res_one_group):
            fill_row = fill_res_one_group[i]
            desc_row = desc_res_one_group[i]
            colNum = len(fill_row)
            if self.generator.is_partition():
                colNum -= 1
            fill_ts = fill_row[0]
            interval_row = self.get_row(interval_res_one_group, j)
            if interval_row is None:
                ## from now on, all rows are generated by fill, fill next, so all should be None
                for colIdx in range(1, colNum):
                    if fill_row[colIdx] != None:
                        tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: None got: {fill_row[colIdx]}")
                    if desc_row[colIdx] != None:
                        tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: None got: {desc_row[colIdx]}")
                i += 1
            else:
                if fill_ts < interval_row[0]:
                    ## this row is generated by fill
                    for colIdx in range(1, colNum):
                        val = self.find_valid_val_for_col(interval_res_one_group, j, True, colIdx)
                        if val != fill_row[colIdx]:
                            tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {fill_row[colIdx]}")
                        if val != desc_row[colIdx]:
                            tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {desc_row[colIdx]}")
                    i += 1
                else:
                    ## this row is copied from interval, but NULL cols are generated by fill
                    for colIdx in range(1, colNum):
                        filled = False
                        if interval_row[colIdx] is None:
                            val = self.find_valid_val_for_col(interval_res_one_group, j + 1, True, colIdx)
                            filled = True
                        else:
                            val = interval_row[colIdx]
                        if val != fill_row[colIdx]:
                            tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {fill_row[colIdx]}")
                        if filled and val != desc_row[colIdx]:
                            tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {desc_row[colIdx]}")
                    i += 1
                    j += 1
    def get_one_group(self, res: List, rowIdx: int):
        if len(res) <= rowIdx:
            return None
        val = res[rowIdx][-1]
        if val is None and self.generator.fill != 'NULL_F':
            tdLog.exit(f"tbname is None {res}")
        for i in range(rowIdx+1, len(res)):
            if val != res[i][-1]:
                return (val, rowIdx, i)
        return (val, rowIdx, len(res))
    
    def split_groups(self, res: List)-> List:
        output = []
        idx = 0
        while True:
            tup = self.get_one_group(res, idx)
            if tup is None:
                break
            else:
                output.append(tup)
                idx = tup[2]
        return output
    
    def split_res_groups(self)-> List:
        res = []
        fill_groups = self.split_groups(self.fill_res)
        interval_groups = self.split_groups(self.interval_res)
        desc_groups: List = self.split_groups(self.desc_res)

        for i in range(0, len(fill_groups)):
            fill_tup = fill_groups[i]
            fill_rows = self.fill_res[fill_tup[1]: fill_tup[2]]
            if len(interval_groups) > 0:
                interval_tup = interval_groups[i]
                interval_rows = self.interval_res[interval_tup[1]: interval_tup[2]]
            else:
                interval_tup = (None, 0, 0)
                interval_rows = []
            desc_tup = desc_groups[i]
            desc_rows = self.desc_res[desc_tup[1]: desc_tup[2]]
            res.append((fill_rows, interval_rows, desc_rows))
        return res
    
    def validate_fill_prev(self):
        if not self.generator.is_partition():
            return self.validate_fill_prev_one_group(self.fill_res, self.interval_res, self.desc_res)
        else:
            groups = self.split_res_groups()
            for group in groups:
                self.validate_fill_prev_one_group(group[0], group[1], group[2])

    def validate_fill_next(self):
        if not self.generator.is_partition():
            return self.validate_fill_next_one_group(self.fill_res, self.interval_res, self.desc_res)
        else:
            groups = self.split_res_groups()
            for group in groups:
                self.validate_fill_next_one_group(group[0], group[1], group[2])
    
    def validate_fill_null_rows(self, fill_res_one_group, interval_res_one_group, desc_res_one_group: List, null_f: bool):
        if not null_f:
            if len(interval_res_one_group) == 0:
                if len(fill_res_one_group) != 0:
                    tdLog.exit(f"interval got no res, fill should got not res too, but not: {len(fill_res_one_group)}")
                return
            if len(fill_res_one_group) != len(desc_res_one_group):
                tdLog.exit(f"fill res: {len(fill_res_one_group)} desc res: {len(desc_res_one_group)}")
            return False
        else:
            if len(interval_res_one_group) == 0:
                if len(fill_res_one_group) == 0:
                    tdLog.exit("fill null_f got not res")
                for row in fill_res_one_group:
                    colIdx = len(row)
                    if self.generator.is_partition():
                        colIdx -= 1
                    for colIdx in range(1, colIdx):
                        if row[colIdx] != None:
                            tdLog.exit(f"got different val for fill_res NULL_F: {row} colIdx: {colIdx}, expect: None got: {row[colIdx]}")
                return True
            else:
                return False

    
    def validate_fill_null_one_group(self, fill_res_one_group, interval_res_one_group, desc_res_one_group, null_f: bool = False):
        if self.validate_fill_null_rows(fill_res_one_group, interval_res_one_group, desc_res_one_group, null_f):
            return
        i = 0
        j = 0
        desc_res_one_group.reverse()
        while i < len(fill_res_one_group):
            fill_row = fill_res_one_group[i]
            desc_row = desc_res_one_group[i]
            colNum = len(fill_row)
            if self.generator.is_partition():
                colNum -= 1
            fill_ts = fill_row[0]
            interval_row = self.get_row(interval_res_one_group, j)
            if interval_row is None:
                ## from now on, all rows are generated by fill, fill next, so all should be None
                for colIdx in range(1, colNum):
                    if fill_row[colIdx] != None:
                        tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: None got: {fill_row[colIdx]}")
                    if fill_row[colIdx] != desc_row[colIdx]:
                        tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {desc_row[colIdx]} got: {fill_row[colIdx]}")
                i += 1
            else:
                if fill_ts < interval_row[0]:
                    ## this row is generated by fill
                    for colIdx in range(1, colNum):
                        val = None
                        if val != fill_row[colIdx]:
                            tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {fill_row[colIdx]}")
                        if val != desc_row[colIdx]:
                            tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {desc_row[colIdx]} got: {fill_row[colIdx]}")
                    i += 1
                else:
                    ## this row is copied from interval
                    for colIdx in range(1, colNum):
                        val = interval_row[colIdx]
                        if val != fill_row[colIdx]:
                            tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {fill_row[colIdx]}")
                    i += 1
                    j += 1
    
    def validate_fill_NULL(self, null_f: bool = False):
        if not self.generator.is_partition():
            return self.validate_fill_null_one_group(self.fill_res, self.interval_res, self.desc_res, null_f)
        else:
            groups = self.split_res_groups()
            for group in groups:
                self.validate_fill_null_one_group(group[0], group[1], group[2], null_f)
    
    def calc_linear_val(self, interval_res_one_group, rowIdx, colIdx, curTs: datetime):
        prevRow = rowIdx - 1
        if prevRow < 0:
            return None
        prevRowVal = interval_res_one_group[prevRow][colIdx]
        curRowVal = interval_res_one_group[rowIdx][colIdx]
        if prevRowVal is None or curRowVal is None:
            return None
        prevRowTs: datetime = interval_res_one_group[prevRow][0]
        curRowTs:datetime = interval_res_one_group[rowIdx][0]
        if curRowTs == prevRowTs:
            raise Exception(f"{curRowTs} == {prevRowTs}")
        else:
            delta = (curRowVal - prevRowVal) / ((curRowTs - prevRowTs).total_seconds() * 1000)
            result = prevRowVal + delta * (curTs - prevRowTs).total_seconds() * 1000
            if isinstance(prevRowVal, int):
                if math.isclose(result, math.ceil(result), rel_tol=1e-6, abs_tol=1e-6) or math.isclose(result, math.floor(result), rel_tol=1e-6, abs_tol=1e-6):
                    result = round(result)
                else:
                    result =  int(result)
            #tdLog.debug(f"calc_linear_val: {prevRowVal} {curRowVal} {prevRowTs} {curRowTs} {delta} {result} deltats: {(curTs - prevRowTs).total_seconds() * 1000} {prevRowVal} + {delta} * {(curTs - prevRowTs).total_seconds() * 1000} = {result}")
            return result

    
    def validate_fill_linear_one_group(self, fill_res_one_group, interval_res_one_group, desc_res_one_group: List):
        if len(interval_res_one_group) == 0:
            if len(fill_res_one_group) != 0:
                tdLog.exit(f"interval got no res, fill should got not res too, but not: {len(fill_res_one_group)}")
            return
        if len(fill_res_one_group) != len(desc_res_one_group):
            tdLog.exit(f"fill res: {len(fill_res_one_group)} desc res: {len(desc_res_one_group)}")
        i = 0
        j = 0
        desc_res_one_group.reverse()
        while i < len(fill_res_one_group):
            fill_row = fill_res_one_group[i]
            desc_row = desc_res_one_group[i]
            fill_ts = fill_row[0]
            colNum = len(fill_row)
            if self.generator.is_partition():
                colNum -= 1
            interval_row = self.get_row(interval_res_one_group, j)
            if interval_row is None:
                ## from now on, all rows are generated by fill linear, since there is no data in interval output, all rows should be None
                val = None
                for colIdx in range(1, colNum):
                    if fill_row[colIdx] != val:
                        tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {fill_row[colIdx]}")
                    if desc_row[colIdx] != val:
                        tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {desc_row[colIdx]}")
                i += 1
            else:
                if fill_ts < interval_row[0]:
                    ## this row is generated by fill
                    for colIdx in range(1, colNum):

                        val = self.calc_linear_val(interval_res_one_group, j, colIdx, fill_ts)
                        if val is None:
                            if fill_row[colIdx] is not None or desc_row[colIdx] is not None:
                                tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {fill_row[colIdx]}")
                        else:
                            pass ## skip check linear val, only check None values
                            #if not math.isclose(val, fill_row[colIdx], rel_tol=1e-5, abs_tol=1e-6):
                                #tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {fill_row[colIdx]}")
                            #if not math.isclose(val, desc_row[colIdx], rel_tol=1e-5, abs_tol=1e-6):
                                #tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {desc_row[colIdx]}")
                    i += 1
                else:
                    ## this row is copied from interval
                    for colIdx in range(1, colNum):
                        filled = False
                        val = interval_row[colIdx]
                        if val != fill_row[colIdx]:
                            tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {fill_row[colIdx]}")
                        if filled and val != desc_row[colIdx]:
                            tdLog.exit(f"got different val for fill_res: {fill_row} rowIdx: {i} colIdx: {colIdx}, expect: {val} got: {desc_row[colIdx]}")
                    i += 1
                    j += 1
        
    def validate_fill_linear(self):
        if not self.generator.is_partition():
            return self.validate_fill_linear_one_group(self.fill_res, self.interval_res, self.desc_res)
        else:
            groups = self.split_res_groups()
            for group in groups:
                self.validate_fill_linear_one_group(group[0], group[1], group[2])
    
    def validate(self):
        if self.generator.fill == 'PREV':
            return self.validate_fill_prev()
        elif self.generator.fill == 'NEXT':
            return self.validate_fill_next()
        elif self.generator.fill == 'NULL':
            return self.validate_fill_NULL()
        elif self.generator.fill == 'LINEAR':
            return self.validate_fill_linear()
        elif self.generator.fill == 'NULL_F':
            return self.validate_fill_NULL(True)
        else:
            return True
        

class TDTestCase:
    updatecfgDict = {
        "asynclog": 0,
        "ttlUnit": 1,
        "ttlPushInterval": 5,
        "ratioOfVnodeStreamThrea": 4,
        "debugFlag": 143,
    }

    def __init__(self):
        self.vgroups = 4
        self.ctbNum = 10
        self.rowsPerTbl = 10000
        self.duraion = "1h"
        self.norm_tb_columns = []
        self.tags = []
        self.stable_name = "meters"
        self.norm_table_name = "nt"
        self.col_prefix = "c"
        self.c_table_prefix = "t"
        self.tag_name_prefix = "t"
        self.db_name = "test"
        self.c_table_num = 10
        self.no_decimal_col_tb_name = "tt"
        self.stb_columns = []
        self.stream_name = "stream1"
        self.stream_out_stb = "stream_out_stb"
        self.tsma_name = "tsma1"
        self.query_test_round = 10000

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)

    def test_decimal_column_ddl(self):
        ## create decimal type table, normal/super table, decimal64/decimal128
        tdLog.printNoPrefix("-------- test create columns")
        self.norm_tb_columns: List[Column] = [
            Column(DecimalType(TypeEnum.DECIMAL, 10, 2)),
            Column(DecimalType(TypeEnum.DECIMAL, 20, 4)),
            Column(DecimalType(TypeEnum.DECIMAL, 30, 8)),
            Column(DecimalType(TypeEnum.DECIMAL, 38, 10)),
            Column(DataType(TypeEnum.TINYINT)),
            Column(DataType(TypeEnum.INT)),
            Column(DataType(TypeEnum.BIGINT)),
            Column(DataType(TypeEnum.DOUBLE)),
            Column(DataType(TypeEnum.FLOAT)),
            Column(DataType(TypeEnum.VARCHAR, 255)),
        ]
        self.tags: List[Column] = [
            Column(DataType(TypeEnum.INT)),
            Column(DataType(TypeEnum.VARCHAR, 255)),
        ]
        self.stb_columns: List[Column] = [
            Column(DecimalType(TypeEnum.DECIMAL, 10, 2)),
            Column(DecimalType(TypeEnum.DECIMAL, 20, 4)),
            Column(DecimalType(TypeEnum.DECIMAL, 30, 8)),
            Column(DecimalType(TypeEnum.DECIMAL, 38, 10)),
            Column(DataType(TypeEnum.TINYINT)),
            Column(DataType(TypeEnum.INT)),
            Column(DataType(TypeEnum.BIGINT)),
            Column(DataType(TypeEnum.DOUBLE)),
            Column(DataType(TypeEnum.FLOAT)),
            Column(DataType(TypeEnum.VARCHAR, 255)),
        ]
        DecimalColumnTableCreater(
            tdSql,
            self.db_name,
            self.stable_name,
            self.stb_columns,
            self.tags,
            col_prefix=self.col_prefix,
            tag_prefix=self.tag_name_prefix,
        ).create()

        DecimalColumnTableCreater(
            tdSql, self.db_name, self.norm_table_name, self.norm_tb_columns
        ).create()

        ## TODO add more values for all rows
        tag_values = ["1", "t1"]
        DecimalColumnTableCreater(
            tdSql, self.db_name, self.stable_name, self.stb_columns
        ).create_child_table(
            self.c_table_prefix, self.c_table_num, self.tags, tag_values
        )

    def test_insert_decimal_values(self):
        tdLog.debug("start to insert values")
        for i in range(self.c_table_num):
            TableInserter(
                tdSql,
                self.db_name,
                f"{self.c_table_prefix}{i}",
                self.stb_columns,
                self.tags,
            ).insert(tb_insert_rows, 1537146000000, 5000)

        TableInserter(
            tdSql, self.db_name, self.norm_table_name, self.norm_tb_columns
        ).insert(tb_insert_rows, 1537146000000, 5000, flush_database=True)
        tdSql.execute("flush database %s" % (self.db_name), queryTimes=1)

    def test_decimal_ddl(self):
        tdSql.execute("create database test cachemodel 'both'", queryTimes=1)
        self.test_decimal_column_ddl()

    def run(self):
        self.test_decimal_ddl()
        self.test_insert_decimal_values()
        self.test_fill(self.db_name, self.norm_table_name, self.norm_tb_columns)
        self.test_fill(self.db_name, self.stable_name, self.stb_columns)

    def get_first_last_ts(self, dbname, tbname):
        sql = f'select cast(first(ts) as bigint), cast(last(ts) as bigint) from {dbname}.{tbname}'
        tdSql.query(sql, queryTimes=1)
        first: int = tdSql.queryResult[0][0]
        last: int = tdSql.queryResult[0][1]
        return (first, last)

    def test_fill(self, dbname, tbname, cols):
        (first, last) = self.get_first_last_ts(dbname, tbname)
        for _ in range(test_round):
            sql_generator = FillQueryGenerator(dbname, tbname, cols, first, last)
            sql = sql_generator.generate_sql()
            interval_sql = sql_generator.no_fill_sql()
            tdSql.query(interval_sql, queryTimes=1)
            interval_res = tdSql.queryResult
            tdLog.debug(sql)
            tdSql.query(sql, queryTimes=1)
            fill_res = tdSql.queryResult
            desc_sql = sql_generator.desc(sql)
            tdSql.query(desc_sql, queryTimes=1)
            desc_res = tdSql.queryResult
            FillResValidator(fill_res, interval_res, desc_res, sql_generator).validate()
            tdLog.debug(f"validate fill res for {sql} success got fill rows: {len(fill_res)}")
    
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

    def wait_query_result(self, sql: str, expect_result, times):
        for i in range(times):
            tdLog.info(f"wait query result for {sql}, times: {i}")
            tdSql.query(sql, queryTimes=1)
            results = tdSql.queryResult
            if results != expect_result:
                time.sleep(1)
                continue
            return True
        tdLog.exit(
            f"wait query result timeout for {sql} failed after {times} time, expect {expect_result}, but got {results}"
        )
    
    def wait_query_at_least_rows(self, sql: str, rows, wait_times):
        for i in range(wait_times):
            tdLog.info(f"wait query rows at least for {sql}, times: {i}")
            tdSql.query(sql, queryTimes=1, show=True)
            results = tdSql.queryResult
            if len(results) < rows:
                time.sleep(1)
                continue
            return True
        tdLog.exit(
            f"wait query rows at least for {sql} failed after {wait_times} times, expect at least {rows} rows, but got {len(results)} rows"
        )

    def run_in_thread(self, times, func, params) -> threading.Thread:
        threads: List[threading.Thread] = []
        for i in range(times):
            t = threading.Thread(target=func, args=params)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

    def run_in_thread2(self, func, params) -> threading.Thread:
        t = threading.Thread(target=func, args=params)
        t.start()
        return t
    
    def test_query_decimal_interval_fill(self):
        pass

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
