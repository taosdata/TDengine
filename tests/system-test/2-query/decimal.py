from ast import Tuple
from pydoc import doc
from random import randrange
from re import A
import time
import threading
import secrets

from regex import D, F
from sympy import Dict, true
from torch import is_conj
import query
from tag_lite import column
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from decimal import *

getcontext().prec = 40

syntax_error = -2147473920
invalid_column = -2147473918
invalid_compress_level = -2147483084
invalid_encode_param = -2147483087
invalid_operation = -2147483136
scalar_convert_err = -2147470768

class DecimalTypeGeneratorConfig:
    def __init__(self):
        self.enable_weight_overflow: bool = False
        self.weightOverflowRatio: float = 0.001
        self.enable_scale_overflow: bool = True
        self.scale_overflow_ratio = 0.1
        self.enable_positive_sign = False
        self.with_corner_case = True
        self.corner_case_ratio = 0.1
        self.positive_ratio = 0.7
        self.prec = 38
        self.scale = 10


class DecimalStringRandomGenerator:
    def __init__(self):
        self.corner_cases = ["0", "NULL", "0.", ".0", "000.000000"]
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

    def add_value(self, value: str):
        self.count += 1
        if value == "NULL":
            self.null_num += 1
        elif value == "None":
            self.none_num += 1
        else:
            v: Decimal = Decimal(value)
            self.sum += v
            if v > self.max:
                self.max = v
            if v < self.min:
                self.min = v


class TaosShell:
    def __init__(self):
        self.queryResult = []
        self.tmp_file_path = "/tmp/taos_shell_result"

    def read_result(self):
        with open(self.tmp_file_path, "r") as f:
            lines = f.readlines()
            lines = lines[1:]
            for line in lines:
                col = 0
                vals: List[str] = line.split(",")
                if len(self.queryResult) == 0:
                    self.queryResult = [[] for i in range(len(vals))]
                for val in vals:
                    self.queryResult[col].append(val.strip())
                    col += 1

    def query(self, sql: str):
        with open(self.tmp_file_path, "r+") as f:
            f.truncate(0)
        try:
            command = f'taos -s "{sql} >> {self.tmp_file_path}"'
            result = subprocess.run(
                command, shell=True, check=True, stderr=subprocess.PIPE
            )
            self.read_result()
        except Exception as e:
            tdLog.exit(f"Command '{sql}' failed with error: {e.stderr.decode('utf-8')}")
            self.queryResult = []
        return self.queryResult

class DecimalColumnExpr:
    def __init__(self, format: str, executor):
        self.format_: str = format
        self.executor_ = executor
        self.params_: Tuple = List

    def __str__(self):
        return self.format_ % (self.params_)

    def execute(self, params):
        return self.executor_(params)
    
    def get_val(self, tbname: str, idx: int):
        params: Tuple = ()
        for p in self.params_:
            params = params + (p.get_val(tbname, idx),)
        return self.execute(params)
    
    def check(self, query_col_res: List, tbname: str):
        for i in range(len(query_col_res)):
            v_from_query = query_col_res[i]
            params: Tuple = ()
            for p in self.params_:
                if isinstance(p, Column) or isinstance(p, DecimalColumnExpr):
                    p = p.get_val(tbname, i)
                params = params + (p,)
            v_from_calc_in_py = self.execute(params)

            if v_from_calc_in_py == 'NULL' or v_from_query == 'NULL':
                if v_from_calc_in_py != v_from_query:
                    tdLog.exit(f"query with expr: {self} calc in py got: {v_from_calc_in_py}, query got: {v_from_query}")
                tdLog.debug(f"query with expr: {self} calc got same result: NULL")
                continue

            if isinstance(v_from_calc_in_py, float):
                dec_from_query = float(v_from_query)
                dec_from_insert = float(v_from_calc_in_py)
            else:
                dec_from_query = Decimal(v_from_query)
                dec_from_insert = Decimal(v_from_calc_in_py)
            if dec_from_query != dec_from_insert:
                tdLog.exit(
                    f"check decimal column failed for expr: {self} params: {params}, query: {v_from_query}, expect {dec_from_insert}, but get {dec_from_query}")
            else:
                tdLog.debug(
                    f"check decimal succ for expr: {self}, params: {params}, insert:{v_from_calc_in_py} query:{v_from_query}, py dec: {dec_from_insert}"
                )


    def generate(self, format_params) -> str:
        self.params_ = format_params
        return f"({self.format_})".format(*format_params)


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
    def get_type_str(type: int):
        if type == TypeEnum.BOOL:
            return "BOOL"
        elif type == TypeEnum.TINYINT:
            return "TINYINT"
        elif type == TypeEnum.SMALLINT:
            return "SMALLINT"
        elif type == TypeEnum.INT:
            return "INT"
        elif type == TypeEnum.BIGINT:
            return "BIGINT"
        elif type == TypeEnum.FLOAT:
            return "FLOAT"
        elif type == TypeEnum.DOUBLE:
            return "DOUBLE"
        elif type == TypeEnum.VARCHAR:
            return "VARCHAR"
        elif type == TypeEnum.TIMESTAMP:
            return "TIMESTAMP"
        elif type == TypeEnum.NCHAR:
            return "NCHAR"
        elif type == TypeEnum.UTINYINT:
            return "TINYINT UNSIGNED"
        elif type == TypeEnum.USMALLINT:
            return "SMALLINT UNSIGNED"
        elif type == TypeEnum.UINT:
            return "INT UNSIGNED"
        elif type == TypeEnum.UBIGINT:
            return "BIGINT UNSIGNED"
        elif type == TypeEnum.JSON:
            return "JSON"
        elif type == TypeEnum.VARBINARY:
            return "VARBINARY"
        elif type == TypeEnum.DECIMAL:
            return "DECIMAL"
        elif type == TypeEnum.BINARY:
            return "BINARY"
        elif type == TypeEnum.GEOMETRY:
            return "GEOMETRY"
        elif type == TypeEnum.DECIMAL64:
            return "DECIMAL"
        else:
            raise Exception("unknow type")


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
        return self.type == other.type and self.length == other.length

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.type, self.length))

    def __repr__(self):
        return f"DataType({self.type}, {self.length}, {self.type_mod})"

    def is_decimal_type(self):
        return self.type == TypeEnum.DECIMAL or self.type == TypeEnum.DECIMAL64

    def prec(self):
        return 0

    def scale(self):
        return 0

    ## TODO generate NULL, None
    def generate_value(self) -> str:
        if self.type == TypeEnum.BOOL:
            return str(secrets.randbelow(2))
        if self.type == TypeEnum.TINYINT:
            return str(secrets.randbelow(256) - 128)
        if self.type == TypeEnum.SMALLINT:
            return str(secrets.randbelow(65536) - 32768)
        if self.type == TypeEnum.INT:
            return str(secrets.randbelow(4294967296) - 2147483648)
        if self.type == TypeEnum.BIGINT:
            return str(secrets.randbelow(9223372036854775808) - 4611686018427387904)
        if self.type == TypeEnum.FLOAT or self.type == TypeEnum.DOUBLE:
            return str(random.random())
        if (
            self.type == TypeEnum.VARCHAR
            or self.type == TypeEnum.NCHAR
            or self.type == TypeEnum.VARBINARY
        ):
            return f"'{secrets.token_urlsafe(random.randint(0, self.length))[0:random.randint(0, self.length)]}'"
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

    def check(self, values, offset: int):
        return True
    
    def get_typed_val(self, val):
        if self.type == TypeEnum.FLOAT or self.type == TypeEnum.DOUBLE:
            return float(val)
        return val

class DecimalType(DataType):
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
        self.generator_config.prec = precision
        self.generator_config.scale = scale
        self.aggregator: DecimalColumnAggregator = DecimalColumnAggregator()
        self.values: List[str] = []

    def get_decimal_type_mod(self) -> int:
        return self.precision_ * 100 + self.scale()

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
        self.aggregator.add_value(val) ## convert to Decimal first
        # self.values.append(val)  ## save it into files maybe
        return val

    def get_typed_val(self, val):
        if val == "NULL":
            return None
        return Decimal(val).quantize(Decimal("1." + "0" * self.scale()), ROUND_HALF_UP)

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
                dec_insert = dec_insert.quantize(Decimal("1." + "0" * self.scale()), ROUND_HALF_UP)
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


class Column:
    def __init__(self, type: DataType):
        self.type_: DataType = type
        self.name_: str = ""
        self.saved_vals:dict[str:[]] = {}

    def is_constant_col(self):
        return '' in self.saved_vals.keys()
    
    def get_typed_val(self, val):
        return self.type_.get_typed_val(val)
    
    def get_constant_val(self):
        return self.get_typed_val(self.saved_vals[''][0])
    
    def __str__(self):
        if self.is_constant_col():
            return self.get_constant_val()
        return self.name_
    
    def get_val(self, tbname: str, idx: int):
        if self.is_constant_col():
            return self.get_constant_val()
        return self.get_typed_val(self.saved_vals[tbname][idx])

    ## tbName: for normal table, pass the tbname, for child table, pass the child table name
    def generate_value(self, tbName: str = '', save: bool = True):
        val = self.type_.generate_value()
        if save:
            if tbName not in self.saved_vals:
                self.saved_vals[tbName] = []
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
        for c in all_types:
            for type in types_to_exclude:
                if c.type_.type == type:
                    all_types.remove(c)
        return all_types


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


class TableDataValidator:
    def __init__(self, columns: List[Column], tbName: str, dbName: str, tbIdx: int = 0):
        self.columns = columns
        self.tbName = tbName
        self.dbName = dbName
        self.tbIdx = tbIdx

    def validate(self):
        sql = f"select * from {self.dbName}.{self.tbName}"
        res = TaosShell().query(sql)
        row_num = len(res[1])
        colIdx = 1
        for col in self.columns:
            if (
                col.type_.type == TypeEnum.DECIMAL
                or col.type_.type == TypeEnum.DECIMAL64
            ):
                col.check(res[colIdx], row_num * self.tbIdx)
            colIdx += 1


class DecimalBinaryOperator(DecimalColumnExpr):
    def __init__(self, op: str):
        super().__init__()
        self.op_ = op

    def __str__(self):
        return self.op_

    def generate(self):
        pass

    @staticmethod
    def execute_plus(params):
        if params[0] is None or params[1] is None:
            return 'NULL'
        if isinstance(params[0], float) or isinstance(params[1], float):
            return float(params[0]) + float(params[1])
        return Decimal(params[0]) + Decimal(params[1])

    @staticmethod
    def execute_minus(params):
        return params[0] - params[1]

    @staticmethod
    def execute_mul(params):
        return params[0] * params[1]

    @staticmethod
    def execute_div(params):
        return params[0] / params[1]

    @staticmethod
    def execute_mod(params):
        return params[0] % params[1]

    @staticmethod
    def execute_eq(params):
        return params[0] == params[1]

    @staticmethod
    def execute_ne(params):
        return params[0] != params[1]

    @staticmethod
    def execute_gt(params):
        return params[0] > params[1]

    @staticmethod
    def execute_lt(params):
        return params[0] < params[1]

    @staticmethod
    def execute_ge(params):
        return params[0] >= params[1]

    @staticmethod
    def execute_le(params):
        return params[0] <= params[1]

    @staticmethod
    def get_all_binary_ops() -> List[DecimalColumnExpr]:
        return [
            DecimalColumnExpr(" {0} + {1} ", DecimalBinaryOperator.execute_plus),
            DecimalColumnExpr(" {0} - {1} ", DecimalBinaryOperator.execute_minus),
            DecimalColumnExpr(" {0} * {1} ", DecimalBinaryOperator.execute_mul),
            DecimalColumnExpr(" {0} / {1} ", DecimalBinaryOperator.execute_div),
            DecimalColumnExpr(" {0} % {1} ", DecimalBinaryOperator.execute_mod),
            DecimalColumnExpr(" {0} == {1} ", DecimalBinaryOperator.execute_eq),
            DecimalColumnExpr(" {0} != {1} ", DecimalBinaryOperator.execute_ne),
            DecimalColumnExpr(" {0} > {1} ", DecimalBinaryOperator.execute_gt),
            DecimalColumnExpr(" {0} < {1} ", DecimalBinaryOperator.execute_lt),
            DecimalColumnExpr(" {0} >= {1} ", DecimalBinaryOperator.execute_ge),
            DecimalColumnExpr(" {0} <= {1} ", DecimalBinaryOperator.execute_le),
        ]

    def execute(self, left, right):
        if self.op_ == "+":
            return left + right
        if self.op_ == "-":
            return left - right
        if self.op_ == "*":
            return left * right
        if self.op_ == "/":
            return left / right
        if self.op_ == "%":
            return left % right
        if self.op_ == "==":
            return left == right
        if self.op_ == "!=":
            return left != right
        if self.op_ == ">":
            return left > right
        if self.op_ == "<":
            return left < right
        if self.op_ == ">=":
            return left >= right
        if self.op_ == "<=":
            return left <= right
        raise Exception(f"unsupport operator {self.op_}")


class DecimalBinaryOperatorIn(DecimalBinaryOperator):
    def __init__(self, op: str):
        super().__init__(op)

    def execute(self, left, right):
        if self.op_.lower()() == "in":
            return left in right
        if self.op_.lower() == "not in":
            return left not in right


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

    def check_desc_for_one_ctb(
        self, ctbPrefix: str, columns: List[Column], tags: List[Column] = []
    ):
        ctb_idx = randrange(self.c_table_num)
        return self.check_desc(f"{ctbPrefix}{ctb_idx}", columns, tags)

    def check_desc(self, tbname: str, columns: List[Column], tags: List[Column] = []):
        sql = f"desc {self.db_name}.{tbname}"
        tdSql.query(sql, queryTimes=1)
        results = tdSql.queryResult
        for i, col in enumerate(columns):
            if col.type_.type == TypeEnum.DECIMAL:
                if results[i + 1][1] != col.type_.__str__():
                    tdLog.info(str(results))
                    tdLog.exit(
                        f"check desc failed for table: {tbname} column {results[i+1][0]} type is {results[i+1][1]}, expect DECIMAL"
                    )
                if results[i + 1][4] != DecimalType.default_encode():
                    tdLog.exit(
                        f"check desc failed for table: {tbname} column {results[i+1][0]} encode is {results[i+1][5]}, expect {DecimalType.default_encode()}"
                    )
                if results[i + 1][5] != DecimalType.default_compression():
                    tdLog.exit(
                        f"check desc failed for table: {tbname} column {results[i+1][0]} compression is {results[i+1][4]}, expect {DecimalType.default_compression()}"
                    )
        if tbname == self.stable_name:
            self.check_desc_for_one_ctb(self.c_table_prefix, columns, tags)

    def check_show_create_table(
        self, tbname: str, cols: List[Column], tags: List[Column] = []
    ):
        sql = f"show create table {self.db_name}.{tbname}"
        tdSql.query(sql, queryTimes=1)
        create_table_sql = tdSql.queryResult[0][1]
        decimal_idx = 0
        results = re.findall(r"DECIMAL\((\d+),(\d+)\)", create_table_sql)
        for i, col in enumerate(cols):
            if (
                col.type_.type == TypeEnum.DECIMAL
                or col.type_.type == TypeEnum.DECIMAL64
            ):
                result_type = DecimalType(
                    col.type_.type,
                    int(results[decimal_idx][0]),
                    int(results[decimal_idx][1]),
                )
                if result_type != col.type_:
                    tdLog.exit(
                        f"check show create table failed for: {tbname} column {i} type is {result_type}, expect {col.type}"
                    )
                decimal_idx += 1

    def test_add_drop_columns_with_decimal(self, tbname: str, columns: List[Column]):
        is_stb = tbname == self.stable_name
        ## alter table add column
        create_c99_sql = (
            f"alter table {self.db_name}.{tbname} add column c99 decimal(37, 19)"
        )
        columns.append(Column(DecimalType(TypeEnum.DECIMAL, 37, 19)))
        tdSql.execute(create_c99_sql, queryTimes=1, show=True)
        self.check_desc(tbname, columns)
        ## alter table add column with compression
        create_c100_sql = f'ALTER TABLE {self.db_name}.{tbname} ADD COLUMN c100 decimal(36, 18) COMPRESS "zstd"'
        tdSql.execute(create_c100_sql, queryTimes=1, show=True)
        columns.append(Column(DecimalType(TypeEnum.DECIMAL, 36, 18)))
        self.check_desc(tbname, columns)

        ## drop non decimal column
        drop_c6_sql = f"alter table {self.db_name}.{tbname} drop column c6"
        tdSql.execute(drop_c6_sql, queryTimes=1, show=True)
        c6 = columns.pop(5)
        self.check_desc(tbname, columns)
        ## drop decimal column and not last column
        drop_c99_sql = f"alter table {self.db_name}.{tbname} drop column c99"
        tdSql.execute(drop_c99_sql, queryTimes=1, show=True)
        c99 = columns.pop(len(columns) - 2)
        self.check_desc(tbname, columns)
        ## drop decimal column and last column
        drop_c100_sql = f"alter table {self.db_name}.{tbname} drop column c100"
        tdSql.execute(drop_c100_sql, queryTimes=1, show=True)
        c100 = columns.pop(len(columns) - 1)
        self.check_desc(tbname, columns)

        ## create decimal back
        tdSql.execute(create_c99_sql, queryTimes=1, show=True)
        tdSql.execute(create_c100_sql, queryTimes=1, show=True)
        columns.append(c99)
        columns.append(c100)
        self.check_desc(tbname, columns)

    def test_decimal_column_ddl(self):
        ## create decimal type table, normal/super table, decimal64/decimal128
        tdLog.printNoPrefix("-------- test create decimal column")
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
        self.check_show_create_table("meters", self.stb_columns, self.tags)

        DecimalColumnTableCreater(
            tdSql, self.db_name, self.norm_table_name, self.norm_tb_columns
        ).create()
        self.check_desc(self.norm_table_name, self.norm_tb_columns)
        self.check_show_create_table(self.norm_table_name, self.norm_tb_columns)

        ## TODO add more values for all rows
        tag_values = ["1", "t1"]
        DecimalColumnTableCreater(
            tdSql, self.db_name, self.stable_name, self.stb_columns
        ).create_child_table(
            self.c_table_prefix, self.c_table_num, self.tags, tag_values
        )
        self.check_desc("meters", self.stb_columns, self.tags)
        self.check_desc("t1", self.stb_columns, self.tags)

        ## invalid precision/scale
        invalid_precision_scale = [
            ("decimal(-1, 2)", syntax_error),
            ("decimal(39, 2)", invalid_column),
            ("decimal(10, -1)", syntax_error),
            ("decimal(10, 39)", invalid_column),
            ("decimal(10, 2.5)", syntax_error),
            ("decimal(10.5, 2)", syntax_error),
            ("decimal(10.5, 2.5)", syntax_error),
            ("decimal(0, 2)", invalid_column),
            ("decimal(0)", invalid_column),
            ("decimal", syntax_error),
            ("decimal()", syntax_error),
        ]
        for i in invalid_precision_scale:
            sql = f"create table {self.db_name}.invalid_decimal_precision_scale (ts timestamp, c1 {i[0]})"
            tdSql.error(sql, i[1])

        ## can't create decimal tag
        sql = (
            "create stable %s.invalid_decimal_tag (ts timestamp) tags (t1 decimal(10, 2))"
            % (self.db_name)
        )
        tdSql.error(sql, invalid_column)

        ## alter table add/drop column
        self.test_add_drop_columns_with_decimal(
            self.norm_table_name, self.norm_tb_columns
        )
        self.test_add_drop_columns_with_decimal(self.stable_name, self.stb_columns)

        ## drop index from stb
        ### These ops will override the previous stbobjs and meta entries, so test it

        ## TODO test encode and compress for decimal type
        sql = f'ALTER TABLE {self.db_name}.{self.norm_table_name} ADD COLUMN c101 decimal(37, 19) ENCODE "simple8b" COMPRESS "zstd"'
        tdSql.error(sql, invalid_encode_param)
        sql = f'ALTER TABLE {self.db_name}.{self.norm_table_name} ADD COLUMN c101 decimal(37, 19) ENCODE "delta-i" COMPRESS "zstd"'
        tdSql.error(sql, invalid_encode_param)
        sql = f'ALTER TABLE {self.db_name}.{self.norm_table_name} ADD COLUMN c101 decimal(37, 19) ENCODE "delta-d" COMPRESS "zstd"'
        tdSql.error(sql, invalid_encode_param)
        sql = f'ALTER TABLE {self.db_name}.{self.norm_table_name} ADD COLUMN c101 decimal(37, 19) ENCODE "bit-packing" COMPRESS "zstd"'
        tdSql.error(sql, invalid_encode_param)

    def test_insert_decimal_values(self):
        tdLog.debug("start to insert decimal values")
        for i in range(self.c_table_num):
            TableInserter(
                tdSql,
                self.db_name,
                f"{self.c_table_prefix}{i}",
                self.stb_columns,
                self.tags,
            ).insert(1000, 1537146000000, 500)

        for i in range(self.c_table_num):
            TableDataValidator(
                self.stb_columns, self.c_table_prefix + str(i), self.db_name, i
            ).validate()

        TableInserter(
            tdSql, self.db_name, self.norm_table_name, self.norm_tb_columns
        ).insert(1000, 1537146000000, 500, flush_database=True)
        TableDataValidator(
            self.norm_tb_columns, self.norm_table_name, self.db_name
        ).validate()

        tdSql.execute("flush database %s" % (self.db_name), queryTimes=1)
        for i in range(self.c_table_num):
            TableDataValidator(
                self.stb_columns, self.c_table_prefix + str(i), self.db_name, i
            ).validate()
        TableDataValidator(
            self.norm_tb_columns, self.norm_table_name, self.db_name
        ).validate()

        ## insert null/None for decimal type

        ## insert with column format

    def no_decimal_table_test(self):
        columns = [
            Column(DataType(TypeEnum.TINYINT)),
            Column(DataType(TypeEnum.INT)),
            Column(DataType(TypeEnum.BIGINT)),
            Column(DataType(TypeEnum.DOUBLE)),
            Column(DataType(TypeEnum.FLOAT)),
            Column(DataType(TypeEnum.VARCHAR, 255)),
        ]
        DecimalColumnTableCreater(
            tdSql, self.db_name, self.no_decimal_col_tb_name, columns, []
        ).create()
        TableInserter(tdSql, self.db_name, self.no_decimal_col_tb_name, columns).insert(
            10000, 1537146000000, 500, flush_database=True
        )
        ## TODO wjm test non support decimal version upgrade to decimal support version, and add decimal column

        ## Test metaentry compatibility problem for decimal type
        ## How to test it?
        ## Create table with no decimal type, the metaentries should not have extschma, and add decimal column, the metaentries should have extschema for all columns.
        sql = f"ALTER TABLE {self.db_name}.{self.no_decimal_col_tb_name} ADD COLUMN c200 decimal(37, 19)"
        tdSql.execute(sql, queryTimes=1)  ## now meta entry has ext schemas
        columns.append(Column(DecimalType(TypeEnum.DECIMAL, 37, 19)))
        self.check_desc(self.no_decimal_col_tb_name, columns)

        ## After drop this only decimal column, the metaentries should not have extschema for all columns.
        sql = (
            f"ALTER TABLE {self.db_name}.{self.no_decimal_col_tb_name} DROP COLUMN c200"
        )
        tdSql.execute(sql, queryTimes=1)  ## now meta entry has no ext schemas
        columns.pop(len(columns) - 1)
        self.check_desc(self.no_decimal_col_tb_name, columns)
        sql = f"ALTER TABLE {self.db_name}.{self.no_decimal_col_tb_name} ADD COLUMN c200 int"
        tdSql.execute(sql, queryTimes=1)  ## meta entry has no ext schemas
        columns.append(Column(DataType(TypeEnum.INT)))
        self.check_desc(self.no_decimal_col_tb_name, columns)

        self.test_add_drop_columns_with_decimal(self.no_decimal_col_tb_name, columns)

    def test_decimal_ddl(self):
        tdSql.execute("create database test", queryTimes=1)
        self.test_decimal_column_ddl()
        ## TODO test decimal column for tmq

    def test_decimal_and_stream(self):
        create_stream = f"CREATE STREAM {self.stream_name} FILL_HISTORY 1 INTO {self.db_name}.{self.stream_out_stb} AS SELECT _wstart, count(c1), avg(c2), sum(c3) FROM {self.db_name}.{self.stable_name} INTERVAL(10s)"
        tdSql.execute(create_stream, queryTimes=1, show=True)
        self.wait_query_result(
            f"select count(*) from {self.db_name}.{self.stream_out_stb}", [(500,)], 30
        )

    def test_decimal_and_tsma(self):
        create_tsma = f"CREATE TSMA {self.tsma_name} ON {self.db_name}.{self.stable_name} FUNCTION(count(c1), min(c2), max(c3), avg(C3)) INTERVAL(1m)"
        tdSql.execute(create_tsma, queryTimes=1, show=True)
        self.wait_query_result(
            f"select count(*) from {self.db_name}.{self.tsma_name}_tsma_res_stb_",
            [(9 * self.c_table_num,)],
            30,
        )

    def run(self):
        self.test_decimal_ddl()
        self.no_decimal_table_test()
        self.test_insert_decimal_values()
        self.test_query_decimal()
        ##self.test_decimal_and_stream()
        ##self.test_decimal_and_tsma()

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

    def check_decimal_binary_expr_results(
        self,
        dbname,
        tbname,
        tb_cols: List[Column],
        constant_cols: List[Column],
        exprs: List[DecimalColumnExpr],
    ):
        for expr in exprs:
            for col in tb_cols:
                left_is_decimal = col.type_.is_decimal_type()
                for const_col in constant_cols:
                    right_is_decimal = const_col.type_.is_decimal_type()
                    if not left_is_decimal and not right_is_decimal:
                        continue
                    const_col.generate_value()
                    select_expr = expr.generate((col, const_col))
                    sql = f"select {select_expr} from {dbname}.{tbname}"
                    res = TaosShell().query(sql)
                    expr.check(res[0], tbname)
        ## query
        ## build expr, expr.generate(column) to generate sql expr
        ## pass this expr into DataValidator.
        # When validating between query results and local values, pass the column data into the Expr, and invoke expr.execute
        ## get result
        ## check result

    ## test others unsupported types operator with decimal
    def test_decimal_unsupported_types(self):
        unsupported_type = [
            TypeEnum.JSON,
            TypeEnum.GEOMETRY,
            TypeEnum.VARBINARY,
        ]
        all_type_columns: List[Column] = Column.get_all_type_columns([TypeEnum.JSON])
        tbname = "test_decimal_unsupported_types"
        tag_cols = [Column(DataType(TypeEnum.JSON))]
        tb_creater = DecimalColumnTableCreater( tdSql, self.db_name, tbname, all_type_columns, tag_cols)
        tb_creater.create()
        tb_creater.create_child_table(tbname, 10, tag_cols, ['{"k1": "v1"}'])
        for i in range(10):
            TableInserter(tdSql, self.db_name, f'{tbname}{i}', all_type_columns, tag_cols).insert(100, 1537146000000, 500, flush_database=True)
        for col in all_type_columns:
            ## only test decimal cols
            if not col.type_.is_decimal_type():
                continue
            for unsupported_col in all_type_columns:
                ## only test unsupported cols
                if not unsupported_col.type_.type in unsupported_type:
                    continue
                for binary_op in DecimalBinaryOperator.get_all_binary_ops():
                    select_expr = binary_op.generate((col.name_, unsupported_col.name_))
                    sql = f"select {select_expr} from {self.db_name}.{tbname}"
                    select_expr_reverse = binary_op.generate(
                        (unsupported_col.name_, col.name_)
                    )
                    sql_reverse = (
                        f"select {select_expr_reverse} from {self.db_name}.{tbname}"
                    )
                    tdLog.info(
                        f"select expr: {select_expr} with type: {col.type_} and {unsupported_col.type_} should err"
                    )
                    err = tdSql.error(sql)
                    if tdSql.errno != invalid_operation and tdSql.errno != scalar_convert_err:
                        tdLog.exit(f"expected err not occured for sql: {sql}, expect: {invalid_operation} or {scalar_convert_err}, but got {tdSql.errno}")

                    tdLog.info(
                        f"select expr: {select_expr} with type: {unsupported_col.type_} and {col.type_} should err"
                    )
                    err = tdSql.error(sql_reverse)
                    if tdSql.errno != invalid_operation and tdSql.errno != scalar_convert_err:
                        tdLog.exit(f"expected err not occured for sql: {sql}, expect: {invalid_operation} or {scalar_convert_err}, but got {tdSql.errno}")

    def test_decimal_operators(self):
        tdLog.debug("start to test decimal operators")
        self.test_decimal_unsupported_types()
        ## tables: meters, nt
        ## columns: c1, c2, c3, c4, c5, c7, c8, c9, c10, c99, c100
        binary_operators = [
            DecimalColumnExpr("%s + %s", DecimalBinaryOperator.execute_plus),
            # DecimalColumnExpr("-"),
            # DecimalColumnExpr("*"),
            # DecimalColumnExpr("/"),
            # DecimalColumnExpr("%"),
            # DecimalColumnExpr(">"),
            # DecimalColumnExpr("<"),
            # DecimalColumnExpr(">="),
            # DecimalColumnExpr("<="),
            # DecimalColumnExpr("=="),
            # DecimalColumnExpr("!="),
            # DecimalBinaryOperatorIn("in"),
            # DecimalBinaryOperatorIn("not in"),
        ]
        all_type_columns = Column.get_all_type_columns()

        ## decimal operator with constants of all other types
        self.check_decimal_binary_expr_results(
            self.db_name,
            self.norm_table_name,
            self.norm_tb_columns,
            all_type_columns,
            binary_operators,
        )

        ## decimal operator with columns of all other types

        unary_operators = ["-"]

    def test_decimal_functions(self):
        self.test_decimal_last_first_func()
        funcs = ["max", "min", "sum", "avg", "count", "first", "last", "cast"]

    def test_decimal_last_first_func(self):
        pass

    def test_query_decimal_with_sma(self):
        pass

    def test_query_decimal_where_clause(self):
        pass

    def test_query_decimal_order_clause(self):
        pass

    def test_query_decimal_group_by_clause(self):
        pass

    def test_query_decimal_having_clause(self):
        pass

    def test_query_decimal_interval_fill(self):
        pass

    def test_query_decimal_partition_by(self):
        pass

    def test_query_decimal_case_when(self):
        pass

    def test_query_decimal(self):
        self.test_decimal_operators()
        self.test_decimal_functions()
        self.test_query_decimal_with_sma()


event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
