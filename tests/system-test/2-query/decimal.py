from pydoc import doc
from random import randrange
from re import A
import time
import threading
import secrets

from sympy import true
from torch import randint
import query
from tag_lite import column
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from decimal import Decimal

syntax_error = -2147473920
invalid_column = -2147473918
invalid_compress_level = -2147483084
invalid_encode_param = -2147483087

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
                res += '.'
                if self.possible(0.8):
                    res += self.generate_digit() * config.scale
        return res
    
    ## 写入大整数的例子, 如10000000000, scale解析时可能为负数
    def generate_(self, config: DecimalTypeGeneratorConfig) -> str:
        ret: str = ''
        sign = self.generate_sign(config.positive_ratio)
        if config.with_corner_case and self.current_should_generate_corner_case(config.corner_case_ratio):
            ret += self.generate_corner_case(config)
        else:
            if config.enable_positive_sign or sign != '+':
                ret += sign
            weight = random.randint(1, config.prec - config.scale)
            scale = random.randint(1, config.scale)
            for i in range(weight):
                ret += self.generate_digit()
            
            if config.enable_weight_overflow and self.possible(config.weightOverflowRatio):
                extra_weight = config.prec - weight + 1 + random.randint(1, self.get_max_prec(config.prec))
                while extra_weight > 0:
                    ret += self.generate_digit()
                    extra_weight -= 1
            ret += '.'
            for i in range(scale):
                ret += self.generate_digit()
            if config.enable_scale_overflow and self.possible(config.scale_overflow_ratio):
                extra_scale = config.scale - scale + 1 + random.randint(1, self.get_max_prec(config.prec))
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
        with open(self.tmp_file_path, 'r') as f:
            lines = f.readlines()
            lines = lines[1:]
            for line in lines:
                col = 0
                vals: List[str] = line.split(',')
                if len(self.queryResult) == 0:
                    self.queryResult = [[] for i in range(len(vals))]
                for val in vals:
                    self.queryResult[col].append(val)
                    col += 1

    def query(self, sql: str):
        try:
            command = f'taos -s "{sql} >> {self.tmp_file_path}"'
            result = subprocess.run(command, shell=True, check=True, stderr=subprocess.PIPE)
            self.read_result()
        except subprocess.CalledProcessError as e:
            tdLog.error(f"Command '{sql}' failed with error: {e.stderr.decode('utf-8')}")
            self.queryResult = []
        return self.queryResult

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
            return "UTINYINT"
        elif type == TypeEnum.USMALLINT:
            return "USMALLINT"
        elif type == TypeEnum.UINT:
            return "UINT"
        elif type == TypeEnum.UBIGINT:
            return "UBIGINT"
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
        else:
            raise Exception("unknow type")

class DataType:
    def __init__(self, type: int, length: int = 0, type_mod: int = 0):
        self.type : int = type
        self.length = length
        self.type_mod = type_mod

    def __str__(self):
        if self.type_mod != 0:
            decimal_type = self.get_decimal_type()
            return f"{TypeEnum.get_type_str(self.type)}({decimal_type.precision}, {decimal_type.scale})"
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
    
    def construct_type_value(self, val: str):
        if self.type == TypeEnum.BINARY or self.type == TypeEnum.VARCHAR or self.type == TypeEnum.NCHAR or self.type == TypeEnum.VARBINARY:
            return f"'{val}'"
        else:
            return val
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
        if self.type == TypeEnum.VARCHAR or self.type == TypeEnum.NCHAR or self.type == TypeEnum.VARBINARY:
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
        raise Exception(f"unsupport type {self.type}")
    def check(self, values, offset: int):
        return True

class DecimalType(DataType):
    def __init__(self, type, precision: int, scale: int):
        self.precision = precision
        self.scale = scale
        if type == TypeEnum.DECIMAL64:
            bytes = 8
        else:
            bytes = 16
        super().__init__(type, bytes, self.get_decimal_type_mod())
        self.generator: DecimalStringRandomGenerator = DecimalStringRandomGenerator()
        self.generator_config: DecimalTypeGeneratorConfig = DecimalTypeGeneratorConfig()
        self.generator_config.prec = precision
        self.generator_config.scale = scale
        self.aggregator: DecimalColumnAggregator = DecimalColumnAggregator()
        self.values: List[str] = []

    def get_decimal_type_mod(self) -> int:
        return self.precision * 100 + self.scale

    def __str__(self):
        return f"DECIMAL({self.precision}, {self.scale})"

    def __eq__(self, other):
        return self.precision == other.precision and self.scale == other.scale

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.precision, self.scale))

    def __repr__(self):
        return f"DecimalType({self.precision}, {self.scale})"
    
    def generate_value(self) -> str:
        val = self.generator.generate_(self.generator_config)
        self.aggregator.add_value(val)
        self.values.append(val)
        return val
    
    @staticmethod
    def default_compression() -> str:
        return "zstd"
    @staticmethod
    def default_encode() -> str:
        return "disabled"
    
    def check(self, values, offset: int):
        val_from_query = values
        val_insert = self.values[offset:]
        for v1, v2 in zip(val_from_query, val_insert):
            dec1: Decimal = Decimal(v1)
            dec2: Decimal = Decimal(v2)
            dec2 = dec2.quantize(Decimal(10) ** -self.scale)
            if dec1 != dec2:
                tdLog.error(f"check decimal column failed, expect {dec2}, but get {dec1}")
                return False


class DecimalColumnTableCreater:
    def __init__(self, conn, dbName: str, tbName: str, columns_types: List[DataType], tags_types: List[DataType] = []):
        self.conn = conn
        self.dbName = dbName
        self.tbName = tbName
        self.tags_types = tags_types
        self.columns_types = columns_types

    def create(self):
        if len(self.tags_types) > 0:
            table = 'stable'
        else:
            table = 'table'
        sql = f"create {table} {self.dbName}.{self.tbName} (ts timestamp"
        for i, column in enumerate(self.columns_types):
            sql += f", c{i+1} {column}"
        if self.tags_types:
            sql += ") tags("
            for i, tag in enumerate(self.tags_types):
                sql += f"t{i+1} {tag}"
                if i != len(self.tags_types) - 1:
                    sql += ", "
        sql += ")"
        self.conn.execute(sql, queryTimes=1)

    def create_child_table(self, ctbPrefix: str, ctbNum: int, tag_types: List[DataType], tag_values: List[str]):
        for i in range(ctbNum):
            sql = f"create table {self.dbName}.{ctbPrefix}{i} using {self.dbName}.{self.tbName} tags("
            for j, tag in enumerate(tag_types):
                sql += f"{tag.construct_type_value(tag_values[j])}"
                if j != len(tag_types) - 1:
                    sql += ", "
            sql += ")"
            self.conn.execute(sql, queryTimes=1)


class TableInserter:
    def __init__(self, conn, dbName: str, tbName: str, columns_types: List[DataType], tags_types: List[DataType] = []):
        self.conn = conn
        self.dbName = dbName
        self.tbName = tbName
        self.tags_types = tags_types
        self.columns_types = columns_types

    def insert(self, rows: int, start_ts: int, step: int, flush_database: bool = False):
        pre_insert = f"insert into {self.dbName}.{self.tbName} values"
        sql = pre_insert
        for i in range(rows):
            sql += f"({start_ts + i * step}"
            for column in self.columns_types:
                sql += f", {column.generate_value()}"
            sql += ")"
            if i != rows - 1:
                sql += ", "
            local_flush_database = i % 5000 == 0
            if len(sql) > 1000:
                #tdLog.debug(f"insert into with sql{sql}")
                if flush_database and local_flush_database:
                    self.conn.execute(f"flush database {self.dbName}", queryTimes=1)
                self.conn.execute(sql, queryTimes=1)
                sql = pre_insert
        if len(sql) > len(pre_insert):
            #tdLog.debug(f"insert into with sql{sql}")
            if flush_database:
                self.conn.execute(f"flush database {self.dbName}", queryTimes=1)
            self.conn.execute(sql, queryTimes=1)

class TableDataValidator:
    def __init__(self, columns: List[DataType], tbName: str, dbName: str, tbIdx: int = 0):
        self.columns = columns
        self.tbName = tbName
        self.dbName = dbName
        self.tbIdx = tbIdx
    
    def validate(self):
        sql = f"select * from {self.dbName}.{self.tbName}"
        res = TaosShell().query(sql)
        row_num = len(res)
        colIdx = 1
        for col in self.columns:
            if col.type == TypeEnum.DECIMAL or col.type == TypeEnum.DECIMAL64:
                col.check(res[colIdx], row_num * self.tbIdx)
            colIdx += 1
                
class TDTestCase:
    updatecfgDict = {'asynclog': 0, 'ttlUnit': 1, 'ttlPushInterval': 5, 'ratioOfVnodeStreamThrea': 4, 'debugFlag': 143}

    def __init__(self):
        self.vgroups = 4
        self.ctbNum = 10
        self.rowsPerTbl = 10000
        self.duraion = '1h'
        self.norm_tb_columns = []
        self.tags = []
        self.stable_name = "meters"
        self.norm_table_name = "nt"
        self.c_table_prefix = "t"
        self.db_name = "test"
        self.c_table_num = 10
        self.no_decimal_col_tb_name = 'tt'
        self.stb_columns = []
        self.stream_name = 'stream1'
        self.stream_out_stb = 'stream_out_stb'
        self.tsma_name = 'tsma1'

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)

    def create_database(self, tsql, dbName, dropFlag=1, vgroups=2, replica=1, duration: str = '1d'):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s" % (dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d duration %s" % (
            dbName, vgroups, replica, duration))
        tdLog.debug("complete to create database %s" % (dbName))

    def create_stable(self, tsql, paraDict):
        colString = tdCom.gen_column_type_str(
            colname_prefix=paraDict["colPrefix"], column_elm_list=paraDict["colSchema"])
        tagString = tdCom.gen_tag_type_str(
            tagname_prefix=paraDict["tagPrefix"], tag_elm_list=paraDict["tagSchema"])
        sqlString = f"create table if not exists %s.%s (%s) tags (%s)" % (
            paraDict["dbName"], paraDict["stbName"], colString, tagString)
        tdLog.debug("%s" % (sqlString))
        tsql.execute(sqlString)

    def create_ctable(self, tsql=None, dbName='dbx', stbName='stb', ctbPrefix='ctb', ctbNum=1, ctbStartIdx=0):
        for i in range(ctbNum):
            sqlString = "create table %s.%s%d using %s.%s tags(%d, 'tb%d', 'tb%d', %d, %d, %d)" % (dbName, ctbPrefix, i+ctbStartIdx, dbName, stbName, (i+ctbStartIdx) % 5, i+ctbStartIdx + random.randint(
                1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100))
            tsql.execute(sqlString)

        tdLog.debug("complete to create %d child tables by %s.%s" %
                    (ctbNum, dbName, stbName))

    def init_normal_tb(self, tsql, db_name: str, tb_name: str, rows: int, start_ts: int, ts_step: int):
        sql = 'CREATE TABLE %s.%s (ts timestamp, c1 INT, c2 INT, c3 INT, c4 double, c5 VARCHAR(255))' % (
            db_name, tb_name)
        tsql.execute(sql)
        sql = 'INSERT INTO %s.%s values' % (db_name, tb_name)
        for j in range(rows):
            sql += f'(%d, %d,%d,%d,{random.random()},"varchar_%d"),' % (start_ts + j * ts_step + randrange(500), j %
                                                     10 + randrange(200), j % 10, j % 10, j % 10 + randrange(100))
        tsql.execute(sql)

    def insert_data(self, tsql, dbName, ctbPrefix, ctbNum, rowsPerTbl, batchNum, startTs, tsStep):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" % dbName)
        pre_insert = "insert into "
        sql = pre_insert

        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s.%s%d values " % (dbName, ctbPrefix, i)
            for j in range(rowsPerTbl):
                if (i < ctbNum/2):
                    sql += "(%d, %d, %d, %d,%d,%d,%d,true,'binary%d', 'nchar%d') " % (startTs + j*tsStep + randrange(
                        500), j % 10 + randrange(100), j % 10 + randrange(200), j % 10, j % 10, j % 10, j % 10, j % 10, j % 10)
                else:
                    sql += "(%d, %d, NULL, %d,NULL,%d,%d,true,'binary%d', 'nchar%d') " % (
                        startTs + j*tsStep + randrange(500), j % 10, j % 10, j % 10, j % 10, j % 10, j % 10)
                rowsBatched += 1
                if ((rowsBatched == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s%d values " % (dbName, ctbPrefix, i)
                    else:
                        sql = "insert into "
        if sql != pre_insert:
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def init_data(self, db: str = 'test', ctb_num: int = 10, rows_per_ctb: int = 10000, start_ts: int = 1537146000000, ts_step: int = 500):
        tdLog.printNoPrefix(
            "======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     db,
                    'dropFlag':   1,
                    'vgroups':    4,
                    'stbName':    'meters',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count': 1}, {'type': 'BIGINT', 'count': 1}, {'type': 'FLOAT', 'count': 1}, {'type': 'DOUBLE', 'count': 1}, {'type': 'smallint', 'count': 1}, {'type': 'tinyint', 'count': 1}, {'type': 'bool', 'count': 1}, {'type': 'binary', 'len': 10, 'count': 1}, {'type': 'nchar', 'len': 10, 'count': 1}],
                    'tagSchema':   [{'type': 'INT', 'count': 1}, {'type': 'nchar', 'len': 20, 'count': 1}, {'type': 'binary', 'len': 20, 'count': 1}, {'type': 'BIGINT', 'count': 1}, {'type': 'smallint', 'count': 1}, {'type': 'DOUBLE', 'count': 1}],
                    'ctbPrefix':  't',
                    'ctbStartIdx': 0,
                    'ctbNum':     ctb_num,
                    'rowsPerTbl': rows_per_ctb,
                    'batchNum':   3000,
                    'startTs':    start_ts,
                    'tsStep':     ts_step}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = ctb_num
        paraDict['rowsPerTbl'] = rows_per_ctb

        tdLog.info("create database")
        self.create_database(tsql=tdSql, dbName=paraDict["dbName"], dropFlag=paraDict["dropFlag"],
                             vgroups=paraDict["vgroups"], replica=self.replicaVar, duration=self.duraion)

        tdLog.info("create stb")
        self.create_stable(tsql=tdSql, paraDict=paraDict)

        tdLog.info("create child tables")
        self.create_ctable(tsql=tdSql, dbName=paraDict["dbName"],
                           stbName=paraDict["stbName"], ctbPrefix=paraDict["ctbPrefix"],
                           ctbNum=paraDict["ctbNum"], ctbStartIdx=paraDict["ctbStartIdx"])
        self.insert_data(tsql=tdSql, dbName=paraDict["dbName"],
                         ctbPrefix=paraDict["ctbPrefix"], ctbNum=paraDict["ctbNum"],
                         rowsPerTbl=paraDict["rowsPerTbl"], batchNum=paraDict["batchNum"],
                         startTs=paraDict["startTs"], tsStep=paraDict["tsStep"])
        self.init_normal_tb(tdSql, paraDict['dbName'], 'norm_tb',
                            paraDict['rowsPerTbl'], paraDict['startTs'], paraDict['tsStep'])

    def check_desc_for_one_ctb(self, ctbPrefix: str, columns: List[DataType], tags: List[DataType] = []):
        ctb_idx = randrange(self.c_table_num)
        return self.check_desc(f"{ctbPrefix}{ctb_idx}", columns, tags)

    def check_desc(self, tbname: str, column_types: List[DataType], tag_types: List[DataType] = []):
        sql = f"desc {self.db_name}.{tbname}"
        tdSql.query(sql, queryTimes=1)
        results = tdSql.queryResult
        for i, column_type in enumerate(column_types):
            if column_type.type == TypeEnum.DECIMAL:
                if results[i+1][1] != column_type.__str__():
                    tdLog.info(str(results))
                    tdLog.exit(f"check desc failed for table: {tbname} column {results[i+1][0]} type is {results[i+1][1]}, expect DECIMAL")
                if results[i+1][4] != DecimalType.default_encode():
                    tdLog.exit(f"check desc failed for table: {tbname} column {results[i+1][0]} encode is {results[i+1][5]}, expect {DecimalType.default_encode()}")
                if results[i+1][5] != DecimalType.default_compression():
                    tdLog.exit(f"check desc failed for table: {tbname} column {results[i+1][0]} compression is {results[i+1][4]}, expect {DecimalType.default_compression()}")
        if tbname == self.stable_name:
            self.check_desc_for_one_ctb(self.c_table_prefix, column_types, tag_types)

    def check_show_create_table(self, tbname: str, column_types: List[DataType], tag_types: List[DataType] = []):
        sql = f"show create table {self.db_name}.{tbname}"
        tdSql.query(sql, queryTimes=1)
        create_table_sql = tdSql.queryResult[0][1]
        decimal_idx = 0
        results = re.findall(r"DECIMAL\((\d+),(\d+)\)", create_table_sql)
        for i, column_type in enumerate(column_types):
            if column_type.type == TypeEnum.DECIMAL or column_type.type == TypeEnum.DECIMAL64:
                result_type = DecimalType(column_type.type, int(results[decimal_idx][0]), int(results[decimal_idx][1]))
                if result_type != column_type:
                    tdLog.exit(f"check show create table failed for: {tbname} column {i} type is {result_type}, expect {column_type.get_decimal_type()}")
                decimal_idx += 1
    
    def test_add_drop_columns_with_decimal(self, tbname: str, columns: List[DataType]):
        is_stb = tbname == self.stable_name
        ## alter table add column
        create_c99_sql = f'alter table {self.db_name}.{tbname} add column c99 decimal(37, 19)'
        columns.append(DecimalType(TypeEnum.DECIMAL, 37, 19))
        tdSql.execute(create_c99_sql, queryTimes=1, show=True)
        self.check_desc(tbname, columns)
        ## alter table add column with compression
        create_c100_sql = f'ALTER TABLE {self.db_name}.{tbname} ADD COLUMN c100 decimal(36, 18) COMPRESS "zstd"'
        tdSql.execute(create_c100_sql, queryTimes=1, show=True)
        columns.append(DecimalType(TypeEnum.DECIMAL, 36, 18))
        self.check_desc(tbname, columns)

        ## drop non decimal column
        drop_c6_sql = f'alter table {self.db_name}.{tbname} drop column c6'
        tdSql.execute(drop_c6_sql, queryTimes=1, show=True)
        c6 = columns.pop(5)
        self.check_desc(tbname, columns)
        ## drop decimal column and not last column
        drop_c99_sql = f'alter table {self.db_name}.{tbname} drop column c99'
        tdSql.execute(drop_c99_sql, queryTimes=1, show=True)
        c99 = columns.pop(len(columns) - 2)
        self.check_desc(tbname, columns)
        ## drop decimal column and last column
        drop_c100_sql = f'alter table {self.db_name}.{tbname} drop column c100'
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
        self.norm_tb_columns = [
            DecimalType(TypeEnum.DECIMAL, 10, 2),
            DecimalType(TypeEnum.DECIMAL, 20, 4),
            DecimalType(TypeEnum.DECIMAL, 30, 8),
            DecimalType(TypeEnum.DECIMAL, 38, 10),
            DataType(TypeEnum.TINYINT),
            DataType(TypeEnum.INT),
            DataType(TypeEnum.BIGINT),
            DataType(TypeEnum.DOUBLE),
            DataType(TypeEnum.FLOAT),
            DataType(TypeEnum.VARCHAR, 255),
        ]
        self.tags = [
            DataType(TypeEnum.INT),
            DataType(TypeEnum.VARCHAR, 255)
        ]
        self.stb_columns = [
            DecimalType(TypeEnum.DECIMAL, 10, 2),
            DecimalType(TypeEnum.DECIMAL, 20, 4),
            DecimalType(TypeEnum.DECIMAL, 30, 8),
            DecimalType(TypeEnum.DECIMAL, 38, 10),
            DataType(TypeEnum.TINYINT),
            DataType(TypeEnum.INT),
            DataType(TypeEnum.BIGINT),
            DataType(TypeEnum.DOUBLE),
            DataType(TypeEnum.FLOAT),
            DataType(TypeEnum.VARCHAR, 255),
        ]
        DecimalColumnTableCreater(tdSql, self.db_name, self.stable_name, self.stb_columns, self.tags).create()
        self.check_show_create_table("meters", self.stb_columns, self.tags)

        DecimalColumnTableCreater(tdSql, self.db_name, self.norm_table_name, self.norm_tb_columns).create()
        self.check_desc(self.norm_table_name, self.norm_tb_columns)
        self.check_show_create_table(self.norm_table_name, self.norm_tb_columns)

        ## TODO add more values for all rows
        tag_values = [
            "1", "t1"
        ]
        DecimalColumnTableCreater(tdSql, self.db_name, self.stable_name, self.norm_tb_columns).create_child_table(self.c_table_prefix, self.c_table_num, self.tags, tag_values)
        self.check_desc("meters", self.stb_columns, self.tags)
        self.check_desc("t1", self.norm_tb_columns, self.tags)

        ## invalid precision/scale
        invalid_precision_scale = [("decimal(-1, 2)", syntax_error), ("decimal(39, 2)", invalid_column), ("decimal(10, -1)", syntax_error), 
                                   ("decimal(10, 39)", invalid_column), ("decimal(10, 2.5)", syntax_error), ("decimal(10.5, 2)", syntax_error), 
                                   ("decimal(10.5, 2.5)", syntax_error), ("decimal(0, 2)", invalid_column), ("decimal(0)", invalid_column), 
                                   ("decimal", syntax_error), ("decimal()", syntax_error)]
        for i in invalid_precision_scale:
            sql = f"create table {self.db_name}.invalid_decimal_precision_scale (ts timestamp, c1 {i[0]})"
            tdSql.error(sql, i[1])

        ## can't create decimal tag
        sql = 'create stable %s.invalid_decimal_tag (ts timestamp) tags (t1 decimal(10, 2))' % (self.db_name)
        tdSql.error(sql, invalid_column)

        ## alter table add/drop column
        self.test_add_drop_columns_with_decimal(self.norm_table_name, self.norm_tb_columns)
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
        for i in range(self.c_table_num):
            TableInserter(tdSql, self.db_name, f"{self.c_table_prefix}{i}", self.stb_columns, self.tags).insert(1000, 1537146000000, 500)

        TableInserter(tdSql, self.db_name, self.norm_table_name, self.norm_tb_columns).insert(10, 1537146000000, 500, flush_database=True)
        TableDataValidator(self.norm_tb_columns, self.norm_table_name, self.db_name).validate()


        ## insert null/None for decimal type

        ## insert with column format

    def no_decimal_table_test(self):
        columns = [
                DataType(TypeEnum.TINYINT),
                DataType(TypeEnum.INT),
                DataType(TypeEnum.BIGINT),
                DataType(TypeEnum.DOUBLE),
                DataType(TypeEnum.FLOAT),
                DataType(TypeEnum.VARCHAR, 255),
                ]
        DecimalColumnTableCreater(tdSql, self.db_name, self.no_decimal_col_tb_name, columns, []).create()
        TableInserter(tdSql, self.db_name, self.no_decimal_col_tb_name, columns).insert(10000, 1537146000000, 500, flush_database=True)
        ## TODO wjm test non support decimal version upgrade to decimal support version, and add decimal column

        ## Test metaentry compatibility problem for decimal type
        ## How to test it?
        ## Create table with no decimal type, the metaentries should not have extschma, and add decimal column, the metaentries should have extschema for all columns.
        sql = f'ALTER TABLE {self.db_name}.{self.no_decimal_col_tb_name} ADD COLUMN c200 decimal(37, 19)'
        tdSql.execute(sql, queryTimes=1) ## now meta entry has ext schemas
        columns.append(DecimalType(TypeEnum.DECIMAL, 37, 19))
        self.check_desc(self.no_decimal_col_tb_name, columns)

        ## After drop this only decimal column, the metaentries should not have extschema for all columns.
        sql = f'ALTER TABLE {self.db_name}.{self.no_decimal_col_tb_name} DROP COLUMN c200'
        tdSql.execute(sql, queryTimes=1) ## now meta entry has no ext schemas
        columns.pop(len(columns) - 1)
        self.check_desc(self.no_decimal_col_tb_name, columns)
        sql = f'ALTER TABLE {self.db_name}.{self.no_decimal_col_tb_name} ADD COLUMN c200 int'
        tdSql.execute(sql, queryTimes=1) ## meta entry has no ext schemas
        columns.append(DataType(TypeEnum.INT))
        self.check_desc(self.no_decimal_col_tb_name, columns)

        self.test_add_drop_columns_with_decimal(self.no_decimal_col_tb_name, columns)

    def test_decimal_ddl(self):
        tdSql.execute("create database test", queryTimes=1)
        self.test_decimal_column_ddl()
        ## TODO test decimal column for tmq

    def test_decimal_and_stream(self):
        create_stream = f'CREATE STREAM {self.stream_name} FILL_HISTORY 1 INTO {self.db_name}.{self.stream_out_stb} AS SELECT _wstart, count(c1), avg(c2), sum(c3) FROM {self.db_name}.{self.stable_name} INTERVAL(10s)'
        tdSql.execute(create_stream, queryTimes=1, show=True)
        self.wait_query_result(f"select count(*) from {self.db_name}.{self.stream_out_stb}", [(50,)], 30)
    
    def test_decimal_and_tsma(self):
        create_tsma = f"CREATE TSMA {self.tsma_name} ON {self.db_name}.{self.stable_name} FUNCTION(count(c1), min(c2), max(c3), avg(C3)) INTERVAL(1m)"
        tdSql.execute(create_tsma, queryTimes=1, show=True)
        self.wait_query_result(f"select count(*) from {self.db_name}.{self.tsma_name}_tsma_res_stb_", [(9*self.c_table_num,)], 30)

    def run(self):
        self.test_decimal_ddl()
        self.no_decimal_table_test()
        self.test_insert_decimal_values()
        self.test_decimal_and_stream()
        self.test_decimal_and_tsma()

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
        tdLog.exit(f"wait query result timeout for {sql} failed after {times} time, expect {expect_result}, but got {results}")


event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
