from random import randrange
import time
import threading
import secrets
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

class DecimalType:
    def __init__(self, precision: int, scale: int):
        self.precision = precision
        self.scale = scale

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
    
    def generate_value(self, allow_weight_overflow = False, allow_scale_overflow = False) -> str:
        if allow_weight_overflow:
            weight = secrets.randbelow(40)
        else:
            weight = secrets.randbelow(self.precision + 1)
        if allow_scale_overflow:
            dscale = secrets.randbelow(40 - weight + 1)
        else:
            dscale = secrets.randbelow(self.precision - weight + 1)
        digits :str = ''
        for i in range(dscale):
            digits += str(secrets.randbelow(10))
        if dscale > 0:
            digits += '.'
        for _ in range(dscale):
            digits += str(secrets.randbelow(10))
        if digits == '':
            digits = '0'
        return digits


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
    
    @staticmethod
    def get_decimal_type_mod(type: DecimalType) -> int:
        return type.precision * 100 + type.scale

    def get_decimal_type(self) -> DecimalType:
        return DecimalType(self.type_mod // 100, self.type_mod % 100)
    
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
        if self.type == TypeEnum.DECIMAL:
            return self.get_decimal_type().generate_value()

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

    def insert(self, rows: int, start_ts: int, step: int):
        pre_insert = f"insert into {self.dbName}.{self.tbName} values"
        sql = pre_insert
        for i in range(rows):
            sql += f"({start_ts + i * step}"
            for column in self.columns_types:
                sql += f", {column.generate_value()}"
            if self.tags_types:
                sql += ") tags("
                for tag in self.tags_types:
                    sql += f"{tag.generate_value()},"
                sql = sql[:-2]
            sql += ")"
            if i != rows - 1:
                sql += ", "
            if len(sql) > 1000:
                tdLog.debug(f"insert into with sql{sql}")
                self.conn.execute(sql, queryTimes=1)
                sql = pre_insert
        if len(sql) > len(pre_insert):
            tdLog.debug(f"insert into with sql{sql}")
            self.conn.execute(sql, queryTimes=1)

class TDTestCase:
    updatecfgDict = {'asynclog': 0, 'ttlUnit': 1, 'ttlPushInterval': 5, 'ratioOfVnodeStreamThrea': 4, 'debugFlag': 143}

    def __init__(self):
        self.vgroups = 4
        self.ctbNum = 10
        self.rowsPerTbl = 10000
        self.duraion = '1h'
        self.columns = []
        self.tags = []
        self.stable_name = "meters"
        self.norm_table_name = "norm_table"
        self.c_table_prefix = "t"
        self.db_name = "test"
        self.c_table_num = 10

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
        return

    def create_stable(self, tsql, paraDict):
        colString = tdCom.gen_column_type_str(
            colname_prefix=paraDict["colPrefix"], column_elm_list=paraDict["colSchema"])
        tagString = tdCom.gen_tag_type_str(
            tagname_prefix=paraDict["tagPrefix"], tag_elm_list=paraDict["tagSchema"])
        sqlString = f"create table if not exists %s.%s (%s) tags (%s)" % (
            paraDict["dbName"], paraDict["stbName"], colString, tagString)
        tdLog.debug("%s" % (sqlString))
        tsql.execute(sqlString)
        return

    def create_ctable(self, tsql=None, dbName='dbx', stbName='stb', ctbPrefix='ctb', ctbNum=1, ctbStartIdx=0):
        for i in range(ctbNum):
            sqlString = "create table %s.%s%d using %s.%s tags(%d, 'tb%d', 'tb%d', %d, %d, %d)" % (dbName, ctbPrefix, i+ctbStartIdx, dbName, stbName, (i+ctbStartIdx) % 5, i+ctbStartIdx + random.randint(
                1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100))
            tsql.execute(sqlString)

        tdLog.debug("complete to create %d child tables by %s.%s" %
                    (ctbNum, dbName, stbName))
        return

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

    def check_desc(self, tbname: str, column_types: List[DataType], tag_types: List[DataType] = []):
        sql = f"desc {self.db_name}.{tbname}"
        tdSql.query(sql, queryTimes=1)
        results = tdSql.queryResult
        for i, column_type in enumerate(column_types):
            if column_type.type == TypeEnum.DECIMAL:
                if results[i+1][1] != "DECIMAL":
                    tdLog.info(str(results))
                    tdLog.exit(f"check desc failed for table: {tbname} column {results[i+1][0]} type is {results[i+1][1]}, expect DECIMAL")
        ## add decimal type bytes check
        ## add compression/encode check

    def check_show_create_table(self, tbname: str, column_types: List[DataType], tag_types: List[DataType] = []):
        sql = f"show create table {self.db_name}.{tbname}"
        tdSql.query(sql, queryTimes=1)
        create_table_sql = tdSql.queryResult[0][1]
        decimal_idx = 0
        results = re.findall(r"DECIMAL\((\d+),(\d+)\)", create_table_sql)
        for i, column_type in enumerate(column_types):
            if column_type.type == TypeEnum.DECIMAL:
                result_type = DecimalType(int(results[decimal_idx][0]), int(results[decimal_idx][1]))
                if result_type != column_type.get_decimal_type():
                    tdLog.exit(f"check show create table failed for: {tbname} column {i} type is {result_type}, expect {column_type.get_decimal_type()}")
                decimal_idx += 1

    def test_create_decimal_column(self):
        ## create decimal type table, normal/super table, decimal64/decimal128
        tdLog.printNoPrefix("-------- test create decimal column")
        self.columns = [
            DataType(TypeEnum.DECIMAL, type_mod=DataType.get_decimal_type_mod(DecimalType(10, 2))),
            DataType(TypeEnum.DECIMAL, type_mod=DataType.get_decimal_type_mod(DecimalType(20, 2))),
            DataType(TypeEnum.DECIMAL, type_mod=DataType.get_decimal_type_mod(DecimalType(30, 2))),
            DataType(TypeEnum.DECIMAL, type_mod=DataType.get_decimal_type_mod(DecimalType(38, 2))),
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
        DecimalColumnTableCreater(tdSql, self.db_name, self.stable_name, self.columns, self.tags).create()
        self.check_desc("meters", self.columns, self.tags)
        self.check_show_create_table("meters", self.columns, self.tags)

        DecimalColumnTableCreater(tdSql, self.db_name, self.norm_table_name, self.columns).create()
        self.check_desc("norm_table", self.columns)
        self.check_show_create_table("norm_table", self.columns)

        ## TODO add more values for all rows
        tag_values = [
            "1", "t1"
        ]
        DecimalColumnTableCreater(tdSql, self.db_name, self.stable_name, self.columns).create_child_table(self.c_table_prefix, self.c_table_num, self.tags, tag_values)
        self.check_desc("t1", self.columns, self.tags)

        return
        ## invalid precision/scale
        invalid_precision_scale = ["decimal(-1, 2)", "decimal(39, 2)", "decimal(10, -1)", "decimal(10, 39)", "decimal(10, 2.5)", "decimal(10.5, 2)", "decimal(10.5, 2.5)", "decimal(0, 2)", "decimal(0)", "decimal", "decimal()"]
        for i in invalid_precision_scale:
            sql = f"create table {self.db_name}.invalid_decimal_precision_scale (ts timestamp, c1 {i})"
            tdSql.error(sql, -1)

        ## can't create decimal tag

        ## alter table
        ## drop index from stb
        ### These ops will override the previous stbobjs and meta entries, so test it

    def test_insert_decimal_values(self):

        for i in range(self.c_table_num):
            pass
            #TableInserter(tdSql, self.db_name, f"{self.c_table_prefix}{i}", self.columns, self.tags).insert(1, 1537146000000, 500)

        TableInserter(tdSql, self.db_name, self.norm_table_name, self.columns).insert(1, 1537146000000, 500)


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
        DecimalColumnTableCreater(tdSql, self.db_name, "tt", columns, []).create()
        TableInserter(tdSql, self.db_name, 'tt', columns).insert(1, 1537146000000, 500)

    def test_decimal_ddl(self):
        tdSql.execute("create database test", queryTimes=1)
        self.test_create_decimal_column()

    def run(self):
        self.test_decimal_ddl()
        self.no_decimal_table_test()
        self.test_insert_decimal_values()
        time.sleep(9999999)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
