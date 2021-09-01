#!/usr/bin/python3.8

from abc import abstractmethod

import time
from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision, BucketsApi
from influxdb_client.client.write_api import SYNCHRONOUS

import argparse
import textwrap
import subprocess
import sys

import taos

from crash_gen.crash_gen_main import Database, TdSuperTable
from crash_gen.service_manager import TdeInstance

from crash_gen.shared.config import Config
from crash_gen.shared.db import DbConn
from crash_gen.shared.misc import Dice, Logging, Helper
from crash_gen.shared.types import TdDataType


# NUM_PROCESSES   = 10
# NUM_REPS        = 1000

tick = int(time.time() - 5000000.0)  # for now we will create max 5M record
value = 101

DB_NAME = 'mydb'
TIME_SERIES_NAME = 'widget'

MAX_SHELF = 500 # shelf number runs up to this, non-inclusive
ITEMS_PER_SHELF = 5
BATCH_SIZE = 2000 # Number of data points per request

# None_RW:
# INFLUX_TOKEN='RRzVQZs8ERCpV9cS2RXqgtM_Y6FEZuJ7Tuk0aHtZItFTfcM9ajixtGDhW8HzqNIBmG3hmztw-P4sHOstfJvjFA=='
# DevOrg_RW:
# INFLUX_TOKEN='o1P8sEhBmXKhxBmNuiCyOUKv8d7qm5wUjMff9AbskBu2LcmNPQzU77NrAn5hDil8hZ0-y1AGWpzpL-4wqjFdkA=='
# DevOrg_All_Access
INFLUX_TOKEN='T2QTr4sloJhINH_oSrwSS-WIIZYjDfD123NK4ou3b7ajRs0c0IphCh3bNc0OsDZQRW1HyCby7opdEndVYFGTWQ=='
INFLUX_ORG="DevOrg"
INFLUX_BUCKET="Bucket01"

def writeTaosBatch(dbc, tblName):
    # Database.setupLastTick()
    global value, tick
    
    data = []
    for i in range(0, 100):
        data.append("('{}', {})".format(Database.getNextTick(), value) )
        value += 1

    sql = "INSERT INTO {} VALUES {}".format(tblName, ''.join(data))
    dbc.execute(sql)

class PerfGenError(taos.error.ProgrammingError):
    pass

class Benchmark():

    # @classmethod
    # def create(cls, dbType):
    #     if dbType == 'taos':
    #         return TaosBenchmark()
    #     elif dbType == 'influx':
    #         return InfluxBenchmark()
    #     else:
    #         raise RuntimeError("Unknown DB type: {}".format(dbType))

    def __init__(self, dbType, loopCount = 0):
        self._dbType = dbType
        self._setLoopCount(loopCount)        

    def _setLoopCount(self, loopCount):
        cfgLoopCount = Config.getConfig().loop_count
        if loopCount == 0: # use config
            self._loopCount = cfgLoopCount
        else:
            if cfgLoopCount :
                Logging.warning("Ignoring loop count for fixed-loop-count benchmarks: {}".format(cfgLoopCount))
            self._loopCount = loopCount

    @abstractmethod
    def doIterate(self): 
        '''
        Execute the benchmark directly, without invoking sub processes,
        effectively using one execution thread.
        '''
        pass

    @abstractmethod
    def prepare(self): 
        '''
        Preparation needed to run a certain benchmark
        '''
        pass

    @abstractmethod
    def execute(self): 
        '''
        Actually execute the benchmark
        '''
        Logging.warning("Unexpected execution")

    @property
    def name(self):
        return self.__class__.__name__

    def run(self):
        print("Running benchmark: {}, class={} ...".format(self.name, self.__class__))    
        startTime = time.time()

        # Prepare to execute the benchmark
        self.prepare()    

        # Actually execute the benchmark
        self.execute()

        # if Config.getConfig().iterate_directly: # execute directly
        #     Logging.debug("Iterating...")
        #     self.doIterate()
        # else:
        #     Logging.debug("Executing via sub process...")
        #     startTime = time.time()
        #     self.prepare()      
        #     self.spawnProcesses()
        #     self.waitForProcecess()
        #     duration = time.time() - startTime
        #     Logging.info("Benchmark execution completed in {:.3f} seconds".format(duration))
        Logging.info("Benchmark {} finished in {:.3f} seconds".format(
            self.name, time.time()-startTime))

    def spawnProcesses(self):        
        self._subProcs = []
        for j in range(0, Config.getConfig().subprocess_count):
            ON_POSIX = 'posix' in sys.builtin_module_names
            tblName = 'cars_reg_{}'.format(j)
            cmdLineStr = './perf_gen.sh -t {} -i -n {} -l {}'.format(
                    self._dbType, 
                    tblName,
                    Config.getConfig().loop_count
                    )
            if Config.getConfig().debug:
                cmdLineStr += ' -d'
            subProc = subprocess.Popen(cmdLineStr,
                shell = True,
                close_fds = ON_POSIX)
            self._subProcs.append(subProc)

    def waitForProcecess(self):
        for sp in self._subProcs:
            sp.wait(300)


class TaosBenchmark(Benchmark):
    
    def __init__(self, loopCount):
        super().__init__('taos', loopCount)
        # self._dbType = 'taos'
        tInst = TdeInstance()
        self._dbc = DbConn.createNative(tInst.getDbTarget())
        self._dbc.open()
        self._sTable = TdSuperTable(TIME_SERIES_NAME + '_s', DB_NAME)    

    def doIterate(self):    
        tblName = Config.getConfig().target_table_name
        print("Benchmarking TAOS database (1 pass) for: {}".format(tblName))
        self._dbc.execute("USE {}".format(DB_NAME))

        self._sTable.ensureRegTable(None, self._dbc, tblName)
        try:
            lCount = Config.getConfig().loop_count
            print("({})".format(lCount))
            for i in range(0, lCount):            
                writeTaosBatch(self._dbc, tblName)    
        except taos.error.ProgrammingError as err:
            Logging.error("Failed to write batch")

    def prepare(self):        
        self._dbc.execute("CREATE DATABASE IF NOT EXISTS {}".format(DB_NAME))
        self._dbc.execute("USE {}".format(DB_NAME))
        # Create the super table
        self._sTable.drop(self._dbc, True)
        self._sTable.create(self._dbc,
                            {'ts': TdDataType.TIMESTAMP, 
                             'temperature': TdDataType.INT,
                             'pressure': TdDataType.INT,
                             'notes': TdDataType.BINARY200
                            },
                            {'rack': TdDataType.INT, 
                             'shelf': TdDataType.INT, 
                             'barcode': TdDataType.BINARY16
                            })

    def execSql(self, sql):
        try:
            self._dbc.execute(sql)
        except taos.error.ProgrammingError as err:
            Logging.warning("SQL Error: 0x{:X}, {}, SQL: {}".format(
                Helper.convertErrno(err.errno), err.msg, sql))
            raise

    def executeWrite(self):
        # Sample: INSERT INTO t1 USING st TAGS(1) VALUES(now, 1) t2 USING st TAGS(2) VALUES(now, 2)
        sqlPrefix  = "INSERT INTO "
        dataTemplate = "{} USING {} TAGS({},{},'barcode_{}') VALUES('{}',{},{},'{}') "

        stName = self._sTable.getName()            
        BATCH_SIZE = 2000 # number of items per request batch
        ITEMS_PER_SHELF = 5

        # rackSize = 10 # shelves per rack
        # shelfSize = 100  # items per shelf
        batchCount = self._loopCount // BATCH_SIZE
        lastRack = 0
        for i in range(batchCount):
            sql = sqlPrefix
            for j in range(BATCH_SIZE):
                n = i*BATCH_SIZE + j # serial number
                # values first
                # rtName = 'rt_' + str(n) # table name contains serial number, has info
                temperature = 20 + (n % 10)
                pressure = 70 + (n % 10)
                # tags
                shelf = (n // ITEMS_PER_SHELF) % MAX_SHELF # shelf number
                rack  = n // (ITEMS_PER_SHELF * MAX_SHELF) # rack number
                barcode = rack + shelf
                # table name
                tableName = "reg_" + str(rack) + '_' + str(shelf)
                # now the SQL
                sql += dataTemplate.format(tableName, stName,# table name
                    rack, shelf, barcode,  # tags
                    Database.getNextTick(), temperature, pressure, 'xxx') # values
                lastRack = rack
            self.execSql(sql)
        Logging.info("Last Rack: {}".format(lastRack))

class TaosWriteBenchmark(TaosBenchmark):
    def execute(self):
        self.executeWrite()
    
class Taos100kWriteBenchmark(TaosWriteBenchmark):
    def __init__(self):
        super().__init__(100*1000)

class Taos10kWriteBenchmark(TaosWriteBenchmark):
    def __init__(self):
        super().__init__(10*1000)

class Taos1mWriteBenchmark(TaosWriteBenchmark):
    def __init__(self):
        super().__init__(1000*1000)

class Taos5mWriteBenchmark(TaosWriteBenchmark):
    def __init__(self):
        super().__init__(5*1000*1000)

class Taos1kQueryBenchmark(TaosBenchmark):
    def __init__(self):
        super().__init__(1000)

class Taos1MCreationBenchmark(TaosBenchmark):
    def __init__(self):
        super().__init__(1000000)


class InfluxBenchmark(Benchmark):
    def __init__(self, loopCount):
        super().__init__('influx', loopCount)
        # self._dbType = 'influx'

        
        # self._client = InfluxDBClient(host='localhost', port=8086)

    # def _writeBatch(self, tblName):
    #     global value, tick
    #     data = []
    #     for i in range(0, 100):
    #         line = "{},device={} value={} {}".format(
    #             TIME_SERIES_NAME,
    #             tblName, 
    #             value, 
    #             tick*1000000000)
    #         # print(line)
    #         data.append(line)
    #         value += 1
    #         tick +=1

    #     self._client.write(data, {'db':DB_NAME}, protocol='line')

    def executeWrite(self):
        global tick # influx tick #TODO refactor

        lineTemplate = TIME_SERIES_NAME + ",rack={},shelf={},barcode='barcode_{}' temperature={},pressure={} {}"

        batchCount = self._loopCount // BATCH_SIZE        
        for i in range(batchCount):
            lineBatch = []
            for j in range(BATCH_SIZE):
                n = i*BATCH_SIZE + j # serial number
                # values first
                # rtName = 'rt_' + str(n) # table name contains serial number, has info
                temperature = 20 + (n % 10)
                pressure = 70 + (n % 10)
                # tags
                shelf = (n // ITEMS_PER_SHELF) % MAX_SHELF # shelf number
                rack  = n // (ITEMS_PER_SHELF * MAX_SHELF) # rack number
                barcode = rack + shelf
                # now the SQL
                line = lineTemplate.format(
                    rack, shelf, barcode,  # tags
                    temperature, pressure, # values
                    tick * 1000000000 )
                tick += 1
                lineBatch.append(line)
            write_api = self._client.write_api(write_options=SYNCHRONOUS)
            write_api.write(INFLUX_BUCKET, INFLUX_ORG, lineBatch)
            # self._client.write(lineBatch, {'db':DB_NAME}, protocol='line')

    # def doIterate(self):    
    #     tblName = Config.getConfig().target_table_name
    #     print("Benchmarking INFLUX database (1 pass) for: {}".format(tblName))

    #     for i in range(0, Config.getConfig().loop_count):            
    #         self._writeBatch(tblName)    

    def _getOrgIdByName(self, orgName):
        """Find org by name.

        """
        orgApi = self._client.organizations_api()
        orgs = orgApi.find_organizations()
        for org in orgs:
            if org.name == orgName:
                return org.id
        raise PerfGenError("Org not found with name: {}".format(orgName))

    def _fetchAuth(self):        
        authApi = self._client.authorizations_api()
        auths = authApi.find_authorizations()
        for auth in auths:
            if auth.token == INFLUX_TOKEN :
                return auth
        raise PerfGenError("No proper auth found")

    def _verifyPermissions(self, perms: list):
        if list:
            return #OK
        raise PerfGenError("No permission found")

    def prepare(self):        
        self._client = InfluxDBClient(
            url="http://127.0.0.1:8086", 
            token=INFLUX_TOKEN, 
            org=INFLUX_ORG)

        auth = self._fetchAuth()

        self._verifyPermissions(auth.permissions)

        bktApi = self._client.buckets_api()
        # Delete
        bkt = bktApi.find_bucket_by_name(INFLUX_BUCKET)
        if bkt:
            bktApi.delete_bucket(bkt)
        # Recreate

        orgId = self._getOrgIdByName(INFLUX_ORG)
        bktApi.create_bucket(bucket=None, bucket_name=INFLUX_BUCKET, org_id=orgId)
        
        # self._client.drop_database(DB_NAME)
        # self._client.create_database(DB_NAME)
        # self._client.switch_database(DB_NAME)

class InfluxWriteBenchmark(InfluxBenchmark):
    def execute(self):
        return self.executeWrite()

class Influx10kWriteBenchmark(InfluxWriteBenchmark):
    def __init__(self):
        super().__init__(10*1000)

class Influx100kWriteBenchmark(InfluxWriteBenchmark):
    def __init__(self):
        super().__init__(100*1000)

class Influx1mWriteBenchmark(InfluxWriteBenchmark):
    def __init__(self):
        super().__init__(1000*1000)

class Influx5mWriteBenchmark(InfluxWriteBenchmark):
    def __init__(self):
        super().__init__(5*1000*1000)

def _buildCmdLineParser():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''\
            TDengine Performance Benchmarking Tool
            ---------------------------------------------------------------------
            
            '''))
    
    parser.add_argument(
        '-b',
        '--benchmark-name',
        action='store',
        default='Taos1kQuery',
        type=str,
        help='Benchmark to use (default: Taos1kQuery)')

    parser.add_argument(
        '-d',
        '--debug',
        action='store_true',
        help='Turn on DEBUG mode for more logging (default: false)')

    parser.add_argument(
        '-i',
        '--iterate-directly',
        action='store_true',
        help='Execution operations directly without sub-process (default: false)')

    parser.add_argument(
        '-l',
        '--loop-count',
        action='store',
        default=1000,
        type=int,
        help='Number of loops to perform, 100 operations per loop. (default: 1000)')        

    parser.add_argument(
        '-n',
        '--target-table-name',
        action='store',
        default=None,
        type=str,
        help='Regular table name in target DB (default: None)')

    parser.add_argument(
        '-s',
        '--subprocess-count',
        action='store',
        default=4,
        type=int,
        help='Number of sub processes to spawn. (default: 10)')        

    parser.add_argument(
        '-t',
        '--target-database',
        action='store',
        default='taos',
        type=str,
        help='Benchmark target: taos, influx (default: taos)')

    return parser

def main():
    parser = _buildCmdLineParser()
    Config.init(parser)
    Logging.clsInit(Config.getConfig().debug)
    Dice.seed(0)  # initial seeding of dice
    
    bName = Config.getConfig().benchmark_name
    bClassName = bName + 'Benchmark'
    x = globals()
    if bClassName in globals():
        bClass = globals()[bClassName]
        bm = bClass() # Benchmark object
        bm.run()
    else:
        raise PerfGenError("No such benchmark: {}".format(bName))

    # bm = Benchmark.create(Config.getConfig().target_database)
    # bm.run()

if __name__ == "__main__":
    main()
    
    
