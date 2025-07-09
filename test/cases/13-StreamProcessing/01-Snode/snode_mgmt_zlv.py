import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, StreamTableType, StreamTable, cluster
from random import randint
import os

class TestSnodeMgmt:
    caseName = "test_stream_sliding_trigger"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    runAll = False
    dbname = "test1"
    trigTbname = ""
    calcTbname = ""
    outTbname = ""
    stName = ""
    resultIdx = ""
    sliding = 1
    subTblNum = 3
    tblRowNum = 10
    tableList = []
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_snode_mgmt(self):
        """Snode mgmt test
        
        1. create snode
        2. show snode
        3. select ins_snodes
        4. drop snode
        5. show snode
        6. select ins_snodes
        7. recreate snode

        Catalog:
            - Streams:Snode

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-7-8 lvze Created

        """

        tdStream.dropAllStreamsAndDbs()
        
        self.prepareData()
        self.createSnodeTest()
        self.dropAllSnodeTest()
        self.createSnodeTest()
        self.dropOneDnode()
        self.createStream()
        
        tdSql.query("select * from information_schema.ins_snodes  order by id;")
        numOfSnodes=tdSql.getRows()
        for i in range(1,numOfSnodes+1):
            self.dropOneSnodeTest()
            
        tdSql.query("select * from information_schema.ins_streams  order by stream_name;")
        numOfStreams=tdSql.getRows()
        for i in range(1,numOfStreams+1):
            self.dropOneStream()

    def prepareData(self):
        tdLog.info(f"prepare data")

        tdStream.dropAllStreamsAndDbs()
        
        tdStream.init_database(self.dbname)
        
        st1 = StreamTable(self.dbname, "st1", StreamTableType.TYPE_SUP_TABLE)
        st1.createTable(3)
        st1.append_data(0, self.tblRowNum)
        
        self.tableList.append("st1")
        for i in range(0, self.subTblNum + 1):
            self.tableList.append(f"st1_{i}")
        
        ntb = StreamTable(self.dbname, "ntb1", StreamTableType.TYPE_NORMAL_TABLE)
        ntb.createTable()
        ntb.append_data(0, self.tblRowNum)
        self.tableList.append(f"ntb1")

    def checkResultRows(self, expectedRows):
        tdSql.checkResultsByFunc(
            f"select * from information_schema.ins_snodes order by id;",
            lambda: tdSql.getRows() == expectedRows,
            delay=0.5, retry=2
        )
    
    def createSnodeTest(self):
        tdLog.info(f"create snode test")
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfNodes=tdSql.getRows()
        tdLog.info(f"numOfNodes: {numOfNodes}")
        
        for i in range(1, numOfNodes + 1):
            tdSql.execute(f"create snode on dnode {i}")
            tdLog.info(f"create snode on dnode {i} success")
        self.checkResultRows(numOfNodes)
        
        tdSql.checkResultsByFunc(
            f"show snodes;", 
            lambda: tdSql.getRows() == numOfNodes,
            delay=0.5, retry=2
        )

    def dropAllSnodeTest(self):
        tdLog.info(f"drop all snode test")
        tdSql.query("select * from information_schema.ins_snodes order by id;")
        numOfSnodes = tdSql.getRows()
        tdLog.info(f"numOfSnodes: {numOfSnodes}")
        
        for i in range(1, numOfSnodes ):
            tdSql.execute(f"drop snode on dnode {i}")
            tdLog.info(f"drop snode {i} success")
            
        self.checkResultRows(1)
        
        tdSql.checkResultsByFunc(
            f"show snodes;", 
            lambda: tdSql.getRows() == 1,
            delay=0.5, retry=2
        )
        
        numOfRows=tdSql.execute(f"drop snode on dnode {numOfSnodes}")
        if numOfRows != 0:
            raise Exception(f" drop all snodes failed! ")
        tdSql.query("select * from information_schema.ins_snodes order by id;")
        numOfSnodes = tdSql.getRows()
        tdLog.info(f"After drop all snodes numOfSnodes: {numOfSnodes}")

    def dropOneSnodeTest(self):
        tdLog.info(f"drop one snode test")
        tdSql.query("select * from information_schema.ins_snodes  order by id;")
        numOfSnodes=tdSql.getRows()
        #只有一个 snode 的时候不再执行删除，因为无法删除掉
        if numOfSnodes >1:
            tdSql.query("select * from information_schema.ins_streams order by stream_name;")
            snodeid = tdSql.getData(0,6)
            tdSql.execute(f"drop snode on dnode {snodeid}")
            tdLog.info(f"drop snode {snodeid} success")
            
        
    def createStream(self):
        tdLog.info(f"create stream ")
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfNodes=tdSql.getRows()
        for i in range(1,numOfNodes+1):
            tdSql.execute(f"create stream `s{i}` sliding(1s) from st1 options(fill_history('2025-01-01 00:00:00')) into `s{i}out` as select cts, cint from st1 where _tcurrent_ts % 2 = 0 order by cts;")
            tdLog.info(f"create stream s{i} success!")
        # tdSql.execute("create stream `s2` sliding(1s) from st1 partition by tint, tbname options(fill_history('2025-01-01 00:00:00')) into `s2out` as select cts, cint from st1 order by cts limit 3;")
        # tdSql.execute("create stream `s3` sliding(1s) from st1 partition by tbname options(pre_filter(cint>2)|fill_history('2025-01-01 00:00:00')) into `s3out` as select cts, cint,   %%tbname from %%trows where cint >15 and tint >0 and  %%tbname like '%2' order by cts;")
        # tdSql.execute("create stream `s4` sliding(1s) from st1 options(fill_history('2025-01-01 00:00:00')) into `s4out` as select _tcurrent_ts, cint from st1 order by cts limit 4;")
        
        
    def dropOneStream(self):
        tdLog.info(f"drop one stream: ")
        tdSql.query("select * from information_schema.ins_streams order by stream_name;")
        numOfStreams = tdSql.getRows()
        tdLog.info(f"Total streams:{numOfStreams}")
        streamid = tdSql.getData(0,0)
        tdSql.execute(f"drop stream {streamid}")
        tdLog.info(f"drop stream {streamid} success")
        
        tdSql.query("select * from information_schema.ins_streams order by stream_name;")
        numOfStreams = tdSql.getRows()
        tdLog.info(f"Total streams:{numOfStreams}")
    
    def dropOneDnode(self):
        tdLog.info(f"drop one dnode: ")
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfDnodes = tdSql.getRows()
        tdLog.info(f"Total dnodes:{numOfDnodes}")
        if numOfDnodes >1:
            dnodeid = tdSql.getData(1,0)
            tdSql.execute(f"drop dnode {dnodeid}")
            tdLog.info(f"drop dnode {dnodeid} success")
        
            tdSql.query("select * from information_schema.ins_dnodes order by id;")
            numOfDnodes = tdSql.getRows()
            tdLog.info(f"Total dnodes:{numOfDnodes}")