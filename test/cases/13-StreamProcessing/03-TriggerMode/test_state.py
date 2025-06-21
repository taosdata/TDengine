import time
import math
import os
from new_test_framework.utils import tdLog, tdSql, tdStream, StreamTableType, StreamTable, StreamItem, tdCom
import datetime

class TestStreamStateTrigger:
    caseName = "test_stream_state_trigger"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    dbname = "test1"
    subTblNum = 3
    tblRowNum = 40
    tableList = []

    querySqls = [
        ("select cts, cint from {calcTbname} order by cts, tbname asc limit 3",
         "select cts, cint, tbname tag_tbname from {calcTbname} order by cts, tbname asc limit 3",
         (0, False), True),
        ("select cts, cint from {calcTbname} order by cts desc, tbname asc limit 4",
         "select cts, cint, tbname tag_tbname from {calcTbname} order by cts desc, tbname asc limit 4", 
         (0, False), True),
        ("select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts from {calcTbname} order by cts, tbname asc",
         "select cts, cint,, tbname tag_tbname _tprev_ts, _tcurrent_ts, _tnext_ts from {calcTbname} order by cts, tbname asc",
         (0, False), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from {calcTbname} order by cts",
         "select cts, cint,, tbname tag_tbname _tgrpid, %%1, %%2, %%tbname from {calcTbname} order by cts",
         (2, True), True),
        ("select cts, cint from {calcTbname} where _tcurrent_ts % 2 = 0 order by cts",
         "select cts, cint, tbname tag_tbname from {calcTbname} where _tcurrent_ts % 2 = 0 order by cts",
         (0, False), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from {calcTbname} where %%tbname like '%1' order by cts",
         "select cts, cint,, tbname tag_tbname _tgrpid, %%1, %%2, %%tbname from {calcTbname} where %%tbname like '%1' order by cts",
         (2, True), True),
        ("select _tcurrent_ts, cint from {calcTbname} order by cts limit 4",
         "select _tcurrent_ts, cint, tbname tag_tbname from {calcTbname} order by cts limit 4",
         (0, False), True),

        ("select cts, cint from %%tbname order by cts limit 3",
         "select cts, cint, tbname tag_tbname from %%tbname order by cts limit 3",
         (1, True), True),
        ("select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts from %%tbname order by cts",
         "select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts, tbname tag_tbname from %%tbname order by cts",
         (1, True), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname order by cts",
         "select cts, cint, _tgrpid, %%1, %%2, %%tbname, tbname tag_tbname from %%tbname order by cts",
         (2, True), True),
        ("select cts, cint from %%tbname where _tcurrent_ts % 2 = 0 order by cts",
         "select cts, cint from %%tbname, tbname tag_tbname where _tcurrent_ts % 2 = 0 order by cts",
         (1, True), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname where %%tbname like '%1' order by cts",
         "select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname, tbname tag_tbname where %%tbname like '%1' order by cts",
         (2, True), True),
        ("select _tcurrent_ts, cint from %%tbname order by cts limit 7",
         "select _tcurrent_ts, cint from %%tbname order by cts limit 7",
         (1, True), True),

        ("select cts, cint from %%tbname partition by cint order by cts",
         "select cts, cint from %%tbname partition by cint order by cts",
         (1, True), True),
        ("select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts from %%tbname partition by cint order by cts",
         "select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts from %%tbname partition by cint order by cts",
         (1, True), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname partition by cint order by cts",
         "select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname partition by cint order by cts",
         (2, True), True),
        ("select cts, cint from %%tbname where _tcurrent_ts % 2 = 0 partition by cint order by cts",
         "select cts, cint from %%tbname where _tcurrent_ts % 2 = 0 partition by cint order by cts",
         (1, True), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname where %%tbname like '%1' partition by cint order by cts",
         "select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname where %%tbname like '%1' partition by cint order by cts",
         (2, True), True),
        ("select _tcurrent_ts, cint from %%tbname partition by cint order by cts",
         "select _tcurrent_ts, cint from %%tbname partition by cint order by cts",
         (1, True), True),
    ]
    createStreamSqls = [ # (SQL, PermitSuperTable)
        ("create stream {stName} state_window(cint) from {trigTbname} into {outTbname} as {querySql};", False),
        ("create stream {stName} state_window(cint) from {trigTbname} partition by tbname into {outTbname} as {querySql};", True),
        ("create stream {stName} state_window(cint) from {trigTbname} partition by cint, tbname into {outTbname} as {querySql};", True),
        ("create stream {stName} state_window(cint) from {trigTbname} partition by cvarchar, tbname, cint into {outTbname} as {querySql};", True),
    ]

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_state_trigger(self):
        """Stream state trigger

        Catalog:
            - Streams:TriggerMode

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-6-20 xsren Created

        """

        tdStream.createSnode()
        self.prepareData()

        if False == self.execCase():
            return

    def prepareData(self):
        tdLog.info(f"prepare data")

        tdStream.dropAllStreamsAndDbs()
        
        tdStream.init_database(self.dbname)
        
        st1 = StreamTable(self.dbname, "st1", StreamTableType.TYPE_SUP_TABLE)
        st1.createTable(3)
        st1.append_data(0, self.tblRowNum)
        
        self.tableList.append("st1")
        for i in range(1, self.subTblNum + 1):
            self.tableList.append(f"st1_{i}")
        
        ntb = StreamTable(self.dbname, "ntb1", StreamTableType.TYPE_NORMAL_TABLE)
        ntb.createTable()
        ntb.append_data(0, self.tblRowNum)
        self.tableList.append(f"ntb1")


    def execCase(self):
        tdLog.info(f"execCase begin")


        caseIdx = 0
        for self.trigTbIdx in range(len(self.tableList)):
            trigTbname = self.tableList[self.trigTbIdx]
            for self.calcTbIdx in range(len(self.tableList)):
                calcTbname = self.tableList[self.calcTbIdx]
                for createStmIdx in range(len(self.createStreamSqls)):
                    for queryIdx in range(len(self.querySqls)):
                        caseIdx += 1
                        stName = f"s{caseIdx}"
                        outTbname = f"{stName}_out"
                        
                        repaceDict = {"stName":stName, "outTbname":outTbname, "calcTbname":calcTbname, "trigTbname":trigTbname}
                        self.streamSql = self.createStreamSqls[createStmIdx][0].replace("{querySql}", self.querySqls[queryIdx][0]) \
                            .format(**repaceDict)
                        tdLog.info(f"create stream sql: {self.streamSql}")
                        if trigTbname == "st1" and self.createStreamSqls[createStmIdx][1] is False:
                            tdSql.error(self.streamSql)
                            continue
                        
                        stream1 = StreamItem(
                            id=caseIdx,
                            stream=self.streamSql,
                            res_query=f"select * from {self.dbname}.{outTbname};",
                            exp_query=self.querySqls[queryIdx][1].format(calcTbname=calcTbname),
                        )
                        stream1.createStream()
                        stream1.awaitStreamRunning()
                        stream1.checkResults()

        return True
