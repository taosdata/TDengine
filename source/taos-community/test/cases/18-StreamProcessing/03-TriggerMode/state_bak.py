import time
import math
import os
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamTableType,
    StreamTable,
    StreamItem,
    SafeDict,
    StreamResultCheckMode,
    QuerySqlCase,
    tdCom,
)
import datetime


class TestStreamStateTrigger:
    caseName = "test_stream_state_trigger"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    dbname = "test"
    subTblNum = 3
    tblRowNum = 40
    tableList = []
    createStreamSqls = [  # (SQL, PermitSuperTable)
        (
            "create stream {stName} state_window(cint) from {trigTbname} into {outTbname} as {querySql};",
            (),
        ),
        (
            "create stream {stName} state_window(cint) from {trigTbname} partition by tbname into {outTbname} as {querySql};",
            ("tbname",),
        ),
        (
            "create stream {stName} state_window(cint) from {trigTbname} partition by tint, tbname into {outTbname} as {querySql};",
            ("tint", "tbname"),
        ),
        (
            "create stream {stName} state_window(cint) from {trigTbname} partition by cvarchar, tbname, tint into {outTbname} as {querySql};",
            ("cvarchar", "tbname", "tint"),
        ),
    ]
    querySqls = [
        QuerySqlCase(
            query_sql="select cts, cint from {calcTbname} order by cts, tbname asc limit 3",
            expected_sql="select cts, cint {outTbTags} from {calcTbname} order by cts, tbname asc limit 3",
        ),
        QuerySqlCase(
            query_sql="select cts, cint from {calcTbname} where _twstart % 60000 = 0 order by cts",
            expected_sql="select cts, cint {outTbTags} from {calcTbname} order by cts, tbname",
            check_mode=StreamResultCheckMode.CHECK_ARRAY_BY_SQL,
        ),
        QuerySqlCase(
            query_sql="select cts, cint from {calcTbname} where _twstart % 60000 != 0 order by cts",
            expected_sql="select cts, cint {outTbTags} from {calcTbname} order by cts, tbname",
            check_mode=StreamResultCheckMode.CHECK_ARRAY_BY_SQL,
        ),
        QuerySqlCase(
            query_sql="select cts, cint from {calcTbname} order by cts",
            check_mode=StreamResultCheckMode.CHECK_BY_FILE,
            generate_file=False,
        ),
        # to do fix
        # QuerySqlCase(
        #     query_sql="select cts, cint from {calcTbname} order by cts desc, tbname asc limit 4",
        #     expected_sql="select cts, cint, tbname tag_tbname from {calcTbname} order by cts desc, tbname asc limit 4",
        #     generate_file=False,
        # ),
        # QuerySqlCase(
        #     query_sql="select cts, cint from {calcTbname} order by cts, tbname asc",
        #     expected_sql="select cts, cint, tbname tag_tbname from {calcTbname} order by cts, tbname asc",
        #     generate_file=False,
        # ),
        # QuerySqlCase(
        #     query_sql="select cts, cint, %%1, %%tbname from {calcTbname} order by cts",
        #     expected_sql="select cts, cint, tbname, tbname, tbname from {calcTbname} order by cts, tbname",
        #     generate_file=False,
        # ),

        # QuerySqlCase(
        #     query_sql="select _tcurrent_ts, cint from {calcTbname} order by cts limit 4",
        #     expected_sql="select _tcurrent_ts, cint, tbname tag_tbname from {calcTbname} order by cts limit 4",
        #     generate_file=False,
        # ),
        # QuerySqlCase(
        #     query_sql="select cts, cint from %%tbname order by cts limit 3",
        #     expected_sql="select cts, cint, tbname tag_tbname from %%tbname order by cts limit 3",
        #     generate_file=False,
        # ),
        # QuerySqlCase(
        #     query_sql="select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts from %%tbname order by cts",
        #     expected_sql="select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts, tbname tag_tbname from %%tbname order by cts",
        #     generate_file=False,
        # ),
        # QuerySqlCase(
        #     query_sql="select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname order by cts",
        #     expected_sql="select cts, cint, _tgrpid, %%1, %%2, %%tbname, tbname tag_tbname from %%tbname order by cts",
        #     generate_file=False,
        # ),
        # QuerySqlCase(
        #     query_sql="select cts, cint from %%tbname where _tcurrent_ts % 2 = 0 order by cts",
        #     expected_sql="select cts, cint from %%tbname, tbname tag_tbname where _tcurrent_ts % 2 = 0 order by cts",
        #     generate_file=False,
        # ),
        # QuerySqlCase(
        #     query_sql="select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname where %%tbname like '%1' order by cts",
        #     expected_sql="select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname, tbname tag_tbname where %%tbname like '%1' order by cts",
        #     generate_file=False,
        # ),
        # QuerySqlCase(
        #     query_sql="select _tcurrent_ts, cint from %%tbname order by cts limit 7",
        #     expected_sql="select _tcurrent_ts, cint from %%tbname order by cts limit 7",
        #     generate_file=False,
        # ),
        # QuerySqlCase(
        #     query_sql="select cts, cint from %%tbname partition by cint order by cts",
        #     expected_sql="select cts, cint from %%tbname partition by cint order by cts",
        #     generate_file=False,
        # ),
        # QuerySqlCase(
        #     query_sql="select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts from %%tbname partition by cint order by cts",
        #     expected_sql="select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts from %%tbname partition by cint order by cts",
        #     generate_file=False,
        # ),
        # QuerySqlCase(
        #     query_sql="select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname partition by cint order by cts",
        #     expected_sql="select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname partition by cint order by cts",
        #     generate_file=False,
        # ),
        # QuerySqlCase(
        #     query_sql="select cts, cint from %%tbname where _tcurrent_ts % 2 = 0 partition by cint order by cts",
        #     expected_sql="select cts, cint from %%tbname where _tcurrent_ts % 2 = 0 partition by cint order by cts",
        #     generate_file=False,
        # ),
        # QuerySqlCase(
        #     query_sql="select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname where %%tbname like '%1' partition by cint order by cts",
        #     expected_sql="select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname where %%tbname like '%1' partition by cint order by cts",
        #     generate_file=False,
        # ),
        # QuerySqlCase(
        #     query_sql="select _tcurrent_ts, cint from %%tbname partition by cint order by cts",
        #     expected_sql="select _tcurrent_ts, cint from %%tbname partition by cint order by cts",
        #     generate_file=False,
        # ),
    ]
    resultIdx = ""
    outTbname = ""
    res_query = ""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_state_trigger(self):
        """Stream state trigger

        xx
        
        Catalog:
            - Streams:TriggerMode

        Since: v3.3.3.7

        Labels: common,ci,skip

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
        for trigTbIdx in range(len(self.tableList)):
            trigTbname = self.tableList[trigTbIdx]
            for calcTbIdx in range(len(self.tableList)):
                calcTbname = self.tableList[calcTbIdx]
                for createStmIdx in range(len(self.createStreamSqls)):
                    for queryIdx in range(len(self.querySqls)):
                        caseIdx += 1
                        self.resultIdx = (
                            f"{trigTbIdx}_{calcTbIdx}_{createStmIdx}_{queryIdx}"
                        )
                        stName = f"s{caseIdx}"
                        self.outTbname = f"{stName}_out"

                        replaceDict = {
                            "stName": stName,
                            "outTbname": self.outTbname,
                            "calcTbname": calcTbname,
                            "trigTbname": trigTbname,
                        }
                        self.streamSql = (
                            self.createStreamSqls[createStmIdx][0]
                            .replace("{querySql}", self.querySqls[queryIdx].query_sql)
                            .format(**replaceDict)
                        )
                        tdLog.info(f"create stream sql: {self.streamSql}")
                        if (
                            trigTbname == "st1"
                            and "tbname" not in self.createStreamSqls[createStmIdx][1]
                        ):
                            tdSql.error(self.streamSql)
                            continue

                        if trigTbname == "st1":
                            res_query = f"select * from {self.dbname}.{self.outTbname} order by cts, tbname;"
                        else:
                            res_query = f"select * from {self.dbname}.{self.outTbname};"
                        stream1 = StreamItem(
                            id=caseIdx,
                            stream=self.streamSql,
                            res_query=res_query,
                            calc_tbname=calcTbname,
                            caseName=self.caseName,
                            result_idx=self.resultIdx,
                            out_tb_tags=self.createStreamSqls[createStmIdx][1],
                        )
                        stream1.addQuerySqlCase(self.querySqls[queryIdx])

                        if (
                            self.querySqls[queryIdx].check_mode
                            == StreamResultCheckMode.CHECK_BY_FILE
                        ):
                            currentDir = os.path.dirname(os.path.abspath(__file__))
                            stream1.setResultFile(
                                f"{currentDir}/ans/{self.caseName}.{self.resultIdx}.csv"
                            )

                        stream1.createStream()
                        stream1.awaitStreamRunning()
                        stream1.checkResultsByMode()

        return True
