import os
import taos
from taos import *

from ctypes import *
from taos.constants import FieldType
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class StmtCommon:

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    # compare
    def compare_result(self, conn, sql2, res2):
        lres1 = []
        lres2 = []
    
        # shor res2
        for row in res2:
            tdLog.debug(f" res2 rows = {row} \n")
            lres2.append(row)

        res1 = conn.query(sql2)
        for row in res1:
            tdLog.debug(f" res1 rows = {row} \n")
            lres1.append(row)

        row1 = len(lres1)
        row2 = len(lres2)
        if row1 != row2:
            tdLog.exit(f"two results row count different. row1={row1} row2={row2} sql={sql2}")

        if row1 == 0:
            return

        col1 = len(lres1[0])
        col2 = len(lres2[0])
        if col1 != col2:
            tdLog.exit(f"two results column count different. col1={col1} col2={col2}")

        for i in range(row1):
            for j in range(col1):
                if lres1[i][j] != lres2[i][j]:
                    tdLog.exit(f" two results data different. i={i} j={j} data1={res1[i][j]} data2={res2[i][j]}\n")


    def compareLine(self, oris, rows):
        n = len(oris)
        if len(rows) != n:
            return False
        for i in range(n):
            if oris[i] != rows[i]:
                if type(rows[i]) == bool:
                    if bool(oris[i]) != rows[i]:
                        return False
        return True


    def checkResultCorrect(self, conn, sql, tagsTb, datasTb):
        # column to rows
        tdLog.debug(f"check sql correct: {sql}\n")
        oris = []
        ncol = len(datasTb)
        nrow = len(datasTb[0]) 

        for i in range(nrow):
            row = []
            for j in range(ncol):
                if j == 0:
                    # ts column
                    c0 = datasTb[j][i]
                    if type(c0) is int :
                        row.append(datasTb[j][i])
                    else:
                        ts = int(conn.bind2._datetime_to_timestamp(c0, PrecisionEnum.Milliseconds))
                        row.append(ts)
                else:
                    row.append(datasTb[j][i])

            if tagsTb is not None:
                row += tagsTb
            oris.append(row)
        
        # fetch all
        lres = []
        tdLog.debug(sql)
        res = conn.query(sql)
        i = 0
        for row in res:
            lrow = list(row)
            lrow[0] = int(lrow[0].timestamp()*1000)
            if self.compareLine(oris[i], lrow) is False:
                tdLog.info(f"insert data differet. i={i}")
                tdLog.info(f"expect ori data={oris[i]}")
                tdLog.info(f"query from db ={lrow}")
                raise(BaseException("check insert data correct failed."))
            else:
                tdLog.debug(f"i={i} origin data same with get from db\n")
                tdLog.debug(f" origin data = {oris[i]} \n")
                tdLog.debug(f" get from db = {lrow} \n")
            i += 1


    def checkResultCorrects(self, conn, dbname, stbname, tbnames, tags, datas):
        count = len(tbnames)
        for i in range(count):
            if stbname is None:
                sql = f"select * from {dbname}.{tbnames[i]} "
                self.checkResultCorrect(conn, sql, None, datas[i])
            else:
                sql = f"select * from {dbname}.{stbname} where tbname='{tbnames[i]}' "
                if tags is None:
                    self.checkResultCorrect(conn, sql, None, datas[i])
                else:
                    self.checkResultCorrect(conn, sql, tags[i], datas[i])

        print("insert data check correct ..................... ok\n")