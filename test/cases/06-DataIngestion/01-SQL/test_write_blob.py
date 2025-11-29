from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck

import time

class TestInsertBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")


    
    def test_insert_basic(self):
        """Write ns precision

        1. create table
        2. insert data
        3. query data

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-7-30 yhDeng create  

        """

        i = 0
        dbPrefix = "d"
        tbPrefix = "t"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdSql.execute(f"create database {db} vgroups 2")
        tdSql.execute(f"use {db}")

        tdLog.info(f"=========== step1 check ordered data")
        self.createTableAndBasicCheck(db, tb, 1000, 1)


        tdLog.info(f"=============step2 check unordered data") 
        for i in range(2):
            tdLog.info(f"=============i check unordered data") 
            tb_orderd = tbPrefix + str(i + 10)
            self.createTableAndBasicCheck(db, tb_orderd, 10000, 0)

        tdSql.execute(f"drop database {db}")

    def checkBlobResult(self, tb, expectColSet, count):
        tdSql.query(f"select * from {tb} limit 100000")
        tdLog.info(f"{tdSql.getRows()}) points data are retrieved")
        tdSql.checkRows(len(expectColSet))
        
        for i in range(len(expectColSet)): 
            #timestamp = str(tdSql.getData(i, 0))
            #sqlCol = tdSql.getData(i, 1) 
            #print(f"timestamp {timestamp}, data {sqlCol}")
            #print(f"check data {i} {tdSql.getData(i, 0)}, expect {expectColSet[i]}")
            result = expectColSet[i]   
            tdLog.debug((f"check data {i} {tdSql.getData(i, 0)}"))
            if result is not None:
                encResult = result.encode()
                tdSql.checkData(i, 1, encResult)
            else:
                tdSql.checkData(i, 1, result)

    def insertBatchData(self, tb, count, colSet, startTs, ordered):
        x = 0
        str1 = f"insert into {tb} values"
        if ordered == 1:
            while x < count:
                blobCol = "" 
                if x%50 == 0:
                    blobCol = None 
                    str1 = str1 + f'({startTs}, NULL)'
                elif x%10 == 0:
                    blobCol = ""
                    str1 = str1 + f'({startTs}, "{blobCol}")'
                else: 
                    blobCol = f"BLOB_{x}"
                    str1 = str1 +  f'({startTs}, "{blobCol}")'
                colSet.append(blobCol)  
                startTs = startTs + 1
                x = x + 1       
        else:
            while x < count:
                blobCol = "" 
                if x%50 == 0:
                    blobCol = None 
                    str1 = str1 + f'({startTs}, NULL)'
                elif x%10 == 0:
                    blobCol = ""
                    str1 = str1 + f'({startTs}, "{blobCol}")'
                else: 
                    blobCol = f"BLOB_{x}"
                    str1 = str1 +  f'({startTs}, "{blobCol}")'
                colSet.append(blobCol)  
                startTs = startTs + 1
                x = x + 1       
        tdSql.execute(str1)
    def createTableAndBasicCheck(self, db, tb, batchCount, ordered):

        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, desc1 blob)")
        count = 0
        colSet = [] 

        self.insertBatchData(tb, batchCount, colSet, 1717122968000,ordered)
        count = count + batchCount

        self.checkBlobResult(tb, colSet, count)
        tdSql.flushDb(f"{db}")
        self.insertBatchData(tb, batchCount, colSet, 1717122968000 + batchCount*10, ordered)
        count = count + batchCount

        self.checkBlobResult(tb, colSet, count)
