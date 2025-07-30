from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")



    def test_insert_basic(self):
        """insert use ns precision

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

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

        i = 1
        tb_orderd = tbPrefix + str(i)

        tdSql.execute(f"create database {db} vgroups 2 precision 'ns'")
        tdSql.execute(f"use {db}")

        tdLog.info(f"=========== step1 check ordered data")
        self.createTableAndBasicCheck(db, tb, 2000, 1)


        tdLog.info(f"=============step2 check unordered data") 
        self.createTableAndBasicCheck(db, tb_orderd, 100, 0)


        tdSql.execute(f"drop database {db}")

    def checkBlobResult(self, tb, expectColSet, count):
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"{tdSql.getRows()}) points data are retrieved")
        tdSql.checkRows(count)
        for i in range(tdSql.queryRows): 
            if expectColSet[i] != None: 
                result = expectColSet[i].encode()
                tdSql.checkData(i, 1, result)
            else:
                tdSql.checkData(i, 1, expectColSet[i])
    def insertBatchData(self, tb, count, colSet, ordered):
        x = 0   
        str1 = f"insert into {tb} values"
        if ordered == 1: 
            while x < count:
                blobCol = "" 
                if x/10 == 0:
                    blobCol = None 
                    str1 = str1 + f'(now, NULL)'
                elif x/50 == 0:
                    blobCol = ""
                    str1 = str1 + f'(now, "{blobCol}")'
                else: 
                    blobCol = f"BLOB_{x}"
                    str1 = str1 +  f'(now, "{blobCol}")'
                colSet.append(blobCol) 
                x = x + 1       
        else:
            while x < count:
                blobCol = "" 
                if x/10 == 0:
                    blobCol = None 
                    str1 = str1 + f'(now - 1h, NULL)'
                elif x/50 == 0:
                    blobCol = ""
                    str1 = str1 + f'(now - 1h, "{blobCol}")'
                else: 
                    blobCol = f"BLOB_{x}"
                    str1 = str1 +  f'(now - 1h, "{blobCol}")'
                colSet.append(blobCol) 
                x = x + 1       
        tdSql.execute(str1)
    def createTableAndBasicCheck(self, db, tb, batchCount, ordered):

        tdSql.execute(f"create table {tb} (ts timestamp, desc1 blob)")
        count = 0
        colSet = []

        self.insertBatchData(tb, batchCount, colSet, ordered)
        count = count + batchCount

        # query before commit

        self.checkBlobResult(tb, colSet, count)

        tdSql.flushDb(f"{db}")
        self.checkBlobResult(tb, colSet, count)

        self.insertBatchData(tb, batchCount, colSet, ordered)
        count = count + batchCount

        self.checkBlobResult(tb, colSet, count)

        tdSql.flushDb(f"{db}")

        self.checkBlobResult(tb, colSet, count)
        tdSql.execute(f"drop database {db}")
