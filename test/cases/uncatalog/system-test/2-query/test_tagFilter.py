from new_test_framework.utils import tdLog, tdSql
import datetime


INT_COL     = "c1"
BINT_COL    = "c2"
SINT_COL    = "c3"
TINT_COL    = "c4"
FLOAT_COL   = "c5"
DOUBLE_COL  = "c6"
BOOL_COL    = "c7"

BINARY_COL  = "c8"
NCHAR_COL   = "c9"
TS_COL      = "c10"

NUM_COL = [INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, ]
UN_NUM_COL = [BOOL_COL, BINARY_COL, NCHAR_COL, ]
TS_TYPE_COL = [TS_COL]

DBNAME = "db"

class TestTagfilter:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)     
        
    def __create_tb(self, dbname="db"):
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, f1 int
            ) tags (tag1 binary(16300))
            '''
        tdSql.execute(create_stb_sql)

        tag_value = 'a'
        for i in range(1200):
            tag_value = tag_value + 'a'

        for i in range(8000):
            tdSql.execute(f"create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( '{tag_value}' )")

    def __query_data(self, rows, dbname="db"):
        tdSql.execute(
            f'''select count(*) from {dbname}.stb1 where tag1 like '%a'
            '''
        )
        tdSql.checkRows(0)

    def __ts4421(self, dbname="db", stbname='stb4421', ctbname='ctb4421'):
        TAG_BIND             = [True, False]
        TAG_TYPE             = ['varchar', 'nchar']
        TAG_LEN              = [2, 8, 200]
        TAG_VAL_INT          = [0, -200, 123456789]
        TAG_VAL_STR          = ["noW()", "now", "'now'", "todAy()", "today", "\"today\"" ]
        TAG_VAL_BOOL_INT     = [ -1, 1, 0, -0]
        TAG_VAL_BOOL_STR     = ["TrUe", "\"true\"","fALse", "'FALSE'"]
        TAG_VAL_TIMESTAMP    = ["now()", "NoW", "'now'", "\"now()\"", "toDay()", "toDaY", "'today'", "\"today()\"", "\"2200-01-01 08:00:00\"", "'2200-01-02'","\"2200-01-02T00:00:00.000Z\"", "'2200-01-02T00:00:00.000'", "2200-01-01 08:00:00",  "\"2200-01-02'", "2200-01-02T00:00:00.000Z"]
        TAG_RESULT_INT       = [True,False,False,True,True,False,True,True,True,True,False,False,True,True,False,True,True,True]
        TAG_RESULT_STR       = [False,False,False,False,False,False,False,True,True,False,True,True,False,True,True,False,True,True,False,False,False,False,False,False,False,True,True,False,True,True,False,True,True,False,True,True]
        TAG_RESULT_BOOL      = ["True","True","False","False"]
        TAG_RESULT_TIMESTAMP = [True, True, True, True, True, True, True, True, True, True, True, True, False, False, False]        

        # check int for vartype(one tag)
        nTagCtb = 0
        for tagType in TAG_TYPE:
            for tagLen in TAG_LEN:
                tdSql.execute(f'create stable {dbname}.{stbname}(ts timestamp, f1 int) tags(t1 %s(%d))'%(tagType,tagLen))
                for tagVal in TAG_VAL_INT:
                    for tagBind in TAG_BIND:
                        if tagBind == True:
                            bindStr = "(t1)"
                        else:
                            bindStr = ""
                        tdLog.info(f'nTagCtb={nTagCtb}, tagType={tagType}, tagLen = {tagLen}, tagVal = {tagVal}, tagBind={tagBind}')
                        if TAG_RESULT_INT[nTagCtb] == False:
                            tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags(%d)'%(bindStr,tagVal))
                            tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} %s tags(%d) values(now,1)'%(bindStr,tagVal))
                            tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags("%d")'%(bindStr,tagVal))
                            tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} %s tags("%d") values(now,1)'%(bindStr,tagVal))
                            tdSql.error(f"create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags('%d')"%(bindStr,tagVal))
                            tdSql.error(f"insert into {dbname}.{ctbname} using {dbname}.{stbname} %s tags('%d') values(now,1)"%(bindStr,tagVal))
                        else:
                            # integer as tag value
                            tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags(%d)'%(bindStr,tagVal))
                            tdSql.execute(f'insert into {dbname}.{ctbname} values(now,1)')
                            tdSql.execute(f'insert into {dbname}.{ctbname}t using {dbname}.{stbname} %s tags(%d) values(now,1)'%(bindStr,tagVal))
                            tdSql.query(f'select * from {dbname}.{stbname} where t1="%d"'%(tagVal))
                            tdSql.checkRows(2)
                            tdSql.execute(f'drop table {dbname}.{ctbname}')
                            tdSql.execute(f'drop table {dbname}.{ctbname}t')
                            # string as tag value
                            tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags("%d")'%(bindStr,tagVal))
                            tdSql.execute(f'insert into {dbname}.{ctbname} values(now,1)')
                            tdSql.execute(f'insert into {dbname}.{ctbname}t using {dbname}.{stbname} %s tags("%d") values(now,1)'%(bindStr,tagVal))
                            tdSql.query(f'select * from {dbname}.{stbname} where t1="%d"'%(tagVal))
                            tdSql.checkRows(2)
                            tdSql.execute(f'drop table {dbname}.{ctbname}')
                            tdSql.execute(f'drop table {dbname}.{ctbname}t')
                            tdSql.execute(f"create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags('%d')"%(bindStr,tagVal))
                            tdSql.execute(f"insert into {dbname}.{ctbname} values(now,1)")
                            tdSql.execute(f"insert into {dbname}.{ctbname}t using {dbname}.{stbname} %s tags('%d') values(now,1)"%(bindStr,tagVal))
                            tdSql.query(f"select * from {dbname}.{stbname} where t1='%d'"%(tagVal))
                            tdSql.checkRows(2)
                            tdSql.execute(f'drop table {dbname}.{ctbname}')
                            tdSql.execute(f'drop table {dbname}.{ctbname}t')
                    nTagCtb += 1
                tdSql.execute(f'drop table {dbname}.{stbname}')
    
        # check int for vartype(two tags/bind tags)
        nTagCtb = 0
        for tagType in TAG_TYPE:
            for tagLen in TAG_LEN:
                tdSql.execute(f'create stable {dbname}.{stbname}(ts timestamp, f1 int) tags(t1 %s(%d),t2 %s(%d) )'%(tagType,tagLen,tagType,tagLen))
                for tagVal in TAG_VAL_INT:
                    for tagBind in TAG_BIND:
                        if tagBind == True:
                            bindStr = "(t1,t2)"
                        else:
                            bindStr = ""
                        if TAG_RESULT_INT[nTagCtb] == False:
                            tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags(%d,%d)'%(bindStr,tagVal,tagVal))
                            tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} %s tags(%d,%d) values(now,1)'%(bindStr,tagVal,tagVal))
                            tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags("%d","%d")'%(bindStr,tagVal,tagVal))
                            tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} %s tags("%d","%d") values(now,1)'%(bindStr,tagVal,tagVal))
                            tdSql.error(f"create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags('%d','%d')"%(bindStr,tagVal,tagVal))
                            tdSql.error(f"insert into {dbname}.{ctbname} using {dbname}.{stbname} %s tags('%d','%d') values(now,1)"%(bindStr,tagVal,tagVal))
                        else:
                            # integer as tag value
                            tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags(%d,%d)'%(bindStr,tagVal,tagVal))
                            tdSql.execute(f'insert into {dbname}.{ctbname} values(now,1)')
                            tdSql.execute(f'insert into {dbname}.{ctbname}t using {dbname}.{stbname} %s tags(%d,%d) values(now,1)'%(bindStr,tagVal,tagVal))
                            tdSql.query(f"select * from {dbname}.{stbname} where t1='%d' and t2='%d'"%(tagVal,tagVal))
                            tdSql.checkRows(2)
                            tdSql.execute(f'drop table {dbname}.{ctbname}')
                            tdSql.execute(f'drop table {dbname}.{ctbname}t')
                            # string as tag value
                            tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags("%d","%d")'%(bindStr,tagVal,tagVal))
                            tdSql.execute(f'insert into {dbname}.{ctbname} values(now,1)')
                            tdSql.execute(f'insert into {dbname}.{ctbname}t using {dbname}.{stbname} %s tags("%d","%d") values(now,1)'%(bindStr,tagVal,tagVal))
                            tdSql.query(f'select * from {dbname}.{stbname} where t1="%d" and t2="%d"'%(tagVal,tagVal))
                            tdSql.checkRows(2)
                            tdSql.execute(f'drop table {dbname}.{ctbname}')
                            tdSql.execute(f'drop table {dbname}.{ctbname}t')
                            tdSql.execute(f"create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags('%d','%d')"%(bindStr,tagVal,tagVal))
                            tdSql.execute(f"insert into {dbname}.{ctbname} values(now,1)")
                            tdSql.execute(f"insert into {dbname}.{ctbname}t using {dbname}.{stbname} %s tags('%d','%d') values(now,1)"%(bindStr,tagVal,tagVal))
                            tdSql.query(f"select * from {dbname}.{stbname} where t1='%d' and t2='%d'"%(tagVal,tagVal))
                            tdSql.checkRows(2)
                            tdSql.execute(f'drop table {dbname}.{ctbname}')
                            tdSql.execute(f'drop table {dbname}.{ctbname}t')
                    nTagCtb += 1
                tdSql.execute(f'drop table {dbname}.{stbname}')

        # check now/today for vartype
        nTagCtb = 0
        for tagType in TAG_TYPE:
            for tagLen in TAG_LEN:
                tdSql.execute(f'create stable {dbname}.{stbname}(ts timestamp, f1 int) tags(t1 %s(%d))'%(tagType,tagLen))
                for tagVal in TAG_VAL_STR:
                    for tagBind in TAG_BIND:
                        if tagBind == True:
                            bindStr = "(t1)"
                        else:
                            bindStr = ""
                        if TAG_RESULT_STR[nTagCtb] == False:
                            tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags(%s)'%(bindStr,tagVal))
                            tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} %s tags(%s) values(now,1)'%(bindStr,tagVal))
                        else:
                            tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags(%s)'%(bindStr,tagVal))
                            tdSql.execute(f'insert into {dbname}.{ctbname} values(now,1)')
                            tdSql.execute(f'insert into {dbname}.{ctbname}t using {dbname}.{stbname} %s tags(%s) values(now,1)'%(bindStr,tagVal))
                            if tagVal.startswith("'") or tagVal.startswith("\""):
                                tdSql.query(f'select * from {dbname}.{stbname} where t1=%s'%(tagVal))
                            else:
                                tdSql.query(f'select * from {dbname}.{stbname} where t1=\"%s\"'%(tagVal))
                            tdSql.checkRows(2)
                            tdSql.execute(f'drop table {dbname}.{ctbname}')
                            tdSql.execute(f'drop table {dbname}.{ctbname}t')
                    nTagCtb += 1
                tdSql.execute(f'drop table {dbname}.{stbname}')

        # check int for bool
        nTagCtb = 0
        tdSql.execute(f'create stable {dbname}.{stbname}(ts timestamp, f1 int) tags(t1 bool)')
        for tagVal in TAG_VAL_BOOL_INT:
            for tagBind in TAG_BIND:
                if tagBind == True:
                    bindStr = "(t1)"
                else:
                    bindStr = ""
                tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags(%d)'%(bindStr,tagVal))
                tdSql.execute(f'insert into {dbname}.{ctbname} values(now,1)')
                tdSql.execute(f'insert into {dbname}.{ctbname}t using {dbname}.{stbname} %s tags(%d) values(now,1)'%(bindStr,tagVal))
                tdSql.query(f'select * from {dbname}.{stbname} where t1=%s'%(TAG_RESULT_BOOL[nTagCtb]))
                tdSql.checkRows(2)
                tdSql.execute(f'drop table {dbname}.{ctbname}')
                tdSql.execute(f'drop table {dbname}.{ctbname}t')
            nTagCtb += 1
        tdSql.execute(f'drop table {dbname}.{stbname}')

        # check str for bool
        nTagCtb = 0
        tdSql.execute(f'create stable {dbname}.{stbname}(ts timestamp, f1 int) tags(t1 bool)')
        for tagVal in TAG_VAL_BOOL_STR:
            for tagBind in TAG_BIND:
                if tagBind == True:
                    bindStr = "(t1)"
                else:
                    bindStr = ""
                tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags(%s)'%(bindStr,tagVal))
                tdSql.execute(f'insert into {dbname}.{ctbname} values(now,1)')
                tdSql.execute(f'insert into {dbname}.{ctbname}t using {dbname}.{stbname} %s tags(%s) values(now,1)'%(bindStr,tagVal))
                tdSql.query(f'select * from {dbname}.{stbname} where t1=%s'%(TAG_RESULT_BOOL[nTagCtb]))
                tdSql.checkRows(2)
                tdSql.execute(f'drop table {dbname}.{ctbname}')
                tdSql.execute(f'drop table {dbname}.{ctbname}t')
            nTagCtb += 1
        tdSql.execute(f'drop table {dbname}.{stbname}')

        # check misc for timestamp
        nTagCtb = 0
        tdSql.execute(f'create stable {dbname}.{stbname}(ts timestamp, f1 int) tags(t1 timestamp)')
        checkTS = datetime.datetime.today() - datetime.timedelta(days=1)
        for tagVal in TAG_VAL_TIMESTAMP:
            for tagBind in TAG_BIND:
                if tagBind == True:
                    bindStr = "(t1)"
                else:
                    bindStr = ""
                if TAG_RESULT_TIMESTAMP[nTagCtb] == False:
                    tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags(%s)'%(bindStr,tagVal))
                    tdSql.error(f'insert into {dbname}.{ctbname}t using {dbname}.{stbname} %s tags(%s) values(now,1)'%(bindStr,tagVal))    
                else:
                    tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} %s tags(%s)'%(bindStr,tagVal))
                    tdSql.execute(f'insert into {dbname}.{ctbname} values(now,1)')
                    tdSql.execute(f'insert into {dbname}.{ctbname}t using {dbname}.{stbname} %s tags(%s) values(now,1)'%(bindStr,tagVal))
                    tdSql.query(f'select * from {dbname}.{stbname} where t1>"{checkTS}"')
                    tdSql.checkRows(2)
                    tdSql.execute(f'drop table {dbname}.{ctbname}')
                    tdSql.execute(f'drop table {dbname}.{ctbname}t')
            nTagCtb += 1
        tdSql.execute(f'drop table {dbname}.{stbname}')

    def test_tagFilter(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb()

        tdLog.printNoPrefix("==========step2:query data")
        self.__query_data(10)

        tdLog.printNoPrefix("==========step3:check ts4421")
        self.__ts4421()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
