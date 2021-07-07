#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>

#include "tscSQLParser.h"

//#include "sql.h"
#include "tstoken.h"
#include "testCommon.h"

static TAOS* conn = NULL;

#define ERROR_SQL_L(s)    doParse(s, false);
#define SUCCESS_SQL_L(s)  doParse(s, true);

void failedTableOperation();

int32_t main(int32_t argc, char** argv) {

    //success
    testShowCmd();
    testCreateDestoryDB();
    testCreateDropCmd();
    testSelect();
    testAlterCmd();
    testSelectSQLParseMetric();
    testInsert();

    //failed test
    failedByParser();
    doParseNConvert();
}

void doParse(char *s, bool val) {
    SSqlInfo pSQLInfo = {0};

    tSQLParse(&pSQLInfo, s);

    assert(pSQLInfo.valid == val);
    if (val == false) {
        printf("error msg: %s\n", pSQLInfo.pzErrMsg);
    }

    SqlInfoDestroy(&pSQLInfo);
}

void failedByParser() {
    //1. show operatiSQLParse
    ERROR_SQL_L("show database;");

    ERROR_SQL_L("show CSQLParseNECTISQLParseS");

    ERROR_SQL_L("show db;");

    ERROR_SQL_L("show databases 12345;");

    ERROR_SQL_L("show db;");

    ERROR_SQL_L("show table;");

    ERROR_SQL_L("show db.tables.abc;");

    ERROR_SQL_L("show db.tables abc like 'a_';");

    ERROR_SQL_L("show db.tables like");

    ERROR_SQL_L("use db bac");

    ERROR_SQL_L("showk mnode");

    ERROR_SQL_L("show dnode");

    ERROR_SQL_L("show dnodes abc");

    ERROR_SQL_L("SHOW ACCOUNTS.123;");

    ERROR_SQL_L("show db.VGROUP;");

    ERROR_SQL_L("show DB.V;");

    ERROR_SQL_L("show ABC.MODULES");

    ERROR_SQL_L("show db.queries");

    ERROR_SQL_L("show db.CSQLParseNECTISQLParseS");

    ERROR_SQL_L("show CSQLParseNECTISQLParse");

    ERROR_SQL_L("show SCORE");

    ERROR_SQL_L("SHOW DB.SCORE");

    //2. use operatiSQLParse
    ERROR_SQL_L("use db01234567890.abc;");

    //3. drop operatiSQLParse
    ERROR_SQL_L("drop databases dbtest1.abc");

    ERROR_SQL_L("drop database 123abc");

    ERROR_SQL_L("drop table db1.tabln1.123");

    ERROR_SQL_L("drop dnode 2.2.2");

    ERROR_SQL_L("drop users tttt(*&.k");

    ERROR_SQL_L("drop accounts accx_$#@");

    ERROR_SQL_L("create dnodes 255.255.255");

    ERROR_SQL_L("create account abc_a.1 pass '123'");

    ERROR_SQL_L("create dnode 255.255.255");

    ERROR_SQL_L("create user 123auser pass 'abc'");

    ERROR_SQL_L("create DATAbase db1.abc");

    ERROR_SQL_L("create dnode");
}

void testShowCmd() {
    SUCCESS_SQL_L("show databases");
    SUCCESS_SQL_L("SHOW databases;");
    SUCCESS_SQL_L("show DATABASES;");
    SUCCESS_SQL_L("SHOW DATABASES;");

    SUCCESS_SQL_L("show tables");
    SUCCESS_SQL_L("show tables like 'tm_'");
    SUCCESS_SQL_L("show tables;");
    SUCCESS_SQL_L("show db.tables;");
    SUCCESS_SQL_L("show db.tables like 'a_';");

    SUCCESS_SQL_L("show METRICS");
    SUCCESS_SQL_L("show vgroups");

    SUCCESS_SQL_L("show test.metrics");
    SUCCESS_SQL_L("show test.vgroups");

    SUCCESS_SQL_L("use test");

    SUCCESS_SQL_L("describe m1");
    SUCCESS_SQL_L("describe test.m1");

    SUCCESS_SQL_L("show mnodes");
    SUCCESS_SQL_L("show dnodes");

    SUCCESS_SQL_L("show ACCOUNTS;");
    SUCCESS_SQL_L("show db.VGROUPS;");

    SUCCESS_SQL_L("show MODULES");
    SUCCESS_SQL_L("show users;");
    SUCCESS_SQL_L("show queries");

    SUCCESS_SQL_L("show SCORES");
}

void testCreateDestoryDB() {
    SUCCESS_SQL_L("create database ttkf");
    SUCCESS_SQL_L("drop database ttkf");
    SUCCESS_SQL_L("create database ttkf replica 1 days 20 keep 3 rows 4096 clog 1 comp 1");
    SUCCESS_SQL_L("show databases");
    SUCCESS_SQL_L("drop database ttkf");
}

void testCreateDropCmd() {
    SUCCESS_SQL_L("use t1");
    SUCCESS_SQL_L("create table tk1(ts timestamp, k int)");

    SUCCESS_SQL_L("drop table tk1");
    SUCCESS_SQL_L("describe tk1");

    SUCCESS_SQL_L("create table tk1(ts timestamp, k tinyint) tags(a float)");
    SUCCESS_SQL_L("describe tk1");

    SUCCESS_SQL_L("drop table tk1");

    SUCCESS_SQL_L("create table tf1 using tk1 tags(12)");
    SUCCESS_SQL_L("describe tf1");

    SUCCESS_SQL_L("create table a(ts timestamp, i int) tags(tgcol int)");

    SUCCESS_SQL_L("create table strx as select count(*) as a, count(i) as b from tm0 interval(20s)");
    SUCCESS_SQL_L("create table strx1 as select count(*) as v1, avg(k) as v2 from tm0 interval(5s)");

    //4. create operatiSQLParse
    SUCCESS_SQL_L("create dnode 255.255.255.255");
    SUCCESS_SQL_L("create account abc pass '123'");

    SUCCESS_SQL_L("create user auser pass 'abc'");

    SUCCESS_SQL_L("create DATAbase db1");
    SUCCESS_SQL_L("create DATAbase db1 replica 3 days 20 keep 34");

    //7. create table
    SUCCESS_SQL_L("create table t.k (ts timestamp, f int);");
    SUCCESS_SQL_L("create table t.k (ts timestamp, f int) tags(a int, b binary(12));");
    SUCCESS_SQL_L("create table t using m1 tags (1, 'abc', 3.3);");
    SUCCESS_SQL_L("create table t.ff as select count(*) as f from t1 where n=12 interval(100m) sliding(3s);");

    //3. drop operatiSQLParse
    SUCCESS_SQL_L("drop database dbtest1");
    SUCCESS_SQL_L("drop table db1.tabln1");
    SUCCESS_SQL_L("drop dnode 2.2.2.2");
    SUCCESS_SQL_L("drop user tttt");
    SUCCESS_SQL_L("drop account accx");
}

void testSelect() {
    SUCCESS_SQL_L("use test");
    SUCCESS_SQL_L("select count(*) from t1.tm0 where ts<now+1d and ts>now interval(1h) fill(1)");
    SUCCESS_SQL_L("select * from tm0 where ts<now-1d");
    SUCCESS_SQL_L("select * from tm0 where ts<=1534068495248");
    SUCCESS_SQL_L("select * from tm0 where ts<=1534068495248 and ts>=1534068493839");
    SUCCESS_SQL_L("select * from tm0 where ts<=1534068497823 and k < 3");
    SUCCESS_SQL_L("select count(*) from tm0 ");

    SUCCESS_SQL_L("select k+1 from t1.tm0");
    SUCCESS_SQL_L("select count(*) from t1.tm0 interval(10a) fill(nSQLParsee)");

    //select count(*) from m_co_mt0 where tgcol < 5 and ts < now + 4m;
    //todo alias name too long
    SUCCESS_SQL_L("select k as kk123456789012345678901234567890 from t1.tm0");
    SUCCESS_SQL_L("select k as kk123456789012345678901234567890123 from t1.tm0");

    SUCCESS_SQL_L("select k+12*10/9.9+t from t1.tk");
    SUCCESS_SQL_L("select k+(9/2)+t from t1.tk");
    SUCCESS_SQL_L("select k+9%2 from t1.tk");
    SUCCESS_SQL_L("select count(*) from t1.m1");
    SUCCESS_SQL_L("select sum(speed) from db.t where ts >= 1500000000000 and"
                     " ts <= 1500000070000 interval(500a) fill(null);");
    //9. select
    SUCCESS_SQL_L("select * from t1;");
    SUCCESS_SQL_L("select t1.* from t1;");
    SUCCESS_SQL_L("select a from db.t1;");
    SUCCESS_SQL_L("select a,b from t1 interval(10a);");
    SUCCESS_SQL_L("select t1.* from t1 interval(10a);");
    SUCCESS_SQL_L("select a as tk from t1;");
    SUCCESS_SQL_L("select count(*) from t1;");
    SUCCESS_SQL_L("select count(*), sum(k) as ss from t2;");
    SUCCESS_SQL_L("select count(sum(k)) from t1;");
    SUCCESS_SQL_L("select count(*) from t2 where a=12 and b=4 and c=7 limit 20 offset 0");
    SUCCESS_SQL_L("select count(*) from t1 where a=2 and b=2 interval(10a) group by a,b,c order by ts asc");
    SUCCESS_SQL_L("select count(*) from t1 where a=2 and b=2 interval(10a) fill(nSQLParsee) group by a,b,c order by ts asc");
    SUCCESS_SQL_L("select first(ts) from t1");
    SUCCESS_SQL_L("select count(*) from t1 interval(10a) fill(none)");
    SUCCESS_SQL_L("select count(*) from t1 interval(10a) fill(value, 1)");
    SUCCESS_SQL_L("select count(*) from t1 interval(10a) fill(null)");
    SUCCESS_SQL_L("select count(*) from t1 interval(10a) fill(prev)");
}

void testSelectSQLParseMetric() {
    SUCCESS_SQL_L("use t1");
    SUCCESS_SQL_L("select BOTTOM(k, 20) from t1.tm0");

    SUCCESS_SQL_L("select * from m1");
    SUCCESS_SQL_L("select count(*) from m1");
    SUCCESS_SQL_L("select min(k) from m1");
    SUCCESS_SQL_L("select max(k) from m1");
    SUCCESS_SQL_L("select avg(k) from m1");
    SUCCESS_SQL_L("select first(k) from m1");
    SUCCESS_SQL_L("select last(k) from m1");
    SUCCESS_SQL_L("select sum(k) from m1");
    SUCCESS_SQL_L("select sum(k) from m1 where ktag=99");
}

void testInsert() {
//    SUCCESS_SQL_L("insert into t1 values(now, 1)");
//    SUCCESS_SQL_L("insert into t1 values(now, 1.1)");
//    SUCCESS_SQL_L("insert into t1 values(now, 1.1)(now+1a, 2.9)");
//    SUCCESS_SQL_L("insert into t1 values(now, 1.1)(now+1a, 2.9) t2 values(now, 9.1)(now+1a, 10.1)");
}

void testAlterCmd() {
    SUCCESS_SQL_L("alter table m1 add tag f binary(12)");
//
    SUCCESS_SQL_L("alter table m1 drop tag f");
    SUCCESS_SQL_L("alter table m1 change tag a ktag");
//
    SUCCESS_SQL_L("alter table tm0 set ktag=99");

    //11. alter table
    SUCCESS_SQL_L("alter table m1 add tag a int");
    SUCCESS_SQL_L("alter table m1 drop tag b");
    SUCCESS_SQL_L("alter table m1 change tag c tt");
    SUCCESS_SQL_L("alter table m1 set c=12");

    //6. ALTER
    SUCCESS_SQL_L("alter user abc pass '123'");
    SUCCESS_SQL_L("alter dnode 1.1.1.1 resetlog");
    SUCCESS_SQL_L("alter local log");

    SUCCESS_SQL_L("alter user abc pass bca");
    SUCCESS_SQL_L("alter user abc privilege read");
}

void doParseNConvert() {
    strcpy(configDir, "/etc/taos");
    taos_init();

    conn = taos_connect("192.168.0.1", "root", "taosdata", 0, 0);
    if (conn == NULL) {
        printf("Failed to connect to DB, reason:%s\n", taos_errstr(conn));
        exit(-1);
    }

    failedDBOperation();
    failedTableOperation();
    failedUserOperation();
    failedDnodeOperation();
    failedQueryOperation();
}

void failedDBOperation() {
    //1. create database

    //database name use keywords
    SQL_PARSE_CMD_FAILED("create database if not exists vgroups");
    SQL_PARSE_CMD_FAILED("create database if not exists databases");
    SQL_PARSE_CMD_FAILED("create database if not exists int");
    SQL_PARSE_CMD_FAILED("create database if not exists nchar");
    SQL_PARSE_CMD_FAILED("create database if not exists binary");
    SQL_PARSE_CMD_FAILED("create database if not exists top");
    SQL_PARSE_CMD_FAILED("create database if not exists percentile");
    SQL_PARSE_CMD_FAILED("create database if not exists ");

    //database name use widechar
    SQL_PARSE_CMD_FAILED("create database if not exists 摄影");

    //database name use number
    SQL_PARSE_CMD_FAILED("create database if not exists 110");
    SQL_PARSE_CMD_FAILED("create database if not exists 110abc");

    //database name use ip
    SQL_PARSE_CMD_FAILED("create database if not exists 192.168.0.1");

    //database name use string
//    SQL_PARSE_CMD_FAILED("create database 'kk' replica 2");

    //database name too long
    SQL_PARSE_CMD_FAILED("create database db12345678901234567890123456789012345678901234567890");

    //invalid options name
    //replica(A) day(B) keep(C) rows(D) cache(E) ablocks(F) tblocks(K) tables(G) ctime(H) clog(I) comp(J)
    SQL_PARSE_CMD_FAILED("create database kt cc 128 rows 9999");

    SQL_PARSE_CMD_FAILED("create database kt replica '摄影'");
    SQL_PARSE_CMD_FAILED("create database kt replica 摄影");

    //invalid options value
    SQL_PARSE_CMD_FAILED("create database kt replica 1 days 10 keep 1 rows 20");

    SQL_PARSE_CMD_FAILED("create database kt replica abc day def keep g rows k");

    //2. drop databases
    //invalid name
    SQL_PARSE_CMD_FAILED("drop database 摄影");
    SQL_PARSE_CMD_FAILED("drop database 123");

    SQL_PARSE_CMD_FAILED("drop database '123'");
    SQL_PARSE_CMD_FAILED("drop database '192.168.0.1'");
    SQL_PARSE_CMD_FAILED("drop database '_*&^%'");
    SQL_PARSE_CMD_FAILED("drop database '123'");

    //database name is keyword
    SQL_PARSE_CMD_FAILED("drop database table");
    SQL_PARSE_CMD_FAILED("drop database int");
    SQL_PARSE_CMD_FAILED("drop database tag");

    //database name too long
    SQL_PARSE_CMD_FAILED("drop database if exists a01234567890123456789012345678901234567890123456789");
}

void failedTableOperation() {
    //1. create table
    taos_query(conn, "create database if not exists parsetest");
    taos_query(conn, "use parsetest");
    SQL_PARSE_CMD_FAILED("create table if not exists 'dddt'.test(a timestamp, k int)");

    //table name is keyword
    SQL_PARSE_CMD_FAILED("create table if not exists int(a timestamp, k int)");
    SQL_PARSE_CMD_FAILED("create table if not exists tag(ts timestamp, k int)");
    SQL_PARSE_CMD_FAILED("create table if not exists metrics(ts timestamp, k int)");
    SQL_PARSE_CMD_FAILED("create table if not exists table(ts timestamp, k int)");
    SQL_PARSE_CMD_FAILED("create table if not exists exists(ts timestamp, k int)");
    SQL_PARSE_CMD_FAILED("create table if not exists integer(ts timestamp, k int)");
    SQL_PARSE_CMD_FAILED("create table if not exists top(ts timestamp, k int)");
    SQL_PARSE_CMD_FAILED("create table if not exists bottom(ts timestamp, k int)");

    //table name too long
    SQL_PARSE_CMD_FAILED("create table if not exists a0123456789012345678901234567890123456789(ts timestamp, k int)");

    //illegal table name
    SQL_PARSE_CMD_FAILED("create table if not exists 123(ts timestamp, k int)");
    SQL_PARSE_CMD_FAILED("create table if not exists 123abc(ts timestamp, k int)");
    SQL_PARSE_CMD_FAILED("create table if not exists 摄影(ts timestamp, k int)");
    SQL_PARSE_CMD_FAILED("create table if not exists 192.168.0.1(ts timestamp, k int)");
    SQL_PARSE_CMD_FAILED("create table if not exists '192.168.0.1'(ts timestamp, k int)");

    //no columns
    SQL_PARSE_CMD_FAILED("create table if not exists abc()");

    //first column not timestamp
    SQL_PARSE_CMD_FAILED("create table if not exists abc(a int, b int)");

    //column name too long
    //column name will be truncated in parse stage
//    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, b01234567890123456789012345678901234 int)");

    //column name is keyword
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, table int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, database int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, smallint int)");

    SQL_PARSE_CMD_FAILED("create table if not exists abc('摄影' timestamp, a int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(摄影 timestamp, a int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(192.168.0.1 timestamp, a int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(. timestamp, a int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(true timestamp, a int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(1e1 timestamp, a int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(\"1e1\" timestamp, a int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(4.123 timestamp, a int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(4ab timestamp, a int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc('table' timestamp, a int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(\"table\" timestamp, a int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(\"table \" timestamp, a int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc('4ab' timestamp, a int)");

    //row length too long
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, b binary(9000))");

    //too many columns

    //only one column
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp)");

    //illegal type
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a short)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a long)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a binary)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a binary())");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a binary(0))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a binary(abc))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a binary(-20))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a binary(1.9))");

    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(1.9))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(99999))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(-298))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(abc))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar())");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(0))");

    //duplicated column name
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(1), a timestamp, c timestamp, k int, c int, a int)");

    //tag null
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS()");

    //tags name keyword
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(int int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(smallint int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(table int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(float int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(double int)");

    //tag name number
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(1.23 int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(99 int)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(99abc int)");

    //tag illegal data type
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b short)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b binary)");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b binary())");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b binary(0))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b binary(-20))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b binary(abc))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b binary(abc))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b nchar(abc))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b nchar())");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b nchar(0))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b nchar(-20))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b nchar(abc))");
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b nchar(f))");

    //too many tags
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b nchar(1), c int, d int,e int, f int, g int, k int)");

    //too long tags
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b nchar(1), c binary(512))");

    //duplicated tag name
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b nchar(1), a int)");

    //timestamp in tags
    SQL_PARSE_CMD_FAILED("create table if not exists abc(ts timestamp, a nchar(12)) TAGS(b nchar(1), c timestamp)");

    //3. create table using metric
    executeSQL(conn, "drop table if exists m1", NULL);
    executeSQL(conn, "create table if not exists m1(ts timestamp, k int) tags(a int, b tinyint, c binary(12), d nchar(1))", NULL);

    //invalid name
    SQL_PARSE_CMD_FAILED("create table if not exists 192.168.0.1 using m1 tags(1, 1, '123', '1')");
    SQL_PARSE_CMD_FAILED("create table if not exists 摄影 using m1 tags(1, 1, '123', '1')");
    SQL_PARSE_CMD_FAILED("create table if not exists '摄影' using m1 tags(1, 1, '123', '1')");
    SQL_PARSE_CMD_FAILED("create table if not exists ' ^&*()1 ' using m1 tags(1, 1, '123', '1')");

    //duplicate name to metric
    SQL_PARSE_CMD_FAILED("create table m1 using m1 tags(1, 1, '123', '1')");

    //less tags
    SQL_PARSE_CMD_FAILED("create table aaa using m1 tags('123', '1')");

    //more tags
    SQL_PARSE_CMD_FAILED("create table aaa using m1 tags('123', '1', 987, 1231, 1, 1, 1, 1)");

    //table name too long
    SQL_PARSE_CMD_FAILED("create table a01234567890123456789012345678901234567890 using m1 tags('123', '1', 1, 1)");

    //int value overflow
    SQL_PARSE_CMD_FAILED("create table a001 using m1 tags('999999999999999999999', 12, 1, 1)");
    SQL_PARSE_CMD_FAILED("create table a001 using m1 tags('9', 999999999999999, 1, 1)");

    //binary overflow tag value, will be truncated automatically
//    SQL_PARSE_CMD_FAILED("create table a001 using m1 tags('9', 9, '012345678901234567890', 1)");

    SQL_PARSE_CMD_FAILED("create table a001 using m1 tags('9', 9, '012345678901234567890', 测试字符串)");

//    SQL_PARSE_CMD_FAILED("create table a001 using m1 tags('9', 9, '012345678901234567890', '测试字符串')");

    //todo null process
//    SQL_PARSE_CMD_FAILED("create table a001 using m1 tags('9', 9, '012345678901234567890', '1')");

    executeSQL(conn, "drop table m1", NULL);
    executeSQL(conn, "create table m2 (ts timestamp, k int) tags(a int, b tinyint, c smallint, d float, e double, f bigint)", NULL);

    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(9999999999999, 1, 1, 1, 1, 1)");
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, 99999999999999, 1, 1, 1, 1)");
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, 1, 9999999999, 1, 1, 1)");
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, 1, 1, 1e90, 1, 1)");
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, 1, 1, 1, 1e9000000, 9)");

//    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, 1, 1, 1, 1e1, 99999999999999999999999999999999999999)");

    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags('999999999999999999999999999999999', 1, 1, 1, 1, 1)");
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, '99999999999999999999999999', 1, 1, 1, 1)");
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, 1, '9999999999', 1, 1, 1)");
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, 1, 1, '1e90', 1, 1)");
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, 1, 1, 1, '1e9000000', 9)");
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, 1, 1, 1, 1e1, '99999999999999999999999999999999999999')");

    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags('-999999999999999999999999999999999', 1, 1, 1, 1, 1)");
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, '-99999999999999999999999999', 1, 1, 1, 1)");
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, 1, '-9999999999', 1, 1, 1)");
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, 1, 1, '-1e90', 1, 1)");
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, 1, 1, 1, '-1e9000000', 9)");
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, 1, 1, 1, 1e1, '-99999999999999999999999999999999999999')");

    //todo null value.................
    SQL_PARSE_CMD_FAILED("create table a010 using m2 tags(1, NULL, NULL, NULL, NULL, NULL)");

    //4. show tables

    //invalid database name
    SQL_PARSE_CMD_FAILED("show 摄影.tables");
    SQL_PARSE_CMD_FAILED("show 192.168.0.1.tables");
    SQL_PARSE_CMD_FAILED("show 192.tables");
    SQL_PARSE_CMD_FAILED("show 192abc.tables");
    SQL_PARSE_CMD_FAILED("show 192abc.tables");

    SQL_PARSE_CMD_FAILED("show '192abc'.tables");
    SQL_PARSE_CMD_FAILED("show \"192abc\".tables");
    SQL_PARSE_CMD_FAILED("show ' 192abc '.tables");
    SQL_PARSE_CMD_FAILED("show ' 摄影 '.tables");
    SQL_PARSE_CMD_FAILED("show ' '.tables");
    SQL_PARSE_CMD_FAILED("show ' table'.tables");
    SQL_PARSE_CMD_FAILED("show ' dnodes'.tables");
    SQL_PARSE_CMD_FAILED("show ' _123&^% '.tables");
    SQL_PARSE_CMD_FAILED("show .tables");

    //database name too long
    SQL_PARSE_CMD_FAILED("show a0123456789012345678901234567890123456789.tables");

    //5.drop tables
    //invalid name
    SQL_PARSE_CMD_FAILED("drop table if exists '摄影 '");
    SQL_PARSE_CMD_FAILED("drop table if exists 'table '");
    SQL_PARSE_CMD_FAILED("drop table if exists '110 '");

    //name too long
    SQL_PARSE_CMD_FAILED("drop table if exists a01234567890123456789012345678901234567890");
//    SQL_PARSE_CMD_FAILED("drop table if exists 'a01234567890123456789012345678901234567890'.a012345678901234567890");

    //invalid db name
    SQL_PARSE_CMD_FAILED("drop table if exists ' _123&^% '.xxx1");
    SQL_PARSE_CMD_FAILED("drop table if exists ' _123&^% '.110");
    SQL_PARSE_CMD_FAILED("drop table if exists _123&.tabx");
    SQL_PARSE_CMD_FAILED("drop table if exists _123&.dnodes");
    SQL_PARSE_CMD_FAILED("drop table if exists _123.show");

    //6. create stream

    //invalid column alias name
    SQL_PARSE_CMD_FAILED("create table t as select count(*) as 123abc from k interval(99a)");

    //invalid table name
    SQL_PARSE_CMD_FAILED("create table t as select count(*) as a from '0123abc' interval(99a)");
    SQL_PARSE_CMD_FAILED("create table t as select count(*) as a from 0123abc interval(99a)");

    //invalid table name
    SQL_PARSE_CMD_FAILED("create table 0921 as select count(*) as a from 0123abc interval(99a)");
    SQL_PARSE_CMD_FAILED("create table abc(*&^ as select count(*) as a from 0123abc interval(99a)");
    SQL_PARSE_CMD_FAILED("create table '  abc(*&^ '  as select count(*) as a from 0123abc interval(99a)");
    SQL_PARSE_CMD_FAILED("create table int as select count(*) as a from 0123abc interval(99a)");
    SQL_PARSE_CMD_FAILED("create table mnodes as select count(*) as a from 0123abc interval(99a)");

    //invalid interval
    SQL_PARSE_CMD_FAILED("create table tt as select count(*) as a from abc interval(9a)");
    SQL_PARSE_CMD_FAILED("create table tt as select count(*) as a from abc interval(99)");
    SQL_PARSE_CMD_FAILED("create table tt as select count(*) as a from abc interval(-100a)");


    //group by clause
    SQL_PARSE_CMD_FAILED("create table td as select count(*) from abc interval(1s) group by b");

    //7. alter table
    executeSQL(conn, "create table td (ts timestamp, k int)", NULL);
    executeSQL(conn, "create table tm1 using m2 tags(1,1,1,1,1,1)", NULL);

    //invalid column name
    SQL_PARSE_CMD_FAILED("alter table td add column _*&^%$ int");
    SQL_PARSE_CMD_FAILED("alter table td add column 链接 int");
    SQL_PARSE_CMD_FAILED("alter table td add column '链接' int");
    SQL_PARSE_CMD_FAILED("alter table td add column 'database' int");

    //invalid data type
    SQL_PARSE_CMD_FAILED("alter table td add column abc short");
    SQL_PARSE_CMD_FAILED("alter table td add column abc varchar");
    SQL_PARSE_CMD_FAILED("alter table td add column abc binary");
    SQL_PARSE_CMD_FAILED("alter table td add column abc nchar");
    SQL_PARSE_CMD_FAILED("alter table td add column abc nchar(0)");
    SQL_PARSE_CMD_FAILED("alter table td add column abc binary(0)");
    SQL_PARSE_CMD_FAILED("alter table td add column abc binary(-20)");
    SQL_PARSE_CMD_FAILED("alter table td add column abc binary(9999)");

    SQL_PARSE_CMD_FAILED("alter table td drop column 123");
    SQL_PARSE_CMD_FAILED("alter table td drop column abc");
    SQL_PARSE_CMD_FAILED("alter table td drop column _123&^%");

    //duplicated name
    SQL_PARSE_CMD_FAILED("alter table td add column k binary(1)");

    //too many columns
//    SQL_PARSE_CMD_FAILED("alter table ");

    //no columns
    SQL_PARSE_CMD_FAILED("alter table td drop column k");
    SQL_PARSE_CMD_FAILED("alter table tm1 drop column k");
    SQL_PARSE_CMD_FAILED("alter table tm1 add column k int");

    //8. alter tags

    //meter add tag
    SQL_PARSE_CMD_FAILED("alter table tm1 add tag f1 int");
    SQL_PARSE_CMD_FAILED("alter table tm1 add tag f int");

    SQL_PARSE_CMD_FAILED("alter table m2 drop tag tt");

    executeSQL(conn, "create table if not exists m3 (ts timestamp, k int) tags(a int)", NULL);
    SQL_PARSE_CMD_FAILED("alter table m3 drop tag a");

    //invalid tag name
    SQL_PARSE_CMD_FAILED("alter table m3 add tag 链接 int");
    SQL_PARSE_CMD_FAILED("alter table m3 add tag '链接' int");
    SQL_PARSE_CMD_FAILED("alter table m3 add tag '_&^%' int");
    SQL_PARSE_CMD_FAILED("alter table m3 add tag 192.168.0.1 int");

    //tags name too long will be truncated
}

void failedUserOperation() {
    SQL_PARSE_CMD_FAILED("show user;");
    SQL_PARSE_CMD_FAILED("create account abc pass");
    SQL_PARSE_CMD_FAILED("create account abc");
    SQL_PARSE_CMD_FAILED("create account pass pass k");
    SQL_PARSE_CMD_FAILED("create account pass pass k");

    //pass not a string
    SQL_PARSE_CMD_FAILED("create account abc pass k");

    //empty string
    SQL_PARSE_CMD_FAILED("create account abc pass ''");

    //user name too long
    SQL_PARSE_CMD_FAILED("create account a012345678901234567890123456789012345678901234567890123456789 pass '123'");

    //password too long
    SQL_PARSE_CMD_FAILED("create account abc pass '012345678901234567890123456789012345678901234567890123456789'");

    //alter user pass too long
    SQL_PARSE_CMD_FAILED("alter user abc pass '012345678901234567890123456789012345678901234567890123456789'");

    //password empty
    SQL_PARSE_CMD_FAILED("alter user abc pass ''");
    SQL_PARSE_CMD_FAILED("alter user abc pass ");

}

void failedDnodeOperation() {
    //invalid ip
    SQL_PARSE_CMD_FAILED("alter dnode flag");
    SQL_PARSE_CMD_FAILED("alter dnode 192.168.0.1 ");
    SQL_PARSE_CMD_FAILED("alter dnode 192.168.0.1 flag ");
    SQL_PARSE_CMD_FAILED("alter dnode 192.168.0.1 debug 135 ");
}

void failedQueryOperation() {
    //should failed sql parse
    /*
     * create table m1(
     * ts timestamp, k int, h binary(20), t bigint,
     * s float, f double, x smallint, y tinyint, z bool
     * )
     * tags(a int, b binary(20), c bigint)
     *
     */
    executeSQL(conn, "drop database parsetest1", NULL);
    executeSQL(conn, "create database parsetest1", NULL);
    executeSQL(conn, "use parsetest1", NULL);
    createEnvironment(conn, 1, 1, 40, 1);

    //1. illegal field name
    SQL_PARSE_CMD_FAILED("select count(ff), count( z), count(x), count(), count(f,k) from tm0 ");
    SQL_PARSE_CMD_FAILED("select ff from tm0 ");
    SQL_PARSE_CMD_FAILED("select z+12 from tm0 ");

    //2. illegal field type
    SQL_PARSE_CMD_FAILED("select first(a) from tm0 ");

    SQL_PARSE_CMD_FAILED("select min(h) from tm0 ");
    SQL_PARSE_CMD_FAILED("select min() from tm0 ");
    SQL_PARSE_CMD_FAILED("select max(z) from tm0 ");
    SQL_PARSE_CMD_FAILED("select max() from tm0 ");

    SQL_PARSE_CMD_FAILED("select sum(z) from tm0 ");
    SQL_PARSE_CMD_FAILED("select sum() from tm0 ");
    SQL_PARSE_CMD_FAILED("select sum(h) from tm0 ");

    SQL_PARSE_CMD_FAILED("select avg(z) from tm0 ");
    SQL_PARSE_CMD_FAILED("select avg() from tm0 ");
    SQL_PARSE_CMD_FAILED("select avg(h) from tm0 ");

    SQL_PARSE_CMD_FAILED("select top(h,12) from tm0 ");
    SQL_PARSE_CMD_FAILED("select top(,12) from tm0 ");
    SQL_PARSE_CMD_FAILED("select top(z,11) from tm0 ");

    SQL_PARSE_CMD_FAILED("select bottom(h,11) from tm0 ");
    SQL_PARSE_CMD_FAILED("select bottom(,11) from tm0 ");
    SQL_PARSE_CMD_FAILED("select bottom(z,11) from tm0 ");

    SQL_PARSE_CMD_FAILED("select leastsquares(z) from tm0 ");
    SQL_PARSE_CMD_FAILED("select leastsquares() from tm0 ");
    SQL_PARSE_CMD_FAILED("select percentile(z,11) from tm0 ");
    SQL_PARSE_CMD_FAILED("select percentile(z,9887655) from tm0 ");
    SQL_PARSE_CMD_FAILED("select percentile(,9887655) from tm0 ");

    SQL_PARSE_CMD_FAILED("select stddev(z) from tm0 ");
    SQL_PARSE_CMD_FAILED("select stddev() from tm0 ");
    SQL_PARSE_CMD_FAILED("select stddev(h) from tm0 ");

    SQL_PARSE_CMD_FAILED("select first(a) from tm0 ");
    SQL_PARSE_CMD_FAILED("select last(b) from tm0 ");

    SQL_PARSE_CMD_FAILED("select diff(h) from tm0 ");
    SQL_PARSE_CMD_FAILED("select diff(z) from tm0 ");

    SQL_PARSE_CMD_FAILED("select z*12 from tm0 ");
    SQL_PARSE_CMD_FAILED("select h+123 from tm0 ");

    SQL_PARSE_CMD_FAILED("select h/123 from tm0 ");
    SQL_PARSE_CMD_FAILED("select h-123 from tm0 ");
    SQL_PARSE_CMD_FAILED("select spread(h) from tm0 ");
    SQL_PARSE_CMD_FAILED("select spread(*) from tm0 ");
    SQL_PARSE_CMD_FAILED("select spread(a) from tm0 ");

    //3. illegal parameter value
    SQL_PARSE_CMD_FAILED("select leastsquares(k,12) from tm0 ");
    SQL_PARSE_CMD_FAILED("select min(k,12) from tm0 ");
    SQL_PARSE_CMD_FAILED("select first(k,12) from tm0 ");

    //3.1 percentile
    SQL_PARSE_CMD_FAILED("select percentile(f, 999) from tm0 ");
    SQL_PARSE_CMD_FAILED("select percentile(f, -20) from tm0 ");

    SQL_PARSE_CMD_FAILED("select percentile(f, k) from tm0 ");  // failed!..
    SQL_PARSE_CMD_FAILED("select percentile(f, ^%$) from tm0 ");

    SQL_PARSE_CMD_FAILED("select percentile(f, ^%$) from tm0 ");
    SQL_PARSE_CMD_FAILED("select percentile(f, ^%$) from tm0 ");

    //3.2 bottom
    SQL_PARSE_CMD_FAILED("select bottom(f, -20) from tm0 ");
    SQL_PARSE_CMD_FAILED("select bottom(f, 1998765) from tm0 ");
//    SQL_PARSE_CMD_FAILED("select bottom(f, 1.732) from tm0 ");  // failed!..
    SQL_PARSE_CMD_FAILED("select bottom(f, 99) from tm0 ");
    SQL_PARSE_CMD_FAILED("select bottom(f, ttt) from tm0 ");

    //3.3 top
    SQL_PARSE_CMD_FAILED("select top(f, -20) from tm0 ");
    SQL_PARSE_CMD_FAILED("select top(f, 99) from tm0 ");
//    SQL_PARSE_CMD_FAILED("select top(f, 1.732) from tm0 ");  //failed!..
    SQL_PARSE_CMD_FAILED("select top(f, 99) from tm0 ");
    SQL_PARSE_CMD_FAILED("select top(f, ttt) from tm0 ");

    SQL_PARSE_CMD_FAILED("select f-a from tm0 ");   //failed!..
    SQL_PARSE_CMD_FAILED("select f*123&^% from tm0 ");

    //4. function compatiable failure
    SQL_PARSE_CMD_FAILED("select top(k, 20),min(k),max(f) from tm0 ");
    SQL_PARSE_CMD_FAILED("select diff(k), min(k) from tm0 ");

    SQL_PARSE_CMD_FAILED("select *, min(k) from tm0 ");
    SQL_PARSE_CMD_FAILED("select z, min(k) from tm0 ");

    SQL_PARSE_CMD_FAILED("select z, diff(s) from tm0 ");
    SQL_PARSE_CMD_FAILED("select diff(s), k+1 from tm0 ");

    SQL_PARSE_CMD_FAILED("select min(f),top(k,12) from tm0 ");

    //5 interval function compatiable check
    SQL_PARSE_CMD_FAILED("select z from tm0 interval(12m)");
    SQL_PARSE_CMD_FAILED("select top(y,11) from tm0 interval(12m)");
    SQL_PARSE_CMD_FAILED("select bottom(s, 3) from tm0 interval(12m)");

    //6. illegal interval range check
    SQL_PARSE_CMD_FAILED("select count(*) from tm0 interval(-m)");
    SQL_PARSE_CMD_FAILED("select count(*) from tm0 interval(0a)");
    SQL_PARSE_CMD_FAILED("select count(*) from tm0 interval(0m)");

    SQL_PARSE_CMD_FAILED("select count(*) from tm0 interval(-763a)"); //failed!..

    //7. illegal query range test
//    SQL_PARSE_CMD_FAILED("select count(*) from tm0 where ts<0");
    SQL_PARSE_CMD_FAILED("select z from tm0 where ts>abc");  //failed!..

    //8. non-exist table
    SQL_PARSE_CMD_FAILED("select count(*) from tm000 where ts<now");

    //9. order by test
    SQL_PARSE_CMD_FAILED("select * from tm000 where ts<now order by ");
    SQL_PARSE_CMD_FAILED("select * from tm000 order by ");

    SQL_PARSE_CMD_FAILED("select * from tm000 where ts<now order by tt desc");
    SQL_PARSE_CMD_FAILED("select * from tm000 where ts<now order by tt asc");
    SQL_PARSE_CMD_FAILED("select * from tm000 where ts<now order by tt aesc");

    //10. supported function test
    SQL_PARSE_CMD_FAILED("select cc(f) from tm0 interval(100a)");
    SQL_PARSE_CMD_FAILED("select cc(f) from tm0 interval(100a) order by k");

    //11. metric query test
    SQL_PARSE_CMD_FAILED("select f+1 from m1 group by b,c");
    SQL_PARSE_CMD_FAILED("select count(*) from m1 group by b,c,d");
    SQL_PARSE_CMD_FAILED("select count(a), avg(a), first(a),b,c from m1 ");
    SQL_PARSE_CMD_FAILED("select b,c from m1 group by k");

    //12.metric supported function test
    SQL_PARSE_CMD_FAILED("select stddev(*) from m1 group by a");
    SQL_PARSE_CMD_FAILED("select percentile(k,1) from m1 group by a");
    SQL_PARSE_CMD_FAILED("select top(k,20) from m1 group by a");
    SQL_PARSE_CMD_FAILED("select bottom(k,20) from m1 group by a");
    SQL_PARSE_CMD_FAILED("select * from m1 group by a");
    SQL_PARSE_CMD_FAILED("select f+12 from m1 group by a");
    SQL_PARSE_CMD_FAILED("select f+12,k/12 from m1 group by a");
    SQL_PARSE_CMD_FAILED("select k/0 from m1 group by a");
    SQL_PARSE_CMD_FAILED("select diff(s) from m1 group by a");

    //12.1 query on non-exists tag field
    SQL_PARSE_CMD_FAILED("select diff(s) from m1 where k=1 group by a");

    //12.2 illegal condition expr & missing where clause
    SQL_PARSE_CMD_FAILED("select count(*) from m1 where  and a><1 group by a");

    //12.2 projection is not compatible with group by/ interval query
    SQL_PARSE_CMD_FAILED("select * from m1 group by k");
    SQL_PARSE_CMD_FAILED("select * from m1 interval(30s)");
    SQL_PARSE_CMD_FAILED("select a from m1 interval(30s)");
    SQL_PARSE_CMD_FAILED("select k,h,f,y,z from m1 group by a");
    SQL_PARSE_CMD_FAILED("select k,h,f,y,z from m1 interval(20s) group by a");

    //13. stream computing support
    //13.1 sub-clause must be aggregation function
    taos_query(conn, "drop table test_st");
    SQL_PARSE_CMD_FAILED("create table test_st as select * from m1 group by k");

    taos_query(conn, "drop table test_st");
    SQL_PARSE_CMD_FAILED("create table test_st as select k,a,b from m1 interval(20s)");

    //13.2 not supported function in stream computing
    taos_query(conn, "drop table test_st");
    SQL_PARSE_CMD_FAILED("create table test_st as select stddev(k),percentile(f,50) from m1 interval(10s)");

    taos_query(conn, "drop table test_st");
    SQL_PARSE_CMD_FAILED("create table test_st as select top(k,20) from m1 interval(30s)");

    taos_query(conn, "drop table test_st");
    SQL_PARSE_CMD_FAILED(
            "create table test_st as select max(k) as cnt, sum(f) as ff123, bottom(k,1) from m1 where a=1 "
            "interval(20s) group by k");

    //13.2 sub-clause with time range condition(error)
    taos_query(conn, "drop table test_st");
    SQL_PARSE_CMD_FAILED(
            "create table test_st as select count(*) from m1 where ts>now and ts<now+1y interval(10s)");

    //13.3 too small time interval & incompatible sliding
    taos_query(conn, "drop table test_st");
    SQL_PARSE_CMD_FAILED("create table test_st as select count(*) from m1 interval(500a)");

    taos_query(conn, "drop table test_st");
    SQL_PARSE_CMD_FAILED("create table test_st as select count(*) from m1 interval(10s) sliding(50s)");

    taos_query(conn, "drop table test_st");
    SQL_PARSE_CMD_FAILED("create table test_st as select count(*) from m1 interval(10s) sliding(1a)");
    taos_query(conn, "drop table test_st");

    //15. arithmetic expression in where clause
    SQL_PARSE_CMD_FAILED("select * from tm0 where count(*)<1");
    SQL_PARSE_CMD_FAILED("select * from tm0 where 12<30");
//    SQL_PARSE_CMD_FAILED("select * from m1 where a+2<3");  //failed! to filter this situation.!!!

//    SQL_PARSE_CMD_FAILED("select * from tm0 where k<12 or k > 7");  //failed! single range for each column to filter

    //17. arithmetic expression in select
    SQL_PARSE_CMD_FAILED("select count(*)*2 from tm0");
    SQL_PARSE_CMD_FAILED("select count(*)+k from tm0");
    SQL_PARSE_CMD_FAILED("select count(*)+count(*) from tm0");
    SQL_PARSE_CMD_FAILED("select top(k, 20)+count(*) from tm0");
    SQL_PARSE_CMD_FAILED("select k^k from tm0");

    //18. pipeline arithmetic expression in where clause
    SQL_PARSE_CMD_FAILED("select * from tm0 where (k+12)<(f+11)");  //failed!!

    //19. binary column in arithmetic expression
    SQL_PARSE_CMD_FAILED("select h+12/99 from tm0");
    SQL_PARSE_CMD_FAILED("select h+'12' from tm0");
    SQL_PARSE_CMD_FAILED("select h from tm0 where h<'123'");

//    20. illegal order by
    SQL_PARSE_CMD_FAILED("select count(*) from m1 interval(1s) group by a,b,c order by b asc");

//    21. zero in division
    SQL_PARSE_CMD_FAILED("select f+12/0 from tm0");
    //SQL_PARSE_CMD_FAILED("select f/0 from tm0");

    //22. fill

    //illegal fill type
    SQL_PARSE_CMD_FAILED("select count(*) from m1 interval(1s) fill()");
    SQL_PARSE_CMD_FAILED("select count(*) from m1 where ts>now and ts<now+1m interval(1s) fill(null1)");
    SQL_PARSE_CMD_FAILED("select count(*) from m1 where ts>now and ts<now+1m interval(1s) fill(1,2,3)");
    SQL_PARSE_CMD_FAILED("select count(*) from m1 where ts>now and ts<now+1m interval(1s) fill(1)");
    SQL_PARSE_CMD_FAILED("select count(*) from m1 where ts>now and ts<now+1m interval(1s) fill(values, 123)");
    SQL_PARSE_CMD_FAILED("select count(*) from m1 where ts>now and ts<now+1m interval(1s) fill(value, )");

    //no query range
    SQL_PARSE_CMD_FAILED("select count(*) from m1 interval(1d) fill(none)");

    //fill data overflow
    SQL_PARSE_CMD_FAILED("select first(k) from m1 where ts>now and ts<now+1m interval(1d) fill(value, 9999999999999999999999999)");
    SQL_PARSE_CMD_FAILED("select first(x) from m1 where ts>now and ts<now+1m interval(1d) fill(value, 9999999999999999999999999)");
    SQL_PARSE_CMD_FAILED("select first(y) from m1 where ts>now and ts<now+1m interval(1d) fill(value, 9999999999999999999999999)");
    SQL_PARSE_CMD_FAILED("select first(f) from m1 where ts>now and ts<now+1m interval(1d) fill(value, 1e20000)");


    //23. complex query expression
    //error column name in expression
    SQL_PARSE_CMD_FAILED("select ((f*5.1)+(9*4)+(d%2)*0.95)/(22-99) from m1");

    SQL_PARSE_CMD_SUCCESS("select ((f*5.1)+(9*4)+(s%2)*0.95)/(22-99) from m1 WHERE ts>now and ts<now+2d and a<12 and a>=3 "
                          "and b='tm0' and c<55 and c>=1");

    //24. alias name too long will be truncated automatically
//    SQL_PARSE_CMD_FAILED("select k as kk123456789012345678901234567890 from tm0");
//    SQL_PARSE_CMD_FAILED("select k as kk123456789012345678901234567890123 from tm0");

    //invalid column name
    SQL_PARSE_CMD_FAILED("alter table ");

    //invalid
    SQL_PARSE_CMD_FAILED("alter table ");

    //too many columns
    SQL_PARSE_CMD_FAILED("alter table ");

    //no columns
    SQL_PARSE_CMD_FAILED("alter table ");

    //binary/nchar length value
    SQL_PARSE_CMD_FAILED("alter table ");

    //8. alter tags
}

