#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <float.h>
#include <wordexp.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <strings.h>
#include <sys/time.h>

#include "taos.h"
#include "tutil.h"
#include "testCommon.h"
#include "../../client/inc/tsclient.h"

static TAOS* conn = NULL;

void setUp();
void tearDown();
void doTest();

void prepareTableNData();
void cleanUpData();

int main(int argc, char **argv) {//sizeof(SSqlCmd) == 3824
    setUp();
    doTest();

    tearDown();
}

void setUp() {
//    conn = connect();
    conn = taos_connect("192.168.0.1", "root", "taosdata", NULL, 0);
    executeSQL(conn, "use t_1", NULL);
//    prepareTableNData();
}

void tearDown() {
//    cleanUpData();
    taos_close(conn);
}

void doTest() {
    //case 1:
//    executeSQL(conn, "select count(*) from m1 where ts>10 and ts<12000 interval(10a) fill(value, 999)", NULL);

    //case 2:
    executeSQL(conn, "select count(*) from m1 where ts>10 and ts<14000 interval(10a) fill(value, 999)", NULL);
//    executeSQL(conn, "select count(*) from m2 where ts>70000 and ts<104000 interval(10a) fill(value, 999)", NULL);

    //case 3:
//    executeSQL(conn, "select count(*) from m1 where ts>7000 and ts<10001 interval(10a) fill(value, 999)", NULL);

    //case 4:
//    executeSQL(conn, "select count(*) from m1 where ts>9998 and ts<10001 interval(10a) fill(value, 999)", NULL);

    //case 5:
//    executeSQL(conn, "select count(*) from m1 where ts>=11000 and ts<12001 interval(10a) fill(value, 999)", NULL);

    //case 6:
//    executeSQL(conn, "select count(*) from m1 where ts>11000 and ts<14000 interval(10a) fill(value, 999)", NULL);

    //case 7:
//    executeSQL(conn, "select count(*) from m1 where ts>12000 and ts<19000 interval(10a) fill(value, 999)", NULL);

    //case 8:
//    executeSQL(conn, "select count(*) from m1 where ts>14000 and ts<19000 interval(10a) fill(value, 999)", NULL);

    //case 9:
//    executeSQL(conn, "select count(*) from m1 where ts>10 and ts<=9999 interval(10a) fill(value, 999)", NULL);

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //the following is fill test for meter
    //rowsInFileBlock = 255

    //case 1:
//    executeSQL(conn, "select count(*) from t where ts>10 and ts<102000 interval(10a) fill(value, 999)", NULL);

    //case 2:
//    executeSQL(conn, "select count(*) from t where ts>70000 and ts<104000 interval(10a) fill(value, 999)", NULL);

    //case 3:
//    executeSQL(conn, "select count(*) from t where ts>70000 and ts<100001 interval(10a) fill(value, 999)", NULL);

    //case 4:
//    executeSQL(conn, "select count(*) from t where ts>99980 and ts<100001 interval(10a) fill(value, 999)", NULL);

    //case 5:
//    executeSQL(conn, "select count(*) from t where ts>=101000 and ts<102001 interval(10a) fill(value, 999)", NULL);

    //case 6:
//    executeSQL(conn, "select count(*) from t where ts>101000 and ts<104000 interval(10a) fill(value, 999)", NULL);

    //case 7:
//    executeSQL(conn, "select count(*) from t where ts>102000 and ts<109000 interval(10a) fill(value, 999)", NULL);

    //case 8:
//    executeSQL(conn, "select count(*) from t where ts>104000 and ts<109000 interval(10a) fill(value, 999)", NULL);

    //case 9:
//    executeSQL(conn, "select count(*) from t where ts>10 and ts<=100000-1 interval(10a) fill(value, 999)", NULL);

}

void prepareTableNData() {
    executeSQL(conn, "create database t_1", NULL);
    executeSQL(conn, "use t_1", NULL);

    char sql[512] = {0};
    executeSQL(conn, "create table m1 (ts timestamp, k int) tags(a int)", NULL);

    executeSQL(conn, "create table tm0 using m1 tags(1)", NULL);
    executeSQL(conn, "create table tm1 using m1 tags(2)", NULL);

    for(int32_t i=0; i<2600; ++i) {
        sprintf(sql, "insert into tm0 values(%d, %d)", i+10000, i);
        executeSQL(conn, sql, NULL);

        sprintf(sql, "insert into tm1 values(%d, %d)", i+10000+1, i);
        executeSQL(conn, sql, NULL);
    }

    executeSQL(conn, "create table m2 (ts timestamp, k int) tags(a int)", NULL);

    executeSQL(conn, "create table t using m2 tags(2)", NULL);
    for(int32_t i=0; i<2600; ++i) {
        sprintf(sql, "insert into t values(%d, %d)", i+100000, i);
        executeSQL(conn, sql, NULL);
    }

}

void cleanUpData() {
    executeSQL(conn, "drop database t_1", NULL);
}