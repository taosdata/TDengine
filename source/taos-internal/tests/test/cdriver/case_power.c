// TAOS asynchronous API example
// this example opens multiple tables, insert/retrieve multiple tables
// it is used by TAOS internally for one performance testing
// for a simple async example, check asyncdemo.c
// to compiple: gcc -o masync masync.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

#include "taos.h"
#include "tsclient.h"

#define AREA_LEN 16
#define TYPE_LEN 2

int64_t st, et;
pthread_mutex_t    finishedMutex;
int finished = 0;

typedef struct {
    TAOS     *taos;
    char      tb[5];
    int       tableTotal;
    int       rowsTotal;
    
    int       tableIndex;
    char      name[16];
    int       rowsInserted;
    int       rowsTried;   
} STable;

void tscInsertsCallBack(void *param, TAOS_RES *tres, int code);
void tscSelectCallBack(void *param, TAOS_RES *tres, int code);
void taos_error(TAOS *taos);

int main(int argc, char *argv[])
{
    TAOS   *taos;
    struct  timeval systemTime;
    char    qstr[256], payload[256];
    char    mt[] = "mt";
    char    db[] = "db";
    char    areaName[AREA_LEN][20] = { "dongcheng", "xicheng", "haidian", "chaoyang", "fengtai", "mentougou", "shijinshan", "fangshan", "tongzhou", "shunyi", "changping", "daxing", "huairou", "pinggu", "yanqing", "miyun" };
    char    tbName[AREA_LEN][5] = { "dc", "xc", "hd", "cy", "ft", "mtg", "sjs", "fs", "tz", "sy", "cp", "dx", "hr", "pg", "yq", "my" };
    char    typeName[TYPE_LEN][10] = { "sun", "zone" };
	STable *tableList;
	int     points = 200;
    int     numOfTables = 5;
    int     replica = 1;
    pthread_mutex_init(&finishedMutex, NULL);

    if (argc == 1) {
        printf("usage: %s tbNum rowNum cfgDir replicaNum \n", argv[0]);
        exit(0);
    }

    // a simple way to parse input parameters
    if (argc >= 2) numOfTables = atoi(argv[1]);
    if (argc >= 3) points = atoi(argv[2]);
    if (argc >= 4) strcpy(configDir, argv[3]);
    if (argc >= 5) replica = atoi(argv[4]);
    if (replica < 1) replica = 1;
    if (replica > 5) replica = 5;
    
    int size = sizeof(STable) * AREA_LEN;
    tableList = (STable *)taosMemoryMalloc(size);
    memset(tableList, 0, size);

    taos_init();
    taos = taos_connect(tsMasterIp, "root", "taosdata", NULL, 0);
    if (taos == NULL)
        taos_error(taos);

    sprintf(payload, "drop database %s", db);
    printf("%s\n", payload);
    taos_query(taos, payload);

    sprintf(payload, "create database %s replica %d", db, replica);
    printf("%s\n", payload);
    if (taos_query(taos, payload) != 0)
        taos_error(taos);

    sprintf(payload, "use %s", db);
    printf("%s\n", payload);
    if (taos_query(taos, payload) != 0)
        taos_error(taos);

    sprintf(payload, "create table %s (ts timestamp, status bool, ia float, ib float, ic float, va float vb float, vc float, p float, q float, pre float, temp float, hum float, pm float) TAGS(area binary(20), type binary(10))"
        , mt);
    printf("%s\n", payload);
    if (taos_query(taos, payload) != 0)
        taos_error(taos);

    printf("creating table ...\n");

    for (int i = 0; i < numOfTables; ++i) {
        int errorTimes = 0;
        for (int j = 0; j < AREA_LEN; ++j) {
            char * area = areaName[j];
            char * tb = tbName[j];
            char * type = typeName[j % TYPE_LEN];

            sprintf(qstr, "create table %s%d using %s tags( '%s' , '%s' )", tb, i, mt, area, type);
            if (taos_query(taos, qstr) != 0) {
                errorTimes++;
                if (errorTimes > 3) {
                    taos_error(taos);
                }
                --j;
            }                
        }
    }

    taos_close(taos);
    printf("%d tables are created, begin insert %d rows each table ... \n", numOfTables * AREA_LEN, points);

    gettimeofday(&systemTime, NULL);
    st = systemTime.tv_sec * 1000000 + systemTime.tv_usec;

    for (int i = 0; i < AREA_LEN; ++i) {
        STable *pTable = tableList + i;

        pTable->taos = taos_connect(tsMasterIp, "root", "taosdata", NULL, 0);
        if (pTable->taos == NULL)
            taos_error(pTable->taos);

        sprintf(payload, "use %s", db);
        if (taos_query(pTable->taos, payload) != 0)
            taos_error(pTable->taos);

        strcpy(pTable->tb, tbName[i]);
        pTable->tableTotal = numOfTables;
        pTable->rowsTotal = points;
        
        //start a insert 
        pTable->tableIndex = 0;
        sprintf(pTable->name, "%s%d", pTable->tb, pTable->tableIndex);        
        pTable->rowsInserted = 0;
        pTable->rowsTried = 0;

        int64_t ts = pTable->rowsInserted;
        ts = ts * 60 * 1000 + 1519833600000;
        sprintf(qstr, "insert into %s values(%lld, 1, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d)"
            , pTable->name, ts
            , pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted
            , pTable->rowsInserted, pTable->rowsInserted
            , pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted);

        taos_query_a(pTable->taos, qstr, tscInsertsCallBack, (void *)pTable);
    }    

    while (true) {
        pthread_mutex_lock(&finishedMutex);
        if (finished >= AREA_LEN) {
            uPrint("-----------> insert finished\n");
            pthread_mutex_unlock(&finishedMutex);
            break;
        }
        pthread_mutex_unlock(&finishedMutex);
        sleep(500);
    }
   
    //getchar();
    for (int i = 0; i < numOfTables; ++i)  {
        STable *pTable = tableList + i;
        taos_close(pTable->taos);
    }
    taosMemoryFree(tableList);
    //printf("quit the program\n");
    
    return 0;
}

void taos_error(TAOS *con)
{
    fprintf(stderr, "TDengine error: %s\n", taos_errstr(con));
    taos_close(con);
    exit(1);
}

void tscInsertsCallBack(void *param, TAOS_RES *tres, int code)
{
    STable *pTable = (STable *)param;
    struct  timeval systemTime;
    char    qstr[128];

    pTable->rowsTried++;

    if (code < 0)  {
        uError("tbName: %s, insert failed, code:%d, tried: %d, inserted: %d", pTable->name, code, pTable->rowsTried, pTable->rowsInserted);
    }
    else if (code == 0) {
        uError("tbName: %s, not inserted, code: 0, tried: %d, inserted: %d", pTable->name, pTable->rowsTried, pTable->rowsInserted);
    }
    else {
        pTable->rowsInserted++;
    }

    if (pTable->rowsInserted < pTable->rowsTotal) {
        int64_t ts = pTable->rowsInserted;
        ts = ts * 60 * 1000 + 1519833600000;
        sprintf(qstr, "insert into %s values(%lld, 1, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d)"
            , pTable->name, ts
            , pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted
            , pTable->rowsInserted, pTable->rowsInserted
            , pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted);

        if (pTable->rowsInserted % 10000 == 0 || pTable->rowsInserted == pTable->rowsTotal) {
           uPrint("tb: %s inserted: %d tried: %d", pTable->name, pTable->rowsInserted, pTable->rowsTried);
        }
        taos_query_a(pTable->taos, qstr, tscInsertsCallBack, (void *)pTable);
    }
    else {
        uPrint("tbName: %s, %d rows data inserted", pTable->name, pTable->rowsInserted);

        pTable->tableIndex++;
        
        if (pTable->tableIndex >= pTable->tableTotal) {
            gettimeofday(&systemTime, NULL);
            et = systemTime.tv_sec * 1000000 + systemTime.tv_usec;
            //uPrint("===>  %ld mseconds to insert %d data points", (et - st) / 1000, pTable->rowsTotal*pTable->tableTotal*AREA_LEN);
            pthread_mutex_lock(&finishedMutex);
            finished++;
            pthread_mutex_unlock(&finishedMutex);
        }
        else {
            sprintf(pTable->name, "%s%d", pTable->tb, pTable->tableIndex);
            pTable->rowsInserted = 0;
            pTable->rowsTried = 0;

            int64_t ts = pTable->rowsInserted;
            ts = ts * 60 * 1000 + 1519833600000;
            sprintf(qstr, "insert into %s values(%lld, 1, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d)"
                , pTable->name, ts
                , pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted
                , pTable->rowsInserted, pTable->rowsInserted
                , pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted, pTable->rowsInserted);

            taos_query_a(pTable->taos, qstr, tscInsertsCallBack, (void *)pTable);
        }
    }
}
