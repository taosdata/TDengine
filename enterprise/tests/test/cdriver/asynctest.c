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

#include "taos.h"
#include "tsclient.h"

int     points = 2000;
int     numOfTables = 100;
int     tablesProcessed = 0;
int64_t st, et;

typedef struct {
    int       id;
    TAOS     *taos;
    char      name[16];
    time_t    timeStamp;
    int       value;
    int       rowsInserted;
    int       rowsTried;
    int       rowsRetrieved;
} STable;

void tscInsertsCallBack(void *param, TAOS_RES *tres, int code);
void tscSelectCallBack(void *param, TAOS_RES *tres, int code);
void taos_error(TAOS *taos);

int main(int argc, char *argv[])
{
  TAOS   *taos;
  struct  timeval systemTime;
  char    qstr[128];
  int     i;
  char    payload[128], prefix[20], db[128];
  STable *tableList;

  if ( argc == 1 ) {
    printf("usage: %s tablePrefix db pointsPerTable numOfTables cfg\n", argv[0]);
    exit(0);
  }

  // a simple way to parse input parameters
  if (argc >= 2 ) strcpy(prefix, argv[1]);
  if (argc >= 3 ) strcpy(db, argv[2]);
  if (argc >= 4 ) points = atoi(argv[3]);
  if (argc >= 5 ) numOfTables = atoi(argv[4]);
  if (argc >= 6 ) strcpy(configDir, argv[5]);

  int size = sizeof(STable) * numOfTables;
  tableList = (STable *)malloc(size);
  memset(tableList, 0, size);

  taos_init();

  taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if ( taos == NULL)
    taos_error(taos);

  sprintf(payload, "create database %s", db);
  if ( taos_query(taos, payload) != 0 )
    taos_error(taos);

  sprintf(payload, "use %s", db);
  if ( taos_query(taos, payload) != 0 )
    taos_error(taos);

  printf("creating table ...\n");

  for (i=0; i<numOfTables; ++i) {
    tableList[i].id = i;
    tableList[i].taos = taos;
    sprintf(tableList[i].name, "%s%d", prefix, i);
    sprintf(qstr, "create table %s%d (ts timestamp, volume bigint)", prefix, i);
    if ( taos_query(taos, qstr) != 0 )
      taos_error(taos);
  }

  gettimeofday(&systemTime, NULL);
  for (i=0; i < numOfTables; ++i)
    tableList[i].timeStamp = (time_t)(systemTime.tv_sec)*1000 + systemTime.tv_usec/1000;

  printf("tables are created, press any key to insert\n");
  getchar();

  printf ("start to insert\n");
  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  for ( i=0; i<numOfTables; ++i) {
    // insert records in asynchronous API
    sprintf(qstr, "insert into %s values(now, 0)", tableList[i].name);
    taos_query_a(taos, qstr, tscInsertsCallBack, (void *)(tableList+i));
  }

  printf ("once finished, presse any key to continue\n");
  getchar();

  tablesProcessed = 0;
  printf ("start to query\n");
  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  for (i=0; i<numOfTables; ++i) {
    sprintf(qstr, "select * from %s", tableList[i].name);
    taos_query_a(taos, qstr, tscSelectCallBack, (void *)(tableList+i));
  }

  printf("once finished, press any key to exit\n");
  getchar();

  for (i=0; i<numOfTables; ++i)  {
    printf("id:%d inserted:%d retrieved:%d\n", i, tableList[i].rowsInserted, tableList[i].rowsRetrieved);
  }

  getchar();

  taos_close(taos);
  free (tableList);

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
  char    sql[128];

  pTable->rowsTried ++;

  if ( code < 0 )  {
    printf("id:%d, insert failed, code:%d, rows:%d\n", pTable->id, code, pTable->rowsTried);
  } else if ( code == 0 ) {
    printf("id:%d, not inserted\n", pTable->id);
  } else {
    pTable->rowsInserted++;
  }

  if ( pTable->rowsTried < points ) {
    sprintf(sql, "insert into %s values(now+%da, %d)", pTable->name, pTable->rowsTried, pTable->rowsTried);
    taos_query_a(pTable->taos, sql, tscInsertsCallBack, (void *)pTable);
  } else {
    printf("index:%d, %d rows data inserted\n", pTable->id, points);
    tablesProcessed++;
    if ( tablesProcessed >= numOfTables ) {
      gettimeofday(&systemTime, NULL);
      et = systemTime.tv_sec*1000000 + systemTime.tv_usec;
      printf("%"PRId64" mseconds to insert %d data points\n", (et-st)/1000, points*numOfTables);
    }
  }
}

void tscRetrieveCallBack(void *param, TAOS_RES *tres, int numOfRows)
{
  STable   *pTable = (STable *)param;
  struct timeval systemTime;

  if ( numOfRows > 0 ) {

    for (int i=0; i<numOfRows; ++i) {
      /*TAOS_ROW row = */taos_fetch_row(tres);
      //printf("%lld %lld\n", *((int64_t *)row[0]), *((int64_t *)row[1]));
    }

    pTable->rowsRetrieved += numOfRows;

    // retrieve next batch of rows
    taos_fetch_rows_a(tres, tscRetrieveCallBack, pTable);

  } else {

    tablesProcessed ++;
    if ( numOfRows < 0 )
      printf("id:%d, retrieve failed, code:%d\n", pTable->id, numOfRows);

    taos_free_result(tres);
    //printf("index:%d, %d rows data retrieved\n", pTable->id, pTable->rowsRetrieved);

    if ( tablesProcessed >= numOfTables ) {
      gettimeofday(&systemTime, NULL);
      et = systemTime.tv_sec*1000000 + systemTime.tv_usec;
      printf("%.3f mseconds to query %d data points\n", (et-st)/1000.0, points*numOfTables);
    }
  }
}

void tscSelectCallBack(void *param, TAOS_RES *tres, int code)
{
  STable *pTable = (STable *) param;

  if ( code == 0 && tres ) {
    taos_fetch_rows_a(tres, tscRetrieveCallBack, pTable);
  } else {
    printf("id:%d, select failed, code:%d\n", pTable->id, code);
    exit(1);
  }
}
