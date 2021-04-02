// TAOS asynchronous API example
// this example opens multiple tables, insert/retrieve multiple tables
// it is used by TAOS internally for one performance testing
// to compiple: gcc -o asyncdemo asyncdemo.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>

#include <taos.h>

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

void taos_insert_call_back(void *param, TAOS_RES *tres, int code);
void taos_select_call_back(void *param, TAOS_RES *tres, int code);
void taos_error(TAOS *taos);

int main(int argc, char *argv[])
{
  TAOS   *taos;
  struct  timeval systemTime;
  int     i;
  char    sql[1024] = { 0 };
  char    prefix[20] = { 0 };
  char    db[128] = { 0 };
  STable *tableList;

  if (argc == 1) {
    printf("usage: %s tablePrefix db pointsPerTable numOfTables cfg\n", argv[0]);
    exit(0);
  }

  // a simple way to parse input parameters
  if (argc >= 2) strcpy(prefix, argv[1]);
  if (argc >= 3) strcpy(db, argv[2]);
  if (argc >= 4) points = atoi(argv[3]);
  if (argc >= 5) numOfTables = atoi(argv[4]);
  if (argc >= 6) strcpy(configDir, argv[5]);

  size_t size = sizeof(STable) * (size_t)numOfTables;
  tableList = (STable *)malloc(size);
  memset(tableList, 0, size);

  taos_init();

  taos = taos_connect("192.168.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL)
    taos_error(taos);

  sprintf(sql, "create database %s", db);
  if (taos_query(taos, sql) != 0)
    taos_error(taos);

  sprintf(sql, "use %s", db);
  if (taos_query(taos, sql) != 0)
    taos_error(taos);

  printf("creating table ...\n");

  for (i = 0; i < numOfTables; ++i) {
    tableList[i].id = i;
    tableList[i].taos = taos;
    sprintf(tableList[i].name, "%s%d", prefix, i);
    sprintf(sql, "create table %s%d (ts timestamp, volume bigint)", prefix, i);
    if (taos_query(taos, sql) != 0)
      taos_error(taos);
  }

  gettimeofday(&systemTime, NULL);
  for (i = 0; i < numOfTables; ++i)
    tableList[i].timeStamp = (time_t)(systemTime.tv_sec) * 1000 + systemTime.tv_usec / 1000;

  printf("tables are created, press any key to insert\n");
  getchar();

  printf("start to insert\n");
  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec * 1000000 + systemTime.tv_usec;

  for (i = 0; i<numOfTables; ++i) {
    // insert records in asynchronous API
    sprintf(sql, "insert into %s values(now, 0)", tableList[i].name);
    taos_query_a(taos, sql, taos_insert_call_back, (void *)(tableList + i));
  }

  printf("once insert finished, presse any key to continue\n");
  getchar();

  printf("start to query\n");
  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec * 1000000 + systemTime.tv_usec;

  for (i = 0; i < numOfTables; ++i) {
    // select records in asynchronous API 
    sprintf(sql, "select * from %s", tableList[i].name);
    taos_query_a(taos, sql, taos_select_call_back, (void *)(tableList + i));
  }

  printf("once finished, press any key to exit\n");
  getchar();

  for (i = 0; i<numOfTables; ++i)  {
    printf("id:%d inserted:%d retrieved:%d\n", i, tableList[i].rowsInserted, tableList[i].rowsRetrieved);
  }

  getchar();

  taos_close(taos);
  free(tableList);

  return 0;
}

void taos_error(TAOS *con)
{
  fprintf(stderr, "TDengine error: %s\n", taos_errstr(con));
  taos_close(con);
  exit(1);
}

void taos_insert_call_back(void *param, TAOS_RES *tres, int code)
{
  STable *pTable = (STable *)param;
  struct  timeval systemTime;
  char    sql[128];

  pTable->rowsTried++;

  if (code < 0)  {
    printf("id:%d, insert failed, code:%d, rows:%d\n", pTable->id, code, pTable->rowsTried);
  }
  else if (code == 0) {
    printf("id:%d, not inserted\n", pTable->id);
  }
  else {
    pTable->rowsInserted++;
  }

  if (pTable->rowsTried < points) {
    // for this demo, insert another record
    sprintf(sql, "insert into %s values(now+%da, %d)", pTable->name, pTable->rowsTried, pTable->rowsTried);
    taos_query_a(pTable->taos, sql, taos_insert_call_back, (void *)pTable);
  }
  else {
    printf("id:%d, %d rows data are inserted\n", pTable->id, points);
    tablesProcessed++;
    if (tablesProcessed >= numOfTables) {
      gettimeofday(&systemTime, NULL);
      et = systemTime.tv_sec * 1000000 + systemTime.tv_usec;
      printf("%lld mseconds to insert %d data points\n", (et - st) / 1000, points*numOfTables);
    }
  }
}

void taos_retrieve_call_back(void *param, TAOS_RES *tres, int numOfRows)
{
  STable   *pTable = (STable *)param;
  struct timeval systemTime;

  if (numOfRows > 0) {

    for (int i = 0; i<numOfRows; ++i) {
      // synchronous API to retrieve a row from batch of records
      /*TAOS_ROW row = */taos_fetch_row(tres);
      // process row
    }

    pTable->rowsRetrieved += numOfRows;

    // retrieve next batch of rows
    taos_fetch_rows_a(tres, taos_retrieve_call_back, pTable);

  }
  else {
    if (numOfRows < 0)
      printf("id:%d, retrieve failed, code:%d\n", pTable->id, numOfRows);

    taos_free_result(tres);
    printf("id:%d, %d rows data retrieved\n", pTable->id, pTable->rowsRetrieved);

    tablesProcessed++;
    if (tablesProcessed >= numOfTables) {
      gettimeofday(&systemTime, NULL);
      et = systemTime.tv_sec * 1000000 + systemTime.tv_usec;
      printf("%lld mseconds to query %d data points\n", (et - st) / 1000, points * numOfTables);
    }
  }
}

void taos_select_call_back(void *param, TAOS_RES *tres, int code)
{
  STable *pTable = (STable *)param;

  if (code == 0 && tres) {
    // asynchronous API to fetch a batch of records
    taos_fetch_rows_a(tres, taos_retrieve_call_back, pTable);
  }
  else {
    printf("id:%d, select failed, code:%d\n", pTable->id, code);
    exit(1);
  }
}
