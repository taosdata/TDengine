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
#include <stdint.h>

#include "taos.h"
#include "tsclient.h"

int     points = 2000;
int     numOfTables = 100;
int     tablesProcessed = 0;
int64_t st, et;

typedef struct {
  int       id;
  TAOS_TAB *table;
  time_t    timeStamp;
  int       value;
  int       rowsInserted;
  int       rowsTried;
  int       rowsRetrieved;
} STable;

void tscInsertsCallBack(void *param, int code);
void tscRetrieveCallBack(void *param, int numOfRows); 
void tscSelectCallBack(void *param, int code);
void taos_error(TAOS *taos);

int main(int argc, char *argv[]) 
{
  TAOS   *taos;
  struct  timeval systemTime;
  char    qstr[128];
  int     i, code;
  char    name[30], payload[128], prefix[20], db[128];
  STable *tableList;

  if ( argc == 1 ) {
    printf("usage: %s tablePrefix db numOfTables cfg\n", argv[0]);
    exit(0);
  }

  // a simple way to parse input parameters
  if (argc >= 2 ) strcpy(prefix, argv[1]);
  if (argc >= 3 ) strcpy(db, argv[2]);
  if (argc >= 4 ) numOfTables = atoi(argv[3]);
  if (argc >= 5 ) strcpy(configDir, argv[4]);

  int size = sizeof(STable) * numOfTables;
  tableList = (STable *)taosMemoryMalloc(size);
  memset(tableList, 0, size);

  taos_init();

  taos = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
  if ( taos == NULL) 
    taos_error(taos);

  sprintf(payload, "USE %s", db);
  if ( taos_query(taos, payload) != 0 ) 
    taos_error(taos);

  // open tables in asynchronous API
  for (i=0; i < numOfTables; ++i) {
    sprintf(name, "%s%d", prefix, i);
    tableList[i].table = taos_open_table(taos, name);
    if ( tableList[i].table == NULL ) 
      taos_error(taos);

    tableList[i].id = i;
    tableList[i].timeStamp = (time_t)(systemTime.tv_sec)*1000 + systemTime.tv_usec/1000;
  }
 
  TAOS_SEARCH search;
  short       colList[8];
  memset(&search, 0, sizeof(search));
  search.numOfCols = 2;
  search.order = 1;
  colList[0] = 0;
  colList[1] = 1;
  search.colList = colList;
  tablesProcessed = 0;

  printf ("start to query\n");
  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec*1000000 + systemTime.tv_usec;

  for (i=0; i<numOfTables; ++i) {
    code = taos_select(tableList[i].table, &search, tscSelectCallBack, (void *)(tableList+i) );
    if ( code != 0 ) {
      printf("id:%d failed to select, code:%d\n", i, code);
      tablesProcessed++;
    }
  }
  
  printf("once finished, press any key to exit\n");
  getchar();

  for (i=0; i<numOfTables; ++i)  {
    printf("id:%d retrieved:%d\n", i, tableList[i].rowsRetrieved);
    taos_free_result_async(tableList[i].table);
  }

  for (i=0; i<numOfTables; ++i)  {
    taos_close_table(tableList[i].table);
  }

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

void tscRetrieveCallBack(void *param, int numOfRows) 
{
  STable   *pTable = (STable *)param;
  TAOS_ROW  row;
  struct timeval systemTime;

  if ( numOfRows > 0 ) {

    for (int i=0; i<numOfRows; ++i) {
      row = taos_fetch_row_async(pTable);
      // printf("%lld %lld\n", *((int64_t *)row[0]), *((int64_t *)row[1]));     
    }

    pTable->rowsRetrieved += numOfRows;

    // retrieve next batch of rows
    int code = taos_retrieve_async(pTable->table, tscRetrieveCallBack, pTable);
    if ( code != 0 ) {
      printf("id:%d, failed to retrieve, code:%d numOfRows:%d\n", pTable->id, code, numOfRows);
      sleep(10000);
      exit(1);
    }

  } else {

    tablesProcessed ++;
    if ( numOfRows < 0 )
      printf("id:%d, retrieve failed, code:%d\n", pTable->id, -numOfRows); 
   
    if ( tablesProcessed >= numOfTables ) {
      gettimeofday(&systemTime, NULL);
      et = systemTime.tv_sec*1000000 + systemTime.tv_usec;
      printf("%.3f mseconds is spent on query\n", (et-st)/1000.0);
    }
  }
}

void tscSelectCallBack(void *param, int code)
{
  STable *pTable = (STable *) param;

  if ( code == 0 ) {
    code = taos_retrieve_async(pTable->table, tscRetrieveCallBack, pTable);
    if ( code != 0 ) {
      printf("id:%d, failed to retrieve after select, code:%d\n", pTable->id, code);
      exit(1);
    }
  } else {
    printf("id:%d, select failed, code:%d\n", pTable->id, code);
    exit(1);
  }
}


