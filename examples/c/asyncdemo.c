/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

// TAOS asynchronous API example
// this example opens multiple tables, insert/retrieve multiple tables
// it is used by TAOS internally for one performance testing
// to compiple: gcc -o asyncdemo asyncdemo.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>
#include <inttypes.h>
#include "taos.h"

int     points = 5;
int     numOfTables = 3;
int     tablesInsertProcessed = 0;
int     tablesSelectProcessed = 0;
int64_t st, et;

typedef struct {
  int       id;
  TAOS     *taos;
  char      name[32];
  time_t    timeStamp;
  int       value;
  int       rowsInserted;
  int       rowsTried;
  int       rowsRetrieved;
} STable;

void taos_insert_call_back(void *param, TAOS_RES *tres, int code);
void taos_select_call_back(void *param, TAOS_RES *tres, int code);
void shellPrintError(TAOS *taos);

static void queryDB(TAOS *taos, char *command) {
  int i;
  TAOS_RES *pSql = NULL;
  int32_t   code = -1;

  for (i = 0; i < 5; i++) {
    if (NULL != pSql) {
      taos_free_result(pSql);
      pSql = NULL;
    }
    
    pSql = taos_query(taos, command);
    code = taos_errno(pSql);
    if (0 == code) {
      break;
    }    
  }

  if (code != 0) {
    fprintf(stderr, "Failed to run %s, reason: %s\n", command, taos_errstr(pSql));
    taos_free_result(pSql);
    taos_close(taos);
    taos_cleanup();
    exit(EXIT_FAILURE);
  }

  taos_free_result(pSql);
}

int main(int argc, char *argv[])
{
  TAOS   *taos;
  struct  timeval systemTime;
  int     i;
  char    sql[1024]  = { 0 };
  char    prefix[20] = { 0 };
  char    db[128]    = { 0 };
  STable *tableList;

  if (argc != 5) {
    printf("usage: %s server-ip dbname rowsPerTable numOfTables\n", argv[0]);
    exit(0);
  }

  // a simple way to parse input parameters
  if (argc >= 3) strncpy(db, argv[2], sizeof(db) - 1);
  if (argc >= 4) points = atoi(argv[3]);
  if (argc >= 5) numOfTables = atoi(argv[4]);

  size_t size = sizeof(STable) * (size_t)numOfTables;
  tableList = (STable *)malloc(size);
  memset(tableList, 0, size);

  taos = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL)
    shellPrintError(taos);

  printf("success to connect to server\n");

  sprintf(sql, "drop database if exists %s", db);
  queryDB(taos, sql);

  sprintf(sql, "create database %s", db);
  queryDB(taos, sql);

  sprintf(sql, "use %s", db);
  queryDB(taos, sql);

  strcpy(prefix, "asytbl_");
  for (i = 0; i < numOfTables; ++i) {
    tableList[i].id = i;
    tableList[i].taos = taos;
    sprintf(tableList[i].name, "%s%d", prefix, i);
    sprintf(sql, "create table %s%d (ts timestamp, volume bigint)", prefix, i);
    queryDB(taos, sql);
  }  

  gettimeofday(&systemTime, NULL);
  for (i = 0; i < numOfTables; ++i)
    tableList[i].timeStamp = (time_t)(systemTime.tv_sec) * 1000 + systemTime.tv_usec / 1000;

  printf("success to create tables, press any key to insert\n");
  getchar();

  printf("start to insert...\n");
  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec * 1000000 + systemTime.tv_usec;

  tablesInsertProcessed = 0;
  tablesSelectProcessed = 0;

  for (i = 0; i<numOfTables; ++i) {
    // insert records in asynchronous API
    sprintf(sql, "insert into %s values(%ld, 0)", tableList[i].name, 1546300800000 + i);
    taos_query_a(taos, sql, taos_insert_call_back, (void *)(tableList + i));
  }

  printf("once insert finished, presse any key to query\n");
  getchar();

  while(1) {
    if (tablesInsertProcessed < numOfTables) {
       printf("wait for process finished\n");
       sleep(1);
       continue;
    }  

    break;
  }

  printf("start to query...\n");
  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec * 1000000 + systemTime.tv_usec;


  for (i = 0; i < numOfTables; ++i) {
    // select records in asynchronous API 
    sprintf(sql, "select * from %s", tableList[i].name);
    taos_query_a(taos, sql, taos_select_call_back, (void *)(tableList + i));
  }

  printf("\nonce finished, press any key to exit\n");
  getchar();

  while(1) {
    if (tablesSelectProcessed < numOfTables) {
       printf("wait for process finished\n");
       sleep(1);
       continue;
    }  

    break;
  }

  for (i = 0; i<numOfTables; ++i)  {
    printf("%s inserted:%d retrieved:%d\n", tableList[i].name, tableList[i].rowsInserted, tableList[i].rowsRetrieved);
  }

  taos_close(taos);
  free(tableList);

  printf("==== async demo end====\n");
  printf("\n");
  return 0;
}

void shellPrintError(TAOS *con)
{
  fprintf(stderr, "TDengine error: %s\n", taos_errstr(con));
  taos_close(con);
  taos_cleanup();
  exit(1);
}

void taos_insert_call_back(void *param, TAOS_RES *tres, int code)
{
  STable *pTable = (STable *)param;
  struct  timeval systemTime;
  char    sql[128];

  pTable->rowsTried++;

  if (code < 0)  {
    printf("%s insert failed, code:%d, rows:%d\n", pTable->name, code, pTable->rowsTried);
  }
  else if (code == 0) {
    printf("%s not inserted\n", pTable->name);
  }
  else {
    pTable->rowsInserted++;
  }

  if (pTable->rowsTried < points) {
    // for this demo, insert another record
    sprintf(sql, "insert into %s values(%ld, %d)", pTable->name, 1546300800000+pTable->rowsTried*1000, pTable->rowsTried);
    taos_query_a(pTable->taos, sql, taos_insert_call_back, (void *)pTable);
  }
  else {
    printf("%d rows data are inserted into %s\n", points, pTable->name);
    tablesInsertProcessed++;
    if (tablesInsertProcessed >= numOfTables) {
      gettimeofday(&systemTime, NULL);
      et = systemTime.tv_sec * 1000000 + systemTime.tv_usec;
      printf("%" PRId64 " mseconds to insert %d data points\n", (et - st) / 1000, points*numOfTables);
    }
  }
  
  taos_free_result(tres);
}

void taos_retrieve_call_back(void *param, TAOS_RES *tres, int numOfRows)
{
  STable   *pTable = (STable *)param;
  struct timeval systemTime;

  if (numOfRows > 0) {

    for (int i = 0; i<numOfRows; ++i) {
      // synchronous API to retrieve a row from batch of records
      /*TAOS_ROW row = */(void)taos_fetch_row(tres);
      // process row
    }

    pTable->rowsRetrieved += numOfRows;

    // retrieve next batch of rows
    taos_fetch_rows_a(tres, taos_retrieve_call_back, pTable);

  }
  else {
    if (numOfRows < 0)
      printf("%s retrieve failed, code:%d\n", pTable->name, numOfRows);

    //taos_free_result(tres);
    printf("%d rows data retrieved from %s\n", pTable->rowsRetrieved, pTable->name);

    tablesSelectProcessed++;
    if (tablesSelectProcessed >= numOfTables) {
      gettimeofday(&systemTime, NULL);
      et = systemTime.tv_sec * 1000000 + systemTime.tv_usec;
      printf("%" PRId64 " mseconds to query %d data rows\n", (et - st) / 1000, points * numOfTables);
    }

    taos_free_result(tres);
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
    printf("%s select failed, code:%d\n", pTable->name, code);
    taos_free_result(tres);
    taos_cleanup();
    exit(1);
  }
}
