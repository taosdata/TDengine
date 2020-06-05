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

// TAOS standard API example. The same syntax as MySQL, but only a subet
// to compile: gcc -o demo demo.c -ltaos

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>  // TAOS header file
#include <unistd.h>

void taosMsleep(int mseconds);

static int32_t doQuery(TAOS* taos, const char* sql) {
  TAOS_RES* res = taos_query(taos, sql);
  if (taos_errno(res) != 0) {
    printf("failed to execute query, reason:%s\n", taos_errstr(taos));
    return -1;
  }
  
  TAOS_ROW row = NULL;
  char buf[512] = {0};
  
  int32_t numOfFields = taos_num_fields(res);
  TAOS_FIELD* pFields = taos_fetch_fields(res);
  
  while((row = taos_fetch_row(res)) != NULL) {
    taos_print_row(buf, row, pFields, numOfFields);
    printf("%s\n", buf);
    memset(buf, 0, 512);
  }
  
  taos_free_result(res);
  return 0;
}

void* oneLoader(void* param) {
  TAOS* conn = (TAOS*) param;
  
  for(int32_t i = 0; i < 20000; ++i) {
//    doQuery(conn, "show databases");
    doQuery(conn, "use test");
//    doQuery(conn, "describe t12");
//    doQuery(conn, "show tables");
//    doQuery(conn, "create table if not exists abc (ts timestamp, k int)");
//    doQuery(conn, "select * from t12");
  }
  
  return 0;
}


static __attribute__((unused)) void multiThreadTest(int32_t numOfThreads, void* conn) {
  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  
  pthread_t* threadId = malloc(sizeof(pthread_t)*numOfThreads);
  
  for (int i = 0; i < numOfThreads; ++i) {
    pthread_create(&threadId[i], NULL, oneLoader, conn);
  }
  
  for (int32_t i = 0; i < numOfThreads; ++i) {
    pthread_join(threadId[i], NULL);
  }
  
  free(threadId);
  pthread_attr_destroy(&thattr);
}

int main(int argc, char *argv[]) {
  TAOS *    taos;
  char      qstr[1024];
  TAOS_RES *result;
  
  // connect to server
  if (argc < 2) {
    printf("please input server-ip \n");
    return 0;
  }
  
  taos_options(TSDB_OPTION_CONFIGDIR, "~/sec/cfg");
  
  // init TAOS
  taos_init();
  
  taos = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to server, reason:%s\n", taos_errstr(taos));
    exit(1);
  }
  printf("success to connect to server\n");
  
//  multiThreadTest(1, taos);
  doQuery(taos, "use test");
  doQuery(taos, "alter table tm99 set tag a=99");
//  for(int32_t i = 0; i < 100000; ++i) {
//    doQuery(taos, "insert into t1 values(now, 2)");
//  }
//  doQuery(taos, "create table t1(ts timestamp, k binary(12), f nchar(2))");
  
  taos_close(taos);
  return 0;
  
  taos_query(taos, "drop database demo");
  if (taos_query(taos, "create database demo") != 0) {
    printf("failed to create database, reason:%s\n", taos_errstr(taos));
    exit(1);
  }
  printf("success to create database\n");
  
  
  taos_query(taos, "use demo");
  
  
  // create table
  if (taos_query(taos, "create table m1 (ts timestamp, speed int)") != 0) {
    printf("failed to create table, reason:%s\n", taos_errstr(taos));
    exit(1);
  }
  printf("success to create table\n");
  
  
  // sleep for one second to make sure table is created on data node
  // taosMsleep(1000);
  
  
  // insert 10 records
  int i = 0;
  for (i = 0; i < 10; ++i) {
    sprintf(qstr, "insert into m1 values (%ld, %d)", 1546300800000 + i * 1000, i * 10);
    if (taos_query(taos, qstr)) {
      printf("failed to insert row: %i, reason:%s\n", i, taos_errstr(taos));
    }
    //sleep(1);
  }
  printf("success to insert rows, total %d rows\n", i);
  
  
  // query the records
  sprintf(qstr, "SELECT * FROM m1");
  if (taos_query(taos, qstr) != 0) {
    printf("failed to select, reason:%s\n", taos_errstr(taos));
    exit(1);
  }
  
  if (result == NULL) {
    printf("failed to get result, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

//  TAOS_ROW    row;
  
  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_field_count(taos);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  char        temp[256];
  
  
  printf("select * from table, result:\n");
  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("%s\n", temp);
  }
  
  taos_free_result(result);
  printf("====demo end====\n\n");
  return getchar();
}
