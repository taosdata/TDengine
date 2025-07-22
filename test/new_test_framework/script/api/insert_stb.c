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

// TAOS standard API example. The same syntax as MySQL, but only a subset
// to compile: gcc -o demo demo.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <stdint.h>

#include "taos.h"  // TAOS header file

static int64_t currTimeInUs() {
  struct timeval start_time;
  gettimeofday(&start_time, NULL);
  return (start_time.tv_sec) * 1000000 + (start_time.tv_usec);  
}

static void executeSql(TAOS *taos, char *command) {
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
    exit(EXIT_FAILURE);
  }

  taos_free_result(pSql);
}

void testInsert(TAOS *taos, char *qstr, double* pElapsedTime)  {
  executeSql(taos, "drop database if exists demo2");
  executeSql(taos, "create database demo2");
  executeSql(taos, "use demo2");

  executeSql(taos, "create table st (ts timestamp, ti tinyint, si smallint, i int, bi bigint, f float, d double, b binary(10)) tags(t1 int, t2 float, t3 binary(10))");
  printf("success to create table\n");

  int64_t ts1 = currTimeInUs();

  for (int tblIdx = 0; tblIdx < 10; ++tblIdx) {
    int len = 0;
    len += sprintf(qstr+len, "insert into ct%d using st tags(%d, %f, '%s')", tblIdx, tblIdx, (float)tblIdx, "childtable");
    int batchStart = len;
    for (int batchIdx = 0; batchIdx < 10000; ++batchIdx) {
      len = batchStart;
      len += sprintf(qstr+len, " values");

      for (int rowIdx = 0; rowIdx < 100; ++ rowIdx) {
        int i = rowIdx + batchIdx * 100 + tblIdx*10000*100;
        len += sprintf(qstr+len, " (%" PRId64 ", %d, %d, %d, %d, %f, %lf, '%s')", (uint64_t)(1546300800000 + i), (int8_t)i, (int16_t)i, i, i, i*1.0, i*2.0, "hello");      
      }
      TAOS_RES *result1 = taos_query(taos, qstr);
      if (result1 == NULL || taos_errno(result1) != 0) {
        printf("failed to insert row, reason:%s. qstr: %s\n", taos_errstr(result1), qstr);
        taos_free_result(result1);
        exit(1);
      }
      taos_free_result(result1);      
    }
  }

  int64_t ts2 = currTimeInUs();
  double elapsedTime = (double)(ts2-ts1) / 1000000.0;  
  *pElapsedTime = elapsedTime;

  printf("elapsed time: %.3f\n", elapsedTime);
  executeSql(taos, "drop database if exists demo2");
}

void testInsertStb(TAOS *taos, char *qstr, double *pElapsedTime)  {
  executeSql(taos, "drop database if exists demo");
  executeSql(taos, "create database demo");
  executeSql(taos, "use demo");

  executeSql(taos, "create table st (ts timestamp, ti tinyint, si smallint, i int, bi bigint, f float, d double, b binary(10)) tags(t1 int, t2 float, t3 binary(10))");
  printf("success to create table\n");

  int64_t ts1 = currTimeInUs();

  for (int tblIdx = 0; tblIdx < 10; ++tblIdx) {
    int len = 0;
    len += sprintf(qstr+len, "insert into st(tbname, t1, t2, t3, ts, ti, si, i, bi, f, d, b)");
    int batchStart = len;
    for (int batchIdx = 0; batchIdx < 10000; ++batchIdx) {
      len = batchStart;
      len += sprintf(qstr+len, " values");

      for (int rowIdx = 0; rowIdx < 100; ++rowIdx) {
        int i = rowIdx + batchIdx * 100 + tblIdx*10000*100;
        len += sprintf(qstr+len, " ('ct%d', %d, %f, '%s', %" PRId64 ", %d, %d, %d, %d, %f, %lf, '%s')", tblIdx, tblIdx, (float)tblIdx, "childtable", 
                      (uint64_t)(1546300800000 + i), (int8_t)i, (int16_t)i, i, i, i*1.0, i*2.0, "hello");      
      }
      TAOS_RES *result1 = taos_query(taos, qstr);
      if (result1 == NULL || taos_errno(result1) != 0) {
        printf("failed to insert row, reason:%s. qstr: %s\n", taos_errstr(result1), qstr);
        taos_free_result(result1);
        exit(1);
      }
      taos_free_result(result1);      
    }
  }
  
  int64_t ts2 = currTimeInUs();
  double elapsedTime = (double)(ts2 - ts1) / 1000000.0;
  *pElapsedTime = elapsedTime;
  printf("elapsed time: %.3f\n", elapsedTime);
  executeSql(taos, "drop database if exists demo");
}


int main(int argc, char *argv[]) {

  // connect to server
  if (argc < 2) {
    printf("please input server-ip \n");
    return 0;
  }

  TAOS *taos = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to server, reason:%s\n", taos_errstr(NULL));
    exit(1);
  }
  char* qstr = malloc(1024*1024);
  {
    printf("test insert into tb using stb\n\n");
    double sum = 0;
    for (int i =0; i < 5; ++i) {
      double elapsed = 0;
      testInsert(taos, qstr, &elapsed);
      sum += elapsed;
    }
    printf("average insert tb using stb time : %.3f\n", sum/5);
  }
  printf("test insert into stb tbname\n\n");
  {
    printf("test insert into stb\n\n");
    double sum = 0;
    for (int i =0; i < 5; ++i) {
      double elapsed = 0;
      testInsertStb(taos, qstr, &elapsed);
      sum += elapsed;
    }
    printf("average insert into stb time : %.3f\n", sum/5);
  }
  free(qstr);
  taos_close(taos);
  taos_cleanup();
}

