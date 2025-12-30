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
#include "taos.h"  // TAOS header file

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
    exit(EXIT_FAILURE);
  }

  taos_free_result(pSql);
}

void Test(TAOS *taos, char *qstr);

int main(int argc, char *argv[]) {
  char      qstr[1024];

  // connect to server
  if (argc < 2) {
    printf("please input server-ip \n");
    return 0;
  }

  TAOS *taos = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to server, reason:%s\n", "null taos"/*taos_errstr(taos)*/);
    exit(1);
  }
  Test(taos, qstr);
  taos_close(taos);
  taos_cleanup();
}
void Test(TAOS *taos, char *qstr)  {
  queryDB(taos, "drop database if exists demo");
  queryDB(taos, "create database demo vgroups 1 minrows 10");
  TAOS_RES *result;
  queryDB(taos, "use demo");

  queryDB(taos, "create table m1 (ts timestamp, ti tinyint, si smallint, i int, bi bigint, f float, d double, b binary(10))");
  printf("success to create table\n");

  int i = 0;
  for (int32_t n = 0; n < 10; ++n) {
    for (i = 0; i < 10; ++i) {
      int32_t v = n * 10 + i;
      sprintf(qstr, "insert into m1 values (%" PRId64 ", %d, %d, %d, %d, %f, %lf, '%s')", (uint64_t)1546300800000, v, v, v, v*10000000, v*1.0, v*2.0, "hello");
      printf("qstr: %s\n", qstr);
      
      TAOS_RES *result1 = taos_query(taos, qstr);
      if (result1 == NULL || taos_errno(result1) != 0) {
        printf("failed to insert row, reason:%s\n", taos_errstr(result1));    
        taos_free_result(result1);
        exit(1);
      } else {
        printf("insert row: %i\n", v);
      }
      taos_free_result(result1);

      strcpy(qstr, "flush database demo");
      result1 = taos_query(taos, qstr);
      if (result1 == NULL || taos_errno(result1) != 0) {
        printf("failed to fluash database, reason:%s\n", taos_errstr(result1));    
        taos_free_result(result1);
        exit(1);
      }
      taos_free_result(result1);    
    }
  }

  // query the records
  sprintf(qstr, "SELECT * FROM m1 order by ts desc");
  result = taos_query(taos, qstr);
  if (result == NULL || taos_errno(result) != 0) {
    printf("failed to select, reason:%s\n", taos_errstr(result));    
    taos_free_result(result);
    exit(1);
  }

  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_field_count(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  printf("num_fields = %d\n", num_fields);
  printf("select * from table order by ts desc, result:\n");
  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    char temp[1024] = {0};
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("%s\n", temp);
  }

  taos_free_result(result);
}

