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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <taos.h>  // TAOS header file

void taosMsleep(int mseconds);

int main(int argc, char *argv[]) {
  TAOS *    taos;
  char      qstr[1024];
  TAOS_RES *result;

  // connect to server
  if (argc < 2) {
    printf("please input server-ip \n");
    return 0;
  }

  // init TAOS
  taos_init();

  taos = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to server, reason:%s\n", taos_errstr(taos));
    exit(1);
  }
  printf("success to connect to server\n");
  

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
    sprintf(qstr, "insert into m1 values (now+%ds, %d)", i, i * 10);
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

  result = taos_use_result(taos);

  if (result == NULL) {
    printf("failed to get result, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

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
