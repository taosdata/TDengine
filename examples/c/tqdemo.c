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
// to compile: gcc -o tqdemo tqdemo.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>  // TAOS header file

void print_results(TAOS *taos, char *qstr);

int main(int argc, char *argv[]) {
  char qstr[1024];
  TAOS_RES *pSql = NULL;

  // connect to server
  if (argc < 2) {
    printf("please input server-ip \n");
    return 0;
  }

  TAOS *taos = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to server, reason:%s\n", "null taos" /*taos_errstr(taos)*/);
    exit(1);
  }
  
  TAOS_RES* res;
  // create topic
  res = taos_query(taos, "create topic tq_test partitions 10");
  taos_free_result(res);

  res = taos_query(taos, "use tq_test");
  taos_free_result(res);

  print_results(taos, "show stables");
  print_results(taos, "show tables");

  // insert data
  for (int i = 0; i < 10; i++) {
    char *sql = "insert into p%d values(now + %ds, %d, 'test')";
    sprintf(qstr, sql, i, i, i);
    res = taos_query(taos, qstr);
    taos_free_result(res);
  }

  // query data
  print_results(taos, "select * from tq_test.ps");

  taos_close(taos);
  taos_cleanup();
}


void print_results(TAOS *taos, char *qstr) {
  TAOS_RES* res;
  res = taos_query(taos, qstr);
  if (res == NULL || taos_errno(res) != 0) {
    printf("failed to select, reason:%s\n", taos_errstr(res));
    taos_free_result(res);
    exit(1);
  }

  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_field_count(res);
  TAOS_FIELD *fields = taos_fetch_fields(res);

  printf("num_fields = %d\n", num_fields);
  printf("%s result:\n", qstr);
  // fetch the records row by row
  while ((row = taos_fetch_row(res))) {
    char temp[1024] = {0};
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("%s\n", temp);
  }
  taos_free_result(res);
}
