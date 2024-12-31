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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "taos.h"
#include "types.h"
#include "tlog.h"


#define CHARSET "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
#define VARSTRLEN 51200 // 50 * 1024
#define SQLLEN (VARSTRLEN + 1024)
//#define N 10485 // (512M / VARSTRLEN)
//#define N 5243 // (256M / VARSTRLEN)
#define N 2621 // (128M / VARSTRLEN)
//#define N 20970 // (1G / VARSTRLEN)
//#define N 83880 // (4G / VARSTRLEN)
//#define N 1745 // (85M / VARSTRLEN)

const char *exe_name;

char* generate_random_string(int length) {

    if (length <= 0) {
        return NULL;
    }

    char* random_string = (char *)taosMemoryMalloc((length + 1) * sizeof(char));
    if (random_string == NULL) {
        return NULL;
    }

    taosSeedRand(taosSafeRand());

    for (int i = 0; i < length; i++) {
        int index = taosRand() % (sizeof(CHARSET) - 1);
        random_string[i] = CHARSET[index];
    }
    random_string[length] = '\0';

    return random_string;
}

void blob_sql_test(int nrows) {
  char *varstr;
  char sql[SQLLEN] = {0};

  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "drop database if exists blob_db");
  taos_free_result(pRes);

  pRes = taos_query(taos, "create database if not exists blob_db vgroups 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use blob_db");
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  ASSERT(code == 0);

  pRes = taos_query(taos, "create stable stb (ts timestamp, vb varbinary(65517) encode 'disabled' compress 'disabled') tags (a int, b varchar(3))");
  taos_free_result(pRes);

  nrows <= 0 ? N : nrows;
  for (int i = 0; i < nrows; i++) {
    varstr = generate_random_string(VARSTRLEN);
    sprintf(sql, "insert into tb6 using stb tags (6, 'ABC') values (now + 4s, '%s')", varstr);
    pRes = taos_query(taos, sql);
    taos_free_result(pRes);
    taosMemoryFree(varstr);
  }

  pRes = taos_query(taos, "select count(*) from tb6");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  taos_close(taos);
}

void usage() {
	printf("\nusage: \n");
	printf("%s <nrows> \n", exe_name);
	printf("\n");
}

int main(int argc, char *argv[]) {
  int ret = 0;

  exe_name = argv[0];

  if (argc != 2 || (atoi(argv[1]) < 0)) {
    usage();
    return -1;
  }
  
  blob_sql_test(atoi(argv[1]));
  return ret;
}
