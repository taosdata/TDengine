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

/**
 *  passwdTest.c
 *   - Run the test case in clear TDengine environment with default root passwd 'taosdata'
 */

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "taos.h"  // TAOS header file

#define nDup      3
#define USER_LEN  24
#define BUF_LEN   1024
#define DB        "d0"
#define DB_BUFFER 32
#define STB       "stb"
#define CTB       "ctb"
#define COL       "c"

#if 0
#define STB_NUM 125
#define CTB_NUM 960
#define COL_NUM 52
#define COL_NCHAR_LEN 320
#else
#define STB_NUM       5
#define CTB_NUM       1
#define COL_NUM       505
#define COL_NCHAR_LEN 32
#endif
typedef uint16_t VarDataLenT;

int32_t isDropDb = 0;

#define TSDB_NCHAR_SIZE    sizeof(int32_t)
#define VARSTR_HEADER_SIZE sizeof(VarDataLenT)

#define GET_FLOAT_VAL(x)  (*(float *)(x))
#define GET_DOUBLE_VAL(x) (*(double *)(x))

#define varDataLen(v) ((VarDataLenT *)(v))[0]

static int32_t queryDB(TAOS *taos, char *command, bool skipError) {
  int       i;
  TAOS_RES *pSql = NULL;
  int32_t   code = -1;

  for (i = 0; i < nDup; ++i) {
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
    fprintf(stderr, "failed to run: %s, reason: %s\n", command, taos_errstr(pSql));
    if (!skipError) {
      taos_free_result(pSql);
      taos_close(taos);
      exit(EXIT_FAILURE);
    }
  } else {
    fprintf(stderr, "success to run: %s\n", command);
  }

  taos_free_result(pSql);
}

static void createDatabaseAlterStbColumns(TAOS *taos, const char *host, char *qstr, int32_t addColMode) {
  if (isDropDb) {
    sprintf(qstr, "drop database if exists %s", DB);
    queryDB(taos, qstr, false);
    sprintf(qstr, "create database if not exists %s vgroups 2 buffer %d", DB, DB_BUFFER);
    queryDB(taos, qstr, false);
  }
  sprintf(qstr, "use %s", DB);
  queryDB(taos, qstr, false);

  // create stb
  if (isDropDb) {
    for (int i = 0; i < STB_NUM; ++i) {
      sprintf(qstr, "CREATE table if not exists %s_%d (ts timestamp, %s_%d NCHAR(32)) tags(t0 nchar(16));", STB, i, COL,
              0);
      queryDB(taos, qstr, false);
      // create ctb
      for (int j = 0; j < CTB_NUM; ++j) {
        sprintf(qstr, "CREATE table %s_%d_%s_%d using %s_%d tags('%d_%d');", STB, i, CTB, j, STB, i, i, j);
        queryDB(taos, qstr, false);
      }
    }
  }

  if (isDropDb) {
    printf("sleep 86400s to wait other terminal\n");
    sleep(86400);
  }

  // alter stb cols
  if (addColMode == 0) {
    for (int i = 0; i < STB_NUM; ++i) {
      for (int c = 1; c < COL_NUM; ++c) {
        sprintf(qstr, "alter table %s_%d add column c_%d NCHAR(%d);", STB, i, c, COL_NCHAR_LEN);
        queryDB(taos, qstr, true);
      }
      sprintf(qstr, "desc %s_%d;", STB, i);
      queryDB(taos, qstr, false);
    }
  } else if (addColMode == 1) {
    for (int c = 1; c < COL_NUM; ++c) {
      for (int i = 0; i < STB_NUM; ++i) {
        sprintf(qstr, "alter table %s_%d add column c_%d NCHAR(%d);", STB, i, c, COL_NCHAR_LEN);
        queryDB(taos, qstr, true);
      }
    }
  }

  // check
  for (int i = 0; i < STB_NUM; ++i) {
    sprintf(qstr, "desc %s_%d;", STB, i);
    queryDB(taos, qstr, false);
  }
}

int main(int argc, char *argv[]) {
  char qstr[1024];

  // connect to server
  if (argc < 2) {
    printf("please input server-ip \n");
    return 0;
  }

  if (argc < 3) {
    printf("please specify if drop DB\n");
    return 0;
  }

  isDropDb = atoi(argv[2]);

  TAOS *taos = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to server, reason:%s\n", "null taos" /*taos_errstr(taos)*/);
    exit(1);
  }
  createDatabaseAlterStbColumns(taos, argv[1], qstr, 0);

  taos_close(taos);
  taos_cleanup();
}