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
 *  alterTableTest.c
 *   - for JIRA: PI-23
 *   - Run the test case in clear TDengine environment with default root passwd 'taosdata'
 *
 * Usage Example: check add column for stable
 *  step 1) Open terminal 1, execute: "./alterTableTest localhost 1 0" to prepare db/stables.
 *  step 2) Open terminal 2 and 3, execute: "./alterTableTest localhost 0 0" to add columns simultaneously.
 *
 *  Check Result: If reproduced, "Invalid value in client" error appears during checking "desc tables ..."
 */

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "taos.h"  // TAOS header file

typedef enum {
  CHECK_ALTER_STABLE_ADD_COL = 0,
  CHECK_ALTER_STABLE_ADD_TAG = 1,
  CHECK_ALTER_STABLE_MODIFY_COL = 2,
  CHECK_ALTER_STABLE_MODIFY_TAG = 3,
  CHECK_ALTER_NTABLE_ADD_COL = 4,
  CHECK_ALTER_NTABLE_MODIFY_COL = 5,
} ENUM_CHECK_ALTER_TYPE;

#define nDup           3
#define USER_LEN       24
#define BUF_LEN        1024
#define DB             "d0"
#define DB_BUFFER      32
#define STB            "stb"
#define NTB            "ntb"
#define CTB            "ctb"
#define COL            "c"
#define STB_NUM        10
#define NTB_NUM        20
#define CTB_NUM        1
#define COL_NUM        505
#define TAG_NUM        127
#define STB_NUM_MODIFY 100  // for modify columns/tags(increase the number if not easy to reproduced)
#define NTB_NUM_MODIFY 500
#define COL_NCHAR_LEN  32

int32_t isDropDb = 0;
int32_t checkType = 0;

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

static void checkAlterStbAddColumn(TAOS *taos, const char *host, char *qstr, int32_t type) {
  // create stb
  if (isDropDb) {
    for (int i = 0; i < STB_NUM; ++i) {
      sprintf(qstr, "CREATE table if not exists %s_%d (ts timestamp, %s_%d NCHAR(32)) tags(t0 nchar(32));", STB, i, COL,
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
    printf("sleep 86400s to wait another terminals (at least 2 terminals) to execute \n");
    sleep(86400);
  }

  int32_t     colNum = type == CHECK_ALTER_STABLE_ADD_COL ? COL_NUM : TAG_NUM;
  const char *colName = type == CHECK_ALTER_STABLE_ADD_COL ? "column" : "tag";

  // alter stb cols
  for (int i = 0; i < STB_NUM; ++i) {
    for (int c = 1; c < colNum; ++c) {
      sprintf(qstr, "alter table %s_%d add %s c_%d NCHAR(%d);", STB, i, colName, c, COL_NCHAR_LEN);
      queryDB(taos, qstr, true);
    }
    sprintf(qstr, "desc %s_%d;", STB, i);
    queryDB(taos, qstr, false);
  }

  // check
  for (int i = 0; i < STB_NUM; ++i) {
    sprintf(qstr, "desc %s_%d;", STB, i);
    queryDB(taos, qstr, false);
  }
}

static void checkAlterStbModifyColumn(TAOS *taos, const char *host, char *qstr, int32_t type) {
  // create stb
  if (isDropDb) {
    for (int i = 0; i < STB_NUM_MODIFY; ++i) {
      sprintf(
          qstr,
          "CREATE table if not exists %s_%d (ts timestamp, c_0 NCHAR(160), c_1 NCHAR(160), c_2 NCHAR(160), c_3 "
          "NCHAR(160),c_4 NCHAR(160),c_5 NCHAR(160),c_6 NCHAR(160),c_7 NCHAR(160),c_8 NCHAR(160),c_9 NCHAR(160),c_10 "
          "NCHAR(160),c_11 NCHAR(160),c_12 NCHAR(160),c_13 NCHAR(160),c_14 NCHAR(160),c_15 NCHAR(160),c_16 "
          "NCHAR(160),c_17 NCHAR(160),c_18 NCHAR(160),c_19 NCHAR(160),c_20 NCHAR(160),c_21 NCHAR(160),c_22 "
          "NCHAR(160),c_23 NCHAR(160),c_24 NCHAR(160),c_25 NCHAR(160),c_26 NCHAR(160),c_27 NCHAR(160),c_28 "
          "NCHAR(160),c_29 NCHAR(160),c_30 NCHAR(160),c_31 NCHAR(160),c_32 NCHAR(160),c_33 NCHAR(160),c_34 "
          "NCHAR(160),c_35 NCHAR(160)) tags(t_0 NCHAR(80), t_1 NCHAR(80), t_2 NCHAR(80), t_3 NCHAR(80),t_4 "
          "NCHAR(80),t_5 NCHAR(80),t_6 NCHAR(80),t_7 NCHAR(80),t_8 NCHAR(80),t_9 NCHAR(80),t_10 NCHAR(80),t_11 "
          "NCHAR(80),t_12 NCHAR(80),t_13 NCHAR(80),t_14 NCHAR(80),t_15 NCHAR(80),t_16 NCHAR(80),t_17 NCHAR(80),t_18 "
          "NCHAR(80),t_19 NCHAR(80),t_20 NCHAR(80),t_21 NCHAR(80),t_22 NCHAR(80),t_23 NCHAR(80),t_24 NCHAR(80),t_25 "
          "NCHAR(80),t_26 NCHAR(80),t_27 NCHAR(80),t_28 NCHAR(80),t_29 NCHAR(80),t_30 NCHAR(80),t_31 NCHAR(80),t_32 "
          "NCHAR(80),t_33 NCHAR(80),t_34 NCHAR(80),t_35 NCHAR(80));",
          STB, i);
      queryDB(taos, qstr, false);
    }
  }

  if (isDropDb) {
    printf("sleep 86400s to wait another terminals (at least 2 terminals) to execute \n");
    sleep(86400);
  }

  int32_t     colLen = type == CHECK_ALTER_STABLE_MODIFY_COL ? 455 : 115;
  const char *colName = type == CHECK_ALTER_STABLE_MODIFY_COL ? "column c_" : "tag t_";

  // alter stb cols
  for (int i = 0; i < STB_NUM_MODIFY; ++i) {
    for (int c = 0; c < 36; ++c) {
      sprintf(qstr, "alter table %s_%d modify %s%d NCHAR(%d);", STB, i, colName, c, colLen);
      queryDB(taos, qstr, true);
      //   usleep(1000);
    }
    sprintf(qstr, "desc %s_%d;", STB, i);
    queryDB(taos, qstr, false);
  }

  // check
  for (int i = 0; i < STB_NUM_MODIFY; ++i) {
    sprintf(qstr, "desc %s_%d;", STB, i);
    queryDB(taos, qstr, false);
  }
}

static void checkAlterNtbAddColumn(TAOS *taos, const char *host, char *qstr) {
  // create ntb
  if (isDropDb) {
    for (int i = 0; i < NTB_NUM; ++i) {
      sprintf(qstr, "CREATE table if not exists %s_%d (ts timestamp, %s_%d NCHAR(32));", NTB, i, COL, 0);
      queryDB(taos, qstr, false);
    }
  }

  if (isDropDb) {
    printf("sleep 86400s to wait another terminals (at least 2 terminals) to execute \n");
    sleep(86400);
  }

  // alter ntb cols
  for (int i = 0; i < NTB_NUM; ++i) {
    for (int c = 1; c < COL_NUM; ++c) {
      sprintf(qstr, "alter table %s_%d add column c_%d NCHAR(%d);", NTB, i, c, COL_NCHAR_LEN);
      queryDB(taos, qstr, true);
    }
    sprintf(qstr, "desc %s_%d;", NTB, i);
    queryDB(taos, qstr, false);
  }

  // check
  for (int i = 0; i < NTB_NUM; ++i) {
    sprintf(qstr, "desc %s_%d;", NTB, i);
    queryDB(taos, qstr, false);
  }
}

static void checkAlterNtbModifyColumn(TAOS *taos, const char *host, char *qstr) {
  // create ntb
  if (isDropDb) {
    for (int i = 0; i < NTB_NUM_MODIFY; ++i) {
      sprintf(
          qstr,
          "CREATE table if not exists %s_%d (ts timestamp, c_0 NCHAR(160), c_1 NCHAR(160), c_2 NCHAR(160), c_3 "
          "NCHAR(160),c_4 NCHAR(160),c_5 NCHAR(160),c_6 NCHAR(160),c_7 NCHAR(160),c_8 NCHAR(160),c_9 NCHAR(160),c_10 "
          "NCHAR(160),c_11 NCHAR(160),c_12 NCHAR(160),c_13 NCHAR(160),c_14 NCHAR(160),c_15 NCHAR(160),c_16 "
          "NCHAR(160),c_17 NCHAR(160),c_18 NCHAR(160),c_19 NCHAR(160),c_20 NCHAR(160),c_21 NCHAR(160),c_22 "
          "NCHAR(160),c_23 NCHAR(160),c_24 NCHAR(160),c_25 NCHAR(160),c_26 NCHAR(160),c_27 NCHAR(160),c_28 "
          "NCHAR(160),c_29 NCHAR(160),c_30 NCHAR(160),c_31 NCHAR(160),c_32 NCHAR(160),c_33 NCHAR(160),c_34 "
          "NCHAR(160),c_35 NCHAR(160));",
          NTB, i);
      queryDB(taos, qstr, false);
    }
  }

  if (isDropDb) {
    printf("sleep 86400s to wait another terminals (at least 2 terminals) to execute \n");
    sleep(86400);
  }

  // alter ntb cols
  for (int i = 0; i < NTB_NUM_MODIFY; ++i) {
    for (int c = 0; c < 36; ++c) {
      sprintf(qstr, "alter table %s_%d modify column c_%d NCHAR(%d);", NTB, i, c, 455);
      queryDB(taos, qstr, true);
    }
    sprintf(qstr, "desc %s_%d;", NTB, i);
    queryDB(taos, qstr, false);
  }

  // check
  for (int i = 0; i < NTB_NUM_MODIFY; ++i) {
    sprintf(qstr, "desc %s_%d;", NTB, i);
    queryDB(taos, qstr, false);
  }
}

int main(int argc, char *argv[]) {
  char qstr[1024];

  // connect to server
  if (argc < 2) {
    printf("please input server-ip \n");  // e.g. localhost
    return 0;
  }

  if (argc < 3) {
    printf("please specify if drop DB to clear env\n");  // 0 not drop, 1 drop
    return 0;
  }

  isDropDb = atoi(argv[2]);

  if (argc < 4) {
    printf("please specify check type\n");  // enum of ENUM_CHECK_ALTER_TYPE
    return 0;
  }

  checkType = atoi(argv[3]);

  TAOS *taos = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to server, reason:%s\n", "null taos" /*taos_errstr(taos)*/);
    exit(1);
  }

  if (isDropDb) {
    sprintf(qstr, "drop database if exists %s", DB);
    queryDB(taos, qstr, false);
    sprintf(qstr, "create database if not exists %s vgroups 2 buffer %d", DB, DB_BUFFER);
    queryDB(taos, qstr, false);
  }
  sprintf(qstr, "use %s", DB);
  queryDB(taos, qstr, false);

  switch (checkType) {
    case CHECK_ALTER_STABLE_ADD_COL:  // reproduced in 3.0.7.1
      checkAlterStbAddColumn(taos, argv[1], qstr, CHECK_ALTER_STABLE_ADD_COL);
      break;
    case CHECK_ALTER_STABLE_ADD_TAG:  // reproduced in 3.0.7.1
      checkAlterStbAddColumn(taos, argv[1], qstr, CHECK_ALTER_STABLE_ADD_TAG);
      break;
    case CHECK_ALTER_STABLE_MODIFY_COL:  // not reproduced in 3.0.7.1 since already checked in mnode
      checkAlterStbModifyColumn(taos, argv[1], qstr, CHECK_ALTER_STABLE_MODIFY_COL);
      break;
    case CHECK_ALTER_STABLE_MODIFY_TAG:  // reproduced in 3.0.7.1
      checkAlterStbModifyColumn(taos, argv[1], qstr, CHECK_ALTER_STABLE_MODIFY_TAG);
      break;
    case CHECK_ALTER_NTABLE_ADD_COL:  // not reproduced in 3.0.7.1
      checkAlterNtbAddColumn(taos, argv[1], qstr);
      break;
    case CHECK_ALTER_NTABLE_MODIFY_COL:  // not reproduced in 3.0.7.1
      checkAlterNtbModifyColumn(taos, argv[1], qstr);
      break;
    default:
      printf("unkown check type:%d\n", checkType);
      break;
  }

  taos_close(taos);
  taos_cleanup();
}