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

#define nDup     1
#define nRoot    10
#define nUser    10
#define USER_LEN 24
#define BUF_LEN  1024

typedef uint16_t VarDataLenT;

#define TSDB_NCHAR_SIZE    sizeof(int32_t)
#define VARSTR_HEADER_SIZE sizeof(VarDataLenT)

#define GET_FLOAT_VAL(x)  (*(float *)(x))
#define GET_DOUBLE_VAL(x) (*(double *)(x))

#define varDataLen(v) ((VarDataLenT *)(v))[0]

void createUsers(TAOS *taos, const char *host, char *qstr);
void passVerTestMulti(const char *host, char *qstr);
void sysInfoTest(TAOS *taos, const char *host, char *qstr);

int   nPassVerNotified = 0;
int nWhiteListVerNotified = 0;
TAOS *taosu[nRoot] = {0};
char  users[nUser][USER_LEN] = {0};

void __taos_notify_cb(void *param, void *ext, int type) {
  switch (type) {
    case TAOS_NOTIFY_PASSVER: {
      ++nPassVerNotified;
      printf("%s:%d type:%d user:%s ver:%d\n", __func__, __LINE__, type, param ? (char *)param : "NULL", *(int *)ext);
      break;
    }
    case TAOS_NOTIFY_WHITELIST_VER: {
      ++nWhiteListVerNotified;
      printf("%s:%d type:%d user:%s ver:%d\n", __func__, __LINE__, type, param ? (char *)param : "NULL", *(int64_t *)ext);
      break;      
    }
    default:
      printf("%s:%d unknown type:%d\n", __func__, __LINE__, type);
      break;
  }
}

static void queryDB(TAOS *taos, char *command) {
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
    taos_free_result(pSql);
    taos_close(taos);
    exit(EXIT_FAILURE);
  } else {
    fprintf(stderr, "success to run: %s\n", command);
  }

  taos_free_result(pSql);
}


int main(int argc, char *argv[]) {
  char qstr[1024];

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
  createUsers(taos, argv[1], qstr);

  taos_close(taos);
  taos_cleanup();
}

void createUsers(TAOS *taos, const char *host, char *qstr) {
  // users
  for (int i = 0; i < nUser; ++i) {
    sprintf(users[i], "user%d", i);
    sprintf(qstr, "CREATE USER %s PASS 'taosdata'", users[i]);
    queryDB(taos, qstr);

    taosu[i] = taos_connect(host, users[i], "taosdata", NULL, 0);
    if (taosu[i] == NULL) {
      printf("failed to connect to server, user:%s, reason:%s\n", users[i], "null taos" /*taos_errstr(taos)*/);
      exit(1);
    }

    int code = taos_set_notify_cb(taosu[i], __taos_notify_cb, users[i], TAOS_NOTIFY_WHITELIST_VER);

    if (code != 0) {
      fprintf(stderr, "failed to run: taos_set_notify_cb for user:%s since %d\n", users[i], code);
    } else {
      fprintf(stderr, "success to run: taos_set_notify_cb for user:%s\n", users[i]);
    }

    // alter pass for users
    sprintf(qstr, "alter user %s add host '%d.%d.%d.%d/24'", users[i], i, i, i, i);
    queryDB(taos, qstr);
  }
}
