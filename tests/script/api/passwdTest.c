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
#include <unistd.h>
#include "taos.h"  // TAOS header file

#define nRepetition 1
#define nTaos       10

void Test(TAOS *taos, char *qstr);
void passVerTestMulti(const char *host, char *qstr);

int nPassVerNotifiedMulti = 0;

void __taos_notify_cb(void *param, void *ext, int type) {
  switch (type) {
    case TAOS_NOTIFY_PASSVER: {
      ++nPassVerNotifiedMulti;
      printf("%s:%d type:%d user:%s ver:%d\n", __func__, __LINE__, type, param ? (char *)param : "NULL", *(int *)ext);
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

  for (i = 0; i < nRepetition; ++i) {
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

  passVerTestMulti(argv[1], qstr);

  taos_close(taos);
  taos_cleanup();
}

void passVerTestMulti(const char *host, char *qstr) {
  TAOS *taos[nTaos] = {0};
  char *userName = calloc(1, 24);
  strcpy(userName, "root");

  for (int i = 0; i < nTaos; ++i) {
    taos[i] = taos_connect(host, "root", "taosdata", NULL, 0);
    if (taos[i] == NULL) {
      printf("failed to connect to server, reason:%s\n", "null taos" /*taos_errstr(taos)*/);
      exit(1);
    }

    int code = taos_set_notify_cb(taos[i], __taos_notify_cb, userName, TAOS_NOTIFY_PASSVER);

    if (code != 0) {
      fprintf(stderr, "failed to run: taos_set_notify_cb since %d\n", code);
    } else {
      fprintf(stderr, "success to run: taos_set_notify_cb\n");
    }
  }

  queryDB(taos[0], "create database if not exists demo1 vgroups 1 minrows 10");
  queryDB(taos[0], "create database if not exists demo2 vgroups 1 minrows 10");
  queryDB(taos[0], "create database if not exists demo3 vgroups 1 minrows 10");

  queryDB(taos[0], "create table demo1.stb (ts timestamp, c1 int) tags(t1 int)");
  queryDB(taos[0], "create table demo2.stb (ts timestamp, c1 int) tags(t1 int)");
  queryDB(taos[0], "create table demo3.stb (ts timestamp, c1 int) tags(t1 int)");

  strcpy(qstr, "alter user root pass 'taos'");
  queryDB(taos[0], qstr);

  for (int i = 0; i < 10; ++i) {
    if (nPassVerNotifiedMulti >= nTaos) break;
    sleep(1);
  }

  if (nPassVerNotifiedMulti >= nTaos) {
    fprintf(stderr, "success to get passVer notification\n");
  } else {
    fprintf(stderr, "failed to get passVer notification\n");
  }

  // sleep(1000);

  free(userName);
}