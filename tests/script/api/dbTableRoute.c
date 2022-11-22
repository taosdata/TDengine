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

// TAOS asynchronous API example
// this example opens multiple tables, insert/retrieve multiple tables
// it is used by TAOS internally for one performance testing
// to compiple: gcc -o asyncdemo asyncdemo.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include "taos.h"

int  rtTables = 20;
char hostName[128];

static void rtExecSQL(TAOS *taos, char *command) {
  int i;
  int32_t   code = -1;

  TAOS_RES *pSql = taos_query(taos, command);
  code = taos_errno(pSql);
  if (code != 0) {
    fprintf(stderr, "Failed to run %s, reason: %s\n", command, taos_errstr(pSql));
    taos_free_result(pSql);
    taos_close(taos);
    taos_cleanup();
    exit(EXIT_FAILURE);
  }

  taos_free_result(pSql);
}

static void rtFetchVgId(TAOS *taos, char *sql, int *vgId) {
  int i;
  int32_t   code = -1;

  TAOS_RES *pSql = taos_query(taos, sql);
  code = taos_errno(pSql);
  if (code != 0) {
    fprintf(stderr, "Failed to run %s, reason: %s\n", sql, taos_errstr(pSql));
    taos_free_result(pSql);
    taos_close(taos);
    taos_cleanup();
    exit(EXIT_FAILURE);
  }

  TAOS_ROW row = taos_fetch_row(pSql);

  *vgId = *(int*)row[0];

  taos_free_result(pSql);
}

void rtError(char* prefix, const char* errMsg) {
  fprintf(stderr, "%s error: %s\n", prefix, errMsg);
}

void rtExit(char* prefix, const char* errMsg) {
  rtError(prefix, errMsg);
  exit(1);
}

int rtPrepare(TAOS   ** p, int prefix, int suffix) {
  char    sql[1024]  = {0};
  int32_t code = 0;
  TAOS   *taos = taos_connect(hostName, "root", "taosdata", NULL, 0);
  if (taos == NULL) rtExit("taos_connect", taos_errstr(NULL));

  strcpy(sql, "drop database if exists db1");
  rtExecSQL(taos, sql);
  
  sprintf(sql, "create database db1 vgroups 10 table_prefix %d table_suffix %d", prefix, suffix);
  rtExecSQL(taos, sql);
  
  strcpy(sql, "use db1");
  rtExecSQL(taos, sql);

  for (int32_t i = 0; i < rtTables; ++i) {
    sprintf(sql, "create table tb%d (ts timestamp, f1 int)", i);
    rtExecSQL(taos, sql);
  }

  *p = taos;

  return 0;
}

int rtGetDbRouteInfo(TAOS   * taos) {
  TAOS_DB_ROUTE_INFO dbInfo;
  int code = taos_get_db_route_info(taos, "db1", &dbInfo);
  if (code) {
    rtExit("taos_get_db_route_info", taos_errstr(NULL));
  }

  printf("db db1 routeVersion:%d hashPrefix:%d hashSuffix:%d hashMethod:%d vgNum %d\n",
     dbInfo.routeVersion, dbInfo.hashPrefix, dbInfo.hashSuffix, dbInfo.hashMethod, dbInfo.vgNum);

  for (int32_t i = 0; i < dbInfo.vgNum; ++i) {
    printf("%dth vg, id:%d hashBegin:%u hashEnd:%u\n", 
        i, dbInfo.vgHash[i].vgId, dbInfo.vgHash[i].hashBegin, dbInfo.vgHash[i].hashEnd);
  }
  
  return 0;
}

int rtGetTableRouteInfo(TAOS   * taos) {
  char table[64] = {0};
  int vgId1 = 0;
  int vgId2 = 0;
  char    sql[1024]  = {0};
  for (int32_t i = 0; i < rtTables; ++i) {
    sprintf(table, "tb%d", i);
    int code = taos_get_table_vgId(taos, "db1", table, &vgId1);
    if (code) {
      rtExit("taos_get_table_vgId", taos_errstr(NULL));
    }
    
    sprintf(sql, "select vgroup_id from information_schema.ins_tables where table_name=\"tb%d\"", i);
    
    rtFetchVgId(taos, sql, &vgId2);
    if (vgId1 != vgId2) {
      fprintf(stderr, "!!!! table tb%d vgId mis-match, vgId(api):%d, vgId(sys):%d\n", i, vgId1, vgId2);
      exit(1);
    } else {
      printf("table tb%d vgId %d\n", i, vgId1);
    }
  }

  return 0;
}

void rtClose(TAOS   * taos) {
  taos_close(taos);
}


int rtRunCase1(void) {
  TAOS *taos = NULL;
  rtPrepare(&taos, 0, 0);
  rtGetDbRouteInfo(taos);
  rtGetTableRouteInfo(taos);
  rtClose(taos);

  return 0;
}

int rtRunCase2(void) {
  TAOS *taos = NULL;
  rtPrepare(&taos, 2, 0);
  rtGetTableRouteInfo(taos);
  rtGetDbRouteInfo(taos);
  rtClose(taos);

  return 0;
}

int main(int argc, char *argv[]) {
  if (argc != 2) {
    printf("usage: %s server-ip\n", argv[0]);
    exit(0);
  }

  srand((unsigned int)time(NULL));
  
  strcpy(hostName, argv[1]);

  rtRunCase1();
  rtRunCase2();

  int32_t l = 5;
  while (l) {
    printf("%d\n", l--);
    sleep(1);
  }

  return 0;
}


