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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taoserror.h"
#include "taos.h"
#include "tulog.h"
#include "tutil.h"
#include "tglobal.h"
#include "hash.h"

#define MAX_RANDOM_POINTS 20000
#define GREEN "\033[1;32m"
#define NC "\033[0m"

#define MAX_DB_NUM 100
void *  con;
char    dbNames[MAX_DB_NUM][48];
int32_t dbNum = 0;
void    parseArgument(int argc, char *argv[]);
void    connDb();
void    getDbNames();
void    printDbNames();
void    queryTables(char *dbName);
void    checkTables(char *dbName);

int main(int argc, char *argv[]) {
  parseArgument(argc, argv);
  taos_init();
  connDb();
  getDbNames();
  printDbNames();
  for (int dbIndex = 0; dbIndex < dbNum; ++dbIndex) {
    queryTables((char*)(dbNames[dbIndex]));
    checkTables((char*)(dbNames[dbIndex]));
  }

  pPrint("all %d database is checked", dbNum);
}

void connDb() {
  con = taos_connect(NULL, "root", "taosdata", NULL, 0);
  if (con == NULL) {
    pError("failed to connect to DB, reason:%s", taos_errstr(con));
    exit(0);
  }
}

void getDbNames() {
  if (dbNum != 0) return;

  char *    qstr = "show databases";
  TAOS_RES *result = taos_query(con, qstr);
  int32_t   code = taos_errno(result);
  if (result == NULL || code != 0) {
    pError("failed to exec sql:%s, code:0x%x reason:%s", qstr, code & 0XFFFF, tstrerror(code));
    exit(0);
  }

  TAOS_ROW row;
  int num_fields = taos_num_fields(result);
  if (num_fields <= 0) return;
  while ((row = taos_fetch_row(result))) {
    char * dbName = (char*)dbNames[dbNum];
    int32_t *length = taos_fetch_lengths(result);
    int len = length[0];
    memcpy(dbName, (char *)row[0], len);
    dbName[len] = 0;
    dbNum++;
  }

  taos_free_result(result);
}

void printDbNames() {
  for (int dbIndex = 0; dbIndex < dbNum; ++dbIndex) {
    pPrint("db:%d %s", dbIndex, dbNames[dbIndex]);
  }
}

void queryTables(char *dbName) {
  char    qstr[1024];
  char    fileName[1024];
  char    ts[35] = {0};
  int32_t precision = 1000;

  sprintf(qstr, "show %s.tables", dbName);
  sprintf(fileName, "%s_tables.txt", dbName); 

  TAOS_RES *result = taos_query(con, qstr);
  int32_t   code = taos_errno(result);
  if (result == NULL || code != 0) {
    pError("failed to exec sql:%s, code:0x%x reason:%s", qstr, code & 0XFFFF, tstrerror(code));
    exit(0);
  }

  FILE *fp = fopen(fileName, "w");
  if (!fp) return;

  TAOS_ROW row;
  int32_t  rows = 0;
  while ((row = taos_fetch_row(result))) {
    char     tableName[256] = {0};
    int32_t *length = taos_fetch_lengths(result);
    int      len = length[0];
    memcpy(tableName, (char *)row[0], len);
    tableName[len] = 0;

    int64_t    t = *((int64_t *)row[1]);
    time_t     tt = t / 1000;
    struct tm *ptm = localtime(&tt);
    int32_t    tl = (int32_t)strftime(ts, 35, "%Y-%m-%d %H:%M:%S", ptm);
    snprintf(ts + tl, 5, ".%03ld", t % precision);

    // fprintf(fp, "%s %s\n", tableName, ts);
    fprintf(fp, "%s.%s\n", dbName, tableName);
    rows++;
  }

  taos_free_result(result);
  fclose(fp);
  pPrint("db:%s has %d tables, write to %s", dbName, rows, fileName);
}

void checkTables(char *dbName) {
  char    qstr[1024];
  char    fileName1[1024];
  char    fileName2[1024];
  
  sprintf(qstr, "show %s.tables", dbName);
  sprintf(fileName1, "%s_tables.txt", dbName);
  sprintf(fileName2, "%s_count.txt", dbName);

  FILE *fp1 = fopen(fileName1, "r");
  if (!fp1) return;

  FILE *fp2 = fopen(fileName2, "w");
  if (!fp2) return;

  int32_t successRows = 0;
  int32_t failedRows = 0;
  char tbName[256];
  while (!feof(fp1)) {
    int size = fscanf(fp1, "%s", tbName);
    if (size <= 0) {
      break;
    }

    sprintf(qstr, "select count(*) from %s", tbName);
    TAOS_RES *result = taos_query(con, qstr);
    int32_t   code = taos_errno(result);
    if (result == NULL || code != 0) {
      pError("failed to exec sql:%s, code:0x%x reason:%s", qstr, code & 0XFFFF, tstrerror(code));
      fprintf(fp2, "%s failed to exec sql:%s, code:0x%x reason:%s", tbName, qstr, code & 0XFFFF, tstrerror(code));
      taos_free_result(result);
      failedRows++;
      continue;
    }

    TAOS_ROW row;
    int64_t  count = 0;
    while ((row = taos_fetch_row(result))) {
      count = *((int64_t *)row[0]);
    }
    fprintf(fp2, "%s %" PRId64 "\n", tbName, count);

    successRows++;
    if (successRows % 1000 == 0) {
      pPrint("query %d tables", successRows);
    }
    taos_free_result(result);
  }

  fclose(fp1);
  fclose(fp2);
  pPrint("db:%s query tables, success:%d failed:%d write to %s", dbName, successRows, failedRows, fileName2);
}

void printHelp() {
  char indent[10] = "        ";
  printf("Used to checkTables\n");

  printf("%s%s\n", indent, "-c");
  printf("%s%s%s%s\n", indent, indent, "Configuration directory, default is ", configDir);
  printf("%s%s\n", indent, "-d");
  printf("%s%s%s%s\n", indent, indent, "The name of the database to be checked, default is ", "all");

  exit(EXIT_SUCCESS);
}

void parseArgument(int argc, char *argv[]) {
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printHelp();
      exit(0);
    } else if (strcmp(argv[i], "-d") == 0) {
      strcpy(dbNames[0], argv[++i]);
      dbNum++;
    } else if (strcmp(argv[i], "-c") == 0) {
      strcpy(configDir, argv[++i]);
    } else {
    }
  }

  pPrint("%s configDir:%s %s", GREEN, configDir, NC);
  pPrint("%s start to checkTables %s", GREEN, NC);
}
