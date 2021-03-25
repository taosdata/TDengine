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

#define _GNU_SOURCE
#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE

#include "os.h"
#include "shell.h"
#include "shellCommand.h"
#include "tglobal.h"
#include "tutil.h"

#define SHELL_SQL_LEN 1024
static int32_t tbNum = 0;
static int32_t tbMallocNum = 0;
static char ** tbNames = NULL;
static int32_t checkedNum = 0;
static int32_t errorNum = 0;

typedef struct {
  pthread_t threadID;
  int       threadIndex;
  int       totalThreads;
  void *    taos;
  char *    db;
} ShellThreadObj;

static int32_t shellUseDb(TAOS *con, char *db) {
  if (db == NULL) {
    fprintf(stdout, "no dbname input\n");
    return -1;
  }

  char sql[SHELL_SQL_LEN] = {0};
  snprintf(sql, SHELL_SQL_LEN, "use %s", db);

  TAOS_RES *pSql = taos_query(con, sql);
  int32_t   code = taos_errno(pSql);
  if (code != 0) {
    fprintf(stdout, "failed to execute sql:%s since %s", sql, taos_errstr(pSql));
  }

  taos_free_result(pSql);
  return code;
}

static int32_t shellShowTables(TAOS *con, char *db) {
  char sql[SHELL_SQL_LEN] = {0};
  snprintf(sql, SHELL_SQL_LEN, "show %s.tables", db);

  TAOS_RES *pSql = taos_query(con, sql);
  int32_t   code = taos_errno(pSql);

  if (code != 0) {
    fprintf(stdout, "failed to execute sql:%s since %s\n", sql, taos_errstr(pSql));
  } else {
    TAOS_ROW row;
    while ((row = taos_fetch_row(pSql))) {
      int32_t tbIndex = tbNum++;
      if (tbMallocNum < tbNum) {
        tbMallocNum = (tbMallocNum * 2 + 1);
        tbNames = realloc(tbNames, tbMallocNum * sizeof(char *));
        if (tbNames == NULL) {
          fprintf(stdout, "failed to malloc tablenames, num:%d\n", tbMallocNum);
          code = TSDB_CODE_TSC_OUT_OF_MEMORY;
          break;
        }
      }

      tbNames[tbIndex] = malloc(TSDB_TABLE_NAME_LEN);
      strncpy(tbNames[tbIndex], (const char *)row[0], TSDB_TABLE_NAME_LEN);
      if (tbIndex % 100000 == 0 && tbIndex != 0) {
        fprintf(stdout, "%d tablenames fetched\n", tbIndex);
      }
    }
  }

  taos_free_result(pSql);

  fprintf(stdout, "total %d tablenames fetched, over\n", tbNum);
  return code;
}

static void shellFreeTbnames() {
  for (int32_t i = 0; i < tbNum; ++i) {
    free(tbNames[i]);
  }
  free(tbNames);
}

static void *shellCheckThreadFp(void *arg) {
  ShellThreadObj *pThread = (ShellThreadObj *)arg;

  int32_t interval = tbNum / pThread->totalThreads + 1;
  int32_t start = pThread->threadIndex * interval;
  int32_t end = (pThread->threadIndex + 1) * interval;

  if (end > tbNum) end = tbNum + 1;

  char file[32] = {0};
  snprintf(file, 32, "tb%d.txt", pThread->threadIndex);

  FILE *fp = fopen(file, "w");
  if (!fp) {
    fprintf(stdout, "failed to open %s, reason:%s", file, strerror(errno));
    return NULL;
  }

  char sql[SHELL_SQL_LEN];
  for (int32_t t = start; t < end; ++t) {
    char *tbname = tbNames[t];
    if (tbname == NULL) break;

    snprintf(sql, SHELL_SQL_LEN, "select * from %s limit 1", tbname);

    TAOS_RES *pSql = taos_query(pThread->taos, sql);
    int32_t   code = taos_errno(pSql);
    if (code != 0) {
      int32_t len = snprintf(sql, SHELL_SQL_LEN, "drop table %s.%s;\n", pThread->db, tbname);
      fwrite(sql, 1, len, fp);
      atomic_add_fetch_32(&errorNum, 1);
    }

    int32_t cnum = atomic_add_fetch_32(&checkedNum, 1);
    if (cnum % 5000 == 0 && cnum != 0) {
      fprintf(stdout, "%d tables checked\n", cnum);
    }

    taos_free_result(pSql);
  }

  fsync(fileno(fp));
  fclose(fp);

  return NULL;
}

static void shellRunCheckThreads(TAOS *con, SShellArguments *args) {
  pthread_attr_t  thattr;
  ShellThreadObj *threadObj = (ShellThreadObj *)calloc(args->threadNum, sizeof(ShellThreadObj));
  for (int t = 0; t < args->threadNum; ++t) {
    ShellThreadObj *pThread = threadObj + t;
    pThread->threadIndex = t;
    pThread->totalThreads = args->threadNum;
    pThread->taos = con;
    pThread->db = args->database;

    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&(pThread->threadID), &thattr, shellCheckThreadFp, (void *)pThread) != 0) {
      fprintf(stderr, "ERROR: thread:%d failed to start\n", pThread->threadIndex);
      exit(0);
    }
  }

  for (int t = 0; t < args->threadNum; ++t) {
    pthread_join(threadObj[t].threadID, NULL);
  }

  for (int t = 0; t < args->threadNum; ++t) {
    taos_close(threadObj[t].taos);
  }
  free(threadObj);
}

void shellCheck(TAOS *con, SShellArguments *args) {
  int64_t start = taosGetTimestampMs();

  if (shellUseDb(con, args->database) != 0) {
    shellFreeTbnames();
    return;
  }

  if (shellShowTables(con, args->database) != 0) {
    shellFreeTbnames();
    return;
  }

  fprintf(stdout, "total %d tables will be checked by %d threads\n", tbNum, args->threadNum);
  shellRunCheckThreads(con, args);

  int64_t end = taosGetTimestampMs();
  fprintf(stdout, "total %d tables checked, failed:%d, time spent %.2f seconds\n", checkedNum, errorNum,
          (end - start) / 1000.0);
}