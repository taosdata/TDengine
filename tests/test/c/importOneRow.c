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
#include "taos.h"
#include "tulog.h"
#include "ttimer.h"
#include "tutil.h"
#include "tglobal.h"

#define MAX_RANDOM_POINTS 20000
#define GREEN "\033[1;32m"
#define NC "\033[0m"

void taos_error(TAOS *taos);
void* taos_execute(void *param);

typedef struct {
  pthread_t pid;
  int       index;
} ThreadObj;

int threadNum = 1;
int rowNum = 1000;
int replica = 1;

void printHelp() {
  char indent[10] = "        ";
  printf("Used to test the performance of TDengine\n After writing one row of data to all tables, write the next row\n");

  printf("%s%s\n", indent, "-r");
  printf("%s%s%s%d\n", indent, indent, "Number of records to write table, default is ", rowNum);
  printf("%s%s\n", indent, "-t");
  printf("%s%s%s%d\n", indent, indent, "Number of threads to be used, default is ", threadNum);
  printf("%s%s\n", indent, "-replica");
  printf("%s%s%s%d\n", indent, indent, "Database parameters replica, default is ", replica);

  exit(EXIT_SUCCESS);
}

void shellParseArgument(int argc, char *argv[]) {
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printHelp();
      exit(0);
    } else if (strcmp(argv[i], "-r") == 0) {
      rowNum = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-t") == 0) {
      threadNum = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-replica") == 0) {
      replica = atoi(argv[++i]);
    } else {
    }
  }

  pPrint("%s rowNum:%d %s", GREEN, rowNum, NC);
  pPrint("%s threadNum:%d %s", GREEN, threadNum, NC);
  pPrint("%s replica:%d %s", GREEN, replica, NC);
}

int main(int argc, char *argv[]) {
  shellParseArgument(argc, argv);

  taos_init();

  ThreadObj *threads = calloc(threadNum, sizeof(ThreadObj));
  for (int i = 0; i < threadNum; ++i) {
    ThreadObj *    pthread = threads + i;
    pthread_attr_t thattr;
    pthread->index = i;
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&pthread->pid, &thattr, taos_execute, pthread);
  }

  for (int i = 0; i < threadNum; i++) {
    pthread_join(threads[i].pid, NULL);
  }

  printf("all finished\n");

  return 0;
}

void taos_error(TAOS *con) {
  fprintf(stderr, "TDengine error: %s\n", taos_errstr(con));
  taos_close(con);
  exit(1);
}

void* taos_execute(void *param) {
  ThreadObj *pThread = (ThreadObj *)param;

  char     fqdn[TSDB_FQDN_LEN];
  uint16_t port;

  taosGetFqdnPortFromEp(tsFirst, fqdn, &port);

  void *taos = taos_connect(fqdn, "root", "taosdata", NULL, port);
  if (taos == NULL) taos_error(taos);

  char sql[1024] = {0};
  sprintf(sql, "create database if not exists db replica %d", replica);
  taos_query(taos, sql);

  sprintf(sql, "create table if not exists db.t%d (ts timestamp, i int, j float, k double)", pThread->index);
  taos_query(taos, sql);

  int64_t timestamp = 1530374400000L;

  sprintf(sql, "insert into db.t%d values(%ld, %d, %d, %d)", pThread->index, timestamp, 0, 0, 0);
  TAOS_RES *pSql = taos_query(taos, sql);
  int code = taos_errno(pSql);
  if (code != 0) 
  {
    printf("error code:%d, sql:%s\n", code, sql);
    taos_free_result(pSql);
    taos_close(taos);
    return NULL;
  }
  int affectrows = taos_affected_rows(taos);
  if (affectrows != 1) 
  {
    printf("affect rows:%d, sql:%s\n", affectrows, sql);
    taos_free_result(pSql);
    taos_close(taos);
    return NULL;
  }
  taos_free_result(pSql);
  pSql = NULL;


  timestamp -= 1000;

  int total_affect_rows = affectrows;

  for (int i = 1; i < rowNum; ++i) {
    sprintf(sql, "import into db.t%d values(%ld, %d, %d, %d)", pThread->index, timestamp, i, i, i);

    pSql = taos_query(taos, sql);
    code = taos_errno(pSql);
    if (code != 0) 
    {
      printf("error code:%d, sql:%s\n", code, sql);
      taos_free_result(pSql);
      pSql = NULL;
      taos_close(taos);
      return NULL;
    }
    int affectrows = taos_affected_rows(taos);
    if (affectrows != 1) {
      printf("affect rows:%d, sql:%s\n", affectrows, sql);
      taos_free_result(pSql);
      pSql = NULL;
      taos_close(taos);
    }

    total_affect_rows += affectrows;
    taos_free_result(pSql);
    pSql = NULL;

    timestamp -= 1000;
  }

  printf("thread:%d run finished total_affect_rows:%d\n", pThread->index, total_affect_rows);
  taos_close(taos);

  return NULL;
}
