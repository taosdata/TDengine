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
#include "tlog.h"
#include "ttimer.h"
#include "tutil.h"

void taos_error(TAOS *taos);
void* taos_execute(void *param);

typedef struct {
  pthread_t pid;
  int       index;
} ThreadObj;

int threadNum = 1;
int rowNum = 1000;
int replica = 1;

int main(int argc, char *argv[]) {
  if (argc == 1) {
    printf("usage: %s rowNum threadNum replica configDir\n", argv[0]);
	printf("default rowNum %d\n", rowNum);
	printf("default threadNum %d\n", threadNum);
	printf("default replica %d\n", replica);
    exit(0);
  }

  // a simple way to parse input parameters
  if (argc >= 2) rowNum = atoi(argv[1]);
  if (argc >= 3) threadNum = atoi(argv[2]);
  if (argc >= 4) replica = atoi(argv[3]);
  if (argc >= 5) strcpy(configDir, argv[4]);

  printf("rowNum:%d threadNum:%d replica:%d\n", threadNum, rowNum, replica);

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

  void *taos = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
  if (taos == NULL) taos_error(taos);

  char sql[1024] = {0};
  sprintf(sql, "create database if not exists db replica %d", replica);
  taos_query(taos, sql);

  sprintf(sql, "create table if not exists db.t%d (ts timestamp, i int, j float, k double)", pThread->index);
  taos_query(taos, sql);

  int64_t timestamp = 1530374400000L;

  sprintf(sql, "insert into db.t%d values(%ld, %d, %d, %d)", pThread->index, timestamp, 0, 0, 0);
  int code = taos_query(taos, sql);
  if (code != 0) printf("error code:%d, sql:%s\n", code, sql);
  int affectrows = taos_affected_rows(taos);
  if (affectrows != 1) printf("affect rows:%d, sql:%s\n", affectrows, sql);

  timestamp -= 1000;

  int total_affect_rows = affectrows;

  for (int i = 1; i < rowNum; ++i) {
    sprintf(sql, "import into db.t%d values(%ld, %d, %d, %d)", pThread->index, timestamp, i, i, i);
    code = taos_query(taos, sql);
    if (code != 0) printf("error code:%d, sql:%s\n", code, sql);
    int affectrows = taos_affected_rows(taos);
    if (affectrows != 1) printf("affect rows:%d, sql:%s\n", affectrows, sql);

    total_affect_rows += affectrows;

    timestamp -= 1000;
  }

  printf("thread:%d run finished total_affect_rows:%d\n", pThread->index, total_affect_rows);

  return NULL;
}
