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


int     points = 5;
int     numOfTables = 3;
int     tablesInsertProcessed = 0;
int     tablesSelectProcessed = 0;
int64_t st, et;

char hostName[128];
char dbName[128];
char tbName[128];
int32_t runTimes = 10;

typedef struct {
  int       id;
  TAOS     *taos;
  char      name[16];
  time_t    timeStamp;
  int       value;
  int       rowsInserted;
  int       rowsTried;
  int       rowsRetrieved;
} STable;

typedef struct SSP_CB_PARAM {
  TAOS    *taos;
  bool     fetch;
  bool     free;
  int32_t *end;
} SSP_CB_PARAM;

#define CASE_ENTER() do { printf("enter case %s\n", __FUNCTION__); } while (0)
#define CASE_LEAVE() do { printf("leave case %s, runTimes %d\n", __FUNCTION__, runTimes); } while (0)

static void sqExecSQL(TAOS *taos, char *command) {
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

static void sqExecSQLE(TAOS *taos, char *command) {
  int i;
  int32_t   code = -1;

  TAOS_RES *pSql = taos_query(taos, command);

  taos_free_result(pSql);
}

void sqError(char* prefix, const char* errMsg) {
  fprintf(stderr, "%s error: %s\n", prefix, errMsg);
}

void sqExit(char* prefix, const char* errMsg) {
  sqError(prefix, errMsg);
  exit(1);
}

void sqStopFetchCb(void *param, TAOS_RES *pRes, int numOfRows) {
  SSP_CB_PARAM *qParam = (SSP_CB_PARAM *)param;
  taos_stop_query(pRes);
  taos_free_result(pRes);

  *qParam->end = 1;
}

void sqStopQueryCb(void *param, TAOS_RES *pRes, int code) {
  SSP_CB_PARAM *qParam = (SSP_CB_PARAM *)param;
  if (code == 0 && pRes) {
    if (qParam->fetch) {
      taos_fetch_rows_a(pRes, sqStopFetchCb, param);
    } else {
      taos_stop_query(pRes);
      taos_free_result(pRes);
      *qParam->end = 1;
    }
  } else {
    sqExit("select", taos_errstr(pRes));
  }
}

void sqFreeFetchCb(void *param, TAOS_RES *pRes, int numOfRows) {
  SSP_CB_PARAM *qParam = (SSP_CB_PARAM *)param;
  taos_free_result(pRes);

  *qParam->end = 1;
}

void sqFreeQueryCb(void *param, TAOS_RES *pRes, int code) {
  SSP_CB_PARAM *qParam = (SSP_CB_PARAM *)param;
  if (code == 0 && pRes) {
    if (qParam->fetch) {
      taos_fetch_rows_a(pRes, sqFreeFetchCb, param);
    } else {
      taos_free_result(pRes);
      *qParam->end = 1;
    }
  } else {
    sqExit("select", taos_errstr(pRes));
  }
}


void sqCloseFetchCb(void *param, TAOS_RES *pRes, int numOfRows) {
  SSP_CB_PARAM *qParam = (SSP_CB_PARAM *)param;
  taos_close(qParam->taos);

  *qParam->end = 1;
  
  taos_free_result(pRes);
}

void sqCloseQueryCb(void *param, TAOS_RES *pRes, int code) {
  SSP_CB_PARAM *qParam = (SSP_CB_PARAM *)param;
  if (code == 0 && pRes) {
    if (qParam->fetch) {
      taos_fetch_rows_a(pRes, sqCloseFetchCb, param);
    } else {
      taos_close(qParam->taos);
      *qParam->end = 1;
      
      taos_free_result(pRes);
    }
  } else {
    sqExit("select", taos_errstr(pRes));
  }
}

void sqKillFetchCb(void *param, TAOS_RES *pRes, int numOfRows) {
  SSP_CB_PARAM *qParam = (SSP_CB_PARAM *)param;
  taos_kill_query(qParam->taos);

  *qParam->end = 1;
}

void sqKillQueryCb(void *param, TAOS_RES *pRes, int code) {
  SSP_CB_PARAM *qParam = (SSP_CB_PARAM *)param;
  if (code == 0 && pRes) {
    if (qParam->fetch) {
      taos_fetch_rows_a(pRes, sqKillFetchCb, param);
    } else {
      taos_kill_query(qParam->taos);
      *qParam->end = 1;
    }
  } else {
    sqExit("select", taos_errstr(pRes));
  }
}

void sqAsyncFetchCb(void *param, TAOS_RES *pRes, int numOfRows) {
  SSP_CB_PARAM *qParam = (SSP_CB_PARAM *)param;
  if (numOfRows > 0) {
    taos_fetch_rows_a(pRes, sqAsyncFetchCb, param);
  } else {
    *qParam->end = 1;
    if (qParam->free) {
      taos_free_result(pRes);
    }
  }
}


void sqAsyncQueryCb(void *param, TAOS_RES *pRes, int code) {
  SSP_CB_PARAM *qParam = (SSP_CB_PARAM *)param;
  if (code == 0 && pRes) {
    if (qParam->fetch) {
      taos_fetch_rows_a(pRes, sqAsyncFetchCb, param);
    } else {
      if (qParam->free) {
        taos_free_result(pRes);
      }
      *qParam->end = 1;
    }
  } else {
    sqError("select", taos_errstr(pRes));
    *qParam->end = 1;
    taos_free_result(pRes);
  }
}


int sqStopSyncQuery(bool fetch) {
  CASE_ENTER();  
  for (int32_t i = 0; i < runTimes; ++i) {
    char    sql[1024]  = {0};
    int32_t code = 0;
    TAOS   *taos = taos_connect(hostName, "root", "taosdata", NULL, 0);
    if (taos == NULL) sqExit("taos_connect", taos_errstr(NULL));

    sprintf(sql, "reset query cache");
    sqExecSQL(taos, sql);

    sprintf(sql, "use %s", dbName);
    sqExecSQL(taos, sql);

    sprintf(sql, "select * from %s", tbName);
    TAOS_RES* pRes = taos_query(taos, sql);
    code = taos_errno(pRes);
    if (code) {
      sqExit("taos_query", taos_errstr(pRes));
    }

    if (fetch) {
      taos_fetch_row(pRes);
    }

    taos_stop_query(pRes);
    taos_free_result(pRes);
    
    taos_close(taos);
  }
  CASE_LEAVE();  
}

int sqStopAsyncQuery(bool fetch) {
  CASE_ENTER();  
  for (int32_t i = 0; i < runTimes; ++i) {
    char    sql[1024]  = {0};
    int32_t code = 0;
    TAOS   *taos = taos_connect(hostName, "root", "taosdata", NULL, 0);
    if (taos == NULL) sqExit("taos_connect", taos_errstr(NULL));

    sprintf(sql, "reset query cache");
    sqExecSQL(taos, sql);

    sprintf(sql, "use %s", dbName);
    sqExecSQL(taos, sql);

    sprintf(sql, "select * from %s", tbName);

    int32_t qEnd = 0;
    SSP_CB_PARAM param = {0};
    param.fetch = fetch;
    param.end = &qEnd;
    taos_query_a(taos, sql, sqStopQueryCb, &param);
    while (0 == qEnd) {
      usleep(5000);
    }
    
    taos_close(taos);
  }
  CASE_LEAVE();  
}

int sqFreeSyncQuery(bool fetch) {
  CASE_ENTER();  
  for (int32_t i = 0; i < runTimes; ++i) {
    char    sql[1024]  = {0};
    int32_t code = 0;
    TAOS   *taos = taos_connect(hostName, "root", "taosdata", NULL, 0);
    if (taos == NULL) sqExit("taos_connect", taos_errstr(NULL));

    sprintf(sql, "reset query cache");
    sqExecSQL(taos, sql);

    sprintf(sql, "use %s", dbName);
    sqExecSQL(taos, sql);

    sprintf(sql, "select * from %s", tbName);
    TAOS_RES* pRes = taos_query(taos, sql);
    code = taos_errno(pRes);
    if (code) {
      sqExit("taos_query", taos_errstr(pRes));
    }

    if (fetch) {
      taos_fetch_row(pRes);
    }
    
    taos_free_result(pRes);
    taos_close(taos);
  }
  CASE_LEAVE();  
}

int sqFreeAsyncQuery(bool fetch) {
  CASE_ENTER();  
  for (int32_t i = 0; i < runTimes; ++i) {
    char    sql[1024]  = {0};
    int32_t code = 0;
    TAOS   *taos = taos_connect(hostName, "root", "taosdata", NULL, 0);
    if (taos == NULL) sqExit("taos_connect", taos_errstr(NULL));

    sprintf(sql, "reset query cache");
    sqExecSQL(taos, sql);

    sprintf(sql, "use %s", dbName);
    sqExecSQL(taos, sql);

    sprintf(sql, "select * from %s", tbName);

    int32_t qEnd = 0;
    SSP_CB_PARAM param = {0};
    param.fetch = fetch;
    param.end = &qEnd;
    taos_query_a(taos, sql, sqFreeQueryCb, &param);
    while (0 == qEnd) {
      usleep(5000);
    }
    
    taos_close(taos);
  }
  CASE_LEAVE();  
}

int sqCloseSyncQuery(bool fetch) {
  CASE_ENTER();  
  for (int32_t i = 0; i < runTimes; ++i) {
    char    sql[1024]  = {0};
    int32_t code = 0;
    TAOS   *taos = taos_connect(hostName, "root", "taosdata", NULL, 0);
    if (taos == NULL) sqExit("taos_connect", taos_errstr(NULL));

    sprintf(sql, "reset query cache");
    sqExecSQL(taos, sql);

    sprintf(sql, "use %s", dbName);
    sqExecSQL(taos, sql);

    sprintf(sql, "select * from %s", tbName);
    TAOS_RES* pRes = taos_query(taos, sql);
    code = taos_errno(pRes);
    if (code) {
      sqExit("taos_query", taos_errstr(pRes));
    }

    if (fetch) {
      taos_fetch_row(pRes);
    }
    
    taos_close(taos);
    taos_free_result(pRes);
  }
  CASE_LEAVE();  
}

int sqCloseAsyncQuery(bool fetch) {
  CASE_ENTER();  
  for (int32_t i = 0; i < runTimes; ++i) {
    char    sql[1024]  = {0};
    int32_t code = 0;
    TAOS   *taos = taos_connect(hostName, "root", "taosdata", NULL, 0);
    if (taos == NULL) sqExit("taos_connect", taos_errstr(NULL));

    sprintf(sql, "reset query cache");
    sqExecSQL(taos, sql);

    sprintf(sql, "use %s", dbName);
    sqExecSQL(taos, sql);

    sprintf(sql, "select * from %s", tbName);

    int32_t qEnd = 0;
    SSP_CB_PARAM param = {0};
    param.fetch = fetch;
    param.end = &qEnd;
    taos_query_a(taos, sql, sqCloseQueryCb, &param);
    while (0 == qEnd) {
      usleep(5000);
    }
  }
  CASE_LEAVE();  
}

void *syncQueryThreadFp(void *arg) {
  SSP_CB_PARAM* qParam = (SSP_CB_PARAM*)arg;
  char    sql[1024]  = {0};
  int32_t code = 0;
  TAOS   *taos = taos_connect(hostName, "root", "taosdata", NULL, 0);
  if (taos == NULL) sqExit("taos_connect", taos_errstr(NULL));

  qParam->taos = taos;

  sprintf(sql, "reset query cache");
  sqExecSQLE(taos, sql);
  
  sprintf(sql, "use %s", dbName);
  sqExecSQLE(taos, sql);
  
  sprintf(sql, "select * from %s", tbName);
  TAOS_RES* pRes = taos_query(taos, sql);
  
  if (qParam->fetch) {
    taos_fetch_row(pRes);
  }

  if (qParam->free) {
    taos_free_result(pRes);
  }
}

void *asyncQueryThreadFp(void *arg) {
  SSP_CB_PARAM* qParam = (SSP_CB_PARAM*)arg;
  char    sql[1024]  = {0};
  int32_t code = 0;
  TAOS   *taos = taos_connect(hostName, "root", "taosdata", NULL, 0);
  if (taos == NULL) sqExit("taos_connect", taos_errstr(NULL));

  qParam->taos = taos;

  sprintf(sql, "reset query cache");
  sqExecSQLE(taos, sql);
  
  sprintf(sql, "use %s", dbName);
  sqExecSQLE(taos, sql);
  
  sprintf(sql, "select * from %s", tbName);
  
  int32_t qEnd = 0;
  SSP_CB_PARAM param = {0};
  param.fetch = qParam->fetch;
  param.end = &qEnd;
  taos_query_a(taos, sql, sqAsyncQueryCb, &param);
  while (0 == qEnd) {
    usleep(5000);
  }
}


void *closeThreadFp(void *arg) {
  SSP_CB_PARAM* qParam = (SSP_CB_PARAM*)arg;
  while (true) {
    if (qParam->taos) {
      usleep(rand() % 10000);
      taos_close(qParam->taos);
      break;
    }
    usleep(1);
  }
}

void *killThreadFp(void *arg) {
  SSP_CB_PARAM* qParam = (SSP_CB_PARAM*)arg;
  while (true) {
    if (qParam->taos) {
      usleep(rand() % 10000);
      taos_kill_query(qParam->taos);
      break;
    }
    usleep(1);
  }
}

void *cleanupThreadFp(void *arg) {
  SSP_CB_PARAM* qParam = (SSP_CB_PARAM*)arg;
  while (true) {
    if (qParam->taos) {
      usleep(rand() % 10000);
      taos_cleanup();
      break;
    }
    usleep(1);
  }
}




int sqConCloseSyncQuery(bool fetch) {
  CASE_ENTER();  
  pthread_t qid, cid;
  for (int32_t i = 0; i < runTimes; ++i) {
    SSP_CB_PARAM param = {0};
    param.fetch = fetch;
    pthread_create(&qid, NULL, syncQueryThreadFp, (void*)&param);
    pthread_create(&cid, NULL, closeThreadFp, (void*)&param);
    
    pthread_join(qid, NULL);
    pthread_join(cid, NULL);
  }
  CASE_LEAVE();  
}

int sqConCloseAsyncQuery(bool fetch) {
  CASE_ENTER();  
  pthread_t qid, cid;
  for (int32_t i = 0; i < runTimes; ++i) {
    SSP_CB_PARAM param = {0};
    param.fetch = fetch;
    pthread_create(&qid, NULL, asyncQueryThreadFp, (void*)&param);
    pthread_create(&cid, NULL, closeThreadFp, (void*)&param);
    
    pthread_join(qid, NULL);
    pthread_join(cid, NULL);
  }
  CASE_LEAVE();  
}


int sqKillSyncQuery(bool fetch) {
  CASE_ENTER();  
  for (int32_t i = 0; i < runTimes; ++i) {
    char    sql[1024]  = {0};
    int32_t code = 0;
    TAOS   *taos = taos_connect(hostName, "root", "taosdata", NULL, 0);
    if (taos == NULL) sqExit("taos_connect", taos_errstr(NULL));

    sprintf(sql, "reset query cache");
    sqExecSQL(taos, sql);

    sprintf(sql, "use %s", dbName);
    sqExecSQL(taos, sql);

    sprintf(sql, "select * from %s", tbName);
    TAOS_RES* pRes = taos_query(taos, sql);
    code = taos_errno(pRes);
    if (code) {
      sqExit("taos_query", taos_errstr(pRes));
    }

    if (fetch) {
      taos_fetch_row(pRes);
    }

    taos_kill_query(taos);
    
    taos_close(taos);
  }
  CASE_LEAVE();  
}

int sqKillAsyncQuery(bool fetch) {
  CASE_ENTER();  
  for (int32_t i = 0; i < runTimes; ++i) {
    char    sql[1024]  = {0};
    int32_t code = 0;
    TAOS   *taos = taos_connect(hostName, "root", "taosdata", NULL, 0);
    if (taos == NULL) sqExit("taos_connect", taos_errstr(NULL));

    sprintf(sql, "reset query cache");
    sqExecSQL(taos, sql);

    sprintf(sql, "use %s", dbName);
    sqExecSQL(taos, sql);

    sprintf(sql, "select * from %s", tbName);

    int32_t qEnd = 0;
    SSP_CB_PARAM param = {0};
    param.fetch = fetch;
    param.end = &qEnd;
    param.taos = taos;
    taos_query_a(taos, sql, sqKillQueryCb, &param);
    while (0 == qEnd) {
      usleep(5000);
    }
    
    taos_close(taos);
  }
  CASE_LEAVE();  
}

int sqConKillSyncQuery(bool fetch) {
  CASE_ENTER();  
  pthread_t qid, cid;
  for (int32_t i = 0; i < runTimes; ++i) {
    SSP_CB_PARAM param = {0};
    param.fetch = fetch;
    pthread_create(&qid, NULL, syncQueryThreadFp, (void*)&param);
    pthread_create(&cid, NULL, killThreadFp, (void*)&param);
    
    pthread_join(qid, NULL);
    pthread_join(cid, NULL);

    taos_close(param.taos);
  }
  CASE_LEAVE();  
}

int sqConKillAsyncQuery(bool fetch) {
  CASE_ENTER();  
  pthread_t qid, cid;
  for (int32_t i = 0; i < runTimes; ++i) {
    SSP_CB_PARAM param = {0};
    param.fetch = fetch;
    pthread_create(&qid, NULL, asyncQueryThreadFp, (void*)&param);
    pthread_create(&cid, NULL, killThreadFp, (void*)&param);
    
    pthread_join(qid, NULL);
    pthread_join(cid, NULL);

    taos_close(param.taos);
  }
  CASE_LEAVE();  
}

int sqConCleanupSyncQuery(bool fetch) {
  CASE_ENTER();  
  pthread_t qid, cid;
  for (int32_t i = 0; i < runTimes; ++i) {
    SSP_CB_PARAM param = {0};
    param.fetch = fetch;
    pthread_create(&qid, NULL, syncQueryThreadFp, (void*)&param);
    pthread_create(&cid, NULL, cleanupThreadFp, (void*)&param);
    
    pthread_join(qid, NULL);
    pthread_join(cid, NULL);
    break;
  }
  CASE_LEAVE();  
}

int sqConCleanupAsyncQuery(bool fetch) {
  CASE_ENTER();  
  pthread_t qid, cid;
  for (int32_t i = 0; i < runTimes; ++i) {
    SSP_CB_PARAM param = {0};
    param.fetch = fetch;
    pthread_create(&qid, NULL, asyncQueryThreadFp, (void*)&param);
    pthread_create(&cid, NULL, cleanupThreadFp, (void*)&param);
    
    pthread_join(qid, NULL);
    pthread_join(cid, NULL);
    break;   
  }
  CASE_LEAVE();  
}



void sqRunAllCase(void) {
  sqStopSyncQuery(false);
  sqStopSyncQuery(true);
  sqStopAsyncQuery(false);
  sqStopAsyncQuery(true);

  sqFreeSyncQuery(false);
  sqFreeSyncQuery(true);
  sqFreeAsyncQuery(false);
  sqFreeAsyncQuery(true);

  sqCloseSyncQuery(false);
  sqCloseSyncQuery(true);
  sqCloseAsyncQuery(false);
  sqCloseAsyncQuery(true);
  
  sqConCloseSyncQuery(false);
  sqConCloseSyncQuery(true);
  sqConCloseAsyncQuery(false);
  sqConCloseAsyncQuery(true);

  sqKillSyncQuery(false);
  sqKillSyncQuery(true);
  sqKillAsyncQuery(false);
  sqKillAsyncQuery(true);

#if 0
  /*  
  sqConKillSyncQuery(false);
  sqConKillSyncQuery(true);
  sqConKillAsyncQuery(false);
  sqConKillAsyncQuery(true);

  sqConCleanupSyncQuery(false);
  sqConCleanupSyncQuery(true);
  sqConCleanupAsyncQuery(false);
  sqConCleanupAsyncQuery(true);
  */
#endif
  
  int32_t l = 5;
  while (l) {
    printf("%d\n", l--);
    sleep(1);
  }
  printf("test done\n");
}

int main(int argc, char *argv[]) {
  if (argc != 4) {
    printf("usage: %s server-ip dbname tablename\n", argv[0]);
    exit(0);
  }

  srand((unsigned int)time(NULL));
  
  strcpy(hostName, argv[1]);
  strcpy(dbName, argv[2]);
  strcpy(tbName, argv[3]);

  sqRunAllCase();

  return 0;
}


