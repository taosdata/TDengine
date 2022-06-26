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
int32_t runTimes = 10000;

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


void sqExit(char* prefix, const char* errMsg) {
  fprintf(stderr, "%s error: %s\n", prefix, errMsg);
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
}

void sqCloseQueryCb(void *param, TAOS_RES *pRes, int code) {
  SSP_CB_PARAM *qParam = (SSP_CB_PARAM *)param;
  if (code == 0 && pRes) {
    if (qParam->fetch) {
      taos_fetch_rows_a(pRes, sqFreeFetchCb, param);
    } else {
      taos_close(qParam->taos);
      *qParam->end = 1;
    }
  } else {
    sqExit("select", taos_errstr(pRes));
  }
}

int sqSyncStopQuery(bool fetch) {
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

int sqAsyncStopQuery(bool fetch) {
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

int sqSyncFreeQuery(bool fetch) {
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

int sqAsyncFreeQuery(bool fetch) {
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

int sqSyncCloseQuery(bool fetch) {
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
  }
  CASE_LEAVE();  
}

int sqAsyncCloseQuery(bool fetch) {
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

  taos_free_result(pRes);
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


int sqConSyncCloseQuery(bool fetch) {
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

void sqRunAllCase(void) {
/*
  sqSyncStopQuery(false);
  sqSyncStopQuery(true);
  sqAsyncStopQuery(false);
  sqAsyncStopQuery(true);

  sqSyncFreeQuery(false);
  sqSyncFreeQuery(true);
  sqAsyncFreeQuery(false);
  sqAsyncFreeQuery(true);

  sqSyncCloseQuery(false);
  sqSyncCloseQuery(true);
  sqAsyncCloseQuery(false);
  sqAsyncCloseQuery(true);
*/  
  sqConSyncCloseQuery(false);
  sqConSyncCloseQuery(true);

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


