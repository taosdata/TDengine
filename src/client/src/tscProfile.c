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

#include <stdio.h>
#include <stdlib.h>

#include "os.h"
#include "tlog.h"
#include "tsclient.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

void tscAddIntoSqlList(SSqlObj *pSql) {
  static uint32_t queryId = 1;

  STscObj *pObj = pSql->pTscObj;
  if (pSql->listed) return;

  pthread_mutex_lock(&pObj->mutex);

  assert(pSql != pObj->sqlList);
  pSql->next = pObj->sqlList;
  if (pObj->sqlList) pObj->sqlList->prev = pSql;
  pObj->sqlList = pSql;
  pSql->queryId = queryId++;

  pthread_mutex_unlock(&pObj->mutex);

  pSql->stime = taosGetTimestampMs();
  pSql->listed = 1;

  tscTrace("%p added into sqlList", pSql);
}

void tscSaveSlowQueryFpCb(void *param, TAOS_RES *result, int code) {
  if (code < 0) {
    tscError("failed to save slowquery, code:%d", code);
  }
}

void tscSaveSlowQueryFp(void *handle, void *tmrId) {
  char *sql = handle;

  static void *taos = NULL;
  if (taos == NULL) {
    taos = taos_connect(NULL, "monitor", tsInternalPass, NULL, 0);
    if (taos == NULL) {
      tscError("failed to save slow query, can't connect to server");
      free(sql);
      return;
    }
  }

  tscTrace("save slow query:sql", sql);
  taos_query_a(taos, sql, tscSaveSlowQueryFpCb, NULL);
  free(sql);
}

void tscSaveSlowQuery(SSqlObj *pSql) {
  const static int64_t SLOW_QUERY_INTERVAL = 3000000L;
  if (pSql->res.useconds < SLOW_QUERY_INTERVAL) return;

  tscTrace("%p query time:%ld sql:%s", pSql, pSql->res.useconds, pSql->sqlstr);

  char *sql = malloc(200);
  int   len = snprintf(sql, 200, "insert into sys.slowquery values(now, '%s', %ld, %ld, '", pSql->pTscObj->user,
                     pSql->stime, pSql->res.useconds);
  int sqlLen = snprintf(sql + len, TSDB_SHOW_SQL_LEN, "%s", pSql->sqlstr);
  if (sqlLen > TSDB_SHOW_SQL_LEN - 1) {
    sqlLen = len + TSDB_SHOW_SQL_LEN - 1;
  } else {
    sqlLen += len;
  }
  strcpy(sql + sqlLen, "')");

  taosTmrStart(tscSaveSlowQueryFp, 200, sql, tscTmr);
}

void tscRemoveFromSqlList(SSqlObj *pSql) {
  STscObj *pObj = pSql->pTscObj;
  if (pSql->listed == 0) return;

  pthread_mutex_lock(&pObj->mutex);

  if (pSql->prev)
    pSql->prev->next = pSql->next;
  else
    pObj->sqlList = pSql->next;

  if (pSql->next) pSql->next->prev = pSql->prev;

  pthread_mutex_unlock(&pObj->mutex);

  pSql->next = NULL;
  pSql->prev = NULL;
  pSql->listed = 0;

  tscSaveSlowQuery(pSql);
  tscTrace("%p removed from sqlList", pSql);
}

void tscKillQuery(STscObj *pObj, uint32_t killId) {
  pthread_mutex_lock(&pObj->mutex);

  SSqlObj *pSql = pObj->sqlList;
  while (pSql) {
    if (pSql->queryId == killId) break;
    pSql = pSql->next;
  }

  pthread_mutex_unlock(&pObj->mutex);

  if (pSql == NULL) return;

  tscTrace("%p query is killed, queryId:%d thandle:%p", pSql, killId, pSql->thandle);
  taos_stop_query(pSql);
}

void tscAddIntoStreamList(SSqlStream *pStream) {
  static uint32_t streamId = 1;
  STscObj *       pObj = pStream->pSql->pTscObj;

  pthread_mutex_lock(&pObj->mutex);

  pStream->next = pObj->streamList;
  if (pObj->streamList) pObj->streamList->prev = pStream;
  pObj->streamList = pStream;
  pStream->streamId = streamId++;

  pthread_mutex_unlock(&pObj->mutex);

  pStream->listed = 1;
}

void tscRemoveFromStreamList(SSqlStream *pStream, SSqlObj *pSqlObj) {
  if (pStream->listed == 0) return;

  STscObj *pObj = pSqlObj->pTscObj;

  pthread_mutex_lock(&pObj->mutex);

  if (pStream->prev)
    pStream->prev->next = pStream->next;
  else
    pObj->streamList = pStream->next;

  if (pStream->next) pStream->next->prev = pStream->prev;

  pthread_mutex_unlock(&pObj->mutex);

  pStream->next = NULL;
  pStream->prev = NULL;

  pStream->listed = 0;
}

void tscKillStream(STscObj *pObj, uint32_t killId) {
  pthread_mutex_lock(&pObj->mutex);

  SSqlStream *pStream = pObj->streamList;
  while (pStream) {
    if (pStream->streamId == killId) break;
    pStream = pStream->next;
  }

  pthread_mutex_unlock(&pObj->mutex);

  tscTrace("%p stream:%p is killed, streamId:%d", pStream->pSql, pStream, killId);

  taos_close_stream(pStream);
  if (pStream->callback) {
    pStream->callback(pStream->param);
  }
}

char *tscBuildQueryStreamDesc(char *pMsg, STscObj *pObj) {
  SQList *pQList = (SQList *)pMsg;
  char *  pMax = pMsg + TSDB_PAYLOAD_SIZE - 256;

  SQDesc *pQdesc = pQList->qdesc;
  pQList->numOfQueries = 0;

  // We extract the lock to tscBuildHeartBeatMsg function.
  /* pthread_mutex_lock (&pObj->mutex); */

  pMsg += sizeof(SQList);
  SSqlObj *pSql = pObj->sqlList;
  while (pSql) {
    /*
     * avoid sqlobj may not be correctly removed from sql list
     * e.g., forgetting to free the sql result may cause the sql object still in sql list
     */
    if (pSql->sqlstr == NULL) {
      pSql = pSql->next;
      continue;
    }

    strncpy(pQdesc->sql, pSql->sqlstr, TSDB_SHOW_SQL_LEN - 1);
    pQdesc->sql[TSDB_SHOW_SQL_LEN - 1] = 0;
    pQdesc->stime = pSql->stime;
    pQdesc->queryId = pSql->queryId;
    pQdesc->useconds = pSql->res.useconds;

    pQList->numOfQueries++;
    pQdesc++;
    pSql = pSql->next;
    pMsg += sizeof(SQDesc);
    if (pMsg > pMax) break;
  }

  SSList *pSList = (SSList *)pMsg;
  SSDesc *pSdesc = pSList->sdesc;
  pSList->numOfStreams = 0;

  pMsg += sizeof(SSList);
  SSqlStream *pStream = pObj->streamList;
  while (pStream) {
    strncpy(pSdesc->sql, pStream->pSql->sqlstr, TSDB_SHOW_SQL_LEN - 1);
    pSdesc->sql[TSDB_SHOW_SQL_LEN - 1] = 0;
    pSdesc->streamId = pStream->streamId;
    pSdesc->num = pStream->num;

    pSdesc->useconds = pStream->useconds;
    pSdesc->stime = pStream->stime - pStream->interval;
    pSdesc->ctime = pStream->ctime;

    pSdesc->slidingTime = pStream->slidingTime;
    pSdesc->interval = pStream->interval;

    pSList->numOfStreams++;
    pSdesc++;
    pStream = pStream->next;
    pMsg += sizeof(SSDesc);
    if (pMsg > pMax) break;
  }

  /* pthread_mutex_unlock (&pObj->mutex); */

  return pMsg;
}

void tscKillConnection(STscObj *pObj) {
  pthread_mutex_lock(&pObj->mutex);

  SSqlObj *pSql = pObj->sqlList;
  while (pSql) {
    taosStopRpcConn(pSql->thandle);
    pSql = pSql->next;
  }

  SSqlStream *pStream = pObj->streamList;
  while (pStream) {
    taos_close_stream(pStream);
    pStream = pStream->next;
  }

  pthread_mutex_unlock(&pObj->mutex);

  taos_close(pObj);

  tscTrace("connection:%p is killed", pObj);
}
