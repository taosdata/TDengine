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

#include "os.h"
#include "tscLog.h"
#include "tsclient.h"
#include "ttimer.h"
#include "tutil.h"
#include "taosmsg.h"

#include "taos.h"

void  tscSaveSlowQueryFp(void *handle, void *tmrId);
TAOS *tscSlowQueryConn = NULL;
bool  tscSlowQueryConnInitialized = false;

void tscInitConnCb(void *param, TAOS_RES *result, int code) {
  char *sql = param;
  if (code < 0) {
    tscError("taos:%p, slow query connect failed, code:%d", tscSlowQueryConn, code);
    taos_close(tscSlowQueryConn);
    tscSlowQueryConn = NULL;
    tscSlowQueryConnInitialized = false;
    free(sql);
  } else {
    tscDebug("taos:%p, slow query connect success, code:%d", tscSlowQueryConn, code);
    tscSlowQueryConnInitialized = true;
    tscSaveSlowQueryFp(sql, NULL);
  }
}

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

  tscDebug("%p added into sqlList", pSql);
}

void tscSaveSlowQueryFpCb(void *param, TAOS_RES *result, int code) {
  if (code < 0) {
    tscError("failed to save slow query, code:%d", code);
  } else {
    tscDebug("success to save slow query, code:%d", code);
  }
}

void tscSaveSlowQueryFp(void *handle, void *tmrId) {
  char *sql = handle;

  if (!tscSlowQueryConnInitialized) {
    if (tscSlowQueryConn == NULL) {
      tscDebug("start to init slow query connect");
      taos_connect_a(NULL, "monitor", tsInternalPass, "", 0, tscInitConnCb, sql, &tscSlowQueryConn);
    } else {
      tscError("taos:%p, slow query connect is already initialized", tscSlowQueryConn);
      free(sql);
    }
  } else {
    tscDebug("taos:%p, save slow query:%s", tscSlowQueryConn, sql);
    taos_query_a(tscSlowQueryConn, sql, tscSaveSlowQueryFpCb, NULL);
    free(sql);
  }
}

void tscSaveSlowQuery(SSqlObj *pSql) {
  const static int64_t SLOW_QUERY_INTERVAL = 3000000L; // todo configurable
  size_t size = 200;  // other part of sql string, expect the main sql str
  
  if (pSql->res.useconds < SLOW_QUERY_INTERVAL) {
    return;
  }

  tscDebug("%p query time:%" PRId64 " sql:%s", pSql, pSql->res.useconds, pSql->sqlstr);
  int32_t sqlSize = (int32_t)(TSDB_SLOW_QUERY_SQL_LEN + size);
  
  char *sql = malloc(sqlSize);
  if (sql == NULL) {
    tscError("%p failed to allocate memory to sent slow query to dnode", pSql);
    return;
  }
  
  int len = snprintf(sql, size, "insert into %s.slowquery values(now, '%s', %" PRId64 ", %" PRId64 ", '", tsMonitorDbName,
          pSql->pTscObj->user, pSql->stime, pSql->res.useconds);
  int sqlLen = snprintf(sql + len, TSDB_SLOW_QUERY_SQL_LEN, "%s", pSql->sqlstr);
  if (sqlLen > TSDB_SLOW_QUERY_SQL_LEN - 1) {
    sqlLen = len + TSDB_SLOW_QUERY_SQL_LEN - 1;
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
  tscDebug("%p removed from sqlList", pSql);
}

void tscKillQuery(STscObj *pObj, uint32_t killId) {
  pthread_mutex_lock(&pObj->mutex);

  SSqlObj *pSql = pObj->sqlList;
  while (pSql) {
    if (pSql->queryId == killId) break;
    pSql = pSql->next;
  }

  pthread_mutex_unlock(&pObj->mutex);

  if (pSql == NULL) {
    tscError("failed to kill query, id:%d, it may have completed/terminated", killId);
  } else {
    tscDebug("%p query is killed, queryId:%d", pSql, killId);
    taos_stop_query(pSql);
  }
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

  if (pStream) {
    tscDebug("%p stream:%p is killed, streamId:%d", pStream->pSql, pStream, killId);
    if (pStream->callback) {
      pStream->callback(pStream->param);
    }
    taos_close_stream(pStream);
  } else {
    tscError("failed to kill stream, streamId:%d not exist", killId);
  }
}

int tscBuildQueryStreamDesc(void *pMsg, STscObj *pObj) {
  SHeartBeatMsg *pHeartbeat = pMsg;
  int allocedQueriesNum = pHeartbeat->numOfQueries;
  int allocedStreamsNum = pHeartbeat->numOfStreams;
  
  pHeartbeat->numOfQueries = 0;
  SQueryDesc *pQdesc = (SQueryDesc *)pHeartbeat->pData;

  // We extract the lock to tscBuildHeartBeatMsg function.

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

    tstrncpy(pQdesc->sql, pSql->sqlstr, sizeof(pQdesc->sql));
    pQdesc->stime = htobe64(pSql->stime);
    pQdesc->queryId = htonl(pSql->queryId);
    pQdesc->useconds = htobe64(pSql->res.useconds);
    pQdesc->qHandle = htobe64(pSql->res.qhandle);

    pHeartbeat->numOfQueries++;
    pQdesc++;
    pSql = pSql->next;
    if (pHeartbeat->numOfQueries >= allocedQueriesNum) break;
  }

  pHeartbeat->numOfStreams = 0;
  SStreamDesc *pSdesc = (SStreamDesc *)pQdesc;

  SSqlStream *pStream = pObj->streamList;
  while (pStream) {
    tstrncpy(pSdesc->sql, pStream->pSql->sqlstr, sizeof(pSdesc->sql));
    pSdesc->streamId = htonl(pStream->streamId);
    pSdesc->num = htobe64(pStream->num);

    pSdesc->useconds = htobe64(pStream->useconds);
    pSdesc->stime = htobe64(pStream->stime - pStream->interval.interval);
    pSdesc->ctime = htobe64(pStream->ctime);

    pSdesc->slidingTime = htobe64(pStream->interval.sliding);
    pSdesc->interval = htobe64(pStream->interval.interval);

    pHeartbeat->numOfStreams++;
    pSdesc++;
    pStream = pStream->next;
    if (pHeartbeat->numOfStreams >= allocedStreamsNum) break;
  }

  int32_t msgLen = pHeartbeat->numOfQueries * sizeof(SQueryDesc) + pHeartbeat->numOfStreams * sizeof(SStreamDesc) +
                   sizeof(SHeartBeatMsg);
  pHeartbeat->connId = htonl(pObj->connId);
  pHeartbeat->numOfQueries = htonl(pHeartbeat->numOfQueries);
  pHeartbeat->numOfStreams = htonl(pHeartbeat->numOfStreams);

  return msgLen;
}

void tscKillConnection(STscObj *pObj) {
  pthread_mutex_lock(&pObj->mutex);

  SSqlObj *pSql = pObj->sqlList;
  while (pSql) {
    pSql = pSql->next;
  }
  

  SSqlStream *pStream = pObj->streamList;
  while (pStream) {
    SSqlStream *tmp = pStream->next;
    taos_close_stream(pStream);
    pStream = tmp;
  }

  pthread_mutex_unlock(&pObj->mutex);

  tscDebug("connection:%p is killed", pObj);
  taos_close(pObj);
}
