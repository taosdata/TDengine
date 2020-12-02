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
#include "taosmsg.h"
#include "taoserror.h"
#include "tutil.h"
#include "tcache.h"
#include "tglobal.h"
#include "tdataformat.h"
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeProfile.h"
#include "mnodeShow.h"
#include "mnodeUser.h"
#include "mnodeWrite.h"

#define CONN_KEEP_TIME  (tsShellActivityTimer * 3)
#define CONN_CHECK_TIME (tsShellActivityTimer * 2)
#define QUERY_ID_SIZE   20
#define QUERY_STREAM_SAVE_SIZE 20

static int32_t tsMnodeConnCache = -1;
static int32_t tsConnIndex = 0;

static int32_t mnodeGetQueryMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveQueries(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mnodeGetConnsMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveConns(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static void    mnodeCancelGetNextConn(void *pIter);
static int32_t mnodeGetStreamMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveStreams(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static void    mnodeFreeConn(void *data);
static int32_t mnodeProcessKillQueryMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessKillStreamMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessKillConnectionMsg(SMnodeMsg *pMsg);

int32_t mnodeInitProfile() {
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_QUERIES, mnodeGetQueryMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_QUERIES, mnodeRetrieveQueries);
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_QUERIES, mnodeCancelGetNextConn);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_CONNS, mnodeGetConnsMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_CONNS, mnodeRetrieveConns);
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_CONNS, mnodeCancelGetNextConn);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_STREAMS, mnodeGetStreamMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_STREAMS, mnodeRetrieveStreams);
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_STREAMS, mnodeCancelGetNextConn);

  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_KILL_QUERY, mnodeProcessKillQueryMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_KILL_STREAM, mnodeProcessKillStreamMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_KILL_CONN, mnodeProcessKillConnectionMsg);

  tsMnodeConnCache = taosCacheInit(TSDB_DATA_TYPE_INT, CONN_CHECK_TIME, true, mnodeFreeConn, "conn");
  return 0;
}

void mnodeCleanupProfile() {
  if (tsMnodeConnCache >= 0) {
    taosCacheCleanup(tsMnodeConnCache);
    tsMnodeConnCache = -1;
  }
}

SConnObj *mnodeCreateConn(char *user, uint32_t ip, uint16_t port, int32_t pid, const char* app) {
#if 0
  int32_t connSize = taosHashGetSize(tsMnodeConnCache->pHashTable);
  if (connSize > tsMaxShellConns) {
    mError("failed to create conn for user:%s ip:%s:%u, conns:%d larger than maxShellConns:%d, ", user, taosIpStr(ip),
           port, connSize, tsMaxShellConns);
    terrno = TSDB_CODE_MND_TOO_MANY_SHELL_CONNS;
    return NULL;
  }
#endif  

  int32_t connId = atomic_add_fetch_32(&tsConnIndex, 1);
  if (connId == 0) atomic_add_fetch_32(&tsConnIndex, 1);

  SConnObj connObj = {
    .ip     = ip,
    .port   = port,
    .connId = connId,
    .stime  = taosGetTimestampMs(),
    .pid    = pid,
  };

  tstrncpy(connObj.user, user, tListLen(connObj.user));
  tstrncpy(connObj.appName, app, tListLen(connObj.appName));

  connObj.lastAccess = connObj.stime;

  SConnObj *pConn = taosCachePut(tsMnodeConnCache, &connId, sizeof(int32_t), &connObj, sizeof(connObj), CONN_KEEP_TIME * 1000);

  mDebug("connId:%d, is created, user:%s ip:%s:%u", connId, user, taosIpStr(ip), port);
  return pConn;
}

void mnodeReleaseConn(SConnObj *pConn) {
  if (pConn == NULL) return;
  taosCacheRelease(pConn);
}

SConnObj *mnodeAccquireConn(int32_t connId, char *user, uint32_t ip, uint16_t port) {
  SConnObj *pConn = taosCacheAcquireByKey(tsMnodeConnCache, &connId, sizeof(int32_t));
  if (pConn == NULL) {
    mDebug("connId:%d, is already destroyed, user:%s ip:%s:%u", connId, user, taosIpStr(ip), port);
    return NULL;
  }

  if (/* pConn->ip != ip || */ pConn->port != port /* || strcmp(pConn->user, user) != 0 */) {
    mDebug("connId:%d, incoming conn user:%s ip:%s:%u, not match exist conn user:%s ip:%s:%u", connId, user,
           taosIpStr(ip), port, pConn->user, taosIpStr(pConn->ip), pConn->port);
    taosCacheRelease(pConn);
    return NULL;
  }

  // mDebug("connId:%d, is incoming, user:%s ip:%s:%u", connId, pConn->user, taosIpStr(pConn->ip), pConn->port);
  pConn->lastAccess = CONN_KEEP_TIME * 1000 + (uint64_t)taosGetTimestampMs();
  return pConn;
}

static void mnodeFreeConn(void *data) {
  SConnObj *pConn = data;
  tfree(pConn->pQueries);
  tfree(pConn->pStreams);

  mDebug("connId:%d, is destroyed", pConn->connId);
}

static void *mnodeGetNextConn(void *pIter, SConnObj **pConn) {
  *pConn = NULL;

  pIter = taosHashIterate(tsMnodeConnCache->pHashTable, pIter);
  if (pIter == NULL) return NULL;

  SCacheDataNode **pNode = pIter;
  if (pNode == NULL || *pNode == NULL) {
    taosHashCancelIterate(tsMnodeConnCache->pHashTable, pIter);
    return NULL;
  }

  *pConn = (SConnObj*)((*pNode)->data);
  return pIter;
}

static void mnodeCancelGetNextConn(void *pIter) {
  taosHashCancelIterate(tsMnodeConnCache->pHashTable, pIter);
}

static int32_t mnodeGetConnsMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) != 0) return TSDB_CODE_MND_NO_RIGHTS;
  
  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "connId");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_USER_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "user");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  // app name
  pShow->bytes[cols] = TSDB_APPNAME_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "program");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  // app pid
  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "pid");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "ip:port");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "login_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "last_access");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = taosHashGetSize(tsMnodeConnCache->pHashTable);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

static int32_t mnodeRetrieveConns(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t   numOfRows = 0;
  SConnObj *pConnObj = NULL;
  int32_t   cols = 0;
  char *    pWrite;
  char      ipStr[TSDB_IPv4ADDR_LEN + 6];

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextConn(pShow->pIter, &pConnObj);
    if (pConnObj == NULL) break;
    
    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *) pWrite = pConnObj->connId;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pConnObj->user, pShow->bytes[cols]);
    cols++;

    // app name
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pConnObj->appName, pShow->bytes[cols]);
    cols++;

    // app pid
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t*)pWrite = pConnObj->pid;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    snprintf(ipStr, sizeof(ipStr), "%s:%u", taosIpStr(pConnObj->ip), pConnObj->port);
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, ipStr, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pConnObj->stime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (pConnObj->lastAccess < pConnObj->stime) pConnObj->lastAccess = pConnObj->stime;
    *(int64_t *)pWrite = pConnObj->lastAccess;
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  
  return numOfRows;
}

// not thread safe, need optimized
int32_t mnodeSaveQueryStreamList(SConnObj *pConn, SHeartBeatMsg *pHBMsg) {
  pConn->numOfQueries = htonl(pHBMsg->numOfQueries);
  if (pConn->numOfQueries > 0) {
    if (pConn->pQueries == NULL) {
      pConn->pQueries = calloc(sizeof(SQueryDesc), QUERY_STREAM_SAVE_SIZE);
    }

    int32_t saveSize = MIN(QUERY_STREAM_SAVE_SIZE, pConn->numOfQueries) * sizeof(SQueryDesc);
    if (saveSize > 0 && pConn->pQueries != NULL) {
      memcpy(pConn->pQueries, pHBMsg->pData, saveSize);
    }
  }

  pConn->numOfStreams = htonl(pHBMsg->numOfStreams);
  if (pConn->numOfStreams > 0) {
    if (pConn->pStreams == NULL) {
      pConn->pStreams = calloc(sizeof(SStreamDesc), QUERY_STREAM_SAVE_SIZE);
    }

    int32_t saveSize = MIN(QUERY_STREAM_SAVE_SIZE, pConn->numOfStreams) * sizeof(SStreamDesc);
    if (saveSize > 0 && pConn->pStreams != NULL) {
      memcpy(pConn->pStreams, pHBMsg->pData + pConn->numOfQueries * sizeof(SQueryDesc), saveSize);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeGetQueryMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) != 0) return TSDB_CODE_MND_NO_RIGHTS;

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = QUERY_ID_SIZE + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "query_id");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_USER_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "user");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "ip:port");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 24;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "qhandle");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "created_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_BIGINT;
  strcpy(pSchema[cols].name, "time(us)");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "sql");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = 1000000;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

static int32_t mnodeRetrieveQueries(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t   numOfRows = 0;
  SConnObj *pConnObj = NULL;
  int32_t   cols = 0;
  char *    pWrite;
  char      str[TSDB_IPv4ADDR_LEN + 6] = {0};

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextConn(pShow->pIter, &pConnObj);
    if (pConnObj == NULL) break;

    for (int32_t i = 0; i < pConnObj->numOfQueries; ++i) {
      SQueryDesc *pDesc = pConnObj->pQueries + i;
      cols = 0;

      snprintf(str, QUERY_ID_SIZE + 1, "%u:%u", pConnObj->connId, htonl(pDesc->queryId));
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, str, pShow->bytes[cols]);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pConnObj->user, pShow->bytes[cols]);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      snprintf(str, tListLen(str), "%s:%u", taosIpStr(pConnObj->ip), pConnObj->port);
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, str, pShow->bytes[cols]);
      cols++;

      char handleBuf[24] = {0};
      snprintf(handleBuf, tListLen(handleBuf), "%p", (void*)htobe64(pDesc->qHandle));
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;

      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, handleBuf, pShow->bytes[cols]);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int64_t *)pWrite = htobe64(pDesc->stime);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int64_t *)pWrite = htobe64(pDesc->useconds);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pDesc->sql, pShow->bytes[cols]);
      cols++;

      numOfRows++;
    }
  }

  pShow->numOfReads += numOfRows;
  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  return numOfRows;
}

static int32_t mnodeGetStreamMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) != 0) return TSDB_CODE_MND_NO_RIGHTS;

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = QUERY_ID_SIZE + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "streamId");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_USER_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "user");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "ip:port");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "created time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "exec time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_BIGINT;
  strcpy(pSchema[cols].name, "time(us)");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "sql");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "cycles");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = 1000000;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

static int32_t mnodeRetrieveStreams(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t   numOfRows = 0;
  SConnObj *pConnObj = NULL;
  int32_t   cols = 0;
  char *    pWrite;
  char      ipStr[TSDB_IPv4ADDR_LEN + 6];

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextConn(pShow->pIter, &pConnObj);
    if (pConnObj == NULL) break;

    for (int32_t i = 0; i < pConnObj->numOfStreams; ++i) {
      SStreamDesc *pDesc = pConnObj->pStreams + i;
      cols = 0;

      snprintf(ipStr, QUERY_ID_SIZE + 1, "%u:%u", pConnObj->connId, htonl(pDesc->streamId));
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, ipStr, pShow->bytes[cols]);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pConnObj->user, pShow->bytes[cols]);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      snprintf(ipStr, sizeof(ipStr), "%s:%u", taosIpStr(pConnObj->ip), pConnObj->port);
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, ipStr, pShow->bytes[cols]);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int64_t *)pWrite = htobe64(pDesc->ctime);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int64_t *)pWrite = htobe64(pDesc->stime);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int64_t *)pWrite = htobe64(pDesc->useconds);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pDesc->sql, pShow->bytes[cols]);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = (int32_t)htobe64(pDesc->num);
      cols++;

      numOfRows++;
    }
  }

  pShow->numOfReads += numOfRows;
  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  return numOfRows;
}

static int32_t mnodeProcessKillQueryMsg(SMnodeMsg *pMsg) {
  SUserObj *pUser = pMsg->pUser;
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) != 0) return TSDB_CODE_MND_NO_RIGHTS;

  SKillQueryMsg *pKill = pMsg->rpcMsg.pCont;
  mInfo("kill query msg is received, queryId:%s", pKill->queryId);

  const char delim = ':';
  char* connIdStr = strtok(pKill->queryId, &delim);
  char* queryIdStr = strtok(NULL, &delim);

  if (queryIdStr == NULL || connIdStr == NULL) {
    mInfo("failed to kill query, queryId:%s", pKill->queryId);
   return TSDB_CODE_MND_INVALID_QUERY_ID;
  }

  int32_t queryId = (int32_t)strtol(queryIdStr, NULL, 10);

  int32_t connId = atoi(connIdStr);
  SConnObj *pConn = taosCacheAcquireByKey(tsMnodeConnCache, &connId, sizeof(int32_t));
  if (pConn == NULL) {
    mError("connId:%s, failed to kill queryId:%d, conn not exist", connIdStr, queryId);
    return TSDB_CODE_MND_INVALID_CONN_ID;
  } else {
    mInfo("connId:%s, queryId:%d is killed by user:%s", connIdStr, queryId, pUser->user);
    pConn->queryId = queryId;
    taosCacheRelease(pConn);
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t mnodeProcessKillStreamMsg(SMnodeMsg *pMsg) {
  SUserObj *pUser = pMsg->pUser;
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) != 0) return TSDB_CODE_MND_NO_RIGHTS;

  SKillQueryMsg *pKill = pMsg->rpcMsg.pCont;
  mInfo("kill stream msg is received, streamId:%s", pKill->queryId);

  const char delim = ':';
  char* connIdStr = strtok(pKill->queryId, &delim);
  char* streamIdStr = strtok(NULL, &delim);

  if (streamIdStr == NULL || connIdStr == NULL) {
    mInfo("failed to kill stream, streamId:%s", pKill->queryId);
   return TSDB_CODE_MND_INVALID_STREAM_ID;
  }

  int32_t streamId = (int32_t)strtol(streamIdStr, NULL, 10);
  int32_t connId = atoi(connIdStr);

  SConnObj *pConn = taosCacheAcquireByKey(tsMnodeConnCache, &connId, sizeof(int32_t));
  if (pConn == NULL) {
    mError("connId:%s, failed to kill streamId:%d, conn not exist", connIdStr, streamId);
    return TSDB_CODE_MND_INVALID_CONN_ID;
  } else {
    mInfo("connId:%s, streamId:%d is killed by user:%s", connIdStr, streamId, pUser->user);
    pConn->streamId = streamId;
    taosCacheRelease(pConn);
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t mnodeProcessKillConnectionMsg(SMnodeMsg *pMsg) {
  SUserObj *pUser = pMsg->pUser;
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) != 0) return TSDB_CODE_MND_NO_RIGHTS;

  SKillConnMsg *pKill = pMsg->rpcMsg.pCont;
  int32_t connId = atoi(pKill->queryId);
  SConnObj *pConn = taosCacheAcquireByKey(tsMnodeConnCache, &connId, sizeof(int32_t));
  if (pConn == NULL) {
    mError("connId:%s, failed to kill, conn not exist", pKill->queryId);
    return TSDB_CODE_MND_INVALID_CONN_ID;
  } else {
    mInfo("connId:%s, is killed by user:%s", pKill->queryId, pUser->user);
    pConn->killed = 1;
    taosCacheRelease(pConn);
    return TSDB_CODE_SUCCESS;
  }
}
