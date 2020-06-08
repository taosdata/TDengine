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
#include "ttime.h"
#include "tcache.h"
#include "tglobal.h"
#include "tdataformat.h"
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeAcct.h"
#include "mnodeDnode.h"
#include "mnodeDb.h"
#include "mnodeMnode.h"
#include "mnodeProfile.h"
#include "mnodeShow.h"
#include "mnodeTable.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"
#include "mnodeWrite.h"

#define CONN_KEEP_TIME  (tsShellActivityTimer * 3)
#define CONN_CHECK_TIME (tsShellActivityTimer * 2)
#define QUERY_ID_SIZE   20

extern void *tsMnodeTmr;
static SCacheObj *tsMnodeConnCache = NULL;
static uint32_t tsConnIndex = 0;

static int32_t mnodeGetQueryMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveQueries(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mnodeGetConnsMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveConns(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mnodeGetStreamMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveStreams(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static void    mnodeFreeConn(void *data);
static int32_t mnodeProcessKillQueryMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessKillStreamMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessKillConnectionMsg(SMnodeMsg *pMsg);

int32_t mnodeInitProfile() {
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_QUERIES, mnodeGetQueryMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_QUERIES, mnodeRetrieveQueries);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_CONNS, mnodeGetConnsMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_CONNS, mnodeRetrieveConns);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_STREAMS, mnodeGetStreamMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_STREAMS, mnodeRetrieveStreams);

  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_KILL_QUERY, mnodeProcessKillQueryMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_KILL_STREAM, mnodeProcessKillStreamMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_KILL_CONN, mnodeProcessKillConnectionMsg);

  tsMnodeConnCache = taosCacheInitWithCb(tsMnodeTmr, CONN_CHECK_TIME, mnodeFreeConn);
  return 0;
}

void mnodeCleanupProfile() {
  if (tsMnodeConnCache != NULL) {
    mPrint("conn cache is cleanup");
    taosCacheCleanup(tsMnodeConnCache);
    tsMnodeConnCache = NULL;
  }
}

SConnObj *mnodeCreateConn(char *user, uint32_t ip, uint16_t port) {
  int32_t connSize = taosHashGetSize(tsMnodeConnCache->pHashTable);
  if (connSize > tsMaxShellConns) {
    mError("failed to create conn for user:%s ip:%s:%u, conns:%d larger than maxShellConns:%d, ", user, taosIpStr(ip),
           port, connSize, tsMaxShellConns);
    terrno = TSDB_CODE_MND_TOO_MANY_SHELL_CONNS;
    return NULL;
  }

  uint32_t connId = atomic_add_fetch_32(&tsConnIndex, 1);
  if (connId == 0) atomic_add_fetch_32(&tsConnIndex, 1);

  SConnObj connObj = {
    .ip     = ip,
    .port   = port,
    .connId = connId,
    .stime  = taosGetTimestampMs()
  };
  strcpy(connObj.user, user);
  
  char key[10];
  sprintf(key, "%u", connId);  
  SConnObj *pConn = taosCachePut(tsMnodeConnCache, key, &connObj, sizeof(connObj), CONN_KEEP_TIME);
  
  mTrace("connId:%d, is created, user:%s ip:%s:%u", connId, user, taosIpStr(ip), port);
  return pConn;
}

void mnodeReleaseConn(SConnObj *pConn) {
  if(pConn == NULL) return;
  taosCacheRelease(tsMnodeConnCache, (void**)&pConn, false);
}

SConnObj *mnodeAccquireConn(uint32_t connId, char *user, uint32_t ip, uint16_t port) {
  char key[10];
  sprintf(key, "%u", connId);
  uint64_t expireTime = CONN_KEEP_TIME * 1000 + (uint64_t)taosGetTimestampMs();

  SConnObj *pConn = taosCacheUpdateExpireTimeByName(tsMnodeConnCache, key, expireTime);
  if (pConn == NULL) {
    mError("connId:%d, is already destroyed, user:%s ip:%s:%u", connId, user, taosIpStr(ip), port);
    return NULL;
  }

  if (pConn->ip != ip || pConn->port != port /* || strcmp(pConn->user, user) != 0 */) {
    mError("connId:%d, incoming conn user:%s ip:%s:%u, not match exist conn user:%s ip:%s:%u", connId, user,
           taosIpStr(ip), port, pConn->user, taosIpStr(pConn->ip), pConn->port);
    taosCacheRelease(tsMnodeConnCache, (void **)&pConn, false);
    return NULL;
  }

  // mTrace("connId:%d, is incoming, user:%s ip:%s:%u", connId, pConn->user, taosIpStr(pConn->ip), pConn->port);
  pConn->lastAccess = expireTime;
  return pConn;
}

static void mnodeFreeConn(void *data) {
  SConnObj *pConn = data;
  tfree(pConn->pQueries);
  tfree(pConn->pQueries);

  mTrace("connId:%d, is destroyed", pConn->connId);
}

static void *mnodeGetNextConn(SHashMutableIterator *pIter, SConnObj **pConn) {
  *pConn = NULL;

  if (pIter == NULL) {
    pIter = taosHashCreateIter(tsMnodeConnCache->pHashTable);
  }

  if (!taosHashIterNext(pIter)) {
    taosHashDestroyIter(pIter);
    return NULL;
  }

  SCacheDataNode **pNode = taosHashIterGet(pIter);
  if (pNode == NULL || *pNode == NULL) {
    taosHashDestroyIter(pIter);
    return NULL;
  }

  *pConn = (SConnObj*)((*pNode)->data);
  return pIter;
}

static int32_t mnodeGetConnsMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;
  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_MND_NO_RIGHTS;
  
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

  pShow->bytes[cols] = TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "ip:port");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "login time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "last access");
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
  char      ipStr[TSDB_IPv4ADDR_LEN + 7];

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextConn(pShow->pIter, &pConnObj);
    if (pConnObj == NULL) break;
    
    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *) pWrite = pConnObj->connId;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pConnObj->user, TSDB_USER_LEN);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    snprintf(ipStr, TSDB_IPv4ADDR_LEN + 6, "%s:%u", taosIpStr(pConnObj->ip), pConnObj->port);
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, ipStr, TSDB_IPv4ADDR_LEN + 6);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pConnObj->stime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pConnObj->lastAccess;
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  const int32_t NUM_OF_COLUMNS = 5;
  mnodeVacuumResult(data, NUM_OF_COLUMNS, numOfRows, rows, pShow);
  
  return numOfRows;
}

// not thread safe, need optimized
int32_t mnodeSaveQueryStreamList(SConnObj *pConn, SCMHeartBeatMsg *pHBMsg) {
  pConn->numOfQueries = htonl(pHBMsg->numOfQueries);
  if (pConn->numOfQueries > 0 && pConn->numOfQueries < 20) {
    pConn->pQueries = calloc(sizeof(SQueryDesc), pConn->numOfQueries);
    memcpy(pConn->pQueries, pHBMsg->pData, pConn->numOfQueries * sizeof(SQueryDesc));
  }

  pConn->numOfStreams = htonl(pHBMsg->numOfStreams);
  if (pConn->numOfStreams > 0 && pConn->numOfStreams < 20) {
    pConn->pStreams = calloc(sizeof(SStreamDesc), pConn->numOfStreams);
    memcpy(pConn->pStreams, pHBMsg->pData + pConn->numOfQueries * sizeof(SQueryDesc),
           pConn->numOfStreams * sizeof(SStreamDesc));
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeGetQueryMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;
  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_MND_NO_RIGHTS;

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = QUERY_ID_SIZE + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "queryId");
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
  char      ipStr[TSDB_IPv4ADDR_LEN + 7];

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextConn(pShow->pIter, &pConnObj);
    if (pConnObj == NULL) break;

    for (int32_t i = 0; i < pConnObj->numOfQueries; ++i) {
      SQueryDesc *pDesc = pConnObj->pQueries + i;
      cols = 0;

      snprintf(ipStr, QUERY_ID_SIZE + 1, "%u:%u", pConnObj->connId, htonl(pDesc->queryId));
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, ipStr, QUERY_ID_SIZE);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pConnObj->user, TSDB_USER_LEN);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      snprintf(ipStr, TSDB_IPv4ADDR_LEN + 6, "%s:%u", taosIpStr(pConnObj->ip), pConnObj->port);
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, ipStr, TSDB_IPv4ADDR_LEN + 6);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int64_t *)pWrite = htobe64(pDesc->stime);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int64_t *)pWrite = htobe64(pDesc->useconds);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pDesc->sql, TSDB_SHOW_SQL_LEN);
      cols++;

      numOfRows++;
    }
  }

  pShow->numOfReads += numOfRows;
  const int32_t NUM_OF_COLUMNS = 6;
  mnodeVacuumResult(data, NUM_OF_COLUMNS, numOfRows, rows, pShow);
  return numOfRows;
}

static int32_t mnodeGetStreamMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;
  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_MND_NO_RIGHTS;

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
  char      ipStr[TSDB_IPv4ADDR_LEN + 7];

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextConn(pShow->pIter, &pConnObj);
    if (pConnObj == NULL) break;

    for (int32_t i = 0; i < pConnObj->numOfStreams; ++i) {
      SStreamDesc *pDesc = pConnObj->pStreams + i;
      cols = 0;

      snprintf(ipStr, QUERY_ID_SIZE + 1, "%u:%u", pConnObj->connId, htonl(pDesc->streamId));
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, ipStr, QUERY_ID_SIZE);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pConnObj->user, TSDB_USER_LEN);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      snprintf(ipStr, TSDB_IPv4ADDR_LEN + 6, "%s:%u", taosIpStr(pConnObj->ip), pConnObj->port);
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, ipStr, TSDB_IPv4ADDR_LEN + 6);
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
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pDesc->sql, TSDB_SHOW_SQL_LEN);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = (int32_t)htobe64(pDesc->num);
      cols++;

      numOfRows++;
    }
  }

  pShow->numOfReads += numOfRows;
  const int32_t NUM_OF_COLUMNS = 8;
  mnodeVacuumResult(data, NUM_OF_COLUMNS, numOfRows, rows, pShow);
  return numOfRows;
}

static int32_t mnodeProcessKillQueryMsg(SMnodeMsg *pMsg) {
  SUserObj *pUser = pMsg->pUser;
  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_MND_NO_RIGHTS;

  SCMKillQueryMsg *pKill = pMsg->rpcMsg.pCont;
  mPrint("kill query msg is received, queryId:%s", pKill->queryId);

  const char delim = ':';
  char* connIdStr = strtok(pKill->queryId, &delim);
  char* queryIdStr = strtok(NULL, &delim);

  if (queryIdStr == NULL || connIdStr == NULL) {
    mPrint("failed to kill query, queryId:%s", pKill->queryId);
   return TSDB_CODE_MND_INVALID_QUERY_ID;
  }

  int32_t queryId = (int32_t)strtol(queryIdStr, NULL, 10);

  SConnObj *pConn = taosCacheAcquireByName(tsMnodeConnCache, connIdStr);
  if (pConn == NULL) {
    mError("connId:%s, failed to kill queryId:%d, conn not exist", connIdStr, queryId);
    return TSDB_CODE_MND_INVALID_CONN_ID;
  } else {
    mPrint("connId:%s, queryId:%d is killed by user:%s", connIdStr, queryId, pUser->user);
    pConn->queryId = queryId;
    taosCacheRelease(tsMnodeConnCache, (void **)&pConn, false);
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t mnodeProcessKillStreamMsg(SMnodeMsg *pMsg) {
  SUserObj *pUser = pMsg->pUser;
  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_MND_NO_RIGHTS;

  SCMKillQueryMsg *pKill = pMsg->rpcMsg.pCont;
  mPrint("kill stream msg is received, streamId:%s", pKill->queryId);

  const char delim = ':';
  char* connIdStr = strtok(pKill->queryId, &delim);
  char* streamIdStr = strtok(NULL, &delim);

  if (streamIdStr == NULL || connIdStr == NULL) {
    mPrint("failed to kill stream, streamId:%s", pKill->queryId);
   return TSDB_CODE_MND_INVALID_STREAM_ID;
  }

  int32_t streamId = (int32_t)strtol(streamIdStr, NULL, 10);

  SConnObj *pConn = taosCacheAcquireByName(tsMnodeConnCache, connIdStr);
  if (pConn == NULL) {
    mError("connId:%s, failed to kill streamId:%d, conn not exist", connIdStr, streamId);
    return TSDB_CODE_MND_INVALID_CONN_ID;
  } else {
    mPrint("connId:%s, streamId:%d is killed by user:%s", connIdStr, streamId, pUser->user);
    pConn->streamId = streamId;
    taosCacheRelease(tsMnodeConnCache, (void **)&pConn, false);
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t mnodeProcessKillConnectionMsg(SMnodeMsg *pMsg) {
  SUserObj *pUser = pMsg->pUser;
  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_MND_NO_RIGHTS;

  SCMKillConnMsg *pKill = pMsg->rpcMsg.pCont;
  SConnObj *      pConn = taosCacheAcquireByName(tsMnodeConnCache, pKill->queryId);
  if (pConn == NULL) {
    mError("connId:%s, failed to kill, conn not exist", pKill->queryId);
    return TSDB_CODE_MND_INVALID_CONN_ID;
  } else {
    mPrint("connId:%s, is killed by user:%s", pKill->queryId, pUser->user);
    pConn->killed = 1;
    taosCacheRelease(tsMnodeConnCache, (void**)&pConn, false);
    return TSDB_CODE_SUCCESS;
  }
}
