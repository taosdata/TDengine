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
#include "mgmtDef.h"
#include "mgmtLog.h"
#include "mgmtAcct.h"
#include "mgmtDnode.h"
#include "mgmtDb.h"
#include "mgmtMnode.h"
#include "mgmtProfile.h"
#include "mgmtShell.h"
#include "mgmtTable.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"

int32_t mgmtSaveQueryStreamList(SCMHeartBeatMsg *pHBMsg);

int32_t mgmtKillQuery(char *qidstr, void *pConn);
int32_t mgmtKillStream(char *qidstr, void *pConn);
int32_t mgmtKillConnection(char *qidstr, void *pConn);

typedef struct {
  char     user[TSDB_TABLE_ID_LEN + 1];
  uint64_t stime;
  uint32_t ip;
  uint16_t port;
} SConnInfo;

typedef struct {
  int       numOfConns;
  int       index;
  SConnInfo connInfo[];
} SConnShow;

typedef struct {
  uint32_t ip;
  uint16_t port;
  char     user[TSDB_TABLE_ID_LEN+ 1];
} SCDesc;

typedef struct {
  int32_t      index;
  int32_t      numOfQueries;
  SCDesc * connInfo;
  SCDesc **cdesc;
  SQueryDesc   qdesc[];
} SQueryShow;

typedef struct {
  int32_t      index;
  int32_t      numOfStreams;
  SCDesc * connInfo;
  SCDesc **cdesc;
  SStreamDesc   sdesc[];
} SStreamShow;

int32_t  mgmtSaveQueryStreamList(SCMHeartBeatMsg *pHBMsg) {
//  SAcctObj *pAcct = pConn->pAcct;
//
//  if (contLen <= 0 || pAcct == NULL) {
//    return 0;
//  }
//
//  pthread_mutex_lock(&pAcct->mutex);
//
//  if (pConn->pQList) {
//    pAcct->acctInfo.numOfQueries -= pConn->pQList->numOfQueries;
//    pAcct->acctInfo.numOfStreams -= pConn->pSList->numOfStreams;
//  }
//
//  pConn->pQList = realloc(pConn->pQList, contLen);
//  memcpy(pConn->pQList, cont, contLen);
//
//  pConn->pSList = (SStreamList *)(((char *)pConn->pQList) + pConn->pQList->numOfQueries * sizeof(SQueryDesc) + sizeof(SQqueryList));
//
//  pAcct->acctInfo.numOfQueries += pConn->pQList->numOfQueries;
//  pAcct->acctInfo.numOfStreams += pConn->pSList->numOfStreams;
//
//  pthread_mutex_unlock(&pAcct->mutex);

  return TSDB_CODE_SUCCESS;
}

int32_t mgmtGetQueries(SShowObj *pShow, void *pConn) {
//  SAcctObj *  pAcct = pConn->pAcct;
//  SQueryShow *pQueryShow;
//
//  pthread_mutex_lock(&pAcct->mutex);
//
//  pQueryShow = malloc(sizeof(SQueryDesc) * pAcct->acctInfo.numOfQueries + sizeof(SQueryShow));
//  pQueryShow->numOfQueries = 0;
//  pQueryShow->index = 0;
//  pQueryShow->connInfo = NULL;
//  pQueryShow->cdesc = NULL;
//
//  if (pAcct->acctInfo.numOfQueries > 0) {
//    pQueryShow->connInfo = (SCDesc *)malloc(pAcct->acctInfo.numOfConns * sizeof(SCDesc));
//    pQueryShow->cdesc = (SCDesc **)malloc(pAcct->acctInfo.numOfQueries * sizeof(SCDesc *));
//
//    pConn = pAcct->pConn;
//    SQueryDesc * pQdesc = pQueryShow->qdesc;
//    SCDesc * pCDesc = pQueryShow->connInfo;
//    SCDesc **ppCDesc = pQueryShow->cdesc;
//
//    while (pConn) {
//      if (pConn->pQList && pConn->pQList->numOfQueries > 0) {
//        pCDesc->ip = pConn->ip;
//        pCDesc->port = pConn->port;
//        strcpy(pCDesc->user, pConn->pUser->user);
//
//        memcpy(pQdesc, pConn->pQList->qdesc, sizeof(SQueryDesc) * pConn->pQList->numOfQueries);
//        pQdesc += pConn->pQList->numOfQueries;
//        pQueryShow->numOfQueries += pConn->pQList->numOfQueries;
//        for (int32_t i = 0; i < pConn->pQList->numOfQueries; ++i, ++ppCDesc) *ppCDesc = pCDesc;
//
//        pCDesc++;
//      }
//      pConn = pConn->next;
//    }
//  }
//
//  pthread_mutex_unlock(&pAcct->mutex);
//
//  // sorting based on useconds
//
//  pShow->pNode = pQueryShow;

  return 0;
}

int32_t mgmtGetQueryMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_USER_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "user");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_IPv4ADDR_LEN + 14;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "ip:port:id");
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

  pShow->bytes[cols] = TSDB_SHOW_SQL_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "sql");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = 1000000;
  pShow->pNode = NULL;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  mgmtGetQueries(pShow, pConn);
  return 0;
}

int32_t mgmtKillQuery(char *qidstr, void *pConn) {
//  char *temp, *chr, idstr[64];
//  strcpy(idstr, qidstr);
//
//  temp = idstr;
//  chr = strchr(temp, ':');
//  if (chr == NULL) goto _error;
//  *chr = 0;
//  uint32_t ip = inet_addr(temp);
//
//  temp = chr + 1;
//  chr = strchr(temp, ':');
//  if (chr == NULL) goto _error;
//  *chr = 0;
//  uint16_t port = htons(atoi(temp));
//
//  temp = chr + 1;
//  uint32_t queryId = atoi(temp);
//
//  SAcctObj *pAcct = pConn->pAcct;
//
//  pthread_mutex_lock(&pAcct->mutex);
//
//  pConn = pAcct->pConn;
//  while (pConn) {
//    if (pConn->ip == ip && pConn->port == port && pConn->pQList) {
//      int32_t     i;
//      SQueryDesc *pQDesc = pConn->pQList->qdesc;
//      for (i = 0; i < pConn->pQList->numOfQueries; ++i, ++pQDesc) {
//        if (pQDesc->queryId == queryId) break;
//      }
//
//      if (i < pConn->pQList->numOfQueries) break;
//    }
//
//    pConn = pConn->next;
//  }
//
//  if (pConn) pConn->queryId = queryId;
//
//  pthread_mutex_unlock(&pAcct->mutex);
//
//  if (pConn == NULL || pConn->pQList == NULL || pConn->pQList->numOfQueries == 0) goto _error;
//
//  mTrace("query:%s is there, kill it", qidstr);
//  return 0;
//
//_error:
//  mTrace("query:%s is not there", qidstr);

  return TSDB_CODE_INVALID_QUERY_ID;
}

int32_t mgmtRetrieveQueries(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t   numOfRows = 0;
  char *pWrite;
  int32_t   cols = 0;

  SQueryShow *pQueryShow = (SQueryShow *)pShow->pNode;

  if (rows > pQueryShow->numOfQueries - pQueryShow->index) rows = pQueryShow->numOfQueries - pQueryShow->index;

  while (numOfRows < rows) {
    SQueryDesc *pNode = pQueryShow->qdesc + pQueryShow->index;
    SCDesc *pCDesc = pQueryShow->cdesc[pQueryShow->index];
    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, pCDesc->user);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    uint32_t ip = pCDesc->ip;
    sprintf(pWrite, "%d.%d.%d.%d:%hu:%d", ip & 0xFF, (ip >> 8) & 0xFF, (ip >> 16) & 0xFF, ip >> 24, htons(pCDesc->port),
            pNode->queryId);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pNode->stime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pNode->useconds;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, pNode->sql);
    cols++;

    numOfRows++;
    pQueryShow->index++;
  }

  if (numOfRows == 0) {
    tfree(pQueryShow->cdesc);
    tfree(pQueryShow->connInfo);
    tfree(pQueryShow);
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

int32_t mgmtGetStreams(SShowObj *pShow, void *pConn) {
//  SAcctObj *   pAcct = pConn->pAcct;
//  SStreamShow *pStreamShow;
//
//  pthread_mutex_lock(&pAcct->mutex);
//
//  pStreamShow = malloc(sizeof(SStreamDesc) * pAcct->acctInfo.numOfStreams + sizeof(SQueryShow));
//  pStreamShow->numOfStreams = 0;
//  pStreamShow->index = 0;
//  pStreamShow->connInfo = NULL;
//  pStreamShow->cdesc = NULL;
//
//  if (pAcct->acctInfo.numOfStreams > 0) {
//    pStreamShow->connInfo = (SCDesc *)malloc(pAcct->acctInfo.numOfConns * sizeof(SCDesc));
//    pStreamShow->cdesc = (SCDesc **)malloc(pAcct->acctInfo.numOfStreams * sizeof(SCDesc *));
//
//    pConn = pAcct->pConn;
//    SStreamDesc * pSdesc = pStreamShow->sdesc;
//    SCDesc * pCDesc = pStreamShow->connInfo;
//    SCDesc **ppCDesc = pStreamShow->cdesc;
//
//    while (pConn) {
//      if (pConn->pSList && pConn->pSList->numOfStreams > 0) {
//        pCDesc->ip = pConn->ip;
//        pCDesc->port = pConn->port;
//        strcpy(pCDesc->user, pConn->pUser->user);
//
//        memcpy(pSdesc, pConn->pSList->sdesc, sizeof(SStreamDesc) * pConn->pSList->numOfStreams);
//        pSdesc += pConn->pSList->numOfStreams;
//        pStreamShow->numOfStreams += pConn->pSList->numOfStreams;
//        for (int32_t i = 0; i < pConn->pSList->numOfStreams; ++i, ++ppCDesc) *ppCDesc = pCDesc;
//
//        pCDesc++;
//      }
//      pConn = pConn->next;
//    }
//  }
//
//  pthread_mutex_unlock(&pAcct->mutex);
//
//  // sorting based on useconds
//
//  pShow->pNode = pStreamShow;

  return 0;
}

int32_t mgmtGetStreamMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t      cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_USER_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "user");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_IPv4ADDR_LEN + 14;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "ip:port:id");
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
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "time(us)");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_SHOW_SQL_LEN;
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
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = 1000000;
  pShow->pNode = NULL;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  mgmtGetStreams(pShow, pConn);
  return 0;
}

int32_t mgmtRetrieveStreams(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t   numOfRows = 0;
  char *pWrite;
  int32_t   cols = 0;

  SStreamShow *pStreamShow = (SStreamShow *)pShow->pNode;

  if (rows > pStreamShow->numOfStreams - pStreamShow->index) rows = pStreamShow->numOfStreams - pStreamShow->index;

  while (numOfRows < rows) {
    SStreamDesc *pNode = pStreamShow->sdesc + pStreamShow->index;
    SCDesc *pCDesc = pStreamShow->cdesc[pStreamShow->index];
    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, pCDesc->user);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    uint32_t ip = pCDesc->ip;
    sprintf(pWrite, "%d.%d.%d.%d:%hu:%d", ip & 0xFF, (ip >> 8) & 0xFF, (ip >> 16) & 0xFF, ip >> 24, htons(pCDesc->port),
            pNode->streamId);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pNode->ctime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pNode->stime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pNode->useconds;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, pNode->sql);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pNode->num;
    cols++;

    numOfRows++;
    pStreamShow->index++;
  }

  if (numOfRows == 0) {
    tfree(pStreamShow->cdesc);
    tfree(pStreamShow->connInfo);
    tfree(pStreamShow);
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

int32_t mgmtKillStream(char *qidstr, void *pConn) {
//  char *temp, *chr, idstr[64];
//  strcpy(idstr, qidstr);
//
//  temp = idstr;
//  chr = strchr(temp, ':');
//  if (chr == NULL) goto _error;
//  *chr = 0;
//  uint32_t ip = inet_addr(temp);
//
//  temp = chr + 1;
//  chr = strchr(temp, ':');
//  if (chr == NULL) goto _error;
//  *chr = 0;
//  uint16_t port = htons(atoi(temp));
//
//  temp = chr + 1;
//  uint32_t streamId = atoi(temp);
//
//  SAcctObj *pAcct = pConn->pAcct;
//
//  pthread_mutex_lock(&pAcct->mutex);
//
//  pConn = pAcct->pConn;
//  while (pConn) {
//    if (pConn->ip == ip && pConn->port == port && pConn->pSList) {
//      int32_t     i;
//      SStreamDesc *pSDesc = pConn->pSList->sdesc;
//      for (i = 0; i < pConn->pSList->numOfStreams; ++i, ++pSDesc) {
//        if (pSDesc->streamId == streamId) break;
//      }
//
//      if (i < pConn->pSList->numOfStreams) break;
//    }
//
//    pConn = pConn->next;
//  }
//
//  if (pConn) pConn->streamId = streamId;
//
//  pthread_mutex_unlock(&pAcct->mutex);
//
//  if (pConn == NULL || pConn->pSList == NULL || pConn->pSList->numOfStreams == 0) goto _error;
//
//  mTrace("stream:%s is there, kill it", qidstr);
//  return 0;
//
//_error:
//  mTrace("stream:%s is not there", qidstr);

  return TSDB_CODE_INVALID_STREAM_ID;
}

int32_t mgmtKillConnection(char *qidstr, void *pConn) {
//  void *pConn1 = NULL;
//  char *    temp, *chr, idstr[64];
//  strcpy(idstr, qidstr);
//
//  temp = idstr;
//  chr = strchr(temp, ':');
//  if (chr == NULL) goto _error;
//  *chr = 0;
//  uint32_t ip = inet_addr(temp);
//
//  temp = chr + 1;
//  uint16_t port = htons(atoi(temp));
//  SAcctObj *pAcct = pConn->pAcct;
//
//  pthread_mutex_lock(&pAcct->mutex);
//
//  pConn = pAcct->pConn;
//  while (pConn) {
//    if (pConn->ip == ip && pConn->port == port) {
//      // there maybe two connections from a shell
//      if (pConn1 == NULL)
//        pConn1 = pConn;
//      else
//        break;
//    }
//
//    pConn = pConn->next;
//  }
//
//  if (pConn1) pConn1->killConnection = 1;
//  if (pConn) pConn->killConnection = 1;
//
//  pthread_mutex_unlock(&pAcct->mutex);
//
//  if (pConn1 == NULL) goto _error;
//
//  mTrace("connection:%s is there, kill it", qidstr);
//  return 0;
//
//_error:
//  mTrace("connection:%s is not there", qidstr);

  return TSDB_CODE_INVALID_CONNECTION;
}

bool mgmtCheckQhandle(uint64_t qhandle) {
  return true;
}

void mgmtSaveQhandle(void *qhandle) {
  mTrace("qhandle:%p is allocated", qhandle);
}

void mgmtFreeQhandle(void *qhandle) {
  mTrace("qhandle:%p is freed", qhandle);
}

int mgmtGetConns(SShowObj *pShow, void *pConn) {
  //  SAcctObj * pAcct = pConn->pAcct;
  //  SConnShow *pConnShow;
  //
  //  pthread_mutex_lock(&pAcct->mutex);
  //
  //  pConnShow = malloc(sizeof(SConnInfo) * pAcct->acctInfo.numOfConns + sizeof(SConnShow));
  //  pConnShow->index = 0;
  //  pConnShow->numOfConns = 0;
  //
  //  if (pAcct->acctInfo.numOfConns > 0) {
  //    pConn = pAcct->pConn;
  //    SConnInfo *pConnInfo = pConnShow->connInfo;
  //
  //    while (pConn && pConn->pUser) {
  //      strcpy(pConnInfo->user, pConn->pUser->user);
  //      pConnInfo->ip = pConn->ip;
  //      pConnInfo->port = pConn->port;
  //      pConnInfo->stime = pConn->stime;
  //
  //      pConnShow->numOfConns++;
  //      pConnInfo++;
  //      pConn = pConn->next;
  //    }
  //  }
  //
  //  pthread_mutex_unlock(&pAcct->mutex);
  //
  //  // sorting based on useconds
  //
  //  pShow->pNode = pConnShow;

  return 0;
}

int32_t mgmtGetConnsMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN;
  SSchema *pSchema = pMeta->schema;

  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "user");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_IPv4ADDR_LEN + 6;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "ip:port");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "login time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = 1000000;
  pShow->pNode = NULL;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  mgmtGetConns(pShow, pConn);
  return 0;
}

int32_t mgmtRetrieveConns(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t   numOfRows = 0;
  char      *pWrite;
  int32_t   cols = 0;

  SConnShow *pConnShow = (SConnShow *)pShow->pNode;

  if (rows > pConnShow->numOfConns - pConnShow->index) rows = pConnShow->numOfConns - pConnShow->index;

  while (numOfRows < rows) {
    SConnInfo *pNode = pConnShow->connInfo + pConnShow->index;
    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, pNode->user);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    uint32_t ip = pNode->ip;
    sprintf(pWrite, "%d.%d.%d.%d:%hu", ip & 0xFF, (ip >> 8) & 0xFF, (ip >> 16) & 0xFF, ip >> 24, htons(pNode->port));
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pNode->stime;
    cols++;

    numOfRows++;
    pConnShow->index++;
  }

  if (numOfRows == 0) {
    tfree(pConnShow);
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

void mgmtProcessKillQueryMsg(SQueuedMsg *pMsg) {
  SRpcMsg rpcRsp = {.handle = pMsg->thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  
  SUserObj *pUser = mgmtGetUserFromConn(pMsg->thandle, NULL);
  if (pUser == NULL) {
    rpcRsp.code = TSDB_CODE_INVALID_USER;
    rpcSendResponse(&rpcRsp);
    return;
  }

  SCMKillQueryMsg *pKill = pMsg->pCont;
  int32_t code;

  if (!pUser->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtKillQuery(pKill->queryId, pMsg->thandle);
  }

  rpcRsp.code = code;
  rpcSendResponse(&rpcRsp);
  mgmtDecUserRef(pUser);
}

void mgmtProcessKillStreamMsg(SQueuedMsg *pMsg) {
  SRpcMsg rpcRsp = {.handle = pMsg->thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  
  SUserObj *pUser = mgmtGetUserFromConn(pMsg->thandle, NULL);
  if (pUser == NULL) {
    rpcRsp.code = TSDB_CODE_INVALID_USER;
    rpcSendResponse(&rpcRsp);
    return;
  }

  SCMKillStreamMsg *pKill = pMsg->pCont;
  int32_t code;

  if (!pUser->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtKillStream(pKill->queryId, pMsg->thandle);
  }

  rpcRsp.code = code;
  rpcSendResponse(&rpcRsp);
  mgmtDecUserRef(pUser);
}

void mgmtProcessKillConnectionMsg(SQueuedMsg *pMsg) {
  SRpcMsg rpcRsp = {.handle = pMsg->thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  
  SUserObj *pUser = mgmtGetUserFromConn(pMsg->thandle, NULL);
  if (pUser == NULL) {
    rpcRsp.code = TSDB_CODE_INVALID_USER;
    rpcSendResponse(&rpcRsp);
    return;
  }

  SCMKillConnMsg *pKill = pMsg->pCont;
  int32_t code;

  if (!pUser->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtKillConnection(pKill->queryId, pMsg->thandle);
  }

  rpcRsp.code = code;
  rpcSendResponse(&rpcRsp);
  mgmtDecUserRef(pUser);
}

int32_t mgmtInitProfile() {
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_QUERIES, mgmtGetQueryMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_QUERIES, mgmtRetrieveQueries);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_CONNS, mgmtGetConnsMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_CONNS, mgmtRetrieveConns);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_STREAMS, mgmtGetStreamMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_STREAMS, mgmtRetrieveStreams);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_KILL_QUERY, mgmtProcessKillQueryMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_KILL_STREAM, mgmtProcessKillStreamMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_KILL_CONN, mgmtProcessKillConnectionMsg);

  return 0;
}

void mgmtCleanUpProfile() {
}

void *mgmtMallocQueuedMsg(SRpcMsg *rpcMsg) {
  bool usePublicIp = false;
  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle, &usePublicIp);
  if (pUser == NULL) {
    return NULL;
  }

  SQueuedMsg *pMsg = calloc(1, sizeof(SQueuedMsg));
  pMsg->thandle = rpcMsg->handle;
  pMsg->msgType = rpcMsg->msgType;
  pMsg->contLen = rpcMsg->contLen;
  pMsg->pCont = rpcMsg->pCont;
  pMsg->pUser = pUser;
  pMsg->usePublicIp = usePublicIp;

  return pMsg;
}

void mgmtFreeQueuedMsg(SQueuedMsg *pMsg) {
  if (pMsg != NULL) {
    rpcFreeCont(pMsg->pCont);
    if (pMsg->pUser) mgmtDecUserRef(pMsg->pUser);
    if (pMsg->pDb) mgmtDecDbRef(pMsg->pDb);
    if (pMsg->pVgroup) mgmtDecVgroupRef(pMsg->pVgroup);
    if (pMsg->pTable) mgmtDecTableRef(pMsg->pTable);
    if (pMsg->pAcct) mgmtDecAcctRef(pMsg->pAcct);
    if (pMsg->pDnode) mgmtDecDnodeRef(pMsg->pDnode);
    free(pMsg);
  }
}

void* mgmtCloneQueuedMsg(SQueuedMsg *pSrcMsg) {
  SQueuedMsg *pDestMsg = calloc(1, sizeof(SQueuedMsg));
  
  pDestMsg->thandle = pSrcMsg->thandle;
  pDestMsg->msgType = pSrcMsg->msgType;
  pDestMsg->pCont   = pSrcMsg->pCont;
  pDestMsg->contLen = pSrcMsg->contLen;
  pDestMsg->retry   = pSrcMsg->retry;
  pDestMsg->maxRetry= pSrcMsg->maxRetry;
  pDestMsg->pUser   = pSrcMsg->pUser;
  pDestMsg->usePublicIp = pSrcMsg->usePublicIp;

  pSrcMsg->pCont = NULL;
  pSrcMsg->pUser = NULL;
  
  return pDestMsg;
}