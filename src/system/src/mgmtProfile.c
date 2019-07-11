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

#include "mgmt.h"
#include <arpa/inet.h>
#include "mgmtProfile.h"
#include "taosmsg.h"
#include "tschemautil.h"

typedef struct {
  uint32_t ip;
  short    port;
  char     user[TSDB_METER_ID_LEN];
} SCDesc;

typedef struct {
  int      index;
  int      numOfQueries;
  SCDesc * connInfo;
  SCDesc **cdesc;
  SQDesc   qdesc[];
} SQueryShow;

typedef struct {
  int      index;
  int      numOfStreams;
  SCDesc * connInfo;
  SCDesc **cdesc;
  SSDesc   sdesc[];
} SStreamShow;

int mgmtSaveQueryStreamList(char *cont, int contLen, SConnObj *pConn) {
  SAcctObj *pAcct = pConn->pAcct;

  if (contLen <= 0) {
    return 0;
  }

  pthread_mutex_lock(&pAcct->mutex);

  if (pConn->pQList) {
    pAcct->acctInfo.numOfQueries -= pConn->pQList->numOfQueries;
    pAcct->acctInfo.numOfStreams -= pConn->pSList->numOfStreams;
  }

  pConn->pQList = realloc(pConn->pQList, contLen);
  memcpy(pConn->pQList, cont, contLen);

  pConn->pSList = (SSList *)(((char *)pConn->pQList) + pConn->pQList->numOfQueries * sizeof(SQDesc) + sizeof(SQList));

  pAcct->acctInfo.numOfQueries += pConn->pQList->numOfQueries;
  pAcct->acctInfo.numOfStreams += pConn->pSList->numOfStreams;

  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}

int mgmtGetQueries(SShowObj *pShow, SConnObj *pConn) {
  SAcctObj *  pAcct = pConn->pAcct;
  SQueryShow *pQueryShow;

  pthread_mutex_lock(&pAcct->mutex);

  pQueryShow = malloc(sizeof(SQDesc) * pAcct->acctInfo.numOfQueries + sizeof(SQueryShow));
  pQueryShow->numOfQueries = 0;
  pQueryShow->index = 0;
  pQueryShow->connInfo = NULL;
  pQueryShow->cdesc = NULL;

  if (pAcct->acctInfo.numOfQueries > 0) {
    pQueryShow->connInfo = (SCDesc *)malloc(pAcct->acctInfo.numOfConns * sizeof(SCDesc));
    pQueryShow->cdesc = (SCDesc **)malloc(pAcct->acctInfo.numOfQueries * sizeof(SCDesc *));

    pConn = pAcct->pConn;
    SQDesc * pQdesc = pQueryShow->qdesc;
    SCDesc * pCDesc = pQueryShow->connInfo;
    SCDesc **ppCDesc = pQueryShow->cdesc;

    while (pConn) {
      if (pConn->pQList && pConn->pQList->numOfQueries > 0) {
        pCDesc->ip = pConn->ip;
        pCDesc->port = pConn->port;
        strcpy(pCDesc->user, pConn->pUser->user);

        memcpy(pQdesc, pConn->pQList->qdesc, sizeof(SQDesc) * pConn->pQList->numOfQueries);
        pQdesc += pConn->pQList->numOfQueries;
        pQueryShow->numOfQueries += pConn->pQList->numOfQueries;
        for (int i = 0; i < pConn->pQList->numOfQueries; ++i, ++ppCDesc) *ppCDesc = pCDesc;

        pCDesc++;
      }
      pConn = pConn->next;
    }
  }

  pthread_mutex_unlock(&pAcct->mutex);

  // sorting based on useconds

  pShow->pNode = pQueryShow;

  return 0;
}

int mgmtGetQueryMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) {
  int cols = 0;

  SSchema *pSchema = tsGetSchema(pMeta);

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
  for (int i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = 1000000;
  pShow->pNode = NULL;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  mgmtGetQueries(pShow, pConn);
  return 0;
}

int mgmtKillQuery(char *qidstr, SConnObj *pConn) {
  char *temp, *chr, idstr[64];
  strcpy(idstr, qidstr);

  temp = idstr;
  chr = strchr(temp, ':');
  if (chr == NULL) goto _error;
  *chr = 0;
  uint32_t ip = inet_addr(temp);

  temp = chr + 1;
  chr = strchr(temp, ':');
  if (chr == NULL) goto _error;
  *chr = 0;
  short port = htons(atoi(temp));

  temp = chr + 1;
  uint32_t queryId = atoi(temp);

  SAcctObj *pAcct = pConn->pAcct;

  pthread_mutex_lock(&pAcct->mutex);

  pConn = pAcct->pConn;
  while (pConn) {
    if (pConn->ip == ip && pConn->port == port && pConn->pQList) {
      int     i;
      SQDesc *pQDesc = pConn->pQList->qdesc;
      for (i = 0; i < pConn->pQList->numOfQueries; ++i, ++pQDesc) {
        if (pQDesc->queryId == queryId) break;
      }

      if (i < pConn->pQList->numOfQueries) break;
    }

    pConn = pConn->next;
  }

  if (pConn) pConn->queryId = queryId;

  pthread_mutex_unlock(&pAcct->mutex);

  if (pConn == NULL || pConn->pQList == NULL || pConn->pQList->numOfQueries == 0) goto _error;

  mTrace("query:%s is there, kill it", qidstr);
  return 0;

_error:
  mTrace("query:%s is not there", qidstr);

  return TSDB_CODE_INVALID_QUERY_ID;
}

int mgmtRetrieveQueries(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int   numOfRows = 0;
  char *pWrite;
  int   cols = 0;

  SQueryShow *pQueryShow = (SQueryShow *)pShow->pNode;

  if (rows > pQueryShow->numOfQueries - pQueryShow->index) rows = pQueryShow->numOfQueries - pQueryShow->index;

  while (numOfRows < rows) {
    SQDesc *pNode = pQueryShow->qdesc + pQueryShow->index;
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

int mgmtGetStreams(SShowObj *pShow, SConnObj *pConn) {
  SAcctObj *   pAcct = pConn->pAcct;
  SStreamShow *pStreamShow;

  pthread_mutex_lock(&pAcct->mutex);

  pStreamShow = malloc(sizeof(SSDesc) * pAcct->acctInfo.numOfStreams + sizeof(SQueryShow));
  pStreamShow->numOfStreams = 0;
  pStreamShow->index = 0;
  pStreamShow->connInfo = NULL;
  pStreamShow->cdesc = NULL;

  if (pAcct->acctInfo.numOfStreams > 0) {
    pStreamShow->connInfo = (SCDesc *)malloc(pAcct->acctInfo.numOfConns * sizeof(SCDesc));
    pStreamShow->cdesc = (SCDesc **)malloc(pAcct->acctInfo.numOfStreams * sizeof(SCDesc *));

    pConn = pAcct->pConn;
    SSDesc * pSdesc = pStreamShow->sdesc;
    SCDesc * pCDesc = pStreamShow->connInfo;
    SCDesc **ppCDesc = pStreamShow->cdesc;

    while (pConn) {
      if (pConn->pSList && pConn->pSList->numOfStreams > 0) {
        pCDesc->ip = pConn->ip;
        pCDesc->port = pConn->port;
        strcpy(pCDesc->user, pConn->pUser->user);

        memcpy(pSdesc, pConn->pSList->sdesc, sizeof(SSDesc) * pConn->pSList->numOfStreams);
        pSdesc += pConn->pSList->numOfStreams;
        pStreamShow->numOfStreams += pConn->pSList->numOfStreams;
        for (int i = 0; i < pConn->pSList->numOfStreams; ++i, ++ppCDesc) *ppCDesc = pCDesc;

        pCDesc++;
      }
      pConn = pConn->next;
    }
  }

  pthread_mutex_unlock(&pAcct->mutex);

  // sorting based on useconds

  pShow->pNode = pStreamShow;

  return 0;
}

int mgmtGetStreamMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) {
  int      cols = 0;
  SSchema *pSchema = tsGetSchema(pMeta);

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
  for (int i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = 1000000;
  pShow->pNode = NULL;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  mgmtGetStreams(pShow, pConn);
  return 0;
}

int mgmtRetrieveStreams(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int   numOfRows = 0;
  char *pWrite;
  int   cols = 0;

  SStreamShow *pStreamShow = (SStreamShow *)pShow->pNode;

  if (rows > pStreamShow->numOfStreams - pStreamShow->index) rows = pStreamShow->numOfStreams - pStreamShow->index;

  while (numOfRows < rows) {
    SSDesc *pNode = pStreamShow->sdesc + pStreamShow->index;
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

int mgmtKillStream(char *qidstr, SConnObj *pConn) {
  char *temp, *chr, idstr[64];
  strcpy(idstr, qidstr);

  temp = idstr;
  chr = strchr(temp, ':');
  if (chr == NULL) goto _error;
  *chr = 0;
  uint32_t ip = inet_addr(temp);

  temp = chr + 1;
  chr = strchr(temp, ':');
  if (chr == NULL) goto _error;
  *chr = 0;
  short port = htons(atoi(temp));

  temp = chr + 1;
  uint32_t streamId = atoi(temp);

  SAcctObj *pAcct = pConn->pAcct;

  pthread_mutex_lock(&pAcct->mutex);

  pConn = pAcct->pConn;
  while (pConn) {
    if (pConn->ip == ip && pConn->port == port && pConn->pSList) {
      int     i;
      SSDesc *pSDesc = pConn->pSList->sdesc;
      for (i = 0; i < pConn->pSList->numOfStreams; ++i, ++pSDesc) {
        if (pSDesc->streamId == streamId) break;
      }

      if (i < pConn->pSList->numOfStreams) break;
    }

    pConn = pConn->next;
  }

  if (pConn) pConn->streamId = streamId;

  pthread_mutex_unlock(&pAcct->mutex);

  if (pConn == NULL || pConn->pSList == NULL || pConn->pSList->numOfStreams == 0) goto _error;

  mTrace("stream:%s is there, kill it", qidstr);
  return 0;

_error:
  mTrace("stream:%s is not there", qidstr);

  return TSDB_CODE_INVALID_STREAM_ID;
}

int mgmtKillConnection(char *qidstr, SConnObj *pConn) {
  SConnObj *pConn1 = NULL;
  char *    temp, *chr, idstr[64];
  strcpy(idstr, qidstr);

  temp = idstr;
  chr = strchr(temp, ':');
  if (chr == NULL) goto _error;
  *chr = 0;
  uint32_t ip = inet_addr(temp);

  temp = chr + 1;
  short port = htons(atoi(temp));

  SAcctObj *pAcct = pConn->pAcct;

  pthread_mutex_lock(&pAcct->mutex);

  pConn = pAcct->pConn;
  while (pConn) {
    if (pConn->ip == ip && pConn->port == port) {
      // there maybe two connections from a shell
      if (pConn1 == NULL)
        pConn1 = pConn;
      else
        break;
    }

    pConn = pConn->next;
  }

  if (pConn1) pConn1->killConnection = 1;
  if (pConn) pConn->killConnection = 1;

  pthread_mutex_unlock(&pAcct->mutex);

  if (pConn1 == NULL) goto _error;

  mTrace("connection:%s is there, kill it", qidstr);
  return 0;

_error:
  mTrace("connection:%s is not there", qidstr);

  return TSDB_CODE_INVALID_CONNECTION;
}
