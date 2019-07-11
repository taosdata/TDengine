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
#include "taosmsg.h"
#include "tschemautil.h"

typedef struct {
  char     user[TSDB_METER_ID_LEN];
  uint64_t stime;
  uint32_t ip;
  short    port;
} SConnInfo;

typedef struct {
  int       numOfConns;
  int       index;
  SConnInfo connInfo[];
} SConnShow;

int mgmtGetConns(SShowObj *pShow, SConnObj *pConn) {
  SAcctObj * pAcct = pConn->pAcct;
  SConnShow *pConnShow;

  pthread_mutex_lock(&pAcct->mutex);

  pConnShow = malloc(sizeof(SConnInfo) * pAcct->acctInfo.numOfConns + sizeof(SConnShow));
  pConnShow->index = 0;
  pConnShow->numOfConns = 0;

  if (pAcct->acctInfo.numOfConns > 0) {
    pConn = pAcct->pConn;
    SConnInfo *pConnInfo = pConnShow->connInfo;

    while (pConn) {
      strcpy(pConnInfo->user, pConn->pUser->user);
      pConnInfo->ip = pConn->ip;
      pConnInfo->port = pConn->port;
      pConnInfo->stime = pConn->stime;

      pConnShow->numOfConns++;
      pConnInfo++;
      pConn = pConn->next;
    }
  }

  pthread_mutex_unlock(&pAcct->mutex);

  // sorting based on useconds

  pShow->pNode = pConnShow;

  return 0;
}

int mgmtGetConnsMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) {
  int cols = 0;

  pShow->bytes[cols] = TSDB_METER_NAME_LEN;
  SSchema *pSchema = tsGetSchema(pMeta);

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

int mgmtRetrieveConns(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int   numOfRows = 0;
  char *pWrite;
  int   cols = 0;

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
