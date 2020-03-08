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
#include "tschemautil.h"
#include "mgmtMnode.h"
#include "mgmtUser.h"

int32_t (*mgmtAddMnodeFp)(uint32_t privateIp, uint32_t publicIp) = NULL;
int32_t (*mgmtRemoveMnodeFp)(uint32_t privateIp) = NULL;
int32_t (*mgmtGetMnodesNumFp)() = NULL;
void *  (*mgmtGetNextMnodeFp)(SShowObj *pShow, SSdbPeer **pMnode) = NULL;

int32_t mgmtAddMnode(uint32_t privateIp, uint32_t publicIp) {
  if (mgmtAddMnodeFp) {
    return (*mgmtAddMnodeFp)(privateIp, publicIp);
  } else {
    return 0;
  }
}

int32_t mgmtRemoveMnode(uint32_t privateIp) {
  if (mgmtRemoveMnodeFp) {
    return (*mgmtRemoveMnodeFp)(privateIp);
  } else {
    return 0;
  }
}

static int32_t mgmtGetMnodesNum() {
  if (mgmtGetMnodesNumFp) {
    return (*mgmtGetMnodesNumFp)();
  } else {
    return 1;
  }
}

static void *mgmtGetNextMnode(SShowObj *pShow, SSdbPeer **pMnode) {
  if (mgmtGetNextMnodeFp) {
    return (*mgmtGetNextMnodeFp)(pShow, pMnode);
  } else {
    if (*pMnode == NULL) {
      *pMnode = NULL;
    } else {
      *pMnode = NULL;
    }
  }

  return *pMnode;
}

int32_t mgmtGetMnodeMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SUserObj *pUser = mgmtGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  SSchema *pSchema = tsGetSchema(pMeta);

  pShow->bytes[cols] = 16;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "IP");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "created time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "role");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 16;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "public ip");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = mgmtGetMnodesNum();
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;

  return 0;
}

int32_t mgmtRetrieveMnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t  numOfRows = 0;
  int32_t  cols      = 0;
  SSdbPeer *pMnode   = NULL;
  char     *pWrite;
  char     ipstr[20];

  while (numOfRows < rows) {
    pShow->pNode = mgmtGetNextMnode(pShow, (SSdbPeer **)&pMnode);
    if (pMnode == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, pMnode->ipstr);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pMnode->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, sdbStatusStr[(uint8_t)pMnode->status]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, sdbRoleStr[(uint8_t)pMnode->role]);
    cols++;

    tinet_ntoa(ipstr, pMnode->publicIp);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, ipstr);
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}
