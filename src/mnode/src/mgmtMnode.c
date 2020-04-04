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
#include "taoserror.h"
#include "tstatus.h"
#include "trpc.h"
#include "mgmtMnode.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtUser.h"

#ifndef _MPEER

static SMnodeObj tsMnodeObj = {0};
static int32_t mgmtGetMnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mgmtRetrieveMnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);

int32_t mgmtInitMnodes() {
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_MNODE, mgmtGetMnodeMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_MNODE, mgmtRetrieveMnodes);

  tsMnodeObj.mnodeId     = 1;
  tsMnodeObj.privateIp   = inet_addr(tsPrivateIp);
  tsMnodeObj.publicIp    = inet_addr(tsPublicIp);
  tsMnodeObj.createdTime = taosGetTimestampMs();
  tsMnodeObj.role        = TSDB_MN_ROLE_MASTER;
  tsMnodeObj.status      = TSDB_MN_STATUS_SERVING;
  tsMnodeObj.numOfMnodes = 1;
  sprintf(tsMnodeObj.mnodeName, "%d", tsMnodeObj.mnodeId);

  return TSDB_CODE_SUCCESS;
}

void mgmtCleanupMnodes() {}
bool mgmtInServerStatus() { return tsMnodeObj.status == TSDB_MN_STATUS_SERVING; }
bool mgmtIsMaster() { return tsMnodeObj.role == TSDB_MN_ROLE_MASTER; }
bool mgmtCheckRedirect(void *handle) { return false; }

static int32_t mgmtGetMnodesNum() {
  return 1;
}

static void *mgmtGetNextMnode(void *pNode, SMnodeObj **pMnode) {
  if (*pMnode == NULL) {
    *pMnode = &tsMnodeObj;
  } else {
    *pMnode = NULL;
  }

  return *pMnode;
}

static int32_t mgmtGetMnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mgmtGetUserFromConn(pConn, NULL);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->pAcct->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 16;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "private ip");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 16;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "public ip");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create time");
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

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = mgmtGetDnodesNum();
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;

  return 0;
}

static int32_t mgmtRetrieveMnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  int32_t    cols      = 0;
  SMnodeObj *pMnode   = NULL;
  char      *pWrite;
  char       ipstr[32];

  while (numOfRows < rows) {
    pShow->pNode = mgmtGetNextMnode(pShow->pNode, (SMnodeObj **)&pMnode);
    if (pMnode == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pMnode->mnodeId;
    cols++;

    tinet_ntoa(ipstr, pMnode->privateIp);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, ipstr);
    cols++;

    tinet_ntoa(ipstr, pMnode->publicIp);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, ipstr);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pMnode->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, taosGetMnodeStatusStr(pMnode->status));
    cols++;

     pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, taosGetMnodeRoleStr(pMnode->role));
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

void mgmtGetMnodePrivateIpList(SRpcIpSet *ipSet) {
  ipSet->inUse = 0;
  ipSet->port = htons(tsMnodeDnodePort);
  ipSet->numOfIps = 1;
  ipSet->ip[0] = htonl(tsMnodeObj.privateIp);
}

void mgmtGetMnodePublicIpList(SRpcIpSet *ipSet) {
  ipSet->inUse = 0;
  ipSet->port = htons(tsMnodeDnodePort);
  ipSet->numOfIps = 1;
  ipSet->ip[0] = htonl(tsMnodeObj.publicIp);
}

#endif