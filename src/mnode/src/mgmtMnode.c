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
#include "trpc.h"
#include "mgmtMnode.h"
#include "mgmtSdb.h"
#include "mgmtUser.h"

int32_t (*mpeerAddMnodeFp)(uint32_t privateIp, uint32_t publicIp) = NULL;
int32_t (*mpeerRemoveMnodeFp)(uint32_t privateIp) = NULL;
int32_t (*mpeerGetMnodesNumFp)() = NULL;
void *  (*mpeerGetNextMnodeFp)(SShowObj *pShow, SMnodeObj **pMnode) = NULL;
int32_t (*mpeerInitMnodesFp)() = NULL;
void    (*mpeerCleanUpMnodesFp)() = NULL;

static SMnodeObj tsMnodeObj = {0};
static bool tsMnodeIsMaster = false;
static bool tsMnodeIsServing = false;
static int32_t mgmtGetMnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mgmtRetrieveMnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static char *mgmtMnodeStatusStr[] = {
  "offline",
  "unsynced",
  "syncing",
  "serving",
  "null"
};

static char *mgmtMnodeRoleStr[] = {
  "unauthed",
  "undecided",
  "master",
  "slave",
  "null"
};

int32_t mgmtInitMnodes() {
  if (mpeerInitMnodesFp) {
    return (*mpeerInitMnodesFp)();
  } else {
    tsMnodeIsServing = true;
    tsMnodeIsMaster = true;
    return 0;
  }
}

void mgmtCleanupMnodes() {
  if (mpeerCleanUpMnodesFp) {
    (*mpeerCleanUpMnodesFp)();
  }
}

bool mgmtInServerStatus() {
  return tsMnodeIsServing; 
}
   
bool mgmtIsMaster() { 
  return tsMnodeIsMaster; 
}

bool mgmtCheckRedirect(void *handle) {
  return false;
}

int32_t mgmtAddMnode(uint32_t privateIp, uint32_t publicIp) {
  if (mpeerAddMnodeFp) {
    return (*mpeerAddMnodeFp)(privateIp, publicIp);
  } else {
    return 0;
  }
}

int32_t mgmtRemoveMnode(uint32_t privateIp) {
  if (mpeerRemoveMnodeFp) {
    return (*mpeerRemoveMnodeFp)(privateIp);
  } else {
    return 0;
  }
}

static int32_t mgmtGetMnodesNum() {
  if (mpeerGetMnodesNumFp) {
    return (*mpeerGetMnodesNumFp)();
  } else {
    return 1;
  }
}

static void *mgmtGetNextMnode(SShowObj *pShow, SMnodeObj **pMnode) {
  if (mpeerGetNextMnodeFp) {
    return (*mpeerGetNextMnodeFp)(pShow, pMnode);
  } else {
    if (*pMnode == NULL) {
      *pMnode = &tsMnodeObj;
    } else {
      *pMnode = NULL;
    }
  }

  return *pMnode;
}

static int32_t mgmtGetMnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SUserObj *pUser = mgmtGetUserFromConn(pConn, NULL);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 16;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "private ip");
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

static int32_t mgmtRetrieveMnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t  numOfRows = 0;
  int32_t  cols      = 0;
  SMnodeObj *pMnode   = NULL;
  char     *pWrite;
  char     ipstr[32];

  while (numOfRows < rows) {
    pShow->pNode = mgmtGetNextMnode(pShow, (SMnodeObj **)&pMnode);
    if (pMnode == NULL) break;

    cols = 0;

    tinet_ntoa(ipstr, pMnode->privateIp);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, ipstr);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pMnode->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, mgmtMnodeStatusStr[pMnode->status]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, mgmtMnodeRoleStr[pMnode->role]);
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

void mgmtGetMnodePrivateIpList(SRpcIpSet *ipSet) {
  ipSet->inUse = 0;
  ipSet->port = htons(tsMnodeDnodePort);
  ipSet->numOfIps = 1;
  ipSet->ip[0] = htonl(inet_addr(tsMasterIp));
}

void mgmtGetMnodePublicIpList(SRpcIpSet *ipSet) {
  ipSet->inUse = 0;
  ipSet->port = htons(tsMnodeDnodePort);
  ipSet->numOfIps = 1;
  ipSet->ip[0] = htonl(inet_addr(tsMasterIp));
}