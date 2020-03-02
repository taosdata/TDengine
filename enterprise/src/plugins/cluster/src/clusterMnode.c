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

#include "mnode.h"
#include "sdb.h"
#include "tschemautil.h"

int mgmtGetMnodeMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn) {
  int cols = 0;

  if (strcmp(pConn->pAcct->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

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
  for (int i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = sdbGetNumOfRows(mnodeSdb);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

int mgmtRetrieveMnodes(SShowObj *pShow, char *data, int rows, void *pConn) {
  int       numOfRows = 0;
  SSdbPeer *pMnode = NULL;
  char *    pWrite;
  int       cols = 0;
  char       ipstr[20];

  while (numOfRows < rows) {
    pShow->pNode = sdbFetchRow(mnodeSdb, pShow->pNode, (void **)&pMnode);
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


int mgmtProcessCfgMnodeMsg(char *pMsg, int msgLen, void *pConn) {
  int      code = 0;
  SCfgDnodeMsg *pCfg = (SCfgDnodeMsg *)pMsg;

  if (!sdbMaster) return mgmtRedirectMsg(pConn, TSDB_MSG_TYPE_CFG_MNODE_RSP);

  if (strcmp(pConn->pAcct->user, "root") != 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = sdbCfgNode(pMsg);
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_CFG_MNODE_RSP, code);

  if (code == 0) mTrace("mnode:%s is configured by %s", pCfg->ip, pConn->pUser->user);

  return 0;
}

int mgmtProcessDropMnodeMsg(char *pMsg, int msgLen, void *pConn) {
  SDropMnodeMsg *pDrop = (SDropMnodeMsg *)pMsg;
  int            code = 0;

  if (!sdbMaster) return mgmtRedirectMsg(pConn, TSDB_MSG_TYPE_DROP_MNODE_RSP);

  if (strcmp(pConn->pUser->user, "root") != 0) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    // code = sdbRemovePeerByIp(inet_addr(pDrop->ip));
    SDnodeObj *pDnode = mgmtGetDnode(inet_addr(pDrop->ip));
    if (pDnode != NULL) {
      code = mgmtUnSetModuleInDnode(pDnode, TSDB_MOD_MGMT);
    }
  }

  taosSendSimpleRsp(pConn->thandle, TSDB_MSG_TYPE_DROP_MNODE_RSP, code);

  if (code == 0) {
    mLPrint("Mnode:%s is dropped by %s", pDrop->ip, pConn->pUser->user);
  }

  return 0;
}