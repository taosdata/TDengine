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
#include "mndTrans.h"

#define SDB_DNODE_VER 1

static SSdbRaw *mndDnodeActionEncode(SDnodeObj *pDnode) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_USER, SDB_DNODE_VER, sizeof(SDnodeObj));
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pDnode->id);
  SDB_SET_INT64(pRaw, dataPos, pDnode->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pDnode->updateTime)
  SDB_SET_INT16(pRaw, dataPos, pDnode->port)
  SDB_SET_BINARY(pRaw, dataPos, pDnode->fqdn, TSDB_FQDN_LEN)
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

static SSdbRow *mndDnodeActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != SDB_DNODE_VER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("failed to decode dnode since %s", terrstr());
    return NULL;
  }

  SSdbRow   *pRow = sdbAllocRow(sizeof(SDnodeObj));
  SDnodeObj *pDnode = sdbGetRowObj(pRow);
  if (pDnode == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, pRow, dataPos, &pDnode->id)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pDnode->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pDnode->updateTime)
  SDB_GET_INT16(pRaw, pRow, dataPos, &pDnode->port)
  SDB_GET_BINARY(pRaw, pRow, dataPos, pDnode->fqdn, TSDB_FQDN_LEN)

  return pRow;
}

static void mnodeResetDnode(SDnodeObj *pDnode) {
  pDnode->rebootTime = 0;
  pDnode->lastAccessTime = 0;
  pDnode->numOfMnodes = 0;
  pDnode->numOfVnodes = 0;
  pDnode->numOfQnodes = 0;
  pDnode->numOfSupportMnodes = 0;
  pDnode->numOfSupportVnodes = 0;
  pDnode->numOfSupportQnodes = 0;
  pDnode->status = DND_STATUS_OFFLINE;
  pDnode->offlineReason = DND_REASON_RESET_BY_MNODE;
  snprintf(pDnode->ep, TSDB_EP_LEN, "%s:%u", pDnode->fqdn, pDnode->port);
}

static int32_t mndDnodeActionInsert(SSdb *pSdb, SDnodeObj *pDnode) {
  mnodeResetDnode(pDnode);
  return 0;
}

static int32_t mndDnodeActionDelete(SSdb *pSdb, SDnodeObj *pDnode) { return 0; }

static int32_t mndDnodeActionUpdate(SSdb *pSdb, SDnodeObj *pSrcDnode, SDnodeObj *pDstDnode) {
  pSrcDnode->id = pDstDnode->id;
  pSrcDnode->createdTime = pDstDnode->createdTime;
  pSrcDnode->updateTime = pDstDnode->updateTime;
  pSrcDnode->port = pDstDnode->port;
  memcpy(pSrcDnode->fqdn, pDstDnode->fqdn, TSDB_FQDN_LEN);
  mnodeResetDnode(pSrcDnode);
}

static int32_t mndCreateDefaultDnode(SMnode *pMnode) {
  SDnodeObj dnodeObj = {0};
  dnodeObj.id = 0;
  dnodeObj.createdTime = taosGetTimestampMs();
  dnodeObj.updateTime = dnodeObj.createdTime;
  dnodeObj.port = pMnode->replicas[0].port;
  memcpy(&dnodeObj.fqdn, pMnode->replicas[0].fqdn, TSDB_FQDN_LEN);

  SSdbRaw *pRaw = mndDnodeActionEncode(&dnodeObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  return sdbWrite(pMnode->pSdb, pRaw);
}

static int32_t mndProcessCreateDnodeMsg(SMnode *pMnode, SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessDropDnodeMsg(SMnode *pMnode, SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessConfigDnodeMsg(SMnode *pMnode, SMnodeMsg *pMsg) { return 0; }

int32_t mndInitDnode(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_USER,
                     .keyType = SDB_KEY_BINARY,
                     .deployFp = (SdbDeployFp)mndCreateDefaultDnode,
                     .encodeFp = (SdbEncodeFp)mndDnodeActionEncode,
                     .decodeFp = (SdbDecodeFp)mndDnodeActionDecode,
                     .insertFp = (SdbInsertFp)mndDnodeActionInsert,
                     .updateFp = (SdbUpdateFp)mndDnodeActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndDnodeActionDelete};

  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CREATE_DNODE, mndProcessCreateDnodeMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_DROP_DNODE, mndProcessDropDnodeMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CONFIG_DNODE, mndProcessConfigDnodeMsg);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupDnode(SMnode *pMnode) {}