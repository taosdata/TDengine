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
#include "mndMnode.h"
#include "mndDnode.h"
#include "mndShow.h"
#include "mndTrans.h"

#define TSDB_MNODE_VER_NUMBER 1
#define TSDB_MNODE_RESERVE_SIZE 64

static int32_t  mndCreateDefaultMnode(SMnode *pMnode);
static SSdbRaw *mndMnodeActionEncode(SMnodeObj *pObj);
static SSdbRow *mndMnodeActionDecode(SSdbRaw *pRaw);
static int32_t  mndMnodeActionInsert(SSdb *pSdb, SMnodeObj *pObj);
static int32_t  mndMnodeActionDelete(SSdb *pSdb, SMnodeObj *pObj);
static int32_t  mndMnodeActionUpdate(SSdb *pSdb, SMnodeObj *pOldMnode, SMnodeObj *pNewMnode);
static int32_t  mndProcessCreateMnodeReq(SMnodeMsg *pMsg);
static int32_t  mndProcessDropMnodeReq(SMnodeMsg *pMsg);
static int32_t  mndProcessCreateMnodeRsp(SMnodeMsg *pMsg);
static int32_t  mndProcessDropMnodeRsp(SMnodeMsg *pMsg);
static int32_t  mndGetMnodeMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta);
static int32_t  mndRetrieveMnodes(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextMnode(SMnode *pMnode, void *pIter);

int32_t mndInitMnode(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_MNODE,
                     .keyType = SDB_KEY_INT32,
                     .deployFp = (SdbDeployFp)mndCreateDefaultMnode,
                     .encodeFp = (SdbEncodeFp)mndMnodeActionEncode,
                     .decodeFp = (SdbDecodeFp)mndMnodeActionDecode,
                     .insertFp = (SdbInsertFp)mndMnodeActionInsert,
                     .updateFp = (SdbUpdateFp)mndMnodeActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndMnodeActionDelete};

  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CREATE_MNODE, mndProcessCreateMnodeReq);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_DROP_MNODE, mndProcessDropMnodeReq);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CREATE_MNODE_IN_RSP, mndProcessCreateMnodeRsp);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_DROP_MNODE_IN_RSP, mndProcessDropMnodeRsp);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_MNODE, mndGetMnodeMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_MNODE, mndRetrieveMnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_MNODE, mndCancelGetNextMnode);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupMnode(SMnode *pMnode) {}

static SMnodeObj *mndAcquireMnode(SMnode *pMnode, int32_t mnodeId) {
  SSdb      *pSdb = pMnode->pSdb;
  SMnodeObj *pObj = sdbAcquire(pSdb, SDB_MNODE, &mnodeId);
  if (pObj == NULL) {
    terrno = TSDB_CODE_MND_MNODE_NOT_EXIST;
  }
  return pObj;
}

static void mndReleaseMnode(SMnode *pMnode, SMnodeObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

char *mndGetRoleStr(int32_t showType) {
  switch (showType) {
    case TAOS_SYNC_STATE_FOLLOWER:
      return "unsynced";
    case TAOS_SYNC_STATE_CANDIDATE:
      return "slave";
    case TAOS_SYNC_STATE_LEADER:
      return "master";
    default:
      return "undefined";
  }
}

static int32_t mndCreateDefaultMnode(SMnode *pMnode) {
  SMnodeObj mnodeObj = {0};
  mnodeObj.id = 1;
  mnodeObj.createdTime = taosGetTimestampMs();
  mnodeObj.updateTime = mnodeObj.createdTime;

  SSdbRaw *pRaw = mndMnodeActionEncode(&mnodeObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mDebug("mnode:%d, will be created while deploy sdb", mnodeObj.id);
  return sdbWrite(pMnode->pSdb, pRaw);
}

static SSdbRaw *mndMnodeActionEncode(SMnodeObj *pObj) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_MNODE, TSDB_MNODE_VER_NUMBER, sizeof(SMnodeObj) + TSDB_MNODE_RESERVE_SIZE);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pObj->id);
  SDB_SET_INT64(pRaw, dataPos, pObj->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pObj->updateTime)
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_MNODE_RESERVE_SIZE)

  return pRaw;
}

static SSdbRow *mndMnodeActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != TSDB_MNODE_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("failed to decode mnode since %s", terrstr());
    return NULL;
  }

  SSdbRow   *pRow = sdbAllocRow(sizeof(SMnodeObj));
  SMnodeObj *pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, pRow, dataPos, &pObj->id)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pObj->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pObj->updateTime)
  SDB_GET_RESERVE(pRaw, pRow, dataPos, TSDB_MNODE_RESERVE_SIZE)

  return pRow;
}

static void mnodeResetMnode(SMnodeObj *pObj) { pObj->role = TAOS_SYNC_STATE_FOLLOWER; }

static int32_t mndMnodeActionInsert(SSdb *pSdb, SMnodeObj *pObj) {
  mTrace("mnode:%d, perform insert action", pObj->id);
  pObj->pDnode = sdbAcquire(pSdb, SDB_DNODE, &pObj->id);
  if (pObj->pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    mError("mnode:%d, failed to perform insert action since %s", pObj->id, terrstr());
    return -1;
  }

  mnodeResetMnode(pObj);
  return 0;
}

static int32_t mndMnodeActionDelete(SSdb *pSdb, SMnodeObj *pObj) {
  mTrace("mnode:%d, perform delete action", pObj->id);
  if (pObj->pDnode != NULL) {
    sdbRelease(pSdb, pObj->pDnode);
    pObj->pDnode = NULL;
  }

  return 0;
}

static int32_t mndMnodeActionUpdate(SSdb *pSdb, SMnodeObj *pOldMnode, SMnodeObj *pNewMnode) {
  mTrace("mnode:%d, perform update action", pOldMnode->id);
  pOldMnode->updateTime = pNewMnode->updateTime;
  return 0;
}

bool mndIsMnode(SMnode *pMnode, int32_t dnodeId) {
  SSdb *pSdb = pMnode->pSdb;

  SMnodeObj *pObj = sdbAcquire(pSdb, SDB_MNODE, &dnodeId);
  if (pObj == NULL) {
    return false;
  }

  sdbRelease(pSdb, pObj);
  return true;
}

void mndGetMnodeEpSet(SMnode *pMnode, SEpSet *pEpSet) {
  SSdb *pSdb = pMnode->pSdb;

  pEpSet->numOfEps = 0;

  void *pIter = NULL;
  while (1) {
    SMnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;
    if (pObj->pDnode == NULL) break;

    pEpSet->port[pEpSet->numOfEps] = htons(pObj->pDnode->port);
    memcpy(pEpSet->fqdn[pEpSet->numOfEps], pObj->pDnode->fqdn, TSDB_FQDN_LEN);
    if (pObj->role == TAOS_SYNC_STATE_LEADER) {
      pEpSet->inUse = pEpSet->numOfEps;
    }

    pEpSet->numOfEps++;
  }
}

static int32_t mndCreateMnode(SMnode *pMnode, SMnodeMsg *pMsg, SCreateMnodeMsg *pCreate) {
  SMnodeObj mnodeObj = {0};
  mnodeObj.id = sdbGetMaxId(pMnode->pSdb, SDB_MNODE);
  mnodeObj.createdTime = taosGetTimestampMs();
  mnodeObj.updateTime = mnodeObj.createdTime;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, pMsg->rpcMsg.handle);
  if (pTrans == NULL) {
    mError("dnode:%d, failed to create since %s", pCreate->dnodeId, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to create dnode:%d", pTrans->id, pCreate->dnodeId);

  SSdbRaw *pRedoRaw = mndMnodeActionEncode(&mnodeObj);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING);

  SSdbRaw *pUndoRaw = mndMnodeActionEncode(&mnodeObj);
  if (pUndoRaw == NULL || mndTransAppendUndolog(pTrans, pUndoRaw) != 0) {
    mError("trans:%d, failed to append undo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED);

  SSdbRaw *pCommitRaw = mndMnodeActionEncode(&mnodeObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessCreateMnodeReq(SMnodeMsg *pMsg) {
  SMnode          *pMnode = pMsg->pMnode;
  SCreateMnodeMsg *pCreate = pMsg->rpcMsg.pCont;

  pCreate->dnodeId = htonl(pCreate->dnodeId);

  mDebug("mnode:%d, start to create", pCreate->dnodeId);

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pCreate->dnodeId);
  if (pDnode == NULL) {
    mError("mnode:%d, dnode not exist", pDnode->id);
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    return -1;
  }
  mndReleaseDnode(pMnode, pDnode);

  SMnodeObj *pObj = mndAcquireMnode(pMnode, pCreate->dnodeId);
  if (pObj != NULL) {
    mError("mnode:%d, mnode already exist", pObj->id);
    terrno = TSDB_CODE_MND_MNODE_ALREADY_EXIST;
    return -1;
  }

  int32_t code = mndCreateMnode(pMnode, pMsg, pCreate);

  if (code != 0) {
    mError("mnode:%d, failed to create since %s", pCreate->dnodeId, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndDropMnode(SMnode *pMnode, SMnodeMsg *pMsg, SMnodeObj *pObj) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, pMsg->rpcMsg.handle);
  if (pTrans == NULL) {
    mError("mnode:%d, failed to drop since %s", pObj->id, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to drop user:%d", pTrans->id, pObj->id);

  SSdbRaw *pRedoRaw = mndMnodeActionEncode(pObj);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING);

  SSdbRaw *pUndoRaw = mndMnodeActionEncode(pObj);
  if (pUndoRaw == NULL || mndTransAppendUndolog(pTrans, pUndoRaw) != 0) {
    mError("trans:%d, failed to append undo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pUndoRaw, SDB_STATUS_READY);

  SSdbRaw *pCommitRaw = mndMnodeActionEncode(pObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessDropMnodeReq(SMnodeMsg *pMsg) {
  SMnode        *pMnode = pMsg->pMnode;
  SDropMnodeMsg *pDrop = pMsg->rpcMsg.pCont;
  pDrop->dnodeId = htonl(pDrop->dnodeId);

  mDebug("mnode:%d, start to drop", pDrop->dnodeId);

  if (pDrop->dnodeId <= 0) {
    terrno = TSDB_CODE_SDB_APP_ERROR;
    mError("mnode:%d, failed to drop since %s", pDrop->dnodeId, terrstr());
    return -1;
  }

  SMnodeObj *pObj = mndAcquireMnode(pMnode, pDrop->dnodeId);
  if (pObj == NULL) {
    mError("mnode:%d, not exist", pDrop->dnodeId);
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    return -1;
  }

  int32_t code = mndDropMnode(pMnode, pMsg, pObj);

  if (code != 0) {
    mError("mnode:%d, failed to drop since %s", pMnode->dnodeId, terrstr());
    return -1;
  }

  sdbRelease(pMnode->pSdb, pMnode);
  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessCreateMnodeRsp(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessDropMnodeRsp(SMnodeMsg *pMsg) { return 0; }

static int32_t mndGetMnodeMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchema;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_EP_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "endpoint");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 12 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "role");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "role_time");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htonl(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_MNODE);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveMnodes(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode    *pMnode = pMsg->pMnode;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  SMnodeObj *pObj = NULL;
  char      *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_MNODE, pShow->pIter, (void **)&pObj);
    if (pShow->pIter == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pObj->id;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pObj->pDnode->ep, pShow->bytes[cols]);

    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    char *roles = mndGetRoleStr(pObj->role);
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, roles, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pObj->roleTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pObj->createdTime;
    cols++;

    numOfRows++;
    sdbRelease(pSdb, pObj);
  }

  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextMnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
