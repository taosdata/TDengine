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
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tname.h"

#define MND_TOPIC_VER_NUMBER 1
#define MND_TOPIC_RESERVE_SIZE 64

static SSdbRaw *mndTopicActionEncode(STopicObj *pTopic);
static SSdbRow *mndTopicActionDecode(SSdbRaw *pRaw);
static int32_t  mndTopicActionInsert(SSdb *pSdb, STopicObj *pTopic);
static int32_t  mndTopicActionDelete(SSdb *pSdb, STopicObj *pTopic);
static int32_t  mndTopicActionUpdate(SSdb *pSdb, STopicObj *pTopic, STopicObj *pNewTopic);
static int32_t  mndProcessCreateTopicMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessAlterTopicMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessDropTopicMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessCreateTopicInRsp(SMnodeMsg *pMsg);
static int32_t  mndProcessAlterTopicInRsp(SMnodeMsg *pMsg);
static int32_t  mndProcessDropTopicInRsp(SMnodeMsg *pMsg);
static int32_t  mndProcessTopicMetaMsg(SMnodeMsg *pMsg);
static int32_t  mndGetTopicMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta);
static int32_t  mndRetrieveTopic(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextTopic(SMnode *pMnode, void *pIter);

int32_t mndInitTopic(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_TOPIC,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndTopicActionEncode,
                     .decodeFp = (SdbDecodeFp)mndTopicActionDecode,
                     .insertFp = (SdbInsertFp)mndTopicActionInsert,
                     .updateFp = (SdbUpdateFp)mndTopicActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndTopicActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_TOPIC, mndProcessCreateTopicMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_TOPIC, mndProcessAlterTopicMsg);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_TOPIC, mndProcessDropTopicMsg);
  mndSetMsgHandle(pMnode, TDMT_VND_CREATE_TOPIC_RSP, mndProcessCreateTopicInRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_ALTER_TOPIC_RSP, mndProcessAlterTopicInRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_DROP_TOPIC_RSP, mndProcessDropTopicInRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_TABLE_META, mndProcessTopicMetaMsg);

  /*mndAddShowMetaHandle(pMnode, TSDB_MGMT_TOPIC, mndGetTopicMeta);*/
  /*mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TOPIC, mndRetrieveTopic);*/
  /*mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TOPIC, mndCancelGetNextTopic);*/

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_TOPIC, mndProcessCreateTopicMsg);
  mndSetMsgHandle(pMnode, TDMT_VND_CREATE_TOPIC_RSP, mndProcessCreateTopicInRsp);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupTopic(SMnode *pMnode) {}

static SSdbRaw *mndTopicActionEncode(STopicObj *pTopic) {
  int32_t  size = sizeof(STopicObj) + MND_TOPIC_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_TOPIC, MND_TOPIC_VER_NUMBER, size);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pTopic->name, TSDB_TABLE_FNAME_LEN);
  SDB_SET_BINARY(pRaw, dataPos, pTopic->db, TSDB_DB_FNAME_LEN);
  SDB_SET_INT64(pRaw, dataPos, pTopic->createTime);
  SDB_SET_INT64(pRaw, dataPos, pTopic->updateTime);
  SDB_SET_INT64(pRaw, dataPos, pTopic->uid);
  SDB_SET_INT64(pRaw, dataPos, pTopic->dbUid);
  SDB_SET_INT32(pRaw, dataPos, pTopic->version);
  SDB_SET_INT32(pRaw, dataPos, pTopic->execLen);
  SDB_SET_BINARY(pRaw, dataPos, pTopic->executor, pTopic->execLen);
  SDB_SET_INT32(pRaw, dataPos, pTopic->sqlLen);
  SDB_SET_BINARY(pRaw, dataPos, pTopic->sql, pTopic->sqlLen);

  SDB_SET_RESERVE(pRaw, dataPos, MND_TOPIC_RESERVE_SIZE);
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

static SSdbRow *mndTopicActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != MND_TOPIC_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("failed to decode topic since %s", terrstr());
    return NULL;
  }

  int32_t    size = sizeof(STopicObj) + TSDB_MAX_COLUMNS * sizeof(SSchema);
  SSdbRow   *pRow = sdbAllocRow(size);
  STopicObj *pTopic = sdbGetRowObj(pRow);
  if (pTopic == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, pRow, dataPos, pTopic->name, TSDB_TABLE_FNAME_LEN);
  SDB_GET_BINARY(pRaw, pRow, dataPos, pTopic->db, TSDB_DB_FNAME_LEN);
  SDB_GET_INT64(pRaw, pRow, dataPos, &pTopic->createTime);
  SDB_GET_INT64(pRaw, pRow, dataPos, &pTopic->updateTime);
  SDB_GET_INT64(pRaw, pRow, dataPos, &pTopic->uid);
  SDB_GET_INT64(pRaw, pRow, dataPos, &pTopic->dbUid);
  SDB_GET_INT32(pRaw, pRow, dataPos, &pTopic->version);
  SDB_GET_INT32(pRaw, pRow, dataPos, &pTopic->execLen);
  SDB_GET_BINARY(pRaw, pRow, dataPos, pTopic->executor, pTopic->execLen);
  SDB_GET_INT32(pRaw, pRow, dataPos, &pTopic->sqlLen);
  SDB_GET_BINARY(pRaw, pRow, dataPos, pTopic->sql, pTopic->sqlLen);

  SDB_GET_RESERVE(pRaw, pRow, dataPos, MND_TOPIC_RESERVE_SIZE);

  return pRow;
}

static int32_t mndTopicActionInsert(SSdb *pSdb, STopicObj *pTopic) {
  mTrace("topic:%s, perform insert action", pTopic->name);
  return 0;
}

static int32_t mndTopicActionDelete(SSdb *pSdb, STopicObj *pTopic) {
  mTrace("topic:%s, perform delete action", pTopic->name);
  return 0;
}

static int32_t mndTopicActionUpdate(SSdb *pSdb, STopicObj *pOldTopic, STopicObj *pNewTopic) {
  mTrace("topic:%s, perform update action", pOldTopic->name);
  atomic_exchange_32(&pOldTopic->updateTime, pNewTopic->updateTime);
  atomic_exchange_32(&pOldTopic->version, pNewTopic->version);

  taosWLockLatch(&pOldTopic->lock);
#if 0

  pOldTopic->numOfColumns = pNewTopic->numOfColumns;
  pOldTopic->numOfTags = pNewTopic->numOfTags;
  int32_t totalCols = pNewTopic->numOfTags + pNewTopic->numOfColumns;
  int32_t totalSize = totalCols * sizeof(SSchema);

  if (pOldTopic->numOfTags + pOldTopic->numOfColumns < totalCols) {
    void *pSchema = malloc(totalSize);
    if (pSchema != NULL) {
      free(pOldTopic->pSchema);
      pOldTopic->pSchema = pSchema;
    }
  }

  memcpy(pOldTopic->pSchema, pNewTopic->pSchema, totalSize);

#endif
  taosWUnLockLatch(&pOldTopic->lock);
  return 0;
}

STopicObj *mndAcquireTopic(SMnode *pMnode, char *topicName) {
  SSdb      *pSdb = pMnode->pSdb;
  STopicObj *pTopic = sdbAcquire(pSdb, SDB_TOPIC, topicName);
  if (pTopic == NULL) {
    terrno = TSDB_CODE_MND_TOPIC_NOT_EXIST;
  }
  return pTopic;
}

void mndReleaseTopic(SMnode *pMnode, STopicObj *pTopic) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pTopic);
}

static SDbObj *mndAcquireDbByTopic(SMnode *pMnode, char *topicName) {
  SName name = {0};
  tNameFromString(&name, topicName, T_NAME_ACCT | T_NAME_DB | T_NAME_TOPIC);

  char db[TSDB_TABLE_FNAME_LEN] = {0};
  tNameGetFullDbName(&name, db);

  return mndAcquireDb(pMnode, db);
}

static SCreateTopicInternalMsg *mndBuildCreateTopicMsg(SMnode *pMnode, SVgObj *pVgroup, STopicObj *pTopic) {
  int32_t totalCols = 0;
  int32_t contLen = sizeof(SCreateTopicInternalMsg) + pTopic->execLen + pTopic->sqlLen;

  SCreateTopicInternalMsg *pCreate = calloc(1, contLen);
  if (pCreate == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pCreate->head.contLen = htonl(contLen);
  pCreate->head.vgId = htonl(pVgroup->vgId);
  memcpy(pCreate->name, pTopic->name, TSDB_TABLE_FNAME_LEN);
  pCreate->tuid = htobe64(pTopic->uid);
  pCreate->sverson = htonl(pTopic->version);

  pCreate->sql = malloc(pTopic->sqlLen);
  if (pCreate->sql == NULL) {
    free(pCreate);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  memcpy(pCreate->sql, pTopic->sql, pTopic->sqlLen);

  pCreate->executor = malloc(pTopic->execLen);
  if (pCreate->executor == NULL) {
    free(pCreate);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  memcpy(pCreate->executor, pTopic->executor, pTopic->execLen);

  return pCreate;
}

static SDropTopicInternalMsg *mndBuildDropTopicMsg(SMnode *pMnode, SVgObj *pVgroup, STopicObj *pTopic) {
  int32_t contLen = sizeof(SDropTopicInternalMsg);

  SDropTopicInternalMsg *pDrop = calloc(1, contLen);
  if (pDrop == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pDrop->head.contLen = htonl(contLen);
  pDrop->head.vgId = htonl(pVgroup->vgId);
  memcpy(pDrop->name, pTopic->name, TSDB_TABLE_FNAME_LEN);
  pDrop->tuid = htobe64(pTopic->uid);

  return pDrop;
}

static int32_t mndCheckCreateTopicMsg(SCreateTopicMsg *pCreate) {
  // deserialize and other stuff
  return 0;
}

static int32_t mndSetCreateTopicRedoLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, STopicObj *pTopic) {
  SSdbRaw *pRedoRaw = mndTopicActionEncode(pTopic);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING) != 0) return -1;

  return 0;
}

static int32_t mndSetCreateTopicUndoLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, STopicObj *pTopic) {
  SSdbRaw *pUndoRaw = mndTopicActionEncode(pTopic);
  if (pUndoRaw == NULL) return -1;
  if (mndTransAppendUndolog(pTrans, pUndoRaw) != 0) return -1;
  if (sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED) != 0) return -1;

  return 0;
}

static int32_t mndSetCreateTopicCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, STopicObj *pTopic) {
  SSdbRaw *pCommitRaw = mndTopicActionEncode(pTopic);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;

  return 0;
}

static int32_t mndSetCreateTopicRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, STopicObj *pTopic) {
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pDb->uid) continue;

    SCreateTopicInternalMsg *pMsg = mndBuildCreateTopicMsg(pMnode, pVgroup, pTopic);
    if (pMsg == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pMsg;
    action.contLen = htonl(pMsg->head.contLen);
    action.msgType = TDMT_VND_CREATE_TOPIC;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      free(pMsg);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      return -1;
    }
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndSetCreateTopicUndoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, STopicObj *pTopic) {
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pDb->uid) continue;

    SDropTopicInternalMsg *pMsg = mndBuildDropTopicMsg(pMnode, pVgroup, pTopic);
    if (pMsg == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pMsg;
    action.contLen = sizeof(SDropTopicInternalMsg);
    action.msgType = TDMT_VND_DROP_TOPIC;
    if (mndTransAppendUndoAction(pTrans, &action) != 0) {
      free(pMsg);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      return -1;
    }
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndCreateTopic(SMnode *pMnode, SMnodeMsg *pMsg, SCreateTopicMsg *pCreate, SDbObj *pDb) {
  STopicObj topicObj = {0};
  tstrncpy(topicObj.name, pCreate->name, TSDB_TABLE_FNAME_LEN);
  tstrncpy(topicObj.db, pDb->name, TSDB_DB_FNAME_LEN);
  topicObj.createTime = taosGetTimestampMs();
  topicObj.updateTime = topicObj.createTime;
  topicObj.uid = mndGenerateUid(pCreate->name, TSDB_TABLE_FNAME_LEN);
  topicObj.dbUid = pDb->uid;
  topicObj.version = 1;

#if 0
  int32_t totalCols = topicObj.numOfColumns + topicObj.numOfTags;
  int32_t totalSize = totalCols * sizeof(SSchema);
  topicObj.sql = malloc(totalSize);
  if (topicObj.sql == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  memcpy(topicObj.sql, pCreate->sql, totalSize);
#endif

  int32_t code = 0;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    mError("topic:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to create topic:%s", pTrans->id, pCreate->name);

  if (mndSetCreateTopicRedoLogs(pMnode, pTrans, pDb, &topicObj) != 0) {
    mError("trans:%d, failed to set redo log since %s", pTrans->id, terrstr());
    goto CREATE_TOPIC_OVER;
  }

  if (mndSetCreateTopicUndoLogs(pMnode, pTrans, pDb, &topicObj) != 0) {
    mError("trans:%d, failed to set undo log since %s", pTrans->id, terrstr());
    goto CREATE_TOPIC_OVER;
  }

  if (mndSetCreateTopicCommitLogs(pMnode, pTrans, pDb, &topicObj) != 0) {
    mError("trans:%d, failed to set commit log since %s", pTrans->id, terrstr());
    goto CREATE_TOPIC_OVER;
  }

  if (mndSetCreateTopicRedoActions(pMnode, pTrans, pDb, &topicObj) != 0) {
    mError("trans:%d, failed to set redo actions since %s", pTrans->id, terrstr());
    goto CREATE_TOPIC_OVER;
  }

  if (mndSetCreateTopicUndoActions(pMnode, pTrans, pDb, &topicObj) != 0) {
    mError("trans:%d, failed to set redo actions since %s", pTrans->id, terrstr());
    goto CREATE_TOPIC_OVER;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  code = 0;

CREATE_TOPIC_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateTopicMsg(SMnodeMsg *pMsg) {
  SMnode          *pMnode = pMsg->pMnode;
  SCreateTopicMsg *pCreate = pMsg->rpcMsg.pCont;

  mDebug("topic:%s, start to create", pCreate->name);

  if (mndCheckCreateTopicMsg(pCreate) != 0) {
    mError("topic:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  STopicObj *pTopic = mndAcquireTopic(pMnode, pCreate->name);
  if (pTopic != NULL) {
    sdbRelease(pMnode->pSdb, pTopic);
    if (pCreate->igExists) {
      mDebug("topic:%s, already exist, ignore exist is set", pCreate->name);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_TOPIC_ALREADY_EXIST;
      mError("db:%s, failed to create since %s", pCreate->name, terrstr());
      return -1;
    }
  }

  // topic should have different name with stb
  SStbObj *pStb = mndAcquireStb(pMnode, pCreate->name);
  if (pStb != NULL) {
    sdbRelease(pMnode->pSdb, pStb);
    terrno = TSDB_CODE_MND_NAME_CONFLICT_WITH_STB;
    mError("topic:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  SDbObj *pDb = mndAcquireDbByTopic(pMnode, pCreate->name);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    mError("topic:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  int32_t code = mndCreateTopic(pMnode, pMsg, pCreate, pDb);
  mndReleaseDb(pMnode, pDb);

  if (code != 0) {
    terrno = code;
    mError("topic:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndCheckAlterTopicMsg(SAlterTopicMsg *pAlter) {
  SSchema *pSchema = &pAlter->schema;
  pSchema->colId = htonl(pSchema->colId);
  pSchema->bytes = htonl(pSchema->bytes);

  if (pSchema->type <= 0) {
    terrno = TSDB_CODE_MND_INVALID_TOPIC_OPTION;
    return -1;
  }
  if (pSchema->colId < 0 || pSchema->colId >= (TSDB_MAX_COLUMNS + TSDB_MAX_TAGS)) {
    terrno = TSDB_CODE_MND_INVALID_TOPIC_OPTION;
    return -1;
  }
  if (pSchema->bytes <= 0) {
    terrno = TSDB_CODE_MND_INVALID_TOPIC_OPTION;
    return -1;
  }
  if (pSchema->name[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_TOPIC_OPTION;
    return -1;
  }

  return 0;
}

static int32_t mndUpdateTopic(SMnode *pMnode, SMnodeMsg *pMsg, STopicObj *pOldTopic, STopicObj *pNewTopic) { return 0; }

static int32_t mndProcessAlterTopicMsg(SMnodeMsg *pMsg) {
  SMnode         *pMnode = pMsg->pMnode;
  SAlterTopicMsg *pAlter = pMsg->rpcMsg.pCont;

  mDebug("topic:%s, start to alter", pAlter->name);

  if (mndCheckAlterTopicMsg(pAlter) != 0) {
    mError("topic:%s, failed to alter since %s", pAlter->name, terrstr());
    return -1;
  }

  STopicObj *pTopic = mndAcquireTopic(pMnode, pAlter->name);
  if (pTopic == NULL) {
    terrno = TSDB_CODE_MND_TOPIC_NOT_EXIST;
    mError("topic:%s, failed to alter since %s", pAlter->name, terrstr());
    return -1;
  }

  STopicObj topicObj = {0};
  memcpy(&topicObj, pTopic, sizeof(STopicObj));

  int32_t code = mndUpdateTopic(pMnode, pMsg, pTopic, &topicObj);
  mndReleaseTopic(pMnode, pTopic);

  if (code != 0) {
    mError("topic:%s, failed to alter since %s", pAlter->name, tstrerror(code));
    return code;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessAlterTopicInRsp(SMnodeMsg *pMsg) {
  mndTransProcessRsp(pMsg);
  return 0;
}

static int32_t mndSetDropTopicRedoLogs(SMnode *pMnode, STrans *pTrans, STopicObj *pTopic) {
  SSdbRaw *pRedoRaw = mndTopicActionEncode(pTopic);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING) != 0) return -1;

  return 0;
}

static int32_t mndSetDropTopicUndoLogs(SMnode *pMnode, STrans *pTrans, STopicObj *pTopic) {
  SSdbRaw *pUndoRaw = mndTopicActionEncode(pTopic);
  if (pUndoRaw == NULL) return -1;
  if (mndTransAppendUndolog(pTrans, pUndoRaw) != 0) return -1;
  if (sdbSetRawStatus(pUndoRaw, SDB_STATUS_READY) != 0) return -1;

  return 0;
}

static int32_t mndSetDropTopicCommitLogs(SMnode *pMnode, STrans *pTrans, STopicObj *pTopic) {
  SSdbRaw *pCommitRaw = mndTopicActionEncode(pTopic);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;

  return 0;
}

static int32_t mndSetDropTopicRedoActions(SMnode *pMnode, STrans *pTrans, STopicObj *pTopic) { return 0; }

static int32_t mndSetDropTopicUndoActions(SMnode *pMnode, STrans *pTrans, STopicObj *pTopic) { return 0; }

static int32_t mndDropTopic(SMnode *pMnode, SMnodeMsg *pMsg, STopicObj *pTopic) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, &pMsg->rpcMsg);
  if (pTrans == NULL) {
    mError("topic:%s, failed to drop since %s", pTopic->name, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to drop topic:%s", pTrans->id, pTopic->name);

  if (mndSetDropTopicRedoLogs(pMnode, pTrans, pTopic) != 0) {
    mError("trans:%d, failed to set redo log since %s", pTrans->id, terrstr());
    goto DROP_TOPIC_OVER;
  }

  if (mndSetDropTopicUndoLogs(pMnode, pTrans, pTopic) != 0) {
    mError("trans:%d, failed to set undo log since %s", pTrans->id, terrstr());
    goto DROP_TOPIC_OVER;
  }

  if (mndSetDropTopicCommitLogs(pMnode, pTrans, pTopic) != 0) {
    mError("trans:%d, failed to set commit log since %s", pTrans->id, terrstr());
    goto DROP_TOPIC_OVER;
  }

  if (mndSetDropTopicRedoActions(pMnode, pTrans, pTopic) != 0) {
    mError("trans:%d, failed to set redo actions since %s", pTrans->id, terrstr());
    goto DROP_TOPIC_OVER;
  }

  if (mndSetDropTopicUndoActions(pMnode, pTrans, pTopic) != 0) {
    mError("trans:%d, failed to set redo actions since %s", pTrans->id, terrstr());
    goto DROP_TOPIC_OVER;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    goto DROP_TOPIC_OVER;
  }

  code = 0;

DROP_TOPIC_OVER:
  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessDropTopicMsg(SMnodeMsg *pMsg) {
  SMnode        *pMnode = pMsg->pMnode;
  SDropTopicMsg *pDrop = pMsg->rpcMsg.pCont;

  mDebug("topic:%s, start to drop", pDrop->name);

  STopicObj *pTopic = mndAcquireTopic(pMnode, pDrop->name);
  if (pTopic == NULL) {
    if (pDrop->igNotExists) {
      mDebug("topic:%s, not exist, ignore not exist is set", pDrop->name);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_TOPIC_NOT_EXIST;
      mError("topic:%s, failed to drop since %s", pDrop->name, terrstr());
      return -1;
    }
  }

  int32_t code = mndDropTopic(pMnode, pMsg, pTopic);
  mndReleaseTopic(pMnode, pTopic);

  if (code != 0) {
    terrno = code;
    mError("topic:%s, failed to drop since %s", pDrop->name, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessDropTopicInRsp(SMnodeMsg *pMsg) {
  mndTransProcessRsp(pMsg);
  return 0;
}

static int32_t mndProcessTopicMetaMsg(SMnodeMsg *pMsg) {
  SMnode        *pMnode = pMsg->pMnode;
  STableInfoMsg *pInfo = pMsg->rpcMsg.pCont;

  mDebug("topic:%s, start to retrieve meta", pInfo->tableFname);

#if 0
  SDbObj *pDb = mndAcquireDbByTopic(pMnode, pInfo->tableFname);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    mError("topic:%s, failed to retrieve meta since %s", pInfo->tableFname, terrstr());
    return -1;
  }

  STopicObj *pTopic = mndAcquireTopic(pMnode, pInfo->tableFname);
  if (pTopic == NULL) {
    mndReleaseDb(pMnode, pDb);
    terrno = TSDB_CODE_MND_INVALID_TOPIC;
    mError("topic:%s, failed to get meta since %s", pInfo->tableFname, terrstr());
    return -1;
  }

  taosRLockLatch(&pTopic->lock);
  int32_t totalCols = pTopic->numOfColumns + pTopic->numOfTags;
  int32_t contLen = sizeof(STableMetaMsg) + totalCols * sizeof(SSchema);

  STableMetaMsg *pMeta = rpcMallocCont(contLen);
  if (pMeta == NULL) {
    taosRUnLockLatch(&pTopic->lock);
    mndReleaseDb(pMnode, pDb);
    mndReleaseTopic(pMnode, pTopic);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("topic:%s, failed to get meta since %s", pInfo->tableFname, terrstr());
    return -1;
  }

  memcpy(pMeta->topicFname, pTopic->name, TSDB_TABLE_FNAME_LEN);
  pMeta->numOfTags = htonl(pTopic->numOfTags);
  pMeta->numOfColumns = htonl(pTopic->numOfColumns);
  pMeta->precision = pDb->cfg.precision;
  pMeta->tableType = TSDB_SUPER_TABLE;
  pMeta->update = pDb->cfg.update;
  pMeta->sversion = htonl(pTopic->version);
  pMeta->tuid = htonl(pTopic->uid);

  for (int32_t i = 0; i < totalCols; ++i) {
    SSchema *pSchema = &pMeta->pSchema[i];
    SSchema *pSrcSchema = &pTopic->pSchema[i];
    memcpy(pSchema->name, pSrcSchema->name, TSDB_COL_NAME_LEN);
    pSchema->type = pSrcSchema->type;
    pSchema->colId = htonl(pSrcSchema->colId);
    pSchema->bytes = htonl(pSrcSchema->bytes);
  }
  taosRUnLockLatch(&pTopic->lock);
  mndReleaseDb(pMnode, pDb);
  mndReleaseTopic(pMnode, pTopic);

  pMsg->pCont = pMeta;
  pMsg->contLen = contLen;

  mDebug("topic:%s, meta is retrieved, cols:%d tags:%d", pInfo->tableFname, pTopic->numOfColumns, pTopic->numOfTags);
#endif
  return 0;
}

static int32_t mndProcessCreateTopicInRsp(SMnodeMsg *pMsg) {
  mndTransProcessRsp(pMsg);
  return 0;
}

static int32_t mndGetNumOfTopics(SMnode *pMnode, char *dbName, int32_t *pNumOfTopics) {
  SSdb *pSdb = pMnode->pSdb;

  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    return -1;
  }

  int32_t numOfTopics = 0;
  void   *pIter = NULL;
  while (1) {
    STopicObj *pTopic = NULL;
    pIter = sdbFetch(pSdb, SDB_TOPIC, pIter, (void **)&pTopic);
    if (pIter == NULL) break;

    if (strcmp(pTopic->db, dbName) == 0) {
      numOfTopics++;
    }

    sdbRelease(pSdb, pTopic);
  }

  *pNumOfTopics = numOfTopics;
  return 0;
}

static int32_t mndGetTopicMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  if (mndGetNumOfTopics(pMnode, pShow->db, &pShow->numOfRows) != 0) {
    return -1;
  }

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchema;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "columns");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "tags");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htonl(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_TOPIC);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));

  return 0;
}

static void mndExtractTableName(char *tableId, char *name) {
  int32_t pos = -1;
  int32_t num = 0;
  for (pos = 0; tableId[pos] != 0; ++pos) {
    if (tableId[pos] == '.') num++;
    if (num == 2) break;
  }

  if (num == 2) {
    strcpy(name, tableId + pos + 1);
  }
}

static int32_t mndRetrieveTopic(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode    *pMnode = pMsg->pMnode;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  STopicObj *pTopic = NULL;
  int32_t    cols = 0;
  char      *pWrite;
  char       prefix[64] = {0};

  tstrncpy(prefix, pShow->db, 64);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = (int32_t)strlen(prefix);

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_TOPIC, pShow->pIter, (void **)&pTopic);
    if (pShow->pIter == NULL) break;

    if (strncmp(pTopic->name, prefix, prefixLen) != 0) {
      sdbRelease(pSdb, pTopic);
      continue;
    }

    cols = 0;

    char topicName[TSDB_TABLE_NAME_LEN] = {0};
    tstrncpy(topicName, pTopic->name + prefixLen, TSDB_TABLE_NAME_LEN);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, topicName);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pTopic->createTime;
    cols++;

    /*pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;*/
    /**(int32_t *)pWrite = pTopic->numOfColumns;*/
    /*cols++;*/

    /*pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;*/
    /**(int32_t *)pWrite = pTopic->numOfTags;*/
    /*cols++;*/

    numOfRows++;
    sdbRelease(pSdb, pTopic);
  }

  pShow->numOfReads += numOfRows;
  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  return numOfRows;
}

static void mndCancelGetNextTopic(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
