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

#include "mndTopic.h"
#include "mndAuth.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndOffset.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndSubscribe.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "parser.h"
#include "tname.h"

#define MND_TOPIC_VER_NUMBER   1
#define MND_TOPIC_RESERVE_SIZE 64

static int32_t mndTopicActionInsert(SSdb *pSdb, SMqTopicObj *pTopic);
static int32_t mndTopicActionDelete(SSdb *pSdb, SMqTopicObj *pTopic);
static int32_t mndTopicActionUpdate(SSdb *pSdb, SMqTopicObj *pTopic, SMqTopicObj *pNewTopic);
static int32_t mndProcessCreateTopicReq(SRpcMsg *pReq);
static int32_t mndProcessDropTopicReq(SRpcMsg *pReq);
static int32_t mndProcessDropTopicInRsp(SRpcMsg *pRsp);

static int32_t mndRetrieveTopic(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextTopic(SMnode *pMnode, void *pIter);

static int32_t mndSetDropTopicCommitLogs(SMnode *pMnode, STrans *pTrans, SMqTopicObj *pTopic);

int32_t mndInitTopic(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_TOPIC,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndTopicActionEncode,
                     .decodeFp = (SdbDecodeFp)mndTopicActionDecode,
                     .insertFp = (SdbInsertFp)mndTopicActionInsert,
                     .updateFp = (SdbUpdateFp)mndTopicActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndTopicActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_TOPIC, mndProcessCreateTopicReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_TOPIC, mndProcessDropTopicReq);
  mndSetMsgHandle(pMnode, TDMT_VND_DROP_TOPIC_RSP, mndProcessDropTopicInRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TOPICS, mndRetrieveTopic);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TOPICS, mndCancelGetNextTopic);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupTopic(SMnode *pMnode) {}

const char *mndTopicGetShowName(const char topic[TSDB_TOPIC_FNAME_LEN]) {
  //
  return strchr(topic, '.') + 1;
}

SSdbRaw *mndTopicActionEncode(SMqTopicObj *pTopic) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  void   *swBuf = NULL;
  int32_t physicalPlanLen = 0;
  if (pTopic->physicalPlan) {
    physicalPlanLen = strlen(pTopic->physicalPlan) + 1;
  }
  int32_t schemaLen = 0;
  if (pTopic->schema.nCols) {
    schemaLen = taosEncodeSSchemaWrapper(NULL, &pTopic->schema);
  }
  int32_t size =
      sizeof(SMqTopicObj) + physicalPlanLen + pTopic->sqlLen + pTopic->astLen + schemaLen + MND_TOPIC_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_TOPIC, MND_TOPIC_VER_NUMBER, size);
  if (pRaw == NULL) goto TOPIC_ENCODE_OVER;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pTopic->name, TSDB_TOPIC_FNAME_LEN, TOPIC_ENCODE_OVER);
  SDB_SET_BINARY(pRaw, dataPos, pTopic->db, TSDB_DB_FNAME_LEN, TOPIC_ENCODE_OVER);
  SDB_SET_INT64(pRaw, dataPos, pTopic->createTime, TOPIC_ENCODE_OVER);
  SDB_SET_INT64(pRaw, dataPos, pTopic->updateTime, TOPIC_ENCODE_OVER);
  SDB_SET_INT64(pRaw, dataPos, pTopic->uid, TOPIC_ENCODE_OVER);
  SDB_SET_INT64(pRaw, dataPos, pTopic->dbUid, TOPIC_ENCODE_OVER);
  SDB_SET_INT32(pRaw, dataPos, pTopic->version, TOPIC_ENCODE_OVER);
  SDB_SET_INT8(pRaw, dataPos, pTopic->subType, TOPIC_ENCODE_OVER);
  SDB_SET_INT8(pRaw, dataPos, pTopic->withTbName, TOPIC_ENCODE_OVER);
  SDB_SET_INT8(pRaw, dataPos, pTopic->withSchema, TOPIC_ENCODE_OVER);
  SDB_SET_INT8(pRaw, dataPos, pTopic->withTag, TOPIC_ENCODE_OVER);

  SDB_SET_INT32(pRaw, dataPos, pTopic->consumerCnt, TOPIC_ENCODE_OVER);
  SDB_SET_INT32(pRaw, dataPos, pTopic->sqlLen, TOPIC_ENCODE_OVER);
  SDB_SET_BINARY(pRaw, dataPos, pTopic->sql, pTopic->sqlLen, TOPIC_ENCODE_OVER);
  SDB_SET_INT32(pRaw, dataPos, pTopic->astLen, TOPIC_ENCODE_OVER);
  if (pTopic->astLen) {
    SDB_SET_BINARY(pRaw, dataPos, pTopic->ast, pTopic->astLen, TOPIC_ENCODE_OVER);
  }
  SDB_SET_INT32(pRaw, dataPos, physicalPlanLen, TOPIC_ENCODE_OVER);
  if (physicalPlanLen) {
    SDB_SET_BINARY(pRaw, dataPos, pTopic->physicalPlan, physicalPlanLen, TOPIC_ENCODE_OVER);
  }
  SDB_SET_INT32(pRaw, dataPos, schemaLen, TOPIC_ENCODE_OVER);
  if (schemaLen) {
    swBuf = taosMemoryMalloc(schemaLen);
    if (swBuf == NULL) {
      goto TOPIC_ENCODE_OVER;
    }
    void *aswBuf = swBuf;
    taosEncodeSSchemaWrapper(&aswBuf, &pTopic->schema);
    SDB_SET_BINARY(pRaw, dataPos, swBuf, schemaLen, TOPIC_ENCODE_OVER);
  }

  SDB_SET_INT32(pRaw, dataPos, pTopic->refConsumerCnt, TOPIC_ENCODE_OVER);

  SDB_SET_RESERVE(pRaw, dataPos, MND_TOPIC_RESERVE_SIZE, TOPIC_ENCODE_OVER);
  SDB_SET_DATALEN(pRaw, dataPos, TOPIC_ENCODE_OVER);

  terrno = TSDB_CODE_SUCCESS;

TOPIC_ENCODE_OVER:
  if (swBuf) taosMemoryFree(swBuf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("topic:%s, failed to encode to raw:%p since %s", pTopic->name, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("topic:%s, encode to raw:%p, row:%p", pTopic->name, pRaw, pTopic);
  return pRaw;
}

SSdbRow *mndTopicActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto TOPIC_DECODE_OVER;

  if (sver != MND_TOPIC_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto TOPIC_DECODE_OVER;
  }

  int32_t  size = sizeof(SMqTopicObj);
  SSdbRow *pRow = sdbAllocRow(size);
  if (pRow == NULL) goto TOPIC_DECODE_OVER;

  SMqTopicObj *pTopic = sdbGetRowObj(pRow);
  if (pTopic == NULL) goto TOPIC_DECODE_OVER;

  int32_t len;
  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pTopic->name, TSDB_TOPIC_FNAME_LEN, TOPIC_DECODE_OVER);
  SDB_GET_BINARY(pRaw, dataPos, pTopic->db, TSDB_DB_FNAME_LEN, TOPIC_DECODE_OVER);
  SDB_GET_INT64(pRaw, dataPos, &pTopic->createTime, TOPIC_DECODE_OVER);
  SDB_GET_INT64(pRaw, dataPos, &pTopic->updateTime, TOPIC_DECODE_OVER);
  SDB_GET_INT64(pRaw, dataPos, &pTopic->uid, TOPIC_DECODE_OVER);
  SDB_GET_INT64(pRaw, dataPos, &pTopic->dbUid, TOPIC_DECODE_OVER);
  SDB_GET_INT32(pRaw, dataPos, &pTopic->version, TOPIC_DECODE_OVER);
  SDB_GET_INT8(pRaw, dataPos, &pTopic->subType, TOPIC_DECODE_OVER);
  SDB_GET_INT8(pRaw, dataPos, &pTopic->withTbName, TOPIC_DECODE_OVER);
  SDB_GET_INT8(pRaw, dataPos, &pTopic->withSchema, TOPIC_DECODE_OVER);
  SDB_GET_INT8(pRaw, dataPos, &pTopic->withTag, TOPIC_DECODE_OVER);

  SDB_GET_INT32(pRaw, dataPos, &pTopic->consumerCnt, TOPIC_DECODE_OVER);

  SDB_GET_INT32(pRaw, dataPos, &pTopic->sqlLen, TOPIC_DECODE_OVER);
  pTopic->sql = taosMemoryCalloc(pTopic->sqlLen, sizeof(char));
  if (pTopic->sql == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto TOPIC_DECODE_OVER;
  }
  SDB_GET_BINARY(pRaw, dataPos, pTopic->sql, pTopic->sqlLen, TOPIC_DECODE_OVER);

  SDB_GET_INT32(pRaw, dataPos, &pTopic->astLen, TOPIC_DECODE_OVER);
  if (pTopic->astLen) {
    pTopic->ast = taosMemoryCalloc(pTopic->astLen, sizeof(char));
    if (pTopic->ast == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto TOPIC_DECODE_OVER;
    }
  } else {
    pTopic->ast = NULL;
  }
  SDB_GET_BINARY(pRaw, dataPos, pTopic->ast, pTopic->astLen, TOPIC_DECODE_OVER);
  SDB_GET_INT32(pRaw, dataPos, &len, TOPIC_DECODE_OVER);
  if (len) {
    pTopic->physicalPlan = taosMemoryCalloc(len, sizeof(char));
    if (pTopic->physicalPlan == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto TOPIC_DECODE_OVER;
    }
    SDB_GET_BINARY(pRaw, dataPos, pTopic->physicalPlan, len, TOPIC_DECODE_OVER);
  } else {
    pTopic->physicalPlan = NULL;
  }

  SDB_GET_INT32(pRaw, dataPos, &len, TOPIC_DECODE_OVER);
  if (len) {
    void *buf = taosMemoryMalloc(len);
    if (buf == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto TOPIC_DECODE_OVER;
    }
    SDB_GET_BINARY(pRaw, dataPos, buf, len, TOPIC_DECODE_OVER);
    if (taosDecodeSSchemaWrapper(buf, &pTopic->schema) == NULL) {
      goto TOPIC_DECODE_OVER;
    }
  } else {
    pTopic->schema.nCols = 0;
    pTopic->schema.sver = 0;
    pTopic->schema.pSchema = NULL;
  }

  SDB_GET_INT32(pRaw, dataPos, &pTopic->refConsumerCnt, TOPIC_DECODE_OVER);

  SDB_GET_RESERVE(pRaw, dataPos, MND_TOPIC_RESERVE_SIZE, TOPIC_DECODE_OVER);

  terrno = TSDB_CODE_SUCCESS;

TOPIC_DECODE_OVER:
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("topic:%s, failed to decode from raw:%p since %s", pTopic->name, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("topic:%s, decode from raw:%p, row:%p", pTopic->name, pRaw, pTopic);
  return pRow;
}

static int32_t mndTopicActionInsert(SSdb *pSdb, SMqTopicObj *pTopic) {
  mTrace("topic:%s, perform insert action", pTopic->name);
  return 0;
}

static int32_t mndTopicActionDelete(SSdb *pSdb, SMqTopicObj *pTopic) {
  mTrace("topic:%s, perform delete action", pTopic->name);
  return 0;
}

static int32_t mndTopicActionUpdate(SSdb *pSdb, SMqTopicObj *pOldTopic, SMqTopicObj *pNewTopic) {
  mTrace("topic:%s, perform update action", pOldTopic->name);
  atomic_exchange_64(&pOldTopic->updateTime, pNewTopic->updateTime);
  atomic_exchange_32(&pOldTopic->version, pNewTopic->version);

  atomic_store_32(&pOldTopic->refConsumerCnt, pNewTopic->refConsumerCnt);

  /*taosWLockLatch(&pOldTopic->lock);*/

  // TODO handle update

  /*taosWUnLockLatch(&pOldTopic->lock);*/
  return 0;
}

SMqTopicObj *mndAcquireTopic(SMnode *pMnode, char *topicName) {
  SSdb        *pSdb = pMnode->pSdb;
  SMqTopicObj *pTopic = sdbAcquire(pSdb, SDB_TOPIC, topicName);
  if (pTopic == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_TOPIC_NOT_EXIST;
  }
  return pTopic;
}

void mndReleaseTopic(SMnode *pMnode, SMqTopicObj *pTopic) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pTopic);
}

#if 0
static SDbObj *mndAcquireDbByTopic(SMnode *pMnode, char *topicName) {
  SName name = {0};
  tNameFromString(&name, topicName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

  char db[TSDB_TOPIC_FNAME_LEN] = {0};
  tNameGetFullDbName(&name, db);

  return mndAcquireDb(pMnode, db);
}
#endif

static SDDropTopicReq *mndBuildDropTopicMsg(SMnode *pMnode, SVgObj *pVgroup, SMqTopicObj *pTopic) {
  int32_t contLen = sizeof(SDDropTopicReq);

  SDDropTopicReq *pDrop = taosMemoryCalloc(1, contLen);
  if (pDrop == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pDrop->head.contLen = htonl(contLen);
  pDrop->head.vgId = htonl(pVgroup->vgId);
  memcpy(pDrop->name, pTopic->name, TSDB_TOPIC_FNAME_LEN);
  pDrop->tuid = htobe64(pTopic->uid);

  return pDrop;
}

static int32_t mndCheckCreateTopicReq(SCMCreateTopicReq *pCreate) {
  if (pCreate->name[0] == 0 || pCreate->sql == NULL || pCreate->sql[0] == 0 || pCreate->subscribeDbName[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_TOPIC;
    return -1;
  }

  return 0;
}

static int32_t mndCreateTopic(SMnode *pMnode, SRpcMsg *pReq, SCMCreateTopicReq *pCreate, SDbObj *pDb) {
  mDebug("topic:%s to create", pCreate->name);
  SMqTopicObj topicObj = {0};
  tstrncpy(topicObj.name, pCreate->name, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(topicObj.db, pDb->name, TSDB_DB_FNAME_LEN);
  topicObj.createTime = taosGetTimestampMs();
  topicObj.updateTime = topicObj.createTime;
  topicObj.uid = mndGenerateUid(pCreate->name, strlen(pCreate->name));
  topicObj.dbUid = pDb->uid;
  topicObj.version = 1;
  topicObj.sql = strdup(pCreate->sql);
  topicObj.sqlLen = strlen(pCreate->sql) + 1;
  topicObj.refConsumerCnt = 0;

  if (pCreate->ast && pCreate->ast[0]) {
    topicObj.ast = strdup(pCreate->ast);
    topicObj.astLen = strlen(pCreate->ast) + 1;
    topicObj.subType = TOPIC_SUB_TYPE__TABLE;
    topicObj.withTbName = pCreate->withTbName;
    topicObj.withSchema = pCreate->withSchema;

    SNode *pAst = NULL;
    if (nodesStringToNode(pCreate->ast, &pAst) != 0) {
      taosMemoryFree(topicObj.ast);
      taosMemoryFree(topicObj.sql);
      mError("topic:%s, failed to create since %s", pCreate->name, terrstr());
      return -1;
    }

    SQueryPlan *pPlan = NULL;

    SPlanContext cxt = {.pAstRoot = pAst, .topicQuery = true};
    if (qCreateQueryPlan(&cxt, &pPlan, NULL) != 0) {
      mError("topic:%s, failed to create since %s", pCreate->name, terrstr());
      taosMemoryFree(topicObj.ast);
      taosMemoryFree(topicObj.sql);
      return -1;
    }

    if (qExtractResultSchema(pAst, &topicObj.schema.nCols, &topicObj.schema.pSchema) != 0) {
      mError("topic:%s, failed to create since %s", pCreate->name, terrstr());
      taosMemoryFree(topicObj.ast);
      taosMemoryFree(topicObj.sql);
      return -1;
    }

    if (nodesNodeToString(pPlan, false, &topicObj.physicalPlan, NULL) != 0) {
      mError("topic:%s, failed to create since %s", pCreate->name, terrstr());
      taosMemoryFree(topicObj.ast);
      taosMemoryFree(topicObj.sql);
      return -1;
    }
  } else {
    topicObj.ast = NULL;
    topicObj.astLen = 0;
    topicObj.physicalPlan = NULL;
    topicObj.subType = TOPIC_SUB_TYPE__DB;
    topicObj.withTbName = 1;
    topicObj.withSchema = 1;
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_CREATE_TOPIC, pReq);
  if (pTrans == NULL) {
    mError("topic:%s, failed to create since %s", pCreate->name, terrstr());
    taosMemoryFreeClear(topicObj.ast);
    taosMemoryFreeClear(topicObj.sql);
    taosMemoryFreeClear(topicObj.physicalPlan);
    return -1;
  }
  mDebug("trans:%d, used to create topic:%s", pTrans->id, pCreate->name);

  SSdbRaw *pRedoRaw = mndTopicActionEncode(&topicObj);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    taosMemoryFreeClear(topicObj.physicalPlan);
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    taosMemoryFreeClear(topicObj.physicalPlan);
    mndTransDrop(pTrans);
    return -1;
  }

  taosMemoryFreeClear(topicObj.physicalPlan);
  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessCreateTopicReq(SRpcMsg *pReq) {
  SMnode           *pMnode = pReq->info.node;
  int32_t           code = -1;
  SMqTopicObj      *pTopic = NULL;
  SDbObj           *pDb = NULL;
  SUserObj         *pUser = NULL;
  SCMCreateTopicReq createTopicReq = {0};

  if (tDeserializeSCMCreateTopicReq(pReq->pCont, pReq->contLen, &createTopicReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto CREATE_TOPIC_OVER;
  }

  mDebug("topic:%s, start to create, sql:%s", createTopicReq.name, createTopicReq.sql);

  if (mndCheckCreateTopicReq(&createTopicReq) != 0) {
    mError("topic:%s, failed to create since %s", createTopicReq.name, terrstr());
    goto CREATE_TOPIC_OVER;
  }

  pTopic = mndAcquireTopic(pMnode, createTopicReq.name);
  if (pTopic != NULL) {
    if (createTopicReq.igExists) {
      mDebug("topic:%s, already exist, ignore exist is set", createTopicReq.name);
      code = 0;
      goto CREATE_TOPIC_OVER;
    } else {
      terrno = TSDB_CODE_MND_TOPIC_ALREADY_EXIST;
      goto CREATE_TOPIC_OVER;
    }
  } else if (terrno != TSDB_CODE_MND_TOPIC_NOT_EXIST) {
    goto CREATE_TOPIC_OVER;
  }

  pDb = mndAcquireDb(pMnode, createTopicReq.subscribeDbName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto CREATE_TOPIC_OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->conn.user);
  if (pUser == NULL) {
    goto CREATE_TOPIC_OVER;
  }

  if (mndCheckWriteAuth(pUser, pDb) != 0) {
    goto CREATE_TOPIC_OVER;
  }

  code = mndCreateTopic(pMnode, pReq, &createTopicReq, pDb);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

CREATE_TOPIC_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("topic:%s, failed to create since %s", createTopicReq.name, terrstr());
  }

  mndReleaseTopic(pMnode, pTopic);
  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);

  tFreeSCMCreateTopicReq(&createTopicReq);
  return code;
}

static int32_t mndDropTopic(SMnode *pMnode, STrans *pTrans, SRpcMsg *pReq, SMqTopicObj *pTopic) {
  SSdbRaw *pRedoRaw = mndTopicActionEncode(pTopic);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPED);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessDropTopicReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  SSdb          *pSdb = pMnode->pSdb;
  SMDropTopicReq dropReq = {0};

  if (tDeserializeSMDropTopicReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SMqTopicObj *pTopic = mndAcquireTopic(pMnode, dropReq.name);
  // if (pTopic == NULL) {
  //   if (dropReq.igNotExists) {
  //     mDebug("topic:%s, not exist, ignore not exist is set", dropReq.name);
  //     return 0;
  //   } else {
  //     terrno = TSDB_CODE_MND_TOPIC_NOT_EXIST;
  //     mError("topic:%s, failed to drop since %s", dropReq.name, terrstr());
  //     return -1;
  //   }
  // }

  if (pTopic->refConsumerCnt != 0) {
    mndReleaseTopic(pMnode, pTopic);
    terrno = TSDB_CODE_MND_TOPIC_SUBSCRIBED;
    mError("topic:%s, failed to drop since %s", dropReq.name, terrstr());
    return -1;
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_DROP_TOPIC, pReq);
  if (pTrans == NULL) {
    mError("topic:%s, failed to drop since %s", pTopic->name, terrstr());
    return -1;
  }

  mDebug("trans:%d, used to drop topic:%s", pTrans->id, pTopic->name);

#if 1
  if (mndDropOffsetByTopic(pMnode, pTrans, dropReq.name) < 0) {
    ASSERT(0);
    return -1;
  }
#endif

  if (mndDropSubByTopic(pMnode, pTrans, dropReq.name) < 0) {
    ASSERT(0);
    return -1;
  }

  int32_t code = mndDropTopic(pMnode, pTrans, pReq, pTopic);
  mndReleaseTopic(pMnode, pTopic);

  if (code != 0) {
    terrno = code;
    mError("topic:%s, failed to drop since %s", dropReq.name, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessDropTopicInRsp(SRpcMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndGetNumOfTopics(SMnode *pMnode, char *dbName, int32_t *pNumOfTopics) {
  SSdb   *pSdb = pMnode->pSdb;
  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    return -1;
  }

  int32_t numOfTopics = 0;
  void   *pIter = NULL;
  while (1) {
    SMqTopicObj *pTopic = NULL;
    pIter = sdbFetch(pSdb, SDB_TOPIC, pIter, (void **)&pTopic);
    if (pIter == NULL) break;

    if (pTopic->dbUid == pDb->uid) {
      numOfTopics++;
    }

    sdbRelease(pSdb, pTopic);
  }

  *pNumOfTopics = numOfTopics;
  mndReleaseDb(pMnode, pDb);
  return 0;
}

static int32_t mndRetrieveTopic(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode      *pMnode = pReq->info.node;
  SSdb        *pSdb = pMnode->pSdb;
  int32_t      numOfRows = 0;
  SMqTopicObj *pTopic = NULL;

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_TOPIC, pShow->pIter, (void **)&pTopic);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char topicName[TSDB_TOPIC_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    tNameFromString(&n, pTopic->name, T_NAME_ACCT | T_NAME_DB);
    tNameGetDbName(&n, varDataVal(topicName));
    varDataSetLen(topicName, strlen(varDataVal(topicName)));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)topicName, false);

    char dbName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    tNameFromString(&n, pTopic->db, T_NAME_ACCT | T_NAME_DB);
    tNameGetDbName(&n, varDataVal(dbName));
    varDataSetLen(dbName, strlen(varDataVal(dbName)));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)dbName, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pTopic->createTime, false);

    char sql[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
    tstrncpy(&sql[VARSTR_HEADER_SIZE], pTopic->sql, TSDB_SHOW_SQL_LEN);
    varDataSetLen(sql, strlen(&sql[VARSTR_HEADER_SIZE]));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)sql, false);

    numOfRows++;
    sdbRelease(pSdb, pTopic);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

int32_t mndSetTopicRedoLogs(SMnode *pMnode, STrans *pTrans, SMqTopicObj *pTopic) {
  SSdbRaw *pRedoRaw = mndTopicActionEncode(pTopic);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY) != 0) return -1;

  return 0;
}

static int32_t mndSetDropTopicCommitLogs(SMnode *pMnode, STrans *pTrans, SMqTopicObj *pTopic) {
  SSdbRaw *pCommitRaw = mndTopicActionEncode(pTopic);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;

  return 0;
}

static void mndCancelGetNextTopic(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

int32_t mndDropTopicByDB(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  int32_t code = -1;
  SSdb   *pSdb = pMnode->pSdb;

  void        *pIter = NULL;
  SMqTopicObj *pTopic = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_TOPIC, pIter, (void **)&pTopic);
    if (pIter == NULL) break;

    if (pTopic->dbUid != pDb->uid) {
      sdbRelease(pSdb, pTopic);
      continue;
    }

    if (mndSetDropTopicCommitLogs(pMnode, pTrans, pTopic) < 0) {
      goto END;
    }
  }

  code = 0;
END:
  return code;
}
