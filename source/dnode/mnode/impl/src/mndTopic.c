/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *f
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "mndTopic.h"
#include "mndConsumer.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndSubscribe.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "osMemPool.h"
#include "parser.h"
#include "tlockfree.h"
#include "tname.h"
#include "audit.h"

#define MND_TOPIC_VER_NUMBER   4
#define MND_TOPIC_RESERVE_SIZE 64

SHashObj *topicsToReload = NULL;

SSdbRaw *mndTopicActionEncode(SMqTopicObj *pTopic);
SSdbRow *mndTopicActionDecode(SSdbRaw *pRaw);

static int32_t mndTopicActionInsert(SSdb *pSdb, SMqTopicObj *pTopic);
static int32_t mndTopicActionDelete(SSdb *pSdb, SMqTopicObj *pTopic);
static int32_t mndTopicActionUpdate(SSdb *pSdb, SMqTopicObj *pOldTopic, SMqTopicObj *pNewTopic);
static int32_t mndProcessCreateTopicReq(SRpcMsg *pReq);
static int32_t mndProcessDropTopicReq(SRpcMsg *pReq);
static int32_t mndProcessReloadTopicReq(SRpcMsg *pReq);

static int32_t mndRetrieveTopic(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextTopic(SMnode *pMnode, void *pIter);

int32_t mndInitTopic(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_TOPIC,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndTopicActionEncode,
      .decodeFp = (SdbDecodeFp)mndTopicActionDecode,
      .insertFp = (SdbInsertFp)mndTopicActionInsert,
      .updateFp = (SdbUpdateFp)mndTopicActionUpdate,
      .deleteFp = (SdbDeleteFp)mndTopicActionDelete,
  };

  if (pMnode == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_CREATE_TOPIC, mndProcessCreateTopicReq);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_DROP_TOPIC, mndProcessDropTopicReq);
  mndSetMsgHandle(pMnode, TDMT_MND_TMQ_RELOAD_TOPIC, mndProcessReloadTopicReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TOPICS, mndRetrieveTopic);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TOPICS, mndCancelGetNextTopic);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupTopic(SMnode *pMnode) {}

void mndTopicGetShowName(const char *fullTopic, char *topic) {
  if (fullTopic == NULL) {
    return;
  }
  char *tmp = strchr(fullTopic, '.');
  if (tmp == NULL) {
    tstrncpy(topic, fullTopic, TSDB_TOPIC_FNAME_LEN);
  } else {
    tstrncpy(topic, tmp + 1, TSDB_TOPIC_FNAME_LEN);
  }
}

SSdbRaw *mndTopicActionEncode(SMqTopicObj *pTopic) {
  if (pTopic == NULL) {
    return NULL;
  }
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  void *  swBuf = NULL;
  int32_t physicalPlanLen = 0;
  if (pTopic->physicalPlan) {
    physicalPlanLen = strlen(pTopic->physicalPlan) + 1;
  }

  int32_t schemaLen = 0;
  if (pTopic->schema.nCols) {
    schemaLen = taosEncodeSSchemaWrapper(NULL, &pTopic->schema);
  }

  int32_t  size = sizeof(SMqTopicObj) + physicalPlanLen + pTopic->sqlLen + schemaLen + MND_TOPIC_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_TOPIC, MND_TOPIC_VER_NUMBER, size);
  if (pRaw == NULL) {
    goto TOPIC_ENCODE_OVER;
  }

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pTopic->name, TSDB_TOPIC_FNAME_LEN, TOPIC_ENCODE_OVER);
  SDB_SET_BINARY(pRaw, dataPos, pTopic->db, TSDB_DB_FNAME_LEN, TOPIC_ENCODE_OVER);
  SDB_SET_BINARY(pRaw, dataPos, pTopic->createUser, TSDB_USER_LEN, TOPIC_ENCODE_OVER);
  SDB_SET_INT64(pRaw, dataPos, pTopic->createTime, TOPIC_ENCODE_OVER);
  SDB_SET_INT64(pRaw, dataPos, pTopic->updateTime, TOPIC_ENCODE_OVER);
  SDB_SET_INT64(pRaw, dataPos, pTopic->uid, TOPIC_ENCODE_OVER);
  SDB_SET_INT64(pRaw, dataPos, pTopic->dbUid, TOPIC_ENCODE_OVER);
  SDB_SET_INT32(pRaw, dataPos, pTopic->version, TOPIC_ENCODE_OVER);
  SDB_SET_INT8(pRaw, dataPos, pTopic->subType, TOPIC_ENCODE_OVER);
  SDB_SET_INT8(pRaw, dataPos, pTopic->withMeta, TOPIC_ENCODE_OVER);

  SDB_SET_INT64(pRaw, dataPos, pTopic->stbUid, TOPIC_ENCODE_OVER);
  SDB_SET_BINARY(pRaw, dataPos, pTopic->stbName, TSDB_TABLE_FNAME_LEN, TOPIC_ENCODE_OVER);
  SDB_SET_INT32(pRaw, dataPos, pTopic->sqlLen, TOPIC_ENCODE_OVER);
  SDB_SET_BINARY(pRaw, dataPos, pTopic->sql, pTopic->sqlLen, TOPIC_ENCODE_OVER);
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
    if (taosEncodeSSchemaWrapper(&aswBuf, &pTopic->schema) < 0) {
      goto TOPIC_ENCODE_OVER;
    }
    SDB_SET_BINARY(pRaw, dataPos, swBuf, schemaLen, TOPIC_ENCODE_OVER);
  }

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

  mDebug("topic:%s, encode to raw:%p, row:%p", pTopic->name, pRaw, pTopic);
  return pRaw;
}

SSdbRow *mndTopicActionDecode(SSdbRaw *pRaw) {
  if (pRaw == NULL) return NULL;
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow *    pRow = NULL;
  SMqTopicObj *pTopic = NULL;
  void *       buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto TOPIC_DECODE_OVER;

  if (sver < 1 || sver > MND_TOPIC_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto TOPIC_DECODE_OVER;
  }

  pRow = sdbAllocRow(sizeof(SMqTopicObj));
  if (pRow == NULL) goto TOPIC_DECODE_OVER;

  pTopic = sdbGetRowObj(pRow);
  if (pTopic == NULL) goto TOPIC_DECODE_OVER;

  int32_t len = 0;
  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pTopic->name, TSDB_TOPIC_FNAME_LEN, TOPIC_DECODE_OVER);
  SDB_GET_BINARY(pRaw, dataPos, pTopic->db, TSDB_DB_FNAME_LEN, TOPIC_DECODE_OVER);
  if (sver >= 2) {
    SDB_GET_BINARY(pRaw, dataPos, pTopic->createUser, TSDB_USER_LEN, TOPIC_DECODE_OVER);
  }
  SDB_GET_INT64(pRaw, dataPos, &pTopic->createTime, TOPIC_DECODE_OVER);
  SDB_GET_INT64(pRaw, dataPos, &pTopic->updateTime, TOPIC_DECODE_OVER);
  SDB_GET_INT64(pRaw, dataPos, &pTopic->uid, TOPIC_DECODE_OVER);
  SDB_GET_INT64(pRaw, dataPos, &pTopic->dbUid, TOPIC_DECODE_OVER);
  SDB_GET_INT32(pRaw, dataPos, &pTopic->version, TOPIC_DECODE_OVER);
  SDB_GET_INT8(pRaw, dataPos, &pTopic->subType, TOPIC_DECODE_OVER);
  SDB_GET_INT8(pRaw, dataPos, &pTopic->withMeta, TOPIC_DECODE_OVER);

  SDB_GET_INT64(pRaw, dataPos, &pTopic->stbUid, TOPIC_DECODE_OVER);
  if (sver >= 3) {
    SDB_GET_BINARY(pRaw, dataPos, pTopic->stbName, TSDB_TABLE_FNAME_LEN, TOPIC_DECODE_OVER);
  }
  SDB_GET_INT32(pRaw, dataPos, &pTopic->sqlLen, TOPIC_DECODE_OVER);
  pTopic->sql = taosMemoryCalloc(pTopic->sqlLen, sizeof(char));
  if (pTopic->sql == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto TOPIC_DECODE_OVER;
  }
  SDB_GET_BINARY(pRaw, dataPos, pTopic->sql, pTopic->sqlLen, TOPIC_DECODE_OVER);

  if (sver < 4) {
    int32_t astLen = 0;
    SDB_GET_INT32(pRaw, dataPos, &astLen, TOPIC_DECODE_OVER);
    dataPos += astLen;
  }

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
    buf = taosMemoryMalloc(len);
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
    pTopic->schema.version = 0;
    pTopic->schema.pSchema = NULL;
  }

  SDB_GET_RESERVE(pRaw, dataPos, MND_TOPIC_RESERVE_SIZE, TOPIC_DECODE_OVER);
  terrno = TSDB_CODE_SUCCESS;

TOPIC_DECODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("topic:%s, failed to decode from raw:%p since %s", pTopic == NULL ? "null" : pTopic->name, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mDebug("topic:%s, decode from raw:%p, row:%p", pTopic->name, pRaw, pTopic);
  return pRow;
}

static int32_t mndTopicActionInsert(SSdb *pSdb, SMqTopicObj *pTopic) {
  mDebug("topic:%s perform insert action", pTopic != NULL ? pTopic->name : "null");
  return 0;
}

static int32_t mndTopicActionDelete(SSdb *pSdb, SMqTopicObj *pTopic) {
  if (pTopic == NULL) return 0;
  mDebug("topic:%s perform delete action", pTopic->name);
  taosMemoryFreeClear(pTopic->sql);
  taosMemoryFreeClear(pTopic->physicalPlan);
  if (pTopic->schema.nCols) taosMemoryFreeClear(pTopic->schema.pSchema);
  return 0;
}

static int32_t mndTopicActionUpdate(SSdb *pSdb, SMqTopicObj *pOldTopic, SMqTopicObj *pNewTopic) {
  if (pOldTopic == NULL || pNewTopic == NULL) return 0;
  mDebug("topic:%s perform update action", pOldTopic->name);
  (void)atomic_exchange_64(&pOldTopic->updateTime, pNewTopic->updateTime);
  (void)atomic_exchange_32(&pOldTopic->version, pNewTopic->version);

  return 0;
}

int32_t mndAcquireTopic(SMnode *pMnode, const char *topicName, SMqTopicObj **pTopic) {
  if (pMnode == NULL || topicName == NULL || pTopic == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SSdb *pSdb = pMnode->pSdb;
  *pTopic = sdbAcquire(pSdb, SDB_TOPIC, topicName);
  if (*pTopic == NULL) {
    return TSDB_CODE_MND_TOPIC_NOT_EXIST;
  }
  return TSDB_CODE_SUCCESS;
}

void mndReleaseTopic(SMnode *pMnode, SMqTopicObj *pTopic) {
  if (pMnode == NULL) return;
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pTopic);
}

static int32_t mndCheckCreateTopicReq(SCMCreateTopicReq *pCreate) {
  if (pCreate == NULL) return TSDB_CODE_INVALID_PARA;
  if (pCreate->sql == NULL) return TSDB_CODE_MND_INVALID_TOPIC;

  if (pCreate->subType == TOPIC_SUB_TYPE__COLUMN) {
    if (pCreate->ast == NULL || pCreate->ast[0] == 0) return TSDB_CODE_MND_INVALID_TOPIC;
  } else if (pCreate->subType == TOPIC_SUB_TYPE__TABLE) {
    if (pCreate->subStbName[0] == 0) return TSDB_CODE_MND_INVALID_TOPIC;
  } else if (pCreate->subType == TOPIC_SUB_TYPE__DB) {
    if (pCreate->subDbName[0] == 0) return TSDB_CODE_MND_INVALID_TOPIC;
  }

  return 0;
}

static int32_t processAst(SMqTopicObj *topicObj, const char *ast) {
  SNode *     pAst = NULL;
  SQueryPlan *pPlan = NULL;
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;

  PRINT_LOG_START
  if (ast == NULL) {
    topicObj->physicalPlan = taosStrdup("");
    goto END;
  }
  qDebugL("%s topic:%s ast %s", __func__, topicObj->name, ast);
  MND_TMQ_RETURN_CHECK(nodesStringToNode(ast, &pAst));
  MND_TMQ_RETURN_CHECK(qExtractResultSchema(pAst, &topicObj->schema.nCols, &topicObj->schema.pSchema));

  SPlanContext cxt = {.pAstRoot = pAst, .topicQuery = true};
  MND_TMQ_RETURN_CHECK(qCreateQueryPlan(&cxt, &pPlan, NULL));
  if (pPlan == NULL) {
    code = TSDB_CODE_MND_INVALID_TOPIC_QUERY;
    goto END;
  }
  int32_t levelNum = LIST_LENGTH(pPlan->pSubplans);
  if (levelNum != 1) {
    code = TSDB_CODE_MND_INVALID_TOPIC_QUERY;
    goto END;
  }

  SNodeListNode *pNodeListNode = (SNodeListNode *)nodesListGetNode(pPlan->pSubplans, 0);
  MND_TMQ_NULL_CHECK(pNodeListNode);
  int32_t opNum = LIST_LENGTH(pNodeListNode->pNodeList);
  if (opNum != 1) {
    code = TSDB_CODE_MND_INVALID_TOPIC_QUERY;
    goto END;
  }

  code = nodesNodeToString(nodesListGetNode(pNodeListNode->pNodeList, 0), false, &topicObj->physicalPlan, NULL);

END:
  nodesDestroyNode(pAst);
  qDestroyQueryPlan(pPlan);
  PRINT_LOG_END
  return code;
}

static int32_t mndCreateTopic(SMnode *pMnode, SRpcMsg *pReq, SCMCreateTopicReq *pCreate, SDbObj *pDb,
                              const char *userName) {
  if (pMnode == NULL || pReq == NULL || pCreate == NULL || pDb == NULL || userName == NULL)
    return TSDB_CODE_INVALID_PARA;
  STrans *    pTrans = NULL;
  int32_t     code = 0;
  int32_t     lino = 0;
  SMqTopicObj topicObj = {0};

  PRINT_LOG_START
  mInfo("start to create topic:%s", pCreate->name);
  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "create-topic");
  MND_TMQ_NULL_CHECK(pTrans);
  mndTransSetDbName(pTrans, pDb->name, NULL);
  MND_TMQ_RETURN_CHECK(mndTransCheckConflict(pMnode, pTrans));

  tstrncpy(topicObj.name, pCreate->name, TSDB_TOPIC_FNAME_LEN);
  tstrncpy(topicObj.db, pDb->name, TSDB_DB_FNAME_LEN);
  tstrncpy(topicObj.createUser, userName, TSDB_USER_LEN);

  MND_TMQ_RETURN_CHECK(mndCheckTopicPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_TOPIC, &topicObj));

  topicObj.createTime = taosGetTimestampMs();
  topicObj.updateTime = topicObj.createTime;
  topicObj.uid = mndGenerateUid(pCreate->name, strlen(pCreate->name));
  topicObj.dbUid = pDb->uid;
  topicObj.version = 1;
  topicObj.sql = taosStrdup(pCreate->sql);
  MND_TMQ_NULL_CHECK(topicObj.sql);
  topicObj.sqlLen = strlen(pCreate->sql) + 1;
  topicObj.subType = pCreate->subType;
  topicObj.withMeta = pCreate->withMeta;

  MND_TMQ_RETURN_CHECK(processAst(&topicObj, pCreate->ast));

  if (pCreate->subStbName[0] != 0) {
    tstrncpy(topicObj.stbName, pCreate->subStbName, TSDB_TABLE_FNAME_LEN);
    SStbObj *pStb = mndAcquireStb(pMnode, topicObj.stbName);
    MND_TMQ_NULL_CHECK(pStb);
    topicObj.stbUid = pStb->uid;
    mndReleaseStb(pMnode, pStb);
  }

  SSdbRaw *pCommitRaw = mndTopicActionEncode(&topicObj);
  MND_TMQ_NULL_CHECK(pCommitRaw);
  code = mndTransAppendCommitlog(pTrans, pCommitRaw);
  if (code != 0) {
    sdbFreeRaw(pCommitRaw);
    goto END;
  }

  MND_TMQ_RETURN_CHECK(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  MND_TMQ_RETURN_CHECK(mndTransPrepare(pMnode, pTrans));

END:
  PRINT_LOG_END
  taosMemoryFreeClear(topicObj.sql);
  taosMemoryFreeClear(topicObj.physicalPlan);
  if (topicObj.schema.nCols) {
    taosMemoryFreeClear(topicObj.schema.pSchema);
  }
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateTopicReq(SRpcMsg *pReq) {
  if (pReq == NULL || pReq->contLen <= 0) {
    return TSDB_CODE_INVALID_MSG;
  }
  SMnode *          pMnode = pReq->info.node;
  int32_t           code = TSDB_CODE_SUCCESS;
  int32_t           lino = 0;
  SMqTopicObj *     pTopic = NULL;
  SDbObj *          pDb = NULL;
  SCMCreateTopicReq createTopicReq = {0};

  PRINT_LOG_START
  MND_TMQ_RETURN_CHECK(tDeserializeSCMCreateTopicReq(pReq->pCont, pReq->contLen, &createTopicReq));

  mInfo("topic:%s start to create, sql:%s", createTopicReq.name, createTopicReq.sql);

  MND_TMQ_RETURN_CHECK(mndCheckCreateTopicReq(&createTopicReq));

  code = mndAcquireTopic(pMnode, createTopicReq.name, &pTopic);
  if (code == TSDB_CODE_SUCCESS) {
    if (createTopicReq.igExists) {
      mInfo("topic:%s already exist, ignore exist is set", createTopicReq.name);
    } else {
      code = TSDB_CODE_MND_TOPIC_ALREADY_EXIST;
    }
    goto END;
  } else if (code != TSDB_CODE_MND_TOPIC_NOT_EXIST) {
    goto END;
  }

  pDb = mndAcquireDb(pMnode, createTopicReq.subDbName);
  MND_TMQ_NULL_CHECK(pDb);

  if (pDb->cfg.walRetentionPeriod == 0) {
    code = TSDB_CODE_MND_DB_RETENTION_PERIOD_ZERO;
    goto END;
  }

  if (sdbGetSize(pMnode->pSdb, SDB_TOPIC) >= tmqMaxTopicNum) {
    code = TSDB_CODE_TMQ_TOPIC_OUT_OF_RANGE;
    goto END;
  }

  MND_TMQ_RETURN_CHECK(grantCheck(TSDB_GRANT_SUBSCRIPTION));
  MND_TMQ_RETURN_CHECK(mndCreateTopic(pMnode, pReq, &createTopicReq, pDb, pReq->info.conn.user));

  auditRecord(pReq, pMnode->clusterId, "createTopic", createTopicReq.subDbName, createTopicReq.name,
              createTopicReq.sql, strlen(createTopicReq.sql));
  code = TSDB_CODE_ACTION_IN_PROGRESS;

END:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("%s failed, topic:%s since %s", __func__, createTopicReq.name, tstrerror(code));
  } else {
    mInfo("topic:%s create successfully", createTopicReq.name);
  }
  mndReleaseTopic(pMnode, pTopic);
  mndReleaseDb(pMnode, pDb);

  tFreeSCMCreateTopicReq(&createTopicReq);
  return code;
}

static int32_t mndDropTopic(SMnode *pMnode, STrans *pTrans, SRpcMsg *pReq, SMqTopicObj *pTopic) {
  if (pMnode == NULL || pTrans == NULL || pReq == NULL || pTopic == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  int32_t  code = 0;
  int32_t  lino = 0;
  SSdbRaw *pCommitRaw = NULL;
  PRINT_LOG_START
  MND_TMQ_RETURN_CHECK(mndUserRemoveTopic(pMnode, pTrans, pTopic->name));
  pCommitRaw = mndTopicActionEncode(pTopic);
  MND_TMQ_NULL_CHECK(pCommitRaw);
  code = mndTransAppendCommitlog(pTrans, pCommitRaw);
  if (code != 0) {
    sdbFreeRaw(pCommitRaw);
    goto END;
  }
  MND_TMQ_RETURN_CHECK(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
  MND_TMQ_RETURN_CHECK(mndTransPrepare(pMnode, pTrans));

END:
  PRINT_LOG_END
  return code;
}

bool checkTopic(SArray *topics, char *topicName) {
  if (topics == NULL || topicName == NULL) {
    return false;
  }
  int32_t sz = taosArrayGetSize(topics);
  for (int32_t i = 0; i < sz; i++) {
    char *name = taosArrayGetP(topics, i);
    if (name && strcmp(name, topicName) == 0) {
      return true;
    }
  }
  return false;
}

static int32_t mndCheckConsumerByTopic(SMnode *pMnode, STrans *pTrans, char *topicName, bool deleteConsumer) {
  if (pMnode == NULL || pTrans == NULL || topicName == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  int32_t         code = 0;
  int32_t         lino = 0;
  SSdb *          pSdb = pMnode->pSdb;
  void *          pIter = NULL;
  SMqConsumerObj *pConsumer = NULL;
  SMqConsumerObj *pConsumerNew = NULL;

  PRINT_LOG_START
  while (1) {
    pIter = sdbFetch(pSdb, SDB_CONSUMER, pIter, (void **)&pConsumer);
    if (pIter == NULL) {
      break;
    }

    bool found1 = checkTopic(pConsumer->assignedTopics, topicName);
    bool found2 = checkTopic(pConsumer->rebRemovedTopics, topicName);
    bool found3 = checkTopic(pConsumer->rebNewTopics, topicName);
    if (found1 || found2 || found3) {
      if (deleteConsumer) {
        MND_TMQ_RETURN_CHECK(
            tNewSMqConsumerObj(pConsumer->consumerId, pConsumer->cgroup, -1, NULL, NULL, &pConsumerNew));
        MND_TMQ_RETURN_CHECK(mndSetConsumerDropLogs(pTrans, pConsumerNew));
        tDeleteSMqConsumerObj(pConsumerNew);
        pConsumerNew = NULL;
      } else {
        mError("topic:%s, failed to drop since subscribed by consumer:0x%" PRIx64 ", in consumer group %s", topicName,
               pConsumer->consumerId, pConsumer->cgroup);
        code = TSDB_CODE_MND_TOPIC_SUBSCRIBED;
        goto END;
      }
    }

    sdbRelease(pSdb, pConsumer);
  }

END:
  PRINT_LOG_END
  tDeleteSMqConsumerObj(pConsumerNew);
  sdbRelease(pSdb, pConsumer);
  sdbCancelFetch(pSdb, pIter);
  return code;
}

static int32_t mndProcessReloadTopicReq(SRpcMsg *pReq) {
  if (pReq == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  SMnode *         pMnode = pReq->info.node;
  SMReloadTopicReq reloadReq = {0};
  int32_t          code = 0;
  int32_t          lino = 0;
  SMqTopicObj *    pTopic = NULL;

  PRINT_LOG_START
  MND_TMQ_RETURN_CHECK(tDeserializeSMReloadTopicReq(pReq->pCont, pReq->contLen, &reloadReq));

  code = mndAcquireTopic(pMnode, reloadReq.name, &pTopic);
  if (code != 0) {
    if (reloadReq.igNotExists) {
      mInfo("topic:%s, not exist, ignore not exist is set", reloadReq.name);
      code = 0;
      goto END;
    } else {
      mError("topic:%s, failed to drop since %s", reloadReq.name, tstrerror(code));
      goto END;
    }
  }

  if (topicsToReload == NULL) {
    topicsToReload = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    MND_TMQ_NULL_CHECK(topicsToReload);
  }
  MND_TMQ_RETURN_CHECK(taosHashPut(topicsToReload, reloadReq.name, strlen(reloadReq.name), reloadReq.name, 1));
  mInfo("topic:%s, marked to reload", reloadReq.name);
  auditRecord(pReq, pMnode->clusterId, "reloadTopic", "", reloadReq.name, reloadReq.sql, reloadReq.sqlLen);

END:
  mndReleaseTopic(pMnode, pTopic);
  PRINT_LOG_END
  tFreeSMReloadTopicReq(&reloadReq);
  return code;
}

static int32_t mndProcessDropTopicReq(SRpcMsg *pReq) {
  if (pReq == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  SMnode *       pMnode = pReq->info.node;
  SMDropTopicReq dropReq = {0};
  int32_t        code = 0;
  int32_t        lino = 0;
  SMqTopicObj *  pTopic = NULL;
  STrans *       pTrans = NULL;

  PRINT_LOG_START
  MND_TMQ_RETURN_CHECK(tDeserializeSMDropTopicReq(pReq->pCont, pReq->contLen, &dropReq));

  code = mndAcquireTopic(pMnode, dropReq.name, &pTopic);
  if (code != 0) {
    if (dropReq.igNotExists) {
      mInfo("topic:%s, not exist, ignore not exist is set", dropReq.name);
      code = 0;
    }
    goto END;
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "drop-topic");
  MND_TMQ_NULL_CHECK(pTrans);

  mndTransSetDbName(pTrans, pTopic->db, NULL);
  MND_TMQ_RETURN_CHECK(mndTransCheckConflict(pMnode, pTrans));
  mInfo("trans:%d, used to drop topic:%s, force:%d", pTrans->id, pTopic->name, dropReq.force);

  MND_TMQ_RETURN_CHECK(mndCheckTopicPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_TOPIC, pTopic));
  MND_TMQ_RETURN_CHECK(mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_READ_DB, pTopic->db));
  MND_TMQ_RETURN_CHECK(mndCheckConsumerByTopic(pMnode, pTrans, dropReq.name, dropReq.force));
  MND_TMQ_RETURN_CHECK(mndDropSubByTopic(pMnode, pTrans, dropReq.name, dropReq.force));
  MND_TMQ_RETURN_CHECK(mndDropTopic(pMnode, pTrans, pReq, pTopic));
  auditRecord(pReq, pMnode->clusterId, "dropTopic", pTopic->db, dropReq.name, dropReq.sql, dropReq.sqlLen);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

END:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("%s failed, topic:%s since %s", __func__, dropReq.name, tstrerror(code));
  } else {
    mInfo("topic:%s dropped successfully", dropReq.name);
  }

  mndReleaseTopic(pMnode, pTopic);
  mndTransDrop(pTrans);
  tFreeSMDropTopicReq(&dropReq);
  return code;
}

int32_t mndGetNumOfTopics(SMnode *pMnode, char *dbName, int32_t *pNumOfTopics) {
  if (pMnode == NULL || dbName == NULL || pNumOfTopics == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  *pNumOfTopics = 0;

  SSdb *  pSdb = pMnode->pSdb;
  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }

  int32_t numOfTopics = 0;
  void *  pIter = NULL;
  while (1) {
    SMqTopicObj *pTopic = NULL;
    pIter = sdbFetch(pSdb, SDB_TOPIC, pIter, (void **)&pTopic);
    if (pIter == NULL) {
      break;
    }

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
  if (pReq == NULL || pShow == NULL || pBlock == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  SMnode *     pMnode = pReq->info.node;
  SSdb *       pSdb = pMnode->pSdb;
  int32_t      numOfRows = 0;
  SMqTopicObj *pTopic = NULL;
  int32_t      code = 0;
  int32_t     lino = 0;
  char *       sql = NULL;
  PRINT_LOG_START

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_TOPIC, pShow->pIter, (void **)&pTopic);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo = NULL;
    SName            n = {0};
    int32_t          cols = 0;

    char        topicName[TSDB_TOPIC_NAME_LEN + VARSTR_HEADER_SIZE + 5] = {0};
    const char *pName = mndGetDbStr(pTopic->name);
    STR_TO_VARSTR(topicName, pName);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    MND_TMQ_NULL_CHECK(pColInfo);
    MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)topicName, false));

    char dbName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    MND_TMQ_RETURN_CHECK(tNameFromString(&n, pTopic->db, T_NAME_ACCT | T_NAME_DB));
    MND_TMQ_RETURN_CHECK(tNameGetDbName(&n, varDataVal(dbName)));
    varDataSetLen(dbName, strlen(varDataVal(dbName)));

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    MND_TMQ_NULL_CHECK(pColInfo);
    MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)dbName, false));

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    MND_TMQ_NULL_CHECK(pColInfo);
    MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)&pTopic->createTime, false));

    sql = taosMemoryMalloc(strlen(pTopic->sql) + VARSTR_HEADER_SIZE);
    MND_TMQ_NULL_CHECK(sql);
    STR_TO_VARSTR(sql, pTopic->sql);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    MND_TMQ_NULL_CHECK(pColInfo);
    MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)sql, false));

    taosMemoryFreeClear(sql);

    char mete[4 + VARSTR_HEADER_SIZE] = {0};
    if (pTopic->withMeta) {
      STR_TO_VARSTR(mete, "yes");
    } else {
      STR_TO_VARSTR(mete, "no");
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    MND_TMQ_NULL_CHECK(pColInfo);
    MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)mete, false));

    numOfRows++;
    sdbRelease(pSdb, pTopic);
    pTopic = NULL;
  }

  pShow->numOfRows += numOfRows;

END:
  sdbCancelFetch(pSdb, pShow->pIter);
  sdbRelease(pSdb, pTopic);
  taosMemoryFreeClear(sql);
  if (code != TSDB_CODE_SUCCESS) {
    mError("%s failed since %s", __func__, tstrerror(code));
    return code;
  } else {
    mDebug("%s retrieved %d rows successfully", __func__, numOfRows);
    return numOfRows;
  }
}

static void mndCancelGetNextTopic(SMnode *pMnode, void *pIter) {
  if (pMnode == NULL) return;
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_TOPIC);
}

bool mndTopicExistsForDb(SMnode *pMnode, SDbObj *pDb) {
  if (pMnode == NULL || pDb == NULL) {
    return false;
  }
  SSdb *       pSdb = pMnode->pSdb;
  void *       pIter = NULL;
  SMqTopicObj *pTopic = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_TOPIC, pIter, (void **)&pTopic);
    if (pIter == NULL) {
      break;
    }

    if (pTopic->dbUid == pDb->uid) {
      sdbRelease(pSdb, pTopic);
      sdbCancelFetch(pSdb, pIter);
      return true;
    }

    sdbRelease(pSdb, pTopic);
  }

  return false;
}
