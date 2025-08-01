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
#include "parser.h"
#include "tname.h"
#include "audit.h"

#define MND_TOPIC_VER_NUMBER   3
#define MND_TOPIC_RESERVE_SIZE 64

SSdbRaw *mndTopicActionEncode(SMqTopicObj *pTopic);
SSdbRow *mndTopicActionDecode(SSdbRaw *pRaw);

static int32_t mndTopicActionInsert(SSdb *pSdb, SMqTopicObj *pTopic);
static int32_t mndTopicActionDelete(SSdb *pSdb, SMqTopicObj *pTopic);
static int32_t mndTopicActionUpdate(SSdb *pSdb, SMqTopicObj *pOldTopic, SMqTopicObj *pNewTopic);
static int32_t mndProcessCreateTopicReq(SRpcMsg *pReq);
static int32_t mndProcessDropTopicReq(SRpcMsg *pReq);

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
  mndSetMsgHandle(pMnode, TDMT_VND_TMQ_ADD_CHECKINFO_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_TMQ_DEL_CHECKINFO_RSP, mndTransProcessRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TOPICS, mndRetrieveTopic);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TOPICS, mndCancelGetNextTopic);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupTopic(SMnode *pMnode) {}

void mndTopicGetShowName(const char* fullTopic, char* topic) {
  if (fullTopic == NULL) {
    return;
  }
  char* tmp = strchr(fullTopic, '.');
  if (tmp == NULL) {
    tstrncpy(topic, fullTopic, TSDB_TOPIC_FNAME_LEN);
  }else {
    tstrncpy(topic, tmp+1, TSDB_TOPIC_FNAME_LEN);
  }
}

SSdbRaw *mndTopicActionEncode(SMqTopicObj *pTopic) {
  if (pTopic == NULL) {
    return NULL;
  }
  int32_t code = 0;
  int32_t lino = 0;
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
  int32_t ntbColLen = taosArrayGetSize(pTopic->ntbColIds) * sizeof(int16_t);

  int32_t size = sizeof(SMqTopicObj) + physicalPlanLen + pTopic->sqlLen + pTopic->astLen + schemaLen + ntbColLen +
                 MND_TOPIC_RESERVE_SIZE;
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
    if(taosEncodeSSchemaWrapper(&aswBuf, &pTopic->schema) < 0){
      goto TOPIC_ENCODE_OVER;
    }
    SDB_SET_BINARY(pRaw, dataPos, swBuf, schemaLen, TOPIC_ENCODE_OVER);
  }

  SDB_SET_INT64(pRaw, dataPos, pTopic->ntbUid, TOPIC_ENCODE_OVER);
  if (pTopic->ntbUid != 0) {
    int32_t sz = taosArrayGetSize(pTopic->ntbColIds);
    SDB_SET_INT32(pRaw, dataPos, sz, TOPIC_ENCODE_OVER);
    for (int32_t i = 0; i < sz; i++) {
      int16_t colId = *(int16_t *)taosArrayGet(pTopic->ntbColIds, i);
      SDB_SET_INT16(pRaw, dataPos, colId, TOPIC_ENCODE_OVER);
    }
  }

  SDB_SET_INT64(pRaw, dataPos, pTopic->ctbStbUid, TOPIC_ENCODE_OVER);

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
  if (pRaw == NULL) return NULL;
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow     *pRow = NULL;
  SMqTopicObj *pTopic = NULL;
  void        *buf = NULL;

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
  SDB_GET_INT64(pRaw, dataPos, &pTopic->ntbUid, TOPIC_DECODE_OVER);
  if (pTopic->ntbUid != 0) {
    int32_t ntbColNum;
    SDB_GET_INT32(pRaw, dataPos, &ntbColNum, TOPIC_DECODE_OVER);
    pTopic->ntbColIds = taosArrayInit(ntbColNum, sizeof(int16_t));
    if (pTopic->ntbColIds == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto TOPIC_DECODE_OVER;
    }
    int16_t colId;
    SDB_GET_INT16(pRaw, dataPos, &colId, TOPIC_DECODE_OVER);
    if (taosArrayPush(pTopic->ntbColIds, &colId) == NULL) {
      goto TOPIC_DECODE_OVER;
    }
  }

  SDB_GET_INT64(pRaw, dataPos, &pTopic->ctbStbUid, TOPIC_DECODE_OVER);
  SDB_GET_RESERVE(pRaw, dataPos, MND_TOPIC_RESERVE_SIZE, TOPIC_DECODE_OVER);
  terrno = TSDB_CODE_SUCCESS;

TOPIC_DECODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("topic:%s, failed to decode from raw:%p since %s", pTopic == NULL ? "null" : pTopic->name, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("topic:%s, decode from raw:%p, row:%p", pTopic->name, pRaw, pTopic);
  return pRow;
}

static int32_t mndTopicActionInsert(SSdb *pSdb, SMqTopicObj *pTopic) {
  mTrace("topic:%s perform insert action", pTopic != NULL ? pTopic->name : "null");
  return 0;
}

static int32_t mndTopicActionDelete(SSdb *pSdb, SMqTopicObj *pTopic) {
  if (pTopic == NULL) return 0;
  mTrace("topic:%s perform delete action", pTopic->name);
  taosMemoryFreeClear(pTopic->sql);
  taosMemoryFreeClear(pTopic->ast);
  taosMemoryFreeClear(pTopic->physicalPlan);
  if (pTopic->schema.nCols) taosMemoryFreeClear(pTopic->schema.pSchema);
  taosArrayDestroy(pTopic->ntbColIds);
  return 0;
}

static int32_t mndTopicActionUpdate(SSdb *pSdb, SMqTopicObj *pOldTopic, SMqTopicObj *pNewTopic) {
  if (pOldTopic == NULL || pNewTopic == NULL) return 0;
  mTrace("topic:%s perform update action", pOldTopic->name);
  (void)atomic_exchange_64(&pOldTopic->updateTime, pNewTopic->updateTime);
  (void)atomic_exchange_32(&pOldTopic->version, pNewTopic->version);

  return 0;
}

int32_t mndAcquireTopic(SMnode *pMnode, const char *topicName, SMqTopicObj **pTopic) {
  if (pMnode == NULL || topicName == NULL || pTopic == NULL){
    return TSDB_CODE_INVALID_PARA;
  }
  SSdb        *pSdb = pMnode->pSdb;
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

static int32_t extractTopicTbInfo(SNode *pAst, SMqTopicObj *pTopic) {
  if (pAst == NULL || pTopic == NULL) return TSDB_CODE_INVALID_PARA;
  SNodeList *pNodeList = NULL;
  int32_t   code = 0;
  MND_TMQ_RETURN_CHECK(nodesCollectColumns((SSelectStmt *)pAst, SQL_CLAUSE_FROM, NULL, COLLECT_COL_TYPE_ALL, &pNodeList));
  int64_t suid = ((SRealTableNode *)((SSelectStmt *)pAst)->pFromTable)->pMeta->suid;
  int8_t  tableType = ((SRealTableNode *)((SSelectStmt *)pAst)->pFromTable)->pMeta->tableType;
  if (tableType == TSDB_CHILD_TABLE) {
    pTopic->ctbStbUid = suid;
  } else if (tableType == TSDB_NORMAL_TABLE) {
    SNode *pNode = NULL;
    FOREACH(pNode, pNodeList) {
      SColumnNode *pCol = (SColumnNode *)pNode;
      if (pCol->tableType == TSDB_NORMAL_TABLE) {
        pTopic->ntbUid = pCol->tableId;
        MND_TMQ_NULL_CHECK(taosArrayPush(pTopic->ntbColIds, &pCol->colId));
      }
    }
  }
  nodesDestroyList(pNodeList);

END:
  return code;
}

static int32_t sendCheckInfoToVnode(STrans *pTrans, SMnode *pMnode, SMqTopicObj *topicObj){
  if (pTrans == NULL || pMnode == NULL || topicObj == NULL) return TSDB_CODE_INVALID_PARA;
  STqCheckInfo info = {0};
  (void)memcpy(info.topic, topicObj->name, TSDB_TOPIC_FNAME_LEN);
  info.ntbUid = topicObj->ntbUid;
  info.colIdList = topicObj->ntbColIds;
  // broadcast forbid alter info
  void   *pIter = NULL;
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  int32_t code = 0;
  void   *buf = NULL;

  while (1) {
    // iterate vg
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->mountVgId) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }
    if (!mndVgroupInDb(pVgroup, topicObj->dbUid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    // encoder check alter info
    int32_t len = 0;
    tEncodeSize(tEncodeSTqCheckInfo, &info, len, code);
    if (code != 0) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto END;
    }
    buf = taosMemoryCalloc(1, sizeof(SMsgHead) + len);
    MND_TMQ_NULL_CHECK(buf);
    void    *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
    SEncoder encoder = {0};
    tEncoderInit(&encoder, abuf, len);
    code = tEncodeSTqCheckInfo(&encoder, &info);
    if (code < 0) {
      tEncoderClear(&encoder);
      goto END;
    }
    tEncoderClear(&encoder);
    ((SMsgHead *)buf)->vgId = htonl(pVgroup->vgId);
    // add redo action
    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = buf;
    action.contLen = sizeof(SMsgHead) + len;
    action.msgType = TDMT_VND_TMQ_ADD_CHECKINFO;
    MND_TMQ_RETURN_CHECK(mndTransAppendRedoAction(pTrans, &action));
    sdbRelease(pSdb, pVgroup);
    buf = NULL;
  }

END:
  taosMemoryFree(buf);
  sdbRelease(pSdb, pVgroup);
  sdbCancelFetch(pSdb, pIter);
  return code;
}

static int32_t mndCreateTopic(SMnode *pMnode, SRpcMsg *pReq, SCMCreateTopicReq *pCreate, SDbObj *pDb,
                              const char *userName) {
  if (pMnode == NULL || pReq == NULL || pCreate == NULL || pDb == NULL || userName == NULL) return TSDB_CODE_INVALID_PARA;
  mInfo("start to create topic:%s", pCreate->name);
  STrans *pTrans = NULL;
  int32_t code = 0;
  SNode  *pAst = NULL;
  SQueryPlan *pPlan = NULL;
  SMqTopicObj topicObj = {0};

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "create-topic");
  MND_TMQ_NULL_CHECK(pTrans);
  mndTransSetDbName(pTrans, pDb->name, NULL);
  MND_TMQ_RETURN_CHECK(mndTransCheckConflict(pMnode, pTrans));
  mInfo("trans:%d to create topic:%s", pTrans->id, pCreate->name);

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

  if (pCreate->subType == TOPIC_SUB_TYPE__COLUMN) {
    topicObj.ast = taosStrdup(pCreate->ast);
    MND_TMQ_NULL_CHECK(topicObj.ast);
    topicObj.astLen = strlen(pCreate->ast) + 1;
    qDebugL("topic:%s ast %s", topicObj.name, topicObj.ast);
    MND_TMQ_RETURN_CHECK(nodesStringToNode(pCreate->ast, &pAst));
    SPlanContext cxt = {.pAstRoot = pAst, .topicQuery = true};
    MND_TMQ_RETURN_CHECK(qCreateQueryPlan(&cxt, &pPlan, NULL));

    topicObj.ntbColIds = taosArrayInit(0, sizeof(int16_t));
    MND_TMQ_NULL_CHECK(topicObj.ntbColIds);
    MND_TMQ_RETURN_CHECK(extractTopicTbInfo(pAst, &topicObj));
    if (topicObj.ntbUid == 0) {
      taosArrayDestroy(topicObj.ntbColIds);
      topicObj.ntbColIds = NULL;
    }

    MND_TMQ_RETURN_CHECK(qExtractResultSchema(pAst, &topicObj.schema.nCols, &topicObj.schema.pSchema));
    MND_TMQ_RETURN_CHECK(nodesNodeToString((SNode *)pPlan, false, &topicObj.physicalPlan, NULL));
  } else if (pCreate->subType == TOPIC_SUB_TYPE__TABLE) {
    SStbObj *pStb = mndAcquireStb(pMnode, pCreate->subStbName);
    MND_TMQ_NULL_CHECK(pStb);
    tstrncpy(topicObj.stbName, pCreate->subStbName, TSDB_TABLE_FNAME_LEN);
    topicObj.stbUid = pStb->uid;
    mndReleaseStb(pMnode, pStb);
    if(pCreate->ast != NULL){
      qDebugL("topic:%s ast %s", topicObj.name, pCreate->ast);
      topicObj.ast = taosStrdup(pCreate->ast);
      MND_TMQ_NULL_CHECK(topicObj.ast);
      topicObj.astLen = strlen(pCreate->ast) + 1;
    }
  }

  SSdbRaw *pCommitRaw = mndTopicActionEncode(&topicObj);
  MND_TMQ_NULL_CHECK(pCommitRaw);
  code = mndTransAppendCommitlog(pTrans, pCommitRaw);
  if(code != 0) {
    sdbFreeRaw(pCommitRaw);
    goto END;
  }

  MND_TMQ_RETURN_CHECK(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  if (topicObj.ntbUid != 0) {
    MND_TMQ_RETURN_CHECK(sendCheckInfoToVnode(pTrans, pMnode, &topicObj));
  }
  MND_TMQ_RETURN_CHECK(mndTransPrepare(pMnode, pTrans));
  code = TSDB_CODE_ACTION_IN_PROGRESS;

END:
  taosMemoryFreeClear(topicObj.physicalPlan);
  taosMemoryFreeClear(topicObj.sql);
  taosMemoryFreeClear(topicObj.ast);
  taosArrayDestroy(topicObj.ntbColIds);
  if (topicObj.schema.nCols) {
    taosMemoryFreeClear(topicObj.schema.pSchema);
  }
  nodesDestroyNode(pAst);
  nodesDestroyNode((SNode *)pPlan);
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateTopicReq(SRpcMsg *pReq) {
  if (pReq == NULL || pReq->contLen <= 0) {
    return TSDB_CODE_INVALID_MSG;
  }
  SMnode           *pMnode = pReq->info.node;
  int32_t           code = TSDB_CODE_SUCCESS;
  SMqTopicObj      *pTopic = NULL;
  SDbObj           *pDb = NULL;
  SCMCreateTopicReq createTopicReq = {0};

  PRINT_LOG_START
  if (tDeserializeSCMCreateTopicReq(pReq->pCont, pReq->contLen, &createTopicReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto END;
  }

  mInfo("topic:%s start to create, sql:%s", createTopicReq.name, createTopicReq.sql);

  MND_TMQ_RETURN_CHECK(mndCheckCreateTopicReq(&createTopicReq));

  code = mndAcquireTopic(pMnode, createTopicReq.name, &pTopic);
  if (code == TSDB_CODE_SUCCESS) {
    if (createTopicReq.igExists) {
      mInfo("topic:%s already exist, ignore exist is set", createTopicReq.name);
      goto END;
    } else {
      code = TSDB_CODE_MND_TOPIC_ALREADY_EXIST;
      goto END;
    }
  } else if (code != TSDB_CODE_MND_TOPIC_NOT_EXIST) {
    goto END;
  }

  pDb = mndAcquireDb(pMnode, createTopicReq.subDbName);
  MND_TMQ_NULL_CHECK(pDb);

  if (pDb->cfg.walRetentionPeriod == 0) {
    code = TSDB_CODE_MND_DB_RETENTION_PERIOD_ZERO;
    mError("db:%s, not allowed to create topic when WAL_RETENTION_PERIOD is zero", pDb->name);
    goto END;
  }

  if (sdbGetSize(pMnode->pSdb, SDB_TOPIC) >= tmqMaxTopicNum){
    code = TSDB_CODE_TMQ_TOPIC_OUT_OF_RANGE;
    mError("topic num out of range");
    goto END;
  }

  MND_TMQ_RETURN_CHECK(grantCheck(TSDB_GRANT_SUBSCRIPTION));
  code = mndCreateTopic(pMnode, pReq, &createTopicReq, pDb, pReq->info.conn.user);
  if (code == 0) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  {
    SName dbname = {0};
    int32_t ret = tNameFromString(&dbname, createTopicReq.subDbName, T_NAME_ACCT | T_NAME_DB);
    if (ret != 0){
      mError("failed to parse db name:%s, ret:%d", createTopicReq.subDbName, ret);
    }
    SName topicName = {0};
    ret = tNameFromString(&topicName, createTopicReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    if (ret != 0){
      mError("failed to parse topic name:%s, ret:%d", createTopicReq.name, ret);
    }
    auditRecord(pReq, pMnode->clusterId, "createTopic", dbname.dbname, topicName.dbname,
                createTopicReq.sql, strlen(createTopicReq.sql));
  }

END:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to create topic:%s since %s", createTopicReq.name, tstrerror(code));
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
  int32_t code = 0;
  SSdbRaw *pCommitRaw = NULL;
  MND_TMQ_RETURN_CHECK(mndUserRemoveTopic(pMnode, pTrans, pTopic->name));
  pCommitRaw = mndTopicActionEncode(pTopic);
  MND_TMQ_NULL_CHECK(pCommitRaw);
  code = mndTransAppendCommitlog(pTrans, pCommitRaw);
  if(code != 0) {
    sdbFreeRaw(pCommitRaw);
    goto END;
  }
  MND_TMQ_RETURN_CHECK(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));
  MND_TMQ_RETURN_CHECK(mndTransPrepare(pMnode, pTrans));

END:
  return code;
}

bool checkTopic(SArray *topics, char *topicName){
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

static int32_t mndCheckConsumerByTopic(SMnode *pMnode, STrans *pTrans, char *topicName, bool deleteConsumer){
  if (pMnode == NULL || pTrans == NULL || topicName == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  int32_t         code = 0;
  SSdb           *pSdb    = pMnode->pSdb;
  void           *pIter = NULL;
  SMqConsumerObj *pConsumer = NULL;
  SMqConsumerObj *pConsumerNew = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_CONSUMER, pIter, (void **)&pConsumer);
    if (pIter == NULL) {
      break;
    }

    bool found = checkTopic(pConsumer->assignedTopics, topicName);
    if (found) {
      if (deleteConsumer) {
        MND_TMQ_RETURN_CHECK(tNewSMqConsumerObj(pConsumer->consumerId, pConsumer->cgroup, -1, NULL, NULL, &pConsumerNew));
        MND_TMQ_RETURN_CHECK(mndSetConsumerDropLogs(pTrans, pConsumerNew));
        tDeleteSMqConsumerObj(pConsumerNew);
        pConsumerNew = NULL;
      } else {
        mError("topic:%s, failed to drop since subscribed by consumer:0x%" PRIx64 ", in consumer group %s",
               topicName, pConsumer->consumerId, pConsumer->cgroup);
        code = TSDB_CODE_MND_TOPIC_SUBSCRIBED;
        goto END;
      }
    }

    sdbRelease(pSdb, pConsumer);
  }

END:
  tDeleteSMqConsumerObj(pConsumerNew);
  sdbRelease(pSdb, pConsumer);
  sdbCancelFetch(pSdb, pIter);
  return code;
}

static int32_t mndDropCheckInfoByTopic(SMnode *pMnode, STrans *pTrans, SMqTopicObj *pTopic){
  if (pMnode == NULL || pTrans == NULL || pTopic == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  // broadcast to all vnode
  void   *pIter = NULL;
  SVgObj *pVgroup = NULL;
  int32_t code    = 0;
  SSdb   *pSdb    = pMnode->pSdb;
  void   *buf     = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->mountVgId) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }
    if (!mndVgroupInDb(pVgroup, pTopic->dbUid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    buf = taosMemoryCalloc(1, sizeof(SMsgHead) + TSDB_TOPIC_FNAME_LEN);
    MND_TMQ_NULL_CHECK(buf);
    void *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
    ((SMsgHead *)buf)->vgId = htonl(pVgroup->vgId);
    (void)memcpy(abuf, pTopic->name, TSDB_TOPIC_FNAME_LEN);

    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = buf;
    action.contLen = sizeof(SMsgHead) + TSDB_TOPIC_FNAME_LEN;
    action.msgType = TDMT_VND_TMQ_DEL_CHECKINFO;
    code = mndTransAppendRedoAction(pTrans, &action);
    if (code != 0) {
      taosMemoryFree(buf);
      goto END;
    }
    sdbRelease(pSdb, pVgroup);
  }

END:
  sdbRelease(pSdb, pVgroup);
  sdbCancelFetch(pSdb, pIter);
  return code;
}

static int32_t mndProcessDropTopicReq(SRpcMsg *pReq) {
  if (pReq == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  SMnode        *pMnode  = pReq->info.node;
  SMDropTopicReq dropReq = {0};
  int32_t        code = 0;
  SMqTopicObj *pTopic = NULL;
  STrans      *pTrans = NULL;

  PRINT_LOG_START
  if (tDeserializeSMDropTopicReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  code = mndAcquireTopic(pMnode, dropReq.name, &pTopic);
  if (code != 0) {
    if (dropReq.igNotExists) {
      mInfo("topic:%s, not exist, ignore not exist is set", dropReq.name);
      tFreeSMDropTopicReq(&dropReq);
      return 0;
    } else {
      mError("topic:%s, failed to drop since %s", dropReq.name, tstrerror(code));
      tFreeSMDropTopicReq(&dropReq);
      return code;
    }
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "drop-topic");
  MND_TMQ_NULL_CHECK(pTrans);

  mndTransSetDbName(pTrans, pTopic->db, NULL);
  MND_TMQ_RETURN_CHECK(mndTransCheckConflict(pMnode, pTrans));
  mInfo("trans:%d, used to drop topic:%s", pTrans->id, pTopic->name);

  MND_TMQ_RETURN_CHECK(mndCheckTopicPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_TOPIC, pTopic));
  MND_TMQ_RETURN_CHECK(mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_READ_DB, pTopic->db));
  MND_TMQ_RETURN_CHECK(mndCheckConsumerByTopic(pMnode, pTrans, dropReq.name, dropReq.force));
  MND_TMQ_RETURN_CHECK(mndDropSubByTopic(pMnode, pTrans, dropReq.name, dropReq.force));

  if (pTopic->ntbUid != 0) {
    MND_TMQ_RETURN_CHECK(mndDropCheckInfoByTopic(pMnode, pTrans, pTopic));
  }

  code = mndDropTopic(pMnode, pTrans, pReq, pTopic);

END:
  mndReleaseTopic(pMnode, pTopic);
  mndTransDrop(pTrans);
  if (code != 0) {
    mError("topic:%s, failed to drop since %s", dropReq.name, tstrerror(code));
    tFreeSMDropTopicReq(&dropReq);
    return code;
  }

  SName name = {0};
  int32_t ret = tNameFromString(&name, dropReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (ret != 0) {
    mError("topic:%s, failed to drop since %s", dropReq.name, tstrerror(ret));
  }
  auditRecord(pReq, pMnode->clusterId, "dropTopic", name.dbname, name.tname, dropReq.sql, dropReq.sqlLen);

  tFreeSMDropTopicReq(&dropReq);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}

int32_t mndGetNumOfTopics(SMnode *pMnode, char *dbName, int32_t *pNumOfTopics) {
  if (pMnode == NULL || dbName == NULL || pNumOfTopics == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  *pNumOfTopics = 0;

  SSdb   *pSdb = pMnode->pSdb;
  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }

  int32_t numOfTopics = 0;
  void   *pIter = NULL;
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

static void schemaToJson(SSchema *schema, int32_t nCols, char *schemaJson){
  if (schema == NULL || schemaJson == NULL) {
    return;
  }
  char*   string = NULL;
  int32_t code = 0;
  cJSON* columns = cJSON_CreateArray();
  MND_TMQ_NULL_CHECK(columns);
  for (int i = 0; i < nCols; i++) {
    cJSON*   column = cJSON_CreateObject();
    MND_TMQ_NULL_CHECK(column);
    SSchema* s = schema + i;
    cJSON*   cname = cJSON_CreateString(s->name);
    MND_TMQ_NULL_CHECK(cname);
    if (!cJSON_AddItemToObject(column, "name", cname)) {
      return;
    }
    cJSON* ctype = cJSON_CreateString(tDataTypes[s->type].name);
    MND_TMQ_NULL_CHECK(ctype);
    if (!cJSON_AddItemToObject(column, "type", ctype)) {
      return;
    }
    int32_t length = 0;
    if (s->type == TSDB_DATA_TYPE_BINARY || s->type == TSDB_DATA_TYPE_VARBINARY ||
        s->type == TSDB_DATA_TYPE_GEOMETRY) {
      length = s->bytes - VARSTR_HEADER_SIZE;
    } else if (s->type == TSDB_DATA_TYPE_NCHAR || s->type == TSDB_DATA_TYPE_JSON) {
      length = (s->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
    } else{
      length = s->bytes;
    }
    cJSON*  cbytes = cJSON_CreateNumber(length);
    MND_TMQ_NULL_CHECK(cbytes);
    if (!cJSON_AddItemToObject(column, "length", cbytes)){
      return;
    }
    if (!cJSON_AddItemToArray(columns, column)){
      return;
    }
  }
  string = cJSON_PrintUnformatted(columns);
  cJSON_Delete(columns);

  size_t len = strlen(string);
  if(string && len <= TSDB_SHOW_SCHEMA_JSON_LEN){
    STR_TO_VARSTR(schemaJson, string);
  }else{
    mError("mndRetrieveTopic build schema error json:%p, json len:%zu", string, len);
  }
  taosMemoryFree(string);

END:
  return;
}

static int32_t mndRetrieveTopic(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  if (pReq == NULL || pShow == NULL || pBlock == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  SMnode      *pMnode = pReq->info.node;
  SSdb        *pSdb = pMnode->pSdb;
  int32_t      numOfRows = 0;
  SMqTopicObj *pTopic = NULL;
  int32_t      code = 0;
  char        *sql  = NULL;
  char        *schemaJson  = NULL;

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_TOPIC, pShow->pIter, (void **)&pTopic);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo= NULL;
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

    schemaJson = taosMemoryMalloc(TSDB_SHOW_SCHEMA_JSON_LEN + VARSTR_HEADER_SIZE);
    MND_TMQ_NULL_CHECK(schemaJson);
    if(pTopic->subType == TOPIC_SUB_TYPE__COLUMN){
      schemaToJson(pTopic->schema.pSchema, pTopic->schema.nCols, schemaJson);
    }else if(pTopic->subType == TOPIC_SUB_TYPE__TABLE){
      SStbObj *pStb = mndAcquireStb(pMnode, pTopic->stbName);
      if (pStb == NULL) {
        STR_TO_VARSTR(schemaJson, "NULL");
        mError("mndRetrieveTopic mndAcquireStb null stbName:%s", pTopic->stbName);
      }else{
        schemaToJson(pStb->pColumns, pStb->numOfColumns, schemaJson);
        mndReleaseStb(pMnode, pStb);
      }
    }else{
      STR_TO_VARSTR(schemaJson, "NULL");
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    MND_TMQ_NULL_CHECK(pColInfo);
    MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)schemaJson, false));
    taosMemoryFreeClear(schemaJson);

    char mete[4 + VARSTR_HEADER_SIZE] = {0};
    if(pTopic->withMeta){
      STR_TO_VARSTR(mete, "yes");
    }else{
      STR_TO_VARSTR(mete, "no");
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    MND_TMQ_NULL_CHECK(pColInfo);
    MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)mete, false));

    char type[8 + VARSTR_HEADER_SIZE] = {0};
    if(pTopic->subType == TOPIC_SUB_TYPE__COLUMN){
      STR_TO_VARSTR(type, "column");
    }else if(pTopic->subType == TOPIC_SUB_TYPE__TABLE){
      STR_TO_VARSTR(type, "stable");
    }else{
      STR_TO_VARSTR(type, "db");
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    MND_TMQ_NULL_CHECK(pColInfo);
    MND_TMQ_RETURN_CHECK(colDataSetVal(pColInfo, numOfRows, (const char *)type, false));

    numOfRows++;
    sdbRelease(pSdb, pTopic);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;

END:
  taosMemoryFreeClear(schemaJson);
  taosMemoryFreeClear(sql);
  return code;
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
  SSdb        *pSdb = pMnode->pSdb;
  void        *pIter = NULL;
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
