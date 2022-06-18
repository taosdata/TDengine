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

#include "mndStream.h"
#include "mndAuth.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndScheduler.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "parser.h"
#include "tname.h"

#define MND_STREAM_VER_NUMBER   1
#define MND_STREAM_RESERVE_SIZE 64

static int32_t mndStreamActionInsert(SSdb *pSdb, SStreamObj *pStream);
static int32_t mndStreamActionDelete(SSdb *pSdb, SStreamObj *pStream);
static int32_t mndStreamActionUpdate(SSdb *pSdb, SStreamObj *pStream, SStreamObj *pNewStream);
static int32_t mndProcessCreateStreamReq(SRpcMsg *pReq);
static int32_t mndProcessDropStreamReq(SRpcMsg *pReq);
/*static int32_t mndProcessDropStreamInRsp(SRpcMsg *pRsp);*/
static int32_t mndProcessStreamMetaReq(SRpcMsg *pReq);
static int32_t mndGetStreamMeta(SRpcMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t mndRetrieveStream(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextStream(SMnode *pMnode, void *pIter);

int32_t mndInitStream(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_STREAM,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndStreamActionEncode,
      .decodeFp = (SdbDecodeFp)mndStreamActionDecode,
      .insertFp = (SdbInsertFp)mndStreamActionInsert,
      .updateFp = (SdbUpdateFp)mndStreamActionUpdate,
      .deleteFp = (SdbDeleteFp)mndStreamActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_STREAM, mndProcessCreateStreamReq);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_DEPLOY_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_STREAM, mndProcessDropStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_STREAM_RSP, mndTransProcessRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndRetrieveStream);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndCancelGetNextStream);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupStream(SMnode *pMnode) {}

SSdbRaw *mndStreamActionEncode(SStreamObj *pStream) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  void *buf = NULL;

  SEncoder encoder;
  tEncoderInit(&encoder, NULL, 0);
  if (tEncodeSStreamObj(&encoder, pStream) < 0) {
    tEncoderClear(&encoder);
    goto STREAM_ENCODE_OVER;
  }
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  int32_t  size = sizeof(int32_t) + tlen + MND_STREAM_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_STREAM, MND_STREAM_VER_NUMBER, size);
  if (pRaw == NULL) goto STREAM_ENCODE_OVER;

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) goto STREAM_ENCODE_OVER;

  tEncoderInit(&encoder, buf, tlen);
  if (tEncodeSStreamObj(&encoder, pStream) < 0) {
    tEncoderClear(&encoder);
    goto STREAM_ENCODE_OVER;
  }
  tEncoderClear(&encoder);

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, STREAM_ENCODE_OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, STREAM_ENCODE_OVER);
  SDB_SET_DATALEN(pRaw, dataPos, STREAM_ENCODE_OVER);

  terrno = TSDB_CODE_SUCCESS;

STREAM_ENCODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("stream:%s, failed to encode to raw:%p since %s", pStream->name, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("stream:%s, encode to raw:%p, row:%p", pStream->name, pRaw, pStream);
  return pRaw;
}

SSdbRow *mndStreamActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  void *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto STREAM_DECODE_OVER;

  if (sver != MND_STREAM_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto STREAM_DECODE_OVER;
  }

  int32_t  size = sizeof(SStreamObj);
  SSdbRow *pRow = sdbAllocRow(size);
  if (pRow == NULL) goto STREAM_DECODE_OVER;

  SStreamObj *pStream = sdbGetRowObj(pRow);
  if (pStream == NULL) goto STREAM_DECODE_OVER;

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, STREAM_DECODE_OVER);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) goto STREAM_DECODE_OVER;
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, STREAM_DECODE_OVER);

  SDecoder decoder;
  tDecoderInit(&decoder, buf, tlen + 1);
  if (tDecodeSStreamObj(&decoder, pStream) < 0) {
    goto STREAM_DECODE_OVER;
  }

  terrno = TSDB_CODE_SUCCESS;

STREAM_DECODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("stream:%s, failed to decode from raw:%p since %s", pStream->name, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("stream:%s, decode from raw:%p, row:%p", pStream->name, pRaw, pStream);
  return pRow;
}

static int32_t mndStreamActionInsert(SSdb *pSdb, SStreamObj *pStream) {
  mTrace("stream:%s, perform insert action", pStream->name);
  return 0;
}

static int32_t mndStreamActionDelete(SSdb *pSdb, SStreamObj *pStream) {
  mTrace("stream:%s, perform delete action", pStream->name);
  return 0;
}

static int32_t mndStreamActionUpdate(SSdb *pSdb, SStreamObj *pOldStream, SStreamObj *pNewStream) {
  mTrace("stream:%s, perform update action", pOldStream->name);
  atomic_exchange_64(&pOldStream->updateTime, pNewStream->updateTime);
  atomic_exchange_32(&pOldStream->version, pNewStream->version);

  taosWLockLatch(&pOldStream->lock);

  // TODO handle update

  taosWUnLockLatch(&pOldStream->lock);
  return 0;
}

SStreamObj *mndAcquireStream(SMnode *pMnode, char *streamName) {
  SSdb       *pSdb = pMnode->pSdb;
  SStreamObj *pStream = sdbAcquire(pSdb, SDB_STREAM, streamName);
  if (pStream == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
  }
  return pStream;
}

void mndReleaseStream(SMnode *pMnode, SStreamObj *pStream) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pStream);
}

static int32_t mndCheckCreateStreamReq(SCMCreateStreamReq *pCreate) {
  if (pCreate->name[0] == 0 || pCreate->sql == NULL || pCreate->sql[0] == 0 || pCreate->sourceDB[0] == 0 ||
      pCreate->targetStbFullName[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_STREAM_OPTION;
    return -1;
  }
  return 0;
}

static int32_t mndStreamGetPlanString(const char *ast, int8_t triggerType, int64_t watermark, char **pStr) {
  if (NULL == ast) {
    return TSDB_CODE_SUCCESS;
  }

  SNode  *pAst = NULL;
  int32_t code = nodesStringToNode(ast, &pAst);

  SQueryPlan *pPlan = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    SPlanContext cxt = {
        .pAstRoot = pAst,
        .topicQuery = false,
        .streamQuery = true,
        .triggerType = triggerType == STREAM_TRIGGER_MAX_DELAY ? STREAM_TRIGGER_WINDOW_CLOSE : triggerType,
        .watermark = watermark,
    };
    code = qCreateQueryPlan(&cxt, &pPlan, NULL);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = nodesNodeToString((SNode *)pPlan, false, pStr, NULL);
  }
  nodesDestroyNode(pAst);
  nodesDestroyNode((SNode *)pPlan);
  terrno = code;
  return code;
}

static int32_t mndBuildStreamObjFromCreateReq(SMnode *pMnode, SStreamObj *pObj, SCMCreateStreamReq *pCreate) {
  SNode *pAst = NULL;

  mDebug("stream:%s to create", pCreate->name);
  memcpy(pObj->name, pCreate->name, TSDB_STREAM_FNAME_LEN);
  pObj->createTime = taosGetTimestampMs();
  pObj->updateTime = pObj->createTime;
  pObj->version = 1;
  pObj->smaId = 0;

  pObj->uid = mndGenerateUid(pObj->name, strlen(pObj->name));
  pObj->status = 0;

  // TODO
  pObj->dropPolicy = 0;
  pObj->trigger = pCreate->triggerType;
  pObj->triggerParam = pCreate->maxDelay;
  pObj->watermark = pCreate->watermark;

  memcpy(pObj->sourceDb, pCreate->sourceDB, TSDB_DB_FNAME_LEN);
  SDbObj *pSourceDb = mndAcquireDb(pMnode, pCreate->sourceDB);
  if (pSourceDb == NULL) {
    /*ASSERT(0);*/
    mDebug("stream:%s failed to create, source db %s not exist", pCreate->name, pObj->sourceDb);
    terrno = TSDB_CODE_MND_DB_NOT_EXIST;
    return -1;
  }
  pObj->sourceDbUid = pSourceDb->uid;

  memcpy(pObj->targetSTbName, pCreate->targetStbFullName, TSDB_TABLE_FNAME_LEN);

  SDbObj *pTargetDb = mndAcquireDbByStb(pMnode, pObj->targetSTbName);
  if (pTargetDb == NULL) {
    mDebug("stream:%s failed to create, target db %s not exist", pCreate->name, pObj->targetDb);
    terrno = TSDB_CODE_MND_DB_NOT_EXIST;
    return -1;
  }
  tstrncpy(pObj->targetDb, pTargetDb->name, TSDB_DB_FNAME_LEN);

  pObj->targetStbUid = mndGenerateUid(pObj->targetSTbName, TSDB_TABLE_FNAME_LEN);
  pObj->targetDbUid = pTargetDb->uid;

  pObj->sql = pCreate->sql;
  pObj->ast = pCreate->ast;

  pCreate->sql = NULL;
  pCreate->ast = NULL;

  // deserialize ast
  if (nodesStringToNode(pObj->ast, &pAst) < 0) {
    /*ASSERT(0);*/
    goto FAIL;
  }

  // extract output schema from ast
  if (qExtractResultSchema(pAst, (int32_t *)&pObj->outputSchema.nCols, &pObj->outputSchema.pSchema) != 0) {
    /*ASSERT(0);*/
    goto FAIL;
  }

  SQueryPlan  *pPlan = NULL;
  SPlanContext cxt = {
      .pAstRoot = pAst,
      .topicQuery = false,
      .streamQuery = true,
      .triggerType = pObj->trigger == STREAM_TRIGGER_MAX_DELAY ? STREAM_TRIGGER_WINDOW_CLOSE : pObj->trigger,
      .watermark = pObj->watermark,
  };

  // using ast and param to build physical plan
  if (qCreateQueryPlan(&cxt, &pPlan, NULL) < 0) {
    /*ASSERT(0);*/
    goto FAIL;
  }

  // save physcial plan
  if (nodesNodeToString((SNode *)pPlan, false, &pObj->physicalPlan, NULL) != 0) {
    /*ASSERT(0);*/
    goto FAIL;
  }

FAIL:
  if (pAst != NULL) nodesDestroyNode(pAst);
  return 0;
}

int32_t mndPersistStream(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  SSdbRaw *pCommitRaw = mndStreamActionEncode(pStream);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
  return 0;
}

int32_t mndPersistDropStreamLog(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  SSdbRaw *pCommitRaw = mndStreamActionEncode(pStream);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);
  return 0;
}

int32_t mndAddStreamToTrans(SMnode *pMnode, SStreamObj *pStream, const char *ast, STrans *pTrans) {
  SNode *pAst = NULL;

  if (nodesStringToNode(ast, &pAst) < 0) {
    return -1;
  }

  if (qExtractResultSchema(pAst, (int32_t *)&pStream->outputSchema.nCols, &pStream->outputSchema.pSchema) != 0) {
    nodesDestroyNode(pAst);
    return -1;
  }
  // free
  nodesDestroyNode(pAst);

#if 0
  printf("|");
  for (int i = 0; i < pStream->outputSchema.nCols; i++) {
    printf(" %15s |", (char *)pStream->outputSchema.pSchema[i].name);
  }
  printf("\n=======================================================\n");

#endif

  if (TSDB_CODE_SUCCESS != mndStreamGetPlanString(ast, pStream->trigger, pStream->watermark, &pStream->physicalPlan)) {
    mError("topic:%s, failed to get plan since %s", pStream->name, terrstr());
    return -1;
  }

  if (mndScheduleStream(pMnode, pTrans, pStream) < 0) {
    mError("stream:%ld, schedule stream since %s", pStream->uid, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to create stream:%s", pTrans->id, pStream->name);

  SSdbRaw *pCommitRaw = mndStreamActionEncode(pStream);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  return 0;
}

static int32_t mndCreateStbForStream(SMnode *pMnode, STrans *pTrans, const SStreamObj *pStream, const char *user) {
  SStbObj *pStb = NULL;
  SDbObj  *pDb = NULL;

  SMCreateStbReq createReq = {0};
  tstrncpy(createReq.name, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
  createReq.numOfColumns = pStream->outputSchema.nCols;
  createReq.numOfTags = 1;  // group id
  createReq.pColumns = taosArrayInit(createReq.numOfColumns, sizeof(SField));
  // build fields
  taosArraySetSize(createReq.pColumns, createReq.numOfColumns);
  for (int32_t i = 0; i < createReq.numOfColumns; i++) {
    SField *pField = taosArrayGet(createReq.pColumns, i);
    tstrncpy(pField->name, pStream->outputSchema.pSchema[i].name, TSDB_COL_NAME_LEN);
    pField->flags = pStream->outputSchema.pSchema[i].flags;
    pField->type = pStream->outputSchema.pSchema[i].type;
    pField->bytes = pStream->outputSchema.pSchema[i].bytes;
  }
  createReq.pTags = taosArrayInit(createReq.numOfTags, sizeof(SField));
  taosArraySetSize(createReq.pTags, 1);
  // build tags
  SField *pField = taosArrayGet(createReq.pTags, 0);
  strcpy(pField->name, "group_id");
  pField->type = TSDB_DATA_TYPE_UBIGINT;
  pField->flags = 0;
  pField->bytes = 8;

  if (mndCheckCreateStbReq(&createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  pStb = mndAcquireStb(pMnode, createReq.name);
  if (pStb != NULL) {
    terrno = TSDB_CODE_MND_STB_ALREADY_EXIST;
    goto _OVER;
  }

  pDb = mndAcquireDbByStb(pMnode, createReq.name);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto _OVER;
  }

  if (mndCheckDbAuth(pMnode, user, MND_OPER_WRITE_DB, pDb) != 0) {
    goto _OVER;
  }

  int32_t numOfStbs = -1;
  if (mndGetNumOfStbs(pMnode, pDb->name, &numOfStbs) != 0) {
    goto _OVER;
  }

  if (pDb->cfg.numOfStables == 1 && numOfStbs != 0) {
    terrno = TSDB_CODE_MND_SINGLE_STB_MODE_DB;
    goto _OVER;
  }

  SStbObj stbObj = {0};

  if (mndBuildStbFromReq(pMnode, &stbObj, &createReq, pDb) != 0) {
    goto _OVER;
  }

  stbObj.uid = pStream->targetStbUid;

  if (mndAddStbToTrans(pMnode, pTrans, pDb, &stbObj) < 0) goto _OVER;

  return 0;
_OVER:
  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);
  return -1;
}

static int32_t mndPersistTaskDropReq(STrans *pTrans, SStreamTask *pTask) {
  ASSERT(pTask->nodeId != 0);

  // vnode
  /*if (pTask->nodeId > 0) {*/
  SVDropStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVDropStreamTaskReq));
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pReq->head.vgId = htonl(pTask->nodeId);
  pReq->taskId = pTask->taskId;
  STransAction action = {0};
  memcpy(&action.epSet, &pTask->epSet, sizeof(SEpSet));
  action.pCont = pReq;
  action.contLen = sizeof(SVDropStreamTaskReq);
  action.msgType = TDMT_VND_STREAM_TASK_DROP;
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }
  /*}*/

  return 0;
}

static int32_t mndDropStreamTasks(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  int32_t lv = taosArrayGetSize(pStream->tasks);
  for (int32_t i = 0; i < lv; i++) {
    SArray *pTasks = taosArrayGetP(pStream->tasks, i);
    int32_t sz = taosArrayGetSize(pTasks);
    for (int32_t j = 0; j < sz; j++) {
      SStreamTask *pTask = taosArrayGetP(pTasks, j);
      if (mndPersistTaskDropReq(pTrans, pTask) < 0) {
        return -1;
      }
    }
  }
  return 0;
}

static int32_t mndCreateStream(SMnode *pMnode, SRpcMsg *pReq, SCMCreateStreamReq *pCreate, SDbObj *pDb) {
  mDebug("stream:%s to create", pCreate->name);
  SStreamObj streamObj = {0};
  tstrncpy(streamObj.name, pCreate->name, TSDB_STREAM_FNAME_LEN);
  tstrncpy(streamObj.sourceDb, pDb->name, TSDB_DB_FNAME_LEN);
  tstrncpy(streamObj.targetSTbName, pCreate->targetStbFullName, TSDB_TABLE_FNAME_LEN);
  streamObj.createTime = taosGetTimestampMs();
  streamObj.updateTime = streamObj.createTime;
  streamObj.uid = mndGenerateUid(pCreate->name, strlen(pCreate->name));
  streamObj.targetStbUid = mndGenerateUid(pCreate->targetStbFullName, TSDB_TABLE_FNAME_LEN);
  streamObj.sourceDbUid = pDb->uid;
  streamObj.version = 1;
  streamObj.sql = pCreate->sql;
  // TODO
  streamObj.fixedSinkVgId = 0;
  streamObj.smaId = 0;
  /*streamObj.physicalPlan = "";*/
  streamObj.trigger = pCreate->triggerType;
  streamObj.watermark = pCreate->watermark;
  streamObj.triggerParam = pCreate->maxDelay;

  if (streamObj.targetSTbName[0]) {
    pDb = mndAcquireDbByStb(pMnode, streamObj.targetSTbName);
    if (pDb == NULL) {
      terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
      return -1;
    }
    tstrncpy(streamObj.targetDb, pDb->name, TSDB_DB_FNAME_LEN);
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq);
  if (pTrans == NULL) {
    mError("stream:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to create stream:%s", pTrans->id, pCreate->name);

  if (mndAddStreamToTrans(pMnode, &streamObj, pCreate->ast, pTrans) != 0) {
    mError("trans:%d, failed to add stream since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  if (streamObj.targetSTbName[0] && mndCreateStbForStream(pMnode, pTrans, &streamObj, pReq->info.conn.user) < 0) {
    mError("trans:%d, failed to create stb for stream since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessCreateStreamReq(SRpcMsg *pReq) {
  SMnode            *pMnode = pReq->info.node;
  int32_t            code = -1;
  SStreamObj        *pStream = NULL;
  SDbObj            *pDb = NULL;
  SCMCreateStreamReq createStreamReq = {0};

  if (tDeserializeSCMCreateStreamReq(pReq->pCont, pReq->contLen, &createStreamReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mDebug("stream:%s, start to create, sql:%s", createStreamReq.name, createStreamReq.sql);

  if (mndCheckCreateStreamReq(&createStreamReq) != 0) {
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
    goto _OVER;
  }

  pStream = mndAcquireStream(pMnode, createStreamReq.name);
  if (pStream != NULL) {
    if (createStreamReq.igExists) {
      mDebug("stream:%s, already exist, ignore exist is set", createStreamReq.name);
      code = 0;
      goto _OVER;
    } else {
      terrno = TSDB_CODE_MND_STREAM_ALREADY_EXIST;
      goto _OVER;
    }
  } else if (terrno != TSDB_CODE_MND_STREAM_NOT_EXIST) {
    goto _OVER;
  }

  // TODO check read auth for source and write auth for target
#if 0
  pDb = mndAcquireDb(pMnode, createStreamReq.sourceDB);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto _OVER;
  }

  if (mndCheckDbAuth(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb) != 0) {
    goto _OVER;
  }
#endif

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB_INSIDE, pReq);
  if (pTrans == NULL) {
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
    goto _OVER;
  }

  mndTransSetDbName(pTrans, createStreamReq.sourceDB, NULL);
  // TODO
  /*mndTransSetDbName(pTrans, streamObj.targetDb, NULL);*/
  mDebug("trans:%d, used to create stream:%s", pTrans->id, createStreamReq.name);

  // build stream obj from request
  SStreamObj streamObj = {0};
  if (mndBuildStreamObjFromCreateReq(pMnode, &streamObj, &createStreamReq) < 0) {
    ASSERT(0);
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
    goto _OVER;
  }

  // create stb for stream
  if (mndCreateStbForStream(pMnode, pTrans, &streamObj, pReq->info.conn.user) < 0) {
    mError("trans:%d, failed to create stb for stream %s since %s", pTrans->id, createStreamReq.name, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }

  // schedule stream task for stream obj
  if (mndScheduleStream(pMnode, pTrans, &streamObj) < 0) {
    mError("stream:%s, failed to schedule since %s", createStreamReq.name, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }

  // add stream to trans
  if (mndPersistStream(pMnode, pTrans, &streamObj) < 0) {
    mError("stream:%s, failed to schedule since %s", createStreamReq.name, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }

  // execute creation
  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }

  mndTransDrop(pTrans);

  /*code = mndCreateStream(pMnode, pReq, &createStreamReq, pDb);*/
  /*if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;*/
  code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
  }

  mndReleaseStream(pMnode, pStream);
  mndReleaseDb(pMnode, pDb);

  tFreeSCMCreateStreamReq(&createStreamReq);
  return code;
}

static int32_t mndProcessDropStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  int32_t     code = -1;
  SStreamObj *pStream = NULL;
  /*SDbObj     *pDb = NULL;*/
  /*SUserObj   *pUser = NULL;*/

  SMDropStreamReq dropReq = *(SMDropStreamReq *)pReq->pCont;

  pStream = mndAcquireStream(pMnode, dropReq.name);

  if (pStream == NULL) {
    if (dropReq.igNotExists) {
      mDebug("stream:%s, not exist, ignore not exist is set", dropReq.name);
      code = 0;
      goto DROP_STREAM_OVER;
    } else {
      terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
      return -1;
    }
  }

#if 0
  // todo check auth
  pUser = mndAcquireUser(pMnode, pReq->info.conn.user);
  if (pUser == NULL) {
    goto DROP_STREAM_OVER;
  }
#endif

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq);
  if (pTrans == NULL) {
    mError("stream:%s, failed to drop since %s", dropReq.name, terrstr());
    return code;
  }
  mDebug("trans:%d, used to drop stream:%s", pTrans->id, dropReq.name);

  // drop all tasks
  if (mndDropStreamTasks(pMnode, pTrans, pStream) < 0) {
    mError("stream:%s, failed to drop task since %s", dropReq.name, terrstr());
    return code;
  }

  // drop stream
  if (mndPersistDropStreamLog(pMnode, pTrans, pStream) < 0) {
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }

DROP_STREAM_OVER:
  return code;
}

int32_t mndDropStreamByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  SSdb *pSdb = pMnode->pSdb;

  void       *pIter = NULL;
  SStreamObj *pStream = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

    if (pStream->sourceDbUid == pDb->uid || pStream->targetDbUid == pDb->uid) {
      if (pStream->sourceDbUid != pStream->targetDbUid) {
        sdbRelease(pSdb, pStream);
        return -1;
      } else {
        // TODO drop all task on snode
        if (mndPersistDropStreamLog(pMnode, pTrans, pStream) < 0) {
          sdbRelease(pSdb, pStream);
          return -1;
        }
      }
    } else {
      sdbRelease(pSdb, pStream);
      continue;
    }

#if 0
    if (mndSetDropOffsetStreamLogs(pMnode, pTrans, pStream) < 0) {
      sdbRelease(pSdb, pStream);
      goto END;
    }
#endif

    sdbRelease(pSdb, pStream);
  }

  return 0;
}

static int32_t mndGetNumOfStreams(SMnode *pMnode, char *dbName, int32_t *pNumOfStreams) {
  SSdb   *pSdb = pMnode->pSdb;
  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    return -1;
  }

  int32_t numOfStreams = 0;
  void   *pIter = NULL;
  while (1) {
    SStreamObj *pStream = NULL;
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

    if (pStream->sourceDbUid == pDb->uid) {
      numOfStreams++;
    }

    sdbRelease(pSdb, pStream);
  }

  *pNumOfStreams = numOfStreams;
  mndReleaseDb(pMnode, pDb);
  return 0;
}

static int32_t mndRetrieveStream(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SStreamObj *pStream = NULL;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_STREAM, pShow->pIter, (void **)&pStream);
    if (pShow->pIter == NULL) break;

    SColumnInfoData *pColInfo;
    SName            n;
    int32_t          cols = 0;

    char streamName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    tNameFromString(&n, pStream->name, T_NAME_ACCT | T_NAME_DB);
    tNameGetDbName(&n, varDataVal(streamName));
    varDataSetLen(streamName, strlen(varDataVal(streamName)));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)streamName, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pStream->createTime, false);

    char sql[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
    tstrncpy(&sql[VARSTR_HEADER_SIZE], pStream->sql, TSDB_SHOW_SQL_LEN);
    varDataSetLen(sql, strlen(&sql[VARSTR_HEADER_SIZE]));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)sql, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pStream->status, true);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pStream->sourceDb, true);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pStream->targetDb, true);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pStream->targetSTbName, true);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pStream->watermark, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pStream->trigger, false);

    numOfRows++;
    sdbRelease(pSdb, pStream);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextStream(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
