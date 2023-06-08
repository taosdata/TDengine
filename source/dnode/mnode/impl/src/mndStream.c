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
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndScheduler.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "parser.h"
#include "tname.h"

#define MND_STREAM_VER_NUMBER   2
#define MND_STREAM_RESERVE_SIZE 64

#define MND_STREAM_MAX_NUM 60

static int32_t mndStreamActionInsert(SSdb *pSdb, SStreamObj *pStream);
static int32_t mndStreamActionDelete(SSdb *pSdb, SStreamObj *pStream);
static int32_t mndStreamActionUpdate(SSdb *pSdb, SStreamObj *pOldStream, SStreamObj *pNewStream);
static int32_t mndProcessCreateStreamReq(SRpcMsg *pReq);
static int32_t mndProcessDropStreamReq(SRpcMsg *pReq);
static int32_t mndProcessStreamCheckpointTmr(SRpcMsg *pReq);
static int32_t mndProcessStreamDoCheckpoint(SRpcMsg *pReq);
static int32_t mndProcessRecoverStreamReq(SRpcMsg *pReq);
static int32_t mndProcessStreamMetaReq(SRpcMsg *pReq);
static int32_t mndGetStreamMeta(SRpcMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t mndRetrieveStream(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextStream(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveStreamTask(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextStreamTask(SMnode *pMnode, void *pIter);
static int32_t mndProcessPauseStreamReq(SRpcMsg *pReq);
static int32_t mndProcessResumeStreamReq(SRpcMsg *pReq);

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
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_STREAM, mndProcessDropStreamReq);
  /*mndSetMsgHandle(pMnode, TDMT_MND_RECOVER_STREAM, mndProcessRecoverStreamReq);*/

  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_DEPLOY_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_DROP_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_PAUSE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_RESUME_RSP, mndTransProcessRsp);

  // mndSetMsgHandle(pMnode, TDMT_MND_STREAM_CHECKPOINT_TIMER, mndProcessStreamCheckpointTmr);
  // mndSetMsgHandle(pMnode, TDMT_MND_STREAM_BEGIN_CHECKPOINT, mndProcessStreamDoCheckpoint);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_REPORT_CHECKPOINT, mndTransProcessRsp);

  mndSetMsgHandle(pMnode, TDMT_MND_PAUSE_STREAM, mndProcessPauseStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_RESUME_STREAM, mndProcessResumeStreamReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndRetrieveStream);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndCancelGetNextStream);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAM_TASKS, mndRetrieveStreamTask);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAM_TASKS, mndCancelGetNextStreamTask);

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
  SSdbRow    *pRow = NULL;
  SStreamObj *pStream = NULL;
  void       *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto STREAM_DECODE_OVER;

  if (sver != 1 && sver != 2) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto STREAM_DECODE_OVER;
  }

  pRow = sdbAllocRow(sizeof(SStreamObj));
  if (pRow == NULL) goto STREAM_DECODE_OVER;

  pStream = sdbGetRowObj(pRow);
  if (pStream == NULL) goto STREAM_DECODE_OVER;

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, STREAM_DECODE_OVER);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) goto STREAM_DECODE_OVER;
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, STREAM_DECODE_OVER);

  SDecoder decoder;
  tDecoderInit(&decoder, buf, tlen + 1);
  if (tDecodeSStreamObj(&decoder, pStream, sver) < 0) {
    tDecoderClear(&decoder);
    goto STREAM_DECODE_OVER;
  }
  tDecoderClear(&decoder);

  terrno = TSDB_CODE_SUCCESS;

STREAM_DECODE_OVER:
  taosMemoryFreeClear(buf);
  if (terrno != TSDB_CODE_SUCCESS) {
    mError("stream:%s, failed to decode from raw:%p since %s", pStream == NULL ? "null" : pStream->name, pRaw,
           terrstr());
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
  taosWLockLatch(&pStream->lock);
  tFreeStreamObj(pStream);
  taosWUnLockLatch(&pStream->lock);
  return 0;
}

static int32_t mndStreamActionUpdate(SSdb *pSdb, SStreamObj *pOldStream, SStreamObj *pNewStream) {
  mTrace("stream:%s, perform update action", pOldStream->name);
  atomic_exchange_64(&pOldStream->updateTime, pNewStream->updateTime);
  atomic_exchange_32(&pOldStream->version, pNewStream->version);

  taosWLockLatch(&pOldStream->lock);

  pOldStream->status = pNewStream->status;

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

static void mndShowStreamStatus(char *dst, SStreamObj *pStream) {
  int8_t status = atomic_load_8(&pStream->status);
  if (status == STREAM_STATUS__NORMAL) {
    strcpy(dst, "normal");
  } else if (status == STREAM_STATUS__STOP) {
    strcpy(dst, "stop");
  } else if (status == STREAM_STATUS__FAILED) {
    strcpy(dst, "failed");
  } else if (status == STREAM_STATUS__RECOVER) {
    strcpy(dst, "recover");
  } else if (status == STREAM_STATUS__PAUSE) {
    strcpy(dst, "pause");
  }
}

static void mndShowStreamTrigger(char *dst, SStreamObj *pStream) {
  int8_t trigger = pStream->trigger;
  if (trigger == STREAM_TRIGGER_AT_ONCE) {
    strcpy(dst, "at once");
  } else if (trigger == STREAM_TRIGGER_WINDOW_CLOSE) {
    strcpy(dst, "window close");
  } else if (trigger == STREAM_TRIGGER_MAX_DELAY) {
    strcpy(dst, "max delay");
  }
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
  SNode      *pAst = NULL;
  SQueryPlan *pPlan = NULL;

  mInfo("stream:%s to create", pCreate->name);
  memcpy(pObj->name, pCreate->name, TSDB_STREAM_FNAME_LEN);
  pObj->createTime = taosGetTimestampMs();
  pObj->updateTime = pObj->createTime;
  pObj->version = 1;
  pObj->smaId = 0;

  pObj->uid = mndGenerateUid(pObj->name, strlen(pObj->name));
  pObj->status = 0;

  pObj->igExpired = pCreate->igExpired;
  pObj->trigger = pCreate->triggerType;
  pObj->triggerParam = pCreate->maxDelay;
  pObj->watermark = pCreate->watermark;
  pObj->fillHistory = pCreate->fillHistory;
  pObj->deleteMark = pCreate->deleteMark;
  pObj->igCheckUpdate = pCreate->igUpdate;

  memcpy(pObj->sourceDb, pCreate->sourceDB, TSDB_DB_FNAME_LEN);
  SDbObj *pSourceDb = mndAcquireDb(pMnode, pCreate->sourceDB);
  if (pSourceDb == NULL) {
    mInfo("stream:%s failed to create, source db %s not exist since %s", pCreate->name, pObj->sourceDb, terrstr());
    return -1;
  }
  pObj->sourceDbUid = pSourceDb->uid;
  mndReleaseDb(pMnode, pSourceDb);

  memcpy(pObj->targetSTbName, pCreate->targetStbFullName, TSDB_TABLE_FNAME_LEN);

  SDbObj *pTargetDb = mndAcquireDbByStb(pMnode, pObj->targetSTbName);
  if (pTargetDb == NULL) {
    mInfo("stream:%s failed to create, target db %s not exist since %s", pCreate->name, pObj->targetDb, terrstr());
    return -1;
  }
  tstrncpy(pObj->targetDb, pTargetDb->name, TSDB_DB_FNAME_LEN);

  if (pCreate->createStb == STREAM_CREATE_STABLE_TRUE) {
    pObj->targetStbUid = mndGenerateUid(pObj->targetSTbName, TSDB_TABLE_FNAME_LEN);
  } else {
    pObj->targetStbUid = pCreate->targetStbUid;
  }
  pObj->targetDbUid = pTargetDb->uid;
  mndReleaseDb(pMnode, pTargetDb);

  pObj->sql = pCreate->sql;
  pObj->ast = pCreate->ast;

  pCreate->sql = NULL;
  pCreate->ast = NULL;

  // deserialize ast
  if (nodesStringToNode(pObj->ast, &pAst) < 0) {
    goto FAIL;
  }

  // extract output schema from ast
  if (qExtractResultSchema(pAst, (int32_t *)&pObj->outputSchema.nCols, &pObj->outputSchema.pSchema) != 0) {
    goto FAIL;
  }

  int32_t numOfNULL = taosArrayGetSize(pCreate->fillNullCols);
  if (numOfNULL > 0) {
    pObj->outputSchema.nCols += numOfNULL;
    SSchema *pFullSchema = taosMemoryCalloc(pObj->outputSchema.nCols, sizeof(SSchema));
    if (!pFullSchema) {
      goto FAIL;
    }

    int32_t nullIndex = 0;
    int32_t dataIndex = 0;
    for (int16_t i = 0; i < pObj->outputSchema.nCols; i++) {
      SColLocation *pos = taosArrayGet(pCreate->fillNullCols, nullIndex);
      if (nullIndex >= numOfNULL || i < pos->slotId) {
        pFullSchema[i].bytes = pObj->outputSchema.pSchema[dataIndex].bytes;
        pFullSchema[i].colId = i + 1;  // pObj->outputSchema.pSchema[dataIndex].colId;
        pFullSchema[i].flags = pObj->outputSchema.pSchema[dataIndex].flags;
        strcpy(pFullSchema[i].name, pObj->outputSchema.pSchema[dataIndex].name);
        pFullSchema[i].type = pObj->outputSchema.pSchema[dataIndex].type;
        dataIndex++;
      } else {
        pFullSchema[i].bytes = 0;
        pFullSchema[i].colId = pos->colId;
        pFullSchema[i].flags = COL_SET_NULL;
        memset(pFullSchema[i].name, 0, TSDB_COL_NAME_LEN);
        pFullSchema[i].type = pos->type;
        nullIndex++;
      }
    }
    taosMemoryFree(pObj->outputSchema.pSchema);
    pObj->outputSchema.pSchema = pFullSchema;
  }

  SPlanContext cxt = {
      .pAstRoot = pAst,
      .topicQuery = false,
      .streamQuery = true,
      .triggerType = pObj->trigger == STREAM_TRIGGER_MAX_DELAY ? STREAM_TRIGGER_WINDOW_CLOSE : pObj->trigger,
      .watermark = pObj->watermark,
      .igExpired = pObj->igExpired,
      .deleteMark = pObj->deleteMark,
      .igCheckUpdate = pObj->igCheckUpdate,
  };

  // using ast and param to build physical plan
  if (qCreateQueryPlan(&cxt, &pPlan, NULL) < 0) {
    goto FAIL;
  }

  // save physcial plan
  if (nodesNodeToString((SNode *)pPlan, false, &pObj->physicalPlan, NULL) != 0) {
    goto FAIL;
  }

  pObj->tagSchema.nCols = pCreate->numOfTags;
  if (pCreate->numOfTags) {
    pObj->tagSchema.pSchema = taosMemoryCalloc(pCreate->numOfTags, sizeof(SSchema));
  }
  /*A(pCreate->numOfTags == taosArrayGetSize(pCreate->pTags));*/
  for (int32_t i = 0; i < pCreate->numOfTags; i++) {
    SField *pField = taosArrayGet(pCreate->pTags, i);
    pObj->tagSchema.pSchema[i].colId = pObj->outputSchema.nCols + i + 1;
    pObj->tagSchema.pSchema[i].bytes = pField->bytes;
    pObj->tagSchema.pSchema[i].flags = pField->flags;
    pObj->tagSchema.pSchema[i].type = pField->type;
    memcpy(pObj->tagSchema.pSchema[i].name, pField->name, TSDB_COL_NAME_LEN);
  }

FAIL:
  if (pAst != NULL) nodesDestroyNode(pAst);
  if (pPlan != NULL) qDestroyQueryPlan(pPlan);
  return 0;
}

int32_t mndPersistTaskDeployReq(STrans *pTrans, const SStreamTask *pTask) {
  SEncoder encoder;
  tEncoderInit(&encoder, NULL, 0);
  tEncodeStreamTask(&encoder, pTask);
  int32_t size = encoder.pos;
  int32_t tlen = sizeof(SMsgHead) + size;
  tEncoderClear(&encoder);
  void *buf = taosMemoryCalloc(1, tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  ((SMsgHead *)buf)->vgId = htonl(pTask->nodeId);
  void *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncoderInit(&encoder, abuf, size);
  tEncodeStreamTask(&encoder, pTask);
  tEncoderClear(&encoder);

  STransAction action = {0};
  action.mTraceId = pTrans->mTraceId;
  memcpy(&action.epSet, &pTask->epSet, sizeof(SEpSet));
  action.pCont = buf;
  action.contLen = tlen;
  action.msgType = TDMT_STREAM_TASK_DEPLOY;
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(buf);
    return -1;
  }
  return 0;
}

int32_t mndPersistStreamTasks(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  int32_t level = taosArrayGetSize(pStream->tasks);
  for (int32_t i = 0; i < level; i++) {
    SArray *pLevel = taosArrayGetP(pStream->tasks, i);
    int32_t sz = taosArrayGetSize(pLevel);
    for (int32_t j = 0; j < sz; j++) {
      SStreamTask *pTask = taosArrayGetP(pLevel, j);
      if (mndPersistTaskDeployReq(pTrans, pTask) < 0) {
        return -1;
      }
    }
  }
  return 0;
}

int32_t mndPersistStream(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  if (mndPersistStreamTasks(pMnode, pTrans, pStream) < 0) {
    return -1;
  }
  SSdbRaw *pCommitRaw = mndStreamActionEncode(pStream);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
  return 0;
}

int32_t mndPersistDropStreamLog(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
  SSdbRaw *pCommitRaw = mndStreamActionEncode(pStream);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);
  return 0;
}

static int32_t mndSetStreamRecover(SMnode *pMnode, STrans *pTrans, const SStreamObj *pStream) {
  SStreamObj streamObj = {0};
  memcpy(streamObj.name, pStream->name, TSDB_STREAM_FNAME_LEN);
  streamObj.status = STREAM_STATUS__RECOVER;

  SSdbRaw *pCommitRaw = mndStreamActionEncode(&streamObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("stream trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
  return 0;
}

static int32_t mndCreateStbForStream(SMnode *pMnode, STrans *pTrans, const SStreamObj *pStream, const char *user) {
  SStbObj *pStb = NULL;
  SDbObj  *pDb = NULL;

  SMCreateStbReq createReq = {0};
  tstrncpy(createReq.name, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
  createReq.numOfColumns = pStream->outputSchema.nCols;
  createReq.numOfTags = 1;  // group id
  createReq.pColumns = taosArrayInit_s(sizeof(SField), createReq.numOfColumns);
  // build fields
  for (int32_t i = 0; i < createReq.numOfColumns; i++) {
    SField *pField = taosArrayGet(createReq.pColumns, i);
    tstrncpy(pField->name, pStream->outputSchema.pSchema[i].name, TSDB_COL_NAME_LEN);
    pField->flags = pStream->outputSchema.pSchema[i].flags;
    pField->type = pStream->outputSchema.pSchema[i].type;
    pField->bytes = pStream->outputSchema.pSchema[i].bytes;
  }

  if (pStream->tagSchema.nCols == 0) {
    createReq.numOfTags = 1;
    createReq.pTags = taosArrayInit_s(sizeof(SField), 1);
    // build tags
    SField *pField = taosArrayGet(createReq.pTags, 0);
    strcpy(pField->name, "group_id");
    pField->type = TSDB_DATA_TYPE_UBIGINT;
    pField->flags = 0;
    pField->bytes = 8;
  } else {
    createReq.numOfTags = pStream->tagSchema.nCols;
    createReq.pTags = taosArrayInit_s(sizeof(SField), createReq.numOfTags);
    for (int32_t i = 0; i < createReq.numOfTags; i++) {
      SField *pField = taosArrayGet(createReq.pTags, i);
      pField->bytes = pStream->tagSchema.pSchema[i].bytes;
      pField->flags = pStream->tagSchema.pSchema[i].flags;
      pField->type = pStream->tagSchema.pSchema[i].type;
      tstrncpy(pField->name, pStream->tagSchema.pSchema[i].name, TSDB_COL_NAME_LEN);
    }
  }

  if (mndCheckCreateStbReq(&createReq) != 0) {
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

  if (mndAddStbToTrans(pMnode, pTrans, pDb, &stbObj) < 0) {
    mndFreeStb(&stbObj);
    goto _OVER;
  }

  tFreeSMCreateStbReq(&createReq);
  mndFreeStb(&stbObj);
  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);

  return 0;
_OVER:
  tFreeSMCreateStbReq(&createReq);
  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);
  return -1;
}

static int32_t mndPersistTaskDropReq(STrans *pTrans, SStreamTask *pTask) {
  // vnode
  /*if (pTask->nodeId > 0) {*/
  SVDropStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVDropStreamTaskReq));
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pReq->head.vgId = htonl(pTask->nodeId);
  pReq->taskId = pTask->id.taskId;
  STransAction action = {0};
  memcpy(&action.epSet, &pTask->epSet, sizeof(SEpSet));
  action.pCont = pReq;
  action.contLen = sizeof(SVDropStreamTaskReq);
  action.msgType = TDMT_STREAM_TASK_DROP;
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }
  /*}*/

  return 0;
}

int32_t mndDropStreamTasks(SMnode *pMnode, STrans *pTrans, SStreamObj *pStream) {
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

static int32_t mndProcessCreateStreamReq(SRpcMsg *pReq) {
  SMnode            *pMnode = pReq->info.node;
  int32_t            code = -1;
  SStreamObj        *pStream = NULL;
  SDbObj            *pDb = NULL;
  SCMCreateStreamReq createStreamReq = {0};
  SStreamObj         streamObj = {0};

  if (tDeserializeSCMCreateStreamReq(pReq->pCont, pReq->contLen, &createStreamReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("stream:%s, start to create, sql:%s", createStreamReq.name, createStreamReq.sql);

  if (mndCheckCreateStreamReq(&createStreamReq) != 0) {
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
    goto _OVER;
  }

  pStream = mndAcquireStream(pMnode, createStreamReq.name);
  if (pStream != NULL) {
    if (createStreamReq.igExists) {
      mInfo("stream:%s, already exist, ignore exist is set", createStreamReq.name);
      code = 0;
      goto _OVER;
    } else {
      terrno = TSDB_CODE_MND_STREAM_ALREADY_EXIST;
      goto _OVER;
    }
  } else if (terrno != TSDB_CODE_MND_STREAM_NOT_EXIST) {
    goto _OVER;
  }

  // build stream obj from request
  if (mndBuildStreamObjFromCreateReq(pMnode, &streamObj, &createStreamReq) < 0) {
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
    goto _OVER;
  }

  {
    int32_t numOfStream = 0;

    SStreamObj *pStream = NULL;
    void       *pIter = NULL;

    while (1) {
      pIter = sdbFetch(pMnode->pSdb, SDB_STREAM, pIter, (void **)&pStream);
      if (pIter == NULL) {
        if (numOfStream > MND_STREAM_MAX_NUM) {
          mError("too many streams, no more than %d for each database", MND_STREAM_MAX_NUM);
          terrno = TSDB_CODE_MND_TOO_MANY_STREAMS;
          goto _OVER;
        }
        break;
      }

      if (pStream->sourceDbUid == streamObj.sourceDbUid) {
        ++numOfStream;
      }

      sdbRelease(pMnode->pSdb, pStream);
      if (numOfStream > MND_STREAM_MAX_NUM) {
        mError("too many streams, no more than %d for each database", MND_STREAM_MAX_NUM);
        terrno = TSDB_CODE_MND_TOO_MANY_STREAMS;
        goto _OVER;
      }

      if (pStream->targetStbUid == streamObj.targetStbUid) {
        mError("Cannot write the same stable as other stream:%s", pStream->name);
        terrno = TSDB_CODE_MND_INVALID_TARGET_TABLE;
        goto _OVER;
      }
    }
  }

  pDb = mndAcquireDb(pMnode, streamObj.sourceDb);
  if (pDb->cfg.replications != 1) {
    mError("stream source db must have only 1 replica, but %s has %d", pDb->name, pDb->cfg.replications);
    terrno = TSDB_CODE_MND_MULTI_REPLICA_SOURCE_DB;
    mndReleaseDb(pMnode, pDb);
    pDb = NULL;
    goto _OVER;
  }

  mndReleaseDb(pMnode, pDb);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB_INSIDE, pReq, "create-stream");
  if (pTrans == NULL) {
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
    goto _OVER;
  }
  mInfo("trans:%d, used to create stream:%s", pTrans->id, createStreamReq.name);

  mndTransSetDbName(pTrans, createStreamReq.sourceDB, streamObj.targetDb);
  if (mndTrancCheckConflict(pMnode, pTrans) != 0) {
    mndTransDrop(pTrans);
    goto _OVER;
  }
  // create stb for stream
  if (createStreamReq.createStb == STREAM_CREATE_STABLE_TRUE &&
      mndCreateStbForStream(pMnode, pTrans, &streamObj, pReq->info.conn.user) < 0) {
    mError("trans:%d, failed to create stb for stream %s since %s", pTrans->id, createStreamReq.name, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }

  // schedule stream task for stream obj
  if (mndScheduleStream(pMnode, &streamObj) < 0) {
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

  if (mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_READ_DB, streamObj.sourceDb) != 0) {
    mndTransDrop(pTrans);
    goto _OVER;
  }

  if (mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, streamObj.targetDb) != 0) {
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

  code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
  }

  mndReleaseStream(pMnode, pStream);

  tFreeSCMCreateStreamReq(&createStreamReq);
  tFreeStreamObj(&streamObj);
  return code;
}

#if 0

static int32_t mndProcessStreamCheckpointTmr(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  void       *pIter = NULL;
  SStreamObj *pStream = NULL;

  // iterate all stream obj
  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;
    // incr tick
    int64_t currentTick = atomic_add_fetch_64(&pStream->currentTick, 1);
    // if >= checkpointFreq, build msg TDMT_MND_STREAM_BEGIN_CHECKPOINT, put into write q
    if (currentTick >= pStream->checkpointFreq) {
      atomic_store_64(&pStream->currentTick, 0);
      SMStreamDoCheckpointMsg *pMsg = rpcMallocCont(sizeof(SMStreamDoCheckpointMsg));

      pMsg->streamId = pStream->uid;
      pMsg->checkpointId = tGenIdPI64();
      memcpy(pMsg->streamName, pStream->name, TSDB_STREAM_FNAME_LEN);

      SRpcMsg rpcMsg = {
          .msgType = TDMT_MND_STREAM_BEGIN_CHECKPOINT,
          .pCont = pMsg,
          .contLen = sizeof(SMStreamDoCheckpointMsg),
      };

      tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
    }
  }

  return 0;
}

static int32_t mndBuildStreamCheckpointSourceReq(void **pBuf, int32_t *pLen, const SStreamTask *pTask,
                                                 SMStreamDoCheckpointMsg *pMsg) {
  SStreamCheckpointSourceReq req = {0};
  req.checkpointId = pMsg->checkpointId;
  req.nodeId = pTask->nodeId;
  req.expireTime = -1;
  req.streamId = pTask->streamId;
  req.taskId = pTask->taskId;

  int32_t code;
  int32_t blen;

  tEncodeSize(tEncodeSStreamCheckpointSourceReq, &req, blen, code);
  if (code < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  int32_t tlen = sizeof(SMsgHead) + blen;

  void *buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  void    *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  tEncodeSStreamCheckpointSourceReq(&encoder, &req);

  SMsgHead *pMsgHead = (SMsgHead *)buf;
  pMsgHead->contLen = htonl(tlen);
  pMsgHead->vgId = htonl(pTask->nodeId);

  tEncoderClear(&encoder);

  *pBuf = buf;
  *pLen = tlen;

  return 0;
}

static int32_t mndProcessStreamDoCheckpoint(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;

  SMStreamDoCheckpointMsg *pMsg = (SMStreamDoCheckpointMsg *)pReq->pCont;

  SStreamObj *pStream = mndAcquireStream(pMnode, pMsg->streamName);

  if (pStream == NULL || pStream->uid != pMsg->streamId) {
    mError("start checkpointing failed since stream %s not found", pMsg->streamName);
    return -1;
  }

  // build new transaction:
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "stream-checkpoint");
  if (pTrans == NULL) return -1;
  mndTransSetDbName(pTrans, pStream->sourceDb, pStream->targetDb);
  if (mndTrancCheckConflict(pMnode, pTrans) != 0) {
    mndReleaseStream(pMnode, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  taosRLockLatch(&pStream->lock);
  // 1. redo action: broadcast checkpoint source msg for all source vg
  int32_t totLevel = taosArrayGetSize(pStream->tasks);
  for (int32_t i = 0; i < totLevel; i++) {
    SArray      *pLevel = taosArrayGetP(pStream->tasks, i);
    SStreamTask *pTask = taosArrayGetP(pLevel, 0);
    if (pTask->taskLevel == TASK_LEVEL__SOURCE) {
      int32_t sz = taosArrayGetSize(pLevel);
      for (int32_t j = 0; j < sz; j++) {
        SStreamTask *pTask = taosArrayGetP(pLevel, j);
        /*A(pTask->nodeId > 0);*/
        SVgObj *pVgObj = mndAcquireVgroup(pMnode, pTask->nodeId);
        if (pVgObj == NULL) {
          taosRUnLockLatch(&pStream->lock);
          mndReleaseStream(pMnode, pStream);
          mndTransDrop(pTrans);
          return -1;
        }

        void   *buf;
        int32_t tlen;
        if (mndBuildStreamCheckpointSourceReq(&buf, &tlen, pTask, pMsg) < 0) {
          taosRUnLockLatch(&pStream->lock);
          mndReleaseStream(pMnode, pStream);
          mndTransDrop(pTrans);
          return -1;
        }

        STransAction action = {0};
        action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
        action.pCont = buf;
        action.contLen = tlen;
        action.msgType = TDMT_VND_STREAM_CHECK_POINT_SOURCE;

        mndReleaseVgroup(pMnode, pVgObj);

        if (mndTransAppendRedoAction(pTrans, &action) != 0) {
          taosMemoryFree(buf);
          taosRUnLockLatch(&pStream->lock);
          mndReleaseStream(pMnode, pStream);
          mndTransDrop(pTrans);
          return -1;
        }
      }
    }
  }
  // 2. reset tick
  atomic_store_64(&pStream->currentTick, 0);
  // 3. commit log: stream checkpoint info
  taosRUnLockLatch(&pStream->lock);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("failed to prepare trans rebalance since %s", terrstr());
    mndTransDrop(pTrans);
    mndReleaseStream(pMnode, pStream);
    return -1;
  }

  mndReleaseStream(pMnode, pStream);
  mndTransDrop(pTrans);

  return 0;
}

#endif

static int32_t mndProcessDropStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  /*SDbObj     *pDb = NULL;*/
  /*SUserObj   *pUser = NULL;*/

  SMDropStreamReq dropReq = {0};
  if (tDeserializeSMDropStreamReq(pReq->pCont, pReq->contLen, &dropReq) < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  pStream = mndAcquireStream(pMnode, dropReq.name);

  if (pStream == NULL) {
    if (dropReq.igNotExists) {
      mInfo("stream:%s, not exist, ignore not exist is set", dropReq.name);
      sdbRelease(pMnode->pSdb, pStream);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
      return -1;
    }
  }

  if (mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->targetDb) != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "drop-stream");
  if (pTrans == NULL) {
    mError("stream:%s, failed to drop since %s", dropReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }
  mInfo("trans:%d, used to drop stream:%s", pTrans->id, dropReq.name);

  mndTransSetDbName(pTrans, pStream->sourceDb, pStream->targetDb);
  if (mndTrancCheckConflict(pMnode, pTrans) != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  // drop all tasks
  if (mndDropStreamTasks(pMnode, pTrans, pStream) < 0) {
    mError("stream:%s, failed to drop task since %s", dropReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  // drop stream
  if (mndPersistDropStreamLog(pMnode, pTrans, pStream) < 0) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare drop stream trans since %s", pTrans->id, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}

int32_t mndDropStreamByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SStreamObj *pStream = NULL;
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

    if (pStream->sourceDbUid == pDb->uid || pStream->targetDbUid == pDb->uid) {
      if (pStream->sourceDbUid != pStream->targetDbUid) {
        sdbRelease(pSdb, pStream);
        sdbCancelFetch(pSdb, pIter);
        mError("db:%s, failed to drop stream:%s since sourceDbUid:%" PRId64 " not match with targetDbUid:%" PRId64,
               pDb->name, pStream->name, pStream->sourceDbUid, pStream->targetDbUid);
        terrno = TSDB_CODE_MND_STREAM_MUST_BE_DELETED;
        return -1;
      } else {
#if 0
        if (mndDropStreamTasks(pMnode, pTrans, pStream) < 0) {
          mError("stream:%s, failed to drop task since %s", pStream->name, terrstr());
          sdbRelease(pMnode->pSdb, pStream);
          sdbCancelFetch(pSdb, pIter);
          return -1;
        }
#endif
        if (mndPersistDropStreamLog(pMnode, pTrans, pStream) < 0) {
          sdbRelease(pSdb, pStream);
          sdbCancelFetch(pSdb, pIter);
          return -1;
        }
      }
    }

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
    STR_WITH_MAXSIZE_TO_VARSTR(streamName, mndGetDbStr(pStream->name), sizeof(streamName));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)streamName, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pStream->createTime, false);

    char sql[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(sql, pStream->sql, sizeof(sql));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)sql, false);

    char status[20 + VARSTR_HEADER_SIZE] = {0};
    char status2[20] = {0};
    mndShowStreamStatus(status2, pStream);
    STR_WITH_MAXSIZE_TO_VARSTR(status, status2, sizeof(status));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&status, false);

    char sourceDB[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(sourceDB, mndGetDbStr(pStream->sourceDb), sizeof(sourceDB));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&sourceDB, false);

    char targetDB[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(targetDB, mndGetDbStr(pStream->targetDb), sizeof(targetDB));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&targetDB, false);

    if (pStream->targetSTbName[0] == 0) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, NULL, true);
    } else {
      char targetSTB[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(targetSTB, mndGetStbStr(pStream->targetSTbName), sizeof(targetSTB));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)&targetSTB, false);
    }

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pStream->watermark, false);

    char trigger[20 + VARSTR_HEADER_SIZE] = {0};
    char trigger2[20] = {0};
    mndShowStreamTrigger(trigger2, pStream);
    STR_WITH_MAXSIZE_TO_VARSTR(trigger, trigger2, sizeof(trigger));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&trigger, false);

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

static int32_t mndRetrieveStreamTask(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SStreamObj *pStream = NULL;

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_STREAM, pShow->pIter, (void **)&pStream);
    if (pShow->pIter == NULL) break;

    // lock
    taosRLockLatch(&pStream->lock);
    // count task num
    int32_t sz = taosArrayGetSize(pStream->tasks);
    int32_t count = 0;
    for (int32_t i = 0; i < sz; i++) {
      SArray *pLevel = taosArrayGetP(pStream->tasks, i);
      count += taosArrayGetSize(pLevel);
    }

    if (numOfRows + count > rowsCapacity) {
      blockDataEnsureCapacity(pBlock, numOfRows + count);
    }
    // add row for each task
    for (int32_t i = 0; i < sz; i++) {
      SArray *pLevel = taosArrayGetP(pStream->tasks, i);
      int32_t levelCnt = taosArrayGetSize(pLevel);
      for (int32_t j = 0; j < levelCnt; j++) {
        SStreamTask *pTask = taosArrayGetP(pLevel, j);

        SColumnInfoData *pColInfo;
        int32_t          cols = 0;

        // stream name
        char streamName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
        STR_WITH_MAXSIZE_TO_VARSTR(streamName, mndGetDbStr(pStream->name), sizeof(streamName));
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        colDataSetVal(pColInfo, numOfRows, (const char *)streamName, false);

        // task id
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        colDataSetVal(pColInfo, numOfRows, (const char *)&pTask->id.taskId, false);

        // node type
        char nodeType[20 + VARSTR_HEADER_SIZE] = {0};
        varDataSetLen(nodeType, 5);
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        if (pTask->nodeId > 0) {
          memcpy(varDataVal(nodeType), "vnode", 5);
        } else {
          memcpy(varDataVal(nodeType), "snode", 5);
        }
        colDataSetVal(pColInfo, numOfRows, nodeType, false);

        // node id
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        int64_t nodeId = TMAX(pTask->nodeId, 0);
        colDataSetVal(pColInfo, numOfRows, (const char *)&nodeId, false);

        // level
        char level[20 + VARSTR_HEADER_SIZE] = {0};
        if (pTask->taskLevel == TASK_LEVEL__SOURCE) {
          memcpy(varDataVal(level), "source", 6);
          varDataSetLen(level, 6);
        } else if (pTask->taskLevel == TASK_LEVEL__AGG) {
          memcpy(varDataVal(level), "agg", 3);
          varDataSetLen(level, 3);
        } else if (pTask->taskLevel == TASK_LEVEL__SINK) {
          memcpy(varDataVal(level), "sink", 4);
          varDataSetLen(level, 4);
        } else if (pTask->taskLevel == TASK_LEVEL__SINK) {
        }
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        colDataSetVal(pColInfo, numOfRows, (const char *)&level, false);

        // status
        char status[20 + VARSTR_HEADER_SIZE] = {0};
        char status2[20] = {0};
        strcpy(status, "normal");
        STR_WITH_MAXSIZE_TO_VARSTR(status, status2, sizeof(status));
        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
        colDataSetVal(pColInfo, numOfRows, (const char *)&status, false);

        numOfRows++;
      }
    }

    // unlock
    taosRUnLockLatch(&pStream->lock);

    sdbRelease(pSdb, pStream);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextStreamTask(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

static int32_t mndPauseStreamTask(STrans *pTrans, SStreamTask *pTask) {
  SVPauseStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVPauseStreamTaskReq));
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pReq->head.vgId = htonl(pTask->nodeId);
  pReq->taskId = pTask->id.taskId;
  STransAction action = {0};
  memcpy(&action.epSet, &pTask->epSet, sizeof(SEpSet));
  action.pCont = pReq;
  action.contLen = sizeof(SVPauseStreamTaskReq);
  action.msgType = TDMT_STREAM_TASK_PAUSE;
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }
  return 0;
}

int32_t mndPauseAllStreamTasks(STrans *pTrans, SStreamObj *pStream) {
  int32_t size = taosArrayGetSize(pStream->tasks);
  for (int32_t i = 0; i < size; i++) {
    SArray *pTasks = taosArrayGetP(pStream->tasks, i);
    int32_t sz = taosArrayGetSize(pTasks);
    for (int32_t j = 0; j < sz; j++) {
      SStreamTask *pTask = taosArrayGetP(pTasks, j);
      if (pTask->taskLevel != TASK_LEVEL__SINK && mndPauseStreamTask(pTrans, pTask) < 0) {
        return -1;
      }
    }
  }
  return 0;
}

static int32_t mndPersistStreamLog(STrans *pTrans, const SStreamObj *pStream, int8_t status) {
  SStreamObj streamObj = {0};
  memcpy(streamObj.name, pStream->name, TSDB_STREAM_FNAME_LEN);
  streamObj.status = status;

  SSdbRaw *pCommitRaw = mndStreamActionEncode(&streamObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("stream trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
  return 0;
}

static int32_t mndProcessPauseStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;

  SMPauseStreamReq pauseReq = {0};
  if (tDeserializeSMPauseStreamReq(pReq->pCont, pReq->contLen, &pauseReq) < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  pStream = mndAcquireStream(pMnode, pauseReq.name);

  if (pStream == NULL) {
    if (pauseReq.igNotExists) {
      mInfo("stream:%s, not exist, if exist is set", pauseReq.name);
      sdbRelease(pMnode->pSdb, pStream);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
      return -1;
    }
  }

  if (mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->targetDb) != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "pause-stream");
  if (pTrans == NULL) {
    mError("stream:%s, failed to pause stream since %s", pauseReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }
  mInfo("trans:%d, used to pause stream:%s", pTrans->id, pauseReq.name);

  mndTransSetDbName(pTrans, pStream->sourceDb, pStream->targetDb);
  if (mndTrancCheckConflict(pMnode, pTrans) != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  // pause all tasks
  if (mndPauseAllStreamTasks(pTrans, pStream) < 0) {
    mError("stream:%s, failed to drop task since %s", pauseReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  // pause stream
  if (mndPersistStreamLog(pTrans, pStream, STREAM_STATUS__PAUSE) < 0) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare pause stream trans since %s", pTrans->id, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}


static int32_t mndResumeStreamTask(STrans *pTrans, SStreamTask *pTask, int8_t igUntreated) {
  SVResumeStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVResumeStreamTaskReq));
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  pReq->head.vgId = htonl(pTask->nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->igUntreated = igUntreated;
  STransAction action = {0};
  memcpy(&action.epSet, &pTask->epSet, sizeof(SEpSet));
  action.pCont = pReq;
  action.contLen = sizeof(SVResumeStreamTaskReq);
  action.msgType = TDMT_STREAM_TASK_RESUME;
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }
  return 0;
}

int32_t mndResumeAllStreamTasks(STrans *pTrans, SStreamObj *pStream, int8_t igUntreated) {
  int32_t size = taosArrayGetSize(pStream->tasks);
  for (int32_t i = 0; i < size; i++) {
    SArray *pTasks = taosArrayGetP(pStream->tasks, i);
    int32_t sz = taosArrayGetSize(pTasks);
    for (int32_t j = 0; j < sz; j++) {
      SStreamTask *pTask = taosArrayGetP(pTasks, j);
      if (pTask->taskLevel != TASK_LEVEL__SINK && mndResumeStreamTask(pTrans, pTask, igUntreated) < 0) {
        return -1;
      }
    }
  }
  return 0;
}

static int32_t mndProcessResumeStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;

  SMResumeStreamReq pauseReq = {0};
  if (tDeserializeSMResumeStreamReq(pReq->pCont, pReq->contLen, &pauseReq) < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  pStream = mndAcquireStream(pMnode, pauseReq.name);

  if (pStream == NULL) {
    if (pauseReq.igNotExists) {
      mInfo("stream:%s, not exist, if exist is set", pauseReq.name);
      sdbRelease(pMnode->pSdb, pStream);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
      return -1;
    }
  }

  if (mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->targetDb) != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "pause-stream");
  if (pTrans == NULL) {
    mError("stream:%s, failed to pause stream since %s", pauseReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }
  mInfo("trans:%d, used to pause stream:%s", pTrans->id, pauseReq.name);

  mndTransSetDbName(pTrans, pStream->sourceDb, pStream->targetDb);
  if (mndTrancCheckConflict(pMnode, pTrans) != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  // resume all tasks
  if (mndResumeAllStreamTasks(pTrans, pStream, pauseReq.igUntreated) < 0) {
    mError("stream:%s, failed to drop task since %s", pauseReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  // resume stream
  if (mndPersistStreamLog(pTrans, pStream, STREAM_STATUS__NORMAL) < 0) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare pause stream trans since %s", pTrans->id, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}
