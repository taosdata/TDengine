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
#include "audit.h"
#include "mndDb.h"
#include "mndPrivilege.h"
#include "mndScheduler.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "osMemory.h"
#include "parser.h"
#include "tmisce.h"
#include "tname.h"

#define MND_STREAM_MAX_NUM 60

typedef struct {
  int8_t placeHolder;  // // to fix windows compile error, define place holder
} SMStreamNodeCheckMsg;

static int32_t  mndNodeCheckSentinel = 0;
SStreamExecInfo execInfo;

static int32_t mndStreamActionInsert(SSdb *pSdb, SStreamObj *pStream);
static int32_t mndStreamActionDelete(SSdb *pSdb, SStreamObj *pStream);
static int32_t mndStreamActionUpdate(SSdb *pSdb, SStreamObj *pOldStream, SStreamObj *pNewStream);
static int32_t mndProcessCreateStreamReq(SRpcMsg *pReq);
static int32_t mndProcessDropStreamReq(SRpcMsg *pReq);
static int32_t mndProcessStreamCheckpointTmr(SRpcMsg *pReq);
static int32_t mndProcessStreamDoCheckpoint(SRpcMsg *pReq);
static int32_t mndProcessStreamCheckpointInCandid(SRpcMsg *pReq);
static int32_t mndRetrieveStream(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextStream(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveStreamTask(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextStreamTask(SMnode *pMnode, void *pIter);
static int32_t mndProcessPauseStreamReq(SRpcMsg *pReq);
static int32_t mndProcessResumeStreamReq(SRpcMsg *pReq);
static int32_t mndBuildStreamCheckpointSourceReq(void **pBuf, int32_t *pLen, int32_t nodeId, int64_t checkpointId,
                                                 int64_t streamId, int32_t taskId, int32_t transId, int8_t mndTrigger);
static int32_t mndProcessNodeCheck(SRpcMsg *pReq);
static int32_t mndProcessNodeCheckReq(SRpcMsg *pMsg);
static SArray *extractNodeListFromStream(SMnode *pMnode);
static int32_t mndProcessStreamReqCheckpoint(SRpcMsg *pReq);

static SVgroupChangeInfo mndFindChangedNodeInfo(SMnode *pMnode, const SArray *pPrevNodeList, const SArray *pNodeList);

static void     removeStreamTasksInBuf(SStreamObj *pStream, SStreamExecInfo *pExecNode);
static int32_t  removeExpirednodeEntryAndTask(SArray *pNodeSnapshot);
static int32_t  doKillCheckpointTrans(SMnode *pMnode, const char *pDbName, size_t len);
static SSdbRow *mndStreamActionDecode(SSdbRaw *pRaw);

SSdbRaw       *mndStreamSeqActionEncode(SStreamObj *pStream);
SSdbRow       *mndStreamSeqActionDecode(SSdbRaw *pRaw);
static int32_t mndStreamSeqActionInsert(SSdb *pSdb, SStreamSeq *pStream);
static int32_t mndStreamSeqActionDelete(SSdb *pSdb, SStreamSeq *pStream);
static int32_t mndStreamSeqActionUpdate(SSdb *pSdb, SStreamSeq *pOldStream, SStreamSeq *pNewStream);

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
  SSdbTable tableSeq = {
      .sdbType = SDB_STREAM_SEQ,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndStreamSeqActionEncode,
      .decodeFp = (SdbDecodeFp)mndStreamSeqActionDecode,
      .insertFp = (SdbInsertFp)mndStreamSeqActionInsert,
      .updateFp = (SdbUpdateFp)mndStreamSeqActionUpdate,
      .deleteFp = (SdbDeleteFp)mndStreamSeqActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_STREAM, mndProcessCreateStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_STREAM, mndProcessDropStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_NODECHECK_TIMER, mndProcessNodeCheck);

  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_DEPLOY_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_DROP_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_PAUSE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_RESUME_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_STOP_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_STREAM_TASK_UPDATE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_STREAM_TASK_RESET_RSP, mndTransProcessRsp);

  mndSetMsgHandle(pMnode, TDMT_VND_STREAM_CHECK_POINT_SOURCE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_CHECKPOINT_TIMER, mndProcessStreamCheckpointTmr);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_BEGIN_CHECKPOINT, mndProcessStreamDoCheckpoint);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_REQ_CHKPT, mndProcessStreamReqCheckpoint);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_CHECKPOINT_CANDIDITATE, mndProcessStreamCheckpointInCandid);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_HEARTBEAT, mndProcessStreamHb);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_REPORT_CHECKPOINT, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_NODECHANGE_CHECK, mndProcessNodeCheckReq);

  mndSetMsgHandle(pMnode, TDMT_MND_PAUSE_STREAM, mndProcessPauseStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_RESUME_STREAM, mndProcessResumeStreamReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndRetrieveStream);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndCancelGetNextStream);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAM_TASKS, mndRetrieveStreamTask);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAM_TASKS, mndCancelGetNextStreamTask);

  mndInitExecInfo();

  if (sdbSetTable(pMnode->pSdb, table) != 0) {
    return -1;
  }
  if (sdbSetTable(pMnode->pSdb, tableSeq) != 0) {
    return -1;
  }
  return 0;
}

void mndCleanupStream(SMnode *pMnode) {
  taosArrayDestroy(execInfo.pTaskList);
  taosHashCleanup(execInfo.pTaskMap);
  taosHashCleanup(execInfo.transMgmt.pDBTrans);
  taosHashCleanup(execInfo.transMgmt.pWaitingList);
  taosHashCleanup(execInfo.pTransferStateStreams);
  taosThreadMutexDestroy(&execInfo.lock);
  mDebug("mnd stream exec info cleanup");
}

SSdbRow *mndStreamActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRow    *pRow = NULL;
  SStreamObj *pStream = NULL;
  void       *buf = NULL;
  int8_t      sver = 0;

  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto STREAM_DECODE_OVER;
  }

  if (sver < 1 || sver > MND_STREAM_VER_NUMBER) {
    terrno = 0;
    mError("stream read invalid ver, data ver: %d, curr ver: %d", sver, MND_STREAM_VER_NUMBER);
    goto STREAM_DECODE_OVER;
  }

  pRow = sdbAllocRow(sizeof(SStreamObj));
  if (pRow == NULL) {
    goto STREAM_DECODE_OVER;
  }

  pStream = sdbGetRowObj(pRow);
  if (pStream == NULL) {
    goto STREAM_DECODE_OVER;
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, STREAM_DECODE_OVER);

  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    goto STREAM_DECODE_OVER;
  }

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
    char *p = (pStream == NULL) ? "null" : pStream->name;
    mError("stream:%s, failed to decode from raw:%p since %s", p, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("stream:%s, decode from raw:%p, row:%p, checkpoint:%" PRId64, pStream->name, pRaw, pStream,
         pStream->checkpointId);
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

  atomic_exchange_32(&pOldStream->version, pNewStream->version);

  taosWLockLatch(&pOldStream->lock);

  pOldStream->status = pNewStream->status;
  pOldStream->updateTime = pNewStream->updateTime;
  pOldStream->checkpointId = pNewStream->checkpointId;
  pOldStream->checkpointFreq = pNewStream->checkpointFreq;

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
    strcpy(dst, "ready");
  } else if (status == STREAM_STATUS__STOP) {
    strcpy(dst, "stop");
  } else if (status == STREAM_STATUS__FAILED) {
    strcpy(dst, "failed");
  } else if (status == STREAM_STATUS__RECOVER) {
    strcpy(dst, "recover");
  } else if (status == STREAM_STATUS__PAUSE) {
    strcpy(dst, "paused");
  }
}

SSdbRaw *mndStreamSeqActionEncode(SStreamObj *pStream) { return NULL; }
SSdbRow *mndStreamSeqActionDecode(SSdbRaw *pRaw) { return NULL; }
int32_t  mndStreamSeqActionInsert(SSdb *pSdb, SStreamSeq *pStream) { return 0; }
int32_t  mndStreamSeqActionDelete(SSdb *pSdb, SStreamSeq *pStream) { return 0; }
int32_t  mndStreamSeqActionUpdate(SSdb *pSdb, SStreamSeq *pOldStream, SStreamSeq *pNewStream) { return 0; }

static void mndShowStreamTrigger(char *dst, SStreamObj *pStream) {
  int8_t trigger = pStream->conf.trigger;
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

static int32_t mndBuildStreamObjFromCreateReq(SMnode *pMnode, SStreamObj *pObj, SCMCreateStreamReq *pCreate) {
  SNode      *pAst = NULL;
  SQueryPlan *pPlan = NULL;

  mInfo("stream:%s to create", pCreate->name);
  memcpy(pObj->name, pCreate->name, TSDB_STREAM_FNAME_LEN);
  pObj->createTime = taosGetTimestampMs();
  pObj->updateTime = pObj->createTime;
  pObj->version = 1;
  pObj->smaId = 0;
  pObj->indexForMultiAggBalance = -1;

  pObj->uid = mndGenerateUid(pObj->name, strlen(pObj->name));

  char p[TSDB_STREAM_FNAME_LEN + 32] = {0};
  snprintf(p, tListLen(p), "%s_%s", pObj->name, "fillhistory");

  pObj->hTaskUid = mndGenerateUid(pObj->name, strlen(pObj->name));
  pObj->status = 0;

  pObj->conf.igExpired = pCreate->igExpired;
  pObj->conf.trigger = pCreate->triggerType;
  pObj->conf.triggerParam = pCreate->maxDelay;
  pObj->conf.watermark = pCreate->watermark;
  pObj->conf.fillHistory = pCreate->fillHistory;
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
      .triggerType = pObj->conf.trigger == STREAM_TRIGGER_MAX_DELAY ? STREAM_TRIGGER_WINDOW_CLOSE : pObj->conf.trigger,
      .watermark = pObj->conf.watermark,
      .igExpired = pObj->conf.igExpired,
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

int32_t mndPersistTaskDeployReq(STrans *pTrans, SStreamTask *pTask) {
  SEncoder encoder;
  tEncoderInit(&encoder, NULL, 0);

  pTask->ver = SSTREAM_TASK_VER;
  tEncodeStreamTask(&encoder, pTask);

  int32_t size = encoder.pos;
  int32_t tlen = sizeof(SMsgHead) + size;
  tEncoderClear(&encoder);

  void *buf = taosMemoryCalloc(1, tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  ((SMsgHead *)buf)->vgId = htonl(pTask->info.nodeId);

  void *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncoderInit(&encoder, abuf, size);

  tEncodeStreamTask(&encoder, pTask);
  tEncoderClear(&encoder);

  int32_t code = setTransAction(pTrans, buf, tlen, TDMT_STREAM_TASK_DEPLOY, &pTask->info.epSet, 0);
  if (code != 0) {
    taosMemoryFree(buf);
    return -1;
  }

  return 0;
}

int32_t mndPersistStreamTasks(STrans *pTrans, SStreamObj *pStream) {
  SStreamTaskIter *pIter = createStreamTaskIter(pStream);
  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = streamTaskIterGetCurrent(pIter);
    if (mndPersistTaskDeployReq(pTrans, pTask) < 0) {
      destroyStreamTaskIter(pIter);
      return -1;
    }
  }

  destroyStreamTaskIter(pIter);

  // persistent stream task for already stored ts data
  if (pStream->conf.fillHistory) {
    int32_t level = taosArrayGetSize(pStream->pHTasksList);

    for (int32_t i = 0; i < level; i++) {
      SArray *pLevel = taosArrayGetP(pStream->pHTasksList, i);

      int32_t numOfTasks = taosArrayGetSize(pLevel);
      for (int32_t j = 0; j < numOfTasks; j++) {
        SStreamTask *pTask = taosArrayGetP(pLevel, j);
        if (mndPersistTaskDeployReq(pTrans, pTask) < 0) {
          return -1;
        }
      }
    }
  }

  return 0;
}

int32_t mndPersistStream(STrans *pTrans, SStreamObj *pStream) {
  if (mndPersistStreamTasks(pTrans, pStream) < 0) {
    return -1;
  }

  return mndPersistTransLog(pStream, pTrans, SDB_STATUS_READY);
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

static int32_t checkForNumOfStreams(SMnode *pMnode, SStreamObj *pStreamObj) {  // check for number of existed tasks
  int32_t     numOfStream = 0;
  SStreamObj *pStream = NULL;
  void       *pIter = NULL;

  while ((pIter = sdbFetch(pMnode->pSdb, SDB_STREAM, pIter, (void **)&pStream)) != NULL) {
    if (pStream->sourceDbUid == pStreamObj->sourceDbUid) {
      ++numOfStream;
    }

    sdbRelease(pMnode->pSdb, pStream);

    if (numOfStream > MND_STREAM_MAX_NUM) {
      mError("too many streams, no more than %d for each database", MND_STREAM_MAX_NUM);
      sdbCancelFetch(pMnode->pSdb, pIter);
      terrno = TSDB_CODE_MND_TOO_MANY_STREAMS;
      return terrno;
    }

    if (pStream->targetStbUid == pStreamObj->targetStbUid) {
      mError("Cannot write the same stable as other stream:%s", pStream->name);
      sdbCancelFetch(pMnode->pSdb, pIter);
      terrno = TSDB_CODE_MND_INVALID_TARGET_TABLE;
      return terrno;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mndProcessCreateStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  SStreamObj  streamObj = {0};
  char       *sql = NULL;
  int32_t     sqlLen = 0;
  terrno = TSDB_CODE_SUCCESS;

  if ((terrno = grantCheck(TSDB_GRANT_STREAMS)) < 0) {
    return terrno;
  }

  SCMCreateStreamReq createReq = {0};
  if (tDeserializeSCMCreateStreamReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

#ifdef WINDOWS
  terrno = TSDB_CODE_MND_INVALID_PLATFORM;
  goto _OVER;
#endif
  mInfo("stream:%s, start to create, sql:%s", createReq.name, createReq.sql);

  if (mndCheckCreateStreamReq(&createReq) != 0) {
    mError("stream:%s, failed to create since %s", createReq.name, terrstr());
    goto _OVER;
  }

  pStream = mndAcquireStream(pMnode, createReq.name);
  if (pStream != NULL) {
    if (createReq.igExists) {
      mInfo("stream:%s, already exist, ignore exist is set", createReq.name);
      goto _OVER;
    } else {
      terrno = TSDB_CODE_MND_STREAM_ALREADY_EXIST;
      goto _OVER;
    }
  } else if (terrno != TSDB_CODE_MND_STREAM_NOT_EXIST) {
    goto _OVER;
  }

  if (createReq.sql != NULL) {
    sqlLen = strlen(createReq.sql);
    sql = taosMemoryMalloc(sqlLen + 1);
    memset(sql, 0, sqlLen + 1);
    memcpy(sql, createReq.sql, sqlLen);
  }

  // build stream obj from request
  if (mndBuildStreamObjFromCreateReq(pMnode, &streamObj, &createReq) < 0) {
    mError("stream:%s, failed to create since %s", createReq.name, terrstr());
    goto _OVER;
  }

  if (checkForNumOfStreams(pMnode, &streamObj) < 0) {
    goto _OVER;
  }

  STrans *pTrans = doCreateTrans(pMnode, &streamObj, pReq, MND_STREAM_CREATE_NAME, "create stream tasks on dnodes");
  if (pTrans == NULL) {
    goto _OVER;
  }

  // create stb for stream
  if (createReq.createStb == STREAM_CREATE_STABLE_TRUE &&
      mndCreateStbForStream(pMnode, pTrans, &streamObj, pReq->info.conn.user) < 0) {
    mError("trans:%d, failed to create stb for stream %s since %s", pTrans->id, createReq.name, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }

  // schedule stream task for stream obj
  if (mndScheduleStream(pMnode, &streamObj, createReq.lastTs, createReq.pVgroupVerList) < 0) {
    mError("stream:%s, failed to schedule since %s", createReq.name, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }

  // add stream to trans
  if (mndPersistStream(pTrans, &streamObj) < 0) {
    mError("stream:%s, failed to schedule since %s", createReq.name, terrstr());
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

  taosThreadMutexLock(&execInfo.lock);
  mDebug("stream stream:%s tasks register into node list", createReq.name);
  saveStreamTasksInfo(&streamObj, &execInfo);
  taosThreadMutexUnlock(&execInfo.lock);

  // execute creation
  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }

  mndTransDrop(pTrans);

  SName dbname = {0};
  tNameFromString(&dbname, createReq.sourceDB, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

  SName name = {0};
  tNameFromString(&name, createReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  // reuse this function for stream

  if (sql != NULL && sqlLen > 0) {
    auditRecord(pReq, pMnode->clusterId, "createStream", dbname.dbname, name.dbname, sql, sqlLen);
  } else {
    char detail[1000] = {0};
    sprintf(detail, "dbname:%s, stream name:%s", dbname.dbname, name.dbname);
    auditRecord(pReq, pMnode->clusterId, "createStream", dbname.dbname, name.dbname, detail, strlen(detail));
  }

_OVER:
  if (terrno != TSDB_CODE_SUCCESS && terrno != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stream:%s, failed to create since %s", createReq.name, terrstr());
  }

  mndReleaseStream(pMnode, pStream);
  tFreeSCMCreateStreamReq(&createReq);
  tFreeStreamObj(&streamObj);
  if (sql != NULL) {
    taosMemoryFreeClear(sql);
  }

  return terrno;
}

int64_t mndStreamGenChkpId(SMnode *pMnode) {
  SStreamObj *pStream = NULL;
  void       *pIter = NULL;
  SSdb       *pSdb = pMnode->pSdb;
  int64_t     maxChkpId = 0;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

    maxChkpId = TMAX(maxChkpId, pStream->checkpointId);
    mDebug("stream:%p, %s id:%" PRIx64 "checkpoint %" PRId64 "", pStream, pStream->name, pStream->uid,
           pStream->checkpointId);
    sdbRelease(pSdb, pStream);
  }

  mDebug("generated checkpoint %" PRId64 "", maxChkpId + 1);
  return maxChkpId + 1;
}

static int32_t mndProcessStreamCheckpointTmr(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  if (sdbGetSize(pSdb, SDB_STREAM) <= 0) {
    return 0;
  }

  SMStreamDoCheckpointMsg *pMsg = rpcMallocCont(sizeof(SMStreamDoCheckpointMsg));
  pMsg->checkpointId = mndStreamGenChkpId(pMnode);

  int32_t size = sizeof(SMStreamDoCheckpointMsg);
  SRpcMsg rpcMsg = {.msgType = TDMT_MND_STREAM_BEGIN_CHECKPOINT, .pCont = pMsg, .contLen = size};
  tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  return 0;
}

static int32_t mndBuildStreamCheckpointSourceReq(void **pBuf, int32_t *pLen, int32_t nodeId, int64_t checkpointId,
                                                 int64_t streamId, int32_t taskId, int32_t transId, int8_t mndTrigger) {
  SStreamCheckpointSourceReq req = {0};
  req.checkpointId = checkpointId;
  req.nodeId = nodeId;
  req.expireTime = -1;
  req.streamId = streamId;  // pTask->id.streamId;
  req.taskId = taskId;      // pTask->id.taskId;
  req.transId = transId;
  req.mndTrigger = mndTrigger;

  int32_t code;
  int32_t blen;

  tEncodeSize(tEncodeStreamCheckpointSourceReq, &req, blen, code);
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
  tEncodeStreamCheckpointSourceReq(&encoder, &req);

  SMsgHead *pMsgHead = (SMsgHead *)buf;
  pMsgHead->contLen = htonl(tlen);
  pMsgHead->vgId = htonl(nodeId);

  tEncoderClear(&encoder);

  *pBuf = buf;
  *pLen = tlen;

  return 0;
}

static int32_t doSetCheckpointAction(SMnode *pMnode, STrans *pTrans, SStreamTask *pTask, int64_t checkpointId,
                                     int8_t mndTrigger) {
  void   *buf;
  int32_t tlen;
  if (mndBuildStreamCheckpointSourceReq(&buf, &tlen, pTask->info.nodeId, checkpointId, pTask->id.streamId,
                                        pTask->id.taskId, pTrans->id, mndTrigger) < 0) {
    taosMemoryFree(buf);
    return -1;
  }

  SEpSet  epset = {0};
  bool    hasEpset = false;
  int32_t code = extractNodeEpset(pMnode, &epset, &hasEpset, pTask->id.taskId, pTask->info.nodeId);
  if (code != TSDB_CODE_SUCCESS || !hasEpset) {
    taosMemoryFree(buf);
    return -1;
  }

  code = setTransAction(pTrans, buf, tlen, TDMT_VND_STREAM_CHECK_POINT_SOURCE, &epset, TSDB_CODE_SYN_PROPOSE_NOT_READY);
  if (code != 0) {
    taosMemoryFree(buf);
  }

  return code;
}

static int32_t mndProcessStreamCheckpointTrans(SMnode *pMnode, SStreamObj *pStream, int64_t checkpointId,
                                               int8_t mndTrigger, bool lock) {
  int32_t code = -1;
  int64_t ts = taosGetTimestampMs();
  if (mndTrigger == 1 && (ts - pStream->checkpointFreq < tsStreamCheckpointInterval * 1000)) {
    //    mWarn("checkpoint interval less than the threshold, ignore it");
    return TSDB_CODE_SUCCESS;
  }

  bool conflict = mndStreamTransConflictCheck(pMnode, pStream->uid, MND_STREAM_CHECKPOINT_NAME, lock);
  if (conflict) {
    mndAddtoCheckpointWaitingList(pStream, checkpointId);
    mWarn("checkpoint conflict with other trans in %s, ignore the checkpoint for stream:%s %" PRIx64, pStream->sourceDb,
          pStream->name, pStream->uid);
    return -1;
  }

  STrans *pTrans = doCreateTrans(pMnode, pStream, NULL, MND_STREAM_CHECKPOINT_NAME, "gen checkpoint for stream");
  if (pTrans == NULL) {
    mError("failed to checkpoint of stream name%s, checkpointId: %" PRId64 ", reason:%s", pStream->name, checkpointId,
           tstrerror(TSDB_CODE_MND_TRANS_CONFLICT));
    goto _ERR;
  }

  mndStreamRegisterTrans(pTrans, MND_STREAM_CHECKPOINT_NAME, pStream->uid);
  mDebug("start to trigger checkpoint for stream:%s, checkpoint: %" PRId64 "", pStream->name, checkpointId);

  taosWLockLatch(&pStream->lock);
  pStream->currentTick = 1;

  // 1. redo action: broadcast checkpoint source msg for all source vg
  int32_t totalLevel = taosArrayGetSize(pStream->tasks);
  for (int32_t i = 0; i < totalLevel; i++) {
    SArray      *pLevel = taosArrayGetP(pStream->tasks, i);
    SStreamTask *p = taosArrayGetP(pLevel, 0);

    if (p->info.taskLevel == TASK_LEVEL__SOURCE) {
      int32_t sz = taosArrayGetSize(pLevel);
      for (int32_t j = 0; j < sz; j++) {
        SStreamTask *pTask = taosArrayGetP(pLevel, j);
        code = doSetCheckpointAction(pMnode, pTrans, pTask, checkpointId, mndTrigger);

        if (code != TSDB_CODE_SUCCESS) {
          taosWUnLockLatch(&pStream->lock);
          goto _ERR;
        }
      }
    }
  }

  // 2. reset tick
  pStream->checkpointId = checkpointId;
  pStream->checkpointFreq = taosGetTimestampMs();
  pStream->currentTick = 0;

  // 3. commit log: stream checkpoint info
  pStream->version = pStream->version + 1;
  taosWUnLockLatch(&pStream->lock);

  if ((code = mndPersistTransLog(pStream, pTrans, SDB_STATUS_READY)) != TSDB_CODE_SUCCESS) {
    return code;
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != TSDB_CODE_SUCCESS) {
    mError("failed to prepare checkpoint trans since %s", terrstr());
    goto _ERR;
  }

  code = 0;
_ERR:
  mndTransDrop(pTrans);
  return code;
}

int32_t initStreamNodeList(SMnode *pMnode) {
  if (execInfo.pNodeList == NULL || (taosArrayGetSize(execInfo.pNodeList) == 0)) {
    execInfo.pNodeList = taosArrayDestroy(execInfo.pNodeList);
    execInfo.pNodeList = extractNodeListFromStream(pMnode);
  }

  return taosArrayGetSize(execInfo.pNodeList);
}

static bool taskNodeIsUpdated(SMnode *pMnode) {
  // check if the node update happens or not
  taosThreadMutexLock(&execInfo.lock);

  int32_t numOfNodes = initStreamNodeList(pMnode);
  if (numOfNodes == 0) {
    mDebug("stream task node change checking done, no vgroups exist, do nothing");
    execInfo.ts = taosGetTimestampSec();
    taosThreadMutexUnlock(&execInfo.lock);
    return false;
  }

  for (int32_t i = 0; i < numOfNodes; ++i) {
    SNodeEntry *pNodeEntry = taosArrayGet(execInfo.pNodeList, i);
    if (pNodeEntry->stageUpdated) {
      mDebug("stream task not ready due to node update detected, checkpoint not issued");
      taosThreadMutexUnlock(&execInfo.lock);
      return true;
    }
  }

  bool    allReady = true;
  SArray *pNodeSnapshot = mndTakeVgroupSnapshot(pMnode, &allReady);
  if (!allReady) {
    mWarn("not all vnodes ready");
    taosArrayDestroy(pNodeSnapshot);
    taosThreadMutexUnlock(&execInfo.lock);
    return 0;
  }

  SVgroupChangeInfo changeInfo = mndFindChangedNodeInfo(pMnode, execInfo.pNodeList, pNodeSnapshot);
  bool              nodeUpdated = (taosArrayGetSize(changeInfo.pUpdateNodeList) > 0);
  taosArrayDestroy(changeInfo.pUpdateNodeList);
  taosHashCleanup(changeInfo.pDBMap);
  taosArrayDestroy(pNodeSnapshot);

  if (nodeUpdated) {
    mDebug("stream task not ready due to node update");
  }

  taosThreadMutexUnlock(&execInfo.lock);
  return nodeUpdated;
}

static int32_t mndCheckNodeStatus(SMnode *pMnode) {
  bool    ready = true;
  int64_t ts = taosGetTimestampSec();
  if (taskNodeIsUpdated(pMnode)) {
    return -1;
  }

  taosThreadMutexLock(&execInfo.lock);
  if (taosArrayGetSize(execInfo.pNodeList) == 0) {
    mDebug("stream task node change checking done, no vgroups exist, do nothing");
    execInfo.ts = ts;
  }

  for (int32_t i = 0; i < taosArrayGetSize(execInfo.pTaskList); ++i) {
    STaskId          *p = taosArrayGet(execInfo.pTaskList, i);
    STaskStatusEntry *pEntry = taosHashGet(execInfo.pTaskMap, p, sizeof(*p));
    if (pEntry == NULL) {
      continue;
    }

    if (pEntry->status != TASK_STATUS__READY) {
      mDebug("s-task:0x%" PRIx64 "-0x%x (nodeId:%d) status:%s not ready, checkpoint msg not issued",
             pEntry->id.streamId, (int32_t)pEntry->id.taskId, pEntry->nodeId, streamTaskGetStatusStr(pEntry->status));
      ready = false;
      break;
    }
  }

  taosThreadMutexUnlock(&execInfo.lock);
  return ready ? 0 : -1;
}

static int32_t mndProcessStreamDoCheckpoint(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  void       *pIter = NULL;
  SStreamObj *pStream = NULL;
  int32_t     code = 0;

  if ((code = mndCheckNodeStatus(pMnode)) != 0) {
    return code;
  }

  // make sure the time interval between two consecutive checkpoint trans is long enough
  SMStreamDoCheckpointMsg *pMsg = (SMStreamDoCheckpointMsg *)pReq->pCont;
  while ((pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream)) != NULL) {
    code = mndProcessStreamCheckpointTrans(pMnode, pStream, pMsg->checkpointId, 1, true);
    sdbRelease(pSdb, pStream);
    if (code == -1) {
      break;
    }
  }

  return code;
}

static int32_t mndProcessStreamCheckpointInCandid(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  void   *pIter = NULL;
  int32_t code = 0;

  taosThreadMutexLock(&execInfo.lock);
  int32_t num = taosHashGetSize(execInfo.transMgmt.pWaitingList);
  taosThreadMutexUnlock(&execInfo.lock);
  if (num == 0) {
    return code;
  }

  if ((code = mndCheckNodeStatus(pMnode)) != 0) {
    return code;
  }

  SArray *pList = taosArrayInit(4, sizeof(int64_t));
  while ((pIter = taosHashIterate(execInfo.transMgmt.pWaitingList, pIter)) != NULL) {
    SCheckpointCandEntry *pEntry = pIter;

    SStreamObj *ps = mndAcquireStream(pMnode, pEntry->pName);
    if (ps == NULL) {
      continue;
    }

    mDebug("start to launch checkpoint for stream:%s %" PRIx64 " in candidate list", pEntry->pName, pEntry->streamId);

    code = mndProcessStreamCheckpointTrans(pMnode, ps, pEntry->checkpointId, 1, true);
    mndReleaseStream(pMnode, ps);

    if (code == TSDB_CODE_SUCCESS) {
      taosArrayPush(pList, &pEntry->streamId);
    }
  }

  for (int32_t i = 0; i < taosArrayGetSize(pList); ++i) {
    int64_t *pId = taosArrayGet(pList, i);

    taosHashRemove(execInfo.transMgmt.pWaitingList, pId, sizeof(*pId));
  }

  int32_t remain = taosHashGetSize(execInfo.transMgmt.pWaitingList);
  mDebug("%d in candidate list generated checkpoint, remaining:%d", (int32_t)taosArrayGetSize(pList), remain);
  taosArrayDestroy(pList);
  return code;
}

static int32_t mndProcessDropStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;

  SMDropStreamReq dropReq = {0};
  if (tDeserializeSMDropStreamReq(pReq->pCont, pReq->contLen, &dropReq) < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  pStream = mndAcquireStream(pMnode, dropReq.name);

  if (pStream == NULL) {
    if (dropReq.igNotExists) {
      mInfo("stream:%s not exist, ignore not exist is set", dropReq.name);
      sdbRelease(pMnode->pSdb, pStream);
      tFreeMDropStreamReq(&dropReq);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
      mError("stream:%s not exist failed to drop", dropReq.name);
      tFreeMDropStreamReq(&dropReq);
      return -1;
    }
  }

  if (mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->targetDb) != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMDropStreamReq(&dropReq);
    return -1;
  }

  // check if it is conflict with other trans in both sourceDb and targetDb.
  bool conflict = mndStreamTransConflictCheck(pMnode, pStream->uid, MND_STREAM_DROP_NAME, true);
  if (conflict) {
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMDropStreamReq(&dropReq);
    return -1;
  }

  STrans *pTrans = doCreateTrans(pMnode, pStream, pReq, MND_STREAM_DROP_NAME, "drop stream");
  if (pTrans == NULL) {
    mError("stream:%s, failed to drop since %s", dropReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMDropStreamReq(&dropReq);
    return -1;
  }

  int32_t code = mndStreamRegisterTrans(pTrans, MND_STREAM_DROP_NAME, pStream->uid);

  // drop all tasks
  if (mndStreamSetDropAction(pMnode, pTrans, pStream) < 0) {
    mError("stream:%s, failed to drop task since %s", dropReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    tFreeMDropStreamReq(&dropReq);
    return -1;
  }

  // drop stream
  if (mndPersistTransLog(pStream, pTrans, SDB_STATUS_DROPPED) < 0) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    tFreeMDropStreamReq(&dropReq);
    return -1;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare drop stream trans since %s", pTrans->id, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    tFreeMDropStreamReq(&dropReq);
    return -1;
  }

  // kill the related checkpoint trans
  int32_t transId = mndStreamGetRelTrans(pMnode, pStream->uid);
  if (transId != 0) {
    mDebug("drop active related transId:%d due to stream:%s dropped", transId, pStream->name);
    mndKillTransImpl(pMnode, transId, pStream->sourceDb);
  }

  removeStreamTasksInBuf(pStream, &execInfo);

  SName name = {0};
  tNameFromString(&name, dropReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

  auditRecord(pReq, pMnode->clusterId, "dropStream", "", name.dbname, dropReq.sql, dropReq.sqlLen);

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);
  tFreeMDropStreamReq(&dropReq);

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
        if (mndStreamSetDropAction(pMnode, pTrans, pStream) < 0) {
          mError("stream:%s, failed to drop task since %s", pStream->name, terrstr());
          sdbRelease(pMnode->pSdb, pStream);
          sdbCancelFetch(pSdb, pIter);
          return -1;
        }
#endif

        // kill the related checkpoint trans
        int32_t transId = mndStreamGetRelTrans(pMnode, pStream->uid);
        if (transId != 0) {
          mDebug("drop active related transId:%d due to stream:%s dropped", transId, pStream->name);
          mndKillTransImpl(pMnode, transId, pStream->sourceDb);
        }

        // drop the stream obj in execInfo
        removeStreamTasksInBuf(pStream, &execInfo);

        if (mndPersistTransLog(pStream, pTrans, SDB_STATUS_DROPPED) < 0) {
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

int32_t mndGetNumOfStreams(SMnode *pMnode, char *dbName, int32_t *pNumOfStreams) {
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
    colDataSetVal(pColInfo, numOfRows, (const char *)&pStream->conf.watermark, false);

    char trigger[20 + VARSTR_HEADER_SIZE] = {0};
    char trigger2[20] = {0};
    mndShowStreamTrigger(trigger2, pStream);
    STR_WITH_MAXSIZE_TO_VARSTR(trigger, trigger2, sizeof(trigger));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&trigger, false);

    char sinkQuota[20 + VARSTR_HEADER_SIZE] = {0};
    sinkQuota[0] = '0';
    char dstStr[20] = {0};
    STR_TO_VARSTR(dstStr, sinkQuota)
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)dstStr, false);

    char scanHistoryIdle[20 + VARSTR_HEADER_SIZE] = {0};
    strcpy(scanHistoryIdle, "100a");

    memset(dstStr, 0, tListLen(dstStr));
    STR_TO_VARSTR(dstStr, scanHistoryIdle)
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)dstStr, false);

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

static int32_t setTaskAttrInResBlock(SStreamObj *pStream, SStreamTask *pTask, SSDataBlock *pBlock, int32_t numOfRows) {
  SColumnInfoData *pColInfo;
  int32_t          cols = 0;

  STaskId id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};

  STaskStatusEntry *pe = taosHashGet(execInfo.pTaskMap, &id, sizeof(id));
  if (pe == NULL) {
    mError("task:0x%" PRIx64 " not exists in vnode, no valid status/stage info", id.taskId);
    return -1;
  }

  // stream name
  char streamName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(streamName, mndGetDbStr(pStream->name), sizeof(streamName));

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  colDataSetVal(pColInfo, numOfRows, (const char *)streamName, false);

  // task id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);

  char    idstr[128] = {0};
  int32_t len = tintToHex(pTask->id.taskId, &idstr[4]);
  idstr[2] = '0';
  idstr[3] = 'x';
  varDataSetLen(idstr, len + 2);
  colDataSetVal(pColInfo, numOfRows, idstr, false);

  // node type
  char nodeType[20 + VARSTR_HEADER_SIZE] = {0};
  varDataSetLen(nodeType, 5);
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  if (pTask->info.nodeId > 0) {
    memcpy(varDataVal(nodeType), "vnode", 5);
  } else {
    memcpy(varDataVal(nodeType), "snode", 5);
  }
  colDataSetVal(pColInfo, numOfRows, nodeType, false);

  // node id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  int64_t nodeId = TMAX(pTask->info.nodeId, 0);
  colDataSetVal(pColInfo, numOfRows, (const char *)&nodeId, false);

  // level
  char level[20 + VARSTR_HEADER_SIZE] = {0};
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    memcpy(varDataVal(level), "source", 6);
    varDataSetLen(level, 6);
  } else if (pTask->info.taskLevel == TASK_LEVEL__AGG) {
    memcpy(varDataVal(level), "agg", 3);
    varDataSetLen(level, 3);
  } else if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    memcpy(varDataVal(level), "sink", 4);
    varDataSetLen(level, 4);
  }

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  colDataSetVal(pColInfo, numOfRows, (const char *)level, false);

  // status
  char status[20 + VARSTR_HEADER_SIZE] = {0};

  const char *pStatus = streamTaskGetStatusStr(pe->status);
  STR_TO_VARSTR(status, pStatus);

  // status
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  colDataSetVal(pColInfo, numOfRows, (const char *)status, false);

  // stage
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  colDataSetVal(pColInfo, numOfRows, (const char *)&pe->stage, false);

  // input queue
  char        vbuf[30] = {0};
  char        buf[25] = {0};
  const char *queueInfoStr = "%4.2fMiB (%5.2f%)";
  sprintf(buf, queueInfoStr, pe->inputQUsed, pe->inputRate);
  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);

  // output queue
  //          sprintf(buf, queueInfoStr, pe->outputQUsed, pe->outputRate);
  //        STR_TO_VARSTR(vbuf, buf);

  //        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  //        colDataSetVal(pColInfo, numOfRows, (const char*)vbuf, false);

  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    const char *sinkStr = "%.2fMiB";
    sprintf(buf, sinkStr, pe->sinkDataSize);
  } else if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    // offset info
    const char *offsetStr = "%" PRId64 " [%" PRId64 ", %" PRId64 "]";
    sprintf(buf, offsetStr, pe->processedVer, pe->verStart, pe->verEnd);
  }

  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);

  return TSDB_CODE_SUCCESS;
}

static int32_t mndRetrieveStreamTask(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SStreamObj *pStream = NULL;

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_STREAM, pShow->pIter, (void **)&pStream);
    if (pShow->pIter == NULL) {
      break;
    }

    // lock
    taosRLockLatch(&pStream->lock);

    int32_t count = mndGetNumOfStreamTasks(pStream);
    if (numOfRows + count > rowsCapacity) {
      blockDataEnsureCapacity(pBlock, numOfRows + count);
    }

    // add row for each task
    SStreamTaskIter *pIter = createStreamTaskIter(pStream);
    while (streamTaskIterNextTask(pIter)) {
      SStreamTask *pTask = streamTaskIterGetCurrent(pIter);

      int32_t code = setTaskAttrInResBlock(pStream, pTask, pBlock, numOfRows);
      if (code == TSDB_CODE_SUCCESS) {
        numOfRows++;
      }
    }

    destroyStreamTaskIter(pIter);
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
      mInfo("stream:%s, not exist, not pause stream", pauseReq.name);
      return 0;
    } else {
      mError("stream:%s not exist, failed to pause stream", pauseReq.name);
      terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
      return -1;
    }
  }

  mInfo("stream:%s,%"PRId64 " start to pause stream", pauseReq.name, pStream->uid);

  if (pStream->status == STREAM_STATUS__PAUSE) {
    sdbRelease(pMnode->pSdb, pStream);
    return 0;
  }

  if (mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->targetDb) != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }

  // check if it is conflict with other trans in both sourceDb and targetDb.
  bool conflict = mndStreamTransConflictCheck(pMnode, pStream->uid, MND_STREAM_PAUSE_NAME, true);
  if (conflict) {
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }

  bool updated = taskNodeIsUpdated(pMnode);
  if (updated) {
    mError("tasks are not ready for pause, node update detected");
    return -1;
  }

  STrans *pTrans = doCreateTrans(pMnode, pStream, pReq, MND_STREAM_PAUSE_NAME, "pause the stream");
  if (pTrans == NULL) {
    mError("stream:%s failed to pause stream since %s", pauseReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }

  int32_t code = mndStreamRegisterTrans(pTrans, MND_STREAM_PAUSE_NAME, pStream->uid);

  // if nodeUpdate happened, not send pause trans
  if (mndStreamSetPauseAction(pMnode, pTrans, pStream) < 0) {
    mError("stream:%s, failed to pause task since %s", pauseReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  // pause stream
  taosWLockLatch(&pStream->lock);
  pStream->status = STREAM_STATUS__PAUSE;
  if (mndPersistTransLog(pStream, pTrans, SDB_STATUS_READY) < 0) {
    taosWUnLockLatch(&pStream->lock);

    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  taosWUnLockLatch(&pStream->lock);

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

static int32_t mndProcessResumeStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;

  if (grantCheckExpire(TSDB_GRANT_STREAMS) < 0) {
    terrno = TSDB_CODE_GRANT_EXPIRED;
    return -1;
  }

  SMResumeStreamReq resumeReq = {0};
  if (tDeserializeSMResumeStreamReq(pReq->pCont, pReq->contLen, &resumeReq) < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  pStream = mndAcquireStream(pMnode, resumeReq.name);

  if (pStream == NULL) {
    if (resumeReq.igNotExists) {
      mInfo("stream:%s not exist, not resume stream", resumeReq.name);
      sdbRelease(pMnode->pSdb, pStream);
      return 0;
    } else {
      mError("stream:%s not exist, failed to resume stream", resumeReq.name);
      terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
      return -1;
    }
  }

  if (pStream->status != STREAM_STATUS__PAUSE) {
    sdbRelease(pMnode->pSdb, pStream);
    return 0;
  }

  if (mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->targetDb) != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }

  // check if it is conflict with other trans in both sourceDb and targetDb.
  bool conflict = mndStreamTransConflictCheck(pMnode, pStream->uid, MND_STREAM_RESUME_NAME, true);
  if (conflict) {
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }

  STrans *pTrans = doCreateTrans(pMnode, pStream, pReq, MND_STREAM_RESUME_NAME, "resume the stream");
  if (pTrans == NULL) {
    mError("stream:%s, failed to resume stream since %s", resumeReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }

  int32_t code = mndStreamRegisterTrans(pTrans, MND_STREAM_RESUME_NAME, pStream->uid);

  // set the resume action
  if (mndStreamSetResumeAction(pTrans, pMnode, pStream, resumeReq.igUntreated) < 0) {
    mError("stream:%s, failed to drop task since %s", resumeReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  // resume stream
  taosWLockLatch(&pStream->lock);
  pStream->status = STREAM_STATUS__NORMAL;
  if (mndPersistTransLog(pStream, pTrans, SDB_STATUS_READY) < 0) {
    taosWUnLockLatch(&pStream->lock);

    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  taosWUnLockLatch(&pStream->lock);
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

static bool isNodeEpsetChanged(const SEpSet *pPrevEpset, const SEpSet *pCurrent) {
  const SEp *pEp = GET_ACTIVE_EP(pPrevEpset);
  const SEp *p = GET_ACTIVE_EP(pCurrent);

  if (pEp->port == p->port && strncmp(pEp->fqdn, p->fqdn, TSDB_FQDN_LEN) == 0) {
    return false;
  }
  return true;
}

// 1. increase the replica does not affect the stream process.
// 2. decreasing the replica may affect the stream task execution in the way that there is one or more running stream
// tasks on the will be removed replica.
// 3. vgroup redistribution is an combination operation of first increase replica and then decrease replica. So we
// will handle it as mentioned in 1 & 2 items.
static SVgroupChangeInfo mndFindChangedNodeInfo(SMnode *pMnode, const SArray *pPrevNodeList, const SArray *pNodeList) {
  SVgroupChangeInfo info = {
      .pUpdateNodeList = taosArrayInit(4, sizeof(SNodeUpdateInfo)),
      .pDBMap = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_NO_LOCK),
  };

  int32_t numOfNodes = taosArrayGetSize(pPrevNodeList);
  for (int32_t i = 0; i < numOfNodes; ++i) {
    SNodeEntry *pPrevEntry = taosArrayGet(pPrevNodeList, i);

    int32_t num = taosArrayGetSize(pNodeList);
    for (int32_t j = 0; j < num; ++j) {
      SNodeEntry *pCurrent = taosArrayGet(pNodeList, j);

      if (pCurrent->nodeId == pPrevEntry->nodeId) {
        if (pPrevEntry->stageUpdated || isNodeEpsetChanged(&pPrevEntry->epset, &pCurrent->epset)) {
          const SEp *pPrevEp = GET_ACTIVE_EP(&pPrevEntry->epset);

          char buf[256] = {0};
          EPSET_TO_STR(&pCurrent->epset, buf);
          mDebug("nodeId:%d restart/epset changed detected, old:%s:%d -> new:%s, stageUpdate:%d", pCurrent->nodeId,
                 pPrevEp->fqdn, pPrevEp->port, buf, pPrevEntry->stageUpdated);

          SNodeUpdateInfo updateInfo = {.nodeId = pPrevEntry->nodeId};
          epsetAssign(&updateInfo.prevEp, &pPrevEntry->epset);
          epsetAssign(&updateInfo.newEp, &pCurrent->epset);
          taosArrayPush(info.pUpdateNodeList, &updateInfo);
        }

        // todo handle the snode info
        if (pCurrent->nodeId != SNODE_HANDLE) {
          SVgObj *pVgroup = mndAcquireVgroup(pMnode, pCurrent->nodeId);
          taosHashPut(info.pDBMap, pVgroup->dbName, strlen(pVgroup->dbName), NULL, 0);
          mndReleaseVgroup(pMnode, pVgroup);
        }

        break;
      }
    }
  }

  return info;
}

static int32_t mndProcessVgroupChange(SMnode *pMnode, SVgroupChangeInfo *pChangeInfo) {
  SSdb       *pSdb = pMnode->pSdb;
  SStreamObj *pStream = NULL;
  void       *pIter = NULL;
  STrans     *pTrans = NULL;

  // conflict check for nodeUpdate trans, here we randomly chose one stream to add into the trans pool
  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) {
      break;
    }

    bool conflict = mndStreamTransConflictCheck(pMnode, pStream->uid, MND_STREAM_TASK_UPDATE_NAME, false);
    sdbRelease(pSdb, pStream);

    if (conflict) {
      mError("nodeUpdate conflict with other trans, current nodeUpdate ignored");
      sdbCancelFetch(pSdb, pIter);
      return -1;
    }
  }

  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) {
      break;
    }

    // here create only one trans
    if (pTrans == NULL) {
      pTrans = doCreateTrans(pMnode, pStream, NULL, MND_STREAM_TASK_UPDATE_NAME, "update task epsets");
      if (pTrans == NULL) {
        sdbRelease(pSdb, pStream);
        sdbCancelFetch(pSdb, pIter);
        return terrno;
      }

      mndStreamRegisterTrans(pTrans, MND_STREAM_TASK_UPDATE_NAME, pStream->uid);
    }

    void *p = taosHashGet(pChangeInfo->pDBMap, pStream->targetDb, strlen(pStream->targetDb));
    void *p1 = taosHashGet(pChangeInfo->pDBMap, pStream->sourceDb, strlen(pStream->sourceDb));
    if (p == NULL && p1 == NULL) {
      mDebug("stream:0x%" PRIx64 " %s not involved nodeUpdate, ignore", pStream->uid, pStream->name);
      sdbRelease(pSdb, pStream);
      continue;
    }

    mDebug("stream:0x%" PRIx64 " %s involved node changed, create update trans, transId:%d", pStream->uid,
           pStream->name, pTrans->id);

    int32_t code = mndStreamSetUpdateEpsetAction(pStream, pChangeInfo, pTrans);

    // todo: not continue, drop all and retry again
    if (code != TSDB_CODE_SUCCESS) {
      mError("stream:0x%" PRIx64 " build nodeUpdate trans failed, ignore and continue, code:%s", pStream->uid,
             tstrerror(code));
      sdbRelease(pSdb, pStream);
      continue;
    }

    code = mndPersistTransLog(pStream, pTrans, SDB_STATUS_READY);
    sdbRelease(pSdb, pStream);

    if (code != TSDB_CODE_SUCCESS) {
      sdbCancelFetch(pSdb, pIter);
      return -1;
    }
  }

  // no need to build the trans to handle the vgroup upddate
  if (pTrans == NULL) {
    return 0;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare update stream trans since %s", pTrans->id, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);
  return 0;
}

static SArray *extractNodeListFromStream(SMnode *pMnode) {
  SSdb       *pSdb = pMnode->pSdb;
  SStreamObj *pStream = NULL;
  void       *pIter = NULL;

  SHashObj *pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) {
      break;
    }

    taosWLockLatch(&pStream->lock);

    SStreamTaskIter *pTaskIter = createStreamTaskIter(pStream);
    while (streamTaskIterNextTask(pTaskIter)) {
      SStreamTask *pTask = streamTaskIterGetCurrent(pTaskIter);

      SNodeEntry entry = {.hbTimestamp = -1, .nodeId = pTask->info.nodeId};
      epsetAssign(&entry.epset, &pTask->info.epSet);
      taosHashPut(pHash, &entry.nodeId, sizeof(entry.nodeId), &entry, sizeof(entry));
    }

    destroyStreamTaskIter(pTaskIter);
    taosWUnLockLatch(&pStream->lock);

    sdbRelease(pSdb, pStream);
  }

  SArray *plist = taosArrayInit(taosHashGetSize(pHash), sizeof(SNodeEntry));

  // convert to list
  pIter = NULL;
  while ((pIter = taosHashIterate(pHash, pIter)) != NULL) {
    SNodeEntry *pEntry = (SNodeEntry *)pIter;
    taosArrayPush(plist, pEntry);

    char buf[256] = {0};
    EPSET_TO_STR(&pEntry->epset, buf);
    mDebug("extract nodeInfo from stream obj, nodeId:%d, %s", pEntry->nodeId, buf);
  }
  taosHashCleanup(pHash);

  return plist;
}

static int32_t doRemoveTasks(SStreamExecInfo *pExecNode, STaskId *pRemovedId) {
  void *p = taosHashGet(pExecNode->pTaskMap, pRemovedId, sizeof(*pRemovedId));
  if (p == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  taosHashRemove(pExecNode->pTaskMap, pRemovedId, sizeof(*pRemovedId));

  for (int32_t k = 0; k < taosArrayGetSize(pExecNode->pTaskList); ++k) {
    STaskId *pId = taosArrayGet(pExecNode->pTaskList, k);
    if (pId->taskId == pRemovedId->taskId && pId->streamId == pRemovedId->streamId) {
      taosArrayRemove(pExecNode->pTaskList, k);

      int32_t num = taosArrayGetSize(pExecNode->pTaskList);
      mInfo("s-task:0x%x removed from buffer, remain:%d", (int32_t)pRemovedId->taskId, num);
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static bool taskNodeExists(SArray *pList, int32_t nodeId) {
  size_t num = taosArrayGetSize(pList);

  for (int32_t i = 0; i < num; ++i) {
    SNodeEntry *pEntry = taosArrayGet(pList, i);
    if (pEntry->nodeId == nodeId) {
      return true;
    }
  }

  return false;
}

int32_t removeExpirednodeEntryAndTask(SArray *pNodeSnapshot) {
  SArray *pRemovedTasks = taosArrayInit(4, sizeof(STaskId));

  int32_t numOfTask = taosArrayGetSize(execInfo.pTaskList);
  for (int32_t i = 0; i < numOfTask; ++i) {
    STaskId          *pId = taosArrayGet(execInfo.pTaskList, i);
    STaskStatusEntry *pEntry = taosHashGet(execInfo.pTaskMap, pId, sizeof(*pId));

    if (pEntry->nodeId == SNODE_HANDLE) continue;

    bool existed = taskNodeExists(pNodeSnapshot, pEntry->nodeId);
    if (!existed) {
      taosArrayPush(pRemovedTasks, pId);
    }
  }

  for (int32_t i = 0; i < taosArrayGetSize(pRemovedTasks); ++i) {
    STaskId *pId = taosArrayGet(pRemovedTasks, i);
    doRemoveTasks(&execInfo, pId);
  }

  mDebug("remove invalid stream tasks:%d, remain:%d", (int32_t)taosArrayGetSize(pRemovedTasks),
         (int32_t)taosArrayGetSize(execInfo.pTaskList));

  int32_t size = taosArrayGetSize(pNodeSnapshot);
  SArray *pValidNodeEntryList = taosArrayInit(4, sizeof(SNodeEntry));
  for (int32_t i = 0; i < taosArrayGetSize(execInfo.pNodeList); ++i) {
    SNodeEntry *p = taosArrayGet(execInfo.pNodeList, i);

    for (int32_t j = 0; j < size; ++j) {
      SNodeEntry *pEntry = taosArrayGet(pNodeSnapshot, j);
      if (pEntry->nodeId == p->nodeId) {
        taosArrayPush(pValidNodeEntryList, p);
        break;
      }
    }
  }

  taosArrayDestroy(execInfo.pNodeList);
  execInfo.pNodeList = pValidNodeEntryList;

  mDebug("remain %d valid node entries", (int32_t)taosArrayGetSize(pValidNodeEntryList));
  taosArrayDestroy(pRemovedTasks);
  return 0;
}

// this function runs by only one thread, so it is not multi-thread safe
static int32_t mndProcessNodeCheckReq(SRpcMsg *pMsg) {
  int32_t code = 0;

  int32_t old = atomic_val_compare_exchange_32(&mndNodeCheckSentinel, 0, 1);
  if (old != 0) {
    mDebug("still in checking node change");
    return 0;
  }

  mDebug("start to do node change checking");
  int64_t ts = taosGetTimestampSec();

  SMnode *pMnode = pMsg->info.node;

  taosThreadMutexLock(&execInfo.lock);
  int32_t numOfNodes = initStreamNodeList(pMnode);
  taosThreadMutexUnlock(&execInfo.lock);

  if (numOfNodes == 0) {
    mDebug("end to do stream task node change checking, no vgroup exists, do nothing");
    execInfo.ts = ts;
    atomic_store_32(&mndNodeCheckSentinel, 0);
    return 0;
  }

  bool    allVgroupsReady = true;
  SArray *pNodeSnapshot = mndTakeVgroupSnapshot(pMnode, &allVgroupsReady);
  if (!allVgroupsReady) {
    taosArrayDestroy(pNodeSnapshot);
    atomic_store_32(&mndNodeCheckSentinel, 0);
    mWarn("not all vnodes are ready, ignore the exec nodeUpdate check");
    return 0;
  }

  taosThreadMutexLock(&execInfo.lock);
  removeExpirednodeEntryAndTask(pNodeSnapshot);

  SVgroupChangeInfo changeInfo = mndFindChangedNodeInfo(pMnode, execInfo.pNodeList, pNodeSnapshot);
  if (taosArrayGetSize(changeInfo.pUpdateNodeList) > 0) {
    // kill current active checkpoint transaction, since the transaction is vnode wide.
    killAllCheckpointTrans(pMnode, &changeInfo);

    code = mndProcessVgroupChange(pMnode, &changeInfo);

    // keep the new vnode snapshot if success
    if (code == TSDB_CODE_SUCCESS || code == TSDB_CODE_ACTION_IN_PROGRESS) {
      mDebug("create trans successfully, update cached node list");
      taosArrayDestroy(execInfo.pNodeList);
      execInfo.pNodeList = pNodeSnapshot;
      execInfo.ts = ts;
    } else {
      mError("unexpected code during create nodeUpdate trans, code:%s", tstrerror(code));
      taosArrayDestroy(pNodeSnapshot);
    }
  } else {
    mDebug("no update found in nodeList");
    taosArrayDestroy(pNodeSnapshot);
  }

  taosThreadMutexUnlock(&execInfo.lock);
  taosArrayDestroy(changeInfo.pUpdateNodeList);
  taosHashCleanup(changeInfo.pDBMap);

  mDebug("end to do stream task node change checking");
  atomic_store_32(&mndNodeCheckSentinel, 0);
  return 0;
}

static int32_t mndProcessNodeCheck(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  if (sdbGetSize(pSdb, SDB_STREAM) <= 0) {
    return 0;
  }

  SMStreamNodeCheckMsg *pMsg = rpcMallocCont(sizeof(SMStreamNodeCheckMsg));

  SRpcMsg rpcMsg = {
      .msgType = TDMT_MND_STREAM_NODECHANGE_CHECK, .pCont = pMsg, .contLen = sizeof(SMStreamNodeCheckMsg)};
  tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  return 0;
}

void saveStreamTasksInfo(SStreamObj *pStream, SStreamExecInfo *pExecNode) {
  SStreamTaskIter *pIter = createStreamTaskIter(pStream);
  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = streamTaskIterGetCurrent(pIter);

    STaskId id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};
    void   *p = taosHashGet(pExecNode->pTaskMap, &id, sizeof(id));
    if (p == NULL) {
      STaskStatusEntry entry = {0};
      streamTaskStatusInit(&entry, pTask);

      taosHashPut(pExecNode->pTaskMap, &id, sizeof(id), &entry, sizeof(entry));
      taosArrayPush(pExecNode->pTaskList, &id);
      mInfo("s-task:0x%x add into task buffer, total:%d", (int32_t)entry.id.taskId,
            (int32_t)taosArrayGetSize(pExecNode->pTaskList));
    }
  }

  destroyStreamTaskIter(pIter);
}

void removeStreamTasksInBuf(SStreamObj *pStream, SStreamExecInfo *pExecNode) {
  SStreamTaskIter *pIter = createStreamTaskIter(pStream);
  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = streamTaskIterGetCurrent(pIter);

    STaskId id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};
    void   *p = taosHashGet(pExecNode->pTaskMap, &id, sizeof(id));
    if (p != NULL) {
      taosHashRemove(pExecNode->pTaskMap, &id, sizeof(id));

      for (int32_t k = 0; k < taosArrayGetSize(pExecNode->pTaskList); ++k) {
        STaskId *pId = taosArrayGet(pExecNode->pTaskList, k);
        if (pId->taskId == id.taskId && pId->streamId == id.streamId) {
          taosArrayRemove(pExecNode->pTaskList, k);

          int32_t num = taosArrayGetSize(pExecNode->pTaskList);
          mInfo("s-task:0x%x removed from buffer, remain:%d", (int32_t)id.taskId, num);
          break;
        }
      }
    }
  }

  destroyStreamTaskIter(pIter);
  ASSERT(taosHashGetSize(pExecNode->pTaskMap) == taosArrayGetSize(pExecNode->pTaskList));
}

static void doAddTaskId(SArray *pList, int32_t taskId, int64_t uid, int32_t numOfTotal) {
  int32_t num = taosArrayGetSize(pList);
  for (int32_t i = 0; i < num; ++i) {
    int32_t *pId = taosArrayGet(pList, i);
    if (taskId == *pId) {
      return;
    }
  }

  taosArrayPush(pList, &taskId);

  int32_t numOfTasks = taosArrayGetSize(pList);
  mDebug("stream:0x%" PRIx64 " receive %d reqs for checkpoint, remain:%d", uid, numOfTasks, numOfTotal - numOfTasks);
}

int32_t mndProcessStreamReqCheckpoint(SRpcMsg *pReq) {
  SMnode                  *pMnode = pReq->info.node;
  SStreamTaskCheckpointReq req = {0};

  SDecoder decoder = {0};
  tDecoderInit(&decoder, pReq->pCont, pReq->contLen);

  if (tDecodeStreamTaskCheckpointReq(&decoder, &req)) {
    tDecoderClear(&decoder);
    terrno = TSDB_CODE_INVALID_MSG;
    mError("invalid task checkpoint req msg received");
    return -1;
  }
  tDecoderClear(&decoder);

  mDebug("receive stream task checkpoint req msg, vgId:%d, s-task:0x%x", req.nodeId, req.taskId);

  // register to the stream task done map, if all tasks has sent this kinds of message, start the checkpoint trans.
  taosThreadMutexLock(&execInfo.lock);

  SStreamObj *pStream = mndGetStreamObj(pMnode, req.streamId);
  if (pStream == NULL) {
    mError("failed to find the stream:0x%" PRIx64 " not handle the checkpoint req", req.streamId);
    terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
    taosThreadMutexUnlock(&execInfo.lock);

    return -1;
  }

  int32_t  numOfTasks = mndGetNumOfStreamTasks(pStream);
  SArray **pReqTaskList = (SArray **)taosHashGet(execInfo.pTransferStateStreams, &req.streamId, sizeof(req.streamId));
  if (pReqTaskList == NULL) {
    SArray *pList = taosArrayInit(4, sizeof(int32_t));
    doAddTaskId(pList, req.taskId, pStream->uid, numOfTasks);
    taosHashPut(execInfo.pTransferStateStreams, &req.streamId, sizeof(int64_t), &pList, sizeof(void *));

    pReqTaskList = (SArray **)taosHashGet(execInfo.pTransferStateStreams, &req.streamId, sizeof(req.streamId));
  } else {
    doAddTaskId(*pReqTaskList, req.taskId, pStream->uid, numOfTasks);
  }

  int32_t total = taosArrayGetSize(*pReqTaskList);
  if (total == numOfTasks) {  // all tasks has send the reqs
    int64_t checkpointId = mndStreamGenChkpId(pMnode);
    mDebug("stream:0x%" PRIx64 " all tasks req, start checkpointId:%" PRId64, pStream->uid, checkpointId);

    // TODO:handle error
    int32_t code = mndProcessStreamCheckpointTrans(pMnode, pStream, checkpointId, 0, false);

    // remove this entry
    taosHashRemove(execInfo.pTransferStateStreams, &req.streamId, sizeof(int64_t));

    int32_t numOfStreams = taosHashGetSize(execInfo.pTransferStateStreams);
    mDebug("stream:0x%" PRIx64 " removed, remain streams:%d fill-history not completed", pStream->uid, numOfStreams);
  }

  mndReleaseStream(pMnode, pStream);
  taosThreadMutexUnlock(&execInfo.lock);

  {
    SRpcMsg rsp = {.code = 0, .info = pReq->info, .contLen = sizeof(SMStreamReqCheckpointRspMsg)};
    rsp.pCont = rpcMallocCont(rsp.contLen);
    SMsgHead* pHead = rsp.pCont;
    pHead->vgId = htonl(req.nodeId);

    tmsgSendRsp(&rsp);

    pReq->info.handle = NULL;   // disable auto rsp
  }

  return 0;
}
