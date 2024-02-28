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
#include "osMemory.h"
#include "parser.h"
#include "tmisce.h"
#include "tname.h"

#define MND_STREAM_VER_NUMBER      4
#define MND_STREAM_RESERVE_SIZE    64
#define MND_STREAM_MAX_NUM         60
#define MND_STREAM_CHECKPOINT_NAME "stream-checkpoint"

typedef struct SNodeEntry {
  int32_t nodeId;
  SEpSet  epset;        // compare the epset to identify the vgroup tranferring between different dnodes.
  int64_t hbTimestamp;  // second
} SNodeEntry;

typedef struct SStreamVnodeRevertIndex {
  SArray *      pNodeEntryList;
  int64_t       ts;  // snapshot ts
  SHashObj *    pTaskMap;
  SArray *      pTaskList;
  TdThreadMutex lock;
} SStreamVnodeRevertIndex;

typedef struct SVgroupChangeInfo {
  SHashObj *pDBMap;
  SArray *  pUpdateNodeList;  // SArray<SNodeUpdateInfo>
} SVgroupChangeInfo;

static int32_t                 mndNodeCheckSentinel = 0;
static SStreamVnodeRevertIndex execNodeList;

static int32_t mndStreamActionInsert(SSdb *pSdb, SStreamObj *pStream);
static int32_t mndStreamActionDelete(SSdb *pSdb, SStreamObj *pStream);
static int32_t mndStreamActionUpdate(SSdb *pSdb, SStreamObj *pOldStream, SStreamObj *pNewStream);
static int32_t mndProcessCreateStreamReq(SRpcMsg *pReq);
static int32_t mndProcessDropStreamReq(SRpcMsg *pReq);
static int32_t mndProcessStreamCheckpointTmr(SRpcMsg *pReq);
static int32_t mndProcessStreamDoCheckpoint(SRpcMsg *pReq);
static int32_t mndProcessStreamHb(SRpcMsg *pReq);
static int32_t mndProcessRecoverStreamReq(SRpcMsg *pReq);
static int32_t mndProcessStreamMetaReq(SRpcMsg *pReq);
static int32_t mndGetStreamMeta(SRpcMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t mndRetrieveStream(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextStream(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveStreamTask(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextStreamTask(SMnode *pMnode, void *pIter);
static int32_t mndProcessPauseStreamReq(SRpcMsg *pReq);
static int32_t mndProcessResumeStreamReq(SRpcMsg *pReq);
static int32_t mndBuildStreamCheckpointSourceReq2(void **pBuf, int32_t *pLen, int32_t nodeId, int64_t checkpointId,
                                                  int64_t streamId, int32_t taskId);
static int32_t mndProcessNodeCheck(SRpcMsg *pReq);
static int32_t mndProcessNodeCheckReq(SRpcMsg *pMsg);
static void    keepStreamTasksInBuf(SStreamObj *pStream, SStreamVnodeRevertIndex *pExecNode);

static SArray *          doExtractNodeListFromStream(SMnode *pMnode);
static SArray *          mndTakeVgroupSnapshot(SMnode *pMnode);
static SVgroupChangeInfo mndFindChangedNodeInfo(SMnode *pMnode, const SArray *pPrevNodeList, const SArray *pNodeList);
static int32_t           mndPersistTransLog(SStreamObj *pStream, STrans *pTrans);
static void initTransAction(STransAction *pAction, void *pCont, int32_t contLen, int32_t msgType, const SEpSet *pEpset);

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
  mndSetMsgHandle(pMnode, TDMT_MND_NODECHECK_TIMER, mndProcessNodeCheck);

  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_DEPLOY_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_DROP_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_PAUSE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_RESUME_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_STOP_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_STREAM_TASK_UPDATE_RSP, mndTransProcessRsp);

  mndSetMsgHandle(pMnode, TDMT_VND_STREAM_CHECK_POINT_SOURCE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_CHECKPOINT_TIMER, mndProcessStreamCheckpointTmr);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_BEGIN_CHECKPOINT, mndProcessStreamDoCheckpoint);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_HEARTBEAT, mndProcessStreamHb);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_REPORT_CHECKPOINT, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_NODECHANGE_CHECK, mndProcessNodeCheckReq);

  mndSetMsgHandle(pMnode, TDMT_MND_PAUSE_STREAM, mndProcessPauseStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_RESUME_STREAM, mndProcessResumeStreamReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndRetrieveStream);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndCancelGetNextStream);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAM_TASKS, mndRetrieveStreamTask);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAM_TASKS, mndCancelGetNextStreamTask);

  taosThreadMutexInit(&execNodeList.lock, NULL);
  execNodeList.pTaskMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_NO_LOCK);
  execNodeList.pTaskList = taosArrayInit(4, sizeof(STaskStatusEntry));

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupStream(SMnode *pMnode) {
  taosArrayDestroy(execNodeList.pTaskList);
  taosHashCleanup(execNodeList.pTaskMap);
  taosThreadMutexDestroy(&execNodeList.lock);
  mDebug("mnd stream cleanup");
}

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
  SSdbRow *   pRow = NULL;
  SStreamObj *pStream = NULL;
  void *      buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto STREAM_DECODE_OVER;
  }

  if (sver != MND_STREAM_VER_NUMBER) {
    terrno = 0;
    mError("stream read invalid ver, data ver: %d, curr ver: %d", sver, MND_STREAM_VER_NUMBER);
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

  atomic_exchange_32(&pOldStream->version, pNewStream->version);

  taosWLockLatch(&pOldStream->lock);

  pOldStream->status = pNewStream->status;
  pOldStream->updateTime = pNewStream->updateTime;

  taosWUnLockLatch(&pOldStream->lock);
  return 0;
}

SStreamObj *mndAcquireStream(SMnode *pMnode, char *streamName) {
  SSdb *      pSdb = pMnode->pSdb;
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

static int32_t mndStreamGetPlanString(const char *ast, int8_t triggerType, int64_t watermark, char **pStr) {
  if (NULL == ast) {
    return TSDB_CODE_SUCCESS;
  }

  SNode * pAst = NULL;
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
  SNode *     pAst = NULL;
  SQueryPlan *pPlan = NULL;

  mInfo("stream:%s to create", pCreate->name);
  memcpy(pObj->name, pCreate->name, TSDB_STREAM_FNAME_LEN);
  pObj->createTime = taosGetTimestampMs();
  pObj->updateTime = pObj->createTime;
  pObj->version = 1;
  pObj->smaId = 0;

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

  STransAction action = {0};
  action.mTraceId = pTrans->mTraceId;
  initTransAction(&action, buf, tlen, TDMT_STREAM_TASK_DEPLOY, &pTask->info.epSet);
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

    int32_t numOfTasks = taosArrayGetSize(pLevel);
    for (int32_t j = 0; j < numOfTasks; j++) {
      SStreamTask *pTask = taosArrayGetP(pLevel, j);
      if (mndPersistTaskDeployReq(pTrans, pTask) < 0) {
        return -1;
      }
    }
  }

  // persistent stream task for already stored ts data
  if (pStream->conf.fillHistory) {
    level = taosArrayGetSize(pStream->pHTasksList);

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
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
  return 0;
}

static int32_t mndCreateStbForStream(SMnode *pMnode, STrans *pTrans, const SStreamObj *pStream, const char *user) {
  SStbObj *pStb = NULL;
  SDbObj * pDb = NULL;

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
  SVDropStreamTaskReq *pReq = taosMemoryCalloc(1, sizeof(SVDropStreamTaskReq));
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pReq->head.vgId = htonl(pTask->info.nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->streamId = pTask->id.streamId;

  STransAction action = {0};
  initTransAction(&action, pReq, sizeof(SVDropStreamTaskReq), TDMT_STREAM_TASK_DROP, &pTask->info.epSet);
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

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
  SMnode *           pMnode = pReq->info.node;
  int32_t            code = -1;
  SStreamObj *       pStream = NULL;
  SDbObj *           pDb = NULL;
  SCMCreateStreamReq createStreamReq = {0};
  SStreamObj         streamObj = {0};
  char              *sql = NULL;
  int32_t            sqlLen = 0;

  if (tDeserializeSCMCreateStreamReq(pReq->pCont, pReq->contLen, &createStreamReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }
#ifdef WINDOWS
  terrno = TSDB_CODE_MND_INVALID_PLATFORM;
  goto _OVER;
#endif
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

  if(createStreamReq.sql != NULL){
    sqlLen = strlen(createStreamReq.sql);
    sql = taosMemoryMalloc(sqlLen + 1);
    memset(sql, 0, sqlLen + 1);
    memcpy(sql, createStreamReq.sql, sqlLen);
  }

  // build stream obj from request
  if (mndBuildStreamObjFromCreateReq(pMnode, &streamObj, &createStreamReq) < 0) {
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
    goto _OVER;
  }

  {
    int32_t numOfStream = 0;

    SStreamObj *pStream = NULL;
    void *      pIter = NULL;

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
        sdbCancelFetch(pMnode->pSdb, pIter);
        goto _OVER;
      }

      if (pStream->targetStbUid == streamObj.targetStbUid) {
        mError("Cannot write the same stable as other stream:%s", pStream->name);
        terrno = TSDB_CODE_MND_INVALID_TARGET_TABLE;
        sdbCancelFetch(pMnode->pSdb, pIter);
        goto _OVER;
      }
    }
  }

  //  pDb = mndAcquireDb(pMnode, streamObj.sourceDb);
  //  if (pDb->cfg.replications != 1) {
  //    mError("stream source db must have only 1 replica, but %s has %d", pDb->name, pDb->cfg.replications);
  //    terrno = TSDB_CODE_MND_MULTI_REPLICA_SOURCE_DB;
  //    mndReleaseDb(pMnode, pDb);
  //    pDb = NULL;
  //    goto _OVER;
  //  }

  //  mndReleaseDb(pMnode, pDb);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_DB_INSIDE, pReq, "create-stream");
  if (pTrans == NULL) {
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
    goto _OVER;
  }

  mInfo("trans:%d, used to create stream:%s", pTrans->id, createStreamReq.name);

  mndTransSetDbName(pTrans, createStreamReq.sourceDB, streamObj.targetDb);
  if (mndTransCheckConflict(pMnode, pTrans) != 0) {
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
  if (mndScheduleStream(pMnode, &streamObj, createStreamReq.lastTs) < 0) {
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

  taosThreadMutexLock(&execNodeList.lock);
  keepStreamTasksInBuf(&streamObj, &execNodeList);
  taosThreadMutexUnlock(&execNodeList.lock);

  code = TSDB_CODE_ACTION_IN_PROGRESS;

  SName dbname = {0};
  tNameFromString(&dbname, createStreamReq.sourceDB, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

  SName name = {0};
  tNameFromString(&name, createStreamReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  // reuse this function for stream

  if (sql != NULL && sqlLen > 0) {
    auditRecord(pReq, pMnode->clusterId, "createStream", dbname.dbname, name.dbname, sql,
                sqlLen);
  }
  else{
    char detail[1000] = {0};
    sprintf(detail, "dbname:%s, stream name:%s", dbname.dbname, name.dbname);
    auditRecord(pReq, pMnode->clusterId, "createStream", dbname.dbname, name.dbname, detail, strlen(detail));
  }
_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
  }

  mndReleaseStream(pMnode, pStream);

  tFreeSCMCreateStreamReq(&createStreamReq);
  tFreeStreamObj(&streamObj);
  if(sql != NULL){
    taosMemoryFreeClear(sql);
  }
  return code;
}

int64_t mndStreamGenChkpId(SMnode *pMnode) {
  SStreamObj *pStream = NULL;
  void *      pIter = NULL;
  SSdb *      pSdb = pMnode->pSdb;
  int64_t     maxChkpId = 0;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

    maxChkpId = TMAX(maxChkpId, pStream->checkpointId);
    sdbRelease(pSdb, pStream);
  }
  return maxChkpId + 1;
}
static int32_t mndProcessStreamCheckpointTmr(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  SSdb *  pSdb = pMnode->pSdb;
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

static int32_t mndBuildStreamCheckpointSourceReq2(void **pBuf, int32_t *pLen, int32_t nodeId, int64_t checkpointId,
                                                  int64_t streamId, int32_t taskId) {
  SStreamCheckpointSourceReq req = {0};
  req.checkpointId = checkpointId;
  req.nodeId = nodeId;
  req.expireTime = -1;
  req.streamId = streamId;  // pTask->id.streamId;
  req.taskId = taskId;      // pTask->id.taskId;

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

  void *   abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
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
// static int32_t mndProcessStreamCheckpointTrans(SMnode *pMnode, SStreamObj *pStream, int64_t checkpointId) {
//   int64_t timestampMs = taosGetTimestampMs();
//   if (timestampMs - pStream->checkpointFreq < tsStreamCheckpointTickInterval * 1000) {
//     return -1;
//   }

//   STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, NULL, "stream-checkpoint");
//   if (pTrans == NULL) return -1;
//   mndTransSetDbName(pTrans, pStream->sourceDb, pStream->targetDb);
//   if (mndTrancCheckConflict(pMnode, pTrans) != 0) {
//     mError("failed to checkpoint of stream name%s, checkpointId: %" PRId64 ", reason:%s", pStream->name,
//     checkpointId,
//            tstrerror(TSDB_CODE_MND_TRANS_CONFLICT));
//     mndTransDrop(pTrans);
//     return -1;
//   }
//   mDebug("start to trigger checkpoint for stream:%s, checkpoint: %" PRId64 "", pStream->name, checkpointId);
//   atomic_store_64(&pStream->currentTick, 1);
//   taosWLockLatch(&pStream->lock);
//   // 1. redo action: broadcast checkpoint source msg for all source vg
//   int32_t totLevel = taosArrayGetSize(pStream->tasks);
//   for (int32_t i = 0; i < totLevel; i++) {
//     SArray      *pLevel = taosArrayGetP(pStream->tasks, i);
//     SStreamTask *pTask = taosArrayGetP(pLevel, 0);
//     if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
//       int32_t sz = taosArrayGetSize(pLevel);
//       for (int32_t j = 0; j < sz; j++) {
//         SStreamTask *pTask = taosArrayGetP(pLevel, j);
//         /*A(pTask->info.nodeId > 0);*/
//         SVgObj *pVgObj = mndAcquireVgroup(pMnode, pTask->info.nodeId);
//         if (pVgObj == NULL) {
//           taosWUnLockLatch(&pStream->lock);
//           mndTransDrop(pTrans);
//           return -1;
//         }

//         void   *buf;
//         int32_t tlen;
//         if (mndBuildStreamCheckpointSourceReq2(&buf, &tlen, pTask->info.nodeId, checkpointId, pTask->id.streamId,
//                                                pTask->id.taskId) < 0) {
//           mndReleaseVgroup(pMnode, pVgObj);
//           taosWUnLockLatch(&pStream->lock);
//           mndTransDrop(pTrans);
//           return -1;
//         }

//         STransAction action = {0};
//         action.epSet = mndGetVgroupEpset(pMnode, pVgObj);
//         action.pCont = buf;
//         action.contLen = tlen;
//         action.msgType = TDMT_VND_STREAM_CHECK_POINT_SOURCE;

//         mndReleaseVgroup(pMnode, pVgObj);

//         if (mndTransAppendRedoAction(pTrans, &action) != 0) {
//           taosMemoryFree(buf);
//           taosWUnLockLatch(&pStream->lock);
//           mndReleaseStream(pMnode, pStream);
//           mndTransDrop(pTrans);
//           return -1;
//         }
//       }
//     }
//   }
//   // 2. reset tick
//   pStream->checkpointFreq = checkpointId;
//   pStream->checkpointId = checkpointId;
//   pStream->checkpointFreq = taosGetTimestampMs();
//   atomic_store_64(&pStream->currentTick, 0);
//   // 3. commit log: stream checkpoint info
//   pStream->version = pStream->version + 1;
//   taosWUnLockLatch(&pStream->lock);

//   //   // code condtion

//   SSdbRaw *pCommitRaw = mndStreamActionEncode(pStream);
//   if (pCommitRaw == NULL) {
//     mError("failed to prepare trans rebalance since %s", terrstr());
//     goto _ERR;
//   }
//   if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
//     sdbFreeRaw(pCommitRaw);
//     mError("failed to prepare trans rebalance since %s", terrstr());
//     goto _ERR;
//   }
//   if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) {
//     sdbFreeRaw(pCommitRaw);
//     mError("failed to prepare trans rebalance since %s", terrstr());
//     goto _ERR;
//   }

//   if (mndTransPrepare(pMnode, pTrans) != 0) {
//     mError("failed to prepare trans rebalance since %s", terrstr());
//     goto _ERR;
//   }
//   mndTransDrop(pTrans);
//   return 0;
// _ERR:
//   mndTransDrop(pTrans);
//   return -1;
// }

static int32_t mndAddStreamCheckpointToTrans(STrans *pTrans, SStreamObj *pStream, SMnode *pMnode,
                                             int64_t checkpointId) {
  taosWLockLatch(&pStream->lock);

  int32_t totLevel = taosArrayGetSize(pStream->tasks);
  for (int32_t i = 0; i < totLevel; i++) {
    SArray *     pLevel = taosArrayGetP(pStream->tasks, i);
    SStreamTask *pTask = taosArrayGetP(pLevel, 0);

    if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
      int32_t sz = taosArrayGetSize(pLevel);
      for (int32_t j = 0; j < sz; j++) {
        pTask = taosArrayGetP(pLevel, j);
        if (pTask->info.fillHistory == 1) {
          continue;
        }
        /*A(pTask->info.nodeId > 0);*/
        SVgObj *pVgObj = mndAcquireVgroup(pMnode, pTask->info.nodeId);
        if (pVgObj == NULL) {
          taosWUnLockLatch(&pStream->lock);
          return -1;
        }

        void *  buf;
        int32_t tlen;
        if (mndBuildStreamCheckpointSourceReq2(&buf, &tlen, pTask->info.nodeId, checkpointId, pTask->id.streamId,
                                               pTask->id.taskId) < 0) {
          mndReleaseVgroup(pMnode, pVgObj);
          taosWUnLockLatch(&pStream->lock);
          return -1;
        }

        STransAction action = {0};
        SEpSet       epset = mndGetVgroupEpset(pMnode, pVgObj);
        initTransAction(&action, buf, tlen, TDMT_VND_STREAM_CHECK_POINT_SOURCE, &epset);
        mndReleaseVgroup(pMnode, pVgObj);

        if (mndTransAppendRedoAction(pTrans, &action) != 0) {
          taosMemoryFree(buf);
          taosWUnLockLatch(&pStream->lock);
          return -1;
        }
      }
    }
  }

  pStream->checkpointId = checkpointId;
  pStream->checkpointFreq = taosGetTimestampMs();
  pStream->currentTick = 0;
  // 3. commit log: stream checkpoint info
  pStream->version = pStream->version + 1;

  taosWUnLockLatch(&pStream->lock);

  SSdbRaw *pCommitRaw = mndStreamActionEncode(pStream);
  if (pCommitRaw == NULL) {
    mError("failed to prepare trans rebalance since %s", terrstr());
    return -1;
  }
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    sdbFreeRaw(pCommitRaw);
    mError("failed to prepare trans rebalance since %s", terrstr());
    return -1;
  }
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) {
    sdbFreeRaw(pCommitRaw);
    mError("failed to prepare trans rebalance since %s", terrstr());
    return -1;
  }
  return 0;
}

static const char *mndGetStreamDB(SMnode *pMnode) {
  SSdb *      pSdb = pMnode->pSdb;
  SStreamObj *pStream = NULL;
  void *      pIter = NULL;

  pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
  if (pIter == NULL) {
    return NULL;
  }

  const char *p = taosStrdup(pStream->sourceDb);
  mndReleaseStream(pMnode, pStream);
  sdbCancelFetch(pSdb, pIter);
  return p;
}

static int32_t mndProcessStreamDoCheckpoint(SRpcMsg *pReq) {
  SMnode *    pMnode = pReq->info.node;
  SSdb *      pSdb = pMnode->pSdb;
  void *      pIter = NULL;
  SStreamObj *pStream = NULL;
  int32_t     code = 0;

  {  // check if the node update happens or not
    int64_t ts = taosGetTimestampSec();

    if (execNodeList.pNodeEntryList == NULL || (taosArrayGetSize(execNodeList.pNodeEntryList) == 0)) {
      if (execNodeList.pNodeEntryList != NULL) {
        execNodeList.pNodeEntryList = taosArrayDestroy(execNodeList.pNodeEntryList);
      }

      execNodeList.pNodeEntryList = doExtractNodeListFromStream(pMnode);
    }

    if (taosArrayGetSize(execNodeList.pNodeEntryList) == 0) {
      mDebug("end to do stream task node change checking, no vgroup exists, do nothing");
      execNodeList.ts = ts;
      atomic_store_32(&mndNodeCheckSentinel, 0);
      return 0;
    }

    SArray *pNodeSnapshot = mndTakeVgroupSnapshot(pMnode);

    SVgroupChangeInfo changeInfo = mndFindChangedNodeInfo(pMnode, execNodeList.pNodeEntryList, pNodeSnapshot);
    bool              nodeUpdated = (taosArrayGetSize(changeInfo.pUpdateNodeList) > 0);
    taosArrayDestroy(changeInfo.pUpdateNodeList);
    taosHashCleanup(changeInfo.pDBMap);
    taosArrayDestroy(pNodeSnapshot);

    if (nodeUpdated) {
      mDebug("stream task not ready due to node update, not generate checkpoint");
      return 0;
    }
  }

  {  // check if all tasks are in TASK_STATUS__NORMAL status
    bool ready = true;

    taosThreadMutexLock(&execNodeList.lock);
    for (int32_t i = 0; i < taosArrayGetSize(execNodeList.pTaskList); ++i) {
      STaskStatusEntry *p = taosArrayGet(execNodeList.pTaskList, i);
      if (p->status != TASK_STATUS__NORMAL) {
        mDebug("s-task:0x%" PRIx64 "-0x%x (nodeId:%d) status:%s not ready, create checkpoint msg not issued",
               p->streamId, p->taskId, 0, streamGetTaskStatusStr(p->status));
        ready = false;
        break;
      }
    }
    taosThreadMutexUnlock(&execNodeList.lock);

    if (!ready) {
      return 0;
    }
  }

  SMStreamDoCheckpointMsg *pMsg = (SMStreamDoCheckpointMsg *)pReq->pCont;
  int64_t                  checkpointId = pMsg->checkpointId;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, NULL, MND_STREAM_CHECKPOINT_NAME);
  if (pTrans == NULL) {
    mError("failed to trigger checkpoint, reason: %s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return -1;
  }
  mDebug("start to trigger checkpoint, checkpointId: %" PRId64 "", checkpointId);

  const char *pDb = mndGetStreamDB(pMnode);
  mndTransSetDbName(pTrans, pDb, "checkpoint");
  taosMemoryFree((void *)pDb);

  if (mndTransCheckConflict(pMnode, pTrans) != 0) {
    mError("failed to trigger checkpoint, checkpointId: %" PRId64 ", reason:%s", checkpointId,
           tstrerror(TSDB_CODE_MND_TRANS_CONFLICT));
    mndTransDrop(pTrans);
    return -1;
  }

  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

    code = mndAddStreamCheckpointToTrans(pTrans, pStream, pMnode, checkpointId);
    sdbRelease(pSdb, pStream);
    if (code == -1) {
      break;
    }
  }

  if (code == 0) {
    if (mndTransPrepare(pMnode, pTrans) != 0) {
      mError("failed to prepre trans rebalance since %s", terrstr());
    }
  }

  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropStreamReq(SRpcMsg *pReq) {
  SMnode *    pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;

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
      tFreeSMDropStreamReq(&dropReq);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
      tFreeSMDropStreamReq(&dropReq);
      return -1;
    }
  }

  if (mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->targetDb) != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    tFreeSMDropStreamReq(&dropReq);
    return -1;
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "drop-stream");
  if (pTrans == NULL) {
    mError("stream:%s, failed to drop since %s", dropReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    tFreeSMDropStreamReq(&dropReq);
    return -1;
  }

  mInfo("trans:%d, used to drop stream:%s", pTrans->id, dropReq.name);

  mndTransSetDbName(pTrans, pStream->sourceDb, pStream->targetDb);
  if (mndTransCheckConflict(pMnode, pTrans) != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    tFreeSMDropStreamReq(&dropReq);
    return -1;
  }
  // mndTransSetSerial(pTrans);

  // drop all tasks
  if (mndDropStreamTasks(pMnode, pTrans, pStream) < 0) {
    mError("stream:%s, failed to drop task since %s", dropReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    tFreeSMDropStreamReq(&dropReq);
    return -1;
  }

  // drop stream
  if (mndPersistDropStreamLog(pMnode, pTrans, pStream) < 0) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    tFreeSMDropStreamReq(&dropReq);
    return -1;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare drop stream trans since %s", pTrans->id, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    tFreeSMDropStreamReq(&dropReq);
    return -1;
  }

  SName name = {0};
  tNameFromString(&name, dropReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  // reuse this function for stream

  auditRecord(pReq, pMnode->clusterId, "dropStream", "", name.dbname, dropReq.sql, dropReq.sqlLen);

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);
  tFreeSMDropStreamReq(&dropReq);

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

int32_t mndGetNumOfStreams(SMnode *pMnode, char *dbName, int32_t *pNumOfStreams) {
  SSdb *  pSdb = pMnode->pSdb;
  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    return -1;
  }

  int32_t numOfStreams = 0;
  void *  pIter = NULL;
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
  SMnode *    pMnode = pReq->info.node;
  SSdb *      pSdb = pMnode->pSdb;
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
    colDataSetVal(pColInfo, numOfRows, (const char *)&pStream->conf.watermark, false);

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
  SMnode *    pMnode = pReq->info.node;
  SSdb *      pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SStreamObj *pStream = NULL;

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_STREAM, pShow->pIter, (void **)&pStream);
    if (pShow->pIter == NULL) {
      break;
    }

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
        colDataSetVal(pColInfo, numOfRows, (const char *)&level, false);

        // status
        char   status[20 + VARSTR_HEADER_SIZE] = {0};
        int8_t taskStatus = atomic_load_8(&pTask->status.taskStatus);
        if (taskStatus == TASK_STATUS__NORMAL) {
          memcpy(varDataVal(status), "normal", 6);
          varDataSetLen(status, 6);
        } else if (taskStatus == TASK_STATUS__DROPPING) {
          memcpy(varDataVal(status), "dropping", 8);
          varDataSetLen(status, 8);
        } else if (taskStatus == TASK_STATUS__FAIL) {
          memcpy(varDataVal(status), "fail", 4);
          varDataSetLen(status, 4);
        } else if (taskStatus == TASK_STATUS__STOP) {
          memcpy(varDataVal(status), "stop", 4);
          varDataSetLen(status, 4);
        } else if (taskStatus == TASK_STATUS__SCAN_HISTORY) {
          memcpy(varDataVal(status), "history", 7);
          varDataSetLen(status, 7);
        } else if (taskStatus == TASK_STATUS__HALT) {
          memcpy(varDataVal(status), "halt", 4);
          varDataSetLen(status, 4);
        } else if (taskStatus == TASK_STATUS__PAUSE) {
          memcpy(varDataVal(status), "pause", 5);
          varDataSetLen(status, 5);
        }
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
    mError("failed to malloc in pause stream, size:%" PRIzu ", code:%s", sizeof(SVPauseStreamTaskReq),
           tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pReq->head.vgId = htonl(pTask->info.nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->streamId = pTask->id.streamId;

  STransAction action = {0};
  initTransAction(&action, pReq, sizeof(SVPauseStreamTaskReq), TDMT_STREAM_TASK_PAUSE, &pTask->info.epSet);
  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }
  return 0;
}

int32_t mndPauseAllStreamTaskImpl(STrans *pTrans, SArray *tasks) {
  int32_t size = taosArrayGetSize(tasks);
  for (int32_t i = 0; i < size; i++) {
    SArray *pTasks = taosArrayGetP(tasks, i);
    int32_t sz = taosArrayGetSize(pTasks);
    for (int32_t j = 0; j < sz; j++) {
      SStreamTask *pTask = taosArrayGetP(pTasks, j);
      if (mndPauseStreamTask(pTrans, pTask) < 0) {
        return -1;
      }

      if (atomic_load_8(&pTask->status.taskStatus) != TASK_STATUS__PAUSE) {
        atomic_store_8(&pTask->status.keepTaskStatus, pTask->status.taskStatus);
        atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__PAUSE);
      }
    }
  }
  return 0;
}

int32_t mndPauseAllStreamTasks(STrans *pTrans, SStreamObj *pStream) {
  int32_t code = mndPauseAllStreamTaskImpl(pTrans, pStream->tasks);
  if (code != 0) {
    return code;
  }
  // pStream->pHTasksList is null
  // code = mndPauseAllStreamTaskImpl(pTrans, pStream->pHTasksList);
  return code;
}

static int32_t mndPersistStreamLog(STrans *pTrans, const SStreamObj *pStream, int8_t status) {
  SStreamObj streamObj = {0};
  memcpy(streamObj.name, pStream->name, TSDB_STREAM_FNAME_LEN);
  streamObj.status = status;

  SSdbRaw *pCommitRaw = mndStreamActionEncode(&streamObj);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("stream trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
  return 0;
}

static int32_t mndProcessPauseStreamReq(SRpcMsg *pReq) {
  SMnode *    pMnode = pReq->info.node;
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
      return 0;
    } else {
      terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
      return -1;
    }
  }

  if (pStream->status == STREAM_STATUS__PAUSE) {
    sdbRelease(pMnode->pSdb, pStream);
    return 0;
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
  if (mndTransCheckConflict(pMnode, pTrans) != 0) {
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
  pReq->head.vgId = htonl(pTask->info.nodeId);
  pReq->taskId = pTask->id.taskId;
  pReq->streamId = pTask->id.streamId;
  pReq->igUntreated = igUntreated;

  STransAction action = {0};
  initTransAction(&action, pReq, sizeof(SVResumeStreamTaskReq), TDMT_STREAM_TASK_RESUME, &pTask->info.epSet);
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
      if (mndResumeStreamTask(pTrans, pTask, igUntreated) < 0) {
        return -1;
      }

      if (atomic_load_8(&pTask->status.taskStatus) == TASK_STATUS__PAUSE) {
        atomic_store_8(&pTask->status.taskStatus, pTask->status.keepTaskStatus);
      }
    }
  }
  // pStream->pHTasksList is null
  return 0;
}

static int32_t mndProcessResumeStreamReq(SRpcMsg *pReq) {
  SMnode *    pMnode = pReq->info.node;
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

  if (pStream->status != STREAM_STATUS__PAUSE) {
    sdbRelease(pMnode->pSdb, pStream);
    return 0;
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
  if (mndTransCheckConflict(pMnode, pTrans) != 0) {
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

static void initNodeUpdateMsg(SStreamTaskNodeUpdateMsg *pMsg, const SVgroupChangeInfo *pInfo, int64_t streamId,
                              int32_t taskId) {
  pMsg->streamId = streamId;
  pMsg->taskId = taskId;
  pMsg->pNodeList = taosArrayInit(taosArrayGetSize(pInfo->pUpdateNodeList), sizeof(SNodeUpdateInfo));
  taosArrayAddAll(pMsg->pNodeList, pInfo->pUpdateNodeList);
}

static int32_t doBuildStreamTaskUpdateMsg(void **pBuf, int32_t *pLen, SVgroupChangeInfo *pInfo, int32_t nodeId,
                                          int64_t streamId, int32_t taskId) {
  SStreamTaskNodeUpdateMsg req = {0};
  initNodeUpdateMsg(&req, pInfo, streamId, taskId);

  int32_t code = 0;
  int32_t blen;

  tEncodeSize(tEncodeStreamTaskUpdateMsg, &req, blen, code);
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

  void *   abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  tEncodeStreamTaskUpdateMsg(&encoder, &req);

  SMsgHead *pMsgHead = (SMsgHead *)buf;
  pMsgHead->contLen = htonl(tlen);
  pMsgHead->vgId = htonl(nodeId);

  tEncoderClear(&encoder);

  *pBuf = buf;
  *pLen = tlen;

  return TSDB_CODE_SUCCESS;
}

int32_t mndPersistTransLog(SStreamObj *pStream, STrans *pTrans) {
  SSdbRaw *pCommitRaw = mndStreamActionEncode(pStream);
  if (pCommitRaw == NULL) {
    mError("failed to encode stream since %s", terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("stream trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    sdbFreeRaw(pCommitRaw);
    mndTransDrop(pTrans);
    return -1;
  }

  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) {
    mError("stream trans:%d failed to set raw status since %s", pTrans->id, terrstr());
    sdbFreeRaw(pCommitRaw);
    mndTransDrop(pTrans);
    return -1;
  }

  return 0;
}

void initTransAction(STransAction *pAction, void *pCont, int32_t contLen, int32_t msgType, const SEpSet *pEpset) {
  pAction->epSet = *pEpset;
  pAction->contLen = contLen;
  pAction->pCont = pCont;
  pAction->msgType = msgType;
}

// todo extract method: traverse stream tasks
// build trans to update the epset
static int32_t createStreamUpdateTrans(SMnode *pMnode, SStreamObj *pStream, SVgroupChangeInfo *pInfo) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, NULL, "stream-task-update");
  if (pTrans == NULL) {
    mError("failed to build stream task DAG update, reason: %s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return -1;
  }

  mDebug("start to build stream:0x%" PRIx64 " task DAG update", pStream->uid);

  mndTransSetDbName(pTrans, pStream->sourceDb, pStream->targetDb);
  if (mndTransCheckConflict(pMnode, pTrans) != 0) {
    mError("failed to build stream:0x%" PRIx64 " task DAG update, code:%s", pStream->uid,
           tstrerror(TSDB_CODE_MND_TRANS_CONFLICT));
    mndTransDrop(pTrans);
    return -1;
  }

  taosWLockLatch(&pStream->lock);
  int32_t numOfLevels = taosArrayGetSize(pStream->tasks);

  for (int32_t j = 0; j < numOfLevels; ++j) {
    SArray *pLevel = taosArrayGetP(pStream->tasks, j);

    int32_t numOfTasks = taosArrayGetSize(pLevel);
    for (int32_t k = 0; k < numOfTasks; ++k) {
      SStreamTask *pTask = taosArrayGetP(pLevel, k);

      void *  pBuf = NULL;
      int32_t len = 0;
      streamTaskUpdateEpsetInfo(pTask, pInfo->pUpdateNodeList);
      doBuildStreamTaskUpdateMsg(&pBuf, &len, pInfo, pTask->info.nodeId, pTask->id.streamId, pTask->id.taskId);

      STransAction action = {0};
      initTransAction(&action, pBuf, len, TDMT_VND_STREAM_TASK_UPDATE, &pTask->info.epSet);
      if (mndTransAppendRedoAction(pTrans, &action) != 0) {
        taosMemoryFree(pBuf);
        taosWUnLockLatch(&pStream->lock);
        mndTransDrop(pTrans);
        return -1;
      }
    }
  }

  taosWUnLockLatch(&pStream->lock);

  int32_t code = mndPersistTransLog(pStream, pTrans);
  if (code != TSDB_CODE_SUCCESS) {
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare update stream trans since %s", pTrans->id, terrstr());
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

  for (int32_t i = 0; i < pCurrent->numOfEps; ++i) {
    const SEp *p = &(pCurrent->eps[i]);
    if (pEp->port == p->port && strncmp(pEp->fqdn, p->fqdn, TSDB_FQDN_LEN) == 0) {
      return false;
    }
  }

  return true;
}

// 1. increase the replica does not affect the stream process.
// 2. decreasing the replica may affect the stream task execution in the way that there is one or more running stream
// tasks on the will be removed replica.
// 3. vgroup redistribution is an combination operation of first increase replica and then decrease replica. So we will
// handle it as mentioned in 1 & 2 items.
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
        if (isNodeEpsetChanged(&pPrevEntry->epset, &pCurrent->epset)) {
          const SEp *pPrevEp = GET_ACTIVE_EP(&pPrevEntry->epset);

          char buf[256] = {0};
          EPSET_TO_STR(&pCurrent->epset, buf);
          mDebug("nodeId:%d epset changed detected, old:%s:%d -> new:%s", pCurrent->nodeId, pPrevEp->fqdn,
                 pPrevEp->port, buf);

          SNodeUpdateInfo updateInfo = {.nodeId = pPrevEntry->nodeId};
          epsetAssign(&updateInfo.prevEp, &pPrevEntry->epset);
          epsetAssign(&updateInfo.newEp, &pCurrent->epset);
          taosArrayPush(info.pUpdateNodeList, &updateInfo);

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

static SArray *mndTakeVgroupSnapshot(SMnode *pMnode) {
  SSdb *  pSdb = pMnode->pSdb;
  void *  pIter = NULL;
  SVgObj *pVgroup = NULL;

  SArray *pVgroupListSnapshot = taosArrayInit(4, sizeof(SNodeEntry));

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) {
      break;
    }

    SNodeEntry entry = {0};
    entry.epset = mndGetVgroupEpset(pMnode, pVgroup);
    entry.nodeId = pVgroup->vgId;
    entry.hbTimestamp = -1;

    taosArrayPush(pVgroupListSnapshot, &entry);
    sdbRelease(pSdb, pVgroup);
  }

  return pVgroupListSnapshot;
}

static int32_t mndProcessVgroupChange(SMnode *pMnode, SVgroupChangeInfo *pChangeInfo) {
  SSdb *pSdb = pMnode->pSdb;

  // check all streams that involved this vnode should update the epset info
  SStreamObj *pStream = NULL;
  void *      pIter = NULL;
  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) {
      break;
    }

    void *p = taosHashGet(pChangeInfo->pDBMap, pStream->targetDb, strlen(pStream->targetDb));
    void *p1 = taosHashGet(pChangeInfo->pDBMap, pStream->sourceDb, strlen(pStream->sourceDb));
    if (p == NULL && p1 == NULL) {
      mndReleaseStream(pMnode, pStream);
      continue;
    }

    mDebug("stream:0x%" PRIx64 " involved node changed, create update trans", pStream->uid);
    int32_t code = createStreamUpdateTrans(pMnode, pStream, pChangeInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return 0;
}

static SArray *doExtractNodeListFromStream(SMnode *pMnode) {
  SSdb *      pSdb = pMnode->pSdb;
  SStreamObj *pStream = NULL;
  void *      pIter = NULL;

  SHashObj *pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) {
      break;
    }

    taosWLockLatch(&pStream->lock);
    int32_t numOfLevels = taosArrayGetSize(pStream->tasks);

    for (int32_t j = 0; j < numOfLevels; ++j) {
      SArray *pLevel = taosArrayGetP(pStream->tasks, j);

      int32_t numOfTasks = taosArrayGetSize(pLevel);
      for (int32_t k = 0; k < numOfTasks; ++k) {
        SStreamTask *pTask = taosArrayGetP(pLevel, k);
        SNodeEntry   entry = {0};
        epsetAssign(&entry.epset, &pTask->info.epSet);
        entry.nodeId = pTask->info.nodeId;
        entry.hbTimestamp = -1;

        taosHashPut(pHash, &entry.nodeId, sizeof(entry.nodeId), &entry, sizeof(entry));
      }
    }

    taosWUnLockLatch(&pStream->lock);
    sdbRelease(pSdb, pStream);
  }

  SArray *plist = taosArrayInit(taosHashGetSize(pHash), sizeof(SNodeEntry));

  // convert to list
  pIter = NULL;
  while ((pIter = taosHashIterate(pHash, pIter)) != NULL) {
    SNodeEntry *pEntry = (SNodeEntry *)pIter;
    taosArrayPush(plist, pEntry);
  }
  taosHashCleanup(pHash);

  return plist;
}

static void doExtractTasksFromStream(SMnode *pMnode) {
  SSdb *      pSdb = pMnode->pSdb;
  SStreamObj *pStream = NULL;
  void *      pIter = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) {
      break;
    }

    keepStreamTasksInBuf(pStream, &execNodeList);
    sdbRelease(pSdb, pStream);
  }
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
  if (execNodeList.pNodeEntryList == NULL || (taosArrayGetSize(execNodeList.pNodeEntryList) == 0)) {
    if (execNodeList.pNodeEntryList != NULL) {
      execNodeList.pNodeEntryList = taosArrayDestroy(execNodeList.pNodeEntryList);
    }

    execNodeList.pNodeEntryList = doExtractNodeListFromStream(pMnode);
  }

  if (taosArrayGetSize(execNodeList.pNodeEntryList) == 0) {
    mDebug("end to do stream task node change checking, no vgroup exists, do nothing");
    execNodeList.ts = ts;
    atomic_store_32(&mndNodeCheckSentinel, 0);
    return 0;
  }

  SArray *pNodeSnapshot = mndTakeVgroupSnapshot(pMnode);

  SVgroupChangeInfo changeInfo = mndFindChangedNodeInfo(pMnode, execNodeList.pNodeEntryList, pNodeSnapshot);
  if (taosArrayGetSize(changeInfo.pUpdateNodeList) > 0) {
    code = mndProcessVgroupChange(pMnode, &changeInfo);
  }

  taosArrayDestroy(changeInfo.pUpdateNodeList);
  taosHashCleanup(changeInfo.pDBMap);

  // keep the new vnode snapshot
  if (code == TSDB_CODE_SUCCESS || code == TSDB_CODE_ACTION_IN_PROGRESS) {
    taosArrayDestroy(execNodeList.pNodeEntryList);
    execNodeList.pNodeEntryList = pNodeSnapshot;
    execNodeList.ts = ts;
  }

  mDebug("end to do stream task node change checking");
  atomic_store_32(&mndNodeCheckSentinel, 0);
  return 0;
}

typedef struct SMStreamNodeCheckMsg {
  int8_t holder;  // // to fix windows compile error, define place holder
} SMStreamNodeCheckMsg;

static int32_t mndProcessNodeCheck(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  SSdb *  pSdb = pMnode->pSdb;
  if (sdbGetSize(pSdb, SDB_STREAM) <= 0) {
    return 0;
  }

  SMStreamNodeCheckMsg *pMsg = rpcMallocCont(sizeof(SMStreamNodeCheckMsg));
  SRpcMsg               rpcMsg = {
      .msgType = TDMT_MND_STREAM_NODECHANGE_CHECK, .pCont = pMsg, .contLen = sizeof(SMStreamNodeCheckMsg)};
  tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  return 0;
}

static void keepStreamTasksInBuf(SStreamObj *pStream, SStreamVnodeRevertIndex *pExecNode) {
  int32_t level = taosArrayGetSize(pStream->tasks);
  for (int32_t i = 0; i < level; i++) {
    SArray *pLevel = taosArrayGetP(pStream->tasks, i);

    int32_t numOfTasks = taosArrayGetSize(pLevel);
    for (int32_t j = 0; j < numOfTasks; j++) {
      SStreamTask *pTask = taosArrayGetP(pLevel, j);
      int64_t      keys[2] = {pTask->id.streamId, pTask->id.taskId};

      void *p = taosHashGet(pExecNode->pTaskMap, keys, sizeof(keys));
      if (p == NULL) {
        STaskStatusEntry entry = {
            .streamId = pTask->id.streamId, .taskId = pTask->id.taskId, .status = TASK_STATUS__STOP};
        taosArrayPush(pExecNode->pTaskList, &entry);

        int32_t ordinal = taosArrayGetSize(pExecNode->pTaskList) - 1;
        taosHashPut(pExecNode->pTaskMap, keys, sizeof(keys), &ordinal, sizeof(ordinal));
      }
    }
  }
}

// todo: this process should be executed by the write queue worker of the mnode
int32_t mndProcessStreamHb(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;

  SStreamHbMsg req = {0};
  int32_t      code = TSDB_CODE_SUCCESS;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, pReq->pCont, pReq->contLen);

  if (tDecodeStreamHbMsg(&decoder, &req) < 0) {
    tDecoderClear(&decoder);
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }
  tDecoderClear(&decoder);

  //  int64_t now = taosGetTimestampSec();
  mTrace("receive stream-meta hb from vgId:%d, active numOfTasks:%d", req.vgId, req.numOfTasks);

  taosThreadMutexLock(&execNodeList.lock);
  int32_t numOfExisted = taosHashGetSize(execNodeList.pTaskMap);
  if (numOfExisted == 0) {
    doExtractTasksFromStream(pMnode);
  }

  for (int32_t i = 0; i < req.numOfTasks; ++i) {
    STaskStatusEntry *p = taosArrayGet(req.pTaskStatus, i);
    int64_t           k[2] = {p->streamId, p->taskId};

    int32_t *index = taosHashGet(execNodeList.pTaskMap, &k, sizeof(k));
    if (index == NULL) {
      continue;
    }

    STaskStatusEntry *pStatusEntry = taosArrayGet(execNodeList.pTaskList, *index);
    pStatusEntry->status = p->status;
    if (p->status != TASK_STATUS__NORMAL) {
      mDebug("received s-task:0x%x not in ready status:%s", p->taskId, streamGetTaskStatusStr(p->status));
    }
  }
  taosThreadMutexUnlock(&execNodeList.lock);

  taosArrayDestroy(req.pTaskStatus);

  //  bool nodeChanged = false;
  //  SArray* pList = taosArrayInit(4, sizeof(int32_t));
  /*
    // record the timeout node
    for(int32_t i = 0; i < taosArrayGetSize(execNodeList.pNodeEntryList); ++i) {
      SNodeEntry* pEntry = taosArrayGet(execNodeList.pNodeEntryList, i);
      int64_t duration = now - pEntry->hbTimestamp;
      if (duration > MND_STREAM_HB_INTERVAL) { // execNode timeout, try next
        taosArrayPush(pList, &pEntry);
        mWarn("nodeId:%d stream node timeout, since last hb:%"PRId64"s", pEntry->nodeId, duration);
        continue;
      }

      if (pEntry->nodeId != req.vgId) {
        continue;
      }

      pEntry->hbTimestamp = now;

      // check epset to identify whether the node has been transferred to other dnodes.
      // node the epset is changed, which means the node transfer has occurred for this node.
  //    if (!isEpsetEqual(&pEntry->epset, &req.epset)) {
  //      nodeChanged = true;
  //      break;
  //    }
    }

    // todo handle the node timeout case. Once the vnode is off-line, we should check the dnode status from mnode,
    // to identify whether the dnode is truely offline or not.

    // handle the node changed case
    if (!nodeChanged) {
      return TSDB_CODE_SUCCESS;
    }

    int32_t nodeId = req.vgId;

    {// check all streams that involved this vnode should update the epset info
      SStreamObj *pStream = NULL;
      void       *pIter = NULL;
      while (1) {
        pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
        if (pIter == NULL) {
          break;
        }

        // update the related upstream and downstream tasks, todo remove this, no need this function
        taosWLockLatch(&pStream->lock);
  //      streamTaskUpdateEpInfo(pStream->tasks, req.vgId, &req.epset);
  //      streamTaskUpdateEpInfo(pStream->pHTasksList, req.vgId, &req.epset);
        taosWUnLockLatch(&pStream->lock);

  //      code = createStreamUpdateTrans(pMnode, pStream, nodeId, );
  //      if (code != TSDB_CODE_SUCCESS) {
  //         todo
  ////      }
  //    }
    }
  */
  return TSDB_CODE_SUCCESS;
}
