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

static int32_t mndProcessCreateStreamReqFromMNode(SRpcMsg *pReq);
static int32_t mndProcessDropStreamReqFromMNode(SRpcMsg *pReq);

static int32_t mndProcessStreamCheckpoint(SRpcMsg *pReq);
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
static int32_t refreshNodeListFromExistedStreams(SMnode *pMnode, SArray *pNodeList);
static int32_t mndProcessStreamReqCheckpoint(SRpcMsg *pReq);
static int32_t mndProcessCheckpointReport(SRpcMsg *pReq);
static int32_t mndProcessConsensusInTmr(SRpcMsg *pMsg);
static void    doSendQuickRsp(SRpcHandleInfo *pInfo, int32_t msgSize, int32_t vgId, int32_t code);


static int32_t setStreamAttrInResBlock(SStreamObj *pStream, SSDataBlock *pBlock, int32_t numOfRows);

static SVgroupChangeInfo mndFindChangedNodeInfo(SMnode *pMnode, const SArray *pPrevNodeList, const SArray *pNodeList);

static void     addAllStreamTasksIntoBuf(SMnode *pMnode, SStreamExecInfo *pExecInfo);
static void     removeExpiredNodeInfo(const SArray *pNodeSnapshot);
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
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_UPDATE_CHKPT_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_STREAM_CONSEN_CHKPT_RSP, mndTransProcessRsp);

  // for msgs inside mnode
  // TODO change the name
  mndSetMsgHandle(pMnode, TDMT_STREAM_CREATE, mndProcessCreateStreamReqFromMNode);
  mndSetMsgHandle(pMnode, TDMT_STREAM_CREATE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_STREAM_DROP, mndProcessDropStreamReqFromMNode);
  mndSetMsgHandle(pMnode, TDMT_STREAM_DROP_RSP, mndTransProcessRsp);

  mndSetMsgHandle(pMnode, TDMT_VND_STREAM_CHECK_POINT_SOURCE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_BEGIN_CHECKPOINT, mndProcessStreamCheckpoint);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_REQ_CHKPT, mndProcessStreamReqCheckpoint);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_CHKPT_REPORT, mndProcessCheckpointReport);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_UPDATE_CHKPT_EVT, mndScanCheckpointReportInfo);
  mndSetMsgHandle(pMnode, TDMT_STREAM_TASK_REPORT_CHECKPOINT, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_HEARTBEAT, mndProcessStreamHb);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_NODECHANGE_CHECK, mndProcessNodeCheckReq);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_CONSEN_TIMER, mndProcessConsensusInTmr);

  mndSetMsgHandle(pMnode, TDMT_MND_PAUSE_STREAM, mndProcessPauseStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_RESUME_STREAM, mndProcessResumeStreamReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndRetrieveStream);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndCancelGetNextStream);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAM_TASKS, mndRetrieveStreamTask);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAM_TASKS, mndCancelGetNextStreamTask);

  int32_t code = mndInitExecInfo();
  if (code) {
    return code;
  }

  code = sdbSetTable(pMnode->pSdb, table);
  if (code) {
    return terrno;
  }

  code = sdbSetTable(pMnode->pSdb, tableSeq);
  return code;
}

void mndCleanupStream(SMnode *pMnode) {
  taosArrayDestroy(execInfo.pTaskList);
  taosArrayDestroy(execInfo.pNodeList);
  taosHashCleanup(execInfo.pTaskMap);
  taosHashCleanup(execInfo.transMgmt.pDBTrans);
  taosHashCleanup(execInfo.pTransferStateStreams);
  taosHashCleanup(execInfo.pChkptStreams);
  taosHashCleanup(execInfo.pStreamConsensus);
  (void) taosThreadMutexDestroy(&execInfo.lock);
  mDebug("mnd stream exec info cleanup");
}

SSdbRow *mndStreamActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;
  int32_t lino = 0;
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
  (void) atomic_exchange_32(&pOldStream->version, pNewStream->version);

  taosWLockLatch(&pOldStream->lock);

  pOldStream->status = pNewStream->status;
  pOldStream->updateTime = pNewStream->updateTime;
  pOldStream->checkpointId = pNewStream->checkpointId;
  pOldStream->checkpointFreq = pNewStream->checkpointFreq;

  taosWUnLockLatch(&pOldStream->lock);
  return 0;
}

int32_t mndAcquireStream(SMnode *pMnode, char *streamName, SStreamObj **pStream) {
  int32_t code = 0;
  SSdb *pSdb = pMnode->pSdb;
  (*pStream) = sdbAcquire(pSdb, SDB_STREAM, streamName);
  if ((*pStream) == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    code = TSDB_CODE_MND_STREAM_NOT_EXIST;
  }
  return code;
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
    return terrno = TSDB_CODE_MND_INVALID_STREAM_OPTION;
  }
  return 0;
}

static int32_t createSchemaByFields(const SArray *pFields, SSchemaWrapper *pWrapper) {
  pWrapper->nCols = taosArrayGetSize(pFields);
  pWrapper->pSchema = taosMemoryCalloc(pWrapper->nCols, sizeof(SSchema));
  if (NULL == pWrapper->pSchema) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SNode  *pNode;
  int32_t index = 0;
  for (int32_t i = 0; i < pWrapper->nCols; i++) {
    SField *pField = (SField *)taosArrayGet(pFields, i);
    if (TSDB_DATA_TYPE_NULL == pField->type) {
      pWrapper->pSchema[index].type = TSDB_DATA_TYPE_VARCHAR;
      pWrapper->pSchema[index].bytes = VARSTR_HEADER_SIZE;
    } else {
      pWrapper->pSchema[index].type = pField->type;
      pWrapper->pSchema[index].bytes = pField->bytes;
    }
    pWrapper->pSchema[index].colId = index + 1;
    strcpy(pWrapper->pSchema[index].name, pField->name);
    pWrapper->pSchema[index].flags = pField->flags;
    index += 1;
  }

  return TSDB_CODE_SUCCESS;
}

static bool hasDestPrimaryKey(SSchemaWrapper *pWrapper) {
  if (pWrapper->nCols < 2) {
    return false;
  }
  for (int32_t i = 1; i < pWrapper->nCols; i++) {
    if (pWrapper->pSchema[i].flags & COL_IS_KEY) {
      return true;
    }
  }
  return false;
}

static int32_t mndBuildStreamObjFromCreateReq(SMnode *pMnode, SStreamObj *pObj, SCMCreateStreamReq *pCreate) {
  SNode      *pAst = NULL;
  SQueryPlan *pPlan = NULL;

  mInfo("stream:%s to create", pCreate->name);
  memcpy(pObj->name, pCreate->name, TSDB_STREAM_FNAME_LEN);
  pObj->createTime = taosGetTimestampMs();
  pObj->updateTime = pObj->createTime;
  pObj->version = 1;
  if (pCreate->smaId > 0) {
    pObj->subTableWithoutMd5 = 1;
  }
  pObj->smaId = pCreate->smaId;
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
    return terrno;
  }
  pObj->sourceDbUid = pSourceDb->uid;
  mndReleaseDb(pMnode, pSourceDb);

  memcpy(pObj->targetSTbName, pCreate->targetStbFullName, TSDB_TABLE_FNAME_LEN);

  SDbObj *pTargetDb = mndAcquireDbByStb(pMnode, pObj->targetSTbName);
  if (pTargetDb == NULL) {
    mInfo("stream:%s failed to create, target db %s not exist since %s", pCreate->name, pObj->targetDb, terrstr());
    return terrno;
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

  // create output schema
  if (createSchemaByFields(pCreate->pCols, &pObj->outputSchema) != TSDB_CODE_SUCCESS) {
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

  bool         hasKey = hasDestPrimaryKey(&pObj->outputSchema);
  SPlanContext cxt = {
      .pAstRoot = pAst,
      .topicQuery = false,
      .streamQuery = true,
      .triggerType = pObj->conf.trigger == STREAM_TRIGGER_MAX_DELAY ? STREAM_TRIGGER_WINDOW_CLOSE : pObj->conf.trigger,
      .watermark = pObj->conf.watermark,
      .igExpired = pObj->conf.igExpired,
      .deleteMark = pObj->deleteMark,
      .igCheckUpdate = pObj->igCheckUpdate,
      .destHasPrimaryKey = hasKey,
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

  if (pTask->ver < SSTREAM_TASK_SUBTABLE_CHANGED_VER) {
    pTask->ver = SSTREAM_TASK_VER;
  }

  int32_t code = tEncodeStreamTask(&encoder, pTask);
  if (code == -1) {
    return TSDB_CODE_INVALID_MSG;
  }

  int32_t size = encoder.pos;
  int32_t tlen = sizeof(SMsgHead) + size;
  tEncoderClear(&encoder);

  void *buf = taosMemoryCalloc(1, tlen);
  if (buf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  ((SMsgHead *)buf)->vgId = htonl(pTask->info.nodeId);

  void *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncoderInit(&encoder, abuf, size);
  code = tEncodeStreamTask(&encoder, pTask);
  tEncoderClear(&encoder);

  if (code == -1) {
    code = TSDB_CODE_INVALID_PARA;
    mError("failed to encode stream task, code:%s", tstrerror(code));
    taosMemoryFree(buf);
    return code;
  }

  code = setTransAction(pTrans, buf, tlen, TDMT_STREAM_TASK_DEPLOY, &pTask->info.epSet, 0, 0);
  if (code) {
    taosMemoryFree(buf);
  }

  return code;
}

int32_t mndPersistStreamTasks(STrans *pTrans, SStreamObj *pStream) {
  SStreamTaskIter *pIter = NULL;
  int32_t code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    mError("failed to create task iter for stream:%s", pStream->name);
    return code;
  }

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      return code;
    }

    code = mndPersistTaskDeployReq(pTrans, pTask);
    if (code) {
      destroyStreamTaskIter(pIter);
      return code;
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
        code = mndPersistTaskDeployReq(pTrans, pTask);
        if (code) {
          return code;
        }
      }
    }
  }

  return code;
}

int32_t mndPersistStream(STrans *pTrans, SStreamObj *pStream) {
  int32_t code = 0;
  if ((code = mndPersistStreamTasks(pTrans, pStream)) < 0) {
    return code;
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
  createReq.pColumns = taosArrayInit_s(sizeof(SFieldWithOptions), createReq.numOfColumns);

  // build fields
  for (int32_t i = 0; i < createReq.numOfColumns; i++) {
    SFieldWithOptions *pField = taosArrayGet(createReq.pColumns, i);
    tstrncpy(pField->name, pStream->outputSchema.pSchema[i].name, TSDB_COL_NAME_LEN);
    pField->flags = pStream->outputSchema.pSchema[i].flags;
    pField->type = pStream->outputSchema.pSchema[i].type;
    pField->bytes = pStream->outputSchema.pSchema[i].bytes;
    pField->compress = createDefaultColCmprByType(pField->type);
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
  mDebug("stream:%s create dst stable:%s, cols:%d", pStream->name, pStream->targetSTbName, pStream->outputSchema.nCols);
  return 0;

_OVER:
  tFreeSMCreateStbReq(&createReq);
  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);

  mDebug("stream:%s failed to create dst stable:%s, code:%s", pStream->name, pStream->targetSTbName, tstrerror(terrno));
  return -1;
}

// 1. stream number check
// 2. target stable can not be target table of other existed streams.
static int32_t doStreamCheck(SMnode *pMnode, SStreamObj *pStreamObj) {
  int32_t     numOfStream = 0;
  SStreamObj *pStream = NULL;
  void       *pIter = NULL;

  while ((pIter = sdbFetch(pMnode->pSdb, SDB_STREAM, pIter, (void **)&pStream)) != NULL) {
    if (pStream->sourceDbUid == pStreamObj->sourceDbUid) {
      ++numOfStream;
    }

    sdbRelease(pMnode->pSdb, pStream);

    if (numOfStream > MND_STREAM_MAX_NUM) {
      mError("too many streams, no more than %d for each database, failed to create stream:%s", MND_STREAM_MAX_NUM,
             pStreamObj->name);
      sdbCancelFetch(pMnode->pSdb, pIter);
      terrno = TSDB_CODE_MND_TOO_MANY_STREAMS;
      return terrno;
    }

    if (pStream->targetStbUid == pStreamObj->targetStbUid) {
      mError("Cannot write the same stable as other stream:%s, failed to create stream:%s", pStream->name,
             pStreamObj->name);
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
  const char *pMsg = "create stream tasks on dnodes";
  int32_t code = 0;
  terrno = TSDB_CODE_SUCCESS;

  SCMCreateStreamReq createReq = {0};
  if (tDeserializeSCMCreateStreamReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

#ifdef WINDOWS
  terrno = TSDB_CODE_MND_INVALID_PLATFORM;
  goto _OVER;
#endif

  mInfo("stream:%s, start to create stream, sql:%s", createReq.name, createReq.sql);
  if ((code = mndCheckCreateStreamReq(&createReq)) != 0) {
    mError("stream:%s, failed to create since %s", createReq.name, terrstr());
    goto _OVER;
  }

  code = mndAcquireStream(pMnode, createReq.name, &pStream);
  if (pStream != NULL && code == 0) {
    if (createReq.igExists) {
      mInfo("stream:%s, already exist, ignore exist is set", createReq.name);
      goto _OVER;
    } else {
      code = TSDB_CODE_MND_STREAM_ALREADY_EXIST;
      goto _OVER;
    }
  } else if (code != TSDB_CODE_MND_STREAM_NOT_EXIST) {
    goto _OVER;
  }

  if ((code = grantCheck(TSDB_GRANT_STREAMS)) < 0) {
    goto _OVER;
  }

  if (createReq.sql != NULL) {
    sqlLen = strlen(createReq.sql);
    sql = taosMemoryMalloc(sqlLen + 1);
    memset(sql, 0, sqlLen + 1);
    memcpy(sql, createReq.sql, sqlLen);
  }

  // build stream obj from request
  if ((code = mndBuildStreamObjFromCreateReq(pMnode, &streamObj, &createReq)) < 0) {
    mError("stream:%s, failed to create since %s", createReq.name, terrstr());
    goto _OVER;
  }

  if ((code = doStreamCheck(pMnode, &streamObj)) < 0) {
    goto _OVER;
  }

  STrans *pTrans = NULL;
  code = doCreateTrans(pMnode, &streamObj, pReq, TRN_CONFLICT_DB, MND_STREAM_CREATE_NAME, pMsg, &pTrans);
  if (pTrans == NULL || code) {
    goto _OVER;
  }

  // create stb for stream
  if (createReq.createStb == STREAM_CREATE_STABLE_TRUE) {
    if ((code = mndCreateStbForStream(pMnode, pTrans, &streamObj, pReq->info.conn.user)) < 0) {
      mError("trans:%d, failed to create stb for stream %s since %s", pTrans->id, createReq.name, terrstr());
      mndTransDrop(pTrans);
      goto _OVER;
    }
  } else {
    mDebug("stream:%s no need create stable", createReq.name);
  }

  // schedule stream task for stream obj
  code = mndScheduleStream(pMnode, &streamObj, createReq.lastTs, createReq.pVgroupVerList);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stream:%s, failed to schedule since %s", createReq.name, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }

  // add stream to trans
  code = mndPersistStream(pTrans, &streamObj);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stream:%s, failed to persist since %s", createReq.name, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }

  if ((code = mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_READ_DB, streamObj.sourceDb)) != 0) {
    mndTransDrop(pTrans);
    goto _OVER;
  }

  if ((code = mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, streamObj.targetDb)) != 0) {
    mndTransDrop(pTrans);
    goto _OVER;
  }

  // add into buffer firstly
  // to make sure when the hb from vnode arrived, the newly created tasks have been in the task map already.
  streamMutexLock(&execInfo.lock);
  mDebug("stream stream:%s start to register tasks into task nodeList and set initial checkpointId", createReq.name);
  saveTaskAndNodeInfoIntoBuf(&streamObj, &execInfo);
  streamMutexUnlock(&execInfo.lock);

  // execute creation
  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }

  mndTransDrop(pTrans);

  SName dbname = {0};
  code = tNameFromString(&dbname, createReq.sourceDB, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (code) {
    mError("invalid source dbname:%s in create stream, code:%s", createReq.sourceDB, tstrerror(code));
    goto _OVER;
  }

  SName name = {0};
  code = tNameFromString(&name, createReq.name, T_NAME_ACCT | T_NAME_TABLE);
  if (code) {
    mError("invalid stream name:%s in create strem, code:%s", createReq.name, tstrerror(code));
    goto _OVER;
  }

  // reuse this function for stream
  if (sql != NULL && sqlLen > 0) {
    auditRecord(pReq, pMnode->clusterId, "createStream", dbname.dbname, name.dbname, sql, sqlLen);
  } else {
    char detail[1000] = {0};
    snprintf(detail, tListLen(detail), "dbname:%s, stream name:%s", dbname.dbname, name.dbname);
    auditRecord(pReq, pMnode->clusterId, "createStream", dbname.dbname, name.dbname, detail, strlen(detail));
  }

_OVER:
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stream:%s, failed to create since %s", createReq.name, terrstr());
  } else {
    mDebug("stream:%s create stream completed", createReq.name);
  }

  mndReleaseStream(pMnode, pStream);
  tFreeSCMCreateStreamReq(&createReq);
  tFreeStreamObj(&streamObj);

  if (sql != NULL) {
    taosMemoryFreeClear(sql);
  }

  return code;
}

int64_t mndStreamGenChkptId(SMnode *pMnode, bool lock) {
  SStreamObj *pStream = NULL;
  void       *pIter = NULL;
  SSdb       *pSdb = pMnode->pSdb;
  int64_t     maxChkptId = 0;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

    maxChkptId = TMAX(maxChkptId, pStream->checkpointId);
    mDebug("stream:%p, %s id:0x%" PRIx64 " checkpoint %" PRId64 "", pStream, pStream->name, pStream->uid,
           pStream->checkpointId);
    sdbRelease(pSdb, pStream);
  }

  {  // check the max checkpoint id from all vnodes.
    int64_t maxCheckpointId = -1;
    if (lock) {
      streamMutexLock(&execInfo.lock);
    }

    for (int32_t i = 0; i < taosArrayGetSize(execInfo.pTaskList); ++i) {
      STaskId *p = taosArrayGet(execInfo.pTaskList, i);

      STaskStatusEntry *pEntry = taosHashGet(execInfo.pTaskMap, p, sizeof(*p));
      if (pEntry == NULL) {
        continue;
      }

      if (pEntry->checkpointInfo.failed) {
        continue;
      }

      if (maxCheckpointId < pEntry->checkpointInfo.latestId) {
        maxCheckpointId = pEntry->checkpointInfo.latestId;
      }
    }

    if (lock) {
      streamMutexUnlock(&execInfo.lock);
    }

    if (maxCheckpointId > maxChkptId) {
      mDebug("max checkpointId in mnode:%" PRId64 ", smaller than max checkpointId in vnode:%" PRId64, maxChkptId,
             maxCheckpointId);
      maxChkptId = maxCheckpointId;
    }
  }

  mDebug("generate new checkpointId:%" PRId64, maxChkptId + 1);
  return maxChkptId + 1;
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
  int32_t pos = tEncodeStreamCheckpointSourceReq(&encoder, &req);
  if (pos == -1) {
    return TSDB_CODE_INVALID_MSG;
  }

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

  code = setTransAction(pTrans, buf, tlen, TDMT_VND_STREAM_CHECK_POINT_SOURCE, &epset, TSDB_CODE_SYN_PROPOSE_NOT_READY,
                        TSDB_CODE_VND_INVALID_VGROUP_ID);
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
    return TSDB_CODE_SUCCESS;
  }

  bool conflict = mndStreamTransConflictCheck(pMnode, pStream->uid, MND_STREAM_CHECKPOINT_NAME, lock);
  if (conflict) {
    mWarn("checkpoint conflict with other trans in %s, ignore the checkpoint for stream:%s %" PRIx64, pStream->sourceDb,
          pStream->name, pStream->uid);
    return TSDB_CODE_MND_TRANS_CONFLICT;
  }

  STrans *pTrans = NULL;
  code = doCreateTrans(pMnode, pStream, NULL, TRN_CONFLICT_NOTHING, MND_STREAM_CHECKPOINT_NAME,
                                 "gen checkpoint for stream", &pTrans);
  if (pTrans == NULL || code) {
    code = TSDB_CODE_MND_TRANS_CONFLICT;
    mError("failed to checkpoint of stream name%s, checkpointId: %" PRId64 ", reason:%s", pStream->name, checkpointId,
           tstrerror(code));
    goto _ERR;
  }

  code = mndStreamRegisterTrans(pTrans, MND_STREAM_CHECKPOINT_NAME, pStream->uid);
  if (code) {
    mError("failed to register checkpoint trans for stream:%s, checkpointId:%" PRId64, pStream->name, checkpointId);
    goto _ERR;
  }

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
    goto _ERR;
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != TSDB_CODE_SUCCESS) {
    code = terrno;
    mError("failed to prepare checkpoint trans since %s", terrstr());
    goto _ERR;
  }

  code = 0;
_ERR:
  mndTransDrop(pTrans);
  return code;
}

int32_t extractStreamNodeList(SMnode *pMnode) {
  if (taosArrayGetSize(execInfo.pNodeList) == 0) {
    int32_t code = refreshNodeListFromExistedStreams(pMnode, execInfo.pNodeList);
    if (code) {
      mError("Failed to extract node list from stream, code:%s", tstrerror(code));
      return code;
    }
  }

  return taosArrayGetSize(execInfo.pNodeList);
}

static bool taskNodeIsUpdated(SMnode *pMnode) {
  // check if the node update happens or not
  streamMutexLock(&execInfo.lock);

  int32_t numOfNodes = extractStreamNodeList(pMnode);
  if (numOfNodes == 0) {
    mDebug("stream task node change checking done, no vgroups exist, do nothing");
    execInfo.ts = taosGetTimestampSec();
    streamMutexUnlock(&execInfo.lock);
    return false;
  }

  for (int32_t i = 0; i < numOfNodes; ++i) {
    SNodeEntry *pNodeEntry = taosArrayGet(execInfo.pNodeList, i);
    if (pNodeEntry->stageUpdated) {
      mDebug("stream task not ready due to node update detected, checkpoint not issued");
      streamMutexUnlock(&execInfo.lock);
      return true;
    }
  }

  bool    allReady = true;
  SArray *pNodeSnapshot = NULL;

  int32_t code = mndTakeVgroupSnapshot(pMnode, &allReady, &pNodeSnapshot);
  if (code) {
    mError("failed to get the vgroup snapshot, ignore it and continue");
  }
  if (!allReady) {
    mWarn("not all vnodes ready, quit from vnodes status check");
    taosArrayDestroy(pNodeSnapshot);
    streamMutexUnlock(&execInfo.lock);
    return true;
  }

  SVgroupChangeInfo changeInfo = mndFindChangedNodeInfo(pMnode, execInfo.pNodeList, pNodeSnapshot);

  bool nodeUpdated = (taosArrayGetSize(changeInfo.pUpdateNodeList) > 0);

  taosArrayDestroy(changeInfo.pUpdateNodeList);
  taosHashCleanup(changeInfo.pDBMap);
  taosArrayDestroy(pNodeSnapshot);

  if (nodeUpdated) {
    mDebug("stream tasks not ready due to node update");
  }

  streamMutexUnlock(&execInfo.lock);
  return nodeUpdated;
}

static int32_t mndCheckTaskAndNodeStatus(SMnode *pMnode) {
  bool ready = true;
  if (taskNodeIsUpdated(pMnode)) {
    return -1;
  }

  streamMutexLock(&execInfo.lock);
  if (taosArrayGetSize(execInfo.pNodeList) == 0) {
    mDebug("stream task node change checking done, no vgroups exist, do nothing");
    ASSERT(taosArrayGetSize(execInfo.pTaskList) == 0);
  }

  SArray *pInvalidList = taosArrayInit(4, sizeof(STaskId));

  for (int32_t i = 0; i < taosArrayGetSize(execInfo.pTaskList); ++i) {
    STaskId          *p = taosArrayGet(execInfo.pTaskList, i);
    STaskStatusEntry *pEntry = taosHashGet(execInfo.pTaskMap, p, sizeof(*p));
    if (pEntry == NULL) {
      continue;
    }

    if (pEntry->status == TASK_STATUS__STOP) {
      for (int32_t j = 0; j < taosArrayGetSize(pInvalidList); ++j) {
        STaskId *pId = taosArrayGet(pInvalidList, j);
        if (pEntry->id.streamId == pId->streamId) {
          void* px = taosArrayPush(pInvalidList, &pEntry->id);
          if (px == NULL) {
            mError("failed to put stream into invalid list, code:%s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
          }
          break;
        }
      }
    }

    if (pEntry->status != TASK_STATUS__READY) {
      mDebug("s-task:0x%" PRIx64 "-0x%x (nodeId:%d) status:%s, checkpoint not issued", pEntry->id.streamId,
             (int32_t)pEntry->id.taskId, pEntry->nodeId, streamTaskGetStatusStr(pEntry->status));
      ready = false;
      break;
    }

    if (pEntry->hTaskId != 0) {
      mDebug("s-task:0x%" PRIx64 "-0x%x (nodeId:%d) status:%s related fill-history task:0x%" PRIx64
             " exists, checkpoint not issued",
             pEntry->id.streamId, (int32_t)pEntry->id.taskId, pEntry->nodeId, streamTaskGetStatusStr(pEntry->status),
             pEntry->hTaskId);
      ready = false;
      break;
    }
  }

  removeTasksInBuf(pInvalidList, &execInfo);
  taosArrayDestroy(pInvalidList);

  streamMutexUnlock(&execInfo.lock);
  return ready ? 0 : -1;
}

int64_t getStreamTaskLastReadyState(SArray *pTaskList, int64_t streamId) {
  int64_t ts = -1;
  int32_t taskId = -1;

  for (int32_t i = 0; i < taosArrayGetSize(pTaskList); ++i) {
    STaskId          *p = taosArrayGet(pTaskList, i);
    STaskStatusEntry *pEntry = taosHashGet(execInfo.pTaskMap, p, sizeof(*p));
    if (pEntry == NULL || pEntry->id.streamId != streamId) {
      continue;
    }

    if (pEntry->status == TASK_STATUS__READY && ts < pEntry->startTime) {
      ts = pEntry->startTime;
      taskId = pEntry->id.taskId;
    }
  }

  mDebug("stream:0x%" PRIx64 " last ready ts:%" PRId64 " s-task:0x%x", streamId, ts, taskId);
  return ts;
}

typedef struct {
  int64_t streamId;
  int64_t duration;
} SCheckpointInterval;

static int32_t streamWaitComparFn(const void *p1, const void *p2) {
  const SCheckpointInterval *pInt1 = p1;
  const SCheckpointInterval *pInt2 = p2;
  if (pInt1->duration == pInt2->duration) {
    return 0;
  }

  return pInt1->duration > pInt2->duration ? -1 : 1;
}

static int32_t mndProcessStreamCheckpoint(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  void       *pIter = NULL;
  SStreamObj *pStream = NULL;
  int32_t     code = 0;
  int32_t     numOfCheckpointTrans = 0;

  if ((code = mndCheckTaskAndNodeStatus(pMnode)) != 0) {
    terrno = TSDB_CODE_STREAM_TASK_IVLD_STATUS;
    return -1;
  }

  SArray *pList = taosArrayInit(4, sizeof(SCheckpointInterval));
  int64_t now = taosGetTimestampMs();

  while ((pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream)) != NULL) {
    int64_t duration = now - pStream->checkpointFreq;
    if (duration < tsStreamCheckpointInterval * 1000) {
      sdbRelease(pSdb, pStream);
      continue;
    }

    streamMutexLock(&execInfo.lock);
    int64_t startTs = getStreamTaskLastReadyState(execInfo.pTaskList, pStream->uid);
    if (startTs != -1 && (now - startTs) < tsStreamCheckpointInterval * 1000) {
      streamMutexUnlock(&execInfo.lock);
      sdbRelease(pSdb, pStream);
      continue;
    }
    streamMutexUnlock(&execInfo.lock);

    SCheckpointInterval in = {.streamId = pStream->uid, .duration = duration};
    void* p = taosArrayPush(pList, &in);
    if (p) {
      int32_t currentSize = taosArrayGetSize(pList);
      mDebug("stream:%s (uid:0x%" PRIx64 ") checkpoint interval beyond threshold: %ds(%" PRId64
             "s) beyond concurrently launch threshold:%d",
             pStream->name, pStream->uid, tsStreamCheckpointInterval, duration / 1000, currentSize);
    } else {
      mError("failed to record the checkpoint interval info, stream:0x%" PRIx64, pStream->uid);
    }
    sdbRelease(pSdb, pStream);
  }

  int32_t size = taosArrayGetSize(pList);
  if (size == 0) {
    taosArrayDestroy(pList);
    return code;
  }

  taosArraySort(pList, streamWaitComparFn);
  code = mndStreamClearFinishedTrans(pMnode, &numOfCheckpointTrans);
  if (code) {
    mError("failed to clear finish trans, code:%s", tstrerror(code));
    taosArrayDestroy(pList);
    return code;
  }

  int32_t numOfQual = taosArrayGetSize(pList);
  if (numOfCheckpointTrans > tsMaxConcurrentCheckpoint) {
    mDebug(
        "%d stream(s) checkpoint interval longer than %ds, ongoing checkpoint trans:%d reach maximum allowed:%d, new "
        "checkpoint trans are not allowed, wait for 30s",
        numOfQual, tsStreamCheckpointInterval, numOfCheckpointTrans, tsMaxConcurrentCheckpoint);
    taosArrayDestroy(pList);
    return code;
  }

  int32_t capacity = tsMaxConcurrentCheckpoint - numOfCheckpointTrans;
  mDebug(
      "%d stream(s) checkpoint interval longer than %ds, %d ongoing checkpoint trans, %d new checkpoint trans allowed, "
      "concurrent trans threshold:%d",
      numOfQual, tsStreamCheckpointInterval, numOfCheckpointTrans, capacity, tsMaxConcurrentCheckpoint);

  int32_t started = 0;
  int64_t checkpointId = mndStreamGenChkptId(pMnode, true);

  for (int32_t i = 0; i < numOfQual; ++i) {
    SCheckpointInterval *pCheckpointInfo = taosArrayGet(pList, i);

    SStreamObj *p = NULL;
    code = mndGetStreamObj(pMnode, pCheckpointInfo->streamId, &p);
    if (p != NULL && code == 0) {
      code = mndProcessStreamCheckpointTrans(pMnode, p, checkpointId, 1, true);
      sdbRelease(pSdb, p);

      if (code != -1) {
        started += 1;

        if (started >= capacity) {
          mDebug("already start %d new checkpoint trans, current active checkpoint trans:%d", started,
                 (started + numOfCheckpointTrans));
          break;
        }
      }
    }
  }

  taosArrayDestroy(pList);
  return code;
}

static int32_t mndProcessDropStreamReq(SRpcMsg *pReq) {
  SMnode *    pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  int32_t     code = 0;

  SMDropStreamReq dropReq = {0};
  if (tDeserializeSMDropStreamReq(pReq->pCont, pReq->contLen, &dropReq) < 0) {
    mError("invalid drop stream msg recv, discarded");
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  mDebug("recv drop stream:%s msg", dropReq.name);

  code = mndAcquireStream(pMnode, dropReq.name, &pStream);
  if (pStream == NULL || code != 0) {
    if (dropReq.igNotExists) {
      mInfo("stream:%s not exist, ignore not exist is set, drop stream exec done with success", dropReq.name);
      sdbRelease(pMnode->pSdb, pStream);
      tFreeMDropStreamReq(&dropReq);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
      mError("stream:%s not exist failed to drop it", dropReq.name);
      tFreeMDropStreamReq(&dropReq);
      return -1;
    }
  }

  if (pStream->smaId != 0) {
    mDebug("stream:%s, uid:0x%" PRIx64 " try to drop sma related stream", dropReq.name, pStream->uid);

    void *   pIter = NULL;
    SSmaObj *pSma = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_SMA, pIter, (void **)&pSma);
    while (pIter) {
      if (pSma && pSma->uid == pStream->smaId) {
        sdbRelease(pMnode->pSdb, pSma);
        sdbRelease(pMnode->pSdb, pStream);

        sdbCancelFetch(pMnode->pSdb, pIter);
        tFreeMDropStreamReq(&dropReq);
        terrno = TSDB_CODE_TSMA_MUST_BE_DROPPED;

        mError("try to drop sma-related stream:%s, uid:0x%" PRIx64 " code:%s only allowed to be dropped along with sma",
               dropReq.name, pStream->uid, tstrerror(terrno));
        return -1;
      }

      if (pSma) {
        sdbRelease(pMnode->pSdb, pSma);
      }

      pIter = sdbFetch(pMnode->pSdb, SDB_SMA, pIter, (void **)&pSma);
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

  STrans *pTrans = NULL;
  code = doCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_NOTHING, MND_STREAM_DROP_NAME, "drop stream", &pTrans);
  if (pTrans == NULL || code) {
    mError("stream:%s uid:0x%" PRIx64 " failed to drop since %s", dropReq.name, pStream->uid, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMDropStreamReq(&dropReq);
    return -1;
  }

  code = mndStreamRegisterTrans(pTrans, MND_STREAM_DROP_NAME, pStream->uid);

  // drop all tasks
  if (mndStreamSetDropAction(pMnode, pTrans, pStream) < 0) {
    mError("stream:%s uid:0x%" PRIx64 " failed to drop task since %s", dropReq.name, pStream->uid, terrstr());
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
    mDebug("drop active transId:%d due to stream:%s uid:0x%" PRIx64 " dropped", transId, pStream->name, pStream->uid);
    mndKillTransImpl(pMnode, transId, pStream->sourceDb);
  }

  mDebug("stream:%s uid:0x%" PRIx64 " transId:%d start to drop related task when dropping stream", dropReq.name,
         pStream->uid, transId);

  removeStreamTasksInBuf(pStream, &execInfo);

  SName name = {0};
  code = tNameFromString(&name, dropReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  auditRecord(pReq, pMnode->clusterId, "dropStream", "", name.dbname, dropReq.sql, dropReq.sqlLen);

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);
  tFreeMDropStreamReq(&dropReq);

  if (code == 0) {
    return TSDB_CODE_ACTION_IN_PROGRESS;
  } else {
    return code;
  }
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

static void int64ToHexStr(int64_t id, char *pBuf, int32_t bufLen) {
  memset(pBuf, 0, bufLen);
  pBuf[2] = '0';
  pBuf[3] = 'x';

  int32_t len = tintToHex(id, &pBuf[4]);
  varDataSetLen(pBuf, len + 2);
}

static int32_t mndRetrieveStream(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SStreamObj *pStream = NULL;
  int32_t     code = 0;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_STREAM, pShow->pIter, (void **)&pStream);
    if (pShow->pIter == NULL) break;

    code = setStreamAttrInResBlock(pStream, pBlock, numOfRows);
    if (code == 0) {
      numOfRows++;
    }
    sdbRelease(pSdb, pStream);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextStream(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

int32_t setStreamAttrInResBlock(SStreamObj *pStream, SSDataBlock *pBlock, int32_t numOfRows) {
  int32_t code = 0;
  int32_t cols = 0;
  int32_t lino = 0;

  char streamName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(streamName, mndGetDbStr(pStream->name), sizeof(streamName));
  SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)streamName, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // create time
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pStream->createTime, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // stream id
  char buf[128] = {0};
  int64ToHexStr(pStream->uid, buf, tListLen(buf));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, buf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // related fill-history stream id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  if (pStream->hTaskUid != 0) {
    int64ToHexStr(pStream->hTaskUid, buf, tListLen(buf));
    code = colDataSetVal(pColInfo, numOfRows, buf, false);
  } else {
    code = colDataSetVal(pColInfo, numOfRows, buf, true);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  // related fill-history stream id
  char sql[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(sql, pStream->sql, sizeof(sql));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)sql, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char status[20 + VARSTR_HEADER_SIZE] = {0};
  char status2[20] = {0};
  mndShowStreamStatus(status2, pStream);
  STR_WITH_MAXSIZE_TO_VARSTR(status, status2, sizeof(status));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&status, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char sourceDB[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(sourceDB, mndGetDbStr(pStream->sourceDb), sizeof(sourceDB));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&sourceDB, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char targetDB[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(targetDB, mndGetDbStr(pStream->targetDb), sizeof(targetDB));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&targetDB, false);
  TSDB_CHECK_CODE(code, lino, _end);

  if (pStream->targetSTbName[0] == 0) {
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, NULL, true);
  } else {
    char targetSTB[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(targetSTB, mndGetStbStr(pStream->targetSTbName), sizeof(targetSTB));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&targetSTB, false);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pStream->conf.watermark, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char trigger[20 + VARSTR_HEADER_SIZE] = {0};
  char trigger2[20] = {0};
  mndShowStreamTrigger(trigger2, pStream);
  STR_WITH_MAXSIZE_TO_VARSTR(trigger, trigger2, sizeof(trigger));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&trigger, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // sink_quota
  char sinkQuota[20 + VARSTR_HEADER_SIZE] = {0};
  sinkQuota[0] = '0';
  char dstStr[20] = {0};
  STR_TO_VARSTR(dstStr, sinkQuota)
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)dstStr, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint interval
  char tmp[20 + VARSTR_HEADER_SIZE] = {0};
  sprintf(varDataVal(tmp), "%d sec", tsStreamCheckpointInterval);
  varDataSetLen(tmp, strlen(varDataVal(tmp)));

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)tmp, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint backup type
  char backup[20 + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(backup, "none")
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)backup, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // history scan idle
  char scanHistoryIdle[20 + VARSTR_HEADER_SIZE] = {0};
  strcpy(scanHistoryIdle, "100a");

  memset(dstStr, 0, tListLen(dstStr));
  STR_TO_VARSTR(dstStr, scanHistoryIdle)
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)dstStr, false);

  _end:
  return code;
}

static int32_t setTaskAttrInResBlock(SStreamObj *pStream, SStreamTask *pTask, SSDataBlock *pBlock, int32_t numOfRows) {
  SColumnInfoData *pColInfo;
  int32_t          cols = 0;
  int32_t code = 0;
  int32_t lino = 0;

  STaskId id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};

  STaskStatusEntry *pe = taosHashGet(execInfo.pTaskMap, &id, sizeof(id));
  if (pe == NULL) {
    mError("task:0x%" PRIx64 " not exists in any vnodes, streamName:%s, streamId:0x%" PRIx64 " createTs:%" PRId64
           " no valid status/stage info",
           id.taskId, pStream->name, pStream->uid, pStream->createTime);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  // stream name
  char streamName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(streamName, mndGetDbStr(pStream->name), sizeof(streamName));

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)streamName, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // task id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);

  char idstr[128] = {0};
  int64ToHexStr(pTask->id.taskId, idstr, tListLen(idstr));
  code = colDataSetVal(pColInfo, numOfRows, idstr, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // node type
  char nodeType[20 + VARSTR_HEADER_SIZE] = {0};
  varDataSetLen(nodeType, 5);
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  if (pTask->info.nodeId > 0) {
    memcpy(varDataVal(nodeType), "vnode", 5);
  } else {
    memcpy(varDataVal(nodeType), "snode", 5);
  }
  code = colDataSetVal(pColInfo, numOfRows, nodeType, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // node id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  int64_t nodeId = TMAX(pTask->info.nodeId, 0);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&nodeId, false);
  TSDB_CHECK_CODE(code, lino, _end);

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
  code = colDataSetVal(pColInfo, numOfRows, (const char *)level, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // status
  char status[20 + VARSTR_HEADER_SIZE] = {0};

  const char *pStatus = streamTaskGetStatusStr(pe->status);
  STR_TO_VARSTR(status, pStatus);

  // status
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)status, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // stage
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->stage, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // input queue
  char        vbuf[40] = {0};
  char        buf[38] = {0};
  const char *queueInfoStr = "%4.2f MiB (%6.2f%)";
  snprintf(buf, tListLen(buf), queueInfoStr, pe->inputQUsed, pe->inputRate);
  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // input total
  const char *formatTotalMb = "%7.2f MiB";
  const char *formatTotalGb = "%7.2f GiB";
  if (pe->procsTotal < 1024) {
    snprintf(buf, tListLen(buf), formatTotalMb, pe->procsTotal);
  } else {
    snprintf(buf, tListLen(buf), formatTotalGb, pe->procsTotal / 1024);
  }

  memset(vbuf, 0, tListLen(vbuf));
  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // process throughput
  const char *formatKb = "%7.2f KiB/s";
  const char *formatMb = "%7.2f MiB/s";
  if (pe->procsThroughput < 1024) {
    snprintf(buf, tListLen(buf), formatKb, pe->procsThroughput);
  } else {
    snprintf(buf, tListLen(buf), formatMb, pe->procsThroughput / 1024);
  }

  memset(vbuf, 0, tListLen(vbuf));
  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // output total
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);

  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    colDataSetNULL(pColInfo, numOfRows);
  } else {
    sprintf(buf, formatTotalMb, pe->outputTotal);
    memset(vbuf, 0, tListLen(vbuf));
    STR_TO_VARSTR(vbuf, buf);

    code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  // output throughput
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);

  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    colDataSetNULL(pColInfo, numOfRows);
  } else {
    if (pe->outputThroughput < 1024) {
      snprintf(buf, tListLen(buf), formatKb, pe->outputThroughput);
    } else {
      snprintf(buf, tListLen(buf), formatMb, pe->outputThroughput / 1024);
    }

    memset(vbuf, 0, tListLen(vbuf));
    STR_TO_VARSTR(vbuf, buf);

    code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  // output queue
  //          sprintf(buf, queueInfoStr, pe->outputQUsed, pe->outputRate);
  //        STR_TO_VARSTR(vbuf, buf);

  //        pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  //        colDataSetVal(pColInfo, numOfRows, (const char*)vbuf, false);

  // info
  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    const char *sinkStr = "%.2f MiB";
    snprintf(buf, tListLen(buf), sinkStr, pe->sinkDataSize);
  } else if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    // offset info
    const char *offsetStr = "%" PRId64 " [%" PRId64 ", %" PRId64 "]";
    snprintf(buf, tListLen(buf), offsetStr, pe->processedVer, pe->verRange.minVer, pe->verRange.maxVer);
  } else {
    memset(buf, 0, tListLen(buf));
  }

  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // start_time
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->startTime, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // start id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->startCheckpointId, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // start ver
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->startCheckpointVer, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint time
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  if (pe->checkpointInfo.latestTime != 0) {
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->checkpointInfo.latestTime, false);
  } else {
    code = colDataSetVal(pColInfo, numOfRows, 0, true);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint_id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->checkpointInfo.latestId, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint version
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->checkpointInfo.latestVer, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint size
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  colDataSetNULL(pColInfo, numOfRows);

  // checkpoint backup status
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, 0, true);
  TSDB_CHECK_CODE(code, lino, _end);

  // ds_err_info
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, 0, true);
  TSDB_CHECK_CODE(code, lino, _end);

  // history_task_id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  if (pe->hTaskId != 0) {
    int64ToHexStr(pe->hTaskId, idstr, tListLen(idstr));
    code = colDataSetVal(pColInfo, numOfRows, idstr, false);
  } else {
    code = colDataSetVal(pColInfo, numOfRows, 0, true);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  // history_task_status
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  code = colDataSetVal(pColInfo, numOfRows, 0, true);
  TSDB_CHECK_CODE(code, lino, _end);

  _end:
  return code;
}

static int32_t mndRetrieveStreamTask(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SStreamObj *pStream = NULL;
  int32_t     code = 0;

  streamMutexLock(&execInfo.lock);
  mndInitStreamExecInfo(pMnode, &execInfo);
  streamMutexUnlock(&execInfo.lock);

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_STREAM, pShow->pIter, (void **)&pStream);
    if (pShow->pIter == NULL) {
      break;
    }

    // lock
    taosRLockLatch(&pStream->lock);

    int32_t count = mndGetNumOfStreamTasks(pStream);
    if (numOfRows + count > rowsCapacity) {
      code = blockDataEnsureCapacity(pBlock, numOfRows + count);
      if (code) {
        mError("failed to prepare the result block buffer, quit return value");
        taosRUnLockLatch(&pStream->lock);
        sdbRelease(pSdb, pStream);
        continue;
      }
    }

    // add row for each task
    SStreamTaskIter *pIter = NULL;
    code = createStreamTaskIter(pStream, &pIter);
    if (code) {
      taosRUnLockLatch(&pStream->lock);
      sdbRelease(pSdb, pStream);
      mError("failed to create task iter for stream:%s", pStream->name);
      continue;
    }

    while (streamTaskIterNextTask(pIter)) {
      SStreamTask *pTask = NULL;
      code = streamTaskIterGetCurrent(pIter, &pTask);
      if (code) {
        destroyStreamTaskIter(pIter);
        break;
      }

      code = setTaskAttrInResBlock(pStream, pTask, pBlock, numOfRows);
      if (code == TSDB_CODE_SUCCESS) {
        numOfRows++;
      }
    }

    pBlock->info.rows = numOfRows;

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
  int32_t     code = 0;

  SMPauseStreamReq pauseReq = {0};
  if (tDeserializeSMPauseStreamReq(pReq->pCont, pReq->contLen, &pauseReq) < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  code = mndAcquireStream(pMnode, pauseReq.name, &pStream);
  if (pStream == NULL || code != 0) {
    if (pauseReq.igNotExists) {
      mInfo("stream:%s, not exist, not pause stream", pauseReq.name);
      return 0;
    } else {
      mError("stream:%s not exist, failed to pause stream", pauseReq.name);
      terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
      return -1;
    }
  }

  mInfo("stream:%s,%" PRId64 " start to pause stream", pauseReq.name, pStream->uid);

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
    sdbRelease(pMnode->pSdb, pStream);
    return -1;
  }

  {  // check for tasks, if tasks are not ready, not allowed to pause
    bool found = false;
    bool readyToPause = true;
    streamMutexLock(&execInfo.lock);

    for (int32_t i = 0; i < taosArrayGetSize(execInfo.pTaskList); ++i) {
      STaskId *p = taosArrayGet(execInfo.pTaskList, i);

      STaskStatusEntry *pEntry = taosHashGet(execInfo.pTaskMap, p, sizeof(*p));
      if (pEntry == NULL) {
        continue;
      }

      if (pEntry->id.streamId != pStream->uid) {
        continue;
      }

      if (pEntry->status == TASK_STATUS__UNINIT || pEntry->status == TASK_STATUS__CK) {
        mError("stream:%s uid:0x%" PRIx64 " vgId:%d task:0x%" PRIx64 " status:%s, not ready for pause", pStream->name,
               pStream->uid, pEntry->nodeId, pEntry->id.taskId, streamTaskGetStatusStr(pEntry->status));
        readyToPause = false;
      }

      found = true;
    }

    streamMutexUnlock(&execInfo.lock);
    if (!found) {
      mError("stream:%s task not report status yet, not ready for pause", pauseReq.name);
      sdbRelease(pMnode->pSdb, pStream);
      return -1;
    }

    if (!readyToPause) {
      mError("stream:%s task not ready for pause yet", pauseReq.name);
      sdbRelease(pMnode->pSdb, pStream);
      return -1;
    }
  }

  STrans *pTrans = NULL;
  code = doCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_NOTHING, MND_STREAM_PAUSE_NAME, "pause the stream", &pTrans);
  if (pTrans == NULL || code) {
    mError("stream:%s failed to pause stream since %s", pauseReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  code = mndStreamRegisterTrans(pTrans, MND_STREAM_PAUSE_NAME, pStream->uid);
  if (code) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  // if nodeUpdate happened, not send pause trans
  code = mndStreamSetPauseAction(pMnode, pTrans, pStream);
  if (code) {
    mError("stream:%s, failed to pause task since %s", pauseReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  // pause stream
  taosWLockLatch(&pStream->lock);
  pStream->status = STREAM_STATUS__PAUSE;
  code = mndPersistTransLog(pStream, pTrans, SDB_STATUS_READY);
  if (code) {
    taosWUnLockLatch(&pStream->lock);
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  taosWUnLockLatch(&pStream->lock);

  code = mndTransPrepare(pMnode, pTrans);
  if (code) {
    mError("trans:%d, failed to prepare pause stream trans since %s", pTrans->id, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}

static int32_t mndProcessResumeStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  int32_t code = 0;

  if ((terrno = grantCheckExpire(TSDB_GRANT_STREAMS)) < 0) {
    return -1;
  }

  SMResumeStreamReq resumeReq = {0};
  if (tDeserializeSMResumeStreamReq(pReq->pCont, pReq->contLen, &resumeReq) < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  code = mndAcquireStream(pMnode, resumeReq.name, &pStream);
  if (pStream == NULL || code != 0) {
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
    return terrno;
  }

  STrans *pTrans = NULL;
  code =
      doCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_NOTHING, MND_STREAM_RESUME_NAME, "resume the stream", &pTrans);
  if (pTrans == NULL || code) {
    mError("stream:%s, failed to resume stream since %s", resumeReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  code = mndStreamRegisterTrans(pTrans, MND_STREAM_RESUME_NAME, pStream->uid);
  if (code) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  // set the resume action
  if (mndStreamSetResumeAction(pTrans, pMnode, pStream, resumeReq.igUntreated) < 0) {
    mError("stream:%s, failed to drop task since %s", resumeReq.name, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  // resume stream
  taosWLockLatch(&pStream->lock);
  pStream->status = STREAM_STATUS__NORMAL;
  if (mndPersistTransLog(pStream, pTrans, SDB_STATUS_READY) < 0) {
    taosWUnLockLatch(&pStream->lock);

    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  taosWUnLockLatch(&pStream->lock);
  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare pause stream trans since %s", pTrans->id, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
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
          (void) epsetToStr(&pCurrent->epset, buf, tListLen(buf));  // ignore this error

          mDebug("nodeId:%d restart/epset changed detected, old:%s:%d -> new:%s, stageUpdate:%d", pCurrent->nodeId,
                 pPrevEp->fqdn, pPrevEp->port, buf, pPrevEntry->stageUpdated);

          SNodeUpdateInfo updateInfo = {.nodeId = pPrevEntry->nodeId};
          epsetAssign(&updateInfo.prevEp, &pPrevEntry->epset);
          epsetAssign(&updateInfo.newEp, &pCurrent->epset);

          void* p = taosArrayPush(info.pUpdateNodeList, &updateInfo);
          if (p == NULL) {
            mError("failed to put update entry into node list, code:%s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
          }
        }

        // todo handle the snode info
        if (pCurrent->nodeId != SNODE_HANDLE) {
          SVgObj *pVgroup = mndAcquireVgroup(pMnode, pCurrent->nodeId);
          int32_t code = taosHashPut(info.pDBMap, pVgroup->dbName, strlen(pVgroup->dbName), NULL, 0);
          mndReleaseVgroup(pMnode, pVgroup);
          if (code) {
            mError("failed to put into dbmap, code:out of memory");
          }
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
  int32_t     code = 0;

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
      return terrno;
    }
  }

  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) {
      break;
    }

    // here create only one trans
    if (pTrans == NULL) {
      code = doCreateTrans(pMnode, pStream, NULL, TRN_CONFLICT_NOTHING, MND_STREAM_TASK_UPDATE_NAME, "update task epsets", &pTrans);
      if (pTrans == NULL || code) {
        sdbRelease(pSdb, pStream);
        sdbCancelFetch(pSdb, pIter);
        return terrno = code;
      }

      code = mndStreamRegisterTrans(pTrans, MND_STREAM_TASK_UPDATE_NAME, pStream->uid);
      if (code) {
        mError("failed to register trans, transId:%d, and continue", pTrans->id);
      }
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

    code = mndStreamSetUpdateEpsetAction(pMnode, pStream, pChangeInfo, pTrans);

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
      return code;
    }
  }

  // no need to build the trans to handle the vgroup update
  if (pTrans == NULL) {
    return 0;
  }

  code = mndTransPrepare(pMnode, pTrans);
  if (code) {
    mError("trans:%d, failed to prepare update stream trans since %s", pTrans->id, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);
  return code;
}

static int32_t refreshNodeListFromExistedStreams(SMnode *pMnode, SArray *pNodeList) {
  SSdb       *pSdb = pMnode->pSdb;
  SStreamObj *pStream = NULL;
  void       *pIter = NULL;
  int32_t     code = 0;

  mDebug("start to refresh node list by existed streams");

  SHashObj *pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (pHash == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) {
      break;
    }

    taosWLockLatch(&pStream->lock);

    SStreamTaskIter *pTaskIter = NULL;
    code = createStreamTaskIter(pStream, &pTaskIter);
    if (code) {
      taosWUnLockLatch(&pStream->lock);
      sdbRelease(pSdb, pStream);
      mError("failed to create task iter for stream:%s", pStream->name);
      continue;
    }

    while (streamTaskIterNextTask(pTaskIter)) {
      SStreamTask *pTask = NULL;
      code = streamTaskIterGetCurrent(pTaskIter, &pTask);
      if (code) {
        break;
      }

      SNodeEntry entry = {.hbTimestamp = -1, .nodeId = pTask->info.nodeId};
      epsetAssign(&entry.epset, &pTask->info.epSet);
      (void)taosHashPut(pHash, &entry.nodeId, sizeof(entry.nodeId), &entry, sizeof(entry));
    }

    destroyStreamTaskIter(pTaskIter);
    taosWUnLockLatch(&pStream->lock);

    sdbRelease(pSdb, pStream);
  }

  taosArrayClear(pNodeList);

  // convert to list
  pIter = NULL;
  while ((pIter = taosHashIterate(pHash, pIter)) != NULL) {
    SNodeEntry *pEntry = (SNodeEntry *)pIter;

    void *p = taosArrayPush(pNodeList, pEntry);
    if (p == NULL) {
      mError("failed to put entry into node list, nodeId:%d, code: out of memory", pEntry->nodeId);
      if (code == 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
      }
      continue;
    }

    char buf[256] = {0};
    (void) epsetToStr(&pEntry->epset, buf, tListLen(buf));  // ignore this error since it is only for log file
    mDebug("extract nodeInfo from stream obj, nodeId:%d, %s", pEntry->nodeId, buf);
  }

  taosHashCleanup(pHash);

  mDebug("numOfNodes:%d for stream after extract nodeInfo from stream", (int32_t)taosArrayGetSize(pNodeList));
  return code;
}

// this function runs by only one thread, so it is not multi-thread safe
static int32_t mndProcessNodeCheckReq(SRpcMsg *pMsg) {
  int32_t code = 0;
  bool    allReady = true;
  SArray *pNodeSnapshot = NULL;

  int32_t old = atomic_val_compare_exchange_32(&mndNodeCheckSentinel, 0, 1);
  if (old != 0) {
    mDebug("still in checking node change");
    return 0;
  }

  mDebug("start to do node change checking");
  int64_t ts = taosGetTimestampSec();

  SMnode *pMnode = pMsg->info.node;

  streamMutexLock(&execInfo.lock);
  int32_t numOfNodes = extractStreamNodeList(pMnode);
  streamMutexUnlock(&execInfo.lock);

  if (numOfNodes == 0) {
    mDebug("end to do stream task(s) node change checking, no stream tasks exist, do nothing");
    execInfo.ts = ts;
    atomic_store_32(&mndNodeCheckSentinel, 0);
    return 0;
  }

  code = mndTakeVgroupSnapshot(pMnode, &allReady, &pNodeSnapshot);
  if (code) {
    mError("failed to take the vgroup snapshot, ignore it and continue");
  }

  if (!allReady) {
    taosArrayDestroy(pNodeSnapshot);
    atomic_store_32(&mndNodeCheckSentinel, 0);
    mWarn("not all vnodes are ready, ignore the exec nodeUpdate check");
    return 0;
  }

  streamMutexLock(&execInfo.lock);

  code = removeExpiredNodeEntryAndTaskInBuf(pNodeSnapshot);
  if (code) {
    goto _end;
  }

  SVgroupChangeInfo changeInfo = mndFindChangedNodeInfo(pMnode, execInfo.pNodeList, pNodeSnapshot);
  if (taosArrayGetSize(changeInfo.pUpdateNodeList) > 0) {
    // kill current active checkpoint transaction, since the transaction is vnode wide.
    killAllCheckpointTrans(pMnode, &changeInfo);
    code = mndProcessVgroupChange(pMnode, &changeInfo);

    // keep the new vnode snapshot if success
    if (code == TSDB_CODE_SUCCESS || code == TSDB_CODE_ACTION_IN_PROGRESS) {
      code = refreshNodeListFromExistedStreams(pMnode, execInfo.pNodeList);
      if (code) {
        mError("failed to extract node list from stream, code:%s", tstrerror(code));
        goto _end;
      }

      execInfo.ts = ts;
      mDebug("create trans successfully, update cached node list, numOfNodes:%d",
             (int)taosArrayGetSize(execInfo.pNodeList));
    } else {
      mError("unexpected code during create nodeUpdate trans, code:%s", tstrerror(code));
    }
  } else {
    mDebug("no update found in nodeList");
  }

  taosArrayDestroy(changeInfo.pUpdateNodeList);
  taosHashCleanup(changeInfo.pDBMap);

  _end:
  streamMutexUnlock(&execInfo.lock);
  taosArrayDestroy(pNodeSnapshot);

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

  int32_t               size = sizeof(SMStreamNodeCheckMsg);
  SMStreamNodeCheckMsg *pMsg = rpcMallocCont(size);

  SRpcMsg rpcMsg = {.msgType = TDMT_MND_STREAM_NODECHANGE_CHECK, .pCont = pMsg, .contLen = size};
  return tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
}

void saveTaskAndNodeInfoIntoBuf(SStreamObj *pStream, SStreamExecInfo *pExecNode) {
  SStreamTaskIter *pIter = NULL;
  int32_t          code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    mError("failed to create task iter for stream:%s", pStream->name);
    return;
  }

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    if (code) {
      break;
    }

    STaskId id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};
    void   *p = taosHashGet(pExecNode->pTaskMap, &id, sizeof(id));
    if (p == NULL) {
      STaskStatusEntry entry = {0};
      streamTaskStatusInit(&entry, pTask);

      code = taosHashPut(pExecNode->pTaskMap, &id, sizeof(id), &entry, sizeof(entry));
      if (code == 0) {
        void *  px = taosArrayPush(pExecNode->pTaskList, &id);
        int32_t num = (int32_t)taosArrayGetSize(pExecNode->pTaskList);
        if (px) {
          mInfo("s-task:0x%x add into task buffer, total:%d", (int32_t)entry.id.taskId, num);
        } else {
          mError("s-task:0x%x failed to add into task buffer, total:%d", (int32_t)entry.id.taskId, num);
        }
      } else {
        mError("s-task:0x%x failed to add into task map, since out of memory", (int32_t) entry.id.taskId);
      }

      // add the new vgroups if not added yet
      bool exist = false;
      for (int32_t j = 0; j < taosArrayGetSize(pExecNode->pNodeList); ++j) {
        SNodeEntry *pEntry = taosArrayGet(pExecNode->pNodeList, j);
        if (pEntry->nodeId == pTask->info.nodeId) {
          exist = true;
          break;
        }
      }

      if (!exist) {
        SNodeEntry nodeEntry = {.hbTimestamp = -1, .nodeId = pTask->info.nodeId};
        epsetAssign(&nodeEntry.epset, &pTask->info.epSet);

        void* px = taosArrayPush(pExecNode->pNodeList, &nodeEntry);
        if (px) {
          mInfo("vgId:%d added into nodeList, total:%d", nodeEntry.nodeId, (int)taosArrayGetSize(pExecNode->pNodeList));
        } else {
          mError("vgId:%d failed to add into nodeList, total:%d", nodeEntry.nodeId, (int)taosArrayGetSize(pExecNode->pNodeList))
        }
      }
    }
  }

  destroyStreamTaskIter(pIter);
}

static void doAddTaskId(SArray *pList, int32_t taskId, int64_t uid, int32_t numOfTotal) {
  int32_t num = taosArrayGetSize(pList);
  for (int32_t i = 0; i < num; ++i) {
    int32_t *pId = taosArrayGet(pList, i);
    if (taskId == *pId) {
      return;
    }
  }

  void* p = taosArrayPush(pList, &taskId);
  int32_t numOfTasks = taosArrayGetSize(pList);

  if (p) {
    mDebug("stream:0x%" PRIx64 " receive %d reqs for checkpoint, remain:%d", uid, numOfTasks, numOfTotal - numOfTasks);
  } else {
    mError("stream:0x%" PRIx64 " receive %d reqs for checkpoint, failed to added into task list, since out of memory",
           uid, numOfTasks);
  }
}

int32_t mndProcessStreamReqCheckpoint(SRpcMsg *pReq) {
  SMnode                  *pMnode = pReq->info.node;
  SStreamTaskCheckpointReq req = {0};

  SDecoder decoder = {0};
  tDecoderInit(&decoder, pReq->pCont, pReq->contLen);

  if (tDecodeStreamTaskCheckpointReq(&decoder, &req)) {
    tDecoderClear(&decoder);
    mError("invalid task checkpoint req msg received");
    return TSDB_CODE_INVALID_MSG;
  }
  tDecoderClear(&decoder);

  mDebug("receive stream task checkpoint req msg, vgId:%d, s-task:0x%x", req.nodeId, req.taskId);

  // register to the stream task done map, if all tasks has sent this kinds of message, start the checkpoint trans.
  streamMutexLock(&execInfo.lock);

  SStreamObj *pStream = NULL;
  int32_t code = mndGetStreamObj(pMnode, req.streamId, &pStream);
  if (pStream == NULL || code != 0) {
    mWarn("failed to find the stream:0x%" PRIx64 ", not handle the checkpoint req, try to acquire in buf",
          req.streamId);

    // not in meta-store yet, try to acquire the task in exec buffer
    // the checkpoint req arrives too soon before the completion of the create stream trans.
    STaskId id = {.streamId = req.streamId, .taskId = req.taskId};
    void   *p = taosHashGet(execInfo.pTaskMap, &id, sizeof(id));
    if (p == NULL) {
      mError("failed to find the stream:0x%" PRIx64 " in buf, not handle the checkpoint req", req.streamId);
      streamMutexUnlock(&execInfo.lock);
      return TSDB_CODE_MND_STREAM_NOT_EXIST;
    } else {
      mDebug("s-task:0x%" PRIx64 "-0x%x in buf not in mnode/meta, create stream trans may not complete yet",
             req.streamId, req.taskId);
    }
  }

  int32_t numOfTasks = (pStream == NULL) ? 0 : mndGetNumOfStreamTasks(pStream);

  SArray **pReqTaskList = (SArray **)taosHashGet(execInfo.pTransferStateStreams, &req.streamId, sizeof(req.streamId));
  if (pReqTaskList == NULL) {
    SArray *pList = taosArrayInit(4, sizeof(int32_t));
    doAddTaskId(pList, req.taskId, req.streamId, numOfTasks);
    code = taosHashPut(execInfo.pTransferStateStreams, &req.streamId, sizeof(int64_t), &pList, sizeof(void *));
    if (code) {
      mError("failed to put into transfer state stream map, code: out of memory");
    }
    pReqTaskList = (SArray **)taosHashGet(execInfo.pTransferStateStreams, &req.streamId, sizeof(req.streamId));
  } else {
    doAddTaskId(*pReqTaskList, req.taskId, req.streamId, numOfTasks);
  }

  int32_t total = taosArrayGetSize(*pReqTaskList);
  if (total == numOfTasks) {  // all tasks has send the reqs
    int64_t checkpointId = mndStreamGenChkptId(pMnode, false);
    mInfo("stream:0x%" PRIx64 " all tasks req checkpoint, start checkpointId:%" PRId64, req.streamId, checkpointId);

    if (pStream != NULL) {  // TODO:handle error
      code = mndProcessStreamCheckpointTrans(pMnode, pStream, checkpointId, 0, false);
      if (code) {
        mError("failed to create checkpoint trans, code:%s", strerror(code));
      }
    } else {
      // todo: wait for the create stream trans completed, and launch the checkpoint trans
      // SStreamObj *pStream = mndGetStreamObj(pMnode, req.streamId);
      // sleep(500ms)
    }

    // remove this entry
    (void) taosHashRemove(execInfo.pTransferStateStreams, &req.streamId, sizeof(int64_t));

    int32_t numOfStreams = taosHashGetSize(execInfo.pTransferStateStreams);
    mDebug("stream:0x%" PRIx64 " removed, remain streams:%d fill-history not completed", req.streamId, numOfStreams);
  }

  if (pStream != NULL) {
    mndReleaseStream(pMnode, pStream);
  }

  streamMutexUnlock(&execInfo.lock);

  {
    SRpcMsg rsp = {.code = 0, .info = pReq->info, .contLen = sizeof(SMStreamReqCheckpointRsp)};
    rsp.pCont = rpcMallocCont(rsp.contLen);
    SMsgHead *pHead = rsp.pCont;
    pHead->vgId = htonl(req.nodeId);

    tmsgSendRsp(&rsp);
    pReq->info.handle = NULL;  // disable auto rsp
  }

  return 0;
}

static void doAddReportStreamTask(SArray* pList, const SCheckpointReport* pReport) {
  bool existed = false;
  for (int32_t i = 0; i < taosArrayGetSize(pList); ++i) {
    STaskChkptInfo *p = taosArrayGet(pList, i);
    if (p->taskId == pReport->taskId) {
      existed = true;
      break;
    }
  }

  if (!existed) {
    STaskChkptInfo info = {
        .streamId = pReport->streamId,
        .taskId = pReport->taskId,
        .transId = pReport->transId,
        .dropHTask = pReport->dropHTask,
        .version = pReport->checkpointVer,
        .ts = pReport->checkpointTs,
        .checkpointId = pReport->checkpointId,
        .nodeId = pReport->nodeId,
    };

    void* p = taosArrayPush(pList, &info);
    if (p == NULL) {
      mError("failed to put into task list, taskId:0x%x", pReport->taskId);
    }
  }
}

int32_t mndProcessCheckpointReport(SRpcMsg *pReq) {
  SMnode           *pMnode = pReq->info.node;
  SCheckpointReport req = {0};

  SDecoder decoder = {0};
  tDecoderInit(&decoder, pReq->pCont, pReq->contLen);

  if (tDecodeStreamTaskChkptReport(&decoder, &req)) {
    tDecoderClear(&decoder);
    mError("invalid task checkpoint-report msg received");
    return TSDB_CODE_INVALID_MSG;
  }
  tDecoderClear(&decoder);

  mDebug("receive stream task checkpoint-report msg, vgId:%d, s-task:0x%x, checkpointId:%" PRId64
         " checkpointVer:%" PRId64 " transId:%d",
         req.nodeId, req.taskId, req.checkpointId, req.checkpointVer, req.transId);

  // register to the stream task done map, if all tasks has sent this kinds of message, start the checkpoint trans.
  streamMutexLock(&execInfo.lock);

  SStreamObj *pStream = NULL;
  int32_t code = mndGetStreamObj(pMnode, req.streamId, &pStream);
  if (pStream == NULL || code != 0) {
    mWarn("failed to find the stream:0x%" PRIx64 ", not handle checkpoint-report, try to acquire in buf", req.streamId);

    // not in meta-store yet, try to acquire the task in exec buffer
    // the checkpoint req arrives too soon before the completion of the create stream trans.
    STaskId id = {.streamId = req.streamId, .taskId = req.taskId};
    void   *p = taosHashGet(execInfo.pTaskMap, &id, sizeof(id));
    if (p == NULL) {
      mError("failed to find the stream:0x%" PRIx64 " in buf, not handle the checkpoint-report", req.streamId);
      terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
      streamMutexUnlock(&execInfo.lock);
      return terrno;
    } else {
      mDebug("s-task:0x%" PRIx64 "-0x%x in buf not in mnode/meta, create stream trans may not complete yet",
             req.streamId, req.taskId);
    }
  }

  int32_t numOfTasks = (pStream == NULL) ? 0 : mndGetNumOfStreamTasks(pStream);

  SArray **pReqTaskList = (SArray **)taosHashGet(execInfo.pChkptStreams, &req.streamId, sizeof(req.streamId));
  if (pReqTaskList == NULL) {
    SArray *pList = taosArrayInit(4, sizeof(STaskChkptInfo));
    doAddReportStreamTask(pList, &req);

    code = taosHashPut(execInfo.pChkptStreams, &req.streamId, sizeof(req.streamId), &pList, POINTER_BYTES);
    if (code) {
      mError("stream:0x%"PRIx64 " failed to put into checkpoint stream", req.streamId);
    }

    pReqTaskList = (SArray **)taosHashGet(execInfo.pChkptStreams, &req.streamId, sizeof(req.streamId));
  } else {
    doAddReportStreamTask(*pReqTaskList, &req);
  }

  int32_t total = taosArrayGetSize(*pReqTaskList);
  if (total == numOfTasks) {  // all tasks has send the reqs
    mInfo("stream:0x%" PRIx64 " %s all %d tasks send checkpoint-report, checkpoint meta-info for checkpointId:%" PRId64
          " will be issued soon",
          req.streamId, pStream->name, total, req.checkpointId);
  }

  if (pStream != NULL) {
    mndReleaseStream(pMnode, pStream);
  }

  streamMutexUnlock(&execInfo.lock);

  doSendQuickRsp(&pReq->info, sizeof(SMStreamUpdateChkptRsp), req.nodeId, TSDB_CODE_SUCCESS);
  return code;
}

static int64_t getConsensusId(int64_t streamId, int32_t numOfTasks, int32_t* pExistedTasks, bool *pAllSame) {
  int32_t num = 0;
  int64_t chkId = INT64_MAX;
  *pExistedTasks = 0;
  *pAllSame = true;

  for(int32_t i = 0; i < taosArrayGetSize(execInfo.pTaskList); ++i) {
    STaskId* p = taosArrayGet(execInfo.pTaskList, i);
    if (p->streamId != streamId) {
      continue;
    }

    num += 1;
    STaskStatusEntry* pe = taosHashGet(execInfo.pTaskMap, p, sizeof(*p));
    if (chkId > pe->checkpointInfo.latestId) {
      if (chkId != INT64_MAX) {
        *pAllSame = false;
      }
      chkId = pe->checkpointInfo.latestId;
    }
  }

  *pExistedTasks = num;
  if (num < numOfTasks) { // not all task send info to mnode through hbMsg, no valid checkpoint Id
    return -1;
  }

  return chkId;
}

static void doSendQuickRsp(SRpcHandleInfo *pInfo, int32_t msgSize, int32_t vgId, int32_t code) {
  SRpcMsg rsp = {.code = code, .info = *pInfo, .contLen = msgSize};
  rsp.pCont = rpcMallocCont(rsp.contLen);
  SMsgHead *pHead = rsp.pCont;
  pHead->vgId = htonl(vgId);

  tmsgSendRsp(&rsp);
  pInfo->handle = NULL;  // disable auto rsp
}

int32_t mndProcessConsensusInTmr(SRpcMsg *pMsg) {
  SMnode *pMnode = pMsg->info.node;
  int64_t now = taosGetTimestampMs();
  SArray *pStreamList = taosArrayInit(4, sizeof(int64_t));

  mDebug("start to process consensus-checkpointId in tmr");

  bool    allReady = true;
  SArray *pNodeSnapshot = NULL;

  int32_t code = mndTakeVgroupSnapshot(pMnode, &allReady, &pNodeSnapshot);
  taosArrayDestroy(pNodeSnapshot);
  if (code) {
    mError("failed to get the vgroup snapshot, ignore it and continue");
  }

  if (!allReady) {
    mWarn("not all vnodes are ready, end to process the consensus-checkpointId in tmr process");
    taosArrayDestroy(pStreamList);
    return 0;
  }

  streamMutexLock(&execInfo.lock);

  void *pIter = NULL;
  while ((pIter = taosHashIterate(execInfo.pStreamConsensus, pIter)) != NULL) {
    SCheckpointConsensusInfo *pInfo = (SCheckpointConsensusInfo *)pIter;

    int64_t streamId = -1;
    int32_t num = taosArrayGetSize(pInfo->pTaskList);
    SArray *pList = taosArrayInit(4, sizeof(int32_t));

    SStreamObj *pStream = NULL;
    code = mndGetStreamObj(pMnode, pInfo->streamId, &pStream);
    if (pStream == NULL || code != 0) {  // stream has been dropped already
      mDebug("stream:0x%" PRIx64 " dropped already, continue", pInfo->streamId);
      taosArrayDestroy(pList);
      continue;
    }

    for (int32_t j = 0; j < num; ++j) {
      SCheckpointConsensusEntry *pe = taosArrayGet(pInfo->pTaskList, j);
      streamId = pe->req.streamId;

      int32_t existed = 0;
      bool    allSame = true;
      int64_t chkId = getConsensusId(pe->req.streamId, pInfo->numOfTasks, &existed, &allSame);
      if (chkId == -1) {
        mDebug("not all(%d/%d) task(s) send hbMsg yet, wait for a while and check again, s-task:0x%x", existed,
               pInfo->numOfTasks, pe->req.taskId);
        break;
      }

      if (((now - pe->ts) >= 10 * 1000) || allSame) {
        mDebug("s-task:0x%x sendTs:%" PRId64 " wait %.2fs and all tasks have same checkpointId", pe->req.taskId,
               pe->req.startTs, (now - pe->ts) / 1000.0);
        ASSERT(chkId <= pe->req.checkpointId);
        code = mndCreateSetConsensusChkptIdTrans(pMnode, pStream, pe->req.taskId, chkId, pe->req.startTs);
        if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
          mError("failed to create consensus-checkpoint trans, stream:0x%" PRIx64, pStream->uid);
        }

        void* p = taosArrayPush(pList, &pe->req.taskId);
        if (p == NULL) {
          mError("failed to put into task list, taskId:0x%x", pe->req.taskId);
        }
        streamId = pe->req.streamId;
      } else {
        mDebug("s-task:0x%x sendTs:%" PRId64 " wait %.2fs already, wait for next round to check", pe->req.taskId,
               pe->req.startTs, (now - pe->ts) / 1000.0);
      }
    }

    mndReleaseStream(pMnode, pStream);

    if (taosArrayGetSize(pList) > 0) {
      for (int32_t i = 0; i < taosArrayGetSize(pList); ++i) {
        int32_t *taskId = taosArrayGet(pList, i);
        for (int32_t k = 0; k < taosArrayGetSize(pInfo->pTaskList); ++k) {
          SCheckpointConsensusEntry *pe = taosArrayGet(pInfo->pTaskList, k);
          if (pe->req.taskId == *taskId) {
            taosArrayRemove(pInfo->pTaskList, k);
            break;
          }
        }
      }
    }

    taosArrayDestroy(pList);

    if (taosArrayGetSize(pInfo->pTaskList) == 0) {
      mndClearConsensusRspEntry(pInfo);
      ASSERT(streamId != -1);
      void* p = taosArrayPush(pStreamList, &streamId);
      if (p == NULL) {
        mError("failed to put into stream list, stream:0x%" PRIx64, streamId);
      }
    }
  }

  for (int32_t i = 0; i < taosArrayGetSize(pStreamList); ++i) {
    int64_t *pStreamId = (int64_t *)taosArrayGet(pStreamList, i);
    code = mndClearConsensusCheckpointId(execInfo.pStreamConsensus, *pStreamId);
  }

  streamMutexUnlock(&execInfo.lock);

  taosArrayDestroy(pStreamList);
  mDebug("end to process consensus-checkpointId in tmr");
  return code;
}

static int32_t mndProcessCreateStreamReqFromMNode(SRpcMsg *pReq) {
  int32_t code = mndProcessCreateStreamReq(pReq);
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    pReq->info.rsp = rpcMallocCont(1);
    pReq->info.rspLen = 1;
    pReq->info.noResp = false;
    pReq->code = code;
  }
  return code;
}

static int32_t mndProcessDropStreamReqFromMNode(SRpcMsg *pReq) {
  int32_t code = mndProcessDropStreamReq(pReq);
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    pReq->info.rsp = rpcMallocCont(1);
    pReq->info.rspLen = 1;
    pReq->info.noResp = false;
    pReq->code = code;
  }
  return code;
}

void mndInitStreamExecInfo(SMnode *pMnode, SStreamExecInfo *pExecInfo) {
  if (pExecInfo->initTaskList || pMnode == NULL) {
    return;
  }

  addAllStreamTasksIntoBuf(pMnode, pExecInfo);
  pExecInfo->initTaskList = true;
}

void addAllStreamTasksIntoBuf(SMnode *pMnode, SStreamExecInfo *pExecInfo) {
  SSdb       *pSdb = pMnode->pSdb;
  SStreamObj *pStream = NULL;
  void       *pIter = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) {
      break;
    }

    saveTaskAndNodeInfoIntoBuf(pStream, pExecInfo);
    sdbRelease(pSdb, pStream);
  }
}

int32_t mndCreateStreamChkptInfoUpdateTrans(SMnode *pMnode, SStreamObj *pStream, SArray *pChkptInfoList) {
  STrans *pTrans = NULL;
  int32_t code = doCreateTrans(pMnode, pStream, NULL, TRN_CONFLICT_NOTHING, MND_STREAM_CHKPT_UPDATE_NAME,
                                 "update checkpoint-info", &pTrans);
  if (pTrans == NULL || code) {
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  code = mndStreamRegisterTrans(pTrans, MND_STREAM_CHKPT_UPDATE_NAME, pStream->uid);
  if (code){
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndStreamSetUpdateChkptAction(pMnode, pTrans, pStream);
  if (code) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndPersistTransLog(pStream, pTrans, SDB_STATUS_READY);
  if (code) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndTransPrepare(pMnode, pTrans);
  if (code) {
    mError("trans:%d, failed to prepare update checkpoint-info meta trans since %s", pTrans->id, terrstr());
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}
