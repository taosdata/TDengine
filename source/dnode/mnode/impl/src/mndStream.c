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
#include "osMemory.h"
#include "parser.h"
#include "taoserror.h"
#include "tmisce.h"
#include "tname.h"

#define MND_STREAM_MAX_NUM 60

typedef struct {
  int8_t placeHolder;  // // to fix windows compile error, define place holder
} SMStreamNodeCheckMsg;

static int32_t  mndNodeCheckSentinel = 0;
SStreamRuntime  mStreamMgmt;

static int32_t mndStreamActionInsert(SSdb *pSdb, SStreamObj *pStream);
static int32_t mndStreamActionDelete(SSdb *pSdb, SStreamObj *pStream);
static int32_t mndStreamActionUpdate(SSdb *pSdb, SStreamObj *pOldStream, SStreamObj *pNewStream);
static int32_t mndProcessDropStreamReq(SRpcMsg *pReq);

static int32_t mndProcessCreateStreamReqFromMNode(SRpcMsg *pReq);
static int32_t mndProcessDropStreamReqFromMNode(SRpcMsg *pReq);

static int32_t mndRetrieveStream(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextStream(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveStreamTask(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextStreamTask(SMnode *pMnode, void *pIter);
static int32_t mndProcessPauseStreamReq(SRpcMsg *pReq);
static int32_t mndProcessResumeStreamReq(SRpcMsg *pReq);
static int32_t mndProcessResetStreamReq(SRpcMsg *pReq);
static int32_t refreshNodeListFromExistedStreams(SMnode *pMnode, SArray *pNodeList);
static void    doSendQuickRsp(SRpcHandleInfo *pInfo, int32_t msgSize, int32_t vgId, int32_t code);

static SSdbRow *mndStreamActionDecode(SSdbRaw *pRaw);

SSdbRaw       *mndStreamSeqActionEncode(SStreamObj *pStream);
SSdbRow       *mndStreamSeqActionDecode(SSdbRaw *pRaw);
static int32_t mndStreamSeqActionInsert(SSdb *pSdb, SStreamSeq *pStream);
static int32_t mndStreamSeqActionDelete(SSdb *pSdb, SStreamSeq *pStream);
static int32_t mndStreamSeqActionUpdate(SSdb *pSdb, SStreamSeq *pOldStream, SStreamSeq *pNewStream);

#ifdef NEW_STREAM
static int32_t mndProcessCreateStreamReq(SRpcMsg *pReq);
#endif

void mndCleanupStream(SMnode *pMnode) {
  taosArrayDestroy(execInfo.pTaskList);
  taosArrayDestroy(execInfo.pNodeList);
  taosArrayDestroy(execInfo.pKilledChkptTrans);
  taosHashCleanup(execInfo.pTaskMap);
  taosHashCleanup(execInfo.transMgmt.pDBTrans);
  taosHashCleanup(execInfo.pTransferStateStreams);
  taosHashCleanup(execInfo.pChkptStreams);
  taosHashCleanup(execInfo.pStreamConsensus);
  (void)taosThreadMutexDestroy(&execInfo.lock);
  mDebug("mnd stream exec info cleanup");
}

SSdbRow *mndStreamActionDecode(SSdbRaw *pRaw) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SSdbRow    *pRow = NULL;
  SStreamObj *pStream = NULL;
  void       *buf = NULL;
  int8_t      sver = 0;
  int32_t     tlen;
  int32_t     dataPos = 0;

  code = sdbGetRawSoftVer(pRaw, &sver);
  TSDB_CHECK_CODE(code, lino, _over);

  if (sver < 1 || sver > MND_STREAM_VER_NUMBER) {
    mError("stream read invalid ver, data ver: %d, curr ver: %d", sver, MND_STREAM_VER_NUMBER);
    goto _over;
  }

  pRow = sdbAllocRow(sizeof(SStreamObj));
  TSDB_CHECK_NULL(pRow, code, lino, _over, terrno);

  pStream = sdbGetRowObj(pRow);
  TSDB_CHECK_NULL(pStream, code, lino, _over, terrno);

  SDB_GET_INT32(pRaw, dataPos, &tlen, _over);

  buf = taosMemoryMalloc(tlen + 1);
  TSDB_CHECK_NULL(buf, code, lino, _over, terrno);

  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, _over);

  SDecoder decoder;
  tDecoderInit(&decoder, buf, tlen + 1);
  code = tDecodeSStreamObj(&decoder, pStream, sver);
  tDecoderClear(&decoder);

  if (code < 0) {
    tFreeStreamObj(pStream);
  }

_over:
  taosMemoryFreeClear(buf);

  if (code != TSDB_CODE_SUCCESS) {
    char *p = (pStream == NULL) ? "null" : pStream->name;
    mError("stream:%s, failed to decode from raw:%p since %s at:%d", p, pRaw, tstrerror(code), lino);
    taosMemoryFreeClear(pRow);

    terrno = code;
    return NULL;
  } else {
    mTrace("stream:%s, decode from raw:%p, row:%p, checkpoint:%" PRId64, pStream->name, pRaw, pStream,
           pStream->checkpointId);

    terrno = 0;
    return pRow;
  }
}

static int32_t mndStreamActionInsert(SSdb *pSdb, SStreamObj *pStream) {
  mTrace("stream:%s, perform insert action", pStream->name);
  return 0;
}

static int32_t mndStreamActionDelete(SSdb *pSdb, SStreamObj *pStream) {
  mInfo("stream:%s, perform delete action", pStream->name);
  taosWLockLatch(&pStream->lock);
  tFreeStreamObj(pStream);
  taosWUnLockLatch(&pStream->lock);
  return 0;
}

static int32_t mndStreamActionUpdate(SSdb *pSdb, SStreamObj *pOldStream, SStreamObj *pNewStream) {
  mTrace("stream:%s, perform update action", pOldStream->name);
  (void)atomic_exchange_32(&pOldStream->version, pNewStream->version);

  taosWLockLatch(&pOldStream->lock);

  pOldStream->status = pNewStream->status;
  pOldStream->updateTime = pNewStream->updateTime;
  pOldStream->checkpointId = pNewStream->checkpointId;
  pOldStream->checkpointFreq = pNewStream->checkpointFreq;
  if (pOldStream->pTaskList == NULL) {
    pOldStream->pTaskList = pNewStream->pTaskList;
    pNewStream->pTaskList = NULL;
  }
  if (pOldStream->pHTaskList == NULL) {
    pOldStream->pHTaskList = pNewStream->pHTaskList;
    pNewStream->pHTaskList = NULL;
  }
  taosWUnLockLatch(&pOldStream->lock);
  return 0;
}

int32_t mndAcquireStream(SMnode *pMnode, char *streamName, SStreamObj **pStream) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
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

SSdbRaw *mndStreamSeqActionEncode(SStreamObj *pStream) { return NULL; }
SSdbRow *mndStreamSeqActionDecode(SSdbRaw *pRaw) { return NULL; }
int32_t  mndStreamSeqActionInsert(SSdb *pSdb, SStreamSeq *pStream) { return 0; }
int32_t  mndStreamSeqActionDelete(SSdb *pSdb, SStreamSeq *pStream) { return 0; }
int32_t  mndStreamSeqActionUpdate(SSdb *pSdb, SStreamSeq *pOldStream, SStreamSeq *pNewStream) { return 0; }

static int32_t createSchemaByFields(const SArray *pFields, SSchemaWrapper *pWrapper) {
  pWrapper->nCols = taosArrayGetSize(pFields);
  pWrapper->pSchema = taosMemoryCalloc(pWrapper->nCols, sizeof(SSchema));
  if (NULL == pWrapper->pSchema) {
    return terrno;
  }

  int32_t index = 0;
  for (int32_t i = 0; i < pWrapper->nCols; i++) {
    SField *pField = (SField *)taosArrayGet(pFields, i);
    if (pField == NULL) {
      return terrno;
    }

    if (TSDB_DATA_TYPE_NULL == pField->type) {
      pWrapper->pSchema[index].type = TSDB_DATA_TYPE_VARCHAR;
      pWrapper->pSchema[index].bytes = VARSTR_HEADER_SIZE;
    } else {
      pWrapper->pSchema[index].type = pField->type;
      pWrapper->pSchema[index].bytes = pField->bytes;
    }
    pWrapper->pSchema[index].colId = index + 1;
    tstrncpy(pWrapper->pSchema[index].name, pField->name, sizeof(pWrapper->pSchema[index].name));
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

static int32_t mndStreamBuildObj(SMnode *pMnode, SStreamObj *pObj, SCMCreateStreamReq *pCreate) {
  SNode      *pAst = NULL;
  SQueryPlan *pPlan = NULL;
  int32_t     code = 0;

  pObj->pCreate = pCreate;

  pObj->createTime = taosGetTimestampMs();
  pObj->updateTime = pObj->createTime;
  pObj->version = 1;

  // deserialize ast
  if ((code = nodesStringToNode(pObj->ast, &pAst)) < 0) {
    goto _ERR;
  }

  // create output schema
  if ((code = createSchemaByFields(pCreate->pCols, &pObj->outputSchema)) != TSDB_CODE_SUCCESS) {
    goto _ERR;
  }

  // save physcial plan
  if ((code = nodesNodeToString((SNode *)pPlan, false, &pObj->physicalPlan, NULL)) != 0) {
    goto _ERR;
  }


_ERR:

  return code;
}

int32_t mndPersistTaskDeployReq(STrans *pTrans, SStreamTask *pTask) {
  SEncoder encoder;
  tEncoderInit(&encoder, NULL, 0);

  if (pTask->ver < SSTREAM_TASK_SUBTABLE_CHANGED_VER) {
    pTask->ver = SSTREAM_TASK_VER;
  }

  int32_t code = tEncodeStreamTask(&encoder, pTask);
  if (code == -1) {
    tEncoderClear(&encoder);
    return TSDB_CODE_INVALID_MSG;
  }

  int32_t size = encoder.pos;
  int32_t tlen = sizeof(SMsgHead) + size;
  tEncoderClear(&encoder);

  void *buf = taosMemoryCalloc(1, tlen);
  if (buf == NULL) {
    return terrno;
  }

  ((SMsgHead *)buf)->vgId = htonl(pTask->info.nodeId);

  void *abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncoderInit(&encoder, abuf, size);
  code = tEncodeStreamTask(&encoder, pTask);
  tEncoderClear(&encoder);

  if (code != 0) {
    mError("failed to encode stream task, code:%s", tstrerror(code));
    taosMemoryFree(buf);
    return code;
  }

  code = setTransAction(pTrans, buf, tlen, TDMT_STREAM_TASK_DEPLOY, &pTask->info.epSet, 0,
                        TSDB_CODE_VND_INVALID_VGROUP_ID);
  if (code) {
    taosMemoryFree(buf);
  }

  return code;
}

int32_t mndPersistStreamTasks(STrans *pTrans, SStreamObj *pStream) {
  SStreamTaskIter *pIter = NULL;
  int32_t          code = createStreamTaskIter(pStream, &pIter);
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
  if (pStream->conf.fillHistory || (pStream->conf.trigger == STREAM_TRIGGER_CONTINUOUS_WINDOW_CLOSE)) {
    int32_t level = taosArrayGetSize(pStream->pHTaskList);

    for (int32_t i = 0; i < level; i++) {
      SArray *pLevel = taosArrayGetP(pStream->pHTaskList, i);

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

static int32_t mndStreamCreateOutStb(SMnode *pMnode, STrans *pTrans, const SStreamObj *pStream, const char *user) {
  SStbObj *pStb = NULL;
  SDbObj  *pDb = NULL;
  int32_t  code = 0;
  int32_t  lino = 0;

  SMCreateStbReq createReq = {0};
  tstrncpy(createReq.name, pStream->targetSTbName, TSDB_TABLE_FNAME_LEN);
  createReq.numOfColumns = pStream->outputSchema.nCols;
  createReq.numOfTags = 1;  // group id
  createReq.pColumns = taosArrayInit_s(sizeof(SFieldWithOptions), createReq.numOfColumns);
  TSDB_CHECK_NULL(createReq.pColumns, code, lino, _OVER, terrno);

  // build fields
  for (int32_t i = 0; i < createReq.numOfColumns; i++) {
    SFieldWithOptions *pField = taosArrayGet(createReq.pColumns, i);
    TSDB_CHECK_NULL(pField, code, lino, _OVER, terrno);

    tstrncpy(pField->name, pStream->outputSchema.pSchema[i].name, TSDB_COL_NAME_LEN);
    pField->flags = pStream->outputSchema.pSchema[i].flags;
    pField->type = pStream->outputSchema.pSchema[i].type;
    pField->bytes = pStream->outputSchema.pSchema[i].bytes;
    pField->compress = createDefaultColCmprByType(pField->type);
    if (IS_DECIMAL_TYPE(pField->type)) {
      uint8_t prec = 0, scale = 0;
      extractDecimalTypeInfoFromBytes(&pField->bytes, &prec, &scale);
      pField->typeMod = decimalCalcTypeMod(prec, scale);
    }
  }

  if (pStream->tagSchema.nCols == 0) {
    createReq.numOfTags = 1;
    createReq.pTags = taosArrayInit_s(sizeof(SField), 1);
    TSDB_CHECK_NULL(createReq.pTags, code, lino, _OVER, terrno);

    // build tags
    SField *pField = taosArrayGet(createReq.pTags, 0);
    TSDB_CHECK_NULL(pField, code, lino, _OVER, terrno);

    tstrncpy(pField->name, "group_id", sizeof(pField->name));
    pField->type = TSDB_DATA_TYPE_UBIGINT;
    pField->flags = 0;
    pField->bytes = 8;
  } else {
    createReq.numOfTags = pStream->tagSchema.nCols;
    createReq.pTags = taosArrayInit_s(sizeof(SField), createReq.numOfTags);
    TSDB_CHECK_NULL(createReq.pTags, code, lino, _OVER, terrno);

    for (int32_t i = 0; i < createReq.numOfTags; i++) {
      SField *pField = taosArrayGet(createReq.pTags, i);
      if (pField == NULL) {
        continue;
      }

      pField->bytes = pStream->tagSchema.pSchema[i].bytes;
      pField->flags = pStream->tagSchema.pSchema[i].flags;
      pField->type = pStream->tagSchema.pSchema[i].type;
      tstrncpy(pField->name, pStream->tagSchema.pSchema[i].name, TSDB_COL_NAME_LEN);
    }
  }

  if ((code = mndCheckCreateStbReq(&createReq)) != 0) {
    goto _OVER;
  }

  pStb = mndAcquireStb(pMnode, createReq.name);
  if (pStb != NULL) {
    code = TSDB_CODE_MND_STB_ALREADY_EXIST;
    goto _OVER;
  }

  pDb = mndAcquireDbByStb(pMnode, createReq.name);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto _OVER;
  }

  int32_t numOfStbs = -1;
  if (mndGetNumOfStbs(pMnode, pDb->name, &numOfStbs) != 0) {
    goto _OVER;
  }

  if (pDb->cfg.numOfStables == 1 && numOfStbs != 0) {
    code = TSDB_CODE_MND_SINGLE_STB_MODE_DB;
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
  return code;

_OVER:
  tFreeSMCreateStbReq(&createReq);
  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);

  mDebug("stream:%s failed to create dst stable:%s, line:%d code:%s", pStream->name, pStream->targetSTbName, lino,
         tstrerror(code));
  return code;
}

static int32_t mndStreamValidateCreate(SMnode *pMnode, char* pUser, SCMCreateStreamReq* pCreate) {
  int32_t code = 0, lino = 0;
  int64_t streamId = pCreate->streamId;

  if (pCreate->triggerDB) {
    code = mndCheckDbPrivilegeByName(pMnode, pUser, MND_OPER_READ_DB, pCreate->triggerDB);
    if (code) {
      mstError("user %s failed to create stream %s using trigger db %s since %s", pUser, pCreate->name, pCreate->triggerDB, tstrerror(code));
    }
    TSDB_CHECK_CODE(code, lino, _OVER);
  }

  if (pCreate->sourceDB) {
    code = mndCheckDbPrivilegeByName(pMnode, pUser, MND_OPER_READ_DB, pCreate->sourceDB);
    if (code) {
      mstError("user %s failed to create stream %s using source db %s since %s", pUser, pCreate->name, pCreate->sourceDB, tstrerror(code));
    }
    TSDB_CHECK_CODE(code, lino, _OVER);
  }

  if (pCreate->outDB) {
    code = mndCheckDbPrivilegeByName(pMnode, pUser, MND_OPER_WRITE_DB, pCreate->outDB);
    if (code) {
      mstError("user %s failed to create stream %s using out db %s since %s", pUser, pCreate->name, pCreate->outDB, tstrerror(code));
    }
    TSDB_CHECK_CODE(code, lino, _OVER);
  }

  int32_t streamNum = sdbGetSize(pMnode->pSdb, SDB_STREAM);
  if (streamNum > MND_STREAM_MAX_NUM) {
    code = TSDB_CODE_MND_TOO_MANY_STREAMS;
    mstError("failed to create stream %s since %s, stream number:%d", pCreate->name, tstrerror(code), streamNum);
    return code;
  }

_OVER:

  return code;
}

static void *notifyAddrDup(void *p) { return taosStrdup((char *)p); }

static int32_t addStreamTaskNotifyInfo(const SCMCreateStreamReq *createReq, const SStreamObj *pStream,
                                       SStreamTask *pTask) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(createReq, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pTask, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pTask->notifyInfo.pNotifyAddrUrls = taosArrayDup(createReq->pNotifyAddrUrls, notifyAddrDup);
  TSDB_CHECK_NULL(pTask->notifyInfo.pNotifyAddrUrls, code, lino, _end, terrno);
  pTask->notifyInfo.notifyEventTypes = createReq->notifyEventTypes;
  pTask->notifyInfo.notifyErrorHandle = createReq->notifyErrorHandle;
  pTask->notifyInfo.streamName = taosStrdup(mndGetDbStr(createReq->name));
  TSDB_CHECK_NULL(pTask->notifyInfo.streamName, code, lino, _end, terrno);
  pTask->notifyInfo.stbFullName = taosStrdup(createReq->targetStbFullName);
  TSDB_CHECK_NULL(pTask->notifyInfo.stbFullName, code, lino, _end, terrno);
  pTask->notifyInfo.pSchemaWrapper = tCloneSSchemaWrapper(&pStream->outputSchema);
  TSDB_CHECK_NULL(pTask->notifyInfo.pSchemaWrapper, code, lino, _end, terrno);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    mError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t addStreamNotifyInfo(SCMCreateStreamReq *createReq, SStreamObj *pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t level = 0;
  int32_t nTasks = 0;
  SArray *pLevel = NULL;

  TSDB_CHECK_NULL(createReq, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pStream, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (taosArrayGetSize(createReq->pNotifyAddrUrls) == 0) {
    goto _end;
  }

  level = taosArrayGetSize(pStream->pTaskList);
  for (int32_t i = 0; i < level; ++i) {
    pLevel = taosArrayGetP(pStream->pTaskList, i);
    nTasks = taosArrayGetSize(pLevel);
    for (int32_t j = 0; j < nTasks; ++j) {
      code = addStreamTaskNotifyInfo(createReq, pStream, taosArrayGetP(pLevel, j));
      TSDB_CHECK_CODE(code, lino, _end);
    }
  }

  if (pStream->conf.fillHistory && createReq->notifyHistory) {
    level = taosArrayGetSize(pStream->pHTaskList);
    for (int32_t i = 0; i < level; ++i) {
      pLevel = taosArrayGetP(pStream->pHTaskList, i);
      nTasks = taosArrayGetSize(pLevel);
      for (int32_t j = 0; j < nTasks; ++j) {
        code = addStreamTaskNotifyInfo(createReq, pStream, taosArrayGetP(pLevel, j));
        TSDB_CHECK_CODE(code, lino, _end);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    mError("%s for stream %s failed at line %d since %s", __func__, pStream->name, lino, tstrerror(code));
  }
  return code;
}


static int32_t mndProcessStopStreamReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  SStreamObj      *pStream = NULL;
  int32_t          code = 0;
  SMPauseStreamReq pauseReq = {0};

  if (tDeserializeSMPauseStreamReq(pReq->pCont, pReq->contLen, &pauseReq) < 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  code = mndAcquireStream(pMnode, pauseReq.name, &pStream);
  if (pStream == NULL || code != 0) {
    if (pauseReq.igNotExists) {
      mInfo("stream:%s, not exist, not restart stream", pauseReq.name);
      return 0;
    } else {
      mError("stream:%s not exist, failed to restart stream", pauseReq.name);
      TAOS_RETURN(TSDB_CODE_MND_STREAM_NOT_EXIST);
    }
  }

  mInfo("stream:%s,%" PRId64 " start to restart stream", pauseReq.name, pStream->uid);
  if ((code = mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->targetDb)) != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  STrans *pTrans = NULL;
  code = doCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_NOTHING, MND_STREAM_STOP_NAME, "stop the stream",
                       &pTrans);
  if (pTrans == NULL || code) {
    mError("stream:%s failed to stop stream since %s", pauseReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  // if nodeUpdate happened, not send pause trans
  code = mndStreamSetStopAction(pMnode, pTrans, pStream);
  if (code) {
    mError("stream:%s, failed to restart task since %s", pauseReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("trans:%d, failed to prepare restart stream trans since %s", pTrans->id, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
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

int32_t mndDropStreamByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  int32_t code = 0;

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
        TAOS_RETURN(TSDB_CODE_MND_STREAM_MUST_BE_DELETED);
      } else {
        // drop the stream obj in execInfo
        removeStreamTasksInBuf(pStream, &execInfo);

        code = mndPersistTransLog(pStream, pTrans, SDB_STATUS_DROPPED);
        if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
          sdbRelease(pSdb, pStream);
          sdbCancelFetch(pSdb, pIter);
          return code;
        }
      }
    }

    sdbRelease(pSdb, pStream);
  }

  return 0;
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
  sdbCancelFetchByType(pSdb, pIter, SDB_STREAM);
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

    int32_t precision = TSDB_TIME_PRECISION_MILLI;
    SDbObj *pSourceDb = mndAcquireDb(pMnode, pStream->sourceDb);
    if (pSourceDb != NULL) {
      precision = pSourceDb->cfg.precision;
      mndReleaseDb(pMnode, pSourceDb);
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

      code = setTaskAttrInResBlock(pStream, pTask, pBlock, numOfRows, precision);
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
  sdbCancelFetchByType(pSdb, pIter, SDB_STREAM);
}

static int32_t mndProcessResetStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  int32_t     code = 0;

  if ((code = grantCheckExpire(TSDB_GRANT_STREAMS)) < 0) {
    return code;
  }

  SMResetStreamReq resetReq = {0};
  if (tDeserializeSMResetStreamReq(pReq->pCont, pReq->contLen, &resetReq) < 0) {
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  mDebug("recv reset stream req, stream:%s", resetReq.name);

  code = mndAcquireStream(pMnode, resetReq.name, &pStream);
  if (pStream == NULL || code != 0) {
    if (resetReq.igNotExists) {
      mInfo("stream:%s, not exist, not pause stream", resetReq.name);
      return 0;
    } else {
      mError("stream:%s not exist, failed to pause stream", resetReq.name);
      TAOS_RETURN(TSDB_CODE_MND_STREAM_NOT_EXIST);
    }
  }

  //todo(liao hao jun)
  return TSDB_CODE_ACTION_IN_PROGRESS;
}

static int32_t refreshNodeListFromExistedStreams(SMnode *pMnode, SArray *pNodeList) {
  SSdb       *pSdb = pMnode->pSdb;
  SStreamObj *pStream = NULL;
  void       *pIter = NULL;
  int32_t     code = 0;

  mDebug("start to refresh node list by existed streams");

  SHashObj *pHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (pHash == NULL) {
    return terrno;
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

      SNodeEntry entry = {.hbTimestamp = -1, .nodeId = pTask->info.nodeId, .lastHbMsgId = -1};
      epsetAssign(&entry.epset, &pTask->info.epSet);
      int32_t ret = taosHashPut(pHash, &entry.nodeId, sizeof(entry.nodeId), &entry, sizeof(entry));
      if (ret != 0 && ret != TSDB_CODE_DUP_KEY) {
        mError("failed to put entry into hash map, nodeId:%d, code:%s", entry.nodeId, tstrerror(code));
      }
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
        code = terrno;
      }
      continue;
    }

    char    buf[256] = {0};
    int32_t ret = epsetToStr(&pEntry->epset, buf, tListLen(buf));  // ignore this error since it is only for log file
    if (ret != 0) {                                                // print error and continue
      mError("failed to convert epset to str, code:%s", tstrerror(ret));
    }

    mDebug("extract nodeInfo from stream obj, nodeId:%d, %s", pEntry->nodeId, buf);
  }

  taosHashCleanup(pHash);

  mDebug("numOfvNodes:%d get after extracting nodeInfo from all streams", (int32_t)taosArrayGetSize(pNodeList));
  return code;
}

static void addAllDbsIntoHashmap(SHashObj *pDBMap, SSdb *pSdb) {
  void   *pIter = NULL;
  int32_t code = 0;
  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) {
      break;
    }

    code = taosHashPut(pDBMap, pVgroup->dbName, strlen(pVgroup->dbName), NULL, 0);
    sdbRelease(pSdb, pVgroup);

    if (code == 0) {
      int32_t size = taosHashGetSize(pDBMap);
      mDebug("add Db:%s into Dbs list (total:%d) for kill checkpoint trans", pVgroup->dbName, size);
    }
  }
}

static void doSendQuickRsp(SRpcHandleInfo *pInfo, int32_t msgSize, int32_t vgId, int32_t code) {
  SRpcMsg rsp = {.code = code, .info = *pInfo, .contLen = msgSize};
  rsp.pCont = rpcMallocCont(rsp.contLen);
  if (rsp.pCont != NULL) {
    SMsgHead *pHead = rsp.pCont;
    pHead->vgId = htonl(vgId);

    tmsgSendRsp(&rsp);
    pInfo->handle = NULL;  // disable auto rsp
  }
}

static int32_t doCleanReqList(SArray *pList, SCheckpointConsensusInfo *pInfo) {
  int32_t alreadySend = taosArrayGetSize(pList);

  for (int32_t i = 0; i < alreadySend; ++i) {
    int32_t *taskId = taosArrayGet(pList, i);
    if (taskId == NULL) {
      continue;
    }

    for (int32_t k = 0; k < taosArrayGetSize(pInfo->pTaskList); ++k) {
      SCheckpointConsensusEntry *pe = taosArrayGet(pInfo->pTaskList, k);
      if ((pe != NULL) && (pe->req.taskId == *taskId)) {
        taosArrayRemove(pInfo->pTaskList, k);
        break;
      }
    }
  }

  return alreadySend;
}

void mndInitStreamExecInfo(SMnode *pMnode, SStreamExecInfo *pExecInfo) {
  if (pExecInfo->initTaskList || pMnode == NULL) {
    return;
  }

  addAllStreamTasksIntoBuf(pMnode, pExecInfo);
  pExecInfo->initTaskList = true;
}

void mndStreamResetInitTaskListLoadFlag() {
  mInfo("reset task list buffer init flag for leader");
  execInfo.initTaskList = false;
}

void mndUpdateStreamExecInfoRole(SMnode *pMnode, int32_t role) {
  execInfo.switchFromFollower = false;

  if (execInfo.role == NODE_ROLE_UNINIT) {
    execInfo.role = role;
    if (role == NODE_ROLE_LEADER) {
      mInfo("init mnode is set to leader");
    } else {
      mInfo("init mnode is set to follower");
    }
  } else {
    if (role == NODE_ROLE_LEADER) {
      if (execInfo.role == NODE_ROLE_FOLLOWER) {
        execInfo.role = role;
        execInfo.switchFromFollower = true;
        mInfo("mnode switch to be leader from follower");
      } else {
        mInfo("mnode remain to be leader, do nothing");
      }
    } else {  // follower's
      if (execInfo.role == NODE_ROLE_LEADER) {
        execInfo.role = role;
        mInfo("mnode switch to be follower from leader");
      } else {
        mInfo("mnode remain to be follower, do nothing");
      }
    }
  }
}

#ifdef NEW_STREAM

bool mndStreamActionDequeue(SStreamActionQ* pQueue, SStreamQNode **param) {
  while (0 == atomic_load_64(&pQueue->qRemainNum)) {
    return false;
  }

  SStreamQNode *orig = pQueue->head;

  SStreamQNode *node = pQueue->head->next;
  pQueue->head = pQueue->head->next;

  *param = node;

  atomic_sub_fetch_64(&pQueue->qRemainNum, 1);

  return true;
}

void mndStreamActionEnqueue(SStreamActionQ* pQueue, SStreamQNode* param) {
  pQueue->tail->next = param;
  pQueue->tail = param;

  atomic_add_fetch_64(&pQueue->qRemainNum, 1);
}


static void mndStreamPostAction(SMnode *pMnode, int64_t streamId, char* streamName, MND_STREAM_ACTION action) {
  SStreamQNode *pNode = taosMemoryMalloc(sizeof(SStreamQNode) + strlen(streamName) + 1);
  if (NULL == pNode) {
    return;
  }

  pNode->streamId = streamId;
  pNode->streamName = pNode + 1;
  pNode->action = action;
  
  pNode->next = NULL;
  
  TAOS_STRCPY(pNode->streamName, streamName);

  mndStreamActionEnqueue(&mStreamMgmt.actionQ[streamGetTargetQIdx(mStreamMgmt.qNum, STREAM_GID(streamId))], pNode);
}

static int32_t mndProcessPauseStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  int32_t     code = 0;

  SMPauseStreamReq pauseReq = {0};
  if (tDeserializeSMPauseStreamReq(pReq->pCont, pReq->contLen, &pauseReq) < 0) {
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  code = mndAcquireStream(pMnode, pauseReq.name, &pStream);
  if (pStream == NULL || code != 0) {
    if (pauseReq.igNotExists) {
      mInfo("stream:%s, not exist, not pause stream", pauseReq.name);
      return 0;
    } else {
      mError("stream:%s not exist, failed to pause stream", pauseReq.name);
      TAOS_RETURN(TSDB_CODE_MND_STREAM_NOT_EXIST);
    }
  }

  int64_t streamId = pStream->pCreate->streamId;
  
  mstInfo("start to pause stream %s", pauseReq.name);

  code = mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->pCreate->streamDB);
  if (code != TSDB_CODE_SUCCESS) {
    mstError("user %s failed to pause stream %s since %s", pReq->info.conn.user, pauseReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  STrans *pTrans = NULL;
  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_NOTHING, MND_STREAM_PAUSE_NAME, &pTrans);
  if (pTrans == NULL || code) {
    mstError("failed to pause stream %s since %s", pauseReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  pStream->paused = true;

  // pause stream
  code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_READY);
  if (code != TSDB_CODE_SUCCESS) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("trans:%d, failed to prepare pause stream trans since %s", pTrans->id, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  mndStreamPostAction(pMnode, streamId, pStream->pCreate->name, STREAM_ACTION_UNDEPLOY);

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}


static int32_t mndProcessResumeStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  int32_t     code = 0;

  if ((code = grantCheckExpire(TSDB_GRANT_STREAMS)) < 0) {
    return code;
  }

  SMResumeStreamReq resumeReq = {0};
  if (tDeserializeSMResumeStreamReq(pReq->pCont, pReq->contLen, &resumeReq) < 0) {
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  code = mndAcquireStream(pMnode, resumeReq.name, &pStream);
  if (pStream == NULL || code != 0) {
    if (resumeReq.igNotExists) {
      mInfo("stream:%s not exist, not resume stream", resumeReq.name);
      sdbRelease(pMnode->pSdb, pStream);
      return 0;
    } else {
      mError("stream:%s not exist, failed to resume stream", resumeReq.name);
      TAOS_RETURN(TSDB_CODE_MND_STREAM_NOT_EXIST);
    }
  }

  int64_t streamId = pStream->pCreate->streamId;
  
  mstInfo("start to resume stream %s from pause", resumeReq.name);

  code = mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->pCreate->streamDB);
  if (code != TSDB_CODE_SUCCESS) {
    mstError("user %s failed to resume stream %s since %s", pReq->info.conn.user, resumeReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  STrans *pTrans = NULL;
  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_NOTHING, MND_STREAM_RESUME_NAME, &pTrans);
  if (pTrans == NULL || code) {
    mstError("failed to resume stream %s since %s", resumeReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  pStream->paused = false;

  code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_READY);
  if (code != TSDB_CODE_SUCCESS) {
    mstError("failed to resume stream %s since %s", resumeReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mstError("trans:%d, failed to prepare pause stream %s trans since %s", pTrans->id, resumeReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  mndStreamPostAction(pMnode, streamId, pStream->pCreate->name, STREAM_ACTION_DEPLOY);

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}


static int32_t mndProcessDropStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  int32_t     code = 0;

  SMDropStreamReq dropReq = {0};
  if (tDeserializeSMDropStreamReq(pReq->pCont, pReq->contLen, &dropReq) < 0) {
    mError("invalid drop stream msg recv, discarded");
    code = TSDB_CODE_INVALID_MSG;
    TAOS_RETURN(code);
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
      mError("stream:%s not exist failed to drop it", dropReq.name);
      tFreeMDropStreamReq(&dropReq);
      TAOS_RETURN(TSDB_CODE_MND_STREAM_NOT_EXIST);
    }
  }

  int64_t streamId = pStream->pCreate->streamId;

  code = mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->pCreate->streamDB);
  if (code != 0) {
    mstError("user %s failed to drop stream %s since %s", pReq->info.conn.user, dropReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMDropStreamReq(&dropReq);
    return code;
  }

  if (pStream->pCreate->tsmaId != 0) {
    mstDebug("try to drop tsma related stream");

    void    *pIter = NULL;
    SSmaObj *pSma = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_SMA, pIter, (void **)&pSma);
    while (pIter) {
      if (pSma && pSma->uid == pStream->pCreate->tsmaId) {
        sdbRelease(pMnode->pSdb, pSma);
        sdbRelease(pMnode->pSdb, pStream);

        sdbCancelFetch(pMnode->pSdb, pIter);
        tFreeMDropStreamReq(&dropReq);
        code = TSDB_CODE_TSMA_MUST_BE_DROPPED;

        mstError("refused to drop tsma-related stream %s since tsma still exists", dropReq.name);
        TAOS_RETURN(code);
      }

      if (pSma) {
        sdbRelease(pMnode->pSdb, pSma);
      }

      pIter = sdbFetch(pMnode->pSdb, SDB_SMA, pIter, (void **)&pSma);
    }
  }

  mndStreamPostAction(pMnode, streamId, pStream->pCreate->name, STREAM_ACTION_UNDEPLOY);

  STrans *pTrans = NULL;
  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_NOTHING, MND_STREAM_DROP_NAME, &pTrans);
  if (pTrans == NULL || code) {
    mstError("failed to drop stream %s since %s", dropReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMDropStreamReq(&dropReq);
    TAOS_RETURN(code);
  }

  // drop stream
  code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_DROPPED);
  if (code) {
    mstError("trans:%d, failed to append drop stream trans since %s", pTrans->id, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    tFreeMDropStreamReq(&dropReq);
    TAOS_RETURN(code);
  }

  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mstError("trans:%d, failed to prepare drop stream trans since %s", pTrans->id, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    tFreeMDropStreamReq(&dropReq);
    TAOS_RETURN(code);
  }

  auditRecord(pReq, pMnode->clusterId, "dropStream", "", pStream->pCreate->streamDB, dropReq.sql, strlen(dropReq.sql));

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  mstDebug("drop stream %s half completed", dropReq.name);
  code = TSDB_CODE_ACTION_IN_PROGRESS;

  tFreeMDropStreamReq(&dropReq);
  
  TAOS_RETURN(code);
}


static int32_t mndProcessCreateStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  SStreamObj  streamObj = {0};
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  STrans     *pTrans = NULL;
  
  SCMCreateStreamReq* pCreate = taosMemoryCalloc(1, sizeof(SCMCreateStreamReq));
  TSDB_CHECK_NULL(pCreate, code, lino, _OVER, terrno);
  
  code = tDeserializeSCMCreateStreamReq(pReq->pCont, pReq->contLen, pCreate);
  TSDB_CHECK_CODE(code, lino, _OVER);

#ifdef WINDOWS
  code = TSDB_CODE_MND_INVALID_PLATFORM;
  goto _OVER;
#endif

  uint64_t    streamId = pCreate->streamId;

  mstInfo("start to create stream %s, sql:%s", pCreate->name, pCreate->sql);

  code = mndStreamCheckSnodeExists(pMnode);
  TSDB_CHECK_CODE(code, lino, _OVER);
  
  code = mndAcquireStream(pMnode, pCreate->name, &pStream);
  if (pStream != NULL && code == 0) {
    if (pCreate->igExists) {
      mstInfo("stream %s already exist, ignore exist is set", pCreate->name);
    } else {
      code = TSDB_CODE_MND_STREAM_ALREADY_EXIST;
    }

    mndReleaseStream(pMnode, pStream);
    goto _OVER;
  } else if (code != TSDB_CODE_MND_STREAM_NOT_EXIST) {
    goto _OVER;
  }

  if ((code = grantCheck(TSDB_GRANT_STREAMS)) < 0) {
    goto _OVER;
  }

  code = mndStreamBuildObj(pMnode, &streamObj, pCreate);
  TSDB_CHECK_CODE(code, lino, _OVER);

  pStream = &streamObj;

  code = mndStreamValidateCreate(pMnode, pReq->info.conn.user, pCreate);
  TSDB_CHECK_CODE(code, lino, _OVER);

  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_DB, MND_STREAM_CREATE_NAME, &pTrans);
  if (pTrans == NULL || code) {
    goto _OVER;
  }

  // create stb for stream
  if (!pCreate->outStbExists) {
    code = mndStreamCreateOutStb(pMnode, pTrans, pStream, pReq->info.conn.user);
    TSDB_CHECK_CODE(code, lino, _OVER);
  } else {
    mstDebug("output table %s already exists", pCreate->outTblName);
  }

  // add stream to trans
  code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_READY);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mstError("failed to persist stream %s since %s", pCreate->name, tstrerror(code));
    goto _OVER;
  }

  // execute creation
  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mstError("trans:%d, failed to prepare since %s", pTrans->id, tstrerror(code));
    goto _OVER;
  }

  auditRecord(pReq, pMnode->clusterId, "createStream", pCreate->streamDB, pCreate->name, pCreate->sql, strlen(pCreate->sql));

  mndStreamPostAction(pMnode, streamId, pStream->pCreate->name, STREAM_ACTION_DEPLOY);

_OVER:

  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mstError("failed to create stream %s at line:%d since %s", pCreate->name, lino, tstrerror(code));
  } else {
    mstDebug("create stream %s half completed", pCreate->name);
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  mndTransDrop(pTrans);
  tFreeStreamObj(&streamObj);

  return code;
}
#endif



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

#ifdef NEW_STREAM
  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_STREAM, mndProcessCreateStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_STREAM, mndProcessDropStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_PAUSE_STREAM, mndProcessPauseStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_RESUME_STREAM, mndProcessResumeStreamReq);

  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_HEARTBEAT, mndProcessStreamHb);  
#endif  


  mndSetMsgHandle(pMnode, TDMT_MND_STOP_STREAM, mndProcessPauseStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_START_STREAM, mndProcessPauseStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_RESET_STREAM, mndProcessResetStreamReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndRetrieveStream);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndCancelGetNextStream);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAM_TASKS, mndRetrieveStreamTask);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAM_TASKS, mndCancelGetNextStreamTask);

  int32_t code = msmInitRuntimeInfo(pMnode);
  if (code) {
    return code;
  }

  code = sdbSetTable(pMnode->pSdb, table);
  if (code) {
    return code;
  }

  code = sdbSetTable(pMnode->pSdb, tableSeq);
  return code;
}


