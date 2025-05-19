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
SStmRuntime  mStreamMgmt;

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
static int32_t mndProcessStopStreamReq(SRpcMsg *pReq);
static int32_t mndProcessStartStreamReq(SRpcMsg *pReq);
static void    doSendQuickRsp(SRpcHandleInfo *pInfo, int32_t msgSize, int32_t vgId, int32_t code);

static SSdbRow *mndStreamActionDecode(SSdbRaw *pRaw);

SSdbRaw       *mndStreamSeqActionEncode(SStreamObj *pStream);
SSdbRow       *mndStreamSeqActionDecode(SSdbRaw *pRaw);
static int32_t mndStreamSeqActionInsert(SSdb *pSdb, SStreamSeq *pStream);
static int32_t mndStreamSeqActionDelete(SSdb *pSdb, SStreamSeq *pStream);
static int32_t mndStreamSeqActionUpdate(SSdb *pSdb, SStreamSeq *pOldStream, SStreamSeq *pNewStream);
static int32_t mndProcessCreateStreamReq(SRpcMsg *pReq);

void mndCleanupStream(SMnode *pMnode) {
  //STREAMTODO
  mDebug("mnd stream runtime info cleanup");
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
    char *p = (pStream == NULL) ? "null" : pStream->pCreate->name;
    mError("stream:%s, failed to decode from raw:%p since %s at:%d", p, pRaw, tstrerror(code), lino);
    taosMemoryFreeClear(pRow);

    terrno = code;
    return NULL;
  } else {
    mTrace("stream:%s, decode from raw:%p, row:%p", pStream->pCreate->name, pRaw, pStream);

    terrno = 0;
    return pRow;
  }
}

static int32_t mndStreamActionInsert(SSdb *pSdb, SStreamObj *pStream) {
  mTrace("stream:%s, perform insert action", pStream->pCreate->name);
  return 0;
}

static int32_t mndStreamActionDelete(SSdb *pSdb, SStreamObj *pStream) {
  mInfo("stream:%s, perform delete action", pStream->pCreate->name);
  tFreeStreamObj(pStream);
  return 0;
}

static int32_t mndStreamActionUpdate(SSdb *pSdb, SStreamObj *pOldStream, SStreamObj *pNewStream) {
  mTrace("stream:%s, perform update action", pOldStream->pCreate->name);

  atomic_store_32(&pOldStream->mainSnodeId, pNewStream->mainSnodeId);
  atomic_store_8(&pOldStream->userStopped, atomic_load_8(&pNewStream->userStopped));
  pOldStream->updateTime = pNewStream->updateTime;
  
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

static int32_t mndStreamBuildObj(SMnode *pMnode, SStreamObj *pObj, SCMCreateStreamReq *pCreate, int32_t snodeId) {
  int32_t     code = 0;

  pObj->pCreate = pCreate;
  strncpy(pObj->name, pCreate->name, TSDB_STREAM_NAME_LEN);
  pObj->mainSnodeId = snodeId;
  
  pObj->userDropped = 0;
  pObj->userStopped = 0;
  
  pObj->createTime = taosGetTimestampMs();
  pObj->updateTime = pObj->createTime;

  return code;
}

static int32_t mndStreamCreateOutStb(SMnode *pMnode, STrans *pTrans, const SCMCreateStreamReq *pStream, const char *user) {
  SStbObj *pStb = NULL;
  SDbObj  *pDb = NULL;
  int32_t  code = 0;
  int32_t  lino = 0;

  SMCreateStbReq createReq = {0};
  TAOS_STRNCAT(createReq.name, pStream->outDB, TSDB_DB_FNAME_LEN);
  TAOS_STRNCAT(createReq.name, ".", 2);
  TAOS_STRNCAT(createReq.name,  pStream->outTblName, TSDB_TABLE_NAME_LEN);
  createReq.numOfColumns = taosArrayGetSize(pStream->outCols);
  createReq.numOfTags = pStream->outTags ? taosArrayGetSize(pStream->outTags) : 1;
  createReq.pColumns = taosArrayInit_s(sizeof(SFieldWithOptions), createReq.numOfColumns);
  TSDB_CHECK_NULL(createReq.pColumns, code, lino, _OVER, terrno);

  // build fields
  for (int32_t i = 0; i < createReq.numOfColumns; i++) {
    SFieldWithOptions *pField = taosArrayGet(createReq.pColumns, i);
    TSDB_CHECK_NULL(pField, code, lino, _OVER, terrno);
    TAOS_FIELD_E *pSrc = taosArrayGet(pStream->outCols, i);

    tstrncpy(pField->name, pSrc->name, TSDB_COL_NAME_LEN);
    pField->flags = 0;
    pField->type = pSrc->type;
    pField->bytes = pSrc->bytes;
    pField->compress = createDefaultColCmprByType(pField->type);
    if (IS_DECIMAL_TYPE(pField->type)) {
      pField->typeMod = decimalCalcTypeMod(pSrc->precision, pSrc->scale);
    }
  }

  if (NULL == pStream->outTags) {
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
    createReq.numOfTags = taosArrayGetSize(pStream->outTags);
    createReq.pTags = taosArrayInit_s(sizeof(SField), createReq.numOfTags);
    TSDB_CHECK_NULL(createReq.pTags, code, lino, _OVER, terrno);

    for (int32_t i = 0; i < createReq.numOfTags; i++) {
      SField *pField = taosArrayGet(createReq.pTags, i);
      if (pField == NULL) {
        continue;
      }

      TAOS_FIELD_E *pSrc = taosArrayGet(pStream->outTags, i);
      pField->bytes = pSrc->bytes;
      pField->flags = 0;
      pField->type = pSrc->type;
      tstrncpy(pField->name, pSrc->name, TSDB_COL_NAME_LEN);
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

  stbObj.uid = pStream->outStbUid;

  if (mndAddStbToTrans(pMnode, pTrans, pDb, &stbObj) < 0) {
    mndFreeStb(&stbObj);
    goto _OVER;
  }

  mDebug("stream:%s create dst stable:%s, cols:%d", pStream->name, pStream->outTblName, createReq.numOfColumns);

  tFreeSMCreateStbReq(&createReq);
  mndFreeStb(&stbObj);
  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);
  return code;

_OVER:
  tFreeSMCreateStbReq(&createReq);
  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);

  mDebug("stream:%s failed to create dst stable:%s, line:%d code:%s", pStream->name, pStream->outTblName, lino,
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

/*  STREAMTODO
  if (pCreate->sourceDB) {
    code = mndCheckDbPrivilegeByName(pMnode, pUser, MND_OPER_READ_DB, pCreate->sourceDB);
    if (code) {
      mstError("user %s failed to create stream %s using source db %s since %s", pUser, pCreate->name, pCreate->sourceDB, tstrerror(code));
    }
    TSDB_CHECK_CODE(code, lino, _OVER);
  }
*/

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
/* STREAMTODO
  int32_t lino = 0;
  int64_t streamId = pStream->pCreate->streamId;

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
    mstError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
*/
  
  return code;
}

static int32_t addStreamNotifyInfo(SCMCreateStreamReq *createReq, SStreamObj *pStream) {
  int32_t code = TSDB_CODE_SUCCESS;
/* STREAMTODO
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
*/  
  return code;
}

int32_t mndDropStreamByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  int32_t code = 0;

  while (1) {
    SStreamObj *pStream = NULL;
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

/* STREAMTODO
    if (pStream->sourceDbUid == pDb->uid || pStream->targetDbUid == pDb->uid) {
      if (pStream->sourceDbUid != pStream->targetDbUid) {
        sdbRelease(pSdb, pStream);
        sdbCancelFetch(pSdb, pIter);
        mError("db:%s, failed to drop stream:%s since sourceDbUid:%" PRId64 " not match with targetDbUid:%" PRId64,
               pDb->name, pStream->name, pStream->sourceDbUid, pStream->targetDbUid);
        TAOS_RETURN(TSDB_CODE_MND_STREAM_MUST_BE_DELETED);
      } else {
        //STREAMTODO drop the stream obj in execInfo
        //removeStreamTasksInBuf(pStream, &execInfo);

        code = mndPersistTransLog(pStream, pTrans, SDB_STATUS_DROPPED);
        if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
          sdbRelease(pSdb, pStream);
          sdbCancelFetch(pSdb, pIter);
          return code;
        }
      }
    }
*/

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

    //STREAMTODO
    //code = mndStreamGenerateResBlock(pStream, pBlock, numOfRows);
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


  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextStreamTask(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_STREAM);
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

static int32_t mndProcessStopStreamReq(SRpcMsg *pReq) {
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
  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_NOTHING, MND_STREAM_STOP_NAME, &pTrans);
  if (pTrans == NULL || code) {
    mstError("failed to pause stream %s since %s", pauseReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  atomic_store_8(&pStream->userStopped, 1);

  msmUndeployStream(pMnode, streamId, pStream->pCreate->name);

  // stop stream
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

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}


static int32_t mndProcessStartStreamReq(SRpcMsg *pReq) {
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

  atomic_store_8(&pStream->userStopped, 0);
  
  mstInfo("start to start stream %s from stopped", resumeReq.name);

  code = mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->pCreate->streamDB);
  if (code != TSDB_CODE_SUCCESS) {
    mstError("user %s failed to resume stream %s since %s", pReq->info.conn.user, resumeReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  STrans *pTrans = NULL;
  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_NOTHING, MND_STREAM_START_NAME, &pTrans);
  if (pTrans == NULL || code) {
    mstError("failed to resume stream %s since %s", resumeReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

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

  mndStreamPostAction(mStreamMgmt.actionQ, streamId, pStream->pCreate->name, STREAM_ACT_DEPLOY);

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
    mstDebug("try to drop tsma related stream, tsmaId:%" PRIx64, pStream->pCreate->tsmaId);

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

  mstInfo("start to drop stream %s", pStream->pCreate->name);

  atomic_store_8(&pStream->userDropped, 1);

  msmUndeployStream(pMnode, streamId, pStream->pCreate->name);

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

  auditRecord(pReq, pMnode->clusterId, "dropStream", "", pStream->pCreate->streamDB, NULL, 0);

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

  int32_t snodeId = msmAssignRandomSnodeId(pMnode, streamId);
  if (0 == snodeId) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _OVER);
  }
  
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

  code = mndStreamValidateCreate(pMnode, pReq->info.conn.user, pCreate);
  TSDB_CHECK_CODE(code, lino, _OVER);

  code = mndStreamBuildObj(pMnode, &streamObj, pCreate, snodeId);
  TSDB_CHECK_CODE(code, lino, _OVER);

  pStream = &streamObj;

  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_DB, MND_STREAM_CREATE_NAME, &pTrans);
  if (pTrans == NULL || code) {
    goto _OVER;
  }

  // create stb for stream
  if (TSDB_SUPER_TABLE == pCreate->outTblType && !pCreate->outStbExists) {
    pCreate->outStbUid = mndGenerateUid(pCreate->outTblName, strlen(pCreate->outTblName));
    code = mndStreamCreateOutStb(pMnode, pTrans, pStream->pCreate, pReq->info.conn.user);
    TSDB_CHECK_CODE(code, lino, _OVER);
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

  mndStreamPostAction(mStreamMgmt.actionQ, streamId, pStream->pCreate->name, STREAM_ACT_DEPLOY);

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
/*
  SSdbTable tableSeq = {
      .sdbType = SDB_STREAM_SEQ,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndStreamSeqActionEncode,
      .decodeFp = (SdbDecodeFp)mndStreamSeqActionDecode,
      .insertFp = (SdbInsertFp)mndStreamSeqActionInsert,
      .updateFp = (SdbUpdateFp)mndStreamSeqActionUpdate,
      .deleteFp = (SdbDeleteFp)mndStreamSeqActionDelete,
  };
*/
  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_STREAM, mndProcessCreateStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_STREAM, mndProcessDropStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_START_STREAM, mndProcessStartStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_STOP_STREAM, mndProcessStopStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_STREAM_HEARTBEAT, mndProcessStreamHb);  

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

  //code = sdbSetTable(pMnode->pSdb, tableSeq);
  return code;
}


