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

#define MND_STREAM_MAX_NUM 100000

typedef struct {
  int8_t placeHolder;  // // to fix windows compile error, define place holder
} SMStreamNodeCheckMsg;

static int32_t  mndNodeCheckSentinel = 0;
SStmRuntime  mStreamMgmt = {0};

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

static SSdbRow *mndStreamActionDecode(SSdbRaw *pRaw);

SSdbRaw       *mndStreamSeqActionEncode(SStreamObj *pStream);
SSdbRow       *mndStreamSeqActionDecode(SSdbRaw *pRaw);
static int32_t mndStreamSeqActionInsert(SSdb *pSdb, SStreamSeq *pStream);
static int32_t mndStreamSeqActionDelete(SSdb *pSdb, SStreamSeq *pStream);
static int32_t mndStreamSeqActionUpdate(SSdb *pSdb, SStreamSeq *pOldStream, SStreamSeq *pNewStream);
static int32_t mndProcessCreateStreamReq(SRpcMsg *pReq);

void mndCleanupStream(SMnode *pMnode) {
  mDebug("try to clean up stream");
  
  msmHandleBecomeNotLeader(pMnode);
  
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

  if (sver != MND_STREAM_VER_NUMBER && sver != MND_STREAM_COMPATIBLE_VER_NUMBER) {
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
    char *p = (pStream == NULL || NULL == pStream->pCreate) ? "null" : pStream->pCreate->name;
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

static bool mndStreamGetNameFromId(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SStreamObj* pStream = pObj;

  if (pStream->pCreate->streamId == *(int64_t*)p1) {
    strncpy((char*)p2, pStream->name, TSDB_STREAM_NAME_LEN);
    return false;
  }

  return true;
}

int32_t mndAcquireStreamById(SMnode *pMnode, int64_t streamId, SStreamObj **pStream) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  char streamName[TSDB_STREAM_NAME_LEN];
  streamName[0] = 0;
  
  sdbTraverse(pSdb, SDB_STREAM, mndStreamGetNameFromId, &streamId, streamName, NULL);
  if (streamName[0]) {
    (*pStream) = sdbAcquire(pSdb, SDB_STREAM, streamName);
    if ((*pStream) == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      code = TSDB_CODE_MND_STREAM_NOT_EXIST;
    }
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

static void mndStreamBuildObj(SMnode *pMnode, SStreamObj *pObj, SCMCreateStreamReq *pCreate, int32_t snodeId) {
  int32_t     code = 0;

  pObj->pCreate = pCreate;
  strncpy(pObj->name, pCreate->name, TSDB_STREAM_FNAME_LEN);
  pObj->mainSnodeId = snodeId;
  
  pObj->userDropped = 0;
  pObj->userStopped = 0;
  
  pObj->createTime = taosGetTimestampMs();
  pObj->updateTime = pObj->createTime;

  mstLogSStreamObj("create stream", pObj);
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
    SFieldWithOptions *pSrc = taosArrayGet(pStream->outCols, i);

    tstrncpy(pField->name, pSrc->name, TSDB_COL_NAME_LEN);
    pField->flags = pSrc->flags;
    pField->type = pSrc->type;
    pField->bytes = pSrc->bytes;
    pField->compress = createDefaultColCmprByType(pField->type);
    if (IS_DECIMAL_TYPE(pField->type)) {
      pField->typeMod = pSrc->typeMod;
      pField->flags |= COL_HAS_TYPE_MOD;
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

  if (pCreate->streamDB) {
    code = mndCheckDbPrivilegeByName(pMnode, pUser, MND_OPER_WRITE_DB, pCreate->streamDB);
    if (code) {
      mstsError("user %s failed to create stream %s in db %s since %s", pUser, pCreate->name, pCreate->streamDB, tstrerror(code));
    }
    TSDB_CHECK_CODE(code, lino, _OVER);
  }

  if (pCreate->triggerDB) {
    code = mndCheckDbPrivilegeByName(pMnode, pUser, MND_OPER_READ_DB, pCreate->triggerDB);
    if (code) {
      mstsError("user %s failed to create stream %s using trigger db %s since %s", pUser, pCreate->name, pCreate->triggerDB, tstrerror(code));
    }
    TSDB_CHECK_CODE(code, lino, _OVER);
  }

  if (pCreate->calcDB) {
    int32_t dbNum = taosArrayGetSize(pCreate->calcDB);
    for (int32_t i = 0; i < dbNum; ++i) {
      char* calcDB = taosArrayGetP(pCreate->calcDB, i);
      code = mndCheckDbPrivilegeByName(pMnode, pUser, MND_OPER_READ_DB, calcDB);
      if (code) {
        mstsError("user %s failed to create stream %s using calcDB %s since %s", pUser, pCreate->name, calcDB, tstrerror(code));
      }
      TSDB_CHECK_CODE(code, lino, _OVER);
    }
  }

  if (pCreate->outDB) {
    code = mndCheckDbPrivilegeByName(pMnode, pUser, MND_OPER_WRITE_DB, pCreate->outDB);
    if (code) {
      mstsError("user %s failed to create stream %s using out db %s since %s", pUser, pCreate->name, pCreate->outDB, tstrerror(code));
    }
    TSDB_CHECK_CODE(code, lino, _OVER);
  }

  int32_t streamNum = sdbGetSize(pMnode->pSdb, SDB_STREAM);
  if (streamNum > MND_STREAM_MAX_NUM) {
    code = TSDB_CODE_MND_TOO_MANY_STREAMS;
    mstsError("failed to create stream %s since %s, stream number:%d", pCreate->name, tstrerror(code), streamNum);
    return code;
  }

_OVER:

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

    if (0 == strcmp(pStream->pCreate->streamDB, pDb->name)) {
      mInfo("start to drop stream %s in db %s", pStream->pCreate->name, pDb->name);
      
      pStream->updateTime = taosGetTimestampMs();
      
      atomic_store_8(&pStream->userDropped, 1);
      
      MND_STREAM_SET_LAST_TS(STM_EVENT_DROP_STREAM, pStream->updateTime);
      
      msmUndeployStream(pMnode, pStream->pCreate->streamId, pStream->pCreate->name);
      
      // drop stream
      code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_DROPPED);
      if (code) {
        mError("drop db trans:%d failed to append drop stream trans since %s", pTrans->id, tstrerror(code));
        sdbRelease(pSdb, pStream);
        sdbCancelFetch(pSdb, pIter);
        TAOS_RETURN(code);
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

    code = mstSetStreamAttrResBlock(pMnode, pStream, pBlock, numOfRows);
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

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_STREAM, pShow->pIter, (void **)&pStream);
    if (pShow->pIter == NULL) {
      break;
    }

    code = mstSetStreamTasksResBlock(pStream, pBlock, &numOfRows, rowsCapacity);

    sdbRelease(pSdb, pStream);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextStreamTask(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_STREAM);
}

static int32_t mndRetrieveStreamRecalculates(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rowsCapacity) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SStreamObj *pStream = NULL;
  int32_t     code = 0;

  while (numOfRows < rowsCapacity) {
    pShow->pIter = sdbFetch(pSdb, SDB_STREAM, pShow->pIter, (void **)&pStream);
    if (pShow->pIter == NULL) {
      break;
    }

    code = mstSetStreamRecalculatesResBlock(pStream, pBlock, &numOfRows, rowsCapacity);

    sdbRelease(pSdb, pStream);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextStreamRecalculates(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_STREAM);
}


static bool mndStreamUpdateTagsFlag(SMnode *pMnode, void *pObj, void *p1, void *p2, void *p3) {
  SStreamObj *pStream = pObj;
  if (atomic_load_8(&pStream->userDropped)) {
    return true;
  }

  if (TSDB_SUPER_TABLE != pStream->pCreate->triggerTblType && 
      TSDB_CHILD_TABLE != pStream->pCreate->triggerTblType && 
      TSDB_VIRTUAL_CHILD_TABLE != pStream->pCreate->triggerTblType) {
    return true;
  }

  if (pStream->pCreate->triggerTblSuid != *(uint64_t*)p1) {
    return true;
  }

  if (NULL == pStream->pCreate->partitionCols) {
    return true;
  }

  SNodeList* pList = NULL;
  int32_t code = nodesStringToList(pStream->pCreate->partitionCols, &pList);
  if (code) {
    nodesDestroyList(pList);
    mstError("partitionCols [%s] nodesStringToList failed with error:%s", (char*)pStream->pCreate->partitionCols, tstrerror(code));
    return true;
  }

  SSchema* pTags = (SSchema*)p2;
  int32_t* tagNum = (int32_t*)p3;

  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    for (int32_t i = 0; i < *tagNum; ++i) {
      if (pCol->colId == pTags[i].colId) {
        pTags[i].flags |= COL_REF_BY_STM;
        break;
      }
    }
  }

  nodesDestroyList(pList);
  
  return true;
}


void mndStreamUpdateTagsRefFlag(SMnode *pMnode, int64_t suid, SSchema* pTags, int32_t tagNum) {
  int32_t streamNum = sdbGetSize(pMnode->pSdb, SDB_STREAM);
  if (streamNum <= 0) {
    return;
  }

  sdbTraverse(pMnode->pSdb, SDB_STREAM, mndStreamUpdateTagsFlag, &suid, pTags, &tagNum);
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
      mInfo("stream:%s, not exist, not stop stream", pauseReq.name);
      taosMemoryFree(pauseReq.name);
      return 0;
    } else {
      mError("stream:%s not exist, failed to stop stream", pauseReq.name);
      taosMemoryFree(pauseReq.name);
      TAOS_RETURN(TSDB_CODE_MND_STREAM_NOT_EXIST);
    }
  }

  taosMemoryFree(pauseReq.name);

  int64_t streamId = pStream->pCreate->streamId;
  
  mstsInfo("start to stop stream %s", pStream->name);

  code = mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->pCreate->streamDB);
  if (code != TSDB_CODE_SUCCESS) {
    mstsError("user %s failed to stop stream %s since %s", pReq->info.conn.user, pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  if (atomic_load_8(&pStream->userDropped)) {
    code = TSDB_CODE_MND_STREAM_DROPPING;
    mstsError("user %s failed to stop stream %s since %s", pReq->info.conn.user, pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  STrans *pTrans = NULL;
  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_NOTHING, MND_STREAM_STOP_NAME, &pTrans);
  if (pTrans == NULL || code) {
    mstsError("failed to stop stream %s since %s", pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  pStream->updateTime = taosGetTimestampMs();

  atomic_store_8(&pStream->userStopped, 1);

  MND_STREAM_SET_LAST_TS(STM_EVENT_STOP_STREAM, pStream->updateTime);

  msmUndeployStream(pMnode, streamId, pStream->name);

  // stop stream
  code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_READY);
  if (code != TSDB_CODE_SUCCESS) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("trans:%d, failed to prepare stop stream trans since %s", pTrans->id, tstrerror(code));
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
      mInfo("stream:%s not exist, not start stream", resumeReq.name);
      taosMemoryFree(resumeReq.name);
      sdbRelease(pMnode->pSdb, pStream);
      return 0;
    } else {
      mError("stream:%s not exist, failed to start stream", resumeReq.name);
      taosMemoryFree(resumeReq.name);
      TAOS_RETURN(TSDB_CODE_MND_STREAM_NOT_EXIST);
    }
  }

  taosMemoryFree(resumeReq.name);

  int64_t streamId = pStream->pCreate->streamId;

  mstsInfo("start to start stream %s from stopped", pStream->name);

  code = mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->pCreate->streamDB);
  if (code != TSDB_CODE_SUCCESS) {
    mstsError("user %s failed to start stream %s since %s", pReq->info.conn.user, pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  if (atomic_load_8(&pStream->userDropped)) {
    code = TSDB_CODE_MND_STREAM_DROPPING;
    mstsError("user %s failed to start stream %s since %s", pReq->info.conn.user, pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  if (0 == atomic_load_8(&pStream->userStopped)) {
    code = TSDB_CODE_MND_STREAM_NOT_STOPPED;
    mstsError("user %s failed to start stream %s since %s", pReq->info.conn.user, pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }
  
  atomic_store_8(&pStream->userStopped, 0);

  pStream->updateTime = taosGetTimestampMs();

  MND_STREAM_SET_LAST_TS(STM_EVENT_START_STREAM, pStream->updateTime);

  STrans *pTrans = NULL;
  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_NOTHING, MND_STREAM_START_NAME, &pTrans);
  if (pTrans == NULL || code) {
    mstsError("failed to start stream %s since %s", pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_READY);
  if (code != TSDB_CODE_SUCCESS) {
    mstsError("failed to start stream %s since %s", pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mstsError("trans:%d, failed to prepare start stream %s trans since %s", pTrans->id, pStream->name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  mstPostStreamAction(mStreamMgmt.actionQ, streamId, pStream->name, NULL, true, STREAM_ACT_DEPLOY);

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
    mstsError("user %s failed to drop stream %s since %s", pReq->info.conn.user, dropReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMDropStreamReq(&dropReq);
    return code;
  }

  if (pStream->pCreate->tsmaId != 0) {
    mstsDebug("try to drop tsma related stream, tsmaId:%" PRIx64, pStream->pCreate->tsmaId);

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

        mstsError("refused to drop tsma-related stream %s since tsma still exists", dropReq.name);
        TAOS_RETURN(code);
      }

      if (pSma) {
        sdbRelease(pMnode->pSdb, pSma);
      }

      pIter = sdbFetch(pMnode->pSdb, SDB_SMA, pIter, (void **)&pSma);
    }
  }

  mstsInfo("start to drop stream %s", pStream->pCreate->name);

  pStream->updateTime = taosGetTimestampMs();

  atomic_store_8(&pStream->userDropped, 1);

  MND_STREAM_SET_LAST_TS(STM_EVENT_DROP_STREAM, pStream->updateTime);

  msmUndeployStream(pMnode, streamId, pStream->pCreate->name);

  STrans *pTrans = NULL;
  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_NOTHING, MND_STREAM_DROP_NAME, &pTrans);
  if (pTrans == NULL || code) {
    mstsError("failed to drop stream %s since %s", dropReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMDropStreamReq(&dropReq);
    TAOS_RETURN(code);
  }

  // drop stream
  code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_DROPPED);
  if (code) {
    mstsError("trans:%d, failed to append drop stream trans since %s", pTrans->id, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    tFreeMDropStreamReq(&dropReq);
    TAOS_RETURN(code);
  }

  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mstsError("trans:%d, failed to prepare drop stream trans since %s", pTrans->id, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    tFreeMDropStreamReq(&dropReq);
    TAOS_RETURN(code);
  }

  auditRecord(pReq, pMnode->clusterId, "dropStream", "", pStream->pCreate->streamDB, NULL, 0);

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  mstsDebug("drop stream %s half completed", dropReq.name);
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
  uint64_t    streamId = 0;
  SCMCreateStreamReq* pCreate = NULL;

  if ((code = grantCheck(TSDB_GRANT_STREAMS)) < 0) {
    goto _OVER;
  }
  
#ifdef WINDOWS
  code = TSDB_CODE_MND_INVALID_PLATFORM;
  goto _OVER;
#endif

  pCreate = taosMemoryCalloc(1, sizeof(SCMCreateStreamReq));
  TSDB_CHECK_NULL(pCreate, code, lino, _OVER, terrno);
  
  code = tDeserializeSCMCreateStreamReq(pReq->pCont, pReq->contLen, pCreate);
  TSDB_CHECK_CODE(code, lino, _OVER);

  streamId = pCreate->streamId;

  mstsInfo("start to create stream %s, sql:%s", pCreate->name, pCreate->sql);

  int32_t snodeId = msmAssignRandomSnodeId(pMnode, streamId);
  if (!GOT_SNODE(snodeId)) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _OVER);
  }
  
  code = mndAcquireStream(pMnode, pCreate->name, &pStream);
  if (pStream != NULL && code == 0) {
    if (pCreate->igExists) {
      mstsInfo("stream %s already exist, ignore exist is set", pCreate->name);
    } else {
      code = TSDB_CODE_MND_STREAM_ALREADY_EXIST;
    }

    mndReleaseStream(pMnode, pStream);
    goto _OVER;
  } else if (code != TSDB_CODE_MND_STREAM_NOT_EXIST) {
    goto _OVER;
  }

  code = mndStreamValidateCreate(pMnode, pReq->info.conn.user, pCreate);
  TSDB_CHECK_CODE(code, lino, _OVER);

  mndStreamBuildObj(pMnode, &streamObj, pCreate, snodeId);
  pCreate = NULL;

  pStream = &streamObj;

  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_DB, MND_STREAM_CREATE_NAME, &pTrans);
  if (pTrans == NULL || code) {
    goto _OVER;
  }

  // create stb for stream
  if (TSDB_SUPER_TABLE == pStream->pCreate->outTblType && !pStream->pCreate->outStbExists) {
    pStream->pCreate->outStbUid = mndGenerateUid(pStream->pCreate->outTblName, strlen(pStream->pCreate->outTblName));
    code = mndStreamCreateOutStb(pMnode, pTrans, pStream->pCreate, pReq->info.conn.user);
    TSDB_CHECK_CODE(code, lino, _OVER);
  }

  // add stream to trans
  code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_READY);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mstsError("failed to persist stream %s since %s", pStream->pCreate->name, tstrerror(code));
    goto _OVER;
  }

  // execute creation
  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mstsError("trans:%d, failed to prepare since %s", pTrans->id, tstrerror(code));
    goto _OVER;
  }
  code = TSDB_CODE_ACTION_IN_PROGRESS;

  auditRecord(pReq, pMnode->clusterId, "createStream", pStream->pCreate->streamDB, pStream->pCreate->name, pStream->pCreate->sql, strlen(pStream->pCreate->sql));

  MND_STREAM_SET_LAST_TS(STM_EVENT_CREATE_STREAM, taosGetTimestampMs());

  mstPostStreamAction(mStreamMgmt.actionQ, streamId, pStream->pCreate->name, NULL, true, STREAM_ACT_DEPLOY);

_OVER:

  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    if (pStream && pStream->pCreate) {
      mstsError("failed to create stream %s at line:%d since %s", pStream->pCreate->name, lino, tstrerror(code));
    } else {
      mstsError("failed to create stream at line:%d since %s", lino, tstrerror(code));
    }
  } else {
    mstsDebug("create stream %s half completed", pStream->pCreate ? pStream->pCreate->name : "unknown");
  }

  tFreeSCMCreateStreamReq(pCreate);
  taosMemoryFreeClear(pCreate);

  mndTransDrop(pTrans);
  tFreeStreamObj(&streamObj);

  return code;
}

static int32_t mndProcessRecalcStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SStreamObj *pStream = NULL;
  int32_t     code = 0;

  if ((code = grantCheckExpire(TSDB_GRANT_STREAMS)) < 0) {
    return code;
  }

  SMRecalcStreamReq recalcReq = {0};
  if (tDeserializeSMRecalcStreamReq(pReq->pCont, pReq->contLen, &recalcReq) < 0) {
    tFreeMRecalcStreamReq(&recalcReq);
    TAOS_RETURN(TSDB_CODE_INVALID_MSG);
  }

  code = mndAcquireStream(pMnode, recalcReq.name, &pStream);
  if (pStream == NULL || code != 0) {
    mError("stream:%s not exist, failed to recalc stream", recalcReq.name);
    tFreeMRecalcStreamReq(&recalcReq);
    TAOS_RETURN(TSDB_CODE_MND_STREAM_NOT_EXIST);
  }

  int64_t streamId = pStream->pCreate->streamId;
  
  mstsInfo("start to recalc stream %s", recalcReq.name);

  code = mndCheckDbPrivilegeByName(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pStream->pCreate->streamDB);
  if (code != TSDB_CODE_SUCCESS) {
    mstsError("user %s failed to recalc stream %s since %s", pReq->info.conn.user, recalcReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMRecalcStreamReq(&recalcReq);
    return code;
  }

  if (atomic_load_8(&pStream->userDropped)) {
    code = TSDB_CODE_MND_STREAM_DROPPING;
    mstsError("user %s failed to recalc stream %s since %s", pReq->info.conn.user, recalcReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMRecalcStreamReq(&recalcReq);
    return code;
  }

  if (atomic_load_8(&pStream->userStopped)) {
    code = TSDB_CODE_MND_STREAM_STOPPED;
    mstsError("user %s failed to recalc stream %s since %s", pReq->info.conn.user, recalcReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMRecalcStreamReq(&recalcReq);
    return code;
  }

  if (WINDOW_TYPE_PERIOD == pStream->pCreate->triggerType) {
    code = TSDB_CODE_OPS_NOT_SUPPORT;
    mstsError("failed to recalc stream %s since %s", recalcReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMRecalcStreamReq(&recalcReq);
    return code;
  }

  /*
  pStream->updateTime = taosGetTimestampMs();

  STrans *pTrans = NULL;
  code = mndStreamCreateTrans(pMnode, pStream, pReq, TRN_CONFLICT_NOTHING, MND_STREAM_RECALC_NAME, &pTrans);
  if (pTrans == NULL || code) {
    mstsError("failed to recalc stream %s since %s", recalcReq.name, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    return code;
  }

  // stop stream
  code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_READY);
  if (code != TSDB_CODE_SUCCESS) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }

  code = mndTransPrepare(pMnode, pTrans);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("trans:%d, failed to prepare stop stream trans since %s", pTrans->id, tstrerror(code));
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return code;
  }
*/

  code = msmRecalcStream(pMnode, pStream->pCreate->streamId, &recalcReq.timeRange);
  if (code != TSDB_CODE_SUCCESS) {
    sdbRelease(pMnode->pSdb, pStream);
    tFreeMRecalcStreamReq(&recalcReq);
    return code;
  }
  
  char buf[128];
  snprintf(buf, sizeof(buf), "start:%" PRId64 ", end:%" PRId64, recalcReq.timeRange.skey, recalcReq.timeRange.ekey);
  auditRecord(pReq, pMnode->clusterId, "recalcStream", pStream->name, recalcReq.name, buf, strlen(buf));

  sdbRelease(pMnode->pSdb, pStream);
  tFreeMRecalcStreamReq(&recalcReq);
//  mndTransDrop(pTrans);

  return TSDB_CODE_SUCCESS;
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

  if (!tsDisableStream) {
    mndSetMsgHandle(pMnode, TDMT_MND_CREATE_STREAM, mndProcessCreateStreamReq);
    mndSetMsgHandle(pMnode, TDMT_MND_DROP_STREAM, mndProcessDropStreamReq);
    mndSetMsgHandle(pMnode, TDMT_MND_START_STREAM, mndProcessStartStreamReq);
    mndSetMsgHandle(pMnode, TDMT_MND_STOP_STREAM, mndProcessStopStreamReq);
    mndSetMsgHandle(pMnode, TDMT_MND_STREAM_HEARTBEAT, mndProcessStreamHb);  
    mndSetMsgHandle(pMnode, TDMT_MND_RECALC_STREAM, mndProcessRecalcStreamReq);
  }
  
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndRetrieveStream);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAMS, mndCancelGetNextStream);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAM_TASKS, mndRetrieveStreamTask);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAM_TASKS, mndCancelGetNextStreamTask);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STREAM_RECALCULATES, mndRetrieveStreamRecalculates);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STREAM_RECALCULATES, mndCancelGetNextStreamRecalculates);

  int32_t code = sdbSetTable(pMnode->pSdb, table);
  if (code) {
    return code;
  }

  //code = sdbSetTable(pMnode->pSdb, tableSeq);
  return code;
}
