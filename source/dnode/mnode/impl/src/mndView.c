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

#include "mndView.h"

int32_t mndInitView(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_VIEW,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndViewActionEncode,
      .decodeFp = (SdbDecodeFp)mndViewActionDecode,
      .insertFp = (SdbInsertFp)mndViewActionInsert,
      .updateFp = (SdbUpdateFp)mndViewActionUpdate,
      .deleteFp = (SdbDeleteFp)mndViewActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_STREAM, mndProcessCreateStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_STREAM, mndProcessDropStreamReq);
  mndSetMsgHandle(pMnode, TDMT_MND_NODECHECK_TIMER, mndProcessNodeCheck);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_VIEWS, mndRetrieveStream);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_VIEWS, mndCancelGetNextStream);

  taosThreadMutexInit(&execNodeList.lock, NULL);
  execNodeList.pTaskMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_NO_LOCK);
  execNodeList.pTaskList = taosArrayInit(4, sizeof(STaskStatusEntry));

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupView(SMnode *pMnode) {
  taosArrayDestroy(execNodeList.pTaskList);
  taosHashCleanup(execNodeList.pTaskMap);
  taosThreadMutexDestroy(&execNodeList.lock);
  mDebug("mnd view cleanup");
}

int32_t tEncodeSStreamObj(SEncoder *pEncoder, const SViewObj *pObj) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeCStr(pEncoder, pObj->name) < 0) return -1;

  if (tEncodeI64(pEncoder, pObj->createTime) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->updateTime) < 0) return -1;
  if (tEncodeI32(pEncoder, pObj->version) < 0) return -1;
  if (tEncodeI32(pEncoder, pObj->totalLevel) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->smaId) < 0) return -1;

  if (tEncodeI64(pEncoder, pObj->uid) < 0) return -1;
  if (tEncodeI8(pEncoder, pObj->status) < 0) return -1;

  if (tEncodeI8(pEncoder, pObj->conf.igExpired) < 0) return -1;
  if (tEncodeI8(pEncoder, pObj->conf.trigger) < 0) return -1;
  if (tEncodeI8(pEncoder, pObj->conf.fillHistory) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->conf.triggerParam) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->conf.watermark) < 0) return -1;

  if (tEncodeI64(pEncoder, pObj->sourceDbUid) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->targetDbUid) < 0) return -1;
  if (tEncodeCStr(pEncoder, pObj->sourceDb) < 0) return -1;
  if (tEncodeCStr(pEncoder, pObj->targetDb) < 0) return -1;
  if (tEncodeCStr(pEncoder, pObj->targetSTbName) < 0) return -1;
  if (tEncodeI64(pEncoder, pObj->targetStbUid) < 0) return -1;
  if (tEncodeI32(pEncoder, pObj->fixedSinkVgId) < 0) return -1;

  if (pObj->sql != NULL) {
    if (tEncodeCStr(pEncoder, pObj->sql) < 0) return -1;
  } else {
    if (tEncodeCStr(pEncoder, "") < 0) return -1;
  }

  if (pObj->ast != NULL) {
    if (tEncodeCStr(pEncoder, pObj->ast) < 0) return -1;
  } else {
    if (tEncodeCStr(pEncoder, "") < 0) return -1;
  }

  if (pObj->physicalPlan != NULL) {
    if (tEncodeCStr(pEncoder, pObj->physicalPlan) < 0) return -1;
  } else {
    if (tEncodeCStr(pEncoder, "") < 0) return -1;
  }

  int32_t sz = taosArrayGetSize(pObj->tasks);
  if (tEncodeI32(pEncoder, sz) < 0) return -1;
  for (int32_t i = 0; i < sz; i++) {
    SArray *pArray = taosArrayGetP(pObj->tasks, i);
    int32_t innerSz = taosArrayGetSize(pArray);
    if (tEncodeI32(pEncoder, innerSz) < 0) return -1;
    for (int32_t j = 0; j < innerSz; j++) {
      SStreamTask *pTask = taosArrayGetP(pArray, j);
      pTask->ver = SSTREAM_TASK_VER;
      if (tEncodeStreamTask(pEncoder, pTask) < 0) return -1;
    }
  }

  if (tEncodeSSchemaWrapper(pEncoder, &pObj->outputSchema) < 0) return -1;

  // 3.0.20 ver =2
  if (tEncodeI64(pEncoder, pObj->checkpointFreq) < 0) return -1;
  if (tEncodeI8(pEncoder, pObj->igCheckUpdate) < 0) return -1;

  // 3.0.50 ver = 3
  if (tEncodeI64(pEncoder, pObj->checkpointId) < 0) return -1;

  if (tEncodeCStrWithLen(pEncoder, pObj->reserve, sizeof(pObj->reserve) - 1) < 0) return -1;

  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeSStreamObj(SDecoder *pDecoder, SViewObj *pObj, int32_t sver) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pObj->name) < 0) return -1;

  if (tDecodeI64(pDecoder, &pObj->createTime) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->updateTime) < 0) return -1;
  if (tDecodeI32(pDecoder, &pObj->version) < 0) return -1;
  if (tDecodeI32(pDecoder, &pObj->totalLevel) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->smaId) < 0) return -1;

  if (tDecodeI64(pDecoder, &pObj->uid) < 0) return -1;
  if (tDecodeI8(pDecoder, &pObj->status) < 0) return -1;

  if (tDecodeI8(pDecoder, &pObj->conf.igExpired) < 0) return -1;
  if (tDecodeI8(pDecoder, &pObj->conf.trigger) < 0) return -1;
  if (tDecodeI8(pDecoder, &pObj->conf.fillHistory) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->conf.triggerParam) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->conf.watermark) < 0) return -1;

  if (tDecodeI64(pDecoder, &pObj->sourceDbUid) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->targetDbUid) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pObj->sourceDb) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pObj->targetDb) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pObj->targetSTbName) < 0) return -1;
  if (tDecodeI64(pDecoder, &pObj->targetStbUid) < 0) return -1;
  if (tDecodeI32(pDecoder, &pObj->fixedSinkVgId) < 0) return -1;

  if (tDecodeCStrAlloc(pDecoder, &pObj->sql) < 0) return -1;
  if (tDecodeCStrAlloc(pDecoder, &pObj->ast) < 0) return -1;
  if (tDecodeCStrAlloc(pDecoder, &pObj->physicalPlan) < 0) return -1;

  pObj->tasks = NULL;
  int32_t sz;
  if (tDecodeI32(pDecoder, &sz) < 0) return -1;
  if (sz != 0) {
    pObj->tasks = taosArrayInit(sz, sizeof(void *));
    for (int32_t i = 0; i < sz; i++) {
      int32_t innerSz;
      if (tDecodeI32(pDecoder, &innerSz) < 0) return -1;
      SArray *pArray = taosArrayInit(innerSz, sizeof(void *));
      for (int32_t j = 0; j < innerSz; j++) {
        SStreamTask *pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
        if (pTask == NULL) {
          taosArrayDestroy(pArray);
          return -1;
        }
        if (tDecodeStreamTask(pDecoder, pTask) < 0) {
          taosMemoryFree(pTask);
          taosArrayDestroy(pArray);
          return -1;
        }
        taosArrayPush(pArray, &pTask);
      }
      taosArrayPush(pObj->tasks, &pArray);
    }
  }

  if (tDecodeSSchemaWrapper(pDecoder, &pObj->outputSchema) < 0) return -1;

  // 3.0.20
  if (sver >= 2) {
    if (tDecodeI64(pDecoder, &pObj->checkpointFreq) < 0) return -1;
    if (!tDecodeIsEnd(pDecoder)) {
      if (tDecodeI8(pDecoder, &pObj->igCheckUpdate) < 0) return -1;
    }
  }
  if (sver >= 3) {
    if (tDecodeI64(pDecoder, &pObj->checkpointId) < 0) return -1;
  }
  if (tDecodeCStrTo(pDecoder, pObj->reserve) < 0) return -1;

  tEndDecode(pDecoder);
  return 0;
}

void tFreeStreamObj(SViewObj *pStream) {
  taosMemoryFree(pStream->sql);
  taosMemoryFree(pStream->ast);
  taosMemoryFree(pStream->physicalPlan);

  if (pStream->outputSchema.nCols || pStream->outputSchema.pSchema) {
    taosMemoryFree(pStream->outputSchema.pSchema);
  }

  pStream->tasks = freeStreamTasks(pStream->tasks);
  pStream->pHTasksList = freeStreamTasks(pStream->pHTasksList);

  if (pStream->tagSchema.nCols > 0) {
    taosMemoryFree(pStream->tagSchema.pSchema);
  }
}


SSdbRaw *mndViewActionEncode(SViewObj *pStream) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  void *buf = NULL;

  SEncoder encoder;
  tEncoderInit(&encoder, NULL, 0);
  if (tEncodeSViewObj(&encoder, pStream) < 0) {
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

SSdbRow *mndViewActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow    *pRow = NULL;
  SViewObj *pStream = NULL;
  void       *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto STREAM_DECODE_OVER;
  }

  if (sver != MND_STREAM_VER_NUMBER) {
    terrno = 0;
    mError("stream read invalid ver, data ver: %d, curr ver: %d", sver, MND_STREAM_VER_NUMBER);
    goto STREAM_DECODE_OVER;
  }

  pRow = sdbAllocRow(sizeof(SViewObj));
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

static int32_t mndViewActionInsert(SSdb *pSdb, SViewObj *pView) {
  mTrace("view:%s, perform insert action", pView->name);
  return 0;
}

static int32_t mndViewActionDelete(SSdb *pSdb, SViewObj *pStream) {
  mTrace("stream:%s, perform delete action", pStream->name);
  taosWLockLatch(&pStream->lock);
  tFreeStreamObj(pStream);
  taosWUnLockLatch(&pStream->lock);
  return 0;
}

static int32_t mndViewActionUpdate(SSdb *pSdb, SViewObj *pOldStream, SViewObj *pNewStream) {
  mTrace("stream:%s, perform update action", pOldStream->name);

  atomic_exchange_32(&pOldStream->version, pNewStream->version);

  taosWLockLatch(&pOldStream->lock);

  pOldStream->status = pNewStream->status;
  pOldStream->updateTime = pNewStream->updateTime;

  taosWUnLockLatch(&pOldStream->lock);
  return 0;
}

SViewObj *mndAcquireView(SMnode *pMnode, char *viewName) {
  SSdb       *pSdb = pMnode->pSdb;
  SViewObj *pStream = sdbAcquire(pSdb, SDB_VIEW, viewName);
  if (pStream == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_STREAM_NOT_EXIST;
  }
  return pStream;
}

void mndReleaseStream(SMnode *pMnode, SViewObj *pStream) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pStream);
}

static int32_t mndBuildViewObj(SMnode *pMnode, SViewObj *pObj, SCMCreateStreamReq *pCreate) {
  SNode      *pAst = NULL;
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


static int32_t mndCreateView(SMnode *pMnode, char *acct, SCreateUserReq *pCreate, SRpcMsg *pReq) {
  SUserObj userObj = {0};
  taosEncryptPass_c((uint8_t *)pCreate->pass, strlen(pCreate->pass), userObj.pass);
  tstrncpy(userObj.user, pCreate->user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.superUser = 0;  // pCreate->superUser;
  userObj.sysInfo = pCreate->sysInfo;
  userObj.enable = pCreate->enable;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to create since %s", pCreate->user, terrstr());
    return -1;
  }
  mInfo("trans:%d, used to create user:%s", pTrans->id, pCreate->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(&userObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessCreateViewReq(SRpcMsg *pReq) {
  SMnode            *pMnode = pReq->info.node;
  int32_t            code = -1;
  SViewObj          *pStream = NULL;
  SDbObj            *pDb = NULL;
  SCMCreateViewReq   createViewReq = {0};
  SViewObj           streamObj = {0};

  if (tDeserializeSCMCreateViewReq(pReq->pCont, pReq->contLen, &createViewReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("start to create view:%s, sql:%s", createViewReq.name, createViewReq.sql);

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

  if (mndCreateView(pMnode, &streamObj, &createStreamReq) < 0) {
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
    goto _OVER;
  }

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
  mDebug("register to stream task node list");
  keepStreamTasksInBuf(&streamObj, &execNodeList);
  taosThreadMutexUnlock(&execNodeList.lock);

  code = TSDB_CODE_ACTION_IN_PROGRESS;

  char detail[2000] = {0};
  sprintf(detail,
          "checkpointFreq:%" PRId64 ", createStb:%d, deleteMark:%" PRId64
          ", fillHistory:%d, igExists:%d, igExpired:%d, igUpdate:%d, lastTs:%" PRId64 ", maxDelay:%" PRId64
          ", numOfTags:%d, sourceDB:%s, targetStbFullName:%s, triggerType:%d, watermark:%" PRId64,
          createStreamReq.checkpointFreq, createStreamReq.createStb, createStreamReq.deleteMark,
          createStreamReq.fillHistory, createStreamReq.igExists, createStreamReq.igExpired, createStreamReq.igUpdate,
          createStreamReq.lastTs, createStreamReq.maxDelay, createStreamReq.numOfTags, createStreamReq.sourceDB,
          createStreamReq.targetStbFullName, createStreamReq.triggerType, createStreamReq.watermark);

  SName name = {0};
  tNameFromString(&name, createStreamReq.name, T_NAME_ACCT | T_NAME_DB);
  //reuse this function for stream

  auditRecord(pReq, pMnode->clusterId, "createView", name.dbname, "", detail);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("stream:%s, failed to create since %s", createStreamReq.name, terrstr());
  }

  mndReleaseStream(pMnode, pStream);

  tFreeSCMCreateStreamReq(&createStreamReq);
  tFreeStreamObj(&streamObj);
  return code;
}

static int32_t mndProcessDropStreamReq(SRpcMsg *pReq) {
  SMnode     *pMnode = pReq->info.node;
  SViewObj *pStream = NULL;

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
  if (mndTransCheckConflict(pMnode, pTrans) != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    mndTransDrop(pTrans);
    return -1;
  }
  // mndTransSetSerial(pTrans);

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

  char detail[100] = {0};
  sprintf(detail, "igNotExists:%d", dropReq.igNotExists);

  SName name = {0};
  tNameFromString(&name, dropReq.name, T_NAME_ACCT | T_NAME_DB);
  //reuse this function for stream

  auditRecord(pReq, pMnode->clusterId, "dropStream", name.dbname, "", detail);

  sdbRelease(pMnode->pSdb, pStream);
  mndTransDrop(pTrans);

  return TSDB_CODE_ACTION_IN_PROGRESS;
}

static int32_t mndRetrieveStream(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SViewObj *pStream = NULL;

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



