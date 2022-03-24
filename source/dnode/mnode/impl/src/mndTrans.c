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

#define _DEFAULT_SOURCE
#include "mndTrans.h"
#include "mndAuth.h"
#include "mndDb.h"
#include "mndShow.h"
#include "mndSync.h"
#include "mndUser.h"

#define MND_TRANS_VER_NUMBER   1
#define MND_TRANS_ARRAY_SIZE   8
#define MND_TRANS_RESERVE_SIZE 64

static SSdbRaw *mndTransActionEncode(STrans *pTrans);
static SSdbRow *mndTransActionDecode(SSdbRaw *pRaw);
static int32_t  mndTransActionInsert(SSdb *pSdb, STrans *pTrans);
static int32_t  mndTransActionUpdate(SSdb *pSdb, STrans *OldTrans, STrans *pOld);
static int32_t  mndTransActionDelete(SSdb *pSdb, STrans *pTrans);

static int32_t mndTransAppendLog(SArray *pArray, SSdbRaw *pRaw);
static int32_t mndTransAppendAction(SArray *pArray, STransAction *pAction);
static void    mndTransDropLogs(SArray *pArray);
static void    mndTransDropActions(SArray *pArray);
static void    mndTransDropData(STrans *pTrans);
static int32_t mndTransExecuteLogs(SMnode *pMnode, SArray *pArray);
static int32_t mndTransExecuteActions(SMnode *pMnode, STrans *pTrans, SArray *pArray);
static int32_t mndTransExecuteRedoLogs(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransExecuteUndoLogs(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransExecuteRedoActions(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransExecuteUndoActions(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransExecuteCommitLogs(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerformPrepareStage(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerformRedoLogStage(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerformRedoActionStage(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerformUndoLogStage(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerformUndoActionStage(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerformCommitLogStage(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerformCommitStage(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerformRollbackStage(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerfromFinishedStage(SMnode *pMnode, STrans *pTrans);

static void    mndTransExecute(SMnode *pMnode, STrans *pTrans);
static void    mndTransSendRpcRsp(STrans *pTrans);
static int32_t mndProcessTransReq(SNodeMsg *pReq);
static int32_t mndProcessKillTransReq(SNodeMsg *pReq);

static int32_t mndGetTransMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t mndRetrieveTrans(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows);
static void    mndCancelGetNextTrans(SMnode *pMnode, void *pIter);

int32_t mndInitTrans(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_TRANS,
                     .keyType = SDB_KEY_INT32,
                     .encodeFp = (SdbEncodeFp)mndTransActionEncode,
                     .decodeFp = (SdbDecodeFp)mndTransActionDecode,
                     .insertFp = (SdbInsertFp)mndTransActionInsert,
                     .updateFp = (SdbUpdateFp)mndTransActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndTransActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_TRANS_TIMER, mndProcessTransReq);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_TRANS, mndProcessKillTransReq);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_TRANS, mndGetTransMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TRANS, mndRetrieveTrans);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TRANS, mndCancelGetNextTrans);
  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupTrans(SMnode *pMnode) {}

static SSdbRaw *mndTransActionEncode(STrans *pTrans) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t rawDataLen = sizeof(STrans) + MND_TRANS_RESERVE_SIZE;
  int32_t redoLogNum = taosArrayGetSize(pTrans->redoLogs);
  int32_t undoLogNum = taosArrayGetSize(pTrans->undoLogs);
  int32_t commitLogNum = taosArrayGetSize(pTrans->commitLogs);
  int32_t redoActionNum = taosArrayGetSize(pTrans->redoActions);
  int32_t undoActionNum = taosArrayGetSize(pTrans->undoActions);

  for (int32_t i = 0; i < redoLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->redoLogs, i);
    rawDataLen += (sdbGetRawTotalSize(pTmp) + sizeof(int32_t));
  }

  for (int32_t i = 0; i < undoLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->undoLogs, i);
    rawDataLen += (sdbGetRawTotalSize(pTmp) + sizeof(int32_t));
  }

  for (int32_t i = 0; i < commitLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->commitLogs, i);
    rawDataLen += (sdbGetRawTotalSize(pTmp) + sizeof(int32_t));
  }

  for (int32_t i = 0; i < redoActionNum; ++i) {
    STransAction *pAction = taosArrayGet(pTrans->redoActions, i);
    rawDataLen += (sizeof(STransAction) + pAction->contLen);
  }

  for (int32_t i = 0; i < undoActionNum; ++i) {
    STransAction *pAction = taosArrayGet(pTrans->undoActions, i);
    rawDataLen += (sizeof(STransAction) + pAction->contLen);
  }

  SSdbRaw *pRaw = sdbAllocRaw(SDB_TRANS, MND_TRANS_VER_NUMBER, rawDataLen);
  if (pRaw == NULL) {
    mError("trans:%d, failed to alloc raw since %s", pTrans->id, terrstr());
    return NULL;
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pTrans->id, TRANS_ENCODE_OVER)
  SDB_SET_INT16(pRaw, dataPos, pTrans->policy, TRANS_ENCODE_OVER)
  SDB_SET_INT16(pRaw, dataPos, pTrans->stage, TRANS_ENCODE_OVER)
  SDB_SET_INT16(pRaw, dataPos, pTrans->transType, TRANS_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pTrans->createdTime, TRANS_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pTrans->dbUid, TRANS_ENCODE_OVER)
  SDB_SET_BINARY(pRaw, dataPos, pTrans->dbname, TSDB_DB_FNAME_LEN, TRANS_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, redoLogNum, TRANS_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, undoLogNum, TRANS_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, commitLogNum, TRANS_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, redoActionNum, TRANS_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, undoActionNum, TRANS_ENCODE_OVER)

  for (int32_t i = 0; i < redoLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->redoLogs, i);
    int32_t  len = sdbGetRawTotalSize(pTmp);
    SDB_SET_INT32(pRaw, dataPos, len, TRANS_ENCODE_OVER)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pTmp, len, TRANS_ENCODE_OVER)
  }

  for (int32_t i = 0; i < undoLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->undoLogs, i);
    int32_t  len = sdbGetRawTotalSize(pTmp);
    SDB_SET_INT32(pRaw, dataPos, len, TRANS_ENCODE_OVER)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pTmp, len, TRANS_ENCODE_OVER)
  }

  for (int32_t i = 0; i < commitLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->commitLogs, i);
    int32_t  len = sdbGetRawTotalSize(pTmp);
    SDB_SET_INT32(pRaw, dataPos, len, TRANS_ENCODE_OVER)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pTmp, len, TRANS_ENCODE_OVER)
  }

  for (int32_t i = 0; i < redoActionNum; ++i) {
    STransAction *pAction = taosArrayGet(pTrans->redoActions, i);
    SDB_SET_BINARY(pRaw, dataPos, (void *)&pAction->epSet, sizeof(SEpSet), TRANS_ENCODE_OVER)
    SDB_SET_INT16(pRaw, dataPos, pAction->msgType, TRANS_ENCODE_OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->acceptableCode, TRANS_ENCODE_OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->contLen, TRANS_ENCODE_OVER)
    SDB_SET_BINARY(pRaw, dataPos, pAction->pCont, pAction->contLen, TRANS_ENCODE_OVER)
  }

  for (int32_t i = 0; i < undoActionNum; ++i) {
    STransAction *pAction = taosArrayGet(pTrans->undoActions, i);
    SDB_SET_BINARY(pRaw, dataPos, (void *)&pAction->epSet, sizeof(SEpSet), TRANS_ENCODE_OVER)
    SDB_SET_INT16(pRaw, dataPos, pAction->msgType, TRANS_ENCODE_OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->acceptableCode, TRANS_ENCODE_OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->contLen, TRANS_ENCODE_OVER)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pAction->pCont, pAction->contLen, TRANS_ENCODE_OVER)
  }

  SDB_SET_RESERVE(pRaw, dataPos, MND_TRANS_RESERVE_SIZE, TRANS_ENCODE_OVER)
  SDB_SET_DATALEN(pRaw, dataPos, TRANS_ENCODE_OVER)

  terrno = 0;

TRANS_ENCODE_OVER:
  if (terrno != 0) {
    mError("trans:%d, failed to encode to raw:%p len:%d since %s", pTrans->id, pRaw, dataPos, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("trans:%d, encode to raw:%p, row:%p len:%d", pTrans->id, pRaw, pTrans, dataPos);
  return pRaw;
}

static SSdbRow *mndTransActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRow     *pRow = NULL;
  STrans      *pTrans = NULL;
  char        *pData = NULL;
  int32_t      dataLen = 0;
  int8_t       sver = 0;
  int32_t      redoLogNum = 0;
  int32_t      undoLogNum = 0;
  int32_t      commitLogNum = 0;
  int32_t      redoActionNum = 0;
  int32_t      undoActionNum = 0;
  int32_t      dataPos = 0;
  STransAction action = {0};

  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto TRANS_DECODE_OVER;

  if (sver != MND_TRANS_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto TRANS_DECODE_OVER;
  }

  pRow = sdbAllocRow(sizeof(STrans));
  if (pRow == NULL) goto TRANS_DECODE_OVER;

  pTrans = sdbGetRowObj(pRow);
  if (pTrans == NULL) goto TRANS_DECODE_OVER;

  SDB_GET_INT32(pRaw, dataPos, &pTrans->id, TRANS_DECODE_OVER)

  int16_t type = 0;
  int16_t policy = 0;
  int16_t stage = 0;
  SDB_GET_INT16(pRaw, dataPos, &policy, TRANS_DECODE_OVER)
  SDB_GET_INT16(pRaw, dataPos, &stage, TRANS_DECODE_OVER)
  SDB_GET_INT16(pRaw, dataPos, &type, TRANS_DECODE_OVER)
  pTrans->policy = policy;
  pTrans->stage = stage;
  pTrans->transType = type;
  SDB_GET_INT64(pRaw, dataPos, &pTrans->createdTime, TRANS_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pTrans->dbUid, TRANS_DECODE_OVER)
  SDB_GET_BINARY(pRaw, dataPos, pTrans->dbname, TSDB_DB_FNAME_LEN, TRANS_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &redoLogNum, TRANS_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &undoLogNum, TRANS_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &commitLogNum, TRANS_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &redoActionNum, TRANS_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &undoActionNum, TRANS_DECODE_OVER)

  pTrans->redoLogs = taosArrayInit(redoLogNum, sizeof(void *));
  pTrans->undoLogs = taosArrayInit(undoLogNum, sizeof(void *));
  pTrans->commitLogs = taosArrayInit(commitLogNum, sizeof(void *));
  pTrans->redoActions = taosArrayInit(redoActionNum, sizeof(STransAction));
  pTrans->undoActions = taosArrayInit(undoActionNum, sizeof(STransAction));

  if (pTrans->redoLogs == NULL) goto TRANS_DECODE_OVER;
  if (pTrans->undoLogs == NULL) goto TRANS_DECODE_OVER;
  if (pTrans->commitLogs == NULL) goto TRANS_DECODE_OVER;
  if (pTrans->redoActions == NULL) goto TRANS_DECODE_OVER;
  if (pTrans->undoActions == NULL) goto TRANS_DECODE_OVER;

  for (int32_t i = 0; i < redoLogNum; ++i) {
    SDB_GET_INT32(pRaw, dataPos, &dataLen, TRANS_DECODE_OVER)
    pData = malloc(dataLen);
    if (pData == NULL) goto TRANS_DECODE_OVER;
    mTrace("raw:%p, is created", pData);
    SDB_GET_BINARY(pRaw, dataPos, pData, dataLen, TRANS_DECODE_OVER);
    if (taosArrayPush(pTrans->redoLogs, &pData) == NULL) goto TRANS_DECODE_OVER;
    pData = NULL;
  }

  for (int32_t i = 0; i < undoLogNum; ++i) {
    SDB_GET_INT32(pRaw, dataPos, &dataLen, TRANS_DECODE_OVER)
    pData = malloc(dataLen);
    if (pData == NULL) goto TRANS_DECODE_OVER;
    mTrace("raw:%p, is created", pData);
    SDB_GET_BINARY(pRaw, dataPos, pData, dataLen, TRANS_DECODE_OVER);
    if (taosArrayPush(pTrans->undoLogs, &pData) == NULL) goto TRANS_DECODE_OVER;
    pData = NULL;
  }

  for (int32_t i = 0; i < commitLogNum; ++i) {
    SDB_GET_INT32(pRaw, dataPos, &dataLen, TRANS_DECODE_OVER)
    pData = malloc(dataLen);
    if (pData == NULL) goto TRANS_DECODE_OVER;
    mTrace("raw:%p, is created", pData);
    SDB_GET_BINARY(pRaw, dataPos, pData, dataLen, TRANS_DECODE_OVER);
    if (taosArrayPush(pTrans->commitLogs, &pData) == NULL) goto TRANS_DECODE_OVER;
    pData = NULL;
  }

  for (int32_t i = 0; i < redoActionNum; ++i) {
    SDB_GET_BINARY(pRaw, dataPos, (void *)&action.epSet, sizeof(SEpSet), TRANS_DECODE_OVER);
    SDB_GET_INT16(pRaw, dataPos, &action.msgType, TRANS_DECODE_OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.acceptableCode, TRANS_DECODE_OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.contLen, TRANS_DECODE_OVER)
    action.pCont = malloc(action.contLen);
    if (action.pCont == NULL) goto TRANS_DECODE_OVER;
    SDB_GET_BINARY(pRaw, dataPos, action.pCont, action.contLen, TRANS_DECODE_OVER);
    if (taosArrayPush(pTrans->redoActions, &action) == NULL) goto TRANS_DECODE_OVER;
    action.pCont = NULL;
  }

  for (int32_t i = 0; i < undoActionNum; ++i) {
    SDB_GET_BINARY(pRaw, dataPos, (void *)&action.epSet, sizeof(SEpSet), TRANS_DECODE_OVER);
    SDB_GET_INT16(pRaw, dataPos, &action.msgType, TRANS_DECODE_OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.acceptableCode, TRANS_DECODE_OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.contLen, TRANS_DECODE_OVER)
    action.pCont = malloc(action.contLen);
    if (action.pCont == NULL) goto TRANS_DECODE_OVER;
    SDB_GET_BINARY(pRaw, dataPos, action.pCont, action.contLen, TRANS_DECODE_OVER);
    if (taosArrayPush(pTrans->undoActions, &action) == NULL) goto TRANS_DECODE_OVER;
    action.pCont = NULL;
  }

  SDB_GET_RESERVE(pRaw, dataPos, MND_TRANS_RESERVE_SIZE, TRANS_DECODE_OVER)

  terrno = 0;

TRANS_DECODE_OVER:
  if (terrno != 0) {
    mError("trans:%d, failed to parse from raw:%p since %s", pTrans->id, pRaw, terrstr());
    mndTransDropData(pTrans);
    tfree(pRow);
    tfree(pData);
    tfree(action.pCont);
    return NULL;
  }

  mTrace("trans:%d, decode from raw:%p, row:%p", pTrans->id, pRaw, pTrans);
  return pRow;
}

static const char *mndTransStr(ETrnStage stage) {
  switch (stage) {
    case TRN_STAGE_PREPARE:
      return "prepare";
    case TRN_STAGE_REDO_LOG:
      return "redoLog";
    case TRN_STAGE_REDO_ACTION:
      return "redoAction";
    case TRN_STAGE_COMMIT:
      return "commit";
    case TRN_STAGE_COMMIT_LOG:
      return "commitLog";
    case TRN_STAGE_UNDO_ACTION:
      return "undoAction";
    case TRN_STAGE_UNDO_LOG:
      return "undoLog";
    case TRN_STAGE_ROLLBACK:
      return "rollback";
    case TRN_STAGE_FINISHED:
      return "finished";
    default:
      return "invalid";
  }
}

static const char *mndTransType(ETrnType type) {
  switch (type) {
    case TRN_TYPE_CREATE_USER:
      return "create-user";
    case TRN_TYPE_ALTER_USER:
      return "alter-user";
    case TRN_TYPE_DROP_USER:
      return "drop-user";
    case TRN_TYPE_CREATE_FUNC:
      return "create-func";
    case TRN_TYPE_DROP_FUNC:
      return "drop-func";
    case TRN_TYPE_CREATE_SNODE:
      return "create-snode";
    case TRN_TYPE_DROP_SNODE:
      return "drop-snode";
    case TRN_TYPE_CREATE_QNODE:
      return "create-qnode";
    case TRN_TYPE_DROP_QNODE:
      return "drop-qnode";
    case TRN_TYPE_CREATE_BNODE:
      return "create-bnode";
    case TRN_TYPE_DROP_BNODE:
      return "drop-bnode";
    case TRN_TYPE_CREATE_MNODE:
      return "create-mnode";
    case TRN_TYPE_DROP_MNODE:
      return "drop-mnode";
    case TRN_TYPE_CREATE_TOPIC:
      return "create-topic";
    case TRN_TYPE_DROP_TOPIC:
      return "drop-topic";
    case TRN_TYPE_SUBSCRIBE:
      return "subscribe";
    case TRN_TYPE_REBALANCE:
      return "rebalance";
    case TRN_TYPE_CREATE_DNODE:
      return "create-qnode";
    case TRN_TYPE_DROP_DNODE:
      return "drop-qnode";
    case TRN_TYPE_CREATE_DB:
      return "create-db";
    case TRN_TYPE_ALTER_DB:
      return "alter-db";
    case TRN_TYPE_DROP_DB:
      return "drop-db";
    case TRN_TYPE_SPLIT_VGROUP:
      return "split-vgroup";
    case TRN_TYPE_MERGE_VGROUP:
      return "merge-vgroup";
    case TRN_TYPE_CREATE_STB:
      return "create-stb";
    case TRN_TYPE_ALTER_STB:
      return "alter-stb";
    case TRN_TYPE_DROP_STB:
      return "drop-stb";
    case TRN_TYPE_CREATE_SMA:
      return "create-sma";
    case TRN_TYPE_DROP_SMA:
      return "drop-sma";
    default:
      return "invalid";
  }
}

static int32_t mndTransActionInsert(SSdb *pSdb, STrans *pTrans) {
  // pTrans->stage = TRN_STAGE_PREPARE;
  mTrace("trans:%d, perform insert action, row:%p stage:%s", pTrans->id, pTrans, mndTransStr(pTrans->stage));
  return 0;
}

static void mndTransDropData(STrans *pTrans) {
  mndTransDropLogs(pTrans->redoLogs);
  mndTransDropLogs(pTrans->undoLogs);
  mndTransDropLogs(pTrans->commitLogs);
  mndTransDropActions(pTrans->redoActions);
  mndTransDropActions(pTrans->undoActions);
  if (pTrans->rpcRsp != NULL) {
    free(pTrans->rpcRsp);
    pTrans->rpcRsp = NULL;
    pTrans->rpcRspLen = 0;
  }
}

static int32_t mndTransActionDelete(SSdb *pSdb, STrans *pTrans) {
  mTrace("trans:%d, perform delete action, row:%p stage:%s", pTrans->id, pTrans, mndTransStr(pTrans->stage));
  mndTransDropData(pTrans);
  return 0;
}

static int32_t mndTransActionUpdate(SSdb *pSdb, STrans *pOld, STrans *pNew) {
  if (pNew->stage == TRN_STAGE_COMMIT) {
    pNew->stage = TRN_STAGE_COMMIT_LOG;
    mTrace("trans:%d, stage from %s to %s", pNew->id, mndTransStr(TRN_STAGE_COMMIT), mndTransStr(TRN_STAGE_COMMIT_LOG));
  }

  if (pNew->stage == TRN_STAGE_ROLLBACK) {
    pNew->stage = TRN_STAGE_FINISHED;
    mTrace("trans:%d, stage from %s to %s", pNew->id, mndTransStr(TRN_STAGE_ROLLBACK), mndTransStr(TRN_STAGE_FINISHED));
  }

  mTrace("trans:%d, perform update action, old row:%p stage:%s, new row:%p stage:%s", pOld->id, pOld,
         mndTransStr(pOld->stage), pNew, mndTransStr(pNew->stage));
  pOld->stage = pNew->stage;
  return 0;
}

static STrans *mndAcquireTrans(SMnode *pMnode, int32_t transId) {
  SSdb   *pSdb = pMnode->pSdb;
  STrans *pTrans = sdbAcquire(pSdb, SDB_TRANS, &transId);
  if (pTrans == NULL) {
    terrno = TSDB_CODE_MND_TRANS_NOT_EXIST;
  }
  return pTrans;
}

static void mndReleaseTrans(SMnode *pMnode, STrans *pTrans) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pTrans);
}

STrans *mndTransCreate(SMnode *pMnode, ETrnPolicy policy, ETrnType type, const SRpcMsg *pReq) {
  STrans *pTrans = calloc(1, sizeof(STrans));
  if (pTrans == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to create transaction since %s", terrstr());
    return NULL;
  }

  pTrans->id = sdbGetMaxId(pMnode->pSdb, SDB_TRANS);
  pTrans->stage = TRN_STAGE_PREPARE;
  pTrans->policy = policy;
  pTrans->transType = type;
  pTrans->createdTime = taosGetTimestampMs();
  pTrans->rpcHandle = pReq->handle;
  pTrans->rpcAHandle = pReq->ahandle;
  pTrans->redoLogs = taosArrayInit(MND_TRANS_ARRAY_SIZE, sizeof(void *));
  pTrans->undoLogs = taosArrayInit(MND_TRANS_ARRAY_SIZE, sizeof(void *));
  pTrans->commitLogs = taosArrayInit(MND_TRANS_ARRAY_SIZE, sizeof(void *));
  pTrans->redoActions = taosArrayInit(MND_TRANS_ARRAY_SIZE, sizeof(STransAction));
  pTrans->undoActions = taosArrayInit(MND_TRANS_ARRAY_SIZE, sizeof(STransAction));

  if (pTrans->redoLogs == NULL || pTrans->undoLogs == NULL || pTrans->commitLogs == NULL ||
      pTrans->redoActions == NULL || pTrans->undoActions == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to create transaction since %s", terrstr());
    return NULL;
  }

  mDebug("trans:%d, is created, data:%p", pTrans->id, pTrans);
  return pTrans;
}

static void mndTransDropLogs(SArray *pArray) {
  int32_t size = taosArrayGetSize(pArray);
  for (int32_t i = 0; i < size; ++i) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, i);
    sdbFreeRaw(pRaw);
  }

  taosArrayDestroy(pArray);
}

static void mndTransDropActions(SArray *pArray) {
  int32_t size = taosArrayGetSize(pArray);
  for (int32_t i = 0; i < size; ++i) {
    STransAction *pAction = taosArrayGet(pArray, i);
    tfree(pAction->pCont);
  }

  taosArrayDestroy(pArray);
}

void mndTransDrop(STrans *pTrans) {
  if (pTrans != NULL) {
    mndTransDropData(pTrans);
    mDebug("trans:%d, is dropped, data:%p", pTrans->id, pTrans);
    tfree(pTrans);
  }
}

static int32_t mndTransAppendLog(SArray *pArray, SSdbRaw *pRaw) {
  if (pArray == NULL || pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  void *ptr = taosArrayPush(pArray, &pRaw);
  if (ptr == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

int32_t mndTransAppendRedolog(STrans *pTrans, SSdbRaw *pRaw) { return mndTransAppendLog(pTrans->redoLogs, pRaw); }

int32_t mndTransAppendUndolog(STrans *pTrans, SSdbRaw *pRaw) { return mndTransAppendLog(pTrans->undoLogs, pRaw); }

int32_t mndTransAppendCommitlog(STrans *pTrans, SSdbRaw *pRaw) { return mndTransAppendLog(pTrans->commitLogs, pRaw); }

static int32_t mndTransAppendAction(SArray *pArray, STransAction *pAction) {
  void *ptr = taosArrayPush(pArray, pAction);
  if (ptr == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

int32_t mndTransAppendRedoAction(STrans *pTrans, STransAction *pAction) {
  return mndTransAppendAction(pTrans->redoActions, pAction);
}

int32_t mndTransAppendUndoAction(STrans *pTrans, STransAction *pAction) {
  return mndTransAppendAction(pTrans->undoActions, pAction);
}

void mndTransSetRpcRsp(STrans *pTrans, void *pCont, int32_t contLen) {
  pTrans->rpcRsp = pCont;
  pTrans->rpcRspLen = contLen;
}

void mndTransSetDbInfo(STrans *pTrans, SDbObj *pDb) {
  pTrans->dbUid = pDb->uid;
  memcpy(pTrans->dbname, pDb->name, TSDB_DB_FNAME_LEN);
}

static int32_t mndTransSync(SMnode *pMnode, STrans *pTrans) {
  SSdbRaw *pRaw = mndTransActionEncode(pTrans);
  if (pRaw == NULL) {
    mError("trans:%d, failed to encode while sync trans since %s", pTrans->id, terrstr());
    return -1;
  }
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mDebug("trans:%d, sync to other nodes", pTrans->id);
  int32_t code = mndSyncPropose(pMnode, pRaw);
  if (code != 0) {
    mError("trans:%d, failed to sync since %s", pTrans->id, terrstr());
    sdbFreeRaw(pRaw);
    return -1;
  }

  mDebug("trans:%d, sync finished", pTrans->id);

  code = sdbWrite(pMnode->pSdb, pRaw);
  if (code != 0) {
    mError("trans:%d, failed to write sdb since %s", pTrans->id, terrstr());
    return -1;
  }

  return 0;
}

static bool mndIsBasicTrans(STrans *pTrans) {
  return pTrans->stage > TRN_TYPE_BASIC_SCOPE && pTrans->stage < TRN_TYPE_BASIC_SCOPE_END;
}

static bool mndIsGlobalTrans(STrans *pTrans) {
  return pTrans->stage > TRN_TYPE_GLOBAL_SCOPE && pTrans->stage < TRN_TYPE_GLOBAL_SCOPE_END;
}

static bool mndIsDbTrans(STrans *pTrans) {
  return pTrans->stage > TRN_TYPE_DB_SCOPE && pTrans->stage < TRN_TYPE_DB_SCOPE_END;
}

static bool mndIsStbTrans(STrans *pTrans) {
  return pTrans->stage > TRN_TYPE_STB_SCOPE && pTrans->stage < TRN_TYPE_STB_SCOPE_END;
}

static int32_t mndCheckTransCanBeStartedInParallel(SMnode *pMnode, STrans *pNewTrans) {
  if (mndIsBasicTrans(pNewTrans)) return 0;

  STrans *pTrans = NULL;
  void   *pIter = NULL;
  int32_t code = 0;

  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_TRANS, pIter, (void **)&pTrans);
    if (pIter == NULL) break;

    if (mndIsGlobalTrans(pNewTrans)) {
      if (mndIsDbTrans(pTrans) || mndIsStbTrans(pTrans)) {
        mError("trans:%d, can't execute since trans:%d in progress db:%s", pNewTrans->id, pTrans->id, pTrans->dbname);
        code = -1;
        break;
      }
    }

    if (mndIsDbTrans(pNewTrans)) {
      if (mndIsBasicTrans(pTrans)) continue;
      if (mndIsGlobalTrans(pTrans)) {
        mError("trans:%d, can't execute since trans:%d in progress", pNewTrans->id, pTrans->id);
        code = -1;
        break;
      }
      if (mndIsDbTrans(pTrans) || mndIsStbTrans(pTrans)) {
        if (pNewTrans->dbUid == pTrans->dbUid) {
          mError("trans:%d, can't execute since trans:%d in progress db:%s", pNewTrans->id, pTrans->id, pTrans->dbname);
          code = -1;
          break;
        }
      }
    }

    if (mndIsStbTrans(pNewTrans)) {
      if (mndIsBasicTrans(pTrans)) continue;
      if (mndIsGlobalTrans(pTrans)) {
        mError("trans:%d, can't execute since trans:%d in progress", pNewTrans->id, pTrans->id);
        code = -1;
        break;
      }
      if (mndIsDbTrans(pTrans)) {
        if (pNewTrans->dbUid == pTrans->dbUid) {
          mError("trans:%d, can't execute since trans:%d in progress db:%s", pNewTrans->id, pTrans->id, pTrans->dbname);
          code = -1;
          break;
        }
      }
      if (mndIsStbTrans(pTrans)) continue;
    }

    sdbRelease(pMnode->pSdb, pTrans);
  }

  sdbCancelFetch(pMnode->pSdb, pIter);
  sdbRelease(pMnode->pSdb, pTrans);
  return code;
}

int32_t mndTransPrepare(SMnode *pMnode, STrans *pTrans) {
  if (mndCheckTransCanBeStartedInParallel(pMnode, pTrans) != 0) {
    terrno = TSDB_CODE_MND_TRANS_CANT_PARALLEL;
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    return -1;
  }

  mDebug("trans:%d, prepare transaction", pTrans->id);
  if (mndTransSync(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    return -1;
  }
  mDebug("trans:%d, prepare finished", pTrans->id);

  STrans *pNew = mndAcquireTrans(pMnode, pTrans->id);
  if (pNew == NULL) {
    mError("trans:%d, failed to read from sdb since %s", pTrans->id, terrstr());
    return -1;
  }

  pNew->rpcHandle = pTrans->rpcHandle;
  pNew->rpcAHandle = pTrans->rpcAHandle;
  pNew->rpcRsp = pTrans->rpcRsp;
  pNew->rpcRspLen = pTrans->rpcRspLen;
  pTrans->rpcRsp = NULL;
  pTrans->rpcRspLen = 0;

  mndTransExecute(pMnode, pNew);
  mndReleaseTrans(pMnode, pNew);
  return 0;
}

static int32_t mndTransCommit(SMnode *pMnode, STrans *pTrans) {
  if (taosArrayGetSize(pTrans->commitLogs) == 0 && taosArrayGetSize(pTrans->redoActions) == 0) return 0;

  mDebug("trans:%d, commit transaction", pTrans->id);
  if (mndTransSync(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to commit since %s", pTrans->id, terrstr());
    return -1;
  }
  mDebug("trans:%d, commit finished", pTrans->id);
  return 0;
}

static int32_t mndTransRollback(SMnode *pMnode, STrans *pTrans) {
  mDebug("trans:%d, rollback transaction", pTrans->id);
  if (mndTransSync(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to rollback since %s", pTrans->id, terrstr());
    return -1;
  }
  mDebug("trans:%d, rollback finished", pTrans->id);
  return 0;
}

static void mndTransSendRpcRsp(STrans *pTrans) {
  bool sendRsp = false;

  if (pTrans->stage == TRN_STAGE_FINISHED) {
    sendRsp = true;
  }

  if (pTrans->policy == TRN_POLICY_ROLLBACK) {
    if (pTrans->stage == TRN_STAGE_UNDO_LOG || pTrans->stage == TRN_STAGE_UNDO_ACTION ||
        pTrans->stage == TRN_STAGE_ROLLBACK) {
      sendRsp = true;
    }
  }

  if (pTrans->policy == TRN_POLICY_RETRY) {
    if (pTrans->stage == TRN_STAGE_REDO_ACTION && pTrans->failedTimes > 0) {
      sendRsp = true;
    }
  }

  if (sendRsp && pTrans->rpcHandle != NULL) {
    void *rpcCont = rpcMallocCont(pTrans->rpcRspLen);
    if (rpcCont != NULL) {
      memcpy(rpcCont, pTrans->rpcRsp, pTrans->rpcRspLen);
    }
    free(pTrans->rpcRsp);

    mDebug("trans:%d, send rsp, code:0x%04x stage:%d app:%p", pTrans->id, pTrans->code & 0xFFFF, pTrans->stage,
           pTrans->rpcAHandle);
    SRpcMsg rspMsg = {.handle = pTrans->rpcHandle,
                      .code = pTrans->code,
                      .ahandle = pTrans->rpcAHandle,
                      .pCont = rpcCont,
                      .contLen = pTrans->rpcRspLen};
    rpcSendResponse(&rspMsg);
    pTrans->rpcHandle = NULL;
    pTrans->rpcRsp = NULL;
    pTrans->rpcRspLen = 0;
  }
}

void mndTransProcessRsp(SNodeMsg *pRsp) {
  SMnode *pMnode = pRsp->pNode;
  int64_t signature = (int64_t)(pRsp->rpcMsg.ahandle);
  int32_t transId = (int32_t)(signature >> 32);
  int32_t action = (int32_t)((signature << 32) >> 32);

  STrans *pTrans = mndAcquireTrans(pMnode, transId);
  if (pTrans == NULL) {
    mError("trans:%d, failed to get transId from vnode rsp since %s", transId, terrstr());
    goto HANDLE_ACTION_RSP_OVER;
  }

  SArray *pArray = NULL;
  if (pTrans->stage == TRN_STAGE_REDO_ACTION) {
    pArray = pTrans->redoActions;
  } else if (pTrans->stage == TRN_STAGE_UNDO_ACTION) {
    pArray = pTrans->undoActions;
  } else {
    mError("trans:%d, invalid trans stage:%d while recv action rsp", pTrans->id, pTrans->stage);
    goto HANDLE_ACTION_RSP_OVER;
  }

  if (pArray == NULL) {
    mError("trans:%d, invalid trans stage:%d", transId, pTrans->stage);
    goto HANDLE_ACTION_RSP_OVER;
  }

  int32_t actionNum = taosArrayGetSize(pTrans->redoActions);
  if (action < 0 || action >= actionNum) {
    mError("trans:%d, invalid action:%d", transId, action);
    goto HANDLE_ACTION_RSP_OVER;
  }

  STransAction *pAction = taosArrayGet(pArray, action);
  if (pAction != NULL) {
    pAction->msgReceived = 1;
    pAction->errCode = pRsp->rpcMsg.code;
    if (pAction->errCode != 0) {
      tstrncpy(pTrans->lastError, tstrerror(pAction->errCode), TSDB_TRANS_ERROR_LEN);
    }
  }

  mDebug("trans:%d, action:%d response is received, code:0x%04x, accept:0x%04x", transId, action, pRsp->rpcMsg.code,
         pAction->acceptableCode);
  mndTransExecute(pMnode, pTrans);

HANDLE_ACTION_RSP_OVER:
  mndReleaseTrans(pMnode, pTrans);
}

static int32_t mndTransExecuteLogs(SMnode *pMnode, SArray *pArray) {
  SSdb   *pSdb = pMnode->pSdb;
  int32_t arraySize = taosArrayGetSize(pArray);

  if (arraySize == 0) return 0;

  for (int32_t i = 0; i < arraySize; ++i) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, i);
    int32_t  code = sdbWriteNotFree(pSdb, pRaw);
    if (code != 0) {
      return code;
    }
  }

  return 0;
}

static int32_t mndTransExecuteRedoLogs(SMnode *pMnode, STrans *pTrans) {
  return mndTransExecuteLogs(pMnode, pTrans->redoLogs);
}

static int32_t mndTransExecuteUndoLogs(SMnode *pMnode, STrans *pTrans) {
  return mndTransExecuteLogs(pMnode, pTrans->undoLogs);
}

static int32_t mndTransExecuteCommitLogs(SMnode *pMnode, STrans *pTrans) {
  return mndTransExecuteLogs(pMnode, pTrans->commitLogs);
}

static void mndTransResetActions(SMnode *pMnode, STrans *pTrans, SArray *pArray) {
  int32_t numOfActions = taosArrayGetSize(pArray);

  for (int32_t action = 0; action < numOfActions; ++action) {
    STransAction *pAction = taosArrayGet(pArray, action);
    if (pAction == NULL) continue;
    if (pAction->msgSent && pAction->msgReceived && pAction->errCode == 0) continue;

    pAction->msgSent = 0;
    pAction->msgReceived = 0;
    pAction->errCode = 0;
    mDebug("trans:%d, action:%d is reset and will be re-executed", pTrans->id, action);
  }
}

static int32_t mndTransSendActionMsg(SMnode *pMnode, STrans *pTrans, SArray *pArray) {
  int32_t numOfActions = taosArrayGetSize(pArray);

  for (int32_t action = 0; action < numOfActions; ++action) {
    STransAction *pAction = taosArrayGet(pArray, action);
    if (pAction == NULL) continue;
    if (pAction->msgSent) continue;

    int64_t signature = pTrans->id;
    signature = (signature << 32);
    signature += action;

    SRpcMsg rpcMsg = {.msgType = pAction->msgType, .contLen = pAction->contLen, .ahandle = (void *)signature};
    rpcMsg.pCont = rpcMallocCont(pAction->contLen);
    if (rpcMsg.pCont == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    memcpy(rpcMsg.pCont, pAction->pCont, pAction->contLen);

    if (tmsgSendReq(&pMnode->msgCb, &pAction->epSet, &rpcMsg) == 0) {
      mDebug("trans:%d, action:%d is sent", pTrans->id, action);
      pAction->msgSent = 1;
      pAction->msgReceived = 0;
      pAction->errCode = 0;
    } else {
      mDebug("trans:%d, action:%d not send since %s", pTrans->id, action, terrstr());
      return -1;
    }
  }

  return 0;
}

static int32_t mndTransExecuteActions(SMnode *pMnode, STrans *pTrans, SArray *pArray) {
  int32_t numOfActions = taosArrayGetSize(pArray);
  if (numOfActions == 0) return 0;

  if (mndTransSendActionMsg(pMnode, pTrans, pArray) != 0) {
    return -1;
  }

  int32_t numOfReceived = 0;
  int32_t errCode = 0;
  for (int32_t action = 0; action < numOfActions; ++action) {
    STransAction *pAction = taosArrayGet(pArray, action);
    if (pAction == NULL) continue;
    if (pAction->msgSent && pAction->msgReceived) {
      numOfReceived++;
      if (pAction->errCode != 0 && pAction->errCode != pAction->acceptableCode) {
        errCode = pAction->errCode;
      }
    }
  }

  if (numOfReceived == numOfActions) {
    if (errCode == 0) {
      mDebug("trans:%d, all %d actions execute successfully", pTrans->id, numOfActions);
      return 0;
    } else {
      mError("trans:%d, all %d actions executed, code:0x%04x", pTrans->id, numOfActions, errCode & 0XFFFF);
      mndTransResetActions(pMnode, pTrans, pArray);
      terrno = errCode;
      return errCode;
    }
  } else {
    mDebug("trans:%d, %d of %d actions executed, code:0x%04x", pTrans->id, numOfReceived, numOfActions, errCode & 0XFFFF);
    return TSDB_CODE_MND_ACTION_IN_PROGRESS;
  }
}

static int32_t mndTransExecuteRedoActions(SMnode *pMnode, STrans *pTrans) {
  return mndTransExecuteActions(pMnode, pTrans, pTrans->redoActions);
}

static int32_t mndTransExecuteUndoActions(SMnode *pMnode, STrans *pTrans) {
  return mndTransExecuteActions(pMnode, pTrans, pTrans->undoActions);
}

static bool mndTransPerformPrepareStage(SMnode *pMnode, STrans *pTrans) {
  bool continueExec = true;
  pTrans->stage = TRN_STAGE_REDO_LOG;
  mDebug("trans:%d, stage from prepare to redoLog", pTrans->id);
  return continueExec;
}

static bool mndTransPerformRedoLogStage(SMnode *pMnode, STrans *pTrans) {
  bool    continueExec = true;
  int32_t code = mndTransExecuteRedoLogs(pMnode, pTrans);

  if (code == 0) {
    pTrans->code = 0;
    pTrans->stage = TRN_STAGE_REDO_ACTION;
    mDebug("trans:%d, stage from redoLog to redoAction", pTrans->id);
  } else {
    pTrans->code = terrno;
    pTrans->stage = TRN_STAGE_UNDO_LOG;
    mError("trans:%d, stage from redoLog to undoLog since %s", pTrans->id, terrstr());
  }

  return continueExec;
}

static bool mndTransPerformRedoActionStage(SMnode *pMnode, STrans *pTrans) {
  bool    continueExec = true;
  int32_t code = mndTransExecuteRedoActions(pMnode, pTrans);

  if (code == 0) {
    pTrans->code = 0;
    pTrans->stage = TRN_STAGE_COMMIT;
    mDebug("trans:%d, stage from redoAction to commit", pTrans->id);
    continueExec = true;
  } else if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mDebug("trans:%d, stage keep on redoAction since %s", pTrans->id, tstrerror(code));
    continueExec = false;
  } else {
    pTrans->code = terrno;
    if (pTrans->policy == TRN_POLICY_ROLLBACK) {
      pTrans->stage = TRN_STAGE_UNDO_ACTION;
      mError("trans:%d, stage from redoAction to undoAction since %s", pTrans->id, terrstr());
      continueExec = true;
    } else {
      pTrans->failedTimes++;
      mError("trans:%d, stage keep on redoAction since %s, failedTimes:%d", pTrans->id, terrstr(), pTrans->failedTimes);
      continueExec = false;
    }
  }

  return continueExec;
}

static bool mndTransPerformCommitStage(SMnode *pMnode, STrans *pTrans) {
  bool    continueExec = true;
  int32_t code = mndTransCommit(pMnode, pTrans);

  if (code == 0) {
    pTrans->code = 0;
    pTrans->stage = TRN_STAGE_COMMIT_LOG;
    mDebug("trans:%d, stage from commit to commitLog", pTrans->id);
    continueExec = true;
  } else {
    pTrans->code = terrno;
    if (pTrans->policy == TRN_POLICY_ROLLBACK) {
      pTrans->stage = TRN_STAGE_REDO_ACTION;
      mError("trans:%d, stage from commit to redoAction since %s, failedTimes:%d", pTrans->id, terrstr(),
             pTrans->failedTimes);
      continueExec = true;
    } else {
      pTrans->failedTimes++;
      mError("trans:%d, stage keep on commit since %s, failedTimes:%d", pTrans->id, terrstr(), pTrans->failedTimes);
      continueExec = false;
    }
  }

  return continueExec;
}

static bool mndTransPerformCommitLogStage(SMnode *pMnode, STrans *pTrans) {
  bool    continueExec = true;
  int32_t code = mndTransExecuteCommitLogs(pMnode, pTrans);

  if (code == 0) {
    pTrans->code = 0;
    pTrans->stage = TRN_STAGE_FINISHED;
    mDebug("trans:%d, stage from commitLog to finished", pTrans->id);
    continueExec = true;
  } else {
    pTrans->code = terrno;
    pTrans->failedTimes++;
    mError("trans:%d, stage keep on commitLog since %s", pTrans->id, terrstr());
    continueExec = false;
  }

  return continueExec;
}

static bool mndTransPerformUndoLogStage(SMnode *pMnode, STrans *pTrans) {
  bool    continueExec = true;
  int32_t code = mndTransExecuteUndoLogs(pMnode, pTrans);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_ROLLBACK;
    mDebug("trans:%d, stage from undoLog to rollback", pTrans->id);
    continueExec = true;
  } else {
    mError("trans:%d, stage keep on undoLog since %s", pTrans->id, terrstr());
    continueExec = false;
  }

  return continueExec;
}

static bool mndTransPerformUndoActionStage(SMnode *pMnode, STrans *pTrans) {
  bool    continueExec = true;
  int32_t code = mndTransExecuteUndoActions(pMnode, pTrans);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_UNDO_LOG;
    mDebug("trans:%d, stage from undoAction to undoLog", pTrans->id);
    continueExec = true;
  } else if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mDebug("trans:%d, stage keep on undoAction since %s", pTrans->id, tstrerror(code));
    continueExec = false;
  } else {
    pTrans->failedTimes++;
    mError("trans:%d, stage keep on undoAction since %s", pTrans->id, terrstr());
    continueExec = false;
  }

  return continueExec;
}

static bool mndTransPerformRollbackStage(SMnode *pMnode, STrans *pTrans) {
  bool    continueExec = true;
  int32_t code = mndTransRollback(pMnode, pTrans);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_FINISHED;
    mDebug("trans:%d, stage from rollback to finished", pTrans->id);
    continueExec = true;
  } else {
    pTrans->failedTimes++;
    mError("trans:%d, stage keep on rollback since %s", pTrans->id, terrstr());
    continueExec = false;
  }

  return continueExec;
}

static bool mndTransPerfromFinishedStage(SMnode *pMnode, STrans *pTrans) {
  bool continueExec = false;

  SSdbRaw *pRaw = mndTransActionEncode(pTrans);
  if (pRaw == NULL) {
    mError("trans:%d, failed to encode while finish trans since %s", pTrans->id, terrstr());
  }
  sdbSetRawStatus(pRaw, SDB_STATUS_DROPPED);

  int32_t code = sdbWrite(pMnode->pSdb, pRaw);
  if (code != 0) {
    mError("trans:%d, failed to write sdb since %s", pTrans->id, terrstr());
  }

  mDebug("trans:%d, finished, code:0x%04x, failedTimes:%d", pTrans->id, pTrans->code, pTrans->failedTimes);
  return continueExec;
}

static void mndTransExecute(SMnode *pMnode, STrans *pTrans) {
  bool continueExec = true;

  while (continueExec) {
    pTrans->lastExecTime = taosGetTimestampMs();
    switch (pTrans->stage) {
      case TRN_STAGE_PREPARE:
        continueExec = mndTransPerformPrepareStage(pMnode, pTrans);
        break;
      case TRN_STAGE_REDO_LOG:
        continueExec = mndTransPerformRedoLogStage(pMnode, pTrans);
        break;
      case TRN_STAGE_REDO_ACTION:
        continueExec = mndTransPerformRedoActionStage(pMnode, pTrans);
        break;
      case TRN_STAGE_UNDO_LOG:
        continueExec = mndTransPerformUndoLogStage(pMnode, pTrans);
        break;
      case TRN_STAGE_UNDO_ACTION:
        continueExec = mndTransPerformUndoActionStage(pMnode, pTrans);
        break;
      case TRN_STAGE_COMMIT_LOG:
        continueExec = mndTransPerformCommitLogStage(pMnode, pTrans);
        break;
      case TRN_STAGE_COMMIT:
        continueExec = mndTransPerformCommitStage(pMnode, pTrans);
        break;
      case TRN_STAGE_ROLLBACK:
        continueExec = mndTransPerformRollbackStage(pMnode, pTrans);
        break;
      case TRN_STAGE_FINISHED:
        continueExec = mndTransPerfromFinishedStage(pMnode, pTrans);
        break;
      default:
        continueExec = false;
        break;
    }
  }

  mndTransSendRpcRsp(pTrans);
}

static int32_t mndProcessTransReq(SNodeMsg *pReq) {
  mndTransPullup(pReq->pNode);
  return 0;
}

static int32_t mndKillTrans(SMnode *pMnode, STrans *pTrans) {
  SArray *pArray = NULL;
  if (pTrans->stage == TRN_STAGE_REDO_ACTION) {
    pArray = pTrans->redoActions;
  } else if (pTrans->stage == TRN_STAGE_UNDO_ACTION) {
    pArray = pTrans->undoActions;
  } else {
    terrno = TSDB_CODE_MND_TRANS_INVALID_STAGE;
    return -1;
  }

  int32_t size = taosArrayGetSize(pArray);

  for (int32_t i = 0; i < size; ++i) {
    STransAction *pAction = taosArrayGet(pArray, i);
    if (pAction == NULL) continue;

    if (pAction->msgReceived == 0) {
      mInfo("trans:%d, action:%d set processed", pTrans->id, i);
      pAction->msgSent = 1;
      pAction->msgReceived = 1;
      pAction->errCode = 0;
    }

    if (pAction->errCode != 0) {
      mInfo("trans:%d, action:%d set processed, errCode from %s to success", pTrans->id, i,
            tstrerror(pAction->errCode));
      pAction->msgSent = 1;
      pAction->msgReceived = 1;
      pAction->errCode = 0;
    }
  }

  mndTransExecute(pMnode, pTrans);
  return 0;
}

static int32_t mndProcessKillTransReq(SNodeMsg *pReq) {
  SMnode       *pMnode = pReq->pNode;
  SKillTransReq killReq = {0};
  int32_t       code = -1;
  SUserObj     *pUser = NULL;
  STrans       *pTrans = NULL;

  if (tDeserializeSKillTransReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &killReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto KILL_OVER;
  }

  mInfo("trans:%d, start to kill", killReq.transId);

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    goto KILL_OVER;
  }

  if (!pUser->superUser) {
    terrno = TSDB_CODE_MND_NO_RIGHTS;
    goto KILL_OVER;
  }

  pTrans = mndAcquireTrans(pMnode, killReq.transId);
  if (pTrans == NULL) {
    terrno = TSDB_CODE_MND_TRANS_NOT_EXIST;
    mError("trans:%d, failed to kill since %s", killReq.transId, terrstr());
    return -1;
  }

  code = mndKillTrans(pMnode, pTrans);

KILL_OVER:
  if (code != 0) {
    mError("trans:%d, failed to kill since %s", killReq.transId, terrstr());
    return -1;
  }

  mndReleaseTrans(pMnode, pTrans);
  return code;
}

void mndTransPullup(SMnode *pMnode) {
  STrans *pTrans = NULL;
  void   *pIter = NULL;

  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_TRANS, pIter, (void **)&pTrans);
    if (pIter == NULL) break;

    mndTransExecute(pMnode, pTrans);
    sdbRelease(pMnode->pSdb, pTrans);
  }

  sdbWriteFile(pMnode->pSdb);
}

static int32_t mndGetTransMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta) {
  SMnode *pMnode = pReq->pNode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchemas;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = TSDB_TRANS_STAGE_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "stage");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = (TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "db");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = TSDB_TRANS_TYPE_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "type");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "last_exec_time");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = (TSDB_TRANS_ERROR_LEN - 1) + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "last_error");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pMeta->numOfColumns = cols;
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_TRANS);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbName, mndShowStr(pShow->type));
  return 0;
}

static int32_t mndRetrieveTrans(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows) {
  SMnode *pMnode = pReq->pNode;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfRows = 0;
  STrans *pTrans = NULL;
  int32_t cols = 0;
  char   *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_TRANS, pShow->pIter, (void **)&pTrans);
    if (pShow->pIter == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pTrans->id;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pTrans->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, mndTransStr(pTrans->stage));
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    char *name = mnGetDbStr(pTrans->dbname);
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, name, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, mndTransType(pTrans->transType));
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pTrans->lastExecTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, pTrans->lastError);
    cols++;

    numOfRows++;
    sdbRelease(pSdb, pTrans);
  }

  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextTrans(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
