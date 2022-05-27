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
#include "mndConsumer.h"
#include "mndDb.h"
#include "mndShow.h"
#include "mndSync.h"
#include "mndUser.h"

#define TRANS_VER_NUMBER   1
#define TRANS_ARRAY_SIZE   8
#define TRANS_RESERVE_SIZE 64

static SSdbRaw *mndTransActionEncode(STrans *pTrans);
static SSdbRow *mndTransActionDecode(SSdbRaw *pRaw);
static int32_t  mndTransActionInsert(SSdb *pSdb, STrans *pTrans);
static int32_t  mndTransActionUpdate(SSdb *pSdb, STrans *OldTrans, STrans *pOld);
static int32_t  mndTransActionDelete(SSdb *pSdb, STrans *pTrans, bool callFunc);

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
static void    mndTransSendRpcRsp(SMnode *pMnode, STrans *pTrans);
static int32_t mndProcessTransReq(SRpcMsg *pReq);
static int32_t mndProcessKillTransReq(SRpcMsg *pReq);

static int32_t mndRetrieveTrans(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextTrans(SMnode *pMnode, void *pIter);

int32_t mndInitTrans(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_TRANS,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndTransActionEncode,
      .decodeFp = (SdbDecodeFp)mndTransActionDecode,
      .insertFp = (SdbInsertFp)mndTransActionInsert,
      .updateFp = (SdbUpdateFp)mndTransActionUpdate,
      .deleteFp = (SdbDeleteFp)mndTransActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_TRANS_TIMER, mndProcessTransReq);
  mndSetMsgHandle(pMnode, TDMT_MND_KILL_TRANS, mndProcessKillTransReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TRANS, mndRetrieveTrans);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TRANS, mndCancelGetNextTrans);
  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupTrans(SMnode *pMnode) {}

static SSdbRaw *mndTransActionEncode(STrans *pTrans) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t rawDataLen = sizeof(STrans) + TRANS_RESERVE_SIZE;
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

  SSdbRaw *pRaw = sdbAllocRaw(SDB_TRANS, TRANS_VER_NUMBER, rawDataLen);
  if (pRaw == NULL) {
    mError("trans:%d, failed to alloc raw since %s", pTrans->id, terrstr());
    return NULL;
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pTrans->id, _OVER)

  ETrnStage stage = pTrans->stage;
  if (stage == TRN_STAGE_REDO_LOG || stage == TRN_STAGE_REDO_ACTION) {
    stage = TRN_STAGE_PREPARE;
  } else if (stage == TRN_STAGE_UNDO_ACTION || stage == TRN_STAGE_UNDO_LOG) {
    stage = TRN_STAGE_ROLLBACK;
  } else if (stage == TRN_STAGE_COMMIT_LOG || stage == TRN_STAGE_FINISHED) {
    stage = TRN_STAGE_COMMIT;
  } else {
  }

  SDB_SET_INT16(pRaw, dataPos, stage, _OVER)
  SDB_SET_INT16(pRaw, dataPos, pTrans->policy, _OVER)
  SDB_SET_INT16(pRaw, dataPos, pTrans->type, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pTrans->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pTrans->dbUid, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pTrans->dbname, TSDB_DB_FNAME_LEN, _OVER)
  SDB_SET_INT32(pRaw, dataPos, redoLogNum, _OVER)
  SDB_SET_INT32(pRaw, dataPos, undoLogNum, _OVER)
  SDB_SET_INT32(pRaw, dataPos, commitLogNum, _OVER)
  SDB_SET_INT32(pRaw, dataPos, redoActionNum, _OVER)
  SDB_SET_INT32(pRaw, dataPos, undoActionNum, _OVER)

  for (int32_t i = 0; i < redoLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->redoLogs, i);
    int32_t  len = sdbGetRawTotalSize(pTmp);
    SDB_SET_INT32(pRaw, dataPos, len, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pTmp, len, _OVER)
  }

  for (int32_t i = 0; i < undoLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->undoLogs, i);
    int32_t  len = sdbGetRawTotalSize(pTmp);
    SDB_SET_INT32(pRaw, dataPos, len, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pTmp, len, _OVER)
  }

  for (int32_t i = 0; i < commitLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->commitLogs, i);
    int32_t  len = sdbGetRawTotalSize(pTmp);
    SDB_SET_INT32(pRaw, dataPos, len, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pTmp, len, _OVER)
  }

  for (int32_t i = 0; i < redoActionNum; ++i) {
    STransAction *pAction = taosArrayGet(pTrans->redoActions, i);
    SDB_SET_BINARY(pRaw, dataPos, (void *)&pAction->epSet, sizeof(SEpSet), _OVER)
    SDB_SET_INT16(pRaw, dataPos, pAction->msgType, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->acceptableCode, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->contLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, pAction->pCont, pAction->contLen, _OVER)
  }

  for (int32_t i = 0; i < undoActionNum; ++i) {
    STransAction *pAction = taosArrayGet(pTrans->undoActions, i);
    SDB_SET_BINARY(pRaw, dataPos, (void *)&pAction->epSet, sizeof(SEpSet), _OVER)
    SDB_SET_INT16(pRaw, dataPos, pAction->msgType, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->acceptableCode, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->contLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pAction->pCont, pAction->contLen, _OVER)
  }

  SDB_SET_INT32(pRaw, dataPos, pTrans->startFunc, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pTrans->stopFunc, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pTrans->paramLen, _OVER)
  if (pTrans->param != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, pTrans->param, pTrans->paramLen, _OVER)
  }

  SDB_SET_RESERVE(pRaw, dataPos, TRANS_RESERVE_SIZE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER)

  terrno = 0;

_OVER:
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

  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != TRANS_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(STrans));
  if (pRow == NULL) goto _OVER;

  pTrans = sdbGetRowObj(pRow);
  if (pTrans == NULL) goto _OVER;

  SDB_GET_INT32(pRaw, dataPos, &pTrans->id, _OVER)

  int16_t stage = 0;
  int16_t policy = 0;
  int16_t type = 0;
  SDB_GET_INT16(pRaw, dataPos, &stage, _OVER)
  SDB_GET_INT16(pRaw, dataPos, &policy, _OVER)
  SDB_GET_INT16(pRaw, dataPos, &type, _OVER)
  pTrans->stage = stage;
  pTrans->policy = policy;
  pTrans->type = type;
  SDB_GET_INT64(pRaw, dataPos, &pTrans->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pTrans->dbUid, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pTrans->dbname, TSDB_DB_FNAME_LEN, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &redoLogNum, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &undoLogNum, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &commitLogNum, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &redoActionNum, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &undoActionNum, _OVER)

  pTrans->redoLogs = taosArrayInit(redoLogNum, sizeof(void *));
  pTrans->undoLogs = taosArrayInit(undoLogNum, sizeof(void *));
  pTrans->commitLogs = taosArrayInit(commitLogNum, sizeof(void *));
  pTrans->redoActions = taosArrayInit(redoActionNum, sizeof(STransAction));
  pTrans->undoActions = taosArrayInit(undoActionNum, sizeof(STransAction));

  if (pTrans->redoLogs == NULL) goto _OVER;
  if (pTrans->undoLogs == NULL) goto _OVER;
  if (pTrans->commitLogs == NULL) goto _OVER;
  if (pTrans->redoActions == NULL) goto _OVER;
  if (pTrans->undoActions == NULL) goto _OVER;

  for (int32_t i = 0; i < redoLogNum; ++i) {
    SDB_GET_INT32(pRaw, dataPos, &dataLen, _OVER)
    pData = taosMemoryMalloc(dataLen);
    if (pData == NULL) goto _OVER;
    mTrace("raw:%p, is created", pData);
    SDB_GET_BINARY(pRaw, dataPos, pData, dataLen, _OVER);
    if (taosArrayPush(pTrans->redoLogs, &pData) == NULL) goto _OVER;
    pData = NULL;
  }

  for (int32_t i = 0; i < undoLogNum; ++i) {
    SDB_GET_INT32(pRaw, dataPos, &dataLen, _OVER)
    pData = taosMemoryMalloc(dataLen);
    if (pData == NULL) goto _OVER;
    mTrace("raw:%p, is created", pData);
    SDB_GET_BINARY(pRaw, dataPos, pData, dataLen, _OVER);
    if (taosArrayPush(pTrans->undoLogs, &pData) == NULL) goto _OVER;
    pData = NULL;
  }

  for (int32_t i = 0; i < commitLogNum; ++i) {
    SDB_GET_INT32(pRaw, dataPos, &dataLen, _OVER)
    pData = taosMemoryMalloc(dataLen);
    if (pData == NULL) goto _OVER;
    mTrace("raw:%p, is created", pData);
    SDB_GET_BINARY(pRaw, dataPos, pData, dataLen, _OVER);
    if (taosArrayPush(pTrans->commitLogs, &pData) == NULL) goto _OVER;
    pData = NULL;
  }

  for (int32_t i = 0; i < redoActionNum; ++i) {
    SDB_GET_BINARY(pRaw, dataPos, (void *)&action.epSet, sizeof(SEpSet), _OVER);
    SDB_GET_INT16(pRaw, dataPos, &action.msgType, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.acceptableCode, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.contLen, _OVER)
    action.pCont = taosMemoryMalloc(action.contLen);
    if (action.pCont == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, action.pCont, action.contLen, _OVER);
    if (taosArrayPush(pTrans->redoActions, &action) == NULL) goto _OVER;
    action.pCont = NULL;
  }

  for (int32_t i = 0; i < undoActionNum; ++i) {
    SDB_GET_BINARY(pRaw, dataPos, (void *)&action.epSet, sizeof(SEpSet), _OVER);
    SDB_GET_INT16(pRaw, dataPos, &action.msgType, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.acceptableCode, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.contLen, _OVER)
    action.pCont = taosMemoryMalloc(action.contLen);
    if (action.pCont == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, action.pCont, action.contLen, _OVER);
    if (taosArrayPush(pTrans->undoActions, &action) == NULL) goto _OVER;
    action.pCont = NULL;
  }

  SDB_GET_INT32(pRaw, dataPos, &pTrans->startFunc, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pTrans->stopFunc, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pTrans->paramLen, _OVER)
  if (pTrans->paramLen != 0) {
    pTrans->param = taosMemoryMalloc(pTrans->paramLen);
    SDB_GET_BINARY(pRaw, dataPos, pTrans->param, pTrans->paramLen, _OVER);
  }

  SDB_GET_RESERVE(pRaw, dataPos, TRANS_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("trans:%d, failed to parse from raw:%p since %s", pTrans->id, pRaw, terrstr());
    mndTransDropData(pTrans);
    taosMemoryFreeClear(pRow);
    taosMemoryFreeClear(pData);
    taosMemoryFreeClear(action.pCont);
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
    case TRN_TYPE_COMMIT_OFFSET:
      return "commit-offset";
    case TRN_TYPE_CREATE_STREAM:
      return "create-stream";
    case TRN_TYPE_DROP_STREAM:
      return "drop-stream";
    case TRN_TYPE_CONSUMER_LOST:
      return "consumer-lost";
    case TRN_TYPE_CONSUMER_RECOVER:
      return "consumer-recover";
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

static void mndTransTestStartFunc(SMnode *pMnode, void *param, int32_t paramLen) {
  mInfo("test trans start, param:%s, len:%d", (char *)param, paramLen);
}

static void mndTransTestStopFunc(SMnode *pMnode, void *param, int32_t paramLen) {
  mInfo("test trans stop, param:%s, len:%d", (char *)param, paramLen);
}

static TransCbFp mndTransGetCbFp(ETrnFuncType ftype) {
  switch (ftype) {
    case TEST_TRANS_START_FUNC:
      return mndTransTestStartFunc;
    case TEST_TRANS_STOP_FUNC:
      return mndTransTestStopFunc;
    case MQ_REB_TRANS_START_FUNC:
      return mndRebCntInc;
    case MQ_REB_TRANS_STOP_FUNC:
      return mndRebCntDec;
    default:
      return NULL;
  }
}

static int32_t mndTransActionInsert(SSdb *pSdb, STrans *pTrans) {
  mTrace("trans:%d, perform insert action, row:%p stage:%s", pTrans->id, pTrans, mndTransStr(pTrans->stage));

  if (pTrans->startFunc > 0) {
    TransCbFp fp = mndTransGetCbFp(pTrans->startFunc);
    if (fp) {
      (*fp)(pSdb->pMnode, pTrans->param, pTrans->paramLen);
    }
  }

  return 0;
}

static void mndTransDropData(STrans *pTrans) {
  mndTransDropLogs(pTrans->redoLogs);
  mndTransDropLogs(pTrans->undoLogs);
  mndTransDropLogs(pTrans->commitLogs);
  mndTransDropActions(pTrans->redoActions);
  mndTransDropActions(pTrans->undoActions);
  if (pTrans->rpcRsp != NULL) {
    taosMemoryFree(pTrans->rpcRsp);
    pTrans->rpcRsp = NULL;
    pTrans->rpcRspLen = 0;
  }
  if (pTrans->param != NULL) {
    taosMemoryFree(pTrans->param);
    pTrans->param = NULL;
    pTrans->paramLen = 0;
  }
}

static int32_t mndTransActionDelete(SSdb *pSdb, STrans *pTrans, bool callFunc) {
  mDebug("trans:%d, perform delete action, row:%p stage:%s callfunc:%d", pTrans->id, pTrans, mndTransStr(pTrans->stage),
         callFunc);
  if (pTrans->stopFunc > 0 && callFunc) {
    TransCbFp fp = mndTransGetCbFp(pTrans->stopFunc);
    if (fp) {
      (*fp)(pSdb->pMnode, pTrans->param, pTrans->paramLen);
    }
  }

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

STrans *mndAcquireTrans(SMnode *pMnode, int32_t transId) {
  STrans *pTrans = sdbAcquire(pMnode->pSdb, SDB_TRANS, &transId);
  if (pTrans == NULL) {
    terrno = TSDB_CODE_MND_TRANS_NOT_EXIST;
  }
  return pTrans;
}

void mndReleaseTrans(SMnode *pMnode, STrans *pTrans) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pTrans);
}

STrans *mndTransCreate(SMnode *pMnode, ETrnPolicy policy, ETrnType type, const SRpcMsg *pReq) {
  STrans *pTrans = taosMemoryCalloc(1, sizeof(STrans));
  if (pTrans == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to create transaction since %s", terrstr());
    return NULL;
  }

  pTrans->id = sdbGetMaxId(pMnode->pSdb, SDB_TRANS);
  pTrans->stage = TRN_STAGE_PREPARE;
  pTrans->policy = policy;
  pTrans->type = type;
  pTrans->createdTime = taosGetTimestampMs();
  if (pReq != NULL) pTrans->rpcInfo = pReq->info;
  pTrans->redoLogs = taosArrayInit(TRANS_ARRAY_SIZE, sizeof(void *));
  pTrans->undoLogs = taosArrayInit(TRANS_ARRAY_SIZE, sizeof(void *));
  pTrans->commitLogs = taosArrayInit(TRANS_ARRAY_SIZE, sizeof(void *));
  pTrans->redoActions = taosArrayInit(TRANS_ARRAY_SIZE, sizeof(STransAction));
  pTrans->undoActions = taosArrayInit(TRANS_ARRAY_SIZE, sizeof(STransAction));

  if (pTrans->redoLogs == NULL || pTrans->undoLogs == NULL || pTrans->commitLogs == NULL ||
      pTrans->redoActions == NULL || pTrans->undoActions == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to create transaction since %s", terrstr());
    return NULL;
  }

  mDebug("trans:%d, local object is created, data:%p", pTrans->id, pTrans);
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
    taosMemoryFreeClear(pAction->pCont);
  }

  taosArrayDestroy(pArray);
}

void mndTransDrop(STrans *pTrans) {
  if (pTrans != NULL) {
    mndTransDropData(pTrans);
    mDebug("trans:%d, local object is freed, data:%p", pTrans->id, pTrans);
    taosMemoryFreeClear(pTrans);
  }
}

static int32_t mndTransAppendLog(SArray *pArray, SSdbRaw *pRaw) {
  if (pArray == NULL || pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
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

void mndTransSetCb(STrans *pTrans, ETrnFuncType startFunc, ETrnFuncType stopFunc, void *param, int32_t paramLen) {
  pTrans->startFunc = startFunc;
  pTrans->stopFunc = stopFunc;
  pTrans->param = param;
  pTrans->paramLen = paramLen;
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

  sdbFreeRaw(pRaw);
  mDebug("trans:%d, sync finished", pTrans->id);
  return 0;
}

static bool mndIsBasicTrans(STrans *pTrans) {
  return pTrans->type > TRN_TYPE_BASIC_SCOPE && pTrans->type < TRN_TYPE_BASIC_SCOPE_END;
}

static bool mndIsGlobalTrans(STrans *pTrans) {
  return pTrans->type > TRN_TYPE_GLOBAL_SCOPE && pTrans->type < TRN_TYPE_GLOBAL_SCOPE_END;
}

static bool mndIsDbTrans(STrans *pTrans) {
  return pTrans->type > TRN_TYPE_DB_SCOPE && pTrans->type < TRN_TYPE_DB_SCOPE_END;
}

static bool mndIsStbTrans(STrans *pTrans) {
  return pTrans->type > TRN_TYPE_STB_SCOPE && pTrans->type < TRN_TYPE_STB_SCOPE_END;
}

static bool mndCheckTransConflict(SMnode *pMnode, STrans *pNewTrans) {
  STrans *pTrans = NULL;
  void   *pIter = NULL;
  bool    conflict = false;

  if (mndIsBasicTrans(pNewTrans)) return conflict;

  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_TRANS, pIter, (void **)&pTrans);
    if (pIter == NULL) break;

    if (mndIsGlobalTrans(pNewTrans)) {
      if (mndIsDbTrans(pTrans) || mndIsStbTrans(pTrans)) {
        mError("trans:%d, can't execute since trans:%d in progress db:%s", pNewTrans->id, pTrans->id, pTrans->dbname);
        conflict = true;
      } else {
      }
    }

    else if (mndIsDbTrans(pNewTrans)) {
      if (mndIsGlobalTrans(pTrans)) {
        mError("trans:%d, can't execute since trans:%d in progress", pNewTrans->id, pTrans->id);
        conflict = true;
      } else if (mndIsDbTrans(pTrans) || mndIsStbTrans(pTrans)) {
        if (pNewTrans->dbUid == pTrans->dbUid) {
          mError("trans:%d, can't execute since trans:%d in progress db:%s", pNewTrans->id, pTrans->id, pTrans->dbname);
          conflict = true;
        }
      } else {
      }
    }

    else if (mndIsStbTrans(pNewTrans)) {
      if (mndIsGlobalTrans(pTrans)) {
        mError("trans:%d, can't execute since trans:%d in progress", pNewTrans->id, pTrans->id);
        conflict = true;
      } else if (mndIsDbTrans(pTrans)) {
        if (pNewTrans->dbUid == pTrans->dbUid) {
          mError("trans:%d, can't execute since trans:%d in progress db:%s", pNewTrans->id, pTrans->id, pTrans->dbname);
          conflict = true;
        }
      } else {
      }
    }

    sdbRelease(pMnode->pSdb, pTrans);
  }

  sdbCancelFetch(pMnode->pSdb, pIter);
  sdbRelease(pMnode->pSdb, pTrans);
  return conflict;
}

int32_t mndTransPrepare(SMnode *pMnode, STrans *pTrans) {
  if (mndCheckTransConflict(pMnode, pTrans)) {
    terrno = TSDB_CODE_MND_TRANS_CONFLICT;
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    return -1;
  }

  if (taosArrayGetSize(pTrans->commitLogs) <= 0) {
    terrno = TSDB_CODE_MND_TRANS_CLOG_IS_NULL;
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

  pNew->rpcInfo = pTrans->rpcInfo;
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

static void mndTransSendRpcRsp(SMnode *pMnode, STrans *pTrans) {
  bool    sendRsp = false;
  int32_t code = pTrans->code;

  if (pTrans->stage == TRN_STAGE_FINISHED) {
    sendRsp = true;
  }

  if (pTrans->policy == TRN_POLICY_ROLLBACK) {
    if (pTrans->stage == TRN_STAGE_UNDO_LOG || pTrans->stage == TRN_STAGE_UNDO_ACTION ||
        pTrans->stage == TRN_STAGE_ROLLBACK) {
      if (code == 0) code = TSDB_CODE_MND_TRANS_UNKNOW_ERROR;
      sendRsp = true;
    }
  } else {
    if (pTrans->stage == TRN_STAGE_REDO_ACTION && pTrans->failedTimes > 6) {
      if (code == 0) code = TSDB_CODE_MND_TRANS_UNKNOW_ERROR;
      sendRsp = true;
    }
  }

  if (sendRsp && pTrans->rpcInfo.handle != NULL) {
    void *rpcCont = rpcMallocCont(pTrans->rpcRspLen);
    if (rpcCont != NULL) {
      memcpy(rpcCont, pTrans->rpcRsp, pTrans->rpcRspLen);
    }
    taosMemoryFree(pTrans->rpcRsp);

    mDebug("trans:%d, send rsp, code:0x%x stage:%d app:%p", pTrans->id, code, pTrans->stage, pTrans->rpcInfo.ahandle);
    SRpcMsg rspMsg = {
        .code = code,
        .pCont = rpcCont,
        .contLen = pTrans->rpcRspLen,
        .info = pTrans->rpcInfo,
    };
    tmsgSendRsp(&rspMsg);
    pTrans->rpcInfo.handle = NULL;
    pTrans->rpcRsp = NULL;
    pTrans->rpcRspLen = 0;
  }
}

void mndTransProcessRsp(SRpcMsg *pRsp) {
  SMnode *pMnode = pRsp->info.node;
  int64_t signature = (int64_t)(pRsp->info.ahandle);
  int32_t transId = (int32_t)(signature >> 32);
  int32_t action = (int32_t)((signature << 32) >> 32);

  STrans *pTrans = mndAcquireTrans(pMnode, transId);
  if (pTrans == NULL) {
    mError("trans:%d, failed to get transId from vnode rsp since %s", transId, terrstr());
    goto _OVER;
  }

  SArray *pArray = NULL;
  if (pTrans->stage == TRN_STAGE_REDO_ACTION) {
    pArray = pTrans->redoActions;
  } else if (pTrans->stage == TRN_STAGE_UNDO_ACTION) {
    pArray = pTrans->undoActions;
  } else {
    mError("trans:%d, invalid trans stage:%d while recv action rsp", pTrans->id, pTrans->stage);
    goto _OVER;
  }

  if (pArray == NULL) {
    mError("trans:%d, invalid trans stage:%d", transId, pTrans->stage);
    goto _OVER;
  }

  int32_t actionNum = taosArrayGetSize(pTrans->redoActions);
  if (action < 0 || action >= actionNum) {
    mError("trans:%d, invalid action:%d", transId, action);
    goto _OVER;
  }

  STransAction *pAction = taosArrayGet(pArray, action);
  if (pAction != NULL) {
    pAction->msgReceived = 1;
    pAction->errCode = pRsp->code;
    if (pAction->errCode != 0) {
      tstrncpy(pTrans->lastError, tstrerror(pAction->errCode), TSDB_TRANS_ERROR_LEN);
    }
  }

  mDebug("trans:%d, action:%d response is received, code:0x%x, accept:0x%04x", transId, action, pRsp->code,
         pAction->acceptableCode);
  mndTransExecute(pMnode, pTrans);

_OVER:
  mndReleaseTrans(pMnode, pTrans);
}

static int32_t mndTransExecuteLogs(SMnode *pMnode, SArray *pArray) {
  SSdb   *pSdb = pMnode->pSdb;
  int32_t arraySize = taosArrayGetSize(pArray);

  if (arraySize == 0) return 0;

  int32_t code = 0;
  for (int32_t i = 0; i < arraySize; ++i) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, i);
    if (sdbWriteWithoutFree(pSdb, pRaw) != 0) {
      code = ((terrno != 0) ? terrno : -1);
    }
  }

  terrno = code;
  return code;
}

static int32_t mndTransExecuteRedoLogs(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteLogs(pMnode, pTrans->redoLogs);
  if (code != 0) {
    mError("failed to execute redoLogs since %s", terrstr());
  }
  return code;
}

static int32_t mndTransExecuteUndoLogs(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteLogs(pMnode, pTrans->undoLogs);
  if (code != 0) {
    mError("failed to execute undoLogs since %s, return success", terrstr());
  }

  return 0;  // return success in any case
}

static int32_t mndTransExecuteCommitLogs(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteLogs(pMnode, pTrans->commitLogs);
  if (code != 0) {
    mError("failed to execute commitLogs since %s", terrstr());
  }
  return code;
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
    mDebug("trans:%d, action:%d execute status is reset", pTrans->id, action);
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

    SRpcMsg rpcMsg = {.msgType = pAction->msgType, .contLen = pAction->contLen, .info.ahandle = (void *)signature};
    rpcMsg.pCont = rpcMallocCont(pAction->contLen);
    if (rpcMsg.pCont == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    memcpy(rpcMsg.pCont, pAction->pCont, pAction->contLen);

    if (tmsgSendReq(&pAction->epSet, &rpcMsg) == 0) {
      mDebug("trans:%d, action:%d is sent to %s:%u", pTrans->id, action, pAction->epSet.eps[pAction->epSet.inUse].fqdn,
             pAction->epSet.eps[pAction->epSet.inUse].port);
      pAction->msgSent = 1;
      pAction->msgReceived = 0;
      pAction->errCode = 0;
    } else {
      pAction->msgSent = 0;
      pAction->msgReceived = 0;
      pAction->errCode = terrno;
      mError("trans:%d, action:%d not send since %s", pTrans->id, action, terrstr());
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
      mError("trans:%d, all %d actions executed, code:0x%x", pTrans->id, numOfActions, errCode & 0XFFFF);
      mndTransResetActions(pMnode, pTrans, pArray);
      terrno = errCode;
      return errCode;
    }
  } else {
    mDebug("trans:%d, %d of %d actions executed", pTrans->id, numOfReceived, numOfActions);
    return TSDB_CODE_ACTION_IN_PROGRESS;
  }
}

static int32_t mndTransExecuteRedoActions(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteActions(pMnode, pTrans, pTrans->redoActions);
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to execute redoActions since:%s, code:0x%x", terrstr(), terrno);
  }
  return code;
}

static int32_t mndTransExecuteUndoActions(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteActions(pMnode, pTrans, pTrans->undoActions);
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to execute undoActions since %s", terrstr());
  }
  return code;
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
  if (!pMnode->deploy && !mndIsMaster(pMnode)) return false;

  bool    continueExec = true;
  int32_t code = mndTransExecuteRedoActions(pMnode, pTrans);

  if (code == 0) {
    pTrans->code = 0;
    pTrans->stage = TRN_STAGE_COMMIT;
    mDebug("trans:%d, stage from redoAction to commit", pTrans->id);
    continueExec = true;
  } else if (code == TSDB_CODE_ACTION_IN_PROGRESS) {
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
      pTrans->stage = TRN_STAGE_UNDO_ACTION;
      mError("trans:%d, stage from commit to undoAction since %s, failedTimes:%d", pTrans->id, terrstr(),
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
    mError("trans:%d, stage keep on commitLog since %s, failedTimes:%d", pTrans->id, terrstr(), pTrans->failedTimes);
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
  if (!pMnode->deploy && !mndIsMaster(pMnode)) return false;

  bool    continueExec = true;
  int32_t code = mndTransExecuteUndoActions(pMnode, pTrans);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_UNDO_LOG;
    mDebug("trans:%d, stage from undoAction to undoLog", pTrans->id);
    continueExec = true;
  } else if (code == TSDB_CODE_ACTION_IN_PROGRESS) {
    mDebug("trans:%d, stage keep on undoAction since %s", pTrans->id, tstrerror(code));
    continueExec = false;
  } else {
    pTrans->failedTimes++;
    mError("trans:%d, stage keep on undoAction since %s, failedTimes:%d", pTrans->id, terrstr(), pTrans->failedTimes);
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
    mError("trans:%d, stage keep on rollback since %s, failedTimes:%d", pTrans->id, terrstr(), pTrans->failedTimes);
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

  mDebug("trans:%d, finished, code:0x%x, failedTimes:%d", pTrans->id, pTrans->code, pTrans->failedTimes);

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

  mndTransSendRpcRsp(pMnode, pTrans);
}

static int32_t mndProcessTransReq(SRpcMsg *pReq) {
  mndTransPullup(pReq->info.node);
  return 0;
}

int32_t mndKillTrans(SMnode *pMnode, STrans *pTrans) {
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
      mInfo("trans:%d, action:%d set processed for kill msg received", pTrans->id, i);
      pAction->msgSent = 1;
      pAction->msgReceived = 1;
      pAction->errCode = 0;
    }

    if (pAction->errCode != 0) {
      mInfo("trans:%d, action:%d set processed for kill msg received, errCode from %s to success", pTrans->id, i,
            tstrerror(pAction->errCode));
      pAction->msgSent = 1;
      pAction->msgReceived = 1;
      pAction->errCode = 0;
    }
  }

  mndTransExecute(pMnode, pTrans);
  return 0;
}

static int32_t mndProcessKillTransReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  SKillTransReq killReq = {0};
  int32_t       code = -1;
  SUserObj     *pUser = NULL;
  STrans       *pTrans = NULL;

  if (tDeserializeSKillTransReq(pReq->pCont, pReq->contLen, &killReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("trans:%d, start to kill", killReq.transId);

  pUser = mndAcquireUser(pMnode, pReq->conn.user);
  if (pUser == NULL) {
    goto _OVER;
  }

  if (mndCheckTransAuth(pUser) != 0) {
    goto _OVER;
  }

  pTrans = mndAcquireTrans(pMnode, killReq.transId);
  if (pTrans == NULL) {
    terrno = TSDB_CODE_MND_TRANS_NOT_EXIST;
    mError("trans:%d, failed to kill since %s", killReq.transId, terrstr());
    return -1;
  }

  code = mndKillTrans(pMnode, pTrans);

_OVER:
  if (code != 0) {
    mError("trans:%d, failed to kill since %s", killReq.transId, terrstr());
    return -1;
  }

  mndReleaseTrans(pMnode, pTrans);
  return code;
}

static int32_t mndCompareTransId(int32_t *pTransId1, int32_t *pTransId2) { return *pTransId1 >= *pTransId2 ? 1 : 0; }

void mndTransPullup(SMnode *pMnode) {
  SSdb   *pSdb = pMnode->pSdb;
  SArray *pArray = taosArrayInit(sdbGetSize(pSdb, SDB_TRANS), sizeof(int32_t));
  if (pArray == NULL) return;

  void *pIter = NULL;
  while (1) {
    STrans *pTrans = NULL;
    pIter = sdbFetch(pMnode->pSdb, SDB_TRANS, pIter, (void **)&pTrans);
    if (pIter == NULL) break;
    taosArrayPush(pArray, &pTrans->id);
    sdbRelease(pSdb, pTrans);
  }

  taosArraySort(pArray, (__compar_fn_t)mndCompareTransId);

  for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
    int32_t *pTransId = taosArrayGet(pArray, i);
    STrans  *pTrans = mndAcquireTrans(pMnode, *pTransId);
    if (pTrans != NULL) {
      mndTransExecute(pMnode, pTrans);
    }
    mndReleaseTrans(pMnode, pTrans);
  }

  sdbWriteFile(pMnode->pSdb);
  taosArrayDestroy(pArray);
}

static int32_t mndRetrieveTrans(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfRows = 0;
  STrans *pTrans = NULL;
  int32_t cols = 0;
  char   *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_TRANS, pShow->pIter, (void **)&pTrans);
    if (pShow->pIter == NULL) break;

    cols = 0;

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pTrans->id, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pTrans->createdTime, false);

    char stage[TSDB_TRANS_STAGE_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(stage, mndTransStr(pTrans->stage), pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)stage, false);

    char dbname[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(dbname, mndGetDbStr(pTrans->dbname), pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)dbname, false);

    char type[TSDB_TRANS_TYPE_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(type, mndTransType(pTrans->type), pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)type, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pTrans->failedTimes, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pTrans->lastExecTime, false);

    char lastError[TSDB_TRANS_ERROR_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(lastError, pTrans->lastError, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)lastError, false);

    numOfRows++;
    sdbRelease(pSdb, pTrans);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextTrans(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
