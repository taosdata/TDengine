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
static int32_t mndTransExecuteActions(SMnode *pMnode, STrans *pTrans, SArray *pArray);
static int32_t mndTransExecuteRedoLogs(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransExecuteUndoLogs(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransExecuteRedoActions(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransExecuteUndoActions(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransExecuteCommitActions(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerformPrepareStage(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerformRedoLogStage(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerformRedoActionStage(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerformUndoLogStage(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerformUndoActionStage(SMnode *pMnode, STrans *pTrans);
static bool    mndTransPerformCommitActionStage(SMnode *pMnode, STrans *pTrans);
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

static int32_t mndTransGetActionsSize(SArray *pArray) {
  int32_t actionNum = taosArrayGetSize(pArray);
  int32_t rawDataLen = 0;

  for (int32_t i = 0; i < actionNum; ++i) {
    STransAction *pAction = taosArrayGet(pArray, i);
    if (pAction->actionType) {
      rawDataLen += (sdbGetRawTotalSize(pAction->pRaw) + sizeof(int32_t));
    } else {
      rawDataLen += (sizeof(STransAction) + pAction->contLen);
    }
    rawDataLen += sizeof(pAction->actionType);
  }

  return rawDataLen;
}

static SSdbRaw *mndTransActionEncode(STrans *pTrans) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t rawDataLen = sizeof(STrans) + TRANS_RESERVE_SIZE;
  rawDataLen += mndTransGetActionsSize(pTrans->redoActions);
  rawDataLen += mndTransGetActionsSize(pTrans->undoActions);
  rawDataLen += mndTransGetActionsSize(pTrans->commitActions);

  SSdbRaw *pRaw = sdbAllocRaw(SDB_TRANS, TRANS_VER_NUMBER, rawDataLen);
  if (pRaw == NULL) {
    mError("trans:%d, failed to alloc raw since %s", pTrans->id, terrstr());
    return NULL;
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pTrans->id, _OVER)
  SDB_SET_INT16(pRaw, dataPos, pTrans->stage, _OVER)
  SDB_SET_INT16(pRaw, dataPos, pTrans->policy, _OVER)
  SDB_SET_INT16(pRaw, dataPos, pTrans->conflict, _OVER)
  SDB_SET_INT16(pRaw, dataPos, pTrans->exec, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pTrans->createdTime, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pTrans->dbname, TSDB_DB_FNAME_LEN, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pTrans->redoActionPos, _OVER)

  int32_t redoActionNum = taosArrayGetSize(pTrans->redoActions);
  int32_t undoActionNum = taosArrayGetSize(pTrans->undoActions);
  int32_t commitActionNum = taosArrayGetSize(pTrans->commitActions);
  SDB_SET_INT32(pRaw, dataPos, redoActionNum, _OVER)
  SDB_SET_INT32(pRaw, dataPos, undoActionNum, _OVER)
  SDB_SET_INT32(pRaw, dataPos, commitActionNum, _OVER)

  for (int32_t i = 0; i < redoActionNum; ++i) {
    STransAction *pAction = taosArrayGet(pTrans->redoActions, i);
    SDB_SET_INT32(pRaw, dataPos, pAction->id, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->errCode, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->acceptableCode, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pAction->actionType, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pAction->stage, _OVER)
    if (pAction->actionType) {
      int32_t len = sdbGetRawTotalSize(pAction->pRaw);
      SDB_SET_INT8(pRaw, dataPos, pAction->rawWritten, _OVER)
      SDB_SET_INT32(pRaw, dataPos, len, _OVER)
      SDB_SET_BINARY(pRaw, dataPos, (void *)pAction->pRaw, len, _OVER)
    } else {
      SDB_SET_BINARY(pRaw, dataPos, (void *)&pAction->epSet, sizeof(SEpSet), _OVER)
      SDB_SET_INT16(pRaw, dataPos, pAction->msgType, _OVER)
      SDB_SET_INT8(pRaw, dataPos, pAction->msgSent, _OVER)
      SDB_SET_INT8(pRaw, dataPos, pAction->msgReceived, _OVER)
      SDB_SET_INT32(pRaw, dataPos, pAction->contLen, _OVER)
      SDB_SET_BINARY(pRaw, dataPos, pAction->pCont, pAction->contLen, _OVER)
    }
  }

  for (int32_t i = 0; i < undoActionNum; ++i) {
    STransAction *pAction = taosArrayGet(pTrans->undoActions, i);
    SDB_SET_INT32(pRaw, dataPos, pAction->id, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->errCode, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->acceptableCode, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pAction->actionType, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pAction->stage, _OVER)
    if (pAction->actionType) {
      int32_t len = sdbGetRawTotalSize(pAction->pRaw);
      SDB_SET_INT8(pRaw, dataPos, pAction->rawWritten, _OVER)
      SDB_SET_INT32(pRaw, dataPos, len, _OVER)
      SDB_SET_BINARY(pRaw, dataPos, (void *)pAction->pRaw, len, _OVER)
    } else {
      SDB_SET_BINARY(pRaw, dataPos, (void *)&pAction->epSet, sizeof(SEpSet), _OVER)
      SDB_SET_INT16(pRaw, dataPos, pAction->msgType, _OVER)
      SDB_SET_INT8(pRaw, dataPos, pAction->msgSent, _OVER)
      SDB_SET_INT8(pRaw, dataPos, pAction->msgReceived, _OVER)
      SDB_SET_INT32(pRaw, dataPos, pAction->contLen, _OVER)
      SDB_SET_BINARY(pRaw, dataPos, pAction->pCont, pAction->contLen, _OVER)
    }
  }

  for (int32_t i = 0; i < commitActionNum; ++i) {
    STransAction *pAction = taosArrayGet(pTrans->commitActions, i);
    SDB_SET_INT32(pRaw, dataPos, pAction->id, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->errCode, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->acceptableCode, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pAction->actionType, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pAction->stage, _OVER)
    if (pAction->actionType) {
      int32_t len = sdbGetRawTotalSize(pAction->pRaw);
      SDB_SET_INT8(pRaw, dataPos, pAction->rawWritten, _OVER)
      SDB_SET_INT32(pRaw, dataPos, len, _OVER)
      SDB_SET_BINARY(pRaw, dataPos, (void *)pAction->pRaw, len, _OVER)
    } else {
      SDB_SET_BINARY(pRaw, dataPos, (void *)&pAction->epSet, sizeof(SEpSet), _OVER)
      SDB_SET_INT16(pRaw, dataPos, pAction->msgType, _OVER)
      SDB_SET_INT8(pRaw, dataPos, pAction->msgSent, _OVER)
      SDB_SET_INT8(pRaw, dataPos, pAction->msgReceived, _OVER)
      SDB_SET_INT32(pRaw, dataPos, pAction->contLen, _OVER)
      SDB_SET_BINARY(pRaw, dataPos, pAction->pCont, pAction->contLen, _OVER)
    }
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
  int32_t      redoActionNum = 0;
  int32_t      undoActionNum = 0;
  int32_t      commitActionNum = 0;
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
  int16_t conflict = 0;
  int16_t exec = 0;
  SDB_GET_INT16(pRaw, dataPos, &stage, _OVER)
  SDB_GET_INT16(pRaw, dataPos, &policy, _OVER)
  SDB_GET_INT16(pRaw, dataPos, &conflict, _OVER)
  SDB_GET_INT16(pRaw, dataPos, &exec, _OVER)
  pTrans->stage = stage;
  pTrans->policy = policy;
  pTrans->conflict = conflict;
  pTrans->exec = exec;
  SDB_GET_INT64(pRaw, dataPos, &pTrans->createdTime, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pTrans->dbname, TSDB_DB_FNAME_LEN, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pTrans->redoActionPos, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &redoActionNum, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &undoActionNum, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &commitActionNum, _OVER)

  pTrans->redoActions = taosArrayInit(redoActionNum, sizeof(STransAction));
  pTrans->undoActions = taosArrayInit(undoActionNum, sizeof(STransAction));
  pTrans->commitActions = taosArrayInit(commitActionNum, sizeof(STransAction));

  if (pTrans->redoActions == NULL) goto _OVER;
  if (pTrans->undoActions == NULL) goto _OVER;
  if (pTrans->commitActions == NULL) goto _OVER;

  for (int32_t i = 0; i < redoActionNum; ++i) {
    SDB_GET_INT32(pRaw, dataPos, &action.id, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.errCode, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.acceptableCode, _OVER)
    SDB_GET_INT8(pRaw, dataPos, &action.actionType, _OVER)
    SDB_GET_INT8(pRaw, dataPos, &action.stage, _OVER)
    if (action.actionType) {
      SDB_GET_INT8(pRaw, dataPos, &action.rawWritten, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &dataLen, _OVER)
      action.pRaw = taosMemoryMalloc(dataLen);
      if (action.pRaw == NULL) goto _OVER;
      mTrace("raw:%p, is created", pData);
      SDB_GET_BINARY(pRaw, dataPos, (void *)action.pRaw, dataLen, _OVER);
      if (taosArrayPush(pTrans->redoActions, &action) == NULL) goto _OVER;
      action.pRaw = NULL;
    } else {
      SDB_GET_BINARY(pRaw, dataPos, (void *)&action.epSet, sizeof(SEpSet), _OVER);
      SDB_GET_INT16(pRaw, dataPos, &action.msgType, _OVER)
      SDB_GET_INT8(pRaw, dataPos, &action.msgSent, _OVER)
      SDB_GET_INT8(pRaw, dataPos, &action.msgReceived, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &action.contLen, _OVER)
      action.pCont = taosMemoryMalloc(action.contLen);
      if (action.pCont == NULL) goto _OVER;
      SDB_GET_BINARY(pRaw, dataPos, action.pCont, action.contLen, _OVER);
      if (taosArrayPush(pTrans->redoActions, &action) == NULL) goto _OVER;
      action.pCont = NULL;
    }
  }

  for (int32_t i = 0; i < undoActionNum; ++i) {
    SDB_GET_INT32(pRaw, dataPos, &action.id, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.errCode, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.acceptableCode, _OVER)
    SDB_GET_INT8(pRaw, dataPos, &action.actionType, _OVER)
    SDB_GET_INT8(pRaw, dataPos, &action.stage, _OVER)
    if (action.actionType) {
      SDB_GET_INT8(pRaw, dataPos, &action.rawWritten, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &dataLen, _OVER)
      action.pRaw = taosMemoryMalloc(dataLen);
      if (action.pRaw == NULL) goto _OVER;
      mTrace("raw:%p, is created", pData);
      SDB_GET_BINARY(pRaw, dataPos, (void *)action.pRaw, dataLen, _OVER);
      if (taosArrayPush(pTrans->undoActions, &action) == NULL) goto _OVER;
      action.pRaw = NULL;
    } else {
      SDB_GET_BINARY(pRaw, dataPos, (void *)&action.epSet, sizeof(SEpSet), _OVER);
      SDB_GET_INT16(pRaw, dataPos, &action.msgType, _OVER)
      SDB_GET_INT8(pRaw, dataPos, &action.msgSent, _OVER)
      SDB_GET_INT8(pRaw, dataPos, &action.msgReceived, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &action.contLen, _OVER)
      action.pCont = taosMemoryMalloc(action.contLen);
      if (action.pCont == NULL) goto _OVER;
      SDB_GET_BINARY(pRaw, dataPos, action.pCont, action.contLen, _OVER);
      if (taosArrayPush(pTrans->undoActions, &action) == NULL) goto _OVER;
      action.pCont = NULL;
    }
  }

  for (int32_t i = 0; i < commitActionNum; ++i) {
    SDB_GET_INT32(pRaw, dataPos, &action.id, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.errCode, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.acceptableCode, _OVER)
    SDB_GET_INT8(pRaw, dataPos, &action.actionType, _OVER)
    SDB_GET_INT8(pRaw, dataPos, &action.stage, _OVER)
    if (action.actionType) {
      SDB_GET_INT8(pRaw, dataPos, &action.rawWritten, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &dataLen, _OVER)
      action.pRaw = taosMemoryMalloc(dataLen);
      if (action.pRaw == NULL) goto _OVER;
      mTrace("raw:%p, is created", pData);
      SDB_GET_BINARY(pRaw, dataPos, (void *)action.pRaw, dataLen, _OVER);
      if (taosArrayPush(pTrans->commitActions, &action) == NULL) goto _OVER;
      action.pRaw = NULL;
    } else {
      SDB_GET_BINARY(pRaw, dataPos, (void *)&action.epSet, sizeof(SEpSet), _OVER);
      SDB_GET_INT16(pRaw, dataPos, &action.msgType, _OVER)
      SDB_GET_INT8(pRaw, dataPos, &action.msgSent, _OVER)
      SDB_GET_INT8(pRaw, dataPos, &action.msgReceived, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &action.contLen, _OVER)
      action.pCont = taosMemoryMalloc(action.contLen);
      if (action.pCont == NULL) goto _OVER;
      SDB_GET_BINARY(pRaw, dataPos, action.pCont, action.contLen, _OVER);
      if (taosArrayPush(pTrans->commitActions, &action) == NULL) goto _OVER;
      action.pCont = NULL;
    }
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
    case TRN_STAGE_REDO_ACTION:
      return "redoAction";
    case TRN_STAGE_ROLLBACK:
      return "rollback";
    case TRN_STAGE_UNDO_ACTION:
      return "undoAction";
    case TRN_STAGE_COMMIT:
      return "commit";
    case TRN_STAGE_COMMIT_ACTION:
      return "commitAction";
    case TRN_STAGE_FINISHED:
      return "finished";
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

static TransCbFp mndTransGetCbFp(ETrnFunc ftype) {
  switch (ftype) {
    case TRANS_START_FUNC_TEST:
      return mndTransTestStartFunc;
    case TRANS_STOP_FUNC_TEST:
      return mndTransTestStopFunc;
    case TRANS_START_FUNC_MQ_REB:
      return mndRebCntInc;
    case TRANS_STOP_FUNC_MQ_REB:
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
  mndTransDropActions(pTrans->redoActions);
  mndTransDropActions(pTrans->undoActions);
  mndTransDropActions(pTrans->commitActions);
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
  mTrace("trans:%d, perform delete action, row:%p stage:%s callfunc:%d", pTrans->id, pTrans, mndTransStr(pTrans->stage),
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

static void mndTransUpdateActions(SArray *pOldArray, SArray *pNewArray) {
  for (int32_t i = 0; i < taosArrayGetSize(pOldArray); ++i) {
    STransAction *pOldAction = taosArrayGet(pOldArray, i);
    STransAction *pNewAction = taosArrayGet(pNewArray, i);
    pOldAction->rawWritten = pNewAction->rawWritten;
    pOldAction->msgSent = pNewAction->msgSent;
    pOldAction->msgReceived = pNewAction->msgReceived;
    pOldAction->errCode = pNewAction->errCode;
  }
}

static int32_t mndTransActionUpdate(SSdb *pSdb, STrans *pOld, STrans *pNew) {
  mTrace("trans:%d, perform update action, old row:%p stage:%s, new row:%p stage:%s", pOld->id, pOld,
         mndTransStr(pOld->stage), pNew, mndTransStr(pNew->stage));
  mndTransUpdateActions(pOld->redoActions, pNew->redoActions);
  mndTransUpdateActions(pOld->undoActions, pNew->undoActions);
  mndTransUpdateActions(pOld->commitActions, pNew->commitActions);
  pOld->stage = pNew->stage;
  pOld->redoActionPos = pNew->redoActionPos;

  if (pOld->stage == TRN_STAGE_COMMIT) {
    pOld->stage = TRN_STAGE_COMMIT_ACTION;
    mTrace("trans:%d, stage from commit to commitAction", pNew->id);
  }

  if (pOld->stage == TRN_STAGE_ROLLBACK) {
    pOld->stage = TRN_STAGE_FINISHED;
    mTrace("trans:%d, stage from rollback to finished", pNew->id);
  }
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

STrans *mndTransCreate(SMnode *pMnode, ETrnPolicy policy, ETrnConflct conflict, const SRpcMsg *pReq) {
  STrans *pTrans = taosMemoryCalloc(1, sizeof(STrans));
  if (pTrans == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to create transaction since %s", terrstr());
    return NULL;
  }

  pTrans->id = sdbGetMaxId(pMnode->pSdb, SDB_TRANS);
  pTrans->stage = TRN_STAGE_PREPARE;
  pTrans->policy = policy;
  pTrans->conflict = conflict;
  pTrans->exec = TRN_EXEC_PRARLLEL;
  pTrans->createdTime = taosGetTimestampMs();
  pTrans->redoActions = taosArrayInit(TRANS_ARRAY_SIZE, sizeof(STransAction));
  pTrans->undoActions = taosArrayInit(TRANS_ARRAY_SIZE, sizeof(STransAction));
  pTrans->commitActions = taosArrayInit(TRANS_ARRAY_SIZE, sizeof(STransAction));

  if (pTrans->redoActions == NULL || pTrans->undoActions == NULL || pTrans->commitActions == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to create transaction since %s", terrstr());
    return NULL;
  }

  if (pReq != NULL) pTrans->rpcInfo = pReq->info;
  mTrace("trans:%d, local object is created, data:%p", pTrans->id, pTrans);
  return pTrans;
}

static void mndTransDropActions(SArray *pArray) {
  int32_t size = taosArrayGetSize(pArray);
  for (int32_t i = 0; i < size; ++i) {
    STransAction *pAction = taosArrayGet(pArray, i);
    if (pAction->actionType) {
      taosMemoryFreeClear(pAction->pRaw);
    } else {
      taosMemoryFreeClear(pAction->pCont);
    }
  }

  taosArrayDestroy(pArray);
}

void mndTransDrop(STrans *pTrans) {
  if (pTrans != NULL) {
    mndTransDropData(pTrans);
    mTrace("trans:%d, local object is freed, data:%p", pTrans->id, pTrans);
    taosMemoryFreeClear(pTrans);
  }
}

static int32_t mndTransAppendAction(SArray *pArray, STransAction *pAction) {
  pAction->id = taosArrayGetSize(pArray);

  void *ptr = taosArrayPush(pArray, pAction);
  if (ptr == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

int32_t mndTransAppendRedolog(STrans *pTrans, SSdbRaw *pRaw) {
  STransAction action = {.stage = TRN_STAGE_REDO_ACTION, .actionType = true, .pRaw = pRaw};
  return mndTransAppendAction(pTrans->redoActions, &action);
}

int32_t mndTransAppendUndolog(STrans *pTrans, SSdbRaw *pRaw) {
  STransAction action = {.stage = TRN_STAGE_UNDO_ACTION, .actionType = true, .pRaw = pRaw};
  return mndTransAppendAction(pTrans->undoActions, &action);
}

int32_t mndTransAppendCommitlog(STrans *pTrans, SSdbRaw *pRaw) {
  STransAction action = {.stage = TRN_STAGE_COMMIT_ACTION, .actionType = true, .pRaw = pRaw};
  return mndTransAppendAction(pTrans->commitActions, &action);
}

int32_t mndTransAppendRedoAction(STrans *pTrans, STransAction *pAction) {
  pAction->stage = TRN_STAGE_REDO_ACTION;
  return mndTransAppendAction(pTrans->redoActions, pAction);
}

int32_t mndTransAppendUndoAction(STrans *pTrans, STransAction *pAction) {
  pAction->stage = TRN_STAGE_UNDO_ACTION;
  return mndTransAppendAction(pTrans->undoActions, pAction);
}

void mndTransSetRpcRsp(STrans *pTrans, void *pCont, int32_t contLen) {
  pTrans->rpcRsp = pCont;
  pTrans->rpcRspLen = contLen;
}

void mndTransSetCb(STrans *pTrans, ETrnFunc startFunc, ETrnFunc stopFunc, void *param, int32_t paramLen) {
  pTrans->startFunc = startFunc;
  pTrans->stopFunc = stopFunc;
  pTrans->param = param;
  pTrans->paramLen = paramLen;
}

void mndTransSetDbInfo(STrans *pTrans, SDbObj *pDb) {
  memcpy(pTrans->dbname, pDb->name, TSDB_DB_FNAME_LEN);
}

void mndTransSetSerial(STrans *pTrans) { pTrans->exec = TRN_EXEC_SERIAL; }

static int32_t mndTransSync(SMnode *pMnode, STrans *pTrans) {
  SSdbRaw *pRaw = mndTransActionEncode(pTrans);
  if (pRaw == NULL) {
    mError("trans:%d, failed to encode while sync trans since %s", pTrans->id, terrstr());
    return -1;
  }
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mDebug("trans:%d, sync to other mnodes", pTrans->id);
  int32_t code = mndSyncPropose(pMnode, pRaw, pTrans->id);
  if (code != 0) {
    mError("trans:%d, failed to sync since %s", pTrans->id, terrstr());
    sdbFreeRaw(pRaw);
    return -1;
  }

  sdbFreeRaw(pRaw);
  mDebug("trans:%d, sync finished", pTrans->id);
  return 0;
}

static bool mndCheckTransConflict(SMnode *pMnode, STrans *pNew) {
  STrans *pTrans = NULL;
  void   *pIter = NULL;
  bool    conflict = false;

  if (pNew->conflict == TRN_CONFLICT_NOTHING) return conflict;

  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_TRANS, pIter, (void **)&pTrans);
    if (pIter == NULL) break;

    if (pNew->conflict == TRN_CONFLICT_GLOBAL) conflict = true;
    if (pNew->conflict == TRN_CONFLICT_DB) {
      if (pTrans->conflict == TRN_CONFLICT_GLOBAL) conflict = true;
      if (pTrans->conflict == TRN_CONFLICT_DB && strcmp(pNew->dbname, pTrans->dbname) == 0) conflict = true;
      if (pTrans->conflict == TRN_CONFLICT_DB_INSIDE && strcmp(pNew->dbname, pTrans->dbname) == 0) conflict = true;
    }
    if (pNew->conflict == TRN_CONFLICT_DB_INSIDE) {
      if (pTrans->conflict == TRN_CONFLICT_GLOBAL) conflict = true;
      if (pTrans->conflict == TRN_CONFLICT_DB && strcmp(pNew->dbname, pTrans->dbname) == 0) conflict = true;
    }
    mError("trans:%d, can't execute since conflict with trans:%d, db:%s", pNew->id, pTrans->id, pTrans->dbname);
    sdbRelease(pMnode->pSdb, pTrans);
  }

  return conflict;
}

int32_t mndTransPrepare(SMnode *pMnode, STrans *pTrans) {
  if (pTrans->conflict == TRN_CONFLICT_DB || pTrans->conflict == TRN_CONFLICT_DB_INSIDE) {
    if (strlen(pTrans->dbname) == 0) {
      terrno = TSDB_CODE_MND_TRANS_CONFLICT;
      mError("trans:%d, failed to prepare conflict db not set", pTrans->id);
      return -1;
    }
  }

  if (mndCheckTransConflict(pMnode, pTrans)) {
    terrno = TSDB_CODE_MND_TRANS_CONFLICT;
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    return -1;
  }

  if (taosArrayGetSize(pTrans->commitActions) <= 0) {
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
    if (pTrans->stage == TRN_STAGE_UNDO_ACTION || pTrans->stage == TRN_STAGE_ROLLBACK) {
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

    mDebug("trans:%d, send rsp, code:0x%x stage:%s app:%p", pTrans->id, code, mndTransStr(pTrans->stage),
           pTrans->rpcInfo.ahandle);
    SRpcMsg rspMsg = {.code = code, .pCont = rpcCont, .contLen = pTrans->rpcRspLen, .info = pTrans->rpcInfo};
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
  }

  mDebug("trans:%d, %s:%d response is received, code:0x%x, accept:0x%x", transId, mndTransStr(pAction->stage), action,
         pRsp->code, pAction->acceptableCode);
  mndTransExecute(pMnode, pTrans);

_OVER:
  mndReleaseTrans(pMnode, pTrans);
}

static void mndTransResetActions(SMnode *pMnode, STrans *pTrans, SArray *pArray) {
  int32_t numOfActions = taosArrayGetSize(pArray);

  for (int32_t action = 0; action < numOfActions; ++action) {
    STransAction *pAction = taosArrayGet(pArray, action);
    if (pAction->msgSent && pAction->msgReceived &&
        (pAction->errCode == 0 || pAction->errCode == pAction->acceptableCode))
      continue;
    if (pAction->rawWritten && (pAction->errCode == 0 || pAction->errCode == pAction->acceptableCode)) continue;

    pAction->rawWritten = 0;
    pAction->msgSent = 0;
    pAction->msgReceived = 0;
    pAction->errCode = 0;
    mDebug("trans:%d, %s:%d execute status is reset", pTrans->id, mndTransStr(pAction->stage), action);
  }
}

static int32_t mndTransWriteSingleLog(SMnode *pMnode, STrans *pTrans, STransAction *pAction) {
  if (pAction->rawWritten) return 0;

  int32_t code = sdbWriteWithoutFree(pMnode->pSdb, pAction->pRaw);
  if (code == 0 || terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    pAction->rawWritten = true;
    pAction->errCode = 0;
    code = 0;
    mDebug("trans:%d, %s:%d write to sdb", pTrans->id, mndTransStr(pAction->stage), pAction->id);
  } else {
    pAction->errCode = (terrno != 0) ? terrno : code;
    mError("trans:%d, %s:%d failed to write sdb since %s", pTrans->id, mndTransStr(pAction->stage), pAction->id,
           terrstr());
  }

  return code;
}

static int32_t mndTransSendSingleMsg(SMnode *pMnode, STrans *pTrans, STransAction *pAction) {
  if (pAction->msgSent) return 0;
  if (!pMnode->deploy && !mndIsMaster(pMnode)) return -1;

  int64_t signature = pTrans->id;
  signature = (signature << 32);
  signature += pAction->id;

  SRpcMsg rpcMsg = {.msgType = pAction->msgType, .contLen = pAction->contLen, .info.ahandle = (void *)signature};
  rpcMsg.pCont = rpcMallocCont(pAction->contLen);
  if (rpcMsg.pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  memcpy(rpcMsg.pCont, pAction->pCont, pAction->contLen);

  int32_t code = tmsgSendReq(&pAction->epSet, &rpcMsg);
  if (code == 0) {
    pAction->msgSent = 1;
    pAction->msgReceived = 0;
    pAction->errCode = 0;
    mDebug("trans:%d, %s:%d is sent to %s:%u", pTrans->id, mndTransStr(pAction->stage), pAction->id,
           pAction->epSet.eps[pAction->epSet.inUse].fqdn, pAction->epSet.eps[pAction->epSet.inUse].port);
  } else {
    pAction->msgSent = 0;
    pAction->msgReceived = 0;
    pAction->errCode = (terrno != 0) ? terrno : code;
    mError("trans:%d, %s:%d not send since %s", pTrans->id, mndTransStr(pAction->stage), pAction->id, terrstr());
  }

  return code;
}

static int32_t mndTransExecSingleAction(SMnode *pMnode, STrans *pTrans, STransAction *pAction) {
  if (pAction->actionType) {
    return mndTransWriteSingleLog(pMnode, pTrans, pAction);
  } else {
    return mndTransSendSingleMsg(pMnode, pTrans, pAction);
  }
}

static int32_t mndTransExecSingleActions(SMnode *pMnode, STrans *pTrans, SArray *pArray) {
  int32_t numOfActions = taosArrayGetSize(pArray);
  int32_t code = 0;

  for (int32_t action = 0; action < numOfActions; ++action) {
    STransAction *pAction = taosArrayGet(pArray, action);
    code = mndTransExecSingleAction(pMnode, pTrans, pAction);
    if (code != 0) break;
  }

  return code;
}

static int32_t mndTransExecuteActions(SMnode *pMnode, STrans *pTrans, SArray *pArray) {
  int32_t numOfActions = taosArrayGetSize(pArray);
  if (numOfActions == 0) return 0;

  if (mndTransExecSingleActions(pMnode, pTrans, pArray) != 0) {
    return -1;
  }

  int32_t       numOfExecuted = 0;
  int32_t       errCode = 0;
  STransAction *pErrAction = NULL;
  for (int32_t action = 0; action < numOfActions; ++action) {
    STransAction *pAction = taosArrayGet(pArray, action);
    if (pAction->msgReceived || pAction->rawWritten) {
      numOfExecuted++;
      if (pAction->errCode != 0 && pAction->errCode != pAction->acceptableCode) {
        errCode = pAction->errCode;
        pErrAction = pAction;
      }
    }
  }

  if (numOfExecuted == numOfActions) {
    if (errCode == 0) {
      pTrans->lastErrorAction = 0;
      pTrans->lastErrorNo = 0;
      pTrans->lastErrorMsgType = 0;
      memset(&pTrans->lastErrorEpset, 0, sizeof(pTrans->lastErrorEpset));
      mDebug("trans:%d, all %d actions execute successfully", pTrans->id, numOfActions);
      return 0;
    } else {
      mError("trans:%d, all %d actions executed, code:0x%x", pTrans->id, numOfActions, errCode & 0XFFFF);
      if (pErrAction != NULL) {
        pTrans->lastErrorMsgType = pErrAction->msgType;
        pTrans->lastErrorAction = pErrAction->id;
        pTrans->lastErrorNo = pErrAction->errCode;
        pTrans->lastErrorEpset = pErrAction->epSet;
      }
      mndTransResetActions(pMnode, pTrans, pArray);
      terrno = errCode;
      return errCode;
    }
  } else {
    mDebug("trans:%d, %d of %d actions executed", pTrans->id, numOfExecuted, numOfActions);
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

static int32_t mndTransExecuteCommitActions(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteActions(pMnode, pTrans, pTrans->commitActions);
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to execute commitActions since %s", terrstr());
  }
  return code;
}

static int32_t mndTransExecuteRedoActionsSerial(SMnode *pMnode, STrans *pTrans) {
  int32_t code = 0;
  int32_t numOfActions = taosArrayGetSize(pTrans->redoActions);
  if (numOfActions == 0) return code;
  if (pTrans->redoActionPos >= numOfActions) return code;

  for (int32_t action = pTrans->redoActionPos; action < numOfActions; ++action) {
    STransAction *pAction = taosArrayGet(pTrans->redoActions, pTrans->redoActionPos);

    code = mndTransExecSingleAction(pMnode, pTrans, pAction);
    if (code == 0) {
      if (pAction->msgSent) {
        if (pAction->msgReceived) {
          if (pAction->errCode != 0 && pAction->errCode != pAction->acceptableCode) {
            code = pAction->errCode;
          }
        } else {
          code = TSDB_CODE_ACTION_IN_PROGRESS;
        }
      }
      if (pAction->rawWritten) {
        if (pAction->errCode != 0 && pAction->errCode != pAction->acceptableCode) {
          code = pAction->errCode;
        }
      }
    }

    if (code == 0) {
      pTrans->lastErrorAction = 0;
      pTrans->lastErrorNo = 0;
      pTrans->lastErrorMsgType = 0;
      memset(&pTrans->lastErrorEpset, 0, sizeof(pTrans->lastErrorEpset));
    } else {
      pTrans->lastErrorMsgType = pAction->msgType;
      pTrans->lastErrorAction = action;
      pTrans->lastErrorNo = pAction->errCode;
      pTrans->lastErrorEpset = pAction->epSet;
    }

    if (code == 0) {
      pTrans->redoActionPos++;
      mDebug("trans:%d, %s:%d is executed and need sync to other mnodes", pTrans->id, mndTransStr(pAction->stage),
             pAction->id);
      code = mndTransSync(pMnode, pTrans);
      if (code != 0) {
        mError("trans:%d, failed to sync redoActionPos since %s", pTrans->id, terrstr());
        break;
      }
    } else if (code == TSDB_CODE_ACTION_IN_PROGRESS) {
      mDebug("trans:%d, %s:%d is in progress and wait it finish", pTrans->id, mndTransStr(pAction->stage), pAction->id);
      break;
    } else {
      mError("trans:%d, %s:%d failed to execute since %s", pTrans->id, mndTransStr(pAction->stage), pAction->id,
             terrstr());
      break;
    }
  }

  return code;
}

static bool mndTransPerformPrepareStage(SMnode *pMnode, STrans *pTrans) {
  bool continueExec = true;
  pTrans->stage = TRN_STAGE_REDO_ACTION;
  mDebug("trans:%d, stage from prepare to redoAction", pTrans->id);
  return continueExec;
}

static bool mndTransPerformRedoActionStage(SMnode *pMnode, STrans *pTrans) {
  bool    continueExec = true;
  int32_t code = 0;

  if (pTrans->exec == TRN_EXEC_SERIAL) {
    code = mndTransExecuteRedoActionsSerial(pMnode, pTrans);
  } else {
    code = mndTransExecuteRedoActions(pMnode, pTrans);
  }

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
    pTrans->stage = TRN_STAGE_COMMIT_ACTION;
    mDebug("trans:%d, stage from commit to commitAction", pTrans->id);
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

static bool mndTransPerformCommitActionStage(SMnode *pMnode, STrans *pTrans) {
  bool    continueExec = true;
  int32_t code = mndTransExecuteCommitActions(pMnode, pTrans);

  if (code == 0) {
    pTrans->code = 0;
    pTrans->stage = TRN_STAGE_FINISHED;
    mDebug("trans:%d, stage from commitAction to finished", pTrans->id);
    continueExec = true;
  } else {
    pTrans->code = terrno;
    pTrans->failedTimes++;
    mError("trans:%d, stage keep on commitAction since %s, failedTimes:%d", pTrans->id, terrstr(), pTrans->failedTimes);
    continueExec = false;
  }

  return continueExec;
}

static bool mndTransPerformUndoActionStage(SMnode *pMnode, STrans *pTrans) {
  bool    continueExec = true;
  int32_t code = mndTransExecuteUndoActions(pMnode, pTrans);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_ROLLBACK;
    mDebug("trans:%d, stage from undoAction to rollback", pTrans->id);
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

  mDebug("trans:%d, execute finished, code:0x%x, failedTimes:%d", pTrans->id, pTrans->code, pTrans->failedTimes);
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
      case TRN_STAGE_REDO_ACTION:
        continueExec = mndTransPerformRedoActionStage(pMnode, pTrans);
        break;
      case TRN_STAGE_COMMIT:
        continueExec = mndTransPerformCommitStage(pMnode, pTrans);
        break;
      case TRN_STAGE_COMMIT_ACTION:
        continueExec = mndTransPerformCommitActionStage(pMnode, pTrans);
        break;
      case TRN_STAGE_UNDO_ACTION:
        continueExec = mndTransPerformUndoActionStage(pMnode, pTrans);
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
      mInfo("trans:%d, %s:%d set processed for kill msg received", pTrans->id, mndTransStr(pAction->stage), i);
      pAction->msgSent = 1;
      pAction->msgReceived = 1;
      pAction->errCode = 0;
    }

    if (pAction->errCode != 0) {
      mInfo("trans:%d, %s:%d set processed for kill msg received, errCode from %s to success", pTrans->id,
            mndTransStr(pAction->stage), i, tstrerror(pAction->errCode));
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

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pTrans->failedTimes, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataAppend(pColInfo, numOfRows, (const char *)&pTrans->lastExecTime, false);

    char lastError[TSDB_TRANS_ERROR_LEN + VARSTR_HEADER_SIZE] = {0};
    char detail[TSDB_TRANS_ERROR_LEN] = {0};
    if (pTrans->lastErrorNo != 0) {
      int32_t len = snprintf(detail, sizeof(detail), "action:%d errno:0x%x(%s) ", pTrans->lastErrorAction,
                             pTrans->lastErrorNo & 0xFFFF, tstrerror(pTrans->lastErrorNo));
      SEpSet  epset = pTrans->lastErrorEpset;
      if (epset.numOfEps > 0) {
        len += snprintf(detail + len, sizeof(detail) - len, "msgType:%s numOfEps:%d inUse:%d ",
                        TMSG_INFO(pTrans->lastErrorMsgType), epset.numOfEps, epset.inUse);
      }
      for (int32_t i = 0; i < pTrans->lastErrorEpset.numOfEps; ++i) {
        len += snprintf(detail + len, sizeof(detail) - len, "ep:%d-%s:%u ", i, epset.eps[i].fqdn, epset.eps[i].port);
      }
    }
    STR_WITH_MAXSIZE_TO_VARSTR(lastError, detail, pShow->pMeta->pSchemas[cols].bytes);
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
