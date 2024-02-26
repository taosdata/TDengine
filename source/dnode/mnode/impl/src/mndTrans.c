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
#include "mndSubscribe.h"
#include "mndDb.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndSync.h"
#include "mndUser.h"

#define TRANS_VER1_NUMBER  1
#define TRANS_VER2_NUMBER  2
#define TRANS_ARRAY_SIZE   8
#define TRANS_RESERVE_SIZE 48

static int32_t mndTransActionInsert(SSdb *pSdb, STrans *pTrans);
static int32_t mndTransActionUpdate(SSdb *pSdb, STrans *OldTrans, STrans *pOld);
static int32_t mndTransDelete(SSdb *pSdb, STrans *pTrans, bool callFunc);

static int32_t mndTransAppendLog(SArray *pArray, SSdbRaw *pRaw);
static int32_t mndTransAppendAction(SArray *pArray, STransAction *pAction);
static void    mndTransDropLogs(SArray *pArray);
static void    mndTransDropActions(SArray *pArray);

static int32_t mndTransExecuteActions(SMnode *pMnode, STrans *pTrans, SArray *pArray, bool topHalf);
static int32_t mndTransExecuteRedoLogs(SMnode *pMnode, STrans *pTrans, bool topHalf);
static int32_t mndTransExecuteUndoLogs(SMnode *pMnode, STrans *pTrans, bool topHalf);
static int32_t mndTransExecuteRedoActions(SMnode *pMnode, STrans *pTrans, bool topHalf);
static int32_t mndTransExecuteUndoActions(SMnode *pMnode, STrans *pTrans, bool topHalf);
static int32_t mndTransExecuteCommitActions(SMnode *pMnode, STrans *pTrans, bool topHalf);
static bool    mndTransPerformRedoLogStage(SMnode *pMnode, STrans *pTrans, bool topHalf);
static bool    mndTransPerformRedoActionStage(SMnode *pMnode, STrans *pTrans, bool topHalf);
static bool    mndTransPerformUndoLogStage(SMnode *pMnode, STrans *pTrans, bool topHalf);
static bool    mndTransPerformUndoActionStage(SMnode *pMnode, STrans *pTrans, bool topHalf);
static bool    mndTransPerformCommitActionStage(SMnode *pMnode, STrans *pTrans, bool topHalf);
static bool    mndTransPerformCommitStage(SMnode *pMnode, STrans *pTrans, bool topHalf);
static bool    mndTransPerformRollbackStage(SMnode *pMnode, STrans *pTrans, bool topHalf);
static bool    mndTransPerformFinishStage(SMnode *pMnode, STrans *pTrans, bool topHalf);

static bool mndCannotExecuteTransAction(SMnode *pMnode, bool topHalf) {
  return (!pMnode->deploy && !mndIsLeader(pMnode)) || !topHalf;
}

static void    mndTransSendRpcRsp(SMnode *pMnode, STrans *pTrans);
static int32_t mndProcessTransTimer(SRpcMsg *pReq);
static int32_t mndProcessTtl(SRpcMsg *pReq);
static int32_t mndProcessKillTransReq(SRpcMsg *pReq);

static int32_t mndRetrieveTrans(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextTrans(SMnode *pMnode, void *pIter);

int32_t mndInitTrans(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_TRANS,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndTransEncode,
      .decodeFp = (SdbDecodeFp)mndTransDecode,
      .insertFp = (SdbInsertFp)mndTransActionInsert,
      .updateFp = (SdbUpdateFp)mndTransActionUpdate,
      .deleteFp = (SdbDeleteFp)mndTransDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_TRANS_TIMER, mndProcessTransTimer);
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
    if (pAction->actionType == TRANS_ACTION_RAW) {
      rawDataLen += (sizeof(STransAction) + sdbGetRawTotalSize(pAction->pRaw));
    } else if (pAction->actionType == TRANS_ACTION_MSG) {
      rawDataLen += (sizeof(STransAction) + pAction->contLen);
    } else {
      // empty
    }
    rawDataLen += sizeof(int8_t);
  }

  return rawDataLen;
}

static int32_t mndTransEncodeAction(SSdbRaw *pRaw, int32_t *offset, SArray *pActions, int32_t actionsNum) {
  int32_t dataPos = *offset;
  int8_t  unused = 0;
  int32_t ret = -1;

  for (int32_t i = 0; i < actionsNum; ++i) {
    STransAction *pAction = taosArrayGet(pActions, i);
    SDB_SET_INT32(pRaw, dataPos, pAction->id, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->errCode, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->acceptableCode, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pAction->retryCode, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pAction->actionType, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pAction->stage, _OVER)
    SDB_SET_INT8(pRaw, dataPos, pAction->reserved, _OVER)
    if (pAction->actionType == TRANS_ACTION_RAW) {
      int32_t len = sdbGetRawTotalSize(pAction->pRaw);
      SDB_SET_INT8(pRaw, dataPos, unused /*pAction->rawWritten*/, _OVER)
      SDB_SET_INT32(pRaw, dataPos, len, _OVER)
      SDB_SET_BINARY(pRaw, dataPos, (void *)pAction->pRaw, len, _OVER)
    } else if (pAction->actionType == TRANS_ACTION_MSG) {
      SDB_SET_BINARY(pRaw, dataPos, (void *)&pAction->epSet, sizeof(SEpSet), _OVER)
      SDB_SET_INT16(pRaw, dataPos, pAction->msgType, _OVER)
      SDB_SET_INT8(pRaw, dataPos, unused /*pAction->msgSent*/, _OVER)
      SDB_SET_INT8(pRaw, dataPos, unused /*pAction->msgReceived*/, _OVER)
      SDB_SET_INT32(pRaw, dataPos, pAction->contLen, _OVER)
      SDB_SET_BINARY(pRaw, dataPos, pAction->pCont, pAction->contLen, _OVER)
    } else {
      // nothing
    }
  }
  ret = 0;

_OVER:
  *offset = dataPos;
  return ret;
}

SSdbRaw *mndTransEncode(STrans *pTrans) {
  terrno = TSDB_CODE_INVALID_MSG;
  int8_t sver = taosArrayGetSize(pTrans->prepareActions) ? TRANS_VER2_NUMBER : TRANS_VER1_NUMBER;

  int32_t rawDataLen = sizeof(STrans) + TRANS_RESERVE_SIZE + pTrans->paramLen;
  rawDataLen += mndTransGetActionsSize(pTrans->prepareActions);
  rawDataLen += mndTransGetActionsSize(pTrans->redoActions);
  rawDataLen += mndTransGetActionsSize(pTrans->undoActions);
  rawDataLen += mndTransGetActionsSize(pTrans->commitActions);

  SSdbRaw *pRaw = sdbAllocRaw(SDB_TRANS, sver, rawDataLen);
  if (pRaw == NULL) {
    mError("trans:%d, failed to alloc raw since %s", pTrans->id, terrstr());
    return NULL;
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pTrans->id, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pTrans->stage, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pTrans->policy, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pTrans->conflict, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pTrans->exec, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pTrans->oper, _OVER)
  SDB_SET_INT8(pRaw, dataPos, 0, _OVER)
  SDB_SET_INT16(pRaw, dataPos, pTrans->originRpcType, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pTrans->createdTime, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pTrans->dbname, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pTrans->stbname, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pTrans->redoActionPos, _OVER)

  int32_t prepareActionNum = taosArrayGetSize(pTrans->prepareActions);
  int32_t redoActionNum = taosArrayGetSize(pTrans->redoActions);
  int32_t undoActionNum = taosArrayGetSize(pTrans->undoActions);
  int32_t commitActionNum = taosArrayGetSize(pTrans->commitActions);

  if (sver > TRANS_VER1_NUMBER) {
    SDB_SET_INT32(pRaw, dataPos, prepareActionNum, _OVER)
  }
  SDB_SET_INT32(pRaw, dataPos, redoActionNum, _OVER)
  SDB_SET_INT32(pRaw, dataPos, undoActionNum, _OVER)
  SDB_SET_INT32(pRaw, dataPos, commitActionNum, _OVER)

  if (mndTransEncodeAction(pRaw, &dataPos, pTrans->prepareActions, prepareActionNum) < 0) goto _OVER;
  if (mndTransEncodeAction(pRaw, &dataPos, pTrans->redoActions, redoActionNum) < 0) goto _OVER;
  if (mndTransEncodeAction(pRaw, &dataPos, pTrans->undoActions, undoActionNum) < 0) goto _OVER;
  if (mndTransEncodeAction(pRaw, &dataPos, pTrans->commitActions, commitActionNum) < 0) goto _OVER;

  SDB_SET_INT32(pRaw, dataPos, pTrans->startFunc, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pTrans->stopFunc, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pTrans->paramLen, _OVER)
  if (pTrans->param != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, pTrans->param, pTrans->paramLen, _OVER)
  }

  SDB_SET_BINARY(pRaw, dataPos, pTrans->opername, TSDB_TRANS_OPER_LEN, _OVER)
  SDB_SET_RESERVE(pRaw, dataPos, TRANS_RESERVE_SIZE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("trans:%d, failed to encode to raw:%p maxlen:%d len:%d since %s", pTrans->id, pRaw, sdbGetRawTotalSize(pRaw),
           dataPos, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("trans:%d, encode to raw:%p, row:%p len:%d", pTrans->id, pRaw, pTrans, dataPos);
  return pRaw;
}

static int32_t mndTransDecodeAction(SSdbRaw *pRaw, int32_t *offset, SArray *pActions, int32_t actionNum) {
  STransAction action = {0};
  int32_t      dataPos = *offset;
  int8_t       unused = 0;
  int8_t       stage = 0;
  int8_t       actionType = 0;
  int32_t      dataLen = 0;
  int32_t      ret = -1;

  for (int32_t i = 0; i < actionNum; ++i) {
    memset(&action, 0, sizeof(action));
    SDB_GET_INT32(pRaw, dataPos, &action.id, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.errCode, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.acceptableCode, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &action.retryCode, _OVER)
    SDB_GET_INT8(pRaw, dataPos, &actionType, _OVER)
    action.actionType = actionType;
    SDB_GET_INT8(pRaw, dataPos, &stage, _OVER)
    action.stage = stage;
    SDB_GET_INT8(pRaw, dataPos, &action.reserved, _OVER)
    if (action.actionType == TRANS_ACTION_RAW) {
      SDB_GET_INT8(pRaw, dataPos, &unused /*&action.rawWritten*/, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &dataLen, _OVER)
      action.pRaw = taosMemoryMalloc(dataLen);
      if (action.pRaw == NULL) goto _OVER;
      mTrace("raw:%p, is created", action.pRaw);
      SDB_GET_BINARY(pRaw, dataPos, (void *)action.pRaw, dataLen, _OVER);
      if (taosArrayPush(pActions, &action) == NULL) goto _OVER;
      action.pRaw = NULL;
    } else if (action.actionType == TRANS_ACTION_MSG) {
      SDB_GET_BINARY(pRaw, dataPos, (void *)&action.epSet, sizeof(SEpSet), _OVER);
      tmsgUpdateDnodeEpSet(&action.epSet);
      SDB_GET_INT16(pRaw, dataPos, &action.msgType, _OVER)
      SDB_GET_INT8(pRaw, dataPos, &unused /*&action.msgSent*/, _OVER)
      SDB_GET_INT8(pRaw, dataPos, &unused /*&action.msgReceived*/, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &action.contLen, _OVER)
      action.pCont = taosMemoryMalloc(action.contLen);
      if (action.pCont == NULL) goto _OVER;
      SDB_GET_BINARY(pRaw, dataPos, action.pCont, action.contLen, _OVER);
      if (taosArrayPush(pActions, &action) == NULL) goto _OVER;
      action.pCont = NULL;
    } else {
      if (taosArrayPush(pActions, &action) == NULL) goto _OVER;
    }
  }
  ret = 0;

_OVER:
  *offset = dataPos;
  taosMemoryFreeClear(action.pCont);
  return ret;
}

SSdbRow *mndTransDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_INVALID_MSG;

  SSdbRow *pRow = NULL;
  STrans  *pTrans = NULL;
  char    *pData = NULL;
  int32_t  dataLen = 0;
  int8_t   sver = 0;
  int32_t  prepareActionNum = 0;
  int32_t  redoActionNum = 0;
  int32_t  undoActionNum = 0;
  int32_t  commitActionNum = 0;
  int32_t  dataPos = 0;

  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != TRANS_VER1_NUMBER && sver != TRANS_VER2_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(STrans));
  if (pRow == NULL) goto _OVER;

  pTrans = sdbGetRowObj(pRow);
  if (pTrans == NULL) goto _OVER;

  SDB_GET_INT32(pRaw, dataPos, &pTrans->id, _OVER)

  int8_t stage = 0;
  int8_t policy = 0;
  int8_t conflict = 0;
  int8_t exec = 0;
  int8_t oper = 0;
  int8_t reserved = 0;
  int8_t actionType = 0;
  SDB_GET_INT8(pRaw, dataPos, &stage, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &policy, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &conflict, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &exec, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &oper, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &reserved, _OVER)
  pTrans->stage = stage;
  pTrans->policy = policy;
  pTrans->conflict = conflict;
  pTrans->exec = exec;
  pTrans->oper = oper;
  SDB_GET_INT16(pRaw, dataPos, &pTrans->originRpcType, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pTrans->createdTime, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pTrans->dbname, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pTrans->stbname, TSDB_TABLE_FNAME_LEN, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pTrans->redoActionPos, _OVER)

  if (sver > TRANS_VER1_NUMBER) {
    SDB_GET_INT32(pRaw, dataPos, &prepareActionNum, _OVER)
  }
  SDB_GET_INT32(pRaw, dataPos, &redoActionNum, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &undoActionNum, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &commitActionNum, _OVER)

  pTrans->prepareActions = taosArrayInit(prepareActionNum, sizeof(STransAction));
  pTrans->redoActions = taosArrayInit(redoActionNum, sizeof(STransAction));
  pTrans->undoActions = taosArrayInit(undoActionNum, sizeof(STransAction));
  pTrans->commitActions = taosArrayInit(commitActionNum, sizeof(STransAction));

  if (pTrans->prepareActions == NULL) goto _OVER;
  if (pTrans->redoActions == NULL) goto _OVER;
  if (pTrans->undoActions == NULL) goto _OVER;
  if (pTrans->commitActions == NULL) goto _OVER;

  if (mndTransDecodeAction(pRaw, &dataPos, pTrans->prepareActions, prepareActionNum) < 0) goto _OVER;
  if (mndTransDecodeAction(pRaw, &dataPos, pTrans->redoActions, redoActionNum) < 0) goto _OVER;
  if (mndTransDecodeAction(pRaw, &dataPos, pTrans->undoActions, undoActionNum) < 0) goto _OVER;
  if (mndTransDecodeAction(pRaw, &dataPos, pTrans->commitActions, commitActionNum) < 0) goto _OVER;

  SDB_GET_INT32(pRaw, dataPos, &pTrans->startFunc, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pTrans->stopFunc, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pTrans->paramLen, _OVER)
  if (pTrans->paramLen != 0) {
    pTrans->param = taosMemoryMalloc(pTrans->paramLen);
    SDB_GET_BINARY(pRaw, dataPos, pTrans->param, pTrans->paramLen, _OVER);
  }

  SDB_GET_BINARY(pRaw, dataPos, pTrans->opername, TSDB_TRANS_OPER_LEN, _OVER);
  SDB_GET_RESERVE(pRaw, dataPos, TRANS_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0 && pTrans != NULL) {
    mError("trans:%d, failed to parse from raw:%p since %s", pTrans->id, pRaw, terrstr());
    mndTransDropData(pTrans);
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  if (pTrans != NULL) {
    mTrace("trans:%d, decode from raw:%p, row:%p", pTrans->id, pRaw, pTrans);
  }
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
    case TRN_STAGE_FINISH:
      return "finished";
    case TRN_STAGE_PRE_FINISH:
      return "pre-finish";
    default:
      return "invalid";
  }
}

static void mndSetTransLastAction(STrans *pTrans, STransAction *pAction) {
  if (pAction != NULL) {
    pTrans->lastAction = pAction->id;
    pTrans->lastMsgType = pAction->msgType;
    pTrans->lastEpset = pAction->epSet;
    pTrans->lastErrorNo = pAction->errCode;
  } else {
    pTrans->lastAction = 0;
    pTrans->lastMsgType = 0;
    memset(&pTrans->lastEpset, 0, sizeof(pTrans->lastEpset));
    pTrans->lastErrorNo = 0;
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
  mInfo("trans:%d, perform insert action, row:%p stage:%s, callfunc:1, startFunc:%d", pTrans->id, pTrans,
        mndTransStr(pTrans->stage), pTrans->startFunc);

  taosThreadMutexInit(&pTrans->mutex, NULL);

  if (pTrans->startFunc > 0) {
    TransCbFp fp = mndTransGetCbFp(pTrans->startFunc);
    if (fp) {
      (*fp)(pSdb->pMnode, pTrans->param, pTrans->paramLen);
    }
    // pTrans->startFunc = 0;
  }

  return 0;
}

void mndTransDropData(STrans *pTrans) {
  if (pTrans->prepareActions != NULL) {
    mndTransDropActions(pTrans->prepareActions);
    pTrans->prepareActions = NULL;
  }
  if (pTrans->redoActions != NULL) {
    mndTransDropActions(pTrans->redoActions);
    pTrans->redoActions = NULL;
  }
  if (pTrans->undoActions != NULL) {
    mndTransDropActions(pTrans->undoActions);
    pTrans->undoActions = NULL;
  }
  if (pTrans->commitActions != NULL) {
    mndTransDropActions(pTrans->commitActions);
    pTrans->commitActions = NULL;
  }
  if (pTrans->pRpcArray != NULL) {
    taosArrayDestroy(pTrans->pRpcArray);
    pTrans->pRpcArray = NULL;
  }
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
  (void)taosThreadMutexDestroy(&pTrans->mutex);
}

static int32_t mndTransDelete(SSdb *pSdb, STrans *pTrans, bool callFunc) {
  mInfo("trans:%d, perform delete action, row:%p stage:%s callfunc:%d, stopFunc:%d", pTrans->id, pTrans,
        mndTransStr(pTrans->stage), callFunc, pTrans->stopFunc);

  if (pTrans->stopFunc > 0 && callFunc) {
    TransCbFp fp = mndTransGetCbFp(pTrans->stopFunc);
    if (fp) {
      (*fp)(pSdb->pMnode, pTrans->param, pTrans->paramLen);
    }
    // pTrans->stopFunc = 0;
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
  mInfo("trans:%d, perform update action, old row:%p stage:%s create:%" PRId64 ", new row:%p stage:%s create:%" PRId64,
        pOld->id, pOld, mndTransStr(pOld->stage), pOld->createdTime, pNew, mndTransStr(pNew->stage), pNew->createdTime);

  if (pOld->createdTime != pNew->createdTime) {
    mError("trans:%d, failed to perform update action since createTime not match, old row:%p stage:%s create:%" PRId64
           ", new row:%p stage:%s create:%" PRId64,
           pOld->id, pOld, mndTransStr(pOld->stage), pOld->createdTime, pNew, mndTransStr(pNew->stage),
           pNew->createdTime);
    // only occured while sync timeout
    terrno = TSDB_CODE_MND_TRANS_SYNC_TIMEOUT;
    return -1;
  }

  mndTransUpdateActions(pOld->prepareActions, pNew->prepareActions);
  mndTransUpdateActions(pOld->redoActions, pNew->redoActions);
  mndTransUpdateActions(pOld->undoActions, pNew->undoActions);
  mndTransUpdateActions(pOld->commitActions, pNew->commitActions);
  pOld->stage = pNew->stage;
  pOld->redoActionPos = pNew->redoActionPos;

  if (pOld->stage == TRN_STAGE_COMMIT) {
    pOld->stage = TRN_STAGE_COMMIT_ACTION;
    mTrace("trans:%d, stage from commit to commitAction since perform update action", pNew->id);
  }

  if (pOld->stage == TRN_STAGE_ROLLBACK) {
    pOld->stage = TRN_STAGE_UNDO_ACTION;
    mTrace("trans:%d, stage from rollback to undoAction since perform update action", pNew->id);
  }

  if (pOld->stage == TRN_STAGE_PRE_FINISH) {
    pOld->stage = TRN_STAGE_FINISH;
    mTrace("trans:%d, stage from pre-finish to finished since perform update action", pNew->id);
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

STrans *mndTransCreate(SMnode *pMnode, ETrnPolicy policy, ETrnConflct conflict, const SRpcMsg *pReq,
                       const char *opername) {
  STrans *pTrans = taosMemoryCalloc(1, sizeof(STrans));
  if (pTrans == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to create transaction since %s", terrstr());
    return NULL;
  }

  if (opername != NULL) {
    tstrncpy(pTrans->opername, opername, TSDB_TRANS_OPER_LEN);
  }

  pTrans->id = sdbGetMaxId(pMnode->pSdb, SDB_TRANS);
  pTrans->stage = TRN_STAGE_PREPARE;
  pTrans->policy = policy;
  pTrans->conflict = conflict;
  pTrans->exec = TRN_EXEC_PARALLEL;
  pTrans->createdTime = taosGetTimestampMs();
  pTrans->prepareActions = taosArrayInit(TRANS_ARRAY_SIZE, sizeof(STransAction));
  pTrans->redoActions = taosArrayInit(TRANS_ARRAY_SIZE, sizeof(STransAction));
  pTrans->undoActions = taosArrayInit(TRANS_ARRAY_SIZE, sizeof(STransAction));
  pTrans->commitActions = taosArrayInit(TRANS_ARRAY_SIZE, sizeof(STransAction));
  pTrans->pRpcArray = taosArrayInit(1, sizeof(SRpcHandleInfo));
  pTrans->mTraceId = pReq ? TRACE_GET_ROOTID(&pReq->info.traceId) : tGenIdPI64();
  taosInitRWLatch(&pTrans->lockRpcArray);
  taosThreadMutexInit(&pTrans->mutex, NULL);

  if (pTrans->redoActions == NULL || pTrans->undoActions == NULL || pTrans->commitActions == NULL ||
      pTrans->pRpcArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to create transaction since %s", terrstr());
    mndTransDrop(pTrans);
    return NULL;
  }

  if (pReq != NULL) {
    taosArrayPush(pTrans->pRpcArray, &pReq->info);
    pTrans->originRpcType = pReq->msgType;
  }

  mInfo("trans:%d, create transaction:%s, origin:%s", pTrans->id, pTrans->opername, opername);

  mTrace("trans:%d, local object is created, data:%p", pTrans->id, pTrans);
  return pTrans;
}

static void mndTransDropActions(SArray *pArray) {
  int32_t size = taosArrayGetSize(pArray);
  for (int32_t i = 0; i < size; ++i) {
    STransAction *pAction = taosArrayGet(pArray, i);
    if (pAction->actionType == TRANS_ACTION_RAW) {
      taosMemoryFreeClear(pAction->pRaw);
    } else if (pAction->actionType == TRANS_ACTION_MSG) {
      taosMemoryFreeClear(pAction->pCont);
    } else {
      // nothing
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
  STransAction action = {
      .stage = TRN_STAGE_REDO_ACTION, .actionType = TRANS_ACTION_RAW, .pRaw = pRaw, .mTraceId = pTrans->mTraceId};
  return mndTransAppendAction(pTrans->redoActions, &action);
}

int32_t mndTransAppendNullLog(STrans *pTrans) {
  STransAction action = {.stage = TRN_STAGE_REDO_ACTION, .actionType = TRANS_ACTION_NULL};
  return mndTransAppendAction(pTrans->redoActions, &action);
}

int32_t mndTransAppendUndolog(STrans *pTrans, SSdbRaw *pRaw) {
  STransAction action = {.stage = TRN_STAGE_UNDO_ACTION, .actionType = TRANS_ACTION_RAW, .pRaw = pRaw};
  return mndTransAppendAction(pTrans->undoActions, &action);
}

int32_t mndTransAppendCommitlog(STrans *pTrans, SSdbRaw *pRaw) {
  STransAction action = {.stage = TRN_STAGE_COMMIT_ACTION, .actionType = TRANS_ACTION_RAW, .pRaw = pRaw};
  return mndTransAppendAction(pTrans->commitActions, &action);
}

int32_t mndTransAppendPrepareLog(STrans *pTrans, SSdbRaw *pRaw) {
  STransAction action = {
      .pRaw = pRaw, .stage = TRN_STAGE_PREPARE, .actionType = TRANS_ACTION_RAW, .mTraceId = pTrans->mTraceId};
  return mndTransAppendAction(pTrans->prepareActions, &action);
}

int32_t mndTransAppendRedoAction(STrans *pTrans, STransAction *pAction) {
  pAction->stage = TRN_STAGE_REDO_ACTION;
  pAction->actionType = TRANS_ACTION_MSG;
  pAction->mTraceId = pTrans->mTraceId;
  return mndTransAppendAction(pTrans->redoActions, pAction);
}

int32_t mndTransAppendUndoAction(STrans *pTrans, STransAction *pAction) {
  pAction->stage = TRN_STAGE_UNDO_ACTION;
  pAction->actionType = TRANS_ACTION_MSG;
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

int32_t mndSetRpcInfoForDbTrans(SMnode *pMnode, SRpcMsg *pMsg, EOperType oper, const char *dbname) {
  STrans *pTrans = NULL;
  void   *pIter = NULL;
  int32_t code = -1;

  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_TRANS, pIter, (void **)&pTrans);
    if (pIter == NULL) break;

    if (pTrans->oper == oper) {
      if (strcasecmp(dbname, pTrans->dbname) == 0) {
        mInfo("trans:%d, db:%s oper:%d matched with input", pTrans->id, dbname, oper);
        taosWLockLatch(&pTrans->lockRpcArray);
        if (pTrans->pRpcArray == NULL) {
          pTrans->pRpcArray = taosArrayInit(4, sizeof(SRpcHandleInfo));
        }
        if (pTrans->pRpcArray != NULL && taosArrayPush(pTrans->pRpcArray, &pMsg->info) != NULL) {
          code = 0;
        }
        taosWUnLockLatch(&pTrans->lockRpcArray);

        sdbRelease(pMnode->pSdb, pTrans);
        break;
      }
    }

    sdbRelease(pMnode->pSdb, pTrans);
  }
  return code;
}

void mndTransSetDbName(STrans *pTrans, const char *dbname, const char *stbname) {
  if (dbname != NULL) {
    tstrncpy(pTrans->dbname, dbname, TSDB_DB_FNAME_LEN);
  }
  if (stbname != NULL) {
    tstrncpy(pTrans->stbname, stbname, TSDB_TABLE_FNAME_LEN);
  }
}

void mndTransSetSerial(STrans *pTrans) { pTrans->exec = TRN_EXEC_SERIAL; }

void mndTransSetParallel(STrans *pTrans) { pTrans->exec = TRN_EXEC_PARALLEL; }

void mndTransSetOper(STrans *pTrans, EOperType oper) { pTrans->oper = oper; }

static int32_t mndTransSync(SMnode *pMnode, STrans *pTrans) {
  SSdbRaw *pRaw = mndTransEncode(pTrans);
  if (pRaw == NULL) {
    mError("trans:%d, failed to encode while sync trans since %s", pTrans->id, terrstr());
    return -1;
  }
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mInfo("trans:%d, sync to other mnodes, stage:%s createTime:%" PRId64, pTrans->id, mndTransStr(pTrans->stage),
        pTrans->createdTime);
  int32_t code = mndSyncPropose(pMnode, pRaw, pTrans->id);
  if (code != 0) {
    mError("trans:%d, failed to sync, errno:%s code:0x%x createTime:%" PRId64 " saved trans:%d", pTrans->id, terrstr(),
           code, pTrans->createdTime, pMnode->syncMgmt.transId);
    sdbFreeRaw(pRaw);
    return -1;
  }

  sdbFreeRaw(pRaw);
  mInfo("trans:%d, sync finished, createTime:%" PRId64, pTrans->id, pTrans->createdTime);
  return 0;
}

static bool mndCheckDbConflict(const char *conflict, STrans *pTrans) {
  if (conflict[0] == 0) return false;
  if (strcasecmp(conflict, pTrans->dbname) == 0 || strcasecmp(conflict, pTrans->stbname) == 0) return true;
  return false;
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
      if (pTrans->conflict == TRN_CONFLICT_DB || pTrans->conflict == TRN_CONFLICT_DB_INSIDE) {
        if (mndCheckDbConflict(pNew->dbname, pTrans)) conflict = true;
        if (mndCheckDbConflict(pNew->stbname, pTrans)) conflict = true;
      }
    }
    if (pNew->conflict == TRN_CONFLICT_DB_INSIDE) {
      if (pTrans->conflict == TRN_CONFLICT_GLOBAL) conflict = true;
      if (pTrans->conflict == TRN_CONFLICT_DB) {
        if (mndCheckDbConflict(pNew->dbname, pTrans)) conflict = true;
        if (mndCheckDbConflict(pNew->stbname, pTrans)) conflict = true;
      }
      if (pTrans->conflict == TRN_CONFLICT_DB_INSIDE) {
        if (mndCheckDbConflict(pNew->stbname, pTrans)) conflict = true;  // for stb
      }
    }

    if (pNew->conflict == TRN_CONFLICT_TOPIC) {
      if (pTrans->conflict == TRN_CONFLICT_GLOBAL) conflict = true;
      if (pTrans->conflict == TRN_CONFLICT_TOPIC || pTrans->conflict == TRN_CONFLICT_TOPIC_INSIDE) {
        if (strcasecmp(pNew->dbname, pTrans->dbname) == 0 ) conflict = true;
      }
    }
    if (pNew->conflict == TRN_CONFLICT_TOPIC_INSIDE) {
      if (pTrans->conflict == TRN_CONFLICT_GLOBAL) conflict = true;
      if (pTrans->conflict == TRN_CONFLICT_TOPIC ) {
        if (strcasecmp(pNew->dbname, pTrans->dbname) == 0 ) conflict = true;
      }
      if (pTrans->conflict == TRN_CONFLICT_TOPIC_INSIDE) {
        if (strcasecmp(pNew->dbname, pTrans->dbname) == 0 && strcasecmp(pNew->stbname, pTrans->stbname) == 0) conflict = true;
      }
    }

    if (conflict) {
      mError("trans:%d, db:%s stb:%s type:%d, can't execute since conflict with trans:%d db:%s stb:%s type:%d",
             pNew->id, pNew->dbname, pNew->stbname, pNew->conflict, pTrans->id, pTrans->dbname, pTrans->stbname,
             pTrans->conflict);
    } else {
      mInfo("trans:%d, db:%s stb:%s type:%d, not conflict with trans:%d db:%s stb:%s type:%d", pNew->id, pNew->dbname,
            pNew->stbname, pNew->conflict, pTrans->id, pTrans->dbname, pTrans->stbname, pTrans->conflict);
    }
    sdbRelease(pMnode->pSdb, pTrans);
  }

  return conflict;
}

int32_t mndTransCheckConflict(SMnode *pMnode, STrans *pTrans) {
  if (pTrans->conflict == TRN_CONFLICT_DB || pTrans->conflict == TRN_CONFLICT_DB_INSIDE) {
    if (strlen(pTrans->dbname) == 0 && strlen(pTrans->stbname) == 0) {
      terrno = TSDB_CODE_MND_TRANS_CONFLICT;
      mError("trans:%d, failed to prepare conflict db not set", pTrans->id);
      return -1;
    }
  }

  if (mndCheckTransConflict(pMnode, pTrans)) {
    terrno = TSDB_CODE_MND_TRANS_CONFLICT;
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    return terrno;
  }

  return 0;
}

int32_t mndTransPrepare(SMnode *pMnode, STrans *pTrans) {
  if(pTrans == NULL) return -1;

  if (mndTransCheckConflict(pMnode, pTrans) != 0) {
    return -1;
  }

  if (taosArrayGetSize(pTrans->commitActions) <= 0) {
    terrno = TSDB_CODE_MND_TRANS_CLOG_IS_NULL;
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    return -1;
  }

  mInfo("trans:%d, prepare transaction", pTrans->id);
  if (mndTransSync(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    return -1;
  }
  mInfo("trans:%d, prepare finished", pTrans->id);

  STrans *pNew = mndAcquireTrans(pMnode, pTrans->id);
  if (pNew == NULL) {
    mError("trans:%d, failed to read from sdb since %s", pTrans->id, terrstr());
    return -1;
  }

  pNew->pRpcArray = pTrans->pRpcArray;
  pNew->rpcRsp = pTrans->rpcRsp;
  pNew->rpcRspLen = pTrans->rpcRspLen;
  pNew->mTraceId = pTrans->mTraceId;
  pTrans->pRpcArray = NULL;
  pTrans->rpcRsp = NULL;
  pTrans->rpcRspLen = 0;

  mndTransExecute(pMnode, pNew);
  mndReleaseTrans(pMnode, pNew);
  return 0;
}

static int32_t mndTransCommit(SMnode *pMnode, STrans *pTrans) {
  mInfo("trans:%d, commit transaction", pTrans->id);
  if (mndTransSync(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to commit since %s", pTrans->id, terrstr());
    return -1;
  }
  mInfo("trans:%d, commit finished", pTrans->id);
  return 0;
}

static int32_t mndTransRollback(SMnode *pMnode, STrans *pTrans) {
  mInfo("trans:%d, rollback transaction", pTrans->id);
  if (mndTransSync(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to rollback since %s", pTrans->id, terrstr());
    return -1;
  }
  mInfo("trans:%d, rollback finished", pTrans->id);
  return 0;
}

static int32_t mndTransPreFinish(SMnode *pMnode, STrans *pTrans) {
  mInfo("trans:%d, pre-finish transaction", pTrans->id);
  if (mndTransSync(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to pre-finish since %s", pTrans->id, terrstr());
    return -1;
  }
  mInfo("trans:%d, pre-finish finished", pTrans->id);
  return 0;
}

static void mndTransSendRpcRsp(SMnode *pMnode, STrans *pTrans) {
  bool    sendRsp = false;
  int32_t code = pTrans->code;

  if (pTrans->stage == TRN_STAGE_FINISH) {
    sendRsp = true;
  }

  if (pTrans->policy == TRN_POLICY_ROLLBACK) {
    if (pTrans->stage == TRN_STAGE_UNDO_ACTION || pTrans->stage == TRN_STAGE_ROLLBACK) {
      if (code == 0) code = TSDB_CODE_MND_TRANS_UNKNOW_ERROR;
      sendRsp = true;
    }
  } else {
    if (pTrans->stage == TRN_STAGE_REDO_ACTION) {
      if (code == TSDB_CODE_SYN_NOT_LEADER || code == TSDB_CODE_SYN_RESTORING || code == TSDB_CODE_APP_IS_STARTING ||
          code == TSDB_CODE_SYN_PROPOSE_NOT_READY) {
        if (pTrans->failedTimes > 60) sendRsp = true;
      } else {
        if (pTrans->failedTimes > 6) sendRsp = true;
      }
      if (code == 0) code = TSDB_CODE_MND_TRANS_UNKNOW_ERROR;
    }
  }

  if (!sendRsp) {
    return;
  } else {
    mInfo("trans:%d, send rsp, stage:%s failedTimes:%d code:0x%x", pTrans->id, mndTransStr(pTrans->stage),
          pTrans->failedTimes, code);
  }

  taosWLockLatch(&pTrans->lockRpcArray);
  int32_t size = taosArrayGetSize(pTrans->pRpcArray);
  if (size <= 0) {
    taosWUnLockLatch(&pTrans->lockRpcArray);
    return;
  }

  for (int32_t i = 0; i < size; ++i) {
    SRpcHandleInfo *pInfo = taosArrayGet(pTrans->pRpcArray, i);
    if (pInfo->handle != NULL) {
      if (code == TSDB_CODE_RPC_NETWORK_UNAVAIL || code == TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED) {
        code = TSDB_CODE_MND_TRANS_NETWORK_UNAVAILL;
      }
      if (code == TSDB_CODE_SYN_TIMEOUT) {
        code = TSDB_CODE_MND_TRANS_SYNC_TIMEOUT;
      }

      if (i != 0 && code == 0) {
        code = TSDB_CODE_MNODE_NOT_FOUND;
      }
      mInfo("trans:%d, client:%d send rsp, code:0x%x stage:%s app:%p", pTrans->id, i, code, mndTransStr(pTrans->stage),
            pInfo->ahandle);

      SRpcMsg rspMsg = {.code = code, .info = *pInfo};

      if (pTrans->originRpcType == TDMT_MND_CREATE_DB) {
        mInfo("trans:%d, origin msgtype:%s", pTrans->id, TMSG_INFO(pTrans->originRpcType));
        SDbObj *pDb = mndAcquireDb(pMnode, pTrans->dbname);
        if (pDb != NULL) {
          for (int32_t j = 0; j < 12; j++) {
            bool ready = mndIsDbReady(pMnode, pDb);
            if (!ready) {
              mInfo("trans:%d, db:%s not ready yet, wait %d times", pTrans->id, pTrans->dbname, j);
              taosMsleep(1000);
            } else {
              break;
            }
          }
        }
        mndReleaseDb(pMnode, pDb);
      } else if (pTrans->originRpcType == TDMT_MND_CREATE_STB) {
        void   *pCont = NULL;
        int32_t contLen = 0;
        if (0 == mndBuildSMCreateStbRsp(pMnode, pTrans->dbname, pTrans->stbname, &pCont, &contLen) != 0) {
          mndTransSetRpcRsp(pTrans, pCont, contLen);
        }
      } else if (pTrans->originRpcType == TDMT_MND_CREATE_INDEX) {
        void   *pCont = NULL;
        int32_t contLen = 0;
        if (0 == mndBuildSMCreateStbRsp(pMnode, pTrans->dbname, pTrans->stbname, &pCont, &contLen) != 0) {
          mndTransSetRpcRsp(pTrans, pCont, contLen);
        }
      }

      if (pTrans->rpcRspLen != 0) {
        void *rpcCont = rpcMallocCont(pTrans->rpcRspLen);
        if (rpcCont != NULL) {
          memcpy(rpcCont, pTrans->rpcRsp, pTrans->rpcRspLen);
          rspMsg.pCont = rpcCont;
          rspMsg.contLen = pTrans->rpcRspLen;
        }
      }

      tmsgSendRsp(&rspMsg);
    }
  }
  taosArrayClear(pTrans->pRpcArray);
  taosWUnLockLatch(&pTrans->lockRpcArray);
}

int32_t mndTransProcessRsp(SRpcMsg *pRsp) {
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
    pTrans->lastErrorNo = pRsp->code;

    mInfo("trans:%d, %s:%d response is received, code:0x%x, accept:0x%x retry:0x%x", transId,
          mndTransStr(pAction->stage), action, pRsp->code, pAction->acceptableCode, pAction->retryCode);
  } else {
    mInfo("trans:%d, invalid action, index:%d, code:0x%x", transId, action, pRsp->code);
  }

  mndTransExecute(pMnode, pTrans);

_OVER:
  mndReleaseTrans(pMnode, pTrans);
  return 0;
}

static void mndTransResetAction(SMnode *pMnode, STrans *pTrans, STransAction *pAction) {
  pAction->rawWritten = 0;
  pAction->msgSent = 0;
  pAction->msgReceived = 0;
  if (pAction->errCode == TSDB_CODE_SYN_NEW_CONFIG_ERROR || pAction->errCode == TSDB_CODE_SYN_INTERNAL_ERROR ||
      pAction->errCode == TSDB_CODE_SYN_NOT_LEADER) {
    pAction->epSet.inUse = (pAction->epSet.inUse + 1) % pAction->epSet.numOfEps;
    mInfo("trans:%d, %s:%d execute status is reset and set epset inuse:%d", pTrans->id, mndTransStr(pAction->stage),
          pAction->id, pAction->epSet.inUse);
  } else {
    mInfo("trans:%d, %s:%d execute status is reset", pTrans->id, mndTransStr(pAction->stage), pAction->id);
  }
  pAction->errCode = 0;
}

static void mndTransResetActions(SMnode *pMnode, STrans *pTrans, SArray *pArray) {
  int32_t numOfActions = taosArrayGetSize(pArray);

  for (int32_t action = 0; action < numOfActions; ++action) {
    STransAction *pAction = taosArrayGet(pArray, action);
    if (pAction->msgSent && pAction->msgReceived &&
        (pAction->errCode == 0 || pAction->errCode == pAction->acceptableCode))
      continue;
    if (pAction->rawWritten && (pAction->errCode == 0 || pAction->errCode == pAction->acceptableCode)) continue;

    mndTransResetAction(pMnode, pTrans, pAction);
  }
}

static int32_t mndTransWriteSingleLog(SMnode *pMnode, STrans *pTrans, STransAction *pAction, bool topHalf) {
  if (pAction->rawWritten) return 0;
  if (topHalf) {
    terrno = TSDB_CODE_MND_TRANS_CTX_SWITCH;
    return TSDB_CODE_MND_TRANS_CTX_SWITCH;
  }

  int32_t code = sdbWriteWithoutFree(pMnode->pSdb, pAction->pRaw);
  if (code == 0 || terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    pAction->rawWritten = true;
    pAction->errCode = 0;
    code = 0;
    mInfo("trans:%d, %s:%d write to sdb, type:%s status:%s", pTrans->id, mndTransStr(pAction->stage), pAction->id,
          sdbTableName(pAction->pRaw->type), sdbStatusName(pAction->pRaw->status));

    mndSetTransLastAction(pTrans, pAction);
  } else {
    pAction->errCode = (terrno != 0) ? terrno : code;
    mError("trans:%d, %s:%d failed to write sdb since %s, type:%s status:%s", pTrans->id, mndTransStr(pAction->stage),
           pAction->id, terrstr(), sdbTableName(pAction->pRaw->type), sdbStatusName(pAction->pRaw->status));
    mndSetTransLastAction(pTrans, pAction);
  }

  return code;
}

static int32_t mndTransSendSingleMsg(SMnode *pMnode, STrans *pTrans, STransAction *pAction, bool topHalf) {
  if (pAction->msgSent) return 0;
  if (mndCannotExecuteTransAction(pMnode, topHalf)) {
    terrno = TSDB_CODE_MND_TRANS_CTX_SWITCH;
    return TSDB_CODE_MND_TRANS_CTX_SWITCH;
  }

  int64_t signature = pTrans->id;
  signature = (signature << 32);
  signature += pAction->id;

  SRpcMsg rpcMsg = {.msgType = pAction->msgType, .contLen = pAction->contLen, .info.ahandle = (void *)signature};
  rpcMsg.pCont = rpcMallocCont(pAction->contLen);
  if (rpcMsg.pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  rpcMsg.info.traceId.rootId = pTrans->mTraceId;

  memcpy(rpcMsg.pCont, pAction->pCont, pAction->contLen);

  char    detail[1024] = {0};
  int32_t len = snprintf(detail, sizeof(detail), "msgType:%s numOfEps:%d inUse:%d", TMSG_INFO(pAction->msgType),
                         pAction->epSet.numOfEps, pAction->epSet.inUse);
  for (int32_t i = 0; i < pAction->epSet.numOfEps; ++i) {
    len += snprintf(detail + len, sizeof(detail) - len, " ep:%d-%s:%u", i, pAction->epSet.eps[i].fqdn,
                    pAction->epSet.eps[i].port);
  }

  int32_t code = tmsgSendReq(&pAction->epSet, &rpcMsg);
  if (code == 0) {
    pAction->msgSent = 1;
    //pAction->msgReceived = 0;
    pAction->errCode = TSDB_CODE_ACTION_IN_PROGRESS;
    mInfo("trans:%d, %s:%d is sent, %s", pTrans->id, mndTransStr(pAction->stage), pAction->id, detail);

    mndSetTransLastAction(pTrans, pAction);
  } else {
    pAction->msgSent = 0;
    pAction->msgReceived = 0;
    pAction->errCode = (terrno != 0) ? terrno : code;
    mError("trans:%d, %s:%d not send since %s, %s", pTrans->id, mndTransStr(pAction->stage), pAction->id, terrstr(),
           detail);

    mndSetTransLastAction(pTrans, pAction);
  }

  return code;
}

static int32_t mndTransExecNullMsg(SMnode *pMnode, STrans *pTrans, STransAction *pAction, bool topHalf) {
  if (!topHalf) return TSDB_CODE_MND_TRANS_CTX_SWITCH;
  pAction->rawWritten = 0;
  pAction->errCode = 0;
  mInfo("trans:%d, %s:%d confirm action executed", pTrans->id, mndTransStr(pAction->stage), pAction->id);

  mndSetTransLastAction(pTrans, pAction);
  return 0;
}

static int32_t mndTransExecSingleAction(SMnode *pMnode, STrans *pTrans, STransAction *pAction, bool topHalf) {
  if (pAction->actionType == TRANS_ACTION_RAW) {
    return mndTransWriteSingleLog(pMnode, pTrans, pAction, topHalf);
  } else if (pAction->actionType == TRANS_ACTION_MSG) {
    return mndTransSendSingleMsg(pMnode, pTrans, pAction, topHalf);
  } else {
    return mndTransExecNullMsg(pMnode, pTrans, pAction, topHalf);
  }
}

static int32_t mndTransExecSingleActions(SMnode *pMnode, STrans *pTrans, SArray *pArray, bool topHalf) {
  int32_t numOfActions = taosArrayGetSize(pArray);
  int32_t code = 0;

  for (int32_t action = 0; action < numOfActions; ++action) {
    STransAction *pAction = taosArrayGet(pArray, action);
    code = mndTransExecSingleAction(pMnode, pTrans, pAction, topHalf);
    if (code != 0) {
      mInfo("trans:%d, action:%d not executed since %s. numOfActions:%d", pTrans->id, action, tstrerror(code),
            numOfActions);
      break;
    }
  }

  return code;
}

static int32_t mndTransExecuteActions(SMnode *pMnode, STrans *pTrans, SArray *pArray, bool topHalf) {
  int32_t numOfActions = taosArrayGetSize(pArray);
  int32_t code = 0;
  if (numOfActions == 0) return 0;

  if ((code = mndTransExecSingleActions(pMnode, pTrans, pArray, topHalf)) != 0) {
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
    } else {
      pErrAction = pAction;
    }
  }

  mndSetTransLastAction(pTrans, pErrAction);

  if (numOfExecuted == numOfActions) {
    if (errCode == 0) {
      mInfo("trans:%d, all %d actions execute successfully", pTrans->id, numOfActions);
      return 0;
    } else {
      mError("trans:%d, all %d actions executed, code:0x%x", pTrans->id, numOfActions, errCode & 0XFFFF);
      mndTransResetActions(pMnode, pTrans, pArray);
      terrno = errCode;
      return errCode;
    }
  } else {
    mInfo("trans:%d, %d of %d actions executed", pTrans->id, numOfExecuted, numOfActions);

    for (int32_t action = 0; action < numOfActions; ++action) {
      STransAction *pAction = taosArrayGet(pArray, action);
      mDebug("trans:%d, %s:%d Sent:%d, Received:%d, errCode:0x%x, acceptableCode:0x%x, retryCode:0x%x", 
              pTrans->id, mndTransStr(pAction->stage), pAction->id, pAction->msgSent, pAction->msgReceived,
              pAction->errCode, pAction->acceptableCode, pAction->retryCode);
      if (pAction->msgSent) {
        if (pAction->msgReceived) {
          if (pAction->errCode != 0 && pAction->errCode != pAction->acceptableCode) {
            mndTransResetAction(pMnode, pTrans, pAction);
            mInfo("trans:%d, %s:%d reset", pTrans->id, mndTransStr(pAction->stage), pAction->id);
          }
        } 
      }
    }
    return TSDB_CODE_ACTION_IN_PROGRESS;
  }
}

static int32_t mndTransExecuteRedoActions(SMnode *pMnode, STrans *pTrans, bool topHalf) {
  int32_t code = mndTransExecuteActions(pMnode, pTrans, pTrans->redoActions, topHalf);
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to execute redoActions since:%s, code:0x%x", terrstr(), terrno);
  }
  return code;
}

static int32_t mndTransExecuteUndoActions(SMnode *pMnode, STrans *pTrans, bool topHalf) {
  int32_t code = mndTransExecuteActions(pMnode, pTrans, pTrans->undoActions, topHalf);
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to execute undoActions since %s", terrstr());
  }
  return code;
}

static int32_t mndTransExecuteCommitActions(SMnode *pMnode, STrans *pTrans, bool topHalf) {
  int32_t code = mndTransExecuteActions(pMnode, pTrans, pTrans->commitActions, topHalf);
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to execute commitActions since %s", terrstr());
  }
  return code;
}

static int32_t mndTransExecuteRedoActionsSerial(SMnode *pMnode, STrans *pTrans, bool topHalf) {
  int32_t code = 0;
  int32_t numOfActions = taosArrayGetSize(pTrans->redoActions);
  if (numOfActions == 0) return code;

  taosThreadMutexLock(&pTrans->mutex);

  if (pTrans->redoActionPos >= numOfActions) {
    taosThreadMutexUnlock(&pTrans->mutex);
    return code;
  }

  mInfo("trans:%d, execute %d actions serial, current redoAction:%d", pTrans->id, numOfActions, pTrans->redoActionPos);

  for (int32_t action = pTrans->redoActionPos; action < numOfActions; ++action) {
    STransAction *pAction = taosArrayGet(pTrans->redoActions, pTrans->redoActionPos);

    code = mndTransExecSingleAction(pMnode, pTrans, pAction, topHalf);
    if (code == 0) {
      if (pAction->msgSent) {
        if (pAction->msgReceived) {
          if (pAction->errCode != 0 && pAction->errCode != pAction->acceptableCode) {
            code = pAction->errCode;
            mndTransResetAction(pMnode, pTrans, pAction);
          } else {
            mInfo("trans:%d, %s:%d execute successfully", pTrans->id, mndTransStr(pAction->stage), action);
          }
        } else {
          code = TSDB_CODE_ACTION_IN_PROGRESS;
        }
      } else if (pAction->rawWritten) {
        if (pAction->errCode != 0 && pAction->errCode != pAction->acceptableCode) {
          code = pAction->errCode;
        } else {
          mInfo("trans:%d, %s:%d write successfully", pTrans->id, mndTransStr(pAction->stage), action);
        }
      } else {
      }
    }

    if (code == 0) {
      pTrans->failedTimes = 0;
    }
    mndSetTransLastAction(pTrans, pAction);

    if (mndCannotExecuteTransAction(pMnode, topHalf)) break;

    if (code == 0) {
      pTrans->code = 0;
      pTrans->redoActionPos++;
      mInfo("trans:%d, %s:%d is executed and need sync to other mnodes", pTrans->id, mndTransStr(pAction->stage),
            pAction->id);
      taosThreadMutexUnlock(&pTrans->mutex);
      code = mndTransSync(pMnode, pTrans);
      taosThreadMutexLock(&pTrans->mutex);
      if (code != 0) {
        pTrans->redoActionPos--;
        pTrans->code = terrno;
        mError("trans:%d, %s:%d is executed and failed to sync to other mnodes since %s", pTrans->id,
               mndTransStr(pAction->stage), pAction->id, terrstr());
        break;
      }
    } else if (code == TSDB_CODE_ACTION_IN_PROGRESS) {
      mInfo("trans:%d, %s:%d is in progress and wait it finish", pTrans->id, mndTransStr(pAction->stage), pAction->id);
      break;
    } else if (code == pAction->retryCode || code == TSDB_CODE_SYN_PROPOSE_NOT_READY ||
               code == TSDB_CODE_SYN_RESTORING || code == TSDB_CODE_SYN_NOT_LEADER) {
      mInfo("trans:%d, %s:%d receive code:0x%x and retry", pTrans->id, mndTransStr(pAction->stage), pAction->id, code);
      pTrans->lastErrorNo = code;
      taosMsleep(300);
      action--;
      continue;
    } else {
      terrno = code;
      pTrans->lastErrorNo = code;
      pTrans->code = code;
      mInfo("trans:%d, %s:%d receive code:0x%x and wait another schedule, failedTimes:%d", pTrans->id,
            mndTransStr(pAction->stage), pAction->id, code, pTrans->failedTimes);
      break;
    }
  }

  taosThreadMutexUnlock(&pTrans->mutex);

  return code;
}

bool mndTransPerformPrepareStage(SMnode *pMnode, STrans *pTrans, bool topHalf) {
  bool    continueExec = true;
  int32_t code = 0;

  int32_t numOfActions = taosArrayGetSize(pTrans->prepareActions);
  if (numOfActions == 0) goto _OVER;

  mInfo("trans:%d, execute %d prepare actions.", pTrans->id, numOfActions);

  for (int32_t action = 0; action < numOfActions; ++action) {
    STransAction *pAction = taosArrayGet(pTrans->prepareActions, action);
    code = mndTransExecSingleAction(pMnode, pTrans, pAction, topHalf);
    if (code != 0) {
      mError("trans:%d, failed to execute prepare action:%d, numOfActions:%d", pTrans->id, action, numOfActions);
      return false;
    }
  }

_OVER:
  pTrans->stage = TRN_STAGE_REDO_ACTION;
  mInfo("trans:%d, stage from prepare to redoAction", pTrans->id);
  return continueExec;
}

static bool mndTransPerformRedoActionStage(SMnode *pMnode, STrans *pTrans, bool topHalf) {
  bool    continueExec = true;
  int32_t code = 0;

  if (pTrans->exec == TRN_EXEC_SERIAL) {
    code = mndTransExecuteRedoActionsSerial(pMnode, pTrans, topHalf);
  } else {
    code = mndTransExecuteRedoActions(pMnode, pTrans, topHalf);
  }

  if (mndCannotExecuteTransAction(pMnode, topHalf)) return false;
  terrno = code;

  if (code == 0) {
    pTrans->code = 0;
    pTrans->stage = TRN_STAGE_COMMIT;
    mInfo("trans:%d, stage from redoAction to commit", pTrans->id);
    continueExec = true;
  } else if (code == TSDB_CODE_ACTION_IN_PROGRESS || code == TSDB_CODE_MND_TRANS_CTX_SWITCH) {
    mInfo("trans:%d, stage keep on redoAction since %s", pTrans->id, tstrerror(code));
    continueExec = false;
  } else {
    pTrans->failedTimes++;
    pTrans->code = terrno;
    if (pTrans->policy == TRN_POLICY_ROLLBACK) {
      if (pTrans->lastAction != 0) {
        STransAction *pAction = taosArrayGet(pTrans->redoActions, pTrans->lastAction);
        if (pAction->retryCode != 0 && pAction->retryCode == pAction->errCode) {
          if (pTrans->failedTimes < 6) {
            mError("trans:%d, stage keep on redoAction since action:%d code:0x%x not 0x%x, failedTimes:%d", pTrans->id,
                   pTrans->lastAction, pTrans->code, pAction->retryCode, pTrans->failedTimes);
            taosMsleep(1000);
            continueExec = true;
            return true;
          }
        }
      }

      pTrans->stage = TRN_STAGE_ROLLBACK;
      mError("trans:%d, stage from redoAction to rollback since %s", pTrans->id, terrstr());
      continueExec = true;
    } else {
      mError("trans:%d, stage keep on redoAction since %s, failedTimes:%d", pTrans->id, terrstr(), pTrans->failedTimes);
      continueExec = false;
    }
  }

  return continueExec;
}

static bool mndTransPerformCommitStage(SMnode *pMnode, STrans *pTrans, bool topHalf) {
  if (mndCannotExecuteTransAction(pMnode, topHalf)) return false;

  bool    continueExec = true;
  int32_t code = mndTransCommit(pMnode, pTrans);

  if (code == 0) {
    pTrans->code = 0;
    pTrans->stage = TRN_STAGE_COMMIT_ACTION;
    mInfo("trans:%d, stage from commit to commitAction", pTrans->id);
    continueExec = true;
  } else {
    pTrans->code = terrno;
    pTrans->failedTimes++;
    mError("trans:%d, stage keep on commit since %s, failedTimes:%d", pTrans->id, terrstr(), pTrans->failedTimes);
    continueExec = false;
  }

  return continueExec;
}

static bool mndTransPerformCommitActionStage(SMnode *pMnode, STrans *pTrans, bool topHalf) {
  bool    continueExec = true;
  int32_t code = mndTransExecuteCommitActions(pMnode, pTrans, topHalf);

  if (code == 0) {
    pTrans->code = 0;
    pTrans->stage = TRN_STAGE_FINISH;  // TRN_STAGE_PRE_FINISH is not necessary
    mInfo("trans:%d, stage from commitAction to finished", pTrans->id);
    continueExec = true;
  } else {
    pTrans->code = terrno;
    pTrans->failedTimes++;
    mError("trans:%d, stage keep on commitAction since %s, failedTimes:%d", pTrans->id, terrstr(), pTrans->failedTimes);
    continueExec = false;
  }

  return continueExec;
}

static bool mndTransPerformUndoActionStage(SMnode *pMnode, STrans *pTrans, bool topHalf) {
  bool    continueExec = true;
  int32_t code = mndTransExecuteUndoActions(pMnode, pTrans, topHalf);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_PRE_FINISH;
    mInfo("trans:%d, stage from undoAction to pre-finish", pTrans->id);
    continueExec = true;
  } else if (code == TSDB_CODE_ACTION_IN_PROGRESS) {
    mInfo("trans:%d, stage keep on undoAction since %s", pTrans->id, tstrerror(code));
    continueExec = false;
  } else {
    pTrans->failedTimes++;
    mError("trans:%d, stage keep on undoAction since %s, failedTimes:%d", pTrans->id, terrstr(), pTrans->failedTimes);
    continueExec = false;
  }

  return continueExec;
}

static bool mndTransPerformRollbackStage(SMnode *pMnode, STrans *pTrans, bool topHalf) {
  if (mndCannotExecuteTransAction(pMnode, topHalf)) return false;

  bool    continueExec = true;
  int32_t code = mndTransRollback(pMnode, pTrans);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_UNDO_ACTION;
    mInfo("trans:%d, stage from rollback to undoAction", pTrans->id);
    continueExec = true;
  } else {
    pTrans->failedTimes++;
    mError("trans:%d, stage keep on rollback since %s, failedTimes:%d", pTrans->id, terrstr(), pTrans->failedTimes);
    continueExec = false;
  }

  return continueExec;
}

static bool mndTransPerformPreFinishStage(SMnode *pMnode, STrans *pTrans, bool topHalf) {
  if (mndCannotExecuteTransAction(pMnode, topHalf)) return false;

  bool    continueExec = true;
  int32_t code = mndTransPreFinish(pMnode, pTrans);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_FINISH;
    mInfo("trans:%d, stage from pre-finish to finish", pTrans->id);
    continueExec = true;
  } else {
    pTrans->failedTimes++;
    mError("trans:%d, stage keep on pre-finish since %s, failedTimes:%d", pTrans->id, terrstr(), pTrans->failedTimes);
    continueExec = false;
  }

  return continueExec;
}

static bool mndTransPerformFinishStage(SMnode *pMnode, STrans *pTrans, bool topHalf) {
  bool continueExec = false;
  if (topHalf) return continueExec;

  SSdbRaw *pRaw = mndTransEncode(pTrans);
  if (pRaw == NULL) {
    mError("trans:%d, failed to encode while finish trans since %s", pTrans->id, terrstr());
    return false;
  }
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_DROPPED);

  int32_t code = sdbWrite(pMnode->pSdb, pRaw);
  if (code != 0) {
    mError("trans:%d, failed to write sdb since %s", pTrans->id, terrstr());
  }

  mInfo("trans:%d, execute finished, code:0x%x, failedTimes:%d createTime:%" PRId64, pTrans->id, pTrans->code,
        pTrans->failedTimes, pTrans->createdTime);
  return continueExec;
}

void mndTransExecuteImp(SMnode *pMnode, STrans *pTrans, bool topHalf) {
  bool continueExec = true;

  while (continueExec) {
    mInfo("trans:%d, continue to execute, stage:%s createTime:%" PRId64 " topHalf:%d", pTrans->id,
          mndTransStr(pTrans->stage), pTrans->createdTime, topHalf);
    pTrans->lastExecTime = taosGetTimestampMs();
    switch (pTrans->stage) {
      case TRN_STAGE_PREPARE:
        continueExec = mndTransPerformPrepareStage(pMnode, pTrans, topHalf);
        break;
      case TRN_STAGE_REDO_ACTION:
        continueExec = mndTransPerformRedoActionStage(pMnode, pTrans, topHalf);
        break;
      case TRN_STAGE_COMMIT:
        continueExec = mndTransPerformCommitStage(pMnode, pTrans, topHalf);
        break;
      case TRN_STAGE_COMMIT_ACTION:
        continueExec = mndTransPerformCommitActionStage(pMnode, pTrans, topHalf);
        break;
      case TRN_STAGE_ROLLBACK:
        continueExec = mndTransPerformRollbackStage(pMnode, pTrans, topHalf);
        break;
      case TRN_STAGE_UNDO_ACTION:
        continueExec = mndTransPerformUndoActionStage(pMnode, pTrans, topHalf);
        break;
      case TRN_STAGE_PRE_FINISH:
        continueExec = mndTransPerformPreFinishStage(pMnode, pTrans, topHalf);
        break;
      case TRN_STAGE_FINISH:
        continueExec = mndTransPerformFinishStage(pMnode, pTrans, topHalf);
        break;
      default:
        continueExec = false;
        break;
    }
  }

  mndTransSendRpcRsp(pMnode, pTrans);
}

void mndTransExecute(SMnode *pMnode, STrans *pTrans) {
  bool topHalf = true;
  return mndTransExecuteImp(pMnode, pTrans, topHalf);
}

void mndTransRefresh(SMnode *pMnode, STrans *pTrans) {
  bool topHalf = false;
  return mndTransExecuteImp(pMnode, pTrans, topHalf);
}

static int32_t mndProcessTransTimer(SRpcMsg *pReq) {
  mTrace("start to process trans timer");
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

  for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
    STransAction *pAction = taosArrayGet(pArray, i);
    mInfo("trans:%d, %s:%d set processed for kill msg received, errCode from %s to success", pTrans->id,
          mndTransStr(pAction->stage), i, tstrerror(pAction->errCode));
    pAction->msgSent = 1;
    pAction->msgReceived = 1;
    pAction->errCode = 0;
  }

  mndTransExecute(pMnode, pTrans);
  return 0;
}

static int32_t mndProcessKillTransReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  SKillTransReq killReq = {0};
  int32_t       code = -1;
  STrans       *pTrans = NULL;

  if (tDeserializeSKillTransReq(pReq->pCont, pReq->contLen, &killReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("trans:%d, start to kill", killReq.transId);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_KILL_TRANS) != 0) {
    goto _OVER;
  }

  pTrans = mndAcquireTrans(pMnode, killReq.transId);
  if (pTrans == NULL) {
    goto _OVER;
  }

  code = mndKillTrans(pMnode, pTrans);

_OVER:
  if (code != 0) {
    mError("trans:%d, failed to kill since %s", killReq.transId, terrstr());
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
  taosArrayDestroy(pArray);
}

static int32_t mndRetrieveTrans(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfRows = 0;
  STrans *pTrans = NULL;
  int32_t cols = 0;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_TRANS, pShow->pIter, (void **)&pTrans);
    if (pShow->pIter == NULL) break;

    cols = 0;

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pTrans->id, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pTrans->createdTime, false);

    char stage[TSDB_TRANS_STAGE_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(stage, mndTransStr(pTrans->stage), pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)stage, false);

    char opername[TSDB_TRANS_OPER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(opername, pTrans->opername, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)opername, false);

    char dbname[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(dbname, mndGetDbStr(pTrans->dbname), pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)dbname, false);

    char stbname[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(stbname, mndGetDbStr(pTrans->stbname), pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)stbname, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pTrans->failedTimes, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pTrans->lastExecTime, false);

    char    lastInfo[TSDB_TRANS_ERROR_LEN + VARSTR_HEADER_SIZE] = {0};
    char    detail[TSDB_TRANS_ERROR_LEN + 1] = {0};
    int32_t len = snprintf(detail, sizeof(detail), "action:%d code:0x%x(%s) ", pTrans->lastAction,
                           pTrans->lastErrorNo & 0xFFFF, tstrerror(pTrans->lastErrorNo));
    SEpSet  epset = pTrans->lastEpset;
    if (epset.numOfEps > 0) {
      len += snprintf(detail + len, sizeof(detail) - len, "msgType:%s numOfEps:%d inUse:%d ",
                      TMSG_INFO(pTrans->lastMsgType), epset.numOfEps, epset.inUse);
      for (int32_t i = 0; i < pTrans->lastEpset.numOfEps; ++i) {
        len += snprintf(detail + len, sizeof(detail) - len, "ep:%d-%s:%u ", i, epset.eps[i].fqdn, epset.eps[i].port);
      }
    }
    STR_WITH_MAXSIZE_TO_VARSTR(lastInfo, detail, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)lastInfo, false);

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
