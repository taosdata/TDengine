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
#include "mndSync.h"

#define MND_TRANS_VER_NUMBER 1
#define MND_TRANS_ARRAY_SIZE 8
#define MND_TRANS_RESERVE_SIZE 64

static SSdbRaw *mndTransActionEncode(STrans *pTrans);
static SSdbRow *mndTransActionDecode(SSdbRaw *pRaw);
static int32_t  mndTransActionInsert(SSdb *pSdb, STrans *pTrans);
static int32_t  mndTransActionUpdate(SSdb *pSdb, STrans *OldTrans, STrans *pOldTrans);
static int32_t  mndTransActionDelete(SSdb *pSdb, STrans *pTrans);

static void    mndTransSendRpcRsp(STrans *pTrans, int32_t code);
static int32_t mndTransAppendLog(SArray *pArray, SSdbRaw *pRaw);
static int32_t mndTransAppendAction(SArray *pArray, STransAction *pAction);
static void    mndTransDropLogs(SArray *pArray);
static void    mndTransDropActions(SArray *pArray);
static int32_t mndTransExecuteLogs(SMnode *pMnode, SArray *pArray);
static int32_t mndTransExecuteActions(SMnode *pMnode, STrans *pTrans, SArray *pArray);
static int32_t mndTransExecuteRedoLogs(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransExecuteUndoLogs(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransExecuteCommitLogs(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransExecuteRedoActions(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransExecuteUndoActions(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransPerformPrepareStage(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransPerformExecuteStage(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransPerformCommitStage(SMnode *pMnode, STrans *pTrans);
static int32_t mndTransPerformRollbackStage(SMnode *pMnode, STrans *pTrans);
static void    mndTransExecute(SMnode *pMnode, STrans *pTrans);

int32_t mndInitTrans(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_TRANS,
                     .keyType = SDB_KEY_INT32,
                     .encodeFp = (SdbEncodeFp)mndTransActionEncode,
                     .decodeFp = (SdbDecodeFp)mndTransActionDecode,
                     .insertFp = (SdbInsertFp)mndTransActionInsert,
                     .updateFp = (SdbUpdateFp)mndTransActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndTransActionDelete};

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupTrans(SMnode *pMnode) {}

static SSdbRaw *mndTransActionEncode(STrans *pTrans) {
  int32_t rawDataLen = sizeof(STrans) + MND_TRANS_RESERVE_SIZE;
  int32_t redoLogNum = taosArrayGetSize(pTrans->redoLogs);
  int32_t undoLogNum = taosArrayGetSize(pTrans->undoLogs);
  int32_t commitLogNum = taosArrayGetSize(pTrans->commitLogs);
  int32_t redoActionNum = taosArrayGetSize(pTrans->redoActions);
  int32_t undoActionNum = taosArrayGetSize(pTrans->undoActions);

  for (int32_t i = 0; i < redoLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->redoLogs, i);
    rawDataLen += sdbGetRawTotalSize(pTmp);
  }

  for (int32_t i = 0; i < undoLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->undoLogs, i);
    rawDataLen += sdbGetRawTotalSize(pTmp);
  }

  for (int32_t i = 0; i < commitLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->commitLogs, i);
    rawDataLen += sdbGetRawTotalSize(pTmp);
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
  SDB_SET_INT32(pRaw, dataPos, pTrans->id)
  SDB_SET_INT8(pRaw, dataPos, pTrans->policy)
  SDB_SET_INT32(pRaw, dataPos, redoLogNum)
  SDB_SET_INT32(pRaw, dataPos, undoLogNum)
  SDB_SET_INT32(pRaw, dataPos, commitLogNum)
  SDB_SET_INT32(pRaw, dataPos, redoActionNum)
  SDB_SET_INT32(pRaw, dataPos, undoActionNum)

  for (int32_t i = 0; i < redoLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->redoLogs, i);
    int32_t  len = sdbGetRawTotalSize(pTmp);
    SDB_SET_INT32(pRaw, dataPos, len)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pTmp, len)
  }

  for (int32_t i = 0; i < undoLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->undoLogs, i);
    int32_t  len = sdbGetRawTotalSize(pTmp);
    SDB_SET_INT32(pRaw, dataPos, len)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pTmp, len)
  }

  for (int32_t i = 0; i < commitLogNum; ++i) {
    SSdbRaw *pTmp = taosArrayGetP(pTrans->commitLogs, i);
    int32_t  len = sdbGetRawTotalSize(pTmp);
    SDB_SET_INT32(pRaw, dataPos, len)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pTmp, len)
  }

  for (int32_t i = 0; i < redoActionNum; ++i) {
    STransAction *pAction = taosArrayGet(pTrans->redoActions, i);
    SDB_SET_BINARY(pRaw, dataPos, (void *)&pAction->epSet, sizeof(SEpSet));
    SDB_SET_INT16(pRaw, dataPos, pAction->msgType)
    SDB_SET_INT32(pRaw, dataPos, pAction->contLen)
    SDB_SET_BINARY(pRaw, dataPos, pAction->pCont, pAction->contLen);
  }

  for (int32_t i = 0; i < undoActionNum; ++i) {
    STransAction *pAction = taosArrayGet(pTrans->undoActions, i);
    SDB_SET_BINARY(pRaw, dataPos, (void *)&pAction->epSet, sizeof(SEpSet));
    SDB_SET_INT16(pRaw, dataPos, pAction->msgType)
    SDB_SET_INT32(pRaw, dataPos, pAction->contLen)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pAction->pCont, pAction->contLen);
  }

  SDB_SET_RESERVE(pRaw, dataPos, MND_TRANS_RESERVE_SIZE)
  SDB_SET_DATALEN(pRaw, dataPos);
  mTrace("trans:%d, encode to raw:%p, len:%d", pTrans->id, pRaw, dataPos);
  return pRaw;
}

static SSdbRow *mndTransActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != MND_TRANS_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("failed to get check soft ver from raw:%p since %s", pRaw, terrstr());
    return NULL;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(STrans));
  STrans  *pTrans = sdbGetRowObj(pRow);
  if (pTrans == NULL) {
    mError("failed to alloc trans from raw:%p since %s", pRaw, terrstr());
    return NULL;
  }

  pTrans->redoLogs = taosArrayInit(MND_TRANS_ARRAY_SIZE, sizeof(void *));
  pTrans->undoLogs = taosArrayInit(MND_TRANS_ARRAY_SIZE, sizeof(void *));
  pTrans->commitLogs = taosArrayInit(MND_TRANS_ARRAY_SIZE, sizeof(void *));
  pTrans->redoActions = taosArrayInit(MND_TRANS_ARRAY_SIZE, sizeof(STransAction));
  pTrans->undoActions = taosArrayInit(MND_TRANS_ARRAY_SIZE, sizeof(STransAction));

  if (pTrans->redoLogs == NULL || pTrans->undoLogs == NULL || pTrans->commitLogs == NULL ||
      pTrans->redoActions == NULL || pTrans->undoActions == NULL) {
    mDebug("trans:%d, failed to create array while parsed from raw:%p", pTrans->id, pRaw);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto TRANS_DECODE_OVER;
  }

  int32_t redoLogNum = 0;
  int32_t undoLogNum = 0;
  int32_t commitLogNum = 0;
  int32_t redoActionNum = 0;
  int32_t undoActionNum = 0;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, pRow, dataPos, &pTrans->id)
  SDB_GET_INT8(pRaw, pRow, dataPos, (int8_t *)&pTrans->policy)
  SDB_GET_INT32(pRaw, pRow, dataPos, &redoLogNum)
  SDB_GET_INT32(pRaw, pRow, dataPos, &undoLogNum)
  SDB_GET_INT32(pRaw, pRow, dataPos, &commitLogNum)
  SDB_GET_INT32(pRaw, pRow, dataPos, &redoActionNum)
  SDB_GET_INT32(pRaw, pRow, dataPos, &undoActionNum)

  for (int32_t i = 0; i < redoLogNum; ++i) {
    int32_t dataLen = 0;
    SDB_GET_INT32(pRaw, pRow, dataPos, &dataLen)
    char *pData = malloc(dataLen);
    SDB_GET_BINARY(pRaw, pRow, dataPos, pData, dataLen);

    void *ret = taosArrayPush(pTrans->redoLogs, &pData);
    if (ret == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto TRANS_DECODE_OVER;
    }
  }

  for (int32_t i = 0; i < undoLogNum; ++i) {
    int32_t dataLen = 0;
    SDB_GET_INT32(pRaw, pRow, dataPos, &dataLen)
    char *pData = malloc(dataLen);
    SDB_GET_BINARY(pRaw, pRow, dataPos, pData, dataLen);

    void *ret = taosArrayPush(pTrans->undoLogs, &pData);
    if (ret == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto TRANS_DECODE_OVER;
    }
  }

  for (int32_t i = 0; i < commitLogNum; ++i) {
    int32_t dataLen = 0;
    SDB_GET_INT32(pRaw, pRow, dataPos, &dataLen)
    char *pData = malloc(dataLen);
    SDB_GET_BINARY(pRaw, pRow, dataPos, pData, dataLen);

    void *ret = taosArrayPush(pTrans->commitLogs, &pData);
    if (ret == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto TRANS_DECODE_OVER;
    }
  }

  for (int32_t i = 0; i < redoActionNum; ++i) {
    STransAction action = {0};
    SDB_GET_BINARY(pRaw, pRow, dataPos, (void *)&action.epSet, sizeof(SEpSet));
    SDB_GET_INT16(pRaw, pRow, dataPos, &action.msgType)
    SDB_GET_INT32(pRaw, pRow, dataPos, &action.contLen)
    action.pCont = malloc(action.contLen);
    if (action.pCont == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto TRANS_DECODE_OVER;
    }
    SDB_GET_BINARY(pRaw, pRow, dataPos, action.pCont, action.contLen);

    void *ret = taosArrayPush(pTrans->redoActions, &action);
    if (ret == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto TRANS_DECODE_OVER;
    }
  }

  for (int32_t i = 0; i < undoActionNum; ++i) {
    STransAction action = {0};
    SDB_GET_BINARY(pRaw, pRow, dataPos, (void *)&action.epSet, sizeof(SEpSet));
    SDB_GET_INT16(pRaw, pRow, dataPos, &action.msgType)
    SDB_GET_INT32(pRaw, pRow, dataPos, &action.contLen)
    action.pCont = malloc(action.contLen);
    if (action.pCont == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto TRANS_DECODE_OVER;
    }
    SDB_GET_BINARY(pRaw, pRow, dataPos, action.pCont, action.contLen);

    void *ret = taosArrayPush(pTrans->undoActions, &action);
    if (ret == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto TRANS_DECODE_OVER;
    }
  }

  SDB_GET_RESERVE(pRaw, pRow, dataPos, MND_TRANS_RESERVE_SIZE)

TRANS_DECODE_OVER:
  if (code != 0) {
    mError("trans:%d, failed to parse from raw:%p since %s", pTrans->id, pRaw, tstrerror(errno));
    mndTransDrop(pTrans);
    terrno = code;
    return NULL;
  }

  mTrace("trans:%d, decode from raw:%p", pTrans->id, pRaw);
  return pRow;
}

static int32_t mndTransActionInsert(SSdb *pSdb, STrans *pTrans) {
  pTrans->stage = TRN_STAGE_PREPARE;
  mTrace("trans:%d, perform insert action, stage:%s", pTrans->id, mndTransStageStr(pTrans->stage));
  return 0;
}

static int32_t mndTransActionDelete(SSdb *pSdb, STrans *pTrans) {
  mTrace("trans:%d, perform delete action, stage:%s", pTrans->id, mndTransStageStr(pTrans->stage));

  mndTransDropLogs(pTrans->redoLogs);
  mndTransDropLogs(pTrans->undoLogs);
  mndTransDropLogs(pTrans->commitLogs);
  mndTransDropActions(pTrans->redoActions);
  mndTransDropActions(pTrans->undoActions);

  return 0;
}

static int32_t mndTransActionUpdate(SSdb *pSdb, STrans *pOldTrans, STrans *pNewTrans) {
  mTrace("trans:%d, perform update action, stage:%s", pOldTrans->id, mndTransStageStr(pNewTrans->stage));
  pOldTrans->stage = pNewTrans->stage;
  return 0;
}

STrans *mndAcquireTrans(SMnode *pMnode, int32_t transId) {
  SSdb   *pSdb = pMnode->pSdb;
  STrans *pTrans = sdbAcquire(pSdb, SDB_TRANS, &transId);
  if (pTrans == NULL) {
    terrno = TSDB_CODE_MND_TRANS_NOT_EXIST;
  }
  return pTrans;
}

void mndReleaseTrans(SMnode *pMnode, STrans *pTrans) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pTrans);
}

char *mndTransStageStr(ETrnStage stage) {
  switch (stage) {
    case TRN_STAGE_PREPARE:
      return "prepare";
    case TRN_STAGE_EXECUTE:
      return "execute";
    case TRN_STAGE_COMMIT:
      return "commit";
    case TRN_STAGE_ROLLBACK:
      return "rollback";
    case TRN_STAGE_OVER:
      return "over";
    default:
      return "undefined";
  }
}

char *mndTransPolicyStr(ETrnPolicy policy) {
  switch (policy) {
    case TRN_POLICY_ROLLBACK:
      return "prepare";
    case TRN_POLICY_RETRY:
      return "retry";
    default:
      return "undefined";
  }
}

STrans *mndTransCreate(SMnode *pMnode, ETrnPolicy policy, SRpcMsg *pMsg) {
  STrans *pTrans = calloc(1, sizeof(STrans));
  if (pTrans == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to create transaction since %s", terrstr());
    return NULL;
  }

  pTrans->id = sdbGetMaxId(pMnode->pSdb, SDB_TRANS);
  pTrans->stage = TRN_STAGE_PREPARE;
  pTrans->policy = policy;
  pTrans->rpcHandle = pMsg->handle;
  pTrans->rpcAHandle = pMsg->ahandle;
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

  mDebug("trans:%d, is created", pTrans->id);
  return pTrans;
}

static void mndTransDropLogs(SArray *pArray) {
  for (int32_t i = 0; i < pArray->size; ++i) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, i);
    tfree(pRaw);
  }

  taosArrayDestroy(pArray);
}

static void mndTransDropActions(SArray *pArray) {
  for (int32_t i = 0; i < pArray->size; ++i) {
    STransAction *pAction = taosArrayGet(pArray, i);
    free(pAction->pCont);
  }

  taosArrayDestroy(pArray);
}

void mndTransDrop(STrans *pTrans) {
  mndTransDropLogs(pTrans->redoLogs);
  mndTransDropLogs(pTrans->undoLogs);
  mndTransDropLogs(pTrans->commitLogs);
  mndTransDropActions(pTrans->redoActions);
  mndTransDropActions(pTrans->undoActions);

  // mDebug("trans:%d, is dropped, data:%p", pTrans->id, pTrans);
  tfree(pTrans);
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

int32_t mndTransAppendRedolog(STrans *pTrans, SSdbRaw *pRaw) {
  int32_t code = mndTransAppendLog(pTrans->redoLogs, pRaw);
  mTrace("trans:%d, raw:%p append to redo logs, code:0x%x", pTrans->id, pRaw, code);
  return code;
}

int32_t mndTransAppendUndolog(STrans *pTrans, SSdbRaw *pRaw) {
  int32_t code = mndTransAppendLog(pTrans->undoLogs, pRaw);
  mTrace("trans:%d, raw:%p append to undo logs, code:0x%x", pTrans->id, pRaw, code);
  return code;
}

int32_t mndTransAppendCommitlog(STrans *pTrans, SSdbRaw *pRaw) {
  int32_t code = mndTransAppendLog(pTrans->commitLogs, pRaw);
  mTrace("trans:%d, raw:%p append to commit logs, code:0x%x", pTrans->id, pRaw, code);
  return code;
}

static int32_t mndTransAppendAction(SArray *pArray, STransAction *pAction) {
  void *ptr = taosArrayPush(pArray, pAction);
  if (ptr == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

int32_t mndTransAppendRedoAction(STrans *pTrans, STransAction *pAction) {
  int32_t code = mndTransAppendAction(pTrans->redoActions, pAction);
  mTrace("trans:%d, msg:%s append to redo actions, code:0x%x", pTrans->id, TMSG_INFO(pAction->msgType), code);
  return code;
}

int32_t mndTransAppendUndoAction(STrans *pTrans, STransAction *pAction) {
  int32_t code = mndTransAppendAction(pTrans->undoActions, pAction);
  mTrace("trans:%d, msg:%s append to undo actions, code:0x%x", pTrans->id, TMSG_INFO(pAction->msgType), code);
  return code;
}

int32_t mndTransPrepare(SMnode *pMnode, STrans *pTrans) {
  mDebug("trans:%d, prepare transaction", pTrans->id);

  SSdbRaw *pRaw = mndTransActionEncode(pTrans);
  if (pRaw == NULL) {
    mError("trans:%d, failed to decode trans since %s", pTrans->id, terrstr());
    return -1;
  }
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mTrace("trans:%d, sync to other nodes", pTrans->id);
  int32_t code = mndSyncPropose(pMnode, pRaw);
  if (code != 0) {
    mError("trans:%d, failed to sync since %s", pTrans->id, terrstr());
    sdbFreeRaw(pRaw);
    return -1;
  }

  mTrace("trans:%d, sync finished", pTrans->id);

  code = sdbWrite(pMnode->pSdb, pRaw);
  if (code != 0) {
    mError("trans:%d, failed to write sdb since %s", pTrans->id, terrstr());
    return -1;
  }

  STrans *pNewTrans = mndAcquireTrans(pMnode, pTrans->id);
  if (pNewTrans == NULL) {
    mError("trans:%d, failed to read from sdb since %s", pTrans->id, terrstr());
    return -1;
  }

  pNewTrans->rpcHandle = pTrans->rpcHandle;
  pNewTrans->rpcAHandle = pTrans->rpcAHandle;
  mndTransExecute(pMnode, pNewTrans);
  mndReleaseTrans(pMnode, pNewTrans);
  return 0;
}

int32_t mndTransCommit(SMnode *pMnode, STrans *pTrans) {
  mDebug("trans:%d, commit transaction", pTrans->id);

  SSdbRaw *pRaw = mndTransActionEncode(pTrans);
  if (pRaw == NULL) {
    mError("trans:%d, failed to decode trans since %s", pTrans->id, terrstr());
    return -1;
  }
  sdbSetRawStatus(pRaw, SDB_STATUS_DROPPED);

  if (taosArrayGetSize(pTrans->commitLogs) != 0) {
    mTrace("trans:%d, sync to other nodes", pTrans->id);
    if (mndSyncPropose(pMnode, pRaw) != 0) {
      mError("trans:%d, failed to sync since %s", pTrans->id, terrstr());
      sdbFreeRaw(pRaw);
      return -1;
    }
    mTrace("trans:%d, sync finished", pTrans->id);
  }

  if (sdbWrite(pMnode->pSdb, pRaw) != 0) {
    mError("trans:%d, failed to write sdb since %s", pTrans->id, terrstr());
    return -1;
  }

  mDebug("trans:%d, commit finished", pTrans->id);
  return 0;
}

int32_t mndTransRollback(SMnode *pMnode, STrans *pTrans) {
  mDebug("trans:%d, rollback transaction", pTrans->id);

  SSdbRaw *pRaw = mndTransActionEncode(pTrans);
  if (pRaw == NULL) {
    mError("trans:%d, failed to decode trans since %s", pTrans->id, terrstr());
    return -1;
  }
  sdbSetRawStatus(pRaw, SDB_STATUS_DROPPED);

  mTrace("trans:%d, sync to other nodes", pTrans->id);
  int32_t code = mndSyncPropose(pMnode, pRaw);
  if (code != 0) {
    mError("trans:%d, failed to sync since %s", pTrans->id, terrstr());
    sdbFreeRaw(pRaw);
    return -1;
  }

  mTrace("trans:%d, sync finished", pTrans->id);
  code = sdbWrite(pMnode->pSdb, pRaw);
  if (code != 0) {
    mError("trans:%d, failed to write sdb since %s", pTrans->id, terrstr());
    return -1;
  }

  mDebug("trans:%d, rollback finished", pTrans->id);
  return 0;
}

static void mndTransSendRpcRsp(STrans *pTrans, int32_t code) {
  if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) return;
  mDebug("trans:%d, send rpc rsp, RPC:%p ahandle:%p code:0x%x", pTrans->id, pTrans->rpcHandle, pTrans->rpcAHandle,
         code & 0xFFFF);

  if (pTrans->rpcHandle != NULL) {
    SRpcMsg rspMsg = {.handle = pTrans->rpcHandle, .code = code, .ahandle = pTrans->rpcAHandle};
    rpcSendResponse(&rspMsg);
  }
}

void mndTransApply(SMnode *pMnode, SSdbRaw *pRaw, STransMsg *pMsg, int32_t code) {
  // todo
}

void mndTransHandleActionRsp(SMnodeMsg *pMsg) {
  SMnode *pMnode = pMsg->pMnode;
  int64_t sig = (int64_t)(pMsg->rpcMsg.ahandle);
  int32_t transId = (int32_t)(sig >> 32);
  int32_t action = (int32_t)((sig << 32) >> 32);

  STrans *pTrans = mndAcquireTrans(pMnode, transId);
  if (pTrans == NULL) {
    mError("trans:%d, failed to get transId from vnode rsp since %s", transId, terrstr());
    goto HANDLE_ACTION_RSP_OVER;
  }

  SArray *pArray = NULL;
  if (pTrans->stage == TRN_STAGE_EXECUTE) {
    pArray = pTrans->redoActions;
  } else if (pTrans->stage == TRN_STAGE_ROLLBACK) {
    pArray = pTrans->undoActions;
  } else {
  }

  if (pArray == NULL) {
    mError("trans:%d, invalid trans stage:%s", transId, mndTransStageStr(pTrans->stage));
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
    pAction->errCode = pMsg->rpcMsg.code;
  }

  mDebug("trans:%d, action:%d response is received, code:0x%x", transId, action, pMsg->rpcMsg.code);
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

static int32_t mndTransExecuteActions(SMnode *pMnode, STrans *pTrans, SArray *pArray) {
  int32_t numOfActions = taosArrayGetSize(pArray);
  if (numOfActions == 0) return 0;

  for (int32_t action = 0; action < numOfActions; ++action) {
    STransAction *pAction = taosArrayGet(pArray, action);
    if (pAction == NULL) continue;
    if (pAction->msgSent && !pAction->msgReceived) continue;
    if (pAction->msgSent && pAction->msgReceived && pAction->errCode == 0) continue;

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

    pAction->msgSent = 1;
    pAction->msgReceived = 0;
    pAction->errCode = 0;

    mDebug("trans:%d, action:%d is sent", pTrans->id, action);
    mndSendMsgToDnode(pMnode, &pAction->epSet, &rpcMsg);
  }

  int32_t numOfReceived = 0;
  int32_t errCode = 0;
  for (int32_t action = 0; action < numOfActions; ++action) {
    STransAction *pAction = taosArrayGet(pArray, action);
    if (pAction == NULL) continue;
    if (pAction->msgSent && pAction->msgReceived) {
      numOfReceived++;
      if (pAction->errCode != 0) {
        errCode = pAction->errCode;
      }
    }
  }

  if (numOfReceived == numOfActions) {
    mDebug("trans:%d, all %d actions executed, code:0x%x", pTrans->id, numOfActions, errCode);
    terrno = errCode;
    return errCode;
  } else {
    mDebug("trans:%d, %d of %d actions executed, code:0x%x", pTrans->id, numOfReceived, numOfActions, errCode);
    return TSDB_CODE_MND_ACTION_IN_PROGRESS;
  }
}

static int32_t mndTransExecuteRedoActions(SMnode *pMnode, STrans *pTrans) {
  return mndTransExecuteActions(pMnode, pTrans, pTrans->redoActions);
}

static int32_t mndTransExecuteUndoActions(SMnode *pMnode, STrans *pTrans) {
  return mndTransExecuteActions(pMnode, pTrans, pTrans->undoActions);
}

static int32_t mndTransPerformPrepareStage(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteRedoLogs(pMnode, pTrans);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_EXECUTE;
    mDebug("trans:%d, stage from prepare to execute", pTrans->id);
  } else {
    pTrans->stage = TRN_STAGE_ROLLBACK;
    mError("trans:%d, stage from prepare to rollback since %s", pTrans->id, terrstr());
  }

  return 0;
}

static int32_t mndTransPerformExecuteStage(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteRedoActions(pMnode, pTrans);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_COMMIT;
    mDebug("trans:%d, stage from execute to commit", pTrans->id);
  } else if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mDebug("trans:%d, stage keep on execute since %s", pTrans->id, tstrerror(code));
  } else {
    if (pTrans->policy == TRN_POLICY_ROLLBACK) {
      pTrans->stage = TRN_STAGE_ROLLBACK;
      mError("trans:%d, stage from execute to rollback since %s", pTrans->id, terrstr());
    } else {
      pTrans->stage = TRN_STAGE_EXECUTE;
      mError("trans:%d, stage keep on execute since %s", pTrans->id, terrstr());
    }
  }

  return code;
}

static int32_t mndTransPerformCommitStage(SMnode *pMnode, STrans *pTrans) {
  mndTransExecuteCommitLogs(pMnode, pTrans);
  pTrans->stage = TRN_STAGE_OVER;
  return 0;
}

static int32_t mndTransPerformRollbackStage(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteUndoActions(pMnode, pTrans);

  if (code == 0) {
    mDebug("trans:%d, rollbacked", pTrans->id);
  } else {
    pTrans->stage = TRN_STAGE_ROLLBACK;
    mError("trans:%d, stage keep on rollback since %s", pTrans->id, terrstr());
  }

  return code;
}

static void mndTransExecute(SMnode *pMnode, STrans *pTrans) {
  int32_t code = 0;

  while (code == 0) {
    switch (pTrans->stage) {
      case TRN_STAGE_PREPARE:
        code = mndTransPerformPrepareStage(pMnode, pTrans);
        break;
      case TRN_STAGE_EXECUTE:
        code = mndTransPerformExecuteStage(pMnode, pTrans);
        break;
      case TRN_STAGE_COMMIT:
        code = mndTransCommit(pMnode, pTrans);
        if (code == 0) {
          mndTransPerformCommitStage(pMnode, pTrans);
        }
        break;
      case TRN_STAGE_ROLLBACK:
        code = mndTransRollback(pMnode, pTrans);
        if (code == 0) {
          mndTransPerformRollbackStage(pMnode, pTrans);
        }
        break;
      default:
        mndTransSendRpcRsp(pTrans, 0);
        return;
    }
  }

  mndTransSendRpcRsp(pTrans, code);
}
