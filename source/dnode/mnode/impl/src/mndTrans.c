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

#define TSDB_TRANS_VER 1
#define TSDB_TRN_ARRAY_SIZE 8
#define TSDB_TRN_RESERVE_SIZE 64

static SSdbRaw *mndTransActionEncode(STrans *pTrans);
static SSdbRow *mndTransActionDecode(SSdbRaw *pRaw);
static int32_t  mndTransActionInsert(SSdb *pSdb, STrans *pTrans);
static int32_t  mndTransActionUpdate(SSdb *pSdb, STrans *OldTrans, STrans *pOldTrans);
static int32_t  mndTransActionDelete(SSdb *pSdb, STrans *pTrans);

static void    mndTransSetRpcHandle(STrans *pTrans, void *rpcHandle);
static void    mndTransSendRpcRsp(STrans *pTrans, int32_t code);
static int32_t mndTransAppendArray(SArray *pArray, SSdbRaw *pRaw);
static void    mndTransDropArray(SArray *pArray);
static int32_t mndTransExecuteArray(SMnode *pMnode, SArray *pArray);
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
  int32_t rawDataLen = 16 * sizeof(int32_t);
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

  SSdbRaw *pRaw = sdbAllocRaw(SDB_TRANS, TSDB_TRANS_VER, rawDataLen);
  if (pRaw == NULL) {
    mError("trans:%d, failed to alloc raw since %s", pTrans->id, terrstr());
    return NULL;
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pTrans->id)
  SDB_SET_INT8(pRaw, dataPos, pTrans->stage)
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

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_TRN_RESERVE_SIZE)
  SDB_SET_DATALEN(pRaw, dataPos);
  mTrace("trans:%d, encode to raw:%p, len:%d", pTrans->id, pRaw, dataPos);
  return pRaw;
}

static SSdbRow *mndTransActionDecode(SSdbRaw *pRaw) {
  int32_t code = 0;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    mError("failed to get soft ver from raw:%p since %s", pRaw, terrstr());
    return NULL;
  }

  if (sver != TSDB_TRANS_VER) {
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

  pTrans->redoLogs = taosArrayInit(TSDB_TRN_ARRAY_SIZE, sizeof(void *));
  pTrans->undoLogs = taosArrayInit(TSDB_TRN_ARRAY_SIZE, sizeof(void *));
  pTrans->commitLogs = taosArrayInit(TSDB_TRN_ARRAY_SIZE, sizeof(void *));
  pTrans->redoActions = taosArrayInit(TSDB_TRN_ARRAY_SIZE, sizeof(void *));
  pTrans->undoActions = taosArrayInit(TSDB_TRN_ARRAY_SIZE, sizeof(void *));

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
  SDB_GET_INT8(pRaw, pRow, dataPos, (int8_t *)&pTrans->stage)
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
      break;
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
      break;
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
      break;
    }
  }

  SDB_GET_RESERVE(pRaw, pRow, dataPos, TSDB_TRN_RESERVE_SIZE)

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
  mTrace("trans:%d, perform insert action, stage:%s", pTrans->id, mndTransStageStr(pTrans->stage));
  return 0;
}

static int32_t mndTransActionDelete(SSdb *pSdb, STrans *pTrans) {
  mTrace("trans:%d, perform delete action, stage:%s", pTrans->id, mndTransStageStr(pTrans->stage));

  mndTransDropArray(pTrans->redoLogs);
  mndTransDropArray(pTrans->undoLogs);
  mndTransDropArray(pTrans->commitLogs);
  mndTransDropArray(pTrans->redoActions);
  mndTransDropArray(pTrans->undoActions);

  return 0;
}

static int32_t mndTransActionUpdate(SSdb *pSdb, STrans *pOldTrans, STrans *pNewTrans) {
  mTrace("trans:%d, perform update action, stage:%s", pOldTrans->id, mndTransStageStr(pNewTrans->stage));
  pOldTrans->stage = pNewTrans->stage;
  return 0;
}

STrans *mndAcquireTrans(SMnode *pMnode, int32_t transId) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbAcquire(pSdb, SDB_TRANS, &transId);
}

void mndReleaseTrans(SMnode *pMnode, STrans *pTrans) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pTrans);
}

static int32_t trnGenerateTransId() {
  static int32_t tmp = 0;
  return ++tmp;
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
    case TRN_STAGE_RETRY:
      return "retry";
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

STrans *mndTransCreate(SMnode *pMnode, ETrnPolicy policy, void *rpcHandle) {
  STrans *pTrans = calloc(1, sizeof(STrans));
  if (pTrans == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to create transaction since %s", terrstr());
    return NULL;
  }

  pTrans->id = trnGenerateTransId();
  pTrans->stage = TRN_STAGE_PREPARE;
  pTrans->policy = policy;
  pTrans->rpcHandle = rpcHandle;
  pTrans->redoLogs = taosArrayInit(TSDB_TRN_ARRAY_SIZE, sizeof(void *));
  pTrans->undoLogs = taosArrayInit(TSDB_TRN_ARRAY_SIZE, sizeof(void *));
  pTrans->commitLogs = taosArrayInit(TSDB_TRN_ARRAY_SIZE, sizeof(void *));
  pTrans->redoActions = taosArrayInit(TSDB_TRN_ARRAY_SIZE, sizeof(void *));
  pTrans->undoActions = taosArrayInit(TSDB_TRN_ARRAY_SIZE, sizeof(void *));

  if (pTrans->redoLogs == NULL || pTrans->undoLogs == NULL || pTrans->commitLogs == NULL ||
      pTrans->redoActions == NULL || pTrans->undoActions == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to create transaction since %s", terrstr());
    return NULL;
  }

  mDebug("trans:%d, data:%p is created", pTrans->id, pTrans);
  return pTrans;
}

static void mndTransDropArray(SArray *pArray) {
  for (int32_t i = 0; i < pArray->size; ++i) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, i);
    tfree(pRaw);
  }

  taosArrayDestroy(pArray);
}

void mndTransDrop(STrans *pTrans) {
  mndTransDropArray(pTrans->redoLogs);
  mndTransDropArray(pTrans->undoLogs);
  mndTransDropArray(pTrans->commitLogs);
  mndTransDropArray(pTrans->redoActions);
  mndTransDropArray(pTrans->undoActions);

  mDebug("trans:%d, data:%p is dropped", pTrans->id, pTrans);
  tfree(pTrans);
}

static void mndTransSetRpcHandle(STrans *pTrans, void *rpcHandle) {
  pTrans->rpcHandle = rpcHandle;
  mTrace("trans:%d, set rpc handle:%p", pTrans->id, rpcHandle);
}

static int32_t mndTransAppendArray(SArray *pArray, SSdbRaw *pRaw) {
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
  int32_t code = mndTransAppendArray(pTrans->redoLogs, pRaw);
  mTrace("trans:%d, raw:%p append to redo logs, code:0x%x", pTrans->id, pRaw, code);
  return code;
}

int32_t mndTransAppendUndolog(STrans *pTrans, SSdbRaw *pRaw) {
  int32_t code = mndTransAppendArray(pTrans->undoLogs, pRaw);
  mTrace("trans:%d, raw:%p append to undo logs, code:0x%x", pTrans->id, pRaw, code);
  return code;
}

int32_t mndTransAppendCommitlog(STrans *pTrans, SSdbRaw *pRaw) {
  int32_t code = mndTransAppendArray(pTrans->commitLogs, pRaw);
  mTrace("trans:%d, raw:%p append to commit logs, code:0x%x", pTrans->id, pRaw, code);
  return code;
}

int32_t mndTransAppendRedoAction(STrans *pTrans, SEpSet *pEpSet, void *pMsg) {
  int32_t code = mndTransAppendArray(pTrans->redoActions, pMsg);
  mTrace("trans:%d, msg:%p append to redo actions", pTrans->id, pMsg);
  return code;
}

int32_t mndTransAppendUndoAction(STrans *pTrans, SEpSet *pEpSet, void *pMsg) {
  int32_t code = mndTransAppendArray(pTrans->undoActions, pMsg);
  mTrace("trans:%d, msg:%p append to undo actions", pTrans->id, pMsg);
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

  mTrace("trans:%d, start sync", pTrans->id);
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
    mError("trans:%d, failed to ready from sdb since %s", pTrans->id, terrstr());
    return -1;
  }

  mDebug("trans:%d, prepare finished", pNewTrans->id);
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

  mTrace("trans:%d, start sync", pTrans->id);
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

  mTrace("trans:%d, start sync", pTrans->id);
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
  mDebug("trans:%d, send rpc rsp, RPC:%p code:0x%x", pTrans->id, pTrans->rpcHandle, code & 0xFFFF);

  if (pTrans->rpcHandle != NULL) {
    SRpcMsg rspMsg = {.handle = pTrans->rpcHandle, .code = code};
    rpcSendResponse(&rspMsg);
  }
}

void mndTransApply(SMnode *pMnode, SSdbRaw *pRaw, STransMsg *pMsg, int32_t code) {
  // todo
}

static int32_t mndTransExecuteArray(SMnode *pMnode, SArray *pArray) {
  SSdb   *pSdb = pMnode->pSdb;
  int32_t arraySize = taosArrayGetSize(pArray);

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
  int32_t code = mndTransExecuteArray(pMnode, pTrans->redoLogs);
  if (code != 0) {
    mError("trans:%d, failed to execute redo logs since %s", pTrans->id, terrstr())
  } else {
    mTrace("trans:%d, execute redo logs finished", pTrans->id)
  }

  return code;
}

static int32_t mndTransExecuteUndoLogs(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteArray(pMnode, pTrans->undoLogs);
  if (code != 0) {
    mError("trans:%d, failed to execute undo logs since %s", pTrans->id, terrstr())
  } else {
    mTrace("trans:%d, execute undo logs finished", pTrans->id)
  }

  return code;
}

static int32_t mndTransExecuteCommitLogs(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteArray(pMnode, pTrans->commitLogs);
  if (code != 0) {
    mError("trans:%d, failed to execute commit logs since %s", pTrans->id, terrstr())
  } else {
    mTrace("trans:%d, execute commit logs finished", pTrans->id)
  }

  return code;
}

static int32_t mndTransExecuteRedoActions(SMnode *pMnode, STrans *pTrans) {
  mTrace("trans:%d, execute redo actions finished", pTrans->id);
  return 0;
}

static int32_t mndTransExecuteUndoActions(SMnode *pMnode, STrans *pTrans) {
  mTrace("trans:%d, execute undo actions finished", pTrans->id);
  return 0;
}

static int32_t mndTransPerformPrepareStage(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteRedoLogs(pMnode, pTrans);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_EXECUTE;
    mTrace("trans:%d, stage from prepare to execute", pTrans->id);
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
    mTrace("trans:%d, stage from execute to commit", pTrans->id);
  } else if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mTrace("trans:%d, stage keep on execute since %s", pTrans->id, terrstr(code));
    return code;
  } else {
    if (pTrans->policy == TRN_POLICY_ROLLBACK) {
      pTrans->stage = TRN_STAGE_ROLLBACK;
      mError("trans:%d, stage from execute to rollback since %s", pTrans->id, terrstr());
    } else {
      pTrans->stage = TRN_STAGE_RETRY;
      mError("trans:%d, stage from execute to retry since %s", pTrans->id, terrstr());
    }
  }

  return 0;
}

static int32_t mndTransPerformCommitStage(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteCommitLogs(pMnode, pTrans);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_COMMIT;
    mTrace("trans:%d, commit stage finished", pTrans->id);
  } else {
    if (pTrans->policy == TRN_POLICY_ROLLBACK) {
      pTrans->stage = TRN_STAGE_ROLLBACK;
      mError("trans:%d, stage from commit to rollback since %s", pTrans->id, terrstr());
    } else {
      pTrans->stage = TRN_STAGE_RETRY;
      mError("trans:%d, stage from commit to retry since %s", pTrans->id, terrstr());
    }
  }

  return code;
}

static int32_t mndTransPerformRollbackStage(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteUndoActions(pMnode, pTrans);

  if (code == 0) {
    mTrace("trans:%d, rollbacked", pTrans->id);
  } else {
    pTrans->stage = TRN_STAGE_ROLLBACK;
    mError("trans:%d, stage keep on rollback since %s", pTrans->id, terrstr());
  }

  return code;
}

static int32_t mndTransPerformRetryStage(SMnode *pMnode, STrans *pTrans) {
  int32_t code = mndTransExecuteRedoActions(pMnode, pTrans);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_COMMIT;
    mTrace("trans:%d, stage from retry to commit", pTrans->id);
  } else {
    pTrans->stage = TRN_STAGE_RETRY;
    mError("trans:%d, stage keep on retry since %s", pTrans->id, terrstr());
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
          code = mndTransPerformCommitStage(pMnode, pTrans);
        }
        break;
      case TRN_STAGE_ROLLBACK:
        code = mndTransPerformRollbackStage(pMnode, pTrans);
        if (code == 0) {
          code = mndTransRollback(pMnode, pTrans);
        }
        break;
      case TRN_STAGE_RETRY:
        code = mndTransPerformRetryStage(pMnode, pTrans);
        break;
    }
  }

  mndTransSendRpcRsp(pTrans, code);
}
