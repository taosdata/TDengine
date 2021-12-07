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

#define SDB_TRANS_VER 1
#define TRN_DEFAULT_ARRAY_SIZE 8

static SSdbRaw *mndTransActionEncode(STrans *pTrans);
static SSdbRow *mndTransActionDecode(SSdbRaw *pRaw);
static int32_t  mndTransActionInsert(SSdb *pSdb, STrans *pTrans);
static int32_t  mndTransActionUpdate(SSdb *pSdb, STrans *OldTrans, STrans *pOldTrans);
static int32_t  mndTransActionDelete(SSdb *pSdb, STrans *pTrans);

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

  SSdbRaw *pRaw = sdbAllocRaw(SDB_TRANS, SDB_TRANS_VER, rawDataLen);
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

  if (sver != SDB_TRANS_VER) {
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

  pTrans->redoLogs = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->undoLogs = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->commitLogs = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->redoActions = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->undoActions = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));

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
  mTrace("trans:%d, perform insert action, stage:%d", pTrans->id, pTrans->stage);

  SArray *pArray = pTrans->redoLogs;
  int32_t arraySize = taosArrayGetSize(pArray);

  for (int32_t i = 0; i < arraySize; ++i) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, i);
    int32_t  code = sdbWrite(pSdb, pRaw);
    if (code != 0) {
      mError("trans:%d, failed to write raw:%p to sdb since %s", pTrans->id, pRaw, terrstr());
      return code;
    }
  }
  return 0;
}

static int32_t mndTransActionDelete(SSdb *pSdb, STrans *pTrans) {
  mTrace("trans:%d, perform delete action, stage:%d", pTrans->id, pTrans->stage);

  SArray *pArray = pTrans->undoLogs;
  int32_t arraySize = taosArrayGetSize(pArray);

  for (int32_t i = 0; i < arraySize; ++i) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, i);
    int32_t  code = sdbWrite(pSdb, pRaw);
    if (code != 0) {
      mError("trans:%d, failed to write raw:%p to sdb since %s", pTrans->id, pRaw, terrstr());
      return code;
    }
  }

  return 0;
}

static int32_t mndTransActionUpdate(SSdb *pSdb, STrans *pOldTrans, STrans *pNewTrans) {
  mTrace("trans:%d, perform update action, stage:%d", pOldTrans->id, pNewTrans->stage);

  SArray *pArray = pOldTrans->commitLogs;
  int32_t arraySize = taosArrayGetSize(pArray);

  for (int32_t i = 0; i < arraySize; ++i) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, i);
    int32_t  code = sdbWrite(pSdb, pRaw);
    if (code != 0) {
      mError("trans:%d, failed to write raw:%p to sdb since %s", pOldTrans->id, pRaw, terrstr());
      return code;
    }
  }

  pOldTrans->stage = pNewTrans->stage;
  return 0;
}

static int32_t trnGenerateTransId() { return 1; }

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
  pTrans->pMnode = pMnode;
  pTrans->rpcHandle = rpcHandle;
  pTrans->redoLogs = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->undoLogs = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->commitLogs = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->redoActions = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->undoActions = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));

  if (pTrans->redoLogs == NULL || pTrans->undoLogs == NULL || pTrans->commitLogs == NULL ||
      pTrans->redoActions == NULL || pTrans->undoActions == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to create transaction since %s", terrstr());
    return NULL;
  }

  mDebug("trans:%d, data:%p is created", pTrans->id, pTrans);
  return pTrans;
}

static void trnDropArray(SArray *pArray) {
  for (int32_t i = 0; i < pArray->size; ++i) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, i);
    tfree(pRaw);
  }

  taosArrayDestroy(pArray);
}

void mndTransDrop(STrans *pTrans) {
  trnDropArray(pTrans->redoLogs);
  trnDropArray(pTrans->undoLogs);
  trnDropArray(pTrans->commitLogs);
  trnDropArray(pTrans->redoActions);
  trnDropArray(pTrans->undoActions);

  mDebug("trans:%d, data:%p is dropped", pTrans->id, pTrans);
  tfree(pTrans);
}

void mndTransSetRpcHandle(STrans *pTrans, void *rpcHandle) {
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
  mTrace("trans:%d, raw:%p append to redo logs, code:%d", pTrans->id, pRaw, code);
  return code;
}

int32_t mndTransAppendUndolog(STrans *pTrans, SSdbRaw *pRaw) {
  int32_t code = mndTransAppendArray(pTrans->undoLogs, pRaw);
  mTrace("trans:%d, raw:%p append to undo logs, code:%d", pTrans->id, pRaw, code);
  return code;
}

int32_t mndTransAppendCommitlog(STrans *pTrans, SSdbRaw *pRaw) {
  int32_t code = mndTransAppendArray(pTrans->commitLogs, pRaw);
  mTrace("trans:%d, raw:%p append to commit logs, code:%d", pTrans->id, pRaw, code);
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

int32_t mndTransPrepare(STrans *pTrans) {
  mDebug("trans:%d, prepare transaction", pTrans->id);

  SSdbRaw *pRaw = mndTransActionEncode(pTrans);
  if (pRaw == NULL) {
    mError("trans:%d, failed to decode trans since %s", pTrans->id, terrstr());
    return -1;
  }
  sdbSetRawStatus(pRaw, SDB_STATUS_CREATING);

  if (sdbWriteNotFree(pTrans->pMnode->pSdb, pRaw) != 0) {
    mError("trans:%d, failed to write trans since %s", pTrans->id, terrstr());
    return -1;
  }

  STransMsg *pMsg = calloc(1, sizeof(STransMsg));
  pMsg->id = pTrans->id;
  pMsg->rpcHandle = pTrans->rpcHandle;

  mDebug("trans:%d, start sync, RPC:%p pMsg:%p", pTrans->id, pTrans->rpcHandle, pMsg);
  if (mndSyncPropose(pTrans->pMnode, pRaw, pMsg) != 0) {
    mError("trans:%d, failed to sync since %s", pTrans->id, terrstr());
    free(pMsg);
    sdbFreeRaw(pRaw);
    return -1;
  }

  sdbFreeRaw(pRaw);
  return 0;
}

static void trnSendRpcRsp(STransMsg *pMsg, int32_t code) {
  mDebug("trans:%d, send rpc rsp, RPC:%p code:0x%x pMsg:%p", pMsg->id, pMsg->rpcHandle, code & 0xFFFF, pMsg);
  if (pMsg->rpcHandle != NULL) {
    SRpcMsg rspMsg = {.handle = pMsg->rpcHandle, .code = code};
    rpcSendResponse(&rspMsg);
  }

  free(pMsg);
}

void mndTransApply(SMnode *pMnode, SSdbRaw *pRaw, STransMsg *pMsg, int32_t code) {
  if (code == 0) {
    mDebug("trans:%d, commit transaction", pMsg->id);
    sdbSetRawStatus(pRaw, SDB_STATUS_READY);
    if (sdbWrite(pMnode->pSdb, pRaw) != 0) {
      code = terrno;
      mError("trans:%d, failed to write sdb while commit since %s", pMsg->id, terrstr());
    }
    trnSendRpcRsp(pMsg, code);
  } else {
    mDebug("trans:%d, rollback transaction", pMsg->id);
    sdbSetRawStatus(pRaw, SDB_STATUS_DROPPED);
    if (sdbWrite(pMnode->pSdb, pRaw) != 0) {
      mError("trans:%d, failed to write sdb while rollback since %s", pMsg->id, terrstr());
    }
    trnSendRpcRsp(pMsg, code);
  }
}

static int32_t trnExecuteArray(SMnode *pMnode, SArray *pArray) {
  for (int32_t i = 0; i < pArray->size; ++i) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, i);
    if (sdbWrite(pMnode->pSdb, pRaw) != 0) {
      return -1;
    }
  }

  return 0;
}

static int32_t trnExecuteRedoLogs(STrans *pTrans) { return trnExecuteArray(pTrans->pMnode, pTrans->redoLogs); }

static int32_t trnExecuteUndoLogs(STrans *pTrans) { return trnExecuteArray(pTrans->pMnode, pTrans->undoLogs); }

static int32_t trnExecuteCommitLogs(STrans *pTrans) { return trnExecuteArray(pTrans->pMnode, pTrans->commitLogs); }

static int32_t trnExecuteRedoActions(STrans *pTrans) { return trnExecuteArray(pTrans->pMnode, pTrans->redoActions); }

static int32_t trnExecuteUndoActions(STrans *pTrans) { return trnExecuteArray(pTrans->pMnode, pTrans->undoActions); }

static int32_t trnPerformPrepareStage(STrans *pTrans) {
  if (trnExecuteRedoLogs(pTrans) == 0) {
    pTrans->stage = TRN_STAGE_EXECUTE;
    return 0;
  } else {
    pTrans->stage = TRN_STAGE_ROLLBACK;
    return -1;
  }
}

static int32_t trnPerformExecuteStage(STrans *pTrans) {
  int32_t code = trnExecuteRedoActions(pTrans);

  if (code == 0) {
    pTrans->stage = TRN_STAGE_COMMIT;
    return 0;
  } else if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    return -1;
  } else {
    if (pTrans->policy == TRN_POLICY_RETRY) {
      pTrans->stage = TRN_STAGE_RETRY;
    } else {
      pTrans->stage = TRN_STAGE_ROLLBACK;
    }
    return 0;
  }
}

static int32_t trnPerformCommitStage(STrans *pTrans) {
  if (trnExecuteCommitLogs(pTrans) == 0) {
    pTrans->stage = TRN_STAGE_EXECUTE;
    return 0;
  } else {
    pTrans->stage = TRN_STAGE_ROLLBACK;
    return -1;
  }
}

static int32_t trnPerformRollbackStage(STrans *pTrans) {
  if (trnExecuteCommitLogs(pTrans) == 0) {
    pTrans->stage = TRN_STAGE_EXECUTE;
    return 0;
  } else {
    pTrans->stage = TRN_STAGE_ROLLBACK;
    return -1;
  }
}

static int32_t trnPerformRetryStage(STrans *pTrans) {
  if (trnExecuteCommitLogs(pTrans) == 0) {
    pTrans->stage = TRN_STAGE_EXECUTE;
    return 0;
  } else {
    pTrans->stage = TRN_STAGE_ROLLBACK;
    return -1;
  }
}

int32_t mndTransExecute(SSdb *pSdb, int32_t tranId) {
  int32_t code = 0;

  STrans *pTrans = sdbAcquire(pSdb, SDB_TRANS, &tranId);
  if (pTrans == NULL) {
    return -1;
  }

  if (pTrans->stage == TRN_STAGE_PREPARE) {
    if (trnPerformPrepareStage(pTrans) != 0) {
      sdbRelease(pSdb, pTrans);
      return -1;
    }
  }

  if (pTrans->stage == TRN_STAGE_EXECUTE) {
    if (trnPerformExecuteStage(pTrans) != 0) {
      sdbRelease(pSdb, pTrans);
      return -1;
    }
  }

  if (pTrans->stage == TRN_STAGE_COMMIT) {
    if (trnPerformCommitStage(pTrans) != 0) {
      sdbRelease(pSdb, pTrans);
      return -1;
    }
  }

  if (pTrans->stage == TRN_STAGE_ROLLBACK) {
    if (trnPerformRollbackStage(pTrans) != 0) {
      sdbRelease(pSdb, pTrans);
      return -1;
    }
  }

  if (pTrans->stage == TRN_STAGE_RETRY) {
    if (trnPerformRetryStage(pTrans) != 0) {
      sdbRelease(pSdb, pTrans);
      return -1;
    }
  }

  sdbRelease(pSdb, pTrans);
  return 0;
}