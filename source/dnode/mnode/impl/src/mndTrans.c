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
#include "trpc.h"

#define SDB_TRANS_VER 1
#define TRN_DEFAULT_ARRAY_SIZE 8

SSdbRaw *trnActionEncode(STrans *pTrans) {
  int32_t rawDataLen = 10 * sizeof(int32_t);
  int32_t redoLogNum = taosArrayGetSize(pTrans->redoLogs);
  int32_t undoLogNum = taosArrayGetSize(pTrans->undoLogs);
  int32_t commitLogNum = taosArrayGetSize(pTrans->commitLogs);
  int32_t redoActionNum = taosArrayGetSize(pTrans->redoActions);
  int32_t undoActionNum = taosArrayGetSize(pTrans->undoActions);

  for (int32_t index = 0; index < redoLogNum; ++index) {
    SSdbRaw *pTmp = taosArrayGet(pTrans->redoLogs, index);
    rawDataLen += sdbGetRawTotalSize(pTmp);
  }

  for (int32_t index = 0; index < undoLogNum; ++index) {
    SSdbRaw *pTmp = taosArrayGet(pTrans->undoLogs, index);
    rawDataLen += sdbGetRawTotalSize(pTmp);
  }

  for (int32_t index = 0; index < commitLogNum; ++index) {
    SSdbRaw *pTmp = taosArrayGet(pTrans->commitLogs, index);
    rawDataLen += sdbGetRawTotalSize(pTmp);
  }

  SSdbRaw *pRaw = sdbAllocRaw(SDB_TRANS, SDB_TRANS_VER, rawDataLen);
  if (pRaw == NULL) {
    mError("trn:%d, failed to alloc raw since %s", pTrans->id, terrstr());
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

  for (int32_t index = 0; index < redoLogNum; ++index) {
    SSdbRaw *pTmp = taosArrayGet(pTrans->redoLogs, index);
    int32_t  len = sdbGetRawTotalSize(pTmp);
    SDB_SET_INT32(pRaw, dataPos, len)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pTmp, len)
  }

  for (int32_t index = 0; index < undoLogNum; ++index) {
    SSdbRaw *pTmp = taosArrayGet(pTrans->undoLogs, index);
    int32_t  len = sdbGetRawTotalSize(pTmp);
    SDB_SET_INT32(pRaw, dataPos, len)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pTmp, len)
  }

  for (int32_t index = 0; index < commitLogNum; ++index) {
    SSdbRaw *pTmp = taosArrayGet(pTrans->commitLogs, index);
    int32_t  len = sdbGetRawTotalSize(pTmp);
    SDB_SET_INT32(pRaw, dataPos, len)
    SDB_SET_BINARY(pRaw, dataPos, (void *)pTmp, len)
  }

  mDebug("trn:%d, is encoded as raw:%p, len:%d", pTrans->id, pRaw, dataPos);
  return pRaw;
}

SSdbRow *trnActionDecode(SSdbRaw *pRaw) {
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

  SSdbRow   *pRow = sdbAllocRow(sizeof(STrans));
  STrans *pTrans = sdbGetRowObj(pRow);
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
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mDebug("trn:%d, failed to create array while parsed from raw:%p", pTrans->id, pRaw);
    return NULL;
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

  int32_t code = 0;
  for (int32_t index = 0; index < redoLogNum; ++index) {
    int32_t dataLen = 0;
    SDB_GET_INT32(pRaw, pRow, dataPos, &dataLen)

    char *pData = malloc(dataLen);
    SDB_GET_BINARY(pRaw, pRow, dataPos, pData, dataLen);
    void *ret = taosArrayPush(pTrans->redoLogs, pData);
    if (ret == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      break;
    }
  }

  if (code != 0) {
    terrno = code;
    mError("trn:%d, failed to parse from raw:%p since %s", pTrans->id, pRaw, terrstr());
    trnDrop(pTrans);
    return NULL;
  }

  mDebug("trn:%d, is parsed from raw:%p", pTrans->id, pRaw);
  return pRow;
}

static int32_t trnActionInsert(STrans *pTrans) {
  SArray *pArray = pTrans->redoLogs;
  int32_t arraySize = taosArrayGetSize(pArray);

  for (int32_t index = 0; index < arraySize; ++index) {
    SSdbRaw *pRaw = taosArrayGet(pArray, index);
    int32_t  code = sdbWrite(pRaw);
    if (code != 0) {
      mError("trn:%d, failed to write raw:%p to sdb since %s", pTrans->id, pRaw, terrstr());
      return code;
    }
  }

  mDebug("trn:%d, write to sdb", pTrans->id);
  return 0;
}

static int32_t trnActionDelete(STrans *pTrans) {
  SArray *pArray = pTrans->redoLogs;
  int32_t arraySize = taosArrayGetSize(pArray);

  for (int32_t index = 0; index < arraySize; ++index) {
    SSdbRaw *pRaw = taosArrayGet(pArray, index);
    int32_t  code = sdbWrite(pRaw);
    if (code != 0) {
      mError("trn:%d, failed to write raw:%p to sdb since %s", pTrans->id, pRaw, terrstr());
      return code;
    }
  }

  mDebug("trn:%d, delete from sdb", pTrans->id);
  return 0;
}

static int32_t trnActionUpdate(STrans *pTrans, STrans *pDstTrans) {
  assert(true);
  SArray *pArray = pTrans->redoLogs;
  int32_t arraySize = taosArrayGetSize(pArray);

  for (int32_t index = 0; index < arraySize; ++index) {
    SSdbRaw *pRaw = taosArrayGet(pArray, index);
    int32_t  code = sdbWrite(pRaw);
    if (code != 0) {
      mError("trn:%d, failed to write raw:%p to sdb since %s", pTrans->id, pRaw, terrstr());
      return code;
    }
  }

  pTrans->stage = pDstTrans->stage;
  mDebug("trn:%d, update in sdb", pTrans->id);
  return 0;
}

static int32_t trnGenerateTransId() { return 1; }

STrans *trnCreate(ETrnPolicy policy, void *rpcHandle) {
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

  mDebug("trn:%d, is created, %p", pTrans->id, pTrans);
  return pTrans;
}

static void trnDropArray(SArray *pArray) {
  for (int32_t index = 0; index < pArray->size; ++index) {
    SSdbRaw *pRaw = taosArrayGet(pArray, index);
    tfree(pRaw);
  }

  taosArrayDestroy(pArray);
}

void trnDrop(STrans *pTrans) {
  trnDropArray(pTrans->redoLogs);
  trnDropArray(pTrans->undoLogs);
  trnDropArray(pTrans->commitLogs);
  trnDropArray(pTrans->redoActions);
  trnDropArray(pTrans->undoActions);

  mDebug("trn:%d, is dropped, %p", pTrans->id, pTrans);
  tfree(pTrans);
}

void trnSetRpcHandle(STrans *pTrans, void *rpcHandle) {
  pTrans->rpcHandle = rpcHandle;
  mTrace("trn:%d, set rpc handle:%p", pTrans->id, rpcHandle);
}

static int32_t trnAppendArray(SArray *pArray, SSdbRaw *pRaw) {
  if (pArray == NULL || pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  void *ptr = taosArrayPush(pArray, pRaw);
  if (ptr == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

int32_t trnAppendRedoLog(STrans *pTrans, SSdbRaw *pRaw) {
  int32_t code = trnAppendArray(pTrans->redoLogs, pRaw);
  mTrace("trn:%d, raw:%p append to redo logs, code:%d", pTrans->id, pRaw, code);
  return code;
}

int32_t trnAppendUndoLog(STrans *pTrans, SSdbRaw *pRaw) {
  int32_t code = trnAppendArray(pTrans->undoLogs, pRaw);
  mTrace("trn:%d, raw:%p append to undo logs, code:%d", pTrans->id, pRaw, code);
  return code;
}

int32_t trnAppendCommitLog(STrans *pTrans, SSdbRaw *pRaw) {
  int32_t code = trnAppendArray(pTrans->commitLogs, pRaw);
  mTrace("trn:%d, raw:%p append to commit logs, code:%d", pTrans->id, pRaw, code);
  return code;
}

int32_t trnAppendRedoAction(STrans *pTrans, SEpSet *pEpSet, void *pMsg) {
  int32_t code = trnAppendArray(pTrans->redoActions, pMsg);
  mTrace("trn:%d, msg:%p append to redo actions", pTrans->id, pMsg);
  return code;
}

int32_t trnAppendUndoAction(STrans *pTrans, SEpSet *pEpSet, void *pMsg) {
  int32_t code = trnAppendArray(pTrans->undoActions, pMsg);
  mTrace("trn:%d, msg:%p append to undo actions", pTrans->id, pMsg);
  return code;
}

int32_t mndInitTrans(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_TRANS,
                     .keyType = SDB_KEY_INT32,
                     .encodeFp = (SdbEncodeFp)trnActionEncode,
                     .decodeFp = (SdbDecodeFp)trnActionDecode,
                     .insertFp = (SdbInsertFp)trnActionInsert,
                     .updateFp = (SdbUpdateFp)trnActionUpdate,
                     .deleteFp = (SdbDeleteFp)trnActionDelete};
  sdbSetTable(pMnode->pSdb, table);

  mInfo("trn module is initialized");
  return 0;
}

void mndCleanupTrans(SMnode *pMnode) { mInfo("trn module is cleaned up"); }


int32_t trnPrepare(STrans *pTrans, int32_t (*syncfp)(SSdbRaw *pRaw, void *pData)) {
  if (syncfp == NULL) return -1;

  SSdbRaw *pRaw = trnActionEncode(pTrans);
  if (pRaw == NULL) {
    mError("trn:%d, failed to decode trans since %s", pTrans->id, terrstr());
    return -1;
  }
  sdbSetRawStatus(pRaw, SDB_STATUS_CREATING);

  if (sdbWrite(pRaw) != 0) {
    mError("trn:%d, failed to write trans since %s", pTrans->id, terrstr());
    return -1;
  }

  if ((*syncfp)(pRaw, pTrans->rpcHandle) != 0) {
    mError("trn:%d, failed to sync trans since %s", pTrans->id, terrstr());
    return -1;
  }

  return 0;
}

static void trnSendRpcRsp(void *rpcHandle, int32_t code) {
  if (rpcHandle != NULL) {
    SRpcMsg rspMsg = {.handle = rpcHandle, .code = terrno};
    rpcSendResponse(&rspMsg);
  }
}

int32_t trnApply(SSdbRaw *pRaw, void *pData, int32_t code) {
  if (code != 0) {
    trnSendRpcRsp(pData, terrno);
    return 0;
  }

  if (sdbWrite(pData) != 0) {
    code = terrno;
    trnSendRpcRsp(pData, code);
    terrno = code;
    return -1;
  }

  return 0;
}

static int32_t trnExecuteArray(SArray *pArray) {
  for (int32_t index = 0; index < pArray->size; ++index) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, index);
    if (sdbWrite(pRaw) != 0) {
      return -1;
    }
  }

  return 0;
}

static int32_t trnExecuteRedoLogs(STrans *pTrans) { return trnExecuteArray(pTrans->redoLogs); }

static int32_t trnExecuteUndoLogs(STrans *pTrans) { return trnExecuteArray(pTrans->undoLogs); }

static int32_t trnExecuteCommitLogs(STrans *pTrans) { return trnExecuteArray(pTrans->commitLogs); }

static int32_t trnExecuteRedoActions(STrans *pTrans) { return trnExecuteArray(pTrans->redoActions); }

static int32_t trnExecuteUndoActions(STrans *pTrans) { return trnExecuteArray(pTrans->undoActions); }

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

int32_t trnExecute(int32_t tranId) {
  int32_t code = 0;

  STrans *pTrans = sdbAcquire(SDB_TRANS, &tranId);
  if (pTrans == NULL) {
    return -1;
  }

  if (pTrans->stage == TRN_STAGE_PREPARE) {
    if (trnPerformPrepareStage(pTrans) != 0) {
      sdbRelease(pTrans);
      return -1;
    }
  }

  if (pTrans->stage == TRN_STAGE_EXECUTE) {
    if (trnPerformExecuteStage(pTrans) != 0) {
      sdbRelease(pTrans);
      return -1;
    }
  }

  if (pTrans->stage == TRN_STAGE_COMMIT) {
    if (trnPerformCommitStage(pTrans) != 0) {
      sdbRelease(pTrans);
      return -1;
    }
  }

  if (pTrans->stage == TRN_STAGE_ROLLBACK) {
    if (trnPerformRollbackStage(pTrans) != 0) {
      sdbRelease(pTrans);
      return -1;
    }
  }

  if (pTrans->stage == TRN_STAGE_RETRY) {
    if (trnPerformRetryStage(pTrans) != 0) {
      sdbRelease(pTrans);
      return -1;
    }
  }

  sdbRelease(pTrans);
  return 0;
}