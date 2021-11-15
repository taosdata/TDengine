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
#include "trnInt.h"

#define SDB_TRANS_VER 1

SSdbRaw *trnActionEncode(STrans *pTrans) {
  int32_t rawDataLen = 10 * sizeof(int32_t);
  int32_t redoLogNum = taosArrayGetSize(pTrans->redoLogs);
  int32_t undoLogNum = taosArrayGetSize(pTrans->undoLogs);
  int32_t commitLogNum = taosArrayGetSize(pTrans->commitLogs);
  int32_t redoActionNum = taosArrayGetSize(pTrans->redoActions);
  int32_t undoActionNum = taosArrayGetSize(pTrans->undoActions);

  for (int32_t index = 0; index < redoLogNum; ++index) {
    SSdbRaw *pRaw = taosArrayGet(pTrans->redoLogs, index);
    rawDataLen += sdbGetRawTotalSize(pRaw);
  }

  for (int32_t index = 0; index < undoLogNum; ++index) {
    SSdbRaw *pRaw = taosArrayGet(pTrans->undoLogs, index);
    rawDataLen += sdbGetRawTotalSize(pRaw);
  }

  for (int32_t index = 0; index < commitLogNum; ++index) {
    SSdbRaw *pRaw = taosArrayGet(pTrans->commitLogs, index);
    rawDataLen += sdbGetRawTotalSize(pRaw);
  }

  SSdbRaw *pRaw = sdbAllocRaw(SDB_TRANS, SDB_TRANS_VER, rawDataLen);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_INT32(pData, dataPos, pTrans->id)
  SDB_SET_INT8(pData, dataPos, pTrans->stage)
  SDB_SET_INT8(pData, dataPos, pTrans->policy)
  SDB_SET_INT32(pData, dataPos, redoLogNum)
  SDB_SET_INT32(pData, dataPos, undoLogNum)
  SDB_SET_INT32(pData, dataPos, commitLogNum)
  SDB_SET_INT32(pData, dataPos, redoActionNum)
  SDB_SET_INT32(pData, dataPos, undoActionNum)
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

STrans *trnActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != SDB_TRANS_VER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    return NULL;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(STrans));
  STrans  *pTrans = sdbGetRowObj(pRow);
  if (pTrans == NULL) return NULL;

  int32_t  redoLogNum = 0;
  int32_t  undoLogNum = 0;
  int32_t  commitLogNum = 0;
  int32_t  redoActionNum = 0;
  int32_t  undoActionNum = 0;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, pRow, dataPos, &pTrans->id)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pTrans->stage)
  SDB_GET_INT8(pRaw, pRow, dataPos, &pTrans->policy)
  SDB_GET_INT32(pRaw, pRow, dataPos, &redoLogNum)
  SDB_GET_INT32(pRaw, pRow, dataPos, &undoLogNum)
  SDB_GET_INT32(pRaw, pRow, dataPos, &commitLogNum)
  SDB_GET_INT32(pRaw, pRow, dataPos, &redoActionNum)
  SDB_GET_INT32(pRaw, pRow, dataPos, &undoActionNum)

  for (int32_t index = 0; index < redoLogNum; ++index) {
    int32_t dataLen = 0;
    SDB_GET_INT32(pRaw, pRow, dataPos, &dataLen)

    char *pData = malloc(dataLen);
    SDB_GET_BINARY(pRaw, pRow, dataPos, pData, dataLen);
    void *ret = taosArrayPush(pTrans->redoLogs, pData);
    if (ret == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      break;
    }
  }

  // if (code != 0) {
  //   trnDrop(pTrans);
  //   terrno = code;
  //   return NULL;
  // }

  return pTrans;
}

int32_t trnActionInsert(STrans *pTrans) {
  SArray *pArray = pTrans->redoLogs;
  int32_t arraySize = taosArrayGetSize(pArray);

  for (int32_t index = 0; index < arraySize; ++index) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, index);
    int32_t  code = sdbWrite(pRaw);
    if (code != 0) {
      return code;
    }
  }

  return 0;
}

int32_t trnActionDelete(STrans *pTrans) {
  SArray *pArray = pTrans->redoLogs;
  int32_t arraySize = taosArrayGetSize(pArray);

  for (int32_t index = 0; index < arraySize; ++index) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, index);
    int32_t  code = sdbWrite(pRaw);
    if (code != 0) {
      return code;
    }
  }

  return 0;
}

int32_t trnActionUpdate(STrans *pSrcTrans, STrans *pDstTrans) { return 0; }

int32_t trnGenerateTransId() { return 1; }

STrans *trnCreate(ETrnPolicy policy) {
  STrans *pTrans = calloc(1, sizeof(STrans));
  if (pTrans == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pTrans->id = trnGenerateTransId();
  pTrans->stage = TRN_STAGE_PREPARE;
  pTrans->policy = policy;
  pTrans->redoLogs = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->undoLogs = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->commitLogs = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->redoActions = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->undoActions = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));

  if (pTrans->redoLogs == NULL || pTrans->undoLogs == NULL || pTrans->commitLogs == NULL ||
      pTrans->redoActions == NULL || pTrans->undoActions == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  return pTrans;
}

static void trnDropArray(SArray *pArray) {
  for (int32_t index = 0; index < pArray->size; ++index) {
    SSdbRaw *pRaw = taosArrayGetP(pArray, index);
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
  tfree(pTrans);
}

void trnSetRpcHandle(STrans *pTrans, void *rpcHandle) { pTrans->rpcHandle = rpcHandle; }

static int32_t trnAppendArray(SArray *pArray, SSdbRaw *pRaw) {
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

int32_t trnAppendRedoLog(STrans *pTrans, SSdbRaw *pRaw) { return trnAppendArray(pTrans->redoLogs, pRaw); }

int32_t trnAppendUndoLog(STrans *pTrans, SSdbRaw *pRaw) { return trnAppendArray(pTrans->undoLogs, pRaw); }

int32_t trnAppendCommitLog(STrans *pTrans, SSdbRaw *pRaw) { return trnAppendArray(pTrans->commitLogs, pRaw); }

int32_t trnAppendRedoAction(STrans *pTrans, SEpSet *pEpSet, void *pMsg) {
  return trnAppendArray(pTrans->redoActions, pMsg);
}

int32_t trnAppendUndoAction(STrans *pTrans, SEpSet *pEpSet, void *pMsg) {
  return trnAppendArray(pTrans->undoActions, pMsg);
}

int32_t trnInit() {
  SSdbTable table = {.sdbType = SDB_TRANS,
                     .keyType = SDB_KEY_INT32,
                     .encodeFp = (SdbEncodeFp)trnActionEncode,
                     .decodeFp = (SdbDecodeFp)trnActionDecode,
                     .insertFp = (SdbInsertFp)trnActionInsert,
                     .updateFp = (SdbUpdateFp)trnActionUpdate,
                     .deleteFp = (SdbDeleteFp)trnActionDelete};
  sdbSetTable(table);

  return 0;
}

void trnCleanup() {}
