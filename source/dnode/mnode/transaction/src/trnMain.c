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

#define TRN_VER 1
#define TRN_DEFAULT_ARRAY_SIZE 8

STrans *trnCreate() {
  STrans *pTrans = calloc(1, sizeof(STrans));
  if (pTrans == NULL) {
    terrno = TSDB_CODE_MND_OUT_OF_MEMORY;
    return NULL;
  }

  pTrans->redoLogs = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->undoLogs = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->commitLogs = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->redoActions = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));
  pTrans->undoActions = taosArrayInit(TRN_DEFAULT_ARRAY_SIZE, sizeof(void *));

  if (pTrans->redoLogs == NULL || pTrans->undoLogs == NULL || pTrans->commitLogs == NULL ||
      pTrans->redoActions == NULL || pTrans->undoActions == NULL) {
    trnDrop(pTrans);
    terrno = TSDB_CODE_MND_OUT_OF_MEMORY;
    return NULL;
  }

  return pTrans;
}

int32_t trnCommit(STrans *pTrans) { return 0; }

static void trnDropLogs(SArray *pArray) {
  for (int32_t index = 0; index < pArray->size; ++index) {
    SSdbRawData *pRaw = taosArrayGetP(pArray, index);
    free(pRaw);
  }

  taosArrayDestroy(pArray);
}

void trnDrop(STrans *pTrans) {
  trnDropLogs(pTrans->redoLogs);
  trnDropLogs(pTrans->undoLogs);
  trnDropLogs(pTrans->commitLogs);
  free(pTrans);
}

int32_t trnAppendRedoLog(STrans *pTrans, SSdbRawData *pRaw) {
  void *ptr = taosArrayPush(pTrans->redoLogs, &pRaw);
  if (ptr == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }
  return 0;
}

int32_t trnAppendUndoLog(STrans *pTrans, SSdbRawData *pRaw) {
  void *ptr = taosArrayPush(pTrans->undoLogs, &pRaw);
  if (ptr == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }
  return 0;
}

int32_t trnAppendCommitLog(STrans *pTrans, SSdbRawData *pRaw) {
  void *ptr = taosArrayPush(pTrans->commitLogs, &pRaw);
  if (ptr == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }
  return 0;
}

int32_t trnAppendRedoAction(STrans *pTrans, SEpSet *pEpSet, void *pMsg) {
  void *ptr = taosArrayPush(pTrans->redoActions, &pMsg);
  if (ptr == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }
  return 0;
}

int32_t trnAppendUndoAction(STrans *pTrans, SEpSet *pEpSet, void *pMsg) {
  void *ptr = taosArrayPush(pTrans->undoActions, &pMsg);
  if (ptr == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }
  return 0;
}

static SSdbRawData *trnActionEncode(STrans *pTrans) {
  int32_t rawDataLen = 5 * sizeof(int32_t);
  int32_t redoLogNum = taosArrayGetSize(pTrans->redoLogs);
  int32_t undoLogNum = taosArrayGetSize(pTrans->undoLogs);
  int32_t commitLogNum = taosArrayGetSize(pTrans->commitLogs);
  int32_t redoActionNum = taosArrayGetSize(pTrans->redoActions);
  int32_t undoActionNum = taosArrayGetSize(pTrans->undoActions);

  for (int32_t index = 0; index < redoLogNum; ++index) {
    SSdbRawData *pRawData = taosArrayGet(pTrans->redoLogs, index);
    rawDataLen += (sizeof(SSdbRawData) + pRawData->dataLen);
  }

  for (int32_t index = 0; index < undoLogNum; ++index) {
    SSdbRawData *pRawData = taosArrayGet(pTrans->undoLogs, index);
    rawDataLen += (sizeof(SSdbRawData) + pRawData->dataLen);
  }

  for (int32_t index = 0; index < commitLogNum; ++index) {
    SSdbRawData *pRawData = taosArrayGet(pTrans->commitLogs, index);
    rawDataLen += (sizeof(SSdbRawData) + pRawData->dataLen);
  }

  SSdbRawData *pRaw = calloc(1, rawDataLen + sizeof(SSdbRawData));
  if (pRaw == NULL) {
    terrno = TSDB_CODE_MND_OUT_OF_MEMORY;
    return NULL;
  }

  int32_t dataLen = 0;
  char   *pData = pRaw->data;
  SDB_SET_INT32_VAL(pData, dataLen, redoLogNum)
  SDB_SET_INT32_VAL(pData, dataLen, undoLogNum)
  SDB_SET_INT32_VAL(pData, dataLen, commitLogNum)
  SDB_SET_INT32_VAL(pData, dataLen, redoActionNum)
  SDB_SET_INT32_VAL(pData, dataLen, undoActionNum)

  pRaw->dataLen = dataLen;
  pRaw->type = SDB_TRANS;
  pRaw->sver = TRN_VER;
  return pRaw;
}

static STrans *trnActionDecode(SSdbRawData *pRaw) {
  if (pRaw->sver != TRN_VER) {
    terrno = TSDB_CODE_SDB_INVAID_RAW_DATA_VER;
    return NULL;
  }

  STrans *pTrans = trnCreate();
  if (pTrans == NULL) {
    terrno = TSDB_CODE_MND_OUT_OF_MEMORY;
    return NULL;
  }

  int32_t redoLogNum = 0;
  int32_t undoLogNum = 0;
  int32_t commitLogNum = 0;
  int32_t redoActionNum = 0;
  int32_t undoActionNum = 0;
  SSdbRawData *pTmp = malloc(sizeof(SSdbRawData));

  int32_t code = 0;
  int32_t dataLen = pRaw->dataLen;
  char   *pData = pRaw->data;
  SDB_GET_INT32_VAL(pData, dataLen, redoLogNum, code)
  SDB_GET_INT32_VAL(pData, dataLen, undoLogNum, code)
  SDB_GET_INT32_VAL(pData, dataLen, commitLogNum, code)
  SDB_GET_INT32_VAL(pData, dataLen, redoActionNum, code)
  SDB_GET_INT32_VAL(pData, dataLen, undoActionNum, code)

  for (int32_t index = 0; index < redoLogNum; ++index) {
    SDB_GET_BINARY_VAL(pData, dataLen, pTmp, sizeof(SSdbRawData), code);
    if (code == 0 && pTmp->dataLen > 0) {
      SSdbRawData *pRead = malloc(sizeof(SSdbRawData) + pTmp->dataLen);
      if (pRead == NULL) {
        code = TSDB_CODE_MND_OUT_OF_MEMORY;
        break;
      }
      memcpy(pRead, pTmp, sizeof(SSdbRawData));
      SDB_GET_BINARY_VAL(pData, dataLen, pRead->data, pRead->dataLen, code);
      void *ret = taosArrayPush(pTrans->redoLogs, &pRead);
      if (ret == NULL) {
        code = TSDB_CODE_MND_OUT_OF_MEMORY;
        break;
      }
    }
  }

  if (code != 0) {
    trnDrop(pTrans);
    terrno = code;
    return NULL;
  }

  return pTrans;
}

static int32_t trnActionInsert(STrans *pTrans) {
  SArray *pArray = pTrans->redoLogs;
  int32_t arraySize = taosArrayGetSize(pArray);

  for (int32_t index = 0; index < arraySize; ++index) {
    SSdbRawData *pRaw = taosArrayGetP(pArray, index);
    int32_t      code = sdbWrite(pRaw);
    if (code != 0) {
      return code;
    }
  }

  return 0;
}

static int32_t trnActionDelete(STrans *pTrans) {
  SArray *pArray = pTrans->redoLogs;
  int32_t arraySize = taosArrayGetSize(pArray);

  for (int32_t index = 0; index < arraySize; ++index) {
    SSdbRawData *pRaw = taosArrayGetP(pArray, index);
    int32_t      code = sdbWrite(pRaw);
    if (code != 0) {
      return code;
    }
  }

  return 0;
}

static int32_t trnActionUpdate(STrans *pSrcUser, STrans *pDstUser) {
  return 0;
}
