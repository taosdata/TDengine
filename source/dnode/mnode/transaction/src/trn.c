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

#define TRN_DEFAULT_ARRAY_SIZE 8

int32_t trnInit() { return 0; }
void    trnCleanup();

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
    taosArrayDestroy(pTrans->redoLogs);
    taosArrayDestroy(pTrans->undoLogs);
    taosArrayDestroy(pTrans->commitLogs);
    taosArrayDestroy(pTrans->redoActions);
    taosArrayDestroy(pTrans->undoActions);
    free(pTrans);
    terrno = TSDB_CODE_MND_OUT_OF_MEMORY;
    return NULL;
  }

  return pTrans;
}

int32_t trnCommit(STrans *pTrans) { return 0; }
void    trnDrop(STrans *pTrans) {}

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
