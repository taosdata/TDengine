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

#include "dataSinkMgt.h"
#include "dataSinkInt.h"
#include "planner.h"
#include "tarray.h"

static SDataSinkManager gDataSinkManager = {0};
SDataSinkStat           gDataSinkStat = {0};

int32_t dsDataSinkMgtInit(SDataSinkMgtCfg* cfg, SStorageAPI* pAPI) {
  gDataSinkManager.cfg = *cfg;
  gDataSinkManager.pAPI = pAPI;
  return 0;  // to avoid compiler eror
}

int32_t dsDataSinkGetCacheSize(SDataSinkStat* pStat) {
  pStat->cachedSize = atomic_load_64(&gDataSinkStat.cachedSize);

  return 0;
}

int32_t dsCreateDataSinker(const SDataSinkNode* pDataSink, DataSinkHandle* pHandle, void* pParam, const char* id) {
  switch ((int)nodeType(pDataSink)) {
    case QUERY_NODE_PHYSICAL_PLAN_DISPATCH:
      return createDataDispatcher(&gDataSinkManager, pDataSink, pHandle);
    case QUERY_NODE_PHYSICAL_PLAN_DELETE: {
      return createDataDeleter(&gDataSinkManager, pDataSink, pHandle, pParam);
    }
    case QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT: {
      return createDataInserter(&gDataSinkManager, pDataSink, pHandle, pParam);
    }
  }

  qError("invalid input node type:%d, %s", nodeType(pDataSink), id);
  return TSDB_CODE_QRY_INVALID_INPUT;
}

int32_t dsPutDataBlock(DataSinkHandle handle, const SInputData* pInput, bool* pContinue) {
  SDataSinkHandle* pHandleImpl = (SDataSinkHandle*)handle;
  return pHandleImpl->fPut(pHandleImpl, pInput, pContinue);
}

void dsEndPut(DataSinkHandle handle, uint64_t useconds) {
  SDataSinkHandle* pHandleImpl = (SDataSinkHandle*)handle;
  return pHandleImpl->fEndPut(pHandleImpl, useconds);
}

void dsGetDataLength(DataSinkHandle handle, int64_t* pLen, bool* pQueryEnd) {
  SDataSinkHandle* pHandleImpl = (SDataSinkHandle*)handle;
  pHandleImpl->fGetLen(pHandleImpl, pLen, pQueryEnd);
}

int32_t dsGetDataBlock(DataSinkHandle handle, SOutputData* pOutput) {
  SDataSinkHandle* pHandleImpl = (SDataSinkHandle*)handle;
  return pHandleImpl->fGetData(pHandleImpl, pOutput);
}

int32_t dsGetCacheSize(DataSinkHandle handle, uint64_t* pSize) {
  SDataSinkHandle* pHandleImpl = (SDataSinkHandle*)handle;
  return pHandleImpl->fGetCacheSize(pHandleImpl, pSize);
}

void dsScheduleProcess(void* ahandle, void* pItem) {
  // todo
}

void dsDestroyDataSinker(DataSinkHandle handle) {
  SDataSinkHandle* pHandleImpl = (SDataSinkHandle*)handle;
  pHandleImpl->fDestroy(pHandleImpl);
  taosMemoryFree(pHandleImpl);
}
