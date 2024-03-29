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

#ifndef _DATA_SINK_INT_H
#define _DATA_SINK_INT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "dataSinkMgt.h"
#include "plannodes.h"
#include "storageapi.h"
#include "tcommon.h"

struct SDataSink;
struct SDataSinkHandle;

typedef struct SDataSinkManager {
  SDataSinkMgtCfg cfg;
  SStorageAPI*    pAPI;
} SDataSinkManager;

typedef int32_t (*FPutDataBlock)(struct SDataSinkHandle* pHandle, const SInputData* pInput, bool* pContinue);
typedef void (*FEndPut)(struct SDataSinkHandle* pHandle, uint64_t useconds);
typedef void (*FReset)(struct SDataSinkHandle* pHandle);
typedef void (*FGetDataLength)(struct SDataSinkHandle* pHandle, int64_t* pLen, bool* pQueryEnd);
typedef int32_t (*FGetDataBlock)(struct SDataSinkHandle* pHandle, SOutputData* pOutput);
typedef int32_t (*FDestroyDataSinker)(struct SDataSinkHandle* pHandle);
typedef int32_t (*FGetCacheSize)(struct SDataSinkHandle* pHandle, uint64_t* size);

typedef struct SDataSinkHandle {
  FPutDataBlock      fPut;
  FEndPut            fEndPut;
  FReset             fReset;
  FGetDataLength     fGetLen;
  FGetDataBlock      fGetData;
  FDestroyDataSinker fDestroy;
  FGetCacheSize      fGetCacheSize;
} SDataSinkHandle;

int32_t createDataDispatcher(SDataSinkManager* pManager, const SDataSinkNode* pDataSink, DataSinkHandle* pHandle);
int32_t createDataDeleter(SDataSinkManager* pManager, const SDataSinkNode* pDataSink, DataSinkHandle* pHandle,
                          void* pParam);
int32_t createDataInserter(SDataSinkManager* pManager, const SDataSinkNode* pDataSink, DataSinkHandle* pHandle,
                           void* pParam);

#ifdef __cplusplus
}
#endif

#endif /*_DATA_SINK_INT_H*/
