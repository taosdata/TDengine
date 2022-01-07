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

#include "common.h"
#include "dataSinkMgt.h"

struct SDataSink;
struct SDataSinkHandle;

typedef int32_t (*FPutDataBlock)(struct SDataSinkHandle* pHandle, const SDataResult* pRes);
typedef int32_t (*FGetDataBlock)(struct SDataSinkHandle* pHandle, char* pData, int32_t* pLen);
typedef int32_t (*FDestroyDataSinker)(struct SDataSinkHandle* pHandle);

typedef struct SDataSinkHandle {
  FPutDataBlock fPut;
  FGetDataBlock fGet;
  FDestroyDataSinker fDestroy;
} SDataSinkHandle;

int32_t createDataDispatcher(const struct SDataSink* pDataSink, DataSinkHandle* pHandle);

#ifdef __cplusplus
}
#endif

#endif /*_DATA_SINK_INT_H*/
