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

#ifndef _DATA_SINK_MGT_H
#define _DATA_SINK_MGT_H

#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

#include "executor.h"
#include "os.h"
#include "plannodes.h"
#include "thash.h"

#define DS_BUF_LOW   1
#define DS_BUF_FULL  2
#define DS_BUF_EMPTY 3

#define DS_FLAG_USE_MEMPOOL (1 << 0)
#define DS_FLAG_PROCESS_ONE_BLOCK (1 << 1)


struct SSDataBlock;

typedef struct SDeleterRes {
  uint64_t suid;
  SArray*  uidList;
  int64_t  skey;
  int64_t  ekey;
  int64_t  affectedRows;
  char     tableName[TSDB_TABLE_NAME_LEN];
  char     tsColName[TSDB_COL_NAME_LEN];
} SDeleterRes;

typedef struct SDeleterParam {
  uint64_t suid;
  SArray*  pUidList;
} SDeleterParam;

typedef enum {
  AUTO_CREATE_TABLE_UNKNOWN = 0,
  AUTO_CREATE_TABLE_STABLE,
  AUTO_CREATE_TABLE_STREAM_STABLE,
  AUTO_CREATE_TABLE_STREAM_NORMAL,
} AUTO_CREATE_TABLE_MODE;

typedef struct SInserterParam {
  SReadHandle*          readHandle;
  SStreamInserterParam* streamInserterParam;
} SInserterParam;

typedef struct SDataSinkStat {
  uint64_t cachedSize;
} SDataSinkStat;

typedef struct SDataSinkMgtCfg {
  int8_t   compress;
  uint32_t maxDataBlockNum;  // todo: this should be numOfRows?
  uint32_t maxDataBlockNumPerQuery;
} SDataSinkMgtCfg;

int32_t dsDataSinkMgtInit(SDataSinkMgtCfg* cfg, SStorageAPI* pAPI, void** ppSinkManager);

typedef struct SStreamDataInserterInfo {
  bool    isAutoCreateTable;
  int64_t streamId;
  int64_t groupId;
  char*   tbName;
  SArray* pTagVals;  // SArray<SStreamTagInfo>
} SStreamDataInserterInfo;

typedef struct SInputData {
  const struct SSDataBlock* pData;
  SStreamDataInserterInfo*  pStreamDataInserterInfo;
} SInputData;

typedef struct SOutputData {
  int32_t numOfBlocks;
  int64_t numOfRows; // int32_t changed to int64_t
  int32_t numOfCols;
  int8_t  compressed;
  char*   pData;
  bool    queryEnd;
  int32_t bufStatus;
  int64_t useconds;
  int8_t  precision;
} SOutputData;

/**
 * Create a subplan's datasinker handle for all later operations.
 * @param pDataSink
 * @param pHandle output
 * @return error code
 */
int32_t dsCreateDataSinker(void* pSinkManager, SDataSinkNode** ppDataSink, DataSinkHandle* pHandle, void* pParam, const char* id, bool processOneBlock);

int32_t dsDataSinkGetCacheSize(SDataSinkStat* pStat);

/**
 * Put the result set returned by the executor into datasinker.
 * @param handle
 * @param pRes
 * @return error code
 */
int32_t dsPutDataBlock(DataSinkHandle handle, const SInputData* pInput, bool* pContinue);

void dsEndPut(DataSinkHandle handle, uint64_t useconds);

void dsReset(DataSinkHandle handle);

/**
 * Get the length of the data returned by the next call to dsGetDataBlock.
 * @param handle
 * @param pLen data length
 */
void dsGetDataLength(DataSinkHandle handle, int64_t* pLen, int64_t* pRawLen, bool* pQueryEnd);

/**
 * Get data, the caller needs to allocate data memory.
 * @param handle
 * @param pOutput output
 * @param pStatus output
 * @return error code
 */
int32_t dsGetDataBlock(DataSinkHandle handle, SOutputData* pOutput);

int32_t dsGetCacheSize(DataSinkHandle handle, uint64_t* pSize);

/**
 * After dsGetStatus returns DS_NEED_SCHEDULE, the caller need to put this into the work queue.
 * @param ahandle
 * @param pItem
 */
void dsScheduleProcess(void* ahandle, void* pItem);

/**
 * Destroy the datasinker handle.
 * @param handle
 */
void dsDestroyDataSinker(DataSinkHandle handle);

int32_t dsGetSinkFlags(DataSinkHandle handle, uint64_t* pFlags);


#ifdef __cplusplus
}
#endif

#endif /*_DATA_SINK_MGT_H*/
