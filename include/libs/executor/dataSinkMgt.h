/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
  int8_t   secureDelete;
  // SArray<SDeleteInterval>, set only when a column filter drives the delete. In-process only:
  // never encoded onto the wire (mirrors SDeleteRes.pSplitPoints in tmsg.h, which this field is
  // copied into by qwGetDeleteResFromSink). Ownership: the producer (dataDeleter.c's
  // putRangeDeleteRes) transfers ownership into pIntervals and nulls its own copy of the pointer,
  // so every SDeleterRes instance must be zero-initialized before use -- an uninitialized
  // pIntervals here would otherwise be read/freed as a garbage pointer by whoever destroys it.
  SArray*  pIntervals;
} SDeleterRes;

typedef struct SDeleterParam {
  uint64_t suid;
  SArray*  pUidList;
  int8_t   secureDelete;
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
  // Byte budget for the ENCODED result data a single query may buffer in its sink.  Counting
  // queued blocks is not enough to bound the byte count when values are multi-megabyte, so the
  // sink also reports DS_BUF_FULL once it holds this many encoded bytes.  <= 0 disables the
  // byte test and keeps the legacy block-count-only behaviour.
  //
  // What is counted is the queued payload length (the buffer's `dataLen`, i.e. the COMPRESSED
  // length when compression is on), not the heap footprint: each queued entry is allocated for
  // the uncompressed (or LZ4-bound) size, so with compression enabled the process memory can
  // exceed this budget by the compression factor.  That is deliberate - the same counter backs
  // the qnode-load statistic `cacheDataSize` (dsDataSinkGetCacheSize -> qload.cacheDataSize),
  // which is meant to report cached encoded data, and compressed bytes are what actually has
  // to be held for the query.  Do not read this as a hard process-memory cap; the large-value
  // case it exists for (BLOB) does not compress, so the two coincide there.
  int64_t maxDataBytesPerQuery;
} SDataSinkMgtCfg;

// Decide the buffer status of a result sink from what it currently holds.  Both the number of
// queued blocks and the encoded byte count are considered: the byte limit is what bounds the
// accumulated payload for large values, while the block limit also covers many tiny blocks.
// Whichever is reached first applies.  blockNums == 0 is EMPTY so that an idle sink never looks
// FULL.  `cachedBytes` is the encoded payload the sink holds, not its heap footprint - see the
// note on maxDataBytesPerQuery.
static inline int32_t dsCalcBufStatus(const SDataSinkMgtCfg* pCfg, int32_t blockNums, uint64_t cachedBytes) {
  if (blockNums <= 0) {
    return DS_BUF_EMPTY;
  }

  if ((uint32_t)blockNums >= pCfg->maxDataBlockNumPerQuery) {
    return DS_BUF_FULL;
  }

  if (pCfg->maxDataBytesPerQuery > 0 && (int64_t)cachedBytes >= pCfg->maxDataBytesPerQuery) {
    return DS_BUF_FULL;
  }

  return DS_BUF_LOW;
}

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
  void*                     pTask;
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
