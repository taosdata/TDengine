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
#ifndef _TD_VNODE_BSE_H_
#define _TD_VNODE_BSE_H_

#include "os.h"
#include "tchecksum.h"
#include "tlog.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#define bseFatal(...) do { if (bseDebugFlag & DEBUG_FATAL) { taosPrintLog("BSE FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define bseError(...) do { if (bseDebugFlag & DEBUG_ERROR) { taosPrintLog("BSE ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define bseWarn(...)  do { if (bseDebugFlag & DEBUG_WARN)  { taosPrintLog("BSE WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define bseInfo(...)  do { if (bseDebugFlag & DEBUG_INFO)  { taosPrintLog("BSE ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define bseDebug(...) do { if (bseDebugFlag & DEBUG_DEBUG) { taosPrintLog("BSE ", DEBUG_DEBUG, bseDebugFlag, __VA_ARGS__); }}    while(0)
#define bseTrace(...) do { if (bseDebugFlag & DEBUG_TRACE) { taosPrintLog("BSE ", DEBUG_TRACE, bseDebugFlag, __VA_ARGS__); }}    while(0)

#define bseGTrace(param, ...) do { if (bseDebugFlag & DEBUG_TRACE) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseTrace(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGFatal(param, ...) do { if (bseDebugFlag & DEBUG_FATAL) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseFatal(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGError(param, ...) do { if (bseDebugFlag & DEBUG_ERROR) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseError(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGWarn(param, ...)  do { if (bseDebugFlag & DEBUG_WARN)  { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseWarn(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGInfo(param, ...)  do { if (bseDebugFlag & DEBUG_INFO)  { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseInfo(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGDebug(param, ...) do { if (bseDebugFlag & DEBUG_DEBUG) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseDebug(param ",QID:%s", __VA_ARGS__, buf);}}    while(0)

// clang-format on
typedef struct {
  uint64_t offset;
  int32_t  size;
  int32_t  vlen;
  int32_t  seq;
} SValueInfo;

typedef struct {
  uint64_t offset;
  uint64_t size;
} SBlkHandle;

typedef struct {
  uint8_t  head[4];
  uint32_t id;
  uint32_t len;
  uint8_t  data[0];
} SBlkData2;

typedef struct {
  uint8_t  bType;
  uint8_t  type;  // content data
  uint32_t crc;
  uint32_t id;
  uint32_t len;
  uint32_t cap;
  uint8_t  flushed;
  uint32_t dataNum;

  SBlkData2 *pData;
  // int8_t   type;   // content data

  // int8_t   bType;  // block type
  // int8_t   type;   // content data
  // uint32_t id;
  // uint32_t len;
  // uint32_t cap;
  // uint8_t *data;
} SBlkData;

typedef struct {
  uint8_t  type;
  uint32_t len;
  uint32_t num;
} SBlkHeader;

typedef struct {
  SBlkHandle metaHandle[1];
  SBlkHandle indexHandle[1];
} STableFooter;
typedef struct {
  char         name[TSDB_FILENAME_LEN];
  TdFilePtr    pDataFile;
  TdFilePtr    pIdxFile;
  SBlkData     data;
  SBlkData     bufBlk;
  STableFooter footer;
  SHashObj    *pCache;
  SArray      *pSeqToBlock;
  int32_t      blockId;
  uint64_t     initSeq;
  uint8_t      commited;
  uint8_t      fileOpened;
  int64_t      seq;
} STable;

typedef struct {
  char         name[TSDB_FILENAME_LEN];
  TdFilePtr    pDataFile;
  TdFilePtr    pIdxFile;
  SBlkData     data;
  SBlkData     bufBlk;
  STableFooter footer;
  SHashObj    *pCache;
  SArray      *pSeqToBlock;
  int32_t      blockId;
  uint64_t     initSeq;
  uint64_t     lastSeq;
} SReaderTable;

typedef struct {
  SReaderTable *pTable;
} SReaderTableWrapper;

typedef struct {
  int32_t vgId;
  int32_t fsyncPeriod;
  int32_t retentionPeriod;  // secs
  int32_t rollPeriod;       // secs
  int64_t retentionSize;
  int64_t segSize;
  int64_t committed;
  int32_t encryptAlgorithm;
  char    encryptKey[ENCRYPT_KEY_LEN + 1];
  int8_t  clearFiles;
} SBseCfg;

typedef struct {
  char    path[TSDB_FILENAME_LEN];
  int64_t ver;
  STable *pTable[2];
  uint8_t inUse;

  TdThreadMutex  mutex;
  TdThreadRwlock rwlock;
  uint64_t       seq;
  uint64_t       commitSeq;
  SHashObj      *pSeqOffsetCache;
  SBseCfg        cfg;
  SArray        *fileSet;
  SHashObj      *pTableCache;
} SBse;

typedef struct {
  int32_t  num;
  uint8_t *buf;
  int32_t  len;
  int32_t  cap;
  int64_t  seq;

  SBse     *pBse;
  SHashObj *pOffset;
  SArray   *pSeq;
} SBseBatch;

int32_t bseOpen(const char *path, SBseCfg *pCfg, SBse **pBse);
int32_t bseAppend(SBse *pBse, uint64_t *seq, uint8_t *value, int32_t len);
int32_t bseGet(SBse *pBse, uint64_t seq, uint8_t **pValue, int32_t *len);
int32_t bseCommit(SBse *pBse);
int32_t bseRollback(SBse *pBse, int64_t ver);
int32_t bseBeginSnapshot(SBse *pBse, int64_t ver);
int32_t bseEndSnapshot(SBse *pBse);
int32_t bseStopSnapshot(SBse *pBse);
void    bseClose(SBse *pBse);

int32_t bseAppendBatch(SBse *pBse, SBseBatch *pBatch);

int32_t bseBatchInit(SBse *pBse, SBseBatch **pBatch, int32_t nKey);
int32_t bseBatchPut(SBseBatch *pBatch, uint64_t *seq, uint8_t *value, int32_t len);
int32_t bseBatchGet(SBseBatch *pBatch, uint64_t seq, uint8_t **pValue, int32_t *len);
int32_t bseBatchDestroy(SBseBatch *pBatch);
int32_t bseBatchCommit(SBseBatch *pBatch);
int32_t bseBatchMayResize(SBseBatch *pBatch, int32_t alen);

#ifdef __cplusplus
}
#endif

#endif