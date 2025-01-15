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

typedef struct {
  uint64_t offset;
  int32_t  size;
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
  int32_t      blockId;
} STable;
typedef struct {
  char    path[TSDB_FILENAME_LEN];
  int64_t ver;
  STable *pTable;
  // SHashObj     *pTableCache;
  TdThreadMutex mutex;
  uint64_t      seq;
  uint64_t      commitSeq;
  SHashObj     *pSeqOffsetCache;
} SBse;

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

int32_t bseOpen(const char *path, SBseCfg *pCfg, SBse **pBse);
int32_t bseAppend(SBse *pBse, uint64_t *seq, uint8_t *value, int32_t len);
int32_t bseGet(SBse *pBse, uint64_t seq, uint8_t **pValue, int32_t *len);
int32_t bseCommit(SBse *pBse);
int32_t bseRollback(SBse *pBse, int64_t ver);
int32_t bseBeginSnapshot(SBse *pBse, int64_t ver);
int32_t bseEndSnapshot(SBse *pBse);
int32_t bseStopSnapshot(SBse *pBse);

#ifdef __cplusplus
}
#endif

#endif