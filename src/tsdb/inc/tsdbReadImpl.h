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

#ifndef _TD_TSDB_READ_IMPL_H_
#define _TD_TSDB_READ_IMPL_H_

#include "taosdef.h"
#include "tdataformat.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SReadH SReadH;

typedef struct {
  int32_t  tid;
  uint32_t len;
  uint32_t offset;
  uint32_t hasLast : 2;
  uint32_t numOfBlocks : 30;
  uint64_t uid;
  TSKEY    maxKey;
} SBlockIdx;

typedef struct {
  int64_t last : 1;
  int64_t offset : 63;
  int32_t algorithm : 8;
  int32_t numOfRows : 24;
  int32_t len;
  int32_t keyLen;     // key column length, keyOffset = offset+sizeof(SBlockData)+sizeof(SBlockCol)*numOfCols
  int16_t numOfSubBlocks;
  int16_t numOfCols; // not including timestamp column
  TSKEY   keyFirst;
  TSKEY   keyLast;
} SBlock;

typedef struct {
  int32_t    delimiter;  // For recovery usage
  int32_t    tid;
  uint64_t   uid;
  SBlock blocks[];
} SBlockInfo;

typedef struct {
  int16_t colId;
  int32_t len;
  int32_t type : 8;
  int32_t offset : 24;
  int64_t sum;
  int64_t max;
  int64_t min;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNull;
  char    padding[2];
} SBlockCol;

typedef struct {
  int32_t  delimiter;  // For recovery usage
  int32_t  numOfCols;  // For recovery usage
  uint64_t uid;        // For recovery usage
  SBlockCol cols[];
} SBlockData;

struct SReadH {
  STsdbRepo * pRepo;
  SDFileSet * pSet;
  SArray *    aBlkIdx;
  int         cidx;
  STable *    pTable;
  SBlockIdx * pBlockIdx;
  SBlockInfo *pBlkInfo;
  SBlockData *pBlkData;
  SDataCols * pDCols[2];
  void *      pBuf;
  void *      pCBuf;
};

#define TSDB_READ_REPO(rh) (rh)->pRepo
#define TSDB_READ_FSET(rh) (rh)->pSet
#define TSDB_READ_BUF(rh) (rh)->pBuf
#define TSDB_READ_COMP_BUF(rh) (rh)->pCBuf
#define TSDB_READ_FSET_IS_SET(rh) ((rh)->pSet != NULL)

#ifdef __cplusplus
}
#endif

#endif /*_TD_TSDB_READ_IMPL_H_*/