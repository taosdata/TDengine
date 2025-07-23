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

#ifndef _BSE_SNAPSHOT_H_
#define _BSE_SNAPSHOT_H_

#include "bseInc.h"
#include "bseUtil.h"

#ifdef __cplusplus
extern "C" {
#endif

enum { BSE_TABLE_SNAP = 0x1, BSE_TABLE_META_SNAP = 0x2, BSE_CURRENT_SNAP = 0x3, BSE_MAX_SNAP = 0x4 };

struct SBseSnapReader {
  SBse *pBse;
  void *pIter;
  void *pBuf;
};

struct SBseRawFileWriter {
  char      name[TSDB_FILENAME_LEN];
  int8_t    fileType;  // fileType
  int8_t    blockType;  // blockType
  SSeqRange range;
  TdFilePtr pFile;
  int32_t   keepDays;
  SBse     *pBse;
  int64_t   offset;
};

struct SBseSnapWriter {
  SBse     *pBse;
  SArray   *pFileSet;
  SSeqRange range;
  int8_t    fileType;  // fileType
  int64_t   ver;

  struct SBseRawFileWriter *pWriter;
};

typedef struct {
  struct SSeqRange range;
  SBse            *pBse;
  int32_t          index;
  int64_t          offset;
  SArray          *pFileSet;
  int8_t           fileType;  // BSE_TABLE_SNAP, BSE_CURRENT_SNAP
  int8_t           isOver;

  void *pTableIter;
  uint8_t *pCurrentBuf;

} SBseIter;

typedef struct {
  struct SSeqRange range;
  int8_t           fileType;   // fileType
  int8_t           blockType;  // blockType
  int64_t          startTimestamp;  // keepDays
} SBseSnapMeta;

int32_t bseOpenIter(SBse *pBse, SBseIter **ppIter);

int32_t bseIterNext(SBseIter *pIter, uint8_t **pBuf, int32_t *len);

void bseIterDestroy(SBseIter *pIter);

int8_t bseIterValid(SBseIter *pIter);

int8_t bseIterIsOver(SBseIter *pIter);

#ifdef __cplusplus
}
#endif

#endif