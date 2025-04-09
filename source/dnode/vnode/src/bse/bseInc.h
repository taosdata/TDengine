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
#ifndef _BSE_INC_H_
#define _BSE_INC_H_
#include "bse.h"
#include "bseUtil.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  SSeqRange range;
  SBse     *pBse;
  int32_t   index;
  int64_t   offset;
  SArray   *pFileSet;
  int8_t    fileType;  // BSE_TABLE_SNAP, BSE_CURRENT_SNAP
  int8_t    isOver;

  void *pTableIter;
} SBseIter;

typedef struct {
  SSeqRange range;
  int8_t    fileType;   // fileType
  int8_t    blockType;  // blockType
  int64_t   keepDays;   // keepDays
} SBseSnapMeta;

int32_t bseOpenIter(SBse *pBse, SBseIter **ppIter);

int32_t bseIterNext(SBseIter *pIter, uint8_t **pBuf, int32_t *len);

void bseIterDestroy(SBseIter *pIter);

int8_t bseIterValid(SBseIter *pIter);

enum { BSE_TABLE_SNAP = 0x1, BSE_CURRENT_SNAP = 0x2, BSE_MAX_SNAP = 0x4 };

typedef struct {
  SArray   *pFileSet;
  SSeqRange range;
  int8_t    fileType;  // fileType
  int64_t   ver;
} SBseSnapWriterWrapper;

int32_t bseBuilderLoad(SBse *pBse, SBseSnapMeta *pMeta, uint8_t *data, int32_t len);

#ifdef __cplusplus
}
#endif
#endif
