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
  TdFilePtr fd;
  int64_t   offset;
  SArray   *pFileSet;
  int8_t    isOver;
} SBseIter;

int32_t bseOpenIter(SBse *pBse, SBseIter **ppIter);

int32_t bseIterNext(SBseIter *pIter, SBseBatch **ppBatch);

void bseIterDestroy(SBseIter *pIter);

#ifdef __cplusplus
}
#endif
#endif
