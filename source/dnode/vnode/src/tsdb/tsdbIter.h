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

#include "trbtree.h"
#include "tsdbDataFileRW.h"
#include "tsdbDef.h"
#include "tsdbSttFileRW.h"

#ifndef _TSDB_ITER_H_
#define _TSDB_ITER_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SIterMerger SIterMerger;
typedef struct STsdbIter   STsdbIter;
typedef TARRAY2(STsdbIter *) TTsdbIterArray;

typedef enum {
  TSDB_ITER_TYPE_STT = 1,
  TSDB_ITER_TYPE_DATA,
  TSDB_ITER_TYPE_MEMT,
  TSDB_ITER_TYPE_STT_TOMB,
  TSDB_ITER_TYPE_DATA_TOMB,
  TSDB_ITER_TYPE_MEMT_TOMB,
} EIterType;

typedef struct {
  EIterType type;
  union {
    SSttFileReader  *sttReader;   //  TSDB_ITER_TYPE_STT || TSDB_ITER_TYPE_STT_TOMB
    SDataFileReader *dataReader;  // TSDB_ITER_TYPE_DATA || TSDB_ITER_TYPE_DATA_TOMB
    struct {
      SMemTable *memt;  // TSDB_ITER_TYPE_MEMT_TOMB
      TSDBKEY    from[1];
    };  // TSDB_ITER_TYPE_MEMT
  };
  bool    filterByVersion;
  int64_t verRange[2];
} STsdbIterConfig;

// STsdbIter ===============
int32_t tsdbIterOpen(const STsdbIterConfig *config, STsdbIter **iter);
int32_t tsdbIterClose(STsdbIter **iter);
int32_t tsdbIterNext(STsdbIter *iter);

// SIterMerger ===============
int32_t tsdbIterMergerOpen(const TTsdbIterArray *iterArray, SIterMerger **merger, bool isTomb);
int32_t tsdbIterMergerClose(SIterMerger **merger);
int32_t tsdbIterMergerNext(SIterMerger *merger);
int32_t tsdbIterMergerSkipTableData(SIterMerger *merger, const TABLEID *tbid);

SRowInfo    *tsdbIterMergerGetData(SIterMerger *merger);
STombRecord *tsdbIterMergerGetTombRecord(SIterMerger *merger);

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_ITER_H_*/