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
#include "tsdbDef.h"

#ifndef _TSDB_ITER_H_
#define _TSDB_ITER_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SIterMerger SIterMerger;
typedef struct STsdbIter   STsdbIter;
typedef TARRAY2(STsdbIter *) TTsdbIterArray;

// STsdbIter ===============

// SIterMerger ===============
int32_t   tsdbIterMergerInit(const TTsdbIterArray *iterArray, SIterMerger **merger);
int32_t   tsdbIterMergerClear(SIterMerger **merger);
int32_t   tsdbIterMergerNext(SIterMerger *merger);
SRowInfo *tsdbIterMergerGet(SIterMerger *merger);
int32_t   tsdbIterMergerSkipTableData(SIterMerger *merger, const TABLEID *tbid);

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_ITER_H_*/