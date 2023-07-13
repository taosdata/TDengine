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
#ifndef TDENGINE_FILTER_H
#define TDENGINE_FILTER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "nodes.h"
#include "tcommon.h"

typedef struct SFilterInfo SFilterInfo;
typedef int32_t (*filer_get_col_from_id)(void *, int32_t, void **);

enum {
  FLT_OPTION_NO_REWRITE = 1,
  FLT_OPTION_TIMESTAMP = 2,
  FLT_OPTION_NEED_UNIQE = 4,
};

#define FILTER_RESULT_ALL_QUALIFIED     0x1
#define FILTER_RESULT_NONE_QUALIFIED    0x2
#define FILTER_RESULT_PARTIAL_QUALIFIED 0x3

typedef struct SFilterColumnParam {
  int32_t numOfCols;
  SArray *pDataBlock;
} SFilterColumnParam;

extern int32_t filterInitFromNode(SNode *pNode, SFilterInfo **pinfo, uint32_t options);
extern int32_t filterExecute(SFilterInfo *info, SSDataBlock *pSrc, SColumnInfoData **p, SColumnDataAgg *statis,
                             int16_t numOfCols, int32_t *pFilterResStatus);
extern int32_t filterSetDataFromSlotId(SFilterInfo *info, void *param);
extern int32_t filterSetDataFromColId(SFilterInfo *info, void *param);
extern int32_t filterGetTimeRange(SNode *pNode, STimeWindow *win, bool *isStrict);
extern int32_t filterConverNcharColumns(SFilterInfo *pFilterInfo, int32_t rows, bool *gotNchar);
extern int32_t filterFreeNcharColumns(SFilterInfo *pFilterInfo);
extern void    filterFreeInfo(SFilterInfo *info);
extern bool    filterRangeExecute(SFilterInfo *info, SColumnDataAgg **pColsAgg, int32_t numOfCols, int32_t numOfRows);

/* condition split interface */
int32_t filterPartitionCond(SNode **pCondition, SNode **pPrimaryKeyCond, SNode **pTagIndexCond, SNode **pTagCond,
                            SNode **pOtherCond);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_FILTER_H
