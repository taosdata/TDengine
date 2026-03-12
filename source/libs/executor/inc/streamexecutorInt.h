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
#ifndef STREAM_EXECUTORINT_H
#define STREAM_EXECUTORINT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "cJSON.h"
#include "cmdnodes.h"
#include "executorInt.h"
#include "querytask.h"
#include "tutil.h"

int32_t doTableScanNext(struct SOperatorInfo* pOperator, SSDataBlock** ppRes);
int32_t extractTableIdList(const STableListInfo* pTableListInfo, SArray** ppArrayRes);
void releaseFlusedPos(void* pRes);
typedef int32_t (*__compare_fn_t)(void* pKey, void* data, int32_t index);
int32_t binarySearchCom(void* keyList, int num, void* pKey, int order, __compare_fn_t comparefn);

#ifdef __cplusplus
}
#endif

#endif  // STREAM_EXECUTORINT_H
