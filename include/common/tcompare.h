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

#ifndef _TD_TCOMPARE_H_
#define _TD_TCOMPARE_H_

#include "compare.h"
#include "ttypes.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t       compareStrPatternComp(const void* pLeft, const void* pRight);
int32_t       compareWStrPatternComp(const void* pLeft, const void* pRight);
__compar_fn_t getComparFunc(int32_t type, int32_t optr);
__compar_fn_t getKeyComparFunc(int32_t keyType, int32_t order);
int32_t       doCompare(const char* a, const char* b, int32_t type, size_t size);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TCOMPARE_H_*/