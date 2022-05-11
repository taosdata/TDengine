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

#ifndef _TD_INDEX_COMM_H_
#define _TD_INDEX_COMM_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "indexInt.h"
#include "tcompare.h"

extern char JSON_COLUMN[];
extern char JSON_VALUE_DELIM;

char* indexPackJsonData(SIndexTerm* itm);
char* indexPackJsonDataPrefix(SIndexTerm* itm, int32_t* skip);

typedef enum { MATCH, CONTINUE, BREAK } TExeCond;

typedef TExeCond (*_cache_range_compare)(void* a, void* b, int8_t type);

TExeCond tDoCommpare(__compar_fn_t func, int8_t comType, void* a, void* b);

_cache_range_compare indexGetCompare(RangeType ty);

#ifdef __cplusplus
}
#endif

#endif
