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

#ifndef TDENGINE_TCOMPARE_H
#define TDENGINE_TCOMPARE_H

#include "os.h"

int32_t compareInt32Val(const void *pLeft, const void *pRight);

int32_t compareInt64Val(const void *pLeft, const void *pRight);

int32_t compareInt16Val(const void *pLeft, const void *pRight);

int32_t compareInt8Val(const void *pLeft, const void *pRight);

int32_t compareIntDoubleVal(const void *pLeft, const void *pRight);

int32_t compareDoubleIntVal(const void *pLeft, const void *pRight);

int32_t compareDoubleVal(const void *pLeft, const void *pRight);

int32_t compareStrVal(const void *pLeft, const void *pRight);

int32_t compareWStrVal(const void *pLeft, const void *pRight);

__compar_fn_t getKeyComparFunc(int32_t keyType);

__compar_fn_t getComparFunc(int32_t type, int32_t filterDataType);

#endif  // TDENGINE_TCOMPARE_H
