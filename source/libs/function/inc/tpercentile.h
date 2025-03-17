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

#ifndef TDENGINE_TPERCENTILE_H
#define TDENGINE_TPERCENTILE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tpagedbuf.h"
#include "functionResInfoInt.h"

struct tMemBucket;

int32_t tMemBucketCreate(int32_t nElemSize, int16_t dataType, STypeMod typeMod, double minval, double maxval, bool hasWindowOrGroup,
                         struct tMemBucket **pBucket, int32_t numOfElements);

void tMemBucketDestroy(struct tMemBucket **pBucket);

int32_t tMemBucketPut(struct tMemBucket *pBucket, const void *data, size_t size);

int32_t getPercentile(struct tMemBucket *pMemBucket, double percent, double *result);

#endif  // TDENGINE_TPERCENTILE_H

#ifdef __cplusplus
}
#endif
