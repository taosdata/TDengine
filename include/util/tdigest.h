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

/*
 * include/tdigest.c
 *
 * Copyright (c) 2016, Usman Masood <usmanm at fastmail dot fm>
 */

#ifndef TDIGEST_H
#define TDIGEST_H

#include "os.h"

#ifndef M_PI
#define M_PI 3.14159265358979323846264338327950288 /* pi             */
#endif

#define DOUBLE_MAX 1.79e+308

#define ADDITION_CENTROID_NUM      2
#define COMPRESSION                300
#define GET_CENTROID(compression)  (ceil(compression * M_PI / 2) + 1 + ADDITION_CENTROID_NUM)
#define GET_THRESHOLD(compression) (7.5 + 0.37 * compression - 2e-4 * pow(compression, 2))
#define TDIGEST_SIZE(compression) \
  (sizeof(TDigest) + sizeof(SCentroid) * GET_CENTROID(compression) + sizeof(SPt) * GET_THRESHOLD(compression))

typedef struct SCentroid {
  double  mean;
  int64_t weight;
} SCentroid;

typedef struct SPt {
  double  value;
  int64_t weight;
} SPt;

typedef struct TDigest {
  double  compression;
  int32_t threshold;
  int64_t size;

  int64_t total_weight;
  double  min;
  double  max;

  int32_t num_buffered_pts;
  SPt    *buffered_pts;

  int32_t    num_centroids;
  SCentroid *centroids;
} TDigest;

TDigest *tdigestNewFrom(void *pBuf, int32_t compression);
void     tdigestAdd(TDigest *t, double x, int64_t w);
void     tdigestMerge(TDigest *t1, TDigest *t2);
double   tdigestQuantile(TDigest *t, double q);
void     tdigestCompress(TDigest *t);
void     tdigestFreeFrom(TDigest *t);
void     tdigestAutoFill(TDigest *t, int32_t compression);

#endif /* TDIGEST_H */
