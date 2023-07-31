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
 * src/tdigest.c
 *
 * Implementation of the t-digest data structure used to compute accurate percentiles.
 *
 * It is based on the MergingDigest implementation found at:
 *   https://github.com/tdunning/t-digest/blob/master/src/main/java/com/tdunning/math/stats/MergingDigest.java
 *
 * Copyright (c) 2016, Usman Masood <usmanm at fastmail dot fm>
 */

#include "tdigest.h"
#include "os.h"
#include "osMath.h"
#include "tlog.h"

#define INTERPOLATE(x, x0, x1) (((x) - (x0)) / ((x1) - (x0)))
//#define INTEGRATED_LOCATION(compression, q)   ((compression) * (asin(2 * (q) - 1) + M_PI / 2) / M_PI)
#define INTEGRATED_LOCATION(compression, q) ((compression) * (asin(2 * (double)(q)-1) / M_PI + (double)1 / 2))
#define FLOAT_EQ(f1, f2)                    (fabs((f1) - (f2)) <= FLT_EPSILON)

typedef struct SMergeArgs {
  TDigest   *t;
  SCentroid *centroids;
  int32_t    idx;
  double     weight_so_far;
  double     k1;
  double     min;
  double     max;
} SMergeArgs;

void tdigestAutoFill(TDigest *t, int32_t compression) {
  t->centroids = (SCentroid *)((char *)t + sizeof(TDigest));
  t->buffered_pts = (SPt *)((char *)t + sizeof(TDigest) + sizeof(SCentroid) * (int32_t)GET_CENTROID(compression));
}

TDigest *tdigestNewFrom(void *pBuf, int32_t compression) {
  memset(pBuf, 0, (size_t)TDIGEST_SIZE(compression));
  TDigest *t = (TDigest *)pBuf;
  tdigestAutoFill(t, compression);

  t->compression = compression;
  t->size = (int64_t)GET_CENTROID(compression);
  t->threshold = (int32_t)GET_THRESHOLD(compression);
  t->min = DOUBLE_MAX;
  t->max = -DOUBLE_MAX;

  return t;
}

static int32_t cmpCentroid(const void *a, const void *b) {
  SCentroid *c1 = (SCentroid *)a;
  SCentroid *c2 = (SCentroid *)b;
  if (c1->mean < c2->mean) return -1;
  if (c1->mean > c2->mean) return 1;
  return 0;
}

static void mergeCentroid(SMergeArgs *args, SCentroid *merge) {
  double     k2;
  SCentroid *c = &args->centroids[args->idx];

  args->weight_so_far += merge->weight;
  k2 = INTEGRATED_LOCATION(args->t->size, args->weight_so_far / args->t->total_weight);
  // idx++
  if (k2 - args->k1 > 1 && c->weight > 0) {
    if (args->idx + 1 < args->t->size && merge->mean != args->centroids[args->idx].mean) {
      args->idx++;
    }
    args->k1 = k2;
  }

  c = &args->centroids[args->idx];
  if (c->mean == merge->mean) {
    c->weight += merge->weight;
  } else {
    c->weight += merge->weight;
    c->mean += (merge->mean - c->mean) * merge->weight / c->weight;

    if (merge->weight > 0) {
      args->min = TMIN(merge->mean, args->min);
      args->max = TMAX(merge->mean, args->max);
    }
  }
}

void tdigestCompress(TDigest *t) {
  SCentroid *unmerged_centroids;
  int64_t    unmerged_weight = 0;
  int32_t    num_unmerged = t->num_buffered_pts;
  int32_t    i, j;
  SMergeArgs args;

  if (t->num_buffered_pts <= 0) return;

  unmerged_centroids = (SCentroid *)taosMemoryMalloc(sizeof(SCentroid) * t->num_buffered_pts);
  for (i = 0; i < num_unmerged; i++) {
    SPt       *p = t->buffered_pts + i;
    SCentroid *c = &unmerged_centroids[i];
    c->mean = p->value;
    c->weight = p->weight;
    unmerged_weight += c->weight;
  }
  t->num_buffered_pts = 0;
  t->total_weight += unmerged_weight;

  taosSort(unmerged_centroids, num_unmerged, sizeof(SCentroid), cmpCentroid);
  memset(&args, 0, sizeof(SMergeArgs));
  args.centroids = (SCentroid *)taosMemoryMalloc((size_t)(sizeof(SCentroid) * t->size));
  memset(args.centroids, 0, (size_t)(sizeof(SCentroid) * t->size));

  args.t = t;
  args.min = DOUBLE_MAX;
  args.max = -DOUBLE_MAX;

  i = 0;
  j = 0;
  while (i < num_unmerged && j < t->num_centroids) {
    SCentroid *a = &unmerged_centroids[i];
    SCentroid *b = &t->centroids[j];

    if (a->mean <= b->mean) {
      mergeCentroid(&args, a);
      ASSERTS(args.idx < t->size, "idx over size");
      i++;
    } else {
      mergeCentroid(&args, b);
      ASSERTS(args.idx < t->size, "idx over size");
      j++;
    }
  }

  while (i < num_unmerged) {
    mergeCentroid(&args, &unmerged_centroids[i++]);
    ASSERTS(args.idx < t->size, "idx over size");
  }
  taosMemoryFree((void *)unmerged_centroids);

  while (j < t->num_centroids) {
    mergeCentroid(&args, &t->centroids[j++]);
    ASSERTS(args.idx < t->size, "idx over size");
  }

  if (t->total_weight > 0) {
    t->min = TMIN(t->min, args.min);
    if (args.centroids[args.idx].weight <= 0) {
      args.idx--;
    }
    t->num_centroids = args.idx + 1;
    t->max = TMAX(t->max, args.max);
  }

  memcpy(t->centroids, args.centroids, sizeof(SCentroid) * t->num_centroids);
  taosMemoryFree((void *)args.centroids);
}

void tdigestAdd(TDigest *t, double x, int64_t w) {
  if (w == 0) return;

  int32_t i = t->num_buffered_pts;
  if (i > 0 && t->buffered_pts[i - 1].value == x) {
    t->buffered_pts[i].weight = w;
  } else {
    t->buffered_pts[i].value = x;
    t->buffered_pts[i].weight = w;
    t->num_buffered_pts++;
  }

  if (t->num_buffered_pts >= t->threshold) tdigestCompress(t);
}

#if 0
double tdigestCDF(TDigest *t, double x) {
  if (t == NULL) return 0;

  int32_t    i;
  double     left, right;
  int64_t    weight_so_far;
  SCentroid *a, *b, tmp;

  tdigestCompress(t);
  if (t->num_centroids == 0) return NAN;
  if (x < t->min) return 0;
  if (x > t->max) return 1;
  if (t->num_centroids == 1) {
    if (FLOAT_EQ(t->max, t->min)) return 0.5;

    return INTERPOLATE(x, t->min, t->max);
  }

  weight_so_far = 0;
  a = b = &tmp;
  b->mean = t->min;
  b->weight = 0;
  right = 0;

  for (i = 0; i < t->num_centroids; i++) {
    SCentroid *c = &t->centroids[i];

    left = b->mean - (a->mean + right);
    a = b;
    b = c;
    right = (b->mean - a->mean) * a->weight / (a->weight + b->weight);

    if (x < a->mean + right) {
      double cdf = (weight_so_far + a->weight * INTERPOLATE(x, a->mean - left, a->mean + right)) / t->total_weight;
      return TMAX(cdf, 0.0);
    }

    weight_so_far += a->weight;
  }

  left = b->mean - (a->mean + right);
  a = b;
  right = t->max - a->mean;

  if (x < a->mean + right) {
    return (weight_so_far + a->weight * INTERPOLATE(x, a->mean - left, a->mean + right)) / t->total_weight;
  }

  return 1;
}
#endif

double tdigestQuantile(TDigest *t, double q) {
  if (t == NULL) return 0;

  int32_t    i;
  double     left, right, idx;
  int64_t    weight_so_far;
  SCentroid *a, *b, tmp;

  tdigestCompress(t);
  if (t->num_centroids == 0) return NAN;
  if (t->num_centroids == 1) return t->centroids[0].mean;
  if (FLOAT_EQ(q, 0.0)) return t->min;
  if (FLOAT_EQ(q, 1.0)) return t->max;

  idx = q * t->total_weight;
  weight_so_far = 0;
  b = &tmp;
  b->mean = t->min;
  b->weight = 0;
  right = t->min;

  for (i = 0; i < t->num_centroids; i++) {
    SCentroid *c = &t->centroids[i];
    a = b;
    left = right;

    b = c;
    right = (b->weight * a->mean + a->weight * b->mean) / (a->weight + b->weight);
    if (idx < weight_so_far + a->weight) {
      double p = (idx - weight_so_far) / ((a->weight == 0) ? 1 : a->weight);
      return left * (1 - p) + right * p;
    }
    weight_so_far += a->weight;
  }

  left = right;
  a = b;
  right = t->max;

  if (idx < weight_so_far + a->weight && a->weight != 0) {
    double p = (idx - weight_so_far) / a->weight;
    return left * (1 - p) + right * p;
  }

  return t->max;
}

void tdigestMerge(TDigest *t1, TDigest *t2) {
  // SPoints
  int32_t num_pts = t2->num_buffered_pts;
  for (int32_t i = num_pts - 1; i >= 0; i--) {
    SPt *p = t2->buffered_pts + i;
    tdigestAdd(t1, p->value, p->weight);
    t2->num_buffered_pts--;
  }
  // centroids
  for (int32_t i = 0; i < t2->num_centroids; i++) {
    tdigestAdd(t1, t2->centroids[i].mean, t2->centroids[i].weight);
  }
}
