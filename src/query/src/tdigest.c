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

#include "os.h"
#include "osMath.h"
#include "tdigest.h"

#define INTERPOLATE(x, x0, x1) (((x) - (x0)) / ((x1) - (x0)))
#define INTEGRATED_LOCATION(compression, q) ((compression) * (asin(2 * (double)(q) - 1)/M_PI + (double)1/2))
#define FLOAT_EQ(f1, f2) (fabs((f1) - (f2)) <= FLT_EPSILON)

typedef struct SMergeArgs {
    TDigest *t;
    SCentroid *centroids;
    int32_t idx;
    double weight_so_far;
    double k1;
    double min;
    double max;
}SMergeArgs;     

void tdigestCopy(TDigest* dst, TDigest* src) {
    memcpy(dst, src, (size_t)TDIGEST_SIZE(COMPRESSION));

    dst->centroids    = (SCentroid*)malloc((int32_t)GET_CENTROID(COMPRESSION) * sizeof(SCentroid));
    memcpy(dst->centroids, src->centroids, (int32_t)GET_CENTROID(COMPRESSION) * sizeof(SCentroid));
    dst->buffered_pts = (SPt*)   ((char*)dst + sizeof(TDigest));
}

TDigest *tdigestNewFrom(void* pBuf, int32_t compression) {
    memset(pBuf, 0, (size_t)TDIGEST_SIZE(compression));
    TDigest* t = (TDigest*)pBuf;
    
    t->centroids    = (SCentroid*)calloc((int32_t)GET_CENTROID(compression), sizeof(SCentroid));
    t->buffered_pts = (SPt*)   ((char*)t + sizeof(TDigest));
    t->compression = compression;
    t->size = (int64_t)GET_CENTROID(compression);
    t->threshold = (int32_t)GET_THRESHOLD(compression);
    t->min = INFINITY;
    return t;
}

static int32_t cmpCentroid(const void *a, const void *b) {
    SCentroid *c1 = (SCentroid *) a;
    SCentroid *c2 = (SCentroid *) b;
    if (c1->mean < c2->mean)
        return -1;
    if (c1->mean > c2->mean)
        return 1;
    return 0;
}

static void mergeCentroid(SMergeArgs *args, SCentroid *merge) {
    double k2;
    SCentroid *c = &args->centroids[args->idx];

    args->weight_so_far += merge->weight;
    k2 = INTEGRATED_LOCATION(args->t->size,
            args->weight_so_far / (args->t->total_weight + merge->weight));
    //idx++
    if(k2 - args->k1 > 1 && c->weight > 0) {
        if(args->idx + 1 < args->t->size
         && merge->mean != args->centroids[args->idx].mean) {
             args->idx++;     
        }
        args->k1 = k2;
    }

    c = &args->centroids[args->idx];
    if(c->mean == merge->mean) {
        c->weight += merge->weight;
    } else {
        c->weight += merge->weight;
        c->mean += (merge->mean - c->mean) * merge->weight / c->weight;

        if (merge->weight > 0) {
            args->min = MIN(merge->mean, args->min);
            args->max = MAX(merge->mean, args->max);
        }
    }
}

void tdigestCompress(TDigest *t) {
    SCentroid *unmerged_centroids = (SCentroid *)t->buffered_pts;
    int32_t num_unmerged = t->num_buffered_pts;
    int32_t i, j;
    SMergeArgs args = {0};

    if (t->num_buffered_pts <= 0)
        return;

    t->num_buffered_pts = 0;

    qsort(unmerged_centroids, num_unmerged, sizeof(SCentroid), cmpCentroid);
    args.centroids = (SCentroid*)calloc(sizeof(SCentroid), (size_t)t->size);

    args.t = t;
    args.min = INFINITY;

    i = 0;
    j = 0;
    while (i < num_unmerged && j < t->num_centroids) {
        SCentroid *a = &unmerged_centroids[i];
        SCentroid *b = &t->centroids[j];

        if (a->mean <= b->mean) {
            mergeCentroid(&args, a);            
            assert(args.idx < (t->size));
            i++;
        } else {
            mergeCentroid(&args, b);
            assert(args.idx < (t->size));
            j++;
        }
    }

    while (i < num_unmerged) {
        mergeCentroid(&args, &unmerged_centroids[i++]);
        assert(args.idx < (t->size));
    }

    while (j < t->num_centroids) {
        mergeCentroid(&args, &t->centroids[j++]);
        assert(args.idx < (t->size));
    }

    if (t->total_weight > 0) {
        t->min = MIN(t->min, args.min);
        if (args.centroids[args.idx].weight <= 0) {
            args.idx--;
        }
        t->num_centroids = args.idx + 1;
        t->max = MAX(t->max, args.max);
    }

    tfree(t->centroids);
    t->centroids = args.centroids;
}

void tdigestAdd(TDigest* t, double x, int64_t w) {
    if (w == 0)
        return;

    int32_t i = t->num_buffered_pts;
    if(i > 0 && t->buffered_pts[i-1].value == x ) {
        t->buffered_pts[i].weight = w;
    } else {
        t->buffered_pts[i].value  = x;
        t->buffered_pts[i].weight = w;
        t->num_buffered_pts++;        
    }

    if (t->num_buffered_pts >= t->threshold)
        tdigestCompress(t);
}


double tdigestQuantile(TDigest *t, double q) {
    if (t == NULL)
        return 0;

    int32_t i;
    double left, right, idx;
    int64_t weight_so_far;
    SCentroid *a, *b, tmp;

    tdigestCompress(t);
    if (t->num_centroids == 0)
        return NAN;
    if (t->num_centroids == 1)
        return t->centroids[0].mean;
    if (FLOAT_EQ(q, 0.0))
        return t->min;
    if (FLOAT_EQ(q, 1.0))
        return t->max;

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
        right = (b->weight * a->mean + a->weight * b->mean)/ (a->weight + b->weight);
        if (idx < weight_so_far + a->weight) {
            double p = (idx - weight_so_far) / a->weight;
            return left * (1 - p) + right * p;
        }
        weight_so_far += a->weight;
    }

    left = right;
    a = b;
    right = t->max;

    if (idx < weight_so_far + a->weight) {
        double p = (idx - weight_so_far) / a->weight;
        return left * (1 - p) + right * p;
    }

    return t->max;
}

void tdigestMerge(TDigest *t1, TDigest *t2) {
    // SPoints
    int32_t num_SPoints = t2->num_buffered_pts;
    for(int32_t i = num_SPoints - 1; i >= 0; i--) {
        SPt* p = t2->buffered_pts + i;
        tdigestAdd(t1, p->value, p->weight);
        t2->num_buffered_pts --;
    }
    // centroids
    for (int32_t i = 0; i < t2->num_centroids; i++) {
        tdigestAdd(t1, t2->centroids[i].mean, t2->centroids[i].weight);
    }
}
