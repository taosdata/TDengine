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

#include <ctype.h>
#include <float.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "osMath.h"
#include "tdigest.h"

#define INTERPOLATE(x, x0, x1) (((x) - (x0)) / ((x1) - (x0)))
#define INTEGRATED_LOCATION(compression, q) ((compression) * (asin(2 * (q) - 1) + M_PI / 2) / M_PI)
#define FLOAT_EQ(f1, f2) (fabs((f1) - (f2)) <= FLT_EPSILON)

typedef struct MergeArgs {
    TDigest *t;
    Centroid *centroids;
    int idx;
    double weight_so_far;
    double k1;
    double min;
    double max;
}MergeArgs;     

void tdigestAutoFill(TDigest* t, int compression) {
    t->centroids    = (Centroid*)((char*)t + sizeof(TDigest));
    t->buffered_pts = (Point*)   ((char*)t + sizeof(TDigest) + sizeof(Centroid) * (int)GET_CENTROID(compression));
}

TDigest *tdigestNewFrom(void* pBuf, int compression) {
    memset(pBuf, 0, sizeof(TDigest) + sizeof(Centroid)*(compression + 1));
    TDigest* t = (TDigest*)pBuf;
    tdigestAutoFill(t, compression);

    t->compression = compression;
    t->size = (long long)GET_CENTROID(compression);
    t->threshold = (int)GET_THRESHOLD(compression);
    t->min = INFINITY;
    return t;
}

static int centroid_cmp(const void *a, const void *b) {
    Centroid *c1 = (Centroid *) a;
    Centroid *c2 = (Centroid *) b;
    if (c1->mean < c2->mean)
        return -1;
    if (c1->mean > c2->mean)
        return 1;
    return 0;
}

static void merge_centroid(MergeArgs *args, Centroid *merge) {
    double k2;
    Centroid *c = &args->centroids[args->idx];

    args->weight_so_far += merge->weight;
    k2 = INTEGRATED_LOCATION(args->t->compression,
            args->weight_so_far / args->t->total_weight);

    if (k2 - args->k1 > 1 && c->weight > 0) {
        args->idx++;
        args->k1 = INTEGRATED_LOCATION(args->t->compression,
                (args->weight_so_far - merge->weight) / args->t->total_weight);
    }

    c = &args->centroids[args->idx];
    c->weight += merge->weight;
    c->mean += (merge->mean - c->mean) * merge->weight / c->weight;

    if (merge->weight > 0) {
        args->min = MIN(merge->mean, args->min);
        args->max = MAX(merge->mean, args->max);
    }
}

void tdigestCompress(TDigest *t) {
    Centroid *unmerged_centroids;
    long long unmerged_weight = 0;
    int num_unmerged = t->num_buffered_pts;
    int i, j;
    MergeArgs args;

    if (!t->num_buffered_pts)
        return;

    unmerged_centroids = (Centroid*)malloc(sizeof(Centroid) * t->num_buffered_pts);
    for (i = 0; i < num_unmerged; i++) {
        Point *p = t->buffered_pts + i;
        Centroid *c = &unmerged_centroids[i];
        c->mean = p->value;
        c->weight = p->weight;
        unmerged_weight += c->weight;
    }

    t->num_buffered_pts = 0;
    t->total_weight += unmerged_weight;

    qsort(unmerged_centroids, num_unmerged, sizeof(Centroid), centroid_cmp);
    memset(&args, 0, sizeof(MergeArgs));
    args.centroids = (Centroid*)malloc(sizeof(Centroid) * t->size);
    memset(args.centroids, 0, sizeof(Centroid) * t->size);

    args.t = t;
    args.min = INFINITY;

    i = 0;
    j = 0;
    while (i < num_unmerged && j < t->num_centroids) {
        Centroid *a = &unmerged_centroids[i];
        Centroid *b = &t->centroids[j];

        if (a->mean <= b->mean) {
            merge_centroid(&args, a);
            i++;
        } else {
            merge_centroid(&args, b);
            j++;
        }
    }

    while (i < num_unmerged)
        merge_centroid(&args, &unmerged_centroids[i++]);
    free((void*)unmerged_centroids);

    while (j < t->num_centroids)
        merge_centroid(&args, &t->centroids[j++]);

    if (t->total_weight > 0) {
        t->min = MIN(t->min, args.min);

        if (args.centroids[args.idx].weight <= 0)
            args.idx--;

        t->num_centroids = args.idx + 1;
        t->max = MAX(t->max, args.max);
    }

    memcpy(t->centroids, args.centroids, sizeof(Centroid) * t->num_centroids);
    free((void*)args.centroids);
}

void tdigestAdd(TDigest* t, double x, long long w) {
    if (w == 0)
        return;

    int i = t->num_buffered_pts;
    t->buffered_pts[i].value  = x;
    t->buffered_pts[i].weight = w;
    t->num_buffered_pts++;

    if (t->num_buffered_pts > t->threshold)
        tdigestCompress(t);
}

double tdigestCDF(TDigest *t, double x) {
    if (t == NULL)
        return 0;

    int i;
    double left, right;
    long long weight_so_far;
    Centroid *a, *b, tmp;

    tdigestCompress(t);

    if (t->num_centroids == 0)
        return NAN;

    if (x < t->min)
        return 0;
    if (x > t->max)
        return 1;

    if (t->num_centroids == 1) {
        if (FLOAT_EQ(t->max, t->min))
            return 0.5;

        return INTERPOLATE(x, t->min, t->max);
    }

    weight_so_far = 0;
    a = b = &tmp;
    b->mean = t->min;
    b->weight = 0;
    right = 0;

    for (i = 0; i < t->num_centroids; i++) {
        Centroid *c = &t->centroids[i];

        left = b->mean - (a->mean + right);
        a = b;
        b = c;
        right = (b->mean - a->mean) * a->weight / (a->weight + b->weight);

        if (x < a->mean + right) {
            double cdf = (weight_so_far
                    + a->weight
                    * INTERPOLATE(x, a->mean - left, a->mean + right))
                    / t->total_weight;
            return MAX(cdf, 0.0);
        }

        weight_so_far += a->weight;
    }

    left = b->mean - (a->mean + right);
    a = b;
    right = t->max - a->mean;

    if (x < a->mean + right) {
        return (weight_so_far + a->weight * INTERPOLATE(x, a->mean - left, a->mean + right))
                / t->total_weight;
    }

    return 1;
}

double tdigestQuantile(TDigest *t, double q) {
    if (t == NULL)
        return 0;

    int i;
    double left, right, idx;
    long long weight_so_far;
    Centroid *a, *b, tmp;

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
        Centroid *c = &t->centroids[i];
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
    // points
    int num_points = t2->num_buffered_pts;
    for(int i = num_points - 1; i >= 0; i--) {
        Point* p = t2->buffered_pts + i;
        tdigestAdd(t1, p->value, p->weight);
        t2->num_buffered_pts --;
    }
    // centroids
    for (int i = 0; i < t2->num_centroids; i++) {
        tdigestAdd(t1, t2->centroids[i].mean, t2->centroids[i].weight);
    }
}