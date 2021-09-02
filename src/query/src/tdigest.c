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

#include "tdigest.h"

#define INTERPOLATE(x, x0, x1) (((x) - (x0)) / ((x1) - (x0)))
#define INTEGRATED_LOCATION(compression, q) ((compression) * (asin(2 * (q) - 1) + M_PI / 2) / M_PI)
#define FLOAT_EQ(f1, f2) (fabs((f1) - (f2)) <= FLT_EPSILON)

/* From http://stackoverflow.com/questions/3437404/min-and-max-in-c */
#define MAX(a,b) \
   ({ __typeof__ (a) _a = (a); \
      __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })
#define MIN(a,b) \
   ({ __typeof__ (a) _a = (a); \
      __typeof__ (b) _b = (b); \
     _a < _b ? _a : _b; })

struct TDigest *tdigestNew(int compression) {
    struct TDigest *t = malloc(sizeof(struct TDigest));

    memset(t, 0, sizeof(struct TDigest));

    t->compression = compression;
    t->size = ceil(compression * M_PI / 2) + 1;
    t->threshold = 7.5 + 0.37 * compression - 2e-4 * pow(compression, 2);
    t->min = INFINITY;

    return t;
}

static int centroid_cmp(const void *a, const void *b) {
    struct Centroid *c1 = (struct Centroid *) a;
    struct Centroid *c2 = (struct Centroid *) b;
    if (c1->mean < c2->mean)
        return -1;
    if (c1->mean > c2->mean)
        return 1;
    return 0;
}

struct MergeArgs {
    struct TDigest *t;
    struct Centroid *centroids;
    int idx;
    double weight_so_far;
    double k1;
    double min;
    double max;
};

static void merge_centroid(struct MergeArgs *args, struct Centroid *merge) {
    double k2;
    struct Centroid *c = &args->centroids[args->idx];

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

void tdigestCompress(struct TDigest *t) {
    struct Centroid *unmerged_centroids;
    long long unmerged_weight = 0;
    int num_unmerged = t->num_buffered_pts;
    int old_num_centroids = t->num_centroids;
    int i, j;
    struct MergeArgs args;

    if (!t->num_buffered_pts)
        return;

    unmerged_centroids = malloc(sizeof(struct Centroid) * t->num_buffered_pts);

    i = 0;
    for (i = 0; i < num_unmerged; i++) {
        struct Point *p = t->buffered_pts;
        struct Centroid *c = &unmerged_centroids[i];

        c->mean = p->value;
        c->weight = p->weight;

        unmerged_weight += c->weight;

        t->buffered_pts = p->next;
        free(p);
    }

    t->num_buffered_pts = 0;
    t->total_weight += unmerged_weight;

    qsort(unmerged_centroids, num_unmerged, sizeof(struct Centroid),
            centroid_cmp);

    memset(&args, 0, sizeof(struct MergeArgs));

    args.centroids = malloc(sizeof(struct Centroid) * t->size);
    memset(args.centroids, 0, sizeof(struct Centroid) * t->size);

    args.t = t;
    args.min = INFINITY;

    i = 0;
    j = 0;
    while (i < num_unmerged && j < t->num_centroids) {
        struct Centroid *a = &unmerged_centroids[i];
        struct Centroid *b = &t->centroids[j];

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

    free(unmerged_centroids);

    while (j < t->num_centroids)
        merge_centroid(&args, &t->centroids[j++]);

    if (t->total_weight > 0) {
        t->min = MIN(t->min, args.min);

        if (args.centroids[args.idx].weight <= 0)
            args.idx--;

        t->num_centroids = args.idx + 1;
        t->max = MAX(t->max, args.max);
    }

    if (t->num_centroids > old_num_centroids) {
        t->centroids = realloc(t->centroids,
                sizeof(struct Centroid) * t->num_centroids);
    }

    memcpy(t->centroids, args.centroids,
            sizeof(struct Centroid) * t->num_centroids);

    free(args.centroids);
}

void tdigestAdd(struct TDigest *t, double x, long long w) {
    if (w == 0)
        return;

    struct Point *p = malloc(sizeof(struct Point));

    p->value = x;
    p->weight = w;
    p->next = t->buffered_pts;

    t->buffered_pts = p;
    t->num_buffered_pts++;

    if (t->num_buffered_pts > t->threshold)
        tdigestCompress(t);
}

double tdigestCDF(struct TDigest *t, double x) {
    if (t == NULL)
        return 0;

    int i;
    double left, right;
    long long weight_so_far;
    struct Centroid *a, *b, tmp;

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
        struct Centroid *c = &t->centroids[i];

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

    if (x < a->mean + right)
        return (weight_so_far
                + a->weight * INTERPOLATE(x, a->mean - left, a->mean + right))
                / t->total_weight;

    return 1;
}

double tdigestQuantile(struct TDigest *t, double q) {
    if (t == NULL)
        return 0;

    int i;
    double left, right, idx;
    long long weight_so_far;
    struct Centroid *a, *b, tmp;

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
        struct Centroid *c = &t->centroids[i];
        a = b;
        left = right;

        b = c;
        right = (b->weight * a->mean + a->weight * b->mean)
                / (a->weight + b->weight);

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

void tdigestMerge(struct TDigest *t1, struct TDigest *t2) {
    int i = t2->num_buffered_pts;
    struct Point *p = t2->buffered_pts;

    while (i) {
        tdigestAdd(t1, p->value, p->weight);
        p = p->next;
        i--;
    }

    for (i = 0; i < t2->num_centroids; i++) {
        tdigestAdd(t1, t2->centroids[i].mean, t2->centroids[i].weight);
    }
}

void tdigestFree(struct TDigest *t) {
    while (t->buffered_pts) {
        struct Point *p = t->buffered_pts;
        t->buffered_pts = t->buffered_pts->next;
        free(p);
    }

    if (t->centroids)
        free(t->centroids);

    free(t);
}

