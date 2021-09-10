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

#ifndef M_PI
#define M_PI        3.14159265358979323846264338327950288   /* pi             */
#endif

#define COMPRESSION 400
#define GET_CENTROID(compression)  (ceil(compression * M_PI / 2) + 1)
#define GET_THRESHOLD(compression) (7.5 + 0.37 * compression - 2e-4 * pow(compression, 2))
#define TDIGEST_SIZE(compression)  (sizeof(TDigest) + sizeof(Centroid)*GET_CENTROID(compression) + sizeof(Point)*GET_THRESHOLD(compression))

typedef struct Centroid {
    double mean;
    long long weight;
}Centroid;

typedef struct Point {
    double value;
    long long weight;
}Point;

typedef struct TDigest {
    double compression;
    int threshold;
    long long size;

    long long total_weight;
    double min;
    double max;

    int num_buffered_pts;
    Point *buffered_pts;

    int num_centroids;
    Centroid *centroids;
}TDigest;

TDigest *tdigestNewFrom(void* pBuf, int compression);
void tdigestAdd(TDigest *t, double x, long long w);
void tdigestMerge(TDigest *t1, TDigest *t2);
double tdigestCDF(TDigest *t, double x);
double tdigestQuantile(TDigest *t, double q);
void tdigestCompress(TDigest *t);
void tdigestFreeFrom(TDigest *t);
void tdigestAutoFill(TDigest* t, int compression);

#endif /* TDIGEST_H */
