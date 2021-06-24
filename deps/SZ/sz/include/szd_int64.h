/**
 *  @file szd_int64.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the szd_int64.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZD_Int64_H
#define _SZD_Int64_H

#ifdef __cplusplus
extern "C" {
#endif

#include "TightDataPointStorageI.h"

void decompressDataSeries_int64_1D(int64_t** data, size_t dataSeriesLength, TightDataPointStorageI* tdps);
void decompressDataSeries_int64_2D(int64_t** data, size_t r1, size_t r2, TightDataPointStorageI* tdps);
void decompressDataSeries_int64_3D(int64_t** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageI* tdps);
void decompressDataSeries_int64_4D(int64_t** data, size_t r1, size_t r2, size_t r3, size_t r4, TightDataPointStorageI* tdps);

void getSnapshotData_int64_1D(int64_t** data, size_t dataSeriesLength, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_int64_2D(int64_t** data, size_t r1, size_t r2, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_int64_3D(int64_t** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_int64_4D(int64_t** data, size_t r1, size_t r2, size_t r3, size_t r4, TightDataPointStorageI* tdps, int errBoundMode);

int SZ_decompress_args_int64(int64_t** newData, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, unsigned char* cmpBytes, size_t cmpSize);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZD_Int64_H  ----- */
