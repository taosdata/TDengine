/**
 *  @file szd_int8.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the szd_int8.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZD_Int8_H
#define _SZD_Int8_H

#ifdef __cplusplus
extern "C" {
#endif

#include "TightDataPointStorageI.h"

#define SZ_INT8_MIN -128
#define SZ_INT8_MAX 127

void decompressDataSeries_int8_1D(int8_t** data, size_t dataSeriesLength, TightDataPointStorageI* tdps);
void decompressDataSeries_int8_2D(int8_t** data, size_t r1, size_t r2, TightDataPointStorageI* tdps);
void decompressDataSeries_int8_3D(int8_t** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageI* tdps);
void decompressDataSeries_int8_4D(int8_t** data, size_t r1, size_t r2, size_t r3, size_t r4, TightDataPointStorageI* tdps);

void getSnapshotData_int8_1D(int8_t** data, size_t dataSeriesLength, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_int8_2D(int8_t** data, size_t r1, size_t r2, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_int8_3D(int8_t** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_int8_4D(int8_t** data, size_t r1, size_t r2, size_t r3, size_t r4, TightDataPointStorageI* tdps, int errBoundMode);

int SZ_decompress_args_int8(int8_t** newData, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, unsigned char* cmpBytes, size_t cmpSize);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZD_Int8_H  ----- */
