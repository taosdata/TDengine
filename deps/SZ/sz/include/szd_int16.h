/**
 *  @file szd_int16.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the szd_int16.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZD_Int16_H
#define _SZD_Int16_H

#ifdef __cplusplus
extern "C" {
#endif

#include "TightDataPointStorageI.h"

#define SZ_INT16_MIN -32768
#define SZ_INT16_MAX 32767

void decompressDataSeries_int16_1D(int16_t** data, size_t dataSeriesLength, TightDataPointStorageI* tdps);
void decompressDataSeries_int16_2D(int16_t** data, size_t r1, size_t r2, TightDataPointStorageI* tdps);
void decompressDataSeries_int16_3D(int16_t** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageI* tdps);
void decompressDataSeries_int16_4D(int16_t** data, size_t r1, size_t r2, size_t r3, size_t r4, TightDataPointStorageI* tdps);

void getSnapshotData_int16_1D(int16_t** data, size_t dataSeriesLength, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_int16_2D(int16_t** data, size_t r1, size_t r2, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_int16_3D(int16_t** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_int16_4D(int16_t** data, size_t r1, size_t r2, size_t r3, size_t r4, TightDataPointStorageI* tdps, int errBoundMode);

int SZ_decompress_args_int16(int16_t** newData, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, unsigned char* cmpBytes, size_t cmpSize);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZD_Int16_H  ----- */
