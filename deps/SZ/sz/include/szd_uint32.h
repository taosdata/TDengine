/**
 *  @file szd_uint32.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the szd_uint32.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZD_UInt32_H
#define _SZD_UInt32_H

#ifdef __cplusplus
extern "C" {
#endif

#include "TightDataPointStorageI.h"

#define SZ_UINT32_MIN 0
#define SZ_UINT32_MAX 4294967295

void decompressDataSeries_uint32_1D(uint32_t** data, size_t dataSeriesLength, TightDataPointStorageI* tdps);
void decompressDataSeries_uint32_2D(uint32_t** data, size_t r1, size_t r2, TightDataPointStorageI* tdps);
void decompressDataSeries_uint32_3D(uint32_t** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageI* tdps);
void decompressDataSeries_uint32_4D(uint32_t** data, size_t r1, size_t r2, size_t r3, size_t r4, TightDataPointStorageI* tdps);

void getSnapshotData_uint32_1D(uint32_t** data, size_t dataSeriesLength, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_uint32_2D(uint32_t** data, size_t r1, size_t r2, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_uint32_3D(uint32_t** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_uint32_4D(uint32_t** data, size_t r1, size_t r2, size_t r3, size_t r4, TightDataPointStorageI* tdps, int errBoundMode);

int SZ_decompress_args_uint32(uint32_t** newData, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, unsigned char* cmpBytes, size_t cmpSize);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZD_UInt32_H  ----- */
