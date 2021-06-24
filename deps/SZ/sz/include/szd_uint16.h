/**
 *  @file szd_uint16.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the szd_uint16.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZD_UInt16_H
#define _SZD_UInt16_H

#ifdef __cplusplus
extern "C" {
#endif

#include "TightDataPointStorageI.h"

#define SZ_UINT16_MIN 0
#define SZ_UINT16_MAX 65535

void decompressDataSeries_uint16_1D(uint16_t** data, size_t dataSeriesLength, TightDataPointStorageI* tdps);
void decompressDataSeries_uint16_2D(uint16_t** data, size_t r1, size_t r2, TightDataPointStorageI* tdps);
void decompressDataSeries_uint16_3D(uint16_t** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageI* tdps);
void decompressDataSeries_uint16_4D(uint16_t** data, size_t r1, size_t r2, size_t r3, size_t r4, TightDataPointStorageI* tdps);

void getSnapshotData_uint16_1D(uint16_t** data, size_t dataSeriesLength, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_uint16_2D(uint16_t** data, size_t r1, size_t r2, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_uint16_3D(uint16_t** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageI* tdps, int errBoundMode);
void getSnapshotData_uint16_4D(uint16_t** data, size_t r1, size_t r2, size_t r3, size_t r4, TightDataPointStorageI* tdps, int errBoundMode);

int SZ_decompress_args_uint16(uint16_t** newData, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, unsigned char* cmpBytes, size_t cmpSize);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZD_Int16_H  ----- */
