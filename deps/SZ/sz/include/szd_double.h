/**
 *  @file szd_double.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the szd_double.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZD_Double_H
#define _SZD_Double_H

#ifdef __cplusplus
extern "C" {
#endif

#include "TightDataPointStorageD.h"

void decompressDataSeries_double_1D(double** data, size_t dataSeriesLength, double* hist_data, TightDataPointStorageD* tdps);
void decompressDataSeries_double_2D(double** data, size_t r1, size_t r2, double* hist_data, TightDataPointStorageD* tdps);
void decompressDataSeries_double_3D(double** data, size_t r1, size_t r2, size_t r3, double* hist_data, TightDataPointStorageD* tdps);
void decompressDataSeries_double_4D(double** data, size_t r1, size_t r2, size_t r3, size_t r4, double* hist_data, TightDataPointStorageD* tdps);

void decompressDataSeries_double_1D_MSST19(double** data, size_t dataSeriesLength, TightDataPointStorageD* tdps);
void decompressDataSeries_double_2D_MSST19(double** data, size_t r1, size_t r2, TightDataPointStorageD* tdps);
void decompressDataSeries_double_3D_MSST19(double** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageD* tdps);

void getSnapshotData_double_1D(double** data, size_t dataSeriesLength, TightDataPointStorageD* tdps, int errBoundMode, int compressionType, double* hist_data);
void getSnapshotData_double_2D(double** data, size_t r1, size_t r2, TightDataPointStorageD* tdps, int errBoundMode, int compressionType, double* hist_data);
void getSnapshotData_double_3D(double** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageD* tdps, int errBoundMode, int compressionType, double* hist_data);
void getSnapshotData_double_4D(double** data, size_t r1, size_t r2, size_t r3, size_t r4, TightDataPointStorageD* tdps, int errBoundMode, int compressionType, double* hist_data);
void decompressDataSeries_double_2D_nonblocked_with_blocked_regression(double** data, size_t r1, size_t r2, unsigned char* comp_data, double* hist_data);
void decompressDataSeries_double_3D_nonblocked_with_blocked_regression(double** data, size_t r1, size_t r2, size_t r3, unsigned char* comp_data, double* hist_data);

size_t decompressDataSeries_double_3D_RA_block(double * data, double mean, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, double realPrecision, int * type, double * unpredictable_data);

int SZ_decompress_args_double(double** newData, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, unsigned char* cmpBytes, size_t cmpSize, int compressionType, double* hist_data);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZD_Double_H  ----- */
