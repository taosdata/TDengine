/**
 *  @file szd_float.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the szd_float.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZD_Float_H
#define _SZD_Float_H

#ifdef __cplusplus
extern "C" {
#endif

#include "TightDataPointStorageF.h"

void decompressDataSeries_float_1D(float** data, size_t dataSeriesLength, float* hist_data, TightDataPointStorageF* tdps);
void decompressDataSeries_float_2D(float** data, size_t r1, size_t r2, float* hist_data, TightDataPointStorageF* tdps);
void decompressDataSeries_float_3D(float** data, size_t r1, size_t r2, size_t r3, float* hist_data, TightDataPointStorageF* tdps);
void decompressDataSeries_float_4D(float** data, size_t r1, size_t r2, size_t r3, size_t r4, float* hist_data, TightDataPointStorageF* tdps);

void decompressDataSeries_float_1D_MSST19(float** data, size_t dataSeriesLength, TightDataPointStorageF* tdps);
void decompressDataSeries_float_2D_MSST19(float** data, size_t r1, size_t r2, TightDataPointStorageF* tdps);
void decompressDataSeries_float_3D_MSST19(float** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageF* tdps);

void getSnapshotData_float_1D(float** data, size_t dataSeriesLength, TightDataPointStorageF* tdps, int errBoundMode, int compressionType, float* hist_data);
void getSnapshotData_float_2D(float** data, size_t r1, size_t r2, TightDataPointStorageF* tdps, int errBoundMode, int compressionType, float* hist_data);
void getSnapshotData_float_3D(float** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageF* tdps, int errBoundMode, int compressionType, float* hist_data);
void getSnapshotData_float_4D(float** data, size_t r1, size_t r2, size_t r3, size_t r4, TightDataPointStorageF* tdps, int errBoundMode, int compressionType, float* hist_data);

size_t decompressDataSeries_float_1D_RA_block(float * data, float mean, size_t dim_0, size_t block_dim_0, double realPrecision, int * type, float * unpredictable_data);
size_t decompressDataSeries_float_2D_RA_block(float * data, float mean, size_t dim_0, size_t dim_1, size_t block_dim_0, size_t block_dim_1, double realPrecision, int * type, float * unpredictable_data);

int SZ_decompress_args_float(float** newData, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, unsigned char* cmpBytes, size_t cmpSize, int compressionType, float* hist_data);

size_t decompressDataSeries_float_3D_RA_block(float * data, float mean, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, double realPrecision, int * type, float * unpredictable_data);

void decompressDataSeries_float_1D_decompression_given_areas_with_blocked_regression(float** data, size_t r1, size_t s1, size_t e1, unsigned char* comp_data);

void decompressDataSeries_float_2D_nonblocked_with_blocked_regression(float** data, size_t r1, size_t r2, unsigned char* comp_data, float* hist_data);
void decompressDataSeries_float_2D_decompression_given_areas_with_blocked_regression(float** data, size_t r1, size_t r2, size_t s1, size_t s2, size_t e1, size_t e2, unsigned char* comp_data);
void decompressDataSeries_float_3D_nonblocked_with_blocked_regression(float** data, size_t r1, size_t r2, size_t r3, unsigned char* comp_data, float* hist_data);
void decompressDataSeries_float_3D_random_access_with_blocked_regression(float** data, size_t r1, size_t r2, size_t r3, unsigned char* comp_data);
void decompressDataSeries_float_3D_decompression_random_access_with_blocked_regression(float** data, size_t r1, size_t r2, size_t r3, unsigned char* comp_data);
void decompressDataSeries_float_3D_decompression_given_areas_with_blocked_regression(float** data, size_t r1, size_t r2, size_t r3, size_t s1, size_t s2, size_t s3, size_t e1, size_t e2, size_t e3, unsigned char* comp_data);
int SZ_decompress_args_randomaccess_float(float** newData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, 
size_t s5, size_t s4, size_t s3, size_t s2, size_t s1, // start point
size_t e5, size_t e4, size_t e3, size_t e2, size_t e1, // end point
unsigned char* cmpBytes, size_t cmpSize);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZD_Float_H  ----- */
