/**
 *  @file sz_float.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the sz_float.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */
#include "DynamicFloatArray.h"

#ifndef _SZ_Float_H
#define _SZ_Float_H

#ifdef __cplusplus
extern "C" {
#endif
unsigned char* SZ_skip_compress_float(float* data, size_t dataLength, size_t* outSize);

void computeReqLength_float(double realPrecision, short radExpo, int* reqLength, float* medianValue);
short computeReqLength_float_MSST19(double realPrecision);

unsigned int optimize_intervals_float_1D(float *oriData, size_t dataLength, double realPrecision);

unsigned int optimize_intervals_and_compute_dense_position_float_1D(float *oriData, size_t dataLength, double realPrecision, float * dense_pos);
unsigned int optimize_intervals_float_1D_opt(float *oriData, size_t dataLength, double realPrecision);

unsigned int optimize_intervals_float_1D_opt_MSST19(float *oriData, size_t dataLength, double realPrecision);

TightDataPointStorageF* SZ_compress_float_1D_MDQ(float *oriData, 
size_t dataLength, float realPrecision, float valueRangeSize, float medianValue_f);

void SZ_compress_args_float_StoreOriData(float* oriData, size_t dataLength, unsigned char* newByteData, size_t *outSize);

bool SZ_compress_args_float_NoCkRngeNoGzip_1D( unsigned char* newByteData, float *oriData, 
size_t dataLength, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f);

size_t SZ_compress_float_1D_MDQ_RA_block(float * block_ori_data, float * mean, size_t dim_0, size_t block_dim_0, double realPrecision, int * type, float * unpredictable_data);

size_t SZ_compress_float_1D_MDQ_RA_block_1D_pred(float * block_ori_data, float * mean, float dense_pos, size_t dim_0, size_t block_dim_0, double realPrecision, int * type, DynamicFloatArray * unpredictable_data);
void SZ_blocked_regression(float * block_ori_data, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, float *params);

unsigned char * SZ_compress_float_1D_MDQ_RA(float *oriData, size_t r1, double realPrecision, size_t * comp_size);


TightDataPointStorageF* SZ_compress_float_1D_MDQ_MSST19(float *oriData, 
size_t dataLength, double realPrecision, float valueRangeSize, float medianValue_f);

void SZ_compress_args_float_withinRange(unsigned char* newByteData, float *oriData, size_t dataLength, size_t *outSize);


int SZ_compress_args_float(float *oriData, size_t r1, unsigned char* newByteData, size_t *outSize, sz_params* params);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_Float_H  ----- */

