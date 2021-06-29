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

void SZ_compress_args_float_StoreOriData(float* oriData, size_t dataLength, unsigned char** newByteData, size_t *outSize);

char SZ_compress_args_float_NoCkRngeNoGzip_1D(int cmprType, unsigned char** newByteData, float *oriData, 
size_t dataLength, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f);

size_t SZ_compress_float_1D_MDQ_RA_block(float * block_ori_data, float * mean, size_t dim_0, size_t block_dim_0, double realPrecision, int * type, float * unpredictable_data);

size_t SZ_compress_float_1D_MDQ_RA_block_1D_pred(float * block_ori_data, float * mean, float dense_pos, size_t dim_0, size_t block_dim_0, double realPrecision, int * type, DynamicFloatArray * unpredictable_data);
void SZ_blocked_regression(float * block_ori_data, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, float *params);

unsigned char * SZ_compress_float_1D_MDQ_RA(float *oriData, size_t r1, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_1D_MDQ_decompression_random_access_with_blocked_regression(float *oriData, size_t r1, double realPrecision, size_t * comp_size);


TightDataPointStorageF* SZ_compress_float_1D_MDQ_MSST19(float *oriData, 
size_t dataLength, double realPrecision, float valueRangeSize, float medianValue_f);

void SZ_compress_args_float_withinRange(unsigned char** newByteData, float *oriData, size_t dataLength, size_t *outSize);

/*int SZ_compress_args_float_wRngeNoGzip(unsigned char** newByteData, float *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio, double pwrErrRatio);*/

int SZ_compress_args_float(int cmprType, int withRegression, unsigned char** newByteData, float *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio, double pwRelBoundRatio);

int SZ_compress_args_float_subblock(unsigned char* compressedBytes, float *oriData,
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1,
size_t s5, size_t s4, size_t s3, size_t s2, size_t s1,
size_t e5, size_t e4, size_t e3, size_t e2, size_t e1,
size_t *outSize, int errBoundMode, double absErr_Bound, double relBoundRatio);

void SZ_compress_args_float_NoCkRnge_1D_subblock(unsigned char* compressedBytes, float *oriData, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f,
size_t r1, size_t s1, size_t e1); 


unsigned int optimize_intervals_float_1D_subblock(float *oriData, double realPrecision, size_t r1, size_t s1, size_t e1); 

TightDataPointStorageF* SZ_compress_float_1D_MDQ_subblock(float *oriData, double realPrecision, float valueRangeSize, float medianValue_f,
size_t r1, size_t s1, size_t e1); 

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_Float_H  ----- */

