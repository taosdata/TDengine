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
unsigned int optimize_intervals_float_2D(float *oriData, size_t r1, size_t r2, double realPrecision);
unsigned int optimize_intervals_float_3D(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision);
unsigned int optimize_intervals_float_4D(float *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision);

unsigned int optimize_intervals_and_compute_dense_position_float_1D(float *oriData, size_t dataLength, double realPrecision, float * dense_pos);
unsigned int optimize_intervals_and_compute_dense_position_float_3D(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, float * dense_pos);
unsigned int optimize_intervals_float_3D_with_freq_and_dense_pos(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, float * dense_pos, float * max_freq, float * mean_freq);
unsigned int optimize_intervals_float_3D_opt(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision);
unsigned int optimize_intervals_float_2D_opt(float *oriData, size_t r1, size_t r2, double realPrecision);
unsigned int optimize_intervals_float_1D_opt(float *oriData, size_t dataLength, double realPrecision);

unsigned int optimize_intervals_float_1D_opt_MSST19(float *oriData, size_t dataLength, double realPrecision);
unsigned int optimize_intervals_float_2D_opt_MSST19(float *oriData, size_t r1, size_t r2, double realPrecision);
unsigned int optimize_intervals_float_3D_opt_MSST19(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision);

TightDataPointStorageF* SZ_compress_float_1D_MDQ(float *oriData, 
size_t dataLength, float realPrecision, float valueRangeSize, float medianValue_f);

void SZ_compress_args_float_StoreOriData(float* oriData, size_t dataLength, unsigned char** newByteData, size_t *outSize);

char SZ_compress_args_float_NoCkRngeNoGzip_1D(int cmprType, unsigned char** newByteData, float *oriData, 
size_t dataLength, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f);

TightDataPointStorageF* SZ_compress_float_2D_MDQ(float *oriData, size_t r1, size_t r2, float realPrecision, float valueRangeSize, float medianValue_f);

char SZ_compress_args_float_NoCkRngeNoGzip_2D(int cmprType, unsigned char** newByteData, float *oriData, size_t r1, size_t r2, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f);

TightDataPointStorageF* SZ_compress_float_3D_MDQ(float *oriData, size_t r1, size_t r2, size_t r3, float realPrecision, float valueRangeSize, float medianValue_f);

char SZ_compress_args_float_NoCkRngeNoGzip_3D(int cmprType, unsigned char** newByteData, float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f);

size_t SZ_compress_float_1D_MDQ_RA_block(float * block_ori_data, float * mean, size_t dim_0, size_t block_dim_0, double realPrecision, int * type, float * unpredictable_data);
size_t SZ_compress_float_2D_MDQ_RA_block(float * block_ori_data, float * mean, size_t dim_0, size_t dim_1, size_t block_dim_0, size_t block_dim_1, double realPrecision, float * P0, float * P1, int * type, float * unpredictable_data);

size_t SZ_compress_float_1D_MDQ_RA_block_1D_pred(float * block_ori_data, float * mean, float dense_pos, size_t dim_0, size_t block_dim_0, double realPrecision, int * type, DynamicFloatArray * unpredictable_data);
size_t SZ_compress_float_2D_MDQ_RA_block_2D_pred(float * block_ori_data, float * mean, float dense_pos, size_t dim_0, size_t dim_1, size_t block_dim_0, size_t block_dim_1, double realPrecision, float * P0, float * P1, int * type, float * unpredictable_data);
size_t SZ_compress_float_3D_MDQ_RA_block(float * block_ori_data, float * mean, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, float realPrecision, float * P0, float * P1, int * type, float * unpredictable_data);
size_t SZ_compress_float_3D_MDQ_RA_block_3D_pred(float * block_ori_data, float * mean, float dense_pos, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, double realPrecision, float * P0, float * P1, int * type, float * unpredictable_data);
size_t SZ_compress_float_3D_MDQ_RA_block_adaptive(float * block_ori_data, float * mean, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, double realPrecision, float * P0, float * P1, int * type, float * unpredictable_data);
//unsigned short SZ_compress_float_3D_MDQ_RA_block_1D_pred(float * block_ori_data, float * mean, float dense_pos, size_t dim_0, size_t dim_1, size_t dim_2, int block_dim_0, int block_dim_1, int block_dim_2, double realPrecision, int * type, float * unpredictable_data);
size_t SZ_compress_float_3D_MDQ_RA_block_3D_pred_flush_after_compare(float * block_ori_data, float * mean, float dense_pos, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, double realPrecision, float * P0, float * P1, int * type, float * unpredictable_data);
size_t SZ_compress_float_3D_MDQ_RA_block_2_layers(float * block_ori_data, float * mean, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, double realPrecision, float * P0, float * P1, float * P_, int * type, float * unpredictable_data);
size_t SZ_compress_float_3D_MDQ_pred_by_regression(float * block_ori_data, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, double realPrecision, float * reg_params, int * type, float * unpredictable_data);
void SZ_blocked_regression(float * block_ori_data, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, float *params);
unsigned char * SZ_compress_float_3D_MDQ_RA_all_by_regression(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t * comp_size);
float SZ_compress_float_3D_MDQ_RA_block_no_mean(float * block_ori_data, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, double realPrecision, float * P0, float * P1, int * type, unsigned short * unpred_count, float * unpredictable_data);
float SZ_compress_float_3D_MDQ_pred_by_regression_with_err(float * block_ori_data, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, double realPrecision, float * reg_params, int * type, unsigned short * unpred_count, float * unpredictable_data);
unsigned char * SZ_compress_float_3D_MDQ_RA_blocked_with_regression(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t * comp_size);
void decompressDataSeries_float_3D_RA_blocked_with_regression(float** data, size_t r1, size_t r2, size_t r3, unsigned char* comp_data);

unsigned char * SZ_compress_float_1D_MDQ_RA(float *oriData, size_t r1, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_2D_MDQ_RA(float *oriData, size_t r1, size_t r2, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_2D_MDQ_nonblocked(float *oriData, size_t r1, size_t r2, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_3D_MDQ_RA(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_3D_MDQ_nonblocked(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_3D_MDQ_nonblocked_ori(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_3D_MDQ_nonblocked_multi_means(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_3D_MDQ_RA_multi_means(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_3D_MDQ_nonblocked_adaptive(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t * comp_size);

unsigned char * SZ_compress_float_2D_MDQ_decompression_random_access_with_blocked_regression(float *oriData, size_t r1, size_t r2, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_1D_MDQ_decompression_random_access_with_blocked_regression(float *oriData, size_t r1, double realPrecision, size_t * comp_size);

TightDataPointStorageF* SZ_compress_float_4D_MDQ(float *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision, float valueRangeSize, float medianValue_f);

char SZ_compress_args_float_NoCkRngeNoGzip_4D(unsigned char** newByteData, float *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f);

TightDataPointStorageF* SZ_compress_float_1D_MDQ_MSST19(float *oriData, 
size_t dataLength, double realPrecision, float valueRangeSize, float medianValue_f);
TightDataPointStorageF* SZ_compress_float_2D_MDQ_MSST19(float *oriData, size_t r1, size_t r2, double realPrecision, float valueRangeSize, float medianValue_f);
TightDataPointStorageF* SZ_compress_float_3D_MDQ_MSST19(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, float valueRangeSize, float medianValue_f);

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

void SZ_compress_args_float_NoCkRnge_2D_subblock(unsigned char* compressedBytes, float *oriData, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f,
size_t r2, size_t r1, size_t s2, size_t s1, size_t e2, size_t e1); 

void SZ_compress_args_float_NoCkRnge_3D_subblock(unsigned char* compressedBytes, float *oriData, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f,
size_t r3, size_t r2, size_t r1, size_t s3, size_t s2, size_t s1, size_t e3, size_t e2, size_t e1); 

void SZ_compress_args_float_NoCkRnge_4D_subblock(unsigned char* compressedBytes, float *oriData, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f,
size_t r4, size_t r3, size_t r2, size_t r1, size_t s4, size_t s3, size_t s2, size_t s1, size_t e4, size_t e3, size_t e2, size_t e1);

unsigned int optimize_intervals_float_1D_subblock(float *oriData, double realPrecision, size_t r1, size_t s1, size_t e1); 
unsigned int optimize_intervals_float_2D_subblock(float *oriData, double realPrecision, size_t r1, size_t r2, size_t s1, size_t s2, size_t e1, size_t e2); 
unsigned int optimize_intervals_float_3D_subblock(float *oriData, double realPrecision, size_t r1, size_t r2, size_t r3, size_t s1, size_t s2, size_t s3, size_t e1, size_t e2, size_t e3); 
unsigned int optimize_intervals_float_4D_subblock(float *oriData, double realPrecision, size_t r1, size_t r2, size_t r3, size_t r4, size_t s1, size_t s2, size_t s3, size_t s4, size_t e1, size_t e2, size_t e3, size_t e4);

TightDataPointStorageF* SZ_compress_float_1D_MDQ_subblock(float *oriData, double realPrecision, float valueRangeSize, float medianValue_f,
size_t r1, size_t s1, size_t e1); 

TightDataPointStorageF* SZ_compress_float_2D_MDQ_subblock(float *oriData, double realPrecision, float valueRangeSize, float medianValue_f,
size_t r1, size_t r2, size_t s1, size_t s2, size_t e1, size_t e2); 

TightDataPointStorageF* SZ_compress_float_3D_MDQ_subblock(float *oriData, double realPrecision, float valueRangeSize, float medianValue_f,
size_t r1, size_t r2, size_t r3, size_t s1, size_t s2, size_t s3, size_t e1, size_t e2, size_t e3); 

TightDataPointStorageF* SZ_compress_float_4D_MDQ_subblock(float *oriData, double realPrecision, float valueRangeSize, float medianValue_f,
size_t r1, size_t r2, size_t r3, size_t r4, size_t s1, size_t s2, size_t s3, size_t s4, size_t e1, size_t e2, size_t e3, size_t e4);


unsigned int optimize_intervals_float_2D_with_freq_and_dense_pos(float *oriData, size_t r1, size_t r2, double realPrecision, float * dense_pos, float * max_freq, float * mean_freq);
unsigned int optimize_intervals_float_3D_with_freq_and_dense_pos(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, float * dense_pos, float * max_freq, float * mean_freq);

unsigned char * SZ_compress_float_2D_MDQ_nonblocked_with_blocked_regression(float *oriData, size_t r1, size_t r2, float realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_3D_MDQ_nonblocked_with_blocked_regression(float *oriData, size_t r1, size_t r2, size_t r3, float realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_3D_MDQ_random_access_with_blocked_regression(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_3D_MDQ_decompression_random_access_with_blocked_regression(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t * comp_size);
#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_Float_H  ----- */

