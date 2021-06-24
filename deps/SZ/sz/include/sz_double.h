/**
 *  @file sz_double.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the sz_double.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZ_Double_H
#define _SZ_Double_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
unsigned char* SZ_skip_compress_double(double* data, size_t dataLength, size_t* outSize);

void computeReqLength_double(double realPrecision, short radExpo, int* reqLength, double* medianValue);
short computeReqLength_double_MSST19(double realPrecision);

unsigned int optimize_intervals_double_1D(double *oriData, size_t dataLength, double realPrecision);
unsigned int optimize_intervals_double_2D(double *oriData, size_t r1, size_t r2, double realPrecision);
unsigned int optimize_intervals_double_3D(double *oriData, size_t r1, size_t r2, size_t r3, double realPrecision);
unsigned int optimize_intervals_double_4D(double *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision);

unsigned int optimize_intervals_double_3D_opt(double *oriData, size_t r1, size_t r2, size_t r3, double realPrecision);
unsigned int optimize_intervals_double_2D_opt(double *oriData, size_t r1, size_t r2, double realPrecision);
unsigned int optimize_intervals_double_1D_opt(double *oriData, size_t dataLength, double realPrecision);

size_t SZ_compress_double_3D_MDQ_RA_block(double * block_ori_data, double * mean, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, double realPrecision, double * P0, double * P1, int * type, double * unpredictable_data);

unsigned int optimize_intervals_double_1D_opt_MSST19(double *oriData, size_t dataLength, double realPrecision);
unsigned int optimize_intervals_double_2D_opt_MSST19(double *oriData, size_t r1, size_t r2, double realPrecision);
unsigned int optimize_intervals_double_3D_opt_MSST19(double *oriData, size_t r1, size_t r2, size_t r3, double realPrecision);
TightDataPointStorageD* SZ_compress_double_1D_MDQ(double *oriData, 
size_t dataLength, double realPrecision, double valueRangeSize, double medianValue_d);
void SZ_compress_args_double_StoreOriData(double* oriData, size_t dataLength, unsigned char** newByteData, size_t *outSize);

char SZ_compress_args_double_NoCkRngeNoGzip_1D(int cmprType, unsigned char** newByteData, double *oriData, size_t dataLength, double realPrecision, size_t *outSize, double valueRangeSize, double medianValue_d);

TightDataPointStorageD* SZ_compress_double_2D_MDQ(double *oriData, size_t r1, size_t r2, double realPrecision, double valueRangeSize, double medianValue_d);
char SZ_compress_args_double_NoCkRngeNoGzip_2D(int cmprType, unsigned char** newByteData, double *oriData, size_t r1, size_t r2, double realPrecision, size_t *outSize, double valueRangeSize, double medianValue_d);

TightDataPointStorageD* SZ_compress_double_3D_MDQ(double *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, double valueRangeSize, double medianValue_d);
char SZ_compress_args_double_NoCkRngeNoGzip_3D(int cmprType, unsigned char** newByteData, double *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t *outSize, double valueRangeSize, double medianValue_d);

TightDataPointStorageD* SZ_compress_double_4D_MDQ(double *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision, double valueRangeSize, double medianValue_d);
char SZ_compress_args_double_NoCkRngeNoGzip_4D(unsigned char** newByteData, double *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision, size_t *outSize, double valueRangeSize, double medianValue_d);

TightDataPointStorageD* SZ_compress_double_1D_MDQ_MSST19(double *oriData, size_t dataLength, double realPrecision, double valueRangeSize, double medianValue_f);
TightDataPointStorageD* SZ_compress_double_2D_MDQ_MSST19(double *oriData, size_t r1, size_t r2, double realPrecision, double valueRangeSize, double medianValue_f);
TightDataPointStorageD* SZ_compress_double_3D_MDQ_MSST19(double *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, double valueRangeSize, double medianValue_f);

void SZ_compress_args_double_withinRange(unsigned char** newByteData, double *oriData, size_t dataLength, size_t *outSize);

/*int SZ_compress_args_double_wRngeNoGzip(unsigned char** newByteData, double *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio, double pwrErrRatio);*/

int SZ_compress_args_double(int cmprType, int withRegression, unsigned char** newByteData, double *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio, double pwRelBoundRatio);

void SZ_compress_args_double_NoCkRnge_1D_subblock(unsigned char* compressedBytes, double *oriData, double realPrecision, size_t *outSize, double valueRangeSize, double medianValue_d,
size_t r1, size_t s1, size_t e1);
void SZ_compress_args_double_NoCkRnge_2D_subblock(unsigned char* compressedBytes, double *oriData, double realPrecision, size_t *outSize, double valueRangeSize, double medianValue_d,
size_t r2, size_t r1, size_t s2, size_t s1, size_t e2, size_t e1);
void SZ_compress_args_double_NoCkRnge_3D_subblock(unsigned char* compressedBytes, double *oriData, double realPrecision, size_t *outSize, double valueRangeSize, double medianValue_d,
size_t r3, size_t r2, size_t r1, size_t s3, size_t s2, size_t s1, size_t e3, size_t e2, size_t e1);
void SZ_compress_args_double_NoCkRnge_4D_subblock(unsigned char* compressedBytes, double *oriData, double realPrecision, size_t *outSize, double valueRangeSize, double medianValue_d,
size_t r4, size_t r3, size_t r2, size_t r1, size_t s4, size_t s3, size_t s2, size_t s1, size_t e4, size_t e3, size_t e2, size_t e1);

unsigned int optimize_intervals_double_1D_subblock(double *oriData, double realPrecision, size_t r1, size_t s1, size_t e1);
unsigned int optimize_intervals_double_2D_subblock(double *oriData, double realPrecision, size_t r1, size_t r2, size_t s1, size_t s2, size_t e1, size_t e2);
unsigned int optimize_intervals_double_3D_subblock(double *oriData, double realPrecision, size_t r1, size_t r2, size_t r3, size_t s1, size_t s2, size_t s3, size_t e1, size_t e2, size_t e3);
unsigned int optimize_intervals_double_4D_subblock(double *oriData, double realPrecision, size_t r1, size_t r2, size_t r3, size_t r4, size_t s1, size_t s2, size_t s3, size_t s4, size_t e1, size_t e2, size_t e3, size_t e4);

TightDataPointStorageD* SZ_compress_double_1D_MDQ_subblock(double *oriData, double realPrecision, double valueRangeSize, double medianValue_d,
size_t r1, size_t s1, size_t e1);
TightDataPointStorageD* SZ_compress_double_2D_MDQ_subblock(double *oriData, double realPrecision, double valueRangeSize, double medianValue_d,
size_t r1, size_t r2, size_t s1, size_t s2, size_t e1, size_t e2);
TightDataPointStorageD* SZ_compress_double_3D_MDQ_subblock(double *oriData, double realPrecision, double valueRangeSize, double medianValue_d,
size_t r1, size_t r2, size_t r3, size_t s1, size_t s2, size_t s3, size_t e1, size_t e2, size_t e3);
TightDataPointStorageD* SZ_compress_double_4D_MDQ_subblock(double *oriData, double realPrecision, double valueRangeSize, double medianValue_d,
size_t r1, size_t r2, size_t r3, size_t r4, size_t s1, size_t s2, size_t s3, size_t s4, size_t e1, size_t e2, size_t e3, size_t e4);

unsigned int optimize_intervals_double_2D_with_freq_and_dense_pos(double *oriData, size_t r1, size_t r2, double realPrecision, double * dense_pos, double * max_freq, double * mean_freq);
unsigned int optimize_intervals_double_3D_with_freq_and_dense_pos(double *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, double * dense_pos, double * max_freq, double * mean_freq);
unsigned char * SZ_compress_double_2D_MDQ_nonblocked_with_blocked_regression(double *oriData, size_t r1, size_t r2, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_double_3D_MDQ_nonblocked_with_blocked_regression(double *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t * comp_size);


#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_Double_H  ----- */

