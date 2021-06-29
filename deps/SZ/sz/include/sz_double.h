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

unsigned int optimize_intervals_double_1D_opt(double *oriData, size_t dataLength, double realPrecision);


unsigned int optimize_intervals_double_1D_opt_MSST19(double *oriData, size_t dataLength, double realPrecision);
TightDataPointStorageD* SZ_compress_double_1D_MDQ(double *oriData, 
size_t dataLength, double realPrecision, double valueRangeSize, double medianValue_d);
void SZ_compress_args_double_StoreOriData(double* oriData, size_t dataLength, unsigned char** newByteData, size_t *outSize);

char SZ_compress_args_double_NoCkRngeNoGzip_1D(int cmprType, unsigned char** newByteData, double *oriData, size_t dataLength, double realPrecision, size_t *outSize, double valueRangeSize, double medianValue_d);

TightDataPointStorageD* SZ_compress_double_1D_MDQ_MSST19(double *oriData, size_t dataLength, double realPrecision, double valueRangeSize, double medianValue_f);

void SZ_compress_args_double_withinRange(unsigned char** newByteData, double *oriData, size_t dataLength, size_t *outSize);

/*int SZ_compress_args_double_wRngeNoGzip(unsigned char** newByteData, double *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio, double pwrErrRatio);*/

int SZ_compress_args_double(int cmprType, int withRegression, unsigned char** newByteData, double *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio, double pwRelBoundRatio);

void SZ_compress_args_double_NoCkRnge_1D_subblock(unsigned char* compressedBytes, double *oriData, double realPrecision, size_t *outSize, double valueRangeSize, double medianValue_d,
size_t r1, size_t s1, size_t e1);

unsigned int optimize_intervals_double_1D_subblock(double *oriData, double realPrecision, size_t r1, size_t s1, size_t e1);

TightDataPointStorageD* SZ_compress_double_1D_MDQ_subblock(double *oriData, double realPrecision, double valueRangeSize, double medianValue_d,
size_t r1, size_t s1, size_t e1);


#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_Double_H  ----- */

