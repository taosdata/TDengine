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
#include <stdbool.h>

unsigned char* SZ_skip_compress_double(double* data, size_t dataLength, size_t* outSize);

void computeReqLength_double(double realPrecision, short radExpo, int* reqLength, double* medianValue);

unsigned int optimize_intervals_double_1D(double *oriData, size_t dataLength, double realPrecision);

unsigned int optimize_intervals_double_1D_opt(double *oriData, size_t dataLength, double realPrecision);


TightDataPointStorageD* SZ_compress_double_1D_MDQ(double *oriData, 
size_t dataLength, double realPrecision, double valueRangeSize, double medianValue_d);
void SZ_compress_args_double_StoreOriData(double* oriData, size_t dataLength, unsigned char* newByteData, size_t *outSize);

bool SZ_compress_args_double_NoCkRngeNoGzip_1D( unsigned char* newByteData, double *oriData, size_t dataLength, double realPrecision, size_t *outSize, double valueRangeSize, double medianValue_d);

void SZ_compress_args_double_withinRange(unsigned char* newByteData, double *oriData, size_t dataLength, size_t *outSize);


int SZ_compress_args_double(double *oriData, size_t r1, unsigned char* newByteData, size_t *outSize, sz_params* params);



#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_Double_H  ----- */

