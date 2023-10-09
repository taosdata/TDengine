/**
 *  @file sz_float.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the sz_float.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZ_Float_H
#define _SZ_Float_H

#ifdef __cplusplus
extern "C" {
#endif

void computeReqLength_float(double realPrecision, short radExpo, int* reqLength, float* medianValue);

unsigned int optimize_intervals_float_1D(float *oriData, size_t dataLength, double realPrecision);

unsigned int optimize_intervals_float_1D_opt(float *oriData, size_t dataLength, double realPrecision);

TightDataPointStorageF* SZ_compress_float_1D_MDQ(float *oriData, 
size_t dataLength, float realPrecision, float valueRangeSize, float medianValue_f);

bool SZ_compress_args_float_NoCkRngeNoGzip_1D( unsigned char* newByteData, float *oriData, 
size_t dataLength, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f);


void SZ_compress_args_float_withinRange(unsigned char* newByteData, float *oriData, size_t dataLength, size_t *outSize);


int SZ_compress_args_float(float *oriData, size_t r1, unsigned char* newByteData, size_t *outSize, sz_params* params);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_Float_H  ----- */

