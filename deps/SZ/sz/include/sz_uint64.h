/**
 *  @file sz_uint64.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the sz_uint64.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZ_UInt64_H
#define _SZ_UInt64_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>

unsigned int optimize_intervals_uint64_1D(uint64_t *oriData, size_t dataLength, double realPrecision);
unsigned int optimize_intervals_uint64_2D(uint64_t *oriData, size_t r1, size_t r2, double realPrecision);
unsigned int optimize_intervals_uint64_3D(uint64_t *oriData, size_t r1, size_t r2, size_t r3, double realPrecision);
unsigned int optimize_intervals_uint64_4D(uint64_t *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision);
TightDataPointStorageI* SZ_compress_uint64_1D_MDQ(uint64_t *oriData, size_t dataLength, double realPrecision, uint64_t valueRangeSize, uint64_t minValue);
void SZ_compress_args_uint64_StoreOriData(uint64_t* oriData, size_t dataLength, TightDataPointStorageI* tdps, unsigned char** newByteData, size_t *outSize);
void SZ_compress_args_uint64_NoCkRngeNoGzip_1D(unsigned char** newByteData, uint64_t *oriData, 
size_t dataLength, double realPrecision, size_t *outSize, uint64_t valueRangeSize, uint64_t minValue);
TightDataPointStorageI* SZ_compress_uint64_2D_MDQ(uint64_t *oriData, size_t r1, size_t r2, double realPrecision, uint64_t valueRangeSize, uint64_t minValue);
TightDataPointStorageI* SZ_compress_uint64_3D_MDQ(uint64_t *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, uint64_t valueRangeSize, uint64_t minValue);
void SZ_compress_args_uint64_NoCkRngeNoGzip_3D(unsigned char** newByteData, uint64_t *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t *outSize, uint64_t valueRangeSize, uint64_t minValue);
TightDataPointStorageI* SZ_compress_uint64_4D_MDQ(uint64_t *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision, uint64_t valueRangeSize, uint64_t minValue);
void SZ_compress_args_uint64_NoCkRngeNoGzip_4D(unsigned char** newByteData, uint64_t *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision, 
size_t *outSize, uint64_t valueRangeSize, uint64_t minValue);
void SZ_compress_args_uint64_withinRange(unsigned char** newByteData, uint64_t *oriData, size_t dataLength, size_t *outSize);

int SZ_compress_args_uint64_wRngeNoGzip(unsigned char** newByteData, uint64_t *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio);

int SZ_compress_args_uint64(unsigned char** newByteData, uint64_t *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_UInt64_H  ----- */

