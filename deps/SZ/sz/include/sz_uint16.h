/**
 *  @file sz_uint16.h
 *  @author Sheng Di
 *  @date Nov, 2017
 *  @brief Header file for the sz_uint16.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZ_UInt16_H
#define _SZ_UInt16_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>

unsigned int optimize_intervals_uint16_1D(uint16_t *oriData, size_t dataLength, double realPrecision);
unsigned int optimize_intervals_uint16_2D(uint16_t *oriData, size_t r1, size_t r2, double realPrecision);
unsigned int optimize_intervals_uint16_3D(uint16_t *oriData, size_t r1, size_t r2, size_t r3, double realPrecision);
unsigned int optimize_intervals_uint16_4D(uint16_t *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision);
TightDataPointStorageI* SZ_compress_uint16_1D_MDQ(uint16_t *oriData, size_t dataLength, double realPrecision, int64_t valueRangeSize, int64_t minValue);
void SZ_compress_args_uint16_StoreOriData(uint16_t* oriData, size_t dataLength, TightDataPointStorageI* tdps, unsigned char** newByteData, size_t *outSize);
void SZ_compress_args_uint16_NoCkRngeNoGzip_1D(unsigned char** newByteData, uint16_t *oriData, 
size_t dataLength, double realPrecision, size_t *outSize, int64_t valueRangeSize, uint16_t minValue);
TightDataPointStorageI* SZ_compress_uint16_2D_MDQ(uint16_t *oriData, size_t r1, size_t r2, double realPrecision, int64_t valueRangeSize, int64_t minValue);
TightDataPointStorageI* SZ_compress_uint16_3D_MDQ(uint16_t *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, int64_t valueRangeSize, int64_t minValue);
void SZ_compress_args_uint16_NoCkRngeNoGzip_3D(unsigned char** newByteData, uint16_t *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t *outSize, int64_t valueRangeSize, int64_t minValue);
TightDataPointStorageI* SZ_compress_uint16_4D_MDQ(uint16_t *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision, int64_t valueRangeSize, int64_t minValue);
void SZ_compress_args_uint16_NoCkRngeNoGzip_4D(unsigned char** newByteData, uint16_t *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision, 
size_t *outSize, int64_t valueRangeSize, int64_t minValue);
void SZ_compress_args_uint16_withinRange(unsigned char** newByteData, uint16_t *oriData, size_t dataLength, size_t *outSize);

int SZ_compress_args_uint16_wRngeNoGzip(unsigned char** newByteData, uint16_t *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio);

int SZ_compress_args_uint16(unsigned char** newByteData, uint16_t *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_UInt16_H  ----- */

