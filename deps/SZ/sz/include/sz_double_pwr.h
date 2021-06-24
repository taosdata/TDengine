/**
 *  @file sz_double.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the sz_double.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZ_Double_PWR_H
#define _SZ_Double_PWR_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdbool.h>

void compute_segment_precisions_double_1D(double *oriData, size_t dataLength, double* pwrErrBound, unsigned char* pwrErrBoundBytes, double globalPrecision);
unsigned int optimize_intervals_double_1D_pwr(double *oriData, size_t dataLength, double* pwrErrBound); 
void compute_segment_precisions_double_2D(double *oriData, double* pwrErrBound, 
size_t r1, size_t r2, size_t R2, size_t edgeSize, unsigned char* pwrErrBoundBytes, double Min, double Max, double globalPrecision);
unsigned int optimize_intervals_double_2D_pwr(double *oriData, size_t r1, size_t r2, size_t R2, size_t edgeSize, double* pwrErrBound);
void compute_segment_precisions_double_3D(double *oriData, double* pwrErrBound, 
size_t r1, size_t r2, size_t r3, size_t R2, size_t R3, size_t edgeSize, unsigned char* pwrErrBoundBytes, double Min, double Max, double globalPrecision);
unsigned int optimize_intervals_double_3D_pwr(double *oriData, size_t r1, size_t r2, size_t r3, size_t R2, size_t R3, size_t edgeSize, double* pwrErrBound);
void SZ_compress_args_double_NoCkRngeNoGzip_1D_pwr(unsigned char** newByteData, double *oriData, double globalPrecision, size_t dataLength, size_t *outSize, double min, double max);
void SZ_compress_args_double_NoCkRngeNoGzip_2D_pwr(unsigned char** newByteData, double *oriData, double globalPrecision, size_t r1, size_t r2,
size_t *outSize, double min, double max);
void SZ_compress_args_double_NoCkRngeNoGzip_3D_pwr(unsigned char** newByteData, double *oriData, double globalPrecision, 
size_t r1, size_t r2, size_t r3, size_t *outSize, double min, double max);

void createRangeGroups_double(double** posGroups, double** negGroups, int** posFlags, int** negFlags);
void compressGroupIDArray_double(char* groupID, TightDataPointStorageD* tdps);
TightDataPointStorageD* SZ_compress_double_1D_MDQ_pwrGroup(double* oriData, size_t dataLength, int errBoundMode, 
double absErrBound, double relBoundRatio, double pwrErrRatio, double valueRangeSize, double medianValue_f);
void SZ_compress_args_double_NoCkRngeNoGzip_1D_pwrgroup(unsigned char** newByteData, double *oriData,
size_t dataLength, double absErrBound, double relBoundRatio, double pwrErrRatio, double valueRangeSize, double medianValue_f, size_t *outSize);

void SZ_compress_args_double_NoCkRngeNoGzip_1D_pwr_pre_log(unsigned char** newByteData, double *oriData, double globalPrecision, size_t dataLength, size_t *outSize, double min, double max);
void SZ_compress_args_double_NoCkRngeNoGzip_2D_pwr_pre_log(unsigned char** newByteData, double *oriData, double globalPrecision, size_t r1, size_t r2, size_t *outSize, double min, double max);
void SZ_compress_args_double_NoCkRngeNoGzip_3D_pwr_pre_log(unsigned char** newByteData, double *oriData, double globalPrecision, size_t r1, size_t r2, size_t r3, size_t *outSize, double min, double max);

void SZ_compress_args_double_NoCkRngeNoGzip_1D_pwr_pre_log_MSST19(unsigned char** newByteData, double *oriData, double pwrErrRatio, size_t dataLength, size_t *outSize, double valueRangeSize, double medianValue_f,
																unsigned char* signs, bool* positive, double min, double max, double nearZero);
void SZ_compress_args_double_NoCkRngeNoGzip_2D_pwr_pre_log_MSST19(unsigned char** newByteData, double *oriData, double pwrErrRatio, size_t r1, size_t r2, size_t *outSize, double valueRangeSize,
																unsigned char* signs, bool* positive, double min, double max, double nearZero);
void SZ_compress_args_double_NoCkRngeNoGzip_3D_pwr_pre_log_MSST19(unsigned char** newByteData, double *oriData, double pwrErrRatio, size_t r1, size_t r2, size_t r3, size_t *outSize, double valueRangeSize, 
																unsigned char* signs, bool* positive, double min, double max, double nearZero);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_Double_PWR_H  ----- */

