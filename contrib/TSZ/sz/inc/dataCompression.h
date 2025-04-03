/**
 *  @file dataCompression.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the dataCompression.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _DataCompression_H
#define _DataCompression_H

#ifdef __cplusplus
extern "C" {
#endif

#include "sz.h"
#include <stdio.h>
#include <stdbool.h>

#define computeMinMax(data) \
        for(i=1;i<size;i++)\
        {\
                data_ = data[i];\
                if(min>data_)\
                        min = data_;\
                else if(max<data_)\
                        max = data_;\
        }\


//dataCompression.c
double computeRangeSize_double(double* oriData, size_t size, double* valueRangeSize, double* medianValue);
float computeRangeSize_float(float* oriData, size_t size, float* valueRangeSize, float* medianValue);

double min_d(double a, double b);
double max_d(double a, double b);
float min_f(float a, float b);
float max_f(float a, float b);
double getRealPrecision_double(double valueRangeSize, int errBoundMode, double absErrBound, double relBoundRatio, int *status);
double getRealPrecision_float(float valueRangeSize, int errBoundMode, double absErrBound, double relBoundRatio, int *status);
double getRealPrecision_int(long valueRangeSize, int errBoundMode, double absErrBound, double relBoundRatio, int *status);
void symTransform_8bytes(unsigned char data[8]);
void symTransform_2bytes(unsigned char data[2]);
void symTransform_4bytes(unsigned char data[4]);

void compressSingleFloatValue(FloatValueCompressElement *vce, float tgtValue, float precision, float medianValue, 
		int reqLength, int reqBytesLength, int resiBitsLength);
void compressSingleDoubleValue(DoubleValueCompressElement *vce, double tgtValue, double precision, double medianValue, 
		int reqLength, int reqBytesLength, int resiBitsLength);
                              
int compIdenticalLeadingBytesCount_double(unsigned char* preBytes, unsigned char* curBytes);
int compIdenticalLeadingBytesCount_float(unsigned char* preBytes, unsigned char* curBytes);
void addExactData(DynamicByteArray *exactMidByteArray, DynamicIntArray *exactLeadNumArray, 
		DynamicIntArray *resiBitArray, LossyCompressionElement *lce);

int getPredictionCoefficients(int layers, int dimension, int **coeff_array, int *status);

int computeBlockEdgeSize_3D(int segmentSize);
int computeBlockEdgeSize_2D(int segmentSize);

int generateLossyCoefficients_float(float* oriData, double precision, size_t nbEle, int* reqBytesLength, int* resiBitsLength, float* medianValue, float* decData);
int compressExactDataArray_float(float* oriData, double precision, size_t nbEle, unsigned char** leadArray, unsigned char** midArray, unsigned char** resiArray, 
int reqLength, int reqBytesLength, int resiBitsLength, float medianValue);

void decompressExactDataArray_float(unsigned char* leadNum, unsigned char* exactMidBytes, unsigned char* residualMidBits, size_t nbEle, int reqLength, float medianValue, float** decData);

int generateLossyCoefficients_double(double* oriData, double precision, size_t nbEle, int* reqBytesLength, int* resiBitsLength, double* medianValue, double* decData);
int compressExactDataArray_double(double* oriData, double precision, size_t nbEle, unsigned char** leadArray, unsigned char** midArray, unsigned char** resiArray, 
int reqLength, int reqBytesLength, int resiBitsLength, double medianValue);

void decompressExactDataArray_double(unsigned char* leadNum, unsigned char* exactMidBytes, unsigned char* residualMidBits, size_t nbEle, int reqLength, double medianValue, double** decData);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _DataCompression_H  ----- */

