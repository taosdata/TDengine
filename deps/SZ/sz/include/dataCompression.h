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
int computeByteSizePerIntValue(long valueRangeSize);
long computeRangeSize_int(void* oriData, int dataType, size_t size, int64_t* valueRangeSize);
double computeRangeSize_double(double* oriData, size_t size, double* valueRangeSize, double* medianValue);
float computeRangeSize_float(float* oriData, size_t size, float* valueRangeSize, float* medianValue);
float computeRangeSize_float_MSST19(float* oriData, size_t size, float* valueRangeSize, float* medianValue, unsigned char * signs, bool* positive, float* nearZero);
double computeRangeSize_double_MSST19(double* oriData, size_t size, double* valueRangeSize, double* medianValue, unsigned char * signs, bool* positive, double* nearZero);

double computeRangeSize_double_subblock(double* oriData, double* valueRangeSize, double* medianValue,
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1,
size_t s5, size_t s4, size_t s3, size_t s2, size_t s1,
size_t e5, size_t e4, size_t e3, size_t e2, size_t e1);
float computeRangeSize_float_subblock(float* oriData, float* valueRangeSize, float* medianValue,
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1,
size_t s5, size_t s4, size_t s3, size_t s2, size_t s1,
size_t e5, size_t e4, size_t e3, size_t e2, size_t e1);
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

void compressInt8Value(int8_t tgtValue, int8_t minValue, int byteSize, unsigned char* bytes);
void compressInt16Value(int16_t tgtValue, int16_t minValue, int byteSize, unsigned char* bytes);
void compressInt32Value(int32_t tgtValue, int32_t minValue, int byteSize, unsigned char* bytes);
void compressInt64Value(int64_t tgtValue, int64_t minValue, int byteSize, unsigned char* bytes);

void compressUInt8Value(uint8_t tgtValue, uint8_t minValue, int byteSize, unsigned char* bytes);
void compressUInt16Value(uint16_t tgtValue, uint16_t minValue, int byteSize, unsigned char* bytes);
void compressUInt32Value(uint32_t tgtValue, uint32_t minValue, int byteSize, unsigned char* bytes);
void compressUInt64Value(uint64_t tgtValue, uint64_t minValue, int byteSize, unsigned char* bytes);

void compressSingleFloatValue(FloatValueCompressElement *vce, float tgtValue, float precision, float medianValue, 
		int reqLength, int reqBytesLength, int resiBitsLength);
void compressSingleFloatValue_MSST19(FloatValueCompressElement *vce, float tgtValue, float precision, int reqLength, int reqBytesLength, int resiBitsLength);
void compressSingleDoubleValue(DoubleValueCompressElement *vce, double tgtValue, double precision, double medianValue, 
		int reqLength, int reqBytesLength, int resiBitsLength);
void compressSingleDoubleValue_MSST19(DoubleValueCompressElement *vce, double tgtValue, double precision, int reqLength, int reqBytesLength, int resiBitsLength);
                              
int compIdenticalLeadingBytesCount_double(unsigned char* preBytes, unsigned char* curBytes);
int compIdenticalLeadingBytesCount_float(unsigned char* preBytes, unsigned char* curBytes);
void addExactData(DynamicByteArray *exactMidByteArray, DynamicIntArray *exactLeadNumArray, 
		DynamicIntArray *resiBitArray, LossyCompressionElement *lce);

int getPredictionCoefficients(int layers, int dimension, int **coeff_array, int *status);

int computeBlockEdgeSize_3D(int segmentSize);
int computeBlockEdgeSize_2D(int segmentSize);
int initRandomAccessBytes(unsigned char* raBytes);

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

