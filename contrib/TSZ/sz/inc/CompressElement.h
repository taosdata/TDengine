/**
 *  @file CompressElement.h
 *  @author Sheng Di
 *  @date April, 2016
 *  @brief Header file for Compress Elements such as DoubleCompressELement.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdint.h>

#ifndef _CompressElement_H
#define _CompressElement_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DoubleValueCompressElement
{
	double data;
	long curValue;
	unsigned char curBytes[8]; //big_endian
	int reqBytesLength;
	int resiBitsLength;
} DoubleValueCompressElement;

typedef struct FloatValueCompressElement
{
	float data;  // diffValue + medianValue
	int curValue; // diff int value
	unsigned char curBytes[4]; // dif bytes value diffValue->iValue big_endian
	int reqBytesLength;
	int resiBitsLength;
} FloatValueCompressElement;

typedef struct LossyCompressionElement
{
	int leadingZeroBytes; //0,1,2,or 3
	unsigned char integerMidBytes[8];
	int integerMidBytes_Length; //they are mid_bits actually
	//char curBytes[8];
	//int curBytes_Length; //4 for single_precision or 8 for double_precision	
	int resMidBitsLength;
	int residualMidBits;
} LossyCompressionElement;


short computeGroupNum_float(float value);
short computeGroupNum_double(double value);

void listAdd_double(double last3CmprsData[3], double value);
void listAdd_float(float last3CmprsData[3], float value);
void listAdd_int(int64_t last3CmprsData[3], int64_t value);
void listAdd_int32(int32_t last3CmprsData[3], int32_t value);
void listAdd_float_group(float *groups, int *flags, char groupNum, float oriValue, float decValue, char* curGroupID);
void listAdd_double_group(double *groups, int *flags, char groupNum, double oriValue, double decValue, char* curGroupID);

int validPrediction_double(double minErr, double precision);
int validPrediction_float(float minErr, float precision);
double* generateGroupErrBounds(int errorBoundMode, double realPrecision, double pwrErrBound);
int generateGroupMaxIntervalCount(double* groupErrBounds);

void new_LossyCompressionElement(LossyCompressionElement *lce, int leadingNum, unsigned char* intMidBytes, 
		int intMidBytes_Length, int resiMidBitsLength, int resiBits);
void updateLossyCompElement_Double(unsigned char* curBytes, unsigned char* preBytes, 
		int reqBytesLength, int resiBitsLength,  LossyCompressionElement *lce);
void updateLossyCompElement_Float(unsigned char* curBytes, unsigned char* preBytes, 
		int reqBytesLength, int resiBitsLength,  LossyCompressionElement *lce);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _CompressElement_H  ----- */
