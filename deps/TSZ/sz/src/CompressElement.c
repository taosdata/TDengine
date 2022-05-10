/**
 *  @file CompressElement.c
 *  @author Sheng Di
 *  @date May, 2016
 *  @brief Functions of CompressElement
 *  (C) 2015 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */
#ifndef WINDOWS
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wchar-subscripts"
#endif

#include <stdlib.h> 
#include <stdio.h>
#include <math.h>
#include <string.h>
#include "sz.h"
#include "CompressElement.h"


INLINE short computeGroupNum_float(float value)
{
	short expo = getExponent_float(value);
	if(expo < 0)
		expo = -1;
	return expo;
}

INLINE short computeGroupNum_double(double value)
{
	short expo = getExponent_double(value);
	if(expo < 0)
		expo = -1;
	return expo;
}

/**
 * Add preceding neighbor values to a buffer.
 * @param  last3CmprsData buffer
 * @param  value the value to be added to the buffer
 * */
INLINE void listAdd_double(double last3CmprsData[3], double value)
{
	last3CmprsData[2] = last3CmprsData[1];
	last3CmprsData[1] = last3CmprsData[0];
	last3CmprsData[0] = value;
}

INLINE void listAdd_float(float last3CmprsData[3], float value)
{
	last3CmprsData[2] = last3CmprsData[1];
	last3CmprsData[1] = last3CmprsData[0];
	last3CmprsData[0] = value;
}

INLINE void listAdd_int(int64_t last3CmprsData[3], int64_t value)
{
	last3CmprsData[2] = last3CmprsData[1];
	last3CmprsData[1] = last3CmprsData[0];
	last3CmprsData[0] = value;
}

INLINE void listAdd_int32(int32_t last3CmprsData[3], int32_t value)
{
	last3CmprsData[2] = last3CmprsData[1];
	last3CmprsData[1] = last3CmprsData[0];
	last3CmprsData[0] = value;
}

INLINE void listAdd_float_group(float *groups, int *flags, char groupNum, float oriValue, float decValue, char* curGroupID)
{
	if(groupNum>=0)
	{
		if(flags[groupNum]==0)
			flags[groupNum] = 1;
		groups[groupNum] = decValue;		
	}
	else
	{
		groups[0] = decValue;
		flags[0] = 1;		
	}

	if(oriValue>=0)
		*curGroupID = groupNum+2; //+[-1,0,1,2,3,....,16] is mapped to [1,2,....,18]
	else
		*curGroupID = -(groupNum+2); //-[-1,0,1,2,3,....,16] is mapped to [-1,-2,....,-18]
}

INLINE void listAdd_double_group(double *groups, int *flags, char groupNum, double oriValue, double decValue, char* curGroupID)
{
	if(groupNum>=0)
	{
		if(flags[groupNum]==0)
			flags[groupNum] = 1;
		groups[groupNum] = decValue;		
	}
	else
	{
		groups[0] = decValue;
		flags[0] = 1;		
	}

	if(oriValue>=0)
		*curGroupID = groupNum+2; //+[-1,0,1,2,3,....,16] is mapped to [1,2,....,18]
	else
		*curGroupID = -(groupNum+2); //-[-1,0,1,2,3,....,16] is mapped to [-1,-2,....,-18]
}

/**
 * Determine whether the prediction value minErr is valid.
 * 
 * */
INLINE int validPrediction_double(double minErr, double precision)
{
	if(minErr<=precision)
		return 1;
	else
		return 0;
}

INLINE int validPrediction_float(float minErr, float precision)
{
	if(minErr<=precision)
		return 1;
	else
		return 0;
}

double* generateGroupErrBounds(int errorBoundMode, double realPrecision, double pwrErrBound)
{
	double pwrError;
	double* result = (double*)malloc(GROUP_COUNT*sizeof(double));
	int i = 0;
	for(i=0;i<GROUP_COUNT;i++)
	{
		pwrError = ((double)pow(2, i))*pwrErrBound;
		switch(errorBoundMode)
		{
		case ABS_AND_PW_REL:
		case REL_AND_PW_REL: 
			result[i] = pwrError<realPrecision?pwrError:realPrecision;
			break;
		case ABS_OR_PW_REL:
		case REL_OR_PW_REL:
			result[i] = pwrError<realPrecision?realPrecision:pwrError;
			break;
		case PW_REL:
			result[i] = pwrError;
			break;
		}
		
	}
	return result;
}

int generateGroupMaxIntervalCount(double* groupErrBounds)
{
	int i = 0;
	int maxCount = 0, count = 0;
	for(i=0;i<GROUP_COUNT;i++)
	{
		count = (int)(pow(2, i)/groupErrBounds[i] + 0.5);
		if(maxCount<count)
			maxCount = count;
	}
	
	return maxCount;
}

void new_LossyCompressionElement(LossyCompressionElement *lce, int leadingNum, unsigned char* intMidBytes, 
int intMidBytes_Length, int resiMidBitsLength, int resiBits)
{
	lce->leadingZeroBytes = leadingNum; //0,1,2,or 3
	memcpy(lce->integerMidBytes,intMidBytes,intMidBytes_Length);
	lce->integerMidBytes_Length = intMidBytes_Length; //they are mid_bits actually
	lce->resMidBitsLength = resiMidBitsLength;
	lce->residualMidBits = resiBits;
}

void updateLossyCompElement_Double(unsigned char* curBytes, unsigned char* preBytes, 
		int reqBytesLength, int resiBitsLength,  LossyCompressionElement *lce)
{
	int resiIndex, intMidBytes_Length = 0;
	int leadingNum = compIdenticalLeadingBytesCount_double(preBytes, curBytes); //in fact, float is enough for both single-precision and double-precisiond ata.
	int fromByteIndex = leadingNum;
	int toByteIndex = reqBytesLength; //later on: should use "< toByteIndex" to tarverse....
	if(fromByteIndex < toByteIndex)
	{
		intMidBytes_Length = reqBytesLength - leadingNum;
		memcpy(lce->integerMidBytes, &(curBytes[fromByteIndex]), intMidBytes_Length);
	}
	int resiBits = 0;
	if(resiBitsLength!=0)
	{
		resiIndex = reqBytesLength;
		if(resiIndex < 8)
			resiBits = (curBytes[resiIndex] & 0xFF) >> (8-resiBitsLength);
	}
	lce->leadingZeroBytes = leadingNum;
	lce->integerMidBytes_Length = intMidBytes_Length;
	lce->resMidBitsLength = resiBitsLength;
	lce->residualMidBits = resiBits;
}

INLINE void updateLossyCompElement_Float(unsigned char* diffBytes, unsigned char* preDiffBytes, 
		int reqBytesLength, int resiBitsLength,  LossyCompressionElement *lce)
{
	int resiIndex, intMidBytes_Length = 0;
	int leadingNum = compIdenticalLeadingBytesCount_float(preDiffBytes, diffBytes); //in fact, float is enough for both single-precision and double-precisiond ata.
	int fromByteIndex = leadingNum;
	int toByteIndex = reqBytesLength; //later on: should use "< toByteIndex" to tarverse....
	if(fromByteIndex < toByteIndex)
	{
		intMidBytes_Length = reqBytesLength - leadingNum;
		// set lce mid data
		memcpy(lce->integerMidBytes, &(diffBytes[fromByteIndex]), intMidBytes_Length);
	}
	int resiBits = 0;
	if(resiBitsLength!=0)
	{
		resiIndex = reqBytesLength;
		if(resiIndex < 8)
			resiBits = (diffBytes[resiIndex] & 0xFF) >> (8-resiBitsLength);
	}

	// set lce
	lce->leadingZeroBytes = leadingNum;
	lce->integerMidBytes_Length = intMidBytes_Length;
	lce->resMidBitsLength = resiBitsLength;
	lce->residualMidBits = resiBits;
}

#ifndef WINDOWS
  #pragma GCC diagnostic pop
#endif