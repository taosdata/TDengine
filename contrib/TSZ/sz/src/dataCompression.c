/**
 *  @file double_compression.c
 *  @author Sheng Di, Dingwen Tao, Xin Liang, Xiangyu Zou, Tao Lu, Wen Xia, Xuan Wang, Weizhe Zhang
 *  @date April, 2016
 *  @brief Compression Technique for double array
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sz.h"
#include "DynamicByteArray.h"
#include "DynamicIntArray.h"
#include "TightDataPointStorageD.h"
#include "CompressElement.h"
#include "dataCompression.h"



float computeRangeSize_float(float* oriData, size_t size, float* valueRangeSize, float* medianValue)
{
	size_t i = 0;
	float min = oriData[0];
	float max = min;
	for(i=1;i<size;i++)
	{
		float data = oriData[i];
		if(min>data)
			min = data;
		else if(max<data)
			max = data;
	}

	*valueRangeSize = max - min;
	*medianValue = min + *valueRangeSize/2;
	return min;
}

double computeRangeSize_double(double* oriData, size_t size, double* valueRangeSize, double* medianValue)
{
	size_t i = 0;
	double min = oriData[0];
	double max = min;
	for(i=1;i<size;i++)
	{
		double data = oriData[i];
		if(min>data)
			min = data;
		else if(max<data)
			max = data;
	}
	
	*valueRangeSize = max - min;
	*medianValue = min + *valueRangeSize/2;
	return min;
}

double min_d(double a, double b)
{
	if(a<b)
		return a;
	else
		return b;
}

double max_d(double a, double b)
{
	if(a>b)
		return a;
	else
		return b;
}

float min_f(float a, float b)
{
	if(a<b)
		return a;
	else
		return b;
}

float max_f(float a, float b)
{
	if(a>b)
		return a;
	else
		return b;
}

double getRealPrecision_double(double valueRangeSize, int errBoundMode, double absErrBound, double relBoundRatio, int *status)
{
	int state = SZ_SUCCESS;
	double precision = 0;
	if(errBoundMode==SZ_ABS||errBoundMode==ABS_OR_PW_REL||errBoundMode==ABS_AND_PW_REL)
		precision = absErrBound; 
	else if(errBoundMode==REL||errBoundMode==REL_OR_PW_REL||errBoundMode==REL_AND_PW_REL)
		precision = relBoundRatio*valueRangeSize;
	else if(errBoundMode==ABS_AND_REL)
		precision = min_d(absErrBound, relBoundRatio*valueRangeSize);
	else if(errBoundMode==ABS_OR_REL)
		precision = max_d(absErrBound, relBoundRatio*valueRangeSize);
	else if(errBoundMode==PW_REL)
		precision = 0;
	else
	{
		printf("Error: error-bound-mode is incorrect!\n");
		state = SZ_BERR;
	}
	*status = state;
	return precision;
}

double getRealPrecision_float(float valueRangeSize, int errBoundMode, double absErrBound, double relBoundRatio, int *status)
{
	int state = SZ_SUCCESS;
	double precision = 0;
	if(errBoundMode==SZ_ABS||errBoundMode==ABS_OR_PW_REL||errBoundMode==ABS_AND_PW_REL)
		precision = absErrBound; 
	else if(errBoundMode==REL||errBoundMode==REL_OR_PW_REL||errBoundMode==REL_AND_PW_REL)
		precision = relBoundRatio*valueRangeSize;
	else if(errBoundMode==ABS_AND_REL)
		precision = min_f(absErrBound, relBoundRatio*valueRangeSize);
	else if(errBoundMode==ABS_OR_REL)
		precision = max_f(absErrBound, relBoundRatio*valueRangeSize);
	else if(errBoundMode==PW_REL)
		precision = 0;
	else
	{
		printf("Error: error-bound-mode is incorrect!\n");
		state = SZ_BERR;
	}
	*status = state;
	return precision;
}

double getRealPrecision_int(long valueRangeSize, int errBoundMode, double absErrBound, double relBoundRatio, int *status)
{
	int state = SZ_SUCCESS;
	double precision = 0;
	if(errBoundMode==SZ_ABS||errBoundMode==ABS_OR_PW_REL||errBoundMode==ABS_AND_PW_REL)
		precision = absErrBound; 
	else if(errBoundMode==REL||errBoundMode==REL_OR_PW_REL||errBoundMode==REL_AND_PW_REL)
		precision = relBoundRatio*valueRangeSize;
	else if(errBoundMode==ABS_AND_REL)
		precision = min_f(absErrBound, relBoundRatio*valueRangeSize);
	else if(errBoundMode==ABS_OR_REL)
		precision = max_f(absErrBound, relBoundRatio*valueRangeSize);
	else if(errBoundMode==PW_REL)
		precision = -1;
	else
	{
		printf("Error: error-bound-mode is incorrect!\n");
		state = SZ_BERR;
	}
	*status = state;
	return precision;
}

void symTransform_8bytes(unsigned char data[8])
{
	unsigned char tmp = data[0];
	data[0] = data[7];
	data[7] = tmp;

	tmp = data[1];
	data[1] = data[6];
	data[6] = tmp;
	
	tmp = data[2];
	data[2] = data[5];
	data[5] = tmp;
	
	tmp = data[3];
	data[3] = data[4];
	data[4] = tmp;
}

INLINE void symTransform_2bytes(unsigned char data[2])
{
	unsigned char tmp = data[0];
	data[0] = data[1];
	data[1] = tmp;
}

INLINE void symTransform_4bytes(unsigned char data[4])
{
	unsigned char tmp = data[0];
	data[0] = data[3];
	data[3] = tmp;

	tmp = data[1];
	data[1] = data[2];
	data[2] = tmp;
}

INLINE void compressSingleFloatValue(FloatValueCompressElement *vce, float oriValue, float precision, float medianValue, 
		int reqLength, int reqBytesLength, int resiBitsLength)
{		
	lfloat diffVal;
	diffVal.value = oriValue - medianValue;

	// calc ignore bit count		
	int ignBitCount = 32 - reqLength;
	if(ignBitCount<0)
		ignBitCount = 0;
	
	intToBytes_bigEndian(vce->curBytes, diffVal.ivalue);
	
	// truncate diff value tail bit with ignBitCount	
	diffVal.ivalue = (diffVal.ivalue >> ignBitCount) << ignBitCount;
	
	// save to vce
	vce->data           = diffVal.value + medianValue;
	vce->curValue       = diffVal.ivalue;
	vce->reqBytesLength = reqBytesLength;
	vce->resiBitsLength = resiBitsLength;
}

void compressSingleDoubleValue(DoubleValueCompressElement *vce, double tgtValue, double precision, double medianValue, 
		int reqLength, int reqBytesLength, int resiBitsLength)
{		
	double normValue = tgtValue - medianValue;

	ldouble lfBuf;
	lfBuf.value = normValue;
			
	int ignBytesLength = 64 - reqLength;
	if(ignBytesLength<0)
		ignBytesLength = 0;

	long tmp_long = lfBuf.lvalue;
	longToBytes_bigEndian(vce->curBytes, tmp_long);
				
	lfBuf.lvalue = (lfBuf.lvalue >> ignBytesLength)<<ignBytesLength;
	
	//double tmpValue = lfBuf.value;
	
	vce->data = lfBuf.value+medianValue;
	vce->curValue = tmp_long;
	vce->reqBytesLength = reqBytesLength;
	vce->resiBitsLength = resiBitsLength;
}

int compIdenticalLeadingBytesCount_double(unsigned char* preBytes, unsigned char* curBytes)
{
	int i, n = 0;
	for(i=0;i<8;i++)
		if(preBytes[i]==curBytes[i])
			n++;
		else
			break;
	if(n>3) n = 3;
	return n;
}

INLINE int compIdenticalLeadingBytesCount_float(unsigned char* preBytes, unsigned char* curBytes)
{
	int i, n = 0;
	for(i=0;i<4;i++)
		if(preBytes[i]==curBytes[i])
			n++;
		else
			break;
	if(n>3) n = 3;
	return n;
}

//TODO double-check the correctness...
INLINE void addExactData(DynamicByteArray *exactMidByteArray, DynamicIntArray *exactLeadNumArray, 
		DynamicIntArray *resiBitArray, LossyCompressionElement *lce)
{
	int i;
	int leadByteLength = lce->leadingZeroBytes;
	addDIA_Data(exactLeadNumArray, leadByteLength);
	unsigned char* intMidBytes = lce->integerMidBytes;
	int integerMidBytesLength = lce->integerMidBytes_Length;
	int resMidBitsLength = lce->resMidBitsLength;
	if(intMidBytes!=NULL||resMidBitsLength!=0)
	{
		if(intMidBytes!=NULL)
			for(i = 0;i<integerMidBytesLength;i++)
				addDBA_Data(exactMidByteArray, intMidBytes[i]);
		if(resMidBitsLength!=0)
			addDIA_Data(resiBitArray, lce->residualMidBits);
	}
}

/**
 * @deprecated
 * @return: the length of the coefficient array.
 * */
int getPredictionCoefficients(int layers, int dimension, int **coeff_array, int *status)
{
	size_t size = 0;
	switch(dimension)
	{
		case 1:
			switch(layers)
			{
				case 1:
					*coeff_array = (int*)malloc(sizeof(int));
					(*coeff_array)[0] = 1;
					size = 1;
					break;
				case 2:
					*coeff_array = (int*)malloc(2*sizeof(int));
					(*coeff_array)[0] = 2;
					(*coeff_array)[1] = -1;
					size = 2;
					break;
				case 3:
					*coeff_array = (int*)malloc(3*sizeof(int));
					(*coeff_array)[0] = 3;
					(*coeff_array)[1] = -3;
					(*coeff_array)[2] = 1;
					break;
			}	
			break;
		case 2:
			switch(layers)
			{
				case 1:
				
					break;
				case 2:
				
					break;
				case 3:
				
					break;
			}				
			break;
		case 3:
			switch(layers)
			{
				case 1:
				
					break;
				case 2:
				
					break;
				case 3:
				
					break;
			}			
			break;
		default:
			printf("Error: dimension must be no greater than 3 in the current version.\n");
			*status = SZ_DERR;
	}
	*status = SZ_SUCCESS;
	return size;
}

int computeBlockEdgeSize_2D(int segmentSize)
{
	int i = 1;
	for(i=1; i<segmentSize;i++)
	{
		if(i*i>segmentSize)
			break;
	}
	return i;
	//return (int)(sqrt(segmentSize)+1);
}

int computeBlockEdgeSize_3D(int segmentSize)
{
	int i = 1;
	for(i=1; i<segmentSize;i++)
	{
		if(i*i*i>segmentSize)
			break;
	}
	return i;	
	//return (int)(pow(segmentSize, 1.0/3)+1);
}

//The following functions are float-precision version of dealing with the unpredictable data points 
int generateLossyCoefficients_float(float* oriData, double precision, size_t nbEle, int* reqBytesLength, int* resiBitsLength, float* medianValue, float* decData)
{
	float valueRangeSize;
	
	computeRangeSize_float(oriData, nbEle, &valueRangeSize, medianValue);
	short radExpo = getExponent_float(valueRangeSize/2);
	
	int reqLength;
	computeReqLength_float(precision, radExpo, &reqLength, medianValue);
	
	*reqBytesLength = reqLength/8;
	*resiBitsLength = reqLength%8;
	
	size_t i = 0;
	for(i = 0;i < nbEle;i++)
	{
		float normValue = oriData[i] - *medianValue;

		lfloat lfBuf;
		lfBuf.value = normValue;
				
		int ignBytesLength = 32 - reqLength;
		if(ignBytesLength<0)
			ignBytesLength = 0;
			
		lfBuf.ivalue = (lfBuf.ivalue >> ignBytesLength) << ignBytesLength;
		
		//float tmpValue = lfBuf.value;
		
		decData[i] = lfBuf.value + *medianValue;
	}
	return reqLength;
}	
		
/**
 * @param float* oriData: inplace argument (input / output)
 * 
 * */		
int compressExactDataArray_float(float* oriData, double precision, size_t nbEle, unsigned char** leadArray, unsigned char** midArray, unsigned char** resiArray, 
int reqLength, int reqBytesLength, int resiBitsLength, float medianValue)
{
	//allocate memory for coefficient compression arrays
	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);	
	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);
	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);
	unsigned char preDataBytes[4] = {0,0,0,0};	

	//allocate memory for vce and lce
	FloatValueCompressElement *vce = (FloatValueCompressElement*)malloc(sizeof(FloatValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));	

	size_t i = 0;
	for(i = 0;i < nbEle;i++)
	{
		compressSingleFloatValue(vce, oriData[i], precision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,4);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
		oriData[i] = vce->data;
	}
	convertDIAtoInts(exactLeadNumArray, leadArray);
	convertDBAtoBytes(exactMidByteArray,midArray);
	convertDIAtoInts(resiBitArray, resiArray);

	size_t midArraySize = exactMidByteArray->size;
	
	free(vce);
	free(lce);
	
	free_DIA(exactLeadNumArray);
	free_DBA(exactMidByteArray);
	free_DIA(resiBitArray);
	
	return midArraySize;
}

void decompressExactDataArray_float(unsigned char* leadNum, unsigned char* exactMidBytes, unsigned char* residualMidBits, size_t nbEle, int reqLength, float medianValue, float** decData)
{
	*decData = (float*)malloc(nbEle*sizeof(float));
	size_t i = 0, j = 0, k = 0, l = 0, p = 0, curByteIndex = 0;
	float exactData = 0;
	unsigned char preBytes[4] = {0,0,0,0};
	unsigned char curBytes[4];
	int resiBits; 
	unsigned char leadingNum;		
	
	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;
	
	for(i = 0; i<nbEle;i++)
	{
		// compute resiBits
		resiBits = 0;
		if (resiBitsLength != 0) {
			int kMod8 = k % 8;
			int rightMovSteps = getRightMovingSteps(kMod8, resiBitsLength);
			if (rightMovSteps > 0) {
				int code = getRightMovingCode(kMod8, resiBitsLength);
				resiBits = (residualMidBits[p] & code) >> rightMovSteps;
			} else if (rightMovSteps < 0) {
				int code1 = getLeftMovingCode(kMod8);
				int code2 = getRightMovingCode(kMod8, resiBitsLength);
				int leftMovSteps = -rightMovSteps;
				rightMovSteps = 8 - leftMovSteps;
				resiBits = (residualMidBits[p] & code1) << leftMovSteps;
				p++;
				resiBits = resiBits
						| ((residualMidBits[p] & code2) >> rightMovSteps);
			} else // rightMovSteps == 0
			{
				int code = getRightMovingCode(kMod8, resiBitsLength);
				resiBits = (residualMidBits[p] & code);
				p++;
			}
			k += resiBitsLength;
		}

		// recover the exact data	
		memset(curBytes, 0, 4);
		leadingNum = leadNum[l++];
		memcpy(curBytes, preBytes, leadingNum);
		for (j = leadingNum; j < reqBytesLength; j++)
			curBytes[j] = exactMidBytes[curByteIndex++];
		if (resiBitsLength != 0) {
			unsigned char resiByte = (unsigned char) (resiBits << (8 - resiBitsLength));
			curBytes[reqBytesLength] = resiByte;
		}

		exactData = bytesToFloat(curBytes);
		(*decData)[i] = exactData + medianValue;
		memcpy(preBytes,curBytes,4);
	}	
}

//double-precision version of dealing with unpredictable data points in sz 2.0
int generateLossyCoefficients_double(double* oriData, double precision, size_t nbEle, int* reqBytesLength, int* resiBitsLength, double* medianValue, double* decData)
{
	double valueRangeSize;
	
	computeRangeSize_double(oriData, nbEle, &valueRangeSize, medianValue);
	short radExpo = getExponent_double(valueRangeSize/2);
	
	int reqLength;
	computeReqLength_double(precision, radExpo, &reqLength, medianValue);
	
	*reqBytesLength = reqLength/8;
	*resiBitsLength = reqLength%8;
	
	size_t i = 0;
	for(i = 0;i < nbEle;i++)
	{
		double normValue = oriData[i] - *medianValue;

		ldouble ldBuf;
		ldBuf.value = normValue;
				
		int ignBytesLength = 64 - reqLength;
		if(ignBytesLength<0)
			ignBytesLength = 0;
			
		ldBuf.lvalue = (ldBuf.lvalue >> ignBytesLength) << ignBytesLength;
		
		decData[i] = ldBuf.value + *medianValue;
	}
	return reqLength;
}	
		
/**
 * @param double* oriData: inplace argument (input / output)
 * 
 * */		
int compressExactDataArray_double(double* oriData, double precision, size_t nbEle, unsigned char** leadArray, unsigned char** midArray, unsigned char** resiArray, 
int reqLength, int reqBytesLength, int resiBitsLength, double medianValue)
{
	//allocate memory for coefficient compression arrays
	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);	
	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);
	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);
	unsigned char preDataBytes[8] = {0,0,0,0,0,0,0,0};	

	//allocate memory for vce and lce
	DoubleValueCompressElement *vce = (DoubleValueCompressElement*)malloc(sizeof(DoubleValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));	

	size_t i = 0;
	for(i = 0;i < nbEle;i++)
	{
		compressSingleDoubleValue(vce, oriData[i], precision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Double(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,8);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
		oriData[i] = vce->data;
	}
	convertDIAtoInts(exactLeadNumArray, leadArray);
	convertDBAtoBytes(exactMidByteArray,midArray);
	convertDIAtoInts(resiBitArray, resiArray);

	size_t midArraySize = exactMidByteArray->size;
	
	free(vce);
	free(lce);
	
	free_DIA(exactLeadNumArray);
	free_DBA(exactMidByteArray);
	free_DIA(resiBitArray);
	
	return midArraySize;
}

void decompressExactDataArray_double(unsigned char* leadNum, unsigned char* exactMidBytes, unsigned char* residualMidBits, size_t nbEle, int reqLength, double medianValue, double** decData)
{
	*decData = (double*)malloc(nbEle*sizeof(double));
	size_t i = 0, j = 0, k = 0, l = 0, p = 0, curByteIndex = 0;
	double exactData = 0;
	unsigned char preBytes[8] = {0,0,0,0,0,0,0,0};
	unsigned char curBytes[8];
	int resiBits; 
	unsigned char leadingNum;		
	
	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;
	
	for(i = 0; i<nbEle;i++)
	{
		// compute resiBits
		resiBits = 0;
		if (resiBitsLength != 0) {
			int kMod8 = k % 8;
			int rightMovSteps = getRightMovingSteps(kMod8, resiBitsLength);
			if (rightMovSteps > 0) {
				int code = getRightMovingCode(kMod8, resiBitsLength);
				resiBits = (residualMidBits[p] & code) >> rightMovSteps;
			} else if (rightMovSteps < 0) {
				int code1 = getLeftMovingCode(kMod8);
				int code2 = getRightMovingCode(kMod8, resiBitsLength);
				int leftMovSteps = -rightMovSteps;
				rightMovSteps = 8 - leftMovSteps;
				resiBits = (residualMidBits[p] & code1) << leftMovSteps;
				p++;
				resiBits = resiBits
						| ((residualMidBits[p] & code2) >> rightMovSteps);
			} else // rightMovSteps == 0
			{
				int code = getRightMovingCode(kMod8, resiBitsLength);
				resiBits = (residualMidBits[p] & code);
				p++;
			}
			k += resiBitsLength;
		}

		// recover the exact data	
		memset(curBytes, 0, 8);
		leadingNum = leadNum[l++];
		memcpy(curBytes, preBytes, leadingNum);
		for (j = leadingNum; j < reqBytesLength; j++)
			curBytes[j] = exactMidBytes[curByteIndex++];
		if (resiBitsLength != 0) {
			unsigned char resiByte = (unsigned char) (resiBits << (8 - resiBitsLength));
			curBytes[reqBytesLength] = resiByte;
		}

		exactData = bytesToDouble(curBytes);
		(*decData)[i] = exactData + medianValue;
		memcpy(preBytes,curBytes,8);
	}
}
