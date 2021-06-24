/**
 *  @file sz_double_ts.c
 *  @author Sheng Di and Dingwen Tao
 *  @date Aug, 2016
 *  @brief 
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include "sz.h"
#include "CompressElement.h"
#include "DynamicByteArray.h"
#include "DynamicIntArray.h"
#include "TightDataPointStorageD.h"
#include "zlib.h"
#include "rw.h"
#include "sz_double_ts.h"

unsigned int optimize_intervals_double_1D_ts(double *oriData, size_t dataLength, double* preData, double realPrecision)
{	
	size_t i = 0, radiusIndex;
	double pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = dataLength/confparams_cpr->sampleDistance;
	for(i=2;i<dataLength;i++)
	{
		if(i%confparams_cpr->sampleDistance==0)
		{
			pred_value = preData[i];
			pred_err = fabs(pred_value - oriData[i]);
			radiusIndex = (unsigned long)((pred_err/realPrecision+1)/2);
			if(radiusIndex>=confparams_cpr->maxRangeRadius)
				radiusIndex = confparams_cpr->maxRangeRadius - 1;			
			intervals[radiusIndex]++;
		}
	}
	//compute the appropriate number
	size_t targetCount = totalSampleSize*confparams_cpr->predThreshold;
	size_t sum = 0;
	for(i=0;i<confparams_cpr->maxRangeRadius;i++)
	{
		sum += intervals[i];
		if(sum>targetCount)
			break;
	}
	if(i>=confparams_cpr->maxRangeRadius)
		i = confparams_cpr->maxRangeRadius-1;
		
	unsigned int accIntervals = 2*(i+1);
	unsigned int powerOf2 = roundUpToPowerOf2(accIntervals);
	
	if(powerOf2<32)
		powerOf2 = 32;
	
	free(intervals);
	return powerOf2;
}

TightDataPointStorageD* SZ_compress_double_1D_MDQ_ts(double *oriData, size_t dataLength, sz_multisteps* multisteps,
double realPrecision, double valueRangeSize, double medianValue_d)
{
	double* preStepData = (double*)(multisteps->hist_data);
	//store the decompressed data
	//double* decData = (double*)malloc(sizeof(double)*dataLength);
	//memset(decData, 0, sizeof(double)*dataLength);
	double* decData = preStepData;
	
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
		quantization_intervals = optimize_intervals_double_1D_ts(oriData, dataLength, preStepData, realPrecision);
	else
		quantization_intervals = exe_params->intvCapacity;
	updateQuantizationInfo(quantization_intervals);	

	size_t i;
	int reqLength;
	double medianValue = medianValue_d;
	short radExpo = getExponent_double(valueRangeSize/2);

	computeReqLength_double(realPrecision, radExpo, &reqLength, &medianValue);	

	int* type = (int*) malloc(dataLength*sizeof(int));
		
	double* spaceFillingValue = oriData; //
	
	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);
	
	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);
	
	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);

	unsigned char preDataBytes[8];
	longToBytes_bigEndian(preDataBytes, 0);
	
	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;

	DoubleValueCompressElement *vce = (DoubleValueCompressElement*)malloc(sizeof(DoubleValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));			
				
	//add the first data	
	type[0] = 0;
	compressSingleDoubleValue(vce, spaceFillingValue[0], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Double(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,8);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	decData[0] = vce->data;
		
	//add the second data
	type[1] = 0;
	compressSingleDoubleValue(vce, spaceFillingValue[1], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Double(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,8);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	decData[1] = vce->data;	
	
	int state = 0;
	double checkRadius = 0;
	double curData = 0;
	double pred = 0;
	double predAbsErr = 0;
	checkRadius = (exe_params->intvCapacity-1)*realPrecision;
	double interval = 2*realPrecision;

	for(i=2;i<dataLength;i++)
	{
		curData = spaceFillingValue[i];
		pred = preStepData[i];
		predAbsErr = fabs(curData - pred);	
		if(predAbsErr<=checkRadius)
		{
			state = (predAbsErr/realPrecision+1)/2;
			if(curData>=pred)
			{
				type[i] = exe_params->intvRadius+state;
				pred = pred + state*interval;
			}
			else //curData<pred
			{
				type[i] = exe_params->intvRadius-state;
				pred = pred - state*interval;
			}
			decData[i] = pred;	
			continue;
		}
		
		//unpredictable data processing
		type[i] = 0;		
		compressSingleDoubleValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Double(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,8);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
		decData[i] = vce->data;
	}//end of for
		
	size_t exactDataNum = exactLeadNumArray->size;
	
	TightDataPointStorageD* tdps;
			
	new_TightDataPointStorageD(&tdps, dataLength, exactDataNum, 
			type, exactMidByteArray->array, exactMidByteArray->size,  
			exactLeadNumArray->array,  
			resiBitArray->array, resiBitArray->size, 
			resiBitsLength, 
			realPrecision, medianValue, (char)reqLength, quantization_intervals, NULL, 0, 0);
	
	//free memory
	free_DIA(exactLeadNumArray);
	free_DIA(resiBitArray);
	free(type);	
	free(vce);
	free(lce);	
	free(exactMidByteArray); //exactMidByteArray->array has been released in free_TightDataPointStorageF(tdps);
		
	//memcpy(preStepData, decData, dataLength*sizeof(double)); //update the data
	//free(decData);
	
	return tdps;
}


