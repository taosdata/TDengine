/**
 *  @file sz_double.c
 *  @author Sheng Di, Dingwen Tao, Xin Liang, Xiangyu Zou, Tao Lu, Wen Xia, Xuan Wang, Weizhe Zhang
 *  @date Aug, 2016
 *  @brief SZ_Init, Compression and Decompression functions
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "sz.h"
#include "CompressElement.h"
#include "DynamicByteArray.h"
#include "DynamicIntArray.h"
#include "TightDataPointStorageD.h"
#include "sz_double.h"
#include "szd_double.h"
#include "utility.h"

unsigned char* SZ_skip_compress_double(double* data, size_t dataLength, size_t* outSize)
{
	*outSize = dataLength*sizeof(double);
	unsigned char* out = (unsigned char*)malloc(dataLength*sizeof(double));
	memcpy(out, data, dataLength*sizeof(double));
	return out;
}

INLINE void computeReqLength_double(double realPrecision, short radExpo, int* reqLength, double* medianValue)
{
	short reqExpo = getPrecisionReqLength_double(realPrecision);
	*reqLength = 12+radExpo - reqExpo; //radExpo-reqExpo == reqMantiLength
	if(*reqLength<12)
		*reqLength = 12;
	if(*reqLength>64)
	{
		*reqLength = 64;
		*medianValue = 0;
	}
}


unsigned int optimize_intervals_double_1D(double *oriData, size_t dataLength, double realPrecision)
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
			//pred_value = 2*oriData[i-1] - oriData[i-2];
			pred_value = oriData[i-1];
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
	//printf("accIntervals=%d, powerOf2=%d\n", accIntervals, powerOf2);
	return powerOf2;
}

TightDataPointStorageD* SZ_compress_double_1D_MDQ(double *oriData, 
size_t dataLength, double realPrecision, double valueRangeSize, double medianValue_d)
{
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
		quantization_intervals = optimize_intervals_double_1D_opt(oriData, dataLength, realPrecision);
	else
		quantization_intervals = exe_params->intvCapacity;
	//updateQuantizationInfo(quantization_intervals);	
	int intvRadius = quantization_intervals/2;

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
	double last3CmprsData[3] = {0};

	DoubleValueCompressElement *vce = (DoubleValueCompressElement*)malloc(sizeof(DoubleValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));			
				
	//add the first data
	type[0] = 0;	
	compressSingleDoubleValue(vce, spaceFillingValue[0], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Double(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,8);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	listAdd_double(last3CmprsData, vce->data);	
		
	//add the second data
	type[1] = 0;
	compressSingleDoubleValue(vce, spaceFillingValue[1], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Double(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,8);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	listAdd_double(last3CmprsData, vce->data);

	int state;
	double checkRadius;
	double curData;
	double pred = last3CmprsData[0];
	double predAbsErr;
	checkRadius = (quantization_intervals-1)*realPrecision;
	double interval = 2*realPrecision;

	double recip_realPrecision = 1/realPrecision;
	for(i=2; i < dataLength; i++)
	{				
		//printf("%.30G\n",last3CmprsData[0]);
		curData = spaceFillingValue[i];
		//pred = 2*last3CmprsData[0] - last3CmprsData[1];
		//pred = last3CmprsData[0];
		predAbsErr = fabs(curData - pred);	
		if(predAbsErr<checkRadius)
		{
			state = (predAbsErr*recip_realPrecision+1)*0.5;
			if(curData>=pred)
			{
				type[i] = intvRadius+state;
				pred = pred + state*interval;
			}
			else //curData<pred
			{
				type[i] = intvRadius-state;
				pred = pred - state*interval;
			}
			//listAdd_double(last3CmprsData, pred);
			continue;
		}
		
		//unpredictable data processing	
		type[i] = 0;	
		compressSingleDoubleValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Double(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,8);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
							
		//listAdd_double(last3CmprsData, vce->data);
		pred = vce->data;
		
	}//end of for
		
	size_t exactDataNum = exactLeadNumArray->size;
	
	TightDataPointStorageD* tdps;
			
	new_TightDataPointStorageD(&tdps, dataLength, exactDataNum, 
			type, exactMidByteArray->array, exactMidByteArray->size,  
			exactLeadNumArray->array,  
			resiBitArray->array, resiBitArray->size, 
			resiBitsLength, 
			realPrecision, medianValue, (char)reqLength, quantization_intervals, 0);
	
//	printf("exactDataNum=%d, expSegmentsInBytes_size=%d, exactMidByteArray->size=%d\n", 
//			exactDataNum, expSegmentsInBytes_size, exactMidByteArray->size);
	
	//free memory
	free_DIA(exactLeadNumArray);
	free_DIA(resiBitArray);
	free(type);
	free(vce);
	free(lce);	
	free(exactMidByteArray); //exactMidByteArray->array has been released in free_TightDataPointStorageF(tdps);	
	
	return tdps;	
}

void SZ_compress_args_double_StoreOriData(double* oriData, size_t dataLength, unsigned char* newByteData, size_t *outSize)
{	
	int doubleSize = sizeof(double);
	size_t k = 0, i;
	size_t totalByteLength = 1 + MetaDataByteLength_double + exe_params->SZ_SIZE_TYPE + 1 + doubleSize*dataLength;
	/*No need to malloc because newByteData should always already be allocated with no less totalByteLength.*/
	//*newByteData = (unsigned char*)malloc(totalByteLength);
	
	unsigned char dsLengthBytes[8];
	newByteData[k++] = versionNumber;
	
	if(exe_params->SZ_SIZE_TYPE==4)//1
		newByteData[k++] = 16; //00010000
	else
		newByteData[k++] = 80;	//01010000: 01000000 indicates the SZ_SIZE_TYPE=8

	convertSZParamsToBytes(confparams_cpr, &(newByteData[k]), exe_params->optQuantMode);
	k = k + MetaDataByteLength_double;

	sizeToBytes(dsLengthBytes,dataLength, exe_params->SZ_SIZE_TYPE);
	for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)//ST: 4 or 8
		newByteData[k++] = dsLengthBytes[i];

	if(sysEndianType==BIG_ENDIAN_SYSTEM)
		memcpy(newByteData+4+MetaDataByteLength_double+exe_params->SZ_SIZE_TYPE, oriData, dataLength*doubleSize);
	else
	{
		unsigned char* p = newByteData+4+MetaDataByteLength_double+exe_params->SZ_SIZE_TYPE;
		for(i=0;i<dataLength;i++,p+=doubleSize)
			doubleToBytes(p, oriData[i]);
	}
	*outSize = totalByteLength;
}


bool SZ_compress_args_double_NoCkRngeNoGzip_1D(unsigned char* newByteData, double *oriData, 
size_t dataLength, double realPrecision, size_t *outSize, double valueRangeSize, double medianValue_d)
{
	TightDataPointStorageD* tdps = NULL; 	
	tdps = SZ_compress_double_1D_MDQ(oriData, dataLength, realPrecision, valueRangeSize, medianValue_d);			
	
	if(!convertTDPStoFlatBytes_double(tdps, newByteData, outSize))
	{
		free_TightDataPointStorageD(tdps);	
		return false;
	} 
	
	//if(*outSize>3 + MetaDataByteLength_double + exe_params->SZ_SIZE_TYPE + 1 + sizeof(double)*dataLength)
	//	SZ_compress_args_double_StoreOriData(oriData, dataLength, newByteData, outSize);
	
	free_TightDataPointStorageD(tdps);	
	return true;
}


void SZ_compress_args_double_withinRange(unsigned char* newByteData, double *oriData, size_t dataLength, size_t *outSize)
{
	TightDataPointStorageD* tdps = (TightDataPointStorageD*) malloc(sizeof(TightDataPointStorageD));
	memset(tdps, 0, sizeof(TightDataPointStorageD));
	tdps->leadNumArray = NULL;
	tdps->residualMidBits = NULL;
	
	tdps->allSameData = 1;
	tdps->dataSeriesLength = dataLength;
	tdps->exactMidBytes = (unsigned char*)malloc(sizeof(unsigned char)*8);
	tdps->isLossless = 0;
	double value = oriData[0];
	doubleToBytes(tdps->exactMidBytes, value);
	tdps->exactMidBytes_size = 8;
	
	size_t tmpOutSize;
	//unsigned char *tmpByteData;
	convertTDPStoFlatBytes_double(tdps, newByteData, &tmpOutSize);
	//convertTDPStoFlatBytes_double(tdps, &tmpByteData, &tmpOutSize);

	//*newByteData = (unsigned char*)malloc(sizeof(unsigned char)*16); //for floating-point data (1+3+4+4)
	//memcpy(*newByteData, tmpByteData, 16);
	*outSize = tmpOutSize;//12==3+1+8(double_size)+MetaDataByteLength_double
	free_TightDataPointStorageD(tdps);	
}

int SZ_compress_args_double(double *oriData, size_t r1, unsigned char* newByteData, size_t *outSize, sz_params* params)
{		
	int status = SZ_SUCCESS;
	size_t dataLength = r1;
	
	double valueRangeSize = 0, medianValue = 0;

	// check at least elements count  
	if(dataLength <= MIN_NUM_OF_ELEMENTS)
	{
		printf("warning, double input elements count=%d less than %d, so need not do compress.\n", (int)dataLength, MIN_NUM_OF_ELEMENTS);
		return SZ_LITTER_ELEMENT;
	}
		

	double min = computeRangeSize_double(oriData, dataLength, &valueRangeSize, &medianValue);	
	double max = min+valueRangeSize;
	params->dmin = min;
	params->dmax = max;
	
	double realPrecision = 0; 
	
	if(params->errorBoundMode==PSNR)
	{
		params->errorBoundMode = SZ_ABS;
		realPrecision = params->absErrBoundDouble = computeABSErrBoundFromPSNR(params->psnr, (double)params->predThreshold, valueRangeSize);
	}
	else if(params->errorBoundMode==NORM) //norm error = sqrt(sum((xi-xi_)^2))
	{
		params->errorBoundMode = SZ_ABS;
		realPrecision = params->absErrBoundDouble = computeABSErrBoundFromNORM_ERR(params->normErr, dataLength);
		//printf("realPrecision=%lf\n", realPrecision);				
	}	
	else
	{
		realPrecision = getRealPrecision_double(valueRangeSize, params->errorBoundMode, params->absErrBoundDouble, params->relBoundRatio, &status);
		params->absErrBoundDouble = realPrecision;
	}	
	if(valueRangeSize <= realPrecision)
	{		
		SZ_compress_args_double_withinRange(newByteData, oriData, dataLength, outSize);
	}
	else
	{
		size_t tmpOutSize = 0;
		unsigned char* tmpByteData = newByteData;
		bool twoStage = params->szMode != SZ_BEST_SPEED;
		if(twoStage)
		{
			tmpByteData = (unsigned char*)malloc(r1*sizeof(double)*1.2);
		}

		if(!SZ_compress_args_double_NoCkRngeNoGzip_1D(tmpByteData, oriData, r1, realPrecision, &tmpOutSize, valueRangeSize, medianValue))
		{
			if(twoStage)
				free(tmpByteData);

			return SZ_ALGORITHM_ERR;
		}
		//if(tmpOutSize>=dataLength*sizeof(double) + 3 + MetaDataByteLength_double + exe_params->SZ_SIZE_TYPE + 1)
		//	SZ_compress_args_double_StoreOriData(oriData, dataLength, tmpByteData, &tmpOutSize);
					
		//		
		//Call Gzip to do the further compression.
		//
		if(twoStage)
		{
			*outSize = sz_lossless_compress(params->losslessCompressor, tmpByteData, tmpOutSize, newByteData);
			free(tmpByteData);
		}
		else
		{
			*outSize = tmpOutSize;
		}
	}

	return status;
}

unsigned int optimize_intervals_double_1D_opt(double *oriData, size_t dataLength, double realPrecision)
{	
	size_t i = 0, radiusIndex;
	double pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = 0;

	double * data_pos = oriData + 2;
	while(data_pos - oriData < dataLength){
		totalSampleSize++;
		pred_value = data_pos[-1];
		pred_err = fabs(pred_value - *data_pos);
		double dbri = (unsigned long)((pred_err/realPrecision+1)/2);
		if(dbri >= (double)confparams_cpr->maxRangeRadius)
			radiusIndex = confparams_cpr->maxRangeRadius - 1;
		else
		    radiusIndex = dbri;
		intervals[radiusIndex]++;

		data_pos += confparams_cpr->sampleDistance;
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
