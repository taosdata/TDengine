/**
 *  @file sz_float.c
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
#include "TightDataPointStorageF.h"
#include "sz_float.h"
#include "szd_float.h"
#include "utility.h"

# define MIN(_a, _b) (((_a) < (_b)) ? (_a) : (_b))



void computeReqLength_float(double realPrecision, short rangeExpo, int* reqLength, float* medianValue)
{
	short realPrecExpo = getPrecisionReqLength_double(realPrecision);
	*reqLength = 9 + rangeExpo - realPrecExpo + 1; //radExpo-reqExpo == reqMantiLength
	if(*reqLength<9)
		*reqLength = 9;
	if(*reqLength>32)
	{	
		*reqLength = 32;
		*medianValue = 0;
	}			
}


unsigned int optimize_intervals_float_1D(float *oriData, size_t dataLength, double realPrecision)
{	
	size_t i = 0, radiusIndex;
	float pred_value = 0, pred_err;
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

TightDataPointStorageF* SZ_compress_float_1D_MDQ(float *oriData, 
				size_t dataLength, float realPrecision, float valueRangeSize, float medianValue_f)
{
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
		quantization_intervals = optimize_intervals_float_1D_opt(oriData, dataLength, realPrecision);
	else
		quantization_intervals = exe_params->intvCapacity;
	//updateQuantizationInfo(quantization_intervals);	
	int half_interval = quantization_intervals/2;

    //
	// calc reqlength and need set medianValue to zero. 
	// 
	size_t i;
	int reqLength; // need save bits length for one float .  value ragne 9~32 
	float medianValue = medianValue_f;
	short rangeExpo = getExponent_float(valueRangeSize/2);
	
	computeReqLength_float(realPrecision, rangeExpo, &reqLength, &medianValue);	

	//
	//  malloc all 
	//
	
	// 1 type 
	int* type = (int*) malloc(dataLength*sizeof(int));	
	float* spaceFillingValue = oriData; //
	// 2 lead
	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);
	// 3 mid
	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);
	// 4 residual bit
	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);
	
	unsigned char preDiffBytes[4];
	intToBytes_bigEndian(preDiffBytes, 0);
	
	// calc save byte length and bit lengths with reqLength
	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;
	//float last3CmprsData[3] = {0};

	FloatValueCompressElement *vce = (FloatValueCompressElement*)malloc(sizeof(FloatValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));
				
	//add the first data	
	type[0] = 0;
	compressSingleFloatValue(vce, spaceFillingValue[0], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	// set lce
	updateLossyCompElement_Float(vce->curBytes, preDiffBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDiffBytes, vce->curBytes, 4);
	// lce to arrays
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	//listAdd_float(last3CmprsData, vce->data);
		
	//add the second data
	type[1] = 0;
	compressSingleFloatValue(vce, spaceFillingValue[1], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDiffBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDiffBytes, vce->curBytes, 4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	//listAdd_float(last3CmprsData, vce->data);

	int state;
	float checkRadius;
	float oriFloat;
	float pred = vce->data;
	float diff;
	checkRadius = (quantization_intervals-1)*realPrecision;
	float double_realpreci = 2*realPrecision;
	float recip_precision = 1/realPrecision;
	
	for(i=2; i < dataLength; i++)
	{	
		oriFloat = spaceFillingValue[i];
		//pred = 2*last3CmprsData[0] - last3CmprsData[1];
		//pred = last3CmprsData[0];
		diff = fabsf(oriFloat - pred);	
		if(diff < checkRadius)
		{
			state = ((int)( diff * recip_precision + 1))>>1;
			if(oriFloat >= pred)
			{
				type[i] = half_interval + state;
				pred = pred + state * double_realpreci;
			}
			else //curData<pred
			{
				type[i] = half_interval - state;
				pred = pred - state * double_realpreci;
			}
				
			//double-check the prediction error in case of machine-epsilon impact	
			if(fabs(oriFloat - pred) > realPrecision)
			{	
				type[i] = 0;				
				compressSingleFloatValue(vce, oriFloat, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDiffBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDiffBytes, vce->curBytes, 4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);		
				
				//listAdd_float(last3CmprsData, vce->data);	
				pred = vce->data;				
			}
			else
			{
				//listAdd_float(last3CmprsData, pred);	
			}	
			// go next
			continue;
		}
		
		//unpredictable data processing		
		type[i] = 0;		
		compressSingleFloatValue(vce, oriFloat, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Float(vce->curBytes, preDiffBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDiffBytes, vce->curBytes, 4);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);

		//listAdd_float(last3CmprsData, vce->data);
		pred = vce->data;	
		
	}//end of for
		
//	char* expSegmentsInBytes;
//	int expSegmentsInBytes_size = convertESCToBytes(esc, &expSegmentsInBytes);
	size_t exactDataNum = exactLeadNumArray->size;
	
	TightDataPointStorageF* tdps = NULL;			
	new_TightDataPointStorageF(&tdps, dataLength, exactDataNum, 
			type, exactMidByteArray->array, exactMidByteArray->size,  
			exactLeadNumArray->array,  
			resiBitArray->array, resiBitArray->size, 
			resiBitsLength,
			realPrecision, medianValue, (char)reqLength, quantization_intervals, 0);

//sdi:Debug
/*	int sum =0;
	for(i=0;i<dataLength;i++)
		if(type[i]==0) sum++;
	printf("opt_quantizations=%d, exactDataNum=%zu, sum=%d\n",quantization_intervals, exactDataNum, sum);
*/	
	//free memory
	free_DIA(exactLeadNumArray);
	free_DIA(resiBitArray);
	free(type);	
	free(vce);
	free(lce);	
	free(exactMidByteArray); //exactMidByteArray->array has been released in free_TightDataPointStorageF(tdps);
	
	return tdps;
}

// compress core algorithm  if success return true else return false
bool SZ_compress_args_float_NoCkRngeNoGzip_1D( unsigned char* newByteData, float *oriData, 
                   size_t dataLength, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f)
{		
	// get tdps
	TightDataPointStorageF* tdps = NULL;	
	tdps = SZ_compress_float_1D_MDQ(oriData, dataLength, realPrecision, valueRangeSize, medianValue_f);	
	if(tdps == NULL)
	  return false;

    // serialize 
	if(!convertTDPStoFlatBytes_float(tdps, newByteData, outSize))
	{
		free_TightDataPointStorageF(tdps);
		return false;
	}
	  
	// check compressed size large than original
	if(*outSize > 1 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1 + sizeof(float)*dataLength)
	{
		return false;
	}	
	free_TightDataPointStorageF(tdps);
	return true;
}


void SZ_compress_args_float_withinRange(unsigned char* newByteData, float *oriData, size_t dataLength, size_t *outSize)
{
	TightDataPointStorageF* tdps = (TightDataPointStorageF*) malloc(sizeof(TightDataPointStorageF));
	memset(tdps, 0, sizeof(TightDataPointStorageF));
	tdps->leadNumArray = NULL;
	tdps->residualMidBits = NULL;
	
	tdps->allSameData = 1;
	tdps->dataSeriesLength = dataLength;
	tdps->exactMidBytes = (unsigned char*)malloc(sizeof(unsigned char)*4);
	tdps->isLossless = 0;
	float value = oriData[0];
	floatToBytes(tdps->exactMidBytes, value);
	tdps->exactMidBytes_size = 4;
	
	size_t tmpOutSize;
	//unsigned char *tmpByteData;
	convertTDPStoFlatBytes_float(tdps, newByteData, &tmpOutSize);

	//*newByteData = (unsigned char*)malloc(sizeof(unsigned char)*12); //for floating-point data (1+3+4+4)
	//memcpy(*newByteData, tmpByteData, 12);
	*outSize = tmpOutSize; //8+SZ_SIZE_TYPE; //8==3+1+4(float_size)
	free_TightDataPointStorageF(tdps);	
}

void cost_start();
double cost_end(const char* tag);
void show_rate( int in_len, int out_len);

int SZ_compress_args_float(float *oriData, size_t r1, unsigned char* newByteData, size_t *outSize, sz_params* params)
{
	int status = SZ_SUCCESS;
	size_t dataLength = r1;
	
	//cost_start();
	// check at least elements count  
	if(dataLength <= MIN_NUM_OF_ELEMENTS)
	{
		printf("warning, input elements count=%d less than %d, so need not do compress.\n", (int)dataLength, MIN_NUM_OF_ELEMENTS);
		return SZ_LITTER_ELEMENT;
	}
	
	float valueRangeSize = 0, medianValue = 0;
	float min = 0;

	min = computeRangeSize_float(oriData, dataLength, &valueRangeSize, &medianValue);	

	// calc max
	float max = min+valueRangeSize;
	params->fmin = min;
	params->fmax = max;
	
	// calc precision
	double realPrecision = 0; 
	if(params->errorBoundMode==PSNR)
	{
		params->errorBoundMode = SZ_ABS;
		realPrecision = params->absErrBound = computeABSErrBoundFromPSNR(params->psnr, (double)params->predThreshold, (double)valueRangeSize);
		//printf("realPrecision=%lf\n", realPrecision);
	}
	else if(params->errorBoundMode==NORM) //norm error = sqrt(sum((xi-xi_)^2))
	{
		params->errorBoundMode = SZ_ABS;
		realPrecision = params->absErrBound = computeABSErrBoundFromNORM_ERR(params->normErr, dataLength);
		//printf("realPrecision=%lf\n", realPrecision);				
	}
	else
	{
		realPrecision = getRealPrecision_float(valueRangeSize, params->errorBoundMode, params->absErrBound, params->relBoundRatio, &status);
		params->absErrBound = realPrecision;
	}	

	//cost_end(" sz_pre_calc");

    //
	// do compress 
	//
	if(valueRangeSize <= realPrecision)
	{		
		// special deal with same data
		SZ_compress_args_float_withinRange(newByteData, oriData, dataLength, outSize);
		return SZ_SUCCESS;
	}

	//
	// first compress with sz
	//		
	size_t tmpOutSize = 0;
	unsigned char* tmpByteData  =  newByteData;
	bool twoStage =  params->szMode != SZ_BEST_SPEED;
	if(twoStage)
	{
		tmpByteData = (unsigned char*)malloc(r1*sizeof(float) + 1024);
	}

	// compress core algorithm
	if(!SZ_compress_args_float_NoCkRngeNoGzip_1D(tmpByteData, oriData, r1, realPrecision, &tmpOutSize, valueRangeSize, medianValue))
	{
		*outSize = 0;
		if(twoStage)
			free(tmpByteData);
		return SZ_ALGORITHM_ERR;
	}

    //cost_end(" sz_first_compress");
	//show_rate(r1*sizeof(float), tmpOutSize);
	//
	//  second compress with Call Zstd or Gzip 
	//
	//cost_start();
	if(!twoStage)
	{
		*outSize = tmpOutSize;
	}
	else
	{
		*outSize = sz_lossless_compress(params->losslessCompressor, tmpByteData, tmpOutSize, newByteData);
		free(tmpByteData);	
	}

	//cost_end(" sz_second_compress");
	//show_rate(r1*sizeof(float), *outSize);
	return SZ_SUCCESS;
}

unsigned int optimize_intervals_float_1D_opt(float *oriData, size_t dataLength, double realPrecision)
{	
	size_t i = 0, radiusIndex;
	float pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = 0;//dataLength/confparams_cpr->sampleDistance;

	float * data_pos = oriData + 2;
	while(data_pos - oriData < dataLength){
		totalSampleSize++;
		pred_value = data_pos[-1];
		pred_err = fabs(pred_value - *data_pos);
		radiusIndex = (unsigned long)((pred_err/realPrecision+1)/2);
		if(radiusIndex>=confparams_cpr->maxRangeRadius)
			radiusIndex = confparams_cpr->maxRangeRadius - 1;			
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
