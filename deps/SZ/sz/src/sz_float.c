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
#include <unistd.h>
#include <math.h>
#include "sz.h"
#include "CompressElement.h"
#include "DynamicByteArray.h"
#include "DynamicIntArray.h"
#include "TightDataPointStorageF.h"
#include "sz_float.h"
#include "sz_float_pwr.h"
#include "szd_float.h"
#include "szd_float_pwr.h"
#include "zlib.h"
#include "rw.h"
#include "sz_float_ts.h"
#include "utility.h"
#include "CacheTable.h"
#include "MultiLevelCacheTableWideInterval.h"
#include "sz_stats.h"

unsigned char* SZ_skip_compress_float(float* data, size_t dataLength, size_t* outSize)
{
	*outSize = dataLength*sizeof(float);
	unsigned char* out = (unsigned char*)malloc(dataLength*sizeof(float));
	memcpy(out, data, dataLength*sizeof(float));
	return out;
}

void computeReqLength_float(double realPrecision, short radExpo, int* reqLength, float* medianValue)
{
	short reqExpo = getPrecisionReqLength_double(realPrecision);
	*reqLength = 9+radExpo - reqExpo+1; //radExpo-reqExpo == reqMantiLength
	if(*reqLength<9)
		*reqLength = 9;
	if(*reqLength>32)
	{	
		*reqLength = 32;
		*medianValue = 0;
	}			
}

inline short computeReqLength_float_MSST19(double realPrecision)
{
	short reqExpo = getPrecisionReqLength_float(realPrecision);
	return 9-reqExpo;
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

unsigned int optimize_intervals_float_2D(float *oriData, size_t r1, size_t r2, double realPrecision)
{	
	size_t i,j, index;
	size_t radiusIndex;
	float pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = (r1-1)*(r2-1)/confparams_cpr->sampleDistance;

	//float max = oriData[0];
	//float min = oriData[0];

	for(i=1;i<r1;i++)
	{
		for(j=1;j<r2;j++)
		{
			if((i+j)%confparams_cpr->sampleDistance==0)
			{
				index = i*r2+j;
				pred_value = oriData[index-1] + oriData[index-r2] - oriData[index-r2-1];
				pred_err = fabs(pred_value - oriData[index]);
				radiusIndex = (unsigned long)((pred_err/realPrecision+1)/2);
				if(radiusIndex>=confparams_cpr->maxRangeRadius)
					radiusIndex = confparams_cpr->maxRangeRadius - 1;
				intervals[radiusIndex]++;

			//	if (max < oriData[index]) max = oriData[index];
			//	if (min > oriData[index]) min = oriData[index];
			}			
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

	//	struct timeval costStart, costEnd;
	//	double cost_est = 0;
	//
	//	gettimeofday(&costStart, NULL);
	//
	//	//compute estimate of bit-rate and distortion
	//	double est_br = 0;
	//	double est_psnr = 0;
	//	double c1 = log2(targetCount)+1;
	//	double c2 = -20.0*log10(realPrecision) + 20.0*log10(max-min) + 10.0*log10(3);
	//
	//	for (i = 0; i < powerOf2/2; i++)
	//	{
	//		int count = intervals[i];
	//		if (count != 0)
	//			est_br += count*log2(count);
	//		est_psnr += count;
	//	}
	//
	//	//compute estimate of bit-rate
	//	est_br -= c1*est_psnr;
	//	est_br /= totalSampleSize;
	//	est_br = -est_br;
	//
	//	//compute estimate of psnr
	//	est_psnr /= totalSampleSize;
	//	printf ("sum of P(i) = %lf\n", est_psnr);
	//	est_psnr = -10.0*log10(est_psnr);
	//	est_psnr += c2;
	//
	//	printf ("estimate bitrate = %.2f\n", est_br);
	//	printf ("estimate psnr = %.2f\n",est_psnr);
	//
	//	gettimeofday(&costEnd, NULL);
	//	cost_est = ((costEnd.tv_sec*1000000+costEnd.tv_usec)-(costStart.tv_sec*1000000+costStart.tv_usec))/1000000.0;
	//
	//	printf ("analysis time = %f\n", cost_est);

	free(intervals);
	//printf("confparams_cpr->maxRangeRadius = %d, accIntervals=%d, powerOf2=%d\n", confparams_cpr->maxRangeRadius, accIntervals, powerOf2);
	return powerOf2;
}

unsigned int optimize_intervals_float_3D(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision)
{	
	size_t i,j,k, index;
	size_t radiusIndex;
	size_t r23=r2*r3;
	float pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = (r1-1)*(r2-1)*(r3-1)/confparams_cpr->sampleDistance;

	//float max = oriData[0];
	//float min = oriData[0];

	for(i=1;i<r1;i++)
	{
		for(j=1;j<r2;j++)
		{
			for(k=1;k<r3;k++)
			{			
				if((i+j+k)%confparams_cpr->sampleDistance==0)
				{
					index = i*r23+j*r3+k;
					pred_value = oriData[index-1] + oriData[index-r3] + oriData[index-r23] 
					- oriData[index-1-r23] - oriData[index-r3-1] - oriData[index-r3-r23] + oriData[index-r3-r23-1];
					pred_err = fabs(pred_value - oriData[index]);
					radiusIndex = (pred_err/realPrecision+1)/2;
					if(radiusIndex>=confparams_cpr->maxRangeRadius)
					{
						radiusIndex = confparams_cpr->maxRangeRadius - 1;
						//printf("radiusIndex=%d\n", radiusIndex);
					}
					intervals[radiusIndex]++;

					//	if (max < oriData[index]) max = oriData[index];
					//	if (min > oriData[index]) min = oriData[index];
				}
			}
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
	
	//	struct timeval costStart, costEnd;
	//	double cost_est = 0;
	//
	//	gettimeofday(&costStart, NULL);
	//
	//	//compute estimate of bit-rate and distortion
	//	double est_br = 0;
	//	double est_psnr = 0;
	//	double c1 = log2(targetCount)+1;
	//	double c2 = -20.0*log10(realPrecision) + 20.0*log10(max-min) + 10.0*log10(3);
	//
	//	for (i = 0; i < powerOf2/2; i++)
	//	{
	//		int count = intervals[i];
	//		if (count != 0)
	//			est_br += count*log2(count);
	//		est_psnr += count;
	//	}
	//
	//	//compute estimate of bit-rate
	//	est_br -= c1*est_psnr;
	//	est_br /= totalSampleSize;
	//	est_br = -est_br;
	//
	//	//compute estimate of psnr
	//	est_psnr /= totalSampleSize;
	//	printf ("sum of P(i) = %lf\n", est_psnr);
	//	est_psnr = -10.0*log10(est_psnr);
	//	est_psnr += c2;
	//
	//	printf ("estimate bitrate = %.2f\n", est_br);
	//	printf ("estimate psnr = %.2f\n",est_psnr);
	//
	//	gettimeofday(&costEnd, NULL);
	//	cost_est = ((costEnd.tv_sec*1000000+costEnd.tv_usec)-(costStart.tv_sec*1000000+costStart.tv_usec))/1000000.0;
	//
	//	printf ("analysis time = %f\n", cost_est);

	free(intervals);
	//printf("targetCount=%d, sum=%d, totalSampleSize=%d, ratio=%f, accIntervals=%d, powerOf2=%d\n", targetCount, sum, totalSampleSize, (double)sum/(double)totalSampleSize, accIntervals, powerOf2);
	return powerOf2;
}


unsigned int optimize_intervals_float_4D(float *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision)
{
	size_t i,j,k,l, index;
	size_t radiusIndex;
	size_t r234=r2*r3*r4;
	size_t r34=r3*r4;
	float pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = (r1-1)*(r2-1)*(r3-1)*(r4-1)/confparams_cpr->sampleDistance;
	for(i=1;i<r1;i++)
	{
		for(j=1;j<r2;j++)
		{
			for(k=1;k<r3;k++)
			{
				for (l=1;l<r4;l++)
				{
					if((i+j+k+l)%confparams_cpr->sampleDistance==0)
					{
						index = i*r234+j*r34+k*r4+l;
						pred_value = oriData[index-1] + oriData[index-r3] + oriData[index-r34]
								- oriData[index-1-r34] - oriData[index-r4-1] - oriData[index-r4-r34] + oriData[index-r4-r34-1];
						pred_err = fabs(pred_value - oriData[index]);
						radiusIndex = (unsigned long)((pred_err/realPrecision+1)/2);
						if(radiusIndex>=confparams_cpr->maxRangeRadius)
							radiusIndex = confparams_cpr->maxRangeRadius - 1;
						intervals[radiusIndex]++;
					}
				}
			}
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

TightDataPointStorageF* SZ_compress_float_1D_MDQ(float *oriData, 
size_t dataLength, float realPrecision, float valueRangeSize, float medianValue_f)
{
#ifdef HAVE_TIMECMPR	
	float* decData = NULL;
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData = (float*)(multisteps->hist_data);
#endif	
	
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
		quantization_intervals = optimize_intervals_float_1D_opt(oriData, dataLength, realPrecision);
	else
		quantization_intervals = exe_params->intvCapacity;
	//updateQuantizationInfo(quantization_intervals);	
	int intvRadius = quantization_intervals/2;

	size_t i;
	int reqLength;
	float medianValue = medianValue_f;
	short radExpo = getExponent_float(valueRangeSize/2);
	
	computeReqLength_float(realPrecision, radExpo, &reqLength, &medianValue);	

	int* type = (int*) malloc(dataLength*sizeof(int));
		
	float* spaceFillingValue = oriData; //
	
	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);
	
	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);
	
	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);
	
	unsigned char preDataBytes[4];
	intToBytes_bigEndian(preDataBytes, 0);
	
	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;
	float last3CmprsData[3] = {0};

	FloatValueCompressElement *vce = (FloatValueCompressElement*)malloc(sizeof(FloatValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));
				
	//add the first data	
	type[0] = 0;
	compressSingleFloatValue(vce, spaceFillingValue[0], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	listAdd_float(last3CmprsData, vce->data);
#ifdef HAVE_TIMECMPR	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData[0] = vce->data;
#endif		
		
	//add the second data
	type[1] = 0;
	compressSingleFloatValue(vce, spaceFillingValue[1], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	listAdd_float(last3CmprsData, vce->data);
#ifdef HAVE_TIMECMPR	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData[1] = vce->data;
#endif
	int state;
	float checkRadius;
	float curData;
	float pred = last3CmprsData[0];
	float predAbsErr;
	checkRadius = (quantization_intervals-1)*realPrecision;
	float interval = 2*realPrecision;
	
	float recip_precision = 1/realPrecision;
	
	for(i=2;i<dataLength;i++)
	{	
		curData = spaceFillingValue[i];
		//pred = 2*last3CmprsData[0] - last3CmprsData[1];
		//pred = last3CmprsData[0];
		predAbsErr = fabsf(curData - pred);	
		if(predAbsErr<checkRadius)
		{
			state = ((int)(predAbsErr*recip_precision+1))>>1;
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
				
			//double-check the prediction error in case of machine-epsilon impact	
			if(fabs(curData-pred)>realPrecision)
			{	
				type[i] = 0;				
				compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);		
				
				//listAdd_float(last3CmprsData, vce->data);	
				pred = vce->data;
#ifdef HAVE_TIMECMPR					
				if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
					decData[i] = vce->data;
#endif					
			}
			else
			{
				//listAdd_float(last3CmprsData, pred);
#ifdef HAVE_TIMECMPR					
				if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
					decData[i] = pred;			
#endif	
			}	
			continue;
		}
		
		//unpredictable data processing		
		type[i] = 0;		
		compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,4);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);

		//listAdd_float(last3CmprsData, vce->data);
		pred = vce->data;
#ifdef HAVE_TIMECMPR
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[i] = vce->data;
#endif	
		
	}//end of for
		
//	char* expSegmentsInBytes;
//	int expSegmentsInBytes_size = convertESCToBytes(esc, &expSegmentsInBytes);
	size_t exactDataNum = exactLeadNumArray->size;
	
	TightDataPointStorageF* tdps;
			
	new_TightDataPointStorageF(&tdps, dataLength, exactDataNum, 
			type, exactMidByteArray->array, exactMidByteArray->size,  
			exactLeadNumArray->array,  
			resiBitArray->array, resiBitArray->size, 
			resiBitsLength,
			realPrecision, medianValue, (char)reqLength, quantization_intervals, NULL, 0, 0);

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

void SZ_compress_args_float_StoreOriData(float* oriData, size_t dataLength, unsigned char** newByteData, size_t *outSize)
{	
	int floatSize=sizeof(float);	
	size_t k = 0, i;
	size_t totalByteLength = 3 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1 + floatSize*dataLength;
	/*No need to malloc because newByteData should always already be allocated with no less totalByteLength.*/
	//*newByteData = (unsigned char*)malloc(totalByteLength);
	
	unsigned char dsLengthBytes[8];
	for (i = 0; i < 3; i++)//3
		(*newByteData)[k++] = versionNumber[i];

	if(exe_params->SZ_SIZE_TYPE==4)//1
		(*newByteData)[k++] = 16; //00010000
	else
		(*newByteData)[k++] = 80;	//01010000: 01000000 indicates the SZ_SIZE_TYPE=8
	
	convertSZParamsToBytes(confparams_cpr, &((*newByteData)[k]));
	k = k + MetaDataByteLength;	
	
	sizeToBytes(dsLengthBytes,dataLength); //SZ_SIZE_TYPE: 4 or 8	
	for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
		(*newByteData)[k++] = dsLengthBytes[i];
		
	if(sysEndianType==BIG_ENDIAN_SYSTEM)
		memcpy((*newByteData)+4+MetaDataByteLength+exe_params->SZ_SIZE_TYPE, oriData, dataLength*floatSize);
	else
	{
		unsigned char* p = (*newByteData)+4+MetaDataByteLength+exe_params->SZ_SIZE_TYPE;
		for(i=0;i<dataLength;i++,p+=floatSize)
			floatToBytes(p, oriData[i]);
	}	
	*outSize = totalByteLength;
}

char SZ_compress_args_float_NoCkRngeNoGzip_1D(int cmprType, unsigned char** newByteData, float *oriData, 
size_t dataLength, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f)
{		
	char compressionType = 0;	
	TightDataPointStorageF* tdps = NULL;	

#ifdef HAVE_TIMECMPR
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
	{
		int timestep = sz_tsc->currentStep;
		if(cmprType == SZ_PERIO_TEMPORAL_COMPRESSION)
		{
			if(timestep % confparams_cpr->snapshotCmprStep != 0)
			{
				tdps = SZ_compress_float_1D_MDQ_ts(oriData, dataLength, multisteps, realPrecision, valueRangeSize, medianValue_f);
				compressionType = 1; //time-series based compression 
			}
			else
			{	
				tdps = SZ_compress_float_1D_MDQ(oriData, dataLength, realPrecision, valueRangeSize, medianValue_f);
				compressionType = 0; //snapshot-based compression
				multisteps->lastSnapshotStep = timestep;
			}
		}
		else if(cmprType == SZ_FORCE_SNAPSHOT_COMPRESSION)
		{
			tdps = SZ_compress_float_1D_MDQ(oriData, dataLength, realPrecision, valueRangeSize, medianValue_f);
			compressionType = 0; //snapshot-based compression
			multisteps->lastSnapshotStep = timestep;			
		}
		else if(cmprType == SZ_FORCE_TEMPORAL_COMPRESSION)
		{
			tdps = SZ_compress_float_1D_MDQ_ts(oriData, dataLength, multisteps, realPrecision, valueRangeSize, medianValue_f);
			compressionType = 1; //time-series based compression 			
		}		
	}
	else
#endif
		tdps = SZ_compress_float_1D_MDQ(oriData, dataLength, realPrecision, valueRangeSize, medianValue_f);	

	convertTDPStoFlatBytes_float(tdps, newByteData, outSize);
	
	if(*outSize>3 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1 + sizeof(float)*dataLength)
		SZ_compress_args_float_StoreOriData(oriData, dataLength, newByteData, outSize);
	
	free_TightDataPointStorageF(tdps);
	return compressionType;
}

TightDataPointStorageF* SZ_compress_float_2D_MDQ(float *oriData, size_t r1, size_t r2, float realPrecision, float valueRangeSize, float medianValue_f)
{
#ifdef HAVE_TIMECMPR
	float* decData = NULL;	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData = (float*)(multisteps->hist_data);
#endif	
	
	float recip_realPrecision = 1/realPrecision;
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_2D_opt(oriData, r1, r2, realPrecision);
		updateQuantizationInfo(quantization_intervals);
	}	
	else
		quantization_intervals = exe_params->intvCapacity;
	int intvRadius = quantization_intervals/2;
	
	size_t i,j; 
	int reqLength;
	float pred1D, pred2D;
	float diff = 0.0;
	float itvNum = 0;
	float *P0, *P1;
		
	size_t dataLength = r1*r2;	
	
	P0 = (float*)malloc(r2*sizeof(float));
	memset(P0, 0, r2*sizeof(float));
	P1 = (float*)malloc(r2*sizeof(float));
	memset(P1, 0, r2*sizeof(float));
		
	float medianValue = medianValue_f;
	short radExpo = getExponent_float(valueRangeSize/2);
	computeReqLength_float(realPrecision, radExpo, &reqLength, &medianValue);	

	int* type = (int*) malloc(dataLength*sizeof(int));
	//type[dataLength]=0;
		
	float* spaceFillingValue = oriData; //

	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);
	
	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);
	
	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);
	
	type[0] = 0;
	unsigned char preDataBytes[4];
	intToBytes_bigEndian(preDataBytes, 0);
	
	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;

	FloatValueCompressElement *vce = (FloatValueCompressElement*)malloc(sizeof(FloatValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));
			
	/* Process Row-0 data 0*/
	type[0] = 0;
	compressSingleFloatValue(vce, spaceFillingValue[0], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	P1[0] = vce->data;
#ifdef HAVE_TIMECMPR	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData[0] = vce->data;
#endif	

	float curData;

	/* Process Row-0 data 1*/
	pred1D = P1[0];
	curData = spaceFillingValue[1];
	diff = curData - pred1D;

	itvNum =  fabs(diff)*recip_realPrecision + 1;

	if (itvNum < quantization_intervals)
	{
		if (diff < 0) itvNum = -itvNum;
		type[1] = (int) (itvNum/2) + intvRadius;
		P1[1] = pred1D + 2 * (type[1] - intvRadius) * realPrecision;	

		//ganrantee comporession error against the case of machine-epsilon
		if(fabs(spaceFillingValue[1]-P1[1])>realPrecision)
		{	
			type[1] = 0;			
			compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);	
			
			P1[1] = vce->data;
		}		
	}
	else
	{
		type[1] = 0;
		compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,4);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
		P1[1] = vce->data;
	}
#ifdef HAVE_TIMECMPR	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData[1] = P1[1];
#endif

    /* Process Row-0 data 2 --> data r2-1 */
	for (j = 2; j < r2; j++)
	{
		pred1D = 2*P1[j-1] - P1[j-2];
		curData = spaceFillingValue[j];
		diff = curData - pred1D;

		itvNum = fabs(diff)*recip_realPrecision + 1;

		if (itvNum < quantization_intervals)
		{
			if (diff < 0) itvNum = -itvNum;
			type[j] = (int) (itvNum/2) + intvRadius;
			P1[j] = pred1D + 2 * (type[j] - intvRadius) * realPrecision;
		
			//ganrantee comporession error against the case of machine-epsilon
			if(fabs(curData-P1[j])>realPrecision)
			{	
				type[j] = 0;				
				compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);	
				
				P1[j] = vce->data;	
			}
		}
		else
		{
			type[j] = 0;
			compressSingleFloatValue(vce,curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P1[j] = vce->data;
		}
#ifdef HAVE_TIMECMPR	
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[j] = P1[j];
#endif		
	}

	/* Process Row-1 --> Row-r1-1 */
	size_t index;
	for (i = 1; i < r1; i++)
	{	
		/* Process row-i data 0 */
		index = i*r2;
		pred1D = P1[0];
		curData = spaceFillingValue[index];
		diff = curData - pred1D;

		itvNum = fabs(diff)*recip_realPrecision + 1;

		if (itvNum < quantization_intervals)
		{
			if (diff < 0) itvNum = -itvNum;
			type[index] = (int) (itvNum/2) + intvRadius;
			P0[0] = pred1D + 2 * (type[index] - intvRadius) * realPrecision;

			//ganrantee comporession error against the case of machine-epsilon
			if(fabs(curData-P0[0])>realPrecision)
			{	
				type[index] = 0;				
				compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);	
				
				P0[0] = vce->data;	
			}
		}
		else
		{
			type[index] = 0;
			compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P0[0] = vce->data;
		}
#ifdef HAVE_TIMECMPR	
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[index] = P0[0];
#endif
									
		/* Process row-i data 1 --> r2-1*/
		for (j = 1; j < r2; j++)
		{
			index = i*r2+j;
			pred2D = P0[j-1] + P1[j] - P1[j-1];

			curData = spaceFillingValue[index];
			diff = curData - pred2D;

			itvNum = fabs(diff)*recip_realPrecision + 1;

			if (itvNum < quantization_intervals)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + intvRadius;
				P0[j] = pred2D + 2 * (type[index] - intvRadius) * realPrecision;
			
				//ganrantee comporession error against the case of machine-epsilon
				if(fabs(curData-P0[j])>realPrecision)
				{	
					type[index] = 0;					
					compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
					updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
					memcpy(preDataBytes,vce->curBytes,4);
					addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);	
					
					P0[j] = vce->data;	
				}			
			}
			else
			{
				type[index] = 0;
				compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P0[j] = vce->data;
			}
#ifdef HAVE_TIMECMPR	
			if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
				decData[index] = P0[j];
#endif			
		}

		float *Pt;
		Pt = P1;
		P1 = P0;
		P0 = Pt;
	}
	
	if(r2!=1)
		free(P0);
	free(P1);			
	size_t exactDataNum = exactLeadNumArray->size;
	
	TightDataPointStorageF* tdps;
			
	new_TightDataPointStorageF(&tdps, dataLength, exactDataNum, 
			type, exactMidByteArray->array, exactMidByteArray->size,  
			exactLeadNumArray->array,  
			resiBitArray->array, resiBitArray->size, 
			resiBitsLength, 
			realPrecision, medianValue, (char)reqLength, quantization_intervals, NULL, 0, 0);

//	printf("exactDataNum=%d, expSegmentsInBytes_size=%d, exactMidByteArray->size=%d\n", 
//			exactDataNum, expSegmentsInBytes_size, exactMidByteArray->size);
	
//	for(i = 3800;i<3844;i++)
//		printf("exactLeadNumArray->array[%d]=%d\n",i,exactLeadNumArray->array[i]);
	
	//free memory
	free_DIA(exactLeadNumArray);
	free_DIA(resiBitArray);
	free(type);
	free(vce);
	free(lce);
	free(exactMidByteArray); //exactMidByteArray->array has been released in free_TightDataPointStorageF(tdps);
	
	return tdps;	
}

/**
 * 
 * Note: @r1 is high dimension
 * 		 @r2 is low dimension 
 * */
char SZ_compress_args_float_NoCkRngeNoGzip_2D(int cmprType, unsigned char** newByteData, float *oriData, size_t r1, size_t r2, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f)
{	
	size_t dataLength = r1*r2;
	char compressionType = 0;	
	TightDataPointStorageF* tdps = NULL; 

#ifdef HAVE_TIMECMPR
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
	{
		int timestep = sz_tsc->currentStep;
		if(cmprType == SZ_PERIO_TEMPORAL_COMPRESSION)
		{
			if(timestep % confparams_cpr->snapshotCmprStep != 0)
			{
				tdps = SZ_compress_float_1D_MDQ_ts(oriData, dataLength, multisteps, realPrecision, valueRangeSize, medianValue_f);
				compressionType = 1; //time-series based compression 
			}
			else
			{	
				tdps = SZ_compress_float_2D_MDQ(oriData, r1, r2, realPrecision, valueRangeSize, medianValue_f);
				compressionType = 0; //snapshot-based compression
				multisteps->lastSnapshotStep = timestep;
			}					
		}
		else if(cmprType == SZ_FORCE_SNAPSHOT_COMPRESSION)
		{
			tdps = SZ_compress_float_2D_MDQ(oriData, r1, r2, realPrecision, valueRangeSize, medianValue_f);
			compressionType = 0; //snapshot-based compression
			multisteps->lastSnapshotStep = timestep;			
		}
		else if(cmprType == SZ_FORCE_TEMPORAL_COMPRESSION)
		{
			tdps = SZ_compress_float_1D_MDQ_ts(oriData, dataLength, multisteps, realPrecision, valueRangeSize, medianValue_f);
			compressionType = 1; //time-series based compression 			
		}
	}
	else
#endif
		tdps = SZ_compress_float_2D_MDQ(oriData, r1, r2, realPrecision, valueRangeSize, medianValue_f);	

	convertTDPStoFlatBytes_float(tdps, newByteData, outSize);

	if(*outSize>3 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1 + sizeof(float)*dataLength)
		SZ_compress_args_float_StoreOriData(oriData, dataLength, newByteData, outSize);
	
	free_TightDataPointStorageF(tdps);	
	
	return compressionType;
}

TightDataPointStorageF* SZ_compress_float_3D_MDQ(float *oriData, size_t r1, size_t r2, size_t r3, float realPrecision, float valueRangeSize, float medianValue_f)
{
#ifdef HAVE_TIMECMPR	
	float* decData = NULL;
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData = (float*)(multisteps->hist_data);
#endif		

	float recip_realPrecision = 1/realPrecision;
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_3D_opt(oriData, r1, r2, r3, realPrecision);
		updateQuantizationInfo(quantization_intervals);
	}	
	else
		quantization_intervals = exe_params->intvCapacity;
	int intvRadius = quantization_intervals/2;	
		
	size_t i,j,k; 
	int reqLength;
	float pred1D, pred2D, pred3D;
	float diff = 0.0;
	float itvNum = 0;
	float *P0, *P1;

	size_t dataLength = r1*r2*r3;
	size_t r23 = r2*r3;
	P0 = (float*)malloc(r23*sizeof(float));
	P1 = (float*)malloc(r23*sizeof(float));

	float medianValue = medianValue_f;
	short radExpo = getExponent_float(valueRangeSize/2);
	computeReqLength_float(realPrecision, radExpo, &reqLength, &medianValue);	

	int* type = (int*) malloc(dataLength*sizeof(int));

	float* spaceFillingValue = oriData; //

	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);

	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);

	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);

	type[0] = 0;

	unsigned char preDataBytes[8];
	longToBytes_bigEndian(preDataBytes, 0);

	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;

	FloatValueCompressElement *vce = (FloatValueCompressElement*)malloc(sizeof(FloatValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));


	///////////////////////////	Process layer-0 ///////////////////////////
	/* Process Row-0 data 0*/
	type[0] = 0;
	compressSingleFloatValue(vce, spaceFillingValue[0], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	P1[0] = vce->data;
#ifdef HAVE_TIMECMPR	
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[0] = P1[0];
#endif

	float curData;

	/* Process Row-0 data 1*/
	pred1D = P1[0];
	curData = spaceFillingValue[1];
	diff = curData - pred1D;

	itvNum = fabs(diff)*recip_realPrecision + 1;

	if (itvNum < quantization_intervals)
	{
		if (diff < 0) itvNum = -itvNum;
		type[1] = (int) (itvNum/2) + intvRadius;
		P1[1] = pred1D + 2 * (type[1] - intvRadius) * realPrecision;
		
		//ganrantee comporession error against the case of machine-epsilon
		if(fabs(curData-P1[1])>realPrecision)
		{	
			type[1] = 0;			
			compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);	
			
			P1[1] = vce->data;	
		}				
	}
	else
	{
		type[1] = 0;
		compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,4);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
		P1[1] = vce->data;
	}
#ifdef HAVE_TIMECMPR	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData[1] = P1[1];
#endif

    /* Process Row-0 data 2 --> data r3-1 */
	for (j = 2; j < r3; j++)
	{
		pred1D = 2*P1[j-1] - P1[j-2];
		curData = spaceFillingValue[j];
		diff = curData - pred1D;

		itvNum = fabs(diff)*recip_realPrecision + 1;

		if (itvNum < quantization_intervals)
		{
			if (diff < 0) itvNum = -itvNum;
			type[j] = (int) (itvNum/2) + intvRadius;
			P1[j] = pred1D + 2 * (type[j] - intvRadius) * realPrecision;
			
			//ganrantee comporession error against the case of machine-epsilon
			if(fabs(curData-P1[j])>realPrecision)
			{	
				type[j] = 0;				
				compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);	
				
				P1[j] = vce->data;	
			}			
		}
		else
		{
			type[j] = 0;
			compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P1[j] = vce->data;
		}
#ifdef HAVE_TIMECMPR	
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[j] = P1[j];
#endif		
	}

	/* Process Row-1 --> Row-r2-1 */
	size_t index;
	for (i = 1; i < r2; i++)
	{
		/* Process row-i data 0 */
		index = i*r3;	
		pred1D = P1[index-r3];
		curData = spaceFillingValue[index];
		diff = curData - pred1D;

		itvNum = fabs(diff)*recip_realPrecision + 1;

		if (itvNum < quantization_intervals)
		{
			if (diff < 0) itvNum = -itvNum;
			type[index] = (int) (itvNum/2) + intvRadius;
			P1[index] = pred1D + 2 * (type[index] - intvRadius) * realPrecision;
			
			//ganrantee comporession error against the case of machine-epsilon
			if(fabs(curData-P1[index])>realPrecision)
			{	
				type[index] = 0;				
				compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);	
				
				P1[index] = vce->data;	
			}			
		}
		else
		{
			type[index] = 0;
			compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P1[index] = vce->data;
		}
#ifdef HAVE_TIMECMPR	
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[index] = P1[index];
#endif		

		/* Process row-i data 1 --> data r3-1*/
		for (j = 1; j < r3; j++)
		{
			index = i*r3+j;
			pred2D = P1[index-1] + P1[index-r3] - P1[index-r3-1];

			curData = spaceFillingValue[index];
			diff = curData - pred2D;

			itvNum = fabs(diff)*recip_realPrecision + 1;

			if (itvNum < quantization_intervals)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + intvRadius;
				P1[index] = pred2D + 2 * (type[index] - intvRadius) * realPrecision;
				
				//ganrantee comporession error against the case of machine-epsilon
				if(fabs(curData-P1[index])>realPrecision)
				{	
					type[index] = 0;					
					compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
					updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
					memcpy(preDataBytes,vce->curBytes,4);
					addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);	
					
					P1[index] = vce->data;	
				}				
			}
			else
			{
				type[index] = 0;
				compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P1[index] = vce->data;
			}
#ifdef HAVE_TIMECMPR	
			if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
				decData[index] = P1[index];
#endif			
		}
	}


	///////////////////////////	Process layer-1 --> layer-r1-1 ///////////////////////////

	for (k = 1; k < r1; k++)
	{
		/* Process Row-0 data 0*/
		index = k*r23;
		pred1D = P1[0];
		curData = spaceFillingValue[index];
		diff = curData - pred1D;

		itvNum = fabs(diff)*recip_realPrecision + 1;

		if (itvNum < quantization_intervals)
		{
			if (diff < 0) itvNum = -itvNum;
			type[index] = (int) (itvNum/2) + intvRadius;
			P0[0] = pred1D + 2 * (type[index] - intvRadius) * realPrecision;
			
			//ganrantee comporession error against the case of machine-epsilon
			if(fabs(curData-P0[0])>realPrecision)
			{	
				type[index] = 0;				
				compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);	
				
				P0[0] = vce->data;	
			}			
		}
		else
		{
			type[index] = 0;
			compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P0[0] = vce->data;
		}
#ifdef HAVE_TIMECMPR	
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[index] = P0[0];
#endif

	    /* Process Row-0 data 1 --> data r3-1 */
		for (j = 1; j < r3; j++)
		{
			//index = k*r2*r3+j;
			index ++;
			pred2D = P0[j-1] + P1[j] - P1[j-1];
			curData = spaceFillingValue[index];
			diff = spaceFillingValue[index] - pred2D;

			itvNum = fabs(diff)*recip_realPrecision + 1;

			if (itvNum < quantization_intervals)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + intvRadius;
				P0[j] = pred2D + 2 * (type[index] - intvRadius) * realPrecision;
				//ganrantee comporession error against the case of machine-epsilon
				if(fabs(curData-P0[j])>realPrecision)
				{	
					type[index] = 0;					
					compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
					updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
					memcpy(preDataBytes,vce->curBytes,4);
					addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);	
					
					P0[j] = vce->data;	
				}
			}
			else
			{
				type[index] = 0;
				compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P0[j] = vce->data;
			}
#ifdef HAVE_TIMECMPR	
			if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
				decData[index] = P0[j];
#endif			
		}

	    /* Process Row-1 --> Row-r2-1 */
		size_t index2D;
		for (i = 1; i < r2; i++)
		{
			/* Process Row-i data 0 */
			index = k*r23 + i*r3;
			index2D = i*r3;		
			pred2D = P0[index2D-r3] + P1[index2D] - P1[index2D-r3];
			curData = spaceFillingValue[index];
			diff = spaceFillingValue[index] - pred2D;

			itvNum = fabs(diff)*recip_realPrecision + 1;

			if (itvNum < quantization_intervals)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + intvRadius;
				P0[index2D] = pred2D + 2 * (type[index] - intvRadius) * realPrecision;
				//ganrantee comporession error against the case of machine-epsilon
				if(fabs(curData-P0[index2D])>realPrecision)
				{	
					type[index] = 0;					
					compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
					updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
					memcpy(preDataBytes,vce->curBytes,4);
					addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);	
					
					P0[index2D] = vce->data;	
				}				
			}
			else
			{
				type[index] = 0;
				compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P0[index2D] = vce->data;
			}
#ifdef HAVE_TIMECMPR	
			if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
				decData[index] = P0[index2D];
#endif			

			/* Process Row-i data 1 --> data r3-1 */
			for (j = 1; j < r3; j++)
			{
//				if(k==63&&i==43&&j==27)
//					printf("i=%d\n", i);
				//index = k*r2*r3 + i*r3 + j;			
				index ++;
				index2D = i*r3 + j;
				pred3D = P0[index2D-1] + P0[index2D-r3]+ P1[index2D] - P0[index2D-r3-1] - P1[index2D-r3] - P1[index2D-1] + P1[index2D-r3-1];
				curData = spaceFillingValue[index];
				diff = curData - pred3D;

				itvNum = fabs(diff)*recip_realPrecision + 1;

				if (itvNum < quantization_intervals)
				{
					if (diff < 0) itvNum = -itvNum;
					type[index] = (int) (itvNum/2) + intvRadius;
					P0[index2D] = pred3D + 2 * (type[index] - intvRadius) * realPrecision;
					
					//ganrantee comporession error against the case of machine-epsilon
					if(fabs(curData-P0[index2D])>realPrecision)
					{	
						type[index] = 0;						
						compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
						updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
						memcpy(preDataBytes,vce->curBytes,4);
						addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);	
						
						P0[index2D] = vce->data;	
					}					
				}
				else
				{
					type[index] = 0;
					compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
					updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
					memcpy(preDataBytes,vce->curBytes,4);
					addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
					P0[index2D] = vce->data;
				}
#ifdef HAVE_TIMECMPR	
				if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
					decData[index] = P0[index2D];
#endif				
			}
		}

		float *Pt;
		Pt = P1;
		P1 = P0;
		P0 = Pt;
	}
	if(r23!=1)
		free(P0);
	free(P1);
	size_t exactDataNum = exactLeadNumArray->size;

	TightDataPointStorageF* tdps;

	new_TightDataPointStorageF(&tdps, dataLength, exactDataNum,
			type, exactMidByteArray->array, exactMidByteArray->size,
			exactLeadNumArray->array,
			resiBitArray->array, resiBitArray->size,
			resiBitsLength, 
			realPrecision, medianValue, (char)reqLength, quantization_intervals, NULL, 0, 0);

//sdi:Debug
/*	int sum =0;
	for(i=0;i<dataLength;i++)
		if(type[i]==0) sum++;
	printf("opt_quantizations=%d, exactDataNum=%d, sum=%d\n",quantization_intervals, exactDataNum, sum);*/


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

/**
 * 
 * @cmprType compressionType (SZ_FORCE_SNAPSHOT_COMPRESSION, SZ_FORCE_TEMPORAL_COMPRESSION or SZ_PEORI_TEMPORAL_COMPRESSION)
 * 
 * */
char SZ_compress_args_float_NoCkRngeNoGzip_3D(int cmprType, unsigned char** newByteData, float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f)
{
	size_t dataLength = r1*r2*r3;
	char compressionType = 0;	
	TightDataPointStorageF* tdps = NULL; 

#ifdef HAVE_TIMECMPR
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
	{
		int timestep = sz_tsc->currentStep;
		if(cmprType == SZ_PERIO_TEMPORAL_COMPRESSION)
		{
			if(timestep % confparams_cpr->snapshotCmprStep != 0)
			{
				tdps = SZ_compress_float_1D_MDQ_ts(oriData, dataLength, multisteps, realPrecision, valueRangeSize, medianValue_f);
				compressionType = 1; //time-series based compression 
			}
			else
			{
				if(confparams_cpr->withRegression == SZ_NO_REGRESSION)	
					tdps = SZ_compress_float_3D_MDQ(oriData, r1, r2, r3, realPrecision, valueRangeSize, medianValue_f);
				else
					*newByteData = SZ_compress_float_3D_MDQ_nonblocked_with_blocked_regression(oriData, r1, r2, r3, realPrecision, outSize);
				compressionType = 0; //snapshot-based compression
				multisteps->lastSnapshotStep = timestep;
			}					
		}
		else if(cmprType == SZ_FORCE_SNAPSHOT_COMPRESSION)
		{
			if(confparams_cpr->withRegression == SZ_NO_REGRESSION)	
				tdps = SZ_compress_float_3D_MDQ(oriData, r1, r2, r3, realPrecision, valueRangeSize, medianValue_f);
			else
				*newByteData = SZ_compress_float_3D_MDQ_nonblocked_with_blocked_regression(oriData, r1, r2, r3, realPrecision, outSize);
			compressionType = 0; //snapshot-based compression
			multisteps->lastSnapshotStep = timestep;			
		}
		else if(cmprType == SZ_FORCE_TEMPORAL_COMPRESSION)
		{
			tdps = SZ_compress_float_1D_MDQ_ts(oriData, dataLength, multisteps, realPrecision, valueRangeSize, medianValue_f);
			compressionType = 1; //time-series based compression 			
		}
	}
	else
#endif
		tdps = SZ_compress_float_3D_MDQ(oriData, r1, r2, r3, realPrecision, valueRangeSize, medianValue_f);

	if(tdps!=NULL)
	{
		convertTDPStoFlatBytes_float(tdps, newByteData, outSize);
		if(*outSize>3 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1 + sizeof(float)*dataLength)
			SZ_compress_args_float_StoreOriData(oriData, dataLength, newByteData, outSize);
		free_TightDataPointStorageF(tdps);
	}

	return compressionType;
}

TightDataPointStorageF* SZ_compress_float_4D_MDQ(float *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision, float valueRangeSize, float medianValue_f)
{
	float recip_realPrecision = 1/realPrecision;
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_4D(oriData, r1, r2, r3, r4, realPrecision);
		updateQuantizationInfo(quantization_intervals);
	}
	else
		quantization_intervals = exe_params->intvCapacity;
	int intvRadius = quantization_intervals/2;

	size_t i,j,k; 
	int reqLength;
	float pred1D, pred2D, pred3D;
	float diff = 0.0;
	double itvNum = 0;
	float *P0, *P1;

	size_t dataLength = r1*r2*r3*r4;

	size_t r234 = r2*r3*r4;
	size_t r34 = r3*r4;

	P0 = (float*)malloc(r34*sizeof(float));
	P1 = (float*)malloc(r34*sizeof(float));

	float medianValue = medianValue_f;
	short radExpo = getExponent_float(valueRangeSize/2);
	computeReqLength_float(realPrecision, radExpo, &reqLength, &medianValue);

	int* type = (int*) malloc(dataLength*sizeof(int));

	float* spaceFillingValue = oriData; //

	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);

	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);

	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);

	unsigned char preDataBytes[4];
	intToBytes_bigEndian(preDataBytes, 0);

	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;

	FloatValueCompressElement *vce = (FloatValueCompressElement*)malloc(sizeof(FloatValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));


	size_t l;
	for (l = 0; l < r1; l++)
	{

		///////////////////////////	Process layer-0 ///////////////////////////
		/* Process Row-0 data 0*/
		size_t index = l*r234;
		size_t index2D = 0;

		type[index] = 0;
		compressSingleFloatValue(vce, spaceFillingValue[index], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,4);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
		P1[index2D] = vce->data;

		/* Process Row-0 data 1*/
		index = l*r234+1;
		index2D = 1;

		pred1D = P1[index2D-1];
		diff = spaceFillingValue[index] - pred1D;

		itvNum = fabs(diff)/realPrecision + 1;

		if (itvNum < quantization_intervals)
		{
			if (diff < 0) itvNum = -itvNum;
			type[index] = (int) (itvNum/2) + intvRadius;
			P1[index2D] = pred1D + 2 * (type[index] - intvRadius) * realPrecision;
		}
		else
		{
			type[index] = 0;
			compressSingleFloatValue(vce, spaceFillingValue[index], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P1[index2D] = vce->data;
		}

		/* Process Row-0 data 2 --> data r4-1 */
		for (j = 2; j < r4; j++)
		{
			index = l*r234+j;
			index2D = j;

			pred1D = 2*P1[index2D-1] - P1[index2D-2];
			diff = spaceFillingValue[index] - pred1D;

			itvNum = fabs(diff)*recip_realPrecision + 1;

			if (itvNum < quantization_intervals)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + intvRadius;
				P1[index2D] = pred1D + 2 * (type[index] - intvRadius) * realPrecision;
			}
			else
			{
				type[index] = 0;
				compressSingleFloatValue(vce, spaceFillingValue[index], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P1[index2D] = vce->data;
			}
		}

		/* Process Row-1 --> Row-r3-1 */
		for (i = 1; i < r3; i++)
		{
			/* Process row-i data 0 */
			index = l*r234+i*r4;
			index2D = i*r4;

			pred1D = P1[index2D-r4];
			diff = spaceFillingValue[index] - pred1D;

			itvNum = fabs(diff)*recip_realPrecision + 1;

			if (itvNum < quantization_intervals)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + intvRadius;
				P1[index2D] = pred1D + 2 * (type[index] - intvRadius) * realPrecision;
			}
			else
			{
				type[index] = 0;
				compressSingleFloatValue(vce, spaceFillingValue[index], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P1[index2D] = vce->data;
			}

			/* Process row-i data 1 --> data r4-1*/
			for (j = 1; j < r4; j++)
			{
				index = l*r234+i*r4+j;
				index2D = i*r4+j;

				pred2D = P1[index2D-1] + P1[index2D-r4] - P1[index2D-r4-1];

				diff = spaceFillingValue[index] - pred2D;

				itvNum = fabs(diff)*recip_realPrecision + 1;

				if (itvNum < quantization_intervals)
				{
					if (diff < 0) itvNum = -itvNum;
					type[index] = (int) (itvNum/2) + intvRadius;
					P1[index2D] = pred2D + 2 * (type[index] - intvRadius) * realPrecision;
				}
				else
				{
					type[index] = 0;
					compressSingleFloatValue(vce, spaceFillingValue[index], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
					updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
					memcpy(preDataBytes,vce->curBytes,4);
					addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
					P1[index2D] = vce->data;
				}
			}
		}


		///////////////////////////	Process layer-1 --> layer-r2-1 ///////////////////////////

		for (k = 1; k < r2; k++)
		{
			/* Process Row-0 data 0*/
			index = l*r234+k*r34;
			index2D = 0;

			pred1D = P1[index2D];
			diff = spaceFillingValue[index] - pred1D;

			itvNum = fabs(diff)*recip_realPrecision + 1;

			if (itvNum < quantization_intervals)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + intvRadius;
				P0[index2D] = pred1D + 2 * (type[index] - intvRadius) * realPrecision;
			}
			else
			{
				type[index] = 0;
				compressSingleFloatValue(vce, spaceFillingValue[index], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P0[index2D] = vce->data;
			}

			/* Process Row-0 data 1 --> data r4-1 */
			for (j = 1; j < r4; j++)
			{
				index = l*r234+k*r34+j;
				index2D = j;

				pred2D = P0[index2D-1] + P1[index2D] - P1[index2D-1];
				diff = spaceFillingValue[index] - pred2D;

				itvNum = fabs(diff)*recip_realPrecision + 1;

				if (itvNum < quantization_intervals)
				{
					if (diff < 0) itvNum = -itvNum;
					type[index] = (int) (itvNum/2) + intvRadius;
					P0[index2D] = pred2D + 2 * (type[index] - intvRadius) * realPrecision;
				}
				else
				{
					type[index] = 0;
					compressSingleFloatValue(vce, spaceFillingValue[index], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
					updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
					memcpy(preDataBytes,vce->curBytes,4);
					addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
					P0[index2D] = vce->data;
				}
			}

			/* Process Row-1 --> Row-r3-1 */
			for (i = 1; i < r3; i++)
			{
				/* Process Row-i data 0 */
				index = l*r234+k*r34+i*r4;
				index2D = i*r4;

				pred2D = P0[index2D-r4] + P1[index2D] - P1[index2D-r4];
				diff = spaceFillingValue[index] - pred2D;

				itvNum = fabs(diff)*recip_realPrecision + 1;

				if (itvNum < quantization_intervals)
				{
					if (diff < 0) itvNum = -itvNum;
					type[index] = (int) (itvNum/2) + intvRadius;
					P0[index2D] = pred2D + 2 * (type[index] - intvRadius) * realPrecision;
				}
				else
				{
					type[index] = 0;
					compressSingleFloatValue(vce, spaceFillingValue[index], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
					updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
					memcpy(preDataBytes,vce->curBytes,4);
					addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
					P0[index2D] = vce->data;
				}

				/* Process Row-i data 1 --> data r4-1 */
				for (j = 1; j < r4; j++)
				{
					index = l*r234+k*r34+i*r4+j;
					index2D = i*r4+j;

					pred3D = P0[index2D-1] + P0[index2D-r4]+ P1[index2D] - P0[index2D-r4-1] - P1[index2D-r4] - P1[index2D-1] + P1[index2D-r4-1];
					diff = spaceFillingValue[index] - pred3D;


					itvNum = fabs(diff)*recip_realPrecision + 1;

					if (itvNum < quantization_intervals)
					{
						if (diff < 0) itvNum = -itvNum;
						type[index] = (int) (itvNum/2) + intvRadius;
						P0[index2D] = pred3D + 2 * (type[index] - intvRadius) * realPrecision;
					}
					else
					{
						type[index] = 0;
						compressSingleFloatValue(vce, spaceFillingValue[index], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
						updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
						memcpy(preDataBytes,vce->curBytes,4);
						addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
						P0[index2D] = vce->data;
					}
				}
			}

			float *Pt;
			Pt = P1;
			P1 = P0;
			P0 = Pt;
		}
	}

	free(P0);
	free(P1);
	size_t exactDataNum = exactLeadNumArray->size;

	TightDataPointStorageF* tdps;

	new_TightDataPointStorageF(&tdps, dataLength, exactDataNum,
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

	return tdps;
}

char SZ_compress_args_float_NoCkRngeNoGzip_4D(unsigned char** newByteData, float *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f)
{
	TightDataPointStorageF* tdps = SZ_compress_float_4D_MDQ(oriData, r1, r2, r3, r4, realPrecision, valueRangeSize, medianValue_f);

	convertTDPStoFlatBytes_float(tdps, newByteData, outSize);

	int dataLength = r1*r2*r3*r4;
	if(*outSize>3 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1 + sizeof(float)*dataLength)
		SZ_compress_args_float_StoreOriData(oriData, dataLength, newByteData, outSize);

	free_TightDataPointStorageF(tdps);
	
	return 0;
}

/*MSST19*/
TightDataPointStorageF* SZ_compress_float_1D_MDQ_MSST19(float *oriData, 
size_t dataLength, double realPrecision, float valueRangeSize, float medianValue_f)
{
#ifdef HAVE_TIMECMPR	
	float* decData = NULL;
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData = (float*)(multisteps->hist_data);
#endif	

	//struct ClockPoint clockPointBuild;
	//TimeDurationStart("build", &clockPointBuild);
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
		quantization_intervals = optimize_intervals_float_1D_opt_MSST19(oriData, dataLength, realPrecision);
	else
		quantization_intervals = exe_params->intvCapacity;
	//updateQuantizationInfo(quantization_intervals);
	int intvRadius = quantization_intervals/2;
	
	double* precisionTable = (double*)malloc(sizeof(double) * quantization_intervals);
	double inv = 2.0-pow(2, -(confparams_cpr->plus_bits));
    for(int i=0; i<quantization_intervals; i++){
        double test = pow((1+realPrecision), inv*(i - intvRadius));
        precisionTable[i] = test;
//        if(i>30000 && i<40000)
//			printf("%d %.30G\n", i, test);
    }
    //float smallest_precision = precisionTable[0], largest_precision = precisionTable[quantization_intervals-1];
	struct TopLevelTableWideInterval levelTable;
    MultiLevelCacheTableWideIntervalBuild(&levelTable, precisionTable, quantization_intervals, realPrecision, confparams_cpr->plus_bits);

	size_t i;
	int reqLength;
	float medianValue = medianValue_f;
	//float medianInverse = 1 / medianValue_f;
	//short radExpo = getExponent_float(valueRangeSize/2);
	
	reqLength = computeReqLength_float_MSST19(realPrecision);	

	int* type = (int*) malloc(dataLength*sizeof(int));
		
	float* spaceFillingValue = oriData; //
	
	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, dataLength/2/8);
	
	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, dataLength/2);
	
	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);
	
	unsigned char preDataBytes[4];
	intToBytes_bigEndian(preDataBytes, 0);
	
	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;
	float last3CmprsData[3] = {0};

	//size_t miss=0, hit=0;

	FloatValueCompressElement *vce = (FloatValueCompressElement*)malloc(sizeof(FloatValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));
				
	//add the first data	
	type[0] = 0;
	compressSingleFloatValue_MSST19(vce, spaceFillingValue[0], realPrecision, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	listAdd_float(last3CmprsData, vce->data);
	//miss++;
#ifdef HAVE_TIMECMPR	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData[0] = vce->data;
#endif		
		
	//add the second data
	type[1] = 0;
	compressSingleFloatValue_MSST19(vce, spaceFillingValue[1], realPrecision, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	listAdd_float(last3CmprsData, vce->data);
	//miss++;
#ifdef HAVE_TIMECMPR	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData[1] = vce->data;
#endif
	int state;
	//double checkRadius;
	float curData;
	float pred = vce->data;

    double predRelErrRatio;

	const uint64_t top = levelTable.topIndex, base = levelTable.baseIndex;
	const uint64_t range = top - base;
	const int bits = levelTable.bits;
	uint64_t* const buffer = (uint64_t*)&predRelErrRatio;
	const int shift = 52-bits;
	uint64_t expoIndex, mantiIndex;
	uint16_t* tables[range+1];
	for(int i=0; i<=range; i++){
		tables[i] = levelTable.subTables[i].table;
	}

	for(i=2;i<dataLength;i++)
	{
		curData = spaceFillingValue[i];
		predRelErrRatio = curData / pred;

		expoIndex = ((*buffer & 0x7fffffffffffffff) >> 52) - base;
		if(expoIndex <= range){
			mantiIndex = (*buffer & 0x000fffffffffffff) >> shift;
			state = tables[expoIndex][mantiIndex];
		}else{
			state = 0;
		}

		if(state)
		{
			type[i] = state;
			pred *= precisionTable[state];
			//hit++;
			continue;
		}

		//unpredictable data processing
		type[i] = 0;
		compressSingleFloatValue_MSST19(vce, curData, realPrecision, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,4);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
		pred =  vce->data;
		//miss++;
#ifdef HAVE_TIMECMPR
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[i] = vce->data;
#endif	
		
	}//end of for
		
//	printf("miss:%d, hit:%d\n", miss, hit);

	size_t exactDataNum = exactLeadNumArray->size;
	
	TightDataPointStorageF* tdps;
			
	new_TightDataPointStorageF(&tdps, dataLength, exactDataNum, 
			type, exactMidByteArray->array, exactMidByteArray->size,  
			exactLeadNumArray->array,  
			resiBitArray->array, resiBitArray->size, 
			resiBitsLength,
			realPrecision, medianValue, (char)reqLength, quantization_intervals, NULL, 0, 0);
    tdps->plus_bits = confparams_cpr->plus_bits;
	
	//free memory
	free_DIA(exactLeadNumArray);
	free_DIA(resiBitArray);
	free(type);	
	free(vce);
	free(lce);	
	free(exactMidByteArray); //exactMidByteArray->array has been released in free_TightDataPointStorageF(tdps);
	free(precisionTable);
	freeTopLevelTableWideInterval(&levelTable);
	return tdps;
}

TightDataPointStorageF* SZ_compress_float_2D_MDQ_MSST19(float *oriData, size_t r1, size_t r2, double realPrecision, float valueRangeSize, float medianValue_f)
{
#ifdef HAVE_TIMECMPR
	float* decData = NULL;	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData = (float*)(multisteps->hist_data);
#endif	
	
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_2D_opt_MSST19(oriData, r1, r2, realPrecision);
		updateQuantizationInfo(quantization_intervals);
	}	
	else
		quantization_intervals = exe_params->intvCapacity;
	int intvRadius = quantization_intervals/2;

	double* precisionTable = (double*)malloc(sizeof(double) * quantization_intervals);
	double inv = 2.0-pow(2, -(confparams_cpr->plus_bits));
	for(int i=0; i<quantization_intervals; i++){
		double test = pow((1+realPrecision), inv*(i - intvRadius));
		precisionTable[i] = test;
	}
	//double smallest_precision = precisionTable[0], largest_precision = precisionTable[quantization_intervals-1];
	struct TopLevelTableWideInterval levelTable;
	MultiLevelCacheTableWideIntervalBuild(&levelTable, precisionTable, quantization_intervals, realPrecision, confparams_cpr->plus_bits);

	size_t i,j; 
	int reqLength;
	float pred1D, pred2D;
	//float diff = 0.0;
	//double itvNum = 0;
	float *P0, *P1;
	double predRelErrRatio;
		
	size_t dataLength = r1*r2;	
	
	P0 = (float*)malloc(r2*sizeof(float));
	memset(P0, 0, r2*sizeof(float));
	P1 = (float*)malloc(r2*sizeof(float));
	memset(P1, 0, r2*sizeof(float));
		
	float medianValue = medianValue_f;
	//float medianValueInverse = 1 / medianValue_f;
	//short radExpo = getExponent_float(valueRangeSize/2);
	reqLength = computeReqLength_double_MSST19(realPrecision);	

	int* type = (int*) malloc(dataLength*sizeof(int));
	//type[dataLength]=0;
		
	float* spaceFillingValue = oriData; //

	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);
	
	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);
	
	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);
	
	type[0] = 0;
	unsigned char preDataBytes[4];
	intToBytes_bigEndian(preDataBytes, 0);
	
	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;

	FloatValueCompressElement *vce = (FloatValueCompressElement*)malloc(sizeof(FloatValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));

    const uint64_t top = levelTable.topIndex, base = levelTable.baseIndex;
    const uint64_t range = top - base;
    const int bits = levelTable.bits;
    uint64_t* const buffer = (uint64_t*)&predRelErrRatio;
    const int shift = 52-bits;
    uint64_t expoIndex, mantiIndex;
    uint16_t* tables[range+1];
    for(int i=0; i<=range; i++){
        tables[i] = levelTable.subTables[i].table;
    }
			
	/* Process Row-0 data 0*/
	type[0] = 0;
	compressSingleFloatValue_MSST19(vce, spaceFillingValue[0], realPrecision, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	P1[0] = vce->data;
#ifdef HAVE_TIMECMPR	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData[0] = vce->data;
#endif	

	float curData;
	int state;

	/* Process Row-0 data 1*/
	pred1D = P1[0];

	curData = spaceFillingValue[1];
	predRelErrRatio = curData / pred1D;

	expoIndex = ((*buffer & 0x7fffffffffffffff) >> 52) - base;
	if(expoIndex <= range){
		mantiIndex = (*buffer & 0x000fffffffffffff) >> shift;
		state = tables[expoIndex][mantiIndex];
	}else{
		state = 0;
	}

	if (state)
	{
		type[1] = state;
		P1[1] = fabs(pred1D) * precisionTable[state];
	}
	else
	{
		type[1] = 0;
		compressSingleFloatValue_MSST19(vce, curData, realPrecision, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,4);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
		P1[1] = vce->data;
	}
#ifdef HAVE_TIMECMPR	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData[1] = P1[1];
#endif

    /* Process Row-0 data 2 --> data r2-1 */
	for (j = 2; j < r2; j++)
	{
		pred1D = P1[j-1] * P1[j-1] / P1[j-2];
		curData = spaceFillingValue[j];
		predRelErrRatio = curData / pred1D;

		expoIndex = ((*buffer & 0x7fffffffffffffff) >> 52) - base;
		if(expoIndex <= range){
			mantiIndex = (*buffer & 0x000fffffffffffff) >> shift;
			state = tables[expoIndex][mantiIndex];
		}else{
			state = 0;
		}

		if (state)
		{
			type[j] = state;
			P1[j] = fabs(pred1D) * precisionTable[state];
		}
		else
		{
			type[j] = 0;
			compressSingleFloatValue_MSST19(vce, curData, realPrecision, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P1[j] = vce->data;
		}
#ifdef HAVE_TIMECMPR	
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[j] = P1[j];
#endif		
	}

	/* Process Row-1 --> Row-r1-1 */
	size_t index;
	for (i = 1; i < r1; i++)
	{	
		/* Process row-i data 0 */
		index = i*r2;
		pred1D = P1[0];
		curData = spaceFillingValue[index];
		predRelErrRatio = curData / pred1D;

		expoIndex = ((*buffer & 0x7fffffffffffffff) >> 52) - base;
		if(expoIndex <= range){
			mantiIndex = (*buffer & 0x000fffffffffffff) >> shift;
			state = tables[expoIndex][mantiIndex];
		}else{
			state = 0;
		}

		if (state)
		{
			type[index] = state;
			P0[0] = fabs(pred1D) * precisionTable[state];
		}
		else
		{
			type[index] = 0;
			compressSingleFloatValue_MSST19(vce, curData, realPrecision, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P0[0] = vce->data;
		}
#ifdef HAVE_TIMECMPR	
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[index] = P0[0];
#endif
									
		/* Process row-i data 1 --> r2-1*/
		for (j = 1; j < r2; j++)
		{
			index = i*r2+j;
			pred2D = P0[j-1] * P1[j] / P1[j-1];

			curData = spaceFillingValue[index];
			predRelErrRatio = curData / pred2D;

			expoIndex = ((*buffer & 0x7fffffffffffffff) >> 52) - base;
			if(expoIndex <= range){
				mantiIndex = (*buffer & 0x000fffffffffffff) >> shift;
				state = tables[expoIndex][mantiIndex];
			}else{
				state = 0;
			}

			if (state)
			{
				type[index] = state;
				P0[j] = fabs(pred2D) * precisionTable[state];
			}
			else
			{
				type[index] = 0;
				compressSingleFloatValue_MSST19(vce, curData, realPrecision, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P0[j] = vce->data;
			}
#ifdef HAVE_TIMECMPR	
			if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
				decData[index] = P0[j];
#endif			
		}

		float *Pt;
		Pt = P1;
		P1 = P0;
		P0 = Pt;
	}
	
	if(r2!=1)
		free(P0);
	free(P1);
	size_t exactDataNum = exactLeadNumArray->size;
	
	TightDataPointStorageF* tdps;
			
	new_TightDataPointStorageF(&tdps, dataLength, exactDataNum, 
			type, exactMidByteArray->array, exactMidByteArray->size,  
			exactLeadNumArray->array,  
			resiBitArray->array, resiBitArray->size, 
			resiBitsLength, 
			realPrecision, medianValue, (char)reqLength, quantization_intervals, NULL, 0, 0);
	tdps->plus_bits = confparams_cpr->plus_bits;

	//free memory
	free_DIA(exactLeadNumArray);
	free_DIA(resiBitArray);
	free(type);
	free(vce);
	free(lce);
	free(exactMidByteArray); //exactMidByteArray->array has been released in free_TightDataPointStorageF(tdps);
	free(precisionTable);
	freeTopLevelTableWideInterval(&levelTable);	
	return tdps;	
}

TightDataPointStorageF* SZ_compress_float_3D_MDQ_MSST19(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, float valueRangeSize, float medianValue_f)
{
#ifdef HAVE_TIMECMPR	
	float* decData = NULL;
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData = (float*)(multisteps->hist_data);
#endif		

	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_3D_opt_MSST19(oriData, r1, r2, r3, realPrecision);
		updateQuantizationInfo(quantization_intervals);
	}	
	else
		quantization_intervals = exe_params->intvCapacity;
	int intvRadius = quantization_intervals/2;

    double* precisionTable = (double*)malloc(sizeof(double) * quantization_intervals);
    double inv = 2.0-pow(2, -(confparams_cpr->plus_bits));
    for(int i=0; i<quantization_intervals; i++){
        double test = pow((1+realPrecision), inv*(i - intvRadius));
        precisionTable[i] = test;
    }
    //double smallest_precision = precisionTable[0], largest_precision = precisionTable[quantization_intervals-1];
    struct TopLevelTableWideInterval levelTable;
    MultiLevelCacheTableWideIntervalBuild(&levelTable, precisionTable, quantization_intervals, realPrecision, confparams_cpr->plus_bits);

    size_t i,j,k;
	int reqLength;
	float pred1D, pred2D, pred3D;
	//float diff = 0.0;
	//double itvNum = 0;
	float *P0, *P1;
    double predRelErrRatio;

	size_t dataLength = r1*r2*r3;
	size_t r23 = r2*r3;
	P0 = (float*)malloc(r23*sizeof(float));
	P1 = (float*)malloc(r23*sizeof(float));

	float medianValue = medianValue_f;
	//float medianValueInverse = 1/ medianValue_f;
	//short radExpo = getExponent_float(valueRangeSize/2);
	reqLength = computeReqLength_float_MSST19(realPrecision);	

	int* type = (int*) malloc(dataLength*sizeof(int));

	float* spaceFillingValue = oriData; //

	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);

	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);

	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);

	unsigned char preDataBytes[4];
	intToBytes_bigEndian(preDataBytes, 0);
	
	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;

	FloatValueCompressElement *vce = (FloatValueCompressElement*)malloc(sizeof(FloatValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));

    const uint64_t top = levelTable.topIndex, base = levelTable.baseIndex;
    const uint64_t range = top - base;
    const int bits = levelTable.bits;
    uint64_t* const buffer = (uint64_t*)&predRelErrRatio;
    const int shift = 52-bits;
    uint64_t expoIndex, mantiIndex;
    uint16_t* tables[range+1];
    for(int i=0; i<=range; i++){
        tables[i] = levelTable.subTables[i].table;
    }
    int state;

    double temp, temp2;


    //size_t miss=0, hit=0;

    ///////////////////////////	Process layer-0 ///////////////////////////
	/* Process Row-0 data 0*/
	type[0] = 0;
	compressSingleFloatValue_MSST19(vce, spaceFillingValue[0], realPrecision, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	P1[0] = vce->data;
	//miss++;
#ifdef HAVE_TIMECMPR	
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[0] = P1[0];
#endif

	float curData;

	/* Process Row-0 data 1*/
	pred1D = P1[0];
	curData = spaceFillingValue[1];
    predRelErrRatio = curData / pred1D;

    expoIndex = ((*buffer & 0x7fffffffffffffff) >> 52) - base;
    if(expoIndex <= range){
        mantiIndex = (*buffer & 0x000fffffffffffff) >> shift;
        state = tables[expoIndex][mantiIndex];
    }else{
        state = 0;
    }

	if (state)
	{
		type[1] = state;
		P1[1] = fabsf(pred1D) * precisionTable[state];
		//hit++;
	}
	else
	{
		type[1] = 0;
		compressSingleFloatValue_MSST19(vce, curData, realPrecision, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,4);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
		P1[1] = vce->data;
		//miss++;
	}
#ifdef HAVE_TIMECMPR	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData[1] = P1[1];
#endif

    /* Process Row-0 data 2 --> data r3-1 */
	for (j = 2; j < r3; j++)
	{
		temp = P1[j-1];
		pred1D = temp * temp / P1[j-2];
		curData = spaceFillingValue[j];
        predRelErrRatio = curData / pred1D;

        expoIndex = ((*buffer & 0x7fffffffffffffff) >> 52) - base;
        if(expoIndex <= range){
            mantiIndex = (*buffer & 0x000fffffffffffff) >> shift;
            state = tables[expoIndex][mantiIndex];
        }else{
            state = 0;
        }

        if (state)
		{
			type[j] = state;
			P1[j] = fabsf(pred1D) * precisionTable[state];
			//hit++;
		}
		else
		{
			type[j] = 0;
			compressSingleFloatValue_MSST19(vce, curData, realPrecision, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P1[j] = vce->data;
			//miss++;
		}
#ifdef HAVE_TIMECMPR	
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[j] = P1[j];
#endif		
	}

	/* Process Row-1 --> Row-r2-1 */
	size_t index;
	for (i = 1; i < r2; i++)
	{
		/* Process row-i data 0 */
		index = i*r3;	
		pred1D = P1[index-r3];
		curData = spaceFillingValue[index];
        predRelErrRatio = curData / pred1D;

        expoIndex = ((*buffer & 0x7fffffffffffffff) >> 52) - base;
        if(expoIndex <= range){
            mantiIndex = (*buffer & 0x000fffffffffffff) >> shift;
            state = tables[expoIndex][mantiIndex];
        }else{
            state = 0;
        }

		if (state)
		{
			type[index] = state;
			P1[index] = pred1D * precisionTable[state];
			//hit++;
		}
		else
		{
			type[index] = 0;
			compressSingleFloatValue_MSST19(vce, curData, realPrecision, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P1[index] = vce->data;
			//miss++;
		}
#ifdef HAVE_TIMECMPR	
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[index] = P1[index];
#endif		

		/* Process row-i data 1 --> data r3-1*/
		for (j = 1; j < r3; j++)
		{
			index = i*r3+j;
			temp = P1[index-1];
			pred2D = temp * P1[index-r3] / P1[index-r3-1];
			//float a = P1[index-1];
			//float b = P1[index-r3];
			//float c = P1[index-r3-1];

			curData = spaceFillingValue[index];
            predRelErrRatio = curData / pred2D;

            expoIndex = ((*buffer & 0x7fffffffffffffff) >> 52) - base;
            if(expoIndex <= range){
                mantiIndex = (*buffer & 0x000fffffffffffff) >> shift;
                state = tables[expoIndex][mantiIndex];
            }else{
                state = 0;
            }

			if (state)
			{
				type[index] = state;
				//float temp1 = precisionTable[state];
				//float temp = fabsf(pred2D) * precisionTable[state];
				P1[index] = fabsf(pred2D) * precisionTable[state];
				//hit++;
			}
			else
			{
				type[index] = 0;
				compressSingleFloatValue_MSST19(vce, curData, realPrecision, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P1[index] = vce->data;
				//miss++;
			}
#ifdef HAVE_TIMECMPR	
			if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
				decData[index] = P1[index];
#endif			
		}
	}


	///////////////////////////	Process layer-1 --> layer-r1-1 ///////////////////////////

	for (k = 1; k < r1; k++)
	{
		/* Process Row-0 data 0*/
		index = k*r23;
		pred1D = P1[0];
		curData = spaceFillingValue[index];
        predRelErrRatio = curData / pred1D;

        expoIndex = ((*buffer & 0x7fffffffffffffff) >> 52) - base;
        if(expoIndex <= range){
            mantiIndex = (*buffer & 0x000fffffffffffff) >> shift;
            state = tables[expoIndex][mantiIndex];
        }else{
            state = 0;
        }

		if (state)
		{
			type[index] = state;
			P0[0] = fabsf(pred1D) * precisionTable[state];
			//hit++;
		}
		else
		{
			type[index] = 0;
			compressSingleFloatValue_MSST19(vce, curData, realPrecision, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P0[0] = vce->data;
			//miss++;
		}
#ifdef HAVE_TIMECMPR	
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[index] = P0[0];
#endif

	    /* Process Row-0 data 1 --> data r3-1 */
		for (j = 1; j < r3; j++)
		{
			//index = k*r2*r3+j;
			index ++;
			temp = P0[j-1];
			pred2D = temp * P1[j] / P1[j-1];
			curData = spaceFillingValue[index];
            predRelErrRatio = curData / pred2D;

            expoIndex = ((*buffer & 0x7fffffffffffffff) >> 52) - base;
            if(expoIndex <= range){
                mantiIndex = (*buffer & 0x000fffffffffffff) >> shift;
                state = tables[expoIndex][mantiIndex];
            }else{
                state = 0;
            }

			if (state)
			{
				type[index] = state;
				P0[j] = fabsf(pred2D) * precisionTable[state];
				//hit++;
			}
			else
			{
				type[index] = 0;
				compressSingleFloatValue_MSST19(vce, curData, realPrecision, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P0[j] = vce->data;
				//miss++;
			}
#ifdef HAVE_TIMECMPR	
			if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
				decData[index] = P0[j];
#endif			
		}

	    /* Process Row-1 --> Row-r2-1 */
		size_t index2D;
		for (i = 1; i < r2; i++)
		{
			/* Process Row-i data 0 */
			index = k*r23 + i*r3;
			index2D = i*r3;
			temp = P0[index2D-r3];
			pred2D = temp * P1[index2D] / P1[index2D-r3];
			curData = spaceFillingValue[index];
            predRelErrRatio = curData / pred2D;

            expoIndex = ((*buffer & 0x7fffffffffffffff) >> 52) - base;
            if(expoIndex <= range){
                mantiIndex = (*buffer & 0x000fffffffffffff) >> shift;
                state = tables[expoIndex][mantiIndex];
            }else{
                state = 0;
            }

			if (state)
			{
				type[index] = state;
				P0[index2D] = fabsf(pred2D) * precisionTable[state];
				//hit++;
			}
			else
			{
				type[index] = 0;
				compressSingleFloatValue_MSST19(vce, curData, realPrecision, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P0[index2D] = vce->data;
				//miss++;
			}
#ifdef HAVE_TIMECMPR	
			if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
				decData[index] = P0[index2D];
#endif			

			/* Process Row-i data 1 --> data r3-1 */
			for (j = 1; j < r3; j++)
			{
				index ++;
				index2D = i*r3 + j;
				//pred3D = P0[index2D-1] * P0[index2D-r3] * P1[index2D] / P0[index2D-r3-1] / P1[index2D-r3] / P1[index2D-1] * P1[index2D-r3-1];
				temp = P0[index2D-1];
				temp2 = P0[index2D-r3-1];
                pred3D = temp * P0[index2D-r3] * P1[index2D] * P1[index2D-r3-1] / (temp2 * P1[index2D-r3] * P1[index2D-1]);

				curData = spaceFillingValue[index];
                predRelErrRatio = curData / pred3D;

                expoIndex = ((*buffer & 0x7fffffffffffffff) >> 52) - base;
                if(expoIndex <= range){
                    mantiIndex = (*buffer & 0x000fffffffffffff) >> shift;
                    state = tables[expoIndex][mantiIndex];
                }else{
                    state = 0;
                }

				if (state)
				{
					type[index] = state;
					P0[index2D] = fabsf(pred3D) * precisionTable[state];
					//hit++;
				}
				else
				{
					type[index] = 0;
					compressSingleFloatValue_MSST19(vce, curData, realPrecision, reqLength, reqBytesLength, resiBitsLength);
					updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
					memcpy(preDataBytes,vce->curBytes,4);
					addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
					P0[index2D] = vce->data;
					//miss++;
				}
#ifdef HAVE_TIMECMPR	
				if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
					decData[index] = P0[index2D];
#endif				
			}
		}

		float *Pt;
		Pt = P1;
		P1 = P0;
		P0 = Pt;
	}
	if(r23!=1)
		free(P0);
	free(P1);
	size_t exactDataNum = exactLeadNumArray->size;

	TightDataPointStorageF* tdps;

	new_TightDataPointStorageF(&tdps, dataLength, exactDataNum,
			type, exactMidByteArray->array, exactMidByteArray->size,
			exactLeadNumArray->array,
			resiBitArray->array, resiBitArray->size,
			resiBitsLength, 
			realPrecision, medianValue, (char)reqLength, quantization_intervals, NULL, 0, 0);
	tdps->plus_bits = confparams_cpr->plus_bits;

	//free memory
	free_DIA(exactLeadNumArray);
	free_DIA(resiBitArray);
	free(type);	
	free(vce);
	free(lce);
	free(exactMidByteArray); //exactMidByteArray->array has been released in free_TightDataPointStorageF(tdps);
	free(precisionTable);
	freeTopLevelTableWideInterval(&levelTable);	
	return tdps;	
}


void SZ_compress_args_float_withinRange(unsigned char** newByteData, float *oriData, size_t dataLength, size_t *outSize)
{
	TightDataPointStorageF* tdps = (TightDataPointStorageF*) malloc(sizeof(TightDataPointStorageF));
	tdps->rtypeArray = NULL;
	tdps->typeArray = NULL;	
	tdps->leadNumArray = NULL;
	tdps->residualMidBits = NULL;
	
	tdps->allSameData = 1;
	tdps->dataSeriesLength = dataLength;
	tdps->exactMidBytes = (unsigned char*)malloc(sizeof(unsigned char)*4);
	tdps->pwrErrBoundBytes = NULL;
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

/*
int SZ_compress_args_float_wRngeNoGzip(unsigned char** newByteData, float *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio, double pwrErrRatio)
{
	int status = SZ_SCES;
	size_t dataLength = computeDataLength(r5,r4,r3,r2,r1);
	float valueRangeSize = 0, medianValue = 0;
	
	float min = computeRangeSize_float(oriData, dataLength, &valueRangeSize, &medianValue);
	float max = min+valueRangeSize;
	double realPrecision = getRealPrecision_float(valueRangeSize, errBoundMode, absErr_Bound, relBoundRatio, &status);
		
	if(valueRangeSize <= realPrecision)
	{
		SZ_compress_args_float_withinRange(newByteData, oriData, dataLength, outSize);
	}
	else
	{
//		SZ_compress_args_float_NoCkRngeNoGzip_2D(newByteData, oriData, r2, r1, realPrecision, outSize);
		if(r5==0&&r4==0&&r3==0&&r2==0)
		{
			if(errBoundMode>=PW_REL)
			{	
				SZ_compress_args_float_NoCkRngeNoGzip_1D_pwr_pre_log(newByteData, oriData, pwrErrRatio, r1, outSize, min, max);
				//SZ_compress_args_float_NoCkRngeNoGzip_1D_pwrgroup(newByteData, oriData, r1, absErr_Bound, relBoundRatio, pwrErrRatio, valueRangeSize, medianValue, outSize);
			}
			else
				SZ_compress_args_float_NoCkRngeNoGzip_1D(newByteData, oriData, r1, realPrecision, outSize, valueRangeSize, medianValue);
		}
		else if(r5==0&&r4==0&&r3==0)
		{
			if(errBoundMode>=PW_REL)
				SZ_compress_args_float_NoCkRngeNoGzip_2D_pwr_pre_log(newByteData, oriData, pwrErrRatio, r2, r1, outSize, min, max);
			else
				SZ_compress_args_float_NoCkRngeNoGzip_2D(newByteData, oriData, r2, r1, realPrecision, outSize, valueRangeSize, medianValue);
		}
		else if(r5==0&&r4==0)
		{
			if(errBoundMode>=PW_REL)
				SZ_compress_args_float_NoCkRngeNoGzip_3D_pwr_pre_log(newByteData, oriData, pwrErrRatio, r3, r2, r1, outSize, min, max);
			else
				SZ_compress_args_float_NoCkRngeNoGzip_3D(newByteData, oriData, r3, r2, r1, realPrecision, outSize, valueRangeSize, medianValue);
		}
		else if(r5==0)
		{
			if(errBoundMode>=PW_REL)
				SZ_compress_args_float_NoCkRngeNoGzip_3D_pwr_pre_log(newByteData, oriData, pwrErrRatio, r4*r3, r2, r1, outSize, min, max);
			else
				SZ_compress_args_float_NoCkRngeNoGzip_3D(newByteData, oriData, r4*r3, r2, r1, realPrecision, outSize, valueRangeSize, medianValue);
		}
	}
	return status;
}
*/

int SZ_compress_args_float(int cmprType, int withRegression, unsigned char** newByteData, float *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio, double pwRelBoundRatio)
{
	confparams_cpr->errorBoundMode = errBoundMode; //this is used to print the metadata if needed...
	if(errBoundMode==PW_REL)
	{
		confparams_cpr->pw_relBoundRatio = pwRelBoundRatio;	
	}
	int status = SZ_SCES;
	size_t dataLength = computeDataLength(r5,r4,r3,r2,r1);
	
	if(dataLength <= MIN_NUM_OF_ELEMENTS)
	{
		*newByteData = SZ_skip_compress_float(oriData, dataLength, outSize);
		return status;
	}
	
	float valueRangeSize = 0, medianValue = 0;
	
	unsigned char * signs = NULL;
	bool positive = true;
	float nearZero = 0.0;
	float min = 0;
	if(pwRelBoundRatio < 0.000009999)
		confparams_cpr->accelerate_pw_rel_compression = 0;
	if(confparams_cpr->errorBoundMode == PW_REL && confparams_cpr->accelerate_pw_rel_compression)
	{
		signs = (unsigned char *) malloc(dataLength);
		memset(signs, 0, dataLength);
		min = computeRangeSize_float_MSST19(oriData, dataLength, &valueRangeSize, &medianValue, signs, &positive, &nearZero);
	}
	else
		min = computeRangeSize_float(oriData, dataLength, &valueRangeSize, &medianValue);	
	float max = min+valueRangeSize;
	confparams_cpr->fmin = min;
	confparams_cpr->fmax = max;
	
	double realPrecision = 0; 
	
	if(confparams_cpr->errorBoundMode==PSNR)
	{
		confparams_cpr->errorBoundMode = ABS;
		realPrecision = confparams_cpr->absErrBound = computeABSErrBoundFromPSNR(confparams_cpr->psnr, (double)confparams_cpr->predThreshold, (double)valueRangeSize);
		//printf("realPrecision=%lf\n", realPrecision);
	}
	else if(confparams_cpr->errorBoundMode==NORM) //norm error = sqrt(sum((xi-xi_)^2))
	{
		confparams_cpr->errorBoundMode = ABS;
		realPrecision = confparams_cpr->absErrBound = computeABSErrBoundFromNORM_ERR(confparams_cpr->normErr, dataLength);
		//printf("realPrecision=%lf\n", realPrecision);				
	}
	else
	{
		realPrecision = getRealPrecision_float(valueRangeSize, errBoundMode, absErr_Bound, relBoundRatio, &status);
		confparams_cpr->absErrBound = realPrecision;
	}	
	if(valueRangeSize <= realPrecision)
	{
		if(confparams_cpr->errorBoundMode>=PW_REL && confparams_cpr->accelerate_pw_rel_compression == 1)
			free(signs);		
		SZ_compress_args_float_withinRange(newByteData, oriData, dataLength, outSize);
	}
	else
	{
		size_t tmpOutSize = 0;
		unsigned char* tmpByteData;
		
		if (r2==0)
		{
			if(confparams_cpr->errorBoundMode>=PW_REL)
			{
				if(confparams_cpr->accelerate_pw_rel_compression && confparams_cpr->maxRangeRadius <= 32768)
					SZ_compress_args_float_NoCkRngeNoGzip_1D_pwr_pre_log_MSST19(&tmpByteData, oriData, pwRelBoundRatio, r1, &tmpOutSize, valueRangeSize, medianValue, signs, &positive, min, max, nearZero);
				else
					SZ_compress_args_float_NoCkRngeNoGzip_1D_pwr_pre_log(&tmpByteData, oriData, pwRelBoundRatio, r1, &tmpOutSize, min, max);
					//SZ_compress_args_float_NoCkRngeNoGzip_1D_pwrgroup(&tmpByteData, oriData, r1, absErr_Bound, relBoundRatio, pwRelBoundRatio, valueRangeSize, medianValue, &tmpOutSize);
			}
			else
#ifdef HAVE_TIMECMPR
				if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
					multisteps->compressionType = SZ_compress_args_float_NoCkRngeNoGzip_1D(cmprType, &tmpByteData, oriData, r1, realPrecision, &tmpOutSize, valueRangeSize, medianValue);
				else
#endif				
					{
#ifdef HAVE_RANDOMACCESS						
						if(confparams_cpr->randomAccess == 0)
						{
#endif							
							SZ_compress_args_float_NoCkRngeNoGzip_1D(cmprType, &tmpByteData, oriData, r1, realPrecision, &tmpOutSize, valueRangeSize, medianValue);
							if(tmpOutSize>=dataLength*sizeof(float) + 3 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1)
								SZ_compress_args_float_StoreOriData(oriData, dataLength, &tmpByteData, &tmpOutSize);
#ifdef HAVE_RANDOMACCESS
						}
						else
							tmpByteData = SZ_compress_float_1D_MDQ_decompression_random_access_with_blocked_regression(oriData, r1, realPrecision, &tmpOutSize);			
#endif							
					}
		}
		else
		if (r3==0)
		{			
			if(confparams_cpr->errorBoundMode>=PW_REL)
			{
				if(confparams_cpr->accelerate_pw_rel_compression && confparams_cpr->maxRangeRadius <= 32768)
					SZ_compress_args_float_NoCkRngeNoGzip_2D_pwr_pre_log_MSST19(&tmpByteData, oriData, pwRelBoundRatio, r2, r1, &tmpOutSize, valueRangeSize, signs, &positive, min, max, nearZero);
				else
					SZ_compress_args_float_NoCkRngeNoGzip_2D_pwr_pre_log(&tmpByteData, oriData, pwRelBoundRatio, r2, r1, &tmpOutSize, min, max);
			}
			else
#ifdef HAVE_TIMECMPR
				if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)				
					multisteps->compressionType = SZ_compress_args_float_NoCkRngeNoGzip_2D(cmprType, &tmpByteData, oriData, r2, r1, realPrecision, &tmpOutSize, valueRangeSize, medianValue);
				else
#endif
				{
#ifdef HAVE_RANDOMACCESS															
					if(confparams_cpr->randomAccess == 0)
					{
#endif							
						if(withRegression == SZ_NO_REGRESSION)
							SZ_compress_args_float_NoCkRngeNoGzip_2D(cmprType, &tmpByteData, oriData, r2, r1, realPrecision, &tmpOutSize, valueRangeSize, medianValue);
						else // SZ 2.1 (2D)
						{
							tmpByteData = SZ_compress_float_2D_MDQ_nonblocked_with_blocked_regression(oriData, r2, r1, realPrecision, &tmpOutSize);//SZ 2.1 (2D)
							if(tmpOutSize>=dataLength*sizeof(float) + 3 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1)
								SZ_compress_args_float_StoreOriData(oriData, dataLength, &tmpByteData, &tmpOutSize);						
						}
#ifdef HAVE_RANDOMACCESS							
					}					
					else 
						tmpByteData = SZ_compress_float_2D_MDQ_decompression_random_access_with_blocked_regression(oriData, r2, r1, realPrecision, &tmpOutSize); 
#endif	
				}
		}
		else
		if (r4==0)
		{
			if(confparams_cpr->errorBoundMode>=PW_REL)
			{
				if(confparams_cpr->accelerate_pw_rel_compression && confparams_cpr->maxRangeRadius <= 32768)
					SZ_compress_args_float_NoCkRngeNoGzip_3D_pwr_pre_log_MSST19(&tmpByteData, oriData, pwRelBoundRatio, r3, r2, r1, &tmpOutSize, valueRangeSize, signs, &positive, min, max, nearZero);
				else
					SZ_compress_args_float_NoCkRngeNoGzip_3D_pwr_pre_log(&tmpByteData, oriData, pwRelBoundRatio, r3, r2, r1, &tmpOutSize, min, max);
			}
			else
#ifdef HAVE_TIMECMPR
				if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)				
						multisteps->compressionType = SZ_compress_args_float_NoCkRngeNoGzip_3D(cmprType, &tmpByteData, oriData, r3, r2, r1, realPrecision, &tmpOutSize, valueRangeSize, medianValue);
				else
#endif
				{
#ifdef HAVE_RANDOMACCESS															
					if(confparams_cpr->randomAccess == 0)
					{
#endif						
						if(withRegression == SZ_NO_REGRESSION)
							SZ_compress_args_float_NoCkRngeNoGzip_3D(cmprType, &tmpByteData, oriData, r3, r2, r1, realPrecision, &tmpOutSize, valueRangeSize, medianValue);
						else  //SZ 2.1 (3D)
						{
							tmpByteData = SZ_compress_float_3D_MDQ_nonblocked_with_blocked_regression(oriData, r3, r2, r1, realPrecision, &tmpOutSize); //SZ 2.1 (3D)
							if(tmpOutSize>=dataLength*sizeof(float) + 3 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1)
								SZ_compress_args_float_StoreOriData(oriData, dataLength, &tmpByteData, &tmpOutSize);		
						}
#ifdef HAVE_RANDOMACCESS							
					}					
					else
						tmpByteData = SZ_compress_float_3D_MDQ_decompression_random_access_with_blocked_regression(oriData, r3, r2, r1, realPrecision, &tmpOutSize);	
#endif					
				}
		}
		else
		if (r5==0)
		{
			if(confparams_cpr->errorBoundMode>=PW_REL)
			{
				if(confparams_cpr->accelerate_pw_rel_compression && confparams_cpr->maxRangeRadius <= 32768)
					SZ_compress_args_float_NoCkRngeNoGzip_3D_pwr_pre_log_MSST19(&tmpByteData, oriData, pwRelBoundRatio, r4*r3, r2, r1, &tmpOutSize, valueRangeSize, signs, &positive, min, max, nearZero);
				else
					SZ_compress_args_float_NoCkRngeNoGzip_3D_pwr_pre_log(&tmpByteData, oriData, pwRelBoundRatio, r4*r3, r2, r1, &tmpOutSize, min, max);				
			}
			else
#ifdef HAVE_TIMECMPR
				if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)				
					multisteps->compressionType = SZ_compress_args_float_NoCkRngeNoGzip_4D(&tmpByteData, oriData, r4, r3, r2, r1, realPrecision, &tmpOutSize, valueRangeSize, medianValue);
				else
#endif
				{
					if(withRegression == SZ_NO_REGRESSION)
						SZ_compress_args_float_NoCkRngeNoGzip_4D(&tmpByteData, oriData, r4, r3, r2, r1, realPrecision, &tmpOutSize, valueRangeSize, medianValue);
					else 
					{
						tmpByteData = SZ_compress_float_3D_MDQ_nonblocked_with_blocked_regression(oriData, r4*r3, r2, r1, realPrecision, &tmpOutSize); //SZ 2.1 4D
						if(tmpOutSize>=dataLength*sizeof(float) + 3 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1)
							SZ_compress_args_float_StoreOriData(oriData, dataLength, &tmpByteData, &tmpOutSize);						
					}
				}
		}
		else
		{
			printf("Error: doesn't support 5 dimensions for now.\n");
			status = SZ_DERR; //dimension error
		}
		//Call Zstd or Gzip to do the further compression.
		if(confparams_cpr->szMode==SZ_BEST_SPEED)
		{
			*outSize = tmpOutSize;
			*newByteData = tmpByteData;
		}
		else if(confparams_cpr->szMode==SZ_BEST_COMPRESSION || confparams_cpr->szMode==SZ_DEFAULT_COMPRESSION || confparams_cpr->szMode==SZ_TEMPORAL_COMPRESSION)
		{
			*outSize = sz_lossless_compress(confparams_cpr->losslessCompressor, confparams_cpr->gzipMode, tmpByteData, tmpOutSize, newByteData);
			free(tmpByteData);
		}
		else
		{
			printf("Error: Wrong setting of confparams_cpr->szMode in the float compression.\n");
			status = SZ_MERR; //mode error			
		}
	}
	
	return status;
}

//TODO
int SZ_compress_args_float_subblock(unsigned char* compressedBytes, float *oriData,
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1,
size_t s5, size_t s4, size_t s3, size_t s2, size_t s1,
size_t e5, size_t e4, size_t e3, size_t e2, size_t e1,
size_t *outSize, int errBoundMode, double absErr_Bound, double relBoundRatio)
{
	int status = SZ_SCES;
	float valueRangeSize = 0, medianValue = 0;
	computeRangeSize_float_subblock(oriData, &valueRangeSize, &medianValue, r5, r4, r3, r2, r1, s5, s4, s3, s2, s1, e5, e4, e3, e2, e1);

	double realPrecision = getRealPrecision_float(valueRangeSize, errBoundMode, absErr_Bound, relBoundRatio, &status);

	if(valueRangeSize <= realPrecision)
	{
		//TODO
		//SZ_compress_args_float_withinRange_subblock();
	}
	else
	{
		if (r2==0)
		{
			if(errBoundMode>=PW_REL)
			{
				//TODO
				//SZ_compress_args_float_NoCkRngeNoGzip_1D_pwr_subblock();
				printf ("Current subblock version does not support point-wise relative error bound.\n");
			}
			else
				SZ_compress_args_float_NoCkRnge_1D_subblock(compressedBytes, oriData, realPrecision, outSize, valueRangeSize, medianValue, r1, s1, e1);
		}
		else
		if (r3==0)
		{
			//TODO
			if(errBoundMode>=PW_REL)
			{
				//TODO
				//SZ_compress_args_float_NoCkRngeNoGzip_2D_pwr_subblock();
				printf ("Current subblock version does not support point-wise relative error bound.\n");
			}
			else
				SZ_compress_args_float_NoCkRnge_2D_subblock(compressedBytes, oriData, realPrecision, outSize, valueRangeSize, medianValue, r2, r1, s2, s1, e2, e1);
		}
		else
		if (r4==0)
		{
			if(errBoundMode>=PW_REL)
			{
				//TODO
				//SZ_compress_args_float_NoCkRngeNoGzip_3D_pwr_subblock();
				printf ("Current subblock version does not support point-wise relative error bound.\n");
			}
			else
				SZ_compress_args_float_NoCkRnge_3D_subblock(compressedBytes, oriData, realPrecision, outSize, valueRangeSize, medianValue, r3, r2, r1, s3, s2, s1, e3, e2, e1);
		}
		else
		if (r5==0)
		{
			if(errBoundMode>=PW_REL)
			{
				//TODO
				//SZ_compress_args_float_NoCkRngeNoGzip_4D_pwr_subblock();
				printf ("Current subblock version does not support point-wise relative error bound.\n");
			}
			else
				SZ_compress_args_float_NoCkRnge_4D_subblock(compressedBytes, oriData, realPrecision, outSize, valueRangeSize, medianValue, r4, r3, r2, r1, s4, s3, s2, s1, e4, e3, e2, e1);
		}
		else
		{
			printf("Error: doesn't support 5 dimensions for now.\n");
			status = SZ_DERR; //dimension error
		}
	}
	return status;
}

void SZ_compress_args_float_NoCkRnge_1D_subblock(unsigned char* compressedBytes, float *oriData, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f,
size_t r1, size_t s1, size_t e1)
{
	TightDataPointStorageF* tdps = SZ_compress_float_1D_MDQ_subblock(oriData, realPrecision, valueRangeSize, medianValue_f, r1, s1, e1);

	if (confparams_cpr->szMode==SZ_BEST_SPEED)
		convertTDPStoFlatBytes_float_args(tdps, compressedBytes, outSize);
	else if(confparams_cpr->szMode==SZ_BEST_COMPRESSION || confparams_cpr->szMode==SZ_DEFAULT_COMPRESSION)
	{
		unsigned char *tmpCompBytes;
		size_t tmpOutSize;
		convertTDPStoFlatBytes_float(tdps, &tmpCompBytes, &tmpOutSize);
		*outSize = zlib_compress3(tmpCompBytes, tmpOutSize, compressedBytes, confparams_cpr->gzipMode);
		free(tmpCompBytes);
	}
	else
	{
		printf ("Error: Wrong setting of confparams_cpr->szMode in the double compression.\n");
	}

	//TODO
//	if(*outSize>dataLength*sizeof(float))
//		SZ_compress_args_float_StoreOriData(oriData, dataLength, newByteData, outSize);

	free_TightDataPointStorageF(tdps);
}

void SZ_compress_args_float_NoCkRnge_2D_subblock(unsigned char* compressedBytes, float *oriData, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f,
size_t r2, size_t r1, size_t s2, size_t s1, size_t e2, size_t e1)
{
	TightDataPointStorageF* tdps = SZ_compress_float_2D_MDQ_subblock(oriData, realPrecision, valueRangeSize, medianValue_f, r2, r1, s2, s1, e2, e1);

	if (confparams_cpr->szMode==SZ_BEST_SPEED)
		convertTDPStoFlatBytes_float_args(tdps, compressedBytes, outSize);
	else if(confparams_cpr->szMode==SZ_BEST_COMPRESSION || confparams_cpr->szMode==SZ_DEFAULT_COMPRESSION)
	{
		unsigned char *tmpCompBytes;
		size_t tmpOutSize;
		convertTDPStoFlatBytes_float(tdps, &tmpCompBytes, &tmpOutSize);
		*outSize = zlib_compress3(tmpCompBytes, tmpOutSize, compressedBytes, confparams_cpr->gzipMode);
		free(tmpCompBytes);
	}
	else
	{
		printf ("Error: Wrong setting of confparams_cpr->szMode in the double compression.\n");
	}

	//TODO
//	if(*outSize>dataLength*sizeof(float))
//		SZ_compress_args_float_StoreOriData(oriData, dataLength, newByteData, outSize);

	free_TightDataPointStorageF(tdps);
}

void SZ_compress_args_float_NoCkRnge_3D_subblock(unsigned char* compressedBytes, float *oriData, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f,
size_t r3, size_t r2, size_t r1, size_t s3, size_t s2, size_t s1, size_t e3, size_t e2, size_t e1)
{
	TightDataPointStorageF* tdps = SZ_compress_float_3D_MDQ_subblock(oriData, realPrecision, valueRangeSize, medianValue_f, r3, r2, r1, s3, s2, s1, e3, e2, e1);

	if (confparams_cpr->szMode==SZ_BEST_SPEED)
		convertTDPStoFlatBytes_float_args(tdps, compressedBytes, outSize);
	else if(confparams_cpr->szMode==SZ_BEST_COMPRESSION || confparams_cpr->szMode==SZ_DEFAULT_COMPRESSION)
	{
		unsigned char *tmpCompBytes;
		size_t tmpOutSize;
		convertTDPStoFlatBytes_float(tdps, &tmpCompBytes, &tmpOutSize);
		*outSize = zlib_compress3(tmpCompBytes, tmpOutSize, compressedBytes, confparams_cpr->gzipMode);
		free(tmpCompBytes);
	}
	else
	{
		printf ("Error: Wrong setting of confparams_cpr->szMode in the double compression.\n");
	}

	//TODO
//	if(*outSize>dataLength*sizeof(float))
//		SZ_compress_args_float_StoreOriData(oriData, dataLength, newByteData, outSize);

	free_TightDataPointStorageF(tdps);
}

void SZ_compress_args_float_NoCkRnge_4D_subblock(unsigned char* compressedBytes, float *oriData, double realPrecision, size_t *outSize, float valueRangeSize, float medianValue_f,
size_t r4, size_t r3, size_t r2, size_t r1, size_t s4, size_t s3, size_t s2, size_t s1, size_t e4, size_t e3, size_t e2, size_t e1)
{
	TightDataPointStorageF* tdps = SZ_compress_float_4D_MDQ_subblock(oriData, realPrecision, valueRangeSize, medianValue_f, r4, r3, r2, r1, s4, s3, s2, s1, e4, e3, e2, e1);

	if (confparams_cpr->szMode==SZ_BEST_SPEED)
		convertTDPStoFlatBytes_float_args(tdps, compressedBytes, outSize);
	else if(confparams_cpr->szMode==SZ_BEST_COMPRESSION || confparams_cpr->szMode==SZ_DEFAULT_COMPRESSION)
	{
		unsigned char *tmpCompBytes;
		size_t tmpOutSize;
		convertTDPStoFlatBytes_float(tdps, &tmpCompBytes, &tmpOutSize);
		*outSize = zlib_compress3(tmpCompBytes, tmpOutSize, compressedBytes, confparams_cpr->gzipMode);
		free(tmpCompBytes);
	}
	else
	{
		printf ("Error: Wrong setting of confparams_cpr->szMode in the double compression.\n");
	}

	//TODO
//	if(*outSize>dataLength*sizeof(float))
//		SZ_compress_args_float_StoreOriData(oriData, dataLength, newByteData, outSize);

	free_TightDataPointStorageF(tdps);

}

unsigned int optimize_intervals_float_1D_subblock(float *oriData, double realPrecision, size_t r1, size_t s1, size_t e1)
{
	size_t dataLength = e1 - s1 + 1;
	oriData = oriData + s1;

	size_t i = 0;
	unsigned long radiusIndex;
	float pred_value = 0, pred_err;
	int *intervals = (int*)malloc(confparams_cpr->maxRangeRadius*sizeof(int));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(int));
	size_t totalSampleSize = dataLength/confparams_cpr->sampleDistance;
	for(i=2;i<dataLength;i++)
	{
		if(i%confparams_cpr->sampleDistance==0)
		{
			pred_value = 2*oriData[i-1] - oriData[i-2];
			//pred_value = oriData[i-1];
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

unsigned int optimize_intervals_float_2D_subblock(float *oriData, double realPrecision, size_t r1, size_t r2, size_t s1, size_t s2, size_t e1, size_t e2)
{
	size_t R1 = e1 - s1 + 1;
	size_t R2 = e2 - s2 + 1;

	size_t i,j, index;
	unsigned long radiusIndex;
	float pred_value = 0, pred_err;
	int *intervals = (int*)malloc(confparams_cpr->maxRangeRadius*sizeof(int));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(int));
	size_t totalSampleSize = R1*R2/confparams_cpr->sampleDistance;
	for(i=s1+1;i<=e1;i++)
	{
		for(j=s2+1;j<=e2;j++)
		{
			if((i+j)%confparams_cpr->sampleDistance==0)
			{
				index = i*r2+j;
				pred_value = oriData[index-1] + oriData[index-r2] - oriData[index-r2-1];
				pred_err = fabs(pred_value - oriData[index]);
				radiusIndex = (unsigned long)((pred_err/realPrecision+1)/2);
				if(radiusIndex>=confparams_cpr->maxRangeRadius)
					radiusIndex = confparams_cpr->maxRangeRadius - 1;
				intervals[radiusIndex]++;
			}
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
	//printf("confparams_cpr->maxRangeRadius = %d, accIntervals=%d, powerOf2=%d\n", confparams_cpr->maxRangeRadius, accIntervals, powerOf2);
	return powerOf2;
}

unsigned int optimize_intervals_float_3D_subblock(float *oriData, double realPrecision, size_t r1, size_t r2, size_t r3, size_t s1, size_t s2, size_t s3, size_t e1, size_t e2, size_t e3)
{
	size_t R1 = e1 - s1 + 1;
	size_t R2 = e2 - s2 + 1;
	size_t R3 = e3 - s3 + 1;

	size_t r23 = r2*r3;

	size_t i,j,k, index;
	unsigned long radiusIndex;
	float pred_value = 0, pred_err;
	int *intervals = (int*)malloc(confparams_cpr->maxRangeRadius*sizeof(int));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(int));
	size_t totalSampleSize = R1*R2*R3/confparams_cpr->sampleDistance;
	for(i=s1+1;i<=e1;i++)
	{
		for(j=s2+1;j<=e2;j++)
		{
			for(k=s3+1;k<=e3;k++)
			{
				if((i+j+k)%confparams_cpr->sampleDistance==0)
				{
					index = i*r23+j*r3+k;
					pred_value = oriData[index-1] + oriData[index-r3] + oriData[index-r23]
					- oriData[index-1-r23] - oriData[index-r3-1] - oriData[index-r3-r23] + oriData[index-r3-r23-1];
					pred_err = fabs(pred_value - oriData[index]);
					radiusIndex = (unsigned long)((pred_err/realPrecision+1)/2);
					if(radiusIndex>=confparams_cpr->maxRangeRadius)
						radiusIndex = confparams_cpr->maxRangeRadius - 1;
					intervals[radiusIndex]++;
				}
			}
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

unsigned int optimize_intervals_float_4D_subblock(float *oriData, double realPrecision,
size_t r1, size_t r2, size_t r3, size_t r4, size_t s1, size_t s2, size_t s3, size_t s4, size_t e1, size_t e2, size_t e3, size_t e4)
{
	size_t R1 = e1 - s1 + 1;
	size_t R2 = e2 - s2 + 1;
	size_t R3 = e3 - s3 + 1;
	size_t R4 = e4 - s4 + 1;

	size_t r34 = r3*r4;
	size_t r234 = r2*r3*r4;

	size_t i,j,k,l, index;
	unsigned long radiusIndex;
	float pred_value = 0, pred_err;
	int *intervals = (int*)malloc(confparams_cpr->maxRangeRadius*sizeof(int));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(int));
	size_t totalSampleSize = R1*R2*R3*R4/confparams_cpr->sampleDistance;
	for(i=s1+1;i<=e1;i++)
	{
		for(j=s2+1;j<=e2;j++)
		{
			for(k=s3+1;k<=e3;k++)
			{
				for (l=s4+1;l<=e4;l++)
				{
					if((i+j+k+l)%confparams_cpr->sampleDistance==0)
					{
						index = i*r234+j*r34+k*r4+l;
						pred_value = oriData[index-1] + oriData[index-r4] + oriData[index-r34]
									- oriData[index-1-r34] - oriData[index-r4-1] - oriData[index-r4-r34] + oriData[index-r4-r34-1];
						pred_err = fabs(pred_value - oriData[index]);
						radiusIndex = (unsigned long)((pred_err/realPrecision+1)/2);
						if(radiusIndex>=confparams_cpr->maxRangeRadius)
							radiusIndex = confparams_cpr->maxRangeRadius - 1;
						intervals[radiusIndex]++;
					}
				}
			}
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

TightDataPointStorageF* SZ_compress_float_1D_MDQ_subblock(float *oriData, double realPrecision, float valueRangeSize, float medianValue_f,
size_t r1, size_t s1, size_t e1)
{
	size_t dataLength = e1 - s1 + 1;
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
		quantization_intervals = optimize_intervals_float_1D_subblock(oriData, realPrecision, r1, s1, e1);
	else
		quantization_intervals = exe_params->intvCapacity;
	//updateQuantizationInfo(quantization_intervals);
	int intvRadius = quantization_intervals/2;

	size_t i; 
	int reqLength;
	float medianValue = medianValue_f;
	short radExpo = getExponent_float(valueRangeSize/2);

	computeReqLength_float(realPrecision, radExpo, &reqLength, &medianValue);

	int* type = (int*) malloc(dataLength*sizeof(int));

	float* spaceFillingValue = oriData + s1;

	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);

	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);

	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);

	type[0] = 0;

	unsigned char preDataBytes[4];
	intToBytes_bigEndian(preDataBytes, 0);

	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;
	float last3CmprsData[3] = {0};

	FloatValueCompressElement *vce = (FloatValueCompressElement*)malloc(sizeof(FloatValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));

	//add the first data
	compressSingleFloatValue(vce, spaceFillingValue[0], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	listAdd_float(last3CmprsData, vce->data);

	//add the second data
	type[1] = 0;
	compressSingleFloatValue(vce, spaceFillingValue[1], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	listAdd_float(last3CmprsData, vce->data);

	int state;
	double checkRadius;
	float curData;
	float pred;
	float predAbsErr;
	checkRadius = (quantization_intervals-1)*realPrecision;
	double interval = 2*realPrecision;

	for(i=2;i<dataLength;i++)
	{
		curData = spaceFillingValue[i];
		pred = 2*last3CmprsData[0] - last3CmprsData[1];
		predAbsErr = fabs(curData - pred);
		if(predAbsErr<=checkRadius)
		{
			state = (predAbsErr/realPrecision+1)/2;
			if(curData>=pred)
			{
				type[i] = intvRadius+state;
				pred = pred + state*interval;
			}
			else
			{
				type[i] = intvRadius-state;
				pred = pred - state*interval;
			}

			listAdd_float(last3CmprsData, pred);
			continue;
		}

		//unpredictable data processing
		type[i] = 0;
		compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,4);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);

		listAdd_float(last3CmprsData, vce->data);
	}

	size_t exactDataNum = exactLeadNumArray->size;

	TightDataPointStorageF* tdps;

	new_TightDataPointStorageF(&tdps, dataLength, exactDataNum,
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

	return tdps;
}

TightDataPointStorageF* SZ_compress_float_2D_MDQ_subblock(float *oriData, double realPrecision, float valueRangeSize, float medianValue_f,
size_t r1, size_t r2, size_t s1, size_t s2, size_t e1, size_t e2)
{
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_2D_subblock(oriData, realPrecision, r1, r2, s1, s2, e1, e2);
		updateQuantizationInfo(quantization_intervals);
	}
	else
		quantization_intervals = exe_params->intvCapacity;
	int intvRadius = quantization_intervals/2;

	size_t i,j; 
	int reqLength;
	float pred1D, pred2D;
	float diff = 0.0;
	double itvNum = 0;
	float *P0, *P1;

	size_t R1 = e1 - s1 + 1;
	size_t R2 = e2 - s2 + 1;
	size_t dataLength = R1*R2;

	P0 = (float*)malloc(R2*sizeof(float));
	memset(P0, 0, R2*sizeof(float));
	P1 = (float*)malloc(R2*sizeof(float));
	memset(P1, 0, R2*sizeof(float));

	float medianValue = medianValue_f;
	short radExpo = getExponent_float(valueRangeSize/2);
	computeReqLength_float(realPrecision, radExpo, &reqLength, &medianValue);

	int* type = (int*) malloc(dataLength*sizeof(int));

	float* spaceFillingValue = oriData; //

	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);

	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);

	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);

	unsigned char preDataBytes[4];
	intToBytes_bigEndian(preDataBytes, 0);

	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;

	FloatValueCompressElement *vce = (FloatValueCompressElement*)malloc(sizeof(FloatValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));

	/* Process Row-s1 data s2*/
	size_t gIndex;
	size_t lIndex;

	gIndex = s1*r2+s2;
	lIndex = 0;

	type[lIndex] = 0;
	compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	P1[0] = vce->data;

	/* Process Row-s1 data s2+1*/
	gIndex = s1*r2+(s2+1);
	lIndex = 1;

	pred1D = P1[0];
	diff = spaceFillingValue[gIndex] - pred1D;

	itvNum =  fabs(diff)/realPrecision + 1;

	if (itvNum < quantization_intervals)
	{
		if (diff < 0) itvNum = -itvNum;
		type[lIndex] = (int) (itvNum/2) + intvRadius;
		P1[1] = pred1D + 2 * (type[lIndex] - intvRadius) * realPrecision;
	}
	else
	{
		type[lIndex] = 0;
		compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,4);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
		P1[1] = vce->data;
	}

    /* Process Row-s1 data s2+2 --> data e2 */
	for (j = 2; j < R2; j++)
	{
		gIndex = s1*r2+(s2+j);
		lIndex = j;

		pred1D = 2*P1[j-1] - P1[j-2];
		diff = spaceFillingValue[gIndex] - pred1D;

		itvNum = fabs(diff)/realPrecision + 1;

		if (itvNum < quantization_intervals)
		{
			if (diff < 0) itvNum = -itvNum;
			type[lIndex] = (int) (itvNum/2) + intvRadius;
			P1[j] = pred1D + 2 * (type[lIndex] - intvRadius) * realPrecision;
		}
		else
		{
			type[lIndex] = 0;
			compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P1[j] = vce->data;
		}
	}

	/* Process Row-s1+1 --> Row-e1 */
	for (i = 1; i < R1; i++)
	{
		/* Process row-s1+i data s2 */
		gIndex = (s1+i)*r2+s2;
		lIndex = i*R2;

		pred1D = P1[0];
		diff = spaceFillingValue[gIndex] - pred1D;

		itvNum = fabs(diff)/realPrecision + 1;

		if (itvNum < quantization_intervals)
		{
			if (diff < 0) itvNum = -itvNum;
			type[lIndex] = (int) (itvNum/2) + intvRadius;
			P0[0] = pred1D + 2 * (type[lIndex] - intvRadius) * realPrecision;
		}
		else
		{
			type[lIndex] = 0;
			compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P0[0] = vce->data;
		}

		/* Process row-s1+i data s2+1 --> e2 */
		for (j = 1; j < R2; j++)
		{
			gIndex = (s1+i)*r2+(s2+j);
			lIndex = i*R2+j;

//			printf ("global index = %d, local index = %d\n", gIndex, lIndex);

			pred2D = P0[j-1] + P1[j] - P1[j-1];

			diff = spaceFillingValue[gIndex] - pred2D;

			itvNum = fabs(diff)/realPrecision + 1;

			if (itvNum < quantization_intervals)
			{
				if (diff < 0) itvNum = -itvNum;
				type[lIndex] = (int) (itvNum/2) + intvRadius;
				P0[j] = pred2D + 2 * (type[lIndex] - intvRadius) * realPrecision;
			}
			else
			{
				type[lIndex] = 0;
				compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P0[j] = vce->data;
			}
		}

		float *Pt;
		Pt = P1;
		P1 = P0;
		P0 = Pt;
	}

	free(P0);
	free(P1);
	size_t exactDataNum = exactLeadNumArray->size;

	TightDataPointStorageF* tdps;

	new_TightDataPointStorageF(&tdps, dataLength, exactDataNum,
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

	return tdps;
}

TightDataPointStorageF* SZ_compress_float_3D_MDQ_subblock(float *oriData, double realPrecision, float valueRangeSize, float medianValue_f,
size_t r1, size_t r2, size_t r3, size_t s1, size_t s2, size_t s3, size_t e1, size_t e2, size_t e3)
{
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_3D_subblock(oriData, realPrecision, r1, r2, r3, s1, s2, s3, e1, e2, e3);
		updateQuantizationInfo(quantization_intervals);
	}
	else
		quantization_intervals = exe_params->intvCapacity;
	int intvRadius = quantization_intervals/2;

	size_t i,j,k; 
	int reqLength;
	float pred1D, pred2D, pred3D;
	float diff = 0.0;
	double itvNum = 0;
	float *P0, *P1;

	size_t R1 = e1 - s1 + 1;
	size_t R2 = e2 - s2 + 1;
	size_t R3 = e3 - s3 + 1;
	size_t dataLength = R1*R2*R3;

	size_t r23 = r2*r3;
	size_t R23 = R2*R3;

	P0 = (float*)malloc(R23*sizeof(float));
	P1 = (float*)malloc(R23*sizeof(float));

	float medianValue = medianValue_f;
	short radExpo = getExponent_float(valueRangeSize/2);
	computeReqLength_float(realPrecision, radExpo, &reqLength, &medianValue);

	int* type = (int*) malloc(dataLength*sizeof(int));
	//type[dataLength]=0;

	float* spaceFillingValue = oriData; //

	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);

	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);

	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);

	unsigned char preDataBytes[4];
	intToBytes_bigEndian(preDataBytes, 0);

	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;

	FloatValueCompressElement *vce = (FloatValueCompressElement*)malloc(sizeof(FloatValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));


	///////////////////////////	Process layer-s1 ///////////////////////////
	/* Process Row-s2 data s3*/
	size_t gIndex; 	//global index
	size_t lIndex; 	//local index
	size_t index2D; 	//local 2D index

	gIndex = s1*r23+s2*r3+s3;
	lIndex = 0;
	index2D = 0;

	type[lIndex] = 0;
	compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	P1[index2D] = vce->data;

	/* Process Row-s2 data s3+1*/
	gIndex = s1*r23+s2*r3+s3+1;
	lIndex = 1;
	index2D = 1;

	pred1D = P1[index2D-1];
	diff = spaceFillingValue[gIndex] - pred1D;

	itvNum = fabs(diff)/realPrecision + 1;

	if (itvNum < quantization_intervals)
	{
		if (diff < 0) itvNum = -itvNum;
		type[lIndex] = (int) (itvNum/2) + intvRadius;
		P1[index2D] = pred1D + 2 * (type[lIndex] - intvRadius) * realPrecision;
	}
	else
	{
		type[lIndex] = 0;
		compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,4);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
		P1[index2D] = vce->data;
	}

    /* Process Row-s2 data s3+2 --> data e3 */
	for (j = 2; j < R3; j++)
	{
		gIndex = s1*r23+s2*r3+s3+j;
		lIndex = j;
		index2D = j;

		pred1D = 2*P1[index2D-1] - P1[index2D-2];
		diff = spaceFillingValue[gIndex] - pred1D;

		itvNum = fabs(diff)/realPrecision + 1;

		if (itvNum < quantization_intervals)
		{
			if (diff < 0) itvNum = -itvNum;
			type[lIndex] = (int) (itvNum/2) + intvRadius;
			P1[index2D] = pred1D + 2 * (type[lIndex] - intvRadius) * realPrecision;
		}
		else
		{
			type[lIndex] = 0;
			compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P1[index2D] = vce->data;
		}
	}

	/* Process Row-s2+1 --> Row-e2 */
	for (i = 1; i < R2; i++)
	{
		/* Process row-s2+i data s3 */
		gIndex = s1*r23+(s2+i)*r3+s3;
		lIndex = i*R3;
		index2D = i*R3;

		pred1D  = P1[index2D-R3];
		diff = spaceFillingValue[gIndex] - pred1D;

		itvNum = fabs(diff)/realPrecision + 1;

		if (itvNum < quantization_intervals)
		{
			if (diff < 0) itvNum = -itvNum;
			type[lIndex] = (int) (itvNum/2) + intvRadius;
			P1[index2D] = pred1D + 2 * (type[lIndex] - intvRadius) * realPrecision;
		}
		else
		{
			type[lIndex] = 0;
			compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P1[index2D] = vce->data;
		}

		/* Process row-s2+i data s3+1 --> data e3*/
		for (j = 1; j < R3; j++)
		{
			gIndex = s1*r23+(s2+i)*r3+s3+j;
			lIndex = i*R3+j;
			index2D = i*R3+j;

			pred2D  = P1[index2D-1] + P1[index2D-R3] - P1[index2D-R3-1];
			diff = spaceFillingValue[gIndex] - pred2D;

			itvNum = fabs(diff)/realPrecision + 1;

			if (itvNum < quantization_intervals)
			{
				if (diff < 0) itvNum = -itvNum;
				type[lIndex] = (int) (itvNum/2) + intvRadius;
				P1[index2D] = pred2D + 2 * (type[lIndex] - intvRadius) * realPrecision;
			}
			else
			{
				type[lIndex] = 0;
				compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P1[index2D] = vce->data;
			}
		}
	}


	///////////////////////////	Process layer-s1+1 --> layer-e1 ///////////////////////////

	for (k = 1; k < R1; k++)
	{
		/* Process Row-s2 data s3*/
		gIndex = (s1+k)*r23+s2*r3+s3;
		lIndex = k*R23;
		index2D = 0;

		pred1D = P1[index2D];
		diff = spaceFillingValue[gIndex] - pred1D;

		itvNum = fabs(diff)/realPrecision + 1;

		if (itvNum < quantization_intervals)
		{
			if (diff < 0) itvNum = -itvNum;
			type[lIndex] = (int) (itvNum/2) + intvRadius;
			P0[index2D] = pred1D + 2 * (type[lIndex] - intvRadius) * realPrecision;
		}
		else
		{
			type[lIndex] = 0;
			compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P0[index2D] = vce->data;
		}

	    /* Process Row-s2 data s3+1 --> data e3 */
		for (j = 1; j < R3; j++)
		{
			gIndex = (s1+k)*r23+s2*r3+s3+j;
			lIndex = k*R23+j;
			index2D = j;

			pred2D = P0[index2D-1] + P1[index2D] - P1[index2D-1];
			diff = spaceFillingValue[gIndex] - pred2D;

			itvNum = fabs(diff)/realPrecision + 1;

			if (itvNum < quantization_intervals)
			{
				if (diff < 0) itvNum = -itvNum;
				type[lIndex] = (int) (itvNum/2) + intvRadius;
				P0[index2D] = pred2D + 2 * (type[lIndex] - intvRadius) * realPrecision;
			}
			else
			{
				type[lIndex] = 0;
				compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P0[index2D] = vce->data;
			}
		}

	    /* Process Row-s2+1 --> Row-e2 */
		for (i = 1; i < R2; i++)
		{
			/* Process Row-s2+i data s3 */
			gIndex = (s1+k)*r23+(s2+i)*r3+s3;
			lIndex = k*R23+i*R3;
			index2D = i*R3;

			pred2D = P0[index2D-R3] + P1[index2D] - P1[index2D-R3];
			diff = spaceFillingValue[gIndex] - pred2D;

			itvNum = fabs(diff)/realPrecision + 1;

			if (itvNum < quantization_intervals)
			{
				if (diff < 0) itvNum = -itvNum;
				type[lIndex] = (int) (itvNum/2) + intvRadius;
				P0[index2D] = pred2D + 2 * (type[lIndex] - intvRadius) * realPrecision;
			}
			else
			{
				type[lIndex] = 0;
				compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P0[index2D] = vce->data;
			}

			/* Process Row-s2+i data s3+1 --> data e3 */
			for (j = 1; j < R3; j++)
			{
				gIndex = (s1+k)*r23+(s2+i)*r3+s3+j;
				lIndex = k*R23+i*R3+j;
				index2D = i*R3+j;

//				printf ("global index = %d, local index = %d\n", gIndex, lIndex);

				pred3D = P0[index2D-1] + P0[index2D-R3]+ P1[index2D] - P0[index2D-R3-1] - P1[index2D-R3] - P1[index2D-1] + P1[index2D-R3-1];
				diff = spaceFillingValue[gIndex] - pred3D;

				itvNum = fabs(diff)/realPrecision + 1;

				if (itvNum < quantization_intervals)
				{
					if (diff < 0) itvNum = -itvNum;
					type[lIndex] = (int) (itvNum/2) + intvRadius;
					P0[index2D] = pred3D + 2 * (type[lIndex] - intvRadius) * realPrecision;
				}
				else
				{
					type[lIndex] = 0;
					compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
					updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
					memcpy(preDataBytes,vce->curBytes,4);
					addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
					P0[index2D] = vce->data;
				}
			}
		}

		float *Pt;
		Pt = P1;
		P1 = P0;
		P0 = Pt;
	}

	free(P0);
	free(P1);
	size_t exactDataNum = exactLeadNumArray->size;

	TightDataPointStorageF* tdps;

	new_TightDataPointStorageF(&tdps, dataLength, exactDataNum,
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

	return tdps;
}

TightDataPointStorageF* SZ_compress_float_4D_MDQ_subblock(float *oriData, double realPrecision, float valueRangeSize, float medianValue_f,
size_t r1, size_t r2, size_t r3, size_t r4, size_t s1, size_t s2, size_t s3, size_t s4, size_t e1, size_t e2, size_t e3, size_t e4)
{
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_4D_subblock(oriData, realPrecision, r1, r2, r3, r4, s1, s2, s3, s4, e1, e2, e3, e4);
		updateQuantizationInfo(quantization_intervals);
	}
	else
		quantization_intervals = exe_params->intvCapacity;
	int intvRadius = quantization_intervals/2;

	size_t i,j,k; 
	int reqLength;
	float pred1D, pred2D, pred3D;
	float diff = 0.0;
	double itvNum = 0;
	float *P0, *P1;

	size_t R1 = e1 - s1 + 1;
	size_t R2 = e2 - s2 + 1;
	size_t R3 = e3 - s3 + 1;
	size_t R4 = e4 - s4 + 1;

	size_t dataLength = R1*R2*R3*R4;

	size_t r34 = r3*r4;
	size_t r234 = r2*r3*r4;
	size_t R34 = R3*R4;
	size_t R234 = R2*R3*R4;

	P0 = (float*)malloc(R34*sizeof(float));
	P1 = (float*)malloc(R34*sizeof(float));

	float medianValue = medianValue_f;
	short radExpo = getExponent_float(valueRangeSize/2);
	computeReqLength_float(realPrecision, radExpo, &reqLength, &medianValue);

	int* type = (int*) malloc(dataLength*sizeof(int));

	float* spaceFillingValue = oriData; //

	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);

	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);

	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);

	unsigned char preDataBytes[4];
	intToBytes_bigEndian(preDataBytes, 0);

	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;

	FloatValueCompressElement *vce = (FloatValueCompressElement*)malloc(sizeof(FloatValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));


	size_t l;
	for (l = 0; l < R1; l++)
	{

		///////////////////////////	Process layer-s2 ///////////////////////////
		/* Process Row-s3 data s4*/
		size_t gIndex; 	//global index
		size_t lIndex; 	//local index
		size_t index2D; 	//local 2D index

		gIndex = (s1+l)*r234+s2*r34+s3*r4+s4;
		lIndex = l*R234;
		index2D = 0;

		type[lIndex] = 0;
		compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,4);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
		P1[index2D] = vce->data;

		/* Process Row-s3 data s4+1*/
		gIndex = (s1+l)*r234+s2*r34+s3*r4+s4+1;
		lIndex = l*R234+1;
		index2D = 1;

		pred1D = P1[index2D-1];
		diff = spaceFillingValue[gIndex] - pred1D;

		itvNum = fabs(diff)/realPrecision + 1;

		if (itvNum < quantization_intervals)
		{
			if (diff < 0) itvNum = -itvNum;
			type[lIndex] = (int) (itvNum/2) + intvRadius;
			P1[index2D] = pred1D + 2 * (type[lIndex] - intvRadius) * realPrecision;
		}
		else
		{
			type[lIndex] = 0;
			compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			P1[index2D] = vce->data;
		}

		/* Process Row-s3 data s4+2 --> data e4 */
		for (j = 2; j < R4; j++)
		{
			gIndex = (s1+l)*r234+s2*r34+s3*r4+s4+j;
			lIndex = l*R234+j;
			index2D = j;

			pred1D = 2*P1[index2D-1] - P1[index2D-2];
			diff = spaceFillingValue[gIndex] - pred1D;

			itvNum = fabs(diff)/realPrecision + 1;

			if (itvNum < quantization_intervals)
			{
				if (diff < 0) itvNum = -itvNum;
				type[lIndex] = (int) (itvNum/2) + intvRadius;
				P1[index2D] = pred1D + 2 * (type[lIndex] - intvRadius) * realPrecision;
			}
			else
			{
				type[lIndex] = 0;
				compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P1[index2D] = vce->data;
			}
		}

		/* Process Row-s3+1 --> Row-e3 */
		for (i = 1; i < R3; i++)
		{
			/* Process row-s2+i data s3 */
			gIndex = (s1+l)*r234+s2*r34+(s3+i)*r4+s4;
			lIndex = l*R234+i*R4;
			index2D = i*R4;

			pred1D  = P1[index2D-R4];
			diff = spaceFillingValue[gIndex] - pred1D;

			itvNum = fabs(diff)/realPrecision + 1;

			if (itvNum < quantization_intervals)
			{
				if (diff < 0) itvNum = -itvNum;
				type[lIndex] = (int) (itvNum/2) + intvRadius;
				P1[index2D] = pred1D + 2 * (type[lIndex] - intvRadius) * realPrecision;
			}
			else
			{
				type[lIndex] = 0;
				compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P1[index2D] = vce->data;
			}

			/* Process row-s3+i data s4+1 --> data e4*/
			for (j = 1; j < R4; j++)
			{
				gIndex = (s1+l)*r234+s2*r34+(s3+i)*r4+s4+j;
				lIndex = l*R234+i*R4+j;
				index2D = i*R4+j;

				pred2D  = P1[index2D-1] + P1[index2D-R4] - P1[index2D-R4-1];
				diff = spaceFillingValue[gIndex] - pred2D;

				itvNum = fabs(diff)/realPrecision + 1;

				if (itvNum < quantization_intervals)
				{
					if (diff < 0) itvNum = -itvNum;
					type[lIndex] = (int) (itvNum/2) + intvRadius;
					P1[index2D] = pred2D + 2 * (type[lIndex] - intvRadius) * realPrecision;
				}
				else
				{
					type[lIndex] = 0;
					compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
					updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
					memcpy(preDataBytes,vce->curBytes,4);
					addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
					P1[index2D] = vce->data;
				}
			}
		}


		///////////////////////////	Process layer-s2+1 --> layer-e2 ///////////////////////////

		for (k = 1; k < R2; k++)
		{
			/* Process Row-s3 data s4*/
			gIndex = (s1+l)*r234+(s2+k)*r34+s3*r4+s4;
			lIndex = l*R234+k*R34;
			index2D = 0;

			pred1D = P1[index2D];
			diff = spaceFillingValue[gIndex] - pred1D;

			itvNum = fabs(diff)/realPrecision + 1;

			if (itvNum < quantization_intervals)
			{
				if (diff < 0) itvNum = -itvNum;
				type[lIndex] = (int) (itvNum/2) + intvRadius;
				P0[index2D] = pred1D + 2 * (type[lIndex] - intvRadius) * realPrecision;
			}
			else
			{
				type[lIndex] = 0;
				compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
				P0[index2D] = vce->data;
			}

			/* Process Row-s3 data s4+1 --> data e4 */
			for (j = 1; j < R4; j++)
			{
				gIndex = (s1+l)*r234+(s2+k)*r34+s3*r4+s4+j;
				lIndex = l*R234+k*R34+j;
				index2D = j;

				pred2D = P0[index2D-1] + P1[index2D] - P1[index2D-1];
				diff = spaceFillingValue[gIndex] - pred2D;

				itvNum = fabs(diff)/realPrecision + 1;

				if (itvNum < quantization_intervals)
				{
					if (diff < 0) itvNum = -itvNum;
					type[lIndex] = (int) (itvNum/2) + intvRadius;
					P0[index2D] = pred2D + 2 * (type[lIndex] - intvRadius) * realPrecision;
				}
				else
				{
					type[lIndex] = 0;
					compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
					updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
					memcpy(preDataBytes,vce->curBytes,4);
					addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
					P0[index2D] = vce->data;
				}
			}

			/* Process Row-s3+1 --> Row-e3 */
			for (i = 1; i < R3; i++)
			{
				/* Process Row-s3+i data s4 */
				gIndex = (s1+l)*r234+(s2+k)*r34+(s3+i)*r4+s4;
				lIndex = l*R234+k*R34+i*R4;
				index2D = i*R4;

				pred2D = P0[index2D-R4] + P1[index2D] - P1[index2D-R4];
				diff = spaceFillingValue[gIndex] - pred2D;

				itvNum = fabs(diff)/realPrecision + 1;

				if (itvNum < quantization_intervals)
				{
					if (diff < 0) itvNum = -itvNum;
					type[lIndex] = (int) (itvNum/2) + intvRadius;
					P0[index2D] = pred2D + 2 * (type[lIndex] - intvRadius) * realPrecision;
				}
				else
				{
					type[lIndex] = 0;
					compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
					updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
					memcpy(preDataBytes,vce->curBytes,4);
					addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
					P0[index2D] = vce->data;
				}

				/* Process Row-s3+i data s4+1 --> data e4 */
				for (j = 1; j < R4; j++)
				{
					gIndex = (s1+l)*r234+(s2+k)*r34+(s3+i)*r4+s4+j;
					lIndex = l*R234+k*R34+i*R4+j;
					index2D = i*R4+j;

//					printf ("global index = %d, local index = %d\n", gIndex, lIndex);

					pred3D = P0[index2D-1] + P0[index2D-R4]+ P1[index2D] - P0[index2D-R4-1] - P1[index2D-R4] - P1[index2D-1] + P1[index2D-R4-1];
					diff = spaceFillingValue[gIndex] - pred3D;

					itvNum = fabs(diff)/realPrecision + 1;

					if (itvNum < quantization_intervals)
					{
						if (diff < 0) itvNum = -itvNum;
						type[lIndex] = (int) (itvNum/2) + intvRadius;
						P0[index2D] = pred3D + 2 * (type[lIndex] - intvRadius) * realPrecision;
					}
					else
					{
						type[lIndex] = 0;
						compressSingleFloatValue(vce, spaceFillingValue[gIndex], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
						updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
						memcpy(preDataBytes,vce->curBytes,4);
						addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
						P0[index2D] = vce->data;
					}
				}
			}

			float *Pt;
			Pt = P1;
			P1 = P0;
			P0 = Pt;
		}

	}

	free(P0);
	free(P1);
	size_t exactDataNum = exactLeadNumArray->size;

	TightDataPointStorageF* tdps;

	new_TightDataPointStorageF(&tdps, dataLength, exactDataNum,
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

	return tdps;
}

unsigned int optimize_intervals_float_1D_opt_MSST19(float *oriData, size_t dataLength, double realPrecision)
{	
	size_t i = 0, radiusIndex;
	float pred_value = 0;
	double pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = 0;//dataLength/confparams_cpr->sampleDistance;

	float * data_pos = oriData + 2;
	float divider = log2(1+realPrecision)*2;
	int tempIndex = 0;
	while(data_pos - oriData < dataLength){
		if(*data_pos == 0){
    		data_pos += confparams_cpr->sampleDistance;
            continue;
		}	
	    tempIndex++;
		totalSampleSize++;
		pred_value = data_pos[-1];
		pred_err = fabs((double)*data_pos / pred_value);
		radiusIndex = (unsigned long)fabs(log2(pred_err)/divider+0.5);
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

unsigned int optimize_intervals_float_2D_opt_MSST19(float *oriData, size_t r1, size_t r2, double realPrecision)
{	
	size_t i;
	size_t radiusIndex;
	float pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = 0;

	size_t offset_count = confparams_cpr->sampleDistance - 1; // count r2 offset
	size_t offset_count_2;
	float * data_pos = oriData + r2 + offset_count;
	float divider = log2(1+realPrecision)*2;
	size_t n1_count = 1; // count i sum
	size_t len = r1 * r2;
	while(data_pos - oriData < len){
		if(*data_pos == 0){
        	data_pos += confparams_cpr->sampleDistance;
        	continue;
		}		
		totalSampleSize++;
		pred_value = data_pos[-1] + data_pos[-r2] - data_pos[-r2-1];
		pred_err = fabs(pred_value / *data_pos);
		radiusIndex = (unsigned long)fabs(log2(pred_err)/divider+0.5);
		if(radiusIndex>=confparams_cpr->maxRangeRadius)
			radiusIndex = confparams_cpr->maxRangeRadius - 1;
		intervals[radiusIndex]++;

		offset_count += confparams_cpr->sampleDistance;
		if(offset_count >= r2){
			n1_count ++;
			offset_count_2 = n1_count % confparams_cpr->sampleDistance;
			data_pos += (r2 + confparams_cpr->sampleDistance - offset_count) + (confparams_cpr->sampleDistance - offset_count_2);
			offset_count = (confparams_cpr->sampleDistance - offset_count_2);
			if(offset_count == 0) offset_count ++;
		}
		else data_pos += confparams_cpr->sampleDistance;
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

unsigned int optimize_intervals_float_3D_opt_MSST19(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision)
{	
	size_t i;
	size_t radiusIndex;
	size_t r23=r2*r3;
	float pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = 0;

	size_t offset_count = confparams_cpr->sampleDistance - 2; // count r3 offset
	size_t offset_count_2;
	float * data_pos = oriData + r23 + r3 + offset_count;
	float divider = log2(1+realPrecision)*2;
	size_t n1_count = 1, n2_count = 1; // count i,j sum
	size_t len = r1 * r2 * r3;
	while(data_pos - oriData < len){
		if(*data_pos == 0){
    		data_pos += confparams_cpr->sampleDistance;
        	continue;
		}		
		totalSampleSize++;
		pred_value = data_pos[-1] + data_pos[-r3] + data_pos[-r23] - data_pos[-1-r23] - data_pos[-r3-1] - data_pos[-r3-r23] + data_pos[-r3-r23-1];
		pred_err = fabsf(*data_pos / pred_value);
		radiusIndex = fabs(log2(pred_err)/divider+0.5);
		if(radiusIndex>=confparams_cpr->maxRangeRadius)
		{
			radiusIndex = confparams_cpr->maxRangeRadius - 1;
		}
		intervals[radiusIndex]++;
		offset_count += confparams_cpr->sampleDistance;
		if(offset_count >= r3){
			n2_count ++;
			if(n2_count == r2){
				n1_count ++;
				n2_count = 1;
				data_pos += r3;
			}
			offset_count_2 = (n1_count + n2_count) % confparams_cpr->sampleDistance;
			data_pos += (r3 + confparams_cpr->sampleDistance - offset_count) + (confparams_cpr->sampleDistance - offset_count_2);
			offset_count = (confparams_cpr->sampleDistance - offset_count_2);
			if(offset_count == 0) offset_count ++;
		}
		else data_pos += confparams_cpr->sampleDistance;
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


unsigned int optimize_intervals_float_3D_opt(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision)
{	
	size_t i;
	size_t radiusIndex;
	size_t r23=r2*r3;
	float pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = 0;

	size_t offset_count = confparams_cpr->sampleDistance - 2; // count r3 offset
	size_t offset_count_2;
	float * data_pos = oriData + r23 + r3 + offset_count;
	size_t n1_count = 1, n2_count = 1; // count i,j sum
	size_t len = r1 * r2 * r3;
	while(data_pos - oriData < len){
		totalSampleSize++;
		pred_value = data_pos[-1] + data_pos[-r3] + data_pos[-r23] - data_pos[-1-r23] - data_pos[-r3-1] - data_pos[-r3-r23] + data_pos[-r3-r23-1];
		pred_err = fabs(pred_value - *data_pos);
		radiusIndex = (pred_err/realPrecision+1)/2;
		if(radiusIndex>=confparams_cpr->maxRangeRadius)
		{
			radiusIndex = confparams_cpr->maxRangeRadius - 1;
		}
		intervals[radiusIndex]++;
		offset_count += confparams_cpr->sampleDistance;
		if(offset_count >= r3){
			n2_count ++;
			if(n2_count == r2){
				n1_count ++;
				n2_count = 1;
				data_pos += r3;
			}
			offset_count_2 = (n1_count + n2_count) % confparams_cpr->sampleDistance;
			data_pos += (r3 + confparams_cpr->sampleDistance - offset_count) + (confparams_cpr->sampleDistance - offset_count_2);
			offset_count = (confparams_cpr->sampleDistance - offset_count_2);
			if(offset_count == 0) offset_count ++;
		}
		else data_pos += confparams_cpr->sampleDistance;
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

size_t SZ_compress_float_3D_MDQ_RA_block(float * block_ori_data, float * mean, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, float realPrecision, float * P0, float * P1, int * type, float * unpredictable_data){

	float recip_realPrecision = 1/realPrecision;
	size_t dim0_offset = dim_1 * dim_2;
	size_t dim1_offset = dim_2;

	// data_pos = block_ori_data;
	// for(size_t i=0; i<block_dim_0; i++){
	// 	for(size_t j=0; j<block_dim_1; j++){
	// 		for(size_t k=0; k<block_dim_2; k++){
	// 			sum += *data_pos;
	// 			data_pos ++;
	// 		}
	// 		data_pos += dim1_offset - block_dim_2;
	// 	}
	// 	data_pos += dim0_offset - block_dim_1 * dim1_offset;
	// }
	// size_t num_elements = block_dim_0 * block_dim_1 * block_dim_2;
	// if(num_elements > 0) mean[0] = sum / num_elements;
	// else mean[0] = 0.0;
	mean[0] = block_ori_data[0];

	size_t unpredictable_count = 0;
	size_t r1, r2, r3;
	r1 = block_dim_0;
	r2 = block_dim_1;
	r3 = block_dim_2;

	float * cur_data_pos = block_ori_data;
	float curData;
	float pred1D, pred2D, pred3D;
	float itvNum;
	float diff;
	size_t i, j, k;
	size_t r23 = r2*r3;
	// Process Row-0 data 0
	pred1D = mean[0];
	curData = *cur_data_pos;
	diff = curData - pred1D;
	itvNum = fabsf(diff)*recip_realPrecision + 1;
	if (itvNum < exe_params->intvCapacity){
		if (diff < 0) itvNum = -itvNum;
		type[0] = (int) (itvNum/2) + exe_params->intvRadius;
		P1[0] = pred1D + 2 * (type[0] - exe_params->intvRadius) * realPrecision;
		//ganrantee comporession error against the case of machine-epsilon
		if(fabsf(curData-P1[0])>realPrecision){	
			type[0] = 0;
			P1[0] = curData;
			unpredictable_data[unpredictable_count ++] = curData;
		}		
	}
	else{
		type[0] = 0;
		P1[0] = curData;
		unpredictable_data[unpredictable_count ++] = curData;
	}

	/* Process Row-0 data 1*/
	pred1D = P1[0];
	curData = cur_data_pos[1];
	diff = curData - pred1D;
	itvNum = fabsf(diff)*recip_realPrecision + 1;
	if (itvNum < exe_params->intvCapacity){
		if (diff < 0) itvNum = -itvNum;
		type[1] = (int) (itvNum/2) + exe_params->intvRadius;
		P1[1] = pred1D + 2 * (type[1] - exe_params->intvRadius) * realPrecision;
		//ganrantee comporession error against the case of machine-epsilon
		if(fabsf(curData-P1[1])>realPrecision){	
			type[1] = 0;
			P1[1] = curData;	
			unpredictable_data[unpredictable_count ++] = curData;
		}		
	}
	else{
		type[1] = 0;
		P1[1] = curData;
		unpredictable_data[unpredictable_count ++] = curData;
	}
    /* Process Row-0 data 2 --> data r3-1 */
	for (j = 2; j < r3; j++){
		pred1D = 2*P1[j-1] - P1[j-2];
		curData = cur_data_pos[j];
		diff = curData - pred1D;
		itvNum = fabsf(diff)*recip_realPrecision + 1;
		if (itvNum < exe_params->intvCapacity){
			if (diff < 0) itvNum = -itvNum;
			type[j] = (int) (itvNum/2) + exe_params->intvRadius;
			P1[j] = pred1D + 2 * (type[j] - exe_params->intvRadius) * realPrecision;
			//ganrantee comporession error against the case of machine-epsilon
			if(fabsf(curData-P1[j])>realPrecision){	
				type[j] = 0;
				P1[j] = curData;	
				unpredictable_data[unpredictable_count ++] = curData;
			}			
		}
		else{
			type[j] = 0;
			P1[j] = curData;
			unpredictable_data[unpredictable_count ++] = curData;
		}
	}
	cur_data_pos += dim1_offset;

	/* Process Row-1 --> Row-r2-1 */
	size_t index;
	for (i = 1; i < r2; i++)
	{
		/* Process row-i data 0 */
		index = i*r3;	
		pred1D = P1[index-r3];
		curData = *cur_data_pos;
		diff = curData - pred1D;

		itvNum = fabsf(diff)*recip_realPrecision + 1;

		if (itvNum < exe_params->intvCapacity)
		{
			if (diff < 0) itvNum = -itvNum;
			type[index] = (int) (itvNum/2) + exe_params->intvRadius;
			P1[index] = pred1D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
			
			//ganrantee comporession error against the case of machine-epsilon
			if(fabsf(curData-P1[index])>realPrecision)
			{	
				type[index] = 0;
				P1[index] = curData;	
				unpredictable_data[unpredictable_count ++] = curData;
			}			
		}
		else
		{
			type[index] = 0;
			P1[index] = curData;
			unpredictable_data[unpredictable_count ++] = curData;
		}

		/* Process row-i data 1 --> data r3-1*/
		for (j = 1; j < r3; j++)
		{
			index = i*r3+j;
			pred2D = P1[index-1] + P1[index-r3] - P1[index-r3-1];

			curData = cur_data_pos[j];
			diff = curData - pred2D;

			itvNum = fabsf(diff)*recip_realPrecision + 1;

			if (itvNum < exe_params->intvCapacity)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + exe_params->intvRadius;
				P1[index] = pred2D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
				
				//ganrantee comporession error against the case of machine-epsilon
				if(fabsf(curData-P1[index])>realPrecision)
				{	
					type[index] = 0;
					P1[index] = curData;	
					unpredictable_data[unpredictable_count ++] = curData;
				}				
			}
			else
			{
				type[index] = 0;
				P1[index] = curData;
				unpredictable_data[unpredictable_count ++] = curData;
			}
		}
		cur_data_pos += dim1_offset;
	}
	cur_data_pos += dim0_offset - r2 * dim1_offset;

	///////////////////////////	Process layer-1 --> layer-r1-1 ///////////////////////////

	for (k = 1; k < r1; k++)
	{
		/* Process Row-0 data 0*/
		index = k*r23;
		pred1D = P1[0];
		curData = *cur_data_pos;
		diff = curData - pred1D;
		itvNum = fabsf(diff)*recip_realPrecision + 1;
		if (itvNum < exe_params->intvCapacity)
		{
			if (diff < 0) itvNum = -itvNum;
			type[index] = (int) (itvNum/2) + exe_params->intvRadius;
			P0[0] = pred1D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
			//ganrantee comporession error against the case of machine-epsilon
			if(fabsf(curData-P0[0])>realPrecision)
			{	
				type[index] = 0;
				P0[0] = curData;	
				unpredictable_data[unpredictable_count ++] = curData;
			}			
		}
		else
		{
			type[index] = 0;
			P0[0] = curData;
			unpredictable_data[unpredictable_count ++] = curData;
		}
	    /* Process Row-0 data 1 --> data r3-1 */
		for (j = 1; j < r3; j++)
		{
			//index = k*r2*r3+j;
			index ++;
			pred2D = P0[j-1] + P1[j] - P1[j-1];
			curData = cur_data_pos[j];
			diff = curData - pred2D;
			itvNum = fabsf(diff)*recip_realPrecision + 1;
			if (itvNum < exe_params->intvCapacity)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + exe_params->intvRadius;
				P0[j] = pred2D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
				//ganrantee comporession error against the case of machine-epsilon
				if(fabsf(curData-P0[j])>realPrecision)
				{	
					type[index] = 0;
					P0[j] = curData;	
					unpredictable_data[unpredictable_count ++] = curData;
				}
			}
			else
			{
				type[index] = 0;
				P0[j] = curData;
				unpredictable_data[unpredictable_count ++] = curData;
			}
		}

		cur_data_pos += dim1_offset;
	    /* Process Row-1 --> Row-r2-1 */
		size_t index2D;
		for (i = 1; i < r2; i++)
		{
			/* Process Row-i data 0 */
			index = k*r23 + i*r3;
			index2D = i*r3;		
			pred2D = P0[index2D-r3] + P1[index2D] - P1[index2D-r3];
			curData = *cur_data_pos;
			diff = curData - pred2D;

			itvNum = fabsf(diff)*recip_realPrecision + 1;

			if (itvNum < exe_params->intvCapacity)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + exe_params->intvRadius;
				P0[index2D] = pred2D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
				//ganrantee comporession error against the case of machine-epsilon
				if(fabsf(curData-P0[index2D])>realPrecision)
				{	
					type[index] = 0;
					P0[index2D] = curData;	
					unpredictable_data[unpredictable_count ++] = curData;
				}				
			}
			else
			{
				type[index] = 0;
				P0[index2D] = curData;
				unpredictable_data[unpredictable_count ++] = curData;
			}

			/* Process Row-i data 1 --> data r3-1 */
			for (j = 1; j < r3; j++)
			{
				//index = k*r2*r3 + i*r3 + j;			
				index ++;
				index2D = i*r3 + j;
				pred3D = P0[index2D-1] + P0[index2D-r3]+ P1[index2D] - P0[index2D-r3-1] - P1[index2D-r3] - P1[index2D-1] + P1[index2D-r3-1];
				curData = cur_data_pos[j];
				diff = curData - pred3D;

				itvNum = fabsf(diff)*recip_realPrecision + 1;

				if (itvNum < exe_params->intvCapacity)
				{
					if (diff < 0) itvNum = -itvNum;
					type[index] = (int) (itvNum/2) + exe_params->intvRadius;
					P0[index2D] = pred3D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
					
					//ganrantee comporession error against the case of machine-epsilon
					if(fabsf(curData-P0[index2D])>realPrecision)
					{	
						type[index] = 0;
						P0[index2D] = curData;	
						unpredictable_data[unpredictable_count ++] = curData;
					}					
				}
				else
				{
					type[index] = 0;
					P0[index2D] = curData;
					unpredictable_data[unpredictable_count ++] = curData;
				}
			}
			cur_data_pos += dim1_offset;
		}
		cur_data_pos += dim0_offset - r2 * dim1_offset;
		float *Pt;
		Pt = P1;
		P1 = P0;
		P0 = Pt;
	}

	return unpredictable_count;
}


unsigned int optimize_intervals_float_2D_opt(float *oriData, size_t r1, size_t r2, double realPrecision)
{	
	size_t i;
	size_t radiusIndex;
	float pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = 0;

	size_t offset_count = confparams_cpr->sampleDistance - 1; // count r2 offset
	size_t offset_count_2;
	float * data_pos = oriData + r2 + offset_count;
	size_t n1_count = 1; // count i sum
	size_t len = r1 * r2;
	while(data_pos - oriData < len){
		totalSampleSize++;
		pred_value = data_pos[-1] + data_pos[-r2] - data_pos[-r2-1];
		pred_err = fabs(pred_value - *data_pos);
		radiusIndex = (unsigned long)((pred_err/realPrecision+1)/2);
		if(radiusIndex>=confparams_cpr->maxRangeRadius)
			radiusIndex = confparams_cpr->maxRangeRadius - 1;
		intervals[radiusIndex]++;

		offset_count += confparams_cpr->sampleDistance;
		if(offset_count >= r2){
			n1_count ++;
			offset_count_2 = n1_count % confparams_cpr->sampleDistance;
			data_pos += (r2 + confparams_cpr->sampleDistance - offset_count) + (confparams_cpr->sampleDistance - offset_count_2);
			offset_count = (confparams_cpr->sampleDistance - offset_count_2);
			if(offset_count == 0) offset_count ++;
		}
		else data_pos += confparams_cpr->sampleDistance;
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



size_t SZ_compress_float_1D_MDQ_RA_block(float * block_ori_data, float * mean, size_t dim_0, size_t block_dim_0, double realPrecision, int * type, float * unpredictable_data){

	mean[0] = block_ori_data[0];
	unsigned short unpredictable_count = 0;

	float curData;
	double itvNum;
	double diff;
	float last_over_thres = mean[0];
	float pred1D;
	size_t type_index = 0;
	float * data_pos = block_ori_data;
	for(size_t i=0; i<block_dim_0; i++){
		curData = *data_pos;

		pred1D = last_over_thres;
		diff = curData - pred1D;
		itvNum = fabs(diff)/realPrecision + 1;
		if (itvNum < exe_params->intvCapacity){
			if (diff < 0) itvNum = -itvNum;
			type[type_index] = (int) (itvNum/2) + exe_params->intvRadius;	
			last_over_thres = pred1D + 2 * (type[type_index] - exe_params->intvRadius) * realPrecision;
			if(fabs(curData-last_over_thres)>realPrecision){
				type[type_index] = 0;
				last_over_thres = curData;
				unpredictable_data[unpredictable_count ++] = curData;
			}

		}
		else{
			type[type_index] = 0;
			unpredictable_data[unpredictable_count ++] = curData;
			last_over_thres = curData;
		}
		type_index ++;
		data_pos ++;
	}
	return unpredictable_count;

}

size_t SZ_compress_float_2D_MDQ_RA_block(float * block_ori_data, float * mean, size_t dim_0, size_t dim_1, size_t block_dim_0, size_t block_dim_1, double realPrecision, float * P0, float * P1, int * type, float * unpredictable_data){

	size_t dim0_offset = dim_1;
	mean[0] = block_ori_data[0];

	size_t unpredictable_count = 0;
	size_t r1, r2;
	r1 = block_dim_0;
	r2 = block_dim_1;

	float * cur_data_pos = block_ori_data;
	float curData;
	float pred1D, pred2D;
	double itvNum;
	double diff;
	size_t i, j;
	/* Process Row-0 data 0*/
	curData = *cur_data_pos;
	pred1D = mean[0];
	diff = curData - pred1D;
	itvNum = fabs(diff)/realPrecision + 1;
	if (itvNum < exe_params->intvCapacity){
		if (diff < 0) itvNum = -itvNum;
		type[0] = (int) (itvNum/2) + exe_params->intvRadius;
		P1[0] = pred1D + 2 * (type[0] - exe_params->intvRadius) * realPrecision;
		//ganrantee comporession error against the case of machine-epsilon
		if(fabs(curData-P1[0])>realPrecision){	
			type[0] = 0;
			P1[0] = curData;
			unpredictable_data[unpredictable_count ++] = curData;
		}		
	}
	else{
		type[0] = 0;
		P1[0] = curData;
		unpredictable_data[unpredictable_count ++] = curData;
	}

	/* Process Row-0 data 1*/
	curData = cur_data_pos[1];
	pred1D = P1[0];
	diff = curData - pred1D;
	itvNum = fabs(diff)/realPrecision + 1;
	if (itvNum < exe_params->intvCapacity){
		if (diff < 0) itvNum = -itvNum;
		type[1] = (int) (itvNum/2) + exe_params->intvRadius;
		P1[1] = pred1D + 2 * (type[1] - exe_params->intvRadius) * realPrecision;
		//ganrantee comporession error against the case of machine-epsilon
		if(fabs(curData-P1[1])>realPrecision){	
			type[1] = 0;
			P1[1] = curData;	
			unpredictable_data[unpredictable_count ++] = curData;
		}		
	}
	else{
		type[1] = 0;
		P1[1] = curData;
		unpredictable_data[unpredictable_count ++] = curData;
	}

    /* Process Row-0 data 2 --> data r2-1 */
	for (j = 2; j < r2; j++)
	{
		curData = cur_data_pos[j];
		pred1D = 2*P1[j-1] - P1[j-2];
		diff = curData - pred1D;
		itvNum = fabs(diff)/realPrecision + 1;
		if (itvNum < exe_params->intvCapacity){
			if (diff < 0) itvNum = -itvNum;
			type[j] = (int) (itvNum/2) + exe_params->intvRadius;
			P1[j] = pred1D + 2 * (type[j] - exe_params->intvRadius) * realPrecision;
			//ganrantee comporession error against the case of machine-epsilon
			if(fabs(curData-P1[j])>realPrecision){	
				type[j] = 0;
				P1[j] = curData;	
				unpredictable_data[unpredictable_count ++] = curData;
			}			
		}
		else{
			type[j] = 0;
			P1[j] = curData;
			unpredictable_data[unpredictable_count ++] = curData;
		}
	}
	cur_data_pos += dim0_offset;
	/* Process Row-1 --> Row-r1-1 */
	size_t index;
	for (i = 1; i < r1; i++)
	{	
		/* Process row-i data 0 */
		index = i*r2;
		curData = *cur_data_pos;
		pred1D = P1[0];
		diff = curData - pred1D;
		itvNum = fabs(diff)/realPrecision + 1;
		if (itvNum < exe_params->intvCapacity){
			if (diff < 0) itvNum = -itvNum;
			type[index] = (int) (itvNum/2) + exe_params->intvRadius;
			P0[0] = pred1D + 2 * (type[j] - exe_params->intvRadius) * realPrecision;
			//ganrantee comporession error against the case of machine-epsilon
			if(fabs(curData-P0[0])>realPrecision){	
				type[index] = 0;
				P0[0] = curData;	
				unpredictable_data[unpredictable_count ++] = curData;
			}			
		}
		else{
			type[index] = 0;
			P0[0] = curData;
			unpredictable_data[unpredictable_count ++] = curData;
		}
									
		/* Process row-i data 1 --> r2-1*/
		for (j = 1; j < r2; j++)
		{
			index = i*r2+j;
			curData = cur_data_pos[j];
			pred2D = P0[j-1] + P1[j] - P1[j-1];
			diff = curData - pred2D;
			itvNum = fabs(diff)/realPrecision + 1;
			if (itvNum < exe_params->intvCapacity)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + exe_params->intvRadius;
				P0[j] = pred2D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
				
				//ganrantee comporession error against the case of machine-epsilon
				if(fabs(curData-P0[j])>realPrecision)
				{	
					type[index] = 0;
					P0[j] = curData;	
					unpredictable_data[unpredictable_count ++] = curData;
				}				
			}
			else
			{
				type[index] = 0;
				P0[j] = curData;
				unpredictable_data[unpredictable_count ++] = curData;
			}
		}
		cur_data_pos += dim0_offset;

		float *Pt;
		Pt = P1;
		P1 = P0;
		P0 = Pt;
	}
	return unpredictable_count;
}

/*The above code is for sz 1.4.13; the following code is for sz 2.0*/
static unsigned int optimize_intervals_float_1D_with_freq_and_dense_pos(float *oriData, size_t r1, double realPrecision, float * dense_pos, float * max_freq, float * mean_freq)
{	
	float mean = 0.0;
	size_t len = r1;
	size_t mean_distance = (int) (sqrt(len));

	float * data_pos = oriData;
	size_t mean_count = 0;
	while(data_pos - oriData < len){
		mean += *data_pos;
		mean_count ++;
		data_pos += mean_distance;
	}
	if(mean_count > 0) mean /= mean_count;
	size_t range = 8192;
	size_t radius = 4096;
	size_t * freq_intervals = (size_t *) malloc(range*sizeof(size_t));
	memset(freq_intervals, 0, range*sizeof(size_t));

	unsigned int maxRangeRadius = confparams_cpr->maxRangeRadius;
	int sampleDistance = confparams_cpr->sampleDistance;
	float predThreshold = confparams_cpr->predThreshold;

	size_t i;
	size_t radiusIndex;
	float pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, maxRangeRadius*sizeof(size_t));

	float mean_diff;
	ptrdiff_t freq_index;
	size_t freq_count = 0;
	size_t sample_count = 0;
	data_pos = oriData + 1;
	while(data_pos - oriData < len){
		pred_value = data_pos[-1];
		pred_err = fabs(pred_value - *data_pos);
		if(pred_err < realPrecision) freq_count ++;
		radiusIndex = (unsigned long)((pred_err/realPrecision+1)/2);
		if(radiusIndex>=maxRangeRadius)
			radiusIndex = maxRangeRadius - 1;
		intervals[radiusIndex]++;

		mean_diff = *data_pos - mean;
		if(mean_diff > 0) freq_index = (ptrdiff_t)(mean_diff/realPrecision) + radius;
		else freq_index = (ptrdiff_t)(mean_diff/realPrecision) - 1 + radius;
		if(freq_index <= 0){
			freq_intervals[0] ++;
		}
		else if(freq_index >= range){
			freq_intervals[range - 1] ++;
		}
		else{
			freq_intervals[freq_index] ++;
		}
		data_pos += sampleDistance;
		sample_count ++;
	}
	*max_freq = freq_count * 1.0/ sample_count;

	//compute the appropriate number
	size_t targetCount = sample_count*predThreshold;
	size_t sum = 0;
	for(i=0;i<maxRangeRadius;i++)
	{
		sum += intervals[i];
		if(sum>targetCount)
			break;
	}
	if(i>=maxRangeRadius)
		i = maxRangeRadius-1;
	unsigned int accIntervals = 2*(i+1);
	unsigned int powerOf2 = roundUpToPowerOf2(accIntervals);

	if(powerOf2<32)
		powerOf2 = 32;

	// collect frequency
	size_t max_sum = 0;
	size_t max_index = 0;
	size_t tmp_sum;
	size_t * freq_pos = freq_intervals + 1;
	for(size_t i=1; i<range-2; i++){
		tmp_sum = freq_pos[0] + freq_pos[1];
		if(tmp_sum > max_sum){
			max_sum = tmp_sum;
			max_index = i;
		}
		freq_pos ++;
	}
	*dense_pos = mean + realPrecision * (ptrdiff_t)(max_index + 1 - radius);
	*mean_freq = max_sum * 1.0 / sample_count;

	free(freq_intervals);
	free(intervals);
	return powerOf2;
}

unsigned int optimize_intervals_float_2D_with_freq_and_dense_pos(float *oriData, size_t r1, size_t r2, double realPrecision, float * dense_pos, float * max_freq, float * mean_freq)
{	
	float mean = 0.0;
	size_t len = r1 * r2;
	size_t mean_distance = (int) (sqrt(len));

	float * data_pos = oriData;
	size_t mean_count = 0;
	while(data_pos - oriData < len){
		mean += *data_pos;
		mean_count ++;
		data_pos += mean_distance;
	}
	if(mean_count > 0) mean /= mean_count;
	size_t range = 8192;
	size_t radius = 4096;
	size_t * freq_intervals = (size_t *) malloc(range*sizeof(size_t));
	memset(freq_intervals, 0, range*sizeof(size_t));

	unsigned int maxRangeRadius = confparams_cpr->maxRangeRadius;
	int sampleDistance = confparams_cpr->sampleDistance;
	float predThreshold = confparams_cpr->predThreshold;

	size_t i;
	size_t radiusIndex;
	float pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, maxRangeRadius*sizeof(size_t));

	float mean_diff;
	ptrdiff_t freq_index;
	size_t freq_count = 0;
	size_t n1_count = 1;
	size_t offset_count = sampleDistance - 1;
	size_t offset_count_2 = 0;
	size_t sample_count = 0;
	data_pos = oriData + r2 + offset_count;
	while(data_pos - oriData < len){
		pred_value = data_pos[-1] + data_pos[-r2] - data_pos[-r2-1];
		pred_err = fabs(pred_value - *data_pos);
		if(pred_err < realPrecision) freq_count ++;
		radiusIndex = (unsigned long)((pred_err/realPrecision+1)/2);
		if(radiusIndex>=maxRangeRadius)
			radiusIndex = maxRangeRadius - 1;
		intervals[radiusIndex]++;

		mean_diff = *data_pos - mean;
		if(mean_diff > 0) freq_index = (ptrdiff_t)(mean_diff/realPrecision) + radius;
		else freq_index = (ptrdiff_t)(mean_diff/realPrecision) - 1 + radius;
		if(freq_index <= 0){
			freq_intervals[0] ++;
		}
		else if(freq_index >= range){
			freq_intervals[range - 1] ++;
		}
		else{
			freq_intervals[freq_index] ++;
		}
		offset_count += sampleDistance;
		if(offset_count >= r2){
			n1_count ++;
			offset_count_2 = n1_count % sampleDistance;
			data_pos += (r2 + sampleDistance - offset_count) + (sampleDistance - offset_count_2);
			offset_count = (sampleDistance - offset_count_2);
			if(offset_count == 0) offset_count ++;
		}
		else data_pos += sampleDistance;
		sample_count ++;
	}
	*max_freq = freq_count * 1.0/ sample_count;

	//compute the appropriate number
	size_t targetCount = sample_count*predThreshold;
	size_t sum = 0;
	for(i=0;i<maxRangeRadius;i++)
	{
		sum += intervals[i];
		if(sum>targetCount)
			break;
	}
	if(i>=maxRangeRadius)
		i = maxRangeRadius-1;
	unsigned int accIntervals = 2*(i+1);
	unsigned int powerOf2 = roundUpToPowerOf2(accIntervals);

	if(powerOf2<32)
		powerOf2 = 32;

	// collect frequency
	size_t max_sum = 0;
	size_t max_index = 0;
	size_t tmp_sum;
	size_t * freq_pos = freq_intervals + 1;
	for(size_t i=1; i<range-2; i++){
		tmp_sum = freq_pos[0] + freq_pos[1];
		if(tmp_sum > max_sum){
			max_sum = tmp_sum;
			max_index = i;
		}
		freq_pos ++;
	}
	*dense_pos = mean + realPrecision * (ptrdiff_t)(max_index + 1 - radius);
	*mean_freq = max_sum * 1.0 / sample_count;

	free(freq_intervals);
	free(intervals);
	return powerOf2;
}

// 2D:  modified for higher performance
#define MIN(a, b) a<b? a : b
unsigned char * SZ_compress_float_2D_MDQ_nonblocked_with_blocked_regression(float *oriData, size_t r1, size_t r2, float realPrecision, size_t * comp_size){

	float recip_realPrecision = 1/realPrecision;
	unsigned int quantization_intervals;
	float sz_sample_correct_freq = -1;//0.5; //-1
	float dense_pos;
	float mean_flush_freq;
	unsigned char use_mean = 0;

	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_2D_with_freq_and_dense_pos(oriData, r1, r2, realPrecision, &dense_pos, &sz_sample_correct_freq, &mean_flush_freq);
		if(mean_flush_freq > 0.5 || mean_flush_freq > sz_sample_correct_freq) use_mean = 1;
		updateQuantizationInfo(quantization_intervals);
	}	
	else{
		quantization_intervals = exe_params->intvCapacity;
	}

	// calculate block dims
	size_t num_x, num_y;
	size_t block_size = 16;

	SZ_COMPUTE_2D_NUMBER_OF_BLOCKS(r1, num_x, block_size);
	SZ_COMPUTE_2D_NUMBER_OF_BLOCKS(r2, num_y, block_size);

	size_t split_index_x, split_index_y;
	size_t early_blockcount_x, early_blockcount_y;
	size_t late_blockcount_x, late_blockcount_y;
	SZ_COMPUTE_BLOCKCOUNT(r1, num_x, split_index_x, early_blockcount_x, late_blockcount_x);
	SZ_COMPUTE_BLOCKCOUNT(r2, num_y, split_index_y, early_blockcount_y, late_blockcount_y);

	size_t max_num_block_elements = early_blockcount_x * early_blockcount_y;
	size_t num_blocks = num_x * num_y;
	size_t num_elements = r1 * r2;

	size_t dim0_offset = r2;	

	int * result_type = (int *) malloc(num_elements * sizeof(int));
	size_t unpred_data_max_size = max_num_block_elements;
	float * result_unpredictable_data = (float *) malloc(unpred_data_max_size * sizeof(float) * num_blocks);
	size_t total_unpred = 0;
	size_t unpredictable_count;
	float * data_pos = oriData;
	int * type = result_type;
	size_t offset_x, offset_y;
	size_t current_blockcount_x, current_blockcount_y;

	float * reg_params = (float *) malloc(num_blocks * 4 * sizeof(float));
	float * reg_params_pos = reg_params;
	// move regression part out
	size_t params_offset_b = num_blocks;
	size_t params_offset_c = 2*num_blocks;
	for(size_t i=0; i<num_x; i++){
		for(size_t j=0; j<num_y; j++){
			current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
			current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
			offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
			offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;

			data_pos = oriData + offset_x * dim0_offset + offset_y;

			{
				float * cur_data_pos = data_pos;
				float fx = 0.0;
				float fy = 0.0;
				float f = 0;
				float sum_x; 
				float curData;
				for(size_t i=0; i<current_blockcount_x; i++){
					sum_x = 0;
					for(size_t j=0; j<current_blockcount_y; j++){
						curData = *cur_data_pos;
						sum_x += curData;
						fy += curData * j;
						cur_data_pos ++;
					}
					fx += sum_x * i;
					f += sum_x;
					cur_data_pos += dim0_offset - current_blockcount_y;
				}
				float coeff = 1.0 / (current_blockcount_x * current_blockcount_y);
				reg_params_pos[0] = (2 * fx / (current_blockcount_x - 1) - f) * 6 * coeff / (current_blockcount_x + 1);
				reg_params_pos[params_offset_b] = (2 * fy / (current_blockcount_y - 1) - f) * 6 * coeff / (current_blockcount_y + 1);
				reg_params_pos[params_offset_c] = f * coeff - ((current_blockcount_x - 1) * reg_params_pos[0] / 2 + (current_blockcount_y - 1) * reg_params_pos[params_offset_b] / 2);
			}

			reg_params_pos ++;
		}
	}

	//Compress coefficient arrays
	float precision_a, precision_b, precision_c;
	float rel_param_err = 0.15/3;
	precision_a = rel_param_err * realPrecision / late_blockcount_x;
	precision_b = rel_param_err * realPrecision / late_blockcount_y;
	precision_c = rel_param_err * realPrecision;

	float mean = 0;
	use_mean = 0;
	if(use_mean){
		// compute mean
		float sum = 0.0;
		size_t mean_count = 0;
		for(size_t i=0; i<num_elements; i++){
			if(fabsf(oriData[i] - dense_pos) < realPrecision){
				sum += oriData[i];
				mean_count ++;
			}
		}
		if(mean_count > 0) mean = sum / mean_count;
	}


	float tmp_realPrecision = realPrecision;

	// use two prediction buffers for higher performance
	float * unpredictable_data = result_unpredictable_data;
	unsigned char * indicator = (unsigned char *) malloc(num_blocks * sizeof(unsigned char));
	memset(indicator, 0, num_blocks * sizeof(unsigned char));
	size_t reg_count = 0;
	size_t strip_dim_0 = early_blockcount_x + 1;
	size_t strip_dim_1 = r2 + 1;
	size_t strip_dim0_offset = strip_dim_1;
	unsigned char * indicator_pos = indicator;
	size_t prediction_buffer_size = strip_dim_0 * strip_dim0_offset * sizeof(float);
	float * prediction_buffer_1 = (float *) malloc(prediction_buffer_size);
	memset(prediction_buffer_1, 0, prediction_buffer_size);
	float * prediction_buffer_2 = (float *) malloc(prediction_buffer_size);
	memset(prediction_buffer_2, 0, prediction_buffer_size);
	float * cur_pb_buf = prediction_buffer_1;
	float * next_pb_buf = prediction_buffer_2;
	float * cur_pb_buf_pos;
	float * next_pb_buf_pos;
	int intvCapacity = quantization_intervals; //exe_params->intvCapacity;
	int intvRadius = intvCapacity/2; //exe_params->intvRadius;
	int use_reg = 0;

	reg_params_pos = reg_params;
	// compress the regression coefficients on the fly
	float last_coeffcients[3] = {0.0};
	int coeff_intvCapacity_sz = 65536;
	int coeff_intvRadius = coeff_intvCapacity_sz / 2;
	int * coeff_type[3];
	int * coeff_result_type = (int *) malloc(num_blocks*3*sizeof(int));
	float * coeff_unpred_data[3];
	float * coeff_unpredictable_data = (float *) malloc(num_blocks*3*sizeof(float));
	float precision[3], recip_precision[3];
	precision[0] = precision_a, precision[1] = precision_b, precision[2] = precision_c;
	//compute the recip_precision
	recip_precision[0] = 1/precision_a, recip_precision[1] = 1/precision_b, recip_precision[2] = 1/precision_c;	
	
	for(int i=0; i<3; i++){
		coeff_type[i] = coeff_result_type + i * num_blocks;
		coeff_unpred_data[i] = coeff_unpredictable_data + i * num_blocks;
	}
	int coeff_index = 0;
	unsigned int coeff_unpredictable_count[3] = {0};
	float noise = realPrecision * 0.81;
	if(use_mean){
		type = result_type;
		int intvCapacity_sz = intvCapacity - 2;
		for(size_t i=0; i<num_x; i++){
			current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
			offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
			data_pos = oriData + offset_x * dim0_offset;

			cur_pb_buf_pos = cur_pb_buf + strip_dim0_offset + 1;
			next_pb_buf_pos = next_pb_buf + 1;
			float * pb_pos = cur_pb_buf_pos;
			float * next_pb_pos = next_pb_buf_pos;

			for(size_t j=0; j<num_y; j++){
				offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
				current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
				
				/*sampling: decide which predictor to use (regression or lorenzo)*/
				{
					float * cur_data_pos;
					float curData;
					float pred_reg, pred_sz;
					float err_sz = 0.0, err_reg = 0.0;
					// [1, 1] [3, 3] [5, 5] [7, 7] [9, 9]
					// [1, 9] [3, 7]		[7, 3] [9, 1]
					int bmi = 0;
					int block_size = MIN(current_blockcount_x, current_blockcount_y);
					for(int i=1; i<block_size; i++){
						cur_data_pos = data_pos + i * dim0_offset + i;
						curData = *cur_data_pos;
						pred_sz = cur_data_pos[-1] + cur_data_pos[-dim0_offset] - cur_data_pos[-dim0_offset - 1];
						pred_reg = reg_params_pos[0] * i + reg_params_pos[params_offset_b] * i + reg_params_pos[params_offset_c];							
						err_sz += MIN(fabsf(pred_sz - curData) + noise, fabsf(mean - curData));
						err_reg += fabsf(pred_reg - curData);

						bmi = block_size - i;
						cur_data_pos = data_pos + i*dim0_offset + bmi;
						curData = *cur_data_pos;
						pred_sz = cur_data_pos[-1] + cur_data_pos[-dim0_offset] - cur_data_pos[-dim0_offset - 1];
						pred_reg = reg_params_pos[0] * i + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c];							
						err_sz += MIN(fabsf(pred_sz - curData) + noise, fabsf(mean - curData));
						err_reg += fabsf(pred_reg - curData);								
					}
					use_reg = (err_reg < err_sz);
				}
				if(use_reg)
				{
					{
						/*predict coefficients in current block via previous reg_block*/
						float cur_coeff;
						float diff, itvNum;
						for(int e=0; e<3; e++){
							cur_coeff = reg_params_pos[e*num_blocks];
							diff = cur_coeff - last_coeffcients[e];
							itvNum = fabsf(diff)*recip_precision[e] + 1;
							if (itvNum < coeff_intvCapacity_sz){
								if (diff < 0) itvNum = -itvNum;
								coeff_type[e][coeff_index] = (int) (itvNum/2) + coeff_intvRadius;
								last_coeffcients[e] = last_coeffcients[e] + 2 * (coeff_type[e][coeff_index] - coeff_intvRadius) * precision[e];
								//ganrantee comporession error against the case of machine-epsilon
								if(fabsf(cur_coeff - last_coeffcients[e])>precision[e]){	
									coeff_type[e][coeff_index] = 0;
									last_coeffcients[e] = cur_coeff;	
									coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
								}					
							}
							else{
								coeff_type[e][coeff_index] = 0;
								last_coeffcients[e] = cur_coeff;
								coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
							}
						}
						coeff_index ++;
					}
					float curData;
					float pred;
					float itvNum;
					float diff;
					size_t index = 0;
					size_t block_unpredictable_count = 0;
					float * cur_data_pos = data_pos;
					for(size_t ii=0; ii<current_blockcount_x - 1; ii++){
						for(size_t jj=0; jj<current_blockcount_y - 1; jj++){
							curData = *cur_data_pos;
							pred = last_coeffcients[0] * ii + last_coeffcients[1] * jj + last_coeffcients[2];
							diff = curData - pred;
							itvNum = fabsf(diff)*recip_realPrecision + 1;
							if (itvNum < intvCapacity){
								if (diff < 0) itvNum = -itvNum;
								type[index] = (int) (itvNum/2) + intvRadius;
								pred = pred + 2 * (type[index] - intvRadius) * realPrecision;
								//ganrantee comporession error against the case of machine-epsilon
								if(fabsf(curData - pred)>realPrecision){	
									type[index] = 0;
									pred = curData;
									unpredictable_data[block_unpredictable_count ++] = curData;
								}		
							}
							else{
								type[index] = 0;
								pred = curData;
								unpredictable_data[block_unpredictable_count ++] = curData;
							}
							index ++;	
							cur_data_pos ++;
						}
						/*dealing with the last jj (boundary)*/
						{
							size_t jj = current_blockcount_y - 1;
							curData = *cur_data_pos;
							pred = last_coeffcients[0] * ii + last_coeffcients[1] * jj + last_coeffcients[2];
							diff = curData - pred;
							itvNum = fabsf(diff)*recip_realPrecision + 1;
							if (itvNum < intvCapacity){
								if (diff < 0) itvNum = -itvNum;
								type[index] = (int) (itvNum/2) + intvRadius;
								pred = pred + 2 * (type[index] - intvRadius) * realPrecision;
								//ganrantee comporession error against the case of machine-epsilon
								if(fabsf(curData - pred)>realPrecision){	
									type[index] = 0;
									pred = curData;
									unpredictable_data[block_unpredictable_count ++] = curData;
								}		
							}
							else{
								type[index] = 0;
								pred = curData;
								unpredictable_data[block_unpredictable_count ++] = curData;
							}

							// assign value to block surfaces
							pb_pos[ii * strip_dim0_offset + jj] = pred;
							index ++;	
							cur_data_pos ++;
						}
						cur_data_pos += dim0_offset - current_blockcount_y;
					}
					/*dealing with the last ii (boundary)*/
					{
						size_t ii = current_blockcount_x - 1;
						for(size_t jj=0; jj<current_blockcount_y - 1; jj++){
							curData = *cur_data_pos;
							pred = last_coeffcients[0] * ii + last_coeffcients[1] * jj + last_coeffcients[2];
							diff = curData - pred;
							itvNum = fabsf(diff)*recip_realPrecision + 1;
							if (itvNum < intvCapacity){
								if (diff < 0) itvNum = -itvNum;
								type[index] = (int) (itvNum/2) + intvRadius;
								pred = pred + 2 * (type[index] - intvRadius) * realPrecision;
								//ganrantee comporession error against the case of machine-epsilon
								if(fabsf(curData - pred)>realPrecision){	
									type[index] = 0;
									pred = curData;
									unpredictable_data[block_unpredictable_count ++] = curData;
								}		
							}
							else{
								type[index] = 0;
								pred = curData;
								unpredictable_data[block_unpredictable_count ++] = curData;
							}
							// assign value to next prediction buffer
							next_pb_pos[jj] = pred;
							index ++;	
							cur_data_pos ++;
						}
						/*dealing with the last jj (boundary)*/
						{
							size_t jj = current_blockcount_y - 1;
							curData = *cur_data_pos;
							pred = last_coeffcients[0] * ii + last_coeffcients[1] * jj + last_coeffcients[2];
							diff = curData - pred;
							itvNum = fabsf(diff)*recip_realPrecision + 1;
							if (itvNum < intvCapacity){
								if (diff < 0) itvNum = -itvNum;
								type[index] = (int) (itvNum/2) + intvRadius;
								pred = pred + 2 * (type[index] - intvRadius) * realPrecision;
								//ganrantee comporession error against the case of machine-epsilon
								if(fabsf(curData - pred)>realPrecision){	
									type[index] = 0;
									pred = curData;
									unpredictable_data[block_unpredictable_count ++] = curData;
								}		
							}
							else{
								type[index] = 0;
								pred = curData;
								unpredictable_data[block_unpredictable_count ++] = curData;
							}

							// assign value to block surfaces
							pb_pos[ii * strip_dim0_offset + jj] = pred;
							// assign value to next prediction buffer
							next_pb_pos[jj] = pred;

							index ++;	
							cur_data_pos ++;
						}
					} // end ii == -1
					unpredictable_count = block_unpredictable_count;
					total_unpred += unpredictable_count;
					unpredictable_data += unpredictable_count;					
					reg_count ++;
				}// end use_reg
				else{
					// use SZ
					// SZ predication
					unpredictable_count = 0;
					float * cur_pb_pos = pb_pos;
					float * cur_data_pos = data_pos;
					float curData;
					float pred2D;
					float itvNum, diff;
					size_t index = 0;
					for(size_t ii=0; ii<current_blockcount_x - 1; ii++){
						for(size_t jj=0; jj<current_blockcount_y; jj++){
							curData = *cur_data_pos;
							if(fabsf(curData - mean) <= realPrecision){
								// adjust type[index] to intvRadius for coherence with freq in reg
								type[index] = intvRadius;
								*cur_pb_pos = mean;
							}
							else
							{
								pred2D = cur_pb_pos[-1] + cur_pb_pos[-strip_dim0_offset] - cur_pb_pos[-strip_dim0_offset - 1];
								diff = curData - pred2D;
								itvNum = fabsf(diff)*recip_realPrecision + 1;
								if (itvNum < intvCapacity_sz){
									if (diff < 0) itvNum = -itvNum;
									type[index] = (int) (itvNum/2) + intvRadius;
									*cur_pb_pos = pred2D + 2 * (type[index] - intvRadius) * tmp_realPrecision;
									if(type[index] <= intvRadius) type[index] -= 1;
									//ganrantee comporession error against the case of machine-epsilon
									if(fabsf(curData - *cur_pb_pos)>tmp_realPrecision){	
										type[index] = 0;
										*cur_pb_pos = curData;	
										unpredictable_data[unpredictable_count ++] = curData;
									}					
								}
								else{
									type[index] = 0;
									*cur_pb_pos = curData;
									unpredictable_data[unpredictable_count ++] = curData;
								}
							}
							index ++;
							cur_pb_pos ++;
							cur_data_pos ++;
						}
						cur_pb_pos += strip_dim0_offset - current_blockcount_y;
						cur_data_pos += dim0_offset - current_blockcount_y;
					}
					/*dealing with the last ii (boundary)*/
					{
						// ii == current_blockcount_x - 1
						for(size_t jj=0; jj<current_blockcount_y; jj++){
							curData = *cur_data_pos;
							if(fabsf(curData - mean) <= realPrecision){
								// adjust type[index] to intvRadius for coherence with freq in reg
								type[index] = intvRadius;
								*cur_pb_pos = mean;
							}
							else
							{
								pred2D = cur_pb_pos[-1] + cur_pb_pos[-strip_dim0_offset] - cur_pb_pos[-strip_dim0_offset - 1];
								diff = curData - pred2D;
								itvNum = fabsf(diff)*recip_realPrecision + 1;
								if (itvNum < intvCapacity_sz){
									if (diff < 0) itvNum = -itvNum;
									type[index] = (int) (itvNum/2) + intvRadius;
									*cur_pb_pos = pred2D + 2 * (type[index] - intvRadius) * tmp_realPrecision;
									if(type[index] <= intvRadius) type[index] -= 1;
									//ganrantee comporession error against the case of machine-epsilon
									if(fabsf(curData - *cur_pb_pos)>tmp_realPrecision){	
										type[index] = 0;
										*cur_pb_pos = curData;	
										unpredictable_data[unpredictable_count ++] = curData;
									}					
								}
								else{
									type[index] = 0;
									*cur_pb_pos = curData;
									unpredictable_data[unpredictable_count ++] = curData;
								}
							}
							next_pb_pos[jj] = *cur_pb_pos;
							index ++;
							cur_pb_pos ++;
							cur_data_pos ++;
						}
					}
					total_unpred += unpredictable_count;
					unpredictable_data += unpredictable_count;
					// change indicator
					indicator_pos[j] = 1;
				}// end SZ
				reg_params_pos ++;
				data_pos += current_blockcount_y;
				pb_pos += current_blockcount_y;
				next_pb_pos += current_blockcount_y;
				type += current_blockcount_x * current_blockcount_y;
			}// end j
			indicator_pos += num_y;
			float * tmp;
			tmp = cur_pb_buf;
			cur_pb_buf = next_pb_buf;
			next_pb_buf = tmp;
		}// end i
	}// end use mean
	else{
		type = result_type;
		int intvCapacity_sz = intvCapacity - 2;
		for(size_t i=0; i<num_x; i++){
			current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
			offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
			data_pos = oriData + offset_x * dim0_offset;

			cur_pb_buf_pos = cur_pb_buf + strip_dim0_offset + 1;
			next_pb_buf_pos = next_pb_buf + 1;
			float * pb_pos = cur_pb_buf_pos;
			float * next_pb_pos = next_pb_buf_pos;

			for(size_t j=0; j<num_y; j++){
				offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
				current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
				/*sampling*/
				{
					// sample [2i + 1, 2i + 1] [2i + 1, bs - 2i]
					float * cur_data_pos;
					float curData;
					float pred_reg, pred_sz;
					float err_sz = 0.0, err_reg = 0.0;
					// [1, 1] [3, 3] [5, 5] [7, 7] [9, 9]
					// [1, 9] [3, 7]		[7, 3] [9, 1]
					int bmi = 0;
					int block_size = MIN(current_blockcount_x, current_blockcount_y);
					for(int i=1; i<block_size; i++){
						cur_data_pos = data_pos + i * dim0_offset + i;
						curData = *cur_data_pos;
						pred_sz = cur_data_pos[-1] + cur_data_pos[-dim0_offset] - cur_data_pos[-dim0_offset - 1];
						pred_reg = reg_params_pos[0] * i + reg_params_pos[params_offset_b] * i + reg_params_pos[params_offset_c];							
						err_sz += fabsf(pred_sz - curData) + noise;
						err_reg += fabsf(pred_reg - curData);

						bmi = block_size - i;
						cur_data_pos = data_pos + i*dim0_offset + bmi;
						curData = *cur_data_pos;
						pred_sz = cur_data_pos[-1] + cur_data_pos[-dim0_offset] - cur_data_pos[-dim0_offset - 1];
						pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c];							
						err_sz += fabsf(pred_sz - curData) + noise;
						err_reg += fabsf(pred_reg - curData);								
					}
					use_reg = (err_reg < err_sz);
				}
				if(use_reg)
				{
					{
						/*predict coefficients in current block via previous reg_block*/
						float cur_coeff;
						float diff, itvNum;
						for(int e=0; e<3; e++){
							cur_coeff = reg_params_pos[e*num_blocks];
							diff = cur_coeff - last_coeffcients[e];
							itvNum = fabsf(diff)/precision[e] + 1;
							if (itvNum < coeff_intvCapacity_sz){
								if (diff < 0) itvNum = -itvNum;
								coeff_type[e][coeff_index] = (int) (itvNum/2) + coeff_intvRadius;
								last_coeffcients[e] = last_coeffcients[e] + 2 * (coeff_type[e][coeff_index] - coeff_intvRadius) * precision[e];
								//ganrantee comporession error against the case of machine-epsilon
								if(fabsf(cur_coeff - last_coeffcients[e])>precision[e]){	
									coeff_type[e][coeff_index] = 0;
									last_coeffcients[e] = cur_coeff;	
									coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
								}					
							}
							else{
								coeff_type[e][coeff_index] = 0;
								last_coeffcients[e] = cur_coeff;
								coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
							}
						}
						coeff_index ++;
					}
					float curData;
					float pred;
					float itvNum;
					float diff;
					size_t index = 0;
					size_t block_unpredictable_count = 0;
					float * cur_data_pos = data_pos;
					for(size_t ii=0; ii<current_blockcount_x - 1; ii++){
						for(size_t jj=0; jj<current_blockcount_y - 1; jj++){
							curData = *cur_data_pos;
							pred = last_coeffcients[0] * ii + last_coeffcients[1] * jj + last_coeffcients[2];
							diff = curData - pred;
							itvNum = fabsf(diff)*recip_realPrecision + 1;
							if (itvNum < intvCapacity){
								if (diff < 0) itvNum = -itvNum;
								type[index] = (int) (itvNum/2) + intvRadius;
								pred = pred + 2 * (type[index] - intvRadius) * realPrecision;
								//ganrantee comporession error against the case of machine-epsilon
								if(fabsf(curData - pred)>realPrecision){	
									type[index] = 0;
									pred = curData;
									unpredictable_data[block_unpredictable_count ++] = curData;
								}		
							}
							else{
								type[index] = 0;
								pred = curData;
								unpredictable_data[block_unpredictable_count ++] = curData;
							}
							index ++;	
							cur_data_pos ++;
						}
						/*dealing with the last jj (boundary)*/
						{
							// jj == current_blockcount_y - 1
							size_t jj = current_blockcount_y - 1;
							curData = *cur_data_pos;
							pred = last_coeffcients[0] * ii + last_coeffcients[1] * jj + last_coeffcients[2];
							diff = curData - pred;
							itvNum = fabsf(diff)*recip_realPrecision + 1;
							if (itvNum < intvCapacity){
								if (diff < 0) itvNum = -itvNum;
								type[index] = (int) (itvNum/2) + intvRadius;
								pred = pred + 2 * (type[index] - intvRadius) * realPrecision;
								//ganrantee comporession error against the case of machine-epsilon
								if(fabsf(curData - pred)>realPrecision){	
									type[index] = 0;
									pred = curData;
									unpredictable_data[block_unpredictable_count ++] = curData;
								}		
							}
							else{
								type[index] = 0;
								pred = curData;
								unpredictable_data[block_unpredictable_count ++] = curData;
							}

							// assign value to block surfaces
							pb_pos[ii * strip_dim0_offset + jj] = pred;
							index ++;	
							cur_data_pos ++;
						}
						cur_data_pos += dim0_offset - current_blockcount_y;
					}
					/*dealing with the last ii (boundary)*/
					{
						size_t ii = current_blockcount_x - 1;
						for(size_t jj=0; jj<current_blockcount_y - 1; jj++){
							curData = *cur_data_pos;
							pred = last_coeffcients[0] * ii + last_coeffcients[1] * jj + last_coeffcients[2];
							diff = curData - pred;
							itvNum = fabsf(diff)*recip_realPrecision + 1;
							if (itvNum < intvCapacity){
								if (diff < 0) itvNum = -itvNum;
								type[index] = (int) (itvNum/2) + intvRadius;
								pred = pred + 2 * (type[index] - intvRadius) * realPrecision;
								//ganrantee comporession error against the case of machine-epsilon
								if(fabsf(curData - pred)>realPrecision){	
									type[index] = 0;
									pred = curData;
									unpredictable_data[block_unpredictable_count ++] = curData;
								}		
							}
							else{
								type[index] = 0;
								pred = curData;
								unpredictable_data[block_unpredictable_count ++] = curData;
							}
							// assign value to next prediction buffer
							next_pb_pos[jj] = pred;
							index ++;	
							cur_data_pos ++;
						}
						/*dealing with the last jj (boundary)*/
						{
							// jj == current_blockcount_y - 1
							size_t jj = current_blockcount_y - 1;
							curData = *cur_data_pos;
							pred = last_coeffcients[0] * ii + last_coeffcients[1] * jj + last_coeffcients[2];
							diff = curData - pred;
							itvNum = fabsf(diff)*recip_realPrecision + 1;
							if (itvNum < intvCapacity){
								if (diff < 0) itvNum = -itvNum;
								type[index] = (int) (itvNum/2) + intvRadius;
								pred = pred + 2 * (type[index] - intvRadius) * realPrecision;
								//ganrantee comporession error against the case of machine-epsilon
								if(fabsf(curData - pred)>realPrecision){	
									type[index] = 0;
									pred = curData;
									unpredictable_data[block_unpredictable_count ++] = curData;
								}		
							}
							else{
								type[index] = 0;
								pred = curData;
								unpredictable_data[block_unpredictable_count ++] = curData;
							}

							// assign value to block surfaces
							pb_pos[ii * strip_dim0_offset + jj] = pred;
							// assign value to next prediction buffer
							next_pb_pos[jj] = pred;

							index ++;	
							cur_data_pos ++;
						}
					} // end ii == -1
					unpredictable_count = block_unpredictable_count;
					total_unpred += unpredictable_count;
					unpredictable_data += unpredictable_count;					
					reg_count ++;
				}// end use_reg
				else{
					// use SZ
					// SZ predication
					unpredictable_count = 0;
					float * cur_pb_pos = pb_pos;
					float * cur_data_pos = data_pos;
					float curData;
					float pred2D;
					float itvNum, diff;
					size_t index = 0;
					for(size_t ii=0; ii<current_blockcount_x - 1; ii++){
						for(size_t jj=0; jj<current_blockcount_y; jj++){
							curData = *cur_data_pos;

							pred2D = cur_pb_pos[-1] + cur_pb_pos[-strip_dim0_offset] - cur_pb_pos[-strip_dim0_offset - 1];
							diff = curData - pred2D;
							itvNum = fabsf(diff)*recip_realPrecision + 1;
							if (itvNum < intvCapacity_sz){
								if (diff < 0) itvNum = -itvNum;
								type[index] = (int) (itvNum/2) + intvRadius;
								*cur_pb_pos = pred2D + 2 * (type[index] - intvRadius) * tmp_realPrecision;
								//ganrantee comporession error against the case of machine-epsilon
								if(fabsf(curData - *cur_pb_pos)>tmp_realPrecision){	
									type[index] = 0;
									*cur_pb_pos = curData;	
									unpredictable_data[unpredictable_count ++] = curData;
								}					
							}
							else{
								type[index] = 0;
								*cur_pb_pos = curData;
								unpredictable_data[unpredictable_count ++] = curData;
							}

							index ++;
							cur_pb_pos ++;
							cur_data_pos ++;
						}
						cur_pb_pos += strip_dim0_offset - current_blockcount_y;
						cur_data_pos += dim0_offset - current_blockcount_y;
					}
					/*dealing with the last ii (boundary)*/
					{
						// ii == current_blockcount_x - 1
						for(size_t jj=0; jj<current_blockcount_y; jj++){
							curData = *cur_data_pos;

							pred2D = cur_pb_pos[-1] + cur_pb_pos[-strip_dim0_offset] - cur_pb_pos[-strip_dim0_offset - 1];
							diff = curData - pred2D;
							itvNum = fabsf(diff)*recip_realPrecision + 1;
							if (itvNum < intvCapacity_sz){
								if (diff < 0) itvNum = -itvNum;
								type[index] = (int) (itvNum/2) + intvRadius;
								*cur_pb_pos = pred2D + 2 * (type[index] - intvRadius) * tmp_realPrecision;
								//ganrantee comporession error against the case of machine-epsilon
								if(fabsf(curData - *cur_pb_pos)>tmp_realPrecision){	
									type[index] = 0;
									*cur_pb_pos = curData;	
									unpredictable_data[unpredictable_count ++] = curData;
								}					
							}
							else{
								type[index] = 0;
								*cur_pb_pos = curData;
								unpredictable_data[unpredictable_count ++] = curData;
							}
							next_pb_pos[jj] = *cur_pb_pos;
							index ++;
							cur_pb_pos ++;
							cur_data_pos ++;
						}
					}
					total_unpred += unpredictable_count;
					unpredictable_data += unpredictable_count;
					// change indicator
					indicator_pos[j] = 1;
				}// end SZ
				reg_params_pos ++;
				data_pos += current_blockcount_y;
				pb_pos += current_blockcount_y;
				next_pb_pos += current_blockcount_y;
				type += current_blockcount_x * current_blockcount_y;
			}// end j
			indicator_pos += num_y;
			float * tmp;
			tmp = cur_pb_buf;
			cur_pb_buf = next_pb_buf;
			next_pb_buf = tmp;
		}// end i		
	}
	free(prediction_buffer_1);
	free(prediction_buffer_2);

	int stateNum = 2*quantization_intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);

	size_t nodeCount = 0;
	size_t i = 0;
	init(huffmanTree, result_type, num_elements);
	for (i = 0; i < stateNum; i++)
		if (huffmanTree->code[i]) nodeCount++; 
	nodeCount = nodeCount*2-1;

	unsigned char *treeBytes;
	unsigned int treeByteSize = convert_HuffTree_to_bytes_anyStates(huffmanTree, nodeCount, &treeBytes);

	unsigned int meta_data_offset = 3 + 1 + MetaDataByteLength;
	// total size 										metadata		  # elements   real precision		intervals	nodeCount		huffman 	 	block index 						unpredicatable count						mean 					 	unpred size 				elements
	unsigned char * result = (unsigned char *) calloc(meta_data_offset + exe_params->SZ_SIZE_TYPE + sizeof(float) + sizeof(int) + sizeof(int) + 5*treeByteSize + 3*num_blocks*sizeof(int) + num_blocks * sizeof(unsigned short) + num_blocks * sizeof(unsigned short) + num_blocks * sizeof(float) + total_unpred * sizeof(float) + num_elements * sizeof(int), 1);
	unsigned char * result_pos = result;
	initRandomAccessBytes(result_pos);
	result_pos += meta_data_offset;

	sizeToBytes(result_pos, num_elements);
	result_pos += exe_params->SZ_SIZE_TYPE;
	
	intToBytes_bigEndian(result_pos, block_size);
	result_pos += sizeof(int);
	floatToBytes(result_pos, realPrecision);
	result_pos += sizeof(float);
	intToBytes_bigEndian(result_pos, quantization_intervals);
	result_pos += sizeof(int);
	intToBytes_bigEndian(result_pos, treeByteSize);
	result_pos += sizeof(int);
	intToBytes_bigEndian(result_pos, nodeCount);
	result_pos += sizeof(int);
	memcpy(result_pos, treeBytes, treeByteSize);
	result_pos += treeByteSize;
	free(treeBytes);

	memcpy(result_pos, &use_mean, sizeof(unsigned char));
	result_pos += sizeof(unsigned char);
	memcpy(result_pos, &mean, sizeof(float));
	result_pos += sizeof(float);

	size_t indicator_size = convertIntArray2ByteArray_fast_1b_to_result(indicator, num_blocks, result_pos);
	result_pos += indicator_size;
	
	//convert the lead/mid/resi to byte stream 	
	if(reg_count>0){
		for(int e=0; e<3; e++){
			int stateNum = 2*coeff_intvCapacity_sz;
			HuffmanTree* huffmanTree = createHuffmanTree(stateNum);
			size_t nodeCount = 0;
			init(huffmanTree, coeff_type[e], reg_count);
			size_t i = 0;
			for (i = 0; i < huffmanTree->stateNum; i++)
				if (huffmanTree->code[i]) nodeCount++; 
			nodeCount = nodeCount*2-1;
			unsigned char *treeBytes;
			unsigned int treeByteSize = convert_HuffTree_to_bytes_anyStates(huffmanTree, nodeCount, &treeBytes);
			floatToBytes(result_pos, precision[e]);
			result_pos += sizeof(float);
			intToBytes_bigEndian(result_pos, coeff_intvRadius);
			result_pos += sizeof(int);
			intToBytes_bigEndian(result_pos, treeByteSize);
			result_pos += sizeof(int);
			intToBytes_bigEndian(result_pos, nodeCount);
			result_pos += sizeof(int);
			memcpy(result_pos, treeBytes, treeByteSize);		
			result_pos += treeByteSize;
			free(treeBytes);
			size_t typeArray_size = 0;
			encode(huffmanTree, coeff_type[e], reg_count, result_pos + sizeof(size_t), &typeArray_size);
			sizeToBytes(result_pos, typeArray_size);
			result_pos += sizeof(size_t) + typeArray_size;
			intToBytes_bigEndian(result_pos, coeff_unpredictable_count[e]);
			result_pos += sizeof(int);
			memcpy(result_pos, coeff_unpred_data[e], coeff_unpredictable_count[e]*sizeof(float));
			result_pos += coeff_unpredictable_count[e]*sizeof(float);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	free(coeff_result_type);
	free(coeff_unpredictable_data);

	//record the number of unpredictable data and also store them
	memcpy(result_pos, &total_unpred, sizeof(size_t));
	result_pos += sizeof(size_t);
	memcpy(result_pos, result_unpredictable_data, total_unpred * sizeof(float));
	result_pos += total_unpred * sizeof(float);
	size_t typeArray_size = 0;
	encode(huffmanTree, result_type, num_elements, result_pos, &typeArray_size);
	result_pos += typeArray_size;
	
#ifdef HAVE_WRITESTATS
	writeHuffmanInfo(treeByteSize, typeArray_size, num_elements*sizeof(float), nodeCount);
	writeBlockInfo(use_mean, block_size, reg_count, num_blocks);
	writeUnpredictDataCounts(total_unpred, num_elements);
#endif	

	size_t totalEncodeSize = result_pos - result;
	free(indicator);
	free(result_unpredictable_data);
	free(result_type);
	free(reg_params);
	
	SZ_ReleaseHuffman(huffmanTree);
	*comp_size = totalEncodeSize;

	return result;
}



unsigned int optimize_intervals_float_3D_with_freq_and_dense_pos(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, float * dense_pos, float * max_freq, float * mean_freq)
{	
	float mean = 0.0;
	size_t len = r1 * r2 * r3;
	size_t mean_distance = (int) (sqrt(len));
	float * data_pos = oriData;
	size_t offset_count = 0;
	size_t offset_count_2 = 0;
	size_t mean_count = 0;
	while(data_pos - oriData < len){
		mean += *data_pos;
		mean_count ++;
		data_pos += mean_distance;
		offset_count += mean_distance;
		offset_count_2 += mean_distance;
		if(offset_count >= r3){
			offset_count = 0;
			data_pos -= 1;
		}
		if(offset_count_2 >= r2 * r3){
			offset_count_2 = 0;
			data_pos -= 1;
		}
	}
	if(mean_count > 0) mean /= mean_count;
	size_t range = 8192;
	size_t radius = 4096;
	size_t * freq_intervals = (size_t *) malloc(range*sizeof(size_t));
	memset(freq_intervals, 0, range*sizeof(size_t));

	unsigned int maxRangeRadius = confparams_cpr->maxRangeRadius;
	int sampleDistance = confparams_cpr->sampleDistance;
	float predThreshold = confparams_cpr->predThreshold;

	size_t i;
	size_t radiusIndex;
	size_t r23=r2*r3;
	float pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, maxRangeRadius*sizeof(size_t));

	float mean_diff;
	ptrdiff_t freq_index;
	size_t freq_count = 0;
	size_t sample_count = 0;

	offset_count = confparams_cpr->sampleDistance - 2; // count r3 offset
	data_pos = oriData + r23 + r3 + offset_count;
	size_t n1_count = 1, n2_count = 1; // count i,j sum

	while(data_pos - oriData < len){

		pred_value = data_pos[-1] + data_pos[-r3] + data_pos[-r23] - data_pos[-1-r23] - data_pos[-r3-1] - data_pos[-r3-r23] + data_pos[-r3-r23-1];
		pred_err = fabs(pred_value - *data_pos);
		if(pred_err < realPrecision) freq_count ++;
		radiusIndex = (pred_err/realPrecision+1)/2;
		if(radiusIndex>=maxRangeRadius)
		{
			radiusIndex = maxRangeRadius - 1;
		}
		intervals[radiusIndex]++;

		mean_diff = *data_pos - mean;
		if(mean_diff > 0) freq_index = (ptrdiff_t)(mean_diff/realPrecision) + radius;
		else freq_index = (ptrdiff_t)(mean_diff/realPrecision) - 1 + radius;
		if(freq_index <= 0){
			freq_intervals[0] ++;
		}
		else if(freq_index >= range){
			freq_intervals[range - 1] ++;
		}
		else{
			freq_intervals[freq_index] ++;
		}
		offset_count += sampleDistance;
		if(offset_count >= r3){
			n2_count ++;
			if(n2_count == r2){
				n1_count ++;
				n2_count = 1;
				data_pos += r3;
			}
			offset_count_2 = (n1_count + n2_count) % sampleDistance;
			data_pos += (r3 + sampleDistance - offset_count) + (sampleDistance - offset_count_2);
			offset_count = (sampleDistance - offset_count_2);
			if(offset_count == 0) offset_count ++;
		}
		else data_pos += sampleDistance;
		sample_count ++;
	}	
	*max_freq = freq_count * 1.0/ sample_count;

	//compute the appropriate number
	size_t targetCount = sample_count*predThreshold;
	size_t sum = 0;
	for(i=0;i<maxRangeRadius;i++)
	{
		sum += intervals[i];
		if(sum>targetCount)
			break;
	}
	if(i>=maxRangeRadius)
		i = maxRangeRadius-1;
	unsigned int accIntervals = 2*(i+1);
	unsigned int powerOf2 = roundUpToPowerOf2(accIntervals);

	if(powerOf2<32)
		powerOf2 = 32;
	// collect frequency
	size_t max_sum = 0;
	size_t max_index = 0;
	size_t tmp_sum;
	size_t * freq_pos = freq_intervals + 1;
	for(size_t i=1; i<range-2; i++){
		tmp_sum = freq_pos[0] + freq_pos[1];
		if(tmp_sum > max_sum){
			max_sum = tmp_sum;
			max_index = i;
		}
		freq_pos ++;
	}
	*dense_pos = mean + realPrecision * (ptrdiff_t)(max_index + 1 - radius);
	*mean_freq = max_sum * 1.0 / sample_count;

	free(freq_intervals);
	free(intervals);
	return powerOf2;
}


// 3D:  modified for higher performance
unsigned char * SZ_compress_float_3D_MDQ_nonblocked_with_blocked_regression(float *oriData, size_t r1, size_t r2, size_t r3, float realPrecision, size_t * comp_size){

#ifdef HAVE_TIMECMPR	
	float* decData = NULL;
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData = (float*)(multisteps->hist_data);
#endif

	float recip_realPrecision = 1/realPrecision;
	//printf("recip_realPrecision = %.20G\n", recip_realPrecision);

	unsigned int quantization_intervals;
	float sz_sample_correct_freq = -1;//0.5; //-1
	float dense_pos;
	float mean_flush_freq;
	unsigned char use_mean = 0;

	// calculate block dims
	size_t num_x, num_y, num_z;
	size_t block_size = 6;
	SZ_COMPUTE_3D_NUMBER_OF_BLOCKS(r1, num_x, block_size);
	SZ_COMPUTE_3D_NUMBER_OF_BLOCKS(r2, num_y, block_size);
	SZ_COMPUTE_3D_NUMBER_OF_BLOCKS(r3, num_z, block_size);

	//printf("num_x=%zu, num_y=%zu, num_z=%zu\n", num_x, num_y, num_z);

	size_t split_index_x, split_index_y, split_index_z;
	size_t early_blockcount_x, early_blockcount_y, early_blockcount_z;
	size_t late_blockcount_x, late_blockcount_y, late_blockcount_z;
	SZ_COMPUTE_BLOCKCOUNT(r1, num_x, split_index_x, early_blockcount_x, late_blockcount_x);
	SZ_COMPUTE_BLOCKCOUNT(r2, num_y, split_index_y, early_blockcount_y, late_blockcount_y);
	SZ_COMPUTE_BLOCKCOUNT(r3, num_z, split_index_z, early_blockcount_z, late_blockcount_z);

	size_t max_num_block_elements = early_blockcount_x * early_blockcount_y * early_blockcount_z;
	size_t num_blocks = num_x * num_y * num_z;
	size_t num_elements = r1 * r2 * r3;

	size_t dim0_offset = r2 * r3;
	size_t dim1_offset = r3;	

	int * result_type = (int *) malloc(num_elements * sizeof(int));
	memset(result_type, 0, num_elements*sizeof(int));
	size_t unpred_data_max_size = max_num_block_elements;
	float * result_unpredictable_data = (float *) malloc(unpred_data_max_size * sizeof(float) * num_blocks);
	size_t total_unpred = 0;
	size_t unpredictable_count;
	size_t max_unpred_count = 0;
	float * data_pos = oriData;
	int * type = result_type;
	size_t type_offset;
	size_t offset_x, offset_y, offset_z;
	size_t current_blockcount_x, current_blockcount_y, current_blockcount_z;

	float * reg_params = (float *) malloc(num_blocks * 4 * sizeof(float));
	float * reg_params_pos = reg_params;
	// move regression part out
	size_t params_offset_b = num_blocks;
	size_t params_offset_c = 2*num_blocks;
	size_t params_offset_d = 3*num_blocks;
	for(size_t i=0; i<num_x; i++){
		for(size_t j=0; j<num_y; j++){
			for(size_t k=0; k<num_z; k++){
				current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
				current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
				current_blockcount_z = (k < split_index_z) ? early_blockcount_z : late_blockcount_z;
				offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
				offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
				offset_z = (k < split_index_z) ? k * early_blockcount_z : k * late_blockcount_z + split_index_z;
	
				data_pos = oriData + offset_x * dim0_offset + offset_y * dim1_offset + offset_z;
				/*Calculate regression coefficients*/
				{
					float * cur_data_pos = data_pos;
					float fx = 0.0;
					float fy = 0.0;
					float fz = 0.0;
					float f = 0;
					float sum_x, sum_y; 
					float curData;
					for(size_t i=0; i<current_blockcount_x; i++){
						sum_x = 0;
						for(size_t j=0; j<current_blockcount_y; j++){
							sum_y = 0;
							for(size_t k=0; k<current_blockcount_z; k++){
								curData = *cur_data_pos;
								// f += curData;
								// fx += curData * i;
								// fy += curData * j;
								// fz += curData * k;
								sum_y += curData;
								fz += curData * k;
								cur_data_pos ++;
							}
							fy += sum_y * j;
							sum_x += sum_y;
							cur_data_pos += dim1_offset - current_blockcount_z;
						}
						fx += sum_x * i;
						f += sum_x;
						cur_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
					}
					float coeff = 1.0 / (current_blockcount_x * current_blockcount_y * current_blockcount_z);
					reg_params_pos[0] = (2 * fx / (current_blockcount_x - 1) - f) * 6 * coeff / (current_blockcount_x + 1);
					reg_params_pos[params_offset_b] = (2 * fy / (current_blockcount_y - 1) - f) * 6 * coeff / (current_blockcount_y + 1);
					reg_params_pos[params_offset_c] = (2 * fz / (current_blockcount_z - 1) - f) * 6 * coeff / (current_blockcount_z + 1);
					reg_params_pos[params_offset_d] = f * coeff - ((current_blockcount_x - 1) * reg_params_pos[0] / 2 + (current_blockcount_y - 1) * reg_params_pos[params_offset_b] / 2 + (current_blockcount_z - 1) * reg_params_pos[params_offset_c] / 2);
				}
				reg_params_pos ++;
			}
		}
	}
	
	//Compress coefficient arrays
	float precision_a, precision_b, precision_c, precision_d;
	float rel_param_err = 0.025;
	precision_a = rel_param_err * realPrecision / late_blockcount_x;
	precision_b = rel_param_err * realPrecision / late_blockcount_y;
	precision_c = rel_param_err * realPrecision / late_blockcount_z;
	precision_d = rel_param_err * realPrecision;

	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_3D_with_freq_and_dense_pos(oriData, r1, r2, r3, realPrecision, &dense_pos, &sz_sample_correct_freq, &mean_flush_freq);
		if(mean_flush_freq > 0.5 || mean_flush_freq > sz_sample_correct_freq) use_mean = 1;
		updateQuantizationInfo(quantization_intervals);
	}	
	else{
		quantization_intervals = exe_params->intvCapacity;
	}

	float mean = 0;
	if(use_mean){
		// compute mean
		float sum = 0.0;
		size_t mean_count = 0;
		for(size_t i=0; i<num_elements; i++){
			if(fabsf(oriData[i] - dense_pos) < realPrecision){
				sum += oriData[i];
				mean_count ++;
			}
		}
		if(mean_count > 0) mean = sum / mean_count;
	}

	float tmp_realPrecision = realPrecision;

	// use two prediction buffers for higher performance
	float * unpredictable_data = result_unpredictable_data;
	unsigned char * indicator = (unsigned char *) malloc(num_blocks * sizeof(unsigned char));
	memset(indicator, 0, num_blocks * sizeof(unsigned char));
	size_t reg_count = 0;
	size_t strip_dim_0 = early_blockcount_x + 1;
	size_t strip_dim_1 = r2 + 1;
	size_t strip_dim_2 = r3 + 1;
	size_t strip_dim0_offset = strip_dim_1 * strip_dim_2;
	size_t strip_dim1_offset = strip_dim_2;
	unsigned char * indicator_pos = indicator;

	size_t prediction_buffer_size = strip_dim_0 * strip_dim0_offset * sizeof(float);
	float * prediction_buffer_1 = (float *) malloc(prediction_buffer_size);
	memset(prediction_buffer_1, 0, prediction_buffer_size);
	float * prediction_buffer_2 = (float *) malloc(prediction_buffer_size);
	memset(prediction_buffer_2, 0, prediction_buffer_size);
	float * cur_pb_buf = prediction_buffer_1;
	float * next_pb_buf = prediction_buffer_2;
	float * cur_pb_buf_pos;
	float * next_pb_buf_pos;
	int intvCapacity = quantization_intervals;// exe_params->intvCapacity;
	int intvRadius = intvCapacity/2; //exe_params->intvRadius;	
	int use_reg = 0;
	float noise = realPrecision * 1.22;

	reg_params_pos = reg_params;
	// compress the regression coefficients on the fly
	float last_coeffcients[4] = {0.0};
	int coeff_intvCapacity_sz = 65536;
	int coeff_intvRadius = coeff_intvCapacity_sz / 2;
	int * coeff_type[4];
	int * coeff_result_type = (int *) malloc(num_blocks*4*sizeof(int));
	float * coeff_unpred_data[4];
	float * coeff_unpredictable_data = (float *) malloc(num_blocks*4*sizeof(float));
	float precision[4], recip_precision[4];
	precision[0] = precision_a, precision[1] = precision_b, precision[2] = precision_c, precision[3] = precision_d;
	recip_precision[0] = 1/precision_a, recip_precision[1] = 1/precision_b, recip_precision[2] = 1/precision_c, recip_precision[3] = 1/precision_d;
	
	for(int i=0; i<4; i++){
		coeff_type[i] = coeff_result_type + i * num_blocks;
		coeff_unpred_data[i] = coeff_unpredictable_data + i * num_blocks;
	}
	int coeff_index = 0;
	unsigned int coeff_unpredictable_count[4] = {0};

	if(use_mean){
		int intvCapacity_sz = intvCapacity - 2;
		for(size_t i=0; i<num_x; i++){
			current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
			offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
			for(size_t j=0; j<num_y; j++){
				offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
				current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
				data_pos = oriData + offset_x * dim0_offset + offset_y * dim1_offset;
				type_offset = offset_x * dim0_offset +  offset_y * current_blockcount_x * dim1_offset;
				type = result_type + type_offset;

				// prediction buffer is (current_block_count_x + 1) * (current_block_count_y + 1) * (current_block_count_z + 1)
				cur_pb_buf_pos = cur_pb_buf + offset_y * strip_dim1_offset + strip_dim0_offset + strip_dim1_offset + 1;
				next_pb_buf_pos = next_pb_buf + offset_y * strip_dim1_offset + strip_dim1_offset + 1;

				size_t current_blockcount_z;
				float * pb_pos = cur_pb_buf_pos;
				float * next_pb_pos = next_pb_buf_pos;
				size_t strip_unpredictable_count = 0;
				for(size_t k=0; k<num_z; k++){
					current_blockcount_z = (k < split_index_z) ? early_blockcount_z : late_blockcount_z;
#ifdef HAVE_TIMECMPR
					size_t offset_z = 0;
					offset_z = (k < split_index_z) ? k * early_blockcount_z : k * late_blockcount_z + split_index_z;
					size_t block_offset = offset_x * dim0_offset + offset_y * dim1_offset + offset_z;
#endif
					/*sampling and decide which predictor*/
					{
						// sample point [1, 1, 1] [1, 1, 4] [1, 4, 1] [1, 4, 4] [4, 1, 1] [4, 1, 4] [4, 4, 1] [4, 4, 4]
						float * cur_data_pos;
						float curData;
						float pred_reg, pred_sz;
						float err_sz = 0.0, err_reg = 0.0;
						int bmi = 0;
						int block_size = MIN(current_blockcount_x, (MIN(current_blockcount_y, current_blockcount_z)));
						for(int i=1; i<block_size; i++){
							cur_data_pos = data_pos + i*dim0_offset + i*dim1_offset + i;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-dim1_offset]+ cur_data_pos[-dim0_offset] - cur_data_pos[-dim1_offset - 1] - cur_data_pos[-dim0_offset - 1] - cur_data_pos[-dim0_offset - dim1_offset] + cur_data_pos[-dim0_offset - dim1_offset - 1];
							pred_reg = reg_params_pos[0] * i + reg_params_pos[params_offset_b] * i + reg_params_pos[params_offset_c] * i + reg_params_pos[params_offset_d];							
							err_sz += MIN(fabsf(pred_sz - curData) + noise, fabsf(mean - curData));
							err_reg += fabsf(pred_reg - curData);

							bmi = block_size - i;
							cur_data_pos = data_pos + i*dim0_offset + i*dim1_offset + bmi;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-dim1_offset]+ cur_data_pos[-dim0_offset] - cur_data_pos[-dim1_offset - 1] - cur_data_pos[-dim0_offset - 1] - cur_data_pos[-dim0_offset - dim1_offset] + cur_data_pos[-dim0_offset - dim1_offset - 1];
							pred_reg = reg_params_pos[0] * i + reg_params_pos[params_offset_b] * i + reg_params_pos[params_offset_c] * bmi + reg_params_pos[params_offset_d];							
							err_sz += MIN(fabsf(pred_sz - curData) + noise, fabsf(mean - curData));
							err_reg += fabsf(pred_reg - curData);								

							cur_data_pos = data_pos + i*dim0_offset + bmi*dim1_offset + i;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-dim1_offset]+ cur_data_pos[-dim0_offset] - cur_data_pos[-dim1_offset - 1] - cur_data_pos[-dim0_offset - 1] - cur_data_pos[-dim0_offset - dim1_offset] + cur_data_pos[-dim0_offset - dim1_offset - 1];
							pred_reg = reg_params_pos[0] * i + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c] * i + reg_params_pos[params_offset_d];							
							err_sz += MIN(fabsf(pred_sz - curData) + noise, fabsf(mean - curData));
							err_reg += fabsf(pred_reg - curData);								

							cur_data_pos = data_pos + i*dim0_offset + bmi*dim1_offset + bmi;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-dim1_offset]+ cur_data_pos[-dim0_offset] - cur_data_pos[-dim1_offset - 1] - cur_data_pos[-dim0_offset - 1] - cur_data_pos[-dim0_offset - dim1_offset] + cur_data_pos[-dim0_offset - dim1_offset - 1];
							pred_reg = reg_params_pos[0] * i + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c] * bmi + reg_params_pos[params_offset_d];							
							err_sz += MIN(fabsf(pred_sz - curData) + noise, fabsf(mean - curData));
							err_reg += fabsf(pred_reg - curData);
						}
						use_reg = (err_reg < err_sz);
					}
					if(use_reg){
						{
							/*predict coefficients in current block via previous reg_block*/
							float cur_coeff;
							float diff, itvNum;
							for(int e=0; e<4; e++){
								cur_coeff = reg_params_pos[e*num_blocks];
								diff = cur_coeff - last_coeffcients[e];
								itvNum = fabsf(diff)*recip_precision[e] + 1;
								if (itvNum < coeff_intvCapacity_sz){
									if (diff < 0) itvNum = -itvNum;
									coeff_type[e][coeff_index] = (int) (itvNum/2) + coeff_intvRadius;
									last_coeffcients[e] = last_coeffcients[e] + 2 * (coeff_type[e][coeff_index] - coeff_intvRadius) * precision[e];
									//ganrantee comporession error against the case of machine-epsilon
									if(fabsf(cur_coeff - last_coeffcients[e])>precision[e]){	
										coeff_type[e][coeff_index] = 0;
										last_coeffcients[e] = cur_coeff;	
										coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
									}					
								}
								else{
									coeff_type[e][coeff_index] = 0;
									last_coeffcients[e] = cur_coeff;
									coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
								}
							}
							coeff_index ++;
						}
						float curData;
						float pred;
						float itvNum;
						float diff;
						size_t index = 0;
						size_t block_unpredictable_count = 0;
						float * cur_data_pos = data_pos;
						for(size_t ii=0; ii<current_blockcount_x - 1; ii++){
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									curData = *cur_data_pos;
									pred = last_coeffcients[0] * ii + last_coeffcients[1] * jj + last_coeffcients[2] * kk + last_coeffcients[3];									
									diff = curData - pred;
									itvNum = fabsf(diff)*recip_realPrecision + 1;
									if (itvNum < intvCapacity){
										if (diff < 0) itvNum = -itvNum;
										type[index] = (int) (itvNum/2) + intvRadius;
										pred = pred + 2 * (type[index] - intvRadius) * tmp_realPrecision;
										//ganrantee comporession error against the case of machine-epsilon
										if(fabsf(curData - pred)>tmp_realPrecision){	
											type[index] = 0;
											pred = curData;
											unpredictable_data[block_unpredictable_count ++] = curData;
										}		
									}
									else{
										type[index] = 0;
										pred = curData;
										unpredictable_data[block_unpredictable_count ++] = curData;
									}
									
#ifdef HAVE_TIMECMPR
									size_t point_offset = ii*dim0_offset + jj*dim1_offset + kk;
									if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
										decData[block_offset + point_offset] = pred;
#endif									
									
									if((jj == current_blockcount_y - 1) || (kk == current_blockcount_z - 1)){
										// assign value to block surfaces
										pb_pos[ii * strip_dim0_offset + jj * strip_dim1_offset + kk] = pred;
									}
									index ++;	
									cur_data_pos ++;
								}
								cur_data_pos += dim1_offset - current_blockcount_z;
							}
							cur_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						/*dealing with the last ii (boundary)*/
						{
							// ii == current_blockcount_x - 1
							size_t ii = current_blockcount_x - 1;
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									curData = *cur_data_pos;
									pred = last_coeffcients[0] * ii + last_coeffcients[1] * jj + last_coeffcients[2] * kk + last_coeffcients[3];									
									diff = curData - pred;
									itvNum = fabsf(diff)*recip_realPrecision + 1;
									if (itvNum < intvCapacity){
										if (diff < 0) itvNum = -itvNum;
										type[index] = (int) (itvNum/2) + intvRadius;
										pred = pred + 2 * (type[index] - intvRadius) * realPrecision;
										//ganrantee comporession error against the case of machine-epsilon
										if(fabsf(curData - pred)>realPrecision){	
											type[index] = 0;
											pred = curData;
											unpredictable_data[block_unpredictable_count ++] = curData;
										}		
									}
									else{
										type[index] = 0;
										pred = curData;
										unpredictable_data[block_unpredictable_count ++] = curData;
									}

#ifdef HAVE_TIMECMPR
									size_t point_offset = ii*dim0_offset + jj*dim1_offset + kk;
									if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
										decData[block_offset + point_offset] = pred;
#endif									

									if((jj == current_blockcount_y - 1) || (kk == current_blockcount_z - 1)){
										// assign value to block surfaces
										pb_pos[ii * strip_dim0_offset + jj * strip_dim1_offset + kk] = pred;
									}
									// assign value to next prediction buffer
									next_pb_pos[jj * strip_dim1_offset + kk] = pred;
									index ++;
									cur_data_pos ++;
								}
								cur_data_pos += dim1_offset - current_blockcount_z;
							}
						}
						unpredictable_count = block_unpredictable_count;
						strip_unpredictable_count += unpredictable_count;
						unpredictable_data += unpredictable_count;
						
						reg_count ++;
					}
					else{
						// use SZ
						// SZ predication
						unpredictable_count = 0;
						float * cur_pb_pos = pb_pos;
						float * cur_data_pos = data_pos;
						float curData;
						float pred3D;
						float itvNum, diff;
						size_t index = 0;
						for(size_t ii=0; ii<current_blockcount_x - 1; ii++){
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){

									curData = *cur_data_pos;
									if(fabsf(curData - mean) <= realPrecision){
										// adjust type[index] to intvRadius for coherence with freq in reg
										type[index] = intvRadius;
										*cur_pb_pos = mean;
									}
									else
									{
										pred3D = cur_pb_pos[-1] + cur_pb_pos[-strip_dim1_offset]+ cur_pb_pos[-strip_dim0_offset] - cur_pb_pos[-strip_dim1_offset - 1]
												 - cur_pb_pos[-strip_dim0_offset - 1] - cur_pb_pos[-strip_dim0_offset - strip_dim1_offset] + cur_pb_pos[-strip_dim0_offset - strip_dim1_offset - 1];
										diff = curData - pred3D;
										itvNum = fabsf(diff)*recip_realPrecision + 1;
										if (itvNum < intvCapacity_sz){
											if (diff < 0) itvNum = -itvNum;
											type[index] = (int) (itvNum/2) + intvRadius;
											*cur_pb_pos = pred3D + 2 * (type[index] - intvRadius) * realPrecision;
											if(type[index] <= intvRadius) type[index] -= 1;
											//ganrantee comporession error against the case of machine-epsilon
											if(fabsf(curData - *cur_pb_pos)>realPrecision){	
												type[index] = 0;
												*cur_pb_pos = curData;	
												unpredictable_data[unpredictable_count ++] = curData;
											}					
										}
										else{
											type[index] = 0;
											*cur_pb_pos = curData;
											unpredictable_data[unpredictable_count ++] = curData;
										}
									}
#ifdef HAVE_TIMECMPR
									size_t point_offset = ii*dim0_offset + jj*dim1_offset + kk;
									if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
										decData[block_offset + point_offset] = *cur_pb_pos;
#endif																		
									
									index ++;
									cur_pb_pos ++;
									cur_data_pos ++;
								}
								cur_pb_pos += strip_dim1_offset - current_blockcount_z;
								cur_data_pos += dim1_offset - current_blockcount_z;
							}
							cur_pb_pos += strip_dim0_offset - current_blockcount_y * strip_dim1_offset;
							cur_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						/*dealing with the last ii (boundary)*/
						{
							// ii == current_blockcount_x - 1
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){

									curData = *cur_data_pos;
									if(fabsf(curData - mean) <= realPrecision){
										// adjust type[index] to intvRadius for coherence with freq in reg
										type[index] = intvRadius;
										*cur_pb_pos = mean;
									}
									else
									{
										pred3D = cur_pb_pos[-1] + cur_pb_pos[-strip_dim1_offset]+ cur_pb_pos[-strip_dim0_offset] - cur_pb_pos[-strip_dim1_offset - 1]
												 - cur_pb_pos[-strip_dim0_offset - 1] - cur_pb_pos[-strip_dim0_offset - strip_dim1_offset] + cur_pb_pos[-strip_dim0_offset - strip_dim1_offset - 1];
										diff = curData - pred3D;
										itvNum = fabsf(diff)*recip_realPrecision + 1;
										if (itvNum < intvCapacity_sz){
											if (diff < 0) itvNum = -itvNum;
											type[index] = (int) (itvNum/2) + intvRadius;
											*cur_pb_pos = pred3D + 2 * (type[index] - intvRadius) * realPrecision;
											if(type[index] <= intvRadius) type[index] -= 1;
											//ganrantee comporession error against the case of machine-epsilon
											if(fabsf(curData - *cur_pb_pos)>realPrecision){	
												type[index] = 0;
												*cur_pb_pos = curData;	
												unpredictable_data[unpredictable_count ++] = curData;
											}					
										}
										else{
											type[index] = 0;
											*cur_pb_pos = curData;
											unpredictable_data[unpredictable_count ++] = curData;
										}
									}
#ifdef HAVE_TIMECMPR
									size_t ii = current_blockcount_x - 1;
									size_t point_offset = ii*dim0_offset + jj*dim1_offset + kk;
									if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
										decData[block_offset + point_offset] = *cur_pb_pos;
#endif																		
									
									next_pb_pos[jj * strip_dim1_offset + kk] = *cur_pb_pos;
									index ++;
									cur_pb_pos ++;
									cur_data_pos ++;
								}
								cur_pb_pos += strip_dim1_offset - current_blockcount_z;
								cur_data_pos += dim1_offset - current_blockcount_z;
							}
						}
						strip_unpredictable_count += unpredictable_count;
						unpredictable_data += unpredictable_count;
						// change indicator
						indicator_pos[k] = 1;
					}// end SZ
					
					reg_params_pos ++;
					data_pos += current_blockcount_z;
					pb_pos += current_blockcount_z;
					next_pb_pos += current_blockcount_z;
					type += current_blockcount_x * current_blockcount_y * current_blockcount_z;

				} // end k

				if(strip_unpredictable_count > max_unpred_count){
					max_unpred_count = strip_unpredictable_count;
				}
				total_unpred += strip_unpredictable_count;
				indicator_pos += num_z;
			}// end j
			float * tmp;
			tmp = cur_pb_buf;
			cur_pb_buf = next_pb_buf;
			next_pb_buf = tmp;
		}// end i
	}
	else{
		int intvCapacity_sz = intvCapacity - 2;
		for(size_t i=0; i<num_x; i++){
			current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
			offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;

			for(size_t j=0; j<num_y; j++){
				offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
				current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
				data_pos = oriData + offset_x * dim0_offset + offset_y * dim1_offset;
				// copy bottom plane from plane buffer
				// memcpy(prediction_buffer, bottom_buffer + offset_y * strip_dim1_offset, (current_blockcount_y + 1) * strip_dim1_offset * sizeof(float));
				type_offset = offset_x * dim0_offset +  offset_y * current_blockcount_x * dim1_offset;
				type = result_type + type_offset;

				// prediction buffer is (current_block_count_x + 1) * (current_block_count_y + 1) * (current_block_count_z + 1)
				cur_pb_buf_pos = cur_pb_buf + offset_y * strip_dim1_offset + strip_dim0_offset + strip_dim1_offset + 1;
				next_pb_buf_pos = next_pb_buf + offset_y * strip_dim1_offset + strip_dim1_offset + 1;

				size_t current_blockcount_z;
				float * pb_pos = cur_pb_buf_pos;
				float * next_pb_pos = next_pb_buf_pos;
				size_t strip_unpredictable_count = 0;
				for(size_t k=0; k<num_z; k++){
					current_blockcount_z = (k < split_index_z) ? early_blockcount_z : late_blockcount_z;
#ifdef HAVE_TIMECMPR
					size_t offset_z = 0;
					offset_z = (k < split_index_z) ? k * early_blockcount_z : k * late_blockcount_z + split_index_z;
					size_t block_offset = offset_x * dim0_offset + offset_y * dim1_offset + offset_z;
#endif														
					/*sampling*/
					{
						// sample point [1, 1, 1] [1, 1, 4] [1, 4, 1] [1, 4, 4] [4, 1, 1] [4, 1, 4] [4, 4, 1] [4, 4, 4]
						float * cur_data_pos;
						float curData;
						float pred_reg, pred_sz;
						float err_sz = 0.0, err_reg = 0.0;
						int bmi = 0;
						int block_size = MIN(current_blockcount_x, (MIN(current_blockcount_y, current_blockcount_z)));
						for(int i=1; i<block_size; i++){
							cur_data_pos = data_pos + i*dim0_offset + i*dim1_offset + i;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-dim1_offset]+ cur_data_pos[-dim0_offset] - cur_data_pos[-dim1_offset - 1] - cur_data_pos[-dim0_offset - 1] - cur_data_pos[-dim0_offset - dim1_offset] + cur_data_pos[-dim0_offset - dim1_offset - 1];
							pred_reg = reg_params_pos[0] * i + reg_params_pos[params_offset_b] * i + reg_params_pos[params_offset_c] * i + reg_params_pos[params_offset_d];							
							err_sz += fabsf(pred_sz - curData) + noise;
							err_reg += fabsf(pred_reg - curData);

							bmi = block_size - i;
							cur_data_pos = data_pos + i*dim0_offset + i*dim1_offset + bmi;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-dim1_offset]+ cur_data_pos[-dim0_offset] - cur_data_pos[-dim1_offset - 1] - cur_data_pos[-dim0_offset - 1] - cur_data_pos[-dim0_offset - dim1_offset] + cur_data_pos[-dim0_offset - dim1_offset - 1];
							pred_reg = reg_params_pos[0] * i + reg_params_pos[params_offset_b] * i + reg_params_pos[params_offset_c] * bmi + reg_params_pos[params_offset_d];							
							err_sz += fabsf(pred_sz - curData) + noise;
							err_reg += fabsf(pred_reg - curData);								

							cur_data_pos = data_pos + i*dim0_offset + bmi*dim1_offset + i;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-dim1_offset]+ cur_data_pos[-dim0_offset] - cur_data_pos[-dim1_offset - 1] - cur_data_pos[-dim0_offset - 1] - cur_data_pos[-dim0_offset - dim1_offset] + cur_data_pos[-dim0_offset - dim1_offset - 1];
							pred_reg = reg_params_pos[0] * i + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c] * i + reg_params_pos[params_offset_d];							
							err_sz += fabsf(pred_sz - curData) + noise;
							err_reg += fabsf(pred_reg - curData);								

							cur_data_pos = data_pos + i*dim0_offset + bmi*dim1_offset + bmi;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-dim1_offset]+ cur_data_pos[-dim0_offset] - cur_data_pos[-dim1_offset - 1] - cur_data_pos[-dim0_offset - 1] - cur_data_pos[-dim0_offset - dim1_offset] + cur_data_pos[-dim0_offset - dim1_offset - 1];
							pred_reg = reg_params_pos[0] * i + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c] * bmi + reg_params_pos[params_offset_d];							
							err_sz += fabsf(pred_sz - curData) + noise;
							err_reg += fabsf(pred_reg - curData);
						}
						use_reg = (err_reg < err_sz);

					}
					if(use_reg)
					{
						{
							/*predict coefficients in current block via previous reg_block*/
							float cur_coeff;
							float diff, itvNum;
							for(int e=0; e<4; e++){
								cur_coeff = reg_params_pos[e*num_blocks];
								diff = cur_coeff - last_coeffcients[e];
								itvNum = fabsf(diff)/precision[e] + 1;
								if (itvNum < coeff_intvCapacity_sz){
									if (diff < 0) itvNum = -itvNum;
									coeff_type[e][coeff_index] = (int) (itvNum/2) + coeff_intvRadius;
									last_coeffcients[e] = last_coeffcients[e] + 2 * (coeff_type[e][coeff_index] - coeff_intvRadius) * precision[e];
									//ganrantee comporession error against the case of machine-epsilon
									if(fabsf(cur_coeff - last_coeffcients[e])>precision[e]){	
										coeff_type[e][coeff_index] = 0;
										last_coeffcients[e] = cur_coeff;	
										coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
									}					
								}
								else{
									coeff_type[e][coeff_index] = 0;
									last_coeffcients[e] = cur_coeff;
									coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
								}
							}
							coeff_index ++;
						}
						float curData;
						float pred;
						float itvNum;
						float diff;
						size_t index = 0;
						size_t block_unpredictable_count = 0;
						float * cur_data_pos = data_pos;
						for(size_t ii=0; ii<current_blockcount_x - 1; ii++){
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){

									curData = *cur_data_pos;
									pred = last_coeffcients[0] * ii + last_coeffcients[1] * jj + last_coeffcients[2] * kk + last_coeffcients[3];									
									diff = curData - pred;
									itvNum = fabsf(diff)*recip_realPrecision + 1;
									if (itvNum < intvCapacity){
										if (diff < 0) itvNum = -itvNum;
										type[index] = (int) (itvNum/2) + intvRadius;
										pred = pred + 2 * (type[index] - intvRadius) * realPrecision;
										//ganrantee comporession error against the case of machine-epsilon
										if(fabsf(curData - pred)>realPrecision){	
											type[index] = 0;
											pred = curData;
											unpredictable_data[block_unpredictable_count ++] = curData;
										}		
									}
									else{
										type[index] = 0;
										pred = curData;
										unpredictable_data[block_unpredictable_count ++] = curData;
									}

#ifdef HAVE_TIMECMPR
									size_t point_offset = ii*dim0_offset + jj*dim1_offset + kk;
									if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
										decData[block_offset + point_offset] = pred;
#endif																		


									if((jj == current_blockcount_y - 1) || (kk == current_blockcount_z - 1)){
										// assign value to block surfaces
										pb_pos[ii * strip_dim0_offset + jj * strip_dim1_offset + kk] = pred;
									}
									index ++;	
									cur_data_pos ++;
								}
								cur_data_pos += dim1_offset - current_blockcount_z;
							}
							cur_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						/*dealing with the last ii (boundary)*/
						{
							// ii == current_blockcount_x - 1
							size_t ii = current_blockcount_x - 1;
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									curData = *cur_data_pos;
									pred = last_coeffcients[0] * ii + last_coeffcients[1] * jj + last_coeffcients[2] * kk + last_coeffcients[3];									
									diff = curData - pred;
									itvNum = fabsf(diff)*recip_realPrecision + 1;
									if (itvNum < intvCapacity){
										if (diff < 0) itvNum = -itvNum;
										type[index] = (int) (itvNum/2) + intvRadius;
										pred = pred + 2 * (type[index] - intvRadius) * realPrecision;
										//ganrantee comporession error against the case of machine-epsilon
										if(fabsf(curData - pred)>realPrecision){	
											type[index] = 0;
											pred = curData;
											unpredictable_data[block_unpredictable_count ++] = curData;
										}		
									}
									else{
										type[index] = 0;
										pred = curData;
										unpredictable_data[block_unpredictable_count ++] = curData;
									}
									
#ifdef HAVE_TIMECMPR
									size_t point_offset = ii*dim0_offset + jj*dim1_offset + kk;
									if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
										decData[block_offset + point_offset] = pred;
#endif																											

									if((jj == current_blockcount_y - 1) || (kk == current_blockcount_z - 1)){
										// assign value to block surfaces
										pb_pos[ii * strip_dim0_offset + jj * strip_dim1_offset + kk] = pred;
									}
									// assign value to next prediction buffer
									next_pb_pos[jj * strip_dim1_offset + kk] = pred;
									index ++;
									cur_data_pos ++;
								}
								cur_data_pos += dim1_offset - current_blockcount_z;
							}
						}
						unpredictable_count = block_unpredictable_count;
						strip_unpredictable_count += unpredictable_count;
						unpredictable_data += unpredictable_count;						
						reg_count ++;
					}
					else{
						// use SZ
						// SZ predication
						unpredictable_count = 0;
						float * cur_pb_pos = pb_pos;
						float * cur_data_pos = data_pos;
						float curData;
						float pred3D;
						float itvNum, diff;
						size_t index = 0;
						for(size_t ii=0; ii<current_blockcount_x - 1; ii++){
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){

									curData = *cur_data_pos;
									pred3D = cur_pb_pos[-1] + cur_pb_pos[-strip_dim1_offset]+ cur_pb_pos[-strip_dim0_offset] - cur_pb_pos[-strip_dim1_offset - 1]
											 - cur_pb_pos[-strip_dim0_offset - 1] - cur_pb_pos[-strip_dim0_offset - strip_dim1_offset] + cur_pb_pos[-strip_dim0_offset - strip_dim1_offset - 1];
									diff = curData - pred3D;
									itvNum = fabsf(diff)*recip_realPrecision + 1;
									if (itvNum < intvCapacity_sz){
										if (diff < 0) itvNum = -itvNum;
										type[index] = (int) (itvNum/2) + intvRadius;
										*cur_pb_pos = pred3D + 2 * (type[index] - intvRadius) * realPrecision;
										//ganrantee comporession error against the case of machine-epsilon
										if(fabsf(curData - *cur_pb_pos)>realPrecision){	
											type[index] = 0;
											*cur_pb_pos = curData;	
											unpredictable_data[unpredictable_count ++] = curData;
										}					
									}
									else{
										type[index] = 0;
										*cur_pb_pos = curData;
										unpredictable_data[unpredictable_count ++] = curData;
									}
									
#ifdef HAVE_TIMECMPR
									size_t point_offset = ii*dim0_offset + jj*dim1_offset + kk;
									if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
										decData[block_offset + point_offset] = *cur_pb_pos;
#endif																											
									index ++;
									cur_pb_pos ++;
									cur_data_pos ++;
								}
								cur_pb_pos += strip_dim1_offset - current_blockcount_z;
								cur_data_pos += dim1_offset - current_blockcount_z;
							}
							cur_pb_pos += strip_dim0_offset - current_blockcount_y * strip_dim1_offset;
							cur_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						/*dealing with the last ii (boundary)*/
						{
							// ii == current_blockcount_x - 1
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){

									curData = *cur_data_pos;
									pred3D = cur_pb_pos[-1] + cur_pb_pos[-strip_dim1_offset]+ cur_pb_pos[-strip_dim0_offset] - cur_pb_pos[-strip_dim1_offset - 1]
											 - cur_pb_pos[-strip_dim0_offset - 1] - cur_pb_pos[-strip_dim0_offset - strip_dim1_offset] + cur_pb_pos[-strip_dim0_offset - strip_dim1_offset - 1];
									diff = curData - pred3D;
									itvNum = fabsf(diff)*recip_realPrecision + 1;
									if (itvNum < intvCapacity_sz){
										if (diff < 0) itvNum = -itvNum;
										type[index] = (int) (itvNum/2) + intvRadius;
										*cur_pb_pos = pred3D + 2 * (type[index] - intvRadius) * realPrecision;
										//ganrantee comporession error against the case of machine-epsilon
										if(fabsf(curData - *cur_pb_pos)>realPrecision){	
											type[index] = 0;
											*cur_pb_pos = curData;	
											unpredictable_data[unpredictable_count ++] = curData;
										}					
									}
									else{
										type[index] = 0;
										*cur_pb_pos = curData;
										unpredictable_data[unpredictable_count ++] = curData;
									}
									
#ifdef HAVE_TIMECMPR
									size_t ii = current_blockcount_x - 1;
									size_t point_offset = ii*dim0_offset + jj*dim1_offset + kk;
									if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
										decData[block_offset + point_offset] = *cur_pb_pos;
#endif																											
									
									// assign value to next prediction buffer
									next_pb_pos[jj * strip_dim1_offset + kk] = *cur_pb_pos;
									index ++;
									cur_pb_pos ++;
									cur_data_pos ++;
								}
								cur_pb_pos += strip_dim1_offset - current_blockcount_z;
								cur_data_pos += dim1_offset - current_blockcount_z;
							}
						}
						strip_unpredictable_count += unpredictable_count;
						unpredictable_data += unpredictable_count;
						// change indicator
						indicator_pos[k] = 1;
					}// end SZ
					
					reg_params_pos ++;
					data_pos += current_blockcount_z;
					pb_pos += current_blockcount_z;
					next_pb_pos += current_blockcount_z;
					type += current_blockcount_x * current_blockcount_y * current_blockcount_z;

				}

				if(strip_unpredictable_count > max_unpred_count){
					max_unpred_count = strip_unpredictable_count;
				}
				total_unpred += strip_unpredictable_count;
				indicator_pos += num_z;
			}
			float * tmp;
			tmp = cur_pb_buf;
			cur_pb_buf = next_pb_buf;
			next_pb_buf = tmp;
		}
	}

	free(prediction_buffer_1);
	free(prediction_buffer_2);

	int stateNum = 2*quantization_intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);

	size_t nodeCount = 0;
	init(huffmanTree, result_type, num_elements);
	size_t i = 0;
	for (i = 0; i < huffmanTree->stateNum; i++)
		if (huffmanTree->code[i]) nodeCount++; 
	nodeCount = nodeCount*2-1;

	unsigned char *treeBytes;
	unsigned int treeByteSize = convert_HuffTree_to_bytes_anyStates(huffmanTree, nodeCount, &treeBytes);

	unsigned int meta_data_offset = 3 + 1 + MetaDataByteLength;
	// total size 										metadata		  # elements     real precision		intervals	nodeCount		huffman 	 	block index 						unpredicatable count						mean 					 	unpred size 				elements
	unsigned char * result = (unsigned char *) calloc(meta_data_offset + exe_params->SZ_SIZE_TYPE + sizeof(float) + sizeof(int) + sizeof(int) + 5*treeByteSize + 4*num_blocks*sizeof(int) + num_blocks * sizeof(unsigned short) + num_blocks * sizeof(unsigned short) + num_blocks * sizeof(float) + total_unpred * sizeof(float) + num_elements * sizeof(int), 1);
	unsigned char * result_pos = result;
	initRandomAccessBytes(result_pos);
	
	result_pos += meta_data_offset;
	
	sizeToBytes(result_pos,num_elements); //SZ_SIZE_TYPE: 4 or 8
	result_pos += exe_params->SZ_SIZE_TYPE;

	intToBytes_bigEndian(result_pos, block_size);
	result_pos += sizeof(int);
	floatToBytes(result_pos, realPrecision);
	result_pos += sizeof(float);
	intToBytes_bigEndian(result_pos, quantization_intervals);
	result_pos += sizeof(int);
	intToBytes_bigEndian(result_pos, treeByteSize);
	result_pos += sizeof(int);
	intToBytes_bigEndian(result_pos, nodeCount);
	result_pos += sizeof(int);
	memcpy(result_pos, treeBytes, treeByteSize);
	result_pos += treeByteSize;
	free(treeBytes);

	memcpy(result_pos, &use_mean, sizeof(unsigned char));
	result_pos += sizeof(unsigned char);
	memcpy(result_pos, &mean, sizeof(float));
	result_pos += sizeof(float);
		
	size_t indicator_size = convertIntArray2ByteArray_fast_1b_to_result(indicator, num_blocks, result_pos);
	result_pos += indicator_size;
	
	//convert the lead/mid/resi to byte stream
	
	//printf("reg_count = %d, num_blocks = %d\n", reg_count, num_blocks);
	if(reg_count > 0){
		for(int e=0; e<4; e++){
			int stateNum = 2*coeff_intvCapacity_sz;
			HuffmanTree* huffmanTree = createHuffmanTree(stateNum);
			size_t nodeCount = 0;
			init(huffmanTree, coeff_type[e], reg_count);
			size_t i = 0;
			for (i = 0; i < huffmanTree->stateNum; i++)
				if (huffmanTree->code[i]) nodeCount++; 
			nodeCount = nodeCount*2-1;
			unsigned char *treeBytes;
			unsigned int treeByteSize = convert_HuffTree_to_bytes_anyStates(huffmanTree, nodeCount, &treeBytes);
			floatToBytes(result_pos, precision[e]);
			result_pos += sizeof(float);
			intToBytes_bigEndian(result_pos, coeff_intvRadius);
			result_pos += sizeof(int);
			intToBytes_bigEndian(result_pos, treeByteSize);
			result_pos += sizeof(int);
			intToBytes_bigEndian(result_pos, nodeCount);
			result_pos += sizeof(int);
			memcpy(result_pos, treeBytes, treeByteSize);		
			result_pos += treeByteSize;
			free(treeBytes);
			size_t typeArray_size = 0;
			encode(huffmanTree, coeff_type[e], reg_count, result_pos + sizeof(size_t), &typeArray_size);
			sizeToBytes(result_pos, typeArray_size);
			result_pos += sizeof(size_t) + typeArray_size;
			intToBytes_bigEndian(result_pos, coeff_unpredictable_count[e]);
			result_pos += sizeof(int);
			memcpy(result_pos, coeff_unpred_data[e], coeff_unpredictable_count[e]*sizeof(float));
			result_pos += coeff_unpredictable_count[e]*sizeof(float);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	free(coeff_result_type);
	free(coeff_unpredictable_data);
	
	//record the number of unpredictable data and also store them
	memcpy(result_pos, &total_unpred, sizeof(size_t));
	result_pos += sizeof(size_t);
	memcpy(result_pos, result_unpredictable_data, total_unpred * sizeof(float));
	result_pos += total_unpred * sizeof(float);
	size_t typeArray_size = 0;
	encode(huffmanTree, result_type, num_elements, result_pos, &typeArray_size);
	result_pos += typeArray_size;
	size_t totalEncodeSize = result_pos - result;
	free(indicator);
	free(result_unpredictable_data);
	free(result_type);
	free(reg_params);

#ifdef HAVE_WRITESTATS
	writeHuffmanInfo(treeByteSize, typeArray_size, num_elements*sizeof(float), nodeCount);
	writeBlockInfo(use_mean, block_size, reg_count, num_blocks);
	writeUnpredictDataCounts(total_unpred, num_elements);
#endif	
	
	SZ_ReleaseHuffman(huffmanTree);
	*comp_size = totalEncodeSize;
	return result;
}


unsigned char * SZ_compress_float_3D_MDQ_random_access_with_blocked_regression(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t * comp_size){

	unsigned int quantization_intervals;
	float sz_sample_correct_freq = -1;//0.5; //-1
	float dense_pos;
	float mean_flush_freq;
	unsigned char use_mean = 0;

	// calculate block dims
	size_t num_x, num_y, num_z;
	size_t block_size = 6;
	num_x = (r1 - 1) / block_size + 1;
	num_y = (r2 - 1) / block_size + 1;
	num_z = (r3 - 1) / block_size + 1;

	size_t max_num_block_elements = block_size * block_size * block_size;
	size_t num_blocks = num_x * num_y * num_z;
	size_t num_elements = r1 * r2 * r3;

	size_t dim0_offset = r2 * r3;
	size_t dim1_offset = r3;	

	int * result_type = (int *) malloc(num_blocks*max_num_block_elements * sizeof(int));
	size_t unpred_data_max_size = max_num_block_elements;
	float * result_unpredictable_data = (float *) malloc(unpred_data_max_size * sizeof(float) * num_blocks);
	size_t total_unpred = 0;
	size_t unpredictable_count;
	float * data_pos = oriData;
	int * type = result_type;
	float * reg_params = (float *) malloc(num_blocks * 4 * sizeof(float));
	float * reg_params_pos = reg_params;
	// move regression part out
	size_t params_offset_b = num_blocks;
	size_t params_offset_c = 2*num_blocks;
	size_t params_offset_d = 3*num_blocks;
	float * pred_buffer = (float *) malloc((block_size+1)*(block_size+1)*(block_size+1)*sizeof(float));
	float * pred_buffer_pos = NULL;
	float * block_data_pos_x = NULL;
	float * block_data_pos_y = NULL;
	float * block_data_pos_z = NULL;
	for(size_t i=0; i<num_x; i++){
		for(size_t j=0; j<num_y; j++){
			for(size_t k=0; k<num_z; k++){
				data_pos = oriData + i*block_size * dim0_offset + j*block_size * dim1_offset + k*block_size;
				pred_buffer_pos = pred_buffer;
				block_data_pos_x = data_pos;
				// use the buffer as block_size*block_size*block_size
				for(int ii=0; ii<block_size; ii++){
					block_data_pos_y = block_data_pos_x;
					for(int jj=0; jj<block_size; jj++){
						block_data_pos_z = block_data_pos_y;
						for(int kk=0; kk<block_size; kk++){
							*pred_buffer_pos = *block_data_pos_z;
							if(k*block_size + kk + 1 < r3) block_data_pos_z ++;
							pred_buffer_pos ++;
						}
						if(j*block_size + jj + 1 < r2) block_data_pos_y += dim1_offset;
					}
					if(i*block_size + ii + 1 < r1) block_data_pos_x += dim0_offset;
				}
				/*Calculate regression coefficients*/
				{
					float * cur_data_pos = pred_buffer;
					float fx = 0.0;
					float fy = 0.0;
					float fz = 0.0;
					float f = 0;
					float sum_x, sum_y; 
					float curData;
					for(size_t i=0; i<block_size; i++){
						sum_x = 0;
						for(size_t j=0; j<block_size; j++){
							sum_y = 0;
							for(size_t k=0; k<block_size; k++){
								curData = *cur_data_pos;
								sum_y += curData;
								fz += curData * k;
								cur_data_pos ++;
							}
							fy += sum_y * j;
							sum_x += sum_y;
						}
						fx += sum_x * i;
						f += sum_x;
					}
					float coeff = 1.0 / (block_size * block_size * block_size);
					reg_params_pos[0] = (2 * fx / (block_size - 1) - f) * 6 * coeff / (block_size + 1);
					reg_params_pos[params_offset_b] = (2 * fy / (block_size - 1) - f) * 6 * coeff / (block_size + 1);
					reg_params_pos[params_offset_c] = (2 * fz / (block_size - 1) - f) * 6 * coeff / (block_size + 1);
					reg_params_pos[params_offset_d] = f * coeff - ((block_size - 1) * reg_params_pos[0] / 2 + (block_size - 1) * reg_params_pos[params_offset_b] / 2 + (block_size - 1) * reg_params_pos[params_offset_c] / 2);
				}
				reg_params_pos ++;
			}
		}
	}
	
	//Compress coefficient arrays
	double precision_a, precision_b, precision_c, precision_d;
	float rel_param_err = 0.025;
	precision_a = rel_param_err * realPrecision / block_size;
	precision_b = rel_param_err * realPrecision / block_size;
	precision_c = rel_param_err * realPrecision / block_size;
	precision_d = rel_param_err * realPrecision;

	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_3D_with_freq_and_dense_pos(oriData, r1, r2, r3, realPrecision, &dense_pos, &sz_sample_correct_freq, &mean_flush_freq);
		if(mean_flush_freq > 0.5 || mean_flush_freq > sz_sample_correct_freq) use_mean = 1;
		updateQuantizationInfo(quantization_intervals);
	}	
	else{
		quantization_intervals = exe_params->intvCapacity;
	}

	float mean = 0;
	if(use_mean){
		// compute mean
		double sum = 0.0;
		size_t mean_count = 0;
		for(size_t i=0; i<num_elements; i++){
			if(fabs(oriData[i] - dense_pos) < realPrecision){
				sum += oriData[i];
				mean_count ++;
			}
		}
		if(mean_count > 0) mean = sum / mean_count;
	}

	double tmp_realPrecision = realPrecision;

	// use two prediction buffers for higher performance
	float * unpredictable_data = result_unpredictable_data;
	unsigned char * indicator = (unsigned char *) malloc(num_blocks * sizeof(unsigned char));
	memset(indicator, 0, num_blocks * sizeof(unsigned char));
	size_t reg_count = 0;
	unsigned char * indicator_pos = indicator;

	int intvCapacity = quantization_intervals; //exe_params->intvCapacity;
	int intvRadius = intvCapacity/2; //exe_params->intvRadius;	
	int use_reg = 0;
	float noise = realPrecision * 1.22;

	reg_params_pos = reg_params;
	// compress the regression coefficients on the fly
	float last_coeffcients[4] = {0.0};
	int coeff_intvCapacity_sz = 65536;
	int coeff_intvRadius = coeff_intvCapacity_sz / 2;
	int * coeff_type[4];
	int * coeff_result_type = (int *) malloc(num_blocks*4*sizeof(int));
	float * coeff_unpred_data[4];
	float * coeff_unpredictable_data = (float *) malloc(num_blocks*4*sizeof(float));
	double precision[4];
	precision[0] = precision_a, precision[1] = precision_b, precision[2] = precision_c, precision[3] = precision_d;
	for(int i=0; i<4; i++){
		coeff_type[i] = coeff_result_type + i * num_blocks;
		coeff_unpred_data[i] = coeff_unpredictable_data + i * num_blocks;
	}
	int coeff_index = 0;
	unsigned int coeff_unpredictable_count[4] = {0};

	memset(pred_buffer, 0, (block_size+1)*(block_size+1)*(block_size+1)*sizeof(float));
	int pred_buffer_block_size = block_size + 1;
	int strip_dim0_offset = pred_buffer_block_size * pred_buffer_block_size;
	int strip_dim1_offset = pred_buffer_block_size;

	if(use_mean){
		int intvCapacity_sz = intvCapacity - 2;
		type = result_type;
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				for(size_t k=0; k<num_z; k++){
					data_pos = oriData + i*block_size * dim0_offset + j*block_size * dim1_offset + k*block_size;
					// add 1 in x, y, z offset
					pred_buffer_pos = pred_buffer + pred_buffer_block_size*pred_buffer_block_size + pred_buffer_block_size + 1;
					block_data_pos_x = data_pos;
					for(int ii=0; ii<block_size; ii++){
						block_data_pos_y = block_data_pos_x;
						for(int jj=0; jj<block_size; jj++){
							block_data_pos_z = block_data_pos_y;
							for(int kk=0; kk<block_size; kk++){
								*pred_buffer_pos = *block_data_pos_z;
								if(k*block_size + kk + 1< r3) block_data_pos_z ++;
								pred_buffer_pos ++;
							}
							// add 1 in z offset
							pred_buffer_pos ++;
							if(j*block_size + jj + 1< r2) block_data_pos_y += dim1_offset;
						}
						// add 1 in y offset
						pred_buffer_pos += pred_buffer_block_size;
						if(i*block_size + ii + 1< r1) block_data_pos_x += dim0_offset;
					}
					/*sampling and decide which predictor*/
					{
						// sample point [1, 1, 1] [1, 1, 4] [1, 4, 1] [1, 4, 4] [4, 1, 1] [4, 1, 4] [4, 4, 1] [4, 4, 4]
						float * cur_data_pos;
						float curData;
						float pred_reg, pred_sz;
						float err_sz = 0.0, err_reg = 0.0;
						int bmi = 0;
						for(int i=2; i<=block_size; i++){
							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + i*pred_buffer_block_size + i;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * (i-1) + reg_params_pos[params_offset_c] * (i-1) + reg_params_pos[params_offset_d];							
							err_sz += MIN(fabs(pred_sz - curData) + noise, fabs(mean - curData));
							err_reg += fabs(pred_reg - curData);

							bmi = block_size - i + 1;
							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + i*pred_buffer_block_size + bmi;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * (i-1) + reg_params_pos[params_offset_c] * bmi + reg_params_pos[params_offset_d];							
							err_sz += MIN(fabs(pred_sz - curData) + noise, fabs(mean - curData));
							err_reg += fabs(pred_reg - curData);								

							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + bmi*pred_buffer_block_size + i;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c] * (i-1) + reg_params_pos[params_offset_d];							
							err_sz += MIN(fabs(pred_sz - curData) + noise, fabs(mean - curData));
							err_reg += fabs(pred_reg - curData);								

							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + bmi*pred_buffer_block_size + bmi;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c] * bmi + reg_params_pos[params_offset_d];							
							err_sz += MIN(fabs(pred_sz - curData) + noise, fabs(mean - curData));
							err_reg += fabs(pred_reg - curData);
						}
						
						use_reg = (err_reg < err_sz);
					}
					if(use_reg){
						{
							/*predict coefficients in current block via previous reg_block*/
							float cur_coeff;
							double diff, itvNum;
							for(int e=0; e<4; e++){
								cur_coeff = reg_params_pos[e*num_blocks];
								diff = cur_coeff - last_coeffcients[e];
								itvNum = fabs(diff)/precision[e] + 1;
								if (itvNum < coeff_intvCapacity_sz){
									if (diff < 0) itvNum = -itvNum;
									coeff_type[e][coeff_index] = (int) (itvNum/2) + coeff_intvRadius;
									last_coeffcients[e] = last_coeffcients[e] + 2 * (coeff_type[e][coeff_index] - coeff_intvRadius) * precision[e];
									//ganrantee comporession error against the case of machine-epsilon
									if(fabs(cur_coeff - last_coeffcients[e])>precision[e]){	
										coeff_type[e][coeff_index] = 0;
										last_coeffcients[e] = cur_coeff;	
										coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
									}					
								}
								else{
									coeff_type[e][coeff_index] = 0;
									last_coeffcients[e] = cur_coeff;
									coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
								}
							}
							coeff_index ++;
						}
						float curData;
						float pred;
						double itvNum;
						double diff;
						size_t index = 0;
						size_t block_unpredictable_count = 0;
						float * cur_data_pos = pred_buffer + pred_buffer_block_size*pred_buffer_block_size + pred_buffer_block_size + 1;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								for(size_t kk=0; kk<block_size; kk++){
									curData = *cur_data_pos;
									pred = last_coeffcients[0] * ii + last_coeffcients[1] * jj + last_coeffcients[2] * kk + last_coeffcients[3];									
									diff = curData - pred;
									itvNum = fabs(diff)/tmp_realPrecision + 1;
									if (itvNum < intvCapacity){
										if (diff < 0) itvNum = -itvNum;
										type[index] = (int) (itvNum/2) + intvRadius;
										pred = pred + 2 * (type[index] - intvRadius) * tmp_realPrecision;
										//ganrantee comporession error against the case of machine-epsilon
										if(fabs(curData - pred)>tmp_realPrecision){	
											type[index] = 0;
											pred = curData;
											unpredictable_data[block_unpredictable_count ++] = curData;
										}		
									}
									else{
										type[index] = 0;
										pred = curData;
										unpredictable_data[block_unpredictable_count ++] = curData;
									}
									index ++;	
									cur_data_pos ++;
								}
								cur_data_pos ++;
							}
							cur_data_pos += pred_buffer_block_size;
						}
						
						total_unpred += block_unpredictable_count;
						unpredictable_data += block_unpredictable_count;						
						reg_count ++;
					}
					else{
						// use SZ
						// SZ predication
						unpredictable_count = 0;
						float * cur_data_pos = pred_buffer + pred_buffer_block_size*pred_buffer_block_size + pred_buffer_block_size + 1;
						float curData;
						float pred3D;
						double itvNum, diff;
						size_t index = 0;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								for(size_t kk=0; kk<block_size; kk++){

									curData = *cur_data_pos;
									if(fabs(curData - mean) <= realPrecision){
										type[index] = 1;
										*cur_data_pos = mean;
									}
									else
									{
										pred3D = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1]
												 - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
										diff = curData - pred3D;
										itvNum = fabs(diff)/realPrecision + 1;
										if (itvNum < intvCapacity_sz){
											if (diff < 0) itvNum = -itvNum;
											type[index] = (int) (itvNum/2) + intvRadius;
											*cur_data_pos = pred3D + 2 * (type[index] - intvRadius) * tmp_realPrecision;
											//ganrantee comporession error against the case of machine-epsilon
											if(fabs(curData - *cur_data_pos)>tmp_realPrecision){	
												type[index] = 0;
												*cur_data_pos = curData;	
												unpredictable_data[unpredictable_count ++] = curData;
											}					
										}
										else{
											type[index] = 0;
											*cur_data_pos = curData;
											unpredictable_data[unpredictable_count ++] = curData;
										}
									}
									index ++;
									cur_data_pos ++;
								}
								cur_data_pos ++;
							}
							cur_data_pos += pred_buffer_block_size;
						}
						total_unpred += unpredictable_count;
						unpredictable_data += unpredictable_count;
						// change indicator
						indicator_pos[k] = 1;
					}// end SZ
					reg_params_pos ++;
					type += block_size * block_size * block_size;
				} // end k
				indicator_pos += num_z;
			}// end j
		}// end i
	}
	else{
		int intvCapacity_sz = intvCapacity - 2;
		type = result_type;
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				for(size_t k=0; k<num_z; k++){
					data_pos = oriData + i*block_size * dim0_offset + j*block_size * dim1_offset + k*block_size;
					// add 1 in x, y, z offset
					pred_buffer_pos = pred_buffer + pred_buffer_block_size*pred_buffer_block_size + pred_buffer_block_size + 1;
					block_data_pos_x = data_pos;
					for(int ii=0; ii<block_size; ii++){
						block_data_pos_y = block_data_pos_x;
						for(int jj=0; jj<block_size; jj++){
							block_data_pos_z = block_data_pos_y;
							for(int kk=0; kk<block_size; kk++){
								*pred_buffer_pos = *block_data_pos_z;
								if(k*block_size + kk < r3) block_data_pos_z ++;
								pred_buffer_pos ++;
							}
							// add 1 in z offset
							pred_buffer_pos ++;
							if(j*block_size + jj < r2) block_data_pos_y += dim1_offset;
						}
						// add 1 in y offset
						pred_buffer_pos += pred_buffer_block_size;
						if(i*block_size + ii < r1) block_data_pos_x += dim0_offset;
					}
					/*sampling*/
					{
						// sample point [1, 1, 1] [1, 1, 4] [1, 4, 1] [1, 4, 4] [4, 1, 1] [4, 1, 4] [4, 4, 1] [4, 4, 4]
						float * cur_data_pos;
						float curData;
						float pred_reg, pred_sz;
						float err_sz = 0.0, err_reg = 0.0;
						int bmi;
						for(int i=2; i<=block_size; i++){
							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + i*pred_buffer_block_size + i;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * (i-1) + reg_params_pos[params_offset_c] * (i-1) + reg_params_pos[params_offset_d];							
							err_sz += fabs(pred_sz - curData) + noise;
							err_reg += fabs(pred_reg - curData);

							bmi = block_size - i + 1;
							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + i*pred_buffer_block_size + bmi;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * (i-1) + reg_params_pos[params_offset_c] * bmi + reg_params_pos[params_offset_d];							
							err_sz += fabs(pred_sz - curData) + noise;
							err_reg += fabs(pred_reg - curData);								

							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + bmi*pred_buffer_block_size + i;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c] * (i-1) + reg_params_pos[params_offset_d];							
							err_sz += fabs(pred_sz - curData) + noise;
							err_reg += fabs(pred_reg - curData);								

							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + bmi*pred_buffer_block_size + bmi;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c] * bmi + reg_params_pos[params_offset_d];							
							err_sz += fabs(pred_sz - curData) + noise;
							err_reg += fabs(pred_reg - curData);
						}
						
						use_reg = (err_reg < err_sz);

					}
					if(use_reg)
					{
						{
							/*predict coefficients in current block via previous reg_block*/
							float cur_coeff;
							double diff, itvNum;
							for(int e=0; e<4; e++){
								cur_coeff = reg_params_pos[e*num_blocks];
								diff = cur_coeff - last_coeffcients[e];
								itvNum = fabs(diff)/precision[e] + 1;
								if (itvNum < coeff_intvCapacity_sz){
									if (diff < 0) itvNum = -itvNum;
									coeff_type[e][coeff_index] = (int) (itvNum/2) + coeff_intvRadius;
									last_coeffcients[e] = last_coeffcients[e] + 2 * (coeff_type[e][coeff_index] - coeff_intvRadius) * precision[e];
									//ganrantee comporession error against the case of machine-epsilon
									if(fabs(cur_coeff - last_coeffcients[e])>precision[e]){	
										coeff_type[e][coeff_index] = 0;
										last_coeffcients[e] = cur_coeff;	
										coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
									}					
								}
								else{
									coeff_type[e][coeff_index] = 0;
									last_coeffcients[e] = cur_coeff;
									coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
								}
							}
							coeff_index ++;
						}
						float curData;
						float pred;
						double itvNum;
						double diff;
						size_t index = 0;
						size_t block_unpredictable_count = 0;
						float * cur_data_pos = pred_buffer + pred_buffer_block_size*pred_buffer_block_size + pred_buffer_block_size + 1;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								for(size_t kk=0; kk<block_size; kk++){
									curData = *cur_data_pos;
									pred = last_coeffcients[0] * ii + last_coeffcients[1] * jj + last_coeffcients[2] * kk + last_coeffcients[3];									
									diff = curData - pred;
									itvNum = fabs(diff)/tmp_realPrecision + 1;
									if (itvNum < intvCapacity){
										if (diff < 0) itvNum = -itvNum;
										type[index] = (int) (itvNum/2) + intvRadius;
										pred = pred + 2 * (type[index] - intvRadius) * tmp_realPrecision;
										//ganrantee comporession error against the case of machine-epsilon
										if(fabs(curData - pred)>tmp_realPrecision){	
											type[index] = 0;
											pred = curData;
											unpredictable_data[block_unpredictable_count ++] = curData;
										}		
									}
									else{
										type[index] = 0;
										pred = curData;
										unpredictable_data[block_unpredictable_count ++] = curData;
									}
									index ++;	
									cur_data_pos ++;
								}
								cur_data_pos ++;
							}
							cur_data_pos += pred_buffer_block_size;
						}
						total_unpred += block_unpredictable_count;
						unpredictable_data += block_unpredictable_count;						
						reg_count ++;
					}
					else{
						// use SZ
						// SZ predication
						unpredictable_count = 0;
						float * cur_data_pos = pred_buffer + pred_buffer_block_size*pred_buffer_block_size + pred_buffer_block_size + 1;
						float curData;
						float pred3D;
						double itvNum, diff;
						size_t index = 0;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								for(size_t kk=0; kk<block_size; kk++){
									curData = *cur_data_pos;
									pred3D = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1]
											 - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
									diff = curData - pred3D;
									itvNum = fabs(diff)/realPrecision + 1;
									if (itvNum < intvCapacity_sz){
										if (diff < 0) itvNum = -itvNum;
										type[index] = (int) (itvNum/2) + intvRadius;
										*cur_data_pos = pred3D + 2 * (type[index] - intvRadius) * tmp_realPrecision;
										//ganrantee comporession error against the case of machine-epsilon
										if(fabs(curData - *cur_data_pos)>tmp_realPrecision){	
											type[index] = 0;
											*cur_data_pos = curData;	
											unpredictable_data[unpredictable_count ++] = curData;
										}					
									}
									else{
										type[index] = 0;
										*cur_data_pos = curData;
										unpredictable_data[unpredictable_count ++] = curData;
									}
									index ++;
									cur_data_pos ++;
								}
								cur_data_pos ++;
							}
							cur_data_pos += pred_buffer_block_size;
						}
						total_unpred += unpredictable_count;
						unpredictable_data += unpredictable_count;
						// change indicator
						indicator_pos[k] = 1;
					}// end SZ					
					reg_params_pos ++;
					type += block_size * block_size * block_size;
				}
				indicator_pos += num_z;
			}
		}
	}
	free(pred_buffer);
	int stateNum = 2*quantization_intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);

	size_t nodeCount = 0;
	init(huffmanTree, result_type, num_blocks*max_num_block_elements);
	size_t i = 0;
	for (i = 0; i < huffmanTree->stateNum; i++)
		if (huffmanTree->code[i]) nodeCount++; 
	nodeCount = nodeCount*2-1;

	unsigned char *treeBytes;
	unsigned int treeByteSize = convert_HuffTree_to_bytes_anyStates(huffmanTree, nodeCount, &treeBytes);

	unsigned int meta_data_offset = 3 + 1 + MetaDataByteLength;
	// total size 										metadata		  # elements     real precision		intervals	nodeCount		huffman 	 	block index 						unpredicatable count						mean 					 	unpred size 				elements
	unsigned char * result = (unsigned char *) calloc(meta_data_offset + exe_params->SZ_SIZE_TYPE + sizeof(double) + sizeof(int) + sizeof(int) + 5*treeByteSize + 4*num_blocks*sizeof(int)+ num_blocks * sizeof(unsigned short) + num_blocks * sizeof(unsigned short) + num_blocks * sizeof(float) + total_unpred * sizeof(float) + num_elements * sizeof(int), 1);
	unsigned char * result_pos = result;
	initRandomAccessBytes(result_pos);
	
	result_pos += meta_data_offset;
	
	sizeToBytes(result_pos,num_elements); //SZ_SIZE_TYPE: 4 or 8
	result_pos += exe_params->SZ_SIZE_TYPE;

	intToBytes_bigEndian(result_pos, block_size);
	result_pos += sizeof(int);
	doubleToBytes(result_pos, realPrecision);
	result_pos += sizeof(double);
	intToBytes_bigEndian(result_pos, quantization_intervals);
	result_pos += sizeof(int);
	intToBytes_bigEndian(result_pos, treeByteSize);
	result_pos += sizeof(int);
	intToBytes_bigEndian(result_pos, nodeCount);
	result_pos += sizeof(int);
	memcpy(result_pos, treeBytes, treeByteSize);
	result_pos += treeByteSize;
	free(treeBytes);

	memcpy(result_pos, &use_mean, sizeof(unsigned char));
	result_pos += sizeof(unsigned char);
	memcpy(result_pos, &mean, sizeof(float));
	result_pos += sizeof(float);
	size_t indicator_size = convertIntArray2ByteArray_fast_1b_to_result(indicator, num_blocks, result_pos);
	result_pos += indicator_size;
	
	//convert the lead/mid/resi to byte stream
	if(reg_count > 0){
		for(int e=0; e<4; e++){
			int stateNum = 2*coeff_intvCapacity_sz;
			HuffmanTree* huffmanTree = createHuffmanTree(stateNum);
			size_t nodeCount = 0;
			init(huffmanTree, coeff_type[e], reg_count);
			size_t i = 0;
			for (i = 0; i < huffmanTree->stateNum; i++)
				if (huffmanTree->code[i]) nodeCount++; 
			nodeCount = nodeCount*2-1;
			unsigned char *treeBytes;
			unsigned int treeByteSize = convert_HuffTree_to_bytes_anyStates(huffmanTree, nodeCount, &treeBytes);
			doubleToBytes(result_pos, precision[e]);
			result_pos += sizeof(double);
			intToBytes_bigEndian(result_pos, coeff_intvRadius);
			result_pos += sizeof(int);
			intToBytes_bigEndian(result_pos, treeByteSize);
			result_pos += sizeof(int);
			intToBytes_bigEndian(result_pos, nodeCount);
			result_pos += sizeof(int);
			memcpy(result_pos, treeBytes, treeByteSize);		
			result_pos += treeByteSize;
			free(treeBytes);
			size_t typeArray_size = 0;
			encode(huffmanTree, coeff_type[e], reg_count, result_pos + sizeof(size_t), &typeArray_size);
			sizeToBytes(result_pos, typeArray_size);
			result_pos += sizeof(size_t) + typeArray_size;
			intToBytes_bigEndian(result_pos, coeff_unpredictable_count[e]);
			result_pos += sizeof(int);
			memcpy(result_pos, coeff_unpred_data[e], coeff_unpredictable_count[e]*sizeof(float));
			result_pos += coeff_unpredictable_count[e]*sizeof(float);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	free(coeff_result_type);
	free(coeff_unpredictable_data);
	
	//record the number of unpredictable data and also store them
	memcpy(result_pos, &total_unpred, sizeof(size_t));
	result_pos += sizeof(size_t);
	memcpy(result_pos, result_unpredictable_data, total_unpred * sizeof(float));
	result_pos += total_unpred * sizeof(float);
	size_t typeArray_size = 0;
	encode(huffmanTree, result_type, num_blocks*max_num_block_elements, result_pos, &typeArray_size);
	result_pos += typeArray_size;
	size_t totalEncodeSize = result_pos - result;
	free(indicator);
	free(result_unpredictable_data);
	free(result_type);
	free(reg_params);

	
	SZ_ReleaseHuffman(huffmanTree);
	*comp_size = totalEncodeSize;
	return result;
}

// random access
unsigned char * SZ_compress_float_1D_MDQ_decompression_random_access_with_blocked_regression(float *oriData, size_t r1, double realPrecision, size_t * comp_size){

	unsigned int quantization_intervals;
	float sz_sample_correct_freq = -1;//0.5; //-1
	float dense_pos;
	float mean_flush_freq;
	unsigned char use_mean = 0;

	// calculate block dims
	size_t num_x;
	size_t block_size = 256;
	num_x = (r1 - 1) / block_size + 1;

	size_t max_num_block_elements = block_size;
	size_t num_blocks = num_x;
	size_t num_elements = r1;

	int * result_type = (int *) malloc(num_blocks*max_num_block_elements * sizeof(int));
	size_t unpred_data_max_size = max_num_block_elements;
	float * result_unpredictable_data = (float *) malloc(unpred_data_max_size * sizeof(float) * num_blocks);
	size_t total_unpred = 0;
	size_t unpredictable_count;
	float * data_pos = oriData;
	int * type = result_type;
	float * reg_params = (float *) malloc(num_blocks * 2 * sizeof(float));
	float * reg_params_pos = reg_params;
	// move regression part out
	size_t params_offset_b = num_blocks;
	float * pred_buffer = (float *) malloc((block_size+1)*sizeof(float));
	float * pred_buffer_pos = NULL;
	float * block_data_pos_x = NULL;
	for(size_t i=0; i<num_x; i++){
		data_pos = oriData + i*block_size;
		pred_buffer_pos = pred_buffer;
		block_data_pos_x = data_pos;
		// use the buffer as block_size
		for(int ii=0; ii<block_size; ii++){
			*pred_buffer_pos = *block_data_pos_x;
			pred_buffer_pos ++;
			if(i*block_size + ii + 1 < r1) block_data_pos_x ++;
		}
		/*Calculate regression coefficients*/
		{
			float * cur_data_pos = pred_buffer;
			float fx = 0.0;
			float f = 0;
			float curData;
			for(size_t i=0; i<block_size; i++){
				curData = *cur_data_pos;
				fx += curData * i;
				f += curData;
				cur_data_pos ++;
			}
			float coeff = 1.0 / block_size;
			reg_params_pos[0] = (2 * fx / (block_size - 1) - f) * 6 * coeff / (block_size + 1);
			reg_params_pos[params_offset_b] = f * coeff - (block_size - 1) * reg_params_pos[0] / 2;
		}
		reg_params_pos ++;
	}
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_1D_with_freq_and_dense_pos(oriData, r1, realPrecision, &dense_pos, &sz_sample_correct_freq, &mean_flush_freq);
		if(mean_flush_freq > 0.5 || mean_flush_freq > sz_sample_correct_freq) use_mean = 1;
		updateQuantizationInfo(quantization_intervals);
	}	
	else{
		quantization_intervals = exe_params->intvCapacity;
	}

	float mean = 0;
	if(use_mean){
		// compute mean
		double sum = 0.0;
		size_t mean_count = 0;
		for(size_t i=0; i<num_elements; i++){
			if(fabs(oriData[i] - dense_pos) < realPrecision){
				sum += oriData[i];
				mean_count ++;
			}
		}
		if(mean_count > 0) mean = sum / mean_count;
	}

	double tmp_realPrecision = realPrecision;

	// use two prediction buffers for higher performance
	float * unpredictable_data = result_unpredictable_data;
	unsigned char * indicator = (unsigned char *) malloc(num_blocks * sizeof(unsigned char));
	memset(indicator, 0, num_blocks * sizeof(unsigned char));
	unsigned char * indicator_pos = indicator;

	int intvCapacity = quantization_intervals; //exe_params->intvCapacity;
	int intvRadius = intvCapacity/2; //exe_params->intvRadius;	
	float noise = realPrecision * 0.5;
	reg_params_pos = reg_params;

	memset(pred_buffer, 0, (block_size+1)*sizeof(float));
	// select
	int sample_distance = sqrt(block_size) + 1;
	if(use_mean){
		for(size_t i=0; i<num_x; i++){
			data_pos = oriData + i*block_size;
			// add 1 in x, y offset
			pred_buffer_pos = pred_buffer + 1;
			block_data_pos_x = data_pos;
			for(int ii=0; ii<block_size; ii++){
				*pred_buffer_pos = *block_data_pos_x;
				pred_buffer_pos ++;
				if(i*block_size + ii + 1< r1) block_data_pos_x ++;
			}
			/*sampling and decide which predictor*/
			{
				float * cur_data_pos;
				float curData;
				float pred_reg, pred_sz;
				float err_sz = 0.0, err_reg = 0.0;
				for(int i=2; i<=block_size; i+=sample_distance){
					cur_data_pos = pred_buffer + i;
					curData = *cur_data_pos;
					pred_sz = cur_data_pos[-1];
					pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b];							
					err_sz += MIN(fabs(pred_sz - curData) + noise, fabs(mean - curData));
					err_reg += fabs(pred_reg - curData);								
				}
				*indicator_pos = !(err_reg < err_sz);
			}
			reg_params_pos ++;
			indicator_pos ++;
		}// end i
	}
	else{
		for(size_t i=0; i<num_x; i++){
			data_pos = oriData + i*block_size;
			// add 1 in x, y offset
			pred_buffer_pos = pred_buffer + 1;
			block_data_pos_x = data_pos;
			for(int ii=0; ii<block_size; ii++){
				*pred_buffer_pos = *block_data_pos_x;
				pred_buffer_pos ++;
				if(i*block_size + ii + 1< r1) block_data_pos_x ++;
			}
			/*sampling and decide which predictor*/
			{
				float * cur_data_pos;
				float curData;
				float pred_reg, pred_sz;
				float err_sz = 0.0, err_reg = 0.0;
				for(int i=2; i<=block_size; i+=sample_distance){
					cur_data_pos = pred_buffer + i;
					curData = *cur_data_pos;
					pred_sz = cur_data_pos[-1];
					pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b];							
					err_sz += fabs(pred_sz - curData) + noise;
					err_reg += fabs(pred_reg - curData);								
				}
				*indicator_pos = !(err_reg < err_sz);
			}
			reg_params_pos ++;
			indicator_pos ++;
		}// end i
	}

	size_t reg_count = 0;
	for(int i=0; i<num_blocks; i++){
		if(!(indicator[i])){
			reg_params[reg_count] = reg_params[i];
			reg_params[reg_count + params_offset_b] = reg_params[i + params_offset_b];
			reg_count ++;
		}
	}
	//Compress coefficient arrays
	double precision_a, precision_b;
	float rel_param_err = 0.1/2;
	precision_a = rel_param_err * realPrecision / block_size;
	precision_b = rel_param_err * realPrecision;
	float last_coeffcients[2] = {0.0};
	int coeff_intvCapacity_sz = 65536;
	int coeff_intvRadius = coeff_intvCapacity_sz / 2;
	int * coeff_type[2];
	int * coeff_result_type = (int *) malloc(reg_count*2*sizeof(int));
	float * coeff_unpred_data[2];
	float * coeff_unpredictable_data = (float *) malloc(reg_count*2*sizeof(float));
	double precision[2];
	precision[0] = precision_a, precision[1] = precision_b;
	for(int i=0; i<2; i++){
		coeff_type[i] = coeff_result_type + i * reg_count;
		coeff_unpred_data[i] = coeff_unpredictable_data + i * reg_count;
	}
	int coeff_index = 0;
	unsigned int coeff_unpredictable_count[2] = {0};

	float * reg_params_separte[2];
	for(int i=0; i<2; i++){
		reg_params_separte[i] = reg_params + i * num_blocks;
	}
	for(size_t i=0; i<reg_count; i++){
		// for each coeff
		float cur_coeff;
		double diff, itvNum;
		for(int e=0; e<2; e++){
			cur_coeff = reg_params_separte[e][i];
			diff = cur_coeff - last_coeffcients[e];
			itvNum = fabs(diff)/precision[e] + 1;
			if (itvNum < coeff_intvCapacity_sz){
				if (diff < 0) itvNum = -itvNum;
				coeff_type[e][coeff_index] = (int) (itvNum/2) + coeff_intvRadius;
				last_coeffcients[e] = last_coeffcients[e] + 2 * (coeff_type[e][coeff_index] - coeff_intvRadius) * precision[e];
				//ganrantee compression error against the case of machine-epsilon
				if(fabs(cur_coeff - last_coeffcients[e])>precision[e]){	
					coeff_type[e][coeff_index] = 0;
					last_coeffcients[e] = cur_coeff;	
					coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
				}					
			}
			else{
				coeff_type[e][coeff_index] = 0;
				last_coeffcients[e] = cur_coeff;
				coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
			}
			reg_params_separte[e][i] = last_coeffcients[e];
		}
		coeff_index ++;
	}
	// pred & quantization
	int * blockwise_unpred_count = (int *) malloc(num_blocks * sizeof(int));
	int * blockwise_unpred_count_pos = blockwise_unpred_count;
	reg_params_pos = reg_params;
	indicator_pos = indicator;
	if(use_mean){
		int intvCapacity_sz = intvCapacity - 2;
		type = result_type;
		for(size_t i=0; i<num_x; i++){
			data_pos = oriData + i*block_size;
			// add 1 in x, y offset
			pred_buffer_pos = pred_buffer + 1;
			block_data_pos_x = data_pos;
			for(int ii=0; ii<block_size; ii++){
				*pred_buffer_pos = *block_data_pos_x;
				pred_buffer_pos ++;
				if(i*block_size + ii + 1< r1) block_data_pos_x ++;
			}
			if(!(*indicator_pos)){
				float curData;
				float pred;
				double itvNum;
				double diff;
				size_t index = 0;
				size_t block_unpredictable_count = 0;
				float * cur_data_pos = pred_buffer + 1;
				for(size_t ii=0; ii<block_size; ii++){
					curData = *cur_data_pos;
					pred = reg_params_pos[0] * ii + reg_params_pos[params_offset_b];									
					diff = curData - pred;
					itvNum = fabs(diff)/tmp_realPrecision + 1;
					if (itvNum < intvCapacity){
						if (diff < 0) itvNum = -itvNum;
						type[index] = (int) (itvNum/2) + intvRadius;
						pred = pred + 2 * (type[index] - intvRadius) * tmp_realPrecision;
						//ganrantee comporession error against the case of machine-epsilon
						if(fabs(curData - pred)>tmp_realPrecision){	
							type[index] = 0;
							pred = curData;
							unpredictable_data[block_unpredictable_count ++] = curData;
						}		
					}
					else{
						type[index] = 0;
						pred = curData;
						unpredictable_data[block_unpredictable_count ++] = curData;
					}
					index ++;
					cur_data_pos ++;
				}
				reg_params_pos ++;
				total_unpred += block_unpredictable_count;
				unpredictable_data += block_unpredictable_count;
				*blockwise_unpred_count_pos = block_unpredictable_count;
			}
			else{
				// use SZ
				// SZ predication
				unpredictable_count = 0;
				float * cur_data_pos = pred_buffer + 1;
				float curData;
				float pred3D;
				double itvNum, diff;
				size_t index = 0;
				for(size_t ii=0; ii<block_size; ii++){
					curData = *cur_data_pos;
					if(fabs(curData - mean) <= realPrecision){
						type[index] = 1;
						*cur_data_pos = mean;
					}
					else
					{
						pred3D = cur_data_pos[-1];
						diff = curData - pred3D;
						itvNum = fabs(diff)/realPrecision + 1;
						if (itvNum < intvCapacity_sz){
							if (diff < 0) itvNum = -itvNum;
							type[index] = (int) (itvNum/2) + intvRadius;
							*cur_data_pos = pred3D + 2 * (type[index] - intvRadius) * tmp_realPrecision;
							//ganrantee comporession error against the case of machine-epsilon
							if(fabs(curData - *cur_data_pos)>tmp_realPrecision){	
								type[index] = 0;
								*cur_data_pos = curData;	
								unpredictable_data[unpredictable_count ++] = curData;
							}					
						}
						else{
							type[index] = 0;
							*cur_data_pos = curData;
							unpredictable_data[unpredictable_count ++] = curData;
						}
					}
					index ++;
					cur_data_pos ++;
				}
				total_unpred += unpredictable_count;
				unpredictable_data += unpredictable_count;
				*blockwise_unpred_count_pos = unpredictable_count;
			}// end SZ
			blockwise_unpred_count_pos ++;
			type += block_size;
			indicator_pos ++;
		}// end i
	}
	else{
		int intvCapacity_sz = intvCapacity;
		type = result_type;
		for(size_t i=0; i<num_x; i++){
			data_pos = oriData + i*block_size;
			// add 1 in x, y offset
			pred_buffer_pos = pred_buffer + 1;
			block_data_pos_x = data_pos;
			for(int ii=0; ii<block_size; ii++){
				*pred_buffer_pos = *block_data_pos_x;
				pred_buffer_pos ++;
				if(i*block_size + ii + 1< r1) block_data_pos_x ++;
			}
			if(!(*indicator_pos)){
				float curData;
				float pred;
				double itvNum;
				double diff;
				size_t index = 0;
				size_t block_unpredictable_count = 0;
				float * cur_data_pos = pred_buffer + 1;
				for(size_t ii=0; ii<block_size; ii++){
					curData = *cur_data_pos;
					pred = reg_params_pos[0] * ii + reg_params_pos[params_offset_b];									
					diff = curData - pred;
					itvNum = fabs(diff)/tmp_realPrecision + 1;
					if (itvNum < intvCapacity){
						if (diff < 0) itvNum = -itvNum;
						type[index] = (int) (itvNum/2) + intvRadius;
						pred = pred + 2 * (type[index] - intvRadius) * tmp_realPrecision;
						//ganrantee comporession error against the case of machine-epsilon
						if(fabs(curData - pred)>tmp_realPrecision){	
							type[index] = 0;
							pred = curData;
							unpredictable_data[block_unpredictable_count ++] = curData;
						}		
					}
					else{
						type[index] = 0;
						pred = curData;
						unpredictable_data[block_unpredictable_count ++] = curData;
					}
					index ++;
					cur_data_pos ++;
				}
				reg_params_pos ++;
				total_unpred += block_unpredictable_count;
				unpredictable_data += block_unpredictable_count;
				*blockwise_unpred_count_pos = block_unpredictable_count;
			}
			else{
				// use SZ
				// SZ predication
				unpredictable_count = 0;
				float * cur_data_pos = pred_buffer + 1;
				float curData;
				float pred3D;
				double itvNum, diff;
				size_t index = 0;
				for(size_t ii=0; ii<block_size; ii++){
					curData = *cur_data_pos;					
					pred3D = cur_data_pos[-1];
					diff = curData - pred3D;
					itvNum = fabs(diff)/realPrecision + 1;
					if (itvNum < intvCapacity_sz){
						if (diff < 0) itvNum = -itvNum;
						type[index] = (int) (itvNum/2) + intvRadius;
						*cur_data_pos = pred3D + 2 * (type[index] - intvRadius) * tmp_realPrecision;
						//ganrantee comporession error against the case of machine-epsilon
						if(fabs(curData - *cur_data_pos)>tmp_realPrecision){	
							type[index] = 0;
							*cur_data_pos = curData;	
							unpredictable_data[unpredictable_count ++] = curData;
						}					
					}
					else{
						type[index] = 0;
						*cur_data_pos = curData;
						unpredictable_data[unpredictable_count ++] = curData;
					}
					index ++;
					cur_data_pos ++;
				}
				total_unpred += unpredictable_count;
				unpredictable_data += unpredictable_count;
				*blockwise_unpred_count_pos = unpredictable_count;
			}// end SZ
			blockwise_unpred_count_pos ++;
			type += block_size;
			indicator_pos ++;
		}// end i
	}	
	free(pred_buffer);
	int stateNum = 2*quantization_intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);

	size_t nodeCount = 0;
	init(huffmanTree, result_type, num_blocks*max_num_block_elements);
	size_t i = 0;
	for (i = 0; i < huffmanTree->stateNum; i++)
		if (huffmanTree->code[i]) nodeCount++; 
	nodeCount = nodeCount*2-1;

	unsigned char *treeBytes;
	unsigned int treeByteSize = convert_HuffTree_to_bytes_anyStates(huffmanTree, nodeCount, &treeBytes);

	unsigned int meta_data_offset = 3 + 1 + MetaDataByteLength;
	// total size 										metadata		  # elements     real precision		intervals	nodeCount		huffman 	 	block index 						unpredicatable count						mean 					 	unpred size 				elements
	unsigned char * result = (unsigned char *) calloc(meta_data_offset + exe_params->SZ_SIZE_TYPE + sizeof(double) + sizeof(int) + sizeof(int) + 5*treeByteSize +4*num_blocks*sizeof(int) + num_blocks * sizeof(unsigned short) + num_blocks * sizeof(unsigned short) + num_blocks * sizeof(float) + total_unpred * sizeof(float) + num_elements * sizeof(int), 1);
	unsigned char * result_pos = result;
	initRandomAccessBytes(result_pos);
	
	result_pos += meta_data_offset;
	
	sizeToBytes(result_pos,num_elements); //SZ_SIZE_TYPE: 4 or 8
	result_pos += exe_params->SZ_SIZE_TYPE;

	intToBytes_bigEndian(result_pos, block_size);
	result_pos += sizeof(int);
	doubleToBytes(result_pos, realPrecision);
	result_pos += sizeof(double);
	intToBytes_bigEndian(result_pos, quantization_intervals);
	result_pos += sizeof(int);
	intToBytes_bigEndian(result_pos, treeByteSize);
	result_pos += sizeof(int);
	intToBytes_bigEndian(result_pos, nodeCount);
	result_pos += sizeof(int);
	memcpy(result_pos, treeBytes, treeByteSize);
	result_pos += treeByteSize;
	free(treeBytes);

	memcpy(result_pos, &use_mean, sizeof(unsigned char));
	result_pos += sizeof(unsigned char);
	memcpy(result_pos, &mean, sizeof(float));
	result_pos += sizeof(float);
	size_t indicator_size = convertIntArray2ByteArray_fast_1b_to_result(indicator, num_blocks, result_pos);
	result_pos += indicator_size;
	
	//convert the lead/mid/resi to byte stream
	if(reg_count > 0){
		for(int e=0; e<2; e++){
			int stateNum = 2*coeff_intvCapacity_sz;
			HuffmanTree* huffmanTree = createHuffmanTree(stateNum);
			size_t nodeCount = 0;
			init(huffmanTree, coeff_type[e], reg_count);
			size_t i = 0;
			for (i = 0; i < huffmanTree->stateNum; i++)
				if (huffmanTree->code[i]) nodeCount++; 
			nodeCount = nodeCount*2-1;
			unsigned char *treeBytes;
			unsigned int treeByteSize = convert_HuffTree_to_bytes_anyStates(huffmanTree, nodeCount, &treeBytes);
			doubleToBytes(result_pos, precision[e]);
			result_pos += sizeof(double);
			intToBytes_bigEndian(result_pos, coeff_intvRadius);
			result_pos += sizeof(int);
			intToBytes_bigEndian(result_pos, treeByteSize);
			result_pos += sizeof(int);
			intToBytes_bigEndian(result_pos, nodeCount);
			result_pos += sizeof(int);
			memcpy(result_pos, treeBytes, treeByteSize);		
			result_pos += treeByteSize;
			free(treeBytes);
			size_t typeArray_size = 0;
			encode(huffmanTree, coeff_type[e], reg_count, result_pos + sizeof(size_t), &typeArray_size);
			sizeToBytes(result_pos, typeArray_size);
			result_pos += sizeof(size_t) + typeArray_size;
			intToBytes_bigEndian(result_pos, coeff_unpredictable_count[e]);
			result_pos += sizeof(int);
			memcpy(result_pos, coeff_unpred_data[e], coeff_unpredictable_count[e]*sizeof(float));
			result_pos += coeff_unpredictable_count[e]*sizeof(float);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	free(coeff_result_type);
	free(coeff_unpredictable_data);
	
	//record the number of unpredictable data and also store them
	memcpy(result_pos, &total_unpred, sizeof(size_t));
	result_pos += sizeof(size_t);
	// record blockwise unpred data
	size_t compressed_blockwise_unpred_count_size;
	unsigned char * compressed_bw_unpred_count = SZ_compress_args(SZ_INT32, blockwise_unpred_count, &compressed_blockwise_unpred_count_size, ABS, 0.5, 0, 0, 0, 0, 0, 0, num_blocks);
	memcpy(result_pos, &compressed_blockwise_unpred_count_size, sizeof(size_t));
	result_pos += sizeof(size_t);
	memcpy(result_pos, compressed_bw_unpred_count, compressed_blockwise_unpred_count_size);
	result_pos += compressed_blockwise_unpred_count_size;
	free(blockwise_unpred_count);
	free(compressed_bw_unpred_count);
	memcpy(result_pos, result_unpredictable_data, total_unpred * sizeof(float));
	result_pos += total_unpred * sizeof(float);

	free(reg_params);
	free(indicator);
	free(result_unpredictable_data);
	// encode type array by block
	type = result_type;
	size_t total_type_array_size = 0;
	unsigned char * type_array_buffer = (unsigned char *) malloc(num_blocks*max_num_block_elements*sizeof(int));
	unsigned short * type_array_block_size = (unsigned short *) malloc(num_blocks*sizeof(unsigned short));
	unsigned char * type_array_buffer_pos = type_array_buffer;
	unsigned short * type_array_block_size_pos = type_array_block_size;

	for(size_t i=0; i<num_x; i++){
		size_t typeArray_size = 0;
		encode(huffmanTree, type, max_num_block_elements, type_array_buffer_pos, &typeArray_size);
		total_type_array_size += typeArray_size;
		*type_array_block_size_pos = typeArray_size;
		type_array_buffer_pos += typeArray_size;
		type += max_num_block_elements;
		type_array_block_size_pos ++;
	}
	size_t compressed_type_array_block_size;
	unsigned char * compressed_type_array_block = SZ_compress_args(SZ_UINT16, type_array_block_size, &compressed_type_array_block_size, ABS, 0.5, 0, 0, 0, 0, 0, 0, num_blocks);
	memcpy(result_pos, &compressed_type_array_block_size, sizeof(size_t));
	result_pos += sizeof(size_t);
	memcpy(result_pos, compressed_type_array_block, compressed_type_array_block_size);
	result_pos += compressed_type_array_block_size;
	memcpy(result_pos, type_array_buffer, total_type_array_size);
	result_pos += total_type_array_size;
	// size_t typeArray_size = 0;
	// encode(huffmanTree, result_type, num_blocks*max_num_block_elements, result_pos, &typeArray_size);
	// result_pos += typeArray_size;

	free(compressed_type_array_block);
	free(type_array_buffer);
	free(type_array_block_size);
	size_t totalEncodeSize = result_pos - result;
	free(result_type);
	SZ_ReleaseHuffman(huffmanTree);
	*comp_size = totalEncodeSize;
	return result;
}

unsigned char * SZ_compress_float_2D_MDQ_decompression_random_access_with_blocked_regression(float *oriData, size_t r1, size_t r2, double realPrecision, size_t * comp_size){

	unsigned int quantization_intervals;
	float sz_sample_correct_freq = -1;//0.5; //-1
	float dense_pos;
	float mean_flush_freq;
	unsigned char use_mean = 0;

	// calculate block dims
	size_t num_x, num_y;
	size_t block_size = 16;
	num_x = (r1 - 1) / block_size + 1;
	num_y = (r2 - 1) / block_size + 1;

	size_t max_num_block_elements = block_size * block_size;
	size_t num_blocks = num_x * num_y;
	size_t num_elements = r1 * r2;
	size_t dim0_offset = r2;

	int * result_type = (int *) malloc(num_blocks*max_num_block_elements * sizeof(int));
	size_t unpred_data_max_size = max_num_block_elements;
	float * result_unpredictable_data = (float *) malloc(unpred_data_max_size * sizeof(float) * num_blocks);
	size_t total_unpred = 0;
	size_t unpredictable_count;
	float * data_pos = oriData;
	int * type = result_type;
	float * reg_params = (float *) malloc(num_blocks * 3 * sizeof(float));
	float * reg_params_pos = reg_params;
	// move regression part out
	size_t params_offset_b = num_blocks;
	size_t params_offset_c = 2*num_blocks;
	float * pred_buffer = (float *) malloc((block_size+1)*(block_size+1)*sizeof(float));
	float * pred_buffer_pos = NULL;
	float * block_data_pos_x = NULL;
	float * block_data_pos_y = NULL;
	for(size_t i=0; i<num_x; i++){
		for(size_t j=0; j<num_y; j++){
			data_pos = oriData + i*block_size * dim0_offset + j*block_size;
			pred_buffer_pos = pred_buffer;
			block_data_pos_x = data_pos;
			// use the buffer as block_size*block_size
			for(int ii=0; ii<block_size; ii++){
				block_data_pos_y = block_data_pos_x;
				for(int jj=0; jj<block_size; jj++){
					*pred_buffer_pos = *block_data_pos_y;
					if(j*block_size + jj + 1< r2) block_data_pos_y ++;
					pred_buffer_pos ++;
				}
				if(i*block_size + ii + 1 < r1) block_data_pos_x += dim0_offset;
			}
			/*Calculate regression coefficients*/
			{
				float * cur_data_pos = pred_buffer;
				float fx = 0.0;
				float fy = 0.0;
				float f = 0;
				float sum_x; 
				float curData;
				for(size_t i=0; i<block_size; i++){
					sum_x = 0;
					for(size_t j=0; j<block_size; j++){
						curData = *cur_data_pos;
						sum_x += curData;
						fy += curData * j;
						cur_data_pos ++;
					}
					fx += sum_x * i;
					f += sum_x;
				}
				float coeff = 1.0 / (block_size * block_size);
				reg_params_pos[0] = (2 * fx / (block_size - 1) - f) * 6 * coeff / (block_size + 1);
				reg_params_pos[params_offset_b] = (2 * fy / (block_size - 1) - f) * 6 * coeff / (block_size + 1);
				reg_params_pos[params_offset_c] = f * coeff - ((block_size - 1) * reg_params_pos[0] / 2 + (block_size - 1) * reg_params_pos[params_offset_b] / 2);
			}
			reg_params_pos ++;
		}
	}
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_2D_with_freq_and_dense_pos(oriData, r1, r2, realPrecision, &dense_pos, &sz_sample_correct_freq, &mean_flush_freq);
		if(mean_flush_freq > 0.5 || mean_flush_freq > sz_sample_correct_freq) use_mean = 1;
		updateQuantizationInfo(quantization_intervals);
	}	
	else{
		quantization_intervals = exe_params->intvCapacity;
	}

	float mean = 0;
	if(use_mean){
		// compute mean
		double sum = 0.0;
		size_t mean_count = 0;
		for(size_t i=0; i<num_elements; i++){
			if(fabs(oriData[i] - dense_pos) < realPrecision){
				sum += oriData[i];
				mean_count ++;
			}
		}
		if(mean_count > 0) mean = sum / mean_count;
	}

	double tmp_realPrecision = realPrecision;

	// use two prediction buffers for higher performance
	float * unpredictable_data = result_unpredictable_data;
	unsigned char * indicator = (unsigned char *) malloc(num_blocks * sizeof(unsigned char));
	memset(indicator, 0, num_blocks * sizeof(unsigned char));
	unsigned char * indicator_pos = indicator;

	int intvCapacity = quantization_intervals; //exe_params->intvCapacity;
	int intvRadius = intvCapacity/2; //exe_params->intvRadius;	
	float noise = realPrecision * 0.81;
	reg_params_pos = reg_params;

	memset(pred_buffer, 0, (block_size+1)*(block_size+1)*sizeof(float));
	int pred_buffer_block_size = block_size + 1;
	int strip_dim0_offset = pred_buffer_block_size;

	// select
	if(use_mean){
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				data_pos = oriData + i*block_size * dim0_offset + j*block_size;
				// add 1 in x, y offset
				pred_buffer_pos = pred_buffer + pred_buffer_block_size + 1;
				block_data_pos_x = data_pos;
				for(int ii=0; ii<block_size; ii++){
					block_data_pos_y = block_data_pos_x;
					for(int jj=0; jj<block_size; jj++){
						*pred_buffer_pos = *block_data_pos_y;
						if(j*block_size + jj + 1< r2) block_data_pos_y ++;
						pred_buffer_pos ++;
					}
					// add 1 in y offset
					pred_buffer_pos ++;
					if(i*block_size + ii + 1< r1) block_data_pos_x += dim0_offset;
				}
				/*sampling and decide which predictor*/
				{
					float * cur_data_pos;
					float curData;
					float pred_reg, pred_sz;
					float err_sz = 0.0, err_reg = 0.0;
					int bmi = 0;
					for(int i=2; i<=block_size; i++){
						cur_data_pos = pred_buffer + i*pred_buffer_block_size + i;
						curData = *cur_data_pos;
						pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim0_offset - 1];
						pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * (i-1) + reg_params_pos[params_offset_c];							
						err_sz += MIN(fabs(pred_sz - curData) + noise, fabs(mean - curData));
						err_reg += fabs(pred_reg - curData);

						bmi = block_size - i + 1;
						cur_data_pos = pred_buffer + i*pred_buffer_block_size + (bmi+1);
						curData = *cur_data_pos;
						pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim0_offset - 1];
						pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c];							
						err_sz += MIN(fabs(pred_sz - curData) + noise, fabs(mean - curData));
						err_reg += fabs(pred_reg - curData);								
					}
					*indicator_pos = !(err_reg < err_sz);
				}
				reg_params_pos ++;
				indicator_pos ++;
			}// end j
		}// end i
	}
	else{
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				data_pos = oriData + i*block_size * dim0_offset + j*block_size;
				// add 1 in x, y offset
				pred_buffer_pos = pred_buffer + pred_buffer_block_size + 1;
				block_data_pos_x = data_pos;
				for(int ii=0; ii<block_size; ii++){
					block_data_pos_y = block_data_pos_x;
					for(int jj=0; jj<block_size; jj++){
						*pred_buffer_pos = *block_data_pos_y;
						if(j*block_size + jj + 1< r2) block_data_pos_y ++;
						pred_buffer_pos ++;
					}
					// add 1 in y offset
					pred_buffer_pos ++;
					if(i*block_size + ii + 1< r1) block_data_pos_x += dim0_offset;
				}
				/*sampling and decide which predictor*/
				{
					float * cur_data_pos;
					float curData;
					float pred_reg, pred_sz;
					float err_sz = 0.0, err_reg = 0.0;
					int bmi = 0;
					for(int i=2; i<=block_size; i++){
						cur_data_pos = pred_buffer + i*pred_buffer_block_size + i;
						curData = *cur_data_pos;
						pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim0_offset - 1];
						pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * (i-1) + reg_params_pos[params_offset_c];							
						err_sz += fabs(pred_sz - curData) + noise;
						err_reg += fabs(pred_reg - curData);

						bmi = block_size - i + 1;
						cur_data_pos = pred_buffer + i*pred_buffer_block_size + (bmi+1);
						curData = *cur_data_pos;
						pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim0_offset - 1];
						pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c];							
						err_sz += fabs(pred_sz - curData) + noise;
						err_reg += fabs(pred_reg - curData);								
					}
					*indicator_pos = !(err_reg < err_sz);
				}
				reg_params_pos ++;
				indicator_pos ++;
			}// end j
		}// end i
	}

	size_t reg_count = 0;
	for(int i=0; i<num_blocks; i++){
		if(!(indicator[i])){
			reg_params[reg_count] = reg_params[i];
			reg_params[reg_count + params_offset_b] = reg_params[i + params_offset_b];
			reg_params[reg_count + params_offset_c] = reg_params[i + params_offset_c];
			reg_count ++;
		}
	}
	//Compress coefficient arrays
	double precision_a, precision_b, precision_c;
	float rel_param_err = 0.15/3;
	precision_a = rel_param_err * realPrecision / block_size;
	precision_b = rel_param_err * realPrecision / block_size;
	precision_c = rel_param_err * realPrecision;
	float last_coeffcients[3] = {0.0};
	int coeff_intvCapacity_sz = 65536;
	int coeff_intvRadius = coeff_intvCapacity_sz / 2;
	int * coeff_type[3];
	int * coeff_result_type = (int *) malloc(reg_count*3*sizeof(int));
	float * coeff_unpred_data[3];
	float * coeff_unpredictable_data = (float *) malloc(reg_count*3*sizeof(float));
	double precision[3];
	precision[0] = precision_a, precision[1] = precision_b, precision[2] = precision_c;
	for(int i=0; i<3; i++){
		coeff_type[i] = coeff_result_type + i * reg_count;
		coeff_unpred_data[i] = coeff_unpredictable_data + i * reg_count;
	}
	int coeff_index = 0;
	unsigned int coeff_unpredictable_count[3] = {0};

	float * reg_params_separte[3];
	for(int i=0; i<3; i++){
		reg_params_separte[i] = reg_params + i * num_blocks;
	}
	for(size_t i=0; i<reg_count; i++){
		// for each coeff
		float cur_coeff;
		double diff, itvNum;
		for(int e=0; e<3; e++){
			cur_coeff = reg_params_separte[e][i];
			diff = cur_coeff - last_coeffcients[e];
			itvNum = fabs(diff)/precision[e] + 1;
			if (itvNum < coeff_intvCapacity_sz){
				if (diff < 0) itvNum = -itvNum;
				coeff_type[e][coeff_index] = (int) (itvNum/2) + coeff_intvRadius;
				last_coeffcients[e] = last_coeffcients[e] + 2 * (coeff_type[e][coeff_index] - coeff_intvRadius) * precision[e];
				//ganrantee compression error against the case of machine-epsilon
				if(fabs(cur_coeff - last_coeffcients[e])>precision[e]){	
					coeff_type[e][coeff_index] = 0;
					last_coeffcients[e] = cur_coeff;	
					coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
				}					
			}
			else{
				coeff_type[e][coeff_index] = 0;
				last_coeffcients[e] = cur_coeff;
				coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
			}
			reg_params_separte[e][i] = last_coeffcients[e];
		}
		coeff_index ++;
	}
	// pred & quantization
	int * blockwise_unpred_count = (int *) malloc(num_blocks * sizeof(int));
	int * blockwise_unpred_count_pos = blockwise_unpred_count;
	reg_params_pos = reg_params;
	indicator_pos = indicator;
	if(use_mean){
		int intvCapacity_sz = intvCapacity - 2;
		type = result_type;
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				data_pos = oriData + i*block_size * dim0_offset + j*block_size;
				// add 1 in x, y offset
				pred_buffer_pos = pred_buffer + pred_buffer_block_size + 1;
				block_data_pos_x = data_pos;
				for(int ii=0; ii<block_size; ii++){
					block_data_pos_y = block_data_pos_x;
					for(int jj=0; jj<block_size; jj++){
						*pred_buffer_pos = *block_data_pos_y;
						if(j*block_size + jj + 1< r2) block_data_pos_y ++;
						pred_buffer_pos ++;
					}
					// add 1 in y offset
					pred_buffer_pos ++;
					if(i*block_size + ii + 1< r1) block_data_pos_x += dim0_offset;
				}
				if(!(*indicator_pos)){
					float curData;
					float pred;
					double itvNum;
					double diff;
					size_t index = 0;
					size_t block_unpredictable_count = 0;
					float * cur_data_pos = pred_buffer + pred_buffer_block_size + 1;
					for(size_t ii=0; ii<block_size; ii++){
						for(size_t jj=0; jj<block_size; jj++){
							curData = *cur_data_pos;
							pred = reg_params_pos[0] * ii + reg_params_pos[params_offset_b] * jj + reg_params_pos[params_offset_c];									
							diff = curData - pred;
							itvNum = fabs(diff)/tmp_realPrecision + 1;
							if (itvNum < intvCapacity){
								if (diff < 0) itvNum = -itvNum;
								type[index] = (int) (itvNum/2) + intvRadius;
								pred = pred + 2 * (type[index] - intvRadius) * tmp_realPrecision;
								//ganrantee comporession error against the case of machine-epsilon
								if(fabs(curData - pred)>tmp_realPrecision){	
									type[index] = 0;
									pred = curData;
									unpredictable_data[block_unpredictable_count ++] = curData;
								}		
							}
							else{
								type[index] = 0;
								pred = curData;
								unpredictable_data[block_unpredictable_count ++] = curData;
							}
							index ++;	
							cur_data_pos ++;
						}
						cur_data_pos ++;
					}
					reg_params_pos ++;
					total_unpred += block_unpredictable_count;
					unpredictable_data += block_unpredictable_count;
					*blockwise_unpred_count_pos = block_unpredictable_count;
				}
				else{
					// use SZ
					// SZ predication
					unpredictable_count = 0;
					float * cur_data_pos = pred_buffer + pred_buffer_block_size + 1;
					float curData;
					float pred3D;
					double itvNum, diff;
					size_t index = 0;
					for(size_t ii=0; ii<block_size; ii++){
						for(size_t jj=0; jj<block_size; jj++){
							curData = *cur_data_pos;
							if(fabs(curData - mean) <= realPrecision){
								type[index] = 1;
								*cur_data_pos = mean;
							}
							else
							{
								pred3D = cur_data_pos[-1] + cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim0_offset - 1];
								diff = curData - pred3D;
								itvNum = fabs(diff)/realPrecision + 1;
								if (itvNum < intvCapacity_sz){
									if (diff < 0) itvNum = -itvNum;
									type[index] = (int) (itvNum/2) + intvRadius;
									*cur_data_pos = pred3D + 2 * (type[index] - intvRadius) * tmp_realPrecision;
									//ganrantee comporession error against the case of machine-epsilon
									if(fabs(curData - *cur_data_pos)>tmp_realPrecision){	
										type[index] = 0;
										*cur_data_pos = curData;	
										unpredictable_data[unpredictable_count ++] = curData;
									}					
								}
								else{
									type[index] = 0;
									*cur_data_pos = curData;
									unpredictable_data[unpredictable_count ++] = curData;
								}
							}
							index ++;
							cur_data_pos ++;
						}
						cur_data_pos ++;
					}
					total_unpred += unpredictable_count;
					unpredictable_data += unpredictable_count;
					*blockwise_unpred_count_pos = unpredictable_count;
				}// end SZ
				blockwise_unpred_count_pos ++;
				type += block_size * block_size;
				indicator_pos ++;
			}// end j
		}// end i
	}
	else{
		int intvCapacity_sz = intvCapacity;
		type = result_type;
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				data_pos = oriData + i*block_size * dim0_offset + j*block_size;
				// add 1 in x, y offset
				pred_buffer_pos = pred_buffer + pred_buffer_block_size + 1;
				block_data_pos_x = data_pos;
				for(int ii=0; ii<block_size; ii++){
					block_data_pos_y = block_data_pos_x;
					for(int jj=0; jj<block_size; jj++){
						*pred_buffer_pos = *block_data_pos_y;
						if(j*block_size + jj + 1< r2) block_data_pos_y ++;
						pred_buffer_pos ++;
					}
					// add 1 in y offset
					pred_buffer_pos ++;
					if(i*block_size + ii + 1< r1) block_data_pos_x += dim0_offset;
				}
				if(!(*indicator_pos)){
					float curData;
					float pred;
					double itvNum;
					double diff;
					size_t index = 0;
					size_t block_unpredictable_count = 0;
					float * cur_data_pos = pred_buffer + pred_buffer_block_size + 1;
					for(size_t ii=0; ii<block_size; ii++){
						for(size_t jj=0; jj<block_size; jj++){
							curData = *cur_data_pos;
							pred = reg_params_pos[0] * ii + reg_params_pos[params_offset_b] * jj + reg_params_pos[params_offset_c];									
							diff = curData - pred;
							itvNum = fabs(diff)/tmp_realPrecision + 1;
							if (itvNum < intvCapacity){
								if (diff < 0) itvNum = -itvNum;
								type[index] = (int) (itvNum/2) + intvRadius;
								pred = pred + 2 * (type[index] - intvRadius) * tmp_realPrecision;
								//ganrantee comporession error against the case of machine-epsilon
								if(fabs(curData - pred)>tmp_realPrecision){	
									type[index] = 0;
									pred = curData;
									unpredictable_data[block_unpredictable_count ++] = curData;
								}		
							}
							else{
								type[index] = 0;
								pred = curData;
								unpredictable_data[block_unpredictable_count ++] = curData;
							}
							index ++;	
							cur_data_pos ++;
						}
						cur_data_pos ++;
					}
					reg_params_pos ++;
					total_unpred += block_unpredictable_count;
					unpredictable_data += block_unpredictable_count;
					*blockwise_unpred_count_pos = block_unpredictable_count;
				}
				else{
					// use SZ
					// SZ predication
					unpredictable_count = 0;
					float * cur_data_pos = pred_buffer + pred_buffer_block_size + 1;
					float curData;
					float pred3D;
					double itvNum, diff;
					size_t index = 0;
					for(size_t ii=0; ii<block_size; ii++){
						for(size_t jj=0; jj<block_size; jj++){
							curData = *cur_data_pos;
							pred3D = cur_data_pos[-1] + cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim0_offset - 1];
							diff = curData - pred3D;
							itvNum = fabs(diff)/realPrecision + 1;
							if (itvNum < intvCapacity_sz){
								if (diff < 0) itvNum = -itvNum;
								type[index] = (int) (itvNum/2) + intvRadius;
								*cur_data_pos = pred3D + 2 * (type[index] - intvRadius) * tmp_realPrecision;
								//ganrantee comporession error against the case of machine-epsilon
								if(fabs(curData - *cur_data_pos)>tmp_realPrecision){	
									type[index] = 0;
									*cur_data_pos = curData;	
									unpredictable_data[unpredictable_count ++] = curData;
								}					
							}
							else{
								type[index] = 0;
								*cur_data_pos = curData;
								unpredictable_data[unpredictable_count ++] = curData;
							}
							index ++;
							cur_data_pos ++;
						}
						cur_data_pos ++;
					}
					total_unpred += unpredictable_count;
					unpredictable_data += unpredictable_count;
					*blockwise_unpred_count_pos = unpredictable_count;
				}// end SZ
				blockwise_unpred_count_pos ++;
				type += block_size * block_size;
				indicator_pos ++;
			}// end j
		}// end i
	}	

	free(pred_buffer);
	int stateNum = 2*quantization_intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);

	size_t nodeCount = 0;
	init(huffmanTree, result_type, num_blocks*max_num_block_elements);
	size_t i = 0;
	for (i = 0; i < huffmanTree->stateNum; i++)
		if (huffmanTree->code[i]) nodeCount++; 
	nodeCount = nodeCount*2-1;

	unsigned char *treeBytes;
	unsigned int treeByteSize = convert_HuffTree_to_bytes_anyStates(huffmanTree, nodeCount, &treeBytes);

	unsigned int meta_data_offset = 3 + 1 + MetaDataByteLength;
	// total size 										metadata		  # elements     real precision		intervals	nodeCount		huffman 	 	block index 						unpredicatable count						mean 					 	unpred size 				elements
	unsigned char * result = (unsigned char *) calloc(meta_data_offset + exe_params->SZ_SIZE_TYPE + sizeof(double) + sizeof(int) + sizeof(int) + 5*treeByteSize + 4*num_blocks*sizeof(int) +num_blocks * sizeof(unsigned short) + num_blocks * sizeof(unsigned short) + num_blocks * sizeof(float) + total_unpred * sizeof(float) + num_elements * sizeof(int), 1);
	unsigned char * result_pos = result;
	initRandomAccessBytes(result_pos);
	
	result_pos += meta_data_offset;
	
	sizeToBytes(result_pos,num_elements); //SZ_SIZE_TYPE: 4 or 8
	result_pos += exe_params->SZ_SIZE_TYPE;

	intToBytes_bigEndian(result_pos, block_size);
	result_pos += sizeof(int);
	doubleToBytes(result_pos, realPrecision);
	result_pos += sizeof(double);
	intToBytes_bigEndian(result_pos, quantization_intervals);
	result_pos += sizeof(int);
	intToBytes_bigEndian(result_pos, treeByteSize);
	result_pos += sizeof(int);
	intToBytes_bigEndian(result_pos, nodeCount);
	result_pos += sizeof(int);
	memcpy(result_pos, treeBytes, treeByteSize);
	result_pos += treeByteSize;
	free(treeBytes);

	memcpy(result_pos, &use_mean, sizeof(unsigned char));
	result_pos += sizeof(unsigned char);
	memcpy(result_pos, &mean, sizeof(float));
	result_pos += sizeof(float);
	size_t indicator_size = convertIntArray2ByteArray_fast_1b_to_result(indicator, num_blocks, result_pos);
	result_pos += indicator_size;
	
	//convert the lead/mid/resi to byte stream
	if(reg_count > 0){
		for(int e=0; e<3; e++){
			int stateNum = 2*coeff_intvCapacity_sz;
			HuffmanTree* huffmanTree = createHuffmanTree(stateNum);
			size_t nodeCount = 0;
			init(huffmanTree, coeff_type[e], reg_count);
			size_t i = 0;
			for (i = 0; i < huffmanTree->stateNum; i++)
				if (huffmanTree->code[i]) nodeCount++; 
			nodeCount = nodeCount*2-1;
			unsigned char *treeBytes;
			unsigned int treeByteSize = convert_HuffTree_to_bytes_anyStates(huffmanTree, nodeCount, &treeBytes);
			doubleToBytes(result_pos, precision[e]);
			result_pos += sizeof(double);
			intToBytes_bigEndian(result_pos, coeff_intvRadius);
			result_pos += sizeof(int);
			intToBytes_bigEndian(result_pos, treeByteSize);
			result_pos += sizeof(int);
			intToBytes_bigEndian(result_pos, nodeCount);
			result_pos += sizeof(int);
			memcpy(result_pos, treeBytes, treeByteSize);		
			result_pos += treeByteSize;
			free(treeBytes);
			size_t typeArray_size = 0;
			encode(huffmanTree, coeff_type[e], reg_count, result_pos + sizeof(size_t), &typeArray_size);
			sizeToBytes(result_pos, typeArray_size);
			result_pos += sizeof(size_t) + typeArray_size;
			intToBytes_bigEndian(result_pos, coeff_unpredictable_count[e]);
			result_pos += sizeof(int);
			memcpy(result_pos, coeff_unpred_data[e], coeff_unpredictable_count[e]*sizeof(float));
			result_pos += coeff_unpredictable_count[e]*sizeof(float);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	free(coeff_result_type);
	free(coeff_unpredictable_data);
	
	//record the number of unpredictable data and also store them
	memcpy(result_pos, &total_unpred, sizeof(size_t));
	result_pos += sizeof(size_t);
	// record blockwise unpred data
	size_t compressed_blockwise_unpred_count_size;
	unsigned char * compressed_bw_unpred_count = SZ_compress_args(SZ_INT32, blockwise_unpred_count, &compressed_blockwise_unpred_count_size, ABS, 0.5, 0, 0, 0, 0, 0, 0, num_blocks);
	memcpy(result_pos, &compressed_blockwise_unpred_count_size, sizeof(size_t));
	result_pos += sizeof(size_t);
	memcpy(result_pos, compressed_bw_unpred_count, compressed_blockwise_unpred_count_size);
	result_pos += compressed_blockwise_unpred_count_size;
	free(blockwise_unpred_count);
	free(compressed_bw_unpred_count);
	memcpy(result_pos, result_unpredictable_data, total_unpred * sizeof(float));
	result_pos += total_unpred * sizeof(float);

	free(reg_params);
	free(indicator);
	free(result_unpredictable_data);
	// encode type array by block
	type = result_type;
	size_t total_type_array_size = 0;
	unsigned char * type_array_buffer = (unsigned char *) malloc(num_blocks*max_num_block_elements*sizeof(int));
	unsigned short * type_array_block_size = (unsigned short *) malloc(num_blocks*sizeof(unsigned short));
	unsigned char * type_array_buffer_pos = type_array_buffer;
	unsigned short * type_array_block_size_pos = type_array_block_size;

	for(size_t i=0; i<num_x; i++){
		for(size_t j=0; j<num_y; j++){
			size_t typeArray_size = 0;
			encode(huffmanTree, type, max_num_block_elements, type_array_buffer_pos, &typeArray_size);
			total_type_array_size += typeArray_size;
			*type_array_block_size_pos = typeArray_size;
			type_array_buffer_pos += typeArray_size;
			type += max_num_block_elements;
			type_array_block_size_pos ++;
		}
	}
	size_t compressed_type_array_block_size;
	unsigned char * compressed_type_array_block = SZ_compress_args(SZ_UINT16, type_array_block_size, &compressed_type_array_block_size, ABS, 0.5, 0, 0, 0, 0, 0, 0, num_blocks);
	memcpy(result_pos, &compressed_type_array_block_size, sizeof(size_t));
	result_pos += sizeof(size_t);
	memcpy(result_pos, compressed_type_array_block, compressed_type_array_block_size);
	result_pos += compressed_type_array_block_size;
	memcpy(result_pos, type_array_buffer, total_type_array_size);
	result_pos += total_type_array_size;
	// size_t typeArray_size = 0;
	// encode(huffmanTree, result_type, num_blocks*max_num_block_elements, result_pos, &typeArray_size);
	// result_pos += typeArray_size;

	free(compressed_type_array_block);
	free(type_array_buffer);
	free(type_array_block_size);
	size_t totalEncodeSize = result_pos - result;
	free(result_type);
	SZ_ReleaseHuffman(huffmanTree);
	*comp_size = totalEncodeSize;
	return result;
}

unsigned char * SZ_compress_float_3D_MDQ_decompression_random_access_with_blocked_regression(float *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t * comp_size){

	unsigned int quantization_intervals;
	float sz_sample_correct_freq = -1;//0.5; //-1
	float dense_pos;
	float mean_flush_freq;
	unsigned char use_mean = 0;

	// calculate block dims
	size_t num_x, num_y, num_z;
	size_t block_size = 6;
	num_x = (r1 - 1) / block_size + 1;
	num_y = (r2 - 1) / block_size + 1;
	num_z = (r3 - 1) / block_size + 1;

	size_t max_num_block_elements = block_size * block_size * block_size;
	size_t num_blocks = num_x * num_y * num_z;
	size_t num_elements = r1 * r2 * r3;

	size_t dim0_offset = r2 * r3;
	size_t dim1_offset = r3;	

	int * result_type = (int *) malloc(num_blocks*max_num_block_elements * sizeof(int));
	size_t unpred_data_max_size = max_num_block_elements;
	float * result_unpredictable_data = (float *) malloc(unpred_data_max_size * sizeof(float) * num_blocks);
	size_t total_unpred = 0;
	size_t unpredictable_count;
	float * data_pos = oriData;
	int * type = result_type;
	float * reg_params = (float *) malloc(num_blocks * 4 * sizeof(float));
	float * reg_params_pos = reg_params;
	// move regression part out
	size_t params_offset_b = num_blocks;
	size_t params_offset_c = 2*num_blocks;
	size_t params_offset_d = 3*num_blocks;
	float * pred_buffer = (float *) malloc((block_size+1)*(block_size+1)*(block_size+1)*sizeof(float));
	float * pred_buffer_pos = NULL;
	float * block_data_pos_x = NULL;
	float * block_data_pos_y = NULL;
	float * block_data_pos_z = NULL;
	for(size_t i=0; i<num_x; i++){
		for(size_t j=0; j<num_y; j++){
			for(size_t k=0; k<num_z; k++){
				data_pos = oriData + i*block_size * dim0_offset + j*block_size * dim1_offset + k*block_size;
				pred_buffer_pos = pred_buffer;
				block_data_pos_x = data_pos;
				// use the buffer as block_size*block_size*block_size
				for(int ii=0; ii<block_size; ii++){
					block_data_pos_y = block_data_pos_x;
					for(int jj=0; jj<block_size; jj++){
						block_data_pos_z = block_data_pos_y;
						for(int kk=0; kk<block_size; kk++){
							*pred_buffer_pos = *block_data_pos_z;
							if(k*block_size + kk + 1 < r3) block_data_pos_z ++;
							pred_buffer_pos ++;
						}
						if(j*block_size + jj + 1 < r2) block_data_pos_y += dim1_offset;
					}
					if(i*block_size + ii + 1 < r1) block_data_pos_x += dim0_offset;
				}
				/*Calculate regression coefficients*/
				{
					float * cur_data_pos = pred_buffer;
					float fx = 0.0;
					float fy = 0.0;
					float fz = 0.0;
					float f = 0;
					float sum_x, sum_y; 
					float curData;
					for(size_t i=0; i<block_size; i++){
						sum_x = 0;
						for(size_t j=0; j<block_size; j++){
							sum_y = 0;
							for(size_t k=0; k<block_size; k++){
								curData = *cur_data_pos;
								sum_y += curData;
								fz += curData * k;
								cur_data_pos ++;
							}
							fy += sum_y * j;
							sum_x += sum_y;
						}
						fx += sum_x * i;
						f += sum_x;
					}
					float coeff = 1.0 / (block_size * block_size * block_size);
					reg_params_pos[0] = (2 * fx / (block_size - 1) - f) * 6 * coeff / (block_size + 1);
					reg_params_pos[params_offset_b] = (2 * fy / (block_size - 1) - f) * 6 * coeff / (block_size + 1);
					reg_params_pos[params_offset_c] = (2 * fz / (block_size - 1) - f) * 6 * coeff / (block_size + 1);
					reg_params_pos[params_offset_d] = f * coeff - ((block_size - 1) * reg_params_pos[0] / 2 + (block_size - 1) * reg_params_pos[params_offset_b] / 2 + (block_size - 1) * reg_params_pos[params_offset_c] / 2);
				}
				reg_params_pos ++;
			}
		}
	}
	
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_3D_with_freq_and_dense_pos(oriData, r1, r2, r3, realPrecision, &dense_pos, &sz_sample_correct_freq, &mean_flush_freq);
		if(mean_flush_freq > 0.5 || mean_flush_freq > sz_sample_correct_freq) use_mean = 1;
		updateQuantizationInfo(quantization_intervals);
	}	
	else{
		quantization_intervals = exe_params->intvCapacity;
	}

	float mean = 0;
	if(use_mean){
		// compute mean
		double sum = 0.0;
		size_t mean_count = 0;
		for(size_t i=0; i<num_elements; i++){
			if(fabs(oriData[i] - dense_pos) < realPrecision){
				sum += oriData[i];
				mean_count ++;
			}
		}
		if(mean_count > 0) mean = sum / mean_count;
	}

	double tmp_realPrecision = realPrecision;

	// use two prediction buffers for higher performance
	float * unpredictable_data = result_unpredictable_data;
	unsigned char * indicator = (unsigned char *) malloc(num_blocks * sizeof(unsigned char));
	memset(indicator, 0, num_blocks * sizeof(unsigned char));
	unsigned char * indicator_pos = indicator;

	int intvCapacity = quantization_intervals; //exe_params->intvCapacity;
	int intvRadius = intvCapacity/2; //exe_params->intvRadius;	
	float noise = realPrecision * 1.22;
	reg_params_pos = reg_params;

	memset(pred_buffer, 0, (block_size+1)*(block_size+1)*(block_size+1)*sizeof(float));
	int pred_buffer_block_size = block_size + 1;
	int strip_dim0_offset = pred_buffer_block_size * pred_buffer_block_size;
	int strip_dim1_offset = pred_buffer_block_size;

	// select
	if(use_mean){
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				for(size_t k=0; k<num_z; k++){
					data_pos = oriData + i*block_size * dim0_offset + j*block_size * dim1_offset + k*block_size;
					// add 1 in x, y, z offset
					pred_buffer_pos = pred_buffer + pred_buffer_block_size*pred_buffer_block_size + pred_buffer_block_size + 1;
					block_data_pos_x = data_pos;
					for(int ii=0; ii<block_size; ii++){
						block_data_pos_y = block_data_pos_x;
						for(int jj=0; jj<block_size; jj++){
							block_data_pos_z = block_data_pos_y;
							for(int kk=0; kk<block_size; kk++){
								*pred_buffer_pos = *block_data_pos_z;
								if(k*block_size + kk + 1< r3) block_data_pos_z ++;
								pred_buffer_pos ++;
							}
							// add 1 in z offset
							pred_buffer_pos ++;
							if(j*block_size + jj + 1< r2) block_data_pos_y += dim1_offset;
						}
						// add 1 in y offset
						pred_buffer_pos += pred_buffer_block_size;
						if(i*block_size + ii + 1< r1) block_data_pos_x += dim0_offset;
					}
					/*sampling and decide which predictor*/
					{
						// sample point [1, 1, 1] [1, 1, 4] [1, 4, 1] [1, 4, 4] [4, 1, 1] [4, 1, 4] [4, 4, 1] [4, 4, 4]
						float * cur_data_pos;
						float curData;
						float pred_reg, pred_sz;
						float err_sz = 0.0, err_reg = 0.0;
						int bmi = 0;
						for(int i=2; i<=block_size; i++){
							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + i*pred_buffer_block_size + i;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * (i-1) + reg_params_pos[params_offset_c] * (i-1) + reg_params_pos[params_offset_d];							
							err_sz += MIN(fabs(pred_sz - curData) + noise, fabs(mean - curData));
							err_reg += fabs(pred_reg - curData);

							bmi = block_size - i + 1;
							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + i*pred_buffer_block_size + (bmi+1);
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * (i-1) + reg_params_pos[params_offset_c] * bmi + reg_params_pos[params_offset_d];							
							err_sz += MIN(fabs(pred_sz - curData) + noise, fabs(mean - curData));
							err_reg += fabs(pred_reg - curData);								

							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + (bmi+1)*pred_buffer_block_size + i;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c] * (i-1) + reg_params_pos[params_offset_d];							
							err_sz += MIN(fabs(pred_sz - curData) + noise, fabs(mean - curData));
							err_reg += fabs(pred_reg - curData);								

							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + (bmi+1)*pred_buffer_block_size + (bmi+1);
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c] * bmi + reg_params_pos[params_offset_d];							
							err_sz += MIN(fabs(pred_sz - curData) + noise, fabs(mean - curData));
							err_reg += fabs(pred_reg - curData);
						}
						// indicator_pos[k] = (err_sz < err_reg);
						indicator_pos[k] = !(err_reg < err_sz);
					}
					reg_params_pos ++;
				} // end k
				indicator_pos += num_z;
			}// end j
		}// end i
	}
	else{
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				for(size_t k=0; k<num_z; k++){
					data_pos = oriData + i*block_size * dim0_offset + j*block_size * dim1_offset + k*block_size;
					// add 1 in x, y, z offset
					pred_buffer_pos = pred_buffer + pred_buffer_block_size*pred_buffer_block_size + pred_buffer_block_size + 1;
					block_data_pos_x = data_pos;
					for(int ii=0; ii<block_size; ii++){
						block_data_pos_y = block_data_pos_x;
						for(int jj=0; jj<block_size; jj++){
							block_data_pos_z = block_data_pos_y;
							for(int kk=0; kk<block_size; kk++){
								*pred_buffer_pos = *block_data_pos_z;
								if(k*block_size + kk + 1< r3) block_data_pos_z ++;
								pred_buffer_pos ++;
							}
							// add 1 in z offset
							pred_buffer_pos ++;
							if(j*block_size + jj + 1< r2) block_data_pos_y += dim1_offset;
						}
						// add 1 in y offset
						pred_buffer_pos += pred_buffer_block_size;
						if(i*block_size + ii +1 < r1) block_data_pos_x += dim0_offset;
					}
					/*sampling*/
					{
						// sample point [1, 1, 1] [1, 1, 4] [1, 4, 1] [1, 4, 4] [4, 1, 1] [4, 1, 4] [4, 4, 1] [4, 4, 4]
						float * cur_data_pos;
						float curData;
						float pred_reg, pred_sz;
						float err_sz = 0.0, err_reg = 0.0;
						int bmi;
						for(int i=2; i<=block_size; i++){
							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + i*pred_buffer_block_size + i;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * (i-1) + reg_params_pos[params_offset_c] * (i-1) + reg_params_pos[params_offset_d];							
							err_sz += fabs(pred_sz - curData) + noise;
							err_reg += fabs(pred_reg - curData);

							bmi = block_size - i + 1;
							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + i*pred_buffer_block_size + (bmi+1);
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * (i-1) + reg_params_pos[params_offset_c] * bmi + reg_params_pos[params_offset_d];							
							err_sz += fabs(pred_sz - curData) + noise;
							err_reg += fabs(pred_reg - curData);								

							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + (bmi+1)*pred_buffer_block_size + i;
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c] * (i-1) + reg_params_pos[params_offset_d];							
							err_sz += fabs(pred_sz - curData) + noise;
							err_reg += fabs(pred_reg - curData);								

							cur_data_pos = pred_buffer + i*pred_buffer_block_size*pred_buffer_block_size + (bmi+1)*pred_buffer_block_size + (bmi+1);
							curData = *cur_data_pos;
							pred_sz = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1] - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
							pred_reg = reg_params_pos[0] * (i-1) + reg_params_pos[params_offset_b] * bmi + reg_params_pos[params_offset_c] * bmi + reg_params_pos[params_offset_d];							
							err_sz += fabs(pred_sz - curData) + noise;
							err_reg += fabs(pred_reg - curData);
						}
						// indicator_pos[k] = (err_sz < err_reg);
						indicator_pos[k] = !(err_reg < err_sz);
					}
					reg_params_pos ++;
				}
				indicator_pos += num_z;
			}
		}
	}

	size_t reg_count = 0;
	for(int i=0; i<num_blocks; i++){
		if(!(indicator[i])){
			reg_params[reg_count] = reg_params[i];
			reg_params[reg_count + params_offset_b] = reg_params[i + params_offset_b];
			reg_params[reg_count + params_offset_c] = reg_params[i + params_offset_c];
			reg_params[reg_count + params_offset_d] = reg_params[i + params_offset_d];
			reg_count ++;
		}
	}
	//Compress coefficient arrays
	double precision_a, precision_b, precision_c, precision_d;
	float rel_param_err = 0.025;
	precision_a = rel_param_err * realPrecision / block_size;
	precision_b = rel_param_err * realPrecision / block_size;
	precision_c = rel_param_err * realPrecision / block_size;
	precision_d = rel_param_err * realPrecision;
	float last_coeffcients[4] = {0.0};
	int coeff_intvCapacity_sz = 65536;
	int coeff_intvRadius = coeff_intvCapacity_sz / 2;
	int * coeff_type[4];
	int * coeff_result_type = (int *) malloc(reg_count*4*sizeof(int));
	float * coeff_unpred_data[4];
	float * coeff_unpredictable_data = (float *) malloc(reg_count*4*sizeof(float));
	double precision[4];
	precision[0] = precision_a, precision[1] = precision_b, precision[2] = precision_c, precision[3] = precision_d;
	for(int i=0; i<4; i++){
		coeff_type[i] = coeff_result_type + i * reg_count;
		coeff_unpred_data[i] = coeff_unpredictable_data + i * reg_count;
	}
	int coeff_index = 0;
	unsigned int coeff_unpredictable_count[4] = {0};

	float * reg_params_separte[4];
	for(int i=0; i<4; i++){
		reg_params_separte[i] = reg_params + i * num_blocks;
	}
	for(size_t i=0; i<reg_count; i++){
		// for each coeff
		float cur_coeff;
		double diff, itvNum;
		for(int e=0; e<4; e++){
			cur_coeff = reg_params_separte[e][i];
			diff = cur_coeff - last_coeffcients[e];
			itvNum = fabs(diff)/precision[e] + 1;
			if (itvNum < coeff_intvCapacity_sz){
				if (diff < 0) itvNum = -itvNum;
				coeff_type[e][coeff_index] = (int) (itvNum/2) + coeff_intvRadius;
				last_coeffcients[e] = last_coeffcients[e] + 2 * (coeff_type[e][coeff_index] - coeff_intvRadius) * precision[e];
				//ganrantee compression error against the case of machine-epsilon
				if(fabs(cur_coeff - last_coeffcients[e])>precision[e]){	
					coeff_type[e][coeff_index] = 0;
					last_coeffcients[e] = cur_coeff;	
					coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
				}					
			}
			else{
				coeff_type[e][coeff_index] = 0;
				last_coeffcients[e] = cur_coeff;
				coeff_unpred_data[e][coeff_unpredictable_count[e] ++] = cur_coeff;
			}
			reg_params_separte[e][i] = last_coeffcients[e];
		}
		coeff_index ++;
	}
	// pred & quantization
	int * blockwise_unpred_count = (int *) malloc(num_blocks * sizeof(int));
	int * blockwise_unpred_count_pos = blockwise_unpred_count;
	reg_params_pos = reg_params;
	indicator_pos = indicator;
	if(use_mean){
		int intvCapacity_sz = intvCapacity - 2;
		type = result_type;
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				for(size_t k=0; k<num_z; k++){
					data_pos = oriData + i*block_size * dim0_offset + j*block_size * dim1_offset + k*block_size;
					// add 1 in x, y, z offset
					pred_buffer_pos = pred_buffer + pred_buffer_block_size*pred_buffer_block_size + pred_buffer_block_size + 1;
					block_data_pos_x = data_pos;
					for(int ii=0; ii<block_size; ii++){
						block_data_pos_y = block_data_pos_x;
						for(int jj=0; jj<block_size; jj++){
							block_data_pos_z = block_data_pos_y;
							for(int kk=0; kk<block_size; kk++){
								*pred_buffer_pos = *block_data_pos_z;
								if(k*block_size + kk + 1< r3) block_data_pos_z ++;
								pred_buffer_pos ++;
							}
							// add 1 in z offset
							pred_buffer_pos ++;
							if(j*block_size + jj + 1< r2) block_data_pos_y += dim1_offset;
						}
						// add 1 in y offset
						pred_buffer_pos += pred_buffer_block_size;
						if(i*block_size + ii + 1< r1) block_data_pos_x += dim0_offset;
					}
					if(!(indicator_pos[k])){
						float curData;
						float pred;
						double itvNum;
						double diff;
						size_t index = 0;
						size_t block_unpredictable_count = 0;
						float * cur_data_pos = pred_buffer + pred_buffer_block_size*pred_buffer_block_size + pred_buffer_block_size + 1;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								for(size_t kk=0; kk<block_size; kk++){
									curData = *cur_data_pos;
									pred = reg_params_pos[0] * ii + reg_params_pos[params_offset_b] * jj + reg_params_pos[params_offset_c] * kk + reg_params_pos[params_offset_d];									
									diff = curData - pred;
									itvNum = fabs(diff)/tmp_realPrecision + 1;
									if (itvNum < intvCapacity){
										if (diff < 0) itvNum = -itvNum;
										type[index] = (int) (itvNum/2) + intvRadius;
										pred = pred + 2 * (type[index] - intvRadius) * tmp_realPrecision;
										//ganrantee comporession error against the case of machine-epsilon
										if(fabs(curData - pred)>tmp_realPrecision){	
											type[index] = 0;
											pred = curData;
											unpredictable_data[block_unpredictable_count ++] = curData;
										}		
									}
									else{
										type[index] = 0;
										pred = curData;
										unpredictable_data[block_unpredictable_count ++] = curData;
									}
									index ++;	
									cur_data_pos ++;
								}
								cur_data_pos ++;
							}
							cur_data_pos += pred_buffer_block_size;
						}
						reg_params_pos ++;
						total_unpred += block_unpredictable_count;
						unpredictable_data += block_unpredictable_count;
						*blockwise_unpred_count_pos = block_unpredictable_count;
					}
					else{
						// use SZ
						// SZ predication
						unpredictable_count = 0;
						float * cur_data_pos = pred_buffer + pred_buffer_block_size*pred_buffer_block_size + pred_buffer_block_size + 1;
						float curData;
						float pred3D;
						double itvNum, diff;
						size_t index = 0;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								for(size_t kk=0; kk<block_size; kk++){

									curData = *cur_data_pos;
									if(fabs(curData - mean) <= realPrecision){
										type[index] = 1;
										*cur_data_pos = mean;
									}
									else
									{
										pred3D = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1]
												 - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
										diff = curData - pred3D;
										itvNum = fabs(diff)/realPrecision + 1;
										if (itvNum < intvCapacity_sz){
											if (diff < 0) itvNum = -itvNum;
											type[index] = (int) (itvNum/2) + intvRadius;
											*cur_data_pos = pred3D + 2 * (type[index] - intvRadius) * tmp_realPrecision;
											//ganrantee comporession error against the case of machine-epsilon
											if(fabs(curData - *cur_data_pos)>tmp_realPrecision){	
												type[index] = 0;
												*cur_data_pos = curData;	
												unpredictable_data[unpredictable_count ++] = curData;
											}					
										}
										else{
											type[index] = 0;
											*cur_data_pos = curData;
											unpredictable_data[unpredictable_count ++] = curData;
										}
									}
									index ++;
									cur_data_pos ++;
								}
								cur_data_pos ++;
							}
							cur_data_pos += pred_buffer_block_size;
						}
						total_unpred += unpredictable_count;
						unpredictable_data += unpredictable_count;
						*blockwise_unpred_count_pos = unpredictable_count;
					}// end SZ
					blockwise_unpred_count_pos ++;
					type += block_size * block_size * block_size;
				} // end k
				indicator_pos += num_z;
			}// end j
		}// end i
	}
	else{
		int intvCapacity_sz = intvCapacity - 2;
		type = result_type;
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				for(size_t k=0; k<num_z; k++){
					data_pos = oriData + i*block_size * dim0_offset + j*block_size * dim1_offset + k*block_size;
					// add 1 in x, y, z offset
					pred_buffer_pos = pred_buffer + pred_buffer_block_size*pred_buffer_block_size + pred_buffer_block_size + 1;
					block_data_pos_x = data_pos;
					for(int ii=0; ii<block_size; ii++){
						block_data_pos_y = block_data_pos_x;
						for(int jj=0; jj<block_size; jj++){
							block_data_pos_z = block_data_pos_y;
							for(int kk=0; kk<block_size; kk++){
								*pred_buffer_pos = *block_data_pos_z;
								if(k*block_size + kk +1< r3) block_data_pos_z ++;
								pred_buffer_pos ++;
							}
							// add 1 in z offset
							pred_buffer_pos ++;
							if(j*block_size + jj +1< r2) block_data_pos_y += dim1_offset;
						}
						// add 1 in y offset
						pred_buffer_pos += pred_buffer_block_size;
						if(i*block_size + ii +1< r1) block_data_pos_x += dim0_offset;
					}
					if(!(indicator_pos[k]))
					{
						float curData;
						float pred;
						double itvNum;
						double diff;
						size_t index = 0;
						size_t block_unpredictable_count = 0;
						float * cur_data_pos = pred_buffer + pred_buffer_block_size*pred_buffer_block_size + pred_buffer_block_size + 1;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								for(size_t kk=0; kk<block_size; kk++){
									curData = *cur_data_pos;
									pred = reg_params_pos[0] * ii + reg_params_pos[params_offset_b] * jj + reg_params_pos[params_offset_c] * kk + reg_params_pos[params_offset_d];									
									diff = curData - pred;
									itvNum = fabs(diff)/tmp_realPrecision + 1;
									if (itvNum < intvCapacity){
										if (diff < 0) itvNum = -itvNum;
										type[index] = (int) (itvNum/2) + intvRadius;
										pred = pred + 2 * (type[index] - intvRadius) * tmp_realPrecision;
										//ganrantee comporession error against the case of machine-epsilon
										if(fabs(curData - pred)>tmp_realPrecision){	
											type[index] = 0;
											pred = curData;
											unpredictable_data[block_unpredictable_count ++] = curData;
										}		
									}
									else{
										type[index] = 0;
										pred = curData;
										unpredictable_data[block_unpredictable_count ++] = curData;
									}
									index ++;	
									cur_data_pos ++;
								}
								cur_data_pos ++;
							}
							cur_data_pos += pred_buffer_block_size;
						}
						reg_params_pos ++;
						total_unpred += block_unpredictable_count;
						unpredictable_data += block_unpredictable_count;						
						*blockwise_unpred_count_pos = block_unpredictable_count;
					}
					else{
						// use SZ
						// SZ predication
						unpredictable_count = 0;
						float * cur_data_pos = pred_buffer + pred_buffer_block_size*pred_buffer_block_size + pred_buffer_block_size + 1;
						float curData;
						float pred3D;
						double itvNum, diff;
						size_t index = 0;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								for(size_t kk=0; kk<block_size; kk++){
									curData = *cur_data_pos;
									pred3D = cur_data_pos[-1] + cur_data_pos[-strip_dim1_offset]+ cur_data_pos[-strip_dim0_offset] - cur_data_pos[-strip_dim1_offset - 1]
											 - cur_data_pos[-strip_dim0_offset - 1] - cur_data_pos[-strip_dim0_offset - strip_dim1_offset] + cur_data_pos[-strip_dim0_offset - strip_dim1_offset - 1];
									diff = curData - pred3D;
									itvNum = fabs(diff)/realPrecision + 1;
									if (itvNum < intvCapacity_sz){
										if (diff < 0) itvNum = -itvNum;
										type[index] = (int) (itvNum/2) + intvRadius;
										*cur_data_pos = pred3D + 2 * (type[index] - intvRadius) * tmp_realPrecision;
										//ganrantee comporession error against the case of machine-epsilon
										if(fabs(curData - *cur_data_pos)>tmp_realPrecision){	
											type[index] = 0;
											*cur_data_pos = curData;	
											unpredictable_data[unpredictable_count ++] = curData;
										}					
									}
									else{
										type[index] = 0;
										*cur_data_pos = curData;
										unpredictable_data[unpredictable_count ++] = curData;
									}
									index ++;
									cur_data_pos ++;
								}
								cur_data_pos ++;
							}
							cur_data_pos += pred_buffer_block_size;
						}
						total_unpred += unpredictable_count;
						unpredictable_data += unpredictable_count;
						*blockwise_unpred_count_pos = unpredictable_count;
					}// end SZ	
					blockwise_unpred_count_pos ++;
					type += block_size * block_size * block_size;
				}
				indicator_pos += num_z;
			}
		}
	}	

	free(pred_buffer);
	int stateNum = 2*quantization_intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);

	size_t nodeCount = 0;
	init(huffmanTree, result_type, num_blocks*max_num_block_elements);
	size_t i = 0;
	for (i = 0; i < huffmanTree->stateNum; i++)
		if (huffmanTree->code[i]) nodeCount++; 
	nodeCount = nodeCount*2-1;

	unsigned char *treeBytes;
	unsigned int treeByteSize = convert_HuffTree_to_bytes_anyStates(huffmanTree, nodeCount, &treeBytes);

	unsigned int meta_data_offset = 3 + 1 + MetaDataByteLength;
	// total size 										metadata		  # elements     real precision		intervals	nodeCount		huffman 	 	block index 						unpredicatable count						mean 					 	unpred size 				elements
	unsigned char * result = (unsigned char *) calloc(meta_data_offset + exe_params->SZ_SIZE_TYPE + sizeof(double) + sizeof(int) + sizeof(int) + 5*treeByteSize + 4*num_blocks*sizeof(int)+num_blocks * sizeof(unsigned short) + num_blocks * sizeof(unsigned short) + num_blocks * sizeof(float) + total_unpred * sizeof(float) + num_elements * sizeof(int), 1);
	unsigned char * result_pos = result;
	initRandomAccessBytes(result_pos);
	
	result_pos += meta_data_offset;
	
	sizeToBytes(result_pos,num_elements); //SZ_SIZE_TYPE: 4 or 8
	result_pos += exe_params->SZ_SIZE_TYPE;

	intToBytes_bigEndian(result_pos, block_size);
	result_pos += sizeof(int);
	doubleToBytes(result_pos, realPrecision);
	result_pos += sizeof(double);
	intToBytes_bigEndian(result_pos, quantization_intervals);
	result_pos += sizeof(int);
	intToBytes_bigEndian(result_pos, treeByteSize);
	result_pos += sizeof(int);
	intToBytes_bigEndian(result_pos, nodeCount);
	result_pos += sizeof(int);
	memcpy(result_pos, treeBytes, treeByteSize);
	result_pos += treeByteSize;
	free(treeBytes);

	memcpy(result_pos, &use_mean, sizeof(unsigned char));
	result_pos += sizeof(unsigned char);
	memcpy(result_pos, &mean, sizeof(float));
	result_pos += sizeof(float);
	size_t indicator_size = convertIntArray2ByteArray_fast_1b_to_result(indicator, num_blocks, result_pos);
	result_pos += indicator_size;
	
	//convert the lead/mid/resi to byte stream
	if(reg_count > 0){
		for(int e=0; e<4; e++){
			int stateNum = 2*coeff_intvCapacity_sz;
			HuffmanTree* huffmanTree = createHuffmanTree(stateNum);
			size_t nodeCount = 0;
			init(huffmanTree, coeff_type[e], reg_count);
			size_t i = 0;
			for (i = 0; i < huffmanTree->stateNum; i++)
				if (huffmanTree->code[i]) nodeCount++; 
			nodeCount = nodeCount*2-1;
			unsigned char *treeBytes;
			unsigned int treeByteSize = convert_HuffTree_to_bytes_anyStates(huffmanTree, nodeCount, &treeBytes);
			doubleToBytes(result_pos, precision[e]);
			result_pos += sizeof(double);
			intToBytes_bigEndian(result_pos, coeff_intvRadius);
			result_pos += sizeof(int);
			intToBytes_bigEndian(result_pos, treeByteSize);
			result_pos += sizeof(int);
			intToBytes_bigEndian(result_pos, nodeCount);
			result_pos += sizeof(int);
			memcpy(result_pos, treeBytes, treeByteSize);		
			result_pos += treeByteSize;
			free(treeBytes);
			size_t typeArray_size = 0;
			encode(huffmanTree, coeff_type[e], reg_count, result_pos + sizeof(size_t), &typeArray_size);
			sizeToBytes(result_pos, typeArray_size);
			result_pos += sizeof(size_t) + typeArray_size;
			intToBytes_bigEndian(result_pos, coeff_unpredictable_count[e]);
			result_pos += sizeof(int);
			memcpy(result_pos, coeff_unpred_data[e], coeff_unpredictable_count[e]*sizeof(float));
			result_pos += coeff_unpredictable_count[e]*sizeof(float);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	free(coeff_result_type);
	free(coeff_unpredictable_data);
	
	//record the number of unpredictable data and also store them
	memcpy(result_pos, &total_unpred, sizeof(size_t));
	result_pos += sizeof(size_t);
	// record blockwise unpred data
	size_t compressed_blockwise_unpred_count_size;
	unsigned char * compressed_bw_unpred_count = SZ_compress_args(SZ_INT32, blockwise_unpred_count, &compressed_blockwise_unpred_count_size, ABS, 0.5, 0, 0, 0, 0, 0, 0, num_blocks);
	memcpy(result_pos, &compressed_blockwise_unpred_count_size, sizeof(size_t));
	result_pos += sizeof(size_t);
	memcpy(result_pos, compressed_bw_unpred_count, compressed_blockwise_unpred_count_size);
	result_pos += compressed_blockwise_unpred_count_size;
	free(blockwise_unpred_count);
	free(compressed_bw_unpred_count);
	memcpy(result_pos, result_unpredictable_data, total_unpred * sizeof(float));
	result_pos += total_unpred * sizeof(float);

	free(reg_params);
	free(indicator);
	free(result_unpredictable_data);
	// encode type array by block
	type = result_type;
	size_t total_type_array_size = 0;
	unsigned char * type_array_buffer = (unsigned char *) malloc(num_blocks*max_num_block_elements*sizeof(int));
	unsigned short * type_array_block_size = (unsigned short *) malloc(num_blocks*sizeof(unsigned short));
	unsigned char * type_array_buffer_pos = type_array_buffer;
	unsigned short * type_array_block_size_pos = type_array_block_size;
	for(size_t i=0; i<num_x; i++){
		for(size_t j=0; j<num_y; j++){
			for(size_t k=0; k<num_z; k++){	
				size_t typeArray_size = 0;
				encode(huffmanTree, type, max_num_block_elements, type_array_buffer_pos, &typeArray_size);
				total_type_array_size += typeArray_size;
				*type_array_block_size_pos = typeArray_size;
				type_array_buffer_pos += typeArray_size;
				type += max_num_block_elements;
				type_array_block_size_pos ++;
			}
		}
	}
	size_t compressed_type_array_block_size;
	unsigned char * compressed_type_array_block = SZ_compress_args(SZ_UINT16, type_array_block_size, &compressed_type_array_block_size, ABS, 0.5, 0, 0, 0, 0, 0, 0, num_blocks);
	memcpy(result_pos, &compressed_type_array_block_size, sizeof(size_t));
	result_pos += sizeof(size_t);
	memcpy(result_pos, compressed_type_array_block, compressed_type_array_block_size);
	result_pos += compressed_type_array_block_size;
	memcpy(result_pos, type_array_buffer, total_type_array_size);
	result_pos += total_type_array_size;
	// size_t typeArray_size = 0;
	// encode(huffmanTree, result_type, num_blocks*max_num_block_elements, result_pos, &typeArray_size);
	// result_pos += typeArray_size;

	free(compressed_type_array_block);
	free(type_array_buffer);
	free(type_array_block_size);
	size_t totalEncodeSize = result_pos - result;
	free(result_type);
	SZ_ReleaseHuffman(huffmanTree);
	*comp_size = totalEncodeSize;
	return result;
}
