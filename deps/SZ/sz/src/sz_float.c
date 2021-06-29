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

# define MIN(_a, _b) (((_a) < (_b)) ? (_a) : (_b))


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
		confparams_cpr->errorBoundMode = SZ_ABS;
		realPrecision = confparams_cpr->absErrBound = computeABSErrBoundFromPSNR(confparams_cpr->psnr, (double)confparams_cpr->predThreshold, (double)valueRangeSize);
		//printf("realPrecision=%lf\n", realPrecision);
	}
	else if(confparams_cpr->errorBoundMode==NORM) //norm error = sqrt(sum((xi-xi_)^2))
	{
		confparams_cpr->errorBoundMode = SZ_ABS;
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
		{
			printf("Error: doesn't support 5 dimensions for now.\n");
			status = SZ_DERR; //dimension error
		}

		//
		//Call Zstd or Gzip to do the further compression.
		//
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
	unsigned char * compressed_bw_unpred_count = SZ_compress_args(SZ_INT32, blockwise_unpred_count, &compressed_blockwise_unpred_count_size, SZ_ABS, 0.5, 0, 0, 0, 0, 0, 0, num_blocks);
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
	unsigned char * compressed_type_array_block = SZ_compress_args(SZ_UINT16, type_array_block_size, &compressed_type_array_block_size, SZ_ABS, 0.5, 0, 0, 0, 0, 0, 0, num_blocks);
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
