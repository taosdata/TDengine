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
#include <unistd.h>
#include <math.h>
#include "sz.h"
#include "CompressElement.h"
#include "DynamicByteArray.h"
#include "DynamicIntArray.h"
#include "TightDataPointStorageD.h"
#include "sz_double.h"
#include "sz_double_pwr.h"
#include "szd_double.h"
#include "szd_double_pwr.h"
#include "zlib.h"
#include "rw.h"
#include "sz_double_ts.h"
#include "utility.h"
#include "CacheTable.h"
#include "MultiLevelCacheTableWideInterval.h"
#include "sz_stats.h"

unsigned char* SZ_skip_compress_double(double* data, size_t dataLength, size_t* outSize)
{
	*outSize = dataLength*sizeof(double);
	unsigned char* out = (unsigned char*)malloc(dataLength*sizeof(double));
	memcpy(out, data, dataLength*sizeof(double));
	return out;
}

inline void computeReqLength_double(double realPrecision, short radExpo, int* reqLength, double* medianValue)
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

inline short computeReqLength_double_MSST19(double realPrecision)
{
	short reqExpo = getPrecisionReqLength_double(realPrecision);
	return 12-reqExpo;
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

unsigned int optimize_intervals_double_2D(double *oriData, size_t r1, size_t r2, double realPrecision)
{	
	size_t i,j, index;
	size_t radiusIndex;
	double pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = (r1-1)*(r2-1)/confparams_cpr->sampleDistance;
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
	//printf("confparams_cpr->maxRangeRadius = %d, accIntervals=%d, powerOf2=%d\n", confparams_cpr->maxRangeRadius, accIntervals, powerOf2);

	if(powerOf2<32)
		powerOf2 = 32;

	free(intervals);
	return powerOf2;
}

unsigned int optimize_intervals_double_3D(double *oriData, size_t r1, size_t r2, size_t r3, double realPrecision)
{	
	size_t i,j,k, index;
	size_t radiusIndex;
	size_t r23=r2*r3;
	double pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = (r1-1)*(r2-1)*(r3-1)/confparams_cpr->sampleDistance;
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
	//printf("confparams_cpr->maxRangeRadius = %d, accIntervals=%d, powerOf2=%d\n", confparams_cpr->maxRangeRadius, accIntervals, powerOf2);
	return powerOf2;
}

unsigned int optimize_intervals_double_4D(double *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision)
{
	size_t i,j,k,l, index;
	size_t radiusIndex;
	size_t r234=r2*r3*r4;
	size_t r34=r3*r4;
	double pred_value = 0, pred_err;
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

TightDataPointStorageD* SZ_compress_double_1D_MDQ(double *oriData, 
size_t dataLength, double realPrecision, double valueRangeSize, double medianValue_d)
{
#ifdef HAVE_TIMECMPR
	double* decData = NULL;	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData = (double*)(multisteps->hist_data);
#endif	
	
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
#ifdef HAVE_TIMECMPR	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData[0] = vce->data;
#endif		
		
	//add the second data
	type[1] = 0;
	compressSingleDoubleValue(vce, spaceFillingValue[1], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Double(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,8);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	listAdd_double(last3CmprsData, vce->data);
#ifdef HAVE_TIMECMPR	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData[1] = vce->data;
#endif
	int state;
	double checkRadius;
	double curData;
	double pred = last3CmprsData[0];
	double predAbsErr;
	checkRadius = (quantization_intervals-1)*realPrecision;
	double interval = 2*realPrecision;

	double recip_realPrecision = 1/realPrecision;
	for(i=2;i<dataLength;i++)
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
#ifdef HAVE_TIMECMPR					
			if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
				decData[i] = pred;			
#endif	
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
		
#ifdef HAVE_TIMECMPR
		if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
			decData[i] = vce->data;
#endif	
		
	}//end of for
		
	size_t exactDataNum = exactLeadNumArray->size;
	
	TightDataPointStorageD* tdps;
			
	new_TightDataPointStorageD(&tdps, dataLength, exactDataNum, 
			type, exactMidByteArray->array, exactMidByteArray->size,  
			exactLeadNumArray->array,  
			resiBitArray->array, resiBitArray->size, 
			resiBitsLength, 
			realPrecision, medianValue, (char)reqLength, quantization_intervals, NULL, 0, 0);
	
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

void SZ_compress_args_double_StoreOriData(double* oriData, size_t dataLength, unsigned char** newByteData, size_t *outSize)
{	
	int doubleSize = sizeof(double);
	size_t k = 0, i;
	size_t totalByteLength = 3 + MetaDataByteLength_double + exe_params->SZ_SIZE_TYPE + 1 + doubleSize*dataLength;
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
	k = k + MetaDataByteLength_double;

	sizeToBytes(dsLengthBytes,dataLength);
	for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)//ST: 4 or 8
		(*newByteData)[k++] = dsLengthBytes[i];

	if(sysEndianType==BIG_ENDIAN_SYSTEM)
		memcpy((*newByteData)+4+MetaDataByteLength_double+exe_params->SZ_SIZE_TYPE, oriData, dataLength*doubleSize);
	else
	{
		unsigned char* p = (*newByteData)+4+MetaDataByteLength_double+exe_params->SZ_SIZE_TYPE;
		for(i=0;i<dataLength;i++,p+=doubleSize)
			doubleToBytes(p, oriData[i]);
	}
	*outSize = totalByteLength;
}


char SZ_compress_args_double_NoCkRngeNoGzip_1D(int cmprType, unsigned char** newByteData, double *oriData, 
size_t dataLength, double realPrecision, size_t *outSize, double valueRangeSize, double medianValue_d)
{
	char compressionType = 0;	
	TightDataPointStorageD* tdps = NULL; 	
#ifdef HAVE_TIMECMPR
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
	{
		int timestep = sz_tsc->currentStep;
		if(cmprType == SZ_PERIO_TEMPORAL_COMPRESSION)
		{
			if(timestep % confparams_cpr->snapshotCmprStep != 0)
			{
				tdps = SZ_compress_double_1D_MDQ_ts(oriData, dataLength, multisteps, realPrecision, valueRangeSize, medianValue_d);
				compressionType = 1; //time-series based compression 
			}
			else
			{	
				tdps = SZ_compress_double_1D_MDQ(oriData, dataLength, realPrecision, valueRangeSize, medianValue_d);
				compressionType = 0; //snapshot-based compression
				multisteps->lastSnapshotStep = timestep;
			}					
		}
		else if(cmprType == SZ_FORCE_SNAPSHOT_COMPRESSION)
		{
			tdps = SZ_compress_double_1D_MDQ(oriData, dataLength, realPrecision, valueRangeSize, medianValue_d);
			compressionType = 0; //snapshot-based compression
			multisteps->lastSnapshotStep = timestep;			
		}
		else if(cmprType == SZ_FORCE_TEMPORAL_COMPRESSION)
		{
			tdps = SZ_compress_double_1D_MDQ_ts(oriData, dataLength, multisteps, realPrecision, valueRangeSize, medianValue_d);
			compressionType = 1; //time-series based compression 			
		}

	}
	else
#endif
		tdps = SZ_compress_double_1D_MDQ(oriData, dataLength, realPrecision, valueRangeSize, medianValue_d);			
	
	convertTDPStoFlatBytes_double(tdps, newByteData, outSize);
	
	if(*outSize>3 + MetaDataByteLength_double + exe_params->SZ_SIZE_TYPE + 1 + sizeof(double)*dataLength)
		SZ_compress_args_double_StoreOriData(oriData, dataLength, newByteData, outSize);
	
	free_TightDataPointStorageD(tdps);	
	return compressionType;
}

/*MSST19*/
TightDataPointStorageD* SZ_compress_double_1D_MDQ_MSST19(double *oriData, 
size_t dataLength, double realPrecision, double valueRangeSize, double medianValue_f)
{
#ifdef HAVE_TIMECMPR	
	double* decData = NULL;
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData = (double*)(multisteps->hist_data);
#endif	

	//struct ClockPoint clockPointBuild;
	//TimeDurationStart("build", &clockPointBuild);
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
		quantization_intervals = optimize_intervals_double_1D_opt_MSST19(oriData, dataLength, realPrecision);
	else
		quantization_intervals = exe_params->intvCapacity;
	//updateQuantizationInfo(quantization_intervals);
	int intvRadius = quantization_intervals/2;
	
	double* precisionTable = (double*)malloc(sizeof(double) * quantization_intervals);
	double inv = 2.0-pow(2, -(confparams_cpr->plus_bits));
    for(int i=0; i<quantization_intervals; i++){
        double test = pow((1+realPrecision), inv*(i - intvRadius));
        precisionTable[i] = test;
    }
    
	struct TopLevelTableWideInterval levelTable;
    MultiLevelCacheTableWideIntervalBuild(&levelTable, precisionTable, quantization_intervals, realPrecision, confparams_cpr->plus_bits);

	size_t i;
	int reqLength;
	double medianValue = medianValue_f;
	//double medianInverse = 1 / medianValue_f;
	//short radExpo = getExponent_double(realPrecision);
	
	reqLength = computeReqLength_double_MSST19(realPrecision);	

	int* type = (int*) malloc(dataLength*sizeof(int));
		
	double* spaceFillingValue = oriData; //
	
	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, dataLength/2/8);
	
	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, dataLength/2);
	
	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);
	
	unsigned char preDataBytes[8];
	intToBytes_bigEndian(preDataBytes, 0);
	
	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;
	double last3CmprsData[3] = {0};

	//size_t miss=0, hit=0;

	DoubleValueCompressElement *vce = (DoubleValueCompressElement*)malloc(sizeof(DoubleValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));
				
	//add the first data	
	type[0] = 0;
	compressSingleDoubleValue_MSST19(vce, spaceFillingValue[0], realPrecision, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Double(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,8);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	listAdd_double(last3CmprsData, vce->data);
	//miss++;
#ifdef HAVE_TIMECMPR	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData[0] = vce->data;
#endif		
		
	//add the second data
	type[1] = 0;
	compressSingleDoubleValue_MSST19(vce, spaceFillingValue[1], realPrecision, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Double(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,8);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	listAdd_double(last3CmprsData, vce->data);
	//miss++;
#ifdef HAVE_TIMECMPR	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
		decData[1] = vce->data;
#endif
	int state;
	//double checkRadius;
	double curData;
	double pred = vce->data;

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
		compressSingleDoubleValue_MSST19(vce, curData, realPrecision, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Double(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,8);
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
	
	TightDataPointStorageD* tdps;
			
	new_TightDataPointStorageD(&tdps, dataLength, exactDataNum, 
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


void SZ_compress_args_double_withinRange(unsigned char** newByteData, double *oriData, size_t dataLength, size_t *outSize)
{
	TightDataPointStorageD* tdps = (TightDataPointStorageD*) malloc(sizeof(TightDataPointStorageD));
	tdps->rtypeArray = NULL;
	tdps->typeArray = NULL;
	tdps->leadNumArray = NULL;
	tdps->residualMidBits = NULL;
	
	tdps->allSameData = 1;
	tdps->dataSeriesLength = dataLength;
	tdps->exactMidBytes = (unsigned char*)malloc(sizeof(unsigned char)*8);
	tdps->pwrErrBoundBytes = NULL;
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

int SZ_compress_args_double(int cmprType, int withRegression, unsigned char** newByteData, double *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio, double pwRelBoundRatio)
{
	confparams_cpr->errorBoundMode = errBoundMode;
	if(errBoundMode==PW_REL)
	{
		confparams_cpr->pw_relBoundRatio = pwRelBoundRatio;	
	}
		
	int status = SZ_SCES;
	size_t dataLength = computeDataLength(r5,r4,r3,r2,r1);
	
	if(dataLength <= MIN_NUM_OF_ELEMENTS)
	{
		*newByteData = SZ_skip_compress_double(oriData, dataLength, outSize);
		return status;
	}
	
	double valueRangeSize = 0, medianValue = 0;
	
	unsigned char * signs = NULL;
	bool positive = true;
	double nearZero = 0.0;
	double min = 0;
	if(pwRelBoundRatio < 0.000009999)
		confparams_cpr->accelerate_pw_rel_compression = 0;
		
	if(confparams_cpr->errorBoundMode == PW_REL && confparams_cpr->accelerate_pw_rel_compression == 1)
	{
		signs = (unsigned char *) malloc(dataLength);
		memset(signs, 0, dataLength);
		min = computeRangeSize_double_MSST19(oriData, dataLength, &valueRangeSize, &medianValue, signs, &positive, &nearZero);
	}
	else
		min = computeRangeSize_double(oriData, dataLength, &valueRangeSize, &medianValue);	
	double max = min+valueRangeSize;
	confparams_cpr->dmin = min;
	confparams_cpr->dmax = max;
	
	double realPrecision = 0; 
	
	if(confparams_cpr->errorBoundMode==PSNR)
	{
		confparams_cpr->errorBoundMode = ABS;
		realPrecision = confparams_cpr->absErrBound = computeABSErrBoundFromPSNR(confparams_cpr->psnr, (double)confparams_cpr->predThreshold, valueRangeSize);
	}
	else if(confparams_cpr->errorBoundMode==NORM) //norm error = sqrt(sum((xi-xi_)^2))
	{
		confparams_cpr->errorBoundMode = ABS;
		realPrecision = confparams_cpr->absErrBound = computeABSErrBoundFromNORM_ERR(confparams_cpr->normErr, dataLength);
		//printf("realPrecision=%lf\n", realPrecision);				
	}	
	else
	{
		realPrecision = getRealPrecision_double(valueRangeSize, errBoundMode, absErr_Bound, relBoundRatio, &status);
		confparams_cpr->absErrBound = realPrecision;
	}	
	if(valueRangeSize <= realPrecision)
	{
		if(confparams_cpr->errorBoundMode>=PW_REL && confparams_cpr->accelerate_pw_rel_compression == 1)
			free(signs);		
		SZ_compress_args_double_withinRange(newByteData, oriData, dataLength, outSize);
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
					SZ_compress_args_double_NoCkRngeNoGzip_1D_pwr_pre_log_MSST19(&tmpByteData, oriData, pwRelBoundRatio, r1, &tmpOutSize, valueRangeSize, medianValue, signs, &positive, min, max, nearZero);
				else
					SZ_compress_args_double_NoCkRngeNoGzip_1D_pwr_pre_log(&tmpByteData, oriData, pwRelBoundRatio, r1, &tmpOutSize, min, max);
					//SZ_compress_args_double_NoCkRngeNoGzip_1D_pwrgroup(&tmpByteData, oriData, r1, absErr_Bound, relBoundRatio, pwRelBoundRatio, valueRangeSize, medianValue, &tmpOutSize);
			}
			else
#ifdef HAVE_TIMECMPR
				if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
					multisteps->compressionType = SZ_compress_args_double_NoCkRngeNoGzip_1D(cmprType, &tmpByteData, oriData, r1, realPrecision, &tmpOutSize, valueRangeSize, medianValue);
				else
#endif
					{
						SZ_compress_args_double_NoCkRngeNoGzip_1D(cmprType, &tmpByteData, oriData, r1, realPrecision, &tmpOutSize, valueRangeSize, medianValue);
						if(tmpOutSize>=dataLength*sizeof(double) + 3 + MetaDataByteLength_double + exe_params->SZ_SIZE_TYPE + 1)
							SZ_compress_args_double_StoreOriData(oriData, dataLength, &tmpByteData, &tmpOutSize);
					}
		}
		else
		{
			printf("Error: doesn't support 5 dimensions for now.\n");
			status = SZ_DERR;
		}

		//		
		//Call Gzip to do the further compression.
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
			printf("Error: Wrong setting of confparams_cpr->szMode in the double compression.\n");
			status = SZ_MERR;	
		}
	}

	return status;
}

//TODO
int SZ_compress_args_double_subblock(unsigned char* compressedBytes, double *oriData,
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1,
size_t s5, size_t s4, size_t s3, size_t s2, size_t s1,
size_t e5, size_t e4, size_t e3, size_t e2, size_t e1,
size_t *outSize, int errBoundMode, double absErr_Bound, double relBoundRatio)
{
	int status = SZ_SCES;
	double valueRangeSize = 0, medianValue = 0;
	computeRangeSize_double_subblock(oriData, &valueRangeSize, &medianValue, r5, r4, r3, r2, r1, s5, s4, s3, s2, s1, e5, e4, e3, e2, e1);

	double realPrecision = getRealPrecision_double(valueRangeSize, errBoundMode, absErr_Bound, relBoundRatio, &status);

	if(valueRangeSize <= realPrecision)
	{
		//TODO
		//SZ_compress_args_double_withinRange_subblock();
	}
	else
	{
		if (r2==0)
		{
			//TODO
			if(errBoundMode==PW_REL)
			{
				//TODO
				//SZ_compress_args_double_NoCkRngeNoGzip_1D_pwr_subblock();
				printf ("Current subblock version does not support point-wise relative error bound.\n");
			}
			else
				SZ_compress_args_double_NoCkRnge_1D_subblock(compressedBytes, oriData, realPrecision, outSize, valueRangeSize, medianValue, r1, s1, e1);
		}
		else
		{
			printf("Error: doesn't support 5 dimensions for now.\n");
			status = SZ_DERR; //dimension error
		}
	}
	return status;
}

void SZ_compress_args_double_NoCkRnge_1D_subblock(unsigned char* compressedBytes, double *oriData, double realPrecision, size_t *outSize, double valueRangeSize, double medianValue_d,
size_t r1, size_t s1, size_t e1)
{
	TightDataPointStorageD* tdps = SZ_compress_double_1D_MDQ_subblock(oriData, realPrecision, valueRangeSize, medianValue_d, r1, s1, e1);

	if (confparams_cpr->szMode==SZ_BEST_SPEED)
		convertTDPStoFlatBytes_double_args(tdps, compressedBytes, outSize);
	else if(confparams_cpr->szMode==SZ_BEST_COMPRESSION || confparams_cpr->szMode==SZ_DEFAULT_COMPRESSION)
	{
		unsigned char *tmpCompBytes;
		size_t tmpOutSize;
		convertTDPStoFlatBytes_double(tdps, &tmpCompBytes, &tmpOutSize);
		*outSize = zlib_compress3(tmpCompBytes, tmpOutSize, compressedBytes, confparams_cpr->gzipMode);
		free(tmpCompBytes);
	}
	else
	{
		printf ("Error: Wrong setting of confparams_cpr->szMode in the double compression.\n");
	}

	//TODO
//	if(*outSize>dataLength*sizeof(double))
//		SZ_compress_args_double_StoreOriData(oriData, dataLength, newByteData, outSize);

	free_TightDataPointStorageD(tdps);
}

unsigned int optimize_intervals_double_1D_subblock(double *oriData, double realPrecision, size_t r1, size_t s1, size_t e1)
{
	size_t dataLength = e1 - s1 + 1;
	oriData = oriData + s1;

	size_t i = 0;
	unsigned long radiusIndex;
	double pred_value = 0, pred_err;
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
	return powerOf2;
}


TightDataPointStorageD* SZ_compress_double_1D_MDQ_subblock(double *oriData, double realPrecision, double valueRangeSize, double medianValue_d,
size_t r1, size_t s1, size_t e1)
{
	size_t dataLength = e1 - s1 + 1;

	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
		quantization_intervals = optimize_intervals_double_1D_subblock(oriData, realPrecision, r1, s1, e1);
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

	double* spaceFillingValue = oriData + s1; //

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
	double last3CmprsData[3] = {0};

	DoubleValueCompressElement *vce = (DoubleValueCompressElement*)malloc(sizeof(DoubleValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));

	//add the first data
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
	double pred;
	double predAbsErr;
	checkRadius = (quantization_intervals-1)*realPrecision;
	double interval = 2*realPrecision;

	for(i=2;i<dataLength;i++)
	{
		//printf("%.30G\n",last3CmprsData[0]);
		curData = spaceFillingValue[i];
		pred = 2*last3CmprsData[0] - last3CmprsData[1];
		//pred = last3CmprsData[0];
		predAbsErr = fabs(curData - pred);
		if(predAbsErr<=checkRadius)
		{
			state = (predAbsErr/realPrecision+1)/2;
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
			listAdd_double(last3CmprsData, pred);
			continue;
		}

		//unpredictable data processing
		type[i] = 0;
		compressSingleDoubleValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Double(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,8);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);

		listAdd_double(last3CmprsData, vce->data);
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

	return tdps;
}


unsigned int optimize_intervals_double_1D_opt_MSST19(double *oriData, size_t dataLength, double realPrecision)
{	
	size_t i = 0, radiusIndex;
	double pred_value = 0;
	double pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = 0;//dataLength/confparams_cpr->sampleDistance;

	double * data_pos = oriData + 2;
	double divider = log2(1+realPrecision)*2;
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
	
	if(powerOf2<64)
		powerOf2 = 64;
	
	free(intervals);
	return powerOf2;
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
