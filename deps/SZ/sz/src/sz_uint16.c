/**
 *  @file sz_uint16.c
 *  @author Sheng Di
 *  @date Aug, 2017
 *  @brief sz_uint16, Compression and Decompression functions
 *  (C) 2017 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
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
#include "zlib.h"
#include "rw.h"
#include "TightDataPointStorageI.h"
#include "sz_uint16.h"
#include "utility.h"

unsigned int optimize_intervals_uint16_1D(uint16_t *oriData, size_t dataLength, double realPrecision)
{	
	size_t i = 0, radiusIndex;
	int64_t pred_value = 0, pred_err;
	size_t *intervals = (size_t*)malloc(confparams_cpr->maxRangeRadius*sizeof(size_t));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(size_t));
	size_t totalSampleSize = dataLength/confparams_cpr->sampleDistance;
	for(i=2;i<dataLength;i++)
	{
		if(i%confparams_cpr->sampleDistance==0)
		{
			//pred_value = 2*oriData[i-1] - oriData[i-2];
			pred_value = oriData[i-1];
			pred_err = llabs(pred_value - oriData[i]);
			radiusIndex = (uint64_t)((pred_err/realPrecision+1)/2);
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

unsigned int optimize_intervals_uint16_2D(uint16_t *oriData, size_t r1, size_t r2, double realPrecision)
{	
	size_t i,j, index;
	size_t radiusIndex;
	int64_t pred_value = 0, pred_err;
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
				pred_err = llabs(pred_value - oriData[index]);
				radiusIndex = (uint64_t)((pred_err/realPrecision+1)/2);
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

unsigned int optimize_intervals_uint16_3D(uint16_t *oriData, size_t r1, size_t r2, size_t r3, double realPrecision)
{	
	size_t i,j,k, index;
	size_t radiusIndex;
	size_t r23=r2*r3;
	int64_t pred_value = 0, pred_err;
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
					pred_err = llabs(pred_value - oriData[index]);
					radiusIndex = (uint64_t)((pred_err/realPrecision+1)/2);
					if(radiusIndex>=confparams_cpr->maxRangeRadius)
					{
						radiusIndex = confparams_cpr->maxRangeRadius - 1;
						//printf("radiusIndex=%d\n", radiusIndex);
					}
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
	//printf("targetCount=%d, sum=%d, totalSampleSize=%d, ratio=%f, accIntervals=%d, powerOf2=%d\n", targetCount, sum, totalSampleSize, (double)sum/(double)totalSampleSize, accIntervals, powerOf2);
	return powerOf2;
}


unsigned int optimize_intervals_uint16_4D(uint16_t *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision)
{
	size_t i,j,k,l, index;
	size_t radiusIndex;
	size_t r234=r2*r3*r4;
	size_t r34=r3*r4;
	int64_t pred_value = 0, pred_err;
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
						pred_err = llabs(pred_value - oriData[index]);
						radiusIndex = (uint64_t)((pred_err/realPrecision+1)/2);
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

TightDataPointStorageI* SZ_compress_uint16_1D_MDQ(uint16_t *oriData, size_t dataLength, double realPrecision, int64_t valueRangeSize, int64_t minValue)
{
	unsigned char bytes[8] = {0,0,0,0,0,0,0,0};
	int byteSize = computeByteSizePerIntValue(valueRangeSize);
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
		quantization_intervals = optimize_intervals_uint16_1D(oriData, dataLength, realPrecision);
	else
		quantization_intervals = exe_params->intvCapacity;
	updateQuantizationInfo(quantization_intervals);	
	size_t i;

	int* type = (int*) malloc(dataLength*sizeof(int));
		
	uint16_t* spaceFillingValue = oriData; //
	
	DynamicByteArray *exactDataByteArray;
	new_DBA(&exactDataByteArray, DynArrayInitLen);
		
	int64_t last3CmprsData[3] = {0,0,0};
				
	//add the first data	
	type[0] = 0;
	compressUInt16Value(spaceFillingValue[0], minValue, byteSize, bytes);
	memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
	listAdd_int(last3CmprsData, spaceFillingValue[0]);
		
	type[1] = 0;
	compressUInt16Value(spaceFillingValue[1], minValue, byteSize, bytes);
	memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
	listAdd_int(last3CmprsData, spaceFillingValue[1]);
	//printf("%.30G\n",last3CmprsData[0]);	
	
	int state;
	double checkRadius = (exe_params->intvCapacity-1)*realPrecision;
	int64_t curData;
	int64_t pred, predAbsErr;
	double interval = 2*realPrecision;
	
	for(i=2;i<dataLength;i++)
	{
		curData = spaceFillingValue[i];
		//pred = 2*last3CmprsData[0] - last3CmprsData[1];
		pred = last3CmprsData[0];
		predAbsErr = llabs(curData - pred);	
		if(predAbsErr<checkRadius)
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
			if(pred>SZ_UINT16_MAX) pred = SZ_UINT16_MAX;
			if(pred<SZ_UINT16_MIN) pred = SZ_UINT16_MIN;			
			listAdd_int(last3CmprsData, pred);					
			continue;
		}
		
		//unpredictable data processing		
		type[i] = 0;
		compressUInt16Value(curData, minValue, byteSize, bytes);
		memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
		listAdd_int(last3CmprsData, curData);
	}//end of for
		
	size_t exactDataNum = exactDataByteArray->size / byteSize;
	
	TightDataPointStorageI* tdps;	
			
	new_TightDataPointStorageI(&tdps, dataLength, exactDataNum, byteSize, 
			type, exactDataByteArray->array, exactDataByteArray->size,  
			realPrecision, minValue, quantization_intervals, SZ_UINT16);

//sdi:Debug
/*	int sum =0;
	for(i=0;i<dataLength;i++)
		if(type[i]==0) sum++;
	printf("opt_quantizations=%d, exactDataNum=%d, sum=%d\n",quantization_intervals, exactDataNum, sum);*/
	
	//free memory
	free(type);	
	free(exactDataByteArray); //exactDataByteArray->array has been released in free_TightDataPointStorageF(tdps);
	
	return tdps;
}

void SZ_compress_args_uint16_StoreOriData(uint16_t* oriData, size_t dataLength, TightDataPointStorageI* tdps, 
unsigned char** newByteData, size_t *outSize)
{
	int intSize=sizeof(uint16_t);	
	size_t k = 0, i;
	tdps->isLossless = 1;
	size_t totalByteLength = 3 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1 + intSize*dataLength;
	*newByteData = (unsigned char*)malloc(totalByteLength);
	
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
		memcpy((*newByteData)+4+MetaDataByteLength+exe_params->SZ_SIZE_TYPE, oriData, dataLength*intSize);
	else
	{
		unsigned char* p = (*newByteData)+4+MetaDataByteLength+exe_params->SZ_SIZE_TYPE;
		for(i=0;i<dataLength;i++,p+=intSize)
			int16ToBytes_bigEndian(p, oriData[i]);
	}	
	*outSize = totalByteLength;
}

void SZ_compress_args_uint16_NoCkRngeNoGzip_1D(unsigned char** newByteData, uint16_t *oriData, 
size_t dataLength, double realPrecision, size_t *outSize, int64_t valueRangeSize, uint16_t minValue)
{
	TightDataPointStorageI* tdps = SZ_compress_uint16_1D_MDQ(oriData, dataLength, realPrecision, valueRangeSize, minValue);
	//TODO: return bytes....
	convertTDPStoFlatBytes_int(tdps, newByteData, outSize);
	if(*outSize > dataLength*sizeof(uint16_t))
		SZ_compress_args_uint16_StoreOriData(oriData, dataLength+2, tdps, newByteData, outSize);
	free_TightDataPointStorageI(tdps);
}

TightDataPointStorageI* SZ_compress_uint16_2D_MDQ(uint16_t *oriData, size_t r1, size_t r2, double realPrecision, int64_t valueRangeSize, int64_t minValue)
{
	unsigned char bytes[8] = {0,0,0,0,0,0,0,0};
	int byteSize = computeByteSizePerIntValue(valueRangeSize);
	
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_uint16_2D(oriData, r1, r2, realPrecision);
		updateQuantizationInfo(quantization_intervals);
	}	
	else
		quantization_intervals = exe_params->intvCapacity;
	size_t i,j; 
	int64_t pred1D, pred2D, curValue, tmp;
	int diff = 0.0;
	double itvNum = 0;
	uint16_t *P0, *P1;
		
	size_t dataLength = r1*r2;	
	
	P0 = (uint16_t*)malloc(r2*sizeof(uint16_t));
	memset(P0, 0, r2*sizeof(uint16_t));
	P1 = (uint16_t*)malloc(r2*sizeof(uint16_t));
	memset(P1, 0, r2*sizeof(uint16_t));
		
	int* type = (int*) malloc(dataLength*sizeof(int));
	//type[dataLength]=0;
		
	uint16_t* spaceFillingValue = oriData; //
	
	DynamicByteArray *exactDataByteArray;
	new_DBA(&exactDataByteArray, DynArrayInitLen);	

	type[0] = 0;
	curValue = P1[0] = spaceFillingValue[0];
	compressUInt16Value(curValue, minValue, byteSize, bytes);
	memcpyDBA_Data(exactDataByteArray, bytes, byteSize);

	/* Process Row-0 data 1*/
	pred1D = P1[0];
	diff = spaceFillingValue[1] - pred1D;

	itvNum =  llabs(diff)/realPrecision + 1;

	if (itvNum < exe_params->intvCapacity)
	{
		if (diff < 0) itvNum = -itvNum;
		type[1] = (int) (itvNum/2) + exe_params->intvRadius;
		tmp = pred1D + 2 * (type[1] - exe_params->intvRadius) * realPrecision;
		if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
			P1[1] = tmp;
		else if(tmp < SZ_UINT16_MIN)
			P1[1] = SZ_UINT16_MIN;
		else
			P1[1] = SZ_UINT16_MAX;
	}
	else
	{
		type[1] = 0;
		curValue = P1[1] = spaceFillingValue[1];
		compressUInt16Value(curValue, minValue, byteSize, bytes);
		memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
	}

    /* Process Row-0 data 2 --> data r2-1 */
	for (j = 2; j < r2; j++)
	{
		pred1D = 2*P1[j-1] - P1[j-2];
		diff = spaceFillingValue[j] - pred1D;

		itvNum = llabs(diff)/realPrecision + 1;

		if (itvNum < exe_params->intvCapacity)
		{
			if (diff < 0) itvNum = -itvNum;
			type[j] = (int) (itvNum/2) + exe_params->intvRadius;
			tmp = pred1D + 2 * (type[j] - exe_params->intvRadius) * realPrecision;
			if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
				P1[j] = tmp;
			else if(tmp < SZ_UINT16_MIN)
				P1[j] = SZ_UINT16_MIN;
			else
				P1[j] = SZ_UINT16_MAX;			
		}
		else
		{
			type[j] = 0;
			curValue = P1[j] = spaceFillingValue[j];
			compressUInt16Value(curValue, minValue, byteSize, bytes);
			memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
		}
	}

	/* Process Row-1 --> Row-r1-1 */
	size_t index;
	for (i = 1; i < r1; i++)
	{	
		/* Process row-i data 0 */
		index = i*r2;
		pred1D = P1[0];
		diff = spaceFillingValue[index] - pred1D;

		itvNum = llabs(diff)/realPrecision + 1;

		if (itvNum < exe_params->intvCapacity)
		{
			if (diff < 0) itvNum = -itvNum;
			type[index] = (int) (itvNum/2) + exe_params->intvRadius;
			tmp = pred1D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
			if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
				P0[0] = tmp;
			else if(tmp < SZ_UINT16_MIN)
				P0[0] = SZ_UINT16_MIN;
			else
				P0[0] = SZ_UINT16_MAX;			
		}
		else
		{
			type[index] = 0;
			curValue = P0[0] = spaceFillingValue[index];
			compressUInt16Value(curValue, minValue, byteSize, bytes);
			memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
		}
									
		/* Process row-i data 1 --> r2-1*/
		for (j = 1; j < r2; j++)
		{
			index = i*r2+j;
			pred2D = P0[j-1] + P1[j] - P1[j-1];

			diff = spaceFillingValue[index] - pred2D;

			itvNum = llabs(diff)/realPrecision + 1;

			if (itvNum < exe_params->intvCapacity)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + exe_params->intvRadius;
				tmp = pred2D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
				if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
					P0[j] = tmp;
				else if(tmp < SZ_UINT16_MIN)
					P0[j] = SZ_UINT16_MIN;
				else
					P0[j] = SZ_UINT16_MAX;						
			}
			else
			{
				type[index] = 0;
				curValue = P0[j] = spaceFillingValue[index];
				compressUInt16Value(curValue, minValue, byteSize, bytes);
				memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
			}
		}

		uint16_t *Pt;
		Pt = P1;
		P1 = P0;
		P0 = Pt;
	}
	
	if(r2!=1)
		free(P0);
	free(P1);			
	
	size_t exactDataNum = exactDataByteArray->size;
	
	TightDataPointStorageI* tdps;	
			
	new_TightDataPointStorageI(&tdps, dataLength, exactDataNum, byteSize, 
			type, exactDataByteArray->array, exactDataByteArray->size,  
			realPrecision, minValue, quantization_intervals, SZ_UINT16);
			
	//free memory
	free(type);	
	free(exactDataByteArray); //exactDataByteArray->array has been released in free_TightDataPointStorageF(tdps);
	
	return tdps;	
}

/**
 * 
 * Note: @r1 is high dimension
 * 		 @r2 is low dimension 
 * */
void SZ_compress_args_uint16_NoCkRngeNoGzip_2D(unsigned char** newByteData, uint16_t *oriData, size_t r1, size_t r2, double realPrecision, size_t *outSize, 
int64_t valueRangeSize, uint16_t minValue)
{
	TightDataPointStorageI* tdps = SZ_compress_uint16_2D_MDQ(oriData, r1, r2, realPrecision, valueRangeSize, minValue);

	convertTDPStoFlatBytes_int(tdps, newByteData, outSize);

	size_t dataLength = r1*r2;
	if(*outSize>dataLength*sizeof(uint16_t))
		SZ_compress_args_uint16_StoreOriData(oriData, dataLength, tdps, newByteData, outSize);
	
	free_TightDataPointStorageI(tdps);	
}

TightDataPointStorageI* SZ_compress_uint16_3D_MDQ(uint16_t *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, int64_t valueRangeSize, int64_t minValue)
{
	unsigned char bytes[8] = {0,0,0,0,0,0,0,0};
	int byteSize = computeByteSizePerIntValue(valueRangeSize);
	
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_uint16_3D(oriData, r1, r2, r3, realPrecision);
		updateQuantizationInfo(quantization_intervals);
	}	
	else
		quantization_intervals = exe_params->intvCapacity;
	size_t i,j,k; 
	int64_t pred1D, pred2D, pred3D, curValue, tmp;
	int diff = 0.0;
	double itvNum = 0;
	uint16_t *P0, *P1;
		
	size_t dataLength = r1*r2*r3;		

	size_t r23 = r2*r3;
	P0 = (uint16_t*)malloc(r23*sizeof(uint16_t));
	P1 = (uint16_t*)malloc(r23*sizeof(uint16_t));

	int* type = (int*) malloc(dataLength*sizeof(int));

	uint16_t* spaceFillingValue = oriData; //
	
	DynamicByteArray *exactDataByteArray;
	new_DBA(&exactDataByteArray, DynArrayInitLen);	

	type[0] = 0;
	P1[0] = spaceFillingValue[0];
	compressUInt16Value(spaceFillingValue[0], minValue, byteSize, bytes);
	memcpyDBA_Data(exactDataByteArray, bytes, byteSize);

	/* Process Row-0 data 1*/
	pred1D = P1[0];
	diff = spaceFillingValue[1] - pred1D;

	itvNum = llabs(diff)/realPrecision + 1;

	if (itvNum < exe_params->intvCapacity)
	{
		if (diff < 0) itvNum = -itvNum;
		type[1] = (int) (itvNum/2) + exe_params->intvRadius;
		tmp = pred1D + 2 * (type[1] - exe_params->intvRadius) * realPrecision;
		if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
			P1[1] = tmp;
		else if(tmp < SZ_UINT16_MIN)
			P1[1] = SZ_UINT16_MIN;
		else
			P1[1] = SZ_UINT16_MAX;		
	}
	else
	{
		type[1] = 0;
		curValue = P1[1] = spaceFillingValue[1];
		compressUInt16Value(curValue, minValue, byteSize, bytes);
		memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
	}

    /* Process Row-0 data 2 --> data r3-1 */
	for (j = 2; j < r3; j++)
	{
		pred1D = 2*P1[j-1] - P1[j-2];
		diff = spaceFillingValue[j] - pred1D;

		itvNum = llabs(diff)/realPrecision + 1;

		if (itvNum < exe_params->intvCapacity)
		{
			if (diff < 0) itvNum = -itvNum;
			type[j] = (int) (itvNum/2) + exe_params->intvRadius;
			tmp = pred1D + 2 * (type[j] - exe_params->intvRadius) * realPrecision;
			if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
				P1[j] = tmp;
			else if(tmp < SZ_UINT16_MIN)
				P1[j] = SZ_UINT16_MIN;
			else
				P1[j] = SZ_UINT16_MAX;			
		}
		else
		{
			type[j] = 0;
			curValue = P1[j] = spaceFillingValue[j];
			compressUInt16Value(curValue, minValue, byteSize, bytes);
			memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
		}
	}

	/* Process Row-1 --> Row-r2-1 */
	size_t index;
	for (i = 1; i < r2; i++)
	{
		/* Process row-i data 0 */
		index = i*r3;	
		pred1D = P1[index-r3];
		diff = spaceFillingValue[index] - pred1D;

		itvNum = llabs(diff)/realPrecision + 1;

		if (itvNum < exe_params->intvCapacity)
		{
			if (diff < 0) itvNum = -itvNum;
			type[index] = (int) (itvNum/2) + exe_params->intvRadius;
			tmp = pred1D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
			if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
				P1[index] = tmp;
			else if(tmp < SZ_UINT16_MIN)
				P1[index] = SZ_UINT16_MIN;
			else
				P1[index] = SZ_UINT16_MAX;			
		}
		else
		{
			type[index] = 0;
			curValue = P1[index] = spaceFillingValue[index];
			compressUInt16Value(curValue, minValue, byteSize, bytes);
			memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
		}

		/* Process row-i data 1 --> data r3-1*/
		for (j = 1; j < r3; j++)
		{
			index = i*r3+j;
			pred2D = P1[index-1] + P1[index-r3] - P1[index-r3-1];

			diff = spaceFillingValue[index] - pred2D;

			itvNum = llabs(diff)/realPrecision + 1;

			if (itvNum < exe_params->intvCapacity)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + exe_params->intvRadius;
				tmp = pred2D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
				if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
					P1[index] = tmp;
				else if(tmp < SZ_UINT16_MIN)
					P1[index] = SZ_UINT16_MIN;
				else
					P1[index] = SZ_UINT16_MAX;				
			}
			else
			{
				type[index] = 0;
				curValue = P1[index] = spaceFillingValue[index];
				compressUInt16Value(curValue, minValue, byteSize, bytes);
				memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
			}
		}
	}


	///////////////////////////	Process layer-1 --> layer-r1-1 ///////////////////////////

	for (k = 1; k < r1; k++)
	{
		/* Process Row-0 data 0*/
		index = k*r23;
		pred1D = P1[0];
		diff = spaceFillingValue[index] - pred1D;

		itvNum = llabs(diff)/realPrecision + 1;

		if (itvNum < exe_params->intvCapacity)
		{
			if (diff < 0) itvNum = -itvNum;
			type[index] = (int) (itvNum/2) + exe_params->intvRadius;
			tmp = pred1D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
			if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
				P0[0] = tmp;
			else if(tmp < SZ_UINT16_MIN)
				P0[0] = SZ_UINT16_MIN;
			else
				P0[0] = SZ_UINT16_MAX;
		}
		else
		{
			type[index] = 0;
			curValue = P0[0] = spaceFillingValue[index];
			compressUInt16Value(curValue, minValue, byteSize, bytes);
			memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
		}


	    /* Process Row-0 data 1 --> data r3-1 */
		for (j = 1; j < r3; j++)
		{
			//index = k*r2*r3+j;
			index ++;
			pred2D = P0[j-1] + P1[j] - P1[j-1];
			diff = spaceFillingValue[index] - pred2D;

			itvNum = llabs(diff)/realPrecision + 1;

			if (itvNum < exe_params->intvCapacity)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + exe_params->intvRadius;
				tmp = pred2D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
				if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
					P0[j] = tmp;
				else if(tmp < SZ_UINT16_MIN)
					P0[j] = SZ_UINT16_MIN;
				else
					P0[j] = SZ_UINT16_MAX;				
			}
			else
			{
				type[index] = 0;
				curValue = P0[j] = spaceFillingValue[index];
				compressUInt16Value(curValue, minValue, byteSize, bytes);
				memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
			}
		}

	    /* Process Row-1 --> Row-r2-1 */
		size_t index2D;
		for (i = 1; i < r2; i++)
		{
			/* Process Row-i data 0 */
			index = k*r23 + i*r3;
			index2D = i*r3;		
			pred2D = P0[index2D-r3] + P1[index2D] - P1[index2D-r3];
			diff = spaceFillingValue[index] - pred2D;

			itvNum = llabs(diff)/realPrecision + 1;

			if (itvNum < exe_params->intvCapacity)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + exe_params->intvRadius;
				tmp = pred2D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
				if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
					P0[index2D] = tmp;
				else if(tmp < SZ_UINT16_MIN)
					P0[index2D] = SZ_UINT16_MIN;
				else
					P0[index2D] = SZ_UINT16_MAX;
			}
			else
			{
				type[index] = 0;
				curValue = P0[index2D] = spaceFillingValue[index];
				compressUInt16Value(curValue, minValue, byteSize, bytes);
				memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
			}

			/* Process Row-i data 1 --> data r3-1 */
			for (j = 1; j < r3; j++)
			{
//				if(k==63&&i==43&&j==27)
//					printf("i=%d\n", i);
				//index = k*r2*r3 + i*r3 + j;			
				index ++;
				index2D = i*r3 + j;
				pred3D = P0[index2D-1] + P0[index2D-r3]+ P1[index2D] - P0[index2D-r3-1] - P1[index2D-r3] - P1[index2D-1] + P1[index2D-r3-1];
				diff = spaceFillingValue[index] - pred3D;

				itvNum = llabs(diff)/realPrecision + 1;

				if (itvNum < exe_params->intvCapacity)
				{
					if (diff < 0) itvNum = -itvNum;
					type[index] = (int) (itvNum/2) + exe_params->intvRadius;
					tmp = pred3D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
					if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
						P0[index2D] = tmp;
					else if(tmp < SZ_UINT16_MIN)
						P0[index2D] = SZ_UINT16_MIN;
					else
						P0[index2D] = SZ_UINT16_MAX;
				}
				else
				{
					type[index] = 0;
					curValue = P0[index2D] = spaceFillingValue[index];
					compressUInt16Value(curValue, minValue, byteSize, bytes);
					memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
				}
			}
		}

		uint16_t *Pt;
		Pt = P1;
		P1 = P0;
		P0 = Pt;
	}
	if(r23!=1)
		free(P0);
	free(P1);

	size_t exactDataNum = exactDataByteArray->size;
	
	TightDataPointStorageI* tdps;	
			
	new_TightDataPointStorageI(&tdps, dataLength, exactDataNum, byteSize, 
			type, exactDataByteArray->array, exactDataByteArray->size,  
			realPrecision, minValue, quantization_intervals, SZ_UINT16);
			
	//free memory
	free(type);	
	free(exactDataByteArray); //exactDataByteArray->array has been released in free_TightDataPointStorageF(tdps);
	
	return tdps;	
}


void SZ_compress_args_uint16_NoCkRngeNoGzip_3D(unsigned char** newByteData, uint16_t *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t *outSize, 
int64_t valueRangeSize, int64_t minValue)
{	
	TightDataPointStorageI* tdps = SZ_compress_uint16_3D_MDQ(oriData, r1, r2, r3, realPrecision, valueRangeSize, minValue);

	convertTDPStoFlatBytes_int(tdps, newByteData, outSize);

	size_t dataLength = r1*r2*r3;
	if(*outSize>dataLength*sizeof(uint16_t))
		SZ_compress_args_uint16_StoreOriData(oriData, dataLength, tdps, newByteData, outSize);
	
	free_TightDataPointStorageI(tdps);	
}


TightDataPointStorageI* SZ_compress_uint16_4D_MDQ(uint16_t *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision, int64_t valueRangeSize, int64_t minValue)
{
	unsigned char bytes[8] = {0,0,0,0,0,0,0,0};
	int byteSize = computeByteSizePerIntValue(valueRangeSize);
	
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_uint16_4D(oriData, r1, r2, r3, r4, realPrecision);
		updateQuantizationInfo(quantization_intervals);
	}	
	else
		quantization_intervals = exe_params->intvCapacity;
	size_t i,j,k; 
	int64_t pred1D, pred2D, pred3D, curValue, tmp;
	int diff = 0.0;
	double itvNum = 0;
	uint16_t *P0, *P1;
		
	size_t dataLength = r1*r2*r3*r4;		

	size_t r234 = r2*r3*r4;
	size_t r34 = r3*r4;

	P0 = (uint16_t*)malloc(r34*sizeof(uint16_t));
	P1 = (uint16_t*)malloc(r34*sizeof(uint16_t));
	
	int* type = (int*) malloc(dataLength*sizeof(int));

	uint16_t* spaceFillingValue = oriData; //
	
	DynamicByteArray *exactDataByteArray;
	new_DBA(&exactDataByteArray, DynArrayInitLen);	

	size_t l;
	for (l = 0; l < r1; l++)
	{

		///////////////////////////	Process layer-0 ///////////////////////////
		/* Process Row-0 data 0*/
		size_t index = l*r234;
		size_t index2D = 0;

		type[index] = 0;
		curValue = P1[index2D] = spaceFillingValue[index];
		compressUInt16Value(curValue, minValue, byteSize, bytes);
		memcpyDBA_Data(exactDataByteArray, bytes, byteSize);

		/* Process Row-0 data 1*/
		index = l*r234+1;
		index2D = 1;

		pred1D = P1[index2D-1];
		diff = curValue - pred1D;

		itvNum = llabs(diff)/realPrecision + 1;

		if (itvNum < exe_params->intvCapacity)
		{
			if (diff < 0) itvNum = -itvNum;
			type[index] = (int) (itvNum/2) + exe_params->intvRadius;
			tmp = pred1D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
			if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
				P1[index2D] = tmp;
			else if(tmp < SZ_UINT16_MIN)
				P1[index2D] = SZ_UINT16_MIN;
			else
				P1[index2D] = SZ_UINT16_MAX;			
		}
		else
		{
			type[index] = 0;

			curValue = P1[index2D] = spaceFillingValue[0];
			compressUInt16Value(curValue, minValue, byteSize, bytes);
			memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
		}

		/* Process Row-0 data 2 --> data r4-1 */
		for (j = 2; j < r4; j++)
		{
			index = l*r234+j;
			index2D = j;

			pred1D = 2*P1[index2D-1] - P1[index2D-2];
			diff = spaceFillingValue[index] - pred1D;

			itvNum = llabs(diff)/realPrecision + 1;

			if (itvNum < exe_params->intvCapacity)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + exe_params->intvRadius;
				tmp = pred1D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
				if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
					P1[index2D] = tmp;
				else if(tmp < SZ_UINT16_MIN)
					P1[index2D] = SZ_UINT16_MIN;
				else
					P1[index2D] = SZ_UINT16_MAX;					
			}
			else
			{
				type[index] = 0;

				curValue = P1[index2D] = spaceFillingValue[0];
				compressUInt16Value(curValue, minValue, byteSize, bytes);
				memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
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

			itvNum = llabs(diff)/realPrecision + 1;

			if (itvNum < exe_params->intvCapacity)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + exe_params->intvRadius;
				tmp = pred1D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
				if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
					P1[index2D] = tmp;
				else if(tmp < SZ_UINT16_MIN)
					P1[index2D] = SZ_UINT16_MIN;
				else
					P1[index2D] = SZ_UINT16_MAX;					
			}
			else
			{
				type[index] = 0;

				curValue = P1[index2D] = spaceFillingValue[0];
				compressUInt16Value(curValue, minValue, byteSize, bytes);
				memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
			}

			/* Process row-i data 1 --> data r4-1*/
			for (j = 1; j < r4; j++)
			{
				index = l*r234+i*r4+j;
				index2D = i*r4+j;

				pred2D = P1[index2D-1] + P1[index2D-r4] - P1[index2D-r4-1];

				diff = spaceFillingValue[index] - pred2D;

				itvNum = llabs(diff)/realPrecision + 1;

				if (itvNum < exe_params->intvCapacity)
				{
					if (diff < 0) itvNum = -itvNum;
					type[index] = (int) (itvNum/2) + exe_params->intvRadius;
					tmp = pred2D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
					if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
						P1[index2D] = tmp;
					else if(tmp < SZ_UINT16_MIN)
						P1[index2D] = SZ_UINT16_MIN;
					else
						P1[index2D] = SZ_UINT16_MAX;						
				}
				else
				{
					type[index] = 0;

					curValue = P1[index2D] = spaceFillingValue[0];
					compressUInt16Value(curValue, minValue, byteSize, bytes);
					memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
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

			itvNum = llabs(diff)/realPrecision + 1;

			if (itvNum < exe_params->intvCapacity)
			{
				if (diff < 0) itvNum = -itvNum;
				type[index] = (int) (itvNum/2) + exe_params->intvRadius;
				tmp = pred1D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
				if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
					P0[index2D] = tmp;
				else if(tmp < SZ_UINT16_MIN)
					P0[index2D] = SZ_UINT16_MIN;
				else
					P0[index2D] = SZ_UINT16_MAX;					
			}
			else
			{
				type[index] = 0;

				curValue = P0[index2D] = spaceFillingValue[0];
				compressUInt16Value(curValue, minValue, byteSize, bytes);
				memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
			}

			/* Process Row-0 data 1 --> data r4-1 */
			for (j = 1; j < r4; j++)
			{
				index = l*r234+k*r34+j;
				index2D = j;

				pred2D = P0[index2D-1] + P1[index2D] - P1[index2D-1];
				diff = spaceFillingValue[index] - pred2D;

				itvNum = llabs(diff)/realPrecision + 1;

				if (itvNum < exe_params->intvCapacity)
				{
					if (diff < 0) itvNum = -itvNum;
					type[index] = (int) (itvNum/2) + exe_params->intvRadius;
					tmp = pred2D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
					if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
						P0[index2D] = tmp;
					else if(tmp < SZ_UINT16_MIN)
						P0[index2D] = SZ_UINT16_MIN;
					else
						P0[index2D] = SZ_UINT16_MAX;						
				}
				else
				{
					type[index] = 0;

					curValue = P0[index2D] = spaceFillingValue[0];
					compressUInt16Value(curValue, minValue, byteSize, bytes);
					memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
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

				itvNum = llabs(diff)/realPrecision + 1;

				if (itvNum < exe_params->intvCapacity)
				{
					if (diff < 0) itvNum = -itvNum;
					type[index] = (int) (itvNum/2) + exe_params->intvRadius;
					tmp = pred2D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
					if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
						P0[index2D] = tmp;
					else if(tmp < SZ_UINT16_MIN)
						P0[index2D] = SZ_UINT16_MIN;
					else
						P0[index2D] = SZ_UINT16_MAX;						
				}
				else
				{
					type[index] = 0;

					curValue = P0[index2D] = spaceFillingValue[0];
					compressUInt16Value(curValue, minValue, byteSize, bytes);
					memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
				}

				/* Process Row-i data 1 --> data r4-1 */
				for (j = 1; j < r4; j++)
				{
					index = l*r234+k*r34+i*r4+j;
					index2D = i*r4+j;

					pred3D = P0[index2D-1] + P0[index2D-r4]+ P1[index2D] - P0[index2D-r4-1] - P1[index2D-r4] - P1[index2D-1] + P1[index2D-r4-1];
					diff = spaceFillingValue[index] - pred3D;


					itvNum = llabs(diff)/realPrecision + 1;

					if (itvNum < exe_params->intvCapacity)
					{
						if (diff < 0) itvNum = -itvNum;
						type[index] = (int) (itvNum/2) + exe_params->intvRadius;
						tmp = pred3D + 2 * (type[index] - exe_params->intvRadius) * realPrecision;
						if(tmp >= SZ_UINT16_MIN&&tmp<SZ_UINT16_MAX)
							P0[index2D] = tmp;
						else if(tmp < SZ_UINT16_MIN)
							P0[index2D] = SZ_UINT16_MIN;
						else
							P0[index2D] = SZ_UINT16_MAX;							
					}
					else
					{
						type[index] = 0;

						curValue = P0[index2D] = spaceFillingValue[0];
						compressUInt16Value(curValue, minValue, byteSize, bytes);
						memcpyDBA_Data(exactDataByteArray, bytes, byteSize);
					}
				}
			}

			uint16_t *Pt;
			Pt = P1;
			P1 = P0;
			P0 = Pt;
		}
	}

	free(P0);
	free(P1);

	size_t exactDataNum = exactDataByteArray->size;
	
	TightDataPointStorageI* tdps;	
			
	new_TightDataPointStorageI(&tdps, dataLength, exactDataNum, byteSize, 
			type, exactDataByteArray->array, exactDataByteArray->size,  
			realPrecision, minValue, quantization_intervals, SZ_UINT16);
			
	//free memory
	free(type);	
	free(exactDataByteArray); //exactDataByteArray->array has been released in free_TightDataPointStorageF(tdps);
	
	return tdps;	
}

void SZ_compress_args_uint16_NoCkRngeNoGzip_4D(unsigned char** newByteData, uint16_t *oriData, size_t r1, size_t r2, size_t r3, size_t r4, double realPrecision, 
size_t *outSize, int64_t valueRangeSize, int64_t minValue)
{
	TightDataPointStorageI* tdps = SZ_compress_uint16_4D_MDQ(oriData, r1, r2, r3, r4, realPrecision, valueRangeSize, minValue);

	convertTDPStoFlatBytes_int(tdps, newByteData, outSize);

	size_t dataLength = r1*r2*r3*r4;
	if(*outSize>dataLength*sizeof(uint16_t))
		SZ_compress_args_uint16_StoreOriData(oriData, dataLength, tdps, newByteData, outSize);

	free_TightDataPointStorageI(tdps);
}

void SZ_compress_args_uint16_withinRange(unsigned char** newByteData, uint16_t *oriData, size_t dataLength, size_t *outSize)
{
	TightDataPointStorageI* tdps = (TightDataPointStorageI*) malloc(sizeof(TightDataPointStorageI));
	tdps->typeArray = NULL;	
	
	tdps->allSameData = 1;
	tdps->dataSeriesLength = dataLength;
	tdps->exactDataBytes = (unsigned char*)malloc(sizeof(unsigned char)*2);
	tdps->isLossless = 0;
	//tdps->exactByteSize = 4;
	tdps->exactDataNum = 1;
	tdps->exactDataBytes_size = 2;
	tdps->dataTypeSize = convertDataTypeSize(sizeof(uint16_t));
	
	uint16_t value = oriData[0];
	int16ToBytes_bigEndian(tdps->exactDataBytes, value);
	
	size_t tmpOutSize;
	convertTDPStoFlatBytes_int(tdps, newByteData, &tmpOutSize);

	*outSize = tmpOutSize;//3+1+sizeof(uint16_t)+SZ_SIZE_TYPE; //8==3+1+4(uint16_size)
	free_TightDataPointStorageI(tdps);	
}

int SZ_compress_args_uint16_wRngeNoGzip(unsigned char** newByteData, uint16_t *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio)
{
	int status = SZ_SCES;
	size_t dataLength = computeDataLength(r5,r4,r3,r2,r1);
	int64_t valueRangeSize = 0;
	
	uint16_t minValue = computeRangeSize_int(oriData, SZ_UINT16, dataLength, &valueRangeSize);
	double realPrecision = getRealPrecision_int(valueRangeSize, errBoundMode, absErr_Bound, relBoundRatio, &status);
		
	if(valueRangeSize <= realPrecision)
	{
		SZ_compress_args_uint16_withinRange(newByteData, oriData, dataLength, outSize);
	}
	else
	{
//		SZ_compress_args_uint16_NoCkRngeNoGzip_2D(newByteData, oriData, r2, r1, realPrecision, outSize);
		if(r5==0&&r4==0&&r3==0&&r2==0)
		{
			SZ_compress_args_uint16_NoCkRngeNoGzip_1D(newByteData, oriData, r1, realPrecision, outSize, valueRangeSize, minValue);
		}
		else if(r5==0&&r4==0&&r3==0)
		{
			SZ_compress_args_uint16_NoCkRngeNoGzip_2D(newByteData, oriData, r2, r1, realPrecision, outSize, valueRangeSize, minValue);
		}
		else if(r5==0&&r4==0)
		{
			SZ_compress_args_uint16_NoCkRngeNoGzip_3D(newByteData, oriData, r3, r2, r1, realPrecision, outSize, valueRangeSize, minValue);
		}
		else if(r5==0)
		{
			SZ_compress_args_uint16_NoCkRngeNoGzip_3D(newByteData, oriData, r4*r3, r2, r1, realPrecision, outSize, valueRangeSize, minValue);
		}
	}
	return status;
}

int SZ_compress_args_uint16(unsigned char** newByteData, uint16_t *oriData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, 
int errBoundMode, double absErr_Bound, double relBoundRatio)
{
	confparams_cpr->errorBoundMode = errBoundMode;
	
	if(errBoundMode>=PW_REL)
	{
		printf("Error: Current SZ version doesn't support integer data compression with point-wise relative error bound being based on pwrType=AVG\n");
		exit(0);
		return SZ_NSCS;
	}
	int status = SZ_SCES;
	size_t dataLength = computeDataLength(r5,r4,r3,r2,r1);
	int64_t valueRangeSize = 0;

	uint16_t minValue = (uint16_t)computeRangeSize_int(oriData, SZ_UINT16, dataLength, &valueRangeSize);
	double realPrecision = 0; 
	
	if(confparams_cpr->errorBoundMode==PSNR)
	{
		confparams_cpr->errorBoundMode = ABS;
		realPrecision = confparams_cpr->absErrBound = computeABSErrBoundFromPSNR(confparams_cpr->psnr, (double)confparams_cpr->predThreshold, (double)valueRangeSize);
		//printf("realPrecision=%lf\n", realPrecision);
	}
	else
		realPrecision = getRealPrecision_int(valueRangeSize, errBoundMode, absErr_Bound, relBoundRatio, &status);

	if(valueRangeSize <= realPrecision)
	{
		SZ_compress_args_uint16_withinRange(newByteData, oriData, dataLength, outSize);
	}
	else
	{
		size_t tmpOutSize = 0;
		unsigned char* tmpByteData;
		if (r2==0)
		{
			SZ_compress_args_uint16_NoCkRngeNoGzip_1D(&tmpByteData, oriData, r1, realPrecision, &tmpOutSize, valueRangeSize, minValue);
		}
		else
		if (r3==0)
		{
			SZ_compress_args_uint16_NoCkRngeNoGzip_2D(&tmpByteData, oriData, r2, r1, realPrecision, &tmpOutSize, valueRangeSize, minValue);
		}
		else
		if (r4==0)
		{
			SZ_compress_args_uint16_NoCkRngeNoGzip_3D(&tmpByteData, oriData, r3, r2, r1, realPrecision, &tmpOutSize, valueRangeSize, minValue);
		}
		else
		if (r5==0)
		{
			SZ_compress_args_uint16_NoCkRngeNoGzip_4D(&tmpByteData, oriData, r4, r3, r2, r1, realPrecision, &tmpOutSize, valueRangeSize, minValue);
		}
		else
		{
			printf("Error: doesn't support 5 dimensions for now.\n");
			status = SZ_DERR; //dimension error
		}
		//Call Gzip to do the further compression.
		if(confparams_cpr->szMode==SZ_BEST_SPEED)
		{
			*outSize = tmpOutSize;
			*newByteData = tmpByteData;
		}
		else if(confparams_cpr->szMode==SZ_BEST_COMPRESSION || confparams_cpr->szMode==SZ_DEFAULT_COMPRESSION)
		{
			*outSize = sz_lossless_compress(confparams_cpr->losslessCompressor, confparams_cpr->gzipMode, tmpByteData, tmpOutSize, newByteData);
			free(tmpByteData);
		}
		else
		{
			printf("Error: Wrong setting of confparams_cpr->szMode in the uint16_t compression.\n");
			status = SZ_MERR; //mode error			
		}
	}
	
	return status;
}
