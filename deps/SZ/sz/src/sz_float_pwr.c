/**
 *  @file sz_float_pwr.c
 *  @author Sheng Di, Dingwen Tao, Xin Liang, Xiangyu Zou, Tao Lu, Wen Xia, Xuan Wang, Weizhe Zhang
 *  @date Aug, 2016
 *  @brief SZ_Init, Compression and Decompression functions
 * This file contains the compression/decompression functions related to point-wise relative errors
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
#include "TightDataPointStorageF.h"
#include "sz_float.h"
#include "sz_float_pwr.h"
#include "zlib.h"
#include "rw.h"
#include "utility.h"

void compute_segment_precisions_float_1D(float *oriData, size_t dataLength, float* pwrErrBound, unsigned char* pwrErrBoundBytes, double globalPrecision)
{
	size_t i = 0, j = 0, k = 0;
	float realPrecision = oriData[0]!=0?fabs(confparams_cpr->pw_relBoundRatio*oriData[0]):confparams_cpr->pw_relBoundRatio; 
	float approxPrecision;
	unsigned char realPrecBytes[4];
	float curPrecision;
	float curValue;
	float sum = 0;
	for(i=0;i<dataLength;i++)
	{
		curValue = oriData[i];
		if(i%confparams_cpr->segment_size==0&&i>0)
		{
			//get two first bytes of the realPrecision
			if(confparams_cpr->pwr_type==SZ_PWR_AVG_TYPE)
			{
				realPrecision = sum/confparams_cpr->segment_size;
				sum = 0;			
			}
			realPrecision *= confparams_cpr->pw_relBoundRatio;
			
			if(confparams_cpr->errorBoundMode==ABS_AND_PW_REL||confparams_cpr->errorBoundMode==REL_AND_PW_REL)
				realPrecision = realPrecision<globalPrecision?realPrecision:globalPrecision; 
			else if(confparams_cpr->errorBoundMode==ABS_OR_PW_REL||confparams_cpr->errorBoundMode==REL_OR_PW_REL)
				realPrecision = realPrecision<globalPrecision?globalPrecision:realPrecision;
				
			floatToBytes(realPrecBytes, realPrecision);
			realPrecBytes[2] = realPrecBytes[3] = 0;
			approxPrecision = bytesToFloat(realPrecBytes);
			//put the realPrecision in float* pwrErBound
			pwrErrBound[j++] = approxPrecision;
			//put the two bytes in pwrErrBoundBytes
			pwrErrBoundBytes[k++] = realPrecBytes[0];
			pwrErrBoundBytes[k++] = realPrecBytes[1];
			
			realPrecision = fabs(curValue);
		}
		
		if(curValue!=0)
		{
			curPrecision = fabs(curValue);
			
			switch(confparams_cpr->pwr_type)
			{
			case SZ_PWR_MIN_TYPE: 
				if(realPrecision>curPrecision)
					realPrecision = curPrecision;	
				break;
			case SZ_PWR_AVG_TYPE:
				sum += curPrecision;
				break;
			case SZ_PWR_MAX_TYPE:
				if(realPrecision<curPrecision)
					realPrecision = curPrecision;					
				break;
			}
		}
	}
	if(confparams_cpr->pwr_type==SZ_PWR_AVG_TYPE)
	{
		int size = dataLength%confparams_cpr->segment_size==0?confparams_cpr->segment_size:dataLength%confparams_cpr->segment_size;
		realPrecision = sum/size;		
	}	
	if(confparams_cpr->errorBoundMode==ABS_AND_PW_REL||confparams_cpr->errorBoundMode==REL_AND_PW_REL)
		realPrecision = realPrecision<globalPrecision?realPrecision:globalPrecision; 
	else if(confparams_cpr->errorBoundMode==ABS_OR_PW_REL||confparams_cpr->errorBoundMode==REL_OR_PW_REL)
		realPrecision = realPrecision<globalPrecision?globalPrecision:realPrecision;	
	floatToBytes(realPrecBytes, realPrecision);
	realPrecBytes[2] = realPrecBytes[3] = 0;
	approxPrecision = bytesToFloat(realPrecBytes);
	//put the realPrecision in float* pwrErBound
	pwrErrBound[j++] = approxPrecision;
	//put the two bytes in pwrErrBoundBytes
	pwrErrBoundBytes[k++] = realPrecBytes[0];
	pwrErrBoundBytes[k++] = realPrecBytes[1];
}

unsigned int optimize_intervals_float_1D_pwr(float *oriData, size_t dataLength, float* pwrErrBound)
{	
	size_t i = 0, j = 0;
	float realPrecision = pwrErrBound[j++];	
	unsigned long radiusIndex;
	float pred_value = 0, pred_err;
	int *intervals = (int*)malloc(confparams_cpr->maxRangeRadius*sizeof(int));
	memset(intervals, 0, confparams_cpr->maxRangeRadius*sizeof(int));
	int totalSampleSize = dataLength/confparams_cpr->sampleDistance;
	for(i=2;i<dataLength;i++)
	{
		if(i%confparams_cpr->segment_size==0)
			realPrecision = pwrErrBound[j++];
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

void SZ_compress_args_float_NoCkRngeNoGzip_1D_pwr(unsigned char* newByteData, float *oriData, double globalPrecision, 
size_t dataLength, size_t *outSize, float min, float max)
{
	size_t pwrLength = dataLength%confparams_cpr->segment_size==0?dataLength/confparams_cpr->segment_size:dataLength/confparams_cpr->segment_size+1;
	float* pwrErrBound = (float*)malloc(sizeof(float)*pwrLength);
	size_t pwrErrBoundBytes_size = sizeof(unsigned char)*pwrLength*2;
	unsigned char* pwrErrBoundBytes = (unsigned char*)malloc(pwrErrBoundBytes_size);
	
	compute_segment_precisions_float_1D(oriData, dataLength, pwrErrBound, pwrErrBoundBytes, globalPrecision);
	
	unsigned int quantization_intervals;
	if(exe_params->optQuantMode==1)
	{
		quantization_intervals = optimize_intervals_float_1D_pwr(oriData, dataLength, pwrErrBound);	
		updateQuantizationInfo(quantization_intervals);
	}
	else
		quantization_intervals = exe_params->intvCapacity;
	size_t i = 0, j = 0;
	int reqLength;
	float realPrecision = pwrErrBound[j++];	
	float medianValue = 0;
	float radius = fabs(max)<fabs(min)?fabs(min):fabs(max);
	short radExpo = getExponent_float(radius);
	
	computeReqLength_float(realPrecision, radExpo, &reqLength, &medianValue);

	int* type = (int*) malloc(dataLength*sizeof(int));
	//type[dataLength]=0;
		
	float* spaceFillingValue = oriData; //
	
	DynamicByteArray *resiBitLengthArray;
	new_DBA(&resiBitLengthArray, DynArrayInitLen);
	
	DynamicIntArray *exactLeadNumArray;
	new_DIA(&exactLeadNumArray, DynArrayInitLen);
	
	DynamicByteArray *exactMidByteArray;
	new_DBA(&exactMidByteArray, DynArrayInitLen);
	
	DynamicIntArray *resiBitArray;
	new_DIA(&resiBitArray, DynArrayInitLen);
	
	type[0] = 0;
	
	unsigned char preDataBytes[4] = {0};
	intToBytes_bigEndian(preDataBytes, 0);
	
	int reqBytesLength = reqLength/8;
	int resiBitsLength = reqLength%8;
	float last3CmprsData[3] = {0};

	FloatValueCompressElement *vce = (FloatValueCompressElement*)malloc(sizeof(FloatValueCompressElement));
	LossyCompressionElement *lce = (LossyCompressionElement*)malloc(sizeof(LossyCompressionElement));
						
	//add the first data	
	addDBA_Data(resiBitLengthArray, (unsigned char)resiBitsLength);
	compressSingleFloatValue(vce, spaceFillingValue[0], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	listAdd_float(last3CmprsData, vce->data);
	//printf("%.30G\n",last3CmprsData[0]);	
		
	//add the second data
	type[1] = 0;
	addDBA_Data(resiBitLengthArray, (unsigned char)resiBitsLength);			
	compressSingleFloatValue(vce, spaceFillingValue[1], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	listAdd_float(last3CmprsData, vce->data);
	//printf("%.30G\n",last3CmprsData[0]);	
	
	int state;
	double checkRadius;
	float curData;
	float pred;
	double predAbsErr;
	checkRadius = (exe_params->intvCapacity-1)*realPrecision;
	double interval = 2*realPrecision;
	int updateReqLength = 0; //a marker: 1 means already updated
	
	for(i=2;i<dataLength;i++)
	{
		curData = spaceFillingValue[i];
		if(i%confparams_cpr->segment_size==0)
		{
			realPrecision = pwrErrBound[j++];
			checkRadius = (exe_params->intvCapacity-1)*realPrecision;
			interval = 2*realPrecision;
			updateReqLength = 0;
		}
		//pred = 2*last3CmprsData[0] - last3CmprsData[1];
		pred = last3CmprsData[0];
		predAbsErr = fabs(curData - pred);	
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
			listAdd_float(last3CmprsData, pred);			
			continue;
		}
		
		//unpredictable data processing		
		if(updateReqLength==0)
		{
			computeReqLength_float(realPrecision, radExpo, &reqLength, &medianValue);
			reqBytesLength = reqLength/8;
			resiBitsLength = reqLength%8;
			updateReqLength = 1;		
		}
		
		type[i] = 0;
		addDBA_Data(resiBitLengthArray, (unsigned char)resiBitsLength);
		
		compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
		updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
		memcpy(preDataBytes,vce->curBytes,4);
		addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);

		listAdd_float(last3CmprsData, vce->data);	
	}//end of for
		
	int exactDataNum = exactLeadNumArray->size;
	
	TightDataPointStorageF* tdps;
			
	new_TightDataPointStorageF2(&tdps, dataLength, exactDataNum, 
			type, exactMidByteArray->array, exactMidByteArray->size,  
			exactLeadNumArray->array,  
			resiBitArray->array, resiBitArray->size, 
			resiBitLengthArray->array, resiBitLengthArray->size, 
			realPrecision, medianValue, (char)reqLength, quantization_intervals, pwrErrBoundBytes, pwrErrBoundBytes_size, radExpo);

	
	//free memory
	free_DBA(resiBitLengthArray);
	free_DIA(exactLeadNumArray);
	free_DIA(resiBitArray);
	free(type);
	
	convertTDPStoFlatBytes_float(tdps, newByteData, outSize);
	
	int floatSize=sizeof(float);
	if(*outSize>dataLength*floatSize)
	{
		size_t k = 0, i;
		tdps->isLossless = 1;
		size_t totalByteLength = 3 + exe_params->SZ_SIZE_TYPE + 1 + floatSize*dataLength;
		*newByteData = (unsigned char*)malloc(totalByteLength);
		
		unsigned char dsLengthBytes[exe_params->SZ_SIZE_TYPE];
		intToBytes_bigEndian(dsLengthBytes, dataLength);//4
		for (i = 0; i < 3; i++)//3
			newByteData[k++] = versionNumber[i];
		
		if(exe_params->SZ_SIZE_TYPE==4)
		{
			newByteData[k++] = 16;	//=00010000	
		}
		else 
		{
			newByteData[k++] = 80;
		}
		for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)//4 or 8
			newByteData[k++] = dsLengthBytes[i];

		
		if(sysEndianType==BIG_ENDIAN_SYSTEM)
			memcpy(newByteData+4+exe_params->SZ_SIZE_TYPE, oriData, dataLength*floatSize);
		else
		{
			unsigned char* p = newByteData+4+exe_params->SZ_SIZE_TYPE;
			for(i=0;i<dataLength;i++,p+=floatSize)
				floatToBytes(p, oriData[i]);
		}
		*outSize = totalByteLength;
	}

	free(pwrErrBound);
	
	free(vce);
	free(lce);
	free_TightDataPointStorageF(tdps);
	free(exactMidByteArray);
}

void createRangeGroups_float(float** posGroups, float** negGroups, int** posFlags, int** negFlags)
{
	size_t size = GROUP_COUNT*sizeof(float);
	size_t size2 = GROUP_COUNT*sizeof(int);
	*posGroups = (float*)malloc(size);
	*negGroups = (float*)malloc(size);
	*posFlags = (int*)malloc(size2);
	*negFlags = (int*)malloc(size2);
	memset(*posGroups, 0, size);
	memset(*negGroups, 0, size);
	memset(*posFlags, 0, size2);
	memset(*negFlags, 0, size2);
}

void compressGroupIDArray_float(char* groupID, TightDataPointStorageF* tdps)
{
	size_t dataLength = tdps->dataSeriesLength;
	int* standGroupID = (int*)malloc(dataLength*sizeof(int));

	size_t i;
	standGroupID[0] = groupID[0]+GROUP_COUNT; //plus an offset such that it would not be a negative number.
	char lastGroupIDValue = groupID[0], curGroupIDValue;
	int offset = 2*(GROUP_COUNT + 2);
	for(i=1; i<dataLength;i++)
	{
		curGroupIDValue = groupID[i];
		standGroupID[i] = (curGroupIDValue - lastGroupIDValue) + offset; 
		lastGroupIDValue = curGroupIDValue;
	}
	
	unsigned char* out = NULL;
	size_t outSize;
	
	HuffmanTree* huffmanTree = SZ_Reset();
	encode_withTree(huffmanTree, standGroupID, dataLength, &out, &outSize);
	SZ_ReleaseHuffman(huffmanTree);
	
	tdps->pwrErrBoundBytes = out; //groupIDArray
	tdps->pwrErrBoundBytes_size = outSize;
	
	free(standGroupID);
}

TightDataPointStorageF* SZ_compress_float_1D_MDQ_pwrGroup(float* oriData, size_t dataLength, int errBoundMode, 
double absErrBound, double relBoundRatio, double pwrErrRatio, float valueRangeSize, float medianValue_f)
{
	size_t i;
	float *posGroups, *negGroups, *groups;
	float pos_01_group = 0, neg_01_group = 0; //[0,1] and [-1,0]
	int *posFlags, *negFlags, *flags;
	int pos_01_flag = 0, neg_01_flag = 0;
	createRangeGroups_float(&posGroups, &negGroups, &posFlags, &negFlags);
	size_t nbBins = (size_t)(1/pwrErrRatio);
	if(nbBins%2==1)
		nbBins++;
	exe_params->intvRadius = nbBins;

	int reqLength, status;
	float medianValue = medianValue_f;
	float realPrecision = (float)getRealPrecision_float(valueRangeSize, errBoundMode, absErrBound, relBoundRatio, &status);
	if(realPrecision<0)
		realPrecision = pwrErrRatio;
	float realGroupPrecision; //precision (error) based on group ID
	getPrecisionReqLength_float(realPrecision);
	short radExpo = getExponent_float(valueRangeSize/2);
	short lastGroupNum = 0, groupNum, grpNum = 0;
	
	double* groupErrorBounds = generateGroupErrBounds(errBoundMode, realPrecision, pwrErrRatio);
	exe_params->intvRadius = generateGroupMaxIntervalCount(groupErrorBounds);
	
	computeReqLength_float(realPrecision, radExpo, &reqLength, &medianValue);

	int* type = (int*) malloc(dataLength*sizeof(int));
	char *groupID = (char*) malloc(dataLength*sizeof(char));
	char *gp = groupID;
		
	float* spaceFillingValue = oriData; 
	
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
			
	int state;
	float curData, decValue;
	float pred;
	float predAbsErr;
	double interval = 0;
	
	//add the first data	
	type[0] = 0;
	compressSingleFloatValue(vce, spaceFillingValue[0], realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
	updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
	memcpy(preDataBytes,vce->curBytes,4);
	addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
	
	curData = spaceFillingValue[0];
	groupNum = computeGroupNum_float(vce->data);

	if(curData > 0 && groupNum >= 0)
	{
		groups = posGroups;
		flags = posFlags;
		grpNum = groupNum;
	}
	else if(curData < 0 && groupNum >= 0)
	{
		groups = negGroups;
		flags = negFlags;
		grpNum = groupNum;
	}
	else if(curData >= 0 && groupNum == -1)
	{
		groups = &pos_01_group;
		flags = &pos_01_flag;
		grpNum = 0;
	}
	else //curData < 0 && groupNum == -1
	{
		groups = &neg_01_group;
		flags = &neg_01_flag;
		grpNum = 0;
	}

	listAdd_float_group(groups, flags, groupNum, spaceFillingValue[0], vce->data, gp);
	gp++;
	
	for(i=1;i<dataLength;i++)
	{
		curData = oriData[i];
		//printf("i=%d, posGroups[3]=%f, negGroups[3]=%f\n", i, posGroups[3], negGroups[3]);
		
		groupNum = computeGroupNum_float(curData);
		
		if(curData > 0 && groupNum >= 0)
		{
			groups = posGroups;
			flags = posFlags;
			grpNum = groupNum;
		}
		else if(curData < 0 && groupNum >= 0)
		{
			groups = negGroups;
			flags = negFlags;
			grpNum = groupNum;
		}
		else if(curData >= 0 && groupNum == -1)
		{
			groups = &pos_01_group;
			flags = &pos_01_flag;
			grpNum = 0;
		}
		else //curData < 0 && groupNum == -1
		{
			groups = &neg_01_group;
			flags = &neg_01_flag;
			grpNum = 0;
		}

		if(groupNum>=GROUP_COUNT)
		{
			type[i] = 0;
			compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			listAdd_float_group(groups, flags, lastGroupNum, curData, vce->data, gp);	//set the group number to be last one in order to get the groupID array as smooth as possible.		
		}
		else if(flags[grpNum]==0) //the dec value may not be in the same group
		{	
			type[i] = 0;
			compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
			updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
			memcpy(preDataBytes,vce->curBytes,4);
			addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);
			//decGroupNum = computeGroupNum_float(vce->data);
			
			//if(decGroupNum < groupNum)
			//	decValue = curData>0?pow(2, groupNum):-pow(2, groupNum);
			//else if(decGroupNum > groupNum)
			//	decValue = curData>0?pow(2, groupNum+1):-pow(2, groupNum+1);
			//else
			//	decValue = vce->data;
			
			decValue = vce->data;	
			listAdd_float_group(groups, flags, groupNum, curData, decValue, gp);
			lastGroupNum = curData>0?groupNum + 2: -(groupNum+2);
		}
		else //if flags[groupNum]==1, the dec value must be in the same group
		{
			pred = groups[grpNum];
			predAbsErr = fabs(curData - pred);
			realGroupPrecision = groupErrorBounds[grpNum]; //compute real error bound
			interval = realGroupPrecision*2;
			state = (predAbsErr/realGroupPrecision+1)/2;
			if(curData>=pred)
			{
				type[i] = exe_params->intvRadius+state;
				decValue = pred + state*interval;
			}
			else //curData<pred
			{
				type[i] = exe_params->intvRadius-state;
				decValue = pred - state*interval;
			}
			//decGroupNum = computeGroupNum_float(pred);
			
			if((decValue>0&&curData<0)||(decValue<0&&curData>=0))
				decValue = 0;
			//else
			//{
			//	if(decGroupNum < groupNum)
			//		decValue = curData>0?pow(2, groupNum):-pow(2, groupNum);
			//	else if(decGroupNum > groupNum)
			//		decValue = curData>0?pow(2, groupNum+1):-pow(2, groupNum+1);
			//	else
			//		decValue = pred;				
			//}
			
			if(fabs(curData-decValue)>realGroupPrecision)
			{	
				type[i] = 0;
				compressSingleFloatValue(vce, curData, realPrecision, medianValue, reqLength, reqBytesLength, resiBitsLength);
				updateLossyCompElement_Float(vce->curBytes, preDataBytes, reqBytesLength, resiBitsLength, lce);
				memcpy(preDataBytes,vce->curBytes,4);
				addExactData(exactMidByteArray, exactLeadNumArray, resiBitArray, lce);

				decValue = vce->data;	
			}
			
			listAdd_float_group(groups, flags, groupNum, curData, decValue, gp);			
			lastGroupNum = curData>=0?groupNum + 2: -(groupNum+2);			
		}
		gp++;	

	}
	
	int exactDataNum = exactLeadNumArray->size;
	
	TightDataPointStorageF* tdps;
			
	//combineTypeAndGroupIDArray(nbBins, dataLength, &type, groupID);

	new_TightDataPointStorageF(&tdps, dataLength, exactDataNum, 
			type, exactMidByteArray->array, exactMidByteArray->size,  
			exactLeadNumArray->array,  
			resiBitArray->array, resiBitArray->size, 
			resiBitsLength, 
			realPrecision, medianValue, (char)reqLength, nbBins, NULL, 0, radExpo);	
	
	compressGroupIDArray_float(groupID, tdps);
	
	free(posGroups);
	free(negGroups);
	free(posFlags);
	free(negFlags);
	free(groupID);
	free(groupErrorBounds);
	
	free_DIA(exactLeadNumArray);
	free_DIA(resiBitArray);
	free(type);	
	free(vce);
	free(lce);	
	free(exactMidByteArray); //exactMidByteArray->array has been released in free_TightDataPointStorageF(tdps);	
	
	return tdps;
}

void SZ_compress_args_float_NoCkRngeNoGzip_1D_pwrgroup(unsigned char* newByteData, float *oriData,
size_t dataLength, double absErrBound, double relBoundRatio, double pwrErrRatio, float valueRangeSize, float medianValue_f, size_t *outSize)
{
        TightDataPointStorageF* tdps = SZ_compress_float_1D_MDQ_pwrGroup(oriData, dataLength, confparams_cpr->errorBoundMode, 
        absErrBound, relBoundRatio, pwrErrRatio, 
        valueRangeSize, medianValue_f);

        convertTDPStoFlatBytes_float(tdps, newByteData, outSize);

        if(*outSize>3 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1 + sizeof(float)*dataLength)
                SZ_compress_args_float_StoreOriData(oriData, dataLength, newByteData, outSize);

        free_TightDataPointStorageF(tdps);
}

#include <stdbool.h>

void SZ_compress_args_float_NoCkRngeNoGzip_1D_pwr_pre_log(unsigned char* newByteData, float *oriData, double pwrErrRatio, size_t dataLength, size_t *outSize, float min, float max){

	float * log_data = (float *) malloc(dataLength * sizeof(float));

	unsigned char * signs = (unsigned char *) malloc(dataLength);
	memset(signs, 0, dataLength);
	// preprocess
	float max_abs_log_data;
    if(min == 0) max_abs_log_data = fabs(log2(fabs(max)));
    else if(max == 0) max_abs_log_data = fabs(log2(fabs(min)));
    else max_abs_log_data = fabs(log2(fabs(min))) > fabs(log2(fabs(max))) ? fabs(log2(fabs(min))) : fabs(log2(fabs(max)));
    float min_log_data = max_abs_log_data;
	bool positive = true;
	for(size_t i=0; i<dataLength; i++){
		if(oriData[i] < 0){
			signs[i] = 1;
			log_data[i] = -oriData[i];
			positive = false;
		}
		else
			log_data[i] = oriData[i];
		if(log_data[i] > 0){
			log_data[i] = log2(log_data[i]);
			if(log_data[i] > max_abs_log_data) max_abs_log_data = log_data[i];
			if(log_data[i] < min_log_data) min_log_data = log_data[i];
		}
	}

	float valueRangeSize, medianValue_f;
	computeRangeSize_float(log_data, dataLength, &valueRangeSize, &medianValue_f);	
	if(fabs(min_log_data) > max_abs_log_data) max_abs_log_data = fabs(min_log_data);
	double realPrecision = log2(1.0 + pwrErrRatio) - max_abs_log_data * 1.2e-7;
	for(size_t i=0; i<dataLength; i++){
		if(oriData[i] == 0){
			log_data[i] = min_log_data - 2.0001*realPrecision;
		}
	}

    TightDataPointStorageF* tdps = SZ_compress_float_1D_MDQ(log_data, dataLength, realPrecision, valueRangeSize, medianValue_f);
    tdps->minLogValue = min_log_data - 1.0001*realPrecision;
    free(log_data);
    if(!positive){
	    unsigned char * comp_signs;
		// compress signs
		unsigned long signSize = sz_lossless_compress(ZSTD_COMPRESSOR, 3, signs, dataLength, &comp_signs);
		tdps->pwrErrBoundBytes = comp_signs;
		tdps->pwrErrBoundBytes_size = signSize;
	}
	else{
		tdps->pwrErrBoundBytes = NULL;
		tdps->pwrErrBoundBytes_size = 0;
	}
	free(signs);

    convertTDPStoFlatBytes_float(tdps, newByteData, outSize);
    if(*outSize>3 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1 + sizeof(float)*dataLength)
            SZ_compress_args_float_StoreOriData(oriData, dataLength, newByteData, outSize);

    free_TightDataPointStorageF(tdps);
}


void SZ_compress_args_float_NoCkRngeNoGzip_1D_pwr_pre_log_MSST19(unsigned char* newByteData, float *oriData, double pwrErrRatio, size_t dataLength, size_t *outSize, float valueRangeSize, float medianValue_f,
																unsigned char* signs, bool* positive, float min, float max, float nearZero){
	float multiplier = pow((1+pwrErrRatio), -3.0001);
	for(int i=0; i<dataLength; i++){
		if(oriData[i] == 0){
			oriData[i] = nearZero * multiplier;
		}
	}

	float median_log = sqrt(fabs(nearZero * max));

	TightDataPointStorageF* tdps = SZ_compress_float_1D_MDQ_MSST19(oriData, dataLength, pwrErrRatio, valueRangeSize, median_log);

	tdps->minLogValue = nearZero / ((1+pwrErrRatio)*(1+pwrErrRatio));
	if(!(*positive)){
		unsigned char * comp_signs;
		// compress signs
		unsigned long signSize = sz_lossless_compress(ZSTD_COMPRESSOR, 3, signs, dataLength, &comp_signs);
		tdps->pwrErrBoundBytes = comp_signs;
		tdps->pwrErrBoundBytes_size = signSize;
	}
	else{
		tdps->pwrErrBoundBytes = NULL;
		tdps->pwrErrBoundBytes_size = 0;
	}
	free(signs);

	convertTDPStoFlatBytes_float(tdps, newByteData, outSize);
	if(*outSize>3 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1 + sizeof(float)*dataLength)
		SZ_compress_args_float_StoreOriData(oriData, dataLength, newByteData, outSize);

	free_TightDataPointStorageF(tdps);
}
