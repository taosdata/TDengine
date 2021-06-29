/**
 *  @file szd_double_pwr.c
 *  @author Sheng Di, Dingwen Tao, Xin Liang, Xiangyu Zou, Tao Lu, Wen Xia, Xuan Wang, Weizhe Zhang
 *  @date Feb, 2019
 *  @brief 
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdlib.h> 
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "TightDataPointStorageD.h"
#include "CompressElement.h"
#include "sz.h"
#include "Huffman.h"
#include "sz_double_pwr.h"
#include "utility.h"
//#include "rw.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wchar-subscripts"

void decompressDataSeries_double_1D_pwr(double** data, size_t dataSeriesLength, TightDataPointStorageD* tdps) 
{
	updateQuantizationInfo(tdps->intervals);
	unsigned char tmpPrecBytes[8] = {0}; //used when needing to convert bytes to double values
	unsigned char* bp = tdps->pwrErrBoundBytes;
	size_t i, j, k = 0, p = 0, l = 0; // k is to track the location of residual_bit
								// in resiMidBits, p is to track the
								// byte_index of resiMidBits, l is for
								// leadNum

	unsigned char* leadNum;
	double interval = 0;// = (double)tdps->realPrecision*2;
	
	convertByteArray2IntArray_fast_2b(tdps->exactDataNum, tdps->leadNumArray, tdps->leadNumArray_size, &leadNum);
	*data = (double*)malloc(sizeof(double)*dataSeriesLength);

	int* type = (int*)malloc(dataSeriesLength*sizeof(int));

	HuffmanTree* huffmanTree = createHuffmanTree(tdps->stateNum);
	decode_withTree(huffmanTree, tdps->typeArray, dataSeriesLength, type);
	SZ_ReleaseHuffman(huffmanTree);	
	
	unsigned char preBytes[8];
	unsigned char curBytes[8];
	
	memset(preBytes, 0, 8);

	size_t curByteIndex = 0;
	int reqLength = 0, reqBytesLength = 0, resiBitsLength = 0, resiBits = 0; 
	unsigned char leadingNum;	
	double medianValue, exactData, predValue = 0, realPrecision = 0;
	
	medianValue = tdps->medianValue;
	
	int type_, updateReqLength = 0;
	for (i = 0; i < dataSeriesLength; i++) 
	{
		if(i%tdps->segment_size==0)
		{
			tmpPrecBytes[0] = *(bp++);
			tmpPrecBytes[1] = *(bp++);
			memset(&tmpPrecBytes[2], 0, 6*sizeof(unsigned char));

			realPrecision = bytesToDouble(tmpPrecBytes);
			interval = realPrecision*2;
			updateReqLength = 0;
		}
		type_ = type[i];
		switch (type_) {
		case 0:
			// compute resiBits
			if(updateReqLength==0)
			{
				computeReqLength_double(realPrecision, tdps->radExpo, &reqLength, &medianValue);
				reqBytesLength = reqLength/8;
				resiBitsLength = reqLength%8;	
				updateReqLength = 1;	
			}
			resiBits = 0;
			if (resiBitsLength != 0) {
				int kMod8 = k % 8;
				int rightMovSteps = getRightMovingSteps(kMod8, resiBitsLength);
				if (rightMovSteps > 0) {
					int code = getRightMovingCode(kMod8, resiBitsLength);
					resiBits = (tdps->residualMidBits[p] & code) >> rightMovSteps;
				} else if (rightMovSteps < 0) {
					int code1 = getLeftMovingCode(kMod8);
					int code2 = getRightMovingCode(kMod8, resiBitsLength);
					int leftMovSteps = -rightMovSteps;
					rightMovSteps = 8 - leftMovSteps;
					resiBits = (tdps->residualMidBits[p] & code1) << leftMovSteps;
					p++;
					resiBits = resiBits
							| ((tdps->residualMidBits[p] & code2) >> rightMovSteps);
				} else // rightMovSteps == 0
				{
					int code = getRightMovingCode(kMod8, resiBitsLength);
					resiBits = (tdps->residualMidBits[p] & code);
					p++;
				}
				k += resiBitsLength;
			}

			// recover the exact data
			memset(curBytes, 0, 8);
			leadingNum = leadNum[l++];
			memcpy(curBytes, preBytes, leadingNum);
			for (j = leadingNum; j < reqBytesLength; j++)
				curBytes[j] = tdps->exactMidBytes[curByteIndex++];
			if (resiBitsLength != 0) {
				unsigned char resiByte = (unsigned char) (resiBits << (8 - resiBitsLength));
				curBytes[reqBytesLength] = resiByte;
			}
			
			exactData = bytesToDouble(curBytes);
			(*data)[i] = exactData + medianValue;
			memcpy(preBytes,curBytes,8);
			break;
		default:
			//predValue = 2 * (*data)[i-1] - (*data)[i-2];
			predValue = (*data)[i-1];
			(*data)[i] = predValue + (type_-exe_params->intvRadius)*interval;
			break;
		}
		//printf("%.30G\n",(*data)[i]);
	}
	free(leadNum);
	free(type);
	return;
}



void decompressDataSeries_double_1D_pwrgroup(double** data, size_t dataSeriesLength, TightDataPointStorageD* tdps) 
{
	double *posGroups, *negGroups, *groups;
	double pos_01_group, neg_01_group;
	int *posFlags, *negFlags;
	
	updateQuantizationInfo(tdps->intervals);
	
	unsigned char* leadNum;
	double interval;// = (double)tdps->realPrecision*2;
	
	convertByteArray2IntArray_fast_2b(tdps->exactDataNum, tdps->leadNumArray, tdps->leadNumArray_size, &leadNum);

	*data = (double*)malloc(sizeof(double)*dataSeriesLength);

	int* type = (int*)malloc(dataSeriesLength*sizeof(int));

	HuffmanTree* huffmanTree = createHuffmanTree(tdps->stateNum);
	decode_withTree(huffmanTree, tdps->typeArray, dataSeriesLength, type);
	SZ_ReleaseHuffman(huffmanTree);	

	createRangeGroups_double(&posGroups, &negGroups, &posFlags, &negFlags);
	
	double realGroupPrecision;
	double realPrecision = tdps->realPrecision;
	char* groupID = decompressGroupIDArray(tdps->pwrErrBoundBytes, tdps->dataSeriesLength);
	
	//note that the groupID values here are [1,2,3,....,18] or [-1,-2,...,-18]
	
	double* groupErrorBounds = generateGroupErrBounds(confparams_dec->errorBoundMode, realPrecision, confparams_dec->pw_relBoundRatio);
	exe_params->intvRadius = generateGroupMaxIntervalCount(groupErrorBounds);
		
	size_t nbBins = (size_t)(1/confparams_dec->pw_relBoundRatio + 0.5);
	if(nbBins%2==1)
		nbBins++;
	exe_params->intvRadius = nbBins;

	

	unsigned char preBytes[8];
	unsigned char curBytes[8];
	
	memset(preBytes, 0, 8);

	size_t curByteIndex = 0;
	int reqLength, reqBytesLength = 0, resiBitsLength = 0, resiBits; 
	unsigned char leadingNum;	
	double medianValue, exactData, curValue, predValue;
	
	medianValue = tdps->medianValue;
	
	size_t i, j, k = 0, p = 0, l = 0; // k is to track the location of residual_bit
							// in resiMidBits, p is to track the
							// byte_index of resiMidBits, l is for
							// leadNum
							
	int type_, updateReqLength = 0;
	char rawGrpID = 0, indexGrpID = 0;
	for (i = 0; i < dataSeriesLength; i++) 
	{
		rawGrpID = groupID[i];
		
		if(rawGrpID >= 2)
		{
			groups = posGroups;
			indexGrpID = rawGrpID - 2;
		}
		else if(rawGrpID <= -2)
		{
			groups = negGroups;
			indexGrpID = -rawGrpID - 2;		}
		else if(rawGrpID == 1)
		{
			groups = &pos_01_group;
			indexGrpID = 0;
		}
		else //rawGrpID == -1
		{
			groups = &neg_01_group;
			indexGrpID = 0;			
		}
		
		type_ = type[i];
		switch (type_) {
		case 0:
			// compute resiBits
			if(updateReqLength==0)
			{
				computeReqLength_double(realPrecision, tdps->radExpo, &reqLength, &medianValue);
				reqBytesLength = reqLength/8;
				resiBitsLength = reqLength%8;	
				updateReqLength = 1;	
			}
			resiBits = 0;
			if (resiBitsLength != 0) {
				int kMod8 = k % 8;
				int rightMovSteps = getRightMovingSteps(kMod8, resiBitsLength);
				if (rightMovSteps > 0) {
					int code = getRightMovingCode(kMod8, resiBitsLength);
					resiBits = (tdps->residualMidBits[p] & code) >> rightMovSteps;
				} else if (rightMovSteps < 0) {
					int code1 = getLeftMovingCode(kMod8);
					int code2 = getRightMovingCode(kMod8, resiBitsLength);
					int leftMovSteps = -rightMovSteps;
					rightMovSteps = 8 - leftMovSteps;
					resiBits = (tdps->residualMidBits[p] & code1) << leftMovSteps;
					p++;
					resiBits = resiBits
							| ((tdps->residualMidBits[p] & code2) >> rightMovSteps);
				} else // rightMovSteps == 0
				{
					int code = getRightMovingCode(kMod8, resiBitsLength);
					resiBits = (tdps->residualMidBits[p] & code);
					p++;
				}
				k += resiBitsLength;
			}

			// recover the exact data	
			memset(curBytes, 0, 8);
			leadingNum = leadNum[l++];
			memcpy(curBytes, preBytes, leadingNum);
			for (j = leadingNum; j < reqBytesLength; j++)
				curBytes[j] = tdps->exactMidBytes[curByteIndex++];
			if (resiBitsLength != 0) {
				unsigned char resiByte = (unsigned char) (resiBits << (8 - resiBitsLength));
				curBytes[reqBytesLength] = resiByte;
			}
			
			exactData = bytesToDouble(curBytes);
			exactData = exactData + medianValue;
			(*data)[i] = exactData;
			memcpy(preBytes,curBytes,8);
			
			groups[indexGrpID] = exactData;
			
			break;
		default:
			predValue = groups[indexGrpID]; //Here, groups[indexGrpID] is the previous value.
			realGroupPrecision = groupErrorBounds[indexGrpID];
			interval = realGroupPrecision*2;		
			
			curValue = predValue + (type_-exe_params->intvRadius)*interval;
			
			//groupNum = computeGroupNum_double(curValue);
			
			if((curValue>0&&rawGrpID<0)||(curValue<0&&rawGrpID>0))
				curValue = 0;
			//else
			//{
			//	realGrpID = fabs(rawGrpID)-2;
			//	if(groupNum<realGrpID)
			//		curValue = rawGrpID>0?pow(2,realGrpID):-pow(2,realGrpID);
			//	else if(groupNum>realGrpID)
			//		curValue = rawGrpID>0?pow(2,groupNum):-pow(2,groupNum);				
			//}	
				
			(*data)[i] = curValue;
			groups[indexGrpID] = curValue;
			break;		
		}
	}	
	
	free(leadNum);
	free(type);
	
	free(posGroups);
	free(negGroups);
	free(posFlags);
	free(negFlags);
	free(groupErrorBounds);
	free(groupID);
}

void decompressDataSeries_double_1D_pwr_pre_log(double** data, size_t dataSeriesLength, TightDataPointStorageD* tdps) {

	decompressDataSeries_double_1D(data, dataSeriesLength, NULL, tdps);
	double threshold = tdps->minLogValue;
	if(tdps->pwrErrBoundBytes_size > 0){
		unsigned char * signs;
		sz_lossless_decompress(ZSTD_COMPRESSOR, tdps->pwrErrBoundBytes, tdps->pwrErrBoundBytes_size, &signs, dataSeriesLength);

		for(size_t i=0; i<dataSeriesLength; i++){
			if((*data)[i] < threshold) (*data)[i] = 0;
			else (*data)[i] = exp2((*data)[i]);
			if(signs[i]) (*data)[i] = -((*data)[i]);
		}
		free(signs);
	}
	else{
		for(size_t i=0; i<dataSeriesLength; i++){
			if((*data)[i] < threshold) (*data)[i] = 0;
			else (*data)[i] = exp2((*data)[i]);
		}
	}

}

void decompressDataSeries_double_1D_pwr_pre_log_MSST19(double** data, size_t dataSeriesLength, TightDataPointStorageD* tdps) 
{
	decompressDataSeries_double_1D_MSST19(data, dataSeriesLength, tdps);
	double threshold = tdps->minLogValue;
	uint64_t* ptr;

	if(tdps->pwrErrBoundBytes_size > 0){
		unsigned char * signs = NULL;
		if(tdps->pwrErrBoundBytes_size==0)
		{
			signs = (unsigned char*)malloc(dataSeriesLength);
			memset(signs, 0, dataSeriesLength);
		}
		else
			sz_lossless_decompress(ZSTD_COMPRESSOR, tdps->pwrErrBoundBytes, tdps->pwrErrBoundBytes_size, &signs, dataSeriesLength);
		for(size_t i=0; i<dataSeriesLength; i++){
			if((*data)[i] < threshold && (*data)[i] >= 0){
				(*data)[i] = 0;
				continue;
			}
			if(signs[i]){
			    ptr = (uint64_t*)(*data) + i;
                *ptr |= 0x8000000000000000;
			}
		}
		free(signs);
	}
	else{
		for(size_t i=0; i<dataSeriesLength; i++){
			if((*data)[i] < threshold) (*data)[i] = 0;
		}
	}
}


#pragma GCC diagnostic pop
