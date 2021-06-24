/**
 *  @file szd_float_ts.c
 *  @author Sheng Di and Dingwen Tao
 *  @date Aug, 2016
 *  @brief 
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdlib.h> 
#include <stdio.h>
#include <string.h>
#include "szd_float.h"
#include "TightDataPointStorageF.h"
#include "sz.h"
#include "Huffman.h"
#include "szd_float_ts.h"

void decompressDataSeries_float_1D_ts(float** data, size_t dataSeriesLength, float* hist_data, TightDataPointStorageF* tdps) 
{
	float* lastSnapshotData = hist_data;
	updateQuantizationInfo(tdps->intervals);
	size_t i, j, k = 0, p = 0, l = 0; // k is to track the location of residual_bit
								// in resiMidBits, p is to track the
								// byte_index of resiMidBits, l is for
								// leadNum
	unsigned char* leadNum;
	double interval = tdps->realPrecision*2;
	
	convertByteArray2IntArray_fast_2b(tdps->exactDataNum, tdps->leadNumArray, tdps->leadNumArray_size, &leadNum);

	*data = (float*)malloc(sizeof(float)*dataSeriesLength);

	int* type = (int*)malloc(dataSeriesLength*sizeof(int));
	
	HuffmanTree* huffmanTree = createHuffmanTree(tdps->stateNum);
	decode_withTree(huffmanTree, tdps->typeArray, dataSeriesLength, type);
	SZ_ReleaseHuffman(huffmanTree);	

	unsigned char preBytes[4];
	unsigned char curBytes[4];
	
	memset(preBytes, 0, 4);

	size_t curByteIndex = 0;
	int reqBytesLength, resiBitsLength, resiBits; 
	unsigned char leadingNum;	
	float medianValue, exactData, predValue = 0;
	
	reqBytesLength = tdps->reqLength/8;
	resiBitsLength = tdps->reqLength%8;
	medianValue = tdps->medianValue;
	
	int type_;
	for (i = 0; i < dataSeriesLength; i++) {
		type_ = type[i];
		switch (type_) {
		case 0:
			// compute resiBits
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
			memset(curBytes, 0, 4);
			leadingNum = leadNum[l++];
			memcpy(curBytes, preBytes, leadingNum);
			for (j = leadingNum; j < reqBytesLength; j++)
				curBytes[j] = tdps->exactMidBytes[curByteIndex++];
			if (resiBitsLength != 0) {
				unsigned char resiByte = (unsigned char) (resiBits << (8 - resiBitsLength));
				curBytes[reqBytesLength] = resiByte;
			}
			
			exactData = bytesToFloat(curBytes);
			(*data)[i] = exactData + medianValue;
			memcpy(preBytes,curBytes,4);
			break;
		default:
			//predValue = (*data)[i-1];
			if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
				predValue = lastSnapshotData[i];
			(*data)[i] = predValue + (type_-exe_params->intvRadius)*interval;
			break;
		}
		//printf("%.30G\n",(*data)[i]);
	}
	
	memcpy(hist_data, (*data), dataSeriesLength*sizeof(float));
	
	free(leadNum);
	free(type);
	return;
}
