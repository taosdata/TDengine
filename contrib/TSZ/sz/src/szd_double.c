/**
 *  @file szd_double.c
 *  @author Sheng Di, Dingwen Tao, Xin Liang, Xiangyu Zou, Tao Lu, Wen Xia, Xuan Wang, Weizhe Zhang
 *  @date Aug, 2016
 *  @brief 
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdlib.h> 
#include <stdio.h>
#include <string.h>
#include "szd_double.h"
#include "TightDataPointStorageD.h"
#include "sz.h"
#include "utility.h"
#include "Huffman.h"
#include "transcode.h"

int SZ_decompress_args_double(double* newData, size_t r1, unsigned char* cmpBytes, 
				size_t cmpSize, int compressionType, double* hist_data, sz_exedata* pde_exe, sz_params* pde_params)
{
	int status = SZ_SUCCESS;
	size_t dataLength = r1;
	
	//unsigned char* tmpBytes;
	size_t targetUncompressSize = dataLength <<3; //i.e., *8
	//tmpSize must be "much" smaller than dataLength
	size_t i, tmpSize = 12+MetaDataByteLength_double+8;
	unsigned char* szTmpBytes = NULL;
	bool needFree = false;

	if(newData == NULL || cmpBytes == NULL || cmpSize == 0 || dataLength == 0 || pde_exe == NULL || pde_params == NULL)
		return SZ_FORMAT_ERR;

	if(cmpSize!=12+4+MetaDataByteLength_double && cmpSize!=12+8+MetaDataByteLength_double)
	{
		pde_params->losslessCompressor = is_lossless_compressed_data(cmpBytes, cmpSize);
		if(pde_params->szMode!=SZ_TEMPORAL_COMPRESSION)
		{
			if(pde_params->losslessCompressor!=-1)
				pde_params->szMode = SZ_BEST_COMPRESSION;
			else
				pde_params->szMode = SZ_BEST_SPEED;			
		}


		if(pde_params->szMode==SZ_BEST_SPEED)
		{
			tmpSize = cmpSize;
			szTmpBytes = cmpBytes;	
		}	
		else
		{
			if(targetUncompressSize<MIN_ZLIB_DEC_ALLOMEM_BYTES) //Considering the minimum size
				targetUncompressSize = MIN_ZLIB_DEC_ALLOMEM_BYTES; 			
			tmpSize = sz_lossless_decompress(pde_params->losslessCompressor, cmpBytes, cmpSize, &szTmpBytes, targetUncompressSize+4+MetaDataByteLength_double+8);			
	        needFree = true;	
		}
	}
	else
		szTmpBytes = cmpBytes;
	
	// calc postion 
	//pde_params->sol_ID = szTmpBytes[1+3-2+14-4]; //szTmpBytes: version(3bytes), samebyte(1byte), [14]:sol_ID=SZ or SZ_Transpose		
	//TODO: convert szTmpBytes to double array.
	TightDataPointStorageD* tdps = NULL;
	int errBoundMode = new_TightDataPointStorageD_fromFlatBytes(&tdps, szTmpBytes, tmpSize, pde_exe, pde_params);
	if(tdps == NULL || errBoundMode != SZ_ABS) //a malformed payload must never reach the decoder
	{
		if(tdps != NULL)
			free_TightDataPointStorageD2(tdps);
		if(szTmpBytes && needFree)
			free(szTmpBytes);
		return SZ_FORMAT_ERR;
	}

	int doubleSize = sizeof(double);
	if(tdps->isLossless)
	{
		// *newData = (double*)malloc(doubleSize*dataLength); comment by tickduan
		size_t rawOffset = 4+MetaDataByteLength_double+pde_exe->SZ_SIZE_TYPE;
		if(!(rawOffset <= tmpSize && dataLength*doubleSize <= tmpSize - rawOffset))
		{
			free_TightDataPointStorageD2(tdps);
			if(szTmpBytes && needFree)
				free(szTmpBytes);
			return SZ_FORMAT_ERR;
		}
		if(sysEndianType==BIG_ENDIAN_SYSTEM)
		{
			memcpy(newData, szTmpBytes+rawOffset, dataLength*doubleSize);
		}
		else
		{
			unsigned char* p = szTmpBytes+rawOffset;
			for(i=0;i<dataLength;i++,p+=doubleSize)
				newData[i] = bytesToDouble(p);
		}		
	}
	else if(pde_params->sol_ID==SZ_Transpose)
	{
		status = getSnapshotData_double_1D(newData,dataLength,tdps, errBoundMode, 0, hist_data, pde_params);		
	}
	else //pde_params->sol_ID==SZ
	{
		if(tdps->raBytes_size > 0) //v2.0
		{
			status = getSnapshotData_double_1D(newData,r1,tdps, errBoundMode, 0, hist_data, pde_params);
		}
		else //1.4.13 or time-based compression
		{
			status = getSnapshotData_double_1D(newData,r1,tdps, errBoundMode, compressionType, hist_data, pde_params);
		}
	}	

	free_TightDataPointStorageD2(tdps);
	if(szTmpBytes && needFree)
		free(szTmpBytes);	
	return status;
}

int decompressDataSeries_double_1D(double* data, size_t dataSeriesLength, double* hist_data, TightDataPointStorageD* tdps) 
{
	//updateQuantizationInfo(tdps->intervals);
	int intvRadius = tdps->intervals/2;
	size_t i, j, k = 0, p = 0, l = 0; // k is to track the location of residual_bit
								// in resiMidBits, p is to track the
								// byte_index of resiMidBits, l is for
								// leadNum
	unsigned char* leadNum = NULL;
	double interval = tdps->realPrecision*2;
	int status = SZ_SUCCESS;
	
	if(data == NULL || tdps == NULL || tdps->reqLength > 64) //reqLength is validated by the parser too
		return SZ_FORMAT_ERR;

	if(convertByteArray2IntArray_fast_2b(tdps->exactDataNum, tdps->leadNumArray, tdps->leadNumArray_size, &leadNum) != SZ_SUCCESS)
		return SZ_FORMAT_ERR;
	//*data = (double*)malloc(sizeof(double)*dataSeriesLength); comment by tickduan

	int* type = (int*)malloc(dataSeriesLength*sizeof(int));
	if(type == NULL)
	{
		free(leadNum);
		return SZ_FORMAT_ERR;
	}

	if (tdps->ifAdtFse == false) {
		HuffmanTree* huffmanTree = createHuffmanTree(tdps->stateNum);
		if(huffmanTree == NULL)
		{
			free(leadNum);
			free(type);
			return SZ_FORMAT_ERR;
		}
		status = decode_withTree(huffmanTree, tdps->typeArray, tdps->typeArray_size, dataSeriesLength, tdps->intervals, type);
		SZ_ReleaseHuffman(huffmanTree);	
	}
	else {
		status = decode_with_fse(type, dataSeriesLength, tdps->intervals, tdps->FseCode, tdps->FseCode_size, 
					tdps->transCodeBits, tdps->transCodeBits_size);
	}
	if(status != SZ_SUCCESS)
	{
		free(leadNum);
		free(type);
		return SZ_FORMAT_ERR;
	}

	unsigned char preBytes[8];
	unsigned char curBytes[8];
	
	memset(preBytes, 0, 8);

	size_t curByteIndex = 0;
	int reqBytesLength, resiBitsLength, resiBits; 
	unsigned char leadingNum;	
	double medianValue, exactData, predValue;
	
	reqBytesLength = tdps->reqLength/8;
	resiBitsLength = tdps->reqLength%8;
	medianValue = tdps->medianValue;
	

	int type_;
	for (i = 0; i < dataSeriesLength; i++) 
	{
		type_ = type[i];
		switch (type_) {
		case 0:
			//the exact-value path indexes the parsed arrays, so every index must stay inside them
			if (leadNum == NULL || l >= tdps->exactDataNum)
			{
				status = SZ_FORMAT_ERR;
				goto cleanup;
			}
			leadingNum = leadNum[l++];
			if (leadingNum < reqBytesLength &&
				curByteIndex + (size_t)(reqBytesLength - leadingNum) > tdps->exactMidBytes_size)
			{
				status = SZ_FORMAT_ERR;
				goto cleanup;
			}
			// compute resiBits
			resiBits = 0;
			if (resiBitsLength != 0) {
				int kMod8 = k % 8;
				int rightMovSteps = getRightMovingSteps(kMod8, resiBitsLength);
				if (rightMovSteps > 0) {
					int code = getRightMovingCode(kMod8, resiBitsLength);
					if (p >= tdps->residualMidBits_size)
					{
						status = SZ_FORMAT_ERR;
						goto cleanup;
					}
					resiBits = (tdps->residualMidBits[p] & code) >> rightMovSteps;
				} else if (rightMovSteps < 0) {
					int code1 = getLeftMovingCode(kMod8);
					int code2 = getRightMovingCode(kMod8, resiBitsLength);
					int leftMovSteps = -rightMovSteps;
					rightMovSteps = 8 - leftMovSteps;
					if (p + 1 >= tdps->residualMidBits_size)
					{
						status = SZ_FORMAT_ERR;
						goto cleanup;
					}
					resiBits = (tdps->residualMidBits[p] & code1) << leftMovSteps;
					p++;
					resiBits = resiBits
							| ((tdps->residualMidBits[p] & code2) >> rightMovSteps);
				} else // rightMovSteps == 0
				{
					int code = getRightMovingCode(kMod8, resiBitsLength);
					if (p >= tdps->residualMidBits_size)
					{
						status = SZ_FORMAT_ERR;
						goto cleanup;
					}
					resiBits = (tdps->residualMidBits[p] & code);
					p++;
				}
				k += resiBitsLength;
			}

			// recover the exact data
			memset(curBytes, 0, 8);
			memcpy(curBytes, preBytes, leadingNum);
			for (j = leadingNum; j < reqBytesLength; j++)
				curBytes[j] = tdps->exactMidBytes[curByteIndex++];
			if (resiBitsLength != 0) {
				unsigned char resiByte = (unsigned char) (resiBits << (8 - resiBitsLength));
				curBytes[reqBytesLength] = resiByte;
			}
			
			exactData = bytesToDouble(curBytes);
			data[i] = exactData + medianValue;
			memcpy(preBytes,curBytes,8);
			break;
		default:
			//predValue = 2 * data[i-1] - data[i-2];
			predValue = (i > 0) ? data[i-1] : 0; //never read in front of the output buffer
			data[i] = predValue + (double)((long long)type_ - (long long)intvRadius)*interval;
			break;
		}
		//printf("%.30G\n",data[i]);
	}
	
cleanup:
	free(leadNum);
	free(type);
	return status;
}


int getSnapshotData_double_1D(double* data, size_t dataSeriesLength, TightDataPointStorageD* tdps, int errBoundMode, int compressionType, double* hist_data, sz_params* pde_params) 
{
	size_t i;
	if (tdps->allSameData) {
		double value = bytesToDouble(tdps->exactMidBytes);

		//*data = (double*)malloc(sizeof(double)*dataSeriesLength); comment by tickduan
		for (i = 0; i < dataSeriesLength; i++)
			data[i] = value;
		return SZ_SUCCESS;
	} 
	
	return decompressDataSeries_double_1D(data, dataSeriesLength, hist_data, tdps);
}

