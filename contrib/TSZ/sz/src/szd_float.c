/**
 *  @file szd_float.c
 *  @author Sheng Di, Dingwen Tao, Xin Liang, Xiangyu Zou, Tao Lu, Wen Xia, Xuan Wang, Weizhe Zhang
 *  @date Aug, 2018
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
#include "utility.h"
#include "Huffman.h"
#include "transcode.h"


/**
 * 
 * int compressionType: 1 (time-based compression) ; 0 (space-based compression)
 * hist_data: only valid when compressionType==1, hist_data is the historical dataset such as the data in previous time step
 * 
 * @return status SUCCESSFUL (SZ_SUCCESS) or not (other error codes) f
 * */
int SZ_decompress_args_float(float* newData, size_t r1, unsigned char* cmpBytes, 
                            size_t cmpSize, int compressionType, float* hist_data, sz_exedata* pde_exe, sz_params* pde_params)
{
	int status = SZ_SUCCESS;
	size_t dataLength = r1;
	
	//unsigned char* tmpBytes;
	size_t targetUncompressSize = dataLength <<2; //i.e., *4
	//tmpSize must be "much" smaller than dataLength
	size_t i, tmpSize = 8+MetaDataByteLength+pde_exe->SZ_SIZE_TYPE;
	unsigned char* szTmpBytes = NULL;	
	bool  needFree = false;
	
	if(cmpSize!=8+4+MetaDataByteLength && cmpSize!=8+8+MetaDataByteLength) //4,8 means two posibilities of SZ_SIZE_TYPE
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
			tmpSize = sz_lossless_decompress(pde_params->losslessCompressor, cmpBytes, (unsigned long)cmpSize, &szTmpBytes, (unsigned long)targetUncompressSize+4+MetaDataByteLength+8);
	        needFree = true;
		}
	}
	else
		szTmpBytes = cmpBytes;	
		
	// calc sol_ID
	//pde_params->sol_ID = szTmpBytes[1+3-2+14-4]; //szTmpBytes: version(1bytes), samebyte(1byte), [14-4]:sol_ID=SZ or SZ_Transpose
		
	//TODO: convert szTmpBytes to data array.
	TightDataPointStorageF* tdps = NULL;
	int errBoundMode = new_TightDataPointStorageF_fromFlatBytes(&tdps, szTmpBytes, tmpSize, pde_exe, pde_params);
	if(tdps == NULL)
	{
		return SZ_FORMAT_ERR;
	}
	
	int floatSize = sizeof(float);
	if(tdps->isLossless)
	{
		//*newData = (float*)malloc(floatSize*dataLength);  comment by tickduan
		if(sysEndianType==BIG_ENDIAN_SYSTEM)
		{
			memcpy(newData, szTmpBytes+4+MetaDataByteLength+pde_exe->SZ_SIZE_TYPE, dataLength*floatSize);
		}
		else
		{
			unsigned char* p = szTmpBytes+4+MetaDataByteLength+pde_exe->SZ_SIZE_TYPE;
			for(i=0;i<dataLength;i++,p+=floatSize)
				newData[i] = bytesToFloat(p);
		}		
	}
	else //pde_params->sol_ID==SZ
	{
		if(tdps->raBytes_size > 0) //v2.0
		{
			getSnapshotData_float_1D(newData,r1,tdps, errBoundMode, 0, hist_data, pde_params);
		}
		else //1.4.13 or time-based compression
		{
			getSnapshotData_float_1D(newData,r1,tdps, errBoundMode, compressionType, hist_data, pde_params);
		}
	}

	//cost_start_();	
	//cost_end_();
	//printf("totalCost_=%f\n", totalCost_);
	free_TightDataPointStorageF2(tdps);
	if(szTmpBytes && needFree)
		free(szTmpBytes);
	return status;
}

void decompressDataSeries_float_1D(float* data, size_t dataSeriesLength, float* hist_data, TightDataPointStorageF* tdps) 
{
	//updateQuantizationInfo(tdps->intervals);
	int intvRadius = tdps->intervals/2;
	size_t i, j, k = 0, p = 0, l = 0; // k is to track the location of residual_bit
								// in resiMidBits, p is to track the
								// byte_index of resiMidBits, l is for
								// leadNum
	unsigned char* leadNum;
	float interval = tdps->realPrecision*2;
    	
	convertByteArray2IntArray_fast_2b(tdps->exactDataNum, tdps->leadNumArray, tdps->leadNumArray_size, &leadNum);
	//data = (float*)malloc(sizeof(float)*dataSeriesLength); // comment by tickduan 
	
	int* types = (int*)malloc(dataSeriesLength*sizeof(int));
	
	if (tdps->ifAdtFse == false) {
		// type tree
		HuffmanTree* huffmanTree = createHuffmanTree(tdps->stateNum);
		decode_withTree(huffmanTree, tdps->typeArray, dataSeriesLength, types);
		SZ_ReleaseHuffman(huffmanTree);	
	}
	else {
		decode_with_fse(types, dataSeriesLength, tdps->intervals, tdps->FseCode, tdps->FseCode_size, 
					tdps->transCodeBits, tdps->transCodeBits_size);
	}

	unsigned char preBytes[4];
	unsigned char curBytes[4];
	
	memset(preBytes, 0, 4);

	size_t curByteIndex = 0;
	int reqBytesLength, resiBitsLength, resiBits; 
	unsigned char leadingNum;	
	float medianValue, exactData, predValue;
	
	reqBytesLength = tdps->reqLength/8;
	resiBitsLength = tdps->reqLength%8;
	medianValue = tdps->medianValue;	
	
	// decompress core
    int type;
	for (i = 0; i < dataSeriesLength; i++) 
	{	
		type = types[i];
		switch (type) 
		{
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
			data[i] = exactData + medianValue;
			memcpy(preBytes,curBytes,4);
			break;
		default:
			//predValue = 2 * data[i-1] - data[i-2];
			predValue = data[i-1];
			data[i] = predValue + (float)(type - intvRadius) * interval;
			break;
		}
		//printf("%.30G\n",data[i]);
	}
	
	free(leadNum);
	free(types);
	return;
}

void getSnapshotData_float_1D(float* data, size_t dataSeriesLength, TightDataPointStorageF* tdps, int errBoundMode, int compressionType, float* hist_data, sz_params* pde_params)
{	
	size_t i;

	if (tdps->allSameData) 
	{
		float value = bytesToFloat(tdps->exactMidBytes);
		//*data = (float*)malloc(sizeof(float)*dataSeriesLength); commnet by tickduan
		for (i = 0; i < dataSeriesLength; i++)
			data[i] = value;
	} 
	else 
	{
	   decompressDataSeries_float_1D(data, dataSeriesLength, hist_data, tdps);
	}
}
