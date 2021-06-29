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
#include "Huffman.h"
#include "szd_double_pwr.h"
#include "szd_double_ts.h"
#include "utility.h"

int SZ_decompress_args_double(double** newData, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, unsigned char* cmpBytes, 
size_t cmpSize, int compressionType, double* hist_data, sz_exedata* pde_exe, sz_params* pde_params)
{
	int status = SZ_SCES;
	size_t dataLength = computeDataLength(r5,r4,r3,r2,r1);
	
	//unsigned char* tmpBytes;
	size_t targetUncompressSize = dataLength <<3; //i.e., *8
	//tmpSize must be "much" smaller than dataLength
	size_t i, tmpSize = 12+MetaDataByteLength_double+exe_params->SZ_SIZE_TYPE;
	unsigned char* szTmpBytes;
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
		else if(pde_params->szMode==SZ_BEST_COMPRESSION || pde_params->szMode==SZ_DEFAULT_COMPRESSION || pde_params->szMode==SZ_TEMPORAL_COMPRESSION)
		{
			if(targetUncompressSize<MIN_ZLIB_DEC_ALLOMEM_BYTES) //Considering the minimum size
				targetUncompressSize = MIN_ZLIB_DEC_ALLOMEM_BYTES; 			
			tmpSize = sz_lossless_decompress(pde_params->losslessCompressor, cmpBytes, (unsigned long)cmpSize, &szTmpBytes, (unsigned long)targetUncompressSize+4+MetaDataByteLength_double+exe_params->SZ_SIZE_TYPE);			
			//szTmpBytes = (unsigned char*)malloc(sizeof(unsigned char)*tmpSize);
			//memcpy(szTmpBytes, tmpBytes, tmpSize);
			//free(tmpBytes); //release useless memory		
		}
		else
		{
			printf("Wrong value of pde_params->szMode in the double compressed bytes.\n");
			status = SZ_MERR;
			return status;
		}	
	}
	else
		szTmpBytes = cmpBytes;
		
	pde_params->sol_ID = szTmpBytes[4+14]; //szTmpBytes: version(3bytes), samebyte(1byte), [14]:sol_ID=SZ or SZ_Transpose		
	//TODO: convert szTmpBytes to double array.
	TightDataPointStorageD* tdps;
	int errBoundMode = new_TightDataPointStorageD_fromFlatBytes(&tdps, szTmpBytes, tmpSize, pde_exe, pde_params);

	int dim = computeDimension(r5,r4,r3,r2,r1);
	int doubleSize = sizeof(double);
	if(tdps->isLossless)
	{
		*newData = (double*)malloc(doubleSize*dataLength);
		if(sysEndianType==BIG_ENDIAN_SYSTEM)
		{
			memcpy(*newData, szTmpBytes+4+MetaDataByteLength_double+exe_params->SZ_SIZE_TYPE, dataLength*doubleSize);
		}
		else
		{
			unsigned char* p = szTmpBytes+4+MetaDataByteLength_double+exe_params->SZ_SIZE_TYPE;
			for(i=0;i<dataLength;i++,p+=doubleSize)
				(*newData)[i] = bytesToDouble(p);
		}		
	}
	else if(pde_params->sol_ID==SZ_Transpose)
	{
		getSnapshotData_double_1D(newData,dataLength,tdps, errBoundMode, 0, hist_data, pde_params);		
	}
	else //pde_params->sol_ID==SZ
	{
		if(tdps->raBytes_size > 0) //v2.0
		{
			if (dim == 1)
				getSnapshotData_double_1D(newData,r1,tdps, errBoundMode, 0, hist_data, pde_params);
			else
			{
				printf("Error: currently support only at most 4 dimensions!\n");
				status = SZ_DERR;
			}	
		}
		else //1.4.13 or time-based compression
		{
			if (dim == 1)
				getSnapshotData_double_1D(newData,r1,tdps, errBoundMode, compressionType, hist_data, pde_params);
			else
			{
				printf("Error: currently support only at most 4 dimensions!\n");
				status = SZ_DERR;
			}			
		}
	}	

	if(pde_params->protectValueRange)
	{
		double* nd = *newData;
		double min = pde_params->dmin;
		double max = pde_params->dmax;		
		for(i=0;i<dataLength;i++)
		{
			double v = nd[i];
			if(v <= max && v >= min)
				continue;
			if(v < min)
				nd[i] = min;
			else if(v > max)
				nd[i] = max;
		}
	}

	free_TightDataPointStorageD2(tdps);
	if(pde_params->szMode!=SZ_BEST_SPEED && cmpSize!=12+MetaDataByteLength_double+exe_params->SZ_SIZE_TYPE)
		free(szTmpBytes);	
	return status;
}

void decompressDataSeries_double_1D(double** data, size_t dataSeriesLength, double* hist_data, TightDataPointStorageD* tdps) 
{
	//updateQuantizationInfo(tdps->intervals);
	int intvRadius = tdps->intervals/2;
	size_t i, j, k = 0, p = 0, l = 0; // k is to track the location of residual_bit
								// in resiMidBits, p is to track the
								// byte_index of resiMidBits, l is for
								// leadNum
	unsigned char* leadNum;
	double interval = tdps->realPrecision*2;
	
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
	int reqBytesLength, resiBitsLength, resiBits; 
	unsigned char leadingNum;	
	double medianValue, exactData, predValue;
	
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
			(*data)[i] = predValue + (type_-intvRadius)*interval;
			break;
		}
		//printf("%.30G\n",(*data)[i]);
	}
	
#ifdef HAVE_TIMECMPR	
	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
		memcpy(hist_data, (*data), dataSeriesLength*sizeof(double));
#endif	
	
	free(leadNum);
	free(type);
	return;
}

/*MSST19*/
void decompressDataSeries_double_1D_MSST19(double** data, size_t dataSeriesLength, TightDataPointStorageD* tdps) 
{
	//updateQuantizationInfo(tdps->intervals);
	int intvRadius = tdps->intervals/2;
	int intvCapacity = tdps->intervals;
	size_t i, j, k = 0, p = 0, l = 0; // k is to track the location of residual_bit
								// in resiMidBits, p is to track the
								// byte_index of resiMidBits, l is for
								// leadNum
	unsigned char* leadNum;
	//double interval = tdps->realPrecision*2;
	
	convertByteArray2IntArray_fast_2b(tdps->exactDataNum, tdps->leadNumArray, tdps->leadNumArray_size, &leadNum);
	*data = (double*)malloc(sizeof(double)*dataSeriesLength);

	int* type = (int*)malloc(dataSeriesLength*sizeof(int));
	
	HuffmanTree* huffmanTree = createHuffmanTree(tdps->stateNum);
	decode_withTree_MSST19(huffmanTree, tdps->typeArray, dataSeriesLength, type, tdps->max_bits);
	//decode_withTree(huffmanTree, tdps->typeArray, dataSeriesLength, type);
	SZ_ReleaseHuffman(huffmanTree);	
	unsigned char preBytes[8];
	unsigned char curBytes[8];
	
	memset(preBytes, 0, 8);

	size_t curByteIndex = 0;
	int reqBytesLength, resiBitsLength, resiBits; 
	unsigned char leadingNum;	
	double exactData, predValue = 0;
	reqBytesLength = tdps->reqLength/8;
	resiBitsLength = tdps->reqLength%8;
	//float threshold = tdps->minLogValue;
	double* precisionTable = (double*)malloc(sizeof(double) * intvCapacity);
	double inv = 2.0-pow(2, -(tdps->plus_bits));
	for(int i=0; i<intvCapacity; i++){
		double test = pow((1+tdps->realPrecision), inv*(i - intvRadius));
		precisionTable[i] = test;
	}

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
			(*data)[i] = exactData;
			memcpy(preBytes,curBytes,8);
			predValue = (*data)[i];
			break;
		default:
			//predValue = 2 * (*data)[i-1] - (*data)[i-2];
			//predValue = (*data)[i-1];
			predValue = fabs(predValue) * precisionTable[type_];
			(*data)[i] = predValue;
			break;
		}
	}
	
#ifdef HAVE_TIMECMPR	
	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
		memcpy(multisteps->hist_data, (*data), dataSeriesLength*sizeof(double));
#endif	
	free(precisionTable);
	free(leadNum);
	free(type);
	return;
}

void getSnapshotData_double_1D(double** data, size_t dataSeriesLength, TightDataPointStorageD* tdps, int errBoundMode, int compressionType, double* hist_data, sz_params* pde_params) 
{
	size_t i;
	if (tdps->allSameData) {
		double value = bytesToDouble(tdps->exactMidBytes);
		*data = (double*)malloc(sizeof(double)*dataSeriesLength);
		for (i = 0; i < dataSeriesLength; i++)
			(*data)[i] = value;
	} else {
		if (tdps->rtypeArray == NULL) {
			if(errBoundMode < PW_REL)
			{
#ifdef HAVE_TIMECMPR				
				if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
				{
					if(multisteps->compressionType == 0) //snapshot
						decompressDataSeries_double_1D(data, dataSeriesLength, hist_data, tdps);
					else
						decompressDataSeries_double_1D_ts(data, dataSeriesLength, hist_data, tdps);					
				}
				else
#endif
					decompressDataSeries_double_1D(data, dataSeriesLength, hist_data, tdps);
			}
			else 
			{
				if(confparams_dec->accelerate_pw_rel_compression)
					decompressDataSeries_double_1D_pwr_pre_log_MSST19(data, dataSeriesLength, tdps);
				else
					decompressDataSeries_double_1D_pwr_pre_log(data, dataSeriesLength, tdps);
				//decompressDataSeries_double_1D_pwrgroup(data, dataSeriesLength, tdps);
			}
			return;
		} else {
			//TODO
		}
	}
}

