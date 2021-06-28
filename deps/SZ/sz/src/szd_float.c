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
#include "Huffman.h"
#include "szd_float_pwr.h"
#include "szd_float_ts.h"
#include "utility.h"


//struct timeval startTime_;
//struct timeval endTime_;  /* Start and end times */
//struct timeval costStart_; /*only used for recording the cost*/
//double totalCost_ = 0;

/*void cost_start_()
{
	totalCost_ = 0;
	gettimeofday(&costStart_, NULL);
}

void cost_end_()
{
	double elapsed;
	struct timeval costEnd;
	gettimeofday(&costEnd, NULL);
	elapsed = ((costEnd.tv_sec*1000000+costEnd.tv_usec)-(costStart_.tv_sec*1000000+costStart_.tv_usec))/1000000.0;
	totalCost_ += elapsed;
}*/


/**
 * 
 * int compressionType: 1 (time-based compression) ; 0 (space-based compression)
 * hist_data: only valid when compressionType==1, hist_data is the historical dataset such as the data in previous time step
 * 
 * @return status SUCCESSFUL (SZ_SCES) or not (other error codes) f
 * */
int SZ_decompress_args_float(float** newData, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, unsigned char* cmpBytes, 
size_t cmpSize, int compressionType, float* hist_data, sz_exedata* pde_exe, sz_params* pde_params)
{
	int status = SZ_SCES;
	size_t dataLength = computeDataLength(r5,r4,r3,r2,r1);
	
	//unsigned char* tmpBytes;
	size_t targetUncompressSize = dataLength <<2; //i.e., *4
	//tmpSize must be "much" smaller than dataLength
	size_t i, tmpSize = 8+MetaDataByteLength+pde_exe->SZ_SIZE_TYPE;
	unsigned char* szTmpBytes;	
	
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
		else if(pde_params->szMode==SZ_BEST_COMPRESSION || pde_params->szMode==SZ_DEFAULT_COMPRESSION || pde_params->szMode==SZ_TEMPORAL_COMPRESSION)
		{
			if(targetUncompressSize<MIN_ZLIB_DEC_ALLOMEM_BYTES) //Considering the minimum size
				targetUncompressSize = MIN_ZLIB_DEC_ALLOMEM_BYTES; 
			tmpSize = sz_lossless_decompress(pde_params->losslessCompressor, cmpBytes, (unsigned long)cmpSize, &szTmpBytes, (unsigned long)targetUncompressSize+4+MetaDataByteLength+exe_params->SZ_SIZE_TYPE);//		(unsigned long)targetUncompressSize+8: consider the total length under lossless compression mode is actually 3+4+1+targetUncompressSize
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
		
	//TODO: convert szTmpBytes to data array.
	TightDataPointStorageF* tdps;
	int errBoundMode = new_TightDataPointStorageF_fromFlatBytes(&tdps, szTmpBytes, tmpSize, pde_exe, pde_params);
	
	//writeByteData(tdps->typeArray, tdps->typeArray_size, "decompress-typebytes.tbt");
	int dim = computeDimension(r5,r4,r3,r2,r1);	
	int floatSize = sizeof(float);
	if(tdps->isLossless)
	{
		*newData = (float*)malloc(floatSize*dataLength);
		if(sysEndianType==BIG_ENDIAN_SYSTEM)
		{
			memcpy(*newData, szTmpBytes+4+MetaDataByteLength+exe_params->SZ_SIZE_TYPE, dataLength*floatSize);
		}
		else
		{
			unsigned char* p = szTmpBytes+4+MetaDataByteLength+exe_params->SZ_SIZE_TYPE;
			for(i=0;i<dataLength;i++,p+=floatSize)
				(*newData)[i] = bytesToFloat(p);
		}		
	}
	else if(pde_params->sol_ID==SZ_Transpose)
	{
		getSnapshotData_float_1D(newData,dataLength,tdps, errBoundMode, 0, hist_data, pde_params);		
	}
	else //pde_params->sol_ID==SZ
	{
		if(tdps->raBytes_size > 0) //v2.0
		{
			if (dim == 1)
				getSnapshotData_float_1D(newData,r1,tdps, errBoundMode, 0, hist_data, pde_params);
			else if(dim == 2)
				decompressDataSeries_float_2D_nonblocked_with_blocked_regression(newData, r2, r1, tdps->raBytes, hist_data);
			else if(dim == 3)
				decompressDataSeries_float_3D_nonblocked_with_blocked_regression(newData, r3, r2, r1, tdps->raBytes, hist_data);
			else if(dim == 4)
				decompressDataSeries_float_3D_nonblocked_with_blocked_regression(newData, r4*r3, r2, r1, tdps->raBytes, hist_data);
			else
			{
				printf("Error: currently support only at most 4 dimensions!\n");
				status = SZ_DERR;
			}	
		}
		else //1.4.13 or time-based compression
		{
			if (dim == 1)
				getSnapshotData_float_1D(newData,r1,tdps, errBoundMode, compressionType, hist_data, pde_params);
			else if (dim == 2)
				getSnapshotData_float_2D(newData,r2,r1,tdps, errBoundMode, compressionType, hist_data);
			else if (dim == 3)
				getSnapshotData_float_3D(newData,r3,r2,r1,tdps, errBoundMode, compressionType, hist_data);
			else if (dim == 4)
				getSnapshotData_float_4D(newData,r4,r3,r2,r1,tdps, errBoundMode, compressionType, hist_data);
			else
			{
				printf("Error: currently support only at most 4 dimensions!\n");
				status = SZ_DERR;
			}			
		}
	}

	//cost_start_();	
	if(pde_params->protectValueRange)
	{
		float* nd = *newData;
		float min = pde_params->fmin;
		float max = pde_params->fmax;		
		for(i=0;i<dataLength;i++)
		{
			float v = nd[i];
			if(v <= max && v >= min)
				continue;
			if(v < min)
				nd[i] = min;
			else if(v > max)
				nd[i] = max;
		}
	}
	//cost_end_();
	//printf("totalCost_=%f\n", totalCost_);
	free_TightDataPointStorageF2(tdps);
	if(pde_params->szMode!=SZ_BEST_SPEED && cmpSize!=8+MetaDataByteLength+exe_params->SZ_SIZE_TYPE)
		free(szTmpBytes);
	return status;
}

void decompressDataSeries_float_1D(float** data, size_t dataSeriesLength, float* hist_data, TightDataPointStorageF* tdps) 
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
	float medianValue, exactData, predValue;
	
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
			//predValue = 2 * (*data)[i-1] - (*data)[i-2];
			predValue = (*data)[i-1];
			(*data)[i] = predValue + (float)(type_-intvRadius)*interval;
			break;
		}
		//printf("%.30G\n",(*data)[i]);
	}
	
#ifdef HAVE_TIMECMPR	
	if(pde_params->szMode == SZ_TEMPORAL_COMPRESSION)
		memcpy(hist_data, (*data), dataSeriesLength*sizeof(float));
#endif	
	
	free(leadNum);
	free(type);
	return;
}

void decompressDataSeries_float_2D(float** data, size_t r1, size_t r2, float* hist_data, TightDataPointStorageF* tdps) 
{
	//updateQuantizationInfo(tdps->intervals);
	int intvRadius = tdps->intervals/2;
	//printf("tdps->intervals=%d, exe_params->intvRadius=%d\n", tdps->intervals, exe_params->intvRadius);
	
	size_t j, k = 0, p = 0, l = 0; // k is to track the location of residual_bit
	// in resiMidBits, p is to track the
	// byte_index of resiMidBits, l is for
	// leadNum
	size_t dataSeriesLength = r1*r2;
	//	printf ("%d %d\n", r1, r2);

	unsigned char* leadNum;
	float realPrecision = tdps->realPrecision;

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
	float medianValue, exactData;
	int type_;

	reqBytesLength = tdps->reqLength/8;
	resiBitsLength = tdps->reqLength%8;
	medianValue = tdps->medianValue;
	
	float pred1D, pred2D;
	size_t ii, jj;

	/* Process Row-0, data 0 */

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
	(*data)[0] = exactData + medianValue;
	memcpy(preBytes,curBytes,4);

	/* Process Row-0, data 1 */
	type_ = type[1]; 
	if (type_ != 0)
	{
		pred1D = (*data)[0];
		(*data)[1] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
	}
	else
	{
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
		(*data)[1] = exactData + medianValue;
		memcpy(preBytes,curBytes,4);
	}

	/* Process Row-0, data 2 --> data r2-1 */
	for (jj = 2; jj < r2; jj++)
	{
		type_ = type[jj];
		if (type_ != 0)
		{
			pred1D = 2*(*data)[jj-1] - (*data)[jj-2];				
			(*data)[jj] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
		}
		else
		{
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
			(*data)[jj] = exactData + medianValue;
			memcpy(preBytes,curBytes,4);
		}
	}

	size_t index;
	/* Process Row-1 --> Row-r1-1 */
	for (ii = 1; ii < r1; ii++)
	{
		/* Process row-ii data 0 */
		index = ii*r2;

		type_ = type[index];
		if (type_ != 0)
		{
			pred1D = (*data)[index-r2];		
			(*data)[index] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
		}
		else
		{
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
			(*data)[index] = exactData + medianValue;
			memcpy(preBytes,curBytes,4);
		}

		/* Process row-ii data 1 --> r2-1*/
		for (jj = 1; jj < r2; jj++)
		{
			index = ii*r2+jj;
			pred2D = (*data)[index-1] + (*data)[index-r2] - (*data)[index-r2-1];

			type_ = type[index];
			if (type_ != 0)
			{
				(*data)[index] = pred2D + 2 * (type_ - intvRadius) * realPrecision;
			}
			else
			{
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
				(*data)[index] = exactData + medianValue;
				memcpy(preBytes,curBytes,4);
			}
		}
	}

#ifdef HAVE_TIMECMPR	
	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
		memcpy(hist_data, (*data), dataSeriesLength*sizeof(float));
#endif	

	free(leadNum);
	free(type);
	return;
}

void decompressDataSeries_float_3D(float** data, size_t r1, size_t r2, size_t r3, float* hist_data, TightDataPointStorageF* tdps) 
{
	//updateQuantizationInfo(tdps->intervals);
	int intvRadius = tdps->intervals/2;
	size_t j, k = 0, p = 0, l = 0; // k is to track the location of residual_bit
	// in resiMidBits, p is to track the
	// byte_index of resiMidBits, l is for
	// leadNum
	size_t dataSeriesLength = r1*r2*r3;
	size_t r23 = r2*r3;
	unsigned char* leadNum;
	float realPrecision = tdps->realPrecision;

	//TODO
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
	float medianValue, exactData;
	int type_;

	reqBytesLength = tdps->reqLength/8;
	resiBitsLength = tdps->reqLength%8;
	medianValue = tdps->medianValue;
	
	float pred1D, pred2D, pred3D;
	size_t ii, jj, kk;

	///////////////////////////	Process layer-0 ///////////////////////////
	/* Process Row-0 data 0*/
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
	(*data)[0] = exactData + medianValue;
	memcpy(preBytes,curBytes,4);

	/* Process Row-0, data 1 */
	pred1D = (*data)[0];

	type_ = type[1];
	if (type_ != 0)
	{
		(*data)[1] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
	}
	else
	{
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
		(*data)[1] = exactData + medianValue;
		memcpy(preBytes,curBytes,4);
	}
	/* Process Row-0, data 2 --> data r3-1 */
	for (jj = 2; jj < r3; jj++)
	{
		pred1D = 2*(*data)[jj-1] - (*data)[jj-2];

		type_ = type[jj];
		if (type_ != 0)
		{
			(*data)[jj] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
		}
		else
		{
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
			(*data)[jj] = exactData + medianValue;
			memcpy(preBytes,curBytes,4);
		}
	}

	size_t index;
	/* Process Row-1 --> Row-r2-1 */
	for (ii = 1; ii < r2; ii++)
	{
		/* Process row-ii data 0 */
		index = ii*r3;
		pred1D = (*data)[index-r3];

		type_ = type[index];
		if (type_ != 0)
		{
			(*data)[index] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
		}
		else
		{
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
			(*data)[index] = exactData + medianValue;
			memcpy(preBytes,curBytes,4);
		}

		/* Process row-ii data 1 --> r3-1*/
		for (jj = 1; jj < r3; jj++)
		{
			index = ii*r3+jj;
			pred2D = (*data)[index-1] + (*data)[index-r3] - (*data)[index-r3-1];

			type_ = type[index];
			if (type_ != 0)
			{
				(*data)[index] = pred2D + 2 * (type_ - intvRadius) * realPrecision;
			}
			else
			{
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
				(*data)[index] = exactData + medianValue;
				memcpy(preBytes,curBytes,4);
			}
		}
	}

	///////////////////////////	Process layer-1 --> layer-r1-1 ///////////////////////////

	for (kk = 1; kk < r1; kk++)
	{
		/* Process Row-0 data 0*/
		index = kk*r23;
		pred1D = (*data)[index-r23];

		type_ = type[index];
		if (type_ != 0)
		{
			(*data)[index] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
		}
		else
		{
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
			(*data)[index] = exactData + medianValue;
			memcpy(preBytes,curBytes,4);
		}

		/* Process Row-0 data 1 --> data r3-1 */
		for (jj = 1; jj < r3; jj++)
		{
			index = kk*r23+jj;
			pred2D = (*data)[index-1] + (*data)[index-r23] - (*data)[index-r23-1];

			type_ = type[index];
			if (type_ != 0)
			{
				(*data)[index] = pred2D + 2 * (type_ - intvRadius) * realPrecision;
			}
			else
			{
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
				(*data)[index] = exactData + medianValue;
				memcpy(preBytes,curBytes,4);
			}
		}

		/* Process Row-1 --> Row-r2-1 */
		for (ii = 1; ii < r2; ii++)
		{
			/* Process Row-i data 0 */
			index = kk*r23 + ii*r3;
			pred2D = (*data)[index-r3] + (*data)[index-r23] - (*data)[index-r23-r3];

			type_ = type[index];
			if (type_ != 0)
			{
				(*data)[index] = pred2D + 2 * (type_ - intvRadius) * realPrecision;
			}
			else
			{
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
				(*data)[index] = exactData + medianValue;
				memcpy(preBytes,curBytes,4);
			}

			/* Process Row-i data 1 --> data r3-1 */
			for (jj = 1; jj < r3; jj++)
			{
				index = kk*r23 + ii*r3 + jj;
				pred3D = (*data)[index-1] + (*data)[index-r3] + (*data)[index-r23]
					- (*data)[index-r3-1] - (*data)[index-r23-r3] - (*data)[index-r23-1] + (*data)[index-r23-r3-1];

				type_ = type[index];
				if (type_ != 0)
				{
					(*data)[index] = pred3D + 2 * (type_ - intvRadius) * realPrecision;
				}
				else
				{
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
					(*data)[index] = exactData + medianValue;
					memcpy(preBytes,curBytes,4);
				}
			}
		}
	}
	
#ifdef HAVE_TIMECMPR	
	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
		memcpy(hist_data, (*data), dataSeriesLength*sizeof(float));
#endif		

	free(leadNum);
	free(type);
	return;
}

void decompressDataSeries_float_4D(float** data, size_t r1, size_t r2, size_t r3, size_t r4, float* hist_data, TightDataPointStorageF* tdps)
{
	//updateQuantizationInfo(tdps->intervals);
	int intvRadius = tdps->intervals;
	size_t j, k = 0, p = 0, l = 0; // k is to track the location of residual_bit
	// in resiMidBits, p is to track the
	// byte_index of resiMidBits, l is for
	// leadNum
	size_t dataSeriesLength = r1*r2*r3*r4;
	size_t r234 = r2*r3*r4;
	size_t r34 = r3*r4;
//	printf ("%d %d %d %d\n", r1, r2, r3, r4);
	unsigned char* leadNum;
	double realPrecision = tdps->realPrecision;

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
	float medianValue, exactData;
	int type_;

	reqBytesLength = tdps->reqLength/8;
	resiBitsLength = tdps->reqLength%8;
	medianValue = tdps->medianValue;

	float pred1D, pred2D, pred3D;
	size_t ii, jj, kk, ll;
	size_t index;

	for (ll = 0; ll < r1; ll++)
	{

		///////////////////////////	Process layer-0 ///////////////////////////
		/* Process Row-0 data 0*/
		index = ll*r234;

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
		(*data)[index] = exactData + medianValue;
		memcpy(preBytes,curBytes,4);

		/* Process Row-0, data 1 */
		index = ll*r234+1;

		pred1D = (*data)[index-1];

		type_ = type[index];
		if (type_ != 0)
		{
			(*data)[index] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
		}
		else
		{
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
			(*data)[index] = exactData + medianValue;
			memcpy(preBytes,curBytes,4);
		}

		/* Process Row-0, data 2 --> data r4-1 */
		for (jj = 2; jj < r4; jj++)
		{
			index = ll*r234+jj;

			pred1D = 2*(*data)[index-1] - (*data)[index-2];

			type_ = type[index];
			if (type_ != 0)
			{
				(*data)[index] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
			}
			else
			{
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
				(*data)[index] = exactData + medianValue;
				memcpy(preBytes,curBytes,4);
			}
		}

		/* Process Row-1 --> Row-r3-1 */
		for (ii = 1; ii < r3; ii++)
		{
			/* Process row-ii data 0 */
			index = ll*r234+ii*r4;

			pred1D = (*data)[index-r4];

			type_ = type[index];
			if (type_ != 0)
			{
				(*data)[index] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
			}
			else
			{
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
				(*data)[index] = exactData + medianValue;
				memcpy(preBytes,curBytes,4);
			}

			/* Process row-ii data 1 --> r4-1*/
			for (jj = 1; jj < r4; jj++)
			{
				index = ll*r234+ii*r4+jj;

				pred2D = (*data)[index-1] + (*data)[index-r4] - (*data)[index-r4-1];

				type_ = type[index];
				if (type_ != 0)
				{
					(*data)[index] = pred2D + 2 * (type_ - intvRadius) * realPrecision;
				}
				else
				{
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
					(*data)[index] = exactData + medianValue;
					memcpy(preBytes,curBytes,4);
				}
			}
		}

		///////////////////////////	Process layer-1 --> layer-r2-1 ///////////////////////////

		for (kk = 1; kk < r2; kk++)
		{
			/* Process Row-0 data 0*/
			index = ll*r234+kk*r34;

			pred1D = (*data)[index-r34];

			type_ = type[index];
			if (type_ != 0)
			{
				(*data)[index] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
			}
			else
			{
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
				(*data)[index] = exactData + medianValue;
				memcpy(preBytes,curBytes,4);
			}

			/* Process Row-0 data 1 --> data r4-1 */
			for (jj = 1; jj < r4; jj++)
			{
				index = ll*r234+kk*r34+jj;

				pred2D = (*data)[index-1] + (*data)[index-r34] - (*data)[index-r34-1];

				type_ = type[index];
				if (type_ != 0)
				{
					(*data)[index] = pred2D + 2 * (type_ - intvRadius) * realPrecision;
				}
				else
				{
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
					(*data)[index] = exactData + medianValue;
					memcpy(preBytes,curBytes,4);
				}
			}

			/* Process Row-1 --> Row-r3-1 */
			for (ii = 1; ii < r3; ii++)
			{
				/* Process Row-i data 0 */
				index = ll*r234+kk*r34+ii*r4;

				pred2D = (*data)[index-r4] + (*data)[index-r34] - (*data)[index-r34-r4];

				type_ = type[index];
				if (type_ != 0)
				{
					(*data)[index] = pred2D + 2 * (type_ - intvRadius) * realPrecision;
				}
				else
				{
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
					(*data)[index] = exactData + medianValue;
					memcpy(preBytes,curBytes,4);
				}

				/* Process Row-i data 1 --> data r4-1 */
				for (jj = 1; jj < r4; jj++)
				{
					index = ll*r234+kk*r34+ii*r4+jj;

					pred3D = (*data)[index-1] + (*data)[index-r4] + (*data)[index-r34]
							- (*data)[index-r4-1] - (*data)[index-r34-r4] - (*data)[index-r34-1] + (*data)[index-r34-r4-1];


					type_ = type[index];
					if (type_ != 0)
					{
						(*data)[index] = pred3D + 2 * (type_ - intvRadius) * realPrecision;
					}
					else
					{
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
						(*data)[index] = exactData + medianValue;
						memcpy(preBytes,curBytes,4);
					}
				}
			}

		}
	}

//I didn't implement time-based compression for 4D actually. 
//#ifdef HAVE_TIMECMPR	
//	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
//		memcpy(multisteps->hist_data, (*data), dataSeriesLength*sizeof(float));
//#endif	

	free(leadNum);
	free(type);
	return;
}

/*MSST19*/
void decompressDataSeries_float_1D_MSST19(float** data, size_t dataSeriesLength, TightDataPointStorageF* tdps) 
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
	*data = (float*)malloc(sizeof(float)*dataSeriesLength);

	int* type = (int*)malloc(dataSeriesLength*sizeof(int));
	
	HuffmanTree* huffmanTree = createHuffmanTree(tdps->stateNum);
	decode_withTree_MSST19(huffmanTree, tdps->typeArray, dataSeriesLength, type, tdps->max_bits);
	SZ_ReleaseHuffman(huffmanTree);	
	unsigned char preBytes[4];
	unsigned char curBytes[4];
	
	memset(preBytes, 0, 4);

	size_t curByteIndex = 0;
	int reqBytesLength, resiBitsLength, resiBits; 
	unsigned char leadingNum;	
	float exactData, predValue = 0;
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
			(*data)[i] = exactData;
			memcpy(preBytes,curBytes,4);
			predValue = (*data)[i];
			break;
		default:
			//predValue = 2 * (*data)[i-1] - (*data)[i-2];
			//predValue = (*data)[i-1];
			predValue = fabs(predValue) * precisionTable[type_];			
			(*data)[i] = predValue;
			break;
		}
		//printf("%.30G\n",(*data)[i]);
	}
	
#ifdef HAVE_TIMECMPR	
	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
		memcpy(multisteps->hist_data, (*data), dataSeriesLength*sizeof(float));
#endif	
	free(precisionTable);
	free(leadNum);
	free(type);
	return;
}

void decompressDataSeries_float_2D_MSST19(float** data, size_t r1, size_t r2, TightDataPointStorageF* tdps) 
{
	//updateQuantizationInfo(tdps->intervals);
	int intvRadius = tdps->intervals/2;
	int intvCapacity = tdps->intervals;
	
	size_t j, k = 0, p = 0, l = 0; // k is to track the location of residual_bit
	// in resiMidBits, p is to track the
	// byte_index of resiMidBits, l is for
	// leadNum
	size_t dataSeriesLength = r1*r2;

	unsigned char* leadNum;
	//double realPrecision = tdps->realPrecision;

	convertByteArray2IntArray_fast_2b(tdps->exactDataNum, tdps->leadNumArray, tdps->leadNumArray_size, &leadNum);

	*data = (float*)malloc(sizeof(float)*dataSeriesLength);

    int* type = (int*)malloc(dataSeriesLength*sizeof(int));

	HuffmanTree* huffmanTree = createHuffmanTree(tdps->stateNum);
	decode_withTree_MSST19(huffmanTree, tdps->typeArray, dataSeriesLength, type, tdps->max_bits);
	SZ_ReleaseHuffman(huffmanTree);	

	unsigned char preBytes[4];
	unsigned char curBytes[4];

	memset(preBytes, 0, 4);

	size_t curByteIndex = 0;
	int reqBytesLength, resiBitsLength, resiBits; 
	unsigned char leadingNum;	
	float exactData;
	int type_;

    double* precisionTable = (double*)malloc(sizeof(double) * intvCapacity);
    double inv = 2.0-pow(2, -(tdps->plus_bits));
    for(int i=0; i<intvCapacity; i++){
        double test = pow((1+tdps->realPrecision), inv*(i - intvRadius));
        precisionTable[i] = test;
    }

    reqBytesLength = tdps->reqLength/8;
	resiBitsLength = tdps->reqLength%8;
	
	float pred1D, pred2D;
	size_t ii, jj;

	/* Process Row-0, data 0 */

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
	(*data)[0] = exactData;
	memcpy(preBytes,curBytes,4);

	/* Process Row-0, data 1 */
	type_ = type[1]; 
	if (type_ != 0)
	{
		pred1D = (*data)[0];
		(*data)[1] = fabs(pred1D) * precisionTable[type_];
	}
	else
	{
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
		(*data)[1] = exactData;
		memcpy(preBytes,curBytes,4);
	}

	/* Process Row-0, data 2 --> data r2-1 */
	for (jj = 2; jj < r2; jj++)
	{
		type_ = type[jj];
		if (type_ != 0)
		{
			pred1D = (*data)[jj-1] * (*data)[jj-1] / (*data)[jj-2];
			(*data)[jj] = fabs(pred1D) * precisionTable[type_];
		}
		else
		{
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
			(*data)[jj] = exactData;
			memcpy(preBytes,curBytes,4);
		}
	}

	size_t index;
	/* Process Row-1 --> Row-r1-1 */
	for (ii = 1; ii < r1; ii++)
	{
		/* Process row-ii data 0 */
		index = ii*r2;

		type_ = type[index];
		if (type_ != 0)
		{
			pred1D = (*data)[index-r2];		
			(*data)[index] = fabs(pred1D) * precisionTable[type_];
		}
		else
		{
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
			(*data)[index] = exactData;
			memcpy(preBytes,curBytes,4);
		}

		/* Process row-ii data 1 --> r2-1*/
		for (jj = 1; jj < r2; jj++)
		{
			index = ii*r2+jj;
			pred2D = (*data)[index-1] * (*data)[index-r2] / (*data)[index-r2-1];

			type_ = type[index];
			if (type_ != 0)
			{
				(*data)[index] = fabs(pred2D) * precisionTable[type_];
			}
			else
			{
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
				(*data)[index] = exactData;
				memcpy(preBytes,curBytes,4);
			}
		}
	}

#ifdef HAVE_TIMECMPR	
	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
		memcpy(multisteps->hist_data, (*data), dataSeriesLength*sizeof(float));
#endif	

	free(leadNum);
	free(type);
	return;
}

void decompressDataSeries_float_3D_MSST19(float** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageF* tdps) 
{
	//updateQuantizationInfo(tdps->intervals);
	int intvRadius = tdps->intervals/2;
	int intvCapacity = tdps->intervals;
	size_t j, k = 0, p = 0, l = 0; // k is to track the location of residual_bit
	// in resiMidBits, p is to track the
	// byte_index of resiMidBits, l is for
	// leadNum
	size_t dataSeriesLength = r1*r2*r3;
	size_t r23 = r2*r3;
	unsigned char* leadNum;
	//double realPrecision = tdps->realPrecision;

	convertByteArray2IntArray_fast_2b(tdps->exactDataNum, tdps->leadNumArray, tdps->leadNumArray_size, &leadNum);

	*data = (float*)malloc(sizeof(float)*dataSeriesLength);
	int* type = (int*)malloc(dataSeriesLength*sizeof(int));

	double* precisionTable = (double*)malloc(sizeof(double) * intvCapacity);
	double inv = 2.0-pow(2, -(tdps->plus_bits));
	for(int i=0; i<intvCapacity; i++){
		double test = pow((1+tdps->realPrecision), inv*(i - intvRadius));
		precisionTable[i] = test;
	}

	HuffmanTree* huffmanTree = createHuffmanTree(tdps->stateNum);
	decode_withTree_MSST19(huffmanTree, tdps->typeArray, dataSeriesLength, type, tdps->max_bits);
	SZ_ReleaseHuffman(huffmanTree);

	unsigned char preBytes[4];
	unsigned char curBytes[4];

	memset(preBytes, 0, 4);
	size_t curByteIndex = 0;
	int reqBytesLength, resiBitsLength, resiBits;
	unsigned char leadingNum;
	float exactData;
	int type_;

	reqBytesLength = tdps->reqLength/8;
	resiBitsLength = tdps->reqLength%8;
	
	float pred1D, pred2D, pred3D;
	double temp;
	double temp2;
	size_t ii, jj, kk;

	///////////////////////////	Process layer-0 ///////////////////////////
	/* Process Row-0 data 0*/
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
	(*data)[0] = exactData;
	memcpy(preBytes,curBytes,4);

	/* Process Row-0, data 1 */
	pred1D = (*data)[0];

	type_ = type[1];
	if (type_ != 0)
	{
		(*data)[1] = fabs(pred1D) * precisionTable[type_];
	}
	else
	{
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
		(*data)[1] = exactData;
		memcpy(preBytes,curBytes,4);
	}
	/* Process Row-0, data 2 --> data r3-1 */
	for (jj = 2; jj < r3; jj++)
	{
		temp = (*data)[jj-1];
		pred1D = temp * ( *data)[jj-1] / (*data)[jj-2];

		type_ = type[jj];
		if (type_ != 0)
		{
			(*data)[jj] = fabsf(pred1D) * precisionTable[type_];
		}
		else
		{
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
			(*data)[jj] = exactData;
			memcpy(preBytes,curBytes,4);
		}
	}

	size_t index;
	/* Process Row-1 --> Row-r2-1 */
	for (ii = 1; ii < r2; ii++)
	{
		/* Process row-ii data 0 */
		index = ii*r3;
		pred1D = (*data)[index-r3];

		type_ = type[index];
		if (type_ != 0)
		{
			(*data)[index] = fabsf(pred1D) * precisionTable[type_];
		}
		else
		{
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
			(*data)[index] = exactData;
			memcpy(preBytes,curBytes,4);
		}

		/* Process row-ii data 1 --> r3-1*/
		for (jj = 1; jj < r3; jj++)
		{
			index = ii*r3+jj;
			temp = (*data)[index-1];
			pred2D = temp * (*data)[index-r3] / (*data)[index-r3-1];

			type_ = type[index];
			if (type_ != 0)
			{
			    //float ppp = precisionTable[type_];
			    //float test = fabsf(pred2D) * precisionTable[type_];
				(*data)[index] = fabsf(pred2D) * precisionTable[type_];
			}
			else
			{
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
				(*data)[index] = exactData;
				memcpy(preBytes,curBytes,4);
			}
		}
	}

	///////////////////////////	Process layer-1 --> layer-r1-1 ///////////////////////////

	for (kk = 1; kk < r1; kk++)
	{
		/* Process Row-0 data 0*/
		index = kk*r23;
		pred1D = (*data)[index-r23];

		type_ = type[index];
		if (type_ != 0)
		{
			(*data)[index] = fabsf(pred1D) * precisionTable[type_];
		}
		else
		{
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
			(*data)[index] = exactData;
			memcpy(preBytes,curBytes,4);
		}

		/* Process Row-0 data 1 --> data r3-1 */
		for (jj = 1; jj < r3; jj++)
		{
			index = kk*r23+jj;
			temp = (*data)[index-1];
			pred2D = temp * (*data)[index-r23] / (*data)[index-r23-1];

			type_ = type[index];
			if (type_ != 0)
			{
				(*data)[index] = fabsf(pred2D) * precisionTable[type_];
			}
			else
			{
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
				(*data)[index] = exactData;
				memcpy(preBytes,curBytes,4);
			}
		}

		/* Process Row-1 --> Row-r2-1 */
		for (ii = 1; ii < r2; ii++)
		{
			/* Process Row-i data 0 */
			index = kk*r23 + ii*r3;
			temp = (*data)[index-r3];
			pred2D = temp * (*data)[index-r23] / (*data)[index-r23-r3];

			type_ = type[index];
			if (type_ != 0)
			{
				(*data)[index] = fabsf(pred2D) * precisionTable[type_];
			}
			else
			{
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
				(*data)[index] = exactData;
				memcpy(preBytes,curBytes,4);
			}

			/* Process Row-i data 1 --> data r3-1 */
			for (jj = 1; jj < r3; jj++)
			{
				index = kk*r23 + ii*r3 + jj;
				//pred3D = (*data)[index-1] + (*data)[index-r3] + (*data)[index-r23]
				//	- (*data)[index-r3-1] - (*data)[index-r23-r3] - (*data)[index-r23-1] + (*data)[index-r23-r3-1];
				temp = (*data)[index-1];
				temp2 = (*data)[index-r3-1];
				pred3D = temp * (*data)[index-r3] * (*data)[index-r23] * (*data)[index-r23-r3-1] / (temp2 * (*data)[index-r23-r3] * (*data)[index-r23-1]);

				type_ = type[index];				
				if (type_ != 0)
				{
					(*data)[index] = fabsf(pred3D) * precisionTable[type_];
				}
				else
				{
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
					(*data)[index] = exactData;
					memcpy(preBytes,curBytes,4);
				}
			}
		}
	}
	
#ifdef HAVE_TIMECMPR	
	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
		memcpy(multisteps->hist_data, (*data), dataSeriesLength*sizeof(float));
#endif		

	free(leadNum);
	free(type);
	return;
}

void getSnapshotData_float_1D(float** data, size_t dataSeriesLength, TightDataPointStorageF* tdps, int errBoundMode, int compressionType, float* hist_data, sz_params* pde_params)
{	
	size_t i;

	if (tdps->allSameData) {
		float value = bytesToFloat(tdps->exactMidBytes);
		*data = (float*)malloc(sizeof(float)*dataSeriesLength);
		for (i = 0; i < dataSeriesLength; i++)
			(*data)[i] = value;
	} else {
		if (tdps->rtypeArray == NULL) {
			if(errBoundMode < PW_REL)
			{
#ifdef HAVE_TIMECMPR				
				if(pde_params->szMode == SZ_TEMPORAL_COMPRESSION)
				{
					if(compressionType == 0) //snapshot
						decompressDataSeries_float_1D(data, dataSeriesLength, hist_data, tdps);
					else
						decompressDataSeries_float_1D_ts(data, dataSeriesLength, hist_data, tdps);					
				}
				else
#endif				
					decompressDataSeries_float_1D(data, dataSeriesLength, hist_data, tdps);
			}
			else 
			{
				if(pde_params->accelerate_pw_rel_compression)
					decompressDataSeries_float_1D_pwr_pre_log_MSST19(data, dataSeriesLength, tdps);
				else
					decompressDataSeries_float_1D_pwr_pre_log(data, dataSeriesLength, tdps);
				//decompressDataSeries_float_1D_pwrgroup(data, dataSeriesLength, tdps);
			}
			return;
		} else { //the special version supporting one value to reserve
			//TODO
		}
	}
}

void getSnapshotData_float_2D(float** data, size_t r1, size_t r2, TightDataPointStorageF* tdps, int errBoundMode, int compressionType, float* hist_data) 
{
	size_t i;
	size_t dataSeriesLength = r1*r2;
	if (tdps->allSameData) {
		float value = bytesToFloat(tdps->exactMidBytes);
		*data = (float*)malloc(sizeof(float)*dataSeriesLength);
		for (i = 0; i < dataSeriesLength; i++)
			(*data)[i] = value;
	} else {
		if (tdps->rtypeArray == NULL) {
			if(errBoundMode < PW_REL)
			{
#ifdef HAVE_TIMECMPR					
				if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
				{
					if(compressionType == 0)
						decompressDataSeries_float_2D(data, r1, r2, hist_data, tdps);
					else
						decompressDataSeries_float_1D_ts(data, dataSeriesLength, hist_data, tdps);					
				}
				else
#endif
					decompressDataSeries_float_2D(data, r1, r2, hist_data, tdps);
			}
			else 
			{
				//decompressDataSeries_float_2D_pwr(data, r1, r2, tdps);
				if(confparams_dec->accelerate_pw_rel_compression)
					decompressDataSeries_float_2D_pwr_pre_log_MSST19(data, r1, r2, tdps);
				else
					decompressDataSeries_float_2D_pwr_pre_log(data, r1, r2, tdps);
			}			

			return;
		} else {
			//TODO
		}
	}
}

void getSnapshotData_float_3D(float** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageF* tdps, int errBoundMode, int compressionType, float* hist_data)
{
	size_t i;
	size_t dataSeriesLength = r1*r2*r3;
	if (tdps->allSameData) {
		float value = bytesToFloat(tdps->exactMidBytes);
		*data = (float*)malloc(sizeof(float)*dataSeriesLength);
		for (i = 0; i < dataSeriesLength; i++)
			(*data)[i] = value;
	} else {
		if (tdps->rtypeArray == NULL) {
			if(errBoundMode < PW_REL)
			{
#ifdef HAVE_TIMECMPR					
				if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
				{
					if(compressionType == 0)
						decompressDataSeries_float_3D(data, r1, r2, r3, hist_data, tdps);
					else
						decompressDataSeries_float_1D_ts(data, dataSeriesLength, hist_data, tdps);					
				}
				else
#endif				
					decompressDataSeries_float_3D(data, r1, r2, r3, hist_data, tdps);
			}
			else 
			{
				//decompressDataSeries_float_3D_pwr(data, r1, r2, r3, tdps);
				if(confparams_dec->accelerate_pw_rel_compression)
					decompressDataSeries_float_3D_pwr_pre_log_MSST19(data, r1, r2, r3, tdps);
				else
					decompressDataSeries_float_3D_pwr_pre_log(data, r1, r2, r3, tdps);
			}					
			
			return;
		} else {
			//TODO
		}
	}
}

void getSnapshotData_float_4D(float** data, size_t r1, size_t r2, size_t r3, size_t r4, TightDataPointStorageF* tdps, int errBoundMode, int compressionType, float* hist_data)
{
	size_t i;
	size_t dataSeriesLength = r1*r2*r3*r4;
	if (tdps->allSameData) {
		float value = bytesToFloat(tdps->exactMidBytes);
		*data = (float*)malloc(sizeof(float)*dataSeriesLength);
		for (i = 0; i < dataSeriesLength; i++)
			(*data)[i] = value;
	} else {
		if (tdps->rtypeArray == NULL) {
			if(errBoundMode < PW_REL)
			{
#ifdef HAVE_TIMECMPR					
				if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
				{
					if(compressionType == 0)
						decompressDataSeries_float_4D(data, r1, r2, r3, r4, hist_data, tdps);
					else
						decompressDataSeries_float_1D_ts(data, r1*r2*r3*r4, hist_data, tdps);					
				}
				else
#endif				
					decompressDataSeries_float_4D(data, r1, r2, r3, r4, hist_data, tdps);
			}
			else 
			{
				if(confparams_dec->accelerate_pw_rel_compression)
					decompressDataSeries_float_3D_pwr_pre_log_MSST19(data, r1*r2, r3, r4, tdps);
				else
					decompressDataSeries_float_3D_pwr_pre_log(data, r1*r2, r3, r4, tdps);
				//decompressDataSeries_float_4D_pwr(data, r1, r2, r3, r4, tdps);
			}					
			return;
		} else {
			//TODO
		}
	}
}

size_t decompressDataSeries_float_3D_RA_block(float * data, float mean, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, double realPrecision, int * type, float * unpredictable_data){
	int intvRadius = exe_params->intvRadius;
	size_t dim0_offset = dim_1 * dim_2;
	size_t dim1_offset = dim_2;
	// printf("SZ_compress_float_3D_MDQ_RA_block real dim: %d %d %d\n", real_block_dims[0], real_block_dims[1], real_block_dims[2]);
	// fflush(stdout);

	size_t unpredictable_count = 0;
	size_t r1, r2, r3;
	r1 = block_dim_0;
	r2 = block_dim_1;
	r3 = block_dim_2;

	float * cur_data_pos = data;
	float * last_row_pos;
	float pred1D, pred2D, pred3D;
	size_t i, j, k;
	size_t r23 = r2*r3;
	int type_;
	// Process Row-0 data 0
	pred1D = mean;
	type_ = type[0];
	// printf("Type 0 %d, mean %.4f\n", type_, mean);
	if (type_ != 0){
		cur_data_pos[0] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
	}
	else{
		cur_data_pos[0] = unpredictable_data[unpredictable_count ++];
	}

	/* Process Row-0 data 1*/
	pred1D = cur_data_pos[0];
	type_ = type[1];
	if (type_ != 0){
		cur_data_pos[1] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
	}
	else{
		cur_data_pos[1] = unpredictable_data[unpredictable_count ++];
	}
    /* Process Row-0 data 2 --> data r3-1 */
	for (j = 2; j < r3; j++){
		pred1D = 2*cur_data_pos[j-1] - cur_data_pos[j-2];
		type_ = type[j];
		if (type_ != 0){
			cur_data_pos[j] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
		}
		else{
			cur_data_pos[j] = unpredictable_data[unpredictable_count ++];
		}
	}

	last_row_pos = cur_data_pos;
	cur_data_pos += dim1_offset;
	// printf("SZ_compress_float_3D_MDQ_RA_block row 0 done, cur_data_pos: %ld\n", cur_data_pos - block_ori_data);
	// fflush(stdout);

	/* Process Row-1 --> Row-r2-1 */
	size_t index;
	for (i = 1; i < r2; i++)
	{
		/* Process row-i data 0 */
		index = i*r3;	
		pred1D = last_row_pos[0];
		type_ = type[index];
		if (type_ != 0){
			cur_data_pos[0] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
		}
		else{
			cur_data_pos[0] = unpredictable_data[unpredictable_count ++];
		}
		/* Process row-i data 1 --> data r3-1*/
		for (j = 1; j < r3; j++)
		{
			index = i*r3+j;
			pred2D = cur_data_pos[j-1] + last_row_pos[j] - last_row_pos[j-1];
			type_ = type[index];
			if (type_ != 0){
				cur_data_pos[j] = pred2D + 2 * (type_ - intvRadius) * realPrecision;
			}
			else{
				cur_data_pos[j] = unpredictable_data[unpredictable_count ++];
			}
			// printf("pred2D %.2f cur_data %.2f last_row_data %.2f %.2f, result %.2f\n", pred2D, cur_data_pos[j-1], last_row_pos[j], last_row_pos[j-1], cur_data_pos[j]);
			// getchar();
		}
		last_row_pos = cur_data_pos;
		cur_data_pos += dim1_offset;
	}
	cur_data_pos += dim0_offset - r2 * dim1_offset;

	// printf("SZ_compress_float_3D_MDQ_RA_block layer 0 done, cur_data_pos: %ld\n", cur_data_pos - block_ori_data);
	// fflush(stdout);
	// exit(0);

	///////////////////////////	Process layer-1 --> layer-r1-1 ///////////////////////////

	for (k = 1; k < r1; k++)
	{
		// if(idx == 63 && idy == 63 && idz == 63){
		// 	printf("SZ_compress_float_3D_MDQ_RA_block layer %d done, cur_data_pos: %ld\n", k-1, cur_data_pos - data);
		// 	fflush(stdout);
		// }
		/* Process Row-0 data 0*/
		index = k*r23;
		pred1D = cur_data_pos[- dim0_offset];
		type_ = type[index];
		if (type_ != 0){
			cur_data_pos[0] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
		}
		else{
			cur_data_pos[0] = unpredictable_data[unpredictable_count ++];
		}
	    /* Process Row-0 data 1 --> data r3-1 */
		for (j = 1; j < r3; j++)
		{
			//index = k*r2*r3+j;
			index ++;
			pred2D = cur_data_pos[j-1] + cur_data_pos[j - dim0_offset] - cur_data_pos[j - 1 - dim0_offset];
			type_ = type[index];
			if (type_ != 0){
				cur_data_pos[j] = pred2D + 2 * (type_ - intvRadius) * realPrecision;
			}
			else{
				cur_data_pos[j] = unpredictable_data[unpredictable_count ++];
			}
			// printf("pred2D %.2f cur_data %.2f %.2f %.2f, result %.2f\n", pred2D, cur_data_pos[j-1], cur_data_pos[j - dim0_offset], cur_data_pos[j - 1 - dim0_offset], cur_data_pos[j]);
			// getchar();
		}
		last_row_pos = cur_data_pos;
		cur_data_pos += dim1_offset;

		// if(idx == 63 && idy == 63 && idz == 63){
		// 	printf("SZ_compress_float_3D_MDQ_RA_block layer row 0 done, cur_data_pos: %ld\n", k-1, cur_data_pos - data);
		// 	fflush(stdout);
		// }

	    /* Process Row-1 --> Row-r2-1 */
		for (i = 1; i < r2; i++)
		{
			// if(idx == 63 && idy == 63 && idz == 63){
			// 	printf("SZ_compress_float_3D_MDQ_RA_block layer row %d done, cur_data_pos: %ld\n", i-1, cur_data_pos - data);
			// 	fflush(stdout);
			// }
			/* Process Row-i data 0 */
			index = k*r23 + i*r3;
			pred2D = last_row_pos[0] + cur_data_pos[- dim0_offset] - last_row_pos[- dim0_offset];
			type_ = type[index];
			if (type_ != 0){
				cur_data_pos[0] = pred2D + 2 * (type_ - intvRadius) * realPrecision;
			}
			else{
				cur_data_pos[0] = unpredictable_data[unpredictable_count ++];
			}

			/* Process Row-i data 1 --> data r3-1 */
			for (j = 1; j < r3; j++)
			{
//				if(k==63&&i==43&&j==27)
//					printf("i=%d\n", i);
				//index = k*r2*r3 + i*r3 + j;			
				index ++;
				pred3D = cur_data_pos[j-1] + last_row_pos[j]+ cur_data_pos[j - dim0_offset] - last_row_pos[j-1] - last_row_pos[j - dim0_offset] - cur_data_pos[j-1 - dim0_offset] + last_row_pos[j-1 - dim0_offset];
				type_ = type[index];
				if (type_ != 0){
					cur_data_pos[j] = pred3D + 2 * (type_ - intvRadius) * realPrecision;
				}
				else{
					cur_data_pos[j] = unpredictable_data[unpredictable_count ++];
				}
			}
			last_row_pos = cur_data_pos;
			cur_data_pos += dim1_offset;
		}
		cur_data_pos += dim0_offset - r2 * dim1_offset;
	}

	return unpredictable_count;
}

size_t decompressDataSeries_float_1D_RA_block(float * data, float mean, size_t dim_0, size_t block_dim_0, double realPrecision, int * type, float * unpredictable_data){
	int intvRadius = exe_params->intvRadius;
	size_t unpredictable_count = 0;
	
	float * cur_data_pos = data;
	size_t type_index = 0;
	int type_;
	float last_over_thres = mean;
	for(size_t i=0; i<block_dim_0; i++){
		type_ = type[type_index];
		if(type_ == 0){
			cur_data_pos[0] = unpredictable_data[unpredictable_count ++];
			last_over_thres = cur_data_pos[0];
		}
		else{
			cur_data_pos[0] = last_over_thres + 2 * (type_ - intvRadius) * realPrecision;
			last_over_thres = cur_data_pos[0];
		}

		type_index ++;
		cur_data_pos ++;
	}

	return unpredictable_count;
}

size_t decompressDataSeries_float_2D_RA_block(float * data, float mean, size_t dim_0, size_t dim_1, size_t block_dim_0, size_t block_dim_1, double realPrecision, int * type, float * unpredictable_data){
	int intvRadius = exe_params->intvRadius;
	size_t dim0_offset = dim_1;
	// printf("SZ_compress_float_3D_MDQ_RA_block real dim: %d %d %d\n", real_block_dims[0], real_block_dims[1], real_block_dims[2]);
	// fflush(stdout);

	size_t unpredictable_count = 0;
	size_t r1, r2;
	r1 = block_dim_0;
	r2 = block_dim_1;

	float * cur_data_pos = data;
	float * last_row_pos;
	float pred1D, pred2D;
	size_t i, j;
	int type_;
	// Process Row-0 data 0
	pred1D = mean;
	type_ = type[0];
	// printf("Type 0 %d, mean %.4f\n", type_, mean);
	if (type_ != 0){
		cur_data_pos[0] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
	}
	else{
		cur_data_pos[0] = unpredictable_data[unpredictable_count ++];
	}

	/* Process Row-0 data 1*/
	pred1D = cur_data_pos[0];
	type_ = type[1];
	if (type_ != 0){
		cur_data_pos[1] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
	}
	else{
		cur_data_pos[1] = unpredictable_data[unpredictable_count ++];
	}
    /* Process Row-0 data 2 --> data r3-1 */
	for (j = 2; j < r2; j++){
		pred1D = 2*cur_data_pos[j-1] - cur_data_pos[j-2];
		type_ = type[j];
		if (type_ != 0){
			cur_data_pos[j] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
		}
		else{
			cur_data_pos[j] = unpredictable_data[unpredictable_count ++];
		}
	}

	last_row_pos = cur_data_pos;
	cur_data_pos += dim0_offset;
	// printf("SZ_compress_float_3D_MDQ_RA_block row 0 done, cur_data_pos: %ld\n", cur_data_pos - block_ori_data);
	// fflush(stdout);

	/* Process Row-1 --> Row-r2-1 */
	size_t index;
	for (i = 1; i < r1; i++)
	{
		/* Process row-i data 0 */
		index = i*r2;	
		type_ = type[index];
		if (type_ != 0){
			pred1D = last_row_pos[0];
			cur_data_pos[0] = pred1D + 2 * (type_ - intvRadius) * realPrecision;
		}
		else{
			cur_data_pos[0] = unpredictable_data[unpredictable_count ++];
		}
		/* Process row-i data 1 --> data r3-1*/
		for (j = 1; j < r2; j++)
		{
			index = i*r2+j;
			pred2D = cur_data_pos[j-1] + last_row_pos[j] - last_row_pos[j-1];
			type_ = type[index];
			if (type_ != 0){
				cur_data_pos[j] = pred2D + 2 * (type_ - intvRadius) * realPrecision;
			}
			else{
				cur_data_pos[j] = unpredictable_data[unpredictable_count ++];
			}
			// printf("pred2D %.2f cur_data %.2f last_row_data %.2f %.2f, result %.2f\n", pred2D, cur_data_pos[j-1], last_row_pos[j], last_row_pos[j-1], cur_data_pos[j]);
			// getchar();
		}
		last_row_pos = cur_data_pos;
		cur_data_pos += dim0_offset;
	}
	return unpredictable_count;
}

void decompressDataSeries_float_2D_nonblocked_with_blocked_regression(float** data, size_t r1, size_t r2, unsigned char* comp_data, float* hist_data){

	size_t dim0_offset = r2;
	size_t num_elements = r1 * r2;

	*data = (float*)malloc(sizeof(float)*num_elements);

	unsigned char * comp_data_pos = comp_data;

	size_t block_size = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);
	// calculate block dims
	size_t num_x, num_y;
	SZ_COMPUTE_3D_NUMBER_OF_BLOCKS(r1, num_x, block_size);
	SZ_COMPUTE_3D_NUMBER_OF_BLOCKS(r2, num_y, block_size);

	size_t split_index_x, split_index_y;
	size_t early_blockcount_x, early_blockcount_y;
	size_t late_blockcount_x, late_blockcount_y;
	SZ_COMPUTE_BLOCKCOUNT(r1, num_x, split_index_x, early_blockcount_x, late_blockcount_x);
	SZ_COMPUTE_BLOCKCOUNT(r2, num_y, split_index_y, early_blockcount_y, late_blockcount_y);

	size_t num_blocks = num_x * num_y;

	float realPrecision = bytesToFloat(comp_data_pos);
	comp_data_pos += sizeof(float);
	unsigned int intervals = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);

	//updateQuantizationInfo(intervals);

	unsigned int tree_size = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);

	int stateNum = 2*intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);
	
	int nodeCount = bytesToInt_bigEndian(comp_data_pos);
	
	node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree,comp_data_pos+sizeof(int), nodeCount);
	comp_data_pos += sizeof(int) + tree_size;

	float mean;
	unsigned char use_mean;
	memcpy(&use_mean, comp_data_pos, sizeof(unsigned char));
	comp_data_pos += sizeof(unsigned char);
	memcpy(&mean, comp_data_pos, sizeof(float));
	comp_data_pos += sizeof(float);
	size_t reg_count = 0;

	unsigned char * indicator;
	size_t indicator_bitlength = (num_blocks - 1)/8 + 1;
	convertByteArray2IntArray_fast_1b(num_blocks, comp_data_pos, indicator_bitlength, &indicator);
	comp_data_pos += indicator_bitlength;
	for(size_t i=0; i<num_blocks; i++){
		if(!indicator[i]) reg_count ++;
	}
	//printf("reg_count: %ld\n", reg_count);

	int coeff_intvRadius[3];
	int * coeff_result_type = (int *) malloc(num_blocks*3*sizeof(int));
	int * coeff_type[3];
	float precision[3];
	float * coeff_unpred_data[3];
	if(reg_count > 0){
		for(int i=0; i<3; i++){
			precision[i] = bytesToFloat(comp_data_pos);
			comp_data_pos += sizeof(float);
			coeff_intvRadius[i] = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			unsigned int tree_size = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			int stateNum = 2*coeff_intvRadius[i]*2;
			HuffmanTree* huffmanTree = createHuffmanTree(stateNum);	
			int nodeCount = bytesToInt_bigEndian(comp_data_pos);
			node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree, comp_data_pos+sizeof(int), nodeCount);
			comp_data_pos += sizeof(int) + tree_size;

			coeff_type[i] = coeff_result_type + i * num_blocks;
			size_t typeArray_size = bytesToSize(comp_data_pos);
			decode(comp_data_pos + sizeof(size_t), reg_count, root, coeff_type[i]);
			comp_data_pos += sizeof(size_t) + typeArray_size;
			int coeff_unpred_count = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			coeff_unpred_data[i] = (float *) comp_data_pos;
			comp_data_pos += coeff_unpred_count * sizeof(float);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	float last_coefficients[3] = {0.0};
	int coeff_unpred_data_count[3] = {0};
	int coeff_index = 0;
	//updateQuantizationInfo(intervals);

	size_t total_unpred;
	memcpy(&total_unpred, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	float * unpred_data = (float *) comp_data_pos;
	comp_data_pos += total_unpred * sizeof(float);

	int * result_type = (int *) malloc(num_elements * sizeof(int));
	decode(comp_data_pos, num_elements, root, result_type);
	SZ_ReleaseHuffman(huffmanTree);
	
	int intvRadius = intervals/2;
	
	int * type;

	float * data_pos = *data;
	size_t offset_x, offset_y;
	size_t current_blockcount_x, current_blockcount_y;
	size_t cur_unpred_count;

	unsigned char * indicator_pos = indicator;
	if(use_mean){
		type = result_type;
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
				offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
				data_pos = *data + offset_x * dim0_offset + offset_y;

				current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
				current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;

				size_t current_block_elements = current_blockcount_x * current_blockcount_y;
				if(*indicator_pos){
					// decompress by SZ

					float * block_data_pos = data_pos;
					float pred;
					size_t index = 0;
					int type_;
					// d11 is current data
					size_t unpredictable_count = 0;
					float d00, d01, d10;
					for(size_t ii=0; ii<current_blockcount_x; ii++){
						for(size_t jj=0; jj<current_blockcount_y; jj++){
							type_ = type[index];
							if(type_ == intvRadius){
								*block_data_pos = mean;
							}
							else if(type_ == 0){
								*block_data_pos = unpred_data[unpredictable_count ++];
							}
							else{
								d00 = d01 = d10 = 1;
								if(i == 0 && ii == 0){
									d00 = d01 = 0;
								}
								if(j == 0 && jj == 0){
									d00 = d10 = 0;
								}
								if(d00){
									d00 = block_data_pos[- dim0_offset - 1];
								}
								if(d01){
									d01 = block_data_pos[- dim0_offset];
								}
								if(d10){
									d10 = block_data_pos[- 1];
								}
								if(type_ < intvRadius) type_ += 1;
								pred = d10 + d01 - d00;
								*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
							}
							index ++;
							block_data_pos ++;
						}
						block_data_pos += dim0_offset - current_blockcount_y;
					}
					cur_unpred_count = unpredictable_count;
				}
				else{
					// decompress by regression
					{
						//restore regression coefficients
						float pred;
						int type_;
						for(int e=0; e<3; e++){
							type_ = coeff_type[e][coeff_index];
							if (type_ != 0){
								pred = last_coefficients[e];
								last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
							}
							else{
								last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
								coeff_unpred_data_count[e] ++;
							}
						}
						coeff_index ++;
					}
					{
						float * block_data_pos = data_pos;
						float pred;
						int type_;
						size_t index = 0;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<current_blockcount_x; ii++){
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								type_ = type[index];
								if (type_ != 0){
									pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2];
									*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
								}
								else{
									*block_data_pos = unpred_data[unpredictable_count ++];
								}

								index ++;	
								block_data_pos ++;
							}
							block_data_pos += dim0_offset - current_blockcount_y;
						}
						cur_unpred_count = unpredictable_count;
					}
				}

				type += current_block_elements;
				indicator_pos ++;
				unpred_data += cur_unpred_count;
			}
		}
	}
	else{
		type = result_type;
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
				offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
				data_pos = *data + offset_x * dim0_offset + offset_y;

				current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
				current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;

				size_t current_block_elements = current_blockcount_x * current_blockcount_y;
				if(*indicator_pos){
					// decompress by SZ
					
					float * block_data_pos = data_pos;
					float pred;
					size_t index = 0;
					int type_;
					// d11 is current data
					size_t unpredictable_count = 0;
					float d00, d01, d10;
					for(size_t ii=0; ii<current_blockcount_x; ii++){
						for(size_t jj=0; jj<current_blockcount_y; jj++){
							type_ = type[index];
							if(type_ == 0){
								*block_data_pos = unpred_data[unpredictable_count ++];
							}
							else{
								d00 = d01 = d10 = 1;
								if(i == 0 && ii == 0){
									d00 = d01 = 0;
								}
								if(j == 0 && jj == 0){
									d00 = d10 = 0;
								}
								if(d00){
									d00 = block_data_pos[- dim0_offset - 1];
								}
								if(d01){
									d01 = block_data_pos[- dim0_offset];
								}
								if(d10){
									d10 = block_data_pos[- 1];
								}
								pred = d10 + d01 - d00;
								*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
							}
							index ++;
							block_data_pos ++;
						}
						block_data_pos += dim0_offset - current_blockcount_y;
					}
					cur_unpred_count = unpredictable_count;
				}
				else{
					// decompress by regression
					{
						//restore regression coefficients
						float pred;
						int type_;
						for(int e=0; e<3; e++){
							type_ = coeff_type[e][coeff_index];
							if (type_ != 0){
								pred = last_coefficients[e];
								last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
							}
							else{
								last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
								coeff_unpred_data_count[e] ++;
							}
						}
						coeff_index ++;
					}
					{
						float * block_data_pos = data_pos;
						float pred;
						int type_;
						size_t index = 0;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<current_blockcount_x; ii++){
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								type_ = type[index];
								if (type_ != 0){
									pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2];
									*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
								}
								else{
									*block_data_pos = unpred_data[unpredictable_count ++];
								}
								index ++;	
								block_data_pos ++;
							}
							block_data_pos += dim0_offset - current_blockcount_y;
						}
						cur_unpred_count = unpredictable_count;
					}
				}

				type += current_block_elements;
				indicator_pos ++;
				unpred_data += cur_unpred_count;
			}
		}
	}
	
#ifdef HAVE_TIMECMPR	
	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
		memcpy(hist_data, (*data), num_elements*sizeof(float));
#endif	
	
	free(coeff_result_type);

	free(indicator);
	free(result_type);
}


void decompressDataSeries_float_3D_nonblocked_with_blocked_regression(float** data, size_t r1, size_t r2, size_t r3, unsigned char* comp_data, float* hist_data){

	size_t dim0_offset = r2 * r3;
	size_t dim1_offset = r3;
	size_t num_elements = r1 * r2 * r3;

	*data = (float*)malloc(sizeof(float)*num_elements);

	unsigned char * comp_data_pos = comp_data;

	size_t block_size = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);
	// calculate block dims
	size_t num_x, num_y, num_z;
	SZ_COMPUTE_3D_NUMBER_OF_BLOCKS(r1, num_x, block_size);
	SZ_COMPUTE_3D_NUMBER_OF_BLOCKS(r2, num_y, block_size);
	SZ_COMPUTE_3D_NUMBER_OF_BLOCKS(r3, num_z, block_size);

	size_t split_index_x, split_index_y, split_index_z;
	size_t early_blockcount_x, early_blockcount_y, early_blockcount_z;
	size_t late_blockcount_x, late_blockcount_y, late_blockcount_z;
	SZ_COMPUTE_BLOCKCOUNT(r1, num_x, split_index_x, early_blockcount_x, late_blockcount_x);
	SZ_COMPUTE_BLOCKCOUNT(r2, num_y, split_index_y, early_blockcount_y, late_blockcount_y);
	SZ_COMPUTE_BLOCKCOUNT(r3, num_z, split_index_z, early_blockcount_z, late_blockcount_z);

	size_t num_blocks = num_x * num_y * num_z;

	float realPrecision = bytesToFloat(comp_data_pos);
	comp_data_pos += sizeof(float);
	unsigned int intervals = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);

	//updateQuantizationInfo(intervals);

	unsigned int tree_size = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);
	
	int stateNum = 2*intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);	
	
	int nodeCount = bytesToInt_bigEndian(comp_data_pos);
	node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree,comp_data_pos+sizeof(int), nodeCount);
	comp_data_pos += sizeof(int) + tree_size;

	float mean;
	unsigned char use_mean;
	memcpy(&use_mean, comp_data_pos, sizeof(unsigned char));
	comp_data_pos += sizeof(unsigned char);
	memcpy(&mean, comp_data_pos, sizeof(float));
	comp_data_pos += sizeof(float);
	size_t reg_count = 0;

	unsigned char * indicator;
	size_t indicator_bitlength = (num_blocks - 1)/8 + 1;
	convertByteArray2IntArray_fast_1b(num_blocks, comp_data_pos, indicator_bitlength, &indicator);
	comp_data_pos += indicator_bitlength;
	for(size_t i=0; i<num_blocks; i++){
		if(!indicator[i]) reg_count ++;
	}

	int coeff_intvRadius[4];
	int * coeff_result_type = (int *) malloc(num_blocks*4*sizeof(int));
	int * coeff_type[4];
	float precision[4];
	float * coeff_unpred_data[4];
	if(reg_count > 0){
		for(int i=0; i<4; i++){
			precision[i] = bytesToFloat(comp_data_pos);
			comp_data_pos += sizeof(float);
			coeff_intvRadius[i] = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			unsigned int tree_size = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			int stateNum = 2*coeff_intvRadius[i]*2;
			HuffmanTree* huffmanTree = createHuffmanTree(stateNum);	
			int nodeCount = bytesToInt_bigEndian(comp_data_pos);
			node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree, comp_data_pos+sizeof(int), nodeCount);
			comp_data_pos += sizeof(int) + tree_size;

			coeff_type[i] = coeff_result_type + i * num_blocks;
			size_t typeArray_size = bytesToSize(comp_data_pos);
			decode(comp_data_pos + sizeof(size_t), reg_count, root, coeff_type[i]);
			comp_data_pos += sizeof(size_t) + typeArray_size;
			int coeff_unpred_count = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			coeff_unpred_data[i] = (float *) comp_data_pos;
			comp_data_pos += coeff_unpred_count * sizeof(float);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	float last_coefficients[4] = {0.0};
	int coeff_unpred_data_count[4] = {0};
	int coeff_index = 0;
	//updateQuantizationInfo(intervals);

	size_t total_unpred;
	memcpy(&total_unpred, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	float * unpred_data = (float *) comp_data_pos;
	comp_data_pos += total_unpred * sizeof(float);

	int * result_type = (int *) malloc(num_elements * sizeof(int));
	decode(comp_data_pos, num_elements, root, result_type);
	SZ_ReleaseHuffman(huffmanTree);
	
	int intvRadius = intervals/2;
	
	int * type;
	float * data_pos = *data;
	size_t offset_x, offset_y, offset_z;
	size_t current_blockcount_x, current_blockcount_y, current_blockcount_z;
	size_t cur_unpred_count;
	unsigned char * indicator_pos = indicator;
	if(use_mean){
		// type = result_type;

		// for(size_t i=0; i<num_x; i++){
		// 	for(size_t j=0; j<num_y; j++){
		// 		for(size_t k=0; k<num_z; k++){
		// 			offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
		// 			offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
		// 			offset_z = (k < split_index_z) ? k * early_blockcount_z : k * late_blockcount_z + split_index_z;
		// 			data_pos = *data + offset_x * dim0_offset + offset_y * dim1_offset + offset_z;

		// 			current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
		// 			current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
		// 			current_blockcount_z = (k < split_index_z) ? early_blockcount_z : late_blockcount_z;

		// 			// type_offset = offset_x * dim0_offset +  offset_y * current_blockcount_x * dim1_offset + offset_z * current_blockcount_x * current_blockcount_y;
		// 			// type = result_type + type_offset;
		// 			size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
		// 			// index = i * num_y * num_z + j * num_z + k;

		// 			// printf("i j k: %ld %ld %ld\toffset: %ld %ld %ld\tindicator: %ld\n", i, j, k, offset_x, offset_y, offset_z, indicator[index]);
		// 			if(*indicator_pos){
		// 				// decompress by SZ
		// 				// cur_unpred_count = decompressDataSeries_float_3D_blocked_nonblock_pred(data_pos, r1, r2, r3, current_blockcount_x, current_blockcount_y, current_blockcount_z, i, j, k, realPrecision, type, unpred_data);
		// 				float * block_data_pos = data_pos;
		// 				float pred;
		// 				size_t index = 0;
		// 				int type_;
		// 				// d111 is current data
		// 				size_t unpredictable_count = 0;
		// 				float d000, d001, d010, d011, d100, d101, d110;
		// 				for(size_t ii=0; ii<current_blockcount_x; ii++){
		// 					for(size_t jj=0; jj<current_blockcount_y; jj++){
		// 						for(size_t kk=0; kk<current_blockcount_z; kk++){
		// 							type_ = type[index];
		// 							if(type_ == intvRadius){
		// 								*block_data_pos = mean;
		// 							}
		// 							else if(type_ == 0){
		// 								*block_data_pos = unpred_data[unpredictable_count ++];
		// 							}
		// 							else{
		// 								d000 = d001 = d010 = d011 = d100 = d101 = d110 = 1;
		// 								if(i == 0 && ii == 0){
		// 									d000 = d001 = d010 = d011 = 0;
		// 								}
		// 								if(j == 0 && jj == 0){
		// 									d000 = d001 = d100 = d101 = 0;
		// 								}
		// 								if(k == 0 && kk == 0){
		// 									d000 = d010 = d100 = d110 = 0;
		// 								}
		// 								if(d000){
		// 									d000 = block_data_pos[- dim0_offset - dim1_offset - 1];
		// 								}
		// 								if(d001){
		// 									d001 = block_data_pos[- dim0_offset - dim1_offset];
		// 								}
		// 								if(d010){
		// 									d010 = block_data_pos[- dim0_offset - 1];
		// 								}
		// 								if(d011){
		// 									d011 = block_data_pos[- dim0_offset];
		// 								}
		// 								if(d100){
		// 									d100 = block_data_pos[- dim1_offset - 1];
		// 								}
		// 								if(d101){
		// 									d101 = block_data_pos[- dim1_offset];
		// 								}
		// 								if(d110){
		// 									d110 = block_data_pos[- 1];
		// 								}
		// 								if(type_ < intvRadius) type_ += 1;
		// 								pred = d110 + d101 + d011 - d100 - d010 - d001 + d000;
		// 								*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
		// 							}
		// 							index ++;
		// 							block_data_pos ++;
		// 						}
		// 						block_data_pos += dim1_offset - current_blockcount_z;
		// 					}
		// 					block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
		// 				}
		// 				cur_unpred_count = unpredictable_count;
		// 			}
		// 			else{
		// 				// decompress by regression
		// 				{
		// 					//restore regression coefficients
		// 					float pred;
		// 					int type_;
		// 					for(int e=0; e<4; e++){
		// 						// if(i == 0 && j == 0 && k == 19){
		// 						// 	printf("~\n");
		// 						// }
		// 						type_ = coeff_type[e][coeff_index];
		// 						if (type_ != 0){
		// 							pred = last_coefficients[e];
		// 							last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
		// 						}
		// 						else{
		// 							last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
		// 							coeff_unpred_data_count[e] ++;
		// 						}
		// 						if(fabs(last_coefficients[e]) > 10000){
		// 							printf("%d %d %d-%d: pred %.4f type %d precision %.4g last_coefficients %.4g\n", i, j, k, e, pred, type_, precision[e], last_coefficients[e]);
		// 							exit(0);
		// 						}
		// 					}
		// 					coeff_index ++;
		// 				}
		// 				{
		// 					float * block_data_pos = data_pos;
		// 					float pred;
		// 					int type_;
		// 					size_t index = 0;
		// 					size_t unpredictable_count = 0;
		// 					for(size_t ii=0; ii<current_blockcount_x; ii++){
		// 						for(size_t jj=0; jj<current_blockcount_y; jj++){
		// 							for(size_t kk=0; kk<current_blockcount_z; kk++){
		// 								if(block_data_pos - (*data) == 19470788){
		// 									printf("dec stop\n");
		// 								}

		// 								type_ = type[index];
		// 								if (type_ != 0){
		// 									pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
		// 									*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
		// 								}
		// 								else{
		// 									*block_data_pos = unpred_data[unpredictable_count ++];
		// 								}
		// 								index ++;	
		// 								block_data_pos ++;
		// 							}
		// 							block_data_pos += dim1_offset - current_blockcount_z;
		// 						}
		// 						block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
		// 					}
		// 					cur_unpred_count = unpredictable_count;
		// 				}
		// 			}

		// 			type += current_block_elements;
		// 			indicator_pos ++;
		// 			unpred_data += cur_unpred_count;
		// 			// decomp_unpred += cur_unpred_count;
		// 			// printf("block comp done, data_offset from %ld to %ld: diff %ld\n", *data, data_pos, data_pos - *data);
		// 			// fflush(stdout);
		// 		}
		// 	}
		// }

		type = result_type;
		// i == 0
		{
			// j == 0
			{
				// k == 0
				{
					data_pos = *data;

					current_blockcount_x = early_blockcount_x;
					current_blockcount_y = early_blockcount_y;
					current_blockcount_z = early_blockcount_z;
					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						// ii == 0
						{
							// jj == 0
							{
								{
									// kk == 0
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = 0;
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							for(size_t jj=1; jj<current_blockcount_y; jj++){
								{
									// kk == 0
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- dim1_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] - block_data_pos[- dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;						
						}
						for(size_t ii=1; ii<current_blockcount_x; ii++){
							// jj == 0
							{
								{
									// kk == 0
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- dim0_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							for(size_t jj=1; jj<current_blockcount_y; jj++){
								{
									// kk == 0
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - dim1_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				} // end k == 0
				// i == 0 j == 0 k != 0
				for(size_t k=1; k<num_z; k++){
					offset_z = (k < split_index_z) ? k * early_blockcount_z : k * late_blockcount_z + split_index_z;
					data_pos = *data + offset_z;

					current_blockcount_x = early_blockcount_x;
					current_blockcount_y = early_blockcount_y;
					current_blockcount_z = (k < split_index_z) ? early_blockcount_z : late_blockcount_z;

					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						// ii == 0
						{
							// jj == 0
							{
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							for(size_t jj=1; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] - block_data_pos[- dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						for(size_t ii=1; ii<current_blockcount_x; ii++){
							// jj == 0
							{
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							for(size_t jj=1; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				}
			}// end j==0
			for(size_t j=1; j<num_y; j++){
				// k == 0
				{
					offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
					data_pos = *data + offset_y * dim1_offset;

					current_blockcount_x = early_blockcount_x;
					current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
					current_blockcount_z = early_blockcount_z;
					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						// ii == 0
						{
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								{
									// kk == 0
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- dim1_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] - block_data_pos[- dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						for(size_t ii=1; ii<current_blockcount_x; ii++){
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								{
									// kk == 0
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - dim1_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				} // end k == 0
				for(size_t k=1; k<num_z; k++){
					offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
					offset_z = (k < split_index_z) ? k * early_blockcount_z : k * late_blockcount_z + split_index_z;
					data_pos = *data + offset_y * dim1_offset + offset_z;

					current_blockcount_x = early_blockcount_x;
					current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
					current_blockcount_z = (k < split_index_z) ? early_blockcount_z : late_blockcount_z;

					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						// ii == 0
						{
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] - block_data_pos[- dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						for(size_t ii=1; ii<current_blockcount_x; ii++){
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				}
			}
		} // end i==0
		for(size_t i=1; i<num_x; i++){
			// j == 0
			{
				// k == 0
				{
					offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
					data_pos = *data + offset_x * dim0_offset;

					current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
					current_blockcount_y = early_blockcount_y;
					current_blockcount_z = early_blockcount_z;
					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<current_blockcount_x; ii++){
							// jj == 0
							{
								{
									// kk == 0
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- dim0_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							for(size_t jj=1; jj<current_blockcount_y; jj++){
								{
									// kk == 0
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - dim1_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				} // end k == 0
				for(size_t k=1; k<num_z; k++){
					offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
					offset_z = (k < split_index_z) ? k * early_blockcount_z : k * late_blockcount_z + split_index_z;
					data_pos = *data + offset_x * dim0_offset + offset_z;

					current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
					current_blockcount_y = early_blockcount_y;
					current_blockcount_z = (k < split_index_z) ? early_blockcount_z : late_blockcount_z;
					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<current_blockcount_x; ii++){
							// jj == 0
							{
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							for(size_t jj=1; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				}
			}// end j = 0
			for(size_t j=1; j<num_y; j++){
				// k == 0
				{
					offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
					offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
					data_pos = *data + offset_x * dim0_offset + offset_y * dim1_offset;

					current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
					current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
					current_blockcount_z = early_blockcount_z;
					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<current_blockcount_x; ii++){
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								{
									// kk == 0
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - dim1_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				} // end k == 0
				for(size_t k=1; k<num_z; k++){
					offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
					offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
					offset_z = (k < split_index_z) ? k * early_blockcount_z : k * late_blockcount_z + split_index_z;
					data_pos = *data + offset_x * dim0_offset + offset_y * dim1_offset + offset_z;

					current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
					current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
					current_blockcount_z = (k < split_index_z) ? early_blockcount_z : late_blockcount_z;

					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<current_blockcount_x; ii++){
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == intvRadius){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										if(type_ < intvRadius) type_ += 1;
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				}
			}
		}
	}
	else{
		type = result_type;
		// i == 0
		{
			// j == 0
			{
				// k == 0
				{
					data_pos = *data;

					current_blockcount_x = early_blockcount_x;
					current_blockcount_y = early_blockcount_y;
					current_blockcount_z = early_blockcount_z;
					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						// ii == 0
						{
							// jj == 0
							{
								{
									// kk == 0
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = 0;
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							for(size_t jj=1; jj<current_blockcount_y; jj++){
								{
									// kk == 0
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- dim1_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] - block_data_pos[- dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;						
						}
						for(size_t ii=1; ii<current_blockcount_x; ii++){
							// jj == 0
							{
								{
									// kk == 0
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- dim0_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							for(size_t jj=1; jj<current_blockcount_y; jj++){
								{
									// kk == 0
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - dim1_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				} // end k == 0
				// i == 0 j == 0 k != 0
				for(size_t k=1; k<num_z; k++){
					offset_z = (k < split_index_z) ? k * early_blockcount_z : k * late_blockcount_z + split_index_z;
					data_pos = *data + offset_z;

					current_blockcount_x = early_blockcount_x;
					current_blockcount_y = early_blockcount_y;
					current_blockcount_z = (k < split_index_z) ? early_blockcount_z : late_blockcount_z;

					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						// ii == 0
						{
							// jj == 0
							{
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							for(size_t jj=1; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] - block_data_pos[- dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						for(size_t ii=1; ii<current_blockcount_x; ii++){
							// jj == 0
							{
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							for(size_t jj=1; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				}
			}// end j==0
			for(size_t j=1; j<num_y; j++){
				// k == 0
				{
					offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
					data_pos = *data + offset_y * dim1_offset;

					current_blockcount_x = early_blockcount_x;
					current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
					current_blockcount_z = early_blockcount_z;
					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						// ii == 0
						{
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								{
									// kk == 0
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- dim1_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] - block_data_pos[- dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						for(size_t ii=1; ii<current_blockcount_x; ii++){
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								{
									// kk == 0
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - dim1_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				} // end k == 0
				for(size_t k=1; k<num_z; k++){
					offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
					offset_z = (k < split_index_z) ? k * early_blockcount_z : k * late_blockcount_z + split_index_z;
					data_pos = *data + offset_y * dim1_offset + offset_z;

					current_blockcount_x = early_blockcount_x;
					current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
					current_blockcount_z = (k < split_index_z) ? early_blockcount_z : late_blockcount_z;

					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						// ii == 0
						{
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] - block_data_pos[- dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						for(size_t ii=1; ii<current_blockcount_x; ii++){
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				}
			}
		} // end i==0
		for(size_t i=1; i<num_x; i++){
			// j == 0
			{
				// k == 0
				{
					offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
					data_pos = *data + offset_x * dim0_offset;

					current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
					current_blockcount_y = early_blockcount_y;
					current_blockcount_z = early_blockcount_z;
					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<current_blockcount_x; ii++){
							// jj == 0
							{
								{
									// kk == 0
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- dim0_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							for(size_t jj=1; jj<current_blockcount_y; jj++){
								{
									// kk == 0
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - dim1_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				} // end k == 0
				for(size_t k=1; k<num_z; k++){
					offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
					offset_z = (k < split_index_z) ? k * early_blockcount_z : k * late_blockcount_z + split_index_z;
					data_pos = *data + offset_x * dim0_offset + offset_z;

					current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
					current_blockcount_y = early_blockcount_y;
					current_blockcount_z = (k < split_index_z) ? early_blockcount_z : late_blockcount_z;

					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<current_blockcount_x; ii++){
							// jj == 0
							{
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							for(size_t jj=1; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				}
			}// end j = 0
			for(size_t j=1; j<num_y; j++){
				// k == 0
				{
					offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
					offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
					data_pos = *data + offset_x * dim0_offset + offset_y * dim1_offset;

					current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
					current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
					current_blockcount_z = early_blockcount_z;
					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<current_blockcount_x; ii++){
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								{
									// kk == 0
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim0_offset - dim1_offset];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								for(size_t kk=1; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				} // end k == 0
				for(size_t k=1; k<num_z; k++){
					offset_x = (i < split_index_x) ? i * early_blockcount_x : i * late_blockcount_x + split_index_x;
					offset_y = (j < split_index_y) ? j * early_blockcount_y : j * late_blockcount_y + split_index_y;
					offset_z = (k < split_index_z) ? k * early_blockcount_z : k * late_blockcount_z + split_index_z;
					data_pos = *data + offset_x * dim0_offset + offset_y * dim1_offset + offset_z;

					current_blockcount_x = (i < split_index_x) ? early_blockcount_x : late_blockcount_x;
					current_blockcount_y = (j < split_index_y) ? early_blockcount_y : late_blockcount_y;
					current_blockcount_z = (k < split_index_z) ? early_blockcount_z : late_blockcount_z;

					size_t current_block_elements = current_blockcount_x * current_blockcount_y * current_blockcount_z;
					if(*indicator_pos){
						// decompress by SZ
						float * block_data_pos = data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<current_blockcount_x; ii++){
							for(size_t jj=0; jj<current_blockcount_y; jj++){
								for(size_t kk=0; kk<current_blockcount_z; kk++){
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[- 1] + block_data_pos[- dim1_offset] + block_data_pos[- dim0_offset] - block_data_pos[- dim1_offset - 1] - block_data_pos[- dim0_offset - 1] - block_data_pos[- dim0_offset - dim1_offset] + block_data_pos[- dim0_offset - dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
									block_data_pos ++;
								}
								block_data_pos += dim1_offset - current_blockcount_z;
							}
							block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float * block_data_pos = data_pos;
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<current_blockcount_x; ii++){
								for(size_t jj=0; jj<current_blockcount_y; jj++){
									for(size_t kk=0; kk<current_blockcount_z; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											*block_data_pos = unpred_data[unpredictable_count ++];
										}
										index ++;	
										block_data_pos ++;
									}
									block_data_pos += dim1_offset - current_blockcount_z;
								}
								block_data_pos += dim0_offset - current_blockcount_y * dim1_offset;
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					type += current_block_elements;
					unpred_data += cur_unpred_count;
				}
			}
		}
	}
	
#ifdef HAVE_TIMECMPR	
	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
		memcpy(hist_data, (*data), num_elements*sizeof(float));
#endif	

	free(coeff_result_type);

	free(indicator);
	free(result_type);
}

void decompressDataSeries_float_3D_random_access_with_blocked_regression(float** data, size_t r1, size_t r2, size_t r3, unsigned char* comp_data){

	size_t dim0_offset = r2 * r3;
	size_t dim1_offset = r3;
	size_t num_elements = r1 * r2 * r3;

	*data = (float*)malloc(sizeof(float)*num_elements);

	unsigned char * comp_data_pos = comp_data;

	size_t block_size = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);
	// calculate block dims
	size_t num_x, num_y, num_z;
	num_x = (r1 - 1) / block_size + 1;
	num_y = (r2 - 1) / block_size + 1;
	num_z = (r3 - 1) / block_size + 1;

	size_t max_num_block_elements = block_size * block_size * block_size;
	size_t num_blocks = num_x * num_y * num_z;

	double realPrecision = bytesToDouble(comp_data_pos);
	comp_data_pos += sizeof(double);
	unsigned int intervals = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);

	//updateQuantizationInfo(intervals);

	unsigned int tree_size = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);
	
	int stateNum = 2*intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);	
	
	int nodeCount = bytesToInt_bigEndian(comp_data_pos);
	node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree,comp_data_pos+sizeof(int), nodeCount);
	comp_data_pos += sizeof(int) + tree_size;

	float mean;
	unsigned char use_mean;
	memcpy(&use_mean, comp_data_pos, sizeof(unsigned char));
	comp_data_pos += sizeof(unsigned char);
	memcpy(&mean, comp_data_pos, sizeof(float));
	comp_data_pos += sizeof(float);
	size_t reg_count = 0;

	unsigned char * indicator;
	size_t indicator_bitlength = (num_blocks - 1)/8 + 1;
	convertByteArray2IntArray_fast_1b(num_blocks, comp_data_pos, indicator_bitlength, &indicator);
	comp_data_pos += indicator_bitlength;
	for(size_t i=0; i<num_blocks; i++){
		if(!indicator[i]) reg_count ++;
	}

	int coeff_intvRadius[4];
	int * coeff_result_type = (int *) malloc(num_blocks*4*sizeof(int));
	int * coeff_type[4];
	double precision[4];
	float * coeff_unpred_data[4];
	if(reg_count > 0){
		for(int i=0; i<4; i++){
			precision[i] = bytesToDouble(comp_data_pos);
			comp_data_pos += sizeof(double);
			coeff_intvRadius[i] = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			unsigned int tree_size = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			int stateNum = 2*coeff_intvRadius[i]*2;
			HuffmanTree* huffmanTree = createHuffmanTree(stateNum);	
			int nodeCount = bytesToInt_bigEndian(comp_data_pos);
			node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree, comp_data_pos+sizeof(int), nodeCount);
			comp_data_pos += sizeof(int) + tree_size;

			coeff_type[i] = coeff_result_type + i * num_blocks;
			size_t typeArray_size = bytesToSize(comp_data_pos);
			decode(comp_data_pos + sizeof(size_t), reg_count, root, coeff_type[i]);
			comp_data_pos += sizeof(size_t) + typeArray_size;
			int coeff_unpred_count = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			coeff_unpred_data[i] = (float *) comp_data_pos;
			comp_data_pos += coeff_unpred_count * sizeof(float);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	float last_coefficients[4] = {0.0};
	int coeff_unpred_data_count[4] = {0};
	int coeff_index = 0;
	//updateQuantizationInfo(intervals);

	size_t total_unpred;
	memcpy(&total_unpred, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	float * unpred_data = (float *) comp_data_pos;
	comp_data_pos += total_unpred * sizeof(float);

	int * result_type = (int *) malloc(num_blocks*max_num_block_elements * sizeof(int));
	decode(comp_data_pos, num_blocks*max_num_block_elements, root, result_type);
	SZ_ReleaseHuffman(huffmanTree);
	
	int intvRadius = intervals/2;
	
	int * type;
	float * data_pos = *data;
	size_t cur_unpred_count;
	unsigned char * indicator_pos = indicator;
	int dec_buffer_size = block_size + 1;
	float * dec_buffer = (float *) malloc(dec_buffer_size*dec_buffer_size*dec_buffer_size*sizeof(float));
	memset(dec_buffer, 0, dec_buffer_size*dec_buffer_size*dec_buffer_size*sizeof(float));
	float * block_data_pos_x = NULL;
	float * block_data_pos_y = NULL;
	float * block_data_pos_z = NULL;
	int block_dim0_offset = dec_buffer_size*dec_buffer_size;
	int block_dim1_offset = dec_buffer_size;
	if(use_mean){
		type = result_type;
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				for(size_t k=0; k<num_z; k++){
					data_pos = dec_buffer + dec_buffer_size*dec_buffer_size + dec_buffer_size + 1;
					if(*indicator_pos){
						// decompress by SZ
						// cur_unpred_count = decompressDataSeries_float_3D_blocked_nonblock_pred(data_pos, r1, r2, r3, current_blockcount_x, current_blockcount_y, current_blockcount_z, i, j, k, realPrecision, type, unpred_data);
						float * block_data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								for(size_t kk=0; kk<block_size; kk++){
									block_data_pos = data_pos + ii*block_dim0_offset + jj*block_dim1_offset + kk;
									type_ = type[index];
									if(type_ == 1){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[-1] + block_data_pos[-block_dim1_offset]+ block_data_pos[-block_dim0_offset] - block_data_pos[-block_dim1_offset - 1]
												 - block_data_pos[-block_dim0_offset - 1] - block_data_pos[-block_dim0_offset - block_dim1_offset] + block_data_pos[-block_dim0_offset - block_dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
								}
							}
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								// if(i == 0 && j == 0 && k == 19){
								// 	printf("~\n");
								// }
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<block_size; ii++){
								for(size_t jj=0; jj<block_size; jj++){
									for(size_t kk=0; kk<block_size; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											data_pos[ii*block_dim0_offset + jj*block_dim1_offset + kk] = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											data_pos[ii*block_dim0_offset + jj*block_dim1_offset + kk] = unpred_data[unpredictable_count ++];
										}
										index ++;	
									}
								}
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					unpred_data += cur_unpred_count;
					// decomp_unpred += cur_unpred_count;
					// printf("block comp done, data_offset from %ld to %ld: diff %ld\n", *data, data_pos, data_pos - *data);
					// fflush(stdout);
					type += block_size * block_size * block_size;

					// mv data back
					block_data_pos_x = *data + i*block_size * dim0_offset + j*block_size * dim1_offset + k*block_size;
					for(int ii=0; ii<block_size; ii++){
						if(i*block_size + ii >= r1) break;
						block_data_pos_y = block_data_pos_x;
						for(int jj=0; jj<block_size; jj++){
							if(j*block_size + jj >= r2) break;
							block_data_pos_z = block_data_pos_y;
							for(int kk=0; kk<block_size; kk++){
								if(k*block_size + kk >= r3) break;
								*block_data_pos_z = data_pos[ii*dec_buffer_size*dec_buffer_size + jj*dec_buffer_size + kk];
								block_data_pos_z ++;
							}
							block_data_pos_y += dim1_offset;
						}
						block_data_pos_x += dim0_offset;
					}

				}
			}
		}

	}
	else{
		type = result_type;
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				for(size_t k=0; k<num_z; k++){
					data_pos = dec_buffer + dec_buffer_size*dec_buffer_size + dec_buffer_size + 1;
					if(*indicator_pos){
						// decompress by SZ
						// cur_unpred_count = decompressDataSeries_float_3D_blocked_nonblock_pred(data_pos, r1, r2, r3, current_blockcount_x, current_blockcount_y, current_blockcount_z, i, j, k, realPrecision, type, unpred_data);
						float * block_data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								for(size_t kk=0; kk<block_size; kk++){
									block_data_pos = data_pos + ii*block_dim0_offset + jj*block_dim1_offset + kk;
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[-1] + block_data_pos[-block_dim1_offset]+ block_data_pos[-block_dim0_offset] - block_data_pos[-block_dim1_offset - 1]
												 - block_data_pos[-block_dim0_offset - 1] - block_data_pos[-block_dim0_offset - block_dim1_offset] + block_data_pos[-block_dim0_offset - block_dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
								}
							}
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								// if(i == 0 && j == 0 && k == 19){
								// 	printf("~\n");
								// }
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<block_size; ii++){
								for(size_t jj=0; jj<block_size; jj++){
									for(size_t kk=0; kk<block_size; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											data_pos[ii*block_dim0_offset + jj*block_dim1_offset + kk] = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											data_pos[ii*block_dim0_offset + jj*block_dim1_offset + kk] = unpred_data[unpredictable_count ++];
										}
										index ++;	
									}
								}
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					unpred_data += cur_unpred_count;
					// decomp_unpred += cur_unpred_count;
					// printf("block comp done, data_offset from %ld to %ld: diff %ld\n", *data, data_pos, data_pos - *data);
					// fflush(stdout);
					type += block_size * block_size * block_size;
					// mv data back
					block_data_pos_x = *data + i*block_size * dim0_offset + j*block_size * dim1_offset + k*block_size;
					for(int ii=0; ii<block_size; ii++){
						if(i*block_size + ii >= r1) break;
						block_data_pos_y = block_data_pos_x;
						for(int jj=0; jj<block_size; jj++){
							if(j*block_size + jj >= r2) break;
							block_data_pos_z = block_data_pos_y;
							for(int kk=0; kk<block_size; kk++){
								if(k*block_size + kk >= r3) break;
								*block_data_pos_z = data_pos[ii*dec_buffer_size*dec_buffer_size + jj*dec_buffer_size + kk];
								block_data_pos_z ++;
							}
							block_data_pos_y += dim1_offset;
						}
						block_data_pos_x += dim0_offset;
					}
				}
			}
		}
	}
	free(dec_buffer);
	free(coeff_result_type);

	free(indicator);
	free(result_type);
}

void decompressDataSeries_float_3D_decompression_random_access_with_blocked_regression(float** data, size_t r1, size_t r2, size_t r3, unsigned char* comp_data){

	size_t dim0_offset = r2 * r3;
	size_t dim1_offset = r3;
	size_t num_elements = r1 * r2 * r3;

	*data = (float*)malloc(sizeof(float)*num_elements);

	unsigned char * comp_data_pos = comp_data;

	size_t block_size = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);
	// calculate block dims
	size_t num_x, num_y, num_z;
	num_x = (r1 - 1) / block_size + 1;
	num_y = (r2 - 1) / block_size + 1;
	num_z = (r3 - 1) / block_size + 1;

	size_t max_num_block_elements = block_size * block_size * block_size;
	size_t num_blocks = num_x * num_y * num_z;

	double realPrecision = bytesToDouble(comp_data_pos);
	comp_data_pos += sizeof(double);
	unsigned int intervals = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);

	//updateQuantizationInfo(intervals);

	unsigned int tree_size = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);
	
	int stateNum = 2*intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);	
	
	int nodeCount = bytesToInt_bigEndian(comp_data_pos);
	node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree,comp_data_pos+sizeof(int), nodeCount);
	comp_data_pos += sizeof(int) + tree_size;

	float mean;
	unsigned char use_mean;
	memcpy(&use_mean, comp_data_pos, sizeof(unsigned char));
	comp_data_pos += sizeof(unsigned char);
	memcpy(&mean, comp_data_pos, sizeof(float));
	comp_data_pos += sizeof(float);
	size_t reg_count = 0;

	unsigned char * indicator;
	size_t indicator_bitlength = (num_blocks - 1)/8 + 1;
	convertByteArray2IntArray_fast_1b(num_blocks, comp_data_pos, indicator_bitlength, &indicator);
	comp_data_pos += indicator_bitlength;
	for(size_t i=0; i<num_blocks; i++){
		if(!indicator[i]) reg_count ++;
	}

	int coeff_intvRadius[4];
	int * coeff_result_type = (int *) malloc(num_blocks*4*sizeof(int));
	int * coeff_type[4];
	double precision[4];
	float * coeff_unpred_data[4];
	if(reg_count > 0){
		for(int i=0; i<4; i++){
			precision[i] = bytesToDouble(comp_data_pos);
			comp_data_pos += sizeof(double);
			coeff_intvRadius[i] = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			unsigned int tree_size = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			int stateNum = 2*coeff_intvRadius[i]*2;
			HuffmanTree* huffmanTree = createHuffmanTree(stateNum);	
			int nodeCount = bytesToInt_bigEndian(comp_data_pos);
			node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree, comp_data_pos+sizeof(int), nodeCount);
			comp_data_pos += sizeof(int) + tree_size;

			coeff_type[i] = coeff_result_type + i * num_blocks;
			size_t typeArray_size = bytesToSize(comp_data_pos);
			decode(comp_data_pos + sizeof(size_t), reg_count, root, coeff_type[i]);
			comp_data_pos += sizeof(size_t) + typeArray_size;
			int coeff_unpred_count = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			coeff_unpred_data[i] = (float *) comp_data_pos;
			comp_data_pos += coeff_unpred_count * sizeof(float);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	float last_coefficients[4] = {0.0};
	int coeff_unpred_data_count[4] = {0};
	int coeff_index = 0;
	//updateQuantizationInfo(intervals);
	int intvRadius = intervals/2;

	size_t total_unpred;
	memcpy(&total_unpred, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	size_t compressed_blockwise_unpred_count_size;
	memcpy(&compressed_blockwise_unpred_count_size, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	int * blockwise_unpred_count = (int *)SZ_decompress(SZ_INT32, comp_data_pos, compressed_blockwise_unpred_count_size, 0, 0, 0, 0, num_blocks);
	comp_data_pos += compressed_blockwise_unpred_count_size;

	float * unpred_data = (float *) comp_data_pos;
	comp_data_pos += total_unpred * sizeof(float);

	size_t compressed_type_array_block_size;
	memcpy(&compressed_type_array_block_size, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	unsigned short * type_array_block_size = (unsigned short *)SZ_decompress(SZ_INT16, comp_data_pos, compressed_type_array_block_size, 0, 0, 0, 0, num_blocks);
	comp_data_pos += compressed_type_array_block_size;

	int * result_type = (int *) malloc(num_blocks*max_num_block_elements * sizeof(int));
	// decode(comp_data_pos, num_blocks*max_num_block_elements, root, result_type);
	int * block_type = result_type;
	unsigned short * type_array_block_size_pos = type_array_block_size;
	for(size_t i=0; i<num_x; i++){
		for(size_t j=0; j<num_y; j++){
			for(size_t k=0; k<num_z; k++){	
				decode(comp_data_pos, max_num_block_elements, root, block_type);
				comp_data_pos += *type_array_block_size_pos;
				type_array_block_size_pos ++;
				block_type += max_num_block_elements;
			}
		}
	}
	free(type_array_block_size);

	SZ_ReleaseHuffman(huffmanTree);	
	int * type;
	float * data_pos = *data;
	size_t cur_unpred_count;
	unsigned char * indicator_pos = indicator;
	int dec_buffer_size = block_size + 1;
	float * dec_buffer = (float *) malloc(dec_buffer_size*dec_buffer_size*dec_buffer_size*sizeof(float));
	memset(dec_buffer, 0, dec_buffer_size*dec_buffer_size*dec_buffer_size*sizeof(float));
	float * block_data_pos_x = NULL;
	float * block_data_pos_y = NULL;
	float * block_data_pos_z = NULL;
	int block_dim0_offset = dec_buffer_size*dec_buffer_size;
	int block_dim1_offset = dec_buffer_size;
	if(use_mean){
		type = result_type;
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				for(size_t k=0; k<num_z; k++){
					data_pos = dec_buffer + dec_buffer_size*dec_buffer_size + dec_buffer_size + 1;
					if(*indicator_pos){
						// decompress by SZ
						// cur_unpred_count = decompressDataSeries_float_3D_blocked_nonblock_pred(data_pos, r1, r2, r3, current_blockcount_x, current_blockcount_y, current_blockcount_z, i, j, k, realPrecision, type, unpred_data);
						float * block_data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								for(size_t kk=0; kk<block_size; kk++){
									block_data_pos = data_pos + ii*block_dim0_offset + jj*block_dim1_offset + kk;
									type_ = type[index];
									if(type_ == 1){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[-1] + block_data_pos[-block_dim1_offset]+ block_data_pos[-block_dim0_offset] - block_data_pos[-block_dim1_offset - 1]
												 - block_data_pos[-block_dim0_offset - 1] - block_data_pos[-block_dim0_offset - block_dim1_offset] + block_data_pos[-block_dim0_offset - block_dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
								}
							}
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								// if(i == 0 && j == 0 && k == 19){
								// 	printf("~\n");
								// }
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<block_size; ii++){
								for(size_t jj=0; jj<block_size; jj++){
									for(size_t kk=0; kk<block_size; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											data_pos[ii*block_dim0_offset + jj*block_dim1_offset + kk] = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											data_pos[ii*block_dim0_offset + jj*block_dim1_offset + kk] = unpred_data[unpredictable_count ++];
										}
										index ++;	
									}
								}
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					unpred_data += cur_unpred_count;
					// decomp_unpred += cur_unpred_count;
					// printf("block comp done, data_offset from %ld to %ld: diff %ld\n", *data, data_pos, data_pos - *data);
					// fflush(stdout);
					type += block_size * block_size * block_size;

					// mv data back
					block_data_pos_x = *data + i*block_size * dim0_offset + j*block_size * dim1_offset + k*block_size;
					for(int ii=0; ii<block_size; ii++){
						if(i*block_size + ii >= r1) break;
						block_data_pos_y = block_data_pos_x;
						for(int jj=0; jj<block_size; jj++){
							if(j*block_size + jj >= r2) break;
							block_data_pos_z = block_data_pos_y;
							for(int kk=0; kk<block_size; kk++){
								if(k*block_size + kk >= r3) break;
								*block_data_pos_z = data_pos[ii*dec_buffer_size*dec_buffer_size + jj*dec_buffer_size + kk];
								block_data_pos_z ++;
							}
							block_data_pos_y += dim1_offset;
						}
						block_data_pos_x += dim0_offset;
					}

				}
			}
		}

	}
	else{
		type = result_type;
		for(size_t i=0; i<num_x; i++){
			for(size_t j=0; j<num_y; j++){
				for(size_t k=0; k<num_z; k++){
					data_pos = dec_buffer + dec_buffer_size*dec_buffer_size + dec_buffer_size + 1;
					if(*indicator_pos){
						// decompress by SZ
						// cur_unpred_count = decompressDataSeries_float_3D_blocked_nonblock_pred(data_pos, r1, r2, r3, current_blockcount_x, current_blockcount_y, current_blockcount_z, i, j, k, realPrecision, type, unpred_data);
						float * block_data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								for(size_t kk=0; kk<block_size; kk++){
									block_data_pos = data_pos + ii*block_dim0_offset + jj*block_dim1_offset + kk;
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = unpred_data[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[-1] + block_data_pos[-block_dim1_offset]+ block_data_pos[-block_dim0_offset] - block_data_pos[-block_dim1_offset - 1]
												 - block_data_pos[-block_dim0_offset - 1] - block_data_pos[-block_dim0_offset - block_dim1_offset] + block_data_pos[-block_dim0_offset - block_dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
								}
							}
						}
						cur_unpred_count = unpredictable_count;
					}
					else{
						// decompress by regression
						{
							//restore regression coefficients
							float pred;
							int type_;
							for(int e=0; e<4; e++){
								// if(i == 0 && j == 0 && k == 19){
								// 	printf("~\n");
								// }
								type_ = coeff_type[e][coeff_index];
								if (type_ != 0){
									pred = last_coefficients[e];
									last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
								}
								else{
									last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
									coeff_unpred_data_count[e] ++;
								}
							}
							coeff_index ++;
						}
						{
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<block_size; ii++){
								for(size_t jj=0; jj<block_size; jj++){
									for(size_t kk=0; kk<block_size; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = last_coefficients[0] * ii + last_coefficients[1] * jj + last_coefficients[2] * kk + last_coefficients[3];
											data_pos[ii*block_dim0_offset + jj*block_dim1_offset + kk] = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											data_pos[ii*block_dim0_offset + jj*block_dim1_offset + kk] = unpred_data[unpredictable_count ++];
										}
										index ++;	
									}
								}
							}
							cur_unpred_count = unpredictable_count;
						}
					}
					indicator_pos ++;
					unpred_data += cur_unpred_count;
					// decomp_unpred += cur_unpred_count;
					// printf("block comp done, data_offset from %ld to %ld: diff %ld\n", *data, data_pos, data_pos - *data);
					// fflush(stdout);
					type += block_size * block_size * block_size;
					// mv data back
					block_data_pos_x = *data + i*block_size * dim0_offset + j*block_size * dim1_offset + k*block_size;
					for(int ii=0; ii<block_size; ii++){
						if(i*block_size + ii >= r1) break;
						block_data_pos_y = block_data_pos_x;
						for(int jj=0; jj<block_size; jj++){
							if(j*block_size + jj >= r2) break;
							block_data_pos_z = block_data_pos_y;
							for(int kk=0; kk<block_size; kk++){
								if(k*block_size + kk >= r3) break;
								*block_data_pos_z = data_pos[ii*dec_buffer_size*dec_buffer_size + jj*dec_buffer_size + kk];
								block_data_pos_z ++;
							}
							block_data_pos_y += dim1_offset;
						}
						block_data_pos_x += dim0_offset;
					}
				}
			}
		}
	}
	free(blockwise_unpred_count);
	free(dec_buffer);
	free(coeff_result_type);

	free(indicator);
	free(result_type);
}


#ifdef HAVE_RANDOMACCESS
void decompressDataSeries_float_1D_decompression_given_areas_with_blocked_regression(float** data, size_t r1, size_t s1, size_t e1, unsigned char* comp_data){

	unsigned char * comp_data_pos = comp_data;

	size_t block_size = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);
	// calculate block dims
	size_t num_x;
	num_x = (r1 - 1) / block_size + 1;

	size_t max_num_block_elements = block_size;
	size_t num_blocks = num_x;

	double realPrecision = bytesToDouble(comp_data_pos);
	comp_data_pos += sizeof(double);
	unsigned int intervals = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);

	//updateQuantizationInfo(intervals);

	unsigned int tree_size = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);
	
	int stateNum = 2*intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);	
	
	int nodeCount = bytesToInt_bigEndian(comp_data_pos);
	node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree,comp_data_pos+sizeof(int), nodeCount);
	comp_data_pos += sizeof(int) + tree_size;

	float mean;
	unsigned char use_mean;
	memcpy(&use_mean, comp_data_pos, sizeof(unsigned char));
	comp_data_pos += sizeof(unsigned char);
	memcpy(&mean, comp_data_pos, sizeof(float));
	comp_data_pos += sizeof(float);
	size_t reg_count = 0;

	unsigned char * indicator;
	size_t indicator_bitlength = (num_blocks - 1)/8 + 1;
	convertByteArray2IntArray_fast_1b(num_blocks, comp_data_pos, indicator_bitlength, &indicator);
	comp_data_pos += indicator_bitlength;
	for(size_t i=0; i<num_blocks; i++){
		if(!indicator[i]) reg_count ++;
	}

	int coeff_intvRadius[2];
	int * coeff_result_type = (int *) malloc(num_blocks*2*sizeof(int));
	int * coeff_type[2];
	double precision[2];
	float * coeff_unpred_data[2];
	if(reg_count > 0){
		for(int i=0; i<2; i++){
			precision[i] = bytesToDouble(comp_data_pos);
			comp_data_pos += sizeof(double);
			coeff_intvRadius[i] = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			unsigned int tree_size = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			int stateNum = 2*coeff_intvRadius[i]*2;
			HuffmanTree* huffmanTree = createHuffmanTree(stateNum);	
			int nodeCount = bytesToInt_bigEndian(comp_data_pos);
			node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree, comp_data_pos+sizeof(int), nodeCount);
			comp_data_pos += sizeof(int) + tree_size;

			coeff_type[i] = coeff_result_type + i * num_blocks;
			size_t typeArray_size = bytesToSize(comp_data_pos);
			decode(comp_data_pos + sizeof(size_t), reg_count, root, coeff_type[i]);
			comp_data_pos += sizeof(size_t) + typeArray_size;
			int coeff_unpred_count = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			coeff_unpred_data[i] = (float *) comp_data_pos;
			comp_data_pos += coeff_unpred_count * sizeof(float);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	float last_coefficients[2] = {0.0};
	int coeff_unpred_data_count[2] = {0};
	// decompress coeffcients
	float * reg_params = (float *) malloc(2*num_blocks*sizeof(float));
	memset(reg_params, 0, 2*num_blocks*sizeof(float));
	float * reg_params_pos = reg_params;
	size_t coeff_index = 0;
	for(size_t i=0; i<num_blocks; i++){
		if(!indicator[i]){
			float pred;
			int type_;
			for(int e=0; e<2; e++){
				type_ = coeff_type[e][coeff_index];
				if (type_ != 0){
					pred = last_coefficients[e];
					last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
				}
				else{
					last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
					coeff_unpred_data_count[e] ++;
				}
				reg_params_pos[e] = last_coefficients[e];
			}
			coeff_index ++;
		}
		reg_params_pos += 2;
	}

	//updateQuantizationInfo(intervals);
	int intvRadius = intervals/2;

	size_t total_unpred;
	memcpy(&total_unpred, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	size_t compressed_blockwise_unpred_count_size;
	memcpy(&compressed_blockwise_unpred_count_size, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	int * blockwise_unpred_count = NULL;
	SZ_decompress_args_int32(&blockwise_unpred_count, 0, 0, 0, 0, num_blocks, comp_data_pos, compressed_blockwise_unpred_count_size);
	comp_data_pos += compressed_blockwise_unpred_count_size;
	size_t * unpred_offset = (size_t *) malloc(num_blocks * sizeof(size_t));
	size_t cur_offset = 0;
	for(size_t i=0; i<num_blocks; i++){
		unpred_offset[i] = cur_offset;
		cur_offset += blockwise_unpred_count[i];
	}

	float * unpred_data = (float *) comp_data_pos;
	comp_data_pos += total_unpred * sizeof(float);

	size_t compressed_type_array_block_size;
	memcpy(&compressed_type_array_block_size, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	unsigned short * type_array_block_size = NULL;
	SZ_decompress_args_uint16(&type_array_block_size, 0, 0, 0, 0, num_blocks, comp_data_pos, compressed_type_array_block_size);

	comp_data_pos += compressed_type_array_block_size;

	// compute given area
	size_t sx = s1 / block_size;
	size_t ex = (e1 - 1) / block_size + 1;

	unsigned short * type_array_block_size_pos = type_array_block_size;
	size_t * type_array_offset = (size_t *) malloc(num_blocks * sizeof(size_t));
	size_t * type_array_offset_pos = type_array_offset;
	size_t cur_type_array_offset = 0;
	for(size_t i=0; i<num_x; i++){
		*(type_array_offset_pos++) = cur_type_array_offset;
		cur_type_array_offset += *(type_array_block_size_pos++);
	}
	free(type_array_block_size);
	int * result_type = (int *) malloc((ex - sx)*block_size*sizeof(int));
	int * block_type = result_type;
	for(size_t i=sx; i<ex; i++){
		size_t index = i;
		decode(comp_data_pos + type_array_offset[index], max_num_block_elements, root, block_type);
		block_type += max_num_block_elements;
	}
	SZ_ReleaseHuffman(huffmanTree);
	free(type_array_offset);

	int * type = NULL;
	float * data_pos = *data;
	int dec_buffer_size = block_size + 1;
	float * dec_buffer = (float *) malloc(dec_buffer_size*sizeof(float));
	memset(dec_buffer, 0, dec_buffer_size*sizeof(float));
	float * block_data_pos_x = NULL;
	// printf("decompression start, %d %d %d, %d %d %d, total unpred %ld\n", sx, sy, sz, ex, ey, ez, total_unpred);
	// fflush(stdout);
	float * dec_block_data = (float *) malloc((ex - sx)*block_size*sizeof(float));
	memset(dec_block_data, 0, (ex - sx)*block_size*sizeof(float));
	if(use_mean){
		for(size_t i=sx; i<ex; i++){
			data_pos = dec_buffer + 1;
			type = result_type + (i-sx) * block_size;
			coeff_index = i;
			float * block_unpred = unpred_data + unpred_offset[coeff_index];
			if(indicator[coeff_index]){
				// decompress by SZ
				float * block_data_pos;
				float pred;
				size_t index = 0;
				int type_;
				size_t unpredictable_count = 0;
				for(size_t ii=0; ii<block_size; ii++){
					block_data_pos = data_pos + ii;
					type_ = type[index];
					if(type_ == 1){
						*block_data_pos = mean;
					}
					else if(type_ == 0){
						*block_data_pos = block_unpred[unpredictable_count ++];
					}
					else{
						pred = block_data_pos[-1];
						*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
					}
					index ++;
				}
			}
			else{
				// decompress by regression
				reg_params_pos = reg_params + 2*coeff_index;
				{
					float pred;
					int type_;
					size_t index = 0;
					size_t unpredictable_count = 0;
					for(size_t ii=0; ii<block_size; ii++){
						type_ = type[index];
						if (type_ != 0){
							pred = reg_params_pos[0] * ii + reg_params_pos[1];
							data_pos[ii] = pred + 2 * (type_ - intvRadius) * realPrecision;
						}
						else{
							data_pos[ii] = block_unpred[unpredictable_count ++];
						}
						index ++;	
					}
				}
			}

			// mv data back
			block_data_pos_x = dec_block_data + (i-sx)*block_size;
			for(int ii=0; ii<block_size; ii++){
				if(i*block_size + ii >= r1) break;
				*block_data_pos_x = data_pos[ii];
				block_data_pos_x ++;
			}
		}

	}
	else{
		for(size_t i=sx; i<ex; i++){
			data_pos = dec_buffer + 1;
			type = result_type + (i-sx) * block_size;
			coeff_index = i;
			float * block_unpred = unpred_data + unpred_offset[coeff_index];
			if(indicator[coeff_index]){
				// decompress by SZ
				float * block_data_pos;
				float pred;
				size_t index = 0;
				int type_;
				size_t unpredictable_count = 0;
				for(size_t ii=0; ii<block_size; ii++){
					block_data_pos = data_pos + ii;
					type_ = type[index];
					if(type_ == 0){
						*block_data_pos = block_unpred[unpredictable_count ++];
					}
					else{
						pred = block_data_pos[-1];
						*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
					}
					index ++;
				}
			}
			else{
				// decompress by regression
				reg_params_pos = reg_params + 2*coeff_index;
				{
					float pred;
					int type_;
					size_t index = 0;
					size_t unpredictable_count = 0;
					for(size_t ii=0; ii<block_size; ii++){
						type_ = type[index];
						if (type_ != 0){
							pred = reg_params_pos[0] * ii + reg_params_pos[1];
							data_pos[ii] = pred + 2 * (type_ - intvRadius) * realPrecision;
						}
						else{
							data_pos[ii] = block_unpred[unpredictable_count ++];
						}
						index ++;	
					}
				}
			}

			// mv data back
			block_data_pos_x = dec_block_data + (i-sx)*block_size;
			for(int ii=0; ii<block_size; ii++){
				if(i*block_size + ii >= r1) break;
				*block_data_pos_x = data_pos[ii];
				block_data_pos_x ++;
			}
		}
	}
	free(unpred_offset);
	free(reg_params);
	free(blockwise_unpred_count);
	free(dec_buffer);
	free(coeff_result_type);

	free(indicator);
	free(result_type);

	// extract data
	int resi_x = s1 % block_size;
	*data = (float*) malloc(sizeof(float)*(e1 - s1));
	float * final_data_pos = *data;
	float * block_data_pos = dec_block_data + resi_x;
	for(int i=0; i<(e1 - s1); i++){
		*(final_data_pos++) = *(block_data_pos++);
	}
	free(dec_block_data);
}

void decompressDataSeries_float_2D_decompression_given_areas_with_blocked_regression(float** data, size_t r1, size_t r2, size_t s1, size_t s2, size_t e1, size_t e2, unsigned char* comp_data){

	unsigned char * comp_data_pos = comp_data;

	size_t block_size = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);
	// calculate block dims
	size_t num_x, num_y;
	num_x = (r1 - 1) / block_size + 1;
	num_y = (r2 - 1) / block_size + 1;

	size_t max_num_block_elements = block_size * block_size;
	size_t num_blocks = num_x * num_y;

	double realPrecision = bytesToDouble(comp_data_pos);
	comp_data_pos += sizeof(double);
	unsigned int intervals = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);

	//updateQuantizationInfo(intervals);

	unsigned int tree_size = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);
	
	int stateNum = 2*intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);	
	
	int nodeCount = bytesToInt_bigEndian(comp_data_pos);
	node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree,comp_data_pos+sizeof(int), nodeCount);
	comp_data_pos += sizeof(int) + tree_size;

	float mean;
	unsigned char use_mean;
	memcpy(&use_mean, comp_data_pos, sizeof(unsigned char));
	comp_data_pos += sizeof(unsigned char);
	memcpy(&mean, comp_data_pos, sizeof(float));
	comp_data_pos += sizeof(float);
	size_t reg_count = 0;

	unsigned char * indicator;
	size_t indicator_bitlength = (num_blocks - 1)/8 + 1;
	convertByteArray2IntArray_fast_1b(num_blocks, comp_data_pos, indicator_bitlength, &indicator);
	comp_data_pos += indicator_bitlength;
	for(size_t i=0; i<num_blocks; i++){
		if(!indicator[i]) reg_count ++;
	}

	int coeff_intvRadius[3];
	int * coeff_result_type = (int *) malloc(num_blocks*3*sizeof(int));
	int * coeff_type[3];
	double precision[3];
	float * coeff_unpred_data[3];
	if(reg_count > 0){
		for(int i=0; i<3; i++){
			precision[i] = bytesToDouble(comp_data_pos);
			comp_data_pos += sizeof(double);
			coeff_intvRadius[i] = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			unsigned int tree_size = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			int stateNum = 2*coeff_intvRadius[i]*2;
			HuffmanTree* huffmanTree = createHuffmanTree(stateNum);	
			int nodeCount = bytesToInt_bigEndian(comp_data_pos);
			node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree, comp_data_pos+sizeof(int), nodeCount);
			comp_data_pos += sizeof(int) + tree_size;

			coeff_type[i] = coeff_result_type + i * num_blocks;
			size_t typeArray_size = bytesToSize(comp_data_pos);
			decode(comp_data_pos + sizeof(size_t), reg_count, root, coeff_type[i]);
			comp_data_pos += sizeof(size_t) + typeArray_size;
			int coeff_unpred_count = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			coeff_unpred_data[i] = (float *) comp_data_pos;
			comp_data_pos += coeff_unpred_count * sizeof(float);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	float last_coefficients[3] = {0.0};
	int coeff_unpred_data_count[3] = {0};
	// decompress coeffcients
	float * reg_params = (float *) malloc(3*num_blocks*sizeof(float));
	memset(reg_params, 0, 3*num_blocks*sizeof(float));
	float * reg_params_pos = reg_params;
	size_t coeff_index = 0;
	for(size_t i=0; i<num_blocks; i++){
		if(!indicator[i]){
			float pred;
			int type_;
			for(int e=0; e<3; e++){
				type_ = coeff_type[e][coeff_index];
				if (type_ != 0){
					pred = last_coefficients[e];
					last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
				}
				else{
					last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
					coeff_unpred_data_count[e] ++;
				}
				reg_params_pos[e] = last_coefficients[e];
			}
			coeff_index ++;
		}
		reg_params_pos += 3;
	}

	//updateQuantizationInfo(intervals);
	int intvRadius = intervals/2;

	size_t total_unpred;
	memcpy(&total_unpred, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	size_t compressed_blockwise_unpred_count_size;
	memcpy(&compressed_blockwise_unpred_count_size, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	int * blockwise_unpred_count = NULL;
	SZ_decompress_args_int32(&blockwise_unpred_count, 0, 0, 0, 0, num_blocks, comp_data_pos, compressed_blockwise_unpred_count_size);
	comp_data_pos += compressed_blockwise_unpred_count_size;
	size_t * unpred_offset = (size_t *) malloc(num_blocks * sizeof(size_t));
	size_t cur_offset = 0;
	for(size_t i=0; i<num_blocks; i++){
		unpred_offset[i] = cur_offset;
		cur_offset += blockwise_unpred_count[i];
	}

	float * unpred_data = (float *) comp_data_pos;
	comp_data_pos += total_unpred * sizeof(float);

	size_t compressed_type_array_block_size;
	memcpy(&compressed_type_array_block_size, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	unsigned short * type_array_block_size = NULL;
	SZ_decompress_args_uint16(&type_array_block_size, 0, 0, 0, 0, num_blocks, comp_data_pos, compressed_type_array_block_size);

	comp_data_pos += compressed_type_array_block_size;

	// compute given area
	size_t sx = s1 / block_size;
	size_t sy = s2 / block_size;
	size_t ex = (e1 - 1) / block_size + 1;
	size_t ey = (e2 - 1) / block_size + 1;

	size_t dec_block_dim0_offset = (ey - sy)*block_size;
	unsigned short * type_array_block_size_pos = type_array_block_size;
	size_t * type_array_offset = (size_t *) malloc(num_blocks * sizeof(size_t));
	size_t * type_array_offset_pos = type_array_offset;
	size_t cur_type_array_offset = 0;
	for(size_t i=0; i<num_x; i++){
		for(size_t j=0; j<num_y; j++){
			*(type_array_offset_pos++) = cur_type_array_offset;
			cur_type_array_offset += *(type_array_block_size_pos++);
		}
	}
	free(type_array_block_size);
	int * result_type = (int *) malloc((ex - sx)*block_size * dec_block_dim0_offset* sizeof(int));
	int * block_type = result_type;
	for(size_t i=sx; i<ex; i++){
		for(size_t j=sy; j<ey; j++){
			size_t index = i*num_y + j;
			decode(comp_data_pos + type_array_offset[index], max_num_block_elements, root, block_type);
			block_type += max_num_block_elements;
		}
	}
	SZ_ReleaseHuffman(huffmanTree);
	free(type_array_offset);

	int * type = NULL;
	float * data_pos = *data;
	int dec_buffer_size = block_size + 1;
	float * dec_buffer = (float *) malloc(dec_buffer_size*dec_buffer_size*sizeof(float));
	memset(dec_buffer, 0, dec_buffer_size*dec_buffer_size*sizeof(float));
	float * block_data_pos_x = NULL;
	float * block_data_pos_y = NULL;
	int block_dim0_offset = dec_buffer_size;
	// printf("decompression start, %d %d %d, %d %d %d, total unpred %ld\n", sx, sy, sz, ex, ey, ez, total_unpred);
	// fflush(stdout);
	float * dec_block_data = (float *) malloc((ex - sx)*block_size * dec_block_dim0_offset*sizeof(float));
	memset(dec_block_data, 0, (ex - sx)*block_size * dec_block_dim0_offset*sizeof(float));
	if(use_mean){
		for(size_t i=sx; i<ex; i++){
			for(size_t j=sy; j<ey; j++){
				data_pos = dec_buffer + dec_buffer_size + 1;
				type = result_type + (i-sx) * block_size * block_size * (ey - sy) +  (j-sy) * block_size * block_size;
				coeff_index = i*num_y + j;
				float * block_unpred = unpred_data + unpred_offset[coeff_index];
				if(indicator[coeff_index]){
					// decompress by SZ
					float * block_data_pos;
					float pred;
					size_t index = 0;
					int type_;
					size_t unpredictable_count = 0;
					for(size_t ii=0; ii<block_size; ii++){
						for(size_t jj=0; jj<block_size; jj++){
							block_data_pos = data_pos + ii*block_dim0_offset + jj;
							type_ = type[index];
							if(type_ == 1){
								*block_data_pos = mean;
							}
							else if(type_ == 0){
								*block_data_pos = block_unpred[unpredictable_count ++];
							}
							else{
								pred = block_data_pos[-1] + block_data_pos[-block_dim0_offset] - block_data_pos[-block_dim0_offset - 1];
								*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
							}
							index ++;
						}
					}
				}
				else{
					// decompress by regression
					reg_params_pos = reg_params + 3*coeff_index;
					{
						float pred;
						int type_;
						size_t index = 0;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								type_ = type[index];
								if (type_ != 0){
									pred = reg_params_pos[0] * ii + reg_params_pos[1] * jj + reg_params_pos[2];
									data_pos[ii*block_dim0_offset + jj] = pred + 2 * (type_ - intvRadius) * realPrecision;
								}
								else{
									data_pos[ii*block_dim0_offset + jj] = block_unpred[unpredictable_count ++];
								}
								index ++;	
							}
						}
					}
				}

				// mv data back
				block_data_pos_x = dec_block_data + (i-sx)*block_size * dec_block_dim0_offset + (j-sy)*block_size;
				for(int ii=0; ii<block_size; ii++){
					if(i*block_size + ii >= r1) break;
					block_data_pos_y = block_data_pos_x;
					for(int jj=0; jj<block_size; jj++){
						if(j*block_size + jj >= r2) break;
						*block_data_pos_y = data_pos[ii*dec_buffer_size + jj];
						block_data_pos_y ++;
					}
					block_data_pos_x += dec_block_dim0_offset;
				}

			}
		}

	}
	else{
		for(size_t i=sx; i<ex; i++){
			for(size_t j=sy; j<ey; j++){
				data_pos = dec_buffer + dec_buffer_size + 1;
				type = result_type + (i-sx) * block_size * block_size * (ey - sy) +  (j-sy) * block_size * block_size;
				coeff_index = i*num_y + j;
				float * block_unpred = unpred_data + unpred_offset[coeff_index];
				if(indicator[coeff_index]){
					// decompress by SZ
					float * block_data_pos;
					float pred;
					size_t index = 0;
					int type_;
					size_t unpredictable_count = 0;
					for(size_t ii=0; ii<block_size; ii++){
						for(size_t jj=0; jj<block_size; jj++){
							block_data_pos = data_pos + ii*block_dim0_offset + jj;
							type_ = type[index];
							if(type_ == 0){
								*block_data_pos = block_unpred[unpredictable_count ++];
							}
							else{
								pred = block_data_pos[-1] + block_data_pos[-block_dim0_offset] - block_data_pos[-block_dim0_offset - 1];
								*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
							}
							index ++;
						}
					}
				}
				else{
					// decompress by regression
					reg_params_pos = reg_params + 3*coeff_index;
					{
						float pred;
						int type_;
						size_t index = 0;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								type_ = type[index];
								if (type_ != 0){
									pred = reg_params_pos[0] * ii + reg_params_pos[1] * jj + reg_params_pos[2];
									data_pos[ii*block_dim0_offset + jj] = pred + 2 * (type_ - intvRadius) * realPrecision;
								}
								else{
									data_pos[ii*block_dim0_offset + jj] = block_unpred[unpredictable_count ++];
								}
								index ++;	
							}
						}
					}
				}

				// mv data back
				block_data_pos_x = dec_block_data + (i-sx)*block_size * dec_block_dim0_offset + (j-sy)*block_size;
				for(int ii=0; ii<block_size; ii++){
					if(i*block_size + ii >= r1) break;
					block_data_pos_y = block_data_pos_x;
					for(int jj=0; jj<block_size; jj++){
						if(j*block_size + jj >= r2) break;
						*block_data_pos_y = data_pos[ii*dec_buffer_size + jj];
						block_data_pos_y ++;
					}
					block_data_pos_x += dec_block_dim0_offset;
				}
			}
		}
	}
	free(unpred_offset);
	free(reg_params);
	free(blockwise_unpred_count);
	free(dec_buffer);
	free(coeff_result_type);

	free(indicator);
	free(result_type);

	// extract data
	int resi_x = s1 % block_size;
	int resi_y = s2 % block_size;
	*data = (float*) malloc(sizeof(float)*(e1 - s1) * (e2 - s2));
	float * final_data_pos = *data;
	for(int i=0; i<(e1 - s1); i++){
		float * block_data_pos = dec_block_data + (i+resi_x)*dec_block_dim0_offset + resi_y;
		for(int j=0; j<(e2 - s2); j++){
			*(final_data_pos++) = *(block_data_pos++);
		}
	}
	free(dec_block_data);
}

void decompressDataSeries_float_3D_decompression_given_areas_with_blocked_regression(float** data, size_t r1, size_t r2, size_t r3, size_t s1, size_t s2, size_t s3, size_t e1, size_t e2, size_t e3, unsigned char* comp_data){

	// size_t dim0_offset = r2 * r3;
	// size_t dim1_offset = r3;

	unsigned char * comp_data_pos = comp_data;

	size_t block_size = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);
	// calculate block dims
	size_t num_x, num_y, num_z;
	num_x = (r1 - 1) / block_size + 1;
	num_y = (r2 - 1) / block_size + 1;
	num_z = (r3 - 1) / block_size + 1;

	size_t max_num_block_elements = block_size * block_size * block_size;
	size_t num_blocks = num_x * num_y * num_z;

	double realPrecision = bytesToDouble(comp_data_pos);
	comp_data_pos += sizeof(double);
	unsigned int intervals = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);

	//updateQuantizationInfo(intervals);

	unsigned int tree_size = bytesToInt_bigEndian(comp_data_pos);
	comp_data_pos += sizeof(int);
	
	int stateNum = 2*intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);	
	
	int nodeCount = bytesToInt_bigEndian(comp_data_pos);
	node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree,comp_data_pos+sizeof(int), nodeCount);
	comp_data_pos += sizeof(int) + tree_size;

	float mean;
	unsigned char use_mean;
	memcpy(&use_mean, comp_data_pos, sizeof(unsigned char));
	comp_data_pos += sizeof(unsigned char);
	memcpy(&mean, comp_data_pos, sizeof(float));
	comp_data_pos += sizeof(float);
	size_t reg_count = 0;

	unsigned char * indicator;
	size_t indicator_bitlength = (num_blocks - 1)/8 + 1;
	convertByteArray2IntArray_fast_1b(num_blocks, comp_data_pos, indicator_bitlength, &indicator);
	comp_data_pos += indicator_bitlength;
	for(size_t i=0; i<num_blocks; i++){
		if(!indicator[i]) reg_count ++;
	}

	int coeff_intvRadius[4];
	int * coeff_result_type = (int *) malloc(num_blocks*4*sizeof(int));
	int * coeff_type[4];
	double precision[4];
	float * coeff_unpred_data[4];
	if(reg_count > 0){
		for(int i=0; i<4; i++){
			precision[i] = bytesToDouble(comp_data_pos);
			comp_data_pos += sizeof(double);
			coeff_intvRadius[i] = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			unsigned int tree_size = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			int stateNum = 2*coeff_intvRadius[i]*2;
			HuffmanTree* huffmanTree = createHuffmanTree(stateNum);	
			int nodeCount = bytesToInt_bigEndian(comp_data_pos);
			node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree, comp_data_pos+sizeof(int), nodeCount);
			comp_data_pos += sizeof(int) + tree_size;

			coeff_type[i] = coeff_result_type + i * num_blocks;
			size_t typeArray_size = bytesToSize(comp_data_pos);
			decode(comp_data_pos + sizeof(size_t), reg_count, root, coeff_type[i]);
			comp_data_pos += sizeof(size_t) + typeArray_size;
			int coeff_unpred_count = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			coeff_unpred_data[i] = (float *) comp_data_pos;
			comp_data_pos += coeff_unpred_count * sizeof(float);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	float last_coefficients[4] = {0.0};
	int coeff_unpred_data_count[4] = {0};
	// decompress coeffcients
	float * reg_params = (float *) malloc(4*num_blocks*sizeof(float));
	memset(reg_params, 0, 4*num_blocks*sizeof(float));
	float * reg_params_pos = reg_params;
	size_t coeff_index = 0;
	for(size_t i=0; i<num_blocks; i++){
		if(!indicator[i]){
			float pred;
			int type_;
			for(int e=0; e<4; e++){
				type_ = coeff_type[e][coeff_index];
				if (type_ != 0){
					pred = last_coefficients[e];
					last_coefficients[e] = pred + 2 * (type_ - coeff_intvRadius[e]) * precision[e];
				}
				else{
					last_coefficients[e] = coeff_unpred_data[e][coeff_unpred_data_count[e]];
					coeff_unpred_data_count[e] ++;
				}
				reg_params_pos[e] = last_coefficients[e];
			}
			coeff_index ++;
		}
		reg_params_pos += 4;
	}

	//updateQuantizationInfo(intervals);
	int intvRadius = intervals/2;

	size_t total_unpred;
	memcpy(&total_unpred, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	size_t compressed_blockwise_unpred_count_size;
	memcpy(&compressed_blockwise_unpred_count_size, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	int * blockwise_unpred_count = NULL;
	SZ_decompress_args_int32(&blockwise_unpred_count, 0, 0, 0, 0, num_blocks, comp_data_pos, compressed_blockwise_unpred_count_size);
	comp_data_pos += compressed_blockwise_unpred_count_size;
	size_t * unpred_offset = (size_t *) malloc(num_blocks * sizeof(size_t));
	size_t cur_offset = 0;
	for(size_t i=0; i<num_blocks; i++){
		unpred_offset[i] = cur_offset;
		cur_offset += blockwise_unpred_count[i];
	}

	float * unpred_data = (float *) comp_data_pos;
	comp_data_pos += total_unpred * sizeof(float);

	size_t compressed_type_array_block_size;
	memcpy(&compressed_type_array_block_size, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	unsigned short * type_array_block_size = NULL;
	SZ_decompress_args_uint16(&type_array_block_size, 0, 0, 0, 0, num_blocks, comp_data_pos, compressed_type_array_block_size);

	comp_data_pos += compressed_type_array_block_size;

	// compute given area
	size_t sx = s1 / block_size;
	size_t sy = s2 / block_size;
	size_t sz = s3 / block_size;
	size_t ex = (e1 - 1) / block_size + 1;
	size_t ey = (e2 - 1) / block_size + 1;
	size_t ez = (e3 - 1) / block_size + 1;

	size_t dec_block_dim1_offset = (ez - sz)*block_size;
	size_t dec_block_dim0_offset = dec_block_dim1_offset * (ey - sy)*block_size;
	unsigned short * type_array_block_size_pos = type_array_block_size;
	size_t * type_array_offset = (size_t *) malloc(num_blocks * sizeof(size_t));
	size_t * type_array_offset_pos = type_array_offset;
	size_t cur_type_array_offset = 0;
	for(size_t i=0; i<num_x; i++){
		for(size_t j=0; j<num_y; j++){
			for(size_t k=0; k<num_z; k++){	
				*(type_array_offset_pos++) = cur_type_array_offset;
				cur_type_array_offset += *(type_array_block_size_pos++);
			}
		}
	}
	free(type_array_block_size);
	int * result_type = (int *) malloc((ex - sx)*block_size * dec_block_dim0_offset* sizeof(int));
	int * block_type = result_type;
	for(size_t i=sx; i<ex; i++){
		for(size_t j=sy; j<ey; j++){
			for(size_t k=sz; k<ez; k++){
				size_t index = i*num_y*num_z + j*num_z + k;
				decode(comp_data_pos + type_array_offset[index], max_num_block_elements, root, block_type);
				block_type += max_num_block_elements;
			}
		}
	}
	SZ_ReleaseHuffman(huffmanTree);
	free(type_array_offset);

	int * type = NULL;
	float * data_pos = *data;
	int dec_buffer_size = block_size + 1;
	float * dec_buffer = (float *) malloc(dec_buffer_size*dec_buffer_size*dec_buffer_size*sizeof(float));
	memset(dec_buffer, 0, dec_buffer_size*dec_buffer_size*dec_buffer_size*sizeof(float));
	float * block_data_pos_x = NULL;
	float * block_data_pos_y = NULL;
	float * block_data_pos_z = NULL;
	int block_dim0_offset = dec_buffer_size*dec_buffer_size;
	int block_dim1_offset = dec_buffer_size;

	// printf("decompression start, %d %d %d, %d %d %d, total unpred %ld\n", sx, sy, sz, ex, ey, ez, total_unpred);
	// fflush(stdout);
	float * dec_block_data = (float *) malloc((ex - sx)*block_size * dec_block_dim0_offset*sizeof(float));
	memset(dec_block_data, 0, (ex - sx)*block_size * dec_block_dim0_offset*sizeof(float));
	if(use_mean){
		for(size_t i=sx; i<ex; i++){
			for(size_t j=sy; j<ey; j++){
				for(size_t k=sz; k<ez; k++){
					data_pos = dec_buffer + dec_buffer_size*dec_buffer_size + dec_buffer_size + 1;
					type = result_type + (i-sx) * block_size * block_size * (ey - sy) * block_size * (ez - sz) +  (j-sy) * block_size * block_size * block_size * (ez - sz) + (k-sz) * block_size * block_size * block_size;
					coeff_index = i*num_y*num_z + j*num_z + k;
					float * block_unpred = unpred_data + unpred_offset[coeff_index];
					if(indicator[coeff_index]){
						// decompress by SZ
						float * block_data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								for(size_t kk=0; kk<block_size; kk++){
									block_data_pos = data_pos + ii*block_dim0_offset + jj*block_dim1_offset + kk;
									type_ = type[index];
									if(type_ == 1){
										*block_data_pos = mean;
									}
									else if(type_ == 0){
										*block_data_pos = block_unpred[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[-1] + block_data_pos[-block_dim1_offset]+ block_data_pos[-block_dim0_offset] - block_data_pos[-block_dim1_offset - 1]
												 - block_data_pos[-block_dim0_offset - 1] - block_data_pos[-block_dim0_offset - block_dim1_offset] + block_data_pos[-block_dim0_offset - block_dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
								}
							}
						}
					}
					else{
						// decompress by regression
						reg_params_pos = reg_params + 4*coeff_index;
						{
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<block_size; ii++){
								for(size_t jj=0; jj<block_size; jj++){
									for(size_t kk=0; kk<block_size; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = reg_params_pos[0] * ii + reg_params_pos[1] * jj + reg_params_pos[2] * kk + reg_params_pos[3];
											data_pos[ii*block_dim0_offset + jj*block_dim1_offset + kk] = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											data_pos[ii*block_dim0_offset + jj*block_dim1_offset + kk] = block_unpred[unpredictable_count ++];
										}
										index ++;	
									}
								}
							}
						}
					}

					// mv data back
					block_data_pos_x = dec_block_data + (i-sx)*block_size * dec_block_dim0_offset + (j-sy)*block_size * dec_block_dim1_offset + (k-sz)*block_size;
					for(int ii=0; ii<block_size; ii++){
						if(i*block_size + ii >= r1) break;
						block_data_pos_y = block_data_pos_x;
						for(int jj=0; jj<block_size; jj++){
							if(j*block_size + jj >= r2) break;
							block_data_pos_z = block_data_pos_y;
							for(int kk=0; kk<block_size; kk++){
								if(k*block_size + kk >= r3) break;
								*block_data_pos_z = data_pos[ii*dec_buffer_size*dec_buffer_size + jj*dec_buffer_size + kk];
								block_data_pos_z ++;
							}
							block_data_pos_y += dec_block_dim1_offset;
						}
						block_data_pos_x += dec_block_dim0_offset;
					}

				}
			}
		}

	}
	else{
		for(size_t i=sx; i<ex; i++){
			for(size_t j=sy; j<ey; j++){
				for(size_t k=sz; k<ez; k++){
					data_pos = dec_buffer + dec_buffer_size*dec_buffer_size + dec_buffer_size + 1;
					type = result_type + (i-sx) * block_size * block_size * (ey - sy) * block_size * (ez - sz) +  (j-sy) * block_size * block_size * block_size * (ez - sz) + (k-sz) * block_size * block_size * block_size;
					coeff_index = i*num_y*num_z + j*num_z + k;
					float * block_unpred = unpred_data + unpred_offset[coeff_index];
					if(indicator[coeff_index]){
						// decompress by SZ
						// cur_unpred_count = decompressDataSeries_float_3D_blocked_nonblock_pred(data_pos, r1, r2, r3, current_blockcount_x, current_blockcount_y, current_blockcount_z, i, j, k, realPrecision, type, unpred_data);
						float * block_data_pos;
						float pred;
						size_t index = 0;
						int type_;
						size_t unpredictable_count = 0;
						for(size_t ii=0; ii<block_size; ii++){
							for(size_t jj=0; jj<block_size; jj++){
								for(size_t kk=0; kk<block_size; kk++){
									block_data_pos = data_pos + ii*block_dim0_offset + jj*block_dim1_offset + kk;
									type_ = type[index];
									if(type_ == 0){
										*block_data_pos = block_unpred[unpredictable_count ++];
									}
									else{
										pred = block_data_pos[-1] + block_data_pos[-block_dim1_offset]+ block_data_pos[-block_dim0_offset] - block_data_pos[-block_dim1_offset - 1]
												 - block_data_pos[-block_dim0_offset - 1] - block_data_pos[-block_dim0_offset - block_dim1_offset] + block_data_pos[-block_dim0_offset - block_dim1_offset - 1];
										*block_data_pos = pred + 2 * (type_ - intvRadius) * realPrecision;
									}
									index ++;
								}
							}
						}
					}
					else{
						// decompress by regression
						reg_params_pos = reg_params + 4*coeff_index;
						{
							float pred;
							int type_;
							size_t index = 0;
							size_t unpredictable_count = 0;
							for(size_t ii=0; ii<block_size; ii++){
								for(size_t jj=0; jj<block_size; jj++){
									for(size_t kk=0; kk<block_size; kk++){
										type_ = type[index];
										if (type_ != 0){
											pred = reg_params_pos[0] * ii + reg_params_pos[1] * jj + reg_params_pos[2] * kk + reg_params_pos[3];
											data_pos[ii*block_dim0_offset + jj*block_dim1_offset + kk] = pred + 2 * (type_ - intvRadius) * realPrecision;
										}
										else{
											data_pos[ii*block_dim0_offset + jj*block_dim1_offset + kk] = block_unpred[unpredictable_count ++];
										}
										index ++;	
									}
								}
							}
						}
					}
					// mv data back
					block_data_pos_x = dec_block_data + (i-sx)*block_size * dec_block_dim0_offset + (j-sy)*block_size * dec_block_dim1_offset + (k-sz)*block_size;
					for(int ii=0; ii<block_size; ii++){
						if(i*block_size + ii >= r1) break;
						block_data_pos_y = block_data_pos_x;
						for(int jj=0; jj<block_size; jj++){
							if(j*block_size + jj >= r2) break;
							block_data_pos_z = block_data_pos_y;
							for(int kk=0; kk<block_size; kk++){
								if(k*block_size + kk >= r3) break;
								*block_data_pos_z = data_pos[ii*dec_buffer_size*dec_buffer_size + jj*dec_buffer_size + kk];
								block_data_pos_z ++;
							}
							block_data_pos_y += dec_block_dim1_offset;
						}
						block_data_pos_x += dec_block_dim0_offset;
					}

				}
			}
		}
	}
	free(unpred_offset);
	free(reg_params);
	free(blockwise_unpred_count);
	free(dec_buffer);
	free(coeff_result_type);

	free(indicator);
	free(result_type);

	// extract data
	int resi_x = s1 % block_size;
	int resi_y = s2 % block_size;
	int resi_z = s3 % block_size;
	*data = (float*) malloc(sizeof(float)*(e1 - s1) * (e2 - s2) * (e3 - s3));
	float * final_data_pos = *data;
	for(int i=0; i<(e1 - s1); i++){
		for(int j=0; j<(e2 - s2); j++){
			float * block_data_pos = dec_block_data + (i+resi_x)*dec_block_dim0_offset + (j+resi_y)*dec_block_dim1_offset + resi_z;
			for(int k=0; k<(e3 - s3); k++){
				*(final_data_pos++) = *(block_data_pos++);
			}
		}
	}
	free(dec_block_data);

}

int SZ_decompress_args_randomaccess_float(float** newData, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, 
size_t s5, size_t s4, size_t s3, size_t s2, size_t s1, // start point
size_t e5, size_t e4, size_t e3, size_t e2, size_t e1, // end point
unsigned char* cmpBytes, size_t cmpSize)
{
	if(confparams_dec==NULL)
		confparams_dec = (sz_params*)malloc(sizeof(sz_params));
	memset(confparams_dec, 0, sizeof(sz_params));
	if(exe_params==NULL)
		exe_params = (sz_exedata*)malloc(sizeof(sz_exedata));
	memset(exe_params, 0, sizeof(sz_exedata));
	
	int x = 1;
	char *y = (char*)&x;
	if(*y==1)
		sysEndianType = LITTLE_ENDIAN_SYSTEM;
	else //=0
		sysEndianType = BIG_ENDIAN_SYSTEM;	

	confparams_dec->randomAccess = 1;
	
	int status = SZ_SCES;
	size_t dataLength = computeDataLength(r5,r4,r3,r2,r1);
	
	//unsigned char* tmpBytes;
	size_t targetUncompressSize = dataLength <<2; //i.e., *4
	//tmpSize must be "much" smaller than dataLength
	size_t i, tmpSize = 8+MetaDataByteLength+exe_params->SZ_SIZE_TYPE;
	unsigned char* szTmpBytes;	
	
	if(cmpSize!=8+4+MetaDataByteLength && cmpSize!=8+8+MetaDataByteLength) //4,8 means two posibilities of SZ_SIZE_TYPE
	{
		confparams_dec->losslessCompressor = is_lossless_compressed_data(cmpBytes, cmpSize);
		if(confparams_dec->szMode!=SZ_TEMPORAL_COMPRESSION)
		{
			if(confparams_dec->losslessCompressor!=-1)
				confparams_dec->szMode = SZ_BEST_COMPRESSION;
			else
				confparams_dec->szMode = SZ_BEST_SPEED;			
		}
		
		if(confparams_dec->szMode==SZ_BEST_SPEED)
		{
			tmpSize = cmpSize;
			szTmpBytes = cmpBytes;	
		}
		else if(confparams_dec->szMode==SZ_BEST_COMPRESSION || confparams_dec->szMode==SZ_DEFAULT_COMPRESSION || confparams_dec->szMode==SZ_TEMPORAL_COMPRESSION)
		{
			if(targetUncompressSize<MIN_ZLIB_DEC_ALLOMEM_BYTES) //Considering the minimum size
				targetUncompressSize = MIN_ZLIB_DEC_ALLOMEM_BYTES; 
			tmpSize = sz_lossless_decompress(confparams_dec->losslessCompressor, cmpBytes, (unsigned long)cmpSize, &szTmpBytes, (unsigned long)targetUncompressSize+4+MetaDataByteLength+exe_params->SZ_SIZE_TYPE);//		(unsigned long)targetUncompressSize+8: consider the total length under lossless compression mode is actually 3+4+1+targetUncompressSize		
		}
		else
		{
			printf("Wrong value of confparams_dec->szMode in the double compressed bytes.\n");
			status = SZ_MERR;
			return status;
		}	
	}
	else
		szTmpBytes = cmpBytes;	

	TightDataPointStorageF* tdps;
	new_TightDataPointStorageF_fromFlatBytes(&tdps, szTmpBytes, tmpSize);
	
	int dim = computeDimension(r5,r4,r3,r2,r1);	
	int floatSize = sizeof(float);
	if(tdps->isLossless)
	{
		*newData = (float*)malloc(floatSize*dataLength);
		if(sysEndianType==BIG_ENDIAN_SYSTEM)
		{
			memcpy(*newData, szTmpBytes+4+MetaDataByteLength+exe_params->SZ_SIZE_TYPE, dataLength*floatSize);
		}
		else
		{
			unsigned char* p = szTmpBytes+4+MetaDataByteLength+exe_params->SZ_SIZE_TYPE;
			for(i=0;i<dataLength;i++,p+=floatSize)
				(*newData)[i] = bytesToFloat(p);
		}		
	}
	else 
	{
		if(confparams_dec->randomAccess == 0 && (s1+s2+s3+s4+s5>0 || (r5-e5+r4-e4+r3-e3+r2-e2+r1-e1 > 0)))
		{
			printf("Error: you specified the random access mode for decompression, but the compressed data were generate in the non-random-access way.!\n");
			status = SZ_DERR;
		}
		else if (dim == 1)
		{
			//printf("Error: random access mode doesn't support 1D yet, but only 3D.\n");
			decompressDataSeries_float_1D_decompression_given_areas_with_blocked_regression(newData, r1, s1, e1, tdps->raBytes);
			//status = SZ_DERR;
		}
		else if(dim == 2)
		{
			//printf("Error: random access mode doesn't support 2D yet, but only 3D.\n");
			decompressDataSeries_float_2D_decompression_given_areas_with_blocked_regression(newData, r2, r1, s2, s1, e2, e1, tdps->raBytes);
			//status = SZ_DERR;
		}	
		else if(dim == 3)
		{
			decompressDataSeries_float_3D_decompression_given_areas_with_blocked_regression(newData, r3, r2, r1, s3, s2, s1, e3, e2, e1, tdps->raBytes);
			status = SZ_SCES;
		}
		else if(dim == 4)
		{
			printf("Error: random access mode doesn't support 4D yet, but only 3D.\n");
			status = SZ_DERR;			
		}	
		else
		{
			printf("Error: currently support only at most 4 dimensions!\n");
			status = SZ_DERR;
		}	
	}	
	
	free_TightDataPointStorageF2(tdps);
	if(confparams_dec->szMode!=SZ_BEST_SPEED && cmpSize!=8+MetaDataByteLength+exe_params->SZ_SIZE_TYPE)
		free(szTmpBytes);
	return status;
}
#endif
