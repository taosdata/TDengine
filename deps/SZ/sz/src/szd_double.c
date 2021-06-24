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
size_t cmpSize, int compressionType, double* hist_data)
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
			tmpSize = sz_lossless_decompress(confparams_dec->losslessCompressor, cmpBytes, (unsigned long)cmpSize, &szTmpBytes, (unsigned long)targetUncompressSize+4+MetaDataByteLength_double+exe_params->SZ_SIZE_TYPE);			
			//szTmpBytes = (unsigned char*)malloc(sizeof(unsigned char)*tmpSize);
			//memcpy(szTmpBytes, tmpBytes, tmpSize);
			//free(tmpBytes); //release useless memory		
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
		
	confparams_dec->sol_ID = szTmpBytes[4+14]; //szTmpBytes: version(3bytes), samebyte(1byte), [14]:sol_ID=SZ or SZ_Transpose		
	//TODO: convert szTmpBytes to double array.
	TightDataPointStorageD* tdps;
	int errBoundMode = new_TightDataPointStorageD_fromFlatBytes(&tdps, szTmpBytes, tmpSize);

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
	else if(confparams_dec->sol_ID==SZ_Transpose)
	{
		getSnapshotData_double_1D(newData,dataLength,tdps, errBoundMode, 0, hist_data);		
	}
	else //confparams_dec->sol_ID==SZ
	{
		if(tdps->raBytes_size > 0) //v2.0
		{
			if (dim == 1)
				getSnapshotData_double_1D(newData,r1,tdps, errBoundMode, 0, hist_data);
			else if(dim == 2)
				decompressDataSeries_double_2D_nonblocked_with_blocked_regression(newData, r2, r1, tdps->raBytes, hist_data);
			else if(dim == 3)
				decompressDataSeries_double_3D_nonblocked_with_blocked_regression(newData, r3, r2, r1, tdps->raBytes, hist_data);
			else if(dim == 4)
				decompressDataSeries_double_3D_nonblocked_with_blocked_regression(newData, r4*r3, r2, r1, tdps->raBytes, hist_data);
			else
			{
				printf("Error: currently support only at most 4 dimensions!\n");
				status = SZ_DERR;
			}	
		}
		else //1.4.13 or time-based compression
		{
			if (dim == 1)
				getSnapshotData_double_1D(newData,r1,tdps, errBoundMode, compressionType, hist_data);
			else
			if (dim == 2)
				getSnapshotData_double_2D(newData,r2,r1,tdps, errBoundMode, compressionType, hist_data);
			else
			if (dim == 3)
				getSnapshotData_double_3D(newData,r3,r2,r1,tdps, errBoundMode, compressionType, hist_data);
			else
			if (dim == 4)
				getSnapshotData_double_4D(newData,r4,r3,r2,r1,tdps, errBoundMode, compressionType, hist_data);			
			else
			{
				printf("Error: currently support only at most 4 dimensions!\n");
				status = SZ_DERR;
			}			
		}
	}	

	if(confparams_dec->protectValueRange)
	{
		double* nd = *newData;
		double min = confparams_dec->dmin;
		double max = confparams_dec->dmax;		
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
	if(confparams_dec->szMode!=SZ_BEST_SPEED && cmpSize!=12+MetaDataByteLength_double+exe_params->SZ_SIZE_TYPE)
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

void decompressDataSeries_double_2D(double** data, size_t r1, size_t r2, double* hist_data, TightDataPointStorageD* tdps) 
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
	double realPrecision = tdps->realPrecision;

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
	double medianValue, exactData;
	int type_;

	reqBytesLength = tdps->reqLength/8;
	resiBitsLength = tdps->reqLength%8;
	medianValue = tdps->medianValue;

	double pred1D, pred2D;
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
	(*data)[0] = exactData + medianValue;
	memcpy(preBytes,curBytes,8);

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
		(*data)[1] = exactData + medianValue;
		memcpy(preBytes,curBytes,8);
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
			(*data)[jj] = exactData + medianValue;
			memcpy(preBytes,curBytes,8);
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
			(*data)[index] = exactData + medianValue;
			memcpy(preBytes,curBytes,8);
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
				(*data)[index] = exactData + medianValue;
				memcpy(preBytes,curBytes,8);
			}
		}
	}

#ifdef HAVE_TIMECMPR	
	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
		memcpy(hist_data, (*data), dataSeriesLength*sizeof(double));
#endif	

	free(leadNum);
	free(type);
	return;
}

void decompressDataSeries_double_3D(double** data, size_t r1, size_t r2, size_t r3, double* hist_data, TightDataPointStorageD* tdps) 
{
	//updateQuantizationInfo(tdps->intervals);
	int intvRadius = tdps->intervals/2;
	size_t j, k = 0, p = 0, l = 0; // k is to track the location of residual_bit
	// in resiMidBits, p is to track the
	// byte_index of resiMidBits, l is for
	// leadNum
	size_t dataSeriesLength = r1*r2*r3;
	size_t r23 = r2*r3;
//	printf ("%d %d %d\n", r1, r2, r3);

	unsigned char* leadNum;
	double realPrecision = tdps->realPrecision;

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
	double medianValue, exactData;
	int type_;

	reqBytesLength = tdps->reqLength/8;
	resiBitsLength = tdps->reqLength%8;
	medianValue = tdps->medianValue;

	double pred1D, pred2D, pred3D;
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
	(*data)[0] = exactData + medianValue;
	memcpy(preBytes,curBytes,8);

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
		(*data)[1] = exactData + medianValue;
		memcpy(preBytes,curBytes,8);
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
			(*data)[jj] = exactData + medianValue;
			memcpy(preBytes,curBytes,8);
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
			(*data)[index] = exactData + medianValue;
			memcpy(preBytes,curBytes,8);
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
				(*data)[index] = exactData + medianValue;
				memcpy(preBytes,curBytes,8);
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
			(*data)[index] = exactData + medianValue;
			memcpy(preBytes,curBytes,8);
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
				(*data)[index] = exactData + medianValue;
				memcpy(preBytes,curBytes,8);
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
				(*data)[index] = exactData + medianValue;
				memcpy(preBytes,curBytes,8);
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
					(*data)[index] = exactData + medianValue;
					memcpy(preBytes,curBytes,8);
				}
			}
		}
	}

#ifdef HAVE_TIMECMPR	
	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
		memcpy(hist_data, (*data), dataSeriesLength*sizeof(double));
#endif	

	free(leadNum);
	free(type);
	return;
}

void decompressDataSeries_double_4D(double** data, size_t r1, size_t r2, size_t r3, size_t r4, double* hist_data, TightDataPointStorageD* tdps)
{
	//updateQuantizationInfo(tdps->intervals);
	int intvRadius = tdps->intervals/2;
	size_t j, k = 0, p = 0, l = 0; // k is to track the location of residual_bit
	// in resiMidBits, p is to track the
	// byte_index of resiMidBits, l is for
	// leadNum
	size_t dataSeriesLength = r1*r2*r3*r4;
	size_t r234 = r2*r3*r4;
	size_t r34 = r3*r4;
//	printf ("%d %d %d\n", r1, r2, r3, r4);

	unsigned char* leadNum;
	double realPrecision = tdps->realPrecision;

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
	double medianValue, exactData;
	int type_;

	reqBytesLength = tdps->reqLength/8;
	resiBitsLength = tdps->reqLength%8;
	medianValue = tdps->medianValue;

	double pred1D, pred2D, pred3D;
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
		(*data)[index] = exactData + medianValue;
		memcpy(preBytes,curBytes,8);

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
			(*data)[index] = exactData + medianValue;
			memcpy(preBytes,curBytes,8);
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
				(*data)[index] = exactData + medianValue;
				memcpy(preBytes,curBytes,8);
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
				(*data)[index] = exactData + medianValue;
				memcpy(preBytes,curBytes,8);
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
					(*data)[index] = exactData + medianValue;
					memcpy(preBytes,curBytes,8);
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
				(*data)[index] = exactData + medianValue;
				memcpy(preBytes,curBytes,8);
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
					(*data)[index] = exactData + medianValue;
					memcpy(preBytes,curBytes,8);
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
					(*data)[index] = exactData + medianValue;
					memcpy(preBytes,curBytes,8);
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
						(*data)[index] = exactData + medianValue;
						memcpy(preBytes,curBytes,8);
					}
				}
			}
		}
	}

//I didn't implement time-based compression for 4D actually. 
//#ifdef HAVE_TIMECMPR	
//	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
//		memcpy(multisteps->hist_data, (*data), dataSeriesLength*sizeof(double));
//#endif	

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

void decompressDataSeries_double_2D_MSST19(double** data, size_t r1, size_t r2, TightDataPointStorageD* tdps) 
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
	double exactData;
	int type_;

    double* precisionTable = (double*)malloc(sizeof(double) * intvCapacity);
    double inv = 2.0-pow(2, -(tdps->plus_bits));
    for(int i=0; i<intvCapacity; i++){
        double test = pow((1+tdps->realPrecision), inv*(i - intvRadius));
        precisionTable[i] = test;
    }

    reqBytesLength = tdps->reqLength/8;
	resiBitsLength = tdps->reqLength%8;
	//medianValue = tdps->medianValue;
	
	double pred1D, pred2D;
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
	(*data)[0] = exactData;
	memcpy(preBytes,curBytes,8);

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
		(*data)[1] = exactData;
		memcpy(preBytes,curBytes,8);
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
			(*data)[jj] = exactData;
			memcpy(preBytes,curBytes,8);
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
			(*data)[index] = exactData;
			memcpy(preBytes,curBytes,8);
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
				(*data)[index] = exactData;
				memcpy(preBytes,curBytes,8);
			}
		}
	}

#ifdef HAVE_TIMECMPR	
	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
		memcpy(multisteps->hist_data, (*data), dataSeriesLength*sizeof(double));
#endif	

	free(leadNum);
	free(type);
	return;
}

void decompressDataSeries_double_3D_MSST19(double** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageD* tdps) 
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

	*data = (double*)malloc(sizeof(double)*dataSeriesLength);
	int* type = (int*)malloc(dataSeriesLength*sizeof(int));

	double* precisionTable = (double*)malloc(sizeof(double) * intvCapacity);
	double inv = 2.0-pow(2, -(tdps->plus_bits));
	for(int i=0; i<intvCapacity; i++){
		double test = pow((1+tdps->realPrecision), inv*(i - intvRadius));
		precisionTable[i] = test;
	}

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
	double exactData;
	int type_;

	reqBytesLength = tdps->reqLength/8;
	resiBitsLength = tdps->reqLength%8;
	
	double pred1D, pred2D, pred3D;
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
	(*data)[0] = exactData;
	memcpy(preBytes,curBytes,8);

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
		(*data)[1] = exactData;
		memcpy(preBytes,curBytes,8);
	}
	/* Process Row-0, data 2 --> data r3-1 */
	for (jj = 2; jj < r3; jj++)
	{
		temp = (*data)[jj-1];
		pred1D = temp * ( *data)[jj-1] / (*data)[jj-2];

		type_ = type[jj];
		if (type_ != 0)
		{
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
			(*data)[jj] = exactData;
			memcpy(preBytes,curBytes,8);
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
			(*data)[index] = exactData;
			memcpy(preBytes,curBytes,8);
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
				(*data)[index] = exactData;
				memcpy(preBytes,curBytes,8);
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
			(*data)[index] = exactData;
			memcpy(preBytes,curBytes,8);
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
				(*data)[index] = exactData;
				memcpy(preBytes,curBytes,8);
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
				(*data)[index] = exactData;
				memcpy(preBytes,curBytes,8);
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
					(*data)[index] = fabs(pred3D) * precisionTable[type_];	
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
					(*data)[index] = exactData;
					memcpy(preBytes,curBytes,8);
				}
			}
		}
	}
	
#ifdef HAVE_TIMECMPR	
	if(confparams_dec->szMode == SZ_TEMPORAL_COMPRESSION)
		memcpy(multisteps->hist_data, (*data), dataSeriesLength*sizeof(double));
#endif		

	free(leadNum);
	free(type);
	return;
}

void getSnapshotData_double_1D(double** data, size_t dataSeriesLength, TightDataPointStorageD* tdps, int errBoundMode, int compressionType, double* hist_data) 
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

void getSnapshotData_double_2D(double** data, size_t r1, size_t r2, TightDataPointStorageD* tdps, int errBoundMode, int compressionType, double* hist_data)  
{
	size_t i;
	size_t dataSeriesLength = r1*r2;
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
					if(compressionType == 0) //snapshot
						decompressDataSeries_double_2D(data, r1, r2, hist_data, tdps);
					else
						decompressDataSeries_double_1D_ts(data, dataSeriesLength, hist_data, tdps);					
				}
				else
#endif						
					decompressDataSeries_double_2D(data, r1, r2, hist_data, tdps);
			}
			else 
				//decompressDataSeries_double_2D_pwr(data, r1, r2, tdps);
				if(confparams_dec->accelerate_pw_rel_compression)
					decompressDataSeries_double_2D_pwr_pre_log_MSST19(data, r1, r2, tdps);
				else
					decompressDataSeries_double_2D_pwr_pre_log(data, r1, r2, tdps);
			return;
		} else {
			//TODO
		}
	}
}

void getSnapshotData_double_3D(double** data, size_t r1, size_t r2, size_t r3, TightDataPointStorageD* tdps, int errBoundMode, int compressionType, double* hist_data) 
{
	size_t i;
	size_t dataSeriesLength = r1*r2*r3;
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
					if(compressionType == 0) //snapshot
						decompressDataSeries_double_3D(data, r1, r2, r3, hist_data, tdps);
					else
						decompressDataSeries_double_1D_ts(data, dataSeriesLength, hist_data, tdps);					
				}
				else
#endif						
					decompressDataSeries_double_3D(data, r1, r2, r3, hist_data, tdps);
			}
			else 
			{
				//decompressDataSeries_double_3D_pwr(data, r1, r2, r3, tdps);
				if(confparams_dec->accelerate_pw_rel_compression)
					decompressDataSeries_double_3D_pwr_pre_log_MSST19(data, r1, r2, r3, tdps);
				else
					decompressDataSeries_double_3D_pwr_pre_log(data, r1, r2, r3, tdps);
			}	
			return;
		} else {
			//TODO
		}
	}
}

void getSnapshotData_double_4D(double** data, size_t r1, size_t r2, size_t r3, size_t r4, TightDataPointStorageD* tdps, int errBoundMode, int compressionType, double* hist_data)
{
	size_t i;
	size_t dataSeriesLength = r1*r2*r3*r4;
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
					if(multisteps->compressionType == 0)
						decompressDataSeries_double_4D(data, r1, r2, r3, r4, hist_data, tdps);
					else
						decompressDataSeries_double_1D_ts(data, r1*r2*r3*r4, hist_data, tdps);					
				}
				else
#endif				
					decompressDataSeries_double_4D(data, r1, r2, r3, r4, hist_data, tdps);
			}
			else 
			{
				//decompressDataSeries_double_3D_pwr(data, r1*r2, r3, r4, tdps);
				if(confparams_dec->accelerate_pw_rel_compression)
					decompressDataSeries_double_3D_pwr_pre_log_MSST19(data, r1*r2, r3, r4, tdps);
				else
					decompressDataSeries_double_3D_pwr_pre_log(data, r1*r2, r3, r4, tdps);
			}					
			return;
		} else {
			//TODO
		}
	}
}

size_t decompressDataSeries_double_3D_RA_block(double * data, double mean, size_t dim_0, size_t dim_1, size_t dim_2, size_t block_dim_0, size_t block_dim_1, size_t block_dim_2, double realPrecision, int * type, double * unpredictable_data)
{
	int intvRadius = exe_params->intvRadius;
	
	size_t dim0_offset = dim_1 * dim_2;
	size_t dim1_offset = dim_2;

	size_t unpredictable_count = 0;
	size_t r1, r2, r3;
	r1 = block_dim_0;
	r2 = block_dim_1;
	r3 = block_dim_2;

	double * cur_data_pos = data;
	double * last_row_pos;
	double pred1D, pred2D, pred3D;
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
	///////////////////////////	Process layer-1 --> layer-r1-1 ///////////////////////////

	for (k = 1; k < r1; k++)
	{
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
		}
		last_row_pos = cur_data_pos;
		cur_data_pos += dim1_offset;

	    /* Process Row-1 --> Row-r2-1 */
		for (i = 1; i < r2; i++)
		{
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

void decompressDataSeries_double_2D_nonblocked_with_blocked_regression(double** data, size_t r1, size_t r2, unsigned char* comp_data, double* hist_data){

	size_t dim0_offset = r2;
	size_t num_elements = r1 * r2;

	*data = (double*)malloc(sizeof(double)*num_elements);

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

	double mean;
	unsigned char use_mean;
	memcpy(&use_mean, comp_data_pos, sizeof(unsigned char));
	comp_data_pos += sizeof(unsigned char);
	memcpy(&mean, comp_data_pos, sizeof(double));
	comp_data_pos += sizeof(double);
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
	double precision[3];
	double * coeff_unpred_data[3];
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
			coeff_unpred_data[i] = (double *) comp_data_pos;
			comp_data_pos += coeff_unpred_count * sizeof(double);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	double last_coefficients[3] = {0.0};
	int coeff_unpred_data_count[3] = {0};
	int coeff_index = 0;
	//updateQuantizationInfo(intervals);

	size_t total_unpred;
	memcpy(&total_unpred, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	double * unpred_data = (double *) comp_data_pos;
	comp_data_pos += total_unpred * sizeof(double);

	int * result_type = (int *) malloc(num_elements * sizeof(int));
	decode(comp_data_pos, num_elements, root, result_type);
	SZ_ReleaseHuffman(huffmanTree);
	
	int intvRadius = intervals/2;
	
	int * type;

	double * data_pos = *data;
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

					double * block_data_pos = data_pos;
					double pred;
					size_t index = 0;
					int type_;
					// d11 is current data
					size_t unpredictable_count = 0;
					double d00, d01, d10;
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
						double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
					
					double * block_data_pos = data_pos;
					double pred;
					size_t index = 0;
					int type_;
					// d11 is current data
					size_t unpredictable_count = 0;
					double d00, d01, d10;
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
						double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
		memcpy(hist_data, (*data), num_elements*sizeof(double));
#endif	
	
	free(coeff_result_type);

	free(indicator);
	free(result_type);
}


void decompressDataSeries_double_3D_nonblocked_with_blocked_regression(double** data, size_t r1, size_t r2, size_t r3, unsigned char* comp_data, double* hist_data){

	size_t dim0_offset = r2 * r3;
	size_t dim1_offset = r3;
	size_t num_elements = r1 * r2 * r3;

	*data = (double*)malloc(sizeof(double)*num_elements);

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
	node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree,comp_data_pos+4, nodeCount);
	comp_data_pos += sizeof(int) + tree_size;

	double mean;
	unsigned char use_mean;
	memcpy(&use_mean, comp_data_pos, sizeof(unsigned char));
	comp_data_pos += sizeof(unsigned char);
	memcpy(&mean, comp_data_pos, sizeof(double));
	comp_data_pos += sizeof(double);
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
	double * coeff_unpred_data[4];
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
			node root = reconstruct_HuffTree_from_bytes_anyStates(huffmanTree, comp_data_pos+4, nodeCount);
			comp_data_pos += sizeof(int) + tree_size;

			coeff_type[i] = coeff_result_type + i * num_blocks;
			size_t typeArray_size = bytesToSize(comp_data_pos);
			decode(comp_data_pos + sizeof(size_t), reg_count, root, coeff_type[i]);
			comp_data_pos += sizeof(size_t) + typeArray_size;
			int coeff_unpred_count = bytesToInt_bigEndian(comp_data_pos);
			comp_data_pos += sizeof(int);
			coeff_unpred_data[i] = (double *) comp_data_pos;
			comp_data_pos += coeff_unpred_count * sizeof(double);
			SZ_ReleaseHuffman(huffmanTree);
		}
	}
	double last_coefficients[4] = {0.0};
	int coeff_unpred_data_count[4] = {0};
	int coeff_index = 0;
	//updateQuantizationInfo(intervals);

	size_t total_unpred;
	memcpy(&total_unpred, comp_data_pos, sizeof(size_t));
	comp_data_pos += sizeof(size_t);
	double * unpred_data = (double *) comp_data_pos;
	comp_data_pos += total_unpred * sizeof(double);

	int * result_type = (int *) malloc(num_elements * sizeof(int));
	decode(comp_data_pos, num_elements, root, result_type);
	SZ_ReleaseHuffman(huffmanTree);
	
	int intvRadius = intervals/2;
	
	int * type;
	double * data_pos = *data;
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
		// 				// cur_unpred_count = decompressDataSeries_double_3D_blocked_nonblock_pred(data_pos, r1, r2, r3, current_blockcount_x, current_blockcount_y, current_blockcount_z, i, j, k, realPrecision, type, unpred_data);
		// 				double * block_data_pos = data_pos;
		// 				double pred;
		// 				size_t index = 0;
		// 				int type_;
		// 				// d111 is current data
		// 				size_t unpredictable_count = 0;
		// 				double d000, d001, d010, d011, d100, d101, d110;
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
		// 					double pred;
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
		// 					double * block_data_pos = data_pos;
		// 					double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
						double * block_data_pos = data_pos;
						double pred;
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
							double pred;
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
							double * block_data_pos = data_pos;
							double pred;
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
		memcpy(hist_data, (*data), num_elements*sizeof(double));
#endif	

	free(coeff_result_type);

	free(indicator);
	free(result_type);
}
