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
