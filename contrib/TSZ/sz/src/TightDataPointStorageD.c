/**
 *  @file TightPointDataStorageD.c
 *  @author Sheng Di and Dingwen Tao
 *  @date Aug, 2016
 *  @brief The functions used to construct the tightPointDataStorage element for storing compressed bytes.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdlib.h> 
#include <stdio.h>
#include <string.h>
#include "TightDataPointStorageD.h"
#include "sz.h"
#include "Huffman.h"
#include "transcode.h"
#include "fse.h"

//shortest possible stream: version(1) + same(1) + meta(MetaDataByteLength_double) + element count(4)
#define TDPS_MIN_HEADER_BYTES (2 + MetaDataByteLength_double + 4)
//largest fixed part that follows the element count: max quant intervals(4) + intervals(4) +
//median(8) + reqLength(1) + realPrecision(8) + type array / FSE sizes (2) + exactDataNum(8) +
//exactMidBytes_size(8)
#define TDPS_FIXED_TAIL_MAX_BYTES (4 + 4 + 8 + 1 + 8 + 8 + 8 + 8 + 8)

//every length and offset of a compressed stream comes from the payload, so all reads are
//range-checked against the buffer that was handed in
static bool tdpsHasBytes(size_t index, size_t need, size_t total)
{
	return index <= total && need <= total - index;
}

void new_TightDataPointStorageD_Empty(TightDataPointStorageD **this)
{
	if(this == NULL)
		return;
	TightDataPointStorageD* tdps = (TightDataPointStorageD*)malloc(sizeof(TightDataPointStorageD));
	if(tdps == NULL)
	{
		*this = NULL;
		return;
	}
	memset(tdps, 0, sizeof(TightDataPointStorageD));
	*this = tdps;
}

int new_TightDataPointStorageD_fromFlatBytes(TightDataPointStorageD **this, unsigned char* flatBytes, size_t flatBytesLength, sz_exedata* pde_exe, sz_params* pde_params)
{
	new_TightDataPointStorageD_Empty(this);
	size_t i, index = 0;

	if ((*this) == NULL || flatBytes == NULL || pde_exe == NULL || pde_params == NULL)
		return SZ_FORMAT_ERR;
	if (flatBytesLength < TDPS_MIN_HEADER_BYTES)
		return SZ_FORMAT_ERR;

	unsigned char version = flatBytes[index++]; //3
	unsigned char sameRByte = flatBytes[index++]; //1

    // parse data format
	switch (version)
	{
	case DATA_FROMAT_VER1:
		break;
	default:
	    printf(" error, compressed data format can not be recognised. ver=%d\n ", version);
		return SZ_ABS;
	}

	int same = sameRByte & 0x01;
	(*this)->ifAdtFse = (sameRByte & 0x02)>>1; 							//0000,0010
	(*this)->isLossless = (sameRByte & 0x10)>>4;
	pde_exe->SZ_SIZE_TYPE = ((sameRByte & 0x40)>>6)==1?8:4;
	//pde_params->protectValueRange = (sameRByte & 0x04)>>2;
	pde_params->accelerate_pw_rel_compression = (sameRByte & 0x08) >> 3;
	int errorBoundMode = SZ_ABS;
	
	convertBytesToSZParams(&(flatBytes[index]), pde_params, pde_exe);

	index += MetaDataByteLength_double;

	int isRegression = (sameRByte >> 7) & 0x01;

	if (!tdpsHasBytes(index, pde_exe->SZ_SIZE_TYPE, flatBytesLength))
		return SZ_FORMAT_ERR;
	unsigned char dsLengthBytes[8];
	for (i = 0; i < pde_exe->SZ_SIZE_TYPE; i++)
		dsLengthBytes[i] = flatBytes[index++];
	(*this)->dataSeriesLength = bytesToSize(dsLengthBytes, pde_exe->SZ_SIZE_TYPE);
	if ((*this)->dataSeriesLength == 0)
		return SZ_FORMAT_ERR;

	if((*this)->isLossless==1)
	{
		//(*this)->exactMidBytes = flatBytes+8;
		return errorBoundMode;
	}
	else if(same==1)
	{
		if (!tdpsHasBytes(index, sizeof(double), flatBytesLength))
			return SZ_FORMAT_ERR;
		(*this)->allSameData = 1;
		(*this)->exactMidBytes = &(flatBytes[index]);
		return errorBoundMode;
	}
	else
		(*this)->allSameData = 0;
		
	if(isRegression == 1)
	{
		if (index > flatBytesLength)
			return SZ_FORMAT_ERR;
		(*this)->raBytes_size = flatBytesLength - index;
		(*this)->raBytes = &(flatBytes[index]);
		return errorBoundMode;
	}						

	if (!tdpsHasBytes(index, TDPS_FIXED_TAIL_MAX_BYTES, flatBytesLength))
		return SZ_FORMAT_ERR;
	unsigned char byteBuf[8];

	for (i = 0; i < 4; i++)
		byteBuf[i] = flatBytes[index++];
	int max_quant_intervals = bytesToInt32_bigEndian(byteBuf);// 4	

	pde_params->maxRangeRadius = max_quant_intervals/2;

	for (i = 0; i < 4; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->intervals = bytesToInt32_bigEndian(byteBuf);// 4	
	if ((*this)->intervals == 0 || (*this)->intervals > MAX_QUANTIZATION_INTERVALS)
		return SZ_FORMAT_ERR;

	for (i = 0; i < 8; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->medianValue = bytesToDouble(byteBuf);//8

	(*this)->reqLength = flatBytes[index++]; //1
	//the decoder indexes fixed 8-byte (double) buffers with reqLength, and the encoder clamps it
	//to 64, so anything above that is a malformed stream
	if ((unsigned char)(*this)->reqLength > 64)
	{
		printf(" error, invalid reqLength=%d in double compressed data\n", (int)(*this)->reqLength);
		return SZ_FORMAT_ERR;
	}
	
	for (i = 0; i < 8; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->realPrecision = bytesToDouble(byteBuf);//8
	
	if ((*this)->ifAdtFse == 0) {
		for (i = 0; i < pde_exe->SZ_SIZE_TYPE; i++)
			byteBuf[i] = flatBytes[index++];
		(*this)->typeArray_size = bytesToSize(byteBuf, pde_exe->SZ_SIZE_TYPE);
	} else {
		for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
			byteBuf[i] = flatBytes[index++];
		(*this)->FseCode_size = bytesToSize(byteBuf, pde_exe->SZ_SIZE_TYPE);// 4
		for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
			byteBuf[i] = flatBytes[index++];
		(*this)->transCodeBits_size = bytesToSize(byteBuf, pde_exe->SZ_SIZE_TYPE);// 4
	}

	if (!tdpsHasBytes(index, pde_exe->SZ_SIZE_TYPE, flatBytesLength))
		return SZ_FORMAT_ERR;
	for (i = 0; i < pde_exe->SZ_SIZE_TYPE; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->exactDataNum = bytesToSize(byteBuf, pde_exe->SZ_SIZE_TYPE);// ST
	if ((*this)->exactDataNum > (*this)->dataSeriesLength) //at most one exact value per element
		return SZ_FORMAT_ERR;

	if (!tdpsHasBytes(index, pde_exe->SZ_SIZE_TYPE, flatBytesLength))
		return SZ_FORMAT_ERR;
	for (i = 0; i < pde_exe->SZ_SIZE_TYPE; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->exactMidBytes_size = bytesToSize(byteBuf, pde_exe->SZ_SIZE_TYPE);// ST

	size_t logicLeadNumBitsNum = (*this)->exactDataNum * 2;
	if (logicLeadNumBitsNum % 8 == 0)
	{
		(*this)->leadNumArray_size = logicLeadNumBitsNum >> 3;
	}
	else
	{
		(*this)->leadNumArray_size = (logicLeadNumBitsNum >> 3) + 1;
	}
	
	if ((*this)->ifAdtFse == 0) {
		if ((*this)->typeArray_size < 8 || !tdpsHasBytes(index, (*this)->typeArray_size, flatBytesLength))
			return SZ_FORMAT_ERR;
		(*this)->typeArray = &flatBytes[index];
		//retrieve the number of states (i.e., stateNum)
		(*this)->allNodes = bytesToInt32_bigEndian((*this)->typeArray); //the first 4 bytes store the stateNum
		//the serialized tree needs at least 7 bytes per node (uchar layout) and the node count sizes
		//the Huffman pool, so bound it before it reaches the tree builder
		if ((*this)->allNodes <= 0 || (*this)->allNodes > MAX_HUFF_TREE_NODES ||
			(size_t)(*this)->allNodes > ((*this)->typeArray_size - 8) / 7)
			return SZ_FORMAT_ERR;
		(*this)->stateNum = ((*this)->allNodes+1)/2;
		index+=(*this)->typeArray_size;
	} else {
		if (!tdpsHasBytes(index, (*this)->FseCode_size, flatBytesLength))
			return SZ_FORMAT_ERR;
		(*this)->FseCode = &flatBytes[index]; 
		index+=(*this)->FseCode_size;

		if (!tdpsHasBytes(index, (*this)->transCodeBits_size, flatBytesLength))
			return SZ_FORMAT_ERR;
		(*this)->transCodeBits = &flatBytes[index]; 
		index+=(*this)->transCodeBits_size;
	}

	if (!tdpsHasBytes(index, (*this)->leadNumArray_size, flatBytesLength))
		return SZ_FORMAT_ERR;
	(*this)->leadNumArray = &flatBytes[index];
	
	index+=(*this)->leadNumArray_size;
	
	if (!tdpsHasBytes(index, (*this)->exactMidBytes_size, flatBytesLength))
		return SZ_FORMAT_ERR;
	(*this)->exactMidBytes = &flatBytes[index];
	
	index+=(*this)->exactMidBytes_size;
	
	(*this)->residualMidBits = &flatBytes[index];
	
	// calc residualMidBits_size: whatever is left after the parsed sections (never underflows).
	// It must be computed after the lead/exact arrays, otherwise the size also covers them and the
	// decoder's residualMidBits bounds check would let a malformed block read past the payload
	if (index > flatBytesLength)
		return SZ_FORMAT_ERR;
	(*this)->residualMidBits_size = flatBytesLength - index;
	
	
	return errorBoundMode;
}

/**
 * 
 * type's length == dataSeriesLength
 * exactMidBytes's length == exactMidBytes_size
 * leadNumIntArray's length == exactDataNum
 * escBytes's length == escBytes_size
 * resiBitLength's length == resiBitLengthSize
 * */
void new_TightDataPointStorageD(TightDataPointStorageD **this, 
		size_t dataSeriesLength, size_t exactDataNum, 
		int* type, unsigned char* exactMidBytes, size_t exactMidBytes_size,
		unsigned char* leadNumIntArray,  //leadNumIntArray contains readable numbers....
		unsigned char* resiMidBits, size_t resiMidBits_size,
		unsigned char resiBitLength, 
		double realPrecision, double medianValue, char reqLength, unsigned int intervals,
		unsigned char radExpo) 
{
	//int i = 0;
	*this = (TightDataPointStorageD *)malloc(sizeof(TightDataPointStorageD));
	memset(*this, 0, sizeof(TightDataPointStorageD));
	(*this)->ifAdtFse = confparams_cpr->ifAdtFse;
	(*this)->allSameData = 0;
	(*this)->realPrecision = realPrecision;
	(*this)->medianValue = medianValue;
	(*this)->reqLength = reqLength;

	(*this)->dataSeriesLength = dataSeriesLength;
	(*this)->exactDataNum = exactDataNum;

	if ((*this)->ifAdtFse == 0) {
		// huffman
		int stateNum = 2 * intervals;
		HuffmanTree* huffmanTree = createHuffmanTree(stateNum);
		encode_withTree(huffmanTree, type, dataSeriesLength, &(*this)->typeArray, &(*this)->typeArray_size);
		SZ_ReleaseHuffman(huffmanTree);
	}
	else {
		// fse
		encode_with_fse(type, dataSeriesLength, intervals, &((*this)->FseCode), &((*this)->FseCode_size), 
					&((*this)->transCodeBits), &((*this)->transCodeBits_size));
	}
		
	(*this)->exactMidBytes = exactMidBytes;
	(*this)->exactMidBytes_size = exactMidBytes_size;

	(*this)->leadNumArray_size = convertIntArray2ByteArray_fast_2b(leadNumIntArray, exactDataNum, &((*this)->leadNumArray));

	(*this)->residualMidBits_size = convertIntArray2ByteArray_fast_dynamic(resiMidBits, resiBitLength, exactDataNum, &((*this)->residualMidBits));
	
	(*this)->intervals = intervals;
	
	(*this)->isLossless = 0;

	(*this)->radExpo = radExpo;
}

void convertTDPStoBytes_double(TightDataPointStorageD* tdps, unsigned char* bytes, unsigned char* dsLengthBytes, unsigned char sameByte)
{
	size_t i, k = 0;
	unsigned char intervalsBytes[4];
	unsigned char typeArrayLengthBytes[8];
	unsigned char exactLengthBytes[8];
	unsigned char exactMidBytesLength[8];
	unsigned char realPrecisionBytes[8];
	unsigned char fsecodeLengthBytes[8];
	unsigned char transcodeLengthBytes[8];
	
	unsigned char medianValueBytes[8];
	unsigned char max_quant_intervals_Bytes[4];
	
	bytes[k++] = versionNumber;
	bytes[k++] = sameByte;	//1	byte	
	
	convertSZParamsToBytes(confparams_cpr, &(bytes[k]), exe_params->optQuantMode);
	k = k + MetaDataByteLength_double;
	
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST: 4 or 8 bytes
		bytes[k++] = dsLengthBytes[i];	
	int32ToBytes_bigEndian(max_quant_intervals_Bytes, confparams_cpr->max_quant_intervals);
	for(i = 0;i<4;i++)//4
		bytes[k++] = max_quant_intervals_Bytes[i];		
	
	int32ToBytes_bigEndian(intervalsBytes, tdps->intervals);
	for(i = 0;i<4;i++)//4
		bytes[k++] = intervalsBytes[i];		
	
	doubleToBytes(medianValueBytes, tdps->medianValue);
	for (i = 0; i < 8; i++)// 8
		bytes[k++] = medianValueBytes[i];		

	bytes[k++] = tdps->reqLength; //1 byte

	doubleToBytes(realPrecisionBytes, tdps->realPrecision);
	for (i = 0; i < 8; i++)// 8
		bytes[k++] = realPrecisionBytes[i];	

   	if (tdps->ifAdtFse == 0) {
		sizeToBytes(typeArrayLengthBytes, tdps->typeArray_size, exe_params->SZ_SIZE_TYPE);
		for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
			bytes[k++] = typeArrayLengthBytes[i];			
	} else {
		sizeToBytes(fsecodeLengthBytes, tdps->FseCode_size, exe_params->SZ_SIZE_TYPE);
		for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
			bytes[k++] = fsecodeLengthBytes[i];	

		sizeToBytes(transcodeLengthBytes, tdps->transCodeBits_size, exe_params->SZ_SIZE_TYPE);
		for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
			bytes[k++] = transcodeLengthBytes[i];
	}	
				
	sizeToBytes(exactLengthBytes, tdps->exactDataNum, exe_params->SZ_SIZE_TYPE);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = exactLengthBytes[i];

	sizeToBytes(exactMidBytesLength, tdps->exactMidBytes_size, exe_params->SZ_SIZE_TYPE);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = exactMidBytesLength[i];

	if(confparams_cpr->errorBoundMode>=PW_REL)
	{
		doubleToBytes(exactMidBytesLength, tdps->minLogValue);
		for(i = 0;i < 8; i++)
			bytes[k++] = exactMidBytesLength[i];
	}
    
	// copy data
	if (tdps->ifAdtFse == 0) {
		memcpy(&(bytes[k]), tdps->typeArray, tdps->typeArray_size);
		k += tdps->typeArray_size;
	} else {
		memcpy(&(bytes[k]), tdps->FseCode, tdps->FseCode_size);
		k += tdps->FseCode_size;
		memcpy(&(bytes[k]), tdps->transCodeBits, tdps->transCodeBits_size);
		k += tdps->transCodeBits_size;
	}
	memcpy(&(bytes[k]), tdps->leadNumArray, tdps->leadNumArray_size);
	k += tdps->leadNumArray_size;
	memcpy(&(bytes[k]), tdps->exactMidBytes, tdps->exactMidBytes_size);
	k += tdps->exactMidBytes_size;

	if(tdps->residualMidBits!=NULL)
	{
		memcpy(&(bytes[k]), tdps->residualMidBits, tdps->residualMidBits_size);
		k += tdps->residualMidBits_size;
	}		
}

//Convert TightDataPointStorageD to bytes...
bool convertTDPStoFlatBytes_double(TightDataPointStorageD *tdps, unsigned char* bytes, size_t *size) 
{
	size_t i, k = 0; 
	unsigned char dsLengthBytes[8];
	
	if(exe_params->SZ_SIZE_TYPE==4)
		int32ToBytes_bigEndian(dsLengthBytes, tdps->dataSeriesLength);//4
	else
		int64ToBytes_bigEndian(dsLengthBytes, tdps->dataSeriesLength);//8
	
	unsigned char sameByte = tdps->allSameData==1?(unsigned char)1:(unsigned char)0;
	//sameByte = sameByte | (confparams_cpr->szMode << 1);
	sameByte = sameByte | (tdps->ifAdtFse << 1);      //0000,0010
	if(tdps->isLossless)
		sameByte = (unsigned char) (sameByte | 0x10);	
	if(confparams_cpr->errorBoundMode>=PW_REL)
		sameByte = (unsigned char) (sameByte | 0x20); // 00100000, the 5th bit
	if(exe_params->SZ_SIZE_TYPE==8)
		sameByte = (unsigned char) (sameByte | 0x40); // 01000000, the 6th bit
	if(confparams_cpr->errorBoundMode == PW_REL && confparams_cpr->accelerate_pw_rel_compression)
		sameByte = (unsigned char) (sameByte | 0x08); 	
	
	if(tdps->allSameData==1)
	{
		size_t totalByteLength = 1 + 1 + MetaDataByteLength_double + exe_params->SZ_SIZE_TYPE + tdps->exactMidBytes_size;
		//bytes = (unsigned char *)malloc(sizeof(unsigned char)*totalByteLength); // comment by tickduan
		if(totalByteLength >= tdps->dataSeriesLength * sizeof(double))
		{
			return false;
		}
	
		bytes[k++] = versionNumber;
		bytes[k++] = sameByte;

		convertSZParamsToBytes(confparams_cpr, &(bytes[k]), exe_params->optQuantMode);
		k = k + MetaDataByteLength_double;

		for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
			bytes[k++] = dsLengthBytes[i];
		
		for (i = 0; i < tdps->exactMidBytes_size; i++)
			bytes[k++] = tdps->exactMidBytes[i];
		
		*size = totalByteLength;
	}
	else
	{
		size_t residualMidBitsLength = tdps->residualMidBits == NULL ? 0 : tdps->residualMidBits_size;
		size_t totalByteLength = 1 + 1 + MetaDataByteLength_double + exe_params->SZ_SIZE_TYPE + 4 + 4 + 8 + 1 + 8 
				+ exe_params->SZ_SIZE_TYPE + exe_params->SZ_SIZE_TYPE
				+ tdps->leadNumArray_size 
				+ tdps->exactMidBytes_size 
				+ residualMidBitsLength;				
			
		if (tdps->ifAdtFse == 0) {
			totalByteLength += tdps->typeArray_size + exe_params->SZ_SIZE_TYPE;
		} else {
			totalByteLength += tdps->FseCode_size + tdps->transCodeBits_size + exe_params->SZ_SIZE_TYPE + exe_params->SZ_SIZE_TYPE;
		}

		//*bytes = (unsigned char *)malloc(sizeof(unsigned char)*totalByteLength);  comment by tickduan
		if(totalByteLength >= tdps->dataSeriesLength * sizeof(double))
		{
			return false;
		}

		convertTDPStoBytes_double(tdps, bytes, dsLengthBytes, sameByte);
		*size = totalByteLength;
	}

	return true;
}


void free_TightDataPointStorageD(TightDataPointStorageD *tdps)
{
	if(tdps->leadNumArray!=NULL)
		free(tdps->leadNumArray);
	if(tdps->FseCode!=NULL)
		free(tdps->FseCode);
	if(tdps->transCodeBits!=NULL)
		free(tdps->transCodeBits);
	if(tdps->exactMidBytes!=NULL)
		free(tdps->exactMidBytes);
	if(tdps->residualMidBits!=NULL)
		free(tdps->residualMidBits);
	if(tdps->typeArray)
	    free(tdps->typeArray);
	free(tdps);
}

/**
 * to free the memory used in the decompression
 * */
void free_TightDataPointStorageD2(TightDataPointStorageD *tdps)
{			
	free(tdps);
}
