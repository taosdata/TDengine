/**
 *  @file TightPointDataStorageI.c
 *  @author Sheng Di and Dingwen Tao
 *  @date Aug, 2016
 *  @brief The functions used to construct the tightPointDataStorage element for storing compressed bytes.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdlib.h> 
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "TightDataPointStorageI.h"
#include "sz.h"
#include "Huffman.h"
//#include "rw.h"

int computeRightShiftBits(int exactByteSize, int dataType)
{
	int rightShift = 0; 
	switch(dataType)
	{
	case SZ_INT8:
	case SZ_UINT8:
		rightShift = 8 - exactByteSize*8;
		break;
	case SZ_INT16:
	case SZ_UINT16:
		rightShift = 16 - exactByteSize*8;
		break;
	case SZ_INT32:
	case SZ_UINT32:
		rightShift = 32 - exactByteSize*8;
		break;
	case SZ_INT64:
	case SZ_UINT64:
		rightShift = 64 - exactByteSize*8;
		break;
	}
	return rightShift;	
}

int convertDataTypeSizeCode(int dataTypeSizeCode)
{
	int result = 0;
	switch(dataTypeSizeCode)
	{
	case 0:
		result = 1;
		break;
	case 1:
		result = 2;
		break;
	case 2:
		result = 4;
		break;
	case 3:
		result = 8;
		break;
	}
	return result;	
}

int convertDataTypeSize(int dataTypeSize)
{
	int result = 0;
	switch(dataTypeSize)
	{
	case 1:
		result = 0; //0000
		break;
	case 2:
		result = 4; //0100
		break;
	case 4:
		result = 8; //1000
		break;
	case 8:
		result = 12; //1100
		break;
	}
	return result;
}

void new_TightDataPointStorageI_Empty(TightDataPointStorageI **this)
{
	*this = (TightDataPointStorageI*)malloc(sizeof(TightDataPointStorageI));

	(*this)->dataSeriesLength = 0;
	(*this)->allSameData = 0;
	(*this)->exactDataNum = 0;
	(*this)->realPrecision = 0;
	(*this)->minValue = 0;
	(*this)->exactByteSize = 0;

	(*this)->typeArray = NULL; //its size is dataSeriesLength/4 (or xxx/4+1) 
	(*this)->typeArray_size = 0;
	
	(*this)->exactDataBytes = NULL;
	(*this)->exactDataBytes_size = 0;

	(*this)->intervals = 0;
	(*this)->isLossless = 0;	
}

int new_TightDataPointStorageI_fromFlatBytes(TightDataPointStorageI **this, unsigned char* flatBytes, size_t flatBytesLength)
{
	new_TightDataPointStorageI_Empty(this);
	size_t i, index = 0;
	char version[3];
	for (i = 0; i < 3; i++)
		version[i] = flatBytes[index++]; //3
	unsigned char sameRByte = flatBytes[index++]; //1
	if(checkVersion2(version)!=1)
	{
		//wrong version
		printf("Wrong version: \nCompressed-data version (%d.%d.%d)\n",version[0], version[1], version[2]);
		printf("Current sz version: (%d.%d.%d)\n", versionNumber[0], versionNumber[1], versionNumber[2]);
		printf("Please double-check if the compressed data (or file) is correct.\n");
		exit(0);
	}
	int same = sameRByte & 0x01;
	//conf_params->szMode = (sameRByte & 0x06)>>1;
	int dataByteSizeCode = (sameRByte & 0x0C)>>2;
	convertDataTypeSizeCode(dataByteSizeCode); //in bytes
	(*this)->isLossless = (sameRByte & 0x10)>>4;

	exe_params->SZ_SIZE_TYPE = ((sameRByte & 0x40)>>6)==1?8:4;
	int errorBoundMode = ABS;
	
	if(confparams_dec==NULL)
	{
		confparams_dec = (sz_params*)malloc(sizeof(sz_params));
		memset(confparams_dec, 0, sizeof(sz_params));
	}	
	convertBytesToSZParams(&(flatBytes[index]), confparams_dec);
	/*sz_params* params = convertBytesToSZParams(&(flatBytes[index]));
	int mode = confparams_dec->szMode;
	int losslessCompressor = confparams_dec->losslessCompressor;
	if(confparams_dec!=NULL)
		free(confparams_dec);
	confparams_dec = params;
	confparams_dec->szMode = mode;
	confparams_dec->losslessCompressor = losslessCompressor;*/
	
	index += MetaDataByteLength; //20	
	
	if(same==0)
		(*this)->exactByteSize = flatBytes[index++]; //1
	
	unsigned char dsLengthBytes[8];
	for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
		dsLengthBytes[i] = flatBytes[index++];
	(*this)->dataSeriesLength = bytesToSize(dsLengthBytes);// ST
	if((*this)->isLossless==1)
	{
		//(*this)->exactMidBytes = flatBytes+8;
		return errorBoundMode;
	}
	else if(same==1)
	{
		(*this)->allSameData = 1;
		(*this)->exactDataBytes = &(flatBytes[index]);
		return errorBoundMode;
	}
	else
		(*this)->allSameData = 0;

	unsigned char byteBuf[8];

	for (i = 0; i < 4; i++)
		byteBuf[i] = flatBytes[index++];
	int max_quant_intervals = bytesToInt_bigEndian(byteBuf);// 4	

	confparams_dec->maxRangeRadius = max_quant_intervals/2;

	if(errorBoundMode>=PW_REL)
	{
		printf("Error: errorBoundMode>=PW_REL in new_TightDataPointStorageI_fromFlatBytes!! Wrong...\n");
		exit(0);
	}

	for (i = 0; i < 4; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->intervals = bytesToInt_bigEndian(byteBuf);// 4	

	for (i = 0; i < 8; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->minValue = bytesToLong_bigEndian(byteBuf); //8
		
	for (i = 0; i < 8; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->realPrecision = bytesToDouble(byteBuf);//8
	
	for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->typeArray_size = bytesToSize(byteBuf);// ST		

	for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->exactDataNum = bytesToSize(byteBuf);// ST
	
	for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->exactDataBytes_size = bytesToSize(byteBuf);// ST		


	(*this)->typeArray = &flatBytes[index];
	//retrieve the number of states (i.e., stateNum)
	(*this)->allNodes = bytesToInt_bigEndian((*this)->typeArray); //the first 4 bytes store the stateNum
	(*this)->stateNum = ((*this)->allNodes+1)/2;		

	index+=(*this)->typeArray_size;
	
	if((*this)->exactDataBytes_size > 0)
	{	
		(*this)->exactDataBytes = &flatBytes[index];
		index+=(*this)->exactDataBytes_size*sizeof(char);	
	}
	else
		(*this)->exactDataBytes = NULL;	
	return errorBoundMode;
}

/**
 *
 * type's length == dataSeriesLength
 * exactDataBytes's length == exactDataBytes_size
 * */
void new_TightDataPointStorageI(TightDataPointStorageI **this,
		size_t dataSeriesLength, size_t exactDataNum, int byteSize, 
		int* type, unsigned char* exactDataBytes, size_t exactDataBytes_size,
		double realPrecision, long minValue, int intervals, int dataType) 
{
	//int i = 0;
	*this = (TightDataPointStorageI *)malloc(sizeof(TightDataPointStorageI));
	(*this)->allSameData = 0;
	(*this)->realPrecision = realPrecision;
	(*this)->minValue = minValue;
	switch(dataType)
	{
	case SZ_INT8:
	case SZ_UINT8:
		(*this)->dataTypeSize = 1;
		break;
	case SZ_INT16:
	case SZ_UINT16:
		(*this)->dataTypeSize = 2;
		break;
	case SZ_INT32:
	case SZ_UINT32:
		(*this)->dataTypeSize = 4;
		break;
	case SZ_INT64:
	case SZ_UINT64:
		(*this)->dataTypeSize = 8;
		break;
	}

	(*this)->dataSeriesLength = dataSeriesLength;
	(*this)->exactDataNum = exactDataNum;
	(*this)->exactByteSize = byteSize;


	int stateNum = 2*intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);
	encode_withTree(huffmanTree, type, dataSeriesLength, &(*this)->typeArray, &(*this)->typeArray_size);
	SZ_ReleaseHuffman(huffmanTree);
		
	(*this)->exactDataBytes = exactDataBytes;
	(*this)->exactDataBytes_size = exactDataBytes_size;
	
	(*this)->intervals = intervals;
	
	(*this)->isLossless = 0;
}

void convertTDPStoBytes_int(TightDataPointStorageI* tdps, unsigned char* bytes, unsigned char sameByte)
{
	size_t i, k = 0;
	
	unsigned char byteBuffer[8] = {0,0,0,0,0,0,0,0};
	
	for(i = 0;i<3;i++)//3 bytes
		bytes[k++] = versionNumber[i];
	bytes[k++] = sameByte;	//1	byte
	
	convertSZParamsToBytes(confparams_cpr, &(bytes[k]));
	k = k + MetaDataByteLength;	
		
	bytes[k++] = tdps->exactByteSize; //1 byte

	sizeToBytes(byteBuffer, tdps->dataSeriesLength);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST: 4 or 8 bytes
		bytes[k++] = byteBuffer[i];	
	
	intToBytes_bigEndian(byteBuffer, confparams_cpr->max_quant_intervals);
	for(i = 0;i<4;i++)//4
		bytes[k++] = byteBuffer[i];
	
	intToBytes_bigEndian(byteBuffer, tdps->intervals);
	for(i = 0;i<4;i++)//4
		bytes[k++] = byteBuffer[i];			
	
	longToBytes_bigEndian(byteBuffer, tdps->minValue);
	for (i = 0; i < 8; i++)// 8
		bytes[k++] = byteBuffer[i];

	doubleToBytes(byteBuffer, tdps->realPrecision);
	for (i = 0; i < 8; i++)// 8
		bytes[k++] = byteBuffer[i];			

	sizeToBytes(byteBuffer, tdps->typeArray_size);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = byteBuffer[i];

	sizeToBytes(byteBuffer, tdps->exactDataNum);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = byteBuffer[i];

	sizeToBytes(byteBuffer, tdps->exactDataBytes_size);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = byteBuffer[i];

	memcpy(&(bytes[k]), tdps->typeArray, tdps->typeArray_size);
	k += tdps->typeArray_size;

	memcpy(&(bytes[k]), tdps->exactDataBytes, tdps->exactDataBytes_size);
	k += tdps->exactDataBytes_size;
}

//convert TightDataPointStorageI to bytes...
void convertTDPStoFlatBytes_int(TightDataPointStorageI *tdps, unsigned char** bytes, size_t *size)
{
	size_t i, k = 0; 
	unsigned char dsLengthBytes[8];
	
	if(exe_params->SZ_SIZE_TYPE==4)
		intToBytes_bigEndian(dsLengthBytes, tdps->dataSeriesLength);//4
	else
		longToBytes_bigEndian(dsLengthBytes, tdps->dataSeriesLength);//8

	unsigned char sameByte = tdps->allSameData==1?(unsigned char)1:(unsigned char)0;
	sameByte = sameByte | (confparams_cpr->szMode << 1);
	if(tdps->isLossless)
		sameByte = (unsigned char) (sameByte | 0x10);
	
	int dataTypeSizeCode = convertDataTypeSize(tdps->dataTypeSize);
	sameByte = (unsigned char) (sameByte | dataTypeSizeCode);
	
	if(exe_params->SZ_SIZE_TYPE==8)
		sameByte = (unsigned char) (sameByte | 0x40); // 01000000, the 6th bit
	
	if(tdps->allSameData==1)
	{
		size_t totalByteLength = 3 + 1 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + tdps->exactDataBytes_size;
		*bytes = (unsigned char *)malloc(sizeof(unsigned char)*totalByteLength);

		for (i = 0; i < 3; i++)//3
			(*bytes)[k++] = versionNumber[i];
		(*bytes)[k++] = sameByte;//1
		
		convertSZParamsToBytes(confparams_cpr, &((*bytes)[k]));
		k = k + MetaDataByteLength;			
		
		for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
			(*bytes)[k++] = dsLengthBytes[i];
		
		for (i = 0; i < tdps->exactDataBytes_size; i++)
			(*bytes)[k++] = tdps->exactDataBytes[i];

		*size = totalByteLength;
	}
	else 
	{
		if(confparams_cpr->errorBoundMode>=PW_REL)
		{			
			printf("Error: errorBoundMode >= PW_REL!! can't be...\n");
			exit(0);
		}

		size_t totalByteLength = 3 + 1 + MetaDataByteLength + 1 + exe_params->SZ_SIZE_TYPE + 4 + 4 + 8 + 8
				+ exe_params->SZ_SIZE_TYPE + exe_params->SZ_SIZE_TYPE + exe_params->SZ_SIZE_TYPE
				+ tdps->typeArray_size + tdps->exactDataBytes_size;

		*bytes = (unsigned char *)malloc(sizeof(unsigned char)*totalByteLength);

		convertTDPStoBytes_int(tdps, *bytes, sameByte);
		
		*size = totalByteLength;
	}
}

void convertTDPStoFlatBytes_int_args(TightDataPointStorageI *tdps, unsigned char* bytes, size_t *size)
{
	size_t i, k = 0; 
	unsigned char dsLengthBytes[8];
	
	if(exe_params->SZ_SIZE_TYPE==4)
		intToBytes_bigEndian(dsLengthBytes, tdps->dataSeriesLength);//4
	else
		longToBytes_bigEndian(dsLengthBytes, tdps->dataSeriesLength);//8
		
	unsigned char sameByte = tdps->allSameData==1?(unsigned char)1:(unsigned char)0;
	sameByte = sameByte | (confparams_cpr->szMode << 1);
	if(tdps->isLossless)
		sameByte = (unsigned char) (sameByte | 0x10);
	if(exe_params->SZ_SIZE_TYPE==8)
		sameByte = (unsigned char) (sameByte | 0x40); // 01000000, the 6th bit
		
	if(tdps->allSameData==1)
	{
		size_t totalByteLength = 3 + 1 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + tdps->exactDataBytes_size;
		//*bytes = (unsigned char *)malloc(sizeof(unsigned char)*totalByteLength);

		for (i = 0; i < 3; i++)//3
			bytes[k++] = versionNumber[i];
		bytes[k++] = sameByte;//1
		
		convertSZParamsToBytes(confparams_cpr, &(bytes[k]));
		k = k + MetaDataByteLength;	
				
		for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)//ST
			bytes[k++] = dsLengthBytes[i];		
		for (i = 0; i < tdps->exactDataBytes_size; i++)
			bytes[k++] = tdps->exactDataBytes[i];

		*size = totalByteLength;
	}
	else
	{
		if(confparams_cpr->errorBoundMode>=PW_REL)
		{			
			printf("Error: errorBoundMode>=PW_REL!! can't be....\n");
			exit(0);
		}

		size_t totalByteLength = 3 + 1 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 1 + 4 + 4 + 8 + 8
				+ exe_params->SZ_SIZE_TYPE + exe_params->SZ_SIZE_TYPE + exe_params->SZ_SIZE_TYPE  
				+ tdps->typeArray_size + tdps->exactDataBytes_size;

		convertTDPStoBytes_int(tdps, bytes, sameByte);
		
		*size = totalByteLength;
	}
}

void free_TightDataPointStorageI(TightDataPointStorageI *tdps)
{
	if(tdps->typeArray!=NULL)
		free(tdps->typeArray);
	if(tdps->exactDataBytes!=NULL)
		free(tdps->exactDataBytes);
	free(tdps);
}

void free_TightDataPointStorageI2(TightDataPointStorageI *tdps)
{
	free(tdps);
}


