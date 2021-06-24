/**
 *  @file TightPointDataStorageF.c
 *  @author Sheng Di and Dingwen Tao
 *  @date Aug, 2016
 *  @brief The functions used to construct the tightPointDataStorage element for storing compressed bytes.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdlib.h> 
#include <stdio.h>
#include <string.h>
#include "TightDataPointStorageF.h"
#include "sz.h"
#include "Huffman.h"
//#include "rw.h"

void new_TightDataPointStorageF_Empty(TightDataPointStorageF **this)
{
	*this = (TightDataPointStorageF*)malloc(sizeof(TightDataPointStorageF));
	(*this)->dataSeriesLength = 0;
	(*this)->allSameData = 0;
	(*this)->exactDataNum = 0;
	(*this)->reservedValue = 0;
	(*this)->reqLength = 0;
	(*this)->radExpo = 0;

	(*this)->rtypeArray = NULL;
	(*this)->rtypeArray_size = 0;

	(*this)->typeArray = NULL; //its size is dataSeriesLength/4 (or xxx/4+1) 
	(*this)->typeArray_size = 0;

	(*this)->leadNumArray = NULL; //its size is exactDataNum/4 (or exactDataNum/4+1)
	(*this)->leadNumArray_size = 0;

	(*this)->exactMidBytes = NULL;
	(*this)->exactMidBytes_size = 0;

	(*this)->residualMidBits = NULL;
	(*this)->residualMidBits_size = 0;
	
	(*this)->intervals = 0;
	(*this)->isLossless = 0;
	
	(*this)->segment_size = 0;
	(*this)->pwrErrBoundBytes = NULL;
	(*this)->pwrErrBoundBytes_size = 0;	
	
	(*this)->raBytes = NULL;
	(*this)->raBytes_size = 0;
}

int new_TightDataPointStorageF_fromFlatBytes(TightDataPointStorageF **this, unsigned char* flatBytes, size_t flatBytesLength)
{
	new_TightDataPointStorageF_Empty(this);
	size_t i, index = 0;
	size_t pwrErrBoundBytes_size = 0, segmentL = 0, radExpoL = 0, pwrErrBoundBytesL = 0;
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
															      //note that 1000,0000 is reserved for regression tag.
	int same = sameRByte & 0x01; 											//0000,0001
	(*this)->isLossless = (sameRByte & 0x10)>>4; 							//0001,0000
	int isPW_REL = (sameRByte & 0x20)>>5; 									//0010,0000
	exe_params->SZ_SIZE_TYPE = ((sameRByte & 0x40)>>6)==1?8:4; 				//0100,0000
	//confparams_dec->randomAccess = (sameRByte & 0x02) >> 1;
	//confparams_dec->szMode = (sameRByte & 0x06) >> 1;			//0000,0110 (in fact, this szMode could be removed because convertSZParamsToBytes will overwrite it)
	
	confparams_dec->protectValueRange = (sameRByte & 0x04)>>2;
	
	confparams_dec->accelerate_pw_rel_compression = (sameRByte & 0x08) >> 3;//0000,1000

	int errorBoundMode = ABS;
	if(isPW_REL)
	{
		errorBoundMode = PW_REL;
		segmentL = exe_params->SZ_SIZE_TYPE;
		pwrErrBoundBytesL = 4;
	}
	
	if(confparams_dec==NULL)
	{
		confparams_dec = (sz_params*)malloc(sizeof(sz_params));
		memset(confparams_dec, 0, sizeof(sz_params));
	}	
	convertBytesToSZParams(&(flatBytes[index]), confparams_dec);
	
	index += MetaDataByteLength;

	int isRegression = (sameRByte >> 7) & 0x01;
	
	unsigned char dsLengthBytes[8];
	for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
		dsLengthBytes[i] = flatBytes[index++];
	(*this)->dataSeriesLength = bytesToSize(dsLengthBytes);// 4 or 8	
	
	if((*this)->isLossless==1)
	{
		//(*this)->exactMidBytes = flatBytes+8;
		return errorBoundMode;
	}
	else if(same==1)
	{
		(*this)->allSameData = 1;
		//size_t exactMidBytesLength = sizeof(double);//flatBytesLength - 3 - 1 - MetaDataByteLength -exe_params->SZ_SIZE_TYPE;
		(*this)->exactMidBytes = &(flatBytes[index]);
		return errorBoundMode;
	}
	else
		(*this)->allSameData = 0;
	if(isRegression == 1)
	{
		(*this)->raBytes_size = flatBytesLength - 3 - 1 - MetaDataByteLength - exe_params->SZ_SIZE_TYPE;
		(*this)->raBytes = &(flatBytes[index]);
		return errorBoundMode;
	}			

	int rtype_ = 0;//sameRByte & 0x08;		//=00001000
	unsigned char byteBuf[8];

	for (i = 0; i < 4; i++)
		byteBuf[i] = flatBytes[index++];
	int max_quant_intervals = bytesToInt_bigEndian(byteBuf);// 4	

	confparams_dec->maxRangeRadius = max_quant_intervals/2;

	if(errorBoundMode>=PW_REL)
	{
		(*this)->radExpo = flatBytes[index++];//1
		radExpoL = 1;
		for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
			byteBuf[i] = flatBytes[index++];
		confparams_dec->segment_size = (*this)->segment_size = bytesToSize(byteBuf);// exe_params->SZ_SIZE_TYPE	

		for (i = 0; i < 4; i++)
			byteBuf[i] = flatBytes[index++];
		pwrErrBoundBytes_size = (*this)->pwrErrBoundBytes_size = bytesToInt_bigEndian(byteBuf);// 4		
	}
	else
	{
		pwrErrBoundBytes_size = 0;
		(*this)->pwrErrBoundBytes = NULL;
	}
	for (i = 0; i < 4; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->intervals = bytesToInt_bigEndian(byteBuf);// 4	

	for (i = 0; i < 4; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->medianValue = bytesToFloat(byteBuf); //4
	
	(*this)->reqLength = flatBytes[index++]; //1
	
	if(isPW_REL && confparams_dec->accelerate_pw_rel_compression)
	{
		(*this)->plus_bits = flatBytes[index++];
		(*this)->max_bits = flatBytes[index++];
	}
	
	for (i = 0; i < 8; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->realPrecision = bytesToDouble(byteBuf);//8

	for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->typeArray_size = bytesToSize(byteBuf);// 4		
	if(rtype_!=0)
	{
		for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++) 
			byteBuf[i] = flatBytes[index++];
		(*this)->rtypeArray_size = bytesToSize(byteBuf);//(ST)
	}
	else
		(*this)->rtypeArray_size = 0;

	for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->exactDataNum = bytesToSize(byteBuf);// ST

	for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->exactMidBytes_size = bytesToSize(byteBuf);// ST

	if (rtype_ != 0) {
		if((*this)->rtypeArray_size>0)
			(*this)->rtypeArray = (unsigned char*)malloc(sizeof(unsigned char)*(*this)->rtypeArray_size);
		else
			(*this)->rtypeArray = NULL;

		for (i = 0; i < 4; i++)
			byteBuf[i] = flatBytes[index++];
		(*this)->reservedValue = bytesToFloat(byteBuf);//4
	}

	size_t logicLeadNumBitsNum = (*this)->exactDataNum * 2;
	if (logicLeadNumBitsNum % 8 == 0)
	{
		(*this)->leadNumArray_size = logicLeadNumBitsNum >> 3;
	}
	else
	{
		(*this)->leadNumArray_size = (logicLeadNumBitsNum >> 3) + 1;
	}

	int minLogValueSize = 0;
	if(errorBoundMode>=PW_REL)
		minLogValueSize = 4;

	if ((*this)->rtypeArray != NULL) 
	{
		(*this)->residualMidBits_size = flatBytesLength - 3 - 1 - MetaDataByteLength - exe_params->SZ_SIZE_TYPE - 4 - radExpoL - segmentL - pwrErrBoundBytesL - 4 - 4 - 1 - 8 
				- exe_params->SZ_SIZE_TYPE - exe_params->SZ_SIZE_TYPE - exe_params->SZ_SIZE_TYPE - minLogValueSize - exe_params->SZ_SIZE_TYPE - 4 - (*this)->rtypeArray_size
				- minLogValueSize - (*this)->typeArray_size - (*this)->leadNumArray_size
				- (*this)->exactMidBytes_size - pwrErrBoundBytes_size - 1 - 1;
		for (i = 0; i < (*this)->rtypeArray_size; i++)
			(*this)->rtypeArray[i] = flatBytes[index++];
	}
	else
	{
		(*this)->residualMidBits_size = flatBytesLength - 3 - 1 - MetaDataByteLength - exe_params->SZ_SIZE_TYPE - 4 - radExpoL - segmentL - pwrErrBoundBytesL - 4 - 4 - 1 - 8 
				- exe_params->SZ_SIZE_TYPE - exe_params->SZ_SIZE_TYPE - exe_params->SZ_SIZE_TYPE - minLogValueSize - (*this)->typeArray_size
				- (*this)->leadNumArray_size - (*this)->exactMidBytes_size - pwrErrBoundBytes_size - 1 - 1;
	}

	if(errorBoundMode>=PW_REL)
	{
		(*this)->minLogValue = bytesToFloat(&flatBytes[index]);
		index+=4;
	}

	(*this)->typeArray = &flatBytes[index]; 
	//retrieve the number of states (i.e., stateNum)
	(*this)->allNodes = bytesToInt_bigEndian((*this)->typeArray); //the first 4 bytes store the stateNum
	(*this)->stateNum = ((*this)->allNodes+1)/2;	

	index+=(*this)->typeArray_size;
	
	(*this)->pwrErrBoundBytes = &flatBytes[index];
	
	index+=pwrErrBoundBytes_size;
	
	(*this)->leadNumArray = &flatBytes[index];
	
	index+=(*this)->leadNumArray_size;
	
	(*this)->exactMidBytes = &flatBytes[index];
	
	index+=(*this)->exactMidBytes_size;
	
	(*this)->residualMidBits = &flatBytes[index];
	
	//index+=(*this)->residualMidBits_size;
	
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
void new_TightDataPointStorageF(TightDataPointStorageF **this,
		size_t dataSeriesLength, size_t exactDataNum, 
		int* type, unsigned char* exactMidBytes, size_t exactMidBytes_size,
		unsigned char* leadNumIntArray,  //leadNumIntArray contains readable numbers....
		unsigned char* resiMidBits, size_t resiMidBits_size,
		unsigned char resiBitLength, 
		double realPrecision, float medianValue, char reqLength, unsigned int intervals, 
		unsigned char* pwrErrBoundBytes, size_t pwrErrBoundBytes_size, unsigned char radExpo) {
	
	*this = (TightDataPointStorageF *)malloc(sizeof(TightDataPointStorageF));
	(*this)->allSameData = 0;
	(*this)->realPrecision = realPrecision;
	(*this)->medianValue = medianValue;
	(*this)->reqLength = reqLength;

	(*this)->dataSeriesLength = dataSeriesLength;
	(*this)->exactDataNum = exactDataNum;

	(*this)->rtypeArray = NULL;
	(*this)->rtypeArray_size = 0;

	int stateNum = 2*intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);
	if(confparams_cpr->errorBoundMode == PW_REL && confparams_cpr->accelerate_pw_rel_compression)
		(*this)->max_bits = encode_withTree_MSST19(huffmanTree, type, dataSeriesLength, &(*this)->typeArray, &(*this)->typeArray_size);
	else
		encode_withTree(huffmanTree, type, dataSeriesLength, &(*this)->typeArray, &(*this)->typeArray_size);
	SZ_ReleaseHuffman(huffmanTree);
		
	(*this)->exactMidBytes = exactMidBytes;
	(*this)->exactMidBytes_size = exactMidBytes_size;

	(*this)->leadNumArray_size = convertIntArray2ByteArray_fast_2b(leadNumIntArray, exactDataNum, &((*this)->leadNumArray));

	(*this)->residualMidBits_size = convertIntArray2ByteArray_fast_dynamic(resiMidBits, resiBitLength, exactDataNum, &((*this)->residualMidBits));
	
	(*this)->intervals = intervals;
	
	(*this)->isLossless = 0;
	
	if(confparams_cpr->errorBoundMode>=PW_REL)
		(*this)->pwrErrBoundBytes = pwrErrBoundBytes;
	else
		(*this)->pwrErrBoundBytes = NULL;
		
	(*this)->radExpo = radExpo;
	
	(*this)->pwrErrBoundBytes_size = pwrErrBoundBytes_size;
}

void new_TightDataPointStorageF2(TightDataPointStorageF **this,
		size_t dataSeriesLength, size_t exactDataNum, 
		int* type, unsigned char* exactMidBytes, size_t exactMidBytes_size,
		unsigned char* leadNumIntArray,  //leadNumIntArray contains readable numbers....
		unsigned char* resiMidBits, size_t resiMidBits_size,
		unsigned char* resiBitLength, size_t resiBitLengthSize, 
		double realPrecision, float medianValue, char reqLength, unsigned int intervals, 
		unsigned char* pwrErrBoundBytes, size_t pwrErrBoundBytes_size, unsigned char radExpo) {
	//int i = 0;
	*this = (TightDataPointStorageF *)malloc(sizeof(TightDataPointStorageF));
	(*this)->allSameData = 0;
	(*this)->realPrecision = realPrecision;
	(*this)->medianValue = medianValue;
	(*this)->reqLength = reqLength;

	(*this)->dataSeriesLength = dataSeriesLength;
	(*this)->exactDataNum = exactDataNum;

	(*this)->rtypeArray = NULL;
	(*this)->rtypeArray_size = 0;

	int stateNum = 2*intervals;
	HuffmanTree* huffmanTree = createHuffmanTree(stateNum);
	encode_withTree(huffmanTree, type, dataSeriesLength, &(*this)->typeArray, &(*this)->typeArray_size);
	SZ_ReleaseHuffman(huffmanTree);
	
	(*this)->exactMidBytes = exactMidBytes;
	(*this)->exactMidBytes_size = exactMidBytes_size;

	(*this)->leadNumArray_size = convertIntArray2ByteArray_fast_2b(leadNumIntArray, exactDataNum, &((*this)->leadNumArray));

	//(*this)->residualMidBits = resiMidBits;
	//(*this)->residualMidBits_size = resiMidBits_size;

	(*this)->residualMidBits_size = convertIntArray2ByteArray_fast_dynamic2(resiMidBits, resiBitLength, resiBitLengthSize, &((*this)->residualMidBits));
	
	(*this)->intervals = intervals;
	
	(*this)->isLossless = 0;
	
	if(confparams_cpr->errorBoundMode>=PW_REL)
		(*this)->pwrErrBoundBytes = pwrErrBoundBytes;
	else
		(*this)->pwrErrBoundBytes = NULL;
		
	(*this)->radExpo = radExpo;
	
	(*this)->pwrErrBoundBytes_size = pwrErrBoundBytes_size;
}

void convertTDPStoBytes_float(TightDataPointStorageF* tdps, unsigned char* bytes, unsigned char* dsLengthBytes, unsigned char sameByte)
{
	size_t i, k = 0;
	unsigned char intervalsBytes[4];
	unsigned char typeArrayLengthBytes[8];
	unsigned char exactLengthBytes[8];
	unsigned char exactMidBytesLength[8];
	unsigned char realPrecisionBytes[8];
	
	unsigned char medianValueBytes[4];
	
	unsigned char segment_sizeBytes[8];
	unsigned char pwrErrBoundBytes_sizeBytes[4];
	unsigned char max_quant_intervals_Bytes[4];
	
	
	for(i = 0;i<3;i++)//3 bytes
		bytes[k++] = versionNumber[i];
	bytes[k++] = sameByte;	//1	byte
	
	convertSZParamsToBytes(confparams_cpr, &(bytes[k]));
	k = k + MetaDataByteLength;
	
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST: 4 or 8 bytes
		bytes[k++] = dsLengthBytes[i];	
	intToBytes_bigEndian(max_quant_intervals_Bytes, confparams_cpr->max_quant_intervals);
	for(i = 0;i<4;i++)//4
		bytes[k++] = max_quant_intervals_Bytes[i];		
	
	if(confparams_cpr->errorBoundMode>=PW_REL)
	{
		bytes[k++] = tdps->radExpo; //1 byte			
		
		sizeToBytes(segment_sizeBytes, confparams_cpr->segment_size);
		for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
			bytes[k++] = segment_sizeBytes[i];				
			
		intToBytes_bigEndian(pwrErrBoundBytes_sizeBytes, tdps->pwrErrBoundBytes_size);
		for(i = 0;i<4;i++)//4
			bytes[k++] = pwrErrBoundBytes_sizeBytes[i];					
	}
	
	intToBytes_bigEndian(intervalsBytes, tdps->intervals);
	for(i = 0;i<4;i++)//4
		bytes[k++] = intervalsBytes[i];			
	
	floatToBytes(medianValueBytes, tdps->medianValue);
	for (i = 0; i < 4; i++)// 4
		bytes[k++] = medianValueBytes[i];		

	bytes[k++] = tdps->reqLength; //1 byte

	if(confparams_cpr->errorBoundMode == PW_REL && confparams_cpr->accelerate_pw_rel_compression)
	{
		bytes[k++] = tdps->plus_bits;
		bytes[k++] = tdps->max_bits;
	}

	doubleToBytes(realPrecisionBytes, tdps->realPrecision);

	for (i = 0; i < 8; i++)// 8
		bytes[k++] = realPrecisionBytes[i];			

	sizeToBytes(typeArrayLengthBytes, tdps->typeArray_size);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = typeArrayLengthBytes[i];

	sizeToBytes(exactLengthBytes, tdps->exactDataNum);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = exactLengthBytes[i];

	sizeToBytes(exactMidBytesLength, tdps->exactMidBytes_size);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = exactMidBytesLength[i];

	if(confparams_cpr->errorBoundMode>=PW_REL)
	{
		floatToBytes(exactMidBytesLength, tdps->minLogValue);
		for(i=0;i<4;i++)
			bytes[k++] = exactMidBytesLength[i];
	}

	memcpy(&(bytes[k]), tdps->typeArray, tdps->typeArray_size);
	k += tdps->typeArray_size;
	if(confparams_cpr->errorBoundMode>=PW_REL)
	{
		memcpy(&(bytes[k]), tdps->pwrErrBoundBytes, tdps->pwrErrBoundBytes_size);
		k += tdps->pwrErrBoundBytes_size;
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

/*deprecated*/
void convertTDPStoBytes_float_reserve(TightDataPointStorageF* tdps, unsigned char* bytes, unsigned char* dsLengthBytes, unsigned char sameByte)
{
	size_t i, k = 0;
	unsigned char intervalsBytes[4];
	unsigned char typeArrayLengthBytes[8];
	unsigned char rTypeLengthBytes[8];
	unsigned char exactLengthBytes[8];
	unsigned char exactMidBytesLength[8];
	unsigned char realPrecisionBytes[8];
	unsigned char reservedValueBytes[4];
	
	unsigned char medianValueBytes[4];
	
	unsigned char segment_sizeBytes[8];
	unsigned char pwrErrBoundBytes_sizeBytes[4];
	unsigned char max_quant_intervals_Bytes[4];	
	
	for(i = 0;i<3;i++)//3
		bytes[k++] = versionNumber[i];		
	bytes[k++] = sameByte;			//1

	convertSZParamsToBytes(confparams_cpr, &(bytes[k]));
	k = k + MetaDataByteLength;
	
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = dsLengthBytes[i];		


	intToBytes_bigEndian(max_quant_intervals_Bytes, confparams_cpr->max_quant_intervals);
	for(i = 0;i<4;i++)//4
		bytes[k++] = max_quant_intervals_Bytes[i];

	if(confparams_cpr->errorBoundMode>=PW_REL)
	{
		bytes[k++] = tdps->radExpo; //1 byte			
		
		sizeToBytes(segment_sizeBytes, confparams_cpr->segment_size);
		for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
			bytes[k++] = segment_sizeBytes[i];				
			
		intToBytes_bigEndian(pwrErrBoundBytes_sizeBytes, tdps->pwrErrBoundBytes_size);
		for(i = 0;i<4;i++)//4
			bytes[k++] = pwrErrBoundBytes_sizeBytes[i];					
	}
	
	intToBytes_bigEndian(intervalsBytes, tdps->intervals);
	for(i = 0;i<4;i++)//4
		bytes[k++] = intervalsBytes[i];	

	floatToBytes(medianValueBytes, tdps->medianValue);
	for (i = 0; i < 4; i++)// 4
		bytes[k++] = medianValueBytes[i];		

	bytes[k++] = tdps->reqLength; //1 byte

	floatToBytes(realPrecisionBytes, tdps->realPrecision);
	for (i = 0; i < 8; i++)// 8
		bytes[k++] = realPrecisionBytes[i];

	sizeToBytes(typeArrayLengthBytes, tdps->typeArray_size);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = typeArrayLengthBytes[i];

	sizeToBytes(rTypeLengthBytes, tdps->rtypeArray_size);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = rTypeLengthBytes[i];

	sizeToBytes(exactLengthBytes, tdps->exactDataNum);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = exactLengthBytes[i];

	sizeToBytes(exactMidBytesLength, tdps->exactMidBytes_size);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = exactMidBytesLength[i];

	floatToBytes(reservedValueBytes, tdps->reservedValue);
	for (i = 0; i < 4; i++)// 4
		bytes[k++] = reservedValueBytes[i];

	memcpy(&(bytes[k]), tdps->rtypeArray, tdps->rtypeArray_size);
	k += tdps->rtypeArray_size;
	
	if(confparams_cpr->errorBoundMode>=PW_REL)
	{
		floatToBytes(exactMidBytesLength, tdps->minLogValue);
		for(i=0;i<4;i++)
			bytes[k++] = exactMidBytesLength[i];
	}	
	
	memcpy(&(bytes[k]), tdps->typeArray, tdps->typeArray_size);
	k += tdps->typeArray_size;
	if(confparams_cpr->errorBoundMode>=PW_REL)
	{
		memcpy(&(bytes[k]), tdps->pwrErrBoundBytes, tdps->pwrErrBoundBytes_size);
		k += tdps->pwrErrBoundBytes_size;
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

//convert TightDataPointStorageD to bytes...
void convertTDPStoFlatBytes_float(TightDataPointStorageF *tdps, unsigned char** bytes, size_t *size)
{
	size_t i, k = 0; 
	unsigned char dsLengthBytes[8];
	
	if(exe_params->SZ_SIZE_TYPE==4)
		intToBytes_bigEndian(dsLengthBytes, tdps->dataSeriesLength);//4
	else
		longToBytes_bigEndian(dsLengthBytes, tdps->dataSeriesLength);//8
		
	unsigned char sameByte = tdps->allSameData==1?(unsigned char)1:(unsigned char)0; //0000,0001
	//sameByte = sameByte | (confparams_cpr->szMode << 1);  //0000,0110 (no need because of convertSZParamsToBytes
	if(tdps->isLossless)
		sameByte = (unsigned char) (sameByte | 0x10);  // 0001,0000
	if(confparams_cpr->errorBoundMode>=PW_REL)
		sameByte = (unsigned char) (sameByte | 0x20); // 0010,0000, the 5th bit
	if(exe_params->SZ_SIZE_TYPE==8)
		sameByte = (unsigned char) (sameByte | 0x40); // 0100,0000, the 6th bit
	if(confparams_cpr->errorBoundMode == PW_REL && confparams_cpr->accelerate_pw_rel_compression)
		sameByte = (unsigned char) (sameByte | 0x08); //0000,1000
	if(confparams_cpr->protectValueRange)
		sameByte = (unsigned char) (sameByte | 0x04); //0000,0100
	
	if(tdps->allSameData==1)
	{
		size_t totalByteLength = 3 + 1 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + tdps->exactMidBytes_size;
		*bytes = (unsigned char *)malloc(sizeof(unsigned char)*totalByteLength);

		for (i = 0; i < 3; i++)//3
			(*bytes)[k++] = versionNumber[i];
		(*bytes)[k++] = sameByte;
		
		convertSZParamsToBytes(confparams_cpr, &((*bytes)[k]));
		k = k + MetaDataByteLength;
				
		for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
			(*bytes)[k++] = dsLengthBytes[i];
		
		for (i = 0; i < tdps->exactMidBytes_size; i++)
			(*bytes)[k++] = tdps->exactMidBytes[i];

		*size = totalByteLength;
	}
	else if (tdps->rtypeArray == NULL)
	{
		size_t residualMidBitsLength = tdps->residualMidBits == NULL ? 0 : tdps->residualMidBits_size;
		size_t segmentL = 0, radExpoL = 0, pwrBoundArrayL = 0;
		int minLogValueSize = 0;
		if(confparams_cpr->errorBoundMode>=PW_REL)
		{			
			segmentL = exe_params->SZ_SIZE_TYPE;
			radExpoL = 1;
			pwrBoundArrayL = 4;
			minLogValueSize = 4;
		}

		size_t totalByteLength = 3 + 1 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 4 + radExpoL + segmentL + pwrBoundArrayL + 4 + 4 + 1 + 8 
				+ exe_params->SZ_SIZE_TYPE + exe_params->SZ_SIZE_TYPE + exe_params->SZ_SIZE_TYPE + minLogValueSize
				+ tdps->typeArray_size + tdps->leadNumArray_size 
				+ tdps->exactMidBytes_size + residualMidBitsLength + tdps->pwrErrBoundBytes_size;
		if(confparams_cpr->errorBoundMode == PW_REL && confparams_cpr->accelerate_pw_rel_compression)
			totalByteLength += (1+1); // for MSST19

		*bytes = (unsigned char *)malloc(sizeof(unsigned char)*totalByteLength);

		convertTDPStoBytes_float(tdps, *bytes, dsLengthBytes, sameByte);
		
		*size = totalByteLength;
	}
	else //the case with reserved value
	{
		//TODO
	}
}

void convertTDPStoFlatBytes_float_args(TightDataPointStorageF *tdps, unsigned char* bytes, size_t *size)
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
	if(confparams_cpr->errorBoundMode>=PW_REL)
		sameByte = (unsigned char) (sameByte | 0x20); // 00100000, the 5th bit
	if(exe_params->SZ_SIZE_TYPE==8)
		sameByte = (unsigned char) (sameByte | 0x40); // 01000000, the 6th bit
	if(confparams_cpr->errorBoundMode == PW_REL && confparams_cpr->accelerate_pw_rel_compression)
		sameByte = (unsigned char) (sameByte | 0x08); 	
				
	if(tdps->allSameData==1)
	{
		size_t totalByteLength = 3 + 1 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + tdps->exactMidBytes_size;
		//*bytes = (unsigned char *)malloc(sizeof(unsigned char)*totalByteLength);

		for (i = 0; i < 3; i++)//3
			bytes[k++] = versionNumber[i];
		bytes[k++] = sameByte;

		convertSZParamsToBytes(confparams_cpr, &(bytes[k]));
		k = k + MetaDataByteLength;

		for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
			bytes[k++] = dsLengthBytes[i];		
		for (i = 0; i < tdps->exactMidBytes_size; i++)
			bytes[k++] = tdps->exactMidBytes[i];

		*size = totalByteLength;
	}
	else if (tdps->rtypeArray == NULL)
	{
		size_t residualMidBitsLength = tdps->residualMidBits == NULL ? 0 : tdps->residualMidBits_size;
		size_t segmentL = 0, radExpoL = 0, pwrBoundArrayL = 0;
		if(confparams_cpr->errorBoundMode>=PW_REL)
		{			
			segmentL = exe_params->SZ_SIZE_TYPE;
			radExpoL = 1;
			pwrBoundArrayL = 4;
		}

		size_t totalByteLength = 3 + 1 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 4 + radExpoL + segmentL + pwrBoundArrayL + 4 + 4 + 1 + 8 
				+ exe_params->SZ_SIZE_TYPE + exe_params->SZ_SIZE_TYPE + exe_params->SZ_SIZE_TYPE  
				+ tdps->typeArray_size + tdps->leadNumArray_size 
				+ tdps->exactMidBytes_size + residualMidBitsLength + tdps->pwrErrBoundBytes_size;
		if(confparams_cpr->errorBoundMode == PW_REL && confparams_cpr->accelerate_pw_rel_compression)
			totalByteLength += (1+1); // for MSST19
		convertTDPStoBytes_float(tdps, bytes, dsLengthBytes, sameByte);
		
		*size = totalByteLength;
	}
	else //the case with reserved value
	{
		//TODO
	}
}

/**
 * to free the memory used in the compression
 * */
void free_TightDataPointStorageF(TightDataPointStorageF *tdps)
{
	if(tdps->rtypeArray!=NULL)
		free(tdps->rtypeArray);
	if(tdps->typeArray!=NULL)
		free(tdps->typeArray);
	if(tdps->leadNumArray!=NULL)
		free(tdps->leadNumArray);
	if(tdps->exactMidBytes!=NULL)
		free(tdps->exactMidBytes);
	if(tdps->residualMidBits!=NULL)
		free(tdps->residualMidBits);
	if(tdps->pwrErrBoundBytes!=NULL)
		free(tdps->pwrErrBoundBytes);
	free(tdps);
}

/**
 * to free the memory used in the decompression
 * */
void free_TightDataPointStorageF2(TightDataPointStorageF *tdps)
{			
	free(tdps);
}
