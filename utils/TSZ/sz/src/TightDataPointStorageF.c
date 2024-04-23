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
#include "transcode.h"
#include "fse.h"

void new_TightDataPointStorageF_Empty(TightDataPointStorageF **this)
{
	TightDataPointStorageF* tdpf = (TightDataPointStorageF*)malloc(sizeof(TightDataPointStorageF));
	memset(tdpf, 0, sizeof(TightDataPointStorageF));
    *this = tdpf;
}

int new_TightDataPointStorageF_fromFlatBytes(TightDataPointStorageF **this, unsigned char* flatBytes, size_t flatBytesLength, sz_exedata* pde_exe, sz_params* pde_params)
{
	new_TightDataPointStorageF_Empty(this);
	size_t i, index = 0;

	//
	// parse tdps
	//

	// 1 version(1)
	unsigned char version = flatBytes[index++]; //1
	unsigned char sameRByte = flatBytes[index++]; //1

    // parse data format
	switch (version)
	{
	case DATA_FROMAT_VER1:
		break;
	default:
	    printf(" error, float compressed data format can not be recognised. ver=%d\n ", version);
		return SZ_ABS;
	}	
	
	// 2 same(1)														      //note that 1000,0000 is reserved for regression tag.
	int same = sameRByte & 0x01; 											//0000,0001
	(*this)->ifAdtFse = (sameRByte & 0x02)>>1; 							//0000,0110
	(*this)->isLossless = (sameRByte & 0x10)>>4; 							//0001,0000								//0010,0000
	pde_exe->SZ_SIZE_TYPE = ((sameRByte & 0x40)>>6)==1?8:4; 				//0100,0000	
	int errorBoundMode = SZ_ABS;
    // 3 meta(2)   
	convertBytesToSZParams(&(flatBytes[index]), pde_params, pde_exe);
	index += MetaDataByteLength;
    // 4 element count(4)
	unsigned char dsLengthBytes[8];
	for (i = 0; i < pde_exe->SZ_SIZE_TYPE; i++)
		dsLengthBytes[i] = flatBytes[index++];
	(*this)->dataSeriesLength = bytesToSize(dsLengthBytes, pde_exe->SZ_SIZE_TYPE);// 4 or 8		
	if((*this)->isLossless==1)
	{
		//(*this)->exactMidBytes = flatBytes+8;
		return errorBoundMode;
	}
	else if(same==1)
	{
		(*this)->allSameData = 1;
		(*this)->exactMidBytes = &(flatBytes[index]);
		return errorBoundMode;
	}
	else
		(*this)->allSameData = 0;
    // regression  
    int isRegression = (sameRByte >> 7) & 0x01;
	if(isRegression == 1)
	{
		(*this)->raBytes_size = flatBytesLength - 1 - 1 - MetaDataByteLength - pde_exe->SZ_SIZE_TYPE;
		(*this)->raBytes = &(flatBytes[index]);
		return errorBoundMode;
	}			
    // 5 quant intervals(4)   
	unsigned char byteBuf[8];
	for (i = 0; i < 4; i++)
		byteBuf[i] = flatBytes[index++];
	int max_quant_intervals = bytesToInt_bigEndian(byteBuf);// 4	
	pde_params->maxRangeRadius = max_quant_intervals/2;
    // 6 intervals
	for (i = 0; i < 4; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->intervals = bytesToInt_bigEndian(byteBuf);// 4	
    // 7 median
	for (i = 0; i < 4; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->medianValue = bytesToFloat(byteBuf); //4
	// 8 reqLength
	(*this)->reqLength = flatBytes[index++]; //1
	// 9 realPrecision(8)
	for (i = 0; i < 8; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->realPrecision = bytesToDouble(byteBuf);//8
	// 10 typeArray_size
	if ((*this)->ifAdtFse == 0) {
		for (i = 0; i < pde_exe->SZ_SIZE_TYPE; i++)
			byteBuf[i] = flatBytes[index++];
		(*this)->typeArray_size = bytesToSize(byteBuf, pde_exe->SZ_SIZE_TYPE);// 4		
	} else {
		for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
			byteBuf[i] = flatBytes[index++];
		(*this)->FseCode_size = bytesToSize(byteBuf, pde_exe->SZ_SIZE_TYPE);// 4
		for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
			byteBuf[i] = flatBytes[index++];
		(*this)->transCodeBits_size = bytesToSize(byteBuf, pde_exe->SZ_SIZE_TYPE);// 4
	}

    // 11 exactNum
	for (i = 0; i < pde_exe->SZ_SIZE_TYPE; i++)
		byteBuf[i] = flatBytes[index++];    
	(*this)->exactDataNum = bytesToSize(byteBuf, pde_exe->SZ_SIZE_TYPE);// ST
    // 12 mid size
	for (i = 0; i < pde_exe->SZ_SIZE_TYPE; i++)
		byteBuf[i] = flatBytes[index++];
	(*this)->exactMidBytes_size = bytesToSize(byteBuf, pde_exe->SZ_SIZE_TYPE);// STqq
    
	// calc leadNumArray_size
	size_t logicLeadNumBitsNum = (*this)->exactDataNum * 2;
	if (logicLeadNumBitsNum % 8 == 0)
	{
		(*this)->leadNumArray_size = logicLeadNumBitsNum >> 3;
	}
	else
	{
		(*this)->leadNumArray_size = (logicLeadNumBitsNum >> 3) + 1;
	}	

    // 13 typeArray
	if ((*this)->ifAdtFse == 0) {
		(*this)->typeArray = &flatBytes[index]; 
		//retrieve the number of states (i.e., stateNum)
		(*this)->allNodes = bytesToInt_bigEndian((*this)->typeArray); //the first 4 bytes store the stateNum
		(*this)->stateNum = ((*this)->allNodes+1)/2;
		index+=(*this)->typeArray_size;
	} else {
		(*this)->FseCode = &flatBytes[index]; 
		index+=(*this)->FseCode_size;

		(*this)->transCodeBits = &flatBytes[index]; 
		index+=(*this)->transCodeBits_size;
	}

    // 14 leadNumArray
	(*this)->leadNumArray = &flatBytes[index];
	index += (*this)->leadNumArray_size;
	// 15 exactMidBytes
	(*this)->exactMidBytes = &flatBytes[index];
	index+=(*this)->exactMidBytes_size;
	// 16 residualMidBits
	(*this)->residualMidBits = &flatBytes[index];

    // calc residualMidBits_size
	(*this)->residualMidBits_size = flatBytesLength - 1 - 1 - MetaDataByteLength - pde_exe->SZ_SIZE_TYPE - 4 - 4 - 4 - 1 - 8 
			- pde_exe->SZ_SIZE_TYPE - pde_exe->SZ_SIZE_TYPE
			- (*this)->leadNumArray_size - (*this)->exactMidBytes_size;	
	if ((*this)->ifAdtFse == 0) {
		(*this)->residualMidBits_size = (*this)->residualMidBits_size - (*this)->typeArray_size - pde_exe->SZ_SIZE_TYPE ;
	} else {
		(*this)->residualMidBits_size = (*this)->residualMidBits_size - (*this)->FseCode_size - (*this)->transCodeBits_size - pde_exe->SZ_SIZE_TYPE - pde_exe->SZ_SIZE_TYPE;
	}
	
	// printTDPS(*this);
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
		unsigned char radExpo) {
	
	*this = (TightDataPointStorageF *)malloc(sizeof(TightDataPointStorageF));
	memset(*this, 0, sizeof(TightDataPointStorageF));
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

void convertTDPStoBytes_float(TightDataPointStorageF* tdps, unsigned char* bytes, unsigned char* dsLengthBytes, unsigned char sameByte)
{
	size_t i, k = 0;
	unsigned char intervalsBytes[4];
	unsigned char typeArrayLengthBytes[8];
	unsigned char exactLengthBytes[8];
	unsigned char exactMidBytesLength[8];
	unsigned char realPrecisionBytes[8];
	unsigned char fsecodeLengthBytes[8];
	unsigned char transcodeLengthBytes[8];
	unsigned char medianValueBytes[4];
	unsigned char max_quant_intervals_Bytes[4];
	
	// 1 version
	bytes[k++] = versionNumber;
	// 2 same
	bytes[k++] = sameByte;	//1	byte
	// 3 meta
	convertSZParamsToBytes(confparams_cpr, &(bytes[k]), exe_params->optQuantMode);
	k = k + MetaDataByteLength;
	// 4 element count
	for(i = 0; i < exe_params->SZ_SIZE_TYPE; i++)//ST: 4 or 8 bytes
		bytes[k++] = dsLengthBytes[i];	
	intToBytes_bigEndian(max_quant_intervals_Bytes, confparams_cpr->max_quant_intervals);
	// 5 max_quant_intervals length
	for(i = 0;i<4;i++)//4
		bytes[k++] = max_quant_intervals_Bytes[i];			
	// 6 intervals
	intToBytes_bigEndian(intervalsBytes, tdps->intervals);
	for(i = 0;i<4;i++)//4
		bytes[k++] = intervalsBytes[i];				
	// 7 median
	floatToBytes(medianValueBytes, tdps->medianValue);
	for (i = 0; i < 4; i++)// 4
		bytes[k++] = medianValueBytes[i];		
    // 8 reqLength
	bytes[k++] = tdps->reqLength; //1 byte
    // 9 realPrecision
	doubleToBytes(realPrecisionBytes, tdps->realPrecision);
	for (i = 0; i < 8; i++)// 8
		bytes[k++] = realPrecisionBytes[i];		
   	// 10 typeArray size
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
    // 11 exactDataNum  leadNum calc by this , so not save leadNum
	sizeToBytes(exactLengthBytes, tdps->exactDataNum, exe_params->SZ_SIZE_TYPE);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = exactLengthBytes[i];
    // 12 Mid size
	sizeToBytes(exactMidBytesLength, tdps->exactMidBytes_size, exe_params->SZ_SIZE_TYPE);
	for(i = 0;i<exe_params->SZ_SIZE_TYPE;i++)//ST
		bytes[k++] = exactMidBytesLength[i];
	// 13 typeArray	
	if (tdps->ifAdtFse == 0) {
		memcpy(&(bytes[k]), tdps->typeArray, tdps->typeArray_size);
		k += tdps->typeArray_size;		
	} else {
		memcpy(&(bytes[k]), tdps->FseCode, tdps->FseCode_size);
		k += tdps->FseCode_size;
		memcpy(&(bytes[k]), tdps->transCodeBits, tdps->transCodeBits_size);
		k += tdps->transCodeBits_size;
	}
    // 14 leadNumArray_size
	memcpy(&(bytes[k]), tdps->leadNumArray, tdps->leadNumArray_size);
	k += tdps->leadNumArray_size;
	// 15 mid data
	memcpy(&(bytes[k]), tdps->exactMidBytes, tdps->exactMidBytes_size);
	k += tdps->exactMidBytes_size;	
    // 16 residualMidBits 
	if(tdps->residualMidBits!=NULL)
	{
		memcpy(&(bytes[k]), tdps->residualMidBits, tdps->residualMidBits_size);
		k += tdps->residualMidBits_size;
	}	
}

//convert TightDataPointStorageD to bytes...
bool convertTDPStoFlatBytes_float(TightDataPointStorageF *tdps, unsigned char* bytes, size_t *size)
{
	// printTDPS(tdps);
	size_t i, k = 0; 
	unsigned char dsLengthBytes[8];
	
	if(exe_params->SZ_SIZE_TYPE==4)
		intToBytes_bigEndian(dsLengthBytes, tdps->dataSeriesLength);//4
	else
		longToBytes_bigEndian(dsLengthBytes, tdps->dataSeriesLength);//8
		
	unsigned char sameByte = tdps->allSameData==1?(unsigned char)1:(unsigned char)0; //0000,0001
	//sameByte = sameByte | (confparams_cpr->szMode << 1);  //0000,0110 (no need because of convertSZParamsToBytes
	sameByte = sameByte | (tdps->ifAdtFse << 1);      // 0000,0010
	if(tdps->isLossless)
		sameByte = (unsigned char) (sameByte | 0x10); // 0001,0000
	if(confparams_cpr->errorBoundMode>=PW_REL)
		sameByte = (unsigned char) (sameByte | 0x20); // 0010,0000, the 5th bit
	if(exe_params->SZ_SIZE_TYPE==8)
		sameByte = (unsigned char) (sameByte | 0x40); // 0100,0000, the 6th bit
	if(confparams_cpr->errorBoundMode == PW_REL && confparams_cpr->accelerate_pw_rel_compression)
		sameByte = (unsigned char) (sameByte | 0x08); //0000,1000
	//if(confparams_cpr->protectValueRange)
	//	sameByte = (unsigned char) (sameByte | 0x04); //0000,0100
	
	if(tdps->allSameData == 1 )
	{
		//
		// same format
		//
		size_t totalByteLength = 1 + 1 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + tdps->exactMidBytes_size;
		//*bytes = (unsigned char *)malloc(sizeof(unsigned char)*totalByteLength); // not need malloc comment by tickduan
		// check output buffer enough
		if(totalByteLength >=  tdps->dataSeriesLength * sizeof(float) )
		{
			*size = 0;
			return false;
		}
		
		// 1 version 1 byte
	    bytes[k++] = versionNumber;
		// 2 same flag 1 bytes
		bytes[k++] = sameByte;
		// 3 metaData 26 bytes
		convertSZParamsToBytes(confparams_cpr, &(bytes[k]), exe_params->optQuantMode);
		k = k + MetaDataByteLength;
		// 4 data Length 4 or 8 bytes	
		for (i = 0; i < exe_params->SZ_SIZE_TYPE; i++)
			bytes[k++] = dsLengthBytes[i];
		// 5 exactMidBytes exactMidBytes_size bytes
		for (i = 0; i < tdps->exactMidBytes_size; i++)
			bytes[k++] = tdps->exactMidBytes[i];

		*size = totalByteLength;
	}
	else
	{
		//
		// not same format
		//
		size_t residualMidBitsLength = tdps->residualMidBits == NULL ? 0 : tdps->residualMidBits_size;

        // version(1) + samebyte(1) 
		size_t totalByteLength = 1 + 1 + MetaDataByteLength + exe_params->SZ_SIZE_TYPE + 4 + 4 + 4 + 1 + 8 
				+ exe_params->SZ_SIZE_TYPE + exe_params->SZ_SIZE_TYPE
			    + tdps->leadNumArray_size 
				+ tdps->exactMidBytes_size 
				+ residualMidBitsLength;		
		if (tdps->ifAdtFse == 0) {
			totalByteLength += tdps->typeArray_size + exe_params->SZ_SIZE_TYPE;
		} else {
			totalByteLength += tdps->FseCode_size + tdps->transCodeBits_size + exe_params->SZ_SIZE_TYPE + exe_params->SZ_SIZE_TYPE;
		}

		//*bytes = (unsigned char *)malloc(sizeof(unsigned char)*totalByteLength);  // comment by tickduan
		if(totalByteLength >= tdps->dataSeriesLength * sizeof(float))
		{
			*size = 0;
			return false;
		}

		convertTDPStoBytes_float(tdps, bytes, dsLengthBytes, sameByte);
		*size = totalByteLength;
		return true;
	}

	return true;
}

/**
 * to free the memory used in the compression
 * */
void free_TightDataPointStorageF(TightDataPointStorageF *tdps)
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
void free_TightDataPointStorageF2(TightDataPointStorageF *tdps)
{			
	free(tdps);
}
