/**
 *  @file sz.c
 *  @author Sheng Di and Dingwen Tao
 *  @date Aug, 2016
 *  @brief SZ_Init, Compression and Decompression functions
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "sz.h"
#include "CompressElement.h"
#include "DynamicByteArray.h"
#include "DynamicIntArray.h"
#include "TightDataPointStorageD.h"
#include "TightDataPointStorageF.h"
#include "zlib.h"
#include "rw.h"
#include "Huffman.h"
#include "conf.h"
#include "utility.h"
#include "exafelSZ.h"
//#include "CurveFillingCompressStorage.h"

int versionNumber[4] = {SZ_VER_MAJOR,SZ_VER_MINOR,SZ_VER_BUILD,SZ_VER_REVISION};
//int SZ_SIZE_TYPE = 8;

int dataEndianType = LITTLE_ENDIAN_DATA; //*endian type of the data read from disk
int sysEndianType; //*sysEndianType is actually set automatically.

//the confparams should be separate between compression and decopmression, in case of mutual-affection when calling compression/decompression alternatively
sz_params *confparams_cpr = NULL; //used for compression
sz_params *confparams_dec = NULL; //used for decompression 

sz_exedata *exe_params = NULL;

/*following global variables are desgined for time-series based compression*/
/*sz_varset is not used in the single-snapshot data compression*/
SZ_VarSet* sz_varset = NULL;
sz_multisteps *multisteps = NULL;
sz_tsc_metadata *sz_tsc = NULL;

//only for Pastri compressor
#ifdef PASTRI
pastri_params pastri_par;
#endif

HuffmanTree* SZ_Reset()
{
	return createDefaultHuffmanTree();
}

int SZ_Init(const char *configFilePath)
{
	int loadFileResult = SZ_LoadConf(configFilePath);
	if(loadFileResult==SZ_NSCS)
		return SZ_NSCS;
	
	exe_params->SZ_SIZE_TYPE = sizeof(size_t);
	
	if(confparams_cpr->szMode == SZ_TEMPORAL_COMPRESSION)
	{
		initSZ_TSC();
	}
	return SZ_SCES;
}

int SZ_Init_Params(sz_params *params)
{
	SZ_Init(NULL);

	if(params->losslessCompressor!=GZIP_COMPRESSOR && params->losslessCompressor!=ZSTD_COMPRESSOR)
		params->losslessCompressor = ZSTD_COMPRESSOR;

	if(params->max_quant_intervals > 0)
		params->maxRangeRadius = params->max_quant_intervals/2;
		
	memcpy(confparams_cpr, params, sizeof(sz_params));

	if(params->quantization_intervals%2!=0)
	{
		printf("Error: quantization_intervals must be an even number!\n");
		return SZ_NSCS;
	}

	return SZ_SCES;
}

int computeDimension(size_t r5, size_t r4, size_t r3, size_t r2, size_t r1)
{
	int dimension;
	if(r1==0) 
	{
		dimension = 0;
	}
	else if(r2==0) 
	{
		dimension = 1;
	}
	else if(r3==0) 
	{
		dimension = 2;
	}
	else if(r4==0) 
	{
		dimension = 3;
	}
	else if(r5==0) 
	{
		dimension = 4;
	}
	else 
	{
		dimension = 5;
	}
	return dimension;	
}

size_t computeDataLength(size_t r5, size_t r4, size_t r3, size_t r2, size_t r1)
{
	size_t dataLength;
	if(r1==0) 
	{
		dataLength = 0;
	}
	else if(r2==0) 
	{
		dataLength = r1;
	}
	else if(r3==0) 
	{
		dataLength = r1*r2;
	}
	else if(r4==0) 
	{
		dataLength = r1*r2*r3;
	}
	else if(r5==0) 
	{
		dataLength = r1*r2*r3*r4;
	}
	else 
	{
		dataLength = r1*r2*r3*r4*r5;
	}
	return dataLength;
}

/*-------------------------------------------------------------------------*/
/**
    @brief      Perform Compression 
    @param      data           data to be compressed
    @param      outSize        the size (in bytes) after compression
    @param		r5,r4,r3,r2,r1	the sizes of each dimension (supporting only 5 dimensions at most in this version.
    @return     compressed data (in binary stream) or NULL(0) if any errors

 **/
/*-------------------------------------------------------------------------*/
unsigned char* SZ_compress_args(int dataType, void *data, size_t *outSize, int errBoundMode, double absErrBound, 
double relBoundRatio, double pwrBoundRatio, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1)
{
	if(confparams_cpr == NULL)
		SZ_Init(NULL);
	else if(exe_params == NULL)
	{
		exe_params = (sz_exedata*)malloc(sizeof(sz_exedata));
		memset(exe_params, 0, sizeof(sz_exedata));
	}
	if(exe_params->intvCapacity == 0)
	{
		exe_params->intvCapacity = confparams_cpr->maxRangeRadius*2;
		exe_params->intvRadius = confparams_cpr->maxRangeRadius;
		exe_params->optQuantMode = 1;		
	}
	
	confparams_cpr->dataType = dataType;
	if(dataType==SZ_FLOAT)
	{
		unsigned char *newByteData = NULL;
		
		SZ_compress_args_float(-1, confparams_cpr->withRegression, &newByteData, (float *)data, r5, r4, r3, r2, r1, 
		outSize, errBoundMode, absErrBound, relBoundRatio, pwrBoundRatio);
		
		return newByteData;
	}
	else if(dataType==SZ_DOUBLE)
	{
		unsigned char *newByteData;
		SZ_compress_args_double(-1, confparams_cpr->withRegression, &newByteData, (double *)data, r5, r4, r3, r2, r1, 
		outSize, errBoundMode, absErrBound, relBoundRatio, pwrBoundRatio);
		
		return newByteData;
	}
	else
	{
		printf("Error: dataType can only be SZ_FLOAT, SZ_DOUBLE, SZ_INT8/16/32/64 or SZ_UINT8/16/32/64.\n");
		return NULL;
	}
}

int SZ_compress_args2(int dataType, void *data, unsigned char* compressed_bytes, size_t *outSize, 
int errBoundMode, double absErrBound, double relBoundRatio, double pwrBoundRatio, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1)
{
	unsigned char* bytes = SZ_compress_args(dataType, data, outSize, errBoundMode, absErrBound, relBoundRatio, pwrBoundRatio, r5, r4, r3, r2, r1);
    memcpy(compressed_bytes, bytes, *outSize);
    free(bytes); 
	return SZ_SCES;
}

int SZ_compress_args3(int dataType, void *data, unsigned char* compressed_bytes, size_t *outSize, int errBoundMode, double absErrBound, double relBoundRatio, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1,
size_t s5, size_t s4, size_t s3, size_t s2, size_t s1,
size_t e5, size_t e4, size_t e3, size_t e2, size_t e1)
{
	confparams_cpr->dataType = dataType;
	if(dataType==SZ_FLOAT)
	{
		SZ_compress_args_float_subblock(compressed_bytes, (float *)data, 
		r5, r4, r3, r2, r1,
		s5, s4, s3, s2, s1,
		e5, e4, e3, e2, e1,
		outSize, errBoundMode, absErrBound, relBoundRatio);
		
		return SZ_SCES;
	}
	else if(dataType==SZ_DOUBLE)
	{
		SZ_compress_args_double_subblock(compressed_bytes, (double *)data, 
		r5, r4, r3, r2, r1,
		s5, s4, s3, s2, s1,
		e5, e4, e3, e2, e1,
		outSize, errBoundMode, absErrBound, relBoundRatio);
		
		return SZ_SCES;
	}
	else
	{
		printf("Error (in SZ_compress_args3): dataType can only be SZ_FLOAT or SZ_DOUBLE.\n");
		return SZ_NSCS;
	}	
}

unsigned char *SZ_compress(int dataType, void *data, size_t *outSize, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1)
{	
	unsigned char *newByteData = SZ_compress_args(dataType, data, outSize, confparams_cpr->errorBoundMode, confparams_cpr->absErrBound, confparams_cpr->relBoundRatio, 
	confparams_cpr->pw_relBoundRatio, r5, r4, r3, r2, r1);
	return newByteData;
}

//////////////////
/*-------------------------------------------------------------------------*/
/**
    @brief      Perform Compression 
    @param      data           data to be compressed
    @param		reservedValue  the reserved value
    @param      outSize        the size (in bytes) after compression
    @param		r5,r4,r3,r2,r1	the sizes of each dimension (supporting only 5 dimensions at most in this version.
    @return     compressed data (in binary stream)

 **/
/*-------------------------------------------------------------------------*/
unsigned char *SZ_compress_rev_args(int dataType, void *data, void *reservedValue, size_t *outSize, int errBoundMode, double absErrBound, double relBoundRatio, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1)
{
	unsigned char *newByteData;
	//TODO
	printf("SZ compression with reserved data is TO BE DONE LATER.\n");
	exit(0);
	
	return newByteData;	
}

int SZ_compress_rev_args2(int dataType, void *data, void *reservedValue, unsigned char* compressed_bytes, size_t *outSize, int errBoundMode, double absErrBound, double relBoundRatio, 
size_t r5, size_t r4, size_t r3, size_t r2, size_t r1)
{
	confparams_cpr->dataType = dataType;
	unsigned char* bytes = SZ_compress_rev_args(dataType, data, reservedValue, outSize, errBoundMode, absErrBound, relBoundRatio, r5, r4, r3, r2, r1);
	memcpy(compressed_bytes, bytes, *outSize);
	free(bytes); //free(bytes) is removed , because of dump error at MIRA system (PPC architecture), fixed?
	return 0;
}

unsigned char *SZ_compress_rev(int dataType, void *data, void *reservedValue, size_t *outSize, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1)
{
	unsigned char *newByteData;
	//TODO
	printf("SZ compression with reserved data is TO BE DONE LATER.\n");
	exit(0);
	
	return newByteData;
}

void *SZ_decompress(int dataType, unsigned char *bytes, size_t byteLength, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1)
{
	sz_exedata de_exe;
	memset(&de_exe, 0, sizeof(sz_exedata));
	de_exe.SZ_SIZE_TYPE = 8;

	sz_params  de_params;
	memset(&de_params, 0, sizeof(sz_params));
	
	
	int x = 1;
	char *y = (char*)&x;
	if(*y==1)
		sysEndianType = LITTLE_ENDIAN_SYSTEM;
	else //=0
		sysEndianType = BIG_ENDIAN_SYSTEM;
	
	if(dataType == SZ_FLOAT)
	{
		float *newFloatData;
		SZ_decompress_args_float(&newFloatData, r5, r4, r3, r2, r1, bytes, byteLength, 0, NULL, &de_exe, &de_params);
		return newFloatData;	
	}
	else if(dataType == SZ_DOUBLE)
	{
		double *newDoubleData;
		SZ_decompress_args_double(&newDoubleData, r5, r4, r3, r2, r1, bytes, byteLength, 0, NULL, &de_exe, &de_params);
		return newDoubleData;	
	}
	else 
	{
		printf("Error: data type cannot be the types other than SZ_FLOAT or SZ_DOUBLE\n");
		return NULL;	
	}
}

/**
 * 
 * 
 * return number of elements or -1 if any errors
 * */
size_t SZ_decompress_args(int dataType, unsigned char *bytes, size_t byteLength, void* decompressed_array, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1)
{
	//size_t i;
	size_t nbEle = computeDataLength(r5,r4,r3,r2,r1);
	
	if(dataType == SZ_FLOAT)
	{
		float* data = (float *)SZ_decompress(dataType, bytes, byteLength, r5, r4, r3, r2, r1);
		float* data_array = (float *)decompressed_array;
		memcpy(data_array, data, nbEle*sizeof(float));
		//for(i=0;i<nbEle;i++)
		//	data_array[i] = data[i];	
		free(data); //this free operation seems to not work with BlueG/Q system.	
	}
	else if (dataType == SZ_DOUBLE)
	{
		double* data = (double *)SZ_decompress(dataType, bytes, byteLength, r5, r4, r3, r2, r1);
		double* data_array = (double *)decompressed_array;
		memcpy(data_array, data, nbEle*sizeof(double));
		//for(i=0;i<nbEle;i++)
		//	data_array[i] = data[i];
		free(data); //this free operation seems to not work with BlueG/Q system.	
	}
	else if(dataType == SZ_INT8)
	{
		int8_t* data = (int8_t*)SZ_decompress(dataType, bytes, byteLength, r5, r4, r3, r2, r1);
		int8_t* data_array = (int8_t *)decompressed_array;
		memcpy(data_array, data, nbEle*sizeof(int8_t));
		free(data);
	}
	else if(dataType == SZ_INT16)
	{
		int16_t* data = (int16_t*)SZ_decompress(dataType, bytes, byteLength, r5, r4, r3, r2, r1);
		int16_t* data_array = (int16_t *)decompressed_array;
		memcpy(data_array, data, nbEle*sizeof(int16_t));
		free(data);	
	}
	else if(dataType == SZ_INT32)
	{
		int32_t* data = (int32_t*)SZ_decompress(dataType, bytes, byteLength, r5, r4, r3, r2, r1);
		int32_t* data_array = (int32_t *)decompressed_array;
		memcpy(data_array, data, nbEle*sizeof(int32_t));
		free(data);	
	}
	else if(dataType == SZ_INT64)
	{
		int64_t* data = (int64_t*)SZ_decompress(dataType, bytes, byteLength, r5, r4, r3, r2, r1);
		int64_t* data_array = (int64_t *)decompressed_array;
		memcpy(data_array, data, nbEle*sizeof(int64_t));
		free(data);		
	}
	else if(dataType == SZ_UINT8)
	{
		uint8_t* data = (uint8_t*)SZ_decompress(dataType, bytes, byteLength, r5, r4, r3, r2, r1);
		uint8_t* data_array = (uint8_t *)decompressed_array;
		memcpy(data_array, data, nbEle*sizeof(uint8_t));
		free(data);
	}
	else if(dataType == SZ_UINT16)
	{
		uint16_t* data = (uint16_t*)SZ_decompress(dataType, bytes, byteLength, r5, r4, r3, r2, r1);
		uint16_t* data_array = (uint16_t *)decompressed_array;
		memcpy(data_array, data, nbEle*sizeof(uint16_t));
		free(data);		
	}
	else if(dataType == SZ_UINT32)
	{
		uint32_t* data = (uint32_t*)SZ_decompress(dataType, bytes, byteLength, r5, r4, r3, r2, r1);
		uint32_t* data_array = (uint32_t *)decompressed_array;
		memcpy(data_array, data, nbEle*sizeof(uint32_t));
		free(data);		
	}
	else if(dataType == SZ_UINT64)
	{
		uint64_t* data = (uint64_t*)SZ_decompress(dataType, bytes, byteLength, r5, r4, r3, r2, r1);
		uint64_t* data_array = (uint64_t *)decompressed_array;
		memcpy(data_array, data, nbEle*sizeof(uint64_t));
		free(data);			
	}
	else
	{ 
		printf("Error: data type cannot be the types other than SZ_FLOAT or SZ_DOUBLE\n");
		return SZ_NSCS; //indicating error		
	}

	return nbEle;
}


sz_metadata* SZ_getMetadata(unsigned char* bytes, sz_exedata* pde_exe)
{
	int index = 0, i, isConstant, isLossless;
	size_t dataSeriesLength = 0;
	int versions[3] = {0,0,0};
	for (i = 0; i < 3; i++)
		versions[i] = bytes[index++]; //3
	unsigned char sameRByte = bytes[index++]; //1
	isConstant = sameRByte & 0x01;
	//confparams_dec->szMode = (sameRByte & 0x06)>>1;
	isLossless = (sameRByte & 0x10)>>4;
	
	int isRegressionBased = (sameRByte >> 7) & 0x01;
	

	pde_exe->SZ_SIZE_TYPE = ((sameRByte & 0x40)>>6)==1?8:4;
	
	if(confparams_dec==NULL)
	{
		confparams_dec = (sz_params*)malloc(sizeof(sz_params));
		memset(confparams_dec, 0, sizeof(sz_params));
	}	
	
	convertBytesToSZParams(&(bytes[index]), confparams_dec, pde_exe);
	/*sz_params* params = convertBytesToSZParams(&(bytes[index]));
	if(confparams_dec!=NULL)
		free(confparams_dec);
	confparams_dec = params;*/	
	if(confparams_dec->dataType==SZ_FLOAT)
		index += MetaDataByteLength;
	else if(confparams_dec->dataType==SZ_DOUBLE)
		index += MetaDataByteLength_double;
	
	if(confparams_dec->dataType!=SZ_FLOAT && confparams_dec->dataType!= SZ_DOUBLE) //if this type is an Int type
		index++; //jump to the dataLength info byte address
	dataSeriesLength = bytesToSize(&(bytes[index]));// 4 or 8	
	index += exe_params->SZ_SIZE_TYPE;
	//index += 4; //max_quant_intervals

	sz_metadata* metadata = (sz_metadata*)malloc(sizeof(struct sz_metadata));
	
	metadata->versionNumber[0] = versions[0];
	metadata->versionNumber[1] = versions[1];
	metadata->versionNumber[2] = versions[2];
	metadata->isConstant = isConstant;
	metadata->isLossless = isLossless;
	metadata->sizeType = exe_params->SZ_SIZE_TYPE;
	metadata->dataSeriesLength = dataSeriesLength;
	
	metadata->conf_params = confparams_dec;
	
	int defactoNBBins = 0; //real # bins
	if(isConstant==0 && isLossless==0)
	{
		if(isRegressionBased==1)
		{
			unsigned char* raBytes = &(bytes[index]);
			defactoNBBins = bytesToInt_bigEndian(raBytes + sizeof(int) + sizeof(double));
		}
		else
		{
			int radExpoL = 0, segmentL = 0, pwrErrBoundBytesL = 0;
			if(metadata->conf_params->errorBoundMode >= PW_REL)
			{
				radExpoL = 1;
				segmentL = exe_params->SZ_SIZE_TYPE;
				pwrErrBoundBytesL = 4;
			}
			
			int mdbl = confparams_dec->dataType==SZ_FLOAT?MetaDataByteLength:MetaDataByteLength_double;
			int offset_typearray = 3 + 1 + mdbl + exe_params->SZ_SIZE_TYPE + 4 + radExpoL + segmentL + pwrErrBoundBytesL + 4 + (4 + confparams_dec->dataType*4) + 1 + 8 
					+ exe_params->SZ_SIZE_TYPE + exe_params->SZ_SIZE_TYPE + exe_params->SZ_SIZE_TYPE + 4;
			defactoNBBins = bytesToInt_bigEndian(bytes+offset_typearray);			
		}

	}	
	
	metadata->defactoNBBins = defactoNBBins;
	return metadata;
}

void SZ_printMetadata(sz_metadata* metadata)
{
	printf("=================SZ Compression Meta Data=================\n");
	printf("Version:                        \t %d.%d.%d\n", metadata->versionNumber[0], metadata->versionNumber[1], metadata->versionNumber[2]);
	printf("Constant data?:                 \t %s\n", metadata->isConstant==1?"YES":"NO");
	printf("Lossless?:                      \t %s\n", metadata->isLossless==1?"YES":"NO");
	printf("Size type (size of # elements): \t %d bytes\n", metadata->sizeType); 
	printf("Num of elements:                \t %zu\n", metadata->dataSeriesLength);
		
	sz_params* params = metadata->conf_params;

	if(params->sol_ID == SZ)
		printf("compressor Name: 		\t SZ\n");
	else if(params->sol_ID == SZ_Transpose)
		printf("compressor Name: 		\t SZ_Transpose\n");
	else
		printf("compressor Name: 		\t Other compressor\n");
	switch(params->dataType)
	{
	case SZ_FLOAT:
		printf("Data type:                      \t FLOAT\n");
		printf("min value of raw data:          \t %f\n", params->fmin);
		printf("max value of raw data:          \t %f\n", params->fmax);		
		break;
	case SZ_DOUBLE:
		printf("Data type:                      \t DOUBLE\n");
		printf("min value of raw data:          \t %f\n", params->dmin);
		printf("max value of raw data:          \t %f\n", params->dmax);	
		break;
	case SZ_INT8:
		printf("Data type:                      \t INT8\n");
		break;	
	case SZ_INT16:
		printf("Data type:                      \t INT16\n");
		break;
	case SZ_INT32:
		printf("Data type:                      \t INT32\n");
		break;	
	case SZ_INT64:
		printf("Data type:                      \t INT64\n");
		break;	
	case SZ_UINT8:
		printf("Data type:                      \t UINT8\n");
		break;	
	case SZ_UINT16:
		printf("Data type:                      \t UINT16\n");
		break;
	case SZ_UINT32:
		printf("Data type:                      \t UINT32\n");
		break;	
	case SZ_UINT64:
		printf("Data type:                      \t UINT64\n");
		break;				
	}
	
	if(exe_params->optQuantMode==1)
	{
		printf("quantization_intervals:         \t 0\n");
		printf("max_quant_intervals:            \t %d\n", params->max_quant_intervals);
		printf("actual used # intervals:        \t %d\n", metadata->defactoNBBins);
	}
	else
	{
		printf("quantization_intervals:         \t %d\n", params->quantization_intervals);
		printf("max_quant_intervals:            \t - %d\n", params->max_quant_intervals);		
	}
	
	printf("dataEndianType (prior raw data):\t %s\n", dataEndianType==BIG_ENDIAN_DATA?"BIG_ENDIAN":"LITTLE_ENDIAN");
	printf("sysEndianType (at compression): \t %s\n", sysEndianType==1?"BIG_ENDIAN":"LITTLE_ENDIAN");
	printf("sampleDistance:                 \t %d\n", params->sampleDistance);
	printf("predThreshold:                  \t %f\n", params->predThreshold);
	switch(params->szMode)
	{
	case SZ_BEST_SPEED:
		printf("szMode:                         \t SZ_BEST_SPEED (without Gzip)\n");
		break;
	case SZ_BEST_COMPRESSION:
		printf("szMode:                         \t SZ_BEST_COMPRESSION (with Zstd or Gzip)\n");
		break;
	}
	switch(params->gzipMode)
	{
	case Z_BEST_SPEED:
		printf("gzipMode:                       \t Z_BEST_SPEED\n");
		break;
	case Z_DEFAULT_COMPRESSION:
		printf("gzipMode:                       \t Z_BEST_SPEED\n");
		break;	
	case Z_BEST_COMPRESSION:
		printf("gzipMode:                       \t Z_BEST_COMPRESSION\n");
		break;
	}
	
	switch(params->errorBoundMode)
	{
	case ABS:
		printf("errBoundMode:                   \t ABS\n");
		printf("absErrBound:                    \t %f\n", params->absErrBound);
		break;
	case REL:
		printf("errBoundMode:                   \t REL (based on value_range extent)\n");
		printf("relBoundRatio:                  \t %f\n", params->relBoundRatio);
		break;
	case ABS_AND_REL:
		printf("errBoundMode:                   \t ABS_AND_REL\n");
		printf("absErrBound:                    \t %f\n", params->absErrBound);
		printf("relBoundRatio:                  \t %f\n", params->relBoundRatio);
		break;
	case ABS_OR_REL:
		printf("errBoundMode:                   \t ABS_OR_REL\n");
		printf("absErrBound:                    \t %f\n", params->absErrBound);
		printf("relBoundRatio:                  \t %f\n", params->relBoundRatio);
		break;
	case PSNR:
		printf("errBoundMode:                   \t PSNR\n");
		printf("psnr:                           \t %f\n", params->psnr);
		break;
	case PW_REL:
		printf("errBoundMode:                   \t PW_REL\n");
		break;
	case ABS_AND_PW_REL:
		printf("errBoundMode:                   \t ABS_AND_PW_REL\n");
		printf("absErrBound:                    \t %f\n", params->absErrBound);
		break;
	case ABS_OR_PW_REL:
		printf("errBoundMode:                   \t ABS_OR_PW_REL\n");
		printf("absErrBound:                    \t %f\n", params->absErrBound);
		break;
	case REL_AND_PW_REL:
		printf("errBoundMode:                   \t REL_AND_PW_REL\n");
		printf("range_relBoundRatio:            \t %f\n", params->relBoundRatio);
		break;
	case REL_OR_PW_REL:
		printf("errBoundMode:                   \t REL_OR_PW_REL\n");
		printf("range_relBoundRatio:            \t %f\n", params->relBoundRatio);
		break;
	}
	
	if(params->errorBoundMode>=PW_REL && params->errorBoundMode<=REL_OR_PW_REL)
	{
		printf("pw_relBoundRatio:               \t %f\n", params->pw_relBoundRatio);
		//printf("segment_size:                   \t %d\n", params->segment_size);
		switch(params->pwr_type)
		{
		case SZ_PWR_MIN_TYPE:
			printf("pwrType:                    \t SZ_PWR_MIN_TYPE\n");
			break;
		case SZ_PWR_AVG_TYPE:
			printf("pwrType:                    \t SZ_PWR_AVG_TYPE\n");
			break;
		case SZ_PWR_MAX_TYPE:
			printf("pwrType:                    \t SZ_PWR_MAX_TYPE\n");
			break;
		}
	}
}

/*-----------------------------------batch data compression--------------------------------------*/

void filloutDimArray(size_t* dim, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1)
{
	if(r2==0)
		dim[0] = r1;
	else if(r3==0)
	{
		dim[0] = r2;
		dim[1] = r1;
	}
	else if(r4==0)
	{
		dim[0] = r3;
		dim[1] = r2;
		dim[2] = r1;
	}
	else if(r5==0)
	{
		dim[0] = r4;
		dim[1] = r3;
		dim[2] = r2;
		dim[3] = r1;
	}
	else
	{
		dim[0] = r5;
		dim[1] = r4;
		dim[2] = r3;
		dim[3] = r2;
		dim[4] = r1;		
	}
}

size_t compute_total_batch_size()
{
	size_t eleNum = 0, totalSize = 0;
	SZ_Variable* p = sz_varset->header;
	while(p->next!=NULL)
	{
		eleNum = computeDataLength(p->next->r5, p->next->r4, p->next->r3, p->next->r2, p->next->r1);
		if(p->next->dataType==SZ_FLOAT)
			totalSize += (eleNum*4);
		else
			totalSize += (eleNum*8);
		p=p->next;
	}
	return totalSize;
}

void SZ_registerVar(int var_id, char* varName, int dataType, void* data, 
			int errBoundMode, double absErrBound, double relBoundRatio, double pwRelBoundRatio, 
			size_t r5, size_t r4, size_t r3, size_t r2, size_t r1)
{
	if(sz_tsc==NULL)
		initSZ_TSC();
		
	//char str[256];
	SZ_batchAddVar(var_id, varName, dataType, data, 
			errBoundMode, absErrBound, relBoundRatio, pwRelBoundRatio, r5, r4, r3, r2, r1);
	//sprintf(str, "%d: %s : %zuX%zuX%zuX%zu%zu : %d : %f : %f : %f\n", sz_varset->count - 1, varName, r5, r4, r3, r2, r1, errBoundMode, absErrBound, relBoundRatio, pwRelBoundRatio);
	//fputs(str, sz_tsc->metadata_file);
}

int SZ_deregisterVar_ID(int var_id)
{
	int state = SZ_batchDelVar_ID(var_id);
	return state;
}

int SZ_deregisterVar(char* varName)
{
	int state = SZ_batchDelVar(varName);
	return state;
}

#ifdef HAVE_TIMECMPR
/**
 * process multiple variables
 * */
int SZ_compress_ts_select_var(int cmprType, unsigned char* var_ids, unsigned char var_count, unsigned char** newByteData, size_t *outSize)
{
	confparams_cpr->szMode = SZ_TEMPORAL_COMPRESSION;
	confparams_cpr->predictionMode = SZ_PREVIOUS_VALUE_ESTIMATE;
	
	SZ_VarSet* vset = sz_varset;
	int i = 0, j = 0, totalSize = 0;	

	SZ_Variable* vp[256];

	SZ_Variable* v = vset->header->next;	
	for(i = 0;i<vset->count;i++)
	{
		int found = checkVarID(v->var_id, var_ids, var_count);
		if (found)
		{
			multisteps = v->multisteps;
			if(v->dataType==SZ_FLOAT)
			{
				SZ_compress_args_float(cmprType, confparams_cpr->withRegression, &(v->compressedBytes), (float*)v->data, v->r5, v->r4, v->r3, v->r2, v->r1, &(v->compressedSize), v->errBoundMode, v->absErrBound, v->relBoundRatio, v->pwRelBoundRatio);
			}
			else if(v->dataType==SZ_DOUBLE)
			{
				SZ_compress_args_double(cmprType, confparams_cpr->withRegression, &(v->compressedBytes), (double*)v->data, v->r5, v->r4, v->r3, v->r2, v->r1, &(v->compressedSize), v->errBoundMode, v->absErrBound, v->relBoundRatio, v->pwRelBoundRatio);
			}
		
			totalSize += v->compressedSize;
			v->compressType = multisteps->compressionType;
			vp[j] = v;
			j++;
		}
		v = v->next;
	}
	
	*outSize = sizeof(int) + sizeof(unsigned short) + totalSize + var_count*(3*sizeof(unsigned char)+sizeof(size_t));
	*newByteData = (unsigned char*)malloc(*outSize); 
	unsigned char* p = *newByteData;

	intToBytes_bigEndian(p, sz_tsc->currentStep);
	p+=4;
	shortToBytes(p, var_count);
	p+=2;

	for(i=0;i<var_count;i++)
	{
		v = vp[i];
		*p = v->var_id; //1 byte
		p++;
		*p = (unsigned char)v->compressType; //1 byte
		p++;
		*p = (unsigned char)v->dataType; //1 byte
		p++;
		sizeToBytes(p, v->compressedSize); //size_t
		p += sizeof(size_t);							
		memcpy(p, v->compressedBytes, v->compressedSize); //outSize_[i]
		p += v->compressedSize;
	}

	sz_tsc->currentStep ++;	
	
	return SZ_SCES;	
}

/**
 * process all variables
 * */
int SZ_compress_ts(int cmprType, unsigned char** newByteData, size_t *outSize)
{
	confparams_cpr->szMode = SZ_TEMPORAL_COMPRESSION;
	confparams_cpr->predictionMode = SZ_PREVIOUS_VALUE_ESTIMATE;
	
	SZ_VarSet* vset = sz_varset;
	
	//char *metadata_str = (char*)malloc(vset->count*256);
	//memset(metadata_str, 0, vset->count*256);
	//sprintf(metadata_str, "step %d", sz_tsc->currentStep);
	
	int i = 0, totalSize = 0;
	
	SZ_Variable* v = vset->header->next;	
	for(i=0;i<vset->count;i++)
	{
		multisteps = v->multisteps; //assign the v's multisteps to the global variable 'multisteps', which will be used in the following compression.

		if(v->dataType==SZ_FLOAT)
		{
			SZ_compress_args_float(cmprType, confparams_cpr->withRegression, &(v->compressedBytes), (float*)v->data, v->r5, v->r4, v->r3, v->r2, v->r1, &(v->compressedSize), v->errBoundMode, v->absErrBound, v->relBoundRatio, v->pwRelBoundRatio);
		}
		else if(v->dataType==SZ_DOUBLE)
		{
			SZ_compress_args_double(cmprType, confparams_cpr->withRegression, &(v->compressedBytes), (double*)v->data, v->r5, v->r4, v->r3, v->r2, v->r1, &(v->compressedSize), v->errBoundMode, v->absErrBound, v->relBoundRatio, v->pwRelBoundRatio);
		}
		//sprintf(metadata_str, "%s:%d,%d,%zu", metadata_str, i, multisteps->lastSnapshotStep, outSize_[i]);
		
		totalSize += v->compressedSize;
		v->compressType = multisteps->compressionType;
		v = v->next;
	}
	
	//sprintf(metadata_str, "%s\n", metadata_str);
	//fputs(metadata_str, sz_tsc->metadata_file);
	//free(metadata_str);
	
	//sizeof(int)==current time step; 2*sizeof(char)+sizeof(size_t)=={compressionType + datatype + compression_data_size}; 
	//sizeof(char)==# variables
	*outSize = sizeof(int) + sizeof(unsigned short) + totalSize + vset->count*(3*sizeof(unsigned char)+sizeof(size_t));
	*newByteData = (unsigned char*)malloc(*outSize); 
	unsigned char* p = *newByteData;

	intToBytes_bigEndian(p, sz_tsc->currentStep);
	p+=4;
	shortToBytes(p, vset->count);
	p+=2;
	
	v = vset->header->next;

	for(i=0;i<vset->count;i++)
	{
		*p = v->var_id; //1 byte
		p++;
		*p = (unsigned char)v->compressType; //1 byte
		p++;
		*p = (unsigned char)v->dataType; //1 byte
		p++;
		sizeToBytes(p, v->compressedSize); //size_t
		p += sizeof(size_t);
		
		memcpy(p, v->compressedBytes, v->compressedSize); //outSize_[i]
		p += v->compressedSize;
		v = v->next;
	}

	sz_tsc->currentStep ++;	
	//free(outSize_);
	
	return SZ_SCES;
}

void SZ_decompress_ts(unsigned char *bytes, size_t bytesLength)
{
	sz_params  de_params;
	memset(&de_params, 0, sizeof(sz_params));
	de_params.szMode = SZ_TEMPORAL_COMPRESSION;
	de_params.predictionMode = SZ_PREVIOUS_VALUE_ESTIMATE;

	sz_exedata de_exe;
	memset(&de_exe, 0, sizeof(sz_exedata));

	
	int x = 1;
	char *y = (char*)&x;
	if(*y==1)
		sysEndianType = LITTLE_ENDIAN_SYSTEM;
	else //=0
		sysEndianType = BIG_ENDIAN_SYSTEM;
	
	int i = 0;
	size_t r5 = 0, r4 = 0, r3 = 0, r2 = 0, r1 = 0;
	unsigned char* q = bytes;
	sz_tsc->currentStep = bytesToInt_bigEndian(q); 
	q += 4;
	unsigned short nbVars = (unsigned short)bytesToShort(q);
	q += 2;
	
	float *newFloatData = NULL;
	double *newDoubleData = NULL;	
	
	for(i=0;i<nbVars;i++)
	{
		unsigned char var_id = *(q++);
		SZ_Variable* p = SZ_getVariable(var_id);
		sz_multisteps* multisteps = p->multisteps;
		multisteps->compressionType = *(q++);
		unsigned char dataType = *(q++);
		size_t cmpSize = bytesToSize(q);
		q += sizeof(size_t);
		
		if(p==NULL)
			q += cmpSize;
		else
		{
			sz_multisteps* multisteps = p->multisteps;
			r5 = p->r5;
			r4 = p->r4;
			r3 = p->r3;
			r2 = p->r2;
			r1 = p->r1;
			size_t dataLen = computeDataLength(r5, r4, r3, r2, r1);				
			
			unsigned char* cmpBytes = q;			
			switch(dataType)
			{
			case SZ_FLOAT:
					SZ_decompress_args_float(&newFloatData, r5, r4, r3, r2, r1, cmpBytes, cmpSize, multisteps->compressionType, multisteps->hist_data, &de_exe, &de_params);
					memcpy(p->data, newFloatData, dataLen*sizeof(float));
					free(newFloatData);
					break;
			case SZ_DOUBLE:
					SZ_decompress_args_double(&newDoubleData, r5, r4, r3, r2, r1, cmpBytes, cmpSize, multisteps->compressionType, multisteps->hist_data, &de_exe, &de_params);
					memcpy(p->data, newDoubleData, dataLen*sizeof(double));
					free(newDoubleData);
					break;
			default:
					printf("Error: data type cannot be the types other than SZ_FLOAT or SZ_DOUBLE\n");
					return;	
			}
			
			q += cmpSize;			
		}
	}	
}

void SZ_decompress_ts_select_var(unsigned char* var_ids, unsigned char var_count, unsigned char *bytes, size_t bytesLength)
{
	sz_params  de_params;
	memset(&de_params, 0, sizeof(sz_params));
	de_params.szMode = SZ_TEMPORAL_COMPRESSION;
	de_params.predictionMode = SZ_PREVIOUS_VALUE_ESTIMATE;
	
	sz_exedata de_exe;
	memset(&de_exe, 0, sizeof(sz_exedata));
	
	int x = 1;
	char *y = (char*)&x;
	if(*y==1)
		sysEndianType = LITTLE_ENDIAN_SYSTEM;
	else //=0
		sysEndianType = BIG_ENDIAN_SYSTEM;
	
	int i = 0;
	size_t r5 = 0, r4 = 0, r3 = 0, r2 = 0, r1 = 0;
	unsigned char* q = bytes;
	sz_tsc->currentStep = bytesToInt_bigEndian(q); 
	q += 4;
	unsigned short nbVars = (unsigned short)bytesToShort(q);
	q += 2;
	
	float *newFloatData = NULL;
	double *newDoubleData = NULL;	
	
	for(i=0;i<nbVars;i++)
	{
		unsigned char var_id = *(q++);
		int selected = checkVarID(var_id, var_ids, var_count);
		SZ_Variable* p = SZ_getVariable(var_id);
		sz_multisteps* multisteps = p->multisteps;
		multisteps->compressionType = *(q++);
		unsigned char dataType = *(q++);
		size_t cmpSize = bytesToSize(q);
		q += sizeof(size_t);
		
		if(p==NULL || selected == 0) //p==NULL means the variable was not registered during compression ; selected==0 means that the variable is not selected
			q += cmpSize;
		else // p!=NULL && selected == 1
		{
			sz_multisteps* multisteps = p->multisteps;
			r5 = p->r5;
			r4 = p->r4;
			r3 = p->r3;
			r2 = p->r2;
			r1 = p->r1;
			size_t dataLen = computeDataLength(r5, r4, r3, r2, r1);				
			
			unsigned char* cmpBytes = q;			
			switch(dataType)
			{
			case SZ_FLOAT:
					SZ_decompress_args_float(&newFloatData, r5, r4, r3, r2, r1, cmpBytes, cmpSize, multisteps->compressionType, multisteps->hist_data, &de_exe, &de_params);
					memcpy(p->data, newFloatData, dataLen*sizeof(float));
					free(newFloatData);
					break;
			case SZ_DOUBLE:
					SZ_decompress_args_double(&newDoubleData, r5, r4, r3, r2, r1, cmpBytes, cmpSize, multisteps->compressionType, multisteps->hist_data, &de_exe, &de_params);
					memcpy(p->data, newDoubleData, dataLen*sizeof(double));
					free(newDoubleData);
					break;
			default:
					printf("Error: data type cannot be the types other than SZ_FLOAT or SZ_DOUBLE\n");
					return;	
			}
			
			q += cmpSize;			
		}
	}	
}
#endif


void SZ_Finalize()
{
#ifdef HAVE_TIMECMPR		
	if(sz_varset!=NULL)
		SZ_freeVarSet(SZ_MAINTAIN_VAR_DATA);
#endif

	if(confparams_dec!=NULL)
	{
		free(confparams_dec);
		confparams_dec = NULL;
	}
	if(confparams_cpr!=NULL)
	{
		free(confparams_cpr);
		confparams_cpr = NULL;
	}	
	if(exe_params!=NULL)
	{
		free(exe_params);
		exe_params = NULL;
	}
	
//#ifdef HAVE_TIMECMPR	
//	if(sz_tsc!=NULL && sz_tsc->metadata_file!=NULL)
//		fclose(sz_tsc->metadata_file);
//#endif
}


/**
 *
 * Inits the compressor for SZ_compress_customize
 *
 * with SZ_Init(NULL) if not previously initialized and no params passed
 * with SZ_InitParam(userPara) otherwise if params are passed
 * and doesn't not initialize otherwise
 *
 * @param sz_params* userPara : the user configuration or null
 * @param sz_params* confparams : the current configuration
 */
static void sz_maybe_init_with_user_params(struct sz_params* userPara, struct sz_params* current_params) {
		if(userPara==NULL && current_params == NULL)
			SZ_Init(NULL);
		else if(userPara != NULL)
			SZ_Init_Params((sz_params*)userPara);
}


/**
 * 
 * The interface for the user-customized compression method 
 * 
 * @param char* comprName : the name of the specific compression approach
 * @param void* userPara : the pointer of the user-customized data stracture containing the cusotmized compressors' requried input parameters
 * @param int dataType : data type (SZ_FLOAT, SZ_DOUBLE, SZ_INT8, SZ_UINT8, SZ_INT16, SZ_UINT16, ....)
 * @param void* data : input dataset
 * @param size_t r5 : the size of dimension 5
 * @param size_t r4 : the size of dimension 4
 * @param size_t r3 : the size of dimension 3
 * @param size_t r2 : the size of dimension 2
 * @param size_t r1 : the size of dimension 1
 * @param size_t outSize : the number of bytes after compression
 * @param int *status : the execution status of the compression operation (success: SZ_SCES or fail: SZ_NSCS)
 * 
 * */
unsigned char* SZ_compress_customize(const char* cmprName, void* userPara, int dataType, void* data, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, int *status)
{
	unsigned char* result = NULL;
	if(strcmp(cmprName, "SZ2.0")==0 || strcmp(cmprName, "SZ2.1")==0 || strcmp(cmprName, "SZ")==0)
	{
		sz_maybe_init_with_user_params(userPara, confparams_cpr);
		result = SZ_compress(dataType, data, outSize, r5, r4, r3, r2, r1);
		*status = SZ_SCES;
	}
	else if(strcmp(cmprName, "SZ1.4")==0)
	{
		sz_maybe_init_with_user_params(userPara, confparams_cpr);
		confparams_cpr->withRegression = SZ_NO_REGRESSION;
		
		result = SZ_compress(dataType, data, outSize, r5, r4, r3, r2, r1);
		*status = SZ_SCES;		
    }
    else if(strcmp(cmprName, "SZ_Transpose")==0)
    {
		void* transData = transposeData(data, dataType, r5, r4, r3, r2, r1);
		sz_maybe_init_with_user_params(userPara, confparams_cpr);
		size_t n = computeDataLength(r5, r4, r3, r2, r1);
		result = SZ_compress(dataType, transData, outSize, 0, 0, 0, 0, n);
	}
    else if(strcmp(cmprName, "ExaFEL")==0){
    	assert(dataType==SZ_FLOAT);
    	assert(r5==0);
    	result = exafelSZ_Compress(userPara,data, r4, r3, r2, r1,outSize);
    	*status = SZ_SCES;
	}
	else
	{
		*status = SZ_NSCS;
	}
	return result;
}

unsigned char* SZ_compress_customize_threadsafe(const char* cmprName, void* userPara, int dataType, void* data, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, size_t *outSize, int *status)
{
	unsigned char* result = NULL;
	if(strcmp(cmprName, "SZ2.0")==0 || strcmp(cmprName, "SZ2.1")==0 || strcmp(cmprName, "SZ")==0)
	{
		SZ_Init(NULL);
		struct sz_params* para = (struct sz_params*)userPara;
		
		if(dataType==SZ_FLOAT)
		{	
			SZ_compress_args_float(-1, SZ_WITH_LINEAR_REGRESSION, &result, (float *)data, r5, r4, r3, r2, r1, 
			outSize, para->errorBoundMode, para->absErrBound, para->relBoundRatio, para->pw_relBoundRatio);
		}
		else if(dataType==SZ_DOUBLE)
		{
			SZ_compress_args_double(-1, SZ_WITH_LINEAR_REGRESSION, &result, (double *)data, r5, r4, r3, r2, r1, 
			outSize, para->errorBoundMode, para->absErrBound, para->relBoundRatio, para->pw_relBoundRatio);
		}		

		*status = SZ_SCES;
		return result;
	}
	else if(strcmp(cmprName, "SZ1.4")==0)
	{
		SZ_Init(NULL);
		struct sz_params* para = (struct sz_params*)userPara;
		
		if(dataType==SZ_FLOAT)
		{	
			SZ_compress_args_float(-1, SZ_NO_REGRESSION, &result, (float *)data, r5, r4, r3, r2, r1, 
			outSize, para->errorBoundMode, para->absErrBound, para->relBoundRatio, para->pw_relBoundRatio);
		}
		else if(dataType==SZ_DOUBLE)
		{
			SZ_compress_args_double(-1, SZ_NO_REGRESSION, &result, (double *)data, r5, r4, r3, r2, r1, 
			outSize, para->errorBoundMode, para->absErrBound, para->relBoundRatio, para->pw_relBoundRatio);
		}		

		*status = SZ_SCES;
		return result;
    }
    else if(strcmp(cmprName, "SZ_Transpose")==0)
    {
		void* transData = transposeData(data, dataType, r5, r4, r3, r2, r1);
		struct sz_params* para = (struct sz_params*)userPara;
	
		size_t n = computeDataLength(r5, r4, r3, r2, r1);
		
		result = SZ_compress_args(dataType, transData, outSize, para->errorBoundMode, para->absErrBound, para->relBoundRatio, para->pw_relBoundRatio, 0, 0, 0, 0, n);
		
		*status = SZ_SCES;
	}
    else if(strcmp(cmprName, "ExaFEL")==0){  //not sure if this part is thread safe!
    	assert(dataType==SZ_FLOAT);
    	assert(r5==0);
    	result = exafelSZ_Compress(userPara,data, r4, r3, r2, r1,outSize);
    	*status = SZ_SCES;
	}
	else
	{
		*status = SZ_NSCS;
	}
	return result;
}


/**
 * 
 * The interface for the user-customized decompression method 
 * 
 * @param char* comprName : the name of the specific compression approach
 * @param void* userPara : the pointer of the user-customized data stracture containing the cusotmized compressors' requried input parameters
 * @param int dataType : data type (SZ_FLOAT, SZ_DOUBLE, SZ_INT8, SZ_UINT8, SZ_INT16, SZ_UINT16, ....)
 * @param unsigned char* bytes : input bytes (the compressed data)
 * @param size_t r5 : the size of dimension 5
 * @param size_t r4 : the size of dimension 4
 * @param size_t r3 : the size of dimension 3
 * @param size_t r2 : the size of dimension 2
 * @param size_t r1 : the size of dimension 1
 * @param int *status : the execution status of the compression operation (success: SZ_SCES or fail: SZ_NSCS)
 * 
 * */
void* SZ_decompress_customize(const char* cmprName, void* userPara, int dataType, unsigned char* bytes, size_t byteLength, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, int *status)
{
	void* result = NULL;
	if(strcmp(cmprName, "SZ2.0")==0 || strcmp(cmprName, "SZ")==0 || strcmp(cmprName, "SZ1.4")==0)
	{
		result = SZ_decompress(dataType, bytes, byteLength, r5, r4, r3, r2, r1);
		* status = SZ_SCES;
	}
    else if(strcmp(cmprName, "SZ_Transpose")==0)
    {
		size_t n = computeDataLength(r5, r4, r3, r2, r1);
		void* tmpData = SZ_decompress(dataType, bytes, byteLength, 0, 0, 0, 0, n);
		result = detransposeData(tmpData, dataType, r5, r4, r3, r2, r1);
	}
  	else if(strcmp(cmprName, "ExaFEL")==0){
    	assert(dataType==SZ_FLOAT);
   		assert(r5==0);
    	result = exafelSZ_Decompress(userPara,bytes, r4, r3, r2, r1,byteLength);
    	*status = SZ_SCES;
	}
	else
	{
		*status = SZ_NSCS;
	}
	return result;	
}


void* SZ_decompress_customize_threadsafe(const char* cmprName, void* userPara, int dataType, unsigned char* bytes, size_t byteLength, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, int *status)
{
	return SZ_decompress_customize(cmprName, userPara, dataType, bytes, byteLength, r5, r4, r3, r2, r1, status);
}
