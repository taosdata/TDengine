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
	else
	{ 
		printf("Error: data type cannot be the types other than SZ_FLOAT or SZ_DOUBLE\n");
		return SZ_NSCS; //indicating error		
	}

	return nbEle;
}




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
