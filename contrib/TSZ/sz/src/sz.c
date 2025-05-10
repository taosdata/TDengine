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
#include "sz.h"
#include "CompressElement.h"
#include "DynamicByteArray.h"
#include "TightDataPointStorageD.h"
#include "TightDataPointStorageF.h"
#include "conf.h"
#include "utility.h"


//#include "CurveFillingCompressStorage.h"

unsigned char versionNumber = DATA_FROMAT_VER1;
int SZ_SIZE_TYPE_DEFUALT = 4;

int dataEndianType = LITTLE_ENDIAN_DATA; //*endian type of the data read from disk
int sysEndianType  = LITTLE_ENDIAN_SYSTEM ; //*sysEndianType is actually set automatically.

//the confparams should be separate between compression and decopmression, in case of mutual-affection when calling compression/decompression alternatively
sz_params *confparams_cpr = NULL; //used for compression
sz_exedata *exe_params = NULL;

int SZ_Init(const char *configFilePath)
{
	// check CPU EndianType
	int x = 1;
	char *y = (char*)&x;
	if(*y==1)
		sysEndianType = LITTLE_ENDIAN_SYSTEM;
	else //=0
		sysEndianType = BIG_ENDIAN_SYSTEM;

	int loadFileResult = SZ_LoadConf(configFilePath);
	if(loadFileResult==SZ_FAILED)
		return SZ_FAILED;
	
	exe_params->SZ_SIZE_TYPE = SZ_SIZE_TYPE_DEFUALT;
	return SZ_SUCCESS;
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
		return SZ_FAILED;
	}

	return SZ_SUCCESS;
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

//
//  compress output data to outData and return outSize
//
size_t SZ_compress_args(int dataType, void *data, size_t r1, unsigned char* outData, sz_params* params)
{
	size_t outSize = 0;
	if(dataType==SZ_FLOAT)
	{
		SZ_compress_args_float((float *)data, r1, outData,  &outSize, params);		
	}
	else if(dataType==SZ_DOUBLE)
	{
		SZ_compress_args_double((double *)data, r1, outData,  &outSize, params);
	}
	else
	{
		printf("Error: dataType can only be SZ_FLOAT, SZ_DOUBLE .\n");
		return 0;
	}

	return outSize;
}

//
// decompress output data to outData and return outSize
//
size_t SZ_decompress(int dataType, unsigned char *bytes, size_t byteLength, size_t r1, unsigned char* outData)
{
	sz_exedata de_exe;
	memset(&de_exe, 0, sizeof(sz_exedata));

	sz_params  de_params;
	memset(&de_params, 0, sizeof(sz_params));
	
	size_t outSize = 0;

	if(dataType == SZ_FLOAT)
	{
		int status = SZ_decompress_args_float((float*)outData, r1, bytes, byteLength, 0, NULL, &de_exe, &de_params);
		if(status == SZ_SUCCESS)
		{
			return r1*sizeof(float);
		}
		return 0;	
	}
	else if(dataType == SZ_DOUBLE)
	{		
		int status =  SZ_decompress_args_double((double*)outData, r1, bytes, byteLength, 0, NULL, &de_exe, &de_params);
		if(status == SZ_SUCCESS)
		{
			return r1*sizeof(double);
		}
		return 0;	
	}
	else 
	{
		printf("Error: data type cannot be the types other than SZ_FLOAT or SZ_DOUBLE\n");	
	}

    return outSize;
}

void SZ_Finalize()
{
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
}


#ifdef WINDOWS
#include <windows.h>
int gettimeofday(struct timeval *tp, void *tzp) {
  time_t clock;
  struct tm tm;
  SYSTEMTIME wtm;
  GetLocalTime(&wtm);
  tm.tm_year   = wtm.wYear - 1900;
  tm.tm_mon   = wtm.wMonth - 1;
  tm.tm_mday   = wtm.wDay;
  tm.tm_hour   = wtm.wHour;
  tm.tm_min   = wtm.wMinute;
  tm.tm_sec   = wtm.wSecond;
  tm. tm_isdst  = -1;
  clock = mktime(&tm);
  tp->tv_sec = clock;
  tp->tv_usec = wtm.wMilliseconds * 1000;
  return 0;
}
#else
#include <sys/time.h>
#endif

struct timeval costStart; /*only used for recording the cost*/
double totalCost = 0;

void cost_start()
{
	totalCost = 0;
    gettimeofday(&costStart, NULL);
}

double cost_end(const char* tag)
{
    double elapsed;
    struct timeval costEnd;
    gettimeofday(&costEnd, NULL);
    elapsed = ((costEnd.tv_sec*1000000+costEnd.tv_usec)-(costStart.tv_sec*1000000+costStart.tv_usec))/1000000.0;
    totalCost += elapsed;
    double use_ms = totalCost*1000;
    printf(" timecost %s : %.3f ms\n", tag, use_ms);
    return use_ms; 
}

void show_rate(int in_len, int out_len)
{
  float rate=100*(float)out_len/(float)in_len;
  printf(" in_len=%d out_len=%d compress rate=%.4f%%\n", in_len, out_len, rate);
}

