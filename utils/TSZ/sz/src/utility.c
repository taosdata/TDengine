/**
 *  @file utility.c
 *  @author Sheng Di, Sihuan Li
 *  @date Aug, 2018
 *  @brief 
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "utility.h"
#include "sz.h"
#include "zstd.h"


int is_lossless_compressed_data(unsigned char* compressedBytes, size_t cmpSize)
{
#if ZSTD_VERSION_NUMBER >= 10300
	unsigned long long frameContentSize = ZSTD_getFrameContentSize(compressedBytes, cmpSize);
	if(frameContentSize != ZSTD_CONTENTSIZE_ERROR)
		return ZSTD_COMPRESSOR;
#else
	unsigned long long frameContentSize = ZSTD_getDecompressedSize(compressedBytes, cmpSize);
	if(frameContentSize != 0)
		return ZSTD_COMPRESSOR;
#endif

	return -1; //fast mode (without GZIP or ZSTD)
}

unsigned long sz_lossless_compress(int losslessCompressor, unsigned char* data, unsigned long dataLength, unsigned char* compressBytes)
{
	unsigned long outSize = 0; 
	int level = 3 ; // fast mode
	size_t estimatedCompressedSize = 0;
	switch(losslessCompressor)
	{
	case ZSTD_COMPRESSOR:
		if(dataLength < 100) 
			estimatedCompressedSize = 200;
		else
			estimatedCompressedSize = dataLength*1.2;
		//*compressBytes = (unsigned char*)malloc(estimatedCompressedSize); // comment by tickduan 
		outSize = ZSTD_compress(compressBytes, estimatedCompressedSize, data, dataLength, level); //default setting of level is 3
		break;
	default:
		printf("Error: Unrecognized lossless compressor in sz_lossless_compress()\n");
	}
	return outSize;
}

unsigned long sz_lossless_decompress(int losslessCompressor, unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData, unsigned long targetOriSize)
{
	unsigned long outSize = 0;
	switch(losslessCompressor)
	{
	case ZSTD_COMPRESSOR:
		*oriData = (unsigned char*)malloc(targetOriSize);
		outSize = ZSTD_decompress(*oriData, targetOriSize, compressBytes, cmpSize);
		break;
	default:
		printf("Error: Unrecognized lossless compressor in sz_lossless_decompress()\n");
	}
	return outSize;
}
