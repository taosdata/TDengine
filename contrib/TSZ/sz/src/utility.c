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

size_t sz_lossless_compress(int losslessCompressor, unsigned char* data, size_t dataLength, unsigned char* compressBytes, size_t compressBytesCapacity)
{
	size_t outSize = 0; 
	int level = 3 ; // fast mode

	//the destination belongs to the caller: never tell the compressor that more room is available
	//than compressBytesCapacity bytes, otherwise a raw (incompressible) frame overflows the buffer
	if(data == NULL || compressBytes == NULL || compressBytesCapacity == 0)
	{
		printf("Error: sz_lossless_compress called without an output buffer\n");
		return 0;
	}

	switch(losslessCompressor)
	{
	case ZSTD_COMPRESSOR:
		//*compressBytes = (unsigned char*)malloc(estimatedCompressedSize); // comment by tickduan 
		outSize = ZSTD_compress(compressBytes, compressBytesCapacity, data, dataLength, level); //default setting of level is 3
		if(ZSTD_isError(outSize) || outSize == 0)
		{
			//the frame does not fit in the buffer we were given: report failure, the caller falls
			//back to storing the data uncompressed
			printf("Error: zstd could not compress %zu bytes into %zu bytes\n", dataLength, compressBytesCapacity);
			return 0;
		}
		break;
	default:
		printf("Error: Unrecognized lossless compressor in sz_lossless_compress()\n");
		return 0;
	}
	return outSize;
}

size_t sz_lossless_decompress(int losslessCompressor, unsigned char* compressBytes, size_t cmpSize, unsigned char** oriData, size_t targetOriSize)
{
	size_t outSize = 0;
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
