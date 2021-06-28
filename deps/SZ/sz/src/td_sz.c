
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "td_sz.h"
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
#include "sz.h"
#include "defines.h"


//
// compress interface to tdengine return value is count of output with bytes
//
int tdszCompress(int type, const char * input, const int nelements, const char * output)
{
	size_t outSize = 0;
	void* pOut = SZ_compress(type, (void*)input, &outSize, 0, 0, 0, 0, (size_t)nelements);
	if(pOut == NULL)
		return 0;
	
	// copy to dest
	memcpy(output, pOut, outSize);
	free(pOut);
    return outSize;
}

//
// decompress interface to tdengine return value is count of output with bytes
//
int tdszDecompress(int type, const char * input, int compressedSize, const int nelements, const char * output)
{
	int width = 0;
	if(type == SZ_FLOAT)
	    width = sizeof(float);
	else if(type == SZ_DOUBLE)
	    width = sizeof(double);	
	else
	    return 0;	

	void* pOut = SZ_decompress(type, (void*)input, compressedSize, 0, 0, 0, 0, (size_t)nelements);
	if(pOut == NULL)
		return 0;


	size_t outSize = nelements * width;

	// copy to dest
	memcpy(output, pOut, outSize);
	free(pOut);

    return outSize;
}
