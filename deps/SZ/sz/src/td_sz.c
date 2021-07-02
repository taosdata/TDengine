
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "td_sz.h"
#include "sz.h"



//
// compress interface to tdengine return value is count of output with bytes
//
int tdszCompress(int type, const char * input, const int nelements, const char * output)
{
	// check valid
	sz_params comp_params = *confparams_cpr;
	
	size_t outSize = SZ_compress_args(type, input, (size_t)nelements, (unsigned char*)output, &comp_params);	
    return (int)outSize;
}

//
// decompress interface to tdengine return value is count of output with bytes
//
int tdszDecompress(int type, const char * input, int compressedSize, const int nelements, const char * output)
{
	size_t outSize = SZ_decompress(type, (void*)input, compressedSize, (size_t)nelements, (unsigned char*)output);
    return (int)outSize;
}
