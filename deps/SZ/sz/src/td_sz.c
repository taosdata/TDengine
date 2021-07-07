
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "td_sz.h"
#include "sz.h"
#include "conf.h"

//
// Init  success return 1 else 0
//
void tdszInit(double fPrecision, double dPrecision, unsigned int maxIntervals, unsigned int intervals, const char* compressor)
{
	// need malloc
	if(confparams_cpr == NULL)
	   confparams_cpr = (sz_params*)malloc(sizeof(sz_params));    
	if(exe_params == NULL)
       exe_params = (sz_exedata*)malloc(sizeof(sz_exedata));
	 
	// set default
	setDefaulParams(exe_params, confparams_cpr);

    // overwrite with args
	confparams_cpr->absErrBound = fPrecision;
	confparams_cpr->absErrBoundDouble = dPrecision;
	confparams_cpr->max_quant_intervals = maxIntervals;
	confparams_cpr->quantization_intervals = intervals;
	if(strcmp(compressor, "GZIP_COMPRESSOR")==0)
		confparams_cpr->losslessCompressor = GZIP_COMPRESSOR;
	else if(strcmp(compressor, "ZSTD_COMPRESSOR")==0)
		confparams_cpr->losslessCompressor = ZSTD_COMPRESSOR;
	
	return ;
}


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

//
//  tdszExit
//
void tdszExit()
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