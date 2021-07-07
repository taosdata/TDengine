
#ifndef _TD_SZ_H
#define _TD_SZ_H
#include "defines.h"


#ifdef __cplusplus
extern "C" {
#endif

void cost_start();
double cost_end(const char* tag);

//
// Init  success return 1 else 0
//
void tdszInit(double fPrecision, double dPrecision, unsigned int maxIntervals, unsigned int intervals, const char* compressor);

//
// compress interface to tdengine return value is count of output with bytes
//
int tdszCompress(int type, const char * input, const int nelements, const char * output);

//
// decompress interface to tdengine return value is count of output with bytes
//
int tdszDecompress(int type, const char * input, int compressedSize, const int nelements, const char * output);

//
//  tdszExit
//
void tdszExit();

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_H  ----- */
