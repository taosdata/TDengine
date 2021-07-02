
#ifndef _TD_SZ_H
#define _TD_SZ_H
#include "defines.h"

#ifdef __cplusplus
extern "C" {
#endif

//
// compress interface to tdengine return value is count of output with bytes
//
int tdszCompress(int type, const char * input, const int nelements, const char * output);

//
// decompress interface to tdengine return value is count of output with bytes
//
int tdszDecompress(int type, const char * input, int compressedSize, const int nelements, const char * output);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_H  ----- */
