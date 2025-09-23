/**
 *  @file utility.h
 *  @author Sheng Di, Sihuan Li
 *  @date July, 2018
 *  @brief Header file for the utility.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _UTILITY_H
#define _UTILITY_H

#include "sz.h"

#ifdef __cplusplus
extern "C" {
#endif


int is_lossless_compressed_data(unsigned char* compressedBytes, size_t cmpSize);
unsigned long sz_lossless_compress(int losslessCompressor, unsigned char* data, unsigned long dataLength, unsigned char* compressBytes);
unsigned long sz_lossless_decompress(int losslessCompressor, unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData, unsigned long targetOriSize);


#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _UTILITY_H  ----- */
