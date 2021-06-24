/**
 *  @file callZlib.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the callZlib.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _CallZlib_H
#define _CallZlib_H

#ifdef __cplusplus
extern "C" {
#endif

//#define SZ_ZLIB_BUFFER_SIZE 1048576	
#define SZ_ZLIB_BUFFER_SIZE 65536

#include <stdio.h>

int isZlibFormat(unsigned char magic1, unsigned char magic2);

//callZlib.c
unsigned long zlib_compress(unsigned char* data, unsigned long dataLength, unsigned char** compressBytes, int level);
unsigned long zlib_compress2(unsigned char* data, unsigned long dataLength, unsigned char** compressBytes, int level);
unsigned long zlib_compress3(unsigned char* data, unsigned long dataLength, unsigned char* compressBytes, int level);
unsigned long zlib_compress4(unsigned char* data, unsigned long dataLength, unsigned char** compressBytes, int level);
unsigned long zlib_compress5(unsigned char* data, unsigned long dataLength, unsigned char** compressBytes, int level);

unsigned long zlib_uncompress4(unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData, unsigned long targetOriSize);
unsigned long zlib_uncompress5(unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData, unsigned long targetOriSize);
unsigned long zlib_uncompress(unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData, unsigned long targetOriSize);
unsigned long zlib_uncompress2(unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData, unsigned long targetOriSize);
unsigned long zlib_uncompress3(unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData, unsigned long targetOriSize);

unsigned long zlib_uncompress65536bytes(unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _CallZlib_H  ----- */

