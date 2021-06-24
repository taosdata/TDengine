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

//sihuan added: use a assistant struct to do sorting and swap that are easy to implement: should
//consider optimizing the performance later.
typedef struct sort_ast_particle{
	int64_t id;
	float var[6];
} sort_ast_particle;

int compare_struct(const void* obj1, const void* obj2);//sihuan added: the compare function in the qsort parameter for 2 structures
void reorder_vars(SZ_VarSet* vset);//sihuan added: reorder the variables increasingly by their index
size_t intersectAndsort(int64_t* preIndex, size_t preLen, SZ_VarSet* curVar, size_t dataLen, unsigned char* bitarray);
//sihuan added: find intersection and keep new var sorted by id
void write_reordered_tofile(SZ_VarSet* curVar, size_t dataLen);
//sihuan added: write the reordered input to files for further decompression validation
float calculate_delta_t(size_t size);//sihuan added

int is_lossless_compressed_data(unsigned char* compressedBytes, size_t cmpSize);
unsigned long sz_lossless_compress(int losslessCompressor, int level, unsigned char* data, unsigned long dataLength, unsigned char** compressBytes);
unsigned long sz_lossless_decompress(int losslessCompressor, unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData, unsigned long targetOriSize);
unsigned long sz_lossless_decompress65536bytes(int losslessCompressor, unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData);
void* detransposeData(void* data, int dataType, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);
void* transposeData(void* data, int dataType, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _UTILITY_H  ----- */
