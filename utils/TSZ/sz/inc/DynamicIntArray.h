/**
 *  @file DynamicIntArray.h
 *  @author Sheng Di
 *  @date April, 2016
 *  @brief Header file for Dynamic Int Array.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _DynamicIntArray_H
#define _DynamicIntArray_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
typedef struct DynamicIntArray
{	
	unsigned char* array; //char* (one byte) is enough, don't have to be int*
	size_t size;
	size_t capacity;
} DynamicIntArray;

void new_DIA(DynamicIntArray **dia, size_t cap);
void convertDIAtoInts(DynamicIntArray *dia, unsigned char **data);
void free_DIA(DynamicIntArray *dia);
int getDIA_Data(DynamicIntArray *dia, size_t pos);
void addDIA_Data(DynamicIntArray *dia, int value);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _DynamicIntArray_H  ----- */
