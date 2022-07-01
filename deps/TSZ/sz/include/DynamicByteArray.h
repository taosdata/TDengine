/**
 *  @file DynamicByteArray.h
 *  @author Sheng Di
 *  @date April, 2016
 *  @brief Header file for Dynamic Byte Array.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _DynamicByteArray_H
#define _DynamicByteArray_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
typedef struct DynamicByteArray
{	
	unsigned char* array;
	size_t size;
	size_t capacity;
} DynamicByteArray;

void new_DBA(DynamicByteArray **dba, size_t cap);
void convertDBAtoBytes(DynamicByteArray *dba, unsigned char** bytes);
void free_DBA(DynamicByteArray *dba);
unsigned char getDBA_Data(DynamicByteArray *dba, size_t pos);
void addDBA_Data(DynamicByteArray *dba, unsigned char value);
void memcpyDBA_Data(DynamicByteArray *dba, unsigned char* data, size_t length);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _DynamicByteArray_H  ----- */
