/**
 *  @file DynamicByteArray.c
 *  @author Sheng Di
 *  @date May, 2016
 *  @brief Dynamic Byte Array
 *  (C) 2015 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdlib.h> 
#include <stdio.h>
#include <string.h>
#include "DynamicByteArray.h"
#include "sz.h"

void new_DBA(DynamicByteArray **dba, size_t cap) {
		*dba = (DynamicByteArray *)malloc(sizeof(DynamicByteArray));
        (*dba)->size = 0;
        (*dba)->capacity = cap;
        (*dba)->array = (unsigned char*)malloc(sizeof(unsigned char)*cap);
}

void convertDBAtoBytes(DynamicByteArray *dba, unsigned char** bytes)
{
	size_t size = dba->size;
	if(size>0)
		*bytes = (unsigned char*)malloc(size * sizeof(unsigned char));
	else
	{
	    *bytes = NULL;
		return ;
	}
	memcpy(*bytes, dba->array, size*sizeof(unsigned char));	
}

void free_DBA(DynamicByteArray *dba)
{
	free(dba->array);
	free(dba);
}

INLINE unsigned char getDBA_Data(DynamicByteArray *dba, size_t pos)
{
	if(pos>=dba->size)
	{
		printf("Error: wrong position of DBA (impossible case unless bugs elsewhere in the code?).\n");
		exit(0);
	}
	return dba->array[pos];
}

INLINE void addDBA_Data(DynamicByteArray *dba, unsigned char value)
{
	if(dba->size==dba->capacity)
	{
		dba->capacity = dba->capacity << 1;
		dba->array = (unsigned char *)realloc(dba->array, dba->capacity*sizeof(unsigned char));
	}
	dba->array[dba->size] = value;
	dba->size ++;
}

INLINE void memcpyDBA_Data(DynamicByteArray *dba, unsigned char* data, size_t length)
{
	if(dba->size + length > dba->capacity)
	{
		dba->capacity = dba->size + length;
		dba->array = (unsigned char *)realloc(dba->array, dba->capacity*sizeof(unsigned char));
	}
	memcpy(&(dba->array[dba->size]), data, length);
	dba->size += length;
}
