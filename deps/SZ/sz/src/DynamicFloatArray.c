/**
 *  @file DynamicFloatArray.c
 *  @author Sheng Di
 *  @date May, 2016
 *  @brief Dynamic Float Array
 *  (C) 2015 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdlib.h> 
#include <stdio.h>
#include <string.h>
#include "DynamicFloatArray.h"

void new_DFA(DynamicFloatArray **dfa, size_t cap) {
		*dfa = (DynamicFloatArray *)malloc(sizeof(DynamicFloatArray));
        (*dfa)->size = 0;
        (*dfa)->capacity = cap;
        (*dfa)->array = (float*)malloc(sizeof(float)*cap);
    }

void convertDFAtoFloats(DynamicFloatArray *dfa, float **data)
{
	size_t size = dfa->size;
	if(size>0)
		*data = (float*)malloc(size * sizeof(float));
	else
		*data = NULL;
	memcpy(*data, dfa->array, size*sizeof(float));	
}

void free_DFA(DynamicFloatArray *dfa)
{
	free(dfa->array);
	free(dfa);
}

float getDFA_Data(DynamicFloatArray *dfa, size_t pos)
{
	if(pos>=dfa->size)
	{
		printf("Error: wrong position of DIA.\n");
		exit(0);
	}
	return dfa->array[pos];
}

void addDFA_Data(DynamicFloatArray *dfa, float value)
{
	if(dfa->size==dfa->capacity)
	{
		dfa->capacity *= 2;
		dfa->array = (float *)realloc(dfa->array, dfa->capacity*sizeof(float));
	}
	dfa->array[dfa->size] = value;
	dfa->size++;
}
