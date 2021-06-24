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
#include "DynamicDoubleArray.h"

void new_DDA(DynamicDoubleArray **dda, size_t cap) {
		*dda = (DynamicDoubleArray *)malloc(sizeof(DynamicDoubleArray));
        (*dda)->size = 0;
        (*dda)->capacity = cap;
        (*dda)->array = (double*)malloc(sizeof(double)*cap);
    }

void convertDDAtoDoubles(DynamicDoubleArray *dba, double **data)
{
	size_t size = dba->size;
	if(size>0)
		*data = (double*)malloc(size * sizeof(double));
	else
		*data = NULL;
	memcpy(*data, dba->array, size*sizeof(double));	
}

void free_DDA(DynamicDoubleArray *dda)
{
	free(dda->array);
	free(dda);
}

double getDDA_Data(DynamicDoubleArray *dda, size_t pos)
{
	if(pos>=dda->size)
	{
		printf("Error: wrong position of DIA.\n");
		exit(0);
	}
	return dda->array[pos];
}

void addDDA_Data(DynamicDoubleArray *dda, double value)
{
	if(dda->size==dda->capacity)
	{
		dda->capacity *= 2;
		dda->array = (double *)realloc(dda->array, dda->capacity*sizeof(double));
	}
	dda->array[dda->size] = value;
	dda->size ++;
}
