/**
 *  @file TypeManager.c
 *  @author Sheng Di
 *  @date May, 2016
 *  @brief TypeManager is used to manage the type array: parsing of the bytes and other types in between.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include "DynamicByteArray.h"
#include "sz.h"

//int convertIntArray2ByteArray_fast_8b()

size_t convertIntArray2ByteArray_fast_1b(unsigned char* intArray, size_t intArrayLength, unsigned char **result)
{
	size_t byteLength = 0;
	size_t i, j; 
	if(intArrayLength%8==0)
		byteLength = intArrayLength/8;
	else
		byteLength = intArrayLength/8+1;
		
	if(byteLength>0)
		*result = (unsigned char*)malloc(byteLength*sizeof(unsigned char));
	else
		*result = NULL;
	size_t n = 0;
	int tmp, type;
	for(i = 0;i<byteLength;i++)
	{
		tmp = 0;
		for(j = 0;j<8&&n<intArrayLength;j++)
		{
			type = intArray[n];
			if(type == 1)
				tmp = (tmp | (1 << (7-j)));
			n++;
		}
    	(*result)[i] = (unsigned char)tmp;
	}
	return byteLength;
}

size_t convertIntArray2ByteArray_fast_1b_to_result(unsigned char* intArray, size_t intArrayLength, unsigned char *result)
{
	size_t byteLength = 0;
	size_t i, j; 
	if(intArrayLength%8==0)
		byteLength = intArrayLength/8;
	else
		byteLength = intArrayLength/8+1;
		
	size_t n = 0;
	int tmp, type;
	for(i = 0;i<byteLength;i++)
	{
		tmp = 0;
		for(j = 0;j<8&&n<intArrayLength;j++)
		{
			type = intArray[n];
			if(type == 1)
				tmp = (tmp | (1 << (7-j)));
			n++;
		}
    	result[i] = (unsigned char)tmp;
	}
	return byteLength;
}


void convertByteArray2IntArray_fast_2b(size_t stepLength, unsigned char* byteArray, size_t byteArrayLength, unsigned char **intArray)
{
	if(stepLength > byteArrayLength*4)
	{
		printf("Error: stepLength > byteArray.length*4\n");
		printf("stepLength=%zu, byteArray.length=%zu\n", stepLength, byteArrayLength);
		exit(0);
	}
	if(stepLength>0)
		*intArray = (unsigned char*)malloc(stepLength*sizeof(unsigned char));
	else
		*intArray = NULL;
	size_t i, n = 0;

	for (i = 0; i < byteArrayLength; i++) {
		unsigned char tmp = byteArray[i];
		(*intArray)[n++] = (tmp & 0xC0) >> 6;
		if(n==stepLength)
			break;
		(*intArray)[n++] = (tmp & 0x30) >> 4;
		if(n==stepLength)
			break;
		(*intArray)[n++] = (tmp & 0x0C) >> 2;
		if(n==stepLength)
			break;
		(*intArray)[n++] = tmp & 0x03;
		if(n==stepLength)
			break;
	}
}

/**
 * little endian
 * [01|10|11|00|....]-->[01|10|11|00][....]
 * @param timeStepType
 * @return
 */
size_t convertIntArray2ByteArray_fast_2b(unsigned char* timeStepType, size_t timeStepTypeLength, unsigned char **result)
{
	size_t i, j, byteLength = 0;
	if(timeStepTypeLength%4==0)
		byteLength = timeStepTypeLength*2/8;
	else
		byteLength = timeStepTypeLength*2/8+1;
	if(byteLength>0)
		*result = (unsigned char*)malloc(byteLength*sizeof(unsigned char));
	else
		*result = NULL;
	size_t n = 0;
	for(i = 0;i<byteLength;i++)
	{
		int tmp = 0;
		for(j = 0;j<4&&n<timeStepTypeLength;j++)
		{
			int type = timeStepType[n];
			switch(type)
			{
			case 0: 
				
				break;
			case 1:
				tmp = (tmp | (1 << (6-j*2)));
				break;
			case 2:
				tmp = (tmp | (2 << (6-j*2)));
				break;
			case 3:
				tmp = (tmp | (3 << (6-j*2)));
				break;
			default:
				printf("Error: wrong timestep type...: type[%zu]=%d\n", n, type);
				exit(0);
			}
			n++;
		}
		(*result)[i] = (unsigned char)tmp;
	}
	return byteLength;
}

INLINE int getLeftMovingSteps(size_t k, unsigned char resiBitLength)
{
	return 8 - k%8 - resiBitLength;
}

/**
 * 
 * @param timeStepType is the resiMidBits
 * @param resiBitLength is the length of resiMidBits for each element, (the number of resiBitLength == the # of unpredictable elements
 * @return
 */
size_t convertIntArray2ByteArray_fast_dynamic(unsigned char* timeStepType, unsigned char resiBitLength, size_t nbEle, unsigned char **bytes)
{
	size_t i = 0, j = 0, k = 0; 
	int value;
	DynamicByteArray* dba;
	new_DBA(&dba, 1024);
	int tmp = 0, leftMovSteps = 0;
	for(j = 0;j<nbEle;j++)
	{
		if(resiBitLength==0)
			continue;
		value = timeStepType[i];
		leftMovSteps = getLeftMovingSteps(k, resiBitLength);
		if(leftMovSteps < 0)
		{
			tmp = tmp | (value >> (-leftMovSteps));
			addDBA_Data(dba, (unsigned char)tmp);
			tmp = 0 | (value << (8+leftMovSteps));
		}
		else if(leftMovSteps > 0)
		{
			tmp = tmp | (value << leftMovSteps);
		}
		else //==0
		{
			tmp = tmp | value;
			addDBA_Data(dba, (unsigned char)tmp);
			tmp = 0;
		}
		i++;
		k += resiBitLength;
	}
	if(leftMovSteps != 0)
		addDBA_Data(dba, (unsigned char)tmp);
	convertDBAtoBytes(dba, bytes);
	size_t size = dba->size;
	free_DBA(dba);
	return size;
}