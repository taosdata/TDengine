/**
 *  @file rw.c
 *  @author Sheng Di
 *  @date April, 2015
 *  @brief io interface for fortrance
 *  (C) 2015 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>

#include "rw.h"
#include "sz.h"

int checkFileExistance(char* filePath)
{
	if( access( filePath, F_OK ) != -1 ) {
		// file exists
		return 1;
	} else {
		// file doesn't exist
		return 0;
	}	
}

float** create2DArray_float(size_t m, size_t n)
{
	size_t i=0;
	float **data = (float**)malloc(sizeof(float*)*m);
	for(i=0;i<m;i++)
		data[i] = (float*)malloc(sizeof(float)*n);
	return data;
}

void free2DArray_float(float** data, size_t m)
{
	size_t i = 0;
	for(i=0;i<m;i++)
		free(data[i]);
	free(data);	
}

float*** create3DArray_float(size_t p, size_t m, size_t n)
{
	size_t i = 0, j = 0;
	float ***data = (float***)malloc(sizeof(float**)*m);
	for(i=0;i<p;i++)
	{
		data[i] = (float**)malloc(sizeof(float*)*n);
		for(j=0;j<m;j++)
			data[i][j] = (float*)malloc(sizeof(float)*n);
	}
	return data;
}

void free3DArray_float(float*** data, size_t p, size_t m)
{
	size_t i,j;
	for(i=0;i<p;i++)
	{
		for(j=0;j<m;j++)
			free(data[i][j]);
		free(data[i]);
	}
	free(data);	
}

double** create2DArray_double(size_t m, size_t n)
{
	size_t i=0;
	double **data = (double**)malloc(sizeof(double*)*m);
	for(i=0;i<m;i++)
			data[i] = (double*)malloc(sizeof(double)*n);
			
	return data;
}

void free2DArray_double(double** data, size_t m)
{
	size_t i;
	for(i=0;i<m;i++)
		free(data[i]);
	free(data);	
}

double*** create3DArray_double(size_t p, size_t m, size_t n)
{
	size_t i = 0, j = 0;
	double ***data = (double***)malloc(sizeof(double**)*m);
	for(i=0;i<p;i++)
	{
		data[i] = (double**)malloc(sizeof(double*)*n);
		for(j=0;j<m;j++)
			data[i][j] = (double*)malloc(sizeof(double)*n);
	}
	return data;
}

void free3DArray_double(double*** data, size_t p, size_t m)
{
	size_t i,j;
	for(i=0;i<p;i++)
	{
		for(j=0;j<m;j++)
			free(data[i][j]);
		free(data[i]);
	}
	free(data);	
}

size_t checkFileSize(char *srcFilePath, int *status)
{
	size_t filesize;
	FILE *pFile = fopen(srcFilePath, "rb");
    if (pFile == NULL)
	{
		printf("Failed to open input file. 1\n");
		*status = SZ_FERR;
		return -1;
	}
	fseek(pFile, 0, SEEK_END);
    filesize = ftell(pFile);
    fclose(pFile);
    *status = SZ_SCES;
    return filesize;
}

unsigned char *readByteData(char *srcFilePath, size_t *byteLength, int *status)
{
	FILE *pFile = fopen(srcFilePath, "rb");
    if (pFile == NULL)
    {
        printf("Failed to open input file. 1\n");
        *status = SZ_FERR;
        return 0;
    }
	fseek(pFile, 0, SEEK_END);
    *byteLength = ftell(pFile);
    fclose(pFile);
    
    unsigned char *byteBuf = ( unsigned char *)malloc((*byteLength)*sizeof(unsigned char)); //sizeof(char)==1
    
    pFile = fopen(srcFilePath, "rb");
    if (pFile == NULL)
    {
        printf("Failed to open input file. 2\n");
        *status = SZ_FERR;
        return 0;
    }
    fread(byteBuf, 1, *byteLength, pFile);
    fclose(pFile);
    *status = SZ_SCES;
    return byteBuf;
}

double *readDoubleData(char *srcFilePath, size_t *nbEle, int *status)
{
	int state = SZ_SCES;
	if(dataEndianType==sysEndianType)
	{
		double *daBuf = readDoubleData_systemEndian(srcFilePath, nbEle,&state);
		*status = state;
		return daBuf;
	}
	else
	{
		size_t i,j;
		
		size_t byteLength;
		unsigned char* bytes = readByteData(srcFilePath, &byteLength, &state);
		if(state==SZ_FERR)
		{
			*status = SZ_FERR;
			return NULL;
		}
		double *daBuf = (double *)malloc(byteLength);
		*nbEle = byteLength/8;
		
		ldouble buf;
		for(i = 0;i<*nbEle;i++)
		{
			j = i*8;
			memcpy(buf.byte, bytes+j, 8);
			symTransform_8bytes(buf.byte);
			daBuf[i] = buf.value;
		}
		free(bytes);
		return daBuf;
	}
}


int8_t *readInt8Data(char *srcFilePath, size_t *nbEle, int *status)
{
	int state = SZ_SCES;
	int8_t *daBuf = readInt8Data_systemEndian(srcFilePath, nbEle, &state);
	*status = state;
	return daBuf;
}

int16_t *readInt16Data(char *srcFilePath, size_t *nbEle, int *status)
{
	int state = SZ_SCES;
	if(dataEndianType==sysEndianType)
	{
		int16_t *daBuf = readInt16Data_systemEndian(srcFilePath, nbEle, &state);
		*status = state;
		return daBuf;
	}
	else
	{
		size_t i,j;

		size_t byteLength;
		unsigned char* bytes = readByteData(srcFilePath, &byteLength, &state);
		if(state == SZ_FERR)
		{
			*status = SZ_FERR;
			return NULL;
		}
		int16_t *daBuf = (int16_t *)malloc(byteLength);
		*nbEle = byteLength/2;

		lint16 buf;
		for(i = 0;i<*nbEle;i++)
		{
			j = i << 1;//*2
			memcpy(buf.byte, bytes+j, 2);
			symTransform_2bytes(buf.byte);
			daBuf[i] = buf.svalue;
		}
		free(bytes);
		return daBuf;
	}
}

uint16_t *readUInt16Data(char *srcFilePath, size_t *nbEle, int *status)
{
	int state = SZ_SCES;
	if(dataEndianType==sysEndianType)
	{
		uint16_t *daBuf = readUInt16Data_systemEndian(srcFilePath, nbEle, &state);
		*status = state;
		return daBuf;
	}
	else
	{
		size_t i,j;

		size_t byteLength;
		unsigned char* bytes = readByteData(srcFilePath, &byteLength, &state);
		if(state == SZ_FERR)
		{
			*status = SZ_FERR;
			return NULL;
		}
		uint16_t *daBuf = (uint16_t *)malloc(byteLength);
		*nbEle = byteLength/2;

		lint16 buf;
		for(i = 0;i<*nbEle;i++)
		{
			j = i << 1;//*2
			memcpy(buf.byte, bytes+j, 2);
			symTransform_2bytes(buf.byte);
			daBuf[i] = buf.usvalue;
		}
		free(bytes);
		return daBuf;
	}
}

int32_t *readInt32Data(char *srcFilePath, size_t *nbEle, int *status)
{
	int state = SZ_SCES;
	if(dataEndianType==sysEndianType)
	{
		int32_t *daBuf = readInt32Data_systemEndian(srcFilePath, nbEle, &state);
		*status = state;
		return daBuf;
	}
	else
	{
		size_t i,j;

		size_t byteLength;
		unsigned char* bytes = readByteData(srcFilePath, &byteLength, &state);
		if(state == SZ_FERR)
		{
			*status = SZ_FERR;
			return NULL;
		}
		int32_t *daBuf = (int32_t *)malloc(byteLength);
		*nbEle = byteLength/4;

		lint32 buf;
		for(i = 0;i<*nbEle;i++)
		{
			j = i*4;
			memcpy(buf.byte, bytes+j, 4);
			symTransform_4bytes(buf.byte);
			daBuf[i] = buf.ivalue;
		}
		free(bytes);
		return daBuf;
	}
}

uint32_t *readUInt32Data(char *srcFilePath, size_t *nbEle, int *status)
{
	int state = SZ_SCES;
	if(dataEndianType==sysEndianType)
	{
		uint32_t *daBuf = readUInt32Data_systemEndian(srcFilePath, nbEle, &state);
		*status = state;
		return daBuf;
	}
	else
	{
		size_t i,j;

		size_t byteLength;
		unsigned char* bytes = readByteData(srcFilePath, &byteLength, &state);
		if(state == SZ_FERR)
		{
			*status = SZ_FERR;
			return NULL;
		}
		uint32_t *daBuf = (uint32_t *)malloc(byteLength);
		*nbEle = byteLength/4;

		lint32 buf;
		for(i = 0;i<*nbEle;i++)
		{
			j = i << 2; //*4
			memcpy(buf.byte, bytes+j, 4);
			symTransform_4bytes(buf.byte);
			daBuf[i] = buf.uivalue;
		}
		free(bytes);
		return daBuf;
	}
}

int64_t *readInt64Data(char *srcFilePath, size_t *nbEle, int *status)
{
	int state = SZ_SCES;
	if(dataEndianType==sysEndianType)
	{
		int64_t *daBuf = readInt64Data_systemEndian(srcFilePath, nbEle, &state);
		*status = state;
		return daBuf;
	}
	else
	{
		size_t i,j;

		size_t byteLength;
		unsigned char* bytes = readByteData(srcFilePath, &byteLength, &state);
		if(state == SZ_FERR)
		{
			*status = SZ_FERR;
			return NULL;
		}
		int64_t *daBuf = (int64_t *)malloc(byteLength);
		*nbEle = byteLength/8;

		lint64 buf;
		for(i = 0;i<*nbEle;i++)
		{
			j = i << 3; //*8
			memcpy(buf.byte, bytes+j, 8);
			symTransform_8bytes(buf.byte);
			daBuf[i] = buf.lvalue;
		}
		free(bytes);
		return daBuf;
	}
}

uint64_t *readUInt64Data(char *srcFilePath, size_t *nbEle, int *status)
{
	int state = SZ_SCES;
	if(dataEndianType==sysEndianType)
	{
		uint64_t *daBuf = readUInt64Data_systemEndian(srcFilePath, nbEle, &state);
		*status = state;
		return daBuf;
	}
	else
	{
		size_t i,j;

		size_t byteLength;
		unsigned char* bytes = readByteData(srcFilePath, &byteLength, &state);
		if(state == SZ_FERR)
		{
			*status = SZ_FERR;
			return NULL;
		}
		uint64_t *daBuf = (uint64_t *)malloc(byteLength);
		*nbEle = byteLength/8;

		lint64 buf;
		for(i = 0;i<*nbEle;i++)
		{
			j = i << 3; //*8
			memcpy(buf.byte, bytes+j, 8);
			symTransform_8bytes(buf.byte);
			daBuf[i] = buf.ulvalue;
		}
		free(bytes);
		return daBuf;
	}
}


float *readFloatData(char *srcFilePath, size_t *nbEle, int *status)
{
	int state = SZ_SCES;
	if(dataEndianType==sysEndianType)
	{
		float *daBuf = readFloatData_systemEndian(srcFilePath, nbEle, &state);
		*status = state;
		return daBuf;
	}
	else
	{
		size_t i,j;
		
		size_t byteLength;
		unsigned char* bytes = readByteData(srcFilePath, &byteLength, &state);
		if(state == SZ_FERR)
		{
			*status = SZ_FERR;
			return NULL;
		}
		float *daBuf = (float *)malloc(byteLength);
		*nbEle = byteLength/4;
		
		lfloat buf;
		for(i = 0;i<*nbEle;i++)
		{
			j = i*4;
			memcpy(buf.byte, bytes+j, 4);
			symTransform_4bytes(buf.byte);
			daBuf[i] = buf.value;
		}
		free(bytes);
		return daBuf;
	}
}

double *readDoubleData_systemEndian(char *srcFilePath, size_t *nbEle, int *status)
{
	size_t inSize;
	FILE *pFile = fopen(srcFilePath, "rb");
    if (pFile == NULL)
    {
        printf("Failed to open input file. 1\n");
        *status = SZ_FERR;
        return NULL;
    }
	fseek(pFile, 0, SEEK_END);
    inSize = ftell(pFile);
    *nbEle = inSize/8; //only support double in this version
    fclose(pFile);
    
    double *daBuf = (double *)malloc(inSize);
    
    pFile = fopen(srcFilePath, "rb");
    if (pFile == NULL)
    {
        printf("Failed to open input file. 2\n");
        *status = SZ_FERR;
        return NULL;
    }
    fread(daBuf, 8, *nbEle, pFile);
    fclose(pFile);
    *status = SZ_SCES;
    return daBuf;
}


int8_t *readInt8Data_systemEndian(char *srcFilePath, size_t *nbEle, int *status)
{
	size_t inSize;
	FILE *pFile = fopen(srcFilePath, "rb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 1\n");
		*status = SZ_FERR;
		return NULL;
	}
	fseek(pFile, 0, SEEK_END);
	inSize = ftell(pFile);
	*nbEle = inSize;
	fclose(pFile);

	if(inSize<=0)
	{
		printf("Error: input file is wrong!\n");
		*status = SZ_FERR;
	}

	int8_t *daBuf = (int8_t *)malloc(inSize);

	pFile = fopen(srcFilePath, "rb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 2\n");
		*status = SZ_FERR;
		return NULL;
	}
	fread(daBuf, 1, *nbEle, pFile);
	fclose(pFile);
	*status = SZ_SCES;
	return daBuf;
}


int16_t *readInt16Data_systemEndian(char *srcFilePath, size_t *nbEle, int *status)
{
	size_t inSize;
	FILE *pFile = fopen(srcFilePath, "rb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 1\n");
		*status = SZ_FERR;
		return NULL;
	}
	fseek(pFile, 0, SEEK_END);
	inSize = ftell(pFile);
	*nbEle = inSize/2; 
	fclose(pFile);

	if(inSize<=0)
	{
		printf("Error: input file is wrong!\n");
		*status = SZ_FERR;
	}

	int16_t *daBuf = (int16_t *)malloc(inSize);

	pFile = fopen(srcFilePath, "rb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 2\n");
		*status = SZ_FERR;
		return NULL;
	}
	fread(daBuf, 2, *nbEle, pFile);
	fclose(pFile);
	*status = SZ_SCES;
	return daBuf;	
}

uint16_t *readUInt16Data_systemEndian(char *srcFilePath, size_t *nbEle, int *status)
{
	size_t inSize;
	FILE *pFile = fopen(srcFilePath, "rb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 1\n");
		*status = SZ_FERR;
		return NULL;
	}
	fseek(pFile, 0, SEEK_END);
	inSize = ftell(pFile);
	*nbEle = inSize/2; 
	fclose(pFile);

	if(inSize<=0)
	{
		printf("Error: input file is wrong!\n");
		*status = SZ_FERR;
	}

	uint16_t *daBuf = (uint16_t *)malloc(inSize);

	pFile = fopen(srcFilePath, "rb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 2\n");
		*status = SZ_FERR;
		return NULL;
	}
	fread(daBuf, 2, *nbEle, pFile);
	fclose(pFile);
	*status = SZ_SCES;
	return daBuf;	
}

int32_t *readInt32Data_systemEndian(char *srcFilePath, size_t *nbEle, int *status)
{
	size_t inSize;
	FILE *pFile = fopen(srcFilePath, "rb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 1\n");
		*status = SZ_FERR;
		return NULL;
	}
	fseek(pFile, 0, SEEK_END);
	inSize = ftell(pFile);
	*nbEle = inSize/4; 
	fclose(pFile);

	if(inSize<=0)
	{
		printf("Error: input file is wrong!\n");
		*status = SZ_FERR;
	}

	int32_t *daBuf = (int32_t *)malloc(inSize);

	pFile = fopen(srcFilePath, "rb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 2\n");
		*status = SZ_FERR;
		return NULL;
	}
	fread(daBuf, 4, *nbEle, pFile);
	fclose(pFile);
	*status = SZ_SCES;
	return daBuf;	
}

uint32_t *readUInt32Data_systemEndian(char *srcFilePath, size_t *nbEle, int *status)
{
	size_t inSize;
	FILE *pFile = fopen(srcFilePath, "rb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 1\n");
		*status = SZ_FERR;
		return NULL;
	}
	fseek(pFile, 0, SEEK_END);
	inSize = ftell(pFile);
	*nbEle = inSize/4; 
	fclose(pFile);

	if(inSize<=0)
	{
		printf("Error: input file is wrong!\n");
		*status = SZ_FERR;
	}

	uint32_t *daBuf = (uint32_t *)malloc(inSize);

	pFile = fopen(srcFilePath, "rb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 2\n");
		*status = SZ_FERR;
		return NULL;
	}
	fread(daBuf, 4, *nbEle, pFile);
	fclose(pFile);
	*status = SZ_SCES;
	return daBuf;	
}

int64_t *readInt64Data_systemEndian(char *srcFilePath, size_t *nbEle, int *status)
{
	size_t inSize;
	FILE *pFile = fopen(srcFilePath, "rb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 1\n");
		*status = SZ_FERR;
		return NULL;
	}
	fseek(pFile, 0, SEEK_END);
	inSize = ftell(pFile);
	*nbEle = inSize/8; 
	fclose(pFile);

	if(inSize<=0)
	{
		printf("Error: input file is wrong!\n");
		*status = SZ_FERR;
	}

	int64_t *daBuf = (int64_t *)malloc(inSize);

	pFile = fopen(srcFilePath, "rb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 2\n");
		*status = SZ_FERR;
		return NULL;
	}
	fread(daBuf, 8, *nbEle, pFile);
	fclose(pFile);
	*status = SZ_SCES;
	return daBuf;
}

uint64_t *readUInt64Data_systemEndian(char *srcFilePath, size_t *nbEle, int *status)
{
	size_t inSize;
	FILE *pFile = fopen(srcFilePath, "rb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 1\n");
		*status = SZ_FERR;
		return NULL;
	}
	fseek(pFile, 0, SEEK_END);
	inSize = ftell(pFile);
	*nbEle = inSize/8; 
	fclose(pFile);

	if(inSize<=0)
	{
		printf("Error: input file is wrong!\n");
		*status = SZ_FERR;
	}

	uint64_t *daBuf = (uint64_t *)malloc(inSize);

	pFile = fopen(srcFilePath, "rb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 2\n");
		*status = SZ_FERR;
		return NULL;
	}
	fread(daBuf, 8, *nbEle, pFile);
	fclose(pFile);
	*status = SZ_SCES;
	return daBuf;
}

float *readFloatData_systemEndian(char *srcFilePath, size_t *nbEle, int *status)
{
	size_t inSize;
	FILE *pFile = fopen(srcFilePath, "rb");
    if (pFile == NULL)
    {
        printf("Failed to open input file. 1\n");
        *status = SZ_FERR;
        return NULL;
    }
	fseek(pFile, 0, SEEK_END);
    inSize = ftell(pFile);
    *nbEle = inSize/4; 
    fclose(pFile);
    
    if(inSize<=0)
    {
		printf("Error: input file is wrong!\n");
		*status = SZ_FERR;
	}
    
    float *daBuf = (float *)malloc(inSize);
    
    pFile = fopen(srcFilePath, "rb");
    if (pFile == NULL)
    {
        printf("Failed to open input file. 2\n");
        *status = SZ_FERR;
        return NULL;
    }
    fread(daBuf, 4, *nbEle, pFile);
    fclose(pFile);
    *status = SZ_SCES;
    return daBuf;
}

void writeByteData(unsigned char *bytes, size_t byteLength, char *tgtFilePath, int *status)
{
	FILE *pFile = fopen(tgtFilePath, "wb");
    if (pFile == NULL)
    {
        printf("Failed to open input file. 3\n");
        *status = SZ_FERR;
        return;
    }
    
    fwrite(bytes, 1, byteLength, pFile); //write outSize bytes
    fclose(pFile);
    *status = SZ_SCES;
}

void writeDoubleData(double *data, size_t nbEle, char *tgtFilePath, int *status)
{
	size_t i = 0;
	char s[64];
	FILE *pFile = fopen(tgtFilePath, "wb");
    if (pFile == NULL)
    {
        printf("Failed to open input file. 3\n");
        *status = SZ_FERR;
        return;
    }
    
    for(i = 0;i<nbEle;i++)
	{
		sprintf(s,"%.20G\n",data[i]);
		fputs(s, pFile);
	}
    
    fclose(pFile);
    *status = SZ_SCES;
}

void writeFloatData(float *data, size_t nbEle, char *tgtFilePath, int *status)
{
	size_t i = 0;
	char s[64];
	FILE *pFile = fopen(tgtFilePath, "wb");
    if (pFile == NULL)
    {
        printf("Failed to open input file. 3\n");
        *status = SZ_FERR;
        return;
    }
   
    for(i = 0;i<nbEle;i++)
	{
		//printf("i=%d\n",i);
		//printf("data[i]=%f\n",data[i]);
		sprintf(s,"%.30G\n",data[i]);
		fputs(s, pFile);
	}
    
    fclose(pFile);
    *status = SZ_SCES;
}

void writeDataSZ(void *data, int dataType, size_t nbEle, char *tgtFilePath, int *status)
{
	int state = SZ_SCES;
	if(dataType == SZ_FLOAT)
	{
		float* dataArray = (float *)data;
		writeFloatData(dataArray, nbEle, tgtFilePath, &state);
	}
	else if(dataType == SZ_DOUBLE)
	{
		double* dataArray = (double *)data;
		writeDoubleData(dataArray, nbEle, tgtFilePath, &state);	
	}
	else
	{
		printf("Error: data type cannot be the types other than SZ_FLOAT or SZ_DOUBLE\n");
		*status = SZ_TERR; //wrong type
		return;
	}
	*status = state;
}

void writeFloatData_inBytes(float *data, size_t nbEle, char* tgtFilePath, int *status)
{
	size_t i = 0; 
	int state = SZ_SCES;
	lfloat buf;
	unsigned char* bytes = (unsigned char*)malloc(nbEle*sizeof(float));
	for(i=0;i<nbEle;i++)
	{
		buf.value = data[i];
		bytes[i*4+0] = buf.byte[0];
		bytes[i*4+1] = buf.byte[1];
		bytes[i*4+2] = buf.byte[2];
		bytes[i*4+3] = buf.byte[3];					
	}

	size_t byteLength = nbEle*sizeof(float);
	writeByteData(bytes, byteLength, tgtFilePath, &state);
	free(bytes);
	*status = state;
}

void writeDoubleData_inBytes(double *data, size_t nbEle, char* tgtFilePath, int *status)
{
	size_t i = 0, index = 0; 
	int state = SZ_SCES;
	ldouble buf;
	unsigned char* bytes = (unsigned char*)malloc(nbEle*sizeof(double));
	for(i=0;i<nbEle;i++)
	{
		index = i*8;
		buf.value = data[i];
		bytes[index+0] = buf.byte[0];
		bytes[index+1] = buf.byte[1];
		bytes[index+2] = buf.byte[2];
		bytes[index+3] = buf.byte[3];
		bytes[index+4] = buf.byte[4];
		bytes[index+5] = buf.byte[5];
		bytes[index+6] = buf.byte[6];
		bytes[index+7] = buf.byte[7];
	}

	size_t byteLength = nbEle*sizeof(double);
	writeByteData(bytes, byteLength, tgtFilePath, &state);
	free(bytes);
	*status = state;
}

void writeShortData_inBytes(short *states, size_t stateLength, char *tgtFilePath, int *status)
{
	int state = SZ_SCES;
	size_t byteLength = stateLength*2;
	unsigned char* bytes = (unsigned char*)malloc(byteLength*sizeof(char));
	convertShortArrayToBytes(states, stateLength, bytes);
	writeByteData(bytes, byteLength, tgtFilePath, &state);
	free(bytes);
	*status = state;
}

void writeUShortData_inBytes(unsigned short *states, size_t stateLength, char *tgtFilePath, int *status)
{
	int state = SZ_SCES;
	size_t byteLength = stateLength*2;
	unsigned char* bytes = (unsigned char*)malloc(byteLength*sizeof(char));
	convertUShortArrayToBytes(states, stateLength, bytes);
	writeByteData(bytes, byteLength, tgtFilePath, &state);
	free(bytes);
	*status = state;
}

void writeIntData_inBytes(int *states, size_t stateLength, char *tgtFilePath, int *status)
{
	int state = SZ_SCES;
	size_t byteLength = stateLength*4;
	unsigned char* bytes = (unsigned char*)malloc(byteLength*sizeof(char));
	convertIntArrayToBytes(states, stateLength, bytes);
	writeByteData(bytes, byteLength, tgtFilePath, &state);
	free(bytes);
	*status = state;
}

void writeUIntData_inBytes(unsigned int *states, size_t stateLength, char *tgtFilePath, int *status)
{
	int state = SZ_SCES;
	size_t byteLength = stateLength*4;
	unsigned char* bytes = (unsigned char*)malloc(byteLength*sizeof(char));
	convertUIntArrayToBytes(states, stateLength, bytes);
	writeByteData(bytes, byteLength, tgtFilePath, &state);
	free(bytes);
	*status = state;
}

void writeLongData_inBytes(int64_t *states, size_t stateLength, char *tgtFilePath, int *status)
{
	int state = SZ_SCES;
	size_t byteLength = stateLength*8;
	unsigned char* bytes = (unsigned char*)malloc(byteLength*sizeof(char));
	convertLongArrayToBytes(states, stateLength, bytes);
	writeByteData(bytes, byteLength, tgtFilePath, &state);
	free(bytes);
	*status = state;
}

void writeULongData_inBytes(uint64_t *states, size_t stateLength, char *tgtFilePath, int *status)
{
	int state = SZ_SCES;
	size_t byteLength = stateLength*8;
	unsigned char* bytes = (unsigned char*)malloc(byteLength*sizeof(char));
	convertULongArrayToBytes(states, stateLength, bytes);
	writeByteData(bytes, byteLength, tgtFilePath, &state);
	free(bytes);
	*status = state;
}

unsigned short* readShortData(char *srcFilePath, size_t *dataLength, int *status)
{
	size_t byteLength = 0; 
	int state = SZ_SCES;
	unsigned char * bytes = readByteData(srcFilePath, &byteLength, &state);
	*dataLength = byteLength/2;
	unsigned short* states = convertByteDataToUShortArray(bytes, byteLength);
	free(bytes);
	*status = state;
	return states;
}

void writeStrings(int nbStr, char *str[], char *tgtFilePath, int *status)
{
	size_t i = 0;
	char s[256];
	FILE *pFile = fopen(tgtFilePath, "wb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 3\n");
		*status = SZ_FERR;
		return;
	}

	for(i = 0;i<nbStr;i++)
	{
		sprintf(s,"%s\n",str[i]);
		fputs(s, pFile);
	}

	fclose(pFile);
	*status = SZ_SCES;
}

/*
//@deprecated
//binToPFM_float is to convert the floating-point data to PFM supported by Jpeg XT
//But wrong version!
//In order to do the conversion, we need to use https://github.com/thorfdbg/difftest_ng according to Thomas Richter.


void convertToPFM_float(float *data, size_t r5, size_t r4, size_t r3, size_t r2, size_t r1, int endianType, char *tgtFilePath, int *status)
{
	size_t i, nbEle = computeDataLength(r5, r4, r3, r2, r1);
	int dim = computeDimension(r5, r4, r3, r2, r1);
	
	FILE *pFile = fopen(tgtFilePath, "wb");
	if (pFile == NULL)
	{
		printf("Failed to open input file. 3\n");
		*status = SZ_NSCS;
		return;
	}	
	fputs("PF\n", pFile);
	char strBuf[256];
	switch(dim)
	{
	case 1: 
		sprintf(strBuf, "%zu\n", r1);
		break;
	case 2:
		sprintf(strBuf, "%zu %zu\n", r1, r2);
		break;
	case 3:
		sprintf(strBuf, "%zu %zu %zu\n", r1, r2, r3);
		break;
	case 4:
		sprintf(strBuf, "%zu %zu %zu %zu\n", r1, r2, r3, r4);
		break;
	case 5:
		sprintf(strBuf, "%zu %zu %zu %zu %zu\n", r1, r2, r3, r4, r5);
		break;
	}
	fputs(strBuf, pFile);
	if(endianType==LITTLE_ENDIAN)
		fputs("-1.0\n", pFile);
	else
		fputs("1.0\n", pFile);

	size_t byteLength = nbEle*sizeof(float);	
	lfloat buf;	
	unsigned char* bytes = (unsigned char*)malloc(byteLength);
	for(i=0;i<nbEle;i++)
	{
		buf.value = data[i];
		bytes[i*4+0] = buf.byte[0];
		bytes[i*4+1] = buf.byte[1];
		bytes[i*4+2] = buf.byte[2];
		bytes[i*4+3] = buf.byte[3];
	}
	
	fwrite(bytes, 1, byteLength, pFile); //write outSize bytes
	fclose(pFile);
	
	free(bytes);
	*status = SZ_SCES;
}*/
