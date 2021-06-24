/**
 *  @file TightDataPointStorageI.h
 *  @author Sheng Di and Dingwen Tao
 *  @date Aug, 2017
 *  @brief Header file for the tight data point storage (TDPS).
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _TightDataPointStorageI_H
#define _TightDataPointStorageI_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h> 

typedef struct TightDataPointStorageI
{
	size_t dataSeriesLength;
	int allSameData;
	double realPrecision; //it's used as the pwrErrBoundRatio when errBoundMode==PW_REL
	size_t exactDataNum;
	long minValue;
	int exactByteSize;
	int dataTypeSize; //the size of data type, e.g., it's 4 when data type is int32_t
	
	int stateNum;
	int allNodes;
	
	unsigned char* typeArray; //its size is dataSeriesLength/4 (or xxx/4+1) 
	size_t typeArray_size;
	
	unsigned char* exactDataBytes;
	size_t exactDataBytes_size;
	
	unsigned int intervals; //quantization_intervals
	
	unsigned char isLossless; //a mark to denote whether it's lossless compression (1 is yes, 0 is no)

} TightDataPointStorageI;

int computeRightShiftBits(int exactByteSize, int dataType);
int convertDataTypeSizeCode(int dataTypeSizeCode);
int convertDataTypeSize(int dataTypeSize);

void new_TightDataPointStorageI_Empty(TightDataPointStorageI **self);
int new_TightDataPointStorageI_fromFlatBytes(TightDataPointStorageI **self, unsigned char* flatBytes, size_t flatBytesLength);
void new_TightDataPointStorageI(TightDataPointStorageI **self,
		size_t dataSeriesLength, size_t exactDataNum, int byteSize, 
		int* type, unsigned char* exactDataBytes, size_t exactDataBytes_size,
		double realPrecision, long minValue, int intervals, int dataType);

void convertTDPStoBytes_int(TightDataPointStorageI* tdps, unsigned char* bytes, unsigned char sameByte);
void convertTDPStoFlatBytes_int(TightDataPointStorageI *tdps, unsigned char** bytes, size_t *size);
void convertTDPStoFlatBytes_int_args(TightDataPointStorageI *tdps, unsigned char* bytes, size_t *size);
void free_TightDataPointStorageI(TightDataPointStorageI *tdps);
void free_TightDataPointStorageI2(TightDataPointStorageI *tdps);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _TightDataPointStorageI_H  ----- */
