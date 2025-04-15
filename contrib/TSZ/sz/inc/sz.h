/**
 *  @file sz.h
 *  @author Sheng Di
 *  @date April, 2015
 *  @brief Header file for the whole compressor.
 *  (C) 2015 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZ_H
#define _SZ_H

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>          /* For time(), in seconds */
#include "pub.h"
#include "CompressElement.h"
#include "DynamicByteArray.h"
#include "DynamicIntArray.h"
#include "TightDataPointStorageD.h"
#include "TightDataPointStorageF.h"
#include "conf.h"
#include "dataCompression.h"
#include "ByteToolkit.h"
#include "TypeManager.h"
#include "sz_float.h"
#include "sz_double.h"
#include "szd_float.h"
#include "szd_double.h"
#include "utility.h"

#ifdef _WIN32
#define PATH_SEPARATOR ';'
#define INLINE
#else
#define PATH_SEPARATOR ':'
#define INLINE inline
#endif


#ifdef __cplusplus
extern "C" {
#endif

void cost_start();
double cost_end(const char* tag);
void show_rate( int in_len, int out_len);

//typedef char int8_t;
//typedef unsigned char uint8_t;
//typedef short int16_t;
//typedef unsigned short uint16_t;
//typedef int int32_t;
//typedef unsigned int uint32_t;
//typedef long int64_t;
//typedef unsigned long uint64_t;

#include "defines.h"
	
//Note: the following setting should be consistent with stateNum in Huffman.h
//#define intvCapacity 65536
//#define intvRadius 32768
//#define intvCapacity 131072
//#define intvRadius 65536

#define SZ_COMPUTE_1D_NUMBER_OF_BLOCKS( COUNT, NUM_BLOCKS, BLOCK_SIZE ) \
    if (COUNT <= BLOCK_SIZE){                  \
        NUM_BLOCKS = 1;             \
    }                                   \
    else{                               \
        NUM_BLOCKS = COUNT / BLOCK_SIZE;       \
    }                                   \

#define SZ_COMPUTE_2D_NUMBER_OF_BLOCKS( COUNT, NUM_BLOCKS, BLOCK_SIZE ) \
    if (COUNT <= BLOCK_SIZE){                   \
        NUM_BLOCKS = 1;             \
    }                                   \
    else{                               \
        NUM_BLOCKS = COUNT / BLOCK_SIZE;        \
    }                                   \

#define SZ_COMPUTE_3D_NUMBER_OF_BLOCKS( COUNT, NUM_BLOCKS, BLOCK_SIZE ) \
    if (COUNT <= BLOCK_SIZE){                   \
        NUM_BLOCKS = 1;             \
    }                                   \
    else{                               \
        NUM_BLOCKS = COUNT / BLOCK_SIZE;        \
    }                                   \

#define SZ_COMPUTE_BLOCKCOUNT( COUNT, NUM_BLOCKS, SPLIT_INDEX,       \
                                       EARLY_BLOCK_COUNT, LATE_BLOCK_COUNT ) \
    EARLY_BLOCK_COUNT = LATE_BLOCK_COUNT = COUNT / NUM_BLOCKS;               \
    SPLIT_INDEX = COUNT % NUM_BLOCKS;                                        \
    if (0 != SPLIT_INDEX) {                                                  \
        EARLY_BLOCK_COUNT = EARLY_BLOCK_COUNT + 1;                           \
    }                                                                        \

//typedef unsigned long unsigned long;
//typedef unsigned int uint;

typedef union lint16
{
	unsigned short usvalue;
	short svalue;
	unsigned char byte[2];
} lint16;

typedef union lint32
{
	int ivalue;
	unsigned int uivalue;
	unsigned char byte[4];
} lint32;

typedef union lint64
{
	long lvalue;
	unsigned long ulvalue;
	unsigned char byte[8];
} lint64;

typedef union ldouble
{
    double value;
    unsigned long lvalue;
    unsigned char byte[8];
} ldouble;

typedef union lfloat
{
    float value;
    unsigned int ivalue;
    unsigned char byte[4];
} lfloat;


typedef struct sz_metadata
{
	unsigned char ver; //only used for checking the version by calling SZ_GetMetaData()
	int isConstant; //only used for checking if the data are constant values by calling SZ_GetMetaData()
	int isLossless; //only used for checking if the data compression was lossless, used only by calling SZ_GetMetaData()
	int sizeType; //only used for checking whether the size type is "int" or "long" in the compression, used only by calling SZ_GetMetaData()
	size_t dataSeriesLength; //# number of data points in the dataset
	int defactoNBBins; //real number of quantization bins
	struct sz_params* conf_params; //configuration parameters
} sz_metadata;


/*We use a linked list to maintain time-step meta info for time-step based compression*/
typedef struct sz_tsc_metainfo
{
	int totalNumOfSteps;
	int currentStep;
	char metadata_filename[256];
	FILE *metadata_file;
	unsigned char* bit_array; //sihuan added
	size_t intersect_size; //sihuan added
	int64_t* hist_index; //sihuan added: prestep index 

} sz_tsc_metadata;

extern unsigned char versionNumber;

//-------------------key global variables--------------
extern int dataEndianType; //*endian type of the data read from disk
extern int sysEndianType; //*sysEndianType is actually set automatically.

extern sz_params *confparams_cpr;
extern sz_exedata *exe_params;


void SZ_Finalize();
int SZ_Init(const char *configFilePath);
int SZ_Init_Params(sz_params *params);

//
//  compress output data to outData and return outSize
//
size_t SZ_compress_args(int dataType, void *data, size_t r1, unsigned char* outData, sz_params* params);


//
// decompress output data to outData and return outSize
//
size_t SZ_decompress(int dataType, unsigned char *bytes, size_t byteLength, size_t r1, unsigned char* outData);


void convertSZParamsToBytes(sz_params* params, unsigned char* result, char optQuantMode);
void convertBytesToSZParams(unsigned char* bytes, sz_params* params, sz_exedata* pde_exe);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_H  ----- */
