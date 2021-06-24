/**
 *  @file defines.h
 *  @author Sheng Di
 *  @date July, 2019
 *  @brief Header file for the dataCompression.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZ_DEFINES_H
#define _SZ_DEFINES_H

#define SZ_VERNUM 0x0200
#define SZ_VER_MAJOR 2
#define SZ_VER_MINOR 1
#define SZ_VER_BUILD 12
#define SZ_VER_REVISION 0

#define PASTRI 103
#define HZ 102 //deprecated
#define SZ 101
#define SZ_Transpose 104

//prediction mode of temporal dimension based compression
#define SZ_PREVIOUS_VALUE_ESTIMATE 0

#define MIN_NUM_OF_ELEMENTS 20 //if the # elements <= 20, skip the compression

#define ABS 0
#define REL 1
#define VR_REL 1  //alternative name to REL
#define ABS_AND_REL 2
#define ABS_OR_REL 3
#define PSNR 4
#define NORM 5

#define PW_REL 10
#define ABS_AND_PW_REL 11
#define ABS_OR_PW_REL 12
#define REL_AND_PW_REL 13
#define REL_OR_PW_REL 14

#define SZ_FLOAT 0
#define SZ_DOUBLE 1
#define SZ_UINT8 2
#define SZ_INT8 3
#define SZ_UINT16 4
#define SZ_INT16 5
#define SZ_UINT32 6
#define SZ_INT32 7
#define SZ_UINT64 8
#define SZ_INT64 9

#define LITTLE_ENDIAN_DATA 0 //refers to the endian type of the data read from the disk
#define BIG_ENDIAN_DATA 1 //big_endian (ppc, max, etc.) ; little_endian (x86, x64, etc.)

#define LITTLE_ENDIAN_SYSTEM 0 //refers to the endian type of the system
#define BIG_ENDIAN_SYSTEM 1

#define DynArrayInitLen 1024

#define MIN_ZLIB_DEC_ALLOMEM_BYTES 1000000

//#define maxRangeRadius 32768
//#define maxRangeRadius 1048576//131072

#define SZ_BEST_SPEED 0
#define SZ_BEST_COMPRESSION 1
#define SZ_DEFAULT_COMPRESSION 2
#define SZ_TEMPORAL_COMPRESSION 3

#define SZ_NO_REGRESSION 0
#define SZ_WITH_LINEAR_REGRESSION 1

#define SZ_PWR_MIN_TYPE 0
#define SZ_PWR_AVG_TYPE 1
#define SZ_PWR_MAX_TYPE 2

#define SZ_FORCE_SNAPSHOT_COMPRESSION 0
#define SZ_FORCE_TEMPORAL_COMPRESSION 1
#define SZ_PERIO_TEMPORAL_COMPRESSION 2

//SUCCESS returning status
#define SZ_SCES 0  //successful
#define SZ_NSCS -1 //Not successful
#define SZ_FERR -2 //Failed to open input file
#define SZ_TERR -3 //wrong data type (should be only float or double)
#define SZ_DERR -4 //dimension error
#define SZ_MERR -5 //sz_mode error
#define SZ_BERR -6 //bound-mode error (should be only ABS, REL, ABS_AND_REL, ABS_OR_REL, or PW_REL)

#define SZ_MAINTAIN_VAR_DATA 0
#define SZ_DESTROY_WHOLE_VARSET 1

#define GROUP_COUNT 16 //2^{16}=65536
	
#define MetaDataByteLength 28
#define MetaDataByteLength_double 36 //meta data length for double type
	
#define numOfBufferedSteps 1 //the number of time steps in the buffer	


#define GZIP_COMPRESSOR 0 //i.e., ZLIB_COMPRSSOR
#define ZSTD_COMPRESSOR 1

#endif /* _SZ_DEFINES_H */
