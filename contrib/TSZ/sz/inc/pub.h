/**
 *  @file sz.h
 *  @author Sheng Di
 *  @date April, 2015
 *  @brief Header file for the whole compressor.
 *  (C) 2015 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _PUB_H
#define _PUB_H


#include <stdio.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif


/* array meta data and compression parameters for SZ_Init_Params() */
typedef struct sz_params
{
	int dataType;
	int ifAdtFse; // 0 (ADT-FSE algorithm) or 1 (original Huffman encoding)
	unsigned int max_quant_intervals; 	//max number of quantization intervals for quantization
	unsigned int quantization_intervals; 
	unsigned int maxRangeRadius;
	int sol_ID;// it's SZ or SZ_Transpose, unless the setting is PASTRI compression mode (./configure --enable-pastri)
	int losslessCompressor;
	int sampleDistance; //2 bytes
	float predThreshold;  // 2 bytes
	int szMode; //* 0 (best speed) or 1 (better compression with Zstd/Gzip) or 3 temporal-dimension based compression
	int  errorBoundMode; //4bits (0.5byte), //SZ_ABS, REL, ABS_AND_REL, or ABS_OR_REL, PSNR, or PW_REL, PSNR
	double absErrBound; //absolute error bound for float
	double absErrBoundDouble; // for double
	double relBoundRatio; //value range based relative error bound ratio
	double psnr; //PSNR
	double normErr;
	double pw_relBoundRatio; //point-wise relative error bound
	int segment_size; //only used for 2D/3D data compression with pw_relBoundRatio (deprecated)
	int pwr_type; //only used for 2D/3D data compression with pw_relBoundRatio
	
	int protectValueRange; //0 or 1
	float fmin, fmax;
	double dmin, dmax;
	
	int snapshotCmprStep; //perform single-snapshot-based compression if time_step == snapshotCmprStep
	int predictionMode;

	int accelerate_pw_rel_compression;
	int plus_bits;
	
	int randomAccess;
	int withRegression;
	
} sz_params;

typedef struct sz_exedata
{
	char optQuantMode;	//opt Quantization (0: fixed ; 1: optimized)	
	int intvCapacity; // the number of intervals for the linear-scaling quantization
	int intvRadius;  // the number of intervals for the radius of the quantization range (intvRadius=intvCapacity/2)
	unsigned int SZ_SIZE_TYPE; //the length (# bytes) of the size_t in the system at runtime //4 or 8: sizeof(size_t) 
} sz_exedata;

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _PUB_H  ----- */
