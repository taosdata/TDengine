/**
 *  @file szf.c
 *  @author Sheng Di
 *  @date April, 2015
 *  @brief the key C binding file to connect Fortran and C
 *  (C) 2015 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */


#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "sz.h"
#include "szf.h"

//special notice: all the function names in this file must be lower-cases!!
void sz_init_c_(char *configFile,int *len,int *ierr)
{
    int i;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=configFile[i];
    s2[*len]='\0';
 //   printf("sconfigFile=%s\n",configFile);
    *ierr = SZ_Init(s2);
}

void sz_finalize_c_()
{
	SZ_Finalize();
}

//compress with config (without args in function)
void sz_compress_d1_float_(float* data, unsigned char *bytes, size_t *outSize, size_t *r1)	
{
	unsigned char *tmp_bytes = SZ_compress(SZ_FLOAT, data, outSize, 0, 0, 0, 0, *r1);
	memcpy(bytes, tmp_bytes, *outSize);	
	free(tmp_bytes);
}

void sz_compress_d1_float_rev_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1)	
{
	unsigned char *tmp_bytes = SZ_compress_rev(SZ_FLOAT, data, reservedValue, outSize, 0, 0, 0, 0, *r1);
	memcpy(bytes, tmp_bytes, *outSize);	
	free(tmp_bytes);
}

void sz_compress_d2_float_(float* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2)
{
	unsigned char *tmp_bytes = SZ_compress(SZ_FLOAT, data, outSize, 0, 0, 0, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d2_float_rev_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2)
{
	unsigned char *tmp_bytes = SZ_compress_rev(SZ_FLOAT, data, reservedValue, outSize, 0, 0, 0, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d3_float_(float* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3)
{
	unsigned char *tmp_bytes = SZ_compress(SZ_FLOAT, data, outSize, 0, 0, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d3_float_rev_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3)
{
	unsigned char *tmp_bytes = SZ_compress_rev(SZ_FLOAT, data, reservedValue, outSize, 0, 0, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d4_float_(float* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4)
{
	unsigned char *tmp_bytes = SZ_compress(SZ_FLOAT, data, outSize, 0, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d4_float_rev_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4)
{
	unsigned char *tmp_bytes = SZ_compress_rev(SZ_FLOAT, data, reservedValue, outSize, 0, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d5_float_(float* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5)
{
	unsigned char *tmp_bytes = SZ_compress(SZ_FLOAT, data, outSize, *r5, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d5_float_rev_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5)
{
	unsigned char *tmp_bytes = SZ_compress_rev(SZ_FLOAT, data, reservedValue, outSize, *r5, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d1_double_(double* data, unsigned char *bytes, size_t *outSize, size_t *r1)
{
	unsigned char *tmp_bytes = SZ_compress(SZ_DOUBLE, data, outSize, 0, 0, 0, 0, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d1_double_rev_(double* data, double *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1)
{
	unsigned char *tmp_bytes = SZ_compress_rev(SZ_DOUBLE, data, reservedValue, outSize, 0, 0, 0, 0, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d2_double_(double* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2)
{
	unsigned char *tmp_bytes = SZ_compress(SZ_DOUBLE, data, outSize, 0, 0, 0, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d2_double_rev_(double* data, double *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2)
{
	unsigned char *tmp_bytes = SZ_compress_rev(SZ_DOUBLE, data, reservedValue, outSize, 0, 0, 0, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d3_double_(double* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3)
{
	unsigned char *tmp_bytes = SZ_compress(SZ_DOUBLE, data, outSize, 0, 0, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d3_double_rev_(double* data, double *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3)
{
	unsigned char *tmp_bytes = SZ_compress_rev(SZ_DOUBLE, data, reservedValue, outSize, 0, 0, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d4_double_(double* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4)
{
	unsigned char *tmp_bytes = SZ_compress(SZ_DOUBLE, data, outSize, 0, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d4_double_rev_(double* data, double *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4)
{
	unsigned char *tmp_bytes = SZ_compress_rev(SZ_DOUBLE, data, reservedValue, outSize, 0, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d5_double_(double* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5)
{
	unsigned char *tmp_bytes = SZ_compress(SZ_DOUBLE, data, outSize, *r5, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d5_double_rev_(double* data, double *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5)
{
	unsigned char *tmp_bytes = SZ_compress_rev(SZ_DOUBLE, data, reservedValue, outSize, *r5, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

//compress with args

void sz_compress_d1_float_args_(float* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1)
{
	unsigned char *tmp_bytes = SZ_compress_args(SZ_FLOAT, data, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, 0, 0, 0, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d2_float_args_(float* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2)
{
	unsigned char *tmp_bytes = SZ_compress_args(SZ_FLOAT, data, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, 0, 0, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d3_float_args_(float* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3)
{
	unsigned char *tmp_bytes = SZ_compress_args(SZ_FLOAT, data, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, 0, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d4_float_args_(float* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4)
{
	unsigned char *tmp_bytes = SZ_compress_args(SZ_FLOAT, data, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d5_float_args_(float* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5)
{
	unsigned char *tmp_bytes = SZ_compress_args(SZ_FLOAT, data, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, *r5, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d1_double_args_(double* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1)
{
	unsigned char *tmp_bytes = SZ_compress_args(SZ_DOUBLE, data, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, 0, 0, 0, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d2_double_args_(double* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2)
{
	unsigned char *tmp_bytes = SZ_compress_args(SZ_DOUBLE, data, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, 0, 0, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d3_double_args_(double* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3)
{
	unsigned char *tmp_bytes = SZ_compress_args(SZ_DOUBLE, data, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, 0, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d4_double_args_(double* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4)
{
	unsigned char *tmp_bytes = SZ_compress_args(SZ_DOUBLE, data, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d5_double_args_(double* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5)
{
	unsigned char *tmp_bytes = SZ_compress_args(SZ_DOUBLE, data, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, *r5, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

//--------------

void sz_compress_d1_float_rev_args_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1)
{
	unsigned char *tmp_bytes = SZ_compress_rev_args(SZ_FLOAT, data, reservedValue, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0, 0, 0, 0, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d2_float_rev_args_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2)
{
	unsigned char *tmp_bytes = SZ_compress_rev_args(SZ_FLOAT, data, reservedValue, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0, 0, 0, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d3_float_rev_args_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3)
{
	unsigned char *tmp_bytes = SZ_compress_rev_args(SZ_FLOAT, data, reservedValue, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0, 0, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d4_float_rev_args_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4)
{
	unsigned char *tmp_bytes = SZ_compress_rev_args(SZ_FLOAT, data, reservedValue, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d5_float_rev_args_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5)
{
	unsigned char *tmp_bytes = SZ_compress_rev_args(SZ_FLOAT, data, reservedValue, outSize, *errBoundMode, *absErrBound, *relBoundRatio, *r5, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d1_double_rev_args_(double* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1)
{
	unsigned char *tmp_bytes = SZ_compress_rev_args(SZ_DOUBLE, data, reservedValue, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0, 0, 0, 0, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d2_double_rev_args_(double* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2)
{
	unsigned char *tmp_bytes = SZ_compress_rev_args(SZ_DOUBLE, data, reservedValue, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0, 0, 0, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d3_double_rev_args_(double* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3)
{
	unsigned char *tmp_bytes = SZ_compress_rev_args(SZ_DOUBLE, data, reservedValue, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0, 0, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
}

void sz_compress_d4_double_rev_args_(double* data, double *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4)
{
	unsigned char *tmp_bytes = SZ_compress_rev_args(SZ_DOUBLE, data, reservedValue, outSize, *errBoundMode, *absErrBound, *relBoundRatio, 0, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

void sz_compress_d5_double_rev_args_(double* data, double *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5)
{
	unsigned char *tmp_bytes = SZ_compress_rev_args(SZ_DOUBLE, data, reservedValue, outSize, *errBoundMode, *absErrBound, *relBoundRatio, *r5, *r4, *r3, *r2, *r1);
	memcpy(bytes, tmp_bytes, *outSize);
	free(tmp_bytes);
}

//decompress

void sz_decompress_d1_float_(unsigned char *bytes, size_t *byteLength, float *data, size_t *r1)
{
	float *tmp_data = SZ_decompress(SZ_FLOAT, bytes, *byteLength, 0, 0, 0, 0, *r1);
	memcpy(data, tmp_data, (*r1)*sizeof(float));
	free(tmp_data);
}

void sz_decompress_d2_float_(unsigned char *bytes, size_t *byteLength, float *data, size_t *r1, size_t *r2)
{
	size_t r;
	float *tmp_data = SZ_decompress(SZ_FLOAT, bytes, *byteLength, 0, 0, 0, *r2, *r1);
	r=(*r1)*(*r2);
	memcpy(data, tmp_data, r*sizeof(float));
	free(tmp_data);
}

void sz_decompress_d3_float_(unsigned char *bytes, size_t *byteLength, float *data, size_t *r1, size_t *r2, size_t *r3)
{
	size_t r;
	float *tmp_data = SZ_decompress(SZ_FLOAT, bytes, *byteLength, 0, 0, *r3, *r2, *r1);
	r=(*r1)*(*r2)*(*r3);
	memcpy(data, tmp_data, r*sizeof(float));
	free(tmp_data);
}

void sz_decompress_d4_float_(unsigned char *bytes, size_t *byteLength, float *data, size_t *r1, size_t *r2, size_t *r3, size_t *r4)
{
	size_t r;
	float *tmp_data = SZ_decompress(SZ_FLOAT, bytes, *byteLength, 0, *r4, *r3, *r2, *r1);
	r=(*r1)*(*r2)*(*r3)*(*r4);
	memcpy(data, tmp_data, r*sizeof(float));
	free(tmp_data);
}

void sz_decompress_d5_float_(unsigned char *bytes, size_t *byteLength, float *data, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5)
{
	size_t r;
	float *tmp_data = SZ_decompress(SZ_FLOAT, bytes, *byteLength, *r5, *r4, *r3, *r2, *r1);
	r=(*r1)*(*r2)*(*r3)*(*r4)*(*r5);
	memcpy(data, tmp_data, r*sizeof(float));
	free(tmp_data);
}

void sz_decompress_d1_double_(unsigned char *bytes, size_t *byteLength, double *data, size_t *r1)
{
	double *tmp_data = SZ_decompress(SZ_DOUBLE, bytes, *byteLength, 0, 0, 0, 0, *r1);
	memcpy(data, tmp_data, (*r1)*sizeof(double));
	free(tmp_data);
}

void sz_decompress_d2_double_(unsigned char *bytes, size_t *byteLength, double *data, size_t *r1, size_t *r2)
{
	size_t r;
	double *tmp_data = SZ_decompress(SZ_DOUBLE, bytes, *byteLength, 0, 0, 0, *r2, *r1);
	r=(*r1)*(*r2);
	memcpy(data, tmp_data, r*sizeof(double));
	free(tmp_data);
}

void sz_decompress_d3_double_(unsigned char *bytes, size_t *byteLength, double *data, size_t *r1, size_t *r2, size_t *r3)
{
	size_t r;
	double *tmp_data = SZ_decompress(SZ_DOUBLE, bytes, *byteLength, 0, 0, *r3, *r2, *r1);
	r=(*r1)*(*r2)*(*r3);
	memcpy(data, tmp_data, r*sizeof(double));
	free(tmp_data);
}

void sz_decompress_d4_double_(unsigned char *bytes, size_t *byteLength, double *data, size_t *r1, size_t *r2, size_t *r3, size_t *r4)
{
	size_t r;
	double *tmp_data = SZ_decompress(SZ_DOUBLE, bytes, *byteLength, 0, *r4, *r3, *r2, *r1);
	r=(*r1)*(*r2)*(*r3)*(*r4);
	memcpy(data, tmp_data, r*sizeof(double));
	free(tmp_data);
}

void sz_decompress_d5_double_(unsigned char *bytes, size_t *byteLength, double *data, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5)
{
	size_t r;
	double *tmp_data = SZ_decompress(SZ_DOUBLE, bytes, *byteLength, *r5, *r4, *r3, *r2, *r1);
	r=(*r1)*(*r2)*(*r3)*(*r4)*(*r5);
	memcpy(data, tmp_data, r*sizeof(double));
	free(tmp_data);
}

//-----------------TODO: batch mode-----------
void sz_batchaddvar_d1_float_(int var_id, char* varName, int *len, float* data, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1)
{
	int i;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=varName[i];
    s2[*len]='\0';		
	SZ_batchAddVar(var_id, s2, SZ_FLOAT, data, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, 0, 0, 0, *r1);
}
void sz_batchaddvar_d2_float_(int var_id, char* varName, int *len, float* data, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2)
{
	int i;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=varName[i];
    s2[*len]='\0';		
	SZ_batchAddVar(var_id, s2, SZ_FLOAT, data, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, 0, 0, *r2, *r1);
}
void sz_batchaddvar_d3_float_(int var_id, char* varName, int *len, float* data, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3)
{
	int i;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=varName[i];
    s2[*len]='\0';		
	SZ_batchAddVar(var_id, s2, SZ_FLOAT, data, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, 0, *r3, *r2, *r1);
}
void sz_batchaddvar_d4_float_(int var_id, char* varName, int *len, float* data, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4)
{
	int i;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=varName[i];
    s2[*len]='\0';		
	SZ_batchAddVar(var_id, s2, SZ_FLOAT, data, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, *r4, *r3, *r2, *r1);
}
void sz_batchaddvar_d5_float_(int var_id, char* varName, int *len, float* data, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5)
{
	int i;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=varName[i];
    s2[*len]='\0';		
	SZ_batchAddVar(var_id, s2, SZ_FLOAT, data, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, *r5, *r4, *r3, *r2, *r1);
}
void sz_batchaddvar_d1_double_(int var_id, char* varName, int *len, double* data, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1)
{
	int i;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=varName[i];
    s2[*len]='\0';		
	SZ_batchAddVar(var_id, s2, SZ_DOUBLE, data, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, 0, 0, 0, *r1);
}
void sz_batchaddvar_d2_double_(int var_id, char* varName, int *len, double* data, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2)
{
	int i;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=varName[i];
    s2[*len]='\0';		
	SZ_batchAddVar(var_id, s2, SZ_DOUBLE, data, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, 0, 0, *r2, *r1);
}
void sz_batchaddvar_d3_double_(int var_id, char* varName, int *len, double* data, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3)
{
	int i;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=varName[i];
    s2[*len]='\0';		
	SZ_batchAddVar(var_id, s2, SZ_DOUBLE, data, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, 0, *r3, *r2, *r1);
}
void sz_batchaddvar_d4_double_(int var_id, char* varName, int *len, double* data, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4)
{
	int i;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=varName[i];
    s2[*len]='\0';		
	SZ_batchAddVar(var_id, s2, SZ_DOUBLE, data, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, 0, *r4, *r3, *r2, *r1);
}
void sz_batchaddvar_d5_double_(int var_id, char* varName, int *len, double* data, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5)
{
	int i;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=varName[i];
    s2[*len]='\0';		
	SZ_batchAddVar(var_id, s2, SZ_DOUBLE, data, *errBoundMode, *absErrBound, *relBoundRatio, 0.1, *r5, *r4, *r3, *r2, *r1);
}
void sz_batchdelvar_c_(char* varName, int *len, int *errState)
{
	int i;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=varName[i];
    s2[*len]='\0';
	*errState = SZ_batchDelVar(s2);
}

/*@deprecated*/
void sz_batch_compress_c_(unsigned char* bytes, size_t *outSize)
{
	//unsigned char* tmp_bytes = SZ_batch_compress(outSize);
	//memcpy(bytes, tmp_bytes, *outSize);
	//free(tmp_bytes);
}
/*@deprecated*/
void sz_batch_decompress_c_(unsigned char* bytes, size_t *byteLength, int *ierr)
{
	//SZ_batch_decompress(bytes, *byteLength, ierr);
}

void sz_getvardim_c_(char* varName, int *len, int *dim, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5)
{
	int i;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=varName[i];
    s2[*len]='\0';
    
    SZ_getVarData(s2, r5, r4, r3, r2, r1);
    *dim = computeDimension(*r5, *r4, *r3, *r2, *r1);
}

void compute_total_batch_size_c_(size_t *totalSize)
{
	*totalSize = compute_total_batch_size();
}

void sz_getvardata_float_(char* varName, int *len, float* data)
{
	int i;
	size_t r1, r2, r3, r4, r5;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=varName[i];
    s2[*len]='\0';	
	
	float* tmp_data = (float*)SZ_getVarData(s2, &r5, &r4, &r3, &r2, &r1);
	int size = computeDataLength(r5, r4, r3, r2, r1);
	memcpy(data, tmp_data, size*sizeof(float));
	free(tmp_data);	
}
void sz_getvardata_double_(char* varName, int *len, double* data)
{
	int i;
	size_t r1, r2, r3, r4, r5;
    char s2[*len+1];
    for(i=0;i<*len;i++)
        s2[i]=varName[i];
    s2[*len]='\0';	
    
	double* tmp_data = (double*)SZ_getVarData(s2, &r5, &r4, &r3, &r2, &r1);
	int size = computeDataLength(r5, r4, r3, r2, r1);
	memcpy(data, tmp_data, size*sizeof(double));
	//free(tmp_data);
}

void sz_freevarset_c_(int *mode)
{
	SZ_freeVarSet(*mode);
}

