/**
 *  @file szf.h
 *  @author Sheng Di
 *  @date July, 2017
 *  @brief Header file for the szf.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _SZF_H
#define _SZF_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>

//szf.c
void sz_init_c_(char *configFile,int *len,int *ierr);
void sz_finalize_c_();
void SZ_writeData_inBinary_d1_Float_(float* data, char *fileName, int *len);
void sz_compress_d1_float_(float* data, unsigned char *bytes, size_t *outSize, size_t *r1);
void sz_compress_d1_float_rev_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1);
void sz_compress_d2_float_(float* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2);
void sz_compress_d2_float_rev_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2);
void sz_compress_d3_float_(float* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3);
void sz_compress_d3_float_rev_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3);
void sz_compress_d4_float_(float* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4);
void sz_compress_d4_float_rev_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4);
void sz_compress_d5_float_(float* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5);
void sz_compress_d5_float_rev_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5);

void sz_compress_d1_double_(double* data, unsigned char *bytes, size_t *outSize, size_t *r1);
void sz_compress_d1_double_rev_(double* data, double *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1);
void sz_compress_d2_double_(double* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2);
void sz_compress_d2_double_rev_(double* data, double *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2);
void sz_compress_d3_double_(double* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3);
void sz_compress_d3_double_rev_(double* data, double *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3);
void sz_compress_d4_double_(double* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4);
void sz_compress_d4_double_rev_(double* data, double *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4);
void sz_compress_d5_double_(double* data, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5);
void sz_compress_d5_double_rev_(double* data, double *reservedValue, unsigned char *bytes, size_t *outSize, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5);

void sz_compress_d1_float_args_(float* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1);
void sz_compress_d2_float_args_(float* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2);
void sz_compress_d3_float_args_(float* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3);
void sz_compress_d4_float_args_(float* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4);
void sz_compress_d5_float_args_(float* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5);
void sz_compress_d1_double_args_(double* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1);
void sz_compress_d2_double_args_(double* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2);
void sz_compress_d3_double_args_(double* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3);
void sz_compress_d4_double_args_(double* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4);
void sz_compress_d5_double_args_(double* data, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5);

void sz_compress_d1_float_rev_args_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1);
void sz_compress_d2_float_rev_args_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2);
void sz_compress_d3_float_rev_args_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3);
void sz_compress_d4_float_rev_args_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4);
void sz_compress_d5_float_rev_args_(float* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5);
void sz_compress_d1_double_rev_args_(double* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1);
void sz_compress_d2_double_rev_args_(double* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2);
void sz_compress_d3_double_rev_args_(double* data, float *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3);
void sz_compress_d4_double_rev_args_(double* data, double *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4);
void sz_compress_d5_double_rev_args_(double* data, double *reservedValue, unsigned char *bytes, size_t *outSize, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5);

void sz_decompress_d1_float_(unsigned char *bytes, size_t *byteLength, float *data, size_t *r1);
void sz_decompress_d2_float_(unsigned char *bytes, size_t *byteLength, float *data, size_t *r1, size_t *r2);
void sz_decompress_d3_float_(unsigned char *bytes, size_t *byteLength, float *data, size_t *r1, size_t *r2, size_t *r3);
void sz_decompress_d4_float_(unsigned char *bytes, size_t *byteLength, float *data, size_t *r1, size_t *r2, size_t *r3, size_t *r4);
void sz_decompress_d5_float_(unsigned char *bytes, size_t *byteLength, float *data, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5);
void sz_decompress_d1_double_(unsigned char *bytes, size_t *byteLength, double *data, size_t *r1);
void sz_decompress_d2_double_(unsigned char *bytes, size_t *byteLength, double *data, size_t *r1, size_t *r2);
void sz_decompress_d3_double_(unsigned char *bytes, size_t *byteLength, double *data, size_t *r1, size_t *r2, size_t *r3);
void sz_decompress_d4_double_(unsigned char *bytes, size_t *byteLength, double *data, size_t *r1, size_t *r2, size_t *r3, size_t *r4);
void sz_decompress_d5_double_(unsigned char *bytes, size_t *byteLength, double *data, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5);

void sz_batchaddVar_d1_float_(int var_id, char* varName, int *len, float* data, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1);
void sz_batchaddvar_d2_float_(int var_id, char* varName, int *len, float* data, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2);
void sz_batchaddvar_d3_float_(int var_id, char* varName, int *len, float* data, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3);
void sz_batchaddvar_d4_float_(int var_id, char* varName, int *len, float* data, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4);
void sz_batchaddvar_d5_float_(int var_id, char* varName, int *len, float* data, int *errBoundMode, float *absErrBound, float *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5);
void sz_batchaddvar_d1_double_(int var_id, char* varName, int *len, double* data, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1);
void sz_batchaddvar_d2_double_(int var_id, char* varName, int *len, double* data, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2);
void sz_batchaddvar_d3_double_(int var_id, char* varName, int *len, double* data, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3);
void sz_batchaddvar_d4_double_(int var_id, char* varName, int *len, double* data, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4);
void sz_batchaddvar_d5_double_(int var_id, char* varName, int *len, double* data, int *errBoundMode, double *absErrBound, double *relBoundRatio, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5);
void sz_batchdelvar_c_(char* varName, int *len, int *errState);
void sz_batch_compress_c_(unsigned char* bytes, size_t *outSize);
void sz_batch_decompress_c_(unsigned char* bytes, size_t *byteLength, int *ierr);
void sz_getvardim_c_(char* varName, int *len, int *dim, size_t *r1, size_t *r2, size_t *r3, size_t *r4, size_t *r5);
void compute_total_batch_size_c_(size_t *totalSize);
void sz_getvardata_float_(char* varName, int *len, float* data);
void sz_getvardata_double_(char* varName, int *len, double* data);
void sz_freevarset_c_(int *mode);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZF_H  ----- */

