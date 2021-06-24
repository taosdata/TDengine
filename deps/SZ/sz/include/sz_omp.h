/**
 *  @file sz_omp.h
 *  @author Xin Liang
 *  @date July, 2017
 *  @brief Header file for the sz_omp.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#ifdef _OPENMP
#include "omp.h"
#endif
#include "sz.h"

#ifndef _SZ_OMP_H
#define _SZ_OMP_H

#ifdef __cplusplus
extern "C" {
#endif

unsigned char * SZ_compress_float_1D_MDQ_openmp(float *oriData, size_t r1, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_2D_MDQ_openmp(float *oriData, size_t r1, size_t r2, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_float_3D_MDQ_openmp(float *oriData, size_t r1, size_t r2, size_t r3, float realPrecision, size_t * comp_size);

void decompressDataSeries_float_1D_openmp(float** data, size_t r1, unsigned char* comp_data);
void decompressDataSeries_float_3D_openmp(float** data, size_t r1, size_t r2, size_t r3, unsigned char* comp_data);
void decompressDataSeries_float_2D_openmp(float** data, size_t r1, size_t r2, unsigned char* comp_data);

unsigned char * SZ_compress_double_1D_MDQ_openmp(double *oriData, size_t r1, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_double_2D_MDQ_openmp(double *oriData, size_t r1, size_t r2, double realPrecision, size_t * comp_size);
unsigned char * SZ_compress_double_3D_MDQ_openmp(double *oriData, size_t r1, size_t r2, size_t r3, double realPrecision, size_t * comp_size);

void decompressDataSeries_double_1D_openmp(double** data, size_t r1, unsigned char* comp_data);
void decompressDataSeries_double_2D_openmp(double** data, size_t r1, size_t r2, unsigned char* comp_data);
void decompressDataSeries_double_3D_openmp(double** data, size_t r1, size_t r2, size_t r3, unsigned char* comp_data);

//void Huffman_init_openmp(HuffmanTree* huffmanTree, int *s, size_t length, int thread_num);
void Huffman_init_openmp(HuffmanTree* huffmanTree, int *s, size_t length, int thread_num, size_t * freq);

#ifdef __cplusplus
}
#endif

#endif /* ----- #ifndef _SZ_OMP_H  ----- */
