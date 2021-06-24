/**
 *  @file CacheTable.h
 *  @author Xiangyu Zou, Tao Lu, Wen Xia, Xuan Wang, Weizhe Zhang, Sheng Di, Dingwen Tao
 *  @date Jan, 2019
 *  @brief Header file.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef SZ_MASTER_CACHETABLE_H
#define SZ_MASTER_CACHETABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "stdio.h"
#include "stdint.h"
#include <math.h>

extern double* g_CacheTable;
extern uint32_t * g_InverseTable;
extern uint32_t baseIndex;
extern uint32_t topIndex;
extern int bits;

int doubleGetExpo(double d);
int CacheTableGetRequiredBits(double precision, int quantization_intervals);
uint32_t CacheTableGetIndex(float value, int bits);
uint64_t CacheTableGetIndexDouble(double value, int bits);
int CacheTableIsInBoundary(uint32_t index);
void CacheTableBuild(double * table, int count, double smallest, double largest, double precision, int quantization_intervals);
uint32_t CacheTableFind(uint32_t index);
void CacheTableFree();

#ifdef __cplusplus
}
#endif

#endif //SZ_MASTER_CACHETABLE_H
