/**
 *  @file MultiLevelCacheTableWideInterval.h
 *  @author Xiangyu Zou, Tao Lu, Wen Xia, Xuan Wang, Weizhe Zhang, Sheng Di, Dingwen Tao
 *  @date Jan, 2019
 *  @brief Header file for MultiLevelCacheTableWideInterval.c.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */


#ifndef _MULTILEVELCACHETABLEWIDEINTERVAL_H
#define _MULTILEVELCACHETABLEWIDEINTERVAL_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <memory.h>
#include <stdlib.h>
#include "stdio.h"

typedef struct SubLevelTableWideInterval{
    uint64_t baseIndex;
    uint64_t topIndex;
    uint16_t* table;
    uint16_t expoIndex;
} SubLevelTableWideInterval;

typedef struct TopLevelTableWideInterval{
    uint16_t bits;
    uint16_t baseIndex;
    uint16_t topIndex;
    struct SubLevelTableWideInterval* subTables;
    double bottomBoundary;
    double topBoundary;
} TopLevelTableWideInterval;

void freeTopLevelTableWideInterval(struct TopLevelTableWideInterval* topTable);

uint16_t MLCTWI_GetExpoIndex(double value);
uint16_t MLCTWI_GetRequiredBits(double precision);
uint64_t MLCTWI_GetMantiIndex(double value, int bits);

double MLTCWI_RebuildDouble(uint16_t expo, uint64_t manti, int bits);
void MultiLevelCacheTableWideIntervalBuild(struct TopLevelTableWideInterval* topTable, double* precisionTable, int count, double precision, int plus_bits);
uint32_t MultiLevelCacheTableWideIntervalGetIndex(double value, struct TopLevelTableWideInterval* topLevelTable);
void MultiLevelCacheTableWideIntervalFree(struct TopLevelTableWideInterval* table);

#ifdef __cplusplus
}
#endif

#endif //_MULTILEVELCACHETABLEWIDEINTERVAL_H
