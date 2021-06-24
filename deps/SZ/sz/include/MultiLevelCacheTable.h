/**
 *  @file MultiLevelCacheTable.h
 *  @author Xiangyu Zou, Tao Lu, Wen Xia, Xuan Wang, Weizhe Zhang, Sheng Di, Dingwen Tao
 *  @date Jan, 2019
 *  @brief Header file.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef _MULTILEVELCACHETABLE_H
#define _MULTILEVELCACHETABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <memory.h>
#include <stdlib.h>
#include "stdio.h"

typedef struct SubLevelTable{
    uint32_t baseIndex;
    uint32_t topIndex;
    uint32_t* table;
    uint8_t expoIndex;
} SubLevelTable;

typedef struct TopLevelTable{
    uint8_t bits;
    uint8_t baseIndex;
    uint8_t topIndex;
    struct SubLevelTable* subTables;
    float bottomBoundary;
    float topBoundary;
} TopLevelTable;

uint8_t MLCT_GetExpoIndex(float value);
uint8_t MLCT_GetRequiredBits(float precision);
uint32_t MLCT_GetMantiIndex(float value, int bits);
float MLTC_RebuildFloat(uint8_t expo, uint32_t manti, int bits);
void MultiLevelCacheTableBuild(struct TopLevelTable* topTable, float* precisionTable, int count, float precision);
uint32_t MultiLevelCacheTableGetIndex(float value, struct TopLevelTable* topLevelTable);
void MultiLevelCacheTableFree(struct TopLevelTable* table);

#ifdef __cplusplus
}
#endif

#endif //_MULTILEVELCACHETABLE_H
