/**
 *  @file MultiLevelCacheTableWideInterval.h
 *  @author Xiangyu Zou, Tao Lu, Wen Xia, Xuan Wang, Weizhe Zhang, Sheng Di, Dingwen Tao
 *  @date Jan, 2019
 *  @brief Header file.
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdbool.h>
#include "MultiLevelCacheTableWideInterval.h"

void freeTopLevelTableWideInterval(struct TopLevelTableWideInterval* topTable)
{
	for(int i=topTable->topIndex-topTable->baseIndex; i>=0; i--)
	{
		struct SubLevelTableWideInterval* processingSubTable = &topTable->subTables[i];
		free(processingSubTable->table);
	}
	free(topTable->subTables);
}

uint16_t MLCTWI_GetExpoIndex(double value){
    uint64_t* ptr = (uint64_t*)&value;
    return (*ptr) >> 52;
}

uint16_t MLCTWI_GetRequiredBits(double precision){
    uint64_t* ptr = (uint64_t*)&precision;
    return -(((*ptr) >> 52) - 1023);
}

uint64_t MLCTWI_GetMantiIndex(double value, int bits){
    uint64_t* ptr = (uint64_t*)&value;
    (*ptr) = (*ptr) << 12 >> 12;
    int shift = 64 - 12 - bits;
    if(shift > 0){
        return (*ptr) >> shift;
    }else{
        return (*ptr);
    }
}

double MLTCWI_RebuildDouble(uint16_t expo, uint64_t manti, int bits){
    double result = 0;
    uint64_t *ptr = (uint64_t*)&result;
    *ptr = expo;
    (*ptr) = (*ptr) << 52;
    (*ptr) += (manti << (52-bits));
    return result;
}

void MultiLevelCacheTableWideIntervalBuild(struct TopLevelTableWideInterval* topTable, double* precisionTable, int count, double precision, int plus_bits){
    uint16_t bits = MLCTWI_GetRequiredBits(precision) + plus_bits;
    topTable->bits = bits;
    topTable->bottomBoundary = precisionTable[1]/(1+precision);
    topTable->topBoundary = precisionTable[count-1]/(1-precision);
    topTable->baseIndex = MLCTWI_GetExpoIndex(topTable->bottomBoundary);
    topTable->topIndex = MLCTWI_GetExpoIndex(topTable->topBoundary);
    int subTableCount = topTable->topIndex - topTable->baseIndex + 1;
    topTable->subTables = (struct SubLevelTableWideInterval*)malloc(sizeof(struct SubLevelTableWideInterval) * subTableCount);
    memset(topTable->subTables, 0, sizeof(struct SubLevelTableWideInterval) * subTableCount);

    for(int i=topTable->topIndex-topTable->baseIndex; i>=0; i--){
        struct SubLevelTableWideInterval* processingSubTable = &topTable->subTables[i];

        uint32_t maxIndex = 0;
        for(int j=0; j<bits; j++){
            maxIndex += 1 << j;
        }
        processingSubTable->topIndex = maxIndex;
        processingSubTable->baseIndex = 0;

        uint64_t subTableLength = processingSubTable->topIndex - processingSubTable-> baseIndex+ 1;
        processingSubTable->table = (uint16_t*)malloc(sizeof(uint16_t) * subTableLength);
        memset(processingSubTable->table, 0, sizeof(uint16_t) * subTableLength);
        processingSubTable->expoIndex = topTable->baseIndex + i;
    }


    uint32_t index = 0;
    bool flag = false;
    for(uint16_t i = 0; i<=topTable->topIndex-topTable->baseIndex; i++){
        struct SubLevelTableWideInterval* processingSubTable = &topTable->subTables[i];
        uint16_t expoIndex = i+topTable->baseIndex;
        for(uint32_t j = 0; j<=processingSubTable->topIndex - processingSubTable->baseIndex; j++){
            uint64_t mantiIndex = j + processingSubTable->baseIndex;
            double sampleBottom = MLTCWI_RebuildDouble(expoIndex, mantiIndex, topTable->bits);
            double sampleTop = MLTCWI_RebuildDouble(expoIndex, mantiIndex+1, topTable->bits);
            double bottomBoundary = precisionTable[index] / (1+precision);
            double topBoundary = precisionTable[index] / (1-precision);
            if(sampleTop < topBoundary && sampleBottom > bottomBoundary){
                processingSubTable->table[j] = index;
                flag = true;
            }else{
                if(flag && index < count-1){
                    index++;
                    processingSubTable->table[j] = index;
                }else{
                    processingSubTable->table[j] = 0;
                }
            }
        }
    }

}

uint32_t MultiLevelCacheTableWideIntervalGetIndex(double value, struct TopLevelTableWideInterval* topLevelTable){
    uint16_t expoIndex = MLCTWI_GetExpoIndex(value);
    if(expoIndex <= topLevelTable->topIndex && expoIndex >= topLevelTable->baseIndex){
        struct SubLevelTableWideInterval* subLevelTable = &topLevelTable->subTables[expoIndex-topLevelTable->baseIndex];
        uint64_t mantiIndex = MLCTWI_GetMantiIndex(value, topLevelTable->bits);
        return subLevelTable->table[mantiIndex - subLevelTable->baseIndex];

    }
    return 0;
}

void MultiLevelCacheTableWideIntervalFree(struct TopLevelTableWideInterval* table){
    for(int i=0; i<table->topIndex - table->baseIndex + 1; i++){
        free(table->subTables[i].table);
    }
    free(table->subTables);
}

