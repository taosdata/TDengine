/**
 *  @file CacheTable.c
 *  @author Xiangyu Zou, Tao Lu, Wen Xia, Xuan Wang, Weizhe Zhang, Sheng Di, Dingwen Tao
 *  @date Jan, 2019
 *  @brief Cache Table
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdlib.h>
#include "CacheTable.h"

double* g_CacheTable;
uint32_t * g_InverseTable;
uint32_t baseIndex;
uint32_t topIndex;
int bits;

inline int doubleGetExpo(double d){
    long* ptr = (long*)&d;
    *ptr = ((*ptr) >> 52) - 1023;
    return *ptr;
}

int CacheTableGetRequiredBits(double precision, int quantization_intervals){
    double min_distance = pow((1+precision), -(quantization_intervals>>1)) * precision;
    return -(doubleGetExpo(min_distance));
}

inline uint32_t CacheTableGetIndex(float value, int bits){
    uint32_t* ptr = (uint32_t*)&value;
    int shift = 32 - 9 - bits;
    if(shift>0){
        return (*ptr) >> shift;
    }else{
        return 0;
    }
}

inline uint64_t CacheTableGetIndexDouble(double value, int bits){
    uint64_t* ptr = (uint64_t*)&value;
    int shift = 64 - 12 - bits;
    if(shift>0){
        return (*ptr) >> shift;
    }else{
        return 0;
    }
}

inline int CacheTableIsInBoundary(uint32_t index){
    if(index <= topIndex && index > baseIndex){
        return 1;
    }else{
        return 0;
    }
}

void CacheTableBuild(double * table, int count, double smallest, double largest, double precision, int quantization_intervals){
    bits = CacheTableGetRequiredBits(precision, quantization_intervals);
    baseIndex = CacheTableGetIndex((float)smallest, bits)+1;
    topIndex = CacheTableGetIndex((float)largest, bits);
    uint32_t range = topIndex - baseIndex + 1;
    g_InverseTable = (uint32_t *)malloc(sizeof(uint32_t) * range);

    /*
    uint32_t fillInPos = 0;
    for(int i=0; i<count; i++){
        if(i == 0){
            continue;
        }
        uint32_t index = CacheTableGetIndex((float)table[i], bits) - baseIndex;
        g_InverseTable[index] = i;
        if(index > fillInPos){
            for(int j=fillInPos; j<index; j++){
                g_InverseTable[j] = g_InverseTable[index];
            }
        }
        fillInPos = index + 1;
    }
     */
    for(int i=count-1; i>0; i--){
        uint32_t upperIndex = CacheTableGetIndex((float)table[i]*(1+precision), bits);
        uint32_t lowerIndex = CacheTableGetIndex((float)table[i]/(1+precision), bits);
        for(uint32_t j = lowerIndex; j<=upperIndex; j++){
            if(j<baseIndex || j >topIndex){
                continue;
            }
            g_InverseTable[j-baseIndex] = i;
        }
    }

}

inline uint32_t CacheTableFind(uint32_t index){
    return g_InverseTable[index-baseIndex];
}

void CacheTableFree(){
    free(g_InverseTable);
}
