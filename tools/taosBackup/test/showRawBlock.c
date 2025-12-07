/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */
    
#include <stdio.h>
#include <stdint.h>
#include "taos.h"

// 辅助函数：获取位图中的 null 标记
static bool isNull(char* nullbitmap, int32_t row) {
    return (nullbitmap[row / 8] & (1 << (7 - (row % 8)))) != 0;
}

void printRawBlock(void* block, int numOfRows) {
    if (block == NULL) return;
    
    char* p = (char*)block;
    
    // 1. 读取头部信息
    int32_t blockVersion = *(int32_t*)p;
    p += sizeof(int32_t);
    
    int32_t dataLen = *(int32_t*)p;
    p += sizeof(int32_t);
    
    int32_t rows = *(int32_t*)p;
    p += sizeof(int32_t);
    
    int32_t cols = *(int32_t*)p;
    p += sizeof(int32_t);
    
    int32_t hasColumnSeg = *(int32_t*)p;
    p += sizeof(int32_t);
    
    uint64_t groupId = *(uint64_t*)p;
    p += sizeof(uint64_t);
    
    printf("=== Block Info ===\n");
    printf("Version: %d\n", blockVersion);
    printf("DataLen: %d\n", dataLen);
    printf("Rows: %d\n", rows);
    printf("Cols: %d\n", cols);
    printf("GroupId: %lu\n", groupId);
    printf("\n");
    
    // 2. 读取字段信息
    typedef struct {
        int8_t type;
        int32_t bytes;
    } FieldInfo;
    
    FieldInfo* fields = (FieldInfo*)malloc(cols * sizeof(FieldInfo));
    for (int32_t i = 0; i < cols; i++) {
        fields[i].type = *(int8_t*)p;
        p += sizeof(int8_t);
        
        fields[i].bytes = *(int32_t*)p;
        p += sizeof(int32_t);
        
        printf("Col[%d]: type=%d, bytes=%d\n", i, fields[i].type, fields[i].bytes);
    }
    printf("\n");
    
    // 3. 读取列长度数组
    int32_t* colLength = (int32_t*)p;
    p += sizeof(int32_t) * cols;
    
    // 4. 解析每列数据
    char* pStart = p;
    for (int32_t col = 0; col < cols; col++) {
        if (blockVersion == 1) {
            colLength[col] = htonl(colLength[col]);
        }
        
        printf("=== Column %d Data ===\n", col);
        
        // 判断是否为变长类型
        bool isVarType = (fields[col].type == TSDB_DATA_TYPE_BINARY || 
                          fields[col].type == TSDB_DATA_TYPE_NCHAR ||
                          fields[col].type == TSDB_DATA_TYPE_JSON ||
                          fields[col].type == TSDB_DATA_TYPE_VARBINARY ||
                          fields[col].type == TSDB_DATA_TYPE_GEOMETRY);
        
        if (isVarType) {
            // 变长类型：offset 数组 + 数据
            int32_t* offsets = (int32_t*)pStart;
            pStart += rows * sizeof(int32_t);
            
            char* colData = pStart;
            for (int32_t row = 0; row < rows; row++) {
                if (offsets[row] == -1) {
                    printf("  Row[%d]: NULL\n", row);
                } else {
                    char* dataPtr = colData + offsets[row];
                    int16_t varLen = *(int16_t*)dataPtr;  // varDataLen
                    char* varData = dataPtr + sizeof(int16_t);
                    
                    if (fields[col].type == TSDB_DATA_TYPE_BINARY) {
                        printf("  Row[%d]: %.*s (len=%d)\n", row, varLen, varData, varLen);
                    } else {
                        printf("  Row[%d]: <data len=%d>\n", row, varLen);
                    }
                }
            }
        } else {
            // 定长类型：nullbitmap + 数据
            char* nullbitmap = pStart;
            int32_t bitmapLen = (rows + 7) / 8;
            pStart += bitmapLen;
            
            char* colData = pStart;
            
            for (int32_t row = 0; row < rows; row++) {
                if (isNull(nullbitmap, row)) {
                    printf("  Row[%d]: NULL\n", row);
                } else {
                    switch (fields[col].type) {
                        case TSDB_DATA_TYPE_TIMESTAMP:
                        case TSDB_DATA_TYPE_BIGINT:
                            printf("  Row[%d]: %ld\n", row, 
                                   ((int64_t*)colData)[row]);
                            break;
                        case TSDB_DATA_TYPE_INT:
                            printf("  Row[%d]: %d\n", row, 
                                   ((int32_t*)colData)[row]);
                            break;
                        case TSDB_DATA_TYPE_SMALLINT:
                            printf("  Row[%d]: %d\n", row, 
                                   ((int16_t*)colData)[row]);
                            break;
                        case TSDB_DATA_TYPE_TINYINT:
                            printf("  Row[%d]: %d\n", row, 
                                   ((int8_t*)colData)[row]);
                            break;
                        case TSDB_DATA_TYPE_FLOAT:
                            printf("  Row[%d]: %f\n", row, 
                                   ((float*)colData)[row]);
                            break;
                        case TSDB_DATA_TYPE_DOUBLE:
                            printf("  Row[%d]: %lf\n", row, 
                                   ((double*)colData)[row]);
                            break;
                        default:
                            printf("  Row[%d]: <unsupported type %d>\n", 
                                   row, fields[col].type);
                    }
                }
            }
        }
        
        pStart += colLength[col];
        printf("\n");
    }
    
    free(fields);
}

// 完整使用示例
int main() {
    TAOS* conn = taos_connect("localhost", "root", "taosdata", NULL, 0);
    if (conn == NULL) {
        printf("Failed to connect\n");
        return -1;
    }
    
    TAOS_RES* res = taos_query(conn, "SELECT * FROM test.meters");
    if (taos_errno(res) != 0) {
        printf("Query failed: %s\n", taos_errstr(res));
        taos_free_result(res);
        taos_close(conn);
        return -1;
    }
    
    // 逐块获取数据
    int blockRows = 0;
    void* block = NULL;
    
    while (taos_fetch_raw_block(res, &blockRows, &block) == TSDB_CODE_SUCCESS) {
        if (blockRows == 0) break;  // 没有更多数据
        
        printf("========== Fetched Block: %d rows ==========\n", blockRows);
        printRawBlock(block, blockRows);
        
        // 重要：如果需要保存这个 block 的数据，必须立即复制
        // 因为下次调用 taos_fetch_raw_block 会覆盖这块内存
        // char* savedBlock = malloc(block_size);
        // memcpy(savedBlock, block, block_size);
    }
    
    taos_free_result(res);
    taos_close(conn);
    return 0;
}

