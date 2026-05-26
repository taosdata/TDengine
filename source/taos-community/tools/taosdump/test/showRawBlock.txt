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
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "taos.h"
#include "taoserror.h"

size_t blockDataGetSerialMetaSize(uint32_t numOfCols); // tdatablock.h

// 辅助函数：获取位图中的 null 标记
static bool isNull(char* nullbitmap, int32_t row) {
    return (nullbitmap[row / 8] & (1 << (7 - (row % 8)))) != 0;
}

// 获取数据类型名称
const char* getTypeName(int8_t type) {
    switch(type) {
        case TSDB_DATA_TYPE_NULL: return "NULL";
        case TSDB_DATA_TYPE_BOOL: return "BOOL";
        case TSDB_DATA_TYPE_TINYINT: return "TINYINT";
        case TSDB_DATA_TYPE_SMALLINT: return "SMALLINT";
        case TSDB_DATA_TYPE_INT: return "INT";
        case TSDB_DATA_TYPE_BIGINT: return "BIGINT";
        case TSDB_DATA_TYPE_FLOAT: return "FLOAT";
        case TSDB_DATA_TYPE_DOUBLE: return "DOUBLE";
        case TSDB_DATA_TYPE_VARCHAR: return "VARCHAR";
        case TSDB_DATA_TYPE_TIMESTAMP: return "TIMESTAMP";
        case TSDB_DATA_TYPE_NCHAR: return "NCHAR";
        case TSDB_DATA_TYPE_UTINYINT: return "UTINYINT";
        case TSDB_DATA_TYPE_USMALLINT: return "USMALLINT";
        case TSDB_DATA_TYPE_UINT: return "UINT";
        case TSDB_DATA_TYPE_UBIGINT: return "UBIGINT";
        case TSDB_DATA_TYPE_JSON: return "JSON";
        case TSDB_DATA_TYPE_VARBINARY: return "VARBINARY";
        case TSDB_DATA_TYPE_DECIMAL: return "DECIMAL";
        case TSDB_DATA_TYPE_BLOB: return "BLOB";
        case TSDB_DATA_TYPE_MEDIUMBLOB: return "MEDIUMBLOB";
        case TSDB_DATA_TYPE_GEOMETRY: return "GEOMETRY";
        case TSDB_DATA_TYPE_DECIMAL64: return "DECIMAL64";
        default: return "UNKNOWN";
    }
}

// 十六进制 dump 辅助函数
void hexDump(const char* label, void* data, int len) {
    unsigned char* p = (unsigned char*)data;
    printf("%s (%d bytes):\n  ", label, len);
    for (int i = 0; i < len; i++) {
        printf("%02X ", p[i]);
        if ((i + 1) % 16 == 0) printf("\n  ");
    }
    if (len % 16 != 0) printf("\n");
}

void printRawBlock(void* block, int numOfRows) {
    if (block == NULL) return;
    
    char* p = (char*)block;
    char* blockStart = p;
    
    // 1. 读取头部信息
    int32_t blockVersion = *(int32_t*)p;
    p += sizeof(int32_t);
    
    int32_t dataLen = *(int32_t*)p;
    p += sizeof(int32_t);
    
    int32_t rows = *(int32_t*)p;
    p += sizeof(int32_t);
    
    int32_t cols = *(int32_t*)p;
    p += sizeof(int32_t);
    
    int32_t flagSeg = *(int32_t*)p;
    p += sizeof(int32_t);
    
    uint64_t groupId = *(uint64_t*)p;
    p += sizeof(uint64_t);
    
    printf("=== Block Info ===\n");
    printf("Version: %d\n", blockVersion);
    printf("DataLen: %d bytes\n", dataLen);
    printf("Rows: %d\n", rows);
    printf("Cols: %d\n", cols);
    printf("FlagSeg: 0x%08X\n", flagSeg);
    printf("GroupId: %lu\n", groupId);
    printf("Header size: %ld bytes\n", p - blockStart);
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
        
        printf("Col[%d]: type=%d (%s), bytes=%d\n", 
               i, fields[i].type, getTypeName(fields[i].type), fields[i].bytes);
    }
    printf("After field info: offset=%ld\n\n", p - blockStart);
    
    // 3. 读取列长度数组
    int32_t* colLength = (int32_t*)p;
    p += sizeof(int32_t) * cols;
    
    printf("=== Column Lengths ===\n");
    for (int32_t i = 0; i < cols; i++) {
        printf("Col[%d] length: %d bytes\n", i, colLength[i]);
    }
    printf("After col lengths: offset=%ld\n\n", p - blockStart);
    
    // 先用十六进制dump前256字节看看数据布局
    printf("=== Raw Data Dump (first 256 bytes) ===\n");
    int dumpLen = (dataLen < 256) ? dataLen : 256;
    hexDump("Block data", blockStart, dumpLen);
    printf("\n");
    
    // 4. 解析每列数据
    char* pStart = p;
    for (int32_t col = 0; col < cols; col++) {
        int32_t len = colLength[col];
        
        printf("=== Column %d (%s) at offset %ld ===\n", 
               col, getTypeName(fields[col].type), pStart - blockStart);
        
        // 判断是否为变长类型
        bool isVarType = (fields[col].type == TSDB_DATA_TYPE_BINARY || 
                          fields[col].type == TSDB_DATA_TYPE_VARCHAR ||
                          fields[col].type == TSDB_DATA_TYPE_NCHAR ||
                          fields[col].type == TSDB_DATA_TYPE_JSON ||
                          fields[col].type == TSDB_DATA_TYPE_VARBINARY ||
                          fields[col].type == TSDB_DATA_TYPE_GEOMETRY);
        
        // 对于变长类型，len 是数据部分长度，总长度 = offsets数组 + 数据部分
        int32_t totalLen = isVarType ? (rows * sizeof(int32_t) + len) : len;
        
        // Hex dump这一列的数据
        hexDump("  Column data", pStart, totalLen > 64 ? 64 : totalLen);
        
        if (isVarType) {
            // 变长类型：offset 数组 + 数据
            int32_t* offsets = (int32_t*)pStart;
            char* colDataStart = pStart + rows * sizeof(int32_t);
            
            printf("  Offsets: [");
            for (int32_t row = 0; row < rows; row++) {
                printf("%d%s", offsets[row], row < rows-1 ? ", " : "");
            }
            printf("]\n");
            
            for (int32_t row = 0; row < rows; row++) {
                if (offsets[row] == -1) {
                    printf("  Row[%d]: NULL\n", row);
                } else {
                    char* dataPtr = colDataStart + offsets[row];
                    int16_t varLen = *(int16_t*)dataPtr;
                    char* varData = dataPtr + sizeof(int16_t);
                    
                    if (fields[col].type == TSDB_DATA_TYPE_BINARY || 
                        fields[col].type == TSDB_DATA_TYPE_VARCHAR) {
                        printf("  Row[%d]: '%.*s' (len=%d, offset=%d)\n", 
                               row, varLen, varData, varLen, offsets[row]);
                    } else if (fields[col].type == TSDB_DATA_TYPE_NCHAR) {
                        // NCHAR 是 UTF-32LE 编码，每个字符4字节
                        printf("  Row[%d]: '", row);
                        int32_t* wchars = (int32_t*)varData;
                        int numChars = varLen / 4;
                        for (int i = 0; i < numChars; i++) {
                            if (wchars[i] < 128) {
                                printf("%c", (char)wchars[i]);
                            } else {
                                printf("\\u%04X", wchars[i]);
                            }
                        }
                        printf("' (len=%d bytes, offset=%d)\n", varLen, offsets[row]);
                    } else {
                        printf("  Row[%d]: <data len=%d>\n", row, varLen);
                    }
                }
            }
        } else {
            // 定长类型：nullbitmap + 数据
            char* nullbitmap = pStart;
            int32_t bitmapLen = (rows + 7) / 8;
            char* colData = pStart + bitmapLen;

            // 更新列数据总长度（定长类型的 colLength 不包括 nullbitmap）
            totalLen = len + bitmapLen;
            
            printf("  Nullbitmap (%d bytes): 0x", bitmapLen);
            for (int i = 0; i < bitmapLen; i++) {
                printf("%02X ", (unsigned char)nullbitmap[i]);
            }
            printf("\n");
            
            for (int32_t row = 0; row < rows; row++) {
                bool null = isNull(nullbitmap, row);
                printf("  Row[%d] (null=%d): ", row, null);
                
                if (null) {
                    printf("NULL\n");
                } else {
                    switch (fields[col].type) {
                        case TSDB_DATA_TYPE_TIMESTAMP:
                        case TSDB_DATA_TYPE_BIGINT: {
                            int64_t val = ((int64_t*)colData)[row];
                            printf("%ld\n", val);
                            break;
                        }
                        case TSDB_DATA_TYPE_INT: {
                            int32_t val = ((int32_t*)colData)[row];
                            printf("%d\n", val);
                            break;
                        }
                        case TSDB_DATA_TYPE_FLOAT: {
                            float val = ((float*)colData)[row];
                            printf("%.2f\n", val);
                            break;
                        }
                        case TSDB_DATA_TYPE_DOUBLE: {
                            double val = ((double*)colData)[row];
                            printf("%.2f\n", val);
                            break;
                        }
                        default:
                            printf("<type %d>\n", fields[col].type);
                    }
                }
            }
        }
        
        pStart += totalLen;
        printf("\n");
    }
    
    free(fields);
    
    printf("Total bytes parsed: %ld (expected: %d)\n", pStart - blockStart, dataLen);
}

int main(int argc, char *argv[]) {
    TAOS* conn = taos_connect("localhost", "root", "taosdata", NULL, 0);
    if (conn == NULL) {
        printf("Failed to connect: %s\n", taos_errstr(NULL));
        return -1;
    }

    int size = blockDataGetSerialMetaSize(5);
    printf("Serial meta size for 5 columns: %d\n", size);
    
    printf("Connected to TDengine successfully!\n\n");
    
    TAOS_RES* res = taos_query(conn, "SELECT * FROM mix.meters");
    if (taos_errno(res) != 0) {
        printf("Query failed: %s\n", taos_errstr(res));
        taos_free_result(res);
        taos_close(conn);
        return -1;
    }
    
    printf("Query executed successfully!\n\n");
    
    int blockRows = 0;
    void* block = NULL;
    int blockCount = 0;
    
    while (taos_fetch_raw_block(res, &blockRows, &block) == TSDB_CODE_SUCCESS) {
        if (blockRows == 0) break;
        
        blockCount++;
        printf("========== Fetched Block #%d: %d rows ==========\n", blockCount, blockRows);
        printRawBlock(block, blockRows);
        printf("\n");
    }
    
    if (blockCount == 0) {
        printf("No data returned from query.\n");
    } else {
        printf("Total %d block(s) fetched.\n", blockCount);
    }
    
    taos_free_result(res);
    taos_close(conn);
    return 0;
}
