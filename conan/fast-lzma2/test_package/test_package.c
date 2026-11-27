/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fast-lzma2.h"

int main() {
    const char* test_data = "Hello, Fast LZMA2! This is a test string for compression.";
    size_t src_size = strlen(test_data);
    
    // 获取版本信息
    unsigned version = FL2_versionNumber();
    printf("Fast LZMA2 version: %u.%u.%u\n", 
           version / 10000, (version / 100) % 100, version % 100);
    
    // 测试压缩边界大小
    size_t max_compressed_size = FL2_compressBound(src_size);
    printf("Source size: %zu bytes\n", src_size);
    printf("Max compressed size: %zu bytes\n", max_compressed_size);
    
    // 简单的压缩测试
    char* compressed = (char*)malloc(max_compressed_size);
    if (!compressed) {
        fprintf(stderr, "Memory allocation failed\n");
        return 1;
    }
    
    size_t compressed_size = FL2_compress(compressed, max_compressed_size,
                                          test_data, src_size, 6);
    
    if (FL2_isError(compressed_size)) {
        fprintf(stderr, "Compression failed: %s\n", 
                FL2_getErrorName(compressed_size));
        free(compressed);
        return 1;
    }
    
    printf("Compressed size: %zu bytes (%.2f%% of original)\n", 
           compressed_size, (compressed_size * 100.0) / src_size);
    
    // 测试解压缩
    char* decompressed = (char*)malloc(src_size + 1);
    if (!decompressed) {
        fprintf(stderr, "Memory allocation failed\n");
        free(compressed);
        return 1;
    }
    
    size_t decompressed_size = FL2_decompress(decompressed, src_size,
                                              compressed, compressed_size);
    
    if (FL2_isError(decompressed_size)) {
        fprintf(stderr, "Decompression failed: %s\n", 
                FL2_getErrorName(decompressed_size));
        free(compressed);
        free(decompressed);
        return 1;
    }
    
    decompressed[decompressed_size] = '\0';
    
    // 验证结果
    if (decompressed_size == src_size && 
        memcmp(test_data, decompressed, src_size) == 0) {
        printf("Test PASSED: Compression and decompression successful!\n");
        printf("Decompressed: %s\n", decompressed);
    } else {
        fprintf(stderr, "Test FAILED: Data mismatch after decompression\n");
        free(compressed);
        free(decompressed);
        return 1;
    }
    
    free(compressed);
    free(decompressed);
    
    return 0;
}
