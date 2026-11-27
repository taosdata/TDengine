/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef INC_BLOCKREADER_H_
#define INC_BLOCKREADER_H_

#include "bck.h"

#ifdef __cplusplus
extern "C" {
#endif

//
// ---------------- define ----------------
//
#define COMPRESS_BLOCK_VERSION 2

//
// ---------------- struct ----------------
//

#pragma pack(push, 1)
typedef struct {
    int32_t  version;
    int32_t  actualLen;
    int32_t  rows;
    int32_t  numOfCols;
    int32_t  flagSegment;
    uint64_t groupId;
} oriBlockHeader;
#pragma pack(pop)

typedef struct BlockReader {
    union {
        void *data;
        oriBlockHeader *oriHeader;
    };

    char* lenPtr;
    char* dataPtr;

    // reader state
    int32_t offsetLen;
    int32_t offsetData;
} BlockReader;


// ---------------- interface ----------------
int32_t initBlockReader(BlockReader* reader, void* blockData);

int32_t getColumnData(BlockReader* reader, int8_t colType, void** colData, int32_t* colDataLen);


#ifdef __cplusplus
}
#endif

#endif  // INC_BLOCKREADER_H_
