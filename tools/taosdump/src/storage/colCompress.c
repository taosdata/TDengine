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

#include "colCompress.h"
#include "blockReader.h"
// engine
#include "tcompression.h"
#include "tcol.h"
#include "tdataformat.h"

// TODO: Implement getDefaultCompressLevel
static int8_t getDefaultCompressLevel(int8_t type) {
    return 0; // Default compression level
}

void fillFieldsInfo(FieldInfo* fieldInfos, TAOS_FIELD_E* fields, int numFields) {
    for (int i = 0; i < numFields; i++) {
        memcpy(fieldInfos[i].name, fields[i].name, sizeof(fieldInfos[i].name));
        fieldInfos[i].type = fields[i].type;
        if (IS_DECIMAL_TYPE(fields[i].type)) {
            // Pack precision/scale into bytes: (actualBytes<<24)|(precision<<8)|scale
            // This lets the restore path recover precision/scale without extra fields.
            int32_t actualBytes = tDataTypes[fields[i].type].bytes;
            fieldInfos[i].bytes = (actualBytes << 24) | (fields[i].precision << 8) | fields[i].scale;
        } else {
            fieldInfos[i].bytes = fields[i].bytes;
        }
        // default from engine
        fieldInfos[i].encode = getDefaultEncode(fields[i].type);
        fieldInfos[i].compress = getDefaultCompress(fields[i].type);
        fieldInfos[i].level = getDefaultCompressLevel(fields[i].type);
    }
}

uint32_t calcCompressBlockSize(BlockReader* reader) {
    uint32_t size = sizeof(CompressBlock);
    // ori header size
    size += sizeof(oriBlockHeader);

    // columns lens size
    size += sizeof(int32_t) * reader->oriHeader->numOfCols;

    // cmprAlgs size
    size += sizeof(int32_t) * reader->oriHeader->numOfCols;

    // data size
    size += reader->oriHeader->actualLen;

    // add 150% size for safety
    size = size * 1.5;

    return size;
}

int32_t compressColData(FieldInfo *fieldInfo, int32_t blockRows,
                        void* input, int32_t inputLen,
                        void* output, int32_t *outputLen,
                        uint32_t cmprAlg,
                        SBuffer *assist) {
    int32_t code = TSDB_CODE_SUCCESS;
    int32_t remain = *outputLen;
    int32_t opos = 0;
    int32_t ipos = 0;

    if (!IS_VAR_DATA_TYPE(fieldInfo->type)) {
        // fixed length type: inputLen = bitmapLen + dataLen
        // reserve 4 bytes for bitmap compressed size prefix
        int32_t prefixPos = opos;
        opos += sizeof(int32_t);
        remain -= sizeof(int32_t);

        // compress bitmap first
        int32_t bitmapLen = (blockRows + 7) / 8;
        SCompressInfo cinfo = {
            .dataType = TSDB_DATA_TYPE_TINYINT,
            .cmprAlg  = cmprAlg,
            .originalSize = bitmapLen,
        };

        code = tCompressData(input, &cinfo, (char *)output + opos, remain, assist);
        if (code != TSDB_CODE_SUCCESS) {
            logError("compress bitmap failed, code: %d", code);
            return code;
        }

        // write bitmap compressed size at prefix position
        *(int32_t *)((char *)output + prefixPos) = cinfo.compressedSize;

        // move past bitmap
        ipos += bitmapLen;
        opos += cinfo.compressedSize;
        remain -= cinfo.compressedSize;
    } else {
        // variable type: store original data length prefix for decompression
        *(int32_t *)((char *)output + opos) = inputLen;
        opos += sizeof(int32_t);
        remain -= sizeof(int32_t);
    }

    // compress actual column data
    int32_t colDataLen = inputLen - ipos;
    SCompressInfo cinfo = {
        .dataType = fieldInfo->type,
        .cmprAlg  = cmprAlg,
        .originalSize = colDataLen,
    };

    code = tCompressData((char *)input + ipos, &cinfo, (char *)output + opos, remain, NULL);
    if (code != TSDB_CODE_SUCCESS) {
        logError("compress col data failed, code: %d", code);
        return code;
    }

    *outputLen = opos + cinfo.compressedSize;
    return TSDB_CODE_SUCCESS;
}

//
// compress block
//
CompressBlock* compressBlock(void*      block,
                             int        blockRows,
                             FieldInfo* fieldInfos,
                             int        numFields,
                             SBuffer*   assist,
                             char**     bufPtr,
                             int32_t*   bufCapPtr,
                             int*       code) {
    // read block
    BlockReader reader;
    int32_t retCode = initBlockReader(&reader, block);
    if (retCode != TSDB_CODE_SUCCESS) {
        *code = retCode;
        return NULL;
    }

    // calculate required size and reuse/realloc buffer
    uint32_t mallocLen = calcCompressBlockSize(&reader);
    if (*bufCapPtr < (int32_t)mallocLen) {
        char *newBuf = (char *)taosMemoryRealloc(*bufPtr, mallocLen);
        if (!newBuf) {
            logError("realloc compress buffer failed: %u bytes", mallocLen);
            *code = TSDB_CODE_BCK_MALLOC_FAILED;
            return NULL;
        }
        *bufPtr = newBuf;
        *bufCapPtr = mallocLen;
    }
    CompressBlock* compressBlock = (CompressBlock*)(*bufPtr);

    //
    // header
    //
    compressBlock->version = COMPRESS_BLOCK_VERSION;
    compressBlock->flag = 0; // no use

    // fill original block header
    compressBlock->oriHeader = *(reader.oriHeader);

    //
    // body (lens + cmprAlgs + data)
    //
    int32_t *colsLen  = (int32_t *)compressBlock->data;
    int32_t *cmprAlgs = (int32_t *)(compressBlock->data + sizeof(int32_t) * numFields);
    char* colsDataStart = compressBlock->data + sizeof(int32_t) * numFields * 2;
    char* curDataPos = colsDataStart;

    // loop columns
    for (int i = 0; i < numFields; i++) {
        // get column data
        void* colData = NULL;
        int32_t colDataLen = 0;
        retCode = getColumnData(&reader, fieldInfos[i].type, &colData, &colDataLen);
        if (retCode != TSDB_CODE_SUCCESS) {
            logError("get column data failed: %d", retCode);
            *code = retCode;
            return NULL;
        }

        //
        // all-NULL optimisation (block version 2+):
        // For fixed-length columns the null-bitmap is the first bitmapLen bytes
        // of colData.  If every bit is 1 (every row is NULL) we store nothing
        // and record COL_LEN_ALL_NULL as the column length sentinel.
        // Variable-length columns are never fully NULL in normal TDengine blocks,
        // so we skip the optimisation for them.
        //
        if (!IS_VAR_DATA_TYPE(fieldInfos[i].type)) {
            int32_t bitmapLen = (blockRows + 7) / 8;
            const uint8_t *bm = (const uint8_t *)colData;
            bool allNull = true;
            // check all full bytes
            for (int b = 0; b < bitmapLen - 1 && allNull; b++) {
                if (bm[b] != 0xFF) allNull = false;
            }
            // check the last (potentially partial) byte
            if (allNull) {
                int rem = blockRows % 8;
                uint8_t mask = (rem == 0) ? 0xFF : (uint8_t)((0xFF << (8 - rem)) & 0xFF);
                if ((bm[bitmapLen - 1] & mask) != mask) allNull = false;
            }
            if (allNull) {
                colsLen[i]  = COL_LEN_ALL_NULL;
                cmprAlgs[i] = 0;
                logDebug("%d column %s: type=%d ALL-NULL, skipped",
                         i, fieldInfos[i].name, fieldInfos[i].type);
                continue; // no data written, curDataPos stays
            }
        }

        // compress column data
        int32_t remainLen = mallocLen - (curDataPos - (char *)compressBlock);
        int32_t compressedLen = remainLen;
        uint32_t cmprAlg = 0;
        SET_COMPRESS(fieldInfos[i].encode, fieldInfos[i].compress, fieldInfos[i].level, cmprAlg);
        *code = compressColData(&fieldInfos[i],             // algo
                                blockRows,                  // rows
                                colData, colDataLen,        // input
                                curDataPos, &compressedLen, // output
                                cmprAlg,
                                assist);
        if (*code != TSDB_CODE_SUCCESS) {
            logError("compress column data failed. code: %d", *code);
            *code = TSDB_CODE_BCK_COMPRESS_FAILED;
            return NULL;
        }

        // set len/algo
        colsLen[i]  = compressedLen;
        cmprAlgs[i] = cmprAlg;

        // debug
        logDebug("%d column %s: type=%d original len: %d, compressed len: %d, cmprAlg: 0x%08X",
                 i, fieldInfos[i].name, fieldInfos[i].type, colDataLen, compressedLen, cmprAlg);

        // move current data pos
        curDataPos += compressedLen;
    }

    // set data length (lens + cmprAlgs + data)
    compressBlock->dataLen = curDataPos - compressBlock->data;

    //
    // check need compress or not
    //
    if(compressBlock->dataLen > reader.oriHeader->actualLen) {
        logDebug("compressed block (%d bytes) >= original (%d bytes), skipping compression",
                 compressBlock->dataLen, reader.oriHeader->actualLen);
        // not compress, use original block
        compressBlock->flag = BLOCK_FLAG_NOT_COMPRESS;
        memcpy(compressBlock->data, block, reader.oriHeader->actualLen);
        compressBlock->dataLen = reader.oriHeader->actualLen;
    }

    *code = TSDB_CODE_SUCCESS;
    return compressBlock;
}

//
// decompress single column data (mirror of compressColData)
//
static int32_t decompressColData(FieldInfo *fieldInfo, int32_t blockRows,
                                 void* input, int32_t inputLen,
                                 void* output, int32_t *outputLen,
                                 uint32_t cmprAlg,
                                 SBuffer *assist) {
    int32_t code = TSDB_CODE_SUCCESS;
    int32_t ipos = 0;
    int32_t opos = 0;
    int32_t remain = *outputLen;
    int32_t dataOriginalSize = 0;

    if (!IS_VAR_DATA_TYPE(fieldInfo->type)) {
        // fixed length type: decompress bitmap first
        // read bitmap compressed size from prefix
        int32_t bitmapCompressedSize = *(int32_t *)((char *)input + ipos);
        ipos += sizeof(int32_t);

        int32_t bitmapLen = (blockRows + 7) / 8;
        SCompressInfo cinfo = {
            .dataType = TSDB_DATA_TYPE_TINYINT,
            .cmprAlg  = cmprAlg,
            .originalSize = bitmapLen,
            .compressedSize = bitmapCompressedSize,
        };

        code = tDecompressData((char *)input + ipos, &cinfo, (char *)output + opos, remain, assist);
        if (code != TSDB_CODE_SUCCESS) {
            logError("decompress bitmap failed, code: %d", code);
            return code;
        }

        ipos += bitmapCompressedSize;
        opos += bitmapLen;
        remain -= bitmapLen;

        // data original size = rows * field raw bytes
        dataOriginalSize = blockRows * fieldGetRawBytes(fieldInfo);
    } else {
        // variable type: read original data length from prefix
        dataOriginalSize = *(int32_t *)((char *)input + ipos);
        ipos += sizeof(int32_t);
    }

    // decompress actual column data
    int32_t dataCompressedSize = inputLen - ipos;
    SCompressInfo cinfo = {
        .dataType = fieldInfo->type,
        .cmprAlg  = cmprAlg,
        .originalSize = dataOriginalSize,
        .compressedSize = dataCompressedSize,
    };

    code = tDecompressData((char *)input + ipos, &cinfo, (char *)output + opos, remain, NULL);
    if (code != TSDB_CODE_SUCCESS) {
        logError("decompress col data failed, code: %d", code);
        return code;
    }

    *outputLen = opos + dataOriginalSize;
    return TSDB_CODE_SUCCESS;
}

//
// decompress block
//
// Space-for-time: the caller may pass a pre-allocated reuse buffer via
// *uncompressBlock / *uncompressLen:
//   On entry:  *uncompressBlock = existing buffer (or NULL for first call)
//              *uncompressLen   = current buffer capacity in bytes (0 if NULL)
//   On exit:   *uncompressBlock = output buffer (reused or realloced)
//              *uncompressLen   = actual decompressed block length
//
// The caller must free *uncompressBlock once when it is no longer needed.
// Passing NULL / 0 on the first call is equivalent to the old single-use
// calloc behaviour.
//
int decompressBlock(CompressBlock* compressBlk,
                    FieldInfo*     fieldInfos,
                    int            numFields,
                    void**         uncompressBlock,
                    int32_t*       uncompressLen,
                    SBuffer*       assist) {
    // read compress block header
    oriBlockHeader* oriHeader = &compressBlk->oriHeader;
    int32_t numOfCols = oriHeader->numOfCols;
    int32_t blockRows = oriHeader->rows;

    // total original block size = actualLen (includes header)
    // Space-for-time: reuse caller's buffer when capacity is sufficient.
    int32_t needed    = oriHeader->actualLen;
    int32_t callerCap = *uncompressLen;   /* caller passes current buffer capacity */
    *uncompressLen    = needed;
    if (*uncompressBlock != NULL && callerCap >= needed) {
        /* buffer is large enough — zero-fill only the needed region (no malloc) */
        memset(*uncompressBlock, 0, needed);
    } else {
        /* grow: realloc from existing or fresh malloc when *uncompressBlock is NULL */
        void *newBuf = (*uncompressBlock)
                     ? taosMemoryRealloc(*uncompressBlock, needed)
                     : taosMemoryMalloc(needed);
        if (!newBuf) {
            logError("malloc uncompress block failed");
            return TSDB_CODE_BCK_MALLOC_FAILED;
        }
        *uncompressBlock = newBuf;
        memset(*uncompressBlock, 0, needed);
    }

    //
    // handle not compressed - data is the original raw block as-is
    //
    if (compressBlk->flag & BLOCK_FLAG_NOT_COMPRESS) {
        memcpy(*uncompressBlock, compressBlk->data, compressBlk->dataLen);
        return TSDB_CODE_SUCCESS;
    }

    //
    // reconstruct the full original block:
    //   oriBlockHeader + schema + col-lengths + col-data
    //
    char* outPos = (char *)(*uncompressBlock);

    // 1. copy original block header
    memcpy(outPos, oriHeader, sizeof(oriBlockHeader));
    outPos += sizeof(oriBlockHeader);

    // 2. reconstruct schema (type(1B) + bytes(4B) per column)
    for (int i = 0; i < numOfCols; i++) {
        *(int8_t *)outPos = fieldInfos[i].type;
        outPos += sizeof(int8_t);
        *(int32_t *)outPos = fieldInfos[i].bytes;
        outPos += sizeof(int32_t);
    }

    // 3. leave space for col-lengths (will fill after decompression)
    int32_t *colLengths = (int32_t *)outPos;
    outPos += sizeof(int32_t) * numOfCols;

    //
    // 4. read compressed data arrays from CompressBlock
    //
    int32_t *colsLen  = (int32_t *)compressBlk->data;
    int32_t *cmprAlgs = (int32_t *)(compressBlk->data + sizeof(int32_t) * numOfCols);
    char* compDataStart = compressBlk->data + sizeof(int32_t) * numOfCols * 2;
    char* curCompPos = compDataStart;

    //
    // 5. decompress each column and fill col-lengths
    //
    for (int i = 0; i < numOfCols; i++) {
        //
        // all-NULL sentinel (block version 2+):
        // synthesise an all-NULL bitmap + zero data payload in-place.
        //
        if (colsLen[i] == COL_LEN_ALL_NULL) {
            if (IS_VAR_DATA_TYPE(fieldInfos[i].type)) {
                // Shouldn't happen, but handle gracefully:
                // write zero-length offsets + empty data
                int32_t offsetsLen = blockRows * sizeof(int32_t);
                memset(outPos, 0, offsetsLen);
                colLengths[i] = 0;
                logDebug("%d column %s: type=%d ALL-NULL (var) synthesised",
                         i, fieldInfos[i].name, fieldInfos[i].type);
                outPos += offsetsLen;
            } else {
                int32_t bitmapLen = (blockRows + 7) / 8;
                int32_t dataLen   = blockRows * fieldGetRawBytes(&fieldInfos[i]);
                // bitmap: all 1s (all NULL)
                memset(outPos, 0xFF, bitmapLen);
                // data area: zeroed (values undefined for NULL rows)
                memset(outPos + bitmapLen, 0, dataLen);
                colLengths[i] = dataLen;
                logDebug("%d column %s: type=%d ALL-NULL synthesised, bitmapLen=%d dataLen=%d",
                         i, fieldInfos[i].name, fieldInfos[i].type, bitmapLen, dataLen);
                outPos += bitmapLen + dataLen;
            }
            // curCompPos does not advance – no data stored for all-NULL columns
            continue;
        }

        int32_t remainOut = *uncompressLen - (int32_t)(outPos - (char *)(*uncompressBlock));
        int32_t outputLen = remainOut;

        int32_t code = decompressColData(&fieldInfos[i],
                                         blockRows,
                                         curCompPos, colsLen[i],
                                         outPos, &outputLen,
                                         cmprAlgs[i],
                                         assist);
        if (code != TSDB_CODE_SUCCESS) {
            logError("decompress column %d (%s) failed: %d", i, fieldInfos[i].name, code);
            taosMemFree(*uncompressBlock);
            *uncompressBlock = NULL;
            *uncompressLen = 0;
            return code;
        }

        // fill col-length: rawLen = totalLen - bitmap/offsets overhead
        if (IS_VAR_DATA_TYPE(fieldInfos[i].type)) {
            int32_t offsetsLen = blockRows * sizeof(int32_t);
            colLengths[i] = outputLen - offsetsLen;
        } else {
            int32_t bitmapLen = (blockRows + 7) / 8;
            colLengths[i] = outputLen - bitmapLen;
        }

        logDebug("%d column %s: type=%d compressed len: %d, decompressed len: %d, rawLen: %d",
                 i, fieldInfos[i].name, fieldInfos[i].type, colsLen[i], outputLen, colLengths[i]);

        // move positions
        curCompPos += colsLen[i];
        outPos += outputLen;
    }

    return TSDB_CODE_SUCCESS;
}

// free compress data
void freeCompressData(CompressBlock* compressBlock) {
    if (compressBlock == NULL) {
        return;
    }

    taosMemFree(compressBlock);
}
