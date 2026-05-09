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

#include "storageTaos.h"
#include "bckLog.h"
#include <taoserror.h>
#include "colCompress.h"
#include "tbuffer.h"
#include "tcompression.h"

// Forward declarations
int closeTaosFile(TaosFile* taosFile);


// flush write buffer to disk
static int flushWriteBuffer(TaosFile* taosFile) {
    if (taosFile->writeBufPos <= 0) return TSDB_CODE_SUCCESS;
    int64_t writeLen = taosWriteFile(taosFile->fp, taosFile->writeBuf, taosFile->writeBufPos);
    if (writeLen != (int64_t)taosFile->writeBufPos) {
        logError("flush write buffer failed, writeLen: %" PRId64 ", expectLen: %d", writeLen, taosFile->writeBufPos);
        return TSDB_CODE_BCK_WRITE_FILE_FAILED;
    }
    taosFile->writeBufPos = 0;
    return TSDB_CODE_SUCCESS;
}

int writeTaosFile(TaosFile* taosFile, void *data, int len) {
    if (taosFile == NULL || taosFile->fp == NULL) {
        logError("invalid TaosFile");
        return TSDB_CODE_BCK_WRITE_FILE_FAILED;
    }

    // if no write buffer allocated, write directly
    if (taosFile->writeBuf == NULL) {
        int64_t writeLen = taosWriteFile(taosFile->fp, data, len);
        if (writeLen != (int64_t)len) {
            logError("write to file failed, writeLen: %" PRId64 ", expectLen: %d", writeLen, len);
            return TSDB_CODE_BCK_WRITE_FILE_FAILED;
        }
        return TSDB_CODE_SUCCESS;
    }

    // if data larger than buffer, flush buffer first then write directly
    if (len >= taosFile->writeBufCap) {
        int code = flushWriteBuffer(taosFile);
        if (code != TSDB_CODE_SUCCESS) return code;
        int64_t writeLen = taosWriteFile(taosFile->fp, data, len);
        if (writeLen != (int64_t)len) {
            logError("write to file failed, writeLen: %" PRId64 ", expectLen: %d", writeLen, len);
            return TSDB_CODE_BCK_WRITE_FILE_FAILED;
        }
        return TSDB_CODE_SUCCESS;
    }

    // if adding data would overflow buffer, flush first
    if (taosFile->writeBufPos + len > taosFile->writeBufCap) {
        int code = flushWriteBuffer(taosFile);
        if (code != TSDB_CODE_SUCCESS) return code;
    }

    // append to buffer
    memcpy(taosFile->writeBuf + taosFile->writeBufPos, data, len);
    taosFile->writeBufPos += len;
    return TSDB_CODE_SUCCESS;
}

TaosFile* createTaosFile(const char *fileName, TAOS_FIELD_E* fields, int numFields, int *code) {
    TaosFile* taosFile = (TaosFile*)taosMemoryMalloc(sizeof(TaosFile) + sizeof(FieldInfo) * numFields);
    if (taosFile == NULL) {
        logError("malloc TaosFile failed");
        return NULL;
    }
    memset(taosFile, 0, sizeof(TaosFile));

    taosFile->fileName = fileName;
    if (taosFile->fileName == NULL) {
        logError("strdup fileName failed: %s", fileName);
        taosMemoryFree(taosFile);
        return NULL;
    }

    // collect data
    memcpy(taosFile->header.magic, TAOSFILE_MAGIC, 4);
    taosFile->header.version = TAOSFILE_VERSION;
    taosFile->header.numFields = numFields;
    taosFile->header.schemaLen = sizeof(FieldInfo) * numFields;
    fillFieldsInfo((FieldInfo*)taosFile->header.schema, fields, numFields);

    // allocate write buffer (owned by TaosFile)
    taosFile->writeBuf = (char *)taosMemoryMalloc(TAOS_FILE_WRITE_BUF_SIZE);
    taosFile->writeBufPos = 0;
    taosFile->writeBufCap = taosFile->writeBuf ? TAOS_FILE_WRITE_BUF_SIZE : 0;
    taosFile->writeBufOwned = true;

    // allocate compress buffer (reused across all blocks)
    taosFile->compressBuf = NULL;
    taosFile->compressBufCap = 0;

    // create
    taosFile->fp = taosOpenFile(fileName, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
    if (taosFile->fp == NULL) {
        logError("fopen file failed: %s", fileName);
        taosMemoryFree(taosFile->writeBuf);
        taosMemoryFree(taosFile);
        return NULL;
    }

    // write header
    *code = writeTaosFile(taosFile, &taosFile->header, sizeof(TaosFileHeader));
    if (*code != TSDB_CODE_SUCCESS) {
        closeTaosFile(taosFile);
        return NULL;
    }

    // write schema
    *code = writeTaosFile(taosFile, taosFile->header.schema, taosFile->header.schemaLen);
    if (*code != TSDB_CODE_SUCCESS) {
        closeTaosFile(taosFile);
        return NULL;
    }

    return taosFile;
}

int closeTaosFile(TaosFile* taosFile) {
    if (taosFile == NULL) {
        return TSDB_CODE_BCK_INVALID_PARAM;
    }

    if (taosFile->fp) {
        // flush remaining write buffer before updating header
        if (taosFile->writeBuf) {
            int flushCode = flushWriteBuffer(taosFile);
            if (flushCode != TSDB_CODE_SUCCESS) {
                logError("flush write buffer failed: %s", taosFile->fileName);
                taosCloseFile(&taosFile->fp);
                taosFile->fp = NULL;
                taosMemoryFree(taosFile);
                return flushCode;
            }
        }
        // update header
        taosLSeekFile(taosFile->fp, 0, SEEK_SET);
        // write header directly (bypass buffer since we seeked)
        int64_t wl = taosWriteFile(taosFile->fp, &taosFile->header, sizeof(TaosFileHeader));
        int code = (wl == sizeof(TaosFileHeader)) ? TSDB_CODE_SUCCESS : TSDB_CODE_BCK_WRITE_FILE_FAILED;
        if (code != TSDB_CODE_SUCCESS) {
            logError("update taos file header failed: %s", taosFile->fileName);
            if (taosFile->fp) taosCloseFile(&taosFile->fp);
            taosFile->fp = NULL;
            taosMemoryFree(taosFile);
            return code;
        }
        // close file
        taosCloseFile(&taosFile->fp);
        taosFile->fp = NULL;
    }

    // free write buffer (only if owned)
    if (taosFile->writeBuf && taosFile->writeBufOwned) {
        taosMemoryFree(taosFile->writeBuf);
        taosFile->writeBuf = NULL;
    }

    // free compress buffer
    if (taosFile->compressBuf) {
        taosMemoryFree(taosFile->compressBuf);
        taosFile->compressBuf = NULL;
    }

    // free memory
    taosMemoryFree(taosFile);

    return TSDB_CODE_SUCCESS;
}



//
// write block to taos file
//
int writeBlockToTaosFile(TaosFile*  taosFile,
                         void*      block,
                         int        blockRows,
                         FieldInfo* fieldInfos,
                         SBuffer*   assist,
                         int        numFields) {
    int code = TSDB_CODE_FAILED;

    // compress block into reusable buffer
    CompressBlock* compBlock = compressBlock(block,
                                             blockRows,
                                             fieldInfos,
                                             numFields,
                                             assist,
                                             &taosFile->compressBuf,
                                             &taosFile->compressBufCap,
                                             &code);
    if (code != TSDB_CODE_SUCCESS) {
        logError("compress block failed: %d", code);
        return code;
    }

//#define TEST_UNCOMPRESS
#ifdef TEST_UNCOMPRESS
    // test decompress
    SBuffer decompressBuf;
    tBufferInit(&decompressBuf);
    void* uncompressBlock = NULL;
    int32_t uncompressLen = 0;
    code = decompressBlock(compBlock, fieldInfos, numFields, &uncompressBlock, &uncompressLen, &decompressBuf);
    if (code != TSDB_CODE_SUCCESS) {
        logError("decompress block failed: %d", code);
        freeCompressData(compBlock);
        tBufferDestroy(&decompressBuf);
        return code;
    }

    // compare original block and uncompressed block
    if (uncompressLen != ((oriBlockHeader*)block)->actualLen ||
        memcmp(block, uncompressBlock, uncompressLen) != 0) {
        logError("uncompressed block data mismatch, uncompressLen: %d, originalLen: %d",
                 uncompressLen,
                 ((oriBlockHeader*)block)->actualLen);
        taosMemFree(uncompressBlock);
        freeCompressData(compBlock);
        tBufferDestroy(&decompressBuf);
        return TSDB_CODE_FAILED;
    }
    logDebug("block compress-decompress test passed, rows: %d, uncompressLen: %d", blockRows, uncompressLen);

    // cleanup
    taosMemFree(uncompressBlock);
    tBufferDestroy(&decompressBuf);

#endif

    // write to file
    code = writeTaosFile(taosFile, compBlock, sizeof(CompressBlock) + compBlock->dataLen);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    return TSDB_CODE_SUCCESS;
}


//
// write block to taos binary file
//
int resultToFileTaos(TAOS_RES *res, const char *fileName, char *writeBuf, int32_t writeBufCap, int64_t *outRows, volatile int64_t *progressCtr) {
    int code = TSDB_CODE_FAILED;
    if (outRows) *outRows = 0;

    int numFields = taos_num_fields(res);
    if (numFields <= 0) {
        logError("fields num is zero. errstr: %s", taos_errstr(res));
        return TSDB_CODE_BCK_NO_FIELDS;
    }
    TAOS_FIELD_E* fields = taos_fetch_fields_e(res);
    if (fields == NULL) {
        logError("fetch fields failed! errstr: %s", taos_errstr(res));
        return TSDB_CODE_BCK_FETCH_FIELDS_FAILED;
    }

    // create file (borrow writeBuf from caller if provided)
    TaosFile* taosFile = (TaosFile*)taosMemoryMalloc(sizeof(TaosFile) + sizeof(FieldInfo) * numFields);
    if (taosFile == NULL) {
        logError("malloc TaosFile failed");
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }
    memset(taosFile, 0, sizeof(TaosFile));

    taosFile->fileName = fileName;
    memcpy(taosFile->header.magic, TAOSFILE_MAGIC, 4);
    taosFile->header.version = TAOSFILE_VERSION;
    taosFile->header.numFields = numFields;
    taosFile->header.schemaLen = sizeof(FieldInfo) * numFields;
    fillFieldsInfo((FieldInfo*)taosFile->header.schema, fields, numFields);

    // use caller's writeBuf if provided, otherwise allocate
    if (writeBuf && writeBufCap > 0) {
        taosFile->writeBuf = writeBuf;
        taosFile->writeBufCap = writeBufCap;
        taosFile->writeBufOwned = false;
    } else {
        taosFile->writeBuf = (char *)taosMemoryMalloc(TAOS_FILE_WRITE_BUF_SIZE);
        taosFile->writeBufCap = taosFile->writeBuf ? TAOS_FILE_WRITE_BUF_SIZE : 0;
        taosFile->writeBufOwned = true;
    }
    taosFile->writeBufPos = 0;

    // allocate compress buffer (reused across all blocks)
    taosFile->compressBuf = NULL;
    taosFile->compressBufCap = 0;

    // open file
    taosFile->fp = taosOpenFile(fileName, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
    if (taosFile->fp == NULL) {
        logError("fopen file failed: %s", fileName);
        if (taosFile->writeBufOwned && taosFile->writeBuf) taosMemoryFree(taosFile->writeBuf);
        taosMemoryFree(taosFile);
        return TSDB_CODE_BCK_CREATE_FILE_FAILED;
    }

    // write header
    code = writeTaosFile(taosFile, &taosFile->header, sizeof(TaosFileHeader));
    if (code != TSDB_CODE_SUCCESS) {
        closeTaosFile(taosFile);
        return code;
    }

    // write schema
    code = writeTaosFile(taosFile, taosFile->header.schema, taosFile->header.schemaLen);
    if (code != TSDB_CODE_SUCCESS) {
        closeTaosFile(taosFile);
        return code;
    }

    // assistant buffer
    SBuffer assist;
    tBufferInit(&assist);

    // while fetch data
    int blockRows = 0;
    void *block = NULL;
    while (taos_fetch_raw_block(res, &blockRows, &block) == TSDB_CODE_SUCCESS) {
        if (g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            tBufferDestroy(&assist);
            closeTaosFile(taosFile);
            return code;
        }

        if (blockRows == 0 || block == NULL) {
            // no data
            break;
        }
        // write block to file
        code = writeBlockToTaosFile(taosFile,
                                    block,
                                    blockRows,
                                    (FieldInfo*)taosFile->header.schema,
                                    &assist,
                                    numFields);
        if (code != TSDB_CODE_SUCCESS) {
            logError("write data block to file failed(%d): %s", code, fileName);
            tBufferDestroy(&assist);
            closeTaosFile(taosFile);
            return code;
        }

        taosFile->header.nBlocks ++;
        taosFile->header.numRows += blockRows;
        if (progressCtr) atomic_add_fetch_64(progressCtr, blockRows);
    }

    // cleanup buffer
    tBufferDestroy(&assist);

    if (outRows) *outRows = taosFile->header.numRows;

    // close file
    code = closeTaosFile(taosFile);
    if (code != TSDB_CODE_SUCCESS) {
        logError("close Taos file failed(%d): %s", code, fileName);
        return code;
    }

    return TSDB_CODE_SUCCESS;
}


//
// ======================== READ .dat FILE ========================
//

static int readFromTaosFile(TaosFile* taosFile, void *data, int len) {
    if (taosFile == NULL || taosFile->fp == NULL) {
        logError("invalid TaosFile for read");
        return TSDB_CODE_BCK_READ_FILE_FAILED;
    }

    int64_t readLen = taosReadFile(taosFile->fp, data, len);
    if (readLen != (int64_t)len) {
        logError("read from file failed, readLen: %" PRId64 ", expectLen: %d", readLen, len);
        return TSDB_CODE_BCK_READ_FILE_FAILED;
    }

    return TSDB_CODE_SUCCESS;
}

TaosFile* openTaosFileForRead(const char *fileName, int *code) {
    // allocate with max schema space
    int maxFields = 4096;
    TaosFile* taosFile = (TaosFile*)taosMemoryMalloc(sizeof(TaosFile) + sizeof(FieldInfo) * maxFields);
    if (taosFile == NULL) {
        logError("malloc TaosFile failed");
        *code = TSDB_CODE_BCK_MALLOC_FAILED;
        return NULL;
    }
    memset(taosFile, 0, sizeof(TaosFile));
    taosFile->fileName = fileName;

    // open file
    taosFile->fp = taosOpenFile(fileName, TD_FILE_READ);
    if (taosFile->fp == NULL) {
        logError("open file for read failed: %s", fileName);
        taosMemoryFree(taosFile);
        *code = TSDB_CODE_BCK_READ_FILE_FAILED;
        return NULL;
    }

    // record file size once at open time (avoids a separate stat() call later)
    {
        int64_t fsz = 0;
        if (taosFStatFile(taosFile->fp, &fsz, NULL) == 0)
            taosFile->fileSize = fsz;
    }

    // read header
    *code = readFromTaosFile(taosFile, &taosFile->header, sizeof(TaosFileHeader));
    if (*code != TSDB_CODE_SUCCESS) {
        logError("read file header failed: %s", fileName);
        taosCloseFile(&taosFile->fp);
        taosMemoryFree(taosFile);
        return NULL;
    }

    // verify magic
    if (memcmp(taosFile->header.magic, TAOSFILE_MAGIC, 4) != 0) {
        logError("invalid file magic: %s", fileName);
        taosCloseFile(&taosFile->fp);
        taosMemoryFree(taosFile);
        *code = TSDB_CODE_BCK_INVALID_FILE;
        return NULL;
    }

    // read schema
    if (taosFile->header.schemaLen > 0) {
        *code = readFromTaosFile(taosFile, taosFile->header.schema, taosFile->header.schemaLen);
        if (*code != TSDB_CODE_SUCCESS) {
            logError("read file schema failed: %s", fileName);
            taosCloseFile(&taosFile->fp);
            taosMemoryFree(taosFile);
            return NULL;
        }
    }

    *code = TSDB_CODE_SUCCESS;
    return taosFile;
}

int readTaosFileBlocks(TaosFile *taosFile, BlockCallback callback, void *userData) {
    if (taosFile == NULL || taosFile->fp == NULL) {
        return TSDB_CODE_BCK_INVALID_PARAM;
    }

    int code = TSDB_CODE_SUCCESS;
    int numFields = taosFile->header.numFields;
    FieldInfo *fieldInfos = (FieldInfo *)taosFile->header.schema;

    SBuffer assist;
    tBufferInit(&assist);

    // Pre-allocated read buffer to avoid per-block malloc/free
    uint32_t readBufCap = 0;
    CompressBlock *readBuf = NULL;

    // Space-for-time: rawBlock is kept alive across blocks so that
    // decompressBlock can reuse the same allocation instead of doing
    // a fresh calloc + free on every iteration.
    void    *rawBlock    = NULL;
    int32_t  rawBlockCap = 0;

    for (uint32_t b = 0; b < taosFile->header.nBlocks; b++) {
        // read CompressBlock header (fixed part)
        CompressBlock compHeader;
        code = readFromTaosFile(taosFile, &compHeader, sizeof(CompressBlock));
        if (code != TSDB_CODE_SUCCESS) {
            logError("read compress block header failed at block %u", b);
            taosMemoryFree(rawBlock);
            taosMemoryFree(readBuf);
            tBufferDestroy(&assist);
            return code;
        }

        // grow read buffer if needed (reuse across blocks)
        uint32_t needSize = sizeof(CompressBlock) + compHeader.dataLen;
        if (needSize > readBufCap) {
            readBufCap = needSize + needSize / 4;  // grow 25% extra
            CompressBlock *newBuf = (CompressBlock *)taosMemoryRealloc(readBuf, readBufCap);
            if (newBuf == NULL) {
                logError("realloc read buffer failed, size: %u", readBufCap);
                taosMemoryFree(rawBlock);
                taosMemoryFree(readBuf);
                tBufferDestroy(&assist);
                return TSDB_CODE_BCK_MALLOC_FAILED;
            }
            readBuf = newBuf;
        }

        // copy header and read data directly into readBuf
        memcpy(readBuf, &compHeader, sizeof(CompressBlock));
        code = readFromTaosFile(taosFile, readBuf->data, compHeader.dataLen);
        if (code != TSDB_CODE_SUCCESS) {
            logError("read compress data failed at block %u", b);
            taosMemoryFree(rawBlock);
            taosMemoryFree(readBuf);
            tBufferDestroy(&assist);
            return code;
        }

        // decompress — pass rawBlock+rawBlockCap so decompressBlock can reuse
        // the allocation when the new block fits in the existing buffer.
        int32_t rawLen = rawBlockCap;  /* pass current capacity as hint */
        code = decompressBlock(readBuf, fieldInfos, numFields, &rawBlock, &rawLen, &assist);

        if (code != TSDB_CODE_SUCCESS) {
            logError("decompress block failed at block %u, code: %d", b, code);
            // rawBlock was freed inside decompressBlock on error (set to NULL)
            taosMemoryFree(readBuf);
            tBufferDestroy(&assist);
            return TSDB_CODE_BCK_DECOMPRESS_FAILED;
        }
        rawBlockCap = rawLen;  /* update capacity for next iteration */

        // callback with decompressed block — rawBlock is NOT freed here
        int32_t blockRows = compHeader.oriHeader.rows;
        code = callback(userData, fieldInfos, numFields, rawBlock, rawLen, blockRows);

        if (code != TSDB_CODE_SUCCESS) {
            logError("block callback failed at block %u, code: %d", b, code);
            taosMemoryFree(rawBlock);
            taosMemoryFree(readBuf);
            tBufferDestroy(&assist);
            return code;
        }
    }

    /* free the reused rawBlock once after all blocks are processed */
    taosMemoryFree(rawBlock);
    taosMemoryFree(readBuf);
    tBufferDestroy(&assist);
    return TSDB_CODE_SUCCESS;
}

int closeTaosFileRead(TaosFile *taosFile) {
    if (taosFile == NULL) {
        return TSDB_CODE_BCK_INVALID_PARAM;
    }

    if (taosFile->fp) {
        taosCloseFile(&taosFile->fp);
        taosFile->fp = NULL;
    }

    taosMemoryFree(taosFile);
    return TSDB_CODE_SUCCESS;
}
