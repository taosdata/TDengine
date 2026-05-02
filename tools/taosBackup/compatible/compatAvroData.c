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

#include "compatAvroSchema.h"
#include "compatAvroUtil.h"
#include "compatAvro.h"
#include <avro.h>
// Temporarily undef TDengine atomic overrides for jansson.h inline functions
#pragma push_macro("__atomic_add_fetch")
#pragma push_macro("__atomic_sub_fetch")
#undef __atomic_add_fetch
#undef __atomic_sub_fetch
#include <jansson.h>
#pragma pop_macro("__atomic_sub_fetch")
#pragma pop_macro("__atomic_add_fetch")
#include "bck.h"


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

// ---- Column buffer helpers for STMT2 column-wise binding ----

// Maximum bytes per fixed-width type
static int typeFixedBytes(int type) {
    switch (type) {
        case TSDB_DATA_TYPE_BOOL:      return sizeof(int8_t);
        case TSDB_DATA_TYPE_TINYINT:   return sizeof(int8_t);
        case TSDB_DATA_TYPE_SMALLINT:  return sizeof(int16_t);
        case TSDB_DATA_TYPE_INT:       return sizeof(int32_t);
        case TSDB_DATA_TYPE_BIGINT:    return sizeof(int64_t);
        case TSDB_DATA_TYPE_FLOAT:     return sizeof(float);
        case TSDB_DATA_TYPE_DOUBLE:    return sizeof(double);
        case TSDB_DATA_TYPE_TIMESTAMP: return sizeof(int64_t);
        case TSDB_DATA_TYPE_UTINYINT:  return sizeof(uint8_t);
        case TSDB_DATA_TYPE_USMALLINT: return sizeof(uint16_t);
        case TSDB_DATA_TYPE_UINT:      return sizeof(uint32_t);
        case TSDB_DATA_TYPE_UBIGINT:   return sizeof(uint64_t);
        default:                       return 0;  // variable length
    }
}

static bool isVarLenType(int type) {
    return (type == TSDB_DATA_TYPE_BINARY   || type == TSDB_DATA_TYPE_NCHAR     ||
            type == TSDB_DATA_TYPE_JSON     || type == TSDB_DATA_TYPE_VARBINARY ||
            type == TSDB_DATA_TYPE_GEOMETRY || type == TSDB_DATA_TYPE_BLOB      ||
            type == TSDB_DATA_TYPE_DECIMAL  || type == TSDB_DATA_TYPE_DECIMAL64);
}

// Column buffer for accumulating rows before batch insert
typedef struct {
    int      type;        // TSDB_DATA_TYPE_*
    int      fixedSize;   // bytes per element for fixed types, max length for var types
    char    *data;        // data buffer
    char    *isNull;      // null flags (1 byte per row)
    int32_t *lengths;     // per-row lengths (var-len only)
    int      capacity;    // max rows
    int      rows;        // current accumulated rows
} ColBuf;

static ColBuf *allocColBufs(int nCols, int batchSize, AvroTableDes *tableDes, int16_t *colMap) {
    ColBuf *bufs = (ColBuf *)taosMemoryCalloc(nCols, sizeof(ColBuf));
    if (!bufs) return NULL;

    for (int i = 0; i < nCols; i++) {
        int ci = colMap ? colMap[i] : i;
        bufs[i].type     = tableDes->cols[ci].type;
        bufs[i].capacity = batchSize;
        bufs[i].rows     = 0;
        bufs[i].isNull   = (char *)taosMemoryCalloc(batchSize, sizeof(char));

        int fb = typeFixedBytes(bufs[i].type);
        if (fb > 0) {
            bufs[i].fixedSize = fb;
            bufs[i].data = (char *)taosMemoryCalloc(batchSize, fb);
        } else {
            // Variable length: allocate with max column length from tableDes
            int maxLen = tableDes->cols[ci].length;
            if (maxLen <= 0) maxLen = 256;
            // DECIMAL is stored as string in AVRO but DESCRIBE returns binary
            // storage size (8 or 16). Use a generous buffer for the string.
            if (bufs[i].type == TSDB_DATA_TYPE_DECIMAL ||
                bufs[i].type == TSDB_DATA_TYPE_DECIMAL64) {
                maxLen = 64;  // enough for max DECIMAL precision + sign + dot
            }
            bufs[i].fixedSize = maxLen;
            bufs[i].data    = (char *)taosMemoryCalloc(batchSize, maxLen);
            bufs[i].lengths = (int32_t *)taosMemoryCalloc(batchSize, sizeof(int32_t));
        }

        if (!bufs[i].data || !bufs[i].isNull) {
            // Cleanup on failure
            for (int j = 0; j <= i; j++) {
                taosMemoryFree(bufs[j].data);
                taosMemoryFree(bufs[j].isNull);
                if (bufs[j].lengths) taosMemoryFree(bufs[j].lengths);
            }
            taosMemoryFree(bufs);
            return NULL;
        }
    }
    return bufs;
}

static void resetColBufs(ColBuf *bufs, int nCols) {
    for (int i = 0; i < nCols; i++) {
        memset(bufs[i].data, 0, (size_t)bufs[i].capacity * bufs[i].fixedSize);
        memset(bufs[i].isNull, 0, (size_t)bufs[i].capacity * sizeof(char));
        if (bufs[i].lengths)
            memset(bufs[i].lengths, 0, (size_t)bufs[i].capacity * sizeof(int32_t));
        bufs[i].rows = 0;
    }
}

static void freeColBufs(ColBuf *bufs, int nCols) {
    if (!bufs) return;
    for (int i = 0; i < nCols; i++) {
        taosMemoryFree(bufs[i].data);
        taosMemoryFree(bufs[i].isNull);
        if (bufs[i].lengths) taosMemoryFree(bufs[i].lengths);
    }
    taosMemoryFree(bufs);
}

// ---- Data extraction: AVRO value → column buffer row ----

// Check if an AVRO union value has the null branch active.
// In taosdump AVRO schema, unions are ["null", T], so null is discriminant 0.
static inline bool avroUnionIsNull(avro_value_t *unionVal) {
    int disc = -1;
    avro_value_get_discriminant(unionVal, &disc);
    return (disc == 0);
}

static void extractDataTimestamp(AvroFieldInfo *fi, avro_value_t *val,
                                 ColBuf *cb, int row) {
    int64_t ts = 0;
    if (fi->nullable) {
        if (avroUnionIsNull(val)) {
            cb->isNull[row] = 1;
            return;
        }
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        avro_value_get_long(&branch, &ts);
    } else {
        avro_value_get_long(val, &ts);
    }
    memcpy(cb->data + (int64_t)row * cb->fixedSize, &ts, sizeof(int64_t));
}

static void extractDataBool(AvroFieldInfo *fi, avro_value_t *val,
                            ColBuf *cb, int row) {
    avro_value_t branch;
    avro_value_get_current_branch(val, &branch);
    if (avroUnionIsNull(val)) {
        cb->isNull[row] = 1;
        return;
    }
    int32_t bl = 0;
    avro_value_get_boolean(&branch, &bl);
    int8_t v = (int8_t)(bl ? 1 : 0);
    memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(int8_t));
}

static void extractDataTinyInt(AvroFieldInfo *fi, avro_value_t *val,
                               ColBuf *cb, int row) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avroUnionIsNull(val)) { cb->isNull[row] = 1; return; }
        int32_t n = 0;
        avro_value_get_int(&branch, &n);
        int8_t v = (int8_t)n;
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(int8_t));
    } else {
        int32_t n = 0;
        avro_value_get_int(val, &n);
        if ((int8_t)n == (int8_t)TSDB_DATA_TINYINT_NULL) { cb->isNull[row] = 1; return; }
        int8_t v = (int8_t)n;
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(int8_t));
    }
}

static void extractDataSmallInt(AvroFieldInfo *fi, avro_value_t *val,
                                ColBuf *cb, int row) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avroUnionIsNull(val)) { cb->isNull[row] = 1; return; }
        int32_t n = 0;
        avro_value_get_int(&branch, &n);
        int16_t v = (int16_t)n;
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(int16_t));
    } else {
        int32_t n = 0;
        avro_value_get_int(val, &n);
        if ((int16_t)n == (int16_t)TSDB_DATA_SMALLINT_NULL) { cb->isNull[row] = 1; return; }
        int16_t v = (int16_t)n;
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(int16_t));
    }
}

static void extractDataInt(AvroFieldInfo *fi, avro_value_t *val,
                           ColBuf *cb, int row) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avroUnionIsNull(val)) { cb->isNull[row] = 1; return; }
        int32_t n = 0;
        avro_value_get_int(&branch, &n);
        if (n == (int32_t)TSDB_DATA_INT_NULL) { cb->isNull[row] = 1; return; }
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &n, sizeof(int32_t));
    } else {
        int32_t n = 0;
        avro_value_get_int(val, &n);
        if (n == (int32_t)TSDB_DATA_INT_NULL) { cb->isNull[row] = 1; return; }
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &n, sizeof(int32_t));
    }
}

static void extractDataBigInt(AvroFieldInfo *fi, avro_value_t *val,
                              ColBuf *cb, int row) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avroUnionIsNull(val)) { cb->isNull[row] = 1; return; }
        int64_t n = 0;
        avro_value_get_long(&branch, &n);
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &n, sizeof(int64_t));
    } else {
        int64_t n = 0;
        avro_value_get_long(val, &n);
        if (n == (int64_t)TSDB_DATA_BIGINT_NULL) { cb->isNull[row] = 1; return; }
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &n, sizeof(int64_t));
    }
}

static void extractDataFloat(AvroFieldInfo *fi, avro_value_t *val,
                             ColBuf *cb, int row) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avroUnionIsNull(val)) { cb->isNull[row] = 1; return; }
        float f = 0;
        avro_value_get_float(&branch, &f);
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &f, sizeof(float));
    } else {
        float f = 0;
        avro_value_get_float(val, &f);
        if (f == TSDB_DATA_FLOAT_NULL) { cb->isNull[row] = 1; return; }
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &f, sizeof(float));
    }
}

static void extractDataDouble(AvroFieldInfo *fi, avro_value_t *val,
                              ColBuf *cb, int row) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avroUnionIsNull(val)) { cb->isNull[row] = 1; return; }
        double d = 0;
        avro_value_get_double(&branch, &d);
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &d, sizeof(double));
    } else {
        double d = 0;
        avro_value_get_double(val, &d);
        if (d == TSDB_DATA_DOUBLE_NULL) { cb->isNull[row] = 1; return; }
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &d, sizeof(double));
    }
}

static void extractDataBinary(AvroFieldInfo *fi, avro_value_t *val,
                              ColBuf *cb, int row) {
    avro_value_t branch;
    avro_value_get_current_branch(val, &branch);
    char *buf = NULL;
    size_t sz = 0;
    avro_value_get_string(&branch, (const char **)&buf, &sz);
    if (!buf || sz == 0) {
        cb->isNull[row] = 1;
        return;
    }
    // Remove trailing null from AVRO string
    if (sz > 0 && buf[sz - 1] == '\0') sz--;
    int32_t copyLen = (int32_t)sz;
    if (copyLen > cb->fixedSize) copyLen = cb->fixedSize;
    memcpy(cb->data + (int64_t)row * cb->fixedSize, buf, copyLen);
    if (cb->lengths) cb->lengths[row] = copyLen;
}

static void extractDataNChar(AvroFieldInfo *fi, avro_value_t *val,
                             ColBuf *cb, int row) {
    void *buf = NULL;
    size_t sz = 0;
    avro_value_t branch;
    avro_value_get_current_branch(val, &branch);
    avro_value_get_bytes(&branch, (const void **)&buf, &sz);
    if (!buf) {
        cb->isNull[row] = 1;
        return;
    }
    int32_t copyLen = (int32_t)sz;
    if (copyLen > cb->fixedSize) copyLen = cb->fixedSize;
    memcpy(cb->data + (int64_t)row * cb->fixedSize, buf, copyLen);
    if (cb->lengths) cb->lengths[row] = copyLen;
}

static void extractDataBytes(AvroFieldInfo *fi, avro_value_t *val,
                             ColBuf *cb, int row) {
    void *buf = NULL;
    size_t sz = 0;
    avro_value_t branch;
    avro_value_get_current_branch(val, &branch);
    avro_value_get_bytes(&branch, (const void **)&buf, &sz);
    if (!buf) {
        cb->isNull[row] = 1;
        return;
    }
    int32_t copyLen = (int32_t)sz;
    if (copyLen > cb->fixedSize) copyLen = cb->fixedSize;
    memcpy(cb->data + (int64_t)row * cb->fixedSize, buf, copyLen);
    if (cb->lengths) cb->lengths[row] = copyLen;
}

// Unsigned types from AVRO array encoding
static void extractDataUnsigned(AvroFieldInfo *fi, avro_value_t *val,
                                ColBuf *cb, int row, int bytes) {
    avro_value_t src = *val;
    bool fromBranch = false;

    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avroUnionIsNull(val)) {
            cb->isNull[row] = 1;
            return;
        }
        src = branch;
        fromBranch = true;
    }

    size_t arrSz = 0;
    avro_value_get_size(&src, &arrSz);

    if (bytes <= 4) {
        uint64_t sum = 0;
        for (size_t j = 0; j < arrSz; j++) {
            avro_value_t item;
            avro_value_get_by_index(&src, j, &item, NULL);
            int32_t n = 0;
            avro_value_get_int(&item, &n);
            sum += (uint32_t)n;
        }
        if (bytes == 1) {
            uint8_t v = (uint8_t)sum;
            memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(uint8_t));
        } else if (bytes == 2) {
            uint16_t v = (uint16_t)sum;
            memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(uint16_t));
        } else {
            uint32_t v = (uint32_t)sum;
            memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(uint32_t));
        }
    } else {
        uint64_t sum = 0;
        for (size_t j = 0; j < arrSz; j++) {
            avro_value_t item;
            avro_value_get_by_index(&src, j, &item, NULL);
            int64_t n = 0;
            avro_value_get_long(&item, &n);
            sum += (uint64_t)n;
        }
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &sum, sizeof(uint64_t));
    }
}

// ==================== Core data import (STMT2) ====================

// Execute a batch via STMT2: convert ColBuf → TAOS_STMT2_BIND, bind + exec.
// Variable-length columns are repacked from fixed-stride to contiguous layout.
static int executeBatchStmt2(TAOS_STMT2 *stmt2, ColBuf *bufs, int nCols, int nRows) {
    TAOS_STMT2_BIND *binds2   = (TAOS_STMT2_BIND *)taosMemoryCalloc(nCols, sizeof(TAOS_STMT2_BIND));
    char           **contigBufs = (char    **)taosMemoryCalloc(nCols, sizeof(char *));
    int32_t        **contigLens = (int32_t **)taosMemoryCalloc(nCols, sizeof(int32_t *));
    if (!binds2 || !contigBufs || !contigLens) {
        taosMemoryFree(binds2);
        taosMemoryFree(contigBufs);
        taosMemoryFree(contigLens);
        return -1;
    }

    int retCode = 0;

    for (int i = 0; i < nCols; i++) {
        binds2[i].buffer_type = bufs[i].type;
        binds2[i].is_null     = bufs[i].isNull;
        binds2[i].num         = nRows;

        if (!isVarLenType(bufs[i].type)) {
            // Fixed-width: pass through directly
            binds2[i].buffer = bufs[i].data;
            binds2[i].length = NULL;
            continue;
        }

        // Variable-length (BINARY, NCHAR, DECIMAL, etc.):
        // STMT2 expects contiguous packing — row r starts at
        //   buffer + sum(length[0..r-1])
        // ColBuf stores fixed-stride (row r at r * fixedSize).
        // Repack here.
        int32_t totalLen = 0;
        for (int r = 0; r < nRows; r++) {
            totalLen += bufs[i].lengths[r];
        }

        char    *contig = (char    *)taosMemoryCalloc(1, totalLen > 0 ? totalLen : 1);
        int32_t *lens   = (int32_t *)taosMemoryCalloc(nRows, sizeof(int32_t));
        if (!contig || !lens) {
            taosMemoryFree(contig);
            taosMemoryFree(lens);
            retCode = -1;
            break;
        }

        int32_t offset = 0;
        for (int r = 0; r < nRows; r++) {
            int32_t rlen = bufs[i].lengths[r];
            lens[r] = rlen;
            if (rlen > 0 && !bufs[i].isNull[r]) {
                memcpy(contig + offset,
                       bufs[i].data + (int64_t)r * bufs[i].fixedSize, rlen);
                offset += rlen;
            }
        }

        binds2[i].buffer = contig;
        binds2[i].length = lens;
        contigBufs[i]    = contig;
        contigLens[i]    = lens;
    }

    if (retCode == 0) {
        TAOS_STMT2_BIND *bindsPtr = binds2;
        TAOS_STMT2_BINDV bv;
        memset(&bv, 0, sizeof(bv));
        bv.count     = 1;
        bv.tbnames   = NULL;
        bv.tags      = NULL;
        bv.bind_cols = &bindsPtr;

        retCode = taos_stmt2_bind_param(stmt2, &bv, -1);
        if (retCode != 0) {
            logError("avro data: stmt2 bind failed: %s", taos_stmt2_error(stmt2));
        }
    }

    if (retCode == 0) {
        int affected = 0;
        retCode = taos_stmt2_exec(stmt2, &affected);
        if (retCode != 0) {
            logError("avro data: stmt2 exec failed: %s", taos_stmt2_error(stmt2));
        }
    }

    for (int i = 0; i < nCols; i++) {
        taosMemoryFree(contigBufs[i]);
        taosMemoryFree(contigLens[i]);
    }
    taosMemoryFree(contigBufs);
    taosMemoryFree(contigLens);
    taosMemoryFree(binds2);
    return retCode;
}

int64_t avroRestoreDataImpl(AvroRestoreCtx *ctx,
                            TAOS *conn,
                            const char *dirPath,
                            const char *fileName,
                            AvroDBChange *pDbChange,
                            AvroStbChange *stbChange) {
    char avroFile[MAX_PATH_LEN];
    snprintf(avroFile, sizeof(avroFile), "%s/%s", dirPath, fileName);

    avro_file_reader_t reader;
    avro_schema_t schema;
    AvroRecordSchema *rs = avroGetSchemaFromFile(AVRO_TYPE_DATA, avroFile,
                                                  &schema, &reader);
    if (!rs) return -1;

    // Create a STMT2 handle per file
    TAOS_STMT2_OPTION stmt2Opt = {0};
    stmt2Opt.singleStbInsert = true;
    TAOS_STMT2 *stmt2 = taos_stmt2_init(conn, &stmt2Opt);
    if (!stmt2) {
        logError("avro data: stmt2_init failed for %s", fileName);
        avroFreeRecordSchema(rs);
        avro_schema_decref(schema);
        avro_file_reader_close(reader);
        return -1;
    }

    // Determine tableDes
    AvroTableDes *tableDes = stbChange ? stbChange->tableDes : NULL;
    AvroTableDes *mallocDes = NULL;

    // Compute bind column count
    int colAdj    = ctx->looseMode ? 0 : 1;
    int nBindCols = rs->numFields - colAdj;
    if (stbChange && stbChange->schemaChanged && stbChange->colBindMap) {
        nBindCols = stbChange->matchedCols;
    }

    int batchSize = ctx->dataBatch;
    if (batchSize <= 0) {
        batchSize = (argStmtVersion() == STMT_VERSION_2)
                    ? STMT2_BATCH_DEFAULT : STMT1_BATCH_DEFAULT;
    }

    // AVRO reader loop
    avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(iface, &value);

    int64_t totalRows = 0;
    int64_t failed    = 0;
    char   *tbName    = NULL;
    bool    prepared  = false;
    ColBuf *colBufs   = NULL;

    while (!avro_file_reader_read_value(reader, &value)) {
        // Extract tbName on first record
        if (!tbName) {
            if (!ctx->looseMode) {
                avro_value_t tbVal, tbBranch;
                avro_value_get_by_name(&value, "tbname", &tbVal, NULL);
                avro_value_get_current_branch(&tbVal, &tbBranch);
                char *avroName = NULL;
                size_t tbSz;
                avro_value_get_string(&tbBranch, (const char **)&avroName, &tbSz);
                tbName = taosStrdup(avroName);
            } else {
                tbName = (char *)taosMemoryCalloc(1, AVRO_TABLE_NAME_LEN + 1);
                char *dup = taosStrdup(fileName);
                char *run = dup;
                strsep(&run, ".");
                char *tb = strsep(&run, ".");
                if (tb) snprintf(tbName, AVRO_TABLE_NAME_LEN, "%s", tb);
                taosMemoryFree(dup);
            }

            // Handle normal table stbChange lookup
            if (!stbChange) {
                stbChange = avroFindStbChange(pDbChange, tbName);
                if (stbChange && stbChange->schemaChanged && stbChange->colBindMap) {
                    nBindCols = stbChange->matchedCols;
                    tableDes  = stbChange->tableDes;
                } else if (stbChange) {
                    nBindCols = stbChange->tableDes->columns;
                    tableDes  = stbChange->tableDes;
                }

                // If no stbChange found, detect NTB schema change on-the-fly
                if (!stbChange) {
                    AvroTableDes *ntbDes = avroAllocTableDes(TSDB_MAX_COLUMNS);
                    if (ntbDes && avroGetTableDes(conn, ctx->targetDb, tbName,
                                                  ntbDes, true) >= 0) {
                        int backupCols = rs->numFields - colAdj;
                        bool colsSame = (backupCols == ntbDes->columns);
                        if (colsSame) {
                            for (int ci = 0; ci < backupCols; ci++) {
                                if (strcasecmp(rs->fields[ci + colAdj].name,
                                               ntbDes->cols[ci].field) != 0) {
                                    colsSame = false;
                                    break;
                                }
                            }
                        }

                        // Legacy taosdump (e.g. v3.1.0.0) encodes column names
                        // as positional col0, col1, ... in the AVRO schema.
                        // When column count matches, detect this pattern and
                        // treat as positional match to avoid false schema-change.
                        if (!colsSame && backupCols == ntbDes->columns) {
                            bool positional = true;
                            for (int ci = 1; ci < backupCols; ci++) {
                                char expected[32];
                                snprintf(expected, sizeof(expected), "col%d", ci - 1);
                                if (strcasecmp(rs->fields[ci + colAdj].name, expected) != 0) {
                                    positional = false;
                                    break;
                                }
                            }
                            if (positional) {
                                colsSame = true;
                                logInfo("avro: %s uses legacy positional column "
                                        "names, mapping by position", tbName);
                            }
                        }

                        if (!colsSame) {
                            AvroStbChange *sc = (AvroStbChange *)taosMemoryCalloc(
                                    1, sizeof(AvroStbChange));
                            if (sc) {
                                sc->tableDes = ntbDes;
                                sc->schemaChanged = true;
                                sc->backupCols = backupCols;
                                int maxCols = ntbDes->columns > backupCols
                                            ? ntbDes->columns : backupCols;
                                int16_t *srvBind = (int16_t *)taosMemoryCalloc(
                                        ntbDes->columns, sizeof(int16_t));
                                sc->serverColMap = (int16_t *)taosMemoryCalloc(
                                        maxCols, sizeof(int16_t));
                                sc->colBindMap = (int16_t *)taosMemoryCalloc(
                                        backupCols, sizeof(int16_t));
                                if (srvBind && sc->serverColMap && sc->colBindMap) {
                                    char colStr[4096];
                                    char *cp = colStr;
                                    *cp++ = '(';
                                    int mc = 0;
                                    for (int si = 0; si < ntbDes->columns; si++) {
                                        srvBind[si] = -1;
                                        for (int bi = 0; bi < backupCols; bi++) {
                                            if (strcasecmp(ntbDes->cols[si].field,
                                                    rs->fields[bi+colAdj].name)==0) {
                                                if (mc > 0) *cp++ = ',';
                                                cp += sprintf(cp, "`%s`",
                                                              ntbDes->cols[si].field);
                                                srvBind[si] = (int16_t)mc;
                                                sc->serverColMap[mc] = (int16_t)si;
                                                mc++;
                                                break;
                                            }
                                        }
                                    }
                                    *cp++ = ')';
                                    *cp = '\0';
                                    sc->strCols = taosStrdup(colStr);
                                    for (int bi = 0; bi < backupCols; bi++) {
                                        sc->colBindMap[bi] = -1;
                                        for (int si = 0; si < ntbDes->columns; si++) {
                                            if (strcasecmp(rs->fields[bi+colAdj].name,
                                                    ntbDes->cols[si].field)==0) {
                                                sc->colBindMap[bi] = srvBind[si];
                                                break;
                                            }
                                        }
                                    }
                                    sc->matchedCols = mc;
                                    logInfo("avro: NTB %s schema changed: "
                                            "backup=%d matched=%d server=%d",
                                            tbName, backupCols, mc,
                                            ntbDes->columns);
                                }
                                taosMemoryFree(srvBind);
                                avroHashMapInsert(&pDbChange->stbMap, tbName, sc);
                                stbChange = sc;
                                nBindCols = sc->matchedCols;
                                tableDes  = sc->tableDes;
                                ntbDes = NULL;  // ownership transferred
                            }
                        }
                        if (ntbDes) avroFreeTableDes(ntbDes, true);
                    } else if (ntbDes) {
                        avroFreeTableDes(ntbDes, true);
                    }
                }
            }

            // Get tableDes if not available
            if (!tableDes) {
                if (rs->tableDes) {
                    tableDes = rs->tableDes;
                } else {
                    mallocDes = avroAllocTableDes(TSDB_MAX_COLUMNS);
                    if (!mallocDes) goto cleanup;
                    tableDes = mallocDes;
                }
            }

            if (mallocDes && (strlen(tableDes->name) == 0
                    || strcmp(tableDes->name, tbName) != 0)) {
                if (avroGetTableDes(conn, ctx->targetDb, tbName, mallocDes, true) < 0) {
                    goto cleanup;
                }
            }

            // Prepare STMT2 with fully-qualified table name
            {
                char stmtBuf[4096];
                char *p = stmtBuf;
                const char *partCols = "";
                if (stbChange && stbChange->schemaChanged && stbChange->strCols) {
                    partCols = stbChange->strCols;
                }
                p += snprintf(p, sizeof(stmtBuf),
                              "INSERT INTO `%s`.`%s` %s VALUES(?",
                              ctx->targetDb, tbName, partCols);
                for (int i = 1; i < nBindCols; i++) {
                    p += sprintf(p, ",?");
                }
                sprintf(p, ")");

                if (taos_stmt2_prepare(stmt2, stmtBuf, 0) != 0) {
                    logError("avro data: stmt2 prepare failed: %s",
                             taos_stmt2_error(stmt2));
                    goto cleanup;
                }
            }

            // Allocate column buffers
            int16_t *colMap = (stbChange && stbChange->schemaChanged && stbChange->serverColMap)
                            ? stbChange->serverColMap : NULL;
            colBufs = allocColBufs(nBindCols, batchSize, tableDes, colMap);
            if (!colBufs) {
                logError("avro data: alloc column buffers failed");
                goto cleanup;
            }

            prepared = true;
        }

        if (!prepared) continue;

        int rowIdx = colBufs[0].rows;

        // Extract each column
        int n = 0;
        bool useColBindMap = stbChange && stbChange->schemaChanged && stbChange->colBindMap;
        for (int i = 0; i < rs->numFields - colAdj; i++) {
            AvroFieldInfo *fi = &rs->fields[i + colAdj];
            if (rowIdx == 0 && totalRows == 0) {
                logDebug("avro extract: col[%d] fi->name=%s fi->type=%d fi->nullable=%d colAdj=%d",
                        i, fi->name, fi->type, fi->nullable, colAdj);
            }

            // Schema evolution filter
            if (useColBindMap) {
                if (i >= stbChange->backupCols || stbChange->colBindMap[i] < 0)
                    continue;
                n = stbChange->colBindMap[i];
            }

            avro_value_t fieldVal;
            if (i == 0) {
                // First column is always timestamp
                avro_value_get_by_name(&value, fi->name, &fieldVal, NULL);
                extractDataTimestamp(fi, &fieldVal, &colBufs[n], rowIdx);
            } else if (avro_value_get_by_name(&value, fi->name, &fieldVal, NULL) == 0) {
                int colIdx = (stbChange && stbChange->serverColMap) ? stbChange->serverColMap[n] : n;
                int colType = tableDes->cols[colIdx].type;
                switch (colType) {
                    case TSDB_DATA_TYPE_BOOL:
                        extractDataBool(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_TINYINT:
                        extractDataTinyInt(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_SMALLINT:
                        extractDataSmallInt(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_INT:
                        extractDataInt(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_BIGINT:
                        extractDataBigInt(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_TIMESTAMP:
                        extractDataTimestamp(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_FLOAT:
                        extractDataFloat(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_DOUBLE:
                        extractDataDouble(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_BINARY:
                    case TSDB_DATA_TYPE_DECIMAL:
                    case TSDB_DATA_TYPE_DECIMAL64:
                        extractDataBinary(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_NCHAR:
                    case TSDB_DATA_TYPE_JSON:
                        extractDataNChar(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_VARBINARY:
                    case TSDB_DATA_TYPE_GEOMETRY:
                    case TSDB_DATA_TYPE_BLOB:
                        extractDataBytes(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_UTINYINT:
                        extractDataUnsigned(fi, &fieldVal, &colBufs[n], rowIdx, 1);
                        break;
                    case TSDB_DATA_TYPE_USMALLINT:
                        extractDataUnsigned(fi, &fieldVal, &colBufs[n], rowIdx, 2);
                        break;
                    case TSDB_DATA_TYPE_UINT:
                        extractDataUnsigned(fi, &fieldVal, &colBufs[n], rowIdx, 4);
                        break;
                    case TSDB_DATA_TYPE_UBIGINT:
                        extractDataUnsigned(fi, &fieldVal, &colBufs[n], rowIdx, 8);
                        break;
                    default:
                        colBufs[n].isNull[rowIdx] = 1;
                        break;
                }
            } else {
                colBufs[n].isNull[rowIdx] = 1;
            }
            if (!useColBindMap) n++;
        }

        // Advance row counter for all columns
        for (int c = 0; c < nBindCols; c++) {
            colBufs[c].rows = rowIdx + 1;
        }
        totalRows++;

        // Execute batch when full
        if (colBufs[0].rows >= batchSize) {
            int code = executeBatchStmt2(stmt2, colBufs, nBindCols, colBufs[0].rows);
            if (code != 0) {
                failed += colBufs[0].rows;
            }
            resetColBufs(colBufs, nBindCols);
        }
    }

    // Flush remaining rows
    if (colBufs && colBufs[0].rows > 0) {
        int code = executeBatchStmt2(stmt2, colBufs, nBindCols, colBufs[0].rows);
        if (code != 0) {
            failed += colBufs[0].rows;
        }
    }

cleanup:
    if (colBufs) freeColBufs(colBufs, nBindCols);
    if (tbName) taosMemoryFree(tbName);
    taos_stmt2_close(stmt2);
    if (mallocDes) avroFreeTableDes(mallocDes, true);
    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    avroFreeRecordSchema(rs);
    avro_schema_decref(schema);
    avro_file_reader_close(reader);

    if (failed > 0) return -(int64_t)failed;
    return totalRows;
}
