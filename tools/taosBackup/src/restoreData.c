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
    
#include "restoreData.h"
#include "storageTaos.h"
#include "storageParquet.h"
#include "parquetBlock.h"
#include "colCompress.h"
#include "blockReader.h"
#include "bckPool.h"
#include "bckDb.h"
#include "bckSchemaChange.h"
#include "decimal.h"
#include "ttypes.h"
#include "osString.h"
#include "bckProgress.h"

//
// -------------------------------------- RESTORE CHECKPOINT -----------------------------------------
//

// Hash table for O(1) checkpoint lookup
typedef struct CkptHashEntry {
    char *filePath;
    struct CkptHashEntry *next;
} CkptHashEntry;

typedef struct {
    CkptHashEntry **buckets;
    int capacity;
    int count;
} CkptHashTable;

static pthread_mutex_t g_ckptMutex = PTHREAD_MUTEX_INITIALIZER;
static char  g_ckptPath[MAX_PATH_LEN] = "";
static CkptHashTable g_ckptHash = {0};
static TdFilePtr g_ckptFp = NULL;  // keep checkpoint file open
static bool  g_allowWriteCP = true;  // false when output dir is read-only

// Thread-local checkpoint buffer
typedef struct {
    char buf[16384];  // 16KB buffer
    int used;
} CkptBuffer;

static __thread CkptBuffer t_ckptBuf = {0};

static uint32_t hashString(const char *str) {
    uint32_t hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

static int nextPowerOf2(int n) {
    if (n <= 0) return 1;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    return n + 1;
}

static void initCkptHashTable(int estimatedFiles) {
    int capacity = nextPowerOf2(estimatedFiles * 2);
    if (capacity < 256) capacity = 256;

    g_ckptHash.buckets = (CkptHashEntry **)taosMemoryCalloc(capacity, sizeof(CkptHashEntry *));
    g_ckptHash.capacity = capacity;
    g_ckptHash.count = 0;
}

static void insertCkptHash(const char *filePath) {
    if (!g_ckptHash.buckets) return;

    uint32_t hash = hashString(filePath);
    int idx = hash & (g_ckptHash.capacity - 1);

    CkptHashEntry *entry = (CkptHashEntry *)taosMemoryMalloc(sizeof(CkptHashEntry));
    entry->filePath = taosStrdup(filePath);
    entry->next = g_ckptHash.buckets[idx];
    g_ckptHash.buckets[idx] = entry;
    g_ckptHash.count++;
}

static bool lookupCkptHash(const char *filePath) {
    if (!g_ckptHash.buckets) return false;

    uint32_t hash = hashString(filePath);
    int idx = hash & (g_ckptHash.capacity - 1);

    for (CkptHashEntry *e = g_ckptHash.buckets[idx]; e; e = e->next) {
        if (strcmp(e->filePath, filePath) == 0) return true;
    }
    return false;
}

static void freeCkptHashTable() {
    if (!g_ckptHash.buckets) return;

    for (int i = 0; i < g_ckptHash.capacity; i++) {
        CkptHashEntry *e = g_ckptHash.buckets[i];
        while (e) {
            CkptHashEntry *next = e->next;
            taosMemoryFree(e->filePath);
            taosMemoryFree(e);
            e = next;
        }
    }
    taosMemoryFree(g_ckptHash.buckets);
    g_ckptHash.buckets = NULL;
    g_ckptHash.capacity = 0;
    g_ckptHash.count = 0;
}

static void loadRestoreCheckpoint(const char *dbName) {
    snprintf(g_ckptPath, sizeof(g_ckptPath), "%s/%s/restore_checkpoint.txt", argOutPath(), dbName);
    g_allowWriteCP = true;

    // free previous
    freeCkptHashTable();
    if (g_ckptFp) {
        taosCloseFile(&g_ckptFp);
        g_ckptFp = NULL;
    }

    // Open checkpoint file for append (keep it open for the entire restore)
    g_ckptFp = taosOpenFile(g_ckptPath, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_APPEND);
    if (!g_ckptFp) {
        logInfo("restore checkpoint disabled: cannot write to %s (read-only directory), restoring without checkpoint", g_ckptPath);
        g_allowWriteCP = false;
        return;
    }

    // Without -C the caller never calls isRestoreDone(), so there is no need
    // to load previous entries into memory.
    if (!argCheckpoint()) return;

    // Read existing checkpoint file to populate hash table
    TdFilePtr rfp = taosOpenFile(g_ckptPath, TD_FILE_READ);
    if (!rfp) {
        // File doesn't exist yet or can't be read - start fresh
        initCkptHashTable(1024);
        return;
    }

    int64_t fileSize = 0;
    if (taosFStatFile(rfp, &fileSize, NULL) != 0 || fileSize <= 0) {
        taosCloseFile(&rfp);
        initCkptHashTable(1024);
        return;
    }

    char *buf = (char *)taosMemoryMalloc(fileSize + 1);
    if (!buf) {
        taosCloseFile(&rfp);
        initCkptHashTable(1024);
        return;
    }

    int64_t readLen = taosReadFile(rfp, buf, fileSize);
    taosCloseFile(&rfp);
    if (readLen <= 0) {
        taosMemoryFree(buf);
        initCkptHashTable(1024);
        return;
    }
    buf[readLen] = '\0';

    // Count lines for hash table sizing
    int lineCount = 0;
    for (int64_t i = 0; i < readLen; i++) {
        if (buf[i] == '\n') lineCount++;
    }

    initCkptHashTable(lineCount);

    // Parse lines and insert into hash table
    char *line = buf;
    while (*line) {
        char *eol = strchr(line, '\n');
        int len;
        if (eol) {
            len = (int)(eol - line);
            if (len > 0 && line[len-1] == '\r') len--;
        } else {
            len = strlen(line);
        }
        if (len > 0) {
            char path[MAX_PATH_LEN];
            if (len < MAX_PATH_LEN) {
                memcpy(path, line, len);
                path[len] = '\0';
                insertCkptHash(path);
            }
        }
        if (!eol) break;
        line = eol + 1;
    }

    taosMemoryFree(buf);
    logInfo("loaded restore checkpoint: %d files already done", g_ckptHash.count);
}

static bool isRestoreDone(const char *filePath) {
    return lookupCkptHash(filePath);
}

// Flush thread-local checkpoint buffer to file
static void flushCkptBuffer() {
    if (t_ckptBuf.used == 0) return;
    if (!g_allowWriteCP || !g_ckptFp) return;

    pthread_mutex_lock(&g_ckptMutex);
    if (g_ckptFp) {
        taosWriteFile(g_ckptFp, t_ckptBuf.buf, t_ckptBuf.used);
        taosFsyncFile(g_ckptFp);  // ensure data is written
    }
    pthread_mutex_unlock(&g_ckptMutex);

    t_ckptBuf.used = 0;
}

static void saveRestoreCheckpoint(const char *filePath) {
    if (!g_allowWriteCP) return;

    char line[MAX_PATH_LEN + 2];
    int len = snprintf(line, sizeof(line), "%s\n", filePath);

    // If buffer would overflow, flush first
    if (t_ckptBuf.used + len > sizeof(t_ckptBuf.buf)) {
        flushCkptBuffer();
    }

    // Append to thread-local buffer
    if (t_ckptBuf.used + len <= sizeof(t_ckptBuf.buf)) {
        memcpy(t_ckptBuf.buf + t_ckptBuf.used, line, len);
        t_ckptBuf.used += len;
    }
}

static void freeRestoreCheckpoint() {
    // Flush any remaining buffered checkpoint data
    flushCkptBuffer();

    // Close the global checkpoint file
    if (g_ckptFp) {
        taosCloseFile(&g_ckptFp);
        g_ckptFp = NULL;
    }

    // Free hash table
    freeCkptHashTable();
}

// Delete the checkpoint file after a fully successful restore so that the
// next restore run always starts fresh instead of skipping everything.
static void deleteRestoreCheckpoint() {
    if (g_allowWriteCP && g_ckptPath[0] != '\0') {
        int ret = taosRemoveFile(g_ckptPath);
        logDebug("deleted restore checkpoint: %s ret:%d", g_ckptPath, ret);
    }
}

//
// -------------------------------------- STMT UTIL -----------------------------------------
//

// Maximum rows per STMT2 bind_param call — equals STMT2_BATCH_MAX from bckArgs.h.
// Kept here as a local alias so the comment context remains self-contained.
// taos_stmt2_bind_param enforces bind->num <= INT16_MAX (32767); STMT2_BATCH_MAX
// (16384) is a safe conservative bound well below that limit.
#define STMT2_MAX_ROWS_PER_BATCH  STMT2_BATCH_MAX

// Minimum row-buffer capacity: must hold at least one complete raw TDengine block.
// TDengine's storage engine limits each compressed block to at most 4096 rows
// (MAXROWS in the engine internals).  stmt2AllocColBuffers enforces this floor so
// that stmt2AccBlock can always accumulate a full block before the first flush,
// even when --data-batch is set to a value smaller than 4096.
#define STMT2_MIN_ROW_BUF_CAP  4096

//
// ----------------------------- STMT2 RESTORE CONTEXT ---------------------------------
//
// Uses TAOS_STMT2 (TDengine v3.3+ native API) for fast batch inserts:
//   - No add_batch() step   : bind_param() + exec() is the whole flow
//   - per-file prepare      : INSERT INTO `db`.`tblname` VALUES(?,?,?...)
//                             The table name is embedded in the SQL so that the
//                             WebSocket driver (libtaosws.so, Rust) can resolve
//                             the schema at prepare time.  Native mode is also
//                             correct since stmtPrepare2() just stores the SQL.
//
// Per-thread workflow:
//   per file:  close prev stmt2 (if any) → init new stmt2 → prepare with tbname
//              read all blocks → accumulate into colBinds[] buffers
//              → taos_stmt2_bind_param (tbnames=NULL) + taos_stmt2_exec
//   cleanup   → taos_stmt2_close

typedef struct {
    TAOS_STMT2  *stmt2;
    TAOS        *conn;
    const char  *dbName;
    char         tbName[TSDB_TABLE_NAME_LEN];
    int          numFields;
    FieldInfo   *fieldInfos;    // points into the TaosFile header (valid while file is open)
    int64_t      totalRows;     // rows successfully inserted this file (stats)
    int64_t      accRows;       // rows accumulated in colBinds[] so far

    // Per-column accumulation buffers, allocated once and reused across files.
    // Fixed-type  col i: buffer = flat [rowBufCap * typeBytes]; length = NULL
    // Variable-type col i: buffer = PACKED layout (rows are written back-to-back);
    //                       length = int32_t[rowBufCap] (actual per-row length)
    //                       is_null = char[rowBufCap] (1 = NULL)
    // STMT2 reads variable-type data in packed layout: current_buf advances by
    // length[j] for each row j (see clientStmt2.c).
    TAOS_STMT2_BIND *colBinds;
    int              colBindsCap;  // how many columns the colBinds array was allocated for
    int64_t          rowBufCap;    // row capacity of each column buffer
    int32_t         *varWriteOffsets; // packed write position per var-type column (size = colBindsCap)
    int32_t         *varBufCapacity;  // allocated byte capacity per var/decimal buffer (for dynamic growth)

    StbChange   *stbChange;
    bool         prepared;         // stmt2 has been prepared (once per thread lifetime)

    // Multi-table batch accumulation (native mode only; not used for WebSocket/DSN).
    // Each PendingTableSlot holds one CTB's column buffers after sealing from ctx.
    // Flush is triggered when numPending == STMT2_MULTI_TABLE_PENDING or
    // totalPendingRows >= effective batchCap.
    bool             multiTable;          // true when batching multiple CTBs
    struct Stmt2TableSlot *pendingSlots;  // dynamic array [STMT2_MULTI_TABLE_PENDING]
    int              numPending;          // slots currently filled
    int64_t          totalPendingRows;    // sum of numRows across all pending slots
} Stmt2RestoreCtx;

// Maximum number of CTBs (child-table files) accumulated into a single
// TAOS_STMT2_BINDV call before flushing to the server.
#define STMT2_MULTI_TABLE_PENDING  64

// One pending slot: holds the accumulated colBinds for a single CTB file
// that has been sealed (read from disk) but not yet sent to the server.
// Buffer ownership is transferred from Stmt2RestoreCtx on sealing and
// returned to the heap after stmt2FlushMultiTableSlots.
typedef struct Stmt2TableSlot {
    char              fqn[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + 8]; // `db`.`tbl`
    TAOS_STMT2_BIND  *colBinds;       // per-column arrays (owns all buffer/length/is_null)
    int               colBindsCap;    // allocated column count in colBinds
    int               numCols;        // rows' column binding count (<= colBindsCap)
    int32_t           numRows;        // accumulated rows in this slot
    int32_t          *varWriteOffsets;// packed byte-write offsets per var-type column
    int32_t          *varBufCapacity; // byte capacity per var-type column
} Stmt2TableSlot;


//
// ----------------------------- STMT1 (legacy) RESTORE CONTEXT -----------------------
//
// Classic TAOS_STMT path.  Kept for compatibility / benchmarking via -I 1.
//
#define STMT_BATCH_THRESHOLD STMT1_BATCH_DEFAULT  // default execute-threshold (rows before taos_stmt_execute)

typedef struct {
    TAOS_STMT  *stmt;
    TAOS       *conn;
    const char *dbName;
    char        tbName[TSDB_TABLE_NAME_LEN];
    int         numFields;
    FieldInfo  *fieldInfos;
    int64_t     totalRows;
    int64_t     batchRows;
    int64_t     pendingRows;   // cross-file accumulated rows (threshold for multi-table batch flush)
    bool        prepared;
    TAOS_MULTI_BIND *bindArray;
    int              bindArrayCap;
    StbChange  *stbChange;
} StmtRestoreCtx;

// init stmt
TAOS_STMT* initStmt(TAOS* taos, bool single) {
    if (!single) {
        return taos_stmt_init(taos);
    }

    TAOS_STMT_OPTIONS op;
    memset(&op, 0, sizeof(op));
    op.singleStbInsert      = single;
    op.singleTableBindOnce  = single;
    return taos_stmt_init_with_options(taos, &op);
}

//
// Prepare STMT for the first file in a thread:
//   INSERT INTO ? VALUES(?,?,?...)
// The `?` placeholder for the table name is a TDengine extension that enables
// efficient child-table switching via taos_stmt_set_tbname() without a
// full re-prepare round-trip.
//
static int stmtPrepareInsert(StmtRestoreCtx *ctx) {
    char *stmtBuf = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
    if (stmtBuf == NULL) {
        logError("malloc stmt buffer failed");
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    // Determine number of bind columns (may be fewer with schema change)
    const char *partCols = "";
    int bindColCount = ctx->numFields;
    if (ctx->stbChange && ctx->stbChange->schemaChanged && ctx->stbChange->partColsStr) {
        partCols = ctx->stbChange->partColsStr;
        bindColCount = ctx->stbChange->matchColCount;
        logInfo("partial column write for %s.%s: %s (bind %d of %d cols)",
                ctx->dbName, ctx->tbName, partCols, bindColCount, ctx->numFields);
    }

    // Always use ? for the table name so that taos_stmt_set_tbname() can
    // switch child tables without a new prepare round-trip.
    char *p = stmtBuf;
    p += snprintf(p, TSDB_MAX_SQL_LEN, "INSERT INTO ? %s VALUES(?", partCols);
    for (int i = 1; i < bindColCount; i++) {
        p += sprintf(p, ",?");
    }
    p += sprintf(p, ")");

    logDebug("stmt prepare: %s", stmtBuf);

    int ret = taos_stmt_prepare(ctx->stmt, stmtBuf, 0);
    if (ret != 0) {
        logError("taos_stmt_prepare failed: %s, sql: %s", taos_stmt_errstr(ctx->stmt), stmtBuf);
        taosMemoryFree(stmtBuf);
        return ret;
    }
    taosMemoryFree(stmtBuf);

    // Immediately bind the first child table name
    char fqn[TSDB_DB_NAME_LEN + 1 + TSDB_TABLE_NAME_LEN + 4];
    snprintf(fqn, sizeof(fqn), "`%s`.`%s`", ctx->dbName, ctx->tbName);
    ret = taos_stmt_set_tbname(ctx->stmt, fqn);
    if (ret != 0) {
        logError("taos_stmt_set_tbname (initial) failed (%s): %s", fqn, taos_stmt_errstr(ctx->stmt));
        return ret;
    }

    ctx->prepared = true;
    return TSDB_CODE_SUCCESS;
}

//
// Switch an already-prepared STMT to a different child table.
// Uses taos_stmt_set_tbname() which avoids a full re-prepare round-trip.
// The STMT must already have been prepared for a table with the same
// column count (i.e. same super table).
//
static int stmtSwitchTable(StmtRestoreCtx *ctx, const char *tbName) {
    /* fully qualified name required by set_tbname */
    char fqn[TSDB_DB_NAME_LEN + 1 + TSDB_TABLE_NAME_LEN + 2 + 2];
    snprintf(fqn, sizeof(fqn), "`%s`.`%s`", ctx->dbName, tbName);
    int ret = taos_stmt_set_tbname(ctx->stmt, fqn);
    if (ret != 0) {
        logError("taos_stmt_set_tbname failed (%s): %s", fqn, taos_stmt_errstr(ctx->stmt));
        return ret;
    }
    strncpy(ctx->tbName, tbName, TSDB_TABLE_NAME_LEN - 1); /* update for error messages */
    ctx->tbName[TSDB_TABLE_NAME_LEN - 1] = '\0';
    ctx->batchRows = 0;
    return TSDB_CODE_SUCCESS;
}


//
// Reset STMT after an error.
// Closes the broken stmt, allocates a fresh one, and marks context as
// not-prepared so the next file will call stmtPrepareInsert() again.
// Returns true on success, false if taos_stmt_init also fails.
//
static bool stmtResetOnError(StmtRestoreCtx *ctx) {
    if (ctx->stmt) {
        taos_stmt_close(ctx->stmt);
        ctx->stmt = NULL;
    }
    ctx->prepared    = false;
    ctx->batchRows   = 0;
    ctx->pendingRows = 0;
    TAOS_STMT *newStmt = initStmt(ctx->conn, true);
    if (newStmt == NULL) {
        logError("stmtResetOnError: initStmt failed — connection may be broken");
        return false;
    }
    ctx->stmt = newStmt;
    return true;
}

//
// Free bind array buffers
//
static void freeBindArray(TAOS_MULTI_BIND *bindArray, int numFields) {
    for (int i = 0; i < numFields; i++) {
        TAOS_MULTI_BIND *bind = &bindArray[i];
        if (bind->buffer) {
            taosMemoryFree(bind->buffer);
            bind->buffer = NULL;
        }
        if (bind->length) {
            taosMemoryFree(bind->length);
            bind->length = NULL;
        }
        if (bind->is_null) {
            taosMemoryFree(bind->is_null);
            bind->is_null = NULL;
        }
    }
}


//
// Build TAOS_MULTI_BIND array for batch rows from a decompressed raw block
// Uses column-batch binding (all rows for each column at once) for efficiency
//
static int bindBlockData(StmtRestoreCtx *ctx, 
                         void *blockData, 
                         int32_t blockRows,
                         FieldInfo *fieldInfos,
                         int numFields,
                         TAOS_MULTI_BIND *bindArray) {
    // Parse block
    BlockReader reader;
    int32_t code = initBlockReader(&reader, blockData);
    if (code != TSDB_CODE_SUCCESS) {
        logError("init block reader failed: %d", code);
        return code;
    }

    // Process each column, skipping columns not in partial write set
    for (int c = 0; c < numFields; c++) {
        // Determine bind index (with schema change, some backup cols are skipped)
        int bindIdx = getPartialWriteBindIdx(ctx->stbChange, c);
        if (bindIdx < 0) {
            // This column is not in the server schema, skip but still advance reader
            void *skipData = NULL;
            int32_t skipLen = 0;
            code = getColumnData(&reader, fieldInfos[c].type, &skipData, &skipLen);
            if (code != TSDB_CODE_SUCCESS) {
                logError("skip column data failed: col=%d", c);
                return code;
            }
            continue;
        }
        void *colData = NULL;
        int32_t colDataLen = 0;
        code = getColumnData(&reader, fieldInfos[c].type, &colData, &colDataLen);
        if (code != TSDB_CODE_SUCCESS) {
            logError("get column data failed: col=%d", c);
            return code;
        }

        TAOS_MULTI_BIND *bind = &bindArray[bindIdx];
        memset(bind, 0, sizeof(TAOS_MULTI_BIND));
        bind->buffer_type = fieldInfos[c].type;
        bind->num = blockRows;

        if (IS_VAR_DATA_TYPE(fieldInfos[c].type)) {
            // Variable type layout: offsets[blockRows] (int32_t each) + raw data
            int32_t *offsets = (int32_t *)colData;
            char *varDataBase = (char *)colData + blockRows * sizeof(int32_t);
            bool isNchar = (fieldInfos[c].type == TSDB_DATA_TYPE_NCHAR);
            
            // Allocate buffers for variable data
            // We need: buffer (concatenated data), length array, is_null array
            int32_t *lengths = (int32_t *)taosMemoryCalloc(blockRows, sizeof(int32_t));
            char    *isNull  = (char *)taosMemoryCalloc(blockRows, sizeof(char));
            if (!lengths || !isNull) {
                taosMemoryFree(lengths);
                taosMemoryFree(isNull);
                return TSDB_CODE_BCK_MALLOC_FAILED;
            }

            // For NCHAR: raw block stores UCS-4 (4 bytes/char), STMT expects UTF-8.
            // We need a temp buffer to convert each row, then measure actual UTF-8 lengths.
            // For other var types (BINARY/VARCHAR): data is already UTF-8, just copy.

            // First pass: calculate lengths and nulls
            // For NCHAR, we'll also convert to UTF-8 into a temp area.
            // Allocate a temp conversion buffer (generous: same size as UCS-4 data)
            char *convBuf = NULL;
            int32_t *utf8Lens = NULL;  // actual UTF-8 byte lengths per row
            if (isNchar) {
                // max UTF-8 bytes = ucs4 bytes (worst case: each UCS-4 char -> 4 UTF-8 bytes, but ucs4 is already 4 bytes)
                int32_t maxConvBuf = fieldInfos[c].bytes * blockRows; // generous
                convBuf = (char *)taosMemoryCalloc(1, maxConvBuf > 0 ? maxConvBuf : 1);
                utf8Lens = (int32_t *)taosMemoryCalloc(blockRows, sizeof(int32_t));
                if (!convBuf || !utf8Lens) {
                    taosMemoryFree(lengths);
                    taosMemoryFree(isNull);
                    taosMemoryFree(convBuf);
                    taosMemoryFree(utf8Lens);
                    return TSDB_CODE_BCK_MALLOC_FAILED;
                }
            }

            int32_t convOffset = 0;
            for (int row = 0; row < blockRows; row++) {
                if (offsets[row] < 0) {
                    isNull[row] = 1;
                    lengths[row] = 0;
                } else {
                    isNull[row] = 0;
                    // TDengine var data: 2-byte length prefix + data
                    uint16_t varLen = *(uint16_t *)(varDataBase + offsets[row]);
                    if (isNchar && varLen > 0) {
                        // Convert UCS-4 -> UTF-8
                        char *ucs4Data = varDataBase + offsets[row] + sizeof(uint16_t);
                        char *utf8Out = convBuf + convOffset;
                        int32_t utf8Len = taosUcs4ToMbs((TdUcs4 *)ucs4Data, varLen, utf8Out, NULL);
                        if (utf8Len < 0) {
                            logError("UCS-4 to UTF-8 conversion failed: col=%d row=%d", c, row);
                            utf8Len = 0;
                        }
                        utf8Lens[row] = utf8Len;
                        lengths[row] = utf8Len;
                        convOffset += utf8Len;
                    } else {
                        lengths[row] = varLen;
                    }
                }
            }

            // Allocate buffer and copy data contiguously
            // For TAOS_MULTI_BIND with variable types, each row's data must be at
            // buffer + row * buffer_length, so we use max length as buffer_length
            int32_t maxLen = 0;
            for (int row = 0; row < blockRows; row++) {
                if (lengths[row] > maxLen) maxLen = lengths[row];
            }
            if (maxLen == 0) maxLen = 1; // minimum 1 byte

            char *buffer = (char *)taosMemoryCalloc(blockRows, maxLen);
            if (!buffer) {
                taosMemoryFree(lengths);
                taosMemoryFree(isNull);
                taosMemoryFree(convBuf);
                taosMemoryFree(utf8Lens);
                return TSDB_CODE_BCK_MALLOC_FAILED;
            }

            if (isNchar) {
                // Copy converted UTF-8 data from convBuf
                int32_t srcOff = 0;
                for (int row = 0; row < blockRows; row++) {
                    if (!isNull[row] && utf8Lens[row] > 0) {
                        memcpy(buffer + row * maxLen, convBuf + srcOff, utf8Lens[row]);
                        srcOff += utf8Lens[row];
                    }
                }
                taosMemoryFree(convBuf);
                taosMemoryFree(utf8Lens);
            } else {
                for (int row = 0; row < blockRows; row++) {
                    if (!isNull[row] && lengths[row] > 0) {
                        char *src = varDataBase + offsets[row] + sizeof(uint16_t);
                        memcpy(buffer + row * maxLen, src, lengths[row]);
                    }
                }
            }

            bind->buffer = buffer;
            bind->buffer_length = maxLen;
            bind->length = lengths;
            bind->is_null = isNull;

        } else if (IS_DECIMAL_TYPE(fieldInfos[c].type)) {
            /* DECIMAL: raw block has bitmap[(blockRows+7)/8] + fixed bytes
             * (8 for DECIMAL, 16 for DECIMAL128) per row.
             * STMT1 requires buffer_type == column type and raw binary data. */
            int32_t actualBytes = fieldGetRawBytes(&fieldInfos[c]);
            int32_t bitmapLen   = (blockRows + 7) / 8;
            char   *bitmap      = (char *)colData;
            char   *fixData     = (char *)colData + bitmapLen;

            char    *buffer  = (char *)taosMemoryCalloc(blockRows, actualBytes);
            int32_t *lengths = (int32_t *)taosMemoryCalloc(blockRows, sizeof(int32_t));
            char    *isNull  = (char *)taosMemoryCalloc(blockRows, sizeof(char));
            if (!buffer || !lengths || !isNull) {
                taosMemoryFree(buffer);
                taosMemoryFree(lengths);
                taosMemoryFree(isNull);
                return TSDB_CODE_BCK_MALLOC_FAILED;
            }

            for (int row = 0; row < blockRows; row++) {
                if (bitmap[row >> 3] & (1u << (7 - (row & 7)))) {
                    isNull[row]  = 1;
                    lengths[row] = 0;
                } else {
                    isNull[row]  = 0;
                    lengths[row] = actualBytes;
                    memcpy(buffer + row * actualBytes,
                           fixData + row * actualBytes,
                           actualBytes);
                }
            }

            /* buffer_type was already set to fieldInfos[c].type (DECIMAL/DECIMAL128)
             * above; keep it — pass raw binary to STMT1. */
            bind->buffer = buffer;
            bind->buffer_length = actualBytes;
            bind->length = lengths;
            bind->is_null = isNull;
        } else {
            // Fixed type layout: bitmap[(blockRows+7)/8] + data[blockRows * bytes]
            int32_t bitmapLen = (blockRows + 7) / 8;
            char *bitmap = (char *)colData;
            char *fixData = (char *)colData + bitmapLen;
            int32_t typeBytes = fieldInfos[c].bytes;

            // Allocate buffer
            char *buffer = (char *)taosMemoryMalloc(blockRows * typeBytes);
            char *isNull = (char *)taosMemoryCalloc(blockRows, sizeof(char));
            if (!buffer || !isNull) {
                taosMemoryFree(buffer);
                taosMemoryFree(isNull);
                return TSDB_CODE_BCK_MALLOC_FAILED;
            }

            // Copy data and check null bitmap (TDengine: bit7=row0, MSB-first)
            memcpy(buffer, fixData, blockRows * typeBytes);
            for (int row = 0; row < blockRows; row++) {
                int byteIdx = row >> 3;
                int bitIdx  = 7 - (row & 7);
                if (bitmap[byteIdx] & (1 << bitIdx)) {
                    isNull[row] = 1;
                } else {
                    isNull[row] = 0;
                }
            }

            bind->buffer = buffer;
            bind->buffer_length = typeBytes;
            bind->length = NULL;  // fixed-length types don't need length array
            bind->is_null = isNull;
        }
    }

    return TSDB_CODE_SUCCESS;
}


//
// Callback: process each decompressed data block with STMT batch insert
//
static int dataBlockCallback(void *userData, 
                             FieldInfo *fieldInfos, 
                             int numFields,
                             void *blockData, 
                             int32_t blockLen, 
                             int32_t blockRows) {
    StmtRestoreCtx *ctx = (StmtRestoreCtx *)userData;
    int code = TSDB_CODE_SUCCESS;

    if (g_interrupted) {
        return TSDB_CODE_BCK_USER_CANCEL;
    }

    if (blockRows == 0) {
        return TSDB_CODE_SUCCESS;
    }

    // Prepare STMT if not yet done
    if (!ctx->prepared) {
        code = stmtPrepareInsert(ctx);
        if (code != TSDB_CODE_SUCCESS) {
            return code;
        }
    }

    // Determine actual bind column count (may differ from numFields with schema change)
    int bindColCount = numFields;
    if (ctx->stbChange && ctx->stbChange->schemaChanged) {
        bindColCount = ctx->stbChange->matchColCount;
    }

    // Ensure pre-allocated bind array is large enough
    if (ctx->bindArray == NULL || ctx->bindArrayCap < bindColCount) {
        if (ctx->bindArray) {
            freeBindArray(ctx->bindArray, ctx->bindArrayCap);
            taosMemoryFree(ctx->bindArray);
        }
        ctx->bindArray = (TAOS_MULTI_BIND *)taosMemoryCalloc(bindColCount, sizeof(TAOS_MULTI_BIND));
        ctx->bindArrayCap = bindColCount;
        if (ctx->bindArray == NULL) {
            return TSDB_CODE_BCK_MALLOC_FAILED;
        }
    } else {
        // free previous column buffers but reuse the array
        freeBindArray(ctx->bindArray, ctx->bindArrayCap);
        memset(ctx->bindArray, 0, bindColCount * sizeof(TAOS_MULTI_BIND));
    }

    // Build bind array from block data (column-batch mode)
    code = bindBlockData(ctx, blockData, blockRows, fieldInfos, numFields, ctx->bindArray);
    if (code != TSDB_CODE_SUCCESS) {
        logError("bind block data failed: %d", code);
        return code;
    }

    // Bind parameters
    code = taos_stmt_bind_param_batch(ctx->stmt, ctx->bindArray);
    if (code != 0) {
        logError("taos_stmt_bind_param_batch failed: %s, table: %s", 
                 taos_stmt_errstr(ctx->stmt), ctx->tbName);
        return code;
    }

    // Add batch
    code = taos_stmt_add_batch(ctx->stmt);
    if (code != 0) {
        logError("taos_stmt_add_batch failed: %s", taos_stmt_errstr(ctx->stmt));
        return code;
    }

    ctx->totalRows += blockRows;
    ctx->batchRows += blockRows;
    ctx->pendingRows += blockRows;   /* cross-file/cross-table row accumulator */

    // Execute when cross-file accumulated rows reach the batch threshold.
    // Using pendingRows (not batchRows) ensures sparse CTBs with 1-2 rows each
    // are batched across many files before an execute RPC is issued.
    int64_t batchThreshold = (argDataBatch() > 0) ? (int64_t)argDataBatch() : STMT_BATCH_THRESHOLD;
    if (ctx->pendingRows >= batchThreshold) {
        code = taos_stmt_execute(ctx->stmt);
        if (code != 0) {
            logError("taos_stmt_execute failed: %s, table: %s.%s, batchRows: %" PRId64, 
                     taos_stmt_errstr(ctx->stmt), ctx->dbName, ctx->tbName, ctx->batchRows);
            return code;
        }
        ctx->batchRows   = 0;
        ctx->pendingRows = 0;  /* reset cross-file counter after execute */
    }

    return TSDB_CODE_SUCCESS;
}


// =====================================================================
//  STMT2 (TAOS_STMT2) implementation
// =====================================================================

// init stmt2
TAOS_STMT2* initStmt2(TAOS* taos, bool single) {
    TAOS_STMT2_OPTION op2;
    memset(&op2, 0, sizeof(op2));
    op2.singleStbInsert      = single;
    op2.singleTableBindOnce  = single;

    TAOS_STMT2* stmt2 = taos_stmt2_init(taos, &op2);
    if (stmt2)
        logDebug("succ  taos_stmt2_init single=%d\n", single);
    else
        logError("failed taos_stmt2_init single=%d\n", single);
    return stmt2;
}

//
// Free all per-column buffers inside ctx->colBinds.
// The colBinds array itself is freed separately.
//
static void stmt2FreeColBuffers(Stmt2RestoreCtx *ctx) {
    if (!ctx->colBinds) return;
    for (int i = 0; i < ctx->colBindsCap; i++) {
        TAOS_STMT2_BIND *b = &ctx->colBinds[i];
        taosMemoryFree(b->buffer);  b->buffer  = NULL;
        taosMemoryFree(b->length);  b->length  = NULL;
        taosMemoryFree(b->is_null); b->is_null = NULL;
    }
    taosMemoryFree(ctx->varWriteOffsets); ctx->varWriteOffsets = NULL;
    taosMemoryFree(ctx->varBufCapacity);  ctx->varBufCapacity  = NULL;
}

//
// Allocate (or reuse) per-column accumulation buffers able to hold
// at least `numRows` rows for `numCols` columns described by `fieldInfos`.
// Reuses existing buffers if capacity is sufficient.
//
static int stmt2AllocColBuffers(Stmt2RestoreCtx *ctx, int64_t numRows,
                                int numCols, FieldInfo *fieldInfos) {
    /* Determine effective batch cap: user override via --data-batch, else STMT2 default */
    int64_t batchCap = (argDataBatch() > 0) ? (int64_t)argDataBatch() : (int64_t)STMT2_BATCH_DEFAULT;
    /* clamp requested row count so we never exceed the effective batch cap */
    int64_t targetCap = (numRows < batchCap) ? numRows : batchCap;
    /* Multi-table mode: the entire file must fit in one buffer without any
     * mid-file flush (the ? placeholder SQL cannot be used with tbnames=NULL).
     * Use numRows directly, capped at STMT2_BATCH_MAX to bound memory per slot. */
    if (ctx->multiTable) {
        int64_t mtCap = (numRows < (int64_t)STMT2_BATCH_MAX) ? numRows : (int64_t)STMT2_BATCH_MAX;
        if (targetCap < mtCap) targetCap = mtCap;
    }
    /* The buffer must be large enough to hold at least one complete raw TDengine
     * block (≤ STMT2_MIN_ROW_BUF_CAP rows).  When --data-batch is smaller than
     * that, stmt2AccBlock uses batchCap as the flush trigger instead; the buffer
     * still needs the larger capacity so that a whole block can be assembled
     * before the first flush opportunity. */
    {
        int64_t minCap = (numRows < STMT2_MIN_ROW_BUF_CAP) ? numRows : STMT2_MIN_ROW_BUF_CAP;
        if (targetCap < minCap) targetCap = minCap;
    }

    bool needAlloc = (ctx->colBinds == NULL ||
                      ctx->colBindsCap < numCols ||
                      ctx->rowBufCap  < targetCap);
    if (!needAlloc) {
        ctx->accRows   = 0;
        ctx->numFields = numCols;
        ctx->fieldInfos = fieldInfos;
        /* reset packed write offsets for all variable-type columns */
        if (ctx->varWriteOffsets)
            memset(ctx->varWriteOffsets, 0, ctx->colBindsCap * sizeof(int32_t));
        /* also reset per-column length arrays for var/decimal columns */
        for (int i = 0; i < numCols; i++) {
            TAOS_STMT2_BIND *bind = &ctx->colBinds[i];
            if (bind->length) memset(bind->length, 0, ctx->rowBufCap * sizeof(int32_t));
        }
        return TSDB_CODE_SUCCESS;
    }

    /* free old buffers */
    if (ctx->colBinds) {
        stmt2FreeColBuffers(ctx);
        taosMemoryFree(ctx->colBinds);
        ctx->colBinds = NULL;
    }

    int64_t cap = targetCap;  /* already clamped to effective batchCap */
    ctx->colBinds = (TAOS_STMT2_BIND *)taosMemoryCalloc(numCols, sizeof(TAOS_STMT2_BIND));
    if (!ctx->colBinds) return TSDB_CODE_BCK_MALLOC_FAILED;

    ctx->varWriteOffsets = (int32_t *)taosMemoryCalloc(numCols, sizeof(int32_t));
    if (!ctx->varWriteOffsets) {
        taosMemoryFree(ctx->colBinds); ctx->colBinds = NULL;
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }
    ctx->varBufCapacity = (int32_t *)taosMemoryCalloc(numCols, sizeof(int32_t));
    if (!ctx->varBufCapacity) {
        taosMemoryFree(ctx->varWriteOffsets); ctx->varWriteOffsets = NULL;
        taosMemoryFree(ctx->colBinds); ctx->colBinds = NULL;
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    for (int i = 0; i < numCols; i++) {
        /* When schema has changed, bind position i corresponds to the i-th
         * matched column, whose actual backup index is colMappings[i].backupIdx.
         * We MUST use that backup field's type/bytes for allocation, not the
         * sequential fieldInfos[i] (which may be a completely different column). */
        int backupColIdx = i;
        if (ctx->stbChange && ctx->stbChange->schemaChanged && ctx->stbChange->colMappings &&
            i < ctx->stbChange->matchColCount) {
            backupColIdx = ctx->stbChange->colMappings[i].backupIdx;
        }
        FieldInfo       *fi   = &fieldInfos[backupColIdx];
        TAOS_STMT2_BIND *bind = &ctx->colBinds[i];
        bind->buffer_type = fi->type;
        if (IS_VAR_DATA_TYPE(fi->type)) {
            /* Cap initial allocation to avoid huge allocs for BLOB/MEDIUMBLOB.
             * Use dynamic realloc in stmt2AccBlock if data exceeds initial cap. */
            int64_t stride = (int64_t)fi->bytes;
            int64_t initialBufSize = cap * stride;
            if (initialBufSize > (int64_t)32 * 1024 * 1024) initialBufSize = (int64_t)32 * 1024 * 1024;
            if (initialBufSize < 4096) initialBufSize = 4096;
            bind->buffer   = taosMemoryCalloc(1, (size_t)initialBufSize);
            bind->length   = (int32_t *)taosMemoryCalloc(cap, sizeof(int32_t));
            ctx->varBufCapacity[i] = (int32_t)initialBufSize;
        } else if (IS_DECIMAL_TYPE(fi->type)) {
            /* DECIMAL is bound to STMT2 as a packed decimal string (same packed
             * layout as var-data).  precision/scale decoded from encoded fi->bytes. */
            uint8_t precision = fieldGetPrecision(fi);
            int32_t maxStrLen = (precision > 0 ? (int32_t)precision : 38) + 10;
            int32_t totalBufSize = (int32_t)cap * maxStrLen;
            bind->buffer   = taosMemoryCalloc(1, totalBufSize);
            bind->length   = (int32_t *)taosMemoryCalloc(cap, sizeof(int32_t));
            ctx->varBufCapacity[i] = totalBufSize;
        } else {
            int32_t typeBytes = tDataTypes[fi->type].bytes;
            bind->buffer   = taosMemoryCalloc(cap, typeBytes);
            bind->length   = NULL;
            ctx->varBufCapacity[i] = 0;
        }
        bind->is_null = (char *)taosMemoryCalloc(cap, 1);
        if (!bind->buffer || !bind->is_null ||
            ((IS_VAR_DATA_TYPE(fi->type) || IS_DECIMAL_TYPE(fi->type)) && !bind->length)) {
            stmt2FreeColBuffers(ctx);
            taosMemoryFree(ctx->colBinds); ctx->colBinds = NULL;
            return TSDB_CODE_BCK_MALLOC_FAILED;
        }
    }
    ctx->colBindsCap = numCols;
    ctx->rowBufCap   = cap;
    ctx->numFields   = numCols;
    ctx->fieldInfos  = fieldInfos;
    ctx->accRows     = 0;
    return TSDB_CODE_SUCCESS;
}

//
// Prepare STMT2 once per thread:
//   INSERT INTO ? VALUES(?,?,?...)
// With singleStbInsert + singleTableBindOnce enabled for maximum server-side
// throughput (no per-call schema re-negotiation).
//
static int stmt2PrepareOnce(Stmt2RestoreCtx *ctx, int numCols, FieldInfo *fieldInfos) {
    /* build SQL — use fully qualified table name so WebSocket mode can resolve
     * the schema without needing a prior taos_select_db or a ? placeholder.
     * Native mode also handles this correctly (local metadata lookup). */
    const char *partCols  = "";
    int         bindCount = numCols;
    if (ctx->stbChange && ctx->stbChange->schemaChanged && ctx->stbChange->partColsStr) {
        partCols   = ctx->stbChange->partColsStr;
        bindCount  = ctx->stbChange->matchColCount;
        logInfo("stmt2 partial column write: %s (bind %d of %d cols)", partCols, bindCount, numCols);
    }

    char *sql = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
    if (!sql) return TSDB_CODE_BCK_MALLOC_FAILED;
    char *p = sql;
    /* Use db.tablename directly — avoids WebSocket STMT2 prepare failure
     * ("Table does not exist") that occurs with INSERT INTO ? over WebSocket
     * because the Rust WebSocket library validates the table at prepare time. */
    p += snprintf(p, TSDB_MAX_SQL_LEN, "INSERT INTO `%s`.`%s` %s VALUES(?",
                  ctx->dbName, ctx->tbName, partCols);
    for (int i = 1; i < bindCount; i++) p += sprintf(p, ",?");
    sprintf(p, ")");
    logDebug("stmt2 prepare: %s", sql);

    /* In WebSocket/DSN mode (multiTable=false) the same handle is always
     * singleTableBindOnce=true and can be reused across files — just call
     * taos_stmt2_prepare again with the new table-specific SQL instead of
     * closing and re-initialising the handle.
     * taosBenchmark's autoTblCreating path does the same: it calls
     * taos_stmt2_prepare once per child-table on the same handle.
     *
     * In native multi-table mode (multiTable=true) the caller already closed
     * the multi-table handle (different singleTableBindOnce option), so
     * ctx->stmt2 will be NULL here and we init a fresh single-table handle. */
    if (!ctx->stmt2) {
        ctx->stmt2 = initStmt2(ctx->conn, true);   /* singleTableBindOnce=true */
        if (!ctx->stmt2) {
            logError("initStmt2 failed for %s", ctx->dbName);
            taosMemoryFree(sql);
            return TSDB_CODE_BCK_STMT_FAILED;
        }
    }

    int ret = taos_stmt2_prepare(ctx->stmt2, sql, 0);
    taosMemoryFree(sql);
    if (ret != 0) {
        logError("taos_stmt2_prepare failed: %s", taos_stmt2_error(ctx->stmt2));
        taos_stmt2_close(ctx->stmt2); ctx->stmt2 = NULL;
        return ret;
    }

    ctx->prepared = true;
    (void)fieldInfos;   /* used only for bind buffer alloc, not here */
    return TSDB_CODE_SUCCESS;
}

/* forward declaration: stmt2ResetOnError calls stmt2FreeSlot (defined below) */
static void stmt2FreeSlot(Stmt2TableSlot *slot);

//
// Close and re-initialise STMT2 after a failure.
// Sets prepared=false so the next file triggers stmt2PrepareOnce().
//
static bool stmt2ResetOnError(Stmt2RestoreCtx *ctx) {
    if (ctx->stmt2) { taos_stmt2_close(ctx->stmt2); ctx->stmt2 = NULL; }
    ctx->prepared  = false;
    ctx->accRows   = 0;
    /* discard pending multi-table slots — they may contain partial data from a
     * failed state; flushing them risks duplicate or corrupt rows */
    if (ctx->numPending > 0) {
        for (int i = 0; i < ctx->numPending; i++) stmt2FreeSlot(&ctx->pendingSlots[i]);
        ctx->numPending       = 0;
        ctx->totalPendingRows = 0;
    }
    return true;   /* caller will retry prepare on next file */
}

/* forward declaration: stmt2AccBlock calls stmt2FlushBatch (defined below) */
static int stmt2FlushBatch(Stmt2RestoreCtx *ctx);

//
// Accumulate one raw block into the per-column stride buffers.
// Called per data block by dataBlockCallbackV2.
//
static int stmt2AccBlock(Stmt2RestoreCtx *ctx, void *blockData, int32_t blockRows,
                         FieldInfo *fieldInfos, int numFields) {
    /* Flush the current batch before accumulating this block when:
     *   (a) the block would overflow the row buffer, OR
     *   (b) the user's --data-batch threshold has already been reached.
     * Case (b) is needed when --data-batch < STMT2_MIN_ROW_BUF_CAP: the buffer
     * can still hold more rows, but we honour the user's requested batch size.
     * In multi-table mode both triggers are suppressed: the buffer is sized to
     * hold all rows of a single file (numRows ≤ STMT2_BATCH_MAX), so overflow
     * cannot occur, and batchThresh flushing would call stmt2FlushBatch with
     * tbnames=NULL which is incompatible with the ? placeholder SQL. */
    int64_t batchThresh = (ctx->multiTable) ? ctx->rowBufCap
                        : (argDataBatch() > 0) ? (int64_t)argDataBatch() : ctx->rowBufCap;
    if (ctx->accRows + blockRows > ctx->rowBufCap ||
        (ctx->accRows > 0 && ctx->accRows >= batchThresh)) {
        int flushCode = stmt2FlushBatch(ctx);
        if (flushCode != TSDB_CODE_SUCCESS) return flushCode;
    }

    BlockReader reader;
    int code = initBlockReader(&reader, blockData);
    if (code != TSDB_CODE_SUCCESS) return code;

    int bindColCount = ctx->numFields; /* may differ from numFields if schema change */
    int bindIdx = 0;
    for (int c = 0; c < numFields; c++) {
        /* check schema-change skip via bindIdxMap */
        int bi = getPartialWriteBindIdx(
            (ctx->stbChange && ctx->stbChange->schemaChanged) ? ctx->stbChange : NULL,
            c);
        if (bi < 0) {
            void *sd = NULL; int32_t sl = 0;
            getColumnData(&reader, fieldInfos[c].type, &sd, &sl);
            continue;
        }
        if (bindIdx >= bindColCount) break;

        void *colData = NULL; int32_t colDataLen = 0;
        code = getColumnData(&reader, fieldInfos[c].type, &colData, &colDataLen);
        if (code != TSDB_CODE_SUCCESS) return code;

        TAOS_STMT2_BIND *bind = &ctx->colBinds[bindIdx];
        int64_t base = ctx->accRows;

        if (IS_VAR_DATA_TYPE(fieldInfos[c].type)) {
            int32_t *offsets    = (int32_t *)colData;
            char    *varDataBase = (char *)colData + blockRows * sizeof(int32_t);
            bool     isNchar    = (fieldInfos[c].type == TSDB_DATA_TYPE_NCHAR);
            for (int row = 0; row < blockRows; row++) {
                if (offsets[row] < 0) {
                    bind->is_null[base + row] = 1;
                    if (bind->length) bind->length[base + row] = 0;
                } else {
                    bind->is_null[base + row] = 0;
                    uint16_t varLen = *(uint16_t *)(varDataBase + offsets[row]);
                    char    *src    = varDataBase + offsets[row] + sizeof(uint16_t);
                    // Write packed: each row's data immediately follows the previous
                    int32_t writeOff = ctx->varWriteOffsets[bindIdx];
                    if (isNchar && varLen > 0) {
                        /* UTF-8 can be at most equal in bytes to UCS-4 size */
                        int32_t maxUtf8 = varLen;
                        /* ensure buffer has room */
                        if (writeOff + maxUtf8 > ctx->varBufCapacity[bindIdx]) {
                            int32_t newCap = (writeOff + maxUtf8) * 2;
                            if (newCap < ctx->varBufCapacity[bindIdx] * 2) newCap = ctx->varBufCapacity[bindIdx] * 2;
                            void *newBuf = taosMemoryRealloc(bind->buffer, newCap);
                            if (!newBuf) return TSDB_CODE_BCK_MALLOC_FAILED;
                            bind->buffer = newBuf;
                            ctx->varBufCapacity[bindIdx] = newCap;
                        }
                        int32_t utf8Len = taosUcs4ToMbs(
                            (TdUcs4 *)src, varLen,
                            (char *)bind->buffer + writeOff, NULL);
                        int32_t actualLen = utf8Len > 0 ? utf8Len : 0;
                        if (bind->length) bind->length[base + row] = actualLen;
                        ctx->varWriteOffsets[bindIdx] += actualLen;
                    } else {
                        int32_t stride = fieldInfos[c].bytes;
                        int32_t cp = (varLen < (uint16_t)stride) ? varLen : (uint16_t)stride;
                        /* ensure buffer has room (handles BLOB/MEDIUMBLOB large data) */
                        if (writeOff + cp > ctx->varBufCapacity[bindIdx]) {
                            int32_t newCap = (writeOff + cp) * 2;
                            if (newCap < ctx->varBufCapacity[bindIdx] * 2) newCap = ctx->varBufCapacity[bindIdx] * 2;
                            void *newBuf = taosMemoryRealloc(bind->buffer, newCap);
                            if (!newBuf) return TSDB_CODE_BCK_MALLOC_FAILED;
                            bind->buffer = newBuf;
                            ctx->varBufCapacity[bindIdx] = newCap;
                        }
                        memcpy((char *)bind->buffer + writeOff, src, cp);
                        if (bind->length) bind->length[base + row] = cp;
                        ctx->varWriteOffsets[bindIdx] += cp;
                    }
                }
            }
        } else if (IS_DECIMAL_TYPE(fieldInfos[c].type)) {
            /* DECIMAL: raw block has bitmap + fixed bytes (8 or 16) per row.
             * STMT2 for DECIMAL expects packed decimal string representation. */
            int32_t actualBytes = fieldGetRawBytes(&fieldInfos[c]);
            uint8_t precision   = fieldGetPrecision(&fieldInfos[c]);
            uint8_t scale       = fieldGetScale(&fieldInfos[c]);
            int32_t bitmapLen   = (blockRows + 7) / 8;
            char   *bitmap      = (char *)colData;
            char   *fixData     = (char *)colData + bitmapLen;
            for (int row = 0; row < blockRows; row++) {
                bind->is_null[base + row] =
                    (bitmap[row >> 3] & (1u << (7 - (row & 7)))) ? 1 : 0;
                if (!bind->is_null[base + row]) {
                    void   *rawDec   = fixData + row * actualBytes;
                    char    str[64]  = "";
                    decimalToStr(rawDec, fieldInfos[c].type, precision, scale, str, sizeof(str));
                    int32_t slen     = (int32_t)strlen(str);
                    int32_t writeOff = ctx->varWriteOffsets[bindIdx];
                    /* ensure buffer has room */
                    if (writeOff + slen > ctx->varBufCapacity[bindIdx]) {
                        int32_t newCap = (writeOff + slen) * 2;
                        if (newCap < ctx->varBufCapacity[bindIdx] * 2) newCap = ctx->varBufCapacity[bindIdx] * 2;
                        void *newBuf = taosMemoryRealloc(bind->buffer, newCap);
                        if (!newBuf) return TSDB_CODE_BCK_MALLOC_FAILED;
                        bind->buffer = newBuf;
                        ctx->varBufCapacity[bindIdx] = newCap;
                    }
                    memcpy((char *)bind->buffer + writeOff, str, slen);
                    bind->length[base + row] = slen;
                    ctx->varWriteOffsets[bindIdx] += slen;
                } else {
                    bind->length[base + row] = 0;
                }
            }
        } else {
            int32_t typeBytes = tDataTypes[fieldInfos[c].type].bytes;
            int32_t bitmapLen = (blockRows + 7) / 8;
            char   *bitmap    = (char *)colData;
            char   *fixData   = (char *)colData + bitmapLen;
            memcpy((char *)bind->buffer + base * typeBytes, fixData, (size_t)blockRows * typeBytes);
            for (int row = 0; row < blockRows; row++) {
                // TDengine bitmap: bit7=row0 (MSB-first within each byte)
                bind->is_null[base + row] =
                    (bitmap[row >> 3] & (1u << (7 - (row & 7)))) ? 1 : 0;
            }
        }
        bindIdx++;
    }
    ctx->accRows += blockRows;
    return TSDB_CODE_SUCCESS;
}

typedef struct { Stmt2RestoreCtx *ctx; FieldInfo *fis; int nf; } S2CallbackData;

static int dataBlockCallbackV2(void *userData, FieldInfo *fieldInfos,
                               int numFields, void *blockData,
                               int32_t blockLen, int32_t blockRows) {
    S2CallbackData *d = (S2CallbackData *)userData;
    (void)blockLen;
    if (g_interrupted) return TSDB_CODE_BCK_USER_CANCEL;
    if (blockRows == 0) return TSDB_CODE_SUCCESS;
    return stmt2AccBlock(d->ctx, blockData, blockRows, d->fis, d->nf);
}

//
// Bind the currently accumulated rows and execute, then reset accRows to 0.
// Called both mid-accumulation (overflow guard) and at end-of-file.
//
static int stmt2FlushBatch(Stmt2RestoreCtx *ctx) {
    if (ctx->accRows == 0) return TSDB_CODE_SUCCESS;

    /* set row count on each column */
    for (int i = 0; i < ctx->numFields; i++)
        ctx->colBinds[i].num = (int)ctx->accRows;

    /* Table name is embedded in the prepare SQL (INSERT INTO db.tbl VALUES(?))
     * so tbnames must be NULL — passing it would override the SQL table name. */
    TAOS_STMT2_BINDV bindv;
    memset(&bindv, 0, sizeof(bindv));
    bindv.count     = 1;
    bindv.tbnames   = NULL;             /* name already in prepared SQL */
    bindv.tags      = NULL;             /* child table already created */
    bindv.bind_cols = &ctx->colBinds;   /* [0] = column array for 1st table */

    int code = taos_stmt2_bind_param(ctx->stmt2, &bindv, -1);
    if (code != 0) {
        logError("taos_stmt2_bind_param failed (%s.%s): %s",
                 ctx->dbName, ctx->tbName, taos_stmt2_error(ctx->stmt2));
        return code;
    }

    int affectedRows = 0;
    code = taos_stmt2_exec(ctx->stmt2, &affectedRows);
    if (code != 0) {
        logError("taos_stmt2_exec failed (%s.%s): %s",
                 ctx->dbName, ctx->tbName, taos_stmt2_error(ctx->stmt2));
        return code;
    }

    ctx->totalRows += affectedRows;
    ctx->accRows    = 0;
    /* reset packed write offsets for variable-type columns */
    if (ctx->varWriteOffsets)
        memset(ctx->varWriteOffsets, 0, ctx->colBindsCap * sizeof(int32_t));
    return TSDB_CODE_SUCCESS;
}

//
// Execute the accumulated rows for the current file using STMT2.
// Delegates to stmt2FlushBatch which also handles mid-file overflow flushes.
//
static int stmt2ExecFile(Stmt2RestoreCtx *ctx) {
    return stmt2FlushBatch(ctx);
}

// =====================================================================
//  STMT2 multi-table batch helpers  (native mode, STB CTBs only)
// =====================================================================

//
// Free all buffers in one pending slot (does not zero the slot itself for
// compatibility with memset-based cleanup in stmt2FlushMultiTableSlots).
//
static void stmt2FreeSlot(Stmt2TableSlot *slot) {
    if (!slot->colBinds) return;
    for (int c = 0; c < slot->colBindsCap; c++) {
        TAOS_STMT2_BIND *b = &slot->colBinds[c];
        taosMemoryFree(b->buffer);  b->buffer  = NULL;
        taosMemoryFree(b->length);  b->length  = NULL;
        taosMemoryFree(b->is_null); b->is_null = NULL;
    }
    taosMemoryFree(slot->varWriteOffsets); slot->varWriteOffsets = NULL;
    taosMemoryFree(slot->varBufCapacity);  slot->varBufCapacity  = NULL;
    taosMemoryFree(slot->colBinds);        slot->colBinds        = NULL;
    memset(slot, 0, sizeof(*slot));
}

//
// Prepare STMT2 once for the whole thread using INSERT INTO ? VALUES(?,...)
// with singleStbInsert=true, singleTableBindOnce=false (multi-table mode).
// This is used for native connections only; WebSocket/DSN falls back to the
// per-file approach (INSERT INTO db.tbl VALUES(?)).
//
static int stmt2PrepareOnceMulti(Stmt2RestoreCtx *ctx, int numCols,
                                  FieldInfo *fieldInfos) {
    const char *partCols  = "";
    int         bindCount = numCols;
    if (ctx->stbChange && ctx->stbChange->schemaChanged && ctx->stbChange->partColsStr) {
        partCols   = ctx->stbChange->partColsStr;
        bindCount  = ctx->stbChange->matchColCount;
        logInfo("stmt2 multi-table partial column write: %s (bind %d of %d cols)",
                partCols, bindCount, numCols);
    }

    char *sql = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
    if (!sql) return TSDB_CODE_BCK_MALLOC_FAILED;
    char *p = sql;
    p += snprintf(p, TSDB_MAX_SQL_LEN, "INSERT INTO ? %s VALUES(?", partCols);
    for (int i = 1; i < bindCount; i++) p += sprintf(p, ",?");
    sprintf(p, ")");
    logDebug("stmt2 multi-table prepare: %s", sql);

    if (ctx->stmt2) { taos_stmt2_close(ctx->stmt2); ctx->stmt2 = NULL; }

    /* singleTableBindOnce=false enables multi-table binding in one TAOS_STMT2_BINDV */
    TAOS_STMT2_OPTION op2;
    memset(&op2, 0, sizeof(op2));
    op2.singleStbInsert     = true;
    op2.singleTableBindOnce = false;
    ctx->stmt2 = taos_stmt2_init(ctx->conn, &op2);
    if (!ctx->stmt2) {
        logError("stmt2 multi-table init failed for %s", ctx->dbName);
        taosMemoryFree(sql);
        return TSDB_CODE_BCK_STMT_FAILED;
    }

    int ret = taos_stmt2_prepare(ctx->stmt2, sql, 0);
    taosMemoryFree(sql);
    if (ret != 0) {
        logError("stmt2 multi-table prepare failed: %s", taos_stmt2_error(ctx->stmt2));
        taos_stmt2_close(ctx->stmt2); ctx->stmt2 = NULL;
        return ret;
    }
    ctx->prepared = true;
    (void)fieldInfos;
    return TSDB_CODE_SUCCESS;
}

//
// Seal: transfer ctx->colBinds ownership into a new pending slot, then reset
// ctx so that the next stmt2AllocColBuffers call allocates fresh buffers.
//
static void stmt2SealSlot(Stmt2RestoreCtx *ctx, const char *fqn) {
    Stmt2TableSlot *slot = &ctx->pendingSlots[ctx->numPending];
    strncpy(slot->fqn, fqn, sizeof(slot->fqn) - 1);
    slot->fqn[sizeof(slot->fqn) - 1] = '\0';

    /* Transfer buffer ownership: ctx → slot */
    slot->colBinds        = ctx->colBinds;
    slot->colBindsCap     = ctx->colBindsCap;
    slot->numCols         = ctx->numFields;
    slot->numRows         = (int32_t)ctx->accRows;
    slot->varWriteOffsets = ctx->varWriteOffsets;
    slot->varBufCapacity  = ctx->varBufCapacity;

    /* Detach from ctx so the next alloc creates fresh per-slot buffers */
    ctx->colBinds        = NULL;
    ctx->colBindsCap     = 0;
    ctx->rowBufCap       = 0;
    ctx->accRows         = 0;
    ctx->varWriteOffsets = NULL;
    ctx->varBufCapacity  = NULL;

    ctx->totalPendingRows += slot->numRows;
    ctx->numPending++;
}

//
// Flush all pending slots as a single multi-table TAOS_STMT2_BINDV call
// (one taos_stmt2_bind_param + one taos_stmt2_exec for N child tables).
// Frees all slot buffers after the call regardless of success/failure.
//
static int stmt2FlushMultiTableSlots(Stmt2RestoreCtx *ctx) {
    if (ctx->numPending == 0) return TSDB_CODE_SUCCESS;

    int N = ctx->numPending;
    char           **tbnames  = (char **)taosMemoryCalloc(N, sizeof(char *));
    TAOS_STMT2_BIND **bindcols = (TAOS_STMT2_BIND **)taosMemoryCalloc(N, sizeof(TAOS_STMT2_BIND *));
    if (!tbnames || !bindcols) {
        taosMemoryFree(tbnames);
        taosMemoryFree(bindcols);
        for (int i = 0; i < N; i++) stmt2FreeSlot(&ctx->pendingSlots[i]);
        ctx->numPending = 0; ctx->totalPendingRows = 0;
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    for (int i = 0; i < N; i++) {
        Stmt2TableSlot *slot = &ctx->pendingSlots[i];
        /* set per-row-count on each column bind of this slot */
        for (int c = 0; c < slot->numCols; c++)
            slot->colBinds[c].num = slot->numRows;
        tbnames[i]  = slot->fqn;
        bindcols[i] = slot->colBinds;
    }

    TAOS_STMT2_BINDV bindv;
    memset(&bindv, 0, sizeof(bindv));
    bindv.count     = N;
    bindv.tbnames   = tbnames;
    bindv.tags      = NULL;
    bindv.bind_cols = bindcols;

    int code = taos_stmt2_bind_param(ctx->stmt2, &bindv, -1);
    taosMemoryFree(tbnames);
    taosMemoryFree(bindcols);

    if (code != 0) {
        logError("stmt2 multi-table bind_param failed (%s, %d tables): %s",
                 ctx->dbName, N, taos_stmt2_error(ctx->stmt2));
        goto cleanup;
    }

    {
        int affectedRows = 0;
        code = taos_stmt2_exec(ctx->stmt2, &affectedRows);
        if (code != 0) {
            logError("stmt2 multi-table exec failed (%s, %d tables): %s",
                     ctx->dbName, N, taos_stmt2_error(ctx->stmt2));
        } else {
            ctx->totalRows += affectedRows;
            logDebug("stmt2 multi-table flush: %d tables, %d rows", N, affectedRows);
        }
    }

cleanup:
    for (int i = 0; i < N; i++) stmt2FreeSlot(&ctx->pendingSlots[i]);
    ctx->numPending       = 0;
    ctx->totalPendingRows = 0;
    return code;
}

//
// Restore one .dat file using TAOS_STMT2.
//
// Two paths depending on ctx->multiTable (set once per thread by restoreDataThread):
//
//   MULTI-TABLE (native, STB CTBs, numRows <= STMT2_BATCH_MAX):
//     Prepare INSERT INTO ? VALUES(?,...)  once per thread.
//     Read file into ctx->colBinds, seal into a pending slot.
//     Flush TAOS_STMT2_BINDV{count=N, tbnames[], bind_cols[]} when
//     numPending == STMT2_MULTI_TABLE_PENDING or totalPendingRows >= batchCap.
//     Final flush happens in restoreDataThread after the file loop.
//
//   SINGLE-TABLE (WebSocket/DSN, NTB, or large file > STMT2_BATCH_MAX rows):
//     Original per-file: close/init/prepare with embedded table name, exec immediately.
//
static int restoreOneDataFileV2(Stmt2RestoreCtx *ctx, const char *filePath) {
    /* extract table name */
    const char *base = strrchr(filePath, '/');
    if (base) base++; else base = filePath;
    char tbName[TSDB_TABLE_NAME_LEN] = {0};
    const char *dot = strrchr(base, '.');
    if (dot && (dot - base) < (int)sizeof(tbName))
        memcpy(tbName, base, dot - base);
    else
        snprintf(tbName, sizeof(tbName), "%s", base);

    bool isNtb = (strstr(filePath, "/_ntb_data") != NULL);

    int code = TSDB_CODE_SUCCESS;
    TaosFile *f = openTaosFileForRead(filePath, &code);
    if (!f) { logError("open failed(%d): %s", code, filePath); return code; }
    if (f->header.numRows == 0) { closeTaosFileRead(f); return TSDB_CODE_SUCCESS; }

    strncpy(ctx->tbName, tbName, TSDB_TABLE_NAME_LEN - 1);
    int       numCols = f->header.numFields;
    FieldInfo *fis    = (FieldInfo *)f->header.schema;
    int64_t   numRows = f->header.numRows;

    /* Per-table NTB schema change detection */
    StbChange *localStbChange = NULL;
    if (ctx->stbChange == NULL && isNtb) {
        localStbChange = buildNtbSchemaChange(ctx->conn, ctx->dbName, tbName,
                                              fis, f->header.numFields);
        if (localStbChange) ctx->stbChange = localStbChange;
    }

    if (ctx->stbChange && ctx->stbChange->schemaChanged)
        numCols = ctx->stbChange->matchColCount;

    /* Choose path: multi-table only for native, STB CTBs, and files that fit
     * entirely within STMT2_BATCH_MAX rows (avoids mid-file flush complications). */
    if (ctx->multiTable && !isNtb && numRows <= (int64_t)STMT2_BATCH_MAX) {
        /* ---- MULTI-TABLE PATH ---- */

        /* One-time prepare: INSERT INTO ? VALUES(?,...)  reused for all files */
        if (!ctx->prepared) {
            /* Select the target database so that the multi-table tbnames look-up
             * can resolve plain table names (no db prefix required). */
            int selRet = taos_select_db(ctx->conn, ctx->dbName);
            if (selRet != 0)
                logWarn("stmt2 multi-table: taos_select_db(%s) failed (code=%d), continuing",
                        ctx->dbName, selRet);
            code = stmt2PrepareOnceMulti(ctx, numCols, fis);
            if (code != TSDB_CODE_SUCCESS) { closeTaosFileRead(f); goto done; }
        }

        /* Allocate per-column buffers for this file (buffer sized to numRows,
         * bypassing the batchCap clamp because multiTable is true). */
        code = stmt2AllocColBuffers(ctx, numRows, numCols, fis);
        if (code != TSDB_CODE_SUCCESS) { closeTaosFileRead(f); goto done; }

        /* Stream blocks into ctx->colBinds */
        S2CallbackData cbd = { ctx, fis, f->header.numFields };
        code = readTaosFileBlocks(f, dataBlockCallbackV2, &cbd);
        closeTaosFileRead(f);
        if (code != TSDB_CODE_SUCCESS) {
            logError("read blocks failed(%d): %s", code, filePath);
            goto done;
        }

        if (ctx->accRows == 0) goto done;  /* file became empty after filtering */

        /* Seal: transfer buffer ownership from ctx into a new pending slot */
        char fqn[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + 8];
        /* Use plain table name (no db prefix): taos_select_db has already set
         * the default database for this connection so tbnames resolution works. */
        snprintf(fqn, sizeof(fqn), "%s", tbName);
        stmt2SealSlot(ctx, fqn);

        /* Flush if batch thresholds are reached */
        int64_t batchCap = (argDataBatch() > 0) ? (int64_t)argDataBatch() : (int64_t)STMT2_BATCH_DEFAULT;
        if (ctx->numPending >= STMT2_MULTI_TABLE_PENDING ||
            ctx->totalPendingRows >= batchCap) {
            code = stmt2FlushMultiTableSlots(ctx);
            if (code != TSDB_CODE_SUCCESS)
                logError("stmt2 multi-table flush failed (%s): %d", ctx->dbName, code);
        }

        logDebug("stmt2 multi-table sealed %s.%s rows: %" PRId64 " pending_slots: %d",
                 ctx->dbName, tbName, ctx->totalRows, ctx->numPending);
    } else {
        /* ---- SINGLE-TABLE PATH (WebSocket / NTB / large file) ---- */

        /* In native multi-table mode (multiTable=true) the current stmt2 handle
         * may be a multi-table one (singleTableBindOnce=false).  Close it so
         * stmt2PrepareOnce will init a new single-table handle.
         * In WebSocket/DSN mode (multiTable=false) the handle is always
         * single-table — reuse it by just calling prepare again, no close+init. */
        if (ctx->multiTable && ctx->stmt2) {
            taos_stmt2_close(ctx->stmt2);
            ctx->stmt2 = NULL;
        }
        code = stmt2PrepareOnce(ctx, numCols, fis);
        if (code != TSDB_CODE_SUCCESS) { closeTaosFileRead(f); goto done; }

        code = stmt2AllocColBuffers(ctx, numRows, numCols, fis);
        if (code != TSDB_CODE_SUCCESS) { closeTaosFileRead(f); goto done; }

        S2CallbackData cbd = { ctx, fis, f->header.numFields };
        code = readTaosFileBlocks(f, dataBlockCallbackV2, &cbd);
        closeTaosFileRead(f);
        if (code != TSDB_CODE_SUCCESS) {
            logError("read blocks failed(%d): %s", code, filePath);
            goto done;
        }

        code = stmt2ExecFile(ctx);
        if (code == TSDB_CODE_SUCCESS)
            logDebug("stmt2 restore done: %s.%s rows: %" PRId64, ctx->dbName, tbName, ctx->totalRows);
        else
            logError("stmt2 exec failed: %s.%s", ctx->dbName, tbName);

        /* Reset prepared flag so that a subsequent multi-table file (native mode)
         * re-establishes the multi-table handle (different singleTableBindOnce). */
        if (ctx->multiTable) ctx->prepared = false;
    }

done:
    if (localStbChange) {
        ctx->stbChange = NULL;
        freeStbChange(localStbChange);
    }
    return code;
}


//
// Restore a single .par (Parquet) file using STMT
//
// The schema is read from the file's embedded metadata; no external schema
// info is required.  Schema-change partial-column mapping is NOT applied
// for Parquet files (the column set is taken verbatim from the file).
//
static int restoreOneParquetFile(TAOS_STMT **stmtPtr, TAOS *conn, const char *dbName,
                                  const char *filePath,
                                  StbChange *stbChange,
                                  int64_t   *rowsOut) {
    (void)stbChange;  /* reserved for future schema-change support */
    int code = TSDB_CODE_SUCCESS;

    // Extract table name:  .../{tbName}.par
    const char *baseName = strrchr(filePath, '/');
    if (baseName) baseName++; else baseName = filePath;

    char tbName[TSDB_TABLE_NAME_LEN] = {0};
    const char *dot = strrchr(baseName, '.');
    if (dot && (dot - baseName) < TSDB_TABLE_NAME_LEN) {
        memcpy(tbName, baseName, dot - baseName);
        tbName[dot - baseName] = '\0';
    } else {
        snprintf(tbName, sizeof(tbName), "%s", baseName);
    }

    logDebug("restore parquet file: %s -> table: %s.%s", filePath, dbName, tbName);

    // Open reader to discover schema (stored in file metadata)
    ParquetReader *pr = parquetReaderOpen(filePath, &code);
    if (pr == NULL) {
        logError("open parquet file failed(%d): %s", code, filePath);
        return code;
    }

    TAOS_FIELD *fields    = NULL;
    int         numFields = parquetReaderGetFields(pr, &fields);
    (void)fields;  /* we only need the count; actual field data stays in the reader */
    if (numFields <= 0) {
        logError("parquet file has no fields: %s", filePath);
        parquetReaderClose(pr);
        return TSDB_CODE_BCK_NO_FIELDS;
    }

    // Init STMT once per thread; re-prepare for each new table (avoids repeated init+close).
    if (!*stmtPtr) {
        *stmtPtr = initStmt(conn, true);
        if (!*stmtPtr) {
            logError("initStmt failed for %s.%s", dbName, tbName);
            parquetReaderClose(pr);
            return TSDB_CODE_BCK_STMT_FAILED;
        }
    }
    TAOS_STMT *stmt = *stmtPtr;

    char *stmtBuf = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
    if (stmtBuf == NULL) {
        parquetReaderClose(pr);
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }
    char *p = stmtBuf;
    p += snprintf(p, TSDB_MAX_SQL_LEN,
                  "INSERT INTO `%s`.`%s` VALUES(?",
                  argRenameDb(dbName), tbName);
    for (int i = 1; i < numFields; i++)
        p += sprintf(p, ",?");
    sprintf(p, ")");

    logDebug("parquet stmt prepare: %s", stmtBuf);
    int ret = taos_stmt_prepare(stmt, stmtBuf, 0);
    taosMemoryFree(stmtBuf);
    if (ret != 0) {
        logError("taos_stmt_prepare failed: %s", taos_stmt_errstr(stmt));
        // Invalidate handle so next file gets a fresh one
        taos_stmt_close(stmt);
        *stmtPtr = NULL;
        parquetReaderClose(pr);
        return ret;
    }

    // Stream all row-groups through the STMT via fileParquetToStmt
    // (closes the internal reader; the schema was already confirmed above)
    parquetReaderClose(pr);

    int64_t rows = 0;
    code = fileParquetToStmt(stmt, filePath, &rows);
    if (code != TSDB_CODE_SUCCESS) {
        logError("restore parquet failed(%d): %s -> %s.%s",
                 code, filePath, dbName, tbName);
    } else {
        logDebug("restore parquet done: %s.%s rows: %" PRId64, dbName, tbName, rows);
        if (rowsOut) *rowsOut = rows;
    }

    // stmt is NOT closed here; the caller owns it and closes after all files are done
    return code;
}

//
// Restore a single .par (Parquet) file using STMT2.
//
// Mirrors restoreOneParquetFile() but uses the TAOS_STMT2 API so the parquet
// restore path honours the user's -v option just like binary files do.
//
static int restoreOneParquetFileV2(TAOS_STMT2 **stmt2Ptr, TAOS *conn, const char *dbName,
                                    const char *filePath,
                                    StbChange *stbChange,
                                    int64_t   *rowsOut) {
    (void)stbChange;  /* reserved for future schema-change support */
    int code = TSDB_CODE_SUCCESS;

    const char *baseName = strrchr(filePath, '/');
    if (baseName) baseName++; else baseName = filePath;

    char tbName[TSDB_TABLE_NAME_LEN] = {0};
    const char *dot = strrchr(baseName, '.');
    if (dot && (dot - baseName) < TSDB_TABLE_NAME_LEN) {
        memcpy(tbName, baseName, dot - baseName);
        tbName[dot - baseName] = '\0';
    } else {
        snprintf(tbName, sizeof(tbName), "%s", baseName);
    }

    logDebug("restore parquet file (STMT2): %s -> table: %s.%s", filePath, dbName, tbName);

    /* Open file briefly just to determine the column count for the SQL */
    int numFields = 0;
    {
        int            err = TSDB_CODE_FAILED;
        ParquetReader *pr  = parquetReaderOpen(filePath, &err);
        if (!pr) {
            logError("open parquet file failed(%d): %s", err, filePath);
            return err;
        }
        TAOS_FIELD *fields = NULL;
        numFields = parquetReaderGetFields(pr, &fields);
        parquetReaderClose(pr);
        if (numFields <= 0) {
            logError("parquet file has no fields: %s", filePath);
            return TSDB_CODE_BCK_NO_FIELDS;
        }
    }

    /* Build INSERT SQL with db.table fully qualified (required for WebSocket) */
    char *sql = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
    if (!sql) return TSDB_CODE_BCK_MALLOC_FAILED;
    char *p = sql;
    p += snprintf(p, TSDB_MAX_SQL_LEN, "INSERT INTO `%s`.`%s` VALUES(?",
                  argRenameDb(dbName), tbName);
    for (int i = 1; i < numFields; i++) p += sprintf(p, ",?");
    sprintf(p, ")");
    logDebug("parquet stmt2 prepare: %s", sql);

    /* Init STMT2 once per thread; re-prepare for each new table (avoids repeated init+close) */
    if (!*stmt2Ptr) {
        *stmt2Ptr = initStmt2(conn, true);
        if (!*stmt2Ptr) {
            taosMemoryFree(sql);
            return TSDB_CODE_BCK_STMT_FAILED;
        }
    }
    TAOS_STMT2 *stmt2 = *stmt2Ptr;

    int ret = taos_stmt2_prepare(stmt2, sql, 0);
    taosMemoryFree(sql);
    if (ret != 0) {
        logError("taos_stmt2_prepare failed for parquet: %s", taos_stmt2_error(stmt2));
        // Invalidate handle so next file gets a fresh one
        taos_stmt2_close(stmt2);
        *stmt2Ptr = NULL;
        return ret;
    }

    int64_t rows = 0;
    code = fileParquetToStmt2(stmt2, filePath, &rows);
    if (code != TSDB_CODE_SUCCESS) {
        logError("restore parquet (STMT2) failed(%d): %s -> %s.%s",
                 code, filePath, dbName, tbName);
    } else {
        logDebug("restore parquet (STMT2) done: %s.%s rows: %" PRId64, dbName, tbName, rows);
        if (rowsOut) *rowsOut = rows;
    }

    // stmt2 is NOT closed here; the caller owns it and closes after all files are done
    return code;
}


//
// Restore a single data .dat file using a pre-prepared (reusable) per-thread STMT.
// On the first call ctx->prepared == false and stmtPrepareInsert() is used.
// On subsequent calls taos_stmt_set_tbname() switches the target table without
// a full re-prepare round-trip.
//
static int restoreOneDataFile(StmtRestoreCtx *ctx,
                               const char     *filePath) {
    int code = TSDB_CODE_SUCCESS;

    // Extract table name from file path: .../{tbName}.dat
    const char *baseName = strrchr(filePath, '/');
    if (baseName) baseName++; else baseName = filePath;

    char tbName[TSDB_TABLE_NAME_LEN] = {0};
    const char *dot = strrchr(baseName, '.');
    if (dot && (dot - baseName) < TSDB_TABLE_NAME_LEN) {
        memcpy(tbName, baseName, dot - baseName);
        tbName[dot - baseName] = '\0';
    } else {
        snprintf(tbName, sizeof(tbName), "%s", baseName);
    }

    logDebug("restore data file: %s -> table: %s.%s", filePath, ctx->dbName, tbName);

    // Open .dat file
    TaosFile *taosFile = openTaosFileForRead(filePath, &code);
    if (taosFile == NULL) {
        logError("open data file failed(%d): %s", code, filePath);
        return code;
    }

    if (taosFile->header.numRows == 0) {
        logDebug("data file has no rows: %s", filePath);
        closeTaosFileRead(taosFile);
        return TSDB_CODE_SUCCESS;
    }

    ctx->numFields  = taosFile->header.numFields;
    ctx->fieldInfos = (FieldInfo *)taosFile->header.schema;
    /* Do NOT reset ctx->totalRows here: it is a cumulative counter used
     * by the thread-loop stats (fileRows = bCtx.totalRows - rowsBefore).  */
    ctx->batchRows  = 0;
    int64_t fileRowsStart = ctx->totalRows;  /* for per-file log below */

    if (!ctx->prepared) {
        // First file: full prepare with concrete table name
        strncpy(ctx->tbName, tbName, TSDB_TABLE_NAME_LEN - 1);
        ctx->tbName[TSDB_TABLE_NAME_LEN - 1] = '\0';
        code = stmtPrepareInsert(ctx);
        if (code != TSDB_CODE_SUCCESS) {
            closeTaosFileRead(taosFile);
            return code;
        }
    } else {
        // Subsequent files: switch target table without re-preparing
        code = stmtSwitchTable(ctx, tbName);
        if (code != TSDB_CODE_SUCCESS) {
            closeTaosFileRead(taosFile);
            return code;
        }
    }

    // Read blocks and write via STMT
    code = readTaosFileBlocks(taosFile, dataBlockCallback, ctx);
    if (code != TSDB_CODE_SUCCESS) {
        logError("restore data blocks failed(%d): %s -> %s.%s",
                 code, filePath, ctx->dbName, tbName);
    } else {
        // Rows are flushed by the cross-file pendingRows threshold in dataBlockCallback.
        // The final flush for all remaining rows is done in restoreDataThread after the
        // file loop, so we intentionally skip per-file taos_stmt_execute here.
        logDebug("restore data file done: %s.%s rows: %" PRId64,
                 ctx->dbName, tbName, ctx->totalRows - fileRowsStart);
    }

    closeTaosFileRead(taosFile);
    return code;
}


//
// -------------------------------------- THREAD -----------------------------------------
//

//
// Restore data thread function: processes assigned data files.
//
// STMT2 optimization (native mode):
//   Multi-table batch mode batches up to STMT2_MULTI_TABLE_PENDING CTBs from the
//   same STB into a single TAOS_STMT2_BINDV call, reducing server RPCs by up to
//   STMT2_MULTI_TABLE_PENDING×.  Enabled automatically for native connections;
//   WebSocket/DSN falls back to the per-file prepare+exec path.
//
// STMT1 optimization:
//   Cross-file accumulation: rows are accumulated across files and executed only
//   when pendingRows >= threshold (or all files done), reducing taos_stmt_execute
//   RPCs from N (one per file) to ceil(totalRows / threshold).
//
// Checkpoint writes are buffered per-thread and flushed once at the
// end, removing the per-file mutex+open+write+close sequence.
//
static void* restoreDataThread(void *arg) {
    RestoreDataThread *thread = (RestoreDataThread *)arg;
    thread->code = TSDB_CODE_SUCCESS;

    const char *dbName = thread->dbInfo->dbName;
    const char *stbName4log = thread->stbInfo ? thread->stbInfo->stbName : "(ntb)";
    logInfo("data thread %d started for %s.%s (files: %d)",
            thread->index, dbName, stbName4log, thread->fileCnt);
    StmtVersion  stmtVer = argStmtVersion();

    /* ---- STMT2 path ---- */
    Stmt2RestoreCtx s2Ctx;
    memset(&s2Ctx, 0, sizeof(s2Ctx));
    s2Ctx.conn      = thread->conn;
    s2Ctx.dbName    = argRenameDb(dbName);
    s2Ctx.stbChange = thread->stbChange;

    /* Allocate pending-slots array for multi-table batching */
    s2Ctx.pendingSlots = (Stmt2TableSlot *)taosMemoryCalloc(
        STMT2_MULTI_TABLE_PENDING, sizeof(Stmt2TableSlot));
    if (!s2Ctx.pendingSlots) {
        logError("restore thread %d: alloc pendingSlots failed", thread->index);
        thread->code = TSDB_CODE_BCK_MALLOC_FAILED;
        return NULL;
    }

    /* Enable multi-table batching for native connections (not WebSocket/DSN).
     * WebSocket STMT2 prepare with INSERT INTO ? fails at the Rust driver level.
     * Set env TAOSBK_SINGLE_TABLE=1 to force single-table mode (benchmark only). */
    s2Ctx.multiTable = (argDriver() != CONN_MODE_WEBSOCKET && !argIsDsn() &&
                        getenv("TAOSBK_SINGLE_TABLE") == NULL);
    logDebug("restore thread %d: STMT2 multiTable=%d", thread->index, s2Ctx.multiTable);

    /* ---- STMT1 (legacy) path ---- */
    StmtRestoreCtx bCtx;
    memset(&bCtx, 0, sizeof(bCtx));
    if (stmtVer == STMT_VERSION_1) {
        TAOS_STMT *s1 = initStmt(thread->conn, true);
        if (!s1) {
            logError("restore thread %d: initStmt failed", thread->index);
            thread->code = TSDB_CODE_BCK_STMT_FAILED;
            return NULL;
        }
        bCtx.stmt      = s1;
        bCtx.conn      = thread->conn;
        bCtx.dbName    = argRenameDb(dbName);
        bCtx.stbChange = thread->stbChange;
    }

    /* Parquet STMT handles — initialized once per thread on first .par file encountered */
    TAOS_STMT  *parquetStmt  = NULL;
    TAOS_STMT2 *parquetStmt2 = NULL;

    /* ----- per-file restore loop ----- */
    for (int i = 0; i < thread->fileCnt; i++) {
        if (g_interrupted) {
            // Flush checkpoint buffer on interrupt
            flushCkptBuffer();
            if (thread->code == TSDB_CODE_SUCCESS) thread->code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }

        const char *filePath = thread->files[i];

        // skip if already restored (resume support; only active with -C / --checkpoint)
        if (argCheckpoint() && isRestoreDone(filePath)) {
            logDebug("restore thread %d: skip already restored: %s", thread->index, filePath);
            atomic_add_fetch_64(&g_stats.dataFilesSkipped, 1);
            atomic_add_fetch_64(&g_stats.dataFilesTotal, 1);
            // count skip as progress so the bar keeps advancing
            atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);
            continue;
        }

        logDebug("restore data thread %d: file %d/%d: %s",
                thread->index, i + 1, thread->fileCnt, filePath);

        // Dispatch on file extension: .dat → binary-taos, .par → parquet
        int   code    = TSDB_CODE_SUCCESS;
        int   pathLen = strlen(filePath);
        bool  isPar   = (pathLen > 4 &&
                         strcmp(filePath + pathLen - 4, ".par") == 0);
        int64_t fileRows = 0;  // rows restored in this single file
        if (isPar) {
            if (stmtVer == STMT_VERSION_2) {
                code = restoreOneParquetFileV2(&parquetStmt2, thread->conn, dbName, filePath,
                                               thread->stbChange, &fileRows);
            } else {
                code = restoreOneParquetFile(&parquetStmt, thread->conn, dbName, filePath,
                                             thread->stbChange, &fileRows);
            }
        } else if (stmtVer == STMT_VERSION_2) {
            int64_t rowsBefore = s2Ctx.totalRows;
            code = restoreOneDataFileV2(&s2Ctx, filePath);
            /* In multi-table mode totalRows is updated only on flush, so use
             * totalPendingRows delta for the real-time per-file row count. */
            fileRows = s2Ctx.totalRows - rowsBefore;
        } else {
            int64_t rowsBefore = bCtx.totalRows;
            code = restoreOneDataFile(&bCtx, filePath);
            fileRows = bCtx.totalRows - rowsBefore;
        }

        atomic_add_fetch_64(&g_stats.dataFilesTotal, 1);
        if (code != TSDB_CODE_SUCCESS) {
            logError("restore data file failed(%d): %s", code, filePath);
            atomic_add_fetch_64(&g_stats.dataFilesFailed, 1);
            if (thread->code == TSDB_CODE_SUCCESS) {
                thread->code = code;  // capture first error
            }
            // After a binary STMT failure the stmt may be in an error state.
            // Reset it so subsequent files can still be attempted.
            if (!isPar) {
                if (stmtVer == STMT_VERSION_2) {
                    stmt2ResetOnError(&s2Ctx);
                } else {
                    if (!stmtResetOnError(&bCtx)) {
                        logError("restore thread %d: cannot recover STMT1, aborting", thread->index);
                        break;
                    }
                }
            }
            // continue with next file (best effort)
        } else {
            // Insert into hash set first for in-memory deduplication
            insertCkptHash(filePath);
            // Write checkpoint immediately so a mid-run kill still saves progress.
            saveRestoreCheckpoint(filePath);
            // accumulate rows immediately so the progress display is real-time
            if (fileRows > 0) atomic_add_fetch_64(&g_stats.totalRows, fileRows);
            // count completed file for progress display
            atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);
            // accumulate actual processed bytes for File Size summary
            {
                int64_t fsz = 0;
                if (taosStatFile(filePath, &fsz, NULL, NULL) == 0 && fsz > 0)
                    atomic_add_fetch_64(&g_stats.dataFilesSizeBytes, fsz);
            }
        }
    }

    /* ----- post-loop flushes ----- */

    /* STMT1: flush remaining cross-file accumulated rows */
    if (stmtVer == STMT_VERSION_1 && bCtx.pendingRows > 0 && bCtx.stmt) {
        int flushCode = taos_stmt_execute(bCtx.stmt);
        if (flushCode != 0) {
            logError("restore thread %d: final STMT1 flush failed: %s",
                     thread->index, taos_stmt_errstr(bCtx.stmt));
            if (thread->code == TSDB_CODE_SUCCESS) thread->code = TSDB_CODE_BCK_STMT_FAILED;
        }
        bCtx.pendingRows = 0;
    }

    /* STMT2: flush any pending multi-table slots that didn't reach the threshold */
    if (stmtVer == STMT_VERSION_2 && s2Ctx.multiTable && s2Ctx.numPending > 0) {
        int64_t rowsBeforePostFlush = s2Ctx.totalRows;
        int flushCode = stmt2FlushMultiTableSlots(&s2Ctx);
        if (flushCode != 0 && thread->code == TSDB_CODE_SUCCESS)
            thread->code = flushCode;
        /* Update global stats for rows committed in this final flush. */
        int64_t finalFlushed = s2Ctx.totalRows - rowsBeforePostFlush;
        if (finalFlushed > 0) atomic_add_fetch_64(&g_stats.totalRows, finalFlushed);
    }

    /* ----- cleanup ----- */
    /* STMT2 */
    if (s2Ctx.colBinds) {
        stmt2FreeColBuffers(&s2Ctx);
        taosMemoryFree(s2Ctx.colBinds);
    }
    /* Free any remaining pending slots (e.g. if interrupted) */
    if (s2Ctx.pendingSlots) {
        for (int i = 0; i < s2Ctx.numPending; i++) stmt2FreeSlot(&s2Ctx.pendingSlots[i]);
        taosMemoryFree(s2Ctx.pendingSlots);
        s2Ctx.pendingSlots = NULL;
    }
    if (s2Ctx.stmt2) { taos_stmt2_close(s2Ctx.stmt2); s2Ctx.stmt2 = NULL; }

    /* STMT1 */
    if (bCtx.bindArray) {
        freeBindArray(bCtx.bindArray, bCtx.bindArrayCap);
        taosMemoryFree(bCtx.bindArray);
    }
    if (bCtx.stmt) {
        taos_stmt_close(bCtx.stmt);
        bCtx.stmt = NULL;
    }

    /* Parquet stmt handles (reused across .par files, closed once here) */
    if (parquetStmt)  { taos_stmt_close(parquetStmt);   parquetStmt  = NULL; }
    if (parquetStmt2) { taos_stmt2_close(parquetStmt2); parquetStmt2 = NULL; }

    // Flush thread-local checkpoint buffer before thread exits
    flushCkptBuffer();

    logInfo("data thread %d finished for %s.%s", thread->index, dbName, stbName4log);
    return NULL;
}


//
// -------------------------------------- FILE SCAN -----------------------------------------
//

//
// Find all data .dat files for a given STB under {outPath}/{dbName}/{stbName}_dataN/
//
static char** findDataFiles(const char *dbName, const char *stbName, int *totalCount) {
    *totalCount = 0;
    char *outPath = argOutPath();
    char dbDir[MAX_PATH_LEN];
    snprintf(dbDir, sizeof(dbDir), "%s/%s", outPath, dbName);

    // scan for directories matching {stbName}_dataN
    TdDirPtr dir = taosOpenDir(dbDir);
    if (dir == NULL) {
        logError("open db dir failed: %s", dbDir);
        return NULL;
    }

    char prefix[TSDB_TABLE_NAME_LEN + 16];
    snprintf(prefix, sizeof(prefix), "%s_data", stbName);
    int prefixLen = strlen(prefix);

    int capacity = 64;
    char **files = (char **)taosMemoryCalloc(capacity + 1, sizeof(char *));
    if (!files) {
        taosCloseDir(&dir);
        return NULL;
    }

    TdDirEntryPtr entry;
    while ((entry = taosReadDir(dir)) != NULL) {
        char *entryName = taosGetDirEntryName(entry);
        if (entryName[0] == '.') continue;

        // match data directories: {stbName}_dataN
        if (strncmp(entryName, prefix, prefixLen) != 0) continue;

        // check if it's a directory
        char subDir[MAX_PATH_LEN];
        snprintf(subDir, sizeof(subDir), "%s/%s", dbDir, entryName);
        
        if (!taosDirExist(subDir)) continue;

        // scan .dat files in this sub-directory
        TdDirPtr subDirPtr = taosOpenDir(subDir);
        if (subDirPtr == NULL) continue;

        TdDirEntryPtr subEntry;
        while ((subEntry = taosReadDir(subDirPtr)) != NULL) {
            char *subEntryName = taosGetDirEntryName(subEntry);
            if (subEntryName[0] == '.') continue;
            
            int nameLen = strlen(subEntryName);
            /* accept both .dat (binary-taos) and .par (parquet) files */
            bool isDat = (nameLen > 4 && strcmp(subEntryName + nameLen - 4, ".dat") == 0);
            bool isPar = (nameLen > 4 && strcmp(subEntryName + nameLen - 4, ".par") == 0);
            if (!isDat && !isPar) continue;

            if (*totalCount >= capacity) {
                capacity *= 2;
                char **tmp = (char **)taosMemoryRealloc(files, (capacity + 1) * sizeof(char *));
                if (!tmp) {
                    freeArrayPtr(files);
                    taosCloseDir(&subDirPtr);
                    taosCloseDir(&dir);
                    return NULL;
                }
                files = tmp;
            }

            char fullPath[MAX_PATH_LEN];
            snprintf(fullPath, sizeof(fullPath), "%s/%s", subDir, subEntryName);
            files[*totalCount] = taosStrdup(fullPath);
            (*totalCount)++;
        }

        taosCloseDir(&subDirPtr);
    }
    files[*totalCount] = NULL;

    taosCloseDir(&dir);
    return files;
}


//
// Restore data for one super table (parallel threads)
//
static int restoreStbData(DBInfo *dbInfo, const char *stbName, StbChangeMap *changeMap) {
    int code = TSDB_CODE_SUCCESS;
    const char *dbName = dbInfo->dbName;

    // Find all data files for this STB
    int fileCnt = 0;
    char **dataFiles = findDataFiles(dbName, stbName, &fileCnt);
    if (dataFiles == NULL || fileCnt == 0) {
        logInfo("no data files found for %s.%s", dbName, stbName);
        if (dataFiles) freeArrayPtr(dataFiles);
        return TSDB_CODE_SUCCESS;
    }

    logDebug("found %d data files for %s.%s", fileCnt, dbName, stbName);

    // update progress: how many files this STB/NTB has
    g_progress.ctbTotalCur = fileCnt;
    atomic_add_fetch_64(&g_progress.ctbTotalAll, fileCnt);
    atomic_store_64(&g_progress.ctbDoneCur, 0);

    // Determine thread count
    int threadCnt = argDataThread();
    if (fileCnt < threadCnt) {
        threadCnt = fileCnt;
    }

    logInfo("[%lld/%lld] db: %s  [%lld/%lld] stb: %s  data start  file: %d  threads: %d",
            (long long)g_progress.dbIndex, (long long)g_progress.dbTotal, dbName,
            (long long)g_progress.stbIndex, (long long)g_progress.stbTotal, stbName,
            fileCnt, threadCnt);

    // Allocate threads
    RestoreDataThread *threads = (RestoreDataThread *)taosMemoryCalloc(threadCnt, sizeof(RestoreDataThread));
    if (threads == NULL) {
        freeArrayPtr(dataFiles);
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    // Distribute files across threads (round-robin)
    // First, allocate file arrays for each thread
    int *fileCounts = (int *)taosMemoryCalloc(threadCnt, sizeof(int));
    char ***threadFiles = (char ***)taosMemoryCalloc(threadCnt, sizeof(char **));
    if (!fileCounts || !threadFiles) {
        taosMemoryFree(fileCounts);
        taosMemoryFree(threadFiles);
        taosMemoryFree(threads);
        freeArrayPtr(dataFiles);
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    // Count files per thread
    for (int i = 0; i < fileCnt; i++) {
        fileCounts[i % threadCnt]++;
    }

    // Allocate per-thread file arrays
    for (int t = 0; t < threadCnt; t++) {
        threadFiles[t] = (char **)taosMemoryCalloc(fileCounts[t] + 1, sizeof(char *));
        if (!threadFiles[t]) {
            for (int j = 0; j < t; j++) taosMemoryFree(threadFiles[j]);
            taosMemoryFree(threadFiles);
            taosMemoryFree(fileCounts);
            taosMemoryFree(threads);
            freeArrayPtr(dataFiles);
            return TSDB_CODE_BCK_MALLOC_FAILED;
        }
        fileCounts[t] = 0; // reset for filling
    }

    // Distribute files
    for (int i = 0; i < fileCnt; i++) {
        int t = i % threadCnt;
        threadFiles[t][fileCounts[t]] = dataFiles[i];
        fileCounts[t]++;
    }

    // Detect schema change for this super table (skip for normal tables "_ntb")
    StbChange *stbChange = NULL;
    if (changeMap && strcmp(stbName, NORMAL_TABLE_DIR) != 0) {
        // Get a connection to query server schema
        int scConnCode = TSDB_CODE_FAILED;
        TAOS *schemaConn = getConnection(&scConnCode);
        if (schemaConn) {
            int scCode = addStbChanged(changeMap, schemaConn, dbName, stbName);
            if (scCode != 0) {
                logWarn("schema change detection failed for %s.%s, proceeding without partial write", dbName, stbName);
            }
            stbChange = findStbChange(changeMap, stbName);
            releaseConnection(schemaConn);
        }
    }

    // Setup and create threads
    StbInfo stbInfo;
    memset(&stbInfo, 0, sizeof(StbInfo));
    stbInfo.dbInfo = dbInfo;
    stbInfo.stbName = stbName;

    for (int i = 0; i < threadCnt; i++) {
        threads[i].dbInfo    = dbInfo;
        threads[i].stbInfo   = &stbInfo;
        threads[i].index     = i + 1;
        threads[i].stbChange = stbChange;  // shared across threads (read-only)
        threads[i].conn    = getConnection(&code);
        if (!threads[i].conn) {
            for (int j = 0; j < i; j++) {
                releaseConnection(threads[j].conn);
            }
            for (int t = 0; t < threadCnt; t++) taosMemoryFree(threadFiles[t]);
            taosMemoryFree(threadFiles);
            taosMemoryFree(fileCounts);
            taosMemoryFree(threads);
            freeArrayPtr(dataFiles);
            return code;
        }
        threads[i].files   = threadFiles[i];
        threads[i].fileCnt = fileCounts[i];

        if (pthread_create(&threads[i].pid, NULL, restoreDataThread, (void *)&threads[i]) != 0) {
            logError("create restore data thread failed(%s) for stb: %s.%s", 
                     strerror(errno), dbName, stbName);
            for (int j = 0; j <= i; j++) {
                releaseConnection(threads[j].conn);
            }
            // cleanup
            for (int t = 0; t < threadCnt; t++) taosMemoryFree(threadFiles[t]);
            taosMemoryFree(threadFiles);
            taosMemoryFree(fileCounts);
            taosMemoryFree(threads);
            freeArrayPtr(dataFiles);
            return TSDB_CODE_BCK_CREATE_THREAD_FAILED;
        }
    }

    // Wait threads and collect first error
    for (int i = 0; i < threadCnt; i++) {
        pthread_join(threads[i].pid, NULL);
        releaseConnection(threads[i].conn);
        if (code == TSDB_CODE_SUCCESS && threads[i].code != TSDB_CODE_SUCCESS) {
            code = threads[i].code;
        }
    }

    // Cleanup
    for (int t = 0; t < threadCnt; t++) {
        taosMemoryFree(threadFiles[t]);
    }
    taosMemoryFree(threadFiles);
    taosMemoryFree(fileCounts);
    taosMemoryFree(threads);

    // Free the dataFiles array but NOT the strings (they were passed to threads)
    // Actually the strings are still owned by dataFiles, so free normally
    freeArrayPtr(dataFiles);

    return code;
}


//
// -------------------------------------- MAIN -----------------------------------------
//

//
// Get STB names from backup directory (same as restoreMeta)
//
static char** getBackupStbNamesForData(const char *dbName, int *code) {
    *code = TSDB_CODE_SUCCESS;

    char *outPath = argOutPath();
    char dbDir[MAX_PATH_LEN];
    snprintf(dbDir, sizeof(dbDir), "%s/%s", outPath, dbName);

    TdDirPtr dir = taosOpenDir(dbDir);
    if (dir == NULL) {
        logError("open backup db dir failed: %s", dbDir);
        *code = TSDB_CODE_BCK_OPEN_DIR_FAILED;
        return NULL;
    }

    int capacity = 16;
    int count = 0;
    char **names = (char **)taosMemoryCalloc(capacity + 1, sizeof(char *));
    if (!names) {
        taosCloseDir(&dir);
        *code = TSDB_CODE_BCK_MALLOC_FAILED;
        return NULL;
    }

    // Look for {stbName}.csv files (schema files identify STBs)
    TdDirEntryPtr entry;
    while ((entry = taosReadDir(dir)) != NULL) {
        char *entryName = taosGetDirEntryName(entry);
        if (entryName[0] == '.') continue;
        
        int nameLen = strlen(entryName);
        if (nameLen > 4 && strcmp(entryName + nameLen - 4, ".csv") == 0) {
            if (count >= capacity) {
                capacity *= 2;
                char **tmp = (char **)taosMemoryRealloc(names, (capacity + 1) * sizeof(char *));
                if (!tmp) {
                    freeArrayPtr(names);
                    taosCloseDir(&dir);
                    *code = TSDB_CODE_BCK_MALLOC_FAILED;
                    return NULL;
                }
                names = tmp;
            }
            char stbName[TSDB_TABLE_NAME_LEN] = {0};
            int stbNameLen = nameLen - 4;
            if (stbNameLen >= TSDB_TABLE_NAME_LEN) stbNameLen = TSDB_TABLE_NAME_LEN - 1;
            memcpy(stbName, entryName, stbNameLen);
            stbName[stbNameLen] = '\0';
            names[count++] = taosStrdup(stbName);
        }
    }
    names[count] = NULL;

    taosCloseDir(&dir);
    return names;
}


//
// restore database data
//
int restoreDatabaseData(const char *dbName) {
    int code = TSDB_CODE_SUCCESS;

    // Reset DATA-phase progress counters so META-phase accumulation in
    // ctbDoneAll / ctbTotalAll doesn't corrupt ETA and speed calculations.
    atomic_store_64(&g_progress.ctbDoneAll,  0);
    atomic_store_64(&g_progress.ctbTotalAll, 0);
    atomic_store_64(&g_progress.ctbDoneCur,  0);
    g_progress.ctbTotalCur = 0;
    g_progress.startMs     = taosGetTimestampMs();

    // load checkpoint for resume support
    loadRestoreCheckpoint(dbName);

    // Initialize schema change map for this database
    StbChangeMap changeMap;
    stbChangeMapInit(&changeMap);

    //
    // super tables data
    //
    int stbCode = TSDB_CODE_SUCCESS;
    char **stbNames = getBackupStbNamesForData(dbName, &stbCode);
    if (stbNames == NULL) {
        if (stbCode != TSDB_CODE_SUCCESS) {
            stbChangeMapDestroy(&changeMap);
            freeRestoreCheckpoint();
            return stbCode;
        }
        // No STBs found — nothing to restore; clean up checkpoint.
        deleteRestoreCheckpoint();
        stbChangeMapDestroy(&changeMap);
        freeRestoreCheckpoint();
        return TSDB_CODE_SUCCESS;
    }

    DBInfo dbInfo;
    dbInfo.dbName = dbName;

    // count STBs for progress (+1 for NTB phase only if NTB dir exists)
    int stbRawCount = 0;
    for (int k = 0; stbNames[k] != NULL; k++) stbRawCount++;
    char ntbDir[MAX_PATH_LEN];
    snprintf(ntbDir, sizeof(ntbDir), "%s/%s/" NORMAL_TABLE_DIR "_data0", argOutPath(), dbName);
    bool hasNtb = taosDirExist(ntbDir);
    g_progress.stbTotal = stbRawCount + (hasNtb ? 1 : 0);
    g_progress.stbIndex = 0;

    for (int i = 0; stbNames[i] != NULL; i++) {
        if (g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }

        // update progress: which STB we're restoring
        g_progress.stbIndex++;
        snprintf(g_progress.stbName, PROGRESS_STB_NAME_LEN, "%s", stbNames[i]);
        g_progress.ctbTotalCur = 0;
        atomic_store_64(&g_progress.ctbDoneCur, 0);

        code = restoreStbData(&dbInfo, stbNames[i], &changeMap);

        // accumulate completed files
        int64_t doneCur = g_progress.ctbDoneCur;
        atomic_add_fetch_64(&g_progress.ctbDoneAll, doneCur);
        atomic_store_64(&g_progress.ctbDoneCur, 0);

        if (code != TSDB_CODE_SUCCESS) {
            logError("restore stb data failed(0x%08X): %s.%s", code, dbName, stbNames[i]);
            freeArrayPtr(stbNames);
            stbChangeMapDestroy(&changeMap);
            freeRestoreCheckpoint();
            return code;
        }
    }

    freeArrayPtr(stbNames);

    if (code != TSDB_CODE_SUCCESS) {
        stbChangeMapDestroy(&changeMap);
        freeRestoreCheckpoint();
        return code;
    }

    //
    // normal tables data (no schema change detection for normal tables)
    //
    if (!hasNtb) {
        logDebug("no normal table data dir for db: %s, skipping", dbName);
        g_progress.stbIndex++;  // keep progress counter consistent
        goto ntb_done;
    }

    // update progress for NTB phase
    g_progress.stbIndex++;
    snprintf(g_progress.stbName, PROGRESS_STB_NAME_LEN, "(ntb)");
    g_progress.ctbTotalCur = 0;
    atomic_store_64(&g_progress.ctbDoneCur, 0);

    code = restoreStbData(&dbInfo, NORMAL_TABLE_DIR, NULL);

    // accumulate NTB files
    {
        int64_t ntbDone = g_progress.ctbDoneCur;
        atomic_add_fetch_64(&g_progress.ctbDoneAll, ntbDone);
        atomic_store_64(&g_progress.ctbDoneCur, 0);
    }

    if (code != TSDB_CODE_SUCCESS) {
        logError("restore normal table data failed(%d): %s", code, dbName);
        stbChangeMapDestroy(&changeMap);
        freeRestoreCheckpoint();
        return code;
    }

ntb_done:
    // All data restored successfully.  Delete the checkpoint file so that
    // the next restore run always starts fresh instead of skipping everything.
    deleteRestoreCheckpoint();
    stbChangeMapDestroy(&changeMap);
    freeRestoreCheckpoint();
    return code;
}
