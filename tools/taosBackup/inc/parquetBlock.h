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

#ifndef INC_PARQUET_BLOCK_H_
#define INC_PARQUET_BLOCK_H_

#include <stdint.h>

#ifndef _WIN32

/*
 * Pure C interface to the Apache Arrow / Parquet C++ library.
 *
 * parquetBlock.cpp compiles the Arrow C++ code and exposes every symbol
 * through extern "C" linkage so that all .c translation units in taosBackup
 * can call these functions without any C++ knowledge.
 *
 *  Backup path :  parquetWriterCreate → parquetWriterWriteBlock × N → parquetWriterClose
 *  Restore path:  parquetReaderOpen  → parquetReaderReadAll           → parquetReaderClose
 */

/* Avoid dragging taos.h C++ issues into this header when included from C++. */
#ifdef __cplusplus
#include <taos.h>
#include "colCompress.h"
extern "C" {
#else
#include "bck.h"
#include "colCompress.h"
#endif

/* ------------------------------------------------------------------ */
/* Opaque handles (implementation lives in parquetBlock.cpp)           */
/* ------------------------------------------------------------------ */
typedef struct ParquetWriter ParquetWriter;
typedef struct ParquetReader ParquetReader;

/* ------------------------------------------------------------------ */
/* Writer API  (backup)                                                 */
/* ------------------------------------------------------------------ */
ParquetWriter *parquetWriterCreate(const char *fileName, TAOS_FIELD *fields,
                                   int numFields, TAOS_FIELD_E *efields,
                                   int *code);
int parquetWriterWriteBlock(ParquetWriter *pw, void *block, int blockRows);
int parquetWriterClose(ParquetWriter *pw);

/* ------------------------------------------------------------------ */
/* Reader API  (restore)                                                */
/* ------------------------------------------------------------------ */
typedef int (*ParquetBindCallback)(void *userData,
                                   TAOS_FIELD *fields, int numFields,
                                   TAOS_MULTI_BIND *bindArray, int32_t numRows);
ParquetReader *parquetReaderOpen(const char *fileName, int *code);
int parquetReaderReadAll(ParquetReader *pr,
                         ParquetBindCallback callback, void *userData);
void parquetReaderClose(ParquetReader *pr);
int64_t parquetGetNumRowsQuick(const char *fileName);
int parquetReaderGetFields(ParquetReader *pr, TAOS_FIELD **outFields);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#else  /* _WIN32 — Parquet/Arrow not supported on Windows */

#include <taos.h>

typedef struct ParquetWriter ParquetWriter;
typedef struct ParquetReader ParquetReader;
typedef int (*ParquetBindCallback)(void *userData,
                                   TAOS_FIELD *fields, int numFields,
                                   TAOS_MULTI_BIND *bindArray, int32_t numRows);

static inline ParquetWriter *parquetWriterCreate(const char *f, TAOS_FIELD *flds,
                                                  int n, TAOS_FIELD_E *ef, int *code) {
    (void)f; (void)flds; (void)n; (void)ef; if (code) *code = -1; return NULL;
}
static inline int parquetWriterWriteBlock(ParquetWriter *pw, void *b, int r) {
    (void)pw; (void)b; (void)r; return -1;
}
static inline int parquetWriterClose(ParquetWriter *pw) { (void)pw; return -1; }
static inline ParquetReader *parquetReaderOpen(const char *f, int *code) {
    (void)f; if (code) *code = -1; return NULL;
}
static inline int parquetReaderReadAll(ParquetReader *pr, ParquetBindCallback cb, void *ud) {
    (void)pr; (void)cb; (void)ud; return -1;
}
static inline void parquetReaderClose(ParquetReader *pr) { (void)pr; }
static inline int64_t parquetGetNumRowsQuick(const char *f) { (void)f; return -1; }
static inline int parquetReaderGetFields(ParquetReader *pr, TAOS_FIELD **out) {
    (void)pr; (void)out; return -1;
}

#endif  /* _WIN32 */

#endif  /* INC_PARQUET_BLOCK_H_ */
