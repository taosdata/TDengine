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

#include <stdint.h>

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

/*
 * Create a Parquet file writer.
 * The TAOS schema is stored in the file's key-value metadata so that
 * the reader can reconstruct exact types and byte-widths on restore.
 *
 * @param fileName  Output file path (.par)
 * @param fields    TAOS_FIELD array describing the result set
 * @param numFields Number of fields
 * @param efields   TAOS_FIELD_E array (may be NULL).  When provided, precision
 *                  and scale for DECIMAL columns are stored in the metadata so
 *                  the reader can reconstruct them without packet format changes.
 * @param code      Receives TSDB_CODE_* on failure
 * @return          Opaque handle, or NULL on failure
 */
ParquetWriter *parquetWriterCreate(const char *fileName, TAOS_FIELD *fields,
                                   int numFields, TAOS_FIELD_E *efields,
                                   int *code);

/*
 * Append one TDengine raw block to the Parquet file.
 * The block is the in-memory columnar format returned by taos_fetch_raw_block.
 */
int parquetWriterWriteBlock(ParquetWriter *pw, void *block, int blockRows);

/*
 * Flush buffers, write the Parquet footer, and free the writer.
 */
int parquetWriterClose(ParquetWriter *pw);


/* ------------------------------------------------------------------ */
/* Reader API  (restore)                                                */
/* ------------------------------------------------------------------ */

/*
 * Callback invoked once per row-group (batch) while reading a .par file.
 *
 * Parameters:
 *   userData   – caller-supplied context pointer
 *   fields     – TAOS_FIELD array recovered from file metadata (numFields entries)
 *   numFields  – column count
 *   bindArray  – ready-to-use TAOS_MULTI_BIND array (one entry per column).
 *                  Memory is owned by the reader; valid only during this call.
 *                  Do NOT free these pointers.
 *   numRows    – number of rows in this batch
 *
 * Return 0 to continue reading, non-zero to abort (the value is propagated
 * as the return code of parquetReaderReadAll).
 */
typedef int (*ParquetBindCallback)(void *userData,
                                   TAOS_FIELD *fields, int numFields,
                                   TAOS_MULTI_BIND *bindArray, int32_t numRows);

/*
 * Open a .par file for reading.
 *
 * @param fileName  Input file path
 * @param code      Receives TSDB_CODE_* on failure
 * @return          Opaque handle, or NULL on failure
 */
ParquetReader *parquetReaderOpen(const char *fileName, int *code);

/*
 * Iterate over all row-groups, invoking @callback for each batch.
 * Returns the first non-zero value returned by @callback, or
 * TSDB_CODE_SUCCESS when all batches were processed without error.
 */
int parquetReaderReadAll(ParquetReader *pr,
                         ParquetBindCallback callback, void *userData);

/*
 * Close the reader and release all resources.
 */
void parquetReaderClose(ParquetReader *pr);

/*
 * Retrieve the schema recovered from the file metadata.
 * @param outFields   Set to a pointer into the reader's internal storage.
 *                    Valid until parquetReaderClose(); do NOT free.
 * @return  Number of fields (>= 1), or a negative TSDB_CODE on error.
 */
int parquetReaderGetFields(ParquetReader *pr, TAOS_FIELD **outFields);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif  /* INC_PARQUET_BLOCK_H_ */
