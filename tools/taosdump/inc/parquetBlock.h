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

#endif  /* INC_PARQUET_BLOCK_H_ */
