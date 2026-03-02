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

#ifndef INC_STORAGE_PARQUET_H_
#define INC_STORAGE_PARQUET_H_

#include "bck.h"

/*
 * Backup: stream TAOS_RES rows into a .par (ApacheArrow/Parquet) file.
 */
int resultToFileParquet(TAOS_RES *res, const char *fileName);

/*
 * Restore: read a .par file and insert all rows into @dbName.@tbName
 * using a pre-initialised TAOS_STMT handle.
 *
 * The function iterates every row-group in the file, populates
 * TAOS_MULTI_BIND arrays from the Arrow data, and calls
 *   taos_stmt_bind_param_batch → taos_stmt_add_batch
 * for each batch, flushing with taos_stmt_execute every
 * STMT_BATCH_THRESHOLD rows.  A final execute is performed after
 * the last batch.
 *
 * @param stmt       Prepared TAOS_STMT (INSERT INTO `db`.`tb` VALUES(?,…))
 * @param fileName   Path to the .par file
 * @param outRows    If non-NULL, receives total rows inserted
 */
int fileParquetToStmt(TAOS_STMT *stmt,
                      const char *fileName,
                      int64_t    *outRows);

#endif  // INC_STORAGE_PARQUET_H_
