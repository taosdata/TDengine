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

//
// ---------------- define ----------------
//


// ---------------- interface ----------------
int writeBlockToFileParquet(const char *fileName, void *block, int blockRows);

#endif  // INC_STORAGE_PARQUET_H_
