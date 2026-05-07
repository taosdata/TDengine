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

#ifndef INC_RESTORE_H_
#define INC_RESTORE_H_

#include "bck.h"

//
// ---------------- define ----------------
//

#define STMT_BATCH_SIZE  1000  // rows per STMT execute batch

// ---------------- interface ----------------
int restoreMain();


#endif  // INC_RESTORE_H_
