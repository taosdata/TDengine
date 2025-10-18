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

#ifndef INC_BCKBACKUP_H_
#define INC_BCKBACKUP_H_

#include <taos.h>
#include <taoserror.h>

#include "bckArgs.h"

//
// ---------------- define ----------------
//


//
// ---------------- function ----------------
//

// backup main function
int backupMain();

int backDBMeta(const char *dbName);
int backDBData(const char *dbName);



#endif  // INC_BCKBACKUP_H_
