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

#ifndef INC_BCKERROR_H_
#define INC_BCKERROR_H_

#include "taoserror.h"

// ---------------- error code ----------------


#define TSDB_CODE_BCK_INVALID_PARAM           TAOS_DEF_ERROR_CODE(0, 0xA000)
#define TSDB_CODE_BCK_CREATE_THREAD_FAILED    TAOS_DEF_ERROR_CODE(0, 0xA001)
#define TSDB_CODE_BCK_CREATE_FILE_FAILED      TAOS_DEF_ERROR_CODE(0, 0xA002)


#endif  // INC_BCKERROR_H_
