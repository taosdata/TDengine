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
#define TSDB_CODE_BCK_MALLOC_FAILED           TAOS_DEF_ERROR_CODE(0, 0xA001)
#define TSDB_CODE_BCK_CREATE_THREAD_FAILED    TAOS_DEF_ERROR_CODE(0, 0xA002)
#define TSDB_CODE_BCK_CREATE_FILE_FAILED      TAOS_DEF_ERROR_CODE(0, 0xA003)
#define TSDB_CODE_BCK_NO_FIELDS               TAOS_DEF_ERROR_CODE(0, 0xA004)
#define TSDB_CODE_BCK_FETCH_FIELDS_FAILED     TAOS_DEF_ERROR_CODE(0, 0xA005)
#define TSDB_CODE_BCK_COMPRESS_FAILED         TAOS_DEF_ERROR_CODE(0, 0xA006)
#define TSDB_CODE_BCK_WRITE_FILE_FAILED       TAOS_DEF_ERROR_CODE(0, 0xA007)
#define TSDB_CODE_BCK_CONN_POOL_EXHAUSTED     TAOS_DEF_ERROR_CODE(0, 0xA008)
#define TSDB_CODE_BCK_READ_FILE_FAILED        TAOS_DEF_ERROR_CODE(0, 0xA009)
#define TSDB_CODE_BCK_INVALID_FILE            TAOS_DEF_ERROR_CODE(0, 0xA00A)
#define TSDB_CODE_BCK_STMT_FAILED             TAOS_DEF_ERROR_CODE(0, 0xA00B)
#define TSDB_CODE_BCK_DECOMPRESS_FAILED       TAOS_DEF_ERROR_CODE(0, 0xA00C)
#define TSDB_CODE_BCK_OPEN_DIR_FAILED         TAOS_DEF_ERROR_CODE(0, 0xA00D)
#define TSDB_CODE_BCK_EXEC_SQL_FAILED         TAOS_DEF_ERROR_CODE(0, 0xA00E)
#define TSDB_CODE_BCK_USER_CANCEL             TAOS_DEF_ERROR_CODE(0, 0xA00F)


#endif  // INC_BCKERROR_H_
