/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef _TD_AZURE_H_
#define _TD_AZURE_H_

#include "os.h"
#include "tarray.h"
#include "tdef.h"
#include "tlog.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

#define WAL_PROTO_VER     0
#define WAL_NOSUFFIX_LEN  20
#define WAL_SUFFIX_AT     (WAL_NOSUFFIX_LEN + 1)
#define WAL_LOG_SUFFIX    "log"
#define WAL_INDEX_SUFFIX  "idx"
#define WAL_REFRESH_MS    1000
#define WAL_PATH_LEN      (TSDB_FILENAME_LEN + 12)
#define WAL_FILE_LEN      (WAL_PATH_LEN + 32)
#define WAL_MAGIC         0xFAFBFCFDF4F3F2F1ULL
#define WAL_SCAN_BUF_SIZE (1024 * 1024 * 3)

typedef enum {
  TAOS_WAL_SKIP = 0,
  TAOS_WAL_WRITE = 1,
  TAOS_WAL_FSYNC = 2,
} EWalType;

typedef struct {
  int32_t  vgId;
  int32_t  fsyncPeriod;      // millisecond
  int32_t  retentionPeriod;  // secs
  int32_t  rollPeriod;       // secs
  int64_t  retentionSize;
  int64_t  segSize;
  EWalType level;  // wal level
  int32_t  encryptAlgorithm;
  char     encryptKey[ENCRYPT_KEY_LEN + 1];
  int8_t   clearFiles;
} SWalCfg;

int64_t walGetVerRetention(SWal *pWal, int64_t bytes);
int64_t walGetCommittedVer(SWal *);
int64_t walGetAppliedVer(SWal *);

#ifdef __cplusplus
}
#endif

#endif  // _TD_AZURE_H_
