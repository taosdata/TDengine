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

int32_t azPutObjectFromFileOffset(const char *file, const char *object_name, int64_t offset, int64_t size);
/*
#define WAL_PROTO_VER    0
#define WAL_NOSUFFIX_LEN 20

typedef enum {
TAOS_WAL_SKIP = 0,
TAOS_WAL_WRITE = 1,
TAOS_WAL_FSYNC = 2,
} EWalType;

typedef struct {
int32_t  vgId;
EWalType level;  // wal level
int8_t   clearFiles;
} SWalCfg;
*/
#ifdef __cplusplus
}
#endif

#endif  // _TD_AZURE_H_
