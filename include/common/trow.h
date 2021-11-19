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

#ifndef _TD_COMMON_ROW_H_
#define _TD_COMMON_ROW_H_

#include "os.h"
#include "tbuffer.h"
#include "tdef.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TD_OR_ROW 0
#define TD_KV_ROW 1

typedef uint16_t col_id_t;

typedef struct {
  TSKEY ts;
} SOrRow;

typedef struct {
  col_id_t cid;
  uint32_t offset;
} SKvRowIdx;

typedef struct {
  uint16_t  ncols;
  SKvRowIdx cidx[];
} SKvRow;

typedef struct {
  union {
    /// union field for encode and decode
    uint64_t info;
    struct {
      /// row type
      uint64_t type : 2;
      /// row schema version
      uint64_t sver : 16;
      /// row total length
      uint64_t len : 46;
    };
  };
  /// row version
  uint64_t ver;
  /// timestamp of the row
  TSKEY    ts;
  char     content[];
} SRow;

typedef enum {
  /// ordinary row builder
  TD_OR_ROW_BUILDER = 0,
  /// kv row builder
  TD_KV_ROW_BUILDER,
  /// self-determined row builder
  TD_SD_ROW_BUILDER
} ERowBbuilderT;

typedef struct {
  /// row builder type
  ERowBbuilderT type;
  /// buffer writer
  SBufferWriter bw;
  /// target row
  SRow *pRow;
} SRowBuilder;

typedef struct {
  /* TODO */
} SRowBatchBuilder;

#define tRBInit(type, allocator, endian) \
  { .type = (type), tbufInitWriter(allocator, endian), NULL }
void tRBClear(SRowBuilder *pRB);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_ROW_H_*/