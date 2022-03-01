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

#ifndef _TD_COMMON_SCHEMA_H_
#define _TD_COMMON_SCHEMA_H_

#include "os.h"
#include "tarray.h"
#include "ttypes.h"

#ifdef __cplusplus
extern "C" {
#endif

#if 0
typedef struct STColumn {
  /// column name
  char *cname;
  union {
    /// for encode purpose
    uint64_t info;
    struct {
      uint64_t sma : 1;
      /// column data type
      uint64_t type : 7;
      /// column id
      uint64_t cid : 16;
      /// max bytes of the column
      uint64_t bytes : 32;
      /// reserved
      uint64_t reserve : 8;
    };
  };
  /// comment about the column
  char *comment;
} STColumn;

typedef struct STSchema {
  /// schema version
  uint16_t sver;
  /// number of columns
  uint16_t ncols;
  /// sma attributes
  struct {
    bool    sma;
    SArray *smaArray;
  };
  /// column info
  STColumn cols[];
} STSchema;

typedef struct {
  uint64_t  size;
  STSchema *pSchema;
} STShemaBuilder;

#define tSchemaBuilderInit(target, capacity) \
  { .size = (capacity), .pSchema = (target) }
void tSchemaBuilderSetSver(STShemaBuilder *pSchemaBuilder, uint16_t sver);
void tSchemaBuilderSetSMA(bool sma, SArray *smaArray);
int32_t  tSchemaBuilderPutColumn(char *cname, bool sma, uint8_t type, col_id_t cid, uint32_t bytes, char *comment);

#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_SCHEMA_H_*/