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
#include "tdataformat.h"
#include "tdef.h"
#include "tschema.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TD_UNDECIDED_ROW 0
#define TD_OR_ROW 1
#define TD_KV_ROW 2

typedef struct {
  // TODO
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
    uint32_t info;
    struct {
      /// row type
      uint32_t type : 2;
      /// row schema version
      uint32_t sver : 16;
      /// is delete row
      uint32_t del : 1;
      /// reserved for back compatibility
      uint32_t reserve : 13;
    };
  };
  /// row total length
  uint32_t len;
  /// row version
  uint64_t ver;
  /// timestamp
  TSKEY ts;
  /// the inline data, maybe a tuple or a k-v tuple
  char data[];
} STSRow;

typedef struct {
  uint32_t nRows;
  char     rows[];
} STSRowBatch;

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
  STSRow *pRow;
} STSRowBuilder;

typedef struct {
  STSchema *pSchema;
  STSRow *    pRow;
} STSRowReader;

typedef struct {
  uint32_t   it;
  STSRowBatch *pRowBatch;
} STSRowBatchIter;

// STSRowBuilder
#define trbInit(rt, allocator, endian, target, size) \
  { .type = (rt), .bw = tbufInitWriter(allocator, endian), .pRow = (target) }
void trbSetRowInfo(STSRowBuilder *pRB, bool del, uint16_t sver);
void trbSetRowVersion(STSRowBuilder *pRB, uint64_t ver);
void trbSetRowTS(STSRowBuilder *pRB, TSKEY ts);
int  trbWriteCol(STSRowBuilder *pRB, void *pData, col_id_t cid);

// STSRowReader
#define tRowReaderInit(schema, row) \
  { .schema = (schema), .row = (row) }
int tRowReaderRead(STSRowReader *pRowReader, col_id_t cid, void *target, uint64_t size);

// STSRowBatchIter
#define tRowBatchIterInit(pRB) \
  { .it = 0, .pRowBatch = (pRB) }
const STSRow *tRowBatchIterNext(STSRowBatchIter *pRowBatchIter);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_ROW_H_*/