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

#define TD_SUPPORT_NONE_VAL

// row type
#define TD_ROW_TP 0  // default
#define TD_ROW_KV 1

// val type
#define TD_VTYPE_NORM 0x0   // normal val: not none, not null
#define TD_VTYPE_NONE 0x01  // none or unknown/undefined
#define TD_VTYPE_NULL 0x02  // null val

#ifdef TD_SUPPORT_NONE_VAL
static FORCE_INLINE bool tdValTypeIsNorm(int8_t valType) { return (valType & TD_VTYPE_NORM); }
static FORCE_INLINE bool tdValTypeIsNone(int8_t valType) { return (valType & TD_VTYPE_NONE); }
static FORCE_INLINE bool tdValTypeIsNull(int8_t valType) { return (valType & TD_VTYPE_NULL); }
#else

#endif

static FORCE_INLINE bool tdValIsNorm(int8_t valType, const void *val, int32_t type) {
#ifdef TD_SUPPORT_NONE_VAL
  return tdValTypeIsNorm(valType);
#else
  return !isNull(val, type);
#endif
}
static FORCE_INLINE bool tdValIsNone(int8_t valType) {
#ifdef TD_SUPPORT_NONE_VAL
  return tdValTypeIsNone(valType);
#else
  return false;
#endif
}
static FORCE_INLINE bool tdValIsNull(int8_t valType, const void *val, int32_t type) {
#ifdef TD_SUPPORT_NONE_VAL
  return tdValTypeIsNull(valType);
#else
  return isNull(val, type);
#endif
}

#define TD_ROW_LEN(r)
#define TD_ROW_TLEN(r)
#define TD_ROW_TYPE(r)
#define TD_ROW_BODY(r)
#define TD_ROW_TKEY(r)
#define TD_ROW_KEY(r)
#define TD_ROW_DELETED(r)
#define TD_ROW_VERSION(r)
#define TD_ROW_MAX_BYTES_FROM_SCHEMA(s)

#define TD_ROW_SET_LEN(r, l)
#define TD_ROW_SET_VERSION(r, v)
#define TD_ROW_CPY(dst, r)

// ----------------- SRow appended with tuple row structure(STpRow)
/*
 * |---------|------------------------------------------------- len -------------------------------------->|
 * |<--------     Head      ------>|<---------   flen  ------------->|<-- blen  -->|                       |
 * |---------+---------------------+---------------------------------+-------------+-----------------------+
 * | uint8_t | uint32_t |  int16_t |                                 |             |                       |
 * |---------+----------+----------+---------------------------------+-------------------------------------+
 * |  flag   |   len    | sversion |(key)      first part            |   bitmap    |    second part        |
 * +---------+----------+----------+---------------------------------+-------------------------------------+
 *
 * NOTE: Timestamp in this row structure is TKEY instead of TSKEY
 *       Use 2 bits in bitmap for each column
 * flag:
 *       0: flag&0x01 0 STpRow, 1 SKvRow // since 2.0
 *       1: flag&0x02 0 without bitmap, 1 with bitmap.  // 如果None值支持数据库或者更小维度，则需要指定一个bit区分。TODO
 *       2: endian(0 big endian, 1 little endian)
 *       3-7: reserved(value 0)
 */

// ----------------- SRow appended with K-V row structure(SKvRow)
/* |--------------------|------------------------------------------------  len --------------------------->|
 * |<--------     Head        ---->|<--------- colsIdxLen ---------->|<-- blen -->|                        |
 * |--------------------+----------+-----------------------------------------------------------------------+
 * | uint8_t | uint32_t |  int16_t |                                 |            |                        |
 * |---------+----------+----------+-----------------------------------------------------------------------+
 * |   flag  |   len    |   ncols  |(keyColId)    cols index         |   bitmap   |     data part          |
 * |---------+----------+----------+---------------------------------+-------------------------------------+
 *
 * NOTE: Timestamp in this row structure is TKEY instead of TSKEY
 */

typedef void *SRow;

typedef struct {
  int8_t valType;
  void * val;
} SCellVal;

typedef struct {
  // TODO
} STpRow;  // tuple

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