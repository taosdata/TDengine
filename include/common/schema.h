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

#ifndef _TD_SCHEMA_H_
#define _TD_SCHEMA_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

// ----------------- TSDB COLUMN DEFINITION
typedef struct {
  int8_t   type;    // Column type
  int16_t  colId;   // column ID
  int16_t  bytes;   // column bytes (restore to int16_t in case of misuse)
  uint16_t offset;  // point offset in SDataRow after the header part.
} STColumn;

#define colType(col) ((col)->type)
#define colColId(col) ((col)->colId)
#define colBytes(col) ((col)->bytes)
#define colOffset(col) ((col)->offset)

#define colSetType(col, t) (colType(col) = (t))
#define colSetColId(col, id) (colColId(col) = (id))
#define colSetBytes(col, b) (colBytes(col) = (b))
#define colSetOffset(col, o) (colOffset(col) = (o))

// ----------------- TSDB SCHEMA DEFINITION
typedef struct {
  int version;    // version
  int numOfCols;  // Number of columns appended
  int tlen;  // maximum length of a SDataRow without the header part (sizeof(VarDataOffsetT) + sizeof(VarDataLenT) + //
             // (bytes))
  uint16_t flen;  // First part length in a SDataRow after the header part
  uint16_t vlen;  // pure value part length, excluded the overhead (bytes only)
  STColumn columns[];
} STSchema;

#define schemaNCols(s) ((s)->numOfCols)
#define schemaVersion(s) ((s)->version)
#define schemaTLen(s) ((s)->tlen)
#define schemaFLen(s) ((s)->flen)
#define schemaVLen(s) ((s)->vlen)
#define schemaColAt(s, i) ((s)->columns + i)
#define tdFreeSchema(s) tfree((s))

STSchema *tdDupSchema(STSchema *pSchema);
int       tdEncodeSchema(void **buf, STSchema *pSchema);
void *    tdDecodeSchema(void *buf, STSchema **pRSchema);

static FORCE_INLINE int comparColId(const void *key1, const void *key2) {
  if (*(int16_t *)key1 > ((STColumn *)key2)->colId) {
    return 1;
  } else if (*(int16_t *)key1 < ((STColumn *)key2)->colId) {
    return -1;
  } else {
    return 0;
  }
}

static FORCE_INLINE STColumn *tdGetColOfID(STSchema *pSchema, int16_t colId) {
  void *ptr = bsearch(&colId, (void *)pSchema->columns, schemaNCols(pSchema), sizeof(STColumn), comparColId);
  if (ptr == NULL) return NULL;
  return (STColumn *)ptr;
}

// ----------------- SCHEMA BUILDER DEFINITION
typedef struct {
  int       tCols;
  int       nCols;
  int       tlen;
  uint16_t  flen;
  uint16_t  vlen;
  int       version;
  STColumn *columns;
} STSchemaBuilder;

int       tdInitTSchemaBuilder(STSchemaBuilder *pBuilder, int32_t version);
void      tdDestroyTSchemaBuilder(STSchemaBuilder *pBuilder);
void      tdResetTSchemaBuilder(STSchemaBuilder *pBuilder, int32_t version);
int       tdAddColToSchema(STSchemaBuilder *pBuilder, int8_t type, int16_t colId, int16_t bytes);
STSchema *tdGetSchemaFromBuilder(STSchemaBuilder *pBuilder);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SCHEMA_H_*/