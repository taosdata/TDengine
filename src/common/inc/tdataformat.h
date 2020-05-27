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
#ifndef _TD_DATA_FORMAT_H_
#define _TD_DATA_FORMAT_H_

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "taosdef.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

#define STR_TO_VARSTR(x, str) do {VarDataLenT __len = strlen(str); \
  *(VarDataLenT*)(x) = __len; \
  strncpy(varDataVal(x), (str), __len);} while(0);

#define STR_WITH_MAXSIZE_TO_VARSTR(x, str, _maxs) do {\
  char* _e = stpncpy(varDataVal(x), (str), (_maxs));\
  varDataSetLen(x, (_e - (x) - VARSTR_HEADER_SIZE));\
} while(0)

#define STR_WITH_SIZE_TO_VARSTR(x, str, _size) do {\
  *(VarDataLenT*)(x) = (_size); \
  strncpy(varDataVal(x), (str), (_size));\
} while(0);

// ----------------- TSDB COLUMN DEFINITION
typedef struct {
  int8_t  type;    // Column type
  int16_t colId;   // column ID
  int32_t bytes;   // column bytes
  int32_t offset;  // point offset in SDataRow after the header part
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
  int      totalCols;  // Total columns allocated
  int      numOfCols;  // Number of columns appended
  int      tlen;       // maximum length of a SDataRow without the header part
  int      flen;       // First part length in a SDataRow after the header part
  STColumn columns[];
} STSchema;

#define schemaNCols(s) ((s)->numOfCols)
#define schemaTotalCols(s) ((s)->totalCols)
#define schemaTLen(s) ((s)->tlen)
#define schemaFLen(s) ((s)->flen)
#define schemaColAt(s, i) ((s)->columns + i)

STSchema *tdNewSchema(int32_t nCols);
#define   tdFreeSchema(s) tfree((s))
int       tdSchemaAddCol(STSchema *pSchema, int8_t type, int16_t colId, int32_t bytes);
STSchema *tdDupSchema(STSchema *pSchema);
int       tdGetSchemaEncodeSize(STSchema *pSchema);
void *    tdEncodeSchema(void *dst, STSchema *pSchema);
STSchema *tdDecodeSchema(void **psrc);

// ----------------- Data row structure

/* A data row, the format is like below:
 * |<------------------------------------- len ---------------------------------->|
 * |<--Head ->|<---------   flen -------------->|                                 |
 * +----------+---------------------------------+---------------------------------+
 * | int32_t  |                                 |                                 |
 * +----------+---------------------------------+---------------------------------+
 * |   len    |           First part            |             Second part         |
 * +----------+---------------------------------+---------------------------------+
 */
typedef void *SDataRow;

#define TD_DATA_ROW_HEAD_SIZE sizeof(int32_t)

#define dataRowLen(r) (*(int32_t *)(r))
#define dataRowTuple(r) POINTER_SHIFT(r, TD_DATA_ROW_HEAD_SIZE)
#define dataRowKey(r) (*(TSKEY *)(dataRowTuple(r)))
#define dataRowSetLen(r, l) (dataRowLen(r) = (l))
#define dataRowCpy(dst, r) memcpy((dst), (r), dataRowLen(r))
#define dataRowMaxBytesFromSchema(s) (schemaTLen(s) + TD_DATA_ROW_HEAD_SIZE)

SDataRow tdNewDataRowFromSchema(STSchema *pSchema);
void     tdFreeDataRow(SDataRow row);
void     tdInitDataRow(SDataRow row, STSchema *pSchema);
SDataRow tdDataRowDup(SDataRow row);

static FORCE_INLINE int tdAppendColVal(SDataRow row, void *value, int8_t type, int32_t bytes, int32_t offset) {
  ASSERT(value != NULL);
  int32_t toffset = offset + TD_DATA_ROW_HEAD_SIZE;
  char *  ptr = (char *)POINTER_SHIFT(row, dataRowLen(row));

  switch (type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      *(VarDataOffsetT *)POINTER_SHIFT(row, toffset) = dataRowLen(row);
      memcpy(ptr, value, varDataTLen(value));
      dataRowLen(row) += varDataTLen(value);
      break;
    default:
      memcpy(POINTER_SHIFT(row, toffset), value, TYPE_BYTES[type]);
      break;
  }

  return 0;
}

// NOTE: offset here including the header size
static FORCE_INLINE void *tdGetRowDataOfCol(SDataRow row, int8_t type, int32_t offset) {
  switch (type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      return POINTER_SHIFT(row, *(VarDataOffsetT *)POINTER_SHIFT(row, offset));
    default:
      return POINTER_SHIFT(row, offset);
  }
}

// ----------------- Data column structure
typedef struct SDataCol {
  int8_t          type;       // column type
  int16_t         colId;      // column ID
  int             bytes;      // column data bytes defined
  int             offset;     // data offset in a SDataRow (including the header size)
  int             spaceSize;  // Total space size for this column
  int             len;        // column data length
  VarDataOffsetT *dataOff;    // For binary and nchar data, the offset in the data column
  void *          pData;      // Actual data pointer
} SDataCol;

static FORCE_INLINE void dataColReset(SDataCol *pDataCol) { pDataCol->len = 0; }

void dataColInit(SDataCol *pDataCol, STColumn *pCol, void **pBuf, int maxPoints);
void dataColAppendVal(SDataCol *pCol, void *value, int numOfRows, int maxPoints);
void dataColPopPoints(SDataCol *pCol, int pointsToPop, int numOfRows);
void dataColSetOffset(SDataCol *pCol, int nEle);

bool isNEleNull(SDataCol *pCol, int nEle);
void dataColSetNEleNull(SDataCol *pCol, int nEle, int maxPoints);

// Get the data pointer from a column-wised data
static FORCE_INLINE void *tdGetColDataOfRow(SDataCol *pCol, int row) {
  switch (pCol->type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      return POINTER_SHIFT(pCol->pData, pCol->dataOff[row]);
      break;

    default:
      return POINTER_SHIFT(pCol->pData, TYPE_BYTES[pCol->type] * row);
      break;
  }
}

static FORCE_INLINE int32_t dataColGetNEleLen(SDataCol *pDataCol, int rows) {
  ASSERT(rows > 0);

  switch (pDataCol->type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      return pDataCol->dataOff[rows - 1] + varDataTLen(tdGetColDataOfRow(pDataCol, rows - 1));
      break;
    default:
      return TYPE_BYTES[pDataCol->type] * rows;
  }
}


typedef struct {
  int      maxRowSize;
  int      maxCols;    // max number of columns
  int      maxPoints;  // max number of points
  int      bufSize;

  int      numOfRows;
  int      numOfCols;  // Total number of cols
  int      sversion;   // TODO: set sversion
  void *   buf;
  SDataCol cols[];
} SDataCols;

#define keyCol(pCols) (&((pCols)->cols[0]))  // Key column
#define dataColsKeyAt(pCols, idx) ((TSKEY *)(keyCol(pCols)->pData))[(idx)]
#define dataColsKeyFirst(pCols) dataColsKeyAt(pCols, 0)
#define dataColsKeyLast(pCols) ((pCols->numOfRows == 0) ? 0 : dataColsKeyAt(pCols, (pCols)->numOfRows - 1))

SDataCols *tdNewDataCols(int maxRowSize, int maxCols, int maxRows);
void       tdResetDataCols(SDataCols *pCols);
void       tdInitDataCols(SDataCols *pCols, STSchema *pSchema);
SDataCols *tdDupDataCols(SDataCols *pCols, bool keepData);
void       tdFreeDataCols(SDataCols *pCols);
void       tdAppendDataRowToDataCol(SDataRow row, SDataCols *pCols);
void       tdPopDataColsPoints(SDataCols *pCols, int pointsToPop); //!!!!
int        tdMergeDataCols(SDataCols *target, SDataCols *src, int rowsToMerge);
void       tdMergeTwoDataCols(SDataCols *target, SDataCols *src1, int *iter1, SDataCols *src2, int *iter2, int tRows);


// ----------------- Tag row structure

/* A tag row, the format is like below:
+----------+----------------------------------------------------------------+
| STagRow  | STagCol | STagCol | STagCol | STagCol | ...| STagCol | STagCol | 
+----------+----------------------------------------------------------------+

pData
+----------+----------------------------------------------------------------+
| value 1     | value 2 |  value 3     | value 4       | ....|value n       |
+----------+----------------------------------------------------------------+

 */


#define TD_TAG_ROW_HEAD_SIZE sizeof(int16_t)

#define tagRowNum(r) (*(int16_t *)(r))
#define tagRowArray(r) POINTER_SHIFT(r, TD_TAG_ROW_HEAD_SIZE)
//#define dataRowKey(r) (*(TSKEY *)(dataRowTuple(r)))
//#define dataRowSetLen(r, l) (dataRowLen(r) = (l))
//#define dataRowCpy(dst, r) memcpy((dst), (r), dataRowLen(r))
//#define dataRowMaxBytesFromSchema(s) (schemaTLen(s) + TD_DATA_ROW_HEAD_SIZE)

typedef struct {
  int16_t colId;   // column ID
  int16_t colType;
  uint16_t offset;  //to store value for numeric col or offset for binary/Nchar
} STagCol;

typedef struct {
  int32_t    len;    
  void *     pData;  // Space to store the tag value   
  uint16_t   dataLen;
  int16_t    ncols;  // Total columns allocated
  STagCol    tagCols[];
} STagRow;


#define tagColSize(r) (sizeof(STagCol) + r.colLen)

int tdSetTagCol(SDataRow row, void *value, int16_t len, int8_t type, int16_t colId);  //insert tag value and update all the information
int tdDeleteTagCol(SDataRow row, int16_t colId);  // delete tag value and update all the information
void * tdQueryTagByID(SDataRow row, int16_t colId, int16_t *type);   //if find tag, 0, else return -1;
int tdAppendTagColVal(SDataRow row, void *value, int8_t type, int32_t bytes, int16_t colId);  
SDataRow tdTagRowDup(SDataRow row);
void tdFreeTagRow(SDataRow row); 
SDataRow tdTagRowDecode(SDataRow row);
int tdTagRowCpy(SDataRow dst, SDataRow src);
void * tdNewTagRowFromSchema(STSchema *pSchema, int16_t numofTags);
STSchema *tdGetSchemaFromData(SDataRow *row);

#ifdef __cplusplus
}
#endif

#endif  // _TD_DATA_FORMAT_H_
