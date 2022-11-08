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

#ifndef _TD_COMMON_DATA_FORMAT_H_
#define _TD_COMMON_DATA_FORMAT_H_

#include "os.h"
#include "talgo.h"
#include "tarray.h"
#include "tencode.h"
#include "ttypes.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SBuffer       SBuffer;
typedef struct SSchema       SSchema;
typedef struct STColumn      STColumn;
typedef struct STSchema      STSchema;
typedef struct SValue        SValue;
typedef struct SColVal       SColVal;
typedef struct STSRow2       STSRow2;
typedef struct STSRowBuilder STSRowBuilder;
typedef struct STagVal       STagVal;
typedef struct STag          STag;
typedef struct SColData      SColData;

#define HAS_NONE  ((uint8_t)0x1)
#define HAS_NULL  ((uint8_t)0x2)
#define HAS_VALUE ((uint8_t)0x4)

// bitmap ================================
const static uint8_t BIT2_MAP[4][4] = {{0b00000000, 0b00000001, 0b00000010, 0},
                                       {0b00000000, 0b00000100, 0b00001000, 2},
                                       {0b00000000, 0b00010000, 0b00100000, 4},
                                       {0b00000000, 0b01000000, 0b10000000, 6}};

#define N1(n)             ((((uint8_t)1) << (n)) - 1)
#define BIT1_SIZE(n)      ((((n)-1) >> 3) + 1)
#define BIT2_SIZE(n)      ((((n)-1) >> 2) + 1)
#define SET_BIT1(p, i, v) ((p)[(i) >> 3] = (p)[(i) >> 3] & N1((i)&7) | (((uint8_t)(v)) << ((i)&7)))
#define GET_BIT1(p, i)    (((p)[(i) >> 3] >> ((i)&7)) & ((uint8_t)1))
#define SET_BIT2(p, i, v) ((p)[(i) >> 2] = (p)[(i) >> 2] & N1(BIT2_MAP[(i)&3][3]) | BIT2_MAP[(i)&3][(v)])
#define GET_BIT2(p, i)    (((p)[(i) >> 2] >> BIT2_MAP[(i)&3][3]) & ((uint8_t)3))

// SBuffer ================================
struct SBuffer {
  int64_t  nBuf;
  uint8_t *pBuf;
};

#define tBufferCreate() \
  (SBuffer) { .nBuf = 0, .pBuf = NULL }
void    tBufferDestroy(SBuffer *pBuffer);
int32_t tBufferInit(SBuffer *pBuffer, int64_t size);
int32_t tBufferPut(SBuffer *pBuffer, const void *pData, int64_t nData);

// STSchema ================================
int32_t tTSchemaCreate(int32_t sver, SSchema *pSchema, int32_t nCols, STSchema **ppTSchema);
void    tTSchemaDestroy(STSchema *pTSchema);

// SValue ================================
int32_t tPutValue(uint8_t *p, SValue *pValue, int8_t type);
int32_t tGetValue(uint8_t *p, SValue *pValue, int8_t type);
int     tValueCmprFn(const SValue *pValue1, const SValue *pValue2, int8_t type);

// SColVal ================================
#define CV_FLAG_VALUE ((int8_t)0x0)
#define CV_FLAG_NONE  ((int8_t)0x1)
#define CV_FLAG_NULL  ((int8_t)0x2)

#define COL_VAL_NONE(CID, TYPE)     ((SColVal){.cid = (CID), .type = (TYPE), .flag = CV_FLAG_NONE})
#define COL_VAL_NULL(CID, TYPE)     ((SColVal){.cid = (CID), .type = (TYPE), .flag = CV_FLAG_NULL})
#define COL_VAL_VALUE(CID, TYPE, V) ((SColVal){.cid = (CID), .type = (TYPE), .value = (V)})

#define COL_VAL_IS_NONE(CV)  ((CV)->flag == CV_FLAG_NONE)
#define COL_VAL_IS_NULL(CV)  ((CV)->flag == CV_FLAG_NULL)
#define COL_VAL_IS_VALUE(CV) ((CV)->flag == CV_FLAG_VALUE)

// STSRow2 ================================
#define TSROW_LEN(PROW, V)  tGetI32v((uint8_t *)(PROW)->data, (V) ? &(V) : NULL)
#define TSROW_SVER(PROW, V) tGetI32v((PROW)->data + TSROW_LEN(PROW, NULL), (V) ? &(V) : NULL)

int32_t tTSRowNew(STSRowBuilder *pBuilder, SArray *pArray, STSchema *pTSchema, STSRow2 **ppRow);
int32_t tTSRowClone(const STSRow2 *pRow, STSRow2 **ppRow);
void    tTSRowFree(STSRow2 *pRow);
void    tTSRowGet(STSRow2 *pRow, STSchema *pTSchema, int32_t iCol, SColVal *pColVal);
int32_t tTSRowToArray(STSRow2 *pRow, STSchema *pTSchema, SArray **ppArray);
int32_t tPutTSRow(uint8_t *p, STSRow2 *pRow);
int32_t tGetTSRow(uint8_t *p, STSRow2 **ppRow);

// STSRowBuilder ================================
#define tsRowBuilderInit() ((STSRowBuilder){0})
#define tsRowBuilderClear(B)     \
  do {                           \
    if ((B)->pBuf) {             \
      taosMemoryFree((B)->pBuf); \
    }                            \
  } while (0)

// STag ================================
int32_t tTagNew(SArray *pArray, int32_t version, int8_t isJson, STag **ppTag);
void    tTagFree(STag *pTag);
bool    tTagIsJson(const void *pTag);
bool    tTagIsJsonNull(void *tagVal);
bool    tTagGet(const STag *pTag, STagVal *pTagVal);
char   *tTagValToData(const STagVal *pTagVal, bool isJson);
int32_t tEncodeTag(SEncoder *pEncoder, const STag *pTag);
int32_t tDecodeTag(SDecoder *pDecoder, STag **ppTag);
int32_t tTagToValArray(const STag *pTag, SArray **ppArray);
void    tTagSetCid(const STag *pTag, int16_t iTag, int16_t cid);
void    debugPrintSTag(STag *pTag, const char *tag, int32_t ln);  // TODO: remove
int32_t parseJsontoTagData(const char *json, SArray *pTagVals, STag **ppTag, void *pMsgBuf);

// SColData ================================
void    tColDataDestroy(void *ph);
void    tColDataInit(SColData *pColData, int16_t cid, int8_t type, int8_t smaOn);
void    tColDataClear(SColData *pColData);
int32_t tColDataAppendValue(SColData *pColData, SColVal *pColVal);
void    tColDataGetValue(SColData *pColData, int32_t iVal, SColVal *pColVal);
uint8_t tColDataGetBitValue(SColData *pColData, int32_t iVal);
int32_t tColDataCopy(SColData *pColDataSrc, SColData *pColDataDest);
extern void (*tColDataCalcSMA[])(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min, int16_t *numOfNull);

// STRUCT ================================
struct STColumn {
  col_id_t colId;
  int8_t   type;
  int8_t   flags;
  int32_t  bytes;
  int32_t  offset;
};

struct STSchema {
  int32_t  numOfCols;
  int32_t  version;
  int32_t  flen;
  int32_t  vlen;
  int32_t  tlen;
  STColumn columns[];
};

#define TSROW_HAS_NONE ((uint8_t)0x1)
#define TSROW_HAS_NULL ((uint8_t)0x2U)
#define TSROW_HAS_VAL  ((uint8_t)0x4U)
#define TSROW_KV_SMALL ((uint8_t)0x10U)
#define TSROW_KV_MID   ((uint8_t)0x20U)
#define TSROW_KV_BIG   ((uint8_t)0x40U)
#pragma pack(push, 1)
struct STSRow2 {
  TSKEY   ts;
  uint8_t flags;
  uint8_t data[];
};
#pragma pack(pop)

struct STSRowBuilder {
  // STSRow2  tsRow;
  int32_t  szBuf;
  uint8_t *pBuf;
};

struct SValue {
  union {
    int64_t val;
    struct {
      uint32_t nData;
      uint8_t *pData;
    };
  };
};

struct SColVal {
  int16_t cid;
  int8_t  type;
  int8_t  flag;
  SValue  value;
};

struct SColData {
  int16_t  cid;
  int8_t   type;
  int8_t   smaOn;
  int32_t  nVal;
  uint8_t  flag;
  uint8_t *pBitMap;
  int32_t *aOffset;
  int32_t  nData;
  uint8_t *pData;
};

#pragma pack(push, 1)
struct STagVal {
  //  char colName[TSDB_COL_NAME_LEN]; // only used for tmq_get_meta
  union {
    int16_t cid;
    char   *pKey;
  };
  int8_t type;
  union {
    int64_t i64;
    struct {
      uint32_t nData;
      uint8_t *pData;
    };
  };
};

#define TD_TAG_JSON  ((int8_t)0x40)  // distinguish JSON string and JSON value with the highest bit
#define TD_TAG_LARGE ((int8_t)0x20)
struct STag {
  int8_t  flags;
  int16_t len;
  int16_t nTag;
  int32_t ver;
  int8_t  idx[];
};
#pragma pack(pop)

#if 1  //================================================================================================================================================
// Imported since 3.0 and use bitmap to demonstrate None/Null/Norm, while use Null/Norm below 3.0 without of bitmap.
#define TD_SUPPORT_BITMAP

#define STR_TO_VARSTR(x, str)                     \
  do {                                            \
    VarDataLenT __len = (VarDataLenT)strlen(str); \
    *(VarDataLenT *)(x) = __len;                  \
    memcpy(varDataVal(x), (str), __len);          \
  } while (0);

#define STR_WITH_MAXSIZE_TO_VARSTR(x, str, _maxs)                         \
  do {                                                                    \
    char *_e = stpncpy(varDataVal(x), (str), (_maxs)-VARSTR_HEADER_SIZE); \
    varDataSetLen(x, (_e - (x)-VARSTR_HEADER_SIZE));                      \
  } while (0)

#define STR_WITH_SIZE_TO_VARSTR(x, str, _size)  \
  do {                                          \
    *(VarDataLenT *)(x) = (VarDataLenT)(_size); \
    memcpy(varDataVal(x), (str), (_size));      \
  } while (0);

// ----------------- SCHEMA BUILDER DEFINITION
typedef struct {
  int32_t      tCols;
  int32_t      nCols;
  schema_ver_t version;
  uint16_t     flen;
  int32_t      vlen;
  int32_t      tlen;
  STColumn    *columns;
} STSchemaBuilder;

// use 2 bits for bitmap(default: STSRow/sub block)
#define TD_VTYPE_BITS        2
#define TD_VTYPE_PARTS       4  // PARTITIONS: 1 byte / 2 bits
#define TD_VTYPE_OPTR        3  // OPERATOR: 4 - 1, utilize to get remainder
#define TD_BITMAP_BYTES(cnt) (((cnt) + TD_VTYPE_OPTR) >> 2)

// use 1 bit for bitmap(super block)
#define TD_VTYPE_BITS_I        1
#define TD_VTYPE_PARTS_I       8  // PARTITIONS: 1 byte / 1 bit
#define TD_VTYPE_OPTR_I        7  // OPERATOR: 8 - 1, utilize to get remainder
#define TD_BITMAP_BYTES_I(cnt) (((cnt) + TD_VTYPE_OPTR_I) >> 3)

int32_t   tdInitTSchemaBuilder(STSchemaBuilder *pBuilder, schema_ver_t version);
void      tdDestroyTSchemaBuilder(STSchemaBuilder *pBuilder);
void      tdResetTSchemaBuilder(STSchemaBuilder *pBuilder, schema_ver_t version);
int32_t   tdAddColToSchema(STSchemaBuilder *pBuilder, int8_t type, int8_t flags, col_id_t colId, col_bytes_t bytes);
STSchema *tdGetSchemaFromBuilder(STSchemaBuilder *pBuilder);

#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_DATA_FORMAT_H_*/
