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

typedef struct SBuffer  SBuffer;
typedef struct SSchema  SSchema;
typedef struct STColumn STColumn;
typedef struct STSchema STSchema;
typedef struct SValue   SValue;
typedef struct SColVal  SColVal;
typedef struct SRow     SRow;
typedef struct SRowIter SRowIter;
typedef struct STagVal  STagVal;
typedef struct STag     STag;
typedef struct SColData SColData;

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
int32_t tBufferReserve(SBuffer *pBuffer, int64_t nData, void **ppData);

// STSchema ================================
void tDestroyTSchema(STSchema *pTSchema);

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

// SRow ================================
int32_t tRowBuild(SArray *aColVal, STSchema *pTSchema, SBuffer *pBuffer);
void    tRowGet(SRow *pRow, STSchema *pTSchema, int32_t iCol, SColVal *pColVal);

// SRowIter ================================
int32_t  tRowIterOpen(SRow *pRow, STSchema *pTSchema, SRowIter **ppIter);
void     tRowIterClose(SRowIter **ppIter);
SColVal *tRowIterNext(SRowIter *pIter);

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
uint8_t tColDataGetBitValue(const SColData *pColData, int32_t iVal);
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
  int32_t  tlen;
  STColumn columns[];
};

struct SRow {
  uint8_t  flag;
  uint8_t  rsv;
  uint16_t sver;
  uint32_t len;
  TSKEY    ts;
  uint8_t  data[];
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
  int32_t      tlen;
  STColumn    *columns;
} STSchemaBuilder;

int32_t   tdInitTSchemaBuilder(STSchemaBuilder *pBuilder, schema_ver_t version);
void      tdDestroyTSchemaBuilder(STSchemaBuilder *pBuilder);
void      tdResetTSchemaBuilder(STSchemaBuilder *pBuilder, schema_ver_t version);
int32_t   tdAddColToSchema(STSchemaBuilder *pBuilder, int8_t type, int8_t flags, col_id_t colId, col_bytes_t bytes);
STSchema *tdGetSchemaFromBuilder(STSchemaBuilder *pBuilder);

STSchema *tBuildTSchema(SSchema *aSchema, int32_t numOfCols, int32_t version);

#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_DATA_FORMAT_H_*/
