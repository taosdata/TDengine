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
typedef struct SSchema2 SSchema2;
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
const static uint8_t BIT1_MAP[8] = {0b11111110, 0b11111101, 0b11111011, 0b11110111,
                                    0b11101111, 0b11011111, 0b10111111, 0b01111111};

const static uint8_t BIT2_MAP[4] = {0b11111100, 0b11110011, 0b11001111, 0b00111111};

#define ONE               ((uint8_t)1)
#define THREE             ((uint8_t)3)
#define DIV_8(i)          ((i) >> 3)
#define MOD_8(i)          ((i)&7)
#define DIV_4(i)          ((i) >> 2)
#define MOD_4(i)          ((i)&3)
#define MOD_4_TIME_2(i)   (MOD_4(i) << 1)
#define BIT1_SIZE(n)      (DIV_8((n)-1) + 1)
#define BIT2_SIZE(n)      (DIV_4((n)-1) + 1)
#define SET_BIT1(p, i, v) ((p)[DIV_8(i)] = (p)[DIV_8(i)] & BIT1_MAP[MOD_8(i)] | ((v) << MOD_8(i)))
#define SET_BIT1_EX(p, i, v) \
  do {                       \
    if (MOD_8(i) == 0) {     \
      (p)[DIV_8(i)] = 0;     \
    }                        \
    SET_BIT1(p, i, v);       \
  } while (0)
#define GET_BIT1(p, i)    (((p)[DIV_8(i)] >> MOD_8(i)) & ONE)
#define SET_BIT2(p, i, v) ((p)[DIV_4(i)] = (p)[DIV_4(i)] & BIT2_MAP[MOD_4(i)] | ((v) << MOD_4_TIME_2(i)))
#define SET_BIT2_EX(p, i, v) \
  do {                       \
    if (MOD_4(i) == 0) {     \
      (p)[DIV_4(i)] = 0;     \
    }                        \
    SET_BIT2(p, i, v);       \
  } while (0)
#define GET_BIT2(p, i) (((p)[DIV_4(i)] >> MOD_4_TIME_2(i)) & THREE)

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
int32_t tRowBuild(SArray *aColVal, const STSchema *pTSchema, SRow **ppRow);
int32_t tRowGet(SRow *pRow, STSchema *pTSchema, int32_t iCol, SColVal *pColVal);
void    tRowDestroy(SRow *pRow);
int32_t tRowSort(SArray *aRowP);
int32_t tRowMerge(SArray *aRowP, STSchema *pTSchema, int8_t flag);
int32_t tRowUpsertColData(SRow *pRow, STSchema *pTSchema, SColData *aColData, int32_t nColData, int32_t flag);

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
typedef void *(*xMallocFn)(void *, int32_t);
void    tColDataDestroy(void *ph);
void    tColDataInit(SColData *pColData, int16_t cid, int8_t type, int8_t smaOn);
void    tColDataClear(SColData *pColData);
void    tColDataDeepClear(SColData *pColData);
int32_t tColDataAppendValue(SColData *pColData, SColVal *pColVal);
int32_t tColDataUpdateValue(SColData *pColData, SColVal *pColVal, bool forward);
void    tColDataGetValue(SColData *pColData, int32_t iVal, SColVal *pColVal);
uint8_t tColDataGetBitValue(const SColData *pColData, int32_t iVal);
int32_t tColDataCopy(SColData *pColDataFrom, SColData *pColData, xMallocFn xMalloc, void *arg);
extern void (*tColDataCalcSMA[])(SColData *pColData, int64_t *sum, int64_t *max, int64_t *min, int16_t *numOfNull);

// for stmt bind
int32_t tColDataAddValueByBind(SColData *pColData, TAOS_MULTI_BIND *pBind, int32_t buffMaxLen);
void    tColDataSortMerge(SArray *colDataArr);

// for raw block
int32_t tColDataAddValueByDataBlock(SColData *pColData, int8_t type, int32_t bytes, int32_t nRows, char *lengthOrbitmap,
                                    char *data);
// for encode/decode
int32_t tPutColData(uint8_t *pBuf, SColData *pColData);
int32_t tGetColData(uint8_t *pBuf, SColData *pColData);

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
  int32_t  numOfNone;   // # of none
  int32_t  numOfNull;   // # of null
  int32_t  numOfValue;  // # of vale
  int32_t  nVal;
  int8_t   flag;
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

// STSchema ================================
STSchema *tBuildTSchema(SSchema *aSchema, int32_t numOfCols, int32_t version);
#define tDestroyTSchema(pTSchema) \
  do {                            \
    if (pTSchema) {               \
      taosMemoryFree(pTSchema);   \
      pTSchema = NULL;            \
    }                             \
  } while (0)

#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_DATA_FORMAT_H_*/
