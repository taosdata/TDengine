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
#include "tbuffer.h"
#include "tencode.h"
#include "tsimplehash.h"
#include "ttypes.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SSchema    SSchema;
typedef struct SSchema2   SSchema2;
typedef struct SSchemaExt SSchemaExt;
typedef struct STColumn   STColumn;
typedef struct STSchema   STSchema;
typedef struct SValue     SValue;
typedef struct SColVal    SColVal;
typedef struct SRow       SRow;
typedef struct SRowIter   SRowIter;
typedef struct STagVal    STagVal;
typedef struct STag       STag;
typedef struct SColData   SColData;
typedef struct SBlobSet   SBlobSet;

typedef struct SRowKey           SRowKey;
typedef struct SValueColumn      SValueColumn;
typedef struct SRowBuildScanInfo SRowBuildScanInfo;

#define ROW_BUILD_NONE   ((uint8_t)0x1)
#define ROW_BUILD_UPDATE ((uint8_t)0x2)
#define ROW_BUILD_MERGE  ((uint8_t)0x4)

typedef struct SBlobValOffset SBlobValOffset;
struct SColumnDataAgg;
typedef struct SColumnDataAgg *SColumnDataAggPtr;

#define HAS_NONE  ((uint8_t)0x1)
#define HAS_NULL  ((uint8_t)0x2)
#define HAS_VALUE ((uint8_t)0x4)
#define HAS_BLOB  ((uint8_t)0x8)

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

// SColVal ================================
#define CV_FLAG_VALUE ((int8_t)0x0)
#define CV_FLAG_NONE  ((int8_t)0x1)
#define CV_FLAG_NULL  ((int8_t)0x2)

#define COL_VAL_NONE(CID, TYPE) ((SColVal){.cid = (CID), .flag = CV_FLAG_NONE, .value = {.type = (TYPE)}})
#define COL_VAL_NULL(CID, TYPE) ((SColVal){.cid = (CID), .flag = CV_FLAG_NULL, .value = {.type = (TYPE)}})
#define COL_VAL_VALUE(CID, V)   ((SColVal){.cid = (CID), .flag = CV_FLAG_VALUE, .value = (V)})

#define COL_VAL_IS_NONE(CV)  ((CV)->flag == CV_FLAG_NONE)
#define COL_VAL_IS_NULL(CV)  ((CV)->flag == CV_FLAG_NULL)
#define COL_VAL_IS_VALUE(CV) ((CV)->flag == CV_FLAG_VALUE)

// Strategies of merging rows with
// same pk in single insert batch.
typedef enum {
  PREFER_NON_NULL = 0,  // choose latest non-null value for each column
  KEEP_CONSISTENCY = 1  // choose latest row
} ERowMergeStrategy;
#define BSE_SEQUECE_SIZE sizeof(uint64_t)

enum { TSDB_DATA_BLOB_VALUE = 0x1, TSDB_DATA_BLOB_EMPTY_VALUE = 0x2, TSDB_DATA_BLOB_NULL_VALUE = 0x4 };

#define tRowGetKey(_pRow, _pKey)                       \
  do {                                                 \
    (_pKey)->ts = taosGetInt64Aligned(&((_pRow)->ts)); \
    (_pKey)->numOfPKs = 0;                             \
    if ((_pRow)->numOfPKs > 0) {                       \
      tRowGetPrimaryKey((_pRow), (_pKey));             \
    }                                                  \
  } while (0)

// SValueColumn ================================
typedef struct {
  int8_t  cmprAlg;  // filled by caller
  int8_t  type;
  int32_t dataOriginalSize;
  int32_t dataCompressedSize;
  int32_t offsetOriginalSize;
  int32_t offsetCompressedSize;
} SValueColumnCompressInfo;

int32_t tValueColumnInit(SValueColumn *valCol);
void    tValueColumnDestroy(SValueColumn *valCol);
void    tValueColumnClear(SValueColumn *valCol);
int32_t tValueColumnAppend(SValueColumn *valCol, const SValue *value);
int32_t tValueColumnUpdate(SValueColumn *valCol, int32_t idx, const SValue *value);
int32_t tValueColumnGet(SValueColumn *valCol, int32_t idx, SValue *value);
int32_t tValueColumnCompress(SValueColumn *valCol, SValueColumnCompressInfo *info, SBuffer *output, SBuffer *assist);
int32_t tValueColumnDecompress(void *input, const SValueColumnCompressInfo *compressInfo, SValueColumn *valCol,
                               SBuffer *buffer);
int32_t tValueColumnCompressInfoEncode(const SValueColumnCompressInfo *compressInfo, SBuffer *buffer);
int32_t tValueColumnCompressInfoDecode(SBufferReader *reader, SValueColumnCompressInfo *compressInfo);
int32_t tValueCompare(const SValue *tv1, const SValue *tv2);

// SRow ================================
int32_t tRowBuild(SArray *aColVal, const STSchema *pTSchema, SRow **ppRow, SRowBuildScanInfo *pScanInfo);
int32_t tRowBuildWithBlob(SArray *aColVal, const STSchema *pTSchema, SRow **ppRow, SBlobSet *pBlobSet,
                          SRowBuildScanInfo *sinfo);
int32_t tRowGet(SRow *pRow, STSchema *pTSchema, int32_t iCol, SColVal *pColVal);

typedef struct {
  uint64_t offset;
  uint32_t len;
  uint32_t dataOffset;
  int8_t   nextRow;
  int8_t   type;
} SBlobValue;

typedef struct {
  uint64_t seq;
  uint32_t seqOffsetInRow;
  void    *data;
  int32_t  len;
  int8_t   type;
} SBlobItem;
int32_t tBlobSetCreate(int64_t cap, int8_t type, SBlobSet **ppBlobSet);
int32_t tBlobSetPush(SBlobSet *pBlobSet, SBlobItem *pBlobItem, uint64_t *seq, int8_t nextRow);
int32_t tBlobSetUpdate(SBlobSet *pBlobSet, uint64_t seq, SBlobItem *pBlobItem);
int32_t tBlobSetGet(SBlobSet *pBlobSet, uint64_t seq, SBlobItem *pItem);
void    tBlobSetDestroy(SBlobSet *pBlowRow);
int32_t tBlobSetSize(SBlobSet *pBlobSet);
void    tBlobSetSwap(SBlobSet *p1, SBlobSet *p2);
// int32_t tBlobRowEnd(SBlobSet *pBlobSet);
//  int32_t tBlobSetRebuild(SBlobSet *pBlobSet, int32_t srow, int32_t nrow, SBlobSet **pNew);

int32_t tRowGetBlobSeq(SRow *pRow, STSchema *pTSchema, int32_t iCol, SColVal *pColVal, uint64_t *seq);
void    tRowDestroy(SRow *pRow);
int32_t tRowSort(SArray *aRowP);
int32_t tRowMerge(SArray *aRowP, STSchema *pTSchema, ERowMergeStrategy strategy);
int32_t tRowUpsertColData(SRow *pRow, STSchema *pTSchema, SColData *aColData, int32_t nColData, int32_t flag);
void    tRowGetPrimaryKey(SRow *pRow, SRowKey *key);
int32_t tRowKeyCompare(const SRowKey *key1, const SRowKey *key2);
void    tRowKeyAssign(SRowKey *pDst, SRowKey *pSrc);
int32_t tRowSortWithBlob(SArray *aRowP, STSchema *pTSchema, SBlobSet *pBlobSet);
int32_t tRowMergeWithBlob(SArray *pRow, STSchema *pTSchema, SBlobSet *pBlobSet, int8_t flag);

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
void    debugPrintSTag(STag *pTag, const char *tag, int32_t ln);  // TODO: remove
int32_t parseJsontoTagData(const char *json, SArray *pTagVals, STag **ppTag, void *pMsgBuf, void *charsetCxt);

// SColData ================================
typedef struct {
  uint32_t cmprAlg;  // filled by caller
  int8_t   columnFlag;
  int8_t   flag;
  int8_t   dataType;
  int16_t  columnId;
  int32_t  numOfData;
  int32_t  bitmapOriginalSize;
  int32_t  bitmapCompressedSize;
  int32_t  offsetOriginalSize;
  int32_t  offsetCompressedSize;
  int32_t  dataOriginalSize;
  int32_t  dataCompressedSize;
} SColDataCompressInfo;

typedef void *(*xMallocFn)(void *, int32_t);
typedef int32_t (*checkWKBGeometryFn)(const unsigned char *geoWKB, size_t nGeom);
typedef int32_t (*initGeosFn)();

void    tColDataDestroy(void *ph);
void    tColDataInit(SColData *pColData, int16_t cid, int8_t type, int8_t cflag);
void    tColDataClear(SColData *pColData);
void    tColDataDeepClear(SColData *pColData);
int32_t tColDataAppendValue(SColData *pColData, SColVal *pColVal);
int32_t tColDataUpdateValue(SColData *pColData, SColVal *pColVal, bool forward);
int32_t tColDataGetValue(SColData *pColData, int32_t iVal, SColVal *pColVal);
uint8_t tColDataGetBitValue(const SColData *pColData, int32_t iVal);
int32_t tColDataCopy(SColData *pColDataFrom, SColData *pColData, xMallocFn xMalloc, void *arg);
void    tColDataArrGetRowKey(SColData *aColData, int32_t nColData, int32_t iRow, SRowKey *key);

extern void (*tColDataCalcSMA[])(SColData *pColData, SColumnDataAggPtr pAggs);

int32_t tColDataCompress(SColData *colData, SColDataCompressInfo *info, SBuffer *output, SBuffer *assist);
int32_t tColDataDecompress(void *input, SColDataCompressInfo *info, SColData *colData, SBuffer *assist);

// for stmt bind
int32_t tColDataAddValueByBind(SColData *pColData, TAOS_MULTI_BIND *pBind, int32_t buffMaxLen, initGeosFn igeos,
                               checkWKBGeometryFn cgeos);
int32_t tColDataSortMerge(SArray **arr);
int32_t tColDataSortMergeWithBlob(SArray **arr, SBlobSet *pBlob);

// for raw block
int32_t tColDataAddValueByDataBlock(SColData *pColData, int8_t type, int32_t bytes, int32_t nRows, char *lengthOrbitmap,
                                    char *data);
// for encode/decode
int32_t tEncodeColData(uint8_t version, SEncoder *pEncoder, SColData *pColData);
int32_t tDecodeColData(uint8_t version, SDecoder *pDecoder, SColData *pColData);
int32_t tEncodeRow(SEncoder *pEncoder, SRow *pRow);
int32_t tDecodeRow(SDecoder *pDecoder, SRow **ppRow);

int32_t tEncodeBlobSet(SEncoder *pEncoder, SBlobSet *pRow);
int32_t tDecodeBlobSet(SDecoder *pDecoder, SBlobSet **pBlobSet);

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

/*
 * 1. Tuple format:
 *      SRow + [(type, offset) * numOfPKs +] [bit map +] fix-length data + [var-length data]
 *
 * 2. K-V format:
 *      SRow + [(type, offset) * numOfPKs +] offset array + ([-]cid [+ data]) * numColsNotNone
 */
struct SRow {
  uint8_t  flag;
  uint8_t  numOfPKs;
  uint16_t sver;
  uint32_t len;
  TSKEY    ts;
  uint8_t  data[];
};

struct SBlobSet {
  int8_t    type;
  int8_t    rowType;
  SHashObj *pSeqToffset;
  int64_t   seq;
  int64_t   len;
  int32_t   cap;
  uint8_t   compress;
  SArray   *pSeqTable;

  SArray  *pSet;
  uint8_t *data;
};

typedef struct {
  int8_t   type;
  uint32_t offset;
} SPrimaryKeyIndex;

#define DATUM_MAX_SIZE 16

struct SValue {
  int8_t type;
  union {
    int64_t val;
    struct {
      uint8_t *pData;
      uint32_t nData;
    };
  };
};

struct SBlobValOffset {
  uint64_t seq;
  uint32_t offset;
  uint32_t rowNum;
};
#define VALUE_GET_DATUM(pVal, type) \
  (IS_VAR_DATA_TYPE(type) || type == TSDB_DATA_TYPE_DECIMAL) ? (pVal)->pData : (void *)&(pVal)->val

#define VALUE_GET_TRIVIAL_DATUM(pVal)    ((pVal)->val)
#define VALUE_SET_TRIVIAL_DATUM(pVal, v) (pVal)->val = v

void valueSetDatum(SValue *pVal, int8_t type, void *pDatum, uint32_t len);
void valueCloneDatum(SValue *pDst, const SValue *pSrc, int8_t type);
void valueClearDatum(SValue *pVal, int8_t type);

#define TD_MAX_PK_COLS 2
struct SRowKey {
  TSKEY   ts;
  uint8_t numOfPKs;
  SValue  pks[TD_MAX_PK_COLS];
};

struct SColVal {
  int16_t cid;
  int8_t  flag;
  SValue  value;
};

struct SColData {
  int16_t  cid;
  int8_t   type;
  int8_t   cflag;
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
    (void)memcpy(varDataVal(x), (str), __len);    \
  } while (0);

#define STR_WITH_MAXSIZE_TO_VARSTR(x, str, _maxs)                         \
  do {                                                                    \
    char *_e = stpncpy(varDataVal(x), (str), (_maxs)-VARSTR_HEADER_SIZE); \
    varDataSetLen(x, (_e - (x)-VARSTR_HEADER_SIZE));                      \
  } while (0)

#define STR_WITH_SIZE_TO_VARSTR(x, str, _size)   \
  do {                                           \
    *(VarDataLenT *)(x) = (VarDataLenT)(_size);  \
    (void)memcpy(varDataVal(x), (str), (_size)); \
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
const STColumn *tTSchemaSearchColumn(const STSchema *pTSchema, int16_t cid);

struct SValueColumn {
  int8_t   type;
  uint32_t numOfValues;
  SBuffer  data;
  SBuffer  offsets;
};

typedef struct {
  int32_t  dataType;      // filled by caller
  uint32_t cmprAlg;       // filled by caller
  int32_t  originalSize;  // filled by caller
  int32_t  compressedSize;
} SCompressInfo;

int32_t tCompressData(void          *input,       // input
                      SCompressInfo *info,        // compress info
                      void          *output,      // output
                      int32_t        outputSize,  // output size
                      SBuffer       *buffer       // assistant buffer provided by caller, can be NULL
);
int32_t tDecompressData(void                *input,       // input
                        const SCompressInfo *info,        // compress info
                        void                *output,      // output
                        int32_t              outputSize,  // output size
                        SBuffer             *buffer       // assistant buffer provided by caller, can be NULL
);
int32_t tCompressDataToBuffer(void *input, SCompressInfo *info, SBuffer *output, SBuffer *assist);
int32_t tDecompressDataToBuffer(void *input, SCompressInfo *info, SBuffer *output, SBuffer *assist);

typedef struct {
  int32_t          columnId;
  int32_t          type;
  TAOS_MULTI_BIND *bind;
} SBindInfo;
int32_t tRowBuildFromBind(SBindInfo *infos, int32_t numOfInfos, bool infoSorted, const STSchema *pTSchema,
                          SArray *rowArray, bool *pOrdered, bool *pDupTs);

// stmt2 binding
int32_t tColDataAddValueByBind2(SColData *pColData, TAOS_STMT2_BIND *pBind, int32_t buffMaxLen);

int32_t tColDataAddValueByBind2WithGeos(SColData *pColData, TAOS_STMT2_BIND *pBind, int32_t buffMaxLen,
                                        initGeosFn igeos, checkWKBGeometryFn cgeos);

int32_t tColDataAddValueByBind2WithBlob(SColData *pColData, TAOS_STMT2_BIND *pBind, int32_t buffMaxLen,
                                        SBlobSet *pBlobSet);

int32_t tColDataAddValueByBind2WithDecimal(SColData *pColData, TAOS_STMT2_BIND *pBind, int32_t buffMaxLen,
                                           uint8_t precision, uint8_t scale);

typedef struct {
  int32_t          columnId;
  int32_t          type;
  int32_t          bytes;
  TAOS_STMT2_BIND *bind;

} SBindInfo2;

int32_t tRowBuildFromBind2(SBindInfo2 *infos, int32_t numOfInfos, SSHashObj *parsedCols, bool infoSorted,
                           const STSchema *pTSchema, const SSchemaExt *pSchemaExt, SArray *rowArray, bool *pOrdered,
                           bool *pDupTs);

int32_t tRowBuildFromBind2WithBlob(SBindInfo2 *infos, int32_t numOfInfos, bool infoSorted, const STSchema *pTSchema,
                                   SArray *rowArray, bool *pOrdered, bool *pDupTs, SBlobSet *pBlobSet);

struct SRowBuildScanInfo {
  int32_t numOfNone;
  int32_t numOfNull;
  int32_t numOfValue;
  int32_t numOfPKs;
  int8_t  flag;

  // tuple
  int8_t           tupleFlag;
  SPrimaryKeyIndex tupleIndices[TD_MAX_PK_COLS];
  int32_t          tuplePKSize;      // primary key size
  int32_t          tupleBitmapSize;  // bitmap size
  int32_t          tupleFixedSize;   // fixed part size
  int32_t          tupleVarSize;     // var part size
  int32_t          tupleRowSize;

  // key-value
  int8_t           kvFlag;
  SPrimaryKeyIndex kvIndices[TD_MAX_PK_COLS];
  int32_t          kvMaxOffset;
  int32_t          kvPKSize;       // primary key size
  int32_t          kvIndexSize;    // offset array size
  int32_t          kvPayloadSize;  // payload size
  int32_t          kvRowSize;

  int8_t hasBlob;
  int8_t scanType;
};

int8_t schemaHasBlob(const STSchema *pSchema);
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_DATA_FORMAT_H_*/
