#ifndef TDENGINE_TTYPE_H
#define TDENGINE_TTYPE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "taosdef.h"

// ----------------- For variable data types such as TSDB_DATA_TYPE_BINARY and TSDB_DATA_TYPE_NCHAR
typedef int32_t VarDataOffsetT;
typedef int16_t VarDataLenT;

typedef struct tstr {
  VarDataLenT len;
  char        data[];
} tstr;

#define VARSTR_HEADER_SIZE  sizeof(VarDataLenT)

#define varDataLen(v)       ((VarDataLenT *)(v))[0]
#define varDataTLen(v)      (sizeof(VarDataLenT) + varDataLen(v))
#define varDataVal(v)       ((void *)((char *)v + VARSTR_HEADER_SIZE))
#define varDataCopy(dst, v) memcpy((dst), (void*) (v), varDataTLen(v))
#define varDataLenByData(v) (*(VarDataLenT *)(((char*)(v)) - VARSTR_HEADER_SIZE))
#define varDataSetLen(v, _len) (((VarDataLenT *)(v))[0] = (VarDataLenT) (_len))
#define IS_VAR_DATA_TYPE(t) (((t) == TSDB_DATA_TYPE_BINARY) || ((t) == TSDB_DATA_TYPE_NCHAR))

// this data type is internally used only in 'in' query to hold the values
#define TSDB_DATA_TYPE_ARRAY      (1000)

#define GET_TYPED_DATA(_v, _finalType, _type, _data) \
  do {                                               \
    switch (_type) {                                 \
      case TSDB_DATA_TYPE_BOOL:                      \
      case TSDB_DATA_TYPE_TINYINT:                   \
        (_v) = (_finalType)GET_INT8_VAL(_data);      \
        break;                                       \
      case TSDB_DATA_TYPE_UTINYINT:                  \
        (_v) = (_finalType)GET_UINT8_VAL(_data);     \
        break;                                       \
      case TSDB_DATA_TYPE_SMALLINT:                  \
        (_v) = (_finalType)GET_INT16_VAL(_data);     \
        break;                                       \
      case TSDB_DATA_TYPE_USMALLINT:                 \
        (_v) = (_finalType)GET_UINT16_VAL(_data);    \
        break;                                       \
      case TSDB_DATA_TYPE_TIMESTAMP:\
      case TSDB_DATA_TYPE_BIGINT:                    \
        (_v) = (_finalType)(GET_INT64_VAL(_data));   \
        break;                                       \
      case TSDB_DATA_TYPE_UBIGINT:                   \
        (_v) = (_finalType)(GET_UINT64_VAL(_data));  \
        break;                                       \
      case TSDB_DATA_TYPE_FLOAT:                     \
        (_v) = (_finalType)GET_FLOAT_VAL(_data);     \
        break;                                       \
      case TSDB_DATA_TYPE_DOUBLE:                    \
        (_v) = (_finalType)GET_DOUBLE_VAL(_data);    \
        break;                                       \
      case TSDB_DATA_TYPE_UINT:                      \
        (_v) = (_finalType)GET_UINT32_VAL(_data);    \
        break;                                       \
      default:                                       \
        (_v) = (_finalType)GET_INT32_VAL(_data);     \
        break;                                       \
    }                                                \
  } while (0)

#define SET_TYPED_DATA(_v, _type, _data)       \
  do {                                         \
    switch (_type) {                           \
      case TSDB_DATA_TYPE_BOOL:                \
      case TSDB_DATA_TYPE_TINYINT:             \
        *(int8_t *)(_v) = (int8_t)(_data);     \
        break;                                 \
      case TSDB_DATA_TYPE_UTINYINT:            \
        *(uint8_t *)(_v) = (uint8_t)(_data);   \
        break;                                 \
      case TSDB_DATA_TYPE_SMALLINT:            \
        *(int16_t *)(_v) = (int16_t)(_data);   \
        break;                                 \
      case TSDB_DATA_TYPE_USMALLINT:           \
        *(uint16_t *)(_v) = (uint16_t)(_data); \
        break;                                 \
      case TSDB_DATA_TYPE_BIGINT:              \
        *(int64_t *)(_v) = (int64_t)(_data);   \
        break;                                 \
      case TSDB_DATA_TYPE_UBIGINT:             \
        *(uint64_t *)(_v) = (uint64_t)(_data); \
        break;                                 \
      case TSDB_DATA_TYPE_FLOAT:               \
        *(float *)(_v) = (float)(_data);       \
        break;                                 \
      case TSDB_DATA_TYPE_DOUBLE:              \
        *(double *)(_v) = (double)(_data);     \
        break;                                 \
      case TSDB_DATA_TYPE_UINT:                \
        *(uint32_t *)(_v) = (uint32_t)(_data); \
        break;                                 \
      default:                                 \
        *(int32_t *)(_v) = (int32_t)(_data);   \
        break;                                 \
    }                                          \
  } while (0)

#define IS_SIGNED_NUMERIC_TYPE(_t)   ((_t) >= TSDB_DATA_TYPE_TINYINT && (_t) <= TSDB_DATA_TYPE_BIGINT)
#define IS_UNSIGNED_NUMERIC_TYPE(_t) ((_t) >= TSDB_DATA_TYPE_UTINYINT && (_t) <= TSDB_DATA_TYPE_UBIGINT)
#define IS_FLOAT_TYPE(_t)            ((_t) == TSDB_DATA_TYPE_FLOAT || (_t) == TSDB_DATA_TYPE_DOUBLE)

#define IS_NUMERIC_TYPE(_t) ((IS_SIGNED_NUMERIC_TYPE(_t)) || (IS_UNSIGNED_NUMERIC_TYPE(_t)) || (IS_FLOAT_TYPE(_t)))

#define IS_VALID_TINYINT(_t)    ((_t) > INT8_MIN  && (_t) <= INT8_MAX)
#define IS_VALID_SMALLINT(_t)   ((_t) > INT16_MIN && (_t) <= INT16_MAX)
#define IS_VALID_INT(_t)        ((_t) > INT32_MIN && (_t) <= INT32_MAX)
#define IS_VALID_BIGINT(_t)     ((_t) > INT64_MIN && (_t) <= INT64_MAX)
#define IS_VALID_UTINYINT(_t)   ((_t) >= 0 && (_t) < UINT8_MAX)
#define IS_VALID_USMALLINT(_t)  ((_t) >= 0 && (_t) < UINT16_MAX)
#define IS_VALID_UINT(_t)       ((_t) >= 0 && (_t) < UINT32_MAX)
#define IS_VALID_UBIGINT(_t)    ((_t) >= 0 && (_t) < UINT64_MAX)

static FORCE_INLINE bool isNull(const char *val, int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      return *(uint8_t *)val == TSDB_DATA_BOOL_NULL;
    case TSDB_DATA_TYPE_TINYINT:
      return *(uint8_t *)val == TSDB_DATA_TINYINT_NULL;
    case TSDB_DATA_TYPE_SMALLINT:
      return *(uint16_t *)val == TSDB_DATA_SMALLINT_NULL;
    case TSDB_DATA_TYPE_INT:
      return *(uint32_t *)val == TSDB_DATA_INT_NULL;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      return *(uint64_t *)val == TSDB_DATA_BIGINT_NULL;
    case TSDB_DATA_TYPE_FLOAT:
      return *(uint32_t *)val == TSDB_DATA_FLOAT_NULL;
    case TSDB_DATA_TYPE_DOUBLE:
      return *(uint64_t *)val == TSDB_DATA_DOUBLE_NULL;
    case TSDB_DATA_TYPE_NCHAR:
      return varDataLen(val) == sizeof(int32_t) && *(uint32_t*) varDataVal(val) == TSDB_DATA_NCHAR_NULL;
    case TSDB_DATA_TYPE_BINARY:
      return varDataLen(val) == sizeof(int8_t) && *(uint8_t *) varDataVal(val) == TSDB_DATA_BINARY_NULL;
    case TSDB_DATA_TYPE_UTINYINT:
      return *(uint8_t*) val == TSDB_DATA_UTINYINT_NULL;
    case TSDB_DATA_TYPE_USMALLINT:
      return *(uint16_t*) val == TSDB_DATA_USMALLINT_NULL;
    case TSDB_DATA_TYPE_UINT:
      return *(uint32_t*) val == TSDB_DATA_UINT_NULL;
    case TSDB_DATA_TYPE_UBIGINT:
      return *(uint64_t*) val == TSDB_DATA_UBIGINT_NULL;

    default:
      return false;
  };
}

typedef struct tDataTypeDescriptor {
  int16_t type;
  int16_t nameLen;
  int32_t bytes;
  char *  name;
  int (*compFunc)(const char *const input, int inputSize, const int nelements, char *const output, int outputSize,
                  char algorithm, char *const buffer, int bufferSize);
  int (*decompFunc)(const char *const input, int compressedSize, const int nelements, char *const output,
                    int outputSize, char algorithm, char *const buffer, int bufferSize);
  void (*statisFunc)(const void *pData, int32_t numofrow, int64_t *min, int64_t *max, int64_t *sum,
                        int16_t *minindex, int16_t *maxindex, int16_t *numofnull);
} tDataTypeDescriptor;

extern tDataTypeDescriptor tDataTypes[15];

bool isValidDataType(int32_t type);

void setVardataNull(char* val, int32_t type);
void setNull(char *val, int32_t type, int32_t bytes);
void setNullN(char *val, int32_t type, int32_t bytes, int32_t numOfElems);
void* getNullValue(int32_t type);

void assignVal(char *val, const char *src, int32_t len, int32_t type);
void tsDataSwap(void *pLeft, void *pRight, int32_t type, int32_t size, void* buf);

int32_t tStrToInteger(const char* z, int16_t type, int32_t n, int64_t* value, bool issigned);

#define SET_DOUBLE_NULL(v) (*(uint64_t *)(v) = TSDB_DATA_DOUBLE_NULL)

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TTYPE_H
