#ifndef TDENGINE_TTYPE_H
#define TDENGINE_TTYPE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "taosdef.h"

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

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TTYPE_H
