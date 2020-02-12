#if !defined(_TD_TYPE_H_)
#define _TD_TYPE_H_

#include <stdint.h>

typedef enum {
  TD_DATATYPE_BOOL = 0,
  TD_DATATYPE_TINYINT,
  TD_DATATYPE_SMALLINT,
  TD_DATATYPE_INT,
  TD_DATATYPE_BIGINT,
  TD_DATATYPE_FLOAT,
  TD_DATATYPE_DOUBLE,
  TD_DATATYPE_VARCHAR,
  TD_DATATYPE_NCHAR,
  TD_DATATYPE_BINARY
} td_datatype_t;

const int32_t rowDataLen[] = {
    sizeof(int8_t),   // TD_DATATYPE_BOOL,
    sizeof(int8_t),   // TD_DATATYPE_TINYINT,
    sizeof(int16_t),  // TD_DATATYPE_SMALLINT,
    sizeof(int32_t),  // TD_DATATYPE_INT,
    sizeof(int64_t),  // TD_DATATYPE_BIGINT,
    sizeof(float),    // TD_DATATYPE_FLOAT,
    sizeof(double),   // TD_DATATYPE_DOUBLE,
    sizeof(int32_t),  // TD_DATATYPE_VARCHAR,
    sizeof(int32_t),  // TD_DATATYPE_NCHAR,
    sizeof(int32_t)   // TD_DATATYPE_BINARY
};

// TODO: finish below
#define TD_DATATYPE_BOOL_NULL
#define TD_DATATYPE_TINYINT_NULL
#define TD_DATATYPE_SMALLINT_NULL
#define TD_DATATYPE_INT_NULL
#define TD_DATATYPE_BIGINT_NULL
#define TD_DATATYPE_FLOAT_NULL
#define TD_DATATYPE_DOUBLE_NULL
#define TD_DATATYPE_VARCHAR_NULL
#define TD_DATATYPE_NCHAR_NULL
#define TD_DATATYPE_BINARY_NULL

#define TD_IS_VALID_DATATYPE(type) (((type) > TD_DATA_TYPE_INVLD) && ((type) <= TD_DATATYPE_BINARY))

#endif  // _TD_TYPE_H_
