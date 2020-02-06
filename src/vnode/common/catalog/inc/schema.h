#if !defined(_TD_SCHEMA_H_)
#define _TD_SCHEMA_H_

#include <stdint.h>

#include "type.h"

typedef struct _scolumn {
  td_datatype_t type;
  int32_t bytes;
} SColumn;

typedef struct SSchema {
  int32_t numOfCols;
  SColumn *columns;
} SSchema;

#endif  // _TD_SCHEMA_H_
