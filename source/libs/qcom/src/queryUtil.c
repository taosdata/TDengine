#include "os.h"
#include "taosmsg.h"

#define VALIDNUMOFCOLS(x)  ((x) >= TSDB_MIN_COLUMNS && (x) <= TSDB_MAX_COLUMNS)
#define VALIDNUMOFTAGS(x)  ((x) >= 0 && (x) <= TSDB_MAX_TAGS)

static struct SSchema _s = {
    .colId = TSDB_TBNAME_COLUMN_INDEX,
    .type  = TSDB_DATA_TYPE_BINARY,
    .bytes = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE,
    .name = "tbname",
};

SSchema* tGetTbnameColumnSchema() {
  return &_s;
}

static bool doValidateSchema(SSchema* pSchema, int32_t numOfCols, int32_t maxLen) {
  int32_t rowLen = 0;

  for (int32_t i = 0; i < numOfCols; ++i) {
    // 1. valid types
    if (!isValidDataType(pSchema[i].type)) {
      return false;
    }

    // 2. valid length for each type
    if (pSchema[i].type == TSDB_DATA_TYPE_BINARY) {
      if (pSchema[i].bytes > TSDB_MAX_BINARY_LEN) {
        return false;
      }
    } else if (pSchema[i].type == TSDB_DATA_TYPE_NCHAR) {
      if (pSchema[i].bytes > TSDB_MAX_NCHAR_LEN) {
        return false;
      }
    } else {
      if (pSchema[i].bytes != tDataTypes[pSchema[i].type].bytes) {
        return false;
      }
    }

    // 3. valid column names
    for (int32_t j = i + 1; j < numOfCols; ++j) {
      if (strncasecmp(pSchema[i].name, pSchema[j].name, sizeof(pSchema[i].name) - 1) == 0) {
        return false;
      }
    }

    rowLen += pSchema[i].bytes;
  }

  return rowLen <= maxLen;
}

bool tIsValidSchema(struct SSchema* pSchema, int32_t numOfCols, int32_t numOfTags) {
  if (!VALIDNUMOFCOLS(numOfCols)) {
    return false;
  }

  if (!VALIDNUMOFTAGS(numOfTags)) {
    return false;
  }

  /* first column must be the timestamp, which is a primary key */
  if (pSchema[0].type != TSDB_DATA_TYPE_TIMESTAMP) {
    return false;
  }

  if (!doValidateSchema(pSchema, numOfCols, TSDB_MAX_BYTES_PER_ROW)) {
    return false;
  }

  if (!doValidateSchema(&pSchema[numOfCols], numOfTags, TSDB_MAX_TAGS_LEN)) {
    return false;
  }

  return true;
}