#include "os.h"
#include "tutil.h"

#include "tname.h"
#include "tstoken.h"
#include "tvariant.h"

#define VALIDNUMOFCOLS(x)  ((x) >= TSDB_MIN_COLUMNS && (x) <= TSDB_MAX_COLUMNS)
#define VALIDNUMOFTAGS(x)  ((x) >= 0 && (x) <= TSDB_MAX_TAGS)

#define VALID_NAME_TYPE(x)  ((x) == TSDB_DB_NAME_T || (x) == TSDB_TABLE_NAME_T)

void extractTableName(const char* tableId, char* name) {
  size_t s1 = strcspn(tableId, &TS_PATH_DELIMITER[0]);
  size_t s2 = strcspn(&tableId[s1 + 1], &TS_PATH_DELIMITER[0]);
  
  tstrncpy(name, &tableId[s1 + s2 + 2], TSDB_TABLE_NAME_LEN);
}

char* extractDBName(const char* tableId, char* name) {
  size_t offset1 = strcspn(tableId, &TS_PATH_DELIMITER[0]);
  size_t len = strcspn(&tableId[offset1 + 1], &TS_PATH_DELIMITER[0]);
  
  return strncpy(name, &tableId[offset1 + 1], len);
}

size_t tableIdPrefix(const char* name, char* prefix, int32_t len) {
  tstrncpy(prefix, name, len);
  strcat(prefix, TS_PATH_DELIMITER);

  return strlen(prefix);
}

SSchema tGetTableNameColumnSchema() {
  SSchema s = {0};
  s.bytes = TSDB_TABLE_NAME_LEN - 1 + VARSTR_HEADER_SIZE;
  s.type  = TSDB_DATA_TYPE_BINARY;
  s.colId = TSDB_TBNAME_COLUMN_INDEX;
  tstrncpy(s.name, TSQL_TBNAME_L, TSDB_COL_NAME_LEN);
  return s;
}

SSchema tGetUserSpecifiedColumnSchema(tVariant* pVal, SStrToken* exprStr, const char* name) {
  SSchema s = {0};

  s.type  = pVal->nType;
  if (s.type == TSDB_DATA_TYPE_BINARY || s.type == TSDB_DATA_TYPE_NCHAR) {
    s.bytes = (int16_t)(pVal->nLen + VARSTR_HEADER_SIZE);
  } else {
    s.bytes = tDataTypes[pVal->nType].bytes;
  }

  s.colId = TSDB_UD_COLUMN_INDEX;
  if (name != NULL) {
    tstrncpy(s.name, name, sizeof(s.name));
  } else {
    size_t tlen = MIN(sizeof(s.name), exprStr->n + 1);
    tstrncpy(s.name, exprStr->z, tlen);
    strdequote(s.name);
  }

  return s;
}

bool tscValidateTableNameLength(size_t len) {
  return len < TSDB_TABLE_NAME_LEN;
}

SColumnFilterInfo* tFilterInfoDup(const SColumnFilterInfo* src, int32_t numOfFilters) {
  if (numOfFilters == 0) {
    assert(src == NULL);
    return NULL;
  }

  SColumnFilterInfo* pFilter = calloc(1, numOfFilters * sizeof(SColumnFilterInfo));

  memcpy(pFilter, src, sizeof(SColumnFilterInfo) * numOfFilters);
  for (int32_t j = 0; j < numOfFilters; ++j) {

    if (pFilter[j].filterstr) {
      size_t len = (size_t) pFilter[j].len + 1 * TSDB_NCHAR_SIZE;
      pFilter[j].pz = (int64_t) calloc(1, len);

      memcpy((char*)pFilter[j].pz, (char*)src[j].pz, (size_t)len);
    }
  }

  assert(src->filterstr == 0 || src->filterstr == 1);
  assert(!(src->lowerRelOptr == TSDB_RELATION_INVALID && src->upperRelOptr == TSDB_RELATION_INVALID));

  return pFilter;
}

#if 0
int64_t taosGetIntervalStartTimestamp(int64_t startTime, int64_t slidingTime, int64_t intervalTime, char timeUnit, int16_t precision) {
  if (slidingTime == 0) {
    return startTime;
  }
  int64_t start = startTime;
  if (timeUnit == 'n' || timeUnit == 'y') {
    start /= 1000;
    if (precision == TSDB_TIME_PRECISION_MICRO) {
      start /= 1000;
    }
    struct tm tm;
    time_t t = (time_t)start;
    localtime_r(&t, &tm);
    tm.tm_sec = 0;
    tm.tm_min = 0;
    tm.tm_hour = 0;
    tm.tm_mday = 1;

    if (timeUnit == 'y') {
      tm.tm_mon = 0;
      tm.tm_year = (int)(tm.tm_year / slidingTime * slidingTime);
    } else {
      int mon = tm.tm_year * 12 + tm.tm_mon;
      mon = (int)(mon / slidingTime * slidingTime);
      tm.tm_year = mon / 12;
      tm.tm_mon = mon % 12;
    }

    start = mktime(&tm) * 1000L;
    if (precision == TSDB_TIME_PRECISION_MICRO) {
      start *= 1000L;
    }
  } else {
    int64_t delta = startTime - intervalTime;
    int32_t factor = delta > 0? 1:-1;

    start = (delta / slidingTime + factor) * slidingTime;

    if (timeUnit == 'd' || timeUnit == 'w') {
      /*
      * here we revised the start time of day according to the local time zone,
      * but in case of DST, the start time of one day need to be dynamically decided.
      */
      // todo refactor to extract function that is available for Linux/Windows/Mac platform
  #if defined(WINDOWS) && _MSC_VER >= 1900
      // see https://docs.microsoft.com/en-us/cpp/c-runtime-library/daylight-dstbias-timezone-and-tzname?view=vs-2019
      int64_t timezone = _timezone;
      int32_t daylight = _daylight;
      char**  tzname = _tzname;
  #endif

      int64_t t = (precision == TSDB_TIME_PRECISION_MILLI) ? MILLISECOND_PER_SECOND : MILLISECOND_PER_SECOND * 1000L;
      start += timezone * t;
    }

    int64_t end = start + intervalTime - 1;
    if (end < startTime) {
      start += slidingTime;
    }
  }

  return start;
}

#endif

/*
 * tablePrefix.columnName
 * extract table name and save it in pTable, with only column name in pToken
 */
void extractTableNameFromToken(SStrToken* pToken, SStrToken* pTable) {
  const char sep = TS_PATH_DELIMITER[0];

  if (pToken == pTable || pToken == NULL || pTable == NULL) {
    return;
  }

  char* r = strnchr(pToken->z, sep, pToken->n, false);

  if (r != NULL) {  // record the table name token
    pTable->n = (uint32_t)(r - pToken->z);
    pTable->z = pToken->z;

    r += 1;
    pToken->n -= (uint32_t)(r - pToken->z);
    pToken->z = r;
  }
}

SSchema tGetTbnameColumnSchema() {
  struct SSchema s = {
      .colId = TSDB_TBNAME_COLUMN_INDEX,
      .type  = TSDB_DATA_TYPE_BINARY,
      .bytes = TSDB_TABLE_NAME_LEN
  };

  strcpy(s.name, TSQL_TBNAME_L);
  return s;
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

int32_t tNameExtractFullName(const SName* name, char* dst) {
  assert(name != NULL && dst != NULL);

  // invalid full name format, abort
  if (!tIsValidName(name)) {
    return -1;
  }

  int32_t len = snprintf(dst, TSDB_ACCT_ID_LEN + 1 + TSDB_DB_NAME_LEN, "%s.%s", name->acctId, name->dbname);

  size_t tnameLen = strlen(name->tname);
  if (tnameLen > 0) {
    assert(name->type == TSDB_TABLE_NAME_T);
    dst[len] = TS_PATH_DELIMITER[0];

    memcpy(dst + len + 1, name->tname, tnameLen);
    dst[len + tnameLen + 1] = 0;
  }

  return 0;
}

int32_t tNameLen(const SName* name) {
  assert(name != NULL);
  int32_t len  = (int32_t) strlen(name->acctId);
  int32_t len1 = (int32_t) strlen(name->dbname);
  int32_t len2 = (int32_t) strlen(name->tname);

  if (name->type == TSDB_DB_NAME_T) {
    assert(len2 == 0);
    return len + len1 + TS_PATH_DELIMITER_LEN;
  } else {
    assert(len2 > 0);
    return len + len1 + len2 + TS_PATH_DELIMITER_LEN * 2;
  }
}

bool tIsValidName(const SName* name) {
  assert(name != NULL);

  if (!VALID_NAME_TYPE(name->type)) {
    return false;
  }

  if (strlen(name->acctId) <= 0) {
    return false;
  }

  if (name->type == TSDB_DB_NAME_T) {
    return strlen(name->dbname) > 0;
  } else {
    return strlen(name->dbname) > 0 && strlen(name->tname) > 0;
  }
}

SName* tNameDup(const SName* name) {
  assert(name != NULL);

  SName* p = calloc(1, sizeof(SName));
  memcpy(p, name, sizeof(SName));
  return p;
}

int32_t tNameGetDbName(const SName* name, char* dst) {
  assert(name != NULL && dst != NULL);
  strncpy(dst, name->dbname, tListLen(name->dbname));
  return 0;
}

int32_t tNameGetFullDbName(const SName* name, char* dst) {
  assert(name != NULL && dst != NULL);
  snprintf(dst, TSDB_ACCT_ID_LEN + TS_PATH_DELIMITER_LEN + TSDB_DB_NAME_LEN,
      "%s.%s", name->acctId, name->dbname);
  return 0;
}

bool tNameIsEmpty(const SName* name) {
  assert(name != NULL);
  return name->type == 0 || strlen(name->acctId) <= 0;
}

const char* tNameGetTableName(const SName* name) {
  assert(name != NULL && name->type == TSDB_TABLE_NAME_T);
  return &name->tname[0];
}

void tNameAssign(SName* dst, const SName* src) {
  memcpy(dst, src, sizeof(SName));
}

int32_t tNameSetDbName(SName* dst, const char* acct, SStrToken* dbToken) {
  assert(dst != NULL && dbToken != NULL && acct != NULL);

  // too long account id or too long db name
  if (strlen(acct) >= tListLen(dst->acctId) || dbToken->n >= tListLen(dst->dbname)) {
    return -1;
  }

  dst->type = TSDB_DB_NAME_T;
  tstrncpy(dst->acctId, acct, tListLen(dst->acctId));
  tstrncpy(dst->dbname, dbToken->z, dbToken->n + 1);
  return 0;
}

int32_t tNameSetAcctId(SName* dst, const char* acct) {
  assert(dst != NULL && acct != NULL);

  // too long account id or too long db name
  if (strlen(acct) >= tListLen(dst->acctId)) {
    return -1;
  }

  tstrncpy(dst->acctId, acct, tListLen(dst->acctId));
  return 0;
}

int32_t tNameFromString(SName* dst, const char* str, uint32_t type) {
  assert(dst != NULL && str != NULL && strlen(str) > 0);

  char* p = NULL;
  if ((type & T_NAME_ACCT) == T_NAME_ACCT) {
    p = strstr(str, TS_PATH_DELIMITER);
    if (p == NULL) {
      return -1;
    }

    int32_t len = p - str;

    // too long account id or too long db name
    if (len >= tListLen(dst->acctId) || len == 0) {
      return -1;
    }

    memcpy (dst->acctId, str, len);
    dst->acctId[len] = 0;
  }

  if ((type & T_NAME_DB) == T_NAME_DB) {
    dst->type = TSDB_DB_NAME_T;
    char* start = (char*)((p == NULL)? str:(p+1));

    int32_t len = 0;
    p = strstr(start, TS_PATH_DELIMITER);
    if (p == NULL) {
      len = strlen(start);
    } else {
      len = p - start;
    }

    // too long account id or too long db name
    if (len >= tListLen(dst->dbname) || len == 0) {
      return -1;
    }

    memcpy (dst->dbname, start, len);
    dst->dbname[len] = 0;
  }

  if ((type & T_NAME_TABLE) == T_NAME_TABLE) {
    dst->type = TSDB_TABLE_NAME_T;
    char* start = (char*) ((p == NULL)? str: (p+1));

    int32_t len = strlen(start);

    // too long account id or too long db name
    if (len >= tListLen(dst->tname) || len == 0) {
      return -1;
    }

    memcpy (dst->tname, start, len);
    dst->tname[len] = 0;
  }

  return 0;
}
