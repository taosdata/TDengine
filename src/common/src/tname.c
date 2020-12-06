#include "os.h"
#include "tutil.h"

#include "tname.h"
#include "tstoken.h"
#include "ttokendef.h"
#include "tvariant.h"

// todo refactor
UNUSED_FUNC static FORCE_INLINE const char* skipSegments(const char* input, char delim, int32_t num) {
  for (int32_t i = 0; i < num; ++i) {
    while (*input != 0 && *input++ != delim) {
    };
  }
  return input;
}

UNUSED_FUNC static FORCE_INLINE size_t copy(char* dst, const char* src, char delimiter) {
  size_t len = 0;
  while (*src != delimiter && *src != 0) {
    *dst++ = *src++;
    len++;
  }
  
  return len;
}

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

int32_t tableIdPrefix(const char* name, char* prefix, int32_t len) {
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
    s.bytes = tDataTypeDesc[pVal->nType].nSize;
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

SColumnFilterInfo* tscFilterInfoClone(const SColumnFilterInfo* src, int32_t numOfFilters) {
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

SSchema tscGetTbnameColumnSchema() {
  struct SSchema s = {
      .colId = TSDB_TBNAME_COLUMN_INDEX,
      .type  = TSDB_DATA_TYPE_BINARY,
      .bytes = TSDB_TABLE_NAME_LEN
  };

  strcpy(s.name, TSQL_TBNAME_L);
  return s;
}
