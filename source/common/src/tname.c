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

#define _DEFAULT_SOURCE
#include "tname.h"
#include "tcommon.h"
#include "tstrbuild.h"

#define VALID_NAME_TYPE(x) ((x) == TSDB_DB_NAME_T || (x) == TSDB_TABLE_NAME_T)

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
    taosLocalTime(&t, &tm);
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

SName* toName(int32_t acctId, const char* pDbName, const char* pTableName, SName* pName) {
  pName->type = TSDB_TABLE_NAME_T;
  pName->acctId = acctId;
  snprintf(pName->dbname, sizeof(pName->dbname), "%s", pDbName);
  snprintf(pName->tname, sizeof(pName->tname), "%s", pTableName);
  return pName;
}

int32_t tNameExtractFullName(const SName* name, char* dst) {
  // invalid full name format, abort
  if (!tNameIsValid(name)) {
    return -1;
  }

  int32_t len = snprintf(dst, TSDB_DB_FNAME_LEN, "%d.%s", name->acctId, name->dbname);

  size_t tnameLen = strlen(name->tname);
  if (tnameLen > 0) {
    /*ASSERT(name->type == TSDB_TABLE_NAME_T);*/
    dst[len] = TS_PATH_DELIMITER[0];

    memcpy(dst + len + 1, name->tname, tnameLen);
    dst[len + tnameLen + 1] = 0;
  }

  return 0;
}

int32_t tNameLen(const SName* name) {
  char    tmp[12] = {0};
  int32_t len = sprintf(tmp, "%d", name->acctId);
  int32_t len1 = (int32_t)strlen(name->dbname);
  int32_t len2 = (int32_t)strlen(name->tname);

  if (name->type == TSDB_DB_NAME_T) {
    return len + len1 + TSDB_NAME_DELIMITER_LEN;
  } else {
    return len + len1 + len2 + TSDB_NAME_DELIMITER_LEN * 2;
  }
}

bool tNameIsValid(const SName* name) {
  if (!VALID_NAME_TYPE(name->type)) {
    return false;
  }

  if (name->type == TSDB_DB_NAME_T) {
    return strlen(name->dbname) > 0;
  } else {
    return strlen(name->dbname) > 0 && strlen(name->tname) > 0;
  }
}

SName* tNameDup(const SName* name) {
  SName* p = taosMemoryMalloc(sizeof(SName));
  memcpy(p, name, sizeof(SName));
  return p;
}

int32_t tNameGetDbName(const SName* name, char* dst) {
  strncpy(dst, name->dbname, tListLen(name->dbname));
  return 0;
}

const char* tNameGetDbNameP(const SName* name) { return &name->dbname[0]; }

int32_t tNameGetFullDbName(const SName* name, char* dst) {
  snprintf(dst, TSDB_DB_FNAME_LEN, "%d.%s", name->acctId, name->dbname);
  return 0;
}

bool tNameIsEmpty(const SName* name) { return name->type == 0 || name->acctId == 0; }

const char* tNameGetTableName(const SName* name) {
  ASSERT(name != NULL && name->type == TSDB_TABLE_NAME_T);
  return &name->tname[0];
}

void tNameAssign(SName* dst, const SName* src) { memcpy(dst, src, sizeof(SName)); }

int32_t tNameSetDbName(SName* dst, int32_t acct, const char* dbName, size_t nameLen) {
  // too long account id or too long db name
  if (nameLen <= 0 || nameLen >= tListLen(dst->dbname)) {
    return -1;
  }

  dst->type = TSDB_DB_NAME_T;
  dst->acctId = acct;
  tstrncpy(dst->dbname, dbName, nameLen + 1);
  return 0;
}

int32_t tNameAddTbName(SName* dst, const char* tbName, size_t nameLen) {
  // too long account id or too long db name
  if (nameLen >= tListLen(dst->tname) || nameLen <= 0) {
    return -1;
  }

  dst->type = TSDB_TABLE_NAME_T;
  tstrncpy(dst->tname, tbName, nameLen + 1);
  return 0;
}

int32_t tNameSetAcctId(SName* dst, int32_t acctId) {
  dst->acctId = acctId;
  return 0;
}

bool tNameDBNameEqual(SName* left, SName* right) {
  if (NULL == left) {
    if (NULL == right) {
      return true;
    }

    return false;
  }

  if (NULL == right) {
    return false;
  }

  if (left->acctId != right->acctId) {
    return false;
  }

  return (0 == strcmp(left->dbname, right->dbname));
}

bool tNameTbNameEqual(SName* left, SName* right) {
  bool equal = tNameDBNameEqual(left, right);
  if (equal) {
    return (0 == strcmp(left->tname, right->tname));
  }

  return equal;
}

int32_t tNameFromString(SName* dst, const char* str, uint32_t type) {
  if (strlen(str) == 0) {
    return -1;
  }

  char* p = NULL;
  if ((type & T_NAME_ACCT) == T_NAME_ACCT) {
    p = strstr(str, TS_PATH_DELIMITER);
    if (p == NULL) {
      return -1;
    }

    dst->acctId = taosStr2Int32(str, NULL, 10);
  }

  if ((type & T_NAME_DB) == T_NAME_DB) {
    dst->type = TSDB_DB_NAME_T;
    char* start = (char*)((p == NULL) ? str : (p + 1));

    int32_t len = 0;
    p = strstr(start, TS_PATH_DELIMITER);
    if (p == NULL) {
      len = (int32_t)strlen(start);
    } else {
      len = (int32_t)(p - start);
    }

    // too long account id or too long db name
    if ((len >= tListLen(dst->dbname)) || (len <= 0)) {
      return -1;
    }

    memcpy(dst->dbname, start, len);
    dst->dbname[len] = 0;
  }

  if ((type & T_NAME_TABLE) == T_NAME_TABLE) {
    dst->type = TSDB_TABLE_NAME_T;
    char* start = (char*)((p == NULL) ? str : (p + 1));

    // too long account id or too long db name
    int32_t len = (int32_t)strlen(start);
    if ((len >= tListLen(dst->tname)) || (len <= 0)) {
      return -1;
    }

    memcpy(dst->tname, start, len);
    dst->tname[len] = 0;
  }

  return 0;
}

static int compareKv(const void* p1, const void* p2) {
  SSmlKv* kv1 = (SSmlKv*)p1;
  SSmlKv* kv2 = (SSmlKv*)p2;
  int32_t kvLen1 = kv1->keyLen;
  int32_t kvLen2 = kv2->keyLen;
  int32_t res = strncasecmp(kv1->key, kv2->key, TMIN(kvLen1, kvLen2));
  if (res != 0) {
    return res;
  } else {
    return kvLen1 - kvLen2;
  }
}

/*
 * use stable name and tags to grearate child table name
 */
void buildChildTableName(RandTableName* rName) {
  SStringBuilder sb = {0};
  taosStringBuilderAppendStringLen(&sb, rName->stbFullName, rName->stbFullNameLen);
  if (sb.buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return;
  }
  taosArraySort(rName->tags, compareKv);
  for (int j = 0; j < taosArrayGetSize(rName->tags); ++j) {
    taosStringBuilderAppendChar(&sb, ',');
    SSmlKv* tagKv = taosArrayGet(rName->tags, j);
    taosStringBuilderAppendStringLen(&sb, tagKv->key, tagKv->keyLen);
    taosStringBuilderAppendChar(&sb, '=');
    if (IS_VAR_DATA_TYPE(tagKv->type)) {
      taosStringBuilderAppendStringLen(&sb, tagKv->value, tagKv->length);
    } else {
      taosStringBuilderAppendStringLen(&sb, (char*)(&(tagKv->value)), tagKv->length);
    }
  }
  size_t    len = 0;
  char*     keyJoined = taosStringBuilderGetResult(&sb, &len);
  T_MD5_CTX context;
  tMD5Init(&context);
  tMD5Update(&context, (uint8_t*)keyJoined, (uint32_t)len);
  tMD5Final(&context);

  char temp[8] = {0};
  rName->ctbShortName[0] = 't';
  rName->ctbShortName[1] = '_';
  for (int i = 0; i < 16; i++) {
    sprintf(temp, "%02x", context.digest[i]);
    strcat(rName->ctbShortName, temp);
  }
  taosStringBuilderDestroy(&sb);
}
