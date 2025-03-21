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

#define VALID_NAME_TYPE(x) ((x) == TSDB_DB_NAME_T || (x) == TSDB_TABLE_NAME_T)

void toName(int32_t acctId, const char* pDbName, const char* pTableName, SName* pName) {
  if (pName == NULL){
    return;
  }
  pName->type = TSDB_TABLE_NAME_T;
  pName->acctId = acctId;
  snprintf(pName->dbname, sizeof(pName->dbname), "%s", pDbName);
  snprintf(pName->tname, sizeof(pName->tname), "%s", pTableName);
}

int32_t tNameExtractFullName(const SName* name, char* dst) {
  // invalid full name format, abort
  if (!tNameIsValid(name)) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t len = tsnprintf(dst, TSDB_DB_FNAME_LEN, "%d.%s", name->acctId, name->dbname);

  size_t tnameLen = strlen(name->tname);
  if (tnameLen > 0) {
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
  if (p) TAOS_MEMCPY(p, name, sizeof(SName));
  return p;
}

int32_t tNameGetDbName(const SName* name, char* dst) {
  tstrncpy(dst, name->dbname, tListLen(name->dbname));
  return 0;
}

const char* tNameGetDbNameP(const SName* name) { return &name->dbname[0]; }

int32_t tNameGetFullDbName(const SName* name, char* dst) {
  if (name == NULL || dst == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  (void)snprintf(dst, TSDB_DB_FNAME_LEN, "%d.%s", name->acctId, name->dbname);
  return 0;
}

bool tNameIsEmpty(const SName* name) { return name->type == 0 || name->acctId == 0; }

const char* tNameGetTableName(const SName* name) {
  if (!(name != NULL && name->type == TSDB_TABLE_NAME_T)) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  return &name->tname[0];
}

int32_t tNameGetFullTableName(const SName* name, char* dst) {
  if (name == NULL || dst == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  (void)snprintf(dst, TSDB_TABLE_FNAME_LEN, "%s.%s", name->dbname, name->tname);
  return 0;
}

void tNameAssign(SName* dst, const SName* src) { memcpy(dst, src, sizeof(SName)); }

int32_t tNameSetDbName(SName* dst, int32_t acct, const char* dbName, size_t nameLen) {
  // too long account id or too long db name
  if (nameLen <= 0 || nameLen >= tListLen(dst->dbname)) {
    return TSDB_CODE_INVALID_PARA;
  }

  dst->type = TSDB_DB_NAME_T;
  dst->acctId = acct;
  tstrncpy(dst->dbname, dbName, nameLen + 1);
  return 0;
}

int32_t tNameAddTbName(SName* dst, const char* tbName, size_t nameLen) {
  // too long account id or too long db name
  if (nameLen >= tListLen(dst->tname) || nameLen <= 0) {
    return TSDB_CODE_INVALID_PARA;
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
    return TSDB_CODE_INVALID_PARA;
  }

  char* p = NULL;
  if ((type & T_NAME_ACCT) == T_NAME_ACCT) {
    p = strstr(str, TS_PATH_DELIMITER);
    if (p == NULL) {
      return TSDB_CODE_INVALID_PARA;
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
      return TSDB_CODE_INVALID_PARA;
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
      return TSDB_CODE_INVALID_PARA;
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
  int32_t res = taosStrncasecmp(kv1->key, kv2->key, TMIN(kvLen1, kvLen2));
  if (res != 0) {
    return res;
  } else {
    return kvLen1 - kvLen2;
  }
}

/*
 * use stable name and tags to generate child table name
 */
int32_t buildChildTableName(RandTableName* rName) {
  taosArraySort(rName->tags, compareKv);

  T_MD5_CTX context;
  tMD5Init(&context);

  // add stable name
  tMD5Update(&context, (uint8_t*)rName->stbFullName, rName->stbFullNameLen);

  // add tags
  for (int j = 0; j < taosArrayGetSize(rName->tags); ++j) {
    SSmlKv* tagKv = taosArrayGet(rName->tags, j);
    if (tagKv == NULL) {
      return TSDB_CODE_SML_INVALID_DATA;
    }

    tMD5Update(&context, (uint8_t*)",", 1);
    tMD5Update(&context, (uint8_t*)tagKv->key, tagKv->keyLen);
    tMD5Update(&context, (uint8_t*)"=", 1);

    if (IS_VAR_DATA_TYPE(tagKv->type)) {
      tMD5Update(&context, (uint8_t*)tagKv->value, tagKv->length);
    } else {
      tMD5Update(&context, (uint8_t*)(&(tagKv->value)), tagKv->length);
    }
  }

  tMD5Final(&context);

  rName->ctbShortName[0] = 't';
  rName->ctbShortName[1] = '_';
  taosByteArrayToHexStr(context.digest, 16, rName->ctbShortName + 2);
  rName->ctbShortName[34] = 0;

  return TSDB_CODE_SUCCESS;
}
