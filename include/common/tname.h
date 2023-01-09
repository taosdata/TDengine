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

#ifndef _TD_COMMON_NAME_H_
#define _TD_COMMON_NAME_H_

#include "tarray.h"
#include "tdef.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_DB_NAME_T    1
#define TSDB_TABLE_NAME_T 2

#define T_NAME_ACCT  0x1u
#define T_NAME_DB    0x2u
#define T_NAME_TABLE 0x4u

typedef struct SName {
  uint8_t type;  // db_name_t, table_name_t
  int32_t acctId;
  char    dbname[TSDB_DB_NAME_LEN];
  char    tname[TSDB_TABLE_NAME_LEN];
} SName;

SName* toName(int32_t acctId, const char* pDbName, const char* pTableName, SName* pName);

int32_t tNameExtractFullName(const SName* name, char* dst);

int32_t tNameLen(const SName* name);

SName* tNameDup(const SName* name);

bool tNameIsValid(const SName* name);

const char* tNameGetTableName(const SName* name);

int32_t     tNameGetDbName(const SName* name, char* dst);
const char* tNameGetDbNameP(const SName* name);

int32_t tNameGetFullDbName(const SName* name, char* dst);

bool tNameIsEmpty(const SName* name);

void tNameAssign(SName* dst, const SName* src);

int32_t tNameSetDbName(SName* dst, int32_t acctId, const char* dbName, size_t nameLen);

int32_t tNameAddTbName(SName* dst, const char* tbName, size_t nameLen);

int32_t tNameFromString(SName* dst, const char* str, uint32_t type);

int32_t tNameSetAcctId(SName* dst, int32_t acctId);

bool tNameDBNameEqual(SName* left, SName* right);

bool tNameTbNameEqual(SName* left, SName* right);

typedef struct {
  // input
  SArray*     tags;            // element is SSmlKv
  const char* stbFullName;     // super table name
  uint8_t     stbFullNameLen;  // the length of super table name

  // output
  char*    ctbShortName;  // must have size of TSDB_TABLE_NAME_LEN;
//  uint64_t uid;           // child table uid, may be useful
} RandTableName;

void buildChildTableName(RandTableName* rName);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_NAME_H_*/
