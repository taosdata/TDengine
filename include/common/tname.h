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

#ifndef TDENGINE_TNAME_H
#define TDENGINE_TNAME_H

#define TSDB_DB_NAME_T     1
#define TSDB_TABLE_NAME_T  2

#define T_NAME_ACCT        0x1u
#define T_NAME_DB          0x2u
#define T_NAME_TABLE       0x4u

typedef struct SName {
  uint8_t type;  //db_name_t, table_name_t
  char    acctId[TSDB_ACCT_ID_LEN];
  char    dbname[TSDB_DB_NAME_LEN];
  char    tname[TSDB_TABLE_NAME_LEN];
} SName;

int32_t tNameExtractFullName(const SName* name, char* dst);

int32_t tNameLen(const SName* name);

SName* tNameDup(const SName* name);

bool tIsValidName(const SName* name);

const char* tNameGetTableName(const SName* name);

int32_t tNameGetDbName(const SName* name, char* dst);

int32_t tNameGetFullDbName(const SName* name, char* dst);

bool tNameIsEmpty(const SName* name);

void tNameAssign(SName* dst, const SName* src);

int32_t tNameFromString(SName* dst, const char* str, uint32_t type);

int32_t tNameSetAcctId(SName* dst, const char* acct);

#if 0
int32_t tNameSetDbName(SName* dst, const char* acct, SToken* dbToken);
#endif

#endif  // TDENGINE_TNAME_H
