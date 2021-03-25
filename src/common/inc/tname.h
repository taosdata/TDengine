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

#ifndef TDENGINE_NAME_H
#define TDENGINE_NAME_H

#include "os.h"
#include "taosmsg.h"
#include "tstoken.h"
#include "tvariant.h"

typedef struct SDataStatis {
  int16_t colId;
  int64_t sum;
  int64_t max;
  int64_t min;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNull;
} SDataStatis;

typedef struct SColumnInfoData {
  SColumnInfo info;
  char* pData;    // the corresponding block data in memory
} SColumnInfoData;

typedef struct SResPair {
  TSKEY  key;
  double avg;
} SResPair;

/* the structure for sql function in select clause */
typedef struct SSqlExpr {
  char      aliasName[TSDB_COL_NAME_LEN];  // as aliasName
  SColIndex colInfo;
  uint64_t  uid;            // refactor use the pointer

  int16_t   functionId;     // function id in aAgg array

  int16_t   resType;        // return value type
  int16_t   resBytes;       // length of return value
  int32_t   interBytes;     // inter result buffer size

  int16_t   colType;        // table column type
  int16_t   colBytes;       // table column bytes

  int16_t   numOfParams;    // argument value of each function
  tVariant  param[3];       // parameters are not more than 3
  int32_t   offset;         // sub result column value of arithmetic expression.
  int16_t   resColId;       // result column id
} SSqlExpr;

typedef struct SExprInfo {
  SSqlExpr    base;
  int64_t     uid;
  struct tExprNode* pExpr;
} SExprInfo;

#define TSDB_DB_NAME_T     1
#define TSDB_TABLE_NAME_T  2

#define T_NAME_ACCT     0x1u
#define T_NAME_DB       0x2u
#define T_NAME_TABLE    0x4u

typedef struct SName {
  uint8_t type;  //db_name_t, table_name_t
  char acctId[TSDB_ACCT_ID_LEN];
  char dbname[TSDB_DB_NAME_LEN];
  char tname[TSDB_TABLE_NAME_LEN];
} SName;

void extractTableName(const char *tableId, char *name);

char* extractDBName(const char *tableId, char *name);

size_t tableIdPrefix(const char* name, char* prefix, int32_t len);

void extractTableNameFromToken(SStrToken *pToken, SStrToken* pTable);

//SSchema tGetTbnameColumnSchema();

SSchema tGetBlockDistColumnSchema();

SSchema tGetUserSpecifiedColumnSchema(tVariant* pVal, SStrToken* exprStr, const char* name);

bool tscValidateTableNameLength(size_t len);

SColumnFilterInfo* tFilterInfoDup(const SColumnFilterInfo* src, int32_t numOfFilters);

SSchema* tGetTbnameColumnSchema();

/**
 * check if the schema is valid or not, including following aspects:
 * 1. number of columns
 * 2. column types
 * 3. column length
 * 4. column names
 * 5. total length
 *
 * @param pSchema
 * @param numOfCols
 * @return
 */
bool tIsValidSchema(struct SSchema* pSchema, int32_t numOfCols, int32_t numOfTags);

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

int32_t tNameSetDbName(SName* dst, const char* acct, SStrToken* dbToken);

#endif  // TDENGINE_NAME_H
