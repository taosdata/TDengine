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
  void* pData;    // the corresponding block data in memory
} SColumnInfoData;

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

SSchema tGetTableNameColumnSchema();

SSchema tGetUserSpecifiedColumnSchema(tVariant* pVal, SStrToken* exprStr, const char* name);

bool tscValidateTableNameLength(size_t len);

SColumnFilterInfo* tFilterInfoDup(const SColumnFilterInfo* src, int32_t numOfFilters);

SSchema tGetTbnameColumnSchema();

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
