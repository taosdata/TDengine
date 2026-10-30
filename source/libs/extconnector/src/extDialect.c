/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include "extDialect.h"

#include "ttime.h"

int32_t extDialectGetDefaultClauseDemotions(int32_t dialect, const EExtClauseKind** ppClauses, int32_t* pNum);
int32_t extDialectGetCommonClauseDemotions(const EExtClauseKind** ppClauses, int32_t* pNum);
int32_t extDialectGetDefaultFuncDemotions(int32_t dialect, const char* const** pppFuncs, int32_t* pNum);
int32_t extDialectGetCommonFuncDemotions(const char* const** pppFuncs, int32_t* pNum);
int32_t extDialectGetDefaultOpDemotions(int32_t dialect, const EOperatorType** ppOps, int32_t* pNum);

int32_t extResolveDialectBySource(int8_t sourceType, int32_t* pDialect) {
  if (NULL == pDialect) {
    uError("extResolveDialectBySource: pDialect is NULL");
    return TSDB_CODE_INVALID_PARA;
  }

  switch ((EExtSourceType)sourceType) {
    case EXT_SOURCE_MYSQL:
      *pDialect = EXT_SQL_DIALECT_MYSQL;
      return TSDB_CODE_SUCCESS;
    case EXT_SOURCE_POSTGRESQL:
      *pDialect = EXT_SQL_DIALECT_POSTGRES;
      return TSDB_CODE_SUCCESS;
    case EXT_SOURCE_INFLUXDB:
      *pDialect = EXT_SQL_DIALECT_INFLUXQL;
      return TSDB_CODE_SUCCESS;
    case EXT_SOURCE_SQLSERVER:
      *pDialect = EXT_SQL_DIALECT_SQLSERVER;
      return TSDB_CODE_SUCCESS;
    case EXT_SOURCE_ORACLE:
      *pDialect = EXT_SQL_DIALECT_ORACLE;
      return TSDB_CODE_SUCCESS;
    default:
      uError("extResolveDialectBySource: unknown sourceType=%d", (int)sourceType);
      return TSDB_CODE_INVALID_PARA;
  }
}

bool extDialectClauseMappable(int32_t dialect, EExtClauseKind clause) {
  const EExtClauseKind* pCommon = NULL;
  int32_t               commonNum = 0;
  if (TSDB_CODE_SUCCESS == extDialectGetCommonClauseDemotions(&pCommon, &commonNum)) {
    for (int32_t i = 0; i < commonNum; ++i) {
      if (pCommon[i] == clause) {
        return false;
      }
    }
  }

  const EExtClauseKind* pDialectDefaults = NULL;
  int32_t               dialectNum = 0;
  if (TSDB_CODE_SUCCESS != extDialectGetDefaultClauseDemotions(dialect, &pDialectDefaults, &dialectNum)) {
    return false;
  }

  for (int32_t i = 0; i < dialectNum; ++i) {
    if (pDialectDefaults[i] == clause) {
      return false;
    }
  }

  return true;
}

int32_t extLoadDialectDefaults(int8_t sourceType, SExtSourceCapability* pCap) {
  if (NULL == pCap) {
    uError("extLoadDialectDefaults: pCap is NULL");
    return TSDB_CODE_INVALID_PARA;
  }

  if (pCap->loadedAtMs > 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t dialect = EXT_SQL_DIALECT_MYSQL;
  int32_t code = extResolveDialectBySource(sourceType, &dialect);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  pCap->dialect = dialect;

  // -- Clause defaults ---------------------------------------------------------
  SExtCapDemoteSpec spec = {.kind = EXT_DEMOTE_KIND_CLAUSE, .reasonCode = EXT_DEMOTE_REASON_DIALECT_BASELINE};

  const EExtClauseKind* pCommonClauses = NULL;
  int32_t               commonClauseNum = 0;
  code = extDialectGetCommonClauseDemotions(&pCommonClauses, &commonClauseNum);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  for (int32_t i = 0; i < commonClauseNum; ++i) {
    spec.detail.clauseKind = pCommonClauses[i];
    code = extCapDemoteApply(pCap, &spec);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  const EExtClauseKind* pDialectClauses = NULL;
  int32_t               dialectClauseNum = 0;
  code = extDialectGetDefaultClauseDemotions(dialect, &pDialectClauses, &dialectClauseNum);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  for (int32_t i = 0; i < dialectClauseNum; ++i) {
    spec.detail.clauseKind = pDialectClauses[i];
    code = extCapDemoteApply(pCap, &spec);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  // -- Function defaults ---------------------------------------------------------
  SExtCapDemoteSpec funcSpec = {.kind = EXT_DEMOTE_KIND_FUNC, .reasonCode = EXT_DEMOTE_REASON_DIALECT_BASELINE};

  const char* const* ppCommonFuncs = NULL;
  int32_t            commonFuncNum = 0;
  code = extDialectGetCommonFuncDemotions(&ppCommonFuncs, &commonFuncNum);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  for (int32_t i = 0; i < commonFuncNum; ++i) {
    tstrncpy(funcSpec.detail.funcName, ppCommonFuncs[i], sizeof(funcSpec.detail.funcName));
    code = extCapDemoteApply(pCap, &funcSpec);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  const char* const* ppDialectFuncs = NULL;
  int32_t            dialectFuncNum = 0;
  code = extDialectGetDefaultFuncDemotions(dialect, &ppDialectFuncs, &dialectFuncNum);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  for (int32_t i = 0; i < dialectFuncNum; ++i) {
    tstrncpy(funcSpec.detail.funcName, ppDialectFuncs[i], sizeof(funcSpec.detail.funcName));
    code = extCapDemoteApply(pCap, &funcSpec);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  // -- Operator defaults ---------------------------------------------------------
  SExtCapDemoteSpec opSpec = {.kind = EXT_DEMOTE_KIND_OP, .reasonCode = EXT_DEMOTE_REASON_DIALECT_BASELINE};

  const EOperatorType* pDialectOps = NULL;
  int32_t              dialectOpNum = 0;
  code = extDialectGetDefaultOpDemotions(dialect, &pDialectOps, &dialectOpNum);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  for (int32_t i = 0; i < dialectOpNum; ++i) {
    opSpec.detail.opType = pDialectOps[i];
    code = extCapDemoteApply(pCap, &opSpec);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  pCap->loadedAtMs = taosGetTimestampMs();
  return TSDB_CODE_SUCCESS;
}
