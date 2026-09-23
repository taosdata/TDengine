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

// -- Clause defaults ---------------------------------------------------------
// REMOVED: All clause-level capability is now expressed solely via the static
// kFqClauseCap[] bitmap matrix in extCapability.c.  The capDisabled() path
// remains for runtime dynamic demotion (catalog error-feedback) and session
// overlay, but dialect baseline clause demotions are no longer loaded here.

int32_t extDialectGetDefaultClauseDemotions(int32_t dialect, const EExtClauseKind** ppClauses, int32_t* pNum) {
  if (NULL == ppClauses || NULL == pNum) {
    return TSDB_CODE_INVALID_PARA;
  }
  (void)dialect;
  *ppClauses = NULL;
  *pNum = 0;
  return TSDB_CODE_SUCCESS;
}

int32_t extDialectGetCommonClauseDemotions(const EExtClauseKind** ppClauses, int32_t* pNum) {
  if (NULL == ppClauses || NULL == pNum) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppClauses = NULL;
  *pNum = 0;
  return TSDB_CODE_SUCCESS;
}

// -- Function defaults --------------------------------------------------------
// REMOVED: All function-level capability is now expressed solely via the static
// funcMgtBuiltins[].fqPushCap bitmap in builtins.c.  The capDisabled() path
// remains for runtime dynamic demotion (catalog error-feedback) and session
// overlay, but dialect baseline function demotions are no longer loaded here.

int32_t extDialectGetDefaultFuncDemotions(int32_t dialect, const char* const** pppFuncs, int32_t* pNum) {
  if (NULL == pppFuncs || NULL == pNum) {
    return TSDB_CODE_INVALID_PARA;
  }
  (void)dialect;
  *pppFuncs = NULL;
  *pNum = 0;
  return TSDB_CODE_SUCCESS;
}

int32_t extDialectGetCommonFuncDemotions(const char* const** pppFuncs, int32_t* pNum) {
  if (NULL == pppFuncs || NULL == pNum) {
    return TSDB_CODE_INVALID_PARA;
  }
  *pppFuncs = NULL;
  *pNum = 0;
  return TSDB_CODE_SUCCESS;
}

// -- Operator defaults --------------------------------------------------------
// REMOVED: All operator-level capability is now expressed solely via the static
// kFqOpCap[] bitmap matrix in extCapability.c.  The capDisabled() path remains
// for runtime dynamic demotion (catalog error-feedback) and session overlay,
// but dialect baseline operator demotions are no longer loaded here.

int32_t extDialectGetDefaultOpDemotions(int32_t dialect, const EOperatorType** ppOps, int32_t* pNum) {
  if (NULL == ppOps || NULL == pNum) {
    return TSDB_CODE_INVALID_PARA;
  }
  (void)dialect;
  *ppOps = NULL;
  *pNum = 0;
  return TSDB_CODE_SUCCESS;
}
