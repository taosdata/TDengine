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

#include <ctype.h>
#include <string.h>

#include "os.h"
#include "taoserror.h"
#include "tmsg.h"
#include "ttime.h"

#define EXT_CAP_HASH_INIT_SIZE 32U

static int32_t extCapInitHash(SHashObj** ppHash, int32_t keyType) {
  if (NULL == ppHash) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (NULL != *ppHash) {
    return TSDB_CODE_SUCCESS;
  }

  SHashObj* pHash = taosHashInit(EXT_CAP_HASH_INIT_SIZE, taosGetDefaultHashFunction(keyType), true, HASH_NO_LOCK);
  if (NULL == pHash) {
    return terrno;
  }

  *ppHash = pHash;
  return TSDB_CODE_SUCCESS;
}

static void extCapDestroyHash(SHashObj** ppHash) {
  if (NULL != ppHash && NULL != *ppHash) {
    taosHashCleanup(*ppHash);
    *ppHash = NULL;
  }
}

static int32_t extCapCloneHash(SHashObj* pSrc, SHashObj** ppDst, int32_t keyType) {
  if (NULL == ppDst) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppDst = NULL;

  if (NULL == pSrc) {
    return TSDB_CODE_SUCCESS;
  }

  uint32_t hashSize = (uint32_t)taosHashGetSize(pSrc);
  if (hashSize < EXT_CAP_HASH_INIT_SIZE) {
    hashSize = EXT_CAP_HASH_INIT_SIZE;
  }

  SHashObj* pDst = taosHashInit(hashSize, taosGetDefaultHashFunction(keyType), true, HASH_NO_LOCK);
  if (NULL == pDst) {
    return terrno;
  }

  void* pIter = taosHashIterate(pSrc, NULL);
  while (NULL != pIter) {
    size_t keyLen = 0;
    void*  pKey = taosHashGetKey(pIter, &keyLen);
    int32_t valueLen = taosHashGetValueSize(pIter);
    if (NULL == pKey || 0 == keyLen || valueLen <= 0) {
      taosHashCleanup(pDst);
      return TSDB_CODE_INVALID_PARA;
    }

    if (0 != taosHashPut(pDst, pKey, keyLen, pIter, (size_t)valueLen)) {
      taosHashCleanup(pDst);
      return terrno;
    }

    pIter = taosHashIterate(pSrc, pIter);
  }

  *ppDst = pDst;
  return TSDB_CODE_SUCCESS;
}

static void extCapNormalizeFuncName(const char* pIn, char* pOut, int32_t outLen) {
  if (NULL == pOut || outLen <= 0) {
    return;
  }

  pOut[0] = '\0';
  if (NULL == pIn) {
    return;
  }

  int32_t i = 0;
  for (; i < outLen - 1 && pIn[i] != '\0'; ++i) {
    pOut[i] = (char)tolower((unsigned char)pIn[i]);
  }
  pOut[i] = '\0';
}

static SHashObj** extCapGetHashByKind(SExtSourceCapability* pCap, EExtCapDemoteKind kind) {
  if (NULL == pCap) {
    return NULL;
  }

  switch (kind) {
    case EXT_DEMOTE_KIND_FUNC:
      return &pCap->disabledFuncs;
    case EXT_DEMOTE_KIND_OP:
      return &pCap->disabledOps;
    case EXT_DEMOTE_KIND_CLAUSE:
      return &pCap->disabledClauses;
    case EXT_DEMOTE_KIND_TYPE:
      return &pCap->disabledTypes;
    default:
      return NULL;
  }
}

static SHashObj** extCapGetOverlayHashByKind(SExtCapSessionOverlay* pOverlay, EExtCapDemoteKind kind) {
  if (NULL == pOverlay) {
    return NULL;
  }

  switch (kind) {
    case EXT_DEMOTE_KIND_FUNC:
      return &pOverlay->disabledFuncs;
    case EXT_DEMOTE_KIND_OP:
      return &pOverlay->disabledOps;
    case EXT_DEMOTE_KIND_CLAUSE:
      return &pOverlay->disabledClauses;
    case EXT_DEMOTE_KIND_TYPE:
      return &pOverlay->disabledTypes;
    default:
      return NULL;
  }
}

static int32_t extCapHashKeyType(EExtCapDemoteKind kind) {
  switch (kind) {
    case EXT_DEMOTE_KIND_FUNC:
      return TSDB_DATA_TYPE_BINARY;
    case EXT_DEMOTE_KIND_OP:
    case EXT_DEMOTE_KIND_CLAUSE:
      return TSDB_DATA_TYPE_INT;
    case EXT_DEMOTE_KIND_TYPE:
      return TSDB_DATA_TYPE_TINYINT;
    default:
      return TSDB_DATA_TYPE_BINARY;
  }
}

static int32_t extCapBuildKey(EExtCapDemoteKind kind, const void* pKey, void** ppNormKey, int32_t* pKeyLen,
                              char* pFnBuf, int32_t fnBufLen) {
  if (NULL == pKey || NULL == pKeyLen || NULL == ppNormKey) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppNormKey = (void*)pKey;
  switch (kind) {
    case EXT_DEMOTE_KIND_FUNC: {
      if (NULL == pFnBuf || fnBufLen <= 0) {
        return TSDB_CODE_INVALID_PARA;
      }
      extCapNormalizeFuncName((const char*)pKey, pFnBuf, fnBufLen);
      if ('\0' == pFnBuf[0]) {
        return TSDB_CODE_INVALID_PARA;
      }
      *ppNormKey = pFnBuf;
      *pKeyLen = (int32_t)strlen(pFnBuf) + 1;
      return TSDB_CODE_SUCCESS;
    }
    case EXT_DEMOTE_KIND_OP:
      *pKeyLen = (int32_t)sizeof(EOperatorType);
      return TSDB_CODE_SUCCESS;
    case EXT_DEMOTE_KIND_CLAUSE:
      *pKeyLen = (int32_t)sizeof(EExtClauseKind);
      return TSDB_CODE_SUCCESS;
    case EXT_DEMOTE_KIND_TYPE:
      *pKeyLen = (int32_t)sizeof(int8_t);
      return TSDB_CODE_SUCCESS;
    default:
      return TSDB_CODE_INVALID_PARA;
  }
}

static int32_t extCapUpsertDisabledEntry(SHashObj** ppHash, EExtCapDemoteKind kind, const void* pKey,
                                         uint8_t reasonCode) {
  if (NULL == ppHash || NULL == pKey || EXT_DEMOTE_KIND_NONE == kind) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = extCapInitHash(ppHash, extCapHashKeyType(kind));
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  char  normalizedName[TSDB_FUNC_NAME_LEN] = {0};
  void* pNormKey = NULL;
  int32_t keyLen = 0;
  code = extCapBuildKey(kind, pKey, &pNormKey, &keyLen, normalizedName, sizeof(normalizedName));
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  SExtCapDisabledEntry entry = {0};
  SExtCapDisabledEntry* pExisting = (SExtCapDisabledEntry*)taosHashGet(*ppHash, pNormKey, keyLen);
  if (NULL != pExisting) {
    entry = *pExisting;
  }

  if (entry.hitCount < UINT16_MAX) {
    entry.hitCount += 1;
  }
  if (0 == entry.firstHitMs) {
    entry.firstHitMs = taosGetTimestampMs();
  }
  entry.reasonCode = reasonCode;

  if (0 != taosHashPut(*ppHash, pNormKey, keyLen, &entry, sizeof(entry))) {
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t extCapInit(SExtSourceCapability* pCap) {
  if (NULL == pCap) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(pCap, 0, sizeof(*pCap));
  return TSDB_CODE_SUCCESS;
}

void extCapDestroy(SExtSourceCapability* pCap) {
  if (NULL == pCap) {
    return;
  }

  extCapDestroyHash(&pCap->disabledFuncs);
  extCapDestroyHash(&pCap->disabledOps);
  extCapDestroyHash(&pCap->disabledTypes);
  extCapDestroyHash(&pCap->disabledClauses);
}

int32_t extCapClone(const SExtSourceCapability* pSrc, SExtSourceCapability* pDst) {
  if (NULL == pSrc || NULL == pDst) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(pDst, 0, sizeof(*pDst));

  pDst->dialect = pSrc->dialect;
  pDst->loadedAtMs = pSrc->loadedAtMs;

  int32_t code = extCapCloneHash(pSrc->disabledFuncs, &pDst->disabledFuncs, TSDB_DATA_TYPE_BINARY);
  if (TSDB_CODE_SUCCESS != code) goto _error;
  code = extCapCloneHash(pSrc->disabledOps, &pDst->disabledOps, TSDB_DATA_TYPE_INT);
  if (TSDB_CODE_SUCCESS != code) goto _error;
  code = extCapCloneHash(pSrc->disabledTypes, &pDst->disabledTypes, TSDB_DATA_TYPE_TINYINT);
  if (TSDB_CODE_SUCCESS != code) goto _error;
  code = extCapCloneHash(pSrc->disabledClauses, &pDst->disabledClauses, TSDB_DATA_TYPE_INT);
  if (TSDB_CODE_SUCCESS != code) goto _error;

  return TSDB_CODE_SUCCESS;

_error:
  extCapDestroy(pDst);
  return code;
}

int32_t extCapSessionOverlayInit(SExtCapSessionOverlay* pOverlay) {
  if (NULL == pOverlay) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(pOverlay, 0, sizeof(*pOverlay));
  return TSDB_CODE_SUCCESS;
}

void extCapSessionOverlayDestroy(SExtCapSessionOverlay* pOverlay) {
  if (NULL == pOverlay) {
    return;
  }

  extCapDestroyHash(&pOverlay->disabledFuncs);
  extCapDestroyHash(&pOverlay->disabledOps);
  extCapDestroyHash(&pOverlay->disabledTypes);
  extCapDestroyHash(&pOverlay->disabledClauses);
}

int32_t extCapSessionOverlayClone(const SExtCapSessionOverlay* pSrc, SExtCapSessionOverlay* pDst) {
  if (NULL == pSrc || NULL == pDst) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(pDst, 0, sizeof(*pDst));
  pDst->attemptCount = pSrc->attemptCount;
  pDst->lastDemoteKind = pSrc->lastDemoteKind;
  pDst->lastDemoteDetail = pSrc->lastDemoteDetail;
  pDst->repeatFailCount = pSrc->repeatFailCount;

  int32_t code = extCapCloneHash(pSrc->disabledFuncs, &pDst->disabledFuncs, TSDB_DATA_TYPE_BINARY);
  if (TSDB_CODE_SUCCESS != code) goto _error;
  code = extCapCloneHash(pSrc->disabledOps, &pDst->disabledOps, TSDB_DATA_TYPE_INT);
  if (TSDB_CODE_SUCCESS != code) goto _error;
  code = extCapCloneHash(pSrc->disabledTypes, &pDst->disabledTypes, TSDB_DATA_TYPE_TINYINT);
  if (TSDB_CODE_SUCCESS != code) goto _error;
  code = extCapCloneHash(pSrc->disabledClauses, &pDst->disabledClauses, TSDB_DATA_TYPE_INT);
  if (TSDB_CODE_SUCCESS != code) goto _error;

  return TSDB_CODE_SUCCESS;

_error:
  extCapSessionOverlayDestroy(pDst);
  return code;
}

bool capDisabled(const SExtSourceCapability* pCap, const SExtCapSessionOverlay* pOverlay,
                 EExtCapDemoteKind kind, const void* pKey) {
  if (EXT_DEMOTE_KIND_NONE == kind || NULL == pKey) {
    return false;
  }

  char  normalizedName[TSDB_FUNC_NAME_LEN] = {0};
  void* pNormKey = NULL;
  int32_t keyLen = 0;
  if (TSDB_CODE_SUCCESS != extCapBuildKey(kind, pKey, &pNormKey, &keyLen, normalizedName, sizeof(normalizedName))) {
    return false;
  }

  if (NULL != pOverlay) {
    SHashObj** ppHash = extCapGetOverlayHashByKind((SExtCapSessionOverlay*)pOverlay, kind);
    if (NULL != ppHash && NULL != *ppHash && NULL != taosHashGet(*ppHash, pNormKey, keyLen)) {
      return true;
    }
  }

  if (NULL != pCap) {
    SHashObj** ppHash = extCapGetHashByKind((SExtSourceCapability*)pCap, kind);
    if (NULL != ppHash && NULL != *ppHash && NULL != taosHashGet(*ppHash, pNormKey, keyLen)) {
      return true;
    }
  }

  return false;
}

int32_t extCapDemoteApply(SExtSourceCapability* pCap, const SExtCapDemoteSpec* pSpec) {
  if (NULL == pCap || NULL == pSpec) {
    return TSDB_CODE_INVALID_PARA;
  }

  switch (pSpec->kind) {
    case EXT_DEMOTE_KIND_FUNC:
      return extCapUpsertDisabledEntry(&pCap->disabledFuncs, pSpec->kind, pSpec->detail.funcName, pSpec->reasonCode);
    case EXT_DEMOTE_KIND_OP:
      return extCapUpsertDisabledEntry(&pCap->disabledOps, pSpec->kind, &pSpec->detail.opType, pSpec->reasonCode);
    case EXT_DEMOTE_KIND_CLAUSE:
      return extCapUpsertDisabledEntry(&pCap->disabledClauses, pSpec->kind, &pSpec->detail.clauseKind,
                                       pSpec->reasonCode);
    case EXT_DEMOTE_KIND_TYPE:
      return extCapUpsertDisabledEntry(&pCap->disabledTypes, pSpec->kind, &pSpec->detail.tsdbTypeCode,
                                       pSpec->reasonCode);
    default:
      return TSDB_CODE_INVALID_PARA;
  }
}

int32_t extCapOverlayDemote(SExtCapSessionOverlay* pOverlay, const SExtCapDemoteSpec* pSpec) {
  if (NULL == pOverlay || NULL == pSpec) {
    return TSDB_CODE_INVALID_PARA;
  }

  switch (pSpec->kind) {
    case EXT_DEMOTE_KIND_FUNC:
      return extCapUpsertDisabledEntry(&pOverlay->disabledFuncs, pSpec->kind, pSpec->detail.funcName,
                                       pSpec->reasonCode);
    case EXT_DEMOTE_KIND_OP:
      return extCapUpsertDisabledEntry(&pOverlay->disabledOps, pSpec->kind, &pSpec->detail.opType,
                                       pSpec->reasonCode);
    case EXT_DEMOTE_KIND_CLAUSE:
      return extCapUpsertDisabledEntry(&pOverlay->disabledClauses, pSpec->kind, &pSpec->detail.clauseKind,
                                       pSpec->reasonCode);
    case EXT_DEMOTE_KIND_TYPE:
      return extCapUpsertDisabledEntry(&pOverlay->disabledTypes, pSpec->kind, &pSpec->detail.tsdbTypeCode,
                                       pSpec->reasonCode);
    default:
      return TSDB_CODE_INVALID_PARA;
  }
}

static bool extCapOverlayIsSameDemote(const SExtCapSessionOverlay* pOverlay, const SExtCapDemoteSpec* pSpec) {
  if (NULL == pOverlay || NULL == pSpec || EXT_DEMOTE_KIND_NONE == pSpec->kind) {
    return false;
  }

  if (pOverlay->lastDemoteKind != pSpec->kind) {
    return false;
  }

  switch (pSpec->kind) {
    case EXT_DEMOTE_KIND_FUNC: {
      char normalizedName[TSDB_FUNC_NAME_LEN] = {0};
      extCapNormalizeFuncName(pSpec->detail.funcName, normalizedName, sizeof(normalizedName));
      return 0 == strcmp(pOverlay->lastDemoteDetail.funcName, normalizedName);
    }
    case EXT_DEMOTE_KIND_OP:
      return pOverlay->lastDemoteDetail.opType == pSpec->detail.opType;
    case EXT_DEMOTE_KIND_CLAUSE:
      return pOverlay->lastDemoteDetail.clauseKind == pSpec->detail.clauseKind;
    case EXT_DEMOTE_KIND_TYPE:
      return pOverlay->lastDemoteDetail.tsdbTypeCode == pSpec->detail.tsdbTypeCode;
    default:
      return false;
  }
}

static void extCapOverlayRememberDemote(SExtCapSessionOverlay* pOverlay, const SExtCapDemoteSpec* pSpec) {
  if (NULL == pOverlay || NULL == pSpec || EXT_DEMOTE_KIND_NONE == pSpec->kind) {
    return;
  }

  bool isSame = extCapOverlayIsSameDemote(pOverlay, pSpec);
  if (isSame) {
    if (pOverlay->repeatFailCount < UINT8_MAX) {
      pOverlay->repeatFailCount += 1;
    }
  } else {
    pOverlay->repeatFailCount = 1;
  }

  pOverlay->lastDemoteKind = pSpec->kind;
  switch (pSpec->kind) {
    case EXT_DEMOTE_KIND_FUNC:
      extCapNormalizeFuncName(pSpec->detail.funcName, pOverlay->lastDemoteDetail.funcName,
                              sizeof(pOverlay->lastDemoteDetail.funcName));
      break;
    case EXT_DEMOTE_KIND_OP:
      pOverlay->lastDemoteDetail.opType = pSpec->detail.opType;
      break;
    case EXT_DEMOTE_KIND_CLAUSE:
      pOverlay->lastDemoteDetail.clauseKind = pSpec->detail.clauseKind;
      break;
    case EXT_DEMOTE_KIND_TYPE:
      pOverlay->lastDemoteDetail.tsdbTypeCode = pSpec->detail.tsdbTypeCode;
      break;
    default:
      break;
  }
}

bool extCapOverlayTryEscalateOnRepeat(SExtCapSessionOverlay* pOverlay, const SExtCapDemoteSpec* pCurrent,
                                      SExtCapDemoteSpec* pEscalated) {
  if (NULL == pOverlay || NULL == pCurrent || NULL == pEscalated || EXT_DEMOTE_KIND_NONE == pCurrent->kind) {
    return false;
  }

  extCapOverlayRememberDemote(pOverlay, pCurrent);
  if (pOverlay->repeatFailCount < 2) {
    return false;
  }

  memset(pEscalated, 0, sizeof(*pEscalated));
  pEscalated->kind = EXT_DEMOTE_KIND_CLAUSE;
  pEscalated->reasonCode = EXT_DEMOTE_REASON_RETRY_ESCALATION;

  switch (pCurrent->kind) {
    case EXT_DEMOTE_KIND_FUNC:
      pEscalated->detail.clauseKind = extResolveFuncClause(pCurrent->detail.funcName);
      return true;
    case EXT_DEMOTE_KIND_OP:
      pEscalated->detail.clauseKind = extResolveOpClause(pCurrent->detail.opType);
      return true;
    case EXT_DEMOTE_KIND_TYPE:
      pEscalated->detail.clauseKind = EXT_CLAUSE_FILTER;
      return true;
    default:
      return false;
  }
}

void extCapOverlayMarkAllCoarse(SExtCapSessionOverlay* pOverlay, uint8_t reasonCode) {
  if (NULL == pOverlay) {
    return;
  }

  SExtCapDemoteSpec spec = {.kind = EXT_DEMOTE_KIND_CLAUSE, .reasonCode = reasonCode};
  spec.detail.clauseKind = EXT_CLAUSE_FILTER;
  (void)extCapOverlayDemote(pOverlay, &spec);
  spec.detail.clauseKind = EXT_CLAUSE_PROJECTION;
  (void)extCapOverlayDemote(pOverlay, &spec);
  spec.detail.clauseKind = EXT_CLAUSE_AGG;
  (void)extCapOverlayDemote(pOverlay, &spec);
  spec.detail.clauseKind = EXT_CLAUSE_ORDER;
  (void)extCapOverlayDemote(pOverlay, &spec);
  spec.detail.clauseKind = EXT_CLAUSE_LIMIT;
  (void)extCapOverlayDemote(pOverlay, &spec);
  spec.detail.clauseKind = EXT_CLAUSE_JOIN;
  (void)extCapOverlayDemote(pOverlay, &spec);
}

bool extCapShouldUseBaseline(const SExtSourceCapability* pCap, const SExtCapSessionOverlay* pOverlay) {
  if (NULL == pCap) {
    return true;
  }

  if (NULL == pOverlay) {
    return false;
  }

  if (pOverlay->attemptCount >= EXT_MAX_PUSHDOWN_ATTEMPTS) {
    return true;
  }

  return capDisabled(NULL, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &(EExtClauseKind){EXT_CLAUSE_FILTER}) &&
         capDisabled(NULL, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &(EExtClauseKind){EXT_CLAUSE_PROJECTION}) &&
         capDisabled(NULL, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &(EExtClauseKind){EXT_CLAUSE_AGG}) &&
         capDisabled(NULL, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &(EExtClauseKind){EXT_CLAUSE_ORDER}) &&
         capDisabled(NULL, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &(EExtClauseKind){EXT_CLAUSE_LIMIT}) &&
         capDisabled(NULL, pOverlay, EXT_DEMOTE_KIND_CLAUSE, &(EExtClauseKind){EXT_CLAUSE_JOIN});
}

EExtClauseKind extResolveFuncClause(const char* funcName) {
  char lowerName[TSDB_FUNC_NAME_LEN] = {0};
  extCapNormalizeFuncName(funcName, lowerName, sizeof(lowerName));

  if ('\0' == lowerName[0]) {
    return EXT_CLAUSE_FILTER;
  }

  if (0 == strcmp(lowerName, "sum") || 0 == strcmp(lowerName, "count") || 0 == strcmp(lowerName, "avg") ||
      0 == strcmp(lowerName, "min") || 0 == strcmp(lowerName, "max")) {
    return EXT_CLAUSE_AGG;
  }

  // Time-bucketing functions (date_bin, date_trunc) are typically used in GROUP BY
  // for time-series aggregation; escalate to the AGG gate.
  if (0 == strcmp(lowerName, "date_bin") || 0 == strcmp(lowerName, "date_trunc")) {
    return EXT_CLAUSE_AGG;
  }

  // Scalar extraction / transformation functions used in SELECT; escalate to PROJECTION.
  if (0 == strcmp(lowerName, "json_extract") || 0 == strcmp(lowerName, "from_unixtime_floor")) {
    return EXT_CLAUSE_PROJECTION;
  }

  // regexp_like is a predicate used in WHERE; escalate to FILTER.
  if (0 == strcmp(lowerName, "regexp_like")) {
    return EXT_CLAUSE_FILTER;
  }

  return EXT_CLAUSE_FILTER;
}

EExtClauseKind extResolveOpClause(EOperatorType opType) {
  switch (opType) {
    case OP_TYPE_IN:
    case OP_TYPE_NOT_IN:
      // IN / NOT IN are filter predicates; individual function-level tracking is
      // handled at the OP layer (EXT_DEMOTE_KIND_OP).  Escalation goes to FILTER.
      return EXT_CLAUSE_FILTER;
    case OP_TYPE_EXISTS:
    case OP_TYPE_NOT_EXISTS:
      // EXISTS / NOT EXISTS are also filter predicates; escalate to FILTER.
      return EXT_CLAUSE_FILTER;
    case OP_TYPE_GREATER_THAN:
    case OP_TYPE_GREATER_EQUAL:
    case OP_TYPE_LOWER_THAN:
    case OP_TYPE_LOWER_EQUAL:
    case OP_TYPE_EQUAL:
    case OP_TYPE_NOT_EQUAL:
    case OP_TYPE_LIKE:
    case OP_TYPE_NOT_LIKE:
    case OP_TYPE_MATCH:
    case OP_TYPE_NMATCH:
      return EXT_CLAUSE_FILTER;
    default:
      return EXT_CLAUSE_PROJECTION;
  }
}

bool extClauseIsGate(EExtClauseKind kind) {
  // Returns true only for the 6 GATE values that participate in 3-level fallback.
  // STATIC values (dialect defaults / manual SQL only) must not pass through the
  // error-based demote pipeline.
  switch (kind) {
    case EXT_CLAUSE_FILTER:
    case EXT_CLAUSE_PROJECTION:
    case EXT_CLAUSE_AGG:
    case EXT_CLAUSE_ORDER:
    case EXT_CLAUSE_LIMIT:
    case EXT_CLAUSE_JOIN:
      return true;
    default:
      return false;
  }
}

int32_t tEncodeSExtPushdownDiagnostic(SEncoder* pEncoder, const SExtPushdownDiagnostic* pDiag) {
  if (NULL == pEncoder || NULL == pDiag) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pDiag->classifiedCode));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, (int8_t)pDiag->reasonCode));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, (int8_t)pDiag->suggestedDemoteKind));

  switch (pDiag->suggestedDemoteKind) {
    case EXT_DEMOTE_KIND_FUNC:
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pDiag->detail.funcName));
      break;
    case EXT_DEMOTE_KIND_OP:
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, (int32_t)pDiag->detail.opType));
      break;
    case EXT_DEMOTE_KIND_CLAUSE:
      TAOS_CHECK_EXIT(tEncodeI32(pEncoder, (int32_t)pDiag->detail.clauseKind));
      break;
    case EXT_DEMOTE_KIND_TYPE:
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pDiag->detail.tsdbTypeCode));
      break;
    default:
      break;
  }

  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pDiag->remoteSqlState));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pDiag->remoteCode));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pDiag->httpStatus));
  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pDiag->remoteMessage));
  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pDiag->generatedSql));

_exit:
  return code;
}

int32_t tDecodeSExtPushdownDiagnostic(SDecoder* pDecoder, SExtPushdownDiagnostic* pDiag) {
  if (NULL == pDecoder || NULL == pDiag) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  memset(pDiag, 0, sizeof(*pDiag));

  int8_t reasonCode = 0;
  int8_t demoteKind = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pDiag->classifiedCode));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &reasonCode));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &demoteKind));
  pDiag->reasonCode = (EExtDemoteReason)reasonCode;
  pDiag->suggestedDemoteKind = (EExtCapDemoteKind)demoteKind;

  switch (pDiag->suggestedDemoteKind) {
    case EXT_DEMOTE_KIND_FUNC:
      TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pDiag->detail.funcName));
      break;
    case EXT_DEMOTE_KIND_OP: {
      int32_t op = 0;
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &op));
      pDiag->detail.opType = (EOperatorType)op;
      break;
    }
    case EXT_DEMOTE_KIND_CLAUSE: {
      int32_t clause = 0;
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &clause));
      pDiag->detail.clauseKind = (EExtClauseKind)clause;
      break;
    }
    case EXT_DEMOTE_KIND_TYPE:
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pDiag->detail.tsdbTypeCode));
      break;
    default:
      break;
  }

  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pDiag->remoteSqlState));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pDiag->remoteCode));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pDiag->httpStatus));
  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pDiag->remoteMessage));
  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pDiag->generatedSql));

_exit:
  return code;
}

// -- FQ operator push-down capability bitmap ----------------------------------
// Indexed by EOperatorType.  Entries for values not in the enum are FQ_CAP_NONE.
// OP_TYPE_ASSIGN = 200 is the highest value; size = 201.
#define FQ_OP_CAP_TABLE_SIZE 201

static const uint16_t kFqOpCap[FQ_OP_CAP_TABLE_SIZE] = {
  // [0] - not a valid operator
  [0]                         = FQ_CAP_NONE,

  // Arithmetic operators - all dialects
  // SQL Server bits are OR-ed per entry (DS 6.2.2: never widen the shared
  // FQ_CAP_ALL macro; phase 2 turns the bits on entry by entry).
  [OP_TYPE_ADD]               = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_SUB]               = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_MULTI]             = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_DIV]               = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_REM]               = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_MINUS]             = FQ_CAP_ALL | FQ_CAP_SQLSERVER,

  // Bitwise operators - MySQL and PG support natively; InfluxDB v3 does not
  [OP_TYPE_BIT_AND]           = FQ_CAP_MYSQL_PG | FQ_CAP_SQLSERVER,
  [OP_TYPE_BIT_OR]            = FQ_CAP_MYSQL_PG | FQ_CAP_SQLSERVER,

  // Comparison operators - all dialects
  [OP_TYPE_GREATER_THAN]      = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_GREATER_EQUAL]     = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_LOWER_THAN]        = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_LOWER_EQUAL]       = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_EQUAL]             = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_NOT_EQUAL]         = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_IN]                = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_NOT_IN]            = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_LIKE]              = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_NOT_LIKE]          = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  // MATCH/NMATCH (regex): MySQL and PG support; InfluxDB DataFusion does not;
  // SQL Server 2019/2022 has no regex operator (local).
  [OP_TYPE_MATCH]             = FQ_CAP_MYSQL_PG,
  [OP_TYPE_NMATCH]            = FQ_CAP_MYSQL_PG,

  // Null/bool checks - all dialects
  [OP_TYPE_IS_NULL]           = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_IS_NOT_NULL]       = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_IS_TRUE]           = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_IS_FALSE]          = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_IS_UNKNOWN]        = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_IS_NOT_TRUE]       = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_IS_NOT_FALSE]      = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [OP_TYPE_IS_NOT_UNKNOWN]    = FQ_CAP_ALL | FQ_CAP_SQLSERVER,

  // EXISTS/NOT EXISTS: MySQL and PG support correlated subqueries; InfluxDB does not
  [OP_TYPE_EXISTS]            = FQ_CAP_MYSQL_PG,
  [OP_TYPE_NOT_EXISTS]        = FQ_CAP_MYSQL_PG,

  // JSON operators: MySQL and PG support JSON path; InfluxDB local
  [OP_TYPE_JSON_GET_VALUE]    = FQ_CAP_MYSQL_PG,
  [OP_TYPE_JSON_CONTAINS]     = FQ_CAP_MYSQL_PG,

  // OP_TYPE_ASSIGN (200) - internal TDengine operator, never pushed
  [OP_TYPE_ASSIGN]            = FQ_CAP_NONE,
};

// -- FQ clause push-down capability bitmap ------------------------------------
// Indexed by EExtClauseKind.
static const uint16_t kFqClauseCap[EXT_CLAUSE_MAX] = {
  // Core push-down gates - all dialects support these.
  // SQL Server bits are OR-ed entry by entry (DS 6.2.2).  Phase 2 M1 opens
  // FILTER + PROJECTION, M2 adds the aggregate clauses (AGG / HAVING /
  // DISTINCT); the remaining clauses follow once their rendering exists
  // (TOP/OFFSET-FETCH, NULLS rewriting, ...).
  [EXT_CLAUSE_FILTER]         = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [EXT_CLAUSE_PROJECTION]     = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [EXT_CLAUSE_AGG]            = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [EXT_CLAUSE_ORDER]          = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [EXT_CLAUSE_LIMIT]          = FQ_CAP_ALL | FQ_CAP_SQLSERVER,   // SQL Server: TOP / OFFSET ... FETCH NEXT
  [EXT_CLAUSE_JOIN]           = FQ_CAP_ALL | FQ_CAP_SQLSERVER,   // All dialects support JOIN (InfluxDB v3 DataFusion included)

  // Static feature flags
  [EXT_CLAUSE_GROUP_ROLLUP]      = FQ_CAP_NONE,   // no dialect supports ROLLUP push
  [EXT_CLAUSE_HAVING]            = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [EXT_CLAUSE_ORDER_NULLS_FIRST] = FQ_CAP_ALL | FQ_CAP_SQLSERVER,  // MySQL: ISNULL() workaround; PG/Influx: native; SQL Server: CASE prefix key
  [EXT_CLAUSE_ORDER_NULLS_LAST]  = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [EXT_CLAUSE_OFFSET]            = FQ_CAP_ALL | FQ_CAP_SQLSERVER,
  [EXT_CLAUSE_DISTINCT]          = FQ_CAP_ALL | FQ_CAP_SQLSERVER,

  // JOIN subtypes
  [EXT_CLAUSE_INNER_JOIN]        = FQ_CAP_ALL | FQ_CAP_SQLSERVER,  // All dialects support INNER JOIN
  [EXT_CLAUSE_LEFT_JOIN]         = FQ_CAP_ALL | FQ_CAP_SQLSERVER,  // All dialects support LEFT OUTER JOIN
  [EXT_CLAUSE_RIGHT_JOIN]        = FQ_CAP_ALL | FQ_CAP_SQLSERVER,  // All dialects support RIGHT OUTER JOIN
  [EXT_CLAUSE_FULL_OUTER_JOIN]   = FQ_CAP_PG_INFLUX | FQ_CAP_SQLSERVER, // MySQL has no FULL OUTER JOIN; PG/InfluxDB v3 and T-SQL support it natively
  [EXT_CLAUSE_SEMI_JOIN]         = FQ_CAP_MYSQL_PG | FQ_CAP_SQLSERVER, // DS section5.3.6.1.4: MySQL/PG via EXISTS; T-SQL EXISTS; InfluxDB: no correlated subquery
  [EXT_CLAUSE_ANTI_JOIN]         = FQ_CAP_MYSQL_PG | FQ_CAP_SQLSERVER, // DS section5.3.6.1.5: MySQL/PG via NOT EXISTS; T-SQL NOT EXISTS; InfluxDB: no correlated subquery

  // Subquery types
  [EXT_CLAUSE_FROM_SUBQUERY]     = FQ_CAP_ALL | FQ_CAP_SQLSERVER,  // DS section5.3.7.1.1: all dialects support nested FROM subquery push; T-SQL derived table (alias required)
  [EXT_CLAUSE_SCALAR_SUBQUERY]   = FQ_CAP_ALL | FQ_CAP_SQLSERVER,  // all dialects: resolved TDengine-side, emitted as literal; T-SQL scalar subquery
  [EXT_CLAUSE_IN_SUBQUERY]       = FQ_CAP_MYSQL_PG | FQ_CAP_SQLSERVER, // InfluxDB: no IN (subquery); T-SQL native IN (subquery)
  [EXT_CLAUSE_ALL_ANY_SUBQUERY]  = FQ_CAP_MYSQL_PG | FQ_CAP_SQLSERVER, // MySQL/PG: op ALL|ANY (subquery); InfluxDB: no correlated subquery; T-SQL SOME == ANY
  [EXT_CLAUSE_EXISTS_SUBQUERY]   = FQ_CAP_MYSQL_PG | FQ_CAP_SQLSERVER, // MySQL/PG: [NOT] EXISTS (subquery); InfluxDB: no EXISTS subquery; T-SQL native

  // INTERVAL
  [EXT_CLAUSE_INTERVAL_TUMBLING] = FQ_CAP_ALL | FQ_CAP_SQLSERVER,  // MySQL: FROM_UNIXTIME(FLOOR(...)); PG: DATE_BIN; InfluxDB: DATE_BIN(INTERVAL); T-SQL: DATEADD+DATEDIFF_BIG
  [EXT_CLAUSE_INTERVAL_SLIDING]  = FQ_CAP_NONE,

  // PARTITION BY
  [EXT_CLAUSE_PARTITION_TBNAME]  = FQ_CAP_INFLUX,  // InfluxDB: tag-based GROUP BY; MySQL/PG: no supertable concept
  [EXT_CLAUSE_PARTITION_COLUMN]  = FQ_CAP_ALL | FQ_CAP_SQLSERVER,

  // Feature flags (connector-reported; default disabled)
  [EXT_CLAUSE_FEAT_CTE]              = FQ_CAP_NONE,
  [EXT_CLAUSE_FEAT_WINDOW_FUNCTIONS] = FQ_CAP_ALL | FQ_CAP_SQLSERVER,   // MySQL 8.0+/PG/InfluxDB v3/T-SQL: OVER() supported (e.g. LAG/LEAD)
  [EXT_CLAUSE_FEAT_LARGE_IN_LIST]    = FQ_CAP_NONE,

  // JOIN subtypes always local
  [EXT_CLAUSE_ASOF_JOIN]      = FQ_CAP_NONE,
  [EXT_CLAUSE_WINDOW_JOIN]    = FQ_CAP_NONE,

  // TDengine window types - always local
  [EXT_CLAUSE_STATE_WINDOW]   = FQ_CAP_NONE,
  [EXT_CLAUSE_SESSION_WINDOW] = FQ_CAP_NONE,
  [EXT_CLAUSE_EVENT_WINDOW]   = FQ_CAP_NONE,
  [EXT_CLAUSE_COUNT_WINDOW]   = FQ_CAP_NONE,
  [EXT_CLAUSE_FILL]           = FQ_CAP_NONE,
  [EXT_CLAUSE_INTERP]         = FQ_CAP_NONE,
  [EXT_CLAUSE_SLIMIT]         = FQ_CAP_NONE,
};

// Transform-pushdown capability (reserved).
// Current DS/product semantics do not ship transform-only subquery pushdown,
// so all subquery-clause entries stay FQ_CAP_NONE.  ORIG_TYPE_2B inner-body
// translation is controlled by planner-side render-mode selection, not by a
// transform-only clause capability matrix.
static const uint16_t kFqClauseTransformCap[EXT_CLAUSE_MAX] = {
  [EXT_CLAUSE_SCALAR_SUBQUERY]  = FQ_CAP_NONE,
  [EXT_CLAUSE_IN_SUBQUERY]      = FQ_CAP_NONE,
  [EXT_CLAUSE_ALL_ANY_SUBQUERY] = FQ_CAP_NONE,
  [EXT_CLAUSE_EXISTS_SUBQUERY]  = FQ_CAP_NONE,
};

uint16_t fqOpCapBitmap(EOperatorType opType) {
  int idx = (int)opType;
  if (idx < 0 || idx >= FQ_OP_CAP_TABLE_SIZE) return FQ_CAP_NONE;
  return kFqOpCap[idx];
}

uint16_t fqClauseCapBitmap(EExtClauseKind clauseKind) {
  int idx = (int)clauseKind;
  if (idx < 0 || idx >= (int)EXT_CLAUSE_MAX) return FQ_CAP_NONE;
  return kFqClauseCap[idx];
}

uint16_t fqClauseTransformCapBitmap(EExtClauseKind clauseKind) {
  int idx = (int)clauseKind;
  if (idx < 0 || idx >= (int)EXT_CLAUSE_MAX) return FQ_CAP_NONE;
  return kFqClauseTransformCap[idx];
}

bool fqOpIsPushable(EOperatorType opType, int32_t dialect,
                    const SExtCapSessionOverlay* pOverlay) {
  uint16_t cap = fqOpCapBitmap(opType);
  uint16_t bit = fqDialectBit(dialect);
  if (bit == 0) return false;
  if (pOverlay != NULL && ((uint16_t)(cap >> FQ_CAP_SESSION_SHIFT) & bit)) return false;
  return (cap & bit) != 0;
}

bool fqClauseIsPushable(EExtClauseKind clauseKind, int32_t dialect,
                        const SExtCapSessionOverlay* pOverlay) {
  uint16_t cap = fqClauseCapBitmap(clauseKind);
  uint16_t bit = fqDialectBit(dialect);
  if (bit == 0) return false;
  if (pOverlay != NULL && ((uint16_t)(cap >> FQ_CAP_SESSION_SHIFT) & bit)) return false;
  return (cap & bit) != 0;
}

bool fqClauseTransformIsPushable(EExtClauseKind clauseKind, int32_t dialect,
                                 const SExtCapSessionOverlay* pOverlay) {
  uint16_t cap = fqClauseTransformCapBitmap(clauseKind);
  uint16_t bit = fqDialectBit(dialect);
  if (bit == 0) return false;
  if (pOverlay != NULL && ((uint16_t)(cap >> FQ_CAP_SESSION_SHIFT) & bit)) return false;
  return (cap & bit) != 0;
}
