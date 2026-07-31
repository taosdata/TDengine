/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#include "extError.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "extConnector.h"
#include "osString.h"
#include "taoserror.h"

static bool extContainsNoCase(const char* text, const char* needle) {
  if (NULL == text || NULL == needle || '\0' == needle[0]) {
    return false;
  }

  size_t textLen = strlen(text);
  size_t needleLen = strlen(needle);
  if (textLen == 0 || needleLen == 0 || needleLen > textLen) {
    return false;
  }

  for (size_t i = 0; i + needleLen <= textLen; ++i) {
    size_t j = 0;
    for (; j < needleLen; ++j) {
      if (tolower((unsigned char)text[i + j]) != tolower((unsigned char)needle[j])) {
        break;
      }
    }
    if (j == needleLen) {
      return true;
    }
  }

  return false;
}

static bool extExtractTokenValue(const char* pMsg, const char* pToken, char* pBuf, int32_t bufLen) {
  if (NULL == pMsg || NULL == pToken || NULL == pBuf || bufLen <= 0) {
    return false;
  }

  pBuf[0] = '\0';
  const char* pPos = taosStrCaseStr(pMsg, pToken);
  if (NULL == pPos) {
    return false;
  }

  pPos += strlen(pToken);
  while (*pPos == ' ' || *pPos == '\'' || *pPos == '"') {
    ++pPos;
  }

  int32_t i = 0;
  while (*pPos != '\0' && *pPos != ',' && *pPos != ']' && !isspace((unsigned char)*pPos) && i < (bufLen - 1)) {
    pBuf[i++] = *pPos++;
  }
  while (i > 0 && (pBuf[i - 1] == '\'' || pBuf[i - 1] == '"')) {
    --i;
  }
  pBuf[i] = '\0';
  return i > 0;
}

static bool extExtractTokenInt(const char* pMsg, const char* pToken, int32_t* pVal) {
  if (NULL == pVal) {
    return false;
  }

  char buf[64] = {0};
  if (!extExtractTokenValue(pMsg, pToken, buf, sizeof(buf))) {
    return false;
  }

  char* end = NULL;
  int64_t v = taosStr2Int64(buf, &end, 10);
  if (NULL == end || *end != '\0') {
    return false;
  }

  *pVal = (int32_t)v;
  return true;
}

static bool extExtractFuncNameFallback(const char* pMsg, char* pBuf, int32_t bufLen) {
  if (NULL == pMsg || NULL == pBuf || bufLen <= 0) {
    return false;
  }

  const char* pPos = taosStrCaseStr(pMsg, "function ");
  if (NULL == pPos) {
    pPos = taosStrCaseStr(pMsg, "unknown function");
    if (NULL == pPos) {
      return false;
    }
    pPos += strlen("unknown function");
  } else {
    pPos += strlen("function ");
  }

  while (*pPos != '\0' && (*pPos == '\'' || *pPos == '"' || *pPos == '`' || isspace((unsigned char)*pPos))) {
    ++pPos;
  }

  int32_t i = 0;
  while (*pPos != '\0' && (isalnum((unsigned char)*pPos) || *pPos == '_' || *pPos == '.') && i < (bufLen - 1)) {
    pBuf[i++] = *pPos++;
  }
  pBuf[i] = '\0';
  return i > 0;
}

static EExtClauseKind extInferClauseFromMessage(const char* pMsg) {
  if (NULL == pMsg || '\0' == pMsg[0]) {
    return EXT_CLAUSE_FILTER;
  }

  if (NULL != taosStrCaseStr(pMsg, "order by") || NULL != taosStrCaseStr(pMsg, "clause=order")) {
    return EXT_CLAUSE_ORDER;
  }
  if (NULL != taosStrCaseStr(pMsg, "group by") || NULL != taosStrCaseStr(pMsg, "having") ||
      NULL != taosStrCaseStr(pMsg, "clause=agg")) {
    return EXT_CLAUSE_AGG;
  }
  if (NULL != taosStrCaseStr(pMsg, "limit") || NULL != taosStrCaseStr(pMsg, "clause=limit")) {
    return EXT_CLAUSE_LIMIT;
  }
  if (NULL != taosStrCaseStr(pMsg, " join ") || NULL != taosStrCaseStr(pMsg, "clause=join")) {
    return EXT_CLAUSE_JOIN;
  }
  if (NULL != taosStrCaseStr(pMsg, "select") || NULL != taosStrCaseStr(pMsg, "projection") ||
      NULL != taosStrCaseStr(pMsg, "clause=projection")) {
    return EXT_CLAUSE_PROJECTION;
  }

  return EXT_CLAUSE_FILTER;
}

static void extFillDiagnosticDetail(SExtPushdownDiagnostic* pDiag, const char* pMsg) {
  if (NULL == pDiag) {
    return;
  }

  int32_t intVal = 0;
  switch (pDiag->suggestedDemoteKind) {
    case EXT_DEMOTE_KIND_FUNC:
      if (!extExtractTokenValue(pMsg, "func=", pDiag->detail.funcName, sizeof(pDiag->detail.funcName))) {
        (void)extExtractFuncNameFallback(pMsg, pDiag->detail.funcName, sizeof(pDiag->detail.funcName));
      }
      break;
    case EXT_DEMOTE_KIND_OP:
      if (extExtractTokenInt(pMsg, "op=", &intVal)) {
        pDiag->detail.opType = (EOperatorType)intVal;
      }
      break;
    case EXT_DEMOTE_KIND_CLAUSE:
      if (extExtractTokenInt(pMsg, "clause=", &intVal) && intVal >= 0 && intVal < EXT_CLAUSE_MAX) {
        pDiag->detail.clauseKind = (EExtClauseKind)intVal;
      } else {
        pDiag->detail.clauseKind = extInferClauseFromMessage(pMsg);
      }
      break;
    case EXT_DEMOTE_KIND_TYPE:
      if (extExtractTokenInt(pMsg, "type=", &intVal) && intVal >= INT8_MIN && intVal <= INT8_MAX) {
        pDiag->detail.tsdbTypeCode = (int8_t)intVal;
      }
      break;
    default:
      break;
  }
}

static void extSetDemoteByCode(int32_t tdCode, SExtPushdownDiagnostic* pDiag) {
  switch (tdCode) {
    case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_FUNC:
      pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_FUNC_MISS;
      pDiag->suggestedDemoteKind = EXT_DEMOTE_KIND_FUNC;
      break;
    case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_OP:
      pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_SYNTAX_PRECISE;
      pDiag->suggestedDemoteKind = EXT_DEMOTE_KIND_OP;
      break;
    case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_CLAUSE:
      pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_SYNTAX_PRECISE;
      pDiag->suggestedDemoteKind = EXT_DEMOTE_KIND_CLAUSE;
      break;
    case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_TYPE:
      pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_TYPE_MISMATCH;
      pDiag->suggestedDemoteKind = EXT_DEMOTE_KIND_TYPE;
      break;
    case TSDB_CODE_EXT_PUSHDOWN_DEMOTE_OVERLAY:
      pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_UNKNOWN;
      pDiag->suggestedDemoteKind = EXT_DEMOTE_KIND_NONE;
      break;
    case TSDB_CODE_EXT_QUERY_TIMEOUT:
      pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_TIMEOUT;
      pDiag->suggestedDemoteKind = EXT_DEMOTE_KIND_NONE;
      break;
    case TSDB_CODE_EXT_CONNECT_FAILED:
      pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_NETWORK;
      pDiag->suggestedDemoteKind = EXT_DEMOTE_KIND_NONE;
      break;
    case TSDB_CODE_EXT_REMOTE_INTERNAL:
    case TSDB_CODE_EXT_FETCH_FAILED:
      pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_INTERNAL;
      pDiag->suggestedDemoteKind = EXT_DEMOTE_KIND_NONE;
      break;
    default:
      pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_UNKNOWN;
      pDiag->suggestedDemoteKind = EXT_DEMOTE_KIND_NONE;
      break;
  }
}

bool extIsRetryableCode(int32_t errCode) {
  switch (errCode) {
    case TSDB_CODE_EXT_CONNECT_FAILED:
    case TSDB_CODE_EXT_QUERY_TIMEOUT:
    case TSDB_CODE_EXT_RESOURCE_EXHAUSTED:
      return true;
    default:
      return false;
  }
}

int32_t extClassifyRemoteError(int32_t tdCode, const char* remoteSqlState, int32_t remoteCode,
                               int32_t httpStatus, const char* remoteMessage,
                               SExtPushdownDiagnostic* pDiag) {
  if (NULL == pDiag) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(pDiag, 0, sizeof(*pDiag));
  pDiag->classifiedCode = tdCode;
  pDiag->remoteCode = remoteCode;
  pDiag->httpStatus = httpStatus;
  extSetDemoteByCode(tdCode, pDiag);

  if (NULL != remoteSqlState && remoteSqlState[0] != '\0') {
    tstrncpy(pDiag->remoteSqlState, remoteSqlState, sizeof(pDiag->remoteSqlState));
  }
  if (NULL != remoteMessage && remoteMessage[0] != '\0') {
    tstrncpy(pDiag->remoteMessage, remoteMessage, sizeof(pDiag->remoteMessage));
  }

  int32_t reasonToken = 0;
  int32_t kindToken = 0;
  if (extExtractTokenInt(remoteMessage, "reason=", &reasonToken) && reasonToken > 0) {
    pDiag->reasonCode = (EExtDemoteReason)reasonToken;
  }
  if (extExtractTokenInt(remoteMessage, "diag_kind=", &kindToken) &&
      kindToken >= EXT_DEMOTE_KIND_NONE && kindToken <= EXT_DEMOTE_KIND_TYPE) {
    pDiag->suggestedDemoteKind = (EExtCapDemoteKind)kindToken;
  }

  if (pDiag->suggestedDemoteKind != EXT_DEMOTE_KIND_NONE) {
    extFillDiagnosticDetail(pDiag, remoteMessage);
    return TSDB_CODE_SUCCESS;
  }

  if (extContainsNoCase(remoteMessage, "function") &&
      (extContainsNoCase(remoteMessage, "does not exist") ||
       extContainsNoCase(remoteMessage, "unknown function") ||
       extContainsNoCase(remoteMessage, "invalid function") ||
       extContainsNoCase(remoteMessage, "no function matches"))) {
    // PostgreSQL: "function xxx does not exist"
    // DataFusion/InfluxDB 3: "Invalid function 'xxx'" / "No function matches ..."
    pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_FUNC_MISS;
    pDiag->suggestedDemoteKind = EXT_DEMOTE_KIND_FUNC;
    extFillDiagnosticDetail(pDiag, remoteMessage);
    return TSDB_CODE_SUCCESS;
  }

  // PostgreSQL/InfluxDB: column not in GROUP BY — the pushed-down AGG clause is invalid.
  if (extContainsNoCase(remoteMessage, "must appear in the group by clause") ||
      extContainsNoCase(remoteMessage, "not in group by clause") ||
      extContainsNoCase(remoteMessage, "schema error: no field named")) {
    pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_SYNTAX_PRECISE;
    pDiag->suggestedDemoteKind = EXT_DEMOTE_KIND_CLAUSE;
    pDiag->detail.clauseKind = EXT_CLAUSE_AGG;
    return TSDB_CODE_SUCCESS;
  }

  if (extContainsNoCase(remoteMessage, "operator does not exist") ||
      extContainsNoCase(remoteMessage, "unsupported operator")) {
    pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_SYNTAX_PRECISE;
    pDiag->suggestedDemoteKind = EXT_DEMOTE_KIND_OP;
    extFillDiagnosticDetail(pDiag, remoteMessage);
    return TSDB_CODE_SUCCESS;
  }

  if (extContainsNoCase(remoteMessage, "type mismatch") ||
      extContainsNoCase(remoteMessage, "cannot cast") ||
      extContainsNoCase(remoteMessage, "incompatible type")) {
    pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_TYPE_MISMATCH;
    pDiag->suggestedDemoteKind = EXT_DEMOTE_KIND_TYPE;
    extFillDiagnosticDetail(pDiag, remoteMessage);
    return TSDB_CODE_SUCCESS;
  }

  if ((NULL != remoteSqlState && strncmp(remoteSqlState, "42", 2) == 0) ||
      extContainsNoCase(remoteMessage, "syntax error")) {
    pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_SYNTAX_VAGUE;
    pDiag->suggestedDemoteKind = EXT_DEMOTE_KIND_NONE;
    return TSDB_CODE_SUCCESS;
  }

  if (extContainsNoCase(remoteMessage, "timeout")) {
    pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_TIMEOUT;
  } else if (extContainsNoCase(remoteMessage, "network") ||
             extContainsNoCase(remoteMessage, "connection reset") ||
             extContainsNoCase(remoteMessage, "broken pipe")) {
    pDiag->reasonCode = EXT_DEMOTE_REASON_REMOTE_NETWORK;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t extBuildPushdownDiagnostic(int32_t tdCode, const SExtConnectorError* pErr,
                                   const char* generatedSql, SExtPushdownDiagnostic* pDiag) {
  if (NULL == pDiag) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = extClassifyRemoteError(tdCode,
                                        (NULL != pErr) ? pErr->remoteSqlstate : NULL,
                                        (NULL != pErr) ? pErr->remoteCode : 0,
                                        (NULL != pErr) ? pErr->httpStatus : 0,
                                        (NULL != pErr) ? pErr->remoteMessage : NULL,
                                        pDiag);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  if (NULL != generatedSql && generatedSql[0] != '\0') {
    tstrncpy(pDiag->generatedSql, generatedSql, sizeof(pDiag->generatedSql));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t extMapDiagnosticToCode(int32_t tdCode, const SExtPushdownDiagnostic* pDiag) {
  if (NULL == pDiag) {
    return tdCode;
  }

  if (tdCode == TSDB_CODE_EXT_PUSHDOWN_DEMOTE_FUNC || tdCode == TSDB_CODE_EXT_PUSHDOWN_DEMOTE_OP ||
      tdCode == TSDB_CODE_EXT_PUSHDOWN_DEMOTE_CLAUSE || tdCode == TSDB_CODE_EXT_PUSHDOWN_DEMOTE_TYPE ||
      tdCode == TSDB_CODE_EXT_PUSHDOWN_DEMOTE_OVERLAY) {
    return tdCode;
  }

  if (tdCode != TSDB_CODE_EXT_FETCH_FAILED && tdCode != TSDB_CODE_EXT_PUSHDOWN_FAILED &&
      tdCode != TSDB_CODE_EXT_SYNTAX_UNSUPPORTED && tdCode != TSDB_CODE_EXT_REMOTE_INTERNAL) {
    return tdCode;
  }

  if (EXT_REASON_IS_PRECISE(pDiag->reasonCode)) {
    switch (pDiag->suggestedDemoteKind) {
      case EXT_DEMOTE_KIND_FUNC:
        return TSDB_CODE_EXT_PUSHDOWN_DEMOTE_FUNC;
      case EXT_DEMOTE_KIND_OP:
        return TSDB_CODE_EXT_PUSHDOWN_DEMOTE_OP;
      case EXT_DEMOTE_KIND_CLAUSE:
        return TSDB_CODE_EXT_PUSHDOWN_DEMOTE_CLAUSE;
      case EXT_DEMOTE_KIND_TYPE:
        return TSDB_CODE_EXT_PUSHDOWN_DEMOTE_TYPE;
      default:
        return TSDB_CODE_EXT_PUSHDOWN_DEMOTE_OVERLAY;
    }
  }

  if (pDiag->reasonCode == EXT_DEMOTE_REASON_REMOTE_SYNTAX_VAGUE) {
    return TSDB_CODE_EXT_PUSHDOWN_DEMOTE_OVERLAY;
  }

  /*
   * Runtime connector failures such as EXT_REMOTE_INTERNAL / EXT_FETCH_FAILED
   * must still surface to the client when classification cannot prove a
   * pushdown-only incompatibility. Demoting these vague internal/unknown
   * errors to overlay causes the client to restart the same statement instead
   * of honoring NEED_CLIENT_RETURN_EXT_SOURCE_ERROR semantics.
   */
  if (pDiag->reasonCode == EXT_DEMOTE_REASON_REMOTE_INTERNAL ||
      pDiag->reasonCode == EXT_DEMOTE_REASON_REMOTE_UNKNOWN) {
    return tdCode;
  }

  return tdCode;
}

int32_t extFormatPushdownDiagnosticHint(const SExtPushdownDiagnostic* pDiag, char* pBuf, int32_t bufLen) {
  if (NULL == pDiag || NULL == pBuf || bufLen <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t offset = snprintf(pBuf, bufLen, "diag_kind=%d, reason=%d",
                            (int32_t)pDiag->suggestedDemoteKind, (int32_t)pDiag->reasonCode);
  if (offset < 0 || offset >= bufLen) {
    pBuf[bufLen - 1] = '\0';
    return TSDB_CODE_SUCCESS;
  }

  switch (pDiag->suggestedDemoteKind) {
    case EXT_DEMOTE_KIND_FUNC:
      if (pDiag->detail.funcName[0] != '\0') {
        (void)snprintf(pBuf + offset, bufLen - offset, ", func=%s", pDiag->detail.funcName);
      }
      break;
    case EXT_DEMOTE_KIND_OP:
      (void)snprintf(pBuf + offset, bufLen - offset, ", op=%d", (int32_t)pDiag->detail.opType);
      break;
    case EXT_DEMOTE_KIND_CLAUSE:
      (void)snprintf(pBuf + offset, bufLen - offset, ", clause=%d", (int32_t)pDiag->detail.clauseKind);
      break;
    case EXT_DEMOTE_KIND_TYPE:
      (void)snprintf(pBuf + offset, bufLen - offset, ", type=%d", (int32_t)pDiag->detail.tsdbTypeCode);
      break;
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}
