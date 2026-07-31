/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#ifndef _TD_EXT_ERROR_H_
#define _TD_EXT_ERROR_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tmsg.h"

typedef struct SExtConnectorError SExtConnectorError;

bool    extIsRetryableCode(int32_t errCode);
int32_t extClassifyRemoteError(int32_t tdCode, const char* remoteSqlState, int32_t remoteCode,
                               int32_t httpStatus, const char* remoteMessage,
                               SExtPushdownDiagnostic* pDiag);
int32_t extBuildPushdownDiagnostic(int32_t tdCode, const SExtConnectorError* pErr,
                                   const char* generatedSql, SExtPushdownDiagnostic* pDiag);
int32_t extMapDiagnosticToCode(int32_t tdCode, const SExtPushdownDiagnostic* pDiag);
int32_t extFormatPushdownDiagnosticHint(const SExtPushdownDiagnostic* pDiag, char* pBuf, int32_t bufLen);

#ifdef __cplusplus
}
#endif

#endif  // _TD_EXT_ERROR_H_
