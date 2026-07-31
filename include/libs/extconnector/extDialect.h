/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#ifndef _TD_EXT_DIALECT_H_
#define _TD_EXT_DIALECT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tmsg.h"

int32_t extResolveDialectBySource(int8_t sourceType, int32_t* pDialect);
int32_t extLoadDialectDefaults(int8_t sourceType, SExtSourceCapability* pCap);
bool    extDialectClauseMappable(int32_t dialect, EExtClauseKind clause);

#ifdef __cplusplus
}
#endif

#endif  // _TD_EXT_DIALECT_H_
