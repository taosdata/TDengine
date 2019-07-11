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

#ifndef TDENGINE_TSHASH_H
#define TDENGINE_TSHASH_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

void *taosInitStrHash(uint32_t maxSessions, uint32_t dataSize, uint32_t (*fp)(void *, char *));

void taosCleanUpStrHash(void *handle);

void *taosGetStrHashData(void *handle, char *string);

void taosDeleteStrHash(void *handle, char *string);

void taosDeleteStrHashNode(void *handle, char *string, void *pDeleteNode);

void *taosAddStrHash(void *handle, char *string, char *pData);

void *taosAddStrHashWithSize(void *handle, char *string, char *pData, int dataSize);

uint32_t taosHashString(void *handle, char *string);

uint32_t taosHashStringStep1(void *handle, char *string);

char *taosVisitStrHashWithFp(void *handle, int (*fp)(char *));

void taosCleanUpStrHashWithFp(void *handle, void (*fp)(char *));

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSHASH_H
