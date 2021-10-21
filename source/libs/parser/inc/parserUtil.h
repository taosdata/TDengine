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

#ifndef TDENGINE_PARSERUTIL_H
#define TDENGINE_PARSERUTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "ttoken.h"

#define UTIL_TABLE_IS_SUPER_TABLE(metaInfo) \
  (((metaInfo)->pTableMeta != NULL) && ((metaInfo)->pTableMeta->tableType == TSDB_SUPER_TABLE))

#define UTIL_TABLE_IS_CHILD_TABLE(metaInfo) \
  (((metaInfo)->pTableMeta != NULL) && ((metaInfo)->pTableMeta->tableType == TSDB_CHILD_TABLE))

#define UTIL_TABLE_IS_NORMAL_TABLE(metaInfo) \
  (!(UTIL_TABLE_IS_SUPER_TABLE(metaInfo) || UTIL_TABLE_IS_CHILD_TABLE(metaInfo)))

#define UTIL_TABLE_IS_TMP_TABLE(metaInfo) \
  (((metaInfo)->pTableMeta != NULL) && ((metaInfo)->pTableMeta->tableType == TSDB_TEMP_TABLE))


int32_t parserValidateIdToken(SToken* pToken);
int32_t buildInvalidOperationMsg(char* dst, int32_t dstBufLen, const char* msg);
int32_t parserSetSyntaxErrMsg(char* dst, int32_t dstBufLen, const char* additionalInfo,  const char* sourceStr);


void columnListCopy(SArray* dst, const SArray* src, uint64_t tableUid);
void columnListCopyAll(SArray* dst, const SArray* src);
void columnListDestroy(SArray* pColumnList);

void cleanupTagCond(STagCond* pTagCond);
void cleanupColumnCond(SArray** pCond);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_PARSERUTIL_H
