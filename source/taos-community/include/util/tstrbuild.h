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

#ifndef _TD_UTIL_STRING_BUILDER_H_
#define _TD_UTIL_STRING_BUILDER_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SStringBuilder {
  jmp_buf jb;
  size_t  size;
  size_t  pos;
  char*   buf;
} SStringBuilder;

#define taosStringBuilderSetJmp(sb) setjmp((sb)->jb)

void  taosStringBuilderEnsureCapacity(SStringBuilder* sb, size_t size);
char* taosStringBuilderGetResult(SStringBuilder* sb, size_t* len);
void  taosStringBuilderDestroy(SStringBuilder* sb);

void taosStringBuilderAppend(SStringBuilder* sb, const void* data, size_t len);
void taosStringBuilderAppendChar(SStringBuilder* sb, char c);
void taosStringBuilderAppendStringLen(SStringBuilder* sb, const char* str, size_t len);
void taosStringBuilderAppendString(SStringBuilder* sb, const char* str);
void taosStringBuilderAppendNull(SStringBuilder* sb);
void taosStringBuilderAppendInteger(SStringBuilder* sb, int64_t v);
void taosStringBuilderAppendDouble(SStringBuilder* sb, double v);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_STRING_BUILDER_H_*/