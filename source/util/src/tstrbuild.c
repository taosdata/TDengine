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

#define _DEFAULT_SOURCE
#include "tstrbuild.h"

void taosStringBuilderEnsureCapacity(SStringBuilder* sb, size_t size) {
  size += sb->pos;
  if (size > sb->size) {
    size *= 2;
    void* tmp = taosMemoryRealloc(sb->buf, size);
    if (tmp == NULL) {
      longjmp(sb->jb, 1);
    }
    sb->buf = (char*)tmp;
    sb->size = size;
  }
}

char* taosStringBuilderGetResult(SStringBuilder* sb, size_t* len) {
  taosStringBuilderEnsureCapacity(sb, 1);
  sb->buf[sb->pos] = 0;
  if (len != NULL) {
    *len = sb->pos;
  }
  return sb->buf;
}

void taosStringBuilderDestroy(SStringBuilder* sb) {
  taosMemoryFree(sb->buf);
  sb->buf = NULL;
  sb->pos = 0;
  sb->size = 0;
}

void taosStringBuilderAppend(SStringBuilder* sb, const void* data, size_t len) {
  taosStringBuilderEnsureCapacity(sb, len);
  memcpy(sb->buf + sb->pos, data, len);
  sb->pos += len;
}

void taosStringBuilderAppendChar(SStringBuilder* sb, char c) {
  taosStringBuilderEnsureCapacity(sb, 1);
  sb->buf[sb->pos++] = c;
}

void taosStringBuilderAppendStringLen(SStringBuilder* sb, const char* str, size_t len) {
  taosStringBuilderEnsureCapacity(sb, len);
  if(!sb->buf) return;
  memcpy(sb->buf + sb->pos, str, len);
  sb->pos += len;
}

void taosStringBuilderAppendString(SStringBuilder* sb, const char* str) {
  taosStringBuilderAppendStringLen(sb, str, strlen(str));
}

void taosStringBuilderAppendNull(SStringBuilder* sb) { taosStringBuilderAppendStringLen(sb, "null", 4); }

void taosStringBuilderAppendInteger(SStringBuilder* sb, int64_t v) {
  char   buf[64] = {0};
  size_t len = snprintf(buf, sizeof(buf), "%" PRId64, v);
  taosStringBuilderAppendStringLen(sb, buf, TMIN(len, sizeof(buf)));
}

void taosStringBuilderAppendDouble(SStringBuilder* sb, double v) {
  char   buf[512] = {0};
  size_t len = snprintf(buf, sizeof(buf), "%.9lf", v);
  taosStringBuilderAppendStringLen(sb, buf, TMIN(len, sizeof(buf)));
}
