/*
 * Copyright (c) 2020 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include <stdlib.h>
#include <memory.h>
#include <assert.h>

#define TBUFFER_DEFINE_FUNCTION(type, name) \
  type tbufRead##name(SBuffer* buf) {            \
    type ret;                                    \
    tbufReadToBuffer(buf, &ret, sizeof(type));   \
    return ret;                                  \
  }\
  void tbufWrite##name(SBuffer* buf, type data) {\
    tbufWrite(buf, &data, sizeof(data));\
  }\
  void tbufWrite##name##At(SBuffer* buf, size_t pos, type data) {\
    tbufWriteAt(buf, pos, &data, sizeof(data));\
  }

#include "tbuffer.h"


////////////////////////////////////////////////////////////////////////////////
// common functions

size_t tbufTell(SBuffer* buf) {
  return buf->pos;
}

size_t tbufSeekTo(SBuffer* buf, size_t pos) {
  if (pos > buf->size) {
    // TODO: update error code, other tbufThrowError need to be changed too
    tbufThrowError(buf, 1);
  }
  size_t old = buf->pos;
  buf->pos = pos;
  return old;
}

size_t tbufSkip(SBuffer* buf, size_t size) {
  return tbufSeekTo(buf, buf->pos + size);
}

void tbufClose(SBuffer* buf, bool keepData) {
  if (!keepData) {
    free(buf->data);
  }
  buf->data = NULL;
  buf->pos = 0;
  buf->size = 0;
}

////////////////////////////////////////////////////////////////////////////////
// read functions

char* tbufRead(SBuffer* buf, size_t size) {
  char* ret = buf->data + buf->pos;
  tbufSkip(buf, size);
  return ret;
}

void tbufReadToBuffer(SBuffer* buf, void* dst, size_t size) {
  assert(dst != NULL);
  // always using memcpy, leave optimization to compiler
  memcpy(dst, tbufRead(buf, size), size);
}

const char* tbufReadString(SBuffer* buf, size_t* len) {
  uint16_t l = tbufReadUint16(buf);
  char*    ret = buf->data + buf->pos;
  tbufSkip(buf, l + 1);
  ret[l] = 0;  // ensure the string end with '\0'
  if (len != NULL) {
    *len = l;
  }
  return ret;
}

size_t tbufReadToString(SBuffer* buf, char* dst, size_t size) {
  assert(dst != NULL);
  size_t      len;
  const char* str = tbufReadString(buf, &len);
  if (len >= size) {
    len = size - 1;
  }
  memcpy(dst, str, len);
  dst[len] = 0;
  return len;
}


////////////////////////////////////////////////////////////////////////////////
// write functions

void tbufEnsureCapacity(SBuffer* buf, size_t size) {
  size += buf->pos;
  if (size > buf->size) {
    size_t nsize = size + buf->size;
    char* data = realloc(buf->data, nsize);
    if (data == NULL) {
      tbufThrowError(buf, 2);
    }
    buf->data = data;
    buf->size = nsize;
  }
}

char* tbufGetData(SBuffer* buf, bool takeOver) {
  char* ret = buf->data;
  if (takeOver) {
    buf->pos = 0;
    buf->size = 0;
    buf->data = NULL;
  }

  return ret;
}

void tbufEndWrite(SBuffer* buf) {
    free(buf->data);
    buf->data = NULL;
    buf->pos = 0;
    buf->size = 0;
}

void tbufWrite(SBuffer* buf, const void* data, size_t size) {
  assert(data != NULL);
  tbufEnsureCapacity(buf, size);
  memcpy(buf->data + buf->pos, data, size);
  buf->pos += size;
}

void tbufWriteAt(SBuffer* buf, size_t pos, const void* data, size_t size) {
  assert(data != NULL);
  // this function can only be called to fill the gap on previous writes,
  // so 'pos + size <= buf->pos' must be true
  assert(pos + size <= buf->pos);
  memcpy(buf->data + pos, data, size);
}

void tbufWriteStringLen(SBuffer* buf, const char* str, size_t len) {
  // maximum string length is 65535, if longer string is required
  // this function and the corresponding read function need to be
  // revised.
  assert(len <= 0xffff);
  tbufWriteUint16(buf, (uint16_t)len);
  tbufWrite(buf, str, len + 1);
}

void tbufWriteString(SBuffer* buf, const char* str) {
  tbufWriteStringLen(buf, str, strlen(str));
}
