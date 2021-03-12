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

#ifndef _todbc_string_h_
#define _todbc_string_h_

#include <stdint.h>
#include <stddef.h>

#include "todbc_buf.h"

// non-thread-safe

typedef struct todbc_string_s            todbc_string_t;
struct todbc_string_s {
  char                     enc[64];
  // null if init failed because of internal resources shortage
  const unsigned char     *buf;             // null-terminator inclusive
  size_t                   total_bytes;     // not counting null-terminator

  // <= total_bytes
  // truncated if < total_bytes
  size_t                   bytes;           // not counting null-terminator
};


// does not copy internally
// bytes: not characters, <0 means bytes unknown
todbc_string_t todbc_string_init(const char *enc, const unsigned char *src, const size_t bytes);
// conv and copy to dst not more than target_bytes (null-terminator-inclusive)
// return'd val->buf == dst, total_bytes<target_bytes, truncated if bytes<total_bytes
todbc_string_t todbc_string_copy(todbc_string_t *str, const char *enc, unsigned char *dst, const size_t target_bytes);
todbc_string_t todbc_copy(const char *from_enc, const unsigned char *src, size_t *inbytes, const char *to_enc, unsigned char *dst, const size_t dlen);

// use todbc_buf_t as mem-allocator
todbc_string_t todbc_string_conv_to(todbc_string_t *str, const char *enc, todbc_buf_t *buf);


#endif // _todbc_string_h_

