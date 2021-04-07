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

#ifndef _todbc_tls_h_
#define _todbc_tls_h_

// !!! functions exported in this header file are all non-thread-safe !!!

#include "taos.h"

#include "todbc_buf.h"
#include "todbc_iconv.h"
#include "todbc_string.h"

// thread local buffers
// non-thread-safe
// returned-buf are all thread-local-accessible until todbc_tls_buf_reclaim
void* todbc_tls_buf_alloc(size_t size);
void* todbc_tls_buf_calloc(size_t count, size_t size);
void* todbc_tls_buf_realloc(void *ptr, size_t size);
// reclaim all above thread-local-buf(s)
void  todbc_tls_buf_reclaim(void);

// return local-thread-buf
todbc_buf_t* todbc_tls_buf(void);

// thread local iconv
// non-thread-safe
todbc_iconv_t* todbc_tls_iconv_get(const char *to_enc, const char *from_enc);
iconv_t todbc_tls_iconv(const char *to_enc, const char *from_enc);

todbc_enc_t    todbc_tls_iconv_enc(const char *enc);

// non-thread-safe
int todbc_legal_chars(const char *enc, const unsigned char *str, todbc_bytes_t *bc);

// at return, *slen stores the remaining #
todbc_string_t todbc_tls_conv(todbc_buf_t *buf, const char *enc_to, const char *enc_from, const unsigned char *src, size_t *slen);
todbc_string_t todbc_tls_write(const char *enc_to, const char *enc_from, const unsigned char *src, size_t *slen, unsigned char *dst, size_t dlen);

char*          todbc_tls_strndup(const char *src, size_t n);

#endif // _todbc_tls_h_

