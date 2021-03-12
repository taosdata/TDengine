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

#ifndef _todbc_iconv_h_
#define _todbc_iconv_h_

#include "todbc_buf.h"
#include "todbc_string.h"

#include <iconv.h>

// non-thread-safe

#define ASCII_ENC    "ASCII"
#define UTF8_ENC     "UTF-8"
#define UTF16_ENC    "UCS-2LE"
#define UNICODE_ENC  "UCS-4LE"
#define GB18030_ENC  "GB18030"

#define MAX_CHARACTER_SIZE  6

typedef enum {
  TSDB_CONV_OK             = 0,
  TSDB_CONV_NOT_AVAIL,
  TSDB_CONV_OOM,
  TSDB_CONV_OOR,
  TSDB_CONV_TRUNC_FRACTION,
  TSDB_CONV_TRUNC,
  TSDB_CONV_CHAR_NOT_NUM,
  TSDB_CONV_CHAR_NOT_TS,
  TSDB_CONV_NOT_VALID_TS,
  TSDB_CONV_GENERAL,
  TSDB_CONV_BAD_CHAR,
  TSDB_CONV_SRC_TOO_LARGE,
  TSDB_CONV_SRC_BAD_SEQ,
  TSDB_CONV_SRC_INCOMPLETE,
  TSDB_CONV_SRC_GENERAL,
} TSDB_CONV_CODE;

typedef struct todbc_iconvset_s       todbc_iconvset_t;
typedef struct todbc_iconv_s          todbc_iconv_t;
typedef struct todbc_enc_s            todbc_enc_t;
struct todbc_enc_s {
  char               enc[64];
  int                char_size;  // character size at most
  int                null_size;  // size for null terminator
  int                variable_char_size; // such as 3 for UTF8
};

todbc_iconvset_t* todbc_iconvset_create(void);
void              todbc_iconvset_free(todbc_iconvset_t *cnvset);
todbc_iconv_t*    todbc_iconvset_get(todbc_iconvset_t *cnvset, const char *enc_to, const char *enc_from);
todbc_enc_t       todbc_iconvset_enc(todbc_iconvset_t *cnvset, const char *enc);

typedef struct todbc_bytes_s          todbc_bytes_t;
typedef struct todbc_err_s            todbc_err_t;

struct todbc_err_s {
  unsigned int   einval:1;   // EINVAL
  unsigned int   eilseq:1;   // EILSEQ
  unsigned int   etoobig:1;  // E2BIG
  unsigned int   eoom:1;     // ENOMEM
};

struct todbc_bytes_s {
  size_t         inbytes, outbytes;
  size_t         chars;

  todbc_err_t    err;
};

typedef struct todbc_iconv_arg_s          todbc_iconv_arg_t;
struct todbc_iconv_arg_s {
  const unsigned char  *inbuf;
  size_t               inbytes;       // -1: not set
  unsigned char        *outbuf;
  size_t               outbytes;      // -1: not set

  size_t               chars;         // -1: not set
};

iconv_t        todbc_iconv_get(todbc_iconv_t *cnv);
int            todbc_iconv_get_legal_chars(todbc_iconv_t *cnv, const unsigned char *str, todbc_bytes_t *bc);
// non-thread-safe
// use todbc_buf_t as mem-allocator, if NULL, fall-back to thread-local version
unsigned char* todbc_iconv_conv(todbc_iconv_t *cnv, todbc_buf_t *buf, const unsigned char *src, todbc_bytes_t *bc);
// null-terminator-inclusive
size_t         todbc_iconv_est_bytes(todbc_iconv_t *cnv, size_t inbytes);
// if inchars>=0 && enc_from has fixed-char-size, returns inchars * char_size
// otherwise -1
size_t         todbc_iconv_bytes(todbc_iconv_t *cnv, size_t inchars);
todbc_enc_t    todbc_iconv_from(todbc_iconv_t *cnv);
todbc_enc_t    todbc_iconv_to(todbc_iconv_t *cnv);

// at return, *slen/*dlen stores the remaining #
int            todbc_iconv_raw(todbc_iconv_t *cnv, const unsigned char *src, size_t *slen, unsigned char *dst, size_t *dlen);
// use todbc_buf_t as mem-allocator, if NULL, fall-back to thread-local version
// at return, *slen stores the remaining #
todbc_string_t todbc_iconv_conv2(todbc_iconv_t *cnv, todbc_buf_t *buf, const unsigned char *src, size_t *slen);

#endif // _todbc_iconv_h_

