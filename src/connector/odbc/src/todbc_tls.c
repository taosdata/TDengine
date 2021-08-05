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

#include "todbc_tls.h"

#include "todbc_buf.h"
#include "todbc_iconv.h"
#include "todbc_log.h"


typedef struct todbc_tls_s                     todbc_tls_t;

struct todbc_tls_s {
  todbc_buf_t                          *buf;
  todbc_iconvset_t                     *cnvset;
};


static void todbc_tls_free(todbc_tls_t *value);

static pthread_key_t            key_this;
static pthread_once_t           key_once      = PTHREAD_ONCE_INIT;
static int                      key_err       = 0;


static void key_init(void);
static void key_destructor(void *arg);


static void key_init(void) {
  key_err = pthread_key_create(&key_this, key_destructor);
  if (key_err) {
    D("thread local initialization failed: [%d]%s", key_err, strerror(key_err));
  }
}

static todbc_tls_t* todbc_tls_create(void);

static todbc_tls_t* key_value(void) {
  pthread_once(&key_once, key_init);
  if (key_err) return NULL;

  int err = 0;

  todbc_tls_t *value = pthread_getspecific(key_this);
  if (value) return value;

  value = todbc_tls_create();
  if (!value) return NULL;

  do {
    err = pthread_setspecific(key_this, value);
    if (err) {
      D("thread local setup failed: [%d]%s", err, strerror(err));
      break;
    }

    return value;
  } while (0);

  todbc_tls_free(value);

  return NULL;
}

static void key_destructor(void *arg) {
  todbc_tls_t *value = (todbc_tls_t*)arg;
  todbc_tls_free(value);
}

static todbc_tls_t* todbc_tls_create(void) {
  int err = 0;
  todbc_tls_t *value = (todbc_tls_t*)calloc(1, sizeof(*value));
  if (!value) {
    err = errno;
    D("thread local creation failed: [%d]%s", err, strerror(err));
    return NULL;
  }
  do {
    return value;
  } while (0);

  todbc_tls_free(value);
  return NULL;
}

static void todbc_tls_free(todbc_tls_t *value) {
  if (value->cnvset) {
    todbc_iconvset_free(value->cnvset);
    value->cnvset = NULL;
  }

  if (value->buf) {
    todbc_buf_free(value->buf);
    value->buf = NULL;
  }

  free(value);
}

static todbc_iconvset_t* do_get_iconvset(void);

// iconv
int todbc_legal_chars(const char *enc, const unsigned char *str, todbc_bytes_t *bc) {
  todbc_iconvset_t *icnv = do_get_iconvset();
  if (!icnv) return -1;
  todbc_iconv_t *cnv = todbc_iconvset_get(icnv, UTF16_ENC, enc);
  if (!cnv) return -1;
  return todbc_iconv_get_legal_chars(cnv, str, bc);
}

todbc_iconv_t* todbc_tls_iconv_get(const char *to_enc, const char *from_enc) {
  todbc_iconvset_t *cnvset = do_get_iconvset();
  if (!cnvset) return NULL;
  todbc_iconv_t *cnv = todbc_iconvset_get(cnvset, to_enc, from_enc);
  return cnv;
}

iconv_t todbc_tls_iconv(const char *to_enc, const char *from_enc) {
  todbc_iconv_t *icnv = todbc_tls_iconv_get(to_enc, from_enc);
  if (!icnv) return (iconv_t)-1;
  return todbc_iconv_get(icnv);
}

todbc_enc_t todbc_tls_iconv_enc(const char *enc) {
  do {
    todbc_iconvset_t *cnvset = do_get_iconvset();
    if (!cnvset) break;
    return todbc_iconvset_enc(cnvset, enc);
  } while (0);

  todbc_enc_t v = {0};
  v.char_size   = -1;
  v.null_size   = -1;

  return v;
}

todbc_string_t todbc_tls_conv(todbc_buf_t *buf, const char *enc_to, const char *enc_from, const unsigned char *src, size_t *slen) {
  todbc_iconv_t *cnv = todbc_tls_iconv_get(enc_to, enc_from);
  if (!cnv) {
    todbc_string_t nul = {0};
    return nul;
  }
  return todbc_iconv_conv2(cnv, buf, src, slen);
}

todbc_string_t todbc_tls_write(const char *enc_to, const char *enc_from,
    const unsigned char *src, size_t *slen, unsigned char *dst, size_t dlen)
{
  todbc_iconv_t *cnv = todbc_tls_iconv_get(enc_to, enc_from);
  if (!cnv) {
    todbc_string_t nul = {0};
    return nul;
  }
  todbc_string_t s = {0};
  s.buf = dst;
  s.total_bytes = dlen;
  size_t inbytes  = *slen;
  size_t outbytes = dlen;
  todbc_iconv_raw(cnv, src, &inbytes, dst, &outbytes);
  s.bytes = dlen - outbytes;
  s.total_bytes = s.bytes;
  if (inbytes) {
    s.total_bytes += 1;
  }
  *slen = inbytes;

  return s;
}

char* todbc_tls_strndup(const char *src, size_t n) {
  todbc_buf_t *buf = todbc_tls_buf();
  if (!buf) return NULL;
  n = strnlen(src, n);
  char *d = todbc_buf_alloc(buf, (n+1));
  if (!d) return NULL;
  snprintf(d, n+1, "%s", src);
  return d;
}

static todbc_iconvset_t* do_get_iconvset(void) {
  todbc_tls_t *tls = key_value();
  if (!tls) return NULL;
  if (!tls->cnvset) {
    tls->cnvset = todbc_iconvset_create();
  }
  return tls->cnvset;
}

// tls_buf
void* todbc_tls_buf_alloc(size_t size) {
  todbc_tls_t *tls = key_value();
  if (!tls) return NULL;
  if (!tls->buf) {
    tls->buf = todbc_buf_create();
    if (!tls->buf) return NULL;
  }
  return todbc_buf_alloc(tls->buf, size);
}

void* todbc_tls_buf_calloc(size_t count, size_t size) {
  todbc_tls_t *tls = key_value();
  if (!tls) return NULL;
  if (!tls->buf) {
    tls->buf = todbc_buf_create();
    if (!tls->buf) return NULL;
  }
  return todbc_buf_calloc(tls->buf, count, size);
}

void* todbc_tls_buf_realloc(void *ptr, size_t size) {
  todbc_tls_t *tls = key_value();
  if (!tls) return NULL;
  if (!tls->buf) {
    tls->buf = todbc_buf_create();
    if (!tls->buf) return NULL;
  }
  return todbc_buf_realloc(tls->buf, ptr, size);
}

void todbc_tls_buf_reclaim(void) {
  todbc_tls_t *tls = key_value();
  if (!tls) return;
  if (!tls->buf) return;

  todbc_buf_reclaim(tls->buf);
}

todbc_buf_t* todbc_tls_buf(void) {
  todbc_tls_t *tls = key_value();
  if (!tls) return NULL;
  if (!tls->buf) {
    tls->buf = todbc_buf_create();
  }
  return tls->buf;
}

