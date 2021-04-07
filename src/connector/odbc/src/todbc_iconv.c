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

#include "todbc_iconv.h"

#include "todbc_hash.h"
#include "todbc_log.h"
#include "todbc_string.h"
#include "todbc_tls.h"

#define invalid_iconv()     ((iconv_t)-1)

struct todbc_iconvset_s {
  todbc_hash_t              *iconv_hash;

  todbc_hash_t              *enc_hash;
};

typedef struct todbc_iconv_key_s          todbc_iconv_key_t;
struct todbc_iconv_key_s {
  const char      *enc_to;
  const char      *enc_from;
};

struct todbc_iconv_s {
  char                   enc_to[64];
  char                   enc_from[64];
  todbc_iconv_key_t      key;
  iconv_t                cnv;

  todbc_enc_t            from;
  todbc_enc_t            to;

  unsigned int           direct:1;
};

static void todbc_iconv_free(todbc_iconv_t *val);

todbc_iconvset_t* todbc_iconvset_create(void) {
  todbc_iconvset_t *cnvset = (todbc_iconvset_t*)calloc(1, sizeof(*cnvset));
  if (!cnvset) return NULL;

  return cnvset;
}

void todbc_iconvset_free(todbc_iconvset_t *cnvset) {
  if (!cnvset) return;
  if (cnvset->iconv_hash) {
    todbc_hash_free(cnvset->iconv_hash);
    cnvset->iconv_hash = NULL;
  }
  if (cnvset->enc_hash) {
    todbc_hash_free(cnvset->enc_hash);
    cnvset->enc_hash = NULL;
  }
  free(cnvset);
}

static void do_iconv_hash_val_free(todbc_hash_t *hash, void *val, void *arg);
static unsigned long do_iconv_hash_key_hash(todbc_hash_t *hash, void *key);
static int  do_iconv_hash_key_comp(todbc_hash_t *hash, void *key, void *val);

static void do_enc_hash_val_free(todbc_hash_t *hash, void *val, void *arg);
static unsigned long do_enc_hash_key_hash(todbc_hash_t *hash, void *key);
static int  do_enc_hash_key_comp(todbc_hash_t *hash, void *key, void *val);

#define CHK(x, y, n) if (strcasecmp(x, y)==0) return n

#define SET_SIZES(cs, enc, a,b) do {        \
  if (strcasecmp(enc->enc, cs)==0) {        \
    enc->char_size            = a;          \
    enc->variable_char_size   = b;          \
    return;                                 \
  }                                         \
} while (0)

static void do_set_sizes(todbc_enc_t *enc) {
  if (!enc) return;

  SET_SIZES("ISO-10646-UCS-2",       enc, 2, -1);
  SET_SIZES("UCS-2",                 enc, 2, -1);
  SET_SIZES("CSUNICODE",             enc, 2, -1);
  SET_SIZES("UCS-2BE",               enc, 2, -1);
  SET_SIZES("UNICODE-1-1",           enc, 2, -1);
  SET_SIZES("UNICODEBIG",            enc, 2, -1);
  SET_SIZES("CSUNICODE11",           enc, 2, -1);
  SET_SIZES("UCS-2LE",               enc, 2, -1);
  SET_SIZES("UNICODELITTLE",         enc, 2, -1);
  SET_SIZES("UTF-16",                enc, 2, -1);
  SET_SIZES("UTF-16BE",              enc, 2, -1);
  SET_SIZES("UTF-16LE",              enc, 2, -1);
  SET_SIZES("UCS-2-INTERNAL",        enc, 2, -1);
  SET_SIZES("UCS-2-SWAPPED",         enc, 2, -1);
  SET_SIZES("ISO-10646-UCS-4",       enc, 4, -1);
  SET_SIZES("UCS-4",                 enc, 4, -1);
  SET_SIZES("CSUCS4",                enc, 4, -1);
  SET_SIZES("UCS-4BE",               enc, 4, -1);
  SET_SIZES("UCS-4LE",               enc, 4, -1);
  SET_SIZES("UTF-32",                enc, 4, -1);
  SET_SIZES("UTF-32BE",              enc, 4, -1);
  SET_SIZES("UTF-32LE",              enc, 4, -1);
  SET_SIZES("UCS-4-INTERNAL",        enc, 4, -1);
  SET_SIZES("UCS-4-SWAPPED",         enc, 4, -1);

  SET_SIZES("UTF-8",                 enc, -1, 3);
  SET_SIZES("UTF8",                  enc, -1, 3);
  SET_SIZES("UTF-8-MAC",             enc, -1, 3);
  SET_SIZES("UTF8-MAC",              enc, -1, 3);

  SET_SIZES("CN-GB",                 enc, 2, -1);
  SET_SIZES("EUC-CN",                enc, 2, -1);
  SET_SIZES("EUCCN",                 enc, 2, -1);
  SET_SIZES("GB2312",                enc, 2, -1);
  SET_SIZES("CSGB2312",              enc, 2, -1);
  SET_SIZES("GBK",                   enc, 2, -1);
  SET_SIZES("CP936",                 enc, 2, -1);
  SET_SIZES("MS936",                 enc, 2, -1);
  SET_SIZES("WINDOWS-936",           enc, 2, -1);
  SET_SIZES("GB18030",               enc, 2, -1);

  // add more setup here after

  enc->char_size           = -1;
  enc->variable_char_size  = -1;
}

static int do_get_null_size(const char *enc, int *null_size);

// static int do_get_unicode_char_size(const char *enc) {
//   if (!enc) return -1;
// 
//   CHK("ISO-10646-UCS-2",       enc, 2);
//   CHK("UCS-2",                 enc, 2);
//   CHK("CSUNICODE",             enc, 2);
//   CHK("UCS-2BE",               enc, 2);
//   CHK("UNICODE-1-1",           enc, 2);
//   CHK("UNICODEBIG",            enc, 2);
//   CHK("CSUNICODE11",           enc, 2);
//   CHK("UCS-2LE",               enc, 2);
//   CHK("UNICODELITTLE",         enc, 2);
//   CHK("UTF-16",                enc, 2);
//   CHK("UTF-16BE",              enc, 2);
//   CHK("UTF-16LE",              enc, 2);
//   CHK("UCS-2-INTERNAL",        enc, 2);
//   CHK("UCS-2-SWAPPED",         enc, 2);
//   CHK("ISO-10646-UCS-4",       enc, 4);
//   CHK("UCS-4",                 enc, 4);
//   CHK("CSUCS4",                enc, 4);
//   CHK("UCS-4BE",               enc, 4);
//   CHK("UCS-4LE",               enc, 4);
//   CHK("UTF-32",                enc, 4);
//   CHK("UTF-32BE",              enc, 4);
//   CHK("UTF-32LE",              enc, 4);
//   CHK("UCS-4-INTERNAL",        enc, 4);
//   CHK("UCS-4-SWAPPED",         enc, 4);
// 
//   return -1;
// }

todbc_enc_t todbc_iconvset_enc(todbc_iconvset_t *cnvset, const char *enc) {
  do {
    if (!cnvset) break;
    if (!enc) break;

    if (!cnvset->enc_hash) {
      todbc_hash_conf_t conf = {0};
      conf.arg           = cnvset;
      conf.val_free      = do_enc_hash_val_free;
      conf.key_hash      = do_enc_hash_key_hash;
      conf.key_comp      = do_enc_hash_key_comp;
      conf.slots         = 7;
      cnvset->enc_hash   = todbc_hash_create(conf);
      if (!cnvset->enc_hash) break;
    }

    void *old = NULL;
    int found = 0;
    int r = todbc_hash_get(cnvset->enc_hash, (void*)enc, &old, &found);
    if (r) {
      DASSERT(found==0);
      DASSERT(old==NULL);
      break;
    }

    if (found) {
      DASSERT(old);
      todbc_enc_t *val = (todbc_enc_t*)old;
      return *val;
    }

    todbc_enc_t *val = (todbc_enc_t*)calloc(1, sizeof(*val));
    if (!val) break;
    do {
      if (snprintf(val->enc, sizeof(val->enc), "%s", enc)>=sizeof(val->enc)) {
        break;
      }
      do_set_sizes(val);

      if (do_get_null_size(val->enc, &val->null_size)) break;

      return *val;
    } while (0);

    free(val);
  } while (0);

  todbc_enc_t v = {0};
  v.char_size   = -1;
  v.null_size   = -1;

  return v;
}

todbc_iconv_t* todbc_iconvset_get(todbc_iconvset_t *cnvset, const char *enc_to, const char *enc_from) {
  if (!cnvset) return NULL;
  if (!enc_to) return NULL;
  if (!enc_from) return NULL;
  todbc_iconv_key_t key;
  key.enc_to      = enc_to;
  key.enc_from    = enc_from;

  if (!cnvset->iconv_hash) {
    todbc_hash_conf_t conf = {0};
    conf.arg           = cnvset;
    conf.val_free      = do_iconv_hash_val_free;
    conf.key_hash      = do_iconv_hash_key_hash;
    conf.key_comp      = do_iconv_hash_key_comp;
    conf.slots         = 7;
    cnvset->iconv_hash = todbc_hash_create(conf);
    if (!cnvset->iconv_hash) return NULL;
  }

  void *old = NULL;
  int found = 0;
  int r = todbc_hash_get(cnvset->iconv_hash, &key, &old, &found);
  if (r) {
    DASSERT(found==0);
    DASSERT(old==NULL);
    return NULL;
  }

  if (found) {
    DASSERT(old);
    todbc_iconv_t *val = (todbc_iconv_t*)old;
    // D("found [%p] for [%s->%s]", val, enc_from, enc_to);
    return val;
  }

  todbc_iconv_t *val = (todbc_iconv_t*)calloc(1, sizeof(*val));
  if (!val) return NULL;
  do {
    if (snprintf(val->enc_to, sizeof(val->enc_to), "%s", enc_to)>=sizeof(val->enc_to)) {
      break;
    }
    if (snprintf(val->enc_from, sizeof(val->enc_from), "%s", enc_from)>=sizeof(val->enc_from)) {
      break;
    }
    val->key.enc_to           = val->enc_to;
    val->key.enc_from         = val->enc_from;
    if (strcasecmp(enc_to, enc_from)==0) {
      val->direct = 1;
    }

    val->from = todbc_tls_iconv_enc(enc_from);
    val->to   = todbc_tls_iconv_enc(enc_to);

    val->cnv = iconv_open(enc_to, enc_from);
    if (val->cnv==invalid_iconv()) break;

    r = todbc_hash_put(cnvset->iconv_hash, &key, val);

    if (r) break;

    // D("created [%p] for [%s->%s]", val, enc_from, enc_to);
    return val;
  } while (0);

  todbc_iconv_free(val);
  return NULL;
}

iconv_t todbc_iconv_get(todbc_iconv_t *cnv) {
  if (!cnv) return invalid_iconv();
  return cnv->cnv;
}

// static int todbc_legal_chars_by_cnv(iconv_t cnv, const unsigned char *str, todbc_bytes_t *bc);

int todbc_iconv_get_legal_chars(todbc_iconv_t *cnv, const unsigned char *str, todbc_bytes_t *bc) {
  DASSERT(0);
  // if (!cnv) return -1;
  // if (bc->inbytes==0 || bc->inbytes > INT64_MAX) {
  //   DASSERT(bc->chars<=INT64_MAX);
  //   if (bc->chars > 0) {
  //     DASSERT(cnv->from_char_size==2 || cnv->from_char_size==4);
  //     bc->inbytes = ((size_t)cnv->from_char_size) * bc->chars;
  //     bc->chars = 0;
  //   }
  // } else {
  //   DASSERT(bc->chars==0);
  // }
  // return todbc_legal_chars_by_cnv(todbc_iconv_get(cnv), str, bc);
}

// static int todbc_legal_chars_by_block(iconv_t cnv, const unsigned char *str, todbc_bytes_t *bc);

// static int todbc_legal_chars_by_cnv(iconv_t cnv, const unsigned char *str, todbc_bytes_t *bc) {
//   size_t max_bytes = bc->inbytes;
//   if (max_bytes > INT64_MAX) max_bytes = (size_t)-1;
//   
//   if (max_bytes != (size_t)-1) {
//     return todbc_legal_chars_by_block(cnv, str, bc);
//   }
// 
//   memset(bc, 0, sizeof(*bc));
// 
//   size_t nbytes = 0;
//   size_t ch_bytes = 0;
// 
//   char buf[16];
//   char *inbuf = (char*)str;
//   char *outbuf;
//   size_t outbytes;
//   size_t inbytes;
// 
//   size_t n = 0;
// 
//   int r = 0;
//   inbytes = 1;
//   while (1) {
//     if (nbytes==max_bytes) break;
//     outbytes = sizeof(buf);
//     outbuf = buf;
// 
//     ch_bytes = inbytes;
//     n = iconv(cnv, &inbuf, &inbytes, &outbuf, &outbytes);
//     nbytes += 1;
//     if (n==(size_t)-1) {
//       int err = errno;
//       if (err!=EINVAL) {
//         E(".......");
//         r = -1;
//         break;
//       }
//       inbytes = ch_bytes + 1;
//       continue;
//     }
//     DASSERT(inbytes==0);
//     n = sizeof(buf) - outbytes;
// 
//     DASSERT(n==2);
//     if (buf[0]=='\0' && buf[1]=='\0') {
//       ch_bytes = 0;
//       break;
//     }
// 
//     bc->inbytes  += ch_bytes;
//     bc->chars    += 1;
//     bc->outbytes += 2;
// 
//     ch_bytes = 0;
//     inbytes = 1;
//   }
// 
//   outbytes = sizeof(buf);
//   outbuf = buf;
//   n = iconv(cnv, NULL, NULL, &outbuf, &outbytes);
// 
//   if (r) return -1;
//   if (n==(size_t)-1) return -1;
//   if (outbytes!=sizeof(buf)) return -1;
//   if (outbuf!=buf) return -1;
//   if (ch_bytes) return -1;
//   return 0;
// }

unsigned char* todbc_iconv_conv(todbc_iconv_t *cnv, todbc_buf_t *buf, const unsigned char *src, todbc_bytes_t *bc) {
  if (!buf) {
    buf = todbc_tls_buf();
    if (!buf) return NULL;
  }
  if (bc==NULL) {
    todbc_bytes_t x = {0};
    x.inbytes  = (size_t)-1;
    return todbc_iconv_conv(cnv, buf, src, &x);
  }

  DASSERT(buf);
  DASSERT(cnv);
  DASSERT(src);

  todbc_string_t s = todbc_string_init(cnv->from.enc, src, bc->inbytes);
  // D("total_bytes/bytes: %d/%zu/%zu", s.total_bytes, s.bytes);
  DASSERT(s.buf==src);
  todbc_string_t t = todbc_string_conv_to(&s, cnv->to.enc, NULL);
  DASSERT(t.buf);
  // bc->outbytes not counting size of null-terminator
  bc->outbytes = t.bytes;
  return (unsigned char*)t.buf;
}

size_t todbc_iconv_est_bytes(todbc_iconv_t *cnv, size_t inbytes) {
  DASSERT(cnv);
  DASSERT(inbytes<=INT64_MAX);
  const todbc_enc_t *from = &cnv->from;
  const todbc_enc_t *to   = &cnv->to;

  size_t outbytes = inbytes;
  do {
    if (from == to) break;

    size_t inchars = inbytes;
    if (from->char_size > 1) {
      inchars = (inbytes + (size_t)from->char_size - 1) / (size_t)from->char_size;
    }
    outbytes = inchars;
    size_t char_size = MAX_CHARACTER_SIZE;
    if (to->char_size > 0) {
      char_size = (size_t)to->char_size;
    } else if (to->variable_char_size > 0) {
      char_size = (size_t)to->variable_char_size;
    }
    outbytes *= char_size;
  } while (0);

  size_t nullbytes = MAX_CHARACTER_SIZE;
  if (to->null_size > 0) {
    nullbytes = (size_t)to->null_size;
  }
  // D("%s->%s: %zu->%zu", from->enc, to->enc, inbytes, outbytes);
  outbytes += nullbytes;
  // D("%s->%s: %zu->%zu", from->enc, to->enc, inbytes, outbytes);

  return outbytes;
}

size_t todbc_iconv_bytes(todbc_iconv_t *cnv, size_t inchars) {
  DASSERT(cnv);
  if (inchars >= INT64_MAX) return (size_t)-1;
  const todbc_enc_t *from = &cnv->from;
  if (from->char_size > 0) {
    return inchars * (size_t)from->char_size;
  }
  return (size_t)-1;
}

todbc_enc_t todbc_iconv_from(todbc_iconv_t *cnv) {
  DASSERT(cnv);
  return cnv->from;
}

todbc_enc_t todbc_iconv_to(todbc_iconv_t *cnv) {
  DASSERT(cnv);
  return cnv->to;
}

int todbc_iconv_raw(todbc_iconv_t *cnv, const unsigned char *src, size_t *slen, unsigned char *dst, size_t *dlen) {
  DASSERT(cnv);
  DASSERT(src && dst);
  DASSERT(slen && *slen < INT64_MAX);
  DASSERT(dlen && *dlen < INT64_MAX);

  const int    null_bytes = todbc_iconv_to(cnv).null_size;

  if (*dlen<=null_bytes) {
    D("target buffer too small to hold even null-terminator");
    *dlen = 0;  // while slen does not change
    return -1;
  }

  char     *inbuf    = (char*)src;
  size_t    inbytes  = *slen;
  char     *outbuf   = (char*)dst;
  size_t    outbytes = *dlen;

  size_t n = iconv(cnv->cnv, &inbuf, &inbytes, &outbuf, &outbytes);
  int e = 0;
  if (n==(size_t)-1) {
    e = errno;
    D("iconv failed: [%s->%s]:[%d]%s", cnv->from.enc, cnv->to.enc, e, strerror(e));
  } else {
    DASSERT(n==0);
  }

  const size_t inremain   = inbytes;
  const size_t outremain  = outbytes;
  *slen = inremain;
  *dlen = outremain;

  // writing null-terminator to make dest a real string
  DASSERT(outbytes <= INT64_MAX);
  if (outbytes < null_bytes) {
    D("target buffer too small to hold null-terminator");
    return -1;
  } else {
    for (int i=0; i<null_bytes; ++i) {
      outbuf[i] = '\0';
    }
  }

  iconv(cnv->cnv, NULL, NULL, NULL, NULL);

  return 0;
}

todbc_string_t todbc_iconv_conv2(todbc_iconv_t *cnv, todbc_buf_t *buf, const unsigned char *src, size_t *slen) {
  const todbc_string_t nul = {0};
  if (!buf) {
    buf = todbc_tls_buf();
    if (!buf) return nul;
  }
  DASSERT(cnv);
  DASSERT(src);

  size_t inbytes = (size_t)-1;
  if (slen && *slen <= INT64_MAX) inbytes = *slen;

  todbc_string_t in = todbc_string_init(todbc_iconv_from(cnv).enc, src, inbytes);
  if (in.buf!=src) return nul;
  if (in.bytes > INT64_MAX) return nul;
  inbytes = in.bytes;

  size_t    outblock = todbc_iconv_est_bytes(cnv, in.bytes);
  if (outblock > INT64_MAX) return nul;

  unsigned char *out    = todbc_buf_alloc(buf, outblock);
  if (!out) return nul;

  const unsigned char *inbuf   = src;
  unsigned char       *outbuf  = out;


  size_t outbytes = outblock;
  int r = todbc_iconv_raw(cnv, inbuf, &inbytes, outbuf, &outbytes);
  if (slen) *slen = inbytes;
  if (r) return nul;
  todbc_string_t s = {0};
  s.buf         = outbuf;
  s.bytes       = outblock - outbytes;
  s.total_bytes = s.bytes;
  return s;
}

// static int todbc_legal_chars_by_block(iconv_t cnv, const unsigned char *str, todbc_bytes_t *bc) {
//   int r = 0;
//   size_t max_bytes = bc->inbytes;
//   char buf[1024*16];
//   memset(bc, 0, sizeof(*bc));
//   char *inbuf = (char*)str;
//   while (bc->inbytes<max_bytes) {
//     size_t inbytes  = max_bytes - bc->inbytes;
//     size_t outbytes = sizeof(buf);
//     char  *outbuf   = buf;
//     size_t n = iconv(cnv, &inbuf, &inbytes, &outbuf, &outbytes);
//     int err = 0;
//     if (n==(size_t)-1) {
//       err = errno;
//       if (err!=E2BIG && err!=EINVAL) r = -1;
//     } else {
//       DASSERT(n==0);
//     }
//     if (inbytes == max_bytes - bc->inbytes) {
//       r = -1;
//     }
//     bc->inbytes  += max_bytes - bc->inbytes - inbytes;
//     bc->outbytes += sizeof(buf) - outbytes;
//     if (r) break;
//   }
//   bc->chars = bc->outbytes / 2;
//   iconv(cnv, NULL, NULL, NULL, NULL);
//   return r ? -1 : 0;
// }

static void todbc_iconv_free(todbc_iconv_t *val) {
  if (!val) return;
  if (val->cnv!=invalid_iconv()) {
    iconv_close(val->cnv);
    val->cnv = invalid_iconv();
  }
  free(val);
}

// http://www.cse.yorku.ca/~oz/hash.html
static const unsigned long hash_seed = 5381;
static unsigned long hashing(unsigned long hash, const unsigned char *str);

static void do_enc_hash_val_free(todbc_hash_t *hash, void *val, void *arg) {
  todbc_iconvset_t *cnv = (todbc_iconvset_t*)arg;
  todbc_enc_t *v        = (todbc_enc_t*)val;
  DASSERT(hash);
  DASSERT(cnv);
  DASSERT(v);
  free(v);
}

static unsigned long do_enc_hash_key_hash(todbc_hash_t *hash, void *key) {
  const char *k = (const char*)key;
  DASSERT(k);
  unsigned long h = hash_seed;
  h = hashing(h, (const unsigned char*)k);
  return h;
}

static int  do_enc_hash_key_comp(todbc_hash_t *hash, void *key, void *val) {
  const char  *k = (const char*)key;
  todbc_enc_t *v = (todbc_enc_t*)val;
  if (strcasecmp(k, v->enc)) return 1;
  return 0;
}

static int do_get_null_size(const char *enc, int *null_size) {
  iconv_t cnv = iconv_open(enc, UTF8_ENC);
  if (cnv==(iconv_t)-1) return -1;

  char         src[]   = "";
  char         dst[64];

  char      *inbuf    = src;
  size_t     inbytes  = 1;
  char      *outbuf   = dst;
  size_t     outbytes = sizeof(dst);

  size_t n = iconv(cnv, &inbuf, &inbytes, &outbuf, &outbytes);
  DASSERT(n==0);
  DASSERT(inbytes==0);

  int size = (int)(sizeof(dst) - outbytes);

  iconv_close(cnv);

  if (null_size) *null_size = size;

  return 0;
}

static void do_iconv_hash_val_free(todbc_hash_t *hash, void *val, void *arg) {
  todbc_iconvset_t *cnv = (todbc_iconvset_t*)arg;
  todbc_iconv_t *v = (todbc_iconv_t*)val;
  DASSERT(hash);
  DASSERT(cnv);
  DASSERT(v);
  todbc_iconv_free(v);
}

static unsigned long do_iconv_hash_key_hash(todbc_hash_t *hash, void *key) {
  todbc_iconv_key_t *k = (todbc_iconv_key_t*)key;
  DASSERT(k);
  unsigned long h = hash_seed;
  h = hashing(h, (const unsigned char*)k->enc_to);
  h = hashing(h, (const unsigned char*)k->enc_from);
  return h;
}

static int do_iconv_hash_key_comp(todbc_hash_t *hash, void *key, void *val) {
  todbc_iconv_key_t *k = (todbc_iconv_key_t*)key;
  todbc_iconv_t *v = (todbc_iconv_t*)val;
  if (strcasecmp(k->enc_to, v->key.enc_to)) return 1;
  if (strcasecmp(k->enc_from, v->key.enc_from)) return 1;
  return 0;
}

static unsigned long hashing(unsigned long hash, const unsigned char *str) {
  unsigned long c;
  while ((c = *str++)!=0) {
    hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
  }

  return hash;
}

