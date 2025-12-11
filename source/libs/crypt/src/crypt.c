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
#include "crypt.h"
#include "taes.h"

extern int32_t CBC_DecryptImpl(SCryptOpts *opts);
extern int32_t CBC_EncryptImpl(SCryptOpts *opts);
extern int32_t Builtin_CBC_DecryptImpl(SCryptOpts *opts);
extern int32_t Builtin_CBC_EncryptImpl(SCryptOpts *opts);

int32_t CBC_Encrypt(SCryptOpts *opts) {
#if defined(TD_ENTERPRISE) && defined(LINUX)
  return CBC_EncryptImpl(opts);
#else
  return Builtin_CBC_EncryptImpl(opts);
#endif
}
int32_t CBC_Decrypt(SCryptOpts *opts) {
#if defined(TD_ENTERPRISE) && defined(LINUX)
  return CBC_DecryptImpl(opts);
#else
  return Builtin_CBC_DecryptImpl(opts);
#endif
}

int32_t Builtin_CBC_Encrypt(SCryptOpts *opts) { return Builtin_CBC_EncryptImpl(opts); }
int32_t Builtin_CBC_Decrypt(SCryptOpts *opts) { return Builtin_CBC_DecryptImpl(opts); }

#if !defined(TD_ENTERPRISE) && !defined(TD_ASTRA)
int32_t CBC_EncryptImpl(SCryptOpts *opts) {
  memcpy(opts->result, opts->source, opts->len);
  return opts->len;
}
int32_t CBC_DecryptImpl(SCryptOpts *opts) {
  memcpy(opts->result, opts->source, opts->len);
  return opts->len;
}
#endif

static void pkcs7_padding_pad_buffer(uint8_t *buffer, size_t data_length, uint8_t modulus) {
  uint8_t pad_byte = modulus - (data_length % modulus);
  int     i = 0;

  while (i < pad_byte) {
    buffer[data_length + i] = pad_byte;
    i++;
  }
}

static size_t pkcs7_padding_data_length(uint8_t *buffer, size_t buffer_size, uint8_t modulus) {
  /* test for valid buffer size */
  if (buffer_size % modulus != 0 || buffer_size < modulus) {
    return 0;
  }
  uint8_t padding_value;
  padding_value = buffer[buffer_size - 1];
  /* test for valid padding value */
  if (padding_value < 1 || padding_value > modulus) {
    return buffer_size;
  }
  /* buffer must be at least padding_value + 1 in size */
  if (buffer_size < padding_value + 1) {
    return 0;
  }
  uint8_t count = 1;
  buffer_size--;
  for (; count < padding_value; count++) {
    buffer_size--;
    if (buffer[buffer_size] != padding_value) {
      return 0;
    }
  }
  return buffer_size;
}

#define SM4_BLOCKLEN 16  // Block length in bytes - SM4 is 128b block only

uint32_t tsm4_encrypt_len(int32_t len) {
  uint32_t paddedlen = len + SM4_BLOCKLEN - (len % SM4_BLOCKLEN);

  return paddedlen;
}

int32_t taosSm4Encrypt(uint8_t *key, int32_t keylen, uint8_t *pBuf, int32_t len) {
  int32_t    count = 0;
  SCryptOpts opts;

  pkcs7_padding_pad_buffer(key, keylen, SM4_BLOCKLEN);
  pkcs7_padding_pad_buffer(pBuf, len, SM4_BLOCKLEN);

  opts.len = tsm4_encrypt_len(len);
  opts.source = pBuf;
  opts.unitLen = 16;
  opts.result = pBuf;

  memset(opts.key, 0, ENCRYPT_KEY_LEN + 1);
  memcpy(opts.key, key, ENCRYPT_KEY_LEN);

  count = CBC_Encrypt(&opts);

  return count;
}

int32_t taosSm4Decrypt(uint8_t *key, int32_t keylen, uint8_t *pBuf, int32_t len) {
  int32_t    count = 0;
  SCryptOpts opts;

  pkcs7_padding_pad_buffer(key, keylen, SM4_BLOCKLEN);

  opts.len = len;
  opts.source = pBuf;
  opts.unitLen = 16;
  opts.result = pBuf;

  memset(opts.key, 0, ENCRYPT_KEY_LEN + 1);
  memcpy(opts.key, key, ENCRYPT_KEY_LEN);

  count = CBC_Decrypt(&opts);
  count = pkcs7_padding_data_length(pBuf, count, SM4_BLOCKLEN);

  return count;
}

uint32_t taes_encrypt_len(int32_t len) {
  uint32_t paddedlen = len + AES_BLOCKLEN - (len % AES_BLOCKLEN);

  return paddedlen;
}

int32_t taosAesEncrypt(uint8_t *key, int32_t keylen, uint8_t *pBuf, int32_t len, const uint8_t *iv) {
  uint32_t       encryptlen = taes_encrypt_len(len);
  int32_t        blocks = encryptlen / AES_BLOCKLEN;
  struct AES_ctx ctx;

  pkcs7_padding_pad_buffer(key, keylen, AES_BLOCKLEN);
  pkcs7_padding_pad_buffer(pBuf, len, AES_BLOCKLEN);

  if (iv) {
    AES_init_ctx_iv(&ctx, key, iv);

    AES_CBC_encrypt_buffer(&ctx, pBuf, encryptlen);
  } else {
    AES_init_ctx(&ctx, key);

    for (int32_t block = 0; block < blocks; ++block) {
      AES_ECB_encrypt(&ctx, pBuf + block * AES_BLOCKLEN);
    }
  }

  return encryptlen;
}

int32_t taosAesDecrypt(uint8_t *key, int32_t keylen, uint8_t *pBuf, int32_t len, const uint8_t *iv) {
  int32_t        blocks = len / AES_BLOCKLEN;
  size_t         datalen = 0;
  struct AES_ctx ctx;

  pkcs7_padding_pad_buffer(key, keylen, AES_BLOCKLEN);

  if (iv) {
    AES_init_ctx_iv(&ctx, key, iv);

    AES_CBC_decrypt_buffer(&ctx, pBuf, len);
  } else {
    AES_init_ctx(&ctx, key);

    for (int32_t block = 0; block < blocks; ++block) {
      AES_ECB_decrypt(&ctx, pBuf + block * AES_BLOCKLEN);
    }
  }

  datalen = pkcs7_padding_data_length(pBuf, len, AES_BLOCKLEN);

  return datalen;
}
