/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "os.h"
#include "taoserror.h"
#include "taes.h"
#include "tglobal.h"

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

enum {
  SPILL_CRYPT_BLOCK_SIZE = 16,
  SPILL_CRYPT_HEADER_SIZE = sizeof(int32_t),
};

struct SSpillCryptCtx {
  char*   pBuffer;
  int32_t cipherCapacity;
  char    key[ENCRYPT_KEY_LEN + 1];
};

static void initSpillCryptOpts(SCryptOpts* pOpts,
                               const char  key[ENCRYPT_KEY_LEN + 1],
                               int32_t length, char* pSource, char* pResult) {
  pOpts->len = length;
  pOpts->source = pSource;
  pOpts->result = pResult;
  pOpts->unitLen = 16;
  pOpts->pOsslAlgrName = TSDB_ENCRYPT_ALGR_SM4_NAME;
  memcpy(pOpts->key, key, ENCRYPT_KEY_LEN);
}

bool taosSpillCryptEnabled(void) {
  return (tsiEncryptScope & DND_CS_QUERY_SPILL) != 0;
}

static int32_t spillCipherLength(int32_t plainLen, int32_t* pCipherLen) {
  if (pCipherLen == NULL || plainLen <= 0 ||
      plainLen > INT32_MAX - (SPILL_CRYPT_BLOCK_SIZE - 1)) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t cipherLen = ENCRYPTED_LEN(plainLen);
  if (cipherLen > INT32_MAX - SPILL_CRYPT_HEADER_SIZE) {
    return TSDB_CODE_INVALID_PARA;
  }
  *pCipherLen = cipherLen;
  return TSDB_CODE_SUCCESS;
}

int32_t taosSpillCryptEncodedLen(int32_t plainLen, int32_t* pEncodedLen) {
  if (pEncodedLen == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t cipherLen = 0;
  int32_t code = spillCipherLength(plainLen, &cipherLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  *pEncodedLen = SPILL_CRYPT_HEADER_SIZE + cipherLen;
  return TSDB_CODE_SUCCESS;
}

static int32_t ensureSpillCryptCapacity(SSpillCryptCtx* pCtx,
                                        int32_t         cipherLen) {
  if (pCtx == NULL || cipherLen < 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pCtx->pBuffer != NULL && pCtx->cipherCapacity >= cipherLen) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t requiredSize = SPILL_CRYPT_HEADER_SIZE + (int64_t)cipherLen * 2;
  char* pBuffer = taosMemoryRealloc(pCtx->pBuffer, (size_t)requiredSize);
  if (pBuffer == NULL) {
    return terrno;
  }
  pCtx->pBuffer = pBuffer;
  pCtx->cipherCapacity = cipherLen;
  return TSDB_CODE_SUCCESS;
}

int32_t taosCreateSpillCryptCtx(SSpillCryptCtx** ppCtx,
                                int32_t          initialPlainCapacity) {
  if (ppCtx == NULL || initialPlainCapacity < 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppCtx = NULL;

  SSpillCryptCtx* pCtx = taosMemoryCalloc(1, sizeof(SSpillCryptCtx));
  if (pCtx == NULL) {
    return terrno;
  }

  if (initialPlainCapacity > 0) {
    int32_t cipherLen = 0;
    int32_t code = spillCipherLength(initialPlainCapacity, &cipherLen);
    if (code == TSDB_CODE_SUCCESS) {
      code = ensureSpillCryptCapacity(pCtx, cipherLen);
    }
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(pCtx);
      return code;
    }
  }

  taosSafeRandBytes((uint8_t*)pCtx->key, ENCRYPT_KEY_LEN);
  pCtx->key[ENCRYPT_KEY_LEN] = 0;
  *ppCtx = pCtx;
  return TSDB_CODE_SUCCESS;
}

void taosDestroySpillCryptCtx(SSpillCryptCtx** ppCtx) {
  if (ppCtx == NULL || *ppCtx == NULL) {
    return;
  }

  SSpillCryptCtx* pCtx = *ppCtx;
  memset(pCtx->key, 0, sizeof(pCtx->key));
  taosMemoryFreeClear(pCtx->pBuffer);
  taosMemoryFreeClear(*ppCtx);
}

int32_t taosEncryptSpillBlock(SSpillCryptCtx* pCtx, const char* pPlain,
                              int32_t plainLen, char** ppEncrypted,
                              int32_t* pEncryptedLen) {
  if (pCtx == NULL || pPlain == NULL || ppEncrypted == NULL ||
      pEncryptedLen == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t cipherLen = 0;
  int32_t code = spillCipherLength(plainLen, &cipherLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = ensureSpillCryptCapacity(pCtx, cipherLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  char* pScratch =
      pCtx->pBuffer + SPILL_CRYPT_HEADER_SIZE + pCtx->cipherCapacity;
  memmove(pScratch, pPlain, plainLen);
  if (cipherLen > plainLen) {
    memset(pScratch + plainLen, 0, cipherLen - plainLen);
  }

  SCryptOpts cryptOpts = {0};
  initSpillCryptOpts(&cryptOpts, pCtx->key, cipherLen, pScratch,
                     pCtx->pBuffer + SPILL_CRYPT_HEADER_SIZE);
  int32_t resultLen = CBC_Encrypt(&cryptOpts);
  if (resultLen != cipherLen) {
    return terrno != TSDB_CODE_SUCCESS ? terrno : TSDB_CODE_INTERNAL_ERROR;
  }

  memcpy(pCtx->pBuffer, &plainLen, sizeof(plainLen));
  *ppEncrypted = pCtx->pBuffer;
  *pEncryptedLen = SPILL_CRYPT_HEADER_SIZE + cipherLen;
  return TSDB_CODE_SUCCESS;
}

int32_t taosPrepareSpillBlockRead(SSpillCryptCtx* pCtx, int32_t encryptedLen,
                                  char** ppEncrypted) {
  if (pCtx == NULL || ppEncrypted == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (encryptedLen <= SPILL_CRYPT_HEADER_SIZE) {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  int32_t cipherLen = encryptedLen - SPILL_CRYPT_HEADER_SIZE;
  if (cipherLen % SPILL_CRYPT_BLOCK_SIZE != 0) {
    return TSDB_CODE_FILE_CORRUPTED;
  }
  int32_t code = ensureSpillCryptCapacity(pCtx, cipherLen);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  *ppEncrypted = pCtx->pBuffer;
  return TSDB_CODE_SUCCESS;
}

int32_t taosDecryptSpillBlock(SSpillCryptCtx* pCtx, const char* pEncrypted,
                              int32_t encryptedLen, char** ppPlain,
                              int32_t* pPlainLen) {
  if (pCtx == NULL || pEncrypted == NULL || ppPlain == NULL ||
      pPlainLen == NULL || encryptedLen <= SPILL_CRYPT_HEADER_SIZE) {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  // The ciphertext must be the context-owned buffer that
  // taosPrepareSpillBlockRead or taosEncryptSpillBlock handed back. The
  // plaintext is written to the scratch area that follows it, sized from
  // cipherCapacity, so accepting an outside buffer would mean growing this
  // one - which moves the very bytes the caller is pointing at. Requiring
  // the pointer keeps that impossible instead of merely unlikely.
  if (pEncrypted != pCtx->pBuffer) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t cipherLen = encryptedLen - SPILL_CRYPT_HEADER_SIZE;
  if (cipherLen % SPILL_CRYPT_BLOCK_SIZE != 0 ||
      cipherLen > pCtx->cipherCapacity) {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  int32_t plainLen = 0;
  memcpy(&plainLen, pEncrypted, sizeof(plainLen));
  if (plainLen < 0 || plainLen > cipherLen) {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  char* pPlain = pCtx->pBuffer + SPILL_CRYPT_HEADER_SIZE + pCtx->cipherCapacity;
  SCryptOpts cryptOpts = {0};
  initSpillCryptOpts(&cryptOpts, pCtx->key, cipherLen,
                     (char*)pEncrypted + SPILL_CRYPT_HEADER_SIZE, pPlain);
  int32_t resultLen = CBC_Decrypt(&cryptOpts);
  if (resultLen != cipherLen) {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  *ppPlain = pPlain;
  *pPlainLen = plainLen;
  return TSDB_CODE_SUCCESS;
}

#if !defined(TD_ENTERPRISE) && !defined(TD_ASTRA)
int32_t CBC_EncryptImpl(SCryptOpts *opts) {
  memcpy(opts->result, opts->source, opts->len);
  return opts->len;
}
int32_t CBC_DecryptImpl(SCryptOpts *opts) {
  memcpy(opts->result, opts->source, opts->len);
  return opts->len;
}

int32_t Builtin_CBC_EncryptImpl(SCryptOpts *opts) {
  memcpy(opts->result, opts->source, opts->len);
  return opts->len;
}
int32_t Builtin_CBC_DecryptImpl(SCryptOpts *opts) {
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
  SCryptOpts opts = {0};

  pkcs7_padding_pad_buffer(key, keylen, SM4_BLOCKLEN);
  pkcs7_padding_pad_buffer(pBuf, len, SM4_BLOCKLEN);

  opts.len = tsm4_encrypt_len(len);
  opts.source = pBuf;
  opts.unitLen = 16;
  opts.result = pBuf;

  memset(opts.key, 0, ENCRYPT_KEY_LEN + 1);
  memcpy(opts.key, key, ENCRYPT_KEY_LEN);

  count = Builtin_CBC_Encrypt(&opts);

  return count;
}

int32_t taosSm4Decrypt(uint8_t *key, int32_t keylen, uint8_t *pBuf, int32_t len) {
  int32_t    count = 0;
  SCryptOpts opts = {0};

  pkcs7_padding_pad_buffer(key, keylen, SM4_BLOCKLEN);

  opts.len = len;
  opts.source = pBuf;
  opts.unitLen = 16;
  opts.result = pBuf;

  memset(opts.key, 0, ENCRYPT_KEY_LEN + 1);
  memcpy(opts.key, key, ENCRYPT_KEY_LEN);

  count = Builtin_CBC_Decrypt(&opts);
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
