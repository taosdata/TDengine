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

#ifndef _CRYPT_H_
#define _CRYPT_H_
#include "tdef.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SCryptOpts {
  int32_t len;
  char*   source;
  char*   result;
  int32_t unitLen;
  char    key[ENCRYPT_KEY_LEN + 1];
  const char*   pOsslAlgrName;
} SCryptOpts;

typedef struct SSpillCryptCtx SSpillCryptCtx;

int32_t CBC_Decrypt(SCryptOpts* opts);
int32_t CBC_Encrypt(SCryptOpts* opts);
int32_t Builtin_CBC_Encrypt(SCryptOpts* opts);
int32_t Builtin_CBC_Decrypt(SCryptOpts* opts);

// True when the query-spill encryption scope is enabled; every consumer of
// spill encryption must gate on this single predicate.
bool    taosSpillCryptEnabled(void);
// On-disk size of one encrypted spill image holding plainLen plaintext
// bytes: [4-byte plaintext length][CBC ciphertext padded to 16 bytes].
// Validates plainLen under the same bounds as taosEncryptSpillBlock and
// fails with TSDB_CODE_INVALID_PARA when it is not positive or would
// overflow; an empty spill block is not a legal image on either side.
int32_t taosSpillCryptEncodedLen(int32_t plainLen, int32_t* pEncodedLen);

// Owns one memory-only key and reusable buffers for one spill file. Returned
// data pointers remain owned by the context and are valid until its next
// operation. initialPlainCapacity may be zero to allocate buffers lazily.
int32_t taosCreateSpillCryptCtx(SSpillCryptCtx** ppCtx,
                                int32_t          initialPlainCapacity);
void    taosDestroySpillCryptCtx(SSpillCryptCtx** ppCtx);
int32_t taosEncryptSpillBlock(SSpillCryptCtx* pCtx, const char* pPlain,
                              int32_t plainLen, char** ppEncrypted,
                              int32_t* pEncryptedLen);
int32_t taosPrepareSpillBlockRead(SSpillCryptCtx* pCtx, int32_t encryptedLen,
                                  char** ppEncrypted);
// pEncrypted must be the pointer taosPrepareSpillBlockRead (or
// taosEncryptSpillBlock) returned; any other buffer is rejected with
// TSDB_CODE_INVALID_PARA. Passing it explicitly lets this call verify that
// the read protocol was followed instead of trusting hidden state.
int32_t taosDecryptSpillBlock(SSpillCryptCtx* pCtx, const char* pEncrypted,
                              int32_t encryptedLen, char** ppPlain,
                              int32_t* pPlainLen);

int32_t  taosSm4Encrypt(uint8_t* key, int32_t keylen, uint8_t* pBuf, int32_t len);
int32_t  taosSm4Decrypt(uint8_t* key, int32_t keylen, uint8_t* pBuf, int32_t len);
uint32_t tsm4_encrypt_len(int32_t len);
int32_t  taosAesEncrypt(uint8_t* key, int32_t keylen, uint8_t* pBuf, int32_t len, const uint8_t* iv);
int32_t  taosAesDecrypt(uint8_t* key, int32_t keylen, uint8_t* pBuf, int32_t len, const uint8_t* iv);
uint32_t taes_encrypt_len(int32_t len);

#ifdef __cplusplus
}
#endif

#endif  // _CRYPT_H_
