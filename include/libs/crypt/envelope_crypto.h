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

#ifndef TDENGINE_INCLUDE_LIBS_CRYPT_ENVELOPE_CRYPTO_H_
#define TDENGINE_INCLUDE_LIBS_CRYPT_ENVELOPE_CRYPTO_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SEnvelopePublicKey SEnvelopePublicKey;
typedef struct SEnvelopeBatch     SEnvelopeBatch;

enum { ENVELOPE_CRYPTO_KEY_ID_LEN = 32 };

/* A zero cap disables the role-specific limit; the 1 MiB protocol limit still applies. */
#define ENVELOPE_CRYPTO_NO_ROLE_CAP UINT32_C(0)

typedef enum EEnvelopeCryptoCode {
  ENVELOPE_CRYPTO_SUCCESS = 0,
  ENVELOPE_CRYPTO_ERROR_INVALID_PARAMETER = -1,
  ENVELOPE_CRYPTO_ERROR_OUT_OF_MEMORY = -2,
  ENVELOPE_CRYPTO_ERROR_STATE_INVALID = -3,
  ENVELOPE_CRYPTO_ERROR_LENGTH_INVALID = -24,
  ENVELOPE_CRYPTO_ERROR_KEY_INVALID = -31,
  ENVELOPE_CRYPTO_ERROR_RANDOM_FAILED = -32,
  ENVELOPE_CRYPTO_ERROR_RSA_WRAP_FAILED = -33,
  ENVELOPE_CRYPTO_ERROR_PLAINTEXT_UTF8_INVALID = -34,
  ENVELOPE_CRYPTO_ERROR_ENCRYPT_FAILED = -100,
} EEnvelopeCryptoCode;

/* Returns NULL when code is a local API/runtime error without a canonical ID. */
const char *envelopeCryptoCanonicalId(int32_t code);

/* key must point to NULL on entry. */
int32_t envelopeLoadPublicKey(const char *pem_path, SEnvelopePublicKey **key);
/* key must point to NULL on entry. */
int32_t envelopeLoadPublicKeyBytes(const uint8_t *pem, int32_t pem_len, SEnvelopePublicKey **key);

/* Copies the public key identifier into key_id. Returns INVALID_PARAMETER for a NULL argument. */
int32_t envelopeGetKeyId(const SEnvelopePublicKey *key, uint8_t key_id[ENVELOPE_CRYPTO_KEY_ID_LEN]);

/*
 * batch must point to NULL on entry. The batch retains its own reference to
 * key until finish or abort.
 */
int32_t envelopeBatchBegin(const SEnvelopePublicKey *key, SEnvelopeBatch **batch);

/*
 * encoded must point to NULL on entry. encoded_len is set to zero before
 * validation. On success, encoded is NUL-terminated and encoded_len excludes
 * that NUL.
 * The implementation allocates encoded with taosMemoryMalloc(); the caller
 * owns it and must release it with taosMemoryFree().
 * Any error, including invalid output arguments, invalidates batch. Encoded
 * fields may be delivered only after a successful envelopeBatchFinish().
 */
int32_t envelopeBatchEncryptField(SEnvelopeBatch *batch, const uint8_t *plain, uint32_t plain_len,
                                  uint32_t max_plain_len, char **encoded, uint32_t *encoded_len);

/*
 * Finishes and releases *batch, then sets it to NULL. A NULL batch pointer
 * returns INVALID_PARAMETER; a NULL, failed, or empty batch returns STATE_INVALID.
 */
int32_t envelopeBatchFinish(SEnvelopeBatch **batch);

/*
 * Aborts and releases *batch, then sets it to NULL. A NULL batch pointer
 * returns INVALID_PARAMETER; a NULL *batch value is idempotent success.
 */
int32_t envelopeBatchAbort(SEnvelopeBatch **batch);

/*
 * Releases *key and sets it to NULL. A NULL key pointer returns
 * INVALID_PARAMETER; a NULL *key value is idempotent success.
 */
int32_t envelopeReleasePublicKey(SEnvelopePublicKey **key);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_INCLUDE_LIBS_CRYPT_ENVELOPE_CRYPTO_H_
