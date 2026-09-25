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

#include "envelope_crypto.h"

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <string.h>

#include <openssl/bio.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rand.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>

#include "osDef.h"
#include "osMemPool.h"
#include "osMemory.h"

enum {
  ENVELOPE_CRYPTO_RSA_BITS = 2048,
  ENVELOPE_CRYPTO_RSA_BYTES = 256,
  ENVELOPE_CRYPTO_AES_KEY_LEN = 32,
  ENVELOPE_CRYPTO_NONCE_LEN = 12,
  ENVELOPE_CRYPTO_TAG_LEN = 16,
  ENVELOPE_CRYPTO_FIXED_RAW_LEN = 68,
  ENVELOPE_CRYPTO_PROTOCOL_MAX_PLAIN_LEN = 1024 * 1024,
  ENVELOPE_CRYPTO_MAX_PEM_LEN = 64 * 1024,
  ENVELOPE_CRYPTO_PROTOCOL_VERSION = 1,
  ENVELOPE_CRYPTO_ALGORITHM_AES_256_GCM = 1,
};

static const char kEnvelopePrefix[] = "XNODE_DSN_V1:";

const char *envelopeCryptoCanonicalId(int32_t code) {
  switch (code) {
    case ENVELOPE_CRYPTO_ERROR_LENGTH_INVALID:
      return "ENVELOPE_LENGTH_INVALID";
    case ENVELOPE_CRYPTO_ERROR_KEY_INVALID:
      return "ENVELOPE_KEY_INVALID";
    case ENVELOPE_CRYPTO_ERROR_RANDOM_FAILED:
      return "ENVELOPE_RANDOM_FAILED";
    case ENVELOPE_CRYPTO_ERROR_RSA_WRAP_FAILED:
      return "ENVELOPE_RSA_WRAP_FAILED";
    case ENVELOPE_CRYPTO_ERROR_PLAINTEXT_UTF8_INVALID:
      return "ENVELOPE_PLAINTEXT_UTF8_INVALID";
    default:
      return NULL;
  }
}

struct SEnvelopePublicKey {
  EVP_PKEY *pkey;
  uint8_t   key_id[ENVELOPE_CRYPTO_KEY_ID_LEN];
};

struct SEnvelopeBatch {
  EVP_PKEY *pkey;
  uint8_t   key_id[ENVELOPE_CRYPTO_KEY_ID_LEN];
  uint8_t   aes_key[ENVELOPE_CRYPTO_AES_KEY_LEN];
  uint8_t   wrapped_key[ENVELOPE_CRYPTO_RSA_BYTES];
  uint16_t  wrapped_key_len;
  uint32_t  field_count;
  bool      secret_initialized;
  bool      failed;
};

static void envelopeClearBatchSecrets(SEnvelopeBatch *batch) {
  OPENSSL_cleanse(batch->aes_key, sizeof(batch->aes_key));
  OPENSSL_cleanse(batch->wrapped_key, sizeof(batch->wrapped_key));
  batch->wrapped_key_len = 0;
  batch->secret_initialized = false;
}

static void envelopeFreeBatch(SEnvelopeBatch *batch) {
  if (batch == NULL) {
    return;
  }

  EVP_PKEY_free(batch->pkey);
  envelopeClearBatchSecrets(batch);
  taosMemoryFree(batch);
}

static int32_t envelopeFailBatch(SEnvelopeBatch *batch, int32_t code) {
  if (batch != NULL && !batch->failed) {
    envelopeClearBatchSecrets(batch);
    batch->failed = true;
  }
  ERR_clear_error();
  return code;
}

static bool envelopeIsValidUtf8(const uint8_t *plain, uint32_t plain_len) {
  uint32_t offset = 0;

  while (offset < plain_len) {
    const uint8_t first = plain[offset++];
    if (first <= UINT8_C(0x7f)) {
      continue;
    }

    if (first >= UINT8_C(0xc2) && first <= UINT8_C(0xdf)) {
      if (offset >= plain_len || plain[offset] < UINT8_C(0x80) || plain[offset] > UINT8_C(0xbf)) {
        return false;
      }
      ++offset;
      continue;
    }

    if (first >= UINT8_C(0xe0) && first <= UINT8_C(0xef)) {
      if (plain_len - offset < 2) {
        return false;
      }
      const uint8_t second = plain[offset];
      const uint8_t third = plain[offset + 1];
      if (third < UINT8_C(0x80) || third > UINT8_C(0xbf) || (first == UINT8_C(0xe0) && second < UINT8_C(0xa0)) ||
          (first == UINT8_C(0xed) && second > UINT8_C(0x9f)) || second < UINT8_C(0x80) || second > UINT8_C(0xbf)) {
        return false;
      }
      offset += 2;
      continue;
    }

    if (first >= UINT8_C(0xf0) && first <= UINT8_C(0xf4)) {
      if (plain_len - offset < 3) {
        return false;
      }
      const uint8_t second = plain[offset];
      const uint8_t third = plain[offset + 1];
      const uint8_t fourth = plain[offset + 2];
      if (third < UINT8_C(0x80) || third > UINT8_C(0xbf) || fourth < UINT8_C(0x80) || fourth > UINT8_C(0xbf) ||
          (first == UINT8_C(0xf0) && second < UINT8_C(0x90)) || (first == UINT8_C(0xf4) && second > UINT8_C(0x8f)) ||
          second < UINT8_C(0x80) || second > UINT8_C(0xbf)) {
        return false;
      }
      offset += 3;
      continue;
    }

    return false;
  }

  return true;
}

static int32_t envelopeWrapBatchKey(SEnvelopeBatch *batch) {
  if (RAND_bytes(batch->aes_key, sizeof(batch->aes_key)) != 1) {
    return ENVELOPE_CRYPTO_ERROR_RANDOM_FAILED;
  }

  EVP_PKEY_CTX *context = EVP_PKEY_CTX_new(batch->pkey, NULL);
  if (context == NULL) {
    return ENVELOPE_CRYPTO_ERROR_OUT_OF_MEMORY;
  }

  size_t  wrapped_key_len = sizeof(batch->wrapped_key);
  int32_t code = ENVELOPE_CRYPTO_ERROR_RSA_WRAP_FAILED;
  if (EVP_PKEY_encrypt_init(context) <= 0 || EVP_PKEY_CTX_set_rsa_padding(context, RSA_PKCS1_OAEP_PADDING) <= 0 ||
      EVP_PKEY_CTX_set_rsa_oaep_md(context, EVP_sha256()) <= 0 ||
      EVP_PKEY_CTX_set_rsa_mgf1_md(context, EVP_sha256()) <= 0 ||
      EVP_PKEY_encrypt(context, batch->wrapped_key, &wrapped_key_len, batch->aes_key, sizeof(batch->aes_key)) <= 0 ||
      wrapped_key_len != sizeof(batch->wrapped_key)) {
    goto cleanup;
  }

  batch->wrapped_key_len = (uint16_t)wrapped_key_len;
  batch->secret_initialized = true;
  code = ENVELOPE_CRYPTO_SUCCESS;

cleanup:
  EVP_PKEY_CTX_free(context);
  return code;
}

static int32_t envelopeEncryptAesGcm(const SEnvelopeBatch *batch, const uint8_t *plain, uint32_t plain_len,
                                     const uint8_t nonce[12], uint8_t *ciphertext, uint8_t tag[16]) {
  EVP_CIPHER_CTX *context = EVP_CIPHER_CTX_new();
  if (context == NULL) {
    return ENVELOPE_CRYPTO_ERROR_OUT_OF_MEMORY;
  }

  int32_t code = ENVELOPE_CRYPTO_ERROR_ENCRYPT_FAILED;
  int     ignored_len = 0;
  if (EVP_EncryptInit_ex(context, EVP_aes_256_gcm(), NULL, NULL, NULL) != 1 ||
      EVP_CIPHER_CTX_ctrl(context, EVP_CTRL_GCM_SET_IVLEN, ENVELOPE_CRYPTO_NONCE_LEN, NULL) != 1 ||
      EVP_EncryptInit_ex(context, NULL, NULL, batch->aes_key, nonce) != 1 ||
      EVP_EncryptUpdate(context, NULL, &ignored_len, NULL, 0) != 1) {
    goto cleanup;
  }

  int ciphertext_len = 0;
  if (plain_len > 0 && EVP_EncryptUpdate(context, ciphertext, &ciphertext_len, plain, (int)plain_len) != 1) {
    goto cleanup;
  }

  int final_len = 0;
  if (EVP_EncryptFinal_ex(context, ciphertext + ciphertext_len, &final_len) != 1 ||
      (uint32_t)(ciphertext_len + final_len) != plain_len ||
      EVP_CIPHER_CTX_ctrl(context, EVP_CTRL_GCM_GET_TAG, ENVELOPE_CRYPTO_TAG_LEN, tag) != 1) {
    goto cleanup;
  }

  code = ENVELOPE_CRYPTO_SUCCESS;

cleanup:
  EVP_CIPHER_CTX_free(context);
  return code;
}

static void envelopeWriteUint16BigEndian(uint8_t *output, uint16_t value) {
  output[0] = (uint8_t)(value >> 8);
  output[1] = (uint8_t)value;
}

static void envelopeWriteUint32BigEndian(uint8_t *output, uint32_t value) {
  output[0] = (uint8_t)(value >> 24);
  output[1] = (uint8_t)(value >> 16);
  output[2] = (uint8_t)(value >> 8);
  output[3] = (uint8_t)value;
}

static int32_t envelopeEncodeBase64Url(const uint8_t *raw, uint32_t raw_len, char **encoded, uint32_t *encoded_len) {
  const uint32_t prefix_len = (uint32_t)(sizeof(kEnvelopePrefix) - 1);
  const uint64_t padded_len_64 = (((uint64_t)raw_len + 2) / 3) * 4;
  if (padded_len_64 > UINT32_MAX - prefix_len - 1) {
    return ENVELOPE_CRYPTO_ERROR_LENGTH_INVALID;
  }
  const uint32_t padded_len = (uint32_t)padded_len_64;

  char *text = taosMemoryMalloc((int64_t)prefix_len + padded_len + 1);
  if (text == NULL) {
    return ENVELOPE_CRYPTO_ERROR_OUT_OF_MEMORY;
  }

  memcpy(text, kEnvelopePrefix, prefix_len);
  const int actual_padded_len = EVP_EncodeBlock((unsigned char *)text + prefix_len, raw, (int)raw_len);
  if (actual_padded_len != (int)padded_len) {
    taosMemoryFree(text);
    return ENVELOPE_CRYPTO_ERROR_ENCRYPT_FAILED;
  }

  for (uint32_t index = prefix_len; index < prefix_len + padded_len; ++index) {
    if (text[index] == '+') {
      text[index] = '-';
    } else if (text[index] == '/') {
      text[index] = '_';
    }
  }

  uint32_t payload_len = padded_len;
  while (payload_len > 0 && text[prefix_len + payload_len - 1] == '=') {
    --payload_len;
  }
  text[prefix_len + payload_len] = '\0';
  *encoded = text;
  *encoded_len = prefix_len + payload_len;
  return ENVELOPE_CRYPTO_SUCCESS;
}

static int32_t envelopeComputeKeyId(EVP_PKEY *pkey, uint8_t key_id[ENVELOPE_CRYPTO_KEY_ID_LEN]) {
  int32_t        code = ENVELOPE_CRYPTO_ERROR_KEY_INVALID;
  unsigned char *der = NULL;
  int            der_len = i2d_PUBKEY(pkey, &der);
  unsigned int   digest_len = 0;

  if (der_len <= 0 || der == NULL) {
    goto cleanup;
  }
  if (EVP_Digest(der, (size_t)der_len, key_id, &digest_len, EVP_sha256(), NULL) != 1 ||
      digest_len != ENVELOPE_CRYPTO_KEY_ID_LEN) {
    goto cleanup;
  }

  code = ENVELOPE_CRYPTO_SUCCESS;

cleanup:
  OPENSSL_free(der);
  return code;
}

static bool envelopeIsAsciiSpace(uint8_t value) {
  return value == ' ' || value == '\t' || value == '\r' || value == '\n' || value == '\v' || value == '\f';
}

static bool envelopeHasSpkiPemPrefix(const uint8_t *pem, int32_t pem_len) {
  static const char prefix[] = "-----BEGIN PUBLIC KEY-----";
  int32_t           offset = 0;

  while (offset < pem_len && envelopeIsAsciiSpace(pem[offset])) {
    ++offset;
  }
  const int32_t remaining = pem_len - offset;
  return remaining >= (int32_t)(sizeof(prefix) - 1) && memcmp(pem + offset, prefix, sizeof(prefix) - 1) == 0;
}

static bool envelopeBioHasOnlyWhitespace(BIO *bio) {
  uint8_t buffer[128] = {0};
  int     read_len = 0;

  while ((read_len = BIO_read(bio, buffer, sizeof(buffer))) > 0) {
    for (int index = 0; index < read_len; ++index) {
      if (!envelopeIsAsciiSpace(buffer[index])) {
        return false;
      }
    }
  }
  return read_len == 0;
}

int32_t envelopeLoadPublicKeyBytes(const uint8_t *pem, int32_t pem_len, SEnvelopePublicKey **key) {
  if (pem == NULL || pem_len <= 0 || key == NULL || *key != NULL) {
    return ENVELOPE_CRYPTO_ERROR_INVALID_PARAMETER;
  }
  if (pem_len > ENVELOPE_CRYPTO_MAX_PEM_LEN || memchr(pem, '\0', (size_t)pem_len) != NULL ||
      !envelopeHasSpkiPemPrefix(pem, pem_len)) {
    return ENVELOPE_CRYPTO_ERROR_KEY_INVALID;
  }

  ERR_clear_error();
  BIO                *bio = BIO_new_mem_buf(pem, pem_len);
  EVP_PKEY           *pkey = NULL;
  SEnvelopePublicKey *loaded_key = NULL;
  int32_t             code = ENVELOPE_CRYPTO_ERROR_KEY_INVALID;
  if (bio == NULL) {
    code = ENVELOPE_CRYPTO_ERROR_OUT_OF_MEMORY;
    goto cleanup;
  }

  pkey = PEM_read_bio_PUBKEY(bio, NULL, NULL, NULL);
  /* Use the pre-3.0 EVP_PKEY accessors: the c2r-emitter CI gate compiles this
     file against the system OpenSSL, which may be 1.1 (the bundled 3.x headers
     map these names onto the EVP_PKEY_get_* variants). */
  if (pkey == NULL || !envelopeBioHasOnlyWhitespace(bio) || EVP_PKEY_base_id(pkey) != EVP_PKEY_RSA ||
      EVP_PKEY_bits(pkey) != ENVELOPE_CRYPTO_RSA_BITS || EVP_PKEY_size(pkey) != ENVELOPE_CRYPTO_RSA_BYTES) {
    goto cleanup;
  }
  if (BIO_free(bio) != 1) {
    bio = NULL;
    goto cleanup;
  }
  bio = NULL;

  loaded_key = taosMemoryCalloc(1, sizeof(SEnvelopePublicKey));
  if (loaded_key == NULL) {
    code = ENVELOPE_CRYPTO_ERROR_OUT_OF_MEMORY;
    goto cleanup;
  }

  loaded_key->pkey = pkey;
  pkey = NULL;
  code = envelopeComputeKeyId(loaded_key->pkey, loaded_key->key_id);
  if (code != ENVELOPE_CRYPTO_SUCCESS) {
    goto cleanup;
  }

  *key = loaded_key;
  loaded_key = NULL;
  code = ENVELOPE_CRYPTO_SUCCESS;

cleanup:
  if (bio != NULL) {
    (void)BIO_free(bio);
  }
  EVP_PKEY_free(pkey);
  if (loaded_key != NULL) {
    EVP_PKEY_free(loaded_key->pkey);
    taosMemoryFree(loaded_key);
  }
  ERR_clear_error();
  return code;
}

int32_t envelopeLoadPublicKey(const char *pem_path, SEnvelopePublicKey **key) {
  if (pem_path == NULL || pem_path[0] == '\0' || key == NULL || *key != NULL) {
    return ENVELOPE_CRYPTO_ERROR_INVALID_PARAMETER;
  }

  ERR_clear_error();
  BIO *bio = BIO_new_file(pem_path, "rb");
  if (bio == NULL) {
    ERR_clear_error();
    return ENVELOPE_CRYPTO_ERROR_KEY_INVALID;
  }

  int32_t  code = ENVELOPE_CRYPTO_ERROR_KEY_INVALID;
  uint8_t *pem = taosMemoryMalloc(ENVELOPE_CRYPTO_MAX_PEM_LEN + 1);
  if (pem == NULL) {
    code = ENVELOPE_CRYPTO_ERROR_OUT_OF_MEMORY;
    goto cleanup_bio;
  }

  int32_t pem_len = 0;
  while (pem_len <= ENVELOPE_CRYPTO_MAX_PEM_LEN) {
    int read_len = BIO_read(bio, pem + pem_len, ENVELOPE_CRYPTO_MAX_PEM_LEN + 1 - pem_len);
    if (read_len < 0) {
      break;
    }
    if (read_len == 0) {
      code = pem_len == 0 ? ENVELOPE_CRYPTO_ERROR_KEY_INVALID : envelopeLoadPublicKeyBytes(pem, pem_len, key);
      break;
    }
    pem_len += read_len;
  }
  taosMemoryFree(pem);

cleanup_bio:
  if (BIO_free(bio) != 1 && code == ENVELOPE_CRYPTO_SUCCESS) {
    (void)envelopeReleasePublicKey(key);
    code = ENVELOPE_CRYPTO_ERROR_KEY_INVALID;
  }
  ERR_clear_error();
  return code;
}

int32_t envelopeGetKeyId(const SEnvelopePublicKey *key, uint8_t key_id[ENVELOPE_CRYPTO_KEY_ID_LEN]) {
  if (key == NULL || key_id == NULL) {
    return ENVELOPE_CRYPTO_ERROR_INVALID_PARAMETER;
  }

  memcpy(key_id, key->key_id, ENVELOPE_CRYPTO_KEY_ID_LEN);
  return ENVELOPE_CRYPTO_SUCCESS;
}

int32_t envelopeBatchBegin(const SEnvelopePublicKey *key, SEnvelopeBatch **batch) {
  if (key == NULL || key->pkey == NULL || batch == NULL || *batch != NULL) {
    return ENVELOPE_CRYPTO_ERROR_INVALID_PARAMETER;
  }

  ERR_clear_error();
  SEnvelopeBatch *new_batch = taosMemoryCalloc(1, sizeof(SEnvelopeBatch));
  if (new_batch == NULL) {
    ERR_clear_error();
    return ENVELOPE_CRYPTO_ERROR_OUT_OF_MEMORY;
  }
  if (EVP_PKEY_up_ref(key->pkey) != 1) {
    taosMemoryFree(new_batch);
    ERR_clear_error();
    return ENVELOPE_CRYPTO_ERROR_KEY_INVALID;
  }

  new_batch->pkey = key->pkey;
  memcpy(new_batch->key_id, key->key_id, sizeof(new_batch->key_id));
  *batch = new_batch;
  ERR_clear_error();
  return ENVELOPE_CRYPTO_SUCCESS;
}

int32_t envelopeBatchEncryptField(SEnvelopeBatch *batch, const uint8_t *plain, uint32_t plain_len,
                                  uint32_t max_plain_len, char **encoded, uint32_t *encoded_len) {
  if (encoded_len != NULL) {
    *encoded_len = 0;
  }
  if (batch == NULL || batch->failed) {
    return ENVELOPE_CRYPTO_ERROR_STATE_INVALID;
  }
  if ((plain == NULL && plain_len != 0) || encoded == NULL || encoded_len == NULL || *encoded != NULL) {
    return envelopeFailBatch(batch, ENVELOPE_CRYPTO_ERROR_INVALID_PARAMETER);
  }
  if (plain_len > ENVELOPE_CRYPTO_PROTOCOL_MAX_PLAIN_LEN ||
      (max_plain_len != ENVELOPE_CRYPTO_NO_ROLE_CAP && plain_len > max_plain_len) ||
      plain_len > UINT32_MAX - ENVELOPE_CRYPTO_FIXED_RAW_LEN - ENVELOPE_CRYPTO_RSA_BYTES) {
    return envelopeFailBatch(batch, ENVELOPE_CRYPTO_ERROR_LENGTH_INVALID);
  }
  if (!envelopeIsValidUtf8(plain, plain_len)) {
    return envelopeFailBatch(batch, ENVELOPE_CRYPTO_ERROR_PLAINTEXT_UTF8_INVALID);
  }
  if (batch->field_count == UINT32_MAX) {
    return envelopeFailBatch(batch, ENVELOPE_CRYPTO_ERROR_STATE_INVALID);
  }

  ERR_clear_error();
  if (!batch->secret_initialized) {
    int32_t code = envelopeWrapBatchKey(batch);
    if (code != ENVELOPE_CRYPTO_SUCCESS) {
      return envelopeFailBatch(batch, code);
    }
  }

  const uint32_t raw_len = ENVELOPE_CRYPTO_FIXED_RAW_LEN + batch->wrapped_key_len + plain_len;
  uint8_t       *raw = taosMemoryMalloc(raw_len);
  if (raw == NULL) {
    return envelopeFailBatch(batch, ENVELOPE_CRYPTO_ERROR_OUT_OF_MEMORY);
  }

  uint32_t offset = 0;
  raw[offset++] = ENVELOPE_CRYPTO_PROTOCOL_VERSION;
  raw[offset++] = ENVELOPE_CRYPTO_ALGORITHM_AES_256_GCM;
  memcpy(raw + offset, batch->key_id, sizeof(batch->key_id));
  offset += sizeof(batch->key_id);
  envelopeWriteUint16BigEndian(raw + offset, batch->wrapped_key_len);
  offset += sizeof(uint16_t);
  memcpy(raw + offset, batch->wrapped_key, batch->wrapped_key_len);
  offset += batch->wrapped_key_len;

  uint8_t *nonce = raw + offset;
  if (RAND_bytes(nonce, ENVELOPE_CRYPTO_NONCE_LEN) != 1) {
    OPENSSL_cleanse(raw, raw_len);
    taosMemoryFree(raw);
    return envelopeFailBatch(batch, ENVELOPE_CRYPTO_ERROR_RANDOM_FAILED);
  }
  offset += ENVELOPE_CRYPTO_NONCE_LEN;
  envelopeWriteUint32BigEndian(raw + offset, plain_len);
  offset += sizeof(uint32_t);

  uint8_t *ciphertext = raw + offset;
  uint8_t *tag = ciphertext + plain_len;
  int32_t  code = envelopeEncryptAesGcm(batch, plain, plain_len, nonce, ciphertext, tag);
  if (code == ENVELOPE_CRYPTO_SUCCESS) {
    code = envelopeEncodeBase64Url(raw, raw_len, encoded, encoded_len);
  }
  if (code != ENVELOPE_CRYPTO_SUCCESS) {
    OPENSSL_cleanse(raw, raw_len);
  }
  taosMemoryFree(raw);
  if (code != ENVELOPE_CRYPTO_SUCCESS) {
    return envelopeFailBatch(batch, code);
  }

  ++batch->field_count;
  ERR_clear_error();
  return ENVELOPE_CRYPTO_SUCCESS;
}

int32_t envelopeBatchFinish(SEnvelopeBatch **batch) {
  if (batch == NULL) {
    return ENVELOPE_CRYPTO_ERROR_INVALID_PARAMETER;
  }
  if (*batch == NULL) {
    return ENVELOPE_CRYPTO_ERROR_STATE_INVALID;
  }

  SEnvelopeBatch *finished_batch = *batch;
  *batch = NULL;
  const int32_t code = finished_batch->failed || finished_batch->field_count == 0 ? ENVELOPE_CRYPTO_ERROR_STATE_INVALID
                                                                                  : ENVELOPE_CRYPTO_SUCCESS;
  envelopeFreeBatch(finished_batch);
  ERR_clear_error();
  return code;
}

int32_t envelopeBatchAbort(SEnvelopeBatch **batch) {
  if (batch == NULL) {
    return ENVELOPE_CRYPTO_ERROR_INVALID_PARAMETER;
  }
  if (*batch == NULL) {
    return ENVELOPE_CRYPTO_SUCCESS;
  }

  SEnvelopeBatch *aborted_batch = *batch;
  *batch = NULL;
  envelopeFreeBatch(aborted_batch);
  ERR_clear_error();
  return ENVELOPE_CRYPTO_SUCCESS;
}

int32_t envelopeReleasePublicKey(SEnvelopePublicKey **key) {
  if (key == NULL) {
    return ENVELOPE_CRYPTO_ERROR_INVALID_PARAMETER;
  }
  if (*key == NULL) {
    return ENVELOPE_CRYPTO_SUCCESS;
  }

  EVP_PKEY_free((*key)->pkey);
  taosMemoryFreeClear(*key);
  return ENVELOPE_CRYPTO_SUCCESS;
}
