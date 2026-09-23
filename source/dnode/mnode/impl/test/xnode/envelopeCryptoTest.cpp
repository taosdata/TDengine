/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

/**
 * @file envelopeCryptoTest.cpp
 * @brief XNODE DSN envelope crypto unit tests (RSA-OAEP key wrap + AES-256-GCM)
 * @version 1.0
 * @date 2026-09-01
 *
 * Tests the envelope_crypto API against the cross-language reference vectors in
 * xnode-dsn-v1-fixtures.json (path injected as ENVELOPE_CRYPTO_FIXTURE_PATH):
 * - public key loading, key id (kid) derivation, and RSA-2048-only enforcement
 * - batch encryption: one wrapped key per batch, a fresh nonce per field
 * - generated-envelope parsing and decryption with the fixture private key
 * - fixture plaintexts from batches[0] only; envelope_text,
 *   expected_wrapped_key_b64url, and batches[1..2] are intentionally not
 *   compared by this C test
 * - role caps, protocol maximum length, UTF-8 validation, and batch error states
 * - canonical error id mapping for cross-language errors only
 */

#include "envelope_crypto.h"

#include <gtest/gtest.h>

#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "osMemPool.h"
#include "osMemory.h"
#include "tjson.h"

namespace {

struct BioDeleter {
  void operator()(BIO *bio) const { BIO_free(bio); }
};

struct JsonDeleter {
  void operator()(SJson *json) const { tjsonDelete(json); }
};

struct PkeyDeleter {
  void operator()(EVP_PKEY *key) const { EVP_PKEY_free(key); }
};

using BioPtr = std::unique_ptr<BIO, BioDeleter>;
using JsonPtr = std::unique_ptr<SJson, JsonDeleter>;
using PkeyPtr = std::unique_ptr<EVP_PKEY, PkeyDeleter>;

constexpr size_t kEnvelopeVersionLength = 1;
constexpr size_t kEnvelopeAlgorithmLength = 1;
constexpr size_t kEnvelopeKeyIdLength = 32;
constexpr size_t kEnvelopeAesKeyLength = 32;
constexpr size_t kEnvelopeWrappedKeyLengthFieldLength = 2;
constexpr size_t kEnvelopeRsaWrappedKeyLength = 256;
constexpr size_t kEnvelopeNonceLength = 12;
constexpr size_t kEnvelopePlaintextLengthFieldLength = 4;
constexpr size_t kEnvelopeTagLength = 16;
constexpr size_t kEnvelopeFixedRawLength = kEnvelopeVersionLength + kEnvelopeAlgorithmLength + kEnvelopeKeyIdLength +
                                           kEnvelopeWrappedKeyLengthFieldLength + kEnvelopeNonceLength +
                                           kEnvelopePlaintextLengthFieldLength + kEnvelopeTagLength;
constexpr size_t kEnvelopeTrailingRawLength =
    kEnvelopeNonceLength + kEnvelopePlaintextLengthFieldLength + kEnvelopeTagLength;

constexpr uint8_t kEnvelopeVersion = 1;
constexpr uint8_t kEnvelopeAlgorithm = 1;
constexpr char    kEnvelopePrefix[] = "XNODE_DSN_V1:";
constexpr size_t  kEnvelopeMaxPemLength = 64U * 1024U;

size_t envelopeBase64UrlLength(size_t raw_length) {
  const size_t padded_length = ((raw_length + 2) / 3) * 4;
  const size_t padding = (3 - raw_length % 3) % 3;
  return padded_length - padding;
}

std::string readFixture() {
  std::ifstream input(ENVELOPE_CRYPTO_FIXTURE_PATH, std::ios::in | std::ios::binary);
  return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

std::vector<uint8_t> decodeBase64Url(const char *text) {
  std::string padded(text == nullptr ? "" : text);
  for (std::string::iterator it = padded.begin(); it != padded.end(); ++it) {
    if (*it == '-') {
      *it = '+';
    } else if (*it == '_') {
      *it = '/';
    }
  }

  const size_t remainder = padded.size() % 4;
  if (remainder != 0) {
    padded.append(4 - remainder, '=');
  }

  std::vector<uint8_t> decoded((padded.size() / 4) * 3);
  const int decoded_len = EVP_DecodeBlock(decoded.data(), reinterpret_cast<const unsigned char *>(padded.data()),
                                          static_cast<int>(padded.size()));
  if (decoded_len < 0) {
    return std::vector<uint8_t>();
  }

  size_t padding = 0;
  if (!padded.empty() && padded[padded.size() - 1] == '=') {
    ++padding;
  }
  if (padded.size() > 1 && padded[padded.size() - 2] == '=') {
    ++padding;
  }
  decoded.resize(static_cast<size_t>(decoded_len) - padding);
  return decoded;
}

bool isCanonicalEnvelopeText(const char *encoded, uint32_t encoded_len) {
  if (encoded == nullptr || encoded_len <= sizeof(kEnvelopePrefix) - 1 ||
      memcmp(encoded, kEnvelopePrefix, sizeof(kEnvelopePrefix) - 1) != 0 || encoded[encoded_len] != '\0') {
    return false;
  }

  for (uint32_t index = sizeof(kEnvelopePrefix) - 1; index < encoded_len; ++index) {
    const char value = encoded[index];
    if (!((value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') ||
          value == '-' || value == '_')) {
      return false;
    }
  }
  return true;
}

std::string spkiDerToPem(const std::vector<uint8_t> &der) {
  const unsigned char *cursor = der.data();
  PkeyPtr              key(d2i_PUBKEY(nullptr, &cursor, static_cast<long>(der.size())));
  if (!key || cursor != der.data() + der.size()) {
    return std::string();
  }

  BioPtr bio(BIO_new(BIO_s_mem()));
  if (!bio || PEM_write_bio_PUBKEY(bio.get(), key.get()) != 1) {
    return std::string();
  }

  BUF_MEM *buffer = nullptr;
  BIO_get_mem_ptr(bio.get(), &buffer);
  if (buffer == nullptr) {
    return std::string();
  }
  return std::string(buffer->data, buffer->length);
}

std::string referencePublicKeyPem() {
  const std::string fixture_text = readFixture();
  if (fixture_text.empty()) {
    return std::string();
  }
  JsonPtr root(tjsonParse(fixture_text.c_str()));
  if (!root) {
    return std::string();
  }
  SJson *key_sets = tjsonGetObjectItem(root.get(), "key_sets");
  if (!tjsonIsArray(key_sets) || tjsonGetArraySize(key_sets) != 1) {
    return std::string();
  }
  SJson      *key_set = tjsonGetArrayItem(key_sets, 0);
  const char *spki_der_text = tjsonGetStringPointer(key_set, "public_spki_der_b64url");
  return spkiDerToPem(decodeBase64Url(spki_der_text));
}

std::vector<std::vector<uint8_t> > referenceFirstBatchPlaintexts() {
  std::vector<std::vector<uint8_t> > plaintexts;
  const std::string                  fixture_text = readFixture();
  if (fixture_text.empty()) {
    return plaintexts;
  }
  JsonPtr root(tjsonParse(fixture_text.c_str()));
  if (!root) {
    return plaintexts;
  }
  SJson *batches = tjsonGetObjectItem(root.get(), "batches");
  if (!tjsonIsArray(batches) || tjsonGetArraySize(batches) == 0) {
    return plaintexts;
  }
  SJson *batch = tjsonGetArrayItem(batches, 0);
  SJson *envelopes = tjsonGetObjectItem(batch, "envelopes");
  if (!tjsonIsArray(envelopes)) {
    return plaintexts;
  }

  const int32_t envelope_count = tjsonGetArraySize(envelopes);
  for (int32_t index = 0; index < envelope_count; ++index) {
    SJson                     *envelope = tjsonGetArrayItem(envelopes, index);
    const char                *plaintext = tjsonGetStringPointer(envelope, "expected_plaintext_bytes_b64url");
    const std::vector<uint8_t> decoded = decodeBase64Url(plaintext);
    if (decoded.empty()) {
      plaintexts.clear();
      return plaintexts;
    }
    plaintexts.push_back(decoded);
  }
  return plaintexts;
}

std::string fixedRsa1024PublicKeyPem() {
  return R"(-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDe+eeXHpkYEeD9GppA2AK/IAlT
fGSKKZWqLKVBeY+YDVpP+BrlDDolKl+/s1WrONUE+pIHjREFXHmQqM232wILddHK
F+j3Wj/ZsFnvzVvED8xAiXyUB+YAW+s6tdcdz55jMeiAnetGyWhMZQKcu2hiDfXL
l7sj9za6/ocjGm0xCQIDAQAB
-----END PUBLIC KEY-----
)";
}

std::string fixedRsa3072PublicKeyPem() {
  return R"(-----BEGIN PUBLIC KEY-----
MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAsqFXUGifIpEZBR/Gvewg
+hpe9RpqJBVogW/BQ5xux2JTdHznr9sc1BYklSEB9kagaJK1BxV0i/7HD3lswTiW
upaRucL8LEmEtsLE8qGTJJlma3qQud8PQ9Sq/MnWS+AREcF1JrSsG75pvwiG6817
IBHPihcAWv6XKBo3YbeldJ5k/K9QY8jgVK1rbc6SvDio6wqU3q4KTAw1g6+kyAqQ
NY83z758CXxJva0x3AoFcePlfZnzLTfkn2wcEe5LaOP/WK5b6c74V/3gYSJq/9Ax
zVib9f3uKAyrQ/L4qrelrBEGUI2HJL4Arw0qpA4EL9gjTRTZti7F7wf9hfG5GDn/
duuMt3pZ9D6pX1DxYASSXi77inGKwtNjbu04IvHqu3lXust0fx76LPAEzHrPFYMy
6ILA0cq9fd775z9mKvo7kXIW3reFZaAlvw3kc+H14TbS/m+9zrjg3sdJeoGctoGD
+lOBiWBDA0qOtQek6Q+KW//CN60Ktgy9B1vzjEL+8VYXAgMBAAE=
-----END PUBLIC KEY-----
)";
}

std::string fixedEcP256PublicKeyPem() {
  return R"(-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEKA8q+qB/gTAwBvI4mVW2mMuAIc+F
WoQRN4lIhVUY/mFG5J+/ms8feSo1CTq/vBlmTJ6ScMNqVyBypakGAXqWdA==
-----END PUBLIC KEY-----
)";
}

PkeyPtr referencePrivateKey() {
  const std::string fixture_text = readFixture();
  if (fixture_text.empty()) {
    return PkeyPtr(nullptr);
  }
  JsonPtr root(tjsonParse(fixture_text.c_str()));
  if (!root) {
    return PkeyPtr(nullptr);
  }
  SJson *key_sets = tjsonGetObjectItem(root.get(), "key_sets");
  if (!tjsonIsArray(key_sets) || tjsonGetArraySize(key_sets) != 1) {
    return PkeyPtr(nullptr);
  }
  SJson                     *key_set = tjsonGetArrayItem(key_sets, 0);
  const char                *private_der_text = tjsonGetStringPointer(key_set, "private_pkcs8_der_b64url");
  const std::vector<uint8_t> private_der = decodeBase64Url(private_der_text);
  if (private_der.empty()) {
    return PkeyPtr(nullptr);
  }

  const unsigned char *cursor = private_der.data();
  PKCS8_PRIV_KEY_INFO *private_info = d2i_PKCS8_PRIV_KEY_INFO(nullptr, &cursor, static_cast<long>(private_der.size()));
  if (private_info == nullptr || cursor != private_der.data() + private_der.size()) {
    PKCS8_PRIV_KEY_INFO_free(private_info);
    return PkeyPtr(nullptr);
  }
  PkeyPtr private_key(EVP_PKCS82PKEY(private_info));
  PKCS8_PRIV_KEY_INFO_free(private_info);
  return private_key;
}

struct ParsedEnvelope {
  uint8_t              version;
  uint8_t              algorithm;
  std::vector<uint8_t> key_id;
  std::vector<uint8_t> wrapped_key;
  std::vector<uint8_t> nonce;
  std::vector<uint8_t> ciphertext;
  std::vector<uint8_t> tag;
};

uint32_t readUint32BigEndian(const uint8_t *value) {
  return (static_cast<uint32_t>(value[0]) << 24) | (static_cast<uint32_t>(value[1]) << 16) |
         (static_cast<uint32_t>(value[2]) << 8) | static_cast<uint32_t>(value[3]);
}

bool parseEnvelope(const char *encoded, uint32_t encoded_len, ParsedEnvelope *envelope) {
  if (encoded == nullptr || envelope == nullptr || encoded_len <= sizeof(kEnvelopePrefix) - 1 ||
      memcmp(encoded, kEnvelopePrefix, sizeof(kEnvelopePrefix) - 1) != 0) {
    return false;
  }

  const std::string payload(encoded + sizeof(kEnvelopePrefix) - 1, encoded_len - (sizeof(kEnvelopePrefix) - 1));
  const std::vector<uint8_t> raw = decodeBase64Url(payload.c_str());
  if (raw.size() < kEnvelopeFixedRawLength) {
    return false;
  }

  size_t offset = 0;
  envelope->version = raw[offset++];
  envelope->algorithm = raw[offset++];
  envelope->key_id.assign(raw.begin() + offset, raw.begin() + offset + kEnvelopeKeyIdLength);
  offset += kEnvelopeKeyIdLength;
  const uint16_t wrapped_key_len = (static_cast<uint16_t>(raw[offset]) << 8) | raw[offset + 1];
  offset += kEnvelopeWrappedKeyLengthFieldLength;
  if (wrapped_key_len != kEnvelopeRsaWrappedKeyLength ||
      raw.size() < offset + wrapped_key_len + kEnvelopeTrailingRawLength) {
    return false;
  }
  envelope->wrapped_key.assign(raw.begin() + offset, raw.begin() + offset + wrapped_key_len);
  offset += wrapped_key_len;
  envelope->nonce.assign(raw.begin() + offset, raw.begin() + offset + kEnvelopeNonceLength);
  offset += kEnvelopeNonceLength;
  const uint32_t ciphertext_len = readUint32BigEndian(raw.data() + offset);
  offset += kEnvelopePlaintextLengthFieldLength;
  if (raw.size() != kEnvelopeFixedRawLength + wrapped_key_len + ciphertext_len) {
    return false;
  }
  envelope->ciphertext.assign(raw.begin() + offset, raw.begin() + offset + ciphertext_len);
  offset += ciphertext_len;
  envelope->tag.assign(raw.begin() + offset, raw.end());
  return envelope->tag.size() == kEnvelopeTagLength;
}

std::vector<uint8_t> decryptEnvelope(const ParsedEnvelope &envelope, EVP_PKEY *private_key) {
  std::vector<uint8_t> result;
  EVP_PKEY_CTX        *rsa_context = EVP_PKEY_CTX_new(private_key, nullptr);
  if (rsa_context == nullptr || EVP_PKEY_decrypt_init(rsa_context) <= 0 ||
      EVP_PKEY_CTX_set_rsa_padding(rsa_context, RSA_PKCS1_OAEP_PADDING) <= 0 ||
      EVP_PKEY_CTX_set_rsa_oaep_md(rsa_context, EVP_sha256()) <= 0 ||
      EVP_PKEY_CTX_set_rsa_mgf1_md(rsa_context, EVP_sha256()) <= 0) {
    EVP_PKEY_CTX_free(rsa_context);
    return result;
  }

  size_t aes_key_len = 0;
  if (EVP_PKEY_decrypt(rsa_context, nullptr, &aes_key_len, envelope.wrapped_key.data(), envelope.wrapped_key.size()) <=
      0) {
    EVP_PKEY_CTX_free(rsa_context);
    return result;
  }
  std::vector<uint8_t> aes_key(aes_key_len);
  if (EVP_PKEY_decrypt(rsa_context, aes_key.data(), &aes_key_len, envelope.wrapped_key.data(),
                       envelope.wrapped_key.size()) <= 0) {
    EVP_PKEY_CTX_free(rsa_context);
    return result;
  }
  EVP_PKEY_CTX_free(rsa_context);
  aes_key.resize(aes_key_len);
  if (aes_key.size() != kEnvelopeAesKeyLength) {
    return result;
  }

  EVP_CIPHER_CTX *cipher_context = EVP_CIPHER_CTX_new();
  if (cipher_context == nullptr ||
      EVP_DecryptInit_ex(cipher_context, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1 ||
      EVP_CIPHER_CTX_ctrl(cipher_context, EVP_CTRL_GCM_SET_IVLEN, static_cast<int>(envelope.nonce.size()), nullptr) !=
          1 ||
      EVP_DecryptInit_ex(cipher_context, nullptr, nullptr, aes_key.data(), envelope.nonce.data()) != 1) {
    EVP_CIPHER_CTX_free(cipher_context);
    return result;
  }

  int ignored_len = 0;
  if (EVP_DecryptUpdate(cipher_context, nullptr, &ignored_len, nullptr, 0) != 1) {
    EVP_CIPHER_CTX_free(cipher_context);
    return result;
  }

  result.resize(envelope.ciphertext.size());
  int plaintext_len = 0;
  if (EVP_DecryptUpdate(cipher_context, result.data(), &plaintext_len, envelope.ciphertext.data(),
                        static_cast<int>(envelope.ciphertext.size())) != 1 ||
      EVP_CIPHER_CTX_ctrl(cipher_context, EVP_CTRL_GCM_SET_TAG, static_cast<int>(envelope.tag.size()),
                          const_cast<uint8_t *>(envelope.tag.data())) != 1) {
    result.clear();
    EVP_CIPHER_CTX_free(cipher_context);
    return result;
  }

  int final_len = 0;
  if (EVP_DecryptFinal_ex(cipher_context, result.data() + plaintext_len, &final_len) != 1) {
    result.clear();
  } else {
    result.resize(static_cast<size_t>(plaintext_len + final_len));
  }
  EVP_CIPHER_CTX_free(cipher_context);
  return result;
}

SEnvelopePublicKey *loadReferencePublicKey() {
  const std::string public_key_pem = referencePublicKeyPem();
  if (public_key_pem.empty()) {
    return nullptr;
  }

  SEnvelopePublicKey *key = nullptr;
  if (envelopeLoadPublicKeyBytes(reinterpret_cast<const uint8_t *>(public_key_pem.data()),
                                 static_cast<int32_t>(public_key_pem.size()), &key) != ENVELOPE_CRYPTO_SUCCESS) {
    return nullptr;
  }
  return key;
}

void releaseEncoded(char **encoded) {
  if (encoded != nullptr && *encoded != nullptr) {
    taosMemoryFree(*encoded);
    *encoded = nullptr;
  }
}

TEST(EnvelopeCryptoTest, LoadsReferenceRsa2048AndComputesKid) {
  const std::string fixture_text = readFixture();
  ASSERT_FALSE(fixture_text.empty());

  JsonPtr root(tjsonParse(fixture_text.c_str()));
  ASSERT_NE(root.get(), nullptr);
  SJson *key_sets = tjsonGetObjectItem(root.get(), "key_sets");
  ASSERT_TRUE(tjsonIsArray(key_sets));
  ASSERT_EQ(tjsonGetArraySize(key_sets), 1);
  SJson *key_set = tjsonGetArrayItem(key_sets, 0);
  ASSERT_NE(key_set, nullptr);

  const char *spki_der_text = tjsonGetStringPointer(key_set, "public_spki_der_b64url");
  const char *expected_kid_text = tjsonGetStringPointer(key_set, "expected_kid_b64url");
  ASSERT_NE(spki_der_text, nullptr);
  ASSERT_NE(expected_kid_text, nullptr);

  const std::vector<uint8_t> spki_der = decodeBase64Url(spki_der_text);
  const std::vector<uint8_t> expected_kid = decodeBase64Url(expected_kid_text);
  ASSERT_FALSE(spki_der.empty());
  ASSERT_EQ(expected_kid.size(), 32U);
  const std::string public_key_pem = spkiDerToPem(spki_der);
  ASSERT_FALSE(public_key_pem.empty());

  SEnvelopePublicKey *key = nullptr;
  ASSERT_EQ(envelopeLoadPublicKeyBytes(reinterpret_cast<const uint8_t *>(public_key_pem.data()),
                                       static_cast<int32_t>(public_key_pem.size()), &key),
            ENVELOPE_CRYPTO_SUCCESS);
  ASSERT_NE(key, nullptr);

  uint8_t actual_kid[kEnvelopeKeyIdLength] = {0};
  EXPECT_EQ(envelopeGetKeyId(key, actual_kid), ENVELOPE_CRYPTO_SUCCESS);
  EXPECT_EQ(std::vector<uint8_t>(actual_kid, actual_kid + sizeof(actual_kid)), expected_kid);
  EXPECT_EQ(envelopeReleasePublicKey(&key), ENVELOPE_CRYPTO_SUCCESS);
  EXPECT_EQ(key, nullptr);
}

TEST(EnvelopeCryptoTest, MapsOnlyCrossLanguageErrorsToCanonicalIds) {
  struct CanonicalMapping {
    int32_t     code;
    const char *canonical_id;
  };
  const CanonicalMapping mappings[] = {
      {ENVELOPE_CRYPTO_ERROR_LENGTH_INVALID, "ENVELOPE_LENGTH_INVALID"},
      {ENVELOPE_CRYPTO_ERROR_KEY_INVALID, "ENVELOPE_KEY_INVALID"},
      {ENVELOPE_CRYPTO_ERROR_RANDOM_FAILED, "ENVELOPE_RANDOM_FAILED"},
      {ENVELOPE_CRYPTO_ERROR_RSA_WRAP_FAILED, "ENVELOPE_RSA_WRAP_FAILED"},
      {ENVELOPE_CRYPTO_ERROR_PLAINTEXT_UTF8_INVALID, "ENVELOPE_PLAINTEXT_UTF8_INVALID"},
  };
  for (size_t index = 0; index < sizeof(mappings) / sizeof(mappings[0]); ++index) {
    EXPECT_STREQ(envelopeCryptoCanonicalId(mappings[index].code), mappings[index].canonical_id);
  }

  const int32_t local_errors[] = {
      ENVELOPE_CRYPTO_SUCCESS,
      ENVELOPE_CRYPTO_ERROR_INVALID_PARAMETER,
      ENVELOPE_CRYPTO_ERROR_OUT_OF_MEMORY,
      ENVELOPE_CRYPTO_ERROR_STATE_INVALID,
      ENVELOPE_CRYPTO_ERROR_ENCRYPT_FAILED,
  };
  for (size_t index = 0; index < sizeof(local_errors) / sizeof(local_errors[0]); ++index) {
    EXPECT_EQ(envelopeCryptoCanonicalId(local_errors[index]), nullptr);
  }

  const int32_t unknown_errors[] = {1, -999};
  for (size_t index = 0; index < sizeof(unknown_errors) / sizeof(unknown_errors[0]); ++index) {
    EXPECT_EQ(envelopeCryptoCanonicalId(unknown_errors[index]), nullptr);
  }
}

TEST(EnvelopeCryptoTest, LoadsReferenceRsa2048FromFile) {
  const std::string public_key_pem = referencePublicKeyPem();
  ASSERT_FALSE(public_key_pem.empty());
  const std::string pem_path = std::string(ENVELOPE_CRYPTO_FIXTURE_PATH) + ".public.pem";
  {
    std::ofstream output(pem_path.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.good());
    output.write(public_key_pem.data(), static_cast<std::streamsize>(public_key_pem.size()));
    ASSERT_TRUE(output.good());
  }

  SEnvelopePublicKey *key = nullptr;
  EXPECT_EQ(envelopeLoadPublicKey(pem_path.c_str(), &key), ENVELOPE_CRYPTO_SUCCESS);
  EXPECT_NE(key, nullptr);
  EXPECT_EQ(envelopeReleasePublicKey(&key), ENVELOPE_CRYPTO_SUCCESS);
  EXPECT_EQ(std::remove(pem_path.c_str()), 0);
}

TEST(EnvelopeCryptoTest, RejectsEmptyPublicKeyFile) {
  const std::string pem_path = std::string(ENVELOPE_CRYPTO_FIXTURE_PATH) + ".empty.public.pem";
  {
    std::ofstream output(pem_path.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.good());
  }

  SEnvelopePublicKey *key = nullptr;
  EXPECT_EQ(envelopeLoadPublicKey(pem_path.c_str(), &key), ENVELOPE_CRYPTO_ERROR_KEY_INVALID);
  EXPECT_EQ(key, nullptr);
  EXPECT_EQ(std::remove(pem_path.c_str()), 0);
}

TEST(EnvelopeCryptoTest, EnforcesPublicKeyFileSizeBoundary) {
  const std::string public_key_pem = referencePublicKeyPem();
  ASSERT_FALSE(public_key_pem.empty());
  ASSERT_LT(public_key_pem.size(), kEnvelopeMaxPemLength);

  const std::string exact_path = std::string(ENVELOPE_CRYPTO_FIXTURE_PATH) + ".max.public.pem";
  std::string       exact_pem = public_key_pem;
  exact_pem.append(kEnvelopeMaxPemLength - exact_pem.size(), '\n');
  {
    std::ofstream output(exact_path.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.good());
    output.write(exact_pem.data(), static_cast<std::streamsize>(exact_pem.size()));
    ASSERT_TRUE(output.good());
  }

  SEnvelopePublicKey *key = nullptr;
  EXPECT_EQ(envelopeLoadPublicKey(exact_path.c_str(), &key), ENVELOPE_CRYPTO_SUCCESS);
  ASSERT_NE(key, nullptr);
  EXPECT_EQ(envelopeReleasePublicKey(&key), ENVELOPE_CRYPTO_SUCCESS);

  {
    std::ofstream output(exact_path.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.good());
    exact_pem.push_back('\n');
    output.write(exact_pem.data(), static_cast<std::streamsize>(exact_pem.size()));
    ASSERT_TRUE(output.good());
  }

  EXPECT_EQ(envelopeLoadPublicKey(exact_path.c_str(), &key), ENVELOPE_CRYPTO_ERROR_KEY_INVALID);
  EXPECT_EQ(key, nullptr);
  EXPECT_EQ(std::remove(exact_path.c_str()), 0);
}

TEST(EnvelopeCryptoTest, RejectsMoreThanOnePemObject) {
  const std::string public_key_pem = referencePublicKeyPem();
  ASSERT_FALSE(public_key_pem.empty());
  const std::string duplicate_pem = public_key_pem + public_key_pem;

  SEnvelopePublicKey *key = nullptr;
  EXPECT_EQ(envelopeLoadPublicKeyBytes(reinterpret_cast<const uint8_t *>(duplicate_pem.data()),
                                       static_cast<int32_t>(duplicate_pem.size()), &key),
            ENVELOPE_CRYPTO_ERROR_KEY_INVALID);
  EXPECT_EQ(key, nullptr);
}

void expectInvalidPublicKey(const std::string &public_key_pem) {
  ASSERT_FALSE(public_key_pem.empty());

  SEnvelopePublicKey *key = nullptr;
  EXPECT_EQ(envelopeLoadPublicKeyBytes(reinterpret_cast<const uint8_t *>(public_key_pem.data()),
                                       static_cast<int32_t>(public_key_pem.size()), &key),
            ENVELOPE_CRYPTO_ERROR_KEY_INVALID);
  EXPECT_EQ(key, nullptr);
}

TEST(EnvelopeCryptoTest, RejectsEcP256PublicKey) { expectInvalidPublicKey(fixedEcP256PublicKeyPem()); }

TEST(EnvelopeCryptoTest, RejectsRsa1024PublicKey) { expectInvalidPublicKey(fixedRsa1024PublicKeyPem()); }

TEST(EnvelopeCryptoTest, RejectsRsa3072PublicKey) { expectInvalidPublicKey(fixedRsa3072PublicKeyPem()); }

TEST(EnvelopeCryptoTest, RejectsEmbeddedNulInPublicKeyPem) {
  const std::string public_key_pem = referencePublicKeyPem();
  ASSERT_FALSE(public_key_pem.empty());
  std::string pem_with_nul = public_key_pem;
  pem_with_nul.insert(pem_with_nul.size() / 2, 1, '\0');

  SEnvelopePublicKey *key = nullptr;
  EXPECT_EQ(envelopeLoadPublicKeyBytes(reinterpret_cast<const uint8_t *>(pem_with_nul.data()),
                                       static_cast<int32_t>(pem_with_nul.size()), &key),
            ENVELOPE_CRYPTO_ERROR_KEY_INVALID);
  EXPECT_EQ(key, nullptr);
}

TEST(EnvelopeCryptoTest, RejectsNonNullKeyAndBatchOutputs) {
  const std::string public_key_pem = referencePublicKeyPem();
  ASSERT_FALSE(public_key_pem.empty());

  SEnvelopePublicKey *key = nullptr;
  ASSERT_EQ(envelopeLoadPublicKeyBytes(reinterpret_cast<const uint8_t *>(public_key_pem.data()),
                                       static_cast<int32_t>(public_key_pem.size()), &key),
            ENVELOPE_CRYPTO_SUCCESS);
  ASSERT_NE(key, nullptr);
  EXPECT_EQ(envelopeLoadPublicKeyBytes(reinterpret_cast<const uint8_t *>(public_key_pem.data()),
                                      static_cast<int32_t>(public_key_pem.size()), &key),
            ENVELOPE_CRYPTO_ERROR_INVALID_PARAMETER);

  SEnvelopeBatch *batch = nullptr;
  ASSERT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_SUCCESS);
  ASSERT_NE(batch, nullptr);
  EXPECT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_ERROR_INVALID_PARAMETER);
  EXPECT_EQ(envelopeBatchAbort(&batch), ENVELOPE_CRYPTO_SUCCESS);
  EXPECT_EQ(envelopeReleasePublicKey(&key), ENVELOPE_CRYPTO_SUCCESS);
}

TEST(EnvelopeCryptoTest, EncryptsBatchWithOneWrappedKeyAndFreshNonces) {
  SEnvelopePublicKey *key = loadReferencePublicKey();
  ASSERT_NE(key, nullptr);
  PkeyPtr private_key = referencePrivateKey();
  ASSERT_NE(private_key.get(), nullptr);
  uint8_t expected_key_id[kEnvelopeKeyIdLength] = {0};
  ASSERT_EQ(envelopeGetKeyId(key, expected_key_id), ENVELOPE_CRYPTO_SUCCESS);

  SEnvelopeBatch *batch = nullptr;
  ASSERT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_SUCCESS);
  ASSERT_NE(batch, nullptr);

  const std::vector<std::vector<uint8_t> > fixture_plaintexts = referenceFirstBatchPlaintexts();
  ASSERT_EQ(fixture_plaintexts.size(), 2U);
  const uint8_t non_dsn_plaintext[] = {
      'n',  'o',  't',  '-',  'a',  '-',  'd',  's',  'n',  0,    0xe4, 0xb8,
      0xad, 0xe6, 0x96, 0x87, 0xf0, 0x9f, 0x98, 0x80, 0xf4, 0x8f, 0xbf, 0xbf,
  };
  char    *first_encoded = nullptr;
  char    *second_encoded = nullptr;
  char    *third_encoded = nullptr;
  uint32_t first_encoded_len = 0;
  uint32_t second_encoded_len = 0;
  uint32_t third_encoded_len = 0;
  ASSERT_EQ(envelopeBatchEncryptField(batch, fixture_plaintexts[0].data(),
                                      static_cast<uint32_t>(fixture_plaintexts[0].size()), 4096, &first_encoded,
                                      &first_encoded_len),
            ENVELOPE_CRYPTO_SUCCESS);
  ASSERT_EQ(envelopeBatchEncryptField(batch, fixture_plaintexts[1].data(),
                                      static_cast<uint32_t>(fixture_plaintexts[1].size()), 2048, &second_encoded,
                                      &second_encoded_len),
            ENVELOPE_CRYPTO_SUCCESS);
  ASSERT_EQ(envelopeBatchEncryptField(batch, non_dsn_plaintext, sizeof(non_dsn_plaintext), ENVELOPE_CRYPTO_NO_ROLE_CAP,
                                      &third_encoded, &third_encoded_len),
            ENVELOPE_CRYPTO_SUCCESS);
  ASSERT_EQ(envelopeBatchFinish(&batch), ENVELOPE_CRYPTO_SUCCESS);
  EXPECT_EQ(batch, nullptr);
  EXPECT_EQ(first_encoded[first_encoded_len], '\0');
  EXPECT_EQ(second_encoded[second_encoded_len], '\0');
  EXPECT_EQ(third_encoded[third_encoded_len], '\0');
  EXPECT_TRUE(isCanonicalEnvelopeText(first_encoded, first_encoded_len));
  EXPECT_TRUE(isCanonicalEnvelopeText(second_encoded, second_encoded_len));
  EXPECT_TRUE(isCanonicalEnvelopeText(third_encoded, third_encoded_len));

  ParsedEnvelope first = {};
  ParsedEnvelope second = {};
  ParsedEnvelope third = {};
  ASSERT_TRUE(parseEnvelope(first_encoded, first_encoded_len, &first));
  ASSERT_TRUE(parseEnvelope(second_encoded, second_encoded_len, &second));
  ASSERT_TRUE(parseEnvelope(third_encoded, third_encoded_len, &third));
  EXPECT_EQ(first.version, kEnvelopeVersion);
  EXPECT_EQ(first.algorithm, kEnvelopeAlgorithm);
  EXPECT_EQ(first.key_id, std::vector<uint8_t>(expected_key_id, expected_key_id + sizeof(expected_key_id)));
  EXPECT_EQ(first.key_id, second.key_id);
  EXPECT_EQ(first.key_id, third.key_id);
  EXPECT_EQ(first.wrapped_key, second.wrapped_key);
  EXPECT_EQ(first.wrapped_key, third.wrapped_key);
  EXPECT_NE(first.nonce, second.nonce);
  EXPECT_NE(first.nonce, third.nonce);
  EXPECT_NE(second.nonce, third.nonce);
  EXPECT_EQ(first.ciphertext.size(), fixture_plaintexts[0].size());
  EXPECT_EQ(second.ciphertext.size(), fixture_plaintexts[1].size());
  EXPECT_EQ(third.ciphertext.size(), sizeof(non_dsn_plaintext));

  const std::vector<uint8_t> first_decrypted = decryptEnvelope(first, private_key.get());
  const std::vector<uint8_t> second_decrypted = decryptEnvelope(second, private_key.get());
  const std::vector<uint8_t> third_decrypted = decryptEnvelope(third, private_key.get());
  EXPECT_EQ(first_decrypted, fixture_plaintexts[0]);
  EXPECT_EQ(second_decrypted, fixture_plaintexts[1]);
  EXPECT_EQ(third_decrypted, std::vector<uint8_t>(non_dsn_plaintext, non_dsn_plaintext + sizeof(non_dsn_plaintext)));

  releaseEncoded(&first_encoded);
  releaseEncoded(&second_encoded);
  releaseEncoded(&third_encoded);
  EXPECT_EQ(envelopeReleasePublicKey(&key), ENVELOPE_CRYPTO_SUCCESS);
}

TEST(EnvelopeCryptoTest, DoesNotReuseWrappedKeyAcrossBatches) {
  SEnvelopePublicKey *key = loadReferencePublicKey();
  ASSERT_NE(key, nullptr);
  const std::string plaintext = "******";
  ParsedEnvelope    envelopes[2] = {};

  for (int index = 0; index < 2; ++index) {
    SEnvelopeBatch *batch = nullptr;
    char           *encoded = nullptr;
    uint32_t        encoded_len = 0;
    ASSERT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_SUCCESS);
    ASSERT_EQ(envelopeBatchEncryptField(batch, reinterpret_cast<const uint8_t *>(plaintext.data()),
                                        static_cast<uint32_t>(plaintext.size()), ENVELOPE_CRYPTO_NO_ROLE_CAP, &encoded,
                                        &encoded_len),
              ENVELOPE_CRYPTO_SUCCESS);
    ASSERT_EQ(envelopeBatchFinish(&batch), ENVELOPE_CRYPTO_SUCCESS);
    ASSERT_TRUE(parseEnvelope(encoded, encoded_len, &envelopes[index]));
    releaseEncoded(&encoded);
  }

  EXPECT_NE(envelopes[0].wrapped_key, envelopes[1].wrapped_key);
  EXPECT_EQ(envelopeReleasePublicKey(&key), ENVELOPE_CRYPTO_SUCCESS);
}

TEST(EnvelopeCryptoTest, PerPageBatchesAreIndependentlyDecryptable) {
  SEnvelopePublicKey *key = loadReferencePublicKey();
  ASSERT_NE(key, nullptr);
  PkeyPtr private_key = referencePrivateKey();
  ASSERT_NE(private_key.get(), nullptr);

  const std::string page_plaintexts[] = {"taos://alice:secret@example/db", "taos://bob:token@example/db2"};
  ParsedEnvelope    pages[2] = {};

  // Each SHOW page finishes its own batch before delivery and the next page
  // begins a fresh batch, so every page is self-contained on the wire.
  for (int index = 0; index < 2; ++index) {
    SEnvelopeBatch *batch = nullptr;
    char           *encoded = nullptr;
    uint32_t        encoded_len = 0;
    ASSERT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_SUCCESS);
    ASSERT_EQ(envelopeBatchEncryptField(batch, reinterpret_cast<const uint8_t *>(page_plaintexts[index].data()),
                                        static_cast<uint32_t>(page_plaintexts[index].size()), 4096, &encoded,
                                        &encoded_len),
              ENVELOPE_CRYPTO_SUCCESS);
    ASSERT_EQ(envelopeBatchFinish(&batch), ENVELOPE_CRYPTO_SUCCESS);
    EXPECT_EQ(batch, nullptr);
    ASSERT_TRUE(parseEnvelope(encoded, encoded_len, &pages[index]));
    releaseEncoded(&encoded);
  }

  EXPECT_NE(pages[0].wrapped_key, pages[1].wrapped_key);
  for (int index = 0; index < 2; ++index) {
    const std::vector<uint8_t> decrypted = decryptEnvelope(pages[index], private_key.get());
    EXPECT_EQ(decrypted, std::vector<uint8_t>(page_plaintexts[index].begin(), page_plaintexts[index].end()));
  }
  EXPECT_EQ(envelopeReleasePublicKey(&key), ENVELOPE_CRYPTO_SUCCESS);
}

TEST(EnvelopeCryptoTest, EnforcesRoleCapsAndInvalidatesFailedBatch) {
  SEnvelopePublicKey *key = loadReferencePublicKey();
  ASSERT_NE(key, nullptr);

  const uint32_t caps[] = {4096, 2048};
  for (size_t index = 0; index < sizeof(caps) / sizeof(caps[0]); ++index) {
    const std::vector<uint8_t> accepted(caps[index], 'x');
    SEnvelopeBatch            *batch = nullptr;
    char                      *encoded = nullptr;
    uint32_t                   encoded_len = 0;
    ASSERT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_SUCCESS);
    EXPECT_EQ(envelopeBatchEncryptField(batch, accepted.data(), static_cast<uint32_t>(accepted.size()), caps[index],
                                        &encoded, &encoded_len),
              ENVELOPE_CRYPTO_SUCCESS);
    EXPECT_EQ(envelopeBatchFinish(&batch), ENVELOPE_CRYPTO_SUCCESS);
    releaseEncoded(&encoded);

    const std::vector<uint8_t> rejected(caps[index] + 1, 'x');
    ASSERT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_SUCCESS);
    EXPECT_EQ(envelopeBatchEncryptField(batch, rejected.data(), static_cast<uint32_t>(rejected.size()), caps[index],
                                        &encoded, &encoded_len),
              ENVELOPE_CRYPTO_ERROR_LENGTH_INVALID);
    EXPECT_EQ(encoded, nullptr);
    EXPECT_EQ(encoded_len, 0U);
    EXPECT_EQ(envelopeBatchFinish(&batch), ENVELOPE_CRYPTO_ERROR_STATE_INVALID);
    EXPECT_EQ(batch, nullptr);

    char    *previous_encoded = nullptr;
    uint32_t previous_encoded_len = 0;
    ASSERT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_SUCCESS);
    ASSERT_EQ(envelopeBatchEncryptField(batch, accepted.data(), static_cast<uint32_t>(accepted.size()), caps[index],
                                        &previous_encoded, &previous_encoded_len),
              ENVELOPE_CRYPTO_SUCCESS);
    EXPECT_EQ(envelopeBatchEncryptField(batch, rejected.data(), static_cast<uint32_t>(rejected.size()), caps[index],
                                        &encoded, &encoded_len),
              ENVELOPE_CRYPTO_ERROR_LENGTH_INVALID);
    EXPECT_EQ(envelopeBatchFinish(&batch), ENVELOPE_CRYPTO_ERROR_STATE_INVALID);
    releaseEncoded(&previous_encoded);
  }

  EXPECT_EQ(envelopeReleasePublicKey(&key), ENVELOPE_CRYPTO_SUCCESS);
}

TEST(EnvelopeCryptoTest, EnforcesProtocolMaximumWithoutRoleCap) {
  SEnvelopePublicKey *key = loadReferencePublicKey();
  ASSERT_NE(key, nullptr);
  const std::vector<uint8_t> too_long(1024U * 1024U + 1U, 'x');
  SEnvelopeBatch            *batch = nullptr;
  char                      *encoded = nullptr;
  uint32_t                   encoded_len = 0;
  ASSERT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_SUCCESS);
  EXPECT_EQ(envelopeBatchEncryptField(batch, too_long.data(), static_cast<uint32_t>(too_long.size()),
                                      ENVELOPE_CRYPTO_NO_ROLE_CAP, &encoded, &encoded_len),
            ENVELOPE_CRYPTO_ERROR_LENGTH_INVALID);
  EXPECT_EQ(encoded, nullptr);
  EXPECT_EQ(encoded_len, 0U);
  EXPECT_EQ(envelopeBatchAbort(&batch), ENVELOPE_CRYPTO_SUCCESS);
  EXPECT_EQ(batch, nullptr);
  EXPECT_EQ(envelopeReleasePublicKey(&key), ENVELOPE_CRYPTO_SUCCESS);
}

TEST(EnvelopeCryptoTest, AcceptsProtocolMaximumAndRetainsBatchKeyReference) {
  SEnvelopePublicKey *key = loadReferencePublicKey();
  ASSERT_NE(key, nullptr);
  PkeyPtr private_key = referencePrivateKey();
  ASSERT_NE(private_key.get(), nullptr);
  const std::vector<uint8_t> maximum_plaintext(1024U * 1024U, 'x');
  SEnvelopeBatch            *batch = nullptr;
  char                      *encoded = nullptr;
  uint32_t                   encoded_len = 0;

  ASSERT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_SUCCESS);
  ASSERT_EQ(envelopeReleasePublicKey(&key), ENVELOPE_CRYPTO_SUCCESS);
  ASSERT_EQ(envelopeBatchEncryptField(batch, maximum_plaintext.data(), static_cast<uint32_t>(maximum_plaintext.size()),
                                      ENVELOPE_CRYPTO_NO_ROLE_CAP, &encoded, &encoded_len),
            ENVELOPE_CRYPTO_SUCCESS);
  ASSERT_EQ(envelopeBatchFinish(&batch), ENVELOPE_CRYPTO_SUCCESS);
  const size_t expected_raw_length = kEnvelopeFixedRawLength + kEnvelopeRsaWrappedKeyLength + maximum_plaintext.size();
  const size_t expected_encoded_length = sizeof(kEnvelopePrefix) - 1 + envelopeBase64UrlLength(expected_raw_length);
  EXPECT_EQ(encoded_len, expected_encoded_length);
  EXPECT_EQ(encoded[encoded_len], '\0');
  EXPECT_TRUE(isCanonicalEnvelopeText(encoded, encoded_len));

  ParsedEnvelope envelope = {};
  ASSERT_TRUE(parseEnvelope(encoded, encoded_len, &envelope));
  EXPECT_EQ(envelope.ciphertext.size(), maximum_plaintext.size());
  EXPECT_EQ(decryptEnvelope(envelope, private_key.get()), maximum_plaintext);
  releaseEncoded(&encoded);
}

TEST(EnvelopeCryptoTest, RejectsInvalidUtf8AndTreatsSixStarsAsBytes) {
  SEnvelopePublicKey *key = loadReferencePublicKey();
  ASSERT_NE(key, nullptr);
  const std::vector<std::vector<uint8_t> > invalid_utf8_cases = {
      {0x80},
      {0xc0, 0xaf},
      {0xe0, 0x80, 0x80},
      {0xed, 0xa0, 0x80},
      {0xe2, 0x82},
      {0xf0, 0x80, 0x80, 0x80},
      {0xf4, 0x90, 0x80, 0x80},
      {0xf5, 0x80, 0x80, 0x80},
  };
  SEnvelopeBatch *batch = nullptr;
  char           *encoded = nullptr;
  uint32_t        encoded_len = 0;

  for (size_t index = 0; index < invalid_utf8_cases.size(); ++index) {
    ASSERT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_SUCCESS);
    EXPECT_EQ(envelopeBatchEncryptField(batch, invalid_utf8_cases[index].data(),
                                        static_cast<uint32_t>(invalid_utf8_cases[index].size()),
                                        ENVELOPE_CRYPTO_NO_ROLE_CAP, &encoded, &encoded_len),
              ENVELOPE_CRYPTO_ERROR_PLAINTEXT_UTF8_INVALID);
    EXPECT_EQ(envelopeBatchEncryptField(batch, invalid_utf8_cases[index].data(),
                                        static_cast<uint32_t>(invalid_utf8_cases[index].size()),
                                        ENVELOPE_CRYPTO_NO_ROLE_CAP, &encoded, &encoded_len),
              ENVELOPE_CRYPTO_ERROR_STATE_INVALID);
    EXPECT_EQ(envelopeBatchAbort(&batch), ENVELOPE_CRYPTO_SUCCESS);
  }

  const std::string six_stars = "******";
  ASSERT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_SUCCESS);
  EXPECT_EQ(envelopeBatchEncryptField(batch, reinterpret_cast<const uint8_t *>(six_stars.data()),
                                      static_cast<uint32_t>(six_stars.size()), ENVELOPE_CRYPTO_NO_ROLE_CAP, &encoded,
                                      &encoded_len),
            ENVELOPE_CRYPTO_SUCCESS);
  EXPECT_EQ(envelopeBatchFinish(&batch), ENVELOPE_CRYPTO_SUCCESS);
  releaseEncoded(&encoded);
  EXPECT_EQ(envelopeReleasePublicKey(&key), ENVELOPE_CRYPTO_SUCCESS);
}

TEST(EnvelopeCryptoTest, EncryptsAndDecryptsEmptyPlaintextWithNullInput) {
  SEnvelopePublicKey *key = loadReferencePublicKey();
  ASSERT_NE(key, nullptr);
  PkeyPtr private_key = referencePrivateKey();
  ASSERT_NE(private_key.get(), nullptr);

  SEnvelopeBatch *batch = nullptr;
  ASSERT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_SUCCESS);
  char    *encoded = nullptr;
  uint32_t encoded_len = 0;
  ASSERT_EQ(envelopeBatchEncryptField(batch, nullptr, 0, ENVELOPE_CRYPTO_NO_ROLE_CAP, &encoded, &encoded_len),
            ENVELOPE_CRYPTO_SUCCESS);
  ASSERT_EQ(envelopeBatchFinish(&batch), ENVELOPE_CRYPTO_SUCCESS);
  ASSERT_NE(encoded, nullptr);

  ParsedEnvelope envelope = {};
  ASSERT_TRUE(parseEnvelope(encoded, encoded_len, &envelope));
  EXPECT_TRUE(envelope.ciphertext.empty());
  EXPECT_EQ(decryptEnvelope(envelope, private_key.get()), std::vector<uint8_t>());

  releaseEncoded(&encoded);
  EXPECT_EQ(envelopeReleasePublicKey(&key), ENVELOPE_CRYPTO_SUCCESS);
}

TEST(EnvelopeCryptoTest, FinishWithoutFieldsFailsAndAbortIsIdempotent) {
  SEnvelopePublicKey *key = loadReferencePublicKey();
  ASSERT_NE(key, nullptr);
  SEnvelopeBatch *batch = nullptr;
  ASSERT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_SUCCESS);
  EXPECT_EQ(envelopeBatchFinish(&batch), ENVELOPE_CRYPTO_ERROR_STATE_INVALID);
  EXPECT_EQ(batch, nullptr);
  EXPECT_EQ(envelopeBatchAbort(&batch), ENVELOPE_CRYPTO_SUCCESS);
  EXPECT_EQ(envelopeReleasePublicKey(&key), ENVELOPE_CRYPTO_SUCCESS);
}

TEST(EnvelopeCryptoTest, NonNullOutputInvalidatesBatchAndClearsLength) {
  SEnvelopePublicKey *key = loadReferencePublicKey();
  ASSERT_NE(key, nullptr);
  SEnvelopeBatch *batch = nullptr;
  ASSERT_EQ(envelopeBatchBegin(key, &batch), ENVELOPE_CRYPTO_SUCCESS);

  char          sentinel = '\0';
  char         *encoded = &sentinel;
  uint32_t      encoded_len = 123;
  const uint8_t plaintext[] = {'x'};
  EXPECT_EQ(envelopeBatchEncryptField(batch, plaintext, sizeof(plaintext), ENVELOPE_CRYPTO_NO_ROLE_CAP, &encoded,
                                      &encoded_len),
            ENVELOPE_CRYPTO_ERROR_INVALID_PARAMETER);
  EXPECT_EQ(encoded, &sentinel);
  EXPECT_EQ(encoded_len, 0U);
  EXPECT_EQ(envelopeBatchFinish(&batch), ENVELOPE_CRYPTO_ERROR_STATE_INVALID);
  EXPECT_EQ(batch, nullptr);
  EXPECT_EQ(envelopeReleasePublicKey(&key), ENVELOPE_CRYPTO_SUCCESS);
}

}  // namespace
