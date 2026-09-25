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
 * @file emitC2RVectors.cpp
 * @brief XNODE DSN envelope crypto dynamic C->Rust interop vector emitter
 * @version 1.0
 * @date 2026-09-24
 *
 * `--emit-c2r-vectors=<path>` runs this emitter instead of the gtest suite.
 * It encrypts fresh batches through the production batch API with the
 * reference public key and writes the envelopes, expected plaintext bytes and
 * deterministic tamper negatives to <path>. The Rust-side `c2r_dynamic` test
 * in envelope-crypto consumes that file, so the two implementations meet only
 * through the wire format: no FFI and no shared build are involved.
 *
 * This translation unit depends only on envelope_crypto.h, osMemory.h and
 * OpenSSL (no gtest, no tjson) so the standalone CI emitter in
 * tools/cicd/tsdb-test-xservice/ci/c2r-emitter can rebuild it with gcc alone.
 */

#include "envelope_crypto.h"

#include <openssl/evp.h>

#include <cstdint>
#include <cstdio>
#include <fstream>
#include <string>
#include <tuple>
#include <vector>

// osMemPool.h must precede osMemory.h: osMemory.h poisons the libc names
// malloc/calloc/free before including osMemPool.h at its tail, and the os.h
// system-header chain (immintrin.h -> mm_malloc.h under -mavx2) still calls
// malloc. envelopeCryptoTest.cpp uses the same include order.
#include "osMemPool.h"
#include "osMemory.h"

int emitC2RVectors(const char *path);

namespace {

constexpr size_t kEnvelopeVersionLength = 1;
constexpr size_t kEnvelopeAlgorithmLength = 1;
constexpr size_t kEnvelopeKeyIdLength = 32;
constexpr size_t kEnvelopeWrappedKeyLengthFieldLength = 2;
constexpr size_t kEnvelopeRsaWrappedKeyLength = 256;
constexpr size_t kEnvelopeNonceLength = 12;
constexpr size_t kEnvelopePlaintextLengthFieldLength = 4;

constexpr char kEnvelopePrefix[] = "XNODE_DSN_V1:";

// PEM form of key_sets[0].public_spki_der_b64url in xnode-dsn-v1-fixtures.json
// (public half of the test-only "reference-rsa-2048-test-only" key pair; its
// SHA-256 kid is nD2TRpEW7AaLjtcF0TTuTzrxE_SmJFBBiXr-UPFikhQ). The private half
// stays in the Rust fixture copy. Regenerate this PEM if the fixture key pair
// is rotated; the c2r_dynamic gate fails loudly on any drift.
std::string referencePublicKeyPem() {
  return R"(-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAnH9xbJ4d/Mve1j7V5966
gZbxTY86fg7MW9s1ITVyADP+Ygj+1haai1qTzIWg8SkAsv2aOMdEelnQdkvwf4oO
dXxqJmY6fS3v+X20IY0IywE0/bTnd++xwRHVp8zXKIatpqy1NqoHC96NqFozN3Ml
pZEucJtcUSKxJ63/XiFj5TI144eKvDm+YXFEUNImEsG6IbQcsp85nwjarRyFDTez
eVao/Dupa14YVysIkSU3pOzoHeXuzqF/xREh0dIRiUiJ3TRZINKazfQOqwfSRYSE
m0aB9tzugVlGYhRS1m+cP7VMfCIyIEYDhz7yAjaWNDZIq40o6SrIohNme6BTJXON
YQIDAQAB
-----END PUBLIC KEY-----
)";
}

SEnvelopePublicKey *loadReferencePublicKey() {
  const std::string public_key_pem = referencePublicKeyPem();
  SEnvelopePublicKey *key = nullptr;
  if (envelopeLoadPublicKeyBytes(reinterpret_cast<const uint8_t *>(public_key_pem.data()),
                                 static_cast<int32_t>(public_key_pem.size()), &key) != ENVELOPE_CRYPTO_SUCCESS) {
    return nullptr;
  }
  return key;
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

std::string encodeBase64Url(const uint8_t *data, size_t length) {
  if (length == 0) {
    return std::string();
  }
  std::string padded(4 * ((length + 2) / 3), '\0');
  const int written =
      EVP_EncodeBlock(reinterpret_cast<unsigned char *>(&padded[0]), data, static_cast<int>(length));
  if (written < 0) {
    return std::string();
  }
  padded.resize(static_cast<size_t>(written));
  std::string result;
  result.reserve(padded.size());
  for (char value : padded) {
    if (value == '=') break;
    result.push_back(value == '+' ? '-' : value == '/' ? '_' : value);
  }
  return result;
}

std::vector<uint8_t> envelopeRawBytes(const std::string &envelope_text) {
  return decodeBase64Url(envelope_text.c_str() + sizeof(kEnvelopePrefix) - 1);
}

std::string rawToEnvelopeText(const std::vector<uint8_t> &raw) {
  return std::string(kEnvelopePrefix) + encodeBase64Url(raw.data(), raw.size());
}

uint32_t readUint32BigEndian(const uint8_t *value) {
  return (static_cast<uint32_t>(value[0]) << 24) | (static_cast<uint32_t>(value[1]) << 16) |
         (static_cast<uint32_t>(value[2]) << 8) | static_cast<uint32_t>(value[3]);
}

void releaseEncoded(char **encoded) {
  if (encoded != nullptr && *encoded != nullptr) {
    taosMemoryFree(*encoded);
    *encoded = nullptr;
  }
}

struct EmittedField {
  std::string field_id;
  std::string field_role;
  uint32_t    cap;
  std::string plaintext_b64url;
  std::string envelope_text;
};

bool encryptEmittedField(SEnvelopeBatch *batch, const char *field_id, const char *field_role, uint32_t cap,
                         const uint8_t *plaintext, uint32_t plaintext_len, EmittedField *out) {
  char    *encoded = nullptr;
  uint32_t encoded_len = 0;
  if (envelopeBatchEncryptField(batch, plaintext, plaintext_len, cap, &encoded, &encoded_len) !=
      ENVELOPE_CRYPTO_SUCCESS) {
    return false;
  }
  out->field_id = field_id;
  out->field_role = field_role;
  out->cap = cap;
  out->plaintext_b64url = encodeBase64Url(plaintext, plaintext_len);
  out->envelope_text.assign(encoded, encoded_len);
  releaseEncoded(&encoded);
  return true;
}

struct EmittedBatch {
  std::string                batch_id;
  std::vector<EmittedField>  fields;
};

bool emitBatch(SEnvelopePublicKey *key, const char *batch_id,
               const std::vector<std::tuple<const char *, const char *, uint32_t, std::vector<uint8_t> > > &specs,
               EmittedBatch *out) {
  SEnvelopeBatch *batch = nullptr;
  if (envelopeBatchBegin(key, &batch) != ENVELOPE_CRYPTO_SUCCESS) {
    return false;
  }
  out->batch_id = batch_id;
  for (const auto &spec : specs) {
    EmittedField field;
    const std::vector<uint8_t> &plaintext = std::get<3>(spec);
    if (!encryptEmittedField(batch, std::get<0>(spec), std::get<1>(spec), std::get<2>(spec),
                             plaintext.empty() ? nullptr : plaintext.data(),
                             static_cast<uint32_t>(plaintext.size()), &field)) {
      envelopeBatchAbort(&batch);
      return false;
    }
    out->fields.push_back(field);
  }
  return envelopeBatchFinish(&batch) == ENVELOPE_CRYPTO_SUCCESS;
}

struct EmittedTamper {
  std::string id;
  std::string expected_error_id;
  std::string envelope_text;
};

void addRawTamper(std::vector<EmittedTamper> *tampers, const char *id, const char *expected_error_id,
                  std::vector<uint8_t> raw) {
  EmittedTamper tamper;
  tamper.id = id;
  tamper.expected_error_id = expected_error_id;
  tamper.envelope_text = rawToEnvelopeText(raw);
  tampers->push_back(tamper);
}

std::vector<EmittedTamper> buildTamperCases(const std::string &envelope_text) {
  const std::vector<uint8_t> raw = envelopeRawBytes(envelope_text);
  const size_t wrapped_offset = kEnvelopeVersionLength + kEnvelopeAlgorithmLength + kEnvelopeKeyIdLength +
                                kEnvelopeWrappedKeyLengthFieldLength;
  const size_t nonce_offset = wrapped_offset + kEnvelopeRsaWrappedKeyLength;
  const size_t ciphertext_len_offset = nonce_offset + kEnvelopeNonceLength;
  const size_t ciphertext_offset = ciphertext_len_offset + kEnvelopePlaintextLengthFieldLength;
  std::vector<EmittedTamper> tampers;

  {
    std::vector<uint8_t> mutated = raw;
    mutated[mutated.size() - 1] ^= 0x01;
    addRawTamper(&tampers, "tampered-tag", "ENVELOPE_GCM_AUTH_FAILED", mutated);
  }
  {
    std::vector<uint8_t> mutated = raw;
    mutated[ciphertext_offset] ^= 0x01;
    addRawTamper(&tampers, "tampered-ciphertext", "ENVELOPE_GCM_AUTH_FAILED", mutated);
  }
  {
    std::vector<uint8_t> mutated = raw;
    mutated[nonce_offset] ^= 0x01;
    addRawTamper(&tampers, "tampered-nonce", "ENVELOPE_GCM_AUTH_FAILED", mutated);
  }
  {
    std::vector<uint8_t> mutated = raw;
    mutated[wrapped_offset] ^= 0x01;
    addRawTamper(&tampers, "tampered-wrapped-key", "ENVELOPE_RSA_UNWRAP_FAILED", mutated);
  }
  {
    std::vector<uint8_t> mutated = raw;
    mutated[kEnvelopeVersionLength + kEnvelopeAlgorithmLength] ^= 0x01;
    addRawTamper(&tampers, "wrong-kid", "ENVELOPE_KID_MISMATCH", mutated);
  }
  {
    // Declare one ciphertext byte less than present: the extra byte becomes
    // trailing data.
    std::vector<uint8_t> mutated = raw;
    const uint32_t declared = readUint32BigEndian(raw.data() + ciphertext_len_offset);
    const uint32_t shortened = declared - 1;
    mutated[ciphertext_len_offset] = static_cast<uint8_t>(shortened >> 24);
    mutated[ciphertext_len_offset + 1] = static_cast<uint8_t>(shortened >> 16);
    mutated[ciphertext_len_offset + 2] = static_cast<uint8_t>(shortened >> 8);
    mutated[ciphertext_len_offset + 3] = static_cast<uint8_t>(shortened);
    addRawTamper(&tampers, "trailing-data", "ENVELOPE_TRAILING_DATA", mutated);
  }
  {
    std::vector<uint8_t> mutated = raw;
    mutated.pop_back();
    addRawTamper(&tampers, "truncated-tag", "ENVELOPE_LENGTH_INVALID", mutated);
  }

  {
    // '+' is outside the unpadded Base64URL alphabet.
    std::string mutated = envelope_text;
    mutated[sizeof(kEnvelopePrefix) - 1] = '+';
    EmittedTamper tamper{"non-canonical-base64", "ENVELOPE_BASE64_INVALID", mutated};
    tampers.push_back(tamper);
  }
  {
    EmittedTamper tamper{"prefix-missing", "ENVELOPE_PREFIX_INVALID",
                         envelope_text.substr(sizeof(kEnvelopePrefix) - 1)};
    tampers.push_back(tamper);
  }
  return tampers;
}

}  // namespace

int emitC2RVectors(const char *path) {
  SEnvelopePublicKey *key = loadReferencePublicKey();
  if (key == nullptr) {
    fprintf(stderr, "emit-c2r-vectors: reference public key unavailable\n");
    return 1;
  }
  uint8_t kid[kEnvelopeKeyIdLength] = {0};
  if (envelopeGetKeyId(key, kid) != ENVELOPE_CRYPTO_SUCCESS) {
    fprintf(stderr, "emit-c2r-vectors: kid derivation failed\n");
    envelopeReleasePublicKey(&key);
    return 1;
  }

  const std::vector<uint8_t> cursor_from = {'t', 'a', 'o', 's', ':', '/', '/', 'a', 'l', 'i', 'c', 'e', ':',
                                            'F', 'u', 'l', 'l', 'P', '%', '4', '0', 's', 's', 'w', '0', 'r', 'd',
                                            '@', '1', '9', '2', '.', '1', '6', '8', '.', '1', '.', '1', '0', ':',
                                            '6', '0', '3', '0', '/', 'd', 'b', '?', 't', 'o', 'k', 'e', 'n', '=',
                                            'a', 'b', 'c', '1', '2', '3'};
  const std::vector<uint8_t> cursor_to = {'t', 'a', 'o', 's', ':', '/', '/', '1', '9', '2', '.', '1', '6', '8',
                                          '.', '1', '.', '2', '0', ':', '6', '0', '4', '1', '/', 't', 'a', 'r',
                                          'g', 'e', 't', '_', 'd', 'b'};
  const std::string            cursor_config_text = std::string("influxdb+http://user:pass@192.168.1.30:8086") +
                                         "/?bucket=\xE6\xB5\x8B\xE8\xAF\x95&precision=ms";
  const std::vector<uint8_t> cursor_config(cursor_config_text.begin(), cursor_config_text.end());
  const std::vector<uint8_t> empty_plaintext;
  const std::string            rpc_from_text = std::string("mqtt://user:pass@192.168.1.40:1883") +
                                    "?client_id=xnode-\xE5\x8A\xA8\xE6\x80\x81";
  const std::vector<uint8_t> rpc_from(rpc_from_text.begin(), rpc_from_text.end());
  const std::vector<uint8_t> rpc_to = {'*', '*', '*', '*', '*', '*'};

  std::vector<EmittedBatch> batches;
  EmittedBatch              cursor_batch;
  if (!emitBatch(key, "cursor-page",
                 {std::make_tuple("tasks.1.from", "task_from", 4096U, cursor_from),
                  std::make_tuple("tasks.1.to", "task_to", 2048U, cursor_to),
                  std::make_tuple("jobs.1.config", "job_from", 1048576U, cursor_config),
                  std::make_tuple("tasks.2.from", "task_from", 4096U, empty_plaintext)},
                 &cursor_batch)) {
    fprintf(stderr, "emit-c2r-vectors: cursor batch encryption failed\n");
    envelopeReleasePublicKey(&key);
    return 1;
  }
  batches.push_back(cursor_batch);

  EmittedBatch rpc_batch;
  if (!emitBatch(key, "rpc-start",
                 {std::make_tuple("task.from", "task_from", 4096U, rpc_from),
                  std::make_tuple("task.to", "task_to", 2048U, rpc_to)},
                 &rpc_batch)) {
    fprintf(stderr, "emit-c2r-vectors: rpc batch encryption failed\n");
    envelopeReleasePublicKey(&key);
    return 1;
  }
  batches.push_back(rpc_batch);

  const std::vector<EmittedTamper> tampers = buildTamperCases(batches[0].fields[0].envelope_text);

  std::ofstream output(path, std::ios::out | std::ios::binary | std::ios::trunc);
  if (!output) {
    fprintf(stderr, "emit-c2r-vectors: cannot open %s\n", path);
    envelopeReleasePublicKey(&key);
    return 1;
  }
  output << "{\n  \"schema\": \"xnode-c2r-dynamic-v1\",\n  \"key_id\": \"reference-rsa-2048-test-only\",\n";
  output << "  \"kid_b64url\": \"" << encodeBase64Url(kid, sizeof(kid)) << "\",\n  \"batches\": [\n";
  for (size_t batch_index = 0; batch_index < batches.size(); ++batch_index) {
    const EmittedBatch &batch = batches[batch_index];
    output << "    {\"batch_id\": \"" << batch.batch_id << "\", \"fields\": [\n";
    for (size_t field_index = 0; field_index < batch.fields.size(); ++field_index) {
      const EmittedField &field = batch.fields[field_index];
      output << "      {\"field_id\": \"" << field.field_id << "\", \"field_role\": \"" << field.field_role
             << "\", \"cap\": " << field.cap << ", \"plaintext_b64url\": \"" << field.plaintext_b64url
             << "\", \"envelope_text\": \"" << field.envelope_text << "\"}"
             << (field_index + 1 < batch.fields.size() ? ",\n" : "\n");
    }
    output << "    ]}" << (batch_index + 1 < batches.size() ? ",\n" : "\n");
  }
  output << "  ],\n  \"tamper_cases\": [\n";
  for (size_t index = 0; index < tampers.size(); ++index) {
    const EmittedTamper &tamper = tampers[index];
    output << "    {\"id\": \"" << tamper.id << "\", \"expected_error_id\": \"" << tamper.expected_error_id
           << "\", \"envelope_text\": \"" << tamper.envelope_text << "\"}"
           << (index + 1 < tampers.size() ? ",\n" : "\n");
  }
  output << "  ]\n}\n";
  output.flush();
  if (!output) {
    fprintf(stderr, "emit-c2r-vectors: failed to write %s\n", path);
    envelopeReleasePublicKey(&key);
    return 1;
  }
  printf("XNODE_C2R_VECTORS_WRITTEN=%s\n", path);
  printf("XNODE_C2R_KID_B64URL=%s\n", encodeBase64Url(kid, sizeof(kid)).c_str());
  envelopeReleasePublicKey(&key);
  return 0;
}
