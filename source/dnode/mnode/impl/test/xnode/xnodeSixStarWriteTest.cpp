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
 * @file xnodeSixStarWriteTest.cpp
 * @brief T8 mnode CREATE/ALTER six-star write semantics tests
 * @version 1.0
 * @date 2026-09-16
 *
 * This test loads the shared six-star merge-semantics fixture (the C mirror
 * copied byte-for-byte from the T1a staging directory) and executes every
 * scan_case and merge_case against the production C implementation in
 * mndXnode.c. The loader follows the T1a two-phase contract: the whole file is
 * validated first (unknown schema_version, unknown fields, duplicate ids,
 * missing conditional fields, JSON null, unknown decision and case_manifest
 * mismatches reject the entire file and zero cases run), then all cases are
 * executed without any C-side filtering or skipping. Expected values come only
 * from the fixture; the few extra tests at the bottom are mnode-specific
 * regressions (output-buffer boundary, real six-star credentials, authority
 * edges without a locatable userinfo password) that the shared fixture
 * deliberately does not pin.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <sstream>
#include <string>
#include <vector>

extern "C" {
#include "mndDef.h"
#include "mndXnode.h"
#include "osMemory.h"
#include "taoserror.h"
#include "tdef.h"
#include "tsha.h"
}
#include <cJSON.h>

namespace {

// ---------------------------------------------------------------------------
// Strict canonical unpadded Base64URL (fixture "bytes" lexical form)
// ---------------------------------------------------------------------------

int B64UrlValue(char c) {
  if (c >= 'A' && c <= 'Z') return c - 'A';
  if (c >= 'a' && c <= 'z') return c - 'a' + 26;
  if (c >= '0' && c <= '9') return c - '0' + 52;
  if (c == '-') return 62;
  if (c == '_') return 63;
  return -1;
}

std::string B64UrlEncode(const std::string &data) {
  static const char *kAlpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  std::string        out;
  for (size_t i = 0; i < data.size(); i += 3) {
    uint32_t block = static_cast<uint8_t>(data[i]) << 16;
    if (i + 1 < data.size()) block |= static_cast<uint8_t>(data[i + 1]) << 8;
    if (i + 2 < data.size()) block |= static_cast<uint8_t>(data[i + 2]);
    out.push_back(kAlpha[(block >> 18) & 63]);
    out.push_back(kAlpha[(block >> 12) & 63]);
    if (i + 1 < data.size()) out.push_back(kAlpha[(block >> 6) & 63]);
    if (i + 2 < data.size()) out.push_back(kAlpha[block & 63]);
  }
  return out;
}

// Decodes only the canonical unpadded form: any character outside the
// Base64URL alphabet, padding, an impossible length, or a re-encoding that
// differs byte-for-byte from the input is a lexical violation.
bool B64UrlDecode(const std::string &in, std::string *out) {
  if (in.size() % 4 == 1) return false;
  std::string decoded;
  uint32_t   acc = 0;
  int        bits = 0;
  for (char c : in) {
    int v = B64UrlValue(c);
    if (v < 0) return false;
    acc = (acc << 6) | static_cast<uint32_t>(v);
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      decoded.push_back(static_cast<char>((acc >> bits) & 0xff));
    }
  }
  if (B64UrlEncode(decoded) != in) return false;
  *out = decoded;
  return true;
}

// ---------------------------------------------------------------------------
// Fixture model
// ---------------------------------------------------------------------------

struct ExpectedSlot {
  std::string role;
  std::string wireKey;
  std::string decodedKey;
  std::string normalizedKey;
  int         ordinal = 0;
  std::string wireValue;
  std::string decodedValue;
};

struct ScanCase {
  std::string               id;
  std::string               input;
  bool                      accept = false;
  std::vector<ExpectedSlot> slots;
  std::string               errorId;
};

struct StateValue {
  bool        omitted = true;
  std::string bytes;
};

struct ObjectFields {
  StateValue from;
  StateValue to;
  StateValue config;
  int32_t    via = 0;
};

struct MergeCase {
  std::string  id;
  bool         create = false;
  bool         protection = false;
  bool         isJob = false;
  ObjectFields oldFields;
  ObjectFields candidate;
  bool         accept = false;
  ObjectFields finalFields;
  std::string  errorId;
};

struct Fixture {
  std::vector<ScanCase>  scanCases;
  std::vector<MergeCase> mergeCases;
};

// ---------------------------------------------------------------------------
// Two-phase fixture loader. Every function returns an empty string on success
// or the canonical FIXTURE_* error id; checks run in registry priority order.
// ---------------------------------------------------------------------------

bool HasKey(cJSON *obj, const char *key) { return cJSON_GetObjectItem(obj, key) != nullptr; }

bool IsUnknownKeyPresent(cJSON *obj, std::initializer_list<const char *> allowed) {
  for (cJSON *child = obj->child; child != nullptr; child = child->next) {
    if (child->string == nullptr) return true;
    bool found = false;
    for (const char *key : allowed) {
      if (strcmp(child->string, key) == 0) {
        found = true;
        break;
      }
    }
    if (!found) return true;
  }
  return false;
}

const char *GetStringField(cJSON *obj, const char *key) {
  cJSON *item = cJSON_GetObjectItem(obj, key);
  if (item == nullptr || !cJSON_IsString(item) || item->valuestring == nullptr) return nullptr;
  return item->valuestring;
}

std::string CheckSchemaVersion(cJSON *root) {
  if (!cJSON_IsObject(root)) return "FIXTURE_SCHEMA_INVALID";
  cJSON *version = cJSON_GetObjectItem(root, "schema_version");
  if (version == nullptr || !cJSON_IsNumber(version) || version->valueint != 1 ||
      version->valuedouble != 1.0) {
    return "FIXTURE_SCHEMA_UNKNOWN";
  }
  return "";
}

std::string CollectCaseIds(cJSON *root, std::vector<std::string> *ids) {
  for (const char *arrayKey : {"scan_cases", "merge_cases"}) {
    cJSON *cases = cJSON_GetObjectItem(root, arrayKey);
    if (!cJSON_IsArray(cases)) continue;
    for (cJSON *entry = cases->child; entry != nullptr; entry = entry->next) {
      const char *id = GetStringField(entry, "id");
      if (id != nullptr) ids->push_back(id);
    }
  }
  return "";
}

std::string CheckDuplicateIds(cJSON *root) {
  std::vector<std::string> ids;
  CollectCaseIds(root, &ids);
  std::vector<std::string> sorted = ids;
  std::sort(sorted.begin(), sorted.end());
  for (size_t i = 1; i < sorted.size(); ++i) {
    if (sorted[i] == sorted[i - 1]) return "FIXTURE_DUPLICATE_ID";
  }
  return "";
}

// Presence and conditional-state checks (FIXTURE_REQUIRED_FIELD): missing
// conditionally required fields and forbidden field states.
std::string CheckStateValueFields(cJSON *stateValue) {
  if (!cJSON_IsObject(stateValue)) return "FIXTURE_REQUIRED_FIELD";
  if (!HasKey(stateValue, "state")) return "FIXTURE_REQUIRED_FIELD";
  const char *state = GetStringField(stateValue, "state");
  if (state == nullptr) return "";  // wrong type falls to FIXTURE_SCHEMA_INVALID later
  if (strcmp(state, "omitted") == 0 && HasKey(stateValue, "bytes_b64url")) return "FIXTURE_REQUIRED_FIELD";
  if (strcmp(state, "value") == 0 && !HasKey(stateValue, "bytes_b64url")) return "FIXTURE_REQUIRED_FIELD";
  return "";
}

std::string CheckObjectFields(cJSON *fields) {
  if (!cJSON_IsObject(fields)) return "FIXTURE_REQUIRED_FIELD";
  cJSON *via = cJSON_GetObjectItem(fields, "via");
  if (via != nullptr && (!cJSON_IsNumber(via) || via->valuedouble != static_cast<double>(via->valueint))) {
    return "FIXTURE_SCHEMA_INVALID";
  }
  for (const char *key : {"from", "to", "config"}) {
    if (!HasKey(fields, key)) return "FIXTURE_REQUIRED_FIELD";
    std::string err = CheckStateValueFields(cJSON_GetObjectItem(fields, key));
    if (!err.empty()) return err;
  }
  return "";
}

const char *StateOf(cJSON *fields, const char *key) {
  cJSON *stateValue = cJSON_GetObjectItem(fields, key);
  if (stateValue == nullptr) return nullptr;
  return GetStringField(stateValue, "state");
}

bool IsState(cJSON *fields, const char *key, const char *state) {
  const char *actual = StateOf(fields, key);
  return actual != nullptr && strcmp(actual, state) == 0;
}

std::string CheckResourceConditions(cJSON *mergeCase, cJSON *fields, bool finalFields) {
  const char *resource = GetStringField(mergeCase, "resource");
  if (resource == nullptr) return "";  // wrong type falls to FIXTURE_SCHEMA_INVALID later
  if (strcmp(resource, "task") == 0) {
    if (!IsState(fields, "config", "omitted")) return "FIXTURE_REQUIRED_FIELD";
    if (finalFields && (!IsState(fields, "from", "value") || !IsState(fields, "to", "value"))) {
      return "FIXTURE_REQUIRED_FIELD";
    }
  } else if (strcmp(resource, "job") == 0) {
    if (!IsState(fields, "from", "omitted") || !IsState(fields, "to", "omitted")) return "FIXTURE_REQUIRED_FIELD";
    if (finalFields && !IsState(fields, "config", "value")) return "FIXTURE_REQUIRED_FIELD";
  }
  return "";
}

std::string CheckExpectedFields(cJSON *mergeCase, cJSON *expected, bool hasSlots) {
  if (!cJSON_IsObject(expected)) return "FIXTURE_REQUIRED_FIELD";
  if (!HasKey(expected, "decision")) return "FIXTURE_REQUIRED_FIELD";
  if (hasSlots && !HasKey(expected, "slots")) return "FIXTURE_REQUIRED_FIELD";
  const char *decision = GetStringField(expected, "decision");
  if (decision == nullptr) return "";  // unknown decision/type handled by later phases
  if (strcmp(decision, "accept") == 0) {
    if (HasKey(expected, "error_id")) return "FIXTURE_REQUIRED_FIELD";
    if (mergeCase != nullptr) {
      if (!HasKey(expected, "final")) return "FIXTURE_REQUIRED_FIELD";
      cJSON *finalFields = cJSON_GetObjectItem(expected, "final");
      std::string err = CheckObjectFields(finalFields);
      if (!err.empty()) return err;
      err = CheckResourceConditions(mergeCase, finalFields, true);
      if (!err.empty()) return err;
    }
  } else if (strcmp(decision, "reject") == 0) {
    if (!HasKey(expected, "error_id")) return "FIXTURE_REQUIRED_FIELD";
    if (mergeCase != nullptr && HasKey(expected, "final")) return "FIXTURE_REQUIRED_FIELD";
  }
  return "";
}

std::string CheckRequiredFields(cJSON *root) {
  for (const char *key : {"case_manifest", "scan_cases", "merge_cases"}) {
    if (!HasKey(root, key)) return "FIXTURE_REQUIRED_FIELD";
  }
  cJSON *manifest = cJSON_GetObjectItem(root, "case_manifest");
  if (!cJSON_IsObject(manifest)) return "FIXTURE_REQUIRED_FIELD";
  for (const char *key : {"scan_case_count", "merge_case_count", "case_ids", "case_ids_sha256_b64url"}) {
    if (!HasKey(manifest, key)) return "FIXTURE_REQUIRED_FIELD";
  }
  cJSON *scanCases = cJSON_GetObjectItem(root, "scan_cases");
  if (!cJSON_IsArray(scanCases)) return "FIXTURE_REQUIRED_FIELD";
  for (cJSON *entry = scanCases->child; entry != nullptr; entry = entry->next) {
    if (!cJSON_IsObject(entry)) return "FIXTURE_REQUIRED_FIELD";
    for (const char *key : {"id", "input_bytes_b64url", "expected"}) {
      if (!HasKey(entry, key)) return "FIXTURE_REQUIRED_FIELD";
    }
    std::string err = CheckExpectedFields(nullptr, cJSON_GetObjectItem(entry, "expected"), true);
    if (!err.empty()) return err;
  }
  cJSON *mergeCases = cJSON_GetObjectItem(root, "merge_cases");
  if (!cJSON_IsArray(mergeCases)) return "FIXTURE_REQUIRED_FIELD";
  for (cJSON *entry = mergeCases->child; entry != nullptr; entry = entry->next) {
    if (!cJSON_IsObject(entry)) return "FIXTURE_REQUIRED_FIELD";
    for (const char *key : {"id", "operation", "protection_enabled", "resource", "old", "candidate", "expected"}) {
      if (!HasKey(entry, key)) return "FIXTURE_REQUIRED_FIELD";
    }
    cJSON     *oldFields = cJSON_GetObjectItem(entry, "old");
    cJSON     *candidate = cJSON_GetObjectItem(entry, "candidate");
    std::string err = CheckObjectFields(oldFields);
    if (!err.empty()) return err;
    err = CheckObjectFields(candidate);
    if (!err.empty()) return err;
    err = CheckResourceConditions(entry, oldFields, false);
    if (!err.empty()) return err;
    err = CheckResourceConditions(entry, candidate, false);
    if (!err.empty()) return err;
    const char *operation = GetStringField(entry, "operation");
    if (operation != nullptr && strcmp(operation, "create") == 0) {
      if (!IsState(oldFields, "from", "omitted") || !IsState(oldFields, "to", "omitted") ||
          !IsState(oldFields, "config", "omitted")) {
        return "FIXTURE_REQUIRED_FIELD";
      }
    }
    err = CheckExpectedFields(entry, cJSON_GetObjectItem(entry, "expected"), false);
    if (!err.empty()) return err;
  }
  return "";
}

std::string CheckDecisions(cJSON *root) {
  for (const char *arrayKey : {"scan_cases", "merge_cases"}) {
    cJSON *cases = cJSON_GetObjectItem(root, arrayKey);
    if (!cJSON_IsArray(cases)) continue;
    for (cJSON *entry = cases->child; entry != nullptr; entry = entry->next) {
      cJSON *expected = cJSON_GetObjectItem(entry, "expected");
      if (expected == nullptr) continue;
      cJSON *decision = cJSON_GetObjectItem(expected, "decision");
      if (decision == nullptr) continue;
      if (!cJSON_IsString(decision) || decision->valuestring == nullptr ||
          (strcmp(decision->valuestring, "accept") != 0 && strcmp(decision->valuestring, "reject") != 0)) {
        return "FIXTURE_UNKNOWN_DECISION";
      }
    }
  }
  return "";
}

std::string CheckCaseManifest(cJSON *root) {
  cJSON *manifest = cJSON_GetObjectItem(root, "case_manifest");
  cJSON *scanCases = cJSON_GetObjectItem(root, "scan_cases");
  cJSON *mergeCases = cJSON_GetObjectItem(root, "merge_cases");
  cJSON *scanCount = cJSON_GetObjectItem(manifest, "scan_case_count");
  cJSON *mergeCount = cJSON_GetObjectItem(manifest, "merge_case_count");
  cJSON *caseIds = cJSON_GetObjectItem(manifest, "case_ids");
  cJSON *digest = cJSON_GetObjectItem(manifest, "case_ids_sha256_b64url");
  if (!cJSON_IsNumber(scanCount) || !cJSON_IsNumber(mergeCount) || !cJSON_IsArray(caseIds) ||
      !cJSON_IsString(digest) || !cJSON_IsArray(scanCases) || !cJSON_IsArray(mergeCases)) {
    return "FIXTURE_CASE_SET_MISMATCH";
  }
  if (scanCount->valueint != cJSON_GetArraySize(scanCases) ||
      mergeCount->valueint != cJSON_GetArraySize(mergeCases)) {
    return "FIXTURE_CASE_SET_MISMATCH";
  }
  std::vector<std::string> declared;
  for (cJSON *entry = caseIds->child; entry != nullptr; entry = entry->next) {
    if (!cJSON_IsString(entry) || entry->valuestring == nullptr) return "FIXTURE_CASE_SET_MISMATCH";
    declared.push_back(entry->valuestring);
  }
  std::vector<std::string> actual;
  CollectCaseIds(root, &actual);
  std::sort(actual.begin(), actual.end());
  if (declared != actual) return "FIXTURE_CASE_SET_MISMATCH";
  std::string joined;
  for (size_t i = 0; i < actual.size(); ++i) {
    if (i > 0) joined.push_back('\n');
    joined += actual[i];
  }
  uint8_t hash[SHA256_DIGEST_SIZE];
  sha256(reinterpret_cast<const uint8_t *>(joined.data()), joined.size(), hash);
  std::string digestBytes;
  if (!B64UrlDecode(digest->valuestring, &digestBytes) ||
      digestBytes.size() != SHA256_DIGEST_SIZE ||
      memcmp(digestBytes.data(), hash, SHA256_DIGEST_SIZE) != 0) {
    return "FIXTURE_CASE_SET_MISMATCH";
  }
  return "";
}

bool IsValidCaseId(const std::string &id, const char *prefix) {
  size_t prefixLen = strlen(prefix);
  if (id.compare(0, prefixLen, prefix) != 0 || id.size() == prefixLen) return false;
  for (char c : id.substr(prefixLen)) {
    if (!isalnum(static_cast<unsigned char>(c)) && c != '.' && c != '_' && c != '-') return false;
  }
  return true;
}

bool IsValidNormalizedKey(const std::string &key) {
  for (char c : key) {
    if ((c >= 'A' && c <= 'Z') || c == '_' || c == '.' || c == '-') return false;
  }
  return true;
}

std::string DecodeBytesField(cJSON *obj, const char *key, std::string *out) {
  cJSON *item = cJSON_GetObjectItem(obj, key);
  if (item == nullptr || cJSON_IsNull(item) || !cJSON_IsString(item) || item->valuestring == nullptr) {
    return "FIXTURE_SCHEMA_INVALID";
  }
  if (!B64UrlDecode(item->valuestring, out)) return "FIXTURE_SCHEMA_INVALID";
  return "";
}

std::string ExtractStateValue(cJSON *stateValue, StateValue *out) {
  if (!cJSON_IsObject(stateValue)) return "FIXTURE_SCHEMA_INVALID";
  if (IsUnknownKeyPresent(stateValue, {"state", "bytes_b64url"})) return "FIXTURE_SCHEMA_INVALID";
  const char *state = GetStringField(stateValue, "state");
  if (state == nullptr) return "FIXTURE_SCHEMA_INVALID";
  if (strcmp(state, "omitted") == 0) {
    out->omitted = true;
    return "";
  }
  if (strcmp(state, "value") != 0) return "FIXTURE_SCHEMA_INVALID";
  out->omitted = false;
  return DecodeBytesField(stateValue, "bytes_b64url", &out->bytes);
}

std::string ExtractObjectFields(cJSON *fields, ObjectFields *out) {
  if (!cJSON_IsObject(fields)) return "FIXTURE_SCHEMA_INVALID";
  if (IsUnknownKeyPresent(fields, {"via", "from", "to", "config"})) return "FIXTURE_SCHEMA_INVALID";
  cJSON *via = cJSON_GetObjectItem(fields, "via");
  if (via != nullptr) out->via = via->valueint;
  std::string err = ExtractStateValue(cJSON_GetObjectItem(fields, "from"), &out->from);
  if (!err.empty()) return err;
  err = ExtractStateValue(cJSON_GetObjectItem(fields, "to"), &out->to);
  if (!err.empty()) return err;
  return ExtractStateValue(cJSON_GetObjectItem(fields, "config"), &out->config);
}

// Six-star error_ids accepted by the shared fixture schema (canonical registry
// IDs 40-46, with 41 SIXSTAR_PERCENT_ENCODING_INVALID retired), mirroring
// EXnodeSixStarError in mndXnode.h.
const char *const kSixStarIds[] = {"SIXSTAR_INPUT_TYPE_INVALID", "SIXSTAR_FIELD_AMBIGUOUS",
                                   "SIXSTAR_CREATE_MASKED",      "SIXSTAR_KEEP_MISSING",
                                   "SIXSTAR_JOB_JSON_INVALID",   "SIXSTAR_CONNECTION_CHANGED"};

std::string ExtractScanCase(cJSON *entry, ScanCase *out) {
  if (IsUnknownKeyPresent(entry, {"id", "input_bytes_b64url", "expected"})) return "FIXTURE_SCHEMA_INVALID";
  const char *id = GetStringField(entry, "id");
  if (id == nullptr || !IsValidCaseId(id, "scan-")) return "FIXTURE_SCHEMA_INVALID";
  out->id = id;
  std::string err = DecodeBytesField(entry, "input_bytes_b64url", &out->input);
  if (!err.empty()) return err;
  cJSON *expected = cJSON_GetObjectItem(entry, "expected");
  if (!cJSON_IsObject(expected)) return "FIXTURE_SCHEMA_INVALID";
  if (IsUnknownKeyPresent(expected, {"decision", "slots", "error_id"})) return "FIXTURE_SCHEMA_INVALID";
  const char *decision = GetStringField(expected, "decision");
  if (decision == nullptr) return "FIXTURE_SCHEMA_INVALID";
  out->accept = strcmp(decision, "accept") == 0;
  cJSON *slots = cJSON_GetObjectItem(expected, "slots");
  if (!cJSON_IsArray(slots)) return "FIXTURE_SCHEMA_INVALID";
  if (!out->accept) {
    if (cJSON_GetArraySize(slots) != 0) return "FIXTURE_SCHEMA_INVALID";
    const char *errorId = GetStringField(expected, "error_id");
    if (errorId == nullptr) return "FIXTURE_SCHEMA_INVALID";
    bool known = false;
    for (const char *candidate : kSixStarIds) {
      if (strcmp(errorId, candidate) == 0) known = true;
    }
    if (!known) return "FIXTURE_SCHEMA_INVALID";
    out->errorId = errorId;
    return "";
  }
  for (cJSON *slot = slots->child; slot != nullptr; slot = slot->next) {
    if (!cJSON_IsObject(slot)) return "FIXTURE_SCHEMA_INVALID";
    if (IsUnknownKeyPresent(slot, {"field_role", "wire_key_b64url", "decoded_key_b64url", "normalized_key",
                                   "duplicate_ordinal", "wire_value_b64url", "decoded_value_b64url"})) {
      return "FIXTURE_SCHEMA_INVALID";
    }
    ExpectedSlot expectedSlot;
    const char  *role = GetStringField(slot, "field_role");
    if (role == nullptr ||
        (strcmp(role, "userinfo_password") != 0 && strcmp(role, "query_sensitive") != 0)) {
      return "FIXTURE_SCHEMA_INVALID";
    }
    expectedSlot.role = role;
    err = DecodeBytesField(slot, "wire_key_b64url", &expectedSlot.wireKey);
    if (!err.empty()) return err;
    err = DecodeBytesField(slot, "decoded_key_b64url", &expectedSlot.decodedKey);
    if (!err.empty()) return err;
    const char *normalizedKey = GetStringField(slot, "normalized_key");
    if (normalizedKey == nullptr || !IsValidNormalizedKey(normalizedKey)) return "FIXTURE_SCHEMA_INVALID";
    expectedSlot.normalizedKey = normalizedKey;
    cJSON *ordinal = cJSON_GetObjectItem(slot, "duplicate_ordinal");
    if (ordinal == nullptr || !cJSON_IsNumber(ordinal) || ordinal->valueint < 0 ||
        ordinal->valuedouble != static_cast<double>(ordinal->valueint)) {
      return "FIXTURE_SCHEMA_INVALID";
    }
    expectedSlot.ordinal = ordinal->valueint;
    err = DecodeBytesField(slot, "wire_value_b64url", &expectedSlot.wireValue);
    if (!err.empty()) return err;
    err = DecodeBytesField(slot, "decoded_value_b64url", &expectedSlot.decodedValue);
    if (!err.empty()) return err;
    out->slots.push_back(expectedSlot);
  }
  return "";
}

std::string ExtractMergeCase(cJSON *entry, MergeCase *out) {
  if (IsUnknownKeyPresent(
          entry, {"id", "operation", "protection_enabled", "resource", "old", "candidate", "expected"})) {
    return "FIXTURE_SCHEMA_INVALID";
  }
  const char *id = GetStringField(entry, "id");
  if (id == nullptr || !IsValidCaseId(id, "merge-")) return "FIXTURE_SCHEMA_INVALID";
  out->id = id;
  const char *operation = GetStringField(entry, "operation");
  if (operation == nullptr || (strcmp(operation, "create") != 0 && strcmp(operation, "alter") != 0)) {
    return "FIXTURE_SCHEMA_INVALID";
  }
  out->create = strcmp(operation, "create") == 0;
  cJSON *protection = cJSON_GetObjectItem(entry, "protection_enabled");
  if (protection == nullptr || !cJSON_IsBool(protection)) return "FIXTURE_SCHEMA_INVALID";
  out->protection = cJSON_IsTrue(protection) != 0;
  const char *resource = GetStringField(entry, "resource");
  if (resource == nullptr || (strcmp(resource, "task") != 0 && strcmp(resource, "job") != 0)) {
    return "FIXTURE_SCHEMA_INVALID";
  }
  out->isJob = strcmp(resource, "job") == 0;
  std::string err = ExtractObjectFields(cJSON_GetObjectItem(entry, "old"), &out->oldFields);
  if (!err.empty()) return err;
  err = ExtractObjectFields(cJSON_GetObjectItem(entry, "candidate"), &out->candidate);
  if (!err.empty()) return err;
  cJSON *expected = cJSON_GetObjectItem(entry, "expected");
  if (!cJSON_IsObject(expected)) return "FIXTURE_SCHEMA_INVALID";
  if (IsUnknownKeyPresent(expected, {"decision", "final", "error_id"})) return "FIXTURE_SCHEMA_INVALID";
  const char *decision = GetStringField(expected, "decision");
  if (decision == nullptr) return "FIXTURE_SCHEMA_INVALID";
  out->accept = strcmp(decision, "accept") == 0;
  if (out->accept) {
    return ExtractObjectFields(cJSON_GetObjectItem(expected, "final"), &out->finalFields);
  }
  const char *errorId = GetStringField(expected, "error_id");
  if (errorId == nullptr) return "FIXTURE_SCHEMA_INVALID";
  bool known = false;
  for (const char *candidate : kSixStarIds) {
    if (strcmp(errorId, candidate) == 0) known = true;
  }
  if (!known) return "FIXTURE_SCHEMA_INVALID";
  out->errorId = errorId;
  return "";
}

// Residual schema validation (unknown fields, JSON null, type and lexical
// violations) plus base64url decoding and extraction into the case model.
std::string ValidateAndExtract(cJSON *root, Fixture *out) {
  if (IsUnknownKeyPresent(root, {"schema_version", "case_manifest", "scan_cases", "merge_cases"})) {
    return "FIXTURE_SCHEMA_INVALID";
  }
  cJSON *manifest = cJSON_GetObjectItem(root, "case_manifest");
  if (IsUnknownKeyPresent(manifest,
                          {"scan_case_count", "merge_case_count", "case_ids", "case_ids_sha256_b64url"})) {
    return "FIXTURE_SCHEMA_INVALID";
  }
  cJSON *scanCases = cJSON_GetObjectItem(root, "scan_cases");
  if (cJSON_GetArraySize(scanCases) < 1) return "FIXTURE_SCHEMA_INVALID";
  for (cJSON *entry = scanCases->child; entry != nullptr; entry = entry->next) {
    ScanCase scanCase;
    std::string err = ExtractScanCase(entry, &scanCase);
    if (!err.empty()) return err;
    out->scanCases.push_back(scanCase);
  }
  cJSON *mergeCases = cJSON_GetObjectItem(root, "merge_cases");
  if (cJSON_GetArraySize(mergeCases) < 1) return "FIXTURE_SCHEMA_INVALID";
  for (cJSON *entry = mergeCases->child; entry != nullptr; entry = entry->next) {
    MergeCase mergeCase;
    std::string err = ExtractMergeCase(entry, &mergeCase);
    if (!err.empty()) return err;
    out->mergeCases.push_back(mergeCase);
  }
  return "";
}

// Phase 1 (load): the whole document is validated; any failure rejects the
// entire file and no case is executed. Returns "" on success.
std::string LoadSixStarFixture(const std::string &jsonText, Fixture *out) {
  cJSON *root = cJSON_ParseWithLength(jsonText.data(), jsonText.size());
  if (root == nullptr) return "FIXTURE_SCHEMA_INVALID";
  std::string err = CheckSchemaVersion(root);
  if (err.empty()) err = CheckDuplicateIds(root);
  if (err.empty()) err = CheckRequiredFields(root);
  if (err.empty()) err = CheckDecisions(root);
  if (err.empty()) err = CheckCaseManifest(root);
  if (err.empty()) err = ValidateAndExtract(root, out);
  cJSON_Delete(root);
  return err;
}

std::string MutateFixture(const std::string &jsonText, const std::function<void(cJSON *)> &mutate) {
  cJSON *root = cJSON_ParseWithLength(jsonText.data(), jsonText.size());
  if (root == nullptr) return "";
  mutate(root);
  char *printed = cJSON_PrintUnformatted(root);
  std::string result = printed != nullptr ? printed : "";
  if (printed != nullptr) cJSON_free(printed);
  cJSON_Delete(root);
  return result;
}

const char *SixStarErrorName(int32_t sixStarError) {
  switch (sixStarError) {
    case XNODE_SIXSTAR_ERROR_INPUT_TYPE_INVALID:
      return "SIXSTAR_INPUT_TYPE_INVALID";
    case XNODE_SIXSTAR_ERROR_FIELD_AMBIGUOUS:
      return "SIXSTAR_FIELD_AMBIGUOUS";
    case XNODE_SIXSTAR_ERROR_CREATE_MASKED:
      return "SIXSTAR_CREATE_MASKED";
    case XNODE_SIXSTAR_ERROR_KEEP_MISSING:
      return "SIXSTAR_KEEP_MISSING";
    case XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID:
      return "SIXSTAR_JOB_JSON_INVALID";
    case XNODE_SIXSTAR_ERROR_CONNECTION_CHANGED:
      return "SIXSTAR_CONNECTION_CHANGED";
    default:
      return "SIXSTAR_UNKNOWN";
  }
}

SXnodeSixStarField FieldOf(const StateValue &state) {
  SXnodeSixStarField field = {nullptr, 0};
  if (!state.omitted) {
    field.ptr = state.bytes.data();
    field.len = static_cast<int32_t>(state.bytes.size());
  }
  return field;
}

class XnodeSixStarWriteTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::ifstream stream(XNODE_SIXSTAR_FIXTURE_PATH, std::ios::binary);
    std::stringstream buffer;
    buffer << stream.rdbuf();
    fixtureText = buffer.str();
    loadError = LoadSixStarFixture(fixtureText, &fixture);
  }

  static std::string fixtureText;
  static std::string loadError;
  static Fixture     fixture;
};

std::string XnodeSixStarWriteTest::fixtureText;
std::string XnodeSixStarWriteTest::loadError;
Fixture     XnodeSixStarWriteTest::fixture;

TEST_F(XnodeSixStarWriteTest, LoadsWholeFixtureWithoutRejection) {
  EXPECT_EQ(loadError, "");
  EXPECT_GT(fixture.scanCases.size(), 0u);
  EXPECT_GT(fixture.mergeCases.size(), 0u);

  // T14 interop gate: emit the load-validated manifest digest so the release
  // report can compare it against the Rust side's report line.
  cJSON *root = cJSON_Parse(fixtureText.c_str());
  if (root != nullptr) {
    cJSON *manifest = cJSON_GetObjectItem(root, "case_manifest");
    cJSON *digest = manifest == nullptr ? nullptr : cJSON_GetObjectItem(manifest, "case_ids_sha256_b64url");
    if (digest != nullptr && cJSON_IsString(digest)) {
      printf("XNODE_SIXSTAR_CASE_IDS_SHA256=%s\n", digest->valuestring);
    }
    cJSON_Delete(root);
  }
}

// Phase 2 (execute): every case runs; a failed case is recorded by the
// framework and the remaining cases still execute.
TEST_F(XnodeSixStarWriteTest, ExecutesAllScanCases) {
  ASSERT_EQ(loadError, "");
  for (const ScanCase &scanCase : fixture.scanCases) {
    SCOPED_TRACE(scanCase.id);
    SXnodeSixStarSlot *slots = nullptr;
    int32_t          numSlots = 0;
    int32_t          sixStarError = XNODE_SIXSTAR_ERROR_NONE;
    int32_t          code = mndXnodeSixStarScanDsn(scanCase.input.data(),
                                                   static_cast<int32_t>(scanCase.input.size()), &slots, &numSlots,
                                                   &sixStarError);
    if (scanCase.accept) {
      EXPECT_EQ(code, TSDB_CODE_SUCCESS);
      EXPECT_EQ(numSlots, static_cast<int32_t>(scanCase.slots.size()));
      if (code == TSDB_CODE_SUCCESS && numSlots == static_cast<int32_t>(scanCase.slots.size())) {
        for (int32_t i = 0; i < numSlots; ++i) {
          const ExpectedSlot     &expected = scanCase.slots[i];
          const SXnodeSixStarSlot &actual = slots[i];
          EXPECT_EQ(actual.role, expected.role == "userinfo_password"
                                     ? XNODE_SIXSTAR_ROLE_USERINFO_PASSWORD
                                     : XNODE_SIXSTAR_ROLE_QUERY_SENSITIVE);
          std::string wireKey;
          if (actual.wireKeyOff >= 0) wireKey = scanCase.input.substr(actual.wireKeyOff, actual.wireKeyLen);
          EXPECT_EQ(wireKey, expected.wireKey);
          EXPECT_EQ(std::string(actual.decodedKey, actual.decodedKeyLen), expected.decodedKey);
          EXPECT_EQ(std::string(actual.normalizedKey, actual.normalizedKeyLen), expected.normalizedKey);
          EXPECT_EQ(actual.duplicateOrdinal, expected.ordinal);
          std::string wireValue = scanCase.input.substr(actual.wireValueOff, actual.wireValueLen);
          EXPECT_EQ(wireValue, expected.wireValue);
          EXPECT_EQ(std::string(actual.decodedValue, actual.decodedValueLen), expected.decodedValue);
        }
      }
    } else {
      // CONNECTION_CHANGED carries its own SQL-visible code so clients can
      // tell "re-enter the sensitive values" apart from other rejections.
      if (scanCase.errorId == "SIXSTAR_CONNECTION_CHANGED") {
        EXPECT_EQ(code, TSDB_CODE_MND_XNODE_DSN_CONNECTION_CHANGED);
      } else {
        EXPECT_EQ(code, TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
      }
      EXPECT_EQ(SixStarErrorName(sixStarError), scanCase.errorId);
      EXPECT_EQ(numSlots, 0);
      EXPECT_EQ(slots, nullptr);
    }
    mndXnodeSixStarFreeSlots(slots, numSlots);
  }
}

TEST_F(XnodeSixStarWriteTest, ExecutesAllMergeCases) {
  ASSERT_EQ(loadError, "");
  for (const MergeCase &mergeCase : fixture.mergeCases) {
    SCOPED_TRACE(mergeCase.id);
    SXnodeSixStarMergeInput input = {0};
    input.create = mergeCase.create;
    input.protectionEnabled = mergeCase.protection;
    input.isJob = mergeCase.isJob;
    input.oldFrom = FieldOf(mergeCase.oldFields.from);
    input.oldTo = FieldOf(mergeCase.oldFields.to);
    input.oldConfig = FieldOf(mergeCase.oldFields.config);
    input.oldVia = mergeCase.oldFields.via;
    input.candidateFrom = FieldOf(mergeCase.candidate.from);
    input.candidateTo = FieldOf(mergeCase.candidate.to);
    input.candidateConfig = FieldOf(mergeCase.candidate.config);
    input.candidateVia = mergeCase.candidate.via;
    input.maxFromLen = TSDB_XNODE_TASK_SOURCE_LEN;
    input.maxToLen = TSDB_XNODE_TASK_SINK_LEN;
    input.maxConfigLen = TSDB_XNODE_TASK_JOB_CONFIG_SHOW_MAX_LEN;
    SXnodeSixStarMergeOutput output = {0};
    int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
    int32_t                  code = mndXnodeSixStarMerge(&input, &output, &sixStarError);
    if (mergeCase.accept) {
      EXPECT_EQ(code, TSDB_CODE_SUCCESS) << "sixStarError=" << SixStarErrorName(sixStarError);
      if (code == TSDB_CODE_SUCCESS) {
        if (mergeCase.isJob) {
          EXPECT_EQ(mergeCase.finalFields.config.omitted, false);
          EXPECT_EQ(output.config != nullptr ? std::string(output.config, output.configLen) : "<null>",
                    mergeCase.finalFields.config.bytes);
          EXPECT_EQ(output.from, nullptr);
          EXPECT_EQ(output.to, nullptr);
        } else {
          EXPECT_EQ(output.from != nullptr ? std::string(output.from, output.fromLen) : "<null>",
                    mergeCase.finalFields.from.bytes);
          EXPECT_EQ(output.to != nullptr ? std::string(output.to, output.toLen) : "<null>",
                    mergeCase.finalFields.to.bytes);
          EXPECT_EQ(output.config, nullptr);
        }
      }
    } else {
      // CONNECTION_CHANGED carries its own SQL-visible code so clients can
      // tell "re-enter the sensitive values" apart from other rejections.
      if (mergeCase.errorId == "SIXSTAR_CONNECTION_CHANGED") {
        EXPECT_EQ(code, TSDB_CODE_MND_XNODE_DSN_CONNECTION_CHANGED);
      } else {
        EXPECT_EQ(code, TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
      }
      EXPECT_EQ(SixStarErrorName(sixStarError), mergeCase.errorId);
      EXPECT_EQ(output.from, nullptr);
      EXPECT_EQ(output.to, nullptr);
      EXPECT_EQ(output.config, nullptr);
    }
    mndXnodeSixStarMergeOutputFree(&output);
  }
}

// ---------------------------------------------------------------------------
// Loader contract negatives (the T8 share of the T1a loader test matrix)
// ---------------------------------------------------------------------------

TEST_F(XnodeSixStarWriteTest, RejectsUnknownSchemaVersion) {
  Fixture     mutated;
  std::string text = MutateFixture(fixtureText, [](cJSON *root) {
    cJSON_ReplaceItemInObject(root, "schema_version", cJSON_CreateNumber(2));
  });
  EXPECT_EQ(LoadSixStarFixture(text, &mutated), "FIXTURE_SCHEMA_UNKNOWN");
}

TEST_F(XnodeSixStarWriteTest, RejectsUnknownFields) {
  Fixture     mutated;
  std::string text = MutateFixture(fixtureText, [](cJSON *root) {
    cJSON_AddStringToObject(root, "unexpected_top_level", "x");
  });
  EXPECT_EQ(LoadSixStarFixture(text, &mutated), "FIXTURE_SCHEMA_INVALID");
}

TEST_F(XnodeSixStarWriteTest, RejectsMissingConditionalFields) {
  Fixture     mutated;
  std::string text = MutateFixture(fixtureText, [](cJSON *root) {
    cJSON *scanCases = cJSON_GetObjectItem(root, "scan_cases");
    cJSON_DeleteItemFromObject(scanCases->child, "expected");
  });
  EXPECT_EQ(LoadSixStarFixture(text, &mutated), "FIXTURE_REQUIRED_FIELD");
}

TEST_F(XnodeSixStarWriteTest, RejectsJsonNull) {
  Fixture     mutated;
  std::string text = MutateFixture(fixtureText, [](cJSON *root) {
    cJSON *scanCases = cJSON_GetObjectItem(root, "scan_cases");
    cJSON_ReplaceItemInObject(scanCases->child, "input_bytes_b64url", cJSON_CreateNull());
  });
  EXPECT_EQ(LoadSixStarFixture(text, &mutated), "FIXTURE_SCHEMA_INVALID");
}

TEST_F(XnodeSixStarWriteTest, RejectsUnknownDecision) {
  Fixture     mutated;
  std::string text = MutateFixture(fixtureText, [](cJSON *root) {
    cJSON *scanCases = cJSON_GetObjectItem(root, "scan_cases");
    cJSON *expected = cJSON_GetObjectItem(scanCases->child, "expected");
    cJSON_ReplaceItemInObject(expected, "decision", cJSON_CreateString("maybe"));
  });
  EXPECT_EQ(LoadSixStarFixture(text, &mutated), "FIXTURE_UNKNOWN_DECISION");
}

TEST_F(XnodeSixStarWriteTest, RejectsThreeStateConflict) {
  Fixture     mutated;
  std::string text = MutateFixture(fixtureText, [](cJSON *root) {
    cJSON *mergeCases = cJSON_GetObjectItem(root, "merge_cases");
    for (cJSON *entry = mergeCases->child; entry != nullptr; entry = entry->next) {
      cJSON *candidate = cJSON_GetObjectItem(entry, "candidate");
      cJSON *config = cJSON_GetObjectItem(candidate, "config");
      if (config != nullptr && IsState(candidate, "config", "omitted")) {
        cJSON_AddStringToObject(config, "bytes_b64url", "eA");
        return;
      }
    }
  });
  EXPECT_EQ(LoadSixStarFixture(text, &mutated), "FIXTURE_REQUIRED_FIELD");
}

TEST_F(XnodeSixStarWriteTest, RejectsResourceFieldMismatch) {
  Fixture     mutated;
  std::string text = MutateFixture(fixtureText, [](cJSON *root) {
    cJSON *mergeCases = cJSON_GetObjectItem(root, "merge_cases");
    for (cJSON *entry = mergeCases->child; entry != nullptr; entry = entry->next) {
      const char *resource = GetStringField(entry, "resource");
      if (resource != nullptr && strcmp(resource, "task") == 0) {
        cJSON *candidate = cJSON_GetObjectItem(entry, "candidate");
        cJSON *config = cJSON_GetObjectItem(candidate, "config");
        cJSON_ReplaceItemInObject(config, "state", cJSON_CreateString("value"));
        cJSON_AddStringToObject(config, "bytes_b64url", "eA");
        return;
      }
    }
  });
  EXPECT_EQ(LoadSixStarFixture(text, &mutated), "FIXTURE_REQUIRED_FIELD");
}

// ---------------------------------------------------------------------------
// mnode-specific regressions (not part of the shared vectors)
// ---------------------------------------------------------------------------

TEST_F(XnodeSixStarWriteTest, KeepsStoredRealSixStarCredentialAsCredential) {
  // A real six-star password committed while the protection was off must keep
  // working as an ordinary credential: KEEP copies it like any other secret.
  SXnodeSixStarMergeInput input = {0};
  std::string             oldFrom = "taos://u:******@from-host/db?token=from-token";
  std::string             oldTo = "taos://u:to-old@to-host/db";
  std::string             candFrom = "taos://u:******@from-host/db?token=from-token";
  std::string             candTo = "taos://u:new-to@new-host/db";
  input.create = false;
  input.protectionEnabled = true;
  input.isJob = false;
  input.oldFrom = {oldFrom.data(), static_cast<int32_t>(oldFrom.size())};
  input.oldTo = {oldTo.data(), static_cast<int32_t>(oldTo.size())};
  input.candidateFrom = {candFrom.data(), static_cast<int32_t>(candFrom.size())};
  input.candidateTo = {candTo.data(), static_cast<int32_t>(candTo.size())};
  input.maxFromLen = TSDB_XNODE_TASK_SOURCE_LEN;
  input.maxToLen = TSDB_XNODE_TASK_SINK_LEN;
  SXnodeSixStarMergeOutput output = {0};
  int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  ASSERT_EQ(mndXnodeSixStarMerge(&input, &output, &sixStarError), TSDB_CODE_SUCCESS);
  EXPECT_EQ(std::string(output.from, output.fromLen), "taos://u:******@from-host/db?token=from-token");
  EXPECT_EQ(std::string(output.to, output.toLen), candTo);
  mndXnodeSixStarMergeOutputFree(&output);

  // The same stored value is still readable: SHOW masking renders it as the
  // six stars it literally is, without failing or treating it as a marker.
  char   *masked = nullptr;
  int32_t maskedLen = 0;
  ASSERT_EQ(mndXnodeMaskDsnForShow(oldFrom.data(), static_cast<int32_t>(oldFrom.size()), &masked, &maskedLen),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(std::string(masked, maskedLen), "taos://u:******@from-host/db?token=******");
  taosMemoryFree(masked);
}

TEST_F(XnodeSixStarWriteTest, KeepsNonSensitiveSixStarUntouched) {
  SXnodeSixStarMergeInput input = {0};
  std::string             oldFrom = "taos://u:from-old@from-host/db";
  std::string             candFrom = "taos://u:brand-new@new-host/db?note=******";
  input.create = false;
  input.protectionEnabled = true;
  input.isJob = false;
  input.oldFrom = {oldFrom.data(), static_cast<int32_t>(oldFrom.size())};
  input.candidateFrom = {candFrom.data(), static_cast<int32_t>(candFrom.size())};
  input.maxFromLen = TSDB_XNODE_TASK_SOURCE_LEN;
  SXnodeSixStarMergeOutput output = {0};
  int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  ASSERT_EQ(mndXnodeSixStarMerge(&input, &output, &sixStarError), TSDB_CODE_SUCCESS);
  EXPECT_EQ(std::string(output.from, output.fromLen), candFrom);
  mndXnodeSixStarMergeOutputFree(&output);
}

TEST_F(XnodeSixStarWriteTest, KeepsTaskSecretWhenViaIsOmitted) {
  SXnodeSixStarMergeInput input = {0};
  std::string             oldFrom = "taos://u:old-password@from-host/db";
  std::string             oldTo = "taos://u:old-password@to-host/db";
  std::string             candidateFrom = "taos://u:******@from-host/db";
  std::string             candidateTo = "taos://u:new-password@to-host/db";
  input.protectionEnabled = true;
  input.oldVia = 7;
  input.oldFrom = {oldFrom.data(), static_cast<int32_t>(oldFrom.size())};
  input.oldTo = {oldTo.data(), static_cast<int32_t>(oldTo.size())};
  input.candidateFrom = {candidateFrom.data(), static_cast<int32_t>(candidateFrom.size())};
  input.candidateTo = {candidateTo.data(), static_cast<int32_t>(candidateTo.size())};
  input.maxFromLen = TSDB_XNODE_TASK_SOURCE_LEN;
  input.maxToLen = TSDB_XNODE_TASK_SINK_LEN;

  SXnodeSixStarMergeOutput output = {0};
  int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  EXPECT_EQ(mndXnodeSixStarMerge(&input, &output, &sixStarError), TSDB_CODE_SUCCESS);
  EXPECT_EQ(std::string(output.from, output.fromLen), oldFrom);
  EXPECT_EQ(std::string(output.to, output.toLen), candidateTo);
  mndXnodeSixStarMergeOutputFree(&output);
}

TEST_F(XnodeSixStarWriteTest, KeepsJobSecretWhenViaIsOmitted) {
  SXnodeSixStarMergeInput input = {0};
  std::string             oldConfig =
      R"({"from":"taos://u:old-password@from-host/db","to":"taos://u:old-password@to-host/db"})";
  std::string candidateConfig =
      R"({"from":"taos://u:******@from-host/db","to":"taos://u:new-password@to-host/db"})";
  std::string expectedConfig =
      R"({"from":"taos://u:old-password@from-host/db","to":"taos://u:new-password@to-host/db"})";
  input.protectionEnabled = true;
  input.isJob = true;
  input.oldVia = 7;
  input.oldConfig = {oldConfig.data(), static_cast<int32_t>(oldConfig.size())};
  input.candidateConfig = {candidateConfig.data(), static_cast<int32_t>(candidateConfig.size())};
  input.maxConfigLen = TSDB_XNODE_TASK_JOB_CONFIG_SHOW_MAX_LEN;

  SXnodeSixStarMergeOutput output = {0};
  int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  EXPECT_EQ(mndXnodeSixStarMerge(&input, &output, &sixStarError), TSDB_CODE_SUCCESS);
  EXPECT_EQ(std::string(output.config, output.configLen), expectedConfig);
  mndXnodeSixStarMergeOutputFree(&output);
}

TEST_F(XnodeSixStarWriteTest, ConnectionChangedMapsToDistinctSqlError) {
  // KEEP while relocating the connection is rejected with
  // TSDB_CODE_MND_XNODE_DSN_CONNECTION_CHANGED (not the generic protection
  // code) so clients can prompt for the sensitive values to be re-entered.
  SXnodeSixStarMergeInput input = {0};
  std::string             oldFrom = "taos://u:old-password@from-host/db";
  std::string             candFrom = "taos://u:******@other-host/db";
  input.protectionEnabled = true;
  input.oldFrom = {oldFrom.data(), static_cast<int32_t>(oldFrom.size())};
  input.candidateFrom = {candFrom.data(), static_cast<int32_t>(candFrom.size())};
  input.maxFromLen = TSDB_XNODE_TASK_SOURCE_LEN;
  SXnodeSixStarMergeOutput output = {0};
  int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  EXPECT_EQ(mndXnodeSixStarMerge(&input, &output, &sixStarError),
            TSDB_CODE_MND_XNODE_DSN_CONNECTION_CHANGED);
  EXPECT_EQ(sixStarError, XNODE_SIXSTAR_ERROR_CONNECTION_CHANGED);
  mndXnodeSixStarMergeOutputFree(&output);

  // An explicit route change under KEEP takes the same distinct code (the
  // connection fields themselves stay identical here).
  std::string keepFrom = "taos://u:******@from-host/db";
  SXnodeSixStarMergeInput viaInput = {0};
  viaInput.protectionEnabled = true;
  viaInput.oldVia = 7;
  viaInput.candidateVia = 9;
  viaInput.oldFrom = {oldFrom.data(), static_cast<int32_t>(oldFrom.size())};
  viaInput.candidateFrom = {keepFrom.data(), static_cast<int32_t>(keepFrom.size())};
  viaInput.maxFromLen = TSDB_XNODE_TASK_SOURCE_LEN;
  SXnodeSixStarMergeOutput viaOutput = {0};
  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  EXPECT_EQ(mndXnodeSixStarMerge(&viaInput, &viaOutput, &sixStarError),
            TSDB_CODE_MND_XNODE_DSN_CONNECTION_CHANGED);
  EXPECT_EQ(sixStarError, XNODE_SIXSTAR_ERROR_CONNECTION_CHANGED);
  mndXnodeSixStarMergeOutputFree(&viaOutput);

  // Other rejection reasons keep the generic protection code.
  SXnodeSixStarMergeInput createInput = {0};
  createInput.create = true;
  createInput.protectionEnabled = true;
  createInput.candidateFrom = {candFrom.data(), static_cast<int32_t>(candFrom.size())};
  createInput.maxFromLen = TSDB_XNODE_TASK_SOURCE_LEN;
  SXnodeSixStarMergeOutput createOutput = {0};
  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  EXPECT_EQ(mndXnodeSixStarMerge(&createInput, &createOutput, &sixStarError),
            TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
  EXPECT_EQ(sixStarError, XNODE_SIXSTAR_ERROR_CREATE_MASKED);
  mndXnodeSixStarMergeOutputFree(&createOutput);
}

TEST_F(XnodeSixStarWriteTest, TreatsPercentEncodedSixStarAsControlMarker) {
  // ALTER: %2A%2A%2A%2A%2A%2A decodes to exactly six stars and KEEPs.
  SXnodeSixStarMergeInput input = {0};
  std::string             oldFrom = "taos://u:real-old@from-host/db";
  std::string             candFrom = "taos://u:%2A%2A%2A%2A%2A%2A@from-host/db";
  input.create = false;
  input.protectionEnabled = true;
  input.oldFrom = {oldFrom.data(), static_cast<int32_t>(oldFrom.size())};
  input.candidateFrom = {candFrom.data(), static_cast<int32_t>(candFrom.size())};
  input.maxFromLen = TSDB_XNODE_TASK_SOURCE_LEN;
  SXnodeSixStarMergeOutput output = {0};
  int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  ASSERT_EQ(mndXnodeSixStarMerge(&input, &output, &sixStarError), TSDB_CODE_SUCCESS);
  EXPECT_EQ(std::string(output.from, output.fromLen), "taos://u:real-old@from-host/db");
  mndXnodeSixStarMergeOutputFree(&output);

  // CREATE: the same encoded marker is rejected instead of being stored.
  SXnodeSixStarMergeInput createInput = {0};
  createInput.create = true;
  createInput.protectionEnabled = true;
  createInput.candidateFrom = {candFrom.data(), static_cast<int32_t>(candFrom.size())};
  createInput.maxFromLen = TSDB_XNODE_TASK_SOURCE_LEN;
  SXnodeSixStarMergeOutput createOutput = {0};
  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  EXPECT_EQ(mndXnodeSixStarMerge(&createInput, &createOutput, &sixStarError),
            TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
  EXPECT_EQ(sixStarError, XNODE_SIXSTAR_ERROR_CREATE_MASKED);
  mndXnodeSixStarMergeOutputFree(&createOutput);
}

TEST_F(XnodeSixStarWriteTest, EnforcesJobConfigOutputBufferBoundary) {
  // The merged Job config projection must succeed at exactly 2 MiB and fail
  // closed one byte above it; this boundary is an mnode business limit and is
  // intentionally not a shared fixture merge case.
  const size_t kMax = TSDB_XNODE_TASK_JOB_CONFIG_SHOW_MAX_LEN;
  const size_t passwordLen = 1000000;
  std::string  password(passwordLen, 'a');
  std::string  oldConfig = "{\"from\":\"taos://u:" + password + "@old/db\",\"to\":\"taos://u:x@h2/db\"}";
  std::string  prefix = "{\"from\":\"taos://u:******@old/db\",\"to\":\"taos://u:y@h2/db\",\"pad\":\"";
  std::string  suffix = "\"}";
  // final = prefix + pad + suffix with "******" replaced by password
  size_t       padLen = kMax - (prefix.size() + suffix.size() - 6 + passwordLen);
  SXnodeSixStarMergeInput input = {0};
  input.create = false;
  input.protectionEnabled = true;
  input.isJob = true;
  input.oldConfig = {oldConfig.data(), static_cast<int32_t>(oldConfig.size())};
  input.maxConfigLen = TSDB_XNODE_TASK_JOB_CONFIG_SHOW_MAX_LEN;
  for (size_t extra = 0; extra <= 1; ++extra) {
    std::string candidate = prefix + std::string(padLen + extra, 'b') + suffix;
    input.candidateConfig = {candidate.data(), static_cast<int32_t>(candidate.size())};
    SXnodeSixStarMergeOutput output = {0};
    int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
    int32_t                  code = mndXnodeSixStarMerge(&input, &output, &sixStarError);
    if (extra == 0) {
      EXPECT_EQ(code, TSDB_CODE_SUCCESS);
      EXPECT_EQ(output.configLen, static_cast<int32_t>(kMax));
    } else {
      EXPECT_EQ(code, TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
      EXPECT_EQ(sixStarError, XNODE_SIXSTAR_ERROR_NONE);
    }
    mndXnodeSixStarMergeOutputFree(&output);
  }
}

TEST_F(XnodeSixStarWriteTest, EnforcesTaskDsnLengthBoundaryAfterKeep) {
  SXnodeSixStarMergeInput input = {0};
  std::string             password(4000, 'a');
  std::string             oldFrom = "taos://u:" + password + "@old-host/db";
  std::string             candFrom = "taos://u:******@old-host/db?note=" + std::string(200, 'h');
  input.create = false;
  input.protectionEnabled = true;
  input.isJob = false;
  input.oldFrom = {oldFrom.data(), static_cast<int32_t>(oldFrom.size())};
  input.candidateFrom = {candFrom.data(), static_cast<int32_t>(candFrom.size())};
  input.maxFromLen = TSDB_XNODE_TASK_SOURCE_LEN;
  SXnodeSixStarMergeOutput output = {0};
  int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  EXPECT_EQ(mndXnodeSixStarMerge(&input, &output, &sixStarError), TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
  EXPECT_EQ(sixStarError, XNODE_SIXSTAR_ERROR_NONE);
  mndXnodeSixStarMergeOutputFree(&output);
}

TEST_F(XnodeSixStarWriteTest, AcceptsDsnsWithoutLocatableUserinfoPassword) {
  // Empty-authority csv/orc sources, empty userinfo, empty host and extra '@'
  // carry no locatable userinfo password, so they merge like any other input
  // without a six-star marker (last-'@' matches the connector DSN parser);
  // structural validity is the connector's call, not the scanner's.
  const char *accepted[] = {"csv:///tmp/demo.csv", "orc:///tmp/demo.orc", "taos://@host/db", "taos://u:p@/db",
                            "taos://u:p@h@other/db"};
  for (const char *dsn : accepted) {
    SXnodeSixStarMergeInput input = {0};
    input.create = true;
    input.protectionEnabled = true;
    input.isJob = false;
    input.candidateFrom = {dsn, static_cast<int32_t>(strlen(dsn))};
    input.maxFromLen = TSDB_XNODE_TASK_SOURCE_LEN;
    SXnodeSixStarMergeOutput output = {0};
    int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
    ASSERT_EQ(mndXnodeSixStarMerge(&input, &output, &sixStarError), TSDB_CODE_SUCCESS) << dsn;
    EXPECT_EQ(std::string(output.from, output.fromLen), dsn) << dsn;
    mndXnodeSixStarMergeOutputFree(&output);
  }

  // A sensitive query parameter is still located in an empty-authority DSN and
  // CREATE still rejects a six-star marker there.
  SXnodeSixStarMergeInput input = {0};
  std::string             candFrom = "csv:///tmp/demo.csv?password=******";
  input.create = true;
  input.protectionEnabled = true;
  input.isJob = false;
  input.candidateFrom = {candFrom.data(), static_cast<int32_t>(candFrom.size())};
  input.maxFromLen = TSDB_XNODE_TASK_SOURCE_LEN;
  SXnodeSixStarMergeOutput output = {0};
  int32_t                  sixStarError = XNODE_SIXSTAR_ERROR_NONE;
  EXPECT_EQ(mndXnodeSixStarMerge(&input, &output, &sixStarError), TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
  EXPECT_EQ(sixStarError, XNODE_SIXSTAR_ERROR_CREATE_MASKED);
  mndXnodeSixStarMergeOutputFree(&output);
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
