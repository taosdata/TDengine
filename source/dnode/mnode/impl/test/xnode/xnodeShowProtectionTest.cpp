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
 * @file xnodeShowProtectionTest.cpp
 * @brief XNode SHOW DSN protection tests: output mode resolution, DSN masking,
 *        job config projection, and envelope batch lifecycle
 * @version 1.0
 * @date 2026-09-15
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>

#ifndef WINDOWS
#include <unistd.h>
#endif

extern "C" {
#include "mndDef.h"
#include "mndXnode.h"
#include "osMemory.h"
#include "taoserror.h"
#include "tdef.h"
#ifndef WINDOWS
#include "os.h"
#include "tconfig.h"
#include "tglobal.h"
#endif
}

#ifndef WINDOWS
#undef close
#endif

namespace {

constexpr int8_t kPlaintext = 0;
constexpr int8_t kMasked = 1;
constexpr int8_t kEncrypted = 2;

class XnodeShowProtectionTest : public ::testing::Test {
 protected:
  static std::string Take(char *value, int32_t length) {
    std::string result(value, static_cast<size_t>(length));
    taosMemoryFree(value);
    return result;
  }
};

TEST_F(XnodeShowProtectionTest, ResolvesConfiguredOutputMode) {
  int8_t mode = -1;
  ASSERT_EQ(mndXnodeResolveShowMode(0, XNODE_DSN_OUTPUT_DEFAULT, &mode), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mode, kPlaintext);
  // With protection disabled, even an explicit WITH ENCRYPTED request is
  // intentionally downgraded to plaintext; pin that documented behavior.
  ASSERT_EQ(mndXnodeResolveShowMode(0, XNODE_DSN_OUTPUT_ENCRYPTED, &mode), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mode, kPlaintext);
#ifdef WINDOWS
  EXPECT_EQ(mndXnodeResolveShowMode(1, XNODE_DSN_OUTPUT_DEFAULT, &mode), TSDB_CODE_NOT_SUPPORTED_IN_WINDOWS);
  EXPECT_EQ(mndXnodeResolveShowMode(1, XNODE_DSN_OUTPUT_ENCRYPTED, &mode), TSDB_CODE_NOT_SUPPORTED_IN_WINDOWS);
#else
  ASSERT_EQ(mndXnodeResolveShowMode(1, XNODE_DSN_OUTPUT_DEFAULT, &mode), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mode, kMasked);
  ASSERT_EQ(mndXnodeResolveShowMode(1, XNODE_DSN_OUTPUT_ENCRYPTED, &mode), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mode, kEncrypted);
#endif
  EXPECT_EQ(mndXnodeResolveShowMode(1, 9, &mode), TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
}

TEST_F(XnodeShowProtectionTest, MatchesOnlyCachedTypedFilter) {
  EXPECT_TRUE(mndXnodeShowFilterMatches(XNODE_RETRIEVE_FILTER_NONE, 0, 7, 11));
  EXPECT_TRUE(mndXnodeShowFilterMatches(XNODE_RETRIEVE_FILTER_ID_EQ, 7, 7, 11));
  EXPECT_FALSE(mndXnodeShowFilterMatches(XNODE_RETRIEVE_FILTER_ID_EQ, 11, 7, 11));
  EXPECT_TRUE(mndXnodeShowFilterMatches(XNODE_RETRIEVE_FILTER_TASK_ID_EQ, 11, 7, 11));
  EXPECT_FALSE(mndXnodeShowFilterMatches(XNODE_RETRIEVE_FILTER_TASK_ID_EQ, 7, 7, 11));
}

TEST_F(XnodeShowProtectionTest, MasksStructuredDsnValuesWithoutReformatting) {
  const std::string input = "taos://alice:secret%2Fvalue@example/db?foo=a%2Fb&password=first&token=&x=1";
  char *masked = nullptr;
  int32_t maskedLen = 0;
  ASSERT_EQ(mndXnodeMaskDsnForShow(input.data(), static_cast<int32_t>(input.size()), &masked, &maskedLen),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(Take(masked, maskedLen),
            "taos://alice:******@example/db?foo=a%2Fb&password=******&token=******&x=1");
}

TEST_F(XnodeShowProtectionTest, MasksSensitiveValueWithInvalidEscapeWholesale) {
  // Sensitive values are replaced as a whole raw span without decoding, so a bare
  // '%' inside a sensitive value cannot fail the masking path.
  const std::string input = "taos://alice:secret@example/db?password=%2";
  char *masked = nullptr;
  int32_t maskedLen = 0;
  ASSERT_EQ(mndXnodeMaskDsnForShow(input.data(), static_cast<int32_t>(input.size()), &masked, &maskedLen),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(Take(masked, maskedLen), "taos://alice:******@example/db?password=******");
}

TEST_F(XnodeShowProtectionTest, MasksEverySensitiveOccurrenceInQuery) {
  const std::string input = "taos://u:p:part@[::1]/db?PASSWORD=one&pass_word=two&x=a+b&token=&%70assword=three";
  char *masked = nullptr;
  int32_t maskedLen = 0;
  ASSERT_EQ(mndXnodeMaskDsnForShow(input.data(), static_cast<int32_t>(input.size()), &masked, &maskedLen),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(Take(masked, maskedLen),
            "taos://u:******@[::1]/db?PASSWORD=******&pass_word=******&x=a+b&token=******&%70assword=******");
}

TEST_F(XnodeShowProtectionTest, StartsQueryAtLastQuestionMarkOfUrlDsn) {
  // A URL-form DSN (with "://") starts its query at the LAST '?', matching the
  // connector DSN parser; ';' and '#' are ordinary bytes, so the text between
  // the authority and the last '?' is subject/path and passes through verbatim.
  const std::string input = "taos://u:p:part@[::1]/db?PASSWORD=one;pass_word=two&x=a+b&token=&%70assword=three#frag?token=visible";
  char *masked = nullptr;
  int32_t maskedLen = 0;
  ASSERT_EQ(mndXnodeMaskDsnForShow(input.data(), static_cast<int32_t>(input.size()), &masked, &maskedLen),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(Take(masked, maskedLen),
            "taos://u:******@[::1]/db?PASSWORD=one;pass_word=two&x=a+b&token=&%70assword=three#frag?token=******");
}

TEST_F(XnodeShowProtectionTest, TreatsSemicolonHashAndExtraQuestionMarksAsData) {
  const struct {
    const char *input;
    const char *expected;
  } cases[] = {
      // ';' inside a sensitive value is masked together with the value; no tail
      // leaks past the mask.
      {"kafka://broker:9092?sasl_password=p1;p2", "kafka://broker:9092?sasl_password=******"},
      // ';' does not start a new parameter: no phantom password param exists.
      {"taos://alice@example/db?a=1;password=x", "taos://alice@example/db?a=1;password=x"},
      // '#' is an ordinary value byte, not a fragment boundary.
      {"taos://u:p@h/db?token=ab#cd", "taos://u:******@h/db?token=******"},
      {"?token=ab#cd", "?token=******"},
      {"taos://u:p@h/db?token=ab#x&x=1", "taos://u:******@h/db?token=******&x=1"},
      // URL form cuts the query at the last '?'; a path-like DSN cuts it at the
      // first '?'.
      {"taos://h/db?a=1?token=x", "taos://h/db?a=1?token=******"},
      {"csv:/data/x.csv?token=a?b=1", "csv:/data/x.csv?token=******"},
  };
  for (const auto &c : cases) {
    char *masked = nullptr;
    int32_t maskedLen = 0;
    ASSERT_EQ(mndXnodeMaskDsnForShow(c.input, static_cast<int32_t>(strlen(c.input)), &masked, &maskedLen),
              TSDB_CODE_SUCCESS)
        << c.input;
    EXPECT_EQ(Take(masked, maskedLen), c.expected) << c.input;
  }
}

TEST_F(XnodeShowProtectionTest, PassesThroughDsnsWithoutLocatablePassword) {
  // Authority shape alone never fails closed, matching the write path: an empty
  // authority or an empty userinfo carries no password slot and passes through
  // verbatim, while an empty host does not exclude the slot located by the last
  // '@'. Query scanning still applies to pass-through DSNs.
  const struct {
    const char *input;
    const char *expected;
  } cases[] = {
      {"taos://", "taos://"},
      {"taos:///db", "taos:///db"},
      {"taos://@host/db", "taos://@host/db"},
      {"taos://u:p@/db", "taos://u:******@/db"},
      {"tmq+ws:///db?group.id=g&token=t", "tmq+ws:///db?group.id=g&token=******"},
  };
  for (const auto &c : cases) {
    char *masked = nullptr;
    int32_t maskedLen = 0;
    ASSERT_EQ(mndXnodeMaskDsnForShow(c.input, static_cast<int32_t>(strlen(c.input)), &masked, &maskedLen),
              TSDB_CODE_SUCCESS)
        << c.input;
    EXPECT_EQ(Take(masked, maskedLen), c.expected) << c.input;
  }
}

TEST_F(XnodeShowProtectionTest, MasksUserinfoSplitAtLastAtSign) {
  // Userinfo may itself contain '@' (e.g. passwords like "Mm@12345"); like the
  // taosx DSN parser, the last '@' is the userinfo/host separator.
  const struct {
    const char *input;
    const char *expected;
  } cases[] = {
      {"taos://u:p@h@other/db", "taos://u:******@other/db"},
      {"taos://u:p@@h/db", "taos://u:******@h/db"},
      {"mqtt://sensor:Mm@12345@broker.local:1883?client_id=pub-01&qos=1",
       "mqtt://sensor:******@broker.local:1883?client_id=pub-01&qos=1"},
  };
  for (const auto &c : cases) {
    char *masked = nullptr;
    int32_t maskedLen = 0;
    ASSERT_EQ(mndXnodeMaskDsnForShow(c.input, static_cast<int32_t>(strlen(c.input)), &masked, &maskedLen),
              TSDB_CODE_SUCCESS)
        << c.input;
    EXPECT_EQ(Take(masked, maskedLen), c.expected) << c.input;
  }
}

TEST_F(XnodeShowProtectionTest, PreservesBarePercentOutsideDecodedKeys) {
  // Only query keys are decoded to decide sensitivity; a literal '%' anywhere
  // else (non-sensitive values like busy_threshold=100%, userinfo passwords) is
  // ordinary text and must pass through verbatim. A bare '%' inside a sensitive
  // value is masked away with the value, even when it follows a literal '#'.
  const struct {
    const char *input;
    const char *expected;
  } cases[] = {
      {"tmq+ws://127.0.0.1:6041/test?auto.offset.reset=earliest&busy_threshold=100%&client.id=asdf&password=p",
       "tmq+ws://127.0.0.1:6041/test?auto.offset.reset=earliest&busy_threshold=100%&client.id=asdf&password=******"},
      {"taos://u:pa%ss@h/db", "taos://u:******@h/db"},
      {"taos://u:p@h/db?x=%2", "taos://u:******@h/db?x=%2"},
      {"taos://u:p@h/db?token=abc#f%", "taos://u:******@h/db?token=******"},
  };
  for (const auto &c : cases) {
    char *masked = nullptr;
    int32_t maskedLen = 0;
    ASSERT_EQ(mndXnodeMaskDsnForShow(c.input, static_cast<int32_t>(strlen(c.input)), &masked, &maskedLen),
              TSDB_CODE_SUCCESS)
        << c.input;
    EXPECT_EQ(Take(masked, maskedLen), c.expected) << c.input;
  }
}

TEST_F(XnodeShowProtectionTest, PassesThroughValueWhenQueryKeyHasInvalidEncoding) {
  // Under lenient decoding an invalid '%' sequence stays as literal bytes and
  // the key is classified as-is: the literal '%' keeps `pa%ssword` from
  // matching the password rule, so a non-sensitive key leaves its value
  // untouched instead of masking it conservatively.
  const struct {
    const char *input;
    const char *expected;
  } cases[] = {
      {"taos://h/db?pa%ssword=x", "taos://h/db?pa%ssword=x"},
      {"tmq+ws://127.0.0.1:6041/test?100%=1&password=p",
       "tmq+ws://127.0.0.1:6041/test?100%=1&password=******"},
  };
  for (const auto &c : cases) {
    char *masked = nullptr;
    int32_t maskedLen = 0;
    ASSERT_EQ(mndXnodeMaskDsnForShow(c.input, static_cast<int32_t>(strlen(c.input)), &masked, &maskedLen),
              TSDB_CODE_SUCCESS)
        << c.input;
    EXPECT_EQ(Take(masked, maskedLen), c.expected) << c.input;
  }
}

TEST_F(XnodeShowProtectionTest, DoesNotTreatQueryTextAsAuthority) {
  const std::string input = "driver?note=taos://u:p@h&password=secret";
  char *masked = nullptr;
  int32_t maskedLen = 0;
  ASSERT_EQ(mndXnodeMaskDsnForShow(input.data(), static_cast<int32_t>(input.size()), &masked, &maskedLen),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(Take(masked, maskedLen), "driver?note=taos://u:p@h&password=******");
}

TEST_F(XnodeShowProtectionTest, MasksLongSensitiveKeysAndRejectsInvalidUtf8) {
  const std::string longKey = std::string(300, 'x') + "password";
  const std::string input = "taos://host/db?" + longKey + "=secret";
  char *masked = nullptr;
  int32_t maskedLen = 0;
  ASSERT_EQ(mndXnodeMaskDsnForShow(input.data(), static_cast<int32_t>(input.size()), &masked, &maskedLen),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(Take(masked, maskedLen), "taos://host/db?" + longKey + "=******");

  std::string invalidUtf8 = "taos://host/db?password=";
  invalidUtf8.push_back(static_cast<char>(0xc0));
  invalidUtf8.push_back(static_cast<char>(0x80));
  masked = nullptr;
  maskedLen = 0;
  EXPECT_EQ(mndXnodeMaskDsnForShow(invalidUtf8.data(), static_cast<int32_t>(invalidUtf8.size()), &masked, &maskedLen),
            TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
  EXPECT_EQ(masked, nullptr);
}

TEST_F(XnodeShowProtectionTest, SplicesMaskedJobConfigInPlace) {
  const std::string input =
      "{ \"via\":null, \"to\":\"taos://sink:p@host/db?secret=s\", \"n\":1.00, \"from\":\"taos://u:p@host/db?x=1\" }";
  char *projected = nullptr;
  int32_t projectedLen = 0;
  SShowObj show = {0};
  show.xnodeOutputMode = kMasked;
  ASSERT_EQ(mndXnodeProjectJobConfigForShow(&show, input.data(), static_cast<int32_t>(input.size()), &projected,
                                            &projectedLen),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(Take(projected, projectedLen),
            "{ \"via\":null, \"to\":\"taos://sink:******@host/db?secret=******\", \"n\":1.00, \"from\":\"taos://u:******@host/db?x=1\" }");
}

TEST_F(XnodeShowProtectionTest, ProtectedJobConfigFailsClosed) {
  const std::string malformed = "{\"from\":\"taos://u:p@h\",\"from\":\"taos://u:q@h\"}";
  char *projected = nullptr;
  int32_t projectedLen = 0;
  SShowObj show = {0};
  show.xnodeOutputMode = kMasked;
  EXPECT_EQ(mndXnodeProjectJobConfigForShow(&show, malformed.data(), static_cast<int32_t>(malformed.size()),
                                            &projected, &projectedLen),
            TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
  EXPECT_EQ(projected, nullptr);
  EXPECT_EQ(projectedLen, 0);
}

TEST_F(XnodeShowProtectionTest, JobConfigKeepsUnchangedBytesAndRejectsUnsafeShapes) {
  const std::string input = "{\"z\":1.00,\"to\":\"taos://u:p@h\",\"nested\":{\"from\":\"taos://u:q@h\"},\"via\":null}";
  char *projected = nullptr;
  int32_t projectedLen = 0;
  SShowObj show = {0};
  show.xnodeOutputMode = kMasked;
  ASSERT_EQ(mndXnodeProjectJobConfigForShow(&show, input.data(), static_cast<int32_t>(input.size()), &projected,
                                            &projectedLen),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(Take(projected, projectedLen),
            "{\"z\":1.00,\"to\":\"taos://u:******@h\",\"nested\":{\"from\":\"taos://u:q@h\"},\"via\":null}");

  const std::string unsafe[] = {
      "[]",
      "{\"from\":1}",
      "{\"from\":\"taos://u:p@h\",\"from\":\"taos://u:q@h\"}",
      "{\"fr\\u006fm\":\"taos://u:p@h\"}",
  };
  for (const std::string &value : unsafe) {
    projected = nullptr;
    projectedLen = 0;
    EXPECT_EQ(mndXnodeProjectJobConfigForShow(&show, value.data(), static_cast<int32_t>(value.size()), &projected,
                                              &projectedLen),
              TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED)
        << value;
    EXPECT_EQ(projected, nullptr);
    EXPECT_EQ(projectedLen, 0);
  }
}

TEST_F(XnodeShowProtectionTest, JobConfigRejectsDecodedNulAndAllowsMissingDirections) {
  const std::string nul = "{\"from\":\"taos://u:p@h\\u0000\"}";
  const std::string nulInOtherField = "{\"note\":\"x\\u0000\"}";
  const std::string missing = "{ \"via\": null, \"n\": 1.00 }";
  SShowObj show = {0};
  show.xnodeOutputMode = kMasked;
  char *projected = nullptr;
  int32_t projectedLen = 0;
  EXPECT_EQ(mndXnodeProjectJobConfigForShow(&show, nul.data(), static_cast<int32_t>(nul.size()), &projected,
                                            &projectedLen),
            TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
  EXPECT_EQ(projected, nullptr);
  EXPECT_EQ(mndXnodeProjectJobConfigForShow(&show, nulInOtherField.data(), static_cast<int32_t>(nulInOtherField.size()),
                                            &projected, &projectedLen),
            TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
  EXPECT_EQ(projected, nullptr);
  ASSERT_EQ(mndXnodeProjectJobConfigForShow(&show, missing.data(), static_cast<int32_t>(missing.size()), &projected,
                                            &projectedLen),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(Take(projected, projectedLen), missing);

  const char empty[] = "";
  projected = nullptr;
  projectedLen = -1;
  EXPECT_EQ(mndXnodeProjectJobConfigForShow(&show, empty, sizeof(empty), &projected, &projectedLen),
            TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
  EXPECT_EQ(projected, nullptr);

  projected = nullptr;
  projectedLen = -1;
  EXPECT_EQ(mndXnodeProjectJobConfigForShow(&show, empty, 0, &projected, &projectedLen),
            TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
  EXPECT_EQ(projected, nullptr);

  show.xnodeOutputMode = kEncrypted;
  projected = nullptr;
  projectedLen = -1;
  EXPECT_EQ(mndXnodeProjectJobConfigForShow(&show, empty, sizeof(empty), &projected, &projectedLen),
            TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
  EXPECT_EQ(projected, nullptr);
}

TEST_F(XnodeShowProtectionTest, PlaintextJobConfigPreservesNonJsonBlob) {
  const std::string input("not-json\0blob", 13);
  char *projected = nullptr;
  int32_t projectedLen = 0;
  SShowObj show = {0};
  show.xnodeOutputMode = kPlaintext;
  ASSERT_EQ(mndXnodeProjectJobConfigForShow(&show, input.data(), static_cast<int32_t>(input.size()), &projected,
                                            &projectedLen),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(Take(projected, projectedLen), input);

  const char empty[] = "";
  projected = nullptr;
  projectedLen = -1;
  ASSERT_EQ(mndXnodeProjectJobConfigForShow(&show, empty, sizeof(empty), &projected, &projectedLen),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(projectedLen, 0);
  EXPECT_EQ(Take(projected, projectedLen), "");

  projected = nullptr;
  projectedLen = -1;
  ASSERT_EQ(mndXnodeProjectJobConfigForShow(&show, empty, 0, &projected, &projectedLen), TSDB_CODE_SUCCESS);
  EXPECT_EQ(projectedLen, 0);
  EXPECT_EQ(Take(projected, projectedLen), "");
}

TEST_F(XnodeShowProtectionTest, RejectsUnknownProjectionMode) {
  const std::string input = "{\"from\":\"taos://u:p@h\"}";
  SShowObj show = {0};
  show.xnodeOutputMode = 9;
  char *projected = nullptr;
  int32_t projectedLen = 0;
  EXPECT_EQ(mndXnodeProjectJobConfigForShow(&show, input.data(), static_cast<int32_t>(input.size()), &projected,
                                            &projectedLen),
            TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED);
  EXPECT_EQ(projected, nullptr);
  EXPECT_EQ(projectedLen, 0);
}

TEST_F(XnodeShowProtectionTest, FinishShowProtectionWithoutBatchIsNoOpSuccess) {
  // Pages without encrypted fields (empty pages, fully filtered pages) carry no
  // batch; per-page finish must be a no-op success and cleanup must stay safe.
  SShowObj show = {0};
  EXPECT_EQ(show.pXnodeBatch, nullptr);
  EXPECT_EQ(mndXnodeFinishShowProtection(&show), TSDB_CODE_SUCCESS);
  EXPECT_EQ(show.pXnodeBatch, nullptr);
  mndXnodeCleanupShowProtection(&show);
  EXPECT_EQ(show.pXnodeBatch, nullptr);
}

TEST_F(XnodeShowProtectionTest, ShowSchemaLeavesRoomForProtectedValues) {
  EXPECT_EQ(TSDB_XNODE_TASK_JOB_CONFIG_SHOW_MAX_LEN, 2 * 1024 * 1024);
}

TEST_F(XnodeShowProtectionTest, EmptyQueryAndHashBytesPassThrough) {
  // Regression: an empty query ("?") must skip the query rewrite branch --
  // entering it would never advance the write position and spin forever. '#'
  // is an ordinary data byte, not a fragment boundary, so a sensitive value
  // containing it is masked wholesale.
  const struct {
    const char *input;
    const char *expected;
  } cases[] = {
      {"taos://h/db?#frag", "taos://h/db?#frag"},
      {"taos://h/db?#", "taos://h/db?#"},
      {"taos://h/db?", "taos://h/db?"},
      {"a?#b", "a?#b"},
      {"taos://u:p@h/db?#frag", "taos://u:******@h/db?#frag"},
      {"taos://h/db?password=x#frag", "taos://h/db?password=******"},
  };
  for (const auto &c : cases) {
    char *masked = nullptr;
    int32_t maskedLen = 0;
    ASSERT_EQ(mndXnodeMaskDsnForShow(c.input, static_cast<int32_t>(strlen(c.input)), &masked, &maskedLen),
              TSDB_CODE_SUCCESS)
        << c.input;
    EXPECT_EQ(Take(masked, maskedLen), c.expected) << c.input;
  }
}

#ifndef WINDOWS
// The e2e test below mutates process-global config state and creates a temp key
// file mid-test; an ASSERT_* failure returns early, so cleanup hangs off these
// scope guards instead of the happy path. Declare CfgStateGuard after
// TempKeyFileGuard so config teardown runs before the file is removed.
struct TempKeyFileGuard {
  char path[64] = {0};
  ~TempKeyFileGuard() {
    if (path[0] != 0) (void)unlink(path);
  }
};

struct CfgStateGuard {
  SArray *args = nullptr;
  ~CfgStateGuard() {
    if (args != nullptr) taosArrayDestroy(args);
    taosCleanupCfg();
  }
};

// End-to-end coverage for the ENCRYPTED output mode with a real RSA public key:
// from/to values are envelope-encrypted through the same helper the retrieve
// path uses, and the per-page batch must finish before delivery.
TEST_F(XnodeShowProtectionTest, EncryptsJobConfigDsnValuesWithRealKey) {
  static const char kPublicKeyPem[] =
      "-----BEGIN PUBLIC KEY-----\n"
      "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAs8qSvvNIiSkYqTFJ+s/R\n"
      "m2tMKilFWV8CiZ7iZ4S4+v/77zFpQNHm0zLwOK8hbf0RRP3mdQhPSINdKTl4lbmi\n"
      "pVL86oVQzIuiBw+xpeYfufOSsuI59xq71l3om5iliWOxRsxZkCK7E1h12mLfjYWz\n"
      "x5R2OEVySN8/1pBIIMvAYPIC8p6iyExYSw6dNTjFdKWX4eAdZkX0c9aCZXlC1fcJ\n"
      "SgNSyCV1wWdtdcYV1v0VuxzDkPGGudrHunoDXO6Fe2OdOqTe34XPSk1DnhZuSIVl\n"
      "fZkhNTV3ezdoAgUBxfwjRjh94llBRjY+9mN78sdiU6hnZQUkXjmHzVPAVa4B7qmv\n"
      "CQIDAQAB\n"
      "-----END PUBLIC KEY-----\n";
  TempKeyFileGuard keyFile;
  memcpy(keyFile.path, "/tmp/taos-xnode-show-pub-XXXXXX", sizeof("/tmp/taos-xnode-show-pub-XXXXXX"));
  const int fd = mkstemp(keyFile.path);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(write(fd, kPublicKeyPem, sizeof(kPublicKeyPem) - 1),
            static_cast<ssize_t>(sizeof(kPublicKeyPem) - 1));
  ASSERT_EQ(close(fd), 0);

  // The server config defaults derive from detected system memory, so the OS
  // layer must be initialized before loading the config framework.
  ASSERT_EQ(osDefaultInit(), TSDB_CODE_SUCCESS);
  taosCleanupCfg();
  CfgStateGuard cfgGuard;
  cfgGuard.args = taosArrayInit(1, sizeof(SConfigPair));
  ASSERT_NE(cfgGuard.args, nullptr);
  SConfigPair dataDir = {.name = "dataDir", .value = TD_TMP_DIR_PATH};
  ASSERT_NE(taosArrayPush(cfgGuard.args, &dataDir), nullptr);
  ASSERT_EQ(taosPreLoadCfg("", nullptr, nullptr, nullptr, cfgGuard.args, false), TSDB_CODE_SUCCESS);
  SConfig *config = taosGetCfg();
  ASSERT_NE(config, nullptr);
  ASSERT_EQ(cfgSetItem(config, "xnodeDsnProtection", "1", CFG_STYPE_CFG_FILE, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(cfgSetItem(config, "xnodeDsnPublicKeyFile", keyFile.path, CFG_STYPE_CFG_FILE, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosApplyCfg("", nullptr, nullptr, nullptr, cfgGuard.args, false), TSDB_CODE_SUCCESS);
  ASSERT_NE(taosGetXnodeDsnPublicKey(), nullptr);

  const std::string input = "{\"from\":\"taos://user:secret-pw@host/db?token=abc\",\"to\":\"taos://sink@h2/db\"}";
  SShowObj show = {0};
  show.xnodeOutputMode = kEncrypted;
  char *projected = nullptr;
  int32_t projectedLen = 0;
  ASSERT_EQ(mndXnodeProjectJobConfigForShow(&show, input.data(), static_cast<int32_t>(input.size()), &projected,
                                          &projectedLen),
            TSDB_CODE_SUCCESS);
  EXPECT_NE(show.pXnodeBatch, nullptr);
  ASSERT_EQ(mndXnodeFinishShowProtection(&show), TSDB_CODE_SUCCESS);
  EXPECT_EQ(show.pXnodeBatch, nullptr);
  const std::string output = Take(projected, projectedLen);
  EXPECT_NE(output, input);
  EXPECT_EQ(output.find("secret-pw"), std::string::npos);
  EXPECT_EQ(output.find("token=abc"), std::string::npos);
  EXPECT_NE(output.find("\"from\":\""), std::string::npos);
  EXPECT_NE(output.find("\"to\":\""), std::string::npos);
  mndXnodeCleanupShowProtection(&show);

  ASSERT_EQ(unlink(keyFile.path), 0);
  keyFile.path[0] = 0;  // disarm on the happy path; the guard covers early returns
}
#endif

}  // namespace
