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

// RFC 6238 TOTP vectors.
//
// The enterprise build carries its own SHA-1/SHA-256/HMAC implementation instead
// of OpenSSL (Windows does not link libcrypto), so nothing else in the build
// would catch a bug in that code.  These vectors pin it to the published
// values: a secret produced on one platform has to verify on another.

#include <gtest/gtest.h>
#include <stdint.h>
#include <string.h>

#include "totp.h"

namespace {

// RFC 6238 Appendix B, SHA-1 seed.  The published vectors use the ASCII bytes
// "12345678901234567890" as the HMAC key, which is 20 bytes - above the 16 byte
// minimum the implementation requires.
const char kRfcSecret[] = "12345678901234567890";

struct RfcVector {
  uint64_t unixTime;  // seconds
  uint32_t expected;  // 8 digit code
};

// RFC 6238 Appendix B lists these for the 20-byte SHA-1 secret.
const RfcVector kRfcVectors[] = {
    {59ULL, 94287082U},          {1111111109ULL, 7081804U},   {1111111111ULL, 14050471U},
    {1234567890ULL, 89005924U},  {2000000000ULL, 69279037U},  {20000000000ULL, 65353130U},
};

}  // namespace

TEST(totpTests, matchesRfc6238Vectors) {
  const uint8_t *secret = (const uint8_t *)kRfcSecret;
  const size_t   secretLen = sizeof(kRfcSecret) - 1;

  for (const RfcVector &v : kRfcVectors) {
    const uint64_t step = v.unixTime / 30;
    EXPECT_EQ(taosGenerateTotpCodeAt(secret, secretLen, step, 8), (int32_t)v.expected)
        << "T=" << v.unixTime << " (time step " << step << ")";
  }
}

TEST(totpTests, formatsWithLeadingZeros) {
  char buf[16] = {0};

  // 7081804 must be rendered as 07081804 for an 8 digit code.
  EXPECT_EQ(taosFormatTotp(7081804, 8, buf, sizeof(buf)), 8);
  EXPECT_STREQ(buf, "07081804");

  EXPECT_EQ(taosFormatTotp(94287082, 8, buf, sizeof(buf)), 8);
  EXPECT_STREQ(buf, "94287082");

  EXPECT_EQ(taosFormatTotp(123456, 6, buf, sizeof(buf)), 6);
  EXPECT_STREQ(buf, "123456");
}

TEST(totpTests, rejectsUnusableArguments) {
  const uint8_t *secret = (const uint8_t *)kRfcSecret;
  const size_t   secretLen = sizeof(kRfcSecret) - 1;
  const uint8_t  shortSecret[8] = {0};

  // A truncated secret cannot carry the entropy TOTP needs.
  EXPECT_EQ(taosGenerateTotpCodeAt(shortSecret, sizeof(shortSecret), 1, 8), -1);

  // mod is accumulated in int32_t: 10 digits would overflow it.
  EXPECT_EQ(taosGenerateTotpCodeAt(secret, secretLen, 1, 10), -1);
  EXPECT_EQ(taosGenerateTotpCodeAt(secret, secretLen, 1, 0), -1);
  EXPECT_EQ(taosGenerateTotpCodeAt(NULL, secretLen, 1, 8), -1);
}

TEST(totpTests, secretDerivationIsStable) {
  uint8_t secret[32] = {0};

  // HMAC-SHA256 with an empty key over the seed; the expected bytes were
  // produced by an independent implementation (Python's hmac/hashlib) so this
  // pins the hand written SHA-256 as well.
  const uint8_t expected[] = {0x2c, 0xa2, 0x71, 0x71, 0xa4, 0x6d, 0x21, 0x6a,
                              0xac, 0x57, 0x9e, 0x38, 0x99, 0x9a, 0x39, 0x54,
                              0x86, 0x92, 0x90, 0xf2, 0x80, 0xab, 0x3b, 0xd5,
                              0xca, 0x59, 0xe1, 0x81, 0x16, 0x88, 0x90, 0xf4};

  ASSERT_EQ(taosGenerateTotpSecret("admin-seed", 0, secret, sizeof(secret)), (int)sizeof(secret));
  EXPECT_EQ(memcmp(secret, expected, sizeof(expected)), 0);

  // Deterministic: the same seed yields the same secret.
  uint8_t again[32] = {0};
  ASSERT_EQ(taosGenerateTotpSecret("admin-seed", 0, again, sizeof(again)), (int)sizeof(again));
  EXPECT_EQ(memcmp(secret, again, sizeof(again)), 0);

  // A different seed must not collide.
  uint8_t other[32] = {0};
  ASSERT_EQ(taosGenerateTotpSecret("other-seed", 0, other, sizeof(other)), (int)sizeof(other));
  EXPECT_NE(memcmp(secret, other, sizeof(other)), 0);
}

TEST(totpTests, verifiesTheCurrentCodeOnly) {
  uint8_t secret[32] = {0};
  ASSERT_EQ(taosGenerateTotpSecret("verify-seed", 0, secret, sizeof(secret)), (int)sizeof(secret));

  const int32_t code = taosGenerateTotpCode(secret, sizeof(secret), 6);
  ASSERT_GE(code, 0);

  // Window 1 so the assertion cannot fail just because the 30 second time step
  // rolled over between generate and verify.
  EXPECT_EQ(taosVerifyTotpCode(secret, sizeof(secret), code, 6, 1), 1);

  // A code that differs in the last digit is rejected.
  const int32_t wrong = (code + 1) % 1000000;
  EXPECT_EQ(taosVerifyTotpCode(secret, sizeof(secret), wrong, 6, 0), 0);

  // A secret that is too short is never accepted.
  const uint8_t shortSecret[8] = {0};
  EXPECT_EQ(taosVerifyTotpCode(shortSecret, sizeof(shortSecret), code, 6, 0), 0);
}
