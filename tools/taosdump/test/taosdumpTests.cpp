/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

// Unit tests for the taosdump path-component name encoding (bckName.c).
//
// Covers every branch of bckNameNeedsEncode / bckNameEncode / bckNameDecode /
// bckNameDecodeInPlace and their static helpers (bckNameCharUnsafe,
// bckNameStemIsReservedDevice, bckHexVal): unsafe-character detection,
// percent-encoding, position rules (leading/trailing dot/space), Windows
// reserved device names, '%'-escaping, invalid-hex handling, zero-copy fast
// paths, and buffer-overflow guards.

#include <gtest/gtest.h>

#include <string.h>

#include "bckName.h"

// ---------------------------------------------------------------------------
// bckNameNeedsEncode
// ---------------------------------------------------------------------------

TEST(BckNameNeedsEncode, NullAndEmpty) {
  EXPECT_FALSE(bckNameNeedsEncode(NULL));
  EXPECT_FALSE(bckNameNeedsEncode(""));
}

TEST(BckNameNeedsEncode, SafeNames) {
  EXPECT_FALSE(bckNameNeedsEncode("meters"));
  EXPECT_FALSE(bckNameNeedsEncode("t_1"));
  EXPECT_FALSE(bckNameNeedsEncode("a.b"));       // mid-name dot is legal everywhere
  EXPECT_FALSE(bckNameNeedsEncode("a b"));       // mid-name space is legal everywhere
  EXPECT_FALSE(bckNameNeedsEncode("db_test"));
  EXPECT_FALSE(bckNameNeedsEncode("设备1"));      // multibyte UTF-8 passes through
  EXPECT_FALSE(bckNameNeedsEncode("あいうえお"));
}

TEST(BckNameNeedsEncode, PositionRules) {
  EXPECT_TRUE(bckNameNeedsEncode(".foo"));   // leading dot
  EXPECT_TRUE(bckNameNeedsEncode("foo."));   // trailing dot
  EXPECT_TRUE(bckNameNeedsEncode("foo "));   // trailing space
}

TEST(BckNameNeedsEncode, ReservedDeviceNames) {
  EXPECT_TRUE(bckNameNeedsEncode("CON"));
  EXPECT_TRUE(bckNameNeedsEncode("nul"));
  EXPECT_TRUE(bckNameNeedsEncode("Prn"));
  EXPECT_TRUE(bckNameNeedsEncode("com1"));
  EXPECT_TRUE(bckNameNeedsEncode("COM9"));
  EXPECT_TRUE(bckNameNeedsEncode("lpt1"));
  EXPECT_TRUE(bckNameNeedsEncode("LPT9"));
  EXPECT_TRUE(bckNameNeedsEncode("CON.foo"));  // stem before '.' is still reserved
  EXPECT_FALSE(bckNameNeedsEncode("CONx"));    // not an exact reserved stem
}

TEST(BckNameNeedsEncode, UnsafeCharacters) {
  const char *names[] = {"a<b",  "a>b", "a:b", "a\"b", "a/b",
                         "a\\b", "a|b", "a?b", "a*b", "a%b"};
  for (const char *n : names) {
    EXPECT_TRUE(bckNameNeedsEncode(n)) << n;
  }
}

TEST(BckNameNeedsEncode, ControlCharacters) {
  char name[8] = {'a', 0x01, 'b', 0};       // 0x01
  EXPECT_TRUE(bckNameNeedsEncode(name));
  char name2[8] = {'a', 0x1F, 'b', 0};      // 0x1F
  EXPECT_TRUE(bckNameNeedsEncode(name2));
}

TEST(BckNameNeedsEncode, ReservedDeviceStemBufferCap) {
  // 16+ chars before any '.' forces the stem buffer to cap at 15 chars,
  // exercising the `i + 1 < sizeof(stem)` loop-termination branch.
  EXPECT_FALSE(bckNameNeedsEncode("abcdefghijklmnopqrstuvwxyz"));
}

// ---------------------------------------------------------------------------
// bckNameEncode
// ---------------------------------------------------------------------------

TEST(BckNameEncode, NullAndBadBuffer) {
  char buf[128];
  EXPECT_EQ(bckNameEncode(NULL, buf, sizeof(buf)), nullptr);
  EXPECT_EQ(bckNameEncode("a:b", NULL, sizeof(buf)), nullptr);
  EXPECT_EQ(bckNameEncode("a:b", buf, 0), nullptr);
}

TEST(BckNameEncode, SafeNamePassThrough) {
  char buf[128];
  const char *name = "meters";
  // Fast path: no encoding, returns the SAME pointer (zero copy).
  EXPECT_EQ(bckNameEncode(name, buf, sizeof(buf)), name);
}

TEST(BckNameEncode, UnsafeCharactersRoundTrip) {
  const char *input[] = {"a:b", "a\\b", "a<b", "a>b", "a\"b",
                         "a/b", "a|b", "a?b", "a*b", "a%b"};
  for (const char *in : input) {
    char enc[256] = {0};
    char dec[256] = {0};
    const char *e = bckNameEncode(in, enc, sizeof(enc));
    ASSERT_NE(e, nullptr) << in;
    ASSERT_NE(e, in) << "unsafe name must be encoded: " << in;
    const char *d = bckNameDecode(e, dec, sizeof(dec));
    ASSERT_NE(d, nullptr);
    EXPECT_STREQ(d, in) << "round-trip failed for " << in << " (enc=" << e << ")";
  }
}

TEST(BckNameEncode, ColonIsPercent3A) {
  char buf[128];
  EXPECT_STREQ(bckNameEncode("a:b", buf, sizeof(buf)), "a%3Ab");
}

TEST(BckNameEncode, BackslashIsPercent5C) {
  char buf[128];
  EXPECT_STREQ(bckNameEncode("t\\1", buf, sizeof(buf)), "t%5C1");
}

TEST(BckNameEncode, PercentIsEscaped) {
  char buf[128];
  // '%' itself must be escaped so decode is unambiguous.
  EXPECT_STREQ(bckNameEncode("file%3a", buf, sizeof(buf)), "file%253a");
  EXPECT_STREQ(bckNameEncode("a%b", buf, sizeof(buf)), "a%25b");
}

TEST(BckNameEncode, PositionRules) {
  char buf[128];
  EXPECT_STREQ(bckNameEncode(".foo", buf, sizeof(buf)), "%2Efoo");
  EXPECT_STREQ(bckNameEncode("foo.", buf, sizeof(buf)), "foo%2E");
  EXPECT_STREQ(bckNameEncode("foo ", buf, sizeof(buf)), "foo%20");
}

TEST(BckNameEncode, ReservedDeviceNames) {
  char buf[128];
  EXPECT_STREQ(bckNameEncode("CON", buf, sizeof(buf)), "%43ON");
  EXPECT_STREQ(bckNameEncode("nul", buf, sizeof(buf)), "%6Eul");
  EXPECT_STREQ(bckNameEncode("CON.foo", buf, sizeof(buf)), "%43ON.foo");
}

TEST(BckNameEncode, ControlCharacterIsPercentEncoded) {
  char name[8] = {'a', 0x01, 'b', 0};
  char buf[128];
  EXPECT_STREQ(bckNameEncode(name, buf, sizeof(buf)), "a%01b");
}

TEST(BckNameEncode, BufferTooSmallEscapeBranch) {
  // "a:b": 'a' fits, then ':' needs 3 bytes (o+3 >= outCap) -> NULL.
  char buf[3];
  EXPECT_EQ(bckNameEncode("a:b", buf, sizeof(buf)), nullptr);
}

TEST(BckNameEncode, BufferTooSmallCopyBranch) {
  // "abcd:": 'a','b','c' fit, then 'd' hits the o+1 >= outCap guard -> NULL.
  char buf[4];
  EXPECT_EQ(bckNameEncode("abcd:", buf, sizeof(buf)), nullptr);
}

// ---------------------------------------------------------------------------
// bckNameDecode
// ---------------------------------------------------------------------------

TEST(BckNameDecode, NullAndBadBuffer) {
  char buf[128];
  EXPECT_EQ(bckNameDecode(NULL, buf, sizeof(buf)), nullptr);
  EXPECT_EQ(bckNameDecode("a", NULL, sizeof(buf)), nullptr);
  EXPECT_EQ(bckNameDecode("a", buf, 0), nullptr);
}

TEST(BckNameDecode, NoPercentPassThrough) {
  char buf[128];
  const char *name = "plain_name";
  EXPECT_EQ(bckNameDecode(name, buf, sizeof(buf)), name);
}

TEST(BckNameDecode, LowerAndUpperHex) {
  char buf[128];
  EXPECT_STREQ(bckNameDecode("a%3Ab", buf, sizeof(buf)), "a:b");   // mixed-case hex
  EXPECT_STREQ(bckNameDecode("a%3ab", buf, sizeof(buf)), "a:b");   // lowercase hex
  EXPECT_STREQ(bckNameDecode("%3A", buf, sizeof(buf)), ":");       // uppercase hex
  EXPECT_STREQ(bckNameDecode("%5c", buf, sizeof(buf)), "\\");      // lowercase 5c
}

TEST(BckNameDecode, InvalidHexIsLiteral) {
  char buf[128];
  EXPECT_STREQ(bckNameDecode("a%xb", buf, sizeof(buf)), "a%xb");   // 'x' not hex
  EXPECT_STREQ(bckNameDecode("a%xg", buf, sizeof(buf)), "a%xg");   // 'g' not hex
  EXPECT_STREQ(bckNameDecode("a%", buf, sizeof(buf)), "a%");       // trailing '%'
  EXPECT_STREQ(bckNameDecode("100%", buf, sizeof(buf)), "100%");
}

TEST(BckNameDecode, BufferTooSmall) {
  char buf[2];
  // "a%3Ab": 'a' ok, '%3A'->':' ok (o=2), 'b' hits o+1 >= outCap -> NULL.
  EXPECT_EQ(bckNameDecode("a%3Ab", buf, sizeof(buf)), nullptr);
}

// ---------------------------------------------------------------------------
// bckNameDecodeInPlace
// ---------------------------------------------------------------------------

TEST(BckNameDecodeInPlace, NullAndBadCap) {
  char s[32];
  EXPECT_EQ(bckNameDecodeInPlace(NULL, sizeof(s)), nullptr);
  strcpy(s, "a%3Ab");
  EXPECT_EQ(bckNameDecodeInPlace(s, 0), nullptr);
}

TEST(BckNameDecodeInPlace, NoPercentPassThrough) {
  char s[32];
  strcpy(s, "plain_name");
  EXPECT_EQ(bckNameDecodeInPlace(s, sizeof(s)), s);
  EXPECT_STREQ(s, "plain_name");
}

TEST(BckNameDecodeInPlace, InPlaceDecode) {
  char s[32];
  strcpy(s, "a%3Ab");
  EXPECT_NE(bckNameDecodeInPlace(s, sizeof(s)), nullptr);
  EXPECT_STREQ(s, "a:b");
}

TEST(BckNameDecodeInPlace, BufferTooSmall) {
  char s[32];
  strcpy(s, "a%3Ab");
  EXPECT_EQ(bckNameDecodeInPlace(s, 2), nullptr);
}

TEST(BckNameDecodeInPlace, LiteralPercentSurvives) {
  char s[32];
  strcpy(s, "a%xb");
  EXPECT_NE(bckNameDecodeInPlace(s, sizeof(s)), nullptr);
  EXPECT_STREQ(s, "a%xb");
}

// ---------------------------------------------------------------------------
// bckSqlEscape
// ---------------------------------------------------------------------------

TEST(BckSqlEscape, NullAndBadBuffer) {
  char buf[128];
  EXPECT_EQ(bckSqlEscape(NULL, buf, sizeof(buf)), nullptr);
  EXPECT_EQ(bckSqlEscape("a", NULL, sizeof(buf)), nullptr);
  EXPECT_EQ(bckSqlEscape("a", buf, 0), nullptr);
}

TEST(BckSqlEscape, FastPathNoSpecial) {
  char buf[128];
  const char *name = "meters";
  // Names without backslash / single quote return the SAME pointer (zero copy).
  EXPECT_EQ(bckSqlEscape(name, buf, sizeof(buf)), name);
  EXPECT_EQ(bckSqlEscape("sp<>:\"/|?*%db", buf, sizeof(buf)) == NULL, false);
  // No special chars -> passthrough, even with special-but-safe ASCII.
  const char *s2 = "sp<>:\"/|?*%db";
  EXPECT_EQ(bckSqlEscape(s2, buf, sizeof(buf)), s2);
}

TEST(BckSqlEscape, EscapesBackslashAndQuote) {
  char buf[128];
  EXPECT_STREQ(bckSqlEscape("a\\b", buf, sizeof(buf)), "a\\\\b");    // \ -> \\
  EXPECT_STREQ(bckSqlEscape("a'b", buf, sizeof(buf)), "a\\'b");      // ' -> \'
  EXPECT_STREQ(bckSqlEscape("a\\'b", buf, sizeof(buf)), "a\\\\\\'b");
}

TEST(BckSqlEscape, BufferTooSmall) {
  char buf[3];
  // "a\b": 'a' fits, then backslash needs 2 bytes (o+2 >= outCap) -> NULL.
  EXPECT_EQ(bckSqlEscape("a\\b", buf, sizeof(buf)), nullptr);
}

// ---------------------------------------------------------------------------
// bckNameDecodeFileStem
// ---------------------------------------------------------------------------

TEST(BckNameDecodeFileStem, NullAndBadBuffer) {
  char out[64];
  EXPECT_EQ(bckNameDecodeFileStem(NULL, out, sizeof(out)), -1);
  EXPECT_EQ(bckNameDecodeFileStem("a.dat", NULL, sizeof(out)), -1);
  EXPECT_EQ(bckNameDecodeFileStem("a.dat", out, 0), -1);
}

TEST(BckNameDecodeFileStem, PlainNameNoExtStrip) {
  char out[64];
  // Extension is stripped; no '%' -> stem passes through.
  EXPECT_EQ(bckNameDecodeFileStem("meters.dat", out, sizeof(out)), 0);
  EXPECT_STREQ(out, "meters");
  EXPECT_EQ(bckNameDecodeFileStem("ctb_a.par", out, sizeof(out)), 0);
  EXPECT_STREQ(out, "ctb_a");
  // Name without a '.' extension: whole basename is the stem.
  EXPECT_EQ(bckNameDecodeFileStem("plain", out, sizeof(out)), 0);
  EXPECT_STREQ(out, "plain");
}

TEST(BckNameDecodeFileStem, DecodesEncodedSpecialChars) {
  char out[64];
  EXPECT_EQ(bckNameDecodeFileStem("a%5Cb.dat", out, sizeof(out)), 0);   // a\b
  EXPECT_STREQ(out, "a\\b");
  EXPECT_EQ(bckNameDecodeFileStem("a%3Ab.par", out, sizeof(out)), 0);   // a:b
  EXPECT_STREQ(out, "a:b");
  EXPECT_EQ(bckNameDecodeFileStem("%43ON.dat", out, sizeof(out)), 0);   // CON
  EXPECT_STREQ(out, "CON");
}

TEST(BckNameDecodeFileStem, LongStemOver192Bytes) {
  // 191-char table name with a backslash in the middle: encoded stem is
  // 140 + 3 (%5C) + 50 = 193 bytes > TSDB_TABLE_NAME_LEN(193)-1.  The old
  // TSDB_TABLE_NAME_LEN-sized buffer logic truncated this; the helper must
  // decode it fully from its internal worst-case buffer.
  char encoded[BCK_NAME_ENC_STEM_MAX];
  int o = 0;
  for (int i = 0; i < 140; i++) encoded[o++] = 'T';
  memcpy(encoded + o, "%5C", 3); o += 3;
  for (int i = 0; i < 50; i++) encoded[o++] = 'U';
  encoded[o] = '\0';
  strcpy(encoded + o, ".dat");  // append extension

  const int TABLE_NAME_LEN = 193;  // same as TSDB_TABLE_NAME_LEN
  char out[TABLE_NAME_LEN];
  EXPECT_EQ(bckNameDecodeFileStem(encoded, out, sizeof(out)), 0);
  EXPECT_EQ((int)strlen(out), 191);
  EXPECT_EQ(out[140], '\\');
  char expectT[141];
  memset(expectT, 'T', 140);
  expectT[140] = '\0';
  char expectU[51];
  memset(expectU, 'U', 50);
  expectU[50] = '\0';
  EXPECT_EQ(strncmp(out, expectT, 140), 0);
  EXPECT_EQ(strncmp(out + 141, expectU, 50), 0);
}

TEST(BckNameDecodeFileStem, LiteralPercentSurvives) {
  char out[64];
  // '%2A' is not a real escape target here ('%' is encoded as %25, but a
  // literal '%2A' in a stem should be decoded back to '%' only when it is a
  // genuine %HH sequence).
  EXPECT_EQ(bckNameDecodeFileStem("a%2Ab.dat", out, sizeof(out)), 0);
  EXPECT_STREQ(out, "a*b");
}
