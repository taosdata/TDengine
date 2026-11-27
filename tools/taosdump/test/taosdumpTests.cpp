/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include <string>
#include <vector>

#include "avroSqlBuf.h"
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

// ---------------------------------------------------------------------------
// avroSqlAppendTagValue: bounded append of dump-controlled tag values
//
// Regression tests for the Avro restore path that builds
// "CREATE TABLE ... USING ... TAGS(...)" in a fixed 4 MiB buffer: the tag value
// length comes from the (untrusted) avro-tbtags file, so the append must reject
// what does not fit instead of writing past the buffer.
// ---------------------------------------------------------------------------

TEST(AvroSqlBuf, AppendsSmallValue) {
  char buf[64] = {0};
  int32_t pos = avroSqlAppendTagValue(buf, sizeof(buf), 0, "abc", 3);
  EXPECT_EQ(pos, 6);
  EXPECT_STREQ(buf, "'abc',");
}

TEST(AvroSqlBuf, AppendsAtGivenPosition) {
  char buf[64] = {0};
  memcpy(buf, "TAGS(", 5);
  int32_t pos = avroSqlAppendTagValue(buf, sizeof(buf), 5, "x", 1);
  EXPECT_EQ(pos, 9);
  EXPECT_STREQ(buf, "TAGS('x',");
}

TEST(AvroSqlBuf, NullValueIsNullLiteral) {
  char buf[64] = {0};
  int32_t pos = avroSqlAppendTagValue(buf, sizeof(buf), 0, nullptr, 0);
  EXPECT_EQ(pos, 5);
  EXPECT_STREQ(buf, "NULL,");
}

TEST(AvroSqlBuf, DoublesSingleQuotes) {
  char buf[64] = {0};
  // Both quote kinds present -> the doubling path, worst case 2 bytes per input.
  const char *val = "a'b\"c";
  int32_t pos = avroSqlAppendTagValue(buf, sizeof(buf), 0, val, strlen(val));
  EXPECT_EQ(pos, 9);
  EXPECT_STREQ(buf, "'a''b\"c',");
}

TEST(AvroSqlBuf, UsesDoubleQuotesWhenOnlySingleQuotePresent) {
  char buf[64] = {0};
  const char *val = "a'b";
  int32_t pos = avroSqlAppendTagValue(buf, sizeof(buf), 0, val, strlen(val));
  EXPECT_EQ(pos, 6);
  EXPECT_STREQ(buf, "\"a'b\",");
}

// The size check must use the worst case (2 * len + quotes + comma): a value
// that fits only because the current data happens to need no escaping must
// still be accepted, and one byte more must be rejected.
TEST(AvroSqlBuf, ReserveIsHonoured) {
  const int32_t cap = 128;
  std::vector<char> buf(cap, 'Z');
  const size_t len = 16;  // worst case = 2*16 + 4 = 36
  std::string val(len, 'a');

  // pos so that pos + 36 + AVRO_SQL_TAIL_RESERVE == cap  -> fits
  int32_t posFits = cap - AVRO_SQL_TAIL_RESERVE - 36;
  EXPECT_GE(avroSqlAppendTagValue(buf.data(), cap, posFits, val.data(), len), 0);

  // One byte further -> must be rejected and must not write anything.
  std::vector<char> buf2(cap, 'Z');
  memcpy(buf2.data(), buf.data(), cap);
  EXPECT_EQ(avroSqlAppendTagValue(buf2.data(), cap, posFits + 1, val.data(), len), -1);
  EXPECT_EQ(memcmp(buf2.data(), buf.data(), cap), 0);
}

TEST(AvroSqlBuf, RejectsValueThatDoesNotFit) {
  const int32_t cap = 64;
  std::vector<char> buf(cap, 'Z');
  std::string val(40, 'a');  // worst case 84 > cap - reserve
  EXPECT_EQ(avroSqlAppendTagValue(buf.data(), cap, 0, val.data(), val.size()), -1);
  // Nothing may be written when the value is rejected.
  for (int32_t i = 0; i < cap; i++) EXPECT_EQ(buf[i], 'Z') << i;
}

// A single > 4 MiB tag value (finding: "taosdump restore appends oversized Avro
// tag strings to 4 MB SQL buffer before checking length") must be rejected
// without touching memory past the buffer.
TEST(AvroSqlBuf, RejectsFiveMiBValueWithCanaryTail) {
  const int32_t cap = 4 * 1024 * 1024;
  std::vector<char> buf((size_t)cap + 64, 'Z');
  std::string val(5 * 1024 * 1024, 'A');

  EXPECT_EQ(avroSqlAppendTagValue(buf.data(), cap, 0, val.data(), val.size()), -1);
  EXPECT_EQ(avroSqlAppendTagValue(buf.data(), cap, cap - 64, val.data(), val.size()), -1);

  // The 64-byte canary after the capacity must be untouched.
  for (size_t i = (size_t)cap; i < buf.size(); i++) EXPECT_EQ(buf[i], 'Z') << i;
}

// Same value but bounded to what fits: exercise the escaping path with a large
// value to make sure the accepted path stays inside the buffer.
TEST(AvroSqlBuf, AcceptsLargeValueAndStaysInBounds) {
  const int32_t cap = 4 * 1024 * 1024;
  std::vector<char> buf((size_t)cap + 64, 'Z');
  const size_t len = 1024 * 1024;  // worst case 2 MiB + 4 <= cap - reserve
  std::string val(len, '\'');
  val[0] = 'a';  // force the doubling branch with both quote kinds
  val[1] = '"';

  int32_t pos = avroSqlAppendTagValue(buf.data(), cap, 0, val.data(), val.size());
  ASSERT_GT(pos, 0);
  EXPECT_LT((size_t)pos, (size_t)cap);
  EXPECT_EQ(buf[pos], '\0');
  EXPECT_EQ(buf[0], '\'');
  EXPECT_EQ(buf[1], 'a');
  for (size_t i = (size_t)cap; i < buf.size(); i++) EXPECT_EQ(buf[i], 'Z') << i;
}

TEST(AvroSqlBuf, InvalidArguments) {
  char buf[64] = {0};
  EXPECT_EQ(avroSqlAppendTagValue(nullptr, 64, 0, "a", 1), -1);
  EXPECT_EQ(avroSqlAppendTagValue(buf, 0, 0, "a", 1), -1);
  EXPECT_EQ(avroSqlAppendTagValue(buf, 64, -1, "a", 1), -1);
}

// ---------------------------------------------------------------------------
// avroSqlBuildInsertStmt: "INSERT INTO ... VALUES(?,?..)" with a dump-controlled
// column count
//
// Regression tests for the restore path that assembled this statement in a
// fixed 4 KiB stack buffer: the column count comes from the (untrusted) avro
// data file, so a file declaring a few thousand columns used to write far past
// the buffer.
// ---------------------------------------------------------------------------

TEST(AvroSqlInsertStmt, BuildsSingleColumn) {
  char buf[256] = {0};
  int32_t n = avroSqlBuildInsertStmt(buf, sizeof(buf), "db", "tb", "", 1);
  ASSERT_GT(n, 0);
  EXPECT_STREQ(buf, "INSERT INTO `db`.`tb`  VALUES(?)");
  EXPECT_EQ((size_t)n, strlen(buf));
}

TEST(AvroSqlInsertStmt, BuildsThreeColumnsWithPartialList) {
  char buf[256] = {0};
  const char *partCols = "(`c1`)";
  int32_t n = avroSqlBuildInsertStmt(buf, sizeof(buf), "db", "tb", partCols, 3);
  ASSERT_GT(n, 0);
  EXPECT_STREQ(buf, "INSERT INTO `db`.`tb` (`c1`) VALUES(?,?,?)");
  EXPECT_EQ((size_t)n, strlen(buf));
}

TEST(AvroSqlInsertStmt, SizeHelperIsSufficient) {
  const char *db = "some_database";
  const char *tb = "some_super_long_table_name_used_by_the_test";
  const char *partCols = "(`tag1`,`tag2`)";
  const int32_t nCols = 4096;

  size_t cap = avroSqlInsertStmtSize(db, tb, partCols, nCols);
  std::vector<char> buf(cap, 'Z');
  int32_t n = avroSqlBuildInsertStmt(buf.data(), cap, db, tb, partCols, nCols);
  ASSERT_GT(n, 0);
  EXPECT_LT((size_t)n, cap);
  EXPECT_EQ(buf[(size_t)n], '\0');
  // Every placeholder must have been emitted.
  int32_t questions = 0;
  for (int32_t i = 0; i < n; i++) {
    if (buf[(size_t)i] == '?') questions++;
  }
  EXPECT_EQ(questions, nCols);
}

// A 4 KiB buffer (the size of the old stack buffer) must be rejected rather than
// overflowed when the file asks for more placeholders than fit.
TEST(AvroSqlInsertStmt, RejectsMoreColumnsThanFitWithCanaryTail) {
  const size_t cap = 4096;
  std::vector<char> buf(cap + 64, 'Z');
  int32_t n = avroSqlBuildInsertStmt(buf.data(), cap, "db", "tb", "", 32767);
  EXPECT_EQ(n, -1);
  for (size_t i = cap; i < buf.size(); i++) EXPECT_EQ(buf[i], 'Z') << i;
}

TEST(AvroSqlInsertStmt, RejectsHeaderThatDoesNotFitWithCanaryTail) {
  const size_t cap = 64;
  std::vector<char> buf(cap + 64, 'Z');
  const std::string longName(400, 'a');
  int32_t n = avroSqlBuildInsertStmt(buf.data(), cap, longName.c_str(), longName.c_str(), "",
                                     2);
  EXPECT_EQ(n, -1);
  // snprintf may truncate into the buffer, but nothing may be written past cap
  // and the result must stay NUL-terminated inside it.
  EXPECT_EQ(buf[cap - 1], '\0');
  for (size_t i = cap; i < buf.size(); i++) EXPECT_EQ(buf[i], 'Z') << i;
}

TEST(AvroSqlInsertStmt, RejectsInvalidArguments) {
  char buf[256] = {0};
  EXPECT_EQ(avroSqlBuildInsertStmt(nullptr, 256, "db", "tb", "", 1), -1);
  EXPECT_EQ(avroSqlBuildInsertStmt(buf, 0, "db", "tb", "", 1), -1);
  EXPECT_EQ(avroSqlBuildInsertStmt(buf, sizeof(buf), "db", "tb", "", -1), -1);
  EXPECT_EQ(avroSqlBuildInsertStmt(buf, sizeof(buf), "db", "tb", "", 0), -1);
}
