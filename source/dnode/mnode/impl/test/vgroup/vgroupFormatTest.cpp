#include <gtest/gtest.h>
#include "mndVgroup.h"

// Test mndBytesToHuman: verify the logic that formats a byte count into a human-readable string.
// Covers 0, boundary values (1023/1024), fractional rounding (1536), GB level (1.5GB),
// as well as edge cases like negative numbers, NULL protection, bufLen<=0, TB and the cap, and the return value.
TEST(vgroupFormat, mndBytesToHuman) {
  char buf[32] = {0};
  // Basic cases: 0, boundary values, and rounding
  mndBytesToHuman(0, buf, sizeof(buf));            EXPECT_STREQ(buf, "0B");
  mndBytesToHuman(1023, buf, sizeof(buf));         EXPECT_STREQ(buf, "1023.0B");
  mndBytesToHuman(1024, buf, sizeof(buf));         EXPECT_STREQ(buf, "1.0KB");
  mndBytesToHuman(1536, buf, sizeof(buf));         EXPECT_STREQ(buf, "1.5KB");
  mndBytesToHuman(1610612736LL, buf, sizeof(buf)); EXPECT_STREQ(buf, "1.5GB");

  // Negative numbers: formatted uniformly as "0B", the same as 0
  mndBytesToHuman(-1, buf, sizeof(buf));        EXPECT_STREQ(buf, "0B");

  // Argument protection: return 0 without crashing when buf is NULL
  EXPECT_EQ(mndBytesToHuman(1024, NULL, 32), 0);
  // Argument protection: return 0 when bufLen<=0
  EXPECT_EQ(mndBytesToHuman(1024, buf, 0), 0);

  // TB level: 1.5TB = 1610612736KB * 1024 bytes, displayed normally as "1.5TB"
  mndBytesToHuman(1610612736LL * 1024, buf, sizeof(buf)); EXPECT_STREQ(buf, "1.5TB");
  // Cap verification: a huge value (2048TB) no longer rounds up and is still displayed with TB as the largest unit
  mndBytesToHuman(2LL * 1024 * 1024 * 1024 * 1024 * 1024, buf, sizeof(buf));
  EXPECT_STREQ(buf, "2048.0TB");

  // Return value: when the "0B" branch is written normally, the return value should be > 0 (characters were written)
  EXPECT_GT(mndBytesToHuman(0, buf, sizeof(buf)), 0);
}
