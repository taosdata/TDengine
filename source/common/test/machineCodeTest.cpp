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

/*
 * Unit tests for the machine code format helpers used by the dnode heartbeat
 * identity gate (mndProcessStatusReq).
 *
 * A machine code is the value `taosd -k` prints as "machine code(N): <code>". Its
 * first byte is the index of the hardware source the grant library derived the
 * digest from, so a release that changes the preferred source makes a legitimate
 * in-place upgrade report a different code for the same machine. The gate accepts
 * only a forward move between known variants; everything else stays a mismatch.
 *
 * The grant library itself is closed source and absent from the community build,
 * which is exactly why the format handling lives in common/ and is tested here
 * with codes captured from real Windows builds.
 */

#include <gtest/gtest.h>

#include "tgrant.h"

namespace {

// Real values reported by `taosd -k` on Windows; the digit is the source variant.
const char *kCodeVariant1 = "AQGDaR/mxGqmHeDZPwLNjRmX";  // 3.4.1.0
const char *kCodeVariant3 = "A7hzIXaXPUsm4seCliw72e8F";  // current build

}  // namespace

TEST(MachineCodeTest, decodesSourceVariantFromFirstByte) {
  EXPECT_EQ(1, taosMachineCodeSourceVariant(kCodeVariant1));
  EXPECT_EQ(3, taosMachineCodeSourceVariant(kCodeVariant3));
  // The two leading characters jointly carry one byte: 0b000001 000000 -> 1.
  EXPECT_EQ(0, taosMachineCodeSourceVariant("AA"));
  EXPECT_EQ(2, taosMachineCodeSourceVariant("Ag"));
  EXPECT_EQ(3, taosMachineCodeSourceVariant("Aw"));
}

TEST(MachineCodeTest, rejectsValuesThatAreNotMachineCodes) {
  EXPECT_EQ(-1, taosMachineCodeSourceVariant(nullptr));
  EXPECT_EQ(-1, taosMachineCodeSourceVariant(""));
  EXPECT_EQ(-1, taosMachineCodeSourceVariant("A"));
  // Padding and other non-alphabet characters cannot carry a variant.
  EXPECT_EQ(-1, taosMachineCodeSourceVariant("=AAA"));
  EXPECT_EQ(-1, taosMachineCodeSourceVariant("A=AA"));
  EXPECT_EQ(-1, taosMachineCodeSourceVariant("\x01\x02"));
}

TEST(MachineCodeTest, acceptsKnownMigrationOnly) {
  // Windows moved its preferred hardware source from 1 (3.4.1.0) to 3, so an
  // upgraded node reports a different code for an unchanged machine.
  EXPECT_TRUE(taosMachineCodeVariantMovedForward(kCodeVariant1, kCodeVariant3));
  // A forward move that no release shipped is not an upgrade: accepting it
  // would let a forged heartbeat choose the variant byte itself.
  EXPECT_FALSE(taosMachineCodeVariantMovedForward("AA", "Ag"));
  EXPECT_FALSE(taosMachineCodeVariantMovedForward("Ag", "Aw"));
  EXPECT_FALSE(taosMachineCodeVariantMovedForward(kCodeVariant1, "Ag"));
}

TEST(MachineCodeTest, rejectsEqualBackwardAndUnknownMoves) {
  EXPECT_FALSE(taosMachineCodeVariantMovedForward(kCodeVariant3, kCodeVariant3));
  // Rolling back to an older source is indistinguishable from a forged heartbeat.
  EXPECT_FALSE(taosMachineCodeVariantMovedForward(kCodeVariant3, kCodeVariant1));
  EXPECT_FALSE(taosMachineCodeVariantMovedForward("Ag", "AA"));
  // Unknown variants must not be treated as an upgrade in either direction.
  EXPECT_FALSE(taosMachineCodeVariantMovedForward("", kCodeVariant3));
  EXPECT_FALSE(taosMachineCodeVariantMovedForward(kCodeVariant1, ""));
  EXPECT_FALSE(taosMachineCodeVariantMovedForward(nullptr, kCodeVariant3));
  EXPECT_FALSE(taosMachineCodeVariantMovedForward(kCodeVariant1, nullptr));
  EXPECT_FALSE(taosMachineCodeVariantMovedForward("=AAA", kCodeVariant3));
}
