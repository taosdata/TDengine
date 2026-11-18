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

// Tests for tFormatQuerySubStatus(), the show queries.sub_status encoder.
//
// The encoder must list every sub task whenever the encoded payload fits into
// a VARCHAR cell, must cut only between entries when it does not, and must
// express task start times as an offset from the query create_time.

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "tarray.h"
#include "tdef.h"
#include "tmsg.h"
#include "types.h"

namespace {

// Maximum VARCHAR payload carried by one cell: the cell cap minus the varstr
// length header.
constexpr int32_t kMaxPayload = TSDB_MAX_FIELD_LEN - VARSTR_HEADER_SIZE;

std::string formatDescs(const std::vector<SQuerySubDesc> &descs, int32_t subPlanNum, int64_t createTimeMs,
                        int32_t cap, int32_t *built) {
  SArray *pSub = taosArrayInit(descs.size(), sizeof(SQuerySubDesc));
  EXPECT_NE(nullptr, pSub);
  for (const SQuerySubDesc &desc : descs) {
    EXPECT_NE(nullptr, taosArrayPush(pSub, &desc));
  }

  std::vector<char> out(cap + 1, '\0');
  int32_t           len = tFormatQuerySubStatus(pSub, subPlanNum, createTimeMs, out.data(), cap, built);
  std::string       result;
  if (len > 0) {
    result.assign(out.data(), len);
  }
  EXPECT_GE(len, 0) << "tFormatQuerySubStatus failed";
  taosArrayDestroy(pSub);
  return result;
}

std::vector<std::string> splitEntries(const std::string &payload) {
  std::vector<std::string> entries;
  if (payload.empty()) {
    return entries;
  }
  size_t start = 0;
  while (true) {
    size_t comma = payload.find(',', start);
    if (comma == std::string::npos) {
      entries.push_back(payload.substr(start));
      break;
    }
    entries.push_back(payload.substr(start, comma - start));
    start = comma + 1;
  }
  return entries;
}

void expectCompleteEntry(const std::string &entry, int64_t tid, const std::string &status,
                         const std::string &offset) {
  std::vector<std::string> fields;
  size_t                   start = 0;
  while (true) {
    size_t colon = entry.find(':', start);
    if (colon == std::string::npos) {
      fields.push_back(entry.substr(start));
      break;
    }
    fields.push_back(entry.substr(start, colon - start));
    start = colon + 1;
  }
  ASSERT_EQ(3U, fields.size()) << "entry must be tid:status:offset, got: " << entry;
  EXPECT_EQ(std::to_string(tid), fields[0]);
  EXPECT_EQ(status, fields[1]);
  EXPECT_EQ(offset, fields[2]);
}

}  // namespace

TEST(SubStatusFormatTest, ListsEveryTaskWhenPayloadFits) {
  // 86 tasks make the payload cross the old 1000 byte limit while staying far
  // below the VARCHAR cap, which is exactly the case that used to be truncated.
  constexpr int32_t        kTaskNum = 86;
  constexpr int64_t        kCreateMs = 1763000000000LL;
  std::vector<SQuerySubDesc> descs;
  for (int32_t i = 0; i < kTaskNum; ++i) {
    SQuerySubDesc desc = {};
    desc.tid = 50971998 + i;
    snprintf(desc.status, sizeof(desc.status), "%s", (i % 2) ? "merge/executing" : "scan/succeed");
    // Task i starts i ms after the query was created.
    desc.startTs = (kCreateMs + i) * 1000;
    descs.push_back(desc);
  }

  int32_t     built = 0;
  std::string payload = formatDescs(descs, kTaskNum, kCreateMs, kMaxPayload, &built);

  EXPECT_EQ(kTaskNum, built);
  EXPECT_GT(static_cast<int32_t>(payload.size()), 1000);
  EXPECT_LT(static_cast<int32_t>(payload.size()), kMaxPayload);

  std::vector<std::string> entries = splitEntries(payload);
  ASSERT_EQ(static_cast<size_t>(kTaskNum), entries.size());
  for (int32_t i = 0; i < kTaskNum; ++i) {
    expectCompleteEntry(entries[i], 50971998 + i, (i % 2) ? "merge/executing" : "scan/succeed",
                        "+" + std::to_string(i) + "ms");
  }
}

TEST(SubStatusFormatTest, NotStartedAndLongOffsets) {
  constexpr int64_t kCreateMs = 1763000000000LL;
  std::vector<SQuerySubDesc> descs(3);
  descs[0].tid = 1;
  snprintf(descs[0].status, sizeof(descs[0].status), "scan/init");
  descs[0].startTs = 0;  // never started
  descs[1].tid = 2;
  snprintf(descs[1].status, sizeof(descs[1].status), "scan/succeed");
  descs[1].startTs = (kCreateMs + 58) * 1000;
  descs[2].tid = 3;
  snprintf(descs[2].status, sizeof(descs[2].status), "merge/executing");
  descs[2].startTs = (kCreateMs + 3600000) * 1000;  // one hour in

  int32_t     built = 0;
  std::string payload = formatDescs(descs, 3, kCreateMs, kMaxPayload, &built);

  std::vector<std::string> entries = splitEntries(payload);
  ASSERT_EQ(3U, entries.size());
  expectCompleteEntry(entries[0], 1, "scan/init", "-");
  expectCompleteEntry(entries[1], 2, "scan/succeed", "+58ms");
  expectCompleteEntry(entries[2], 3, "merge/executing", "+3600000ms");
}

TEST(SubStatusFormatTest, TruncatesOnlyBetweenEntriesAtTheVarcharLimit) {
  // Longest practical entry: 19-digit tid, 31-char state, 10-char offset.
  constexpr int64_t kCreateMs = 1763000000000LL;
  constexpr int64_t kTid = 1234567890123456789LL;
  const char *const kStatus = "compute/partial_succeed_waiting";

  SQuerySubDesc probe = {};
  probe.tid = kTid;
  snprintf(probe.status, sizeof(probe.status), "%s", kStatus);
  probe.startTs = (kCreateMs + 3600000) * 1000;
  int32_t     probeBuilt = 0;
  std::string probePayload = formatDescs({probe}, 1, kCreateMs, kMaxPayload, &probeBuilt);
  ASSERT_EQ(1, probeBuilt);
  const int32_t entryLen = static_cast<int32_t>(probePayload.size());
  const int32_t entryNum = kMaxPayload / (entryLen + 1);

  // Exact encoded size of the entries that fit into one VARCHAR cell.
  const int32_t prefixLen = entryNum * (entryLen + 1) - 1;

  // One more entry than fits: the capacity is exactly the size of the entries
  // that fit, so the encoder must stop between two entries.
  std::vector<SQuerySubDesc> descs;
  for (int32_t i = 0; i < entryNum + 1; ++i) {
    descs.push_back(probe);
  }

  int32_t     built = 0;
  std::string payload = formatDescs(descs, entryNum + 1, kCreateMs, prefixLen, &built);
  EXPECT_EQ(entryNum, built);
  EXPECT_EQ(prefixLen, static_cast<int32_t>(payload.size()));
  std::vector<std::string> entries = splitEntries(payload);
  ASSERT_EQ(static_cast<size_t>(entryNum), entries.size());
  for (const std::string &entry : entries) {
    expectCompleteEntry(entry, kTid, kStatus, "+3600000ms");
  }

  // The same number of entries fits into the VARCHAR cap and is returned
  // complete, right below the limit.
  int32_t     fullBuilt = 0;
  std::string full = formatDescs(descs, entryNum, kCreateMs, kMaxPayload, &fullBuilt);
  EXPECT_EQ(entryNum, fullBuilt);
  EXPECT_EQ(prefixLen, static_cast<int32_t>(full.size()));
  EXPECT_EQ(static_cast<size_t>(entryNum), splitEntries(full).size());
  EXPECT_LE(static_cast<int32_t>(full.size()), kMaxPayload);
}

TEST(SubStatusFormatTest, StopsAtSubPlanNum) {
  constexpr int64_t          kCreateMs = 1763000000000LL;
  std::vector<SQuerySubDesc> descs(3);
  for (int32_t i = 0; i < 3; ++i) {
    descs[i].tid = i + 1;
    snprintf(descs[i].status, sizeof(descs[i].status), "scan/init");
  }

  int32_t     built = 0;
  std::string payload = formatDescs(descs, 2, kCreateMs, kMaxPayload, &built);
  EXPECT_EQ(2, built);
  EXPECT_EQ(2U, splitEntries(payload).size());
}
