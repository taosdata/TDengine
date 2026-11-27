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

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstdio>
#include <memory>
#include <set>
#include <string>

#include "os.h"
#include "osEnv.h"
#include "spillDistinctSet.h"
#include "taoserror.h"
#include "tglobal.h"
#include "tpagedbuf.h"

namespace {

// The spill paths write pages to tsTempDir. A bare gtest binary never loads a config, so that
// global is empty and taosGetTmpfilePath() would index tmpPath[-1]. osDefaultInit() fills in the
// platform default, exactly as the server does at startup.
class TempDirEnv : public ::testing::Environment {
 public:
  void SetUp() override {
    if (osDefaultInit() != TSDB_CODE_SUCCESS) {
      std::fprintf(stderr, "osDefaultInit failed; spill tests require a temporary directory\n");
      std::exit(EXIT_FAILURE);
    }
  }
};

::testing::Environment* const kTempDirEnv =
    ::testing::AddGlobalTestEnvironment(new TempDirEnv());


struct Collector {
  std::multiset<std::string> values;
};

struct SpillSetDeleter {
  void operator()(SSpillDistinctSet* pSet) const { spillDistinctSetDestroy(pSet); }
};

using SpillSetPtr = std::unique_ptr<SSpillDistinctSet, SpillSetDeleter>;

int32_t collect(void* param, const char* pData, int32_t len) {
  auto* pc = static_cast<Collector*>(param);
  pc->values.insert(std::string(pData, len));
  return TSDB_CODE_SUCCESS;
}

// Values are stored as a raw int32 key, mirroring how the aggregate hands over fixed-width data.
int32_t addInt(SSpillDistinctSet* pSet, int32_t v, bool* pIsNew) {
  return spillDistinctSetAdd(pSet, reinterpret_cast<const char*>(&v), sizeof(int32_t), pIsNew);
}

}  // namespace

// Small cardinality stays resident: every add answers authoritatively and finalize agrees.
TEST(spillDistinctSetTest, residentExactMembership) {
  SSpillDistinctSet* pSet = nullptr;
  ASSERT_EQ(spillDistinctSetCreate(&pSet, TSDB_DATA_TYPE_INT, sizeof(int32_t), 1 << 20, false,
                                   "resident"),
            TSDB_CODE_SUCCESS);

  int32_t newCount = 0;
  for (int32_t round = 0; round < 3; ++round) {
    for (int32_t v = 0; v < 500; ++v) {
      bool isNew = false;
      ASSERT_EQ(addInt(pSet, v, &isNew), TSDB_CODE_SUCCESS);
      if (isNew) ++newCount;
    }
  }

  EXPECT_FALSE(spillDistinctSetSpilled(pSet));
  EXPECT_EQ(newCount, 500);  // only the first round reports new values
  EXPECT_EQ(spillDistinctSetResidentCount(pSet), 500);

  Collector c;
  ASSERT_EQ(spillDistinctSetFinalize(pSet, collect, &c), TSDB_CODE_SUCCESS);
  EXPECT_EQ(c.values.size(), 500u);

  spillDistinctSetDestroy(pSet);
}

// A tiny budget forces the spill. Per-add answers stop being authoritative, but finalize must still
// report each distinct value exactly once -- that is the property the whole design rests on.
TEST(spillDistinctSetTest, spilledFinalizeIsExact) {
  SSpillDistinctSet* pSet = nullptr;
  // A small budget against 2048 distinct records guarantees the threshold is crossed early.
  ASSERT_EQ(spillDistinctSetCreate(&pSet, TSDB_DATA_TYPE_INT, sizeof(int32_t), 4096, false,
                                   "spilled"),
            TSDB_CODE_SUCCESS);

  const int32_t kDistinct = 2048;
  // Insert every value three times, in an order that interleaves duplicates across the spill
  // boundary so a value can be written both before and after spilling.
  for (int32_t round = 0; round < 3; ++round) {
    for (int32_t v = 0; v < kDistinct; ++v) {
      bool isNew = false;
      ASSERT_EQ(addInt(pSet, v, &isNew), TSDB_CODE_SUCCESS);
    }
  }

  ASSERT_TRUE(spillDistinctSetSpilled(pSet));

  Collector c;
  ASSERT_EQ(spillDistinctSetFinalize(pSet, collect, &c), TSDB_CODE_SUCCESS);

  // Exactly the distinct values, each reported once.
  EXPECT_EQ(c.values.size(), static_cast<size_t>(kDistinct));
  std::set<std::string> unique(c.values.begin(), c.values.end());
  EXPECT_EQ(unique.size(), static_cast<size_t>(kDistinct));

  // And the values themselves round-trip intact.
  for (int32_t v = 0; v < kDistinct; ++v) {
    std::string key(reinterpret_cast<const char*>(&v), sizeof(int32_t));
    EXPECT_EQ(unique.count(key), 1u) << "missing value " << v;
  }

  spillDistinctSetDestroy(pSet);
}

// Variable-width values must survive the page encoding too.
TEST(spillDistinctSetTest, spilledVarLengthValues) {
  SSpillDistinctSet* pSet = nullptr;
  ASSERT_EQ(spillDistinctSetCreate(&pSet, TSDB_DATA_TYPE_BINARY, 64, 4096, false, "spilledVar"),
            TSDB_CODE_SUCCESS);

  const int32_t kDistinct = 1024;
  for (int32_t round = 0; round < 2; ++round) {
    for (int32_t v = 0; v < kDistinct; ++v) {
      // Deliberately vary the length so records are not uniformly sized.
      std::string s = "value-" + std::to_string(v);
      ASSERT_EQ(spillDistinctSetAdd(pSet, s.data(), (int32_t)s.size(), nullptr),
                TSDB_CODE_SUCCESS);
    }
  }

  ASSERT_TRUE(spillDistinctSetSpilled(pSet));

  Collector c;
  ASSERT_EQ(spillDistinctSetFinalize(pSet, collect, &c), TSDB_CODE_SUCCESS);
  std::set<std::string> unique(c.values.begin(), c.values.end());
  EXPECT_EQ(unique.size(), static_cast<size_t>(kDistinct));
  EXPECT_EQ(unique.count("value-0"), 1u);
  EXPECT_EQ(unique.count("value-1023"), 1u);

  spillDistinctSetDestroy(pSet);
}

// A per-result-row set uses small pages, but a var-type value may be far wider than one. The page
// must grow to fit, not reject the value.
TEST(spillDistinctSetTest, wideValuesExceedDefaultPage) {
  SSpillDistinctSet* pSet = nullptr;
  // 40000-byte declared width against the 4096-byte per-result-row page.
  const int32_t kWidth = 40000;
  ASSERT_EQ(spillDistinctSetCreate(&pSet, TSDB_DATA_TYPE_BINARY, kWidth, 4096, true, "wide"),
            TSDB_CODE_SUCCESS);

  // Three distinct wide values, each added twice.
  std::string a(kWidth, 'a');
  std::string b(kWidth, 'b');
  std::string c(kWidth, 'c');
  for (int round = 0; round < 2; ++round) {
    for (const std::string* s : {&a, &b, &c}) {
      ASSERT_EQ(spillDistinctSetAdd(pSet, s->data(), (int32_t)s->size(), nullptr),
                TSDB_CODE_SUCCESS)
          << "a value of the declared width must be storable";
    }
  }

  ASSERT_TRUE(spillDistinctSetSpilled(pSet));

  Collector col;
  ASSERT_EQ(spillDistinctSetFinalize(pSet, collect, &col), TSDB_CODE_SUCCESS);
  std::set<std::string> unique(col.values.begin(), col.values.end());
  EXPECT_EQ(unique.size(), 3u);
  EXPECT_EQ(unique.count(a), 1u);
  EXPECT_EQ(unique.count(c), 1u);

  spillDistinctSetDestroy(pSet);
}

// Scalar expressions can advertise a width beyond the table-column limit. Runtime VARCHAR values
// are still bounded by their 16-bit length header, so creation must normalize that capacity rather
// than reject the aggregate.
TEST(spillDistinctSetTest, normalizesVariableScalarWidth) {
  SSpillDistinctSet* pSet = nullptr;
  ASSERT_EQ(spillDistinctSetCreate(&pSet, TSDB_DATA_TYPE_VARCHAR, 65544, 4096, true,
                                   "scalarVar"),
            TSDB_CODE_SUCCESS);

  const int32_t maxEncodedBytes = UINT16_MAX + VARSTR_HEADER_SIZE;
  std::string   value(maxEncodedBytes, 'v');
  for (int32_t round = 0; round < 2; ++round) {
    ASSERT_EQ(spillDistinctSetAdd(pSet, value.data(), maxEncodedBytes, nullptr),
              TSDB_CODE_SUCCESS);
  }

  ASSERT_TRUE(spillDistinctSetSpilled(pSet));
  Collector collector;
  ASSERT_EQ(spillDistinctSetFinalize(pSet, collect, &collector), TSDB_CODE_SUCCESS);
  ASSERT_EQ(collector.values.size(), 1u);
  EXPECT_EQ(collector.values.count(value), 1u);

  spillDistinctSetDestroy(pSet);
}

// The finalize pass is itself bounded: a partition too large to dedup in one in-memory hash is
// re-partitioned to disk on the next hash slice and finalized recursively. A tiny budget forces
// that path for every partition, so this exercises the recursion rather than the fast path.
TEST(spillDistinctSetTest, finalizeRepartitionsUnderTinyBudget) {
  SSpillDistinctSet* pSet = nullptr;
  // 512 bytes: crossed almost immediately, both while accumulating and again inside finalize.
  ASSERT_EQ(spillDistinctSetCreate(&pSet, TSDB_DATA_TYPE_INT, sizeof(int32_t), 512, false,
                                   "repartition"),
            TSDB_CODE_SUCCESS);

  const int32_t kDistinct = 2048;
  for (int32_t round = 0; round < 2; ++round) {
    for (int32_t v = 0; v < kDistinct; ++v) {
      bool isNew = false;
      ASSERT_EQ(addInt(pSet, v, &isNew), TSDB_CODE_SUCCESS);
    }
  }
  ASSERT_TRUE(spillDistinctSetSpilled(pSet));

  Collector c;
  ASSERT_EQ(spillDistinctSetFinalize(pSet, collect, &c), TSDB_CODE_SUCCESS);

  // Exactness is the whole point: every distinct value once, no duplicates, none lost.
  EXPECT_EQ(c.values.size(), static_cast<size_t>(kDistinct));
  std::set<std::string> unique(c.values.begin(), c.values.end());
  EXPECT_EQ(unique.size(), static_cast<size_t>(kDistinct));
  for (int32_t v = 0; v < kDistinct; ++v) {
    std::string key(reinterpret_cast<const char*>(&v), sizeof(int32_t));
    EXPECT_EQ(unique.count(key), 1u) << "missing value " << v;
  }

  spillDistinctSetDestroy(pSet);
}

// valueBytes sizes the pages, so an out-of-range width must be refused at creation rather than
// producing a set whose pages cannot hold one record.
TEST(spillDistinctSetTest, rejectsInvalidValueBytes) {
  SSpillDistinctSet* pSet = nullptr;

  EXPECT_EQ(spillDistinctSetCreate(&pSet, TSDB_DATA_TYPE_INT, 0, 4096, false, "zero"),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(pSet, nullptr);

  EXPECT_EQ(spillDistinctSetCreate(&pSet, TSDB_DATA_TYPE_INT, -1, 4096, false, "negative"),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(pSet, nullptr);

  // A fixed-width declaration cannot be normalized from its runtime encoding and must remain
  // strictly rejected before page-size arithmetic.
  EXPECT_EQ(spillDistinctSetCreate(&pSet, TSDB_DATA_TYPE_INT, INT32_MAX, 4096, false, "huge"),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(pSet, nullptr);

  EXPECT_EQ(spillDistinctSetCreate(nullptr, TSDB_DATA_TYPE_INT, sizeof(int32_t), 4096, false, "null"),
            TSDB_CODE_INVALID_PARA);
}

// An empty set finalizes cleanly rather than faulting.
TEST(spillDistinctSetTest, emptySet) {
  SSpillDistinctSet* pSet = nullptr;
  ASSERT_EQ(spillDistinctSetCreate(&pSet, TSDB_DATA_TYPE_INT, sizeof(int32_t), 4096, true, "empty"),
            TSDB_CODE_SUCCESS);

  Collector c;
  ASSERT_EQ(spillDistinctSetFinalize(pSet, collect, &c), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(c.values.empty());
  EXPECT_FALSE(spillDistinctSetSpilled(pSet));

  spillDistinctSetDestroy(pSet);
}

TEST(spillDistinctSetTest, sharedBufferSupportsIndependentSets) {
  SDiskbasedBuf* shared = nullptr;
  ASSERT_EQ(createDiskbasedBuf(&shared, 4096, 32768, "sharedDistinct", tsTempDir),
            TSDB_CODE_SUCCESS);

  SSpillDistinctSet* first = nullptr;
  SSpillDistinctSet* second = nullptr;
  SpillSetPtr firstGuard;
  SpillSetPtr secondGuard;
  ASSERT_EQ(spillDistinctSetCreateWithBuf(&first, TSDB_DATA_TYPE_INT, sizeof(int32_t), 512, true,
                                          "first", &shared),
            TSDB_CODE_SUCCESS);
  firstGuard.reset(first);
  ASSERT_EQ(spillDistinctSetCreateWithBuf(&second, TSDB_DATA_TYPE_INT, sizeof(int32_t), 512, true,
                                          "second", &shared),
            TSDB_CODE_SUCCESS);
  secondGuard.reset(second);

  for (int32_t i = 0; i < 256; ++i) {
    ASSERT_EQ(addInt(first, i, nullptr), TSDB_CODE_SUCCESS);
    ASSERT_EQ(addInt(second, i + 1000, nullptr), TSDB_CODE_SUCCESS);
  }
  ASSERT_TRUE(spillDistinctSetSpilled(first));
  ASSERT_TRUE(spillDistinctSetSpilled(second));

  Collector firstValues;
  Collector secondValues;
  ASSERT_EQ(spillDistinctSetFinalize(first, collect, &firstValues), TSDB_CODE_SUCCESS);
  ASSERT_EQ(spillDistinctSetFinalize(second, collect, &secondValues), TSDB_CODE_SUCCESS);
  EXPECT_EQ(firstValues.values.size(), 256U);
  EXPECT_EQ(secondValues.values.size(), 256U);

  firstGuard.reset();
  secondGuard.reset();
  destroyDiskbasedBuf(shared);
}
