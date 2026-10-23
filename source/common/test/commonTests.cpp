/*
 * Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
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
#include <atomic>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "os.h"

#include "taos.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "tdef.h"
#include "dmRepair.h"
#include "tmisce.h"
#include "ttime.h"
#include "ttokendef.h"
#include "tvariant.h"
#include "tanalytics.h"
#include "tglobal.h"
#include "envelope_crypto.h"

#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>

#include <unistd.h>

#undef close

namespace {

class ScopedTempPath {
 public:
  explicit ScopedTempPath(const std::string &path) : path_(path) {}
  ~ScopedTempPath() {
    if (!path_.empty()) {
      (void)unlink(path_.c_str());
    }
  }

 private:
  std::string path_;
};

std::string createTemporaryRsaPublicKey() {
  EVP_PKEY_CTX *context = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
  if (context == nullptr || EVP_PKEY_keygen_init(context) != 1 ||
      EVP_PKEY_CTX_set_rsa_keygen_bits(context, 2048) != 1) {
    EVP_PKEY_CTX_free(context);
    return {};
  }

  EVP_PKEY *key = nullptr;
  if (EVP_PKEY_keygen(context, &key) != 1) {
    EVP_PKEY_CTX_free(context);
    return {};
  }
  EVP_PKEY_CTX_free(context);

  BIO *bio = BIO_new(BIO_s_mem());
  if (bio == nullptr || PEM_write_bio_PUBKEY(bio, key) != 1) {
    BIO_free(bio);
    EVP_PKEY_free(key);
    return {};
  }

  BUF_MEM *buffer = nullptr;
  BIO_get_mem_ptr(bio, &buffer);
  if (buffer == nullptr || buffer->data == nullptr || buffer->length == 0) {
    BIO_free(bio);
    EVP_PKEY_free(key);
    return {};
  }

  const size_t pem_length = buffer->length;

  char path[] = "/tmp/taos-xnode-public-XXXXXX";
  const int file_descriptor = mkstemp(path);
  if (file_descriptor < 0) {
    BIO_free(bio);
    EVP_PKEY_free(key);
    return {};
  }

  const ssize_t written = write(file_descriptor, buffer->data, buffer->length);
  const int close_result = close(file_descriptor);
  BIO_free(bio);
  EVP_PKEY_free(key);
  if (written != static_cast<ssize_t>(pem_length) || close_result != 0) {
    unlink(path);
    return {};
  }
  return path;
}

SArray *createServerConfigArgs() {
  SArray *args = taosArrayInit(1, sizeof(SConfigPair));
  if (args == nullptr) {
    return nullptr;
  }

  SConfigPair data_dir = {.name = "dataDir", .value = TD_TMP_DIR_PATH};
  if (taosArrayPush(args, &data_dir) == nullptr) {
    taosArrayDestroy(args);
    return nullptr;
  }
  return args;
}

}  // namespace

#ifdef TD_ENTERPRISE
namespace {
//
class ClsConfigDynamicTest : public ::testing::Test {
 protected:
  void SetUp() override {
    taosCleanupCfg();
    tsClsEnabled = false;
    tsClsRefreshInterval = 3600;
    gGrantClsPreRefreshInterval = 3600;
    ASSERT_EQ(cfgInit(&tsCfg), TSDB_CODE_SUCCESS);
    ASSERT_EQ(cfgAddBool(tsCfg, "clsEnabled", tsClsEnabled, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_GLOBAL,
                         CFG_PRIV_SYSTEM),
              TSDB_CODE_SUCCESS);
    ASSERT_EQ(cfgAddInt32(tsCfg, "clsRefreshInterval", tsClsRefreshInterval, 1, 86400, CFG_SCOPE_SERVER,
                          CFG_DYN_SERVER, CFG_CATEGORY_GLOBAL, CFG_PRIV_SYSTEM),
              TSDB_CODE_SUCCESS);
  }

  void TearDown() override { taosCleanupCfg(); }

  void applyServerConfig(const char *name, const char *value, ECfgSrcType stype = CFG_STYPE_ALTER_SERVER_CMD) {
    SConfig *pCfg = taosGetCfg();
    ASSERT_NE(pCfg, nullptr);
    ASSERT_EQ(cfgSetItem(pCfg, name, value, stype, true), TSDB_CODE_SUCCESS);
    ASSERT_EQ(taosCfgDynamicOptions(pCfg, name, true), TSDB_CODE_SUCCESS);
  }
};
}  // namespace
#endif

int main(int argc, char** argv) {
  if (osDefaultInit() != TSDB_CODE_SUCCESS) {
    return 1;
  }
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, colDataAssign_zero_rows_without_source) {
  SColumnInfoData dst = {0};
  dst.info.type = TSDB_DATA_TYPE_INT;
  dst.info.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;

  // Empty scalar results intentionally have no source column to copy.
  EXPECT_EQ(colDataAssign(&dst, NULL, 0, NULL), TSDB_CODE_SUCCESS);
  EXPECT_EQ(colDataAssign(&dst, NULL, 1, NULL), TSDB_CODE_INVALID_PARA);
}

TEST(testCase, colDataAssignNRows_clears_stale_null_bitmap) {
  SColumnInfoData src = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  SColumnInfoData dst = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 2);
  ASSERT_EQ(colInfoDataEnsureCapacity(&src, 1, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(colInfoDataEnsureCapacity(&dst, 1, true), TSDB_CODE_SUCCESS);

  int32_t value = 42;
  ASSERT_EQ(colDataSetVal(&src, 0, (const char*)&value, false), TSDB_CODE_SUCCESS);
  src.hasNull = false;
  colDataSetNItemsNull(&dst, 0, 1);
  ASSERT_TRUE(colDataIsNull_s(&dst, 0));

  ASSERT_EQ(colDataAssignNRows(&dst, 0, &src, 0, 1), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(colDataIsNull_s(&dst, 0));
  EXPECT_EQ(*(int32_t*)dst.pData, value);

  colDataDestroy(&src);
  colDataDestroy(&dst);
}

namespace {

// These literals independently describe the legacy wire contract so an
// implementation layout change remains visible to the compatibility tests.
constexpr size_t kBlockFlagSegmentOffset = sizeof(int32_t) * 4;
constexpr uint32_t kBlockColumnInfoFlag = 1u << 31;
constexpr uint32_t kBlockScanPhaseMask = 3u << 29;

class DataBlockGuard {
 public:
  DataBlockGuard() = default;
  DataBlockGuard(const DataBlockGuard&) = delete;
  DataBlockGuard& operator=(const DataBlockGuard&) = delete;
  ~DataBlockGuard() { blockDataDestroy(pBlock_); }

  SSDataBlock* get() const { return pBlock_; }
  SSDataBlock** put() { return &pBlock_; }
  SSDataBlock* operator->() const { return pBlock_; }

 private:
  SSDataBlock* pBlock_ = nullptr;
};

// Creates a timestamp block tagged with the requested scan phase.
int32_t createScanPhaseTestBlock(int8_t scanPhase,
                                 SSDataBlock** ppBlock) {
  int32_t code = createDataBlock(ppBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SColumnInfoData col =
      createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY),
                           PRIMARYKEY_TIMESTAMP_COL_ID);
  code = blockDataAppendColInfo(*ppBlock, &col);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(*ppBlock);
    *ppBlock = nullptr;
    return code;
  }

  constexpr int32_t kRows = 3;
  code = blockDataEnsureCapacity(*ppBlock, kRows);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(*ppBlock);
    *ppBlock = nullptr;
    return code;
  }

  SColumnInfoData* pCol =
      static_cast<SColumnInfoData*>(taosArrayGet((*ppBlock)->pDataBlock, 0));
  if (pCol == nullptr) {
    code = terrno;
    blockDataDestroy(*ppBlock);
    *ppBlock = nullptr;
    return code;
  }

  TSKEY values[kRows] = {10, 20, 30};
  for (int32_t i = 0; i < kRows; ++i) {
    code = colDataSetVal(pCol, i,
                         reinterpret_cast<const char*>(&values[i]), false);
    if (code != TSDB_CODE_SUCCESS) {
      blockDataDestroy(*ppBlock);
      *ppBlock = nullptr;
      return code;
    }
  }

  (*ppBlock)->info.rows = kRows;
  (*ppBlock)->info.scanPhase = scanPhase;
  return TSDB_CODE_SUCCESS;
}

// Reads the flag segment without assuming the encoded buffer is aligned.
uint32_t getBlockFlagSegment(const std::vector<char>& encoded) {
  uint32_t flagSegment = 0;
  memcpy(&flagSegment, encoded.data() + kBlockFlagSegmentOffset,
         sizeof(flagSegment));
  return flagSegment;
}

// Rewrites the flag segment to emulate a legacy wire layout.
void setBlockFlagSegment(std::vector<char>* encoded,
                         uint32_t flagSegment) {
  memcpy(encoded->data() + kBlockFlagSegmentOffset, &flagSegment,
         sizeof(flagSegment));
}

}  // namespace

// Verifies that new and cleaned blocks default to MAIN, while block-copy APIs
// preserve a source block's non-MAIN phase.
TEST(ScanPhaseTest, DefaultsAndCopies) {
  DataBlockGuard empty;
  ASSERT_EQ(createDataBlock(empty.put()), TSDB_CODE_SUCCESS);
  ASSERT_NE(empty.get(), nullptr);
  EXPECT_EQ(empty->info.scanPhase, SCAN_PHASE_MAIN);

  empty->info.scanPhase = SCAN_PHASE_PREV;
  blockDataCleanup(empty.get());
  EXPECT_EQ(empty->info.scanPhase, SCAN_PHASE_MAIN);

  DataBlockGuard source;
  ASSERT_EQ(createScanPhaseTestBlock(SCAN_PHASE_PREV, source.put()),
            TSDB_CODE_SUCCESS);

  DataBlockGuard created;
  ASSERT_EQ(createOneDataBlock(source.get(), true, created.put()),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(created.get(), nullptr);
  EXPECT_EQ(created->info.scanPhase, SCAN_PHASE_PREV);

  created->info.scanPhase = SCAN_PHASE_MAIN;
  ASSERT_EQ(copyDataBlock(created.get(), source.get()), TSDB_CODE_SUCCESS);
  EXPECT_EQ(created->info.scanPhase, SCAN_PHASE_PREV);

  DataBlockGuard assigned;
  ASSERT_EQ(createDataBlock(assigned.put()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(assignOneDataBlock(assigned.get(), source.get()),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(assigned->info.scanPhase, SCAN_PHASE_PREV);
}

TEST(ScanPhaseTest, ResetRestoresMainPhase) {
  DataBlockGuard block;
  ASSERT_EQ(createScanPhaseTestBlock(SCAN_PHASE_PREV, block.put()),
            TSDB_CODE_SUCCESS);

  blockDataReset(block.get());
  EXPECT_EQ(block->info.scanPhase, SCAN_PHASE_MAIN);
}

// Verifies the internal codec's phase round trip and wire compatibility: the
// column-info bit remains visible to legacy decoders, and legacy blocks with
// zero phase bits decode as MAIN.
TEST(ScanPhaseTest, InternalCodecAndOldFormatCompatibility) {
  DataBlockGuard source;
  ASSERT_EQ(createScanPhaseTestBlock(SCAN_PHASE_PREV, source.put()),
            TSDB_CODE_SUCCESS);

  int32_t encodedSize = blockGetInternalEncodeSize(source.get());
  ASSERT_GT(encodedSize, 0);
  std::vector<char> encoded(encodedSize);
  int32_t encodedLen =
      blockEncodeInternal(source.get(), encoded.data(), encoded.size(), 1);
  ASSERT_GT(encodedLen, 0);

  uint32_t flagSegment = getBlockFlagSegment(encoded);
  EXPECT_EQ(flagSegment >> 31, 1u);
  EXPECT_EQ(flagSegment & kBlockScanPhaseMask,
            (uint32_t)SCAN_PHASE_PREV << 29);

  DataBlockGuard decoded;
  ASSERT_EQ(createOneDataBlock(source.get(), false, decoded.put()),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDecodeInternal(decoded.get(), encoded.data(), nullptr),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(decoded->info.scanPhase, SCAN_PHASE_PREV);

  flagSegment &= ~kBlockScanPhaseMask;
  setBlockFlagSegment(&encoded, flagSegment);
  blockDataCleanup(decoded.get());
  ASSERT_EQ(blockDecodeInternal(decoded.get(), encoded.data(), nullptr),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(decoded->info.scanPhase, SCAN_PHASE_MAIN);
  EXPECT_NE(flagSegment & kBlockColumnInfoFlag, 0u);
}

// Verifies that the row-oriented codec pair carries scan phase independently
// of the internal codec and applies the same legacy zero-to-MAIN fallback.
TEST(ScanPhaseTest, RowCodecRoundTrip) {
  DataBlockGuard source;
  ASSERT_EQ(createScanPhaseTestBlock(SCAN_PHASE_PREV, source.put()),
            TSDB_CODE_SUCCESS);

  int32_t encodedSize = blockGetEncodeSizeOfRows(source.get(), 0, 2);
  ASSERT_GT(encodedSize, 0);
  std::vector<char> encoded(encodedSize);
  int32_t encodedLen = 0;
  ASSERT_EQ(blockEncodeAsRows(source.get(), encoded.data(), encoded.size(),
                              1, 0, 2, &encodedLen),
            TSDB_CODE_SUCCESS);
  ASSERT_GT(encodedLen, 0);

  DataBlockGuard decoded;
  ASSERT_EQ(createOneDataBlock(source.get(), false, decoded.put()),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockSpecialDecodeLaterPart(decoded.get(), encoded.data(), 0, 10,
                                        30),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(decoded->info.scanPhase, SCAN_PHASE_PREV);

  uint32_t flagSegment =
      getBlockFlagSegment(encoded) & ~kBlockScanPhaseMask;
  setBlockFlagSegment(&encoded, flagSegment);
  blockDataCleanup(decoded.get());
  ASSERT_EQ(blockSpecialDecodeLaterPart(decoded.get(), encoded.data(), 0, 10,
                                        30),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(decoded->info.scanPhase, SCAN_PHASE_MAIN);
}

TEST(ScanPhaseTest, InternalCodecRejectsInvalidPhases) {
  constexpr int8_t kReservedPhase = 3;
  DataBlockGuard source;
  ASSERT_EQ(createScanPhaseTestBlock(kReservedPhase, source.put()),
            TSDB_CODE_SUCCESS);

  int32_t encodedSize = blockGetInternalEncodeSize(source.get());
  ASSERT_GT(encodedSize, 0);
  std::vector<char> encoded(encodedSize);
  EXPECT_EQ(blockEncodeInternal(source.get(), encoded.data(), encoded.size(),
                                1),
            -1);
  EXPECT_EQ(terrno, TSDB_CODE_INVALID_PARA);

  source->info.scanPhase = SCAN_PHASE_MAIN;
  ASSERT_GT(blockEncodeInternal(source.get(), encoded.data(), encoded.size(),
                                1),
            0);
  uint32_t flagSegment =
      getBlockFlagSegment(encoded) | kBlockScanPhaseMask;
  setBlockFlagSegment(&encoded, flagSegment);

  DataBlockGuard decoded;
  ASSERT_EQ(createOneDataBlock(source.get(), false, decoded.put()),
            TSDB_CODE_SUCCESS);
  terrno = TSDB_CODE_SUCCESS;
  EXPECT_EQ(blockDecodeInternal(decoded.get(), encoded.data(), nullptr),
            TSDB_CODE_MSG_DECODE_ERROR);
  EXPECT_EQ(terrno, TSDB_CODE_MSG_DECODE_ERROR);
}

TEST(ScanPhaseTest, RowCodecRejectsInvalidPhases) {
  constexpr int8_t kReservedPhase = 3;
  DataBlockGuard source;
  ASSERT_EQ(createScanPhaseTestBlock(kReservedPhase, source.put()),
            TSDB_CODE_SUCCESS);

  int32_t encodedSize = blockGetEncodeSizeOfRows(source.get(), 0, 2);
  ASSERT_GT(encodedSize, 0);
  std::vector<char> encoded(encodedSize);
  int32_t encodedLen = 0;
  terrno = TSDB_CODE_SUCCESS;
  EXPECT_EQ(blockEncodeAsRows(source.get(), encoded.data(), encoded.size(), 1,
                              0, 2, &encodedLen),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(terrno, TSDB_CODE_INVALID_PARA);

  source->info.scanPhase = SCAN_PHASE_MAIN;
  ASSERT_EQ(blockEncodeAsRows(source.get(), encoded.data(), encoded.size(), 1,
                              0, 2, &encodedLen),
            TSDB_CODE_SUCCESS);
  uint32_t flagSegment =
      getBlockFlagSegment(encoded) | kBlockScanPhaseMask;
  setBlockFlagSegment(&encoded, flagSegment);

  DataBlockGuard decoded;
  ASSERT_EQ(createOneDataBlock(source.get(), false, decoded.put()),
            TSDB_CODE_SUCCESS);
  terrno = TSDB_CODE_SUCCESS;
  EXPECT_EQ(blockSpecialDecodeLaterPart(decoded.get(), encoded.data(), 0, 10,
                                        30),
            TSDB_CODE_MSG_DECODE_ERROR);
  EXPECT_EQ(terrno, TSDB_CODE_MSG_DECODE_ERROR);
}

TEST(XnodeDsnConfigTest, RegistersServerOnlyDisabledByDefault) {
  taosCleanupCfg();
  SArray *args = createServerConfigArgs();
  ASSERT_NE(args, nullptr);
  ASSERT_EQ(taosPreLoadCfg("", nullptr, nullptr, nullptr, args, false), TSDB_CODE_SUCCESS);

  SConfigItem *protection = cfgGetItem(taosGetCfg(), "xnodeDsnProtection");
  SConfigItem *public_key = cfgGetItem(taosGetCfg(), "xnodeDsnPublicKeyFile");
  SConfigItem *private_key = cfgGetItem(taosGetCfg(), "xnodedDsnPrivateKeyFile");
  ASSERT_NE(protection, nullptr);
  ASSERT_NE(public_key, nullptr);
  ASSERT_NE(private_key, nullptr);
  EXPECT_EQ(protection->i32, 0);
  EXPECT_EQ(protection->scope, CFG_SCOPE_SERVER);
  EXPECT_EQ(protection->dynScope, CFG_DYN_NONE);
  EXPECT_EQ(public_key->scope, CFG_SCOPE_SERVER);
  EXPECT_EQ(public_key->dynScope, CFG_DYN_NONE);
  EXPECT_EQ(public_key->category, CFG_CATEGORY_LOCAL);
  EXPECT_EQ(private_key->scope, CFG_SCOPE_SERVER);
  EXPECT_EQ(private_key->dynScope, CFG_DYN_NONE);
  EXPECT_EQ(private_key->category, CFG_CATEGORY_LOCAL);
  EXPECT_STREQ(public_key->str, "");
  EXPECT_STREQ(private_key->str, "");
  taosArrayDestroy(args);
  taosCleanupCfg();
}

TEST(XnodeDsnConfigTest, ProtectionAcceptsOnlyZeroOrOne) {
  taosCleanupCfg();
  SArray *args = createServerConfigArgs();
  ASSERT_NE(args, nullptr);
  ASSERT_EQ(taosPreLoadCfg("", nullptr, nullptr, nullptr, args, false), TSDB_CODE_SUCCESS);
  SConfig *config = taosGetCfg();
  ASSERT_NE(config, nullptr);
  EXPECT_EQ(cfgSetItem(config, "xnodeDsnProtection", "1", CFG_STYPE_CFG_FILE, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(cfgSetItem(config, "xnodeDsnProtection", "2", CFG_STYPE_CFG_FILE, true), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgSetItem(config, "xnodeDsnProtection", "true", CFG_STYPE_CFG_FILE, true),
            TSDB_CODE_INVALID_CFG_VALUE);
  taosArrayDestroy(args);
  taosCleanupCfg();
}

TEST(XnodeDsnConfigTest, DisabledProtectionDoesNotReadPublicKey) {
  taosCleanupCfg();
  SArray *args = createServerConfigArgs();
  ASSERT_NE(args, nullptr);
  ASSERT_EQ(taosPreLoadCfg("", nullptr, nullptr, nullptr, args, false), TSDB_CODE_SUCCESS);
  SConfig *config = taosGetCfg();
  ASSERT_NE(config, nullptr);
  ASSERT_EQ(cfgSetItem(config, "xnodeDsnPublicKeyFile", "/missing/xnode-public.pem", CFG_STYPE_CFG_FILE, true),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosApplyCfg("", nullptr, nullptr, nullptr, args, false), TSDB_CODE_SUCCESS);
  taosArrayDestroy(args);
  taosCleanupCfg();
}

TEST(XnodeDsnConfigTest, RejectsPathsThatDoNotFitGlobalBuffers) {
  taosCleanupCfg();
  SArray *args = createServerConfigArgs();
  ASSERT_NE(args, nullptr);
  ASSERT_EQ(taosPreLoadCfg("", nullptr, nullptr, nullptr, args, false), TSDB_CODE_SUCCESS);
  SConfig *config = taosGetCfg();
  ASSERT_NE(config, nullptr);

  const std::string oversized_path(PATH_MAX, 'a');
  ASSERT_EQ(cfgSetItem(config, "xnodeDsnPublicKeyFile", oversized_path.c_str(), CFG_STYPE_CFG_FILE, true),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosApplyCfg("", nullptr, nullptr, nullptr, args, false), TSDB_CODE_INVALID_CFG_VALUE);

  taosArrayDestroy(args);
  taosCleanupCfg();
}

TEST(XnodeDsnConfigTest, EnabledProtectionRejectsMissingPublicKey) {
  taosCleanupCfg();
  SArray *args = createServerConfigArgs();
  ASSERT_NE(args, nullptr);
  ASSERT_EQ(taosPreLoadCfg("", nullptr, nullptr, nullptr, args, false), TSDB_CODE_SUCCESS);
  SConfig *config = taosGetCfg();
  ASSERT_NE(config, nullptr);
  ASSERT_EQ(cfgSetItem(config, "xnodeDsnProtection", "1", CFG_STYPE_CFG_FILE, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(cfgSetItem(config, "xnodeDsnPublicKeyFile", "/missing/xnode-public.pem", CFG_STYPE_CFG_FILE, true),
            TSDB_CODE_SUCCESS);
  EXPECT_NE(taosApplyCfg("", nullptr, nullptr, nullptr, args, false), TSDB_CODE_SUCCESS);
  taosArrayDestroy(args);
  taosCleanupCfg();
}

TEST(XnodeDsnConfigTest, EnabledProtectionLoadsValidPublicKey) {
  const std::string public_key_path = createTemporaryRsaPublicKey();
  ScopedTempPath cleanup(public_key_path);
  ASSERT_FALSE(public_key_path.empty());

  taosCleanupCfg();
  SArray *args = createServerConfigArgs();
  ASSERT_NE(args, nullptr);
  ASSERT_EQ(taosPreLoadCfg("", nullptr, nullptr, nullptr, args, false), TSDB_CODE_SUCCESS);
  SConfig *config = taosGetCfg();
  ASSERT_NE(config, nullptr);
  ASSERT_EQ(cfgSetItem(config, "xnodeDsnProtection", "1", CFG_STYPE_CFG_FILE, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(cfgSetItem(config, "xnodeDsnPublicKeyFile", public_key_path.c_str(), CFG_STYPE_CFG_FILE, true),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosApplyCfg("", nullptr, nullptr, nullptr, args, false), TSDB_CODE_SUCCESS);
  EXPECT_NE(taosGetXnodeDsnPublicKey(), nullptr);
  taosArrayDestroy(args);
  taosCleanupCfg();
}

TEST(XnodeDsnConfigTest, LaterConfigSyncKeepsStartupPublicKey) {
  const std::string public_key_path = createTemporaryRsaPublicKey();
  ScopedTempPath cleanup(public_key_path);
  ASSERT_FALSE(public_key_path.empty());

  taosCleanupCfg();
  SArray *args = createServerConfigArgs();
  ASSERT_NE(args, nullptr);
  ASSERT_EQ(taosPreLoadCfg("", nullptr, nullptr, nullptr, args, false), TSDB_CODE_SUCCESS);
  SConfig *config = taosGetCfg();
  ASSERT_NE(config, nullptr);
  ASSERT_EQ(cfgSetItem(config, "xnodeDsnProtection", "1", CFG_STYPE_CFG_FILE, true), TSDB_CODE_SUCCESS);
  ASSERT_EQ(cfgSetItem(config, "xnodeDsnPublicKeyFile", public_key_path.c_str(), CFG_STYPE_CFG_FILE, true),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(cfgSetItem(config, "xnodedDsnPrivateKeyFile", "/startup/private-key.pem", CFG_STYPE_CFG_FILE, true),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosApplyCfg("", nullptr, nullptr, nullptr, args, false), TSDB_CODE_SUCCESS);

  const SEnvelopePublicKey *startup_key = taosGetXnodeDsnPublicKey();
  ASSERT_NE(startup_key, nullptr);
  ASSERT_EQ(unlink(public_key_path.c_str()), 0);
  ASSERT_EQ(cfgSetItem(config, "xnodeDsnPublicKeyFile", "/missing/reloaded-key.pem", CFG_STYPE_CFG_FILE, true),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(cfgSetItem(config, "xnodedDsnPrivateKeyFile", "/reloaded/private-key.pem", CFG_STYPE_CFG_FILE, true),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(setAllConfigs(config), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosGetXnodeDsnPublicKey(), startup_key);
  EXPECT_EQ(tsXnodeDsnProtection, 1);
  EXPECT_STREQ(tsXnodeDsnPublicKeyFile, public_key_path.c_str());
  EXPECT_STREQ(tsXnodedDsnPrivateKeyFile, "/startup/private-key.pem");

  taosArrayDestroy(args);
  taosCleanupCfg();
}

TEST(testCase, blockDataUpdateHasNull_preserves_existing_flags) {
  for (int32_t type : {TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_VARCHAR}) {
    SCOPED_TRACE(type);
    SSDataBlock* pBlock = nullptr;
    ASSERT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
    std::unique_ptr<SSDataBlock, decltype(&blockDataDestroy)> block(
        pBlock, blockDataDestroy);

    int32_t number = 42;
    char text[VARSTR_HEADER_SIZE + 3] = {};
    varDataSetLen(text, 3);
    memcpy(varDataVal(text), "abc", 3);
    int32_t bytes = type == TSDB_DATA_TYPE_INT ? sizeof(number) : sizeof(text);
    auto col = createColumnInfoData(type, bytes, 0);
    ASSERT_EQ(blockDataAppendColInfo(pBlock, &col), TSDB_CODE_SUCCESS);
    ASSERT_EQ(blockDataEnsureCapacity(pBlock, 1), TSDB_CODE_SUCCESS);
    auto* pCol = static_cast<SColumnInfoData*>(
        taosArrayGet(pBlock->pDataBlock, 0));
    ASSERT_NE(pCol, nullptr);
    const char* value = type == TSDB_DATA_TYPE_INT ?
                        reinterpret_cast<const char*>(&number) : text;
    ASSERT_EQ(colDataSetVal(pCol, 0, value, false), TSDB_CODE_SUCCESS);
    pBlock->info.rows = 1;

    // A conservative flag may remain after all NULL rows have been removed.
    for (bool hasNull : {false, true}) {
      SCOPED_TRACE(hasNull);
      pCol->hasNull = hasNull;
      blockDataUpdateHasNull(pBlock);
      EXPECT_EQ(pCol->hasNull, hasNull);
      EXPECT_FALSE(colDataIsNull_s(pCol, 0));
    }
  }
}

TEST(testCase, toUIntegerEx_test) {
  uint64_t val = 0;

  char*   s = "123";
  int32_t ret = toUIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 123);

  s = "1000u";
  ret = toUIntegerEx(s, strlen(s), 0, &val);
  ASSERT_EQ(ret, -1);

  s = "0x1f";
  ret = toUIntegerEx(s, strlen(s), TK_NK_HEX, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 31);

  s = "0b110";
  ret = toUIntegerEx(s, strlen(s), 0, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 6);

  s = "2567.4787";
  ret = toUIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 2567);

  s = "1.869895343e4";
  ret = toUIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 18699);

  s = "-1";
  ret = toUIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, -1);

  s = "-0b10010";
  ret = toUIntegerEx(s, strlen(s), 0, &val);
  ASSERT_EQ(ret, -1);

  s = "-0x40";
  ret = toUIntegerEx(s, strlen(s), TK_NK_HEX, &val);
  ASSERT_EQ(ret, -1);

  s = "-80.9999";
  ret = toUIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, -1);

  s = "-5.2343544534e10";
  ret = toUIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, -1);

  // INT64_MAX
  s = "9223372036854775807";
  ret = toUIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 9223372036854775807);

  // UINT64_MAX
  s = "18446744073709551615";
  ret = toUIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 18446744073709551615u);

  // out of range
  s = "18446744073709551616";
  ret = toUIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, -1);

  s = "5.23e25";
  ret = toUIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, -1);
}

TEST(testCase, toIntegerEx_test) {
  int64_t val = 0;

  char*   s = "123";
  int32_t ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 123);

  s = "-1";
  ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -1);

  s = "1000u";
  ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, -1);

  s = "0x1f";
  ret = toIntegerEx(s, strlen(s), TK_NK_HEX, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 31);

  s = "-0x40";
  ret = toIntegerEx(s, strlen(s), TK_NK_HEX, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -64);

  s = "0b110";
  ret = toIntegerEx(s, strlen(s), TK_NK_BIN, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 6);

  s = "-0b10010";
  ret = toIntegerEx(s, strlen(s), 0, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -18);

  s = "-80.9999";
  ret = toIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -81);

  s = "2567.8787";
  ret = toIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 2568);

  s = "-5.2343544534e10";
  ret = toIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -52343544534);

  s = "1.869895343e4";
  ret = toIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 18699);

  // INT64_MAX
  s = "9223372036854775807";
  ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 9223372036854775807LL);

  s = "-9223372036854775808";
  ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, 0);
  // ASSERT_EQ(val, -9223372036854775808);

  // out of range
  s = "9323372036854775807";
  ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, -1);

  s = "-9323372036854775807";
  ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, -1);

  // UINT64_MAX
  s = "18446744073709551615";
  ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, -1);
}

TEST(testCase, toInteger_test) {
  int64_t val = 0;

  char*   s = "123";
  int32_t ret = toInteger(s, strlen(s), 10, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 123);

  s = "-1";
  ret = toInteger(s, strlen(s), 10, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -1);

  s = "1000u";
  ret = toInteger(s, strlen(s), 10, &val);
  ASSERT_EQ(ret, -1);

  s = "0x10";
  ret = toInteger(s, strlen(s), 16, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 16);

  s = "110";
  ret = toInteger(s, strlen(s), 2, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 6);

  s = "110";
  ret = toInteger(s, strlen(s), 8, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 72);

  s = "9223372036854775807";
  ret = toInteger(s, strlen(s), 10, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 9223372036854775807);

  s = "-9223372036854775808";
  ret = toInteger(s, strlen(s), 10, &val);
  ASSERT_EQ(ret, 0);
  // ASSERT_EQ(val, -9223372036854775808);

  // out of range
  s = "9323372036854775807";
  ret = toInteger(s, strlen(s), 10, &val);
  ASSERT_EQ(ret, -1);

  s = "-9323372036854775807";
  ret = toInteger(s, strlen(s), 10, &val);
  ASSERT_EQ(ret, -1);
}

#ifdef TD_ENTERPRISE
TEST_F(ClsConfigDynamicTest, clsEnabledTransitionKeepsLastExplicitRefreshInterval) {
  ASSERT_EQ(tsClsEnabled, false);
  ASSERT_EQ(tsClsRefreshInterval, 3600);

  applyServerConfig("clsRefreshInterval", "30");
  ASSERT_EQ(tsClsRefreshInterval, 30);

  applyServerConfig("clsEnabled", "1");
  ASSERT_EQ(tsClsEnabled, true);
  ASSERT_EQ(gGrantClsPreRefreshInterval, 30);
  ASSERT_EQ(tsClsRefreshInterval, 2);

  applyServerConfig("clsEnabled", "0");
  ASSERT_EQ(tsClsEnabled, false);
  ASSERT_EQ(gGrantClsPreRefreshInterval, 30);
  ASSERT_EQ(tsClsRefreshInterval, 1);

  applyServerConfig("clsEnabled", "1");
  ASSERT_EQ(tsClsEnabled, true);
  ASSERT_EQ(gGrantClsPreRefreshInterval, 30);
  ASSERT_EQ(tsClsRefreshInterval, 2);

  applyServerConfig("clsRefreshInterval", "40");
  ASSERT_EQ(tsClsRefreshInterval, 40);

  applyServerConfig("clsEnabled", "0");
  ASSERT_EQ(tsClsEnabled, false);
  ASSERT_EQ(gGrantClsPreRefreshInterval, 40);
  ASSERT_EQ(tsClsRefreshInterval, 1);
}
#endif

TEST(testCase, dmRepairDefaultsToNoWalRepair) {
  ASSERT_FALSE(dmRepairNeedWalRepair(123));
}

TEST(testCase, Datablock_test_inc) {
  {
    SColumnInfoData cinfo = {0};
    uint32_t        row = 0;

    bool ret = colDataIsNull_s(&cinfo, row);
    EXPECT_EQ(ret, false);

    cinfo.hasNull = 1;
    cinfo.info.type = TSDB_DATA_TYPE_INT;
    ret = colDataIsNull_s(&cinfo, row);
    EXPECT_EQ(ret, false);
  }

  {
    SColumnInfoData cinfo = {0};
    uint32_t        row = 0;
    bool            isVarType = false;

    bool ret = colDataIsNull_t(&cinfo, row, isVarType);
    EXPECT_EQ(ret, false);

    cinfo.hasNull = 1;
    ret = colDataIsNull_t(&cinfo, row, isVarType);
    EXPECT_EQ(ret, false);
  }

  {
    SColumnInfoData cinfo = {0};
    uint32_t        totalRows = 0;
    uint32_t        row = 0;
    SColumnDataAgg  colAgg = {0};

    bool ret = colDataIsNull(&cinfo, totalRows, row, &colAgg);
    EXPECT_EQ(ret, false);

    cinfo.hasNull = 1;
    ret = colDataIsNull(&cinfo, totalRows, row, &colAgg);
    EXPECT_EQ(ret, true);

    totalRows = 1;
    ret = colDataIsNull(&cinfo, totalRows, row, &colAgg);
    EXPECT_EQ(ret, false);

    colAgg.colId = -1;
    cinfo.info.type = TSDB_DATA_TYPE_INT;
    ret = colDataIsNull(&cinfo, totalRows, row, &colAgg);
    EXPECT_EQ(ret, false);
  }
}

TEST(testCase, Datablock_test) {
  DataBlockGuard b;
  int32_t code = createDataBlock(b.put());
  ASSERT(code == 0);

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_INT, 4, 1);
  taosArrayPush(b->pDataBlock, &infoData);
  blockDataAppendColInfo(b.get(), &infoData);

  SColumnInfoData infoData1 = createColumnInfoData(TSDB_DATA_TYPE_BINARY, 40, 2);
  blockDataAppendColInfo(b.get(), &infoData1);
  blockDataEnsureCapacity(b.get(), 40);

  char* str = "the value of: %d";
  char  buf[128] = {0};
  char  varbuf[128] = {0};

  for (int32_t i = 0; i < 40; ++i) {
    SColumnInfoData* p0 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 0);
    SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 1);

    if (i & 0x01) {
      int32_t len = sprintf(buf, str, i);
      STR_TO_VARSTR(varbuf, buf)
      colDataSetVal(p0, i, (const char*)&i, false);
      colDataSetVal(p1, i, (const char*)varbuf, false);

      memset(varbuf, 0, sizeof(varbuf));
      memset(buf, 0, sizeof(buf));
    } else {
      colDataSetVal(p0, i, (const char*)&i, true);
      colDataSetVal(p1, i, (const char*)varbuf, true);
    }

    b->info.rows++;
  }

  SColumnInfoData* p0 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 0);
  SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 1);
  for (int32_t i = 0; i < 40; ++i) {
    if (i & 0x01) {
      ASSERT_EQ(colDataIsNull_f(p0, i), false);
      ASSERT_EQ(colDataIsNull(p1, b->info.rows, i, nullptr), false);
    } else {
      ASSERT_EQ(colDataIsNull_f(p0, i), true);

      ASSERT_EQ(colDataIsNull(p0, b->info.rows, i, nullptr), true);
      ASSERT_EQ(colDataIsNull(p1, b->info.rows, i, nullptr), true);
    }
  }

  printf("binary column length:%d\n", *(int32_t*)p1->pData);

  ASSERT_EQ(blockDataGetNumOfCols(b.get()), 3);
  ASSERT_EQ(blockDataGetNumOfRows(b.get()), 40);

  char* pData = colDataGetData(p1, 3);
  printf("the second row of binary:%s, length:%d\n", (char*)varDataVal(pData), varDataLen(pData));

  SArray*         pOrderInfo = taosArrayInit(3, sizeof(SBlockOrderInfo));
  SBlockOrderInfo order = {true, TSDB_ORDER_ASC, 0, NULL};
  taosArrayPush(pOrderInfo, &order);

  blockDataSort(b.get(), pOrderInfo);

  taosArrayDestroy(pOrderInfo);
}

TEST(testCase, DatablockSmaCleanup_test) {
  SSDataBlock* b = NULL;
  ASSERT_EQ(createDataBlock(&b), TSDB_CODE_SUCCESS);

  SColumnInfoData fixedCol = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  SColumnInfoData varCol = createColumnInfoData(TSDB_DATA_TYPE_BINARY, 40, 2);
  ASSERT_EQ(blockDataAppendColInfo(b, &fixedCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(b, &varCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(b, 16), TSDB_CODE_SUCCESS);

  SColumnInfoData* pFixedCol = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 0);
  SColumnInfoData* pVarCol = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 1);
  ASSERT_NE(pFixedCol, nullptr);
  ASSERT_NE(pVarCol, nullptr);

  pFixedCol->nullbitmap[0] = 0xff;
  pFixedCol->nullbitmap[1] = 0x5a;
  pFixedCol->hasNull = true;
  pVarCol->varmeta.offset[0] = 123;
  pVarCol->varmeta.offset[1] = 456;
  pVarCol->varmeta.length = 32;
  pVarCol->hasNull = true;
  pVarCol->reassigned = true;
  SColumnDataAgg* pBlockAggBuf = (SColumnDataAgg*)taosMemoryCalloc(2, sizeof(SColumnDataAgg));
  b->pBlockAggBuf = pBlockAggBuf;
  ASSERT_NE(b->pBlockAggBuf, nullptr);
  b->pBlockAggBufSize = 2;
  b->pBlockAgg = b->pBlockAggBuf;
  b->info.rows = 1;
  b->info.smaSourceRows = 10;

  blockDataEmpty(b);

  EXPECT_EQ(b->info.capacity, 16);
  EXPECT_EQ(b->info.rows, 0);
  EXPECT_EQ(b->info.smaSourceRows, 0);
  EXPECT_EQ(b->pBlockAgg, nullptr);
  EXPECT_EQ(b->pBlockAggBuf, pBlockAggBuf);
  EXPECT_FALSE(pFixedCol->hasNull);
  EXPECT_EQ((uint8_t)pFixedCol->nullbitmap[0], 0xff);
  EXPECT_EQ((uint8_t)pFixedCol->nullbitmap[1], 0x5a);
  EXPECT_FALSE(pVarCol->hasNull);
  EXPECT_FALSE(pVarCol->reassigned);
  EXPECT_EQ(pVarCol->varmeta.length, 0);
  EXPECT_EQ(pVarCol->varmeta.offset[0], 123);
  EXPECT_EQ(pVarCol->varmeta.offset[1], 456);
  EXPECT_EQ(pVarCol->varmeta.length, 0);
  EXPECT_FALSE(pVarCol->hasNull);

  blockDataDestroy(b);
}

static SSDataBlock* createRecycleTestBlock(uint32_t capacity) {
  SSDataBlock* pBlock = nullptr;
  if (createDataBlock(&pBlock) != TSDB_CODE_SUCCESS) {
    return nullptr;
  }

  SColumnInfoData column = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  if (blockDataAppendColInfo(pBlock, &column) != TSDB_CODE_SUCCESS ||
      blockDataEnsureCapacity(pBlock, capacity) != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return nullptr;
  }

  pBlock->info.rows = capacity;
  return pBlock;
}

TEST(testCase, DatablockRecycleExpiration_test) {
  constexpr uint32_t kRecycleCapacity = 100001;

  blockDataRecycleCleanup();
  blockDataRecycleSetCurrentTime(1000);

  SSDataBlock* pRecycled = createRecycleTestBlock(kRecycleCapacity);
  ASSERT_NE(pRecycled, nullptr);
  pRecycled->recycleEnabled = true;
  blockDataRecycle(pRecycled);

  SSDataBlock* pTemplate = createRecycleTestBlock(kRecycleCapacity);
  ASSERT_NE(pTemplate, nullptr);
  SSDataBlock* pResult = nullptr;
  bool         reused = false;

  blockDataRecycleTrimExpired(1000 + BLOCK_RECYCLE_TIMEOUT_MS - 1);
  ASSERT_EQ(createOneDataBlockFromRecycle(pTemplate, &pResult, &reused), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(reused);
  blockDataRecycle(pResult);

  blockDataRecycleTrimExpired(1000 + BLOCK_RECYCLE_TIMEOUT_MS);
  pResult = nullptr;
  reused = true;
  ASSERT_EQ(createOneDataBlockFromRecycle(pTemplate, &pResult, &reused), TSDB_CODE_SUCCESS);
  ASSERT_FALSE(reused);
  blockDataDestroy(pResult);

  blockDataDestroy(pTemplate);
  blockDataRecycleCleanup();
}

struct SBlockRecycleRaceContext {
  const SSDataBlock*    pTemplate = nullptr;
  std::atomic<int32_t>  errorCode{TSDB_CODE_SUCCESS};
  std::atomic<int32_t>  ready{0};
  std::atomic<bool>     start{false};
};

static void setBlockRecycleRaceError(SBlockRecycleRaceContext* pContext) {
  int32_t expected = TSDB_CODE_SUCCESS;
  pContext->errorCode.compare_exchange_strong(expected, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
}

static void waitForBlockRecycleRaceStart(SBlockRecycleRaceContext* pContext) {
  pContext->ready.fetch_add(1);
  while (!pContext->start.load()) {
    std::this_thread::yield();
  }
}

static void recycleBlocksConcurrently(SBlockRecycleRaceContext* pContext) {
  constexpr uint32_t kRecycleCapacity = 100001;
  waitForBlockRecycleRaceStart(pContext);

  for (int32_t i = 0; i < 128; ++i) {
    if (pContext->errorCode.load() != TSDB_CODE_SUCCESS) {
      return;
    }

    SSDataBlock* pBlock = createRecycleTestBlock(kRecycleCapacity);
    if (pBlock == nullptr) {
      setBlockRecycleRaceError(pContext);
      return;
    }

    pBlock->recycleEnabled = true;
    blockDataRecycle(pBlock);
  }
}

static void takeRecycledBlocksConcurrently(SBlockRecycleRaceContext* pContext) {
  waitForBlockRecycleRaceStart(pContext);

  for (int32_t i = 0; i < 128; ++i) {
    if (pContext->errorCode.load() != TSDB_CODE_SUCCESS) {
      return;
    }

    SSDataBlock* pBlock = nullptr;
    bool         reused = false;
    if (createOneDataBlockFromRecycle(pContext->pTemplate, &pBlock, &reused) != TSDB_CODE_SUCCESS || pBlock == nullptr) {
      setBlockRecycleRaceError(pContext);
      return;
    }

    blockDataRecycle(pBlock);
  }
}

static void trimRecycledBlocksConcurrently(SBlockRecycleRaceContext* pContext) {
  waitForBlockRecycleRaceStart(pContext);

  for (int32_t i = 0; i < 128; ++i) {
    if (pContext->errorCode.load() != TSDB_CODE_SUCCESS) {
      return;
    }

    blockDataRecycleTrimExpired(1000 + BLOCK_RECYCLE_TIMEOUT_MS + i);
  }
}

TEST(testCase, DatablockRecycleConcurrentTrim_test) {
  constexpr uint32_t kRecycleCapacity = 100001;

  blockDataRecycleCleanup();
  blockDataRecycleSetCurrentTime(1000);

  SSDataBlock* pTemplate = createRecycleTestBlock(kRecycleCapacity);
  ASSERT_NE(pTemplate, nullptr);

  SBlockRecycleRaceContext context;
  context.pTemplate = pTemplate;
  std::thread              recycleThread(recycleBlocksConcurrently, &context);
  std::thread              takeThread(takeRecycledBlocksConcurrently, &context);
  std::thread              trimThread(trimRecycledBlocksConcurrently, &context);
  while (context.ready.load() != 3) {
    std::this_thread::yield();
  }
  context.start.store(true);

  recycleThread.join();
  takeThread.join();
  trimThread.join();

  EXPECT_EQ(context.errorCode.load(), TSDB_CODE_SUCCESS);
  blockDataRecycleCleanup();
  blockDataDestroy(pTemplate);
}

TEST(testCase, DatablockReassignedVarColumn_test) {
  SSDataBlock* src = NULL;
  SSDataBlock* dst = NULL;
  ASSERT_EQ(createDataBlock(&src), TSDB_CODE_SUCCESS);
  ASSERT_EQ(createDataBlock(&dst), TSDB_CODE_SUCCESS);

  SColumnInfoData srcCol = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 32, 1);
  SColumnInfoData dstCol = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 32, 1);
  ASSERT_EQ(blockDataAppendColInfo(src, &srcCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(dst, &dstCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(src, 4), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(dst, 4), TSDB_CODE_SUCCESS);

  char value[32] = {0};
  STR_TO_VARSTR(value, "table_name")
  SColumnInfoData* pSrcCol = (SColumnInfoData*)taosArrayGet(src->pDataBlock, 0);
  SColumnInfoData* pDstCol = (SColumnInfoData*)taosArrayGet(dst->pDataBlock, 0);
  ASSERT_NE(pSrcCol, nullptr);
  ASSERT_NE(pDstCol, nullptr);
  ASSERT_EQ(colDataCopyNItems(pSrcCol, 0, value, 4, false), TSDB_CODE_SUCCESS);
  src->info.rows = 4;
  ASSERT_TRUE(pSrcCol->reassigned);
  ASSERT_EQ(pSrcCol->varmeta.length, varDataTLen(value));

  // A var record is length prefixed and is NOT NUL terminated, so strcmp() would read one byte
  // past the record. Compare the payload by its declared length instead.
  auto checkColumnValue = [](const SColumnInfoData* pCol, const char* expected) {
    const int32_t expectLen = (int32_t)strlen(expected);
    for (int32_t i = 0; i < 4; ++i) {
      const char* pVal = (const char*)colDataGetData(pCol, i);
      ASSERT_NE(pVal, nullptr) << "row " << i;
      ASSERT_EQ((int32_t)varDataLen(pVal), expectLen) << "row " << i;
      EXPECT_EQ(memcmp(varDataVal(pVal), expected, expectLen), 0) << "row " << i;
    }
  };

  checkColumnValue(pSrcCol, "table_name");

  ASSERT_EQ(colDataAssignNRows(pDstCol, 0, pSrcCol, 0, 4), TSDB_CODE_SUCCESS);
  dst->info.rows = 4;
  checkColumnValue(pDstCol, "table_name");

  ASSERT_EQ(colDataAssign(pDstCol, pSrcCol, 4, &src->info), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(pDstCol->reassigned);
  checkColumnValue(pDstCol, "table_name");

  colInfoDataCleanup(pDstCol, dst->info.capacity);
  ASSERT_FALSE(pDstCol->reassigned);
  blockDataDestroy(src);
  blockDataDestroy(dst);
}

#if 0
TEST(testCase, non_var_dataBlock_split_test) {
  SSDataBlock* b = static_cast<SSDataBlock*>(taosMemoryCalloc(1, sizeof(SSDataBlock)));
  b->info.numOfCols = 2;
  b->pDataBlock = taosArrayInit(4, sizeof(SColumnInfoData));

  SColumnInfoData infoData = {0};
  infoData.info.bytes = 4;
  infoData.info.type = TSDB_DATA_TYPE_INT;
  infoData.info.colId = 1;

  int32_t numOfRows = 1000000;

  infoData.pData = (char*) taosMemoryCalloc(numOfRows, infoData.info.bytes);
  infoData.nullbitmap = (char*) taosMemoryCalloc(1, sizeof(char) * (numOfRows/8));
  taosArrayPush(b->pDataBlock, &infoData);

  SColumnInfoData infoData1 = {0};
  infoData1.info.bytes = 1;
  infoData1.info.type = TSDB_DATA_TYPE_TINYINT;
  infoData1.info.colId = 2;

  infoData1.pData = (char*) taosMemoryCalloc(numOfRows, infoData.info.bytes);
  infoData1.nullbitmap = (char*) taosMemoryCalloc(1, sizeof(char) * (numOfRows/8));
  taosArrayPush(b->pDataBlock, &infoData1);

  for(int32_t i = 0; i < numOfRows; ++i) {
    SColumnInfoData* p0 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 0);
    SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 1);

    int8_t v = i;
    colDataSetVal(p0, i, (const char*)&i, false);
    colDataSetVal(p1, i, (const char*)&v, false);
    b->info.rows++;
  }

  int32_t pageSize = 64 * 1024;

  int32_t startIndex= 0;
  int32_t stopIndex = 0;
  int32_t count = 1;
  while(1) {
    blockDataSplitRows(b, false, startIndex, &stopIndex, pageSize);
    printf("the %d split, from: %d to %d\n", count++, startIndex, stopIndex);

    if (stopIndex == numOfRows - 1) {
      break;
    }

    startIndex = stopIndex + 1;
  }

}

#endif

TEST(testCase, var_dataBlock_split_test) {
  int32_t numOfRows = 1000000;

  DataBlockGuard b;
  int32_t code = createDataBlock(b.put());
  ASSERT(code == 0);

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_INT, 4, 1);
  blockDataAppendColInfo(b.get(), &infoData);

  SColumnInfoData infoData1 = createColumnInfoData(TSDB_DATA_TYPE_BINARY, 40, 2);
  blockDataAppendColInfo(b.get(), &infoData1);

  blockDataEnsureCapacity(b.get(), numOfRows);

  char buf[41] = {0};
  char buf1[100] = {0};

  for (int32_t i = 0; i < numOfRows; ++i) {
    SColumnInfoData* p0 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 0);
    SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 1);

    int8_t v = i;
    colDataSetVal(p0, i, (const char*)&i, false);

    sprintf(buf, "the number of row:%d", i);
    int32_t len = sprintf(buf1, buf, i);
    STR_TO_VARSTR(buf1, buf)
    colDataSetVal(p1, i, buf1, false);
    b->info.rows++;

    memset(buf, 0, sizeof(buf));
    memset(buf1, 0, sizeof(buf1));
  }

  int32_t pageSize = 64 * 1024;

  int32_t startIndex = 0;
  int32_t stopIndex = 0;
  int32_t count = 1;
  while (1) {
    blockDataSplitRows(b.get(), true, startIndex, &stopIndex, pageSize);
    printf("the %d split, from: %d to %d\n", count++, startIndex, stopIndex);

    if (stopIndex == numOfRows - 1) {
      break;
    }

    startIndex = stopIndex + 1;
  }
}

// The var data of a block is not guaranteed to be stored in row order: the stream wal reader
// appends the rows of each table slice in the write order, which may differ from the row index
// order. offset[endIndex + 1] - offset[startIndex] is negative in that case, and used to be
// propagated into blockEncodeAsRows() as a huge size_t length, crashing the memmove.
TEST(testCase, blockEncodeAsRowsWithOutOfOrderVarData) {
  int32_t      numOfRows = 12;
  SSDataBlock* b = NULL;

  ASSERT_EQ(createDataBlock(&b), 0);

  SColumnInfoData tsCol = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, 8, 1);
  ASSERT_EQ(blockDataAppendColInfo(b, &tsCol), TSDB_CODE_SUCCESS);
  SColumnInfoData varCol = createColumnInfoData(TSDB_DATA_TYPE_BINARY, 32, 2);
  ASSERT_EQ(blockDataAppendColInfo(b, &varCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(b, numOfRows), TSDB_CODE_SUCCESS);

  SColumnInfoData* pTs = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 0);
  SColumnInfoData* pVar = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 1);
  ASSERT_NE(pTs, nullptr);
  ASSERT_NE(pVar, nullptr);

  for (int32_t i = 0; i < numOfRows; ++i) {
    TSKEY ts = 1792575900000LL + i;
    colDataSetVal(pTs, i, (const char*)&ts, false);
  }

  // rows [6, 12) are written before rows [0, 6), so offset[5] > offset[6]
  int32_t order[12] = {6, 7, 8, 9, 10, 11, 0, 1, 2, 3, 4, 5};
  char    varbuf[128] = {0};
  for (int32_t k = 0; k < numOfRows; ++k) {
    char raw[32] = {0};
    snprintf(raw, sizeof(raw), "value_of_row_%d", order[k]);
    STR_TO_VARSTR(varbuf, raw)
    colDataSetVal(pVar, order[k], varbuf, false);
    memset(varbuf, 0, sizeof(varbuf));
  }
  b->info.rows = numOfRows;

  ASSERT_GT(pVar->varmeta.offset[5], pVar->varmeta.offset[6]);

  // single row which is the last row of the later written slice
  int64_t encodeSize = blockGetEncodeSizeOfRows(b, 5, 5);
  ASSERT_GT(encodeSize, 0);

  char* buf = (char*)taosMemoryCalloc(1, (size_t)encodeSize);
  ASSERT_NE(buf, nullptr);
  int32_t len = 0;
  int32_t code = blockEncodeAsRows(b, buf, (size_t)encodeSize, 2, 5, 5, &len);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(len, (int32_t)encodeSize);

  // the whole range, containing the non-monotonic jump, must be encodable as well
  int64_t fullSize = blockGetEncodeSizeOfRows(b, 0, numOfRows - 1);
  ASSERT_GT(fullSize, 0);
  char* buf2 = (char*)taosMemoryCalloc(1, (size_t)fullSize);
  ASSERT_NE(buf2, nullptr);
  len = 0;
  code = blockEncodeAsRows(b, buf2, (size_t)fullSize, 2, 0, numOfRows - 1, &len);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(len, (int32_t)fullSize);

  // the encoded data must be decodable and the values must be in row order
  SSDataBlock* dec = (SSDataBlock*)taosMemoryCalloc(1, sizeof(SSDataBlock));
  ASSERT_NE(dec, nullptr);
  ASSERT_EQ(blockSpecialDecodeLaterPart(dec, buf2, 0, TSKEY_MIN, TSKEY_MAX), TSDB_CODE_SUCCESS);
  ASSERT_EQ(dec->info.rows, numOfRows);

  SColumnInfoData* pDecVar = (SColumnInfoData*)taosArrayGet(dec->pDataBlock, 1);
  ASSERT_NE(pDecVar, nullptr);
  for (int32_t r = 0; r < numOfRows; ++r) {
    char exp[32] = {0};
    snprintf(exp, sizeof(exp), "value_of_row_%d", r);
    const char* pVal = colDataGetData(pDecVar, r);
    ASSERT_NE(pVal, nullptr) << "row " << r;
    int32_t n = varDataLen((char*)pVal);
    ASSERT_EQ(n, (int32_t)strlen(exp)) << "row " << r;
    EXPECT_EQ(memcmp(varDataVal((char*)pVal), exp, n), 0) << "row " << r;
  }
  blockDataDestroy(dec);

  taosMemoryFree(buf);
  taosMemoryFree(buf2);
  blockDataDestroy(b);
}

TEST(testCase, blockEncodeAsRowsWithShortTrailingVarData) {
  for (const char* value : {"", "a"}) {
    for (bool outOfOrder : {false, true}) {
      SCOPED_TRACE(value);
      SCOPED_TRACE(outOfOrder);
      DataBlockGuard source;
      ASSERT_EQ(createDataBlock(source.put()), TSDB_CODE_SUCCESS);
      SColumnInfoData tsCol = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, 8, 1);
      SColumnInfoData varCol = createColumnInfoData(TSDB_DATA_TYPE_BINARY, 32, 2);
      ASSERT_EQ(blockDataAppendColInfo(source.get(), &tsCol), TSDB_CODE_SUCCESS);
      ASSERT_EQ(blockDataAppendColInfo(source.get(), &varCol), TSDB_CODE_SUCCESS);
      ASSERT_EQ(blockDataEnsureCapacity(source.get(), 2), TSDB_CODE_SUCCESS);
      auto* timestamps = (SColumnInfoData*)taosArrayGet(source->pDataBlock, 0);
      auto* strings = (SColumnInfoData*)taosArrayGet(source->pDataBlock, 1);
      ASSERT_NE(timestamps, nullptr);
      ASSERT_NE(strings, nullptr);
      for (int32_t row = 0; row < 2; ++row) {
        TSKEY timestamp = 1792575900000LL + row;
        ASSERT_EQ(colDataSetVal(timestamps, row, (const char*)&timestamp, false), TSDB_CODE_SUCCESS);
      }
      int32_t tailRow = outOfOrder ? 0 : 1;
      char varbuf[32] = {0};
      STR_TO_VARSTR(varbuf, "prefix")
      ASSERT_EQ(colDataSetVal(strings, 1 - tailRow, varbuf, false), TSDB_CODE_SUCCESS);
      STR_TO_VARSTR(varbuf, value)
      ASSERT_EQ(colDataSetVal(strings, tailRow, varbuf, false), TSDB_CODE_SUCCESS);
      source->info.rows = 2;
      ASSERT_EQ(strings->varmeta.length - strings->varmeta.offset[tailRow],
                VARSTR_HEADER_SIZE + strlen(value));

      for (bool singleRow : {false, true}) {
        SCOPED_TRACE(singleRow);
        int32_t start = singleRow ? tailRow : 0;
        int32_t end = singleRow ? tailRow : 1;
        int64_t encodedSize = blockGetEncodeSizeOfRows(source.get(), start, end);
        ASSERT_GT(encodedSize, 0);
        std::vector<char> encoded((size_t)encodedSize);
        int32_t encodedLen = 0;
        ASSERT_EQ(blockEncodeAsRows(source.get(), encoded.data(), encoded.size(), 2, start, end, &encodedLen),
                  TSDB_CODE_SUCCESS);
        ASSERT_EQ(encodedLen, encodedSize);
        DataBlockGuard decoded;
        ASSERT_EQ(createOneDataBlock(source.get(), false, decoded.put()), TSDB_CODE_SUCCESS);
        ASSERT_EQ(blockSpecialDecodeLaterPart(decoded.get(), encoded.data(), 0, TSKEY_MIN, TSKEY_MAX),
                  TSDB_CODE_SUCCESS);
        ASSERT_EQ(decoded->info.rows, end - start + 1);
        auto* decodedStrings = (SColumnInfoData*)taosArrayGet(decoded->pDataBlock, 1);
        ASSERT_NE(decodedStrings, nullptr);
        for (int32_t row = start; row <= end; ++row) {
          const char* expected = row == tailRow ? value : "prefix";
          const char* actual = colDataGetData(decodedStrings, row - start);
          ASSERT_NE(actual, nullptr);
          ASSERT_EQ(varDataLen(actual), strlen(expected));
          EXPECT_EQ(memcmp(varDataVal(actual), expected, strlen(expected)), 0);
        }
      }
    }
  }
}

// colDataCopyNItems() broadcasts one value to many rows and keeps a single copy of the payload
// (the column is marked reassigned and every row points at the same offset). The row encoding
// must keep that single copy: expanding it per row multiplies the encoded size by the row count,
// which for a 3KB tag over 4096 rows exceeds the 10MiB align window capacity.
TEST(testCase, blockEncodeAsRowsWithSharedVarData) {
  const int32_t numOfRows = 4096;
  const int32_t valueLen = 3072;

  DataBlockGuard source;
  ASSERT_EQ(createDataBlock(source.put()), TSDB_CODE_SUCCESS);
  SColumnInfoData tsCol = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, 8, 1);
  SColumnInfoData varCol = createColumnInfoData(TSDB_DATA_TYPE_BINARY, 4096, 2);
  ASSERT_EQ(blockDataAppendColInfo(source.get(), &tsCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(source.get(), &varCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(source.get(), numOfRows), TSDB_CODE_SUCCESS);

  auto* timestamps = (SColumnInfoData*)taosArrayGet(source->pDataBlock, 0);
  auto* strings = (SColumnInfoData*)taosArrayGet(source->pDataBlock, 1);
  ASSERT_NE(timestamps, nullptr);
  ASSERT_NE(strings, nullptr);
  for (int32_t row = 0; row < numOfRows; ++row) {
    TSKEY timestamp = 1792575900000LL + row;
    ASSERT_EQ(colDataSetVal(timestamps, row, (const char*)&timestamp, false), TSDB_CODE_SUCCESS);
  }

  std::vector<char> value(VARSTR_HEADER_SIZE + valueLen, 'x');
  VarDataLenT       payloadLen = valueLen;
  memcpy(value.data(), &payloadLen, VARSTR_HEADER_SIZE);
  ASSERT_EQ(colDataCopyNItems(strings, 0, value.data(), numOfRows, false), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(strings->reassigned);
  ASSERT_EQ(strings->varmeta.length, (uint32_t)(VARSTR_HEADER_SIZE + valueLen));
  source->info.rows = numOfRows;

  int64_t encodedSize = blockGetEncodeSizeOfRows(source.get(), 0, numOfRows - 1);
  ASSERT_GT(encodedSize, 0);
  // one payload copy plus the per-row offset array, far below the 10MiB align block capacity
  ASSERT_LT(encodedSize, 10 * 1024 * 1024);
  ASSERT_LT(encodedSize, (int64_t)numOfRows * (VARSTR_HEADER_SIZE + valueLen) / 2);

  std::vector<char> encoded((size_t)encodedSize);
  int32_t           encodedLen = 0;
  ASSERT_EQ(blockEncodeAsRows(source.get(), encoded.data(), encoded.size(), 2, 0, numOfRows - 1, &encodedLen),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(encodedLen, encodedSize);

  DataBlockGuard decoded;
  ASSERT_EQ(createOneDataBlock(source.get(), false, decoded.put()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockSpecialDecodeLaterPart(decoded.get(), encoded.data(), 0, TSKEY_MIN, TSKEY_MAX), TSDB_CODE_SUCCESS);
  ASSERT_EQ(decoded->info.rows, numOfRows);
  auto* decodedStrings = (SColumnInfoData*)taosArrayGet(decoded->pDataBlock, 1);
  ASSERT_NE(decodedStrings, nullptr);
  for (int32_t row = 0; row < numOfRows; ++row) {
    const char* actual = colDataGetData(decodedStrings, row);
    ASSERT_NE(actual, nullptr) << "row " << row;
    ASSERT_EQ(varDataLen((char*)actual), valueLen) << "row " << row;
    EXPECT_EQ(memcmp(varDataVal((char*)actual), value.data() + VARSTR_HEADER_SIZE, valueLen), 0) << "row " << row;
  }
}

// A broadcast column whose rows were all nulled afterwards keeps reassigned set but no longer
// carries a usable record. The shared-span fast path must fall back to the row-by-row layout
// instead of treating the stale offset as a shared value.
TEST(testCase, blockEncodeAsRowsWithReassignedAllNullVarData) {
  const int32_t numOfRows = 4;

  DataBlockGuard source;
  ASSERT_EQ(createDataBlock(source.put()), TSDB_CODE_SUCCESS);
  SColumnInfoData tsCol = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, 8, 1);
  SColumnInfoData varCol = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 32, 2);
  ASSERT_EQ(blockDataAppendColInfo(source.get(), &tsCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(source.get(), &varCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(source.get(), numOfRows), TSDB_CODE_SUCCESS);

  auto* timestamps = (SColumnInfoData*)taosArrayGet(source->pDataBlock, 0);
  auto* strings = (SColumnInfoData*)taosArrayGet(source->pDataBlock, 1);
  ASSERT_NE(timestamps, nullptr);
  ASSERT_NE(strings, nullptr);
  for (int32_t row = 0; row < numOfRows; ++row) {
    TSKEY timestamp = 1792575900000LL + row;
    ASSERT_EQ(colDataSetVal(timestamps, row, (const char*)&timestamp, false), TSDB_CODE_SUCCESS);
  }

  char value[32] = {0};
  STR_TO_VARSTR(value, "table_name")
  ASSERT_EQ(colDataCopyNItems(strings, 0, value, numOfRows, false), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(strings->reassigned);

  colDataSetNItemsNull(strings, 0, numOfRows);
  ASSERT_TRUE(strings->reassigned);
  source->info.rows = numOfRows;

  int64_t encodedSize = blockGetEncodeSizeOfRows(source.get(), 0, numOfRows - 1);
  ASSERT_GT(encodedSize, 0);

  std::vector<char> encoded((size_t)encodedSize);
  int32_t           encodedLen = 0;
  ASSERT_EQ(blockEncodeAsRows(source.get(), encoded.data(), encoded.size(), 2, 0, numOfRows - 1, &encodedLen),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(encodedLen, encodedSize);

  DataBlockGuard decoded;
  ASSERT_EQ(createOneDataBlock(source.get(), false, decoded.put()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockSpecialDecodeLaterPart(decoded.get(), encoded.data(), 0, TSKEY_MIN, TSKEY_MAX), TSDB_CODE_SUCCESS);
  ASSERT_EQ(decoded->info.rows, numOfRows);
  auto* decodedStrings = (SColumnInfoData*)taosArrayGet(decoded->pDataBlock, 1);
  ASSERT_NE(decodedStrings, nullptr);
  for (int32_t row = 0; row < numOfRows; ++row) {
    EXPECT_TRUE(colDataIsNull_s(decodedStrings, row)) << "row " << row;
  }
}

// A corrupt var offset on a broadcast column must be reported as an error by the row-by-row
// fallback, not silently trusted by the shared-span fast path.
TEST(testCase, blockEncodeAsRowsWithReassignedInvalidVarOffset) {
  const int32_t numOfRows = 4;

  DataBlockGuard source;
  ASSERT_EQ(createDataBlock(source.put()), TSDB_CODE_SUCCESS);
  SColumnInfoData tsCol = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, 8, 1);
  SColumnInfoData varCol = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 32, 2);
  ASSERT_EQ(blockDataAppendColInfo(source.get(), &tsCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(source.get(), &varCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(source.get(), numOfRows), TSDB_CODE_SUCCESS);

  auto* timestamps = (SColumnInfoData*)taosArrayGet(source->pDataBlock, 0);
  auto* strings = (SColumnInfoData*)taosArrayGet(source->pDataBlock, 1);
  ASSERT_NE(timestamps, nullptr);
  ASSERT_NE(strings, nullptr);
  for (int32_t row = 0; row < numOfRows; ++row) {
    TSKEY timestamp = 1792575900000LL + row;
    ASSERT_EQ(colDataSetVal(timestamps, row, (const char*)&timestamp, false), TSDB_CODE_SUCCESS);
  }

  char value[32] = {0};
  STR_TO_VARSTR(value, "table_name")
  ASSERT_EQ(colDataCopyNItems(strings, 0, value, numOfRows, false), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(strings->reassigned);
  strings->varmeta.offset[2] = (int32_t)0x7FFFFFF0;
  source->info.rows = numOfRows;

  EXPECT_EQ(blockGetEncodeSizeOfRows(source.get(), 0, numOfRows - 1), -1);

  std::vector<char> encoded(1024);
  int32_t           encodedLen = 0;
  EXPECT_NE(blockEncodeAsRows(source.get(), encoded.data(), encoded.size(), 2, 0, numOfRows - 1, &encodedLen),
            TSDB_CODE_SUCCESS);
}

void check_tm(const STm* tm, int32_t y, int32_t mon, int32_t d, int32_t h, int32_t m, int32_t s, int64_t fsec) {
  ASSERT_EQ(tm->tm.tm_year, y);
  ASSERT_EQ(tm->tm.tm_mon, mon);
  ASSERT_EQ(tm->tm.tm_mday, d);
  ASSERT_EQ(tm->tm.tm_hour, h);
  ASSERT_EQ(tm->tm.tm_min, m);
  ASSERT_EQ(tm->tm.tm_sec, s);
  ASSERT_EQ(tm->fsec, fsec);
}

void test_timestamp_tm_conversion(int64_t ts, int32_t precision, int32_t y, int32_t mon, int32_t d, int32_t h,
                                  int32_t m, int32_t s, int64_t fsec) {
  int64_t    ts_tmp;
  char       buf[128] = {0};
  struct STm tm;
  taosFormatUtcTime(buf, 128, ts, precision);
  printf("formated ts of %ld, precision: %d is: %s\n", ts, precision, buf);
  taosTs2Tm(ts, precision, &tm, NULL);
  check_tm(&tm, y, mon, d, h, m, s, fsec);
  taosTm2Ts(&tm, &ts_tmp, precision, NULL);
  ASSERT_EQ(ts, ts_tmp);
}

TEST(timeTest, timestamp2tm) {
  const char* ts_str_ns = "2023-10-12T11:29:00.775726171+0800";
  const char* ts_str_us = "2023-10-12T11:29:00.775726+0800";
  const char* ts_str_ms = "2023-10-12T11:29:00.775+0800";
  int64_t     ts, tmp_ts = 0;
  struct STm  tm;

  ASSERT_EQ(TSDB_CODE_SUCCESS, taosParseTime(ts_str_ns, &ts, strlen(ts_str_ns), TSDB_TIME_PRECISION_NANO, NULL));
  test_timestamp_tm_conversion(ts, TSDB_TIME_PRECISION_NANO, 2023 - 1900, 9 /* mon start from 0*/, 12, 11, 29, 0,
                               775726171L);

  ASSERT_EQ(TSDB_CODE_SUCCESS, taosParseTime(ts_str_us, &ts, strlen(ts_str_us), TSDB_TIME_PRECISION_MICRO, NULL));
  test_timestamp_tm_conversion(ts, TSDB_TIME_PRECISION_MICRO, 2023 - 1900, 9 /* mon start from 0*/, 12, 11, 29, 0,
                               775726000L);

  ASSERT_EQ(TSDB_CODE_SUCCESS, taosParseTime(ts_str_ms, &ts, strlen(ts_str_ms), TSDB_TIME_PRECISION_MILLI, NULL));
  test_timestamp_tm_conversion(ts, TSDB_TIME_PRECISION_MILLI, 2023 - 1900, 9 /* mon start from 0*/, 12, 11, 29, 0,
                               775000000L);

  ts = -5364687943000;  // milliseconds since epoch, Wednesday, January 1, 1800 1:00:00 AM GMT+08:06
  test_timestamp_tm_conversion(ts, TSDB_TIME_PRECISION_MILLI, 1800 - 1900, 0 /* mon start from 0*/, 1, 1, 0, 0,
                               000000000L);

  ts = 0;
  test_timestamp_tm_conversion(ts, TSDB_TIME_PRECISION_MILLI, 1970 - 1900, 0 /* mon start from 0*/, 1, 8, 0, 0,
                               000000000L);

  ts = -62198784343000;  // milliseconds before epoch, Friday, January 1, -0001 12:00:00 AM GMT+08:06
  test_timestamp_tm_conversion(ts, TSDB_TIME_PRECISION_MILLI, -1 - 1900, 0 /* mon start from 0*/, 1,
                               0 /* hour start from 0*/, 0, 0, 000000000L);
}

TEST(timeTest, validateUnsignedPosixUtcTimezone) {
  const struct {
    const char* input;
    const char* normalized;
    int64_t     offsetSeconds;
  } validCases[] = {
      {"UTC0", "UTC+0", 0},
      {"UTC8", "UTC+8", -8 * 3600},
      {"UTC8:30", "UTC+8:30", -(8 * 3600 + 30 * 60)},
      {"UTC13:59", "UTC+13:59", -(13 * 3600 + 59 * 60)},
      {"UTC14", "UTC+14", -14 * 3600},
      // The UTC prefix is case-insensitive on every validation path.
      {"utc8", "UTC+8", -8 * 3600},
      {"UtC8:30", "UTC+8:30", -(8 * 3600 + 30 * 60)},
      {"utc+8", "UTC+8", -8 * 3600},
      {"uTc-8", "UTC-8", 8 * 3600},
      {"utc+08:00", "UTC+8", -8 * 3600},
      {"utc", "UTC", 0},
  };

  for (const auto& test : validCases) {
    char       normalized[TD_TIMEZONE_LEN] = {0};
    timezone_t normalizedTz = NULL;
    ASSERT_EQ(taosValidateAndNormalizeTimezone(
                  test.input, normalized, sizeof(normalized), &normalizedTz),
              TSDB_CODE_SUCCESS)
        << test.input;
    EXPECT_STREQ(normalized, test.normalized);

    int64_t offsetSeconds = 0;
    ASSERT_EQ(taosGetTimezoneOffsetAtSeconds(0, normalizedTz,
                                             &offsetSeconds),
              TSDB_CODE_SUCCESS);
    EXPECT_EQ(offsetSeconds, test.offsetSeconds) << test.input;
    tzfree(normalizedTz);

    timezone_t directTz = NULL;
    ASSERT_EQ(taosValidateTimezone(test.input, &directTz),
              TSDB_CODE_SUCCESS)
        << test.input;
    ASSERT_EQ(taosGetTimezoneOffsetAtSeconds(0, directTz, &offsetSeconds),
              TSDB_CODE_SUCCESS);
    EXPECT_EQ(offsetSeconds, test.offsetSeconds) << test.input;
    tzfree(directTz);
  }

  char tooSmall[sizeof("UTC+13:59") - 1] = {0};
  EXPECT_EQ(taosValidateAndNormalizeTimezone(
                "UTC13:59", tooSmall, sizeof(tooSmall), NULL),
            TSDB_CODE_PAR_INVALID_TIMEZONE);

  const char* const invalidCases[] = {
      "UTC15",
      "UTC14:01",
      "UTC8:60",
      "UTC8EDT",
  };
  for (const char* input : invalidCases) {
    char normalized[TD_TIMEZONE_LEN] = {0};
    EXPECT_EQ(taosValidateAndNormalizeTimezone(
                  input, normalized, sizeof(normalized), NULL),
              TSDB_CODE_PAR_INVALID_TIMEZONE)
        << input;
    EXPECT_EQ(taosValidateTimezone(input, NULL),
              TSDB_CODE_PAR_INVALID_TIMEZONE)
        << input;
  }
}

void test_ts2char(int64_t ts, const char* format, int32_t precison, const char* expected) {
  char    buf[256] = {0};
  int32_t code = TEST_ts2char(format, ts, precison, buf, 256);
  ASSERT_EQ(code, 0);
  printf("ts: %ld format: %s res: [%s], expected: [%s]\n", ts, format, buf, expected);
  ASSERT_STREQ(expected, buf);
}

TEST(timeTest, ts2char) {
  osDefaultInit();
  int32_t code = 0;
  if (taosGetLocalTimezoneOffset(&code) != TdEastZone8) GTEST_SKIP();
  int64_t     ts;
  const char* format = "YYYY-MM-DD";
  ts = 0;
  test_ts2char(ts, format, TSDB_TIME_PRECISION_MILLI, "1970-01-01");
  test_ts2char(ts, format, TSDB_TIME_PRECISION_MICRO, "1970-01-01");
  test_ts2char(ts, format, TSDB_TIME_PRECISION_NANO, "1970-01-01");
  test_ts2char(ts, format, TSDB_TIME_PRECISION_SECONDS, "1970-01-01");

  ts = 1697163517;
  test_ts2char(ts, "YYYY-MM-DD", TSDB_TIME_PRECISION_SECONDS, "2023-10-13");
  ts = 1697163517000;
  test_ts2char(ts, "YYYY-MM-DD-Day-DAY", TSDB_TIME_PRECISION_MILLI, "2023-10-13-Friday   -FRIDAY   ");
#ifndef WINDOWS
  // double quoted: year, month, day are not parsed
  test_ts2char(ts,
               "YYYY-YYY-YY-Y-yyyy-yyy-yy-y-\"年\"-MONTH-MON-Month-Mon-month-mon-\"月\"-DDD-DD-D-ddd-dd-d-DAY-Day-"
               "day-\"日\"",
               TSDB_TIME_PRECISION_MILLI,
               "2023-023-23-3-2023-023-23-3-年-OCTOBER  -OCT-October  -Oct-october  "
               "-oct-月-286-13-6-286-13-6-FRIDAY   -Friday   -friday   -日");
#endif
  ts = 1697182085123L;  // Friday, October 13, 2023 3:28:05.123 PM GMT+08:00
  test_ts2char(ts, "HH24:hh24:HH12:hh12:HH:hh:MI:mi:SS:ss:MS:ms:US:us:NS:ns:PM:AM:pm:am", TSDB_TIME_PRECISION_MILLI,
               "15:15:03:03:03:03:28:28:05:05:123:123:123000:123000:123000000:123000000:PM:PM:pm:pm");

  // double quotes normal output
  test_ts2char(ts, "\\\"HH24:hh24:HH12:hh12:HH:hh:MI:mi:SS:ss:MS:ms:US:us:NS:ns:PM:AM:pm:am\\\"",
               TSDB_TIME_PRECISION_MILLI,
               "\"15:15:03:03:03:03:28:28:05:05:123:123:123000:123000:123000000:123000000:PM:PM:pm:pm\"");
  test_ts2char(ts, "\\\"HH24:hh24:HH12:hh12:HH:hh:MI:mi:SS:ss:MS:ms:US:us:NS:ns:PM:AM:pm:am", TSDB_TIME_PRECISION_MILLI,
               "\"15:15:03:03:03:03:28:28:05:05:123:123:123000:123000:123000000:123000000:PM:PM:pm:pm");
  // double quoted strings recognized as literal string, parsing skipped
  test_ts2char(ts, "\"HH24:hh24:HH12:hh12:HH:hh:MI:mi:SS:ss:MS:ms:US:us:NS:ns:PM:AM:pm:am", TSDB_TIME_PRECISION_MILLI,
               "HH24:hh24:HH12:hh12:HH:hh:MI:mi:SS:ss:MS:ms:US:us:NS:ns:PM:AM:pm:am");
  test_ts2char(ts, "yyyy-mm-dd hh24:mi:ss.nsamaaa", TSDB_TIME_PRECISION_MILLI, "2023-10-13 15:28:05.123000000pmaaa");
  test_ts2char(ts, "aaa--yyyy-mm-dd hh24:mi:ss.nsamaaa", TSDB_TIME_PRECISION_MILLI,
               "aaa--2023-10-13 15:28:05.123000000pmaaa");
  test_ts2char(ts, "add--yyyy-mm-dd hh24:mi:ss.nsamaaa", TSDB_TIME_PRECISION_MILLI,
               "a13--2023-10-13 15:28:05.123000000pmaaa");

  ts = 1693946405000;
  test_ts2char(ts, "Day, Month dd, YYYY hh24:mi:ss AM TZH:tzh", TSDB_TIME_PRECISION_MILLI,
               "Wednesday, September 06, 2023 04:40:05 AM +08:+08");

  ts = -62198784343000;  // milliseconds before epoch, Friday, January 1, -0001 12:00:00 AM GMT+08:06
  test_ts2char(ts, "Day, Month dd, YYYY hh12:mi:ss AM", TSDB_TIME_PRECISION_MILLI,
               "Friday   , January   01, -001 12:00:00 AM");
}

TEST(timeTest, char2ts) {
  osDefaultInit();
  int32_t code = 0;
  if (taosGetLocalTimezoneOffset(&code) != TdEastZone8) GTEST_SKIP();
  int64_t ts;
  code =
      TEST_char2ts("YYYY-DD-MM HH12:MI:SS:MSPM", &ts, TSDB_TIME_PRECISION_MILLI, "2023-10-10 12:00:00.000AM");
  ASSERT_EQ(code, 0);
  ASSERT_EQ(ts, 1696867200000LL);

  // 2009-1-1 00:00:00
  ASSERT_EQ(0, TEST_char2ts("YYYY-YYY-YY-Y", &ts, TSDB_TIME_PRECISION_MILLI, "2023-123-23-9"));
  ASSERT_EQ(1230739200000LL, ts);
  // 2023-1-1
  ASSERT_EQ(0, TEST_char2ts("YYYY-YYY-YY", &ts, TSDB_TIME_PRECISION_MILLI, "2023-123-23-9"));
  ASSERT_EQ(ts, 1672502400000LL);

  // 2123-1-1, the second year(123) is used, which converted to 2123
  ASSERT_EQ(0, TEST_char2ts("YYYY-YYY", &ts, TSDB_TIME_PRECISION_MILLI, "2023-123-23-9"));
  ASSERT_EQ(ts, 4828176000000LL);
  // 2023-1-1 12:10:10am
  ASSERT_EQ(0, TEST_char2ts("yyyy-mm-dd HH12:MI:SSAM", &ts, TSDB_TIME_PRECISION_MILLI, "2023-1-1 12:10:10am"));
  ASSERT_EQ(ts, 1672503010000LL);

  // 2023-1-1 21:10:10.123
  ASSERT_EQ(0, TEST_char2ts("yy-MM-dd HH12:MI:ss.msa.m.", &ts, TSDB_TIME_PRECISION_MILLI, "23-1-01 9:10:10.123p.m."));
  ASSERT_EQ(ts, 1672578610123LL);

  // 2023-1-1 21:10:10.123456789
  ASSERT_EQ(0, TEST_char2ts("yy-MM-dd HH:MI:ss.ms.us.nsa.m.", &ts, TSDB_TIME_PRECISION_NANO,
                            "23-1-01 9:10:10.123.000456.000000789p.m."));
  ASSERT_EQ(ts, 1672578610123456789LL);

  // 2023-1-1 21:10:10.120450780
  ASSERT_EQ(0, TEST_char2ts("yy-MM-dd HH24:MI:SS.ms.us.ns", &ts, TSDB_TIME_PRECISION_NANO,
                            "   23   - 1 - 01   \t  21:10:10 .  12 .  \t 00045 . 00000078  \t"));
  ASSERT_EQ(ts, 1672578610120450780LL);

#ifndef WINDOWS
  // 2023-1-1 21:10:10.120450780
  ASSERT_EQ(0, TEST_char2ts("yy \"年\"-MM 月-dd  \"日 子\" HH24:MI:ss.ms.us.ns TZH", &ts, TSDB_TIME_PRECISION_NANO,
                            "   23  年 - 1 月 - 01 日 子  \t  21:10:10 .  12 .  \t 00045 . 00000078  \t+08"));
  ASSERT_EQ(ts, 1672578610120450780LL);
#endif

  // 2023-1-1 19:10:10.123456789+06 -> 2023-1-1 21:10:10.123456789+08
  ASSERT_EQ(0, TEST_char2ts("yy-MM-dd HH:MI:ss.ms.us.nsa.m.TZH", &ts, TSDB_TIME_PRECISION_NANO,
                            "23-1-01 7:10:10.123.000456.000000789p.m.6"));
  ASSERT_EQ(ts, 1672578610123456789LL);

  // 2023-1-1 12:10:10.123456789-01 -> 2023-1-1 21:10:10.123456789+08
  ASSERT_EQ(0, TEST_char2ts("yy-MM-dd HH24:MI:ss.ms.us.nsTZH", &ts, TSDB_TIME_PRECISION_NANO,
                            "23-1-01 12:10:10.123.000456.000000789-1"));
  ASSERT_EQ(ts, 1672578610123456789LL);

  // 2100-01-01 11:10:10.124456+08
  ASSERT_EQ(
      0, TEST_char2ts("yyyy-MM-dd HH24:MI:ss.usTZH", &ts, TSDB_TIME_PRECISION_MICRO, "2100-01-01 11:10:10.124456+08"));
  ASSERT_EQ(ts, 4102456210124456LL);

  // 2100-01-01 11:10:10.124456+08 Firday
  ASSERT_EQ(0, TEST_char2ts("yyyy/MONTH/dd DAY HH24:MI:ss.usTZH", &ts, TSDB_TIME_PRECISION_MICRO,
                            "2100/january/01 friday 11:10:10.124456+08"));
  ASSERT_EQ(ts, 4102456210124456LL);

  ASSERT_EQ(0, TEST_char2ts("yyyy/Month/dd Day HH24:MI:ss.usTZH", &ts, TSDB_TIME_PRECISION_MICRO,
                            "2100/january/01 FRIDAY 11:10:10.124456+08"));
  ASSERT_EQ(ts, 4102456210124456LL);
  ASSERT_EQ(0, TEST_char2ts("yyyy/Month/dd Dy HH24:MI:ss.usTZH", &ts, TSDB_TIME_PRECISION_MICRO,
                            "2100/january/01 Fri 11:10:10.124456+08:00"));
  ASSERT_EQ(ts, 4102456210124456LL);

  ASSERT_EQ(0, TEST_char2ts("yyyy/month/dd day HH24:MI:ss.usTZH", &ts, TSDB_TIME_PRECISION_MICRO,
                            "2100/january/01 Friday 11:10:10.124456+08"));
  ASSERT_EQ(ts, 4102456210124456LL);

  // 2100-02-01 11:10:10.124456+08 Firday
  ASSERT_EQ(0, TEST_char2ts("yyyy/mon/dd DY HH24:MI:ss.usTZH", &ts, TSDB_TIME_PRECISION_MICRO,
                            "2100/Feb/01 Mon 11:10:10.124456+08"));
  ASSERT_EQ(ts, 4105134610124456LL);

  // 2100-02-01 11:10:10.124456+08 Firday
  ASSERT_EQ(0, TEST_char2ts("yyyy/mon/dd DY DDD-DD-D HH24:MI:ss.usTZH", &ts, TSDB_TIME_PRECISION_MICRO,
                            "2100/Feb/01 Mon 100-1-01 11:10:10.124456+08"));
  ASSERT_EQ(ts, 4105134610124456LL);

  ASSERT_EQ(0, TEST_char2ts("yyyyMMdd ", &ts, TSDB_TIME_PRECISION_MICRO, "21000101"));

  // What is Fe?
  ASSERT_EQ(-1, TEST_char2ts("yyyy/mon/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "2100/Fe/01"));
  // '/' cannot convert to MM
  ASSERT_EQ(-1, TEST_char2ts("yyyyMMdd ", &ts, TSDB_TIME_PRECISION_MICRO, "2100/2/1"));
  // nothing to be converted to dd
  ASSERT_EQ(0, TEST_char2ts("yyyyMMdd ", &ts, TSDB_TIME_PRECISION_MICRO, "210012"));
  ASSERT_EQ(ts, 4131273600000000LL);  // 2100-12-1
  ASSERT_EQ(-1, TEST_char2ts("yyyyMMdd ", &ts, TSDB_TIME_PRECISION_MICRO, "21001"));
  ASSERT_EQ(-1, TEST_char2ts("yyyyMM-dd ", &ts, TSDB_TIME_PRECISION_MICRO, "23a1-1"));

  // 2100-1-2
  ASSERT_EQ(0, TEST_char2ts("yyyyMM/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "21001/2"));
  ASSERT_EQ(ts, 4102502400000000LL);

  // default to 1970-1-1 00:00:00+08 -> 1969-12-31 16:00:00+00
  ASSERT_EQ(0, TEST_char2ts("YYYY", &ts, TSDB_TIME_PRECISION_SECONDS, "1970"));
  ASSERT_EQ(ts, -1 * taosGetLocalTimezoneOffset(&code));

  ASSERT_EQ(0, TEST_char2ts("yyyyMM1/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "210001/2"));
  ASSERT_EQ(ts, 4102502400000000LL);

  ASSERT_EQ(-2, TEST_char2ts("yyyyMM/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "210013/2"));
  ASSERT_EQ(-2, TEST_char2ts("yyyyMM/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "210011/32"));
  ASSERT_EQ(-1, TEST_char2ts("HH12:MI:SS", &ts, TSDB_TIME_PRECISION_MICRO, "21:12:12"));
  ASSERT_EQ(-1, TEST_char2ts("yyyy/MM1/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "2100111111111/11/2"));

  TEST_char2ts("yyyy/MM1/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO, "23/11/2-13");
  // ASSERT_EQ(-2, TEST_char2ts("yyyy/MM1/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO, "23/11/2-13"));
  ASSERT_EQ(0, TEST_char2ts("yyyy年 MM/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO, "1970年1/1+0"));
  // ASSERT_EQ(ts, 0);
  ASSERT_EQ(-1, TEST_char2ts("yyyy年a MM/dd", &ts, TSDB_TIME_PRECISION_MICRO, "2023年1/2"));
  ASSERT_EQ(0, TEST_char2ts("yyyy年 MM/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO, "1970年   1/1+0"));
  // ASSERT_EQ(ts, 0);
  ASSERT_EQ(0, TEST_char2ts("yyyy年 a a a MM/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO, "1970年 a a a 1/1+0"));
  ASSERT_EQ(0, TEST_char2ts("yyyy年 a a a a a a a a a a a a a a a MM/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO,
                            "1970年 a     "));
  ASSERT_EQ(-3, TEST_char2ts("yyyy-mm-DDD", &ts, TSDB_TIME_PRECISION_MILLI, "1970-01-001"));
}

TEST(timeTest, char2tsIgnoresStaleErrno) {
  osDefaultInit();
  int64_t       ts = 0;
  const int32_t previousErrno = ERRNO;

  SET_ERRNO(ERANGE);
  const int32_t code = TEST_char2ts("yyyy-MM-dd", &ts, TSDB_TIME_PRECISION_MILLI, "2026-05-01");
  SET_ERRNO(previousErrno);

  ASSERT_EQ(0, code);
}

TEST(timeTest, epSet) {
  {
    SEpSet ep = {0};
    addEpIntoEpSet(&ep, "local", 14);
    addEpIntoEpSet(&ep, "aocal", 13);
    addEpIntoEpSet(&ep, "abcal", 12);
    addEpIntoEpSet(&ep, "abcaleb", 11);
    epsetSort(&ep);
    ASSERT_EQ(strcmp(ep.eps[0].fqdn, "abcal"), 0);
    ASSERT_EQ(ep.eps[0].port, 12);

    ASSERT_EQ(strcmp(ep.eps[1].fqdn, "abcaleb"), 0);
    ASSERT_EQ(ep.eps[1].port, 11);

    ASSERT_EQ(strcmp(ep.eps[2].fqdn, "aocal"), 0);
    ASSERT_EQ(ep.eps[2].port, 13);

    ASSERT_EQ(strcmp(ep.eps[3].fqdn, "local"), 0);
    ASSERT_EQ(ep.eps[3].port, 14);
  }
  {
    SEpSet ep = {0};
    addEpIntoEpSet(&ep, "local", 14);
    addEpIntoEpSet(&ep, "local", 13);
    addEpIntoEpSet(&ep, "local", 12);
    addEpIntoEpSet(&ep, "local", 11);
    epsetSort(&ep);
    ASSERT_EQ(strcmp(ep.eps[0].fqdn, "local"), 0);
    ASSERT_EQ(ep.eps[0].port, 11);

    ASSERT_EQ(strcmp(ep.eps[0].fqdn, "local"), 0);
    ASSERT_EQ(ep.eps[1].port, 12);

    ASSERT_EQ(strcmp(ep.eps[0].fqdn, "local"), 0);
    ASSERT_EQ(ep.eps[2].port, 13);

    ASSERT_EQ(strcmp(ep.eps[0].fqdn, "local"), 0);
    ASSERT_EQ(ep.eps[3].port, 14);
  }
  {
    SEpSet ep = {0};
    addEpIntoEpSet(&ep, "local", 14);
    epsetSort(&ep);
    ASSERT_EQ(ep.numOfEps, 1);
  }
}

// Define test cases
TEST(AlreadyAddGroupIdTest, GroupIdAdded) {
  // Test case 1: Group ID has been added
  char    ctbName[64] = "abc123";
  int64_t groupId = 123;
  bool    result = alreadyAddGroupId(ctbName, groupId);
  EXPECT_TRUE(result);
}

TEST(AlreadyAddGroupIdTest, GroupIdNotAdded) {
  // Test case 2: Group ID has not been added
  char    ctbName[64] = "abc456";
  int64_t groupId = 123;
  bool    result = alreadyAddGroupId(ctbName, groupId);
  EXPECT_FALSE(result);
}

TEST(AlreadyAddGroupIdTest, GroupIdAddedAtTheEnd) {
  // Test case 3: Group ID has been added at the end
  char    ctbName[64] = "xyz1";
  int64_t groupId = 1;
  bool    result = alreadyAddGroupId(ctbName, groupId);
  EXPECT_TRUE(result);
}

TEST(AlreadyAddGroupIdTest, GroupIdAddedWithDifferentLength) {
  // Test case 4: Group ID has been added with different length
  char    ctbName[64] = "def";
  int64_t groupId = 123456;
  bool    result = alreadyAddGroupId(ctbName, groupId);
  EXPECT_FALSE(result);
}

#define SLOW_LOG_TYPE_NULL   0x0
#define SLOW_LOG_TYPE_QUERY  0x1
#define SLOW_LOG_TYPE_INSERT 0x2
#define SLOW_LOG_TYPE_OTHERS 0x4
#define SLOW_LOG_TYPE_ALL    0x7

static int32_t taosSetSlowLogScope2(char* pScopeStr, int32_t* pScope) {
  if (NULL == pScopeStr || 0 == strlen(pScopeStr)) {
    *pScope = SLOW_LOG_TYPE_QUERY;
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  int32_t slowScope = 0;

  char* scope = NULL;
  char* tmp = NULL;
  while ((scope = strsep(&pScopeStr, "|")) != NULL) {
    taosMemoryFreeClear(tmp);
    tmp = taosStrdup(scope);
    strtrim(tmp);
    if (0 == strcasecmp(tmp, "all")) {
      slowScope |= SLOW_LOG_TYPE_ALL;
      continue;
    }

    if (0 == strcasecmp(tmp, "query")) {
      slowScope |= SLOW_LOG_TYPE_QUERY;
      continue;
    }

    if (0 == strcasecmp(tmp, "insert")) {
      slowScope |= SLOW_LOG_TYPE_INSERT;
      continue;
    }

    if (0 == strcasecmp(tmp, "others")) {
      slowScope |= SLOW_LOG_TYPE_OTHERS;
      continue;
    }

    if (0 == strcasecmp(tmp, "none")) {
      slowScope |= SLOW_LOG_TYPE_NULL;
      continue;
    }

    taosMemoryFreeClear(tmp);
    uError("Invalid slowLog scope value:%s", pScopeStr);
    TAOS_RETURN(TSDB_CODE_INVALID_CFG_VALUE);
  }

  *pScope = slowScope;
  taosMemoryFreeClear(tmp);
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

TEST(TaosSetSlowLogScopeTest, NullPointerInput) {
  char*   pScopeStr = NULL;
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, SLOW_LOG_TYPE_QUERY);
}

TEST(TaosSetSlowLogScopeTest, EmptyStringInput) {
  char    pScopeStr[1] = "";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, SLOW_LOG_TYPE_QUERY);
}

TEST(TaosSetSlowLogScopeTest, AllScopeInput) {
  char    pScopeStr[] = "all";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);

  EXPECT_EQ(scope, SLOW_LOG_TYPE_ALL);
}

TEST(TaosSetSlowLogScopeTest, QueryScopeInput) {
  char    pScopeStr[] = " query";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, SLOW_LOG_TYPE_QUERY);
}

TEST(TaosSetSlowLogScopeTest, InsertScopeInput) {
  char    pScopeStr[] = "insert";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, SLOW_LOG_TYPE_INSERT);
}

TEST(TaosSetSlowLogScopeTest, OthersScopeInput) {
  char    pScopeStr[] = "others";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, SLOW_LOG_TYPE_OTHERS);
}

TEST(TaosSetSlowLogScopeTest, NoneScopeInput) {
  char    pScopeStr[] = "none";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, SLOW_LOG_TYPE_NULL);
}

TEST(TaosSetSlowLogScopeTest, InvalidScopeInput) {
  char    pScopeStr[] = "invalid";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  // EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  // EXPECT_EQ(scope, -1);
}

TEST(TaosSetSlowLogScopeTest, MixedScopesInput) {
  char    pScopeStr[] = "query|insert|others|none";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, (SLOW_LOG_TYPE_QUERY | SLOW_LOG_TYPE_INSERT | SLOW_LOG_TYPE_OTHERS));
}

TEST(TaosSetSlowLogScopeTest, MixedScopesInputWithSpaces) {
  char    pScopeStr[] = "query | insert | others ";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, (SLOW_LOG_TYPE_QUERY | SLOW_LOG_TYPE_INSERT | SLOW_LOG_TYPE_OTHERS));
}

TEST(testCase, function_param_check) {
  char* param = (char*) taosMemoryMalloc(1024);
  strcpy(param, "'algorithm=arima, frows=12'");

  SHashObj* p = NULL;
  int32_t code = taosAnalyGetOpts(param, &p);

  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(taosHashGetSize(p), 2);

    void* pVal = taosHashGet(p, "algo", strlen("algo"));
    EXPECT_TRUE(pVal == NULL);

    pVal = taosHashGet(p, "rows", strlen("rows"));
    EXPECT_TRUE(pVal == NULL);

    pVal = taosHashGet(p, "frows", strlen("frows"));

    char* pStr = taosStrndup((const char*) pVal, taosHashGetValueSize(pVal));
    EXPECT_STREQ(pStr, "12");

    taosMemoryFree(pStr);

    pVal = taosHashGet(p, "algorithm", strlen("algorithm"));
    pStr = taosStrndup((const char*) pVal, taosHashGetValueSize(pVal));
    EXPECT_STREQ(pStr, "arima");

    taosMemoryFree(pStr);
  }

  taosHashCleanup(p);
  p = NULL;

  strcpy(param, " ");
  code = taosAnalyGetOpts(param, &p);
  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(taosHashGetSize(p), 0);

    void* pVal = taosHashGet(p, "algorithm", strlen("algorithm"));
    EXPECT_TRUE(pVal == NULL);
  }

  taosHashCleanup(p);
  p = NULL;

  strcpy(param, " , , ,");
  code = taosAnalyGetOpts(param, &p);
  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(taosHashGetSize(p), 0);

    void* pVal = taosHashGet(p, "algorithm", strlen("algorithm"));
    EXPECT_TRUE(pVal == NULL);
  }

  taosHashCleanup(p);
  p = NULL;

  strcpy(param, "a, b, c,");
  code = taosAnalyGetOpts(param, &p);
  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(taosHashGetSize(p), 0);
  }

  taosHashCleanup(p);
  p = NULL;

  strcpy(param, "\" a, b, c, d = 12 \"");
  code = taosAnalyGetOpts(param, &p);
  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(taosHashGetSize(p), 1);

    void* pVal = taosHashGet(p, "d", strlen("d"));
    char* pStr = taosStrndup((const char*) pVal, taosHashGetValueSize(pVal));

    EXPECT_STREQ(pStr, "12");
    taosMemoryFree(pStr);
  }

  taosHashCleanup(p);
  p = NULL;

  strcpy(param, "\" a, b, c, d = , c = 911 \"");
  code = taosAnalyGetOpts(param, &p);
  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(taosHashGetSize(p), 2);

    void* pVal = taosHashGet(p, "c", strlen("c"));
    char* pStr = taosStrndup((const char*) pVal, taosHashGetValueSize(pVal));

    EXPECT_STREQ((char*) pStr, "911");
    taosMemoryFree(pStr);
  }

  taosHashCleanup(p);
  p = NULL;

  taosMemoryFree(param);
}

TEST(testCase, function_fqdn) {
  tsEnableIpv6 = 1;
  {
    SEp ep = {0};
    char *para = "127.0.0.1";
   taosGetFqdnPortFromEp(para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "127.0.0.1"), 0);
    ASSERT_EQ(ep.port, 6030);
  }

  {
    SEp ep = {0};
    char *para = "::1";
   taosGetFqdnPortFromEp (para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "::1"), 0);
    ASSERT_EQ(ep.port, tsServerPort);
  }
   
  {
    SEp ep = {0};
    char *para = "::1:6030";
   taosGetFqdnPortFromEp(para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "::1"), 0);
    ASSERT_EQ(ep.port, 6030);
  }
  {
    SEp ep = {0};
    char *para = "::1:7030";
   taosGetFqdnPortFromEp(para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "::1"), 0);
    ASSERT_EQ(ep.port, 7030);
  }

  {
    SEp ep = {0};
    char *para = "test:7030";
   taosGetFqdnPortFromEp(para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "test"), 0);
    ASSERT_EQ(ep.port, 7030);
  }

  {
    SEp ep = {0};
    char *para = "test";
   taosGetFqdnPortFromEp(para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "test"), 0);
    ASSERT_EQ(ep.port, 6030);
  }

  {
    SEp ep = {0};
    char *para = "[test]";
   taosGetFqdnPortFromEp(para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "test"), 0);
    ASSERT_EQ(ep.port, 6030);
  }
  {
    SEp ep = {0};
    char *para = "[test]:6030";
   taosGetFqdnPortFromEp(para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "test"), 0);
    ASSERT_EQ(ep.port, 6030);
  }

}

TEST(testCase, function_taosTimeTruncate) {
  int64_t ts = 1633450000000;
  SInterval interval = {};
  interval.timezone = NULL;
  interval.intervalUnit = 'n';
  interval.slidingUnit = 'n';
  interval.offsetUnit = 0;
  interval.precision = 0;
  interval.interval = 11;
  interval.sliding = 11;
  interval.offset = 24105600000;
  interval.timeRange.skey = INT64_MIN;
  interval.timeRange.ekey = INT64_MAX;
  int64_t res = taosTimeTruncate(ts, &interval);
  ASSERT_LE(res, 1633450000000);
}

/*
 * Test: day-interval alignment under DST timezone (America/New_York).
 *
 * America/New_York has two UTC offsets:
 *   Winter (EST): UTC-5  (-18000s)
 *   Summer (EDT): UTC-4  (-14400s)
 *
 * For INTERVAL(1d) TIMEZONE('America/New_York'):
 *   - A winter timestamp should align to midnight EST  = 05:00 UTC
 *   - A summer timestamp should align to midnight EDT  = 04:00 UTC
 *
 * Historical BUG being tested: taosTimeTruncate internally calls
 * taosGetTZOffsetSeconds() which returns the offset for "now" (the
 * current wall-clock time), not the offset for the target timestamp.
 * So if "now" is summer, a winter timestamp gets the EDT offset,
 * making the day boundary 04:00 UTC instead of the correct
 * 05:00 UTC — off by 1 hour.
 */
#ifndef WINDOWS
// RAII guard to restore global timezone even if test assertions fail mid-way.
struct TzRestoreGuard {
  ~TzRestoreGuard() { taosSetGlobalTimezone("Asia/Shanghai"); }
};

TEST(testCase, taosTimeTruncate_DST_day_interval) {
  TzRestoreGuard tzGuard;  // restores Asia/Shanghai on scope exit

  // Setup: create a timezone object for America/New_York
  timezone_t ny = tzalloc("America/New_York");
  ASSERT_NE(ny, nullptr);

  // Set global timezone too (for consistency)
  ASSERT_EQ(taosSetGlobalTimezone("America/New_York"), TSDB_CODE_SUCCESS);

  // -- Prepare timestamps (millisecond precision, TSDB_TIME_PRECISION_MILLI=0) --
  //
  // Winter:  2024-01-15 15:30:00 UTC  = 2024-01-15 10:30:00 EST
  //          epoch_ms = 1705329000000
  //          Correct day start = 2024-01-15 00:00:00 EST = 2024-01-15 05:00:00 UTC
  //                            = 1705294800000 ms
  //
  // Summer:  2024-07-15 15:30:00 UTC  = 2024-07-15 11:30:00 EDT
  //          epoch_ms = 1721057400000
  //          Correct day start = 2024-07-15 00:00:00 EDT = 2024-07-15 04:00:00 UTC
  //                            = 1721016000000 ms

  const int64_t winter_ts_ms         = 1705329000000LL;  // 2024-01-15 15:30:00 UTC
  const int64_t winter_day_start_ms  = 1705294800000LL;  // 2024-01-15 05:00:00 UTC (midnight EST)

  const int64_t summer_ts_ms         = 1721057400000LL;  // 2024-07-15 15:30:00 UTC
  const int64_t summer_day_start_ms  = 1721016000000LL;  // 2024-07-15 04:00:00 UTC (midnight EDT)

  // Verify expected timestamps with taosLocalTime
  {
    time_t t;
    struct tm tm_val;

    // Verify winter day start is indeed midnight EST
    t = (time_t)(winter_day_start_ms / 1000);
    ASSERT_NE(taosLocalTime(&t, &tm_val, NULL, 0, ny), nullptr);
    ASSERT_EQ(tm_val.tm_hour, 0) << "winter day start should be midnight local";
    ASSERT_EQ(tm_val.tm_min, 0);
    ASSERT_EQ(tm_val.tm_sec, 0);
    ASSERT_EQ(tm_val.tm_mon + 1, 1);   // January
    ASSERT_EQ(tm_val.tm_mday, 15);

    // Verify summer day start is indeed midnight EDT
    t = (time_t)(summer_day_start_ms / 1000);
    ASSERT_NE(taosLocalTime(&t, &tm_val, NULL, 0, ny), nullptr);
    ASSERT_EQ(tm_val.tm_hour, 0) << "summer day start should be midnight local";
    ASSERT_EQ(tm_val.tm_min, 0);
    ASSERT_EQ(tm_val.tm_sec, 0);
    ASSERT_EQ(tm_val.tm_mon + 1, 7);   // July
    ASSERT_EQ(tm_val.tm_mday, 15);
  }

  // Build SInterval for INTERVAL(1d)
  const int64_t one_day_ms = 86400LL * 1000;
  SInterval interval = {};
  interval.timezone     = ny;
  interval.intervalUnit = 'd';
  interval.slidingUnit  = 'd';
  interval.offsetUnit   = 0;
  interval.precision    = TSDB_TIME_PRECISION_MILLI;
  interval.interval     = one_day_ms;
  interval.sliding      = one_day_ms;
  interval.offset       = 0;
  interval.timeRange.skey = INT64_MIN;
  interval.timeRange.ekey = INT64_MAX;

  // -- Test winter timestamp --
  int64_t winter_result = taosTimeTruncate(winter_ts_ms, &interval);

  // Convert result to local time to show what we got
  {
    time_t t = (time_t)(winter_result / 1000);
    struct tm tm_val;
    taosLocalTime(&t, &tm_val, NULL, 0, ny);
    std::cout << "Winter ts truncated to: "
              << (1900 + tm_val.tm_year) << "-"
              << (tm_val.tm_mon + 1) << "-" << tm_val.tm_mday
              << " " << tm_val.tm_hour << ":" << tm_val.tm_min << ":" << tm_val.tm_sec
              << " (epoch_ms=" << winter_result << ")" << std::endl;
    std::cout << "Expected:              epoch_ms=" << winter_day_start_ms << std::endl;
  }

  EXPECT_EQ(winter_result, winter_day_start_ms)
      << "Winter day boundary should be midnight EST (05:00 UTC). "
         "If this fails, taosTimeTruncate used the current DST offset "
         "instead of the winter offset.";

  // -- Test summer timestamp --
  int64_t summer_result = taosTimeTruncate(summer_ts_ms, &interval);

  {
    time_t t = (time_t)(summer_result / 1000);
    struct tm tm_val;
    taosLocalTime(&t, &tm_val, NULL, 0, ny);
    std::cout << "Summer ts truncated to: "
              << (1900 + tm_val.tm_year) << "-"
              << (tm_val.tm_mon + 1) << "-" << tm_val.tm_mday
              << " " << tm_val.tm_hour << ":" << tm_val.tm_min << ":" << tm_val.tm_sec
              << " (epoch_ms=" << summer_result << ")" << std::endl;
    std::cout << "Expected:               epoch_ms=" << summer_day_start_ms << std::endl;
  }

  EXPECT_EQ(summer_result, summer_day_start_ms)
      << "Summer day boundary should be midnight EDT (04:00 UTC). "
         "If this fails, taosTimeTruncate used the current DST offset "
         "instead of the summer offset.";

  // After the fix, BOTH seasons should align correctly.
  bool both_correct = (winter_result == winter_day_start_ms) &&
                      (summer_result == summer_day_start_ms);
  ASSERT_TRUE(both_correct)
      << "Both winter and summer day boundaries should be correct "
         "now that taosTimeTruncate uses per-timestamp DST resolution.";

  tzfree(ny);
  // TzRestoreGuard destructor handles taosSetGlobalTimezone("Asia/Shanghai")
}

TEST(testCase, taosTimeAdd_DST_day_interval_duration) {
  TzRestoreGuard tzGuard;

  timezone_t ny = tzalloc("America/New_York");
  ASSERT_NE(ny, nullptr);

  const int64_t one_day_ms = 86400LL * 1000;
  SInterval interval = {};
  interval.timezone     = ny;
  interval.intervalUnit = 'd';
  interval.slidingUnit  = 'd';
  interval.offsetUnit   = 0;
  interval.precision    = TSDB_TIME_PRECISION_MILLI;
  interval.interval     = one_day_ms;
  interval.sliding      = one_day_ms;
  interval.offset       = 0;
  interval.timeRange.skey = INT64_MIN;
  interval.timeRange.ekey = INT64_MAX;

  const int64_t spring_start_ms = 1741496400000LL;  // 2025-03-09 00:00:00 America/New_York
  const int64_t spring_next_ms  = 1741579200000LL;  // 2025-03-10 00:00:00 America/New_York
  const int64_t fall_start_ms   = 1762056000000LL;  // 2025-11-02 00:00:00 America/New_York
  const int64_t fall_next_ms    = 1762146000000LL;  // 2025-11-03 00:00:00 America/New_York

  EXPECT_EQ(taosTimeAdd(spring_start_ms, one_day_ms, 'd', TSDB_TIME_PRECISION_MILLI, ny), spring_next_ms)
      << "Spring-forward local day should be 23 hours, ending at the next local midnight.";
  EXPECT_EQ(taosTimeAdd(fall_start_ms, one_day_ms, 'd', TSDB_TIME_PRECISION_MILLI, ny), fall_next_ms)
      << "Fall-back local day should be 25 hours, ending at the next local midnight.";

  EXPECT_EQ(getNextTimeWindowStart(&interval, spring_start_ms, TSDB_ORDER_ASC), spring_next_ms);
  EXPECT_EQ(getNextTimeWindowStart(&interval, fall_start_ms, TSDB_ORDER_ASC), fall_next_ms);

  EXPECT_EQ(taosTimeGetIntervalEnd(spring_start_ms, &interval) + 1, spring_next_ms);
  EXPECT_EQ(taosTimeGetIntervalEnd(fall_start_ms, &interval) + 1, fall_next_ms);
  EXPECT_EQ((spring_next_ms - spring_start_ms), 23LL * 3600 * 1000);
  EXPECT_EQ((fall_next_ms - fall_start_ms), 25LL * 3600 * 1000);

  EXPECT_EQ(taosTimeTruncate(spring_start_ms + 12LL * 3600 * 1000, &interval), spring_start_ms);
  EXPECT_EQ(taosTimeTruncate(fall_start_ms + 12LL * 3600 * 1000, &interval), fall_start_ms);
  EXPECT_EQ(taosTimeTruncate(fall_next_ms + 12LL * 3600 * 1000, &interval), fall_next_ms);

  tzfree(ny);
}

TEST(testCase, taosTimeCountIntervalForFill_DST_calendar_days) {
  TzRestoreGuard tzGuard;

  timezone_t ny = tzalloc("America/New_York");
  ASSERT_NE(ny, nullptr);

  const int64_t day_ms = 86400LL * 1000;

  // Local-midnight anchors in America/New_York.
  const int64_t mar_02 = 1740891600000LL;  // 2025-03-02 00:00 (before spring-forward)
  const int64_t mar_08 = 1741410000000LL;  // 2025-03-08 00:00 (spring-forward week)
  const int64_t mar_10 = 1741579200000LL;  // 2025-03-10 00:00
  const int64_t mar_16 = 1742097600000LL;  // 2025-03-16 00:00
  const int64_t nov_01 = 1761969600000LL;  // 2025-11-01 00:00 (fall-back week)
  const int64_t nov_03 = 1762146000000LL;  // 2025-11-03 00:00

  /*
   * The count must equal the number of calendar-day steps taosTimeAdd takes to
   * advance skey to ekey, +1 (the trailing window). Derive the expected value
   * straight from the stepping so the test is anchored to the real arithmetic.
   */
  auto stepCount = [&](int64_t skey, int64_t ekey, int64_t step, char unit) -> int32_t {
    int32_t n = 0;
    int64_t cur = skey;
    while (cur < ekey) {
      cur = taosTimeAdd(cur, step, unit, TSDB_TIME_PRECISION_MILLI, ny);
      ++n;
    }
    return n + 1;  // ret + 1, matching taosTimeCountIntervalForFill semantics
  };

  /*
   * 1d: spring-forward (Mar 9 is 23h) and fall-back (Nov 2 is 25h) span the same
   * 2 calendar days, so they must return the same count. The old fixed
   * (ekey-skey)/interval formula returned 2 (spring) vs 3 (fall).
   */
  EXPECT_EQ(taosTimeCountIntervalForFill(mar_08, mar_10, day_ms, 'd', TSDB_TIME_PRECISION_MILLI, TSDB_ORDER_ASC, ny),
            stepCount(mar_08, mar_10, day_ms, 'd'));
  EXPECT_EQ(taosTimeCountIntervalForFill(nov_01, nov_03, day_ms, 'd', TSDB_TIME_PRECISION_MILLI, TSDB_ORDER_ASC, ny),
            stepCount(nov_01, nov_03, day_ms, 'd'));
  EXPECT_EQ(taosTimeCountIntervalForFill(mar_08, mar_10, day_ms, 'd', TSDB_TIME_PRECISION_MILLI, TSDB_ORDER_ASC, ny),
            taosTimeCountIntervalForFill(nov_01, nov_03, day_ms, 'd', TSDB_TIME_PRECISION_MILLI, TSDB_ORDER_ASC, ny))
      << "spring-forward and fall-back must count the same number of calendar days";

  // DESC must also stay consistent with the stepping.
  EXPECT_EQ(taosTimeCountIntervalForFill(nov_01, nov_03, day_ms, 'd', TSDB_TIME_PRECISION_MILLI, TSDB_ORDER_DESC, ny),
            stepCount(nov_01, nov_03, day_ms, 'd'));

  // 1w spanning the spring-forward week (Mar 2 -> Mar 16 = 2 calendar weeks).
  EXPECT_EQ(taosTimeCountIntervalForFill(mar_02, mar_16, 7 * day_ms, 'w', TSDB_TIME_PRECISION_MILLI, TSDB_ORDER_ASC, ny),
            stepCount(mar_02, mar_16, 7 * day_ms, 'w'));

  tzfree(ny);
}
TEST(testCase, formatTimestampTz_negative_fraction) {
  TzRestoreGuard tzGuard;

  timezone_t utc = tzalloc("UTC");
  ASSERT_NE(utc, nullptr);

  char buf[128];

  /* -500ms: val = -500, precision = MILLI (0)
   * Expected: 1969-12-31 23:59:59.500, NOT .:-500
   */
  char *r = formatTimestampTz(buf, sizeof(buf), -500, TSDB_TIME_PRECISION_MILLI, utc);
  ASSERT_NE(r, nullptr);
  std::string s(buf);
  EXPECT_NE(s.find(".500"), std::string::npos)
      << "Negative ms timestamp fraction should be positive: " << s;
  /* fraction must not be negative (e.g. ".-500") */
  EXPECT_EQ(s.find(".-"), std::string::npos)
      << "Fraction part should not be negative: " << s;

  /* -1us: val = -1, precision = MICRO (1) → should be .999999 */
  r = formatTimestampTz(buf, sizeof(buf), -1, TSDB_TIME_PRECISION_MICRO, utc);
  ASSERT_NE(r, nullptr);
  s = std::string(buf);
  EXPECT_NE(s.find(".999999"), std::string::npos)
      << "Negative us timestamp fraction should be 999999: " << s;

  /* -1ns: val = -1, precision = NANO (2) → should be .999999999 */
  r = formatTimestampTz(buf, sizeof(buf), -1, TSDB_TIME_PRECISION_NANO, utc);
  ASSERT_NE(r, nullptr);
  s = std::string(buf);
  EXPECT_NE(s.find(".999999999"), std::string::npos)
      << "Negative ns timestamp fraction should be 999999999: " << s;

  tzfree(utc);
}
#endif

#pragma GCC diagnostic pop
// ---------------------------------------------------------------------------
// BLOB byte-bound check used by the row-append loops.
//
// A table carries at most one BLOB column, so the append loops resolve that single
// column once per block (blockDataGetBlobCol) and then only compare its accumulated
// payload (blockDataBlobInfoExceedLimit).  A block that does hold several BLOB columns
// cannot be bounded by one pointer: the resolution then reports the column count and the
// check has to fall back to blockDataBlobExceedLimit(), which walks every column.
//
// The block below only sets the accounting fields (varmeta.length), so the 240 MB
// trigger length is reached without allocating any payload.
// ---------------------------------------------------------------------------
namespace {

// One maximum sized value may still be appended at this length without leaving the
// per-block budget, i.e. the first length at which the block has to be closed is
// kBlobTriggerLength + 1.
constexpr uint64_t kBlobTriggerLength =
    (uint64_t)TSDB_MAX_BLOB_BYTES_PER_BLOCK - (uint64_t)TSDB_MAX_BLOB_LEN;

// A ts column plus `numOfBlobCols` BLOB columns.  When no BLOB column is requested a
// plain column is added instead, so the block always has more than one column.
SSDataBlock *makeBlobBoundTestBlock(int32_t numOfBlobCols) {
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
  EXPECT_EQ(blockDataEnsureCapacity(pBlock, 4), TSDB_CODE_SUCCESS);

  SColumnInfoData tsCol = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 1);
  EXPECT_EQ(blockDataAppendColInfo(pBlock, &tsCol), TSDB_CODE_SUCCESS);

  for (int32_t i = 0; i < numOfBlobCols; ++i) {
    SColumnInfoData blobCol = createColumnInfoData(TSDB_DATA_TYPE_BLOB, 0, (int16_t)(i + 2));
    EXPECT_EQ(blockDataAppendColInfo(pBlock, &blobCol), TSDB_CODE_SUCCESS);
  }
  if (numOfBlobCols == 0) {
    SColumnInfoData intCol = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 2);
    EXPECT_EQ(blockDataAppendColInfo(pBlock, &intCol), TSDB_CODE_SUCCESS);
  }

  pBlock->info.rows = 1;
  return pBlock;
}

}  // namespace

TEST(BlobBoundCheckTest, NullBlockHasNoBlobColumn) {
  SBlobColInfo info = blockDataGetBlobCol(nullptr);
  EXPECT_EQ(info.pCol, nullptr);
  EXPECT_EQ(info.numOfBlobCols, 0);
  EXPECT_FALSE(blockDataBlobInfoExceedLimit(&info, nullptr));
}

TEST(BlobBoundCheckTest, BlockWithoutBlobColumnSkipsTheCheck) {
  SSDataBlock *pBlock = makeBlobBoundTestBlock(0);
  ASSERT_NE(pBlock, nullptr);

  SBlobColInfo info = blockDataGetBlobCol(pBlock);
  EXPECT_EQ(info.pCol, nullptr);
  EXPECT_EQ(info.numOfBlobCols, 0);
  EXPECT_FALSE(blockDataBlobInfoExceedLimit(&info, pBlock));

  // even a variable-length column far above the budget must not close the block: the
  // check only applies to BLOB columns
  SColumnInfoData *pCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, 1);
  ASSERT_NE(pCol, nullptr);
  pCol->varmeta.length = (uint32_t)kBlobTriggerLength + 1;
  EXPECT_FALSE(blockDataBlobInfoExceedLimit(&info, pBlock));
  EXPECT_FALSE(blockDataBlobExceedLimit(pBlock));

  blockDataDestroy(pBlock);
}

TEST(BlobBoundCheckTest, SingleBlobColumnIsResolvedOnce) {
  SSDataBlock *pBlock = makeBlobBoundTestBlock(1);
  ASSERT_NE(pBlock, nullptr);

  SBlobColInfo info = blockDataGetBlobCol(pBlock);
  EXPECT_EQ(info.numOfBlobCols, 1);
  ASSERT_NE(info.pCol, nullptr);

  SColumnInfoData *pBlobCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, 1);
  ASSERT_NE(pBlobCol, nullptr);
  EXPECT_EQ(info.pCol, (const SColumnInfoData *)pBlobCol);

  // at the trigger length one more maximum sized value only reaches the budget
  pBlobCol->varmeta.length = (uint32_t)kBlobTriggerLength;
  EXPECT_FALSE(blockDataBlobInfoExceedLimit(&info, pBlock));
  EXPECT_FALSE(blockDataBlobExceedLimit(pBlock));

  // one byte more and the block has to be closed; the single column check and the
  // column walk must agree, otherwise the cached path changes block boundaries
  pBlobCol->varmeta.length = (uint32_t)kBlobTriggerLength + 1;
  EXPECT_TRUE(blockDataBlobInfoExceedLimit(&info, pBlock));
  EXPECT_TRUE(blockDataBlobExceedLimit(pBlock));

  pBlobCol->varmeta.length = 0;
  EXPECT_FALSE(blockDataBlobInfoExceedLimit(&info, pBlock));

  blockDataDestroy(pBlock);
}

TEST(BlobBoundCheckTest, SeveralBlobColumnsFallBackToTheColumnWalk) {
  SSDataBlock *pBlock = makeBlobBoundTestBlock(2);
  ASSERT_NE(pBlock, nullptr);

  SBlobColInfo info = blockDataGetBlobCol(pBlock);
  // a single resolved column cannot bound the block: the caller must use the walk
  EXPECT_EQ(info.pCol, nullptr);
  EXPECT_EQ(info.numOfBlobCols, 2);

  SColumnInfoData *pFirst = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, 1);
  SColumnInfoData *pSecond = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, 2);
  ASSERT_NE(pFirst, nullptr);
  ASSERT_NE(pSecond, nullptr);

  // both columns under budget: the block stays open
  pFirst->varmeta.length = (uint32_t)kBlobTriggerLength;
  pSecond->varmeta.length = 1024;
  EXPECT_FALSE(blockDataBlobInfoExceedLimit(&info, pBlock));
  EXPECT_FALSE(blockDataBlobExceedLimit(pBlock));

  // only the second column crosses the budget: a single-pointer cache would miss it,
  // the fallback (column walk) must still close the block
  pSecond->varmeta.length = (uint32_t)kBlobTriggerLength + 1;
  EXPECT_TRUE(blockDataBlobInfoExceedLimit(&info, pBlock));
  EXPECT_TRUE(blockDataBlobExceedLimit(pBlock));

  blockDataDestroy(pBlock);
}
