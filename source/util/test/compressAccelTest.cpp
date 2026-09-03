/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * Verifies the runtime loading of an external (dlopen'ed) L2 compression
 * library: when TAOS_COMPRESS_ACCEL_ZLIB points at an external shared object,
 * tsCompressInit() must dlopen it, rebind compressL2Dict[L2_ZLIB] away from
 * the stock implementation, and the accelerated path must round-trip data and
 * interoperate with the stock implementation.
 *
 * The external library is not vendored. Instead the test asks the dynamic
 * loader itself for the system zlib (dlopen("libz.so.1") + dlinfo to recover
 * its real path), so it works on any Linux CI image that has zlib -- which is
 * essentially all of them, since the glibc ecosystem depends on it. If the
 * system libz cannot be found the test is skipped rather than failed, so a
 * minimal image never breaks CI.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <gtest/gtest.h>
#include <dlfcn.h>
#include <link.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <vector>

extern "C" {
#include "tcompression.h"
}

// Symbols defined in tcompression.c but not (unconditionally) declared in
// tcompression.h.
extern "C" TCmprL2FnSet compressL2Dict[];
extern "C" int32_t l2CompressImpl_zlib(const char *const, const int32_t, char *const, int32_t, const char, int8_t);
extern "C" int32_t l2DecompressImpl_zlib(const char *const, const int32_t, char *const, int32_t, const char);
extern "C" void tsCompressInit(char *, float, double, uint32_t, uint32_t, int32_t, const char *);
extern "C" void tsCompressExit(void);

namespace {

// Ask the dynamic loader to resolve `soname` and return the real path of the
// object it loads (e.g. "/usr/lib/x86_64-linux-gnu/libz.so.1"). Returns an
// empty string if the library cannot be loaded.
std::string findSystemSharedLib(const char *soname) {
  void *h = dlopen(soname, RTLD_NOW | RTLD_LOCAL);
  if (h == NULL) {
    return std::string();
  }
  struct link_map *lm = NULL;
  std::string      path;
  if (dlinfo(h, RTLD_DI_LINKMAP, &lm) == 0 && lm != NULL && lm->l_name != NULL && lm->l_name[0] != '\0') {
    path = lm->l_name;
  }
  dlclose(h);
  return path;
}

void clearAccelEnv() {
  unsetenv("TAOS_COMPRESS_ACCEL");
  unsetenv("TAOS_COMPRESS_ACCEL_ZLIB");
  unsetenv("TAOS_COMPRESS_ACCEL_ZSTD");
  unsetenv("TAOS_COMPRESS_ACCEL_LZ4");
}

}  // namespace

TEST(CompressAccelZlib, LoadsExternalLibAndRoundTrips) {
  // Locate the system zlib through the dynamic loader. No hard-coded path, so
  // this works across CI images; skip (not fail) when zlib is absent.
  const std::string libz = findSystemSharedLib("libz.so.1");
  if (libz.empty()) {
    GTEST_SKIP() << "system libz.so.1 not resolvable; cannot test accel loading";
  }

  clearAccelEnv();

  // Sanity: before any accel init the dispatch table must point at the stock
  // zlib implementation (this is a fresh process, so nothing has patched it).
  ASSERT_TRUE(compressL2Dict[L2_ZLIB].comprFn == l2CompressImpl_zlib)
      << "dispatch table is not on the stock zlib implementation at test start";

  // Point the accel loader at the system libz and run the same init that taosd
  // runs at startup.
  ASSERT_EQ(setenv("TAOS_COMPRESS_ACCEL_ZLIB", libz.c_str(), 1), 0);
  char lossyCols[] = "";
  tsCompressInit(lossyCols, 0.0f, 0.0, 0, 0, 0, "");
  clearAccelEnv();

  // The dispatch entry must now be rebound away from the stock implementation.
  ASSERT_TRUE(compressL2Dict[L2_ZLIB].comprFn != l2CompressImpl_zlib)
      << "compressL2Dict[L2_ZLIB] was not patched; accel library " << libz << " not loaded";

  // Build a test block: the first half is highly compressible, the second half
  // is high-entropy, so both the compress and the "stored verbatim" branches
  // of the codecs are exercised where applicable.
  const int32_t kSize = 1 << 16;  // 64 KiB
  std::vector<char> input((size_t)kSize);
  for (size_t i = 0; i < (size_t)kSize / 2; ++i) {
    input[i] = (char)(i % 7);
  }
  uint32_t x = 0x12345678u;
  for (size_t i = (size_t)kSize / 2; i < (size_t)kSize; ++i) {
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    input[i] = (char)(x & 0xff);
  }

  std::vector<char> comp((size_t)kSize * 2 + 1024);
  std::vector<char> decomp((size_t)kSize);
  const int8_t kLvl = 6;  // zlib "medium" (compressL2LevelDict: {1, 6, 9})

  // 1. Compress + decompress through the (patched) dispatch table.
  int32_t csz = compressL2Dict[L2_ZLIB].comprFn(input.data(), kSize, comp.data(), (int32_t)comp.size(), 0, kLvl);
  ASSERT_GT(csz, 0);
  int32_t dsz = compressL2Dict[L2_ZLIB].decomprFn(comp.data(), csz, decomp.data(), kSize, 0);
  ASSERT_EQ(dsz, kSize);
  ASSERT_EQ(memcmp(decomp.data(), input.data(), (size_t)kSize), 0);

  // 2. Interop, stock -> accel: a block produced by the statically linked
  //    stock zlib must decompress correctly through the accel entry.
  std::vector<char> stockComp((size_t)kSize * 2 + 1024);
  int32_t           scsz = l2CompressImpl_zlib(input.data(), kSize, stockComp.data(), (int32_t)stockComp.size(), 0, kLvl);
  ASSERT_GT(scsz, 0);
  dsz = compressL2Dict[L2_ZLIB].decomprFn(stockComp.data(), scsz, decomp.data(), kSize, 0);
  ASSERT_EQ(dsz, kSize);
  ASSERT_EQ(memcmp(decomp.data(), input.data(), (size_t)kSize), 0);

  // 3. Interop, accel -> stock: the stock decompressor must accept what the
  //    external library produced (ABI-compatible drop-in contract).
  dsz = l2DecompressImpl_zlib(comp.data(), csz, decomp.data(), kSize, 0);
  ASSERT_EQ(dsz, kSize);
  ASSERT_EQ(memcmp(decomp.data(), input.data(), (size_t)kSize), 0);

  // 4. Verbatim marker path: a manually built marker-0 block must be returned
  //    untouched by the accel decompressor.
  const char kPayload[] = "not-compressed-payload";
  std::vector<char> raw((size_t)(sizeof(kPayload) + 1));
  raw[0] = 0;
  memcpy(raw.data() + 1, kPayload, sizeof(kPayload));
  dsz = compressL2Dict[L2_ZLIB].decomprFn(raw.data(), (int32_t)(sizeof(kPayload) + 1), decomp.data(),
                                          (int32_t)sizeof(kPayload) + 16, 0);
  ASSERT_EQ(dsz, (int32_t)sizeof(kPayload));
  ASSERT_EQ(memcmp(decomp.data(), kPayload, sizeof(kPayload)), 0);

  tsCompressExit();
}
