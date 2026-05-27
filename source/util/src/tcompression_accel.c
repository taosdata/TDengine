/*
 * Runtime-pluggable accelerated L2 compression backends.
 *
 * Stock zlib / zstd / lz4 are statically linked into util at build time and
 * remain the default. When BUILD_WITH_ACCEL_COMPRESS=ON and the operator
 * points TAOS_COMPRESS_ACCEL{,_ZLIB,_ZSTD,_LZ4} at a drop-in ABI-compatible
 * shared object (e.g. an Intel QAT/IAA-accelerated libz, ISA-L's libz, an
 * arm64-optimized libzstd), startup dlopens it, resolves the public symbols,
 * and rebinds the corresponding entries in compressL2Dict[]. Any failure
 * (env unset, file missing, missing symbols) leaves the stock implementation
 * in place and is logged.
 *
 * The whole translation unit compiles to an empty object when the build
 * option is off, so callers don't need any #ifdef around tcompressionAccelInit().
 *
 * See docs/zh/26-tdinternal/11-compress.md for the operator-facing contract.
 */

#ifdef TD_BUILD_WITH_ACCEL_COMPRESS

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "tcompression.h"
#include "tdef.h"
#include "tlog.h"

// Stock dispatch table lives in tcompression.c
extern TCmprL2FnSet compressL2Dict[];

// ---- Resolved function pointers per backend ---------------------------------

typedef struct {
  void *handle;
  int (*compress2_fn)(unsigned char *dest, unsigned long *destLen,
                      const unsigned char *source, unsigned long sourceLen, int level);
  int (*uncompress_fn)(unsigned char *dest, unsigned long *destLen,
                       const unsigned char *source, unsigned long sourceLen);
} SZlibAccel;

typedef struct {
  void *handle;
  size_t (*compress_fn)(void *dst, size_t dstCapacity,
                        const void *src, size_t srcSize, int compressionLevel);
  size_t (*decompress_fn)(void *dst, size_t dstCapacity,
                          const void *src, size_t compressedSize);
} SZstdAccel;

typedef struct {
  void *handle;
  int (*compress_default_fn)(const char *src, char *dst, int srcSize, int dstCapacity);
  int (*decompress_safe_fn)(const char *src, char *dst, int compressedSize, int dstCapacity);
} SLz4Accel;

static SZlibAccel g_zlibAccel;
static SZstdAccel g_zstdAccel;
static SLz4Accel  g_lz4Accel;

// ---- Wrappers (signatures match l2*Impl_* in tcompression.c) ---------------

static int32_t accelCompress_zlib(const char *const input, const int32_t inputSize, char *const output,
                                  int32_t outputSize, const char type, int8_t lvl) {
  unsigned long dstLen = (unsigned long)(outputSize - 1);
  int           rc     = g_zlibAccel.compress2_fn((unsigned char *)(output + 1), &dstLen,
                                                   (const unsigned char *)input, (unsigned long)inputSize, lvl);
  if (rc == 0 /* Z_OK */) {
    output[0] = 1;
    return (int32_t)dstLen + 1;
  }
  output[0] = 0;
  memcpy(output + 1, input, inputSize);
  return inputSize + 1;
}

static int32_t accelDecompress_zlib(const char *const input, const int32_t compressedSize, char *const output,
                                    int32_t outputSize, const char type) {
  if (input[0] == 1) {
    unsigned long len = (unsigned long)outputSize;
    int           rc  = g_zlibAccel.uncompress_fn((unsigned char *)output, &len,
                                                  (const unsigned char *)input + 1,
                                                  (unsigned long)(compressedSize - 1));
    if (rc == 0) return (int32_t)len;
    return TSDB_CODE_THIRDPARTY_ERROR;
  } else if (input[0] == 0) {
    memcpy(output, input + 1, compressedSize - 1);
    return compressedSize - 1;
  }
  return TSDB_CODE_THIRDPARTY_ERROR;
}

static int32_t accelCompress_zstd(const char *const input, const int32_t inputSize, char *const output,
                                  int32_t outputSize, const char type, int8_t lvl) {
  size_t len = g_zstdAccel.compress_fn(output + 1, (size_t)(outputSize - 1), input, (size_t)inputSize, lvl);
  // ZSTD_isError() returns a value > srcSize for error codes, so the same
  // sentinel as the stock path catches both error and ratio-not-worth-it.
  if (len > (size_t)inputSize) {
    output[0] = 0;
    memcpy(output + 1, input, inputSize);
    return inputSize + 1;
  }
  output[0] = 1;
  return (int32_t)len + 1;
}

static int32_t accelDecompress_zstd(const char *const input, const int32_t compressedSize, char *const output,
                                    int32_t outputSize, const char type) {
  if (input[0] == 1) {
    return (int32_t)g_zstdAccel.decompress_fn(output, (size_t)outputSize, input + 1, (size_t)(compressedSize - 1));
  } else if (input[0] == 0) {
    memcpy(output, input + 1, compressedSize - 1);
    return compressedSize - 1;
  }
  return TSDB_CODE_THIRDPARTY_ERROR;
}

static int32_t accelCompress_lz4(const char *const input, const int32_t inputSize, char *const output,
                                 int32_t outputSize, const char type, int8_t lvl) {
  const int32_t n = g_lz4Accel.compress_default_fn(input, output + 1, inputSize, outputSize - 1);
  if (n <= 0 || n > inputSize) {
    output[0] = 0;
    memcpy(output + 1, input, inputSize);
    return inputSize + 1;
  }
  output[0] = 1;
  return n + 1;
}

static int32_t accelDecompress_lz4(const char *const input, const int32_t compressedSize, char *const output,
                                   int32_t outputSize, const char type) {
  if (input[0] == 1) {
    const int32_t n = g_lz4Accel.decompress_safe_fn(input + 1, output, compressedSize - 1, outputSize);
    if (n < 0) {
      uError("accel lz4: LZ4_decompress_safe returned %d", n);
      return TSDB_CODE_THIRDPARTY_ERROR;
    }
    return n;
  } else if (input[0] == 0) {
    memcpy(output, input + 1, compressedSize - 1);
    return compressedSize - 1;
  }
  return TSDB_CODE_THIRDPARTY_ERROR;
}

// ---- dlopen + dlsym + dispatch-table patch ---------------------------------

static void *openLibOrLog(const char *codec, const char *path) {
  void *h = dlopen(path, RTLD_NOW | RTLD_LOCAL);
  if (h == NULL) {
    uWarn("accel %s: dlopen(\"%s\") failed: %s", codec, path, dlerror());
  }
  return h;
}

static void *symOrLog(void *h, const char *codec, const char *sym) {
  dlerror();  // clear stale error
  void *p = dlsym(h, sym);
  const char *err = dlerror();
  if (err != NULL) {
    uWarn("accel %s: dlsym(\"%s\") failed: %s", codec, sym, err);
    return NULL;
  }
  return p;
}

static void tryLoadZlibAccel(const char *path) {
  void *h = openLibOrLog("zlib", path);
  if (h == NULL) return;

  void *fn_c = symOrLog(h, "zlib", "compress2");
  void *fn_u = symOrLog(h, "zlib", "uncompress");
  if (fn_c == NULL || fn_u == NULL) {
    dlclose(h);
    return;
  }
  g_zlibAccel.handle        = h;
  g_zlibAccel.compress2_fn  = (int (*)(unsigned char *, unsigned long *, const unsigned char *, unsigned long, int))fn_c;
  g_zlibAccel.uncompress_fn = (int (*)(unsigned char *, unsigned long *, const unsigned char *, unsigned long))fn_u;
  compressL2Dict[L2_ZLIB].comprFn   = accelCompress_zlib;
  compressL2Dict[L2_ZLIB].decomprFn = accelDecompress_zlib;
  uInfo("accel zlib: loaded from %s, L2_ZLIB dispatch patched", path);
}

static void tryLoadZstdAccel(const char *path) {
  void *h = openLibOrLog("zstd", path);
  if (h == NULL) return;

  void *fn_c = symOrLog(h, "zstd", "ZSTD_compress");
  void *fn_u = symOrLog(h, "zstd", "ZSTD_decompress");
  if (fn_c == NULL || fn_u == NULL) {
    dlclose(h);
    return;
  }
  g_zstdAccel.handle         = h;
  g_zstdAccel.compress_fn    = (size_t (*)(void *, size_t, const void *, size_t, int))fn_c;
  g_zstdAccel.decompress_fn  = (size_t (*)(void *, size_t, const void *, size_t))fn_u;
  compressL2Dict[L2_ZSTD].comprFn   = accelCompress_zstd;
  compressL2Dict[L2_ZSTD].decomprFn = accelDecompress_zstd;
  uInfo("accel zstd: loaded from %s, L2_ZSTD dispatch patched", path);
}

static void tryLoadLz4Accel(const char *path) {
  void *h = openLibOrLog("lz4", path);
  if (h == NULL) return;

  void *fn_c = symOrLog(h, "lz4", "LZ4_compress_default");
  void *fn_u = symOrLog(h, "lz4", "LZ4_decompress_safe");
  if (fn_c == NULL || fn_u == NULL) {
    dlclose(h);
    return;
  }
  g_lz4Accel.handle               = h;
  g_lz4Accel.compress_default_fn  = (int (*)(const char *, char *, int, int))fn_c;
  g_lz4Accel.decompress_safe_fn   = (int (*)(const char *, char *, int, int))fn_u;
  compressL2Dict[L2_LZ4].comprFn   = accelCompress_lz4;
  compressL2Dict[L2_LZ4].decomprFn = accelDecompress_lz4;
  uInfo("accel lz4: loaded from %s, L2_LZ4 dispatch patched", path);
}

// Resolve "<dir>/<basename>" without overflowing the destination buffer.
// Returns 0 on success, -1 on overflow (with the buffer left empty).
static int joinDirAndName(char *dst, size_t dstSize, const char *dir, const char *base) {
  int n = snprintf(dst, dstSize, "%s/%s", dir, base);
  if (n < 0 || (size_t)n >= dstSize) {
    dst[0] = '\0';
    return -1;
  }
  return 0;
}

void tcompressionAccelInit(void) {
  const char *dir     = getenv("TAOS_COMPRESS_ACCEL");
  const char *zlibLib = getenv("TAOS_COMPRESS_ACCEL_ZLIB");
  const char *zstdLib = getenv("TAOS_COMPRESS_ACCEL_ZSTD");
  const char *lz4Lib  = getenv("TAOS_COMPRESS_ACCEL_LZ4");

  if (dir == NULL && zlibLib == NULL && zstdLib == NULL && lz4Lib == NULL) {
    uInfo("accel compression: TAOS_COMPRESS_ACCEL{,_ZLIB,_ZSTD,_LZ4} unset; using stock L2 implementations");
    return;
  }
  if (dir != NULL && strcmp(dir, "off") == 0) {
    uInfo("accel compression: explicitly disabled (TAOS_COMPRESS_ACCEL=off)");
    return;
  }

  char buf[1024];

  if (zlibLib != NULL) {
    tryLoadZlibAccel(zlibLib);
  } else if (dir != NULL && joinDirAndName(buf, sizeof(buf), dir, "libz.so") == 0) {
    tryLoadZlibAccel(buf);
  }

  if (zstdLib != NULL) {
    tryLoadZstdAccel(zstdLib);
  } else if (dir != NULL && joinDirAndName(buf, sizeof(buf), dir, "libzstd.so") == 0) {
    tryLoadZstdAccel(buf);
  }

  if (lz4Lib != NULL) {
    tryLoadLz4Accel(lz4Lib);
  } else if (dir != NULL && joinDirAndName(buf, sizeof(buf), dir, "liblz4.so") == 0) {
    tryLoadLz4Accel(buf);
  }
}

#else  // !TD_BUILD_WITH_ACCEL_COMPRESS — no-op stub so callers stay simple

void tcompressionAccelInit(void) {}

#endif  // TD_BUILD_WITH_ACCEL_COMPRESS
