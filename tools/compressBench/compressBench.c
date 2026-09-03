/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * L2 compression layer micro-benchmark.
 *
 * Measures per-block compress and decompress latency + throughput for each
 * codec in the TDengine L2 dispatch table.  Supports multiple iterations,
 * warmup rounds, and outputs p50/p95/stdev so stock vs. accelerated backends
 * can be compared side-by-side.
 *
 * Build: linked via tools/compressBench/CMakeLists.txt (BUILD_TOOLS=ON).
 * Run:   compressBench --help
 */

// Standalone bench: it does not participate in the TDengine memory/file/clock
// accounting layer, so it calls libc malloc/free/fopen/clock_gettime directly.
// The escape hatch must be defined BEFORE any include that pulls in os/.
#define ALLOW_FORBID_FUNC

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Pull in the L2 types and tsCompressInit/tsCompressExit */
#include "tcompression.h"

/* -------------------------------------------------------------------------
 * External symbols from tcompression.c
 * ---------------------------------------------------------------------- */
extern TCmprL2FnSet compressL2Dict[];

/* Stock compress pointers – used for backend=stock|accel detection.
 * On Windows/Darwin tcompression.c does NOT define the zlib/zstd/xz variants;
 * compressL2Dict[] there aliases all of them to l2CompressImpl_lz4 instead.
 * Only extern what actually exists on the target platform or the link breaks
 * with "Undefined symbols _l2CompressImpl_{zlib,zstd,xz}" on macOS/Windows. */
extern int32_t l2CompressImpl_lz4(const char *const, const int32_t, char *const, int32_t, const char, int8_t);
#if !defined(WINDOWS) && !defined(_TD_DARWIN_64)
extern int32_t l2CompressImpl_zlib(const char *const, const int32_t, char *const, int32_t, const char, int8_t);
extern int32_t l2CompressImpl_zstd(const char *const, const int32_t, char *const, int32_t, const char, int8_t);
extern int32_t l2CompressImpl_xz(const char *const, const int32_t, char *const, int32_t, const char, int8_t);
#endif

/* tsGetCompressL2Level lives in tcompression.c but has no header prototype */
extern int8_t tsGetCompressL2Level(uint8_t alg, uint8_t lvl);

/* -------------------------------------------------------------------------
 * Small helpers
 * ---------------------------------------------------------------------- */

static double ns_to_ms(double ns) { return ns * 1e-6; }
static double ns_to_mbs(double ns, size_t bytes) {
  /* MB/s = bytes / (ns * 1e-9) / 1e6 */
  return (ns > 0.0) ? ((double)bytes / ns * 1e3) : 0.0;
}

/* xorshift64 – fast PRNG with good entropy, no deps */
static uint64_t xstate = 42ULL;
static uint64_t xorshift64(void) {
  xstate ^= xstate << 13;
  xstate ^= xstate >> 7;
  xstate ^= xstate << 17;
  return xstate;
}

static int cmp_double(const void *a, const void *b) {
  double x = *(const double *)a;
  double y = *(const double *)b;
  return (x > y) - (x < y);
}

typedef struct {
  double mean;
  double p50;
  double p95;
  double stdev;
} Stats;

static Stats compute_stats(double *samples, int n) {
  Stats s = {0};
  if (n <= 0) return s;
  qsort(samples, n, sizeof(double), cmp_double);
  double sum = 0.0;
  for (int i = 0; i < n; i++) sum += samples[i];
  s.mean = sum / n;
  s.p50 = samples[(int)(n * 0.50)];
  s.p95 = samples[(int)(n * 0.95 < n - 1 ? n * 0.95 : n - 1)];
  double var = 0.0;
  for (int i = 0; i < n; i++) {
    double d = samples[i] - s.mean;
    var += d * d;
  }
  s.stdev = (n > 1) ? sqrt(var / (n - 1)) : 0.0;
  return s;
}

/* -------------------------------------------------------------------------
 * Data generators
 * ---------------------------------------------------------------------- */

typedef enum { SHAPE_RANDOM = 0, SHAPE_REPEATING, SHAPE_SEQUENTIAL, SHAPE_MIXED } Shape;

static void gen_random(uint8_t *buf, size_t sz) {
  size_t i = 0;
  for (; i + 8 <= sz; i += 8) {
    uint64_t v = xorshift64();
    memcpy(buf + i, &v, 8);
  }
  if (i < sz) {
    uint64_t v = xorshift64();
    memcpy(buf + i, &v, sz - i);
  }
}

static void gen_repeating(uint8_t *buf, size_t sz) {
  /* 64-byte pattern repeated */
  uint8_t pat[64];
  for (int i = 0; i < 64; i++) pat[i] = (uint8_t)(i * 3 + 7);
  for (size_t i = 0; i < sz; i++) buf[i] = pat[i % 64];
}

static void gen_sequential(uint8_t *buf, size_t sz) {
  /* Monotonic int64 timestamps: base + i*1000000 (1ms apart) */
  int64_t ts = 1700000000000000000LL; /* 2023-ish nanoseconds */
  size_t  i  = 0;
  for (; i + 8 <= sz; i += 8, ts += 1000000LL) {
    memcpy(buf + i, &ts, 8);
  }
  if (i < sz) {
    memcpy(buf + i, &ts, sz - i);
  }
}

static void gen_mixed(uint8_t *buf, size_t sz) {
  size_t third = sz / 3;
  size_t rem   = sz - 2 * third;
  gen_random(buf, third);
  gen_repeating(buf + third, third);
  gen_sequential(buf + 2 * third, rem);
}

static void fill_input(uint8_t *buf, size_t sz, Shape shape, uint64_t seed) {
  xstate = seed ? seed : 42ULL;
  switch (shape) {
    case SHAPE_RANDOM:    gen_random(buf, sz); break;
    case SHAPE_REPEATING: gen_repeating(buf, sz); break;
    case SHAPE_SEQUENTIAL:gen_sequential(buf, sz); break;
    default:              gen_mixed(buf, sz); break;
  }
}

/* -------------------------------------------------------------------------
 * Timing
 * ---------------------------------------------------------------------- */

static inline double now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec * 1e9 + (double)ts.tv_nsec;
}

/* -------------------------------------------------------------------------
 * CSV helpers
 * ---------------------------------------------------------------------- */

static void csv_ensure_header(FILE *f) {
  /* Write header when file is newly created / empty */
  if (ftell(f) == 0) {
    fprintf(f,
            "timestamp,label,codec,size_bytes,lvl,shape,iters,"
            "c_mean_ms,c_p50_ms,c_p95_ms,c_stdev_ms,c_throughput_MBs,"
            "d_mean_ms,d_p50_ms,d_p95_ms,d_stdev_ms,d_throughput_MBs,"
            "ratio,backend\n");
  }
}

/* -------------------------------------------------------------------------
 * Backend detection
 * ---------------------------------------------------------------------- */

static const char *detect_backend(int codec_idx) {
  __data_compress_l2_fn_t fn = compressL2Dict[codec_idx].comprFn;
  switch (codec_idx) {
    case L2_LZ4:  return (fn == l2CompressImpl_lz4)  ? "stock" : "accel";
#if !defined(WINDOWS) && !defined(_TD_DARWIN_64)
    case L2_ZLIB: return (fn == l2CompressImpl_zlib) ? "stock" : "accel";
    case L2_ZSTD: return (fn == l2CompressImpl_zstd) ? "stock" : "accel";
    case L2_XZ:   return (fn == l2CompressImpl_xz)   ? "stock" : "accel";
#else
    // Windows / Darwin alias these three to lz4 in tcompression.c, so the
    // stock fn pointer is the lz4 one. accel substitution isn't compiled
    // on those platforms anyway (BUILD_WITH_ACCEL_COMPRESS is Linux-only).
    case L2_ZLIB:
    case L2_ZSTD:
    case L2_XZ:   return (fn == l2CompressImpl_lz4)  ? "stock" : "accel";
#endif
    default:      return "auto";
  }
}

/* -------------------------------------------------------------------------
 * Per-codec bench
 * ---------------------------------------------------------------------- */

typedef struct {
  int         codec_idx;
  const char *codec_name;
  size_t      input_size;
  int         warmup;
  int         iters;
  int8_t      level;
  const char *shape_name;
  const char *label;
  const char *csv_path;
} BenchParams;

static int run_bench(const BenchParams *p, const uint8_t *input, uint8_t *compressed, uint8_t *decompressed) {
  size_t out_buf_sz = p->input_size * 2 + 1024;
  int    total      = p->warmup + p->iters;

  double *c_ns = (double *)malloc(sizeof(double) * (size_t)total);
  double *d_ns = (double *)malloc(sizeof(double) * (size_t)total);
  if (!c_ns || !d_ns) {
    fprintf(stderr, "OOM allocating sample arrays\n");
    free(c_ns);
    free(d_ns);
    return 1;
  }

  tsCompressInit("", 0.0f, 0.0, 0, 0, 0, "");

  int exit_code = 0;
  for (int it = 0; it < total && exit_code == 0; it++) {
    /* Compress */
    double t0 = now_ns();
    int32_t compressed_sz = compressL2Dict[p->codec_idx].comprFn(
        (const char *)input, (int32_t)p->input_size,
        (char *)compressed, (int32_t)out_buf_sz,
        /* type */ 0, p->level);
    double t1 = now_ns();

    if (compressed_sz <= 0) {
      fprintf(stderr, "codec %s compress returned %d at iter %d\n",
              p->codec_name, compressed_sz, it);
      exit_code = 2;
      break;
    }

    /* Decompress */
    double t2 = now_ns();
    int32_t decompressed_sz = compressL2Dict[p->codec_idx].decomprFn(
        (const char *)compressed, compressed_sz,
        (char *)decompressed, (int32_t)p->input_size,
        /* type */ 0);
    double t3 = now_ns();

    if (decompressed_sz < 0) {
      fprintf(stderr, "codec %s decompress returned %d at iter %d\n",
              p->codec_name, decompressed_sz, it);
      exit_code = 3;
      break;
    }

    /* Integrity check */
    if ((size_t)decompressed_sz != p->input_size ||
        memcmp(input, decompressed, p->input_size) != 0) {
      fprintf(stderr,
              "INTEGRITY FAIL: codec=%s iter=%d decompressed_sz=%d expected=%zu\n",
              p->codec_name, it, decompressed_sz, p->input_size);
      exit_code = 4;
      break;
    }

    c_ns[it] = t1 - t0;
    d_ns[it] = t3 - t2;
  }

  tsCompressExit();

  if (exit_code != 0) {
    free(c_ns);
    free(d_ns);
    return exit_code;
  }

  /* Skip warmup samples */
  double *c_meas = c_ns + p->warmup;
  double *d_meas = d_ns + p->warmup;
  int     n      = p->iters;

  /* Convert to ms for display */
  double *c_ms_arr = (double *)malloc(sizeof(double) * (size_t)n);
  double *d_ms_arr = (double *)malloc(sizeof(double) * (size_t)n);
  if (!c_ms_arr || !d_ms_arr) {
    free(c_ns); free(d_ns); free(c_ms_arr); free(d_ms_arr);
    return 1;
  }
  for (int i = 0; i < n; i++) {
    c_ms_arr[i] = ns_to_ms(c_meas[i]);
    d_ms_arr[i] = ns_to_ms(d_meas[i]);
  }

  Stats cs = compute_stats(c_ms_arr, n);
  Stats ds = compute_stats(d_ms_arr, n);

  /* Use mean ns for throughput */
  double c_mean_ns = 0.0, d_mean_ns = 0.0;
  for (int i = 0; i < n; i++) { c_mean_ns += c_meas[i]; d_mean_ns += d_meas[i]; }
  c_mean_ns /= n; d_mean_ns /= n;

  double c_mbps = ns_to_mbs(c_mean_ns, p->input_size);
  double d_mbps = ns_to_mbs(d_mean_ns, p->input_size);

  /* Compute compression ratio from last iteration */
  /* Re-run once outside timing to get stable compressed_sz */
  tsCompressInit("", 0.0f, 0.0, 0, 0, 0, "");
  int32_t last_csz = compressL2Dict[p->codec_idx].comprFn(
      (const char *)input, (int32_t)p->input_size,
      (char *)compressed, (int32_t)out_buf_sz,
      0, p->level);
  tsCompressExit();
  double ratio = (last_csz > 0) ? ((double)p->input_size / (double)last_csz) : 0.0;

  const char *backend = detect_backend(p->codec_idx);

  double size_mib = (double)p->input_size / (1024.0 * 1024.0);
  char   size_str[32];
  if (size_mib >= 1.0)
    snprintf(size_str, sizeof(size_str), "%.0fMiB", size_mib);
  else
    snprintf(size_str, sizeof(size_str), "%.0fKiB", size_mib * 1024.0);

  /* stdout summary */
  printf("BENCH %-6s size=%-7s lvl=%-6s shape=%-10s iters=%d\n"
         "       compress   mean=%6.2fms p50=%6.2f p95=%6.2f stdev=%5.2f  throughput=%7.1fMB/s\n"
         "       decompress mean=%6.2fms p50=%6.2f p95=%6.2f stdev=%5.2f  throughput=%7.1fMB/s\n"
         "       ratio=%.2fx  backend=%s\n\n",
         p->codec_name, size_str, "medium", p->shape_name, n,
         cs.mean, cs.p50, cs.p95, cs.stdev, c_mbps,
         ds.mean, ds.p50, ds.p95, ds.stdev, d_mbps,
         ratio, backend);

  /* CSV append */
  if (p->csv_path) {
    FILE *cf = fopen(p->csv_path, "a");
    if (cf) {
      csv_ensure_header(cf);
      time_t now = time(NULL);
      fprintf(cf,
              "%lld,%s,%s,%zu,%d,%s,%d,"
              "%.4f,%.4f,%.4f,%.4f,%.2f,"
              "%.4f,%.4f,%.4f,%.4f,%.2f,"
              "%.4f,%s\n",
              (long long)now, p->label, p->codec_name, p->input_size,
              (int)p->level, p->shape_name, n,
              cs.mean, cs.p50, cs.p95, cs.stdev, c_mbps,
              ds.mean, ds.p50, ds.p95, ds.stdev, d_mbps,
              ratio, backend);
      fclose(cf);
    } else {
      fprintf(stderr, "warning: cannot open csv file: %s\n", p->csv_path);
    }
  }

  free(c_ns); free(d_ns); free(c_ms_arr); free(d_ms_arr);
  return 0;
}

/* -------------------------------------------------------------------------
 * CLI parsing
 * ---------------------------------------------------------------------- */

static void usage(const char *prog) {
  fprintf(stderr,
    "Usage: %s [options]\n"
    "\n"
    "  --codec {lz4|zlib|zstd|xz|all}         default: all\n"
    "  --size <MiB>                             default: 1   (try 0.004 for 4KiB)\n"
    "  --iters <N>                              default: 30\n"
    "  --warmup <N>                             default: 5\n"
    "  --lvl {low|medium|high}                  default: medium\n"
    "  --shape {random|repeating|sequential|mixed}  default: mixed\n"
    "  --seed <u64>                             default: 42\n"
    "  --csv <path>                             append CSV row(s)\n"
    "  --label <str>                            opaque label (default: bench)\n"
    "\n", prog);
}

int main(int argc, char **argv) {
  /* Defaults */
  int         codec_all  = 1;
  int         codec_idx  = L2_LZ4; /* used when codec_all==0 */
  const char *codec_name = "lz4";
  double      size_mib   = 1.0;
  int         iters      = 30;
  int         warmup     = 5;
  uint8_t     lvl_enum   = L2_LVL_MEDIUM;
  Shape       shape      = SHAPE_MIXED;
  const char *shape_name = "mixed";
  uint64_t    seed       = 42ULL;
  const char *csv_path   = NULL;
  const char *label      = "bench";

  /* Hand-rolled option parser */
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
      usage(argv[0]);
      return 0;
    }
#define NEED_ARG(flag)                                                   \
  do {                                                                   \
    if (i + 1 >= argc) {                                                 \
      fprintf(stderr, "error: %s requires an argument\n", flag);         \
      return 1;                                                          \
    }                                                                    \
    i++;                                                                 \
  } while (0)

    else if (strcmp(argv[i], "--codec") == 0) {
      NEED_ARG("--codec");
      const char *v = argv[i];
      if (strcmp(v, "all") == 0) {
        codec_all = 1;
      } else if (strcmp(v, "lz4") == 0) {
        codec_all = 0; codec_idx = L2_LZ4;  codec_name = "lz4";
      } else if (strcmp(v, "zlib") == 0) {
        codec_all = 0; codec_idx = L2_ZLIB; codec_name = "zlib";
      } else if (strcmp(v, "zstd") == 0) {
        codec_all = 0; codec_idx = L2_ZSTD; codec_name = "zstd";
      } else if (strcmp(v, "xz") == 0) {
        codec_all = 0; codec_idx = L2_XZ;   codec_name = "xz";
      } else {
        fprintf(stderr, "error: unknown codec '%s'\n", v);
        return 1;
      }
    } else if (strcmp(argv[i], "--size") == 0) {
      NEED_ARG("--size");
      size_mib = atof(argv[i]);
      if (size_mib <= 0.0) { fprintf(stderr, "error: --size must be > 0\n"); return 1; }
    } else if (strcmp(argv[i], "--iters") == 0) {
      NEED_ARG("--iters");
      iters = atoi(argv[i]);
      if (iters < 1) { fprintf(stderr, "error: --iters must be >= 1\n"); return 1; }
    } else if (strcmp(argv[i], "--warmup") == 0) {
      NEED_ARG("--warmup");
      warmup = atoi(argv[i]);
      if (warmup < 0) { fprintf(stderr, "error: --warmup must be >= 0\n"); return 1; }
    } else if (strcmp(argv[i], "--lvl") == 0) {
      NEED_ARG("--lvl");
      const char *v = argv[i];
      if      (strcmp(v, "low")    == 0) lvl_enum = L2_LVL_LOW;
      else if (strcmp(v, "medium") == 0) lvl_enum = L2_LVL_MEDIUM;
      else if (strcmp(v, "high")   == 0) lvl_enum = L2_LVL_HIGH;
      else { fprintf(stderr, "error: unknown level '%s'\n", v); return 1; }
    } else if (strcmp(argv[i], "--shape") == 0) {
      NEED_ARG("--shape");
      const char *v = argv[i];
      if      (strcmp(v, "random")     == 0) { shape = SHAPE_RANDOM;     shape_name = "random"; }
      else if (strcmp(v, "repeating")  == 0) { shape = SHAPE_REPEATING;  shape_name = "repeating"; }
      else if (strcmp(v, "sequential") == 0) { shape = SHAPE_SEQUENTIAL; shape_name = "sequential"; }
      else if (strcmp(v, "mixed")      == 0) { shape = SHAPE_MIXED;      shape_name = "mixed"; }
      else { fprintf(stderr, "error: unknown shape '%s'\n", v); return 1; }
    } else if (strcmp(argv[i], "--seed") == 0) {
      NEED_ARG("--seed");
      seed = (uint64_t)strtoull(argv[i], NULL, 10);
    } else if (strcmp(argv[i], "--csv") == 0) {
      NEED_ARG("--csv");
      csv_path = argv[i];
    } else if (strcmp(argv[i], "--label") == 0) {
      NEED_ARG("--label");
      label = argv[i];
    } else {
      fprintf(stderr, "error: unknown option '%s'\n", argv[i]);
      usage(argv[0]);
      return 1;
    }
#undef NEED_ARG
  }

  /* Allocate buffers */
  size_t input_size = (size_t)(size_mib * 1024.0 * 1024.0);
  if (input_size < 16) input_size = 16;
  size_t out_buf_sz = input_size * 2 + 1024;

  uint8_t *input       = (uint8_t *)malloc(input_size);
  uint8_t *compressed  = (uint8_t *)malloc(out_buf_sz);
  uint8_t *decompressed= (uint8_t *)malloc(input_size);
  if (!input || !compressed || !decompressed) {
    fprintf(stderr, "OOM: cannot allocate %.2f MiB benchmark buffers\n", size_mib);
    free(input); free(compressed); free(decompressed);
    return 1;
  }

  fill_input(input, input_size, shape, seed);

  /* Print header */
  double size_mib_act = (double)input_size / (1024.0 * 1024.0);
  printf("compressBench: size=%.4f MiB iters=%d warmup=%d seed=%llu label=%s\n\n",
         size_mib_act, iters, warmup, (unsigned long long)seed, label);

  /* Codec list for --codec=all */
  typedef struct { int idx; const char *name; } CodecEntry;
  static const CodecEntry ALL_CODECS[] = {
    {L2_LZ4,  "lz4"},
    {L2_ZLIB, "zlib"},
    {L2_ZSTD, "zstd"},
    {L2_XZ,   "xz"},
  };
  static const int N_CODECS = (int)(sizeof(ALL_CODECS) / sizeof(ALL_CODECS[0]));

  int ret = 0;
  if (codec_all) {
    for (int c = 0; c < N_CODECS && ret == 0; c++) {
      int8_t level = tsGetCompressL2Level((uint8_t)ALL_CODECS[c].idx, lvl_enum);
      BenchParams bp = {
        .codec_idx  = ALL_CODECS[c].idx,
        .codec_name = ALL_CODECS[c].name,
        .input_size = input_size,
        .warmup     = warmup,
        .iters      = iters,
        .level      = level,
        .shape_name = shape_name,
        .label      = label,
        .csv_path   = csv_path,
      };
      ret = run_bench(&bp, input, compressed, decompressed);
      if (ret != 0) {
        fprintf(stderr, "FAIL: codec=%s rc=%d\n", ALL_CODECS[c].name, ret);
      }
    }
  } else {
    int8_t level = tsGetCompressL2Level((uint8_t)codec_idx, lvl_enum);
    BenchParams bp = {
      .codec_idx  = codec_idx,
      .codec_name = codec_name,
      .input_size = input_size,
      .warmup     = warmup,
      .iters      = iters,
      .level      = level,
      .shape_name = shape_name,
      .label      = label,
      .csv_path   = csv_path,
    };
    ret = run_bench(&bp, input, compressed, decompressed);
    if (ret != 0) {
      fprintf(stderr, "FAIL: codec=%s rc=%d\n", codec_name, ret);
    }
  }

  free(input);
  free(compressed);
  free(decompressed);
  return ret;
}
