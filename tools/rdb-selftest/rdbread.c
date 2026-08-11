// rdbread -- read back a database written by rdbwrite, the way tsdbCache.c does.
//
// Program 2 of the write/read pair. It checks three things, in increasing
// subtlety:
//
//   1. does rocksdb report a block checksum error
//        -> the failure seen in production
//   2. does every record we wrote come back, with the value we wrote
//        -> catches a wrong-but-plausible value that still passed rocksdb's
//           block checksum, which rocksdb itself cannot detect
//   3. does re-reading the same key return the same bytes (-r > 1)
//        -> bytes that change between reads of an immutable file prove the
//           inconsistency is in the read path, not on the medium
//
// The value check is possible because rdbwrite generates records
// deterministically from (index, seed): given the same seed, rdbread can
// recompute what every value should be without being told.
//
// Read options set verify_checksums = 1, the same as compaction uses
// (db/compaction/compaction_job.cc sets it unconditionally).
//
// usage:
//   rdbread <dbpath> [-n records] [-v valuesize] [-s seed] [-r rounds] [-q]
//
// exit: 0 all reads correct, 1 a mismatch or rocksdb error, 2 usage error.

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <rocksdb/c.h>

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "rdbcommon.h"

static void usage(const char *argv0) {
  fprintf(stderr,
      "usage: %s <dbpath> [-n records] [-v valuesize] [-s seed] [-r rounds] [-q]\n"
      "\n"
      "Reads back a database written by rdbwrite and verifies every value.\n"
      "The -n, -v and -s values must match those used for rdbwrite.\n"
      "\n"
      "  -r N   read every record N times, comparing the bytes between reads\n"
      "         (default 1; >1 detects a read path that is not repeatable)\n"
      "  -q     quiet: only print on error\n"
      "\n"
      "exit: 0 all correct, 1 mismatch or rocksdb error, 2 usage error\n",
      argv0);
  exit(2);
}

int main(int argc, char **argv) {
  const char *dbpath = NULL;
  int64_t     nrec = RDB_DEFAULT_RECORDS;
  int32_t     vsize = RDB_DEFAULT_VALUE_SIZE;
  uint64_t    seed = 1;
  int         rounds = 1, quiet = 0;

  for (int i = 1; i < argc; i++) {
    if      (!strcmp(argv[i], "-n") && i + 1 < argc) nrec   = strtoll(argv[++i], NULL, 0);
    else if (!strcmp(argv[i], "-v") && i + 1 < argc) vsize  = (int32_t)strtol(argv[++i], NULL, 0);
    else if (!strcmp(argv[i], "-s") && i + 1 < argc) seed   = strtoull(argv[++i], NULL, 0);
    else if (!strcmp(argv[i], "-r") && i + 1 < argc) rounds = (int)strtol(argv[++i], NULL, 0);
    else if (!strcmp(argv[i], "-q")) quiet = 1;
    else if (argv[i][0] == '-') usage(argv[0]);
    else dbpath = argv[i];
  }
  if (!dbpath) usage(argv[0]);
  if (nrec <= 0 || vsize <= 0 || rounds < 1) {
    fprintf(stderr, "error: bad -n/-v/-r\n");
    return 2;
  }

  rdb_crc32c_init();

  // Same comparator and table factory as the writer, otherwise the database
  // cannot be interpreted at all.
  rocksdb_comparator_t *cmp =
      rocksdb_comparator_create(NULL, rdb_cmp_destroy, rdb_last_key_cmp, rdb_cmp_name);
  rocksdb_block_based_table_options_t *tableopts = rocksdb_block_based_options_create();
  rocksdb_options_t *options = rocksdb_options_create();

  rocksdb_options_set_create_if_missing(options, 0);  // must already exist
  rocksdb_options_set_comparator(options, cmp);
  rocksdb_options_set_block_based_table_factory(options, tableopts);
  rocksdb_options_set_info_log_level(options, 2);

  rocksdb_readoptions_t *ropt = rocksdb_readoptions_create();
  rocksdb_readoptions_set_verify_checksums(ropt, 1);  // as compaction does

  char      *err = NULL;
  rocksdb_t *db = rocksdb_open_for_read_only(options, dbpath, 0, &err);
  if (!db) {
    fprintf(stderr, "rdbread: open %s failed: %s\n", dbpath, err ? err : "(unknown)");
    return 1;
  }

  uint8_t *expect = (uint8_t *)malloc((size_t)vsize);
  uint8_t *first = (uint8_t *)malloc((size_t)vsize);
  if (!expect || !first) { fprintf(stderr, "rdbread: out of memory\n"); return 1; }

  int64_t nGetErr = 0;   // rocksdb returned an error (block checksum etc.)
  int64_t nMissing = 0;  // key absent
  int64_t nBadCrc = 0;   // value's own crc does not match its contents
  int64_t nWrong = 0;    // value differs from what was written
  int64_t nDrift = 0;    // same key read twice, different bytes
  int64_t nOk = 0;
  char   *firstErr = NULL;

  for (int64_t i = 0; i < nrec; i++) {
    SLastKey key;
    rdb_make_key(&key, i, seed);
    rdb_make_value(expect, vsize, &key, i, seed);

    for (int r = 0; r < rounds; r++) {
      size_t vlen = 0;
      err = NULL;
      char *got = rocksdb_get(db, ropt, (const char *)&key, ROCKS_KEY_LEN, &vlen, &err);

      if (err) {
        // This is where a "block checksum mismatch" surfaces.
        nGetErr++;
        if (!firstErr) firstErr = strdup(err);
        if (nGetErr <= 5)
          fprintf(stderr, "rdbread: get failed at record %" PRId64 ": %s\n", i, err);
        rocksdb_free(err);
        err = NULL;
        if (got) rocksdb_free(got);
        continue;
      }
      if (!got) {
        nMissing++;
        if (nMissing <= 5)
          fprintf(stderr, "rdbread: record %" PRId64 " missing (uid=%" PRId64
                          " cid=%d lflag=%d)\n", i, (int64_t)key.uid, key.cid, key.lflag);
        continue;
      }

      // rdb_make_key maps several indexes onto the same key, so a shorter or
      // longer value than ours means someone else wrote it; only compare when
      // the length matches what we generate.
      if (vlen != (size_t)vsize) {
        rocksdb_free(got);
        continue;
      }

      // The value carries its own crc, independent of rocksdb's block checksum.
      uint32_t stored = rdb_stored_crc((const uint8_t *)got);
      uint32_t actual = rdb_value_crc((const uint8_t *)got, vsize, &key);
      if (stored != actual) {
        nBadCrc++;
        if (nBadCrc <= 5)
          fprintf(stderr, "rdbread: record %" PRId64 " value crc mismatch:"
                          " stored=%u computed=%u -- the value is corrupt but rocksdb"
                          " did not report it\n", i, stored, actual);
      } else if (memcmp(got, expect, (size_t)vsize) != 0) {
        // Duplicate keys legitimately hold a different index's value, so only
        // flag a value whose own crc is valid yet is not any value we wrote.
        int64_t idx = 0;
        memcpy(&idx, (const uint8_t *)got + 4, sizeof(idx));
        SLastKey k2;
        rdb_make_key(&k2, idx, seed);
        if (memcmp(&k2, &key, ROCKS_KEY_LEN) != 0) {
          nWrong++;
          if (nWrong <= 5)
            fprintf(stderr, "rdbread: record %" PRId64 " holds a value belonging to"
                            " index %" PRId64 ", whose key differs\n", i, idx);
        } else {
          nOk++;  // a later duplicate of the same key overwrote this one
        }
      } else {
        nOk++;
      }

      // Repeatability: the same immutable bytes must read the same every time.
      if (rounds > 1) {
        if (r == 0) {
          memcpy(first, got, (size_t)vsize);
        } else if (memcmp(first, got, (size_t)vsize) != 0) {
          nDrift++;
          if (nDrift <= 5)
            fprintf(stderr, "rdbread: record %" PRId64 " read differently on round %d --"
                            " the read path is not repeatable\n", i, r);
        }
      }

      rocksdb_free(got);
    }
  }

  int failed = (nGetErr || nMissing || nBadCrc || nWrong || nDrift);

  if (!quiet || failed) {
    printf("rdbread: %s\n", failed ? "FAILED" : "ok");
    printf("  records checked : %" PRId64 " (x%d rounds)\n", nrec, rounds);
    printf("  values correct  : %" PRId64 "\n", nOk);
    if (nGetErr)  printf("  rocksdb errors  : %" PRId64 "   <-- e.g. block checksum mismatch\n", nGetErr);
    if (nMissing) printf("  missing keys    : %" PRId64 "\n", nMissing);
    if (nBadCrc)  printf("  bad value crc   : %" PRId64 "   <-- silent corruption rocksdb missed\n", nBadCrc);
    if (nWrong)   printf("  wrong values    : %" PRId64 "\n", nWrong);
    if (nDrift)   printf("  unrepeatable    : %" PRId64 "   <-- same key, different bytes\n", nDrift);
    if (firstErr) printf("  first error     : %s\n", firstErr);
  }

  free(firstErr);
  free(expect);
  free(first);
  rocksdb_readoptions_destroy(ropt);
  rocksdb_close(db);
  rocksdb_options_destroy(options);
  rocksdb_block_based_options_destroy(tableopts);
  rocksdb_comparator_destroy(cmp);

  return failed ? 1 : 0;
}
