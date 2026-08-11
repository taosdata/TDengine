// rdbwrite -- write a rocksdb database the way tsdbCache.c does.
//
// Program 1 of the write/read pair. Together with rdbread and run.sh this
// reproduces, in isolation, the sequence that failed in production:
//
//     write last values in batches -> flush -> read them back
//
// The rocksdb setup mirrors tsdbOpenRocksCache() in
// source/dnode/vnode/src/tsdb/tsdbCache.c:
//
//   - the same SLastKey record and myCmp comparator, so key ordering and
//     therefore block layout match what taosd produces
//   - create_if_missing = 1
//   - WAL disabled on writes (rocksdb_writeoptions_disable_WAL)
//   - block-based table factory with default options, so checksum type is
//     kXXH3 as in production
//   - writes accumulated in a write batch and flushed every ROCKS_BATCH_SIZE
//     records, as rocksMayWrite() does
//
// Every value carries its own xxhash-independent check field (a crc32c of the
// payload plus the key), so rdbread can tell a rocksdb-level failure from a
// silently wrong value that still passed rocksdb's block checksum.
//
// usage:
//   rdbwrite <dbpath> [-n records] [-v valuesize] [-s seed] [-q]
//
// exit: 0 success, 1 a rocksdb call failed, 2 usage error.

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
      "usage: %s <dbpath> [-n records] [-v valuesize] [-s seed] [-q]\n"
      "\n"
      "Writes a rocksdb database using the same key layout, comparator and\n"
      "write options as the tsdb last-value cache.\n"
      "\n"
      "  -n N   number of records (default %d)\n"
      "  -v N   value payload size in bytes (default %d)\n"
      "  -s N   seed, so a later run can regenerate identical data (default 1)\n"
      "  -q     quiet: only print on error\n"
      "\n"
      "exit: 0 success, 1 rocksdb error, 2 usage error\n",
      argv0, RDB_DEFAULT_RECORDS, RDB_DEFAULT_VALUE_SIZE);
  exit(2);
}

int main(int argc, char **argv) {
  const char *dbpath = NULL;
  int64_t     nrec = RDB_DEFAULT_RECORDS;
  int32_t     vsize = RDB_DEFAULT_VALUE_SIZE;
  uint64_t    seed = 1;
  int         quiet = 0;

  for (int i = 1; i < argc; i++) {
    if      (!strcmp(argv[i], "-n") && i + 1 < argc) nrec  = strtoll(argv[++i], NULL, 0);
    else if (!strcmp(argv[i], "-v") && i + 1 < argc) vsize = (int32_t)strtol(argv[++i], NULL, 0);
    else if (!strcmp(argv[i], "-s") && i + 1 < argc) seed  = strtoull(argv[++i], NULL, 0);
    else if (!strcmp(argv[i], "-q")) quiet = 1;
    else if (argv[i][0] == '-') usage(argv[0]);
    else dbpath = argv[i];
  }
  if (!dbpath) usage(argv[0]);
  if (nrec <= 0 || vsize <= 0 || vsize > RDB_MAX_VALUE_SIZE) {
    fprintf(stderr, "error: bad -n or -v (value size limit %d)\n", RDB_MAX_VALUE_SIZE);
    return 2;
  }

  rdb_crc32c_init();

  // --- options, matching tsdbOpenRocksCache() ---
  rocksdb_comparator_t *cmp =
      rocksdb_comparator_create(NULL, rdb_cmp_destroy, rdb_last_key_cmp, rdb_cmp_name);
  rocksdb_block_based_table_options_t *tableopts = rocksdb_block_based_options_create();
  rocksdb_options_t *options = rocksdb_options_create();

  rocksdb_options_set_create_if_missing(options, 1);
  rocksdb_options_set_comparator(options, cmp);
  rocksdb_options_set_block_based_table_factory(options, tableopts);
  rocksdb_options_set_info_log_level(options, 2);  // WARN_LEVEL, as in tsdbCache.c

  rocksdb_writeoptions_t *wopt = rocksdb_writeoptions_create();
  rocksdb_writeoptions_disable_WAL(wopt, 1);       // as in tsdbCache.c

  char      *err = NULL;
  rocksdb_t *db = rocksdb_open(options, dbpath, &err);
  if (!db) {
    fprintf(stderr, "rdbwrite: open %s failed: %s\n", dbpath, err ? err : "(unknown)");
    return 1;
  }

  rocksdb_writebatch_t *wb = rocksdb_writebatch_create();
  uint8_t              *val = (uint8_t *)malloc((size_t)vsize);
  if (!val) { fprintf(stderr, "rdbwrite: out of memory\n"); return 1; }

  int64_t written = 0, batches = 0;
  int     failed = 0;

  for (int64_t i = 0; i < nrec && !failed; i++) {
    SLastKey key;
    rdb_make_key(&key, i, seed);
    rdb_make_value(val, vsize, &key, i, seed);

    rocksdb_writebatch_put(wb, (const char *)&key, ROCKS_KEY_LEN,
                           (const char *)val, (size_t)vsize);

    // Flush the batch on the same boundary rocksMayWrite() uses.
    if (rocksdb_writebatch_count(wb) >= ROCKS_BATCH_SIZE) {
      rocksdb_write(db, wopt, wb, &err);
      if (err) {
        fprintf(stderr, "rdbwrite: write batch %" PRId64 " failed after %" PRId64
                        " records: %s\n", batches, written, err);
        rocksdb_free(err);
        err = NULL;
        failed = 1;
        break;
      }
      rocksdb_writebatch_clear(wb);
      batches++;
    }
    written++;
  }

  // Final partial batch, as rocksMayWrite(force=true) does.
  if (!failed && rocksdb_writebatch_count(wb) > 0) {
    rocksdb_write(db, wopt, wb, &err);
    if (err) {
      fprintf(stderr, "rdbwrite: final write failed: %s\n", err);
      rocksdb_free(err);
      err = NULL;
      failed = 1;
    } else {
      batches++;
    }
    rocksdb_writebatch_clear(wb);
  }

  // Flush memtables to SST, as tsdbCacheCommit() does. This is what turns the
  // written data into the on-disk blocks whose checksums are at issue.
  if (!failed) {
    rocksdb_flushoptions_t *fopt = rocksdb_flushoptions_create();
    rocksdb_flush(db, fopt, &err);
    if (err) {
      fprintf(stderr, "rdbwrite: flush failed: %s\n", err);
      rocksdb_free(err);
      err = NULL;
      failed = 1;
    }
    rocksdb_flushoptions_destroy(fopt);
  }

  if (!quiet && !failed)
    printf("rdbwrite: %" PRId64 " records, %" PRId64 " batches, value size %d, seed %" PRIu64
           " -> %s\n", written, batches, vsize, seed, dbpath);

  free(val);
  rocksdb_writebatch_destroy(wb);
  rocksdb_close(db);
  rocksdb_writeoptions_destroy(wopt);
  rocksdb_options_destroy(options);
  rocksdb_block_based_options_destroy(tableopts);
  rocksdb_comparator_destroy(cmp);

  return failed ? 1 : 0;
}
