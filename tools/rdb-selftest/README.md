# rdb-selftest

Can this machine write and read a rocksdb database reliably?

These tools exist to answer that question on a host where taosd reported:

```
Corruption: block checksum mismatch: stored = 2282375535, computed = 1111998889,
type = 4  in /data/taos/data/vnode/vnode15/tsdb/cache.rdb/000009.sst
offset 14273165 size 4032
```

They use only rocksdb's own interfaces, and link nothing from taosd, so they can
be built and run on the affected machine on its own.

| Tool | Purpose |
|---|---|
| `rdbsst` | Verify one SST with rocksdb's API, printing `stored` and `computed`. |
| `rdbwrite` | Write a database the way `tsdbCache.c` does. |
| `rdbread` | Read it back and verify every value. |
| `run.sh` | Loop write/read to catch an intermittent fault. |

设计原理见 [PRINCIPLE.md](PRINCIPLE.md)。

## Dependencies

All three link rocksdb **statically**, so at runtime they need only the standard
system libraries:

```
$ ldd rdbsst
    linux-vdso.so.1
    libstdc++.so.6    (rocksdb is C++)
    libm.so.6
    libgcc_s.so.1
    libc.so.6
```

No librocksdb at runtime, nothing from taosd. The binaries are large because all
of rocksdb is inside them — ~150 MB unstripped, 12 MB after `strip` — but they
are self-contained. Copy to any host of the same architecture and they run.

Link-time extras depend on how the rocksdb archive was built: `-lz -lsnappy
-lbz2` above, plus `-llz4 -lzstd` if that rocksdb was built with them. If a link
fails on a missing `-l`, either install that dev package or drop the flag if the
archive does not actually need it.

**Careful with binaries from `debug/build/bin/`.** If the tree was configured
with `BUILD_SANITIZER=ON` they also link `libasan`/`libubsan`, will not start on
a host without those libraries, and run several times slower. Since the point of
`run.sh` is to perform as many write/read cycles as possible, prefer a plain
build for field use:

```bash
ldd debug/build/bin/rdbread | grep -q asan && echo "ASAN build - rebuild plain"
```

## Building

With the project build:

```bash
cd debug
make rdbwrite rdbread rdbsst -j8      # -> debug/build/bin/
```

Standalone, given any `librocksdb.a` and its headers:

```bash
RD=<rocksdb>                          # dir containing include/rocksdb/c.h
L="<path>/librocksdb.a -lpthread -lz -lsnappy -lbz2 -ldl"

gcc -O2 -I"$RD/include" -I. -o rdbwrite rdbwrite.c $L -lstdc++ -lm
gcc -O2 -I"$RD/include" -I. -o rdbread  rdbread.c  $L -lstdc++ -lm
g++ -std=c++17 -O2 -I"$RD/include" -o rdbsst rdbsst.cc $L
```

`-std=c++17` is required for `rdbsst`: rocksdb's own headers use
`std::string_view` (`slice.h`) and `std::make_from_tuple` (`wide_columns.h`).
Without it the compiler stops on rocksdb's headers, not on our code:

```
rocksdb/slice.h:46: error: 'string_view' in namespace 'std' does not name a type
  note: 'std::string_view' is only available from C++17 onwards
```

`rdbwrite` and `rdbread` use only rocksdb's C API and need no `-std` flag.

If the build also fails with:

```
cc1plus: error: unrecognized command line option '-Wno-stringop-overread' [-Werror]
```

that flag only exists from GCC 11 and is coming from the project's own build
flags, not from these tools. The CMake build drops it automatically on older
compilers; for a standalone build just leave it out, as the commands above do.

If the host has taosd installed, the rocksdb static library and headers that
came with the build can be reused; otherwise any rocksdb 6.27+ will do, since
only the stable C API and `SstFileReader` are used.

## 1. Checking a single SST

Give it the offset and size from the error message:

```bash
./rdbsst /data/taos/data/vnode/vnode15/tsdb/cache.rdb/000009.sst \
    -o 14273165 -s 4032
```

```
file:     .../000009.sst
size:     23917195 bytes
checksum: kXXH3 (4)

1. SstFileReader::Open()      : OK
2. SstFileReader::VerifyChecksum(): OK -- every block verifies
3. iterator read              : skipped (pass -i to enable)

4. block named on the command line, recomputed with RocksDB's
   ComputeBuiltinChecksum():
  block at offset 14273165 size 4032 (compression byte 0x00)
  stored   = 2282375535
  computed = 2282375535
  -> MATCH: this block is intact on disk

VERDICT: the file reads correctly on this machine.
```

Every step is rocksdb's own code: `SstFileReader::Open()`,
`VerifyChecksum()`, `NewIterator()`, and `ComputeBuiltinChecksum()`.

`VerifyChecksum()` reports numbers only when it fails, so step 4 recomputes the
named block explicitly and prints both values either way — otherwise a passing
run gives nothing to compare against the logged error. Values are unmasked for
`kCRC32c`, matching how rocksdb prints them.

Exit status: `0` verified, `1` verification failed, `2` usage or open error.
That makes it safe to script over a whole directory:

```bash
for f in /data/taos/data/vnode/vnode*/tsdb/cache.rdb/*.sst; do
    ./rdbsst "$f" >/dev/null 2>&1 || echo "FAILED: $f"
done
```

### About the iterator step

`-i` reads keys through an iterator, but the count is misleading for
`cache.rdb`: `SstFileReader` has no API for supplying a comparator, so it
iterates bytewise, while `cache.rdb` is ordered by `myCmp` (uid, then cid, then
lflag — see `tsdbCache.c`). Under the wrong comparator iteration stops early, so
a low key count is expected and is not a sign of damage. It is off by default for
that reason.

`VerifyChecksum()` is unaffected: it walks every block by file offset and never
compares keys, so step 2 covers the whole file regardless.

## 2. Looping write/read to catch an intermittent fault

A single clean pass only proves the machine behaved for that one read. An
intermittent fault is quiet most of the time, so one clean result is expected,
not exculpatory. `run.sh` repeats the production sequence — write last values in
batches, flush to SST, read them back — until something breaks or you stop it.

```bash
# 20 iterations, defaults
./run.sh

# hammer it: unbounded, 4 concurrent, larger DBs, 3 read rounds, cold reads
./run.sh -i 0 -j 4 -n 500000 -r 3 --drop-cache

# isolate the read path: write once, then only re-read
./run.sh --keep-db -i 0 -r 5
```

Options:

```
-d DIR        working directory (default ./rdbselftest-work)
-i N          iterations, 0 = until Ctrl-C (default 20)
-n N          records per database (default 200000)
-v N          value size in bytes (default 96)
-r N          read rounds per iteration; >1 also checks read repeatability
-t SECONDS    stop after this long
-j N          run N iterations concurrently, adding memory and I/O pressure
--keep-db     write once, then only re-read each iteration
--drop-cache  drop the page cache before each read (needs root)
```

Exit status: `0` no failure observed, `1` at least one iteration failed.

### What the write and read programs check

`rdbwrite` mirrors `tsdbOpenRocksCache()`: the same `SLastKey` record and
`myCmp` comparator (so key ordering, and therefore block layout, match what
taosd produces), WAL disabled on writes, the block-based table factory with
default options (hence `kXXH3` checksums), records accumulated in a write batch
and flushed every `ROCKS_BATCH_SIZE` = 4096 records as `rocksMayWrite()` does,
then a final `rocksdb_flush()` as `tsdbCacheCommit()` does.

`rdbread` checks three things, in increasing subtlety:

1. does rocksdb report a block checksum error — the production failure
2. does every value come back byte-for-byte as written. Each value carries its
   own crc32c over the payload plus the key, so a value that is wrong but still
   passed rocksdb's block checksum is caught too. Rocksdb cannot detect that
   case itself
3. with `-r N`, does the same key read the same bytes every time

Records are generated deterministically from (index, seed), so the reader
recomputes what every value should be without being told.

## Interpreting a failure

This is the part that matters. When `run.sh` reports a failure it keeps the
database and re-checks it with `rdbsst`:

| `rdbsst` on the kept file | Meaning |
|---|---|
| also MISMATCH | The bytes on disk really are wrong. Persistent corruption: look at the media, controller, RAID write cache and its battery, and the filesystem. |
| OK / MATCH | The bytes on disk are correct, so the failing read was transient. SST files are immutable once written, so nothing changed the file between the two reads. The fault is between the platter and the process: RAM, CPU cache, HBA, cable, or controller cache. |

Either result narrows the search considerably, but neither pins down a single
component on its own. Check the hardware counters as well:

```bash
# storage: bytes fine on the platter but wrong on arrival
smartctl -a /dev/XXX | grep -iE "UDMA_CRC_Error|Media_and_Data_Integrity|Current_Pending|Reallocated"

# memory
ras-mc-ctl --error-count
edac-util -v
dmidecode -t memory | grep -i "error correction"   # non-ECC => counters stay empty
dmesg -T | grep -iE "edac|mce|hardware error|blk_update_request|i/o error"
```

`UDMA_CRC_Error_Count` is worth singling out: it counts interface transmission
errors, which is precisely the "disk is fine, transfer was not" case.

If `dmidecode` reports non-ECC memory, `ras-mc-ctl` and `edac-util` will return
nothing regardless of whether memory is faulty — an empty result there is not
evidence of health, and memtest86+ is the alternative.

## Why a clean ASAN run does not settle this

ASAN instruments the program's own memory accesses and finds software defects:
overflows, use-after-free, leaks. It cannot observe a byte that was correct in
DRAM and arrived wrong in a register, or that left the platter correct and
reached the page cache wrong. Such a buffer has entirely legal bounds and
lifetime — no memory-safety rule was broken — so ASAN correctly reports nothing.

A clean ASAN run therefore makes a memory-safety bug in taosd unlikely. It says
nothing about the read path, which is what these tools probe.

## Limitations

A clean run is evidence only in proportion to how long it ran and under how much
load. The original incident coincided with heavy WAL replay across 42 vnodes, so
reproducing it may require comparable memory pressure — consider running
`run.sh -j 4` alongside the real workload rather than on an idle machine.

`run.sh` uses a fresh database per iteration by default so that a failure is
attributable to that iteration. `--keep-db` writes once and only re-reads, which
separates a read-path fault from a write-path one.
