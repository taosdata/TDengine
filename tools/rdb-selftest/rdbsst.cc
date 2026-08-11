// rdbsst -- verify an SST with RocksDB's own API, and print stored/computed.
//
// Deliverable 1: takes an SST path, uses only RocksDB interfaces to check
// whether the file can be read here, and prints the stored and computed
// checksums so they can be compared against the numbers in a
// "block checksum mismatch" message.
//
// Everything here is RocksDB's own code:
//   - SstFileReader::Open()             can the file be opened at all
//   - SstFileReader::VerifyChecksum()   full verification of every block
//   - SstFileReader::NewIterator()      read every key, as a query would
//   - ComputeBuiltinChecksum()          recompute one named block
//
// VerifyChecksum() only reports numbers when it fails, so for a specific block
// (-o/-s) we also call ComputeBuiltinChecksum() directly and print both values
// either way. Without that, a passing run gives nothing to compare against.
//
// The file is only ever opened read-only.
//
// usage:
//   rdbsst <file.sst> [-o <offset> -s <size>] [-i]
//
// exit: 0 readable and all checksums verify, 1 verification failed,
//       2 usage or open error.

#include <rocksdb/options.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/table.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

// Declared by hand rather than including table/format.h, which needs RocksDB's
// internal build defines. The symbol is exported from librocksdb.
namespace rocksdb {
extern uint32_t ComputeBuiltinChecksum(ChecksumType type, const char* data, size_t n);
}

namespace {

constexpr int kFooterLen = 53;       // Footer::kNewVersionsEncodedLength
constexpr int kBlockTrailerSize = 5; // 1-byte compression type + 4-byte checksum

// crc32c checksums are stored masked; RocksDB unmasks them when reporting, so
// do the same here to keep the numbers comparable with its messages.
uint32_t Crc32cUnmask(uint32_t masked) {
  uint32_t rot = masked - 0xa282ead8ul;
  return (rot << 15) | (rot >> 17);
}

const char* ChecksumName(int t) {
  switch (t) {
    case rocksdb::kNoChecksum: return "kNoChecksum";
    case rocksdb::kCRC32c:     return "kCRC32c";
    case rocksdb::kxxHash:     return "kxxHash";
    case rocksdb::kxxHash64:   return "kxxHash64";
    case rocksdb::kXXH3:       return "kXXH3";
    default:                   return "unknown";
  }
}

// The checksum type is the first byte of the 53-byte footer for all
// format_versions >= 1.
int ReadChecksumType(const char* path, long long* file_size) {
  FILE* f = fopen(path, "rb");
  if (!f) return -1;
  if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return -1; }
  long long sz = ftell(f);
  if (file_size) *file_size = sz;
  if (sz < kFooterLen) { fclose(f); return -1; }
  if (fseek(f, sz - kFooterLen, SEEK_SET) != 0) { fclose(f); return -1; }
  int c = fgetc(f);
  fclose(f);
  if (c < 0 || c > 4) return -1;
  return c;
}

// Recompute one block with RocksDB's own function and print both values.
// Returns 0 match, 1 mismatch, 2 could not read.
int CheckOneBlock(const char* path, long long off, long long sz, int ctype) {
  FILE* f = fopen(path, "rb");
  if (!f) { printf("  cannot open file: %s\n", strerror(errno)); return 2; }

  std::vector<char> buf(static_cast<size_t>(sz) + kBlockTrailerSize);
  if (fseek(f, static_cast<long>(off), SEEK_SET) != 0 ||
      fread(buf.data(), 1, buf.size(), f) != buf.size()) {
    printf("  cannot read %lld bytes at offset %lld (file too short?)\n",
           static_cast<long long>(buf.size()), off);
    fclose(f);
    return 2;
  }
  fclose(f);

  const unsigned char* p = reinterpret_cast<const unsigned char*>(buf.data());
  uint32_t stored = static_cast<uint32_t>(p[sz + 1]) |
                    (static_cast<uint32_t>(p[sz + 2]) << 8) |
                    (static_cast<uint32_t>(p[sz + 3]) << 16) |
                    (static_cast<uint32_t>(p[sz + 4]) << 24);

  // Checksummed range is contents plus the one-byte compression type.
  uint32_t computed = rocksdb::ComputeBuiltinChecksum(
      static_cast<rocksdb::ChecksumType>(ctype), buf.data(),
      static_cast<size_t>(sz) + 1);

  uint32_t s = stored, c = computed;
  if (ctype == rocksdb::kCRC32c) { s = Crc32cUnmask(s); c = Crc32cUnmask(c); }

  printf("  block at offset %lld size %lld (compression byte 0x%02x)\n",
         off, sz, p[sz]);
  printf("  stored   = %u\n", s);
  printf("  computed = %u\n", c);
  printf("  -> %s\n", s == c ? "MATCH: this block is intact on disk"
                             : "MISMATCH: this block is damaged on disk");

  if (s != c) {
    int bits = __builtin_popcount(stored ^ computed);
    if (bits <= 2)
      printf("  note: only %d bit(s) differ -- the 4-byte checksum field itself\n"
             "        may be the damaged part, rather than the block contents\n", bits);
    return 1;
  }
  return 0;
}

void Usage(const char* argv0) {
  fprintf(stderr,
      "usage: %s <file.sst> [-o <offset> -s <size>] [-q]\n"
      "\n"
      "Verifies an SST using only RocksDB's own interfaces, and prints the\n"
      "stored and computed checksums so they can be compared against the\n"
      "numbers in a \"block checksum mismatch\" message.\n"
      "\n"
      "  -o <offset>  also check this one block, from the error message\n"
      "  -s <size>    block size to go with -o\n"
      "  -i           also read keys through an iterator. Off by default: for\n"
      "               cache.rdb the count is misleadingly low, because\n"
      "               SstFileReader cannot use the myCmp comparator\n"
      "\n"
      "example:\n"
      "  %s /data/taos/data/vnode/vnode15/tsdb/cache.rdb/000009.sst \\\n"
      "      -o 14273165 -s 4032\n"
      "\n"
      "exit: 0 readable and verified, 1 verification failed, 2 usage/open error\n",
      argv0, argv0);
  exit(2);
}

}  // namespace

int main(int argc, char** argv) {
  const char* path = nullptr;
  long long   off = 0, size = 0;
  bool        have_block = false, scan = false;

  for (int i = 1; i < argc; i++) {
    if (!strcmp(argv[i], "-o") && i + 1 < argc) { off = atoll(argv[++i]); have_block = true; }
    else if (!strcmp(argv[i], "-s") && i + 1 < argc) { size = atoll(argv[++i]); }
    else if (!strcmp(argv[i], "-i")) { scan = true; }
    else if (argv[i][0] == '-') Usage(argv[0]);
    else path = argv[i];
  }
  if (!path) Usage(argv[0]);
  if (have_block && size <= 0) {
    fprintf(stderr, "error: -o requires -s with a positive block size\n");
    return 2;
  }

  long long fsz = 0;
  int ctype = ReadChecksumType(path, &fsz);

  printf("file:     %s\n", path);
  printf("size:     %lld bytes\n", fsz);
  if (ctype >= 0) printf("checksum: %s (%d)\n", ChecksumName(ctype), ctype);
  else            printf("checksum: could not read footer\n");
  printf("\n");

  int rc = 0;

  // --- 1. can RocksDB open it here ---
  rocksdb::Options options;
  rocksdb::SstFileReader reader(options);
  rocksdb::Status s = reader.Open(path);
  printf("1. SstFileReader::Open()      : %s\n",
         s.ok() ? "OK" : s.ToString().c_str());
  if (!s.ok()) {
    printf("\nThe file cannot be opened, so it cannot be read on this machine.\n");
    return 2;
  }

  // --- 2. RocksDB's own full verification ---
  s = reader.VerifyChecksum();
  if (s.ok()) {
    printf("2. SstFileReader::VerifyChecksum(): OK -- every block verifies\n");
  } else {
    printf("2. SstFileReader::VerifyChecksum(): %s\n", s.ToString().c_str());
    rc = 1;
  }

  // --- 3. read keys through an iterator ---
  //
  // Caveat, and the reason this is not the primary signal: SstFileReader has no
  // API for supplying a custom comparator, so it iterates with the default
  // bytewise one. cache.rdb is written with myCmp (see tsdbCache.c), which
  // orders by uid, then cid, then lflag -- not bytewise. Under the wrong
  // comparator the iterator stops as soon as it sees keys it considers
  // out of order, so the key count here is a lower bound and will be far below
  // the real entry count for a cache.rdb file. That is expected and is not a
  // sign of damage.
  //
  // Step 2 is unaffected: VerifyChecksum() walks every block by file offset and
  // never compares keys, so it covers the whole file regardless of comparator.
  if (scan) {
    rocksdb::ReadOptions ro;
    ro.verify_checksums = true;
    rocksdb::Iterator* it = reader.NewIterator(ro);
    long long n = 0, bytes = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      n++;
      bytes += it->key().size() + it->value().size();
    }
    rocksdb::Status is = it->status();
    if (is.ok()) {
      printf("3. iterator read              : OK -- %lld keys read, %lld bytes\n", n, bytes);
      printf("   (a low count is expected for cache.rdb: SstFileReader cannot use\n"
             "    the myCmp comparator, so iteration stops early. Step 2 still\n"
             "    covered every block.)\n");
    } else {
      // An error here is meaningful even with the wrong comparator: a checksum
      // failure is reported through the same status.
      printf("3. iterator read              : FAILED after %lld keys: %s\n",
             n, is.ToString().c_str());
      rc = 1;
    }
    delete it;
  } else {
    printf("3. iterator read              : skipped (pass -i to enable)\n");
  }

  // --- 4. the specific block named in the error message ---
  if (have_block) {
    printf("\n4. block named on the command line, recomputed with RocksDB's\n"
           "   ComputeBuiltinChecksum():\n");
    if (ctype < 0) {
      printf("  cannot determine checksum type from the footer\n");
      rc = 2;
    } else {
      int br = CheckOneBlock(path, off, size, ctype);
      if (br == 1) rc = 1;
      else if (br == 2 && rc == 0) rc = 2;
    }
  }

  printf("\n");
  if (rc == 0) {
    printf("VERDICT: the file reads correctly on this machine.\n");
    if (have_block)
      printf("The named block is intact on disk. If RocksDB reported it as corrupt\n"
             "elsewhere, the bytes were altered between leaving the disk and being\n"
             "checksummed in memory on that machine -- SST files are immutable once\n"
             "written, so the file itself did not change.\n");
  } else if (rc == 1) {
    printf("VERDICT: verification FAILED -- the bytes on disk really are wrong.\n");
  } else {
    printf("VERDICT: could not complete verification (see messages above).\n");
  }
  return rc;
}
