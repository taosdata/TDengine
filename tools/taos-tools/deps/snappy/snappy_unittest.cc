// Copyright 2005 and onwards Google Inc.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include <math.h>
#include <stdlib.h>

#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "snappy.h"
#include "snappy-internal.h"
#include "snappy-test.h"
#include "snappy-sinksource.h"

DEFINE_int32(start_len, -1,
             "Starting prefix size for testing (-1: just full file contents)");
DEFINE_int32(end_len, -1,
             "Starting prefix size for testing (-1: just full file contents)");
DEFINE_int32(bytes, 10485760,
             "How many bytes to compress/uncompress per file for timing");

DEFINE_bool(zlib, false,
            "Run zlib compression (http://www.zlib.net)");
DEFINE_bool(lzo, false,
            "Run LZO compression (http://www.oberhumer.com/opensource/lzo/)");
DEFINE_bool(snappy, true, "Run snappy compression");

DEFINE_bool(write_compressed, false,
            "Write compressed versions of each file to <file>.comp");
DEFINE_bool(write_uncompressed, false,
            "Write uncompressed versions of each file to <file>.uncomp");

DEFINE_bool(snappy_dump_decompression_table, false,
            "If true, we print the decompression table during tests.");

namespace snappy {

#if defined(HAVE_FUNC_MMAP) && defined(HAVE_FUNC_SYSCONF)

// To test against code that reads beyond its input, this class copies a
// string to a newly allocated group of pages, the last of which
// is made unreadable via mprotect. Note that we need to allocate the
// memory with mmap(), as POSIX allows mprotect() only on memory allocated
// with mmap(), and some malloc/posix_memalign implementations expect to
// be able to read previously allocated memory while doing heap allocations.
class DataEndingAtUnreadablePage {
 public:
  explicit DataEndingAtUnreadablePage(const std::string& s) {
    const size_t page_size = sysconf(_SC_PAGESIZE);
    const size_t size = s.size();
    // Round up space for string to a multiple of page_size.
    size_t space_for_string = (size + page_size - 1) & ~(page_size - 1);
    alloc_size_ = space_for_string + page_size;
    mem_ = mmap(NULL, alloc_size_,
                PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    CHECK_NE(MAP_FAILED, mem_);
    protected_page_ = reinterpret_cast<char*>(mem_) + space_for_string;
    char* dst = protected_page_ - size;
    memcpy(dst, s.data(), size);
    data_ = dst;
    size_ = size;
    // Make guard page unreadable.
    CHECK_EQ(0, mprotect(protected_page_, page_size, PROT_NONE));
  }

  ~DataEndingAtUnreadablePage() {
    const size_t page_size = sysconf(_SC_PAGESIZE);
    // Undo the mprotect.
    CHECK_EQ(0, mprotect(protected_page_, page_size, PROT_READ|PROT_WRITE));
    CHECK_EQ(0, munmap(mem_, alloc_size_));
  }

  const char* data() const { return data_; }
  size_t size() const { return size_; }

 private:
  size_t alloc_size_;
  void* mem_;
  char* protected_page_;
  const char* data_;
  size_t size_;
};

#else  // defined(HAVE_FUNC_MMAP) && defined(HAVE_FUNC_SYSCONF)

// Fallback for systems without mmap.
using DataEndingAtUnreadablePage = std::string;

#endif

enum CompressorType {
  ZLIB, LZO, SNAPPY
};

const char* names[] = {
  "ZLIB", "LZO", "SNAPPY"
};

static size_t MinimumRequiredOutputSpace(size_t input_size,
                                         CompressorType comp) {
  switch (comp) {
#ifdef ZLIB_VERSION
    case ZLIB:
      return ZLib::MinCompressbufSize(input_size);
#endif  // ZLIB_VERSION

#ifdef LZO_VERSION
    case LZO:
      return input_size + input_size/64 + 16 + 3;
#endif  // LZO_VERSION

    case SNAPPY:
      return snappy::MaxCompressedLength(input_size);

    default:
      LOG(FATAL) << "Unknown compression type number " << comp;
      return 0;
  }
}

// Returns true if we successfully compressed, false otherwise.
//
// If compressed_is_preallocated is set, do not resize the compressed buffer.
// This is typically what you want for a benchmark, in order to not spend
// time in the memory allocator. If you do set this flag, however,
// "compressed" must be preinitialized to at least MinCompressbufSize(comp)
// number of bytes, and may contain junk bytes at the end after return.
static bool Compress(const char* input, size_t input_size, CompressorType comp,
                     std::string* compressed, bool compressed_is_preallocated) {
  if (!compressed_is_preallocated) {
    compressed->resize(MinimumRequiredOutputSpace(input_size, comp));
  }

  switch (comp) {
#ifdef ZLIB_VERSION
    case ZLIB: {
      ZLib zlib;
      uLongf destlen = compressed->size();
      int ret = zlib.Compress(
          reinterpret_cast<Bytef*>(string_as_array(compressed)),
          &destlen,
          reinterpret_cast<const Bytef*>(input),
          input_size);
      CHECK_EQ(Z_OK, ret);
      if (!compressed_is_preallocated) {
        compressed->resize(destlen);
      }
      return true;
    }
#endif  // ZLIB_VERSION

#ifdef LZO_VERSION
    case LZO: {
      unsigned char* mem = new unsigned char[LZO1X_1_15_MEM_COMPRESS];
      lzo_uint destlen;
      int ret = lzo1x_1_15_compress(
          reinterpret_cast<const uint8*>(input),
          input_size,
          reinterpret_cast<uint8*>(string_as_array(compressed)),
          &destlen,
          mem);
      CHECK_EQ(LZO_E_OK, ret);
      delete[] mem;
      if (!compressed_is_preallocated) {
        compressed->resize(destlen);
      }
      break;
    }
#endif  // LZO_VERSION

    case SNAPPY: {
      size_t destlen;
      snappy::RawCompress(input, input_size,
                          string_as_array(compressed),
                          &destlen);
      CHECK_LE(destlen, snappy::MaxCompressedLength(input_size));
      if (!compressed_is_preallocated) {
        compressed->resize(destlen);
      }
      break;
    }

    default: {
      return false;     // the asked-for library wasn't compiled in
    }
  }
  return true;
}

static bool Uncompress(const std::string& compressed, CompressorType comp,
                       int size, std::string* output) {
  switch (comp) {
#ifdef ZLIB_VERSION
    case ZLIB: {
      output->resize(size);
      ZLib zlib;
      uLongf destlen = output->size();
      int ret = zlib.Uncompress(
          reinterpret_cast<Bytef*>(string_as_array(output)),
          &destlen,
          reinterpret_cast<const Bytef*>(compressed.data()),
          compressed.size());
      CHECK_EQ(Z_OK, ret);
      CHECK_EQ(static_cast<uLongf>(size), destlen);
      break;
    }
#endif  // ZLIB_VERSION

#ifdef LZO_VERSION
    case LZO: {
      output->resize(size);
      lzo_uint destlen;
      int ret = lzo1x_decompress(
          reinterpret_cast<const uint8*>(compressed.data()),
          compressed.size(),
          reinterpret_cast<uint8*>(string_as_array(output)),
          &destlen,
          NULL);
      CHECK_EQ(LZO_E_OK, ret);
      CHECK_EQ(static_cast<lzo_uint>(size), destlen);
      break;
    }
#endif  // LZO_VERSION

    case SNAPPY: {
      snappy::RawUncompress(compressed.data(), compressed.size(),
                            string_as_array(output));
      break;
    }

    default: {
      return false;     // the asked-for library wasn't compiled in
    }
  }
  return true;
}

static void Measure(const char* data,
                    size_t length,
                    CompressorType comp,
                    int repeats,
                    int block_size) {
  // Run tests a few time and pick median running times
  static const int kRuns = 5;
  double ctime[kRuns];
  double utime[kRuns];
  int compressed_size = 0;

  {
    // Chop the input into blocks
    int num_blocks = (length + block_size - 1) / block_size;
    std::vector<const char*> input(num_blocks);
    std::vector<size_t> input_length(num_blocks);
    std::vector<std::string> compressed(num_blocks);
    std::vector<std::string> output(num_blocks);
    for (int b = 0; b < num_blocks; b++) {
      int input_start = b * block_size;
      int input_limit = std::min<int>((b+1)*block_size, length);
      input[b] = data+input_start;
      input_length[b] = input_limit-input_start;

      // Pre-grow the output buffer so we don't measure string append time.
      compressed[b].resize(MinimumRequiredOutputSpace(block_size, comp));
    }

    // First, try one trial compression to make sure the code is compiled in
    if (!Compress(input[0], input_length[0], comp, &compressed[0], true)) {
      LOG(WARNING) << "Skipping " << names[comp] << ": "
                   << "library not compiled in";
      return;
    }

    for (int run = 0; run < kRuns; run++) {
      CycleTimer ctimer, utimer;

      for (int b = 0; b < num_blocks; b++) {
        // Pre-grow the output buffer so we don't measure string append time.
        compressed[b].resize(MinimumRequiredOutputSpace(block_size, comp));
      }

      ctimer.Start();
      for (int b = 0; b < num_blocks; b++)
        for (int i = 0; i < repeats; i++)
          Compress(input[b], input_length[b], comp, &compressed[b], true);
      ctimer.Stop();

      // Compress once more, with resizing, so we don't leave junk
      // at the end that will confuse the decompressor.
      for (int b = 0; b < num_blocks; b++) {
        Compress(input[b], input_length[b], comp, &compressed[b], false);
      }

      for (int b = 0; b < num_blocks; b++) {
        output[b].resize(input_length[b]);
      }

      utimer.Start();
      for (int i = 0; i < repeats; i++)
        for (int b = 0; b < num_blocks; b++)
          Uncompress(compressed[b], comp, input_length[b], &output[b]);
      utimer.Stop();

      ctime[run] = ctimer.Get();
      utime[run] = utimer.Get();
    }

    compressed_size = 0;
    for (size_t i = 0; i < compressed.size(); i++) {
      compressed_size += compressed[i].size();
    }
  }

  std::sort(ctime, ctime + kRuns);
  std::sort(utime, utime + kRuns);
  const int med = kRuns/2;

  float comp_rate = (length / ctime[med]) * repeats / 1048576.0;
  float uncomp_rate = (length / utime[med]) * repeats / 1048576.0;
  std::string x = names[comp];
  x += ":";
  std::string urate = (uncomp_rate >= 0) ? StrFormat("%.1f", uncomp_rate)
                                         : std::string("?");
  printf("%-7s [b %dM] bytes %6d -> %6d %4.1f%%  "
         "comp %5.1f MB/s  uncomp %5s MB/s\n",
         x.c_str(),
         block_size/(1<<20),
         static_cast<int>(length), static_cast<uint32>(compressed_size),
         (compressed_size * 100.0) / std::max<int>(1, length),
         comp_rate,
         urate.c_str());
}

static int VerifyString(const std::string& input) {
  std::string compressed;
  DataEndingAtUnreadablePage i(input);
  const size_t written = snappy::Compress(i.data(), i.size(), &compressed);
  CHECK_EQ(written, compressed.size());
  CHECK_LE(compressed.size(),
           snappy::MaxCompressedLength(input.size()));
  CHECK(snappy::IsValidCompressedBuffer(compressed.data(), compressed.size()));

  std::string uncompressed;
  DataEndingAtUnreadablePage c(compressed);
  CHECK(snappy::Uncompress(c.data(), c.size(), &uncompressed));
  CHECK_EQ(uncompressed, input);
  return uncompressed.size();
}

static void VerifyStringSink(const std::string& input) {
  std::string compressed;
  DataEndingAtUnreadablePage i(input);
  const size_t written = snappy::Compress(i.data(), i.size(), &compressed);
  CHECK_EQ(written, compressed.size());
  CHECK_LE(compressed.size(),
           snappy::MaxCompressedLength(input.size()));
  CHECK(snappy::IsValidCompressedBuffer(compressed.data(), compressed.size()));

  std::string uncompressed;
  uncompressed.resize(input.size());
  snappy::UncheckedByteArraySink sink(string_as_array(&uncompressed));
  DataEndingAtUnreadablePage c(compressed);
  snappy::ByteArraySource source(c.data(), c.size());
  CHECK(snappy::Uncompress(&source, &sink));
  CHECK_EQ(uncompressed, input);
}

static void VerifyIOVec(const std::string& input) {
  std::string compressed;
  DataEndingAtUnreadablePage i(input);
  const size_t written = snappy::Compress(i.data(), i.size(), &compressed);
  CHECK_EQ(written, compressed.size());
  CHECK_LE(compressed.size(),
           snappy::MaxCompressedLength(input.size()));
  CHECK(snappy::IsValidCompressedBuffer(compressed.data(), compressed.size()));

  // Try uncompressing into an iovec containing a random number of entries
  // ranging from 1 to 10.
  char* buf = new char[input.size()];
  std::minstd_rand0 rng(input.size());
  std::uniform_int_distribution<size_t> uniform_1_to_10(1, 10);
  size_t num = uniform_1_to_10(rng);
  if (input.size() < num) {
    num = input.size();
  }
  struct iovec* iov = new iovec[num];
  int used_so_far = 0;
  std::bernoulli_distribution one_in_five(1.0 / 5);
  for (size_t i = 0; i < num; ++i) {
    assert(used_so_far < input.size());
    iov[i].iov_base = buf + used_so_far;
    if (i == num - 1) {
      iov[i].iov_len = input.size() - used_so_far;
    } else {
      // Randomly choose to insert a 0 byte entry.
      if (one_in_five(rng)) {
        iov[i].iov_len = 0;
      } else {
        std::uniform_int_distribution<size_t> uniform_not_used_so_far(
            0, input.size() - used_so_far - 1);
        iov[i].iov_len = uniform_not_used_so_far(rng);
      }
    }
    used_so_far += iov[i].iov_len;
  }
  CHECK(snappy::RawUncompressToIOVec(
      compressed.data(), compressed.size(), iov, num));
  CHECK(!memcmp(buf, input.data(), input.size()));
  delete[] iov;
  delete[] buf;
}

// Test that data compressed by a compressor that does not
// obey block sizes is uncompressed properly.
static void VerifyNonBlockedCompression(const std::string& input) {
  if (input.length() > snappy::kBlockSize) {
    // We cannot test larger blocks than the maximum block size, obviously.
    return;
  }

  std::string prefix;
  Varint::Append32(&prefix, input.size());

  // Setup compression table
  snappy::internal::WorkingMemory wmem(input.size());
  int table_size;
  uint16* table = wmem.GetHashTable(input.size(), &table_size);

  // Compress entire input in one shot
  std::string compressed;
  compressed += prefix;
  compressed.resize(prefix.size()+snappy::MaxCompressedLength(input.size()));
  char* dest = string_as_array(&compressed) + prefix.size();
  char* end = snappy::internal::CompressFragment(input.data(), input.size(),
                                                dest, table, table_size);
  compressed.resize(end - compressed.data());

  // Uncompress into std::string
  std::string uncomp_str;
  CHECK(snappy::Uncompress(compressed.data(), compressed.size(), &uncomp_str));
  CHECK_EQ(uncomp_str, input);

  // Uncompress using source/sink
  std::string uncomp_str2;
  uncomp_str2.resize(input.size());
  snappy::UncheckedByteArraySink sink(string_as_array(&uncomp_str2));
  snappy::ByteArraySource source(compressed.data(), compressed.size());
  CHECK(snappy::Uncompress(&source, &sink));
  CHECK_EQ(uncomp_str2, input);

  // Uncompress into iovec
  {
    static const int kNumBlocks = 10;
    struct iovec vec[kNumBlocks];
    const int block_size = 1 + input.size() / kNumBlocks;
    std::string iovec_data(block_size * kNumBlocks, 'x');
    for (int i = 0; i < kNumBlocks; i++) {
      vec[i].iov_base = string_as_array(&iovec_data) + i * block_size;
      vec[i].iov_len = block_size;
    }
    CHECK(snappy::RawUncompressToIOVec(compressed.data(), compressed.size(),
                                       vec, kNumBlocks));
    CHECK_EQ(std::string(iovec_data.data(), input.size()), input);
  }
}

// Expand the input so that it is at least K times as big as block size
static std::string Expand(const std::string& input) {
  static const int K = 3;
  std::string data = input;
  while (data.size() < K * snappy::kBlockSize) {
    data += input;
  }
  return data;
}

static int Verify(const std::string& input) {
  VLOG(1) << "Verifying input of size " << input.size();

  // Compress using string based routines
  const int result = VerifyString(input);

  // Verify using sink based routines
  VerifyStringSink(input);

  VerifyNonBlockedCompression(input);
  VerifyIOVec(input);
  if (!input.empty()) {
    const std::string expanded = Expand(input);
    VerifyNonBlockedCompression(expanded);
    VerifyIOVec(input);
  }

  return result;
}

static bool IsValidCompressedBuffer(const std::string& c) {
  return snappy::IsValidCompressedBuffer(c.data(), c.size());
}
static bool Uncompress(const std::string& c, std::string* u) {
  return snappy::Uncompress(c.data(), c.size(), u);
}

// This test checks to ensure that snappy doesn't coredump if it gets
// corrupted data.
TEST(CorruptedTest, VerifyCorrupted) {
  std::string source = "making sure we don't crash with corrupted input";
  VLOG(1) << source;
  std::string dest;
  std::string uncmp;
  snappy::Compress(source.data(), source.size(), &dest);

  // Mess around with the data. It's hard to simulate all possible
  // corruptions; this is just one example ...
  CHECK_GT(dest.size(), 3);
  dest[1]--;
  dest[3]++;
  // this really ought to fail.
  CHECK(!IsValidCompressedBuffer(dest));
  CHECK(!Uncompress(dest, &uncmp));

  // This is testing for a security bug - a buffer that decompresses to 100k
  // but we lie in the snappy header and only reserve 0 bytes of memory :)
  source.resize(100000);
  for (size_t i = 0; i < source.length(); ++i) {
    source[i] = 'A';
  }
  snappy::Compress(source.data(), source.size(), &dest);
  dest[0] = dest[1] = dest[2] = dest[3] = 0;
  CHECK(!IsValidCompressedBuffer(dest));
  CHECK(!Uncompress(dest, &uncmp));

  if (sizeof(void *) == 4) {
    // Another security check; check a crazy big length can't DoS us with an
    // over-allocation.
    // Currently this is done only for 32-bit builds.  On 64-bit builds,
    // where 3 GB might be an acceptable allocation size, Uncompress()
    // attempts to decompress, and sometimes causes the test to run out of
    // memory.
    dest[0] = dest[1] = dest[2] = dest[3] = '\xff';
    // This decodes to a really large size, i.e., about 3 GB.
    dest[4] = 'k';
    CHECK(!IsValidCompressedBuffer(dest));
    CHECK(!Uncompress(dest, &uncmp));
  } else {
    LOG(WARNING) << "Crazy decompression lengths not checked on 64-bit build";
  }

  // This decodes to about 2 MB; much smaller, but should still fail.
  dest[0] = dest[1] = dest[2] = '\xff';
  dest[3] = 0x00;
  CHECK(!IsValidCompressedBuffer(dest));
  CHECK(!Uncompress(dest, &uncmp));

  // try reading stuff in from a bad file.
  for (int i = 1; i <= 3; ++i) {
    std::string data =
        ReadTestDataFile(StrFormat("baddata%d.snappy", i).c_str(), 0);
    std::string uncmp;
    // check that we don't return a crazy length
    size_t ulen;
    CHECK(!snappy::GetUncompressedLength(data.data(), data.size(), &ulen)
          || (ulen < (1<<20)));
    uint32 ulen2;
    snappy::ByteArraySource source(data.data(), data.size());
    CHECK(!snappy::GetUncompressedLength(&source, &ulen2) ||
          (ulen2 < (1<<20)));
    CHECK(!IsValidCompressedBuffer(data));
    CHECK(!Uncompress(data, &uncmp));
  }
}

// Helper routines to construct arbitrary compressed strings.
// These mirror the compression code in snappy.cc, but are copied
// here so that we can bypass some limitations in the how snappy.cc
// invokes these routines.
static void AppendLiteral(std::string* dst, const std::string& literal) {
  if (literal.empty()) return;
  int n = literal.size() - 1;
  if (n < 60) {
    // Fit length in tag byte
    dst->push_back(0 | (n << 2));
  } else {
    // Encode in upcoming bytes
    char number[4];
    int count = 0;
    while (n > 0) {
      number[count++] = n & 0xff;
      n >>= 8;
    }
    dst->push_back(0 | ((59+count) << 2));
    *dst += std::string(number, count);
  }
  *dst += literal;
}

static void AppendCopy(std::string* dst, int offset, int length) {
  while (length > 0) {
    // Figure out how much to copy in one shot
    int to_copy;
    if (length >= 68) {
      to_copy = 64;
    } else if (length > 64) {
      to_copy = 60;
    } else {
      to_copy = length;
    }
    length -= to_copy;

    if ((to_copy >= 4) && (to_copy < 12) && (offset < 2048)) {
      assert(to_copy-4 < 8);            // Must fit in 3 bits
      dst->push_back(1 | ((to_copy-4) << 2) | ((offset >> 8) << 5));
      dst->push_back(offset & 0xff);
    } else if (offset < 65536) {
      dst->push_back(2 | ((to_copy-1) << 2));
      dst->push_back(offset & 0xff);
      dst->push_back(offset >> 8);
    } else {
      dst->push_back(3 | ((to_copy-1) << 2));
      dst->push_back(offset & 0xff);
      dst->push_back((offset >> 8) & 0xff);
      dst->push_back((offset >> 16) & 0xff);
      dst->push_back((offset >> 24) & 0xff);
    }
  }
}

TEST(Snappy, SimpleTests) {
  Verify("");
  Verify("a");
  Verify("ab");
  Verify("abc");

  Verify("aaaaaaa" + std::string(16, 'b') + std::string("aaaaa") + "abc");
  Verify("aaaaaaa" + std::string(256, 'b') + std::string("aaaaa") + "abc");
  Verify("aaaaaaa" + std::string(2047, 'b') + std::string("aaaaa") + "abc");
  Verify("aaaaaaa" + std::string(65536, 'b') + std::string("aaaaa") + "abc");
  Verify("abcaaaaaaa" + std::string(65536, 'b') + std::string("aaaaa") + "abc");
}

// Verify max blowup (lots of four-byte copies)
TEST(Snappy, MaxBlowup) {
  std::mt19937 rng;
  std::uniform_int_distribution<int> uniform_byte(0, 255);
  std::string input;
  for (int i = 0; i < 80000; ++i)
    input.push_back(static_cast<char>(uniform_byte(rng)));

  for (int i = 0; i < 80000; i += 4) {
    std::string four_bytes(input.end() - i - 4, input.end() - i);
    input.append(four_bytes);
  }
  Verify(input);
}

TEST(Snappy, RandomData) {
  std::minstd_rand0 rng(FLAGS_test_random_seed);
  std::uniform_int_distribution<int> uniform_0_to_3(0, 3);
  std::uniform_int_distribution<int> uniform_0_to_8(0, 8);
  std::uniform_int_distribution<int> uniform_byte(0, 255);
  std::uniform_int_distribution<size_t> uniform_4k(0, 4095);
  std::uniform_int_distribution<size_t> uniform_64k(0, 65535);
  std::bernoulli_distribution one_in_ten(1.0 / 10);

  constexpr int num_ops = 20000;
  for (int i = 0; i < num_ops; i++) {
    if ((i % 1000) == 0) {
      VLOG(0) << "Random op " << i << " of " << num_ops;
    }

    std::string x;
    size_t len = uniform_4k(rng);
    if (i < 100) {
      len = 65536 + uniform_64k(rng);
    }
    while (x.size() < len) {
      int run_len = 1;
      if (one_in_ten(rng)) {
        int skewed_bits = uniform_0_to_8(rng);
        // int is guaranteed to hold at least 16 bits, this uses at most 8 bits.
        std::uniform_int_distribution<int> skewed_low(0,
                                                      (1 << skewed_bits) - 1);
        run_len = skewed_low(rng);
      }
      char c = static_cast<char>(uniform_byte(rng));
      if (i >= 100) {
        int skewed_bits = uniform_0_to_3(rng);
        // int is guaranteed to hold at least 16 bits, this uses at most 3 bits.
        std::uniform_int_distribution<int> skewed_low(0,
                                                      (1 << skewed_bits) - 1);
        c = static_cast<char>(skewed_low(rng));
      }
      while (run_len-- > 0 && x.size() < len) {
        x.push_back(c);
      }
    }

    Verify(x);
  }
}

TEST(Snappy, FourByteOffset) {
  // The new compressor cannot generate four-byte offsets since
  // it chops up the input into 32KB pieces.  So we hand-emit the
  // copy manually.

  // The two fragments that make up the input string.
  std::string fragment1 = "012345689abcdefghijklmnopqrstuvwxyz";
  std::string fragment2 = "some other string";

  // How many times each fragment is emitted.
  const int n1 = 2;
  const int n2 = 100000 / fragment2.size();
  const int length = n1 * fragment1.size() + n2 * fragment2.size();

  std::string compressed;
  Varint::Append32(&compressed, length);

  AppendLiteral(&compressed, fragment1);
  std::string src = fragment1;
  for (int i = 0; i < n2; i++) {
    AppendLiteral(&compressed, fragment2);
    src += fragment2;
  }
  AppendCopy(&compressed, src.size(), fragment1.size());
  src += fragment1;
  CHECK_EQ(length, src.size());

  std::string uncompressed;
  CHECK(snappy::IsValidCompressedBuffer(compressed.data(), compressed.size()));
  CHECK(snappy::Uncompress(compressed.data(), compressed.size(),
                           &uncompressed));
  CHECK_EQ(uncompressed, src);
}

TEST(Snappy, IOVecEdgeCases) {
  // Test some tricky edge cases in the iovec output that are not necessarily
  // exercised by random tests.

  // Our output blocks look like this initially (the last iovec is bigger
  // than depicted):
  // [  ] [ ] [    ] [        ] [        ]
  static const int kLengths[] = { 2, 1, 4, 8, 128 };

  struct iovec iov[ARRAYSIZE(kLengths)];
  for (int i = 0; i < ARRAYSIZE(kLengths); ++i) {
    iov[i].iov_base = new char[kLengths[i]];
    iov[i].iov_len = kLengths[i];
  }

  std::string compressed;
  Varint::Append32(&compressed, 22);

  // A literal whose output crosses three blocks.
  // [ab] [c] [123 ] [        ] [        ]
  AppendLiteral(&compressed, "abc123");

  // A copy whose output crosses two blocks (source and destination
  // segments marked).
  // [ab] [c] [1231] [23      ] [        ]
  //           ^--^   --
  AppendCopy(&compressed, 3, 3);

  // A copy where the input is, at first, in the block before the output:
  //
  // [ab] [c] [1231] [231231  ] [        ]
  //           ^---     ^---
  // Then during the copy, the pointers move such that the input and
  // output pointers are in the same block:
  //
  // [ab] [c] [1231] [23123123] [        ]
  //                  ^-    ^-
  // And then they move again, so that the output pointer is no longer
  // in the same block as the input pointer:
  // [ab] [c] [1231] [23123123] [123     ]
  //                    ^--      ^--
  AppendCopy(&compressed, 6, 9);

  // Finally, a copy where the input is from several blocks back,
  // and it also crosses three blocks:
  //
  // [ab] [c] [1231] [23123123] [123b    ]
  //   ^                            ^
  // [ab] [c] [1231] [23123123] [123bc   ]
  //       ^                         ^
  // [ab] [c] [1231] [23123123] [123bc12 ]
  //           ^-                     ^-
  AppendCopy(&compressed, 17, 4);

  CHECK(snappy::RawUncompressToIOVec(
      compressed.data(), compressed.size(), iov, ARRAYSIZE(iov)));
  CHECK_EQ(0, memcmp(iov[0].iov_base, "ab", 2));
  CHECK_EQ(0, memcmp(iov[1].iov_base, "c", 1));
  CHECK_EQ(0, memcmp(iov[2].iov_base, "1231", 4));
  CHECK_EQ(0, memcmp(iov[3].iov_base, "23123123", 8));
  CHECK_EQ(0, memcmp(iov[4].iov_base, "123bc12", 7));

  for (int i = 0; i < ARRAYSIZE(kLengths); ++i) {
    delete[] reinterpret_cast<char *>(iov[i].iov_base);
  }
}

TEST(Snappy, IOVecLiteralOverflow) {
  static const int kLengths[] = { 3, 4 };

  struct iovec iov[ARRAYSIZE(kLengths)];
  for (int i = 0; i < ARRAYSIZE(kLengths); ++i) {
    iov[i].iov_base = new char[kLengths[i]];
    iov[i].iov_len = kLengths[i];
  }

  std::string compressed;
  Varint::Append32(&compressed, 8);

  AppendLiteral(&compressed, "12345678");

  CHECK(!snappy::RawUncompressToIOVec(
      compressed.data(), compressed.size(), iov, ARRAYSIZE(iov)));

  for (int i = 0; i < ARRAYSIZE(kLengths); ++i) {
    delete[] reinterpret_cast<char *>(iov[i].iov_base);
  }
}

TEST(Snappy, IOVecCopyOverflow) {
  static const int kLengths[] = { 3, 4 };

  struct iovec iov[ARRAYSIZE(kLengths)];
  for (int i = 0; i < ARRAYSIZE(kLengths); ++i) {
    iov[i].iov_base = new char[kLengths[i]];
    iov[i].iov_len = kLengths[i];
  }

  std::string compressed;
  Varint::Append32(&compressed, 8);

  AppendLiteral(&compressed, "123");
  AppendCopy(&compressed, 3, 5);

  CHECK(!snappy::RawUncompressToIOVec(
      compressed.data(), compressed.size(), iov, ARRAYSIZE(iov)));

  for (int i = 0; i < ARRAYSIZE(kLengths); ++i) {
    delete[] reinterpret_cast<char *>(iov[i].iov_base);
  }
}

static bool CheckUncompressedLength(const std::string& compressed,
                                    size_t* ulength) {
  const bool result1 = snappy::GetUncompressedLength(compressed.data(),
                                                     compressed.size(),
                                                     ulength);

  snappy::ByteArraySource source(compressed.data(), compressed.size());
  uint32 length;
  const bool result2 = snappy::GetUncompressedLength(&source, &length);
  CHECK_EQ(result1, result2);
  return result1;
}

TEST(SnappyCorruption, TruncatedVarint) {
  std::string compressed, uncompressed;
  size_t ulength;
  compressed.push_back('\xf0');
  CHECK(!CheckUncompressedLength(compressed, &ulength));
  CHECK(!snappy::IsValidCompressedBuffer(compressed.data(), compressed.size()));
  CHECK(!snappy::Uncompress(compressed.data(), compressed.size(),
                            &uncompressed));
}

TEST(SnappyCorruption, UnterminatedVarint) {
  std::string compressed, uncompressed;
  size_t ulength;
  compressed.push_back('\x80');
  compressed.push_back('\x80');
  compressed.push_back('\x80');
  compressed.push_back('\x80');
  compressed.push_back('\x80');
  compressed.push_back(10);
  CHECK(!CheckUncompressedLength(compressed, &ulength));
  CHECK(!snappy::IsValidCompressedBuffer(compressed.data(), compressed.size()));
  CHECK(!snappy::Uncompress(compressed.data(), compressed.size(),
                            &uncompressed));
}

TEST(SnappyCorruption, OverflowingVarint) {
  std::string compressed, uncompressed;
  size_t ulength;
  compressed.push_back('\xfb');
  compressed.push_back('\xff');
  compressed.push_back('\xff');
  compressed.push_back('\xff');
  compressed.push_back('\x7f');
  CHECK(!CheckUncompressedLength(compressed, &ulength));
  CHECK(!snappy::IsValidCompressedBuffer(compressed.data(), compressed.size()));
  CHECK(!snappy::Uncompress(compressed.data(), compressed.size(),
                            &uncompressed));
}

TEST(Snappy, ReadPastEndOfBuffer) {
  // Check that we do not read past end of input

  // Make a compressed string that ends with a single-byte literal
  std::string compressed;
  Varint::Append32(&compressed, 1);
  AppendLiteral(&compressed, "x");

  std::string uncompressed;
  DataEndingAtUnreadablePage c(compressed);
  CHECK(snappy::Uncompress(c.data(), c.size(), &uncompressed));
  CHECK_EQ(uncompressed, std::string("x"));
}

// Check for an infinite loop caused by a copy with offset==0
TEST(Snappy, ZeroOffsetCopy) {
  const char* compressed = "\x40\x12\x00\x00";
  //  \x40              Length (must be > kMaxIncrementCopyOverflow)
  //  \x12\x00\x00      Copy with offset==0, length==5
  char uncompressed[100];
  EXPECT_FALSE(snappy::RawUncompress(compressed, 4, uncompressed));
}

TEST(Snappy, ZeroOffsetCopyValidation) {
  const char* compressed = "\x05\x12\x00\x00";
  //  \x05              Length
  //  \x12\x00\x00      Copy with offset==0, length==5
  EXPECT_FALSE(snappy::IsValidCompressedBuffer(compressed, 4));
}

namespace {

int TestFindMatchLength(const char* s1, const char *s2, unsigned length) {
  std::pair<size_t, bool> p =
      snappy::internal::FindMatchLength(s1, s2, s2 + length);
  CHECK_EQ(p.first < 8, p.second);
  return p.first;
}

}  // namespace

TEST(Snappy, FindMatchLength) {
  // Exercise all different code paths through the function.
  // 64-bit version:

  // Hit s1_limit in 64-bit loop, hit s1_limit in single-character loop.
  EXPECT_EQ(6, TestFindMatchLength("012345", "012345", 6));
  EXPECT_EQ(11, TestFindMatchLength("01234567abc", "01234567abc", 11));

  // Hit s1_limit in 64-bit loop, find a non-match in single-character loop.
  EXPECT_EQ(9, TestFindMatchLength("01234567abc", "01234567axc", 9));

  // Same, but edge cases.
  EXPECT_EQ(11, TestFindMatchLength("01234567abc!", "01234567abc!", 11));
  EXPECT_EQ(11, TestFindMatchLength("01234567abc!", "01234567abc?", 11));

  // Find non-match at once in first loop.
  EXPECT_EQ(0, TestFindMatchLength("01234567xxxxxxxx", "?1234567xxxxxxxx", 16));
  EXPECT_EQ(1, TestFindMatchLength("01234567xxxxxxxx", "0?234567xxxxxxxx", 16));
  EXPECT_EQ(4, TestFindMatchLength("01234567xxxxxxxx", "01237654xxxxxxxx", 16));
  EXPECT_EQ(7, TestFindMatchLength("01234567xxxxxxxx", "0123456?xxxxxxxx", 16));

  // Find non-match in first loop after one block.
  EXPECT_EQ(8, TestFindMatchLength("abcdefgh01234567xxxxxxxx",
                                   "abcdefgh?1234567xxxxxxxx", 24));
  EXPECT_EQ(9, TestFindMatchLength("abcdefgh01234567xxxxxxxx",
                                   "abcdefgh0?234567xxxxxxxx", 24));
  EXPECT_EQ(12, TestFindMatchLength("abcdefgh01234567xxxxxxxx",
                                    "abcdefgh01237654xxxxxxxx", 24));
  EXPECT_EQ(15, TestFindMatchLength("abcdefgh01234567xxxxxxxx",
                                    "abcdefgh0123456?xxxxxxxx", 24));

  // 32-bit version:

  // Short matches.
  EXPECT_EQ(0, TestFindMatchLength("01234567", "?1234567", 8));
  EXPECT_EQ(1, TestFindMatchLength("01234567", "0?234567", 8));
  EXPECT_EQ(2, TestFindMatchLength("01234567", "01?34567", 8));
  EXPECT_EQ(3, TestFindMatchLength("01234567", "012?4567", 8));
  EXPECT_EQ(4, TestFindMatchLength("01234567", "0123?567", 8));
  EXPECT_EQ(5, TestFindMatchLength("01234567", "01234?67", 8));
  EXPECT_EQ(6, TestFindMatchLength("01234567", "012345?7", 8));
  EXPECT_EQ(7, TestFindMatchLength("01234567", "0123456?", 8));
  EXPECT_EQ(7, TestFindMatchLength("01234567", "0123456?", 7));
  EXPECT_EQ(7, TestFindMatchLength("01234567!", "0123456??", 7));

  // Hit s1_limit in 32-bit loop, hit s1_limit in single-character loop.
  EXPECT_EQ(10, TestFindMatchLength("xxxxxxabcd", "xxxxxxabcd", 10));
  EXPECT_EQ(10, TestFindMatchLength("xxxxxxabcd?", "xxxxxxabcd?", 10));
  EXPECT_EQ(13, TestFindMatchLength("xxxxxxabcdef", "xxxxxxabcdef", 13));

  // Same, but edge cases.
  EXPECT_EQ(12, TestFindMatchLength("xxxxxx0123abc!", "xxxxxx0123abc!", 12));
  EXPECT_EQ(12, TestFindMatchLength("xxxxxx0123abc!", "xxxxxx0123abc?", 12));

  // Hit s1_limit in 32-bit loop, find a non-match in single-character loop.
  EXPECT_EQ(11, TestFindMatchLength("xxxxxx0123abc", "xxxxxx0123axc", 13));

  // Find non-match at once in first loop.
  EXPECT_EQ(6, TestFindMatchLength("xxxxxx0123xxxxxxxx",
                                   "xxxxxx?123xxxxxxxx", 18));
  EXPECT_EQ(7, TestFindMatchLength("xxxxxx0123xxxxxxxx",
                                   "xxxxxx0?23xxxxxxxx", 18));
  EXPECT_EQ(8, TestFindMatchLength("xxxxxx0123xxxxxxxx",
                                   "xxxxxx0132xxxxxxxx", 18));
  EXPECT_EQ(9, TestFindMatchLength("xxxxxx0123xxxxxxxx",
                                   "xxxxxx012?xxxxxxxx", 18));

  // Same, but edge cases.
  EXPECT_EQ(6, TestFindMatchLength("xxxxxx0123", "xxxxxx?123", 10));
  EXPECT_EQ(7, TestFindMatchLength("xxxxxx0123", "xxxxxx0?23", 10));
  EXPECT_EQ(8, TestFindMatchLength("xxxxxx0123", "xxxxxx0132", 10));
  EXPECT_EQ(9, TestFindMatchLength("xxxxxx0123", "xxxxxx012?", 10));

  // Find non-match in first loop after one block.
  EXPECT_EQ(10, TestFindMatchLength("xxxxxxabcd0123xx",
                                    "xxxxxxabcd?123xx", 16));
  EXPECT_EQ(11, TestFindMatchLength("xxxxxxabcd0123xx",
                                    "xxxxxxabcd0?23xx", 16));
  EXPECT_EQ(12, TestFindMatchLength("xxxxxxabcd0123xx",
                                    "xxxxxxabcd0132xx", 16));
  EXPECT_EQ(13, TestFindMatchLength("xxxxxxabcd0123xx",
                                    "xxxxxxabcd012?xx", 16));

  // Same, but edge cases.
  EXPECT_EQ(10, TestFindMatchLength("xxxxxxabcd0123", "xxxxxxabcd?123", 14));
  EXPECT_EQ(11, TestFindMatchLength("xxxxxxabcd0123", "xxxxxxabcd0?23", 14));
  EXPECT_EQ(12, TestFindMatchLength("xxxxxxabcd0123", "xxxxxxabcd0132", 14));
  EXPECT_EQ(13, TestFindMatchLength("xxxxxxabcd0123", "xxxxxxabcd012?", 14));
}

TEST(Snappy, FindMatchLengthRandom) {
  constexpr int kNumTrials = 10000;
  constexpr int kTypicalLength = 10;
  std::minstd_rand0 rng(FLAGS_test_random_seed);
  std::uniform_int_distribution<int> uniform_byte(0, 255);
  std::bernoulli_distribution one_in_two(1.0 / 2);
  std::bernoulli_distribution one_in_typical_length(1.0 / kTypicalLength);

  for (int i = 0; i < kNumTrials; i++) {
    std::string s, t;
    char a = static_cast<char>(uniform_byte(rng));
    char b = static_cast<char>(uniform_byte(rng));
    while (!one_in_typical_length(rng)) {
      s.push_back(one_in_two(rng) ? a : b);
      t.push_back(one_in_two(rng) ? a : b);
    }
    DataEndingAtUnreadablePage u(s);
    DataEndingAtUnreadablePage v(t);
    int matched = TestFindMatchLength(u.data(), v.data(), t.size());
    if (matched == t.size()) {
      EXPECT_EQ(s, t);
    } else {
      EXPECT_NE(s[matched], t[matched]);
      for (int j = 0; j < matched; j++) {
        EXPECT_EQ(s[j], t[j]);
      }
    }
  }
}

static uint16 MakeEntry(unsigned int extra,
                        unsigned int len,
                        unsigned int copy_offset) {
  // Check that all of the fields fit within the allocated space
  assert(extra       == (extra & 0x7));          // At most 3 bits
  assert(copy_offset == (copy_offset & 0x7));    // At most 3 bits
  assert(len         == (len & 0x7f));           // At most 7 bits
  return len | (copy_offset << 8) | (extra << 11);
}

// Check that the decompression table is correct, and optionally print out
// the computed one.
TEST(Snappy, VerifyCharTable) {
  using snappy::internal::LITERAL;
  using snappy::internal::COPY_1_BYTE_OFFSET;
  using snappy::internal::COPY_2_BYTE_OFFSET;
  using snappy::internal::COPY_4_BYTE_OFFSET;
  using snappy::internal::char_table;

  uint16 dst[256];

  // Place invalid entries in all places to detect missing initialization
  int assigned = 0;
  for (int i = 0; i < 256; i++) {
    dst[i] = 0xffff;
  }

  // Small LITERAL entries.  We store (len-1) in the top 6 bits.
  for (unsigned int len = 1; len <= 60; len++) {
    dst[LITERAL | ((len-1) << 2)] = MakeEntry(0, len, 0);
    assigned++;
  }

  // Large LITERAL entries.  We use 60..63 in the high 6 bits to
  // encode the number of bytes of length info that follow the opcode.
  for (unsigned int extra_bytes = 1; extra_bytes <= 4; extra_bytes++) {
    // We set the length field in the lookup table to 1 because extra
    // bytes encode len-1.
    dst[LITERAL | ((extra_bytes+59) << 2)] = MakeEntry(extra_bytes, 1, 0);
    assigned++;
  }

  // COPY_1_BYTE_OFFSET.
  //
  // The tag byte in the compressed data stores len-4 in 3 bits, and
  // offset/256 in 5 bits.  offset%256 is stored in the next byte.
  //
  // This format is used for length in range [4..11] and offset in
  // range [0..2047]
  for (unsigned int len = 4; len < 12; len++) {
    for (unsigned int offset = 0; offset < 2048; offset += 256) {
      dst[COPY_1_BYTE_OFFSET | ((len-4)<<2) | ((offset>>8)<<5)] =
        MakeEntry(1, len, offset>>8);
      assigned++;
    }
  }

  // COPY_2_BYTE_OFFSET.
  // Tag contains len-1 in top 6 bits, and offset in next two bytes.
  for (unsigned int len = 1; len <= 64; len++) {
    dst[COPY_2_BYTE_OFFSET | ((len-1)<<2)] = MakeEntry(2, len, 0);
    assigned++;
  }

  // COPY_4_BYTE_OFFSET.
  // Tag contents len-1 in top 6 bits, and offset in next four bytes.
  for (unsigned int len = 1; len <= 64; len++) {
    dst[COPY_4_BYTE_OFFSET | ((len-1)<<2)] = MakeEntry(4, len, 0);
    assigned++;
  }

  // Check that each entry was initialized exactly once.
  EXPECT_EQ(256, assigned) << "Assigned only " << assigned << " of 256";
  for (int i = 0; i < 256; i++) {
    EXPECT_NE(0xffff, dst[i]) << "Did not assign byte " << i;
  }

  if (FLAGS_snappy_dump_decompression_table) {
    printf("static const uint16 char_table[256] = {\n  ");
    for (int i = 0; i < 256; i++) {
      printf("0x%04x%s",
             dst[i],
             ((i == 255) ? "\n" : (((i%8) == 7) ? ",\n  " : ", ")));
    }
    printf("};\n");
  }

  // Check that computed table matched recorded table.
  for (int i = 0; i < 256; i++) {
    EXPECT_EQ(dst[i], char_table[i]) << "Mismatch in byte " << i;
  }
}

static void CompressFile(const char* fname) {
  std::string fullinput;
  CHECK_OK(file::GetContents(fname, &fullinput, file::Defaults()));

  std::string compressed;
  Compress(fullinput.data(), fullinput.size(), SNAPPY, &compressed, false);

  CHECK_OK(file::SetContents(std::string(fname).append(".comp"), compressed,
                             file::Defaults()));
}

static void UncompressFile(const char* fname) {
  std::string fullinput;
  CHECK_OK(file::GetContents(fname, &fullinput, file::Defaults()));

  size_t uncompLength;
  CHECK(CheckUncompressedLength(fullinput, &uncompLength));

  std::string uncompressed;
  uncompressed.resize(uncompLength);
  CHECK(snappy::Uncompress(fullinput.data(), fullinput.size(), &uncompressed));

  CHECK_OK(file::SetContents(std::string(fname).append(".uncomp"), uncompressed,
                             file::Defaults()));
}

static void MeasureFile(const char* fname) {
  std::string fullinput;
  CHECK_OK(file::GetContents(fname, &fullinput, file::Defaults()));
  printf("%-40s :\n", fname);

  int start_len = (FLAGS_start_len < 0) ? fullinput.size() : FLAGS_start_len;
  int end_len = fullinput.size();
  if (FLAGS_end_len >= 0) {
    end_len = std::min<int>(fullinput.size(), FLAGS_end_len);
  }
  for (int len = start_len; len <= end_len; len++) {
    const char* const input = fullinput.data();
    int repeats = (FLAGS_bytes + len) / (len + 1);
    if (FLAGS_zlib)     Measure(input, len, ZLIB, repeats, 1024<<10);
    if (FLAGS_lzo)      Measure(input, len, LZO, repeats, 1024<<10);
    if (FLAGS_snappy)    Measure(input, len, SNAPPY, repeats, 4096<<10);

    // For block-size based measurements
    if (0 && FLAGS_snappy) {
      Measure(input, len, SNAPPY, repeats, 8<<10);
      Measure(input, len, SNAPPY, repeats, 16<<10);
      Measure(input, len, SNAPPY, repeats, 32<<10);
      Measure(input, len, SNAPPY, repeats, 64<<10);
      Measure(input, len, SNAPPY, repeats, 256<<10);
      Measure(input, len, SNAPPY, repeats, 1024<<10);
    }
  }
}

static struct {
  const char* label;
  const char* filename;
  size_t size_limit;
} files[] = {
  { "html", "html", 0 },
  { "urls", "urls.10K", 0 },
  { "jpg", "fireworks.jpeg", 0 },
  { "jpg_200", "fireworks.jpeg", 200 },
  { "pdf", "paper-100k.pdf", 0 },
  { "html4", "html_x_4", 0 },
  { "txt1", "alice29.txt", 0 },
  { "txt2", "asyoulik.txt", 0 },
  { "txt3", "lcet10.txt", 0 },
  { "txt4", "plrabn12.txt", 0 },
  { "pb", "geo.protodata", 0 },
  { "gaviota", "kppkn.gtb", 0 },
};

static void BM_UFlat(int iters, int arg) {
  StopBenchmarkTiming();

  // Pick file to process based on "arg"
  CHECK_GE(arg, 0);
  CHECK_LT(arg, ARRAYSIZE(files));
  std::string contents =
      ReadTestDataFile(files[arg].filename, files[arg].size_limit);

  std::string zcontents;
  snappy::Compress(contents.data(), contents.size(), &zcontents);
  char* dst = new char[contents.size()];

  SetBenchmarkBytesProcessed(static_cast<int64>(iters) *
                             static_cast<int64>(contents.size()));
  SetBenchmarkLabel(files[arg].label);
  StartBenchmarkTiming();
  while (iters-- > 0) {
    CHECK(snappy::RawUncompress(zcontents.data(), zcontents.size(), dst));
  }
  StopBenchmarkTiming();

  delete[] dst;
}
BENCHMARK(BM_UFlat)->DenseRange(0, ARRAYSIZE(files) - 1);

static void BM_UValidate(int iters, int arg) {
  StopBenchmarkTiming();

  // Pick file to process based on "arg"
  CHECK_GE(arg, 0);
  CHECK_LT(arg, ARRAYSIZE(files));
  std::string contents =
      ReadTestDataFile(files[arg].filename, files[arg].size_limit);

  std::string zcontents;
  snappy::Compress(contents.data(), contents.size(), &zcontents);

  SetBenchmarkBytesProcessed(static_cast<int64>(iters) *
                             static_cast<int64>(contents.size()));
  SetBenchmarkLabel(files[arg].label);
  StartBenchmarkTiming();
  while (iters-- > 0) {
    CHECK(snappy::IsValidCompressedBuffer(zcontents.data(), zcontents.size()));
  }
  StopBenchmarkTiming();
}
BENCHMARK(BM_UValidate)->DenseRange(0, 4);

static void BM_UIOVec(int iters, int arg) {
  StopBenchmarkTiming();

  // Pick file to process based on "arg"
  CHECK_GE(arg, 0);
  CHECK_LT(arg, ARRAYSIZE(files));
  std::string contents =
      ReadTestDataFile(files[arg].filename, files[arg].size_limit);

  std::string zcontents;
  snappy::Compress(contents.data(), contents.size(), &zcontents);

  // Uncompress into an iovec containing ten entries.
  const int kNumEntries = 10;
  struct iovec iov[kNumEntries];
  char *dst = new char[contents.size()];
  int used_so_far = 0;
  for (int i = 0; i < kNumEntries; ++i) {
    iov[i].iov_base = dst + used_so_far;
    if (used_so_far == contents.size()) {
      iov[i].iov_len = 0;
      continue;
    }

    if (i == kNumEntries - 1) {
      iov[i].iov_len = contents.size() - used_so_far;
    } else {
      iov[i].iov_len = contents.size() / kNumEntries;
    }
    used_so_far += iov[i].iov_len;
  }

  SetBenchmarkBytesProcessed(static_cast<int64>(iters) *
                             static_cast<int64>(contents.size()));
  SetBenchmarkLabel(files[arg].label);
  StartBenchmarkTiming();
  while (iters-- > 0) {
    CHECK(snappy::RawUncompressToIOVec(zcontents.data(), zcontents.size(), iov,
                                       kNumEntries));
  }
  StopBenchmarkTiming();

  delete[] dst;
}
BENCHMARK(BM_UIOVec)->DenseRange(0, 4);

static void BM_UFlatSink(int iters, int arg) {
  StopBenchmarkTiming();

  // Pick file to process based on "arg"
  CHECK_GE(arg, 0);
  CHECK_LT(arg, ARRAYSIZE(files));
  std::string contents =
      ReadTestDataFile(files[arg].filename, files[arg].size_limit);

  std::string zcontents;
  snappy::Compress(contents.data(), contents.size(), &zcontents);
  char* dst = new char[contents.size()];

  SetBenchmarkBytesProcessed(static_cast<int64>(iters) *
                             static_cast<int64>(contents.size()));
  SetBenchmarkLabel(files[arg].label);
  StartBenchmarkTiming();
  while (iters-- > 0) {
    snappy::ByteArraySource source(zcontents.data(), zcontents.size());
    snappy::UncheckedByteArraySink sink(dst);
    CHECK(snappy::Uncompress(&source, &sink));
  }
  StopBenchmarkTiming();

  std::string s(dst, contents.size());
  CHECK_EQ(contents, s);

  delete[] dst;
}

BENCHMARK(BM_UFlatSink)->DenseRange(0, ARRAYSIZE(files) - 1);

static void BM_ZFlat(int iters, int arg) {
  StopBenchmarkTiming();

  // Pick file to process based on "arg"
  CHECK_GE(arg, 0);
  CHECK_LT(arg, ARRAYSIZE(files));
  std::string contents =
      ReadTestDataFile(files[arg].filename, files[arg].size_limit);

  char* dst = new char[snappy::MaxCompressedLength(contents.size())];

  SetBenchmarkBytesProcessed(static_cast<int64>(iters) *
                             static_cast<int64>(contents.size()));
  StartBenchmarkTiming();

  size_t zsize = 0;
  while (iters-- > 0) {
    snappy::RawCompress(contents.data(), contents.size(), dst, &zsize);
  }
  StopBenchmarkTiming();
  const double compression_ratio =
      static_cast<double>(zsize) / std::max<size_t>(1, contents.size());
  SetBenchmarkLabel(StrFormat("%s (%.2f %%)", files[arg].label,
                              100.0 * compression_ratio));
  VLOG(0) << StrFormat("compression for %s: %zd -> %zd bytes",
                       files[arg].label, static_cast<int>(contents.size()),
                       static_cast<int>(zsize));
  delete[] dst;
}
BENCHMARK(BM_ZFlat)->DenseRange(0, ARRAYSIZE(files) - 1);

static void BM_ZFlatAll(int iters, int arg) {
  StopBenchmarkTiming();

  CHECK_EQ(arg, 0);
  const int num_files = ARRAYSIZE(files);

  std::vector<std::string> contents(num_files);
  std::vector<char*> dst(num_files);

  int64 total_contents_size = 0;
  for (int i = 0; i < num_files; ++i) {
    contents[i] = ReadTestDataFile(files[i].filename, files[i].size_limit);
    dst[i] = new char[snappy::MaxCompressedLength(contents[i].size())];
    total_contents_size += contents[i].size();
  }

  SetBenchmarkBytesProcessed(static_cast<int64>(iters) * total_contents_size);
  StartBenchmarkTiming();

  size_t zsize = 0;
  while (iters-- > 0) {
    for (int i = 0; i < num_files; ++i) {
      snappy::RawCompress(contents[i].data(), contents[i].size(), dst[i],
                          &zsize);
    }
  }
  StopBenchmarkTiming();

  for (int i = 0; i < num_files; ++i) {
    delete[] dst[i];
  }
  SetBenchmarkLabel(StrFormat("%d files", num_files));
}
BENCHMARK(BM_ZFlatAll)->DenseRange(0, 0);

static void BM_ZFlatIncreasingTableSize(int iters, int arg) {
  StopBenchmarkTiming();

  CHECK_EQ(arg, 0);
  CHECK_GT(ARRAYSIZE(files), 0);
  const std::string base_content =
      ReadTestDataFile(files[0].filename, files[0].size_limit);

  std::vector<std::string> contents;
  std::vector<char*> dst;
  int64 total_contents_size = 0;
  for (int table_bits = kMinHashTableBits; table_bits <= kMaxHashTableBits;
       ++table_bits) {
    std::string content = base_content;
    content.resize(1 << table_bits);
    dst.push_back(new char[snappy::MaxCompressedLength(content.size())]);
    total_contents_size += content.size();
    contents.push_back(std::move(content));
  }

  size_t zsize = 0;
  SetBenchmarkBytesProcessed(static_cast<int64>(iters) * total_contents_size);
  StartBenchmarkTiming();
  while (iters-- > 0) {
    for (int i = 0; i < contents.size(); ++i) {
      snappy::RawCompress(contents[i].data(), contents[i].size(), dst[i],
                          &zsize);
    }
  }
  StopBenchmarkTiming();

  for (int i = 0; i < dst.size(); ++i) {
    delete[] dst[i];
  }
  SetBenchmarkLabel(StrFormat("%zd tables", contents.size()));
}
BENCHMARK(BM_ZFlatIncreasingTableSize)->DenseRange(0, 0);

}  // namespace snappy

int main(int argc, char** argv) {
  InitGoogle(argv[0], &argc, &argv, true);
  RunSpecifiedBenchmarks();

  if (argc >= 2) {
    for (int arg = 1; arg < argc; arg++) {
      if (FLAGS_write_compressed) {
        snappy::CompressFile(argv[arg]);
      } else if (FLAGS_write_uncompressed) {
        snappy::UncompressFile(argv[arg]);
      } else {
        snappy::MeasureFile(argv[arg]);
      }
    }
    return 0;
  }

  return RUN_ALL_TESTS();
}
