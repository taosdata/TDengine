// rdbcommon.h -- shared key layout, comparator and payload generation for
// rdbwrite and rdbread.
//
// The key record and comparator are copies of SLastKey and myCmp from
// source/dnode/vnode/src/tsdb/tsdbCache.c. Keeping them identical matters:
// the comparator decides key ordering, ordering decides how records are packed
// into blocks, and block layout is what the checksums cover. A different
// comparator would produce a different file and test something else.

#ifndef RDB_COMMON_H
#define RDB_COMMON_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

// ---- from tsdbCache.c ----
typedef int64_t tb_uid_t;

typedef struct {
  tb_uid_t uid;
  int16_t  cid;
  int8_t   lflag;
} SLastKey;

// tsdbCache.c hashes only these bytes, not sizeof(SLastKey), which would
// include padding.
#define ROCKS_KEY_LEN (sizeof(tb_uid_t) + sizeof(int16_t) + sizeof(int8_t))

enum { LFLAG_LAST_ROW = 0, LFLAG_LAST = 1 };

#define ROCKS_BATCH_SIZE 4096  // tsdbCache.c:23

// ---- defaults ----
#define RDB_DEFAULT_RECORDS    200000
#define RDB_DEFAULT_VALUE_SIZE 96
#define RDB_MAX_VALUE_SIZE     (1 << 20)

// ---- comparator, copied from myCmp() in tsdbCache.c ----
static const char *rdb_cmp_name(void *state) {
  (void)state;
  return "myCmp";
}

static void rdb_cmp_destroy(void *state) { (void)state; }

static int rdb_last_key_cmp(void *state, const char *a, size_t alen, const char *b,
                            size_t blen) {
  (void)state;
  (void)alen;
  (void)blen;
  SLastKey *lhs = (SLastKey *)a;
  SLastKey *rhs = (SLastKey *)b;

  if (lhs->uid < rhs->uid) return -1;
  else if (lhs->uid > rhs->uid) return 1;

  if (lhs->cid < rhs->cid) return -1;
  else if (lhs->cid > rhs->cid) return 1;

  if ((lhs->lflag & LFLAG_LAST) < (rhs->lflag & LFLAG_LAST)) return -1;
  else if ((lhs->lflag & LFLAG_LAST) > (rhs->lflag & LFLAG_LAST)) return 1;

  return 0;
}

// ---- crc32c, for an end-to-end check independent of rocksdb's own ----
// rocksdb verifies block integrity; this verifies that the value we read back
// is the value we wrote. A block can pass its checksum and still hand back the
// wrong bytes if something went wrong above that layer.
static uint32_t rdb_crc32c_table[256];

static void rdb_crc32c_init(void) {
  for (uint32_t i = 0; i < 256; i++) {
    uint32_t c = i;
    for (int k = 0; k < 8; k++) c = (c & 1) ? (0x82f63b78u ^ (c >> 1)) : (c >> 1);
    rdb_crc32c_table[i] = c;
  }
}

static uint32_t rdb_crc32c(const uint8_t *p, size_t n) {
  uint32_t c = 0xffffffffu;
  for (size_t i = 0; i < n; i++) c = rdb_crc32c_table[(c ^ p[i]) & 0xff] ^ (c >> 8);
  return c ^ 0xffffffffu;
}

// ---- deterministic key/value generation ----
// Same (i, seed) always yields the same record, so a reader can recompute the
// expected value without the writer telling it anything.

static uint64_t rdb_mix(uint64_t x) {
  // splitmix64: cheap, well-distributed, and reproducible across machines.
  x += 0x9e3779b97f4a7c15ull;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
  return x ^ (x >> 31);
}

static void rdb_make_key(SLastKey *key, int64_t i, uint64_t seed) {
  uint64_t h = rdb_mix((uint64_t)i ^ (seed * 0x100000001b3ull));
  memset(key, 0, sizeof(*key));
  // Spread over many uids with several columns each, as a real cache would be.
  key->uid   = (tb_uid_t)(h % 20000) + 1;
  key->cid   = (int16_t)((h >> 20) % 64);
  key->lflag = (int8_t)((h >> 40) & 1);
}

// Layout: [4-byte crc32c][8-byte index][payload...]
// The crc covers the index, the payload, and the key bytes, so a value that is
// intact but attached to the wrong key is also detected.
static void rdb_make_value(uint8_t *val, int32_t vsize, const SLastKey *key, int64_t i,
                           uint64_t seed) {
  memset(val, 0, (size_t)vsize);
  if (vsize >= 12) {
    memcpy(val + 4, &i, sizeof(i));
    uint64_t h = rdb_mix((uint64_t)i * 0x9e3779b1u + seed);
    for (int32_t k = 12; k < vsize; k++) {
      if (((k - 12) & 7) == 0) h = rdb_mix(h);
      val[k] = (uint8_t)(h >> (8 * ((k - 12) & 7)));
    }
  }
  // crc over everything after the crc field, plus the key
  uint32_t crc = 0;
  if (vsize > 4) {
    crc = rdb_crc32c(val + 4, (size_t)vsize - 4);
    uint8_t kb[ROCKS_KEY_LEN];
    memcpy(kb, key, ROCKS_KEY_LEN);
    // fold the key in
    for (size_t k = 0; k < ROCKS_KEY_LEN; k++)
      crc = rdb_crc32c_table[(crc ^ kb[k]) & 0xff] ^ (crc >> 8);
  }
  val[0] = (uint8_t)(crc & 0xff);
  val[1] = (uint8_t)((crc >> 8) & 0xff);
  val[2] = (uint8_t)((crc >> 16) & 0xff);
  val[3] = (uint8_t)((crc >> 24) & 0xff);
}

// Recompute the crc of a value as read back, for comparison with its stored crc.
// Used by rdbread only; the attribute keeps rdbwrite warning-free.
static __attribute__((unused)) uint32_t rdb_value_crc(const uint8_t *val, int32_t vsize, const SLastKey *key) {
  if (vsize <= 4) return 0;
  uint32_t crc = rdb_crc32c(val + 4, (size_t)vsize - 4);
  uint8_t  kb[ROCKS_KEY_LEN];
  memcpy(kb, key, ROCKS_KEY_LEN);
  for (size_t k = 0; k < ROCKS_KEY_LEN; k++)
    crc = rdb_crc32c_table[(crc ^ kb[k]) & 0xff] ^ (crc >> 8);
  return crc;
}

static __attribute__((unused)) uint32_t rdb_stored_crc(const uint8_t *val) {
  return (uint32_t)val[0] | ((uint32_t)val[1] << 8) | ((uint32_t)val[2] << 16) |
         ((uint32_t)val[3] << 24);
}

#endif  // RDB_COMMON_H
