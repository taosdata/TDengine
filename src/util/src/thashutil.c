/**
 * MurmurHash3 by Austin Appleby
 * @ref
 * https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
 *
 * Plese refers to the link above for the complete implementation of
 * MurmurHash algorithm
 *
 */
#include "tutil.h"
#include "hashutil.h"

#define ROTL32(x, r) ((x) << (r) | (x) >> (32 - (r)))

#define FMIX32(h)      \
  do {                 \
    (h) ^= (h) >> 16;  \
    (h) *= 0x85ebca6b; \
    (h) ^= (h) >> 13;  \
    (h) *= 0xc2b2ae35; \
    (h) ^= (h) >> 16;  \
  } while (0)

static void MurmurHash3_32_s(const void *key, int len, uint32_t seed, void *out) {
  const uint8_t *data = (const uint8_t *)key;
  const int      nblocks = len / 4;

  uint32_t h1 = seed;

  const uint32_t c1 = 0xcc9e2d51;
  const uint32_t c2 = 0x1b873593;

  const uint32_t *blocks = (const uint32_t *)(data + nblocks * 4);

  for (int i = -nblocks; i; i++) {
    uint32_t k1 = blocks[i];

    k1 *= c1;
    k1 = ROTL32(k1, 15);
    k1 *= c2;

    h1 ^= k1;
    h1 = ROTL32(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
  }

  const uint8_t *tail = (data + nblocks * 4);

  uint32_t k1 = 0;

  switch (len & 3) {
    case 3:
      k1 ^= tail[2] << 16;
    case 2:
      k1 ^= tail[1] << 8;
    case 1:
      k1 ^= tail[0];
      k1 *= c1;
      k1 = ROTL32(k1, 15);
      k1 *= c2;
      h1 ^= k1;
  };

  h1 ^= len;

  FMIX32(h1);

  *(uint32_t *)out = h1;
}

uint32_t MurmurHash3_32(const char *key, uint32_t len) {
  const int32_t hashSeed = 0x12345678;

  uint32_t val = 0;
  MurmurHash3_32_s(key, len, hashSeed, &val);

  return val;
}

uint32_t taosIntHash_32(const char *key, uint32_t UNUSED_PARAM(len)) { return *(uint32_t *)key; }
uint32_t taosIntHash_16(const char *key, uint32_t UNUSED_PARAM(len)) { return *(uint16_t *)key; }
uint32_t taosIntHash_8(const char *key, uint32_t UNUSED_PARAM(len)) { return *(uint8_t *)key; }

uint32_t taosIntHash_64(const char *key, uint32_t UNUSED_PARAM(len)) {
  uint64_t val = *(uint64_t *)key;

  uint64_t hash = val >> 16U;
  hash += (val & 0xFFFFU);
  
  return hash;
}

_hash_fn_t taosGetDefaultHashFunction(int32_t type) {
  _hash_fn_t fn = NULL;
  switch(type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_BIGINT: fn = taosIntHash_64;break;
    case TSDB_DATA_TYPE_BINARY: fn = MurmurHash3_32;break;
    case TSDB_DATA_TYPE_INT: fn = taosIntHash_32; break;
    case TSDB_DATA_TYPE_SMALLINT: fn = taosIntHash_16; break;
    case TSDB_DATA_TYPE_TINYINT: fn = taosIntHash_8; break;
    default: fn = taosIntHash_32;break;
  }
  
  return fn;
}