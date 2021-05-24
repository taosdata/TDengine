/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "os.h"
#include "hashfunc.h"
#include "tutil.h"

#define ROTL32(x, r) ((x) << (r) | (x) >> (32u - (r)))

#define FMIX32(h)      \
  do {                 \
    (h) ^= (h) >> 16;  \
    (h) *= 0x85ebca6b; \
    (h) ^= (h) >> 13;  \
    (h) *= 0xc2b2ae35; \
    (h) ^= (h) >> 16;  \
  } while (0)
  
uint32_t MurmurHash3_32(const char *key, uint32_t len) {
  const uint8_t *data = (const uint8_t *)key;
  const int nblocks = len >> 2u;

  uint32_t h1 = 0x12345678;

  const uint32_t c1 = 0xcc9e2d51;
  const uint32_t c2 = 0x1b873593;

  const uint32_t *blocks = (const uint32_t *)(data + nblocks * 4);

  for (int i = -nblocks; i; i++) {
    uint32_t k1 = blocks[i];

    k1 *= c1;
    k1 = ROTL32(k1, 15u);
    k1 *= c2;

    h1 ^= k1;
    h1 = ROTL32(h1, 13u);
    h1 = h1 * 5 + 0xe6546b64;
  }

  const uint8_t *tail = (data + nblocks * 4);

  uint32_t k1 = 0;

  switch (len & 3u) {
    case 3:
      k1 ^= tail[2] << 16;
    case 2:
      k1 ^= tail[1] << 8;
    case 1:
      k1 ^= tail[0];
      k1 *= c1;
      k1 = ROTL32(k1, 15u);
      k1 *= c2;
      h1 ^= k1;
  };

  h1 ^= len;

  FMIX32(h1);

  return h1;
}

uint32_t taosIntHash_32(const char *key, uint32_t UNUSED_PARAM(len)) { return *(uint32_t *)key; }
uint32_t taosIntHash_16(const char *key, uint32_t UNUSED_PARAM(len)) { return *(uint16_t *)key; }
uint32_t taosIntHash_8(const char *key, uint32_t UNUSED_PARAM(len)) { return *(uint8_t *)key; }

uint32_t taosIntHash_64(const char *key, uint32_t UNUSED_PARAM(len)) {
  uint64_t val = *(uint64_t *)key;

  uint64_t hash = val >> 16U;
  hash += (val & 0xFFFFU);
  
  return (uint32_t)hash;
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