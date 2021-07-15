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
#include "tcompare.h"

#define ROTL32(x, r) ((x) << (r) | (x) >> (32u - (r)))

#define DLT (FLT_COMPAR_TOL_FACTOR * FLT_EPSILON)
#define BASE 1000

#define FMIX32(h)      \
  do {                 \
    (h) ^= (h) >> 16;  \
    (h) *= 0x85ebca6b; \
    (h) ^= (h) >> 13;  \
    (h) *= 0xc2b2ae35; \
    (h) ^= (h) >> 16; } while (0)
  
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
uint32_t taosFloatHash(const char *key, uint32_t UNUSED_PARAM(len)) {
  float f = GET_FLOAT_VAL(key); 
  if (isnan(f)) {
    return 0x7fc00000;
  }
   
  if (FLT_EQUAL(f, 0.0)) {
    return 0;
  } 
  if (fabs(f) < FLT_MAX/BASE - DLT) {
   int t = (int)(round(BASE * (f + DLT)));
   return (uint32_t)t;
  } else {
   return 0x7fc00000;
  }
}
uint32_t taosDoubleHash(const char *key, uint32_t UNUSED_PARAM(len)) {
  double f = GET_DOUBLE_VAL(key);  
  if (isnan(f)) {
    return 0x7fc00000;
  }

  if (FLT_EQUAL(f, 0.0)) {
    return 0;
  } 
  if (fabs(f) < DBL_MAX/BASE - DLT) {
   int t = (int)(round(BASE * (f + DLT)));
   return (uint32_t)t;
  } else {
   return 0x7fc00000;
  } 
}
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
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_BIGINT:   
      fn = taosIntHash_64;
      break;
    case TSDB_DATA_TYPE_BINARY:   
      fn = MurmurHash3_32;
      break;
    case TSDB_DATA_TYPE_NCHAR:    
      fn = MurmurHash3_32;
      break;
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_INT:      
      fn = taosIntHash_32; 
      break;
    case TSDB_DATA_TYPE_SMALLINT: 
    case TSDB_DATA_TYPE_USMALLINT:  
      fn = taosIntHash_16; 
      break;
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_TINYINT:  
      fn = taosIntHash_8; 
      break;
    case TSDB_DATA_TYPE_FLOAT:    
      fn = taosFloatHash; 
      break;                             
    case TSDB_DATA_TYPE_DOUBLE:   
      fn = taosDoubleHash; 
      break;                             
    default: 
      fn = taosIntHash_32;
      break;
  }
  
  return fn;
}

int32_t taosFloatEqual(const void *a, const void *b, size_t UNUSED_PARAM(sz)) {
  return getComparFunc(TSDB_DATA_TYPE_FLOAT, -1)(a, b);  
}

int32_t taosDoubleEqual(const void *a, const void *b, size_t UNUSED_PARAM(sz)) {
  return getComparFunc(TSDB_DATA_TYPE_DOUBLE, -1)(a, b);  
}

_equal_fn_t taosGetDefaultEqualFunction(int32_t type) {
  _equal_fn_t fn = NULL;
  switch (type) {
    case TSDB_DATA_TYPE_FLOAT:  fn = taosFloatEqual;  break; 
    case TSDB_DATA_TYPE_DOUBLE: fn = taosDoubleEqual; break;
    default: fn = memcmp; break;
  }
  return fn;
  
}
