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

#ifndef _TD_META_IMPL_H_
#define _TD_META_IMPL_H_

#include "os.h"

#include "taosmsg.h"

#ifdef __cplusplus
extern "C" {
#endif
typedef uint64_t tb_uid_t;

/* ------------------------ SMetaOptions ------------------------ */
struct SMetaOptions {
  size_t lruCacheSize;  // LRU cache size
};

/* ------------------------ STbOptions ------------------------ */
#define META_NORMAL_TABLE ((uint8_t)1)
#define META_SUPER_TABLE ((uint8_t)2)
#define META_CHILD_TABLE ((uint8_t)3)

typedef struct {
} SSMAOptions;

// super table options
typedef struct {
  tb_uid_t  uid;
  STSchema* pSchema;
  STSchema* pTagSchema;
} SSTbOptions;

// child table options
typedef struct {
  tb_uid_t suid;
  SKVRow   tags;
} SCTbOptions;

// normal table options
typedef struct {
  SSchema* pSchame;
} SNTbOptions;

struct STbOptions {
  uint8_t     type;
  char*       name;
  uint64_t    ttl;   // time to live
  SSMAOptions bsma;  // Block-wise sma
  union {
    SSTbOptions stbOptions;
    SNTbOptions ntbOptions;
    SCTbOptions ctbOptions;
  };
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_META_IMPL_H_*/