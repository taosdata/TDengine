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

#ifndef _TD_TKV_DEF_H_
#define _TD_TKV_DEF_H_

#ifdef USE_ROCKSDB
#include <rocksdb/c.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif
struct STkvDb {
#ifdef USE_ROCKSDB
  rocksdb_t *db;
#endif
};

struct STkvOpts {
#ifdef USE_ROCKSDB
  rocksdb_options_t *opts;
#endif
};

struct STkvCache {
  // TODO
};

struct STkvReadOpts {
#ifdef USE_ROCKSDB
  rocksdb_readoptions_t *ropts;
#endif
};

struct STkvWriteOpts {
#ifdef USE_ROCKSDB
  rocksdb_writeoptions_t *wopts;
#endif
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_TKV_DEF_H_*/