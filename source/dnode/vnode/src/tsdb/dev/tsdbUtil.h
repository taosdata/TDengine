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

#ifndef _TSDB_UTIL_H
#define _TSDB_UTIL_H

#include "tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Exposed Handle */
typedef struct SDelBlock SDelBlock;
typedef struct SDelBlk   SDelBlk;

/* Exposed APIs */

/* Exposed Structs */
struct SDelBlock {
  // <suid, uid, version, skey, ekey>
  SColData aColData[4];
};

struct SDelBlk {
  int64_t suidMax;
  int64_t suidMin;
  int64_t uidMax;
  int64_t uidMin;
  int64_t verMax;
  int64_t verMin;
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_UTIL_H*/