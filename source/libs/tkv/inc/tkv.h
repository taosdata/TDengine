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

#ifndef _TD_TKV_H_
#define _TD_TKV_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

// Forward declaration
typedef struct TDB     TDB;
typedef struct TDB_ENV TDB_ENV;

// SKey
typedef struct {
  void *   bdata;
  uint32_t size;
} TDB_KEY, TDB_VALUE;

#ifdef __cplusplus
}
#endif

#endif /*_TD_TKV_H_*/