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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taosdef.h"
#include "tglobal.h"
#include "ttimer.h"
#include "tulog.h"
#include "tutil.h"

int32_t BUILDIN_CLZL(uint64_t val) {
  unsigned long r = 0;
  _BitScanReverse64(&r, val);
  return (int)(r >> 3);
}

int32_t BUILDIN_CLZ(uint32_t val) {
  unsigned long r = 0;
  _BitScanReverse(&r, val);
  return (int)(r >> 3);
}

int32_t BUILDIN_CTZL(uint64_t val) {
  unsigned long r = 0;
  _BitScanForward64(&r, val);
  return (int)(r >> 3);
}

int32_t BUILDIN_CTZ(uint32_t val) {
  unsigned long r = 0;
  _BitScanForward(&r, val);
  return (int)(r >> 3);
}
