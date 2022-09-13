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

#include "tsdb.h"

// Integer =====================================================
typedef struct {
  int8_t   rawCopy;
  int64_t  prevVal;
  int32_t  nVal;
  int32_t  nBuf;
  uint8_t *pBuf;
} SIntCompressor;

#define I64_SAFE_ADD(a, b) (((a) >= 0 && (b) <= INT64_MAX - (b)) || ((a) < 0 && (b) >= INT64_MIN - (a)))
#define SIMPLE8B_MAX       ((uint64_t)1152921504606846974LL)

static int32_t tsdbCmprI64(SIntCompressor *pCompressor, int64_t val) {
  int32_t code = 0;

  // raw copy
  if (pCompressor->rawCopy) {
    memcpy(pCompressor->pBuf + pCompressor->nBuf, &val, sizeof(val));
    pCompressor->nBuf += sizeof(val);
    pCompressor->nVal++;
    goto _exit;
  }

  if (!I64_SAFE_ADD(val, pCompressor->prevVal)) {
    pCompressor->rawCopy = 1;
    // TODO: decompress and copy
    pCompressor->nVal++;
    goto _exit;
  }

  int64_t diff = val - pCompressor->prevVal;
  uint8_t zigzag = ZIGZAGE(int64_t, diff);

  if (zigzag >= SIMPLE8B_MAX) {
    pCompressor->rawCopy = 1;
    // TODO: decompress and copy
    pCompressor->nVal++;
    goto _exit;
  }

_exit:
  return code;
}

// Timestamp =====================================================

// Float =====================================================