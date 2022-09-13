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

typedef struct SDiskColBuilder SDiskColBuilder;
struct SDiskColBuilder {
  uint8_t  flags;
  uint8_t *pBitMap;
  int32_t *aOffset;
  int32_t  nData;
  uint8_t *pData;
};

int32_t tDiskColAddVal(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;
  // TODO
  return code;
}

// ================================================================
typedef struct SDiskDataBuilder SDiskDataBuilder;
struct SDiskDataBuilder {
  SDiskDataHdr hdr;
  SArray      *aBlockCol;  // SArray<SBlockCol>
};

int32_t tDiskDataBuilderCreate(SDiskDataBuilder **ppBuilder) {
  int32_t code = 0;
  // TODO
  return code;
}

void tDiskDataBuilderDestroy(SDiskDataBuilder *pBuilder) {
  // TODO
}

void tDiskDataBuilderInit(SDiskDataBuilder *pBuilder, int64_t suid, int64_t uid, STSchema *pTSchema, int8_t cmprAlg) {
  pBuilder->hdr = (SDiskDataHdr){.delimiter = TSDB_FILE_DLMT,  //
                                 .fmtVer = 0,
                                 .suid = suid,
                                 .uid = uid,
                                 .cmprAlg = cmprAlg};
}

void tDiskDataBuilderReset(SDiskDataBuilder *pBuilder) {
  // TODO
}

int32_t tDiskDataBuilderAddRow(SDiskDataBuilder *pBuilder, TSDBROW *pRow, STSchema *pTSchema, int64_t uid) {
  int32_t code = 0;

  // uid (todo)

  // version (todo)

  // TSKEY (todo)

  SRowIter iter = {0};
  tRowIterInit(&iter, pRow, pTSchema);

  for (int32_t iDiskCol = 0; iDiskCol < 0; iDiskCol++) {
  }

  return code;
}

int32_t tDiskDataBuilderGet(SDiskDataBuilder *pBuilder, uint8_t **ppData) {
  int32_t code = 0;
  // TODO
  return code;
}