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

#include "vnodeInt.h"

const STsdbCfg defautlTsdbOptions = {.precision = 0,
                                     .lruCacheSize = 0,
                                     .days = 10,
                                     .minRows = 100,
                                     .maxRows = 4096,
                                     .keep2 = 3650,
                                     .keep0 = 3650,
                                     .keep1 = 3650,
                                     .update = 0,
                                     .compression = TWO_STAGE_COMP};

int tsdbValidateOptions(const STsdbCfg *pTsdbOptions) {
  // TODO
  return 0;
}

void tsdbOptionsCopy(STsdbCfg *pDest, const STsdbCfg *pSrc) { memcpy(pDest, pSrc, sizeof(STsdbCfg)); }