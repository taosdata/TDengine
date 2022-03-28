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

#include "vnd.h"

const SVnodeCfg defaultVnodeOptions = {
    .wsize = 96 * 1024 * 1024, .ssize = 1 * 1024 * 1024, .lsize = 1024, .walCfg = {.level = TAOS_WAL_WRITE}}; /* TODO */

void vnodeOptionsInit(SVnodeCfg *pVnodeOptions) { /* TODO */
  vnodeOptionsCopy(pVnodeOptions, &defaultVnodeOptions);
}

void vnodeOptionsClear(SVnodeCfg *pVnodeOptions) { /* TODO */
}

int vnodeValidateOptions(const SVnodeCfg *pVnodeOptions) {
  // TODO
  return 0;
}

void vnodeOptionsCopy(SVnodeCfg *pDest, const SVnodeCfg *pSrc) {
  memcpy((void *)pDest, (void *)pSrc, sizeof(SVnodeCfg));
}

int vnodeValidateTableHash(SVnodeCfg *pVnodeOptions, char *tableFName) {
  uint32_t hashValue = 0;
  
  switch (pVnodeOptions->hashMethod) {
    default:
      hashValue = MurmurHash3_32(tableFName, strlen(tableFName));
      break;
  }

  // TODO OPEN THIS !!!!!!!
#if 0
  if (hashValue < pVnodeOptions->hashBegin || hashValue > pVnodeOptions->hashEnd) {
    terrno = TSDB_CODE_VND_HASH_MISMATCH;
    return TSDB_CODE_VND_HASH_MISMATCH;
  }
#endif

  return TSDB_CODE_SUCCESS;
}


