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

#include "tdbInt.h"

struct STEnv {
  char *   rootDir;
  int      jfd;
  SPCache *pCache;
};

int tdbEnvOpen(const char *rootDir, int pageSize, int cacheSize, STEnv **ppEnv) {
  STEnv *  pEnv;
  int      dsize;
  int      zsize;
  uint8_t *pPtr;

  *ppEnv = NULL;

  dsize = strlen(rootDir);
  zsize = sizeof(*pEnv) + dsize + 1;

  pPtr = (uint8_t *)calloc(1, zsize);
  if (pPtr == NULL) {
    return -1;
  }

  pEnv = (STEnv *)pPtr;
  pPtr += sizeof(*pEnv);
  pEnv->rootDir = pPtr;
  memcpy(pEnv->rootDir, rootDir, dsize);
  pEnv->rootDir[dsize] = '\0';

  pEnv->jfd = -1;

  tdbPCacheOpen(pageSize, cacheSize, 0, &(pEnv->pCache));

  *ppEnv = pEnv;
  return 0;
}

int tdbEnvClose(STEnv *pEnv) {
  // TODO
  return 0;
}