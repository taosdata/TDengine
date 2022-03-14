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
#define ALLOW_FORBID_FUNC
#define _DEFAULT_SOURCE
#include "os.h"
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#else
#include <sys/file.h>
#include <unistd.h>
#endif

void taosSeedRand(uint32_t seed) { return srand(seed); }

uint32_t taosRand(void) { return rand(); }

uint32_t taosRandR(uint32_t *pSeed) { return rand_r(pSeed); }

uint32_t taosSafeRand(void) {
  TdFilePtr pFile;
  int seed;

  pFile = taosOpenFile("/dev/urandom", TD_FILE_READ);
  if (pFile == NULL) {
    seed = (int)taosGetTimestampSec();
  } else {
    int len = taosReadFile(pFile, &seed, sizeof(seed));
    if (len < 0) {
      seed = (int)taosGetTimestampSec();
    }
    taosCloseFile(&pFile);
  }

  return (uint32_t)seed;
}

void taosRandStr(char* str, int32_t size) {
  const char* set = "abcdefghijklmnopqrstuvwxyz0123456789-_.";
  int32_t     len = 39;

  for (int32_t i = 0; i < size; ++i) {
    str[i] = set[taosRand() % len];
  }
}