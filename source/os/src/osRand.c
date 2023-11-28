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
#ifdef WINDOWS
#include "wincrypt.h"
#include "windows.h"
#else
#include <sys/file.h>
#include <unistd.h>
#endif

void taosSeedRand(uint32_t seed) { return srand(seed); }

uint32_t taosRand(void) {
#ifdef WINDOWS
  unsigned int pSeed;
  rand_s(&pSeed);
  return pSeed;
#else
  return rand();
#endif
}

uint32_t taosRandR(uint32_t* pSeed) {
#ifdef WINDOWS
  return rand_s(pSeed);
#else
  return rand_r(pSeed);
#endif
}

uint32_t taosSafeRand(void) {
#ifdef WINDOWS
  uint32_t   seed = taosRand();
  HCRYPTPROV hCryptProv;
  if (!CryptAcquireContext(&hCryptProv, NULL, NULL, PROV_RSA_FULL, 0)) {
    if (!CryptAcquireContext(&hCryptProv, NULL, NULL, PROV_RSA_FULL, CRYPT_NEWKEYSET)) {
      return seed;
    }
  }
  if (hCryptProv != NULL) {
    if (!CryptGenRandom(hCryptProv, 4, &seed)) return seed;
  }
  if (hCryptProv != NULL) CryptReleaseContext(hCryptProv, 0);
  return seed;
#else
  TdFilePtr pFile;
  int       seed;

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
#endif
}

void taosRandStr(char* str, int32_t size) {
  const char* set = "abcdefghijklmnopqrstuvwxyz0123456789-_.";
  int32_t     len = 39;

  for (int32_t i = 0; i < size; ++i) {
    str[i] = set[taosRand() % len];
  }
}

void taosRandStr2(char* str, int32_t size) {
  const char* set = "abcdefghijklmnopqrstuvwxyz0123456789@";
  int32_t     len = strlen(set);

  for (int32_t i = 0; i < size; ++i) {
    str[i] = set[taosRand() % len];
  }
}