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
#include <malloc.h>
#include "os.h"
#include "taos.h"
#include "tcache.h"
#include "tulog.h"
#include "tutil.h"

#define MAX_REFRESH_TIME_SEC 2
#define MAX_RANDOM_POINTS 20000
#define GREEN "\033[1;32m"
#define NC "\033[0m"

int32_t tsKeepTimeInSec = 3;
int32_t tsNumOfRows = 1000000;
int32_t tsSizeOfRow = 64 * 1024;
void *  tsCacheObj = NULL;
int32_t destroyTimes = 0;

typedef int64_t CacheTestKey;
typedef struct CacheTestRow {
  int32_t index;
  void ** ppRow;
  void *  data;
} CacheTestRow;

CacheTestRow *initRow(int32_t index) {
  CacheTestRow *row = calloc(sizeof(CacheTestRow), 1);
  row->index = index;
  row->data = malloc(tsSizeOfRow * sizeof(int8_t));
  return row;
}

void detroyRow(void *data) {
  CacheTestRow *row = *(CacheTestRow **)data;
  free(row->data);
  free(row);
  destroyTimes++;
  if (destroyTimes % 50000 == 0) {
    pPrint("%s ===> destroyTimes:%d %s", GREEN, destroyTimes, NC);
  }
}

void initCache() {
  tsCacheObj = taosCacheInit(TSDB_DATA_TYPE_BIGINT, MAX_REFRESH_TIME_SEC, true, detroyRow, "cachetest");
}

void putRowInCache() {
  for (int index = 0; index < tsNumOfRows; ++index) {
    CacheTestRow *row = initRow(index);
    uint64_t      key = (uint64_t)row;
    void **ppRow = taosCachePut(tsCacheObj, &key, sizeof(int64_t), &row, sizeof(int64_t), tsKeepTimeInSec * 1000);
    row->ppRow = ppRow;
    taosCacheRelease(tsCacheObj, (void **)&ppRow, false);
  }
}

void cleanupCache() {
  taosCacheCleanup(tsCacheObj);
}

void initGetMemory() {
  osInit();
  taos_init();
}

float getProcMemory() {
  float procMemoryUsedMB = 0;
  taosGetProcMemory(&procMemoryUsedMB);
  return procMemoryUsedMB;
}

void doTest() {
  initCache();
  pPrint("%s initialize procMemory %f MB %s", GREEN, getProcMemory(), NC);

  putRowInCache();
  pPrint("%s insert %d rows, procMemory %f MB %s", GREEN, tsNumOfRows, getProcMemory(), NC);

  int32_t sleepMs = (MAX_REFRESH_TIME_SEC * 3) * 1000 + tsKeepTimeInSec * 1000;
  taosMsleep(sleepMs);
  pPrint("%s after sleep %d ms, procMemory %f MB %s", GREEN, sleepMs, getProcMemory(), NC);

  cleanupCache();
  taosMsleep(sleepMs);
  pPrint("%s after cleanup cache, procMemory %f MB %s", GREEN, getProcMemory(), NC);
  
  malloc_trim(0);
  taosMsleep(sleepMs);
  pPrint("%s after malloc_trim, procMemory %f MB %s", GREEN, getProcMemory(), NC);
}

void printHelp() {
  char indent[10] = "        ";
  printf("Used to test the performance of cache\n");

  printf("%s%s\n", indent, "-k");
  printf("%s%s%s%d\n", indent, indent, "KeepTimeInSec, default is ", tsKeepTimeInSec);
  printf("%s%s\n", indent, "-n");
  printf("%s%s%s%d\n", indent, indent, "NumOfRows, default is ", tsNumOfRows);
  printf("%s%s\n", indent, "-s");
  printf("%s%s%s%d\n", indent, indent, "SizeOfData, default is ", tsSizeOfRow);

  exit(EXIT_SUCCESS);
}

void parseArgument(int argc, char *argv[]) {
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printHelp();
      exit(0);
    } else if (strcmp(argv[i], "-k") == 0) {
      tsKeepTimeInSec = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-n") == 0) {
      tsNumOfRows = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-s") == 0) {
      tsSizeOfRow = atoi(argv[++i]);
    } else {
    }
  }

  pPrint("%s KeepTimeInSec:%d %s", GREEN, tsKeepTimeInSec, NC);
  pPrint("%s NumOfRows:%d %s", GREEN, tsNumOfRows, NC);
  pPrint("%s SizeOfData:%d %s", GREEN, tsSizeOfRow, NC);
}

int main(int argc, char *argv[]) {
  initGetMemory();
  parseArgument(argc, argv);
  doTest();
}
