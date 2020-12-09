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
#include "taos.h"
#include "tulog.h"
#include "tutil.h"
#include "hash.h"

#define MAX_RANDOM_POINTS 20000
#define GREEN "\033[1;32m"
#define NC "\033[0m"

int32_t capacity = 128;
int32_t q1Times = 10;
int32_t q2Times = 10;
int32_t keyNum = 100000;
int32_t printInterval = 1000;
void *  hashHandle;
pthread_t thread;

typedef struct HashTestRow {
  int32_t keySize;
  char    key[100];
} HashTestRow;

void shellParseArgument(int argc, char *argv[]);

void testHashPerformance() {
  int64_t    initialMs = taosGetTimestampMs();
  _hash_fn_t hashFp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  hashHandle = taosHashInit(128, hashFp, true, HASH_NO_LOCK);

  int64_t startMs = taosGetTimestampMs();
  float   seconds = (startMs - initialMs) / 1000.0;
  pPrint("initial time %.2f sec", seconds);

  for (int32_t t = 1; t <= keyNum; ++t) {
    HashTestRow row = {0};
    row.keySize = sprintf(row.key, "0.db.st%d", t);

    for (int32_t q = 0; q < q1Times; q++) {
      taosHashGet(hashHandle, row.key, row.keySize);
    }

    taosHashPut(hashHandle, row.key, row.keySize, &row, sizeof(HashTestRow));

    for (int32_t q = 0; q < q2Times; q++) {
      taosHashGet(hashHandle, row.key, row.keySize);
    }

    // test iterator
    {
      HashTestRow *row = taosHashIterate(hashHandle, NULL);
      while (row) {
        taosHashGet(hashHandle, row->key, row->keySize);
        row = taosHashIterate(hashHandle, row);
      }
    }

    if (t % printInterval == 0) {
      int64_t endMs = taosGetTimestampMs();
      int64_t hashSize = taosHashGetSize(hashHandle);
      float seconds = (endMs - startMs) / 1000.0;
      float speed = printInterval / seconds;
      pPrint("time:%.2f sec, speed:%.1f rows/second, hashSize:%ld", seconds, speed, hashSize);
      startMs = endMs;
    }
  }

  int64_t endMs = taosGetTimestampMs();
  int64_t hashSize = taosHashGetSize(hashHandle);
  seconds = (endMs - initialMs) / 1000.0;
  float speed = hashSize / seconds;

  pPrint("total time:%.2f sec, avg speed:%.1f rows/second, hashSize:%ld", seconds, speed, hashSize);
  taosHashCleanup(hashHandle);
}

void *multiThreadFunc(void *param) {
  for (int i = 0; i < 100; ++i) {
    taosMsleep(1000);
    HashTestRow *row = taosHashIterate(hashHandle, NULL);
    while (row) {
      taosHashGet(hashHandle, row->key, row->keySize);
      row = taosHashIterate(hashHandle, row);
    }
    int64_t hashSize = taosHashGetSize(hashHandle);
    pPrint("i:%d hashSize:%ld", i, hashSize);
  }

  return NULL;
}

void multiThreadTest() {
  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  
  // Start threads to write
  pthread_create(&thread, &thattr, multiThreadFunc, NULL);
}

int main(int argc, char *argv[]) {
  shellParseArgument(argc, argv);
  multiThreadTest();
  testHashPerformance();
  pthread_join(thread, NULL);
}

void printHelp() {
  char indent[10] = "        ";
  printf("Used to test the performance of cache\n");

  printf("%s%s\n", indent, "-k");
  printf("%s%s%s%d\n", indent, indent, "key num, default is ", keyNum);
  printf("%s%s\n", indent, "-p");
  printf("%s%s%s%d\n", indent, indent, "print interval while put into hash, default is ", printInterval);
  printf("%s%s\n", indent, "-c");
  printf("%s%s%s%d\n", indent, indent, "the initial capacity of hash ", capacity);
  printf("%s%s\n", indent, "-q1");
  printf("%s%s%s%d\n", indent, indent, "query times before put into hash", q1Times);
  printf("%s%s\n", indent, "-q2");
  printf("%s%s%s%d\n", indent, indent, "query times after put into hash", q2Times);
  
  exit(EXIT_SUCCESS);
}

void shellParseArgument(int argc, char *argv[]) {
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printHelp();
      exit(0);
    } else if (strcmp(argv[i], "-k") == 0) {
      keyNum = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-p") == 0) {
      printInterval = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-c") == 0) {
      capacity = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-q1") == 0) {
      q1Times = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-q2") == 0) {
      q2Times = atoi(argv[++i]);
    } else {
    }
  }

  pPrint("%s capacity:%d %s", GREEN, capacity, NC);
  pPrint("%s printInterval:%d %s", GREEN, printInterval, NC);
  pPrint("%s q1Times:%d %s", GREEN, q1Times, NC);
  pPrint("%s q2Times:%d %s", GREEN, q2Times, NC);
  pPrint("%s keyNum:%d %s", GREEN, keyNum, NC);
}
