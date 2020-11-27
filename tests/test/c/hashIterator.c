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

typedef struct HashTestRow {
  int32_t keySize;
  char    key[100];
} HashTestRow;

int main(int argc, char *argv[]) {
  _hash_fn_t hashFp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  void *     hashHandle = taosHashInit(100, hashFp, true, HASH_ENTRY_LOCK);

  pPrint("insert 3 rows to hash");
  for (int32_t t = 0; t < 3; ++t) {
    HashTestRow row = {0};
    row.keySize = sprintf(row.key, "0.db.st%d", t);

    taosHashPut(hashHandle, row.key, row.keySize, &row, sizeof(HashTestRow));
  }

  pPrint("start iterator");
  HashTestRow *row = taosHashIterate(hashHandle, NULL);
  while (row) {
    pPrint("drop key:%s", row->key);
    taosHashRemove(hashHandle, row->key, row->keySize);

    pPrint("get rows from hash");
    for (int32_t t = 0; t < 3; ++t) {
      HashTestRow r = {0};
      r.keySize = sprintf(r.key, "0.db.st%d", t);

      void *result = taosHashGet(hashHandle, r.key, r.keySize);
      pPrint("get key:%s result:%p", r.key, result);
    }

    //Before getting the next iterator, the object just deleted can be obtained
    row = taosHashIterate(hashHandle, row);
  }

  pPrint("stop iterator");
  taosHashCancelIterate(hashHandle, row);

  pPrint("get rows from hash");
  for (int32_t t = 0; t < 3; ++t) {
    HashTestRow r = {0};
    r.keySize = sprintf(r.key, "0.db.st%d", t);

    void *result = taosHashGet(hashHandle, r.key, r.keySize);
    pPrint("get key:%s result:%p", r.key, result);
  }

  return 0;
}