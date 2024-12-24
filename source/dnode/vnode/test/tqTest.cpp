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

#include <gtest/gtest.h>
#include <vnodeInt.h>

#include <taoserror.h>
#include <tglobal.h>
#include <iostream>

#include <tmsg.h>
#include <vnodeInt.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

SDmNotifyHandle dmNotifyHdl = {.state = 0};

#include "tq.h"
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

void tqWriteOffset() {
  TdFilePtr pFile = taosOpenFile(TQ_OFFSET_NAME, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);

  STqOffset offset = {.val = {.type = TMQ_OFFSET__LOG, .version = 8923}};
  strcpy(offset.subKey, "testtest");
  int32_t    bodyLen;
  int32_t    code;
  tEncodeSize(tEncodeSTqOffset, &offset, bodyLen, code);
  int32_t totLen = INT_BYTES + bodyLen;
  void*   buf = taosMemoryCalloc(1, totLen);
  void*   abuf = POINTER_SHIFT(buf, INT_BYTES);

  *(int32_t*)buf = htonl(bodyLen);
  SEncoder encoder;
  tEncoderInit(&encoder, (uint8_t*)abuf, bodyLen);
  tEncodeSTqOffset(&encoder, &offset);
  taosWriteFile(pFile, buf, totLen);

  taosMemoryFree(buf);

  taosCloseFile(&pFile);
}

TEST(testCase, tqOffsetTest) {
  STQ* pTq = (STQ*)taosMemoryCalloc(1, sizeof(STQ));
  pTq->path = taosStrdup("./");

  pTq->pOffset = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_ENTRY_LOCK);
  taosHashSetFreeFp(pTq->pOffset, (FDelete)tDeleteSTqOffset);

  tdbOpen(pTq->path, 16 * 1024, 1, &pTq->pMetaDB, 0, 0, NULL);
  tdbTbOpen("tq.offset.db", -1, -1, NULL, pTq->pMetaDB, &pTq->pOffsetStore, 0);

  tqWriteOffset();
  tqOffsetRestoreFromFile(pTq, TQ_OFFSET_NAME);
  taosRemoveFile(TQ_OFFSET_NAME);
  tqClose(pTq);
}

#pragma GCC diagnostic pop
