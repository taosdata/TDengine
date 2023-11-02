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

#include <taoserror.h>
#include <tglobal.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include "rsync.h"
#include "streamInt.h"

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);

  strcpy(tsSnodeIp, "127.0.0.1");
  return RUN_ALL_TESTS();
}

TEST(testCase, checkpointUpload_Test) {
  stopRsync();
  startRsync();

  taosSsleep(5);
  SArray* fileList = taosArrayInit(0, POINTER_BYTES);
  char* id = "2013892036";
  char* file1 = "/Users/mingmingwanng/wal1";
  char* file2 = "/Users/mingmingwanng/java_error_in_clion.hprof";
  taosArrayPush(fileList, &file1);
  taosArrayPush(fileList, &file2);

  uploadCheckpoint(id, fileList);
}

TEST(testCase, checkpointDownload_Test) {
  char* id = "2013892036";
  downloadRsync(id, "/Users/mingmingwanng/rsync/tmp");
}

TEST(testCase, checkpointDelete_Test) {
  char* id = "2013892036";
  deleteRsync(id);
}
