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
#include "cos.h"

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);

  if (taosInitCfg("/etc/taos/", NULL, NULL, NULL, NULL, 0) != 0) {
    printf("error");
  }
  if (s3Init() < 0) {
    return -1;
  }
  strcpy(tsSnodeAddress, "127.0.0.1");
  int ret = RUN_ALL_TESTS();
  s3CleanUp();
  return ret;
}

TEST(testCase, checkpointUpload_Test) {
  stopRsync();
  startRsync();

  taosSsleep(5);
  char* id = "2013892036";

  uploadCheckpoint(id, "/root/offset/");
}

TEST(testCase, checkpointDownload_Test) {
  char* id = "2013892036";
  downloadCheckpoint(id, "/root/offset/download/");
}

TEST(testCase, checkpointDelete_Test) {
  char* id = "2013892036";
  deleteCheckpoint(id);
}

TEST(testCase, checkpointDeleteFile_Test) {
  char* id = "2013892036";
  deleteCheckpointFile(id, "offset-ver0");
}
