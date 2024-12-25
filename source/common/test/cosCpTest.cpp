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

#include <cos_cp.h>
#include <taoserror.h>
#include <tglobal.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

TEST(testCase, cpOpenCloseRemove) {
  int32_t code = 0, lino = 0;

  int64_t       contentLength = 1024;
  const int64_t MULTIPART_CHUNK_SIZE = 64 << 20;  // multipart is 768M
  uint64_t      chunk_size = MULTIPART_CHUNK_SIZE >> 3;
  int           totalSeq = (contentLength + chunk_size - 1) / chunk_size;
  const int     max_part_num = 10000;
  if (totalSeq > max_part_num) {
    chunk_size = (contentLength + max_part_num - contentLength % max_part_num) / max_part_num;
    totalSeq = (contentLength + chunk_size - 1) / chunk_size;
  }
  SCheckpoint cp;
  char const *file = "./afile";
  char        file_cp_path[TSDB_FILENAME_LEN];

  (void)snprintf(file_cp_path, TSDB_FILENAME_LEN, "%s.cp", file);

  cp.parts = (SCheckpointPart *)taosMemoryCalloc(max_part_num, sizeof(SCheckpointPart));
  if (!cp.parts) {
    TAOS_CHECK_EXIT(terrno);
  }

  EXPECT_EQ(cos_cp_open(file_cp_path, &cp), TSDB_CODE_SUCCESS);

  if (cp.thefile) {
    EXPECT_EQ(cos_cp_close(cp.thefile), TSDB_CODE_SUCCESS);
  }
  if (cp.parts) {
    taosMemoryFree(cp.parts);
  }

  EXPECT_EQ(cos_cp_remove(file_cp_path), TSDB_CODE_SUCCESS);

  return;

_exit:
  std::cout << "code: " << code << std::endl;
}

TEST(testCase, cpBuild) {
  int32_t code = 0, lino = 0;

  int64_t       contentLength = 1024;
  const int64_t MULTIPART_CHUNK_SIZE = 64 << 20;  // multipart is 768M
  uint64_t      chunk_size = MULTIPART_CHUNK_SIZE >> 3;
  int           totalSeq = (contentLength + chunk_size - 1) / chunk_size;
  const int     max_part_num = 10000;
  if (totalSeq > max_part_num) {
    chunk_size = (contentLength + max_part_num - contentLength % max_part_num) / max_part_num;
    totalSeq = (contentLength + chunk_size - 1) / chunk_size;
  }
  SCheckpoint cp;
  char const *file = "./afile";
  char        file_cp_path[TSDB_FILENAME_LEN];
  int64_t     lmtime = 20241220141705;
  char const *upload_id = "upload-id-xxx";

  (void)snprintf(file_cp_path, TSDB_FILENAME_LEN, "%s.cp", file);
  (void)memset(&cp, 0, sizeof(cp));

  cp.parts = (SCheckpointPart *)taosMemoryCalloc(max_part_num, sizeof(SCheckpointPart));
  if (!cp.parts) {
    TAOS_CHECK_EXIT(terrno);
  }

  EXPECT_EQ(cos_cp_open(file_cp_path, &cp), TSDB_CODE_SUCCESS);

  cos_cp_build_upload(&cp, file, contentLength, lmtime, upload_id, chunk_size);

  EXPECT_EQ(cos_cp_dump(&cp), TSDB_CODE_SUCCESS);

  if (cp.thefile) {
    EXPECT_EQ(cos_cp_close(cp.thefile), TSDB_CODE_SUCCESS);
  }
  if (cp.parts) {
    taosMemoryFree(cp.parts);
  }

  return;

_exit:
  std::cout << "code: " << code << std::endl;
}

TEST(testCase, cpLoad) {
  int32_t code = 0, lino = 0;

  int64_t       contentLength = 1024;
  const int64_t MULTIPART_CHUNK_SIZE = 64 << 20;  // multipart is 768M
  uint64_t      chunk_size = MULTIPART_CHUNK_SIZE >> 3;
  int           totalSeq = (contentLength + chunk_size - 1) / chunk_size;
  const int     max_part_num = 10000;
  if (totalSeq > max_part_num) {
    chunk_size = (contentLength + max_part_num - contentLength % max_part_num) / max_part_num;
    totalSeq = (contentLength + chunk_size - 1) / chunk_size;
  }
  SCheckpoint cp;
  char const *file = "./afile";
  char        file_cp_path[TSDB_FILENAME_LEN];
  int64_t     lmtime = 20241220141705;
  char const *upload_id = "upload-id-xxx";

  (void)snprintf(file_cp_path, TSDB_FILENAME_LEN, "%s.cp", file);
  (void)memset(&cp, 0, sizeof(cp));

  cp.parts = (SCheckpointPart *)taosMemoryCalloc(max_part_num, sizeof(SCheckpointPart));
  if (!cp.parts) {
    TAOS_CHECK_EXIT(terrno);
  }

  if (taosCheckExistFile(file_cp_path)) {
    EXPECT_EQ(cos_cp_load(file_cp_path, &cp), TSDB_CODE_SUCCESS);

    EXPECT_EQ(cos_cp_is_valid_upload(&cp, contentLength, lmtime), true);

    EXPECT_EQ(cp.cp_type, COS_CP_TYPE_UPLOAD);
    EXPECT_EQ(cp.md5, std::string(""));
    EXPECT_EQ(cp.thefile, nullptr);
    EXPECT_EQ(std::string(cp.file_path), "./afile");
    EXPECT_EQ(cp.file_size, 1024);
    EXPECT_EQ(cp.file_last_modified, 20241220141705);
    EXPECT_EQ(cp.file_md5, std::string(""));
    EXPECT_EQ(cp.object_name, std::string(""));
    EXPECT_EQ(cp.object_size, 0);
    EXPECT_EQ(cp.object_last_modified, std::string(""));
    EXPECT_EQ(cp.object_etag, std::string(""));
    EXPECT_EQ(cp.upload_id, std::string("upload-id-xxx"));

    EXPECT_EQ(cp.part_num, 1);
    EXPECT_EQ(cp.part_size, 8388608);
    EXPECT_EQ(cp.parts[0].index, 0);
    EXPECT_EQ(cp.parts[0].offset, 0);
    EXPECT_EQ(cp.parts[0].size, 1024);
    EXPECT_EQ(cp.parts[0].completed, 0);
    EXPECT_EQ(cp.parts[0].etag, std::string(""));
    EXPECT_EQ(cp.parts[0].crc64, 0);
  }

  if (cp.thefile) {
    EXPECT_EQ(cos_cp_close(cp.thefile), TSDB_CODE_SUCCESS);
  }
  if (cp.parts) {
    taosMemoryFree(cp.parts);
  }

  EXPECT_EQ(cos_cp_remove(file_cp_path), TSDB_CODE_SUCCESS);

  return;

_exit:
  std::cout << "code: " << code << std::endl;
}

TEST(testCase, cpBuildUpdate) {
  int32_t code = 0, lino = 0;

  int64_t       contentLength = 1024;
  const int64_t MULTIPART_CHUNK_SIZE = 64 << 20;  // multipart is 768M
  uint64_t      chunk_size = MULTIPART_CHUNK_SIZE >> 3;
  int           totalSeq = (contentLength + chunk_size - 1) / chunk_size;
  const int     max_part_num = 10000;
  if (totalSeq > max_part_num) {
    chunk_size = (contentLength + max_part_num - contentLength % max_part_num) / max_part_num;
    totalSeq = (contentLength + chunk_size - 1) / chunk_size;
  }
  SCheckpoint cp;
  char const *file = "./afile";
  char        file_cp_path[TSDB_FILENAME_LEN];
  int64_t     lmtime = 20241220141705;
  char const *upload_id = "upload-id-xxx";
  int         seq = 1;
  char       *etags[1] = {"etags-1-xxx"};

  (void)snprintf(file_cp_path, TSDB_FILENAME_LEN, "%s.cp", file);
  (void)memset(&cp, 0, sizeof(cp));

  cp.parts = (SCheckpointPart *)taosMemoryCalloc(max_part_num, sizeof(SCheckpointPart));
  if (!cp.parts) {
    TAOS_CHECK_EXIT(terrno);
  }

  EXPECT_EQ(cos_cp_open(file_cp_path, &cp), TSDB_CODE_SUCCESS);

  cos_cp_build_upload(&cp, file, contentLength, lmtime, upload_id, chunk_size);

  cos_cp_update(&cp, cp.parts[seq - 1].index, etags[seq - 1], 0);

  EXPECT_EQ(cos_cp_dump(&cp), TSDB_CODE_SUCCESS);

  if (cp.thefile) {
    EXPECT_EQ(cos_cp_close(cp.thefile), TSDB_CODE_SUCCESS);
  }
  if (cp.parts) {
    taosMemoryFree(cp.parts);
  }

  return;

_exit:
  std::cout << "code: " << code << std::endl;
}

TEST(testCase, cpLoadUpdate) {
  int32_t code = 0, lino = 0;

  int64_t       contentLength = 1024;
  const int64_t MULTIPART_CHUNK_SIZE = 64 << 20;  // multipart is 768M
  uint64_t      chunk_size = MULTIPART_CHUNK_SIZE >> 3;
  int           totalSeq = (contentLength + chunk_size - 1) / chunk_size;
  const int     max_part_num = 10000;
  if (totalSeq > max_part_num) {
    chunk_size = (contentLength + max_part_num - contentLength % max_part_num) / max_part_num;
    totalSeq = (contentLength + chunk_size - 1) / chunk_size;
  }
  SCheckpoint cp;
  char const *file = "./afile";
  char        file_cp_path[TSDB_FILENAME_LEN];
  int64_t     lmtime = 20241220141705;
  char const *upload_id = "upload-id-xxx";

  (void)snprintf(file_cp_path, TSDB_FILENAME_LEN, "%s.cp", file);
  (void)memset(&cp, 0, sizeof(cp));

  cp.parts = (SCheckpointPart *)taosMemoryCalloc(max_part_num, sizeof(SCheckpointPart));
  if (!cp.parts) {
    TAOS_CHECK_EXIT(terrno);
  }

  if (taosCheckExistFile(file_cp_path)) {
    EXPECT_EQ(cos_cp_load(file_cp_path, &cp), TSDB_CODE_SUCCESS);

    EXPECT_EQ(cos_cp_is_valid_upload(&cp, contentLength, lmtime), true);

    EXPECT_EQ(cp.cp_type, COS_CP_TYPE_UPLOAD);
    EXPECT_EQ(cp.md5, std::string(""));
    EXPECT_EQ(cp.thefile, nullptr);
    EXPECT_EQ(std::string(cp.file_path), "./afile");
    EXPECT_EQ(cp.file_size, 1024);
    EXPECT_EQ(cp.file_last_modified, 20241220141705);
    EXPECT_EQ(cp.file_md5, std::string(""));
    EXPECT_EQ(cp.object_name, std::string(""));
    EXPECT_EQ(cp.object_size, 0);
    EXPECT_EQ(cp.object_last_modified, std::string(""));
    EXPECT_EQ(cp.object_etag, std::string(""));
    EXPECT_EQ(cp.upload_id, std::string("upload-id-xxx"));

    EXPECT_EQ(cp.part_num, 1);
    EXPECT_EQ(cp.part_size, 8388608);
    EXPECT_EQ(cp.parts[0].index, 0);
    EXPECT_EQ(cp.parts[0].offset, 0);
    EXPECT_EQ(cp.parts[0].size, 1024);
    EXPECT_EQ(cp.parts[0].completed, 1);
    EXPECT_EQ(cp.parts[0].etag, std::string("etags-1-xxx"));
    EXPECT_EQ(cp.parts[0].crc64, 0);
  }

  if (cp.thefile) {
    EXPECT_EQ(cos_cp_close(cp.thefile), TSDB_CODE_SUCCESS);
  }
  if (cp.parts) {
    taosMemoryFree(cp.parts);
  }

  EXPECT_EQ(cos_cp_remove(file_cp_path), TSDB_CODE_SUCCESS);

  return;

_exit:
  std::cout << "code: " << code << std::endl;
}
