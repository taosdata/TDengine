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

#include <cstring>
#include <iostream>
#include <queue>

#include "tssInt.h"

static void tssInstTest(SSharedStorage* ss, uint32_t largeFileSizeInMB) {
    const char* path = "shared-storage-instance-test-file";
    char* smallData = "small data of the shared storage instance test file.";

    char* data = smallData;
    int64_t dataSize = (int64_t)strlen(data);
    char* fileData = NULL;
    int64_t fileSize = 0;

    // print config
    tssPrintConfig(ss);

    // upload a small file
    int32_t code = tssUpload(ss, path, data, dataSize);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    // get the file size
    code = tssGetFileSize(ss, path, &fileSize);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    GTEST_ASSERT_EQ(fileSize, dataSize);

    // list files, the test file should be found
    SArray* paths = taosArrayInit(10, sizeof(char*));
    code = tssListFile(ss, NULL, paths);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    bool found = false;
    for(int i = 0; i < taosArrayGetSize(paths); i++) {
        char* p = *(char**)taosArrayGet(paths, i);
        if (strcmp(p, path) == 0) {
            found = true;
        }
        taosMemFree(p);
    }
    taosArrayDestroy(paths);
    GTEST_ASSERT_TRUE(found);

    // read the small file
    fileSize = dataSize * 2;
    data = (char*)taosMemoryCalloc(1, fileSize);
    code = tssReadFile(ss, path, 0, data, &fileSize);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    GTEST_ASSERT_EQ(fileSize, dataSize);

    GTEST_ASSERT_EQ(memcmp(data, smallData, dataSize), 0);
    taosMemFree(data);

    // large file test, prepare data
    dataSize = largeFileSizeInMB * (uint64_t)1024 * 1024;
    data = (char*)taosMemoryMalloc(dataSize);
    GTEST_ASSERT_TRUE(data != NULL);
    for (uint64_t i = 0; i < largeFileSizeInMB; i++) {
        memset(data + i * 1024 * 1024, 'a' + i % 26, 1024 * 1024);
    }

    // upload the large test file, this will overwrite the previous small test file
    code = tssUpload(ss, path, data, dataSize);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    // get the size of the large test file
    code = tssGetFileSize(ss, path, &fileSize);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    GTEST_ASSERT_EQ(fileSize, dataSize);

    // read a block of the large test file
    dataSize = 1024 * 1024;
    code = tssReadFile(ss, path, 1024 * 1024, data, &dataSize);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    GTEST_ASSERT_EQ(dataSize, 1024 * 1024);
    for(uint64_t i = 0; i < dataSize; i++) {
        GTEST_ASSERT_EQ(data[i], 'b');
    }
    taosMemFree(data);

    // delete the test file
    code = tssDeleteFile(ss, path);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    // list files again, the test file should not be found
    paths = taosArrayInit(10, sizeof(char*));
    code = tssListFile(ss, NULL, paths);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    found = false;
    for(int i = 0; i < taosArrayGetSize(paths); i++) {
        char* p = *(char**)taosArrayGet(paths, i);
        if (strcmp(p, path) == 0) {
            found = true;
        }
        taosMemFree(p);
    }
    taosArrayDestroy(paths);
    GTEST_ASSERT_FALSE(found);
}


TEST(TssTest, BasicTest) {
    SSharedStorage* ss = NULL;

    int32_t code = tssInit();
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    // an invalid access string
    const char* as = "notexist:a=10;b=11;c=12";
    code = tssCreateInstance(as, &ss);
    GTEST_ASSERT_EQ(code, TSDB_CODE_NOT_FOUND);

#if 0
    printf("test file system based shared storage\n");
    as = "fs:baseDir=/tmp/ss";
    code = tssCreateInstance(as, &ss);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    tssInstTest(ss, 512);
    tssCloseInstance(ss);
#endif

#ifdef USE_S3
    printf("test libs3 based shared storage\n");
    as = "s3:endpoint=192.168.1.52:9000;bucket=test-bucket;uriStyle=path;protocol=http;accessKeyId=fGPPyYjzytw05nw44ViA;secretAccessKey=vK1VcwxgSOykicx6hk8fL1x15uEtyDSFU3w4hTaZ;chunkSize=64;maxChunks=10000;maxRetry=5";
    code = tssCreateInstance(as, &ss);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    tssInstTest(ss, 512);
    tssCloseInstance(ss);
#endif

    tssUninit();
}