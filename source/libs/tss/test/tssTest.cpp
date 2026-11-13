/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include <stdio.h>

#include <cstring>
#include <iostream>
#include <queue>

#include "osDir.h"
#include "tglobal.h"
#include "tssInt.h"
#include "../../test/scopedTempDir.h"

class ScopedTssRuntime {
 public:
    ScopedTssRuntime() : code(tssInit()) {}
    ~ScopedTssRuntime() {
        if (code == TSDB_CODE_SUCCESS) EXPECT_EQ(tssUninit(), TSDB_CODE_SUCCESS);
    }
    ScopedTssRuntime(const ScopedTssRuntime&) = delete;
    ScopedTssRuntime& operator=(const ScopedTssRuntime&) = delete;
    const int32_t code;
};

class ScopedTssInstance {
 public:
    ~ScopedTssInstance() {
        if (value != nullptr) EXPECT_EQ(tssCloseInstance(value), TSDB_CODE_SUCCESS);
    }
    ScopedTssInstance() = default;
    ScopedTssInstance(const ScopedTssInstance&) = delete;
    ScopedTssInstance& operator=(const ScopedTssInstance&) = delete;
    SSharedStorage* value = nullptr;
};

class ScopedTssDefaultInstance {
 public:
    ScopedTssDefaultInstance() : code(tssCreateDefaultInstance()) {}
    ~ScopedTssDefaultInstance() {
        if (code == TSDB_CODE_SUCCESS) EXPECT_EQ(tssCloseDefaultInstance(), TSDB_CODE_SUCCESS);
    }
    ScopedTssDefaultInstance(const ScopedTssDefaultInstance&) = delete;
    ScopedTssDefaultInstance& operator=(const ScopedTssDefaultInstance&) = delete;
    const int32_t code;
};

// Builds a name that is unique for every test run. Multiple tssTest processes may
// run in parallel (e.g. in CI) and share the same remote storage, so each run must
// use its own remote paths; otherwise one process may delete the files another
// process is still using.
static void tssMakeUniqueName(char* buf, int32_t size, const char* prefix, const char* fileExt) {
    snprintf(buf, size, "%s-%d-%u%s", prefix, taosGetPId(), taosSafeRand(), fileExt == NULL ? "" : fileExt);
}

static void tssInstTest(SSharedStorage* ss, uint32_t largeFileSizeInMB, const char* localFile) {
    char name[64];
    tssMakeUniqueName(name, sizeof(name), "tss-test", "");

    char path[192];
    snprintf(path, sizeof(path), "%s/shared-storage-instance-test-file", name);

    const char* smallData = "small data of the shared storage instance test file.";

    const void* uploadData = smallData;
    char* data = NULL;
    int64_t dataSize = (int64_t)strlen(smallData);
    char* fileData = NULL;
    int64_t fileSize = 0;

    // print config
    tssPrintConfig(ss);

    // upload a small file
    int32_t code = tssUpload(ss, path, uploadData, dataSize);
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
        taosMemoryFree(p);
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
        taosMemoryFree(p);
    }
    taosArrayDestroy(paths);
    GTEST_ASSERT_FALSE(found);
    
    // upload/download file and delete file by prefix
    char prefix[192];
    snprintf(prefix, sizeof(prefix), "%s/test/", name);
    for(int i = 0; i < 10; i++) {
        char objPath[224];
        snprintf(objPath, sizeof(objPath), "%stest%d.txt", prefix, i);
        code = tssUploadFile(ss, objPath, localFile, 0, 0);
        GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    }

    char firstObjPath[224];
    snprintf(firstObjPath, sizeof(firstObjPath), "%stest0.txt", prefix);
    code = tssDownloadFile(ss, firstObjPath, localFile, 0, 0);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    code = tssDeleteFileByPrefix(ss, prefix);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
}


TEST(TssTest, BasicTest) {
    ScopedTestTempDir directory;
    ASSERT_EQ(directory.code, TSDB_CODE_SUCCESS);
    const std::string localFile = directory.child("ss_test.txt");
    const std::string accessString = "fs:baseDir=" + directory.child("ss");
    TdFilePtr f = taosOpenFile(localFile.c_str(), TD_FILE_CREATE | TD_FILE_WRITE);
    GTEST_ASSERT_NE(f, nullptr);
    int64_t written = taosWriteFile(f, "test data", 9);
    taosCloseFile(&f);
    GTEST_ASSERT_EQ(written, 9);

    ScopedTssRuntime runtime;
    GTEST_ASSERT_EQ(runtime.code, TSDB_CODE_SUCCESS);
    ScopedTssInstance invalidInstance;

    // an invalid access string
    const char* as = "notexist:a=10;b=11;c=12";
    int32_t code = tssCreateInstance(as, &invalidInstance.value);
    GTEST_ASSERT_EQ(code, TSDB_CODE_NOT_FOUND);

    printf("test file system based shared storage\n");
    as = accessString.c_str();
    {
        ScopedTssInstance instance;
        code = tssCreateInstance(as, &instance.value);
        GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
        ASSERT_NO_FATAL_FAILURE(tssInstTest(instance.value, 256, localFile.c_str()));
    }

#ifdef USE_S3
    printf("test libs3 based shared storage\n");
    as = "s3:endpoint=s3.tdengine.net:9000;bucket=test-bucket;uriStyle=path;protocol=http;accessKeyId=mkJO481qGlWR9hpkRweR;secretAccessKey=2KBJOZ2DXrvjAhIB1Qr64IGzUPxUl566GKWv4Sak;chunkSize=64;maxChunks=10000;maxRetry=5";
    {
        ScopedTssInstance instance;
        code = tssCreateInstance(as, &instance.value);
        GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
        ASSERT_NO_FATAL_FAILURE(tssInstTest(instance.value, 256, localFile.c_str()));
    }
#endif
}


TEST(TssTest, CloseTest) {
    void mockRegisterType();
    mockRegisterType();

    SSharedStorage* ss = NULL;
    int32_t code = tssCloseInstance(ss);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    code = tssCreateInstance("mock:", &ss);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    code = tssCloseInstance(ss);
    GTEST_ASSERT_EQ(code, TSDB_CODE_FAILED);
}



TEST(TssTest, DefaultInstanceTest) {
    ScopedTestTempDir directory;
    ASSERT_EQ(directory.code, TSDB_CODE_SUCCESS);
    const std::string localFile = directory.child("ss_test.txt");
    const std::string accessString = "fs:baseDir=" + directory.child("ss");
    TdFilePtr f = taosOpenFile(localFile.c_str(), TD_FILE_CREATE | TD_FILE_WRITE);
    GTEST_ASSERT_NE(f, nullptr);
    int64_t written = taosWriteFile(f, "test data", 9);
    taosCloseFile(&f);
    GTEST_ASSERT_EQ(written, 9);

    const std::string savedAccessString = tsSsAccessString;
    struct RestoreAccessString {
        const std::string& saved;
        ~RestoreAccessString() { strcpy(tsSsAccessString, saved.c_str()); }
    } restoreAccessString{savedAccessString};
    ASSERT_LT(accessString.size(), 1024U);
    strcpy(tsSsAccessString, accessString.c_str());

    ScopedTssRuntime runtime;
    GTEST_ASSERT_EQ(runtime.code, TSDB_CODE_SUCCESS);
    ScopedTssDefaultInstance instance;
    GTEST_ASSERT_EQ(instance.code, TSDB_CODE_SUCCESS);

    tssPrintDefaultConfig();

    char name[64];
    tssMakeUniqueName(name, sizeof(name), "tss-test", "");

    char path[192];
    snprintf(path, sizeof(path), "%s/shared-storage-instance-test-file", name);

    const char* smallData = "small data of the shared storage instance test file.";

    const void* uploadData = smallData;
    char* data = NULL;
    int64_t dataSize = (int64_t)strlen(smallData);
    char* fileData = NULL;
    int64_t fileSize = 0, largeFileSizeInMB = 256;

    // upload a small file
    int32_t code = tssUploadToDefault(path, uploadData, dataSize);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    // get the file size
    code = tssGetFileSizeOfDefault(path, &fileSize);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    GTEST_ASSERT_EQ(fileSize, dataSize);

    // list files, the test file should be found
    SArray* paths = taosArrayInit(10, sizeof(char*));
    code = tssListFileOfDefault(NULL, paths);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    bool found = false;
    for(int i = 0; i < taosArrayGetSize(paths); i++) {
        char* p = *(char**)taosArrayGet(paths, i);
        if (strcmp(p, path) == 0) {
            found = true;
        }
        taosMemoryFree(p);
    }
    taosArrayDestroy(paths);
    GTEST_ASSERT_TRUE(found);

    // read the small file
    fileSize = dataSize * 2;
    data = (char*)taosMemoryCalloc(1, fileSize);
    code = tssReadFileFromDefault(path, 0, data, &fileSize);
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
    code = tssUploadToDefault(path, data, dataSize);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    // get the size of the large test file
    code = tssGetFileSizeOfDefault(path, &fileSize);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    GTEST_ASSERT_EQ(fileSize, dataSize);

    // read a block of the large test file
    dataSize = 1024 * 1024;
    code = tssReadFileFromDefault(path, 1024 * 1024, data, &dataSize);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    GTEST_ASSERT_EQ(dataSize, 1024 * 1024);
    for(uint64_t i = 0; i < dataSize; i++) {
        GTEST_ASSERT_EQ(data[i], 'b');
    }
    taosMemFree(data);

    // delete the test file
    code = tssDeleteFileFromDefault(path);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    // list files again, the test file should not be found
    paths = taosArrayInit(10, sizeof(char*));
    code = tssListFileOfDefault(NULL, paths);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    found = false;
    for(int i = 0; i < taosArrayGetSize(paths); i++) {
        char* p = *(char**)taosArrayGet(paths, i);
        if (strcmp(p, path) == 0) {
            found = true;
        }
        taosMemoryFree(p);
    }
    taosArrayDestroy(paths);
    GTEST_ASSERT_FALSE(found);
    
    // upload/download file and delete file by prefix
    char prefix[192];
    snprintf(prefix, sizeof(prefix), "%s/test/", name);
    for(int i = 0; i < 10; i++) {
        char objPath[224];
        snprintf(objPath, sizeof(objPath), "%stest%d.txt", prefix, i);
        code = tssUploadFileToDefault(objPath, localFile.c_str(), 0, 0);
        GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    }

    char firstObjPath[224];
    snprintf(firstObjPath, sizeof(firstObjPath), "%stest0.txt", prefix);
    code = tssDownloadFileFromDefault(firstObjPath, localFile.c_str(), 0, 0);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    code = tssDeleteFileByPrefixFromDefault(prefix);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    code = tssCheckDefaultInstance(0);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    code = tssCheckDefaultInstance(128);
    GTEST_ASSERT_EQ(code, TSDB_CODE_SUCCESS);

}
