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

#include "tssInt.h"


// mocked shared storage instance, for testing.
typedef struct {
    // [type] is inherited from SSharedStorage, it must be the first member
    // to allow casting to SSharedStorage.
    const SSharedStorageType* type;
} SSharedStorageMock;


// printConfig implements SSharedStorageType::printConfig.
static void printConfig(SSharedStorage* pss) {
    SSharedStorageMock* ss = (SSharedStorageMock*)pss;
    printf("type   : %s\n", ss->type->name);
}

// createInstance implements SSharedStorageType::createInstance.
// access string format:
//  mock:
static int32_t createInstance(const char* accessString, SSharedStorageMock** ppSS) {
    size_t asLen = strlen(accessString) + 1;

    SSharedStorageMock* ss = (SSharedStorageMock*)taosMemCalloc(1, sizeof(SSharedStorageMock) + asLen);
    if (!ss) {
        tssError("failed to allocate memory for SSharedStorageMock");
        TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }

    *ppSS = ss;
    TAOS_RETURN(TSDB_CODE_SUCCESS);
}



static int32_t closeInstance(SSharedStorage* pss) {
    SSharedStorageMock* ss = (SSharedStorageMock*)pss;
    taosMemFree(ss);
    return TSDB_CODE_FAILED;
}


// upload implements SSharedStorageType::upload.
static int32_t upload(SSharedStorage* pss, const char* dstPath, const void* data, int64_t size) {
    TAOS_RETURN(TSDB_CODE_SUCCESS);
}


// uploadFile implements SSharedStorageType::uploadFile.
static int32_t uploadFile(SSharedStorage* pss, const char* dstPath, const char* srcPath, int64_t offset, int64_t size) {
    return TSDB_CODE_SUCCESS;
}



// readFile implements SSharedStorageType::readFile.
static int32_t readFile(SSharedStorage* pss, const char* srcPath, int64_t offset, char* buffer, int64_t* size) {
    return TSDB_CODE_SUCCESS;
}



// downloadFile implements SSharedStorageType::downloadFile.
static int32_t downloadFile(SSharedStorage* pss, const char* srcPath, const char* dstPath, int64_t offset, int64_t size) {
    return TSDB_CODE_SUCCESS;
}



// listFile implements SSharedStorageType::listFile.
static int32_t listFile(SSharedStorage* pss, const char* prefix, SArray* paths) {
    return TSDB_CODE_SUCCESS;
}



// deleteFile implements SSharedStorageType::deleteFile.
static int32_t deleteFile(SSharedStorage* pss, const char* path) {
    return TSDB_CODE_SUCCESS;
}



// getFileSize implements SSharedStorageType::getFileSize.
static int32_t getFileSize(SSharedStorage* pss, const char* path, int64_t* size) {
    return TSDB_CODE_SUCCESS;
}



static int32_t mockCreateInstance(const char* as, SSharedStorage** ppSS);

static const SSharedStorageType sstMock = {
    .name = "mock",
    .printConfig = &printConfig,
    .createInstance = &mockCreateInstance,
    .closeInstance = &closeInstance,
    .upload = &upload,
    .uploadFile = &uploadFile,
    .readFile = &readFile,
    .downloadFile = &downloadFile,
    .listFile = &listFile,
    .deleteFile = &deleteFile,
    .getFileSize = &getFileSize,
};

static int32_t mockCreateInstance(const char* as, SSharedStorage** ppSS) {
    SSharedStorageMock* ss = NULL;

    int32_t code = createInstance(as, &ss);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }
    ss->type = &sstMock;

    *ppSS = (SSharedStorage*)ss;
    return TSDB_CODE_SUCCESS;
}



// register the mock type.
void mockRegisterType() {
    tssRegisterType(&sstMock);
}

