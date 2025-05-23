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

#ifndef _TD_TSS_H_
#define _TD_TSS_H_

#include "os.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

// TODO: rename these variables?
extern int8_t tsS3Enabled;
extern int8_t tsS3EnabledCfg;
extern int32_t tsS3UploadDelaySec;
extern int32_t tsS3BlockSize;
extern int32_t tsS3BlockCacheSize;
extern int32_t tsS3PageCacheSize;
extern int8_t tsS3StreamEnabled;


// forward declaration of SSharedStorage & SSharedStorageType
struct SSharedStorage;
typedef struct SSharedStorage SSharedStorage;

struct SSharedStorageType;
typedef struct SSharedStorageType SSharedStorageType;


// SSharedStorage is the base structure of a shared storage. An concrete shared
// storage type should inherit from this structure and add its own type-specific
// data.
struct SSharedStorage {
    SSharedStorageType* type; // the type of the shared storage

    // type-specific data
};


// SSharedStorageType is defines the interface of a shared storage.
struct SSharedStorageType {
     // the name of the shared storage type.
    const char* name;

    // printConfig prints the configuration of the shared storage instance.
    void (*printConfig)(SSharedStorage* ss);

    // createInstance creates a shared storage instance according to the access string.
    int32_t (*createInstance)(const char* accessString, SSharedStorage** pp);

    // closeInstance closes a shared storage instance.
    int32_t (*closeInstance)(SSharedStorage* ss);

    // upload uploads a block of data to the shared storage at the specified path.
    // It uploads an empty file if [size] is 0.
    int32_t (*upload)(SSharedStorage* ss, const char* dstPath, const void* data, int64_t size);

    // uploadFile uploads a file to the shared storage at the specified path.
    // [offset] is the start offset of the source file, [size] is the size of the data to be uploaded.
    // If [size] is negative, upload until the end of the source file. If [size] is 0, upload an empty file.
    int32_t (*uploadFile)(SSharedStorage* ss, const char* dstPath, const char* srcPath, int64_t offset, int64_t size);

    // readFile reads a file or a block of a file from the shared storage to [buffer].
    // read starts at [offset] and reads [*size] bytes.
    // [*size] is updated to the number of bytes read when the function returns.
    int32_t (*readFile)(SSharedStorage* ss, const char* srcPath, int64_t offset, char* buffer, int64_t* size);

    // downloadFile downloads a file or a block of a file from the shared storage to the local file system.
    // download starts at [offset] and downloads [size] bytes.
    // If [size] is zero, download until the end of the source file.
    int32_t (*downloadFile)(SSharedStorage* ss, const char* srcPath, const char* dstPath, int64_t offset, int64_t size);

    // listFile lists the files in the shared storage.
    // [prefix] is the prefix of the files to be listed, and should be a valid path.
    // [paths] is a pointer to a new initialized SArray, which will be filled with the paths.
    // If the call succeeds, the caller is responsible for freeing the items in [paths]
    // and [paths] itself.
    // if the call fails, [paths] will be cleared and the function returns an error code,
    // the caller is responsible for freeing [paths] itself.
    int32_t (*listFile)(SSharedStorage* ss, const char* prefix, struct SArray* paths);

    // deleteFile deletes a file from the shared storage.
    int32_t (*deleteFile)(SSharedStorage* ss, const char* path);

    // getFileSize gets the size of a file in the shared storage.
    int32_t (*getFileSize)(SSharedStorage* ss, const char* path, int64_t* size);
};


// tssRegisterType registers a shared storage type.
void tssRegisterType(const SSharedStorageType* t);

// tssInit initializes the tss module.
int32_t tssInit();

// tssUninit uninitializes the tss module.
int32_t tssUninit();


// these functions wrap the functions in SSharedStorageType, please refer to the
// comments in SSharedStorageType for details.
void tssPrintConfig(SSharedStorage* ss);
int32_t tssCreateInstance(const char* accessString, SSharedStorage** pp);
int32_t tssCloseInstance(SSharedStorage* ss);
int32_t tssUpload(SSharedStorage* ss, const char* dstPath, const void* data, int64_t size);
int32_t tssUploadFile(SSharedStorage* ss, const char* dstPath, const char* srcPath, int64_t offset, int64_t size);
int32_t tssReadFile(SSharedStorage* ss, const char* srcPath, int64_t offset, char* buffer, int64_t* size);
int32_t tssDownloadFile(SSharedStorage* ss, const char* srcPath, const char* dstPath, int64_t offset, int64_t size);
int32_t tssListFile(SSharedStorage* ss, const char* prefix, struct SArray* paths);
int32_t tssDeleteFile(SSharedStorage* ss, const char* path);
int32_t tssDeleteFileByPrefix(SSharedStorage* ss, const char* prefix);
int32_t tssGetFileSize(SSharedStorage* ss, const char* path, int64_t* size);


// these functions wrap the functions in SSharedStorageType for the default instance.
void tssPrintDefaultConfig();
// tssCreateDefaultInstance creates the default shared storage instance using
// [tsSsAccessString] as the access string.
int32_t tssCreateDefaultInstance();
int32_t tssCloseDefaultInstance();
int32_t tssUploadToDefault(const char* dstPath, const void* data, int64_t size);
int32_t tssUploadFileToDefault(const char* dstPath, const char* srcPath, int64_t offset, int64_t size);
int32_t tssReadFileFromDefault(const char* srcPath, int64_t offset, char* buffer, int64_t* size);
int32_t tssDownloadFileFromDefault(const char* srcPath, const char* dstPath, int64_t offset, int64_t size);
int32_t tssListFileOfDefault(const char* prefix, struct SArray* paths);
int32_t tssDeleteFileFromDefault(const char* path);
int32_t tssDeleteFileByPrefixFromDefault(const char* prefix);
int32_t tssGetFileSizeOfDefault(const char* path, int64_t* size);


// tssCheckInstance checks the configuration of the shared storage instance by
// uploading, listing, downloading, and deleting a test file.
// if [largeFileSizeInMB] is greater than 0, a large test file of the specified size
// will be checked.
int32_t tssCheckInstance(SSharedStorage* ss, uint32_t largeFileSizeInMB);
int32_t tssCheckDefaultInstance(uint32_t largeFileSizeInMB);


#ifdef __cplusplus
}
#endif

#endif
