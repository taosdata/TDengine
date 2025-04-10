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

#ifdef __cplusplus
extern "C" {
#endif

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
     // the name of the shared storage type
    const char* name;

    // createInstance creates a shared storage instance
    int32_t (*createInstance)(const char* accessString, SSharedStorage** ppSS);

    // closeInstance closes a shared storage instance.
    int32_t (*closeInstance)(SSharedStorage* ss);

    // list lists files with path prefix 'prefix' in the shared storage.
    int32_t (*list)(SSharedStorage* ss, const char* prefix, char* paths[], int* npaths);    

    // upload uploads a block of a file (from 'offset' to 'offset + size' when 'size >= 0',
    // or from 'offset' to the end of the source file when 'size == -1') onto the shared storage.
    // The uploaded block is saved as a standalone file in the shared storage at dstPath.
    int32_t (*upload)(SSharedStorage* ss, const char* dstPath, const char* srcPath, int64_t offset, int64_t size);

    // download downloads a block of a file (from 'offset' to 'offset + size' when 'size >= 0',
    // or from 'offset' to the end of the source file when 'size == -1') from the shared storage.
    // The downloaded file is saved as a standalone file in the local file system at dstPath.
    int32_t (*download)(SSharedStorage* ss, const char* srcPath, const char* dstPath, int64_t offset, int64_t size);

    // delete deletes files from the shared storage.
    int32_t (*delete)(SSharedStorage* ss, const char* paths[], int npaths);
};


// tssRegisterType registers a shared storage type with the given name and
// initialization function.
void tssRegisterType(const SSharedStorageType* t);

// tssInit initializes the tss module.
int32_t tssInit();

// tssUninit uninitializes the tss module.
int32_t tssUninit();


// tssCreateInstance creates a shared storage instance according to the access string.
int32_t tssCreateInstance(const char* accessString, SSharedStorage** ppSS);

// tssCloseInstance uninitializes a shared storage instance.
int32_t tssCloseInstance(SSharedStorage* ss);


int32_t tssUploadFile(SSharedStorage* ss, const char* srcPath, const char* dstpath);
int32_t tssUploadBlock(SSharedStorage* ss, const char* dstPath, const char* srcPath , int64_t offset, int64_t size);




int32_t tssCreateDefaultInstance();
int32_t tssCloseDefaultInstance();


#ifdef __cplusplus
}
#endif

#endif
