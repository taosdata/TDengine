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


// the registry of shared storage types
static const SSharedStorageType* g_registry[8] = {0};


// the default shared storage
static SSharedStorage* g_default = NULL;



// tssRegisterType registers a shared storage type with the given name and
// initialization function.
void tssRegisterType(const SSharedStorageType* t) {
    for (int i = 0; i < countof(g_registry); ++i) {
        const SSharedStorageType* type = g_registry[i];
        if (type != NULL && strcmp(type->name, t->name) == 0) {
            tssFatal("shared storage type '%s' already registered\n", t->name);
            return;
        }
    }

    for (int i = 0; i < countof(g_registry); ++i) {
        if (g_registry[i] == NULL) {
            g_registry[i] = (SSharedStorageType*)t;
            return;
        }
    }

    tssFatal("no space left in the registry for shared storage type '%s'\n", t->name);
}



int32_t tssInit() {
    void s3RegisterType();

    s3RegisterType();
    return TSDB_CODE_SUCCESS;
}



int32_t tssUninit() {
    return TSDB_CODE_SUCCESS;
}



int32_t tssCreateInstance(const char* as, SSharedStorage** pp) {
    size_t asLen = strlen(as);

    for (int i = 0; i < countof(g_registry); ++i) {
        const SSharedStorageType* t = g_registry[i];
        if (t == NULL) {
            continue;
        }

        const char* name = t->name;
        size_t nameLen = strlen(t->name);
        if (asLen <= nameLen || strncmp(as, name, nameLen) != 0 || as[nameLen] != ':') {
            continue;
        }

        return t->createInstance(as, pp);
    }

    tssError("shared storage type not found, access string is: %s\n", as);
    return TSDB_CODE_NOT_FOUND;
}


int32_t tssCloseInstance(SSharedStorage* ss) {
    if (ss == NULL) {
        return TSDB_CODE_SUCCESS;
    }

    int32_t code = ss->type->closeInstance(ss);
    if (code != TSDB_CODE_SUCCESS) {
        tssError("failed to uninitialize shared storage, code: %d\n", code);
    }
    return code;
}


int32_t tssUpload(SSharedStorage* ss, const char* dstPath, const void* data, int64_t size) {
    return ss->type->upload(ss, dstPath, data, size);
}


int32_t tssUploadFile(SSharedStorage* ss, const char* dstPath, const char* srcPath, int64_t offset, int64_t size) {
    return ss->type->uploadFile(ss, dstPath, srcPath, offset, size);
}


int32_t tssReadFile(SSharedStorage* ss, const char* srcPath, int64_t offset, char* buffer, int64_t* size) {
    return ss->type->readFile(ss, srcPath, offset, buffer, size);
}


int32_t tssDownloadFile(SSharedStorage* ss, const char* srcPath, const char* dstPath, int64_t offset, int64_t size) {
    return ss->type->downloadFile(ss, srcPath, dstPath, offset, size);
}


int32_t tssListFile(SSharedStorage* ss, const char* prefix, struct SArray* paths) {
    return ss->type->listFile(ss, prefix, paths);
}


int32_t tssDeleteFile(SSharedStorage* ss, const char* path) {
    return ss->type->deleteFile(ss, path);
}


int32_t tssGetFileSize(SSharedStorage* ss, const char* path, int64_t* size) {
    return ss->type->getFileSize(ss, path, size);
}



int32_t tssCreateDefaultInstance() {
    extern char tsSsAccessString[];

    if (strlen(tsSsAccessString) == 0) {
        tssInfo("access string is empty, default shared storage is disabled\n");
        return 0;
    }

    if (g_default != NULL) {
        tssError("default shared storage already initialized\n");
        return 0; // already initialized
    }

    return tssCreateInstance(tsSsAccessString, &g_default);
}


int32_t tssCloseDefaultInstance() {
    int32_t code = tssCloseInstance(g_default);
    g_default = NULL;
    return code;
}


int32_t tssUploadToDefault(const char* dstPath, const void* data, int64_t size) {
    return g_default->type->upload(g_default, dstPath, data, size);
}


int32_t tssUploadFileToDefault(const char* dstPath, const char* srcPath, int64_t offset, int64_t size) {
    return g_default->type->uploadFile(g_default, dstPath, srcPath, offset, size);
}


int32_t tssReadFileFromDefault(const char* srcPath, int64_t offset, char* buffer, int64_t* size) {
    return g_default->type->readFile(g_default, srcPath, offset, buffer, size);
}


int32_t tssDownloadFileFromDefault(const char* srcPath, const char* dstPath, int64_t offset, int64_t size) {
    return g_default->type->downloadFile(g_default, srcPath, dstPath, offset, size);
}


int32_t tssListFileOfDefault(const char* prefix, struct SArray* paths) {
    return g_default->type->listFile(g_default, prefix, paths);
}


int32_t tssDeleteFileFromDefault(const char* path) {
    return g_default->type->deleteFile(g_default, path);
}


int32_t tssGetFileSizeOfDefault(const char* path, int64_t* size) {
    return g_default->type->getFileSize(g_default, path, size);
}
