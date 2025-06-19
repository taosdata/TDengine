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


// File system based shared storage instance, mainly used for testing.
typedef struct {
    // [type] is inherited from SSharedStorage, it must be the first member
    // to allow casting to SSharedStorage.
    const SSharedStorageType* type;

    // type-specific data
    const char* baseDir;
    uint32_t baseDirLen;

    // variable-length buffer for other type-specific data.
    char buf[0];
} SSharedStorageFS;



// printConfig implements SSharedStorageType::printConfig.
static void printConfig(SSharedStorage* pss) {
    SSharedStorageFS* ss = (SSharedStorageFS*)pss;

    printf("type: %s\n", ss->type->name);
    printf("baseDir: %s\n", ss->baseDir);
}



// joinPath is a helper function which joins the base directory and the path to
// create a full path.
static char* joinPath(SSharedStorageFS* ss, const char* path, char* fullPath) {
    uint32_t len = ss->baseDirLen;

    memcpy(fullPath, ss->baseDir, len);
    if( fullPath[len - 1] != TD_DIRSEP_CHAR) {
        fullPath[len++] = TD_DIRSEP_CHAR;
        fullPath[len] = 0;
    }

    if (path != NULL) {
        if (path[0] == TD_DIRSEP_CHAR) {
            path++;
        }
        strcpy(fullPath + len, path);
    }

    return fullPath;
}


// initInstance initializes the SSharedStorageFS instance from the access string.
static bool initInstance(SSharedStorageFS* ss, const char* as) {
    strcpy(ss->buf, as);

    // skip storage type
    char* p = strchr(ss->buf, ':');
    if (p == NULL) {
        tssError("invalid access string: %s", as);
        return false;
    }
    p++;

    // parse key-value pairs
    while (p != NULL) {
        // find key
        char* key = p;
        p = strchr(p, '=');
        if (p == NULL) {
            tssError("invalid access string: %s", as);
            return false;
        }
        *p++ = 0;
        if (strlen(key) == 0) {
            tssError("blank key in access string: %s", as);
            return false;
        }

        // find val
        char* val = p;
        p = strchr(p, ';');
        if (p != NULL) {
            *p++ = 0;
        }
        if (strlen(val) == 0) {
            val = NULL;
        }

        if (taosStrcasecmp(key, "baseDir") == 0) {
            ss->baseDir = val;
        } else {
            tssError("unknown key '%s' in access string: %s", key, as);
            return false;
        }
    }

    if (ss->baseDir == NULL) {
        tssError("baseDir is not configured in access string: %s", as);
        return false;
    }

    ss->baseDirLen = strlen(ss->baseDir);
    return true;
}



// createInstance implements SSharedStorageType::createInstance.
// access string format:
//  fs:baseDir=/path/to/base/dir
static int32_t createInstance(const char* accessString, SSharedStorageFS** ppSS) {
    size_t asLen = strlen(accessString) + 1;

    SSharedStorageFS* ss = (SSharedStorageFS*)taosMemCalloc(1, sizeof(SSharedStorageFS) + asLen);
    if (!ss) {
        tssError("failed to allocate memory for SSharedStorageFS");
        TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }

    if (initInstance(ss, accessString)) {
        *ppSS = ss;
        TAOS_RETURN(TSDB_CODE_SUCCESS);
    }

    taosMemFree(ss);
    TAOS_RETURN(TSDB_CODE_FAILED);
}



static int32_t closeInstance(SSharedStorage* pss) {
    SSharedStorageFS* ss = (SSharedStorageFS*)pss;
    taosMemFree(ss);
    return TSDB_CODE_SUCCESS;
}



// upload implements SSharedStorageType::upload.
static int32_t upload(SSharedStorage* pss, const char* dstPath, const void* data, int64_t size) {
    SSharedStorageFS* ss = (SSharedStorageFS*)pss;

    char fullPath[1024];
    joinPath(ss, dstPath, fullPath);

    int32_t code = taosMulMkDir(taosDirName(fullPath));
    if (code != 0) {
        tssError("failed to create directory %s, code = %d", fullPath, code);
        TAOS_RETURN(code);
    }

    joinPath(ss, dstPath, fullPath);
    TdFilePtr dstFile = taosOpenFile(fullPath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
    if (dstFile == NULL) {
        tssError("failed to open destination file: %s", fullPath);
        TAOS_RETURN(terrno);
    }

    int64_t wrote = taosWriteFile(dstFile, data, size);
    if (wrote < 0) {
        code = terrno;
        tssError("failed to write to destination file: %s", fullPath);
    } else if (wrote != size) {
        code = terrno;
        tssError("failed to write to destination file: %s, want to write %ld bytes, actually write %ld bytes", fullPath, size, wrote);
    }

    taosCloseFile(&dstFile);
    TAOS_RETURN(code);
}



static int32_t copyFile(const char* srcPath, const char* dstPath, int64_t offset, int64_t size) {
    if (offset < 0) {
        tssError("invalid offset %ld for file %s: ", offset, srcPath);
        TAOS_RETURN(TSDB_CODE_INVALID_PARA);
    }

    int32_t code = 0;
    int64_t fileSize = 0;
    if (taosStatFile(srcPath, &fileSize, NULL, NULL) < 0) {
        code = terrno;
        tssError("failed to stat file %s, code = %d", srcPath, code);
        TAOS_RETURN(code);
    }

    // if size is negative, copy until the end of the file
    if (size < 0) {
        size = fileSize - offset;
    }
    if (size < 0 || offset + size > fileSize) {
        tssError("invalid offset %ld and size %ld for file %s", offset, size, srcPath);
        TAOS_RETURN(TSDB_CODE_INVALID_PARA);
    }

    TdFilePtr srcFile = taosOpenFile(srcPath, TD_FILE_READ);
    if (srcFile == NULL) {
        code = terrno;
        tssError("failed to open source file %s, code = %d", srcPath, code);
        TAOS_RETURN(code);
    }

    if (offset > 0 && taosLSeekFile(srcFile, offset, SEEK_SET) < 0) {
        code = terrno;
        tssError("failed to seek source file %s to offset %ld, code = %d", srcPath, offset, code);
        (void)taosCloseFile(&srcFile);
        TAOS_RETURN(code);
    }

    TdFilePtr dstFile = taosOpenFile(dstPath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
    if (dstFile == NULL) {
        code = terrno;
        tssError("failed to open destination file %s, code = %d", dstPath, code);
        (void)taosCloseFile(&srcFile);
        TAOS_RETURN(code);
    }

    char buf[65536];
    while (size > 0) {
        int64_t read = size > sizeof(buf) ? sizeof(buf) : size;
        read = taosReadFile(srcFile, buf, read);
        if (read < 0) {
            code = terrno;
            tssError("failed to read source file, code = %d", code);
            break;
        }

        if (read == 0) {
            break;
        }

        int64_t wrote = taosWriteFile(dstFile, buf, read);
        if (wrote < 0) {
            code = terrno;
            tssError("failed to write to destination file, code = %d", code);
            break;
        } else if (wrote != read) {
            code = terrno;
            tssError("failed to write to destination file, want to write %ld bytes, actually write %ld bytes", read, wrote);
            break;
        }

        size -= read;
    }

    (void)taosCloseFile(&srcFile);
    (void)taosCloseFile(&dstFile);
    TAOS_RETURN(code);
}



// uploadFile implements SSharedStorageType::uploadFile.
static int32_t uploadFile(SSharedStorage* pss, const char* dstPath, const char* srcPath, int64_t offset, int64_t size) {
    SSharedStorageFS* ss = (SSharedStorageFS*)pss;

    char fullPath[1024];
    joinPath(ss, dstPath, fullPath);
    int32_t code = taosMulMkDir(taosDirName(fullPath));
    if (code != 0) {
        tssError("failed to create directory %s, code = %d", fullPath, code);
        TAOS_RETURN(code);
    }

    joinPath(ss, dstPath, fullPath);
    return copyFile(srcPath, fullPath, offset, size);
}



// readFile implements SSharedStorageType::readFile.
static int32_t readFile(SSharedStorage* pss, const char* srcPath, int64_t offset, char* buffer, int64_t* size) {
    SSharedStorageFS* ss = (SSharedStorageFS*)pss;

    char fullPath[1024];
    joinPath(ss, srcPath, fullPath);

    int32_t code = 0;
    TdFilePtr srcFile = taosOpenFile(fullPath, TD_FILE_READ);
    if (srcFile == NULL) {
        code = terrno;
        tssError("failed to open source file %s, code = %d", fullPath, code);
        TAOS_RETURN(code);
    }

    if (offset > 0 && taosLSeekFile(srcFile, offset, SEEK_SET) < 0) {
        code = terrno;
        tssError("failed to seek source file %s to offset %ld, code = %d", fullPath, offset, code);
        (void)taosCloseFile(&srcFile);
        TAOS_RETURN(code);
    }

    int64_t read = taosReadFile(srcFile, buffer, *size);
    if (read < 0) {
        *size = 0;
        code = terrno;
        tssError("failed to read source file %s, code = %d", fullPath, code);
    } else {
        *size = read;
    }

    taosCloseFile(&srcFile);
    return code;
}



// downloadFile implements SSharedStorageType::downloadFile.
static int32_t downloadFile(SSharedStorage* pss, const char* srcPath, const char* dstPath, int64_t offset, int64_t size) {
    SSharedStorageFS* ss = (SSharedStorageFS*)pss;

    char fullPath[1024];
    joinPath(ss, srcPath, fullPath);
    return copyFile(fullPath, dstPath, offset, size);
}



static int32_t listFileInDir(const char* dirPath, size_t baseLen, SArray* res) {
    int32_t code = 0;

    TdDirPtr dir = taosOpenDir(dirPath);
    if (dir == NULL) {
        code = terrno;
        tssError("failed to open directory %s, code = %d", dirPath, code);
        TAOS_RETURN(code);
    }

    size_t dirPathLen = strlen(dirPath);
    char path[1024];
    memcpy(path, dirPath, dirPathLen);

    TdDirEntryPtr entry = NULL;
    while ((entry = taosReadDir(dir)) != NULL) {
        char* name = taosGetDirEntryName(entry);

        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
            continue;
        }

        strcpy(path + dirPathLen, name);

        if (taosDirEntryIsDir(entry)) {
            size_t len = strlen(path);
            if (path[len - 1] != TD_DIRSEP_CHAR) {
                path[len++] = TD_DIRSEP_CHAR;
                path[len] = 0;
            }
            code = listFileInDir(path, baseLen, res);
        } else {
            char* p = strdup(path + baseLen);
            if ( p == NULL ) {
                code = TSDB_CODE_OUT_OF_MEMORY;
            } else if (taosArrayPush(res, &p) == NULL) {
                code = TSDB_CODE_OUT_OF_MEMORY;
                taosMemFree(p);
            }
        }

        if (code != TSDB_CODE_SUCCESS) {
            break;
        }
    }

    int32_t ret = taosCloseDir(&dir);
    if (code == 0 && ret != 0) {
        code = ret;
    }

    TAOS_RETURN(code);
}


// listFile implements SSharedStorageType::listFile.
static int32_t listFile(SSharedStorage* pss, const char* prefix, SArray* paths) {
    SSharedStorageFS* ss = (SSharedStorageFS*)pss;

    char fullPath[1024];
    joinPath(ss, prefix, fullPath);
    size_t len = strlen(fullPath);
    if (fullPath[len - 1] != TD_DIRSEP_CHAR) {
        fullPath[len++] = TD_DIRSEP_CHAR;
        fullPath[len] = 0;
    }

    len = ss->baseDirLen;
    if (ss->baseDir[len - 1] != TD_DIRSEP_CHAR) {
        len++;
    }
    int32_t code = listFileInDir(fullPath, len, paths);
    if (code != TSDB_CODE_SUCCESS) {
        tssError("failed to list files in directory %s, code = %d", fullPath, code);
        for (int i = 0; i < taosArrayGetSize(paths); i++) {
            char* p = *(char**)taosArrayGet(paths, i);
            taosMemFree(p);
        }
        taosArrayClear(paths);
        paths = NULL;
    }

    TAOS_RETURN(code);
}



// deleteFile implements SSharedStorageType::deleteFile.
static int32_t deleteFile(SSharedStorage* pss, const char* path) {
    SSharedStorageFS* ss = (SSharedStorageFS*)pss;

    char fullPath[1024];
    joinPath(ss, path, fullPath);
    return taosRemoveFile(fullPath);
}



// getFileSize implements SSharedStorageType::getFileSize.
static int32_t getFileSize(SSharedStorage* pss, const char* path, int64_t* size) {
    SSharedStorageFS* ss = (SSharedStorageFS*)pss;

    char fullPath[1024];
    joinPath(ss, path, fullPath);
    if (taosStatFile(fullPath, size, NULL, NULL) < 0) {
        int32_t code = terrno;
        if (code == TAOS_SYSTEM_ERROR(ENOENT)) {
            code = TSDB_CODE_NOT_FOUND;
        } else {
            tssError("failed to stat file %s, code = %d", fullPath, code);
        }
        TAOS_RETURN(code);
    }

    TAOS_RETURN(TSDB_CODE_SUCCESS);
}



static int32_t fsCreateInstance(const char* as, SSharedStorage** ppSS);

static const SSharedStorageType sstFS = {
    .name = "fs",
    .printConfig = &printConfig,
    .createInstance = &fsCreateInstance,
    .closeInstance = &closeInstance,
    .upload = &upload,
    .uploadFile = &uploadFile,
    .readFile = &readFile,
    .downloadFile = &downloadFile,
    .listFile = &listFile,
    .deleteFile = &deleteFile,
    .getFileSize = &getFileSize,
};

static int32_t fsCreateInstance(const char* as, SSharedStorage** ppSS) {
    SSharedStorageFS* ss = NULL;

    int32_t code = createInstance(as, &ss);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }
    ss->type = &sstFS;

    *ppSS = (SSharedStorage*)ss;
    return TSDB_CODE_SUCCESS;
}



// register the FS type.
void fsRegisterType() {
    tssRegisterType(&sstFS);
}
