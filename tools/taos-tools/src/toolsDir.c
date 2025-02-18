/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#ifndef WINDOWS
#include <sys/time.h>
#endif
#include "toolsdef.h"

#define toolsMemoryFree free
#define toolsMemoryMalloc malloc

#ifdef WINDOWS

#include <windows.h>

typedef struct TdDirEntry {
    WIN32_FIND_DATA findFileData;
} TdDirEntry;

typedef struct TdDir {
    TdDirEntry dirEntry;
    HANDLE     hFind;
} TdDir;

enum {
    WRDE_NOSPACE = 1, /* Ran out of memory.  */
    WRDE_BADCHAR,     /* A metachar appears in the wrong place.  */
    WRDE_BADVAL,      /* Undefined var reference with WRDE_UNDEF.  */
    WRDE_CMDSUB,      /* Command substitution with WRDE_NOCMD.  */
    WRDE_SYNTAX       /* Shell syntax error.  */
};

int wordexp(char *words, wordexp_t *pwordexp, int flags) {
    pwordexp->we_offs = 0;
    pwordexp->we_wordc = 1;
    pwordexp->we_wordv[0] = pwordexp->wordPos;

    memset(pwordexp->wordPos, 0, 1025);
    if (_fullpath(pwordexp->wordPos, words, 1024) == NULL) {
        pwordexp->we_wordv[0] = words;
        printf("failed to parse relative path:%s to abs path\n", words);
        return -1;
    }

    // printf("parse relative path:%s to abs path:%s\n", words, pwordexp->wordPos);
    return 0;
}

void wordfree(wordexp_t *pwordexp) {}

#elif defined(DARWIN)

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <wordexp.h>
#include <string.h>

typedef struct dirent dirent;
typedef struct dirent TdDirEntry;

typedef struct TdDir {
    TdDirEntry    dirEntry;
    TdDirEntry    dirEntry1;
    TdDirEntryPtr dirEntryPtr;
    DIR          *pDir;
} TdDir;

#else

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <wordexp.h>
#include <string.h>

typedef struct dirent dirent;
typedef struct DIR    TdDir;
typedef struct dirent TdDirEntry;

#endif

int32_t toolsExpandDir(const char *dirname, char *outname, int32_t maxlen) {
    wordexp_t full_path;
    switch (wordexp(dirname, &full_path, 0)) {
        case 0:
            break;
        case WRDE_NOSPACE:
            wordfree(&full_path);
            // printf("failed to expand path:%s since Out of memory\n", dirname);
            return -1;
        case WRDE_BADCHAR:
            // printf("failed to expand path:%s since illegal occurrence of newline or one of |, &, ;, <, >, (, ), {, }\n",
            // dirname);
            return -1;
        case WRDE_SYNTAX:
            // printf("failed to expand path:%s since Shell syntax error, such as unbalanced parentheses or unmatched
            // quotes\n", dirname);
            return -1;
        default:
            // printf("failed to expand path:%s since %s\n", dirname, strerror(errno));
            return -1;
    }

    if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {
        strncpy(outname, full_path.we_wordv[0], maxlen);
    }

    wordfree(&full_path);

    return 0;
}

TdDirPtr toolsOpenDir(const char *dirname) {
    if (dirname == NULL) {
        return NULL;
    }

#ifdef WINDOWS
    char   szFind[MAX_PATH];  //这是要找的
    HANDLE hFind;

    TdDirPtr pDir = toolsMemoryMalloc(sizeof(TdDir));

    strcpy(szFind, dirname);
    strcat(szFind, "\\*.*");  //利用通配符找这个目录下的所以文件，包括目录

    pDir->hFind = FindFirstFile(szFind, &(pDir->dirEntry.findFileData));
    if (INVALID_HANDLE_VALUE == pDir->hFind) {
        toolsMemoryFree(pDir);
        return NULL;
    }
    return pDir;
#elif defined(DARWIN)
    DIR *pDir = opendir(dirname);
    if (pDir == NULL) return NULL;
    TdDirPtr dirPtr = (TdDirPtr)taosMemoryMalloc(sizeof(TdDir));
    dirPtr->dirEntryPtr = (TdDirEntryPtr) & (dirPtr->dirEntry1);
    dirPtr->pDir = pDir;
    return dirPtr;
#else
    return (TdDirPtr)opendir(dirname);
#endif
}

TdDirEntryPtr toolsReadDir(TdDirPtr pDir) {
    if (pDir == NULL) {
        return NULL;
    }
#ifdef WINDOWS
    if (!FindNextFile(pDir->hFind, &(pDir->dirEntry.findFileData))) {
        return NULL;
    }
    return (TdDirEntryPtr) & (pDir->dirEntry.findFileData);
#elif defined(DARWIN)
    if (readdir_r(pDir->pDir, (dirent *)&(pDir->dirEntry), (dirent **)&(pDir->dirEntryPtr)) == 0) {
        return pDir->dirEntryPtr;
    } else {
        return NULL;
    }
#else
    return (TdDirEntryPtr)readdir((DIR *)pDir);
#endif
}

char *toolsGetDirEntryName(TdDirEntryPtr pDirEntry) {
    /*if (pDirEntry == NULL) {*/
    /*return NULL;*/
    /*}*/
#ifdef WINDOWS
    return pDirEntry->findFileData.cFileName;
#else
    return ((dirent *)pDirEntry)->d_name;
#endif
}

int32_t toolsCloseDir(TdDirPtr *ppDir) {
    if (ppDir == NULL || *ppDir == NULL) {
        return -1;
    }
#ifdef WINDOWS
    FindClose((*ppDir)->hFind);
    toolsMemoryFree(*ppDir);
    *ppDir = NULL;
    return 0;
#elif defined(DARWIN)
    closedir((*ppDir)->pDir);
    toolsMemoryFree(*ppDir);
    *ppDir = NULL;
    return 0;
#else
    closedir((DIR *)*ppDir);
    *ppDir = NULL;
    return 0;
#endif
}
