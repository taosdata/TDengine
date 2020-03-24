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
#include <dirent.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "tsdbFile.h"

#define TSDB_FILE_HEAD_SIZE 512
#define TSDB_FILE_DELIMITER 0xF00AFA0F

const char *tsdbFileSuffix[] = {
    ".head",  // TSDB_FILE_TYPE_HEAD
    ".data",  // TSDB_FILE_TYPE_DATA
    ".last"   // TSDB_FILE_TYPE_LAST
};

static int compFGroupKey(const void *key, const void *fgroup);
static int compFGroup(const void *arg1, const void *arg2);
static int tsdbGetFileName(char *dataDir, int fileId, int8_t type, char *fname);
static int tsdbCreateFile(char *dataDir, int fileId, int8_t type, int maxTables, SFile *pFile);
static int tsdbWriteFileHead(SFile *pFile);
static int tsdbWriteHeadFileIdx(SFile *pFile, int maxTables);

STsdbFileH *tsdbInitFileH(char *dataDir, int maxFiles) {
  STsdbFileH *pFileH = (STsdbFileH *)calloc(1, sizeof(STsdbFileH) + sizeof(SFileGroup) * maxFiles);
  if (pFileH == NULL) {  // TODO: deal with ERROR here
    return NULL;
  }

  pFileH->maxFGroups = maxFiles;

  DIR *dir = opendir(dataDir);
  if (dir == NULL) {
    free(pFileH);
    return NULL;
  }

  struct dirent *dp;
  while ((dp = readdir(dir)) != NULL) {
    if (strncmp(dp->d_name, ".", 1) == 0 || strncmp(dp->d_name, "..", 1) == 0) continue;
    // TODO
  }

  return pFileH;
}

void tsdbCloseFileH(STsdbFileH *pFileH) { free(pFileH); }

int tsdbCreateFGroup(STsdbFileH *pFileH, char *dataDir, int fid, int maxTables) {
  if (pFileH->numOfFGroups >= pFileH->maxFGroups) return -1;

  SFileGroup  fGroup;
  SFileGroup *pFGroup = &fGroup;
  if (fid < TSDB_MIN_FILE_ID(pFileH) || fid > TSDB_MAX_FILE_ID(pFileH) ||
      bsearch((void *)&fid, (void *)(pFileH->fGroup), pFileH->numOfFGroups, sizeof(SFileGroup), compFGroupKey) ==
          NULL) {
    pFGroup->fileId = fid;
    for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) {
      if (tsdbCreateFile(dataDir, fid, type, maxTables, &(pFGroup->files[type])) < 0) {
        // TODO: deal with the ERROR here, remove those creaed file
        return -1;
      }
    }

    pFileH->fGroup[pFileH->numOfFGroups++] = fGroup;
    qsort((void *)(pFileH->fGroup), pFileH->numOfFGroups, sizeof(SFileGroup), compFGroup);
  }
  return 0;
}

int tsdbRemoveFileGroup(STsdbFileH *pFileH, int fid) {
  SFileGroup *pGroup =
      bsearch((void *)&fid, (void *)(pFileH->fGroup), pFileH->numOfFGroups, sizeof(SFileGroup), compFGroupKey);
  if (pGroup == NULL) return -1;

  // Remove from disk
  for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) {
    remove(pGroup->files[type].fname);
  }

  // Adjust the memory
  int filesBehind = pFileH->numOfFGroups - (((char *)pGroup - (char *)(pFileH->fGroup)) / sizeof(SFileGroup) + 1);
  if (filesBehind > 0) {
    memmove((void *)pGroup, (void *)((char *)pGroup + sizeof(SFileGroup)), sizeof(SFileGroup) * filesBehind);
  }
  pFileH->numOfFGroups--;

  return 0;
}

static int compFGroupKey(const void *key, const void *fgroup) {
  int         fid = *(int *)key;
  SFileGroup *pFGroup = (SFileGroup *)fgroup;
  return (fid - pFGroup->fileId);
}

static int compFGroup(const void *arg1, const void *arg2) {
  return ((SFileGroup *)arg1)->fileId - ((SFileGroup *)arg2)->fileId;
}

static int tsdbWriteFileHead(SFile *pFile) {
  char head[TSDB_FILE_HEAD_SIZE] = "\0";

  pFile->size += TSDB_FILE_HEAD_SIZE;

  // TODO: write version and File statistic to the head
  lseek(pFile->fd, 0, SEEK_SET);
  if (write(pFile->fd, head, TSDB_FILE_HEAD_SIZE) < 0) return -1;

  return 0;
}

static int tsdbWriteHeadFileIdx(SFile *pFile, int maxTables) {
  int   size = sizeof(SCompIdx) * maxTables;
  void *buf = calloc(1, size);
  if (buf == NULL) return -1;

  if (lseek(pFile->fd, TSDB_FILE_HEAD_SIZE, SEEK_SET) < 0) {
    free(buf);
    return -1;
  }

  if (write(pFile->fd, buf, size) < 0) {
    free(buf);
    return -1;
  }

  pFile->size += size;

  free(buf);
  return 0;
}

static int tsdbGetFileName(char *dataDir, int fileId, int8_t type, char *fname) {
  if (dataDir == NULL || fname == NULL || !IS_VALID_TSDB_FILE_TYPE(type)) return -1;

  sprintf(fname, "%s/f%d%s", dataDir, fileId, tsdbFileSuffix[type]);

  return 0;
}

static int tsdbOpenFileForWrite(SFile *pFile, int oflag) { // TODO: change the function
  if (TSDB_IS_FILE_OPENED(pFile)) return -1;

  pFile->fd = open(pFile->fname, oflag, 0755);
  if (pFile->fd < 0) return -1;

  return 0;
}

static int tsdbCloseFile(SFile *pFile) {
  if (!TSDB_IS_FILE_OPENED(pFile)) return -1;
  int ret = close(pFile->fd);
  pFile->fd = -1;

  return ret;
}

static int tsdbCreateFile(char *dataDir, int fileId, int8_t type, int maxTables, SFile *pFile) {
  memset((void *)pFile, 0, sizeof(SFile));
  pFile->type = type;
  pFile->fd = -1;

  tsdbGetFileName(dataDir, fileId, type, pFile->fname);
  if (access(pFile->fname, F_OK) == 0) {
    // File already exists
    return -1;
  }

  if (tsdbOpenFileForWrite(pFile, O_WRONLY | O_CREAT) < 0) {
    // TODO: deal with the ERROR here
    return -1;
  }

  if (type == TSDB_FILE_TYPE_HEAD) {
    if (tsdbWriteHeadFileIdx(pFile, maxTables) < 0) {
      tsdbCloseFile(pFile);
      return -1;
    }
  }

  if (tsdbWriteFileHead(pFile) < 0) {
    tsdbCloseFile(pFile);
    return -1;
  }

  tsdbCloseFile(pFile);

  return 0;
}

void tsdbGetKeyRangeOfFileId(int32_t daysPerFile, int8_t precision, int32_t fileId, TSKEY *minKey,
                                    TSKEY *maxKey) {
  *minKey = fileId * daysPerFile * tsMsPerDay[precision];
  *maxKey = *minKey + daysPerFile * tsMsPerDay[precision] - 1;
}