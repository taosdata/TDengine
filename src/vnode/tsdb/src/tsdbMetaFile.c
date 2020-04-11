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
#include "os.h"

#include "taosdef.h"
#include "hash.h"
#include "tsdbMain.h"

#define TSDB_META_FILE_VERSION_MAJOR 1
#define TSDB_META_FILE_VERSION_MINOR 0
#define TSDB_META_FILE_HEADER_SIZE 512

typedef struct {
  int32_t offset;
  int32_t size;
  int64_t uid;
} SRecordInfo;

static int32_t tsdbGetMetaFileName(char *rootDir, char *fname);
// static int32_t tsdbCheckMetaHeader(int fd);
static int32_t tsdbWriteMetaHeader(int fd);
static int     tsdbCreateMetaFile(char *fname);
static int     tsdbRestoreFromMetaFile(char *fname, SMetaFile *mfh);

SMetaFile *tsdbInitMetaFile(char *rootDir, int32_t maxTables, iterFunc iFunc, afterFunc aFunc, void *appH) {
  char fname[128] = "\0";
  if (tsdbGetMetaFileName(rootDir, fname) < 0) return NULL;

  SMetaFile *mfh = (SMetaFile *)calloc(1, sizeof(SMetaFile));
  if (mfh == NULL) return NULL;

  mfh->iFunc = iFunc;
  mfh->aFunc = aFunc;
  mfh->appH = appH;
  mfh->nDel = 0;
  mfh->tombSize = 0;
  mfh->size = 0;

  // OPEN MAP
  mfh->map =
      taosHashInit(maxTables * TSDB_META_HASH_FRACTION, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false);
  if (mfh->map == NULL) {
    free(mfh);
    return NULL;
  }

  // OPEN FILE
  if (access(fname, F_OK) < 0) {  // file not exists
    mfh->fd = tsdbCreateMetaFile(fname);
    if (mfh->fd < 0) {
      taosHashCleanup(mfh->map);
      free(mfh);
      return NULL;
    }
    mfh->size += TSDB_META_FILE_HEADER_SIZE;
  } else {  // file exists, recover from file
    if (tsdbRestoreFromMetaFile(fname, mfh) < 0) {
      taosHashCleanup(mfh->map);
      free(mfh);
      return NULL;
    }
  }

  return mfh;
}

int32_t tsdbInsertMetaRecord(SMetaFile *mfh, int64_t uid, void *cont, int32_t contLen) {
  if (taosHashGet(mfh->map, (char *)(&uid), sizeof(uid)) != NULL) {
    return -1;
  }

  SRecordInfo info;
  info.offset = mfh->size;
  info.size = contLen;
  info.uid = uid;

  mfh->size += (contLen + sizeof(SRecordInfo));

  if (taosHashPut(mfh->map, (char *)(&uid), sizeof(uid), (void *)(&info), sizeof(SRecordInfo)) < 0) {
    return -1;
  }

  // TODO: make below a function to implement
  if (lseek(mfh->fd, info.offset, SEEK_SET) < 0) {
    return -1;
  }

  if (write(mfh->fd, (void *)(&info), sizeof(SRecordInfo)) < 0) {
    return -1;
  }

  if (write(mfh->fd, cont, contLen) < 0) {
    return -1;
  }

  fsync(mfh->fd);

  mfh->tombSize++;

  return 0;
}

int32_t tsdbDeleteMetaRecord(SMetaFile *mfh, int64_t uid) {
  char *ptr = taosHashGet(mfh->map, (char *)(&uid), sizeof(uid));
  if (ptr == NULL) return -1;

  SRecordInfo info = *(SRecordInfo *)ptr;

  // Remove record from hash table
  taosHashRemove(mfh->map, (char *)(&uid), sizeof(uid));

  // Remove record from file

  info.offset = -info.offset;
  if (lseek(mfh->fd, -info.offset, SEEK_CUR) < 0) {
    return -1;
  }

  if (write(mfh->fd, (void *)(&info), sizeof(SRecordInfo)) < 0) {
    return -1;
  }

  fsync(mfh->fd);

  mfh->nDel++;

  return 0;
}

int32_t tsdbUpdateMetaRecord(SMetaFile *mfh, int64_t uid, void *cont, int32_t contLen) {
  char *ptr = taosHashGet(mfh->map, (char *)(&uid), sizeof(uid));
  if (ptr == NULL) return -1;

  SRecordInfo info = *(SRecordInfo *)ptr;
  // Update the hash table
  if (taosHashPut(mfh->map, (char *)(&uid), sizeof(uid), (void *)(&info), sizeof(SRecordInfo)) < 0) {
    return -1;
  }

  // Update record in file
  if (info.size >= contLen) {  // Just update it in place
    info.size = contLen;

  } else {  // Just append to the end of file
    info.offset = mfh->size;
    info.size = contLen;

    mfh->size += contLen;
  }
  if (lseek(mfh->fd, -info.offset, SEEK_CUR) < 0) {
    return -1;
  }

  if (write(mfh->fd, (void *)(&info), sizeof(SRecordInfo)) < 0) {
    return -1;
  }

  fsync(mfh->fd);

  return 0;
}

void tsdbCloseMetaFile(SMetaFile *mfh) {
  if (mfh == NULL) return;
  close(mfh->fd);

  taosHashCleanup(mfh->map);
  tfree(mfh);
}

static int32_t tsdbGetMetaFileName(char *rootDir, char *fname) {
  if (rootDir == NULL) return -1;
  sprintf(fname, "%s/%s", rootDir, TSDB_META_FILE_NAME);
  return 0;
}

// static int32_t tsdbCheckMetaHeader(int fd) {
//   // TODO: write the meta file header check function
//   return 0;
// }

static int32_t tsdbWriteMetaHeader(int fd) {
  // TODO: write the meta file header to file
  char head[TSDB_META_FILE_HEADER_SIZE] = "\0";
  sprintf(head, "version: %d.%d", TSDB_META_FILE_VERSION_MAJOR, TSDB_META_FILE_VERSION_MINOR);

  write(fd, (void *)head, TSDB_META_FILE_HEADER_SIZE);
  return 0;
}

// static int32_t tsdbReadMetaHeader(int fd) {
//   lseek(fd, TSDB_META_FILE_HEADER_SIZE, SEEK_SET);
//   return 0;
// }

static int tsdbCreateMetaFile(char *fname) {
  int fd = open(fname, O_RDWR | O_CREAT, 0755);
  if (fd < 0) return -1;

  if (tsdbWriteMetaHeader(fd) < 0) {
    close(fd);
    return -1;
  }

  return fd;
}

static int tsdbCheckMetaFileIntegrety(int fd) {
  // TODO
  return 0;
}

static int tsdbRestoreFromMetaFile(char *fname, SMetaFile *mfh) {
  int fd = open(fname, O_RDWR);
  if (fd < 0) return -1;

  if (tsdbCheckMetaFileIntegrety(fd) < 0) {
    // TODO: decide if to auto-recover the file
    close(fd);
    return -1;
  }

  if (lseek(fd, TSDB_META_FILE_HEADER_SIZE, SEEK_SET) < 0) {
    // TODO: deal with the error
    close(fd);
    return -1;
  }

  mfh->size += TSDB_META_FILE_HEADER_SIZE;

  mfh->fd = fd;

  void *buf = NULL;
  // int buf_size = 0;

  SRecordInfo info;
  while (1) {
    if (read(mfh->fd, (void *)(&info), sizeof(SRecordInfo)) == 0) break;
    if (info.offset < 0) {
      mfh->size += (info.size + sizeof(SRecordInfo));
      mfh->tombSize += (info.size + sizeof(SRecordInfo));
      lseek(mfh->fd, info.size, SEEK_CUR);
      mfh->size = mfh->size + sizeof(SRecordInfo) + info.size;
      mfh->tombSize = mfh->tombSize + sizeof(SRecordInfo) + info.size;
    } else {
      if (taosHashPut(mfh->map, (char *)(&info.uid), sizeof(info.uid), (void *)(&info), sizeof(SRecordInfo)) < 0) {
        if (buf) free(buf);
        return -1;
      }

      buf = realloc(buf, info.size);
      if (buf == NULL) return -1;

      if (read(mfh->fd, buf, info.size) < 0) {
        if (buf) free(buf);
        return -1;
      }
      (*mfh->iFunc)(mfh->appH, buf, info.size);

      mfh->size = mfh->size + sizeof(SRecordInfo) + info.size;
    }

  }
  (*mfh->aFunc)(mfh->appH);

  if (buf) free(buf);

  return 0;
}