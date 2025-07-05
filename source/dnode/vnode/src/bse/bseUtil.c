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
#include "bseUtil.h"
#include "bseInc.h"
#include "bseTable.h"
#include "tcompression.h"
// compress func set
typedef int32_t (*compressFunc)(void *src, int32_t srcSize, void *dst, int32_t *dstSize);
typedef int32_t (*decompressFunc)(void *src, int32_t srcSize, void *dst, int32_t *dstSize);

// plain compress
static int32_t plainCompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize);
static int32_t plainDecompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize);
// lz4 func
static int32_t lz4Compress(void *src, int32_t srcSize, void *dst, int32_t *dstSize);
static int32_t lz4Decompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize);

static int32_t zlibCompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize);
static int32_t zlibDecompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize);

static int32_t zstdCompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize);
static int32_t zstdDecompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize);

static int32_t xzCompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize);
static int32_t xzDecompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize);

typedef struct {
  char           name[8];
  compressFunc   compress;
  decompressFunc decompress;
} SCompressFuncSet;

static SCompressFuncSet bseCompressFuncSet[] = {
    {"plain", plainCompress, plainDecompress}, {"lz4", lz4Compress, lz4Decompress},
    {"zlib", zlibCompress, zlibDecompress},    {"zstd", zstdCompress, zstdDecompress},
    {"xz", xzCompress, xzDecompress},
};

int32_t plainCompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  int32_t size = *dstSize;
  if (size < srcSize) {
    return -1;
  }
  memcpy(dst, src, srcSize);
  return srcSize;
}

int32_t plainDecompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  int32_t size = *dstSize;
  if (size < srcSize) {
    return -1;
  }
  memcpy(dst, src, srcSize);
  return 0;
}
int32_t lz4Compress(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  return lz4CompressImpl(src, srcSize, dst, dstSize);
}
int32_t lz4Decompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  return lz4DecompressImpl(src, srcSize, dst, dstSize);
}

int32_t zlibCompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  return zlibCompressImpl(src, srcSize, dst, dstSize);
}
int32_t zlibDecompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  return zlibDecompressImpl(src, srcSize, dst, dstSize);
}

int32_t zstdCompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  return zstdCompressImpl(src, srcSize, dst, dstSize);
}
int32_t zstdDecompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  return zstdDecompressImpl(src, srcSize, dst, dstSize);
}

int32_t xzCompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  return xzCompressImpl(src, srcSize, dst, dstSize);
}
int32_t xzDecompress(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  return xzDecompressImpl(src, srcSize, dst, dstSize);
}

int32_t bseCompressData(int8_t type, void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  int32_t code = 0;
  if (type < 0 || type >= sizeof(bseCompressFuncSet) / sizeof(bseCompressFuncSet[0])) {
    return TSDB_CODE_INVALID_CFG;
  }
  bseDebug("compress %s ,srcSize %d, dstSize %d", bseCompressFuncSet[type].name, srcSize, *dstSize);
  return bseCompressFuncSet[type].compress(src, srcSize, dst, dstSize);
}

int32_t bseDecompressData(int8_t type, void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  int32_t code = 0;
  if (type < 0 || type >= sizeof(bseCompressFuncSet) / sizeof(bseCompressFuncSet[0])) {
    return TSDB_CODE_INVALID_CFG;
  }

  bseDebug("decompress %s, srcSize %d, dstSize %d", bseCompressFuncSet[type].name, srcSize, *dstSize);
  return bseCompressFuncSet[type].decompress(src, srcSize, dst, dstSize);
}

// build file path func
void bseBuildDataFullName(SBse *pBse, char *name, char *buf) {
  // build data file name
  // snprintf(name, strlen(name), "%s/%s020"."BSE_DATA_SUFFIX, ver, pBse->path);
  // sprintf(name, strlen(name), "%s/%020"."BSE_DATA_SUFFIX, ver, pBse->path);
  TAOS_UNUSED(sprintf(buf, "%s/%s.%s", pBse->path, name, BSE_DATA_SUFFIX));
}

void bseBuildIndexFullName(SBse *pBse, int64_t ver, char *buf) {
  // build index file name
  TAOS_UNUSED(sprintf(buf, "%s/%020" PRId64 "." BSE_INDEX_SUFFIX, pBse->path, ver));
}
void bseBuildLogFullName(SBse *pBse, int64_t ver, char *buf) {
  TAOS_UNUSED(sprintf(buf, "%s/%020" PRId64 "." BSE_LOG_SUFFIX, pBse->path, ver));
}

void bseBuildCurrentName(SBse *pBse, char *name) {
  snprintf(name, BSE_FILE_FULL_LEN, "%s%sCURRENT", pBse->path, TD_DIRSEP);
}

void bseBuildTempCurrentName(SBse *pBse, char *name) {
  snprintf(name, BSE_FILE_FULL_LEN, "%s%sCURRENT-temp", pBse->path, TD_DIRSEP);
}

void bseBuildFullMetaName(SBse *pBse, char *name, char *path) {
  snprintf(path, BSE_FILE_FULL_LEN, "%s%s%s.%s", pBse->path, TD_DIRSEP, name, BSE_META_SUFFIX);
}
void bseBuildFullTempMetaName(SBse *pBse, char *name, char *path) {
  snprintf(path, BSE_FILE_FULL_LEN, "%s%s%s.%s-temp", pBse->path, TD_DIRSEP, name, BSE_META_SUFFIX);
}

void bseBuildMetaName(int64_t ts, char *name) {
  // refactor later
  snprintf(name, BSE_FILE_FULL_LEN, "%" PRId64 ".%s", ts, BSE_META_SUFFIX);
}
void bseBuildTempMetaName(int64_t ts, char *name) {
  // refactor later
  snprintf(name, BSE_FILE_FULL_LEN, "%" PRId64 ".%s-temp", ts, BSE_META_SUFFIX);
}

void bseBuildFullName(SBse *pBse, char *name, char *fullname) {
  snprintf(fullname, BSE_FILE_FULL_LEN, "%s%s%s", pBse->path, TD_DIRSEP, name);
}

void bseBuildDataName(int64_t ts, char *name) {
  snprintf(name, BSE_FILE_FULL_LEN, "%" PRId64 ".%s", ts, BSE_DATA_SUFFIX);
}

int32_t bseGetRetentionTsBySeq(SBse *pBse, int64_t seq, int64_t *retentionTs) {
  int32_t code = 0;
  int64_t tts = 0;

  SBseCommitInfo *pCommitInfo = &pBse->commitInfo;
  for (int32_t i = 0; i < taosArrayGetSize(pCommitInfo->pFileList); i++) {
    SBseLiveFileInfo *pInfo = taosArrayGet(pCommitInfo->pFileList, i);
    if (seqRangeContains(&pInfo->range, seq)) {
      tts = pInfo->retentionTs;
      break;
    }
  }

  *retentionTs = tts;
  return code;
}
