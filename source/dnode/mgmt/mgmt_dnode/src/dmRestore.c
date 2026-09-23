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

#include "dmInt.h"
#include "libs/sm4/sm4.h"
#include "mnode.h"
#include "tencrypt.h"
#include "tversion.h"
#include "version.h"
#include "vnd.h"
#include "vnodeInt.h"
#include "wal.h"

typedef struct {
  int64_t id;
  SJson *pManifest;
} SDmRestoreBackup;

#define DM_RESTORE_PROGRESS_INTERVAL (INT64_C(1) << 30)

static void dmRestoreChunkPath(const SDmBackupFile *pBackupFile, int32_t chunkIndex, char *path, size_t pathSize) {
  (void)snprintf(path, pathSize, "%s%s%s%s%08d", pBackupFile->chunkDir, TD_DIRSEP, pBackupFile->chunkBase,
                 TD_DIRSEP, chunkIndex);
}

// Close the current chunk file and open the next one for reading. The IV is
// only stored in chunk 1, so subsequent chunks carry a full data area. Every
// non-final chunk is exactly chunkSize bytes (the writer only rolls to the
// next chunk when the current one is full), so only the final chunk may be
// smaller and needs its actual size read back.
static int32_t dmBackupFileOpenNextChunkRead(SDmBackupFile *pBackupFile) {
  char path[PATH_MAX] = {0};

  if (taosCloseFile(&pBackupFile->pFile) != 0) {
    pBackupFile->pFile = NULL;
    return terrno != 0 ? terrno : TSDB_CODE_FAILED;
  }
  ++pBackupFile->chunkIndex;
  dmRestoreChunkPath(pBackupFile, pBackupFile->chunkIndex, path, sizeof(path));
  pBackupFile->pFile = taosOpenFile(path, TD_FILE_READ);
  if (pBackupFile->pFile == NULL) {
    return terrno;
  }
  if (pBackupFile->encrypted) {
    pBackupFile->fileSize = pBackupFile->chunkSize;
    if (pBackupFile->chunkIndex == pBackupFile->numOfChunks) {
      pBackupFile->fileSize = taosLSeekFile(pBackupFile->pFile, 0, SEEK_END);
      if (pBackupFile->fileSize <= 0 || pBackupFile->fileSize % DM_BACKUP_SM4_BLOCK_SIZE != 0 ||
          taosLSeekFile(pBackupFile->pFile, 0, SEEK_SET) < 0) {
        (void)taosCloseFile(&pBackupFile->pFile);
        pBackupFile->pFile = NULL;
        return TSDB_CODE_FILE_CORRUPTED;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

// Open a chunked vnode backup: <dir>/<baseName>/00000001 ... 0000000N.
// The size of every non-final chunk must be identical and the final chunk must
// not exceed it (tolerance-based verification, independent of the chunk size
// used when the backup was created). Encrypted mode reads the per-logical-file
// IV from the head of chunk 1; the padding block is only verified when the
// final chunk is exhausted.
static int32_t dmBackupFileOpenChunked(SDmBackupFile *pBackupFile, const char *dir, const char *baseName,
                                       int32_t numOfChunks, const char *password) {
  memset(pBackupFile, 0, sizeof(*pBackupFile));
  if (numOfChunks <= 0) {
    return TSDB_CODE_FILE_CORRUPTED;
  }
  pBackupFile->chunked = true;
  pBackupFile->chunkIndex = 1;
  pBackupFile->numOfChunks = numOfChunks;
  tstrncpy(pBackupFile->chunkDir, dir, sizeof(pBackupFile->chunkDir));
  tstrncpy(pBackupFile->chunkBase, baseName, sizeof(pBackupFile->chunkBase));
  pBackupFile->encrypted = password != NULL;
  sha256_init(&pBackupFile->checksumCtx);

  // Tolerance-based size verification: all non-final chunks must be equal
  // sized, and the final chunk must be no larger than that size.
  char    path[PATH_MAX] = {0};
  int64_t firstSize = 0;
  for (int32_t i = 1; i <= numOfChunks; ++i) {
    int64_t size = 0;
    dmRestoreChunkPath(pBackupFile, i, path, sizeof(path));
    if (taosStatFile(path, &size, NULL, NULL) < 0) {
      return TSDB_CODE_FILE_CORRUPTED;
    }
    if (i == 1) {
      if (size <= 0) {
        return TSDB_CODE_FILE_CORRUPTED;
      }
      firstSize = size;
      pBackupFile->chunkSize = size;
    } else if (i < numOfChunks) {
      if (size != firstSize) {
        return TSDB_CODE_FILE_CORRUPTED;
      }
    } else if (size <= 0 || size > firstSize) {
      return TSDB_CODE_FILE_CORRUPTED;
    }
  }

  dmRestoreChunkPath(pBackupFile, 1, path, sizeof(path));
  pBackupFile->pFile = taosOpenFile(path, TD_FILE_READ);
  if (pBackupFile->pFile == NULL) {
    return terrno;
  }
  if (!pBackupFile->encrypted) {
    return TSDB_CODE_SUCCESS;
  }
  // Chunk 1 carries the per-logical-file random IV; the rest of its data area
  // is firstSize - sizeof(iv), aligned to the SM4 block size by construction.
  pBackupFile->fileSize = firstSize;
  if (pBackupFile->fileSize <= DM_BACKUP_SM4_BLOCK_SIZE ||
      (pBackupFile->fileSize - DM_BACKUP_SM4_BLOCK_SIZE) % DM_BACKUP_SM4_BLOCK_SIZE != 0 ||
      taosReadFile(pBackupFile->pFile, pBackupFile->iv, sizeof(pBackupFile->iv)) != sizeof(pBackupFile->iv)) {
    (void)taosCloseFile(&pBackupFile->pFile);
    pBackupFile->pFile = NULL;
    return TSDB_CODE_FILE_CORRUPTED;
  }
  sha256_update(&pBackupFile->checksumCtx, pBackupFile->iv, sizeof(pBackupFile->iv));
  pBackupFile->fileSize -= sizeof(pBackupFile->iv);
  memcpy(pBackupFile->key, password, sizeof(pBackupFile->key));
  return TSDB_CODE_SUCCESS;
}

static int64_t dmBackupFileReadRaw(SDmBackupFile *pBackupFile, void *buffer, int64_t size) {
  int64_t totalRead = 0;

  while (totalRead < size) {
    int64_t bytesRead = taosReadFile(pBackupFile->pFile, (uint8_t *)buffer + totalRead, size - totalRead);
    if (bytesRead < 0) {
      return -1;
    }
    if (bytesRead == 0) {
      // In chunked plaintext mode, EOF of the current chunk moves on to the
      // next one. In encrypted mode chunk boundaries are driven by
      // fileSize/block alignment inside dmBackupFileRead, so we never cross
      // chunks here.
      if (pBackupFile->chunked && !pBackupFile->encrypted && pBackupFile->chunkIndex < pBackupFile->numOfChunks) {
        int32_t code = dmBackupFileOpenNextChunkRead(pBackupFile);
        if (code != TSDB_CODE_SUCCESS) {
          terrno = code;
          return -1;
        }
        continue;
      }
      break;
    }
    sha256_update(&pBackupFile->checksumCtx, (uint8_t *)buffer + totalRead, bytesRead);
    totalRead += bytesRead;
  }
  return totalRead;
}

static int32_t dmBackupFileOpen(SDmBackupFile *pBackupFile, const char *path, const char *password) {
  memset(pBackupFile, 0, sizeof(*pBackupFile));
  pBackupFile->pFile = taosOpenFile(path, TD_FILE_READ);
  if (pBackupFile->pFile == NULL) {
    return terrno;
  }
  sha256_init(&pBackupFile->checksumCtx);
  pBackupFile->encrypted = password != NULL;
  if (!pBackupFile->encrypted) {
    return TSDB_CODE_SUCCESS;
  }
  pBackupFile->fileSize = taosLSeekFile(pBackupFile->pFile, 0, SEEK_END);
  if (pBackupFile->fileSize <= DM_BACKUP_SM4_BLOCK_SIZE ||
      (pBackupFile->fileSize - DM_BACKUP_SM4_BLOCK_SIZE) % DM_BACKUP_SM4_BLOCK_SIZE != 0 ||
      taosLSeekFile(pBackupFile->pFile, 0, SEEK_SET) < 0) {
    (void)taosCloseFile(&pBackupFile->pFile);
    return TSDB_CODE_FILE_CORRUPTED;
  }
  // The first block of an encrypted backup file is the per-file random IV,
  // which must be recovered before decrypting the payload.
  if (taosReadFile(pBackupFile->pFile, pBackupFile->iv, sizeof(pBackupFile->iv)) != sizeof(pBackupFile->iv)) {
    (void)taosCloseFile(&pBackupFile->pFile);
    return TSDB_CODE_FILE_CORRUPTED;
  }
  sha256_update(&pBackupFile->checksumCtx, pBackupFile->iv, sizeof(pBackupFile->iv));
  pBackupFile->fileSize -= sizeof(pBackupFile->iv);
  memcpy(pBackupFile->key, password, sizeof(pBackupFile->key));
  return TSDB_CODE_SUCCESS;
}

static int64_t dmBackupFileRead(SDmBackupFile *pBackupFile, void *buffer, int64_t size) {
  if (pBackupFile->pFile == NULL || buffer == NULL || size < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }

  if (!pBackupFile->encrypted) {
    return dmBackupFileReadRaw(pBackupFile, buffer, size);
  }

  int64_t totalRead = 0;

  // Serve the leftover plaintext tail (0..15 bytes) kept from a previous
  // batch first; it never exceeds one SM4 block, so buffer is enough.
  if (pBackupFile->bufferLen > 0) {
    int32_t copyLen = TMIN((int32_t)(size), pBackupFile->bufferLen);
    memcpy((uint8_t *)buffer, pBackupFile->buffer, copyLen);
    pBackupFile->bufferLen -= copyLen;
    if (pBackupFile->bufferLen > 0) {
      memmove(pBackupFile->buffer, pBackupFile->buffer + copyLen, pBackupFile->bufferLen);
    }
    totalRead += copyLen;
  }

  while (totalRead < size && !pBackupFile->finalized) {
    uint8_t cipher[4096], plain[4096];
    int32_t want = (int32_t)TMIN((int64_t)sizeof(cipher), size - totalRead);
    int32_t batchLen = (want + DM_BACKUP_SM4_BLOCK_SIZE - 1) & ~(DM_BACKUP_SM4_BLOCK_SIZE - 1);
    batchLen = TMIN(batchLen, (int32_t)pBackupFile->fileSize);
    if (batchLen <= 0) {
      break;
    }
    if (dmBackupFileReadRaw(pBackupFile, cipher, batchLen) != batchLen) {
      terrno = TSDB_CODE_FILE_CORRUPTED;
      return -1;
    }
    unsigned int outLen = 0;
    int ret = SM4_CBC_Decrypt(
        pBackupFile->key,
        sizeof(pBackupFile->key),
        pBackupFile->iv,
        sizeof(pBackupFile->iv),
        cipher,
        (unsigned int)batchLen,
        plain,
        &outLen
      );
    if (ret != 0 || outLen != (unsigned int)batchLen) {
      terrno = TSDB_CODE_FILE_CORRUPTED;
      return -1;
    }
    memcpy(pBackupFile->iv, cipher + batchLen - DM_BACKUP_SM4_BLOCK_SIZE, sizeof(pBackupFile->iv));
    pBackupFile->fileSize -= batchLen;

    int32_t avail = batchLen;  // plaintext bytes that may be consumed
    if (pBackupFile->fileSize == 0) {
      if (pBackupFile->chunked && pBackupFile->chunkIndex < pBackupFile->numOfChunks) {
        // Chunk boundary reached (chunk size is a multiple of the SM4 block
        // size, so a ciphertext block never straddles two chunk files). The
        // padding block only lives in the final chunk.
        int32_t code = dmBackupFileOpenNextChunkRead(pBackupFile);
        if (code != TSDB_CODE_SUCCESS) {
          terrno = code;
          return -1;
        }
      } else {
        // Logical end of the file: the final plaintext block carries the
        // padding; validate it and shrink the consumable window so the padding
        // is never handed to the caller.
        uint8_t *pLast = plain + batchLen - DM_BACKUP_SM4_BLOCK_SIZE;
        uint8_t  padding = pLast[DM_BACKUP_SM4_BLOCK_SIZE - 1];
        if (padding == 0 || padding > DM_BACKUP_SM4_BLOCK_SIZE) {
          terrno = TSDB_CODE_FILE_CORRUPTED;
          return -1;
        }
        for (int32_t i = DM_BACKUP_SM4_BLOCK_SIZE - padding; i < DM_BACKUP_SM4_BLOCK_SIZE; ++i) {
          if (pLast[i] != padding) {
            terrno = TSDB_CODE_FILE_CORRUPTED;
            return -1;
          }
        }
        avail -= padding;
        pBackupFile->finalized = true;
      }
    }

    int32_t give = TMIN(avail, (int32_t)(size - totalRead));
    memcpy((uint8_t *)buffer + totalRead, plain, give);
    totalRead += give;
    if (avail > give) {
      // Keep the leftover plaintext (always < 16 bytes: the batch size is
      // aligned up from the request and only the final block carries padding)
      // in buffer for the next call.
      pBackupFile->bufferLen = avail - give;
      memcpy(pBackupFile->buffer, plain + give, pBackupFile->bufferLen);
    }
  }

  return totalRead;
}

static int32_t dmBackupFileCloseRead(SDmBackupFile *pBackupFile) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pBackupFile->pFile == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  if (pBackupFile->encrypted) {
    uint8_t discard[DM_BACKUP_SM4_BLOCK_SIZE] = {0};
    while (dmBackupFileRead(pBackupFile, discard, sizeof(discard)) > 0) {
    }
    if (!pBackupFile->finalized) {
      code = terrno != 0 ? terrno : TSDB_CODE_FILE_CORRUPTED;
    }
  }
  if (taosCloseFile(&pBackupFile->pFile) != 0 && code == TSDB_CODE_SUCCESS) {
    code = terrno;
  }
  if (code == TSDB_CODE_SUCCESS) {
    sha256_final(&pBackupFile->checksumCtx, pBackupFile->checksum);
  }
  return code;
}

static int32_t dmRestoreCopyBackupFile(const char *source, const char *target, const char *password) {
  SDmBackupFile backupFile = {0};
  TdFilePtr     pTarget = NULL;
  uint8_t       buffer[64 * 1024] = {0};
  int32_t       code = dmBackupFileOpen(&backupFile, source, password);

  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pTarget = taosOpenFile(target, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pTarget == NULL) {
    code = terrno;
    goto _exit;
  }
  while (true) {
    int64_t bytesRead = dmBackupFileRead(&backupFile, buffer, sizeof(buffer));
    if (bytesRead < 0) {
      code = terrno;
      goto _exit;
    }
    if (bytesRead == 0) {
      break;
    }
    if (taosWriteFile(pTarget, buffer, bytesRead) != bytesRead) {
      code = terrno != 0 ? terrno : TSDB_CODE_FAILED;
      goto _exit;
    }
  }

_exit:
  if (pTarget != NULL && taosCloseFile(&pTarget) != 0 && code == TSDB_CODE_SUCCESS) {
    code = terrno;
  }
  int32_t closeCode = dmBackupFileCloseRead(&backupFile);
  if (code == TSDB_CODE_SUCCESS && closeCode != TSDB_CODE_SUCCESS) {
    code = closeCode;
  }
  return code;
}

static int32_t dmRestoreBackupCompare(const void *left, const void *right) {
  const SDmRestoreBackup *pLeft = left;
  const SDmRestoreBackup *pRight = right;

  if (pLeft->id < pRight->id) {
    return -1;
  }
  if (pLeft->id > pRight->id) {
    return 1;
  }
  return 0;
}
static int32_t dmRestoreDnodeIdCompare(const void *left, const void *right) {
  const int32_t leftId = *(const int32_t *)left;
  const int32_t rightId = *(const int32_t *)right;

  return leftId < rightId ? -1 : leftId > rightId;
}

static int32_t dmRestoreDnodeCompare(const void *left, const void *right) {
  const SMnodeRestoreDnode *pLeft = left;
  const SMnodeRestoreDnode *pRight = right;

  return pLeft->id < pRight->id ? -1 : pLeft->id > pRight->id;
}

static void dmRestoreAggregateFileDigests(SBackupFileDigest *digests, int32_t numOfDigests, uint8_t *checksum) {
  sha256_ctx shaCtx = {0};

  for (int32_t i = 1; i < numOfDigests; ++i) {
    SBackupFileDigest current = digests[i];
    int32_t           j = i - 1;
    while (j >= 0 && strcmp(digests[j].relPath, current.relPath) > 0) {
      digests[j + 1] = digests[j];
      --j;
    }
    digests[j + 1] = current;
  }

  sha256_init(&shaCtx);
  for (int32_t i = 0; i < numOfDigests; ++i) {
    sha256_update(&shaCtx, (const uint8_t *)digests[i].relPath, strlen(digests[i].relPath) + 1);
    sha256_update(&shaCtx, digests[i].digest, sizeof(digests[i].digest));
  }
  sha256_final(&shaCtx, checksum);
}

static int32_t dmRestoreParseDigest(const char *value, uint8_t *digest) {
  const size_t digestStringLen = sizeof("sha256:") - 1 + SHA256_DIGEST_SIZE * 2;
  if (value == NULL || strlen(value) != digestStringLen || strncmp(value, "sha256:", 7) != 0) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }

  for (int32_t i = 0; i < SHA256_DIGEST_SIZE; ++i) {
    char high = value[7 + i * 2];
    char low = value[7 + i * 2 + 1];
    int32_t highValue = isdigit(high) ? high - '0' : (high >= 'a' && high <= 'f' ? high - 'a' + 10 : -1);
    int32_t lowValue = isdigit(low) ? low - '0' : (low >= 'a' && low <= 'f' ? low - 'a' + 10 : -1);
    if (highValue < 0 || lowValue < 0) {
      return TSDB_CODE_INVALID_DATA_FMT;
    }
    digest[i] = (uint8_t)((highValue << 4) | lowValue);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t dmRestoreGetRequiredString(const SJson *pJson, const char *name, const char **value) {
  SJson *pItem = tjsonGetObjectItem(pJson, name);
  if (pItem == NULL) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  if (tjsonGetObjectValueString(pItem, (char **)value) != 0) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  if ((*value)[0] == '\0') {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t dmRestoreParseInt64(const SJson *pItem, int64_t *value) {
  char *stringValue = NULL;
  if (tjsonGetObjectValueString(pItem, &stringValue) == 0) {
    char   *end = NULL;
    int64_t parsed = taosStr2Int64(stringValue, &end, 10);
    if (end == NULL || *end != '\0') {
      return TSDB_CODE_INVALID_DATA_FMT;
    }
    *value = parsed;
    return TSDB_CODE_SUCCESS;
  }

  const cJSON *pNumber = pItem;
  if (!cJSON_IsNumber(pNumber)) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }

  int64_t parsed = (int64_t)pNumber->valuedouble;
  if ((double)parsed != pNumber->valuedouble) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  *value = parsed;
  return TSDB_CODE_SUCCESS;
}

static int32_t dmRestoreGetRequiredInt64(const SJson *pJson, const char *name, int64_t *value) {
  SJson *pItem = tjsonGetObjectItem(pJson, name);
  if (pItem == NULL) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  return dmRestoreParseInt64(pItem, value);
}

static bool dmRestoreCheckBackupVersions(const SArray *pBackups) {
  int32_t     currentVersion = 0;
  int32_t code = taosVersionStrToInt(td_version, &currentVersion);
  if (code != TSDB_CODE_SUCCESS) {
    printf("failed to parse current taosd version:%s since %s\n", td_version, tstrerror(code));
    return false;
  }

  bool compatible = true;
  printf("current taosd version: %s\n", td_version);
  for (int32_t i = 0; i < taosArrayGetSize(pBackups); ++i) {
    SDmRestoreBackup *pBackup = taosArrayGet(pBackups, i);
    int32_t           backupVersionNumber = 0;

    const char* backupVersion = NULL;
    code = dmRestoreGetRequiredString(pBackup->pManifest, "tsdbVersion", &backupVersion);
    if (code == TSDB_CODE_SUCCESS) {
      code = taosVersionStrToInt(backupVersion, &backupVersionNumber);
    }
    if (backupVersion == NULL || code != TSDB_CODE_SUCCESS) {
      printf("backup %" PRId64 " taosd version: <version cannot be parsed>\n", pBackup->id);
      compatible = false;
    } else if (taosCheckVersionCompatible(backupVersionNumber, currentVersion, 3) != TSDB_CODE_SUCCESS) {
      printf("backup %" PRId64 " taosd version: %s, maybe incompatible\n", pBackup->id, backupVersion);
      compatible = false;
    } else {
      printf("backup %" PRId64 " taosd version: %s\n", pBackup->id, backupVersion);
    }
  }

  return compatible;
}

static bool dmRestoreConfirmIncompatibleVersions(void) {
  char confirmation[16] = {0};

#ifdef WINDOWS
  printf("backup version mismatch detected, if you still want to continue, please type 'YES':\n");
#else
  printf("\033[33mBackup version mismatch detected, if you still want to continue, please type '\033[32mYES\033[33m':\033[0m\n");
#endif
  fflush(stdout);
  if (fgets(confirmation, sizeof(confirmation), stdin) == NULL) {
    return false;
  }
  confirmation[strcspn(confirmation, "\r\n")] = '\0';
  return strcmp(confirmation, "YES") == 0;
}

static bool dmRestoreConfirmDestructiveRestore(void) {
  char confirmation[16] = {0};

#ifdef WINDOWS
  printf("EXISTING DATA WILL BE DELETED AND CANNOT BE ROLLED BACK, EVEN IF RESTORE FAILS.\nTYPE 'YES' TO CONTINUE:\n");
#else
  printf("\033[31mExisting data will be deleted and cannot be rolled back, even if restore fails.\nType '\033[32mYES\033[31m' to continue:\033[0m\n");
#endif
  fflush(stdout);
  if (fgets(confirmation, sizeof(confirmation), stdin) == NULL) {
    return false;
  }
  confirmation[strcspn(confirmation, "\r\n")] = '\0';
  return strcmp(confirmation, "YES") == 0;
}

static int32_t dmRestoreClearDataDir(const char *dataDir) {
  terrno = TSDB_CODE_SUCCESS;
  TdDirPtr pDir = taosOpenDir(dataDir);
  if (pDir == NULL) {
    return terrno;
  }

  int32_t       code = TSDB_CODE_SUCCESS;
  TdDirEntryPtr pEntry = NULL;
  while (true) {
    pEntry = taosReadDir(pDir);
    if (pEntry == NULL) {
      // NULL means either end-of-directory (terrno == 0) or a read error
      // (terrno != 0); check it right here so a stale global cannot mask it.
      if (terrno != 0) {
        code = terrno;
        break;
      }
      break;
    }
    char *name = taosGetDirEntryName(pEntry);
    if (name == NULL || strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
      continue;
    }
    if (taosDirEntryIsDir(pEntry) && strcmp(name, "dnode") == 0) {
      continue;
    }

    char path[PATH_MAX] = {0};
    snprintf(path, sizeof(path), "%s%s%s", dataDir, TD_DIRSEP, name);
    if (taosDirEntryIsDir(pEntry)) {
      // taosRemoveDir recursively removes the directory tree; it returns void
      // but reports failures through terrno (via taosRemoveFile/rmdir).
      taosRemoveDir(path);
      if (terrno != 0) {
        code = terrno;
        break;
      }
    } else if ((code = taosRemoveFile(path)) != 0) {
      break;
    }
  }

  if (code == TSDB_CODE_SUCCESS && terrno != 0) {
    code = terrno;
  }
  if (taosCloseDir(&pDir) != 0 && code == TSDB_CODE_SUCCESS) {
    code = terrno;
  }
  return code;
}

static int32_t dmRestoreClearDataDirs(const SDiskCfg *pDisks, int32_t numOfDisks) {
  for (int32_t i = 0; i < numOfDisks; ++i) {
    int32_t code = dmRestoreClearDataDir(pDisks[i].dir);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t dmRestoreParseManifestDnodeIds(const SJson *pManifest, SArray **ppDnodeIds) {
  SJson  *pDnodes = tjsonGetObjectItem(pManifest, "dnodes");
  SArray *pDnodeIds = NULL;
  int32_t code = TSDB_CODE_SUCCESS;

  if (ppDnodeIds == NULL || pDnodes == NULL || !cJSON_IsArray(pDnodes) || tjsonGetArraySize(pDnodes) == 0) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  *ppDnodeIds = NULL;
  pDnodeIds = taosArrayInit(tjsonGetArraySize(pDnodes), sizeof(int32_t));
  if (pDnodeIds == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < tjsonGetArraySize(pDnodes); ++i) {
    int64_t sourceId = 0;
    code = dmRestoreParseInt64(tjsonGetArrayItem(pDnodes, i), &sourceId);
    if (code != 0 || sourceId <= 0 || sourceId > INT32_MAX) {
      code = TSDB_CODE_INVALID_DATA_FMT;
      goto _exit;
    }
    int32_t dnodeId = (int32_t)sourceId;
    for (int32_t j = 0; j < taosArrayGetSize(pDnodeIds); ++j) {
      if (*(int32_t *)taosArrayGet(pDnodeIds, j) == dnodeId) {
        code = TSDB_CODE_INVALID_DATA_FMT;
        goto _exit;
      }
    }
    if (taosArrayPush(pDnodeIds, &dnodeId) == NULL) {
      code = terrno;
      goto _exit;
    }
  }

  taosArraySort(pDnodeIds, dmRestoreDnodeIdCompare);
  *ppDnodeIds = pDnodeIds;
  return TSDB_CODE_SUCCESS;

_exit:
  taosArrayDestroy(pDnodeIds);
  return code;
}

static int32_t dmRestoreReadFileDigest(const char *path, uint8_t *digest) {
  SDmBackupFile backupFile = {0};
  char          buffer[64 * 1024] = {0};

  // always open without password, since checksum is calculated after encryption
  int32_t       code = dmBackupFileOpen(&backupFile, path, NULL);

  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FILE_CORRUPTED;
  }
  while (true) {
    int64_t bytesRead = dmBackupFileRead(&backupFile, buffer, sizeof(buffer));
    if (bytesRead < 0) {
      code = TSDB_CODE_FILE_CORRUPTED;
      break;
    }
    if (bytesRead == 0) {
      break;
    }
  }
  int32_t closeCode = dmBackupFileCloseRead(&backupFile);
  if (code == TSDB_CODE_SUCCESS && closeCode != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_FILE_CORRUPTED;
  }
  if (code == TSDB_CODE_SUCCESS) {
    memcpy(digest, backupFile.checksum, sizeof(backupFile.checksum));
  }
  return code;
}

// Read the digest of a chunked vnode backup as one continuous byte stream
// (all chunk files concatenated), which is how the checksum was computed when
// the backup was created.
static int32_t dmRestoreReadChunkedDigest(const char *backupDir, const char *baseName, int32_t numOfChunks,
                                          uint8_t *digest) {
  SDmBackupFile backupFile = {0};
  char          buffer[64 * 1024] = {0};

  // always open without password, since checksum is calculated after encryption
  int32_t       code = dmBackupFileOpenChunked(&backupFile, backupDir, baseName, numOfChunks, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FILE_CORRUPTED;
  }
  while (true) {
    int64_t bytesRead = dmBackupFileRead(&backupFile, buffer, sizeof(buffer));
    if (bytesRead < 0) {
      code = TSDB_CODE_FILE_CORRUPTED;
      break;
    }
    if (bytesRead == 0) {
      break;
    }
  }
  int32_t closeCode = dmBackupFileCloseRead(&backupFile);
  if (code == TSDB_CODE_SUCCESS && closeCode != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_FILE_CORRUPTED;
  }
  if (code == TSDB_CODE_SUCCESS) {
    memcpy(digest, backupFile.checksum, sizeof(backupFile.checksum));
  }
  return code;
}

static int32_t dmRestoreCheckRelPath(const char *relPath) {
  if (relPath == NULL || relPath[0] == '\0') {
    return TSDB_CODE_INVALID_DATA_FMT;
  }

#ifdef WINDOWS
  if (relPath[0] == '/' || relPath[0] == '\\') {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  if (((relPath[0] >= 'A' && relPath[0] <= 'Z') || (relPath[0] >= 'a' && relPath[0] <= 'z')) && relPath[1] == ':') {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
#else
  if (relPath[0] == '/') {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
#endif

  // Split on '/' (valid on every platform, including Windows) plus the
  // platform separator, so '..' / '.' segments are recognized regardless of
  // which separator a crafted path uses. On non-Windows TD_DIRSEP_CHAR is
  // '/', making this equivalent to splitting on '/' only (a lone '\' is an
  // ordinary file name character there).
  const char *pSegStart = relPath;
  for (const char *p = relPath;; ++p) {
#ifdef WINDOWS
    if (*p == '\0' || *p == '/' || *p == '\\') {
#else
    if (*p == '\0' || *p == '/') {
#endif
      int32_t segLen = (int32_t)(p - pSegStart);
      if ((segLen == 1 && pSegStart[0] == '.') || (segLen == 2 && pSegStart[0] == '.' && pSegStart[1] == '.')) {
        return TSDB_CODE_INVALID_DATA_FMT;
      }
      if (*p == '\0') {
        break;
      }
      pSegStart = p + 1;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t dmRestoreVerifyFile(const char *backupDir, const char *relPath, const char *checksum) {
  int32_t code = 0;
  char    path[PATH_MAX] = {0};
  uint8_t expected[SHA256_DIGEST_SIZE] = {0};
  uint8_t actual[SHA256_DIGEST_SIZE] = {0};

  snprintf(path, sizeof(path), "%s%s%s", backupDir, TD_DIRSEP, relPath);
  code = dmRestoreParseDigest(checksum, expected);
  if (code != 0) {
    return code;
  }
  code = dmRestoreReadFileDigest(path, actual);
  if (code != 0) {
    return code;
  }
  return memcmp(expected, actual, sizeof(expected)) == 0 ? TSDB_CODE_SUCCESS : TSDB_CODE_CHECKSUM_ERROR;
}

static int32_t dmRestoreVerifyMnode(const char *backupDir, const SJson *pManifest) {
  static const char *paths[] = {"data/sdb.data", "mnode.json", "sync/raft_config.json", "sync/raft_store.json"};
  int32_t           code = 0;
  SJson            *pMnode = tjsonGetObjectItem(pManifest, "mnode");
  const char       *checksum = NULL;
  SBackupFileDigest digests[sizeof(paths) / sizeof(paths[0])] = {0};
  uint8_t           expected[SHA256_DIGEST_SIZE] = {0};
  uint8_t           actual[SHA256_DIGEST_SIZE] = {0};

  if (pMnode == NULL) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  code = dmRestoreGetRequiredString(pMnode, "checksum", &checksum);
  if (code != 0) {
    return code;
  }
  code = dmRestoreParseDigest(checksum, expected);
  if (code != 0) {
    return code;
  }

  for (int32_t i = 0; i < sizeof(paths) / sizeof(paths[0]); ++i) {
    tstrncpy(digests[i].relPath, paths[i], sizeof(digests[i].relPath));
    char path[PATH_MAX] = {0};
    snprintf(path, sizeof(path), "%s%smnode%s%s", backupDir, TD_DIRSEP, TD_DIRSEP, paths[i]);
    code = dmRestoreReadFileDigest(path, digests[i].digest);
    if (code != 0) {
      return code;
    }
  }

  dmRestoreAggregateFileDigests(digests, sizeof(digests) / sizeof(digests[0]), actual);
  return memcmp(expected, actual, sizeof(expected)) == 0 ? TSDB_CODE_SUCCESS : TSDB_CODE_CHECKSUM_ERROR;
}

static int32_t dmRestoreVerifySnode(const char *backupDir, const SJson *pManifest) {
  SJson *pSnodes = tjsonGetObjectItem(pManifest, "snodes");
  if (pSnodes == NULL || !cJSON_IsArray(pSnodes)) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }

  for (int32_t i = 0; i < tjsonGetArraySize(pSnodes); ++i) {
    SJson      *pSnode = tjsonGetArrayItem(pSnodes, i);
    SJson      *pStreams = pSnode == NULL ? NULL : tjsonGetObjectItem(pSnode, "streams");
    const char *checksum = NULL;
    int64_t     snodeId = 0;
    int32_t     code = dmRestoreGetRequiredInt64(pSnode, "id", &snodeId);

    if (code == 0) {
      code = dmRestoreGetRequiredString(pSnode, "checksum", &checksum);
    }
    if (code != 0 || snodeId <= 0 || snodeId > INT32_MAX || pStreams == NULL || !cJSON_IsArray(pStreams)) {
      return code != 0 ? code : TSDB_CODE_INVALID_DATA_FMT;
    }

    char relPath[PATH_MAX] = {0};
    snprintf(relPath, sizeof(relPath), "snode%sdnode%" PRId64 "%ssnode.json", TD_DIRSEP, snodeId, TD_DIRSEP);
    code = dmRestoreVerifyFile(backupDir, relPath, checksum);
    if (code != 0) {
      return code;
    }

    for (int32_t j = 0; j < tjsonGetArraySize(pStreams); ++j) {
      SJson      *pStream = tjsonGetArrayItem(pStreams, j);
      const char *checkpoint = NULL;
      const char *streamChecksum = NULL;
      code = dmRestoreGetRequiredString(pStream, "checkpoint", &checkpoint);
      if (code == 0) {
        code = dmRestoreGetRequiredString(pStream, "checksum", &streamChecksum);
      }
      if (code != 0) {
        return code;
      }
      if ((code = dmRestoreCheckRelPath(checkpoint)) != 0) {
        return code;
      }
      snprintf(relPath, sizeof(relPath), "snode%sdnode%" PRId64 "%s%s", TD_DIRSEP, snodeId, TD_DIRSEP, checkpoint);
      code = dmRestoreVerifyFile(backupDir, relPath, streamChecksum);
      if (code != 0) {
        return code;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

// Verify the checksum of one vnode backup: parse the manifest entry and
// compare the recorded digest against the chunked vnode data on disk. Read
// only, so it is safe to run concurrently for different vnodes.
static int32_t dmRestoreVerifyOneVnode(const char *backupDir, const SJson *pVnode) {
  const char *checksum = NULL;
  int64_t     vgId = 0;
  int64_t     snapshotIndex = 0;
  int64_t     numOfChunks = 0;
  int32_t     code = dmRestoreGetRequiredInt64(pVnode, "id", &vgId);
  if (code != 0) {
    return code;
  }
  code = dmRestoreGetRequiredInt64(pVnode, "endVersion", &snapshotIndex);
  if (code != 0) {
    return code;
  }
  code = dmRestoreGetRequiredInt64(pVnode, "numOfChunks", &numOfChunks);
  if (code != 0) {
    return code;
  }
  code = dmRestoreGetRequiredString(pVnode, "checksum", &checksum);
  if (code != 0) {
    return code;
  }
  if (vgId <= 0 || vgId > INT32_MAX || numOfChunks <= 0 || numOfChunks > INT32_MAX) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }

  uint8_t expected[SHA256_DIGEST_SIZE] = {0};
  uint8_t actual[SHA256_DIGEST_SIZE] = {0};
  code = dmRestoreParseDigest(checksum, expected);
  if (code != 0) {
    return code;
  }
  char baseName[TSDB_FILENAME_LEN] = {0};
  snprintf(baseName, sizeof(baseName), "vnode%" PRId64, vgId);
  code = dmRestoreReadChunkedDigest(backupDir, baseName, (int32_t)numOfChunks, actual);
  if (code != 0) {
    return code;
  }
  if (memcmp(expected, actual, sizeof(expected)) != 0) {
    return TSDB_CODE_CHECKSUM_ERROR;
  }
  return TSDB_CODE_SUCCESS;
}

typedef struct {
  const char        *backupDir;
  SJson             *pVnodes;       // manifest "vnodes" array (read only)
  int32_t volatile   nextIndex;     // atomic index into pVnodes
  int32_t volatile   failedCode;    // first failure, 0 = none; atomic CAS on write
} SDmRestoreVerifyVnodeCtx;

static void *dmRestoreVerifyVnodeWorkerFp(void *pParam) {
  SDmRestoreVerifyVnodeCtx *pCtx = pParam;

  while (true) {
    if (atomic_load_32(&pCtx->failedCode) != 0) {
      break;
    }
    int32_t idx = atomic_fetch_add_32(&pCtx->nextIndex, 1);
    if (idx >= tjsonGetArraySize(pCtx->pVnodes)) {
      break;
    }
    int32_t code = dmRestoreVerifyOneVnode(pCtx->backupDir, tjsonGetArrayItem(pCtx->pVnodes, idx));
    if (code != 0) {
      atomic_val_compare_exchange_32(&pCtx->failedCode, 0, code);
      break;
    }
  }
  return NULL;
}

// Verify the checksums of all vnodes in the manifest, up to pOpt->parallel
// workers at a time. The per-vnode digest reads are independent, so they run
// concurrently; the first failure is recorded atomically and propagated.
static int32_t dmRestoreVerifyVnodes(const SDmRestoreOption *pOpt, const char *backupDir, const SJson *pManifest) {
  SJson *pVnodes = tjsonGetObjectItem(pManifest, "vnodes");
  if (pVnodes == NULL || !cJSON_IsArray(pVnodes)) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }

  int32_t code = 0;
  int32_t parallelism = TMIN(pOpt->parallel, tjsonGetArraySize(pVnodes));
  parallelism = TMAX(parallelism, 1);

  SDmRestoreVerifyVnodeCtx ctx = {0};
  ctx.backupDir = backupDir;
  ctx.pVnodes = pVnodes;

  TdThread *threads = taosMemoryCalloc(parallelism, sizeof(TdThread));
  int32_t   created = 0;
  if (threads == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
  }
  for (int32_t i = 0; i < parallelism; ++i) {
    TdThreadAttr thAttr;
    (void)taosThreadAttrInit(&thAttr);
    (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (taosThreadCreate(&threads[i], &thAttr, dmRestoreVerifyVnodeWorkerFp, &ctx) != 0) {
      code = TAOS_SYSTEM_ERROR(ERRNO);
      atomic_store_32(&ctx.failedCode, code);
      (void)taosThreadAttrDestroy(&thAttr);
      break;
    }
    (void)taosThreadAttrDestroy(&thAttr);
    ++created;
  }
  for (int32_t i = 0; i < created; ++i) {
    (void)taosThreadJoin(threads[i], NULL);
  }
  taosMemoryFree(threads);

  if (code != 0) {
    return code;
  }
  return atomic_load_32(&ctx.failedCode);
}

static int32_t dmRestoreReadManifest(const SDmRestoreOption *pOpt, const char *backupDir, SJson **ppManifest) {
  SDmBackupFile backupFile = {0};
  int32_t       code = 0;
  char          manifestPath[PATH_MAX] = {0};
  char         *content = NULL;
  int64_t       size = 0;

  snprintf(manifestPath, sizeof(manifestPath), "%s%sbackup.json", backupDir, TD_DIRSEP);
  if (taosStatFile(manifestPath, &size, NULL, NULL) < 0) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  // Encrypted manifests carry an extra IV header block plus the padding block.
  if (size <= 0 || size > 16 * 1024 * 1024 + DM_BACKUP_SM4_BLOCK_SIZE * 2) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }

  content = taosMemoryMalloc(size + 1);
  if (content == NULL) {
    return terrno;
  }
  code = dmBackupFileOpen(&backupFile, manifestPath, pOpt->hasPassword ? pOpt->password : NULL);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }
  int64_t contentLen = dmBackupFileRead(&backupFile, content, size);
  if (contentLen < 0) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _exit;
  }
  content[contentLen] = '\0';
  code = dmBackupFileCloseRead(&backupFile);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }
  *ppManifest = tjsonParse(content);
  if (*ppManifest == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
  }

_exit:
  (void)dmBackupFileCloseRead(&backupFile);
  taosMemoryFree(content);
  return code;
}

static int32_t dmRestoreVerifyBackup(const SDmRestoreOption *pOpt, const char *backupDir, const SJson *pManifest) {
  int32_t code = 0;

  code = dmRestoreVerifyMnode(backupDir, pManifest);
  if (code != 0) {
    return code;
  }
  code = dmRestoreVerifySnode(backupDir, pManifest);
  if (code != 0) {
    return code;
  }
  code = dmRestoreVerifyVnodes(pOpt, backupDir, pManifest);
  return code;
}

static int32_t dmRestoreHasMnode(const SJson *pManifest, int32_t dnodeId, const SArray *pDnodes,
                                 bool *hasMnode) {
  int32_t code = 0;
  SJson  *pMnode = NULL;
  SJson  *pManifestDnodes = NULL;

  *hasMnode = false;
  pMnode = tjsonGetObjectItem(pManifest, "mnode");
  pManifestDnodes = pMnode == NULL ? NULL : tjsonGetObjectItem(pMnode, "dnodes");
  if (pManifestDnodes == NULL) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  for (int32_t i = 0; i < tjsonGetArraySize(pManifestDnodes); ++i) {
    SJson  *pItem = tjsonGetArrayItem(pManifestDnodes, i);
    int64_t manifestDnodeId = 0;
    if (pItem == NULL) {
      return TSDB_CODE_INVALID_DATA_FMT;
    }
    code = dmRestoreParseInt64(pItem, &manifestDnodeId);
    if (code != 0) {
      return code;
    }
    if (manifestDnodeId <= 0 || manifestDnodeId > INT32_MAX) {
      return TSDB_CODE_INVALID_DATA_FMT;
    }
    int32_t mappedDnodeId = mndRestoreMapDnodeId(pDnodes, (int32_t)manifestDnodeId);
    if (mappedDnodeId < 0) {
      return TSDB_CODE_INVALID_DATA_FMT;
    }
    if (mappedDnodeId == dnodeId) {
      *hasMnode = true;
      break;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static SMnodeRestoreDnode *dmRestoreFindDnode(const SArray *pDnodes, int32_t dnodeId) {
  for (int32_t i = 0; i < taosArrayGetSize(pDnodes); ++i) {
    SMnodeRestoreDnode *pDnode = taosArrayGet(pDnodes, i);
    if (pDnode->id == dnodeId) {
      return pDnode;
    }
  }
  return NULL;
}

static int32_t dmRestoreLoadDnodes(SArray **ppDnodes, int32_t *pLocalDnodeId) {
  char    path[PATH_MAX] = {0};
  char   *content = NULL;
  int32_t contentLen = 0;
  SJson  *pRoot = NULL;
  SArray *pDnodes = NULL;
  int64_t clusterId = 0;
  int32_t code = TSDB_CODE_SUCCESS;

  *ppDnodes = NULL;
  *pLocalDnodeId = 0;
  snprintf(path, sizeof(path), "%s%sdnode%sdnode.json", tsDataDir, TD_DIRSEP, TD_DIRSEP);
  code = taosReadCfgFile(path, &content, &contentLen);
  if (code != 0) {
    goto _exit;
  }
  pRoot = tjsonParse(content);
  if (pRoot == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }
  tjsonGetNumberValue(pRoot, "clusterId", clusterId, code);
  if (code < 0 || clusterId == 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }
  tjsonGetInt32ValueFromDouble(pRoot, "dnodeId", *pLocalDnodeId, code);
  if (code < 0 || *pLocalDnodeId <= 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }
  SJson *pJsonDnodes = tjsonGetObjectItem(pRoot, "dnodes");
  if (pJsonDnodes == NULL || !cJSON_IsArray(pJsonDnodes) || tjsonGetArraySize(pJsonDnodes) == 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }
  pDnodes = taosArrayInit(tjsonGetArraySize(pJsonDnodes), sizeof(SMnodeRestoreDnode));
  if (pDnodes == NULL) {
    code = terrno;
    goto _exit;
  }
  for (int32_t i = 0; i < tjsonGetArraySize(pJsonDnodes); ++i) {
    SJson            *pJsonDnode = tjsonGetArrayItem(pJsonDnodes, i);
    SMnodeRestoreDnode dnode = {.clusterId = clusterId};

    if (pJsonDnode == NULL) {
      code = TSDB_CODE_INVALID_JSON_FORMAT;
      goto _exit;
    }
    tjsonGetInt32ValueFromDouble(pJsonDnode, "id", dnode.id, code);
    if (code < 0 || dnode.id <= 0 ||
        tjsonGetStringValue1(pJsonDnode, "fqdn", dnode.ep.fqdn, sizeof(dnode.ep.fqdn)) < 0) {
      code = TSDB_CODE_INVALID_JSON_FORMAT;
      goto _exit;
    }
    tjsonGetUInt16ValueFromDouble(pJsonDnode, "port", dnode.ep.port, code);
    if (code < 0 || dnode.ep.fqdn[0] == '\0' || dnode.ep.port == 0 || dmRestoreFindDnode(pDnodes, dnode.id) != NULL) {
      code = TSDB_CODE_INVALID_JSON_FORMAT;
      goto _exit;
    }
    if (taosArrayPush(pDnodes, &dnode) == NULL) {
      code = terrno;
      goto _exit;
    }
  }
  if (dmRestoreFindDnode(pDnodes, *pLocalDnodeId) == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }
  *ppDnodes = pDnodes;
  pDnodes = NULL;

_exit:
  taosArrayDestroy(pDnodes);
  tjsonDelete(pRoot);
  taosMemoryFree(content);
  return code;
}

static int32_t dmRestoreBuildDnodeMap(const SArray *pBackups, SArray *pDnodes) {
  SArray *pSourceDnodeIds = NULL;
  int32_t code = TSDB_CODE_SUCCESS;

  SDmRestoreBackup *pFirstBackup = taosArrayGet(pBackups, 0);
  if ((code = dmRestoreParseManifestDnodeIds(pFirstBackup->pManifest, &pSourceDnodeIds)) != 0) {
    goto _exit;
  }
  if (taosArrayGetSize(pSourceDnodeIds) != taosArrayGetSize(pDnodes)) {
    code = TSDB_CODE_INVALID_DATA_FMT;
    goto _exit;
  }
  taosArraySort(pDnodes, dmRestoreDnodeCompare);
  for (int32_t i = 0; i < taosArrayGetSize(pDnodes); ++i) {
    SMnodeRestoreDnode *pDnode = taosArrayGet(pDnodes, i);
    pDnode->sourceId = *(int32_t *)taosArrayGet(pSourceDnodeIds, i);
  }

_exit:
  taosArrayDestroy(pSourceDnodeIds);
  return code;
}

static int32_t dmRestoreAdjustMnodeSyncConfig(int32_t dnodeId, const SArray *pDnodes) {
  char   path[PATH_MAX] = {0};
  char  *content = NULL;
  char  *output = NULL;
  int32_t contentLen = 0;
  SJson *pRoot = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  bool    foundLocal = false;

  snprintf(path, sizeof(path), "%s%smnode%ssync%sraft_config.json", tsDataDir, TD_DIRSEP, TD_DIRSEP, TD_DIRSEP);
  code = taosReadCfgFile(path, &content, &contentLen);
  if (code != 0) {
    goto _exit;
  }
  pRoot = tjsonParse(content);
  if (pRoot == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }

  SJson *pRaftCfg = tjsonGetObjectItem(pRoot, "RaftCfg");
  SJson *pSyncCfg = pRaftCfg == NULL ? NULL : tjsonGetObjectItem(pRaftCfg, "SSyncCfg");
  SJson *pNodeInfo = pSyncCfg == NULL ? NULL : tjsonGetObjectItem(pSyncCfg, "nodeInfo");
  if (pNodeInfo == NULL || !cJSON_IsArray(pNodeInfo)) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }

  for (int32_t i = 0; i < tjsonGetArraySize(pNodeInfo); ++i) {
    SJson    *pNode = tjsonGetArrayItem(pNodeInfo, i);
    int32_t   nodeId = 0;

    if (pNode == NULL) {
      code = TSDB_CODE_INVALID_JSON_FORMAT;
      goto _exit;
    }
    tjsonGetNumberValue(pNode, "nodeId", nodeId, code);
    if (code < 0 || nodeId <= 0) {
      code = TSDB_CODE_INVALID_JSON_FORMAT;
      goto _exit;
    }
    int32_t mappedDnodeId = mndRestoreMapDnodeId(pDnodes, nodeId);
    if (mappedDnodeId < 0) {
      code = TSDB_CODE_INVALID_DATA_FMT;
      goto _exit;
    }
    SMnodeRestoreDnode *pDnode = dmRestoreFindDnode(pDnodes, mappedDnodeId);
    if (pDnode == NULL) {
      code = TSDB_CODE_INVALID_DATA_FMT;
      goto _exit;
    }
    tjsonDeleteItemFromObject(pNode, "nodeId");
    tjsonDeleteItemFromObject(pNode, "nodeFqdn");
    tjsonDeleteItemFromObject(pNode, "nodePort");
    tjsonDeleteItemFromObject(pNode, "clusterId");
    if ((code = tjsonAddIntegerToObject(pNode, "nodeId", mappedDnodeId)) != 0 ||
        (code = tjsonAddStringToObject(pNode, "nodeFqdn", pDnode->ep.fqdn)) != 0 ||
        (code = tjsonAddDoubleToObject(pNode, "nodePort", pDnode->ep.port)) != 0 ||
        (code = tjsonAddIntegerToObject(pNode, "clusterId", pDnode->clusterId)) != 0) {
      goto _exit;
    }
    if (mappedDnodeId == dnodeId) {
      tjsonDeleteItemFromObject(pSyncCfg, "myIndex");
      if ((code = tjsonAddDoubleToObject(pSyncCfg, "myIndex", i)) != 0) {
        goto _exit;
      }
      foundLocal = true;
    }
  }
  if (!foundLocal) {
    code = TSDB_CODE_INVALID_DATA_FMT;
    goto _exit;
  }
  output = tjsonToString(pRoot);
  if (output == NULL) {
    code = terrno;
    goto _exit;
  }
  code = taosWriteCfgFile(path, output, strlen(output));

_exit:
  taosMemoryFree(output);
  tjsonDelete(pRoot);
  taosMemoryFree(content);
  return code;
}

static int32_t dmRestoreResetMnodeRaftStore(void) {
  char    path[PATH_MAX] = {0};
  char   *content = NULL;
  char   *output = NULL;
  int32_t contentLen = 0;
  SJson  *pRoot = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t currentTerm = 0;

  snprintf(path, sizeof(path), "%s%smnode%ssync%sraft_store.json", tsDataDir, TD_DIRSEP, TD_DIRSEP, TD_DIRSEP);
  code = taosReadCfgFile(path, &content, &contentLen);
  if (code != 0) {
    goto _exit;
  }
  pRoot = tjsonParse(content);
  if (pRoot == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }
  tjsonGetNumberValue(pRoot, "current_term", currentTerm, code);
  if (code < 0 || currentTerm < 0) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }

  tjsonDeleteItemFromObject(pRoot, "vote_for_addr");
  tjsonDeleteItemFromObject(pRoot, "vote_for_vgid");
  if ((code = tjsonAddIntegerToObject(pRoot, "vote_for_addr", 0)) != 0 ||
      (code = tjsonAddDoubleToObject(pRoot, "vote_for_vgid", 0)) != 0) {
    goto _exit;
  }
  output = tjsonToString(pRoot);
  if (output == NULL) {
    code = terrno;
    goto _exit;
  }
  code = taosWriteCfgFile(path, output, strlen(output));

_exit:
  taosMemoryFree(output);
  tjsonDelete(pRoot);
  taosMemoryFree(content);
  return code;
}

static int32_t dmRestoreMnode(const SDmRestoreOption *pOpt, const char *backupDir, const SJson *pManifest,
                              int32_t dnodeId, const SArray *pDnodes) {
  static const char *paths[] = {"data/sdb.data", "mnode.json", "sync/raft_config.json", "sync/raft_store.json"};
  char                path[PATH_MAX] = {0};
  bool                hasMnode = false;

  int32_t code = dmRestoreHasMnode(pManifest, dnodeId, pDnodes, &hasMnode);
  if (code != 0 || !hasMnode) {
    return code;
  }

  snprintf(path, sizeof(path), "%s%smnode%sdata", tsDataDir, TD_DIRSEP, TD_DIRSEP);
  if ((code = taosMulMkDir(path)) != 0) {
    return code;
  }

  snprintf(path, sizeof(path), "%s%smnode%ssync", tsDataDir, TD_DIRSEP, TD_DIRSEP);
  if ((code = taosMulMkDir(path)) != 0) {
    return code;
  }

  for (int32_t i = 0; i < sizeof(paths) / sizeof(paths[0]); ++i) {
    char source[PATH_MAX] = {0};
    snprintf(source, sizeof(source), "%s%smnode%s%s", backupDir, TD_DIRSEP, TD_DIRSEP, paths[i]);
    snprintf(path, sizeof(path), "%s%smnode%s%s", tsDataDir, TD_DIRSEP, TD_DIRSEP, paths[i]);
    if (taosCheckExistFile(path) && (code = taosRemoveFile(path)) != 0) {
      return code;
    }
    code = dmRestoreCopyBackupFile(source, path, pOpt->hasPassword ? pOpt->password : NULL);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  if ((code = dmRestoreAdjustMnodeSyncConfig(dnodeId, pDnodes)) != 0) {
    return code;
  }
  if ((code = dmRestoreResetMnodeRaftStore()) != 0) {
    return code;
  }
  if ((code = mndRestoreUpdateMetadata(pDnodes)) != 0) {
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t dmRestoreRewriteSnodePeer(SJson *pRoot, const char *name, const SArray *pDnodes) {
  SJson *pPeer = tjsonGetObjectItem(pRoot, name);
  int64_t sourceId = 0;
  int32_t code = pPeer == NULL ? TSDB_CODE_INVALID_DATA_FMT : dmRestoreGetRequiredInt64(pPeer, "nodeId", &sourceId);

  if (code != 0 || sourceId < 0 || sourceId > INT32_MAX) {
    return code != 0 ? code : TSDB_CODE_INVALID_DATA_FMT;
  }
  int32_t targetId = sourceId == 0 ? 0 : mndRestoreMapDnodeId(pDnodes, (int32_t)sourceId);
  if (targetId < 0) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }

  tjsonDeleteItemFromObject(pPeer, "nodeId");
  tjsonDeleteItemFromObject(pPeer, "numOfEps");
  tjsonDeleteItemFromObject(pPeer, "eps");
  if ((code = tjsonAddIntegerToObject(pPeer, "nodeId", targetId)) != 0 ||
      (code = tjsonAddIntegerToObject(pPeer, "numOfEps", targetId == 0 ? 0 : 1)) != 0) {
    return code;
  }

  SJson *pEps = tjsonAddArrayToObject(pPeer, "eps");
  if (pEps == NULL) {
    return terrno;
  }
  if (targetId == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SMnodeRestoreDnode *pDnode = dmRestoreFindDnode(pDnodes, targetId);
  SJson              *pEp = tjsonCreateObject();
  if (pDnode == NULL || pEp == NULL) {
    tjsonDelete(pEp);
    return pDnode == NULL ? TSDB_CODE_INVALID_DATA_FMT : terrno;
  }
  if ((code = tjsonAddStringToObject(pEp, "fqdn", pDnode->ep.fqdn)) != 0 ||
      (code = tjsonAddIntegerToObject(pEp, "port", pDnode->ep.port)) != 0 ||
      (code = tjsonAddItemToArray(pEps, pEp)) != 0) {
    tjsonDelete(pEp);
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t dmRestoreRewriteSnodeConfig(const char *path, int32_t sourceId, const SArray *pDnodes) {
  char   *content = NULL;
  char   *output = NULL;
  int32_t contentLen = 0;
  int64_t configSnodeId = 0;
  int32_t code = taosReadCfgFile(path, &content, &contentLen);
  SJson  *pRoot = NULL;

  if (code != 0) {
    goto _exit;
  }
  pRoot = tjsonParse(content);
  if (pRoot == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }
  if ((code = dmRestoreGetRequiredInt64(pRoot, "snodeId", &configSnodeId)) != 0 || configSnodeId != sourceId) {
    code = code != 0 ? code : TSDB_CODE_INVALID_DATA_FMT;
    goto _exit;
  }

  int32_t targetId = mndRestoreMapDnodeId(pDnodes, sourceId);
  if (targetId < 0) {
    code = TSDB_CODE_INVALID_DATA_FMT;
    goto _exit;
  }
  tjsonDeleteItemFromObject(pRoot, "snodeId");
  if ((code = tjsonAddIntegerToObject(pRoot, "snodeId", targetId)) != 0 ||
      (code = dmRestoreRewriteSnodePeer(pRoot, "leader0", pDnodes)) != 0 ||
      (code = dmRestoreRewriteSnodePeer(pRoot, "leader1", pDnodes)) != 0 ||
      (code = dmRestoreRewriteSnodePeer(pRoot, "replica", pDnodes)) != 0) {
    goto _exit;
  }
  output = tjsonToString(pRoot);
  if (output == NULL) {
    code = terrno;
    goto _exit;
  }
  code = taosWriteCfgFile(path, output, strlen(output));

_exit:
  taosMemoryFree(content);
  taosMemoryFree(output);
  tjsonDelete(pRoot);
  return code;
}

static int32_t dmRestoreSnode(const SDmRestoreOption *pOpt, const char *backupDir, const SJson *pManifest,
                              int32_t dnodeId, const SArray *pDnodes) {
  SJson *pSnodes = tjsonGetObjectItem(pManifest, "snodes");
  char   path[PATH_MAX] = {0};
  int32_t restored = 0;

  if (pSnodes == NULL || !cJSON_IsArray(pSnodes)) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }

  for (int32_t i = 0; i < tjsonGetArraySize(pSnodes); ++i) {
    SJson  *pSnode = tjsonGetArrayItem(pSnodes, i);
    SJson  *pStreams = pSnode == NULL ? NULL : tjsonGetObjectItem(pSnode, "streams");
    int64_t sourceId = 0;
    int32_t code = dmRestoreGetRequiredInt64(pSnode, "id", &sourceId);
    if (code != 0 || sourceId <= 0 || sourceId > INT32_MAX || pStreams == NULL || !cJSON_IsArray(pStreams)) {
      return code != 0 ? code : TSDB_CODE_INVALID_DATA_FMT;
    }
    if (mndRestoreMapDnodeId(pDnodes, (int32_t)sourceId) != dnodeId) {
      continue;
    }

    printf("  restoring snode checkpoints\n");
    snprintf(path, sizeof(path), "%s%ssnode%ssnode%d", tsDataDir, TD_DIRSEP, TD_DIRSEP, dnodeId);
    if (taosMulMkDir(path) != 0) {
      return terrno;
    }
    snprintf(path, sizeof(path), "%s%ssnode%scheckpoint", tsDataDir, TD_DIRSEP, TD_DIRSEP);
    if (taosMulMkDir(path) != 0) {
      return terrno;
    }
    char source[PATH_MAX] = {0};
    snprintf(source, sizeof(source), "%s%ssnode%sdnode%" PRId64 "%ssnode.json", backupDir, TD_DIRSEP, TD_DIRSEP, sourceId, TD_DIRSEP);
    snprintf(path, sizeof(path), "%s%ssnode%ssnode%d%ssnode.json", tsDataDir, TD_DIRSEP, TD_DIRSEP, dnodeId, TD_DIRSEP);
    if ((code = dmRestoreCopyBackupFile(source, path, pOpt->hasPassword ? pOpt->password : NULL)) != 0 ||
        (code = dmRestoreRewriteSnodeConfig(path, (int32_t)sourceId, pDnodes)) != 0) {
      return code;
    }

    for (int32_t j = 0; j < tjsonGetArraySize(pStreams); ++j) {
      SJson      *pStream = tjsonGetArrayItem(pStreams, j);
      const char *checkpoint = NULL;
      if ((code = dmRestoreGetRequiredString(pStream, "checkpoint", &checkpoint)) != 0) {
        return code;
      }
      if ((code = dmRestoreCheckRelPath(checkpoint)) != 0) {
        return code;
      }
      snprintf(source, sizeof(source), "%s%ssnode%sdnode%" PRId64 "%s%s", backupDir, TD_DIRSEP, TD_DIRSEP, sourceId, TD_DIRSEP, checkpoint);
      snprintf(path, sizeof(path), "%s%ssnode%s%s", tsDataDir, TD_DIRSEP, TD_DIRSEP, checkpoint);
      if ((code = dmRestoreCopyBackupFile(source, path, pOpt->hasPassword ? pOpt->password : NULL)) != 0) {
        return code;
      }
      ++restored;
    }
  }
  printf("  restored %d snode checkpoints\n", restored);
  return TSDB_CODE_SUCCESS;
}

static int32_t dmRestoreReadVnodeConfig(const SDmRestoreOption *pOpt, const char *backupDir, int32_t vgId,
                                        int32_t numOfChunks, SVnodeCfg *pCfg) {
  char                   baseName[TSDB_FILENAME_LEN] = {0};
  SDmBackupFile          backupFile = {0};
  SBackupVnodeDataHeader backupHeader = {0};
  SSnapDataHdr           snapHeader = {0};
  SVnodeInfo             info = {0};
  uint8_t               *data = NULL;
  int32_t                code = TSDB_CODE_SUCCESS;

  snprintf(baseName, sizeof(baseName), "vnode%d", vgId);
  code = dmBackupFileOpenChunked(&backupFile, backupDir, baseName, numOfChunks,
                                 pOpt->hasPassword ? pOpt->password : NULL);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (dmBackupFileRead(&backupFile, &backupHeader, sizeof(backupHeader)) != sizeof(backupHeader) ||
      backupHeader.magic != BACKUP_VNODE_DATA_MAGIC ||
      dmBackupFileRead(&backupFile, &snapHeader, sizeof(snapHeader)) != sizeof(snapHeader) || snapHeader.type != SNAP_DATA_CFG ||
      snapHeader.size <= 0 || snapHeader.size > INT32_MAX) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _exit;
  }

  data = taosMemoryCalloc(1, snapHeader.size + 1);
  if (data == NULL) {
    code = terrno;
    goto _exit;
  }
  if (dmBackupFileRead(&backupFile, data, snapHeader.size) != snapHeader.size || data[snapHeader.size - 1] != '\0') {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _exit;
  }
  if ((code = vnodeDecodeInfo(data, &info)) != 0) {
    goto _exit;
  }
  if (info.config.vgId != vgId) {
    code = TSDB_CODE_INVALID_DATA_FMT;
  } else {
    *pCfg = info.config;
  }

_exit:
  taosMemoryFree(data);
  // NOTE: do NOT use dmBackupFileCloseRead here. For an encrypted chunked
  // backup it would drain the whole file (decrypt every remaining chunk,
  // possibly tens of GB) just to validate the trailing padding and compute a
  // checksum we do not need: this function only reads the leading config
  // block. Integrity is verified by the (independently skippable) verify
  // stage, and dmRestoreVnodeData validates the padding when it reads the
  // file to the end. Just close the handle.
  if (backupFile.pFile != NULL) {
    (void)taosCloseFile(&backupFile.pFile);
  }
  return code;
}

static void dmRestorePrintVnodeProgress(int32_t vgId, int64_t bytes) {
  if (bytes < 1024) {
    printf("    vnode %d: %" PRId64 " Bytes restored\n", vgId, bytes);
  } else if (bytes < 1024 * 1024) {
    printf("    vnode %d: %.1f KiB restored\n", vgId, (double)bytes / 1024);
  } else if (bytes < 1024 * 1024 * 1024) {
    printf("    vnode %d: %.1f MiB restored\n", vgId, (double)bytes / (1024 * 1024));
  } else {
    printf("    vnode %d: %.1f GiB restored\n", vgId, (double)bytes / (1024 * 1024 * 1024));
  }
}

static int32_t dmRestoreInitVnodeRaftStore(SVnode *pVnode) {
  char   vnodeDir[TSDB_FILENAME_LEN] = {0};
  char   syncDir[TSDB_FILENAME_LEN] = {0};
  char   path[TSDB_FILENAME_LEN] = {0};
  char  *output = NULL;
  SJson *pRoot = NULL;
  int32_t code = TSDB_CODE_SUCCESS;

  if (pVnode->state.commitTerm < 0) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }

  vnodeGetPrimaryPath(pVnode, false, vnodeDir, sizeof(vnodeDir));
  snprintf(syncDir, sizeof(syncDir), "%s%ssync", vnodeDir, TD_DIRSEP);
  if ((code = taosMulMkDir(syncDir)) != 0) {
    goto _exit;
  }
  snprintf(path, sizeof(path), "%s%sraft_store.json", syncDir, TD_DIRSEP);

  pRoot = tjsonCreateObject();
  if (pRoot == NULL) {
    code = terrno;
    goto _exit;
  }
  if ((code = tjsonAddIntegerToObject(pRoot, "current_term", pVnode->state.commitTerm)) != 0 ||
      (code = tjsonAddIntegerToObject(pRoot, "vote_for_addr", 0)) != 0 ||
      (code = tjsonAddDoubleToObject(pRoot, "vote_for_vgid", 0)) != 0) {
    goto _exit;
  }
  output = tjsonToString(pRoot);
  if (output == NULL) {
    code = terrno;
    goto _exit;
  }
  code = taosWriteCfgFile(path, output, strlen(output));

_exit:
  taosMemoryFree(output);
  tjsonDelete(pRoot);
  return code;
}

static int32_t dmRestoreVnodeData(const SDmRestoreOption *pOpt, const char *backupDir, int32_t vgId,
                                  int32_t numOfChunks, const char *vnodeDir, STfs *pTfs) {
  int32_t                code = TSDB_CODE_SUCCESS;
  char                   baseName[TSDB_FILENAME_LEN] = {0};
  SDmBackupFile          backupFile = {0};
  SVnode                *pVnode = NULL;
  SVSnapWriter          *pWriter = NULL;
  SBackupVnodeDataHeader backupHeader = {0};
  SSnapshotParam         param = {0};
  SSnapshot              snapshot = {0};
  int64_t                restoredBytes = 0;
  int64_t                nextProgressBytes = DM_RESTORE_PROGRESS_INTERVAL;

  snprintf(baseName, sizeof(baseName), "vnode%d", vgId);
  code = dmBackupFileOpenChunked(&backupFile, backupDir, baseName, numOfChunks,
                                 pOpt->hasPassword ? pOpt->password : NULL);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (dmBackupFileRead(&backupFile, &backupHeader, sizeof(backupHeader)) != sizeof(backupHeader) ||
      backupHeader.magic != BACKUP_VNODE_DATA_MAGIC) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _exit;
  }

  pVnode = vnodeOpen(vnodeDir, 0, pTfs, NULL, (SMsgCb){0}, true);
  if (pVnode == NULL) {
    code = terrno;
    goto _exit;
  }
  param.start = 0;
  param.end = backupHeader.snapshotIndex;
  if ((code = vnodeSnapWriterOpen(pVnode, &param, &pWriter)) != 0) {
    goto _exit;
  }

  while (true) {
    SSnapDataHdr snapHeader = {0};
    int64_t      bytesRead = dmBackupFileRead(&backupFile, &snapHeader, sizeof(snapHeader));
    if (bytesRead == 0) {
      break;
    }
    if (bytesRead != sizeof(snapHeader) || snapHeader.size < 0 ||
        snapHeader.size > UINT32_MAX - sizeof(SSnapDataHdr)) {
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _exit;
    }

    uint32_t dataLen = sizeof(SSnapDataHdr) + (uint32_t)snapHeader.size;
    uint8_t *data = taosMemoryMalloc(dataLen);
    if (data == NULL) {
      code = terrno;
      goto _exit;
    }
    (void)memcpy(data, &snapHeader, sizeof(snapHeader));
    if (snapHeader.size > 0 &&
      dmBackupFileRead(&backupFile, data + sizeof(snapHeader), snapHeader.size) != snapHeader.size) {
      taosMemoryFree(data);
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _exit;
    }
    code = vnodeSnapWrite(pWriter, data, dataLen);
    taosMemoryFree(data);
    if (code != 0) {
      goto _exit;
    }
    restoredBytes += dataLen;
    if (restoredBytes >= nextProgressBytes) {
      dmRestorePrintVnodeProgress(vgId, restoredBytes);
      nextProgressBytes = (restoredBytes / DM_RESTORE_PROGRESS_INTERVAL + 1) * DM_RESTORE_PROGRESS_INTERVAL;
    }
  }

_exit:
  if (pWriter != NULL) {
    int32_t closeCode = vnodeSnapWriterClose(pWriter, code != 0, &snapshot);
    if (code == 0 && closeCode != 0) {
      code = closeCode;
    }
  }
  if (code == 0 && pVnode != NULL) {
    code = dmRestoreInitVnodeRaftStore(pVnode);
  }
  if (pVnode != NULL) {
    vnodeClose(pVnode);
  }
  int32_t closeCode = dmBackupFileCloseRead(&backupFile);
  if (code == TSDB_CODE_SUCCESS && closeCode != TSDB_CODE_SUCCESS) {
    code = closeCode;
  }
  if (code == TSDB_CODE_SUCCESS) {
    dmRestorePrintVnodeProgress(vgId, restoredBytes);
  }
  return code;
}

typedef struct {
  int32_t vgId;
  int32_t numOfChunks;
} SDmRestoreLocalVnode;

// Register all restored vnodes in vnodes.json in one pass (single read-modify-
// write of the shared file). The caller has already restored every vnode's
// data; pLocalVnodes holds the (vgId, numOfChunks) list of this dnode.
static int32_t dmRestoreRegisterVnodes(const SArray *pLocalVnodes) {
  char    path[PATH_MAX] = {0};
  char   *content = NULL;
  char   *output = NULL;
  int32_t contentLen = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  SJson  *pRoot = NULL;
  SJson  *pVnodes = NULL;

  snprintf(path, sizeof(path), "%s%svnode%svnodes.json", tsDataDir, TD_DIRSEP, TD_DIRSEP);
  if (taosStatFile(path, NULL, NULL, NULL) < 0) {
    if (!errorIsFileNotExist(terrno)) {
      return terrno;
    }
    pRoot = tjsonCreateObject();
    pVnodes = pRoot == NULL ? NULL : tjsonAddArrayToObject(pRoot, "vnodes");
  } else {
    code = taosReadCfgFile(path, &content, &contentLen);
    if (code != 0) {
      goto _exit;
    }
    pRoot = tjsonParse(content);
    pVnodes = pRoot == NULL ? NULL : tjsonGetObjectItem(pRoot, "vnodes");
  }
  if (pRoot == NULL || pVnodes == NULL || !cJSON_IsArray(pVnodes)) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }

  // Append every restored vnode, skipping any vgId that is already registered.
  for (int32_t i = 0; i < taosArrayGetSize(pLocalVnodes); ++i) {
    SDmRestoreLocalVnode *pVnode = taosArrayGet(pLocalVnodes, i);
    int32_t vgId = pVnode->vgId;

    bool alreadyRegistered = false;
    for (int32_t j = 0; j < tjsonGetArraySize(pVnodes); ++j) {
      int64_t registeredVgId = 0;
      code = dmRestoreGetRequiredInt64(tjsonGetArrayItem(pVnodes, j), "vgId", &registeredVgId);
      if (code != 0) {
        goto _exit;
      }
      if (registeredVgId == vgId) {
        alreadyRegistered = true;
        break;
      }
    }
    if (alreadyRegistered) {
      continue;
    }

    SJson *pVnodeJson = tjsonCreateObject();
    if (pVnodeJson == NULL || tjsonAddDoubleToObject(pVnodeJson, "vgId", vgId) != 0 ||
        tjsonAddDoubleToObject(pVnodeJson, "dropped", 0) != 0 ||
        tjsonAddDoubleToObject(pVnodeJson, "vgVersion", 0) != 0 ||
        tjsonAddDoubleToObject(pVnodeJson, "diskPrimary", 0) != 0 ||
        tjsonAddDoubleToObject(pVnodeJson, "toVgId", 0) != 0 ||
        tjsonAddIntegerToObject(pVnodeJson, "mountId", 0) != 0 ||
        tjsonAddItemToArray(pVnodes, pVnodeJson) != 0) {
      tjsonDelete(pVnodeJson);
      code = terrno;
      goto _exit;
    }
  }

  output = tjsonToString(pRoot);
  if (output == NULL) {
    code = terrno;
    goto _exit;
  }
  code = taosWriteCfgFile(path, output, strlen(output));

_exit:
  taosMemoryFree(output);
  tjsonDelete(pRoot);
  taosMemoryFree(content);
  return code;
}

static int32_t dmRestoreAdjustVnodeConfig(SVnodeCfg *pCfg, int32_t dnodeId, const SArray *pDnodes) {
  SSyncCfg *pSyncCfg = &pCfg->syncCfg;
  bool      foundLocal = false;

  if (pSyncCfg->totalReplicaNum <= 0 || pSyncCfg->totalReplicaNum > TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  for (int32_t i = 0; i < pSyncCfg->totalReplicaNum; ++i) {
    SNodeInfo *pNode = &pSyncCfg->nodeInfo[i];
    if (pNode->nodeId <= 0) {
      return TSDB_CODE_INVALID_DATA_FMT;
    }
    int32_t mappedDnodeId = mndRestoreMapDnodeId(pDnodes, pNode->nodeId);
    if (mappedDnodeId < 0) {
      return TSDB_CODE_INVALID_DATA_FMT;
    }
    SMnodeRestoreDnode *pDnode = dmRestoreFindDnode(pDnodes, mappedDnodeId);
    if (pDnode == NULL) {
      return TSDB_CODE_INVALID_DATA_FMT;
    }
    pNode->nodeId = mappedDnodeId;
    tstrncpy(pNode->nodeFqdn, pDnode->ep.fqdn, sizeof(pNode->nodeFqdn));
    pNode->nodePort = pDnode->ep.port;
    pNode->clusterId = pDnode->clusterId;
    if (mappedDnodeId == dnodeId) {
      pSyncCfg->myIndex = i;
      foundLocal = true;
    }
  }
  return foundLocal ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_DATA_FMT;
}

// Collect the vnodes that belong to this dnode (vgId + numOfChunks) from the
// manifest in one pass. The manifest is read-only; the returned array is
// consumed by the parallel workers.
static int32_t dmRestoreCollectLocalVnodes(const SJson *pVnodes, int32_t dnodeId, const SArray *pDnodes,
                                           SArray **ppLocalVnodes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SArray *pLocalVnodes = taosArrayInit(tjsonGetArraySize(pVnodes), sizeof(SDmRestoreLocalVnode));
  if (pLocalVnodes == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < tjsonGetArraySize(pVnodes); ++i) {
    SJson   *pVnode = tjsonGetArrayItem(pVnodes, i);
    SJson   *pVnodeDnodes = pVnode == NULL ? NULL : tjsonGetObjectItem(pVnode, "dnodes");
    int64_t  manifestVgId = 0;
    int64_t  manifestNumOfChunks = 0;
    bool     isLocal = false;
    code = dmRestoreGetRequiredInt64(pVnode, "id", &manifestVgId);
    if (code != 0) {
      goto _exit;
    }
    code = dmRestoreGetRequiredInt64(pVnode, "numOfChunks", &manifestNumOfChunks);
    if (code != 0 || manifestNumOfChunks <= 0 || manifestNumOfChunks > INT32_MAX) {
      code = code != 0 ? code : TSDB_CODE_INVALID_DATA_FMT;
      goto _exit;
    }
    if (pVnodeDnodes == NULL || !cJSON_IsArray(pVnodeDnodes)) {
      code = TSDB_CODE_INVALID_DATA_FMT;
      goto _exit;
    }
    for (int32_t j = 0; j < tjsonGetArraySize(pVnodeDnodes); ++j) {
      int64_t manifestDnodeId = 0;
      code = dmRestoreParseInt64(tjsonGetArrayItem(pVnodeDnodes, j), &manifestDnodeId);
      if (code != 0) {
        goto _exit;
      }
      if (manifestDnodeId <= 0 || manifestDnodeId > INT32_MAX) {
        code = TSDB_CODE_INVALID_DATA_FMT;
        goto _exit;
      }
      int32_t mappedDnodeId = mndRestoreMapDnodeId(pDnodes, (int32_t)manifestDnodeId);
      if (mappedDnodeId < 0) {
        code = TSDB_CODE_INVALID_DATA_FMT;
        goto _exit;
      }
      if (mappedDnodeId == dnodeId) {
        isLocal = true;
        break;
      }
    }
    if (!isLocal) {
      continue;
    }

    SDmRestoreLocalVnode local = {.vgId = (int32_t)manifestVgId, .numOfChunks = (int32_t)manifestNumOfChunks};
    if (taosArrayPush(pLocalVnodes, &local) == NULL) {
      code = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

  *ppLocalVnodes = pLocalVnodes;
  return TSDB_CODE_SUCCESS;

_exit:
  taosArrayDestroy(pLocalVnodes);
  return code;
}

typedef struct {
  const SDmRestoreOption *pOpt;
  const char             *backupDir;
  int32_t                 dnodeId;
  const SArray           *pDnodes;
  STfs                   *pTfs;
  SArray                 *pLocalVnodes;   // SDmRestoreLocalVnode: vnodes to restore
  int32_t volatile        nextIndex;       // shared atomic index into pLocalVnodes
  int32_t volatile        restored;        // shared atomic progress counter
  int32_t volatile        failedCode;      // first failure, 0 = none; atomic CAS on write
} SDmRestoreVnodeCtx;

// Restore one local vnode: read config, adjust it, create the vnode, and write
// the snapshot data. Registration in vnodes.json is deferred until every
// worker has finished (see dmRestoreVnodes), so no shared state is touched
// here and the workers need no mutex.
static int32_t dmRestoreOneVnode(SDmRestoreVnodeCtx *pCtx, SDmRestoreLocalVnode *pVnode) {
  int32_t code = TSDB_CODE_SUCCESS;

  SVnodeCfg cfg = {0};
  code = dmRestoreReadVnodeConfig(pCtx->pOpt, pCtx->backupDir, pVnode->vgId, pVnode->numOfChunks, &cfg);
  if (code != 0) {
    return code;
  }
  code = dmRestoreAdjustVnodeConfig(&cfg, pCtx->dnodeId, pCtx->pDnodes);
  if (code != 0) {
    return code;
  }
  char vnodeRoot[PATH_MAX] = {0};
  char vnodeDir[TSDB_FILENAME_LEN] = {0};
  snprintf(vnodeRoot, sizeof(vnodeRoot), "%s%svnode", tsDataDir, TD_DIRSEP);
  if ((code = taosMulMkDir(vnodeRoot)) != 0) {
    return code;
  }
  snprintf(vnodeDir, sizeof(vnodeDir), "vnode%svnode%d", TD_DIRSEP, pVnode->vgId);
  if ((code = vnodeCreate(vnodeDir, &cfg, 0, pCtx->pTfs)) != 0) {
    return code;
  }
  if ((code = dmRestoreVnodeData(pCtx->pOpt, pCtx->backupDir, pVnode->vgId, pVnode->numOfChunks, vnodeDir,
                                 pCtx->pTfs)) != 0) {
    return code;
  }
  return code;
}

static void *dmRestoreVnodeWorkerFp(void *pParam) {
  SDmRestoreVnodeCtx *pCtx = pParam;

  while (true) {
    if (atomic_load_32(&pCtx->failedCode) != 0) {
      break;
    }
    int32_t total = taosArrayGetSize(pCtx->pLocalVnodes);
    int32_t idx = atomic_fetch_add_32(&pCtx->nextIndex, 1);
    if (idx >= total) {
      break;
    }
    SDmRestoreLocalVnode *pVnode = taosArrayGet(pCtx->pLocalVnodes, idx);
    int32_t seq = atomic_fetch_add_32(&pCtx->restored, 1) + 1;
    printf("  restoring vnode %d (%d/%d)\n", pVnode->vgId, seq, total);

    int32_t code = dmRestoreOneVnode(pCtx, pVnode);
    if (code != 0) {
      printf("  failed to restore vnode %d: %s\n", pVnode->vgId, tstrerror(code));
      // Record the first failure atomically; a concurrent failing worker
      // keeps the already-stored code.
      atomic_val_compare_exchange_32(&pCtx->failedCode, 0, code);
      break;
    }
    printf("  vnode %d restored\n", pVnode->vgId);
  }
  return NULL;
}

static int32_t dmRestoreVnodes(const SDmRestoreOption *pOpt, const char *backupDir, const SJson *pManifest,
                               int32_t dnodeId, const SArray *pDnodes, STfs *pTfs) {
  SJson   *pVnodes = tjsonGetObjectItem(pManifest, "vnodes");
  if (pVnodes == NULL || !cJSON_IsArray(pVnodes)) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }

  SArray  *pLocalVnodes = NULL;
  int32_t  code = dmRestoreCollectLocalVnodes(pVnodes, dnodeId, pDnodes, &pLocalVnodes);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (taosArrayGetSize(pLocalVnodes) == 0) {
    taosArrayDestroy(pLocalVnodes);
    return TSDB_CODE_SUCCESS;
  }

  // Parallel: one worker per slot, vnodes taken from a shared atomic index.
  // vnodeCreate/vnodeOpen/dmRestoreVnodeData operate on per-vnode dirs and
  // are safe to run concurrently; vnodes.json registration is deferred until
  // all workers have joined, so the workers need no shared mutex.
  int32_t parallelism = TMIN(pOpt->parallel, (int32_t)taosArrayGetSize(pLocalVnodes));
  parallelism = TMAX(parallelism, 1);

  SDmRestoreVnodeCtx ctx = {0};
  ctx.pOpt = pOpt;
  ctx.backupDir = backupDir;
  ctx.dnodeId = dnodeId;
  ctx.pDnodes = pDnodes;
  ctx.pTfs = pTfs;
  ctx.pLocalVnodes = pLocalVnodes;

  TdThread *threads = taosMemoryCalloc(parallelism, sizeof(TdThread));
  int32_t   created = 0;
  if (threads == NULL) {
    code = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    taosArrayDestroy(pLocalVnodes);
    return code;
  }
  for (int32_t i = 0; i < parallelism; ++i) {
    TdThreadAttr thAttr;
    (void)taosThreadAttrInit(&thAttr);
    (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (taosThreadCreate(&threads[i], &thAttr, dmRestoreVnodeWorkerFp, &ctx) != 0) {
      code = TAOS_SYSTEM_ERROR(ERRNO);
      atomic_store_32(&ctx.failedCode, code);
      (void)taosThreadAttrDestroy(&thAttr);
      break;
    }
    (void)taosThreadAttrDestroy(&thAttr);
    ++created;
  }
  for (int32_t i = 0; i < created; ++i) {
    (void)taosThreadJoin(threads[i], NULL);
  }
  taosMemoryFree(threads);

  if (code != 0) {
    taosArrayDestroy(pLocalVnodes);
    return code;
  }

  code = atomic_load_32(&ctx.failedCode);
  if (code != 0) {
    taosArrayDestroy(pLocalVnodes);
    return code;
  }

  // Register all restored vnode in vnodes.json
  code = dmRestoreRegisterVnodes(pLocalVnodes);
  taosArrayDestroy(pLocalVnodes);
  return code;
}

static SArray* dmRestoreCollectBackups(const SDmRestoreOption *pOpt) {
  SArray   *pBackups = taosArrayInit(4, sizeof(SDmRestoreBackup));
  if (pBackups == NULL) {
    return NULL;
  }

  TdDirPtr pDir = taosOpenDir(pOpt->backupChainPath);
  if (pDir == NULL) {
    goto _exit;
  }

  TdDirEntryPtr pEntry = NULL;
  while (true) {
    pEntry = taosReadDir(pDir);
    if (pEntry == NULL) {
      // NULL means either end-of-directory (terrno == 0) or a read error
      // (terrno != 0); check it right here so loop-body operations or a
      // stale global cannot mask a real read failure.
      if (terrno != 0) {
        goto _exit;
      }
      break;
    }
    char *name = taosGetDirEntryName(pEntry);
    if (name == NULL || !taosDirEntryIsDir(pEntry)) {
      continue;
    }

    char  *end = NULL;
    int64_t id = taosStr2Int64(name, &end, 10);
    if (end == NULL || *end != '\0') {
      continue;
    }

    if (pOpt->lastBackupId != 0 && id > pOpt->lastBackupId) {
      continue;
    }

    SDmRestoreBackup backup = {.id = id};
    if (taosArrayPush(pBackups, &backup) == NULL) {
      goto _exit;
    }
  }

  if (taosCloseDir(&pDir) != 0) {
    goto _exit;
  }
  if (taosArrayGetSize(pBackups) == 0) {
    terrno = TSDB_CODE_NOT_FOUND;
    goto _exit;
  }

  taosArraySort(pBackups, dmRestoreBackupCompare);
  return pBackups;

_exit:
  if (pDir != NULL) {
    (void)taosCloseDir(&pDir);
  }
  taosArrayDestroy(pBackups);
  return NULL;
}

static int32_t dmVerifyRestoreBackupChain(const SDmRestoreOption *pOpt, SArray* pBackups) {
  int32_t code = 0;
  int64_t lastBackupId = 0;
  SArray *pFirstDnodeIds = NULL;
  int32_t numOfBackups = taosArrayGetSize(pBackups);

  for (int32_t i = 0; i < taosArrayGetSize(pBackups); ++i) {
    SDmRestoreBackup *pBackup = taosArrayGet(pBackups, i);
    SJson            *pManifest = NULL;

    char backupDir[PATH_MAX] = {0};
    snprintf(backupDir, sizeof(backupDir), "%s%s%" PRId64, pOpt->backupChainPath, TD_DIRSEP, pBackup->id);

    if (pOpt->verify == 0) {
      printf("[verify %d/%d] backup %" PRId64 ": checksum verification skipped\n", i + 1, numOfBackups,
             pBackup->id);
    } else {
      printf("[verify %d/%d] backup %" PRId64 ": verifying checksums\n", i + 1, numOfBackups, pBackup->id);
    }

    code = dmRestoreReadManifest(pOpt, backupDir, &pManifest);
    if (code != 0) {
      printf("failed to read backup manifest for '%s': %s\n", backupDir, tstrerror(code));
      break;
    }

    if (i > 0) {
      int64_t previous = 0;
      code = dmRestoreGetRequiredInt64(pManifest, "previous", &previous);
      if (code != 0) {
        printf("failed to read previous backup link in manifest '%s': %s\n", backupDir, tstrerror(code));
        tjsonDelete(pManifest);
        break;
      }
      if (previous != lastBackupId) {
        printf("invalid previous backup link in manifest '%s'\n", backupDir);
        tjsonDelete(pManifest);
        code = TSDB_CODE_INVALID_DATA_FMT;
        break;
      }
    }

    if (pOpt->verify != 0) {
      code = dmRestoreVerifyBackup(pOpt, backupDir, pManifest);
      if (code != 0) {
        printf("backup integrity check failed for '%s': %s\n", backupDir, tstrerror(code));
        tjsonDelete(pManifest);
        break;
      }
    }
    SArray *pDnodeIds = NULL;
    code = dmRestoreParseManifestDnodeIds(pManifest, &pDnodeIds);
    if (code != 0) {
      printf("failed to parse dnode IDs from manifest '%s': %s\n", backupDir, tstrerror(code));
      tjsonDelete(pManifest);
      break;
    }
    if (pFirstDnodeIds == NULL) {
      pFirstDnodeIds = pDnodeIds;
      pDnodeIds = NULL;
    } else {
      size_t size1 = taosArrayGetSize(pFirstDnodeIds);
      size_t size2 = taosArrayGetSize(pDnodeIds);
      void* p1 = TARRAY_GET_ELEM(pFirstDnodeIds, 0);
      void* p2 = TARRAY_GET_ELEM(pDnodeIds, 0);
      if (size1 != size2 || memcmp(p1, p2, size1 * sizeof(int32_t)) != 0) {
        printf("dnode IDs in manifest '%s' do not match the first backup in the chain\n", backupDir);
        taosArrayDestroy(pDnodeIds);
        tjsonDelete(pManifest);
        code = TSDB_CODE_INVALID_DATA_FMT;
        break;
      }
    }
    taosArrayDestroy(pDnodeIds);

    pBackup->pManifest = pManifest;
    lastBackupId = pBackup->id;
    printf("[verify %d/%d] backup %" PRId64 ": verified\n", i + 1, numOfBackups, pBackup->id);
  }

  taosArrayDestroy(pFirstDnodeIds);
  return code;
}

static int32_t dmRestoreFromBackup(const SDmRestoreOption *pOpt, SDmRestoreBackup *pBackup, int32_t localDnodeId,
                                   const SArray *pDnodes, STfs *pTfs) {
  char backupDir[PATH_MAX] = {0};
  snprintf(backupDir, sizeof(backupDir), "%s%s%" PRId64, pOpt->backupChainPath, TD_DIRSEP, pBackup->id);

  printf("  restoring mnode data\n");
  int32_t code = dmRestoreMnode(pOpt, backupDir, pBackup->pManifest, localDnodeId, pDnodes);
  if (code != 0) {
    return code;
  }
  printf("  mnode data restored\n");
  code = dmRestoreSnode(pOpt, backupDir, pBackup->pManifest, localDnodeId, pDnodes);
  if (code != 0) {
    return code;
  }
  return dmRestoreVnodes(pOpt, backupDir, pBackup->pManifest, localDnodeId, pDnodes, pTfs);
}

int32_t dmRestoreFromBackupChain(const SDmRestoreOption *pOpt) {
  SArray* pBackups = dmRestoreCollectBackups(pOpt);
  SArray* pDnodes = NULL;
  STfs*   pTfs = NULL;
  int32_t localDnodeId = 0;
  int32_t code = 0;
  bool    walInited = false;

  if (pBackups == NULL) {
    if (terrno == 0) {
      terrno = TSDB_CODE_NOT_FOUND;
    }
    printf("failed to read backup chain '%s': %s\n", pOpt->backupChainPath, tstrerror(terrno));
    return terrno;
  }

  int32_t numOfBackups = taosArrayGetSize(pBackups);
  SDmRestoreBackup *pFirstBackup = taosArrayGet(pBackups, 0);
  SDmRestoreBackup *pLastBackup = taosArrayGet(pBackups, numOfBackups - 1);
  printf("found %d backups, restoring from %" PRId64 " to %" PRId64 "\n", numOfBackups, pFirstBackup->id,
         pLastBackup->id);

  code = dmVerifyRestoreBackupChain(pOpt, pBackups);
  if (code != 0) {
    printf("failed to verify backup chain '%s': %s\n", pOpt->backupChainPath, tstrerror(code));
    goto _exit;
  }
  bool compatible = dmRestoreCheckBackupVersions(pBackups);
  printf("backup chain '%s' verified successfully\n", pOpt->backupChainPath);
  if (pOpt->verify == 2) {
    if (!compatible) {
      printf("backup version check failed: incompatible backup versions\n");
    }
    goto _exit;
  }
  if (!compatible && !dmRestoreConfirmIncompatibleVersions()) {
    code = TSDB_CODE_VERSION_NOT_COMPATIBLE;
    goto _exit;
  }
  if (!dmRestoreConfirmDestructiveRestore()) {
    code = TSDB_CODE_RESTORE_CANCELLED;
    goto _exit;
  }

  SDiskCfg fallbackDisk = {.level = 0, .primary = 1, .disable = 0};
  SDiskCfg *pDisks = tsDiskCfg;
  int32_t   numOfDisks = tsDiskCfgNum;
  if (numOfDisks <= 0 || pDisks == NULL) {
    tstrncpy(fallbackDisk.dir, tsDataDir, TSDB_FILENAME_LEN);
    pDisks = &fallbackDisk;
    numOfDisks = 1;
  }
  printf("clearing existing data directories\n");
  code = dmRestoreClearDataDirs(pDisks, numOfDisks);
  if (code != TSDB_CODE_SUCCESS) {
    printf("failed to clear existing data: %s\n", tstrerror(code));
    goto _exit;
  }
  code = tfsOpen(pDisks, numOfDisks, &pTfs);
  if (code != 0) {
    goto _exit;
  }
  code = dmRestoreLoadDnodes(&pDnodes, &localDnodeId);
  if (code != 0) {
    printf("failed to read dnode configuration '%s%sdnode%sdnode.json'.\n", tsDataDir, TD_DIRSEP, TD_DIRSEP);
    goto _exit;
  }
  code = dmRestoreBuildDnodeMap(pBackups, pDnodes);
  if (code != 0) {
    printf("failed to map source and target dnodes.\n");
    goto _exit;
  }

  printf("initializing WAL\n");
  code = walInit(NULL);
  if (code != 0) {
    printf("failed to initialize wal for restore: %s\n", tstrerror(code));
    goto _exit;
  }
  walInited = true;

  for (int32_t i = 0; i < taosArrayGetSize(pBackups); ++i) {
    SDmRestoreBackup *pBackup = taosArrayGet(pBackups, i);
    printf("restoring backup %" PRId64 " (%d/%d)\n", pBackup->id, i + 1, numOfBackups);
    code = dmRestoreFromBackup(pOpt, pBackup, localDnodeId, pDnodes, pTfs);
    if (code != 0) {
      printf("failed to restore from backup '%" PRId64 "'.\n", pBackup->id);
      goto _exit;
    }
    printf("backup %" PRId64 " restored\n", pBackup->id);
  }

_exit:
  for (int32_t i = 0; i < taosArrayGetSize(pBackups); ++i) {
    SDmRestoreBackup *pBackup = taosArrayGet(pBackups, i);
    if (pBackup->pManifest != NULL) {
      tjsonDelete(pBackup->pManifest);
      pBackup->pManifest = NULL;
    }
  }
  taosArrayDestroy(pBackups);
  taosArrayDestroy(pDnodes);
  tfsClose(pTfs);
  if (walInited) {
    walCleanUp();
  }

  if (code == 0) {
    printf("restore completed successfully\n");
  } else {
    printf("restore failed: %s\n", tstrerror(code));
  }

  return code;
}
