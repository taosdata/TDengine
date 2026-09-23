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
#include "libs/crypt/crypt.h"
#include "libs/sm4/sm4.h"
#include "tencrypt.h"

typedef struct {
  char    name[TSDB_DB_FNAME_LEN];
  SArray *vgIds;  // int32_t
} SBackupDatabaseInfo;

typedef struct {
  SDnodeMgmt       *pMgmt;
  SBackupTask      *pTask;
  const char       *backupDir;
  int32_t volatile nextVgIndex;  // shared atomic counter of the next vgroup
} SBackupVgroupWorkerCtx;

static void dmBackupChunkPath(const SDmBackupFile *pBackupFile, int32_t chunkIndex, char *path, size_t pathSize) {
  (void)snprintf(path, pathSize, "%s%s%s%s%08d", pBackupFile->chunkDir, TD_DIRSEP, pBackupFile->chunkBase,
                 TD_DIRSEP, chunkIndex);
}

// Close the current chunk file and open the next one for writing.
static int32_t dmBackupFileNextChunk(SDmBackupFile *pBackupFile) {
  char path[PATH_MAX] = {0};
  int32_t code = 0;

  if (taosFsyncFile(pBackupFile->pFile) != 0) {
    code = terrno != 0 ? terrno : TSDB_CODE_FAILED;
    (void)taosCloseFile(&pBackupFile->pFile);
    pBackupFile->pFile = NULL;
    return code;
  }
  if (taosCloseFile(&pBackupFile->pFile) != 0) {
    code = terrno != 0 ? terrno : TSDB_CODE_FAILED;
    pBackupFile->pFile = NULL;
    return code;
  }
  ++pBackupFile->chunkIndex;
  pBackupFile->chunkWritten = 0;
  dmBackupChunkPath(pBackupFile, pBackupFile->chunkIndex, path, sizeof(path));
  pBackupFile->pFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pBackupFile->pFile == NULL) {
    return terrno;
  }
  return 0;
}

static int32_t writeDataToBackupFile(SDmBackupFile *pBackupFile, const void *data, int32_t dataLen) {
  const uint8_t *input = data;

  while (dataLen > 0) {
    int32_t toWrite = dataLen;
    if (pBackupFile->chunked && !pBackupFile->finalChunk) {
      int64_t space = pBackupFile->chunkSize - pBackupFile->chunkWritten;
      if (space <= 0) {
        // current chunk is full; roll over before writing anything
        int32_t code = dmBackupFileNextChunk(pBackupFile);
        if (code != 0) {
          return code;
        }
        space = pBackupFile->chunkSize;
      }
      toWrite = TMIN(toWrite, (int32_t)space);
    }

    if (taosWriteFile(pBackupFile->pFile, input, toWrite) != toWrite) {
      return terrno != 0 ? terrno : TSDB_CODE_FAILED;
    }
    sha256_update(&pBackupFile->checksumCtx, input, toWrite);
    input += toWrite;
    dataLen -= toWrite;
    pBackupFile->chunkWritten += toWrite;

    if (!pBackupFile->chunked || pBackupFile->finalChunk) {
      break;
    }
  }

  return 0;
}

static int32_t dmBackupFileWriteEncrypted(SDmBackupFile *pBackupFile, const void *data, int32_t dataLen) {
  if (pBackupFile->finalized || dataLen < 0 || (dataLen > 0 && data == NULL)) {
    return TSDB_CODE_INVALID_PARA;
  }

  const uint8_t *input = data;
  uint8_t outBuf[16384];
  unsigned int outBufLen = 0;

  if (pBackupFile->bufferLen > 0) {
    int32_t copyLen = TMIN(dataLen, sizeof(pBackupFile->buffer) - pBackupFile->bufferLen);
    memcpy(pBackupFile->buffer + pBackupFile->bufferLen, input, copyLen);
    pBackupFile->bufferLen += copyLen;
    input += copyLen;
    dataLen -= copyLen;
    if (pBackupFile->bufferLen < sizeof(pBackupFile->buffer)) {
      return 0;
    }

    int ret = SM4_CBC_Encrypt(
        pBackupFile->key,
        sizeof(pBackupFile->key),
        pBackupFile->iv,
        sizeof(pBackupFile->iv),
        pBackupFile->buffer,
        sizeof(pBackupFile->buffer),
        outBuf,
        &outBufLen
      );
    if (ret != 0 || outBufLen != sizeof(pBackupFile->buffer)) {
      return TSDB_CODE_FAILED;
    }
    // CBC chaining: the block just encrypted becomes the next IV.
    memcpy(pBackupFile->iv, outBuf, sizeof(pBackupFile->iv));
  }

  while (dataLen >= sizeof(pBackupFile->buffer)) {
    unsigned int inLen = TMIN(sizeof(outBuf) - outBufLen, dataLen) / DM_BACKUP_SM4_BLOCK_SIZE * DM_BACKUP_SM4_BLOCK_SIZE;
    unsigned int outLen = 0;

    int ret = SM4_CBC_Encrypt(
        pBackupFile->key,
        sizeof(pBackupFile->key),
        pBackupFile->iv,
        sizeof(pBackupFile->iv),
        (uint8_t *)input,
        inLen,
        outBuf + outBufLen,
        &outLen
      );
    if (ret != 0 || outLen != inLen) {
      return TSDB_CODE_FAILED;
    }

    input += inLen;
    dataLen -= inLen;
    outBufLen += outLen;
    // CBC chaining: the last ciphertext block of this batch becomes the next
    // IV for the following batch (SM4_CBC_Encrypt never writes pIV back).
    memcpy(pBackupFile->iv, outBuf + outBufLen - DM_BACKUP_SM4_BLOCK_SIZE, sizeof(pBackupFile->iv));

    int32_t code = writeDataToBackupFile(pBackupFile, outBuf, outBufLen);
    if (code != 0) {
      return code;
    }
    outBufLen = 0;
  }

  if (outBufLen > 0) {
    int32_t code = writeDataToBackupFile(pBackupFile, outBuf, outBufLen);
    if (code != 0) {
      return code;
    }
  }

  if (dataLen > 0) {
    memcpy(pBackupFile->buffer, input, dataLen);
    pBackupFile->bufferLen = dataLen;
  }

  return 0;
}

static int32_t dmBackupFileFinal(SDmBackupFile *pBackupFile) {
  uint8_t padding = DM_BACKUP_SM4_BLOCK_SIZE - pBackupFile->bufferLen;

  if (pBackupFile->finalized) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(pBackupFile->buffer + pBackupFile->bufferLen, padding, padding);
  // The padding block must stay in the final chunk file, even when it exactly
  // fills the chunk, so no empty trailing chunk is produced.
  pBackupFile->finalChunk = true;

  uint8_t outBuf[sizeof(pBackupFile->buffer)];
  unsigned int outBufLen = 0;

  int ret = SM4_CBC_Encrypt(
      pBackupFile->key,
      sizeof(pBackupFile->key),
      pBackupFile->iv,
      sizeof(pBackupFile->iv),
      pBackupFile->buffer,
      sizeof(pBackupFile->buffer),
      outBuf,
      &outBufLen
    );
  if (ret != 0 || outBufLen != sizeof(pBackupFile->buffer)) {
    return TSDB_CODE_FAILED;
  }

  int32_t code = writeDataToBackupFile(pBackupFile, outBuf, outBufLen);
  if (code != 0) {
    return code;
  }

  pBackupFile->bufferLen = 0;
  pBackupFile->finalized = true;
  return 0;
}

static int32_t dmBackupFileCreate(SDmBackupFile *pBackupFile, const char *path, const char *password) {
  memset(pBackupFile, 0, sizeof(*pBackupFile));
  pBackupFile->pFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pBackupFile->pFile == NULL) {
    return terrno;
  }
  pBackupFile->encrypted = password != NULL;
  sha256_init(&pBackupFile->checksumCtx);
  if (pBackupFile->encrypted) {
    memcpy(pBackupFile->key, password, sizeof(pBackupFile->key));
    // Use a per-file random IV (stored in the file header) instead of deriving
    // it from the password, so identical plaintext prefixes of different files
    // no longer produce identical ciphertext prefixes.
    taosSafeRandBytes(pBackupFile->iv, sizeof(pBackupFile->iv));
    if (taosWriteFile(pBackupFile->pFile, pBackupFile->iv, sizeof(pBackupFile->iv)) != sizeof(pBackupFile->iv)) {
      int32_t code = terrno != 0 ? terrno : TSDB_CODE_FAILED;
      (void)taosCloseFile(&pBackupFile->pFile);
      return code;
    }
    sha256_update(&pBackupFile->checksumCtx, pBackupFile->iv, sizeof(pBackupFile->iv));
  }
  return 0;
}

// Create a chunked backup file: the logical file is split into equal-sized
// chunk files <dir>/<baseName>/00000001, 00000002, ... The caller must have
// created the <dir>/<baseName> directory first. The IV (encrypted mode) is
// only written at the head of chunk 1; the padding block is only written by
// dmBackupFileClose into the final chunk, so the whole logical file stays one
// continuous ciphertext stream.
static int32_t dmBackupFileCreateChunked(SDmBackupFile *pBackupFile, const char *dir, const char *baseName,
                                         const char *password) {
  memset(pBackupFile, 0, sizeof(*pBackupFile));
  pBackupFile->chunked = true;
  pBackupFile->chunkSize = DM_BACKUP_VNODE_CHUNK_SIZE;
  pBackupFile->chunkIndex = 1;
  tstrncpy(pBackupFile->chunkDir, dir, sizeof(pBackupFile->chunkDir));
  tstrncpy(pBackupFile->chunkBase, baseName, sizeof(pBackupFile->chunkBase));
  pBackupFile->encrypted = password != NULL;
  sha256_init(&pBackupFile->checksumCtx);
  if (pBackupFile->encrypted) {
    memcpy(pBackupFile->key, password, sizeof(pBackupFile->key));
  }

  char path[PATH_MAX] = {0};
  dmBackupChunkPath(pBackupFile, 1, path, sizeof(path));
  pBackupFile->pFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pBackupFile->pFile == NULL) {
    return terrno;
  }
  if (pBackupFile->encrypted) {
    taosSafeRandBytes(pBackupFile->iv, sizeof(pBackupFile->iv));
    if (taosWriteFile(pBackupFile->pFile, pBackupFile->iv, sizeof(pBackupFile->iv)) != sizeof(pBackupFile->iv)) {
      int32_t code = terrno != 0 ? terrno : TSDB_CODE_FAILED;
      (void)taosCloseFile(&pBackupFile->pFile);
      pBackupFile->pFile = NULL;
      return code;
    }
    sha256_update(&pBackupFile->checksumCtx, pBackupFile->iv, sizeof(pBackupFile->iv));
    pBackupFile->chunkWritten = sizeof(pBackupFile->iv);
  }
  return 0;
}

static int32_t dmBackupFileWrite(SDmBackupFile *pBackupFile, const void *data, int32_t dataLen) {
  if (pBackupFile->pFile == NULL || dataLen < 0 || (dataLen > 0 && data == NULL)) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pBackupFile->encrypted) {
    return dmBackupFileWriteEncrypted(pBackupFile, data, dataLen);
  }
  return writeDataToBackupFile(pBackupFile, data, dataLen);
}

static int32_t dmBackupFileClose(SDmBackupFile *pBackupFile) {
  int32_t code = 0;

  if (pBackupFile->pFile == NULL) {
    return 0;
  }
  if (pBackupFile->encrypted) {
    code = dmBackupFileFinal(pBackupFile);
  }
  if (code == 0) {
    code = taosFsyncFile(pBackupFile->pFile);
  }
  if (code == 0) {
    sha256_final(&pBackupFile->checksumCtx, pBackupFile->checksum);
  }
  if (pBackupFile->chunked && code == 0) {
    pBackupFile->numOfChunks = pBackupFile->chunkIndex;
  }
  taosCloseFile(&pBackupFile->pFile);
  return code;
}

static void dmBackupDigestToString(const uint8_t *digest, char *output, size_t outputSize) {
  static const char hex[] = "0123456789abcdef";

  if (outputSize < sizeof("sha256:") + SHA256_DIGEST_SIZE * 2) {
    return;
  }
  (void)snprintf(output, outputSize, "sha256:");
  for (int32_t i = 0; i < SHA256_DIGEST_SIZE; ++i) {
    output[sizeof("sha256:") - 1 + i * 2] = hex[digest[i] >> 4];
    output[sizeof("sha256:") + i * 2] = hex[digest[i] & 0x0f];
  }
  output[sizeof("sha256:") - 1 + SHA256_DIGEST_SIZE * 2] = '\0';
}

static void dmBackupAggregateFileDigests(SBackupFileDigest *digests, int32_t numOfDigests, uint8_t *checksum) {
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

static int32_t dmBackupAddIntegerToArray(SJson *pArray, int64_t value) {
  cJSON *pValue = cJSON_CreateNumber(value);
  if (pValue == NULL) {
    return terrno;
  }
  if (!cJSON_AddItemToArray(pArray, pValue)) {
    cJSON_Delete(pValue);
    return terrno;
  }
  return 0;
}

static void dmBackupDestroyDatabases(SArray *pDatabases) {
  if (pDatabases == NULL) {
    return;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pDatabases); ++i) {
    SBackupDatabaseInfo *pDatabase = taosArrayGet(pDatabases, i);
    taosArrayDestroy(pDatabase->vgIds);
  }
  taosArrayDestroy(pDatabases);
}

static int32_t dmBackupCollectDatabases(const SArray *pVgroups, SArray **ppDatabases) {
  int32_t code = 0;
  SArray *pDatabases = taosArrayInit(pVgroups == NULL ? 0 : taosArrayGetSize(pVgroups), sizeof(SBackupDatabaseInfo));
  if (pDatabases == NULL) {
    return terrno;
  }

  for (int32_t i = 0; pVgroups != NULL && i < taosArrayGetSize(pVgroups); ++i) {
    SBackupVgroupInfo  *pVgroup = taosArrayGet(pVgroups, i);
    SBackupDatabaseInfo *pDatabase = NULL;
    for (int32_t j = 0; j < taosArrayGetSize(pDatabases); ++j) {
      SBackupDatabaseInfo *pCurrent = taosArrayGet(pDatabases, j);
      if (strcmp(pCurrent->name, pVgroup->dbName) == 0) {
        pDatabase = pCurrent;
        break;
      }
    }
    if (pDatabase == NULL) {
      SBackupDatabaseInfo database = {0};
      tstrncpy(database.name, pVgroup->dbName, sizeof(database.name));
      database.vgIds = taosArrayInit(1, sizeof(int32_t));
      if (database.vgIds == NULL || taosArrayPush(pDatabases, &database) == NULL) {
        taosArrayDestroy(database.vgIds);
        code = terrno;
        goto _exit;
      }
      pDatabase = taosArrayGet(pDatabases, taosArrayGetSize(pDatabases) - 1);
    }
    if (taosArrayPush(pDatabase->vgIds, &pVgroup->vgId) == NULL) {
      code = terrno;
      goto _exit;
    }
  }
  *ppDatabases = pDatabases;
  return 0;

_exit:
  dmBackupDestroyDatabases(pDatabases);
  return code;
}

static int32_t dmBackupWriteManifest(const SBackupTask *pTask, const char *backupDir) {
  int32_t   code = 0;
  SJson    *pJson = NULL;
  SArray   *pDatabases = NULL;
  char     *buffer = NULL;
  char      manifestPath[PATH_MAX] = {0};
  char      id[32] = {0};
  char      chain[32] = {0};
  char      checksum[sizeof("sha256:") + SHA256_DIGEST_SIZE * 2] = {0};
  const SBackupDnodeReq *pReq = &pTask->req;
  SDmBackupFile backupFile = {0};

  pJson = tjsonCreateObject();
  if (pJson == NULL) {
    code = terrno;
    goto _exit;
  }
  (void)snprintf(id, sizeof(id), "%" PRId64, pReq->backupId);
  (void)snprintf(chain, sizeof(chain), "%" PRId64, pReq->chainId);
  if ((code = tjsonAddIntegerToObject(pJson, "version", 1)) != 0 ||
      (code = tjsonAddStringToObject(pJson, "tsdbVersion", td_version)) != 0 ||
      (code = tjsonAddStringToObject(pJson, "id", id)) != 0 ||
      (code = tjsonAddStringToObject(pJson, "chain", chain)) != 0 ||
      (code = tjsonAddStringToObject(pJson, "previous", "")) != 0) {
    goto _exit;
  }

  SJson *pDnodes = tjsonAddArrayToObject(pJson, "dnodes");
  if (pDnodes == NULL) {
    code = terrno;
    goto _exit;
  }
  for (int32_t i = 0; pReq->dnodes != NULL && i < taosArrayGetSize(pReq->dnodes); ++i) {
    SBackupDnodeInfo *pDnode = taosArrayGet(pReq->dnodes, i);
    if ((code = dmBackupAddIntegerToArray(pDnodes, pDnode->id)) != 0) {
      goto _exit;
    }
  }

  SJson *pMnode = tjsonCreateObject();
  if (pMnode == NULL) {
    code = terrno;
    goto _exit;
  }
  SJson *pMnodeDnodes = tjsonAddArrayToObject(pMnode, "dnodes");
  if (pMnodeDnodes == NULL) {
    tjsonDelete(pMnode);
    code = terrno;
    goto _exit;
  }
  for (int32_t i = 0; pReq->dnodes != NULL && i < taosArrayGetSize(pReq->dnodes); ++i) {
    SBackupDnodeInfo *pDnode = taosArrayGet(pReq->dnodes, i);
    if (pDnode->isMnode && (code = dmBackupAddIntegerToArray(pMnodeDnodes, pDnode->id)) != 0) {
      tjsonDelete(pMnode);
      goto _exit;
    }
  }
  dmBackupDigestToString(pTask->mnodeChecksum, checksum, sizeof(checksum));
  if ((code = tjsonAddStringToObject(pMnode, "checksum", checksum)) != 0 ||
      (code = tjsonAddItemToObject(pJson, "mnode", pMnode)) != 0) {
    tjsonDelete(pMnode);
    goto _exit;
  }

  SJson *pSnodes = tjsonAddArrayToObject(pJson, "snodes");
  if (pSnodes == NULL) {
    code = terrno;
    goto _exit;
  }
  for (int32_t i = 0; pTask->snodeConfigs != NULL && i < taosArrayGetSize(pTask->snodeConfigs); ++i) {
    SBackupSnodeConfigDigest *pConfig = taosArrayGet(pTask->snodeConfigs, i);
    SJson                    *pSnode = tjsonCreateObject();
    if (pSnode == NULL) {
      code = terrno;
      goto _exit;
    }
    SJson *pStreams = tjsonAddArrayToObject(pSnode, "streams");
    if (pStreams == NULL) {
      tjsonDelete(pSnode);
      code = terrno;
      goto _exit;
    }
    dmBackupDigestToString(pConfig->digest, checksum, sizeof(checksum));
    if ((code = tjsonAddIntegerToObject(pSnode, "id", pConfig->dnodeId)) != 0 ||
        (code = tjsonAddStringToObject(pSnode, "checksum", checksum)) != 0) {
      tjsonDelete(pSnode);
      goto _exit;
    }
    for (int32_t j = 0; pTask->snodeCheckpoints != NULL && j < taosArrayGetSize(pTask->snodeCheckpoints); ++j) {
      SBackupCheckpointDigest *pCheckpoint = taosArrayGet(pTask->snodeCheckpoints, j);
      SJson                   *pStream = NULL;
      if (pCheckpoint->dnodeId != pConfig->dnodeId) {
        continue;
      }
      pStream = tjsonCreateObject();
      if (pStream == NULL) {
        tjsonDelete(pSnode);
        code = terrno;
        goto _exit;
      }
      dmBackupDigestToString(pCheckpoint->digest, checksum, sizeof(checksum));
      if ((code = tjsonAddStringToObject(pStream, "checkpoint", pCheckpoint->checkpoint)) != 0 ||
          (code = tjsonAddStringToObject(pStream, "checksum", checksum)) != 0 ||
          (code = tjsonAddItemToArray(pStreams, pStream)) != 0) {
        tjsonDelete(pStream);
        tjsonDelete(pSnode);
        goto _exit;
      }
    }
    if ((code = tjsonAddItemToArray(pSnodes, pSnode)) != 0) {
      tjsonDelete(pSnode);
      goto _exit;
    }
  }

  if ((code = dmBackupCollectDatabases(pReq->vgroups, &pDatabases)) != 0) {
    goto _exit;
  }
  SJson *pDatabasesJson = tjsonAddArrayToObject(pJson, "databases");
  SJson *pVnodesJson = tjsonAddArrayToObject(pJson, "vnodes");
  if (pDatabasesJson == NULL || pVnodesJson == NULL) {
    code = terrno;
    goto _exit;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pDatabases); ++i) {
    SBackupDatabaseInfo *pDatabase = taosArrayGet(pDatabases, i);
    SJson               *pDatabaseJson = tjsonCreateObject();
    if (pDatabaseJson == NULL) {
      code = terrno;
      goto _exit;
    }
    SJson *pDatabaseVnodes = tjsonAddArrayToObject(pDatabaseJson, "vnodes");
    if (pDatabaseVnodes == NULL ||
        (code = tjsonAddStringToObject(pDatabaseJson, "name", pDatabase->name)) != 0) {
      tjsonDelete(pDatabaseJson);
      code = code != 0 ? code : terrno;
      goto _exit;
    }
    for (int32_t j = 0; j < taosArrayGetSize(pDatabase->vgIds); ++j) {
      if ((code = dmBackupAddIntegerToArray(pDatabaseVnodes, *(int32_t *)taosArrayGet(pDatabase->vgIds, j))) != 0) {
        tjsonDelete(pDatabaseJson);
        goto _exit;
      }
    }
    if ((code = tjsonAddItemToArray(pDatabasesJson, pDatabaseJson)) != 0) {
      tjsonDelete(pDatabaseJson);
      goto _exit;
    }
  }
  for (int32_t i = 0; pReq->vgroups != NULL && i < taosArrayGetSize(pReq->vgroups); ++i) {
    SBackupVgroupInfo  *pVgroup = taosArrayGet(pReq->vgroups, i);
    SBackupVnodeDigest *pDigest = NULL;
    SJson              *pVnode = NULL;
    SJson              *pDnodes = NULL;
    for (int32_t j = 0; pTask->vnodeSnapshots != NULL && j < taosArrayGetSize(pTask->vnodeSnapshots); ++j) {
      SBackupVnodeDigest *pCurrent = taosArrayGet(pTask->vnodeSnapshots, j);
      if (pCurrent->vgId == pVgroup->vgId) {
        pDigest = pCurrent;
        break;
      }
    }
    if (pDigest == NULL) {
      code = TSDB_CODE_INVALID_DATA_FMT;
      goto _exit;
    }
    pVnode = tjsonCreateObject();
    if (pVnode == NULL) {
      code = terrno;
      goto _exit;
    }
    pDnodes = tjsonAddArrayToObject(pVnode, "dnodes");
    if (pDnodes == NULL ||
        (code = tjsonAddIntegerToObject(pVnode, "id", pVgroup->vgId)) != 0) {
      tjsonDelete(pVnode);
      code = code != 0 ? code : terrno;
      goto _exit;
    }
    for (int32_t j = 0; j < pVgroup->numOfDnodes; ++j) {
      if ((code = dmBackupAddIntegerToArray(pDnodes, pVgroup->dnodeIds[j])) != 0) {
        tjsonDelete(pVnode);
        goto _exit;
      }
    }
    if ((code = tjsonAddIntegerToObject(pVnode, "startVersion", 0)) != 0 ||
        (code = tjsonAddIntegerToObject(pVnode, "endVersion", pDigest->snapshotIndex)) != 0 ||
        (code = tjsonAddIntegerToObject(pVnode, "numOfChunks", pDigest->numOfChunks)) != 0) {
      tjsonDelete(pVnode);
      goto _exit;
    }
    dmBackupDigestToString(pDigest->digest, checksum, sizeof(checksum));
    if ((code = tjsonAddStringToObject(pVnode, "checksum", checksum)) != 0 ||
        (code = tjsonAddItemToArray(pVnodesJson, pVnode)) != 0) {
      tjsonDelete(pVnode);
      goto _exit;
    }
  }
  if ((code = tjsonAddIntegerToObject(pJson, "totalSize", pTask->totalSize)) != 0) {
    goto _exit;
  }

  buffer = tjsonToString(pJson);
  if (buffer == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }
  (void)snprintf(manifestPath, sizeof(manifestPath), "%s%sbackup.json", backupDir, TD_DIRSEP);
  int32_t len = strlen(buffer);
  code = dmBackupFileCreate(&backupFile, manifestPath, pReq->hasPassword ? pReq->password : NULL);
  if (code == 0) {
    code = dmBackupFileWrite(&backupFile, buffer, len);
  }
  int32_t closeCode = dmBackupFileClose(&backupFile);
  if (code == 0) {
    code = closeCode;
  }
  if (code != 0) {
    goto _exit;
  }

_exit:
  taosMemoryFree(buffer);
  dmBackupDestroyDatabases(pDatabases);
  tjsonDelete(pJson);
  return code;
}

void dmBackupFreeTaskArtifacts(SBackupTask *pTask) {
  if (pTask == NULL) {
    return;
  }
  taosArrayDestroy(pTask->snodeCheckpoints);
  taosArrayDestroy(pTask->snodeConfigs);
  taosArrayDestroy(pTask->vnodeSnapshots);
  pTask->snodeCheckpoints = NULL;
  pTask->snodeConfigs = NULL;
  pTask->vnodeSnapshots = NULL;
}

static int32_t dmBackupCheckCancelled(SDnodeMgmt *pMgmt, const SBackupTask *pTask) {
  int32_t code = 0;
  (void)taosThreadMutexLock(&pMgmt->backupMutex);
  if (pMgmt->pBackupTask == pTask && pTask->cancelled) {
    code = TSDB_CODE_BACKUP_CANCELLED;
  } else if (pTask->code != 0) {
    // A parallel vgroup worker failed: propagate its error so the other
    // workers (and the serial mnode/snode stages) abort promptly.
    code = pTask->code;
  }
  (void)taosThreadMutexUnlock(&pMgmt->backupMutex);
  return code;
}

// Write one packaged backup file to <unitDir>/<relPath>, creating any
// intermediate directories (relPath may contain a subdir like "sync/...").
static int32_t dmBackupWriteFile(const SBackupDnodeReq *pReq, const char *unitDir, const SBackupFile *pMf,
                                 uint8_t *digest, int64_t *pTotalSize) {
  int32_t   code = 0;
  char      outPath[PATH_MAX] = {0};
  char      dirPath[PATH_MAX] = {0};
  SDmBackupFile backupFile = {0};

  (void)snprintf(outPath, sizeof(outPath), "%s%s%s", unitDir, TD_DIRSEP, pMf->relPath);

  tstrncpy(dirPath, outPath, sizeof(dirPath));
  char *pSep = strrchr(dirPath, TD_DIRSEP[0]);
  if (pSep != NULL) {
    *pSep = '\0';
    if (!taosDirExist(dirPath) && (code = taosMulMkDir(dirPath)) != 0) {
      dError("failed to create backup meta subdir:%s since %s", dirPath, tstrerror(code));
      return code;
    }
  }

  code = dmBackupFileCreate(&backupFile, outPath, pReq->hasPassword ? pReq->password : NULL);
  if (code == 0) {
    code = dmBackupFileWrite(&backupFile, pMf->data, pMf->dataLen);
  }
  int32_t closeCode = dmBackupFileClose(&backupFile);
  if (code == 0) {
    code = closeCode;
  }
  if (code != 0) {
    dError("failed to write %s since %s", outPath, tstrerror(code));
    goto _exit;
  }
  if (digest != NULL) {
    memcpy(digest, backupFile.checksum, sizeof(backupFile.checksum));
  }
  if (pTotalSize != NULL) {
    *pTotalSize += pMf->dataLen;
  }
  dInfo("backup mnode meta file written:%s bytes:%d", outPath, pMf->dataLen);

_exit:
  return code;
}

static int32_t dmBackupPullMnodeMeta(SDnodeMgmt *pMgmt, SBackupTask *pTask, const char *backupDir) {
  int32_t        code = 0;
  char           mnodeDir[PATH_MAX] = {0};
  SBackupMetaRsp metaRsp = {0};
  SBackupFileDigest digests[4] = {0};
  SBackupDnodeReq *pReq = &pTask->req;

  if ((code = dmBackupCheckCancelled(pMgmt, pTask)) != 0) {
    return code;
  }

  (void)snprintf(mnodeDir, sizeof(mnodeDir), "%s%smnode", backupDir, TD_DIRSEP);
  if ((code = taosMulMkDir(mnodeDir)) != 0) {
    dError("failed to create mnode backup dir:%s since %s", mnodeDir, tstrerror(code));
    return code;
  }

  SBackupMetaReq metaReq = {.chainId = pReq->chainId, .backupId = pReq->backupId};
  int32_t        contLen = tSerializeSBackupMetaReq(NULL, 0, &metaReq);
  if (contLen < 0) {
    return contLen;
  }
  void *pCont = rpcMallocCont(contLen);
  if (pCont == NULL) {
    return terrno;
  }
  contLen = tSerializeSBackupMetaReq(pCont, contLen, &metaReq);
  if (contLen < 0) {
    rpcFreeCont(pCont);
    return contLen;
  }

  SRpcMsg rpcMsg = {.pCont = pCont, .contLen = contLen, .msgType = TDMT_MND_BACKUP_META};
  SRpcMsg rpcRsp = {0};
  SEpSet  epSet = {0};
  (void)dmGetMnodeEpSet(pMgmt->pData, &epSet);

  code = rpcSendRecv(pMgmt->msgCb.clientRpc, &epSet, &rpcMsg, &rpcRsp);
  if (code != 0) {
    dError("failed to pull mnode meta since %s", tstrerror(code));
    return code;
  }
  if (rpcRsp.code != 0) {
    code = rpcRsp.code;
    dError("mnode rejected backup meta req since %s", tstrerror(code));
    rpcFreeCont(rpcRsp.pCont);
    return code;
  }

  if ((code = tDeserializeSBackupMetaRsp(rpcRsp.pCont, rpcRsp.contLen, &metaRsp)) != 0) {
    dError("failed to decode backup meta rsp since %s", tstrerror(code));
    rpcFreeCont(rpcRsp.pCont);
    return code;
  }
  rpcFreeCont(rpcRsp.pCont);

  if (metaRsp.numOfFiles > tListLen(digests)) {
    dError("too many mnode meta files:%d", metaRsp.numOfFiles);
    code = TSDB_CODE_INVALID_DATA_FMT;
    goto _exit;
  }

  if ((code = dmBackupCheckCancelled(pMgmt, pTask)) != 0) {
    goto _exit;
  }

  for (int32_t i = 0; i < metaRsp.numOfFiles; ++i) {
    if ((code = dmBackupCheckCancelled(pMgmt, pTask)) != 0) {
      goto _exit;
    }
    tstrncpy(digests[i].relPath, metaRsp.files[i].relPath, sizeof(digests[i].relPath));
    if ((code = dmBackupWriteFile(pReq, mnodeDir, &metaRsp.files[i], digests[i].digest, &pTask->totalSize)) != 0) {
      goto _exit;
    }
  }
  dmBackupAggregateFileDigests(digests, metaRsp.numOfFiles, pTask->mnodeChecksum);

  dInfo("backup mnode meta done, dir:%s files:%d sdbEndVersion:%" PRId64, mnodeDir, metaRsp.numOfFiles, metaRsp.sdbEndVersion);

_exit:
  tFreeSBackupMetaRsp(&metaRsp);
  return code;
}

static int32_t dmBackupPullSnodeCheckpoints(SDnodeMgmt *pMgmt, SBackupTask *pTask, const char *backupDir) {
  int32_t code = 0;
  SArray *snodeDnodes = NULL;
  char    snodeDir[PATH_MAX] = {0};
  SBackupDnodeReq *pReq = &pTask->req;

  if ((code = dmBackupCheckCancelled(pMgmt, pTask)) != 0) {
    return code;
  }

  int32_t numOfStreams = pReq->streamSnodes == NULL ? 0 : taosArrayGetSize(pReq->streamSnodes);
  int32_t numOfDnodes = pReq->dnodes == NULL ? 0 : taosArrayGetSize(pReq->dnodes);
  int32_t numOfSnodes = 0;
  for (int32_t i = 0; i < numOfDnodes; ++i) {
    SBackupDnodeInfo *pDnode = taosArrayGet(pReq->dnodes, i);
    numOfSnodes += pDnode->isSnode;
  }
  if (numOfSnodes == 0) {
    dInfo("backup skips snode because no snode is deployed");
    return 0;
  }

  snodeDnodes = taosArrayInit(numOfSnodes, sizeof(int32_t));
  if (snodeDnodes == NULL) {
    return terrno;
  }
  for (int32_t i = 0; i < numOfDnodes; ++i) {
    SBackupDnodeInfo *pDnode = taosArrayGet(pReq->dnodes, i);
    int32_t           dnodeId = pDnode->id;
    if (!pDnode->isSnode) {
      continue;
    }
    if (dnodeId <= 0 || taosArrayPush(snodeDnodes, &dnodeId) == NULL) {
      code = terrno;
      goto _exit;
    }
  }

  (void)snprintf(snodeDir, sizeof(snodeDir), "%s%ssnode", backupDir, TD_DIRSEP);
  if ((code = taosMulMkDir(snodeDir)) != 0) {
    goto _exit;
  }

  for (int32_t i = 0; i < taosArrayGetSize(snodeDnodes); ++i) {
    if ((code = dmBackupCheckCancelled(pMgmt, pTask)) != 0) {
      goto _exit;
    }
    int32_t           dnodeId = *(int32_t *)taosArrayGet(snodeDnodes, i);
    char              fqdn[TSDB_FQDN_LEN] = {0};
    uint16_t          port = 0;
    SEpSet            epSet = {0};
    SBackupSnodeReq   snodeReq = {0};
    SBackupSnodeRsp   snodeRsp = {0};
    SRpcMsg           snodeRpcRsp = {0};

    dmGetDnodeEp(pMgmt->pData, dnodeId, NULL, fqdn, &port);
    if (fqdn[0] == '\0' || port == 0) {
      dError("backup snapshot snode dnode:%d has no endpoint in the local dnode cache", dnodeId);
      code = TSDB_CODE_MND_DNODE_NOT_EXIST;
      goto _exit;
    }
    epSet.numOfEps = 1;
    tstrncpy(epSet.eps[0].fqdn, fqdn, sizeof(epSet.eps[0].fqdn));
    epSet.eps[0].port = port;

    snodeReq.dnodeId = dnodeId;
    snodeReq.streamIds = taosArrayInit(numOfStreams, sizeof(int64_t));
    if (snodeReq.streamIds == NULL) {
      code = terrno;
      goto _exit;
    }
    for (int32_t j = 0; j < numOfStreams; ++j) {
      SBackupStreamSnodeInfo *pInfo = taosArrayGet(pReq->streamSnodes, j);
      if (pInfo->snodeDnodeId == dnodeId && taosArrayPush(snodeReq.streamIds, &pInfo->streamId) == NULL) {
        tFreeSBackupSnodeReq(&snodeReq);
        code = terrno;
        goto _exit;
      }
    }
    int32_t contLen = tSerializeSBackupSnodeReq(NULL, 0, &snodeReq);
    if (contLen < 0) {
      tFreeSBackupSnodeReq(&snodeReq);
      code = contLen;
      goto _exit;
    }
    void *pCont = rpcMallocCont(contLen);
    if (pCont == NULL) {
      tFreeSBackupSnodeReq(&snodeReq);
      code = terrno;
      goto _exit;
    }
    if (tSerializeSBackupSnodeReq(pCont, contLen, &snodeReq) < 0) {
      rpcFreeCont(pCont);
      tFreeSBackupSnodeReq(&snodeReq);
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
    tFreeSBackupSnodeReq(&snodeReq);

    SRpcMsg snodeRpcMsg = {.pCont = pCont, .contLen = contLen, .msgType = TDMT_DND_BACKUP_SNODE};
    code = rpcSendRecv(pMgmt->msgCb.clientRpc, &epSet, &snodeRpcMsg, &snodeRpcRsp);
    if (code == 0) code = snodeRpcRsp.code;
    if (code == 0) code = tDeserializeSBackupSnodeRsp(snodeRpcRsp.pCont, snodeRpcRsp.contLen, &snodeRsp);
    rpcFreeCont(snodeRpcRsp.pCont);
    if (code != 0) {
      tFreeSBackupSnodeRsp(&snodeRsp);
      dError("failed to pull checkpoints from snapshot snode dnode:%d since %s", dnodeId, tstrerror(code));
      goto _exit;
    }

    if (taosArrayGetSize(snodeRsp.files) <= 0) {
      tFreeSBackupSnodeRsp(&snodeRsp);
      dError("invalid backup snode rsp from dnode:%d, numOfFiles:%d", dnodeId, (int32_t)taosArrayGetSize(snodeRsp.files));
      code = TSDB_CODE_INVALID_DATA_FMT;
      goto _exit;
    }

    SBackupFile              *pConfigFile = taosArrayGet(snodeRsp.files, 0);
    SBackupSnodeConfigDigest  config = {.dnodeId = dnodeId};
    snprintf(config.config, sizeof(config.config), "dnode%d%ssnode.json", dnodeId, TD_DIRSEP);
    tstrncpy(pConfigFile->relPath, config.config, sizeof(pConfigFile->relPath));
    if ((code = dmBackupWriteFile(pReq, snodeDir, pConfigFile, config.digest, &pTask->totalSize)) != 0) {
      tFreeSBackupSnodeRsp(&snodeRsp);
      goto _exit;
    }
    if (pTask->snodeConfigs == NULL) {
      pTask->snodeConfigs = taosArrayInit(taosArrayGetSize(snodeDnodes), sizeof(SBackupSnodeConfigDigest));
      if (pTask->snodeConfigs == NULL) {
        code = terrno;
        tFreeSBackupSnodeRsp(&snodeRsp);
        goto _exit;
      }
    }
    if (taosArrayPush(pTask->snodeConfigs, &config) == NULL) {
      code = terrno;
      tFreeSBackupSnodeRsp(&snodeRsp);
      goto _exit;
    }

    for (int32_t j = 1; j < taosArrayGetSize(snodeRsp.files); ++j) {
      if ((code = dmBackupCheckCancelled(pMgmt, pTask)) != 0) {
        break;
      }
      SBackupFile *pFile = taosArrayGet(snodeRsp.files, j);
      SBackupCheckpointDigest digest = {.dnodeId = dnodeId};

      char checkpoint[TSDB_FILENAME_LEN] = {0};
      snprintf(checkpoint, sizeof(checkpoint), "dnode%d%s%s", dnodeId, TD_DIRSEP, pFile->relPath);
      tstrncpy(digest.checkpoint, pFile->relPath, sizeof(digest.checkpoint));
      tstrncpy(pFile->relPath, checkpoint, sizeof(pFile->relPath));
      if ((code = dmBackupWriteFile(pReq, snodeDir, pFile, digest.digest, &pTask->totalSize)) != 0) {
        break;
      }
      if (pTask->snodeCheckpoints == NULL) {
        pTask->snodeCheckpoints = taosArrayInit(numOfStreams, sizeof(SBackupCheckpointDigest));
        if (pTask->snodeCheckpoints == NULL) {
          code = terrno;
          break;
        }
      }
      if (taosArrayPush(pTask->snodeCheckpoints, &digest) == NULL) {
        code = terrno;
        break;
      }
    }
    tFreeSBackupSnodeRsp(&snodeRsp);
    if (code != 0) {
      goto _exit;
    }
  }

  dInfo("backup snode checkpoints done, streams:%d", numOfStreams);

_exit:
  taosArrayDestroy(snodeDnodes);
  return code;
}

static int32_t dmBackupGetVgroupLeader(SDnodeMgmt *pMgmt, const SBackupDnodeReq *pReq, int32_t vgId, SBackupVgroupLeaderRsp *pRsp) {
  int32_t                  code = 0;
  SBackupVgroupLeaderReq  req = {.chainId = pReq->chainId, .backupId = pReq->backupId, .vgId = vgId};
  SRpcMsg                  rpcMsg = {0};
  SRpcMsg                  rpcRsp = {0};
  SEpSet                   mnodeEpSet = {0};

  int32_t contLen = tSerializeSBackupVgroupLeaderReq(NULL, 0, &req);
  if (contLen < 0) {
    return contLen;
  }
  rpcMsg.pCont = rpcMallocCont(contLen);
  if (rpcMsg.pCont == NULL) {
    return terrno;
  }
  rpcMsg.contLen = tSerializeSBackupVgroupLeaderReq(rpcMsg.pCont, contLen, &req);
  if (rpcMsg.contLen < 0) {
    code = rpcMsg.contLen;
    rpcFreeCont(rpcMsg.pCont);
    return code;
  }
  rpcMsg.msgType = TDMT_MND_BACKUP_VGROUP_LEADER;

  (void)dmGetMnodeEpSet(pMgmt->pData, &mnodeEpSet);
  code = rpcSendRecv(pMgmt->msgCb.clientRpc, &mnodeEpSet, &rpcMsg, &rpcRsp);
  if (code == 0) {
    code = rpcRsp.code;
  }
  if (code == 0) {
    code = tDeserializeSBackupVgroupLeaderRsp(rpcRsp.pCont, rpcRsp.contLen, pRsp);
  }
  rpcFreeCont(rpcRsp.pCont);
  if (code != 0) {
    dError("failed to resolve backup vgroup:%d leader since %s", vgId, tstrerror(code));
    return code;
  }
  if (pRsp->vgId != vgId || pRsp->leaderDnodeId <= 0 || pRsp->leaderEpSet.numOfEps != 1) {
    dError("invalid backup vgroup:%d leader response, rsp vgId:%d dnode:%d endpoints:%d", vgId, pRsp->vgId, pRsp->leaderDnodeId, pRsp->leaderEpSet.numOfEps);
    return TSDB_CODE_INVALID_MSG;
  }
  return 0;
}

static int32_t dmBackupVnodeSnapRpc(SDnodeMgmt *pMgmt, const SEpSet *pEpSet, const SBackupVnodeSnapReq *pReq, SBackupVnodeSnapRsp *pRsp) {
  int32_t reqLen = tSerializeSBackupVnodeSnapReq(NULL, 0, (SBackupVnodeSnapReq *)pReq);
  if (reqLen < 0) {
    return reqLen;
  }
  int32_t contLen = sizeof(SMsgHead) + reqLen;
  void *pCont = rpcMallocCont(contLen);
  if (pCont == NULL) {
    return terrno;
  }
  SMsgHead *pHead = pCont;
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pReq->vgId);
  if (tSerializeSBackupVnodeSnapReq((char *)pCont + sizeof(SMsgHead), reqLen, (SBackupVnodeSnapReq *)pReq) < 0) {
    rpcFreeCont(pCont);
    return TSDB_CODE_INVALID_MSG;
  }

  SRpcMsg rpcMsg = {.pCont = pCont, .contLen = contLen, .msgType = TDMT_VND_BACKUP_SNAP};
  SRpcMsg rpcRsp = {0};
  int32_t code = rpcSendRecv(pMgmt->msgCb.clientRpc, (SEpSet *)pEpSet, &rpcMsg, &rpcRsp);
  if (code == 0) {
    code = rpcRsp.code;
  }
  if (code == 0) {
    code = tDeserializeSBackupVnodeSnapRsp(rpcRsp.pCont, rpcRsp.contLen, pRsp);
  }
  rpcFreeCont(rpcRsp.pCont);
  return code;
}

static int32_t dmBackupVnodeSyncCommitRpc(SDnodeMgmt *pMgmt, const SEpSet *pEpSet, int32_t vgId) {
  SVnodeSyncCommitReq req = {.vgId = vgId};
  int32_t reqLen = tSerializeSVnodeSyncCommitReq(NULL, 0, &req);
  if (reqLen < 0) {
    return reqLen;
  }
  int32_t contLen = sizeof(SMsgHead) + reqLen;
  void *pCont = rpcMallocCont(contLen);
  if (pCont == NULL) {
    return terrno;
  }
  SMsgHead *pHead = pCont;
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(vgId);
  if (tSerializeSVnodeSyncCommitReq((char *)pCont + sizeof(SMsgHead), reqLen, &req) < 0) {
    rpcFreeCont(pCont);
    return TSDB_CODE_INVALID_MSG;
  }

  SRpcMsg rpcMsg = {.pCont = pCont, .contLen = contLen, .msgType = TDMT_VND_SYNC_COMMIT};
  SRpcMsg rpcRsp = {0};
  int32_t code = rpcSendRecv(pMgmt->msgCb.clientRpc, (SEpSet *)pEpSet, &rpcMsg, &rpcRsp);
  if (code == 0) {
    code = rpcRsp.code;
  }
  rpcFreeCont(rpcRsp.pCont);
  return code;
}

static int32_t dmBackupPullVgroupSnapshot(SDnodeMgmt *pMgmt, SBackupTask *pTask, const SBackupVgroupInfo *pVgroup, const char *backupDir) {
  int32_t                  code = 0;
  bool                     started = false;
  SDmBackupFile            backupFile = {0};
  char                     snapshotDir[PATH_MAX] = {0};
  char                     chunkBase[TSDB_FILENAME_LEN] = {0};
  SBackupVgroupLeaderRsp  leaderRsp = {0};
  SBackupVnodeSnapReq     snapReq = {.vgId = pVgroup->vgId,
                                      .chainId = pTask->req.chainId,
                                      .backupId = pTask->req.backupId,
                                      .isFull = pTask->req.isFull};
  SBackupVnodeSnapRsp     snapRsp = {0};

  if ((code = dmBackupCheckCancelled(pMgmt, pTask)) != 0) {
    return code;
  }

  // Resolve immediately before BEGIN; the returned endpoint stays pinned for this session.
  code = dmBackupGetVgroupLeader(pMgmt, &pTask->req, pVgroup->vgId, &leaderRsp);
  if (code != 0) {
    goto _exit;
  }
  if ((code = dmBackupCheckCancelled(pMgmt, pTask)) != 0) {
    goto _exit;
  }

  code = dmBackupVnodeSyncCommitRpc(pMgmt, &leaderRsp.leaderEpSet, pVgroup->vgId);
  if (code != 0) {
    goto _exit;
  }
  snapReq.op = BACKUP_VNODE_SNAP_BEGIN;
  code = dmBackupVnodeSnapRpc(pMgmt, &leaderRsp.leaderEpSet, &snapReq, &snapRsp);
  if (code != 0) {
    goto _exit;
  }
  started = true;
  if ((code = dmBackupCheckCancelled(pMgmt, pTask)) != 0) {
    goto _exit;
  }

  // Store the vnode snapshot as equal-sized chunk files under vnode{id}/ so a
  // single vnode never exceeds the max file size of the backup filesystem.
  (void)snprintf(snapshotDir, sizeof(snapshotDir), "%s%svnode%d", backupDir, TD_DIRSEP, pVgroup->vgId);
  if ((code = taosMulMkDir(snapshotDir)) != 0) {
    goto _exit;
  }
  (void)snprintf(chunkBase, sizeof(chunkBase), "vnode%d", pVgroup->vgId);
  code = dmBackupFileCreateChunked(&backupFile, backupDir, chunkBase,
                                   pTask->req.hasPassword ? pTask->req.password : NULL);
  if (code != 0) {
    goto _exit;
  }
  SBackupVnodeDataHeader header = {.magic = BACKUP_VNODE_DATA_MAGIC, .snapshotIndex = snapRsp.snapshotIndex};
  if ((code = dmBackupFileWrite(&backupFile, &header, sizeof(header))) != 0) {
    goto _exit;
  }
  (void)taosThreadMutexLock(&pMgmt->backupMutex);
  pTask->totalSize += sizeof(header);
  (void)taosThreadMutexUnlock(&pMgmt->backupMutex);
  tFreeSBackupVnodeSnapRsp(&snapRsp);

  snapReq.op = BACKUP_VNODE_SNAP_FETCH;
  while (true) {
    if ((code = dmBackupCheckCancelled(pMgmt, pTask)) != 0) {
      goto _exit;
    }
    code = dmBackupVnodeSnapRpc(pMgmt, &leaderRsp.leaderEpSet, &snapReq, &snapRsp);
    if (code != 0) {
      goto _exit;
    }
    if ((code = dmBackupCheckCancelled(pMgmt, pTask)) != 0) {
      goto _exit;
    }
    if (snapRsp.dataLen > 0 &&
      (code = dmBackupFileWrite(&backupFile, snapRsp.data, snapRsp.dataLen)) != 0) {
      tFreeSBackupVnodeSnapRsp(&snapRsp);
      goto _exit;
    }
    if (snapRsp.dataLen > 0) {
      (void)taosThreadMutexLock(&pMgmt->backupMutex);
      pTask->totalSize += snapRsp.dataLen;
      (void)taosThreadMutexUnlock(&pMgmt->backupMutex);
    }
    bool eof = snapRsp.eof;
    tFreeSBackupVnodeSnapRsp(&snapRsp);
    if (eof) {
      break;
    }
  }
  code = dmBackupFileClose(&backupFile);
  if (code != 0) {
    goto _exit;
  }
  (void)taosThreadMutexLock(&pMgmt->backupMutex);
  if (pTask->vnodeSnapshots == NULL) {
    pTask->vnodeSnapshots = taosArrayInit(pTask->totalVgroups, sizeof(SBackupVnodeDigest));
    if (pTask->vnodeSnapshots == NULL) {
      code = terrno;
    }
  }
  SBackupVnodeDigest digest = {.vgId = pVgroup->vgId,
                               .snapshotIndex = header.snapshotIndex,
                               .numOfChunks = backupFile.numOfChunks};
  memcpy(digest.digest, backupFile.checksum, sizeof(backupFile.checksum));
  if (code == 0 && taosArrayPush(pTask->vnodeSnapshots, &digest) == NULL) {
    code = terrno;
  }
  (void)taosThreadMutexUnlock(&pMgmt->backupMutex);
  if (code != 0) {
    goto _exit;
  }
  dInfo("backup vgroup:%d snapshot written:%s chunks:%d leader dnode:%d", pVgroup->vgId, snapshotDir,
        backupFile.numOfChunks, leaderRsp.leaderDnodeId);

_exit:
  tFreeSBackupVnodeSnapRsp(&snapRsp);
  int32_t closeCode = dmBackupFileClose(&backupFile);
  if (code == 0 && closeCode != 0) {
    code = closeCode;
  }
  if (started) {
    SBackupVnodeSnapRsp releaseRsp = {0};
    snapReq.op = BACKUP_VNODE_SNAP_RELEASE;
    int32_t releaseCode = dmBackupVnodeSnapRpc(pMgmt, &leaderRsp.leaderEpSet, &snapReq, &releaseRsp);
    tFreeSBackupVnodeSnapRsp(&releaseRsp);
    if (releaseCode != 0 && code == 0) {
      code = releaseCode;
    }
  }
  return code;
}

// One parallel vgroup backup worker. Takes vgroup indices from a shared
// atomic counter, so any number of workers (even if some fail to start)
// still covers every vgroup exactly once.
static void *dmBackupVgroupWorkerFp(void *pParam) {
  SBackupVgroupWorkerCtx *pCtx = pParam;
  SBackupTask            *pTask = pCtx->pTask;

  while (true) {
    int32_t idx = atomic_fetch_add_32(&pCtx->nextVgIndex, 1);
    if (idx >= pTask->totalVgroups) {
      break;
    }

    int32_t code = dmBackupCheckCancelled(pCtx->pMgmt, pTask);
    if (code != 0) {
      // Propagate cancellation/error to other workers and the calling stage.
      (void)taosThreadMutexLock(&pCtx->pMgmt->backupMutex);
      if (pTask->code == 0) {
        pTask->code = code;
      }
      (void)taosThreadMutexUnlock(&pCtx->pMgmt->backupMutex);
      break;
    }

    SBackupVgroupInfo *pVgroup = taosArrayGet(pTask->req.vgroups, idx);

    code = dmBackupPullVgroupSnapshot(pCtx->pMgmt, pTask, pVgroup, pCtx->backupDir);
    if (code != 0) {
      dError("failed to backup vgroup:%d since %s", pVgroup->vgId, tstrerror(code));
      // Record the first failure; dmBackupCheckCancelled propagates it to
      // the other workers so they abort on their next check point.
      (void)taosThreadMutexLock(&pCtx->pMgmt->backupMutex);
      if (pTask->code == 0) {
        pTask->code = code;
      }
      (void)taosThreadMutexUnlock(&pCtx->pMgmt->backupMutex);
      break;
    }

    (void)taosThreadMutexLock(&pCtx->pMgmt->backupMutex);
    ++pTask->finishedVgroups;
    (void)taosThreadMutexUnlock(&pCtx->pMgmt->backupMutex);
  }
  return NULL;
}

// Back up all vgroups with up to req.parallel concurrent workers.
static int32_t dmBackupPullVgroups(SDnodeMgmt *pMgmt, SBackupTask *pTask, const char *backupDir) {
  int32_t code = 0;
  int32_t created = 0;
  int32_t parallelism = pTask->req.parallel > 1 ? pTask->req.parallel : 1;
  parallelism = TMIN(parallelism, pTask->totalVgroups);

  TdThread *threads = taosMemoryCalloc(parallelism, sizeof(TdThread));
  if (threads == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
  }

  SBackupVgroupWorkerCtx workerCtx = {
      .pMgmt = pMgmt,
      .pTask = pTask,
      .backupDir = backupDir,
      .nextVgIndex = 0,
  };

  for (int32_t i = 0; i < parallelism; ++i) {
    TdThreadAttr thAttr;
    (void)taosThreadAttrInit(&thAttr);
    (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (taosThreadCreate(&threads[i], &thAttr, dmBackupVgroupWorkerFp, &workerCtx) != 0) {
      code = TAOS_SYSTEM_ERROR(ERRNO);
      dError("failed to create backup vgroup worker:%d since %s", i, tstrerror(code));
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

  if (code == 0 && pTask->code != 0) {
    code = pTask->code;
  }
  return code;
}

int32_t dmProcessBackupTask(SDnodeMgmt *pMgmt, SBackupTask *pTask) {
  int32_t         code = 0;
  SBackupDnodeReq *pReq = &pTask->req;
  char            chainDir[PATH_MAX] = {0};
  char            backupDir[PATH_MAX] = {0};
  char            stagingDir[PATH_MAX] = {0};
  char           *publishDir = NULL;
  char           *backupWriteDir = NULL;
  bool            stagingDirCreated = false;

  dInfo("start backup task, root:%s chainId:%" PRId64 " backupId:%" PRId64, pReq->backupRoot, pReq->chainId, pReq->backupId);

  if (pReq->backupRoot[0] == '\0') {
    dError("failed to create backup chain since backup root is empty");
    return TSDB_CODE_INVALID_PARA;
  }
  if ((code = dmBackupCheckCancelled(pMgmt, pTask)) != 0) {
    return code;
  }

  (void)snprintf(chainDir, sizeof(chainDir), "%s%s%" PRId64, pReq->backupRoot, TD_DIRSEP, pReq->chainId);

  if (pReq->isFull) {
    (void)snprintf(stagingDir, sizeof(stagingDir), "%s.tmp", chainDir);
    if (taosDirExist(chainDir) || taosDirExist(stagingDir)) {
      dError("failed to create full backup since chain or temporary chain dir already exists:%s", chainDir);
      return TSDB_CODE_BACKUP_ALREADY_EXIST;
    }
    (void)snprintf(backupDir, sizeof(backupDir), "%s%s%" PRId64, stagingDir, TD_DIRSEP, pReq->backupId);
    publishDir = chainDir;
    backupWriteDir = backupDir;
  } else {
    if (!taosDirExist(chainDir)) {
      dError("failed to create incremental backup since chain dir does not exist:%s", chainDir);
      return TSDB_CODE_NOT_FOUND;
    }
    (void)snprintf(backupDir, sizeof(backupDir), "%s%s%" PRId64, chainDir, TD_DIRSEP, pReq->backupId);
    (void)snprintf(stagingDir, sizeof(stagingDir), "%s.tmp", backupDir);
    if (taosDirExist(backupDir) || taosDirExist(stagingDir)) {
      dError("failed to create incremental backup since backup or temporary dir already exists:%s", backupDir);
      return TSDB_CODE_BACKUP_ALREADY_EXIST;
    }
    publishDir = backupDir;
    backupWriteDir = stagingDir;
  }

  if ((code = taosMulMkDir(backupWriteDir)) != 0) {
    dError("failed to create temporary backup dir:%s since %s", backupWriteDir, tstrerror(code));
    return code;
  }
  stagingDirCreated = true;
  dInfo("backup temporary dir created:%s", backupWriteDir);

  if ((code = dmBackupPullMnodeMeta(pMgmt, pTask, backupWriteDir)) != 0) {
    dError("failed to backup mnode meta since %s", tstrerror(code));
    goto _exit;
  }
  if ((code = dmBackupCheckCancelled(pMgmt, pTask)) != 0) {
    goto _exit;
  }
  if ((code = dmBackupPullSnodeCheckpoints(pMgmt, pTask, backupWriteDir)) != 0) {
    dError("failed to backup snode checkpoints since %s", tstrerror(code));
    goto _exit;
  }

  pTask->totalVgroups = pReq->vgroups == NULL ? 0 : taosArrayGetSize(pReq->vgroups);
  if (pTask->totalVgroups > 0) {
    code = dmBackupPullVgroups(pMgmt, pTask, backupWriteDir);
    if (code != 0) {
      dError("failed to backup vgroups since %s", tstrerror(code));
      goto _exit;
    }
  }

  if ((code = dmBackupWriteManifest(pTask, backupWriteDir)) != 0) {
    dError("failed to write backup manifest since %s", tstrerror(code));
    goto _exit;
  }
  if ((code = dmBackupCheckCancelled(pMgmt, pTask)) != 0) {
    goto _exit;
  }
  if ((code = taosRenameFile(stagingDir, publishDir)) != 0) {
    dError("failed to publish backup from:%s to:%s since %s", stagingDir, publishDir, tstrerror(code));
    goto _exit;
  }
  stagingDirCreated = false;
  dInfo("backup task chainId:%" PRId64 " backupId:%" PRId64 " published:%s", pReq->chainId, pReq->backupId, publishDir);

_exit:
  if (stagingDirCreated && (code != 0 || dmBackupCheckCancelled(pMgmt, pTask) != 0)) {
    taosRemoveDir(stagingDir);
    dInfo("backup task chainId:%" PRId64 " backupId:%" PRId64 " removed partial output:%s", pReq->chainId, pReq->backupId, stagingDir);
  }
  return code;
}

int32_t dmProcessBackupReq(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SBackupTask     *pTask = NULL;
  SBackupTaskItem *pTaskItem = NULL;

  pTask = taosMemoryCalloc(1, sizeof(SBackupTask));
  if (pTask == NULL) {
    return terrno;
  }
  if (tDeserializeSBackupDnodeReq(pMsg->pCont, pMsg->contLen, &pTask->req) != 0) {
    taosMemoryFree(pTask);
    return TSDB_CODE_INVALID_MSG;
  }
  pTask->running = true;

  (void)taosThreadMutexLock(&pMgmt->backupMutex);
  if (pMgmt->pBackupTask != NULL && pMgmt->pBackupTask->running) {
    (void)taosThreadMutexUnlock(&pMgmt->backupMutex);
    tFreeSBackupDnodeReq(&pTask->req);
    taosMemoryFree(pTask);
    return TSDB_CODE_BACKUP_IN_PROGRESS;
  }
  if (pMgmt->pBackupTask != NULL) {
    tFreeSBackupDnodeReq(&pMgmt->pBackupTask->req);
    dmBackupFreeTaskArtifacts(pMgmt->pBackupTask);
  }
  taosMemoryFreeClear(pMgmt->pBackupTask);
  pMgmt->pBackupTask = pTask;
  (void)taosThreadMutexUnlock(&pMgmt->backupMutex);

  if (taosAllocateQitem(sizeof(SBackupTaskItem), DEF_QITEM, 0, (void **)&pTaskItem) != 0) {
    (void)taosThreadMutexLock(&pMgmt->backupMutex);
    if (pMgmt->pBackupTask == pTask) {
      pMgmt->pBackupTask = NULL;
    }
    (void)taosThreadMutexUnlock(&pMgmt->backupMutex);
    tFreeSBackupDnodeReq(&pTask->req);
    dmBackupFreeTaskArtifacts(pTask);
    taosMemoryFree(pTask);
    return terrno;
  }
  pTaskItem->pTask = pTask;

  int32_t code = dmPutBackupTaskToQueue(pMgmt, pTaskItem);
  if (code != 0) {
    taosFreeQitem(pTaskItem);
    (void)taosThreadMutexLock(&pMgmt->backupMutex);
    if (pMgmt->pBackupTask == pTask) {
      pMgmt->pBackupTask = NULL;
    }
    (void)taosThreadMutexUnlock(&pMgmt->backupMutex);
    tFreeSBackupDnodeReq(&pTask->req);
    dmBackupFreeTaskArtifacts(pTask);
    taosMemoryFree(pTask);
    return code;
  }

  dInfo("accepted backup task, chainId:%" PRId64 " backupId:%" PRId64, pTask->req.chainId, pTask->req.backupId);
  return 0;
}

int32_t dmProcessBackupProgressReq(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SBackupProgressReq req = {0};
  SBackupProgressRsp rsp = {0};

  if (tDeserializeSBackupProgressReq(pMsg->pCont, pMsg->contLen, &req) != 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  (void)taosThreadMutexLock(&pMgmt->backupMutex);
  SBackupTask *pTask = pMgmt->pBackupTask;
  if (pTask == NULL || pTask->req.chainId != req.chainId || pTask->req.backupId != req.backupId) {
    (void)taosThreadMutexUnlock(&pMgmt->backupMutex);
    return TSDB_CODE_SDB_OBJ_NOT_THERE;
  }
  rsp.chainId = pTask->req.chainId;
  rsp.backupId = pTask->req.backupId;
  rsp.finished = pTask->running ? 0 : 1;
  rsp.totalSize = pTask->totalSize;
  rsp.totalVgroups = pTask->totalVgroups;
  rsp.finishedVgroups = pTask->finishedVgroups;
  rsp.code = pTask->code;
  (void)taosThreadMutexUnlock(&pMgmt->backupMutex);

  int32_t contLen = tSerializeSBackupProgressRsp(NULL, 0, &rsp);
  if (contLen < 0) {
    return contLen;
  }
  void *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    return terrno;
  }
  if (tSerializeSBackupProgressRsp(pRsp, contLen, &rsp) < 0) {
    rpcFreeCont(pRsp);
    return TSDB_CODE_INVALID_MSG;
  }
  pMsg->info.rsp = pRsp;
  pMsg->info.rspLen = contLen;
  return 0;
}

int32_t dmProcessKillBackupReq(SDnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  SBackupProgressReq req = {0};
  if (tDeserializeSBackupProgressReq(pMsg->pCont, pMsg->contLen, &req) != 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  (void)taosThreadMutexLock(&pMgmt->backupMutex);
  SBackupTask *pTask = pMgmt->pBackupTask;
  if (pTask != NULL && pTask->running && pTask->req.chainId == req.chainId && pTask->req.backupId == req.backupId) {
    pTask->cancelled = true;
    dInfo("backup task chainId:%" PRId64 " backupId:%" PRId64 " cancellation requested", req.chainId, req.backupId);
  }
  (void)taosThreadMutexUnlock(&pMgmt->backupMutex);
  return 0;
}
