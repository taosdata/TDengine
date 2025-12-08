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

#define _DEFAULT_SOURCE
#include "tencrypt.h"
#include "crypt.h"
#include "os.h"
#include "tdef.h"
#include "tglobal.h"

/**
 * Write file with encryption header using atomic file replacement.
 *
 * This function writes data to a file with an encryption header at the beginning.
 * The encryption header contains:
 * - Magic number "tdEncrypt" for quick identification
 * - Algorithm identifier (e.g., SM4 = 1)
 * - File format version
 * - Length of encrypted data
 *
 * Atomic file replacement strategy:
 * 1. Write to temporary file: filepath.tmp.timestamp
 * 2. Sync temporary file to disk
 * 3. Atomically rename temporary file to target filepath
 * 4. Remove old file if rename succeeds
 *
 * This ensures the operation is atomic - no partial writes or corrupted files
 * even if the process is interrupted.
 *
 * @param filepath Target file path
 * @param algorithm Encryption algorithm identifier
 * @param data Data buffer to write (can be NULL for empty file with header only)
 * @param dataLen Length of data to write (0 for empty file)
 * @return 0 on success, error code on failure
 */
int32_t taosWriteEncryptFileHeader(const char *filepath, int32_t algorithm, const void *data, int32_t dataLen) {
  if (filepath == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  // Validate algorithm
  if (algorithm < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  // Validate data parameters
  if (dataLen > 0 && data == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  int32_t code = 0;
  int64_t now = taosGetTimestampMs();

  // Prepare encryption header (plaintext)
  STdEncryptFileHeader header;
  memset(&header, 0, sizeof(STdEncryptFileHeader));
  strncpy(header.magic, TD_ENCRYPT_FILE_MAGIC, TD_ENCRYPT_MAGIC_LEN - 1);
  header.algorithm = algorithm;
  header.version = TD_ENCRYPT_FILE_VERSION;
  header.dataLen = dataLen;

  // Create temporary file for atomic write
  char tempFile[PATH_MAX];
  snprintf(tempFile, sizeof(tempFile), "%s.tmp.%ld", filepath, now);

  // Open temp file
  TdFilePtr pFile = taosOpenFile(tempFile, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    code = terrno;
    return code;
  }

  // Write header (plaintext)
  int64_t written = taosWriteFile(pFile, &header, sizeof(STdEncryptFileHeader));
  if (written != sizeof(STdEncryptFileHeader)) {
    code = (terrno != 0) ? terrno : TSDB_CODE_FILE_CORRUPTED;
    taosCloseFile(&pFile);
    taosRemoveFile(tempFile);
    terrno = code;
    return code;
  }

  // Write data if present
  if (dataLen > 0 && data != NULL) {
    written = taosWriteFile(pFile, data, dataLen);
    if (written != dataLen) {
      code = (terrno != 0) ? terrno : TSDB_CODE_FILE_CORRUPTED;
      taosCloseFile(&pFile);
      taosRemoveFile(tempFile);
      terrno = code;
      return code;
    }
  }

  // Sync to disk
  code = taosFsyncFile(pFile);
  if (code != 0) {
    taosCloseFile(&pFile);
    taosRemoveFile(tempFile);
    return code;
  }

  // Close temp file
  taosCloseFile(&pFile);

  // Atomic replacement - rename temp file to target
  code = taosRenameFile(tempFile, filepath);
  if (code != 0) {
    taosRemoveFile(tempFile);
    return code;
  }

  return 0;
}

/**
 * Read encryption header from file.
 *
 * Reads and validates the encryption header from the beginning of a file.
 * Checks:
 * - Magic number matches "tdEncrypt"
 * - Version is supported
 *
 * @param filepath File path to read
 * @param header Output parameter for header data
 * @return 0 on success, error code on failure
 */
int32_t taosReadEncryptFileHeader(const char *filepath, STdEncryptFileHeader *header) {
  if (filepath == NULL || header == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  // Open file for reading
  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_READ);
  if (pFile == NULL) {
    return terrno;
  }

  // Read header
  int64_t nread = taosReadFile(pFile, header, sizeof(STdEncryptFileHeader));
  taosCloseFile(&pFile);

  if (nread != sizeof(STdEncryptFileHeader)) {
    terrno = TSDB_CODE_FILE_CORRUPTED;
    return terrno;
  }

  // Verify magic number
  if (strncmp(header->magic, TD_ENCRYPT_FILE_MAGIC, strlen(TD_ENCRYPT_FILE_MAGIC)) != 0) {
    terrno = TSDB_CODE_FILE_CORRUPTED;
    return terrno;
  }

  // Verify version (currently only version 1 is supported)
  if (header->version != TD_ENCRYPT_FILE_VERSION) {
    terrno = TSDB_CODE_FILE_CORRUPTED;
    return terrno;
  }

  return 0;
}

/**
 * Check if file has encryption header.
 *
 * Quickly checks if a file begins with the encryption magic number.
 * This is faster than reading the full header when you only need to
 * know if the file is encrypted.
 *
 * @param filepath File path to check
 * @param algorithm Output parameter for algorithm (can be NULL)
 * @return true if file is encrypted, false otherwise
 */
bool taosIsEncryptedFile(const char *filepath, int32_t *algorithm) {
  if (filepath == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return false;
  }

  // Check if file exists
  if (!taosCheckExistFile(filepath)) {
    return false;
  }

  // Read header
  STdEncryptFileHeader header;
  int32_t              code = taosReadEncryptFileHeader(filepath, &header);

  if (code != 0) {
    return false;
  }

  // Return algorithm if requested
  if (algorithm != NULL) {
    *algorithm = header.algorithm;
  }

  return true;
}

/**
 * Write configuration file with encryption support using atomic file replacement.
 *
 * This function writes a configuration file with optional encryption based on tsCfgKey.
 * If tsCfgKey is enabled (not empty), it encrypts the data using SM4 CBC algorithm
 * and writes it with an encryption header. Otherwise, it writes the file normally.
 *
 * Atomic file replacement strategy (same for both encrypted and plain files):
 * 1. Write to temporary file: filepath.tmp
 * 2. Sync temporary file to disk
 * 3. Atomically rename temporary file to target filepath
 * 4. Remove old file if rename succeeds
 *
 * @param filepath Target file path
 * @param data Data buffer to write
 * @param dataLen Length of data to write
 * @return 0 on success, error code on failure
 */
int32_t taosWriteCfgFile(const char *filepath, const void *data, int32_t dataLen) {
  if (filepath == NULL || data == NULL || dataLen <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  int32_t code = 0;
  char    tempFile[PATH_MAX];
  snprintf(tempFile, sizeof(tempFile), "%s.tmp", filepath);

  // Check if CFG_KEY encryption is enabled
  if (!tsCfgKeyEnabled || tsCfgKey[0] == '\0') {
    // No encryption, write file normally with atomic operation
    TdFilePtr pFile = taosOpenFile(tempFile, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
    if (pFile == NULL) {
      return terrno;
    }

    if (taosWriteFile(pFile, data, dataLen) != dataLen) {
      code = (terrno != 0) ? terrno : TSDB_CODE_FILE_CORRUPTED;
      taosCloseFile(&pFile);
      taosRemoveFile(tempFile);
      terrno = code;
      return code;
    }

    code = taosFsyncFile(pFile);
    if (code != 0) {
      taosCloseFile(&pFile);
      taosRemoveFile(tempFile);
      return code;
    }

    taosCloseFile(&pFile);

    // Atomic replacement - rename temp file to target
    code = taosRenameFile(tempFile, filepath);
    if (code != 0) {
      taosRemoveFile(tempFile);
      return code;
    }

    return 0;
  }

  // Encryption enabled - encrypt data first
  int32_t cryptedDataLen = ENCRYPTED_LEN(dataLen);
  char *plainBuf = NULL;
  char *encryptedBuf = NULL;

  // Allocate buffer for padding
  plainBuf = taosMemoryMalloc(cryptedDataLen);
  if (plainBuf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  // Copy data and zero padding
  (void)memset(plainBuf, 0, cryptedDataLen);
  (void)memcpy(plainBuf, data, dataLen);

  // Allocate buffer for encrypted data
  encryptedBuf = taosMemoryMalloc(cryptedDataLen);
  if (encryptedBuf == NULL) {
    taosMemoryFree(plainBuf);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  // Setup encryption options (similar to walWrite.c)
  SCryptOpts opts = {0};
  opts.len = cryptedDataLen;
  opts.source = plainBuf;
  opts.result = encryptedBuf;
  opts.unitLen = 16;
  opts.pOsslAlgrName = TSDB_ENCRYPT_ALGO_SM4_STR;
  tstrncpy((char *)opts.key, tsCfgKey, ENCRYPT_KEY_LEN + 1);

  // Encrypt the data
  int32_t count = Builtin_CBC_Encrypt(&opts);
  if (count != opts.len) {
    code = TSDB_CODE_FAILED;
    goto _cleanup;
  }

  // Write encrypted file with header (uses atomic operation internally)
  code = taosWriteEncryptFileHeader(filepath, TSDB_ENCRYPT_ALGO_SM4, encryptedBuf, cryptedDataLen);

_cleanup:
  if (plainBuf != NULL) taosMemoryFree(plainBuf);
  if (encryptedBuf != NULL) taosMemoryFree(encryptedBuf);

  if (code != 0) {
    terrno = code;
  }
  return code;
}

