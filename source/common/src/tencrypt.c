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

  // Prepare encryption header (plaintext)
  STdEncryptFileHeader header;
  memset(&header, 0, sizeof(STdEncryptFileHeader));
  strncpy(header.magic, TD_ENCRYPT_FILE_MAGIC, TD_ENCRYPT_MAGIC_LEN - 1);
  header.algorithm = algorithm;
  header.version = TD_ENCRYPT_FILE_VERSION;
  header.dataLen = dataLen;

  // Create temporary file for atomic write
  char tempFile[PATH_MAX];
  snprintf(tempFile, sizeof(tempFile), "%s.tmp", filepath);

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
    (void)taosCloseFile(&pFile);
    (void)taosRemoveFile(tempFile);
    terrno = code;
    return code;
  }

  // Write data if present
  if (dataLen > 0 && data != NULL) {
    written = taosWriteFile(pFile, data, dataLen);
    if (written != dataLen) {
      code = (terrno != 0) ? terrno : TSDB_CODE_FILE_CORRUPTED;
      (void)taosCloseFile(&pFile);
      (void)taosRemoveFile(tempFile);
      terrno = code;
      return code;
    }
  }

  // Sync to disk
  code = taosFsyncFile(pFile);
  if (code != 0) {
    (void)taosCloseFile(&pFile);
    (void)taosRemoveFile(tempFile);
    return code;
  }

  // Close temp file
  (void)taosCloseFile(&pFile);

  // Atomic replacement - rename temp file to target
  code = taosRenameFile(tempFile, filepath);
  if (code != 0) {
    (void)taosRemoveFile(tempFile);
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
  (void)taosCloseFile(&pFile);

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
  if (tsCfgKey[0] == '\0') {
    // No encryption, write file normally with atomic operation
    TdFilePtr pFile = taosOpenFile(tempFile, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
    if (pFile == NULL) {
      return terrno;
    }

    if (taosWriteFile(pFile, data, dataLen) != dataLen) {
      code = (terrno != 0) ? terrno : TSDB_CODE_FILE_CORRUPTED;
      (void)taosCloseFile(&pFile);
      (void)taosRemoveFile(tempFile);
      terrno = code;
      return code;
    }

    code = taosFsyncFile(pFile);
    if (code != 0) {
      (void)taosCloseFile(&pFile);
      (void)taosRemoveFile(tempFile);
      return code;
    }

    (void)taosCloseFile(&pFile);

    // Atomic replacement - rename temp file to target
    code = taosRenameFile(tempFile, filepath);
    if (code != 0) {
      (void)taosRemoveFile(tempFile);
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

/**
 * Read configuration file with automatic decryption support.
 *
 * This function reads a configuration file and automatically handles decryption if needed.
 * It checks if the file has an encryption header:
 * - If encrypted: reads header, reads encrypted data, decrypts using tsCfgKey
 * - If not encrypted: reads file content directly
 *
 * The caller is responsible for freeing the returned buffer.
 *
 * @param filepath File path to read
 * @param data Output parameter for data buffer (caller must free)
 * @param dataLen Output parameter for data length (actual plaintext length)
 * @return 0 on success, error code on failure
 */
int32_t taosReadCfgFile(const char *filepath, char **data, int32_t *dataLen) {
  if (filepath == NULL || data == NULL || dataLen == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  *data = NULL;
  *dataLen = 0;

  int32_t              code = 0;
  TdFilePtr            pFile = NULL;
  char                *fileContent = NULL;
  char                *decryptedBuf = NULL;
  STdEncryptFileHeader header;

  // Check if file exists
  if (!taosCheckExistFile(filepath)) {
    terrno = TSDB_CODE_FILE_CORRUPTED;
    return terrno;
  }

  // Check if file is encrypted
  bool isEncrypted = taosIsEncryptedFile(filepath, NULL);

  // Open file for reading
  pFile = taosOpenFile(filepath, TD_FILE_READ);
  if (pFile == NULL) {
    code = terrno;
    return code;
  }

  // Get file size
  int64_t fileSize = 0;
  code = taosFStatFile(pFile, &fileSize, NULL);
  if (code != 0) {
    (void)taosCloseFile(&pFile);
    return code;
  }

  if (fileSize <= 0) {
    (void)taosCloseFile(&pFile);
    terrno = TSDB_CODE_FILE_CORRUPTED;
    return terrno;
  }

  if (isEncrypted) {
    // File is encrypted - read header first
    int64_t nread = taosReadFile(pFile, &header, sizeof(STdEncryptFileHeader));
    if (nread != sizeof(STdEncryptFileHeader)) {
      code = (terrno != 0) ? terrno : TSDB_CODE_FILE_CORRUPTED;
      (void)taosCloseFile(&pFile);
      terrno = code;
      return code;
    }

    // Verify magic number
    if (strncmp(header.magic, TD_ENCRYPT_FILE_MAGIC, strlen(TD_ENCRYPT_FILE_MAGIC)) != 0) {
      (void)taosCloseFile(&pFile);
      terrno = TSDB_CODE_FILE_CORRUPTED;
      return terrno;
    }

    // Read encrypted data
    int32_t encryptedDataLen = header.dataLen;
    if (encryptedDataLen <= 0 || encryptedDataLen > fileSize) {
      (void)taosCloseFile(&pFile);
      terrno = TSDB_CODE_FILE_CORRUPTED;
      return terrno;
    }

    fileContent = taosMemoryMalloc(encryptedDataLen);
    if (fileContent == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      (void)taosCloseFile(&pFile);
      terrno = code;
      return code;
    }

    nread = taosReadFile(pFile, fileContent, encryptedDataLen);
    if (nread != encryptedDataLen) {
      code = (terrno != 0) ? terrno : TSDB_CODE_FILE_CORRUPTED;
      taosMemoryFree(fileContent);
      (void)taosCloseFile(&pFile);
      terrno = code;
      return code;
    }

    (void)taosCloseFile(&pFile);

    // Check if CFG_KEY is available
    if (tsCfgKey[0] == '\0') {
      // File is encrypted but no key available
      taosMemoryFree(fileContent);
      terrno = TSDB_CODE_FAILED;
      return terrno;
    }

    // Decrypt data (reference: sdbFile.c decrypt implementation)
    // Allocate buffer for plaintext (same size as encrypted data for CBC padding)
    char *plainContent = taosMemoryMalloc(encryptedDataLen);
    if (plainContent == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      taosMemoryFree(fileContent);
      terrno = code;
      return code;
    }

    // Setup decryption options
    SCryptOpts opts = {0};
    opts.len = encryptedDataLen;
    opts.source = fileContent;
    opts.result = plainContent;
    opts.unitLen = 16;
    tstrncpy(opts.key, tsCfgKey, ENCRYPT_KEY_LEN + 1);

    // Decrypt the data
    int32_t count = Builtin_CBC_Decrypt(&opts);
    if (count != encryptedDataLen) {
      code = TSDB_CODE_FAILED;
      taosMemoryFree(fileContent);
      taosMemoryFree(plainContent);
      terrno = code;
      return code;
    }

    taosMemoryFree(fileContent);

    // Return decrypted data (JSON parser will handle the content)
    // Note: plainContent already has padding zeros from decryption, which is fine for JSON
    *data = plainContent;
    *dataLen = encryptedDataLen;

  } else {
    // File is not encrypted - read directly
    fileContent = taosMemoryMalloc(fileSize + 1);
    if (fileContent == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      (void)taosCloseFile(&pFile);
      terrno = code;
      return code;
    }

    int64_t nread = taosReadFile(pFile, fileContent, fileSize);
    if (nread != fileSize) {
      code = (terrno != 0) ? terrno : TSDB_CODE_FILE_CORRUPTED;
      taosMemoryFree(fileContent);
      (void)taosCloseFile(&pFile);
      terrno = code;
      return code;
    }

    (void)taosCloseFile(&pFile);

    fileContent[fileSize] = '\0';

    // Return file content
    *data = fileContent;
    *dataLen = fileSize;
  }

  return 0;
}

/**
 * Encrypt a single configuration file if it's not already encrypted.
 *
 * This function checks if a file exists and is not encrypted, then encrypts it in place.
 * The operation is atomic - uses temporary file and rename.
 *
 * @param filepath File path to encrypt
 * @return 0 on success or file already encrypted, error code on failure
 */
static int32_t taosEncryptSingleCfgFile(const char *filepath) {
  if (filepath == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Check if file exists
  if (!taosCheckExistFile(filepath)) {
    // File doesn't exist, nothing to do
    return 0;
  }

  // Check if file is already encrypted
  if (taosIsEncryptedFile(filepath, NULL)) {
    // Already encrypted, nothing to do
    return 0;
  }

  // Read plaintext file
  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_READ);
  if (pFile == NULL) {
    return terrno;
  }

  int64_t fileSize = 0;
  int32_t code = taosFStatFile(pFile, &fileSize, NULL);
  if (code != 0) {
    (void)taosCloseFile(&pFile);
    return code;
  }

  if (fileSize <= 0) {
    (void)taosCloseFile(&pFile);
    // Empty file, just skip it
    return 0;
  }

  char *plainData = taosMemoryMalloc(fileSize);
  if (plainData == NULL) {
    (void)taosCloseFile(&pFile);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int64_t nread = taosReadFile(pFile, plainData, fileSize);
  (void)taosCloseFile(&pFile);

  if (nread != fileSize) {
    taosMemoryFree(plainData);
    return (terrno != 0) ? terrno : TSDB_CODE_FILE_CORRUPTED;
  }

  // Encrypt the file using taosWriteCfgFile (which handles encryption and atomic write)
  code = taosWriteCfgFile(filepath, plainData, fileSize);
  taosMemoryFree(plainData);

  return code;
}

/**
 * Encrypt existing configuration files that are not yet encrypted.
 *
 * This function scans common configuration file locations and encrypts any
 * plaintext files it finds. It's called after encryption keys are loaded
 * to ensure all sensitive config files are encrypted.
 *
 * Files checked:
 * - dnode: dnode.info, dnode.json
 * - mnode: mnode.json, raft_config.json, raft_store.json
 * - vnode: vnodes.json, vnode.json (all vnodes), raft_config.json, raft_store.json, current.json
 * - snode: snode.json
 *
 * @param dataDir Data directory path (tsDataDir)
 * @return 0 on success, error code on failure (first error encountered)
 */
int32_t taosEncryptExistingCfgFiles(const char *dataDir) {
  if (dataDir == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Check if encryption is enabled
  if (tsCfgKey[0] == '\0') {
    // Encryption not enabled, nothing to do
    return 0;
  }

  int32_t code = 0;
  char    filepath[PATH_MAX];

  // 1. Encrypt dnode config files
  // dnode.info
  snprintf(filepath, sizeof(filepath), "%s%sdnode%sdnode.info", dataDir, TD_DIRSEP, TD_DIRSEP);
  if (taosCheckExistFile(filepath) && !taosIsEncryptedFile(filepath, NULL)) {
    code = taosEncryptSingleCfgFile(filepath);
    if (code != 0) {
      goto _err;
    } else {
      uInfo("successfully encrypted file %s", filepath);
    }
  }

  // dnode.json (ep.json)
  snprintf(filepath, sizeof(filepath), "%s%sdnode%sdnode.json", dataDir, TD_DIRSEP, TD_DIRSEP);
  if (taosCheckExistFile(filepath) && !taosIsEncryptedFile(filepath, NULL)) {
    code = taosEncryptSingleCfgFile(filepath);
    if (code != 0) {
      goto _err;
    } else {
      uInfo("successfully encrypted file %s", filepath);
    }
  }

  // 2. Encrypt mnode config files
  snprintf(filepath, sizeof(filepath), "%s%smnode%smnode.json", dataDir, TD_DIRSEP, TD_DIRSEP);
  if (taosCheckExistFile(filepath) && !taosIsEncryptedFile(filepath, NULL)) {
    code = taosEncryptSingleCfgFile(filepath);
    if (code != 0) {
      goto _err;
    } else {
      uInfo("successfully encrypted file %s", filepath);
    }
  }

  snprintf(filepath, sizeof(filepath), "%s%smnode%ssync%sraft_config.json", dataDir, TD_DIRSEP, TD_DIRSEP, TD_DIRSEP);
  if (taosCheckExistFile(filepath) && !taosIsEncryptedFile(filepath, NULL)) {
    code = taosEncryptSingleCfgFile(filepath);
    if (code != 0) {
      goto _err;
    } else {
      uInfo("successfully encrypted file %s", filepath);
    }
  }

  snprintf(filepath, sizeof(filepath), "%s%smnode%ssync%sraft_store.json", dataDir, TD_DIRSEP, TD_DIRSEP, TD_DIRSEP);
  if (taosCheckExistFile(filepath) && !taosIsEncryptedFile(filepath, NULL)) {
    code = taosEncryptSingleCfgFile(filepath);
    if (code != 0) {
      goto _err;
    } else {
      uInfo("successfully encrypted file %s", filepath);
    }
  }
  // 3. Encrypt snode config files
  snprintf(filepath, sizeof(filepath), "%s%ssnode%ssnode.json", dataDir, TD_DIRSEP, TD_DIRSEP);
  if (taosCheckExistFile(filepath) && !taosIsEncryptedFile(filepath, NULL)) {
    code = taosEncryptSingleCfgFile(filepath);
    if (code != 0) {
      goto _err;
    } else {
      uInfo("successfully encrypted file %s", filepath);
    }
  }

  // 4. Encrypt vnode config files
  // vnodes.json
  snprintf(filepath, sizeof(filepath), "%s%svnode%svnodes.json", dataDir, TD_DIRSEP, TD_DIRSEP);
  if (taosCheckExistFile(filepath) && !taosIsEncryptedFile(filepath, NULL)) {
    code = taosEncryptSingleCfgFile(filepath);
    if (code != 0) {
      goto _err;
    } else {
      uInfo("successfully encrypted file %s", filepath);
    }
  }

  // Note: Individual vnode directories (vnode1, vnode2, etc.) are not traversed here
  // because they would require scanning the vnode directory structure.
  // These files will be encrypted on next write by taosWriteCfgFile.

  uInfo("finished encrypting existing config files");
  return 0;
_err:
  uError("failed to encrypt existing config files, code:%s", tstrerror(code));
  return code;
}

/**
 * Wait for CFG encryption key to be loaded with timeout.
 *
 * This function polls the encryption key status at regular intervals (100ms).
 * It returns immediately if the key is already loaded, otherwise it waits
 * until either the key is loaded or the timeout expires.
 *
 * Timeout is controlled by TD_ENCRYPT_KEY_WAIT_TIMEOUT_MS macro.
 *
 * @return 0 if key loaded successfully, TSDB_CODE_TIMEOUT_ERROR if timeout occurs
 */
int32_t taosWaitCfgKeyLoaded(void) {
  int32_t encryptKeysLoaded = atomic_load_32(&tsEncryptKeysStatus);
  if (encryptKeysLoaded == TSDB_ENCRYPT_KEY_STAT_LOADED || encryptKeysLoaded == TSDB_ENCRYPT_KEY_STAT_NOT_EXIST ||
      encryptKeysLoaded == TSDB_ENCRYPT_KEY_STAT_DISABLED) {
    return 0;
  }

  const int32_t checkIntervalMs = 100;  // Check every 100ms
  int32_t       elapsedMs = 0;

  while (elapsedMs < TD_ENCRYPT_KEY_WAIT_TIMEOUT_MS) {
    // Check if CFG key is loaded
    encryptKeysLoaded = atomic_load_32(&tsEncryptKeysStatus);
    if (encryptKeysLoaded == TSDB_ENCRYPT_KEY_STAT_LOADED || encryptKeysLoaded == TSDB_ENCRYPT_KEY_STAT_NOT_EXIST ||
        encryptKeysLoaded == TSDB_ENCRYPT_KEY_STAT_DISABLED) {
      uDebug("CFG encryption key loaded successfully after %d ms", elapsedMs);
      return 0;
    }

    // Sleep for check interval
    taosMsleep(checkIntervalMs);
    elapsedMs += checkIntervalMs;
  }

  // Timeout occurred
  uError("failed to wait for CFG encryption key to load, waited %d ms", elapsedMs);
  terrno = TSDB_CODE_TIMEOUT_ERROR;
  return TSDB_CODE_TIMEOUT_ERROR;
}
