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

#include <sys/stat.h>
#include "crypt.h"
#include "taoskInt.h"
#include "tbase64.h"

extern STaoskArgs g_args;

// ============================================================================
// Data Directory Helper Functions
// ============================================================================

/**
 * Parse taos.cfg to get dataDir value
 *
 * @param configPath Config directory or file path
 * @param dataDir Output buffer for dataDir value
 * @param dataDirLen Size of dataDir buffer
 * @return 0 on success, error code on failure
 */
int32_t taoskParseDataDir(const char *configPath, char *dataDir, int32_t dataDirLen) {
  if (configPath == NULL || dataDir == NULL || dataDirLen <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  char cfgFile[PATH_MAX] = {0};

  // Check if configPath is a directory or file
  if (taosIsDir(configPath)) {
    // It's a directory, append taos.cfg
    snprintf(cfgFile, sizeof(cfgFile), "%s%staos.cfg", configPath, TD_DIRSEP);
  } else {
    // It's a file, use directly
    tstrncpy(cfgFile, configPath, sizeof(cfgFile));
  }

  // Check if config file exists
  if (!taosCheckExistFile(cfgFile)) {
    fprintf(stderr, "Warning: Config file not found: %s, using default dataDir\n", cfgFile);
    return TSDB_CODE_CFG_NOT_FOUND;
  }

  // Open config file
  TdFilePtr pFile = taosOpenFile(cfgFile, TD_FILE_READ);
  if (pFile == NULL) {
    fprintf(stderr, "Warning: Failed to open config file: %s\n", cfgFile);
    return terrno;
  }

  // Get file size
  int64_t fileSize = 0;
  if (taosStatFile(cfgFile, &fileSize, NULL, NULL) < 0) {
    taosCloseFile(&pFile);
    return terrno;
  }

  if (fileSize <= 0 || fileSize > 10 * 1024 * 1024) {  // Max 10MB
    taosCloseFile(&pFile);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  // Read file content
  char *content = taosMemoryMalloc(fileSize + 1);
  if (content == NULL) {
    taosCloseFile(&pFile);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (taosReadFile(pFile, content, fileSize) != fileSize) {
    int32_t err = terrno;
    taosMemoryFree(content);
    taosCloseFile(&pFile);
    return err;
  }
  content[fileSize] = '\0';
  taosCloseFile(&pFile);

  // Parse line by line to find dataDir
  char *line = content;
  char *nextLine = NULL;
  bool  found = false;

  while (line != NULL && *line != '\0') {
    // Find next line
    nextLine = strchr(line, '\n');
    if (nextLine != NULL) {
      *nextLine = '\0';
      nextLine++;
    }

    // Trim leading spaces
    while (*line == ' ' || *line == '\t') {
      line++;
    }

    // Skip empty lines and comments
    if (*line == '\0' || *line == '#') {
      line = nextLine;
      continue;
    }

    // Check if line starts with "dataDir"
    if (strncmp(line, "dataDir", 7) == 0) {
      char *p = line + 7;

      // Skip spaces after "dataDir"
      while (*p == ' ' || *p == '\t') {
        p++;
      }

      // Skip to value (after any separator)
      if (*p != '\0') {
        // Trim trailing spaces and comments
        char *valueStart = p;
        char *valueEnd = valueStart;

        while (*valueEnd != '\0' && *valueEnd != '#' && *valueEnd != '\n' && *valueEnd != '\r') {
          valueEnd++;
        }

        // Trim trailing spaces
        while (valueEnd > valueStart && (*(valueEnd - 1) == ' ' || *(valueEnd - 1) == '\t')) {
          valueEnd--;
        }

        int32_t valueLen = valueEnd - valueStart;
        if (valueLen > 0 && valueLen < dataDirLen) {
          strncpy(dataDir, valueStart, valueLen);
          dataDir[valueLen] = '\0';
          found = true;
          break;
        }
      }
    }

    line = nextLine;
  }

  taosMemoryFree(content);

  if (!found) {
    fprintf(stderr, "Warning: dataDir not found in config file, using default\n");
    return TSDB_CODE_CFG_NOT_FOUND;
  }

  return TSDB_CODE_SUCCESS;
}

// ============================================================================
// Internal Helper Functions
// ============================================================================

// Build master key data structure (svrKey and dbKey only)
int32_t taoskBuildMasterKeyData(const char *svrKey, const char *dbKey, int32_t algorithm, int32_t cfgAlgorithm,
                                int32_t metaAlgorithm, int32_t keyVersion, int64_t createTime, int64_t svrKeyUpdateTime,
                                int64_t dbKeyUpdateTime, SMasterKeyData *keyData) {
  if (svrKey == NULL || dbKey == NULL || keyData == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(keyData, 0, sizeof(SMasterKeyData));
  int64_t now = taosGetTimestampMs();
  int32_t code = 0;
  char   *machineId = NULL;
  char   *encrypted = NULL;

  // Get machine ID for encryption
  code = tGetMachineId(&machineId);
  if (code != 0 || machineId == NULL) {
    return code;
  }

  // Encrypt SVR_KEY with machine code
  code = taoskEncryptData(svrKey, machineId, &encrypted);
  taosMemoryFreeClear(machineId);
  if (code != 0) {
    return code;
  }
  strncpy(keyData->svrKeyEncrypted, encrypted, ENCRYPTED_KEY_MAX_LEN);
  taosMemoryFreeClear(encrypted);

  // Encrypt DB_KEY with SVR_KEY
  code = taoskEncryptData(dbKey, svrKey, &encrypted);
  if (code != 0) {
    return code;
  }
  strncpy(keyData->dbKeyEncrypted, encrypted, ENCRYPTED_KEY_MAX_LEN);
  taosMemoryFreeClear(encrypted);

  // Set metadata
  keyData->metadata.algorithm = (algorithm > 0) ? algorithm : ENCRYPT_FILE_VERSION;
  keyData->metadata.cfgAlgorithm = (cfgAlgorithm > 0) ? cfgAlgorithm : algorithm;
  keyData->metadata.metaAlgorithm = (metaAlgorithm > 0) ? metaAlgorithm : algorithm;
  keyData->metadata.keyVersion = (keyVersion > 0) ? keyVersion : 1;
  keyData->metadata.createTime = (createTime > 0) ? createTime : now;
  keyData->metadata.svrKeyUpdateTime = (svrKeyUpdateTime > 0) ? svrKeyUpdateTime : now;
  keyData->metadata.dbKeyUpdateTime = (dbKeyUpdateTime > 0) ? dbKeyUpdateTime : now;

  return 0;
}

// Build derived key data structure (cfgKey, metaKey, dataKey)
int32_t taoskBuildDerivedKeyData(const char *cfgKey, const char *metaKey, const char *dataKey, const char *dbKey,
                                 int32_t cfgAlgorithm, int32_t metaAlgorithm, SDerivedKeyData *keyData) {
  if (dbKey == NULL || keyData == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(keyData, 0, sizeof(SDerivedKeyData));
  int32_t code = 0;
  char   *encrypted = NULL;

  // Encrypt CFG_KEY with DB_KEY
  if (cfgKey != NULL && cfgKey[0] != '\0') {
    code = taoskEncryptData(cfgKey, dbKey, &encrypted);
    if (code == 0 && encrypted != NULL) {
      strncpy(keyData->cfgKeyEncrypted, encrypted, ENCRYPTED_KEY_MAX_LEN);
      keyData->cfgKeyEnabled = true;
      keyData->cfgAlgorithm = cfgAlgorithm;
      taosMemoryFreeClear(encrypted);
    }
  }

  // Encrypt META_KEY with DB_KEY
  if (metaKey != NULL && metaKey[0] != '\0') {
    code = taoskEncryptData(metaKey, dbKey, &encrypted);
    if (code == 0 && encrypted != NULL) {
      strncpy(keyData->metaKeyEncrypted, encrypted, ENCRYPTED_KEY_MAX_LEN);
      keyData->metaKeyEnabled = true;
      keyData->metaAlgorithm = metaAlgorithm;
      taosMemoryFreeClear(encrypted);
    }
  }

  // Encrypt DATA_KEY with DB_KEY
  if (dataKey != NULL && dataKey[0] != '\0') {
    code = taoskEncryptData(dataKey, dbKey, &encrypted);
    if (code == 0 && encrypted != NULL) {
      strncpy(keyData->dataKeyEncrypted, encrypted, ENCRYPTED_KEY_MAX_LEN);
      keyData->dataKeyEnabled = true;
      taosMemoryFreeClear(encrypted);
    }
  }

  keyData->generationTime = taosGetTimestampMs();

  return 0;
}

// Write master key data to file with atomic operation
int32_t taoskWriteMasterKeyFile(const char *filepath, const SMasterKeyData *keyData) {
  if (filepath == NULL || keyData == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = 0;
  int64_t now = taosGetTimestampMs();

  // Prepare header
  SEncryptFileHeader header = {0};
  strncpy(header.magic, ENCRYPT_FILE_MAGIC, sizeof(header.magic) - 1);
  header.version = ENCRYPT_FILE_VERSION;
  header.dataLen = sizeof(SMasterKeyData);

  // Write to temp file (atomic operation)
  char tempFile[PATH_MAX] = {0};
  snprintf(tempFile, sizeof(tempFile), "%s.tmp.%" PRId64, filepath, now);

  TdFilePtr pFile = taosOpenFile(tempFile, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    return terrno;
  }

  // Write header
  if (taosWriteFile(pFile, &header, sizeof(SEncryptFileHeader)) != sizeof(SEncryptFileHeader)) {
    code = terrno;
    taosCloseFile(&pFile);
    taosRemoveFile(tempFile);
    return code;
  }

  // Write master key data
  if (taosWriteFile(pFile, keyData, sizeof(SMasterKeyData)) != sizeof(SMasterKeyData)) {
    code = terrno;
    taosCloseFile(&pFile);
    taosRemoveFile(tempFile);
    return code;
  }

  taosFsyncFile(pFile);
  taosCloseFile(&pFile);

  // Set file permissions (600 - owner read/write only)
  chmod(tempFile, 0600);

  // Atomic replacement
  if (taosRenameFile(tempFile, filepath) != 0) {
    code = terrno;
    taosRemoveFile(tempFile);
    return code;
  }

  return 0;
}

// Write derived key data to file with atomic operation
// This ensures only one derived key file exists at a time
int32_t taoskWriteDerivedKeyFile(const char *filepath, const SDerivedKeyData *keyData) {
  if (filepath == NULL || keyData == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = 0;
  int64_t now = taosGetTimestampMs();

  // Prepare header
  SEncryptFileHeader header = {0};
  strncpy(header.magic, ENCRYPT_FILE_MAGIC, sizeof(header.magic) - 1);
  header.version = ENCRYPT_FILE_VERSION;
  header.dataLen = sizeof(SDerivedKeyData);

  // Write to temp file (atomic operation)
  char tempFile[PATH_MAX] = {0};
  snprintf(tempFile, sizeof(tempFile), "%s.tmp.%" PRId64, filepath, now);

  TdFilePtr pFile = taosOpenFile(tempFile, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    return terrno;
  }

  // Write header
  if (taosWriteFile(pFile, &header, sizeof(SEncryptFileHeader)) != sizeof(SEncryptFileHeader)) {
    code = terrno;
    taosCloseFile(&pFile);
    taosRemoveFile(tempFile);
    return code;
  }

  // Write derived key data
  if (taosWriteFile(pFile, keyData, sizeof(SDerivedKeyData)) != sizeof(SDerivedKeyData)) {
    code = terrno;
    taosCloseFile(&pFile);
    taosRemoveFile(tempFile);
    return code;
  }

  taosFsyncFile(pFile);
  taosCloseFile(&pFile);

  // Set file permissions (600 - owner read/write only)
  chmod(tempFile, 0600);

  // Atomic replacement - this ensures only one derived key file exists
  if (taosRenameFile(tempFile, filepath) != 0) {
    code = terrno;
    taosRemoveFile(tempFile);
    return code;
  }

  return 0;
}

// Read master key file and decrypt svrKey and dbKey
int32_t taoskReadMasterKeyFile(const char *filepath, char **svrKey, char **dbKey, SMasterKeyData *keyData) {
  if (filepath == NULL || svrKey == NULL || dbKey == NULL || keyData == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_READ);
  if (pFile == NULL) {
    fprintf(stderr, "Error: Failed to open master key file: %s\n", filepath);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  // Read header
  SEncryptFileHeader header = {0};
  int64_t            nread = taosReadFile(pFile, &header, sizeof(SEncryptFileHeader));
  if (nread != sizeof(SEncryptFileHeader)) {
    fprintf(stderr, "Error: Failed to read master key file header\n");
    taosCloseFile(&pFile);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  // Verify magic
  if (strncmp(header.magic, ENCRYPT_FILE_MAGIC, strlen(ENCRYPT_FILE_MAGIC)) != 0) {
    fprintf(stderr, "Error: Invalid master key file format\n");
    taosCloseFile(&pFile);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  // Read master key data
  nread = taosReadFile(pFile, keyData, sizeof(SMasterKeyData));
  if (nread != sizeof(SMasterKeyData)) {
    fprintf(stderr, "Error: Failed to read master key data\n");
    taosCloseFile(&pFile);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  taosCloseFile(&pFile);

  // Get machine ID for decryption
  char   *machineId = NULL;
  int32_t code = tGetMachineId(&machineId);
  if (code != 0 || machineId == NULL) {
    fprintf(stderr, "Error: Failed to get machine code for decryption\n");
    return code;
  }

  // Decrypt SVR_KEY with machine code
  code = taoskDecryptData(keyData->svrKeyEncrypted, machineId, svrKey);
  taosMemoryFreeClear(machineId);
  if (code != 0 || *svrKey == NULL) {
    fprintf(stderr, "Error: Failed to decrypt SVR_KEY\n");
    return code;
  }

  // Decrypt DB_KEY with SVR_KEY
  code = taoskDecryptData(keyData->dbKeyEncrypted, *svrKey, dbKey);
  if (code != 0 || *dbKey == NULL) {
    fprintf(stderr, "Error: Failed to decrypt DB_KEY\n");
    taosMemoryFreeClear(*svrKey);
    return code;
  }

  return 0;
}

// Read derived key file and decrypt cfgKey, metaKey, dataKey
int32_t taoskReadDerivedKeyFile(const char *filepath, const char *dbKey, char **cfgKey, char **metaKey, char **dataKey,
                                SDerivedKeyData *keyData) {
  if (filepath == NULL || dbKey == NULL || keyData == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_READ);
  if (pFile == NULL) {
    fprintf(stderr, "Error: Failed to open derived key file: %s\n", filepath);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  // Read header
  SEncryptFileHeader header = {0};
  int64_t            nread = taosReadFile(pFile, &header, sizeof(SEncryptFileHeader));
  if (nread != sizeof(SEncryptFileHeader)) {
    fprintf(stderr, "Error: Failed to read derived key file header\n");
    taosCloseFile(&pFile);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  // Verify magic
  if (strncmp(header.magic, ENCRYPT_FILE_MAGIC, strlen(ENCRYPT_FILE_MAGIC)) != 0) {
    fprintf(stderr, "Error: Invalid derived key file format\n");
    taosCloseFile(&pFile);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  // Read derived key data
  nread = taosReadFile(pFile, keyData, sizeof(SDerivedKeyData));
  if (nread != sizeof(SDerivedKeyData)) {
    fprintf(stderr, "Error: Failed to read derived key data\n");
    taosCloseFile(&pFile);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  taosCloseFile(&pFile);

  int32_t code = 0;

  // Decrypt CFG_KEY with DB_KEY
  if (keyData->cfgKeyEnabled && cfgKey != NULL) {
    code = taoskDecryptData(keyData->cfgKeyEncrypted, dbKey, cfgKey);
    if (code != 0) {
      fprintf(stderr, "Error: Failed to decrypt CFG_KEY\n");
      return code;
    }
  }

  // Decrypt META_KEY with DB_KEY
  if (keyData->metaKeyEnabled && metaKey != NULL) {
    code = taoskDecryptData(keyData->metaKeyEncrypted, dbKey, metaKey);
    if (code != 0) {
      fprintf(stderr, "Error: Failed to decrypt META_KEY\n");
      if (cfgKey && *cfgKey) taosMemoryFreeClear(*cfgKey);
      return code;
    }
  }

  // Decrypt DATA_KEY with DB_KEY
  if (keyData->dataKeyEnabled && dataKey != NULL) {
    code = taoskDecryptData(keyData->dataKeyEncrypted, dbKey, dataKey);
    if (code != 0) {
      fprintf(stderr, "Error: Failed to decrypt DATA_KEY\n");
      if (cfgKey && *cfgKey) taosMemoryFreeClear(*cfgKey);
      if (metaKey && *metaKey) taosMemoryFreeClear(*metaKey);
      return code;
    }
  }

  return 0;
}

// Build encrypted key data structure with multi-layer encryption
int32_t taoskBuildEncryptedKeyData(const char *svrKey, const char *dbKey, const char *cfgKey, const char *metaKey,
                                   const char *dataKey, int32_t algorithm, int32_t keyVersion, int64_t createTime,
                                   int64_t svrKeyUpdateTime, int64_t dbKeyUpdateTime, SEncryptedKeyData *keyData) {
  if (svrKey == NULL || dbKey == NULL || keyData == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  memset(keyData, 0, sizeof(SEncryptedKeyData));
  int64_t now = taosGetTimestampMs();
  int32_t code = 0;
  char   *machineId = NULL;
  char   *encrypted = NULL;

  // Get machine ID for encryption
  code = tGetMachineId(&machineId);
  if (code != 0 || machineId == NULL) {
    return code;
  }

  // Layer 1: Encrypt SVR_KEY with machine code
  code = taoskEncryptData(svrKey, machineId, &encrypted);
  taosMemoryFreeClear(machineId);
  if (code != 0) {
    return code;
  }
  strncpy(keyData->svrKeyEncrypted, encrypted, ENCRYPTED_KEY_MAX_LEN);
  taosMemoryFreeClear(encrypted);

  // Layer 2: Encrypt DB_KEY with SVR_KEY
  code = taoskEncryptData(dbKey, svrKey, &encrypted);
  if (code != 0) {
    return code;
  }
  strncpy(keyData->dbKeyEncrypted, encrypted, ENCRYPTED_KEY_MAX_LEN);
  taosMemoryFreeClear(encrypted);

  // Layer 3: Encrypt sub-keys with DB_KEY
  if (cfgKey != NULL && cfgKey[0] != '\0') {
    code = taoskEncryptData(cfgKey, dbKey, &encrypted);
    if (code == 0 && encrypted != NULL) {
      strncpy(keyData->cfgKeyEncrypted, encrypted, ENCRYPTED_KEY_MAX_LEN);
      keyData->cfgKeyEnabled = true;
      taosMemoryFreeClear(encrypted);
    }
  }

  if (metaKey != NULL && metaKey[0] != '\0') {
    code = taoskEncryptData(metaKey, dbKey, &encrypted);
    if (code == 0 && encrypted != NULL) {
      strncpy(keyData->metaKeyEncrypted, encrypted, ENCRYPTED_KEY_MAX_LEN);
      keyData->metaKeyEnabled = true;
      taosMemoryFreeClear(encrypted);
    }
  }

  if (dataKey != NULL && dataKey[0] != '\0') {
    code = taoskEncryptData(dataKey, dbKey, &encrypted);
    if (code == 0 && encrypted != NULL) {
      strncpy(keyData->dataKeyEncrypted, encrypted, ENCRYPTED_KEY_MAX_LEN);
      keyData->dataKeyEnabled = true;
      taosMemoryFreeClear(encrypted);
    }
  }

  // Set metadata
  keyData->metadata.algorithm = (algorithm > 0) ? algorithm : ENCRYPT_FILE_VERSION;
  keyData->metadata.keyVersion = (keyVersion > 0) ? keyVersion : 1;
  keyData->metadata.createTime = (createTime > 0) ? createTime : now;
  keyData->metadata.svrKeyUpdateTime = (svrKeyUpdateTime > 0) ? svrKeyUpdateTime : now;
  keyData->metadata.dbKeyUpdateTime = (dbKeyUpdateTime > 0) ? dbKeyUpdateTime : now;

  return 0;
}

// Write encrypted key data to file with atomic operation
int32_t taoskWriteEncryptedFile(const char *filepath, const SEncryptedKeyData *keyData) {
  if (filepath == NULL || keyData == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = 0;
  int64_t now = taosGetTimestampMs();

  // Prepare header
  SEncryptFileHeader header = {0};
  strncpy(header.magic, ENCRYPT_FILE_MAGIC, sizeof(header.magic) - 1);
  header.version = ENCRYPT_FILE_VERSION;  // file format version, not algorithm
  header.dataLen = sizeof(SEncryptedKeyData);

  // Write to temp file (atomic operation)
  char tempFile[PATH_MAX] = {0};
  snprintf(tempFile, sizeof(tempFile), "%s.tmp.%" PRId64, filepath, now);

  TdFilePtr pFile = taosOpenFile(tempFile, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    return terrno;
  }

  // Write header
  if (taosWriteFile(pFile, &header, sizeof(SEncryptFileHeader)) != sizeof(SEncryptFileHeader)) {
    code = terrno;
    taosCloseFile(&pFile);
    taosRemoveFile(tempFile);
    return code;
  }

  // Write encrypted key data
  if (taosWriteFile(pFile, keyData, sizeof(SEncryptedKeyData)) != sizeof(SEncryptedKeyData)) {
    code = terrno;
    taosCloseFile(&pFile);
    taosRemoveFile(tempFile);
    return code;
  }

  taosFsyncFile(pFile);
  taosCloseFile(&pFile);

  // Set file permissions (600 - owner read/write only)
  chmod(tempFile, 0600);

  // Atomic replacement
  if (taosRenameFile(tempFile, filepath) != 0) {
    code = terrno;
    taosRemoveFile(tempFile);
    return code;
  }

  return 0;
}

// Load encryption keys from split files (master.bin and derived.bin)
// This function is called by taosd (dmFile.c) to load keys into global variables
//
// Parameters:
//   masterKeyFile: Full path to master.bin file
//   derivedKeyFile: Full path to derived.bin file
int32_t taoskLoadEncryptKeys(const char *masterKeyFile, const char *derivedKeyFile, char *svrKey, char *dbKey,
                             char *cfgKey, char *metaKey, char *dataKey, int32_t *algorithm, int32_t *cfgAlgorithm,
                             int32_t *metaAlgorithm, int32_t *fileVersion, int32_t *keyVersion, int64_t *createTime,
                             int64_t *svrKeyUpdateTime, int64_t *dbKeyUpdateTime) {
  if (masterKeyFile == NULL || derivedKeyFile == NULL || svrKey == NULL || dbKey == NULL || cfgKey == NULL ||
      metaKey == NULL || dataKey == NULL || algorithm == NULL || cfgAlgorithm == NULL || metaAlgorithm == NULL ||
      fileVersion == NULL || keyVersion == NULL || createTime == NULL || svrKeyUpdateTime == NULL ||
      dbKeyUpdateTime == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Initialize output buffers
  memset(svrKey, 0, ENCRYPT_KEY_LEN + 1);
  memset(dbKey, 0, ENCRYPT_KEY_LEN + 1);
  memset(cfgKey, 0, ENCRYPT_KEY_LEN + 1);
  memset(metaKey, 0, ENCRYPT_KEY_LEN + 1);
  memset(dataKey, 0, ENCRYPT_KEY_LEN + 1);
  *algorithm = 0;
  *cfgAlgorithm = 0;
  *metaAlgorithm = 0;
  *fileVersion = 0;
  *keyVersion = 0;
  *createTime = 0;
  *svrKeyUpdateTime = 0;
  *dbKeyUpdateTime = 0;

  int32_t code = 0;

  // Read master key file
  SMasterKeyData masterKeyData = {0};
  char          *svrKeyDecrypted = NULL;
  char          *dbKeyDecrypted = NULL;

  code = taoskReadMasterKeyFile(masterKeyFile, &svrKeyDecrypted, &dbKeyDecrypted, &masterKeyData);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to read master key file: %s\n", masterKeyFile);
    return code;
  }

  // Copy decrypted master keys (only copy ENCRYPT_KEY_LEN bytes)
  strncpy(svrKey, svrKeyDecrypted, ENCRYPT_KEY_LEN);
  svrKey[ENCRYPT_KEY_LEN] = '\0';
  strncpy(dbKey, dbKeyDecrypted, ENCRYPT_KEY_LEN);
  dbKey[ENCRYPT_KEY_LEN] = '\0';

  // Set metadata from master key file
  *fileVersion = ENCRYPT_FILE_VERSION;
  *algorithm = masterKeyData.metadata.algorithm;
  *cfgAlgorithm = masterKeyData.metadata.cfgAlgorithm;
  *metaAlgorithm = masterKeyData.metadata.metaAlgorithm;
  *keyVersion = masterKeyData.metadata.keyVersion;
  *createTime = masterKeyData.metadata.createTime;
  *svrKeyUpdateTime = masterKeyData.metadata.svrKeyUpdateTime;
  *dbKeyUpdateTime = masterKeyData.metadata.dbKeyUpdateTime;

  // Read derived key file
  SDerivedKeyData derivedKeyData = {0};
  char           *cfgKeyDecrypted = NULL;
  char           *metaKeyDecrypted = NULL;
  char           *dataKeyDecrypted = NULL;

  code = taoskReadDerivedKeyFile(derivedKeyFile, dbKeyDecrypted, &cfgKeyDecrypted, &metaKeyDecrypted, &dataKeyDecrypted,
                                 &derivedKeyData);
  if (code == 0) {
    // Copy decrypted derived keys (only copy ENCRYPT_KEY_LEN bytes)
    if (cfgKeyDecrypted != NULL) {
      strncpy(cfgKey, cfgKeyDecrypted, ENCRYPT_KEY_LEN);
      cfgKey[ENCRYPT_KEY_LEN] = '\0';
      taosMemoryFreeClear(cfgKeyDecrypted);
    }
    if (metaKeyDecrypted != NULL) {
      strncpy(metaKey, metaKeyDecrypted, ENCRYPT_KEY_LEN);
      metaKey[ENCRYPT_KEY_LEN] = '\0';
      taosMemoryFreeClear(metaKeyDecrypted);
    }
    if (dataKeyDecrypted != NULL) {
      strncpy(dataKey, dataKeyDecrypted, ENCRYPT_KEY_LEN);
      dataKey[ENCRYPT_KEY_LEN] = '\0';
      taosMemoryFreeClear(dataKeyDecrypted);
    }
  }
  // If derived key file doesn't exist, that's ok - derived keys remain empty

  taosMemoryFreeClear(svrKeyDecrypted);
  taosMemoryFreeClear(dbKeyDecrypted);

  return 0;
}

// Save encryption keys to split files (master.bin and derived.bin)
// This function is called by taosd to save updated keys (e.g., after key rotation via SQL)
//
// Parameters:
//   masterKeyFile: Full path to master.bin file
//   derivedKeyFile: Full path to derived.bin file
int32_t taoskSaveEncryptKeys(const char *masterKeyFile, const char *derivedKeyFile, const char *svrKey,
                             const char *dbKey, const char *cfgKey, const char *metaKey, const char *dataKey,
                             int32_t algorithm, int32_t cfgAlgorithm, int32_t metaAlgorithm, int32_t keyVersion,
                             int64_t createTime, int64_t svrKeyUpdateTime, int64_t dbKeyUpdateTime) {
  if (masterKeyFile == NULL || derivedKeyFile == NULL || svrKey == NULL || dbKey == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Verify required keys are not empty
  if (svrKey[0] == '\0' || dbKey[0] == '\0') {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = 0;

  // Build and write master key data
  SMasterKeyData masterKeyData = {0};
  code = taoskBuildMasterKeyData(svrKey, dbKey, algorithm, cfgAlgorithm, metaAlgorithm, keyVersion, createTime,
                                 svrKeyUpdateTime, dbKeyUpdateTime, &masterKeyData);
  if (code != 0) {
    return code;
  }

  code = taoskWriteMasterKeyFile(masterKeyFile, &masterKeyData);
  if (code != 0) {
    return code;
  }

  // Build and write derived key data if any derived keys are present
  if ((cfgKey != NULL && cfgKey[0] != '\0') || (metaKey != NULL && metaKey[0] != '\0') ||
      (dataKey != NULL && dataKey[0] != '\0')) {
    SDerivedKeyData derivedKeyData = {0};
    code = taoskBuildDerivedKeyData(cfgKey, metaKey, dataKey, dbKey, cfgAlgorithm, metaAlgorithm, &derivedKeyData);
    if (code != 0) {
      return code;
    }

    code = taoskWriteDerivedKeyFile(derivedKeyFile, &derivedKeyData);
    if (code != 0) {
      return code;
    }
  }

  return 0;
}

// Backup master keys to portable format (without machine ID binding)
// This creates a backup file where svrKey is encrypted with user's password instead of machine ID
int32_t taoskBackupMasterKeysPortable(const char *masterKeyFile, const char *backupFile,
                                      const char *svrKeyForVerification) {
  if (masterKeyFile == NULL || backupFile == NULL || svrKeyForVerification == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t        code = 0;
  char          *svrKeyDecrypted = NULL;
  char          *dbKeyDecrypted = NULL;
  SMasterKeyData masterKeyData = {0};

  // Step 1: Read and decrypt current master key file
  printf("Reading master key file: %s\n", masterKeyFile);
  code = taoskReadMasterKeyFile(masterKeyFile, &svrKeyDecrypted, &dbKeyDecrypted, &masterKeyData);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to read master key file: %s\n", tstrerror(code));
    return code;
  }

  // Step 2: Verify the provided svrKey matches the decrypted svrKey
  printf("Verifying server key...\n");
  if (strcmp(svrKeyDecrypted, svrKeyForVerification) != 0) {
    fprintf(stderr, "Error: Server key verification failed!\n");
    fprintf(stderr, "       The provided server key does not match the key in the file.\n");
    taosMemoryFreeClear(svrKeyDecrypted);
    taosMemoryFreeClear(dbKeyDecrypted);
    return TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
  }
  printf("Server key verified successfully.\n");

  // Step 3: Create portable backup data structure
  // In portable backup, svrKey is encrypted with user's password (same as verification key)
  // instead of machine ID
  printf("Creating portable backup (without machine ID)...\n");
  SPortableBackupData backupData = {0};
  char               *encrypted = NULL;

  // Encrypt svrKey with user's password
  code = taoskEncryptData(svrKeyDecrypted, svrKeyForVerification, &encrypted);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to encrypt server key for backup\n");
    goto cleanup;
  }
  strncpy(backupData.svrKeyEncrypted, encrypted, ENCRYPTED_KEY_MAX_LEN);
  taosMemoryFreeClear(encrypted);

  // Encrypt dbKey with svrKey (same as original)
  code = taoskEncryptData(dbKeyDecrypted, svrKeyDecrypted, &encrypted);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to encrypt database key for backup\n");
    goto cleanup;
  }
  strncpy(backupData.dbKeyEncrypted, encrypted, ENCRYPTED_KEY_MAX_LEN);
  taosMemoryFreeClear(encrypted);

  // Copy metadata
  memcpy(&backupData.metadata, &masterKeyData.metadata, sizeof(SEncryptMetadata));
  backupData.backupTime = taosGetTimestampMs();

  // Step 4: Write portable backup file
  printf("Writing portable backup file: %s\n", backupFile);

  // Prepare header
  SEncryptFileHeader header = {0};
  strncpy(header.magic, ENCRYPT_FILE_MAGIC, sizeof(header.magic) - 1);
  header.version = ENCRYPT_FILE_VERSION;
  header.dataLen = sizeof(SPortableBackupData);

  // Write to temp file (atomic operation)
  char tempFile[PATH_MAX] = {0};
  snprintf(tempFile, sizeof(tempFile), "%s.tmp.%" PRId64, backupFile, taosGetTimestampMs());

  TdFilePtr pFile = taosOpenFile(tempFile, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    code = terrno;
    fprintf(stderr, "Error: Failed to create backup file\n");
    goto cleanup;
  }

  // Write header
  if (taosWriteFile(pFile, &header, sizeof(SEncryptFileHeader)) != sizeof(SEncryptFileHeader)) {
    code = terrno;
    taosCloseFile(&pFile);
    taosRemoveFile(tempFile);
    goto cleanup;
  }

  // Write portable backup data
  if (taosWriteFile(pFile, &backupData, sizeof(SPortableBackupData)) != sizeof(SPortableBackupData)) {
    code = terrno;
    taosCloseFile(&pFile);
    taosRemoveFile(tempFile);
    goto cleanup;
  }

  taosFsyncFile(pFile);
  taosCloseFile(&pFile);

  // Set file permissions (600 - owner read/write only)
  chmod(tempFile, 0600);

  // Atomic replacement
  if (taosRenameFile(tempFile, backupFile) != 0) {
    code = terrno;
    taosRemoveFile(tempFile);
    goto cleanup;
  }

  printf("Backup completed successfully.\n");
  code = 0;

cleanup:
  taosMemoryFreeClear(svrKeyDecrypted);
  taosMemoryFreeClear(dbKeyDecrypted);
  return code;
}

// Restore master keys from portable backup (add machine ID binding)
// This reads a portable backup and creates a master key file bound to current machine ID
int32_t taoskRestoreMasterKeysPortable(const char *backupFile, const char *masterKeyFile, const char *svrKeyPassword) {
  if (backupFile == NULL || masterKeyFile == NULL || svrKeyPassword == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = 0;
  char   *svrKeyDecrypted = NULL;
  char   *dbKeyDecrypted = NULL;
  char   *machineId = NULL;

  // Step 1: Read portable backup file
  printf("Reading portable backup file: %s\n", backupFile);

  TdFilePtr pFile = taosOpenFile(backupFile, TD_FILE_READ);
  if (pFile == NULL) {
    fprintf(stderr, "Error: Failed to open backup file: %s\n", backupFile);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  // Read header
  SEncryptFileHeader header = {0};
  int64_t            nread = taosReadFile(pFile, &header, sizeof(SEncryptFileHeader));
  if (nread != sizeof(SEncryptFileHeader)) {
    fprintf(stderr, "Error: Failed to read backup file header\n");
    taosCloseFile(&pFile);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  // Verify magic
  if (strncmp(header.magic, ENCRYPT_FILE_MAGIC, strlen(ENCRYPT_FILE_MAGIC)) != 0) {
    fprintf(stderr, "Error: Invalid backup file format\n");
    taosCloseFile(&pFile);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  // Read portable backup data
  SPortableBackupData backupData = {0};
  nread = taosReadFile(pFile, &backupData, sizeof(SPortableBackupData));
  if (nread != sizeof(SPortableBackupData)) {
    fprintf(stderr, "Error: Failed to read backup data\n");
    taosCloseFile(&pFile);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  taosCloseFile(&pFile);

  // Step 2: Decrypt keys from portable backup using user's password
  printf("Decrypting keys from backup...\n");

  // Decrypt svrKey with user's password
  code = taoskDecryptData(backupData.svrKeyEncrypted, svrKeyPassword, &svrKeyDecrypted);
  if (code != 0 || svrKeyDecrypted == NULL) {
    fprintf(stderr, "Error: Failed to decrypt server key from backup.\n");
    fprintf(stderr, "       The provided server key may be incorrect.\n");
    return code != 0 ? code : TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
  }

  // Verify the provided svrKeyPassword matches the decrypted svrKey
  // In backup, svrKey is encrypted with itself as password, so they should match
  printf("Verifying server key...\n");
  if (strcmp(svrKeyDecrypted, svrKeyPassword) != 0) {
    fprintf(stderr, "Error: Server key verification failed!\n");
    fprintf(stderr, "       The provided server key does not match the key in the backup.\n");
    taosMemoryFreeClear(svrKeyDecrypted);
    return TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
  }
  printf("Server key verified successfully.\n");

  // Decrypt dbKey with svrKey
  code = taoskDecryptData(backupData.dbKeyEncrypted, svrKeyDecrypted, &dbKeyDecrypted);
  if (code != 0 || dbKeyDecrypted == NULL) {
    fprintf(stderr, "Error: Failed to decrypt database key from backup\n");
    taosMemoryFreeClear(svrKeyDecrypted);
    return code != 0 ? code : TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
  }

  printf("Keys decrypted successfully.\n");

  // Step 3: Get current machine ID
  printf("Binding keys to current machine...\n");
  code = tGetMachineId(&machineId);
  if (code != 0 || machineId == NULL) {
    fprintf(stderr, "Error: Failed to get machine ID: %s\n", tstrerror(code));
    fprintf(stderr, "Storage security feature requires valid machine code.\n");
    taosMemoryFreeClear(svrKeyDecrypted);
    taosMemoryFreeClear(dbKeyDecrypted);
    return code == 0 ? TSDB_CODE_FAILED : code;
  }

  printf("Machine Code: %s\n", machineId);

  // Step 4: Create new master key data with current machine ID
  SMasterKeyData masterKeyData = {0};
  code = taoskBuildMasterKeyData(
      svrKeyDecrypted, dbKeyDecrypted, backupData.metadata.algorithm, backupData.metadata.cfgAlgorithm,
      backupData.metadata.metaAlgorithm, backupData.metadata.keyVersion, backupData.metadata.createTime,
      backupData.metadata.svrKeyUpdateTime, backupData.metadata.dbKeyUpdateTime, &masterKeyData);

  taosMemoryFreeClear(machineId);
  taosMemoryFreeClear(svrKeyDecrypted);
  taosMemoryFreeClear(dbKeyDecrypted);

  if (code != 0) {
    fprintf(stderr, "Error: Failed to build master key data\n");
    return code;
  }

  // Step 5: Check if destination file exists
  if (taosStatFile(masterKeyFile, NULL, NULL, NULL) >= 0) {
    printf("Warning: Master key file %s already exists and will be overwritten\n", masterKeyFile);
  }

  // Step 6: Write new master key file
  printf("Writing master key file: %s\n", masterKeyFile);
  code = taoskWriteMasterKeyFile(masterKeyFile, &masterKeyData);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to write master key file: %s\n", tstrerror(code));
    return code;
  }

  printf("Restore completed successfully.\n");
  return 0;
}

// ============================================================================
// Encryption/Decryption Functions using Community CBC Implementation
// ============================================================================

// Encrypt data using CBC encryption (SM4/AES based on algorithm)
// This replaces the OpenSSL-based AES-GCM implementation
int32_t taoskEncryptData(const char *plaintext, const char *key, char **ciphertext) {
  if (plaintext == NULL || key == NULL || ciphertext == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t plaintextLen = strlen(plaintext);
  int32_t keyLen = strlen(key);

  if (plaintextLen == 0 || keyLen < ENCRYPT_KEY_LEN_MIN) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Calculate encrypted length (align to 16-byte boundary)
  int32_t encryptedLen = ENCRYPTED_LEN(plaintextLen);

  // Allocate and zero-fill source buffer
  char *paddedPlaintext = taosMemoryCalloc(1, encryptedLen);
  if (paddedPlaintext == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  memcpy(paddedPlaintext, plaintext, plaintextLen);

  // Allocate result buffer
  char *encrypted = taosMemoryCalloc(1, encryptedLen + 1);
  if (encrypted == NULL) {
    taosMemoryFree(paddedPlaintext);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // Setup encryption options
  SCryptOpts opts = {0};
  opts.len = encryptedLen;
  opts.source = paddedPlaintext;
  opts.result = encrypted;
  opts.unitLen = 16;
  opts.pOsslAlgrName = "SM4-CBC:SM4";  // Use SM4 algorithm
  tstrncpy((char *)opts.key, key, ENCRYPT_KEY_LEN + 1);

  // Perform CBC encryption (CBC_Encrypt handles padding internally)
  int32_t count = CBC_Encrypt(&opts);
  taosMemoryFree(paddedPlaintext);

  if (count != opts.len) {
    taosMemoryFree(encrypted);
    return terrno ? terrno : TSDB_CODE_FAILED;
  }

  // Base64 encode for storage
  char *base64Output = NULL;
  if (base64_encode((unsigned char *)encrypted, encryptedLen, &base64Output) != 0 || !base64Output) {
    taosMemoryFree(encrypted);
    return TSDB_CODE_FAILED;
  }
  taosMemoryFree(encrypted);

  *ciphertext = base64Output;
  return 0;
}

// Decrypt data using CBC decryption (SM4/AES based on algorithm)
// This replaces the OpenSSL-based AES-GCM implementation
int32_t taoskDecryptData(const char *ciphertext, const char *key, char **plaintext) {
  if (ciphertext == NULL || key == NULL || plaintext == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t keyLen = strlen(key);
  if (keyLen < ENCRYPT_KEY_LEN_MIN) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Base64 decode
  int32_t  decodedLen = 0;
  uint8_t *decoded = NULL;
  int32_t  code = base64_decode(ciphertext, strlen(ciphertext), &decodedLen, &decoded);
  if (code != 0 || !decoded || decodedLen <= 0) {
    return code != 0 ? code : TSDB_CODE_INVALID_PARA;
  }

  // Allocate buffer for decrypted data
  char *decrypted = taosMemoryCalloc(1, decodedLen + 1);
  if (decrypted == NULL) {
    taosMemoryFree(decoded);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // Setup decryption options
  SCryptOpts opts = {0};
  opts.len = decodedLen;
  opts.source = (char *)decoded;
  opts.result = decrypted;
  opts.unitLen = 16;
  opts.pOsslAlgrName = "SM4-CBC:SM4";  // Use SM4 algorithm
  tstrncpy((char *)opts.key, key, ENCRYPT_KEY_LEN + 1);

  // Perform CBC decryption (CBC_Decrypt handles padding removal internally)
  int32_t count = CBC_Decrypt(&opts);
  taosMemoryFree(decoded);

  if (count != opts.len) {
    taosMemoryFree(decrypted);
    return terrno ? terrno : TSDB_CODE_FAILED;
  }

  // Null-terminate the result (strlen will find actual length without padding)
  decrypted[decodedLen] = '\0';
  int32_t actualLen = strlen(decrypted);

  // Trim to actual length to remove any trailing nulls from padding
  if (actualLen < decodedLen) {
    decrypted[actualLen] = '\0';
  }

  *plaintext = decrypted;
  return 0;
}
